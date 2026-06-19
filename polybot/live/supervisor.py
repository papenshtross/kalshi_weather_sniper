"""Multi-strategy supervisor.

Reads a supervisor YAML, spawns one asyncio task per strategy config.
Each task is an independent BinaryArbMM instance running on its own market,
writing to Postgres tagged with its own strategy_id.

Supports auto-rolling for 15-minute BTC Up/Down markets: if a strategy config
has `auto_roll: btc_updown_15m`, the supervisor picks the currently-active
15m event from Gamma API and restarts the strategy on the next block when
the current one expires.

Usage:
    python -m polybot.live.supervisor --config config/supervisor.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import websockets
import yaml
from dotenv import load_dotenv
from loguru import logger

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.live.momentum_5m_runner import run_momentum_strategy
from polybot.persistence.writer import PolybotWriter
from polybot.strategies.binary_arb_mm import BinaryArbMM

WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
STARTING_CASH = 10_000.0


# ====================================================================== market discovery


def should_select_btc_15m_event(end_dt: datetime, now: datetime | None = None) -> bool:
    """Return True for any still-live 15m BTC market, including near-expiry.

    The old picker skipped markets with <60s remaining. If the previous market
    stopped early and the next market had just appeared with less than that
    buffer, the supervisor could sleep through part of the live window. For live
    trading we want the next currently-live market immediately, with no extra
    buffer beyond avoiding already-expired events.
    """
    now = now or datetime.now(timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    return (end_dt - now).total_seconds() > 0


def fast_roll_retry_sleep_seconds() -> float:
    """Subsecond retry interval for auto-roll gaps around market boundaries."""
    return 0.25


async def pick_btc_updown_15m(client: httpx.AsyncClient, exclude_slugs: set[str] | None = None) -> dict | None:
    """Return the soonest-ending live 15m BTC up/down event that has not ended."""
    exclude_slugs = exclude_slugs or set()
    r = await client.get("https://gamma-api.polymarket.com/events", params={
        "closed": "false", "active": "true", "limit": 500,
        "tag_slug": "crypto", "order": "endDate", "ascending": "true",
    })
    if r.status_code != 200:
        return None
    now = datetime.now(timezone.utc)
    best = None
    best_secs = None
    for e in r.json():
        slug = e.get("slug") or ""
        if "btc-updown-15m" not in slug:
            continue
        if slug in exclude_slugs:
            continue
        end = e.get("endDate", "")
        try:
            dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        except Exception:
            continue
        if not should_select_btc_15m_event(dt, now):
            continue
        secs = (dt - now).total_seconds()
        if best_secs is None or secs < best_secs:
            best = e
            best_secs = secs
    return best


async def resolve_event_tokens(client: httpx.AsyncClient, slug: str) -> dict | None:
    r = await client.get("https://gamma-api.polymarket.com/events", params={"slug": slug})
    if r.status_code != 200:
        return None
    events = r.json()
    if not events:
        return None
    ev = events[0]
    markets = ev.get("markets", [])
    if not markets:
        return None
    m = markets[0]
    toks = m.get("clobTokenIds")
    outs = m.get("outcomes")
    if isinstance(toks, str):
        toks = json.loads(toks)
    if isinstance(outs, str):
        outs = json.loads(outs)
    if not toks or len(toks) < 2:
        return None
    end = ev.get("endDate", "")
    try:
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except Exception:
        end_dt = None
    return {
        "title": ev.get("title") or m.get("question", slug),
        "slug": slug,
        "yes_token": str(toks[0]),
        "no_token": str(toks[1]),
        "yes_label": outs[0] if outs else "YES",
        "no_label": outs[1] if outs else "NO",
        "tick_size": str(m.get("orderPriceMinTickSize") or m.get("minimumTickSize") or m.get("tickSize") or "0.01"),
        "order_min_size": _safe_float(m.get("orderMinSize"), 0.0),
        "neg_risk": bool(m.get("negRisk") or m.get("neg_risk") or False),
        "end_dt": end_dt,
    }


async def rest_book(client: httpx.AsyncClient, token_id: str) -> tuple[float, float] | None:
    full = await rest_book_full(client, token_id)
    if full is None:
        return None
    return full[0], full[1]


async def rest_book_full(client: httpx.AsyncClient, token_id: str):
    """Return (best_bid, best_ask, bids_list, asks_list) or None."""
    try:
        r = await client.get("https://clob.polymarket.com/book", params={"token_id": token_id}, timeout=10)
        if r.status_code != 200:
            return None
        b = r.json()
        bids = [{"price": float(x["price"]), "size": float(x["size"])} for x in b.get("bids", [])]
        asks = [{"price": float(x["price"]), "size": float(x["size"])} for x in b.get("asks", [])]
        if not bids or not asks:
            return None
        bb = max(x["price"] for x in bids)
        ba = min(x["price"] for x in asks)
        if 0 < bb <= ba < 1:
            return bb, ba, bids, asks
    except Exception:
        pass
    return None


# ====================================================================== live execution helpers

from dataclasses import dataclass


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


@dataclass
class LiveExecutionPlan:
    first_leg: str
    second_leg: str
    first_token: str
    second_token: str
    first_limit: float
    second_limit: float
    pair_size: float
    trigger_cap: float
    estimated_pair_cost: float
    tick_size: str = "0.01"
    neg_risk: bool = False


def _best_level_size(asks: list[dict[str, float]]) -> float:
    if not asks:
        return 0.0
    best = min(_safe_float(x.get("price")) for x in asks)
    return sum(_safe_float(x.get("size")) for x in asks if _safe_float(x.get("price")) <= best + 1e-9)


def _depth_available_through_price(asks: list[dict[str, float]], max_price: float) -> float:
    return sum(_safe_float(x.get("size")) for x in asks if 0 < _safe_float(x.get("price")) <= max_price + 1e-9)


def _best_bid_size(bids: list[dict[str, float]]) -> tuple[float, float]:
    if not bids:
        return 0.0, 0.0
    best = max(_safe_float(x.get("price")) for x in bids)
    size = sum(_safe_float(x.get("size")) for x in bids if _safe_float(x.get("price")) >= best - 1e-9)
    return best, size


def _sell_price_for_size(bids: list[dict[str, float]], target_size: float) -> tuple[float, float]:
    """Return the lowest limit price that should fully cross enough bid depth.

    For an emergency SELL unwind, a limit placed at the worst bid level needed to
    satisfy the full target size can still execute immediately at that level or
    better. This is safer than requiring the entire size to be sitting only at
    the top of book.
    """
    if target_size <= 0:
        return 0.0, 0.0
    cumulative = 0.0
    worst_price = 0.0
    for level in sorted(bids, key=lambda x: _safe_float(x.get("price")), reverse=True):
        px = _safe_float(level.get("price"))
        sz = _safe_float(level.get("size"))
        if px <= 0 or sz <= 0:
            continue
        cumulative += sz
        worst_price = px
        if cumulative + 1e-9 >= target_size:
            return worst_price, cumulative
    return 0.0, cumulative


def _apply_runtime_config(strat: BinaryArbMM, cfg: dict[str, Any]) -> None:
    strat.threshold = float(cfg.get("threshold", strat.threshold))
    strat.fee_per_share = float(cfg.get("fee_per_share", strat.fee_per_share))
    strat.min_edge = float(cfg.get("min_edge", strat.min_edge))
    strat.pair_size = float(cfg.get("pair_size", strat.pair_size))
    strat.max_inventory = float(cfg.get("max_position_size", cfg.get("max_inventory", strat.max_inventory)))
    strat.slow_offset = float(cfg.get("slow_offset", strat.slow_offset))
    strat.max_wait_seconds = float(cfg.get("max_wait_seconds", strat.max_wait_seconds))


def _build_live_execution_plan(
    strat: BinaryArbMM,
    cfg: dict[str, Any],
    executed_orders: int,
    yes_asks: list[dict[str, float]],
    no_asks: list[dict[str, float]],
) -> tuple[LiveExecutionPlan | None, str, dict[str, float]]:
    max_orders = int(cfg.get("max_executed_orders", 0) or 0)
    if max_orders and executed_orders + 2 > max_orders:
        return None, "order_limit", {}
    if not (0 < strat.yes_ask < 1 and 0 < strat.no_ask < 1):
        return None, "missing_book", {}
    if not yes_asks or not no_asks:
        return None, "missing_book", {}

    trigger_cap = min(float(cfg.get("threshold", strat.threshold)), 1.0 - 2 * strat.fee_per_share - strat.min_edge)
    total_ask = strat.yes_ask + strat.no_ask
    if total_ask > trigger_cap:
        return None, "threshold", {"total_ask": total_ask, "trigger_cap": trigger_cap}

    yes_spread = strat.yes_ask - strat.yes_bid
    no_spread = strat.no_ask - strat.no_bid
    first_leg = "YES" if (yes_spread > no_spread or (abs(yes_spread - no_spread) < 1e-9 and strat.yes_ask <= strat.no_ask)) else "NO"
    second_leg = "NO" if first_leg == "YES" else "YES"

    first_token = strat.yes_token if first_leg == "YES" else strat.no_token
    second_token = strat.no_token if second_leg == "NO" else strat.yes_token
    first_asks = yes_asks if first_leg == "YES" else no_asks
    second_asks = no_asks if second_leg == "NO" else yes_asks

    desired_pair_size = float(cfg.get("pair_size", strat.pair_size) or strat.pair_size)
    max_order_size = float(cfg.get("max_order_size", 0) or 0)
    max_position_size = float(cfg.get("max_position_size", strat.max_inventory))
    current_cost = strat.yes_pos * strat.yes_avg + strat.no_pos * strat.no_avg
    remaining_position_budget = max(0.0, max_position_size - current_cost)
    max_position_cap = remaining_position_budget / trigger_cap if trigger_cap > 0 else 0.0

    candidate_levels = sorted({round(_safe_float(x.get("price")), 3) for x in first_asks if 0 < _safe_float(x.get("price")) < 1})
    best_candidate = None
    best_feasible_size = 0.0
    best_details = None
    min_order_notional = float(cfg.get("min_order_notional", 1.0) or 0.0)
    min_order_size_shares = float(cfg.get("min_order_size_shares", 0.0) or 0.0)

    for first_limit in candidate_levels:
        second_limit = round(trigger_cap - first_limit, 3)
        if second_limit <= 0:
            continue
        notional_min_size = max(
            min_order_notional / first_limit if min_order_notional > 0 else 0.0,
            min_order_notional / max(second_limit, 1e-9) if min_order_notional > 0 else 0.0,
        )
        min_required_size = max(min_order_size_shares, notional_min_size)
        max_order_cap = min(max_order_size / first_limit, max_order_size / second_limit) if max_order_size > 0 else float("inf")
        first_depth_cap = _depth_available_through_price(first_asks, first_limit)
        second_depth_cap = _depth_available_through_price(second_asks, second_limit)
        feasible_size = min(desired_pair_size, max_order_cap, max_position_cap, first_depth_cap, second_depth_cap)
        details = {
            "min_required_size": min_required_size,
            "max_feasible_size": feasible_size,
            "first_limit": first_limit,
            "second_limit": second_limit,
            "estimated_pair_cost": trigger_cap * min_required_size,
            "trigger_cap": trigger_cap,
            "max_order_cap": max_order_cap,
            "max_position_cap": max_position_cap,
            "first_depth_cap": first_depth_cap,
            "second_depth_cap": second_depth_cap,
            "max_order_size": max_order_size,
        }
        if feasible_size + 1e-9 >= min_required_size:
            if (feasible_size > best_feasible_size + 1e-9) or (best_candidate is None):
                best_feasible_size = feasible_size
                best_candidate = (first_limit, second_limit, min_required_size)
                best_details = details
        elif best_details is None or feasible_size > best_details.get("max_feasible_size", 0.0):
            best_details = details

    if best_candidate is None:
        details = best_details or {
            "min_required_size": 0.0,
            "max_feasible_size": 0.0,
            "first_limit": 0.0,
            "second_limit": 0.0,
            "estimated_pair_cost": 0.0,
            "trigger_cap": trigger_cap,
            "max_order_cap": 0.0,
            "max_position_cap": max_position_cap,
            "first_depth_cap": 0.0,
            "second_depth_cap": 0.0,
            "max_order_size": max_order_size,
        }
        if details.get("second_limit", 0.0) <= 0:
            return None, "second_leg_budget", details
        if details.get("max_position_cap", 0.0) + 1e-9 < details.get("min_required_size", 0.0):
            return None, "max_position_size", details
        if details.get("max_order_cap", 0.0) + 1e-9 < details.get("min_required_size", 0.0):
            return None, "max_order_size", details
        return None, "insufficient_depth", details

    first_limit, second_limit, _ = best_candidate
    plan = LiveExecutionPlan(
        first_leg=first_leg,
        second_leg=second_leg,
        first_token=first_token,
        second_token=second_token,
        first_limit=first_limit,
        second_limit=second_limit,
        pair_size=round(best_feasible_size, 4),
        trigger_cap=trigger_cap,
        estimated_pair_cost=round(trigger_cap * best_feasible_size, 4),
        tick_size=str(cfg.get("tick_size") or "0.01"),
        neg_risk=bool(cfg.get("neg_risk", False)),
    )
    return plan, "ok", best_details or {}


async def _execute_live_pair(
    exec_client: PolymarketExecutionClient,
    writer: PolybotWriter,
    strategy_id: str,
    strat: BinaryArbMM,
    cfg: dict[str, Any],
    fill_seq: list[int],
    executed_orders: list[int],
    plan: LiveExecutionPlan,
    latest_books: dict[str, dict[str, list[dict[str, float]]]],
) -> bool:
    pair_size = Decimal(str(plan.pair_size))
    first_px = Decimal(str(round(plan.first_limit, 3)))
    second_px = Decimal(str(round(plan.second_limit, 3)))
    ts = time.time()
    try:
        await writer.log_strategy_event(
            strategy_id,
            f"Submitting live {plan.first_leg} order size={float(pair_size):.4f} px={float(first_px):.3f}",
        )
        first_resp = exec_client.submit(PolyOrder(
            token_id=plan.first_token,
            side="BUY",
            price=first_px,
            size=pair_size,
            order_type="FOK",
            tick_size=plan.tick_size,
            neg_risk=plan.neg_risk,
            use_limit_order=bool(cfg.get("use_limit_fok", False)),
        ))
        if not first_resp.get("success"):
            logger.warning("[{}] live {} order not successful: {}", strat.market[:30], plan.first_leg, first_resp)
            await writer.log_strategy_event(strategy_id, f"{plan.first_leg} order rejected: {first_resp}", level="ERROR")
            return False
        fill_seq[0] += 1
        await writer.record_fill(strategy_id, fill_seq[0], f"{strat.market[:40]} [LIVE] {plan.first_leg}", "BUY", float(first_px), float(pair_size), kind="LIVE")
        await writer.log_strategy_event(strategy_id, f"{plan.first_leg} order filled: order_id={first_resp.get('orderID')} making={first_resp.get('makingAmount')} taking={first_resp.get('takingAmount')}")
        executed_orders[0] += 1

        second_budget = max(0.0, round(plan.trigger_cap - float(first_px), 3))
        second_px = Decimal(str(round(min(plan.second_limit, second_budget), 3)))
        await writer.log_strategy_event(
            strategy_id,
            f"Submitting live {plan.second_leg} hedge size={float(pair_size):.4f} px={float(second_px):.3f} remaining_budget={second_budget:.3f}",
        )
        try:
            second_resp = exec_client.submit(PolyOrder(
                token_id=plan.second_token,
                side="BUY",
                price=second_px,
                size=pair_size,
                order_type="FOK",
                tick_size=plan.tick_size,
                neg_risk=plan.neg_risk,
                use_limit_order=bool(cfg.get("use_limit_fok", False)),
            ))
        except Exception as e:
            second_resp = {"success": False, "error": str(e), "exception_type": type(e).__name__}
        if not second_resp.get("success"):
            logger.error("[{}] live {} leg failed after {} filled: {}", strat.market[:30], plan.second_leg, plan.first_leg, second_resp)
            await writer.log_strategy_event(strategy_id, f"{plan.second_leg} order rejected after {plan.first_leg} fill: {second_resp}", level="ERROR")

            unwind_books = latest_books.get(plan.first_token, {})
            best_bid, top_bid_size = _best_bid_size(unwind_books.get("bids", []))
            unwind_limit, cumulative_bid_size = _sell_price_for_size(unwind_books.get("bids", []), float(pair_size))
            if unwind_limit > 0 and cumulative_bid_size + 1e-9 >= float(pair_size):
                unwind_px = Decimal(str(round(unwind_limit, 3)))
                await writer.log_strategy_event(
                    strategy_id,
                    f"Attempting recovery unwind on {plan.first_leg} size={float(pair_size):.4f} px={float(unwind_px):.3f} top_bid={best_bid:.3f} top_bid_size={top_bid_size:.4f} cumulative_bid_size={cumulative_bid_size:.4f}",
                    level="WARN",
                )
                try:
                    unwind_resp = exec_client.submit(PolyOrder(
                        token_id=plan.first_token,
                        side="SELL",
                        price=unwind_px,
                        size=pair_size,
                        order_type="FOK",
                        tick_size=plan.tick_size,
                        neg_risk=plan.neg_risk,
                    ))
                except Exception as e:
                    unwind_resp = {"success": False, "error": str(e), "exception_type": type(e).__name__}
                if unwind_resp.get("success"):
                    fill_seq[0] += 1
                    await writer.record_fill(strategy_id, fill_seq[0], f"{strat.market[:40]} [RECOVERY] {plan.first_leg}", "SELL", float(unwind_px), float(pair_size), kind="RECOVERY")
                    executed_orders[0] += 1
                    realized = strat.apply_live_recovery_loss(plan.first_leg, float(first_px), float(unwind_px), float(pair_size))
                    await writer.log_strategy_event(strategy_id, f"Recovery unwind filled: {plan.first_leg} sold @ {float(unwind_px):.3f}, realized_pnl=${realized:.4f}", level="WARN")
                    return False
                await writer.log_strategy_event(strategy_id, f"Recovery unwind failed: {unwind_resp}", level="ERROR")
            else:
                await writer.log_strategy_event(strategy_id, f"Recovery unwind unavailable: top_bid={best_bid:.3f} top_bid_size={top_bid_size:.4f} cumulative_bid_size={cumulative_bid_size:.4f}", level="ERROR")

            strat.apply_live_residual(ts, plan.first_leg, float(first_px), float(pair_size))
            await writer.log_strategy_event(strategy_id, f"Residual exposure remains on {plan.first_leg}: size={float(pair_size):.4f} px={float(first_px):.3f}. Stopping strategy.", level="ERROR")
            await writer.set_strategy_status(strategy_id, "stopped")
            return False

        fill_seq[0] += 1
        await writer.record_fill(strategy_id, fill_seq[0], f"{strat.market[:40]} [LIVE] {plan.second_leg}", "BUY", float(second_px), float(pair_size), kind="LIVE")
        await writer.log_strategy_event(strategy_id, f"{plan.second_leg} order filled: order_id={second_resp.get('orderID')} making={second_resp.get('makingAmount')} taking={second_resp.get('takingAmount')}")
        executed_orders[0] += 1

        yes_px = float(first_px if plan.first_leg == "YES" else second_px)
        no_px = float(first_px if plan.first_leg == "NO" else second_px)
        strat.apply_live_pair(ts, yes_px, no_px, float(pair_size))
        profit = (1.0 - yes_px - no_px - 2 * strat.fee_per_share) * float(pair_size)
        logger.info("★ [{}] LIVE ARB size={} profit=${:.4f} (YES@{} NO@{})", strat.market[:30], float(pair_size), profit, yes_px, no_px)
        await writer.log_strategy_event(strategy_id, f"LIVE ARB completed: size={float(pair_size):.4f} YES@{yes_px:.3f} NO@{no_px:.3f} profit=${profit:.4f}")
        return True
    except Exception as e:
        logger.error("[{}] live pair execution error: {}", strat.market[:30], e)
        await writer.log_strategy_event(strategy_id, f"Live pair execution error: {e}", level="ERROR")
        return False


# ====================================================================== single-strategy task

async def run_strategy(
    writer: PolybotWriter,
    strategy_id: str,
    name: str,
    cfg: dict[str, Any],
    stop: asyncio.Event,
    runtime_mode: str = "paper",
) -> None:
    """One asyncio task = one strategy instance on one market window.

    If `cfg['auto_roll'] == 'btc_updown_15m'`, rolls to next 15m event when
    the current one expires.
    """
    fill_seq_base = int(time.time() * 1000) % 10_000_000
    fill_seq = [fill_seq_base]
    paused = asyncio.Event()  # set = strategy is stopped via dashboard
    simulate_execution = runtime_mode != "live"
    exec_client = PolymarketExecutionClient() if runtime_mode == "live" else None
    executed_orders = [0]
    effective_cfg = dict(cfg)
    current_market_stop: list[asyncio.Event | None] = [None]
    restart_requested = [False]

    async def status_poller():
        """Poll Postgres every 2s for dashboard control commands."""
        while not stop.is_set():
            try:
                st = await writer.get_strategy_status(strategy_id)
                if st == "stop_requested" or st == "stopped":
                    if not paused.is_set():
                        logger.info("[{}] STOP received from dashboard", name)
                        await writer.log_strategy_event(strategy_id, "STOP received from dashboard")
                    paused.set()
                    if st == "stop_requested":
                        await writer.set_strategy_status(strategy_id, "stopped")
                elif st == "restart_requested":
                    if not restart_requested[0]:
                        logger.info("[{}] RESTART requested after config save", name)
                        await writer.log_strategy_event(strategy_id, "RESTART requested after config save")
                    restart_requested[0] = True
                    paused.set()
                    if current_market_stop[0] is not None:
                        current_market_stop[0].set()
                elif st == "running":
                    if paused.is_set() and not restart_requested[0]:
                        logger.info("[{}] START received from dashboard", name)
                        await writer.log_strategy_event(strategy_id, "START received from dashboard")
                    paused.clear()
            except Exception as e:
                logger.warning("[{}] status poll error: {}", name, e)
            await asyncio.sleep(2)

    poller_task = asyncio.create_task(status_poller())
    executed_orders[0] = await writer.count_fills(strategy_id)
    preferred_roll_slug: list[str | None] = [None]

    while not stop.is_set():
        # ---- merge dashboard-saved config over file config ----
        try:
            db_cfg = await writer.get_strategy_config(strategy_id)
            effective_cfg = {**cfg, **(db_cfg or {})}
        except Exception:
            effective_cfg = dict(cfg)

        # ---- pick market ----
        async with httpx.AsyncClient(timeout=15) as c:
            if cfg.get("auto_roll") == "btc_updown_15m":
                if preferred_roll_slug[0]:
                    ev_raw = {"slug": preferred_roll_slug[0]}
                    preferred_roll_slug[0] = None
                else:
                    ev_raw = await pick_btc_updown_15m(c)
                if not ev_raw:
                    logger.warning("[{}] no 15m BTC market available, retrying in {:.2f}s", name, fast_roll_retry_sleep_seconds())
                    await asyncio.sleep(fast_roll_retry_sleep_seconds())
                    continue
                ev = await resolve_event_tokens(c, ev_raw["slug"])
            else:
                ev = await resolve_event_tokens(c, cfg["event_slug"])
            if not ev:
                logger.error("[{}] could not resolve event", name)
                await asyncio.sleep(10)
                continue
            # Carry CLOB V2 market metadata through to execution so signed
            # orders use the correct tick and neg-risk exchange contract.
            effective_cfg = {
                **effective_cfg,
                "tick_size": effective_cfg.get("tick_size") or ev.get("tick_size") or "0.01",
                # Do not inherit Polymarket orderMinSize as an arb sizing limiter.
                # Crypto arb shards size from dollar caps/notional, not a shared
                # min-shares floor that can block skewed YES/NO books.
                "min_order_size_shares": effective_cfg.get("min_order_size_shares") or 0,
                "neg_risk": effective_cfg.get("neg_risk", ev.get("neg_risk", False)),
            }

            strat = BinaryArbMM(
                market=ev["title"][:80],
                yes_token=ev["yes_token"],
                no_token=ev["no_token"],
                threshold=float(effective_cfg.get("threshold", 0.99)),
                fee_per_share=float(effective_cfg.get("fee_per_share", 0.0)),
                min_edge=float(effective_cfg.get("min_edge", 0.002)),
                pair_size=float(effective_cfg.get("pair_size", 20)),
                max_inventory=float(effective_cfg.get("max_position_size", effective_cfg.get("max_inventory", 500))),
                slow_offset=float(effective_cfg.get("slow_offset", 0.01)),
                max_wait_seconds=float(effective_cfg.get("max_wait_seconds", 60)),
            )
            _apply_runtime_config(strat, effective_cfg)

            await writer.register_strategy(
                strategy_id=strategy_id, name=name, kind="binary_arb_mm",
                market=ev["title"][:80], config=effective_cfg,
            )
            logger.info("[{}] market: {}", name, ev["title"])
            await writer.log_strategy_event(strategy_id, f"Market selected: {ev['title']}")

            # seed books
            y_full = await rest_book_full(c, strat.yes_token)
            n_full = await rest_book_full(c, strat.no_token)

        latest_books: dict[str, dict[str, list[dict[str, float]]]] = {
            strat.yes_token: {"bids": [], "asks": []},
            strat.no_token: {"bids": [], "asks": []},
        }
        now = time.time()
        if y_full:
            ybb, yba, ybids, yasks = y_full
            latest_books[strat.yes_token] = {"bids": ybids, "asks": yasks}
            strat.on_book(strat.yes_token, ybb, yba, now, simulate_fills=simulate_execution)
        if n_full:
            nbb, nba, nbids, nasks = n_full
            latest_books[strat.no_token] = {"bids": nbids, "asks": nasks}
            strat.on_book(strat.no_token, nbb, nba, now, simulate_fills=simulate_execution)

        # Clear any stale book_latest rows from a previous market window for this strategy
        try:
            async with writer._pool.acquire() as _con:
                await _con.execute(
                    "DELETE FROM book_latest WHERE strategy_id=$1 AND token <> ALL($2::text[])",
                    strategy_id, [strat.yes_token, strat.no_token],
                )
        except Exception:
            pass

        # token → (label, last_tick_ts)
        token_labels = {strat.yes_token: ev["yes_label"], strat.no_token: ev["no_label"]}
        last_tick_write: dict[str, float] = {}
        purge_counter = [0]

        async def write_market_data(token: str, bb: float, ba: float,
                                    bids: list | None = None, asks: list | None = None):
            label = token_labels.get(token, "?")
            tnow = time.time()
            # Always update book_latest (cheap upsert, one row per token)
            if bids is not None and asks is not None:
                try:
                    top_bids = sorted(
                        ({"px": float(b["price"]), "sz": float(b["size"])} for b in bids),
                        key=lambda x: -x["px"])[:10]
                    top_asks = sorted(
                        ({"px": float(a["price"]), "sz": float(a["size"])} for a in asks),
                        key=lambda x: x["px"])[:10]
                    await writer.upsert_book(strategy_id, token, label, top_bids, top_asks, bb, ba)
                except Exception:
                    pass
            else:
                await writer.upsert_book(strategy_id, token, label, [], [], bb, ba)
            # Throttle time-series ticks to 1 Hz per token
            if tnow - last_tick_write.get(token, 0) >= 1.0:
                await writer.record_tick(strategy_id, token, label, bb, ba)
                last_tick_write[token] = tnow
                purge_counter[0] += 1
                if purge_counter[0] >= 120:  # ~every 2 minutes
                    purge_counter[0] = 0
                    try:
                        await writer.purge_old_ticks(strategy_id, keep_minutes=60)
                    except Exception:
                        pass

        async def flush_state():
            st = strat.state_dict()
            await writer.upsert_position(
                strategy_id=strategy_id,
                market=st["market"], side=st["side"],
                size=st["size"], entry=st["entry"], last=st["last"], pnl=st["pnl"],
            )

        await flush_state()

        market_end = ev.get("end_dt")
        market_stop = asyncio.Event()
        current_market_stop[0] = market_stop

        async def equity_loop():
            """Snapshot equity every 60s (realized cash only, no unrealized noise)."""
            while not market_stop.is_set() and not stop.is_set():
                await writer.snapshot_equity(strategy_id, round(STARTING_CASH + strat.cash, 2))
                await flush_state()
                try:
                    await asyncio.wait_for(asyncio.wait(
                        [asyncio.create_task(market_stop.wait()), asyncio.create_task(stop.wait())],
                        return_when=asyncio.FIRST_COMPLETED,
                    ), timeout=60.0)
                except asyncio.TimeoutError:
                    pass

        eq_task = asyncio.create_task(equity_loop())

        async def book_poll_loop():
            # Periodic REST snapshot — guarantees book_latest (with depth) +
            # price_ticks keep updating even when the WS is quiet or the
            # strategy is paused. 0.5s cadence for max responsiveness.
            async with httpx.AsyncClient() as poll_client:
                while not market_stop.is_set() and not stop.is_set():
                    for tok in (strat.yes_token, strat.no_token):
                        try:
                            full = await rest_book_full(poll_client, tok)
                            if full is None:
                                continue
                            bb, ba, bids, asks = full
                            latest_books[tok] = {"bids": bids, "asks": asks}
                            # Always update strategy books; only paper mode simulates fills.
                            if not paused.is_set() and strat.state != "STOPPED":
                                strat.on_book(tok, bb, ba, time.time(), simulate_fills=simulate_execution)
                            await write_market_data(tok, bb, ba, bids, asks)
                        except Exception as e:
                            logger.debug("[{}] book poll err: {}", name, e)

                    if not simulate_execution and not paused.is_set() and strat.state != "STOPPED":
                        try:
                            db_cfg_live = await writer.get_strategy_config(strategy_id)
                            effective_cfg = {**cfg, **(db_cfg_live or {})}
                            _apply_runtime_config(strat, effective_cfg)
                        except Exception as e:
                            logger.debug("[{}] live config refresh err: {}", name, e)
                        plan, reason, details = _build_live_execution_plan(
                            strat,
                            effective_cfg,
                            executed_orders[0],
                            latest_books.get(strat.yes_token, {}).get("asks", []),
                            latest_books.get(strat.no_token, {}).get("asks", []),
                        )
                        if plan is not None and exec_client is not None:
                            ok = await _execute_live_pair(exec_client, writer, strategy_id, strat, effective_cfg, fill_seq, executed_orders, plan, latest_books)
                            await flush_state()
                            max_orders = int(effective_cfg.get("max_executed_orders", 0) or 0)
                            if max_orders and executed_orders[0] >= max_orders:
                                logger.info("[{}] max executed orders reached ({}), stopping", name, executed_orders[0])
                                await writer.log_strategy_event(strategy_id, f"Max executed orders reached ({executed_orders[0]}), stopping strategy")
                                await writer.set_strategy_status(strategy_id, "stopped")
                                paused.set()
                                market_stop.set()
                                break
                            if not ok and await writer.get_strategy_status(strategy_id) == "stopped":
                                paused.set()
                                market_stop.set()
                                break
                        elif reason == "max_position_size":
                            await writer.log_strategy_event(
                                strategy_id,
                                f"Skipped live execution: minimum profitable pair requires {details.get('min_required_size', 0.0):.4f} shares and estimated pair cost ${details.get('estimated_pair_cost', 0.0):.2f}, but max_position_size=${float(effective_cfg.get('max_position_size', 0)):.2f} only allows {details.get('max_position_cap', 0.0):.4f} shares",
                                level="WARN",
                            )
                        elif reason == "max_order_size":
                            await writer.log_strategy_event(
                                strategy_id,
                                f"Skipped live execution: minimum profitable pair requires {details.get('min_required_size', 0.0):.4f} shares, but max_order_size=${float(effective_cfg.get('max_order_size', 0)):.2f} only allows {details.get('max_order_cap', 0.0):.4f} shares at first_limit={details.get('first_limit', 0.0):.3f} and second_limit={details.get('second_limit', 0.0):.3f}",
                                level="WARN",
                            )
                        elif reason == "insufficient_depth":
                            await writer.log_strategy_event(
                                strategy_id,
                                f"Skipped live execution: book depth only supports first_leg={details.get('first_depth_cap', 0.0):.4f} shares and second_leg={details.get('second_depth_cap', 0.0):.4f} shares within profitable limits; need at least {details.get('min_required_size', 0.0):.4f}",
                                level="WARN",
                            )
                        elif reason == "second_leg_budget":
                            await writer.log_strategy_event(
                                strategy_id,
                                f"Skipped live execution: after first_leg limit {details.get('first_limit', 0.0):.3f}, remaining profitable budget for leg 2 is {details.get('second_limit', 0.0):.3f} (trigger_cap={details.get('trigger_cap', 0.0):.3f}), so no valid hedge price remains",
                                level="WARN",
                            )
                    try:
                        await asyncio.wait_for(
                            asyncio.wait(
                                [asyncio.create_task(market_stop.wait()), asyncio.create_task(stop.wait())],
                                return_when=asyncio.FIRST_COMPLETED,
                            ),
                            timeout=0.5,
                        )
                    except asyncio.TimeoutError:
                        pass

        book_task = asyncio.create_task(book_poll_loop())

        # ---- websocket loop for this market window ----
        asset_ids = [strat.yes_token, strat.no_token]
        logger.info("[{}] connecting ws → {} / {}", name, ev["yes_label"], ev["no_label"])

        last_roll_market_check = [0.0]

        try:
            while not market_stop.is_set() and not stop.is_set():
                # Watchdog: switch immediately when a different live auto-roll
                # market is available. Do not sleep through the boundary or wait
                # for closed-market resolution; only stay on the current market
                # while no replacement is live yet.
                if market_end:
                    now_utc = datetime.now(timezone.utc)
                    remaining = (market_end - now_utc).total_seconds()
                    if cfg.get("auto_roll") == "btc_updown_15m" and time.time() - last_roll_market_check[0] >= fast_roll_retry_sleep_seconds():
                        last_roll_market_check[0] = time.time()
                        try:
                            async with httpx.AsyncClient(timeout=5) as roll_client:
                                next_raw = await pick_btc_updown_15m(roll_client, exclude_slugs={ev["slug"]})
                            if next_raw and next_raw.get("slug"):
                                preferred_roll_slug[0] = str(next_raw["slug"])
                                logger.info("[{}] next live market {} available with {:.0f}s left in current; rolling immediately", name, preferred_roll_slug[0], remaining)
                                await writer.log_strategy_event(strategy_id, f"Next live market available; rolling immediately to {preferred_roll_slug[0]}")
                                market_stop.set()
                                break
                        except Exception as e:
                            logger.debug("[{}] next auto-roll market check failed: {}", name, e)
                    if remaining <= 0:
                        logger.info("[{}] market {} ended, rolling", name, ev["title"])
                        market_stop.set()
                        break

                try:
                    async with websockets.connect(WS_MARKET, ping_interval=20, ping_timeout=20) as ws:
                        await ws.send(json.dumps({"type": "Market", "assets_ids": asset_ids}))
                        logger.info("[{}] subscribed", name)
                        await writer.log_strategy_event(strategy_id, "Subscribed to market websocket")
                        msg_n = 0
                        async for raw in ws:
                            if market_stop.is_set() or stop.is_set():
                                break
                            if market_end:
                                remaining = (market_end - datetime.now(timezone.utc)).total_seconds()
                                if cfg.get("auto_roll") == "btc_updown_15m" and time.time() - last_roll_market_check[0] >= fast_roll_retry_sleep_seconds():
                                    last_roll_market_check[0] = time.time()
                                    try:
                                        async with httpx.AsyncClient(timeout=5) as roll_client:
                                            next_raw = await pick_btc_updown_15m(roll_client, exclude_slugs={ev["slug"]})
                                        if next_raw and next_raw.get("slug"):
                                            preferred_roll_slug[0] = str(next_raw["slug"])
                                            await writer.log_strategy_event(strategy_id, f"Next live market available; rolling immediately to {preferred_roll_slug[0]}")
                                            market_stop.set()
                                            break
                                    except Exception as e:
                                        logger.debug("[{}] next auto-roll market check failed: {}", name, e)
                                if remaining <= 0:
                                    market_stop.set()
                                    break
                            try:
                                msgs = json.loads(raw)
                            except Exception:
                                continue
                            if not isinstance(msgs, list):
                                msgs = [msgs]
                            msg_n += len(msgs)
                            any_update = False

                            # If paused via dashboard, drop all resting orders and skip processing
                            if paused.is_set():
                                if strat.state != "STOPPED":
                                    strat.cancel_all()
                                    logger.info("[{}] paused → cancelled resting orders", name)
                                    any_update = True
                                continue
                            elif strat.state == "STOPPED":
                                # resumed from dashboard
                                strat.state = "SCANNING"

                            for m in msgs:
                                # Shape A: book snapshot
                                if "bids" in m and "asks" in m:
                                    tok = str(m.get("asset_id", ""))
                                    try:
                                        bb = max(float(b["price"]) for b in m["bids"]) if m["bids"] else 0.0
                                        ba = min(float(a["price"]) for a in m["asks"]) if m["asks"] else 0.0
                                    except Exception:
                                        continue
                                    if 0 < bb <= ba < 1:
                                        new_fills = strat.on_book(tok, bb, ba, time.time(), simulate_fills=simulate_execution)
                                        if simulate_execution and new_fills:
                                            any_update = True
                                            await _record_fills(writer, strategy_id, strat, new_fills, fill_seq)
                                        await write_market_data(tok, bb, ba, m["bids"], m["asks"])
                                    continue
                                # Shape B: price_changes array
                                pcs = m.get("price_changes")
                                if isinstance(pcs, list):
                                    for pc in pcs:
                                        tok = str(pc.get("asset_id", ""))
                                        try:
                                            bb = float(pc.get("best_bid", 0) or 0)
                                            ba = float(pc.get("best_ask", 0) or 0)
                                        except Exception:
                                            continue
                                        if 0 < bb <= ba < 1:
                                            new_fills = strat.on_book(tok, bb, ba, time.time(), simulate_fills=simulate_execution)
                                            if simulate_execution and new_fills:
                                                any_update = True
                                                await _record_fills(writer, strategy_id, strat, new_fills, fill_seq)
                                            await write_market_data(tok, bb, ba)

                            if any_update:
                                await flush_state()
                except Exception as e:
                    if not (market_stop.is_set() or stop.is_set()):
                        logger.warning("[{}] ws error: {} — reconnecting", name, e)
                        await asyncio.sleep(2)
        finally:
            current_market_stop[0] = None
            eq_task.cancel()
            book_task.cancel()
            await writer.snapshot_equity(strategy_id, round(STARTING_CASH + strat.cash, 2))
            await flush_state()
            logger.info("[{}] window closed: arbs={} residuals={} cash=${:.2f} total_pnl=${:.2f}",
                        name, strat.arb_count, strat.residual_count, strat.cash, strat.total_pnl)

        if restart_requested[0]:
            restart_requested[0] = False
            paused.clear()
            await writer.set_strategy_status(strategy_id, "running")
            await writer.log_strategy_event(strategy_id, "Strategy restarted with latest saved config")
            await asyncio.sleep(1)
            continue

        if not cfg.get("auto_roll"):
            await writer.set_strategy_status(strategy_id, "stopped")
            poller_task.cancel()
            return
        await asyncio.sleep(5)  # brief pause before rolling to next 15m window

    poller_task.cancel()


async def _record_fills(writer, strategy_id, strat, fills, seq_box):
    for f in fills:
        seq_box[0] += 1
        label = f"{strat.market[:40]} [{f.kind}]"
        if f.yes_px:
            await writer.record_fill(strategy_id, seq_box[0], f"{label} YES", "BUY", f.yes_px, f.size, kind=f.kind)
            seq_box[0] += 1
        if f.no_px:
            await writer.record_fill(strategy_id, seq_box[0], f"{label} NO ", "BUY", f.no_px, f.size, kind=f.kind)
        if f.kind == "ARB":
            logger.info("★ [{}] ARB size={} profit=${:.2f} (YES@{} NO@{})",
                        strat.market[:30], f.size, f.profit, f.yes_px, f.no_px)


# ====================================================================== main

async def main_async(config_path: Path) -> None:
    load_dotenv()
    cfg = yaml.safe_load(config_path.read_text()) or {}
    runtime_mode = str(cfg.get("mode") or "paper").lower()
    strategies = cfg.get("strategies") or []
    if not strategies:
        raise SystemExit("supervisor config must have a 'strategies:' list")

    writer = PolybotWriter(os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL"))
    await writer.connect()

    logger.info("supervisor starting with {} strategies", len(strategies))

    stop = asyncio.Event()
    def _stop(*_):
        stop.set()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    tasks = []
    for s in strategies:
        sid = s["id"]
        name = s.get("name", sid)
        kind = str(s.get("kind") or "binary_arb_mm")
        logger.info("  → {} ({}) [{}]", name, sid, kind)
        if kind == "momentum_consensus_dynamic_entry_5m":
            tasks.append(asyncio.create_task(run_momentum_strategy(writer, sid, name, s, stop)))
        else:
            tasks.append(asyncio.create_task(run_strategy(writer, sid, name, s, stop, runtime_mode=runtime_mode)))

    await asyncio.gather(*tasks, return_exceptions=True)
    await writer.close()
    logger.info("supervisor stopped")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, required=True)
    args = p.parse_args()
    asyncio.run(main_async(args.config))


if __name__ == "__main__":
    main()
