#!/usr/bin/env python3
"""Polymarket L2 websocket high-frequency momentum/volatility runner.

This is intentionally event-driven: it subscribes to CLOB market websocket books,
maintains hot in-memory state, evaluates signals on every book event, and keeps
DB writes out of the hot path except for status/order events.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

import requests
import websockets
from dotenv import load_dotenv
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.persistence.writer import PolybotWriter

GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK = "https://clob.polymarket.com/book"
CLOB_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Prism-live-hf-l2-momentum/1.0"})


def D(x: Any) -> Decimal:
    return Decimal(str(x))


def q4(x: Decimal) -> Decimal:
    return D(x).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def parse_jsonish(v: Any, default: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return default
    return v if v is not None else default


def parse_dt(v: Any) -> datetime | None:
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        try:
            dt = datetime.strptime(str(v), "%Y-%m-%d %H:%M:%S%z")
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_live_sports_market(m: dict[str, Any], cfg: dict[str, Any], now: datetime | None = None) -> bool:
    """Return true only for sports markets whose actual game window is live/near-live.

    Polymarket Gamma marks many sports futures as active+acceptingOrders; for HF in-play
    momentum we require gameStartTime/eventStartTime and a bounded live window.
    """
    if not bool(cfg.get("l2_live_sports_only", True)):
        return True
    now = now or datetime.now(timezone.utc)
    start = parse_dt(m.get("gameStartTime") or m.get("eventStartTime"))
    if start is None:
        # Futures/outrights like World Cup winner are sports category but not live events.
        return False
    pre_grace = timedelta(minutes=float(cfg.get("l2_live_pre_start_grace_min", 5)))
    max_age = timedelta(hours=float(cfg.get("l2_live_max_event_age_hours", 5)))
    # We intentionally do not trust endDate for games; it is often market expiry, not match clock.
    return (start - pre_grace) <= now <= (start + max_age)


def infer_category(title: str, slug: str = "") -> str:
    s = (title + " " + slug).lower()
    sports = ["nba", "nfl", "nhl", "mlb", "ncaab", "ufc", "tennis", "soccer", "football", "vs.", " v ", "fifa", "baseball", "basketball", "hockey", "spread:"]
    crypto = ["bitcoin", "btc", "ethereum", "eth", "solana", " sol ", "xrp", "doge", "crypto", "up or down"]
    esports = ["esports", "league of legends", "lol", "valorant", "counter-strike", "cs2", "dota"]
    if any(x in s for x in crypto):
        return "crypto"
    if any(x in s for x in esports):
        return "esports"
    if any(x in s for x in sports):
        return "sports"
    return "other-liquid"


def event_family(slug: str) -> str:
    """Collapse moneyline/spread/total child markets for the same game into one risk family."""
    s = str(slug or "")
    for marker in ("-total-", "-spread-"):
        if marker in s:
            return s.split(marker, 1)[0]
    return s


def levels(book: dict[str, Any]) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
    bids = [(D(x["price"]), D(x["size"])) for x in book.get("bids", []) if D(x.get("size", 0)) > 0]
    asks = [(D(x["price"]), D(x["size"])) for x in book.get("asks", []) if D(x.get("size", 0)) > 0]
    return bids, asks


def book_metrics(book: dict[str, Any], band: Decimal = D("0.02")) -> dict[str, Any] | None:
    bids, asks = levels(book)
    if not bids or not asks:
        return None
    bid = max(p for p, _ in bids)
    ask = min(p for p, _ in asks)
    if ask <= bid:
        return None
    mid = (bid + ask) / D("2")
    spread = ask - bid
    lo = mid - band
    hi = mid + band
    bid_depth = sum(p * s for p, s in bids if p >= lo and p <= mid)
    ask_depth = sum(p * s for p, s in asks if p >= mid and p <= hi)
    ratio = float(bid_depth / max(ask_depth, D("0.000001")))
    return {"bid": bid, "ask": ask, "mid": mid, "spread": spread, "bid_depth": bid_depth, "ask_depth": ask_depth, "ratio": ratio}


def order_id(resp: Any) -> str | None:
    if not isinstance(resp, dict):
        return None
    for k in ("orderID", "order_id", "id"):
        if resp.get(k):
            return str(resp[k])
    if isinstance(resp.get("order"), dict):
        return order_id(resp["order"])
    return None


def status_from(resp: Any) -> str:
    raw = str((resp or {}).get("status") or "").lower()
    if raw in {"matched", "filled"}:
        return "filled"
    if raw in {"delayed", "pending", "live"}:
        return "submitted"
    if (resp or {}).get("success") is True and order_id(resp):
        return "submitted"
    if (resp or {}).get("success") is False:
        return "rejected"
    return raw or "submitted"


async def db_exec(writer: PolybotWriter, sql: str, *args: Any):
    async with writer._pool.acquire() as con:
        return await con.execute(sql, *args)


async def db_fetchval(writer: PolybotWriter, sql: str, *args: Any):
    async with writer._pool.acquire() as con:
        return await con.fetchval(sql, *args)


async def db_fetchrow(writer: PolybotWriter, sql: str, *args: Any):
    async with writer._pool.acquire() as con:
        return await con.fetchrow(sql, *args)


async def record_attempt(writer, sid, market_slug, token, outcome, side, order_type, price, size, stake, status, response, signal, cfg, err=None):
    await db_exec(
        writer,
        """
        INSERT INTO order_attempts(strategy_id,market_slug,token,outcome,side,order_type,price,size,stake_usd,status,response,error,signal,config)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13::jsonb,$14::jsonb)
        """,
        sid,
        market_slug,
        str(token),
        outcome,
        side,
        order_type,
        float(price),
        float(size),
        float(stake),
        status,
        json.dumps(response or {}),
        err,
        json.dumps(signal or {}),
        json.dumps(cfg or {}),
    )


@dataclass
class TokenMeta:
    token: str
    slug: str
    title: str
    outcome: str
    tick: Decimal
    neg_risk: bool


@dataclass
class HotAsset:
    meta: TokenMeta
    mids: deque[tuple[float, Decimal]] = field(default_factory=lambda: deque(maxlen=400))
    last_signal_ms: int = 0
    book_events: int = 0
    last_book_ms: int = 0
    metrics: dict[str, Any] | None = None


def discover_universe(cfg: dict[str, Any]) -> dict[str, TokenMeta]:
    allowed = set(cfg.get("category_filter") or ["sports"])
    max_markets = int(cfg.get("l2_max_markets", 40))
    max_assets = int(cfg.get("l2_max_assets", max_markets * 2))
    out: dict[str, TokenMeta] = {}
    now = datetime.now(timezone.utc)
    offset = 0
    while len(out) < max_assets and offset < int(cfg.get("l2_discovery_scan_limit", 1000)):
        rows = SESSION.get(
            GAMMA_MARKETS,
            params={"active": "true", "closed": "false", "limit": 100, "offset": offset, "order": "volume24hr", "ascending": "false"},
            timeout=20,
        ).json()
        if not rows:
            break
        for m in rows:
            if not m.get("acceptingOrders", True):
                continue
            title = m.get("question") or m.get("title") or ""
            slug = m.get("slug") or ""
            if allowed and infer_category(title, slug) not in allowed:
                continue
            if "sports" in allowed and not is_live_sports_market(m, cfg, now):
                continue
            toks = parse_jsonish(m.get("clobTokenIds"), [])
            outs = parse_jsonish(m.get("outcomes"), ["Yes", "No"])
            # Gamma can report 0.001 ticks for markets where CLOB rejects with
            # "minimum for the market is 0.01". Use a conservative 1c floor; 1c
            # prices are valid multiples on 0.001 markets and avoid rejected exits.
            tick = max(D(m.get("orderPriceMinTickSize") or m.get("minimumTickSize") or "0.01"), D("0.01"))
            for i, tok in enumerate(toks[:2]):
                tok = str(tok)
                if tok and tok not in out:
                    out[tok] = TokenMeta(tok, slug, title, outs[i] if i < len(outs) else str(i), tick, bool(m.get("negRisk") or False))
                if len(out) >= max_assets:
                    break
            if len(out) >= max_assets:
                break
        offset += 100
    return out


class L2Runner:
    def __init__(self, sid: str, writer: PolybotWriter, ex: PolymarketExecutionClient, cfg: dict[str, Any]):
        self.sid = sid
        self.writer = writer
        self.ex = ex
        self.cfg = self._capital_safe_cfg(cfg)
        self.assets: dict[str, HotAsset] = {}
        self.state_path = Path(cfg.get("state_path") or f"/home/administrator/projects/polybot/data/live_state_{sid}.json")
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {"open": {}, "seen_signals": []}
        self.stop = asyncio.Event()
        self.last_status_ms = 0
        self.event_count = 0
        self.trade_count = 0
        self.start_ms = int(time.time() * 1000)
        self.last_daily = 0.0
        self.wallet_balance: float | None = None
        self.active_order = False

    def _capital_safe_cfg(self, cfg: dict[str, Any]) -> dict[str, Any]:
        """Apply Prism 3 live sports runtime guardrails.

        Post-loss-analysis safety note: the 2026-05-31 high-frequency/static-L2
        patch (delta=0, ratio≈1.05, depth=$25, spread=5c, many correlated opens)
        produced high trade count but large negative PnL. Do not resurrect it on
        refresh/restart. Preserve the user's $100 daily-cap floor while restoring
        conservative, validated-entry guardrails until a fair-value/model-based
        strategy passes shadow/backtest gates.
        """
        out = dict(cfg or {})
        if self.sid == "live_hf_sports_vol_breakout_prism3":
            out.update(
                {
                    "daily_order_limit_usd": max(float(out.get("daily_order_limit_usd", 100) or 100), 100.0),
                    "daily_spend_usd": max(float(out.get("daily_spend_usd", 100) or 100), 100.0),
                    "daily_spend_limit_usd": max(float(out.get("daily_spend_limit_usd", 100) or 100), 100.0),
                    "user_min_daily_limit_usd": 100.0,
                    "order_size_usd": min(float(out.get("order_size_usd", 1) or 1), 1.0),
                    "target_trades_per_day": None,
                    "l2_entry_imbalance_ratio": max(float(out.get("l2_entry_imbalance_ratio", 1.5) or 1.5), 1.5),
                    "min_depth_usd": max(float(out.get("min_depth_usd", 250) or 250), 250.0),
                    "l2_stop_min_bid_depth_usd": max(float(out.get("l2_stop_min_bid_depth_usd", 250) or 250), 250.0),
                    "max_spread": str(min(float(out.get("max_spread", 0.02) or 0.02), 0.02)),
                    "l2_max_open_positions": min(int(out.get("l2_max_open_positions", 1) or 1), 1),
                    "concurrency_cap": min(int(out.get("concurrency_cap", 1) or 1), 1),
                    "l2_max_open_per_event_family": min(int(out.get("l2_max_open_per_event_family", 1) or 1), 1),
                    "l2_max_buys_per_market_24h": min(int(out.get("l2_max_buys_per_market_24h", 1) or 1), 1),
                    "l2_max_buys_per_event_family_24h": min(int(out.get("l2_max_buys_per_event_family_24h", 1) or 1), 1),
                    "cooldown_seconds": max(float(out.get("cooldown_seconds", 30) or 30), 30.0),
                    "l2_live_sports_only": True,
                    "l2_min_mid_delta": str(max(float(out.get("l2_min_mid_delta", 0.02) or 0.02), 0.02)),
                    "l2_lookback_ms": min(int(out.get("l2_lookback_ms", 750) or 750), 750),
                    "l2_min_book_events": max(int(out.get("l2_min_book_events", 4) or 4), 4),
                    "l2_entry_price_min": max(float(out.get("l2_entry_price_min", 0.25) or 0.25), 0.25),
                    "l2_entry_price_max": min(float(out.get("l2_entry_price_max", 0.85) or 0.85), 0.85),
                    "l2_discovery_scan_limit": max(int(out.get("l2_discovery_scan_limit", 10000) or 10000), 10000),
                    "l2_max_markets": max(int(out.get("l2_max_markets", 250) or 250), 250),
                    "l2_max_assets": max(int(out.get("l2_max_assets", 500) or 500), 500),
                    "frequency_patch_version": None,
                    "safety_lock_reason": "2026-06-01 loss analysis: static L2 50+/100-trades config disabled pending model/fair-value validation",
                }
            )
        return out

    async def refresh_cfg(self):
        st = await db_fetchrow(self.writer, "select status, config from strategies where id=$1", self.sid)
        if not st or st["status"] != "running":
            return False
        raw_cfg = dict(st["config"]) if isinstance(st["config"], dict) else json.loads(st["config"])
        self.cfg = self._capital_safe_cfg(raw_cfg)
        return True

    async def refresh_universe(self):
        meta = await asyncio.to_thread(discover_universe, self.cfg)
        self.assets = {tok: self.assets.get(tok, HotAsset(m)) for tok, m in meta.items()}
        for tok, m in meta.items():
            self.assets[tok].meta = m
        await self.writer.log_strategy_event(self.sid, f"L2 universe refreshed: {len(self.assets)} assets via websocket", "INFO")

    async def update_status(self, force: bool = False):
        now = int(time.time() * 1000)
        if not force and now - self.last_status_ms < int(self.cfg.get("status_update_ms", 5000)):
            return
        self.last_status_ms = now
        try:
            collateral = self.ex.http.clob.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", self.cfg.get("signature_type", 1) or 1)))
            )
            self.wallet_balance = float(D((collateral or {}).get("balance", "0")) / D(10) ** 6)
        except Exception:
            pass
        live_assets = sum(1 for a in self.assets.values() if a.last_book_ms and now - a.last_book_ms < 30000)
        avg_event_ms = (now - self.start_ms) / max(1, self.event_count)
        await db_exec(
            self.writer,
            """
            UPDATE strategies SET config = jsonb_strip_nulls(config || $2::jsonb), updated_at=now()
            WHERE id=$1
            """,
            self.sid,
            json.dumps(
                {
                    "data_mode": "clob_l2_websocket",
                    "implementation_suitability": "event_driven_l2_hot_book",
                    "last_heartbeat_at": now,
                    "last_daily_buy_usd": round(self.last_daily, 6),
                    "last_open_count": len(self.state.get("open", {})),
                    "last_scan_trade_count": None,
                    "last_scan_asset_count": len(self.assets),
                    "last_l2_assets_subscribed": len(self.assets),
                    "last_l2_assets_live_30s": live_assets,
                    "last_l2_book_events": self.event_count,
                    "last_l2_avg_event_ms": round(avg_event_ms, 3),
                    "l2_live_sports_only": bool(self.cfg.get("l2_live_sports_only", True)),
                    "l2_live_pre_start_grace_min": float(self.cfg.get("l2_live_pre_start_grace_min", 5)),
                    "l2_live_max_event_age_hours": float(self.cfg.get("l2_live_max_event_age_hours", 5)),
                    "l2_min_exit_profit_ticks": int(self.cfg.get("l2_min_exit_profit_ticks", 1)),
                    "exit_policy": "normalized_and_profitable_or_slippage_guarded_stop",
                    "l2_stop_loss_enabled": bool(self.cfg.get("l2_stop_loss_enabled", True)),
                    "l2_stop_loss_cents": float(self.cfg.get("l2_stop_loss_cents", 0.07)),
                    "l2_stop_min_bid_depth_usd": float(self.cfg.get("l2_stop_min_bid_depth_usd", self.cfg.get("min_depth_usd", 25))),
                    "l2_max_stop_slippage_cents": float(self.cfg.get("l2_max_stop_slippage_cents", 0.02)),
                    "l2_gap_quarantine_seconds": int(self.cfg.get("l2_gap_quarantine_seconds", 1800)),
                    "l2_entry_price_min": float(self.cfg.get("l2_entry_price_min", 0.25)),
                    "l2_entry_price_max": float(self.cfg.get("l2_entry_price_max", 0.85)),
                    "l2_max_open_positions": int(self.cfg.get("l2_max_open_positions", self.cfg.get("concurrency_cap", 2))),
                    "l2_max_open_per_event_family": int(self.cfg.get("l2_max_open_per_event_family", 1)),
                    "last_wallet_balance": self.wallet_balance,
                    "wallet_proxy": os.getenv("POLYMARKET_PROXY_ADDRESS") or self.cfg.get("wallet_proxy") or self.cfg.get("proxy_address"),
                }
            ),
        )

    async def daily_used(self) -> float:
        self.last_daily = float(
            await db_fetchval(
                self.writer,
                "select coalesce(sum(stake_usd),0) from order_attempts where strategy_id=$1 and side='BUY' and status in ('filled','submitted') and ts>now()-interval '24 hours'",
                self.sid,
            )
            or 0
        )
        return self.last_daily

    async def maybe_exit_positions(self):
        for asset, pos in list(self.state.get("open", {}).items()):
            if time.time() - float(pos.get("entry_ts", 0)) < float(self.cfg.get("min_hold_seconds", 10)):
                continue
            h = self.assets.get(asset)
            if not h or not h.metrics:
                continue
            m = h.metrics
            entry_price = D(pos.get("entry_price", pos.get("entry_mid", "0")))
            px = max(D("0.001"), m["bid"].quantize(h.meta.tick, rounding=ROUND_DOWN))
            min_exit_profit_ticks = int(self.cfg.get("l2_min_exit_profit_ticks", 1))
            profitable_px = entry_price + (h.meta.tick * D(min_exit_profit_ticks))
            take_profit_px = entry_price * (D("1") + D(str(self.cfg.get("take_profit_pct", "0.02"))))
            take_profit = entry_price > 0 and px >= take_profit_px
            bid_only_book = bool(m.get("bid_only"))
            normalized = bid_only_book or float(m["ratio"]) <= float(self.cfg.get("l2_exit_imbalance_ratio", 1.2))
            max_hold = time.time() - float(pos.get("entry_ts", 0)) >= float(self.cfg.get("max_hold_seconds", 300))
            stop_loss_enabled = bool(self.cfg.get("l2_stop_loss_enabled", True))
            stop_loss_cents = D(str(self.cfg.get("l2_stop_loss_cents", "0.07")))
            max_stop_slippage = D(str(self.cfg.get("l2_max_stop_slippage_cents", "0.02")))
            stop_px = entry_price - stop_loss_cents
            min_stop_fill_px = stop_px - max_stop_slippage
            stop_loss = stop_loss_enabled and entry_price > 0 and px <= stop_px
            stop_slippage_ok = px >= min_stop_fill_px
            stop_depth_ok = m["bid_depth"] >= D(str(self.cfg.get("l2_stop_min_bid_depth_usd", self.cfg.get("min_depth_usd", 25))))
            gap_shock = stop_loss and not stop_slippage_ok
            if gap_shock:
                fam = event_family(pos.get("slug") or h.meta.slug)
                self.state.setdefault("quarantined_families", {})[fam] = time.time() + float(self.cfg.get("l2_gap_quarantine_seconds", 1800))
                self.state_path.write_text(json.dumps(self.state, indent=2))
            try:
                bal = D((self.ex.http.clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=asset)) or {}).get("balance", "0")) / D(10) ** 6
            except Exception:
                bal = D("0")
            dust_threshold = D(str(self.cfg.get("l2_position_dust_threshold_shares", "0.01")))
            if bal <= dust_threshold:
                self.state["open"].pop(asset, None)
                self.state_path.write_text(json.dumps(self.state, indent=2))
                await self.writer.log_strategy_event(self.sid, f"L2 cleared dust position {h.meta.title[:80]} bal={bal} threshold={dust_threshold}", "INFO")
                continue

            pending_oid = pos.get("pending_exit_order_id")
            pending_ts = float(pos.get("pending_exit_ts", 0) or 0)
            if pending_oid:
                try:
                    order_state = self.ex.get_order(str(pending_oid))
                except Exception:
                    order_state = None
                order_status = str((order_state or {}).get("status") or "").upper()
                matched = D((order_state or {}).get("size_matched") or "0")
                if order_status == "MATCHED" or matched > 0:
                    self.state["open"].pop(asset, None)
                    self.state_path.write_text(json.dumps(self.state, indent=2))
                    continue
                # Delayed FOK exits often disappear/null after they fail to match. Clear
                # stale pending state even if the current book no longer satisfies exit
                # conditions, otherwise an expired pending_exit_order_id can persist in
                # live_state indefinitely and mislead accounting/supervision.
                if time.time() - pending_ts < float(self.cfg.get("l2_pending_exit_retry_seconds", 30)):
                    continue
                pos.pop("pending_exit_order_id", None)
                pos.pop("pending_exit_ts", None)
                pos.pop("pending_exit_price", None)
                pos.pop("pending_exit_size", None)
                self.state["open"][asset] = pos
                self.state_path.write_text(json.dumps(self.state, indent=2))

            # Normal exit: wait for momentum/imbalance to normalize AND require executable
            # best bid to be at least entry + configured profit ticks. A bid-only book is
            # treated as normalized for profitable exits because late/settling sports
            # books can have executable bids and no asks; otherwise positions can block
            # all future entries forever. Stop-loss remains slippage/depth gated.
            profitable_exit = entry_price > 0 and px >= profitable_px
            guarded_stop = stop_loss and stop_slippage_ok and stop_depth_ok
            if not ((normalized and profitable_exit) or guarded_stop):
                continue

            size = q4(bal)
            try:
                resp = self.ex.submit(PolyOrder(token_id=asset, side="SELL", price=px, size=size, order_type="FOK", use_limit_order=True, tick_size=str(h.meta.tick), neg_risk=h.meta.neg_risk))
                status, err = status_from(resp), None
            except Exception as e:
                resp, status, err = {"error": repr(e)}, "rejected", repr(e)
            await record_attempt(self.writer, self.sid, h.meta.slug, asset, h.meta.outcome, "SELL", "FOK", px, size, px * size, status, resp, {"exit": "l2", "take_profit": take_profit, "normalized": normalized, "bid_only_book": bid_only_book, "max_hold": max_hold, "stop_loss": stop_loss, "guarded_stop": guarded_stop, "stop_slippage_ok": stop_slippage_ok, "stop_depth_ok": stop_depth_ok, "gap_shock": gap_shock, "entry_price": str(entry_price), "min_profitable_exit_px": str(profitable_px), "stop_px": str(stop_px), "min_stop_fill_px": str(min_stop_fill_px)}, self.cfg, err)
            await self.writer.log_strategy_event(self.sid, f"L2 SELL {h.meta.title[:80]} px={px} size={size} wallet={self.cfg.get('wallet_name')} status={status}{(' error='+err[:200]) if err else ''}", "WARN")
            if status == "filled":
                self.state["open"].pop(asset, None)
            elif status == "submitted" and order_id(resp):
                pos["pending_exit_order_id"] = order_id(resp)
                pos["pending_exit_ts"] = time.time()
                pos["pending_exit_price"] = str(px)
                pos["pending_exit_size"] = str(size)
                self.state["open"][asset] = pos
            self.state_path.write_text(json.dumps(self.state, indent=2))

    async def on_book(self, asset: str, book: dict[str, Any]):
        h = self.assets.get(asset)
        if not h:
            return
        now = int(time.time() * 1000)
        m = book_metrics(book, D(str(self.cfg.get("l2_depth_band_price", "0.02"))))
        if not m:
            # Some late/settling sports books expose executable bids but no asks. They
            # are unusable for new entries, but must still feed exit logic for open
            # positions so a profitable bid can be taken and the single-position canary
            # does not remain blocked.
            if asset in self.state.get("open", {}):
                bids, _asks = levels(book)
                if bids:
                    bid = max(p for p, _ in bids)
                    band = D(str(self.cfg.get("l2_depth_band_price", "0.02")))
                    bid_depth = sum(p * s for p, s in bids if p >= bid - band)
                    h.metrics = {"bid": bid, "ask": None, "mid": bid, "spread": D("0"), "bid_depth": bid_depth, "ask_depth": D("0"), "ratio": 0.0, "bid_only": True}
                    self.event_count += 1
                    h.book_events += 1
                    h.last_book_ms = now
                    await self.maybe_exit_positions()
            return
        self.event_count += 1
        h.book_events += 1
        h.last_book_ms = now
        h.metrics = m
        h.mids.append((time.time(), m["mid"]))
        await self.maybe_exit_positions()
        open_positions = self.state.get("open", {})
        if self.active_order or asset in open_positions:
            return
        max_open = int(self.cfg.get("l2_max_open_positions", self.cfg.get("concurrency_cap", 2)))
        if len(open_positions) >= max_open:
            return
        fam = event_family(h.meta.slug)
        now_s = time.time()
        qfam = self.state.setdefault("quarantined_families", {})
        qfam = {k: v for k, v in qfam.items() if float(v) > now_s}
        self.state["quarantined_families"] = qfam
        if fam in qfam:
            return
        max_open_family = int(self.cfg.get("l2_max_open_per_event_family", 1))
        same_family_open = sum(1 for p in open_positions.values() if event_family(p.get("slug", "")) == fam)
        if same_family_open >= max_open_family:
            return
        cooldown_ms = int(float(self.cfg.get("cooldown_seconds", 30)) * 1000)
        if now - h.last_signal_ms < cooldown_ms:
            return
        lookback_ms = int(self.cfg.get("l2_lookback_ms", 750))
        old = None
        cutoff = time.time() - lookback_ms / 1000
        for ts, mid in h.mids:
            if ts <= cutoff:
                old = mid
        if old is None or len(h.mids) < int(self.cfg.get("l2_min_book_events", 4)):
            return
        delta = m["mid"] - old
        min_delta = D(str(self.cfg.get("l2_min_mid_delta", "0.02")))
        if delta < min_delta:
            return
        if m["spread"] > D(str(self.cfg.get("max_spread", "0.03"))):
            return
        if min(m["ask_depth"], m["bid_depth"]) < D(str(self.cfg.get("min_depth_usd", 25))):
            return
        if float(m["ratio"]) < float(self.cfg.get("l2_entry_imbalance_ratio", 1.5)):
            return
        cap = m["ask"].quantize(h.meta.tick, rounding=ROUND_UP)
        min_entry_px = D(str(self.cfg.get("l2_entry_price_min", "0.25")))
        max_entry_px = D(str(self.cfg.get("l2_entry_price_max", "0.85")))
        if cap < min_entry_px or cap > max_entry_px:
            return
        max_buys_per_family = int(self.cfg.get("l2_max_buys_per_event_family_24h", 2))
        recent_family_buys = int(
            await db_fetchval(
                self.writer,
                "select count(*) from order_attempts where strategy_id=$1 and market_slug like $2 and side='BUY' and status in ('filled','submitted') and ts>now()-interval '24 hours'",
                self.sid,
                fam + "%",
            )
            or 0
        )
        if recent_family_buys >= max_buys_per_family:
            h.last_signal_ms = now
            return
        daily = await self.daily_used()
        stake = D(str(self.cfg.get("order_size_usd", 1)))
        # Honor the configured daily cap directly. A user_min_daily_limit_usd
        # override must default to 0, not silently floor conservative live
        # sports configs back to $100.
        configured_daily_limit = float(self.cfg.get("daily_order_limit_usd", self.cfg.get("daily_spend_limit_usd", 30)))
        user_min_daily_limit = float(self.cfg.get("user_min_daily_limit_usd", 0) or 0)
        daily_limit = max(configured_daily_limit, user_min_daily_limit)
        if daily + float(stake) > daily_limit:
            return
        max_buys_per_market = int(self.cfg.get("l2_max_buys_per_market_24h", 2))
        recent_market_buys = int(
            await db_fetchval(
                self.writer,
                "select count(*) from order_attempts where strategy_id=$1 and token=$2 and side='BUY' and status in ('filled','submitted') and ts>now()-interval '24 hours'",
                self.sid,
                asset,
            )
            or 0
        )
        if recent_market_buys >= max_buys_per_market:
            h.last_signal_ms = now
            return
        sig_key = f"{asset}:{now}:{m['mid']}:{delta}"
        size = q4(stake / cap)
        signal = {
            "signal_key": sig_key,
            "source": "clob_l2_websocket",
            "title": h.meta.title,
            "slug": h.meta.slug,
            "asset": asset,
            "mid": str(m["mid"]),
            "delta": str(delta),
            "lookback_ms": lookback_ms,
            "spread": str(m["spread"]),
            "bid_depth": str(m["bid_depth"]),
            "ask_depth": str(m["ask_depth"]),
            "ratio": m["ratio"],
            "event_family": fam,
            "entry_price_min": str(min_entry_px),
            "entry_price_max": str(max_entry_px),
            "wallet_name": self.cfg.get("wallet_name"),
        }
        self.active_order = True
        try:
            resp = self.ex.submit(PolyOrder(token_id=asset, side="BUY", price=cap, size=size, order_type="FAK", use_limit_order=False, tick_size=str(h.meta.tick), neg_risk=h.meta.neg_risk))
            status, err = status_from(resp), None
        except Exception as e:
            resp, status, err = {"error": repr(e)}, "rejected", repr(e)
        finally:
            self.active_order = False
        await record_attempt(self.writer, self.sid, h.meta.slug, asset, h.meta.outcome, "BUY", "FAK", cap, size, stake, status, resp, signal, self.cfg, err)
        await self.writer.log_strategy_event(self.sid, f"L2 BUY {h.meta.title[:80]} px={cap} stake=${stake} Δ={delta} {lookback_ms}ms wallet={self.cfg.get('wallet_name')} status={status}{(' error='+err[:200]) if err else ''}", "WARN")
        h.last_signal_ms = now
        if status in {"filled", "submitted"}:
            self.state.setdefault("open", {})[asset] = {"asset": asset, "slug": h.meta.slug, "title": h.meta.title, "entry_ts": time.time(), "entry_mid": str(m["mid"]), "entry_price": str(cap), "size": str(size)}
            self.state.setdefault("seen_signals", []).append(sig_key)
            self.state["seen_signals"] = self.state["seen_signals"][-500:]
            self.state_path.write_text(json.dumps(self.state, indent=2))

    async def websocket_loop(self):
        while not self.stop.is_set():
            await self.refresh_cfg()
            if not self.assets:
                await self.refresh_universe()
            assets = list(self.assets.keys())
            if not assets:
                await asyncio.sleep(10)
                continue
            try:
                async with websockets.connect(CLOB_WSS, ping_interval=10, ping_timeout=10, close_timeout=5, max_queue=4096) as ws:
                    await ws.send(json.dumps({"type": "market", "assets_ids": assets, "custom_feature_enabled": True}))
                    subscribed_assets = set(assets)
                    await self.writer.log_strategy_event(self.sid, f"L2 websocket connected: subscribed {len(assets)} assets; lookback={self.cfg.get('l2_lookback_ms',750)}ms", "INFO")
                    while not self.stop.is_set():
                        current_assets = set(self.assets.keys())
                        # Maintenance refresh can grow/replace the in-play universe while the
                        # websocket remains subscribed to the old token list. Reconnect so the
                        # hot path actually receives book events for newly live games.
                        if current_assets and current_assets != subscribed_assets:
                            await self.writer.log_strategy_event(self.sid, f"L2 websocket resubscribe: {len(subscribed_assets)} -> {len(current_assets)} assets", "INFO")
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=float(self.cfg.get("l2_ws_recv_timeout_seconds", 30)))
                        except asyncio.TimeoutError:
                            if set(self.assets.keys()) != subscribed_assets:
                                await self.writer.log_strategy_event(self.sid, f"L2 websocket timeout/resubscribe: {len(subscribed_assets)} -> {len(self.assets)} assets", "INFO")
                                break
                            continue
                        try:
                            parsed = json.loads(raw)
                        except Exception:
                            continue
                        for item in parsed if isinstance(parsed, list) else [parsed]:
                            if not isinstance(item, dict):
                                continue
                            if item.get("event_type") not in (None, "book", "price_change"):
                                continue
                            asset = str(item.get("asset_id") or item.get("token_id") or "")
                            if item.get("bids") and item.get("asks") and asset:
                                await self.on_book(asset, item)
            except Exception as e:
                await self.writer.log_strategy_event(self.sid, f"L2 websocket error/reconnect: {repr(e)[:500]}", "ERROR")
                await asyncio.sleep(2)

    async def maintenance_loop(self):
        refresh_s = int(self.cfg.get("l2_universe_refresh_seconds", 300))
        last_refresh = 0.0
        while not self.stop.is_set():
            await self.refresh_cfg()
            if time.time() - last_refresh > refresh_s:
                await self.refresh_universe()
                last_refresh = time.time()
            await self.update_status()
            self.state_path.write_text(json.dumps(self.state, indent=2))
            await asyncio.sleep(1)

    async def run(self):
        await self.refresh_universe()
        await self.update_status(force=True)
        await asyncio.gather(self.websocket_loop(), self.maintenance_loop())


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy-id", required=True)
    ap.add_argument("--wallet-env")
    args = ap.parse_args()
    for f in ["/home/administrator/projects/polybot/.env", "/home/administrator/projects/polybot/.env.live", "/home/administrator/projects/polybot-dash/.env.local", args.wallet_env]:
        if f and Path(f).exists():
            load_dotenv(f, override=True)
    dsn = os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
    writer = PolybotWriter(dsn)
    await writer.connect()
    cfg = await writer.get_strategy_config(args.strategy_id)
    ex = PolymarketExecutionClient()
    await writer.set_strategy_status(args.strategy_id, "running")
    patch = dict(cfg)
    if args.strategy_id == "live_hf_sports_vol_breakout_prism3":
        # Post-loss-analysis safety lock: keep the $100 daily-cap floor, but do
        # not re-apply the losing static-L2 high-frequency patch on restart.
        # Throughput targets must be reintroduced only after model/fair-value
        # validation and position-accounting repair.
        patch.update(
            {
                "daily_order_limit_usd": max(float(patch.get("daily_order_limit_usd", 100) or 100), 100.0),
                "daily_spend_usd": max(float(patch.get("daily_spend_usd", 100) or 100), 100.0),
                "daily_spend_limit_usd": max(float(patch.get("daily_spend_limit_usd", 100) or 100), 100.0),
                "user_min_daily_limit_usd": 100.0,
                "order_size_usd": min(float(patch.get("order_size_usd", 1) or 1), 1.0),
                "target_trades_per_day": None,
                "l2_entry_imbalance_ratio": max(float(patch.get("l2_entry_imbalance_ratio", 1.5) or 1.5), 1.5),
                "min_depth_usd": max(float(patch.get("min_depth_usd", 250) or 250), 250.0),
                "l2_stop_min_bid_depth_usd": max(float(patch.get("l2_stop_min_bid_depth_usd", 250) or 250), 250.0),
                "max_spread": str(min(float(patch.get("max_spread", 0.02) or 0.02), 0.02)),
                "l2_max_open_positions": min(int(patch.get("l2_max_open_positions", 1) or 1), 1),
                "concurrency_cap": min(int(patch.get("concurrency_cap", 1) or 1), 1),
                "l2_max_open_per_event_family": min(int(patch.get("l2_max_open_per_event_family", 1) or 1), 1),
                "l2_max_buys_per_market_24h": min(int(patch.get("l2_max_buys_per_market_24h", 1) or 1), 1),
                "l2_max_buys_per_event_family_24h": min(int(patch.get("l2_max_buys_per_event_family_24h", 1) or 1), 1),
                "cooldown_seconds": max(float(patch.get("cooldown_seconds", 30) or 30), 30.0),
                "l2_live_sports_only": True,
                "l2_min_mid_delta": str(max(float(patch.get("l2_min_mid_delta", 0.02) or 0.02), 0.02)),
                "l2_lookback_ms": min(int(patch.get("l2_lookback_ms", 750) or 750), 750),
                "l2_min_book_events": max(int(patch.get("l2_min_book_events", 4) or 4), 4),
                "l2_entry_price_min": max(float(patch.get("l2_entry_price_min", 0.25) or 0.25), 0.25),
                "l2_entry_price_max": min(float(patch.get("l2_entry_price_max", 0.85) or 0.85), 0.85),
                "l2_discovery_scan_limit": max(int(patch.get("l2_discovery_scan_limit", 10000) or 10000), 10000),
                "l2_max_markets": max(int(patch.get("l2_max_markets", 250) or 250), 250),
                "l2_max_assets": max(int(patch.get("l2_max_assets", 500) or 500), 500),
                "frequency_patch_version": None,
                "safety_lock_reason": "2026-06-01 loss analysis: static L2 50+/100-trades config disabled pending model/fair-value validation",
            }
        )
    patch.update(
        {
            "data_mode": "clob_l2_websocket",
            "implementation_suitability": "event_driven_l2_hot_book",
            "poll_seconds": None,
            "l2_lookback_ms": int(cfg.get("l2_lookback_ms", 750)),
            "l2_min_mid_delta": str(cfg.get("l2_min_mid_delta", "0.02")),
            "l2_entry_imbalance_ratio": float(cfg.get("l2_entry_imbalance_ratio", 1.5)),
            "l2_exit_imbalance_ratio": float(cfg.get("l2_exit_imbalance_ratio", 1.2)),
            "l2_max_markets": int(cfg.get("l2_max_markets", 40)),
            "l2_max_assets": int(cfg.get("l2_max_assets", 80)),
            "status_update_ms": int(cfg.get("status_update_ms", 5000)),
            "l2_max_buys_per_market_24h": int(cfg.get("l2_max_buys_per_market_24h", 2)),
            "l2_live_sports_only": bool(cfg.get("l2_live_sports_only", True)),
            "l2_live_pre_start_grace_min": float(cfg.get("l2_live_pre_start_grace_min", 5)),
            "l2_live_max_event_age_hours": float(cfg.get("l2_live_max_event_age_hours", 5)),
            "l2_min_exit_profit_ticks": int(cfg.get("l2_min_exit_profit_ticks", 1)),
            "exit_policy": "normalized_and_profitable_or_slippage_guarded_stop",
            "l2_stop_loss_enabled": bool(cfg.get("l2_stop_loss_enabled", True)),
            "l2_stop_loss_cents": float(cfg.get("l2_stop_loss_cents", 0.07)),
            "l2_stop_min_bid_depth_usd": float(cfg.get("l2_stop_min_bid_depth_usd", cfg.get("min_depth_usd", 25))),
            "l2_max_stop_slippage_cents": float(cfg.get("l2_max_stop_slippage_cents", 0.02)),
            "l2_gap_quarantine_seconds": int(cfg.get("l2_gap_quarantine_seconds", 1800)),
            "l2_entry_price_min": float(cfg.get("l2_entry_price_min", 0.25)),
            "l2_entry_price_max": float(cfg.get("l2_entry_price_max", 0.85)),
            "l2_max_open_positions": int(cfg.get("l2_max_open_positions", cfg.get("concurrency_cap", 2))),
            "l2_max_open_per_event_family": int(cfg.get("l2_max_open_per_event_family", 1)),
        }
    )
    if args.strategy_id == "live_hf_sports_vol_breakout_prism3":
        # Re-apply the post-loss-analysis safety lock after the startup metadata
        # merge above, because that merge reads from cfg and can otherwise
        # reintroduce stale high-frequency values.
        patch.update(
            {
                "daily_order_limit_usd": 100.0,
                "daily_spend_usd": 100.0,
                "daily_spend_limit_usd": 100.0,
                "user_min_daily_limit_usd": 100.0,
                "order_size_usd": min(float(patch.get("order_size_usd", 1) or 1), 1.0),
                "target_trades_per_day": None,
                "l2_entry_imbalance_ratio": 1.5,
                "min_depth_usd": 250.0,
                "l2_stop_min_bid_depth_usd": 250.0,
                "max_spread": "0.02",
                "l2_max_open_positions": 1,
                "concurrency_cap": 1,
                "l2_max_open_per_event_family": 1,
                "l2_max_buys_per_market_24h": 1,
                "l2_max_buys_per_event_family_24h": 1,
                "cooldown_seconds": 30.0,
                "l2_live_sports_only": True,
                "l2_min_mid_delta": "0.02",
                "l2_lookback_ms": 750,
                "l2_min_book_events": 4,
                "l2_entry_price_min": 0.25,
                "l2_entry_price_max": 0.85,
                "l2_discovery_scan_limit": 10000,
                "l2_max_markets": 250,
                "l2_max_assets": 500,
                "frequency_patch_version": None,
                "safety_lock_reason": "2026-06-01 loss analysis: static L2 50+/100-trades config disabled pending model/fair-value validation",
            }
        )
    await db_exec(writer, "UPDATE strategies SET config=config || $2::jsonb, updated_at=now() WHERE id=$1", args.strategy_id, json.dumps(patch))
    await writer.log_strategy_event(args.strategy_id, f"L2 HF runner started wallet={patch.get('wallet_name')} order=${patch.get('order_size_usd')} daily_limit=${patch.get('daily_order_limit_usd')}", "INFO")
    runner = L2Runner(args.strategy_id, writer, ex, patch)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, runner.stop.set)
        except NotImplementedError:
            pass
    try:
        await runner.run()
    finally:
        await runner.update_status(force=True)
        await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
