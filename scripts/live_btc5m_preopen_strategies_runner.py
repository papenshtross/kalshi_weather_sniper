#!/usr/bin/env python3
"""Live BTC 5m pre-open/open-entry runners for Prism 6/7.

Strategies implemented from the SII-backed large backtest:

1. Prism 6 core robust SuperTrend both-side:
   st_f30_l180_n5_m3.5_both
   - frame=30s, lookback=180s, n=5, mult=3.5
   - side=both

2. Prism 7 high-edge overlay:
   confirm_up_bb5_90_10_2.5_st15_180_7_3
   - BB breakout: frame=5s, lookback=90s, n=10, k=2.5, fade=false
   - SuperTrend: frame=15s, lookback=180s, n=7, mult=3
   - require agreement, side=UP only

Signal is computed from BTCUSDT 1s Binance candles before market open and
submitted during the first open_entry_window_seconds after the BTC 5m market opens.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_UP
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import httpx
from dotenv import load_dotenv
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.live.momentum_5m_runner import fetch_binance_recent_candles, resolve_5m_event, rest_book_full
from polybot.persistence.writer import PolybotWriter
from polybot.security.wallet_registry import wallet_secret
from scripts.live_btc5m_adx_trend_runner import D, book_sides, status_from

STARTING_EQUITY = 10_000.0
MODEL_VERSION = "btc5m_preopen_open_entry_sii_top2_v1"


def candles_to_dicts(candles: list[Any]) -> list[dict[str, float]]:
    return [
        {
            "ts": int(getattr(c, "ts")),
            "open": float(getattr(c, "open")),
            "high": float(getattr(c, "high")),
            "low": float(getattr(c, "low")),
            "close": float(getattr(c, "close")),
            "volume": float(getattr(c, "volume", 0.0) or 0.0),
            "taker_buy_volume": float(getattr(c, "taker_buy_volume", 0.0) or 0.0),
        }
        for c in candles
    ]


def aggregate_preopen(candles: list[dict[str, float]], start_ts: int, lookback_s: int, frame_s: int) -> list[dict[str, float]]:
    lo = int(start_ts) - int(lookback_s)
    rows = [c for c in candles if lo <= int(c["ts"]) <= int(start_ts) and float(c.get("close") or 0) > 0]
    buckets: dict[int, dict[str, float]] = {}
    for c in rows:
        ts = int(c["ts"])
        b = (ts // int(frame_s)) * int(frame_s)
        close = float(c["close"])
        d = buckets.setdefault(
            b,
            {
                "ts": float(b),
                "open": float(c.get("open") or close),
                "high": float(c.get("high") or close),
                "low": float(c.get("low") or close),
                "close": close,
                "volume": 0.0,
                "taker_buy_volume": 0.0,
            },
        )
        d["high"] = max(d["high"], float(c.get("high") or close))
        d["low"] = min(d["low"], float(c.get("low") or close))
        d["close"] = close
        d["volume"] += float(c.get("volume") or 0.0)
        d["taker_buy_volume"] += float(c.get("taker_buy_volume") or 0.0)
    return [buckets[k] for k in sorted(buckets)]


def xs(bars: list[dict[str, float]], key: str = "close") -> list[float]:
    return [float(b[key]) for b in bars]


def sma(values: list[float], n: int) -> float | None:
    return sum(values[-n:]) / n if len(values) >= n and n > 0 else None


def std(values: list[float], n: int) -> float | None:
    if len(values) < n:
        return None
    m = sma(values, n)
    if m is None:
        return None
    return (sum((x - m) ** 2 for x in values[-n:]) / n) ** 0.5


def atr(bars: list[dict[str, float]], n: int) -> float | None:
    if len(bars) < n + 1:
        return None
    tr: list[float] = []
    for i in range(1, len(bars)):
        h, l, pc = float(bars[i]["high"]), float(bars[i]["low"]), float(bars[i - 1]["close"])
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(tr[-n:]) / n if len(tr) >= n else None


def bb_break_signal(candles: list[dict[str, float]], start_ts: int, p: dict[str, Any]) -> int:
    bars = aggregate_preopen(candles, start_ts, int(p["lookback"]), int(p["frame"]))
    c = xs(bars)
    n = int(p["n"])
    m = sma(c, n)
    sd = std(c, n)
    if m is None or sd is None:
        return 0
    up = m + float(p["k"]) * sd
    dn = m - float(p["k"]) * sd
    if p.get("fade"):
        return -1 if c[-1] > up else 1 if c[-1] < dn else 0
    return 1 if c[-1] > up else -1 if c[-1] < dn else 0


def supertrend_signal(candles: list[dict[str, float]], start_ts: int, p: dict[str, Any]) -> int:
    bars = aggregate_preopen(candles, start_ts, int(p["lookback"]), int(p["frame"]))
    if not bars:
        return 0
    c = xs(bars)
    a = atr(bars, int(p["n"]))
    if a is None:
        return 0
    hl2 = (float(bars[-1]["high"]) + float(bars[-1]["low"])) / 2.0
    mult = float(p["mult"])
    return 1 if c[-1] > hl2 + mult * a * 0.1 else -1 if c[-1] < hl2 - mult * a * 0.1 else 0


@dataclass(frozen=True)
class PreopenSignal:
    side: str
    reason: str
    score: float
    details: dict[str, Any]


def evaluate_strategy(candles: list[dict[str, float]], start_ts: int, cfg: dict[str, Any]) -> PreopenSignal | None:
    stype = str(cfg.get("preopen_strategy_type") or "").strip()
    if stype == "supertrend_both":
        p = dict(cfg["supertrend"])
        sig = supertrend_signal(candles, start_ts, p)
        if sig == 0:
            return None
        return PreopenSignal("UP" if sig > 0 else "DOWN", "supertrend_preopen_both", float(sig), {"supertrend": p, "raw_signal": sig})
    if stype == "bb_break_both":
        bb = dict(cfg["bb_breakout"])
        sig = bb_break_signal(candles, start_ts, bb)
        if sig == 0:
            return None
        return PreopenSignal("UP" if sig > 0 else "DOWN", "bb_breakout_preopen_both", float(sig), {"bb_breakout": bb, "raw_signal": sig})
    if stype == "bb_supertrend_confirm_up":
        bb = dict(cfg["bb_breakout"])
        st = dict(cfg["supertrend"])
        s1 = bb_break_signal(candles, start_ts, bb)
        s2 = supertrend_signal(candles, start_ts, st)
        if s1 == 1 and s2 == 1:
            return PreopenSignal("UP", "bb_breakout_and_supertrend_confirm_up", 2.0, {"bb_breakout": bb, "supertrend": st, "bb_signal": s1, "supertrend_signal": s2})
        return None
    raise ValueError(f"unknown preopen_strategy_type={stype!r}")


async def fetch_binance_candles_full(client: httpx.AsyncClient, start_ts: int, end_ts: int):
    rows: list[Any] = []
    cursor = int(start_ts)
    while cursor <= int(end_ts):
        chunk_end = min(int(end_ts), cursor + 899)
        part = await fetch_binance_recent_candles(client, cursor, chunk_end)
        rows.extend(part)
        cursor = chunk_end + 1
    dedup = {int(getattr(r, "ts", 0)): r for r in rows}
    return [dedup[k] for k in sorted(dedup)]


class Btc5mPreopenRunner:
    def __init__(self, strategy_id: str, writer: PolybotWriter, ex: PolymarketExecutionClient, cfg: dict[str, Any]):
        self.strategy_id = strategy_id
        self.writer = writer
        self.ex = ex
        self.cfg = dict(cfg)
        self.stop = asyncio.Event()
        self.last_wallet_balance: float | None = None

    async def db_fetchval(self, sql: str, *args: Any):
        async with self.writer._pool.acquire() as con:
            return await con.fetchval(sql, *args)

    async def db_exec(self, sql: str, *args: Any):
        async with self.writer._pool.acquire() as con:
            return await con.execute(sql, *args)

    async def refresh_cfg(self) -> bool:
        row = await self.db_fetchval("select config from strategies where id=$1 and status='running'", self.strategy_id)
        if row is None:
            return False
        self.cfg = dict(row) if isinstance(row, dict) else json.loads(row)
        return True

    async def daily_used(self) -> float:
        val = await self.db_fetchval(
            """
            select coalesce(sum(stake_usd),0) from order_attempts
            where strategy_id=$1 and side='BUY' and status in ('filled','submitted')
              and ts > now() - interval '24 hours'
            """,
            self.strategy_id,
        )
        return float(val or 0.0)

    async def market_has_terminal_buy(self, slug: str) -> bool:
        val = await self.db_fetchval(
            "select count(*) from order_attempts where strategy_id=$1 and market_slug=$2 and side='BUY' and status in ('filled','submitted')",
            self.strategy_id,
            slug,
        )
        return int(val or 0) > 0

    async def market_attempt_count(self, slug: str) -> int:
        val = await self.db_fetchval(
            "select count(*) from order_attempts where strategy_id=$1 and market_slug=$2 and side='BUY'",
            self.strategy_id,
            slug,
        )
        return int(val or 0)

    async def update_heartbeat(self, ev: dict[str, Any] | None, signal_payload: dict[str, Any] | None = None):
        try:
            collateral = self.ex.http.clob.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", self.cfg.get("signature_type", 3) or 3)))
            )
            self.last_wallet_balance = float(D((collateral or {}).get("balance", "0")) / D(10) ** 6)
        except Exception:
            pass
        await self.db_exec(
            "UPDATE strategies SET config=jsonb_strip_nulls(config || $2::jsonb), updated_at=now() WHERE id=$1",
            self.strategy_id,
            json.dumps(
                {
                    "last_heartbeat_at": int(time.time() * 1000),
                    "last_market_slug": ev.get("slug") if ev else None,
                    "last_market_start_ts": int(ev.get("start_ts") or 0) if ev else None,
                    "last_signal": signal_payload,
                    "last_daily_buy_usd": round(await self.daily_used(), 6),
                    "last_wallet_balance": self.last_wallet_balance,
                    "data_mode": "binance_1s_preopen_plus_clob_rest_books",
                    "implementation_suitability": MODEL_VERSION,
                }
            ),
        )

    async def pick_market(self, client: httpx.AsyncClient) -> dict[str, Any] | None:
        now_ts = int(time.time())
        current_start = now_ts - (now_ts % 300)
        open_window = int(self.cfg.get("open_entry_window_seconds", 20) or 20)
        candidates: list[int] = []
        if 0 <= now_ts - current_start <= open_window:
            candidates.append(current_start)
        candidates.append(current_start + 300)
        if current_start not in candidates:
            candidates.append(current_start)
        for start in candidates:
            ev = await resolve_5m_event(client, f"btc-updown-5m-{start}")
            if ev:
                return ev
        return None

    async def maybe_trade_once(self, client: httpx.AsyncClient):
        if not await self.refresh_cfg():
            await asyncio.sleep(2)
            return
        ev = await self.pick_market(client)
        if not ev:
            await self.update_heartbeat(None)
            return
        now_ts = int(time.time())
        start_ts = int(ev["start_ts"])
        end_ts = int(ev["end_dt"].timestamp())
        open_window = int(self.cfg.get("open_entry_window_seconds", 20) or 20)
        lookback = int(self.cfg.get("signal_lookback_seconds", 180) or 180)
        candles_obj = await fetch_binance_candles_full(client, start_ts - lookback - 5, min(now_ts, start_ts) + 2)
        candles = candles_to_dicts(candles_obj)
        latest_ts = int(candles[-1]["ts"]) if candles else None
        source_age_s = max(0, min(now_ts, start_ts) - latest_ts) if latest_ts else None
        up_book = await rest_book_full(client, ev["up_token"])
        down_book = await rest_book_full(client, ev["down_token"])
        up_bid, up_ask, up_bids, up_asks = book_sides(up_book)
        down_bid, down_ask, down_bids, down_asks = book_sides(down_book)
        sig = evaluate_strategy(candles, start_ts, self.cfg) if candles else None
        signal_payload: dict[str, Any] = asdict(sig) if sig else {"side": "SKIP", "reason": "no_preopen_signal"}
        signal_payload.update({"seconds_since_open": now_ts - start_ts, "seconds_until_open": start_ts - now_ts, "model_version": MODEL_VERSION, "source_age_s": source_age_s})
        await self.writer.record_market_observation(
            self.strategy_id,
            ev["slug"],
            ev.get("title") or ev["slug"],
            start_ts,
            end_ts,
            ev.get("price_to_beat"),
            ev.get("final_price"),
            ev["up_token"],
            ev["down_token"],
            up_bid,
            up_ask,
            down_bid,
            down_ask,
            up_bids,
            up_asks,
            down_bids,
            down_asks,
            {"latest_ts": latest_ts, "latest_close": float(candles[-1]["close"]) if candles else None, "candles": candles[-180:]},
            signal_payload,
            self.cfg,
            {"daily_used": await self.daily_used(), "seconds_since_open": now_ts - start_ts},
        )
        await self.writer.upsert_books([
            {"strategy_id": self.strategy_id, "token": ev["up_token"], "label": "UP", "bids": up_bids, "asks": up_asks, "best_bid": up_bid or 0, "best_ask": up_ask or 0},
            {"strategy_id": self.strategy_id, "token": ev["down_token"], "label": "DOWN", "bids": down_bids, "asks": down_asks, "best_bid": down_bid or 0, "best_ask": down_ask or 0},
        ])
        await self.update_heartbeat(ev, signal_payload)
        if sig is None:
            return
        if not (0 <= now_ts - start_ts <= open_window):
            return
        max_source_age = float(self.cfg.get("max_source_data_age_seconds", 5) or 5)
        if source_age_s is None or source_age_s > max_source_age:
            await self.writer.log_strategy_event(self.strategy_id, f"Skip stale Binance pre-open data: age={source_age_s}s max={max_source_age}s", "WARN")
            return
        if await self.market_has_terminal_buy(ev["slug"]):
            return
        attempt_count = await self.market_attempt_count(ev["slug"])
        max_attempts = int(self.cfg.get("entry_max_attempts_per_market", 30) or 30)
        if attempt_count >= max_attempts:
            await self.writer.log_strategy_event(self.strategy_id, f"Skip {ev['slug']}: entry attempts exhausted ({attempt_count}/{max_attempts})", "WARN")
            return
        stake = float(self.cfg.get("order_size_usd", 1.0) or 1.0)
        daily_limit = float(self.cfg.get("daily_order_limit_usd", 200.0) or 200.0)
        daily = await self.daily_used()
        if daily + stake > daily_limit:
            await self.writer.log_strategy_event(self.strategy_id, f"Daily cap reached: used=${daily:.2f}, stake=${stake:.2f}, limit=${daily_limit:.2f}", "WARN")
            return
        selected_token = ev["up_token"] if sig.side == "UP" else ev["down_token"]
        selected_ask = up_ask if sig.side == "UP" else down_ask
        selected_asks = up_asks if sig.side == "UP" else down_asks
        if selected_ask is None:
            return
        max_entry_ask = float(self.cfg.get("max_entry_ask", 0.98) or 0.98)
        if selected_ask > max_entry_ask:
            await self.writer.log_strategy_event(self.strategy_id, f"Skip {sig.side} ask {selected_ask:.3f} > max_entry_ask {max_entry_ask:.3f}", "WARN")
            return
        min_depth = float(self.cfg.get("min_ask_depth_usd", 1.0) or 1.0)
        depth = sum(float(x["price"]) * float(x["size"]) for x in selected_asks if float(x["price"]) <= selected_ask + 0.02)
        if depth < min_depth:
            return
        tick = str(max(D(ev.get("tick_size") or "0.01"), D("0.01")))
        px = D(selected_ask).quantize(D(tick), rounding=ROUND_UP)
        size = (D(stake) / px).quantize(D("0.0001"))
        signal_payload.update({"selected_token": selected_token, "selected_ask": float(px), "ask_depth_usd_2c": depth})
        try:
            resp = self.ex.submit(PolyOrder(token_id=selected_token, side="BUY", price=px, size=size, order_type="FAK", use_limit_order=False, tick_size=tick, neg_risk=bool(ev.get("neg_risk"))))
            status, err = status_from(resp), None
        except Exception as e:
            resp, status, err = {"error": repr(e)}, "rejected", repr(e)
        await self.writer.record_order_attempt(self.strategy_id, ev["slug"], selected_token, sig.side, "BUY", "FAK", float(px), float(size), stake, status, resp, err, signal_payload, self.cfg)
        await self.writer.log_strategy_event(self.strategy_id, f"BTC5M PREOPEN BUY {sig.side} {ev['slug']} px={px} stake=${stake:.2f} seconds_since_open={now_ts-start_ts} status={status}{(' error='+err[:200]) if err else ''}", "WARN")
        if status in {"filled", "submitted"}:
            await self.writer.upsert_position(self.strategy_id, ev["slug"], sig.side, float(size), float(px), float(px), 0.0)

    async def run(self):
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
            await self.writer.log_strategy_event(self.strategy_id, "BTC5M pre-open/open-entry live runner loop started", "INFO")
            while not self.stop.is_set():
                try:
                    await self.maybe_trade_once(client)
                except Exception as e:
                    await self.writer.log_strategy_event(self.strategy_id, f"Runner error: {repr(e)[:500]}", "ERROR")
                    await asyncio.sleep(2)
                await asyncio.sleep(float(self.cfg.get("poll_seconds", 0.5)))


def default_config(strategy_id: str, wallet_id: str) -> tuple[str, str, dict[str, Any]]:
    common = {
        "wallet_id": wallet_id,
        "wallet_name": f"Prism {wallet_id.replace('prism','')}",
        "signature_type": int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "3")),
        "wallet_proxy": os.environ.get("POLYMARKET_PROXY_ADDRESS"),
        "proxy_address": os.environ.get("POLYMARKET_PROXY_ADDRESS"),
        "order_size_usd": 1.0,
        "max_order_size": 1.0,
        "daily_order_limit_usd": 200.0,
        "daily_spend_limit_usd": 200.0,
        "max_orders_per_market": 1,
        "entry_max_attempts_per_market": 30,
        "open_entry_window_seconds": 60,
        "max_source_data_age_seconds": 5,
        "min_ask_depth_usd": 1.0,
        "max_entry_ask": 0.98,
        "poll_seconds": 0.5,
        "dashboard_enabled": True,
        "strategy_family": "btc5m_preopen_open_entry",
        "model_version": MODEL_VERSION,
        "data_source_policy": "Polymarket labels backtested from SII-WANGZJ/Polymarket_data; Binance candles supplemental for BTC signal input.",
    }
    if strategy_id == "live_btc5m_preopen_bb_break_prism6":
        cfg = {
            **common,
            "strategy_name": "BTC 5m Pre-open BB breakout both-side",
            "strategy_short_name": "BB f5 l60 n10 k2.5 breakout both",
            "preopen_strategy_type": "bb_break_both",
            "signal_lookback_seconds": 60,
            "side_filter": "both",
            "bb_breakout": {"frame": 5, "lookback": 60, "n": 10, "k": 2.5, "fade": False},
            "supertrend": None,
            "exact_backtest_variant": "original_2_bb_break_f5_l60_n10_k2.5_breakout_both",
        }
        return "Prism 6 · BTC 5m Pre-open BB breakout", "btc5m_preopen_bb_break", cfg
    if strategy_id == "live_btc5m_preopen_supertrend_prism7":
        cfg = {
            **common,
            "strategy_name": "BTC 5m Pre-open SuperTrend both-side",
            "strategy_short_name": "ST f30 l180 n5 m3 both",
            "preopen_strategy_type": "supertrend_both",
            "signal_lookback_seconds": 180,
            "side_filter": "both",
            "supertrend": {"frame": 30, "lookback": 180, "n": 5, "mult": 3},
            "bb_breakout": None,
            "exact_backtest_variant": "original_3_supertrend_f30_l180_n5_m3_both",
        }
        return "Prism 7 · BTC 5m Pre-open SuperTrend", "btc5m_preopen_supertrend", cfg
    raise ValueError(f"unknown strategy_id {strategy_id!r}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy-id", required=True)
    ap.add_argument("--wallet-id", required=True)
    args = ap.parse_args()
    for f in ["/home/administrator/projects/polybot/.env", "/home/administrator/projects/polybot/.env.live", "/home/administrator/projects/polybot-dash/.env.local"]:
        if Path(f).exists():
            load_dotenv(f, override=True)
    sec = wallet_secret(args.wallet_id)
    if sec is None:
        raise RuntimeError(f"wallet {args.wallet_id!r} not found in encrypted registry")
    for key, value in sec.values.items():
        if key.startswith("POLYMARKET_") or key.startswith("POLY_") or key in {"PROXY_ADDRESS"}:
            os.environ[key] = value
    os.environ["POLYBOT_WALLET_ID"] = args.wallet_id
    os.environ["POLYMARKET_WALLET_ID"] = args.wallet_id
    name, kind, defaults = default_config(args.strategy_id, args.wallet_id)
    dsn = os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
    writer = PolybotWriter(dsn)
    await writer.connect()
    cfg = await writer.get_strategy_config(args.strategy_id)
    merged = {**defaults, **cfg}
    # Force exact safety/live sizing requested by user; dashboard edits must not drift these core params.
    merged.update({
        "wallet_id": args.wallet_id,
        "wallet_name": defaults["wallet_name"],
        "wallet_proxy": os.environ.get("POLYMARKET_PROXY_ADDRESS"),
        "proxy_address": os.environ.get("POLYMARKET_PROXY_ADDRESS"),
        "signature_type": int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "3")),
        "order_size_usd": 1.0,
        "max_order_size": 1.0,
        "daily_order_limit_usd": 200.0,
        "daily_spend_limit_usd": 200.0,
    })
    if args.strategy_id in {"live_btc5m_preopen_bb_break_prism6", "live_btc5m_preopen_supertrend_prism7"}:
        merged.update(defaults)
    await writer.register_strategy(args.strategy_id, name, kind, "BTC Up/Down 5m", merged)
    async with writer._pool.acquire() as con:
        await con.execute(
            """
            UPDATE strategies
            SET name=$2, kind=$3, market='BTC Up/Down 5m', mode='live', config=$4::jsonb, updated_at=now()
            WHERE id=$1
            """,
            args.strategy_id,
            name,
            kind,
            json.dumps(merged),
        )
    await writer.set_strategy_status(args.strategy_id, "running")
    await writer.snapshot_equity(args.strategy_id, STARTING_EQUITY)
    await writer.log_strategy_event(args.strategy_id, f"BTC5M pre-open runner starting wallet={args.wallet_id} order=$1.00 daily_limit=$200 exact_variant={merged.get('exact_backtest_variant')}", "INFO")
    ex = PolymarketExecutionClient()
    runner = Btc5mPreopenRunner(args.strategy_id, writer, ex, merged)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, runner.stop.set)
        except NotImplementedError:
            pass
    try:
        await runner.run()
    finally:
        await runner.update_heartbeat(None)
        await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
