#!/usr/bin/env python3
"""Live BTC 5m Up/Down Ichimoku high-confidence late trend runner for Prism 4.

Research candidate implemented exactly from the user's provided backtest result:
- family: ichimoku
- 10s candle frame, 1200s lookback
- Tenkan=7, Kijun=30, SpanB=42
- UP if close > cloud_high and Tenkan > Kijun
- DOWN if close < cloud_low and Tenkan < Kijun
- enter 45s before close
- price cap 0.95
- dashboard-controlled $1 order size by default

Research-only source metrics supplied by user:
Trades 414, Wins 389, Losses 25, Hit rate 93.96%, Avg entry cost 0.8640,
Net PnL +31.29, ROI +8.75%, Test trades 186, Test ROI +10.10%.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_UP
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.persistence.writer import PolybotWriter
from polybot.security.wallet_registry import wallet_secret
from polybot.live.momentum_5m_runner import (
    fetch_binance_recent_candles,
    resolve_5m_event,
    rest_book_full,
)

STARTING_EQUITY = 10_000.0
MODEL_VERSION = "btc5m_ichimoku_high_conf_late_trend_v1"


def D(x: Any) -> Decimal:
    return Decimal(str(x))


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
    if (resp or {}).get("success") is True and raw in {"matched", "success", ""}:
        return "filled"
    if (resp or {}).get("success") is False:
        return "rejected"
    return raw or "submitted"


def opposite_btc5m_side(side: str) -> str:
    raw = str(side or "").upper()
    if raw == "UP":
        return "DOWN"
    if raw == "DOWN":
        return "UP"
    return raw


@dataclass(frozen=True)
class IchimokuSignal:
    side: str
    score: float
    close: float
    tenkan: float
    kijun: float
    spanb: float
    cloud_high: float
    cloud_low: float
    reason: str
    bars: int


def aggregate_frame(candles: list[Any], end_ts: int, lookback_s: int = 1200, frame_s: int = 10) -> list[dict[str, float]]:
    start = int(end_ts) - int(lookback_s) + 1
    buckets: dict[int, dict[str, float]] = {}
    for c in candles:
        ts = int(getattr(c, "ts", 0))
        if ts < start or ts > end_ts:
            continue
        close = float(getattr(c, "close", 0.0))
        if close <= 0:
            continue
        b = (ts // frame_s) * frame_s
        row = buckets.setdefault(
            b,
            {"ts": float(b), "open": float(getattr(c, "open", close) or close), "high": close, "low": close, "close": close, "volume": 0.0, "taker_buy_volume": 0.0},
        )
        row["high"] = max(row["high"], float(getattr(c, "high", close) or close))
        row["low"] = min(row["low"], float(getattr(c, "low", close) or close))
        row["close"] = close
        row["volume"] += float(getattr(c, "volume", 0.0) or 0.0)
        row["taker_buy_volume"] += float(getattr(c, "taker_buy_volume", 0.0) or 0.0)
    return [buckets[k] for k in sorted(buckets)]


def midpoint_high_low(bars: list[dict[str, float]], n: int) -> float | None:
    if len(bars) < n or n <= 0:
        return None
    window = bars[-n:]
    return (max(float(x["high"]) for x in window) + min(float(x["low"]) for x in window)) / 2.0


def ichimoku_signal(candles: list[Any], now_ts: int, cfg: dict[str, Any]) -> IchimokuSignal | None:
    frame = int(cfg.get("frame", cfg.get("ichimoku_frame_seconds", 10)))
    lookback = int(cfg.get("lookback", cfg.get("ichimoku_lookback_seconds", 1200)))
    tenkan_n = int(cfg.get("tenkan", 7))
    kijun_n = int(cfg.get("kijun", 30))
    spanb_n = int(cfg.get("spanb", 42))
    bars = aggregate_frame(candles, now_ts, lookback, frame)
    if len(bars) < max(tenkan_n, kijun_n, spanb_n):
        return None
    tenkan = midpoint_high_low(bars, tenkan_n)
    kijun = midpoint_high_low(bars, kijun_n)
    spanb = midpoint_high_low(bars, spanb_n)
    if tenkan is None or kijun is None or spanb is None:
        return None
    spana = (tenkan + kijun) / 2.0
    cloud_high = max(spana, spanb)
    cloud_low = min(spana, spanb)
    close = float(bars[-1]["close"])
    # Use a normalized cloud-distance score for dashboards/logs; signal logic itself is exact.
    denom = max(abs(close), 1e-12)
    if close > cloud_high and tenkan > kijun:
        score = ((close - cloud_high) + (tenkan - kijun)) / denom
        return IchimokuSignal("UP", score, close, tenkan, kijun, spanb, cloud_high, cloud_low, "ichimoku_cloud_breakout_up", len(bars))
    if close < cloud_low and tenkan < kijun:
        score = -(((cloud_low - close) + (kijun - tenkan)) / denom)
        return IchimokuSignal("DOWN", score, close, tenkan, kijun, spanb, cloud_high, cloud_low, "ichimoku_cloud_breakdown_down", len(bars))
    return None


def book_sides(book: tuple[Any, Any, list, list] | None) -> tuple[float | None, float | None, list, list]:
    if not book:
        return None, None, [], []
    bid, ask, bids, asks = book
    return float(bid), float(ask), bids, asks


class Btc5mIchimokuRunner:
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

    async def already_traded_market(self, slug: str) -> bool:
        val = await self.db_fetchval(
            "select count(*) from order_attempts where strategy_id=$1 and market_slug=$2 and side='BUY' and status in ('filled','submitted','rejected')",
            self.strategy_id,
            slug,
        )
        return int(val or 0) > 0

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
                    "last_market_end_ts": int(ev["end_dt"].timestamp()) if ev and ev.get("end_dt") else None,
                    "last_signal": signal_payload,
                    "last_daily_buy_usd": round(await self.daily_used(), 6),
                    "last_wallet_balance": self.last_wallet_balance,
                    "data_mode": "binance_1s_plus_clob_rest_books",
                    "implementation_suitability": MODEL_VERSION,
                }
            ),
        )

    async def pick_entry_market(self, client: httpx.AsyncClient) -> dict[str, Any] | None:
        now_ts = int(time.time())
        current_start = now_ts - (now_ts % 300)
        current_remaining = current_start + 300 - now_ts
        min_remaining = int(self.cfg.get("min_seconds_before_close", 2))
        if current_remaining >= min_remaining:
            ev = await resolve_5m_event(client, f"btc-updown-5m-{current_start}")
            if ev:
                return ev
        return await resolve_5m_event(client, f"btc-updown-5m-{current_start + 300}")

    async def maybe_trade_once(self, client: httpx.AsyncClient):
        if not await self.refresh_cfg():
            await asyncio.sleep(2)
            return
        ev = await self.pick_entry_market(client)
        if not ev or not ev.get("end_dt"):
            await self.update_heartbeat(None)
            await asyncio.sleep(1)
            return
        now_ts = int(time.time())
        end_ts = int(ev["end_dt"].timestamp())
        secs_remaining = end_ts - now_ts
        entry_before = int(self.cfg.get("entry_before", self.cfg.get("entry_seconds_before_close", 45)))
        min_remaining = int(self.cfg.get("min_seconds_before_close", 2))
        lookback = int(self.cfg.get("lookback", self.cfg.get("ichimoku_lookback_seconds", 1200)))
        candles = await fetch_binance_recent_candles(client, now_ts - lookback - 30, now_ts)
        up_book = await rest_book_full(client, ev["up_token"])
        down_book = await rest_book_full(client, ev["down_token"])
        up_bid, up_ask, up_bids, up_asks = book_sides(up_book)
        down_bid, down_ask, down_bids, down_asks = book_sides(down_book)
        sig = ichimoku_signal(candles, now_ts, self.cfg)
        signal_payload: dict[str, Any] = asdict(sig) if sig else {"side": "SKIP", "reason": "no_ichimoku_signal"}
        signal_payload.update({"secs_remaining": secs_remaining, "model_version": MODEL_VERSION})
        await self.writer.record_market_observation(
            self.strategy_id,
            ev["slug"],
            ev.get("title") or ev["slug"],
            int(ev.get("start_ts") or 0),
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
            {
                "latest_ts": int(getattr(candles[-1], "ts", now_ts)) if candles else None,
                "latest_close": float(getattr(candles[-1], "close", 0.0)) if candles else None,
                "candles": [getattr(c, "__dict__", {}) for c in candles[-150:]],
            },
            signal_payload,
            self.cfg,
            {"daily_used": await self.daily_used(), "secs_remaining": secs_remaining},
        )
        await self.writer.upsert_books([
            {"strategy_id": self.strategy_id, "token": ev["up_token"], "label": "UP", "bids": up_bids, "asks": up_asks, "best_bid": up_bid or 0, "best_ask": up_ask or 0},
            {"strategy_id": self.strategy_id, "token": ev["down_token"], "label": "DOWN", "bids": down_bids, "asks": down_asks, "best_bid": down_bid or 0, "best_ask": down_ask or 0},
        ])
        await self.update_heartbeat(ev, signal_payload)
        if sig is None:
            return
        if not (min_remaining <= secs_remaining <= entry_before):
            return
        if await self.already_traded_market(ev["slug"]):
            return
        stake = float(self.cfg.get("order_size_usd", 1.0) or 1.0)
        daily_limit = float(self.cfg.get("daily_order_limit_usd", self.cfg.get("daily_spend_limit_usd", 200)) or 200)
        daily = await self.daily_used()
        if daily + stake > daily_limit:
            await self.writer.log_strategy_event(self.strategy_id, f"Daily cap reached: used=${daily:.2f}, stake=${stake:.2f}, limit=${daily_limit:.2f}", "WARN")
            return
        reverse_mode = bool(self.cfg.get("reverse_mode", self.cfg.get("reverse_signal", False)))
        original_side = sig.side
        trade_side = opposite_btc5m_side(original_side) if reverse_mode else original_side
        selected_token = ev["up_token"] if trade_side == "UP" else ev["down_token"]
        selected_ask = up_ask if trade_side == "UP" else down_ask
        selected_asks = up_asks if trade_side == "UP" else down_asks
        if selected_ask is None:
            return
        price_cap = float(self.cfg.get("price_cap", 0.95))
        if selected_ask > price_cap:
            return
        min_depth = float(self.cfg.get("min_ask_depth_usd", 1.0) or 1.0)
        depth = sum(float(x["price"]) * float(x["size"]) for x in selected_asks if float(x["price"]) <= selected_ask + 0.02)
        if depth < min_depth:
            return
        tick = str(max(D(ev.get("tick_size") or "0.01"), D("0.01")))
        px = D(selected_ask).quantize(D(tick), rounding=ROUND_UP)
        size = (D(stake) / px).quantize(D("0.0001"))
        signal_payload.update({
            "original_side": original_side,
            "effective_side": trade_side,
            "reverse_mode": reverse_mode,
            "selected_token": selected_token,
            "selected_ask": float(px),
            "ask_depth_usd_2c": depth,
        })
        try:
            resp = self.ex.submit(PolyOrder(token_id=selected_token, side="BUY", price=px, size=size, order_type="FAK", use_limit_order=False, tick_size=tick, neg_risk=bool(ev.get("neg_risk"))))
            status, err = status_from(resp), None
        except Exception as e:
            resp, status, err = {"error": repr(e)}, "rejected", repr(e)
        await self.writer.record_order_attempt(self.strategy_id, ev["slug"], selected_token, trade_side, "BUY", "FAK", float(px), float(size), stake, status, resp, err, signal_payload, self.cfg)
        mode_note = f" reverse={original_side}->{trade_side}" if reverse_mode else ""
        await self.writer.log_strategy_event(self.strategy_id, f"BTC5M Ichimoku BUY {trade_side}{mode_note} {ev['slug']} px={px} stake=${stake:.2f} secs={secs_remaining} close={sig.close:.2f} tenkan={sig.tenkan:.2f} kijun={sig.kijun:.2f} cloud=[{sig.cloud_low:.2f},{sig.cloud_high:.2f}] status={status}{(' error='+err[:200]) if err else ''}", "WARN")
        if status in {"filled", "submitted"}:
            await self.writer.upsert_position(self.strategy_id, ev["slug"], trade_side, float(size), float(px), float(px), 0.0)

    async def run(self):
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
            while not self.stop.is_set():
                try:
                    await self.maybe_trade_once(client)
                except Exception as e:
                    await self.writer.log_strategy_event(self.strategy_id, f"Runner error: {repr(e)[:500]}", "ERROR")
                    await asyncio.sleep(2)
                await asyncio.sleep(float(self.cfg.get("poll_seconds", 1.0)))


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy-id", default="live_btc5m_ichimoku_prism4")
    ap.add_argument("--wallet-id", default="prism4")
    args = ap.parse_args()
    for f in ["/home/administrator/projects/polybot/.env", "/home/administrator/projects/polybot/.env.live", "/home/administrator/projects/polybot-dash/.env.local"]:
        if f and Path(f).exists():
            load_dotenv(f, override=True)
    sec = wallet_secret(args.wallet_id)
    if sec is None:
        raise RuntimeError(f"wallet {args.wallet_id!r} not found in encrypted registry")
    # Force the requested wallet. Existing .env may contain the default wallet;
    # using setdefault here would silently trade the wrong account.
    for key, value in sec.values.items():
        if key.startswith("POLYMARKET_") or key.startswith("POLY_") or key in {"PROXY_ADDRESS"}:
            os.environ[key] = value
    os.environ["POLYBOT_WALLET_ID"] = args.wallet_id
    os.environ["POLYMARKET_WALLET_ID"] = args.wallet_id
    dsn = os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
    writer = PolybotWriter(dsn)
    await writer.connect()
    cfg = await writer.get_strategy_config(args.strategy_id)
    defaults = {
        "wallet_name": "Prism 4",
        "wallet_id": args.wallet_id,
        "wallet_proxy": os.environ.get("POLYMARKET_PROXY_ADDRESS"),
        "proxy_address": os.environ.get("POLYMARKET_PROXY_ADDRESS"),
        "signature_type": int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "3")),
        "order_size_usd": 1.0,
        "max_order_size": 1.0,
        "max_position_size": 1.0,
        "max_orders_per_market": 1,
        "entry_max_attempts_per_market": 1,
        "daily_order_limit_usd": 200.0,
        "daily_spend_limit_usd": 200.0,
        "entry_before": 45,
        "entry_seconds_before_close": 45,
        "min_seconds_before_close": 2,
        "price_cap": 0.95,
        "min_ask_depth_usd": 1.0,
        "frame": 10,
        "ichimoku_frame_seconds": 10,
        "lookback": 1200,
        "ichimoku_lookback_seconds": 1200,
        "tenkan": 7,
        "kijun": 30,
        "spanb": 42,
        "poll_seconds": 1.0,
        "dashboard_enabled": True,
        "strategy_family": "ichimoku",
        "strategy_name": "Ichimoku high-confidence late trend",
        "model_version": MODEL_VERSION,
        "reverse_mode": False,
        "research_metrics": {
            "trades": 414,
            "wins": 389,
            "losses": 25,
            "hit_rate": 0.9396,
            "avg_entry_cost": 0.8640,
            "net_pnl": 31.29,
            "roi_on_cost": 0.0875,
            "max_drawdown": 0.57,
            "train_pnl": 15.42,
            "train_roi": 0.0769,
            "test_trades": 186,
            "test_pnl": 15.88,
            "test_roi": 0.1010,
        },
    }
    merged = {**defaults, **cfg}
    # Dashboard Order size ($) is the single source of truth; keep legacy alias synced.
    merged["max_order_size"] = float(merged.get("order_size_usd", defaults["order_size_usd"]) or defaults["order_size_usd"])
    await writer.register_strategy(args.strategy_id, "Prism 4 · BTC 5m Ichimoku High-Confidence Late Trend", "btc5m_ichimoku_late_trend", "BTC Up/Down 5m", merged)
    async with writer._pool.acquire() as con:
        await con.execute(
            """
            UPDATE strategies
            SET mode='live', config=config || $2::jsonb, updated_at=now()
            WHERE id=$1
            """,
            args.strategy_id,
            json.dumps(merged),
        )
    await writer.set_strategy_status(args.strategy_id, "running")
    await writer.snapshot_equity(args.strategy_id, STARTING_EQUITY)
    ex = PolymarketExecutionClient()
    runner = Btc5mIchimokuRunner(args.strategy_id, writer, ex, merged)
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
