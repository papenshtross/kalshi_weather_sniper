#!/usr/bin/env python3
"""Live BTC 5m Up/Down ADX/DMI trend runner for Prism 3.

Top backtest candidate implemented:
- 30s candle frame, 600s lookback, DMI/ADX n=10
- Signal UP when ADX > 10 and +DI > -DI + 5; DOWN inverse
- Evaluate inside final 15 seconds of each BTC 5m market
- Uses dashboard-configured FAK market-buy notional, $200 rolling 24h daily cap, one order per market
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_UP
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.persistence.writer import PolybotWriter
from polybot.live.momentum_5m_runner import (
    fetch_binance_recent_candles,
    resolve_5m_event,
    rest_book_full,
)

STARTING_EQUITY = 10_000.0


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
    if (resp or {}).get("success") is False:
        return "rejected"
    return raw or "submitted"


@dataclass(frozen=True)
class AdxDmiSignal:
    side: str
    score: float
    pdi: float
    mdi: float
    adx: float
    gap: float
    reason: str


def aggregate_30s(candles: list[Any], end_ts: int, lookback_s: int = 600, frame_s: int = 30) -> list[dict[str, float]]:
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
            {"ts": float(b), "open": close, "high": close, "low": close, "close": close, "volume": 0.0, "taker_buy_volume": 0.0},
        )
        if row["volume"] == 0 and row["open"] == row["close"]:
            row["open"] = float(getattr(c, "open", close))
        row["high"] = max(row["high"], float(getattr(c, "high", close)))
        row["low"] = min(row["low"], float(getattr(c, "low", close)))
        row["close"] = close
        row["volume"] += float(getattr(c, "volume", 0.0) or 0.0)
        row["taker_buy_volume"] += float(getattr(c, "taker_buy_volume", 0.0) or 0.0)
    return [buckets[k] for k in sorted(buckets)]


def dmi_adx(bars: list[dict[str, float]], n: int = 10) -> tuple[float, float, float] | None:
    if len(bars) < n + 2:
        return None
    plus: list[float] = []
    minus: list[float] = []
    trs: list[float] = []
    for i in range(1, len(bars)):
        up = bars[i]["high"] - bars[i - 1]["high"]
        dn = bars[i - 1]["low"] - bars[i]["low"]
        plus.append(up if up > dn and up > 0 else 0.0)
        minus.append(dn if dn > up and dn > 0 else 0.0)
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    tr = sum(trs[-n:])
    if tr <= 1e-12:
        return None
    pdi = 100.0 * sum(plus[-n:]) / tr
    mdi = 100.0 * sum(minus[-n:]) / tr
    dxs: list[float] = []
    for j in range(max(n, len(trs) - n + 1), len(trs) + 1):
        trj = sum(trs[j - n:j])
        if trj <= 1e-12:
            continue
        pd = 100.0 * sum(plus[j - n:j]) / trj
        md = 100.0 * sum(minus[j - n:j]) / trj
        dxs.append(100.0 * abs(pd - md) / (pd + md + 1e-12))
    if not dxs:
        return None
    return pdi, mdi, sum(dxs) / len(dxs)


def adx_dmi_signal(candles: list[Any], now_ts: int, cfg: dict[str, Any]) -> AdxDmiSignal | None:
    frame = int(cfg.get("adx_frame_seconds", 30))
    lookback = int(cfg.get("adx_lookback_seconds", 600))
    n = int(cfg.get("adx_n", 10))
    threshold = float(cfg.get("adx_threshold", 10))
    gap_threshold = float(cfg.get("dmi_gap_threshold", 5))
    bars = aggregate_30s(candles, now_ts, lookback, frame)
    vals = dmi_adx(bars, n)
    if vals is None:
        return None
    pdi, mdi, adx = vals
    gap = abs(pdi - mdi)
    if adx > threshold and pdi > mdi + gap_threshold:
        return AdxDmiSignal("UP", adx + gap, pdi, mdi, adx, pdi - mdi, "adx_dmi_up")
    if adx > threshold and mdi > pdi + gap_threshold:
        return AdxDmiSignal("DOWN", -(adx + gap), pdi, mdi, adx, mdi - pdi, "adx_dmi_down")
    return None


def book_sides(book: tuple[Any, Any, list, list] | None) -> tuple[float | None, float | None, list, list]:
    if not book:
        return None, None, [], []
    bid, ask, bids, asks = book
    return float(bid), float(ask), bids, asks


class Btc5mAdxRunner:
    def __init__(self, strategy_id: str, writer: PolybotWriter, ex: PolymarketExecutionClient, cfg: dict[str, Any]):
        self.strategy_id = strategy_id
        self.writer = writer
        self.ex = ex
        self.cfg = dict(cfg)
        self.stop = asyncio.Event()
        self.last_slug: str | None = None
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
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", self.cfg.get("signature_type", 1) or 1)))
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
                    "implementation_suitability": "live_btc5m_adx_dmi_top_backtest_candidate",
                }
            ),
        )

    async def pick_entry_market(self, client: httpx.AsyncClient) -> dict[str, Any] | None:
        """Pick current BTC 5m market through the final entry seconds.

        The shared momentum picker intentionally switches away when <15s remains;
        this ADX strategy is designed to enter in that final <=15s window, so we
        resolve the deterministic current slug directly until min_seconds_before_close.
        """
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
        entry_before = int(self.cfg.get("entry_seconds_before_close", 15))
        min_remaining = int(self.cfg.get("min_seconds_before_close", 2))
        # Always record observations, but only trade inside the intended entry window.
        candles = await fetch_binance_recent_candles(client, max(ev["start_ts"] - 600, now_ts - 700), now_ts)
        up_book = await rest_book_full(client, ev["up_token"])
        down_book = await rest_book_full(client, ev["down_token"])
        up_bid, up_ask, up_bids, up_asks = book_sides(up_book)
        down_bid, down_ask, down_bids, down_asks = book_sides(down_book)
        sig = adx_dmi_signal(candles, now_ts, self.cfg)
        signal_payload = asdict(sig) if sig else {"side": "SKIP", "reason": "no_adx_dmi_signal"}
        signal_payload.update({"secs_remaining": secs_remaining, "model_version": "btc5m_adx_dmi_v1_top_candidate"})
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
                "candles": [getattr(c, "__dict__", {}) for c in candles[-120:]],
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
        selected_token = ev["up_token"] if sig.side == "UP" else ev["down_token"]
        selected_ask = up_ask if sig.side == "UP" else down_ask
        selected_asks = up_asks if sig.side == "UP" else down_asks
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
        signal_payload.update({"selected_token": selected_token, "selected_ask": float(px), "ask_depth_usd_2c": depth})
        try:
            resp = self.ex.submit(PolyOrder(token_id=selected_token, side="BUY", price=px, size=size, order_type="FAK", use_limit_order=False, tick_size=tick, neg_risk=bool(ev.get("neg_risk"))))
            status, err = status_from(resp), None
        except Exception as e:
            resp, status, err = {"error": repr(e)}, "rejected", repr(e)
        await self.writer.record_order_attempt(self.strategy_id, ev["slug"], selected_token, sig.side, "BUY", "FAK", float(px), float(size), stake, status, resp, err, signal_payload, self.cfg)
        await self.writer.log_strategy_event(self.strategy_id, f"BTC5M ADX/DMI BUY {sig.side} {ev['slug']} px={px} stake=${stake:.2f} secs={secs_remaining} ADX={sig.adx:.2f} +DI={sig.pdi:.2f} -DI={sig.mdi:.2f} status={status}{(' error='+err[:200]) if err else ''}", "WARN")
        if status in {"filled", "submitted"}:
            # Settlement is binary at expiry; record a dashboard position marker until reconciliation catches up.
            await self.writer.upsert_position(self.strategy_id, ev["slug"], sig.side, float(size), float(px), float(px), 0.0)

    async def run(self):
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
            await self.writer.log_strategy_event(self.strategy_id, "BTC5M ADX/DMI live runner loop started", "INFO")
            while not self.stop.is_set():
                try:
                    await self.maybe_trade_once(client)
                except Exception as e:
                    await self.writer.log_strategy_event(self.strategy_id, f"Runner error: {repr(e)[:500]}", "ERROR")
                    await asyncio.sleep(2)
                await asyncio.sleep(float(self.cfg.get("poll_seconds", 1.0)))


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy-id", default="live_btc5m_adx_dmi_prism3")
    ap.add_argument("--wallet-env", default="/home/administrator/projects/polybot/config/wallets/prism3.env")
    args = ap.parse_args()
    for f in ["/home/administrator/projects/polybot/.env", "/home/administrator/projects/polybot/.env.live", "/home/administrator/projects/polybot-dash/.env.local", args.wallet_env]:
        if f and Path(f).exists():
            load_dotenv(f, override=True)
    dsn = os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
    writer = PolybotWriter(dsn)
    await writer.connect()
    cfg = await writer.get_strategy_config(args.strategy_id)
    defaults = {
        "wallet_name": "Prism 3",
        "order_size_usd": 1.0,
        "max_position_size": 1.0,
        "max_orders_per_market": 1,
        "entry_max_attempts_per_market": 1,
        "daily_order_limit_usd": 200.0,
        "daily_spend_limit_usd": 200.0,
        "entry_seconds_before_close": 15,
        "min_seconds_before_close": 2,
        "price_cap": 0.95,
        "min_ask_depth_usd": 1.0,
        "adx_frame_seconds": 30,
        "adx_lookback_seconds": 600,
        "adx_n": 10,
        "adx_threshold": 10,
        "dmi_gap_threshold": 5,
        "poll_seconds": 1.0,
        "dashboard_enabled": True,
        "strategy_family": "btc5m_adx_dmi_top_candidate",
    }
    merged = {**defaults, **cfg}
    # The dashboard's Order size ($) setting is the single source of truth for
    # this strategy. max_order_size is only a legacy dashboard alias; keep it in
    # sync instead of letting an old hardcoded $1 value survive in config.
    merged["max_order_size"] = float(merged.get("order_size_usd", defaults["order_size_usd"]) or defaults["order_size_usd"])
    await writer.register_strategy(args.strategy_id, "Prism 3 · BTC 5m ADX/DMI Trend", "btc5m_trend_adx", "BTC Up/Down 5m", merged)
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
    await writer.log_strategy_event(args.strategy_id, f"BTC5M ADX/DMI runner starting wallet=Prism 3 order=${float(merged.get('order_size_usd', 1.0) or 1.0):.2f} daily_limit=${float(merged.get('daily_order_limit_usd', 200.0) or 200.0):.2f}", "INFO")
    ex = PolymarketExecutionClient()
    runner = Btc5mAdxRunner(args.strategy_id, writer, ex, merged)
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
