#!/usr/bin/env python3
"""Live BTC 5m combined ADX/DMI + EMA/Ichimoku late trend runner for Prism 5.

Candidate formula requested:
- Primary side = ADX/DMI direction
- Confirm if EMA slope agrees OR Ichimoku cloud agrees
- Reject if ask > price cap
- Reject if market/source data stale
- Reject if no visible executable ask depth
- Prefer entry window 10-20 seconds before close
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
from polybot.live.momentum_5m_runner import fetch_binance_recent_candles, resolve_5m_event, rest_book_full
from scripts.live_btc5m_adx_trend_runner import D, aggregate_30s, dmi_adx, book_sides, status_from
from scripts.live_btc5m_ichimoku_prism4_runner import aggregate_frame, midpoint_high_low

STARTING_EQUITY = 10_000.0
MODEL_VERSION = "btc5m_adx_ema_ichimoku_late_confirm_v1"


@dataclass(frozen=True)
class SideSignal:
    side: str
    reason: str
    score: float
    details: dict[str, Any]


@dataclass(frozen=True)
class CombinedSignal:
    side: str
    score: float
    reason: str
    dmi: dict[str, Any]
    ema: dict[str, Any] | None
    ichimoku: dict[str, Any] | None
    confirmations: list[str]


def ema(values: list[float], n: int) -> list[float]:
    if not values or n <= 0:
        return []
    alpha = 2.0 / (n + 1.0)
    out = [float(values[0])]
    for v in values[1:]:
        out.append(alpha * float(v) + (1.0 - alpha) * out[-1])
    return out


def atr(bars: list[dict[str, float]], n: int = 14) -> float | None:
    if len(bars) < 2:
        return None
    trs: list[float] = []
    for i in range(1, len(bars)):
        h, l, pc = float(bars[i]["high"]), float(bars[i]["low"]), float(bars[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return None
    return sum(trs[-n:]) / min(len(trs), n)


def dmi_side(candles: list[Any], now_ts: int, cfg: dict[str, Any]) -> SideSignal | None:
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
    if adx > threshold and pdi - mdi > gap_threshold:
        return SideSignal("UP", "adx_dmi_up", adx + (pdi - mdi), {"pdi": pdi, "mdi": mdi, "adx": adx, "gap": pdi - mdi, "bars": len(bars)})
    if adx > threshold and mdi - pdi > gap_threshold:
        return SideSignal("DOWN", "adx_dmi_down", -(adx + (mdi - pdi)), {"pdi": pdi, "mdi": mdi, "adx": adx, "gap": mdi - pdi, "bars": len(bars)})
    return None


def ema_side(candles: list[Any], now_ts: int, cfg: dict[str, Any]) -> SideSignal | None:
    frame = int(cfg.get("ema_frame_seconds", 10))
    lookback = int(cfg.get("ema_lookback_seconds", 600))
    fast_n = int(cfg.get("ema_fast_n", 8))
    slow_n = int(cfg.get("ema_slow_n", 20))
    slope_periods = int(cfg.get("ema_slope_periods", 2))
    slope_threshold = float(cfg.get("ema_slope_atr_threshold", 0.05))
    atr_n = int(cfg.get("atr_n", 14))
    bars = aggregate_frame(candles, now_ts, lookback, frame)
    if len(bars) < max(slow_n + slope_periods + 1, atr_n + 2):
        return None
    closes = [float(b["close"]) for b in bars]
    ema_fast = ema(closes, fast_n)
    ema_slow = ema(closes, slow_n)
    a = atr(bars, atr_n)
    if not a or a <= 1e-12 or len(ema_fast) <= slope_periods:
        return None
    slope_norm = (ema_fast[-1] - ema_fast[-1 - slope_periods]) / a
    details = {"ema_fast": ema_fast[-1], "ema_slow": ema_slow[-1], "slope_norm": slope_norm, "atr": a, "bars": len(bars)}
    if ema_fast[-1] > ema_slow[-1] and slope_norm > slope_threshold:
        return SideSignal("UP", "ema_slope_up", slope_norm, details)
    if ema_fast[-1] < ema_slow[-1] and slope_norm < -slope_threshold:
        return SideSignal("DOWN", "ema_slope_down", slope_norm, details)
    return None


def ichimoku_side(candles: list[Any], now_ts: int, cfg: dict[str, Any]) -> SideSignal | None:
    frame = int(cfg.get("ichimoku_frame_seconds", cfg.get("frame", 10)))
    lookback = int(cfg.get("ichimoku_lookback_seconds", cfg.get("lookback", 1200)))
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
    details = {"close": close, "tenkan": tenkan, "kijun": kijun, "spanb": spanb, "cloud_high": cloud_high, "cloud_low": cloud_low, "bars": len(bars)}
    denom = max(abs(close), 1e-12)
    if close > cloud_high and tenkan > kijun:
        return SideSignal("UP", "ichimoku_cloud_breakout_up", ((close - cloud_high) + (tenkan - kijun)) / denom, details)
    if close < cloud_low and tenkan < kijun:
        return SideSignal("DOWN", "ichimoku_cloud_breakdown_down", -(((cloud_low - close) + (kijun - tenkan)) / denom), details)
    return None


async def fetch_binance_recent_candles_full(client: httpx.AsyncClient, start_ts: int, end_ts: int):
    """Fetch Binance 1s candles in chunks so the latest candle is not lost to the 1000-row API limit."""
    rows: list[Any] = []
    cursor = int(start_ts)
    while cursor <= int(end_ts):
        chunk_end = min(int(end_ts), cursor + 899)
        part = await fetch_binance_recent_candles(client, cursor, chunk_end)
        rows.extend(part)
        cursor = chunk_end + 1
    dedup: dict[int, Any] = {int(getattr(r, "ts", 0)): r for r in rows}
    return [dedup[k] for k in sorted(dedup)]


def combined_signal(candles: list[Any], now_ts: int, cfg: dict[str, Any]) -> CombinedSignal | None:
    dmi = dmi_side(candles, now_ts, cfg)
    if dmi is None:
        return None
    em = ema_side(candles, now_ts, cfg)
    ichi = ichimoku_side(candles, now_ts, cfg)
    confirmations: list[str] = []
    if em and em.side == dmi.side:
        confirmations.append("EMA")
    if ichi and ichi.side == dmi.side:
        confirmations.append("Ichimoku")
    if not confirmations:
        return None
    score = float(dmi.score) + sum(abs(float(x.score)) for x in (em, ichi) if x and x.side == dmi.side)
    return CombinedSignal(
        side=dmi.side,
        score=score if dmi.side == "UP" else -abs(score),
        reason="adx_dmi_confirmed_by_" + "_".join(c.lower() for c in confirmations),
        dmi={"side": dmi.side, "reason": dmi.reason, **dmi.details},
        ema={"side": em.side, "reason": em.reason, **em.details} if em else None,
        ichimoku={"side": ichi.side, "reason": ichi.reason, **ichi.details} if ichi else None,
        confirmations=confirmations,
    )


class Btc5mCombinedTrendRunner:
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
        min_remaining = int(self.cfg.get("min_seconds_before_close", 10))
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
        min_remaining = int(self.cfg.get("min_seconds_before_close", 10))
        max_remaining = int(self.cfg.get("max_seconds_before_close", self.cfg.get("entry_seconds_before_close", 20)))
        lookback = max(int(self.cfg.get("ichimoku_lookback_seconds", 1200)), int(self.cfg.get("ema_lookback_seconds", 600)), int(self.cfg.get("adx_lookback_seconds", 600)))
        candles = await fetch_binance_recent_candles_full(client, now_ts - lookback - 60, now_ts)
        latest_ts = int(getattr(candles[-1], "ts", 0)) if candles else None
        source_age_s = max(0, now_ts - latest_ts) if latest_ts else None
        up_book = await rest_book_full(client, ev["up_token"])
        down_book = await rest_book_full(client, ev["down_token"])
        up_bid, up_ask, up_bids, up_asks = book_sides(up_book)
        down_bid, down_ask, down_bids, down_asks = book_sides(down_book)
        sig = combined_signal(candles, now_ts, self.cfg)
        signal_payload: dict[str, Any] = asdict(sig) if sig else {"side": "SKIP", "reason": "no_combined_signal"}
        signal_payload.update({"secs_remaining": secs_remaining, "model_version": MODEL_VERSION, "source_age_s": source_age_s})
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
                "latest_ts": latest_ts,
                "latest_close": float(getattr(candles[-1], "close", 0.0)) if candles else None,
                "candles": [getattr(c, "__dict__", {}) for c in candles[-180:]],
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
        if not (min_remaining <= secs_remaining <= max_remaining):
            return
        max_source_age = float(self.cfg.get("max_source_data_age_seconds", 5) or 5)
        if source_age_s is None or source_age_s > max_source_age:
            await self.writer.log_strategy_event(self.strategy_id, f"Skip stale Binance data: age={source_age_s}s max={max_source_age}s", "WARN")
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
        await self.writer.log_strategy_event(self.strategy_id, f"BTC5M COMBINED BUY {sig.side} {ev['slug']} px={px} stake=${stake:.2f} secs={secs_remaining} confirmations={','.join(sig.confirmations)} status={status}{(' error='+err[:200]) if err else ''}", "WARN")
        if status in {"filled", "submitted"}:
            await self.writer.upsert_position(self.strategy_id, ev["slug"], sig.side, float(size), float(px), float(px), 0.0)

    async def run(self):
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
            await self.writer.log_strategy_event(self.strategy_id, "BTC5M combined ADX/EMA/Ichimoku live runner loop started", "INFO")
            while not self.stop.is_set():
                try:
                    await self.maybe_trade_once(client)
                except Exception as e:
                    await self.writer.log_strategy_event(self.strategy_id, f"Runner error: {repr(e)[:500]}", "ERROR")
                    await asyncio.sleep(2)
                await asyncio.sleep(float(self.cfg.get("poll_seconds", 1.0)))


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy-id", default="live_btc5m_combined_trend_prism5")
    ap.add_argument("--wallet-id", default="prism5")
    args = ap.parse_args()
    for f in ["/home/administrator/projects/polybot/.env", "/home/administrator/projects/polybot/.env.live", "/home/administrator/projects/polybot-dash/.env.local"]:
        if f and Path(f).exists():
            load_dotenv(f, override=True)
    sec = wallet_secret(args.wallet_id)
    if sec is None:
        raise RuntimeError(f"wallet {args.wallet_id!r} not found in encrypted registry")
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
        "wallet_name": "Prism 5",
        "wallet_id": args.wallet_id,
        "wallet_proxy": os.environ.get("POLYMARKET_PROXY_ADDRESS"),
        "proxy_address": os.environ.get("POLYMARKET_PROXY_ADDRESS"),
        "signature_type": int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "3")),
        "order_size_usd": 1.0,
        "max_order_size": 1.0,
        "max_position_size": 1.0,
        "max_orders_per_market": 1,
        "entry_max_attempts_per_market": 1,
        "daily_order_limit_usd": 5.0,
        "daily_spend_limit_usd": 5.0,
        "min_seconds_before_close": 10,
        "max_seconds_before_close": 20,
        "entry_seconds_before_close": 20,
        "price_cap": 0.95,
        "min_ask_depth_usd": 1.0,
        "max_source_data_age_seconds": 5,
        "adx_frame_seconds": 30,
        "adx_lookback_seconds": 600,
        "adx_n": 10,
        "adx_threshold": 10,
        "dmi_gap_threshold": 5,
        "ema_frame_seconds": 10,
        "ema_lookback_seconds": 600,
        "ema_fast_n": 8,
        "ema_slow_n": 20,
        "ema_slope_periods": 2,
        "ema_slope_atr_threshold": 0.05,
        "atr_n": 14,
        "ichimoku_frame_seconds": 10,
        "ichimoku_lookback_seconds": 1200,
        "tenkan": 7,
        "kijun": 30,
        "spanb": 42,
        "poll_seconds": 1.0,
        "dashboard_enabled": True,
        "strategy_family": "btc5m_combined_adx_ema_ichimoku",
        "strategy_name": "ADX/DMI + EMA/Ichimoku late trend",
        "model_version": MODEL_VERSION,
    }
    merged = {**defaults, **cfg}
    merged["max_order_size"] = float(merged.get("order_size_usd", defaults["order_size_usd"]) or defaults["order_size_usd"])
    await writer.register_strategy(args.strategy_id, "Prism 5 · BTC 5m ADX/DMI + EMA/Ichimoku", "btc5m_combined_trend", "BTC Up/Down 5m", merged)
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
    await writer.log_strategy_event(args.strategy_id, f"BTC5M combined runner starting wallet=Prism 5 order=${float(merged.get('order_size_usd', 1.0) or 1.0):.2f} daily_limit=${float(merged.get('daily_order_limit_usd', 5.0) or 5.0):.2f}", "INFO")
    ex = PolymarketExecutionClient()
    runner = Btc5mCombinedTrendRunner(args.strategy_id, writer, ex, merged)
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
