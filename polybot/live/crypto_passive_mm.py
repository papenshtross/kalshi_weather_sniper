"""Passive fair-value market maker for Polymarket crypto Up/Down markets.

This runner is intentionally separate from ``arb_sniper``.  It does not wait for
YES+NO taker-arb opportunities.  Instead it keeps small post-only GTC BUY quotes
resting below model fair value on both outcomes.  When one side fills it resizes
the opposite quote to hedge the residual shares.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import httpx
import yaml
from loguru import logger

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.crypto.fair_price import CryptoFairPriceModel, FairPriceSnapshot
from polybot.live.arb_sniper import (
    Book,
    _merge_cfg,
    _safe_bool,
    _safe_float,
    clob_response_error,
    resolve_event_pairs,
    rest_books_full,
)
from polybot.persistence.writer import PolybotWriter

Leg = Literal["YES", "NO"]
CLOB_FILLED = {"MATCHED", "FILLED", "CONFIRMED", "COMPLETE", "COMPLETED"}
CLOB_CANCELLED = {"CANCELED", "CANCELLED", "EXPIRED"}
PASSIVE_MM_CONFIG_VERSION = 2
PASSIVE_MM_VERSION_KEYS = ("passive_mm_config_version", "config_version")
# These keys are source-of-truth in the checked-in live YAML unless the dashboard
# DB config explicitly carries the same passive_mm_config_version.  This prevents
# old dashboard rows from silently overriding newly reviewed live risk/sizing
# fixes after deploy/restart, while still allowing the dashboard to change status
# and non-critical presentation fields.
PASSIVE_MM_CRITICAL_FILE_KEYS = {
    "quote_notional_usd",
    "quote_size_shares",
    "max_quote_shares",
    "shares_max_per_market",
    "max_market_exposure_shares",
    "min_order_notional_usd",
    "min_quote_price",
    "max_quote_price",
    "quote_edge_cents",
    "hedge_edge_cents",
    "replace_threshold_cents",
    "size_replace_threshold_shares",
    "quote_update_interval_seconds",
    "min_seconds_to_expiry_for_entry",
    "dry_run",
    "fair_model_fallback_sigma",
    "fair_model_vol_floor",
    "fair_model_vol_cap",
    "fair_model_sample_seconds",
    "fair_model_ewma_lambda",
    "fair_model_winsor_sigma",
    "fair_model_latency_buffer_seconds",
    "fair_model_window_samples",
}


def _env_dsn() -> str | None:
    return os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")


def _now() -> float:
    return time.time()


def _round_price(px: float, tick: str) -> float:
    try:
        step = float(tick or "0.01")
    except Exception:
        step = 0.01
    step = step if step in {0.1, 0.01, 0.001, 0.0001} else 0.01
    return max(0.01, min(0.99, math.floor(float(px) / step + 1e-12) * step))


def _size_for_notional(notional: float, price: float, min_size: float = 0.0) -> float:
    if price <= 0:
        return 0.0
    # Base quote sizing helper for notional-targeted orders. Hedging is handled
    # separately because live BTC15m CLOB maker limits accepted 5-share orders
    # below $1 notional; hedges should work the imbalance down one 5-share clip.
    size = float(notional) / float(price)
    size = max(size, float(min_size or 0.0))
    size = math.ceil(size * 10_000 - 1e-12) / 10_000
    # Guard against binary/decimal rounding or CLOB-side truncation causing
    # "amount $0.999, min $1" rejects.
    while price * size + 1e-9 < float(notional):
        size = round(size + 0.0001, 4)
    return size


def _cfg_version(cfg: dict[str, Any] | None) -> int:
    if not isinstance(cfg, dict):
        return 0
    for key in PASSIVE_MM_VERSION_KEYS:
        try:
            return int(cfg.get(key) or 0)
        except Exception:
            continue
    return 0


def _merge_passive_cfg(file_cfg: dict[str, Any], db_cfg: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    """Merge dashboard config safely for the passive MM live runner.

    Dashboard rows are still accepted, but stale rows cannot override critical
    sizing/risk/model keys unless they advertise the same config version as the
    file.  The returned config is written back via register_strategy(), so a stale
    DB row self-heals on service start instead of repeating the same old-config
    failure after every deploy.
    """
    db_cfg = db_cfg or {}
    merged = _merge_cfg(file_cfg, db_cfg)
    file_version = _cfg_version(file_cfg) or PASSIVE_MM_CONFIG_VERSION
    db_version = _cfg_version(db_cfg)
    warnings: list[str] = []
    if db_cfg and db_version < file_version:
        overridden = []
        for key in sorted(PASSIVE_MM_CRITICAL_FILE_KEYS):
            if key in file_cfg and merged.get(key) != file_cfg.get(key):
                merged[key] = file_cfg[key]
                overridden.append(key)
        if overridden:
            warnings.append(
                f"stale DB config version {db_version or 'missing'} < file version {file_version}; "
                f"used file values for critical keys: {', '.join(overridden)}"
            )
    merged["passive_mm_config_version"] = file_version
    merged["config_version"] = file_version
    return merged, warnings


def _passive_cfg_issues(cfg: dict[str, Any], *, order_min_size: float = 5.0) -> list[str]:
    issues: list[str] = []
    try:
        min_size = max(0.0, float(order_min_size or cfg.get("order_min_size") or 5.0))
    except Exception:
        min_size = 5.0
    try:
        max_market = float(cfg.get("shares_max_per_market", cfg.get("max_market_exposure_shares", cfg.get("max_position_size", 10.0))) or 0.0)
    except Exception:
        max_market = 0.0
    if max_market > 0 and max_market / 2.0 + 1e-9 < min_size:
        issues.append(f"per-side cap {max_market / 2.0:.4f} is below exchange min size {min_size:.4f}")
    try:
        quote_size = float(cfg.get("quote_size_shares", 5.0) or 0.0)
        max_quote = float(cfg.get("max_quote_shares", quote_size) or 0.0)
    except Exception:
        quote_size = max_quote = 0.0
    if quote_size <= 0 or max_quote <= 0:
        issues.append("quote_size_shares/max_quote_shares must be positive")
    return issues


def _order_id(resp: dict[str, Any]) -> str | None:
    if not isinstance(resp, dict):
        return None
    for key in ("orderID", "order_id", "id"):
        val = resp.get(key)
        if val:
            return str(val)
    data = resp.get("data")
    if isinstance(data, dict):
        return _order_id(data)
    return None


def _status(raw: dict[str, Any]) -> str:
    for key in ("status", "state", "orderStatus"):
        val = raw.get(key) if isinstance(raw, dict) else None
        if val:
            return str(val).upper()
    return ""


def _matched_size(raw: dict[str, Any], fallback_full_size: float = 0.0) -> float:
    if not isinstance(raw, dict):
        return 0.0
    for key in ("matched_size", "matchedSize", "size_matched", "sizeMatched", "filled_size", "filledSize", "filled"):
        val = raw.get(key)
        try:
            f = float(val)
            if f > 0:
                return f
        except Exception:
            pass
    if _status(raw) in CLOB_FILLED:
        return float(fallback_full_size or 0.0)
    return 0.0


@dataclass
class RestingOrder:
    leg: Leg
    token: str
    order_id: str
    price: float
    size: float
    created_ts: float
    purpose: str = "base"
    filled_size: float = 0.0


class PassiveCryptoMMRunner:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.file_cfg = yaml.safe_load(config_path.read_text()) or {}
        self.strategy_id = str(self.file_cfg.get("id") or "crypto_passive_mm_btc_15m")
        self.name = str(self.file_cfg.get("name") or "Crypto_MM_v1 · Passive BTC 15m")
        self.writer = PolybotWriter(_env_dsn())
        self.exec_client: PolymarketExecutionClient | None = None
        self.stop = asyncio.Event()
        self.pair: dict[str, Any] | None = None
        self.books: dict[Leg, Book] = {"YES": Book(), "NO": Book()}
        self.open: dict[Leg, RestingOrder | None] = {"YES": None, "NO": None}
        self.filled: dict[Leg, float] = {"YES": 0.0, "NO": 0.0}
        self.fill_cost: dict[Leg, float] = {"YES": 0.0, "NO": 0.0}
        self.crypto_ref_prices: list[float] = []
        self.crypto_ref_price = 0.0
        self.crypto_start_price = 0.0
        self.crypto_start_ts = 0
        self.fair_model = CryptoFairPriceModel()
        self.fair_cache_key: tuple[int, int, int, int, int] | None = None
        self.fair_cache: FairPriceSnapshot | None = None
        self.last_quote_log_ts = 0.0
        self.fill_seq = int(time.time() * 1000) % 10_000_000

    async def _effective_cfg(self) -> tuple[dict[str, Any], list[str]]:
        return _merge_passive_cfg(self.file_cfg, await self.writer.get_strategy_config(self.strategy_id))

    async def setup(self) -> None:
        await self.writer.connect()
        cfg, cfg_warnings = await self._effective_cfg()
        await self.writer.register_strategy(
            self.strategy_id,
            self.name,
            str(cfg.get("kind") or "crypto_passive_mm"),
            str(cfg.get("market_slug") or "auto:crypto-updown:btc:15m"),
            cfg,
        )
        for warning in cfg_warnings:
            await self.writer.log_strategy_event(self.strategy_id, f"Passive MM config checker repaired config: {warning}", level="WARNING")
        self.pair = (await resolve_event_pairs(str(cfg.get("market_slug") or "auto:crypto-updown:btc:15m"), all_markets=False))[0]
        issues = _passive_cfg_issues(cfg, order_min_size=float(self.pair.get("order_min_size") or 5.0))
        if issues:
            msg = "; ".join(issues)
            await self.writer.log_strategy_event(self.strategy_id, f"Passive MM config checker blocking unsafe live config: {msg}", level="ERROR")
            raise RuntimeError(f"unsafe passive MM config: {msg}")
        self._configure_market_reference(cfg)
        await self._refresh_start_price(cfg)
        await self._load_existing_passive_fills()
        if not _safe_bool(cfg.get("dry_run", False), False):
            self.exec_client = PolymarketExecutionClient()
        await self.writer.log_strategy_event(
            self.strategy_id,
            f"Passive crypto MM loaded: {self.pair.get('title')} market={self.pair.get('slug')} quote_size={float(cfg.get('quote_size_shares', 5.0) or 5.0):.4f} shares/side max_market={float(cfg.get('shares_max_per_market', cfg.get('max_market_exposure_shares', 10.0)) or 10.0):.4f} quote_edge={float(cfg.get('quote_edge_cents', 7.0) or 7.0):.1f}c post_only=GTC",
        )

    async def _load_existing_passive_fills(self) -> None:
        if not self.pair or not getattr(self.writer, "_pool", None):
            return
        title = str(self.pair.get("title") or "")
        if not title:
            return
        try:
            async with self.writer._pool.acquire() as con:  # type: ignore[attr-defined]
                rows = await con.fetch(
                    """
                    SELECT market, sum(size)::float AS size, sum((px::float) * (size::float))::float AS cost
                    FROM fills
                    WHERE strategy_id=$1 AND kind='PASSIVE_MM' AND market LIKE $2
                    GROUP BY market
                    """,
                    self.strategy_id,
                    f"{title} [PASSIVE] %",
                )
        except Exception as exc:
            await self.writer.log_strategy_event(self.strategy_id, f"Existing fill load failed: {type(exc).__name__}: {exc}", level="WARNING")
            return
        loaded = {"YES": 0.0, "NO": 0.0}
        loaded_cost = {"YES": 0.0, "NO": 0.0}
        for r in rows:
            market = str(r.get("market") or "")
            size = float(r.get("size") or 0.0)
            cost = float(r.get("cost") or 0.0)
            if market.endswith(" YES"):
                loaded["YES"] += size
                loaded_cost["YES"] += cost
            elif market.endswith(" NO"):
                loaded["NO"] += size
                loaded_cost["NO"] += cost
        self.filled.update(loaded)
        self.fill_cost.update(loaded_cost)
        if loaded["YES"] or loaded["NO"]:
            avg = {leg: (self.fill_cost[leg] / self.filled[leg] if self.filled[leg] > 0 else 0.0) for leg in ("YES", "NO")}
            await self.writer.log_strategy_event(self.strategy_id, f"Loaded existing passive fills for current market: filled={self.filled} avg_px={avg}")

    def _configure_market_reference(self, cfg: dict[str, Any]) -> None:
        slug = str((self.pair or {}).get("slug") or cfg.get("market_slug") or "")
        m = re.search(r"-(\d{10})$", slug)
        start_ts = int(m.group(1)) if m else 0
        if start_ts != self.crypto_start_ts:
            self.crypto_start_ts = start_ts
            self.crypto_start_price = 0.0
            self.crypto_ref_prices.clear()
            self.fair_cache_key = None
            self.fair_cache = None
        self.fair_model = CryptoFairPriceModel(
            fallback_sigma=float(cfg.get("fair_model_fallback_sigma", 0.80) or 0.80),
            vol_floor=float(cfg.get("fair_model_vol_floor", 0.05) or 0.05),
            vol_cap=float(cfg.get("fair_model_vol_cap", 5.0) or 5.0),
            ewma_lambda=float(cfg.get("fair_model_ewma_lambda", 0.94) or 0.94),
            winsor_sigma=float(cfg.get("fair_model_winsor_sigma", 6.0) or 6.0),
            latency_buffer_seconds=float(cfg.get("fair_model_latency_buffer_seconds", 0.0) or 0.0),
        )

    def _binance_symbol(self, cfg: dict[str, Any]) -> str:
        asset = str(cfg.get("crypto_asset") or "btc").strip().lower()
        return {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT"}.get(asset, "BTCUSDT")

    async def _refresh_start_price(self, cfg: dict[str, Any]) -> None:
        if not self.crypto_start_ts:
            return
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": self._binance_symbol(cfg), "interval": "1s", "startTime": self.crypto_start_ts * 1000, "limit": 1}
        rows = []
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(3.0, connect=1.0)) as client:
                r = await client.get(url, params=params)
                r.raise_for_status()
                rows = r.json() or []
        except Exception as exc:
            await self.writer.log_strategy_event(
                self.strategy_id,
                f"Binance start-price refresh failed: {type(exc).__name__}: {exc}; trying market metadata fallback",
                level="WARNING",
            )
        if rows:
            px = _safe_float(rows[0][1])
            if px > 0:
                self.crypto_start_price = px
                self.fair_cache_key = None
                self.fair_cache = None
                return
        fallback = _safe_float((self.pair or {}).get("price_to_beat") or (self.pair or {}).get("priceToBeat"))
        if fallback > 0:
            self.crypto_start_price = fallback
            self.fair_cache_key = None
            self.fair_cache = None

    async def _refresh_current_price(self, cfg: dict[str, Any]) -> None:
        url = "https://api.binance.com/api/v3/ticker/price"
        async with httpx.AsyncClient(timeout=httpx.Timeout(2.5, connect=1.0)) as client:
            r = await client.get(url, params={"symbol": self._binance_symbol(cfg)})
            r.raise_for_status()
            px = _safe_float((r.json() or {}).get("price"))
            if px > 0:
                self.crypto_ref_price = px
                self.crypto_ref_prices.append(px)
                self.fair_cache_key = None
                self.fair_cache = None
                maxlen = int(cfg.get("fair_model_window_samples", 300) or 300)
                if len(self.crypto_ref_prices) > maxlen:
                    del self.crypto_ref_prices[: len(self.crypto_ref_prices) - maxlen]

    def _seconds_left(self) -> float:
        if not self.pair:
            return 0.0
        try:
            end_dt = datetime.fromisoformat(str(self.pair.get("end_date")).replace("Z", "+00:00"))
        except Exception:
            return 0.0
        return (end_dt - datetime.now(timezone.utc)).total_seconds()

    def _fair(self, cfg: dict[str, Any]) -> FairPriceSnapshot | None:
        if self.crypto_start_price <= 0 or self.crypto_ref_price <= 0 or not self.pair:
            return None
        seconds_left = self._seconds_left()
        if seconds_left < float(cfg.get("min_seconds_to_expiry_for_entry", 15) or 15):
            return None
        sample_seconds = float(cfg.get("fair_model_sample_seconds", 1.0) or 1.0)
        key = (
            int(self.crypto_start_ts),
            int(round(self.crypto_start_price * 100)),
            int(round(self.crypto_ref_price * 100)),
            int(round(seconds_left)),
            int(round(sample_seconds * 1000)),
        )
        if self.fair_cache_key == key and self.fair_cache is not None:
            return self.fair_cache
        snap = self.fair_model.price(
            start_price=self.crypto_start_price,
            current_price=self.crypto_ref_price,
            seconds_to_expiry=seconds_left,
            recent_prices=self.crypto_ref_prices,
            sample_seconds=sample_seconds,
        )
        self.fair_cache_key = key
        self.fair_cache = snap
        return snap

    async def _refresh_books(self) -> None:
        if not self.pair:
            return
        async with httpx.AsyncClient(timeout=httpx.Timeout(3.0, connect=1.0), http2=True) as client:
            b = await rest_books_full(client, [self.pair["yes_token"], self.pair["no_token"]])
        if self.pair["yes_token"] in b:
            self.books["YES"] = b[self.pair["yes_token"]]
        if self.pair["no_token"] in b:
            self.books["NO"] = b[self.pair["no_token"]]

    def _target_quotes(self, fair: FairPriceSnapshot, cfg: dict[str, Any]) -> dict[Leg, tuple[float, float, str]]:
        assert self.pair is not None
        tick = str(self.pair.get("tick_size") or cfg.get("tick_size") or "0.01")
        edge = float(cfg.get("quote_edge_cents", 7.0) or 7.0) / 100.0
        min_px = float(cfg.get("min_quote_price", 0.01) or 0.01)
        max_px = float(cfg.get("max_quote_price", 0.99) or 0.99)
        notional = float(cfg.get("quote_notional_usd", 0.0) if cfg.get("quote_notional_usd") is not None else 0.0)
        min_notional = float(cfg.get("min_order_notional_usd", 1.0) or 1.0)
        min_size = float(self.pair.get("order_min_size") or cfg.get("order_min_size") or 5.0)
        # Passive BTC15m maker orders are share-targeted.  A live post-only GTC
        # BUY of 5 shares @ $0.01 was accepted, so do not inflate maker quotes to
        # a $1 notional. The current production guard is exactly one exchange-
        # valid 5-share clip per side per market.
        max_quote_shares = float(cfg.get("max_quote_shares", cfg.get("quote_size_shares", 5.0)) or 5.0)
        quote_size_shares = float(cfg.get("quote_size_shares", 5.0) or 5.0)
        desired_clip = max(float(min_size or 0.0), max(0.0, min(float(max_quote_shares or quote_size_shares), quote_size_shares)))
        # Hard per-market share exposure cap, split evenly across YES/NO. Keep
        # the older max_market_exposure_shares key as a dashboard/backward-
        # compatible alias. This gates new/replacement quotes from confirmed
        # current-market fills loaded from DB and CLOB order reconciliation.
        max_market_exposure = float(cfg.get("shares_max_per_market", cfg.get("max_market_exposure_shares", cfg.get("max_position_size", 10.0))) or 10.0)
        max_side_exposure = max(0.0, max_market_exposure / 2.0)
        out: dict[Leg, tuple[float, float, str]] = {}
        fill_cost = getattr(self, "fill_cost", {"YES": 0.0, "NO": 0.0})
        hedge_edge = float(cfg.get("hedge_edge_cents", cfg.get("quote_edge_cents", 7.0)) or 7.0) / 100.0
        total_filled = float(self.filled.get("YES", 0.0) or 0.0) + float(self.filled.get("NO", 0.0) or 0.0)
        for leg, fp in (("YES", fair.fair_up), ("NO", fair.fair_down)):
            fair_edge_max = fp - edge
            book = self.books.get(leg)
            ask = float((book.ask if book else 0.0) or 0.0)
            try:
                step = float(tick or "0.01")
            except Exception:
                step = 0.01
            step = step if step in {0.1, 0.01, 0.001, 0.0001} else 0.01
            side_remaining = max(0.0, max_side_exposure - float(self.filled.get(leg, 0.0) or 0.0))
            market_remaining = max(0.0, max_market_exposure - total_filled)
            order_cap_remaining = min(side_remaining, market_remaining)
            opp: Leg = "NO" if leg == "YES" else "YES"
            hedge_residual = max(0.0, self.filled[opp] - self.filled[leg])
            purpose = "base"
            if hedge_residual > 1e-9:
                # Hedge at the fixed paired-profit limit from the original fill.
                # Do NOT trail this lower with model fair-value changes: after one
                # side fills, the opposite hedge should rest at the minimum price
                # that still locks the configured binary edge and wait/fill there.
                opp_avg = float(fill_cost.get(opp, 0.0) or 0.0) / self.filled[opp] if self.filled[opp] > 0 else 1.0
                hedge_price = 1.0 - opp_avg - hedge_edge
                if hedge_price < min_px - 1e-12:
                    out[leg] = (_round_price(min_px, tick), 0.0, "hedge_edge")
                    continue
                raw_price = max(min_px, min(max_px, hedge_price))
                purpose = "hedge"
            else:
                # Total exposure cap is across both outcomes combined. After any
                # fill, do not add a fresh base order on either side; only work the
                # explicit opposite-side hedge residual.
                if total_filled > 1e-9:
                    out[leg] = (_round_price(min_px, tick), 0.0, "awaiting_hedge")
                    continue
                max_price = min(max_px, fair_edge_max)
                if max_price < min_px - 1e-12:
                    out[leg] = (_round_price(min_px, tick), 0.0, "edge_floor")
                    continue
                raw_price = max(min_px, max_price)
                # Base post-only BUY must not cross the visible ask. Quote at least
                # one tick below ask, while still respecting fair-edge target.
                if ask > 0:
                    raw_price = min(raw_price, ask - step)
                if raw_price < min_px - 1e-12:
                    out[leg] = (_round_price(min_px, tick), 0.0, "post_only_cross")
                    continue
            price = _round_price(raw_price, tick)
            min_valid_size = float(min_size or 0.0)
            quote_clip = max(desired_clip, min_valid_size)
            if hedge_residual > 1e-9:
                # Strict current-market cap: work only one 5-share hedge clip when
                # doing so cannot exceed 5 filled shares on that side. If a partial
                # fill leaves residual below the exchange minimum, do not post a
                # minimum-size over-hedge that would exceed the user's 5-per-side
                # limit for this market.
                if order_cap_remaining + 1e-9 < min_valid_size:
                    size = 0.0
                    purpose = "side_filled_cap" if side_remaining + 1e-9 < min_valid_size else "market_filled_cap"
                elif hedge_residual + 1e-9 < min_valid_size:
                    size = 0.0
                    purpose = "residual_below_min"
                else:
                    size = min(hedge_residual, quote_clip, order_cap_remaining)
                    purpose = "hedge" if size >= min_valid_size - 1e-9 else "residual_below_min"
            elif order_cap_remaining + 1e-9 < min_valid_size:
                size = 0.0
                purpose = "side_filled_cap" if side_remaining + 1e-9 < min_valid_size else "market_filled_cap"
            else:
                size = min(quote_clip, order_cap_remaining)
                purpose = "base" if size >= min_valid_size - 1e-9 else "exposure_cap"
            size = math.ceil(size * 10_000 - 1e-12) / 10_000
            out[leg] = (price, size, purpose if size > 0 else purpose)
        return out

    async def _record_order_match(self, ro: RestingOrder, matched: float, source: str) -> float:
        matched = max(0.0, min(float(matched or 0.0), ro.size))
        delta = max(0.0, matched - ro.filled_size)
        if delta <= 1e-9:
            return 0.0
        ro.filled_size = matched
        self.filled[ro.leg] += delta
        self.fill_cost[ro.leg] += ro.price * delta
        self.fill_seq += 1
        await self.writer.record_fill(
            self.strategy_id,
            self.fill_seq,
            f"{(self.pair or {}).get('title','')} [PASSIVE] {ro.leg}",
            "BUY",
            ro.price,
            delta,
            kind="PASSIVE_MM",
        )
        await self.writer.log_strategy_event(
            self.strategy_id,
            f"Detected fill {ro.leg} order={ro.order_id} source={source} delta={delta:.4f} total_filled={self.filled}",
        )
        return delta

    async def _cancel(self, leg: Leg, reason: str) -> float:
        ro = self.open.get(leg)
        if not ro or not self.exec_client:
            self.open[leg] = None
            return 0.0
        fill_delta = 0.0
        try:
            # A cancel can succeed after a partial maker fill. Query matched size
            # before canceling so replacement logic does not forget the fill and
            # post another same-side order that exceeds the per-side cap.
            raw_before = self.exec_client.get_order(ro.order_id)
            fill_delta += await self._record_order_match(ro, _matched_size(raw_before, ro.size), "pre_cancel_reconcile")
        except Exception as exc:
            await self.writer.log_strategy_event(self.strategy_id, f"pre-cancel get_order failed {leg} {ro.order_id}: {type(exc).__name__}: {exc}", level="WARNING")
        try:
            resp = self.exec_client.cancel(ro.order_id)
            await self.writer.log_strategy_event(self.strategy_id, f"Cancelled {leg} order {ro.order_id} reason={reason} resp={str(resp)[:180]}")
            try:
                raw_after = self.exec_client.get_order(ro.order_id)
                fill_delta += await self._record_order_match(ro, _matched_size(raw_after, ro.size), "post_cancel_reconcile")
            except Exception:
                pass
            not_canceled = resp.get("not_canceled") if isinstance(resp, dict) else None
            msg = str((not_canceled or {}).get(ro.order_id, "")).lower() if isinstance(not_canceled, dict) else ""
            if "matched" in msg and "can't be canceled" in msg:
                fill_delta += await self._record_order_match(ro, ro.size, "cancel_not_canceled_matched")
        except Exception as exc:
            await self.writer.log_strategy_event(self.strategy_id, f"Cancel error {leg} order {ro.order_id}: {type(exc).__name__}: {exc}", level="WARNING")
        self.open[leg] = None
        return fill_delta

    async def _place(self, leg: Leg, price: float, size: float, purpose: str, cfg: dict[str, Any]) -> None:
        assert self.pair is not None
        token = self.pair["yes_token"] if leg == "YES" else self.pair["no_token"]
        notional = price * size
        book = self.books.get(leg)
        if size <= 0:
            if purpose == "exposure_cap":
                await self.writer.log_strategy_event(self.strategy_id, f"Skip {leg} quote @{price:.3f}: max market exposure reached", level="INFO")
            else:
                await self.writer.log_strategy_event(self.strategy_id, f"Skip {leg} {purpose} quote @{price:.3f} x {size:.4f}: below exchange minimum", level="INFO")
            return
        if book and book.ask and purpose != "hedge" and price >= float(book.ask) - 1e-12:
            await self.writer.log_strategy_event(self.strategy_id, f"Skip {leg} post-only quote @{price:.3f}: would cross ask={book.ask:.3f}", level="INFO")
            return
        # Passive maker GTC orders are share-floor validated for BTC15m; do not
        # apply the taker/FOK $1 notional floor here, otherwise a valid 5 @ $0.01
        # post-only maker quote would be skipped.
        if _safe_bool(cfg.get("dry_run", False), False):
            oid = f"dry-{leg}-{int(time.time()*1000)}"
            resp = {"success": True, "orderID": oid, "dry_run": True}
        else:
            assert self.exec_client is not None
            order = PolyOrder(
                token_id=token,
                side="BUY",
                price=Decimal(str(price)),
                size=Decimal(str(size)),
                order_type="GTC",
                post_only=(purpose != "hedge"),
                use_limit_order=True,
                tick_size=str(self.pair.get("tick_size") or cfg.get("tick_size") or "0.01"),
                neg_risk=_safe_bool(self.pair.get("neg_risk", False), False),
                builder_code=str(cfg.get("builder_code") or os.getenv("POLYMARKET_BUILDER_CODE") or os.getenv("POLY_BUILDER_CODE") or "") or None,
            )
            try:
                resp = self.exec_client.submit(order)
            except Exception as exc:
                # Post-only can still be rejected if the visible book moved between
                # book snapshot and submit. Treat it as a skipped/rejected quote,
                # not as a process-level failure.
                resp = {"success": False, "exception": type(exc).__name__, "error": str(exc)[:500]}
            oid = _order_id(resp) or ""
        status = "submitted" if oid else "rejected"
        await self.writer.record_order_attempt(
            self.strategy_id,
            str(self.pair.get("slug") or ""),
            token,
            leg,
            "BUY",
            "GTC_LIMIT" if purpose == "hedge" else "GTC_POST_ONLY",
            price,
            size,
            round(notional, 4),
            status,
            response=resp,
            error=clob_response_error(resp),
            signal={"purpose": purpose, "fair_quote_offset_cents": float(cfg.get("quote_edge_cents", 7.0) or 7.0), "resting": True},
            config=cfg,
        )
        if oid:
            self.open[leg] = RestingOrder(leg, token, oid, price, size, _now(), purpose)
            await self.writer.log_strategy_event(self.strategy_id, f"Placed {purpose} resting {leg} BUY post-only GTC @{price:.3f} x {size:.4f} notional=${notional:.2f} order={oid}")
        else:
            await self.writer.log_strategy_event(self.strategy_id, f"Failed to place resting {leg} @{price:.3f} x {size:.4f}: {str(resp)[:220]}", level="WARNING")

    async def _reconcile_orders(self, cfg: dict[str, Any]) -> None:
        if not self.exec_client:
            return
        for leg, ro in list(self.open.items()):
            if not ro:
                continue
            try:
                raw = self.exec_client.get_order(ro.order_id)
            except Exception as exc:
                await self.writer.log_strategy_event(self.strategy_id, f"get_order failed {leg} {ro.order_id}: {type(exc).__name__}: {exc}", level="WARNING")
                continue
            matched = _matched_size(raw, ro.size)
            delta = max(0.0, matched - ro.filled_size)
            if delta > 1e-9:
                await self._record_order_match(ro, matched, f"reconcile_status={_status(raw)}")
            if _status(raw) in CLOB_FILLED | CLOB_CANCELLED or matched >= ro.size - 1e-9:
                self.open[leg] = None

    async def _roll_market_if_needed(self, cfg: dict[str, Any], *, force: bool = False) -> None:
        min_left = float(cfg.get("min_seconds_to_expiry_for_entry", 15) or 15)
        if not force and self.pair and self._seconds_left() >= min_left:
            return
        old_slug = str((self.pair or {}).get("slug") or "")
        # Never leave stale quotes sitting through a roll/expiry.
        for leg in ("YES", "NO"):
            await self._cancel(leg, "market_roll_or_expiry")
        try:
            new_pair = (await resolve_event_pairs(str(cfg.get("market_slug") or "auto:crypto-updown:btc:15m"), all_markets=False))[0]
        except Exception as exc:
            await self.writer.log_strategy_event(self.strategy_id, f"Market roll discovery failed: {type(exc).__name__}: {exc}", level="WARNING")
            return
        new_slug = str(new_pair.get("slug") or "")
        if new_slug == old_slug and not force:
            return
        self.pair = new_pair
        self.books = {"YES": Book(), "NO": Book()}
        self.open = {"YES": None, "NO": None}
        self.filled = {"YES": 0.0, "NO": 0.0}
        self.fill_cost = {"YES": 0.0, "NO": 0.0}
        self._configure_market_reference(cfg)
        try:
            await self._refresh_start_price(cfg)
            await self._load_existing_passive_fills()
        except Exception as exc:
            await self.writer.log_strategy_event(self.strategy_id, f"Start-price refresh after roll failed: {type(exc).__name__}: {exc}", level="WARNING")
        await self.writer.log_strategy_event(self.strategy_id, f"Rolled passive MM market: old={old_slug or 'none'} new={new_slug} title={new_pair.get('title')}")

    async def _quote_once(self, cfg: dict[str, Any]) -> None:
        status = await self.writer.get_strategy_status(self.strategy_id)
        if status != "running":
            for leg in ("YES", "NO"):
                await self._cancel(leg, f"strategy_status={status}")
            return
        await self._roll_market_if_needed(cfg)
        await self._refresh_current_price(cfg)
        await self._refresh_books()
        await self._reconcile_orders(cfg)
        fair = self._fair(cfg)
        if fair is None:
            await self.writer.log_strategy_event(self.strategy_id, "Passive MM fair unavailable or too close to expiry; not quoting", level="WARNING")
            return
        targets = self._target_quotes(fair, cfg)
        replace_bps = float(cfg.get("replace_threshold_cents", 1.0) or 1.0) / 100.0
        size_replace = float(cfg.get("size_replace_threshold_shares", 0.1) or 0.1)
        for leg in ("YES", "NO"):
            opp: Leg = "NO" if leg == "YES" else "YES"
            # Once one side has more fills, stop adding inventory on that same
            # side. Only keep the under-filled opposite quote live until hedge
            # fills catch up. This prevents repeated same-side fills while the
            # hedge quote is resting.
            if self.filled[leg] > self.filled[opp] + 1e-9:
                if self.open.get(leg):
                    await self._cancel(leg, f"inventory_imbalance filled_{leg}={self.filled[leg]:.4f} filled_{opp}={self.filled[opp]:.4f}")
                continue
            price, size, purpose = targets[leg]
            ro = self.open.get(leg)
            if ro and ro.purpose == "hedge" and purpose == "hedge":
                # Hedge order is intentionally fixed at the hedge-min price from
                # the first fill. Do not chase/reprice it lower while waiting.
                continue
            if ro and abs(ro.price - price) < replace_bps - 1e-12 and abs(ro.size - size) < size_replace:
                continue
            if ro:
                fill_delta = await self._cancel(leg, f"requote target={price:.3f}x{size:.4f} purpose={purpose}")
                if fill_delta > 1e-9:
                    targets = self._target_quotes(fair, cfg)
                    if self.filled[leg] > self.filled[opp] + 1e-9:
                        continue
                    price, size, purpose = targets[leg]
            # A zero-size target is an intentional risk/cap state, not an order
            # attempt. Do not call _place() every quote loop, otherwise capped
            # markets spam "below exchange minimum" logs at quote cadence.
            if size <= 0:
                continue
            await self._place(leg, price, size, purpose, cfg)
        if time.time() - self.last_quote_log_ts > float(cfg.get("health_log_interval_seconds", 15) or 15):
            self.last_quote_log_ts = time.time()
            yes_q, no_q = targets["YES"], targets["NO"]
            await self.writer.log_strategy_event(
                self.strategy_id,
                f"Passive MM health: fair_up={fair.fair_up:.3f} fair_down={fair.fair_down:.3f} ref={fair.current_price:.2f} start={fair.start_price:.2f} sec_left={fair.seconds_to_expiry:.1f} YES_quote={yes_q[0]:.3f}x{yes_q[1]:.4f}/{yes_q[2]} NO_quote={no_q[0]:.3f}x{no_q[1]:.4f}/{no_q[2]} filled={self.filled}",
            )

    async def run(self) -> None:
        await self.setup()
        cfg, _cfg_warnings = await self._effective_cfg()
        while not self.stop.is_set():
            interval = max(0.5, float(cfg.get("quote_update_interval_seconds", 2.0) or 2.0))
            try:
                cfg, cfg_warnings = await self._effective_cfg()
                for warning in cfg_warnings:
                    await self.writer.log_strategy_event(self.strategy_id, f"Passive MM config checker repaired config: {warning}", level="WARNING")
                    await self.writer.register_strategy(
                        self.strategy_id,
                        self.name,
                        str(cfg.get("kind") or "crypto_passive_mm"),
                        str(cfg.get("market_slug") or "auto:crypto-updown:btc:15m"),
                        cfg,
                    )
                issues = _passive_cfg_issues(cfg, order_min_size=float((self.pair or {}).get("order_min_size") or cfg.get("order_min_size") or 5.0))
                if issues:
                    await self.writer.log_strategy_event(self.strategy_id, f"Passive MM config checker skipped quote: {'; '.join(issues)}", level="ERROR")
                else:
                    interval = max(0.5, float(cfg.get("quote_update_interval_seconds", 2.0) or 2.0))
                    await self._quote_once(cfg)
            except Exception as exc:
                logger.exception("passive crypto mm loop error")
                try:
                    await self.writer.log_strategy_event(self.strategy_id, f"Passive MM loop error: {type(exc).__name__}: {exc}", level="ERROR")
                except Exception:
                    pass
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
        for leg in ("YES", "NO"):
            await self._cancel(leg, "shutdown")
        await self.writer.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    runner = PassiveCryptoMMRunner(Path(args.config))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, runner.stop.set)
        except NotImplementedError:
            pass
    loop.run_until_complete(runner.run())


if __name__ == "__main__":
    main()
