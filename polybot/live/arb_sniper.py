"""Low-latency live YES+NO arbitrage sniper for Polymarket binary markets.

This runner is intentionally separate from the general multi-strategy supervisor.
It keeps the hot path small:

    websocket best-ask update -> in-memory plan -> CLOB V2 batch FOK YES+NO -> rescue/hold residual

Postgres/dashboard writes are outside the critical decision path. The runner can
be left running while the dashboard controls `strategies.status`; it only submits
orders when status == "running".
"""
from __future__ import annotations

import argparse
import asyncio
import gc
import hashlib
import json
import math
import os
import re
import signal
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import httpx
import websockets
import yaml
from dotenv import load_dotenv
from loguru import logger

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.crypto.fair_price import CryptoFairPriceModel, FairPriceSnapshot, fair_edge_accepts_pair
from polybot.live.weather_safety_filter import STATIONS, analyze_city_safety, c_to_f, event_target_date
from polybot.persistence.writer import PolybotWriter

WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_REST = "https://clob.polymarket.com"
GAMMA_REST = "https://gamma-api.polymarket.com"
STARTING_CASH = 10_000.0
Leg = Literal["YES", "NO"]
GAMMA_CACHE_DIR = Path(os.getenv("POLYBOT_GAMMA_CACHE_DIR", "data/runtime/gamma_cache"))
GAMMA_HEADERS = {
    "User-Agent": os.getenv("POLYBOT_GAMMA_USER_AGENT", "polybot-live/1.0 (+market-discovery)"),
    "Accept": "application/json",
}


class GammaDiscoveryError(RuntimeError):
    """Raised when public Gamma market discovery returns non-JSON/block pages."""


def _gamma_cache_path(key: str) -> Path:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]
    return GAMMA_CACHE_DIR / f"{digest}.json"


def _looks_like_cloudflare_block(text: str, content_type: str = "") -> bool:
    sample = (text or "")[:4096].lower()
    return (
        "text/html" in (content_type or "").lower()
        or "attention required" in sample
        or "sorry, you have been blocked" in sample
        or "cloudflare ray id" in sample
        or "/cdn-cgi/" in sample
    )


def _read_gamma_cache(key: str, max_age_seconds: float) -> Any | None:
    path = _gamma_cache_path(key)
    try:
        raw = json.loads(path.read_text())
        if time.time() - float(raw.get("saved_at") or 0) > max_age_seconds:
            return None
        return raw.get("data")
    except Exception:
        return None


def _write_gamma_cache(key: str, data: Any) -> None:
    try:
        GAMMA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _gamma_cache_path(key).write_text(json.dumps({"saved_at": time.time(), "data": data}))
    except Exception as e:
        logger.debug("gamma cache write failed: {}", e)


async def gamma_get_json(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any],
    *,
    cache_key: str,
    cache_max_age_seconds: float = 300.0,
    stale_max_age_seconds: float = 7200.0,
    attempts: int = 4,
) -> Any:
    """Fetch public Gamma JSON with jittered retry and stale-cache fallback.

    Gamma is public/no-auth, but it is fronted by Cloudflare. On transient WAF
    block/challenge HTML we keep the live service alive by returning recent cached
    metadata when available instead of exiting the daemon.
    """
    fresh = _read_gamma_cache(cache_key, cache_max_age_seconds)
    if fresh is not None:
        return fresh
    last_error: Exception | None = None
    for attempt in range(max(1, attempts)):
        try:
            r = await client.get(f"{GAMMA_REST}{path}", params=params, headers=GAMMA_HEADERS)
            ctype = str(r.headers.get("content-type") or "")
            text_prefix = r.text[:4096]
            if r.status_code != 200 or _looks_like_cloudflare_block(text_prefix, ctype):
                cf_ray = r.headers.get("cf-ray") or ""
                raise GammaDiscoveryError(f"Gamma discovery non-json/block status={r.status_code} ctype={ctype} cf_ray={cf_ray}")
            data = r.json()
            _write_gamma_cache(cache_key, data)
            return data
        except Exception as e:
            last_error = e
            delay = min(20.0, 0.75 * (2 ** attempt))
            delay += (int(hashlib.sha256(f"{cache_key}:{attempt}".encode()).hexdigest()[:4], 16) % 500) / 1000.0
            logger.warning("Gamma discovery attempt {}/{} failed for {}: {}; retrying in {:.2f}s", attempt + 1, attempts, cache_key, e, delay)
            await asyncio.sleep(delay)
    stale = _read_gamma_cache(cache_key, stale_max_age_seconds)
    if stale is not None:
        logger.warning("Using stale Gamma discovery cache for {} after error: {}", cache_key, last_error)
        return stale
    raise GammaDiscoveryError(f"Gamma discovery failed for {cache_key}: {last_error}")


def clob_response_indicates_fill(resp: dict[str, Any] | None) -> bool:
    """Return true only for a response that actually represents a matched CLOB order.

    Some py-clob-client batch responses can contain ``success: true`` while also
    carrying an ``errorMsg`` and no ``orderID``/status. Those are API-level
    acknowledgement objects, not fills. Treating them as fills creates fake
    dashboard trades that never happened on Polymarket.
    """
    if not isinstance(resp, dict):
        return False
    if str(resp.get("error") or resp.get("errorMsg") or resp.get("message") or "").strip():
        return False
    status = str(resp.get("status") or "").strip().lower()
    if status in {"matched", "filled"}:
        return True
    # Any other acknowledgement (including success+orderID with missing/unknown
    # status, delayed, live, pending) is not proof of an immediate fill. The hot
    # path must not record fills or submit hedge legs from ambiguous acks.
    return False


def clob_response_error(resp: dict[str, Any] | None) -> str | None:
    if not isinstance(resp, dict):
        return None
    err = resp.get("error") or resp.get("errorMsg") or resp.get("message")
    return str(err) if err else None


def _clob_numeric(resp: dict[str, Any] | None, *keys: str) -> float:
    if not isinstance(resp, dict):
        return 0.0
    for key in keys:
        value = resp.get(key)
        if value is None:
            continue
        try:
            out = float(value)
        except Exception:
            continue
        if out > 0:
            return out
    return 0.0


def clob_response_matched_size(resp: dict[str, Any] | None, fallback_full_size: float = 0.0) -> float:
    """Best-effort matched share count from CLOB response payloads.

    FAK/IOC orders can partially fill; account using the matched size when CLOB
    exposes it. Older/simplified SDK responses may only say ``status=matched``;
    in that case fall back to the requested size, matching the prior FOK behavior.
    """
    matched = _clob_numeric(
        resp,
        "matched_size",
        "matchedSize",
        "size_matched",
        "sizeMatched",
        "filled_size",
        "filledSize",
        "filled",
        "takerAmount",
        "taker_amount",
        "takingAmount",
        "taking_amount",
    )
    if matched > 0:
        return matched
    return float(fallback_full_size or 0.0) if clob_response_indicates_fill(resp) else 0.0


def clob_response_matched_notional(resp: dict[str, Any] | None, fallback_notional: float = 0.0) -> float:
    matched = _clob_numeric(
        resp,
        "matched_amount",
        "matchedAmount",
        "amount_matched",
        "amountMatched",
        "makerAmount",
        "maker_amount",
        "makingAmount",
        "making_amount",
        "notional",
        "cost",
    )
    if matched > 0:
        return matched
    return float(fallback_notional or 0.0) if clob_response_indicates_fill(resp) else 0.0


@dataclass
class Book:
    bid: float = 0.0
    ask: float = 0.0
    bids: list[dict[str, float]] | None = None
    asks: list[dict[str, float]] | None = None
    updated_ts: float = 0.0
    tick_size: str | None = None
    neg_risk: bool | None = None
    order_min_size: float | None = None


class LatencyStats:
    """Tiny rolling latency window for live diagnostics without DB in hot path."""

    def __init__(self, maxlen: int = 200) -> None:
        self.samples: deque[float] = deque(maxlen=max(1, int(maxlen or 1)))

    def add(self, value_ms: float) -> None:
        try:
            v = float(value_ms)
        except Exception:
            return
        if v >= 0:
            self.samples.append(v)

    def summary(self) -> dict[str, float | int | None]:
        vals = sorted(self.samples)
        if not vals:
            return {"count": 0, "min_ms": None, "median_ms": None, "max_ms": None}
        return {
            "count": len(vals),
            "min_ms": vals[0],
            "median_ms": vals[len(vals) // 2],
            "max_ms": vals[-1],
        }


@dataclass
class ArbPlan:
    yes_size: float
    no_size: float
    size: float
    yes_limit: float
    no_limit: float
    yes_cost_est: float
    no_cost_est: float
    total_cost_est: float
    avg_sum_est: float
    edge_per_pair: float
    first_leg: Leg
    second_leg: Leg


@dataclass
class ArbSkipDiagnostic:
    reason: str
    opportunity_spotted: bool
    yes_ask: float
    no_ask: float
    raw_sum: float
    friction: float
    edge: float
    min_edge: float
    details: dict[str, Any]


def fair_model_accepts_arb_plan(
    plan: ArbPlan,
    fair: FairPriceSnapshot | None,
    *,
    min_model_edge: float = 0.0,
    max_leg_overpay: float = 0.0,
) -> bool:
    """Gate an executable YES+NO plan against model fair values.

    Disabled when ``fair`` is None or min_model_edge <= 0.  For enabled gates,
    compare the depth-weighted average leg costs to model fair, not just top of
    book.  This keeps backtest/live parity for the wallet-derived strategy:
    complementary pair edge must clear AND each leg must be cheap versus the
    short-horizon fair-value model.
    """
    if fair is None or float(min_model_edge or 0.0) <= 0:
        return True
    yes_avg = float(plan.yes_cost_est) / max(float(plan.yes_size), 1e-9)
    no_avg = float(plan.no_cost_est) / max(float(plan.no_size), 1e-9)
    return fair_edge_accepts_pair(
        yes_avg=yes_avg,
        no_avg=no_avg,
        fair_up=float(fair.fair_up),
        min_model_edge=float(min_model_edge),
        max_leg_overpay=float(max_leg_overpay or 0.0),
    )


@dataclass
class WeatherOutlierPlan:

    pair: dict[str, Any]
    token: str
    temp_value: float
    winning_temp: float
    winning_price: float
    ask: float
    size: float
    notional: float
    distance_degrees: float
    min_edge: float
    max_no_price: float
    tier_edge_multiplier: float = 1.0
    tier_notional_multiplier: float = 1.0
    tier_target_notional: float = 0.0
    tier_remaining_notional: float = 0.0
    boundary_forecast_high_c: float | None = None
    boundary_distance_c: float | None = None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _safe_bool(x: Any, default: bool = False) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        text = x.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default
    return bool(x)


def market_websocket_enabled(cfg: dict[str, Any]) -> bool:
    """Return whether the Polymarket market websocket loop should run.

    Weather outlier shards have an independent REST /books polling hot path. This
    flag lets those shards disable noisy websocket connections without stopping
    trading/evaluation.
    """
    if _safe_bool(cfg.get("disable_market_websocket", False), False):
        return False
    return _safe_bool(cfg.get("market_websocket_enabled", cfg.get("websocket_enabled", True)), True)


def _weather_market_event_key(slug: str) -> str:
    """Return the same-date weather event key from a temperature market slug."""
    text = str(slug or "").strip().lower()
    m = re.match(r"(highest-temperature-in-[a-z0-9-]+-on-[a-z]+-\d{1,2}-\d{4})(?:-|$)", text)
    return m.group(1) if m else text


def _outlier_direction(temp_value: float | None, winning_temp: float | None) -> str | None:
    """Classify an outlier bracket relative to the then-current favorite."""
    if temp_value is None or winning_temp is None:
        return None
    if float(temp_value) > float(winning_temp) + 1e-9:
        return "higher"
    if float(temp_value) < float(winning_temp) - 1e-9:
        return "lower"
    return None


def _direction_allows_candidate(lock_direction: str | None, candidate_direction: str | None) -> bool:
    if not lock_direction or not candidate_direction:
        return True
    return lock_direction == candidate_direction


def _weather_boundary_veto_threshold_c(cfg: dict[str, Any]) -> float:
    if not _safe_bool(cfg.get("weather_outlier_boundary_veto_enabled", True), True):
        return 0.0
    return max(0.0, _safe_float(cfg.get("weather_outlier_boundary_veto_degrees_c", cfg.get("weather_outlier_boundary_veto_degrees", 2.0)), 2.0))


def _weather_boundary_forecast_high_c(safety_result: dict[str, Any] | None) -> float | None:
    metrics = (safety_result or {}).get("metrics") or {}
    val = metrics.get("forecast_high_c")
    high = _safe_float(val, float("nan"))
    return None if math.isnan(high) else high


def _weather_boundary_veto_reason(temp_c: float, forecast_high_c: float | None, threshold_c: float) -> str | None:
    if forecast_high_c is None or threshold_c <= 0:
        return None
    distance_c = abs(float(temp_c) - float(forecast_high_c))
    if distance_c <= threshold_c + 1e-9:
        forecast_f = c_to_f(forecast_high_c)
        threshold_f = threshold_c * 9.0 / 5.0
        temp_f = c_to_f(temp_c)
        return (
            f"boundary veto: candidate {temp_c:g}°C/{temp_f:.1f}°F is {distance_c:.2f}°C "
            f"from resolution-source forecast high {forecast_high_c:g}°C/{forecast_f:.1f}°F "
            f"(threshold {threshold_c:g}°C/{threshold_f:.1f}°F)"
        )
    return None


def _weather_outlier_rebuy_tiers(cfg: dict[str, Any]) -> list[tuple[float, float]]:
    """Return (edge_multiplier, target_notional_multiplier) tiers.

    Default behavior remains the original one-shot tier: buy up to 1x base
    notional at `min_edge`. When `weather_outlier_rebuy_tiers_enabled` is true,
    the default ladder is 1x at 1*edge, 2x total at 2*edge, and 3x total at
    3*edge. A custom `weather_outlier_rebuy_tiers` list/string may override this,
    e.g. `1:1,2:2,3:3`.
    """
    enabled = _safe_bool(cfg.get("weather_outlier_rebuy_tiers_enabled", False), False)
    raw = cfg.get("weather_outlier_rebuy_tiers")
    tiers: list[tuple[float, float]] = []
    if raw:
        if isinstance(raw, str):
            items: list[Any] = [x.strip() for x in raw.split(",") if x.strip()]
        elif isinstance(raw, list):
            items = raw
        else:
            items = []
        for item in items:
            edge_mult = notional_mult = None
            if isinstance(item, dict):
                edge_mult = _safe_float(item.get("edge_multiplier"), 0.0)
                notional_mult = _safe_float(item.get("notional_multiplier"), 0.0)
            elif isinstance(item, str):
                sep = ":" if ":" in item else "x"
                parts = [p.strip() for p in item.split(sep, 1)]
                if len(parts) == 2:
                    edge_mult = _safe_float(parts[0], 0.0)
                    notional_mult = _safe_float(parts[1], 0.0)
            if edge_mult and edge_mult > 0 and notional_mult and notional_mult > 0:
                tiers.append((float(edge_mult), float(notional_mult)))
    if not tiers:
        tiers = [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)] if enabled else [(1.0, 1.0)]
    dedup: dict[float, float] = {}
    for edge_mult, notional_mult in tiers:
        dedup[float(edge_mult)] = max(float(notional_mult), dedup.get(float(edge_mult), 0.0))
    return sorted(dedup.items(), key=lambda x: x[0]) or [(1.0, 1.0)]


def deterministic_rest_poll_phase_ms(strategy_id: str, interval_ms: float, configured_phase: Any = None) -> float:
    """Return a stable per-shard initial REST poll offset.

    Weather arb shards run as separate systemd services. If they all start at the
    same time and poll every 225ms, total request rate may be under the documented
    /books budget while still arriving in synchronized bursts. A deterministic
    phase spreads the same number of requests across the interval without adding
    per-cycle latency or reducing each shard's polling cadence.
    """
    try:
        interval = float(interval_ms)
    except Exception:
        return 0.0
    if interval <= 0:
        return 0.0
    if configured_phase not in (None, "", "auto"):
        try:
            return max(0.0, float(configured_phase)) % interval
        except Exception:
            pass
    digest = hashlib.blake2s(str(strategy_id or "arb-sniper").encode("utf-8"), digest_size=4).digest()
    bucket = int.from_bytes(digest, "big")
    return float(bucket % max(1, int(interval)))


def _market_tick_size(market: dict[str, Any]) -> str:
    tick = str(
        market.get("orderPriceMinTickSize")
        or market.get("minimumTickSize")
        or market.get("minTickSize")
        or market.get("tickSize")
        or "0.01"
    )
    return tick if tick in {"0.1", "0.01", "0.001", "0.0001"} else "0.01"


def _price_matches_tick(price: float, tick_size: str | float) -> bool:
    """True when price is directly executable for the market tick size."""
    tick = _safe_float(tick_size, 0.01)
    if tick <= 0:
        tick = 0.01
    px = max(0.001, min(0.999, float(price or 0.0)))
    units = round(px / tick)
    return abs(px - units * tick) <= 1e-9


def _parse_levels(levels: list[Any]) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    for x in levels or []:
        px = _safe_float(x.get("price") if isinstance(x, dict) else 0)
        sz = _safe_float(x.get("size") if isinstance(x, dict) else 0)
        if 0 < px < 1 and sz > 0:
            out.append({"price": px, "size": sz})
    return out


def _cost_for_size(asks: list[dict[str, float]], target_size: float) -> tuple[float, float] | None:
    """Return (estimated_cost, worst_limit_price) to buy target_size shares."""
    if target_size <= 0:
        return None
    remaining = target_size
    cost = 0.0
    worst = 0.0
    for level in sorted(asks, key=lambda x: _safe_float(x.get("price"))):
        px = _safe_float(level.get("price"))
        sz = _safe_float(level.get("size"))
        if px <= 0 or sz <= 0:
            continue
        take = min(remaining, sz)
        cost += take * px
        remaining -= take
        worst = px
        if remaining <= 1e-9:
            return cost, worst
    return None


def _size_for_notional(asks: list[dict[str, float]], target_notional: float, *, precision: int = 4) -> tuple[float, float, float] | None:
    """Return (size, estimated_cost, worst_limit_price) for a fixed dollar BUY.

    Fixed-dollar mode intentionally sizes each leg independently from the configured
    dollar value (for example $1 YES and $1 NO). It does *not* force equal YES/NO
    shares; setting the fixed dollar values to zero falls back to the equal-share
    offset/hedge planner.
    """
    return _size_for_notional_capped(asks, target_notional, precision=precision, allow_partial=False)


def _size_for_notional_capped(
    asks: list[dict[str, float]],
    target_notional: float,
    *,
    precision: int = 4,
    allow_partial: bool = False,
    min_notional_usd: float = 0.0,
) -> tuple[float, float, float] | None:
    """Size a BUY up to target_notional from already price-capped ask levels.

    When ``allow_partial`` is true, return all visible capped liquidity even if it
    does not reach the target, as long as the resulting order still clears the
    configured minimum notional. This is used by weather outlier snipers to sweep
    partial liquidity now and keep buying later until the per-market dollar cap is
    reached.
    """
    if target_notional <= 0:
        return None
    remaining = float(target_notional)
    size = 0.0
    worst = 0.0
    for level in sorted(asks, key=lambda x: _safe_float(x.get("price"))):
        px = _safe_float(level.get("price"))
        available = _safe_float(level.get("size"))
        if px <= 0 or available <= 0:
            continue
        take = min(available, remaining / px)
        if take <= 0:
            continue
        size += take
        remaining -= take * px
        worst = px
        if remaining <= 1e-9:
            break
    if size <= 0 or worst <= 0:
        return None
    if remaining > 1e-9 and not allow_partial:
        return None
    rounded_size = round(size, precision)
    if rounded_size <= 0:
        return None
    rounded_cost = _cost_for_size(asks, rounded_size)
    if rounded_cost is None:
        return None
    estimated_cost = round(rounded_cost[0], 4)
    if min_notional_usd > 0 and estimated_cost + 1e-9 < min_notional_usd:
        return None
    return rounded_size, estimated_cost, rounded_cost[1]


def polymarket_fee_per_share(price: float, fee_rate: float = 0.0) -> float:
    """Polymarket taker fee per share for one matched order.

    Fee docs use a price-dependent formula: feeRate * price * (1 - price).
    Keep fee_rate configurable because fees are market-specific and can be zero.
    """
    try:
        p = float(price)
        r = float(fee_rate)
    except Exception:
        return 0.0
    if not (0.0 < p < 1.0) or r <= 0:
        return 0.0
    return r * p * (1.0 - p)


def _candidate_sizes(yes_asks: list[dict[str, float]], no_asks: list[dict[str, float]], max_size: float) -> list[float]:
    """Breakpoints where either leg walks to a new ask level."""
    vals = {round(max_size, 4)}
    cum = 0.0
    for level in sorted(yes_asks, key=lambda x: _safe_float(x.get("price"))):
        cum += _safe_float(level.get("size"))
        if 0 < cum <= max_size:
            vals.add(round(cum, 4))
    cum = 0.0
    for level in sorted(no_asks, key=lambda x: _safe_float(x.get("price"))):
        cum += _safe_float(level.get("size"))
        if 0 < cum <= max_size:
            vals.add(round(cum, 4))
    return sorted(vals, reverse=True)


def build_arb_plan(
    yes_book: Book,
    no_book: Book,
    *,
    order_limit_usd: float,
    min_edge: float,
    fee_per_share: float = 0.0,
    fee_rate: float = 0.0,
    gas_per_share: float = 0.0,
    stale_quote_buffer: float = 0.0,
    min_order_size_shares: float = 0.0,
    min_order_notional_usd: float = 0.0,
    share_size_increment: float = 0.0001,
    max_order_size_usd: float | None = None,
    first_leg_order_value_usd: float | None = None,
    second_leg_max_order_value_usd: float | None = None,
    max_book_age_ms: float = 150.0,
    require_full_depth_for_fixed_dollar: bool = False,
    now: float | None = None,
) -> ArbPlan | None:
    """Build the largest equal-share YES+NO taker plan that is profitable.

    Profit condition includes fees/friction:
        1 - avg_yes_cost - avg_no_cost - leg_fees - gas - buffer >= min_edge
    """
    now = now or time.time()
    if not (0 < yes_book.ask < 1 and 0 < no_book.ask < 1):
        return None
    if max_book_age_ms > 0:
        max_age = max(now - yes_book.updated_ts, now - no_book.updated_ts) * 1000
        if max_age > max_book_age_ms:
            return None
    yes_asks = yes_book.asks or [{"price": yes_book.ask, "size": float("inf")}]
    no_asks = no_book.asks or [{"price": no_book.ask, "size": float("inf")}]
    best_friction = (
        2 * fee_per_share
        + polymarket_fee_per_share(yes_book.ask, fee_rate)
        + polymarket_fee_per_share(no_book.ask, fee_rate)
        + gas_per_share
        + stale_quote_buffer
    )
    best_sum = yes_book.ask + no_book.ask + best_friction
    if best_sum + min_edge > 1.0 + 1e-12:
        return None

    fixed_first_usd = float(first_leg_order_value_usd or 0.0)
    fixed_second_usd = float(second_leg_max_order_value_usd or 0.0)
    if fixed_first_usd > 0 and fixed_second_usd > 0:
        # Fixed-dollar live orders must be sized from a full depth snapshot.
        # A websocket top-of-book update only tells us the best price, not the
        # executable size available there.  Treating unknown top depth as
        # infinite creates $1 FOK orders that are correctly killed by the CLOB.
        if require_full_depth_for_fixed_dollar and (yes_book.asks is None or no_book.asks is None):
            return None
        # Fixed-dollar mode: buy the configured dollar value of each leg independently.
        # first_leg_order_value_usd applies to the larger-notional/more expensive leg;
        # second_leg_max_order_value_usd applies to the smaller/cheaper leg. If either
        # knob is zero, fall through to legacy equal-share offset sizing.
        yes_is_first = yes_book.ask >= no_book.ask
        yes_target = fixed_first_usd if yes_is_first else fixed_second_usd
        no_target = fixed_second_usd if yes_is_first else fixed_first_usd
        yes_fixed = _size_for_notional(yes_asks, yes_target)
        no_fixed = _size_for_notional(no_asks, no_target)
        if yes_fixed is None or no_fixed is None:
            return None
        yes_size, yes_cost, yes_limit = yes_fixed
        no_size, no_cost, no_limit = no_fixed
        total_cost = yes_cost + no_cost
        if order_limit_usd > 0 and total_cost > order_limit_usd + 1e-9:
            return None
        if min_order_notional_usd and min_order_notional_usd > 0:
            if yes_cost + 1e-9 < min_order_notional_usd or no_cost + 1e-9 < min_order_notional_usd:
                return None
        yes_avg = yes_cost / max(yes_size, 1e-9)
        no_avg = no_cost / max(no_size, 1e-9)
        friction = (
            2 * fee_per_share
            + polymarket_fee_per_share(yes_avg, fee_rate)
            + polymarket_fee_per_share(no_avg, fee_rate)
            + gas_per_share
            + stale_quote_buffer
        )
        edge = 1.0 - yes_avg - no_avg - friction
        if edge + 1e-12 < min_edge:
            return None
        first: Leg = "YES" if yes_is_first else "NO"
        second: Leg = "NO" if first == "YES" else "YES"
        return ArbPlan(
            yes_size=round(yes_size, 4),
            no_size=round(no_size, 4),
            size=round(min(yes_size, no_size), 4),
            yes_limit=round(yes_limit, 3),
            no_limit=round(no_limit, 3),
            yes_cost_est=round(yes_cost, 4),
            no_cost_est=round(no_cost, 4),
            total_cost_est=round(total_cost, 4),
            avg_sum_est=round(yes_avg + no_avg, 5),
            edge_per_pair=round(edge, 5),
            first_leg=first,
            second_leg=second,
        )

    # Initial upper bounds from visible depth and configured dollar caps.
    # `order_limit_usd` is a total YES+NO pair cap. `max_order_size_usd`
    # remains the legacy per-leg cap. `first_leg_order_value_usd`, when set,
    # sizes/caps the larger-notional first leg from a dollar value instead of
    # inheriting exchange minimum share-size metadata. `second_leg_max_order_value_usd`,
    # when set, caps the smaller-notional hedge leg; execution sends the larger leg
    # first and then the smaller leg second.
    max_depth = min(
        sum(_safe_float(x.get("size")) for x in yes_asks),
        sum(_safe_float(x.get("size")) for x in no_asks),
    )
    if max_depth <= 0:
        return None
    max_by_total_cap = order_limit_usd / max(best_sum, 1e-9) if order_limit_usd > 0 else max_depth
    max_size = min(max_depth, max_by_total_cap)
    if first_leg_order_value_usd and first_leg_order_value_usd > 0:
        max_size = min(max_size, first_leg_order_value_usd / max(max(yes_book.ask, no_book.ask), 1e-9))
    if max_order_size_usd and max_order_size_usd > 0:
        max_size = min(max_size, max_order_size_usd / max(yes_book.ask, 1e-9), max_order_size_usd / max(no_book.ask, 1e-9))
    # Do not convert the $1 per-order notional rule into a shared YES/NO share
    # floor. In skewed crypto books that turns the cheap leg into an artificial
    # 10+ share requirement and blocks otherwise viable fixed-dollar shards.
    # Fixed-dollar mode validates each leg's notional directly above; legacy
    # equal-share sizing should only respect an explicitly configured share floor.
    if min_order_size_shares > 0 and max_size + 1e-9 < min_order_size_shares:
        return None

    try:
        increment = float(share_size_increment or 0.0001)
    except Exception:
        increment = 0.0001
    if increment <= 0:
        increment = 0.0001

    candidates = _candidate_sizes(yes_asks, no_asks, max_size)

    def quantize_size(sz: float, *, up: bool = False) -> float:
        import math
        units = (math.ceil if up else math.floor)(max(0.0, sz) / increment - (0 if up else 1e-12))
        return round(units * increment, 4)

    def feasible(sz: float) -> bool:
        yc = _cost_for_size(yes_asks, sz)
        nc = _cost_for_size(no_asks, sz)
        if yc is None or nc is None:
            return False
        yes_cost, _ = yc
        no_cost, _ = nc
        total_cost = yes_cost + no_cost
        if order_limit_usd > 0 and total_cost > order_limit_usd + 1e-9:
            return False
        if min_order_notional_usd and min_order_notional_usd > 0:
            if yes_cost + 1e-9 < min_order_notional_usd or no_cost + 1e-9 < min_order_notional_usd:
                return False
        larger_cost = max(yes_cost, no_cost)
        if first_leg_order_value_usd and first_leg_order_value_usd > 0 and larger_cost > first_leg_order_value_usd + 1e-9:
            return False
        smaller_cost = min(yes_cost, no_cost)
        if second_leg_max_order_value_usd and second_leg_max_order_value_usd > 0 and smaller_cost > second_leg_max_order_value_usd + 1e-9:
            return False
        if max_order_size_usd and max_order_size_usd > 0 and (yes_cost > max_order_size_usd + 1e-9 or no_cost > max_order_size_usd + 1e-9):
            return False
        yes_avg = yes_cost / sz
        no_avg = no_cost / sz
        friction = (
            2 * fee_per_share
            + polymarket_fee_per_share(yes_avg, fee_rate)
            + polymarket_fee_per_share(no_avg, fee_rate)
            + gas_per_share
            + stale_quote_buffer
        )
        edge = 1.0 - (total_cost / sz) - friction
        return edge + 1e-12 >= min_edge

    lo = quantize_size(min_order_size_shares, up=True) if min_order_size_shares > 0 else quantize_size(increment, up=True)
    hi = quantize_size(max_size, up=False)
    if hi >= lo and feasible(lo):
        for _ in range(32):
            mid = quantize_size((lo + hi) / 2, up=False)
            if mid <= lo:
                break
            if feasible(mid):
                lo = mid
            else:
                hi = mid
        candidates.append(lo)

    for size in sorted({quantize_size(x, up=False) for x in candidates}, reverse=True):
        if size <= 0:
            continue
        if min_order_size_shares > 0 and size + 1e-9 < min_order_size_shares:
            continue
        yc = _cost_for_size(yes_asks, size)
        nc = _cost_for_size(no_asks, size)
        if yc is None or nc is None:
            continue
        yes_cost, yes_limit = yc
        no_cost, no_limit = nc
        total_cost = yes_cost + no_cost
        if order_limit_usd > 0 and total_cost > order_limit_usd + 1e-9:
            continue
        if min_order_notional_usd and min_order_notional_usd > 0:
            if yes_cost + 1e-9 < min_order_notional_usd or no_cost + 1e-9 < min_order_notional_usd:
                continue
        larger_cost = max(yes_cost, no_cost)
        if first_leg_order_value_usd and first_leg_order_value_usd > 0:
            if larger_cost > first_leg_order_value_usd + 1e-9:
                continue
        smaller_cost = min(yes_cost, no_cost)
        if second_leg_max_order_value_usd and second_leg_max_order_value_usd > 0:
            if smaller_cost > second_leg_max_order_value_usd + 1e-9:
                continue
        if max_order_size_usd and max_order_size_usd > 0:
            if yes_cost > max_order_size_usd + 1e-9 or no_cost > max_order_size_usd + 1e-9:
                continue
        avg_sum = total_cost / size
        friction = (
            2 * fee_per_share
            + polymarket_fee_per_share(yes_cost / size, fee_rate)
            + polymarket_fee_per_share(no_cost / size, fee_rate)
            + gas_per_share
            + stale_quote_buffer
        )
        edge = 1.0 - avg_sum - friction
        if edge + 1e-12 < min_edge:
            continue
        # Buy the larger-notional leg first: prove the hard/expensive side fills
        # before submitting the smaller hedge leg.
        first: Leg = "YES" if yes_cost >= no_cost else "NO"
        second: Leg = "NO" if first == "YES" else "YES"
        return ArbPlan(
            yes_size=round(size, 4),
            no_size=round(size, 4),
            size=round(size, 4),
            yes_limit=round(yes_limit, 3),
            no_limit=round(no_limit, 3),
            yes_cost_est=round(yes_cost, 4),
            no_cost_est=round(no_cost, 4),
            total_cost_est=round(total_cost, 4),
            avg_sum_est=round(avg_sum, 5),
            edge_per_pair=round(edge, 5),
            first_leg=first,
            second_leg=second,
        )
    return None


def diagnose_arb_plan_skip(
    yes_book: Book,
    no_book: Book,
    *,
    order_limit_usd: float,
    min_edge: float,
    fee_per_share: float = 0.0,
    fee_rate: float = 0.0,
    gas_per_share: float = 0.0,
    stale_quote_buffer: float = 0.0,
    min_order_size_shares: float = 0.0,
    min_order_notional_usd: float = 0.0,
    share_size_increment: float = 0.0001,
    max_order_size_usd: float | None = None,
    first_leg_order_value_usd: float | None = None,
    second_leg_max_order_value_usd: float | None = None,
    max_book_age_ms: float = 150.0,
    now: float | None = None,
) -> ArbSkipDiagnostic:
    """Explain why the current YES+NO book did not produce an executable plan.

    `opportunity_spotted` means the top-of-book YES+NO sum clears the configured
    net edge threshold before sizing constraints. It can still be unexecutable
    because of stale quotes, visible depth, min order size, or dollar caps.
    """
    now = now or time.time()
    yes_ask = float(yes_book.ask or 0.0)
    no_ask = float(no_book.ask or 0.0)
    friction = (
        2 * fee_per_share
        + polymarket_fee_per_share(yes_ask, fee_rate)
        + polymarket_fee_per_share(no_ask, fee_rate)
        + gas_per_share
        + stale_quote_buffer
    )
    raw_sum = yes_ask + no_ask
    edge = 1.0 - raw_sum - friction
    spotted = bool(0 < yes_ask < 1 and 0 < no_ask < 1 and edge + 1e-12 >= min_edge)
    details: dict[str, Any] = {
        "order_limit_usd": order_limit_usd,
        "max_order_size_usd": max_order_size_usd,
        "first_leg_order_value_usd": first_leg_order_value_usd,
        "second_leg_max_order_value_usd": second_leg_max_order_value_usd,
        "min_order_size_shares": min_order_size_shares,
        "min_order_notional_usd": min_order_notional_usd,
        "share_size_increment": share_size_increment,
        "max_book_age_ms": max_book_age_ms,
    }
    if not (0 < yes_ask < 1 and 0 < no_ask < 1):
        return ArbSkipDiagnostic("missing_quotes", spotted, yes_ask, no_ask, raw_sum, friction, edge, min_edge, details)
    if max_book_age_ms > 0:
        y_age = (now - yes_book.updated_ts) * 1000 if yes_book.updated_ts else float("inf")
        n_age = (now - no_book.updated_ts) * 1000 if no_book.updated_ts else float("inf")
        details.update({"yes_age_ms": y_age, "no_age_ms": n_age})
        if max(y_age, n_age) > max_book_age_ms:
            return ArbSkipDiagnostic("stale_quotes", spotted, yes_ask, no_ask, raw_sum, friction, edge, min_edge, details)
    if not spotted:
        return ArbSkipDiagnostic("edge_below_min", spotted, yes_ask, no_ask, raw_sum, friction, edge, min_edge, details)

    yes_asks = yes_book.asks or [{"price": yes_book.ask, "size": float("inf")}]
    no_asks = no_book.asks or [{"price": no_book.ask, "size": float("inf")}]
    yes_depth = sum(_safe_float(x.get("size")) for x in yes_asks)
    no_depth = sum(_safe_float(x.get("size")) for x in no_asks)
    max_depth = min(yes_depth, no_depth)
    best_sum = raw_sum + friction
    max_by_total_cap = order_limit_usd / max(best_sum, 1e-9) if order_limit_usd > 0 else max_depth
    max_size = min(max_depth, max_by_total_cap)
    details.update({
        "yes_depth_shares": yes_depth,
        "no_depth_shares": no_depth,
        "max_depth_shares": max_depth,
        "max_by_total_cap_shares": max_by_total_cap,
    })
    if max_depth <= 0:
        return ArbSkipDiagnostic("insufficient_depth", spotted, yes_ask, no_ask, raw_sum, friction, edge, min_edge, details)
    if max_order_size_usd and max_order_size_usd > 0:
        max_by_yes_leg = max_order_size_usd / max(yes_ask, 1e-9)
        max_by_no_leg = max_order_size_usd / max(no_ask, 1e-9)
        max_size = min(max_size, max_by_yes_leg, max_by_no_leg)
        details.update({"max_by_yes_leg_usd_shares": max_by_yes_leg, "max_by_no_leg_usd_shares": max_by_no_leg})
    if first_leg_order_value_usd and first_leg_order_value_usd > 0:
        larger_ask = max(yes_ask, no_ask)
        max_by_first_leg_cap = first_leg_order_value_usd / max(larger_ask, 1e-9)
        max_size = min(max_size, max_by_first_leg_cap)
        details["max_by_first_leg_cap_shares"] = max_by_first_leg_cap
    if second_leg_max_order_value_usd and second_leg_max_order_value_usd > 0:
        smaller_ask = min(yes_ask, no_ask)
        max_by_second_leg_cap = second_leg_max_order_value_usd / max(smaller_ask, 1e-9)
        max_size = min(max_size, max_by_second_leg_cap)
        details["max_by_second_leg_cap_shares"] = max_by_second_leg_cap
    # Do not derive an artificial min_order_size_shares from min_order_notional.
    # That is what produced dashboard messages like min_order_size_shares=10
    # when the cheap crypto leg was ~0.10. Notional is checked separately where
    # fixed-dollar sizing is used; diagnostics should not report it as a share
    # setting/limiter.
    try:
        increment = float(share_size_increment or 0.0001)
    except Exception:
        increment = 0.0001
    if increment > 0:
        import math
        min_order_size_shares = math.ceil(min_order_size_shares / increment - 1e-12) * increment
        max_size = math.floor(max_size / increment + 1e-12) * increment
        details["min_order_size_shares"] = min_order_size_shares
        details["max_feasible_before_profit_shares"] = max_size
    if min_order_size_shares > 0 and max_size + 1e-9 < min_order_size_shares:
        limiter = "min_order_size"
        if order_limit_usd > 0 and max_by_total_cap <= max_depth:
            limiter = "order_limit_usd"
        elif first_leg_order_value_usd and first_leg_order_value_usd > 0:
            limiter = "first_leg_order_value_usd"
        elif second_leg_max_order_value_usd and second_leg_max_order_value_usd > 0:
            limiter = "second_leg_max_order_value_usd"
        elif max_order_size_usd and max_order_size_usd > 0:
            limiter = "max_order_size"
        return ArbSkipDiagnostic(limiter, spotted, yes_ask, no_ask, raw_sum, friction, edge, min_edge, details)
    return ArbSkipDiagnostic("no_feasible_profitable_size", spotted, yes_ask, no_ask, raw_sum, friction, edge, min_edge, details)


CRYPTO_UPDOWN_PREFIXES = {"btc": "btc-updown", "eth": "eth-updown", "sol": "sol-updown"}

def _auto_crypto_updown(slug: str) -> tuple[str, str] | None:
    aliases = {"btc-updown-15m-auto": ("btc", "15m"), "auto:btc-updown-15m": ("btc", "15m")}
    if slug in aliases:
        return aliases[slug]
    prefix = "auto:crypto-updown:"
    if slug.startswith(prefix):
        parts = slug[len(prefix):].strip().lower().replace("_", "-").split(":")
        if len(parts) == 2 and parts[0] in CRYPTO_UPDOWN_PREFIXES and parts[1] in {"5m", "15m"}:
            return (parts[0], parts[1])
    return None

async def pick_crypto_updown_event(client: httpx.AsyncClient, asset: str = "btc", timeframe: str = "15m") -> str | None:
    asset = str(asset or "btc").lower()
    timeframe = str(timeframe or "15m").lower()
    slug_prefix = f"{CRYPTO_UPDOWN_PREFIXES.get(asset, asset + '-updown')}-{timeframe}"
    window = 900 if timeframe == "15m" else 300
    now_ts = int(time.time())
    # First try exact rolling slugs around the current window. This avoids each
    # crypto shard downloading the large tag=crypto listing and dramatically
    # reduces Cloudflare/WAF surface area.
    for start_ts in [now_ts - (now_ts % window) + (i * window) for i in (-1, 0, 1, 2)]:
        slug = f"{slug_prefix}-{start_ts}"
        try:
            events = await gamma_get_json(
                client,
                "/events",
                {"slug": slug},
                cache_key=f"event-slug:{slug}",
                cache_max_age_seconds=5.0,
                stale_max_age_seconds=max(1800.0, window * 4.0),
                attempts=2,
            )
        except Exception as e:
            logger.debug("direct crypto Gamma slug lookup failed for {}: {}", slug, e)
            continue
        if not events:
            continue
        end = _event_end_dt(events[0])
        if end is None or end <= datetime.now(timezone.utc):
            continue
        return slug

    events = await gamma_get_json(
        client,
        "/events",
        {
            "closed": "false",
            "active": "true",
            "limit": 500,
            "tag_slug": "crypto",
            "order": "endDate",
            "ascending": "true",
        },
        cache_key=f"events:crypto:{asset}:{timeframe}",
        cache_max_age_seconds=20.0,
        stale_max_age_seconds=1800.0,
        attempts=4,
    )
    now = datetime.now(timezone.utc)
    best_slug = None
    best_secs = None
    for e in events:
        slug = str(e.get("slug") or "")
        if not slug.startswith(slug_prefix + "-"):
            continue
        end = _event_end_dt(e)
        if end is None:
            continue
        secs = (end - now).total_seconds()
        if secs <= 0:
            continue
        if best_secs is None or secs < best_secs:
            best_slug = slug
            best_secs = secs
    return best_slug

async def pick_btc_updown_15m(client: httpx.AsyncClient) -> str | None:
    return await pick_crypto_updown_event(client, "btc", "15m")


HIGHER_BRACKET_ONLY_WEATHER_CITIES: set[str] = {
    "qingdao",
    "shenzhen",
}


def _weather_city_slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", str(value or "").strip().lower().replace("_", "-").replace(" ", "-")).strip("-")


def _auto_weather_city(slug: str) -> str | None:
    prefix = "auto:weather-high-temp:"
    if slug.startswith(prefix):
        city = _weather_city_slug(slug[len(prefix):])
        return city or None
    return None


def _weather_outlier_blacklist(cfg: dict[str, Any]) -> set[str]:
    raw = cfg.get("weather_outlier_blacklist", cfg.get("weather_blacklist", cfg.get("blacklist", [])))
    if raw is None:
        return set()
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            raw = parsed if isinstance(parsed, list) else raw
        except Exception:
            pass
    if isinstance(raw, str):
        items = re.split(r"[,\n]", raw)
    elif isinstance(raw, (list, tuple, set)):
        items = raw
    else:
        items = []
    return {_weather_city_slug(item) for item in items if _weather_city_slug(item)}


def _weather_outlier_city_from_cfg(cfg: dict[str, Any]) -> str:
    city = _weather_city_slug(cfg.get("weather_city"))
    if city:
        return city
    slug = str(cfg.get("market_slug") or cfg.get("market") or cfg.get("event_slug") or "")
    return _auto_weather_city(slug) or ""


def _weather_outlier_is_blacklisted(cfg: dict[str, Any]) -> bool:
    city = _weather_outlier_city_from_cfg(cfg)
    return bool(city and city in _weather_outlier_blacklist(cfg))


def _weather_outlier_higher_bracket_only_cities(cfg: dict[str, Any] | None = None) -> set[str]:
    """Cities where outlier BUYs may only be hotter than the favorite bucket."""
    out = set(HIGHER_BRACKET_ONLY_WEATHER_CITIES)
    raw = (cfg or {}).get("weather_outlier_higher_bracket_only_cities")
    if raw:
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                raw = parsed if isinstance(parsed, list) else raw
            except Exception:
                pass
        if isinstance(raw, str):
            items = re.split(r"[,\n]", raw)
        elif isinstance(raw, (list, tuple, set)):
            items = raw
        else:
            items = []
        out.update(_weather_city_slug(item) for item in items if _weather_city_slug(item))
    return out


def _weather_outlier_is_higher_bracket_only(cfg: dict[str, Any]) -> bool:
    city = _weather_outlier_city_from_cfg(cfg)
    return bool(city and city in _weather_outlier_higher_bracket_only_cities(cfg))


NWS_HEAT_ALERT_EVENTS = {
    "heat advisory",
    "excessive heat watch",
    "excessive heat warning",
    "extreme heat warning",
}


def _parse_dt_utc(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _weather_outlier_is_higher_no(temp_value: Any, winning_temp: Any) -> bool:
    try:
        return float(temp_value) > float(winning_temp) + 1e-9
    except Exception:
        return False


def _weather_nws_heat_alert_in_effect(status: dict[str, Any] | None, now: datetime | None = None) -> bool:
    if not isinstance(status, dict) or not _safe_bool(status.get("active"), False):
        return False
    now = now or datetime.now(timezone.utc)
    start = _parse_dt_utc(status.get("onset") or status.get("effective") or status.get("sent"))
    end = _parse_dt_utc(status.get("ends") or status.get("expires"))
    if start and now < start:
        return False
    if end and now > end:
        return False
    return True


def _weather_nws_heat_alert_local_date(status: dict[str, Any] | None) -> str | None:
    if not isinstance(status, dict):
        return None
    for key in ("onset", "effective", "sent"):
        raw = str(status.get(key) or "")
        if len(raw) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", raw[:10]):
            return raw[:10]
    return None


def _weather_nws_heat_alert_applies_to_event(status: dict[str, Any] | None, event_slug: str | None, now: datetime | None = None) -> bool:
    if not _weather_nws_heat_alert_in_effect(status, now):
        return False
    target_date = event_target_date(event_slug) if event_slug else None
    alert_date = _weather_nws_heat_alert_local_date(status)
    # If both dates are known, avoid blocking the next rolled daily market because
    # a same-day alert is still active for yesterday/today's settlement date.
    if target_date and alert_date:
        return str(target_date) == str(alert_date)
    return True


def _weather_nws_heat_alert_blocks_higher_no(plan_temp: Any, winning_temp: Any, status: dict[str, Any] | None, now: datetime | None = None, event_slug: str | None = None) -> bool:
    return _weather_outlier_is_higher_no(plan_temp, winning_temp) and _weather_nws_heat_alert_applies_to_event(status, event_slug, now)


def _event_end_dt(event: dict[str, Any]) -> datetime | None:
    raw = str(event.get("endDate") or event.get("end_date") or "")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


async def pick_weather_high_temp_event(client: httpx.AsyncClient, city_slug: str, *, horizon_hours: float = 72.0) -> str | None:
    """Pick the next active daily high-temperature event for a city.

    Weather shards use this as an auto-roll slug so a resolved daily event is
    replaced by the next available day without changing the strategy config.
    Use a multi-day horizon because Polymarket weather event end times are tied
    to local settlement windows; for Asia/Europe/Americas, the next tradable city
    event can be more than 36h away in UTC even though it is the next local day.
    """
    city_slug = city_slug.strip().lower().replace("_", "-").replace(" ", "-")
    wanted = f"highest-temperature-in-{city_slug}-on-"
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=horizon_hours)

    def consider_events(events: Any) -> tuple[datetime, float, str] | None:
        best: tuple[datetime, float, str] | None = None
        for e in events or []:
            if not isinstance(e, dict):
                continue
            slug = str(e.get("slug") or "")
            if not slug.startswith(wanted):
                continue
            if _safe_bool(e.get("closed"), False) or _safe_bool(e.get("archived"), False) or not _safe_bool(e.get("active"), True):
                continue
            end = _event_end_dt(e)
            if end is None or end <= now or end > cutoff:
                continue
            try:
                volume = float(e.get("volume") or e.get("volumeNum") or 0.0)
            except Exception:
                volume = 0.0
            candidate = (end, -volume, slug)
            if best is None or candidate < best:
                best = candidate
        return best

    events = await gamma_get_json(
        client,
        "/events",
        {
            "closed": "false",
            "active": "true",
            "archived": "false",
            "limit": 500,
            "tag_slug": "weather",
            "order": "endDate",
            "ascending": "true",
        },
        cache_key=f"events:weather:{city_slug}",
        cache_max_age_seconds=60.0,
        stale_max_age_seconds=6 * 3600.0,
        attempts=4,
    )
    best = consider_events(events)
    if best is not None:
        return best[2]

    # The tag-filtered weather list is capped and can omit otherwise active city
    # events.  Fall back to exact slug probes for the next few UTC dates so a city
    # shard does not go dark just because its event was outside the first 500 tag
    # results.
    exact_events: list[dict[str, Any]] = []
    for day_offset in range(0, 4):
        d = (now + timedelta(days=day_offset)).date()
        slug = f"highest-temperature-in-{city_slug}-on-{d.strftime('%B').lower()}-{d.day}-{d.year}"
        try:
            got = await gamma_get_json(
                client,
                "/events",
                {"slug": slug},
                cache_key=f"event:slug:{slug}",
                cache_max_age_seconds=60.0,
                stale_max_age_seconds=6 * 3600.0,
                attempts=2,
            )
        except Exception as exc:
            logger.debug("weather exact-slug fallback failed for {}: {}", slug, exc)
            continue
        if isinstance(got, list):
            exact_events.extend(e for e in got if isinstance(e, dict))
    best = consider_events(exact_events)
    return best[2] if best else None


def is_auto_roll_slug(slug: str | None) -> bool:
    if not slug:
        return False
    return _auto_crypto_updown(str(slug)) is not None or _auto_weather_city(str(slug)) is not None


def _temperature_to_celsius(value: float, unit: str | None) -> float:
    unit = (unit or "").strip().lower()
    if unit.startswith("f"):
        return (value - 32.0) * 5.0 / 9.0
    return value


def weather_temperature_value(pair: dict[str, Any]) -> float | None:
    """Extract a representative temperature value from a weather option title.

    Weather high-temp events are listed as many binary options around degree
    values/ranges. The outlier strategy only needs an ordered numeric value so it
    can compare every option to the currently highest-priced/winning option.
    """
    text = " ".join(str(pair.get(k) or "") for k in ("title", "slug"))
    lowered = text.lower().replace("°", " ")
    # Prefer explicit Celsius/Fahrenheit degree mentions and common range forms.
    # Normalize Fahrenheit markets to Celsius before comparing distances so a
    # 4-degree setting always means 4°C regardless of how the event is labeled.
    natural_range = re.search(r"between\s+(-?\d+(?:\.\d+)?)\s*(?:-|and|to)\s*(-?\d+(?:\.\d+)?)\s*(c|f|degrees?|deg)?", lowered)
    if natural_range:
        unit = natural_range.group(3)
        a = _temperature_to_celsius(float(natural_range.group(1)), unit)
        b = _temperature_to_celsius(float(natural_range.group(2)), unit)
        return (a + b) / 2.0
    # Avoid interpreting slug separators as negative signs (e.g. temp-17c is +17, not -17).
    nums = [(_temperature_to_celsius(float(x), unit), unit) for x, unit in re.findall(r"(?<![A-Za-z0-9])(-?\d+(?:\.\d+)?)\s*(c|f|degrees?|deg|°)", lowered)]
    if len(nums) >= 2 and re.search(r"between|from|to|-", lowered):
        return (nums[0][0] + nums[1][0]) / 2.0
    if nums:
        return nums[0][0]
    # Slug fallback: weather high-temp markets typically encode the option value
    # immediately after words such as between/less-than/at-least/or-more.
    m = re.search(r"between-(-?\d+(?:\.\d+)?)(?:-and)?-(-?\d+(?:\.\d+)?)(?:-(c|f))?", lowered)
    if m:
        unit = m.group(3)
        return (_temperature_to_celsius(float(m.group(1)), unit) + _temperature_to_celsius(float(m.group(2)), unit)) / 2.0
    m = re.search(r"(?:less-than|under|below|at-least|more-than|or-more|above)-(-?\d+(?:\.\d+)?)(?:-(c|f))?", lowered)
    if m:
        return _temperature_to_celsius(float(m.group(1)), m.group(2))
    return None


def weather_prediction_price(book: Book) -> float:
    """Reference YES price used to identify the currently winning weather value.

    Use the best bid when available because the dashboard/Polymarket displayed
    "currently winning" value follows executable demand, not a stray high ask.
    Fall back to midpoint/ask only when bids are absent.
    """
    if 0 < book.bid < 1:
        return book.bid
    if 0 < book.bid < 1 and 0 < book.ask < 1:
        return (book.bid + book.ask) / 2.0
    if 0 < book.ask < 1:
        return book.ask
    return 0.0


def _parse_binary_market(ev: dict[str, Any], m: dict[str, Any], event_slug: str) -> dict[str, Any] | None:
    toks = m.get("clobTokenIds")
    outs = m.get("outcomes")
    if isinstance(toks, str):
        toks = json.loads(toks)
    if isinstance(outs, str):
        outs = json.loads(outs)
    if not toks or len(toks) < 2:
        return None
    try:
        order_min_size = float(m.get("orderMinSize") or 0)
    except Exception:
        order_min_size = 0.0
    market_slug = str(m.get("slug") or event_slug)
    question = str(m.get("question") or ev.get("title") or market_slug)
    yes_label = str(outs[0] if outs else "YES")
    no_label = str(outs[1] if outs else "NO")
    return {
        "event_slug": event_slug,
        "slug": market_slug,
        "title": question,
        "event_title": ev.get("title") or question,
        "yes_token": str(toks[0]),
        "no_token": str(toks[1]),
        "yes_label": yes_label,
        "no_label": no_label,
        "order_min_size": order_min_size,
        "tick_size": _market_tick_size(m),
        "neg_risk": _safe_bool(m.get("negRisk") or m.get("neg_risk"), False),
        "condition_id": m.get("conditionId") or m.get("condition_id"),
        "end_date": m.get("endDate") or ev.get("endDate"),
    }


async def resolve_event_pairs(slug: str, *, all_markets: bool = False) -> list[dict[str, Any]]:
    timeout = httpx.Timeout(15.0, connect=3.0)
    limits = httpx.Limits(max_keepalive_connections=4, max_connections=8, keepalive_expiry=30.0)
    async with httpx.AsyncClient(timeout=timeout, limits=limits, http2=True) as c:
        crypto_auto = _auto_crypto_updown(slug)
        if crypto_auto:
            asset, timeframe = crypto_auto
            picked = await pick_crypto_updown_event(c, asset, timeframe)
            if not picked:
                raise ValueError(f"no live {asset}-updown-{timeframe} market available")
            slug = picked
        city = _auto_weather_city(slug)
        if city:
            picked = await pick_weather_high_temp_event(c, city)
            if not picked:
                raise ValueError(f"no active high-temperature weather event available for city={city}")
            slug = picked
        events = await gamma_get_json(
            c,
            "/events",
            {"slug": slug},
            cache_key=f"event-slug:{slug}",
            cache_max_age_seconds=15.0,
            stale_max_age_seconds=7200.0,
            attempts=4,
        )
    if not events:
        raise ValueError(f"no event found for slug={slug}")
    ev = events[0]
    markets = ev.get("markets") or []
    if not markets:
        raise ValueError(f"event has no markets: {slug}")
    pairs: list[dict[str, Any]] = []
    for m in markets:
        pair = _parse_binary_market(ev, m, str(ev.get("slug") or slug))
        if pair:
            pairs.append(pair)
        if pair and not all_markets:
            break
    if not pairs:
        raise ValueError(f"event has no binary CLOB markets: {slug}")
    return pairs


async def resolve_event(slug: str) -> dict[str, Any]:
    return (await resolve_event_pairs(slug, all_markets=False))[0]


async def rest_book_full(client: httpx.AsyncClient, token_id: str) -> Book | None:
    try:
        r = await client.get(f"{CLOB_REST}/book", params={"token_id": token_id}, timeout=5)
        if r.status_code != 200:
            return None
        b = r.json()
        bids = _parse_levels(b.get("bids", []))
        asks = _parse_levels(b.get("asks", []))
        # BUY entry logic needs executable asks, but take-profit SELL logic needs
        # bid-only snapshots. Near-certain weather legs can have NO bids at 0.999
        # with no NO asks; dropping those snapshots hides sellable inventory from
        # the take-profit scanner.
        if not bids and not asks:
            return None
        return Book(
            bid=max((x["price"] for x in bids), default=0.0),
            ask=min((x["price"] for x in asks), default=0.0),
            bids=bids,
            asks=asks,
            updated_ts=time.time(),
            tick_size=str(b.get("tick_size") or b.get("minimum_tick_size") or "") or None,
            neg_risk=_safe_bool(b.get("neg_risk"), False) if b.get("neg_risk") is not None else None,
            order_min_size=_safe_float(b.get("min_order_size"), 0.0) or None,
        )
    except Exception:
        return None


async def rest_books_full(
    client: httpx.AsyncClient,
    token_ids: list[str],
    *,
    split_on_failure: bool = True,
) -> dict[str, Book]:
    """Fetch full L2 books for many CLOB token ids in one request.

    Polymarket documents POST /books for batching order-book snapshots. This is
    the practical way to keep multi-option weather events fresh: one keep-alive
    HTTP request can refresh all YES/NO books instead of waiting for each sparse
    market to emit a websocket delta.

    Weather events can contain stale/expired tokens during daily rollover.  A
    single bad token or transient 400 must not age an entire city shard's book
    cache, so non-429 failures are bisected to salvage valid tokens.  Rate-limit
    responses deliberately do not split because that would multiply request load.
    """
    tokens = list(dict.fromkeys(str(t) for t in token_ids if str(t or "").strip()))
    if not tokens:
        return {}
    status_code: int | None = None
    try:
        r = await client.post(f"{CLOB_REST}/books", json=[{"token_id": t} for t in tokens], timeout=2.5)
        status_code = int(getattr(r, "status_code", 0) or 0)
        if status_code != 200:
            reason = str(getattr(r, "text", ""))[:200]
            if status_code == 429:
                logger.warning("REST /books rate limited batch size={} reason={}", len(tokens), reason)
                return {}
            if split_on_failure and len(tokens) > 1:
                mid = max(1, len(tokens) // 2)
                left, right = await asyncio.gather(
                    rest_books_full(client, tokens[:mid], split_on_failure=split_on_failure),
                    rest_books_full(client, tokens[mid:], split_on_failure=split_on_failure),
                )
                merged = dict(left)
                merged.update(right)
                return merged
            logger.warning("REST /books skipped batch size={} status={} reason={}", len(tokens), status_code, reason)
            return {}
        now = time.time()
        out: dict[str, Book] = {}
        for raw in r.json() or []:
            if not isinstance(raw, dict):
                continue
            tok = str(raw.get("asset_id") or raw.get("token_id") or "")
            bids = _parse_levels(raw.get("bids", []))
            asks = _parse_levels(raw.get("asks", []))
            if not tok or (not bids and not asks):
                continue
            out[tok] = Book(
                bid=max((x["price"] for x in bids), default=0.0),
                ask=min((x["price"] for x in asks), default=0.0),
                bids=bids,
                asks=asks,
                updated_ts=now,
                tick_size=str(raw.get("tick_size") or raw.get("minimum_tick_size") or "") or None,
                neg_risk=_safe_bool(raw.get("neg_risk"), False) if raw.get("neg_risk") is not None else None,
                order_min_size=_safe_float(raw.get("min_order_size"), 0.0) or None,
            )
        return out
    except Exception as exc:
        if split_on_failure and len(tokens) > 1:
            mid = max(1, len(tokens) // 2)
            left, right = await asyncio.gather(
                rest_books_full(client, tokens[:mid], split_on_failure=split_on_failure),
                rest_books_full(client, tokens[mid:], split_on_failure=split_on_failure),
            )
            merged = dict(left)
            merged.update(right)
            return merged
        logger.warning("REST /books exception batch size={} error={}", len(tokens), exc)
        return {}


def _merge_cfg(file_cfg: dict[str, Any], db_cfg: dict[str, Any]) -> dict[str, Any]:
    merged = {**file_cfg, **(db_cfg or {})}
    if "market" in merged and "market_slug" not in merged:
        merged["market_slug"] = merged["market"]
    return merged


class ArbSniperRunner:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.file_cfg = yaml.safe_load(config_path.read_text()) or {}
        self.strategy_id = str(self.file_cfg.get("id") or "live_arb_sniper_btc15m_v1")
        self.name = str(self.file_cfg.get("name") or "Live · YES/NO Arb Sniper")
        self.writer = PolybotWriter(os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL"))
        self.exec_client: PolymarketExecutionClient | None = None
        self.stop = asyncio.Event()
        self.books: dict[Leg, Book] = {"YES": Book(), "NO": Book()}
        self.books_by_slug: dict[str, dict[Leg, Book]] = {}
        self.market_pairs: list[dict[str, Any]] = []
        self.ev: dict[str, Any] | None = None
        self.executed_attempts = 0
        self.fill_seq = int(time.time() * 1000) % 10_000_000
        self.last_trade_ts = 0.0
        self.trading_lock = asyncio.Lock()
        self.market_reload = asyncio.Event()
        self.cached_status = "stopped"
        self.residual_inventory: dict[Leg, float] = {"YES": 0.0, "NO": 0.0}
        self.last_health_log_ts = 0.0
        self.last_logged_status: str | None = None
        self.rest_book_latency = LatencyStats()
        self.submit_latency = LatencyStats()
        self.ws_update_count = 0
        self.rest_book_refresh_count = 0
        self.last_rest_requested_tokens = 0
        self.last_rest_returned_tokens = 0
        self.last_rest_missing_tokens: list[str] = []
        self.last_rest_missing_labels: list[str] = []
        self.last_gc_collect_ts = 0.0
        self.last_skip_log: dict[str, float] = {}
        self.last_dashboard_book_write_ts = 0.0
        self.dashboard_book_write_task: asyncio.Task[None] | None = None
        self.weather_outlier_order_pause_until = 0.0
        self.weather_outlier_order_pause_reason = ""
        self.weather_outlier_market_cooldown_until: dict[str, float] = {}
        self.weather_outlier_hot_until_by_slug: dict[str, float] = {}
        self.weather_outlier_local_positions: dict[str, float] = {}
        self.weather_outlier_last_take_profit_ts: dict[str, float] = {}
        self.weather_outlier_legacy_market_meta: dict[str, dict[str, Any]] = {}
        self.weather_safety_status: dict[str, Any] | None = None
        self.weather_safety_last_check_ts = 0.0
        self.weather_safety_cache_key: tuple[str, str | None] | None = None
        self.weather_nws_heat_alert_status: dict[str, Any] | None = None
        self.weather_nws_heat_alert_last_check_ts = 0.0
        self.weather_nws_station_point: tuple[float, float] | None = None
        self._ws_subscription_logged = False
        self.crypto_fair_model = CryptoFairPriceModel()
        self.crypto_ref_prices: deque[float] = deque(maxlen=900)
        self.crypto_ref_price = 0.0
        self.crypto_ref_second = 0
        self.crypto_ref_event_ms = 0
        self.crypto_start_price = 0.0
        self.crypto_start_ts = 0
        self.crypto_fair_cache_key: tuple[int, int, int, int, int, int] | None = None
        self.crypto_fair_cache: FairPriceSnapshot | None = None

    async def setup(self) -> None:
        await self.writer.connect()
        initial_cfg = dict(self.file_cfg)
        market_slug = initial_cfg.get("market_slug") or initial_cfg.get("market") or initial_cfg.get("event_slug") or "btc-updown-15m-auto"
        await self._ensure_strategy_row(str(market_slug), initial_cfg)
        self.exec_client = PolymarketExecutionClient()
        if _safe_bool(initial_cfg.get("warm_execution_client", True), True):
            # Build/derive CLOB credentials before the first opportunity so the
            # first hot-path order does not pay API-key/client initialisation cost.
            try:
                _ = self.exec_client.http.clob
                logger.info("Polymarket execution client warmed")
            except Exception as e:
                logger.warning("Polymarket execution client warmup failed: {}", e)

    async def _ensure_strategy_row(self, market: str, cfg: dict[str, Any]) -> None:
        import json as _json
        async with self.writer._pool.acquire() as con:  # type: ignore[attr-defined]
            await con.execute(
                """
                INSERT INTO strategies(id, name, kind, market, status, config, mode)
                VALUES ($1, $2, $6, $3, 'stopped', $4::jsonb, $5)
                ON CONFLICT (id) DO UPDATE
                SET name=$2,
                    kind=$6,
                    market=COALESCE(NULLIF(strategies.market, ''), $3),
                    status=strategies.status,
                    -- file cfg supplies defaults only; dashboard/DB values win on conflicts
                    config=$4::jsonb || strategies.config,
                    mode=COALESCE(NULLIF(strategies.mode, ''), $5),
                    updated_at=now()
                """,
                self.strategy_id, self.name, market, _json.dumps(cfg), str(cfg.get("mode") or "live"), str(cfg.get("kind") or "binary_arb_sniper"),
            )

    async def current_state(self) -> tuple[dict[str, Any], str]:
        async with self.writer._pool.acquire() as con:  # type: ignore[attr-defined]
            row = await con.fetchrow("SELECT config, status FROM strategies WHERE id=$1", self.strategy_id)
        if not row:
            return dict(self.file_cfg), "stopped"
        db_cfg = row["config"] or {}
        if isinstance(db_cfg, str):
            db_cfg = json.loads(db_cfg)
        status = str(row["status"] or "stopped")
        return _merge_cfg(self.file_cfg, db_cfg), status

    async def current_cfg(self) -> dict[str, Any]:
        cfg, status = await self.current_state()
        self.cached_status = status
        return cfg

    async def load_market(self, cfg: dict[str, Any]) -> None:
        slug = cfg.get("market_slug") or cfg.get("market") or cfg.get("event_slug")
        if not slug:
            raise ValueError("Arb sniper requires market_slug in config/settings")
        previous_key = ",".join(p.get("slug", "") for p in self.market_pairs) if self.market_pairs else (self.ev.get("slug") if self.ev else None)
        all_markets = bool(cfg.get("monitor_all_markets") or cfg.get("event_all_markets") or cfg.get("market_mode") == "event_all_binary_markets")
        pairs = await resolve_event_pairs(str(slug), all_markets=all_markets)
        self.market_pairs = pairs
        self.ev = pairs[0]
        self.books_by_slug = {p["slug"]: {"YES": Book(), "NO": Book()} for p in pairs}
        # Preserve legacy aliases for single-pair paths/tests.
        self.books = self.books_by_slug[self.ev["slug"]]
        title = str(pairs[0].get("event_title") or pairs[0]["title"])
        market_label = title if len(pairs) == 1 else f"{title} · {len(pairs)} options"
        async with self.writer._pool.acquire() as con:  # type: ignore[attr-defined]
            await con.execute("UPDATE strategies SET market=$2, updated_at=now() WHERE id=$1", self.strategy_id, market_label[:100])
            await con.execute("DELETE FROM book_latest WHERE strategy_id=$1", self.strategy_id)
        asset_ids: list[str] = []
        for pair in pairs:
            asset_ids.extend([pair["yes_token"], pair["no_token"]])
        async with httpx.AsyncClient(timeout=httpx.Timeout(2.5, connect=1.0), limits=httpx.Limits(max_keepalive_connections=4, max_connections=8, keepalive_expiry=30.0)) as c:
            t0 = time.perf_counter()
            snapshots = await rest_books_full(c, asset_ids)
            self.rest_book_latency.add((time.perf_counter() - t0) * 1000)
            for token, book in snapshots.items():
                self._apply_full_book(token, book)
            if snapshots:
                self.rest_book_refresh_count += 1
        self._configure_crypto_fair_reference(cfg)
        if _safe_bool(cfg.get("fair_model_enabled", False), False):
            try:
                await self._refresh_crypto_start_price(cfg)
            except Exception as e:
                logger.warning("crypto fair-model start-price refresh failed: {}", e)
        await self._write_book_rows()
        current_key = ",".join(p.get("slug", "") for p in pairs)
        if previous_key and previous_key != current_key:
            self.market_reload.set()
        await self.writer.log_strategy_event(self.strategy_id, f"Arb sniper market loaded: {market_label} ({str(slug)}) monitoring {len(pairs)} binary option(s)")

    def _crypto_asset(self, cfg: dict[str, Any]) -> str:
        raw = str(cfg.get("crypto_asset") or "btc").strip().lower()
        return raw if raw in {"btc", "eth", "sol"} else "btc"

    def _binance_symbol(self, cfg: dict[str, Any]) -> str:
        return {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT"}.get(self._crypto_asset(cfg), "BTCUSDT")

    def _configure_crypto_fair_reference(self, cfg: dict[str, Any]) -> None:
        pair = self.ev or {}
        slug = str(pair.get("event_slug") or pair.get("slug") or cfg.get("market_slug") or "")
        m = re.search(r"-(\d{10})$", slug)
        start_ts = int(m.group(1)) if m else 0
        if start_ts and start_ts != self.crypto_start_ts:
            self.crypto_start_ts = start_ts
            self.crypto_start_price = 0.0
            self.crypto_ref_prices.clear()
            self.crypto_fair_cache_key = None
            self.crypto_fair_cache = None
        self.crypto_fair_model = CryptoFairPriceModel(
            fallback_sigma=float(cfg.get("fair_model_fallback_sigma", 0.80) or 0.80),
            vol_floor=float(cfg.get("fair_model_vol_floor", 0.05) or 0.05),
            vol_cap=float(cfg.get("fair_model_vol_cap", 5.0) or 5.0),
            ewma_lambda=float(cfg.get("fair_model_ewma_lambda", 0.94) or 0.94),
            winsor_sigma=float(cfg.get("fair_model_winsor_sigma", 6.0) or 6.0),
            latency_buffer_seconds=float(cfg.get("fair_model_latency_buffer_seconds", 0.0) or 0.0),
        )

    async def _refresh_crypto_start_price(self, cfg: dict[str, Any]) -> None:
        if not self.crypto_start_ts:
            return
        symbol = self._binance_symbol(cfg)
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": symbol, "interval": "1s", "startTime": self.crypto_start_ts * 1000, "limit": 1}
        rows = []
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(3.0, connect=1.0)) as client:
                r = await client.get(url, params=params)
                r.raise_for_status()
                rows = r.json()
        except Exception as e:
            logger.warning("Binance start-price refresh failed for {} @ {}: {}; trying metadata fallback", symbol, self.crypto_start_ts, e)
        if rows:
            # Prefer the one-second open at market start, matching the backtest strike proxy.
            px = _safe_float(rows[0][1])
            if px > 0:
                self.crypto_start_price = px
                self.crypto_fair_cache_key = None
                self.crypto_fair_cache = None
                return
        # Gamma market metadata often includes the official price-to-beat/start
        # price.  Use it as a safe fallback so a transient Binance REST failure
        # does not leave a live shard permanently unable to evaluate fair value.
        fallback = _safe_float((self.ev or {}).get("price_to_beat") or (self.ev or {}).get("priceToBeat"))
        if fallback > 0:
            self.crypto_start_price = fallback
            self.crypto_fair_cache_key = None
            self.crypto_fair_cache = None

    def _current_fair_snapshot(self, cfg: dict[str, Any]) -> FairPriceSnapshot | None:
        if not _safe_bool(cfg.get("fair_model_enabled", False), False):
            return None
        if self.crypto_start_price <= 0 or self.crypto_ref_price <= 0:
            return None
        max_ref_age_ms = float(cfg.get("fair_model_max_ref_age_ms", cfg.get("max_book_age_ms", 2500)) or 2500)
        if self.crypto_ref_event_ms > 0 and max_ref_age_ms > 0:
            if time.time() * 1000.0 - float(self.crypto_ref_event_ms) > max_ref_age_ms:
                return None
        end_raw = str((self.ev or {}).get("end_date") or "")
        try:
            end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00")) if end_raw else None
        except Exception:
            end_dt = None
        seconds_left = (end_dt - datetime.now(timezone.utc)).total_seconds() if end_dt else 0.0
        sample_seconds = float(cfg.get("fair_model_sample_seconds", 1.0) or 1.0)
        # Hot-path cache: volatility scans the rolling price deque, so compute at
        # most once per Binance trade-second/config bucket.  Book updates can then
        # evaluate several CLOB plans without rebuilding the same fair snapshot.
        key = (
            int(self.crypto_start_ts),
            int(self.crypto_ref_second),
            int(round(self.crypto_start_price * 100)),
            int(round(self.crypto_ref_price * 100)),
            int(round(seconds_left)),
            int(round(sample_seconds * 1000)),
        )
        if self.crypto_fair_cache_key == key and self.crypto_fair_cache is not None:
            return self.crypto_fair_cache
        snap = self.crypto_fair_model.price(
            start_price=self.crypto_start_price,
            current_price=self.crypto_ref_price,
            seconds_to_expiry=seconds_left,
            recent_prices=list(self.crypto_ref_prices),
            sample_seconds=sample_seconds,
        )
        self.crypto_fair_cache_key = key
        self.crypto_fair_cache = snap
        return snap

    def _fair_model_accepts_plan(self, plan: ArbPlan, cfg: dict[str, Any]) -> bool:
        if not _safe_bool(cfg.get("fair_model_enabled", False), False):
            return True
        fair = self._current_fair_snapshot(cfg)
        if fair is None:
            return False
        return fair_model_accepts_arb_plan(
            plan,
            fair,
            min_model_edge=float(cfg.get("fair_model_min_edge", 0.0) or 0.0),
            max_leg_overpay=float(cfg.get("fair_model_max_leg_overpay", 0.0) or 0.0),
        )

    def _seconds_to_end(self, pair: dict[str, Any] | None = None) -> float | None:
        raw = str((pair or self.ev or {}).get("end_date") or "")
        if not raw:
            return None
        try:
            end_dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
        return (end_dt - datetime.now(timezone.utc)).total_seconds()

    def _entry_window_open(self, cfg: dict[str, Any], pair: dict[str, Any] | None = None) -> bool:
        min_left = float(cfg.get("min_seconds_to_expiry_for_entry", 0.0) or 0.0)
        if min_left <= 0:
            return True
        seconds_left = self._seconds_to_end(pair)
        return seconds_left is None or seconds_left >= min_left

    async def crypto_reference_loop(self, cfg_ref: dict[str, Any]) -> None:
        """Low-latency Binance reference stream for fair-value gating."""
        while not self.stop.is_set():
            cfg = cfg_ref
            symbol = self._binance_symbol(cfg).lower()
            url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
            try:
                async with websockets.connect(url, ping_interval=10, ping_timeout=5, close_timeout=1, max_queue=1, compression=None) as ws:
                    async for raw in ws:
                        if self.stop.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        px = _safe_float(msg.get("p"))
                        if px > 0:
                            self.crypto_ref_price = px
                            event_ms = int(_safe_float(msg.get("E")) or time.time() * 1000)
                            self.crypto_ref_event_ms = event_ms
                            sec = max(1, event_ms // 1000)
                            if sec != self.crypto_ref_second:
                                self.crypto_ref_second = sec
                                self.crypto_ref_prices.append(px)
                                self.crypto_fair_cache_key = None
                                self.crypto_fair_cache = None
                            elif self.crypto_ref_prices:
                                # Keep one-second close parity with backtests.
                                self.crypto_ref_prices[-1] = px
                                self.crypto_fair_cache_key = None
                                self.crypto_fair_cache = None
            except Exception as e:
                logger.debug("crypto reference websocket error: {}", e)
                await asyncio.sleep(1.0)

    def _book_for_token(self, token: str) -> tuple[dict[str, Any], Leg, Book] | None:
        for pair in self.market_pairs or ([] if self.ev is None else [self.ev]):
            books = self.books_by_slug.get(pair["slug"], self.books)
            if token == pair["yes_token"]:
                return pair, "YES", books["YES"]
            if token == pair["no_token"]:
                return pair, "NO", books["NO"]
        return None

    def _apply_full_book(self, token: str, snapshot: Book) -> bool:
        found = self._book_for_token(token)
        if not found:
            return False
        _pair, _leg, book = found
        book.bid = snapshot.bid
        book.ask = snapshot.ask
        book.bids = snapshot.bids
        book.asks = snapshot.asks
        book.updated_ts = snapshot.updated_ts or time.time()
        return True

    def _update_top_of_book(self, token: str, bid: float, ask: float) -> bool:
        found = self._book_for_token(token)
        # Extreme/skewed crypto outcomes often have no bid while the ask side is
        # still live/executable.  Do not reject ask-only websocket updates with
        # best_bid=0, otherwise one leg can remain stale for minutes and block
        # otherwise valid opportunities behind `reason=stale_quotes`.
        if not found or not (0 <= bid <= ask < 1 and ask > 0):
            return False
        _pair, _leg, book = found
        ask_changed = abs(book.ask - ask) > 1e-9
        book.bid = bid
        book.ask = ask
        # Top-of-book websocket updates do not include the full depth ladder.
        # Drop old ladders so any immediate hot-path decision only uses the
        # exact top ask as executable depth until REST /books refreshes depth.
        if ask_changed:
            book.asks = None
            book.bids = None
        book.updated_ts = time.time()
        return True

    def _subscription_payload(self, asset_ids: list[str], cfg: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "market",
            "assets_ids": asset_ids,
            "custom_feature_enabled": bool(cfg.get("websocket_custom_features", True)),
        }

    async def _write_book_rows(self, *, record_ticks: bool = True) -> None:
        pairs = self.market_pairs or ([] if self.ev is None else [self.ev])
        if not pairs:
            return
        rows: list[dict[str, Any]] = []
        ticks: list[tuple[str, str, float, float]] = []
        for pair in pairs:
            books = self.books_by_slug.get(pair["slug"], self.books)
            option = str(pair.get("title") or pair.get("slug") or "")[:80]
            for leg, token, label in (
                ("YES", pair["yes_token"], f"{option} · {pair['yes_label']}"),
                ("NO", pair["no_token"], f"{option} · {pair['no_label']}"),
            ):
                b = books[leg]
                bids = sorted(({"px": x["price"], "sz": x["size"]} for x in (b.bids or [])), key=lambda x: -x["px"])[:10]
                asks = sorted(({"px": x["price"], "sz": x["size"]} for x in (b.asks or [])), key=lambda x: x["px"])[:10]
                rows.append({
                    "strategy_id": self.strategy_id,
                    "token": token,
                    "label": label,
                    "bids": bids,
                    "asks": asks,
                    "best_bid": b.bid,
                    "best_ask": b.ask,
                })
                ticks.append((token, label, b.bid, b.ask))
        bulk_upsert = getattr(self.writer, "upsert_books", None)
        if bulk_upsert is not None:
            await bulk_upsert(rows)
        else:  # compatibility for tests/custom writers
            for row in rows:
                await self.writer.upsert_book(
                    row["strategy_id"], row["token"], row["label"], row["bids"], row["asks"], row["best_bid"], row["best_ask"]
                )
        if record_ticks:
            for token, label, bid, ask in ticks:
                await self.writer.record_tick(self.strategy_id, token, label, bid, ask)

    def _schedule_dashboard_book_write(self, cfg: dict[str, Any]) -> None:
        """Keep dashboard book_latest fresh without putting DB writes on hot path."""
        interval_ms = float(cfg.get("dashboard_book_write_ms", cfg.get("book_dashboard_write_ms", 250)) or 0)
        if interval_ms <= 0:
            return
        if self.dashboard_book_write_task is not None and not self.dashboard_book_write_task.done():
            return
        now = time.time()
        if (now - self.last_dashboard_book_write_ts) * 1000 < interval_ms:
            return
        self.last_dashboard_book_write_ts = now

        async def _run() -> None:
            try:
                await self._write_book_rows(record_ticks=False)
            except Exception as e:
                logger.debug("dashboard book write error: {}", e)

        self.dashboard_book_write_task = asyncio.create_task(_run())

    async def _flush_state(self, cfg: dict[str, Any], pnl: float = 0.0) -> None:
        market = (self.ev.get("event_title") or self.ev["title"])[:80] if self.ev else str(cfg.get("market_slug") or "arb")
        sums = []
        for pair in self.market_pairs or ([] if self.ev is None else [self.ev]):
            books = self.books_by_slug.get(pair["slug"], self.books)
            s = (books["YES"].ask or 0) + (books["NO"].ask or 0)
            if s > 0:
                sums.append(s)
        last_sum = min(sums) if sums else ((self.books["YES"].ask or 0) + (self.books["NO"].ask or 0))
        await self.writer.upsert_position(
            self.strategy_id,
            market,
            "SCANNING",
            float(cfg.get("order_limit_usd", cfg.get("max_order_size", 1.0)) or 0),
            float(cfg.get("min_edge", 0.002) or 0),
            round(last_sum, 4),
            pnl,
        )
        await self.writer.snapshot_equity(self.strategy_id, round(STARTING_CASH + pnl, 2))

    def _plan_from_cfg(self, cfg: dict[str, Any], pair: dict[str, Any] | None = None) -> ArbPlan | None:
        pair = pair or self.ev
        if not pair:
            return None
        books = self.books_by_slug.get(pair["slug"], self.books)
        # Do not inherit Polymarket's market minimum share metadata (often 5 shares)
        # into arb sizing. For live crypto shards, size is derived from configured
        # dollar knobs such as first_leg_order_value_usd instead.
        min_order_size = float(cfg.get("min_order_size_shares") or 0.0)
        # Marketable Polymarket BUY orders have a hard per-order $1 minimum.
        # Enforce that before submit for every live paired arb leg.  Earlier
        # crypto/weather configs allowed sub-min notional to "log attempts",
        # but that produces guaranteed CLOB rejects like `$0.96, min size: $1`.
        min_order_notional = float(cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0)) or 1.0)
        if min_order_notional <= 0:
            min_order_notional = 1.0
        share_increment = float(cfg.get("share_size_increment", 1.0 if cfg.get("use_limit_fok", True) else 0.0001) or 0.0001)
        plan = build_arb_plan(
            books["YES"],
            books["NO"],
            order_limit_usd=float(cfg.get("order_limit_usd", cfg.get("max_order_size", 1.0)) or 1.0),
            min_edge=float(cfg.get("min_edge", 0.002) or 0.0),
            fee_per_share=float(cfg.get("fee_per_share", 0.0) or 0.0),
            fee_rate=float(cfg.get("polymarket_taker_fee_rate", cfg.get("fee_rate", 0.0)) or 0.0),
            gas_per_share=float(cfg.get("merge_gas_per_share", cfg.get("gas_per_share", 0.0)) or 0.0),
            stale_quote_buffer=float(cfg.get("stale_quote_buffer", 0.0) or 0.0),
            min_order_size_shares=min_order_size,
            min_order_notional_usd=min_order_notional,
            share_size_increment=share_increment,
            max_order_size_usd=float(cfg.get("max_order_size", 0) or 0) or None,
            first_leg_order_value_usd=float(cfg.get("first_leg_order_value_usd", cfg.get("first_leg_order_value", 0)) or 0) or None,
            second_leg_max_order_value_usd=float(cfg.get("second_leg_max_order_value_usd", cfg.get("second_leg_max_order_value", 0)) or 0) or None,
            max_book_age_ms=float(cfg.get("max_book_age_ms", 150) if cfg.get("max_book_age_ms") is not None else 150),
            require_full_depth_for_fixed_dollar=bool(cfg.get("require_full_depth_for_fixed_dollar", True)),
        )
        if plan is not None and not self._fair_model_accepts_plan(plan, cfg):
            return None
        return plan

    def _skip_diagnostic_from_cfg(self, cfg: dict[str, Any], pair: dict[str, Any] | None = None) -> ArbSkipDiagnostic | None:
        pair = pair or self.ev
        if not pair:
            return None
        books = self.books_by_slug.get(pair["slug"], self.books)
        min_order_size = float(cfg.get("min_order_size_shares") or 0.0)
        min_order_notional = float(cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0)) or 1.0)
        if min_order_notional <= 0:
            min_order_notional = 1.0
        share_increment = float(cfg.get("share_size_increment", 1.0 if cfg.get("use_limit_fok", True) else 0.0001) or 0.0001)
        return diagnose_arb_plan_skip(
            books["YES"],
            books["NO"],
            order_limit_usd=float(cfg.get("order_limit_usd", cfg.get("max_order_size", 1.0)) or 1.0),
            min_edge=float(cfg.get("min_edge", 0.002) or 0.0),
            fee_per_share=float(cfg.get("fee_per_share", 0.0) or 0.0),
            fee_rate=float(cfg.get("polymarket_taker_fee_rate", cfg.get("fee_rate", 0.0)) or 0.0),
            gas_per_share=float(cfg.get("merge_gas_per_share", cfg.get("gas_per_share", 0.0)) or 0.0),
            stale_quote_buffer=float(cfg.get("stale_quote_buffer", 0.0) or 0.0),
            min_order_size_shares=min_order_size,
            min_order_notional_usd=min_order_notional,
            share_size_increment=share_increment,
            max_order_size_usd=float(cfg.get("max_order_size", 0) or 0) or None,
            first_leg_order_value_usd=float(cfg.get("first_leg_order_value_usd", cfg.get("first_leg_order_value", 0)) or 0) or None,
            second_leg_max_order_value_usd=float(cfg.get("second_leg_max_order_value_usd", cfg.get("second_leg_max_order_value", 0)) or 0) or None,
            max_book_age_ms=float(cfg.get("max_book_age_ms", 150) if cfg.get("max_book_age_ms") is not None else 150),
        )

    def _test_mode_plan_from_books(
        self,
        cfg: dict[str, Any],
        pair: dict[str, Any],
        diag: ArbSkipDiagnostic | None = None,
    ) -> ArbPlan | None:
        """Create a synthetic paired plan so test mode can buy only the cheap leg.

        The normal paired planner can return None before `_submit_pair` is reached
        when the configured paired order cap cannot afford the exchange's minimum
        share size on both legs. Dashboard test mode is explicitly meant to still
        test those extreme-skew opportunities by buying only the lower-notional
        leg at the CLOB minimum notional.
        """
        if not (_safe_bool(cfg.get("arb_test_mode", False), False) or _safe_bool(cfg.get("test_mode", False), False)):
            return None
        if diag is not None and not diag.opportunity_spotted:
            return None
        min_notional = float(cfg.get("test_mode_min_notional_usd", cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0))) or 1.0)
        if min_notional <= 0:
            return None
        books = self.books_by_slug.get(pair["slug"], self.books)
        yes, no = books["YES"], books["NO"]
        if not (0 < yes.ask < 1 and 0 < no.ask < 1):
            return None
        lower: Leg = "YES" if yes.ask <= no.ask else "NO"
        lower_book = yes if lower == "YES" else no
        other_book = no if lower == "YES" else yes
        base_size = float(cfg.get("min_order_size_shares") or 0.0)
        increment = float(cfg.get("share_size_increment", 1.0 if cfg.get("use_limit_fok", True) else 0.0001) or 0.0001)
        if increment <= 0:
            increment = 0.0001
        if base_size <= 0:
            base_size = increment
        base_size = round(math.ceil(base_size / increment - 1e-12) * increment, 4)
        lower_cost_at_min_size = lower_book.ask * base_size
        if lower_cost_at_min_size + 1e-9 >= min_notional:
            return None
        lower_limit = lower_book.ask
        lower_asks = lower_book.asks or [{"price": lower_book.ask, "size": float("inf")}]
        # Ensure the cheap side has visible/top-of-book depth for the $1 test order.
        test_size = round(math.ceil((min_notional / max(lower_limit, 1e-9)) / increment - 1e-12) * increment, 4)
        if _cost_for_size(lower_asks, test_size) is None:
            return None
        yes_size = base_size
        no_size = base_size
        yes_cost = yes.ask * yes_size
        no_cost = no.ask * no_size
        first = lower
        second: Leg = "NO" if first == "YES" else "YES"
        return ArbPlan(
            yes_size=round(yes_size, 4),
            no_size=round(no_size, 4),
            size=round(base_size, 4),
            yes_limit=round(yes.ask, 3),
            no_limit=round(no.ask, 3),
            yes_cost_est=round(yes_cost, 4),
            no_cost_est=round(no_cost, 4),
            total_cost_est=round(yes_cost + no_cost, 4),
            avg_sum_est=round(yes.ask + no.ask, 5),
            edge_per_pair=round(diag.edge if diag is not None else 1.0 - yes.ask - no.ask, 5),
            first_leg=first,
            second_leg=second,
        )

    def _format_skip_message(self, pair: dict[str, Any], diag: ArbSkipDiagnostic) -> str:
        d = diag.details
        market = str(pair.get("slug") or "")
        title = str(pair.get("title") or market)[:80]
        def fmt(x: Any, nd: int = 4) -> str:
            try:
                if x == float("inf"):
                    return "inf"
                return f"{float(x):.{nd}f}"
            except Exception:
                return str(x)
        return (
            f"Opportunity spotted but no order attempted: reason={diag.reason} market={market} title={title} "
            f"YES_ask={diag.yes_ask:.3f} NO_ask={diag.no_ask:.3f} raw_sum={diag.raw_sum:.4f} "
            f"friction={diag.friction:.4f} edge={diag.edge:.4f} min_edge={diag.min_edge:.4f} "
            f"order_limit=${fmt(d.get('order_limit_usd'), 2)} max_order_size=${fmt(d.get('max_order_size_usd'), 2)} "
            f"first_leg_value=${fmt(d.get('first_leg_order_value_usd'), 2)} second_leg_max=${fmt(d.get('second_leg_max_order_value_usd'), 2)} "
            f"explicit_min_shares={fmt(d.get('min_order_size_shares'))} "
            f"depth_yes={fmt(d.get('yes_depth_shares'))} depth_no={fmt(d.get('no_depth_shares'))} "
            f"max_feasible_shares={fmt(d.get('max_feasible_before_profit_shares'))} "
            f"quote_age_yes={fmt(d.get('yes_age_ms'), 0)}ms quote_age_no={fmt(d.get('no_age_ms'), 0)}ms "
            f"stale_guard={fmt(d.get('max_book_age_ms'), 0)}ms"
        )

    async def _log_skipped_opportunity(self, pair: dict[str, Any], diag: ArbSkipDiagnostic, cfg: dict[str, Any]) -> None:
        if not diag.opportunity_spotted:
            return
        interval = float(cfg.get("skip_opportunity_log_interval_seconds", 10) if cfg.get("skip_opportunity_log_interval_seconds") is not None else 10)
        key = f"{pair.get('slug')}:{diag.reason}:{round(diag.yes_ask, 3)}:{round(diag.no_ask, 3)}"
        now = time.time()
        if interval > 0 and now - self.last_skip_log.get(key, 0.0) < interval:
            return
        self.last_skip_log[key] = now
        await self.writer.log_strategy_event(self.strategy_id, self._format_skip_message(pair, diag), level="WARNING")

    def quote_health_summary(self, cfg: dict[str, Any], *, now: float | None = None) -> dict[str, Any]:
        """Return live in-memory quote freshness and best-edge diagnostics.

        `book_latest.updated_at` is the dashboard write timestamp, not the CLOB
        quote timestamp. This summary uses each Book.updated_ts so the dashboard
        can show whether the sniper's hot path is working with fresh quotes.
        """
        now = now or time.time()
        max_book_age_ms = float(cfg.get("max_book_age_ms", 150) if cfg.get("max_book_age_ms") is not None else 150)
        fee_per_share = float(cfg.get("fee_per_share", 0.0) or 0.0)
        fee_rate = float(cfg.get("polymarket_taker_fee_rate", cfg.get("fee_rate", 0.0)) or 0.0)
        gas_per_share = float(cfg.get("merge_gas_per_share", cfg.get("gas_per_share", 0.0)) or 0.0)
        stale_quote_buffer = float(cfg.get("stale_quote_buffer", 0.0) or 0.0)
        min_edge = float(cfg.get("min_edge", 0.0) or 0.0)
        pairs = self.market_pairs or ([] if self.ev is None else [self.ev])
        token_ages_ms: list[float] = []
        fresh_pairs = 0
        stale_pairs = 0
        best_edge: float | None = None
        best_sum: float | None = None
        best_pair = ""
        best_plan: ArbPlan | None = None
        for pair in pairs:
            books = self.books_by_slug.get(pair["slug"], self.books)
            y = books["YES"]
            n = books["NO"]
            y_age = max(0.0, (now - y.updated_ts) * 1000) if y.updated_ts else float("inf")
            n_age = max(0.0, (now - n.updated_ts) * 1000) if n.updated_ts else float("inf")
            token_ages_ms.extend([y_age, n_age])
            pair_fresh = y_age <= max_book_age_ms and n_age <= max_book_age_ms
            fresh_pairs += 1 if pair_fresh else 0
            stale_pairs += 0 if pair_fresh else 1
            if not (0 < y.ask < 1 and 0 < n.ask < 1):
                continue
            friction = (
                2 * fee_per_share
                + polymarket_fee_per_share(y.ask, fee_rate)
                + polymarket_fee_per_share(n.ask, fee_rate)
                + gas_per_share
                + stale_quote_buffer
            )
            raw_sum = y.ask + n.ask
            edge = 1.0 - raw_sum - friction
            if best_edge is None or edge > best_edge:
                best_edge = edge
                best_sum = raw_sum + friction
                best_pair = str(pair.get("title") or pair.get("slug") or "")[:70]
                best_plan = self._plan_from_cfg(cfg, pair)
        finite_ages = sorted(x for x in token_ages_ms if x != float("inf"))
        max_age = max(finite_ages) if finite_ages else float("inf")
        median_age = finite_ages[len(finite_ages) // 2] if finite_ages else float("inf")
        return {
            "pairs": len(pairs),
            "tokens": len(token_ages_ms),
            "fresh_pairs": fresh_pairs,
            "stale_pairs": stale_pairs,
            "max_age_ms": max_age,
            "median_age_ms": median_age,
            "max_book_age_ms": max_book_age_ms,
            "best_edge": best_edge,
            "best_sum_with_friction": best_sum,
            "best_pair": best_pair,
            "opportunity_now": best_plan is not None,
            "min_edge": min_edge,
        }

    def _health_message(self, cfg: dict[str, Any]) -> tuple[str, str]:
        h = self.quote_health_summary(cfg)
        def fmt_age(x: float) -> str:
            return "never" if x == float("inf") else f"{x:.0f}ms"
        def fmt_stat(stats: LatencyStats, label: str) -> str:
            s = stats.summary()
            med = s.get("median_ms")
            mx = s.get("max_ms")
            if med is None or mx is None:
                return f"{label}=n/a"
            return f"{label}_median={float(med):.0f}ms {label}_max={float(mx):.0f}ms"
        level = "INFO" if h["stale_pairs"] == 0 else "WARNING"
        best_edge = h["best_edge"]
        best = "n/a" if best_edge is None else f"edge={best_edge:.4f} sum+fees={h['best_sum_with_friction']:.4f} pair={h['best_pair']}"
        message = (
            f"Data health: status={self.cached_status} fresh_pairs={h['fresh_pairs']}/{h['pairs']} "
            f"tokens={h['tokens']} quote_age_median={fmt_age(h['median_age_ms'])} "
            f"quote_age_max={fmt_age(h['max_age_ms'])} stale_guard={h['max_book_age_ms']:.0f}ms "
            f"{fmt_stat(self.rest_book_latency, 'rest_books')} {fmt_stat(self.submit_latency, 'submit')} "
            f"ws_updates={self.ws_update_count} rest_refreshes={self.rest_book_refresh_count} "
            f"opportunity_now={h['opportunity_now']} {best} min_edge={h['min_edge']:.4f}"
        )
        return message, level

    async def maybe_log_health(self, cfg: dict[str, Any]) -> None:
        interval_raw = cfg.get("health_log_interval_seconds", 15)
        interval = 15.0 if interval_raw is None else float(interval_raw)
        if interval <= 0:
            return
        now = time.time()
        if now - self.last_health_log_ts < interval:
            return
        self.last_health_log_ts = now
        if self._is_weather_outlier_strategy(cfg):
            message, level = self._weather_outlier_health_message(cfg)
        else:
            message, level = self._health_message(cfg)
        if str(level).upper() == "INFO" and not _safe_bool(cfg.get("dashboard_verbose_info_logs", False), False):
            # Keep dashboard logs for material events only: market changes, order
            # attempts/fills, warnings/errors. Routine healthy polling summaries
            # belong in systemd/journal logs, not the UI event feed.
            logger.debug("suppressed dashboard INFO health log for {}: {}", self.strategy_id, message)
            return
        await self.writer.log_strategy_event(self.strategy_id, message, level=level)

    def _is_weather_outlier_strategy(self, cfg: dict[str, Any]) -> bool:
        return str(cfg.get("kind") or "").strip().lower() == "weather_outlier_sniper" or _safe_bool(cfg.get("weather_outlier_strategy"), False)

    def _weather_outlier_plan_from_cfg(
        self,
        cfg: dict[str, Any],
        *,
        exclude_market_slugs: set[str] | None = None,
        remaining_usd_by_slug: dict[str, float] | None = None,
        direction_lock_by_event: dict[str, str] | None = None,
        safety_result: dict[str, Any] | None = None,
        diagnostics: list[dict[str, Any]] | None = None,
    ) -> WeatherOutlierPlan | None:
        exclude_market_slugs = exclude_market_slugs or set()
        remaining_usd_by_slug = remaining_usd_by_slug or {}
        direction_lock_by_event = direction_lock_by_event or {}
        if not _safe_bool(cfg.get("weather_outlier_direction_lock_enabled", True), True):
            direction_lock_by_event = {}

        def add_diag(reason: str, pair: dict[str, Any] | None = None, **details: Any) -> None:
            if diagnostics is None:
                return
            row = {"reason": reason, **details}
            if pair is not None:
                row.setdefault("candidate", str(pair.get("slug") or ""))
                temp = weather_temperature_value(pair)
                if temp is not None:
                    row.setdefault("temp", temp)
            diagnostics.append(row)

        pairs = self.market_pairs or ([] if self.ev is None else [self.ev])
        if not pairs:
            add_diag("no_markets_loaded")
            return None
        max_age_ms = self._weather_scan_max_book_age_ms(cfg)
        now = time.time()
        priced: list[tuple[float, float, dict[str, Any], Book, Book]] = []
        for pair in pairs:
            slug = str(pair.get("slug") or "")
            if slug in exclude_market_slugs:
                add_diag("excluded_market", pair)
                continue
            temp = weather_temperature_value(pair)
            if temp is None:
                add_diag("missing_temperature_value", pair)
                continue
            books = self.books_by_slug.get(pair["slug"], self.books)
            yes_book = books["YES"]
            no_book = books["NO"]
            yes_price = weather_prediction_price(yes_book)
            if not (0 < yes_price < 1):
                add_diag("invalid_yes_price", pair, yes_price=yes_price)
                continue
            if not (0 < no_book.ask < 1):
                add_diag("invalid_no_ask", pair, no_ask=no_book.ask)
                continue
            priced.append((yes_price, temp, pair, yes_book, no_book))
        if not priced:
            add_diag("no_fresh_priced_markets")
            return None
        winning_price, winning_temp, _winning_pair, _winning_yes_book, _winning_no_book = max(priced, key=lambda x: x[0])
        offset = float(cfg.get("outlier_temperature_offset_degrees", cfg.get("temperature_outlier_degrees", 4)) or 4)
        order_usd = float(cfg.get("outlier_order_usd", cfg.get("order_limit_usd", 1.0)) or 1.0)
        min_edge = max(0.0, float(cfg.get("min_edge", 0.01) or 0.0))
        rebuy_tiers = _weather_outlier_rebuy_tiers(cfg)
        max_tier_notional_mult = max(mult for _edge_mult, mult in rebuy_tiers)
        max_no_price = max(0.0, min(1.0, 1.0 - min_edge))
        boundary_threshold_c = _weather_boundary_veto_threshold_c(cfg)
        boundary_forecast_high_c = _weather_boundary_forecast_high_c(safety_result)
        if offset <= 0 or order_usd <= 0:
            add_diag("invalid_config", offset=offset, order_usd=order_usd)
            return None
        candidates: list[WeatherOutlierPlan] = []
        for _yes_ask, temp, pair, _yes_book, no_book in priced:
            distance = abs(temp - winning_temp)
            if distance + 1e-9 < offset:
                add_diag("distance_below_offset", pair, winning_temp=winning_temp, winning_price=winning_price, distance=distance, offset=offset)
                continue
            ask = no_book.ask
            if ask <= 0:
                add_diag("invalid_no_ask", pair, no_ask=ask)
                continue
            slug = str(pair.get("slug") or "")
            candidate_direction = _outlier_direction(temp, winning_temp)
            if _weather_outlier_is_higher_bracket_only(cfg) and candidate_direction != "higher":
                add_diag("higher_bracket_only", pair, winning_temp=winning_temp, direction=candidate_direction)
                continue
            direction_lock = direction_lock_by_event.get(_weather_market_event_key(slug))
            if not _direction_allows_candidate(direction_lock, candidate_direction):
                add_diag("direction_lock", pair, winning_temp=winning_temp, direction=candidate_direction, locked_direction=direction_lock)
                continue
            if _weather_nws_heat_alert_blocks_higher_no(temp, winning_temp, self.weather_nws_heat_alert_status, event_slug=slug):
                add_diag(
                    "nws_heat_alert_higher_no",
                    pair,
                    winning_temp=winning_temp,
                    direction=candidate_direction,
                    nws_event=(self.weather_nws_heat_alert_status or {}).get("event"),
                    nws_headline=(self.weather_nws_heat_alert_status or {}).get("headline"),
                    nws_onset=(self.weather_nws_heat_alert_status or {}).get("onset"),
                    nws_ends=(self.weather_nws_heat_alert_status or {}).get("ends"),
                )
                continue
            boundary_reason = _weather_boundary_veto_reason(temp, boundary_forecast_high_c, boundary_threshold_c)
            if boundary_reason:
                add_diag(
                    "forecast_boundary_veto",
                    pair,
                    winning_temp=winning_temp,
                    winning_price=winning_price,
                    no_ask=ask,
                    max_no_price=max_no_price,
                    forecast_high_c=boundary_forecast_high_c,
                    boundary_threshold_c=boundary_threshold_c,
                    boundary_distance_c=abs(float(temp) - float(boundary_forecast_high_c)) if boundary_forecast_high_c is not None else None,
                    detail=boundary_reason,
                )
                continue
            max_target_notional = order_usd * max_tier_notional_mult
            remaining_to_max = float(remaining_usd_by_slug.get(slug, max_target_notional))
            spent_notional = max(0.0, max_target_notional - remaining_to_max)
            eligible_tiers: list[tuple[float, float, float]] = []
            for edge_mult, notional_mult in rebuy_tiers:
                tier_max_no_price = max(0.0, min(1.0, 1.0 - (min_edge * edge_mult)))
                tier_target_notional = order_usd * notional_mult
                if ask <= tier_max_no_price + 1e-9 and spent_notional + 1e-9 < tier_target_notional:
                    eligible_tiers.append((edge_mult, notional_mult, tier_max_no_price))
            if not eligible_tiers:
                add_diag(
                    "no_eligible_rebuy_tier",
                    pair,
                    winning_temp=winning_temp,
                    no_ask=ask,
                    spent_notional=spent_notional,
                    max_target_notional=max_target_notional,
                    tier_caps=[1.0 - (min_edge * edge_mult) for edge_mult, _ in rebuy_tiers],
                )
                continue
            tier_edge_mult, tier_notional_mult, tier_max_no_price = max(eligible_tiers, key=lambda x: (x[0], x[1]))
            raw_asks = no_book.asks or [{"price": ask, "size": float("inf")}]
            capped_asks = [level for level in raw_asks if _safe_float(level.get("price")) <= tier_max_no_price + 1e-9]
            tier_target_notional = order_usd * tier_notional_mult
            remaining_order_usd = max(0.0, tier_target_notional - spent_notional)
            if remaining_order_usd <= 0:
                add_diag("no_remaining_order_budget", pair, winning_temp=winning_temp, tier_target_notional=tier_target_notional, spent_notional=spent_notional)
                continue
            # Sweep all currently visible liquidity priced at or below the edge
            # cap, but never exceed the remaining per-market order budget. If the
            # eligible book only has e.g. $0.42 available under a $200 cap, try to
            # buy that now with the same capped limit price; if the book has moved
            # before CLOB sees the order, the FAK/FOK order simply will not match
            # above that limit. Future loops can buy newly-visible liquidity until
            # the per-market dollar cap is exhausted.
            sized = _size_for_notional_capped(
                capped_asks,
                remaining_order_usd,
                precision=4,
                allow_partial=True,
                min_notional_usd=0.0,
            )
            if sized is None:
                add_diag(
                    "insufficient_capped_liquidity",
                    pair,
                    winning_temp=winning_temp,
                    no_ask=ask,
                    tier_max_no_price=tier_max_no_price,
                    remaining_order_usd=remaining_order_usd,
                    min_order_notional=float(cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0)) or 1.0),
                    capped_levels=len(capped_asks),
                )
                continue
            size, estimated_cost, worst_limit = sized
            if worst_limit <= 0 or worst_limit > tier_max_no_price + 1e-9:
                add_diag("worst_limit_exceeds_tier_cap", pair, winning_temp=winning_temp, worst_limit=worst_limit, tier_max_no_price=tier_max_no_price)
                continue
            candidates.append(WeatherOutlierPlan(
                pair=pair,
                token=pair["no_token"],
                temp_value=temp,
                winning_temp=winning_temp,
                winning_price=winning_price,
                ask=worst_limit,
                size=size,
                notional=estimated_cost,
                distance_degrees=distance,
                min_edge=min_edge,
                max_no_price=tier_max_no_price,
                tier_edge_multiplier=tier_edge_mult,
                tier_notional_multiplier=tier_notional_mult,
                tier_target_notional=tier_target_notional,
                tier_remaining_notional=remaining_order_usd,
                boundary_forecast_high_c=boundary_forecast_high_c,
                boundary_distance_c=abs(temp - boundary_forecast_high_c) if boundary_forecast_high_c is not None else None,
            ))
        if not candidates:
            return None
        # Prefer the most aggressively-priced NO liquidity that still clears the
        # configured edge; ties go farther from the current winning value.
        selected = max(candidates, key=lambda p: (p.tier_edge_multiplier, p.ask, p.distance_degrees))
        self._mark_weather_outlier_hot_pair(selected.pair, cfg, reason="candidate")
        return selected

    def _weather_outlier_health_message(self, cfg: dict[str, Any]) -> tuple[str, str]:
        pairs = self.market_pairs or ([] if self.ev is None else [self.ev])
        max_age_ms = self._weather_scan_max_book_age_ms(cfg)
        city = _weather_outlier_city_from_cfg(cfg)
        blacklist = _weather_outlier_blacklist(cfg)

        def fmt_age(x: float) -> str:
            return "never" if x == float("inf") else f"{x:.0f}ms"

        def fmt_stat(stats: LatencyStats, label: str) -> str:
            s = stats.summary()
            med = s.get("median_ms")
            mx = s.get("max_ms")
            if med is None or mx is None:
                return f"{label}=n/a"
            return f"{label}_median={float(med):.0f}ms {label}_max={float(mx):.0f}ms"

        if city and city in blacklist:
            shown = ", ".join(sorted(blacklist)) or "none"
            return f"Weather outlier health: status={self.cached_status} city={city} blacklisted=true blacklist=[{shown}] new BUYs disabled; take-profit exits remain enabled", "INFO"
        now = time.time()
        priced = []
        token_ages_ms: list[float] = []
        token_diag_rows: list[tuple[float, str, str, str]] = []
        fresh_tokens = 0
        total_tokens = 0
        for pair in pairs:
            temp = weather_temperature_value(pair)
            if temp is None:
                continue
            books = self.books_by_slug.get(pair["slug"], self.books)
            for leg in ("YES", "NO"):
                b = books[leg]
                token = str(pair.get("yes_token") if leg == "YES" else pair.get("no_token") or "")
                total_tokens += 1
                age_ms = max(0.0, (now - b.updated_ts) * 1000) if b.updated_ts else float("inf")
                token_ages_ms.append(age_ms)
                token_diag_rows.append((age_ms, str(pair.get("slug") or ""), leg, token))
                if age_ms <= max_age_ms:
                    fresh_tokens += 1
            book = books["YES"]
            age_ms = max(0.0, (now - book.updated_ts) * 1000) if book.updated_ts else float("inf")
            yes_price = weather_prediction_price(book)
            if 0 < yes_price < 1:
                priced.append((yes_price, temp, age_ms))
        finite_ages = sorted(x for x in token_ages_ms if x != float("inf"))
        median_age = finite_ages[len(finite_ages) // 2] if finite_ages else float("inf")
        max_age = max(finite_ages) if finite_ages else float("inf")
        winning = max(priced, default=None, key=lambda x: x[0])
        oldest_rows = sorted(token_diag_rows, key=lambda x: x[0], reverse=True)[:3]
        oldest_text = ",".join(
            f"{slug}:{leg}:{token}:{fmt_age(age)}" for age, slug, leg, token in oldest_rows if token
        ) or "none"
        missing_count = max(0, int(getattr(self, "last_rest_requested_tokens", 0) or 0) - int(getattr(self, "last_rest_returned_tokens", 0) or 0))
        missing_total = int(getattr(self, "last_rest_requested_tokens", 0) or 0)
        missing_labels = list(getattr(self, "last_rest_missing_labels", []) or [])
        if not missing_labels:
            missing_labels = list(getattr(self, "last_rest_missing_tokens", []) or [])
        missing_text = ",".join(str(x) for x in missing_labels[:6]) or "none"
        diag_warn_age_ms = float(cfg.get("weather_outlier_diag_warn_age_ms", 10000) or 10000)
        plan = self._weather_outlier_plan_from_cfg(cfg)
        offset = float(cfg.get("outlier_temperature_offset_degrees", cfg.get("temperature_outlier_degrees", 4)) or 4)
        order_usd = float(cfg.get("outlier_order_usd", cfg.get("order_limit_usd", 1.0)) or 1.0)
        if not winning:
            return (
                f"Weather outlier health: status={self.cached_status} no fresh temperature options order=${order_usd:.2f} "
                f"offset={offset:g}° max_book_age={max_age_ms:.0f}ms quote_age_median={fmt_age(median_age)} "
                f"quote_age_max={fmt_age(max_age)} fresh_tokens={fresh_tokens}/{total_tokens} "
                f"oldest_tokens={oldest_text} missing_last_poll={missing_count}/{missing_total} missing_tokens={missing_text} "
                f"{fmt_stat(self.rest_book_latency, 'rest_books')} {fmt_stat(self.submit_latency, 'submit')} "
                f"ws_updates={self.ws_update_count} rest_refreshes={self.rest_book_refresh_count}"
            ), "WARNING"
        max_per_market = int(cfg.get("max_orders_per_market", 1) or 0)
        tiers_enabled = _safe_bool(cfg.get("weather_outlier_rebuy_tiers_enabled", False), False)
        tier_text = "/".join(f"{edge:g}x→{notional:g}x" for edge, notional in _weather_outlier_rebuy_tiers(cfg))
        msg = (
            f"Weather outlier health: status={self.cached_status} options={len(priced)}/{len(pairs)} "
            f"winning_temp={winning[1]:g} winning_yes={winning[0]:.3f} offset={offset:g}° order=${order_usd:.2f} min_edge={float(cfg.get('min_edge', 0.01) or 0.0):.3f} "
            f"rebuy_tiers={'on' if tiers_enabled else 'off'}[{tier_text}] max_orders_per_market={max_per_market} candidate_now={plan is not None} "
            f"quote_age_median={fmt_age(median_age)} quote_age_max={fmt_age(max_age)} fresh_tokens={fresh_tokens}/{total_tokens} "
            f"oldest_tokens={oldest_text} missing_last_poll={missing_count}/{missing_total} missing_tokens={missing_text} "
            f"{fmt_stat(self.rest_book_latency, 'rest_books')} {fmt_stat(self.submit_latency, 'submit')} "
            f"ws_updates={self.ws_update_count} rest_refreshes={self.rest_book_refresh_count}"
        )
        if _weather_outlier_is_higher_bracket_only(cfg):
            msg += " higher_bracket_only=true"
        level = "WARNING" if diag_warn_age_ms > 0 and max_age != float("inf") and max_age >= diag_warn_age_ms else "INFO"
        if plan:
            msg += f" candidate_temp={plan.temp_value:g} distance={plan.distance_degrees:g}° NO@{plan.ask:.3f} max_no={plan.max_no_price:.3f} tier={plan.tier_edge_multiplier:g}x→{plan.tier_notional_multiplier:g}x size={plan.size:.4f}"
        return msg, level

    async def _refresh_weather_nws_heat_alert(self, cfg: dict[str, Any], client: httpx.AsyncClient | None = None) -> dict[str, Any] | None:
        if not _safe_bool(cfg.get("weather_outlier_nws_heat_alert_guard_enabled", True), True):
            self.weather_nws_heat_alert_status = {"active": False, "reason": "disabled"}
            return self.weather_nws_heat_alert_status
        city = _weather_outlier_city_from_cfg(cfg)
        station = STATIONS.get(city)
        if station is None or not str(station.station_id or "").upper().startswith("K"):
            self.weather_nws_heat_alert_status = {"active": False, "city": city, "reason": "nws_not_applicable"}
            return self.weather_nws_heat_alert_status
        own_client = client is None
        timeout = httpx.Timeout(8.0, connect=2.0, read=5.0, write=1.0, pool=1.0)
        headers = {"User-Agent": "polybot-weather-nws-heat-alert-guard/1.0", "Accept": "application/geo+json,application/json"}
        if own_client:
            client = httpx.AsyncClient(timeout=timeout, headers=headers, limits=httpx.Limits(max_keepalive_connections=2, max_connections=4, keepalive_expiry=30.0))
        assert client is not None
        try:
            lat = station.lat
            lon = station.lon
            if self.weather_nws_station_point is not None:
                lat, lon = self.weather_nws_station_point
            if lat is None or lon is None:
                r = await client.get("https://aviationweather.gov/api/data/stationinfo", params={"ids": station.station_id, "format": "json"})
                r.raise_for_status()
                data = r.json() or []
                row = data[0] if isinstance(data, list) and data else {}
                lat = _safe_float(row.get("lat") or row.get("latitude"))
                lon = _safe_float(row.get("lon") or row.get("longitude"))
                if lat is not None and lon is not None:
                    self.weather_nws_station_point = (lat, lon)
            if lat is None or lon is None:
                self.weather_nws_heat_alert_status = {"active": False, "city": city, "station": station.station_id, "reason": "missing_station_point", "fetched_at": datetime.now(timezone.utc).isoformat()}
                return self.weather_nws_heat_alert_status
            r = await client.get("https://api.weather.gov/alerts/active", params={"point": f"{lat:.4f},{lon:.4f}"})
            r.raise_for_status()
            features = (r.json() or {}).get("features") or []
            heat_alerts = []
            for feature in features:
                props = (feature or {}).get("properties") or {}
                event = str(props.get("event") or "").strip().lower()
                if event in NWS_HEAT_ALERT_EVENTS:
                    heat_alerts.append(props)
            def sort_key(props: dict[str, Any]) -> tuple[datetime, datetime]:
                start = _parse_dt_utc(props.get("onset") or props.get("effective") or props.get("sent")) or datetime.min.replace(tzinfo=timezone.utc)
                end = _parse_dt_utc(props.get("ends") or props.get("expires")) or datetime.max.replace(tzinfo=timezone.utc)
                return (start, end)
            heat_alerts.sort(key=sort_key)
            if heat_alerts:
                props = heat_alerts[0]
                status = {
                    "active": True,
                    "city": city,
                    "station": station.station_id,
                    "event": props.get("event"),
                    "headline": props.get("headline"),
                    "areaDesc": props.get("areaDesc"),
                    "sent": props.get("sent"),
                    "effective": props.get("effective"),
                    "onset": props.get("onset"),
                    "expires": props.get("expires"),
                    "ends": props.get("ends"),
                    "id": props.get("id"),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
            else:
                status = {"active": False, "city": city, "station": station.station_id, "reason": "no_active_heat_alert", "fetched_at": datetime.now(timezone.utc).isoformat()}
            self.weather_nws_heat_alert_status = status
            self.weather_nws_heat_alert_last_check_ts = time.time()
            return status
        except Exception as e:
            status = {"active": False, "city": city, "station": station.station_id, "reason": f"nws_refresh_error={type(e).__name__}: {e}", "fetched_at": datetime.now(timezone.utc).isoformat()}
            self.weather_nws_heat_alert_status = status
            self.weather_nws_heat_alert_last_check_ts = time.time()
            logger.debug("NWS heat-alert refresh failed for {}: {}", self.strategy_id, e)
            return status
        finally:
            if own_client:
                await client.aclose()

    async def weather_nws_heat_alert_loop(self, cfg_ref: dict[str, Any]) -> None:
        cfg = cfg_ref
        interval = max(300.0, float(cfg.get("weather_outlier_nws_heat_alert_refresh_seconds", 600) or 600))
        # Refresh once immediately after startup/restart; jitter only subsequent
        # periodic polls so the hot path never waits for weather.gov but protection
        # is available quickly.
        # Spread the 50 weather shards so they do not hammer weather.gov together.
        jitter = (int(hashlib.sha256((self.strategy_id + ":nws").encode()).hexdigest()[:8], 16) % int(interval)) if interval > 1 else 0
        timeout = httpx.Timeout(8.0, connect=2.0, read=5.0, write=1.0, pool=1.0)
        headers = {"User-Agent": "polybot-weather-nws-heat-alert-guard/1.0", "Accept": "application/geo+json,application/json"}
        async with httpx.AsyncClient(timeout=timeout, headers=headers, limits=httpx.Limits(max_keepalive_connections=2, max_connections=4, keepalive_expiry=30.0)) as client:
            while not self.stop.is_set():
                try:
                    await self._refresh_weather_nws_heat_alert(cfg, client=client)
                except Exception as e:
                    logger.debug("NWS heat-alert loop error for {}: {}", self.strategy_id, e)
                if jitter > 0:
                    await asyncio.sleep(float(jitter))
                    jitter = 0
                else:
                    await asyncio.sleep(interval)

    async def _refresh_weather_safety_filter(self, cfg: dict[str, Any], *, force: bool = False) -> dict[str, Any] | None:
        if not self._is_weather_outlier_strategy(cfg):
            return None
        if not _safe_bool(cfg.get("weather_safety_filter_report_enabled", True), True) and not _safe_bool(cfg.get("weather_safety_filter_enabled", False), False):
            return None
        interval = max(60.0, float(cfg.get("weather_safety_filter_refresh_seconds", 900) or 900))
        now = time.time()
        city = _weather_outlier_city_from_cfg(cfg)
        if not city:
            return None
        event_slug = str((self.ev or {}).get("event_slug") or (self.ev or {}).get("slug") or "") or None
        target_date = event_target_date(event_slug)
        cache_key = (city, target_date or event_slug)
        if (
            not force
            and self.weather_safety_status is not None
            and self.weather_safety_cache_key == cache_key
            and now - self.weather_safety_last_check_ts < interval
        ):
            return self.weather_safety_status
        try:
            result = await analyze_city_safety(city, event_slug=event_slug)
        except Exception as e:
            result = {
                "city_slug": city,
                "city": city,
                "station": "",
                "source": "",
                "gate": "RED",
                "reason": f"weather safety filter exception={type(e).__name__}: {e}",
                "reasons": [f"exception={type(e).__name__}: {e}"],
                "warnings": [],
                "metrics": {},
                "weather_codes": [],
                "weather_code_names": [],
                "expected_temp_fluctuation_c": None,
                "size_multiplier": 0.0,
                "event_slug": event_slug,
            }
        enabled = _safe_bool(cfg.get("weather_safety_filter_enabled", False), False)
        if result.get("gate") == "RED":
            result["size_multiplier"] = 0.0
        else:
            # GREEN and YELLOW both use the normal single-order size. The only
            # YELLOW restriction is disabling same-market ladder top-ups.
            result["size_multiplier"] = 1.0
        self.weather_safety_status = result
        self.weather_safety_last_check_ts = now
        self.weather_safety_cache_key = cache_key
        try:
            if hasattr(self.writer, "upsert_weather_safety_filter"):
                await self.writer.upsert_weather_safety_filter(self.strategy_id, result, enabled=enabled)
        except Exception as e:
            logger.debug("weather safety DB upsert failed for {}: {}", self.strategy_id, e)
        return result

    async def _weather_safety_allows_new_buy(self, cfg: dict[str, Any]) -> tuple[bool, float, dict[str, Any] | None]:
        enabled = _safe_bool(cfg.get("weather_safety_filter_enabled", False), False)
        result = await self._refresh_weather_safety_filter(cfg)
        if not enabled:
            return True, 1.0, result
        if result is None:
            fail_closed = _safe_bool(cfg.get("weather_safety_filter_fail_closed", True), True)
            return (not fail_closed), (0.0 if fail_closed else 1.0), None
        gate = str(result.get("gate") or "GREEN").upper()
        if gate == "RED":
            return False, 0.0, result
        # When enforcement is enabled, YELLOW cities may still trade but only via
        # the normal one-shot order budget. Do not scale the order down and do not
        # allow same-market re-buy ladder top-ups for YELLOW gates.
        return True, 1.0, result

    def _weather_safety_adjusted_plan_cfg(self, cfg: dict[str, Any], safety_result: dict[str, Any] | None) -> dict[str, Any]:
        if not _safe_bool(cfg.get("weather_safety_filter_enabled", False), False):
            return cfg
        gate = str((safety_result or {}).get("gate") or "GREEN").upper()
        if gate != "YELLOW":
            return cfg
        plan_cfg = dict(cfg)
        plan_cfg["weather_outlier_rebuy_tiers_enabled"] = False
        return plan_cfg

    def _weather_scan_max_book_age_ms(self, cfg: dict[str, Any]) -> float:
        """Freshness guard for broad weather scanning/candidate detection."""
        raw = cfg.get("scan_max_book_age_ms", cfg.get("weather_outlier_scan_max_book_age_ms", cfg.get("max_book_age_ms", 250)))
        return float(raw if raw is not None else 250)

    def _weather_execution_max_book_age_ms(self, cfg: dict[str, Any]) -> float:
        """Freshness guard for live order submission after immediate revalidation."""
        raw = cfg.get("execution_max_book_age_ms", cfg.get("weather_outlier_execution_max_book_age_ms", cfg.get("max_book_age_ms", 1000)))
        return float(raw if raw is not None else 1000)

    def _mark_weather_outlier_hot_pair(self, pair: dict[str, Any], cfg: dict[str, Any], *, reason: str = "candidate") -> None:
        if not _safe_bool(cfg.get("weather_outlier_hot_poll_enabled", True), True):
            return
        slug = str(pair.get("slug") or "")
        if not slug:
            return
        seconds = max(0.0, float(cfg.get("weather_outlier_hot_poll_seconds", cfg.get("hot_poll_seconds", 90)) or 90))
        if seconds <= 0:
            return
        self.weather_outlier_hot_until_by_slug[slug] = max(self.weather_outlier_hot_until_by_slug.get(slug, 0.0), time.time() + seconds)

    def _weather_outlier_hot_token_ids(self) -> list[str]:
        now = time.time()
        expired = [slug for slug, until in self.weather_outlier_hot_until_by_slug.items() if until <= now]
        for slug in expired:
            self.weather_outlier_hot_until_by_slug.pop(slug, None)
        tokens: list[str] = []
        hot_slugs = set(self.weather_outlier_hot_until_by_slug)
        for pair in self.market_pairs or ([] if self.ev is None else [self.ev]):
            if str(pair.get("slug") or "") in hot_slugs:
                tokens.extend([str(pair.get("yes_token") or ""), str(pair.get("no_token") or "")])
        return [tok for tok in dict.fromkeys(tokens) if tok]

    def _weather_outlier_plan_fresh_for_execution(self, plan: WeatherOutlierPlan, cfg: dict[str, Any]) -> tuple[bool, str]:
        # Do not reject weather-outlier BUYs solely because the local snapshot is
        # old. Execution orders are sent with a capped limit price; if capped
        # liquidity disappeared before CLOB receives the FAK/FOK order, it simply
        # will not match above the cap. Keep only the price/side sanity check.
        books = self.books_by_slug.get(plan.pair["slug"], self.books)
        no_book = books["NO"]
        if not (0 < no_book.ask <= plan.max_no_price + 1e-9):
            return False, "execution_price_moved"
        return True, "ok"

    async def _revalidate_weather_outlier_candidate_books(self, plan: WeatherOutlierPlan, cfg: dict[str, Any]) -> tuple[bool, str]:
        if not _safe_bool(cfg.get("execution_revalidate_books", cfg.get("weather_outlier_execution_revalidate_books", False)), False):
            return self._weather_outlier_plan_fresh_for_execution(plan, cfg)
        self._mark_weather_outlier_hot_pair(plan.pair, cfg, reason="revalidate")
        tokens = [str(plan.pair.get("yes_token") or ""), str(plan.pair.get("no_token") or "")]
        timeout = httpx.Timeout(2.0, connect=0.5, read=1.5, write=0.5, pool=0.5)
        limits = httpx.Limits(max_keepalive_connections=2, max_connections=4, keepalive_expiry=15.0)
        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            results = await asyncio.gather(*(rest_book_full(client, tok) for tok in tokens if tok), return_exceptions=True)
        applied = 0
        for tok, snapshot in zip([tok for tok in tokens if tok], results):
            if isinstance(snapshot, Book) and self._apply_full_book(tok, snapshot):
                applied += 1
        if applied:
            self.rest_book_refresh_count += 1
        # A failed refresh should not be a freshness blocker. Fall through to the
        # current capped-price check; the submitted FAK/FOK order remains bounded
        # by plan.max_no_price and cannot take worse liquidity.
        return self._weather_outlier_plan_fresh_for_execution(plan, cfg)

    async def _refresh_weather_outlier_hot_books(self, client: httpx.AsyncClient, cfg: dict[str, Any]) -> int:
        if not _safe_bool(cfg.get("weather_outlier_hot_poll_enabled", True), True):
            return 0
        tokens = self._weather_outlier_hot_token_ids()
        if not tokens:
            return 0
        results = await asyncio.gather(*(rest_book_full(client, tok) for tok in tokens), return_exceptions=True)
        applied = 0
        for tok, snapshot in zip(tokens, results):
            if isinstance(snapshot, Book) and self._apply_full_book(tok, snapshot):
                applied += 1
        if applied:
            self.rest_book_refresh_count += 1
        return applied

    def _format_weather_outlier_block_message(self, diagnostics: list[dict[str, Any]], cfg: dict[str, Any], safety_result: dict[str, Any] | None) -> str:
        def fmt(value: Any, nd: int = 3) -> str:
            try:
                val = float(value)
                if math.isinf(val):
                    return "inf"
                if val.is_integer():
                    return str(int(val))
                return f"{val:.{nd}f}"
            except Exception:
                return str(value)

        gate = str((safety_result or {}).get("gate") or ("DISABLED" if not _safe_bool(cfg.get("weather_safety_filter_enabled", False), False) else "UNKNOWN")).upper()
        metrics = (safety_result or {}).get("metrics") or {}
        forecast_high = metrics.get("forecast_high_c")
        threshold = _weather_boundary_veto_threshold_c(cfg)
        counts: dict[str, int] = {}
        for row in diagnostics:
            counts[str(row.get("reason") or "unknown")] = counts.get(str(row.get("reason") or "unknown"), 0) + 1
        counts_text = ",".join(f"{reason}={count}" for reason, count in sorted(counts.items())) or "none"
        interesting = [
            row
            for row in diagnostics
            if row.get("reason")
            in {
                "forecast_boundary_veto",
                "nws_heat_alert_higher_no",
                "no_eligible_rebuy_tier",
                "insufficient_capped_liquidity",
                "direction_lock",
                "higher_bracket_only",
                "stale_book",
                "invalid_no_ask",
                "invalid_yes_price",
                "no_fresh_priced_markets",
            }
        ]
        if not interesting:
            interesting = diagnostics[:5]
        parts = []
        for row in interesting[:8]:
            fields = [
                f"reason={row.get('reason')}",
                f"candidate={row.get('candidate', '')}",
            ]
            for key in (
                "temp",
                "winning_temp",
                "no_ask",
                "max_no_price",
                "forecast_high_c",
                "boundary_distance_c",
                "boundary_threshold_c",
                "distance",
                "offset",
                "tier_max_no_price",
                "remaining_order_usd",
                "min_order_notional",
                "yes_age_ms",
                "no_age_ms",
                "max_age_ms",
                "yes_price",
                "nws_event",
                "nws_onset",
                "nws_ends",
            ):
                if key in row and row.get(key) is not None:
                    fields.append(f"{key}={fmt(row.get(key))}")
            if row.get("detail"):
                fields.append(f"detail={row.get('detail')}")
            parts.append("{" + " ".join(fields) + "}")
        return (
            f"Weather outlier entry blocked: safety_gate={gate} forecast_high_c={fmt(forecast_high) if forecast_high is not None else 'n/a'} "
            f"boundary_threshold_c={fmt(threshold)} criteria_counts=[{counts_text}] candidates=" + " ".join(parts)
        )

    async def _log_weather_outlier_blocked_entry(self, diagnostics: list[dict[str, Any]], cfg: dict[str, Any], safety_result: dict[str, Any] | None) -> None:
        # Keep a durable audit trail for missed-entry classes that can hide real
        # opportunities: stale/missing books, invalid/missing executable asks,
        # insufficient eligible depth, and post-signal execution gates. The log is
        # rate-limited below, so including these reasons should not spam the UI but
        # lets us distinguish "no trade because no edge" from "no trade because our
        # data/order path was stale or unusable" after the fact.
        blocking_reasons = {
            "forecast_boundary_veto",
            "nws_heat_alert_higher_no",
            "no_eligible_rebuy_tier",
            "insufficient_capped_liquidity",
            "direction_lock",
            "higher_bracket_only",
            "excluded_market",
            "no_remaining_order_budget",
            "worst_limit_exceeds_tier_cap",
            "stale_book",
            "invalid_no_ask",
            "invalid_yes_price",
            "no_fresh_priced_markets",
            "no_markets_loaded",
        }
        if not any(row.get("reason") in blocking_reasons for row in diagnostics):
            return
        interval = float(cfg.get("weather_outlier_block_log_interval_seconds", cfg.get("skip_opportunity_log_interval_seconds", 10)) if cfg.get("weather_outlier_block_log_interval_seconds", cfg.get("skip_opportunity_log_interval_seconds", 10)) is not None else 10)
        key_parts = []
        for row in diagnostics[:12]:
            key_parts.append(f"{row.get('candidate')}:{row.get('reason')}:{row.get('temp')}:{row.get('winning_temp')}:{row.get('no_ask')}:{row.get('forecast_high_c')}")
        key = "weather_outlier_block:" + "|".join(key_parts)
        now = time.time()
        if interval > 0 and now - self.last_skip_log.get(key, 0.0) < interval:
            return
        self.last_skip_log[key] = now
        await self.writer.log_strategy_event(self.strategy_id, self._format_weather_outlier_block_message(diagnostics, cfg, safety_result), level="INFO")

    async def _weather_outlier_excluded_slugs(self, cfg: dict[str, Any]) -> set[str]:
        remaining = await self._weather_outlier_remaining_usd_by_slug(cfg)
        return {slug for slug, left in remaining.items() if left <= 1e-9}

    async def _weather_outlier_successful_buy_market_slugs(self) -> set[str]:
        """Return distinct market slugs already bought/submitted for this city shard."""
        if hasattr(self.writer, "successful_buy_market_slugs"):
            try:
                return await self.writer.successful_buy_market_slugs(self.strategy_id)
            except Exception as e:
                logger.warning("Weather outlier successful market lookup failed for {}: {}", self.strategy_id, e)
        return set()

    async def _weather_outlier_direction_lock_by_event(self) -> dict[str, str]:
        """Return same-date direction locks from the first successful BUY per event.

        If the first bought outlier bracket was above the then-current favorite,
        future lower-side brackets for that city/date are blocked; if it was
        below, future higher-side brackets are blocked.
        """
        if not hasattr(self.writer, "first_successful_buy_outlier_signal_by_event"):
            return {}
        try:
            first_by_event = await self.writer.first_successful_buy_outlier_signal_by_event(self.strategy_id)
        except Exception as e:
            logger.warning("Weather outlier direction-lock lookup failed for {}: {}", self.strategy_id, e)
            return {}
        locks: dict[str, str] = {}
        for event_key, data in (first_by_event or {}).items():
            direction = _outlier_direction(data.get("temp_value"), data.get("winning_temp"))
            if direction:
                locks[str(event_key)] = direction
        return locks

    def _weather_outlier_trade_reconcile_rows(self, order_ids: set[str], maker_address: str) -> dict[str, dict[str, Any]]:
        if not order_ids or self.exec_client is None or not maker_address:
            return {}
        try:
            trades = self.exec_client.get_trades(maker_address=maker_address)
        except Exception as e:
            logger.warning("Weather outlier delayed FAK trade lookup failed for {}: {}", self.strategy_id, e)
            return {}
        out: dict[str, dict[str, Any]] = {}
        for trade in trades or []:
            if not isinstance(trade, dict):
                continue
            oid = str(trade.get("taker_order_id") or trade.get("order_id") or "")
            if oid in order_ids:
                out[oid] = trade
        return out

    async def _reconcile_weather_outlier_delayed_entries(self, cfg: dict[str, Any]) -> None:
        """Reconcile delayed FAK BUY entries into real filled/no_fill attempts.

        CLOB commonly returns success=true/status=delayed for marketable FAK orders;
        the actual match appears a few seconds later in user trades/wallet balance.
        Until we reconcile, counting the submitted attempt as spent is safe but can
        leave stale pending exposure if the delayed FAK was ultimately canceled.
        """
        if self.exec_client is None or not hasattr(self.writer, "pending_order_attempts_by_response_order_id"):
            return
        max_age = int(cfg.get("weather_outlier_delayed_reconcile_max_age_seconds", 900) or 900)
        try:
            pending = await self.writer.pending_order_attempts_by_response_order_id(
                self.strategy_id,
                order_type="FAK_LIMIT",
                side="BUY",
                status="submitted",
                max_age_seconds=max_age,
            )
        except Exception as e:
            logger.warning("Weather outlier pending FAK lookup failed for {}: {}", self.strategy_id, e)
            return
        if not pending:
            return
        order_ids = {str((row.get("response") or {}).get("orderID") or (row.get("response") or {}).get("order_id") or "") for row in pending}
        order_ids.discard("")
        maker_address = str(
            cfg.get("polymarket_proxy_address")
            or cfg.get("proxy_address")
            or getattr(getattr(getattr(self.exec_client, "http", None), "cfg", None), "proxy_address", None)
            or os.getenv("POLYMARKET_PROXY_ADDRESS")
            or ""
        )
        trades_by_order = self._weather_outlier_trade_reconcile_rows(order_ids, maker_address)
        now = time.time()
        grace = float(cfg.get("weather_outlier_delayed_reconcile_grace_seconds", 30) or 30)
        for row in pending:
            response = row.get("response") or {}
            order_id = str(response.get("orderID") or response.get("order_id") or "")
            if not order_id:
                continue
            market_slug = str(row.get("market_slug") or "")
            token = str(row.get("token") or "")
            trade = trades_by_order.get(order_id)
            if trade:
                size = _safe_float(trade.get("size"), 0.0)
                price = _safe_float(trade.get("price"), _safe_float(row.get("price"), 0.0))
                stake = size * price if size > 0 and price > 0 else _safe_float(row.get("stake_usd"), 0.0)
                if size <= 0:
                    continue
                updated = False
                if hasattr(self.writer, "update_order_attempt_by_order_id"):
                    updated = await self.writer.update_order_attempt_by_order_id(
                        self.strategy_id,
                        order_id,
                        status="filled",
                        price=price,
                        size=size,
                        stake_usd=stake,
                        response_patch={"reconciled_trade": trade, "reconciled_at": time.time()},
                    )
                if updated:
                    self.weather_outlier_local_positions[token] = self.weather_outlier_local_positions.get(token, 0.0) + size
                    self.fill_seq += 1
                    title = market_slug[:40]
                    await self.writer.record_fill(self.strategy_id, self.fill_seq, f"{title} [OUTLIER] NO", "BUY", price, size, kind="OUTLIER")
                    await self.writer.log_strategy_event(
                        self.strategy_id,
                        f"Weather outlier delayed FAK reconciled filled: market={market_slug} order={order_id} NO@{price:.3f} size={size:.6f} notional=${stake:.2f}",
                        level="INFO",
                    )
                continue
            ts = row.get("ts")
            try:
                age = now - float(ts.timestamp())
            except Exception:
                age = grace + 1.0
            if age >= grace and hasattr(self.writer, "update_order_attempt_by_order_id"):
                updated = await self.writer.update_order_attempt_by_order_id(
                    self.strategy_id,
                    order_id,
                    status="no_fill",
                    size=0.0,
                    stake_usd=0.0,
                    response_patch={"reconciled_no_fill": True, "reconciled_at": time.time()},
                )
                if updated:
                    cooldown = max(0.0, float(cfg.get("weather_outlier_fak_no_fill_cooldown_seconds", 20) or 20))
                    if cooldown > 0 and market_slug:
                        self.weather_outlier_market_cooldown_until[market_slug] = time.time() + cooldown
                    await self.writer.log_strategy_event(
                        self.strategy_id,
                        f"Weather outlier delayed FAK reconciled no_fill: market={market_slug} order={order_id}",
                        level="INFO",
                    )

    async def _weather_outlier_daily_buy_spend_usd(self, cfg: dict[str, Any]) -> float:
        """Return this shard's successful/submitted BUY notional in the last 24h."""
        pool = getattr(self.writer, "_pool", None)
        if pool is None:
            return 0.0
        try:
            async with pool.acquire() as con:
                val = await con.fetchval(
                    """
                    SELECT COALESCE(SUM(COALESCE(stake_usd, 0)), 0)::float
                    FROM order_attempts
                    WHERE strategy_id=$1
                      AND side='BUY'
                      AND ts > now() - interval '24 hours'
                      AND (status IN ('filled','submitted','matched','delayed') OR response->>'success'='true')
                    """,
                    self.strategy_id,
                )
            return max(0.0, float(val or 0.0))
        except Exception as e:
            logger.warning("Weather outlier 24h spend lookup failed for {}: {}", self.strategy_id, e)
            return 0.0

    async def _weather_outlier_has_active_city_trade(self, cfg: dict[str, Any]) -> bool:
        """True when this city shard already has any DB/live open NO inventory.

        The live deployment can scan/trade many cities concurrently because each city
        has its own shard, but with max_active_trades_per_city=1 each shard must not
        open a second outlier until its current city inventory is closed.
        """
        if int(cfg.get("max_active_trades_per_city", 0) or 0) != 1:
            return False
        pool = getattr(self.writer, "_pool", None)
        if pool is None:
            return False
        try:
            async with pool.acquire() as con:
                val = await con.fetchval(
                    """
                    SELECT COALESCE(SUM(CASE
                      WHEN side='BUY' AND (status IN ('filled','submitted','matched','delayed') OR response->>'success'='true') THEN COALESCE(size,0)
                      WHEN side='SELL' AND (status IN ('filled','submitted','matched','delayed') OR response->>'success'='true') THEN -COALESCE(size,0)
                      ELSE 0 END), 0)::float
                    FROM order_attempts
                    WHERE strategy_id=$1 AND token<>''
                    """,
                    self.strategy_id,
                )
            if float(val or 0.0) > 0.000001:
                return True
        except Exception as e:
            logger.warning("Weather outlier active city-trade lookup failed for {}: {}", self.strategy_id, e)
        for pair in self.market_pairs or ([] if self.ev is None else [self.ev]):
            try:
                if await self._weather_outlier_open_size(pair) > 0.000001:
                    return True
            except Exception:
                continue
        return False

    async def _weather_outlier_remaining_usd_by_slug(self, cfg: dict[str, Any]) -> dict[str, float]:
        """Return remaining buy budget per outlier market based on successful/submitted BUYs."""
        order_usd = float(cfg.get("outlier_order_usd", cfg.get("order_limit_usd", 1.0)) or 1.0)
        max_tier_notional_mult = max(mult for _edge_mult, mult in _weather_outlier_rebuy_tiers(cfg))
        max_order_usd = order_usd * max_tier_notional_mult
        min_order_notional = float(cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0)) or 1.0)
        pairs = self.market_pairs or ([] if self.ev is None else [self.ev])
        remaining: dict[str, float] = {}
        spent_by_slug_token: dict[tuple[str, str], float] = {}
        slug_tokens = [(str(pair.get("slug") or ""), str(pair.get("no_token") or "")) for pair in pairs if str(pair.get("slug") or "")]
        if hasattr(self.writer, "successful_order_stake_usd_many"):
            try:
                spent_by_slug_token = await self.writer.successful_order_stake_usd_many(self.strategy_id, slug_tokens, side="BUY")
            except Exception as e:
                logger.warning("Weather outlier batch successful-stake lookup failed for {}: {}", self.strategy_id, e)
                spent_by_slug_token = {}
        for pair in pairs:
            slug = str(pair.get("slug") or "")
            if not slug:
                continue
            token = str(pair.get("no_token") or "")
            spent = spent_by_slug_token.get((slug, token), 0.0)
            if not spent and not hasattr(self.writer, "successful_order_stake_usd_many"):
                try:
                    if hasattr(self.writer, "successful_order_stake_usd"):
                        spent = await self.writer.successful_order_stake_usd(self.strategy_id, slug, side="BUY", token=token)
                    elif hasattr(self.writer, "count_successful_order_attempts"):
                        # Compatibility for tests/older writers: any successful order
                        # means at least one configured order chunk was consumed.
                        spent = order_usd if await self.writer.count_successful_order_attempts(self.strategy_id, slug) else 0.0
                except Exception as e:
                    logger.warning("Weather outlier successful-stake lookup failed for {} {}: {}", self.strategy_id, slug, e)
                    spent = 0.0
            left = max(0.0, max_order_usd - float(spent or 0.0))
            # If the remaining cap cannot clear Polymarket's per-order minimum,
            # mark it exhausted; otherwise keep it available for later liquidity.
            remaining[slug] = 0.0 if 0 < left < min_order_notional else left
        return remaining

    def _conditional_balance_shares(self, token: str) -> float:
        if self.exec_client is None:
            return 0.0
        clob = getattr(getattr(self.exec_client, "http", None), "clob", None)
        if clob is None or not hasattr(clob, "get_balance_allowance"):
            return 0.0
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams  # type: ignore
            bal = clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token))
            return max(0.0, _safe_float((bal or {}).get("balance")) / 1_000_000.0)
        except Exception as e:
            logger.debug("conditional balance lookup failed for {}: {}", token, e)
            return 0.0

    async def _weather_outlier_open_size(self, pair: dict[str, Any]) -> float:
        slug = str(pair.get("slug") or "")
        token = str(pair.get("no_token") or "")
        if token in self.weather_outlier_local_positions:
            local_size = max(0.0, self.weather_outlier_local_positions.get(token, 0.0))
            live_balance = self._conditional_balance_shares(token)
            return min(local_size, live_balance) if live_balance > 0 else local_size
        db_size = 0.0
        try:
            if hasattr(self.writer, "net_filled_order_size"):
                db_size = max(0.0, float(await self.writer.net_filled_order_size(self.strategy_id, slug, token)))
        except Exception as e:
            logger.debug("weather outlier DB position lookup failed for {} {}: {}", slug, token, e)
        if db_size > 0:
            live_balance = self._conditional_balance_shares(token)
            return min(db_size, live_balance) if live_balance > 0 else db_size
        # Avoid selling unrelated/manual holdings of the same token after a restart.
        # If this strategy has no recorded filled BUY inventory, take-profit exits
        # wait until DB/local state shows a position.
        return 0.0

    async def _weather_outlier_legacy_open_positions(self) -> list[dict[str, Any]]:
        """Return DB-known weather outlier inventory not in the current rolled event.

        Daily weather shards roll to a new event every day. Positions from the
        previous event can still be sellable at 0.999 after the runner has moved
        `market_pairs` to the next date, so take-profit must also scan strategy-
        owned historical tokens. Include submitted/success=true BUYs because CLOB
        `delayed` orders can create real token balances before reconciliation;
        legacy SELLs are still capped by live conditional-token balance before
        submission.
        """
        pool = getattr(self.writer, "_pool", None)
        if pool is None:
            return []
        current_tokens = {str(p.get("no_token") or "") for p in (self.market_pairs or ([] if self.ev is None else [self.ev]))}
        try:
            async with pool.acquire() as con:
                rows = await con.fetch(
                    """
                    WITH pos AS (
                      SELECT market_slug, token,
                             SUM(CASE
                               WHEN side='BUY' AND (status IN ('filled','submitted') OR response->>'success'='true') THEN COALESCE(size,0)
                               WHEN side='SELL' AND (status IN ('filled','submitted') OR response->>'success'='true') THEN -COALESCE(size,0)
                               ELSE 0 END)::float AS open_size,
                             SUM(CASE WHEN side='BUY' AND status='filled' THEN COALESCE(size,0) ELSE 0 END)::float AS filled_buy_size,
                             MAX(ts) AS last_attempt_ts
                      FROM order_attempts
                      WHERE strategy_id=$1 AND token<>''
                      GROUP BY market_slug, token
                    )
                    SELECT market_slug, token, open_size, filled_buy_size, last_attempt_ts
                    FROM pos
                    WHERE open_size > 0.000001 AND NOT (token = ANY($2::text[]))
                    ORDER BY last_attempt_ts DESC
                    LIMIT 100
                    """,
                    self.strategy_id,
                    sorted(t for t in current_tokens if t),
                )
            return [
                {
                    "slug": str(r["market_slug"] or ""),
                    "title": str(r["market_slug"] or ""),
                    "no_token": str(r["token"] or ""),
                    "db_open_size": float(r["open_size"] or 0.0),
                    "filled_buy_size": float(r["filled_buy_size"] or 0.0),
                }
                for r in rows
                if r["market_slug"] and r["token"]
            ]
        except Exception as e:
            logger.debug("weather outlier legacy-position lookup failed for {}: {}", self.strategy_id, e)
            return []

    async def _weather_outlier_enrich_legacy_positions(self, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add live Gamma metadata needed to sign SELLs for historical tokens.

        Legacy positions come from order_attempts after a daily weather event rolls.
        The DB rows only know slug/token/size, but CLOB signing still needs market
        metadata such as negRisk and tick size. Defaulting missing `neg_risk` to
        false can sign against the wrong exchange contract and CLOB rejects the
        order as `invalid signature`.
        """
        missing = [p for p in positions if p.get("slug") and str(p.get("slug")) not in self.weather_outlier_legacy_market_meta]
        if missing:
            timeout = httpx.Timeout(10.0, connect=2.0)
            limits = httpx.Limits(max_keepalive_connections=2, max_connections=4, keepalive_expiry=15.0)
            async with httpx.AsyncClient(timeout=timeout, limits=limits, http2=True) as c:
                for p in missing:
                    slug = str(p.get("slug") or "")
                    if not slug:
                        continue
                    try:
                        markets = await gamma_get_json(
                            c,
                            "/markets",
                            {"slug": slug},
                            cache_key=f"market-slug:{slug}",
                            cache_max_age_seconds=300.0,
                            stale_max_age_seconds=86400.0,
                            attempts=2,
                        )
                        m = markets[0] if markets else None
                        if isinstance(m, dict):
                            self.weather_outlier_legacy_market_meta[slug] = {
                                "title": str(m.get("question") or m.get("title") or slug),
                                "tick_size": _market_tick_size(m),
                                "neg_risk": _safe_bool(m.get("negRisk") or m.get("neg_risk"), False),
                            }
                    except Exception as e:
                        logger.debug("weather outlier legacy metadata lookup failed for {}: {}", slug, e)
        enriched: list[dict[str, Any]] = []
        for p in positions:
            slug = str(p.get("slug") or "")
            meta = self.weather_outlier_legacy_market_meta.get(slug, {})
            enriched.append({**p, **{k: v for k, v in meta.items() if v not in (None, "")}})
        return enriched

    async def _weather_outlier_pre_alert_higher_no_positions(self, alert_status: dict[str, Any]) -> list[dict[str, Any]]:
        pool = getattr(self.writer, "_pool", None)
        if pool is None:
            return []
        alert_cutoff = _parse_dt_utc(alert_status.get("sent") or alert_status.get("effective") or alert_status.get("onset"))
        if alert_cutoff is None:
            return []
        current_tokens = {str(p.get("no_token") or "") for p in (self.market_pairs or ([] if self.ev is None else [self.ev])) if p.get("no_token")}
        if not current_tokens:
            return []
        try:
            async with pool.acquire() as con:
                rows = await con.fetch(
                    """
                    WITH ledger AS (
                      SELECT market_slug, token,
                             SUM(CASE
                               WHEN side='BUY' AND status='filled' THEN COALESCE(size,0)
                               WHEN side='SELL' AND status='filled' THEN -COALESCE(size,0)
                               ELSE 0 END)::float AS open_size,
                             SUM(CASE
                               WHEN side='BUY' AND status='filled' AND ts < $3
                                AND signal ? 'temp_value' AND signal ? 'winning_temp'
                                AND (signal->>'temp_value')::float > (signal->>'winning_temp')::float
                               THEN COALESCE(size,0) ELSE 0 END)::float AS pre_alert_higher_size,
                             SUM(CASE
                               WHEN side='BUY' AND status='filled' AND ts < $3
                                AND signal ? 'temp_value' AND signal ? 'winning_temp'
                                AND (signal->>'temp_value')::float > (signal->>'winning_temp')::float
                               THEN COALESCE(stake_usd, price * size, 0) ELSE 0 END)::float AS pre_alert_higher_notional,
                             MIN(CASE
                               WHEN side='BUY' AND status='filled' AND ts < $3
                                AND signal ? 'temp_value' AND signal ? 'winning_temp'
                                AND (signal->>'temp_value')::float > (signal->>'winning_temp')::float
                               THEN ts ELSE NULL END) AS first_pre_alert_buy_ts
                      FROM order_attempts
                      WHERE strategy_id=$1 AND token=ANY($2::text[])
                      GROUP BY market_slug, token
                    )
                    SELECT market_slug, token, open_size, pre_alert_higher_size,
                           pre_alert_higher_notional, first_pre_alert_buy_ts
                    FROM ledger
                    WHERE open_size > 0.000001 AND pre_alert_higher_size > 0.000001
                    """,
                    self.strategy_id,
                    sorted(current_tokens),
                    alert_cutoff,
                )
            out = []
            for r in rows:
                size = min(float(r["open_size"] or 0.0), float(r["pre_alert_higher_size"] or 0.0))
                notional = float(r["pre_alert_higher_notional"] or 0.0)
                avg_price = notional / float(r["pre_alert_higher_size"] or 1.0) if notional > 0 else 0.0
                if size > 0 and avg_price > 0:
                    out.append({
                        "slug": str(r["market_slug"] or ""),
                        "token": str(r["token"] or ""),
                        "open_size": size,
                        "entry_price": avg_price,
                        "first_pre_alert_buy_ts": r["first_pre_alert_buy_ts"],
                    })
            return out
        except Exception as e:
            logger.debug("NWS heat-alert pre-alert position lookup failed for {}: {}", self.strategy_id, e)
            return []

    async def maybe_exit_weather_outlier_nws_heat_alert(self, cfg: dict[str, Any]) -> bool:
        if self.exec_client is None or not _safe_bool(cfg.get("weather_outlier_nws_heat_alert_exit_enabled", True), True):
            return False
        status = self.weather_nws_heat_alert_status
        current_event_slug = next((str(p.get("slug") or "") for p in (self.market_pairs or ([] if self.ev is None else [self.ev])) if p.get("slug")), None)
        if not _weather_nws_heat_alert_applies_to_event(status, current_event_slug):
            return False
        cooldown = float(cfg.get("weather_outlier_nws_heat_alert_exit_cooldown_seconds", 15) or 15)
        min_notional = float(cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0)) or 1.0)
        positions = await self._weather_outlier_pre_alert_higher_no_positions(status or {})
        if not positions:
            return False
        pairs_by_slug = {str(p.get("slug") or ""): p for p in (self.market_pairs or ([] if self.ev is None else [self.ev]))}
        for pos in positions:
            slug = str(pos.get("slug") or "")
            token = str(pos.get("token") or "")
            pair = pairs_by_slug.get(slug)
            if not pair or not token:
                continue
            books = self.books_by_slug.get(slug, self.books)
            no_book = books["NO"]
            entry_price = max(0.001, min(0.999, float(pos.get("entry_price") or 0.0)))
            bid = float(no_book.bid or 0.0)
            tick_size = str(no_book.tick_size or pair.get("tick_size") or cfg.get("tick_size") or "0.01")
            if not _price_matches_tick(entry_price, tick_size):
                await self.writer.log_strategy_event(self.strategy_id, f"NWS heat-alert exit skipped: market={slug} entry_price={entry_price:.4f} invalid for tick_size={tick_size}; refusing lower sell", level="WARNING")
                continue
            if bid + 1e-9 < entry_price:
                continue
            now = time.time()
            if cooldown > 0 and now - self.weather_outlier_last_take_profit_ts.get(token, 0.0) < cooldown:
                continue
            open_size = await self._weather_outlier_open_size(pair)
            open_size = min(open_size, float(pos.get("open_size") or 0.0))
            if open_size <= 0:
                continue
            bid_depth = sum(_safe_float(level.get("size")) for level in (no_book.bids or [{"price": bid, "size": open_size}]) if _safe_float(level.get("price")) + 1e-9 >= entry_price)
            sell_size = round(min(open_size, bid_depth), 4)
            if sell_size <= 0 or sell_size * entry_price + 1e-9 < min_notional:
                continue
            neg_risk = _safe_bool(no_book.neg_risk if no_book.neg_risk is not None else pair.get("neg_risk", cfg.get("neg_risk", False)), False)
            order = PolyOrder(
                token_id=token,
                side="SELL",
                price=Decimal(str(entry_price)),
                size=Decimal(str(sell_size)),
                order_type="FOK",
                post_only=False,
                use_limit_order=True,
                tick_size=tick_size,
                neg_risk=neg_risk,
                builder_code=str(cfg.get("builder_code") or os.getenv("POLYMARKET_BUILDER_CODE") or os.getenv("POLY_BUILDER_CODE") or "") or None,
            )
            try:
                resp = self.exec_client.submit(order)
                response = resp if isinstance(resp, dict) else {"raw": resp}
                if clob_response_indicates_fill(response):
                    order_status = "filled"
                elif response.get("success") is True and (response.get("orderID") or response.get("order_id")):
                    order_status = "submitted"
                else:
                    order_status = "rejected"
                err = clob_response_error(response)
            except Exception as exc:
                response = {}
                order_status = "error"
                err = f"{type(exc).__name__}: {exc}"
            self.weather_outlier_last_take_profit_ts[token] = time.time()
            if order_status == "filled":
                self.weather_outlier_local_positions[token] = max(0.0, self.weather_outlier_local_positions.get(token, open_size) - sell_size)
            await self.writer.record_order_attempt(
                self.strategy_id, slug, token, "NO", "SELL", "FOK_NWS_HEAT_ALERT_EXIT", entry_price, sell_size, entry_price * sell_size, order_status,
                response=response, error=err,
                signal={"strategy": "weather_outlier_sniper", "nws_heat_alert_exit": True, "entry_price": entry_price, "bid": bid, "tick_size": tick_size, "neg_risk": neg_risk, "alert": status}, config=cfg,
            )
            if order_status == "filled":
                self.fill_seq += 1
                await self.writer.record_fill(self.strategy_id, self.fill_seq, f"{slug[:40]} [NWS HEAT EXIT] NO", "SELL", entry_price, sell_size, kind="OUTLIER_NWS_HEAT_EXIT")
            await self.writer.log_strategy_event(
                self.strategy_id,
                f"NWS heat-alert exit {order_status}: market={slug} NO@{entry_price:.3f} bid={bid:.3f} size={sell_size:.4f} notional=${entry_price * sell_size:.2f} alert={str((status or {}).get('event') or '')} response={str(err or response.get('status') or response.get('orderID') or response.get('order_id') or 'no_response')[:180]}",
                level="INFO" if order_status in {"filled", "submitted"} else "WARNING",
            )
            await self._flush_state(cfg)
            return order_status in {"filled", "submitted"}
        return False

    async def maybe_take_profit_weather_outlier(self, cfg: dict[str, Any]) -> bool:
        threshold = float(cfg.get("outlier_take_profit_price", cfg.get("take_profit_price", 0.999)) or 0.0)
        if threshold <= 0 or self.exec_client is None:
            return False
        threshold = max(0.001, min(0.999, threshold))
        min_notional = float(cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0)) or 1.0)
        cooldown = float(cfg.get("take_profit_cooldown_seconds", 15) or 15)
        for pair in self.market_pairs or ([] if self.ev is None else [self.ev]):
            slug = str(pair.get("slug") or "")
            token = str(pair.get("no_token") or "")
            books = self.books_by_slug.get(slug, self.books)
            no_book = books["NO"]
            bid = float(no_book.bid or 0.0)
            tick_size = str(no_book.tick_size or pair.get("tick_size") or cfg.get("tick_size") or "0.01")
            if not _price_matches_tick(threshold, tick_size):
                if bid + 1e-9 >= threshold:
                    key = f"weather_outlier_tp_tick_skip:{slug}:{threshold}:{tick_size}"
                    now_log = time.time()
                    if now_log - self.last_skip_log.get(key, 0.0) >= 300:
                        self.last_skip_log[key] = now_log
                        await self.writer.log_strategy_event(
                            self.strategy_id,
                            f"Weather outlier take-profit skipped: configured_threshold={threshold:.3f} is not valid for market tick_size={tick_size}; refusing to sell lower (bid={bid:.3f})",
                            level="WARNING",
                        )
                continue
            if bid + 1e-9 < threshold:
                continue
            now = time.time()
            if cooldown > 0 and now - self.weather_outlier_last_take_profit_ts.get(token, 0.0) < cooldown:
                continue
            open_size = await self._weather_outlier_open_size(pair)
            if open_size <= 0:
                continue
            bid_depth = sum(_safe_float(level.get("size")) for level in (no_book.bids or [{"price": bid, "size": open_size}]) if _safe_float(level.get("price")) + 1e-9 >= threshold)
            sell_size = round(min(open_size, bid_depth), 4)
            if sell_size <= 0 or sell_size * threshold + 1e-9 < min_notional:
                continue
            neg_risk = _safe_bool(no_book.neg_risk if no_book.neg_risk is not None else pair.get("neg_risk", cfg.get("neg_risk", False)), False)
            order = PolyOrder(
                token_id=token,
                side="SELL",
                price=Decimal(str(threshold)),
                size=Decimal(str(sell_size)),
                order_type="FOK",
                post_only=False,
                use_limit_order=True,
                tick_size=tick_size,
                neg_risk=neg_risk,
                builder_code=str(cfg.get("builder_code") or os.getenv("POLYMARKET_BUILDER_CODE") or os.getenv("POLY_BUILDER_CODE") or "") or None,
            )
            try:
                resp = self.exec_client.submit(order)
                response = resp if isinstance(resp, dict) else {"raw": resp}
                if clob_response_indicates_fill(response):
                    status = "filled"
                elif response.get("success") is True and (response.get("orderID") or response.get("order_id")):
                    status = "submitted"
                else:
                    status = "rejected"
                err = clob_response_error(response)
            except Exception as exc:
                response = {}
                status = "error"
                err = f"{type(exc).__name__}: {exc}"
            self.weather_outlier_last_take_profit_ts[token] = time.time()
            if status == "filled":
                self.weather_outlier_local_positions[token] = max(0.0, self.weather_outlier_local_positions.get(token, open_size) - sell_size)
            title = str(pair.get("title") or slug)[:40]
            await self.writer.record_order_attempt(
                self.strategy_id, slug, token, "NO", "SELL", "FOK_TAKE_PROFIT", threshold, sell_size, threshold * sell_size, status,
                response=response, error=err,
                signal={"strategy": "weather_outlier_sniper", "take_profit": True, "threshold": threshold, "bid": bid, "tick_size": tick_size, "neg_risk": neg_risk}, config=cfg,
            )
            if status == "filled":
                self.fill_seq += 1
                await self.writer.record_fill(self.strategy_id, self.fill_seq, f"{title} [OUTLIER TP] NO", "SELL", threshold, sell_size, kind="OUTLIER_TAKE_PROFIT")
            await self.writer.log_strategy_event(
                self.strategy_id,
                f"Weather outlier take-profit {status}: market={slug} NO@{threshold:.3f} bid={bid:.3f} size={sell_size:.4f} notional=${threshold * sell_size:.2f} response={str(err or response.get('status') or response.get('orderID') or response.get('order_id') or 'no_response')[:180]}",
                level="INFO" if status in {"filled", "submitted"} else "WARNING",
            )
            await self._flush_state(cfg)
            return status in {"filled", "submitted"}

        legacy_positions = await self._weather_outlier_enrich_legacy_positions(await self._weather_outlier_legacy_open_positions())
        if legacy_positions:
            tokens = [str(p.get("no_token") or "") for p in legacy_positions if p.get("no_token")]
            async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=1.0), limits=httpx.Limits(max_keepalive_connections=2, max_connections=4, keepalive_expiry=15.0), http2=True) as c:
                legacy_books = await rest_books_full(c, tokens)
            for pair in legacy_positions:
                slug = str(pair.get("slug") or "")
                token = str(pair.get("no_token") or "")
                no_book = legacy_books.get(token)
                if no_book is None:
                    continue
                bid = float(no_book.bid or 0.0)
                tick_size = str(no_book.tick_size or pair.get("tick_size") or cfg.get("tick_size") or "0.01")
                if not _price_matches_tick(threshold, tick_size):
                    if bid + 1e-9 >= threshold:
                        key = f"weather_outlier_legacy_tp_tick_skip:{slug}:{threshold}:{tick_size}"
                        now_log = time.time()
                        if now_log - self.last_skip_log.get(key, 0.0) >= 300:
                            self.last_skip_log[key] = now_log
                            await self.writer.log_strategy_event(
                                self.strategy_id,
                                f"Weather outlier legacy take-profit skipped: configured_threshold={threshold:.3f} is not valid for market tick_size={tick_size}; refusing to sell lower (bid={bid:.3f})",
                                level="WARNING",
                            )
                    continue
                if bid + 1e-9 < threshold:
                    continue
                now = time.time()
                if cooldown > 0 and now - self.weather_outlier_last_take_profit_ts.get(token, 0.0) < cooldown:
                    continue
                live_balance = self._conditional_balance_shares(token)
                if live_balance <= 0:
                    continue
                open_size = min(float(pair.get("db_open_size") or 0.0), live_balance)
                if open_size <= 0:
                    continue
                bid_depth = sum(_safe_float(level.get("size")) for level in (no_book.bids or [{"price": bid, "size": open_size}]) if _safe_float(level.get("price")) + 1e-9 >= threshold)
                sell_size = round(min(open_size, bid_depth), 4)
                if sell_size <= 0 or sell_size * threshold + 1e-9 < min_notional:
                    continue
                neg_risk = _safe_bool(no_book.neg_risk if no_book.neg_risk is not None else pair.get("neg_risk", cfg.get("neg_risk", False)), False)
                order = PolyOrder(
                    token_id=token,
                    side="SELL",
                    price=Decimal(str(threshold)),
                    size=Decimal(str(sell_size)),
                    order_type="FOK",
                    post_only=False,
                    use_limit_order=True,
                    tick_size=tick_size,
                    neg_risk=neg_risk,
                    builder_code=str(cfg.get("builder_code") or os.getenv("POLYMARKET_BUILDER_CODE") or os.getenv("POLY_BUILDER_CODE") or "") or None,
                )
                try:
                    resp = self.exec_client.submit(order)
                    response = resp if isinstance(resp, dict) else {"raw": resp}
                    if clob_response_indicates_fill(response):
                        status = "filled"
                    elif response.get("success") is True and (response.get("orderID") or response.get("order_id")):
                        status = "submitted"
                    else:
                        status = "rejected"
                    err = clob_response_error(response)
                except Exception as exc:
                    response = {}
                    status = "error"
                    err = f"{type(exc).__name__}: {exc}"
                self.weather_outlier_last_take_profit_ts[token] = time.time()
                if status == "filled":
                    self.weather_outlier_local_positions[token] = max(0.0, self.weather_outlier_local_positions.get(token, open_size) - sell_size)
                title = str(pair.get("title") or slug)[:40]
                await self.writer.record_order_attempt(
                    self.strategy_id, slug, token, "NO", "SELL", "FOK_TAKE_PROFIT", threshold, sell_size, threshold * sell_size, status,
                    response=response, error=err,
                    signal={"strategy": "weather_outlier_sniper", "take_profit": True, "legacy_position": True, "threshold": threshold, "bid": bid, "tick_size": tick_size, "neg_risk": neg_risk}, config=cfg,
                )
                if status == "filled":
                    self.fill_seq += 1
                    await self.writer.record_fill(self.strategy_id, self.fill_seq, f"{title} [OUTLIER TP] NO", "SELL", threshold, sell_size, kind="OUTLIER_TAKE_PROFIT")
                await self.writer.log_strategy_event(
                    self.strategy_id,
                    f"Weather outlier legacy take-profit {status}: market={slug} NO@{threshold:.3f} bid={bid:.3f} size={sell_size:.4f} notional=${threshold * sell_size:.2f} response={str(err or response.get('status') or response.get('orderID') or response.get('order_id') or 'no_response')[:180]}",
                    level="INFO" if status in {"filled", "submitted"} else "WARNING",
                )
                await self._flush_state(cfg)
                return status in {"filled", "submitted"}
        return False

    async def maybe_execute_weather_outlier(self, cfg: dict[str, Any]) -> None:
        if self.exec_client is None or self.ev is None:
            return
        if self.trading_lock.locked():
            return
        if float(cfg.get("cooldown_ms", 0) or 0) > 0 and (time.time() - self.last_trade_ts) * 1000 < float(cfg.get("cooldown_ms", 0) or 0):
            return
        if self.weather_outlier_order_pause_until > time.time():
            return
        # A blacklisted city must not open new positions, but allow risk exits and
        # take-profit exits for any inventory acquired before it was blacklisted.
        if await self.maybe_exit_weather_outlier_nws_heat_alert(cfg):
            return
        if await self.maybe_take_profit_weather_outlier(cfg):
            return
        if _weather_outlier_is_blacklisted(cfg):
            return
        await self._reconcile_weather_outlier_delayed_entries(cfg)
        safety_ok, safety_size_mult, safety_result = await self._weather_safety_allows_new_buy(cfg)
        if not safety_ok:
            return
        plan_cfg = self._weather_safety_adjusted_plan_cfg(cfg, safety_result)
        max_attempts = int(cfg.get("max_executed_orders", cfg.get("order_limit_count", 0)) or 0)
        if max_attempts and self.executed_attempts >= max_attempts:
            await self.writer.set_strategy_status(self.strategy_id, "stopped")
            return
        if await self._weather_outlier_has_active_city_trade(cfg):
            await self.writer.log_strategy_event(self.strategy_id, "Weather outlier entry skipped: max_active_trades_per_city=1 and this city already has open DB/live inventory", level="INFO")
            return
        daily_limit = float(cfg.get("weather_outlier_daily_limit_usd", cfg.get("daily_limit_usd", 0)) or 0)
        daily_remaining = None
        if daily_limit > 0:
            daily_spent = await self._weather_outlier_daily_buy_spend_usd(cfg)
            daily_remaining = max(0.0, daily_limit - daily_spent)
            if daily_remaining < float(cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0)) or 1.0):
                return
        remaining_usd_by_slug = await self._weather_outlier_remaining_usd_by_slug(plan_cfg)
        if daily_remaining is not None:
            remaining_usd_by_slug = {slug: min(left, daily_remaining) for slug, left in remaining_usd_by_slug.items()}
        direction_lock_by_event = await self._weather_outlier_direction_lock_by_event()
        now_ts = time.time()
        excluded_slugs = {slug for slug, left in remaining_usd_by_slug.items() if left <= 1e-9}
        excluded_slugs.update(slug for slug, until in self.weather_outlier_market_cooldown_until.items() if until > now_ts)
        diagnostics: list[dict[str, Any]] = []
        plan = self._weather_outlier_plan_from_cfg(
            plan_cfg,
            exclude_market_slugs=excluded_slugs,
            remaining_usd_by_slug=remaining_usd_by_slug,
            direction_lock_by_event=direction_lock_by_event,
            safety_result=safety_result,
            diagnostics=diagnostics,
        )
        if plan is None:
            await self._log_weather_outlier_blocked_entry(diagnostics, plan_cfg, safety_result)
            return
        async with self.trading_lock:
            safety_ok, safety_size_mult, safety_result = await self._weather_safety_allows_new_buy(cfg)
            if not safety_ok:
                return
            plan_cfg = self._weather_safety_adjusted_plan_cfg(cfg, safety_result)
            remaining_usd_by_slug = await self._weather_outlier_remaining_usd_by_slug(plan_cfg)
            daily_remaining = None
            daily_limit = float(cfg.get("weather_outlier_daily_limit_usd", cfg.get("daily_limit_usd", 0)) or 0)
            if daily_limit > 0:
                daily_spent = await self._weather_outlier_daily_buy_spend_usd(cfg)
                daily_remaining = max(0.0, daily_limit - daily_spent)
                if daily_remaining < float(cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0)) or 1.0):
                    return
            if daily_remaining is not None:
                remaining_usd_by_slug = {slug: min(left, daily_remaining) for slug, left in remaining_usd_by_slug.items()}
            direction_lock_by_event = await self._weather_outlier_direction_lock_by_event()
            now_ts = time.time()
            excluded_slugs = {slug for slug, left in remaining_usd_by_slug.items() if left <= 1e-9}
            excluded_slugs.update(slug for slug, until in self.weather_outlier_market_cooldown_until.items() if until > now_ts)
            diagnostics = []
            plan = self._weather_outlier_plan_from_cfg(
                plan_cfg,
                exclude_market_slugs=excluded_slugs,
                remaining_usd_by_slug=remaining_usd_by_slug,
                direction_lock_by_event=direction_lock_by_event,
                safety_result=safety_result,
                diagnostics=diagnostics,
            )
            if plan is None:
                await self._log_weather_outlier_blocked_entry(diagnostics, plan_cfg, safety_result)
                return
            revalidated = False
            for _attempt in range(2):
                ok, reason = await self._revalidate_weather_outlier_candidate_books(plan, plan_cfg)
                if not ok:
                    await self.writer.log_strategy_event(
                        self.strategy_id,
                        f"Weather outlier entry revalidation blocked: market={plan.pair.get('slug')} reason={reason} execution_max_book_age_ms={self._weather_execution_max_book_age_ms(plan_cfg):.0f}",
                        level="INFO",
                    )
                    return
                revalidated = True
                refreshed_plan = self._weather_outlier_plan_from_cfg(
                    plan_cfg,
                    exclude_market_slugs=excluded_slugs,
                    remaining_usd_by_slug=remaining_usd_by_slug,
                    direction_lock_by_event=direction_lock_by_event,
                    safety_result=safety_result,
                    diagnostics=[],
                )
                if refreshed_plan is None:
                    await self.writer.log_strategy_event(self.strategy_id, "Weather outlier entry revalidation blocked: opportunity disappeared after fresh book refresh", level="INFO")
                    return
                if refreshed_plan.pair.get("slug") == plan.pair.get("slug"):
                    plan = refreshed_plan
                    break
                plan = refreshed_plan
            if not revalidated:
                return
            ok, reason = self._weather_outlier_plan_fresh_for_execution(plan, plan_cfg)
            if not ok:
                await self.writer.log_strategy_event(
                    self.strategy_id,
                    f"Weather outlier entry blocked after replan: market={plan.pair.get('slug')} reason={reason} execution_max_book_age_ms={self._weather_execution_max_book_age_ms(plan_cfg):.0f}",
                    level="INFO",
                )
                return
            # Prefer token/market metadata resolved from Gamma over strategy-wide
            # config defaults.  A stale/default config tick of 0.01 can make a
            # valid 0.999 order fail on 0.001-tick weather markets.
            tick_size = str(plan.pair.get("tick_size") or cfg.get("tick_size") or "0.01")
            neg_risk = _safe_bool(plan.pair.get("neg_risk", cfg.get("neg_risk", False)), False)
            # Weather outlier entries are taker sweeps with a hard edge cap. Use
            # FAK so any immediately executable liquidity at or below max_no_price
            # is captured, while the unfilled remainder is canceled. Size is set so
            # the SDK market-buy path receives plan.notional as the pUSD budget
            # (it computes amount as price * size); this keeps spend capped even
            # when fills occur below the worst accepted price.
            execution_order_type = str(cfg.get("weather_outlier_entry_order_type") or "FAK").upper()
            if execution_order_type not in {"FAK", "FOK"}:
                execution_order_type = "FAK"
            execution_limit = plan.max_no_price if execution_order_type == "FAK" else plan.ask
            execution_size = plan.size
            if execution_order_type == "FAK" and execution_limit > 0:
                execution_size = max(0.0, plan.notional / execution_limit)
            order = PolyOrder(
                token_id=plan.token,
                side="BUY",
                price=Decimal(str(execution_limit)),
                size=Decimal(str(execution_size)),
                order_type=execution_order_type,
                post_only=False,
                use_limit_order=False,
                tick_size=tick_size,
                neg_risk=neg_risk,
                builder_code=str(cfg.get("builder_code") or os.getenv("POLYMARKET_BUILDER_CODE") or os.getenv("POLY_BUILDER_CODE") or "") or None,
            )
            matched_size = 0.0
            matched_notional = 0.0
            try:
                resp = self.exec_client.submit(order)
                response = resp if isinstance(resp, dict) else {"raw": resp}
                matched_size = clob_response_matched_size(response, plan.size)
                matched_notional = clob_response_matched_notional(response, 0.0)
                if matched_size > 0 or clob_response_indicates_fill(response):
                    status = "filled"
                    if matched_notional <= 0 and matched_size > 0:
                        matched_notional = min(plan.notional, matched_size * execution_limit)
                    if matched_notional <= 0:
                        matched_notional = plan.notional
                elif response.get("success") is True and (response.get("orderID") or response.get("order_id")):
                    status = "submitted"
                elif response.get("success") is True and str(response.get("status") or "").strip().lower() in {"canceled", "cancelled", "unmatched"}:
                    status = "no_fill"
                else:
                    status = "rejected"
                err = clob_response_error(response)
            except Exception as exc:
                response = {}
                err = f"{type(exc).__name__}: {exc}"
                # CLOB raises this for a FAK order whose quoted liquidity vanished
                # between our book snapshot and submit. This is an expected missed
                # execution / opportunity-disappeared outcome, not an infra error.
                status = "no_fill" if execution_order_type == "FAK" and "no orders found to match with FAK order" in err else "error"
            self.last_trade_ts = time.time()
            self.executed_attempts += 1
            title = str(plan.pair.get("title") or plan.pair.get("slug") or "")[:40]
            market_slug = str(plan.pair.get("slug") or cfg.get("market_slug") or "")
            await self.writer.record_order_attempt(
                self.strategy_id,
                market_slug,
                plan.token,
                "NO",
                "BUY",
                "FAK_LIMIT" if execution_order_type == "FAK" else "FOK_MARKET",
                execution_limit,
                matched_size if status == "filled" and matched_size > 0 else plan.size,
                matched_notional if status == "filled" and matched_notional > 0 else plan.notional,
                status,
                response=response,
                error=err,
                signal={
                    "strategy": "weather_outlier_sniper",
                    "temp_value": plan.temp_value,
                    "winning_temp": plan.winning_temp,
                    "winning_price": plan.winning_price,
                    "distance_degrees": plan.distance_degrees,
                    "offset_degrees": float(cfg.get("outlier_temperature_offset_degrees", 4) or 4),
                    "min_edge": plan.min_edge,
                    "max_no_price": plan.max_no_price,
                    "execution_order_type": execution_order_type,
                    "execution_limit_price": execution_limit,
                    "tick_size": tick_size,
                    "neg_risk": neg_risk,
                    "planned_worst_book_price": plan.ask,
                    "planned_size": plan.size,
                    "planned_notional": plan.notional,
                    "matched_size": matched_size,
                    "matched_notional": matched_notional,
                    "tier_edge_multiplier": plan.tier_edge_multiplier,
                    "tier_notional_multiplier": plan.tier_notional_multiplier,
                    "tier_target_notional": plan.tier_target_notional,
                    "tier_remaining_notional": plan.tier_remaining_notional,
                    "boundary_veto_threshold_c": _weather_boundary_veto_threshold_c(cfg),
                    "boundary_veto_threshold_f": _weather_boundary_veto_threshold_c(cfg) * 9.0 / 5.0,
                    "boundary_forecast_high_c": plan.boundary_forecast_high_c,
                    "boundary_forecast_high_f": c_to_f(plan.boundary_forecast_high_c) if plan.boundary_forecast_high_c is not None else None,
                    "boundary_distance_c": plan.boundary_distance_c,
                    "side": "NO",
                    "max_orders_per_market": int(cfg.get("max_orders_per_market", 1) or 0),
                    "weather_safety_filter_enabled": _safe_bool(cfg.get("weather_safety_filter_enabled", False), False),
                    "weather_safety_gate": (safety_result or {}).get("gate") if safety_result else None,
                    "weather_safety_size_multiplier": safety_size_mult,
                    "weather_safety_expected_fluctuation_c": (safety_result or {}).get("expected_temp_fluctuation_c") if safety_result else None,
                },
                config=cfg,
            )
            actual_size = matched_size if status == "filled" and matched_size > 0 else plan.size
            actual_notional = matched_notional if status == "filled" and matched_notional > 0 else plan.notional
            actual_price = min(execution_limit, actual_notional / actual_size) if actual_size > 0 and actual_notional > 0 else execution_limit
            if status == "filled":
                self.weather_outlier_local_positions[plan.token] = self.weather_outlier_local_positions.get(plan.token, 0.0) + actual_size
                self.fill_seq += 1
                await self.writer.record_fill(self.strategy_id, self.fill_seq, f"{title} [OUTLIER] NO", "BUY", actual_price, actual_size, kind="OUTLIER")
            if execution_order_type == "FAK" and status in {"no_fill", "rejected", "error"}:
                cooldown = max(0.0, float(cfg.get("weather_outlier_fak_no_fill_cooldown_seconds", 20) or 20))
                if cooldown > 0:
                    self.weather_outlier_market_cooldown_until[market_slug] = time.time() + cooldown
            elif execution_order_type == "FAK" and status == "filled" and actual_notional + 1e-9 < plan.notional:
                cooldown = max(0.0, float(cfg.get("weather_outlier_fak_partial_cooldown_seconds", 5) or 5))
                if cooldown > 0:
                    self.weather_outlier_market_cooldown_until[market_slug] = time.time() + cooldown
            await self.writer.log_strategy_event(
                self.strategy_id,
                f"Weather outlier {status}: market={market_slug} temp={plan.temp_value:g} winning_temp={plan.winning_temp:g} winning_yes={plan.winning_price:.3f} distance={plan.distance_degrees:g}° offset={float(cfg.get('outlier_temperature_offset_degrees', 4) or 4):g}° min_edge={plan.min_edge:.3f} max_no={plan.max_no_price:.3f} tier={plan.tier_edge_multiplier:g}x→{plan.tier_notional_multiplier:g}x max_orders_per_market={int(cfg.get('max_orders_per_market', 1) or 0)} order={execution_order_type}@{execution_limit:.3f} planned_NO@{plan.ask:.3f} size={actual_size:.4f}/{plan.size:.4f} notional=${actual_notional:.2f}/${plan.notional:.2f} response={str(err or response.get('status') or response.get('orderID') or response.get('order_id') or 'no_response')[:180]}",
                level="INFO" if status in {"filled", "submitted", "no_fill"} else "WARNING",
            )
            err_text = str(err or response).lower()
            if status != "filled" and any(s in err_text for s in ("not enough balance", "insufficient balance", "allowance")):
                pause_seconds = max(30.0, float(cfg.get("balance_error_pause_seconds", 300) or 300))
                self.weather_outlier_order_pause_until = time.time() + pause_seconds
                self.weather_outlier_order_pause_reason = err_text[:240]
                await self.writer.log_strategy_event(
                    self.strategy_id,
                    f"Weather outlier order submissions paused for {pause_seconds:.0f}s: balance/allowance is insufficient for ${plan.notional:.2f} order. Fund/approve the Polymarket profile, then the live runner will resume automatically.",
                    level="ERROR",
                )
            await self._flush_state(cfg)
            max_attempts = int(cfg.get("max_executed_orders", cfg.get("order_limit_count", 0)) or 0)
            if max_attempts and self.executed_attempts >= max_attempts:
                await self.writer.log_strategy_event(self.strategy_id, f"Weather outlier order attempt limit reached ({self.executed_attempts}); stopping.")
                await self.writer.set_strategy_status(self.strategy_id, "stopped")

    async def maybe_execute_hot_path(self, cfg: dict[str, Any]) -> None:
        if self._is_weather_outlier_strategy(cfg):
            await self.maybe_execute_weather_outlier(cfg)
            return
        if self.exec_client is None or self.ev is None:
            return
        if any(v > 1e-9 for v in self.residual_inventory.values()):
            # Residual one-sided inventory is held automatically until resolution; do not open new arbs.
            return
        if self.trading_lock.locked():
            return
        if float(cfg.get("cooldown_ms", 0) or 0) > 0 and (time.time() - self.last_trade_ts) * 1000 < float(cfg.get("cooldown_ms", 0) or 0):
            return
        max_attempts = int(cfg.get("max_executed_orders", cfg.get("order_limit_count", 0)) or 0)
        if max_attempts and self.executed_attempts >= max_attempts:
            await self.writer.set_strategy_status(self.strategy_id, "stopped")
            return
        candidate_pair: dict[str, Any] | None = None
        candidate_plan: ArbPlan | None = None
        best_skip_pair: dict[str, Any] | None = None
        best_skip_diag: ArbSkipDiagnostic | None = None
        for pair in self.market_pairs or [self.ev]:
            if not self._entry_window_open(cfg, pair):
                continue
            plan = self._plan_from_cfg(cfg, pair)
            if plan is not None and (candidate_plan is None or plan.edge_per_pair > candidate_plan.edge_per_pair):
                candidate_pair, candidate_plan = pair, plan
            elif plan is None:
                diag = self._skip_diagnostic_from_cfg(cfg, pair)
                if diag and diag.opportunity_spotted:
                    test_plan = self._test_mode_plan_from_books(cfg, pair, diag)
                    if test_plan is not None and (candidate_plan is None or test_plan.edge_per_pair > candidate_plan.edge_per_pair):
                        candidate_pair, candidate_plan = pair, test_plan
                    if best_skip_diag is None or diag.edge > best_skip_diag.edge:
                        best_skip_pair, best_skip_diag = pair, diag
        if candidate_pair is None or candidate_plan is None:
            if best_skip_pair is not None and best_skip_diag is not None:
                await self._log_skipped_opportunity(best_skip_pair, best_skip_diag, cfg)
            return
        async with self.trading_lock:
            if not self._entry_window_open(cfg, candidate_pair):
                return
            # Re-plan inside lock using latest books; no DB/log writes before submit.
            plan = self._plan_from_cfg(cfg, candidate_pair)
            if plan is None:
                diag = self._skip_diagnostic_from_cfg(cfg, candidate_pair)
                plan = self._test_mode_plan_from_books(cfg, candidate_pair, diag) if diag else None
                if plan is None:
                    if diag:
                        await self._log_skipped_opportunity(candidate_pair, diag, cfg)
                    return
            t0 = time.perf_counter()
            try:
                ok, pnl, filled_legs, responses, state = self._submit_pair(candidate_pair, plan, cfg)
            except Exception as e:
                self.submit_latency.add((time.perf_counter() - t0) * 1000)
                self.last_trade_ts = time.time()
                self.executed_attempts += 1
                await self._record_submit_exception(candidate_pair, plan, cfg, e)
                return
            self.submit_latency.add((time.perf_counter() - t0) * 1000)
            self.last_trade_ts = time.time()
            self.executed_attempts += 1
            await self._record_execution(candidate_pair, plan, ok, pnl, filled_legs, responses, cfg, state)
            await self._flush_state(cfg, pnl=pnl)
            max_attempts = int(cfg.get("max_executed_orders", cfg.get("order_limit_count", 0)) or 0)
            if max_attempts and self.executed_attempts >= max_attempts:
                await self.writer.log_strategy_event(self.strategy_id, f"Order attempt limit reached ({self.executed_attempts}); stopping.")
                await self.writer.set_strategy_status(self.strategy_id, "stopped")

    def _test_mode_lower_leg_plan(self, plan: ArbPlan, cfg: dict[str, Any]) -> tuple[ArbPlan, Leg] | None:
        """Build a one-leg test order when the cheap leg is below CLOB $1 minimum.

        Normal sniper mode tries to batch exact-share YES+NO FOK legs. In extreme
        skew, the cheap leg can be e.g. $0.43 notional and CLOB rejects it with
        `invalid amount ... min size: $1`. When dashboard test mode is enabled,
        intentionally submit only the lower-notional leg sized to the configured
        test minimum so we can verify live signing/fills with limited exposure.
        """
        if not (_safe_bool(cfg.get("arb_test_mode", False), False) or _safe_bool(cfg.get("test_mode", False), False)):
            return None
        min_notional = float(cfg.get("test_mode_min_notional_usd", cfg.get("min_order_notional_usd", cfg.get("min_order_notional", 1.0))) or 1.0)
        if min_notional <= 0:
            return None
        cost_by_leg = {"YES": plan.yes_cost_est, "NO": plan.no_cost_est}
        px_by_leg = {"YES": plan.yes_limit, "NO": plan.no_limit}
        lower: Leg = "YES" if cost_by_leg["YES"] <= cost_by_leg["NO"] else "NO"
        if cost_by_leg[lower] + 1e-9 >= min_notional:
            return None
        px = max(px_by_leg[lower], 1e-9)
        increment = float(cfg.get("share_size_increment", 1.0 if cfg.get("use_limit_fok", True) else 0.0001) or 0.0001)
        if increment <= 0:
            increment = 0.0001
        size = round(math.ceil((min_notional / px) / increment - 1e-12) * increment, 4)
        if size <= 0:
            return None
        yes_size = size if lower == "YES" else 0.0
        no_size = size if lower == "NO" else 0.0
        yes_cost = round(plan.yes_limit * yes_size, 4)
        no_cost = round(plan.no_limit * no_size, 4)
        test_plan = ArbPlan(
            yes_size=yes_size,
            no_size=no_size,
            size=size,
            yes_limit=plan.yes_limit,
            no_limit=plan.no_limit,
            yes_cost_est=yes_cost,
            no_cost_est=no_cost,
            total_cost_est=round(yes_cost + no_cost, 4),
            avg_sum_est=plan.avg_sum_est,
            edge_per_pair=plan.edge_per_pair,
            first_leg=lower,
            second_leg="NO" if lower == "YES" else "YES",
        )
        return test_plan, lower

    def _submit_pair(self, pair: dict[str, Any] | ArbPlan, plan: ArbPlan | dict[str, Any], cfg: dict[str, Any] | None = None) -> tuple[bool, float, list[Leg], dict[Leg, dict[str, Any]], str]:
        # Backward-compatible call shape for tests/old code: _submit_pair(plan, cfg).
        if cfg is None:
            cfg = plan if isinstance(plan, dict) else {}
            plan = pair  # type: ignore[assignment]
            pair = self.ev or {}
        assert self.exec_client is not None and isinstance(plan, ArbPlan) and isinstance(pair, dict)
        token_by_leg = {"YES": pair["yes_token"], "NO": pair["no_token"]}
        px_by_leg = {"YES": plan.yes_limit, "NO": plan.no_limit}
        size_by_leg = {"YES": plan.yes_size, "NO": plan.no_size}
        unequal_fixed_sizes = abs(plan.yes_size - plan.no_size) > 1e-9
        use_limit_fok = bool(cfg.get("use_limit_fok", True))
        tick_size = str(cfg.get("tick_size") or pair.get("tick_size") or "0.01")
        neg_risk_cfg = cfg.get("neg_risk")
        neg_risk = _safe_bool(pair.get("neg_risk", False) if neg_risk_cfg is None else neg_risk_cfg, False)
        builder_code = cfg.get("builder_code") or os.getenv("POLYMARKET_BUILDER_CODE") or os.getenv("POLY_BUILDER_CODE")
        responses: dict[Leg, dict[str, Any]] = {}
        filled_legs: list[Leg] = []

        planned_notional_by_leg = {"YES": float(plan.yes_cost_est), "NO": float(plan.no_cost_est)}

        def make_buy(leg: Leg, price: float, size: float) -> PolyOrder:
            use_limit_order = use_limit_fok and not unequal_fixed_sizes
            order_size = float(size)
            if not use_limit_order and unequal_fixed_sizes:
                # In the SDK market-buy path, Polymarket's `amount` is derived by
                # the adapter as price * size.  If we widen the FOK limit from the
                # top ask to the profit-preserving cap, keep the stake fixed by
                # shrinking this synthetic size so `amount` stays at the planned
                # cents-denominated leg notional (usually exactly $1.00).
                notional = max(0.0, planned_notional_by_leg.get(leg, price * order_size))
                order_size = notional / max(float(price), 1e-9)
            return PolyOrder(
                token_id=token_by_leg[leg],
                side="BUY",
                price=Decimal(str(price)),
                size=Decimal(str(round(order_size, 4))),
                order_type="FOK",
                post_only=False,
                # Equal-share arb plans use share-sized limit FOKs. Fixed-dollar
                # plans intentionally have unequal/non-integer leg sizes; CLOB
                # marketable BUY limit orders floor BUY size to integer shares,
                # which can turn a $1.00 leg into e.g. $0.96 and get rejected.
                # For those fixed-dollar plans, use the SDK market-buy FOK path so
                # the maker amount is the configured cents-denominated notional.
                use_limit_order=use_limit_order,
                tick_size=tick_size,
                neg_risk=neg_risk,
                builder_code=str(builder_code) if builder_code else None,
            )

        if _safe_bool(cfg.get("log_hotpath_submit", False), False):
            logger.info(
                "ARB HOTPATH batch submit size={} YES@{} NO@{} edge={} cost=${}",
                plan.size, plan.yes_limit, plan.no_limit, plan.edge_per_pair, plan.total_cost_est,
            )
        test_mode_plan = self._test_mode_lower_leg_plan(plan, cfg)
        if test_mode_plan is not None:
            test_plan, lower = test_mode_plan
            order = make_buy(lower, px_by_leg[lower], test_plan.size)
            logger.warning(
                "ARB TEST MODE submitting only lower leg {} size={} px={} notional=${:.4f}; paired arb was size={} cost=${:.4f}",
                lower, test_plan.size, px_by_leg[lower], px_by_leg[lower] * test_plan.size, plan.size, plan.total_cost_est,
            )
            resp = self.exec_client.submit(order)
            responses[lower] = resp if isinstance(resp, dict) else {"raw": resp}
            responses[lower]["_attempt_size"] = test_plan.size
            responses[lower]["_attempt_price"] = px_by_leg[lower]
            responses[lower]["_test_mode_lower_leg"] = True
            if clob_response_indicates_fill(responses[lower]):
                filled_legs.append(lower)
                self.residual_inventory[lower] += test_plan.size
                return False, 0.0, filled_legs, responses, "TEST_LOWER_LEG_FILLED"
            return False, 0.0, filled_legs, responses, "TEST_LOWER_LEG_REJECTED"

        mode = str(cfg.get("arb_execution_mode") or cfg.get("execution_mode") or "sequential_budgeted").strip().lower()
        legacy_batch = mode in {"batch", "batch_fok", "simultaneous", "simultaneous_fok", "legacy_batch"}
        if not legacy_batch:
            # Polymarket has no atomic two-leg primitive. The safest live flow is:
            # 1) take the cheaper-notional leg at the planned depth-derived limit;
            # 2) only if it fills, immediately take the missing leg at the maximum
            #    price that still preserves configured min_edge after fees/friction.
            # This gives the second leg profitable slippage room without allowing a
            # completed YES+NO pair above the budget.
            first = plan.first_leg
            second: Leg = "NO" if first == "YES" else "YES"
            first_size = size_by_leg[first]
            first_order = make_buy(first, px_by_leg[first], first_size)
            try:
                first_resp = self.exec_client.submit(first_order)
            except Exception as exc:
                first_resp = {"success": False, "error": f"{type(exc).__name__}: {exc}"}
                logger.warning("first-leg submit did not fill for {}: {}", first, first_resp["error"])
            responses[first] = first_resp if isinstance(first_resp, dict) else {"raw": first_resp}
            responses[first]["_attempt_price"] = float(first_order.price)
            responses[first]["_attempt_size"] = float(first_order.size)

            # Only spend first-leg slippage after the original best-ask FOK has
            # already failed flat.  Crossing the first leg pre-emptively can use
            # up all edge and leave too little room for the hedge leg, creating
            # residual inventory.  A rejected first leg is safe to retry because
            # no exposure exists yet.
            if (not clob_response_indicates_fill(responses[first])
                    and unequal_fixed_sizes
                    and _safe_bool(cfg.get("profit_slippage_first_leg", True), True)):
                max_first_price = min(0.999, self._max_rescue_price(second, plan, cfg, filled_price=px_by_leg[second]))
                reserve = max(0.0, float(cfg.get("max_rescue_slippage", 0.0) or 0.0))
                retry_price = max(px_by_leg[first], max_first_price - reserve)
                if retry_price > float(first_order.price) + 1e-9:
                    retry_order = make_buy(first, retry_price, first_size)
                    try:
                        retry_resp = self.exec_client.submit(retry_order)
                    except Exception as exc:
                        retry_resp = {"success": False, "error": f"{type(exc).__name__}: {exc}"}
                        logger.warning("first-leg slippage retry did not fill for {}: {}", first, retry_resp["error"])
                    prior = dict(responses[first])
                    responses[first] = retry_resp if isinstance(retry_resp, dict) else {"raw": retry_resp}
                    responses[first]["_initial_attempt"] = prior
                    responses[first]["_attempt_price"] = float(retry_order.price)
                    responses[first]["_attempt_size"] = float(retry_order.size)
                    first_order = retry_order

            if not clob_response_indicates_fill(responses[first]):
                return False, 0.0, filled_legs, responses, "FLAT_NO_FILL"
            filled_legs.append(first)

            second_price = min(0.999, self._max_rescue_price(first, plan, cfg, filled_price=float(first_order.price)))
            second_size = size_by_leg[second]
            second_order = make_buy(second, second_price, second_size)
            try:
                second_resp = self.exec_client.submit(second_order)
            except Exception as exc:
                second_resp = {"success": False, "error": f"{type(exc).__name__}: {exc}"}
                logger.warning("budgeted second-leg submit failed for {} after {} filled: {}", second, first, second_resp["error"])
            responses[second] = second_resp if isinstance(second_resp, dict) else {"raw": second_resp}
            responses[second]["_attempt_price"] = float(second_order.price)
            responses[second]["_attempt_size"] = float(second_order.size)
            if clob_response_indicates_fill(responses[second]):
                filled_legs.append(second)
                pnl = self._plan_pnl(plan, cfg)
                logger.info("★ LIVE SNIPER ARB sequentially hedged missing {} size={} max_price={} pnl=${:.4f}", second, second_size, second_price, pnl)
                return True, pnl, filled_legs, responses, "SEQUENTIAL_HEDGED"

            self.residual_inventory[first] += first_size
            logger.warning("holding residual {} inventory size={} after budgeted second leg failed; no stop/alert", first, first_size)
            return False, 0.0, filled_legs, responses, "HOLD_RESIDUAL"

        batch_orders = [make_buy("YES", plan.yes_limit, plan.yes_size), make_buy("NO", plan.no_limit, plan.no_size)]
        raw_resps = self.exec_client.submit_batch(batch_orders) if hasattr(self.exec_client, "submit_batch") else [self.exec_client.submit(o) for o in batch_orders]
        if not isinstance(raw_resps, list):
            raw_resps = [raw_resps]
        for leg, resp in zip(("YES", "NO"), raw_resps):
            responses[leg] = resp if isinstance(resp, dict) else {"raw": resp}
            if clob_response_indicates_fill(responses[leg]):
                filled_legs.append(leg)

        if len(filled_legs) == 0:
            return False, 0.0, filled_legs, responses, "FLAT_NO_FILL"
        if len(filled_legs) == 2:
            pnl = self._plan_pnl(plan, cfg)
            logger.info("★ LIVE SNIPER ARB completed size={} pnl=${:.4f}", plan.size, pnl)
            return True, pnl, filled_legs, responses, "HEDGED"

        filled = filled_legs[0]
        missing: Leg = "NO" if filled == "YES" else "YES"
        rescue_price = self._max_rescue_price(filled, plan, cfg)
        book = self.books_by_slug.get(pair["slug"], self.books)[missing]
        can_rescue = bool(book.ask and book.ask <= rescue_price + float(cfg.get("max_rescue_slippage", 0.0) or 0.0))
        if can_rescue:
            try:
                rescue_resp = self.exec_client.submit(make_buy(missing, min(0.999, rescue_price), size_by_leg[missing]))
            except Exception as exc:
                # A rescue submit can raise after the initial batch already returned
                # a real matched leg. Do not let the outer hot-path exception handler
                # discard that partial fill and record both legs as generic errors.
                # Preserve the filled leg in `filled_legs`, attach the rescue error
                # to only the missing leg, then fall through to residual accounting.
                rescue_resp = {"success": False, "error": f"{type(exc).__name__}: {exc}"}
                logger.warning("missing-leg rescue submit failed for {} after {} filled: {}", missing, filled, rescue_resp["error"])
            responses[missing] = rescue_resp if isinstance(rescue_resp, dict) else {"raw": rescue_resp}
            if clob_response_indicates_fill(responses[missing]):
                filled_legs.append(missing)
                pnl = self._plan_pnl(plan, cfg)
                logger.info("★ LIVE SNIPER ARB rescued missing {} size={} pnl=${:.4f}", missing, size_by_leg[missing], pnl)
                return True, pnl, filled_legs, responses, "RESCUED_HEDGED"

        self.residual_inventory[filled] += size_by_leg[filled]
        logger.warning("holding residual {} inventory size={} until resolution; no stop/alert", filled, size_by_leg[filled])
        return False, 0.0, filled_legs, responses, "HOLD_RESIDUAL"

    def _plan_pnl(self, plan: ArbPlan, cfg: dict[str, Any]) -> float:
        fee_rate = float(cfg.get("polymarket_taker_fee_rate", cfg.get("fee_rate", 0.0)) or 0.0)
        yes_avg = plan.yes_cost_est / max(plan.yes_size, 1e-9)
        no_avg = plan.no_cost_est / max(plan.no_size, 1e-9)
        friction = (
            2 * float(cfg.get("fee_per_share", 0.0) or 0.0)
            + polymarket_fee_per_share(yes_avg, fee_rate)
            + polymarket_fee_per_share(no_avg, fee_rate)
            + float(cfg.get("merge_gas_per_share", cfg.get("gas_per_share", 0.0)) or 0.0)
            + float(cfg.get("stale_quote_buffer", 0.0) or 0.0)
        )
        # Equal-share plans are fully offset; fixed-dollar plans may intentionally
        # have unequal shares, so report the matched YES+NO component only.
        matched_size = min(plan.yes_size, plan.no_size)
        return (1.0 - yes_avg - no_avg - friction) * matched_size

    def _max_rescue_price(self, filled: Leg, plan: ArbPlan, cfg: dict[str, Any], filled_price: float | None = None) -> float:
        fee_rate = float(cfg.get("polymarket_taker_fee_rate", cfg.get("fee_rate", 0.0)) or 0.0)
        filled_size = plan.yes_size if filled == "YES" else plan.no_size
        filled_cost = plan.yes_cost_est if filled == "YES" else plan.no_cost_est
        filled_avg = float(filled_price) if filled_price is not None else filled_cost / max(filled_size, 1e-9)
        filled_fee = float(cfg.get("fee_per_share", 0.0) or 0.0) + polymarket_fee_per_share(filled_avg, fee_rate)
        # Assume missing leg fee at the limit; solve conservatively by using current configured flat fee
        # plus dynamic fee evaluated at the remaining budget after required profit/friction.
        base = 1.0 - filled_avg - filled_fee - float(cfg.get("merge_gas_per_share", cfg.get("gas_per_share", 0.0)) or 0.0) - float(cfg.get("stale_quote_buffer", 0.0) or 0.0) - float(cfg.get("min_edge", 0.0) or 0.0)
        missing_fee = float(cfg.get("fee_per_share", 0.0) or 0.0) + polymarket_fee_per_share(max(0.001, min(0.999, base)), fee_rate)
        return max(0.001, min(0.999, base - missing_fee))

    async def _record_submit_exception(self, pair: dict[str, Any], plan: ArbPlan, cfg: dict[str, Any], exc: Exception) -> None:
        token_by_leg = {"YES": pair["yes_token"], "NO": pair["no_token"]}
        px_by_leg = {"YES": plan.yes_limit, "NO": plan.no_limit}
        size_by_leg = {"YES": plan.yes_size, "NO": plan.no_size}
        market_slug = str(pair.get("slug") or cfg.get("market_slug") or "")
        err = f"{type(exc).__name__}: {exc}"
        for leg in (plan.first_leg, plan.second_leg):
            await self.writer.record_order_attempt(
                self.strategy_id,
                market_slug,
                token_by_leg[leg],
                leg,
                "BUY",
                "FOK_LIMIT" if cfg.get("use_limit_fok", True) else "FOK_MARKET",
                px_by_leg[leg],
                size_by_leg[leg],
                round(px_by_leg[leg] * size_by_leg[leg], 4),
                "error",
                response={},
                error=err,
                signal={"edge_per_pair": plan.edge_per_pair, "avg_sum_est": plan.avg_sum_est, "pair_cost_est": plan.total_cost_est, "state": "SUBMIT_EXCEPTION"},
                config=cfg,
            )
        await self.writer.log_strategy_event(
            self.strategy_id,
            f"Order attempt failed during submit: state=SUBMIT_EXCEPTION market={market_slug} size={plan.size:.4f} YES@{plan.yes_limit:.3f} NO@{plan.no_limit:.3f} edge={plan.edge_per_pair:.4f} est_cost=${plan.total_cost_est:.2f} error={err[:300]}",
            level="ERROR",
        )

    async def _record_execution(self, pair: dict[str, Any], plan: ArbPlan, ok: bool, pnl: float, filled_legs: list[Leg], responses: dict[Leg, dict[str, Any]], cfg: dict[str, Any], state: str = "") -> None:
        token_by_leg = {"YES": pair["yes_token"], "NO": pair["no_token"]}
        px_by_leg = {"YES": plan.yes_limit, "NO": plan.no_limit}
        size_by_leg = {"YES": plan.yes_size, "NO": plan.no_size}
        title = str(pair.get("title") or pair.get("slug") or "")[:40]
        market_slug = str(pair.get("slug") or cfg.get("market_slug") or "")
        order_log_parts = []
        attempted_legs = (plan.first_leg, plan.second_leg)
        if state.startswith("TEST_LOWER_LEG"):
            attempted_legs = tuple(leg for leg in ("YES", "NO") if leg in responses) or (plan.first_leg,)
        for leg in attempted_legs:
            resp = responses.get(leg, {})
            attempt_size = float(resp.get("_attempt_size", size_by_leg[leg])) if isinstance(resp, dict) else size_by_leg[leg]
            attempt_price = float(resp.get("_attempt_price", px_by_leg[leg])) if isinstance(resp, dict) else px_by_leg[leg]
            status = "filled" if leg in filled_legs else ("rejected" if resp else "not_submitted")
            await self.writer.record_order_attempt(
                self.strategy_id,
                market_slug,
                token_by_leg[leg],
                leg,
                "BUY",
                "FOK_LIMIT" if cfg.get("use_limit_fok", True) else "FOK_MARKET",
                attempt_price,
                attempt_size,
                round(attempt_price * attempt_size, 4),
                status,
                response=resp,
                signal={"edge_per_pair": plan.edge_per_pair, "avg_sum_est": plan.avg_sum_est, "pair_cost_est": plan.total_cost_est, "state": state},
                config=cfg,
            )
            response_hint = clob_response_error(resp) or resp.get("status") or resp.get("orderID") or resp.get("order_id") or "no_response"
            order_log_parts.append(f"{leg}={status}@{attempt_price:.3f}x{attempt_size:.4f} notional=${attempt_price * attempt_size:.4f} resp={str(response_hint)[:180]}")
            if leg in filled_legs:
                self.fill_seq += 1
                await self.writer.record_fill(self.strategy_id, self.fill_seq, f"{title} [SNIPER] {leg}", "BUY", attempt_price, attempt_size, kind="ARB" if ok else "ARB_RESIDUAL")
        log_level = "INFO" if ok else ("WARNING" if state == "FLAT_NO_FILL" and not filled_legs else "ERROR")
        await self.writer.log_strategy_event(
            self.strategy_id,
            f"Opportunity spotted; {'completed' if ok else 'attempted'} sniper arb state={state}: market={market_slug} filled={','.join(filled_legs) or 'none'} residual={self.residual_inventory} size={plan.size:.4f} YES@{plan.yes_limit:.3f} NO@{plan.no_limit:.3f} edge={plan.edge_per_pair:.4f} est_cost=${plan.total_cost_est:.2f} pnl=${pnl:.4f} attempts=[{' | '.join(order_log_parts)}]",
            level=log_level,
        )

    async def run(self) -> None:
        max_initial_stagger_ms = self.file_cfg.get("discovery_initial_stagger_ms", self.file_cfg.get("gamma_initial_stagger_ms"))
        if max_initial_stagger_ms is None:
            max_initial_stagger_ms = 5000
        # Deterministically spread simultaneous systemd shard starts before both
        # CLOB auth warmup and Gamma metadata discovery. The config value is the
        # maximum jitter window, not a fixed delay for every shard.
        try:
            max_initial_stagger = max(0.0, float(max_initial_stagger_ms or 0))
        except Exception:
            max_initial_stagger = 5000.0
        if max_initial_stagger > 0:
            initial_stagger_ms = int(hashlib.sha256(self.strategy_id.encode()).hexdigest()[:8], 16) % int(max_initial_stagger + 1)
            logger.info("Startup discovery/auth stagger: {:.0f}ms of max {:.0f}ms", float(initial_stagger_ms), max_initial_stagger)
            await asyncio.sleep(float(initial_stagger_ms) / 1000.0)
        await self.setup()
        last_cfg_reload = 0.0
        cfg, status = await self.current_state()
        self.cached_status = status
        while not self.stop.is_set():
            try:
                await self.load_market(cfg)
                break
            except GammaDiscoveryError as e:
                await self.writer.log_strategy_event(self.strategy_id, f"Gamma discovery blocked/unavailable; retrying without stopping service: {e}", level="warning")
                logger.warning("initial Gamma discovery failed; keeping service alive and retrying: {}", e)
                await asyncio.sleep(30.0)
            except Exception as e:
                await self.writer.log_strategy_event(self.strategy_id, f"Initial market load failed; retrying without stopping service: {e}", level="warning")
                logger.warning("initial market load failed; keeping service alive and retrying: {}", e)
                await asyncio.sleep(15.0)
        if self.stop.is_set():
            return
        await self._flush_state(cfg)
        if _safe_bool(cfg.get("disable_gc", True), True):
            gc.disable()
            logger.info("Python cyclic GC disabled for arb sniper hot path")

        async def status_loop() -> None:
            nonlocal cfg, last_cfg_reload
            while not self.stop.is_set():
                try:
                    if time.time() - last_cfg_reload > 2:
                        next_cfg, next_status = await self.current_state()
                        self.cached_status = next_status
                        if next_status == "stop_requested":
                            await self.writer.set_strategy_status(self.strategy_id, "stopped")
                            self.cached_status = "stopped"
                        next_slug = next_cfg.get("market_slug") or next_cfg.get("market") or next_cfg.get("event_slug")
                        cur_slug = cfg.get("market_slug") or cfg.get("market") or cfg.get("event_slug")
                        cfg_changed = next_cfg != cfg
                        cfg = next_cfg
                        last_cfg_reload = time.time()
                        if next_status != self.last_logged_status:
                            await self.writer.log_strategy_event(self.strategy_id, f"Strategy status observed: {next_status}")
                            self.last_logged_status = next_status
                        if cfg_changed and next_status == "running" and self._is_weather_outlier_strategy(cfg):
                            await self.maybe_execute_hot_path(cfg)
                        auto_roll = is_auto_roll_slug(str(next_slug) if next_slug else None)
                        if next_slug and (next_slug != cur_slug or auto_roll):
                            previous_resolved = self.ev.get("event_slug") or self.ev.get("slug") if self.ev else None
                            if next_slug != cur_slug or previous_resolved is None:
                                await self.load_market(cfg)
                            elif auto_roll:
                                crypto_auto = _auto_crypto_updown(str(next_slug))
                                if crypto_auto:
                                    asset, timeframe = crypto_auto
                                    async with httpx.AsyncClient(timeout=5, http2=True) as c:
                                        picked = await pick_crypto_updown_event(c, asset, timeframe)
                                else:
                                    city = _auto_weather_city(str(next_slug))
                                    async with httpx.AsyncClient(timeout=15, http2=True) as c:
                                        picked = await pick_weather_high_temp_event(c, city) if city else None
                                if picked and picked != previous_resolved:
                                    await self.load_market(cfg)
                    await self._write_book_rows()
                    await self._flush_state(cfg)
                    if self._is_weather_outlier_strategy(cfg):
                        await self._refresh_weather_safety_filter(cfg)
                    await self.maybe_log_health(cfg)
                    gc_interval = float(cfg.get("gc_collect_interval_seconds", 60) or 0)
                    if not gc.isenabled() and gc_interval > 0 and time.time() - self.last_gc_collect_ts >= gc_interval:
                        self.last_gc_collect_ts = time.time()
                        gc.collect()
                except Exception as e:
                    logger.debug("status loop error: {}", e)
                await asyncio.sleep(1.0)

        async def rest_poll_loop() -> None:
            nonlocal cfg
            timeout = httpx.Timeout(2.0, connect=0.5, read=1.5, write=0.5, pool=0.5)
            limits = httpx.Limits(max_keepalive_connections=4, max_connections=8, keepalive_expiry=30.0)
            phased_for_key: tuple[str, float] | None = None
            async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
                while not self.stop.is_set():
                    try:
                        interval_ms = float(cfg.get("rest_book_poll_ms", cfg.get("book_poll_interval_ms", 0)) or 0)
                        if interval_ms <= 0:
                            await asyncio.sleep(0.25)
                            continue
                        phase_key = (self.strategy_id, interval_ms)
                        if phased_for_key != phase_key:
                            phase_ms = deterministic_rest_poll_phase_ms(
                                self.strategy_id,
                                interval_ms,
                                cfg.get("rest_book_poll_phase_ms", cfg.get("rest_book_initial_phase_ms")),
                            )
                            phased_for_key = phase_key
                            if phase_ms > 0:
                                logger.info("REST /books poll phase offset: {:.0f}ms for interval {:.0f}ms", phase_ms, interval_ms)
                                await asyncio.sleep(phase_ms / 1000.0)
                        pairs = self.market_pairs or ([] if self.ev is None else [self.ev])
                        asset_ids: list[str] = []
                        for pair in pairs:
                            asset_ids.extend([pair["yes_token"], pair["no_token"]])
                        if not asset_ids:
                            await asyncio.sleep(interval_ms / 1000.0)
                            continue
                        t0 = time.perf_counter()
                        snapshots = await rest_books_full(client, asset_ids)
                        elapsed_ms = (time.perf_counter() - t0) * 1000
                        self.rest_book_latency.add(elapsed_ms)
                        self.last_rest_requested_tokens = len(dict.fromkeys(str(t) for t in asset_ids))
                        self.last_rest_returned_tokens = len(snapshots)
                        missing_tokens = [str(t) for t in dict.fromkeys(asset_ids) if str(t) not in snapshots]
                        self.last_rest_missing_tokens = missing_tokens
                        missing_labels: list[str] = []
                        for token in missing_tokens[:10]:
                            found = self._book_for_token(token)
                            if found:
                                pair, leg, _book = found
                                missing_labels.append(f"{pair.get('slug')}:{leg}:{token}")
                            else:
                                missing_labels.append(token)
                        self.last_rest_missing_labels = missing_labels
                        applied = 0
                        for token, book in snapshots.items():
                            applied += 1 if self._apply_full_book(token, book) else 0
                        if applied:
                            self.rest_book_refresh_count += 1
                        # Evaluate the trading hot path on every successful REST snapshot,
                        # not only when the top-of-book changed. A config threshold change
                        # can make the already-cached book tradable, and unchanged snapshots
                        # still confirm the opportunity is live/fresh.
                        if snapshots and self.cached_status == "running":
                            await self.maybe_execute_hot_path(cfg)
                        if applied:
                            self._schedule_dashboard_book_write(cfg)
                        await asyncio.sleep(max(0.0, (interval_ms - elapsed_ms) / 1000.0))
                    except Exception as e:
                        logger.debug("REST book poll error: {}", e)
                        await asyncio.sleep(0.25)

        async def hot_weather_poll_loop() -> None:
            nonlocal cfg
            timeout = httpx.Timeout(2.0, connect=0.5, read=1.5, write=0.5, pool=0.5)
            limits = httpx.Limits(max_keepalive_connections=4, max_connections=8, keepalive_expiry=30.0)
            async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
                while not self.stop.is_set():
                    try:
                        if not self._is_weather_outlier_strategy(cfg):
                            await asyncio.sleep(1.0)
                            continue
                        interval_ms = float(cfg.get("weather_outlier_hot_poll_ms", cfg.get("hot_book_poll_ms", 500)) or 500)
                        interval_ms = max(100.0, interval_ms)
                        applied = await self._refresh_weather_outlier_hot_books(client, cfg)
                        if applied and self.cached_status == "running":
                            await self.maybe_execute_hot_path(cfg)
                            self._schedule_dashboard_book_write(cfg)
                        await asyncio.sleep(interval_ms / 1000.0)
                    except Exception as e:
                        logger.debug("weather hot book poll error: {}", e)
                        await asyncio.sleep(0.5)

        status_task = asyncio.create_task(status_loop())
        rest_poll_task = asyncio.create_task(rest_poll_loop())
        hot_weather_poll_task = asyncio.create_task(hot_weather_poll_loop())
        nws_heat_alert_task: asyncio.Task[None] | None = None
        if self._is_weather_outlier_strategy(cfg) and _safe_bool(cfg.get("weather_outlier_nws_heat_alert_guard_enabled", True), True):
            nws_heat_alert_task = asyncio.create_task(self.weather_nws_heat_alert_loop(cfg))
        crypto_ref_task: asyncio.Task[None] | None = None
        if _safe_bool(cfg.get("fair_model_enabled", False), False):
            crypto_ref_task = asyncio.create_task(self.crypto_reference_loop(cfg))
            await self.writer.log_strategy_event(self.strategy_id, f"Crypto fair model enabled: symbol={self._binance_symbol(cfg)} min_model_edge={float(cfg.get('fair_model_min_edge', 0.0) or 0.0):.4f}")
        await self.writer.log_strategy_event(self.strategy_id, f"Strategy status observed: {self.cached_status}")
        self.last_logged_status = self.cached_status

        while not self.stop.is_set():
            self.market_reload.clear()
            assert self.ev is not None
            if not market_websocket_enabled(cfg):
                await asyncio.sleep(1.0)
                continue
            asset_ids = []
            for pair in self.market_pairs or [self.ev]:
                asset_ids.extend([pair["yes_token"], pair["no_token"]])
            try:
                ws_ping_interval_raw = cfg.get("websocket_ping_interval", 10)
                ws_ping_interval = None if ws_ping_interval_raw in (None, "", 0, "0") else float(ws_ping_interval_raw)
                ws_ping_timeout_raw = cfg.get("websocket_ping_timeout", 5)
                ws_ping_timeout = None if ws_ping_timeout_raw in (None, "", 0, "0") else float(ws_ping_timeout_raw)
                async with websockets.connect(
                    WS_MARKET,
                    ping_interval=ws_ping_interval,
                    ping_timeout=ws_ping_timeout,
                    close_timeout=1,
                    max_queue=1,
                    compression=None,
                ) as ws:
                    await ws.send(json.dumps(self._subscription_payload(asset_ids, cfg)))
                    subscription_msg = (
                        "Weather outlier market websocket connected/subscribed"
                        if self._is_weather_outlier_strategy(cfg)
                        else "Arb sniper subscribed to market websocket"
                    )
                    if not self._ws_subscription_logged:
                        await self.writer.log_strategy_event(self.strategy_id, subscription_msg)
                        self._ws_subscription_logged = True
                    else:
                        logger.info("{} reconnected and resubscribed", self.strategy_id)
                    async for raw in ws:
                        if self.stop.is_set() or self.market_reload.is_set():
                            break
                        try:
                            msgs = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        for m in msgs:
                            if "bids" in m and "asks" in m:
                                tok = str(m.get("asset_id", ""))
                                bids = _parse_levels(m.get("bids", []))
                                asks = _parse_levels(m.get("asks", []))
                                if bids and asks:
                                    if self._apply_full_book(
                                        tok,
                                        Book(
                                            bid=max(x["price"] for x in bids),
                                            ask=min(x["price"] for x in asks),
                                            bids=bids,
                                            asks=asks,
                                            updated_ts=time.time(),
                                        ),
                                    ):
                                        self.ws_update_count += 1
                                continue
                            if m.get("event_type") == "best_bid_ask":
                                tok = str(m.get("asset_id", ""))
                                if self._update_top_of_book(tok, _safe_float(m.get("best_bid")), _safe_float(m.get("best_ask"))):
                                    self.ws_update_count += 1
                                continue
                            pcs = m.get("price_changes")
                            if isinstance(pcs, list):
                                for pc in pcs:
                                    tok = str(pc.get("asset_id", ""))
                                    if self._update_top_of_book(tok, _safe_float(pc.get("best_bid")), _safe_float(pc.get("best_ask"))):
                                        self.ws_update_count += 1
                        if self.cached_status == "running":
                            await self.maybe_execute_hot_path(cfg)
                        if self.ws_update_count:
                            self._schedule_dashboard_book_write(cfg)
            except Exception as e:
                if not self.stop.is_set():
                    logger.warning("arb sniper ws error: {} — reconnecting", e)
                    await asyncio.sleep(1.0)
        status_task.cancel()
        rest_poll_task.cancel()
        hot_weather_poll_task.cancel()
        if nws_heat_alert_task is not None:
            nws_heat_alert_task.cancel()
        if crypto_ref_task is not None:
            crypto_ref_task.cancel()
        if self.dashboard_book_write_task is not None and not self.dashboard_book_write_task.done():
            self.dashboard_book_write_task.cancel()
        await self.writer.close()


def install_fast_event_loop() -> bool:
    """Use uvloop when available; return True when installed."""
    try:
        import uvloop  # type: ignore
    except Exception:
        return False
    uvloop.install()
    return True


def main() -> None:
    load_dotenv()
    fast_loop = install_fast_event_loop()
    if fast_loop:
        logger.info("uvloop installed for arb sniper")
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, required=True)
    args = p.parse_args()
    runner = ArbSniperRunner(args.config)

    def _stop(*_: Any) -> None:
        runner.stop.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    asyncio.run(runner.run())


if __name__ == "__main__":
    main()
