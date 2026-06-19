from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from typing import Any
from urllib.parse import urlencode

import httpx
from loguru import logger

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.backtest.binance_strategy_lab import CandlePoint, SideSignal, TrendSignal
from polybot.live.momentum_5m import (
    MomentumLiveState,
    build_fixed_stake_order_from_asks,
    build_live_now_signal,
    dynamic_momentum_strategy_spec_from_config,
)
from polybot.persistence.writer import PolybotWriter

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
CHAINLINK_BTC_USD_FEED = "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"
CHAINLINK_LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"
CHAINLINK_DATA_STREAMS_URL = "https://data.chain.link/api/live-data-engine-stream-data"
CHAINLINK_BTC_USD_CEXPRICE_STREAM_FEED_ID = "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8"
STARTING_CASH = 10_000.0


def normalize_signal_source(cfg: dict[str, Any] | None) -> str:
    source = str((cfg or {}).get("signal_source") or "binance").strip().lower()
    return source if source in {"binance", "chainlink", "lightgbm", "lightgbm_probability", "lightgbm_probability_v2"} else "binance"


def signal_source_is_lightgbm(source: str | None) -> bool:
    return str(source or "").strip().lower() in {"lightgbm", "lightgbm_probability", "lightgbm_probability_v2"}


def signal_source_target_mode(source: str | None) -> str:
    return "polymarket_strike" if str(source or "").strip().lower() in {"lightgbm_probability", "lightgbm_probability_v2"} else "current_5m_return"


def lightgbm_predict_url(cfg: dict[str, Any] | None) -> str:
    cfg = cfg or {}
    if cfg.get("lightgbm_predict_url"):
        return str(cfg["lightgbm_predict_url"])
    source = normalize_signal_source(cfg)
    if source == "lightgbm_probability_v2":
        return str(os.environ.get("BTC5M_PROBABILITY_SIGNAL_V2_PREDICT_URL") or "http://127.0.0.1:8789/predict")
    if source == "lightgbm_probability":
        return str(os.environ.get("BTC5M_PROBABILITY_SIGNAL_PREDICT_URL") or "http://127.0.0.1:8788/predict")
    return str(os.environ.get("BTC5M_SIGNAL_PREDICT_URL") or "http://127.0.0.1:8787/predict")


def signal_history_lookback_seconds(cfg: dict[str, Any] | None) -> int:
    """Return how much price history live signal construction must fetch.

    LightGBM feature generation uses up to 300s rolling windows, so fetching only
    from the current market start leaves early-market features missing. Keep a
    small buffer over the max model window and allow config override for future
    wider models.
    """
    cfg = cfg or {}
    source = normalize_signal_source(cfg)
    if signal_source_is_lightgbm(source):
        default = 330
    else:
        default = 180
    raw = cfg.get("signal_history_lookback_seconds", cfg.get("lightgbm_model_lookback_seconds", default))
    try:
        return max(1, int(raw))
    except Exception:
        return default


def signal_candle_start_ts(cfg: dict[str, Any] | None, *, market_start_ts: int, now_ts: int) -> int:
    lookback = signal_history_lookback_seconds(cfg)
    if signal_source_is_lightgbm(normalize_signal_source(cfg)):
        return int(now_ts) - lookback
    return max(int(market_start_ts), int(now_ts) - lookback)


SIGNAL_FILTER_ALIASES = {
    "lightgbm_market_state": "lightgbm_market_state",
    "market_state_confirmation": "lightgbm_market_state",
    "market_state": "lightgbm_market_state",
}


def normalize_signal_filters(cfg: dict[str, Any] | None) -> set[str]:
    cfg = cfg or {}
    raw = cfg.get("signal_filters", cfg.get("enabled_signal_filters", []))
    if raw is None:
        items: list[Any] = []
    elif isinstance(raw, str):
        items = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        items = [raw]
    normalized: set[str] = set()
    for item in items:
        key = str(item or "").strip().lower()
        alias = SIGNAL_FILTER_ALIASES.get(key)
        if alias:
            normalized.add(alias)
    return normalized


def _close_at_or_before(candles: list[Any], ts: int) -> float | None:
    latest = None
    for candle in candles:
        if int(getattr(candle, "ts", 0)) <= ts:
            latest = float(getattr(candle, "close"))
        else:
            break
    return latest


def _candle_at_or_before(candles: list[Any], ts: int) -> Any | None:
    latest = None
    for candle in candles:
        if int(getattr(candle, "ts", 0)) <= ts:
            latest = candle
        else:
            break
    return latest


def _safe_log_return(cur: float | None, base: float | None) -> float | None:
    if cur is None or base is None or cur <= 0 or base <= 0:
        return None
    import math
    return math.log(cur / base)


def _realized_vol_from_values(values: list[float]) -> float | None:
    if len(values) < 3:
        return None
    rets = [_safe_log_return(values[i], values[i - 1]) for i in range(1, len(values))]
    clean = [float(r) for r in rets if r is not None]
    if len(clean) < 2:
        return None
    mean = sum(clean) / len(clean)
    return (sum((r - mean) ** 2 for r in clean) / (len(clean) - 1)) ** 0.5


def _consensus_score_from_candles(candles: list[Any], current_ts: int, windows: tuple[int, ...] = (5, 10, 20, 40, 80)) -> int | None:
    cur = _close_at_or_before(candles, current_ts)
    if cur is None:
        return None
    score = 0
    for window in windows:
        base = _close_at_or_before(candles, current_ts - window)
        if base is None:
            return None
        score += 1 if cur > base else -1 if cur < base else 0
    return score


def build_lightgbm_prediction_request(
    *,
    candles: list[Any],
    current_ts: int,
    window_start_ts: int,
    window_end_ts: int,
    price_to_beat: float,
    chainlink_price: float | None,
    chainlink_ts: int | None,
    up_ask: float,
    down_ask: float,
    target_mode: str = "current_5m_return",
) -> dict[str, Any]:
    """Build the btc5m-signal /predict payload from live Binance candles and Chainlink quality fields."""
    latest_binance_candle = _candle_at_or_before(candles, current_ts)
    binance_price = float(getattr(latest_binance_candle, "close")) if latest_binance_candle is not None else None
    binance_ts = int(getattr(latest_binance_candle, "ts")) if latest_binance_candle is not None else None
    features: dict[str, Any] = {
        "seconds_elapsed": int(current_ts - window_start_ts),
        "seconds_remaining": int(window_end_ts - current_ts),
        "price_to_beat": float(price_to_beat),
        "binance_price": binance_price,
        "binance_distance_to_beat_bps": 10000.0 * (float(binance_price) / float(price_to_beat) - 1.0) if binance_price and price_to_beat else None,
    }
    windows = (1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 300)
    for window in windows:
        base = _close_at_or_before(candles, current_ts - window)
        log_ret = _safe_log_return(binance_price, base)
        features[f"log_return_{window}s"] = log_ret
        vals = [float(c.close) for c in candles if current_ts - window <= int(c.ts) <= current_ts]
        if vals and binance_price:
            high = max(vals)
            low = min(vals)
            mean = sum(vals) / len(vals)
            rng = high - low
            realized_vol = _realized_vol_from_values(vals)
            features[f"rolling_high_distance_{window}s"] = float(binance_price) / high - 1.0
            features[f"rolling_low_distance_{window}s"] = float(binance_price) / low - 1.0
            features[f"vwap_distance_{window}s"] = float(binance_price) / mean - 1.0
            features[f"range_position_{window}s"] = (float(binance_price) - low) / rng if rng > 0 else 0.5
            features[f"range_bps_{window}s"] = 10000.0 * (high / low - 1.0) if low else None
            features[f"realized_vol_{window}s"] = realized_vol
            features[f"return_vol_ratio_{window}s"] = (log_ret / realized_vol) if log_ret is not None and realized_vol and abs(realized_vol) > 1e-12 else None
        else:
            features[f"rolling_high_distance_{window}s"] = None
            features[f"rolling_low_distance_{window}s"] = None
            features[f"vwap_distance_{window}s"] = None
            features[f"range_position_{window}s"] = None
            features[f"range_bps_{window}s"] = None
            features[f"realized_vol_{window}s"] = None
            features[f"return_vol_ratio_{window}s"] = None
        volume = _sum_candle_attr_since(candles, current_ts, window, "volume")
        taker_buy = _sum_candle_attr_since(candles, current_ts, window, "taker_buy_volume")
        features[f"volume_{window}s"] = volume
        features[f"taker_buy_volume_{window}s"] = taker_buy
        features[f"taker_buy_ratio_{window}s"] = (taker_buy / volume) if volume > 0 else None
        features[f"signed_taker_volume_{window}s"] = 2.0 * taker_buy - volume if volume > 0 else None
        if price_to_beat:
            signs = [1 if v >= float(price_to_beat) else -1 for v in vals]
            features[f"strike_crosses_{window}s"] = sum(1 for a, b in zip(signs, signs[1:]) if a != b) if len(signs) >= 2 else None
        else:
            features[f"strike_crosses_{window}s"] = None
    features["consensus_score_1_2_3_5_10"] = _consensus_score_from_candles(candles, current_ts, (1, 2, 3, 5, 10))
    features["consensus_score_5_10_20_40_80"] = _consensus_score_from_candles(candles, current_ts)
    features["consensus_score_15_30_60_120"] = _consensus_score_from_candles(candles, current_ts, (15, 30, 60, 120))
    returns = [features.get(f"log_return_{w}s") for w in windows]
    features["trend_votes_up_14w"] = sum(1 for r in returns if r is not None and r > 0)
    features["trend_votes_down_14w"] = sum(1 for r in returns if r is not None and r < 0)
    features["ret_accel_5_30"] = float(features.get("log_return_5s") or 0.0) - float(features.get("log_return_30s") or 0.0)
    features["ret_accel_10_60"] = float(features.get("log_return_10s") or 0.0) - float(features.get("log_return_60s") or 0.0)
    features["ret_accel_30_120"] = float(features.get("log_return_30s") or 0.0) - float(features.get("log_return_120s") or 0.0)
    rv15 = features.get("realized_vol_15s")
    rv30 = features.get("realized_vol_30s")
    rv60 = features.get("realized_vol_60s")
    rv120 = features.get("realized_vol_120s")
    rv300 = features.get("realized_vol_300s")
    features["vol_shock_15_120"] = (rv15 / rv120) if rv15 and rv120 and abs(float(rv120)) > 1e-12 else None
    features["vol_shock_30_300"] = (rv30 / rv300) if rv30 and rv300 and abs(float(rv300)) > 1e-12 else None
    dist_bps = features.get("binance_distance_to_beat_bps")
    features["abs_distance_to_beat_bps"] = abs(float(dist_bps)) if dist_bps is not None else None
    features["above_strike"] = 1 if binance_price is not None and price_to_beat and float(binance_price) >= float(price_to_beat) else 0
    features["distance_over_vol_60"] = (float(dist_bps) / (float(rv60) * 10000.0)) if dist_bps is not None and rv60 and abs(float(rv60)) > 1e-12 else None
    features["distance_over_vol_120"] = (float(dist_bps) / (float(rv120) * 10000.0)) if dist_bps is not None and rv120 and abs(float(rv120)) > 1e-12 else None
    return {
        "ts_ms": int(current_ts * 1000),
        "window_start_ms": int(window_start_ts * 1000),
        "window_end_ms": int(window_end_ts * 1000),
        "price_to_beat": float(price_to_beat),
        "chainlink_price": chainlink_price,
        "chainlink_ts_ms": int(chainlink_ts * 1000) if chainlink_ts is not None else None,
        "binance_spot_price": binance_price,
        "binance_spot_ts_ms": int(binance_ts * 1000) if binance_ts is not None else None,
        "target_mode": target_mode,
        "target_reference_price": float(price_to_beat) if target_mode == "polymarket_strike" else (float(binance_price) if binance_price is not None else (float(chainlink_price) if chainlink_price is not None else None)),
        "features": features,
        "polymarket": {"up_best_ask": float(up_ask), "down_best_ask": float(down_ask)},
    }


def lightgbm_response_to_signal(
    resp: dict[str, Any],
    *,
    current_ts: int,
    up_ask: float,
    down_ask: float,
    min_model_edge: float = 0.0,
    edge_mode: bool = False,
) -> TrendSignal:
    action = str(resp.get("action") or "NO_TRADE").upper()
    p_up = float(resp.get("p_up", 0.0) or 0.0)
    p_down = float(resp.get("p_down", 0.0) or 0.0)
    confidence = float(resp.get("confidence", max(p_up, p_down)) or 0.0)
    edge_up = resp.get("edge_up")
    edge_down = resp.get("edge_down")
    edge_up = p_up - float(up_ask) if edge_up is None else float(edge_up)
    edge_down = p_down - float(down_ask) if edge_down is None else float(edge_down)
    diagnostics = {
        "model": "lightgbm",
        "p_up": p_up,
        "p_down": p_down,
        "confidence": confidence,
        "edge_up": edge_up,
        "edge_down": edge_down,
        "min_model_edge": float(min_model_edge),
        "edge_mode": bool(edge_mode),
        "model_version": resp.get("model_version"),
        "target_mode": resp.get("target_mode"),
        "target_reference_price": resp.get("target_reference_price"),
        "reason_codes": resp.get("reason_codes") or [],
        "data_quality": resp.get("data_quality") or {},
    }
    data_quality = diagnostics["data_quality"] if isinstance(diagnostics["data_quality"], dict) else {}
    if action == "NO_TRADE":
        reason_codes = diagnostics.get("reason_codes") or []
        reason = "lightgbm_no_trade"
        if reason_codes:
            reason = "lightgbm_no_trade_" + str(reason_codes[0])
        return TrendSignal(side=SideSignal.SKIP, score=0.0, entry_ts=current_ts, entry_price=None, reason=reason, diagnostics=diagnostics)
    if data_quality and data_quality.get("ok") is False:
        return TrendSignal(side=SideSignal.SKIP, score=0.0, entry_ts=current_ts, entry_price=None, reason="lightgbm_data_quality_not_ok", diagnostics=diagnostics)
    if edge_mode:
        selected_side = "UP" if edge_up >= edge_down else "DOWN"
        selected_edge = edge_up if selected_side == "UP" else edge_down
        diagnostics["selected_edge"] = selected_edge
        if selected_edge < float(min_model_edge):
            return TrendSignal(side=SideSignal.SKIP, score=selected_edge, entry_ts=current_ts, entry_price=None, reason="lightgbm_edge_below_min", diagnostics=diagnostics)
        if selected_side == "UP":
            return TrendSignal(side=SideSignal.UP, score=selected_edge, entry_ts=current_ts, entry_price=up_ask, diagnostics=diagnostics)
        return TrendSignal(side=SideSignal.DOWN, score=-selected_edge, entry_ts=current_ts, entry_price=down_ask, diagnostics=diagnostics)
    if action == "UP":
        diagnostics["selected_edge"] = edge_up
        return TrendSignal(side=SideSignal.UP, score=confidence, entry_ts=current_ts, entry_price=up_ask, diagnostics=diagnostics)
    if action == "DOWN":
        diagnostics["selected_edge"] = edge_down
        return TrendSignal(side=SideSignal.DOWN, score=-confidence, entry_ts=current_ts, entry_price=down_ask, diagnostics=diagnostics)
    return TrendSignal(side=SideSignal.SKIP, score=0.0, entry_ts=current_ts, entry_price=None, reason="lightgbm_no_trade", diagnostics=diagnostics)


def _sum_candle_attr_since(candles: list[Any], current_ts: int, seconds: int, attr: str) -> float:
    return sum(float(getattr(c, attr, 0.0) or 0.0) for c in candles if current_ts - seconds < int(getattr(c, "ts", 0)) <= current_ts)


def _trend_vote_details(candles: list[Any], current_ts: int, windows: tuple[int, ...] = (5, 10, 20, 40, 80)) -> dict[str, Any]:
    cur = _close_at_or_before(candles, current_ts)
    votes: dict[str, str] = {}
    score = 0
    for window in windows:
        base = _close_at_or_before(candles, current_ts - window)
        if cur is None or base is None:
            votes[str(window)] = "WAIT"
            continue
        vote = "UP" if cur > base else "DOWN" if cur < base else "FLAT"
        votes[str(window)] = vote
        score += 1 if vote == "UP" else -1 if vote == "DOWN" else 0
    return {"score": score, "votes": votes, "current_price": cur}


def lightgbm_market_state_filter_signal(
    signal: TrendSignal,
    *,
    candles: list[Any],
    current_ts: int,
    window_start_ts: int,
    price_to_beat: float | None,
    cfg: dict[str, Any] | None = None,
) -> TrendSignal:
    """Optional live-market confirmation gate for LightGBM probability entries.

    This filter is intentionally conservative: the model still chooses the side,
    but the order is blocked unless live Binance trend, taker-flow, strike state,
    timing, and volume do not materially contradict that side.
    """
    cfg = cfg or {}
    if "lightgbm_market_state" not in normalize_signal_filters(cfg) or signal.side not in {SideSignal.UP, SideSignal.DOWN, "UP", "DOWN"}:
        return signal
    side = str(signal.side)
    diagnostics = dict(signal.diagnostics or {})
    trend = _trend_vote_details(candles, current_ts)
    trend_score = int(trend["score"])
    latest_price = trend.get("current_price")
    ret60 = _safe_log_return(latest_price, _close_at_or_before(candles, current_ts - 60))
    vol60 = _sum_candle_attr_since(candles, current_ts, 60, "volume")
    buy60 = _sum_candle_attr_since(candles, current_ts, 60, "taker_buy_volume")
    taker_buy_ratio = (buy60 / vol60) if vol60 > 0 else None
    selected_edge = float(diagnostics.get("selected_edge", abs(float(signal.score or 0.0))) or 0.0)
    min_model_edge = float(cfg.get("min_model_edge", diagnostics.get("min_model_edge", cfg.get("min_edge", 0.0))) or 0.0)
    seconds_elapsed = int(current_ts - window_start_ts)
    losing_vs_strike = False
    if latest_price is not None and price_to_beat:
        losing_vs_strike = latest_price < float(price_to_beat) if side == "UP" else latest_price > float(price_to_beat)

    failed: list[str] = []
    if side == "UP" and trend_score <= -1:
        failed.append("trend_majority_opposed")
    if side == "DOWN" and trend_score >= 1:
        failed.append("trend_majority_opposed")
    if ret60 is not None:
        if side == "UP" and ret60 < -float(cfg.get("lightgbm_filter_max_opposed_60s_return", 0.0) or 0.0):
            failed.append("trend_60s_opposed")
        if side == "DOWN" and ret60 > float(cfg.get("lightgbm_filter_max_opposed_60s_return", 0.0) or 0.0):
            failed.append("trend_60s_opposed")
    if taker_buy_ratio is not None:
        sell_dominated = float(cfg.get("lightgbm_filter_sell_dominated_ratio", 0.45) or 0.45)
        buy_dominated = float(cfg.get("lightgbm_filter_buy_dominated_ratio", 0.55) or 0.55)
        if side == "UP" and taker_buy_ratio < sell_dominated:
            failed.append("taker_flow_opposed")
        if side == "DOWN" and taker_buy_ratio > buy_dominated:
            failed.append("taker_flow_opposed")
    min_vol60 = float(cfg.get("lightgbm_filter_min_60s_volume", 2.0) or 0.0)
    if min_vol60 > 0 and vol60 < min_vol60:
        failed.append("low_60s_volume")
    late_after = int(cfg.get("lightgbm_filter_late_recovery_after_seconds", 90) or 90)
    strong_score = int(cfg.get("lightgbm_filter_strong_reversal_score", 4) or 4)
    strong_reversal = (trend_score >= strong_score and taker_buy_ratio is not None and taker_buy_ratio >= 0.55) if side == "UP" else (trend_score <= -strong_score and taker_buy_ratio is not None and taker_buy_ratio <= 0.45)
    if losing_vs_strike and seconds_elapsed >= late_after and not strong_reversal:
        failed.append("late_recovery_bet")
    extra_edge = float(cfg.get("lightgbm_filter_risky_extra_edge", 0.10) or 0.0)
    if (losing_vs_strike or any(r in failed for r in ("trend_60s_opposed", "taker_flow_opposed", "low_60s_volume"))) and selected_edge < min_model_edge + extra_edge:
        failed.append("conditional_edge_too_thin")

    filter_result = {
        "name": "lightgbm_market_state",
        "passed": not failed,
        "failed_reasons": sorted(set(failed)),
        "side": side,
        "latest_price": latest_price,
        "price_to_beat": price_to_beat,
        "losing_vs_strike": losing_vs_strike,
        "seconds_elapsed": seconds_elapsed,
        "trend_score_5_10_20_40_80": trend_score,
        "trend_votes": trend["votes"],
        "return_60s": ret60,
        "volume_60s": vol60,
        "taker_buy_ratio_60s": taker_buy_ratio,
        "selected_edge": selected_edge,
        "required_edge": min_model_edge + extra_edge if failed else min_model_edge,
    }
    diagnostics["filter_result"] = filter_result
    if failed:
        return TrendSignal(
            side=SideSignal.SKIP,
            score=signal.score,
            entry_ts=signal.entry_ts,
            entry_price=None,
            breakout_probability=signal.breakout_probability,
            opposite_implied_probability=signal.opposite_implied_probability,
            reason="filtered_lightgbm_market_state",
            diagnostics=diagnostics,
        )
    return TrendSignal(
        side=signal.side,
        score=signal.score,
        entry_ts=signal.entry_ts,
        entry_price=signal.entry_price,
        breakout_probability=signal.breakout_probability,
        opposite_implied_probability=signal.opposite_implied_probability,
        reason=signal.reason,
        diagnostics=diagnostics,
    )


async def fetch_lightgbm_signal(
    client: httpx.AsyncClient,
    cfg: dict[str, Any],
    *,
    candles: list[Any],
    current_ts: int,
    window_start_ts: int,
    window_end_ts: int,
    price_to_beat: float,
    chainlink_price: float | None,
    chainlink_ts: int | None,
    up_ask: float,
    down_ask: float,
) -> TrendSignal | None:
    req = build_lightgbm_prediction_request(
        candles=candles,
        current_ts=current_ts,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
        price_to_beat=price_to_beat,
        chainlink_price=chainlink_price,
        chainlink_ts=chainlink_ts,
        up_ask=up_ask,
        down_ask=down_ask,
        target_mode=signal_source_target_mode(normalize_signal_source(cfg)),
    )
    try:
        r = await client.post(lightgbm_predict_url(cfg), json=req, timeout=float(cfg.get("lightgbm_timeout_seconds", 1.5) or 1.5))
        r.raise_for_status()
        return lightgbm_response_to_signal(
            r.json(),
            current_ts=current_ts,
            up_ask=up_ask,
            down_ask=down_ask,
            min_model_edge=float(cfg.get("min_model_edge", cfg.get("min_edge", 0.0)) or 0.0),
            edge_mode=normalize_signal_source(cfg) in {"lightgbm_probability", "lightgbm_probability_v2"},
        )
    except Exception as e:
        logger.warning("LightGBM signal fetch error: {}", e)
        return None


def chainlink_rpc_url(cfg: dict[str, Any] | None) -> str:
    return str(
        (cfg or {}).get("chainlink_rpc_url")
        or os.environ.get("CHAINLINK_ETH_RPC_URL")
        or os.environ.get("ETH_RPC_URL")
        or "https://ethereum.publicnode.com"
    )


def chainlink_data_streams_url(cfg: dict[str, Any] | None) -> str:
    """Return Chainlink Data Streams UI API URL for BTC/USD CEX benchmark data.

    The public data.chain.link stream page uses this endpoint for the live chart.
    It is not the slow mainnet AggregatorV3 latestRoundData feed.
    """
    cfg = cfg or {}
    if cfg.get("chainlink_data_streams_url"):
        return str(cfg["chainlink_data_streams_url"])
    base_url = str(cfg.get("chainlink_data_streams_base_url") or os.environ.get("CHAINLINK_DATA_STREAMS_BASE_URL") or CHAINLINK_DATA_STREAMS_URL)
    params = {
        "feedId": str(cfg.get("chainlink_data_streams_feed_id") or os.environ.get("CHAINLINK_DATA_STREAMS_FEED_ID") or CHAINLINK_BTC_USD_CEXPRICE_STREAM_FEED_ID),
        "abiIndex": str(cfg.get("chainlink_data_streams_abi_index") or os.environ.get("CHAINLINK_DATA_STREAMS_ABI_INDEX") or "0"),
        "queryWindow": str(cfg.get("chainlink_data_streams_query_window") or os.environ.get("CHAINLINK_DATA_STREAMS_QUERY_WINDOW") or "1m"),
        "attributeName": str(cfg.get("chainlink_data_streams_attribute") or os.environ.get("CHAINLINK_DATA_STREAMS_ATTRIBUTE") or "benchmark"),
    }
    return f"{base_url}?{urlencode(params)}"


def _decode_uint256(word_hex: str) -> int:
    return int(word_hex, 16)


def _decode_int256(word_hex: str) -> int:
    value = int(word_hex, 16)
    if value >= 1 << 255:
        value -= 1 << 256
    return value


def decode_chainlink_latest_round_data(data_hex: str) -> CandlePoint:
    """Decode Chainlink AggregatorV3 latestRoundData() return data into a price point."""
    data = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if len(data) < 64 * 5:
        raise ValueError("invalid Chainlink latestRoundData payload")
    words = [data[i : i + 64] for i in range(0, 64 * 5, 64)]
    answer = _decode_int256(words[1])
    updated_at = _decode_uint256(words[3])
    if answer <= 0 or updated_at <= 0:
        raise ValueError("invalid Chainlink latestRoundData answer")
    price = answer / 1e8
    return CandlePoint(ts=int(updated_at), open=price, high=price, low=price, close=price, volume=0.0, taker_buy_volume=0.0)


def decode_chainlink_streams_latest_point(payload: dict[str, Any]) -> CandlePoint:
    nodes = (((payload.get("data") or {}).get("allStreamValuesGenerics") or {}).get("nodes") or [])
    if not nodes:
        raise ValueError("invalid Chainlink Data Streams payload: no nodes")
    best: dict[str, Any] | None = None
    best_dt: datetime | None = None
    for node in nodes:
        if str(node.get("attributeName") or "benchmark") != "benchmark":
            continue
        ts_raw = str(node.get("validAfterTs") or "")
        value_raw = str(node.get("valueNumeric") or "")
        if not ts_raw or not value_raw:
            continue
        dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        if best_dt is None or dt > best_dt:
            best = node
            best_dt = dt
    if best is None or best_dt is None:
        raise ValueError("invalid Chainlink Data Streams payload: no benchmark values")
    price = float(best["valueNumeric"])
    ts = int(best_dt.timestamp())
    return CandlePoint(ts=ts, open=price, high=price, low=price, close=price, volume=0.0, taker_buy_volume=0.0)


async def fetch_chainlink_aggregator_btc_usd_point(client: httpx.AsyncClient, cfg: dict[str, Any]) -> CandlePoint | None:
    try:
        r = await client.post(
            chainlink_rpc_url(cfg),
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": CHAINLINK_BTC_USD_FEED, "data": CHAINLINK_LATEST_ROUND_DATA_SELECTOR}, "latest"],
            },
            timeout=10,
        )
        r.raise_for_status()
        payload = r.json()
        if payload.get("error"):
            raise RuntimeError(payload["error"])
        return decode_chainlink_latest_round_data(str(payload.get("result") or ""))
    except Exception as e:
        logger.warning("Chainlink AggregatorV3 BTC/USD fetch error: {}", e)
        return None


async def fetch_chainlink_btc_usd_point(client: httpx.AsyncClient, cfg: dict[str, Any]) -> CandlePoint | None:
    try:
        r = await client.get(chainlink_data_streams_url(cfg), timeout=10)
        r.raise_for_status()
        return decode_chainlink_streams_latest_point(r.json())
    except Exception as e:
        logger.warning("Chainlink Data Streams BTC/USD fetch error: {}", e)
        if bool(cfg.get("chainlink_fallback_to_aggregator", False)):
            return await fetch_chainlink_aggregator_btc_usd_point(client, cfg)
        return None


def append_price_point(history: list[CandlePoint], point: CandlePoint, *, min_ts: int, max_points: int = 600) -> list[CandlePoint]:
    filtered = [c for c in history if c.ts >= min_ts]
    if filtered and filtered[-1].ts == point.ts:
        filtered[-1] = point
    elif not filtered or point.ts > filtered[-1].ts:
        filtered.append(point)
    return filtered[-max_points:]


def seconds_until_next_5m_start(now_ts: float | int | None = None) -> float:
    ts = time.time() if now_ts is None else float(now_ts)
    return float((300.0 - (ts % 300.0)) % 300.0)


def market_discovery_sleep_seconds(now_ts: float | int | None = None) -> float:
    """Poll aggressively around 5m rollovers so entry is not delayed by seconds."""
    ts = time.time() if now_ts is None else float(now_ts)
    seconds_into_window = ts % 300.0
    until_next = seconds_until_next_5m_start(ts)
    if seconds_into_window <= 1.0 or until_next <= 1.0:
        return 0.02
    if seconds_into_window <= 10.0 or until_next <= 3.0:
        return 0.25
    return min(2.0, max(0.25, until_next - 3.0))


def active_market_loop_sleep_seconds(now_ts: float | int | None = None, retry_sleep_seconds: float | None = None, cfg: dict[str, Any] | None = None) -> float:
    if retry_sleep_seconds is not None:
        return retry_sleep_seconds
    if cfg and cfg.get("active_loop_sleep_seconds") is not None:
        return max(0.02, float(cfg.get("active_loop_sleep_seconds") or 0.02))
    until_next = seconds_until_next_5m_start(now_ts)
    if until_next <= 1.0:
        return 0.02
    if until_next <= 3.0:
        return 0.10
    return 1.0


def signal_to_observation(signal: Any | None) -> dict[str, Any]:
    if signal is None:
        return {}
    return {
        "side": str(getattr(signal, "side", "")),
        "score": float(getattr(signal, "score", 0.0) or 0.0),
        "entry_ts": getattr(signal, "entry_ts", None),
        "entry_price": getattr(signal, "entry_price", None),
        "breakout_probability": float(getattr(signal, "breakout_probability", 0.0) or 0.0),
        "opposite_implied_probability": float(getattr(signal, "opposite_implied_probability", 0.0) or 0.0),
        "reason": getattr(signal, "reason", None),
        "diagnostics": getattr(signal, "diagnostics", None) or {},
    }


def _fmt_float(value: Any, digits: int = 3) -> str | None:
    try:
        if value is None:
            return None
        return f"{float(value):.{digits}f}"
    except Exception:
        return None


def signal_log_summary(signal: Any | None) -> str:
    """Compact, human-readable signal context for strategy execution logs."""
    if signal is None:
        return "signal=unknown"
    side = str(getattr(signal, "side", "") or "unknown")
    parts = [f"signal={side}"]
    score = _fmt_float(getattr(signal, "score", None))
    if score is not None:
        parts.append(f"score={score}")
    reason = getattr(signal, "reason", None)
    if reason:
        parts.append(f"reason={reason}")
    diagnostics = getattr(signal, "diagnostics", None) or {}
    for key in ("p_up", "p_down", "confidence"):
        value = _fmt_float(diagnostics.get(key))
        if value is not None:
            parts.append(f"{key}={value}")
    model_version = diagnostics.get("model_version") or diagnostics.get("model")
    if model_version:
        parts.append(f"model={model_version}")
    target_mode = diagnostics.get("target_mode")
    target_reference = _fmt_float(diagnostics.get("target_reference_price"), digits=2)
    if target_mode and target_reference is not None:
        parts.append(f"target={target_mode}@{target_reference}")
    elif target_mode:
        parts.append(f"target={target_mode}")
    data_quality = diagnostics.get("data_quality") or {}
    if isinstance(data_quality, dict) and data_quality:
        quality = "ok" if data_quality.get("ok") else "bad"
        warnings = data_quality.get("warnings") or []
        if warnings:
            quality = f"{quality}:" + ",".join(str(w) for w in warnings)
        parts.append(f"quality={quality}")
    reason_codes = diagnostics.get("reason_codes") or []
    if reason_codes:
        parts.append("reasons=" + ",".join(str(r) for r in reason_codes))
    return " ".join(parts)


def candles_to_observation(candles: list[Any]) -> dict[str, Any]:
    latest = candles[-1] if candles else None
    return {
        "latest_ts": getattr(latest, "ts", None) if latest else None,
        "latest_close": getattr(latest, "close", None) if latest else None,
        "candles": [
            {
                "ts": c.ts,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
                "taker_buy_volume": c.taker_buy_volume,
            }
            for c in candles
        ],
    }


async def persist_momentum_observation(
    writer: PolybotWriter,
    strategy_id: str,
    ev: dict[str, Any],
    up_bb: float,
    up_ba: float,
    up_bids: list[dict[str, float]],
    up_asks: list[dict[str, float]],
    down_bb: float,
    down_ba: float,
    down_bids: list[dict[str, float]],
    down_asks: list[dict[str, float]],
    candles: list[Any],
    signal: Any | None,
    config: dict[str, Any],
    state: MomentumLiveState,
) -> None:
    await writer.record_market_observation(
        strategy_id=strategy_id,
        market_slug=str(ev.get("slug") or ""),
        market_title=str(ev.get("title") or ""),
        market_start_ts=ev.get("start_ts"),
        market_end_ts=int(ev["end_dt"].timestamp()) if ev.get("end_dt") else None,
        price_to_beat=ev.get("price_to_beat"),
        final_price=ev.get("final_price"),
        up_token=str(ev.get("up_token") or ""),
        down_token=str(ev.get("down_token") or ""),
        up_bid=up_bb,
        up_ask=up_ba,
        down_bid=down_bb,
        down_ask=down_ba,
        up_bids=sorted(up_bids, key=lambda x: -x["price"])[:10],
        up_asks=sorted(up_asks, key=lambda x: x["price"])[:10],
        down_bids=sorted(down_bids, key=lambda x: -x["price"])[:10],
        down_asks=sorted(down_asks, key=lambda x: x["price"])[:10],
        binance=candles_to_observation(candles),
        signal=signal_to_observation(signal),
        config=config,
        state=state.state_dict(),
    )


async def pick_btc_updown_5m(client: httpx.AsyncClient) -> dict | None:
    """Pick the current BTC 5m market without waiting for Gamma active-list lag.

    The slug is deterministic (`btc-updown-5m-{start_ts}`), and Gamma can resolve
    current/upcoming slug lookups before the market appears in the active list.
    This keeps rollover entry close to T+0 instead of T+3-5s.
    """
    now_ts = int(time.time())
    current_start = now_ts - (now_ts % 300)
    candidates: list[int] = []
    if current_start + 300 - now_ts >= 15:
        candidates.append(current_start)
    candidates.append(current_start + 300)

    for start_ts in candidates:
        slug = f"btc-updown-5m-{start_ts}"
        ev = await resolve_5m_event(client, slug)
        if ev:
            return ev

    r = await client.get(
        GAMMA_EVENTS_URL,
        params={
            "closed": "false",
            "active": "true",
            "limit": 500,
            "tag_slug": "crypto",
            "order": "endDate",
            "ascending": "true",
        },
    )
    if r.status_code != 200:
        return None
    now = datetime.now(timezone.utc)
    best = None
    best_secs = None
    for e in r.json():
        slug = e.get("slug") or ""
        if "btc-updown-5m" not in slug:
            continue
        end = e.get("endDate", "")
        try:
            dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        except Exception:
            continue
        secs = (dt - now).total_seconds()
        if secs < 15:
            continue
        if best_secs is None or secs < best_secs:
            best = e
            best_secs = secs
    return best


async def resolve_5m_event(client: httpx.AsyncClient, slug: str) -> dict | None:
    r = await client.get(GAMMA_EVENTS_URL, params={"slug": slug})
    if r.status_code != 200:
        return None
    events = r.json()
    if not events:
        return None
    ev = events[0]
    markets = ev.get("markets") or []
    if not markets:
        return None
    m = markets[0]
    toks = m.get("clobTokenIds")
    outs = m.get("outcomes")
    if isinstance(toks, str):
        toks = json.loads(toks)
    if isinstance(outs, str):
        outs = json.loads(outs)
    if not toks or len(toks) < 2 or not outs or len(outs) < 2:
        return None
    end = ev.get("endDate", "")
    try:
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except Exception:
        return None
    start_ts = int(slug.rsplit("-", 1)[-1]) if slug.rsplit("-", 1)[-1].isdigit() else int(end_dt.timestamp()) - 300
    meta = ev.get("eventMetadata") or {}
    labels = [str(x) for x in outs]
    token_map = {labels[i].lower(): str(toks[i]) for i in range(min(2, len(labels)))}
    up_token = next((token_map[k] for k in token_map if k.startswith("up")), str(toks[0]))
    down_token = next((token_map[k] for k in token_map if k.startswith("down")), str(toks[1]))
    return {
        "title": ev.get("title") or m.get("question", slug),
        "slug": slug,
        "start_ts": start_ts,
        "end_dt": end_dt,
        "price_to_beat": float(meta.get("priceToBeat")) if meta.get("priceToBeat") is not None else None,
        "final_price": float(meta.get("finalPrice")) if meta.get("finalPrice") is not None else None,
        "up_token": up_token,
        "down_token": down_token,
        "up_label": next((labels[i] for i in range(len(labels)) if str(labels[i]).lower().startswith("up")), labels[0]),
        "down_label": next((labels[i] for i in range(len(labels)) if str(labels[i]).lower().startswith("down")), labels[1]),
        "tick_size": str(m.get("orderPriceMinTickSize") or m.get("minimumTickSize") or m.get("tickSize") or "0.01"),
        "order_min_size": float(m.get("orderMinSize") or 0),
        "neg_risk": bool(m.get("negRisk") or m.get("neg_risk") or False),
    }


async def rest_book_full(client: httpx.AsyncClient, token_id: str):
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


async def fetch_binance_recent_candles(client: httpx.AsyncClient, start_ts: int, end_ts: int):
    r = await client.get(
        BINANCE_KLINES_URL,
        params={
            "symbol": "BTCUSDT",
            "interval": "1s",
            "startTime": start_ts * 1000,
            "endTime": (end_ts + 1) * 1000,
            "limit": 1000,
        },
        timeout=15,
    )
    r.raise_for_status()
    from polybot.backtest.binance_strategy_lab import CandlePoint
    rows = []
    for k in r.json():
        rows.append(
            CandlePoint(
                ts=int(k[0]) // 1000,
                open=float(k[1]),
                high=float(k[2]),
                low=float(k[3]),
                close=float(k[4]),
                volume=float(k[5]),
                taker_buy_volume=float(k[9]),
            )
        )
    return rows


async def wait_for_resolution(client: httpx.AsyncClient, slug: str, timeout_seconds: int = 180) -> SideSignal | None:
    from polybot.backtest.binance_strategy_lab import SideSignal
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        ev = await resolve_5m_event(client, slug)
        if ev and ev.get("price_to_beat") is not None and ev.get("final_price") is not None:
            return SideSignal.UP if float(ev["final_price"]) >= float(ev["price_to_beat"]) else SideSignal.DOWN
        await asyncio.sleep(2)
    return None


def should_poll_market_when_status(status: str | None) -> bool:
    """Return True only when the dashboard has explicitly started the strategy.

    A stopped live momentum strategy must not select live markets, fetch books,
    write ticks/equity, or submit orders. The always-on supervisor may keep the
    task alive, but live market activity is gated by the UI-backed DB status.
    """
    return status == "running"


def momentum_resolution_wait_timeout(cfg: dict[str, Any]) -> int:
    """Seconds to wait for final resolution after a 5m market ends.

    Default is zero: immediately move to the next market window when the current
    one ends. This keeps the strategy continuously scanning fresh markets rather
    than sitting out up to 180s for Gamma finalPrice metadata.
    """
    try:
        return max(0, int(cfg.get("resolution_wait_seconds", 0) or 0))
    except Exception:
        return 0


def entry_max_attempts_per_market(cfg: dict[str, Any]) -> int:
    """Maximum live entry attempts for one market window.

    Defaults to one attempt unless FOK-kill retry is explicitly enabled; then the
    safe default is three bounded FOK attempts with no resting orders.
    """
    default = 3 if bool(cfg.get("entry_retry_on_fok_kill", False)) else 1
    try:
        raw = cfg.get("entry_max_attempts_per_market", default)
        return max(1, int(raw))
    except Exception:
        return default


def entry_retry_cooldown_seconds(cfg: dict[str, Any]) -> float:
    try:
        return max(0.0, float(cfg.get("entry_retry_cooldown_seconds", 0.75) or 0.0))
    except Exception:
        return 0.75


def is_fok_liquidity_kill_error(resp: dict[str, Any] | None) -> bool:
    if not resp or resp.get("success"):
        return False
    text = " ".join(str(resp.get(k) or "") for k in ("error", "message", "status"))
    text = text.lower()
    return (
        "fok" in text
        and "fully filled" in text
        and ("killed" in text or "kill" in text)
    )


def normalize_attempt_response(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return dict(parsed) if isinstance(parsed, dict) else {"message": raw}
        except Exception:
            return {"message": raw}
    if raw is None:
        return {}
    try:
        return dict(raw)
    except Exception:
        return {"message": str(raw)}


def should_retry_entry_attempt(
    resp: dict[str, Any] | None,
    *,
    attempts_so_far: int,
    cfg: dict[str, Any],
    consensus_still_valid: bool,
) -> bool:
    if not bool(cfg.get("entry_retry_on_fok_kill", False)):
        return False
    if not consensus_still_valid:
        return False
    if attempts_so_far >= entry_max_attempts_per_market(cfg):
        return False
    return is_fok_liquidity_kill_error(resp)


def hedge_profit_buffer(cfg: dict[str, Any]) -> float:
    """Return required profit buffer for completed entry+opposite hedge pair.

    Prefer explicit decimal `hedge_profit_buffer`; otherwise accept dashboard-style
    cent ticks via `hedge_buffer_ticks`. The buffer may be zero or negative;
    `hedge_consensus_trigger=0` is the hedging kill switch.
    """
    try:
        if cfg.get("hedge_profit_buffer") is not None:
            return max(-0.99, min(0.99, float(cfg.get("hedge_profit_buffer") or 0.0)))
        if cfg.get("hedge_buffer_ticks") is not None:
            return max(-0.99, min(0.99, int(cfg.get("hedge_buffer_ticks") or 0) / 100.0))
    except Exception:
        pass
    return 0.0


def hedge_consensus_trigger(cfg: dict[str, Any]) -> int:
    """Consensus weakening amount to subtract from ``min_consensus``.

    Example: with a 5/5 entry and trigger=2, hedge arms once original-side
    consensus weakens to 3/5 or lower. With trigger=10, the threshold is
    5 - 10 = -5, so an UP entry hedges only at score -5 and a DOWN entry only
    at score +5. The trigger is not clamped to the 5-vote consensus range.
    """
    try:
        return max(0, int(cfg.get("hedge_consensus_trigger", 0)))
    except Exception:
        return 0


def hedge_enabled(cfg: dict[str, Any]) -> bool:
    """Return whether post-entry hedging is enabled for this strategy config.

    Both knobs must intentionally enable hedging: a positive weakening trigger and
    a positive hedge profit buffer. Zero buffer is treated as no-hedge mode for
    base/live clones, not as permission to hedge at breakeven.
    """
    return hedge_consensus_trigger(cfg) > 0 and hedge_profit_buffer(cfg) > 0


def hedge_arm_score_threshold(cfg: dict[str, Any]) -> int:
    try:
        min_consensus = max(1, min(5, int(cfg.get("min_consensus", 5))))
    except Exception:
        min_consensus = 5
    return min_consensus - hedge_consensus_trigger(cfg)


def should_arm_hedge(*, entry_side: str, signal_score: float | int | None, cfg: dict[str, Any]) -> bool:
    if not hedge_enabled(cfg):
        return False
    if signal_score is None:
        return False
    threshold = hedge_arm_score_threshold(cfg)
    side = str(entry_side).upper()
    score = float(signal_score)
    if side == "UP":
        return score <= threshold
    if side == "DOWN":
        return score >= -threshold
    return False


def update_hedge_armed(
    *, currently_armed: bool, entry_side: str, signal_score: float | int | None, cfg: dict[str, Any]
) -> bool:
    """Return whether hedge monitoring should be active for the current signal.

    Hedge monitoring is tied to the current weakened-consensus condition, not a
    permanent latch. If consensus recovers past the threshold, stop checking the
    opposite leg until it weakens again.
    """
    _ = currently_armed
    return should_arm_hedge(entry_side=entry_side, signal_score=signal_score, cfg=cfg)


def opposite_outcome(side: str) -> str:
    return "DOWN" if str(side).upper() == "UP" else "UP"


def cfg_bool(cfg: dict[str, Any] | None, key: str, default: bool = False) -> bool:
    raw = (cfg or {}).get(key, default)
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    if isinstance(raw, (int, float)):
        return bool(raw)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def maybe_reverse_signal_side(
    signal: TrendSignal,
    cfg: dict[str, Any] | None,
    *,
    up_ask: float,
    down_ask: float,
) -> TrendSignal:
    """Optionally buy the opposite Polymarket side after the normal signal/filter gate.

    The LightGBM probability model and market-state filter still decide whether a
    trade is allowed. When ``reverse_signal_side`` is enabled, only allowed UP/DOWN
    signals are flipped immediately before order/hedge handling.
    """
    if not cfg_bool(cfg, "reverse_signal_side", False) or signal.side not in {SideSignal.UP, SideSignal.DOWN, "UP", "DOWN"}:
        return signal
    original_side = str(signal.side).upper()
    reversed_side = opposite_outcome(original_side)
    diagnostics = dict(signal.diagnostics or {})
    diagnostics["reversed"] = True
    diagnostics["original_side"] = original_side
    diagnostics["original_entry_price"] = signal.entry_price
    diagnostics["reversal_mode"] = "buy_opposite_side"
    score = abs(float(signal.score or 0.0))
    if reversed_side == "UP":
        side = SideSignal.UP
        signed_score = score
        entry_price = up_ask
    else:
        side = SideSignal.DOWN
        signed_score = -score
        entry_price = down_ask
    reason = signal.reason or "signal_reversed"
    if reason != "signal_reversed":
        reason = f"{reason}|signal_reversed"
    return TrendSignal(
        side=side,
        score=signed_score,
        entry_ts=signal.entry_ts,
        entry_price=entry_price,
        breakout_probability=signal.breakout_probability,
        opposite_implied_probability=signal.opposite_implied_probability,
        reason=reason,
        diagnostics=diagnostics,
    )


def polymarket_taker_fee_usdc(*, price: float, shares: float, fee_rate: float = 0.072) -> float:
    """Return Polymarket taker fee in USDC for a matched order.

    Polymarket fee formula: C * feeRate * p * (1-p), where C is shares.
    BTC 5m markets are crypto, so the default taker fee rate is 0.072.
    """
    try:
        p = max(0.0, min(1.0, float(price)))
        c = max(0.0, float(shares))
        r = max(0.0, float(fee_rate))
    except Exception:
        return 0.0
    return c * r * p * (1.0 - p)


def build_hedge_order_from_asks(
    asks: list[dict[str, float]],
    *,
    entry_price: float,
    entry_shares: float,
    hedge_buffer: float,
    min_notional: float = 1.0,
    fee_rate: float = 0.072,
) -> dict[str, float] | None:
    """Build a same-share opposite BUY hedge if visible depth is profitable.

    A completed binary hedge locks `shares * (1 - entry_price - hedge_price)`
    before fees. Entry taker fee is subtracted from the target price cap and from
    reported locked PnL so the configured hedge buffer is fee-aware.
    We only cross visible ask prices at or below target `1-entry-buffer-entry_fee`.
    If the same-share hedge is below Polymarket's minimum order notional, size up
    to the minimum order instead of skipping.
    """
    if not asks or entry_price <= 0 or entry_shares <= 0:
        return None
    entry_fee = polymarket_taker_fee_usdc(price=float(entry_price), shares=float(entry_shares), fee_rate=fee_rate)
    target = max(0.0, min(0.99, 1.0 - float(entry_price) - float(hedge_buffer) - entry_fee))
    if target <= 0:
        return None
    cumulative_size = 0.0
    chosen_price: float | None = None
    for level in sorted(asks, key=lambda level: float(level.get("price", 0.0))):
        price = float(level.get("price", 0.0) or 0.0)
        size = float(level.get("size", 0.0) or 0.0)
        if price <= 0 or size <= 0:
            continue
        if price > target + 1e-12:
            break
        cumulative_size += size
        chosen_price = price

    if chosen_price is None:
        return None

    same_share_stake = float(entry_shares) * chosen_price
    if same_share_stake + 1e-9 >= float(min_notional):
        if cumulative_size + 1e-9 < float(entry_shares):
            return None
        hedge_shares = float(entry_shares)
        sizing_mode = "same_shares"
    else:
        hedge_shares = float(
            (Decimal(str(min_notional)) / Decimal(str(chosen_price))).quantize(
                Decimal("0.0001"), rounding=ROUND_CEILING
            )
        )
        sizing_mode = "min_notional"

    stake = hedge_shares * chosen_price
    locked_pnl = min(float(entry_shares), hedge_shares) * (1.0 - float(entry_price) - chosen_price) - entry_fee
    return {
        "shares": hedge_shares,
        "limit_price": chosen_price,
        "stake_usd": stake,
        "target_price": target,
        "entry_fee_usdc": entry_fee,
        "fee_rate": float(fee_rate),
        "locked_pnl": locked_pnl,
        "sizing_mode": sizing_mode,
        "matched_entry_shares": min(float(entry_shares), hedge_shares),
    }


def should_switch_to_next_5m_event(now_ts: float, current_ev: dict[str, Any], next_event_hint: dict[str, Any] | None) -> bool:
    """Return True when the runner should leave the current window immediately.

    Prefer the pre-resolved next event exactly when its start timestamp arrives.
    If no pre-resolved hint exists, still leave once the current event end time is
    reached so discovery can poll for the next market without a resolution wait.
    """
    if next_event_hint is not None:
        try:
            if now_ts >= float(next_event_hint.get("start_ts", 0)):
                return True
        except Exception:
            pass
    end_dt = current_ev.get("end_dt")
    if end_dt is not None:
        try:
            return now_ts >= float(end_dt.timestamp())
        except Exception:
            return False
    return False


async def run_momentum_strategy(
    writer: PolybotWriter,
    strategy_id: str,
    name: str,
    cfg: dict[str, Any],
    stop: asyncio.Event,
) -> None:
    spec = dynamic_momentum_strategy_spec_from_config(cfg)
    exec_client = PolymarketExecutionClient()
    paused = asyncio.Event()
    fill_seq = int(time.time() * 1000) % 10_000_000
    executed_orders = 0

    async def status_poller():
        while not stop.is_set():
            try:
                st = await writer.get_strategy_status(strategy_id)
                if st in {"stop_requested", "stopped"}:
                    paused.set()
                    if st == "stop_requested":
                        await writer.set_strategy_status(strategy_id, "stopped")
                elif st == "running":
                    paused.clear()
            except Exception as e:
                logger.warning("[{}] momentum status poll error: {}", name, e)
            await asyncio.sleep(2)

    poller_task = asyncio.create_task(status_poller())
    logger.info("[{}] momentum strategy task started", name)
    next_event_hint: dict[str, Any] | None = None

    while not stop.is_set():
        try:
            db_cfg = await writer.get_strategy_config(strategy_id)
            effective_cfg = {**cfg, **(db_cfg or {})}
            spec = dynamic_momentum_strategy_spec_from_config(effective_cfg)
        except Exception:
            effective_cfg = dict(cfg)

        try:
            current_status = await writer.get_strategy_status(strategy_id)
            if not should_poll_market_when_status(current_status):
                paused.set()
                if current_status == "stop_requested":
                    await writer.set_strategy_status(strategy_id, "stopped")
                await asyncio.sleep(2)
                continue
            paused.clear()
        except Exception as e:
            logger.warning("[{}] initial momentum status check error: {}", name, e)
            await asyncio.sleep(2)
            continue

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                if next_event_hint and int(time.time()) < int(next_event_hint["end_dt"].timestamp()) - 15:
                    ev_raw = next_event_hint
                    next_event_hint = None
                else:
                    ev_raw = await pick_btc_updown_5m(client)
                if not ev_raw:
                    await asyncio.sleep(market_discovery_sleep_seconds())
                    continue
                ev = ev_raw if ev_raw.get("end_dt") else await resolve_5m_event(client, ev_raw["slug"])
                if not ev:
                    await asyncio.sleep(market_discovery_sleep_seconds())
                    continue
                while not stop.is_set() and time.time() < float(ev["start_ts"]):
                    await asyncio.sleep(min(0.02, max(0.0, float(ev["start_ts"]) - time.time())))

                state = MomentumLiveState(market=ev["title"][:80], market_slug=ev["slug"], end_dt=ev["end_dt"])
                chainlink_candles: list[CandlePoint] = []
                entry_attempt_count = 0
                hedge_attempt_count = 0
                attempted_entry = False
                hedge_armed = False
                hedge_armed_logged = False
                hedge_attempted = False
                hedge_waiting_for_price_logged = False
                hedge_min_notional_logged = False
                try:
                    entry_attempt_count = await writer.count_order_attempts(strategy_id, ev["slug"])
                    latest_attempt = await writer.latest_order_attempt(strategy_id, ev["slug"]) if entry_attempt_count else None
                except Exception as e:
                    logger.warning("[{}] could not load prior momentum order attempts for {}: {}", name, ev["slug"], e)
                    latest_attempt = None
                max_entry_attempts = entry_max_attempts_per_market(effective_cfg)
                if entry_attempt_count:
                    latest_resp = normalize_attempt_response(latest_attempt.get("response") if latest_attempt else None)
                    if latest_attempt and latest_attempt.get("error") and "error" not in latest_resp:
                        latest_resp["error"] = latest_attempt.get("error")
                    can_resume_retry = should_retry_entry_attempt(
                        latest_resp,
                        attempts_so_far=entry_attempt_count,
                        cfg=effective_cfg,
                        consensus_still_valid=True,
                    )
                    attempted_entry = not can_resume_retry
                    if can_resume_retry:
                        logger.info("[{}] momentum market {} has {} prior FOK-kill attempt(s); allowing bounded retry", name, ev["slug"], entry_attempt_count)
                        await writer.log_strategy_event(strategy_id, f"Market has {entry_attempt_count} prior FOK-kill attempt(s); bounded retry still allowed: {ev['title']}")
                    else:
                        logger.info("[{}] momentum market {} already has {} order attempt(s); suppressing re-entry", name, ev["slug"], entry_attempt_count)
                        await writer.log_strategy_event(strategy_id, f"Market already has {entry_attempt_count}/{max_entry_attempts} recorded order attempt(s) or non-retryable error; suppressing re-entry: {ev['title']}")
                await writer.register_strategy(
                    strategy_id=strategy_id,
                    name=name,
                    kind=str(effective_cfg.get("kind") or "momentum_consensus_dynamic_entry_5m"),
                    market=ev["title"][:80],
                    config=effective_cfg,
                )
                logger.info("[{}] momentum market selected: {}", name, ev["title"])
                await writer.log_strategy_event(strategy_id, f"Market selected: {ev['title']}")

                while not stop.is_set():
                    if paused.is_set() and not state.has_position():
                        await writer.set_strategy_status(strategy_id, "stopped")
                        break

                    try:
                        loop_db_cfg = await writer.get_strategy_config(strategy_id)
                        effective_cfg = {**cfg, **(loop_db_cfg or {})}
                        spec = dynamic_momentum_strategy_spec_from_config(effective_cfg)
                    except Exception as e:
                        logger.warning("[{}] momentum config refresh error: {}", name, e)

                    now_dt = datetime.now(timezone.utc)
                    now_ts = int(now_dt.timestamp())
                    seconds_to_end = (ev["end_dt"] - now_dt).total_seconds()
                    if next_event_hint is None and 0 < seconds_to_end <= 15.0:
                        next_start_ts = int(ev["start_ts"]) + 300
                        try:
                            next_event_hint = await resolve_5m_event(client, f"btc-updown-5m-{next_start_ts}")
                        except Exception as e:
                            logger.debug("[{}] next 5m pre-resolve failed: {}", name, e)
                    if should_switch_to_next_5m_event(time.time(), ev, next_event_hint):
                        if next_event_hint is not None:
                            await writer.log_strategy_event(strategy_id, "Next 5m market is live; switching immediately without waiting for current market close processing")
                        else:
                            await writer.log_strategy_event(strategy_id, "Market ended; moving to next market without waiting for resolution")
                        await writer.snapshot_equity(strategy_id, round(STARTING_CASH + state.cash, 2))
                        await writer.upsert_position(strategy_id, state.market, state.side, state.size, state.entry, state.last, state.unrealized_pnl())
                        break

                    up_book = await rest_book_full(client, ev["up_token"])
                    down_book = await rest_book_full(client, ev["down_token"])
                    if up_book is None or down_book is None:
                        await asyncio.sleep(1)
                        continue
                    up_bb, up_ba, up_bids, up_asks = up_book
                    down_bb, down_ba, down_bids, down_asks = down_book
                    await writer.upsert_book(strategy_id, ev["up_token"], ev["up_label"], sorted(up_bids, key=lambda x: -x["price"])[:10], sorted(up_asks, key=lambda x: x["price"])[:10], up_bb, up_ba)
                    await writer.upsert_book(strategy_id, ev["down_token"], ev["down_label"], sorted(down_bids, key=lambda x: -x["price"])[:10], sorted(down_asks, key=lambda x: x["price"])[:10], down_bb, down_ba)
                    await writer.record_tick(strategy_id, ev["up_token"], ev["up_label"], up_bb, up_ba)
                    await writer.record_tick(strategy_id, ev["down_token"], ev["down_label"], down_bb, down_ba)

                    signal = None
                    candles = []
                    retry_sleep_seconds: float | None = None
                    if state.has_position():
                        best_bid = up_bb if state.side == "UP" else down_bb
                        state.mark_price(best_bid)
                        signal_source = normalize_signal_source(effective_cfg)
                        candle_start = signal_candle_start_ts(effective_cfg, market_start_ts=int(ev["start_ts"]), now_ts=now_ts)
                        if signal_source == "chainlink":
                            chainlink_point = await fetch_chainlink_btc_usd_point(client, effective_cfg)
                            if chainlink_point is not None:
                                sampled = CandlePoint(ts=now_ts, open=chainlink_point.close, high=chainlink_point.close, low=chainlink_point.close, close=chainlink_point.close, volume=0.0, taker_buy_volume=0.0)
                                chainlink_candles = append_price_point(chainlink_candles, sampled, min_ts=candle_start)
                            candles = chainlink_candles
                        else:
                            candles = await fetch_binance_recent_candles(client, candle_start, now_ts)
                        if candles:
                            reference_price = float(ev["price_to_beat"]) if ev.get("price_to_beat") is not None else float(candles[0].open)
                            if signal_source_is_lightgbm(signal_source):
                                chainlink_point = await fetch_chainlink_btc_usd_point(client, effective_cfg)
                                signal = await fetch_lightgbm_signal(
                                    client,
                                    effective_cfg,
                                    candles=candles,
                                    current_ts=now_ts,
                                    window_start_ts=int(ev["start_ts"]),
                                    window_end_ts=int(ev["end_dt"].timestamp()) if ev.get("end_dt") else now_ts + 1,
                                    price_to_beat=reference_price,
                                    chainlink_price=chainlink_point.close if chainlink_point is not None else None,
                                    chainlink_ts=chainlink_point.ts if chainlink_point is not None else None,
                                    up_ask=up_ba,
                                    down_ask=down_ba,
                                ) or TrendSignal(side=SideSignal.SKIP, score=0.0, entry_ts=now_ts, entry_price=None, reason="lightgbm_unavailable")
                                signal = lightgbm_market_state_filter_signal(
                                    signal,
                                    candles=candles,
                                    current_ts=now_ts,
                                    window_start_ts=int(ev["start_ts"]),
                                    price_to_beat=reference_price,
                                    cfg=effective_cfg,
                                )
                                signal = maybe_reverse_signal_side(signal, effective_cfg, up_ask=up_ba, down_ask=down_ba)
                            else:
                                signal = build_live_now_signal(
                                    candles=candles,
                                    up_ask=up_ba,
                                    down_ask=down_ba,
                                    current_ts=now_ts,
                                    price_to_beat=reference_price,
                                    spec=spec,
                                )
                            new_hedge_armed = update_hedge_armed(
                                currently_armed=hedge_armed,
                                entry_side=state.side,
                                signal_score=getattr(signal, "score", None),
                                cfg=effective_cfg,
                            )
                            if new_hedge_armed and not hedge_armed:
                                hedge_armed = True
                                hedge_armed_logged = True
                                hedge_waiting_for_price_logged = False
                                hedge_min_notional_logged = False
                                await writer.log_strategy_event(
                                    strategy_id,
                                    f"Hedge armed: entry_side={state.side} score={signal.score:.0f} trigger={hedge_consensus_trigger(effective_cfg)} entry_px={state.entry:.3f} shares={state.size:.4f}",
                                    level="WARN",
                                )
                            elif not new_hedge_armed and hedge_armed:
                                hedge_armed = False
                                hedge_armed_logged = False
                                hedge_waiting_for_price_logged = False
                                hedge_min_notional_logged = False
                                await writer.log_strategy_event(
                                    strategy_id,
                                    f"Hedge disarmed: entry_side={state.side} score={signal.score:.0f} recovered beyond trigger={hedge_consensus_trigger(effective_cfg)}",
                                    level="INFO",
                                )
                            else:
                                hedge_armed = new_hedge_armed
                            if hedge_armed and not hedge_attempted and hedge_attempt_count < int(effective_cfg.get("hedge_max_attempts_per_market", 3) or 3):
                                hedge_side = opposite_outcome(state.side)
                                hedge_token = ev["down_token"] if hedge_side == "DOWN" else ev["up_token"]
                                hedge_asks = down_asks if hedge_side == "DOWN" else up_asks
                                buffer = hedge_profit_buffer(effective_cfg)
                                fee_rate = float(effective_cfg.get("polymarket_taker_fee_rate", 0.072) or 0.072)
                                hedge_info = build_hedge_order_from_asks(
                                    hedge_asks,
                                    entry_price=state.entry,
                                    entry_shares=state.size,
                                    hedge_buffer=buffer,
                                    min_notional=float(effective_cfg.get("min_order_size", 1.0) or 1.0),
                                    fee_rate=fee_rate,
                                )
                                entry_fee = polymarket_taker_fee_usdc(price=state.entry, shares=state.size, fee_rate=fee_rate)
                                target_px = max(0.0, min(0.99, 1.0 - state.entry - buffer - entry_fee))
                                if hedge_info is None:
                                    visible_best = min([float(a.get("price", 0.0) or 0.0) for a in hedge_asks], default=0.0)
                                    projected_notional = state.size * visible_best if visible_best > 0 and visible_best <= target_px else 0.0
                                    if projected_notional and projected_notional < float(effective_cfg.get("min_order_size", 1.0) or 1.0):
                                        if not hedge_min_notional_logged:
                                            hedge_min_notional_logged = True
                                            await writer.log_strategy_event(
                                                strategy_id,
                                                f"Hedge armed; profitable opposite {hedge_side} depth seen but waiting/submitting only as ${float(effective_cfg.get('min_order_size', 1.0) or 1.0):.2f} min-notional FOK at target<={target_px:.3f}",
                                                level="INFO",
                                            )
                                    elif not hedge_waiting_for_price_logged:
                                        hedge_waiting_for_price_logged = True
                                        await writer.log_strategy_event(
                                            strategy_id,
                                            f"Hedge armed; waiting for opposite {hedge_side} ask depth <= {target_px:.3f} with shares={state.size:.4f}",
                                            level="INFO",
                                        )
                                else:
                                    hedge_attempt_count += 1
                                    try:
                                        hedge_resp = exec_client.submit(
                                            PolyOrder(
                                                token_id=hedge_token,
                                                side="BUY",
                                                price=Decimal(str(hedge_info["limit_price"])),
                                                size=Decimal(str(hedge_info["shares"])),
                                                order_type="FOK",
                                                tick_size=str(effective_cfg.get("tick_size") or ev.get("tick_size") or "0.01"),
                                                neg_risk=bool(effective_cfg.get("neg_risk", ev.get("neg_risk", False))),
                                            )
                                        )
                                    except Exception as e:
                                        logger.exception("[{}] Momentum hedge submit error for market {}: {}", name, ev["slug"], e)
                                        hedge_resp = {"success": False, "error": str(e)}
                                    hedge_status = "filled" if hedge_resp.get("success") else "rejected"
                                    await writer.record_order_attempt(
                                        strategy_id=strategy_id,
                                        market_slug=ev["slug"],
                                        token=hedge_token,
                                        outcome=f"HEDGE_{hedge_side}",
                                        side="BUY",
                                        order_type="FOK_HEDGE",
                                        price=float(hedge_info["limit_price"]),
                                        size=float(hedge_info["shares"]),
                                        stake_usd=float(hedge_info["stake_usd"]),
                                        status=hedge_status,
                                        response=hedge_resp,
                                        error=None if hedge_resp.get("success") else str(hedge_resp.get("error") or hedge_resp),
                                        signal=signal_to_observation(signal),
                                        config=effective_cfg,
                                    )
                                    if hedge_resp.get("success"):
                                        hedge_attempted = True
                                        fill_seq += 1
                                        state.apply_hedge(hedge_side, hedge_token, hedge_info["limit_price"], hedge_info["stake_usd"], hedge_info["shares"], now_ts)
                                        await writer.record_fill(strategy_id, fill_seq, f"{state.market[:40]} [LIVE_MOMENTUM_HEDGE] {hedge_side}", "BUY", float(hedge_info["limit_price"]), float(hedge_info["shares"]), kind="LIVE_MOMENTUM_HEDGE")
                                        hedge_signal_summary = signal_log_summary(signal)
                                        await writer.log_strategy_event(strategy_id, f"Hedge filled: side={hedge_side} px={hedge_info['limit_price']:.3f} shares={hedge_info['shares']:.4f} stake=${hedge_info['stake_usd']:.2f} locked_pnl=${hedge_info['locked_pnl']:.4f} | trigger {hedge_signal_summary}", level="WARN")
                                        logger.warning("[{}] Momentum hedge filled: side={} px={} shares={} locked_pnl=${} trigger_signal={}", name, hedge_side, hedge_info["limit_price"], hedge_info["shares"], hedge_info["locked_pnl"], hedge_signal_summary)
                                    else:
                                        retryable = is_fok_liquidity_kill_error(hedge_resp) and hedge_attempt_count < int(effective_cfg.get("hedge_max_attempts_per_market", 3) or 3)
                                        hedge_attempted = not retryable
                                        await writer.log_strategy_event(
                                            strategy_id,
                                            f"Hedge order rejected ({hedge_attempt_count}/{int(effective_cfg.get('hedge_max_attempts_per_market', 3) or 3)}), retryable={retryable}: {hedge_resp}",
                                            level="ERROR" if not retryable else "WARN",
                                        )
                                        retry_sleep_seconds = entry_retry_cooldown_seconds(effective_cfg) if retryable else retry_sleep_seconds
                    elif not paused.is_set() and not attempted_entry:
                        signal_source = normalize_signal_source(effective_cfg)
                        candle_start = signal_candle_start_ts(effective_cfg, market_start_ts=int(ev["start_ts"]), now_ts=now_ts)
                        if signal_source == "chainlink":
                            chainlink_point = await fetch_chainlink_btc_usd_point(client, effective_cfg)
                            if chainlink_point is not None:
                                sampled = CandlePoint(ts=now_ts, open=chainlink_point.close, high=chainlink_point.close, low=chainlink_point.close, close=chainlink_point.close, volume=0.0, taker_buy_volume=0.0)
                                chainlink_candles = append_price_point(chainlink_candles, sampled, min_ts=candle_start)
                            candles = chainlink_candles
                        else:
                            candles = await fetch_binance_recent_candles(client, candle_start, now_ts)
                        if candles:
                            reference_price = float(ev["price_to_beat"]) if ev.get("price_to_beat") is not None else float(candles[0].open)
                            if signal_source_is_lightgbm(signal_source):
                                chainlink_point = await fetch_chainlink_btc_usd_point(client, effective_cfg)
                                signal = await fetch_lightgbm_signal(
                                    client,
                                    effective_cfg,
                                    candles=candles,
                                    current_ts=now_ts,
                                    window_start_ts=int(ev["start_ts"]),
                                    window_end_ts=int(ev["end_dt"].timestamp()) if ev.get("end_dt") else now_ts + 1,
                                    price_to_beat=reference_price,
                                    chainlink_price=chainlink_point.close if chainlink_point is not None else None,
                                    chainlink_ts=chainlink_point.ts if chainlink_point is not None else None,
                                    up_ask=up_ba,
                                    down_ask=down_ba,
                                ) or TrendSignal(side=SideSignal.SKIP, score=0.0, entry_ts=now_ts, entry_price=None, reason="lightgbm_unavailable")
                                signal = lightgbm_market_state_filter_signal(
                                    signal,
                                    candles=candles,
                                    current_ts=now_ts,
                                    window_start_ts=int(ev["start_ts"]),
                                    price_to_beat=reference_price,
                                    cfg=effective_cfg,
                                )
                                signal = maybe_reverse_signal_side(signal, effective_cfg, up_ask=up_ba, down_ask=down_ba)
                            else:
                                signal = build_live_now_signal(
                                    candles=candles,
                                    up_ask=up_ba,
                                    down_ask=down_ba,
                                    current_ts=now_ts,
                                    price_to_beat=reference_price,
                                    spec=spec,
                                )
                            if signal.side in {"UP", "DOWN"}:
                                stake_usd = float(effective_cfg.get("max_order_size", 1.0) or 1.0)
                                slippage_ticks = int(effective_cfg.get("entry_slippage_ticks", 1) or 0)
                                if signal.side == "UP":
                                    order_info = build_fixed_stake_order_from_asks(up_asks, stake_usd, slippage_ticks=slippage_ticks)
                                    token_id = ev["up_token"]
                                    outcome = "UP"
                                else:
                                    order_info = build_fixed_stake_order_from_asks(down_asks, stake_usd, slippage_ticks=slippage_ticks)
                                    token_id = ev["down_token"]
                                    outcome = "DOWN"
                                if order_info is not None:
                                    try:
                                        resp = exec_client.submit(
                                            PolyOrder(
                                                token_id=token_id,
                                                side="BUY",
                                                price=Decimal(str(order_info["limit_price"])),
                                                size=Decimal(str(order_info["shares"])),
                                                order_type="FOK",
                                                tick_size=str(effective_cfg.get("tick_size") or ev.get("tick_size") or "0.01"),
                                                neg_risk=bool(effective_cfg.get("neg_risk", ev.get("neg_risk", False))),
                                            )
                                        )
                                    except Exception as e:
                                        resp = {"success": False, "error": str(e)}
                                        if is_fok_liquidity_kill_error(resp):
                                            logger.warning("[{}] Momentum entry FOK liquidity kill for market {}: {}", name, ev["slug"], e)
                                        else:
                                            logger.exception("[{}] Momentum entry submit error for market {}: {}", name, ev["slug"], e)
                                    order_status = "filled" if resp.get("success") else "rejected"
                                    await writer.record_order_attempt(
                                        strategy_id=strategy_id,
                                        market_slug=ev["slug"],
                                        token=token_id,
                                        outcome=outcome,
                                        side="BUY",
                                        order_type="FOK",
                                        price=float(order_info["limit_price"]),
                                        size=float(order_info["shares"]),
                                        stake_usd=float(stake_usd),
                                        status=order_status,
                                        response=resp,
                                        error=None if resp.get("success") else str(resp.get("error") or resp),
                                        signal=signal_to_observation(signal),
                                        config=effective_cfg,
                                    )
                                    entry_attempt_count += 1
                                    if resp.get("success"):
                                        attempted_entry = True
                                        fill_seq += 1
                                        state.apply_fill(str(signal.side), token_id, order_info["limit_price"], stake_usd, order_info["shares"], now_ts)
                                        executed_orders += 1
                                        await writer.record_fill(strategy_id, fill_seq, f"{state.market[:40]} [LIVE_MOMENTUM] {signal.side}", "BUY", float(order_info["limit_price"]), float(order_info["shares"]), kind="LIVE_MOMENTUM")
                                        signal_summary = signal_log_summary(signal)
                                        await writer.log_strategy_event(strategy_id, f"Momentum entry filled: side={signal.side} px={order_info['limit_price']:.3f} shares={order_info['shares']:.4f} stake=${stake_usd:.2f} | {signal_summary}")
                                        logger.info("[{}] Momentum entry filled: side={} px={} shares={} stake=${} signal={}", name, signal.side, order_info['limit_price'], order_info['shares'], stake_usd, signal_summary)
                                    else:
                                        can_retry = should_retry_entry_attempt(
                                            resp,
                                            attempts_so_far=entry_attempt_count,
                                            cfg=effective_cfg,
                                            consensus_still_valid=signal.side in {"UP", "DOWN"},
                                        )
                                        attempted_entry = not can_retry
                                        if can_retry:
                                            retry_sleep_seconds = entry_retry_cooldown_seconds(effective_cfg)
                                            await writer.log_strategy_event(
                                                strategy_id,
                                                f"Momentum entry FOK liquidity kill; retrying after {retry_sleep_seconds:.2f}s if 5/5 consensus still exists ({entry_attempt_count}/{entry_max_attempts_per_market(effective_cfg)}): {resp}",
                                                level="WARN",
                                            )
                                            logger.warning("[{}] Momentum entry FOK-killed; retry allowed for market {} attempt {}/{}: {}", name, ev["slug"], entry_attempt_count, entry_max_attempts_per_market(effective_cfg), resp)
                                        else:
                                            await writer.log_strategy_event(strategy_id, f"Momentum entry rejected; no retry for market ({entry_attempt_count}/{entry_max_attempts_per_market(effective_cfg)}): {resp}", level="ERROR")
                                            logger.error("[{}] Momentum entry rejected; no retry for market {} attempt {}/{}: {}", name, ev["slug"], entry_attempt_count, entry_max_attempts_per_market(effective_cfg), resp)
                                else:
                                    await writer.log_strategy_event(strategy_id, f"Signal {signal.side} skipped: insufficient top-level depth for ${stake_usd:.2f} at current ask", level="WARN")

                    if not candles:
                        candle_start = signal_candle_start_ts(effective_cfg, market_start_ts=int(ev["start_ts"]), now_ts=now_ts)
                        try:
                            if normalize_signal_source(effective_cfg) == "chainlink":
                                chainlink_point = await fetch_chainlink_btc_usd_point(client, effective_cfg)
                                if chainlink_point is not None:
                                    sampled = CandlePoint(ts=now_ts, open=chainlink_point.close, high=chainlink_point.close, low=chainlink_point.close, close=chainlink_point.close, volume=0.0, taker_buy_volume=0.0)
                                    chainlink_candles = append_price_point(chainlink_candles, sampled, min_ts=candle_start)
                                candles = chainlink_candles
                            else:
                                candles = await fetch_binance_recent_candles(client, candle_start, now_ts)
                        except Exception as e:
                            logger.warning("[{}] {} observation fetch error: {}", name, normalize_signal_source(effective_cfg), e)
                            candles = []
                    await persist_momentum_observation(
                        writer, strategy_id, ev,
                        up_bb, up_ba, up_bids, up_asks,
                        down_bb, down_ba, down_bids, down_asks,
                        candles, signal, effective_cfg, state,
                    )

                    await writer.snapshot_equity(strategy_id, round(STARTING_CASH + state.cash + state.unrealized_pnl(), 2))
                    await writer.upsert_position(strategy_id, state.market, state.side, state.size, state.entry, state.last, state.unrealized_pnl())

                    max_orders = int(effective_cfg.get("max_executed_orders", 0) or 0)
                    if max_orders and executed_orders >= max_orders and not state.has_position():
                        await writer.log_strategy_event(strategy_id, f"Max executed orders reached ({executed_orders}), stopping strategy")
                        await writer.set_strategy_status(strategy_id, "stopped")
                        paused.set()
                        break

                    if paused.is_set() and not state.has_position():
                        await writer.set_strategy_status(strategy_id, "stopped")
                        break

                    await asyncio.sleep(active_market_loop_sleep_seconds(time.time(), retry_sleep_seconds, effective_cfg))
        except Exception as e:
            logger.exception("[{}] momentum strategy loop error: {}", name, e)
            await writer.log_strategy_event(strategy_id, f"Momentum strategy loop error: {e}", level="ERROR")
            await asyncio.sleep(5)
            continue

        # After a market closes, immediately scan for the next 5m market. A fixed
        # post-window sleep delays first-book/first-signal handling by seconds.
        if paused.is_set() and not stop.is_set():
            await asyncio.sleep(2)
            continue

    poller_task.cancel()
