from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from math import erf, sqrt
from typing import Any, Iterable


class SideSignal(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    SKIP = "SKIP"


@dataclass(frozen=True)
class PricePoint:
    ts: int
    price: float


@dataclass(frozen=True)
class CandlePoint:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    taker_buy_volume: float


@dataclass(frozen=True)
class BinanceLabConfig:
    epsilon: float = 1e-9
    volatility_window_seconds: int = 60
    assumed_fee_per_share: float = 0.0
    contract_trade_staleness_seconds: int | None = 20


@dataclass(frozen=True)
class StrategySpec:
    name: str
    family: str
    windows: tuple[int, ...]
    entry_seconds_before_close: int
    min_consensus: int
    price_cap: float | None
    drift_threshold: float = 0.0
    score_threshold: float = 0.0
    taker_imbalance_threshold: float = 0.0
    vwap_deviation_threshold: float = 0.0
    breakout_threshold: float = 0.0
    efficiency_threshold: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["windows"] = list(self.windows)
        return payload


@dataclass(frozen=True)
class TrendSignal:
    side: SideSignal
    score: float
    entry_ts: int
    entry_price: float | None
    breakout_probability: float = 0.0
    opposite_implied_probability: float = 0.0
    reason: str | None = None
    diagnostics: dict[str, float] | None = None


@dataclass(frozen=True)
class MarketBacktestInput:
    market_id: str
    market_slug: str
    start_ts: int
    end_ts: int
    price_to_beat: float
    final_price: float
    binance_start_price: float
    binance_end_price: float
    candles: list[CandlePoint]
    up_points: list[PricePoint]
    down_points: list[PricePoint]


@dataclass(frozen=True)
class MarketBacktestResult:
    market_id: str
    market_slug: str
    executed: bool
    side: SideSignal
    score: float
    entry_ts: int
    entry_price: float | None
    payout: float
    pnl: float
    net_pnl: float
    breakout_probability: float = 0.0
    opposite_implied_probability: float = 0.0
    reference: str = "chainlink"
    reason: str | None = None
    diagnostics: dict[str, float] | None = None


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + erf(value / sqrt(2.0)))


def price_at_or_before(points: Iterable[PricePoint], ts: int) -> float | None:
    latest: float | None = None
    for point in points:
        if point.ts <= ts:
            latest = point.price
        else:
            break
    return latest



def price_point_at_or_before(points: Iterable[PricePoint], ts: int) -> PricePoint | None:
    latest: PricePoint | None = None
    for point in points:
        if point.ts <= ts:
            latest = point
        else:
            break
    return latest



def candle_at_or_before(candles: Iterable[CandlePoint], ts: int) -> CandlePoint | None:
    latest: CandlePoint | None = None
    for candle in candles:
        if candle.ts <= ts:
            latest = candle
        else:
            break
    return latest



def candles_between(candles: Iterable[CandlePoint], start_ts: int, end_ts: int) -> list[CandlePoint]:
    return [candle for candle in candles if start_ts <= candle.ts <= end_ts]



def estimate_breakout_probability(
    candles: list[CandlePoint],
    entry_ts: int,
    entry_price: float,
    market_end_ts: int,
    config: BinanceLabConfig,
    breakout_level: float,
) -> float:
    horizon_seconds = max(0, market_end_ts - entry_ts)
    if horizon_seconds <= 0:
        return 0.0

    recent = candles_between(candles, entry_ts - config.volatility_window_seconds, entry_ts)
    if len(recent) < 3:
        return 0.0
    deltas = [recent[index].close - recent[index - 1].close for index in range(1, len(recent))]
    mean_delta = sum(deltas) / len(deltas)
    variance = sum((delta - mean_delta) ** 2 for delta in deltas) / max(1, len(deltas) - 1)
    sigma = sqrt(max(variance, 0.0))
    if sigma <= config.epsilon:
        return 0.0

    distance = abs(entry_price - breakout_level)
    if distance <= config.epsilon:
        return 1.0

    scaled_sigma = sigma * sqrt(horizon_seconds)
    if scaled_sigma <= config.epsilon:
        return 0.0
    z_score = distance / scaled_sigma
    probability = 1.0 - _normal_cdf(z_score)
    return max(0.0, min(1.0, probability))



def realized_volatility(candles: list[CandlePoint]) -> float:
    if len(candles) < 2:
        return 0.0
    deltas = [candles[index].close - candles[index - 1].close for index in range(1, len(candles))]
    squared = sum(delta * delta for delta in deltas)
    return sqrt(max(squared, 0.0))



def vwap(candles: list[CandlePoint]) -> float | None:
    total_volume = sum(candle.volume for candle in candles)
    if total_volume <= 0:
        return None
    return sum(candle.close * candle.volume for candle in candles) / total_volume



def taker_imbalance(candles: list[CandlePoint]) -> float:
    buy_volume = sum(candle.taker_buy_volume for candle in candles)
    total_volume = sum(candle.volume for candle in candles)
    sell_volume = max(0.0, total_volume - buy_volume)
    denom = buy_volume + sell_volume
    if denom <= 0:
        return 0.0
    return (buy_volume - sell_volume) / denom



def efficiency_ratio(candles: list[CandlePoint], epsilon: float) -> float:
    if len(candles) < 2:
        return 0.0
    directional = abs(candles[-1].close - candles[0].close)
    path = sum(abs(candles[index].close - candles[index - 1].close) for index in range(1, len(candles)))
    if path <= epsilon:
        return 0.0
    return directional / path



def ema(candles: list[CandlePoint], span: int) -> float | None:
    if not candles:
        return None
    alpha = 2.0 / (span + 1.0)
    value = candles[0].close
    for candle in candles[1:]:
        value = alpha * candle.close + (1.0 - alpha) * value
    return value



def regression_slope(candles: list[CandlePoint], epsilon: float) -> float:
    if len(candles) < 2:
        return 0.0
    xs = [float(index) for index in range(len(candles))]
    ys = [candle.close for candle in candles]
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=False))
    denominator = sum((x - x_mean) ** 2 for x in xs)
    if denominator <= epsilon:
        return 0.0
    return numerator / denominator



def close_return(candles: list[CandlePoint], end_ts: int, window: int, epsilon: float) -> float | None:
    end_candle = candle_at_or_before(candles, end_ts)
    start_candle = candle_at_or_before(candles, end_ts - window)
    if end_candle is None or start_candle is None:
        return None
    if abs(start_candle.close) <= epsilon:
        return None
    return (end_candle.close - start_candle.close) / start_candle.close



def _select_entry_price(
    side: SideSignal,
    up_points: list[PricePoint],
    down_points: list[PricePoint],
    entry_ts: int,
    config: BinanceLabConfig,
) -> tuple[float | None, str | None]:
    selected = up_points if side == SideSignal.UP else down_points
    point = price_point_at_or_before(selected, entry_ts)
    if point is None:
        return None, "missing_contract_entry"
    if config.contract_trade_staleness_seconds is not None and entry_ts - point.ts > config.contract_trade_staleness_seconds:
        return None, "stale_contract_entry"
    return point.price, None



def _window_map(candles: list[CandlePoint], entry_ts: int, windows: tuple[int, ...]) -> dict[int, list[CandlePoint]]:
    return {window: candles_between(candles, entry_ts - window, entry_ts) for window in windows}



def _signal_from_score(score: float, threshold: float) -> SideSignal:
    if score >= threshold:
        return SideSignal.UP
    if score <= -threshold:
        return SideSignal.DOWN
    return SideSignal.SKIP



def _momentum_consensus(window_map: dict[int, list[CandlePoint]], strategy: StrategySpec, config: BinanceLabConfig) -> tuple[float, SideSignal, dict[str, float]]:
    votes = 0
    diagnostics: dict[str, float] = {}
    for window, candles in window_map.items():
        if len(candles) < 2:
            return 0.0, SideSignal.SKIP, {"reason_code": -1.0}
        start = candles[0].close
        end = candles[-1].close
        diagnostics[f"ret_{window}"] = (end - start) / start if abs(start) > config.epsilon else 0.0
        if end - start > config.epsilon:
            votes += 1
        elif start - end > config.epsilon:
            votes -= 1
    side = SideSignal.SKIP
    if votes >= strategy.min_consensus:
        side = SideSignal.UP
    elif votes <= -strategy.min_consensus:
        side = SideSignal.DOWN
    return float(votes), side, diagnostics



def _drift_vol_consensus(window_map: dict[int, list[CandlePoint]], strategy: StrategySpec, config: BinanceLabConfig) -> tuple[float, SideSignal, dict[str, float]]:
    votes = 0
    diagnostics: dict[str, float] = {}
    for window, candles in window_map.items():
        if len(candles) < 2:
            return 0.0, SideSignal.SKIP, {"reason_code": -1.0}
        start = candles[0].close
        end = candles[-1].close
        rv = realized_volatility(candles)
        drift = (end - start) / max(rv, config.epsilon)
        diagnostics[f"dv_{window}"] = drift
        if drift >= strategy.drift_threshold:
            votes += 1
        elif drift <= -strategy.drift_threshold:
            votes -= 1
    side = SideSignal.SKIP
    if votes >= strategy.min_consensus:
        side = SideSignal.UP
    elif votes <= -strategy.min_consensus:
        side = SideSignal.DOWN
    return float(votes), side, diagnostics



def _ema_stack(window_map: dict[int, list[CandlePoint]], strategy: StrategySpec, config: BinanceLabConfig) -> tuple[float, SideSignal, dict[str, float]]:
    spans = strategy.windows
    longest = max(spans)
    candles = window_map[longest]
    if len(candles) < longest:
        return 0.0, SideSignal.SKIP, {"reason_code": -1.0}
    ema_values = [ema(candles, span) for span in spans]
    if any(value is None for value in ema_values):
        return 0.0, SideSignal.SKIP, {"reason_code": -1.0}
    ema_values = [float(value) for value in ema_values if value is not None]
    diagnostics = {f"ema_{span}": value for span, value in zip(spans, ema_values, strict=False)}
    if ema_values == sorted(ema_values, reverse=True):
        slope = regression_slope(candles[-min(10, len(candles)):], config.epsilon)
        diagnostics["slope"] = slope
        if slope > 0:
            return float(len(spans)), SideSignal.UP, diagnostics
    if ema_values == sorted(ema_values):
        slope = regression_slope(candles[-min(10, len(candles)):], config.epsilon)
        diagnostics["slope"] = slope
        if slope < 0:
            return -float(len(spans)), SideSignal.DOWN, diagnostics
    return 0.0, SideSignal.SKIP, diagnostics



def _breakout_pressure(window_map: dict[int, list[CandlePoint]], strategy: StrategySpec, config: BinanceLabConfig) -> tuple[float, SideSignal, dict[str, float]]:
    window = max(strategy.windows)
    candles = window_map[window]
    if len(candles) < 4:
        return 0.0, SideSignal.SKIP, {"reason_code": -1.0}
    current = candles[-1].close
    prior = candles[:-1]
    prior_high = max(candle.high for candle in prior)
    prior_low = min(candle.low for candle in prior)
    rv = realized_volatility(candles)
    imbalance = taker_imbalance(candles[-min(10, len(candles)):])
    breakout_up = (current - prior_high) / max(rv, config.epsilon)
    breakout_down = (prior_low - current) / max(rv, config.epsilon)
    diagnostics = {
        "breakout_up": breakout_up,
        "breakout_down": breakout_down,
        "taker_imbalance": imbalance,
    }
    if breakout_up >= strategy.breakout_threshold and imbalance >= strategy.taker_imbalance_threshold:
        return breakout_up, SideSignal.UP, diagnostics
    if breakout_down >= strategy.breakout_threshold and imbalance <= -strategy.taker_imbalance_threshold:
        return -breakout_down, SideSignal.DOWN, diagnostics
    return 0.0, SideSignal.SKIP, diagnostics



def _vwap_taker(window_map: dict[int, list[CandlePoint]], strategy: StrategySpec, config: BinanceLabConfig) -> tuple[float, SideSignal, dict[str, float]]:
    window = max(strategy.windows)
    candles = window_map[window]
    if len(candles) < 2:
        return 0.0, SideSignal.SKIP, {"reason_code": -1.0}
    last_close = candles[-1].close
    current_vwap = vwap(candles)
    if current_vwap is None:
        return 0.0, SideSignal.SKIP, {"reason_code": -1.0}
    rv = realized_volatility(candles)
    deviation = (last_close - current_vwap) / max(rv, config.epsilon)
    imbalance = taker_imbalance(candles)
    diagnostics = {"vwap_dev": deviation, "taker_imbalance": imbalance}
    if deviation >= strategy.vwap_deviation_threshold and imbalance >= strategy.taker_imbalance_threshold:
        return deviation + imbalance, SideSignal.UP, diagnostics
    if deviation <= -strategy.vwap_deviation_threshold and imbalance <= -strategy.taker_imbalance_threshold:
        return deviation - imbalance, SideSignal.DOWN, diagnostics
    return 0.0, SideSignal.SKIP, diagnostics



def _hybrid_score(window_map: dict[int, list[CandlePoint]], strategy: StrategySpec, config: BinanceLabConfig) -> tuple[float, SideSignal, dict[str, float]]:
    longest = max(strategy.windows)
    candles = window_map[longest]
    if len(candles) < 4:
        return 0.0, SideSignal.SKIP, {"reason_code": -1.0}
    start = candles[0].close
    end = candles[-1].close
    rv = realized_volatility(candles)
    drift = (end - start) / max(rv, config.epsilon)
    vwap_value = vwap(candles) or end
    vwap_dev = (end - vwap_value) / max(rv, config.epsilon)
    imbalance = taker_imbalance(candles[-min(15, len(candles)):])
    eff = efficiency_ratio(candles, config.epsilon)
    slope = regression_slope(candles, config.epsilon) * len(candles) / max(rv, config.epsilon)
    score = (0.35 * drift) + (0.20 * vwap_dev) + (0.20 * imbalance) + (0.15 * eff) + (0.10 * slope)
    diagnostics = {
        "drift": drift,
        "vwap_dev": vwap_dev,
        "imbalance": imbalance,
        "efficiency": eff,
        "slope": slope,
    }
    side = _signal_from_score(score, strategy.score_threshold)
    return score, side, diagnostics


_SIGNAL_FNS = {
    "momentum_consensus": _momentum_consensus,
    "drift_vol_consensus": _drift_vol_consensus,
    "ema_stack": _ema_stack,
    "breakout_pressure": _breakout_pressure,
    "vwap_taker": _vwap_taker,
    "hybrid_score": _hybrid_score,
}



def evaluate_strategy_signal(
    candles: list[CandlePoint],
    up_points: list[PricePoint],
    down_points: list[PricePoint],
    market_end_ts: int,
    price_to_beat: float,
    config: BinanceLabConfig,
    strategy: StrategySpec,
) -> TrendSignal:
    entry_ts = market_end_ts - strategy.entry_seconds_before_close
    entry_candle = candle_at_or_before(candles, entry_ts)
    if entry_candle is None:
        return TrendSignal(side=SideSignal.SKIP, score=0.0, entry_ts=entry_ts, entry_price=None, reason="missing_binance_entry")
    window_map = _window_map(candles, entry_ts, strategy.windows)
    longest = max(strategy.windows)
    if len(window_map[longest]) < 2:
        return TrendSignal(side=SideSignal.SKIP, score=0.0, entry_ts=entry_ts, entry_price=None, reason="insufficient_history")

    score, side, diagnostics = _SIGNAL_FNS[strategy.family](window_map, strategy, config)
    if side == SideSignal.SKIP:
        reason = "insufficient_history" if diagnostics.get("reason_code") == -1.0 else "no_consensus"
        return TrendSignal(side=SideSignal.SKIP, score=score, entry_ts=entry_ts, entry_price=None, reason=reason, diagnostics=diagnostics)

    entry_price, price_reason = _select_entry_price(side, up_points, down_points, entry_ts, config)
    if entry_price is None:
        return TrendSignal(side=SideSignal.SKIP, score=score, entry_ts=entry_ts, entry_price=None, reason=price_reason, diagnostics=diagnostics)
    if strategy.price_cap is not None and entry_price > strategy.price_cap:
        return TrendSignal(side=SideSignal.SKIP, score=score, entry_ts=entry_ts, entry_price=entry_price, reason="price_above_cap", diagnostics=diagnostics)

    breakout_probability = estimate_breakout_probability(
        candles=candles,
        entry_ts=entry_ts,
        entry_price=entry_candle.close,
        market_end_ts=market_end_ts,
        config=config,
        breakout_level=price_to_beat,
    )
    opposite_implied_probability = max(0.0, min(1.0, 1.0 - entry_price))
    if breakout_probability >= opposite_implied_probability:
        return TrendSignal(
            side=SideSignal.SKIP,
            score=score,
            entry_ts=entry_ts,
            entry_price=entry_price,
            breakout_probability=breakout_probability,
            opposite_implied_probability=opposite_implied_probability,
            reason="breakout_risk_too_high",
            diagnostics=diagnostics,
        )
    return TrendSignal(
        side=side,
        score=score,
        entry_ts=entry_ts,
        entry_price=entry_price,
        breakout_probability=breakout_probability,
        opposite_implied_probability=opposite_implied_probability,
        diagnostics=diagnostics,
    )



def replay_live_decision(market: MarketBacktestInput, strategy: StrategySpec, config: BinanceLabConfig) -> TrendSignal:
    entry_ts = market.end_ts - strategy.entry_seconds_before_close
    candles = [candle for candle in market.candles if candle.ts <= entry_ts]
    up_points = [point for point in market.up_points if point.ts <= entry_ts]
    down_points = [point for point in market.down_points if point.ts <= entry_ts]
    return evaluate_strategy_signal(
        candles=candles,
        up_points=up_points,
        down_points=down_points,
        market_end_ts=market.end_ts,
        price_to_beat=market.price_to_beat,
        config=config,
        strategy=strategy,
    )



def _winner_for_reference(market: MarketBacktestInput, reference: str) -> SideSignal:
    if reference == "binance":
        return SideSignal.UP if market.binance_end_price >= market.binance_start_price else SideSignal.DOWN
    return SideSignal.UP if market.final_price >= market.price_to_beat else SideSignal.DOWN



def backtest_market(
    market: MarketBacktestInput,
    strategy: StrategySpec,
    config: BinanceLabConfig,
    reference: str = "chainlink",
) -> MarketBacktestResult:
    signal = evaluate_strategy_signal(
        candles=market.candles,
        up_points=market.up_points,
        down_points=market.down_points,
        market_end_ts=market.end_ts,
        price_to_beat=market.price_to_beat,
        config=config,
        strategy=strategy,
    )
    if signal.side == SideSignal.SKIP or signal.entry_price is None:
        return MarketBacktestResult(
            market_id=market.market_id,
            market_slug=market.market_slug,
            executed=False,
            side=signal.side,
            score=signal.score,
            entry_ts=signal.entry_ts,
            entry_price=signal.entry_price,
            payout=0.0,
            pnl=0.0,
            net_pnl=0.0,
            breakout_probability=signal.breakout_probability,
            opposite_implied_probability=signal.opposite_implied_probability,
            reference=reference,
            reason=signal.reason,
            diagnostics=signal.diagnostics,
        )
    winner = _winner_for_reference(market, reference)
    payout = 1.0 if signal.side == winner else 0.0
    pnl = payout - signal.entry_price
    net_pnl = pnl - config.assumed_fee_per_share
    return MarketBacktestResult(
        market_id=market.market_id,
        market_slug=market.market_slug,
        executed=True,
        side=signal.side,
        score=signal.score,
        entry_ts=signal.entry_ts,
        entry_price=signal.entry_price,
        payout=payout,
        pnl=pnl,
        net_pnl=net_pnl,
        breakout_probability=signal.breakout_probability,
        opposite_implied_probability=signal.opposite_implied_probability,
        reference=reference,
        diagnostics=signal.diagnostics,
    )



def summarize_results(results: list[MarketBacktestResult]) -> dict[str, Any]:
    executed = [result for result in results if result.executed]
    wins = [result for result in executed if result.pnl > 0]
    skip_reasons: dict[str, int] = {}
    for result in results:
        if result.executed or not result.reason:
            continue
        skip_reasons[result.reason] = skip_reasons.get(result.reason, 0) + 1
    total_pnl = round(sum(result.pnl for result in executed), 6)
    total_net_pnl = round(sum(result.net_pnl for result in executed), 6)
    avg_pnl = round(total_pnl / len(executed), 6) if executed else 0.0
    avg_net_pnl = round(total_net_pnl / len(executed), 6) if executed else 0.0
    return {
        "markets_total": len(results),
        "executed_trades": len(executed),
        "skipped_trades": len(results) - len(executed),
        "win_rate": (len(wins) / len(executed)) if executed else 0.0,
        "total_pnl": total_pnl,
        "total_net_pnl": total_net_pnl,
        "avg_pnl": avg_pnl,
        "avg_net_pnl": avg_net_pnl,
        "skip_reasons": skip_reasons,
    }



def _specs_for_family(family: str, entries: list[StrategySpec]) -> list[StrategySpec]:
    return entries



def build_strategy_universe() -> list[StrategySpec]:
    strategies: list[StrategySpec] = []

    # 8 momentum consensus
    for idx, (windows, entry_s, consensus, cap) in enumerate([
        ((5, 15, 30), 5, 2, 0.95),
        ((5, 15, 30, 60), 5, 3, 0.97),
        ((3, 8, 21, 55), 5, 3, 0.95),
        ((10, 20, 40, 80), 10, 3, 0.93),
        ((5, 15, 45, 90), 10, 3, 0.95),
        ((15, 30, 60, 120), 15, 3, 0.93),
        ((5, 10, 20, 40, 80), 5, 3, 0.97),
        ((8, 21, 34, 55), 5, 3, 0.95),
    ], start=1):
        strategies.append(StrategySpec(f"momentum_consensus_{idx:02d}", "momentum_consensus", windows, entry_s, consensus, cap))

    # 8 drift/vol
    for idx, (windows, entry_s, consensus, cap, drift) in enumerate([
        ((5, 15, 30), 5, 2, 0.95, 0.30),
        ((5, 15, 30, 60), 5, 3, 0.97, 0.45),
        ((10, 20, 40, 80), 10, 3, 0.95, 0.55),
        ((15, 30, 60, 120), 10, 3, 0.93, 0.65),
        ((8, 21, 55), 5, 2, 0.95, 0.35),
        ((5, 10, 20, 40, 80), 5, 3, 0.97, 0.50),
        ((12, 24, 48, 96), 10, 3, 0.95, 0.60),
        ((20, 40, 80, 160), 15, 3, 0.93, 0.75),
    ], start=1):
        strategies.append(StrategySpec(f"drift_vol_consensus_{idx:02d}", "drift_vol_consensus", windows, entry_s, consensus, cap, drift_threshold=drift))

    # 8 ema stack
    for idx, (windows, entry_s, cap) in enumerate([
        ((5, 15, 30), 5, 0.95),
        ((8, 21, 55), 5, 0.95),
        ((10, 20, 40), 10, 0.93),
        ((12, 26, 55), 10, 0.95),
        ((15, 30, 60), 10, 0.93),
        ((20, 40, 80), 15, 0.93),
        ((5, 15, 45), 5, 0.97),
        ((6, 18, 54), 5, 0.97),
    ], start=1):
        strategies.append(StrategySpec(f"ema_stack_{idx:02d}", "ema_stack", windows, entry_s, 2, cap))

    # 8 breakout pressure
    for idx, (window, entry_s, cap, breakout, imbalance) in enumerate([
        (30, 5, 0.95, 0.05, 0.02),
        (45, 5, 0.95, 0.05, 0.04),
        (60, 5, 0.97, 0.08, 0.03),
        (60, 10, 0.95, 0.10, 0.04),
        (90, 10, 0.93, 0.12, 0.05),
        (120, 15, 0.93, 0.15, 0.06),
        (75, 5, 0.97, 0.08, 0.06),
        (150, 15, 0.93, 0.18, 0.08),
    ], start=1):
        strategies.append(StrategySpec(f"breakout_pressure_{idx:02d}", "breakout_pressure", (window,), entry_s, 1, cap, breakout_threshold=breakout, taker_imbalance_threshold=imbalance))

    # 8 vwap + taker
    for idx, (window, entry_s, cap, dev, imbalance) in enumerate([
        (30, 5, 0.95, 0.20, 0.02),
        (45, 5, 0.95, 0.25, 0.03),
        (60, 5, 0.97, 0.25, 0.04),
        (60, 10, 0.95, 0.30, 0.05),
        (90, 10, 0.93, 0.35, 0.05),
        (120, 15, 0.93, 0.40, 0.06),
        (75, 5, 0.97, 0.30, 0.03),
        (150, 15, 0.93, 0.45, 0.07),
    ], start=1):
        strategies.append(StrategySpec(f"vwap_taker_{idx:02d}", "vwap_taker", (window,), entry_s, 1, cap, taker_imbalance_threshold=imbalance, vwap_deviation_threshold=dev))

    # 10 hybrid
    for idx, (windows, entry_s, cap, threshold) in enumerate([
        ((15, 30, 60), 5, 0.95, 0.20),
        ((15, 30, 60), 10, 0.95, 0.25),
        ((30, 60, 120), 10, 0.93, 0.25),
        ((20, 40, 80), 5, 0.97, 0.18),
        ((20, 40, 80), 10, 0.95, 0.22),
        ((10, 30, 90), 5, 0.95, 0.20),
        ((10, 30, 90), 10, 0.93, 0.24),
        ((8, 21, 55), 5, 0.97, 0.18),
        ((12, 24, 48, 96), 10, 0.95, 0.26),
        ((20, 60, 120), 15, 0.93, 0.30),
    ], start=1):
        strategies.append(StrategySpec(f"hybrid_score_{idx:02d}", "hybrid_score", windows, entry_s, 1, cap, score_threshold=threshold))

    if len(strategies) != 50:
        raise AssertionError(f"expected 50 strategies, got {len(strategies)}")
    return strategies
