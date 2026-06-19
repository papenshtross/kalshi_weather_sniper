from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from math import erf, sqrt
from typing import Iterable


class SideSignal(StrEnum):
    UP = 'UP'
    DOWN = 'DOWN'
    SKIP = 'SKIP'


@dataclass(frozen=True)
class PricePoint:
    ts: int
    price: float


@dataclass(frozen=True)
class CloseTrendConfig:
    entry_seconds_before_close: int = 10
    trend_windows: tuple[int, ...] = (1, 2, 5, 10, 15, 30)
    min_consensus: int = 2
    epsilon: float = 1e-6
    volatility_window_seconds: int = 30
    breakout_level: float = 0.5


@dataclass(frozen=True)
class TrendSignal:
    side: SideSignal
    score: int
    entry_ts: int
    entry_price: float | None
    breakout_probability: float = 0.0
    opposite_implied_probability: float = 0.0
    reason: str | None = None


@dataclass(frozen=True)
class MarketBacktestInput:
    market_id: str
    market_slug: str
    question: str
    start_ts: int
    end_ts: int
    winner: SideSignal
    up_points: list[PricePoint]
    down_points: list[PricePoint]


@dataclass(frozen=True)
class MarketBacktestResult:
    market_id: str
    market_slug: str
    executed: bool
    side: SideSignal
    score: int
    entry_ts: int
    entry_price: float | None
    payout: float
    pnl: float
    breakout_probability: float = 0.0
    opposite_implied_probability: float = 0.0
    reason: str | None = None


def price_at_or_before(points: Iterable[PricePoint], ts: int) -> float | None:
    latest: float | None = None
    for point in points:
        if point.ts <= ts:
            latest = point.price
        else:
            break
    return latest


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + erf(value / sqrt(2.0)))


def estimate_breakout_probability(
    points: list[PricePoint],
    entry_ts: int,
    entry_price: float,
    market_end_ts: int,
    config: CloseTrendConfig,
    breakout_level: float | None = None,
) -> float:
    horizon_seconds = max(0, market_end_ts - entry_ts)
    if horizon_seconds <= 0:
        return 0.0

    window_start = entry_ts - config.volatility_window_seconds
    recent = [point for point in points if window_start <= point.ts <= entry_ts]
    if len(recent) < 3:
        return 0.0

    deltas = [recent[idx].price - recent[idx - 1].price for idx in range(1, len(recent))]
    mean_delta = sum(deltas) / len(deltas)
    variance = sum((delta - mean_delta) ** 2 for delta in deltas) / max(1, len(deltas) - 1)
    sigma = sqrt(max(variance, 0.0))
    if sigma <= config.epsilon:
        return 0.0

    target_breakout_level = config.breakout_level if breakout_level is None else breakout_level
    distance = abs(entry_price - target_breakout_level)
    if distance <= config.epsilon:
        return 1.0

    scaled_sigma = sigma * sqrt(horizon_seconds)
    if scaled_sigma <= config.epsilon:
        return 0.0

    z_score = distance / scaled_sigma
    probability = 1.0 - _normal_cdf(z_score)
    return max(0.0, min(1.0, probability))


def compute_trend_signal(
    points: list[PricePoint],
    market_end_ts: int,
    config: CloseTrendConfig,
    breakout_level: float | None = None,
    opposite_implied_probability: float | None = None,
) -> TrendSignal:
    entry_ts = market_end_ts - config.entry_seconds_before_close
    entry_price = price_at_or_before(points, entry_ts)
    if entry_price is None:
        return TrendSignal(side=SideSignal.SKIP, score=0, entry_ts=entry_ts, entry_price=None, reason='missing_entry_price')

    score = 0
    for window in config.trend_windows:
        base_price = price_at_or_before(points, entry_ts - window)
        if base_price is None:
            return TrendSignal(side=SideSignal.SKIP, score=0, entry_ts=entry_ts, entry_price=entry_price, reason='insufficient_history')
        delta = entry_price - base_price
        if delta > config.epsilon:
            score += 1
        elif delta < -config.epsilon:
            score -= 1

    if score >= config.min_consensus:
        side = SideSignal.UP
    elif score <= -config.min_consensus:
        side = SideSignal.DOWN
    else:
        side = SideSignal.SKIP

    breakout_probability = estimate_breakout_probability(
        points,
        entry_ts,
        entry_price,
        market_end_ts,
        config,
        breakout_level=breakout_level,
    )
    chosen_opposite_implied_probability = (
        max(0.0, min(1.0, 1.0 - entry_price))
        if opposite_implied_probability is None
        else max(0.0, min(1.0, opposite_implied_probability))
    )

    if side != SideSignal.SKIP and breakout_probability >= chosen_opposite_implied_probability:
        return TrendSignal(
            side=SideSignal.SKIP,
            score=score,
            entry_ts=entry_ts,
            entry_price=entry_price,
            breakout_probability=breakout_probability,
            opposite_implied_probability=chosen_opposite_implied_probability,
            reason='breakout_risk_too_high',
        )

    return TrendSignal(
        side=side,
        score=score,
        entry_ts=entry_ts,
        entry_price=entry_price,
        breakout_probability=breakout_probability,
        opposite_implied_probability=chosen_opposite_implied_probability,
        reason=None if side != SideSignal.SKIP else 'no_consensus',
    )


def backtest_market(market: MarketBacktestInput, config: CloseTrendConfig) -> MarketBacktestResult:
    signal = compute_trend_signal(market.up_points, market.end_ts, config)
    selected_points = market.down_points if signal.side == SideSignal.DOWN else market.up_points
    selected_entry_price = price_at_or_before(selected_points, signal.entry_ts)

    if signal.side == SideSignal.SKIP or selected_entry_price is None:
        return MarketBacktestResult(
            market_id=market.market_id,
            market_slug=market.market_slug,
            executed=False,
            side=signal.side,
            score=signal.score,
            entry_ts=signal.entry_ts,
            entry_price=selected_entry_price,
            payout=0.0,
            pnl=0.0,
            breakout_probability=signal.breakout_probability,
            opposite_implied_probability=signal.opposite_implied_probability,
            reason=signal.reason,
        )

    opposite_implied_probability = max(0.0, min(1.0, 1.0 - selected_entry_price))
    breakout_probability = estimate_breakout_probability(selected_points, signal.entry_ts, selected_entry_price, market.end_ts, config)
    if breakout_probability >= opposite_implied_probability:
        return MarketBacktestResult(
            market_id=market.market_id,
            market_slug=market.market_slug,
            executed=False,
            side=SideSignal.SKIP,
            score=signal.score,
            entry_ts=signal.entry_ts,
            entry_price=selected_entry_price,
            payout=0.0,
            pnl=0.0,
            breakout_probability=breakout_probability,
            opposite_implied_probability=opposite_implied_probability,
            reason='breakout_risk_too_high',
        )

    payout = 1.0 if signal.side == market.winner else 0.0
    pnl = payout - selected_entry_price
    return MarketBacktestResult(
        market_id=market.market_id,
        market_slug=market.market_slug,
        executed=True,
        side=signal.side,
        score=signal.score,
        entry_ts=signal.entry_ts,
        entry_price=selected_entry_price,
        payout=payout,
        pnl=pnl,
        breakout_probability=breakout_probability,
        opposite_implied_probability=opposite_implied_probability,
        reason=None,
    )


def summarize_results(results: list[MarketBacktestResult]) -> dict[str, float | int]:
    executed = [result for result in results if result.executed]
    wins = [result for result in executed if result.pnl > 0]
    total_pnl = round(sum(result.pnl for result in executed), 6)
    avg_pnl = round(total_pnl / len(executed), 6) if executed else 0.0
    return {
        'markets_total': len(results),
        'executed_trades': len(executed),
        'skipped_trades': len(results) - len(executed),
        'win_rate': (len(wins) / len(executed)) if executed else 0.0,
        'total_pnl': total_pnl,
        'avg_pnl': avg_pnl,
    }
