from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from math import erf, sqrt
from typing import Iterable


class SideSignal(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    SKIP = "SKIP"


@dataclass(frozen=True)
class PricePoint:
    ts: int
    price: float


@dataclass(frozen=True)
class BinanceTrendConfig:
    entry_seconds_before_close: int = 10
    trend_windows: tuple[int, ...] = (5, 15, 30, 60)
    min_consensus: int = 3
    epsilon: float = 1e-9
    volatility_window_seconds: int = 60
    price_cap: float | None = 0.97
    assumed_fee_per_share: float = 0.0


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
    start_ts: int
    end_ts: int
    price_to_beat: float
    final_price: float
    binance_start_price: float
    binance_end_price: float
    binance_points: list[PricePoint]
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
    net_pnl: float
    breakout_probability: float = 0.0
    opposite_implied_probability: float = 0.0
    reference: str = "chainlink"
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
    config: BinanceTrendConfig,
    breakout_level: float,
) -> float:
    horizon_seconds = max(0, market_end_ts - entry_ts)
    if horizon_seconds <= 0:
        return 0.0

    window_start = entry_ts - config.volatility_window_seconds
    recent = [point for point in points if window_start <= point.ts <= entry_ts]
    if len(recent) < 3:
        return 0.0

    deltas = [recent[index].price - recent[index - 1].price for index in range(1, len(recent))]
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


def compute_multiframe_signal(
    binance_points: list[PricePoint],
    up_points: list[PricePoint],
    down_points: list[PricePoint],
    market_end_ts: int,
    price_to_beat: float,
    config: BinanceTrendConfig,
) -> TrendSignal:
    entry_ts = market_end_ts - config.entry_seconds_before_close
    binance_entry = price_at_or_before(binance_points, entry_ts)
    if binance_entry is None:
        return TrendSignal(side=SideSignal.SKIP, score=0, entry_ts=entry_ts, entry_price=None, reason="missing_binance_entry")

    score = 0
    for window in config.trend_windows:
        base_price = price_at_or_before(binance_points, entry_ts - window)
        if base_price is None:
            return TrendSignal(side=SideSignal.SKIP, score=0, entry_ts=entry_ts, entry_price=None, reason="insufficient_history")
        delta = binance_entry - base_price
        if delta > config.epsilon:
            score += 1
        elif delta < -config.epsilon:
            score -= 1

    if score >= config.min_consensus:
        side = SideSignal.UP
    elif score <= -config.min_consensus:
        side = SideSignal.DOWN
    else:
        return TrendSignal(side=SideSignal.SKIP, score=score, entry_ts=entry_ts, entry_price=None, reason="no_consensus")

    selected_points = up_points if side == SideSignal.UP else down_points
    selected_entry_price = price_at_or_before(selected_points, entry_ts)
    if selected_entry_price is None:
        return TrendSignal(side=SideSignal.SKIP, score=score, entry_ts=entry_ts, entry_price=None, reason="missing_contract_entry")

    if config.price_cap is not None and selected_entry_price > config.price_cap:
        return TrendSignal(side=SideSignal.SKIP, score=score, entry_ts=entry_ts, entry_price=selected_entry_price, reason="price_above_cap")

    breakout_probability = estimate_breakout_probability(
        points=binance_points,
        entry_ts=entry_ts,
        entry_price=binance_entry,
        market_end_ts=market_end_ts,
        config=config,
        breakout_level=price_to_beat,
    )
    opposite_implied_probability = max(0.0, min(1.0, 1.0 - selected_entry_price))
    if breakout_probability >= opposite_implied_probability:
        return TrendSignal(
            side=SideSignal.SKIP,
            score=score,
            entry_ts=entry_ts,
            entry_price=selected_entry_price,
            breakout_probability=breakout_probability,
            opposite_implied_probability=opposite_implied_probability,
            reason="breakout_risk_too_high",
        )

    return TrendSignal(
        side=side,
        score=score,
        entry_ts=entry_ts,
        entry_price=selected_entry_price,
        breakout_probability=breakout_probability,
        opposite_implied_probability=opposite_implied_probability,
    )


def _winner_for_reference(market: MarketBacktestInput, reference: str) -> SideSignal:
    if reference == "binance":
        return SideSignal.UP if market.binance_end_price >= market.binance_start_price else SideSignal.DOWN
    return SideSignal.UP if market.final_price >= market.price_to_beat else SideSignal.DOWN


def backtest_market(
    market: MarketBacktestInput,
    config: BinanceTrendConfig,
    reference: str = "chainlink",
) -> MarketBacktestResult:
    signal = compute_multiframe_signal(
        binance_points=market.binance_points,
        up_points=market.up_points,
        down_points=market.down_points,
        market_end_ts=market.end_ts,
        price_to_beat=market.price_to_beat,
        config=config,
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
    )


def summarize_results(results: list[MarketBacktestResult]) -> dict[str, float | int | dict[str, int]]:
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
