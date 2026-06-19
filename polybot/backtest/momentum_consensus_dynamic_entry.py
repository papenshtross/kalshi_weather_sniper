from __future__ import annotations

from dataclasses import replace

from polybot.backtest.binance_strategy_lab import (
    BinanceLabConfig,
    MarketBacktestInput,
    MarketBacktestResult,
    SideSignal,
    StrategySpec,
    estimate_breakout_probability,
    price_at_or_before,
    build_strategy_universe,
)


TOP_NAME = "momentum_consensus_07"


def top_momentum_consensus_no_price_cap() -> StrategySpec:
    for strategy in build_strategy_universe():
        if strategy.name == TOP_NAME:
            return replace(strategy, price_cap=None)
    raise RuntimeError(f"strategy {TOP_NAME} not found")



def _winner_for_reference(market: MarketBacktestInput, reference: str) -> SideSignal:
    if reference == "binance":
        return SideSignal.UP if market.binance_end_price >= market.binance_start_price else SideSignal.DOWN
    return SideSignal.UP if market.final_price >= market.price_to_beat else SideSignal.DOWN



def backtest_market_first_consensus(
    market: MarketBacktestInput,
    strategy: StrategySpec | None = None,
    reference: str = "chainlink",
    config: BinanceLabConfig | None = None,
) -> MarketBacktestResult:
    base_strategy = strategy or top_momentum_consensus_no_price_cap()
    lab_config = config or BinanceLabConfig()
    max_window = max(base_strategy.windows)
    candles = market.candles
    close_by_ts = {candle.ts: candle.close for candle in candles}
    candle_by_ts = {candle.ts: candle for candle in candles}

    first_ts = market.start_ts + max_window
    last_ts = market.end_ts - 1

    for entry_ts in range(first_ts, last_ts + 1):
        entry_close = close_by_ts.get(entry_ts)
        if entry_close is None:
            continue
        score = 0
        for window in base_strategy.windows:
            base_price = close_by_ts.get(entry_ts - window)
            if base_price is None:
                score = 0
                break
            delta = entry_close - base_price
            if delta > lab_config.epsilon:
                score += 1
            elif delta < -lab_config.epsilon:
                score -= 1

        if score >= base_strategy.min_consensus:
            side = SideSignal.UP
            selected_points = market.up_points
        elif score <= -base_strategy.min_consensus:
            side = SideSignal.DOWN
            selected_points = market.down_points
        else:
            continue

        entry_price = price_at_or_before(selected_points, entry_ts)
        if entry_price is None:
            continue

        breakout_probability = estimate_breakout_probability(
            candles=candles,
            entry_ts=entry_ts,
            entry_price=entry_close,
            market_end_ts=market.end_ts,
            config=lab_config,
            breakout_level=market.price_to_beat,
        )
        opposite_implied_probability = max(0.0, min(1.0, 1.0 - entry_price))
        if breakout_probability >= opposite_implied_probability:
            continue

        winner = _winner_for_reference(market, reference)
        payout = 1.0 if side == winner else 0.0
        pnl = payout - entry_price
        net_pnl = pnl - lab_config.assumed_fee_per_share
        return MarketBacktestResult(
            market_id=market.market_id,
            market_slug=market.market_slug,
            executed=True,
            side=side,
            score=float(score),
            entry_ts=entry_ts,
            entry_price=entry_price,
            payout=payout,
            pnl=pnl,
            net_pnl=net_pnl,
            breakout_probability=breakout_probability,
            opposite_implied_probability=opposite_implied_probability,
            reference=reference,
        )

    return MarketBacktestResult(
        market_id=market.market_id,
        market_slug=market.market_slug,
        executed=False,
        side=SideSignal.SKIP,
        score=0.0,
        entry_ts=market.end_ts,
        entry_price=None,
        payout=0.0,
        pnl=0.0,
        net_pnl=0.0,
        reference=reference,
        reason="no_consensus",
    )
