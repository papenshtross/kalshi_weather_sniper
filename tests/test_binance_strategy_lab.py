import pytest

from polybot.backtest.binance_strategy_lab import (
    BinanceLabConfig,
    CandlePoint,
    MarketBacktestInput,
    PricePoint,
    SideSignal,
    StrategySpec,
    backtest_market,
    build_strategy_universe,
    evaluate_strategy_signal,
    replay_live_decision,
)


def _candles(values, start_ts=0):
    candles = []
    ts = start_ts
    for close, volume, taker_buy_ratio in values:
        open_price = candles[-1].close if candles else close - 0.1
        high = max(open_price, close) + 0.05
        low = min(open_price, close) - 0.05
        taker_buy_volume = volume * taker_buy_ratio
        candles.append(
            CandlePoint(
                ts=ts,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=volume,
                taker_buy_volume=taker_buy_volume,
            )
        )
        ts += 1
    return candles


def test_drift_vol_signal_votes_up_on_smooth_rally():
    candles = _candles([
        (100.0, 10, 0.55),
        (100.1, 11, 0.56),
        (100.2, 11, 0.57),
        (100.35, 12, 0.59),
        (100.5, 12, 0.60),
        (100.7, 13, 0.62),
        (100.9, 13, 0.63),
        (101.1, 14, 0.64),
        (101.25, 14, 0.65),
        (101.4, 15, 0.66),
    ], start_ts=90)
    spec = StrategySpec(
        name="drift-vol-up",
        family="drift_vol_consensus",
        windows=(3, 5, 8),
        entry_seconds_before_close=1,
        min_consensus=2,
        price_cap=0.8,
        drift_threshold=0.7,
    )

    signal = evaluate_strategy_signal(
        candles=candles,
        up_points=[PricePoint(ts=99, price=0.58)],
        down_points=[PricePoint(ts=99, price=0.42)],
        market_end_ts=100,
        price_to_beat=100.5,
        config=BinanceLabConfig(),
        strategy=spec,
    )

    assert signal.side == SideSignal.UP
    assert signal.score > 0
    assert signal.entry_price == pytest.approx(0.58)


def test_ema_stack_signal_votes_down_on_bearish_alignment():
    candles = _candles([
        (101.6, 10, 0.48),
        (101.4, 11, 0.46),
        (101.1, 11, 0.45),
        (100.9, 12, 0.44),
        (100.7, 12, 0.43),
        (100.5, 13, 0.42),
        (100.35, 13, 0.40),
        (100.2, 14, 0.39),
        (100.0, 14, 0.38),
        (99.8, 15, 0.37),
    ], start_ts=90)
    spec = StrategySpec(
        name="ema-stack-down",
        family="ema_stack",
        windows=(3, 5, 8),
        entry_seconds_before_close=1,
        min_consensus=2,
        price_cap=0.8,
    )

    signal = evaluate_strategy_signal(
        candles=candles,
        up_points=[PricePoint(ts=99, price=0.35)],
        down_points=[PricePoint(ts=99, price=0.61)],
        market_end_ts=100,
        price_to_beat=100.5,
        config=BinanceLabConfig(),
        strategy=spec,
    )

    assert signal.side == SideSignal.DOWN
    assert signal.entry_price == pytest.approx(0.61)


def test_vwap_taker_signal_skips_without_confirmation():
    candles = _candles([
        (100.0, 10, 0.45),
        (100.2, 10, 0.46),
        (100.4, 10, 0.44),
        (100.6, 10, 0.43),
        (100.8, 10, 0.42),
        (101.0, 10, 0.41),
        (101.1, 10, 0.40),
        (101.2, 10, 0.39),
        (101.3, 10, 0.38),
        (101.4, 10, 0.37),
    ], start_ts=90)
    spec = StrategySpec(
        name="vwap-needs-flow",
        family="vwap_taker",
        windows=(5,),
        entry_seconds_before_close=1,
        min_consensus=1,
        price_cap=0.8,
        taker_imbalance_threshold=0.1,
        vwap_deviation_threshold=0.2,
    )

    signal = evaluate_strategy_signal(
        candles=candles,
        up_points=[PricePoint(ts=99, price=0.52)],
        down_points=[PricePoint(ts=99, price=0.48)],
        market_end_ts=100,
        price_to_beat=100.5,
        config=BinanceLabConfig(),
        strategy=spec,
    )

    assert signal.side == SideSignal.SKIP
    assert signal.reason == "no_consensus"


def test_replay_live_decision_matches_backtest_decision_without_future_leakage():
    market = MarketBacktestInput(
        market_id="m1",
        market_slug="btc-updown-5m-100",
        start_ts=0,
        end_ts=100,
        price_to_beat=100.2,
        final_price=100.6,
        binance_start_price=99.9,
        binance_end_price=100.6,
        candles=_candles([
            (99.95, 10, 0.52),
            (100.0, 10, 0.53),
            (100.1, 11, 0.55),
            (100.15, 11, 0.56),
            (100.2, 12, 0.57),
            (100.28, 12, 0.58),
            (100.34, 12, 0.59),
            (100.42, 13, 0.60),
            (100.5, 13, 0.61),
            (100.58, 14, 0.62),
        ], start_ts=90),
        up_points=[PricePoint(ts=99, price=0.57), PricePoint(ts=100, price=0.7)],
        down_points=[PricePoint(ts=99, price=0.43), PricePoint(ts=100, price=0.3)],
    )
    spec = StrategySpec(
        name="hybrid-parity",
        family="hybrid_score",
        windows=(3, 5, 8),
        entry_seconds_before_close=1,
        min_consensus=1,
        price_cap=0.8,
        score_threshold=0.25,
    )
    config = BinanceLabConfig()

    signal = evaluate_strategy_signal(
        candles=market.candles,
        up_points=market.up_points,
        down_points=market.down_points,
        market_end_ts=market.end_ts,
        price_to_beat=market.price_to_beat,
        config=config,
        strategy=spec,
    )
    replay = replay_live_decision(market, spec, config)
    result = backtest_market(market, spec, config=config, reference="chainlink")

    assert replay.side == signal.side
    assert replay.entry_price == signal.entry_price
    assert result.entry_price == signal.entry_price
    assert result.entry_ts == signal.entry_ts


def test_build_strategy_universe_returns_50_unique_strategies():
    strategies = build_strategy_universe()

    assert len(strategies) == 50
    assert len({strategy.name for strategy in strategies}) == 50
    families = {strategy.family for strategy in strategies}
    assert {"momentum_consensus", "drift_vol_consensus", "ema_stack", "breakout_pressure", "vwap_taker", "hybrid_score"}.issubset(families)
