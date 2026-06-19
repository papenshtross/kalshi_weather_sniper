import pytest

from polybot.backtest.binance_strategy_lab import CandlePoint, MarketBacktestInput, PricePoint, SideSignal
from polybot.backtest.momentum_consensus_dynamic_entry import (
    backtest_market_first_consensus,
    top_momentum_consensus_no_price_cap,
)


def _candles(values, start_ts=0):
    candles = []
    ts = start_ts
    for close in values:
        open_price = candles[-1].close if candles else close - 0.1
        candles.append(
            CandlePoint(
                ts=ts,
                open=open_price,
                high=max(open_price, close) + 0.05,
                low=min(open_price, close) - 0.05,
                close=close,
                volume=10.0,
                taker_buy_volume=6.0,
            )
        )
        ts += 1
    return candles


def test_dynamic_entry_uses_first_timestamp_where_consensus_appears():
    market = MarketBacktestInput(
        market_id="m1",
        market_slug="btc-updown-5m-100",
        start_ts=0,
        end_ts=100,
        price_to_beat=100.0,
        final_price=101.0,
        binance_start_price=100.0,
        binance_end_price=101.0,
        candles=_candles([
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            100.0, 100.0, 100.0, 100.1, 100.2, 100.3,
            100.4, 100.5, 100.6, 100.7, 100.8, 100.9,
            101.0, 101.1, 101.2, 101.3, 101.4, 101.5,
            101.6, 101.7, 101.8, 101.9, 102.0, 102.1,
            102.2, 102.3, 102.4, 102.5, 102.6, 102.7,
            102.8, 102.9, 103.0, 103.1, 103.2, 103.3,
            103.4, 103.5, 103.6, 103.7, 103.8, 103.9,
            104.0, 104.1, 104.2, 104.3, 104.4, 104.5,
            104.6, 104.7, 104.8, 104.9, 105.0, 105.1,
            105.2, 105.3, 105.4, 105.5, 105.6, 105.7,
            105.8, 105.9, 106.0, 106.1, 106.2, 106.3,
            106.4, 106.5, 106.6, 106.7, 106.8, 106.9,
            107.0,
        ]),
        up_points=[PricePoint(ts=85, price=0.99), PricePoint(ts=90, price=0.98), PricePoint(ts=95, price=0.97)],
        down_points=[PricePoint(ts=85, price=0.01), PricePoint(ts=90, price=0.02), PricePoint(ts=95, price=0.03)],
    )

    result = backtest_market_first_consensus(market, top_momentum_consensus_no_price_cap(), reference="chainlink")

    assert result.executed is True
    assert result.side == SideSignal.UP
    assert result.entry_ts < 95


def test_dynamic_entry_ignores_price_cap_and_can_enter_above_0_97():
    market = MarketBacktestInput(
        market_id="m2",
        market_slug="btc-updown-5m-200",
        start_ts=0,
        end_ts=100,
        price_to_beat=100.0,
        final_price=101.0,
        binance_start_price=100.0,
        binance_end_price=101.0,
        candles=_candles([100 + 0.1 * i for i in range(101)]),
        up_points=[PricePoint(ts=85, price=0.985)],
        down_points=[PricePoint(ts=85, price=0.015)],
    )

    result = backtest_market_first_consensus(market, top_momentum_consensus_no_price_cap(), reference="chainlink")

    assert result.executed is True
    assert result.entry_price == pytest.approx(0.985)
