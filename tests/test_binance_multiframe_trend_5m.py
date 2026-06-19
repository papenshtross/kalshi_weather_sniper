import pytest

from polybot.backtest.binance_multiframe_trend_5m import (
    BinanceTrendConfig,
    MarketBacktestInput,
    PricePoint,
    SideSignal,
    backtest_market,
    compute_multiframe_signal,
    summarize_results,
)


def test_compute_multiframe_signal_votes_up_when_all_timeframes_rise():
    binance_points = [
        PricePoint(ts=35, price=99.7),
        PricePoint(ts=40, price=100.0),
        PricePoint(ts=55, price=100.4),
        PricePoint(ts=70, price=100.8),
        PricePoint(ts=85, price=101.2),
        PricePoint(ts=95, price=101.5),
    ]
    up_points = [PricePoint(ts=95, price=0.62)]
    down_points = [PricePoint(ts=95, price=0.38)]
    config = BinanceTrendConfig(entry_seconds_before_close=5, trend_windows=(5, 15, 30, 60), min_consensus=3, price_cap=0.8)

    signal = compute_multiframe_signal(
        binance_points=binance_points,
        up_points=up_points,
        down_points=down_points,
        market_end_ts=100,
        price_to_beat=100.0,
        config=config,
    )

    assert signal.side == SideSignal.UP
    assert signal.score == 4
    assert signal.entry_price == pytest.approx(0.62)
    assert signal.breakout_probability < signal.opposite_implied_probability


def test_backtest_market_uses_down_contract_price_for_down_signal():
    market = MarketBacktestInput(
        market_id="m-down",
        market_slug="btc-updown-5m-100",
        start_ts=0,
        end_ts=100,
        price_to_beat=98.0,
        final_price=97.5,
        binance_start_price=100.0,
        binance_end_price=99.4,
        binance_points=[
            PricePoint(ts=35, price=101.0),
            PricePoint(ts=65, price=100.6),
            PricePoint(ts=80, price=100.2),
            PricePoint(ts=90, price=99.9),
            PricePoint(ts=95, price=99.7),
        ],
        up_points=[PricePoint(ts=95, price=0.04)],
        down_points=[PricePoint(ts=95, price=0.96)],
    )
    config = BinanceTrendConfig(entry_seconds_before_close=5, trend_windows=(5, 15, 30, 60), min_consensus=3, price_cap=0.99)

    result = backtest_market(market, config, reference="chainlink")

    assert result.executed is True
    assert result.side == SideSignal.DOWN
    assert result.entry_price == pytest.approx(0.96)
    assert result.pnl == pytest.approx(0.04)


def test_compute_multiframe_signal_skips_when_selected_contract_is_above_price_cap():
    binance_points = [
        PricePoint(ts=35, price=99.7),
        PricePoint(ts=40, price=100.0),
        PricePoint(ts=55, price=100.4),
        PricePoint(ts=70, price=100.8),
        PricePoint(ts=85, price=101.2),
        PricePoint(ts=95, price=101.5),
    ]
    config = BinanceTrendConfig(entry_seconds_before_close=5, trend_windows=(5, 15, 30, 60), min_consensus=3, price_cap=0.6)

    signal = compute_multiframe_signal(
        binance_points=binance_points,
        up_points=[PricePoint(ts=95, price=0.74)],
        down_points=[PricePoint(ts=95, price=0.26)],
        market_end_ts=100,
        price_to_beat=100.0,
        config=config,
    )

    assert signal.side == SideSignal.SKIP
    assert signal.reason == "price_above_cap"


def test_summarize_results_reports_skip_reasons_and_net_pnl():
    market_win = MarketBacktestInput(
        market_id="a",
        market_slug="slug-a",
        start_ts=0,
        end_ts=100,
        price_to_beat=100.0,
        final_price=101.0,
        binance_start_price=100.0,
        binance_end_price=101.0,
        binance_points=[
            PricePoint(ts=35, price=99.8),
            PricePoint(ts=40, price=100.0),
            PricePoint(ts=80, price=100.7),
            PricePoint(ts=90, price=100.9),
            PricePoint(ts=95, price=101.0),
        ],
        up_points=[PricePoint(ts=95, price=0.55)],
        down_points=[PricePoint(ts=95, price=0.45)],
    )
    market_loss = MarketBacktestInput(
        market_id="b",
        market_slug="slug-b",
        start_ts=0,
        end_ts=100,
        price_to_beat=100.0,
        final_price=99.0,
        binance_start_price=100.0,
        binance_end_price=99.0,
        binance_points=[
            PricePoint(ts=35, price=99.8),
            PricePoint(ts=40, price=100.0),
            PricePoint(ts=80, price=100.7),
            PricePoint(ts=90, price=100.9),
            PricePoint(ts=95, price=101.0),
        ],
        up_points=[PricePoint(ts=95, price=0.61)],
        down_points=[PricePoint(ts=95, price=0.39)],
    )
    config = BinanceTrendConfig(entry_seconds_before_close=5, trend_windows=(5, 15, 30, 60), min_consensus=1, price_cap=0.99, assumed_fee_per_share=0.01)

    executed_win = backtest_market(market_win, config, reference="chainlink")
    executed_loss = backtest_market(market_loss, config, reference="chainlink")
    skipped = executed_loss.__class__(
        market_id="c",
        market_slug="slug-c",
        executed=False,
        side=SideSignal.SKIP,
        score=0,
        entry_ts=95,
        entry_price=None,
        payout=0.0,
        pnl=0.0,
        net_pnl=0.0,
        breakout_probability=0.0,
        opposite_implied_probability=0.0,
        reference="chainlink",
        reason="price_above_cap",
    )

    summary = summarize_results([executed_win, executed_loss, skipped])

    assert summary["markets_total"] == 3
    assert summary["executed_trades"] == 2
    assert summary["skipped_trades"] == 1
    assert summary["skip_reasons"]["price_above_cap"] == 1
    assert summary["total_net_pnl"] == pytest.approx((0.45 - 0.01) + (-0.61 - 0.01))
