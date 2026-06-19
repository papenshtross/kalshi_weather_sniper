import pytest

from polybot.backtest.trend_close_5m import (
    CloseTrendConfig,
    MarketBacktestInput,
    MarketBacktestResult,
    PricePoint,
    SideSignal,
    backtest_market,
    compute_trend_signal,
    price_at_or_before,
    summarize_results,
)


def test_price_at_or_before_returns_latest_known_point():
    points = [
        PricePoint(ts=100, price=0.45),
        PricePoint(ts=105, price=0.47),
        PricePoint(ts=111, price=0.49),
    ]

    assert price_at_or_before(points, 109) == pytest.approx(0.47)
    assert price_at_or_before(points, 111) == pytest.approx(0.49)
    assert price_at_or_before(points, 99) is None


def test_compute_trend_signal_votes_up_when_all_windows_rising():
    points = [
        PricePoint(ts=60, price=0.37),
        PricePoint(ts=70, price=0.40),
        PricePoint(ts=80, price=0.43),
        PricePoint(ts=85, price=0.46),
        PricePoint(ts=88, price=0.49),
        PricePoint(ts=89, price=0.50),
        PricePoint(ts=90, price=0.52),
    ]
    config = CloseTrendConfig(entry_seconds_before_close=10, trend_windows=(1, 2, 5, 10, 15, 30), min_consensus=2)

    signal = compute_trend_signal(points, market_end_ts=100, config=config)

    assert signal.side == SideSignal.UP
    assert signal.score == 6
    assert signal.entry_ts == 90
    assert signal.entry_price == pytest.approx(0.52)
    assert signal.breakout_probability < signal.opposite_implied_probability


def test_compute_trend_signal_votes_down_when_all_windows_falling():
    points = [
        PricePoint(ts=60, price=0.40),
        PricePoint(ts=70, price=0.30),
        PricePoint(ts=80, price=0.20),
        PricePoint(ts=85, price=0.10),
        PricePoint(ts=88, price=0.06),
        PricePoint(ts=89, price=0.04),
        PricePoint(ts=90, price=0.02),
    ]
    config = CloseTrendConfig(entry_seconds_before_close=10, trend_windows=(1, 2, 5, 10, 15, 30), min_consensus=2)

    signal = compute_trend_signal(points, market_end_ts=100, config=config)

    assert signal.side == SideSignal.DOWN
    assert signal.score == -6
    assert signal.entry_price == pytest.approx(0.02)
    assert signal.breakout_probability < signal.opposite_implied_probability


def test_backtest_market_uses_down_price_for_down_signal():
    up_points = [
        PricePoint(ts=60, price=0.40),
        PricePoint(ts=70, price=0.30),
        PricePoint(ts=80, price=0.20),
        PricePoint(ts=85, price=0.10),
        PricePoint(ts=88, price=0.06),
        PricePoint(ts=89, price=0.04),
        PricePoint(ts=90, price=0.02),
    ]
    down_points = [
        PricePoint(ts=60, price=0.60),
        PricePoint(ts=70, price=0.70),
        PricePoint(ts=80, price=0.80),
        PricePoint(ts=85, price=0.90),
        PricePoint(ts=88, price=0.94),
        PricePoint(ts=89, price=0.96),
        PricePoint(ts=90, price=0.98),
    ]
    market = MarketBacktestInput(
        market_id='m-down',
        market_slug='btc-updown-5m-100-down',
        question='Bitcoin Up or Down',
        start_ts=95,
        end_ts=100,
        winner=SideSignal.DOWN,
        up_points=up_points,
        down_points=down_points,
    )
    config = CloseTrendConfig(entry_seconds_before_close=10, trend_windows=(1, 2, 5, 10, 15, 30), min_consensus=2)

    result = backtest_market(market, config)

    assert result.executed is True
    assert result.side == SideSignal.DOWN
    assert result.entry_price == pytest.approx(0.98)
    assert result.payout == pytest.approx(1.0)
    assert result.pnl == pytest.approx(0.02)


def test_compute_trend_signal_skips_when_history_is_missing():
    points = [PricePoint(ts=89, price=0.50), PricePoint(ts=90, price=0.51)]
    config = CloseTrendConfig(entry_seconds_before_close=10, trend_windows=(1, 2, 5, 10), min_consensus=2)

    signal = compute_trend_signal(points, market_end_ts=100, config=config)

    assert signal.side == SideSignal.SKIP
    assert signal.reason == 'insufficient_history'


def test_compute_trend_signal_skips_when_breakout_risk_exceeds_opposite_implied_probability():
    points = [
        PricePoint(ts=60, price=0.50),
        PricePoint(ts=70, price=0.70),
        PricePoint(ts=80, price=0.90),
        PricePoint(ts=85, price=0.96),
        PricePoint(ts=88, price=0.99),
        PricePoint(ts=89, price=0.97),
        PricePoint(ts=90, price=0.98),
    ]
    config = CloseTrendConfig(
        entry_seconds_before_close=10,
        trend_windows=(1, 2, 5, 10, 15, 30),
        min_consensus=2,
        volatility_window_seconds=30,
    )

    signal = compute_trend_signal(points, market_end_ts=100, config=config)

    assert signal.side == SideSignal.SKIP
    assert signal.reason == 'breakout_risk_too_high'
    assert signal.breakout_probability > signal.opposite_implied_probability


def test_backtest_market_returns_winning_pnl_for_up_signal():
    up_points = [
        PricePoint(ts=60, price=0.37),
        PricePoint(ts=70, price=0.40),
        PricePoint(ts=80, price=0.43),
        PricePoint(ts=85, price=0.46),
        PricePoint(ts=88, price=0.49),
        PricePoint(ts=89, price=0.50),
        PricePoint(ts=90, price=0.52),
    ]
    down_points = [PricePoint(ts=90, price=0.48)]
    market = MarketBacktestInput(
        market_id='m1',
        market_slug='btc-updown-5m-100',
        question='Bitcoin Up or Down',
        start_ts=95,
        end_ts=100,
        winner=SideSignal.UP,
        up_points=up_points,
        down_points=down_points,
    )
    config = CloseTrendConfig(entry_seconds_before_close=10, trend_windows=(1, 2, 5, 10, 15, 30), min_consensus=2)

    result = backtest_market(market, config)

    assert result.executed is True
    assert result.side == SideSignal.UP
    assert result.entry_price == pytest.approx(0.52)
    assert result.payout == pytest.approx(1.0)
    assert result.pnl == pytest.approx(0.48)


def test_compute_trend_signal_supports_custom_breakout_level_and_implied_opposite_probability():
    points = [
        PricePoint(ts=60, price=100.0),
        PricePoint(ts=70, price=100.5),
        PricePoint(ts=80, price=101.0),
        PricePoint(ts=85, price=101.4),
        PricePoint(ts=88, price=101.7),
        PricePoint(ts=89, price=101.8),
        PricePoint(ts=90, price=101.9),
    ]
    config = CloseTrendConfig(entry_seconds_before_close=10, trend_windows=(1, 2, 5, 10, 15, 30), min_consensus=2)

    signal = compute_trend_signal(
        points,
        market_end_ts=100,
        config=config,
        breakout_level=100.0,
        opposite_implied_probability=0.03,
    )

    assert signal.side == SideSignal.UP
    assert signal.breakout_probability < 0.03
    assert signal.opposite_implied_probability == pytest.approx(0.03)


def test_summarize_results_reports_execution_and_pnl_stats():
    results = [
        MarketBacktestResult(
            market_id='a',
            market_slug='slug-a',
            executed=True,
            side=SideSignal.UP,
            score=4,
            entry_ts=95,
            entry_price=0.55,
            payout=1.0,
            pnl=0.45,
        ),
        MarketBacktestResult(
            market_id='b',
            market_slug='slug-b',
            executed=True,
            side=SideSignal.DOWN,
            score=-3,
            entry_ts=95,
            entry_price=0.61,
            payout=0.0,
            pnl=-0.61,
        ),
        MarketBacktestResult(
            market_id='c',
            market_slug='slug-c',
            executed=False,
            side=SideSignal.SKIP,
            score=0,
            entry_ts=95,
            entry_price=None,
            payout=0.0,
            pnl=0.0,
            reason='insufficient_history',
        ),
    ]

    summary = summarize_results(results)

    assert summary['markets_total'] == 3
    assert summary['executed_trades'] == 2
    assert summary['skipped_trades'] == 1
    assert summary['win_rate'] == pytest.approx(0.5)
    assert summary['total_pnl'] == pytest.approx(-0.16)
    assert summary['avg_pnl'] == pytest.approx(-0.08)
