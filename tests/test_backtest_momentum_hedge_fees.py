import pytest

from polybot.backtest.binance_strategy_lab import BinanceLabConfig, CandlePoint, MarketBacktestInput, PricePoint
from scripts.backtest_momentum_requested_variations import (
    backtest_first_consensus_with_optional_hedge,
    polymarket_dynamic_fee_per_share,
)


def _candles_for_entry_then_weakening() -> list[CandlePoint]:
    candles = []
    for ts in range(90):
        close = 100.0
        if ts == 80:
            close = 101.0  # First timestamp with 5/5 UP consensus.
        elif ts > 80:
            close = 99.0  # Original UP consensus immediately weakens below trigger.
        candles.append(CandlePoint(ts=ts, open=close, high=close, low=close, close=close, volume=1.0, taker_buy_volume=0.5))
    return candles


def _market(*, hedge_price: float, final_price: float = 101.0) -> MarketBacktestInput:
    return MarketBacktestInput(
        market_id="m1",
        market_slug="btc-updown-5m-fee-test",
        start_ts=0,
        end_ts=90,
        price_to_beat=100.0,
        final_price=final_price,
        binance_start_price=100.0,
        binance_end_price=final_price,
        candles=_candles_for_entry_then_weakening(),
        up_points=[PricePoint(ts=80, price=0.50)],
        down_points=[PricePoint(ts=81, price=hedge_price)],
    )


def test_polymarket_dynamic_fee_per_share_uses_price_dependent_formula():
    assert polymarket_dynamic_fee_per_share(0.50) == pytest.approx(0.072 * 0.50 * 0.50)
    assert polymarket_dynamic_fee_per_share(0.90) == pytest.approx(0.072 * 0.90 * 0.10)
    assert polymarket_dynamic_fee_per_share(-1.0) == 0.0
    assert polymarket_dynamic_fee_per_share(2.0) == 0.0


def test_hedge_requires_profit_buffer_after_entry_and_hedge_dynamic_fees():
    result = backtest_first_consensus_with_optional_hedge(
        _market(hedge_price=0.38),
        min_consensus=5,
        hedge_profit_buffer=0.10,
        reference="chainlink",
        config=BinanceLabConfig(assumed_fee_per_share=0.0, contract_trade_staleness_seconds=20),
        hedge_trigger_consensus=3,
    )

    assert result["executed"] is True
    assert result["hedged"] is False
    assert result["hedge_target"] == pytest.approx(0.3653062531909217)


def test_hedged_pnl_subtracts_both_dynamic_taker_fees():
    result = backtest_first_consensus_with_optional_hedge(
        _market(hedge_price=0.36),
        min_consensus=5,
        hedge_profit_buffer=0.10,
        reference="chainlink",
        config=BinanceLabConfig(assumed_fee_per_share=0.0, contract_trade_staleness_seconds=20),
        hedge_trigger_consensus=3,
    )

    entry_fee = 0.072 * 0.50 * 0.50
    hedge_fee = 0.072 * 0.36 * 0.64
    assert result["hedged"] is True
    assert result["gross_pnl"] == pytest.approx(1.0 - 0.50 - 0.36)
    assert result["entry_fee"] == pytest.approx(entry_fee)
    assert result["hedge_fee"] == pytest.approx(hedge_fee)
    assert result["pnl"] == pytest.approx(1.0 - 0.50 - 0.36 - entry_fee - hedge_fee)
    assert result["net_pnl"] == pytest.approx(result["pnl"])
