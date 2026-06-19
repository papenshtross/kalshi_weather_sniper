import math

import pytest

from polybot.crypto.fair_price import (
    CryptoFairPriceModel,
    FairPriceSnapshot,
    estimate_realized_volatility,
    normal_cdf,
)


def test_normal_cdf_is_stable_without_scipy():
    assert normal_cdf(0) == pytest.approx(0.5)
    assert normal_cdf(1.0) == pytest.approx(0.841344746, abs=1e-9)
    assert normal_cdf(-1.0) == pytest.approx(0.158655254, abs=1e-9)


def test_estimate_realized_volatility_annualizes_one_second_log_returns():
    # Three 10bp-ish log returns.  The estimator should be finite, positive, and annualized.
    prices = [100.0, 100.1, 100.0, 100.2]
    vol = estimate_realized_volatility(prices, sample_seconds=1.0)
    assert 0.5 < vol <= 5.0


def test_fair_price_moves_with_reference_price_and_time_left():
    model = CryptoFairPriceModel(vol_floor=0.01, vol_cap=10.0)
    snap = model.price(
        start_price=100.0,
        current_price=101.0,
        seconds_to_expiry=60.0,
        recent_prices=[100.0, 100.2, 100.5, 100.8, 101.0],
        sample_seconds=1.0,
    )
    assert isinstance(snap, FairPriceSnapshot)
    assert snap.fair_up > 0.5
    assert snap.fair_down == pytest.approx(1.0 - snap.fair_up)
    assert 0.0 < snap.sigma_annualized <= 10.0


def test_fair_price_handles_zero_time_as_deterministic_resolution():
    model = CryptoFairPriceModel()
    assert model.price(start_price=100.0, current_price=101.0, seconds_to_expiry=0.0).fair_up == 1.0
    assert model.price(start_price=100.0, current_price=99.0, seconds_to_expiry=-1.0).fair_up == 0.0
    # Polymarket Up wins on equality: end >= start.
    assert model.price(start_price=100.0, current_price=100.0, seconds_to_expiry=0.0).fair_up == 1.0


def test_robust_volatility_clips_single_bad_tick_and_reports_metadata():
    prices = [100.0, 100.02, 100.03, 120.0, 100.04, 100.05, 100.06]
    model = CryptoFairPriceModel(vol_floor=0.01, vol_cap=10.0, winsor_sigma=3.0)
    snap = model.price(
        start_price=100.0,
        current_price=100.05,
        seconds_to_expiry=120.0,
        recent_prices=prices,
    )
    assert snap.vol_source in {"ewma", "sample"}
    assert snap.vol_observations == 6
    assert 0.01 <= snap.sigma_annualized <= 10.0


def test_latency_buffer_adds_uncertainty_toward_midpoint():
    base = CryptoFairPriceModel(vol_floor=0.5, vol_cap=0.5, latency_buffer_seconds=0.0)
    buffered = CryptoFairPriceModel(vol_floor=0.5, vol_cap=0.5, latency_buffer_seconds=2.0)
    kwargs = dict(start_price=100.0, current_price=100.02, seconds_to_expiry=10.0, recent_prices=[])
    assert buffered.price(**kwargs).fair_up < base.price(**kwargs).fair_up
