"""Latency-friendly fair-value model for crypto Up/Down binaries.

The implementation is deliberately dependency-light so the exact same code can be
used in live trading and backtests.  The model prices the Up leg as a short-
horizon lognormal terminal probability around the official market strike/start
price and uses a robust realized-volatility estimator to avoid one bad tick or a
quiet window dominating the decision.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import median
from typing import Iterable, Sequence

SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0


def normal_cdf(z: float) -> float:
    """Numerically stable standard-normal CDF without scipy."""
    if math.isnan(z):
        return 0.5
    if z >= 8.0:
        return 1.0
    if z <= -8.0:
        return 0.0
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _clean_prices(prices: Sequence[float] | Iterable[float]) -> list[float]:
    out: list[float] = []
    for raw in prices:
        try:
            px = float(raw)
        except Exception:
            continue
        if px > 0 and math.isfinite(px):
            out.append(px)
    return out


def _log_returns(prices: Sequence[float] | Iterable[float]) -> list[float]:
    vals = _clean_prices(prices)
    return [math.log(vals[i] / vals[i - 1]) for i in range(1, len(vals)) if vals[i - 1] > 0]


def _winsorize_returns(returns: list[float], winsor_sigma: float) -> list[float]:
    """Clip extreme returns using a robust median/MAD scale estimate."""
    if len(returns) < 3 or winsor_sigma <= 0:
        return returns
    med = median(returns)
    abs_dev = [abs(r - med) for r in returns]
    mad = median(abs_dev)
    if mad <= 0 or not math.isfinite(mad):
        return returns
    robust_sigma = 1.4826 * mad
    lo = med - winsor_sigma * robust_sigma
    hi = med + winsor_sigma * robust_sigma
    return [min(hi, max(lo, r)) for r in returns]


@dataclass(frozen=True)
class VolatilitySnapshot:
    sigma_annualized: float
    observations: int
    source: str


def estimate_realized_volatility_snapshot(
    prices: Sequence[float] | Iterable[float],
    *,
    sample_seconds: float = 1.0,
    min_returns: int = 2,
    fallback_sigma: float = 0.80,
    vol_floor: float = 0.05,
    vol_cap: float = 5.0,
    ewma_lambda: float = 0.94,
    winsor_sigma: float = 6.0,
) -> VolatilitySnapshot:
    """Estimate annualized realized volatility from equally spaced prices.

    The estimator is robust for live use:
    - ignores bad/non-positive prices;
    - winsorizes one-off outlier returns with median/MAD;
    - uses an EWMA variance so recent volatility matters more than stale history;
    - applies floor/cap and a fallback for missing data.
    """
    rets = _winsorize_returns(_log_returns(prices), float(winsor_sigma or 0.0))
    if len(rets) < int(min_returns):
        sigma = float(fallback_sigma)
        source = "fallback"
    else:
        lam = min(0.999, max(0.0, float(ewma_lambda)))
        mean = sum(rets) / len(rets)
        if lam <= 0.0 or len(rets) < 4:
            var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
            source = "sample"
        else:
            # Newest return gets weight 1-lambda.  Normalize weights to keep the
            # scale stable for short windows.
            weights = [(1.0 - lam) * (lam ** (len(rets) - 1 - i)) for i in range(len(rets))]
            wsum = sum(weights) or 1.0
            wmean = sum(w * r for w, r in zip(weights, rets)) / wsum
            var = sum(w * (r - wmean) ** 2 for w, r in zip(weights, rets)) / wsum
            source = "ewma"
        per_sample = math.sqrt(max(0.0, var))
        dt = max(float(sample_seconds), 1e-9)
        sigma = per_sample * math.sqrt(SECONDS_PER_YEAR / dt)
    if not math.isfinite(sigma) or sigma <= 0:
        sigma = float(fallback_sigma)
        source = "fallback"
    sigma = max(float(vol_floor), min(float(vol_cap), sigma))
    return VolatilitySnapshot(sigma_annualized=sigma, observations=len(rets), source=source)


def estimate_realized_volatility(
    prices: Sequence[float] | Iterable[float],
    *,
    sample_seconds: float = 1.0,
    min_returns: int = 2,
    fallback_sigma: float = 0.80,
    vol_floor: float = 0.05,
    vol_cap: float = 5.0,
    ewma_lambda: float = 0.94,
    winsor_sigma: float = 6.0,
) -> float:
    """Backward-compatible helper returning only annualized sigma."""
    return estimate_realized_volatility_snapshot(
        prices,
        sample_seconds=sample_seconds,
        min_returns=min_returns,
        fallback_sigma=fallback_sigma,
        vol_floor=vol_floor,
        vol_cap=vol_cap,
        ewma_lambda=ewma_lambda,
        winsor_sigma=winsor_sigma,
    ).sigma_annualized


@dataclass(frozen=True)
class FairPriceSnapshot:
    fair_up: float
    fair_down: float
    sigma_annualized: float
    z_score: float
    seconds_to_expiry: float
    start_price: float
    current_price: float
    vol_observations: int = 0
    vol_source: str = "unknown"
    log_moneyness: float = 0.0
    latency_buffer_seconds: float = 0.0


@dataclass(frozen=True)
class CryptoFairPriceModel:
    """Short-horizon lognormal terminal-probability model.

    ``fair_up = P(S_expiry >= S_start | S_now, sigma, time_left)``.
    At live horizons (5m/15m), drift is intentionally omitted; latency/staleness
    is represented as extra variance time via ``latency_buffer_seconds``.
    """

    fallback_sigma: float = 0.80
    vol_floor: float = 0.05
    vol_cap: float = 5.0
    min_seconds_to_expiry: float = 0.001
    ewma_lambda: float = 0.94
    winsor_sigma: float = 6.0
    latency_buffer_seconds: float = 0.0

    def price(
        self,
        *,
        start_price: float,
        current_price: float,
        seconds_to_expiry: float,
        recent_prices: Sequence[float] | Iterable[float] | None = None,
        sample_seconds: float = 1.0,
        volatility_override: float | None = None,
    ) -> FairPriceSnapshot:
        start = float(start_price)
        current = float(current_price)
        t = float(seconds_to_expiry)
        if start <= 0 or current <= 0 or not math.isfinite(start) or not math.isfinite(current):
            raise ValueError("start_price and current_price must be finite positive values")
        if t <= 0:
            # Polymarket crypto Up/Down rules resolve Up on equality (end >= start).
            fair_up = 1.0 if current >= start else 0.0
            z = math.inf if current > start else (-math.inf if current < start else 0.0)
            return FairPriceSnapshot(
                fair_up=fair_up,
                fair_down=1.0 - fair_up,
                sigma_annualized=float(self.fallback_sigma),
                z_score=z,
                seconds_to_expiry=t,
                start_price=start,
                current_price=current,
                vol_source="deterministic_expired",
                log_moneyness=math.log(current / start),
            )
        if volatility_override is not None and float(volatility_override) > 0 and math.isfinite(float(volatility_override)):
            vol = VolatilitySnapshot(
                sigma_annualized=max(float(self.vol_floor), min(float(self.vol_cap), float(volatility_override))),
                observations=0,
                source="override",
            )
        else:
            vol = estimate_realized_volatility_snapshot(
                list(recent_prices or []),
                sample_seconds=sample_seconds,
                fallback_sigma=self.fallback_sigma,
                vol_floor=self.vol_floor,
                vol_cap=self.vol_cap,
                ewma_lambda=self.ewma_lambda,
                winsor_sigma=self.winsor_sigma,
            )
        effective_t = max(t + max(0.0, float(self.latency_buffer_seconds)), self.min_seconds_to_expiry)
        log_m = math.log(current / start)
        denom = vol.sigma_annualized * math.sqrt(effective_t / SECONDS_PER_YEAR)
        z = log_m / max(denom, 1e-12)
        fair_up = normal_cdf(z)
        return FairPriceSnapshot(
            fair_up=max(0.0, min(1.0, fair_up)),
            fair_down=max(0.0, min(1.0, 1.0 - fair_up)),
            sigma_annualized=vol.sigma_annualized,
            z_score=z,
            seconds_to_expiry=t,
            start_price=start,
            current_price=current,
            vol_observations=vol.observations,
            vol_source=vol.source,
            log_moneyness=log_m,
            latency_buffer_seconds=max(0.0, float(self.latency_buffer_seconds)),
        )


def fair_edge_accepts_pair(
    *,
    yes_avg: float,
    no_avg: float,
    fair_up: float,
    min_model_edge: float,
    max_leg_overpay: float = 0.0,
) -> bool:
    """Return True when both legs are acceptable under model fair.

    ``max_leg_overpay`` allows a tiny overpay on one leg only when complementary
    pair edge is already enforced elsewhere.  Default is strict: both legs must be
    below fair by ``min_model_edge``.
    """
    try:
        y = float(yes_avg)
        n = float(no_avg)
        up = float(fair_up)
        edge = float(min_model_edge)
        overpay = float(max_leg_overpay or 0.0)
    except Exception:
        return False
    if not (0.0 <= y <= 1.0 and 0.0 <= n <= 1.0 and 0.0 <= up <= 1.0):
        return False
    fair_down = 1.0 - up
    return y <= up - edge + overpay + 1e-12 and n <= fair_down - edge + overpay + 1e-12
