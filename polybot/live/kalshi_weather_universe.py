from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import httpx

# Only Kalshi cities that also exist in the Polymarket weather safety table are
# listed here; this keeps the copied yellow/red city universe intentionally
# smaller, as requested.
KALSHI_HIGH_TEMP_SERIES: dict[str, dict[str, Any]] = {
    "los-angeles": {"series_ticker": "KXHIGHLAX", "city": "Los Angeles", "lat": 33.9416, "lon": -118.4085, "station": "KLAX"},
    "atlanta": {"series_ticker": "KXHIGHTATL", "city": "Atlanta", "lat": 33.6407, "lon": -84.4277, "station": "KATL"},
    "houston": {"series_ticker": "KXHOUHIGH", "city": "Houston", "lat": 29.6454, "lon": -95.2789, "station": "KHOU"},
}

# Extra Kalshi daily-high cities discovered from Kalshi series, but not present
# in the Polymarket copied city risk list. They can be monitored/traded, but do
# not inherit a Polymarket red/yellow city mapping.
KALSHI_EXTRA_HIGH_TEMP_SERIES: dict[str, dict[str, Any]] = {
    "san-antonio": {"series_ticker": "KXHIGHTSATX", "city": "San Antonio", "lat": 29.5337, "lon": -98.4698, "station": "KSAT"},
    "oklahoma-city": {"series_ticker": "KXHIGHTOKC", "city": "Oklahoma City", "lat": 35.3931, "lon": -97.6007, "station": "KOKC"},
}

ALL_KALSHI_HIGH_TEMP_SERIES = {**KALSHI_HIGH_TEMP_SERIES, **KALSHI_EXTRA_HIGH_TEMP_SERIES}


@dataclass(frozen=True)
class NwsForecast:
    high_f: float | None
    source: str
    checked_at: datetime
    raw: dict[str, Any]


def f_to_c(f: float) -> float:
    return (float(f) - 32.0) * 5.0 / 9.0


async def nws_daily_high(lat: float, lon: float, target: date | None = None) -> NwsForecast:
    target = target or datetime.now(timezone.utc).date()
    headers = {"User-Agent": "kalshi-weather-sniper/0.1 (forecast veto)", "Accept": "application/geo+json,application/json"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0), headers=headers) as client:
        points = await client.get(f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}")
        points.raise_for_status()
        points_json = points.json()
        forecast_url = points_json.get("properties", {}).get("forecast")
        if not forecast_url:
            raise RuntimeError("NWS points response did not include forecast URL")
        forecast = await client.get(forecast_url)
        forecast.raise_for_status()
        data = forecast.json()
    highs: list[float] = []
    for period in data.get("properties", {}).get("periods", []) or []:
        start = str(period.get("startTime") or "")[:10]
        is_daytime = bool(period.get("isDaytime"))
        temp = period.get("temperature")
        unit = str(period.get("temperatureUnit") or "F").upper()
        if start == target.isoformat() and is_daytime and temp is not None:
            val = float(temp)
            highs.append(val if unit == "F" else val * 9.0 / 5.0 + 32.0)
    return NwsForecast(max(highs) if highs else None, forecast_url, datetime.now(timezone.utc), data)


def boundary_veto_reason(candidate_temp_f: float | None, forecast_high_f: float | None, threshold_f: float) -> str | None:
    if candidate_temp_f is None or forecast_high_f is None or threshold_f <= 0:
        return None
    distance = abs(float(candidate_temp_f) - float(forecast_high_f))
    if distance <= threshold_f + 1e-9:
        return f"NWS boundary veto: candidate {candidate_temp_f:g}°F is {distance:.1f}°F from NWS high {forecast_high_f:g}°F (threshold {threshold_f:g}°F)"
    return None
