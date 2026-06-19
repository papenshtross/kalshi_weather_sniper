from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import httpx

# Kalshi daily high-temperature city universe, normalized from Climate and
# Weather / Daily temperature series. Duplicate/legacy series are collapsed to
# the active KX* ticker where active markets were observed or strongly implied.
# Cities that also exist in the Polymarket weather safety table are marked with
# inherited_polymarket_risk_city=true and copy the Polymarket station mapping.
KALSHI_HIGH_TEMP_SERIES: dict[str, dict[str, Any]] = {
    "atlanta": {"series_ticker": "KXHIGHTATL", "city": "Atlanta", "lat": 33.6407, "lon": -84.4277, "station": "KATL", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
    "austin": {"series_ticker": "KXHIGHAUS", "city": "Austin", "lat": 30.1945, "lon": -97.6699, "station": "KAUS", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
    "chicago": {"series_ticker": "KXHIGHCHI", "city": "Chicago", "lat": 41.9742, "lon": -87.9073, "station": "KORD", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
    "dallas": {"series_ticker": "KXHIGHTDAL", "city": "Dallas", "lat": 32.8471, "lon": -96.8518, "station": "KDAL", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
    "denver": {"series_ticker": "KXHIGHDEN", "city": "Denver", "lat": 39.8466, "lon": -104.6562, "station": "KBKF", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
    "houston": {"series_ticker": "KXHIGHTHOU", "city": "Houston", "lat": 29.6454, "lon": -95.2789, "station": "KHOU", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
    "los-angeles": {"series_ticker": "KXHIGHLAX", "city": "Los Angeles", "lat": 33.9416, "lon": -118.4085, "station": "KLAX", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
    "miami": {"series_ticker": "KXHIGHMIA", "city": "Miami", "lat": 25.7959, "lon": -80.2870, "station": "KMIA", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
    "nyc": {"series_ticker": "KXHIGHNY", "city": "NYC", "lat": 40.7769, "lon": -73.8740, "station": "KLGA", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
    "san-francisco": {"series_ticker": "KXHIGHTSFO", "city": "San Francisco", "lat": 37.6213, "lon": -122.3790, "station": "KSFO", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 3.5, "nws_boundary_veto_degrees_f": 6.3},
    "seattle": {"series_ticker": "KXHIGHTSEA", "city": "Seattle", "lat": 47.4502, "lon": -122.3088, "station": "KSEA", "inherited_polymarket_risk_city": True, "polymarket_boundary_veto_degrees_c": 2.0, "nws_boundary_veto_degrees_f": 3.6},
}

KALSHI_EXTRA_HIGH_TEMP_SERIES: dict[str, dict[str, Any]] = {
    "boston": {"series_ticker": "KXHIGHTBOS", "city": "Boston", "lat": 42.3656, "lon": -71.0096, "station": "KBOS", "inherited_polymarket_risk_city": False},
    "las-vegas": {"series_ticker": "KXHIGHTLV", "city": "Las Vegas", "lat": 36.0840, "lon": -115.1537, "station": "KLAS", "inherited_polymarket_risk_city": False},
    "minneapolis": {"series_ticker": "KXHIGHTMIN", "city": "Minneapolis", "lat": 44.8848, "lon": -93.2223, "station": "KMSP", "inherited_polymarket_risk_city": False},
    "new-orleans": {"series_ticker": "KXHIGHTNOLA", "city": "New Orleans", "lat": 29.9934, "lon": -90.2580, "station": "KMSY", "inherited_polymarket_risk_city": False},
    "oklahoma-city": {"series_ticker": "KXHIGHTOKC", "city": "Oklahoma City", "lat": 35.3931, "lon": -97.6007, "station": "KOKC", "inherited_polymarket_risk_city": False},
    "philadelphia": {"series_ticker": "KXHIGHPHIL", "city": "Philadelphia", "lat": 39.8744, "lon": -75.2424, "station": "KPHL", "inherited_polymarket_risk_city": False},
    "phoenix": {"series_ticker": "KXHIGHTPHX", "city": "Phoenix", "lat": 33.4342, "lon": -112.0116, "station": "KPHX", "inherited_polymarket_risk_city": False},
    "san-antonio": {"series_ticker": "KXHIGHTSATX", "city": "San Antonio", "lat": 29.5337, "lon": -98.4698, "station": "KSAT", "inherited_polymarket_risk_city": False},
    "washington-dc": {"series_ticker": "KXHIGHTDC", "city": "Washington DC", "lat": 38.8512, "lon": -77.0402, "station": "KDCA", "inherited_polymarket_risk_city": False},
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
