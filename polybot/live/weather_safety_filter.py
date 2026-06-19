"""Weather event safety filter for live daily high-temperature outlier shards.

The filter is deliberately split from the trading strategy: it can be evaluated
and shown on the dashboard while enforcement remains disabled.  When enforcement
is enabled, RED blocks new BUY entries and YELLOW allows only the normal
single-order budget while disabling same-market re-buy/top-up ladders. Take-profit
SELLs are never blocked by this module.
"""
from __future__ import annotations

import asyncio
import os
import math
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx


@dataclass(frozen=True)
class StationSpec:
    city_slug: str
    city: str
    station_id: str
    source_class: str
    unit: str = "C"
    lat: float | None = None
    lon: float | None = None
    structural_note: str = ""


STATIONS: dict[str, StationSpec] = {
    "hong-kong": StationSpec("hong-kong", "Hong Kong", "HKO", "Hong Kong Observatory daily extract", "C", 22.302, 114.174, "HKO current exact-station obs unavailable; final source is official daily extract"),
    "moscow": StationSpec("moscow", "Moscow", "UUWW", "NOAA/Weather.gov WRH time series", "C"),
    "istanbul": StationSpec("istanbul", "Istanbul", "LTFM", "NOAA/Weather.gov WRH time series", "C"),
    "tel-aviv": StationSpec("tel-aviv", "Tel Aviv", "LLBG", "NOAA/Weather.gov WRH time series", "C"),
    "chicago": StationSpec("chicago", "Chicago", "KORD", "Wunderground station history", "F"),
    "nyc": StationSpec("nyc", "NYC", "KLGA", "Wunderground station history", "F"),
    "denver": StationSpec("denver", "Denver", "KBKF", "Wunderground station history", "F"),
    "seattle": StationSpec("seattle", "Seattle", "KSEA", "Wunderground station history", "F"),
    "miami": StationSpec("miami", "Miami", "KMIA", "Wunderground station history", "F"),
    "atlanta": StationSpec("atlanta", "Atlanta", "KATL", "Wunderground station history", "F"),
    "houston": StationSpec("houston", "Houston", "KHOU", "Wunderground station history", "F"),
    "austin": StationSpec("austin", "Austin", "KAUS", "Wunderground station history", "F"),
    "dallas": StationSpec("dallas", "Dallas", "KDAL", "Wunderground station history", "F"),
    "los-angeles": StationSpec("los-angeles", "Los Angeles", "KLAX", "Wunderground station history", "F"),
    "san-francisco": StationSpec("san-francisco", "San Francisco", "KSFO", "Wunderground station history", "F"),
    "london": StationSpec("london", "London", "EGLC", "Wunderground station history", "C"),
    "seoul": StationSpec("seoul", "Seoul", "RKSI", "Wunderground station history", "C"),
    "shanghai": StationSpec("shanghai", "Shanghai", "ZSPD", "Wunderground station history", "C"),
    "singapore": StationSpec("singapore", "Singapore", "WSSS", "Wunderground station history", "C"),
    "paris": StationSpec("paris", "Paris", "LFPB", "Wunderground station history", "C"),
    "buenos-aires": StationSpec("buenos-aires", "Buenos Aires", "SAEZ", "Wunderground station history", "C"),
    "wellington": StationSpec("wellington", "Wellington", "NZWN", "Wunderground station history", "C"),
    "jakarta": StationSpec("jakarta", "Jakarta", "WIHH", "Wunderground station history", "C"),
    "lucknow": StationSpec("lucknow", "Lucknow", "VILK", "Wunderground station history", "C"),
    "tokyo": StationSpec("tokyo", "Tokyo", "RJTT", "Wunderground station history", "C"),
    "helsinki": StationSpec("helsinki", "Helsinki", "EFHK", "Wunderground station history", "C"),
    "munich": StationSpec("munich", "Munich", "EDDM", "Wunderground station history", "C"),
    "beijing": StationSpec("beijing", "Beijing", "ZBAA", "Wunderground station history", "C"),
    "taipei": StationSpec("taipei", "Taipei", "RCSS", "Wunderground station history", "C"),
    "amsterdam": StationSpec("amsterdam", "Amsterdam", "EHAM", "Wunderground station history", "C"),
    "ankara": StationSpec("ankara", "Ankara", "LTAC", "Wunderground station history", "C"),
    "madrid": StationSpec("madrid", "Madrid", "LEMD", "Wunderground station history", "C"),
    "milan": StationSpec("milan", "Milan", "LIMC", "Wunderground station history", "C"),
    "warsaw": StationSpec("warsaw", "Warsaw", "EPWA", "Wunderground station history", "C"),
    "toronto": StationSpec("toronto", "Toronto", "CYYZ", "Wunderground station history", "C"),
    "chongqing": StationSpec("chongqing", "Chongqing", "ZUCK", "Wunderground station history", "C"),
    "wuhan": StationSpec("wuhan", "Wuhan", "ZHHH", "Wunderground station history", "C"),
    "chengdu": StationSpec("chengdu", "Chengdu", "ZUUU", "Wunderground station history", "C"),
    "shenzhen": StationSpec("shenzhen", "Shenzhen", "ZGSZ", "Wunderground station history", "C"),
    "kuala-lumpur": StationSpec("kuala-lumpur", "Kuala Lumpur", "WMKK", "Wunderground station history", "C"),
    "sao-paulo": StationSpec("sao-paulo", "São Paulo", "SBGR", "Wunderground station history", "C"),
    "manila": StationSpec("manila", "Manila", "RPLL", "Wunderground station history", "C"),
    "guangzhou": StationSpec("guangzhou", "Guangzhou", "ZGGG", "Wunderground station history", "C"),
    "karachi": StationSpec("karachi", "Karachi", "OPKC", "Wunderground station history", "C"),
    "busan": StationSpec("busan", "Busan", "RKPK", "Wunderground station history", "C"),
    "jeddah": StationSpec("jeddah", "Jeddah", "OEJN", "Wunderground station history", "C"),
    "mexico-city": StationSpec("mexico-city", "Mexico City", "MMMX", "Wunderground station history", "C"),
    "panama-city": StationSpec("panama-city", "Panama City", "MPMG", "Wunderground station history", "C", None, None, "Panama MPMG settlement station, not Florida Panhandle context"),
    "qingdao": StationSpec("qingdao", "Qingdao", "ZSQD", "Wunderground station history", "C"),
    "cape-town": StationSpec("cape-town", "Cape Town", "FACT", "Wunderground station history", "C"),
}

STRUCTURAL_RISK = {
    "denver", "mexico-city", "san-francisco", "los-angeles", "seattle", "wellington", "miami", "houston", "austin", "dallas", "atlanta", "chicago", "qingdao", "busan", "cape-town", "istanbul", "chengdu", "chongqing", "wuhan",
}

SEVERE_CODES = {95, 96, 99}
SNOW_CODES = {71, 73, 75, 77, 85, 86}
HEAVY_RAIN_CODES = {65, 82}
RAIN_CODES = {51, 53, 55, 61, 63, 65, 80, 81, 82}
FOG_CODES = {45, 48}
HEAVY_RAIN_MM_PER_HOUR = 2.5
HEAVY_WIND_GUST_KMH = 50.0
HEAVY_WIND_SPEED_KMH = 30.0

# Historical 51-city daily-high-change calibration retained for dashboard context only.
# The live sizing gate is now the empirically targeted chart-risk filter below:
# RED = snow/winter, rain + empirical bad city, or watch city + snow/heavy
# rain/heavy wind; YELLOW = empirical watch city, or non-watch city + heavy
# rain/heavy wind. Ordinary rain alone is context only and does not size.
# Static >=4C city risk is informational and does not size.
TEMP_4C_CHANGE_BASELINE_RATE = 0.13164573331393564
CITY_TEMP_4C_STATS: dict[str, dict[str, float]] = {
    "denver": {"rate": 0.3882934872217642, "events": 471, "n": 1213, "avg_abs_delta": 3.7681, "max_abs_delta": 21.6},
    "chicago": {"rate": 0.33470733718054413, "events": 406, "n": 1213, "avg_abs_delta": 3.5145, "max_abs_delta": 18.1},
    "toronto": {"rate": 0.2885408079142622, "events": 350, "n": 1213, "avg_abs_delta": 3.1472, "max_abs_delta": 18.7},
    "nyc": {"rate": 0.2868920032976092, "events": 348, "n": 1213, "avg_abs_delta": 3.0465, "max_abs_delta": 15.3},
    "cape-town": {"rate": 0.2786479802143446, "events": 338, "n": 1213, "avg_abs_delta": 2.9475, "max_abs_delta": 14.9},
    "dallas": {"rate": 0.24649629018961253, "events": 299, "n": 1213, "avg_abs_delta": 2.7599, "max_abs_delta": 22.6},
    "qingdao": {"rate": 0.22423742786479803, "events": 272, "n": 1213, "avg_abs_delta": 2.5782, "max_abs_delta": 14.4},
    "atlanta": {"rate": 0.213520197856554, "events": 259, "n": 1213, "avg_abs_delta": 2.4751, "max_abs_delta": 14.8},
    "austin": {"rate": 0.20362737015663643, "events": 247, "n": 1213, "avg_abs_delta": 2.5077, "max_abs_delta": 22.6},
    "beijing": {"rate": 0.20362737015663643, "events": 247, "n": 1213, "avg_abs_delta": 2.5495, "max_abs_delta": 14.2},
    "seoul": {"rate": 0.18878812860676009, "events": 229, "n": 1213, "avg_abs_delta": 2.3389, "max_abs_delta": 14.8},
    "munich": {"rate": 0.1838417147568013, "events": 223, "n": 1213, "avg_abs_delta": 2.4587, "max_abs_delta": 12.0},
    "warsaw": {"rate": 0.1731244847485573, "events": 210, "n": 1213, "avg_abs_delta": 2.3204, "max_abs_delta": 12.4},
    "taipei": {"rate": 0.169002473206925, "events": 205, "n": 1213, "avg_abs_delta": 2.1213, "max_abs_delta": 11.6},
    "houston": {"rate": 0.16488046166529266, "events": 200, "n": 1213, "avg_abs_delta": 2.1322, "max_abs_delta": 18.0},
    "moscow": {"rate": 0.1615828524319868, "events": 196, "n": 1213, "avg_abs_delta": 2.1591, "max_abs_delta": 14.1},
    "buenos-aires": {"rate": 0.15993404781533388, "events": 194, "n": 1213, "avg_abs_delta": 2.2650, "max_abs_delta": 13.3},
    "shanghai": {"rate": 0.1574608408903545, "events": 191, "n": 1213, "avg_abs_delta": 2.1915, "max_abs_delta": 13.4},
    "sao-paulo": {"rate": 0.1483924154987634, "events": 180, "n": 1213, "avg_abs_delta": 2.1215, "max_abs_delta": 14.5},
    "busan": {"rate": 0.14591920857378401, "events": 177, "n": 1213, "avg_abs_delta": 2.1209, "max_abs_delta": 11.7},
    "istanbul": {"rate": 0.14509480626545754, "events": 176, "n": 1213, "avg_abs_delta": 2.0142, "max_abs_delta": 12.9},
    "tokyo": {"rate": 0.14014839241549876, "events": 170, "n": 1213, "avg_abs_delta": 2.0333, "max_abs_delta": 10.2},
    "wuhan": {"rate": 0.14014839241549876, "events": 170, "n": 1213, "avg_abs_delta": 2.1200, "max_abs_delta": 18.2},
}
RED_TEMP_4C_CITIES = {city for city, stats in CITY_TEMP_4C_STATS.items() if stats["rate"] >= 0.24}
YELLOW_TEMP_4C_CITIES = {city for city, stats in CITY_TEMP_4C_STATS.items() if 0.14 <= stats["rate"] < 0.24}
EMPIRICAL_BAD_CITIES: set[str] = set()
EMPIRICAL_WATCH_CITIES = {"chengdu", "beijing", "guangzhou", "istanbul", "qingdao", "shenzhen", "toronto", "taipei", "seoul"}

WEATHER_CODE_NAMES = {
    0: "clear", 1: "mainly clear", 2: "partly cloudy", 3: "overcast", 45: "fog", 48: "rime fog",
    51: "light drizzle", 53: "drizzle", 55: "dense drizzle", 61: "slight rain", 63: "moderate rain", 65: "heavy rain",
    71: "slight snow", 73: "snow", 75: "heavy snow", 77: "snow grains", 80: "slight showers", 81: "showers", 82: "violent showers",
    85: "snow showers", 86: "heavy snow showers", 95: "thunderstorm", 96: "thunderstorm with hail", 99: "severe thunderstorm with hail",
}


def city_slug(raw: Any) -> str:
    return str(raw or "").strip().lower().replace("_", "-").replace(" ", "-")


def _safe_env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        y = float(x)
        return None if math.isnan(y) else y
    except Exception:
        return None


def percentile(xs: list[float], p: float) -> float | None:
    xs = sorted(x for x in xs if x is not None and not math.isnan(x))
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return xs[lo]
    return xs[lo] * (hi - k) + xs[hi] * (k - lo)


def _worst(a: str, b: str) -> str:
    order = {"GREEN": 0, "YELLOW": 1, "RED": 2}
    return a if order.get(a, 0) >= order.get(b, 0) else b


def c_to_f(temp_c: float | None) -> float | None:
    if temp_c is None:
        return None
    return temp_c * 9.0 / 5.0 + 32.0


def forecast_provider_for_resolution_source(source: str | None) -> str:
    """Choose the live forecast role for the market's resolution source.

    Wunderground is the settlement/history identity for most weather sniper
    markets. When a Weather Company key is configured, prefer the TWC/Weather.com
    ICAO forecast API because checked Wunderground station pages use the same TWC
    backend for ICAO current/forecast data. Open-Meteo is retained as fallback.
    """
    text = str(source or "").strip().lower()
    if "wunderground" in text or "weather underground" in text:
        return "twc_weather"
    if "open-meteo" in text or "open meteo" in text:
        return "open_meteo"
    return "twc_weather"


def forecast_role_metadata(primary_provider: str | None, resolution_source: str | None) -> dict[str, Any]:
    primary = str(primary_provider or "open_meteo").strip().lower()
    source = str(resolution_source or "")
    if primary == "station_proxy":
        return {
            "resolution_source": source,
            "forecast_provider_role": "station_proxy",
            "forecast_exact_resolution_service": False,
            "forecast_alignment": "resolution_station_latlon",
            "forecast_reason": "Wunderground station history has no stable public forecast API; using live station-coordinate forecast proxy stack",
        }
    if primary == "twc_weather":
        return {
            "resolution_source": source,
            "forecast_provider_role": "twc_weather_primary",
            "forecast_exact_resolution_service": "wunderground" in source.lower() or "weather underground" in source.lower(),
            "forecast_alignment": "resolution_station_icao",
            "forecast_reason": "using Weather Company/Weather.com ICAO forecast as primary for Wunderground-backed station markets; Open-Meteo fallback only",
        }
    exact = ("open-meteo" in source.lower() or "open meteo" in source.lower()) and primary == "open_meteo"
    return {
        "resolution_source": source,
        "forecast_provider_role": "direct_or_default",
        "forecast_exact_resolution_service": exact,
        "forecast_alignment": "resolution_station_latlon",
        "forecast_reason": "using configured/default forecast provider at resolution station coordinates; not exact settlement service unless the resolution source itself is Open-Meteo",
    }


def _indices(times: list[str], date: str, start_h: int | None = None, end_h: int | None = None) -> list[int]:
    out: list[int] = []
    for i, t in enumerate(times):
        s = str(t)
        if not s.startswith(date):
            continue
        if start_h is not None:
            try:
                h = int(s[11:13])
            except Exception:
                continue
            if h < start_h or h > (end_h if end_h is not None else h):
                continue
        out.append(i)
    return out


def _local_today(hourly: dict[str, Any]) -> str:
    dates = sorted(set(str(t)[:10] for t in (hourly.get("time") or [])))
    return dates[min(1, len(dates) - 1)] if dates else datetime.now().date().isoformat()


_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def event_target_date(event_slug: str | None) -> str | None:
    """Extract the local market date from a highest-temperature event slug.

    Examples:
      highest-temperature-in-jakarta-on-may-5-2026 -> 2026-05-05
      highest-temperature-in-nyc-on-september-12-2026 -> 2026-09-12
    """
    slug = str(event_slug or "").strip().lower()
    if not slug:
        return None
    m = re.search(r"-on-([a-z]+)-(\d{1,2})-(\d{4})(?:$|-)", slug)
    if not m:
        return None
    month = _MONTHS.get(m.group(1))
    if not month:
        return None
    try:
        return date(int(m.group(3)), month, int(m.group(2))).isoformat()
    except ValueError:
        return None


def _forecast_days_for_target(target_date: str | None, *, default: int = 2) -> int:
    """Return forecast_days covering target_date plus one past day for deltas."""
    if not target_date:
        return default
    try:
        target = date.fromisoformat(target_date)
    except ValueError:
        return default
    days_ahead = (target - datetime.now(timezone.utc).date()).days
    # Open-Meteo forecast_days counts from today. Keep enough range for the
    # connected market date, capped to the free forecast API's usual 16-day span.
    return max(default, min(16, days_ahead + 1))


def _circ_diff(a: float, b: float) -> float:
    d = abs(a - b) % 360
    return min(d, 360 - d)


def _max_wind_shift(vals: list[float]) -> float | None:
    vals = [v for v in vals if v is not None and not math.isnan(v)]
    if len(vals) < 2:
        return None
    return max(_circ_diff(a, b) for a in vals for b in vals)


async def get_station_info(client: httpx.AsyncClient, station_id: str) -> dict[str, Any] | None:
    if station_id == "HKO":
        return {"id": "HKO", "lat": 22.302, "lon": 114.174, "site": "Hong Kong Observatory"}
    r = await client.get("https://aviationweather.gov/api/data/stationinfo", params={"ids": station_id, "format": "json"})
    if r.status_code != 200:
        return None
    data = r.json() or []
    return data[0] if data else None


async def get_metars(client: httpx.AsyncClient, station_id: str, hours: int = 8) -> list[dict[str, Any]]:
    if station_id == "HKO":
        return []
    r = await client.get("https://aviationweather.gov/api/data/metar", params={"ids": station_id, "format": "json", "hours": hours})
    if r.status_code != 200:
        return []
    return sorted(r.json() or [], key=lambda m: m.get("obsTime") or 0, reverse=True)


async def _open_meteo_json(client: httpx.AsyncClient, url: str, params: dict[str, Any]) -> dict[str, Any] | None:
    """Fetch Open-Meteo JSON with a small 429/backoff guard."""
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            r = await client.get(url, params=params)
            if r.status_code == 429 and attempt < 2:
                retry_after = safe_float(r.headers.get("retry-after")) or (2.0 * (attempt + 1))
                await asyncio.sleep(min(8.0, max(1.0, retry_after)))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_exc = exc
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise last_exc
    return None


async def get_open_meteo(client: httpx.AsyncClient, lat: float, lon: float, *, target_date: str | None = None) -> dict[str, Any]:
    hourly = "temperature_2m,precipitation_probability,precipitation,rain,showers,snowfall,weather_code,cloud_cover,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m,cape"
    forecast_days = _forecast_days_for_target(target_date)
    params = {"latitude": lat, "longitude": lon, "hourly": hourly, "daily": "temperature_2m_max,precipitation_sum,rain_sum,showers_sum,snowfall_sum,wind_gusts_10m_max", "timezone": "auto", "forecast_days": forecast_days, "past_days": 1, "wind_speed_unit": "kmh"}
    data = await _open_meteo_json(client, "https://api.open-meteo.com/v1/forecast", params)
    return data or {}


async def get_open_meteo_ensemble(client: httpx.AsyncClient, lat: float, lon: float, *, target_date: str | None = None) -> dict[str, Any] | None:
    try:
        return await _open_meteo_json(client, "https://ensemble-api.open-meteo.com/v1/ensemble", {"latitude": lat, "longitude": lon, "hourly": "temperature_2m", "timezone": "auto", "forecast_days": _forecast_days_for_target(target_date), "past_days": 1})
    except Exception:
        return None


async def get_prev_run(client: httpx.AsyncClient, lat: float, lon: float, *, target_date: str | None = None) -> dict[str, Any] | None:
    try:
        return await _open_meteo_json(client, "https://previous-runs-api.open-meteo.com/v1/forecast", {"latitude": lat, "longitude": lon, "daily": "temperature_2m_max", "timezone": "auto", "forecast_days": _forecast_days_for_target(target_date), "past_days": 1, "forecast_hours": 48})
    except Exception:
        return None


def _wttr_hour_to_iso(day: str, hour_token: Any) -> str:
    try:
        hour = int(str(hour_token or "0")) // 100
    except Exception:
        hour = 0
    return f"{day}T{max(0, min(23, hour)):02d}:00"


def _wttr_to_open_meteo_shape(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize wttr.in JSON into the subset of Open-Meteo shape we evaluate."""
    weather = data.get("weather") or []
    hourly: dict[str, list[Any]] = {
        "time": [],
        "temperature_2m": [],
        "precipitation_probability": [],
        "precipitation": [],
        "rain": [],
        "showers": [],
        "snowfall": [],
        "weather_code": [],
        "cloud_cover": [],
        "surface_pressure": [],
        "wind_speed_10m": [],
        "wind_direction_10m": [],
        "wind_gusts_10m": [],
        "cape": [],
    }
    daily = {"time": [], "temperature_2m_max": [], "precipitation_sum": [], "rain_sum": [], "showers_sum": [], "snowfall_sum": [], "wind_gusts_10m_max": []}
    for day in weather:
        d = str(day.get("date") or "")
        if not d:
            continue
        daily["time"].append(d)
        daily["temperature_2m_max"].append(safe_float(day.get("maxtempC")))
        daily["snowfall_sum"].append(safe_float(day.get("totalSnow_cm")) or 0.0)
        day_precip = 0.0
        day_gusts: list[float] = []
        for h in day.get("hourly") or []:
            hourly["time"].append(_wttr_hour_to_iso(d, h.get("time")))
            hourly["temperature_2m"].append(safe_float(h.get("tempC")))
            precip = safe_float(h.get("precipMM")) or 0.0
            hourly["precipitation"].append(precip)
            # wttr does not separate showers; use precipMM as rain unless chance of snow dominates.
            chance_snow = safe_float(h.get("chanceofsnow")) or 0.0
            snow_cm = precip / 10.0 if chance_snow >= 50 else 0.0
            rain_mm = 0.0 if snow_cm else precip
            hourly["rain"].append(rain_mm)
            hourly["showers"].append(0.0)
            hourly["snowfall"].append(snow_cm)
            hourly["precipitation_probability"].append(safe_float(h.get("chanceofrain")))
            hourly["weather_code"].append(safe_float(h.get("weatherCode")))
            hourly["cloud_cover"].append(safe_float(h.get("cloudcover")))
            hourly["surface_pressure"].append(safe_float(h.get("pressure")))
            hourly["wind_speed_10m"].append(safe_float(h.get("windspeedKmph")))
            hourly["wind_direction_10m"].append(safe_float(h.get("winddirDegree")))
            gust = safe_float(h.get("WindGustKmph"))
            hourly["wind_gusts_10m"].append(gust)
            hourly["cape"].append(None)
            day_precip += precip
            if gust is not None:
                day_gusts.append(gust)
        daily["precipitation_sum"].append(day_precip)
        daily["rain_sum"].append(day_precip)
        daily["showers_sum"].append(0.0)
        daily["wind_gusts_10m_max"].append(max(day_gusts) if day_gusts else None)
    return {"hourly": hourly, "daily": daily, "_provider": "wttr"}


def _weatherapi_to_open_meteo_shape(data: dict[str, Any]) -> dict[str, Any]:
    forecast_days = ((data.get("forecast") or {}).get("forecastday") or [])
    hourly: dict[str, list[Any]] = {
        "time": [], "temperature_2m": [], "precipitation_probability": [], "precipitation": [],
        "rain": [], "showers": [], "snowfall": [], "weather_code": [], "cloud_cover": [],
        "surface_pressure": [], "wind_speed_10m": [], "wind_direction_10m": [], "wind_gusts_10m": [], "cape": [],
    }
    daily = {"time": [], "temperature_2m_max": [], "precipitation_sum": [], "rain_sum": [], "showers_sum": [], "snowfall_sum": [], "wind_gusts_10m_max": []}
    for day in forecast_days:
        d = str(day.get("date") or "")
        dd = day.get("day") or {}
        if not d:
            continue
        daily["time"].append(d)
        daily["temperature_2m_max"].append(safe_float(dd.get("maxtemp_c")))
        total_precip = safe_float(dd.get("totalprecip_mm")) or 0.0
        total_snow_cm = safe_float(dd.get("totalsnow_cm")) or 0.0
        daily["precipitation_sum"].append(total_precip)
        daily["rain_sum"].append(max(0.0, total_precip - total_snow_cm * 10.0))
        daily["showers_sum"].append(0.0)
        daily["snowfall_sum"].append(total_snow_cm)
        gusts: list[float] = []
        for h in day.get("hour") or []:
            hourly["time"].append(str(h.get("time") or "").replace(" ", "T"))
            hourly["temperature_2m"].append(safe_float(h.get("temp_c")))
            precip = safe_float(h.get("precip_mm")) or 0.0
            snow_cm = safe_float(h.get("snow_cm")) or 0.0
            hourly["precipitation"].append(precip)
            hourly["rain"].append(max(0.0, precip - snow_cm * 10.0))
            hourly["showers"].append(0.0)
            hourly["snowfall"].append(snow_cm)
            hourly["precipitation_probability"].append(safe_float(h.get("chance_of_rain")))
            hourly["weather_code"].append(safe_float((h.get("condition") or {}).get("code")))
            hourly["cloud_cover"].append(safe_float(h.get("cloud")))
            hourly["surface_pressure"].append(safe_float(h.get("pressure_mb")))
            hourly["wind_speed_10m"].append(safe_float(h.get("wind_kph")))
            hourly["wind_direction_10m"].append(safe_float(h.get("wind_degree")))
            gust = safe_float(h.get("gust_kph"))
            hourly["wind_gusts_10m"].append(gust)
            hourly["cape"].append(None)
            if gust is not None:
                gusts.append(gust)
        daily["wind_gusts_10m_max"].append(max(gusts) if gusts else None)
    return {"hourly": hourly, "daily": daily, "_provider": "weatherapi"}


async def get_weatherapi_forecast(client: httpx.AsyncClient, lat: float, lon: float, *, target_date: str | None = None) -> dict[str, Any] | None:
    key = os.getenv("WEATHERAPI_KEY") or os.getenv("WEATHER_API_KEY")
    if not key:
        return None
    days = _forecast_days_for_target(target_date)
    r = await client.get("https://api.weatherapi.com/v1/forecast.json", params={"key": key, "q": f"{lat},{lon}", "days": min(14, max(1, days)), "aqi": "no", "alerts": "no"})
    r.raise_for_status()
    return _weatherapi_to_open_meteo_shape(r.json() or {})


def _twc_key() -> str | None:
    return (
        os.getenv("TWC_API_KEY")
        or os.getenv("WEATHER_COMPANY_API_KEY")
        or os.getenv("WEATHERCOMPANY_API_KEY")
        or os.getenv("WEATHER_COM_API_KEY")
    )


def _twc_to_open_meteo_shape(hourly_data: dict[str, Any], daily_data: dict[str, Any]) -> dict[str, Any]:
    """Normalize TWC/Weather.com forecast JSON into the evaluator shape."""
    hourly: dict[str, list[Any]] = {
        "time": [], "temperature_2m": [], "precipitation_probability": [], "precipitation": [],
        "rain": [], "showers": [], "snowfall": [], "weather_code": [], "cloud_cover": [],
        "surface_pressure": [], "wind_speed_10m": [], "wind_direction_10m": [], "wind_gusts_10m": [], "cape": [],
    }
    times = hourly_data.get("validTimeLocal") or []
    temps = hourly_data.get("temperature") or []
    qpf = hourly_data.get("qpf") or []
    qpf_rain = hourly_data.get("qpfRain") or []
    qpf_snow = hourly_data.get("qpfSnow") or []
    precip_chance = hourly_data.get("precipChance") or []
    precip_type = hourly_data.get("precipType") or []
    cloud_cover = hourly_data.get("cloudCover") or []
    pressure = hourly_data.get("pressureMeanSeaLevel") or []
    wind_speed = hourly_data.get("windSpeed") or []
    wind_dir = hourly_data.get("windDirection") or []
    wind_gust = hourly_data.get("windGust") or []
    for i, raw_time in enumerate(times):
        t = str(raw_time or "")
        if not t:
            continue
        hourly["time"].append(t[:16])
        hourly["temperature_2m"].append(safe_float(temps[i] if i < len(temps) else None))
        precip = safe_float(qpf[i] if i < len(qpf) else None) or 0.0
        rain = safe_float(qpf_rain[i] if i < len(qpf_rain) else None)
        snow = safe_float(qpf_snow[i] if i < len(qpf_snow) else None)
        ptype = str(precip_type[i] if i < len(precip_type) else "").lower()
        if rain is None:
            rain = 0.0 if ptype == "snow" else precip
        if snow is None:
            snow = precip if ptype == "snow" else 0.0
        hourly["precipitation_probability"].append(safe_float(precip_chance[i] if i < len(precip_chance) else None))
        hourly["precipitation"].append(precip)
        hourly["rain"].append(rain)
        hourly["showers"].append(0.0)
        hourly["snowfall"].append(snow)
        hourly["weather_code"].append(None)
        hourly["cloud_cover"].append(safe_float(cloud_cover[i] if i < len(cloud_cover) else None))
        hourly["surface_pressure"].append(safe_float(pressure[i] if i < len(pressure) else None))
        hourly["wind_speed_10m"].append(safe_float(wind_speed[i] if i < len(wind_speed) else None))
        hourly["wind_direction_10m"].append(safe_float(wind_dir[i] if i < len(wind_dir) else None))
        hourly["wind_gusts_10m"].append(safe_float(wind_gust[i] if i < len(wind_gust) else None))
        hourly["cape"].append(None)

    daily = {"time": [], "temperature_2m_max": [], "precipitation_sum": [], "rain_sum": [], "showers_sum": [], "snowfall_sum": [], "wind_gusts_10m_max": []}
    daily_times = daily_data.get("validTimeLocal") or []
    daily_highs = daily_data.get("calendarDayTemperatureMax") or []
    daily_qpf = daily_data.get("qpf") or []
    daily_rain = daily_data.get("qpfRain") or []
    daily_snow = daily_data.get("qpfSnow") or []
    daypart = (daily_data.get("daypart") or [{}])[0] if isinstance(daily_data.get("daypart"), list) else {}
    daypart_wind = daypart.get("windSpeed") or []
    for i, raw_time in enumerate(daily_times):
        d = str(raw_time or "")[:10]
        if not d:
            continue
        daily["time"].append(d)
        daily["temperature_2m_max"].append(safe_float(daily_highs[i] if i < len(daily_highs) else None))
        precip = safe_float(daily_qpf[i] if i < len(daily_qpf) else None) or 0.0
        rain = safe_float(daily_rain[i] if i < len(daily_rain) else None)
        snow = safe_float(daily_snow[i] if i < len(daily_snow) else None)
        daily["precipitation_sum"].append(precip)
        daily["rain_sum"].append(precip if rain is None else rain)
        daily["showers_sum"].append(0.0)
        daily["snowfall_sum"].append(0.0 if snow is None else snow)
        pair = [safe_float(daypart_wind[j]) for j in (2 * i, 2 * i + 1) if j < len(daypart_wind)]
        daily["wind_gusts_10m_max"].append(max([x for x in pair if x is not None], default=None))
    return {"hourly": hourly, "daily": daily, "_provider": "twc_weather", "_twc_daily_highs_are_integer_c": True, "_prefer_daily_high_for_boundary": True}


async def get_twc_weather_forecast(client: httpx.AsyncClient, lat: float, lon: float, *, target_date: str | None = None, station_id: str | None = None) -> dict[str, Any] | None:
    key = _twc_key()
    if not key:
        return None
    params: dict[str, Any] = {"format": "json", "units": "m", "language": "en-US", "apiKey": key}
    if station_id and station_id != "HKO":
        params["icaoCode"] = station_id
    else:
        params["geocode"] = f"{lat:.4f},{lon:.4f}"
    days = _forecast_days_for_target(target_date)
    hourly_product = "hourly/15day" if days > 2 else "hourly/2day"
    hourly_r = await client.get(f"https://api.weather.com/v3/wx/forecast/{hourly_product}", params=params)
    hourly_r.raise_for_status()
    daily_r = await client.get("https://api.weather.com/v3/wx/forecast/daily/5day", params=params)
    daily_r.raise_for_status()
    return _twc_to_open_meteo_shape(hourly_r.json() or {}, daily_r.json() or {})


def _visual_crossing_to_open_meteo_shape(data: dict[str, Any]) -> dict[str, Any]:
    days = data.get("days") or []
    hourly: dict[str, list[Any]] = {
        "time": [], "temperature_2m": [], "precipitation_probability": [], "precipitation": [],
        "rain": [], "showers": [], "snowfall": [], "weather_code": [], "cloud_cover": [],
        "surface_pressure": [], "wind_speed_10m": [], "wind_direction_10m": [], "wind_gusts_10m": [], "cape": [],
    }
    daily = {"time": [], "temperature_2m_max": [], "precipitation_sum": [], "rain_sum": [], "showers_sum": [], "snowfall_sum": [], "wind_gusts_10m_max": []}
    for day in days:
        d = str(day.get("datetime") or "")
        if not d:
            continue
        daily["time"].append(d)
        daily["temperature_2m_max"].append(safe_float(day.get("tempmax")))
        precip = safe_float(day.get("precip")) or 0.0
        snow = safe_float(day.get("snow")) or 0.0
        daily["precipitation_sum"].append(precip)
        daily["rain_sum"].append(max(0.0, precip - snow * 10.0))
        daily["showers_sum"].append(0.0)
        daily["snowfall_sum"].append(snow)
        daily["wind_gusts_10m_max"].append(safe_float(day.get("windgust")))
        for h in day.get("hours") or []:
            hourly["time"].append(f"{d}T{str(h.get('datetime') or '00:00:00')[:5]}")
            hourly["temperature_2m"].append(safe_float(h.get("temp")))
            hprecip = safe_float(h.get("precip")) or 0.0
            hsnow = safe_float(h.get("snow")) or 0.0
            hourly["precipitation"].append(hprecip)
            hourly["rain"].append(max(0.0, hprecip - hsnow * 10.0))
            hourly["showers"].append(0.0)
            hourly["snowfall"].append(hsnow)
            hourly["precipitation_probability"].append(safe_float(h.get("precipprob")))
            hourly["weather_code"].append(None)
            hourly["cloud_cover"].append(safe_float(h.get("cloudcover")))
            hourly["surface_pressure"].append(safe_float(h.get("pressure")))
            hourly["wind_speed_10m"].append(safe_float(h.get("windspeed")))
            hourly["wind_direction_10m"].append(safe_float(h.get("winddir")))
            hourly["wind_gusts_10m"].append(safe_float(h.get("windgust")))
            hourly["cape"].append(None)
    return {"hourly": hourly, "daily": daily, "_provider": "visual_crossing"}


async def get_visual_crossing_forecast(client: httpx.AsyncClient, lat: float, lon: float, *, target_date: str | None = None) -> dict[str, Any] | None:
    key = os.getenv("VISUAL_CROSSING_API_KEY") or os.getenv("VISUALCROSSING_API_KEY")
    if not key:
        return None
    try:
        start = datetime.now(timezone.utc).date().isoformat()
        end = target_date or start
        if date.fromisoformat(end) < date.fromisoformat(start):
            end = start
    except ValueError:
        start = end = datetime.now(timezone.utc).date().isoformat()
    r = await client.get(f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}/{start}/{end}", params={"unitGroup": "metric", "include": "days,hours", "key": key, "contentType": "json"})
    r.raise_for_status()
    return _visual_crossing_to_open_meteo_shape(r.json() or {})


def _met_no_to_open_meteo_shape(data: dict[str, Any], lon: float) -> dict[str, Any]:
    """Normalize MET Norway Locationforecast to evaluator shape.

    MET Norway is an official meteorological institute source. It returns UTC
    timestamps; without a city timezone database in this module we convert to an
    approximate civil local hour from longitude. This provider is still preferred
    over anonymous aggregator JSON, but key-backed Visual Crossing/WeatherAPI are
    preferred when configured.
    """
    offset_hours = int(round(float(lon) / 15.0))
    hourly: dict[str, list[Any]] = {
        "time": [], "temperature_2m": [], "precipitation_probability": [], "precipitation": [],
        "rain": [], "showers": [], "snowfall": [], "weather_code": [], "cloud_cover": [],
        "surface_pressure": [], "wind_speed_10m": [], "wind_direction_10m": [], "wind_gusts_10m": [], "cape": [],
    }
    by_day: dict[str, dict[str, Any]] = {}
    for item in (((data.get("properties") or {}).get("timeseries")) or []):
        raw_time = str(item.get("time") or "")
        try:
            dt = datetime.fromisoformat(raw_time.replace("Z", "+00:00")).astimezone(timezone.utc)
            local_dt = dt.fromtimestamp(dt.timestamp() + offset_hours * 3600, tz=timezone.utc)
            local_iso = local_dt.strftime("%Y-%m-%dT%H:%M")
        except Exception:
            continue
        details = (((item.get("data") or {}).get("instant") or {}).get("details")) or {}
        next1 = ((((item.get("data") or {}).get("next_1_hours") or {}).get("details")) or {})
        precip = safe_float(next1.get("precipitation_amount")) or 0.0
        temp = safe_float(details.get("air_temperature"))
        wind_ms = safe_float(details.get("wind_speed"))
        gust_ms = safe_float(details.get("wind_speed_of_gust"))
        hourly["time"].append(local_iso)
        hourly["temperature_2m"].append(temp)
        hourly["precipitation_probability"].append(None)
        hourly["precipitation"].append(precip)
        hourly["rain"].append(precip)
        hourly["showers"].append(0.0)
        hourly["snowfall"].append(0.0)
        hourly["weather_code"].append(None)
        hourly["cloud_cover"].append(safe_float(details.get("cloud_area_fraction")))
        hourly["surface_pressure"].append(safe_float(details.get("air_pressure_at_sea_level")))
        hourly["wind_speed_10m"].append(wind_ms * 3.6 if wind_ms is not None else None)
        hourly["wind_direction_10m"].append(safe_float(details.get("wind_from_direction")))
        hourly["wind_gusts_10m"].append(gust_ms * 3.6 if gust_ms is not None else None)
        hourly["cape"].append(None)
        day = local_iso[:10]
        bucket = by_day.setdefault(day, {"temps": [], "precip": 0.0, "gusts": []})
        if temp is not None:
            bucket["temps"].append(temp)
        bucket["precip"] += precip
        if gust_ms is not None:
            bucket["gusts"].append(gust_ms * 3.6)
    daily = {"time": [], "temperature_2m_max": [], "precipitation_sum": [], "rain_sum": [], "showers_sum": [], "snowfall_sum": [], "wind_gusts_10m_max": []}
    for day in sorted(by_day):
        bucket = by_day[day]
        daily["time"].append(day)
        daily["temperature_2m_max"].append(max(bucket["temps"]) if bucket["temps"] else None)
        daily["precipitation_sum"].append(bucket["precip"])
        daily["rain_sum"].append(bucket["precip"])
        daily["showers_sum"].append(0.0)
        daily["snowfall_sum"].append(0.0)
        daily["wind_gusts_10m_max"].append(max(bucket["gusts"]) if bucket["gusts"] else None)
    return {"hourly": hourly, "daily": daily, "_provider": "met_no"}


async def get_met_no_forecast(client: httpx.AsyncClient, lat: float, lon: float, *, target_date: str | None = None) -> dict[str, Any] | None:
    r = await client.get("https://api.met.no/weatherapi/locationforecast/2.0/complete", params={"lat": f"{lat:.4f}", "lon": f"{lon:.4f}"})
    r.raise_for_status()
    return _met_no_to_open_meteo_shape(r.json() or {}, lon)


async def get_wttr_forecast(client: httpx.AsyncClient, lat: float, lon: float) -> dict[str, Any]:
    r = await client.get(f"https://wttr.in/{lat:.4f},{lon:.4f}", params={"format": "j1"})
    r.raise_for_status()
    return _wttr_to_open_meteo_shape(r.json() or {})


async def get_weathercom_web_diagnostic(client: httpx.AsyncClient, lat: float, lon: float, *, station_id: str | None = None) -> dict[str, Any]:
    """Best-effort Weather.com/Wunderground web diagnostic, not a trading feed.

    Wunderground no longer offers normal public API keys. This function only
    checks whether the public Weather.com web page is reachable for the station
    coordinates and extracts coarse evidence if embedded page data is visible. It
    must not be used as the authoritative forecast source for order decisions.
    """
    url = f"https://weather.com/weather/tenday/l/{lat:.4f},{lon:.4f}"
    try:
        r = await client.get(url, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0 polybot-weather-diagnostic/1.0"})
        text = r.text or ""
        ok = r.status_code == 200 and ("DailyForecast" in text or "temperature" in text.lower() or "Temperature" in text)
        highs: list[float] = []
        for m in re.finditer(r'"temperatureMax"\s*:\s*(-?\d+(?:\.\d+)?)', text):
            val = safe_float(m.group(1))
            if val is not None:
                highs.append(val)
            if len(highs) >= 10:
                break
        return {
            "weathercom_web_reachable": r.status_code == 200,
            "weathercom_web_status": r.status_code,
            "weathercom_web_url": str(r.url),
            "weathercom_web_station_id": station_id,
            "weathercom_web_has_embedded_forecast": bool(highs),
            "weathercom_web_temperature_max_values": highs[:10],
            "weathercom_web_usable_for_trading": False,
            "weathercom_web_note": "diagnostic only; public page is not a stable forecast API",
            "weathercom_web_ok": ok,
        }
    except Exception as exc:
        return {
            "weathercom_web_reachable": False,
            "weathercom_web_status": None,
            "weathercom_web_station_id": station_id,
            "weathercom_web_has_embedded_forecast": False,
            "weathercom_web_temperature_max_values": [],
            "weathercom_web_usable_for_trading": False,
            "weathercom_web_note": f"diagnostic failed: {type(exc).__name__}: {str(exc)[:160]}",
            "weathercom_web_ok": False,
        }


async def get_wunderground_forecast(client: httpx.AsyncClient, lat: float, lon: float, *, target_date: str | None = None, station_id: str | None = None) -> dict[str, Any] | None:
    """Deprecated compatibility shim.

    Wunderground station history is a resolution source, not a usable public
    forecast API. Return None so callers fall back to the station-proxy stack.
    """
    return None


async def get_forecast_with_fallback(client: httpx.AsyncClient, lat: float, lon: float, *, target_date: str | None = None, primary_provider: str | None = None, station_id: str | None = None, resolution_source: str | None = None) -> tuple[dict[str, Any], str, list[str]]:
    errors: list[str] = []
    primary_provider = str(primary_provider or "open_meteo").strip().lower()
    role_meta = forecast_role_metadata(primary_provider, resolution_source)

    async def try_provider(provider_name: str) -> dict[str, Any] | None:
        if provider_name == "open_meteo":
            forecast = await get_open_meteo(client, lat, lon, target_date=target_date)
            forecast["_provider"] = "open_meteo"
            return forecast
        if provider_name == "station_proxy":
            forecast = await get_open_meteo(client, lat, lon, target_date=target_date)
            forecast["_provider"] = "open_meteo"
            forecast["_provider_role"] = "station_proxy"
            return forecast
        if provider_name == "twc_weather":
            return await get_twc_weather_forecast(client, lat, lon, target_date=target_date, station_id=station_id)
        if provider_name == "wunderground":
            return await get_wunderground_forecast(client, lat, lon, target_date=target_date, station_id=station_id)
        if provider_name == "visual_crossing":
            return await get_visual_crossing_forecast(client, lat, lon, target_date=target_date)
        if provider_name == "weatherapi":
            return await get_weatherapi_forecast(client, lat, lon, target_date=target_date)
        if provider_name == "met_no":
            return await get_met_no_forecast(client, lat, lon, target_date=target_date)
        raise ValueError(f"unsupported forecast provider {provider_name}")

    provider_order = [primary_provider]
    if primary_provider == "twc_weather":
        provider_order = ["twc_weather", "open_meteo"]
    if primary_provider == "station_proxy":
        provider_order = ["station_proxy", "met_no", "visual_crossing", "weatherapi"]
    fallback_names = ("open_meteo",) if primary_provider == "twc_weather" else ("open_meteo", "met_no", "visual_crossing", "weatherapi")
    for name in fallback_names:
        if name not in provider_order:
            provider_order.append(name)

    diagnostic: dict[str, Any] | None = None
    if _safe_env_bool("POLYBOT_WEATHERCOM_WEB_DIAGNOSTIC", False):
        diagnostic = await get_weathercom_web_diagnostic(client, lat, lon, station_id=station_id)

    for provider_name in provider_order:
        try:
            forecast = await try_provider(provider_name)
            if forecast:
                forecast["_provider_errors"] = errors
                forecast["_provider_fallback_from"] = primary_provider if provider_name != primary_provider else None
                forecast["_resolution_station_id"] = station_id
                forecast.update({f"_{k}": v for k, v in role_meta.items()})
                if diagnostic is not None:
                    forecast["_weathercom_web_diagnostic"] = diagnostic
                return forecast, str(forecast.get("_provider") or provider_name), errors
            errors.append(f"{provider_name}=not_available")
        except Exception as exc:
            errors.append(f"{provider_name}={type(exc).__name__}: {exc}")

    if _safe_env_bool("POLYBOT_ALLOW_WTTR_FALLBACK", False):
        try:
            forecast = await get_wttr_forecast(client, lat, lon)
            forecast["_provider_errors"] = errors
            forecast["_provider_fallback_from"] = primary_provider
            forecast["_resolution_station_id"] = station_id
            forecast.update({f"_{k}": v for k, v in role_meta.items()})
            if diagnostic is not None:
                forecast["_weathercom_web_diagnostic"] = diagnostic
            return forecast, "wttr", errors
        except Exception as exc:
            errors.append(f"wttr={type(exc).__name__}: {exc}")

    raise RuntimeError("; ".join(errors))

def evaluate_weather_safety(spec: StationSpec, station_info: dict[str, Any] | None, metars: list[dict[str, Any]], forecast: dict[str, Any] | None, ensemble: dict[str, Any] | None, prev_run: dict[str, Any] | None, event_slug: str | None = None, target_date: str | None = None, error: str | None = None) -> dict[str, Any]:
    """Empirical weather chart-risk gate.

    Gate rules:
    - RED: snow/winter code; rain + empirical bad city; or watch city +
      snow/heavy rain/heavy wind during the 10:00-16:00 local peak window.
    - YELLOW: empirical watch city; or non-watch city + heavy rain/heavy wind
      during the 10:00-16:00 local peak window.
    - GREEN: no snow, no watch-city flag, and no heavy rain/wind signal.

    Ordinary rain remains a dashboard context signal but is not a standalone
    YELLOW sizing trigger.

    The legacy 51-city >=4C daily-high calibration remains in metrics/warnings for
    dashboard context only; it does not change gate or sizing by itself.
    """
    gate = "GREEN"
    reasons: list[str] = []
    warnings: list[str] = []
    target_date = target_date or event_target_date(event_slug)
    metrics: dict[str, Any] = {
        "filter_model": "empirical_weather_chart_risk_v4_event_date",
        "baseline_4c_change_rate": round(TEMP_4C_CHANGE_BASELINE_RATE, 4),
        "empirical_bad_city": spec.city_slug in EMPIRICAL_BAD_CITIES,
        "empirical_watch_city": spec.city_slug in EMPIRICAL_WATCH_CITIES,
        "static_city_risk_informational_only": True,
        "event_target_date": target_date,
        "event_slug": event_slug,
    }

    def flag(level: str, reason: str) -> None:
        nonlocal gate
        gate = _worst(gate, level)
        reasons.append(reason)

    city_stats = CITY_TEMP_4C_STATS.get(spec.city_slug)
    if city_stats:
        rate = float(city_stats["rate"])
        rr = rate / TEMP_4C_CHANGE_BASELINE_RATE if TEMP_4C_CHANGE_BASELINE_RATE else None
        metrics.update({
            "historical_4c_change_rate": round(rate, 4),
            "historical_4c_change_events": int(city_stats["events"]),
            "historical_4c_change_days": int(city_stats["n"]),
            "historical_4c_change_rr": round(rr, 2) if rr else None,
            "historical_avg_abs_delta_c": city_stats.get("avg_abs_delta"),
            "historical_max_abs_delta_c": city_stats.get("max_abs_delta"),
        })
        warnings.append(
            f"static city context only: historical >=4°C high-change rate {rate*100:.1f}% "
            f"({int(city_stats['events'])}/{int(city_stats['n'])}), {rr:.2f}x 51-city baseline; not used for sizing by itself"
        )
    else:
        metrics["historical_4c_change_rate"] = None

    watch_city = spec.city_slug in EMPIRICAL_WATCH_CITIES

    if error:
        warnings.append(f"data_error={error}; live rain/snow unavailable")
        if watch_city:
            flag("YELLOW", "empirical watch city: allow one normal order, disable same-market re-buy ladder")
        expected = round(float(city_stats.get("avg_abs_delta") or 0.0), 2) if city_stats else None
        metrics["expected_temp_fluctuation_c"] = expected
        size_multiplier = 0.0 if gate == "RED" else 1.0
        reason_text = "; ".join((reasons if reasons else warnings)[:4]) or "GREEN: no snow/rain/watch/bad-city signal"
        return {"gate": gate, "reason": reason_text, "reasons": reasons, "warnings": warnings, "metrics": metrics, "weather_codes": [], "weather_code_names": [], "expected_temp_fluctuation_c": expected, "size_multiplier": size_multiplier, "event_slug": event_slug}

    latest = metars[0] if metars else None
    obs_temp_c = safe_float(latest.get("temp")) if latest else None
    if latest:
        obs_ts = safe_float(latest.get("obsTime"))
        obs_age_min = (time.time() - obs_ts) / 60 if obs_ts else None
        metrics["obs_age_min"] = round(obs_age_min, 1) if obs_age_min is not None else None
        metrics["obs_temp_c"] = obs_temp_c
        if obs_age_min is None or obs_temp_c is None:
            warnings.append("latest station observation missing temp/time")
        elif obs_age_min > 180:
            warnings.append(f"station observation stale {obs_age_min:.0f}m >180m; using forecast risk signals")
    elif spec.station_id == "HKO":
        warnings.append("HKO current exact-station obs not fetched; using forecast risk signals")
    else:
        warnings.append("no recent METAR/station observation; using forecast risk signals")

    if forecast:
        metrics["forecast_provider_used"] = forecast.get("_provider") or "unknown"
        metrics["forecast_provider_errors"] = forecast.get("_provider_errors") or []
        metrics["forecast_provider_fallback_from"] = forecast.get("_provider_fallback_from")
        metrics["resolution_station_id"] = forecast.get("_resolution_station_id")
        for key in (
            "resolution_source",
            "forecast_provider_role",
            "forecast_exact_resolution_service",
            "forecast_alignment",
            "forecast_reason",
        ):
            if f"_{key}" in forecast:
                metrics[key] = forecast.get(f"_{key}")
        if forecast.get("_weathercom_web_diagnostic"):
            metrics["weathercom_web_diagnostic"] = forecast.get("_weathercom_web_diagnostic")
    if not forecast:
        warnings.append("forecast unavailable; live rain/snow unavailable")
        if watch_city:
            flag("YELLOW", "empirical watch city: allow one normal order, disable same-market re-buy ladder")
        expected = round(float(city_stats.get("avg_abs_delta") or 0.0), 2) if city_stats else None
        metrics["expected_temp_fluctuation_c"] = expected
        size_multiplier = 0.0 if gate == "RED" else 1.0
        reason_text = "; ".join((reasons if reasons else warnings)[:4]) or "GREEN: no snow/rain/watch/bad-city signal"
        return {"gate": gate, "reason": reason_text, "reasons": reasons, "warnings": warnings, "metrics": metrics, "weather_codes": [], "weather_code_names": [], "expected_temp_fluctuation_c": expected, "size_multiplier": size_multiplier, "event_slug": event_slug}

    hourly = forecast.get("hourly") or {}
    times = hourly.get("time") or []
    available_dates = sorted(set(str(t)[:10] for t in times))
    today = target_date if target_date in available_dates else _local_today(hourly)
    if target_date and target_date not in available_dates:
        warnings.append(f"target market date {target_date} not in forecast; using {today}")
    day_ix = _indices(times, today)
    # Rain and wind risk gates must be based only on the target market date's
    # local 10:00-16:00 peak-temperature window. Do not fall back to full-day
    # data; if the forecast has no peak-window hourly rows, rain/wind stay non-triggering.
    peak_ix = _indices(times, today, 10, 16)
    metrics["local_date"] = today

    def vals(name: str, idxs: list[int]) -> list[float]:
        arr = hourly.get(name) or []
        return [v for v in (safe_float(arr[i]) for i in idxs if i < len(arr)) if v is not None]

    prev_daily_high = None
    daily = forecast.get("daily") or {}
    daily_times = [str(t) for t in (daily.get("time") or [])]
    daily_highs = daily.get("temperature_2m_max") or []
    daily_forecast_high = None
    try:
        ti = daily_times.index(today)
        if ti < len(daily_highs):
            daily_forecast_high = safe_float(daily_highs[ti])
        if ti > 0 and ti - 1 < len(daily_highs):
            prev_daily_high = safe_float(daily_highs[ti - 1])
    except Exception:
        prev_daily_high = None
    temps = vals("temperature_2m", day_ix)
    prefer_daily_high = bool(forecast.get("_prefer_daily_high_for_boundary"))
    forecast_high = daily_forecast_high if prefer_daily_high and daily_forecast_high is not None else (max(temps) if temps else daily_forecast_high)
    forecast_high_f = c_to_f(forecast_high)
    metrics["forecast_high_c"] = round(forecast_high, 2) if forecast_high is not None else None
    metrics["forecast_high_f"] = round(forecast_high_f, 2) if forecast_high_f is not None else None
    metrics["forecast_high_source"] = "daily" if prefer_daily_high and daily_forecast_high is not None else ("hourly" if temps else ("daily" if daily_forecast_high is not None else None))
    if forecast.get("_twc_daily_highs_are_integer_c"):
        metrics["forecast_daily_highs_are_integer_c"] = True
    day_to_day_delta = abs(forecast_high - prev_daily_high) if forecast_high is not None and prev_daily_high is not None else None
    if day_to_day_delta is not None:
        metrics["forecast_vs_yesterday_high_delta_c"] = round(day_to_day_delta, 2)

    max_precip = max(vals("precipitation", peak_ix), default=None)
    max_rain = max(vals("rain", peak_ix), default=None)
    max_showers = max(vals("showers", peak_ix), default=None)
    max_snow = max(vals("snowfall", peak_ix), default=None)
    max_gust = max(vals("wind_gusts_10m", peak_ix), default=None)
    max_wind = max(vals("wind_speed_10m", peak_ix), default=None)
    codes = {int(x) for x in vals("weather_code", peak_ix)}
    code_names = [WEATHER_CODE_NAMES.get(c, str(c)) for c in sorted(codes)]

    daily_precip = None
    daily_snow = None
    try:
        for t, p, s in zip(daily.get("time") or [], daily.get("precipitation_sum") or [], daily.get("snowfall_sum") or []):
            if str(t) == today:
                daily_precip, daily_snow = safe_float(p), safe_float(s)
                break
    except Exception:
        pass

    snow_risk = bool(codes & SNOW_CODES) or (max_snow or 0) > 0 or (daily_snow or 0) > 0
    rain_context = bool(codes & RAIN_CODES) or (max_rain or 0) > 0 or (max_showers or 0) > 0 or (max_precip or 0) > 0
    peak_rain_mm = max(max_precip or 0, max_rain or 0, max_showers or 0)
    heavy_rain_peak = bool(codes & HEAVY_RAIN_CODES) or peak_rain_mm >= HEAVY_RAIN_MM_PER_HOUR
    heavy_wind_peak = (max_gust or 0) >= HEAVY_WIND_GUST_KMH or (max_wind or 0) >= HEAVY_WIND_SPEED_KMH
    severe_peak_weather = heavy_rain_peak or heavy_wind_peak

    metrics.update({
        "gust_peak_kmh": round(max_gust, 1) if max_gust is not None else None,
        "wind_speed_peak_kmh": round(max_wind, 1) if max_wind is not None else None,
        "max_precip_peak_mm": round(max_precip, 2) if max_precip is not None else None,
        "daily_precip_mm": round(daily_precip, 2) if daily_precip is not None else None,
        "max_snow_peak_cm": round(max_snow, 2) if max_snow is not None else None,
        "daily_snow_cm": round(daily_snow, 2) if daily_snow is not None else None,
        "weather_codes_peak": sorted(codes),
        "weather_code_names_peak": code_names,
        "snow_signal": snow_risk,
        "rain_signal": rain_context,
        "heavy_rain_peak_signal": heavy_rain_peak,
        "heavy_wind_peak_signal": heavy_wind_peak,
        "heavy_rain_threshold_mm_per_hour": HEAVY_RAIN_MM_PER_HOUR,
        "heavy_wind_gust_threshold_kmh": HEAVY_WIND_GUST_KMH,
        "heavy_wind_speed_threshold_kmh": HEAVY_WIND_SPEED_KMH,
    })

    if snow_risk:
        if spec.city_slug == "toronto":
            flag("RED", f"Toronto + snow/winter signal: codes={sorted(codes & SNOW_CODES)} snow={max(max_snow or 0, daily_snow or 0):.1f}")
        else:
            flag("RED", f"snow/winter signal: codes={sorted(codes & SNOW_CODES)} snow={max(max_snow or 0, daily_snow or 0):.1f}")
    if rain_context:
        if spec.city_slug in EMPIRICAL_BAD_CITIES:
            flag("RED", "rain + empirical bad city: block new BUYs")
        else:
            warnings.append("rain signal present, but ordinary rain alone is not a sizing trigger")
    if severe_peak_weather:
        severe_bits = []
        if heavy_rain_peak:
            severe_bits.append(f"heavy rain 10-16 local: codes={sorted(codes & HEAVY_RAIN_CODES)} peak={peak_rain_mm:.1f}mm/h")
        if heavy_wind_peak:
            severe_bits.append(f"heavy wind 10-16 local: gust={max_gust or 0:.1f}km/h wind={max_wind or 0:.1f}km/h")
        severe_reason = "; ".join(severe_bits)
        if watch_city:
            flag("RED", f"watch city + {severe_reason}: block new BUYs")
        elif gate != "RED":
            flag("YELLOW", f"{severe_reason}: allow one normal order, disable same-market re-buy ladder")
        else:
            warnings.append(severe_reason)
    if watch_city and gate != "RED":
        flag("YELLOW", "empirical watch city: allow one normal order, disable same-market re-buy ladder")

    contributors = [0.0]
    if city_stats:
        contributors.append(float(city_stats.get("avg_abs_delta") or 0.0))
    if snow_risk:
        contributors.append(4.5)
    elif rain_context:
        contributors.append(2.5)
    expected_fluctuation = round(max(contributors), 2)
    metrics["expected_temp_fluctuation_c"] = expected_fluctuation

    size_multiplier = 0.0 if gate == "RED" else 1.0
    reason_text = "; ".join((reasons if reasons else warnings)[:4]) or "GREEN: no snow, no rain, not empirical bad/watch city"
    return {
        "gate": gate,
        "reason": reason_text,
        "reasons": reasons,
        "warnings": warnings,
        "metrics": metrics,
        "weather_codes": sorted(codes),
        "weather_code_names": code_names,
        "expected_temp_fluctuation_c": expected_fluctuation,
        "size_multiplier": size_multiplier,
        "event_slug": event_slug,
    }


async def analyze_city_safety(city: str, event_slug: str | None = None) -> dict[str, Any]:
    slug = city_slug(city)
    spec = STATIONS.get(slug)
    if spec is None:
        return {"city_slug": slug, "city": city, "station": None, "source": None, "gate": "RED", "reason": "city missing from station map", "reasons": ["city missing from station map"], "warnings": [], "metrics": {}, "weather_codes": [], "weather_code_names": [], "expected_temp_fluctuation_c": None, "size_multiplier": 0.0, "event_slug": event_slug, "checked_at": datetime.now(timezone.utc).isoformat()}
    timeout = httpx.Timeout(30.0, connect=7.0, read=22.0)
    async with httpx.AsyncClient(timeout=timeout, limits=httpx.Limits(max_connections=6, max_keepalive_connections=3), headers={"User-Agent": "polybot-weather-safety-filter/1.0"}) as client:
        try:
            station_info = None
            station_error: str | None = None
            try:
                station_info = await get_station_info(client, spec.station_id)
            except Exception as exc:
                station_error = f"station_info={type(exc).__name__}: {str(exc)[:160]}"
            lat = safe_float((station_info or {}).get("lat")) or spec.lat
            lon = safe_float((station_info or {}).get("lon")) or spec.lon
            if lat is None or lon is None:
                raise RuntimeError(f"missing station coordinates; {station_error or 'station_info unavailable'}")
            target_date = event_target_date(event_slug)
            primary_provider = forecast_provider_for_resolution_source(spec.source_class)

            metars_res, forecast_res, ensemble_res, prev_run_res = await asyncio.gather(
                get_metars(client, spec.station_id),
                get_forecast_with_fallback(
                    client,
                    lat,
                    lon,
                    target_date=target_date,
                    primary_provider=primary_provider,
                    station_id=spec.station_id,
                    resolution_source=spec.source_class,
                ),
                get_open_meteo_ensemble(client, lat, lon, target_date=target_date),
                get_prev_run(client, lat, lon, target_date=target_date),
                return_exceptions=True,
            )
            metars = metars_res if isinstance(metars_res, list) else []
            if isinstance(forecast_res, Exception):
                raise forecast_res
            if not isinstance(forecast_res, tuple):
                raise RuntimeError(f"forecast returned unexpected type {type(forecast_res).__name__}")
            forecast, _forecast_provider, _forecast_errors = forecast_res
            ensemble = ensemble_res if isinstance(ensemble_res, dict) else None
            prev_run = prev_run_res if isinstance(prev_run_res, dict) else None
            ev = evaluate_weather_safety(spec, station_info, metars, forecast, ensemble, prev_run, event_slug=event_slug, target_date=target_date)
            transient_warnings: list[str] = []
            if station_error:
                transient_warnings.append(station_error)
            if isinstance(metars_res, Exception):
                transient_warnings.append(f"metar={type(metars_res).__name__}: {str(metars_res)[:160]}")
            if isinstance(ensemble_res, Exception):
                transient_warnings.append(f"ensemble={type(ensemble_res).__name__}: {str(ensemble_res)[:160]}")
            if isinstance(prev_run_res, Exception):
                transient_warnings.append(f"previous_run={type(prev_run_res).__name__}: {str(prev_run_res)[:160]}")
            if transient_warnings:
                ev.setdefault("warnings", []).extend(transient_warnings)
                ev.setdefault("metrics", {})["transient_data_warnings"] = transient_warnings
        except Exception as exc:
            ev = evaluate_weather_safety(spec, None, [], None, None, None, event_slug=event_slug, target_date=event_target_date(event_slug), error=f"{type(exc).__name__}: {exc}")
    return {"city_slug": slug, "city": spec.city, "station": spec.station_id, "source": spec.source_class, "checked_at": datetime.now(timezone.utc).isoformat(), **ev}
