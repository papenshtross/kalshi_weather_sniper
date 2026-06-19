#!/usr/bin/env python3
"""Standalone Polymarket daily-high weather risk gate.

This script is intentionally NOT integrated with any strategy. It reads the same
weather-city universe as the live weather outlier sniper configs, resolves each
city's current active Polymarket highest-temperature event, pulls settlement
station observations plus Open-Meteo forecast/ensemble risk inputs, and prints a
markdown report for manual review.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from polybot.live.arb_sniper import pick_weather_high_temp_event, resolve_event_pairs  # noqa: E402


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


# Source/station map from the current Polymarket weather-market rules supplied
# by Roman. Most station IDs are ICAO/METAR-capable even when final settlement is
# Wunderground station history; settlement truth remains the exact market rules.
STATIONS: dict[str, StationSpec] = {
    "hong-kong": StationSpec("hong-kong", "Hong Kong", "HKO", "Hong Kong Observatory daily extract", "C", 22.302, 114.174, "official HKO source-class exception"),
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
    "panama-city": StationSpec("panama-city", "Panama City", "MPMG", "Wunderground station history", "C", None, None, "watch wording: Panama MPMG, not Florida Panhandle AI context"),
    "qingdao": StationSpec("qingdao", "Qingdao", "ZSQD", "Wunderground station history", "C"),
    "cape-town": StationSpec("cape-town", "Cape Town", "FACT", "Wunderground station history", "C"),
}

STRUCTURAL_RISK = {
    "denver", "mexico-city", "san-francisco", "los-angeles", "seattle", "wellington", "miami", "houston", "austin", "dallas", "atlanta", "chicago", "qingdao", "busan", "cape-town", "istanbul", "chengdu", "chongqing", "wuhan",
}


def c_to_f(c: float | None) -> float | None:
    return None if c is None else c * 9 / 5 + 32


def safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        y = float(x)
        if math.isnan(y):
            return None
        return y
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


def worst_gate(a: str, b: str) -> str:
    order = {"GREEN": 0, "YELLOW": 1, "RED": 2}
    return a if order[a] >= order[b] else b


def local_today_from_hourly(hourly: dict[str, Any]) -> str:
    times = hourly.get("time") or []
    if not times:
        return datetime.now().date().isoformat()
    # Open-Meteo returns local ISO timestamps when timezone=auto; choose the date
    # containing the latest available/forecast path around now by using first date
    # at or after current local-ish midpoint if possible. Simpler: API ordered,
    # past_days=1, so use the middle/current date.
    dates = sorted(set(str(t)[:10] for t in times))
    return dates[min(1, len(dates) - 1)]


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
        # HKO settlement is official daily extract. The public current-weather API
        # does not cleanly expose the exact daily-extract max in the same format,
        # so keep observation freshness/path as unavailable rather than faking it.
        return []
    r = await client.get("https://aviationweather.gov/api/data/metar", params={"ids": station_id, "format": "json", "hours": hours})
    if r.status_code != 200:
        return []
    data = r.json() or []
    return sorted(data, key=lambda m: m.get("obsTime") or 0, reverse=True)


async def get_open_meteo(client: httpx.AsyncClient, lat: float, lon: float) -> dict[str, Any]:
    vars_hourly = "temperature_2m,precipitation_probability,precipitation,rain,showers,weather_code,cloud_cover,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m,cape"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": vars_hourly,
        "daily": "temperature_2m_max",
        "timezone": "auto",
        "forecast_days": 2,
        "past_days": 1,
        "wind_speed_unit": "kmh",
    }
    r = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
    r.raise_for_status()
    return r.json()


async def get_open_meteo_ensemble(client: httpx.AsyncClient, lat: float, lon: float) -> dict[str, Any] | None:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "timezone": "auto",
        "forecast_days": 2,
        "past_days": 1,
    }
    try:
        r = await client.get("https://ensemble-api.open-meteo.com/v1/ensemble", params=params)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


async def get_prev_run(client: httpx.AsyncClient, lat: float, lon: float) -> dict[str, Any] | None:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max",
        "timezone": "auto",
        "forecast_days": 2,
        "past_days": 1,
        "forecast_hours": 48,
    }
    try:
        r = await client.get("https://previous-runs-api.open-meteo.com/v1/forecast", params=params)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def indices_for_date_and_window(times: list[str], date: str, start_h: int | None = None, end_h: int | None = None) -> list[int]:
    out = []
    for i, t in enumerate(times):
        if not str(t).startswith(date):
            continue
        if start_h is not None:
            try:
                hour = int(str(t)[11:13])
            except Exception:
                continue
            if hour < start_h or hour > (end_h if end_h is not None else hour):
                continue
        out.append(i)
    return out


def circular_deg_diff(a: float, b: float) -> float:
    d = abs(a - b) % 360
    return min(d, 360 - d)


def max_wind_shift(values: list[float]) -> float | None:
    vals = [v for v in values if v is not None and not math.isnan(v)]
    if len(vals) < 2:
        return None
    return max(circular_deg_diff(a, b) for a in vals for b in vals)


def evaluate(spec: StationSpec, event_slug: str | None, event_title: str | None, station_info: dict[str, Any] | None, metars: list[dict[str, Any]], forecast: dict[str, Any] | None, ensemble: dict[str, Any] | None, prev_run: dict[str, Any] | None, error: str | None = None) -> dict[str, Any]:
    reasons: list[str] = []
    warnings: list[str] = []
    gate = "GREEN"
    metrics: dict[str, Any] = {}

    def flag(level: str, reason: str) -> None:
        nonlocal gate
        gate = worst_gate(gate, level)
        (reasons if level == "RED" else warnings).append(reason)

    if error:
        flag("RED", f"data_error={error}")
        return {"gate": gate, "reasons": reasons, "warnings": warnings, "metrics": metrics}

    if not event_slug:
        flag("RED", "no active same-city Polymarket event resolved")
    if not station_info:
        flag("RED", f"station metadata unavailable for {spec.station_id}")
        lat = spec.lat
        lon = spec.lon
    else:
        lat = safe_float(station_info.get("lat")) or spec.lat
        lon = safe_float(station_info.get("lon")) or spec.lon
        metrics["latlon"] = [lat, lon]

    if spec.structural_note:
        warnings.append(spec.structural_note)
    if spec.city_slug in STRUCTURAL_RISK:
        warnings.append("structural risk: stricter filter location (fronts/marine/terrain/convection/cloud timing)")

    source_ok = bool(event_slug and station_info and spec.station_id)
    metrics["source_ok"] = source_ok
    if not source_ok:
        flag("RED", "resolution source/station not cleanly parsed")

    latest = metars[0] if metars else None
    obs_temp_c = safe_float(latest.get("temp")) if latest else None
    if latest:
        obs_ts = safe_float(latest.get("obsTime"))
        obs_age_min = (time.time() - obs_ts) / 60 if obs_ts else None
        metrics["obs_age_min"] = round(obs_age_min, 1) if obs_age_min is not None else None
        metrics["obs_temp_c"] = obs_temp_c
        if obs_age_min is None or obs_temp_c is None:
            flag("RED", "latest station observation missing temp/time")
        elif obs_age_min > 120:
            flag("RED", f"observation stale {obs_age_min:.0f}m >120m")
        elif obs_age_min > 75:
            flag("YELLOW", f"observation age {obs_age_min:.0f}m >75m")
    else:
        if spec.station_id == "HKO":
            flag("YELLOW", "HKO current exact-station obs not fetched; final source is official daily extract")
        else:
            flag("RED", "no recent METAR/station observation")

    if metars and len(metars) >= 2:
        # Sudden spike/drop check between consecutive reports, usually hourly but
        # sometimes special reports appear more frequently.
        temps = [(safe_float(m.get("obsTime")), safe_float(m.get("temp"))) for m in metars[:6]]
        temps = [(t, x) for t, x in temps if t and x is not None]
        max_jump_30 = 0.0
        for (t1, x1), (t2, x2) in zip(temps, temps[1:]):
            minutes = abs(t1 - t2) / 60
            if minutes <= 35:
                max_jump_30 = max(max_jump_30, abs(x1 - x2))
        metrics["max_temp_jump_30m_c"] = round(max_jump_30, 2)
        if max_jump_30 >= 3:
            flag("RED", f"station spike/drop {max_jump_30:.1f}°C within <=30m")

    if not forecast:
        flag("RED", "Open-Meteo forecast unavailable")
        return {"gate": gate, "reasons": reasons, "warnings": warnings, "metrics": metrics}

    hourly = forecast.get("hourly") or {}
    times = hourly.get("time") or []
    today = local_today_from_hourly(hourly)
    metrics["local_date"] = today
    day_ix = indices_for_date_and_window(times, today)
    peak_ix = indices_for_date_and_window(times, today, 10, 16)
    if not day_ix or not peak_ix:
        flag("RED", "Open-Meteo hourly forecast missing today/peak window")
        return {"gate": gate, "reasons": reasons, "warnings": warnings, "metrics": metrics}

    temps = hourly.get("temperature_2m") or []
    high_latest = max((safe_float(temps[i]) for i in day_ix if i < len(temps)), default=None)
    metrics["forecast_high_c"] = round(high_latest, 2) if high_latest is not None else None

    # Previous run daily max: endpoint often returns current/previous run arrays;
    # use same local date if present. If unavailable, mark yellow rather than red.
    prev_high = None
    if prev_run:
        d = prev_run.get("daily") or {}
        try:
            for t, v in zip(d.get("time") or [], d.get("temperature_2m_max") or []):
                if str(t) == today:
                    prev_high = safe_float(v)
                    break
        except Exception:
            prev_high = None
    if prev_high is not None and high_latest is not None:
        delta = abs(high_latest - prev_high)
        metrics["high_prev_run_c"] = round(prev_high, 2)
        metrics["high_run_delta_c"] = round(delta, 2)
        if delta > 2.0:
            flag("RED", f"forecast run high shifted {delta:.1f}°C >2.0°C")
        elif delta > 1.5:
            flag("YELLOW", f"forecast run high shifted {delta:.1f}°C >1.5°C")
    else:
        flag("YELLOW", "previous-run high unavailable")

    # Ensemble high spread by member: for each ensemble temp member, max over today.
    ens_spread = None
    if ensemble and (ensemble.get("hourly") or {}).get("time"):
        eh = ensemble.get("hourly") or {}
        etimes = eh.get("time") or []
        e_day_ix = indices_for_date_and_window(etimes, today)
        highs = []
        for k, vals in eh.items():
            if not k.startswith("temperature_2m_member") or not isinstance(vals, list):
                continue
            vals_today = [safe_float(vals[i]) for i in e_day_ix if i < len(vals)]
            vals_today = [v for v in vals_today if v is not None]
            if vals_today:
                highs.append(max(vals_today))
        p10 = percentile(highs, 0.10)
        p90 = percentile(highs, 0.90)
        if p10 is not None and p90 is not None:
            ens_spread = p90 - p10
            metrics["ens_p10_high_c"] = round(p10, 2)
            metrics["ens_p90_high_c"] = round(p90, 2)
            metrics["ens_spread_c"] = round(ens_spread, 2)
    if ens_spread is None:
        flag("YELLOW", "ensemble high spread unavailable")
    elif ens_spread > 3.5:
        flag("RED", f"ensemble high spread {ens_spread:.1f}°C >3.5°C")
    elif ens_spread > 2.5:
        flag("YELLOW", f"ensemble high spread {ens_spread:.1f}°C >2.5°C")

    def max_var(name: str, idxs: list[int]) -> float | None:
        vals = hourly.get(name) or []
        xs = [safe_float(vals[i]) for i in idxs if i < len(vals)]
        xs = [x for x in xs if x is not None]
        return max(xs) if xs else None

    max_pop = max_var("precipitation_probability", peak_ix)
    max_precip = max_var("precipitation", peak_ix)
    max_rain = max_var("rain", peak_ix)
    max_showers = max_var("showers", peak_ix)
    max_cape = max_var("cape", peak_ix)
    max_gust = max_var("wind_gusts_10m", peak_ix)
    pressure_vals = [safe_float((hourly.get("surface_pressure") or [])[i]) for i in peak_ix if i < len(hourly.get("surface_pressure") or [])]
    pressure_vals = [x for x in pressure_vals if x is not None]
    pressure_change = max(pressure_vals) - min(pressure_vals) if pressure_vals else None
    cloud_vals = [safe_float((hourly.get("cloud_cover") or [])[i]) for i in peak_ix if i < len(hourly.get("cloud_cover") or [])]
    cloud_vals = [x for x in cloud_vals if x is not None]
    cloud_swing = max(cloud_vals) - min(cloud_vals) if cloud_vals else None
    wind_dirs = [safe_float((hourly.get("wind_direction_10m") or [])[i]) for i in peak_ix if i < len(hourly.get("wind_direction_10m") or [])]
    wind_shift = max_wind_shift([x for x in wind_dirs if x is not None])
    wind_speed = max_var("wind_speed_10m", peak_ix)
    weather_codes = [safe_float((hourly.get("weather_code") or [])[i]) for i in peak_ix if i < len(hourly.get("weather_code") or [])]
    weather_codes_i = {int(x) for x in weather_codes if x is not None}
    thunder = bool(weather_codes_i & {95, 96, 99})
    rain_or_shower_code = bool(weather_codes_i & {61, 63, 65, 80, 81, 82}) or (max_rain or 0) > 0 or (max_showers or 0) > 0

    metrics.update({
        "max_pop_peak_pct": round(max_pop, 1) if max_pop is not None else None,
        "max_precip_peak_mm": round(max_precip, 2) if max_precip is not None else None,
        "cape_peak": round(max_cape, 0) if max_cape is not None else None,
        "gust_peak_kmh": round(max_gust, 1) if max_gust is not None else None,
        "wind_shift_peak_deg": round(wind_shift, 0) if wind_shift is not None else None,
        "wind_speed_peak_kmh": round(wind_speed, 1) if wind_speed is not None else None,
        "cloud_swing_pct": round(cloud_swing, 0) if cloud_swing is not None else None,
        "pressure_change_hpa_peak": round(pressure_change, 1) if pressure_change is not None else None,
        "weather_codes_peak": sorted(weather_codes_i),
    })

    if max_pop is not None:
        if max_pop >= 40:
            flag("RED", f"peak PoP {max_pop:.0f}% >=40%")
        elif max_pop >= 25:
            flag("YELLOW", f"peak PoP {max_pop:.0f}% >=25%")
    if max_precip is not None and max_precip >= 1:
        flag("RED", f"peak precip {max_precip:.1f}mm >=1mm")
    if thunder:
        flag("RED", "thunderstorm code in peak window")
    if rain_or_shower_code:
        flag("RED", "rain/shower code or modeled rain in peak window")
    if max_cape is not None:
        if max_cape >= 1000:
            flag("RED", f"CAPE {max_cape:.0f} >=1000")
        elif max_cape >= 500:
            flag("YELLOW", f"CAPE {max_cape:.0f} >=500")
    if cloud_swing is not None and cloud_swing > 40:
        flag("RED", f"cloud swing {cloud_swing:.0f}pp >40pp")
    if wind_shift is not None and wind_shift >= 60 and (wind_speed or 0) >= 15:
        flag("RED", f"wind shift {wind_shift:.0f}° with speed {wind_speed:.0f}km/h")
    elif wind_shift is not None and wind_shift >= 45:
        flag("YELLOW", f"wind shift {wind_shift:.0f}° >=45°")
    if max_gust is not None:
        if max_gust >= 35:
            flag("RED", f"gusts {max_gust:.0f}km/h >=35")
        elif max_gust >= 30:
            flag("YELLOW", f"gusts {max_gust:.0f}km/h >=30")
    if pressure_change is not None and pressure_change >= 3:
        flag("RED", f"pressure change {pressure_change:.1f}hPa in peak window >=3")

    # Current path tracking: compare latest station temp to nearest forecast hour.
    if latest and obs_temp_c is not None:
        report_time = latest.get("reportTime") or latest.get("receiptTime")
        if report_time:
            # Match by local hour string if possible; otherwise use latest available forecast hour before now.
            try:
                obs_dt_utc = datetime.fromisoformat(str(report_time).replace("Z", "+00:00"))
                tzname = forecast.get("timezone")
                obs_local = obs_dt_utc.astimezone(ZoneInfo(tzname)) if tzname else obs_dt_utc
                obs_hour = obs_local.strftime("%Y-%m-%dT%H:00")
                if obs_hour in times:
                    fi = times.index(obs_hour)
                else:
                    fi = max((i for i, t in enumerate(times) if str(t) <= obs_hour), default=None)
                ftemp = safe_float(temps[fi]) if fi is not None and fi < len(temps) else None
                if ftemp is not None:
                    delta = abs(obs_temp_c - ftemp)
                    metrics["obs_vs_forecast_path_delta_c"] = round(delta, 2)
                    metrics["forecast_path_temp_c"] = round(ftemp, 2)
                    if delta > 1.5:
                        flag("RED", f"actual temp vs forecast path mismatch {delta:.1f}°C >1.5°C")
                    elif delta > 1.0:
                        flag("YELLOW", f"actual temp vs forecast path mismatch {delta:.1f}°C >1.0°C")
            except Exception:
                flag("YELLOW", "could not compare station obs to forecast path")

    # Tail estimate proxy from ensemble high distribution and run/path instability.
    tail_proxy = 0.0
    if ens_spread is not None:
        # Approximate normal sigma from P90-P10; not a real calibrated model, just
        # a ranking feature for the requested standalone trial.
        sigma = ens_spread / 2.563 if ens_spread > 0 else 0.0
        if sigma > 0:
            z = 4.0 / sigma
            tail_proxy = math.erfc(z / math.sqrt(2))
    metrics["p_4c_miss_proxy_pct"] = round(tail_proxy * 100, 2)
    if tail_proxy > 0.06:
        flag("RED", f"proxy P(|miss|>=4°C) {tail_proxy*100:.1f}% >6%")
    elif tail_proxy > 0.05:
        flag("YELLOW", f"proxy P(|miss|>=4°C) {tail_proxy*100:.1f}% >5%")

    return {"gate": gate, "reasons": reasons, "warnings": warnings, "metrics": metrics}


async def analyze_city(client: httpx.AsyncClient, spec: StationSpec) -> dict[str, Any]:
    try:
        station_info_task = get_station_info(client, spec.station_id)
        event_task = pick_weather_high_temp_event(client, spec.city_slug)
        station_info, event_slug = await asyncio.gather(station_info_task, event_task)
        event_title = None
        options = None
        if event_slug:
            try:
                pairs = await resolve_event_pairs(f"auto:weather-high-temp:{spec.city_slug}", all_markets=True)
                if pairs:
                    event_title = pairs[0].get("event_title")
                    options = len(pairs)
            except Exception:
                pass
        lat = safe_float((station_info or {}).get("lat")) or spec.lat
        lon = safe_float((station_info or {}).get("lon")) or spec.lon
        if lat is None or lon is None:
            return {"city": spec.city, "station": spec.station_id, "event_slug": event_slug, "event_title": event_title, "source": spec.source_class, "gate": "RED", "reasons": ["missing station coordinates"], "warnings": [], "metrics": {}}
        metars_task = get_metars(client, spec.station_id)
        forecast_task = get_open_meteo(client, lat, lon)
        ensemble_task = get_open_meteo_ensemble(client, lat, lon)
        prev_task = get_prev_run(client, lat, lon)
        metars, forecast, ensemble, prev_run = await asyncio.gather(metars_task, forecast_task, ensemble_task, prev_task)
        ev = evaluate(spec, event_slug, event_title, station_info, metars, forecast, ensemble, prev_run)
        return {
            "city": spec.city,
            "city_slug": spec.city_slug,
            "station": spec.station_id,
            "source": spec.source_class,
            "unit": spec.unit,
            "event_slug": event_slug,
            "event_title": event_title,
            "options": options,
            **ev,
        }
    except Exception as exc:
        return {"city": spec.city, "city_slug": spec.city_slug, "station": spec.station_id, "source": spec.source_class, "unit": spec.unit, "event_slug": None, "event_title": None, "gate": "RED", "reasons": [f"exception={type(exc).__name__}: {exc}"], "warnings": [], "metrics": {}}


def load_weather_cities(config_dir: Path) -> list[str]:
    cities = []
    for p in sorted(config_dir.glob("weather-outlier-sniper-*-live.yaml")):
        try:
            cfg = yaml.safe_load(p.read_text()) or {}
        except Exception:
            continue
        city = str(cfg.get("weather_city") or "").strip().lower().replace("_", "-").replace(" ", "-")
        if not city:
            slug = str(cfg.get("market_slug") or "")
            prefix = "auto:weather-high-temp:"
            if slug.startswith(prefix):
                city = slug[len(prefix):]
        if city:
            cities.append(city)
    return sorted(set(cities))


def fmt_metric(m: dict[str, Any], k: str, suffix: str = "") -> str:
    v = m.get(k)
    if v is None:
        return "n/a"
    return f"{v}{suffix}"


def make_markdown(results: list[dict[str, Any]]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    counts = {g: sum(1 for r in results if r.get("gate") == g) for g in ["GREEN", "YELLOW", "RED"]}
    lines = [
        f"# Standalone weather risk gate — {now}",
        "",
        "Not integrated with strategies. Gate is for manual validation only. Trade rule under test: **GREEN only**, skip RED, treat YELLOW as reduce/skip.",
        f"Summary: GREEN={counts['GREEN']} · YELLOW={counts['YELLOW']} · RED={counts['RED']} · total={len(results)}",
        "",
    ]
    for gate in ["GREEN", "YELLOW", "RED"]:
        group = [r for r in results if r.get("gate") == gate]
        if not group:
            continue
        lines.append(f"## {gate}")
        for r in sorted(group, key=lambda x: x.get("city", "")):
            m = r.get("metrics") or {}
            bits = [
                f"station={r.get('station')} ({r.get('source')})",
                f"event={r.get('event_slug') or 'n/a'}",
                f"high={fmt_metric(m, 'forecast_high_c', '°C')}",
                f"ens_spread={fmt_metric(m, 'ens_spread_c', '°C')}",
                f"run_delta={fmt_metric(m, 'high_run_delta_c', '°C')}",
                f"PoP={fmt_metric(m, 'max_pop_peak_pct', '%')}",
                f"CAPE={fmt_metric(m, 'cape_peak')}",
                f"gust={fmt_metric(m, 'gust_peak_kmh', 'km/h')}",
                f"wind_shift={fmt_metric(m, 'wind_shift_peak_deg', '°')}",
                f"path_delta={fmt_metric(m, 'obs_vs_forecast_path_delta_c', '°C')}",
                f"4Cmiss_proxy={fmt_metric(m, 'p_4c_miss_proxy_pct', '%')}",
            ]
            reasons = r.get("reasons") or []
            warnings = r.get("warnings") or []
            if gate == "GREEN":
                why = "passes hard criteria"
                if warnings:
                    why += "; notes: " + "; ".join(warnings[:2])
            else:
                why = "; ".join(reasons[:4] + warnings[:2]) or "criteria warning"
            lines.append(f"- **{r.get('city')}** — {why}. " + " · ".join(bits))
        lines.append("")
    return "\n".join(lines).strip() + "\n"


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config-dir", type=Path, default=ROOT / "config")
    ap.add_argument("--json", type=Path, default=None)
    ap.add_argument("--markdown", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    cities = load_weather_cities(args.config_dir)
    specs = [STATIONS[c] for c in cities if c in STATIONS]
    missing = [c for c in cities if c not in STATIONS]
    if args.limit:
        specs = specs[: args.limit]

    timeout = httpx.Timeout(25.0, connect=5.0, read=20.0)
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
    async with httpx.AsyncClient(timeout=timeout, limits=limits, headers={"User-Agent": "polybot-weather-risk-gate/0.1"}) as client:
        sem = asyncio.Semaphore(8)
        async def wrapped(spec: StationSpec):
            async with sem:
                return await analyze_city(client, spec)
        results = await asyncio.gather(*(wrapped(s) for s in specs))

    if missing:
        results.extend({"city": c, "city_slug": c, "station": None, "source": None, "unit": "C", "event_slug": None, "event_title": None, "gate": "RED", "reasons": ["city missing from station map"], "warnings": [], "metrics": {}} for c in missing)

    order = {"GREEN": 0, "YELLOW": 1, "RED": 2}
    results = sorted(results, key=lambda r: (order.get(r.get("gate"), 9), r.get("city", "")))
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    md = make_markdown(results)
    if args.markdown:
        args.markdown.parent.mkdir(parents=True, exist_ok=True)
        args.markdown.write_text(md)
    print(md)


if __name__ == "__main__":
    asyncio.run(main())
