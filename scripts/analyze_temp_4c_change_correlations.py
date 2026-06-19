#!/usr/bin/env python3
"""Large-sample analysis of >=4C daily-high changes for Polymarket weather cities.

Builds two datasets:
1) all available historical Open-Meteo city-days for current Polymarket weather cities
2) subset of days that match closed Polymarket daily-high weather events

Outputs JSON/CSV/Markdown under reports/temp_4c_change_analysis/.
"""
from __future__ import annotations

import asyncio
import csv
import json
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiohttp
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from polybot.live.weather_safety_filter import (  # noqa: E402
    HEAVY_RAIN_CODES,
    RAIN_CODES,
    SEVERE_CODES,
    SNOW_CODES,
    STATIONS,
    STRUCTURAL_RISK,
)

OUT = ROOT / "reports" / "temp_4c_change_analysis"
CACHE = OUT / "cache"
OUT.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)
UA = {"User-Agent": "polybot-temp-4c-correlation/1.0 Mozilla/5.0"}
MONTHS = "january february march april may june july august september october november december".split()
MON = {m: i + 1 for i, m in enumerate(MONTHS)}

US_F_CITIES = {
    "chicago", "nyc", "denver", "seattle", "miami", "atlanta", "houston", "austin", "dallas", "los-angeles", "san-francisco"
}
COASTAL_OR_MARINE = {
    "nyc", "seattle", "miami", "houston", "los-angeles", "san-francisco", "london", "hong-kong", "seoul", "shanghai",
    "singapore", "paris", "buenos-aires", "wellington", "jakarta", "tokyo", "helsinki", "taipei", "amsterdam",
    "milan", "toronto", "shenzhen", "kuala-lumpur", "sao-paulo", "manila", "guangzhou", "karachi", "busan", "jeddah",
    "panama-city", "qingdao", "cape-town",
}
INTERIOR_CONTINENTAL = set(STATIONS) - COASTAL_OR_MARINE


def cache_path(kind: str, key: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(key))[:220]
    return CACHE / f"{kind}_{safe}.json"


async def get_json(session: aiohttp.ClientSession, url: str, cache: Path | None = None, timeout: int = 90) -> Any:
    if cache and cache.exists():
        return json.loads(cache.read_text())
    last = None
    for attempt in range(5):
        try:
            async with session.get(url, headers=UA, timeout=timeout) as r:
                txt = await r.text()
                if r.status == 429:
                    # Open-Meteo archive has a strict per-minute cap. Back off for a full
                    # minute rather than burning all retries immediately.
                    retry_after = safe_float(r.headers.get("Retry-After")) or 65.0
                    await asyncio.sleep(retry_after + 5.0 * attempt)
                    last = RuntimeError(f"HTTP 429 {txt[:120]}")
                    continue
                if r.status != 200:
                    raise RuntimeError(f"HTTP {r.status} {txt[:240]}")
                data = json.loads(txt)
                if cache:
                    cache.write_text(json.dumps(data))
                return data
        except Exception as e:
            last = e
            if attempt < 4:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
    raise last or RuntimeError("request failed")


def safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        y = float(x)
        return None if math.isnan(y) else y
    except Exception:
        return None


def circ_range(degs: list[Any]) -> float | None:
    vals = [float(x) % 360 for x in degs if x is not None]
    if not vals:
        return None
    vals = sorted(vals)
    gaps = [vals[i + 1] - vals[i] for i in range(len(vals) - 1)] + [vals[0] + 360 - vals[-1]]
    return 360 - max(gaps)


def mean(xs: list[float]) -> float | None:
    xs = [x for x in xs if x is not None and not math.isnan(x)]
    return sum(xs) / len(xs) if xs else None


async def station_coords(session: aiohttp.ClientSession, city_slug: str) -> dict[str, Any] | None:
    spec = STATIONS[city_slug]
    if spec.lat is not None and spec.lon is not None:
        return {"lat": spec.lat, "lon": spec.lon, "source": "skill_static"}
    if spec.station_id == "HKO":
        return {"lat": 22.302, "lon": 114.174, "source": "hko_static"}
    url = "https://aviationweather.gov/api/data/stationinfo?" + urlencode({"ids": spec.station_id, "format": "json"})
    try:
        data = await get_json(session, url, cache_path("station", spec.station_id), timeout=30)
        row = (data or [None])[0]
        if row and row.get("lat") is not None and row.get("lon") is not None:
            return {"lat": float(row["lat"]), "lon": float(row["lon"]), "source": "aviationweather"}
    except Exception:
        pass
    # Last-resort geocode by city display name.
    url = "https://geocoding-api.open-meteo.com/v1/search?" + urlencode({"name": spec.city, "count": 1, "language": "en", "format": "json"})
    data = await get_json(session, url, cache_path("geo", city_slug), timeout=30)
    row = (data.get("results") or [None])[0]
    if row:
        return {"lat": float(row["latitude"]), "lon": float(row["longitude"]), "source": "geocode", "country": row.get("country")}
    return None


async def archive_city(session: aiohttp.ClientSession, city_slug: str, lat: float, lon: float, start: str, end: str) -> dict[str, Any]:
    hourly = "temperature_2m,precipitation,rain,showers,snowfall,weather_code,cloud_cover,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m"
    daily = "temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,showers_sum,snowfall_sum,weather_code,wind_speed_10m_max,wind_gusts_10m_max"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": daily,
        "hourly": hourly,
        "timezone": "auto",
        "wind_speed_unit": "kmh",
    }
    url = "https://archive-api.open-meteo.com/v1/archive?" + urlencode(params)
    return await get_json(session, url, cache_path("archive", f"{city_slug}_{start}_{end}_{lat:.3f}_{lon:.3f}"), timeout=120)


def day_metrics(city_slug: str, data: dict[str, Any]) -> list[dict[str, Any]]:
    daily = data.get("daily") or {}
    hourly = data.get("hourly") or {}
    days = daily.get("time") or []
    h_times = hourly.get("time") or []
    by_date: dict[str, list[int]] = defaultdict(list)
    for i, t in enumerate(h_times):
        by_date[str(t)[:10]].append(i)

    rows: list[dict[str, Any]] = []
    for idx, d in enumerate(days):
        ix_all = by_date.get(str(d), [])
        ix_peak = [i for i in ix_all if 10 <= int(str(h_times[i])[11:13]) <= 16]
        if not ix_peak:
            ix_peak = ix_all
        def darr(k: str) -> Any:
            arr = daily.get(k) or []
            return arr[idx] if idx < len(arr) else None
        def hvals(k: str, ixs: list[int] = ix_peak) -> list[float]:
            arr = hourly.get(k) or []
            out = []
            for i in ixs:
                if i < len(arr):
                    v = safe_float(arr[i])
                    if v is not None:
                        out.append(v)
            return out
        codes = set(int(x) for x in hvals("weather_code", ix_peak))
        daily_code = safe_float(darr("weather_code"))
        if daily_code is not None:
            codes.add(int(daily_code))
        pressure_vals = hvals("surface_pressure", ix_peak)
        all_pressure = hvals("surface_pressure", ix_all)
        temp_vals = hvals("temperature_2m", ix_peak)
        cloud_vals = hvals("cloud_cover", ix_peak)
        wind_vals = hvals("wind_speed_10m", ix_peak)
        gust_vals = hvals("wind_gusts_10m", ix_peak)
        precip_vals = hvals("precipitation", ix_peak)
        rain_vals = hvals("rain", ix_peak)
        showers_vals = hvals("showers", ix_peak)
        snow_vals = hvals("snowfall", ix_peak)
        row = {
            "city_slug": city_slug,
            "date": str(d),
            "unit_market": "F" if city_slug in US_F_CITIES else "C",
            "high_c": safe_float(darr("temperature_2m_max")),
            "low_c": safe_float(darr("temperature_2m_min")),
            "daily_precip_mm": safe_float(darr("precipitation_sum")),
            "daily_rain_mm": safe_float(darr("rain_sum")),
            "daily_showers_mm": safe_float(darr("showers_sum")),
            "daily_snow_cm": safe_float(darr("snowfall_sum")),
            "daily_wind_max_kmh": safe_float(darr("wind_speed_10m_max")),
            "daily_gust_max_kmh": safe_float(darr("wind_gusts_10m_max")),
            "peak_temp_range_c": (max(temp_vals) - min(temp_vals)) if len(temp_vals) >= 2 else None,
            "peak_precip_mm": sum(precip_vals) if precip_vals else 0.0,
            "peak_rain_mm": sum(rain_vals) if rain_vals else 0.0,
            "peak_showers_mm": sum(showers_vals) if showers_vals else 0.0,
            "peak_snow_cm": sum(snow_vals) if snow_vals else 0.0,
            "peak_cloud_max_pct": max(cloud_vals) if cloud_vals else None,
            "peak_cloud_swing_pp": (max(cloud_vals) - min(cloud_vals)) if len(cloud_vals) >= 2 else None,
            "peak_pressure_range_hpa": (max(pressure_vals) - min(pressure_vals)) if len(pressure_vals) >= 2 else None,
            "day_pressure_mean_hpa": mean(all_pressure),
            "peak_wind_max_kmh": max(wind_vals) if wind_vals else None,
            "peak_gust_max_kmh": max(gust_vals) if gust_vals else None,
            "peak_wind_shift_deg": circ_range(hvals("wind_direction_10m", ix_peak)),
            "weather_codes": sorted(codes),
            "rain_code": bool(codes & RAIN_CODES),
            "heavy_rain_code": bool(codes & HEAVY_RAIN_CODES),
            "storm_code": bool(codes & SEVERE_CODES),
            "snow_code": bool(codes & SNOW_CODES),
            "structural_risk_city": city_slug in STRUCTURAL_RISK,
            "coastal_or_marine": city_slug in COASTAL_OR_MARINE,
            "interior_continental": city_slug in INTERIOR_CONTINENTAL,
        }
        row["rain_any"] = bool(row["rain_code"] or (row["daily_precip_mm"] or 0) > 0 or (row["peak_precip_mm"] or 0) > 0)
        row["heavy_precip"] = bool(row["heavy_rain_code"] or (row["daily_precip_mm"] or 0) >= 10 or (row["peak_precip_mm"] or 0) >= 3)
        row["windy"] = bool((row["peak_gust_max_kmh"] or row["daily_gust_max_kmh"] or 0) >= 45 or (row["peak_wind_max_kmh"] or 0) >= 30)
        row["very_windy"] = bool((row["peak_gust_max_kmh"] or row["daily_gust_max_kmh"] or 0) >= 60 or (row["peak_wind_max_kmh"] or 0) >= 40)
        row["front_proxy"] = bool((row["peak_pressure_range_hpa"] or 0) >= 4 or ((row["peak_wind_shift_deg"] or 0) >= 90 and (row["peak_wind_max_kmh"] or 0) >= 20))
        row["cloud_swing"] = bool((row["peak_cloud_swing_pp"] or 0) >= 60)
        row["storm_or_convective"] = bool(row["storm_code"] or row["heavy_precip"] or (row["peak_showers_mm"] or 0) >= 2)
        rows.append(row)

    rows.sort(key=lambda r: r["date"])
    prev: dict[str, Any] | None = None
    for r in rows:
        if prev and prev.get("high_c") is not None and r.get("high_c") is not None:
            delta = r["high_c"] - prev["high_c"]
            r["delta_high_c"] = round(delta, 3)
            r["abs_delta_high_c"] = round(abs(delta), 3)
            r["change_4c"] = abs(delta) >= 4.0
            r["warmup_4c"] = delta >= 4.0
            r["cooldown_4c"] = delta <= -4.0
            if prev.get("day_pressure_mean_hpa") is not None and r.get("day_pressure_mean_hpa") is not None:
                r["pressure_mean_delta_hpa"] = round(r["day_pressure_mean_hpa"] - prev["day_pressure_mean_hpa"], 3)
            r["prev_rain_any"] = bool(prev.get("rain_any"))
            r["prev_windy"] = bool(prev.get("windy"))
            r["prev_front_proxy"] = bool(prev.get("front_proxy"))
            r["prev_weather_codes"] = prev.get("weather_codes") or []
        else:
            r["delta_high_c"] = None
            r["abs_delta_high_c"] = None
            r["change_4c"] = False
            r["warmup_4c"] = False
            r["cooldown_4c"] = False
            r["pressure_mean_delta_hpa"] = None
            r["prev_rain_any"] = False
            r["prev_windy"] = False
            r["prev_front_proxy"] = False
            r["prev_weather_codes"] = []
        prev = r
    return rows


def parse_event_date(e: dict[str, Any]) -> str | None:
    title = e.get("title", "")
    m = re.match(r"Highest temperature in (.+) on ([A-Za-z]+) (\d{1,2})\?", title)
    if not m:
        return None
    month = MON.get(m.group(2).lower())
    day = int(m.group(3))
    if not month:
        return None
    # Use slug year when present, else endDate year.
    sm = re.search(r"-on-[a-z]+-\d{1,2}-(\d{4})$", e.get("slug", ""))
    if sm:
        year = int(sm.group(1))
    else:
        year = datetime.fromisoformat(e["endDate"].replace("Z", "+00:00")).year if e.get("endDate") else datetime.now().year
    return f"{year:04d}-{month:02d}-{day:02d}"


def city_slug_from_event_slug(slug: str) -> str | None:
    m = re.match(r"highest-temperature-in-(.+)-on-[a-z]+-\d{1,2}-\d{4}$", slug or "")
    return m.group(1) if m else None


async def collect_polymarket_events(session: aiohttp.ClientSession, max_offset: int = 100000) -> list[dict[str, Any]]:
    pat = re.compile(r"^Highest temperature in .+ on .+\?$", re.I)
    events: list[dict[str, Any]] = []
    seen = set()
    for off in range(0, max_offset + 1, 500):
        url = "https://gamma-api.polymarket.com/events?" + urlencode({"closed": "true", "limit": 500, "offset": off, "order": "endDate", "ascending": "false"})
        data = await get_json(session, url, cache_path("pm_events", str(off)), timeout=90)
        if not data:
            break
        for e in data:
            if pat.match(e.get("title", "")) and e.get("slug") not in seen:
                slug = e.get("slug")
                city = city_slug_from_event_slug(slug)
                d = parse_event_date(e)
                if city in STATIONS and d:
                    seen.add(slug)
                    events.append({
                        "slug": slug,
                        "title": e.get("title"),
                        "endDate": e.get("endDate"),
                        "city_slug": city,
                        "date": d,
                        "markets_count": len(e.get("markets") or []),
                        "volume": safe_float(e.get("volume")),
                        "liquidity": safe_float(e.get("liquidity")),
                    })
        if off and off % 5000 == 0:
            print(f"polymarket offset={off} events={len(events)} page_end={data[-1].get('endDate')}")
        # Stop after catalog is clearly older than launch of current weather set and no new weather events in long tail.
        if off >= 40000 and len(events) == 0:
            break
    return events


def point_biserial(rows: list[dict[str, Any]], xkey: str, ykey: str = "change_4c") -> float | None:
    xs, ys = [], []
    for r in rows:
        x = safe_float(r.get(xkey))
        if x is None:
            continue
        xs.append(x)
        ys.append(1.0 if r.get(ykey) else 0.0)
    if len(xs) < 20 or len(set(ys)) < 2:
        return None
    return float(np.corrcoef(np.array(xs), np.array(ys))[0, 1])


def flag_stats(rows: list[dict[str, Any]], flag: str, ykey: str = "change_4c") -> dict[str, Any]:
    yes = [r for r in rows if r.get(flag)]
    no = [r for r in rows if not r.get(flag)]
    def rate(sub: list[dict[str, Any]]) -> float:
        return sum(1 for r in sub if r.get(ykey)) / len(sub) if sub else 0.0
    ry, rn = rate(yes), rate(no)
    return {
        "flag": flag,
        "n_yes": len(yes),
        "n_no": len(no),
        "events_yes": sum(1 for r in yes if r.get(ykey)),
        "events_no": sum(1 for r in no if r.get(ykey)),
        "rate_yes": ry,
        "rate_no": rn,
        "risk_ratio": (ry / rn if rn > 0 else None),
        "rate_diff_pp": (ry - rn) * 100,
        "avg_abs_delta_yes": mean([r.get("abs_delta_high_c") for r in yes]),
        "avg_abs_delta_no": mean([r.get("abs_delta_high_c") for r in no]),
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def summarize(name: str, rows: list[dict[str, Any]], pm_event_dates: set[tuple[str, str]]) -> dict[str, Any]:
    rows = [r for r in rows if r.get("abs_delta_high_c") is not None]
    flags = [
        "rain_any", "heavy_precip", "storm_code", "storm_or_convective", "snow_code", "windy", "very_windy", "front_proxy", "cloud_swing",
        "prev_rain_any", "prev_windy", "prev_front_proxy", "structural_risk_city", "coastal_or_marine", "interior_continental",
    ]
    nums = [
        "daily_precip_mm", "peak_precip_mm", "peak_rain_mm", "peak_showers_mm", "daily_snow_cm", "peak_snow_cm",
        "daily_gust_max_kmh", "peak_gust_max_kmh", "peak_wind_max_kmh", "peak_wind_shift_deg", "peak_pressure_range_hpa",
        "pressure_mean_delta_hpa", "peak_cloud_swing_pp", "peak_temp_range_c",
    ]
    code_counts: dict[int, dict[str, int]] = defaultdict(lambda: {"n": 0, "events": 0})
    for r in rows:
        for c in set(r.get("weather_codes") or []):
            code_counts[int(c)]["n"] += 1
            code_counts[int(c)]["events"] += int(bool(r.get("change_4c")))
    base = sum(r.get("change_4c") for r in rows) / len(rows) if rows else 0
    code_lifts = []
    for c, st in code_counts.items():
        if st["n"] >= 30:
            rate = st["events"] / st["n"]
            code_lifts.append({"code": c, "n": st["n"], "events": st["events"], "rate": rate, "lift": rate / base if base else None})
    city_stats = []
    by_city = defaultdict(list)
    for r in rows:
        by_city[r["city_slug"]].append(r)
    for city, sub in by_city.items():
        city_stats.append({
            "city_slug": city,
            "n": len(sub),
            "events": sum(r.get("change_4c") for r in sub),
            "rate": sum(r.get("change_4c") for r in sub) / len(sub),
            "warmups": sum(r.get("warmup_4c") for r in sub),
            "cooldowns": sum(r.get("cooldown_4c") for r in sub),
            "avg_abs_delta": mean([r.get("abs_delta_high_c") for r in sub]),
            "max_abs_delta": max(r.get("abs_delta_high_c") for r in sub if r.get("abs_delta_high_c") is not None),
            "pm_days": sum((r["city_slug"], r["date"]) in pm_event_dates for r in sub),
        })
    return {
        "name": name,
        "n_city_days": len(rows),
        "n_cities": len(by_city),
        "n_4c_changes": sum(r.get("change_4c") for r in rows),
        "base_rate": base,
        "warmups_4c": sum(r.get("warmup_4c") for r in rows),
        "cooldowns_4c": sum(r.get("cooldown_4c") for r in rows),
        "flag_stats": sorted([flag_stats(rows, f) for f in flags], key=lambda x: (x["risk_ratio"] or 0), reverse=True),
        "numeric_correlations": sorted([{"feature": n, "r": point_biserial(rows, n)} for n in nums], key=lambda x: abs(x["r"] or 0), reverse=True),
        "weather_code_lifts": sorted(code_lifts, key=lambda x: (x["lift"] or 0), reverse=True),
        "city_stats": sorted(city_stats, key=lambda x: (x["rate"], x["events"]), reverse=True),
        "top_events": sorted([r for r in rows if r.get("change_4c")], key=lambda r: r.get("abs_delta_high_c") or 0, reverse=True)[:80],
    }


async def main() -> None:
    end_dt = date.today() - timedelta(days=5)  # archive completeness guard
    start_dt = date(2023, 1, 1)
    start, end = start_dt.isoformat(), end_dt.isoformat()
    conn = aiohttp.TCPConnector(limit=8, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        pm_events_task = asyncio.create_task(collect_polymarket_events(session))
        coord_tasks = {city: asyncio.create_task(station_coords(session, city)) for city in sorted(STATIONS)}
        coords = {city: await task for city, task in coord_tasks.items()}
        missing = [c for c, v in coords.items() if not v]
        if missing:
            print("missing coords", missing)

        sem = asyncio.Semaphore(2)
        async def city_task(city: str) -> tuple[str, list[dict[str, Any]]]:
            c = coords[city]
            async with sem:
                data = await archive_city(session, city, c["lat"], c["lon"], start, end)
                rows = day_metrics(city, data)
                for r in rows:
                    r["lat"] = c["lat"]
                    r["lon"] = c["lon"]
                    r["coord_source"] = c.get("source")
                    r["station"] = STATIONS[city].station_id
                    r["source_class"] = STATIONS[city].source_class
                print("archive", city, len(rows))
                return city, rows

        all_rows: list[dict[str, Any]] = []
        for city, rows in await asyncio.gather(*(city_task(c) for c in sorted(STATIONS) if coords.get(c))):
            all_rows.extend(rows)
        pm_events = await pm_events_task

    pm_event_dates = {(e["city_slug"], e["date"]) for e in pm_events}
    pm_rows = [r for r in all_rows if (r["city_slug"], r["date"]) in pm_event_dates]
    pm_by_key = defaultdict(list)
    for e in pm_events:
        pm_by_key[(e["city_slug"], e["date"])].append(e)
    for r in all_rows:
        evs = pm_by_key.get((r["city_slug"], r["date"]), [])
        r["polymarket_event_count"] = len(evs)
        r["polymarket_markets_count"] = sum(e.get("markets_count") or 0 for e in evs)
        r["polymarket_volume"] = sum(e.get("volume") or 0 for e in evs)

    all_summary = summarize("all_open_meteo_city_days", all_rows, pm_event_dates)
    pm_summary = summarize("polymarket_event_days", pm_rows, pm_event_dates)
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "range": {"start": start, "end": end},
        "cities": sorted(STATIONS),
        "polymarket_events_collected": len(pm_events),
        "polymarket_city_dates": len(pm_event_dates),
        "summaries": {"all": all_summary, "polymarket_days": pm_summary},
        "polymarket_events": pm_events,
    }
    (OUT / "temp_4c_change_analysis.json").write_text(json.dumps(result, indent=2, default=str))

    fields = [
        "city_slug", "date", "station", "high_c", "delta_high_c", "abs_delta_high_c", "change_4c", "warmup_4c", "cooldown_4c",
        "weather_codes", "rain_any", "heavy_precip", "storm_code", "storm_or_convective", "snow_code", "windy", "very_windy", "front_proxy", "cloud_swing",
        "prev_rain_any", "prev_windy", "prev_front_proxy", "daily_precip_mm", "peak_precip_mm", "peak_rain_mm", "peak_showers_mm", "daily_snow_cm", "peak_snow_cm",
        "daily_gust_max_kmh", "peak_gust_max_kmh", "peak_wind_max_kmh", "peak_wind_shift_deg", "peak_pressure_range_hpa", "pressure_mean_delta_hpa", "peak_cloud_swing_pp", "peak_temp_range_c",
        "structural_risk_city", "coastal_or_marine", "interior_continental", "polymarket_event_count", "polymarket_markets_count", "polymarket_volume",
    ]
    write_csv(OUT / "city_day_features.csv", all_rows, fields)
    write_csv(OUT / "polymarket_event_day_features.csv", pm_rows, fields)
    write_csv(OUT / "flag_stats_all.csv", all_summary["flag_stats"], list(all_summary["flag_stats"][0].keys()))
    write_csv(OUT / "flag_stats_polymarket_days.csv", pm_summary["flag_stats"], list(pm_summary["flag_stats"][0].keys()) if pm_summary["flag_stats"] else ["flag"])
    write_csv(OUT / "city_stats_all.csv", all_summary["city_stats"], list(all_summary["city_stats"][0].keys()))
    write_csv(OUT / "weather_code_lifts_all.csv", all_summary["weather_code_lifts"], list(all_summary["weather_code_lifts"][0].keys()))

    def pct(x: float | None) -> str:
        return "n/a" if x is None else f"{100*x:.1f}%"
    lines = []
    lines.append("# >=4°C daily-high change correlation study")
    lines.append(f"Generated: {result['generated_at']}")
    lines.append(f"Range: {start} to {end}; cities={len(STATIONS)}")
    lines.append("")
    lines.append("## Dataset")
    lines.append(f"- Open-Meteo city-days with previous-day high: **{all_summary['n_city_days']}**")
    lines.append(f"- Polymarket closed high-temperature events collected: **{len(pm_events)}**")
    lines.append(f"- Matching Polymarket event city-days in archive range: **{pm_summary['n_city_days']}**")
    lines.append(f"- All-days >=4°C high changes: **{all_summary['n_4c_changes']}** ({pct(all_summary['base_rate'])}); warmups={all_summary['warmups_4c']}, cooldowns={all_summary['cooldowns_4c']}")
    lines.append(f"- Polymarket-days >=4°C high changes: **{pm_summary['n_4c_changes']}** ({pct(pm_summary['base_rate'])}); warmups={pm_summary['warmups_4c']}, cooldowns={pm_summary['cooldowns_4c']}")
    lines.append("")
    lines.append("Definition: target is `abs(today_daily_high_c - previous_day_daily_high_c) >= 4.0`. Features are mostly same-day peak-window 10:00–16:00 local, plus previous-day flags where noted.")
    lines.append("")
    lines.append("## Strongest binary flags — all city-days")
    for st in all_summary["flag_stats"][:15]:
        rr = "inf" if st["risk_ratio"] is None else f"{st['risk_ratio']:.2f}x"
        lines.append(f"- **{st['flag']}**: {pct(st['rate_yes'])} ({st['events_yes']}/{st['n_yes']}) vs {pct(st['rate_no'])} ({st['events_no']}/{st['n_no']}), RR={rr}, diff={st['rate_diff_pp']:.1f}pp, avg_abs_delta={st['avg_abs_delta_yes']:.2f}C vs {st['avg_abs_delta_no']:.2f}C")
    lines.append("")
    lines.append("## Numeric correlations with 4°C-change indicator — all city-days")
    for c in all_summary["numeric_correlations"][:12]:
        lines.append(f"- {c['feature']}: r={c['r']:.3f}" if c["r"] is not None else f"- {c['feature']}: n/a")
    lines.append("")
    lines.append("## Weather-code lifts — all city-days")
    for c in all_summary["weather_code_lifts"][:15]:
        lines.append(f"- code {c['code']}: rate={pct(c['rate'])} ({c['events']}/{c['n']}), lift={c['lift']:.2f}x")
    lines.append("")
    lines.append("## Highest-rate cities — all city-days")
    for c in all_summary["city_stats"][:20]:
        lines.append(f"- **{c['city_slug']}**: rate={pct(c['rate'])} ({c['events']}/{c['n']}), warmups={c['warmups']}, cooldowns={c['cooldowns']}, avg_abs_delta={c['avg_abs_delta']:.2f}C, max_abs_delta={c['max_abs_delta']:.1f}C, PM_days={c['pm_days']}")
    lines.append("")
    lines.append("## Largest individual >=4°C changes")
    for r in all_summary["top_events"][:30]:
        lines.append(f"- **{r['city_slug']} {r['date']}**: high={r['high_c']:.1f}C delta={r['delta_high_c']:+.1f}C codes={r.get('weather_codes')} rain={r.get('rain_any')} heavy={r.get('heavy_precip')} wind={r.get('windy')} front={r.get('front_proxy')} cloud_swing={r.get('cloud_swing')} gust={r.get('peak_gust_max_kmh')} pressure_rng={r.get('peak_pressure_range_hpa')} PM_events={r.get('polymarket_event_count')}")
    lines.append("")
    lines.append("## Files")
    lines.append(f"- JSON: `{OUT / 'temp_4c_change_analysis.json'}`")
    lines.append(f"- All city-day CSV: `{OUT / 'city_day_features.csv'}`")
    lines.append(f"- Polymarket event-day CSV: `{OUT / 'polymarket_event_day_features.csv'}`")
    lines.append(f"- Flag stats CSV: `{OUT / 'flag_stats_all.csv'}`")
    lines.append(f"- City stats CSV: `{OUT / 'city_stats_all.csv'}`")
    report = "\n".join(lines) + "\n"
    (OUT / "temp_4c_change_report.md").write_text(report)
    print(report)


if __name__ == "__main__":
    asyncio.run(main())
