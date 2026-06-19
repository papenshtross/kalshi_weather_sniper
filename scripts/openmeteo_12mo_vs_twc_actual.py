#!/usr/bin/env python3
"""Compare Open-Meteo 12-month historical forecasts against TWC station actual highs.

This is a read-only analysis. It uses Open-Meteo's historical-forecast archive
for forecast daily max temperature and TWC/Wunderground v1 historical observations
for final station/METAR-like actual highs. TWC does not expose 12-month archived
forecast snapshots publicly, so this compares Open-Meteo historical forecasts to
TWC actual highs, then places that next to the shorter local TWC forecast-log audit
where available.
"""
from __future__ import annotations

import csv
import json
import math
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from polybot.live.weather_safety_filter import STATIONS  # noqa: E402

PUBLIC_WU_KEY = "e1f10a1e78da46f5b10a1e78da96f525"
OUTDIR = ROOT / "reports" / "openmeteo_12mo_vs_twc_actual"
ACTUAL_STATION_OVERRIDES = {"istanbul": "LTBA"}
HTTP_SLEEP_S = 0.02


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def c_to_f(c: float | None) -> float | None:
    return None if c is None else c * 9 / 5 + 32


def parse_epoch(x: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(x), tz=timezone.utc)
    except Exception:
        return None


def mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def pctl(xs: list[float], q: float) -> float | None:
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * q
    lo = math.floor(k)
    hi = math.ceil(k)
    return xs[lo] if lo == hi else xs[lo] * (hi - k) + xs[hi] * (k - lo)


def rr(x: float | None, nd: int = 4) -> float | None:
    return None if x is None else round(x, nd)


def loc_id_for_station(session: requests.Session, icao: str) -> tuple[str | None, str | None, float | None, float | None, str | None]:
    r = session.get(
        "https://api.weather.com/v3/location/point",
        params={"apiKey": PUBLIC_WU_KEY, "language": "en-US", "icaoCode": icao, "format": "json"},
        timeout=25,
    )
    if r.status_code != 200:
        return None, None, None, None, f"location_point_http_{r.status_code}:{r.text[:120]}"
    loc = (r.json() or {}).get("location") or {}
    tz = loc.get("ianaTimeZone")
    cc = loc.get("countryCode")
    lat = loc.get("latitude")
    lon = loc.get("longitude")
    try:
        lat = float(lat) if lat is not None else None
        lon = float(lon) if lon is not None else None
    except Exception:
        lat = lon = None
    loc_id = f"{icao}:9:{cc}" if cc else None
    return loc_id, tz, lat, lon, None if loc_id and tz and lat is not None and lon is not None else "missing_location_fields"


def fetch_twc_actuals(session: requests.Session, loc_id: str, tzname: str, start: date, end: date) -> dict[str, dict[str, Any]]:
    z = ZoneInfo(tzname)
    out_vals: dict[date, list[tuple[float, datetime, datetime]]] = defaultdict(list)
    cur = start
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=29))
        r = session.get(
            f"https://api.weather.com/v1/location/{loc_id}/observations/historical.json",
            params={"apiKey": PUBLIC_WU_KEY, "units": "m", "startDate": cur.strftime("%Y%m%d"), "endDate": chunk_end.strftime("%Y%m%d")},
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.wunderground.com/"},
            timeout=60,
        )
        if r.status_code != 200:
            raise RuntimeError(f"twc_actual_http_{r.status_code}:{r.text[:160]}")
        for o in (r.json() or {}).get("observations") or []:
            t = parse_epoch(o.get("valid_time_gmt"))
            temp = o.get("temp")
            if t is None or temp is None:
                continue
            try:
                temp_c = float(temp)
            except Exception:
                continue
            lt = t.astimezone(z)
            d = lt.date()
            if start <= d <= end:
                out_vals[d].append((temp_c, t, lt))
        cur = chunk_end + timedelta(days=1)
        time.sleep(HTTP_SLEEP_S)
    out: dict[str, dict[str, Any]] = {}
    for d, vals in out_vals.items():
        high = max(v[0] for v in vals)
        hi = min([v for v in vals if v[0] == high], key=lambda v: v[1])
        out[d.isoformat()] = {
            "actual_high_c": high,
            "actual_high_f": c_to_f(high),
            "actual_high_time_utc": hi[1].isoformat(),
            "actual_high_time_local": hi[2].isoformat(),
            "obs_count": len(vals),
        }
    return out


def fetch_openmeteo_forecast(session: requests.Session, lat: float, lon: float, start: date, end: date, *, model: str | None = None) -> tuple[dict[str, float], dict[str, Any]]:
    params: dict[str, Any] = {
        "latitude": f"{lat:.5f}",
        "longitude": f"{lon:.5f}",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": "temperature_2m_max",
        "temperature_unit": "celsius",
        "timezone": "auto",
    }
    if model:
        params["models"] = model
    r = session.get("https://historical-forecast-api.open-meteo.com/v1/forecast", params=params, timeout=90)
    if r.status_code != 200:
        raise RuntimeError(f"openmeteo_http_{r.status_code}:{r.text[:200]}")
    data = r.json() or {}
    daily = data.get("daily") or {}
    times = daily.get("time") or []
    vals = daily.get("temperature_2m_max") or []
    out = {}
    for d, v in zip(times, vals):
        if v is None:
            continue
        try:
            out[str(d)] = float(v)
        except Exception:
            pass
    meta = {k: data.get(k) for k in ["latitude", "longitude", "timezone", "elevation", "generationtime_ms"]}
    return out, meta


def summarize(rows: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[tuple(r[k] for k in keys)].append(r)
    out = []
    for key, rs in groups.items():
        errs = [float(r["error_c"]) for r in rs]
        ab = [abs(x) for x in errs]
        out.append({
            **{keys[i]: key[i] for i in range(len(keys))},
            "n": len(rs),
            "mae_c": rr(mean(ab)),
            "mae_f_delta": rr((mean(ab) or 0) * 1.8),
            "median_abs_c": rr(pctl(ab, 0.5)),
            "p90_abs_c": rr(pctl(ab, 0.9)),
            "p95_abs_c": rr(pctl(ab, 0.95)),
            "bias_c": rr(mean(errs)),
            "overforecast_rate": rr(mean([1.0 if x > 0 else 0.0 for x in errs])),
            "underforecast_rate": rr(mean([1.0 if x < 0 else 0.0 for x in errs])),
            "within_1c_rate": rr(mean([1.0 if x <= 1 else 0.0 for x in ab])),
            "within_2c_rate": rr(mean([1.0 if x <= 2 else 0.0 for x in ab])),
            "within_3c_rate": rr(mean([1.0 if x <= 3 else 0.0 for x in ab])),
            "max_abs_c": rr(max(ab)),
        })
    return out


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames: list[str] = []
    for row in rows:
        for k in row.keys():
            if k not in fieldnames:
                fieldnames.append(k)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def load_twc_local_summary() -> dict[str, Any] | None:
    p = ROOT / "reports" / "weather_forecast_accuracy_city_groups_latest" / "twc_by_lead.csv"
    cov = ROOT / "reports" / "weather_forecast_accuracy_city_groups_latest" / "coverage.json"
    if not p.exists():
        return None
    rows = list(csv.DictReader(p.open()))
    return {"coverage": json.loads(cov.read_text()) if cov.exists() else None, "by_lead": rows}


def main() -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    end = utcnow().date() - timedelta(days=1)
    start = end - timedelta(days=364)
    rows: list[dict[str, Any]] = []
    statuses: list[dict[str, Any]] = []
    location_cache: dict[str, Any] = {}

    for slug, spec in STATIONS.items():
        forecast_station = str(spec.station_id)
        actual_station = ACTUAL_STATION_OVERRIDES.get(slug) or forecast_station
        if forecast_station == "HKO":
            # Forecast coordinates can be geocode; no TWC METAR-like actual source for HKO in this endpoint.
            lat, lon = spec.lat, spec.lon
            statuses.append({"city_slug": slug, "forecast_station": forecast_station, "actual_station": "", "status": "skipped_actual", "error": "HKO actual not available from TWC METAR-like history endpoint"})
            continue
        try:
            loc_id, tz, lat, lon, loc_err = loc_id_for_station(session, forecast_station)
            time.sleep(HTTP_SLEEP_S)
            if loc_err:
                raise RuntimeError(f"forecast_location:{loc_err}")
            actual_loc_id, actual_tz, _alat, _alon, actual_loc_err = loc_id_for_station(session, actual_station)
            time.sleep(HTTP_SLEEP_S)
            if actual_loc_err:
                raise RuntimeError(f"actual_location:{actual_loc_err}")
            assert loc_id and tz and lat is not None and lon is not None and actual_loc_id and actual_tz
            om, om_meta = fetch_openmeteo_forecast(session, lat, lon, start, end)
            time.sleep(HTTP_SLEEP_S)
            actuals = fetch_twc_actuals(session, actual_loc_id, actual_tz, start, end)
            location_cache[slug] = {"forecast_loc_id": loc_id, "actual_loc_id": actual_loc_id, "forecast_station": forecast_station, "actual_station": actual_station, "lat": lat, "lon": lon, "tz": tz, "actual_tz": actual_tz, "openmeteo_meta": om_meta}
            n_join = 0
            for d, fc in sorted(om.items()):
                a = actuals.get(d)
                if not a:
                    continue
                err = fc - float(a["actual_high_c"])
                dd = date.fromisoformat(d)
                rows.append({
                    "city_slug": slug,
                    "city": spec.city,
                    "forecast_station": forecast_station,
                    "actual_station": actual_station,
                    "target_date": d,
                    "month": d[:7],
                    "openmeteo_forecast_high_c": round(fc, 3),
                    "openmeteo_forecast_high_f": round(c_to_f(fc) or 0, 3),
                    "twc_actual_high_c": round(float(a["actual_high_c"]), 3),
                    "twc_actual_high_f": round(float(a["actual_high_f"]), 3),
                    "twc_actual_high_time_utc": a["actual_high_time_utc"],
                    "twc_actual_obs_count": a["obs_count"],
                    "error_c": round(err, 3),
                    "error_f_delta": round(err * 1.8, 3),
                    "abs_error_c": round(abs(err), 3),
                    "over_under": "over" if err > 0 else ("under" if err < 0 else "exact"),
                    "latitude": lat,
                    "longitude": lon,
                })
                n_join += 1
            statuses.append({"city_slug": slug, "forecast_station": forecast_station, "actual_station": actual_station, "status": "ok", "openmeteo_days": len(om), "twc_actual_days": len(actuals), "joined_days": n_join, "error": ""})
            print(f"OK {slug} joined={n_join}", flush=True)
        except Exception as e:
            statuses.append({"city_slug": slug, "forecast_station": forecast_station, "actual_station": actual_station, "status": "failed", "error": str(e)[:240]})
            print(f"FAIL {slug}: {e}", flush=True)

    by_city = sorted(summarize(rows, ["city_slug", "forecast_station", "actual_station"]), key=lambda r: (-(r["mae_c"] or 0), r["city_slug"]))
    by_month = sorted(summarize(rows, ["month"]), key=lambda r: r["month"])
    by_city_month = sorted(summarize(rows, ["city_slug", "month"]), key=lambda r: (r["city_slug"], r["month"]))
    overall = summarize(rows, [])[0] if rows else None
    outliers = sorted([r for r in rows if r["abs_error_c"] >= 5.0], key=lambda r: r["abs_error_c"], reverse=True)
    twc_local = load_twc_local_summary()

    write_csv(OUTDIR / "openmeteo_vs_twc_actual_samples.csv", rows)
    write_csv(OUTDIR / "openmeteo_by_city.csv", by_city)
    write_csv(OUTDIR / "openmeteo_by_month.csv", by_month)
    write_csv(OUTDIR / "openmeteo_by_city_month.csv", by_city_month)
    write_csv(OUTDIR / "openmeteo_large_outliers.csv", outliers)
    write_csv(OUTDIR / "station_status.csv", statuses)
    coverage = {
        "generated_at_utc": utcnow().isoformat(),
        "window": [start.isoformat(), end.isoformat()],
        "source_forecast": "Open-Meteo historical-forecast-api daily temperature_2m_max, default model/best_match",
        "source_actual": "TWC/Wunderground v1 historical station observations, max temp by local station date",
        "configured_cities": len(STATIONS),
        "scored_rows": len(rows),
        "ok_cities": sum(1 for s in statuses if s["status"] == "ok"),
        "statuses": statuses,
        "location_cache": location_cache,
        "twc_local_forecast_log_comparison": twc_local,
        "caveat": "This is not TWC historical forecast-vs-Open-Meteo forecast because public TWC does not expose 12-month archived forecast snapshots. It compares Open-Meteo historical forecasts to TWC final station actual highs, and includes the shorter local TWC forecast-log audit as context.",
    }
    (OUTDIR / "coverage.json").write_text(json.dumps(coverage, indent=2, default=str))

    def fmt(x: Any, nd: int = 2) -> str:
        try:
            if x is None:
                return "n/a"
            return f"{float(x):.{nd}f}"
        except Exception:
            return str(x)

    lines = [
        "# Open-Meteo 12-month historical forecast vs TWC station actual highs",
        "",
        f"Generated: `{coverage['generated_at_utc']}`",
        f"Window: `{start}` → `{end}`",
        f"Scored rows: `{len(rows)}` across `{coverage['ok_cities']}` cities",
        "",
        "## Important caveat",
        coverage["caveat"],
        "",
        "## Open-Meteo vs TWC actual overall",
    ]
    if overall:
        lines.append(f"- n={overall['n']}; MAE={fmt(overall['mae_c'])}°C/{fmt(overall['mae_f_delta'])}°F delta; median={fmt(overall['median_abs_c'])}°C; p90={fmt(overall['p90_abs_c'])}°C; p95={fmt(overall['p95_abs_c'])}°C; bias={fmt(overall['bias_c'])}°C; within2°C={float(overall['within_2c_rate']):.1%}; max={fmt(overall['max_abs_c'])}°C")
    lines += ["", "## Worst cities by MAE"]
    for r in by_city[:15]:
        lines.append(f"- {r['city_slug']} ({r['forecast_station']}→{r['actual_station']}): n={r['n']}; MAE={fmt(r['mae_c'])}°C; p90={fmt(r['p90_abs_c'])}°C; bias={fmt(r['bias_c'])}°C; within2={float(r['within_2c_rate']):.1%}; max={fmt(r['max_abs_c'])}°C")
    lines += ["", "## Best cities by MAE"]
    for r in sorted(by_city, key=lambda r: (r["mae_c"] or 999, r["city_slug"]))[:12]:
        lines.append(f"- {r['city_slug']} ({r['forecast_station']}): n={r['n']}; MAE={fmt(r['mae_c'])}°C; p90={fmt(r['p90_abs_c'])}°C; bias={fmt(r['bias_c'])}°C")
    lines += ["", "## Monthly aggregate"]
    for r in by_month:
        lines.append(f"- {r['month']}: n={r['n']}; MAE={fmt(r['mae_c'])}°C; p90={fmt(r['p90_abs_c'])}°C; bias={fmt(r['bias_c'])}°C; within2={float(r['within_2c_rate']):.1%}")
    lines += ["", "## Largest Open-Meteo vs TWC actual deviations"]
    for r in outliers[:20]:
        lines.append(f"- {r['target_date']} {r['city_slug']} {r['forecast_station']}→{r['actual_station']}: OM={r['openmeteo_forecast_high_c']}°C TWC_actual={r['twc_actual_high_c']}°C error={r['error_c']:+.1f}°C")
    if twc_local:
        cov = twc_local.get("coverage") or {}
        lines += ["", "## Context: shorter local TWC forecast-log audit"]
        lines.append(f"- Local TWC forecast-log window: `{(cov.get('forecast_log_available_bounds') or {}).get('mn')}` → `{(cov.get('forecast_log_available_bounds') or {}).get('mx')}`")
        lines.append(f"- Local TWC samples: `{cov.get('sample_rows')}`")
        for r in twc_local.get("by_lead") or []:
            lines.append(f"- TWC log {r.get('lead_hours_before_actual_high')}h: n={r.get('n')}; MAE={r.get('mae_c')}°C; p90={r.get('p90_abs_c')}°C; bias={r.get('bias_c')}°C")
    lines += ["", "Files:", f"- `{OUTDIR / 'report.md'}`", f"- `{OUTDIR / 'openmeteo_vs_twc_actual_samples.csv'}`", f"- `{OUTDIR / 'openmeteo_by_city.csv'}`", f"- `{OUTDIR / 'coverage.json'}`"]
    report = "\n".join(lines) + "\n"
    (OUTDIR / "report.md").write_text(report)
    print(report)


if __name__ == "__main__":
    main()
