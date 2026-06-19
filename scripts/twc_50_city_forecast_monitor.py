#!/usr/bin/env python3
"""Daily TWC 50-city forecast monitor.

Collects live TWC daily forecasts for all configured weather cities, stores them
locally, fetches final station/METAR-like TWC historical observation highs, scores
forecast-vs-actual errors, imports existing local forecast-log audit samples, and
prints a compact daily outlier/statistics update suitable for Hermes cron delivery.

This intentionally does not claim TWC has a public historical forecast archive: old
forecast-vs-actual rows can only come from previously recorded local forecast logs
or public Wayback/WU reconstructions. Forward data is collected here so future
analysis is owned locally.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sqlite3
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

DATA_DIR = ROOT / "data" / "twc_forecast_monitor"
DB_PATH = DATA_DIR / "twc_forecast_monitor.sqlite3"
REPORT_DIR = ROOT / "reports" / "twc_forecast_monitor"
PUBLIC_WU_KEY = "e1f10a1e78da46f5b10a1e78da96f525"
FORECAST_PRODUCT = "daily/15day"
OUTLIER_C = 3.0
HTTP_SLEEP_S = 0.08
REPORT_STATS_WINDOW_HOURS = 24
# Accuracy statistics intentionally use a fixed 24h forecast lead, not every
# forecast snapshot scored/imported during the reporting window. A ±1h tolerance
# keeps rows stable when actual-high timestamps or snapshot times are not exactly
# on the hour while excluding 48h/72h/88h-style leads from all reported stats.
REPORT_FIXED_LEAD_HOURS = 24.0
REPORT_FIXED_LEAD_TOLERANCE_HOURS = 1.0
# TWC v1 historical observations does not accept LTFM even though v3 location
# lookup/forecast does. LTBA is the working Istanbul TWC METAR/history fallback;
# reports surface this through actual_station_id.
ACTUAL_STATION_OVERRIDES = {"istanbul": "LTBA"}


def load_envs() -> None:
    for p in [ROOT / ".env", ROOT / ".env.live", ROOT.parent / "polybot-dash/.env.local"]:
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"\''))


def twc_key() -> str:
    return (
        os.getenv("TWC_API_KEY")
        or os.getenv("WEATHER_COMPANY_API_KEY")
        or os.getenv("WEATHERCOMPANY_API_KEY")
        or os.getenv("WEATHER_COM_API_KEY")
        or PUBLIC_WU_KEY
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(x: str | None) -> datetime | None:
    if not x:
        return None
    try:
        return datetime.fromisoformat(x.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_epoch(x: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(x), tz=timezone.utc)
    except Exception:
        return None


def c_to_f(c: float | None) -> float | None:
    return None if c is None else c * 9 / 5 + 32


def f_to_c(f: float | None) -> float | None:
    return None if f is None else (f - 32) * 5 / 9


def mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def pctl(xs: list[float], q: float) -> float | None:
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * q
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return xs[lo]
    return xs[lo] * (hi - k) + xs[hi] * (k - lo)


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS forecast_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            snapshot_ts_utc TEXT NOT NULL,
            city_slug TEXT NOT NULL,
            city TEXT NOT NULL,
            station_id TEXT NOT NULL,
            target_date TEXT NOT NULL,
            forecast_high_c REAL,
            forecast_high_f REAL,
            forecast_low_c REAL,
            forecast_low_f REAL,
            valid_time_local TEXT,
            expire_time_utc TEXT,
            raw_json TEXT,
            created_at_utc TEXT NOT NULL,
            UNIQUE(source, snapshot_ts_utc, city_slug, target_date)
        );
        CREATE TABLE IF NOT EXISTS actual_highs (
            city_slug TEXT NOT NULL,
            city TEXT NOT NULL,
            station_id TEXT NOT NULL,
            target_date TEXT NOT NULL,
            actual_high_c REAL,
            actual_high_f REAL,
            actual_high_time_utc TEXT,
            actual_high_time_local TEXT,
            obs_count INTEGER,
            loc_id TEXT,
            tz TEXT,
            source TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ok',
            error TEXT,
            PRIMARY KEY(city_slug, target_date, source)
        );
        CREATE TABLE IF NOT EXISTS scored_forecasts (
            source TEXT NOT NULL,
            snapshot_ts_utc TEXT NOT NULL,
            city_slug TEXT NOT NULL,
            city TEXT NOT NULL,
            station_id TEXT NOT NULL,
            target_date TEXT NOT NULL,
            lead_hours_before_actual_high REAL,
            forecast_high_c REAL,
            forecast_high_f REAL,
            actual_high_c REAL,
            actual_high_f REAL,
            actual_high_time_utc TEXT,
            error_c REAL,
            error_f REAL,
            abs_error_c REAL,
            abs_error_f REAL,
            scored_at_utc TEXT NOT NULL,
            first_scored_at_utc TEXT NOT NULL,
            PRIMARY KEY(source, snapshot_ts_utc, city_slug, target_date)
        );
        CREATE TABLE IF NOT EXISTS imported_historical_scores (
            source TEXT NOT NULL,
            snapshot_ts_utc TEXT NOT NULL,
            city_slug TEXT NOT NULL,
            city TEXT NOT NULL,
            station_id TEXT NOT NULL,
            target_date TEXT NOT NULL,
            lead_hours_before_actual_high REAL,
            forecast_high_c REAL,
            forecast_high_f REAL,
            actual_high_c REAL,
            actual_high_f REAL,
            actual_high_time_utc TEXT,
            error_c REAL,
            error_f REAL,
            abs_error_c REAL,
            abs_error_f REAL,
            imported_at_utc TEXT NOT NULL,
            PRIMARY KEY(source, snapshot_ts_utc, city_slug, target_date, lead_hours_before_actual_high)
        );
        CREATE TABLE IF NOT EXISTS run_log (
            run_ts_utc TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            details_json TEXT
        );
        """
    )
    conn.commit()


def loc_id_for_station(session: requests.Session, icao: str, key: str) -> tuple[str | None, str | None, str | None]:
    r = session.get(
        "https://api.weather.com/v3/location/point",
        params={"apiKey": key, "language": "en-US", "icaoCode": icao, "format": "json"},
        timeout=25,
    )
    if r.status_code != 200:
        return None, None, f"location_point_http_{r.status_code}"
    loc = (r.json() or {}).get("location") or {}
    tz = loc.get("ianaTimeZone")
    cc = loc.get("countryCode")
    loc_id = f"{icao}:9:{cc}" if cc else None
    return loc_id, tz, None if loc_id and tz else "missing_country_or_tz"


def fetch_forecast(session: requests.Session, key: str, slug: str, spec: Any) -> tuple[list[dict[str, Any]], str | None]:
    params = {"apiKey": key, "format": "json", "units": "m", "language": "en-US"}
    if spec.station_id != "HKO":
        params["icaoCode"] = spec.station_id
    elif spec.lat is not None and spec.lon is not None:
        params["geocode"] = f"{spec.lat:.4f},{spec.lon:.4f}"
    else:
        return [], "missing_station_or_geocode"
    r = session.get(f"https://api.weather.com/v3/wx/forecast/{FORECAST_PRODUCT}", params=params, timeout=30)
    if r.status_code != 200:
        return [], f"forecast_http_{r.status_code}:{r.text[:120]}"
    data = r.json() or {}
    highs = data.get("calendarDayTemperatureMax") or []
    lows = data.get("calendarDayTemperatureMin") or []
    valid = data.get("validTimeLocal") or []
    expire = data.get("expirationTimeUtc") or []
    rows: list[dict[str, Any]] = []
    for i, vt in enumerate(valid):
        d = str(vt or "")[:10]
        if not d:
            continue
        hi = highs[i] if i < len(highs) else None
        lo = lows[i] if i < len(lows) else None
        try:
            hi = float(hi) if hi is not None else None
        except Exception:
            hi = None
        try:
            lo = float(lo) if lo is not None else None
        except Exception:
            lo = None
        exp_dt = parse_epoch(expire[i]) if i < len(expire) else None
        rows.append(
            {
                "target_date": d,
                "forecast_high_c": hi,
                "forecast_high_f": c_to_f(hi),
                "forecast_low_c": lo,
                "forecast_low_f": c_to_f(lo),
                "valid_time_local": str(vt) if vt is not None else None,
                "expire_time_utc": exp_dt.isoformat() if exp_dt else None,
            }
        )
    return rows, None


def collect_forecasts(conn: sqlite3.Connection, session: requests.Session, key: str, snapshot_ts: datetime) -> dict[str, Any]:
    ok = 0
    failed = []
    inserted = 0
    for slug, spec in STATIONS.items():
        rows, err = fetch_forecast(session, key, slug, spec)
        time.sleep(HTTP_SLEEP_S)
        if err:
            failed.append({"city_slug": slug, "station": spec.station_id, "error": err})
            continue
        ok += 1
        raw = None  # avoid storing large duplicate payloads per row
        for row in rows:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO forecast_snapshots(
                    source, snapshot_ts_utc, city_slug, city, station_id, target_date,
                    forecast_high_c, forecast_high_f, forecast_low_c, forecast_low_f,
                    valid_time_local, expire_time_utc, raw_json, created_at_utc
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "twc_daily15_forward",
                    snapshot_ts.isoformat(),
                    slug,
                    spec.city,
                    spec.station_id,
                    row["target_date"],
                    row["forecast_high_c"],
                    row["forecast_high_f"],
                    row["forecast_low_c"],
                    row["forecast_low_f"],
                    row["valid_time_local"],
                    row["expire_time_utc"],
                    raw,
                    utcnow().isoformat(),
                ),
            )
            inserted += cur.rowcount
    conn.commit()
    return {"forecast_cities_ok": ok, "forecast_cities_failed": failed, "forecast_rows_inserted": inserted}


def fetch_actuals_for_city(session: requests.Session, key: str, slug: str, spec: Any, start: date, end: date) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    # The v1 WU/TWC historical observations endpoint is keyed differently from the
    # paid v3 forecast key; the public WU site key has been the reliable source for
    # station-history/METAR-like actuals in prior audits.
    hist_key = PUBLIC_WU_KEY
    if spec.station_id == "HKO":
        return {}, {"status": "skipped", "error": "HKO is not METAR/ICAO in TWC location_point; official HKO daily extract needed separately"}
    actual_station_id = ACTUAL_STATION_OVERRIDES.get(slug) or str(spec.station_id)
    loc_id, tzname, err = loc_id_for_station(session, actual_station_id, hist_key)
    time.sleep(HTTP_SLEEP_S)
    if err or not loc_id or not tzname:
        return {}, {"status": "loc_failed", "actual_station_id": actual_station_id, "loc_id": loc_id, "tz": tzname, "error": err}
    r = session.get(
        f"https://api.weather.com/v1/location/{loc_id}/observations/historical.json",
        params={"apiKey": hist_key, "units": "m", "startDate": start.strftime("%Y%m%d"), "endDate": (end + timedelta(days=1)).strftime("%Y%m%d")},
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.wunderground.com/"},
        timeout=60,
    )
    if r.status_code != 200:
        return {}, {"status": "actual_failed", "actual_station_id": actual_station_id, "loc_id": loc_id, "tz": tzname, "error": f"historical_http_{r.status_code}:{r.text[:120]}"}
    z = ZoneInfo(tzname)
    by: dict[date, list[tuple[float, datetime, datetime]]] = defaultdict(list)
    for o in (r.json() or {}).get("observations") or []:
        t = parse_epoch(o.get("valid_time_gmt"))
        temp = o.get("temp")
        if t is None or temp is None:
            continue
        try:
            temp_f = float(temp)
        except Exception:
            continue
        lt = t.astimezone(z)
        d = lt.date()
        if start <= d <= end:
            by[d].append((temp_f, t, lt))
    out: dict[str, dict[str, Any]] = {}
    for d, vals in by.items():
        high_c = max(v[0] for v in vals)
        hi = min([v for v in vals if v[0] == high_c], key=lambda v: v[1])
        out[d.isoformat()] = {
            "actual_high_c": high_c,
            "actual_high_f": c_to_f(high_c),
            "actual_high_time_utc": hi[1].isoformat(),
            "actual_high_time_local": hi[2].isoformat(),
            "obs_count": len(vals),
            "loc_id": loc_id,
            "tz": tzname,
            "actual_station_id": actual_station_id,
        }
    return out, {"status": "ok", "actual_station_id": actual_station_id, "loc_id": loc_id, "tz": tzname, "actual_days": len(out), "error": ""}


def update_actuals(conn: sqlite3.Connection, session: requests.Session, key: str, days_back: int) -> dict[str, Any]:
    today = utcnow().date()
    start = today - timedelta(days=days_back)
    end = today - timedelta(days=1)
    ok = 0
    statuses = []
    upserts = 0
    for slug, spec in STATIONS.items():
        actuals, st = fetch_actuals_for_city(session, key, slug, spec, start, end)
        statuses.append({"city_slug": slug, "station": spec.station_id, **st})
        if st.get("status") == "ok":
            ok += 1
        for d, a in actuals.items():
            conn.execute(
                """
                INSERT INTO actual_highs(city_slug, city, station_id, target_date, actual_high_c, actual_high_f,
                    actual_high_time_utc, actual_high_time_local, obs_count, loc_id, tz, source, updated_at_utc, status, error)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(city_slug, target_date, source) DO UPDATE SET
                    actual_high_c=excluded.actual_high_c, actual_high_f=excluded.actual_high_f,
                    actual_high_time_utc=excluded.actual_high_time_utc, actual_high_time_local=excluded.actual_high_time_local,
                    obs_count=excluded.obs_count, loc_id=excluded.loc_id, tz=excluded.tz,
                    updated_at_utc=excluded.updated_at_utc, status=excluded.status, error=excluded.error
                """,
                (slug, spec.city, a.get("actual_station_id") or spec.station_id, d, a["actual_high_c"], a["actual_high_f"], a["actual_high_time_utc"], a["actual_high_time_local"], a["obs_count"], a["loc_id"], a["tz"], "twc_historical_observations", utcnow().isoformat(), "ok", ""),
            )
            upserts += 1
    conn.commit()
    return {"actual_window": [start.isoformat(), end.isoformat()], "actual_cities_ok": ok, "actual_statuses": statuses, "actual_rows_upserted": upserts}


def score_forward(conn: sqlite3.Connection) -> dict[str, Any]:
    before = conn.total_changes
    rows = conn.execute(
        """
        SELECT f.source, f.snapshot_ts_utc, f.city_slug, f.city, f.station_id, f.target_date,
               f.forecast_high_c, f.forecast_high_f,
               a.actual_high_c, a.actual_high_f, a.actual_high_time_utc
        FROM forecast_snapshots f
        JOIN actual_highs a ON a.city_slug=f.city_slug AND a.target_date=f.target_date AND a.source='twc_historical_observations'
        WHERE f.forecast_high_c IS NOT NULL AND a.actual_high_c IS NOT NULL
        """
    ).fetchall()
    newly = []
    now = utcnow().isoformat()
    for r in rows:
        snap = parse_dt(r[1])
        actual_ts = parse_dt(r[10])
        if not snap or not actual_ts:
            continue
        forecast_c = float(r[6])
        actual_c = float(r[8])
        forecast_f = c_to_f(forecast_c)
        actual_f = c_to_f(actual_c)
        err_c = forecast_c - actual_c
        err_f = (forecast_f or 0.0) - (actual_f or 0.0)
        prior = conn.execute(
            "SELECT 1 FROM scored_forecasts WHERE source=? AND snapshot_ts_utc=? AND city_slug=? AND target_date=?",
            (r[0], r[1], r[2], r[5]),
        ).fetchone()
        first = now if prior is None else conn.execute(
            "SELECT first_scored_at_utc FROM scored_forecasts WHERE source=? AND snapshot_ts_utc=? AND city_slug=? AND target_date=?",
            (r[0], r[1], r[2], r[5]),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO scored_forecasts(source, snapshot_ts_utc, city_slug, city, station_id, target_date,
                lead_hours_before_actual_high, forecast_high_c, forecast_high_f, actual_high_c, actual_high_f,
                actual_high_time_utc, error_c, error_f, abs_error_c, abs_error_f, scored_at_utc, first_scored_at_utc)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source, snapshot_ts_utc, city_slug, target_date) DO UPDATE SET
                actual_high_c=excluded.actual_high_c, actual_high_f=excluded.actual_high_f,
                actual_high_time_utc=excluded.actual_high_time_utc, error_c=excluded.error_c,
                error_f=excluded.error_f, abs_error_c=excluded.abs_error_c, abs_error_f=excluded.abs_error_f,
                scored_at_utc=excluded.scored_at_utc
            """,
            (r[0], r[1], r[2], r[3], r[4], r[5], (actual_ts - snap).total_seconds() / 3600.0, forecast_c, forecast_f, actual_c, actual_f, r[10], err_c, err_f, abs(err_c), abs(err_f), now, first),
        )
        if prior is None:
            newly.append({"city_slug": r[2], "station": r[4], "target_date": r[5], "lead_h": (actual_ts - snap).total_seconds() / 3600.0, "forecast_c": forecast_c, "actual_c": actual_c, "error_c": err_c})
    conn.commit()
    return {"scored_rows_seen": len(rows), "newly_scored": newly, "db_changes": conn.total_changes - before}


def import_existing_report(conn: sqlite3.Connection) -> dict[str, Any]:
    p = ROOT / "reports" / "weather_forecast_accuracy_city_groups_latest" / "twc_city_samples.csv"
    if not p.exists():
        return {"imported_existing_rows": 0, "error": f"missing {p}"}
    n = 0
    now = utcnow().isoformat()
    with p.open(newline="") as f:
        for r in csv.DictReader(f):
            err_c = float(r["error_c"])
            forecast_c = float(r["forecast_high_c"])
            actual_c = float(r["actual_high_c"])
            forecast_f = c_to_f(forecast_c) or float(r["forecast_high_f"])
            actual_f = c_to_f(actual_c) or float(r["actual_high_f"])
            err_f = forecast_f - actual_f
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO imported_historical_scores(source, snapshot_ts_utc, city_slug, city, station_id, target_date,
                    lead_hours_before_actual_high, forecast_high_c, forecast_high_f, actual_high_c, actual_high_f,
                    actual_high_time_utc, error_c, error_f, abs_error_c, abs_error_f, imported_at_utc)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("local_strategy_logs", r["forecast_snapshot_ts_utc"], r["city_slug"], r["city"], r["station"], r["target_date"], float(r["lead_hours_before_actual_high"]), forecast_c, forecast_f, actual_c, actual_f, r["actual_high_time_utc"], err_c, err_f, abs(err_c), abs(err_f), now),
            )
            n += cur.rowcount
    conn.commit()
    return {"imported_existing_rows": n, "source_csv": str(p)}


def fixed_lead_bounds() -> tuple[float, float]:
    return (
        REPORT_FIXED_LEAD_HOURS - REPORT_FIXED_LEAD_TOLERANCE_HOURS,
        REPORT_FIXED_LEAD_HOURS + REPORT_FIXED_LEAD_TOLERANCE_HOURS,
    )


def union_rows(conn: sqlite3.Connection, stats_since_utc: datetime | None = None) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    lead_min, lead_max = fixed_lead_bounds()
    params: list[Any] = [lead_min, lead_max]
    where = "WHERE lead_hours_before_actual_high BETWEEN ? AND ?"
    if stats_since_utc is not None:
        where += " AND stats_event_ts_utc >= ?"
        params.append(stats_since_utc.isoformat())
    return conn.execute(
        """
        SELECT * FROM (
        SELECT 'forward_collected' AS dataset, source, snapshot_ts_utc, first_scored_at_utc AS stats_event_ts_utc,
               city_slug, city, station_id, target_date,
               lead_hours_before_actual_high, forecast_high_c, forecast_high_f, actual_high_c, actual_high_f,
               actual_high_time_utc, error_c, error_f, abs_error_c, abs_error_f
        FROM scored_forecasts
        UNION ALL
        SELECT 'historical_local_logs' AS dataset, source, snapshot_ts_utc, imported_at_utc AS stats_event_ts_utc,
               city_slug, city, station_id, target_date,
               lead_hours_before_actual_high, forecast_high_c, forecast_high_f, actual_high_c, actual_high_f,
               actual_high_time_utc, error_c, error_f, abs_error_c, abs_error_f
        FROM imported_historical_scores
        ) all_rows
        """
        + where,
        params,
    ).fetchall()


def summarize(rows: list[sqlite3.Row], key: str | None = None) -> list[dict[str, Any]]:
    groups: dict[Any, list[sqlite3.Row]] = defaultdict(list)
    for r in rows:
        groups[r[key] if key else "overall"].append(r)
    out = []
    for k, rs in groups.items():
        errs = [float(r["error_c"]) for r in rs if r["error_c"] is not None]
        ab = [abs(x) for x in errs]
        if not ab:
            continue
        out.append({"key": k, "n": len(ab), "mae_c": mean(ab), "p90_abs_c": pctl(ab, 0.9), "bias_c": mean(errs), "within2_rate": mean([1.0 if x <= 2 else 0.0 for x in ab]), "max_abs_c": max(ab)})
    return out


def write_outputs(conn: sqlite3.Connection, newly_scored: list[dict[str, Any]], run_details: dict[str, Any]) -> str:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_now = utcnow()
    stats_since = report_now - timedelta(hours=REPORT_STATS_WINDOW_HOURS)
    lead_min, lead_max = fixed_lead_bounds()
    rows = union_rows(conn, stats_since)
    overall = summarize(rows)[0] if rows else None
    by_city = sorted(summarize(rows, "city_slug"), key=lambda x: (-x["mae_c"], str(x["key"])))
    by_dataset = sorted(summarize(rows, "dataset"), key=lambda x: str(x["key"]))
    by_source = sorted(summarize(rows, "source"), key=lambda x: str(x["key"]))
    outliers_new = sorted(
        [
            x
            for x in newly_scored
            if abs(x["error_c"]) >= OUTLIER_C and lead_min <= float(x.get("lead_h", -9999.0)) <= lead_max
        ],
        key=lambda x: abs(x["error_c"]),
        reverse=True,
    )
    outliers_recent = conn.execute(
        """
        SELECT city_slug, station_id, target_date, lead_hours_before_actual_high, forecast_high_c, actual_high_c, error_c, abs_error_c, snapshot_ts_utc
        FROM scored_forecasts
        WHERE abs_error_c >= ? AND first_scored_at_utc >= ?
          AND lead_hours_before_actual_high BETWEEN ? AND ?
        ORDER BY target_date DESC, abs_error_c DESC
        LIMIT 20
        """,
        (OUTLIER_C, stats_since.isoformat(), lead_min, lead_max),
    ).fetchall()

    def fmt(x: float | None, nd: int = 2) -> str:
        return "n/a" if x is None else f"{x:.{nd}f}"

    lines = []
    lines.append("# TWC 50-city forecast monitor daily update")
    lines.append("")
    lines.append(f"Generated: `{report_now.isoformat()}`")
    lines.append(f"DB: `{DB_PATH}`")
    lines.append(f"Cities configured: `{len(STATIONS)}`")
    lines.append(f"Statistics window: last `{REPORT_STATS_WINDOW_HOURS}h` for rows first-scored/imported since `{stats_since.isoformat()}`; accuracy stats are fixed to `{REPORT_FIXED_LEAD_HOURS:.0f}h` forecast lead (`{lead_min:.0f}–{lead_max:.0f}h` before actual high).")
    lines.append("")
    lines.append("## Run status")
    lines.append(f"- Forecast collection: {run_details.get('forecast_cities_ok', 0)}/50 cities ok; rows inserted={run_details.get('forecast_rows_inserted', 0)}")
    lines.append(f"- Actual/METAR-like TWC history: {run_details.get('actual_cities_ok', 0)}/50 cities ok; rows upserted={run_details.get('actual_rows_upserted', 0)}")
    lines.append(f"- New scored forward rows: {len(newly_scored)}")
    if run_details.get("imported_existing_rows") is not None:
        lines.append(f"- Imported existing local forecast-log rows: {run_details.get('imported_existing_rows')}")
    lines.append("")
    lines.append(f"## Aggregate accuracy — last {REPORT_STATS_WINDOW_HOURS}h, fixed {REPORT_FIXED_LEAD_HOURS:.0f}h lead")
    if overall:
        mae_f_delta = (overall['mae_c'] * 1.8) if overall['mae_c'] is not None else None
        lines.append(f"- Combined rows: n={overall['n']}; MAE={fmt(overall['mae_c'])}°C/{fmt(mae_f_delta)}°F delta; p90={fmt(overall['p90_abs_c'])}°C; bias={fmt(overall['bias_c'])}°C; within2°C={overall['within2_rate']:.1%}; max_abs={fmt(overall['max_abs_c'])}°C")
    else:
        lines.append("- No forecast-vs-actual rows entered the reporting window yet; forward collection has started/continued.")
    lines.append("")
    lines.append(f"## Dataset/source breakdown — last {REPORT_STATS_WINDOW_HOURS}h, fixed {REPORT_FIXED_LEAD_HOURS:.0f}h lead")
    for r in by_dataset:
        lines.append(f"- {r['key']}: n={r['n']}; MAE={fmt(r['mae_c'])}°C; p90={fmt(r['p90_abs_c'])}°C; bias={fmt(r['bias_c'])}°C")
    for r in by_source:
        lines.append(f"- source={r['key']}: n={r['n']}; MAE={fmt(r['mae_c'])}°C; p90={fmt(r['p90_abs_c'])}°C")
    lines.append("")
    lines.append(f"## New substantial deviations today (abs error ≥ {OUTLIER_C}°C / {OUTLIER_C*1.8:.1f}°F)")
    if outliers_new:
        for x in outliers_new[:15]:
            lines.append(f"- {x['city_slug']} {x['station']} target={x['target_date']} lead={x['lead_h']:.1f}h forecast={x['forecast_c']:.1f}°C actual={x['actual_c']:.1f}°C error={x['error_c']:+.1f}°C")
    else:
        lines.append("- None newly scored in this run.")
    lines.append("")
    lines.append(f"## Worst city groups — last {REPORT_STATS_WINDOW_HOURS}h, fixed {REPORT_FIXED_LEAD_HOURS:.0f}h lead")
    for r in by_city[:12]:
        lines.append(f"- {r['key']}: n={r['n']}; MAE={fmt(r['mae_c'])}°C; p90={fmt(r['p90_abs_c'])}°C; bias={fmt(r['bias_c'])}°C; max={fmt(r['max_abs_c'])}°C")
    lines.append("")
    lines.append(f"## Recent forward-collection outliers — last {REPORT_STATS_WINDOW_HOURS}h, fixed {REPORT_FIXED_LEAD_HOURS:.0f}h lead")
    if outliers_recent:
        for r in outliers_recent[:12]:
            lines.append(f"- {r['city_slug']} {r['station_id']} target={r['target_date']} lead={float(r['lead_hours_before_actual_high']):.1f}h forecast={float(r['forecast_high_c']):.1f}°C actual={float(r['actual_high_c']):.1f}°C error={float(r['error_c']):+.1f}°C snapshot={r['snapshot_ts_utc']}")
    else:
        lines.append("- None yet from forward collection.")
    fail_forecast = run_details.get("forecast_cities_failed") or []
    actual_statuses = run_details.get("actual_statuses") or []
    fail_actual = [s for s in actual_statuses if s.get("status") != "ok"]
    if fail_forecast or fail_actual:
        lines.append("")
        lines.append("## Data-source exceptions")
        for f in fail_forecast[:10]:
            lines.append(f"- Forecast failed: {f.get('city_slug')} {f.get('station')}: {f.get('error')}")
        for f in fail_actual[:10]:
            lines.append(f"- Actual skipped/failed: {f.get('city_slug')} {f.get('station')}: {f.get('status')} {f.get('error','')}")
    lines.append("")
    lines.append("Files:")
    lines.append(f"- `{REPORT_DIR / 'daily_report.md'}`")
    lines.append(f"- `{DB_PATH}`")
    text = "\n".join(lines) + "\n"
    (REPORT_DIR / "daily_report.md").write_text(text)
    (REPORT_DIR / "run_details.json").write_text(json.dumps(run_details, indent=2, default=str))
    return text


def run(args: argparse.Namespace) -> str:
    load_envs()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    key = twc_key()
    session = requests.Session()
    details: dict[str, Any] = {"run_ts_utc": utcnow().isoformat(), "forecast_product": FORECAST_PRODUCT, "db_path": str(DB_PATH)}
    if args.import_existing:
        details.update(import_existing_report(conn))
    if not args.import_only:
        snapshot_ts = utcnow().replace(microsecond=0)
        details.update(collect_forecasts(conn, session, key, snapshot_ts))
        details.update(update_actuals(conn, session, key, args.actual_days_back))
        scored = score_forward(conn)
        details.update({k: v for k, v in scored.items() if k != "newly_scored"})
        newly = scored["newly_scored"]
    else:
        newly = []
    report = write_outputs(conn, newly, details)
    conn.execute("INSERT OR REPLACE INTO run_log(run_ts_utc, mode, status, details_json) VALUES(?,?,?,?)", (details["run_ts_utc"], "import_only" if args.import_only else "daily", "ok", json.dumps(details, default=str)))
    conn.commit()
    conn.close()
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--import-existing", action="store_true", help="Import latest local strategy-log forecast accuracy CSV into the monitor DB")
    ap.add_argument("--import-only", action="store_true", help="Only import existing rows and render report; do not call TWC APIs")
    ap.add_argument("--actual-days-back", type=int, default=21, help="How many previous local dates to refresh actual highs for")
    args = ap.parse_args()
    print(run(args))


if __name__ == "__main__":
    main()
