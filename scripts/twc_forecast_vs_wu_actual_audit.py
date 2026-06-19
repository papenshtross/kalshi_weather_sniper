#!/usr/bin/env python3
"""Audit recorded TWC forecast highs in strategy_logs vs Wunderground/Weather.com historical actual station observations.

Read-only. Does not print secrets. Writes CSV artifacts under reports/weather_forecast_accuracy/.
"""
from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import re
import statistics as stats
import sys
import time
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from polybot.live.weather_safety_filter import STATIONS  # noqa: E402

PUBLIC_WU_KEY = "e1f10a1e78da46f5b10a1e78da96f525"
OUTDIR = ROOT / "reports" / "weather_forecast_accuracy"
LEADS = [48, 32, 24, 12, 8, 4, 1]
MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], start=1
)}
TARGET_RE = re.compile(r"on-([a-z]+)-(\d{1,2})-(\d{4})", re.I)
FC_RE = re.compile(r"forecast_high_c=([-+]?\d+(?:\.\d+)?)")


def slug_from_strategy(s: str) -> str | None:
    s = s.replace("live_weather_outlier_sniper_", "").replace("_auto_v1", "").replace("_", "-")
    return s if s in STATIONS else None


def target_date_from_msg(msg: str) -> date | None:
    m = TARGET_RE.search(msg)
    if not m:
        return None
    mo = MONTHS.get(m.group(1).lower())
    if not mo:
        return None
    return date(int(m.group(3)), mo, int(m.group(2)))


def parse_dt_utc_from_valid_time_gmt(x) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(x), tz=timezone.utc)
    except Exception:
        return None


def c_to_f(c: float) -> float:
    return c * 9 / 5 + 32


def loc_id_for_station(session: requests.Session, icao: str) -> tuple[str | None, str | None, str | None]:
    r = session.get(
        "https://api.weather.com/v3/location/point",
        params={"apiKey": PUBLIC_WU_KEY, "language": "en-US", "icaoCode": icao, "format": "json"},
        timeout=25,
    )
    if r.status_code != 200:
        return None, None, f"location_point_http_{r.status_code}"
    loc = (r.json() or {}).get("location") or {}
    tz = loc.get("ianaTimeZone")
    cc = loc.get("countryCode")
    # IMPORTANT: v3 location/point often returns city/IATA locId (e.g. TLV/AMS/GRU)
    # that is not the settlement METAR station and can produce nonsense actuals.
    # Wunderground/Weather.com historical station observations accept ICAO:9:<countryCode>
    # for the airport-backed METAR stations that work, so force that identity.
    loc_id = f"{icao}:9:{cc}" if cc else None
    return loc_id, tz, None if loc_id and tz else "missing_country_or_tz"


def fetch_actuals(session: requests.Session, loc_id: str, tzname: str, start: date, end: date) -> dict[date, dict]:
    # TWC historical obs endpoint returns hourly-ish Weather.com/WU observations for a date range.
    r = session.get(
        f"https://api.weather.com/v1/location/{loc_id}/observations/historical.json",
        params={"apiKey": PUBLIC_WU_KEY, "units": "m", "startDate": start.strftime("%Y%m%d"), "endDate": (end + timedelta(days=1)).strftime("%Y%m%d")},
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.wunderground.com/"},
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"historical_http_{r.status_code}")
    obs = (r.json() or {}).get("observations") or []
    z = ZoneInfo(tzname)
    byday = defaultdict(list)
    for o in obs:
        t = parse_dt_utc_from_valid_time_gmt(o.get("valid_time_gmt"))
        temp = o.get("temp")
        if t is None or temp is None:
            continue
        lt = t.astimezone(z)
        d = lt.date()
        if start <= d <= end:
            try:
                byday[d].append((float(temp), t, lt, o))
            except Exception:
                pass
    out = {}
    for d, vals in byday.items():
        if not vals:
            continue
        high = max(v[0] for v in vals)
        # first timestamp attaining daily high
        high_items = [v for v in vals if v[0] == high]
        high_item = min(high_items, key=lambda v: v[1])
        out[d] = {
            "actual_high_c": high,
            "actual_high_f": c_to_f(high),
            "actual_high_time_utc": high_item[1],
            "actual_high_time_local": high_item[2],
            "obs_count": len(vals),
            "source_loc_id": loc_id,
            "tz": tzname,
        }
    return out


async def fetch_logs(start_ts: datetime, end_ts: datetime):
    conn = await asyncpg.connect(os.environ["POSTGRES_URL"])
    rows = await conn.fetch(
        """
        select ts, strategy_id, message
        from strategy_logs
        where ts >= $1 and ts <= $2 and message like '%forecast_high_c=%'
        order by ts
        """,
        start_ts,
        end_ts,
    )
    await conn.close()
    snapshots = defaultdict(list)
    for r in rows:
        slug = slug_from_strategy(r["strategy_id"])
        if not slug:
            continue
        m = FC_RE.search(r["message"] or "")
        td = target_date_from_msg(r["message"] or "")
        if not m or not td:
            continue
        snapshots[(slug, td)].append({"ts": r["ts"], "forecast_high_c": float(m.group(1)), "strategy_id": r["strategy_id"]})
    return snapshots, len(rows)


def pick_snapshot(snaps, target_ts: datetime):
    if not snaps:
        return None
    best = min(snaps, key=lambda s: abs((s["ts"] - target_ts).total_seconds()))
    age_hours = (best["ts"] - target_ts).total_seconds() / 3600.0
    if abs(age_hours) > 2.0:  # strict enough for sparse outages but avoids false matches
        return None
    return best | {"delta_to_requested_hours": age_hours}


def mean(xs): return sum(xs)/len(xs) if xs else None

def pct(x): return 100*x if x is not None else None

def pctl(xs, q):
    xs = sorted(xs)
    if not xs: return None
    k = (len(xs)-1)*q
    lo = math.floor(k); hi = math.ceil(k)
    if lo == hi: return xs[lo]
    return xs[lo]*(hi-k)+xs[hi]*(k-lo)


def summarize(rows, keys):
    groups = defaultdict(list)
    for r in rows:
        groups[tuple(r[k] for k in keys)].append(r)
    out=[]
    for key, rs in groups.items():
        errs=[r['error_c'] for r in rs]
        abss=[abs(x) for x in errs]
        pcts=[r['abs_pct_error'] for r in rs if r['abs_pct_error'] is not None]
        out.append({
            **{keys[i]: key[i] for i in range(len(keys))},
            'n': len(rs),
            'mae_c': mean(abss),
            'median_abs_c': pctl(abss, .5),
            'p90_abs_c': pctl(abss, .9),
            'bias_c': mean(errs),
            'overforecast_rate': mean([1 if x>0 else 0 for x in errs]),
            'underforecast_rate': mean([1 if x<0 else 0 for x in errs]),
            'within_1c_rate': mean([1 if abs(x)<=1 else 0 for x in errs]),
            'within_2c_rate': mean([1 if abs(x)<=2 else 0 for x in errs]),
            'mae_pct': mean(pcts),
            'median_abs_pct': pctl(pcts, .5),
            'p90_abs_pct': pctl(pcts, .9),
            'max_abs_c': max(abss) if abss else None,
        })
    return out


def write_csv(path: Path, rows: list[dict]):
    if not rows:
        path.write_text("")
        return
    fields = list(rows[0].keys())
    with path.open('w', newline='') as f:
        w=csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)


async def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    actual_end = (now.date() - timedelta(days=1))
    actual_start = actual_end - timedelta(days=29)
    log_start = datetime.combine(actual_start - timedelta(days=3), datetime.min.time(), timezone.utc)
    logs, raw_log_rows = await fetch_logs(log_start, now)

    session = requests.Session()
    session.headers.update({'User-Agent':'Mozilla/5.0'})
    stations = [(slug, spec) for slug, spec in STATIONS.items() if spec.station_id != 'HKO']
    actuals_by = {}
    station_status=[]
    for i,(slug,spec) in enumerate(stations,1):
        loc_id, tzname, err = loc_id_for_station(session, spec.station_id)
        time.sleep(0.08)
        if err:
            station_status.append({'city_slug':slug,'station':spec.station_id,'status':'loc_failed','error':err})
            continue
        try:
            acts = fetch_actuals(session, loc_id, tzname, actual_start, actual_end)
            actuals_by[slug]=acts
            station_status.append({'city_slug':slug,'station':spec.station_id,'loc_id':loc_id,'tz':tzname,'status':'ok','actual_days':len(acts),'error':''})
        except Exception as e:
            station_status.append({'city_slug':slug,'station':spec.station_id,'loc_id':loc_id,'tz':tzname,'status':'actual_failed','actual_days':0,'error':str(e)[:120]})
        time.sleep(0.12)

    sample_rows=[]
    for slug, acts in actuals_by.items():
        spec=STATIONS[slug]
        for d, a in sorted(acts.items()):
            snaps = logs.get((slug,d), [])
            for lead in LEADS:
                desired = a['actual_high_time_utc'] - timedelta(hours=lead)
                s = pick_snapshot(snaps, desired)
                if not s:
                    continue
                fc=s['forecast_high_c']; actual=a['actual_high_c']
                err=fc-actual
                sample_rows.append({
                    'city_slug':slug,
                    'city':spec.city,
                    'station':spec.station_id,
                    'target_date':d.isoformat(),
                    'lead_hours_before_actual_high':lead,
                    'requested_forecast_time_utc':desired.isoformat(),
                    'forecast_snapshot_ts_utc':s['ts'].isoformat(),
                    'snapshot_delta_minutes':round(s['delta_to_requested_hours']*60,2),
                    'forecast_high_c':round(fc,3),
                    'forecast_high_f':round(c_to_f(fc),3),
                    'actual_high_c':round(actual,3),
                    'actual_high_f':round(a['actual_high_f'],3),
                    'actual_high_time_utc':a['actual_high_time_utc'].isoformat(),
                    'actual_high_time_local':a['actual_high_time_local'].isoformat(),
                    'actual_obs_count':a['obs_count'],
                    'error_c':round(err,3),
                    'abs_error_c':round(abs(err),3),
                    'abs_pct_error':round(abs(err)/abs(actual)*100,3) if actual not in (0,None) else None,
                    'over_under': 'over' if err>0 else ('under' if err<0 else 'exact'),
                    'strategy_id':s['strategy_id'],
                })
    rows=sample_rows
    # create summary rows with rounded floats
    def rounded_summary(summary):
        out=[]
        for r in summary:
            rr={}
            for k,v in r.items():
                if isinstance(v,float): rr[k]=round(v,4)
                else: rr[k]=v
            out.append(rr)
        return out
    by_station_lead=rounded_summary(summarize(rows,['city_slug','station','lead_hours_before_actual_high']))
    by_lead=rounded_summary(summarize(rows,['lead_hours_before_actual_high']))
    by_station=rounded_summary(summarize(rows,['city_slug','station']))
    write_csv(OUTDIR/'twc_forecast_vs_wu_actual_samples.csv', rows)
    write_csv(OUTDIR/'twc_forecast_vs_wu_actual_by_station_lead.csv', by_station_lead)
    write_csv(OUTDIR/'twc_forecast_vs_wu_actual_by_lead.csv', sorted(by_lead, key=lambda r:r['lead_hours_before_actual_high']))
    write_csv(OUTDIR/'twc_forecast_vs_wu_actual_by_station.csv', by_station)
    write_csv(OUTDIR/'twc_forecast_vs_wu_station_status.csv', station_status)
    # coverage report
    coverage = {
        'generated_at_utc': now.isoformat(),
        'requested_actual_window': [actual_start.isoformat(), actual_end.isoformat()],
        'forecast_log_query_start_utc': log_start.isoformat(),
        'raw_forecast_log_rows': raw_log_rows,
        'parsed_station_day_forecast_groups': len(logs),
        'tracked_metar_stations_ex_hko': len(stations),
        'station_status_counts': dict((k, sum(1 for s in station_status if s['status']==k)) for k in sorted(set(s['status'] for s in station_status))),
        'sample_rows': len(rows),
        'complete_station_lead_groups': len(by_station_lead),
        'lead_counts': {str(lead): sum(1 for r in rows if r['lead_hours_before_actual_high']==lead) for lead in LEADS},
        'files': [str(p) for p in sorted(OUTDIR.glob('twc_forecast_vs_wu_*'))],
        'important_caveat': 'Forecast logs only start 2026-05-28 19:42 UTC in strategy_logs, so the requested 30-day forecast-at-lead audit cannot be fully reconstructed unless older forecast snapshots exist elsewhere. Actual WU/Weather.com observations were fetched for the full requested 30 completed days where the endpoint allowed it.',
    }
    (OUTDIR/'twc_forecast_vs_wu_coverage.json').write_text(json.dumps(coverage, indent=2))
    print(json.dumps(coverage, indent=2))
    print('\nBY_LEAD')
    for r in sorted(by_lead, key=lambda r:r['lead_hours_before_actual_high']):
        print(r)
    print('\nSAMPLE_ROWS')
    for r in rows[:12]:
        print(r)

if __name__ == '__main__':
    asyncio.run(main())
