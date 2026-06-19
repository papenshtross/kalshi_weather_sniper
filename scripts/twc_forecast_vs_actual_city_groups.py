#!/usr/bin/env python3
"""Extended TWC forecast-vs-WU/METAR station actual audit grouped by city deviation.

Read-only. Uses recorded TWC forecast_high_c values in strategy_logs, then compares
chosen lead snapshots to Weather.com/Wunderground historical station daily highs.
Attempts a 12-month actual-observation window, chunked to avoid Weather.com
historical endpoint range limits. Forecast comparison period is constrained by
available strategy_logs forecast snapshots.
"""
from __future__ import annotations

import asyncio, csv, json, math, os, re, statistics as stats, sys, time, zipfile
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
OUTDIR = ROOT / "reports" / "weather_forecast_accuracy_12mo_city_groups"
# Statistics reports must use one fixed forecast horizon. Keep this at 24h so
# city groups and historical rows are comparable; do not mix 1/4/8/12/32/48h
# leads into aggregate accuracy.
LEADS = [24]
MONTHS = {m.lower(): i for i, m in enumerate(["January","February","March","April","May","June","July","August","September","October","November","December"], start=1)}
TARGET_RE = re.compile(r"on-([a-z]+)-(\d{1,2})-(\d{4})", re.I)
FC_RE = re.compile(r"forecast_high_c=([-+]?\d+(?:\.\d+)?)")


def load_envs():
    for p in [ROOT/'.env', ROOT/'.env.live', ROOT.parent/'polybot-dash/.env.local']:
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            if not line.strip() or line.lstrip().startswith('#') or '=' not in line:
                continue
            k,v=line.split('=',1)
            os.environ.setdefault(k.strip(), v.strip().strip('"\''))
    if 'POSTGRES_URL' not in os.environ:
        for k in ['DATABASE_URL','NAUTILUS_DB_URL']:
            if os.environ.get(k):
                os.environ['POSTGRES_URL']=os.environ[k]
                break


def slug_from_strategy(s: str) -> str | None:
    s = s.replace("live_weather_outlier_sniper_", "").replace("_auto_v1", "").replace("_", "-")
    return s if s in STATIONS else None


def target_date_from_msg(msg: str) -> date | None:
    m = TARGET_RE.search(msg)
    if not m: return None
    mo = MONTHS.get(m.group(1).lower())
    if not mo: return None
    try: return date(int(m.group(3)), mo, int(m.group(2)))
    except Exception: return None


def parse_dt_utc_from_valid_time_gmt(x) -> datetime | None:
    try: return datetime.fromtimestamp(int(x), tz=timezone.utc)
    except Exception: return None


def c_to_f(c: float) -> float: return c*9/5+32


def loc_id_for_station(session: requests.Session, icao: str):
    r=session.get('https://api.weather.com/v3/location/point', params={'apiKey':PUBLIC_WU_KEY,'language':'en-US','icaoCode':icao,'format':'json'}, timeout=25)
    if r.status_code != 200: return None, None, f'location_point_http_{r.status_code}'
    loc=(r.json() or {}).get('location') or {}; tz=loc.get('ianaTimeZone'); cc=loc.get('countryCode')
    loc_id=f'{icao}:9:{cc}' if cc else None
    return loc_id, tz, None if loc_id and tz else 'missing_country_or_tz'


def fetch_actuals_range(session: requests.Session, loc_id: str, tzname: str, start: date, end: date) -> dict[date, dict]:
    out={}; z=ZoneInfo(tzname); cur=start
    while cur <= end:
        chunk_end=min(cur+timedelta(days=29), end)
        r=session.get(
            f'https://api.weather.com/v1/location/{loc_id}/observations/historical.json',
            params={'apiKey':PUBLIC_WU_KEY,'units':'m','startDate':cur.strftime('%Y%m%d'),'endDate':(chunk_end+timedelta(days=1)).strftime('%Y%m%d')},
            headers={'User-Agent':'Mozilla/5.0','Referer':'https://www.wunderground.com/'}, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f'historical_http_{r.status_code}_chunk_{cur}_{chunk_end}')
        byday=defaultdict(list)
        for o in (r.json() or {}).get('observations') or []:
            t=parse_dt_utc_from_valid_time_gmt(o.get('valid_time_gmt')); temp=o.get('temp')
            if t is None or temp is None: continue
            lt=t.astimezone(z); d=lt.date()
            if cur <= d <= chunk_end:
                try: byday[d].append((float(temp),t,lt,o))
                except Exception: pass
        for d, vals in byday.items():
            if not vals: continue
            high=max(v[0] for v in vals); hi=min([v for v in vals if v[0]==high], key=lambda v:v[1])
            out[d]={'actual_high_c':high,'actual_high_f':c_to_f(high),'actual_high_time_utc':hi[1],'actual_high_time_local':hi[2],'obs_count':len(vals),'source_loc_id':loc_id,'tz':tzname}
        cur=chunk_end+timedelta(days=1)
        time.sleep(0.05)
    return out


async def fetch_logs(start_ts: datetime, end_ts: datetime):
    conn=await asyncpg.connect(os.environ['POSTGRES_URL'])
    bounds=await conn.fetchrow("select min(ts) mn, max(ts) mx, count(*) n from strategy_logs where message like '%forecast_high_c=%'")
    rows=await conn.fetch("""
        select ts, strategy_id, message
        from strategy_logs
        where ts >= $1 and ts <= $2 and message like '%forecast_high_c=%'
        order by ts
    """, start_ts, end_ts)
    await conn.close()
    snapshots=defaultdict(list)
    for r in rows:
        slug=slug_from_strategy(r['strategy_id'])
        if not slug: continue
        msg=r['message'] or ''; m=FC_RE.search(msg); td=target_date_from_msg(msg)
        if not m or not td: continue
        snapshots[(slug,td)].append({'ts':r['ts'], 'forecast_high_c':float(m.group(1)), 'strategy_id':r['strategy_id']})
    return snapshots, len(rows), dict(bounds)


def pick_snapshot(snaps, target_ts: datetime):
    if not snaps: return None
    best=min(snaps, key=lambda s: abs((s['ts']-target_ts).total_seconds()))
    age=(best['ts']-target_ts).total_seconds()/3600.0
    if abs(age)>2.0: return None
    return best | {'delta_to_requested_hours':age}


def mean(xs): return sum(xs)/len(xs) if xs else None

def pctl(xs,q):
    xs=sorted(xs)
    if not xs: return None
    k=(len(xs)-1)*q; lo=math.floor(k); hi=math.ceil(k)
    return xs[lo] if lo==hi else xs[lo]*(hi-k)+xs[hi]*(k-lo)


def summarize(rows, keys):
    groups=defaultdict(list)
    for r in rows: groups[tuple(r[k] for k in keys)].append(r)
    out=[]
    for key,rs in groups.items():
        errs=[r['error_c'] for r in rs]; abss=[abs(x) for x in errs]; pcts=[r['abs_pct_error'] for r in rs if r['abs_pct_error'] is not None]
        out.append({**{keys[i]:key[i] for i in range(len(keys))}, 'n':len(rs), 'mae_c':mean(abss), 'median_abs_c':pctl(abss,.5), 'p90_abs_c':pctl(abss,.9), 'bias_c':mean(errs), 'overforecast_rate':mean([1 if x>0 else 0 for x in errs]), 'underforecast_rate':mean([1 if x<0 else 0 for x in errs]), 'within_1c_rate':mean([1 if abs(x)<=1 else 0 for x in errs]), 'within_2c_rate':mean([1 if abs(x)<=2 else 0 for x in errs]), 'mae_pct':mean(pcts), 'median_abs_pct':pctl(pcts,.5), 'p90_abs_pct':pctl(pcts,.9), 'max_abs_c':max(abss) if abss else None})
    return out


def round_row(r):
    return {k:(round(v,4) if isinstance(v,float) else v) for k,v in r.items()}


def group_for(mae, n):
    if n < 10: return 'insufficient_sample'
    if mae <= 0.75: return 'excellent_<=0.75C_MAE'
    if mae <= 1.25: return 'good_0.75-1.25C_MAE'
    if mae <= 1.75: return 'moderate_1.25-1.75C_MAE'
    if mae <= 2.50: return 'high_deviation_1.75-2.50C_MAE'
    return 'very_high_deviation_>2.50C_MAE'


def write_csv(path: Path, rows: list[dict]):
    if not rows: path.write_text(''); return
    fields=list(rows[0].keys())
    with path.open('w', newline='') as f:
        w=csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)


async def main():
    load_envs(); OUTDIR.mkdir(parents=True, exist_ok=True)
    now=datetime.now(timezone.utc); actual_end=now.date()-timedelta(days=1); actual_start=actual_end-timedelta(days=364)
    logs, raw_log_rows, log_bounds = await fetch_logs(datetime.combine(actual_start-timedelta(days=3), datetime.min.time(), timezone.utc), now)
    session=requests.Session(); session.headers.update({'User-Agent':'Mozilla/5.0'})
    stations=[(slug,spec) for slug,spec in STATIONS.items() if spec.station_id!='HKO']
    actuals_by={}; station_status=[]
    for i,(slug,spec) in enumerate(stations,1):
        loc_id,tzname,err=loc_id_for_station(session,spec.station_id); time.sleep(0.04)
        if err:
            station_status.append({'city_slug':slug,'station':spec.station_id,'status':'loc_failed','actual_days':0,'error':err}); continue
        try:
            acts=fetch_actuals_range(session,loc_id,tzname,actual_start,actual_end)
            actuals_by[slug]=acts
            station_status.append({'city_slug':slug,'station':spec.station_id,'loc_id':loc_id,'tz':tzname,'status':'ok','actual_days':len(acts),'error':''})
        except Exception as e:
            station_status.append({'city_slug':slug,'station':spec.station_id,'loc_id':loc_id,'tz':tzname,'status':'actual_failed','actual_days':0,'error':str(e)[:200]})
        print(f'{i}/{len(stations)} {slug} {station_status[-1]["status"]} days={station_status[-1].get("actual_days",0)}', flush=True)
    rows=[]
    for slug,acts in actuals_by.items():
        spec=STATIONS[slug]
        for d,a in sorted(acts.items()):
            snaps=logs.get((slug,d), [])
            if not snaps: continue
            for lead in LEADS:
                desired=a['actual_high_time_utc']-timedelta(hours=lead)
                s=pick_snapshot(snaps, desired)
                if not s: continue
                fc=s['forecast_high_c']; actual=a['actual_high_c']; err=fc-actual
                rows.append({'city_slug':slug,'city':spec.city,'station':spec.station_id,'target_date':d.isoformat(),'lead_hours_before_actual_high':lead,'requested_forecast_time_utc':desired.isoformat(),'forecast_snapshot_ts_utc':s['ts'].isoformat(),'snapshot_delta_minutes':round(s['delta_to_requested_hours']*60,2),'forecast_high_c':round(fc,3),'forecast_high_f':round(c_to_f(fc),3),'actual_high_c':round(actual,3),'actual_high_f':round(a['actual_high_f'],3),'actual_high_time_utc':a['actual_high_time_utc'].isoformat(),'actual_high_time_local':a['actual_high_time_local'].isoformat(),'actual_obs_count':a['obs_count'],'error_c':round(err,3),'abs_error_c':round(abs(err),3),'abs_pct_error':round(abs(err)/abs(actual)*100,3) if actual not in (0,None) else None,'over_under':'over' if err>0 else ('under' if err<0 else 'exact'),'strategy_id':s['strategy_id']})
    by_lead=[round_row(r) for r in summarize(rows,['lead_hours_before_actual_high'])]
    by_station=[round_row(r) for r in summarize(rows,['city_slug','station'])]
    by_station_lead=[round_row(r) for r in summarize(rows,['city_slug','station','lead_hours_before_actual_high'])]
    for r in by_station:
        r['deviation_group']=group_for(r['mae_c'], r['n'])
    grouped=defaultdict(list)
    for r in by_station: grouped[r['deviation_group']].append(r)
    group_summary=[]
    for g,rs in grouped.items():
        group_summary.append({'deviation_group':g,'cities':len(rs),'sample_rows':sum(r['n'] for r in rs),'avg_city_mae_c':round(mean([r['mae_c'] for r in rs]),4),'median_city_mae_c':round(pctl([r['mae_c'] for r in rs],.5),4),'city_slugs':', '.join(sorted(r['city_slug'] for r in rs))})
    group_summary=sorted(group_summary, key=lambda r: ['excellent_<=0.75C_MAE','good_0.75-1.25C_MAE','moderate_1.25-1.75C_MAE','high_deviation_1.75-2.50C_MAE','very_high_deviation_>2.50C_MAE','insufficient_sample'].index(r['deviation_group']) if r['deviation_group'] in ['excellent_<=0.75C_MAE','good_0.75-1.25C_MAE','moderate_1.25-1.75C_MAE','high_deviation_1.75-2.50C_MAE','very_high_deviation_>2.50C_MAE','insufficient_sample'] else 99)
    by_station=sorted(by_station, key=lambda r:(r['deviation_group'], r['mae_c']))
    write_csv(OUTDIR/'twc_forecast_vs_actual_samples.csv', rows)
    write_csv(OUTDIR/'twc_forecast_vs_actual_by_lead.csv', sorted(by_lead,key=lambda r:r['lead_hours_before_actual_high']))
    write_csv(OUTDIR/'twc_forecast_vs_actual_by_city.csv', by_station)
    write_csv(OUTDIR/'twc_forecast_vs_actual_by_city_lead.csv', by_station_lead)
    write_csv(OUTDIR/'twc_forecast_vs_actual_city_groups.csv', group_summary)
    write_csv(OUTDIR/'twc_forecast_vs_actual_station_status.csv', station_status)
    coverage={'generated_at_utc':now.isoformat(),'requested_actual_window':[actual_start.isoformat(),actual_end.isoformat()],'forecast_log_available_bounds':{k:(v.isoformat() if hasattr(v,'isoformat') else str(v)) for k,v in log_bounds.items()},'raw_forecast_log_rows_in_query':raw_log_rows,'parsed_station_day_forecast_groups':len(logs),'tracked_metar_stations_ex_hko':len(stations),'station_status_counts':{k:sum(1 for s in station_status if s['status']==k) for k in sorted(set(s['status'] for s in station_status))},'sample_rows':len(rows),'lead_counts':{str(lead):sum(1 for r in rows if r['lead_hours_before_actual_high']==lead) for lead in LEADS},'actual_days_ok_total':sum(s.get('actual_days',0) for s in station_status if s['status']=='ok'),'analysis_caveat':'Attempted 12 months of Weather.com/WU station actuals. Actual observations were available for most stations, but forecast-vs-actual comparison is limited by recorded TWC forecast_high_c logs, available only from 2026-05-28 to 2026-06-06 on this system. Weather.com does not provide archived forecast snapshots via the public endpoint used here; only our logs can supply forecast-at-lead.'}
    (OUTDIR/'twc_forecast_vs_actual_coverage.json').write_text(json.dumps(coverage,indent=2))
    md=['# TWC forecast high vs station actual high — city deviation groups','',f"Generated: `{coverage['generated_at_utc']}`",'',f"Requested actual window: `{coverage['requested_actual_window'][0]}` to `{coverage['requested_actual_window'][1]}`",f"Forecast log bounds: `{coverage['forecast_log_available_bounds'].get('mn')}` to `{coverage['forecast_log_available_bounds'].get('mx')}`",f"Sample rows: `{len(rows)}`",'', '## Important caveat', coverage['analysis_caveat'], '', '## Overall by lead','']
    for r in sorted(by_lead,key=lambda r:r['lead_hours_before_actual_high']):
        md.append(f"- {r['lead_hours_before_actual_high']}h: n={r['n']}, MAE={r['mae_c']}°C, median={r['median_abs_c']}°C, p90={r['p90_abs_c']}°C, bias={r['bias_c']}°C, within1C={r['within_1c_rate']:.1%}, within2C={r['within_2c_rate']:.1%}")
    md += ['', '## City groups by overall MAE','']
    for g in group_summary:
        md.append(f"### {g['deviation_group']}")
        md.append(f"- cities={g['cities']}, rows={g['sample_rows']}, avg_city_mae={g['avg_city_mae_c']}°C, median_city_mae={g['median_city_mae_c']}°C")
        md.append(f"- {g['city_slugs']}")
        md.append('')
    md += ['## Per-city summary sorted by MAE','']
    for r in sorted(by_station, key=lambda r:r['mae_c']):
        md.append(f"- {r['city_slug']} ({r['station']}): group={r['deviation_group']}, n={r['n']}, MAE={r['mae_c']}°C, median={r['median_abs_c']}°C, p90={r['p90_abs_c']}°C, bias={r['bias_c']}°C, within2C={r['within_2c_rate']:.1%}, max={r['max_abs_c']}°C")
    (OUTDIR/'twc_forecast_vs_actual_city_group_report.md').write_text('\n'.join(md)+'\n')
    zpath=OUTDIR/'twc_forecast_vs_actual_city_groups_audit.zip'
    with zipfile.ZipFile(zpath,'w',zipfile.ZIP_DEFLATED) as z:
        for p in OUTDIR.glob('twc_forecast_vs_actual_*'):
            if p.name != zpath.name: z.write(p, arcname=p.name)
    print(json.dumps(coverage,indent=2))
    print('\n'.join(md[:80]))
    print('ZIP', zpath)

if __name__=='__main__':
    asyncio.run(main())
