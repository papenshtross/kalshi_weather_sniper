#!/usr/bin/env python3
"""Fast grouped city analysis for TWC forecast_high_c logs vs WU/METAR station actual highs.

Attempts 12-month scope logically by querying all available forecast logs; actual high
fetching is limited to the dates for which forecast logs exist, because archived
TWC forecasts are not publicly available except through our recorded logs.
"""
from __future__ import annotations
import asyncio, csv, json, math, os, re, sys, time, zipfile
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import asyncpg, requests
ROOT=Path(__file__).resolve().parents[1]; sys.path.insert(0,str(ROOT))
from polybot.live.weather_safety_filter import STATIONS
PUBLIC_WU_KEY="e1f10a1e78da46f5b10a1e78da96f525"
OUTDIR=ROOT/"reports"/"weather_forecast_accuracy_city_groups_latest"
# Statistics reports must use one fixed forecast horizon. Keep this at 24h so
# city groups and historical rows are comparable; do not mix 1/4/8/12/32/48h
# leads into aggregate accuracy.
LEADS=[24]
MONTHS={m.lower():i for i,m in enumerate(["January","February","March","April","May","June","July","August","September","October","November","December"],1)}
TARGET_RE=re.compile(r"on-([a-z]+)-(\d{1,2})-(\d{4})",re.I); FC_RE=re.compile(r"forecast_high_c=([-+]?\d+(?:\.\d+)?)")

def load_envs():
    for p in [ROOT/'.env', ROOT/'.env.live', ROOT.parent/'polybot-dash/.env.local']:
        if p.exists():
            for line in p.read_text().splitlines():
                if not line.strip() or line.lstrip().startswith('#') or '=' not in line: continue
                k,v=line.split('=',1); os.environ.setdefault(k.strip(), v.strip().strip('"\''))
    if 'POSTGRES_URL' not in os.environ:
        for k in ['DATABASE_URL','NAUTILUS_DB_URL']:
            if os.environ.get(k): os.environ['POSTGRES_URL']=os.environ[k]

def slug_from_strategy(s):
    s=s.replace('live_weather_outlier_sniper_','').replace('_auto_v1','').replace('_','-')
    return s if s in STATIONS else None

def target_date_from_msg(msg):
    m=TARGET_RE.search(msg or '')
    if not m: return None
    mo=MONTHS.get(m.group(1).lower())
    if not mo: return None
    try: return date(int(m.group(3)), mo, int(m.group(2)))
    except Exception: return None

def c_to_f(c): return c*9/5+32

def parse_time(x):
    try: return datetime.fromtimestamp(int(x),tz=timezone.utc)
    except Exception: return None

async def fetch_logs():
    con=await asyncpg.connect(os.environ['POSTGRES_URL'])
    bounds=await con.fetchrow("select min(ts) mn, max(ts) mx, count(*) n from strategy_logs where message like '%forecast_high_c=%'")
    rows=await con.fetch("select ts,strategy_id,message from strategy_logs where message like '%forecast_high_c=%' order by ts")
    await con.close()
    snaps=defaultdict(list); raw=0; target_dates=[]
    for r in rows:
        raw+=1; slug=slug_from_strategy(r['strategy_id'])
        if not slug: continue
        msg=r['message'] or ''; m=FC_RE.search(msg); td=target_date_from_msg(msg)
        if not m or not td: continue
        target_dates.append(td); snaps[(slug,td)].append({'ts':r['ts'],'forecast_high_c':float(m.group(1)),'strategy_id':r['strategy_id']})
    return snaps, dict(bounds), raw, target_dates

def loc_id_for_station(session, icao):
    r=session.get('https://api.weather.com/v3/location/point', params={'apiKey':PUBLIC_WU_KEY,'language':'en-US','icaoCode':icao,'format':'json'}, timeout=25)
    if r.status_code!=200: return None,None,f'location_point_http_{r.status_code}'
    loc=(r.json() or {}).get('location') or {}; tz=loc.get('ianaTimeZone'); cc=loc.get('countryCode')
    loc_id=f'{icao}:9:{cc}' if cc else None
    return loc_id,tz,None if loc_id and tz else 'missing_country_or_tz'

def fetch_actuals(session, loc_id, tzname, start, end):
    r=session.get(f'https://api.weather.com/v1/location/{loc_id}/observations/historical.json', params={'apiKey':PUBLIC_WU_KEY,'units':'m','startDate':start.strftime('%Y%m%d'),'endDate':(end+timedelta(days=1)).strftime('%Y%m%d')}, headers={'User-Agent':'Mozilla/5.0','Referer':'https://www.wunderground.com/'}, timeout=60)
    if r.status_code!=200: raise RuntimeError(f'historical_http_{r.status_code}')
    z=ZoneInfo(tzname); by=defaultdict(list)
    for o in (r.json() or {}).get('observations') or []:
        t=parse_time(o.get('valid_time_gmt')); temp=o.get('temp')
        if t is None or temp is None: continue
        lt=t.astimezone(z); d=lt.date()
        if start<=d<=end:
            try: by[d].append((float(temp),t,lt))
            except Exception: pass
    out={}
    for d,vals in by.items():
        high=max(v[0] for v in vals); hi=min([v for v in vals if v[0]==high], key=lambda v:v[1])
        out[d]={'actual_high_c':high,'actual_high_f':c_to_f(high),'actual_high_time_utc':hi[1],'actual_high_time_local':hi[2],'obs_count':len(vals)}
    return out

def pick(snaps, desired):
    if not snaps: return None
    best=min(snaps,key=lambda s:abs((s['ts']-desired).total_seconds()))
    delta=(best['ts']-desired).total_seconds()/3600
    if abs(delta)>2: return None
    return best|{'delta_to_requested_hours':delta}

def mean(xs): return sum(xs)/len(xs) if xs else None

def pctl(xs,q):
    xs=sorted(xs)
    if not xs: return None
    k=(len(xs)-1)*q; lo=math.floor(k); hi=math.ceil(k)
    return xs[lo] if lo==hi else xs[lo]*(hi-k)+xs[hi]*(k-lo)

def summarize(rows, keys):
    g=defaultdict(list)
    for r in rows: g[tuple(r[k] for k in keys)].append(r)
    out=[]
    for key,rs in g.items():
        errs=[r['error_c'] for r in rs]; ab=[abs(x) for x in errs]; pcts=[r['abs_pct_error'] for r in rs if r['abs_pct_error'] is not None]
        out.append({**{keys[i]:key[i] for i in range(len(keys))},'n':len(rs),'mae_c':mean(ab),'median_abs_c':pctl(ab,.5),'p90_abs_c':pctl(ab,.9),'bias_c':mean(errs),'overforecast_rate':mean([1 if x>0 else 0 for x in errs]),'underforecast_rate':mean([1 if x<0 else 0 for x in errs]),'within_1c_rate':mean([1 if abs(x)<=1 else 0 for x in errs]),'within_2c_rate':mean([1 if abs(x)<=2 else 0 for x in errs]),'mae_pct':mean(pcts),'median_abs_pct':pctl(pcts,.5),'p90_abs_pct':pctl(pcts,.9),'max_abs_c':max(ab) if ab else None})
    return out

def rr(r): return {k:(round(v,4) if isinstance(v,float) else v) for k,v in r.items()}

def group(mae,n):
    if n<10: return 'insufficient_sample'
    if mae<=0.75: return 'excellent_<=0.75C'
    if mae<=1.25: return 'good_0.75-1.25C'
    if mae<=1.75: return 'moderate_1.25-1.75C'
    if mae<=2.50: return 'high_deviation_1.75-2.50C'
    return 'very_high_deviation_>2.50C'

def write_csv(path, rows):
    if not rows: path.write_text(''); return
    with path.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)

async def main():
    load_envs(); OUTDIR.mkdir(parents=True, exist_ok=True)
    snaps,bounds,raw,target_dates=await fetch_logs()
    completed_end=datetime.now(timezone.utc).date()-timedelta(days=1)
    tds=[d for d in target_dates if d<=completed_end]
    start=min(tds); end=max(tds)
    session=requests.Session(); actuals={}; status=[]
    for slug,spec in [(s,sp) for s,sp in STATIONS.items() if sp.station_id!='HKO']:
        loc,tz,err=loc_id_for_station(session,spec.station_id); time.sleep(.03)
        if err: status.append({'city_slug':slug,'station':spec.station_id,'status':'loc_failed','actual_days':0,'error':err}); continue
        try:
            acts=fetch_actuals(session,loc,tz,start,end); actuals[slug]=acts
            status.append({'city_slug':slug,'station':spec.station_id,'loc_id':loc,'tz':tz,'status':'ok','actual_days':len(acts),'error':''})
        except Exception as e:
            status.append({'city_slug':slug,'station':spec.station_id,'loc_id':loc,'tz':tz,'status':'actual_failed','actual_days':0,'error':str(e)[:120]})
    rows=[]
    for slug,acts in actuals.items():
        spec=STATIONS[slug]
        for d,a in sorted(acts.items()):
            ss=snaps.get((slug,d),[])
            if not ss: continue
            for lead in LEADS:
                desired=a['actual_high_time_utc']-timedelta(hours=lead); s=pick(ss,desired)
                if not s: continue
                fc=s['forecast_high_c']; actual=a['actual_high_c']; err=fc-actual
                rows.append({'city_slug':slug,'city':spec.city,'station':spec.station_id,'target_date':d.isoformat(),'lead_hours_before_actual_high':lead,'requested_forecast_time_utc':desired.isoformat(),'forecast_snapshot_ts_utc':s['ts'].isoformat(),'snapshot_delta_minutes':round(s['delta_to_requested_hours']*60,2),'forecast_high_c':round(fc,3),'forecast_high_f':round(c_to_f(fc),3),'actual_high_c':round(actual,3),'actual_high_f':round(a['actual_high_f'],3),'actual_high_time_utc':a['actual_high_time_utc'].isoformat(),'actual_high_time_local':a['actual_high_time_local'].isoformat(),'actual_obs_count':a['obs_count'],'error_c':round(err,3),'abs_error_c':round(abs(err),3),'abs_pct_error':round(abs(err)/abs(actual)*100,3) if actual not in (0,None) else None,'over_under':'over' if err>0 else ('under' if err<0 else 'exact'),'strategy_id':s['strategy_id']})
    by_lead=[rr(x) for x in summarize(rows,['lead_hours_before_actual_high'])]
    by_city=[rr(x) for x in summarize(rows,['city_slug','station'])]
    by_city_lead=[rr(x) for x in summarize(rows,['city_slug','station','lead_hours_before_actual_high'])]
    for r in by_city: r['deviation_group']=group(r['mae_c'],r['n'])
    by_city=sorted(by_city,key=lambda r:(r['mae_c'],r['city_slug']))
    groups=[]
    for gname in ['excellent_<=0.75C','good_0.75-1.25C','moderate_1.25-1.75C','high_deviation_1.75-2.50C','very_high_deviation_>2.50C','insufficient_sample']:
        rs=[r for r in by_city if r['deviation_group']==gname]
        if rs: groups.append({'deviation_group':gname,'cities':len(rs),'sample_rows':sum(r['n'] for r in rs),'avg_city_mae_c':round(mean([r['mae_c'] for r in rs]),4),'median_city_mae_c':round(pctl([r['mae_c'] for r in rs],.5),4),'city_slugs':', '.join(r['city_slug'] for r in rs)})
    coverage={'generated_at_utc':datetime.now(timezone.utc).isoformat(),'requested_scope':'last 12 months if archived forecasts available; otherwise longest forecast-log-backed period possible','forecast_log_available_bounds':{k:(v.isoformat() if hasattr(v,'isoformat') else str(v)) for k,v in bounds.items()},'comparison_target_date_window':[start.isoformat(),end.isoformat()],'raw_forecast_log_rows':raw,'parsed_station_day_forecast_groups':len(snaps),'sample_rows':len(rows),'lead_counts':{str(l):sum(1 for r in rows if r['lead_hours_before_actual_high']==l) for l in LEADS},'station_status_counts':{k:sum(1 for s in status if s['status']==k) for k in sorted(set(s['status'] for s in status))},'caveat':'No 12-month archived TWC forecast snapshots are available from the public API; this audit uses the longest available recorded TWC forecast logs on this host (2026-05-28..2026-06-06). Actual station observations were fetched only for matching completed target dates.'}
    write_csv(OUTDIR/'twc_city_samples.csv',rows); write_csv(OUTDIR/'twc_by_lead.csv',sorted(by_lead,key=lambda r:r['lead_hours_before_actual_high'])); write_csv(OUTDIR/'twc_by_city.csv',by_city); write_csv(OUTDIR/'twc_by_city_lead.csv',by_city_lead); write_csv(OUTDIR/'twc_city_groups.csv',groups); write_csv(OUTDIR/'twc_station_status.csv',status)
    (OUTDIR/'coverage.json').write_text(json.dumps(coverage,indent=2))
    md=['# TWC forecast deviation by city groups','',f"Generated: `{coverage['generated_at_utc']}`",f"Forecast log bounds: `{coverage['forecast_log_available_bounds']['mn']}` → `{coverage['forecast_log_available_bounds']['mx']}`",f"Completed target-date comparison window: `{start}` → `{end}`",f"Samples: `{len(rows)}`",'', '## Caveat', coverage['caveat'],'','## Overall by lead']
    for r in sorted(by_lead,key=lambda r:r['lead_hours_before_actual_high']): md.append(f"- {r['lead_hours_before_actual_high']}h: n={r['n']}, MAE={r['mae_c']}°C, median={r['median_abs_c']}°C, p90={r['p90_abs_c']}°C, bias={r['bias_c']}°C, within1={r['within_1c_rate']:.1%}, within2={r['within_2c_rate']:.1%}")
    md+=['','## City groups']
    for g in groups: md += [f"### {g['deviation_group']}", f"- cities={g['cities']}, rows={g['sample_rows']}, avg_city_mae={g['avg_city_mae_c']}°C", f"- {g['city_slugs']}", '']
    md+=['## Per-city sorted by MAE']
    for r in by_city: md.append(f"- {r['city_slug']} ({r['station']}): {r['deviation_group']}; n={r['n']}; MAE={r['mae_c']}°C; p90={r['p90_abs_c']}°C; bias={r['bias_c']}°C; within2={r['within_2c_rate']:.1%}; max={r['max_abs_c']}°C")
    (OUTDIR/'report.md').write_text('\n'.join(md)+'\n')
    z=OUTDIR/'twc_city_deviation_groups_latest.zip'
    with zipfile.ZipFile(z,'w',zipfile.ZIP_DEFLATED) as zz:
        for p in OUTDIR.iterdir():
            if p.name!=z.name: zz.write(p,p.name)
    print('\n'.join(md[:120])); print('ZIP',z)
if __name__=='__main__': asyncio.run(main())
