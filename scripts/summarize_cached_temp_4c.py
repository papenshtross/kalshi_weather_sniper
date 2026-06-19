#!/usr/bin/env python3
from __future__ import annotations
import json, re, math, csv, sys
from pathlib import Path
from collections import defaultdict
import numpy as np

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from polybot.live.weather_safety_filter import RAIN_CODES, HEAVY_RAIN_CODES, SEVERE_CODES, SNOW_CODES, STRUCTURAL_RISK, STATIONS

CACHE=ROOT/'reports/temp_4c_change_analysis/cache'
OUT=ROOT/'reports/temp_4c_change_analysis'
OUT.mkdir(parents=True,exist_ok=True)
COASTAL_OR_MARINE=set('nyc seattle miami houston los-angeles san-francisco london hong-kong seoul shanghai singapore paris buenos-aires wellington jakarta tokyo helsinki taipei amsterdam milan toronto shenzhen kuala-lumpur sao-paulo manila guangzhou karachi busan jeddah panama-city qingdao cape-town'.split())

def safe(x):
    try:
        if x is None: return None
        y=float(x); return None if math.isnan(y) else y
    except Exception: return None

def circ(vals):
    vals=[float(x)%360 for x in vals if x is not None]
    if not vals: return None
    vals=sorted(vals); gaps=[vals[i+1]-vals[i] for i in range(len(vals)-1)]+[vals[0]+360-vals[-1]]
    return 360-max(gaps)

def avg(xs):
    xs=[x for x in xs if x is not None]
    return sum(xs)/len(xs) if xs else None

def day_rows(city,data):
    daily=data.get('daily') or {}; hourly=data.get('hourly') or {}; days=daily.get('time') or []; htimes=hourly.get('time') or []
    by=defaultdict(list)
    for i,t in enumerate(htimes): by[str(t)[:10]].append(i)
    rows=[]
    for idx,d in enumerate(days):
        ixall=by.get(str(d),[]); ix=[i for i in ixall if 10<=int(str(htimes[i])[11:13])<=16] or ixall
        def darr(k):
            a=daily.get(k) or []; return a[idx] if idx<len(a) else None
        def hvals(k,ixs=ix):
            a=hourly.get(k) or []; out=[]
            for i in ixs:
                if i<len(a):
                    v=safe(a[i])
                    if v is not None: out.append(v)
            return out
        codes=set(int(x) for x in hvals('weather_code'))
        dc=safe(darr('weather_code'))
        if dc is not None: codes.add(int(dc))
        cloud=hvals('cloud_cover'); pressure=hvals('surface_pressure'); wind=hvals('wind_speed_10m'); gust=hvals('wind_gusts_10m')
        precip=hvals('precipitation'); rain=hvals('rain'); showers=hvals('showers'); snow=hvals('snowfall'); temps=hvals('temperature_2m')
        r={'city_slug':city,'date':str(d),'high_c':safe(darr('temperature_2m_max')),'low_c':safe(darr('temperature_2m_min')),
           'daily_precip_mm':safe(darr('precipitation_sum')),'daily_rain_mm':safe(darr('rain_sum')),'daily_showers_mm':safe(darr('showers_sum')),'daily_snow_cm':safe(darr('snowfall_sum')),
           'daily_wind_max_kmh':safe(darr('wind_speed_10m_max')),'daily_gust_max_kmh':safe(darr('wind_gusts_10m_max')),
           'peak_precip_mm':sum(precip) if precip else 0.0,'peak_rain_mm':sum(rain) if rain else 0.0,'peak_showers_mm':sum(showers) if showers else 0.0,'peak_snow_cm':sum(snow) if snow else 0.0,
           'peak_cloud_swing_pp':(max(cloud)-min(cloud)) if len(cloud)>=2 else None,'peak_pressure_range_hpa':(max(pressure)-min(pressure)) if len(pressure)>=2 else None,
           'peak_wind_max_kmh':max(wind) if wind else None,'peak_gust_max_kmh':max(gust) if gust else None,'peak_wind_shift_deg':circ(hvals('wind_direction_10m')),
           'peak_temp_range_c':(max(temps)-min(temps)) if len(temps)>=2 else None,'weather_codes':sorted(codes),'structural_risk_city':city in STRUCTURAL_RISK,'coastal_or_marine':city in COASTAL_OR_MARINE}
        r['rain_code']=bool(codes&RAIN_CODES); r['heavy_rain_code']=bool(codes&HEAVY_RAIN_CODES); r['storm_code']=bool(codes&SEVERE_CODES); r['snow_code']=bool(codes&SNOW_CODES)
        r['rain_any']=bool(r['rain_code'] or (r['daily_precip_mm'] or 0)>0 or (r['peak_precip_mm'] or 0)>0)
        r['heavy_precip']=bool(r['heavy_rain_code'] or (r['daily_precip_mm'] or 0)>=10 or (r['peak_precip_mm'] or 0)>=3)
        r['windy']=bool((r['peak_gust_max_kmh'] or r['daily_gust_max_kmh'] or 0)>=45 or (r['peak_wind_max_kmh'] or 0)>=30)
        r['very_windy']=bool((r['peak_gust_max_kmh'] or r['daily_gust_max_kmh'] or 0)>=60 or (r['peak_wind_max_kmh'] or 0)>=40)
        r['front_proxy']=bool((r['peak_pressure_range_hpa'] or 0)>=4 or ((r['peak_wind_shift_deg'] or 0)>=90 and (r['peak_wind_max_kmh'] or 0)>=20))
        r['cloud_swing']=bool((r['peak_cloud_swing_pp'] or 0)>=60)
        r['storm_or_convective']=bool(r['storm_code'] or r['heavy_precip'] or (r['peak_showers_mm'] or 0)>=2)
        rows.append(r)
    rows.sort(key=lambda x:x['date']); prev=None
    for r in rows:
        if prev and prev.get('high_c') is not None and r.get('high_c') is not None:
            d=r['high_c']-prev['high_c']; r['delta_high_c']=d; r['abs_delta_high_c']=abs(d); r['change_4c']=abs(d)>=4; r['warmup_4c']=d>=4; r['cooldown_4c']=d<=-4
            r['prev_rain_any']=prev.get('rain_any',False); r['prev_windy']=prev.get('windy',False); r['prev_front_proxy']=prev.get('front_proxy',False)
        else:
            r.update(delta_high_c=None,abs_delta_high_c=None,change_4c=False,warmup_4c=False,cooldown_4c=False,prev_rain_any=False,prev_windy=False,prev_front_proxy=False)
        prev=r
    return rows

rows=[]
for p in CACHE.glob('archive_*.json'):
    city=p.name[len('archive_'):].split('_2023-')[0]
    if city not in STATIONS: continue
    rows += day_rows(city,json.loads(p.read_text()))
rows=[r for r in rows if r.get('abs_delta_high_c') is not None]
# PM events from cached pages
mon={m:i+1 for i,m in enumerate('january february march april may june july august september october november december'.split())}
pm_dates=set(); pm_events=[]
for p in CACHE.glob('pm_events_*.json'):
    try: data=json.loads(p.read_text())
    except Exception: continue
    for e in data or []:
        sl=e.get('slug',''); m=re.match(r'highest-temperature-in-(.+)-on-([a-z]+)-(\d{1,2})-(\d{4})$',sl)
        if m and m.group(1) in STATIONS:
            key=(m.group(1),f"{int(m.group(4)):04d}-{mon[m.group(2)]:02d}-{int(m.group(3)):02d}")
            pm_dates.add(key); pm_events.append(e)
for r in rows: r['polymarket_event_day']=(r['city_slug'],r['date']) in pm_dates
pm_rows=[r for r in rows if r['polymarket_event_day']]

def flagstat(rs,flag):
    yes=[r for r in rs if r.get(flag)]; no=[r for r in rs if not r.get(flag)]
    rate=lambda s: sum(r['change_4c'] for r in s)/len(s) if s else 0
    ry, rn=rate(yes), rate(no)
    return {'flag':flag,'n_yes':len(yes),'n_no':len(no),'events_yes':sum(r['change_4c'] for r in yes),'events_no':sum(r['change_4c'] for r in no),'rate_yes':ry,'rate_no':rn,'risk_ratio':(ry/rn if rn else None),'diff_pp':(ry-rn)*100,'avg_abs_delta_yes':avg([r['abs_delta_high_c'] for r in yes]),'avg_abs_delta_no':avg([r['abs_delta_high_c'] for r in no])}

def corr(rs,k):
    xs=[]; ys=[]
    for r in rs:
        x=safe(r.get(k))
        if x is not None: xs.append(x); ys.append(1.0 if r['change_4c'] else 0.0)
    if len(xs)<20 or len(set(ys))<2: return None
    return float(np.corrcoef(np.array(xs),np.array(ys))[0,1])
flags='rain_any heavy_precip storm_code storm_or_convective snow_code windy very_windy front_proxy cloud_swing prev_rain_any prev_windy prev_front_proxy structural_risk_city coastal_or_marine'.split()
nums='daily_precip_mm peak_precip_mm peak_rain_mm peak_showers_mm daily_snow_cm peak_snow_cm daily_gust_max_kmh peak_gust_max_kmh peak_wind_max_kmh peak_wind_shift_deg peak_pressure_range_hpa peak_cloud_swing_pp peak_temp_range_c'.split()
flag_stats=sorted([flagstat(rows,f) for f in flags],key=lambda x:(x['risk_ratio'] or 0),reverse=True)
num_corrs=sorted([{'feature':k,'r':corr(rows,k)} for k in nums],key=lambda x:abs(x['r'] or 0),reverse=True)
city=[]
by=defaultdict(list)
for r in rows: by[r['city_slug']].append(r)
for c,sub in by.items(): city.append({'city_slug':c,'n':len(sub),'events':sum(r['change_4c'] for r in sub),'rate':sum(r['change_4c'] for r in sub)/len(sub),'warmups':sum(r['warmup_4c'] for r in sub),'cooldowns':sum(r['cooldown_4c'] for r in sub),'avg_abs_delta':avg([r['abs_delta_high_c'] for r in sub]),'max_abs_delta':max(r['abs_delta_high_c'] for r in sub),'pm_days':sum(r['polymarket_event_day'] for r in sub)})
city=sorted(city,key=lambda x:(x['rate'],x['events']),reverse=True)
code=defaultdict(lambda:{'n':0,'events':0}); base=sum(r['change_4c'] for r in rows)/len(rows)
for r in rows:
    for c in set(r['weather_codes']): code[c]['n']+=1; code[c]['events']+=int(r['change_4c'])
code_lifts=[]
for c,st in code.items():
    if st['n']>=30:
        rate=st['events']/st['n']; code_lifts.append({'code':c,'n':st['n'],'events':st['events'],'rate':rate,'lift':rate/base})
code_lifts=sorted(code_lifts,key=lambda x:x['lift'],reverse=True)
res={'n_rows':len(rows),'n_cities':len(by),'pm_events_cached':len(pm_events),'pm_city_dates':len(pm_dates),'pm_rows':len(pm_rows),'base_rate':base,'n_4c':sum(r['change_4c'] for r in rows),'warmups':sum(r['warmup_4c'] for r in rows),'cooldowns':sum(r['cooldown_4c'] for r in rows),'pm_rate':(sum(r['change_4c'] for r in pm_rows)/len(pm_rows) if pm_rows else None),'pm_4c':sum(r['change_4c'] for r in pm_rows),'flag_stats':flag_stats,'numeric_correlations':num_corrs,'city_stats':city,'code_lifts':code_lifts,'top_events':sorted([r for r in rows if r['change_4c']],key=lambda r:r['abs_delta_high_c'],reverse=True)[:50]}
(OUT/'temp_4c_cached_analysis.json').write_text(json.dumps(res,indent=2,default=str))
# CSVs
for name,data in [('flag_stats_cached.csv',flag_stats),('city_stats_cached.csv',city),('weather_code_lifts_cached.csv',code_lifts)]:
    with (OUT/name).open('w',newline='') as f:
        w=csv.DictWriter(f,list(data[0].keys()) if data else ['empty']); w.writeheader(); w.writerows(data)
fields=['city_slug','date','high_c','delta_high_c','abs_delta_high_c','change_4c','warmup_4c','cooldown_4c','weather_codes','rain_any','heavy_precip','storm_code','storm_or_convective','snow_code','windy','very_windy','front_proxy','cloud_swing','daily_precip_mm','peak_gust_max_kmh','peak_pressure_range_hpa','peak_wind_shift_deg','peak_cloud_swing_pp','polymarket_event_day']
with (OUT/'city_day_features_cached.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fields,extrasaction='ignore'); w.writeheader(); w.writerows(rows)
def pct(x): return 'n/a' if x is None else f'{100*x:.1f}%'
lines=['# Cached >=4C daily-high change correlation study','',f"Open-Meteo city-days: **{len(rows)}** across **{len(by)}** Polymarket weather cities",f"Cached Polymarket event city-days matched: **{len(pm_rows)}** from **{len(pm_dates)}** city-date pairs",f"All-day >=4C changes: **{res['n_4c']}** ({pct(base)}); warmups={res['warmups']}, cooldowns={res['cooldowns']}",f"PM event-day >=4C changes: **{res['pm_4c']}** ({pct(res['pm_rate'])})",'','## Strongest binary flags']
for st in flag_stats[:14]:
    rr='inf' if st['risk_ratio'] is None else f"{st['risk_ratio']:.2f}x"
    lines.append(f"- **{st['flag']}**: {pct(st['rate_yes'])} ({st['events_yes']}/{st['n_yes']}) vs {pct(st['rate_no'])} ({st['events_no']}/{st['n_no']}), RR={rr}, diff={st['diff_pp']:.1f}pp")
lines+=['','## Numeric correlations']
for c in num_corrs[:12]: lines.append(f"- {c['feature']}: r={c['r']:.3f}" if c['r'] is not None else f"- {c['feature']}: n/a")
lines+=['','## Weather code lifts']
for c in code_lifts[:12]: lines.append(f"- code {c['code']}: {pct(c['rate'])} ({c['events']}/{c['n']}), lift={c['lift']:.2f}x")
lines+=['','## Highest-rate cities']
for c in city[:20]: lines.append(f"- **{c['city_slug']}**: {pct(c['rate'])} ({c['events']}/{c['n']}), warmups={c['warmups']}, cooldowns={c['cooldowns']}, avg_abs_delta={c['avg_abs_delta']:.2f}C, max={c['max_abs_delta']:.1f}C, PM_days={c['pm_days']}")
lines+=['','## Largest individual changes']
for r in res['top_events'][:25]: lines.append(f"- **{r['city_slug']} {r['date']}**: delta={r['delta_high_c']:+.1f}C high={r['high_c']:.1f}C codes={r['weather_codes']} rain={r['rain_any']} heavy={r['heavy_precip']} wind={r['windy']} front={r['front_proxy']} cloud={r['cloud_swing']}")
lines+=['','## Files',f"- JSON: `{OUT/'temp_4c_cached_analysis.json'}`",f"- Features CSV: `{OUT/'city_day_features_cached.csv'}`",f"- Flag stats CSV: `{OUT/'flag_stats_cached.csv'}`",f"- City stats CSV: `{OUT/'city_stats_cached.csv'}`"]
(OUT/'temp_4c_cached_report.md').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines))
