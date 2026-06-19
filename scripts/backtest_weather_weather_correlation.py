#!/usr/bin/env python3
"""Backtest daily-high weather outlier NO entries and correlate post-entry drawdowns with weather conditions.

Outputs to /tmp/weather_corr_research/.
"""
import asyncio, aiohttp, json, re, math, csv, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode, quote
from collections import Counter, defaultdict
import numpy as np

OUT=Path('/tmp/weather_corr_research'); OUT.mkdir(parents=True, exist_ok=True)
CACHE=OUT/'cache'; CACHE.mkdir(exist_ok=True)
UA={'User-Agent':'Mozilla/5.0'}
MONTHS='January February March April May June July August September October November December'.split()
MON={m:i+1 for i,m in enumerate(MONTHS)}
RAIN_CODES={51,53,55,56,57,61,63,65,66,67,80,81,82}
STORM_CODES={95,96,99}

CITY_ALIASES={
    'nyc':'New York City', 'sao-paulo':'Sao Paulo', 'mexico-city':'Mexico City',
    'panama-city':'Panama City, Panama', 'hong-kong':'Hong Kong', 'kuala-lumpur':'Kuala Lumpur',
    'los-angeles':'Los Angeles', 'san-francisco':'San Francisco', 'tel-aviv':'Tel Aviv',
}
STRUCTURAL_RISK_CITIES=set('jakarta kuala-lumpur singapore panama-city houston miami hong-kong shenzhen guangzhou busan tokyo seattle san-francisco los-angeles cape-town wellington chicago dallas austin denver mexico-city sao-paulo istanbul qingdao'.split())
COASTAL_TROPICAL=set('jakarta kuala-lumpur singapore panama-city miami hong-kong shenzhen guangzhou busan cape-town wellington qingdao sao-paulo houston'.split())


def event_city(e):
    title=e.get('title','')
    m=re.match(r'Highest temperature in (.+) on ([A-Za-z]+) (\d{1,2})\?', title)
    return m.group(1) if m else None

def city_slug_from_event_slug(slug):
    # highest-temperature-in-sao-paulo-on-may-2-2026
    m=re.match(r'highest-temperature-in-(.+)-on-[a-z]+-\d+-\d{4}$', slug)
    return m.group(1) if m else None

def parse_event_date(e):
    title=e.get('title','')
    m=re.match(r'Highest temperature in (.+) on ([A-Za-z]+) (\d{1,2})\?', title)
    if not m: return None
    month=MON.get(m.group(2)); day=int(m.group(3))
    # Use endDate year (markets title often no year)
    y=datetime.fromisoformat(e['endDate'].replace('Z','+00:00')).year if e.get('endDate') else 2026
    return f'{y:04d}-{month:02d}-{day:02d}'

def parse_temp(slug, question=''):
    s=slug.lower()
    m=re.search(r'-(\d+)-(\d+)f$', s)
    if m: return ((float(m.group(1))+float(m.group(2)))/2-32)*5/9, f"{m.group(1)}-{m.group(2)}F"
    m=re.search(r'-(\d+)f$', s)
    if m: return (float(m.group(1))-32)*5/9, f"{m.group(1)}F"
    m=re.search(r'-(\d+)c(orhigher|orbelow)?$', s)
    if m:
        suff='C'
        if m.group(2)=='orhigher': suff='C or higher'
        if m.group(2)=='orbelow': suff='C or below'
        return float(m.group(1)), f"{m.group(1)}{suff}"
    m=re.search(r'(\d+)\s*°?c', question.lower())
    if m: return float(m.group(1)), f"{m.group(1)}C"
    return None,None

def jloads(x):
    if isinstance(x,(dict,list)): return x
    if not x: return None
    return json.loads(x)

def dt_ts(s): return datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
def iso(ts): return datetime.fromtimestamp(ts,timezone.utc).isoformat().replace('+00:00','Z')

def cache_path(kind,key):
    safe=re.sub(r'[^A-Za-z0-9_.-]+','_',str(key))[:220]
    return CACHE/f'{kind}_{safe}.json'

async def get_json(session,url,cache=None,timeout=60):
    if cache and cache.exists():
        return json.loads(cache.read_text())
    for attempt in range(4):
        try:
            async with session.get(url,headers=UA,timeout=timeout) as r:
                txt=await r.text()
                if r.status!=200: raise RuntimeError(f'HTTP {r.status} {txt[:200]}')
                data=json.loads(txt)
                if cache: cache.write_text(json.dumps(data))
                return data
        except Exception as e:
            if attempt==3: raise
            await asyncio.sleep(1.5*(attempt+1))

def summarize(hist, entry_t, end_t, entry_px=0.97):
    pts=[p for p in hist if entry_t<=p['t']<=end_t+12*3600]
    if not pts: return None
    within=[p for p in pts if p['t']<=end_t]
    close=(within[-1] if within else pts[-1])
    maxp=max(pts,key=lambda p:p['p']); minp=min(pts,key=lambda p:p['p'])
    max_down=minp['p']-entry_px; max_up=maxp['p']-entry_px; final=close['p']-entry_px
    if max_down<=-0.20: severity='crash_20c'
    elif max_down<=-0.10: severity='major_10c'
    elif max_down<=-0.05: severity='significant_5c'
    elif max_down<=-0.02: severity='moderate_2c'
    else: severity='small'
    if maxp['p']>=entry_px+0.005 and close['p']<entry_px-0.005: cat='popped_then_faded'
    elif close['p']<entry_px-0.005 and maxp['p']<=entry_px+0.005: cat='downtrend_continued'
    elif maxp['p']>=entry_px+0.005 and close['p']>=entry_px: cat='up_after_entry'
    else: cat='flat_choppy'
    return dict(max_up=max_up,max_down=max_down,final_change=final,max_px=maxp['p'],min_px=minp['p'],final_px=close['p'],min_t=minp['t'],max_t=maxp['t'],final_t=close['t'],category=cat,severity=severity)

def nearest_before(hist,t,max_gap=7200):
    pts=[p for p in hist if p['t']<=t]
    if not pts: return None
    p=max(pts,key=lambda x:x['t'])
    return p if t-p['t']<=max_gap else None

async def collect_events(session, target_events=230, max_offset=32000):
    pat=re.compile(r'^Highest temperature in .+ on .+\?$')
    events=[]; seen=set()
    for off in range(0,max_offset,500):
        url=f'https://gamma-api.polymarket.com/events?closed=true&limit=500&offset={off}&order=endDate&ascending=false'
        data=await get_json(session,url,cache=cache_path('events',off),timeout=90)
        if not data: break
        for e in data:
            if pat.match(e.get('title','')) and e.get('markets'):
                if e['slug'] not in seen:
                    seen.add(e['slug']); events.append(e)
        print('offset',off,'weather_events',len(events),'page_end',data[-1].get('endDate'))
        if len(events)>=target_events: break
    return events[:target_events]

async def fetch_history(session, token):
    data=await get_json(session,'https://clob.polymarket.com/prices-history?'+urlencode({'market':str(token),'interval':'max','fidelity':'1'}),cache=cache_path('hist',token),timeout=45)
    return [{'t':int(p['t']),'p':float(p['p'])} for p in data.get('history',[]) if p.get('p') is not None]

async def geocode(session, city_slug, city_name):
    q=CITY_ALIASES.get(city_slug, city_name)
    url='https://geocoding-api.open-meteo.com/v1/search?'+urlencode({'name':q,'count':1,'language':'en','format':'json'})
    data=await get_json(session,url,cache=cache_path('geo',city_slug),timeout=30)
    res=(data.get('results') or [None])[0]
    return res

async def archive_weather(session, city_slug, lat, lon, date):
    params={'latitude':lat,'longitude':lon,'start_date':date,'end_date':date,'hourly':'temperature_2m,precipitation,rain,weather_code,cloud_cover,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m','timezone':'auto','wind_speed_unit':'kmh'}
    data=await get_json(session,'https://archive-api.open-meteo.com/v1/archive?'+urlencode(params),cache=cache_path('wx',f'{city_slug}_{date}_{lat:.3f}_{lon:.3f}'),timeout=60)
    return data

def circ_range(degs):
    vals=[float(x)%360 for x in degs if x is not None]
    if not vals: return None
    vals=sorted(vals); gaps=[vals[(i+1)%len(vals)]-vals[i] if i+1<len(vals) else vals[0]+360-vals[i] for i in range(len(vals))]
    return 360-max(gaps)

def weather_metrics(data):
    h=data.get('hourly') or {}
    times=h.get('time') or []
    # Local peak window 10:00-16:00 where daily high usually matters.
    ix=[i for i,t in enumerate(times) if 10<=int(t[11:13])<=16]
    if not ix: ix=list(range(len(times)))
    def vals(k): return [h.get(k,[None]*len(times))[i] for i in ix if i < len(h.get(k,[])) and h.get(k,[None])[i] is not None]
    def mx(k):
        v=vals(k); return max(v) if v else None
    def rng(k):
        v=vals(k); return (max(v)-min(v)) if v else None
    codes=set(int(x) for x in vals('weather_code') if x is not None)
    wind_dirs=vals('wind_direction_10m')
    m={
        'temp_range_c': rng('temperature_2m'),
        'precip_mm': sum(vals('precipitation') or [0]),
        'rain_mm': sum(vals('rain') or [0]),
        'max_cloud': mx('cloud_cover'),
        'cloud_swing_pp': rng('cloud_cover'),
        'pressure_range_hpa': rng('surface_pressure'),
        'max_wind_kmh': mx('wind_speed_10m'),
        'max_gust_kmh': mx('wind_gusts_10m'),
        'wind_shift_deg': circ_range(wind_dirs),
        'rain_code': bool(codes & RAIN_CODES),
        'storm_code': bool(codes & STORM_CODES),
        'weather_codes': sorted(codes),
    }
    m['rain_or_storm']=bool(m['rain_code'] or m['storm_code'] or (m['precip_mm'] or 0)>=1.0 or (m['rain_mm'] or 0)>0)
    m['windy']=bool((m['max_gust_kmh'] or 0)>=35 or (m['max_wind_kmh'] or 0)>=25)
    m['wind_shift_flag']=bool((m['wind_shift_deg'] or 0)>=60 and (m['max_wind_kmh'] or 0)>=15)
    m['pressure_swing']=bool((m['pressure_range_hpa'] or 0)>=3)
    m['cloud_swing_flag']=bool((m['cloud_swing_pp'] or 0)>=40)
    m['weather_volatility_score']=sum([m['rain_or_storm'],m['windy'],m['wind_shift_flag'],m['pressure_swing'],m['cloud_swing_flag']])
    return m

def corr(x,y):
    xs=[]; ys=[]
    for a,b in zip(x,y):
        if a is None or b is None or math.isnan(a) or math.isnan(b): continue
        xs.append(float(a)); ys.append(float(b))
    if len(xs)<3: return None
    return float(np.corrcoef(np.array(xs),np.array(ys))[0,1])

def flag_stats(rows, flag):
    yes=[r for r in rows if r.get(flag)]
    no=[r for r in rows if not r.get(flag)]
    def rate(sub): return sum(1 for r in sub if r['significant_down'])/len(sub) if sub else 0
    ry=rate(yes); rn=rate(no)
    return dict(flag=flag, n_yes=len(yes), n_no=len(no), sig_yes=sum(1 for r in yes if r['significant_down']), sig_no=sum(1 for r in no if r['significant_down']), rate_yes=ry, rate_no=rn, risk_ratio=(ry/rn if rn>0 else None), avg_max_down_yes=(sum(r['max_down'] for r in yes)/len(yes) if yes else None), avg_max_down_no=(sum(r['max_down'] for r in no)/len(no) if no else None))

async def main():
    conn=aiohttp.TCPConnector(limit=24,ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        events=await collect_events(session, target_events=230)
        print('collected events',len(events),'markets',sum(len(e.get('markets') or []) for e in events))
        # Prepare market entries and histories.
        markets=[]; tokens=[]
        for e in events:
            city_slug=city_slug_from_event_slug(e['slug']) or (event_city(e) or '').lower().replace(' ','-')
            date=parse_event_date(e)
            for m in e.get('markets') or []:
                toks=jloads(m.get('clobTokenIds')) or []
                temp,label=parse_temp(m.get('slug',''),m.get('question',''))
                if len(toks)>=2 and temp is not None:
                    token=str(toks[1]); tokens.append(token)
                    markets.append({'event_slug':e['slug'],'event_title':e['title'],'event_end':e.get('endDate'),'event_date':date,'city':event_city(e),'city_slug':city_slug,'market_slug':m.get('slug'),'question':m.get('question'),'temp_c':temp,'temp_label':label,'no_token':token})
        uniq_tokens=sorted(set(tokens))
        sem=asyncio.Semaphore(24)
        async def hist_task(t):
            async with sem:
                try: return t, await fetch_history(session,t)
                except Exception as e: return t, []
        histories={}
        for i in range(0,len(uniq_tokens),300):
            chunk=uniq_tokens[i:i+300]
            for t,h in await asyncio.gather(*(hist_task(t) for t in chunk)):
                histories[t]=h
            print('histories',min(i+300,len(uniq_tokens)),'/',len(uniq_tokens))
        # Candidate entries.
        by_event=defaultdict(list)
        for m in markets: by_event[m['event_slug']].append(m)
        candidates=[]; all_scanned=0
        for es,ms in by_event.items():
            end_t=dt_ts(ms[0]['event_end']); start_t=end_t-24*3600
            # each market scanned regardless of entry qualification
            all_scanned+=len(ms)
            for m in ms:
                hist=histories.get(m['no_token'],[])
                pts=[p for p in hist if start_t<=p['t']<=end_t and 0.965<=p['p']<=0.975]
                if not pts: continue
                entry=pts[0]
                # Favorite at entry: lowest NO among buckets with recent chart point.
                favs=[]
                for n in ms:
                    p=nearest_before(histories.get(n['no_token'],[]), entry['t'])
                    if p: favs.append((p['p'],n['temp_c'],n['market_slug']))
                if not favs: continue
                fav=min(favs,key=lambda x:x[0])
                offset=abs(m['temp_c']-fav[1])
                if offset<4: continue
                s=summarize(hist,entry['t'],end_t,0.97)
                if not s: continue
                candidates.append({**m,'entry_t':entry['t'],'entry_iso':iso(entry['t']),'observed_entry_px':entry['p'],'favorite_temp_c':fav[1],'favorite_slug':fav[2],'offset_c':offset,**s})
        # Weather data per event.
        event_wx={}
        geos={}
        for e in events:
            slug=e['slug']; city=event_city(e); city_slug=city_slug_from_event_slug(slug)
            date=parse_event_date(e)
            if not city or not city_slug or not date: continue
            try:
                geo=await geocode(session,city_slug,city)
                geos[city_slug]=geo
                if not geo: continue
                wx=await archive_weather(session,city_slug,float(geo['latitude']),float(geo['longitude']),date)
                met=weather_metrics(wx)
                met.update({'city_slug':city_slug,'city':city,'event_date':date,'lat':geo['latitude'],'lon':geo['longitude'],'country':geo.get('country'),'structural_risk_city':city_slug in STRUCTURAL_RISK_CITIES,'coastal_tropical':city_slug in COASTAL_TROPICAL})
                event_wx[slug]=met
            except Exception as ex:
                event_wx[slug]={'city_slug':city_slug,'city':city,'event_date':date,'error':str(ex)}
        rows=[]
        for r in candidates:
            wx=event_wx.get(r['event_slug'],{})
            row={**r, **{k:v for k,v in wx.items() if k not in ['city','city_slug','event_date']}}
            row['significant_down']=r['max_down']<=-0.05
            row['major_down']=r['max_down']<=-0.10
            row['crash_down']=r['max_down']<=-0.20
            rows.append(row)
        # Stats.
        flags=['rain_or_storm','windy','wind_shift_flag','pressure_swing','cloud_swing_flag','structural_risk_city','coastal_tropical']
        flag_table=[flag_stats(rows,f) for f in flags]
        numeric=['precip_mm','rain_mm','max_gust_kmh','max_wind_kmh','wind_shift_deg','pressure_range_hpa','cloud_swing_pp','temp_range_c','weather_volatility_score']
        corrs={n:corr([r.get(n) for r in rows],[-r['max_down'] for r in rows]) for n in numeric}
        by_city=[]
        for city,sub in defaultdict(list, { }).items(): pass
        d=defaultdict(list)
        for r in rows: d[r['city_slug']].append(r)
        for city,sub in d.items():
            by_city.append({'city_slug':city,'n':len(sub),'sig_down':sum(r['significant_down'] for r in sub),'major_down':sum(r['major_down'] for r in sub),'rate_sig':sum(r['significant_down'] for r in sub)/len(sub),'avg_max_down':sum(r['max_down'] for r in sub)/len(sub),'worst_max_down':min(r['max_down'] for r in sub),'rain_or_storm':event_wx.get(sub[0]['event_slug'],{}).get('rain_or_storm'),'weather_volatility_score_avg':sum((r.get('weather_volatility_score') or 0) for r in sub)/len(sub)})
        by_city=sorted(by_city,key=lambda x:(-x['rate_sig'],-x['n']))
        result={'generated_at':datetime.now(timezone.utc).isoformat().replace('+00:00','Z'),'events_scanned':len(events),'markets_scanned':all_scanned,'candidate_entries':len(rows),'rows':rows,'flag_stats':flag_table,'correlations':corrs,'by_city':by_city,'event_weather':event_wx}
        (OUT/'weather_corr_research.json').write_text(json.dumps(result,indent=2,default=str))
        # CSVs
        fields=['event_slug','city_slug','market_slug','temp_label','entry_iso','offset_c','observed_entry_px','category','severity','max_down','max_up','final_change','significant_down','major_down','rain_or_storm','windy','wind_shift_flag','pressure_swing','cloud_swing_flag','structural_risk_city','coastal_tropical','precip_mm','rain_mm','max_gust_kmh','max_wind_kmh','wind_shift_deg','pressure_range_hpa','cloud_swing_pp','weather_volatility_score']
        with open(OUT/'weather_corr_candidates.csv','w',newline='') as f:
            w=csv.DictWriter(f,fields); w.writeheader();
            for r in rows: w.writerow({k:r.get(k) for k in fields})
        with open(OUT/'weather_corr_flag_stats.csv','w',newline='') as f:
            w=csv.DictWriter(f, list(flag_table[0].keys())); w.writeheader(); w.writerows(flag_table)
        with open(OUT/'weather_corr_by_city.csv','w',newline='') as f:
            w=csv.DictWriter(f, list(by_city[0].keys()) if by_city else ['city_slug']); w.writeheader(); w.writerows(by_city)
        # Markdown report
        lines=[]
        lines.append('# Weather outlier 0.97 backtest + weather-condition correlation')
        lines.append(f"Generated: {result['generated_at']}")
        lines.append('')
        lines.append('## Dataset')
        lines.append(f"- Polymarket closed daily-high weather events scanned: **{len(events)}**")
        lines.append(f"- Option markets scanned: **{all_scanned}**")
        lines.append(f"- Qualified 0.97 / 4+C NO entries: **{len(rows)}**")
        lines.append('- Entry rule: first NO chart point in last 24h before event close with price 0.965–0.975, assumed fill at 0.9700; option temp at least 4°C from contemporaneous favorite bucket.')
        lines.append('- Weather condition source: Open-Meteo historical archive at geocoded city coordinates for the market date, peak window 10:00–16:00 local. This is a proxy for settlement-station conditions, not official Wunderground station truth.')
        lines.append('')
        c=Counter(r['severity'] for r in rows); cats=Counter(r['category'] for r in rows)
        lines.append('## Outcome distribution')
        lines.append('- Categories: '+', '.join(f'{k}={v}' for k,v in cats.most_common()))
        lines.append('- Drawdown severity: '+', '.join(f'{k}={v}' for k,v in c.most_common()))
        lines.append(f"- Significant drawdown <= -5¢: **{sum(r['significant_down'] for r in rows)} / {len(rows)}** ({100*sum(r['significant_down'] for r in rows)/len(rows):.1f}%)")
        lines.append(f"- Major drawdown <= -10¢: **{sum(r['major_down'] for r in rows)} / {len(rows)}** ({100*sum(r['major_down'] for r in rows)/len(rows):.1f}%)")
        lines.append(f"- Crashes <= -20¢: **{sum(r['crash_down'] for r in rows)} / {len(rows)}** ({100*sum(r['crash_down'] for r in rows)/len(rows):.1f}%)")
        lines.append('')
        lines.append('## Correlation / risk-ratio table')
        for st in flag_table:
            rr='inf' if st['risk_ratio'] is None else f"{st['risk_ratio']:.2f}x"
            lines.append(f"- **{st['flag']}**: sig-down rate {100*st['rate_yes']:.1f}% ({st['sig_yes']}/{st['n_yes']}) vs {100*st['rate_no']:.1f}% ({st['sig_no']}/{st['n_no']}), RR={rr}, avg max-down yes={100*(st['avg_max_down_yes'] or 0):.1f}¢ vs no={100*(st['avg_max_down_no'] or 0):.1f}¢")
        lines.append('')
        lines.append('## Numeric correlations with drawdown magnitude (-max_down)')
        for k,v in sorted(corrs.items(), key=lambda kv: (kv[1] is None, -(abs(kv[1]) if kv[1] is not None else 0))):
            lines.append(f"- {k}: r={v:.3f}" if v is not None else f"- {k}: n/a")
        lines.append('')
        lines.append('## Worst cities / locations')
        for bc in by_city[:20]:
            lines.append(f"- **{bc['city_slug']}**: n={bc['n']}, sig={bc['sig_down']}, major={bc['major_down']}, sig_rate={100*bc['rate_sig']:.1f}%, avg max-down={100*bc['avg_max_down']:.1f}¢, worst={100*bc['worst_max_down']:.1f}¢, wx_score_avg={bc['weather_volatility_score_avg']:.1f}")
        lines.append('')
        lines.append('## Worst individual cases')
        for r in sorted(rows,key=lambda x:x['max_down'])[:25]:
            lines.append(f"- **{r['city_slug']} {r['temp_label']}** {r['event_slug']} entry {r['entry_iso']}: max_down={100*r['max_down']:.1f}¢, final={100*r['final_change']:.1f}¢, cat={r['category']}; rain={r.get('rain_or_storm')}, wind={r.get('windy')}, gust={r.get('max_gust_kmh')}, wind_shift={r.get('wind_shift_deg')}, pressure_rng={r.get('pressure_range_hpa')}, cloud_swing={r.get('cloud_swing_pp')}, wx_score={r.get('weather_volatility_score')}")
        lines.append('')
        lines.append('## Files')
        lines.append(f"- JSON: `{OUT/'weather_corr_research.json'}`")
        lines.append(f"- Candidates CSV: `{OUT/'weather_corr_candidates.csv'}`")
        lines.append(f"- Flag stats CSV: `{OUT/'weather_corr_flag_stats.csv'}`")
        lines.append(f"- City stats CSV: `{OUT/'weather_corr_by_city.csv'}`")
        (OUT/'weather_corr_report.md').write_text('\n'.join(lines))
        print('\n'.join(lines[:80]))
        print('WROTE', OUT)

if __name__=='__main__': asyncio.run(main())
