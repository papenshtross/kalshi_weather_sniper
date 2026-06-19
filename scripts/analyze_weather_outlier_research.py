#!/usr/bin/env python3
import asyncio, asyncpg, os, json, re, math, time, csv
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

UA={'User-Agent':'Mozilla/5.0'}
OUT=Path('/tmp/weather_outlier_research')
OUT.mkdir(parents=True, exist_ok=True)

def fetch_json(url, timeout=30):
    req=Request(url, headers=UA)
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())

def event_slug_from_market(slug):
    # Strip terminal option suffix: -28corbelow, -29corhigher, -52-53f, -17c
    # C buckets are single values (e.g. -22c); F can be a range (-52-53f).
    # Do not let a C suffix regex consume the event year (e.g. ...-2026-22c).
    return re.sub(r'-(?:\d+-\d+f|\d+f|\d+c|\d+corhigher|\d+corbelow)$','',slug)

def parse_temp(slug, question=''):
    s=slug.lower()
    m=re.search(r'-(\d+)-(\d+)f$', s)
    if m:
        return ((float(m.group(1))+float(m.group(2)))/2 - 32) * 5/9, f"{m.group(1)}-{m.group(2)}F"
    m=re.search(r'-(\d+)f$', s)
    if m:
        return (float(m.group(1))-32)*5/9, f"{m.group(1)}F"
    m=re.search(r'-(\d+)c(?:orhigher|orbelow)?$', s)
    if m:
        suffix='C+'
        if s.endswith('orbelow'): suffix='C or below'
        elif s.endswith('orhigher'): suffix='C or higher'
        else: suffix='C'
        return float(m.group(1)), f"{m.group(1)}{suffix}"
    m=re.search(r'(\d+)\s*°?c', question.lower())
    if m: return float(m.group(1)), f"{m.group(1)}C"
    return None, None

def iso(dt):
    if isinstance(dt, str): return dt
    return dt.astimezone(timezone.utc).isoformat().replace('+00:00','Z')

def ts(dt):
    if isinstance(dt, str):
        return datetime.fromisoformat(dt.replace('Z','+00:00')).timestamp()
    return dt.timestamp()

def jloads(x):
    if not x: return None
    if isinstance(x, (list,dict)): return x
    return json.loads(x)

def hist_for_token(token):
    try:
        url='https://clob.polymarket.com/prices-history?'+urlencode({'market':str(token),'interval':'max','fidelity':'1'})
        data=fetch_json(url, timeout=25)
        return [{'t':int(p['t']), 'p':float(p['p'])} for p in data.get('history',[]) if p.get('p') is not None]
    except Exception as e:
        return {'error':str(e)}

def nearest_at_or_after(hist, t):
    pts=[p for p in hist if p['t']>=t]
    return min(pts, key=lambda p:p['t']) if pts else None

def nearest_before_or_at(hist, t, max_gap=3600):
    pts=[p for p in hist if p['t']<=t]
    if not pts: return None
    p=max(pts, key=lambda p:p['t'])
    return p if t-p['t']<=max_gap else None

def summarize_path(hist, entry_t, end_t=None, entry_px=None):
    after=[p for p in hist if p['t']>=entry_t and (end_t is None or p['t']<=end_t+86400)]
    if not after: return None
    if entry_px is None: entry_px=after[0]['p']
    maxp=max(after, key=lambda p:p['p']); minp=min(after, key=lambda p:p['p']); last=after[-1]
    end_slice=[p for p in after if end_t is None or p['t']<=end_t]
    close=end_slice[-1] if end_slice else last
    # first 6 points after entry, then final
    sample=after[:6]
    if last not in sample: sample=sample+[last]
    ups=[p for p in after if p['p']>entry_px+1e-9]
    downs=[p for p in after if p['p']<entry_px-1e-9]
    if maxp['p'] >= entry_px + 0.005 and close['p'] >= entry_px:
        cat='up_after_entry'
    elif close['p'] < entry_px - 0.005 and maxp['p'] <= entry_px + 0.005:
        cat='downtrend_continued'
    elif maxp['p'] >= entry_px + 0.005 and close['p'] < entry_px:
        cat='popped_then_faded'
    else:
        cat='flat_choppy'
    return {
        'points':len(after), 'entry_px':entry_px,
        'max_px':maxp['p'], 'max_t':maxp['t'], 'max_up':maxp['p']-entry_px,
        'min_px':minp['p'], 'min_t':minp['t'], 'max_down':minp['p']-entry_px,
        'last_px':last['p'], 'last_t':last['t'], 'final_px':close['p'], 'final_t':close['t'],
        'final_change':close['p']-entry_px, 'category':cat,
        'sample':sample,
    }

async def load_db():
    conn=await asyncpg.connect(os.environ['POSTGRES_URL'])
    fills=await conn.fetch("""
      select id,strategy_id,ts,market_slug,token,outcome,side,price,size,stake_usd,status,response
      from order_attempts
      where strategy_id ilike 'live_weather_outlier%' and side='BUY' and outcome='NO' and status='filled'
      order by ts
    """)
    sells=await conn.fetch("""
      select id,strategy_id,ts,market_slug,token,outcome,side,price,size,stake_usd,status,error,response
      from order_attempts
      where strategy_id ilike 'live_weather_outlier%' and side='SELL' and outcome='NO'
      order by ts
    """)
    # fills table may include reconciled entries/take-profit not represented as status=filled attempts
    filltbl=await conn.fetch("""
      select strategy_id,id,ts,market,side,px,size,kind
      from fills
      where strategy_id ilike 'live_weather_outlier%'
      order by ts
    """)
    strategies=await conn.fetch("select id,name,market,config,status,updated_at from strategies where id ilike 'live_weather_outlier%' order by id")
    await conn.close()
    return fills,sells,filltbl,strategies

async def main():
    fills,sells,filltbl,strategies=await load_db()
    # aggregate confirmed status=filled BUY attempts by market_slug/token
    pos={}
    for r in fills:
        key=(r['market_slug'], str(r['token']))
        d=pos.setdefault(key, {'market_slug':r['market_slug'], 'token':str(r['token']), 'strategy_id':r['strategy_id'], 'fills':[]})
        d['fills'].append({k:(float(r[k]) if k in ('price','size','stake_usd') and r[k] is not None else (iso(r[k]) if k=='ts' else r[k])) for k in ['id','strategy_id','ts','market_slug','token','price','size','stake_usd','status']})
    # add reconciled fills by matching token unavailable? skip if no market slug; report separately
    unique_event_slugs=sorted(set(event_slug_from_market(k[0]) for k in pos.keys()))
    # include submitted slugs and strategy market-derived event slugs for backtest universe
    all_attempt_slugs=set([r['market_slug'] for r in fills+sells if r['market_slug']])
    event_universe=set(event_slug_from_market(s) for s in all_attempt_slugs)
    # Gamma event metadata
    events={}
    for es in sorted(event_universe):
        try:
            data=fetch_json('https://gamma-api.polymarket.com/events?'+urlencode({'slug':es,'closed':'true'}))
            if not data:
                # Some events are past endDate but not Gamma-closed yet; fetch without closed=true too.
                data=fetch_json('https://gamma-api.polymarket.com/events?'+urlencode({'slug':es}))
            if data: events[es]=data[0]
            else: events[es]={'slug':es,'error':'not_found','markets':[]}
        except Exception as e:
            events[es]={'slug':es,'error':str(e),'markets':[]}
    # map market info
    market_info={}
    for es,e in events.items():
        for m in e.get('markets') or []:
            try: toks=jloads(m.get('clobTokenIds')) or []
            except: toks=[]
            temp,label=parse_temp(m.get('slug',''), m.get('question',''))
            market_info[m.get('slug')]=dict(event_slug=es,event_title=e.get('title'),event_closed=e.get('closed'),event_end=e.get('endDate'),event_closed_time=e.get('closedTime'),question=m.get('question'),closed=m.get('closed'),outcomePrices=m.get('outcomePrices'),clobTokenIds=toks,temp_c=temp,temp_label=label)
    tokens=sorted(set(d['token'] for d in pos.values()))
    # plus all NO tokens in event universe for backtest
    for m in market_info.values():
        toks=m.get('clobTokenIds') or []
        if len(toks)>=2: tokens.append(str(toks[1]))
    tokens=sorted(set(tokens))
    histories={}
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(hist_for_token,t):t for t in tokens}
        for fut in as_completed(futs): histories[futs[fut]]=fut.result()
    # Analyze bot fills
    bot_rows=[]
    for (slug,token),d in sorted(pos.items(), key=lambda kv:min(f['ts'] for f in kv[1]['fills'])):
        fills_list=d['fills']
        total_size=sum(f['size'] for f in fills_list)
        avg_px=sum(f['price']*f['size'] for f in fills_list)/total_size if total_size else sum(f['price'] for f in fills_list)/len(fills_list)
        first_ts=min(datetime.fromisoformat(f['ts'].replace('Z','+00:00')) for f in fills_list)
        last_ts=max(datetime.fromisoformat(f['ts'].replace('Z','+00:00')) for f in fills_list)
        info=market_info.get(slug,{})
        end_t=ts(info['event_end']) if info.get('event_end') else None
        hist=histories.get(token,[])
        summ=summarize_path(hist if isinstance(hist,list) else [], first_ts.timestamp(), end_t, avg_px)
        related_sells=[dict(x) for x in sells if x['market_slug']==slug]
        bot_rows.append({
            'city_strategy':d['strategy_id'].replace('live_weather_outlier_sniper_','').replace('_auto_v1',''),
            'market_slug':slug,'event_slug':event_slug_from_market(slug),'question':info.get('question'),
            'event_end':info.get('event_end'),'event_closed':info.get('event_closed'),'market_closed':info.get('closed'),
            'outcomePrices':info.get('outcomePrices'),'temp_c':info.get('temp_c'),'temp_label':info.get('temp_label'),
            'token':token,'fills_count':len(fills_list),'first_entry':iso(first_ts),'last_entry':iso(last_ts),
            'shares':total_size,'avg_entry_px':avg_px,'stake_usd':sum(f['stake_usd'] or 0 for f in fills_list),
            'summary':summ,'sells_count':len(related_sells),'sell_statuses':[x['status'] for x in related_sells],
        })
    # Backtest on event universe: first NO history point around 0.97 in last 24h; outlier by >=4C from favorite at that time.
    backtest=[]
    for es,e in events.items():
        if e.get('error'): continue
        end=e.get('endDate')
        if not end: continue
        end_t=ts(end); start_t=end_t-24*3600
        ms=[]
        for m in e.get('markets') or []:
            toks=jloads(m.get('clobTokenIds')) or []
            if len(toks)<2: continue
            temp,label=parse_temp(m.get('slug',''),m.get('question',''))
            if temp is None: continue
            token=str(toks[1])
            hist=histories.get(token,[])
            if not isinstance(hist,list) or not hist: continue
            ms.append({'slug':m.get('slug'),'question':m.get('question'),'token':token,'temp':temp,'label':label,'hist':hist})
        for m in ms:
            candidates=[p for p in m['hist'] if start_t<=p['t']<=end_t and 0.965<=p['p']<=0.975]
            if not candidates: continue
            entry=candidates[0]
            # Find favorite by nearest NO price: lowest NO = highest YES.
            prices=[]
            for n in ms:
                p=nearest_before_or_at(n['hist'], entry['t'], max_gap=2*3600)
                if p: prices.append((p['p'], n['temp'], n['slug']))
            if not prices: continue
            fav=min(prices, key=lambda x:x[0])
            offset=abs(m['temp']-fav[1])
            if offset < 4: continue
            summ=summarize_path(m['hist'], entry['t'], end_t, 0.97)
            if not summ: continue
            backtest.append({'event_slug':es,'event_title':e.get('title'),'event_end':end,'market_slug':m['slug'],'question':m['question'],'temp_c':m['temp'],'temp_label':m['label'],'favorite_temp_c':fav[1],'favorite_slug':fav[2],'offset_c':offset,'entry_t':entry['t'],'entry_iso':datetime.fromtimestamp(entry['t'],timezone.utc).isoformat().replace('+00:00','Z'),'observed_entry_px':entry['p'],'assumed_entry_px':0.97,'summary':summ})
    # de-dupe backtest by event+market (earliest)
    seen={}; bt=[]
    for r in sorted(backtest, key=lambda x:(x['event_slug'], x['entry_t'])):
        k=(r['event_slug'],r['market_slug'])
        if k not in seen:
            seen[k]=1; bt.append(r)
    backtest=bt
    # write CSVs
    with open(OUT/'bot_fill_analysis.csv','w',newline='') as f:
        fields=['city_strategy','market_slug','question','event_end','event_closed','fills_count','first_entry','shares','avg_entry_px','stake_usd','temp_label','category','max_up','max_down','final_change','max_px','min_px','final_px','sells_count','sell_statuses']
        w=csv.DictWriter(f,fields); w.writeheader()
        for r in bot_rows:
            s=r['summary'] or {}
            w.writerow({**{k:r.get(k) for k in fields}, 'category':s.get('category'), 'max_up':s.get('max_up'), 'max_down':s.get('max_down'), 'final_change':s.get('final_change'), 'max_px':s.get('max_px'), 'min_px':s.get('min_px'), 'final_px':s.get('final_px')})
    with open(OUT/'backtest_097_4c_outliers.csv','w',newline='') as f:
        fields=['event_slug','market_slug','question','event_end','entry_iso','temp_label','favorite_temp_c','offset_c','observed_entry_px','category','max_up','max_down','final_change','max_px','min_px','final_px']
        w=csv.DictWriter(f,fields); w.writeheader()
        for r in backtest:
            s=r['summary']
            w.writerow({**{k:r.get(k) for k in fields}, 'category':s.get('category'), 'max_up':s.get('max_up'), 'max_down':s.get('max_down'), 'final_change':s.get('final_change'), 'max_px':s.get('max_px'), 'min_px':s.get('min_px'), 'final_px':s.get('final_px')})
    # Markdown summary
    def cents(x): return 'n/a' if x is None else f"{x*100:+.2f}¢"
    lines=[]
    lines.append('# Weather outlier bot: post-entry price movement research')
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat().replace('+00:00','Z')}")
    lines.append('')
    lines.append('## Scope / method')
    lines.append('- Bot scope: confirmed `order_attempts.status=filled` BUY/NO attempts from `live_weather_outlier_*` strategies. Submitted-but-not-confirmed attempts are excluded from bot-fill PnL/path stats.')
    lines.append('- Price path: Polymarket CLOB `prices-history` for the NO token, after first confirmed entry, compared to weighted average entry price; endpoint is chart-level (~10 minute snapshots), not full tick/orderbook replay.')
    lines.append('- Up means NO price moved higher after entry; downtrend means NO price kept falling/ended below entry.')
    lines.append(f"- Bot confirmed filled market rows analyzed: {len(bot_rows)} unique market/token positions across {len(set(r['event_slug'] for r in bot_rows))} weather events.")
    cats={}
    for r in bot_rows:
        c=(r['summary'] or {}).get('category','no_history'); cats[c]=cats.get(c,0)+1
    lines.append('- Bot categories: '+', '.join(f"{k}={v}" for k,v in sorted(cats.items())))
    lines.append('')
    lines.append('## Bot filled/redeemed/closed markets')
    for r in bot_rows:
        s=r['summary'] or {}
        lines.append(f"### {r['city_strategy']} — {r['temp_label']} — {r['market_slug']}")
        lines.append(f"- Question: {r.get('question')}")
        lines.append(f"- Entry: {r['first_entry']} UTC, {r['fills_count']} fills, {r['shares']:.4f} NO shares, avg `{r['avg_entry_px']:.4f}` ({r['stake_usd']:.4f} USDC). Event end `{r.get('event_end')}`, closed={r.get('event_closed')}.")
        if s:
            lines.append(f"- Category: **{s['category']}**. Max up: `{cents(s['max_up'])}` to `{s['max_px']:.4f}`; max down: `{cents(s['max_down'])}` to `{s['min_px']:.4f}`; final/close change: `{cents(s['final_change'])}` to `{s['final_px']:.4f}`.")
            sample=', '.join(datetime.fromtimestamp(p['t'],timezone.utc).strftime('%m-%d %H:%M')+f"={p['p']:.4f}" for p in s['sample'])
            lines.append(f"- Chart sample: {sample}")
        else:
            lines.append('- No chart history returned after entry.')
        if r['sells_count']:
            lines.append(f"- Bot sell/TP attempts on same market: {r['sells_count']} statuses={r['sell_statuses']}")
        lines.append('')
    # Backtest stats
    lines.append('## Step 2 backtest: hypothetical NO entry at 0.97, 24h before close, 4+C outliers')
    lines.append('- Universe: same bot weather event universe discovered from bot order attempts (not every weather market on Polymarket).')
    lines.append('- Entry rule: first chart point in last 24h with NO price between 0.965 and 0.975, assume fill at 0.9700. Outlier rule: option temp at least 4°C from the event favorite at that timestamp; favorite approximated as lowest NO / highest YES among event buckets from nearest chart snapshot.')
    lines.append(f"- Backtest candidates: {len(backtest)} market entries across {len(set(r['event_slug'] for r in backtest))} events.")
    bc={}
    avg_up=avg_down=avg_final=0
    for r in backtest:
        c=r['summary']['category']; bc[c]=bc.get(c,0)+1
        avg_up+=r['summary']['max_up']; avg_down+=r['summary']['max_down']; avg_final+=r['summary']['final_change']
    n=len(backtest) or 1
    lines.append('- Backtest categories: '+(', '.join(f"{k}={v}" for k,v in sorted(bc.items())) if bc else 'none'))
    if backtest:
        lines.append(f"- Average max-up: `{cents(avg_up/n)}`; average max-down: `{cents(avg_down/n)}`; average final change: `{cents(avg_final/n)}`.")
    for r in backtest:
        s=r['summary']
        lines.append(f"- {r['event_slug']} / {r['temp_label']} ({r['offset_c']:.1f}°C from favorite {r['favorite_temp_c']:.1f}°C), entry {r['entry_iso']} observed `{r['observed_entry_px']:.4f}` → **{s['category']}**, max up `{cents(s['max_up'])}`, max down `{cents(s['max_down'])}`, final `{s['final_px']:.4f}` ({cents(s['final_change'])}).")
    lines.append('')
    lines.append('## Files')
    lines.append(f"- JSON: `{OUT/'weather_outlier_research.json'}`")
    lines.append(f"- Bot CSV: `{OUT/'bot_fill_analysis.csv'}`")
    lines.append(f"- Backtest CSV: `{OUT/'backtest_097_4c_outliers.csv'}`")
    result={'bot_rows':bot_rows,'backtest':backtest,'events':{k:{kk:v for kk,v in e.items() if kk!='markets'} for k,e in events.items()}}
    (OUT/'weather_outlier_research.json').write_text(json.dumps(result, indent=2, default=str))
    (OUT/'weather_outlier_research.md').write_text('\n'.join(lines))
    print('\n'.join(lines[:60]))
    print(f"\nWROTE {OUT/'weather_outlier_research.md'}")
    print(f"BOT_ROWS {len(bot_rows)} BACKTEST {len(backtest)}")

if __name__=='__main__':
    asyncio.run(main())
