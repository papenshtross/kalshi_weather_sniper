#!/usr/bin/env python3
from __future__ import annotations
"""Retest 10 BTC 5m pre-open trend candidates on larger data and Cronos L2.

Research-only: no live services, no orders.

Directional model follows research_2 handoff exactly:
- compute trend before Polymarket BTC 5m market opens from Binance candles
- enter instantly at market open
- ignore Polymarket entry price for directional hit-rate scoring
- outcome = UP iff final_price >= price_to_beat

Executable L2 overlay:
- for markets present in CronosVirus00 public L2 parquet sample, map predicted side to Up/Down token
- simulate market-open buy using latest captured ask book at/after open within a configurable latency window
- fill $notional against asks; payout 1 if predicted side wins, 0 otherwise
"""
import argparse, bisect, csv, json, math, sys, time, zipfile, io
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
EXTRACTED = ROOT / 'reports/preopen_trend_sweep/extracted_research_2'
sys.path.insert(0, str(EXTRACTED / 'scripts'))
import backtest_btc5m_preopen_trend_sweep as pre  # noqa

GAMMA_EVENTS_URL='https://gamma-api.polymarket.com/events'
BINANCE_KLINES_URL='https://api.binance.com/api/v3/klines'

CANDIDATES = [
    ('market_structure', {'frame':30,'lookback':180,'w':1,'min_atr':0.8}),
    ('bb_break', {'frame':5,'lookback':60,'n':10,'k':2.5,'fade':False}),
    ('supertrend', {'frame':30,'lookback':180,'n':5,'mult':3}),
    ('market_structure', {'frame':30,'lookback':180,'w':1,'min_atr':0}),
    ('raw_return', {'frame':1,'lookback':90,'n':3,'thr':0.0005}),
    ('ensemble', {'frame':30,'lookback':180,'members':[
        ('adx_dmi', {'adx':5,'gap':5,'n':7}),
        ('ema_slope', {'fast':5,'k':1,'slow':8,'thr':0}),
        ('raw_return', {'n':5,'thr':0.0001}),
        ('adx_dmi', {'adx':10,'gap':0,'n':3}),
        ('bb_break', {'fade':True,'k':1.5,'n':5}),
    ], 'min_votes':3, 'vote_gap':1}),
    ('candles', {'frame':30,'lookback':120,'wick':0.8,'body':0.45,'mode':'reversal'}),
    ('candles', {'frame':30,'lookback':120,'wick':0.8,'body':0.55,'mode':'reversal'}),
    ('supertrend', {'frame':30,'lookback':90,'n':3,'mult':2.5}),
    ('supertrend', {'frame':15,'lookback':180,'n':7,'mult':3}),
]

def get_json(url, params=None, timeout=60, attempts=5):
    if params:
        url = url + '?' + urlencode(params, doseq=True)
    last=None
    for a in range(attempts):
        try:
            req=Request(url, headers={'User-Agent':'Hermes preopen retest'})
            with urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last=e; time.sleep(min(2**a,10))
    raise RuntimeError(f'GET failed {url}: {last!r}')

def iter_market_start_timestamps(start_ts:int,end_ts:int):
    cur=start_ts-(start_ts%300)
    while cur<=end_ts:
        yield cur; cur+=300

def batched(xs,n):
    for i in range(0,len(xs),n): yield xs[i:i+n]

def parse_iso(v):
    if not v: return None
    try: return int(datetime.fromisoformat(v.replace('Z','+00:00')).timestamp())
    except Exception: return None

def load_gamma_btc_markets(days:int, cache_path:Path):
    if cache_path.exists():
        return json.loads(cache_path.read_text())
    end_ts=int(datetime.now(timezone.utc).timestamp())
    start_ts=end_ts-days*86400
    slugs=[f'btc-updown-5m-{ts}' for ts in iter_market_start_timestamps(start_ts,end_ts)]
    markets=[]
    for idx,b in enumerate(batched(slugs,100),1):
        params=[('slug',s) for s in b]+[('closed','true'),('limit',str(len(b)))]
        page=get_json(GAMMA_EVENTS_URL, params=params, timeout=60)
        for ev in page:
            slug=str(ev.get('slug') or '')
            if not slug.startswith('btc-updown-5m-'): continue
            md=ev.get('eventMetadata') or {}; ms=ev.get('markets') or []
            if not ms: continue
            m=ms[0]; outs=m.get('outcomes') or []; toks=m.get('clobTokenIds') or []
            if isinstance(outs,str): outs=json.loads(outs)
            if isinstance(toks,str): toks=json.loads(toks)
            if len(outs)<2 or len(toks)<2: continue
            try: up_i=outs.index('Up'); down_i=outs.index('Down')
            except ValueError: continue
            ptb=md.get('priceToBeat'); final=md.get('finalPrice')
            if ptb is None or final is None: continue
            st=int(slug.rsplit('-',1)[-1]); et=parse_iso(ev.get('endDate')) or st+300
            markets.append({'market_id':str(m.get('id')),'condition_id':str(m.get('conditionId')),'market_slug':slug,'start_ts':st,'end_ts':et,'price_to_beat':float(ptb),'final_price':float(final),'up_token':str(toks[up_i]),'down_token':str(toks[down_i])})
        print(f'gamma batch {idx}/{math.ceil(len(slugs)/100)} markets={len(markets)}', flush=True)
        time.sleep(0.05)
    markets=sorted({m['market_slug']:m for m in markets}.values(), key=lambda m:m['start_ts'])
    payload={'generated_at':datetime.now(timezone.utc).isoformat(),'days':days,'markets':markets}
    cache_path.parent.mkdir(parents=True, exist_ok=True); cache_path.write_text(json.dumps(payload))
    return payload

def fetch_binance_1s_range(start_ts:int,end_ts:int, cache_path:Path):
    """Fetch BTCUSDT 1s candles using Binance daily zip archives.

    The REST klines API is too slow for 60d inline. Archive zips are ~2-3MB/day and
    include the same 1s OHLCV rows. Cache daily zips, not a huge JSON candle map.
    """
    zip_dir = cache_path.parent / 'binance_1s_daily_zips'
    zip_dir.mkdir(parents=True, exist_ok=True)
    candles={}
    d0=datetime.fromtimestamp(start_ts, timezone.utc).date()
    d1=datetime.fromtimestamp(end_ts, timezone.utc).date()
    day=d0; nday=(d1-d0).days+1; done=0
    while day<=d1:
        name=f'BTCUSDT-1s-{day.isoformat()}.zip'
        zp=zip_dir/name
        if not zp.exists() or zp.stat().st_size < 1000:
            url=f'https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s/{name}'
            try:
                req=Request(url, headers={'User-Agent':'Hermes preopen retest'})
                with urlopen(req, timeout=60) as r:
                    zp.write_bytes(r.read())
            except Exception as e:
                print(f'archive miss {day}: {e!r}; falling back to REST for that day', flush=True)
                cur=max(start_ts, int(datetime(day.year,day.month,day.day,tzinfo=timezone.utc).timestamp()))
                day_end=min(end_ts, cur+86399)
                while cur<=day_end:
                    chunk_end=min(cur+999,day_end)
                    params={'symbol':'BTCUSDT','interval':'1s','startTime':cur*1000,'endTime':(chunk_end+1)*1000,'limit':1000}
                    rows=get_json(BINANCE_KLINES_URL, params=params, timeout=30, attempts=6)
                    for k in rows:
                        ts=int(k[0])//1000
                        candles[ts]={'ts':ts,'open':float(k[1]),'high':float(k[2]),'low':float(k[3]),'close':float(k[4]),'volume':float(k[5]),'taker_buy_volume':float(k[9])}
                    cur=chunk_end+1
                day += timedelta(days=1); done += 1; continue
        try:
            with zipfile.ZipFile(zp) as z:
                member=z.namelist()[0]
                with z.open(member) as f:
                    for raw in f:
                        parts=raw.decode().strip().split(',')
                        if not parts or parts[0]=='open_time':
                            continue
                        t0=int(parts[0])
                        ts=t0//1000000 if t0>10_000_000_000_000 else t0//1000
                        if start_ts <= ts <= end_ts:
                            candles[ts]={'ts':ts,'open':float(parts[1]),'high':float(parts[2]),'low':float(parts[3]),'close':float(parts[4]),'volume':float(parts[5]),'taker_buy_volume':float(parts[9])}
        except Exception as e:
            print(f'bad zip {zp}: {e!r}', flush=True)
        done += 1
        if done%5==0 or done==nday:
            print(f'binance archive days={done}/{nday} candles={len(candles)} through={day}', flush=True)
        day += timedelta(days=1)
    meta={'generated_at':datetime.now(timezone.utc).isoformat(),'start_ts':start_ts,'end_ts':end_ts,'days':nday,'candles':len(candles),'zip_dir':str(zip_dir)}
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    (cache_path.with_suffix('.meta.json')).write_text(json.dumps(meta,indent=2))
    return candles

def load_existing_30d(cache):
    d=json.loads(Path(cache).read_text())
    ms=[]
    for m in d['market_inputs']:
        if m.get('price_to_beat') and m.get('final_price') and m.get('candles'):
            ms.append({'market_slug':m.get('market_slug'), 'start_ts':int(m['start_ts']), 'end_ts':int(m['end_ts']), 'price_to_beat':float(m['price_to_beat']), 'final_price':float(m['final_price']), 'candles':m['candles'], 'up_token':m.get('up_token'), 'down_token':m.get('down_token')})
    return sorted(ms,key=lambda m:m['start_ts'])

def build_larger_markets(days:int, out_dir:Path, fallback_cache:Path):
    gamma_cache=out_dir/f'gamma_btc5m_{days}d.json'
    binance_cache=out_dir/f'binance_1s_{days}d_preopen_open.json'
    try:
        g=load_gamma_btc_markets(days, gamma_cache)
        gm=g['markets']
        if not gm: raise RuntimeError('no gamma markets')
        min_ts=min(m['start_ts'] for m in gm)-180; max_ts=max(m['end_ts'] for m in gm)+1
        bc=fetch_binance_1s_range(min_ts,max_ts,binance_cache)
        markets=[]
        for m in gm:
            cs=[bc[t] for t in range(m['start_ts']-180, m['end_ts']+2) if t in bc]
            if len(cs)>=30:
                mm=dict(m); mm['candles']=cs; markets.append(mm)
        if len(markets) >= 10000:
            return markets, {'source':'fresh_gamma_binance','days':days,'gamma_markets':len(gm),'prepared_markets':len(markets),'cache_files':[str(gamma_cache),str(binance_cache)]}
        print(f'fresh fetch produced only {len(markets)} markets; falling back to existing cache', flush=True)
    except Exception as e:
        print(f'fresh larger fetch failed: {e!r}; falling back to existing cache', flush=True)
    ms=load_existing_30d(fallback_cache)
    return ms, {'source':'fallback_existing_30d_cache','prepared_markets':len(ms),'cache':str(fallback_cache)}

def eval_candidates(markets, variants):
    split=int(len(markets)*0.7)
    contexts=sorted({(v.params['frame'], v.params['lookback']) for v in variants})
    bars_cache={}
    for i,m in enumerate(markets):
        for frame,look in contexts:
            bars_cache[(i,frame,look)] = pre.aggregate_preopen(m['candles'], int(m['start_ts']), look, frame)
    summaries=[]; details=[]
    for num,v in enumerate(variants,1):
        wins=tr=up=dn=tw=tt=vw=vt=0; by_month=defaultdict(lambda:[0,0]); by_side=defaultdict(lambda:[0,0])
        for i,m in enumerate(markets):
            b=bars_cache[(i,v.params['frame'],v.params['lookback'])]
            s=pre.signal(v,b)
            if not s: continue
            outcome=1 if float(m['final_price'])>=float(m['price_to_beat']) else -1
            win=s==outcome
            tr+=1; wins+=win; up+=s>0; dn+=s<0
            if i<split: tt+=1; tw+=win
            else: vt+=1; vw+=win
            mo=datetime.utcfromtimestamp(int(m['start_ts'])).strftime('%Y-%m')
            by_month[mo][0]+=1; by_month[mo][1]+=int(win)
            by_side['UP' if s>0 else 'DOWN'][0]+=1; by_side['UP' if s>0 else 'DOWN'][1]+=int(win)
            details.append({'candidate_no':num,'family':v.family,'market_slug':m.get('market_slug'),'start_ts':m['start_ts'],'signal':'UP' if s>0 else 'DOWN','outcome':'UP' if outcome>0 else 'DOWN','win':win,'price_to_beat':m['price_to_beat'],'final_price':m['final_price']})
        summaries.append({'candidate_no':num,'family':v.family,'params':v.params,'trades':tr,'wins':wins,'losses':tr-wins,'hit_rate':wins/tr if tr else 0,'coverage':tr/len(markets) if markets else 0,'up_calls':up,'down_calls':dn,'train_trades':tt,'train_hit_rate':tw/tt if tt else 0,'test_trades':vt,'test_hit_rate':vw/vt if vt else 0,'by_month':{k:{'trades':a,'hit_rate':b/a if a else 0} for k,(a,b) in by_month.items()},'by_side':{k:{'trades':a,'hit_rate':b/a if a else 0} for k,(a,b) in by_side.items()}})
    return summaries, details

def load_token_maps_for_slugs(slugs, cache_path):
    cached=json.loads(cache_path.read_text()) if cache_path.exists() else {}
    changed=False
    for slug in slugs:
        if slug in cached and cached[slug]: continue
        try:
            data=get_json(GAMMA_EVENTS_URL, params={'slug':slug}, timeout=30)
            if not data or not data[0].get('markets'):
                cached[slug]={}
            else:
                m=data[0]['markets'][0]
                outs=m.get('outcomes') or []; toks=m.get('clobTokenIds') or []
                if isinstance(outs,str): outs=json.loads(outs)
                if isinstance(toks,str): toks=json.loads(toks)
                cached[slug]={str(tok):str(out).lower() for out,tok in zip(outs,toks)}
            changed=True; time.sleep(0.05)
        except Exception as e:
            cached[slug]={'_error':repr(e)}; changed=True
    if changed:
        cache_path.parent.mkdir(parents=True, exist_ok=True); cache_path.write_text(json.dumps(cached,indent=2,sort_keys=True))
    return cached

def run_l2(cronos_dir:Path, markets, variants, token_cache:Path, notional:float, open_window_s:float):
    import pyarrow.parquet as pq
    files=list(cronos_dir.glob('*.parquet'))
    slugs=sorted({f.name.split('_')[0] for f in files})
    token_maps=load_token_maps_for_slugs(slugs, token_cache)
    market_by_slug={m['market_slug']:m for m in markets if m.get('market_slug') in slugs}
    l2_markets=sorted(market_by_slug.values(), key=lambda m:m['start_ts'])
    # load book snapshots by (slug, side)
    books=defaultdict(list); meta=Counter(); unknown=Counter()
    for f in files:
        tab=pq.read_table(f, columns=['timestamp','slug','asset_id','bids','asks'])
        d=tab.to_pydict()
        for ts,slug,aid,bids_s,asks_s in zip(d['timestamp'],d['slug'],d['asset_id'],d['bids'],d['asks']):
            side=token_maps.get(slug,{}).get(str(aid))
            if side not in {'up','down'}:
                meta['unknown_asset_rows']+=1; unknown[(slug,str(aid))]+=1; continue
            try:
                asks_raw=json.loads(asks_s) if isinstance(asks_s,str) else asks_s
                asks=sorted((float(x['price']),float(x['size'])) for x in asks_raw if float(x.get('size',0))>0)
            except Exception:
                meta['bad_rows']+=1; continue
            books[(slug,side)].append((float(ts),asks)); meta['book_rows']+=1
    for k in list(books): books[k].sort(key=lambda x:x[0])
    def first_book(slug,side,start_ts):
        arr=books.get((slug,side),[]); times=[x[0] for x in arr]
        i=bisect.bisect_left(times,float(start_ts))
        if i>=len(arr): return None
        ts,asks=arr[i]
        if ts-float(start_ts)>open_window_s: return None
        return ts,asks
    def fill(asks, shares):
        rem=shares; cost=0; depth=sum(sz for _,sz in asks); levels=0
        for px,sz in asks:
            take=min(rem,sz)
            if take>0:
                cost+=take*px; rem-=take; levels+=1
            if rem<=1e-12: break
        return {'filled':rem<=1e-12,'avg_price':cost/shares if rem<=1e-12 else None,'depth':depth,'levels':levels,'best_ask':asks[0][0] if asks else None}
    summaries=[]; rows=[]
    contexts=sorted({(v.params['frame'], v.params['lookback']) for v in variants})
    bars_cache={}
    for i,m in enumerate(l2_markets):
        for frame,look in contexts:
            bars_cache[(i,frame,look)] = pre.aggregate_preopen(m['candles'], int(m['start_ts']), look, frame)
    for num,v in enumerate(variants,1):
        sigs=fills=0; pnl=[]; reasons=Counter(); wins=0
        for i,m in enumerate(l2_markets):
            s=pre.signal(v,bars_cache[(i,v.params['frame'],v.params['lookback'])])
            if not s: continue
            sigs+=1; side='up' if s>0 else 'down'; outcome=1 if m['final_price']>=m['price_to_beat'] else -1; win=s==outcome; wins+=int(win)
            b=first_book(m['market_slug'], side, m['start_ts'])
            row={'candidate_no':num,'family':v.family,'market_slug':m['market_slug'],'start_ts':m['start_ts'],'side':side,'direction_win':win,'strict_l2_filled':False}
            if not b:
                reasons['no_side_book_at_open_window']+=1; row['l2_reason']='no_side_book_at_open_window'; rows.append(row); continue
            bts,asks=b
            if not asks:
                reasons['empty_asks']+=1; row['l2_reason']='empty_asks'; rows.append(row); continue
            shares=notional/asks[0][0]
            fl=fill(asks, shares)
            row.update({'entry_book_ts':bts,'entry_book_age_s':bts-m['start_ts'],'entry_best_ask':fl['best_ask'],'entry_depth_shares':fl['depth'],'shares':shares})
            if not fl['filled']:
                reasons['insufficient_ask_depth']+=1; row['l2_reason']='insufficient_ask_depth'; rows.append(row); continue
            p=(1.0 if win else 0.0)*shares - fl['avg_price']*shares
            pnl.append(p); fills+=1
            row.update({'strict_l2_filled':True,'l2_reason':'filled_open_ask','entry_avg_price':fl['avg_price'],'l2_pnl':p})
            rows.append(row)
        summaries.append({'candidate_no':num,'family':v.family,'params':v.params,'l2_markets_total':len(l2_markets),'signals_on_l2_sample':sigs,'direction_hit_on_l2_signals':wins/sigs if sigs else 0,'strict_l2_fills':fills,'strict_l2_fill_rate_vs_signals':fills/sigs if sigs else 0,'l2_total_pnl':sum(pnl),'l2_avg_pnl':sum(pnl)/len(pnl) if pnl else 0,'l2_win_rate':sum(1 for x in pnl if x>0)/len(pnl) if pnl else 0,'l2_fail_reasons':dict(reasons)})
    return summaries, rows, {'cronos_dir':str(cronos_dir),'parquet_files':len(files),'unique_slugs':len(slugs),'markets_with_cache':len(l2_markets),'book_rows':meta['book_rows'],'unknown_asset_rows':meta['unknown_asset_rows'],'bad_rows':meta['bad_rows'],'open_window_s':open_window_s,'notional':notional}

def write_csv(path, rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    if not rows:
        path.write_text(''); return
    keys=[]
    for r in rows:
        for k in r:
            if k not in keys: keys.append(k)
    with path.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=keys,extrasaction='ignore'); w.writeheader(); w.writerows(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--days',type=int,default=60)
    ap.add_argument('--out-dir',type=Path,default=ROOT/'reports/preopen_trend_sweep/retest_large_l2')
    ap.add_argument('--fallback-cache',type=Path,default=ROOT/'data/backtests/cache/btc_5m_market_inputs_30d.json')
    ap.add_argument('--cronos-dir',type=Path,default=Path('/home/administrator/poly_search/repos/polymarket-BTC5min-database/market_data'))
    ap.add_argument('--token-cache',type=Path,default=ROOT/'data/backtests/cache/cronos_btc5m_token_maps.json')
    ap.add_argument('--notional',type=float,default=1.0)
    ap.add_argument('--open-window-s',type=float,default=15.0)
    args=ap.parse_args()
    variants=[pre.Variant(i,f,p) for i,(f,p) in enumerate(CANDIDATES,1)]
    markets, data_meta=build_larger_markets(args.days,args.out_dir,args.fallback_cache)
    directional, directional_rows=eval_candidates(markets,variants)
    l2_summary=[]; l2_rows=[]; l2_meta={}
    try:
        l2_summary,l2_rows,l2_meta=run_l2(args.cronos_dir,markets,variants,args.token_cache,args.notional,args.open_window_s)
    except Exception as e:
        l2_meta={'error':repr(e)}
    payload={'generated_at':datetime.now(timezone.utc).isoformat(),'method':{'directional':'pre-open signal before start_ts; outcome UP iff final_price >= price_to_beat; Polymarket price ignored','l2':'predicted side bought against first captured ask book at/after open within open_window_s, $notional, hold to settlement'},'data':data_meta,'directional_summary':directional,'l2_meta':l2_meta,'l2_summary':l2_summary,'artifacts':{}}
    out_json=args.out_dir/'preopen_10_large_l2_results.json'
    out_dir_csv=args.out_dir/'preopen_10_directional_summary.csv'
    out_l2_csv=args.out_dir/'preopen_10_l2_summary.csv'
    details_csv=args.out_dir/'preopen_10_directional_details.csv'
    l2_details_csv=args.out_dir/'preopen_10_l2_details.csv'
    flat=[]
    for s in directional:
        r=dict(s); r['params']=json.dumps(r['params'],sort_keys=True); r['by_month']=json.dumps(r['by_month'],sort_keys=True); r['by_side']=json.dumps(r['by_side'],sort_keys=True); flat.append(r)
    write_csv(out_dir_csv,flat)
    flat2=[]
    for s in l2_summary:
        r=dict(s); r['params']=json.dumps(r['params'],sort_keys=True); r['l2_fail_reasons']=json.dumps(r['l2_fail_reasons'],sort_keys=True); flat2.append(r)
    write_csv(out_l2_csv,flat2); write_csv(details_csv,directional_rows); write_csv(l2_details_csv,l2_rows)
    payload['artifacts']={'summary_json':str(out_json),'directional_summary_csv':str(out_dir_csv),'l2_summary_csv':str(out_l2_csv),'directional_details_csv':str(details_csv),'l2_details_csv':str(l2_details_csv)}
    out_json.write_text(json.dumps(payload,indent=2,sort_keys=True))
    print(json.dumps({'out':str(out_json),'data':data_meta,'directional':directional,'l2_meta':l2_meta,'l2':l2_summary},indent=2)[:20000])
if __name__=='__main__': main()
