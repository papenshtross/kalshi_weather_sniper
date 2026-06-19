#!/usr/bin/env python3
from __future__ import annotations
"""Retest 10 BTC 5m pre-open candidates using preferred SII Polymarket_data.

Data policy:
- Polymarket market labels/outcomes come from SII-WANGZJ/Polymarket_data markets.parquet.
- warproxxx/poly_data was checked; local generated data is absent, and its repo is a pipeline.
- Binance 1s archives are supplemental because preferred Polymarket repos do not contain BTCUSDT pre-open candles.

Research-only: no live services, no orders.
"""
import argparse, ast, csv, json, sys, time, zipfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
EXTRACTED = ROOT / 'reports/preopen_trend_sweep/extracted_research_2'
sys.path.insert(0, str(EXTRACTED / 'scripts'))
import backtest_btc5m_preopen_trend_sweep as pre  # noqa

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

def ts_to_day(ts:int):
    return datetime.fromtimestamp(ts, timezone.utc).date()

def ensure_sii_markets(path:Path):
    if path.exists():
        return path
    from huggingface_hub import hf_hub_download
    path.parent.mkdir(parents=True, exist_ok=True)
    p = hf_hub_download('SII-WANGZJ/Polymarket_data', filename='markets.parquet', repo_type='dataset', local_dir=str(path.parent))
    return Path(p)

def parse_prices(s:str):
    try:
        v=ast.literal_eval(s)
        return [float(x) for x in v]
    except Exception:
        try: return [float(x) for x in json.loads(s)]
        except Exception: return []

def load_sii_btc5m(markets_path:Path, min_start:int|None=None, max_start:int|None=None):
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    cols=['slug','question','event_title','outcome_prices','closed','active','answer1','answer2','end_date']
    t=pq.read_table(markets_path, columns=cols)
    mask=pc.match_substring(t['slug'], 'btc-updown-5m-')
    sub=t.filter(mask).to_pylist()
    out=[]; skipped=0
    seen=set()
    for r in sub:
        slug=str(r['slug'])
        if slug in seen: continue
        seen.add(slug)
        try: st=int(slug.rsplit('-',1)[-1])
        except Exception: skipped+=1; continue
        if min_start and st < min_start: continue
        if max_start and st > max_start: continue
        prices=parse_prices(str(r.get('outcome_prices')))
        if len(prices)<2: skipped+=1; continue
        # Schema has answer1=Up, answer2=Down for BTC 5m rows in SII. Winner is final outcome price 1.
        if prices[0] == prices[1]: skipped+=1; continue
        outcome=1 if prices[0] > prices[1] else -1
        out.append({'market_slug':slug,'question':r.get('question') or r.get('event_title') or slug,'start_ts':st,'end_ts':st+300,'outcome':outcome,'outcome_prices':prices,'source_end_date':str(r.get('end_date'))})
    out.sort(key=lambda m:m['start_ts'])
    return out, {'sii_btc5m_rows':len(sub),'prepared_markets':len(out),'skipped':skipped,'min_start_ts':out[0]['start_ts'] if out else None,'max_start_ts':out[-1]['start_ts'] if out else None}

def download_daily_zip(day, zip_dir):
    name=f'BTCUSDT-1s-{day.isoformat()}.zip'
    zp=zip_dir/name
    if zp.exists() and zp.stat().st_size>1000:
        return zp
    url=f'https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s/{name}'
    req=Request(url, headers={'User-Agent':'Hermes SII large preopen retest'})
    with urlopen(req, timeout=90) as r:
        zp.write_bytes(r.read())
    return zp

def load_binance_candles_for_range(start_ts:int, end_ts:int, out_dir:Path):
    zip_dir=out_dir/'binance_1s_daily_zips'
    zip_dir.mkdir(parents=True, exist_ok=True)
    candles={}
    d0=ts_to_day(start_ts); d1=ts_to_day(end_ts)
    day=d0; total=(d1-d0).days+1; done=0; misses=[]
    while day<=d1:
        try:
            zp=download_daily_zip(day, zip_dir)
            with zipfile.ZipFile(zp) as z:
                member=z.namelist()[0]
                with z.open(member) as f:
                    for raw in f:
                        parts=raw.decode().strip().split(',')
                        if not parts or parts[0]=='open_time': continue
                        t0=int(parts[0]); ts=t0//1000000 if t0>10_000_000_000_000 else t0//1000
                        if start_ts <= ts <= end_ts:
                            candles[ts]={'ts':ts,'open':float(parts[1]),'high':float(parts[2]),'low':float(parts[3]),'close':float(parts[4]),'volume':float(parts[5]),'taker_buy_volume':float(parts[9])}
        except Exception as e:
            misses.append({'day':day.isoformat(),'error':repr(e)})
        done+=1
        if done%10==0 or done==total:
            print(f'binance days {done}/{total} candles={len(candles)} misses={len(misses)} through={day}', flush=True)
        day+=timedelta(days=1)
    return candles, {'zip_dir':str(zip_dir),'days_requested':total,'candles':len(candles),'misses':misses}

def attach_candles(markets, candles):
    out=[]; skipped=0
    for m in markets:
        cs=[candles[t] for t in range(m['start_ts']-180, m['end_ts']+2) if t in candles]
        if len(cs) < 60:
            skipped += 1; continue
        mm=dict(m); mm['candles']=cs; out.append(mm)
    return out, skipped

def eval_candidates(markets, variants):
    split=int(len(markets)*0.7)
    contexts=sorted({(v.params['frame'], v.params['lookback']) for v in variants})
    bars_cache={}
    for i,m in enumerate(markets):
        for frame,look in contexts:
            bars_cache[(i,frame,look)] = pre.aggregate_preopen(m['candles'], int(m['start_ts']), look, frame)
    summaries=[]; details=[]
    for num,v in enumerate(variants,1):
        wins=tr=up=dn=tw=tt=vw=vt=0
        by_month=defaultdict(lambda:[0,0]); by_side=defaultdict(lambda:[0,0])
        for i,m in enumerate(markets):
            s=pre.signal(v,bars_cache[(i,v.params['frame'],v.params['lookback'])])
            if not s: continue
            outcome=int(m['outcome'])
            win=s==outcome
            tr+=1; wins+=int(win); up+=int(s>0); dn+=int(s<0)
            if i<split: tt+=1; tw+=int(win)
            else: vt+=1; vw+=int(win)
            mo=datetime.utcfromtimestamp(m['start_ts']).strftime('%Y-%m')
            by_month[mo][0]+=1; by_month[mo][1]+=int(win)
            side='UP' if s>0 else 'DOWN'
            by_side[side][0]+=1; by_side[side][1]+=int(win)
            details.append({'candidate_no':num,'family':v.family,'market_slug':m['market_slug'],'start_ts':m['start_ts'],'signal':side,'outcome':'UP' if outcome>0 else 'DOWN','win':win})
        summaries.append({'candidate_no':num,'family':v.family,'params':v.params,'trades':tr,'wins':wins,'losses':tr-wins,'hit_rate':wins/tr if tr else 0,'coverage':tr/len(markets) if markets else 0,'up_calls':up,'down_calls':dn,'train_trades':tt,'train_hit_rate':tw/tt if tt else 0,'test_trades':vt,'test_hit_rate':vw/vt if vt else 0,'by_month':{k:{'trades':a,'hit_rate':b/a if a else 0} for k,(a,b) in by_month.items()},'by_side':{k:{'trades':a,'hit_rate':b/a if a else 0} for k,(a,b) in by_side.items()}})
    return summaries, details

def write_csv(path:Path, rows:list[dict[str,Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text(''); return
    keys=[]
    for r in rows:
        for k in r:
            if k not in keys: keys.append(k)
    with path.open('w', newline='') as f:
        w=csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
        w.writeheader(); w.writerows(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--sii-markets', type=Path, default=ROOT/'data/preferred_sources/SII-WANGZJ_Polymarket_data/markets.parquet')
    ap.add_argument('--out-dir', type=Path, default=ROOT/'reports/preopen_trend_sweep/sii_large_retest')
    ap.add_argument('--min-start', type=int, default=None)
    ap.add_argument('--max-start', type=int, default=None)
    args=ap.parse_args()
    variants=[pre.Variant(i,f,p) for i,(f,p) in enumerate(CANDIDATES,1)]
    sii_path=ensure_sii_markets(args.sii_markets)
    raw_markets, sii_meta=load_sii_btc5m(sii_path, args.min_start, args.max_start)
    if not raw_markets: raise SystemExit('no SII BTC 5m markets loaded')
    candle_start=min(m['start_ts'] for m in raw_markets)-180
    candle_end=max(m['end_ts'] for m in raw_markets)+1
    candles, candle_meta=load_binance_candles_for_range(candle_start,candle_end,args.out_dir)
    markets, skipped_candles=attach_candles(raw_markets,candles)
    summaries, details=eval_candidates(markets,variants)
    out_json=args.out_dir/'sii_preopen_10_large_results.json'
    sum_csv=args.out_dir/'sii_preopen_10_large_summary.csv'
    det_csv=args.out_dir/'sii_preopen_10_large_details.csv'
    flat=[]
    for s in summaries:
        r=dict(s); r['params']=json.dumps(r['params'],sort_keys=True); r['by_month']=json.dumps(r['by_month'],sort_keys=True); r['by_side']=json.dumps(r['by_side'],sort_keys=True); flat.append(r)
    write_csv(sum_csv,flat); write_csv(det_csv,details)
    payload={'generated_at':datetime.now(timezone.utc).isoformat(),'method':{'polymarket_source':'SII-WANGZJ/Polymarket_data markets.parquet','supplemental_signal_source':'Binance BTCUSDT 1s daily archives because preferred Polymarket repos do not contain BTC candles','rule':'pre-open signal before start_ts; outcome from SII outcome_prices: Up if Up outcome price > Down outcome price'},'data':{'sii':sii_meta,'binance':candle_meta,'markets_with_candles':len(markets),'skipped_for_missing_candles':skipped_candles},'summaries':summaries,'artifacts':{'summary_csv':str(sum_csv),'details_csv':str(det_csv)}}
    out_json.write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(json.dumps({'out':str(out_json),'data':payload['data'],'summaries':summaries}, indent=2)[:30000])
if __name__ == '__main__': main()
