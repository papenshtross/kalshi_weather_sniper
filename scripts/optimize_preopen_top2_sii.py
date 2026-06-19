#!/usr/bin/env python3
from __future__ import annotations
"""Optimize variations around top two BTC 5m pre-open candidates.

Preferred data-source policy:
- Polymarket markets/outcomes from SII-WANGZJ/Polymarket_data markets.parquet.
- Binance BTCUSDT 1s archives only for BTC pre-open candles, which the Polymarket repos do not contain.

Base candidates varied:
1) BB breakout: frame=5, lookback=60, n=10, k=2.5, fade=false
2) SuperTrend: frame=30, lookback=180, n=5, mult=3

Adds improvements:
- UP-only / DOWN-only / both-side versions
- volume pressure filters from pre-open taker buy ratio
- confirmation hybrids requiring BB and SuperTrend agreement
- OR hybrids with side filters
"""
import argparse, csv, json, sys, math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'))
import retest_preopen_10_sii_large as data_src  # noqa
EXTRACTED=ROOT/'reports/preopen_trend_sweep/extracted_research_2'
sys.path.insert(0, str(EXTRACTED/'scripts'))
import backtest_btc5m_preopen_trend_sweep as pre  # noqa


def base_signal(family:str, params:dict[str,Any], bars:list[dict[str,float]])->int:
    return pre.signal(pre.Variant(0, family, params), bars)


def volume_pressure_ok(sig:int, bars:list[dict[str,float]], threshold:float|None)->bool:
    if threshold is None:
        return True
    vol=sum(float(b.get('volume') or 0) for b in bars)
    if vol <= 1e-12:
        return False
    buy=sum(float(b.get('taker_buy_volume') or 0) for b in bars)
    ratio=buy/vol
    if sig > 0:
        return ratio >= threshold
    return ratio <= (1-threshold)


def side_filter_ok(sig:int, side:str)->bool:
    if side == 'both': return True
    if side == 'up': return sig > 0
    if side == 'down': return sig < 0
    return True


def signal_spec(spec:dict[str,Any], bars_cache:dict[tuple[int,int],list[dict[str,float]]], idx:int)->int:
    kind=spec['kind']
    side=spec.get('side','both')
    vol_thr=spec.get('vol_pressure')
    if kind in {'bb_break','supertrend'}:
        p=spec['params']
        bars=bars_cache[(idx,p['frame'],p['lookback'])]
        s=base_signal(kind,p,bars)
        if not s or not side_filter_ok(s,side) or not volume_pressure_ok(s,bars,vol_thr): return 0
        return s
    if kind == 'confirm_agree':
        bp=spec['bb']; sp=spec['st']
        bbars=bars_cache[(idx,bp['frame'],bp['lookback'])]
        sbars=bars_cache[(idx,sp['frame'],sp['lookback'])]
        s1=base_signal('bb_break',bp,bbars); s2=base_signal('supertrend',sp,sbars)
        if not s1 or s1 != s2: return 0
        bars=sbars if spec.get('vol_on')=='st' else bbars
        if not side_filter_ok(s1,side) or not volume_pressure_ok(s1,bars,vol_thr): return 0
        return s1
    if kind == 'or_priority':
        # use BB when available, else ST; optional agreement_boost filters disagreements if set
        bp=spec['bb']; sp=spec['st']
        bbars=bars_cache[(idx,bp['frame'],bp['lookback'])]
        sbars=bars_cache[(idx,sp['frame'],sp['lookback'])]
        s1=base_signal('bb_break',bp,bbars); s2=base_signal('supertrend',sp,sbars)
        if spec.get('require_no_conflict') and s1 and s2 and s1 != s2:
            return 0
        s=s1 or s2
        if not s: return 0
        bars=bbars if s1 else sbars
        if not side_filter_ok(s,side) or not volume_pressure_ok(s,bars,vol_thr): return 0
        return s
    return 0


def make_variations()->list[dict[str,Any]]:
    specs=[]
    def add(name, kind, **kw):
        specs.append({'id':len(specs)+1,'name':name,'kind':kind,**kw})
    # 40 BB variations around winner #2
    bb_frames=[1,3,5,10]
    bb_looks=[45,60,90,120,180]
    bb_ns=[8,10,13,20]
    bb_ks=[2.0,2.25,2.5,2.75,3.0]
    seeds=[]
    for frame in bb_frames:
        for look in bb_looks:
            for n in bb_ns:
                for k in bb_ks:
                    if look >= n*frame:
                        seeds.append((abs(frame-5)+abs(look-60)/30+abs(n-10)/5+abs(k-2.5),frame,look,n,k))
    for _,frame,look,n,k in sorted(seeds)[:24]:
        add(f'bb_f{frame}_l{look}_n{n}_k{k}_both','bb_break',params={'frame':frame,'lookback':look,'n':n,'k':k,'fade':False},side='both')
    for _,frame,look,n,k in sorted(seeds)[:8]:
        add(f'bb_f{frame}_l{look}_n{n}_k{k}_up','bb_break',params={'frame':frame,'lookback':look,'n':n,'k':k,'fade':False},side='up')
    for _,frame,look,n,k in sorted(seeds)[:4]:
        add(f'bb_f{frame}_l{look}_n{n}_k{k}_up_vol55','bb_break',params={'frame':frame,'lookback':look,'n':n,'k':k,'fade':False},side='up',vol_pressure=0.55)
    for _,frame,look,n,k in sorted(seeds)[:4]:
        add(f'bb_f{frame}_l{look}_n{n}_k{k}_down','bb_break',params={'frame':frame,'lookback':look,'n':n,'k':k,'fade':False},side='down')
    # 40 SuperTrend variations around winner #3
    st_frames=[15,30,45]
    st_looks=[90,120,150,180,240]
    st_ns=[3,5,7,10]
    st_mults=[2,2.5,3,3.5,4]
    st=[]
    for frame in st_frames:
        for look in st_looks:
            for n in st_ns:
                for mult in st_mults:
                    if look >= (n+1)*frame:
                        st.append((abs(frame-30)/15+abs(look-180)/30+abs(n-5)/2+abs(mult-3),frame,look,n,mult))
    for _,frame,look,n,mult in sorted(st)[:24]:
        add(f'st_f{frame}_l{look}_n{n}_m{mult}_both','supertrend',params={'frame':frame,'lookback':look,'n':n,'mult':mult},side='both')
    for _,frame,look,n,mult in sorted(st)[:8]:
        add(f'st_f{frame}_l{look}_n{n}_m{mult}_up','supertrend',params={'frame':frame,'lookback':look,'n':n,'mult':mult},side='up')
    for _,frame,look,n,mult in sorted(st)[:4]:
        add(f'st_f{frame}_l{look}_n{n}_m{mult}_up_vol55','supertrend',params={'frame':frame,'lookback':look,'n':n,'mult':mult},side='up',vol_pressure=0.55)
    for _,frame,look,n,mult in sorted(st)[:4]:
        add(f'st_f{frame}_l{look}_n{n}_m{mult}_down','supertrend',params={'frame':frame,'lookback':look,'n':n,'mult':mult},side='down')
    # 20 hybrids/improvements
    bb_top=[{'frame':5,'lookback':60,'n':10,'k':2.5,'fade':False}, {'frame':5,'lookback':60,'n':13,'k':2.5,'fade':False}, {'frame':5,'lookback':90,'n':10,'k':2.5,'fade':False}, {'frame':10,'lookback':60,'n':10,'k':2.5,'fade':False}]
    st_top=[{'frame':30,'lookback':180,'n':5,'mult':3}, {'frame':30,'lookback':150,'n':5,'mult':3}, {'frame':30,'lookback':180,'n':7,'mult':3}, {'frame':30,'lookback':120,'n':5,'mult':2.5}, {'frame':15,'lookback':180,'n':7,'mult':3}]
    c=0
    for bp in bb_top:
        for sp in st_top:
            if c<10:
                add(f'confirm_bb{bp["frame"]}_{bp["lookback"]}_{bp["n"]}_{bp["k"]}_st{sp["frame"]}_{sp["lookback"]}_{sp["n"]}_{sp["mult"]}','confirm_agree',bb=bp,st=sp,side='both')
            elif c<15:
                add(f'confirm_up_bb{bp["frame"]}_{bp["lookback"]}_{bp["n"]}_{bp["k"]}_st{sp["frame"]}_{sp["lookback"]}_{sp["n"]}_{sp["mult"]}','confirm_agree',bb=bp,st=sp,side='up')
            elif c<20:
                add(f'or_noconflict_bb{bp["frame"]}_{bp["lookback"]}_{bp["n"]}_{bp["k"]}_st{sp["frame"]}_{sp["lookback"]}_{sp["n"]}_{sp["mult"]}','or_priority',bb=bp,st=sp,side='up',require_no_conflict=True)
            c+=1
            if c>=20: break
        if c>=20: break
    assert len(specs)==100, len(specs)
    return specs


def score_result(r:dict[str,Any])->float:
    # Require usable sample, then prefer OOS/test hit, stability, and coverage.
    if r['trades'] < 300 or r['test_trades'] < 100:
        return -999 + r['test_hit_rate']
    return r['test_hit_rate'] + 0.15*r['hit_rate'] + 0.03*min(0.20,r['coverage']) - 0.25*abs(r['train_hit_rate']-r['test_hit_rate'])


def eval_specs(markets:list[dict[str,Any]], specs:list[dict[str,Any]]):
    split=int(len(markets)*0.7)
    contexts=set()
    for s in specs:
        if s['kind'] in {'bb_break','supertrend'}:
            p=s['params']; contexts.add((p['frame'],p['lookback']))
        else:
            for key in ['bb','st']:
                p=s[key]; contexts.add((p['frame'],p['lookback']))
    contexts=sorted(contexts)
    bars_cache={}
    for i,m in enumerate(markets):
        for frame,look in contexts:
            bars_cache[(i,frame,look)]=pre.aggregate_preopen(m['candles'], int(m['start_ts']), look, frame)
    summaries=[]; details=[]
    for spec in specs:
        tr=wins=up=dn=tt=tw=vt=vw=0
        by_month=defaultdict(lambda:[0,0]); by_side=defaultdict(lambda:[0,0])
        for i,m in enumerate(markets):
            sig=signal_spec(spec,bars_cache,i)
            if not sig: continue
            outcome=int(m['outcome']); win=sig==outcome
            tr+=1; wins+=int(win); up+=int(sig>0); dn+=int(sig<0)
            if i<split: tt+=1; tw+=int(win)
            else: vt+=1; vw+=int(win)
            mo=datetime.utcfromtimestamp(m['start_ts']).strftime('%Y-%m')
            by_month[mo][0]+=1; by_month[mo][1]+=int(win)
            side='UP' if sig>0 else 'DOWN'; by_side[side][0]+=1; by_side[side][1]+=int(win)
            details.append({'variant_id':spec['id'],'variant_name':spec['name'],'market_slug':m['market_slug'],'start_ts':m['start_ts'],'signal':side,'outcome':'UP' if outcome>0 else 'DOWN','win':win})
        r={'variant_id':spec['id'],'name':spec['name'],'kind':spec['kind'],'spec':spec,'trades':tr,'wins':wins,'losses':tr-wins,'hit_rate':wins/tr if tr else 0,'coverage':tr/len(markets) if markets else 0,'up_calls':up,'down_calls':dn,'train_trades':tt,'train_hit_rate':tw/tt if tt else 0,'test_trades':vt,'test_hit_rate':vw/vt if vt else 0,'by_month':{k:{'trades':a,'hit_rate':b/a if a else 0} for k,(a,b) in by_month.items()},'by_side':{k:{'trades':a,'hit_rate':b/a if a else 0} for k,(a,b) in by_side.items()}}
        r['score']=score_result(r)
        summaries.append(r)
    summaries.sort(key=lambda r:(r['score'],r['test_hit_rate'],r['hit_rate'],r['trades']), reverse=True)
    return summaries, details


def write_csv(path:Path, rows:list[dict[str,Any]]):
    path.parent.mkdir(parents=True,exist_ok=True)
    if not rows:
        path.write_text(''); return
    flat=[]
    for r in rows:
        rr=dict(r)
        for k in ['spec','by_month','by_side']:
            if k in rr: rr[k]=json.dumps(rr[k],sort_keys=True)
        flat.append(rr)
    keys=[]
    for r in flat:
        for k in r:
            if k not in keys: keys.append(k)
    with path.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=keys,extrasaction='ignore'); w.writeheader(); w.writerows(flat)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--out-dir',type=Path,default=ROOT/'reports/preopen_trend_sweep/sii_top2_100_variations')
    ap.add_argument('--sii-markets', type=Path, default=ROOT/'data/preferred_sources/SII-WANGZJ_Polymarket_data/markets.parquet')
    args=ap.parse_args()
    specs=make_variations()
    sii_path=data_src.ensure_sii_markets(args.sii_markets)
    raw, sii_meta=data_src.load_sii_btc5m(sii_path)
    candle_start=min(m['start_ts'] for m in raw)-180
    candle_end=max(m['end_ts'] for m in raw)+1
    candles, candle_meta=data_src.load_binance_candles_for_range(candle_start,candle_end,ROOT/'reports/preopen_trend_sweep/sii_large_retest')
    markets, skipped=data_src.attach_candles(raw,candles)
    summaries, details=eval_specs(markets,specs)
    out_json=args.out_dir/'top2_100_variations_results.json'
    sum_csv=args.out_dir/'top2_100_variations_summary.csv'
    det_csv=args.out_dir/'top2_100_variations_details.csv'
    write_csv(sum_csv,summaries); write_csv(det_csv,details)
    payload={'generated_at':datetime.now(timezone.utc).isoformat(),'data':{'polymarket_source':'SII-WANGZJ/Polymarket_data markets.parquet','sii':sii_meta,'binance':candle_meta,'markets_with_candles':len(markets),'skipped_for_missing_candles':skipped},'variation_count':len(specs),'selection_score':'test_hit + 0.15*all_hit + 0.03*coverage_capped - 0.25*train_test_gap, with trades>=300 and test_trades>=100; lower samples penalized','top5':summaries[:5],'all_summaries':summaries,'artifacts':{'summary_csv':str(sum_csv),'details_csv':str(det_csv)}}
    out_json.parent.mkdir(parents=True,exist_ok=True); out_json.write_text(json.dumps(payload,indent=2,sort_keys=True))
    print(json.dumps({'out':str(out_json),'data':payload['data'],'top5':summaries[:5]},indent=2)[:30000])
if __name__=='__main__': main()
