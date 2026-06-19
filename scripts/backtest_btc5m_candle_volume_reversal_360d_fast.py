#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, json, random, math
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, median
from datetime import datetime, timezone
EPS=1e-12
@dataclass(frozen=True)
class Variant:
    id:int; vol_n:int; spike_x:float; min_body_pct:float; spike_body_pct:float; streak_min_for_reversal:int; streak_max_continue:int; require_spike_same_as_streak:bool; wick_exhaustion:float; exhaustion_reversal:bool; use_taker_confirmation:bool
@dataclass
class Result:
    variant_id:int; params:dict; trades:int; wins:int; losses:int; hit_rate:float; train_trades:int; train_hit_rate:float; test_trades:int; test_hit_rate:float; flips:int; flip_rate:float; flip_hit_rate:float; continuation_hit_rate:float; avg_spike_x_on_flips:float; score:float

def read_rows(path):
    rows=[]
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append({k: float(r[k]) for k in ['open','high','low','close','volume','taker_buy_base']} | {'open_time': int(r['open_time'])})
    return rows

def build_variants(total=1000,seed=42):
    r=random.Random(seed); variants=[]; seen=set()
    vol_ns=[3,5,8,10,13,21,34,55,89]; xs=[1.15,1.25,1.35,1.5,1.75,2.0,2.5,3.0,4.0]
    body=[0.0,0.10,0.20,0.30,0.45]; spike_body=[0.0,0.20,0.35,0.50,0.65]; streak_mins=[1,2,3,4,5]; streak_max=[0,3,5,8,13]; wicks=[0.0,0.45,0.55,0.65,0.75]
    while len(variants)<total:
        tup=(r.choice(vol_ns),r.choice(xs),r.choice(body),r.choice(spike_body),r.choice(streak_mins),r.choice(streak_max),r.choice([False,True]),r.choice(wicks),r.choice([False,True]),r.choice([False,True]))
        if tup in seen: continue
        seen.add(tup); variants.append(Variant(len(variants),*tup))
    return variants

def prep(rows):
    n=len(rows); colors=[]; bodies=[]; upper=[]; lower=[]; taker=[]; vols=[]; prefix=[0.0]
    for c in rows:
        col=1 if c['close']>c['open'] else -1 if c['close']<c['open'] else 0; colors.append(col)
        rng=max(EPS,c['high']-c['low']); bodies.append(abs(c['close']-c['open'])/rng); upper.append((c['high']-max(c['open'],c['close']))/rng); lower.append((min(c['open'],c['close'])-c['low'])/rng)
        vols.append(c['volume']); taker.append(c['taker_buy_base']/max(EPS,c['volume'])); prefix.append(prefix[-1]+c['volume'])
    streak=[0]*n
    for i,c in enumerate(colors): streak[i]=(streak[i-1]+1 if i and colors[i-1]==c else 1) if c else 0
    return colors,bodies,upper,lower,taker,vols,prefix,streak

def eval_variant(rows, arrays, v, split):
    colors,bodies,upper,lower,taker,vols,prefix,streak=arrays; n=len(rows); start=max(v.vol_n+2,3)
    trades=wins=tw=tt=vw=vt=flips=fw=cw=ct=0; ratios=[]
    for i in range(start,n):
        p=i-1; base=colors[p]
        if base==0:
            base=1 if rows[p]['close']>=rows[p-1]['close'] else -1
        if bodies[p] < v.min_body_pct:
            base=1 if rows[p]['close']>=rows[p-1]['close'] else -1
        avg=(prefix[p]-prefix[p-v.vol_n])/v.vol_n
        ratio=vols[p]/max(EPS,avg)
        spike=(avg>0 and ratio>=v.spike_x and bodies[p]>=v.spike_body_pct)
        if v.use_taker_confirmation:
            if (base==1 and taker[p]<0.52) or (base==-1 and taker[p]>0.48): spike=False
        flip=False
        if spike and streak[p] >= v.streak_min_for_reversal:
            if (not v.require_spike_same_as_streak) or colors[p]==base: flip=True
        if v.streak_max_continue and streak[p] >= v.streak_max_continue:
            wick_ok=(v.wick_exhaustion<=0 or (base==1 and upper[p]>=v.wick_exhaustion) or (base==-1 and lower[p]>=v.wick_exhaustion))
            if v.exhaustion_reversal and wick_ok: flip=True
        pred=-base if flip else base; actual=colors[i]
        if actual==0: continue
        win=pred==actual; trades+=1; wins+=win
        if i<split: tt+=1; tw+=win
        else: vt+=1; vw+=win
        if flip: flips+=1; fw+=win; ratios.append(ratio)
        else: ct+=1; cw+=win
    if not trades or not vt: return None
    hit=wins/trades; th=tw/max(1,tt); vh=vw/max(1,vt); flip_hr=fw/max(1,flips); cont_hr=cw/max(1,ct)
    score=vh+0.10*hit-0.20*abs(th-vh)+0.03*flip_hr-0.02*abs((flips/max(1,trades))-0.12)
    return Result(v.id,{k:val for k,val in asdict(v).items() if k!='id'},trades,wins,trades-wins,hit,tt,th,vt,vh,flips,flips/trades,flip_hr,cont_hr,mean(ratios) if ratios else 0.0,score)

def vol_stats(rows,arrays,ns):
    colors,bodies,upper,lower,taker,vols,prefix,streak=arrays; out={}
    for n in ns:
        rev=[]; cont=[]
        for i in range(n+2,len(rows)):
            if colors[i-1]==0 or colors[i]==0: continue
            avg=(prefix[i-1]-prefix[i-1-n])/n; ratio=vols[i-1]/max(EPS,avg)
            (rev if colors[i]==-colors[i-1] else cont).append(ratio)
        def q(a,p):
            b=sorted(a); return b[min(len(b)-1,int((len(b)-1)*p))]
        out[str(n)]={'reversal_count':len(rev),'continuation_count':len(cont),'reversal_mean_ratio':mean(rev),'continuation_mean_ratio':mean(cont),'reversal_median_ratio':median(rev),'continuation_median_ratio':median(cont),'reversal_p75_ratio':q(rev,.75),'reversal_p90_ratio':q(rev,.90),'continuation_p90_ratio':q(cont,.90)}
    return out

def iso(ms): return datetime.fromtimestamp(ms/1000,tz=timezone.utc).isoformat()
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--csv',default='/home/administrator/projects/polybot/reports/btc5m_candle_volume_reversal_360d/binance_btcusdt_5m_360d.csv'); ap.add_argument('--variants',type=int,default=1000); ap.add_argument('--seed',type=int,default=20260606); ap.add_argument('--out-dir',default='/home/administrator/projects/polybot/reports/btc5m_candle_volume_reversal_360d')
    a=ap.parse_args(); out=Path(a.out_dir); out.mkdir(parents=True,exist_ok=True); rows=read_rows(a.csv); arrays=prep(rows); split=int(len(rows)*.70); vars=build_variants(a.variants,a.seed)
    res=[eval_variant(rows,arrays,v,split) for v in vars]; res=[r for r in res if r]; res.sort(key=lambda r:(r.score,r.test_hit_rate,r.hit_rate), reverse=True)
    stats=vol_stats(rows,arrays,sorted({v.vol_n for v in vars}))
    report={'assumptions':{'data':'Binance BTCUSDT 5m OHLCV; each closed 5m candle approximates one Polymarket BTC 5m up/down market.','resolution':'UP if close > open; DOWN if close < open; flats skipped for scoring.','entry':'Signal for candle i uses candle i-1 and earlier only; no Polymarket price/PnL/spread.','base_rule':'previous candle green -> UP, previous candle red -> DOWN; volume spike/streak exhaustion can flip to reversal.','stake_model':'directional hit-rate only for fixed-size every-market bets.'},'dataset':{'rows':len(rows),'start_utc':iso(rows[0]['open_time']),'end_utc':iso(rows[-1]['open_time']),'train_rows':split,'test_rows':len(rows)-split,'csv':a.csv},'variants_evaluated':len(res),'volume_reversal_stats_by_n':stats,'top_25':[asdict(r) for r in res[:25]]}
    (out/'summary.json').write_text(json.dumps(report,indent=2,sort_keys=True))
    with (out/'top_25.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(asdict(res[0]).keys())); w.writeheader(); w.writerows(asdict(r) for r in res[:25])
    print(json.dumps({'dataset':report['dataset'],'top_5':[asdict(r) for r in res[:5]],'summary':str(out/'summary.json')},indent=2,sort_keys=True))
if __name__=='__main__': main()
