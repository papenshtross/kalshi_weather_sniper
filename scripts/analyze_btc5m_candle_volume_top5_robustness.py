#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, json, sys
from pathlib import Path
from datetime import datetime, timezone
from statistics import mean
import importlib.util

FAST = Path('/home/administrator/projects/polybot/scripts/backtest_btc5m_candle_volume_reversal_360d_fast.py')
spec = importlib.util.spec_from_file_location('fast', FAST)
assert spec is not None and spec.loader is not None
fast = importlib.util.module_from_spec(spec)
sys.modules['fast'] = fast
spec.loader.exec_module(fast)
OUT = Path('/home/administrator/projects/polybot/reports/btc5m_candle_volume_reversal_360d')
summary=json.loads((OUT/'summary.json').read_text())
rows=fast.read_rows(summary['dataset']['csv'])
arrays=fast.prep(rows)
variants=[]
for r in summary['top_25'][:5]:
    p=r['params']
    variants.append(fast.Variant(r['variant_id'], p['vol_n'], p['spike_x'], p['min_body_pct'], p['spike_body_pct'], p['streak_min_for_reversal'], p['streak_max_continue'], p['require_spike_same_as_streak'], p['wick_exhaustion'], p['exhaustion_reversal'], p['use_taker_confirmation']))

def month_key(ms):
    return datetime.fromtimestamp(ms/1000,tz=timezone.utc).strftime('%Y-%m')

def eval_slice(start,end,v):
    colors,bodies,upper,lower,taker,vols,prefix,streak=arrays
    wins=tr=flips=fw=cw=ct=0
    s=max(start, v.vol_n+2, 3)
    for i in range(s,end):
        # duplicated signal loop, same as fast.eval_variant but slice-local stats
        p=i-1; base=colors[p]
        if base==0: base=1 if rows[p]['close']>=rows[p-1]['close'] else -1
        if bodies[p] < v.min_body_pct: base=1 if rows[p]['close']>=rows[p-1]['close'] else -1
        avg=(prefix[p]-prefix[p-v.vol_n])/v.vol_n; ratio=vols[p]/max(1e-12,avg)
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
        win=pred==actual; tr+=1; wins+=win
        if flip: flips+=1; fw+=win
        else: ct+=1; cw+=win
    return {'trades':tr,'hit_rate':wins/tr if tr else None,'flips':flips,'flip_rate':flips/tr if tr else 0,'flip_hit_rate':fw/flips if flips else None,'continuation_hit_rate':cw/ct if ct else None}

# monthly index ranges
months=[]; cur=None; start=0
for i,r in enumerate(rows):
    k=month_key(r['open_time'])
    if cur is None: cur=k; start=i
    elif k!=cur:
        months.append((cur,start,i)); cur=k; start=i
months.append((cur,start,len(rows)))

month_rows=[]
for v in variants:
    for m,s,e in months:
        rec=eval_slice(s,e,v); rec.update({'variant_id':v.id,'month':m}); month_rows.append(rec)

# rolling 30d / 8640 candle windows stepped 30d
window=30*24*12; step=window
rolling=[]
for v in variants:
    for s in range(0, len(rows)-window+1, step):
        e=s+window; rec=eval_slice(s,e,v)
        rec.update({'variant_id':v.id,'start_utc':datetime.fromtimestamp(rows[s]['open_time']/1000,tz=timezone.utc).isoformat(),'end_utc':datetime.fromtimestamp(rows[e-1]['open_time']/1000,tz=timezone.utc).isoformat()}); rolling.append(rec)

robust={}
for v in variants:
    ms=[r for r in month_rows if r['variant_id']==v.id and r['hit_rate'] is not None]
    rs=[r for r in rolling if r['variant_id']==v.id and r['hit_rate'] is not None]
    robust[str(v.id)]={
        'monthly_periods':len(ms),
        'monthly_profitable_over_50pct':sum(1 for r in ms if r['hit_rate']>0.5),
        'monthly_min_hit_rate':min(r['hit_rate'] for r in ms),
        'monthly_max_hit_rate':max(r['hit_rate'] for r in ms),
        'monthly_avg_hit_rate':mean(r['hit_rate'] for r in ms),
        'rolling_30d_windows':len(rs),
        'rolling_30d_over_50pct':sum(1 for r in rs if r['hit_rate']>0.5),
        'rolling_30d_min_hit_rate':min(r['hit_rate'] for r in rs),
        'rolling_30d_max_hit_rate':max(r['hit_rate'] for r in rs),
        'rolling_30d_avg_hit_rate':mean(r['hit_rate'] for r in rs),
    }

(OUT/'top5_monthly_robustness.json').write_text(json.dumps({'robustness':robust,'monthly':month_rows,'rolling_30d':rolling},indent=2,sort_keys=True))
with (OUT/'top5_monthly.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=['variant_id','month','trades','hit_rate','flips','flip_rate','flip_hit_rate','continuation_hit_rate']); w.writeheader(); w.writerows(month_rows)
with (OUT/'top5_rolling_30d.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=['variant_id','start_utc','end_utc','trades','hit_rate','flips','flip_rate','flip_hit_rate','continuation_hit_rate']); w.writeheader(); w.writerows(rolling)
print(json.dumps(robust,indent=2,sort_keys=True))
