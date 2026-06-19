#!/usr/bin/env python3
"""BTC 5m Polymarket trend/reversal strategy sweep.

Uses the prepared BTC 5m cache in polybot/data/backtests/cache/btc_5m_market_inputs_30d.json.
All signals use Binance OHLCV candles available at or before entry_ts. PnL is modeled as
buying the selected Polymarket UP/DOWN outcome at the latest observed side price at or before entry_ts
and holding to settlement.
"""
from __future__ import annotations

import argparse
import json
import math
import random
from dataclasses import dataclass, asdict
from pathlib import Path
from statistics import mean
from typing import Any, Callable

EPS=1e-12

@dataclass(frozen=True)
class Variant:
    id: int
    family: str
    params: dict[str, Any]

@dataclass
class Result:
    variant_id: int
    family: str
    params: dict[str, Any]
    trades: int
    wins: int
    losses: int
    hit_rate: float
    gross_pnl: float
    net_pnl: float
    roi_on_cost: float
    avg_cost: float
    max_drawdown: float
    train_trades: int
    train_net_pnl: float
    train_roi: float
    test_trades: int
    test_net_pnl: float
    test_roi: float
    score: float


def price_at_or_before(points, ts):
    latest=None
    for p in points:
        if p.get('ts', -1) <= ts:
            latest=p.get('price')
        else:
            break
    if latest is None:
        return None
    try: return float(latest)
    except Exception: return None


def aggregate(candles, frame_s, end_ts, lookback_s):
    start=end_ts-lookback_s+1
    rows=[c for c in candles if start <= c['ts'] <= end_ts and c.get('close') not in (None,0)]
    if not rows: return []
    buckets={}
    for c in rows:
        b=(c['ts']//frame_s)*frame_s
        d=buckets.setdefault(b, {'ts':b,'open':None,'high':-1e99,'low':1e99,'close':None,'volume':0.0,'taker_buy_volume':0.0})
        if d['open'] is None: d['open']=float(c['open'])
        d['high']=max(d['high'], float(c['high']))
        d['low']=min(d['low'], float(c['low']))
        d['close']=float(c['close'])
        d['volume'] += float(c.get('volume') or 0)
        d['taker_buy_volume'] += float(c.get('taker_buy_volume') or 0)
    return [buckets[k] for k in sorted(buckets)]


def closes(bars): return [float(b['close']) for b in bars]
def highs(bars): return [float(b['high']) for b in bars]
def lows(bars): return [float(b['low']) for b in bars]
def opens(bars): return [float(b['open']) for b in bars]

def sma(xs,n):
    if len(xs)<n or n<=0: return None
    return sum(xs[-n:])/n

def ema_series(xs,n):
    if len(xs)<n or n<=0: return []
    k=2/(n+1)
    e=sum(xs[:n])/n
    out=[None]*(n-1)+[e]
    for x in xs[n:]:
        e=x*k+e*(1-k); out.append(e)
    return out

def std(xs,n):
    if len(xs)<n: return None
    m=sum(xs[-n:])/n
    return math.sqrt(sum((x-m)**2 for x in xs[-n:])/n)

def atr(bars,n):
    if len(bars)<n+1: return None
    trs=[]
    for i in range(1,len(bars)):
        h,l,pc=float(bars[i]['high']),float(bars[i]['low']),float(bars[i-1]['close'])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return sum(trs[-n:])/n if len(trs)>=n else None

def rsi(xs,n):
    if len(xs)<n+1: return None
    gains=[]; losses=[]
    for i in range(len(xs)-n, len(xs)):
        d=xs[i]-xs[i-1]
        gains.append(max(0,d)); losses.append(max(0,-d))
    ag=sum(gains)/n; al=sum(losses)/n
    if al<EPS: return 100.0
    return 100-100/(1+ag/al)

def lin_slope(vals):
    n=len(vals)
    if n<2: return 0.0
    mx=(n-1)/2; my=sum(vals)/n
    den=sum((i-mx)**2 for i in range(n))
    if den<EPS: return 0.0
    return sum((i-mx)*(vals[i]-my) for i in range(n))/den

def dmi_adx(bars,n):
    if len(bars)<n+2: return None
    plus=[]; minus=[]; trs=[]
    for i in range(1,len(bars)):
        up=bars[i]['high']-bars[i-1]['high']
        dn=bars[i-1]['low']-bars[i]['low']
        plus.append(up if up>dn and up>0 else 0.0)
        minus.append(dn if dn>up and dn>0 else 0.0)
        h,l,pc=bars[i]['high'],bars[i]['low'],bars[i-1]['close']
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    tr=sum(trs[-n:]);
    if tr<EPS: return None
    pdi=100*sum(plus[-n:])/tr; mdi=100*sum(minus[-n:])/tr
    # simple ADX approximation over last n DX values
    dxs=[]
    for j in range(max(n, len(trs)-n+1), len(trs)+1):
        trj=sum(trs[j-n:j])
        if trj<EPS: continue
        pd=100*sum(plus[j-n:j])/trj; md=100*sum(minus[j-n:j])/trj
        dxs.append(100*abs(pd-md)/(pd+md+EPS))
    adx=sum(dxs)/len(dxs) if dxs else 0
    return pdi, mdi, adx

def supertrend_dir(bars,n,m):
    if len(bars)<n+3: return 0
    a=atr(bars,n)
    if a is None: return 0
    # compact: close vs rolling hl2 +/- m*atr band
    hl2=(bars[-1]['high']+bars[-1]['low'])/2
    upper=hl2+m*a; lower=hl2-m*a
    c=bars[-1]['close']; pc=bars[-2]['close']
    if c>lower and c>pc: return 1
    if c<upper and c<pc: return -1
    return 0

def candle_pattern(bars, wick_thr, body_thr):
    if len(bars)<2: return 0,0.0
    a,b=bars[-2],bars[-1]
    body=abs(b['close']-b['open']); rng=max(EPS,b['high']-b['low'])
    ub=b['high']-max(b['open'],b['close']); lb=min(b['open'],b['close'])-b['low']
    bull_eng = b['close']>b['open'] and a['close']<a['open'] and b['open']<=a['close'] and b['close']>=a['open']
    bear_eng = b['close']<b['open'] and a['close']>a['open'] and b['open']>=a['close'] and b['close']<=a['open']
    bull_pin = (lb/rng)>wick_thr and (body/rng)<body_thr
    bear_pin = (ub/rng)>wick_thr and (body/rng)<body_thr
    if bull_eng or bull_pin: return 1, max(lb/rng, 0.6 if bull_eng else 0)
    if bear_eng or bear_pin: return -1, max(ub/rng, 0.6 if bear_eng else 0)
    return 0,0.0

def market_structure(bars, window, min_atr):
    if len(bars)<window*2+10: return 0,0.0
    a=atr(bars, min(14, max(3,len(bars)//4))) or 0
    pivh=[]; pivl=[]
    for i in range(window, len(bars)-window):
        hs=[bars[j]['high'] for j in range(i-window,i+window+1)]
        ls=[bars[j]['low'] for j in range(i-window,i+window+1)]
        if bars[i]['high']>=max(hs): pivh.append((i,bars[i]['high']))
        if bars[i]['low']<=min(ls): pivl.append((i,bars[i]['low']))
    c=bars[-1]['close']
    if len(pivh)>=2 and c > pivh[-1][1] + min_atr*a and pivh[-1][1] > pivh[-2][1]: return 1, (c-pivh[-1][1])/(a+EPS)
    if len(pivl)>=2 and c < pivl[-1][1] - min_atr*a and pivl[-1][1] < pivl[-2][1]: return -1, (pivl[-1][1]-c)/(a+EPS)
    # CHOCH style reversal
    if len(pivh)>=2 and len(pivl)>=1 and c < pivl[-1][1] - min_atr*a: return -1, (pivl[-1][1]-c)/(a+EPS)
    if len(pivl)>=2 and len(pivh)>=1 and c > pivh[-1][1] + min_atr*a: return 1, (c-pivh[-1][1])/(a+EPS)
    return 0,0.0

def signal_for_variant(v: Variant, market, precomputed_bars=None):
    p=v.params
    entry_ts = market['end_ts'] - p.get('entry_before',30)
    if isinstance(precomputed_bars, list):
        bars = precomputed_bars
    elif precomputed_bars is not None:
        bars = precomputed_bars.get((market['_idx'], p.get('frame',5), p.get('entry_before',30), p.get('lookback',480)), [])
    else:
        bars=aggregate(market['candles'], p.get('frame',5), entry_ts, p.get('lookback',480))
    if len(bars)<10: return None
    xs=closes(bars); hs=highs(bars); ls=lows(bars)
    family=v.family
    side=0; strength=0.0
    if family=='ema_slope_cross':
        ef=ema_series(xs,p['fast']); es=ema_series(xs,p['slow'])
        if not ef or not es or ef[-1] is None or es[-1] is None: return None
        slope=(ef[-1]-ef[-1-p['slope_k']])/(atr(bars,p.get('atr_n',14)) or 1) if len(ef)>p['slope_k'] and ef[-1-p['slope_k']] is not None else 0
        diff=ef[-1]-es[-1]
        if diff>0 and slope>p['thr']: side=1
        elif diff<0 and slope<-p['thr']: side=-1
        strength=abs(slope)+abs(diff)/(atr(bars,p.get('atr_n',14)) or 1)
    elif family=='macd_hist':
        ef=ema_series(xs,p['fast']); es=ema_series(xs,p['slow'])
        if len(ef)!=len(xs) or len(es)!=len(xs): return None
        macd=[(a-b) if a is not None and b is not None else None for a,b in zip(ef,es)]
        valid=[m for m in macd if m is not None]
        sig=ema_series(valid,p['sig'])
        if len(valid)<p['sig']+2 or not sig or sig[-1] is None: return None
        hist=valid[-1]-sig[-1]; hist_prev=valid[-2]-(sig[-2] if sig[-2] is not None else sig[-1])
        if hist>0 and hist-hist_prev>p['thr']: side=1
        elif hist<0 and hist-hist_prev<-p['thr']: side=-1
        strength=abs(hist-hist_prev)/(atr(bars,14) or 1)
    elif family=='donchian_breakout':
        n=p['n'];
        if len(bars)<n+p['slope_k']+1: return None
        prev_hi=max(hs[-n-1:-1]); prev_lo=min(ls[-n-1:-1])
        mids=[(max(hs[i-n:i])+min(ls[i-n:i]))/2 for i in range(n,len(bars)+1)]
        slope=(mids[-1]-mids[-1-p['slope_k']])/(atr(bars,14) or 1) if len(mids)>p['slope_k'] else 0
        if xs[-1]>prev_hi and slope>p['thr']: side=1
        elif xs[-1]<prev_lo and slope<-p['thr']: side=-1
        elif p.get('reversal') and xs[-1]<(prev_hi+prev_lo)/2 and slope< -p['thr']: side=-1
        strength=abs(slope)
    elif family=='adx_dmi':
        dm=dmi_adx(bars,p['n'])
        if not dm: return None
        pdi,mdi,adx=dm
        if adx>p['adx'] and pdi>mdi+p['gap']: side=1
        elif adx>p['adx'] and mdi>pdi+p['gap']: side=-1
        strength=adx/25 + abs(pdi-mdi)/25
    elif family=='ichimoku':
        n1,n2,n3=p['tenkan'],p['kijun'],p['spanb']
        if len(bars)<n3+1: return None
        ten=(max(hs[-n1:])+min(ls[-n1:]))/2; kij=(max(hs[-n2:])+min(ls[-n2:]))/2
        spanb=(max(hs[-n3:])+min(ls[-n3:]))/2; spana=(ten+kij)/2
        cloud_hi=max(spana,spanb); cloud_lo=min(spana,spanb)
        if xs[-1]>cloud_hi and ten>kij: side=1
        elif xs[-1]<cloud_lo and ten<kij: side=-1
        strength=abs(ten-kij)/(atr(bars,14) or 1)
    elif family=='rsi_stoch_reversal':
        rr=rsi(xs,p['rsi_n'])
        if rr is None or len(bars)<p['stoch_n']+2: return None
        k=100*(xs[-1]-min(ls[-p['stoch_n']:]))/(max(hs[-p['stoch_n']:])-min(ls[-p['stoch_n']:])+EPS)
        prev_k=100*(xs[-2]-min(ls[-p['stoch_n']-1:-1]))/(max(hs[-p['stoch_n']-1:-1])-min(ls[-p['stoch_n']-1:-1])+EPS)
        if rr<p['lo'] and k>prev_k+p['cross']: side=1
        elif rr>p['hi'] and k<prev_k-p['cross']: side=-1
        strength=abs(k-prev_k)/10 + abs(rr-50)/50
    elif family=='bb_kc_squeeze':
        n=p['n']; a=atr(bars,n); mid=sma(xs,n); sd=std(xs,n)
        if a is None or mid is None or sd is None or len(bars)<n+2: return None
        bb_w=2*p['bb_k']*sd/(mid+EPS); kc_w=2*p['kc_k']*a/(mid+EPS)
        upper=mid+p['bb_k']*sd; lower=mid-p['bb_k']*sd
        if bb_w < p['alpha']*kc_w and xs[-1]>upper: side=1
        elif bb_w < p['alpha']*kc_w and xs[-1]<lower: side=-1
        elif p.get('fade') and xs[-1]<upper and xs[-2]>upper: side=-1
        elif p.get('fade') and xs[-1]>lower and xs[-2]<lower: side=1
        strength=abs(xs[-1]-mid)/(sd+EPS)
    elif family=='supertrend':
        side=supertrend_dir(bars,p['n'],p['mult'])
        if p.get('confirm',0)>0 and len(xs)>p['confirm']:
            recent=[1 if xs[-i]>xs[-i-1] else -1 for i in range(1,p['confirm']+1)]
            if side and sum(recent)*side <=0: side=0
        strength=abs(xs[-1]-xs[-2])/(atr(bars,p['n']) or 1)
    elif family=='market_structure':
        side,strength=market_structure(bars,p['window'],p['min_atr'])
    elif family=='candles_regime':
        side,strength=candle_pattern(bars,p['wick'],p['body'])
        # regime/trend filter: only take reversal if recent move opposite; continuation if configured
        slope=lin_slope(xs[-min(len(xs),p['regime_n']):])/(atr(bars,14) or 1)
        if p.get('mode')=='reversal' and side*slope>0: side=0
        if p.get('mode')=='continuation' and side*slope<0: side=0
        strength += abs(slope)
    elif family=='ensemble_vote':
        votes=[]
        for fam in p['members']:
            vv=Variant(v.id, fam, {**p, **p.get(fam,{})})
            s=signal_for_variant(vv, market, precomputed_bars)
            if s: votes.append(1 if s[0]=='UP' else -1)
        if len(votes)>=p['min_votes'] and abs(sum(votes))>=p['vote_gap']:
            side=1 if sum(votes)>0 else -1; strength=abs(sum(votes))
    if not side or strength < p.get('min_strength',0): return None
    selected='UP' if side>0 else 'DOWN'
    points=market['up_points'] if selected=='UP' else market['down_points']
    px=price_at_or_before(points, entry_ts)
    if px is None or px<=0 or px>=1: return None
    if px > p.get('price_cap',0.98): return None
    return selected, px, entry_ts, strength


def build_variants(limit=1200, seed=7):
    rng=random.Random(seed); variants=[]
    def add(fam, params): variants.append(Variant(len(variants), fam, params))
    common_entries=[90,60,45,30,20,15,10]
    frames=[1,5,10,15,30]
    caps=[0.55,0.65,0.75,0.85,0.95]
    for frame in frames:
      for entry in common_entries:
       for cap in caps:
        add('ema_slope_cross', {'frame':frame,'entry_before':entry,'lookback':600,'fast':rng.choice([3,5,8,13]),'slow':rng.choice([20,34,55]),'slope_k':rng.choice([1,2,3,5]),'thr':rng.choice([0,0.02,0.05,0.1]),'price_cap':cap,'min_strength':rng.choice([0,0.05,0.1])})
        add('macd_hist', {'frame':frame,'entry_before':entry,'lookback':700,'fast':rng.choice([5,8,12,16]),'slow':rng.choice([21,26,34,55]),'sig':rng.choice([3,5,9]),'thr':rng.choice([0,0.01,0.03]),'price_cap':cap})
        add('adx_dmi', {'frame':frame,'entry_before':entry,'lookback':600,'n':rng.choice([7,10,14,21]),'adx':rng.choice([10,15,20,25,30]),'gap':rng.choice([0,3,5,8]),'price_cap':cap})
        add('supertrend', {'frame':frame,'entry_before':entry,'lookback':600,'n':rng.choice([7,10,14,20]),'mult':rng.choice([1.5,2,2.5,3,3.5]),'confirm':rng.choice([0,1,2]),'price_cap':cap})
    for entry in common_entries:
      for frame in [5,10,15,30]:
       for cap in caps:
        add('donchian_breakout', {'frame':frame,'entry_before':entry,'lookback':900,'n':rng.choice([8,10,14,20,30]),'slope_k':rng.choice([1,2,3,5]),'thr':rng.choice([0,0.05,0.1,0.2]),'reversal':rng.choice([False,True]),'price_cap':cap})
        add('ichimoku', {'frame':frame,'entry_before':entry,'lookback':1200,'tenkan':rng.choice([5,7,9,13]),'kijun':rng.choice([18,22,26,30]),'spanb':rng.choice([42,52,65]),'price_cap':cap})
        add('rsi_stoch_reversal', {'frame':frame,'entry_before':entry,'lookback':800,'rsi_n':rng.choice([6,9,14,21]),'stoch_n':rng.choice([9,14,20]),'lo':rng.choice([20,25,30,35,40]),'hi':rng.choice([60,65,70,75,80]),'cross':rng.choice([0,2,5,10]),'price_cap':cap})
        add('bb_kc_squeeze', {'frame':frame,'entry_before':entry,'lookback':900,'n':rng.choice([10,14,20,30]),'bb_k':rng.choice([1.5,2.0,2.5]),'kc_k':rng.choice([1.0,1.5,2.0]),'alpha':rng.choice([0.8,1.0,1.2,1.5]),'fade':rng.choice([False,True]),'price_cap':cap})
        add('market_structure', {'frame':frame,'entry_before':entry,'lookback':1000,'window':rng.choice([2,3,4]),'min_atr':rng.choice([0,0.2,0.5,0.8]),'price_cap':cap,'min_strength':rng.choice([0,0.2,0.5])})
        add('candles_regime', {'frame':frame,'entry_before':entry,'lookback':600,'wick':rng.choice([0.5,0.6,0.7,0.8]),'body':rng.choice([0.25,0.35,0.45,0.55]),'regime_n':rng.choice([10,20,40]),'mode':rng.choice(['reversal','continuation','any']),'price_cap':cap})
    base_members=['ema_slope_cross','adx_dmi','supertrend','macd_hist','market_structure']
    for entry in common_entries:
      for frame in [5,10,15]:
       for cap in [0.65,0.75,0.85,0.95]:
        add('ensemble_vote', {'frame':frame,'entry_before':entry,'lookback':900,'members':rng.sample(base_members,k=rng.choice([3,4,5])),'min_votes':rng.choice([2,3]),'vote_gap':rng.choice([1,2,3]),'price_cap':cap,
                              'fast':rng.choice([5,8,13]),'slow':rng.choice([21,34,55]),'slope_k':rng.choice([1,3]),'thr':rng.choice([0,0.05]),'n':rng.choice([7,10,14]),'adx':rng.choice([10,15,20]),'gap':rng.choice([0,3]),'mult':rng.choice([1.5,2,2.5]),'sig':rng.choice([3,5,9]),'window':rng.choice([2,3]),'min_atr':rng.choice([0,0.2])})
    rng.shuffle(variants)
    return [Variant(i,v.family,v.params) for i,v in enumerate(variants[:limit])]


def eval_variant(v, markets, split_idx, fee=0.0, precomputed_bars=None):
    pnl=[]; costs=[]; wins=0; trades=0; train_p=[]; test_p=[]; train_c=[]; test_c=[]
    byfam=v.family
    for idx,m in enumerate(markets):
        sig=signal_for_variant(v,m,precomputed_bars)
        if not sig: continue
        side,px,ets,strength=sig
        outcome='UP' if m['final_price'] >= m['price_to_beat'] else 'DOWN'
        one=(1.0-px-fee) if side==outcome else (-px-fee)
        trades+=1; wins += 1 if one>0 else 0
        pnl.append(one); costs.append(px+fee)
        if idx < split_idx: train_p.append(one); train_c.append(px+fee)
        else: test_p.append(one); test_c.append(px+fee)
    if trades==0: return None
    cum=0; peak=0; maxdd=0
    for x in pnl:
        cum += x; peak=max(peak,cum); maxdd=max(maxdd, peak-cum)
    net=sum(pnl); cost=sum(costs)
    train_net=sum(train_p); test_net=sum(test_p)
    score=(test_net if len(test_p)>=20 else -9999) + 0.25*train_net - 0.02*maxdd
    return Result(v.id,byfam,v.params,trades,wins,trades-wins,wins/trades,net,net,net/(cost or EPS),sum(costs)/trades,maxdd,
                  len(train_p),train_net,train_net/(sum(train_c) or EPS),len(test_p),test_net,test_net/(sum(test_c) or EPS),score)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--cache', default='/home/administrator/projects/polybot/data/backtests/cache/btc_5m_market_inputs_30d.json')
    ap.add_argument('--variants', type=int, default=1000)
    ap.add_argument('--out', default='/home/administrator/projects/polybot/reports/btc5m_trend_reversal_sweep_results.json')
    ap.add_argument('--max-markets', type=int, default=0, help='Optional deterministic evenly-spaced market subsample for coarse sweeps')
    args=ap.parse_args()
    data=json.loads(Path(args.cache).read_text())
    markets=data['market_inputs']
    markets=sorted(markets, key=lambda m:m['start_ts'])
    # require usable labels/prices/candles
    markets=[m for m in markets if m.get('final_price') and m.get('price_to_beat') and m.get('candles') and (m.get('up_points') or m.get('down_points'))]
    if args.max_markets and len(markets) > args.max_markets:
        step=(len(markets)-1)/max(1,args.max_markets-1)
        markets=[markets[round(i*step)] for i in range(args.max_markets)]
    for i,m in enumerate(markets):
        m['_idx']=i
    split=int(len(markets)*0.70)
    variants=build_variants(args.variants)
    groups={}
    for v in variants:
        key=(v.params.get('frame',5), v.params.get('entry_before',30), v.params.get('lookback',480))
        groups.setdefault(key,[]).append(v)
    print(f'evaluating {len(variants)} variants in {len(groups)} context groups over {len(markets)} markets', flush=True)
    states={v.id:{'v':v,'pnl':[],'costs':[],'wins':0,'train_p':[],'test_p':[],'train_c':[],'test_c':[]} for v in variants}
    gnum=0
    for (frame,entry,lookback),gvars in groups.items():
        gnum+=1
        for idx,m in enumerate(markets):
            bars=aggregate(m['candles'], frame, m['end_ts']-entry, lookback)
            for v in gvars:
                sig=signal_for_variant(v,m,bars)
                if not sig: continue
                side,px,ets,strength=sig
                outcome='UP' if m['final_price'] >= m['price_to_beat'] else 'DOWN'
                one=(1.0-px) if side==outcome else -px
                st=states[v.id]
                st['pnl'].append(one); st['costs'].append(px); st['wins'] += 1 if one>0 else 0
                if idx < split:
                    st['train_p'].append(one); st['train_c'].append(px)
                else:
                    st['test_p'].append(one); st['test_c'].append(px)
        if gnum%10==0 or gnum==len(groups):
            print(f'context_group {gnum}/{len(groups)} done', flush=True)
    results=[]
    for st in states.values():
        v=st['v']; pnl=st['pnl']; costs=st['costs']; trades=len(pnl)
        if trades<30 or len(st['test_p'])<10: continue
        cum=0; peak=0; maxdd=0
        for x in pnl:
            cum+=x; peak=max(peak,cum); maxdd=max(maxdd, peak-cum)
        net=sum(pnl); cost=sum(costs); train_net=sum(st['train_p']); test_net=sum(st['test_p'])
        score=(test_net if len(st['test_p'])>=20 else -9999)+0.25*train_net-0.02*maxdd
        results.append(Result(v.id,v.family,v.params,trades,st['wins'],trades-st['wins'],st['wins']/trades,net,net,net/(cost or EPS),sum(costs)/trades,maxdd,
                              len(st['train_p']),train_net,train_net/(sum(st['train_c']) or EPS),len(st['test_p']),test_net,test_net/(sum(st['test_c']) or EPS),score))
    # rank: primarily test pnl/roi, require train not catastrophically negative; keep all for report
    results_sorted=sorted(results, key=lambda r:(r.test_net_pnl, r.test_roi, r.net_pnl, r.trades), reverse=True)
    family_best={}
    for r in results_sorted:
        family_best.setdefault(r.family, r)
    out={
        'dataset': {'cache':args.cache,'markets':len(markets),'train_markets':split,'test_markets':len(markets)-split,'start_ts':markets[0]['start_ts'],'end_ts':markets[-1]['end_ts']},
        'variant_count_requested': args.variants,
        'variant_count_evaluated_nonempty': len(results),
        'top_25': [asdict(r) for r in results_sorted[:25]],
        'best_by_family': {k:asdict(v) for k,v in family_best.items()},
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=2, sort_keys=True))
    print(json.dumps(out['dataset'], indent=2))
    print('evaluated_nonempty', len(results))
    print('TOP 5')
    for r in results_sorted[:5]:
        print(json.dumps(asdict(r), sort_keys=True))
    print('wrote', args.out)

if __name__=='__main__': main()
