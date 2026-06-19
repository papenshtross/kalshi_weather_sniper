#!/usr/bin/env python3
"""Pre-open BTC 5m trend-direction sweep.

Research-only / no live side effects.

Goal: determine trend BEFORE each Polymarket BTC 5m market opens, enter instantly at
market availability/open, ignore Polymarket entry price, and score only whether the
market resolves in the predicted direction (final_price >= price_to_beat -> UP).

Runs variants in batches so 1000 variations can be executed as 10x100 without
loading the live stack or touching running strategies.
"""
from __future__ import annotations

import argparse, json, math, random
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

EPS=1e-12

@dataclass(frozen=True)
class Variant:
    id:int
    family:str
    params:dict[str,Any]

@dataclass
class Result:
    variant_id:int
    family:str
    params:dict[str,Any]
    trades:int
    up_calls:int
    down_calls:int
    wins:int
    losses:int
    hit_rate:float
    train_trades:int
    train_hit_rate:float
    test_trades:int
    test_hit_rate:float
    coverage:float
    score:float


def aggregate_preopen(candles:list[dict[str,Any]], start_ts:int, lookback_s:int, frame_s:int)->list[dict[str,float]]:
    lo=start_ts-lookback_s
    rows=[c for c in candles if lo <= int(c['ts']) <= start_ts and float(c.get('close') or 0)>0]
    buckets={}
    for c in rows:
        ts=int(c['ts']); b=(ts//frame_s)*frame_s
        close=float(c['close'])
        d=buckets.setdefault(b, {'ts':b,'open':float(c.get('open') or close),'high':float(c.get('high') or close),'low':float(c.get('low') or close),'close':close,'volume':0.0,'taker_buy_volume':0.0})
        d['high']=max(d['high'], float(c.get('high') or close)); d['low']=min(d['low'], float(c.get('low') or close)); d['close']=close
        d['volume']+=float(c.get('volume') or 0); d['taker_buy_volume']+=float(c.get('taker_buy_volume') or 0)
    return [buckets[k] for k in sorted(buckets)]

def xs(b,k='close'): return [float(bi[k]) for bi in b]
def sma(a,n): return sum(a[-n:])/n if len(a)>=n and n>0 else None
def std(a,n):
    if len(a)<n: return None
    m=sma(a,n); return math.sqrt(sum((x-m)**2 for x in a[-n:])/n)
def ema_series(a,n):
    if len(a)<n or n<=0: return []
    k=2/(n+1); e=sum(a[:n])/n; out=[None]*(n-1)+[e]
    for x in a[n:]: e=x*k+e*(1-k); out.append(e)
    return out
def atr(b,n):
    if len(b)<n+1: return None
    tr=[]
    for i in range(1,len(b)):
        h,l,pc=b[i]['high'],b[i]['low'],b[i-1]['close']; tr.append(max(h-l,abs(h-pc),abs(l-pc)))
    return sum(tr[-n:])/n if len(tr)>=n else None
def lin_slope(v):
    n=len(v)
    if n<2: return 0.0
    mx=(n-1)/2; my=sum(v)/n; den=sum((i-mx)**2 for i in range(n))
    return sum((i-mx)*(v[i]-my) for i in range(n))/(den or EPS)
def rsi(a,n):
    if len(a)<n+1: return None
    g=[]; l=[]
    for i in range(len(a)-n,len(a)):
        d=a[i]-a[i-1]; g.append(max(0,d)); l.append(max(0,-d))
    ag=sum(g)/n; al=sum(l)/n
    return 100.0 if al<EPS else 100-100/(1+ag/al)
def dmi_adx(b,n):
    if len(b)<n+2: return None
    plus=[]; minus=[]; trs=[]
    for i in range(1,len(b)):
        up=b[i]['high']-b[i-1]['high']; dn=b[i-1]['low']-b[i]['low']
        plus.append(up if up>dn and up>0 else 0.0); minus.append(dn if dn>up and dn>0 else 0.0)
        h,l,pc=b[i]['high'],b[i]['low'],b[i-1]['close']; trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    tr=sum(trs[-n:])
    if tr<EPS: return None
    pdi=100*sum(plus[-n:])/tr; mdi=100*sum(minus[-n:])/tr
    dx=[]
    for j in range(max(n,len(trs)-n+1), len(trs)+1):
        trj=sum(trs[j-n:j])
        if trj<EPS: continue
        pd=100*sum(plus[j-n:j])/trj; md=100*sum(minus[j-n:j])/trj
        dx.append(100*abs(pd-md)/(pd+md+EPS))
    return (pdi,mdi,sum(dx)/len(dx)) if dx else None

def candle_pat(b,wick,body,mode):
    if len(b)<2: return 0
    a,c=b[-2],b[-1]; rng=max(EPS,c['high']-c['low']); bd=abs(c['close']-c['open'])
    bull_eng=c['close']>c['open'] and a['close']<a['open'] and c['open']<=a['close'] and c['close']>=a['open']
    bear_eng=c['close']<c['open'] and a['close']>a['open'] and c['open']>=a['close'] and c['close']<=a['open']
    lb=min(c['open'],c['close'])-c['low']; ub=c['high']-max(c['open'],c['close'])
    bull_pin=lb/rng>wick and bd/rng<body; bear_pin=ub/rng>wick and bd/rng<body
    sig=1 if bull_eng or bull_pin else -1 if bear_eng or bear_pin else 0
    if not sig: return 0
    slope=lin_slope(xs(b)[-min(len(b),20):])/(atr(b,min(7,max(2,len(b)//3))) or 1)
    if mode=='continuation' and sig*slope<0: return 0
    if mode=='reversal' and sig*slope>0: return 0
    return sig

def market_structure(b,w,min_atr):
    if len(b)<2*w+5: return 0
    aa=atr(b,min(7,max(2,len(b)//3))) or 0
    ph=[]; pl=[]
    for i in range(w,len(b)-w):
        if b[i]['high']>=max(x['high'] for x in b[i-w:i+w+1]): ph.append((i,b[i]['high']))
        if b[i]['low']<=min(x['low'] for x in b[i-w:i+w+1]): pl.append((i,b[i]['low']))
    c=b[-1]['close']
    if len(ph)>=2 and c>ph[-1][1]+min_atr*aa: return 1
    if len(pl)>=2 and c<pl[-1][1]-min_atr*aa: return -1
    return 0

def signal(v:Variant,bars:list[dict[str,float]])->int:
    p=v.params; fam=v.family
    if len(bars)<4: return 0
    c=xs(bars); h=xs(bars,'high'); l=xs(bars,'low')
    if fam=='raw_return':
        n=p['n'];
        if len(c)<n+1: return 0
        r=c[-1]/c[-1-n]-1
        return 1 if r>p['thr'] else -1 if r<-p['thr'] else 0
    if fam=='ema_slope':
        ef=ema_series(c,p['fast']); es=ema_series(c,p['slow'])
        if not ef or not es or ef[-1] is None or es[-1] is None or len(ef)<=p['k'] or ef[-1-p['k']] is None: return 0
        slope=(ef[-1]-ef[-1-p['k']])/(atr(bars,p.get('atr_n',5)) or 1)
        return 1 if ef[-1]>es[-1] and slope>p['thr'] else -1 if ef[-1]<es[-1] and slope<-p['thr'] else 0
    if fam=='macd':
        ef=ema_series(c,p['fast']); es=ema_series(c,p['slow'])
        mac=[a-b for a,b in zip(ef,es) if a is not None and b is not None]
        if len(mac)<p['sig']+2: return 0
        sg=ema_series(mac,p['sig']);
        if not sg or sg[-1] is None: return 0
        hist=mac[-1]-sg[-1]; prev=mac[-2]-(sg[-2] if sg[-2] is not None else sg[-1])
        return 1 if hist>0 and hist-prev>p['thr'] else -1 if hist<0 and hist-prev<-p['thr'] else 0
    if fam=='donchian':
        n=p['n']
        if len(c)<n+2: return 0
        hi=max(h[-n-1:-1]); lo=min(l[-n-1:-1]); mid=(hi+lo)/2
        if c[-1]>hi: return 1
        if c[-1]<lo: return -1
        if p.get('mid_bias'): return 1 if c[-1]>mid else -1 if c[-1]<mid else 0
        return 0
    if fam=='adx_dmi':
        d=dmi_adx(bars,p['n'])
        if not d: return 0
        pdi,mdi,adx=d
        return 1 if adx>p['adx'] and pdi>mdi+p['gap'] else -1 if adx>p['adx'] and mdi>pdi+p['gap'] else 0
    if fam=='ichimoku':
        n1,n2,n3=p['tenkan'],p['kijun'],p['spanb']
        if len(c)<n3: return 0
        ten=(max(h[-n1:])+min(l[-n1:]))/2; kij=(max(h[-n2:])+min(l[-n2:]))/2; spb=(max(h[-n3:])+min(l[-n3:]))/2; spa=(ten+kij)/2
        return 1 if c[-1]>max(spa,spb) and ten>kij else -1 if c[-1]<min(spa,spb) and ten<kij else 0
    if fam=='rsi_stoch':
        rr=rsi(c,p['rsi_n'])
        if rr is None or len(c)<p['stoch_n']+1: return 0
        k=100*(c[-1]-min(l[-p['stoch_n']:]))/(max(h[-p['stoch_n']:])-min(l[-p['stoch_n']:])+EPS)
        if p['mode']=='momentum': return 1 if rr>p['hi'] and k>p['khi'] else -1 if rr<p['lo'] and k<p['klo'] else 0
        return -1 if rr>p['hi'] and k>p['khi'] else 1 if rr<p['lo'] and k<p['klo'] else 0
    if fam=='bb_break':
        n=p['n']; m=sma(c,n); sd=std(c,n)
        if m is None or sd is None: return 0
        up=m+p['k']*sd; dn=m-p['k']*sd
        if p.get('fade'): return -1 if c[-1]>up else 1 if c[-1]<dn else 0
        return 1 if c[-1]>up else -1 if c[-1]<dn else 0
    if fam=='supertrend':
        a=atr(bars,p['n'])
        if a is None: return 0
        hl2=(bars[-1]['high']+bars[-1]['low'])/2
        return 1 if c[-1]>hl2+p['mult']*a*0.1 else -1 if c[-1]<hl2-p['mult']*a*0.1 else 0
    if fam=='market_structure': return market_structure(bars,p['w'],p['min_atr'])
    if fam=='candles': return candle_pat(bars,p['wick'],p['body'],p['mode'])
    if fam=='ensemble':
        votes=[]
        for f,pp in p['members']:
            s=signal(Variant(v.id,f,{**p,**pp}),bars)
            if s: votes.append(s)
        sm=sum(votes)
        return 1 if len(votes)>=p['min_votes'] and sm>=p['vote_gap'] else -1 if len(votes)>=p['min_votes'] and sm<=-p['vote_gap'] else 0
    return 0

def build_variants(total:int=1000,seed:int=11)->list[Variant]:
    r=random.Random(seed); variants=[]
    def add(f,p): variants.append(Variant(len(variants),f,p))
    frames=[1,5,10,15,30]; looks=[60,90,120,150,180]
    while len(variants)<total:
        frame=r.choice(frames); look=r.choice(looks); base={'frame':frame,'lookback':look}
        fam=r.choice(['raw_return','ema_slope','macd','donchian','adx_dmi','ichimoku','rsi_stoch','bb_break','supertrend','market_structure','candles','ensemble'])
        if fam=='raw_return': add(fam,{**base,'n':r.choice([1,2,3,5,8,13]),'thr':r.choice([0,0.00005,0.0001,0.0002,0.0005])})
        elif fam=='ema_slope': add(fam,{**base,'fast':r.choice([2,3,5,8]),'slow':r.choice([5,8,13,21]),'k':r.choice([1,2,3]),'thr':r.choice([0,0.02,0.05,0.1])})
        elif fam=='macd': add(fam,{**base,'fast':r.choice([2,3,5,8]),'slow':r.choice([8,13,21]),'sig':r.choice([2,3,5]),'thr':r.choice([0,0.01,0.03])})
        elif fam=='donchian': add(fam,{**base,'n':r.choice([3,5,8,10,13]),'mid_bias':r.choice([False,True])})
        elif fam=='adx_dmi': add(fam,{**base,'n':r.choice([3,5,7,10]),'adx':r.choice([5,10,15,20,25]),'gap':r.choice([0,2,5,8,12])})
        elif fam=='ichimoku': add(fam,{**base,'tenkan':r.choice([2,3,5,7]),'kijun':r.choice([5,7,9,13]),'spanb':r.choice([8,10,13,18])})
        elif fam=='rsi_stoch': add(fam,{**base,'rsi_n':r.choice([3,5,7,10]),'stoch_n':r.choice([3,5,8,13]),'lo':r.choice([20,30,40,45]),'hi':r.choice([55,60,70,80]),'klo':r.choice([20,30,40]),'khi':r.choice([60,70,80]),'mode':r.choice(['momentum','reversal'])})
        elif fam=='bb_break': add(fam,{**base,'n':r.choice([5,8,10,13,20]),'k':r.choice([1.0,1.5,2.0,2.5]),'fade':r.choice([False,True])})
        elif fam=='supertrend': add(fam,{**base,'n':r.choice([3,5,7,10]),'mult':r.choice([1,1.5,2,2.5,3])})
        elif fam=='market_structure': add(fam,{**base,'w':r.choice([1,2,3]),'min_atr':r.choice([0,0.2,0.5,0.8])})
        elif fam=='candles': add(fam,{**base,'wick':r.choice([0.5,0.6,0.7,0.8]),'body':r.choice([0.25,0.35,0.45,0.55]),'mode':r.choice(['any','continuation','reversal'])})
        else:
            members=[]
            for _ in range(r.choice([3,4,5])):
                members.append(r.choice([
                    ('raw_return',{'n':r.choice([2,3,5]),'thr':r.choice([0,0.0001])}),
                    ('ema_slope',{'fast':r.choice([2,3,5]),'slow':r.choice([8,13]),'k':r.choice([1,2]),'thr':r.choice([0,0.02])}),
                    ('adx_dmi',{'n':r.choice([3,5,7]),'adx':r.choice([5,10,15]),'gap':r.choice([0,2,5])}),
                    ('bb_break',{'n':r.choice([5,8,10]),'k':r.choice([1.0,1.5,2.0]),'fade':r.choice([False,True])}),
                ]))
            add(fam,{**base,'members':members,'min_votes':r.choice([2,3]),'vote_gap':r.choice([1,2,3])})
    return variants[:total]

def eval_variant(v,markets,bars_cache,split):
    wins=tr=up=dn=tw=tt=vw=vt=0
    for i,m in enumerate(markets):
        b=bars_cache[(i,v.params['frame'],v.params['lookback'])]
        s=signal(v,b)
        if not s: continue
        outcome=1 if float(m['final_price'])>=float(m['price_to_beat']) else -1
        tr+=1; up+=s>0; dn+=s<0; win=(s==outcome); wins+=win
        if i<split: tt+=1; tw+=win
        else: vt+=1; vw+=win
    if tr==0: return None
    hit=wins/tr; th=tw/tt if tt else 0; vh=vw/vt if vt else 0
    # prefer out-of-sample hit rate, require coverage, penalize train/test instability
    cov=tr/len(markets); score=vh + 0.15*hit + 0.05*cov - 0.25*abs(th-vh)
    return Result(v.id,v.family,v.params,tr,up,dn,wins,tr-wins,hit,tt,th,vt,vh,cov,score)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--cache',default='/home/administrator/projects/polybot/data/backtests/cache/btc_5m_market_inputs_30d.json')
    ap.add_argument('--variants',type=int,default=1000)
    ap.add_argument('--batch',type=int,default=0)
    ap.add_argument('--batch-size',type=int,default=100)
    ap.add_argument('--out-dir',default='/home/administrator/projects/polybot/reports/preopen_trend_sweep')
    args=ap.parse_args()
    data=json.loads(Path(args.cache).read_text()); markets=sorted(data['market_inputs'],key=lambda m:m['start_ts'])
    markets=[m for m in markets if m.get('price_to_beat') and m.get('final_price') and m.get('candles')]
    split=int(len(markets)*0.7)
    variants=build_variants(args.variants)
    if args.batch>=0:
        variants=variants[args.batch*args.batch_size:(args.batch+1)*args.batch_size]
    contexts=sorted({(v.params['frame'],v.params['lookback']) for v in variants})
    print(f'dataset markets={len(markets)} train={split} test={len(markets)-split} variants={len(variants)} contexts={len(contexts)} batch={args.batch}', flush=True)
    bars_cache={}
    for i,m in enumerate(markets):
        for frame,look in contexts:
            bars_cache[(i,frame,look)] = aggregate_preopen(m['candles'], int(m['start_ts']), look, frame)
    results=[]
    for v in variants:
        r=eval_variant(v,markets,bars_cache,split)
        if r and r.trades>=50 and r.test_trades>=15: results.append(r)
    results=sorted(results,key=lambda r:(r.score,r.test_hit_rate,r.hit_rate,r.trades),reverse=True)
    out={'dataset':{'cache':args.cache,'markets':len(markets),'train_markets':split,'test_markets':len(markets)-split,'start_ts':markets[0]['start_ts'],'end_ts':markets[-1]['end_ts']},'batch':args.batch,'evaluated_variants':len(variants),'kept_results':len(results),'top_25':[asdict(r) for r in results[:25]]}
    od=Path(args.out_dir); od.mkdir(parents=True,exist_ok=True); path=od/f'batch_{args.batch:02d}.json'
    path.write_text(json.dumps(out,indent=2,sort_keys=True))
    print('TOP5')
    for r in results[:5]: print(json.dumps(asdict(r),sort_keys=True))
    print('wrote',path)

if __name__=='__main__': main()
