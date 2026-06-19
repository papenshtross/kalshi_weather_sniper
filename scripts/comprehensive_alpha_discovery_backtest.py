#!/usr/bin/env python3
from __future__ import annotations

import json, time, math, statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
import requests

UA={'User-Agent':'Mozilla/5.0 (Hermes comprehensive alpha discovery)','Accept':'application/json'}
DATA='https://data-api.polymarket.com'
OUT=Path('reports/alpha_discovery_comprehensive')
CACHE=OUT/'cache'


def jget(path_or_url, params=None, tries=4):
    url=path_or_url if path_or_url.startswith('http') else DATA+path_or_url
    last=None
    for i in range(tries):
        try:
            r=requests.get(url,params=params,headers=UA,timeout=35)
            r.raise_for_status(); return r.json()
        except Exception as e:
            last=e; time.sleep(0.7*(i+1))
    raise RuntimeError(f'GET failed {url} {params}') from last


def cache_json(name, fn):
    CACHE.mkdir(parents=True,exist_ok=True)
    p=CACHE/name
    if p.exists():
        return json.loads(p.read_text())
    data=fn(); p.write_text(json.dumps(data)); return data


def category(t):
    s=' '.join(str(t.get(k) or '').lower() for k in ('title','slug','eventSlug'))
    if any(x in s for x in ['bitcoin','btc','ethereum','eth','solana','sol','xrp','doge','updown','up-or-down','crypto']): return 'crypto'
    if any(x in s for x in ['nba','nfl','nhl','mlb','soccer','fifa','tennis','cricket','ufc','f1','esports','dota','cs2','knicks','cavaliers','lakers','champions']): return 'sports'
    if any(x in s for x in ['trump','election','president','senate','iran','ukraine','russia','israel','ceasefire','peace','china','taiwan','congress','minister','tariff']): return 'politics/geopolitics'
    if any(x in s for x in ['fed','oil','wti','inflation','recession','ipo','stock','rate','gdp','cpi']): return 'macro/finance'
    if any(x in s for x in ['temperature','weather','rain','snow','hurricane','celsius']): return 'weather'
    if any(x in s for x in ['spacex','starship','openai','ai','tesla','apple','google','nvidia']): return 'tech/science'
    if any(x in s for x in ['album','movie','box office','tweet','gta','celebrity']): return 'culture/media'
    return 'other'


def leaderboard(limit=100):
    return cache_json(f'leaderboard_{limit}.json', lambda: jget('/v1/leaderboard', {'limit':limit}))


def wallet_trades(wallet, max_rows=1200):
    def fetch():
        out=[]
        for off in range(0,max_rows,500):
            rows=jget('/trades', {'user':wallet,'limit':500,'offset':off})
            if not rows: break
            out.extend(rows)
            if len(rows)<500: break
            time.sleep(0.05)
        seen=set(); clean=[]
        for r in out:
            key=(r.get('transactionHash'),r.get('asset'),r.get('side'),r.get('timestamp'),round(float(r.get('price') or 0),8),round(float(r.get('size') or 0),5))
            if key in seen: continue
            seen.add(key); clean.append(r)
        return sorted(clean,key=lambda x:int(x.get('timestamp') or 0))
    return cache_json(f'wallet_{wallet.lower()}_{max_rows}.json', fetch)


def market_trades(condition, max_rows=1000):
    def fetch():
        out=[]
        for off in range(0,max_rows,500):
            rows=jget('/trades', {'market':condition,'limit':500,'offset':off})
            if not rows: break
            out.extend(rows)
            if len(rows)<500: break
            time.sleep(0.05)
        return sorted(out,key=lambda x:int(x.get('timestamp') or 0))
    return cache_json(f'market_{condition}_{max_rows}.json', fetch)


def opposite_asset_hint(rows, asset):
    # choose most common other asset in same condition as opposite
    c=Counter(str(r.get('asset')) for r in rows if str(r.get('asset'))!=str(asset))
    return c.most_common(1)[0][0] if c else None


def reconstruct_realized(trades):
    inv=defaultdict(float); avg=defaultdict(float); pnl=0; cost=0; closed=0; wins=0
    for r in trades:
        a=str(r.get('asset')); side=r.get('side'); q=float(r.get('size') or 0); p=float(r.get('price') or 0)
        if q<=0 or not 0<p<1: continue
        if side=='BUY':
            old=inv[a]; new=old+q; avg[a]=(avg[a]*old+p*q)/new if new else 0; inv[a]=new; cost+=p*q
        elif side=='SELL' and inv[a]>0:
            q2=min(q,inv[a]); x=(p-avg[a])*q2; pnl+=x; closed+=1; wins += x>0; inv[a]-=q2
    return {'realized_pnl':pnl,'cost':cost,'roi':pnl/cost if cost else 0,'closed_trades':closed,'win_rate':wins/closed if closed else 0}


def wallet_profile(lb, trades, split_ts):
    train=[t for t in trades if int(t.get('timestamp') or 0)<split_ts]
    test=[t for t in trades if int(t.get('timestamp') or 0)>=split_ts]
    cats=Counter(category(t) for t in train)
    realized=reconstruct_realized(train)
    buys=[t for t in train if t.get('side')=='BUY']
    prices=[float(t.get('price') or 0) for t in buys if 0<float(t.get('price') or 0)<1]
    sizes=[float(t.get('size') or 0) for t in buys if float(t.get('size') or 0)>0]
    topcat=cats.most_common(1)[0][0] if cats else 'other'
    score=0
    score += math.log10(max(1,float(lb.get('pnl') or 0)))
    score += max(-2,min(2,realized['roi']*5))
    score += math.log10(max(1,len(train)))
    if cats and cats[topcat]/sum(cats.values())>0.65: score += 1.0
    return {'wallet':(lb.get('proxyWallet') or '').lower(),'name':lb.get('userName') or '', 'leaderboard_rank':lb.get('rank'), 'leaderboard_pnl':float(lb.get('pnl') or 0), 'leaderboard_volume':float(lb.get('vol') or 0), 'train_trades':len(train),'test_trades':len(test),'top_category':topcat,'category_mix':dict(cats.most_common(6)), 'train_realized':realized, 'median_train_price':statistics.median(prices) if prices else 0, 'median_train_size':statistics.median(sizes) if sizes else 0, 'score':score}

@dataclass
class Strategy:
    name:str; mode:str; category_filter:str|None; delay_s:int; max_chase:float; slippage:float; price_min:float=0.0; price_max:float=1.0; min_size_quantile:float=0.0; accumulation_n:int=1; take_profit_mult:float=0.0; stop_frac:float=0.0; max_hold_s:int=0


def strategies(topcat):
    return [
        Strategy('copy_specialist_30s','copy',topcat,30,0.020,0.010),
        Strategy('copy_specialist_2m','copy',topcat,120,0.025,0.015),
        Strategy('copy_specialist_10m','copy',topcat,600,0.035,0.025),
        Strategy('copy_all_2m','copy',None,120,0.025,0.015),
        Strategy('copy_large_specialist','copy',topcat,120,0.025,0.015,min_size_quantile=0.75),
        Strategy('copy_midprob_specialist','copy',topcat,120,0.020,0.015,price_min=0.25,price_max=0.75),
        Strategy('copy_cheap_pump_tp3x','copy',topcat,120,0.015,0.012,price_max=0.20,take_profit_mult=3.0,stop_frac=0.5,max_hold_s=48*3600),
        Strategy('copy_highconviction','copy',topcat,120,0.015,0.012,price_min=0.75),
        Strategy('copy_accumulation_2nd_buy','copy',topcat,120,0.020,0.015,accumulation_n=2),
        Strategy('fade_overconfident_buy','fade_buy',topcat,120,0.025,0.018,price_min=0.80,price_max=0.99,max_hold_s=24*3600),
        Strategy('fade_large_buy','fade_buy',topcat,120,0.025,0.018,min_size_quantile=0.80,max_hold_s=24*3600),
        Strategy('exit_follow_opposite','fade_sell',topcat,120,0.025,0.018,max_hold_s=24*3600),
    ]


def quantile(xs,q):
    if not xs: return 0
    ys=sorted(xs); idx=min(len(ys)-1,max(0,int((len(ys)-1)*q))); return ys[idx]


def next_fill(mrows, asset, side, after_ts, limit_px, qty, participation=0.10, horizon=6*3600):
    # BUY: take observed sell/buy prints at <= limit as proxy available. SELL: prints at >= limit.
    filled=0; cost=0; last_ts=None
    for r in mrows:
        ts=int(r.get('timestamp') or 0)
        if ts<after_ts: continue
        if ts>after_ts+horizon: break
        if str(r.get('asset'))!=str(asset): continue
        p=float(r.get('price') or 0); q=float(r.get('size') or 0)*participation
        if q<=0 or not 0<p<1: continue
        ok=(side=='BUY' and p<=limit_px) or (side=='SELL' and p>=limit_px)
        if not ok: continue
        take=min(q,qty-filled); filled+=take; cost+=take*p; last_ts=ts
        if filled>=qty*0.999: break
    return {'qty':filled,'avg':cost/filled if filled else 0,'ts':last_ts}


def simulate(wallet, trades, profile, strat:Strategy, market_cache_limit=250):
    all_ts=sorted(int(t.get('timestamp') or 0) for t in trades)
    split_ts=all_ts[int(len(all_ts)*0.70)]
    train=[t for t in trades if int(t.get('timestamp') or 0)<split_ts]
    test=[t for t in trades if int(t.get('timestamp') or 0)>=split_ts]
    train_sizes=[float(t.get('size') or 0) for t in train if t.get('side')=='BUY' and float(t.get('size') or 0)>0]
    min_size=quantile(train_sizes,strat.min_size_quantile) if strat.min_size_quantile else 0
    prior_buys=Counter(str(t.get('asset')) for t in train if t.get('side')=='BUY')
    inv=defaultdict(float); avg=defaultdict(float); entry_ts={}; pnl=0; cost=0; fills=0; exit_count=0; missed=0; signals=0; examples=[]; conditions=set()
    # limit number of market fetches: focus on top notional signals
    sorted_test=sorted(test,key=lambda r: float(r.get('size') or 0)*float(r.get('price') or 0), reverse=True)
    allowed_conditions={str(r.get('conditionId')) for r in sorted_test[:market_cache_limit] if r.get('conditionId')}
    test=sorted(test,key=lambda r:int(r.get('timestamp') or 0))
    market_cache={}
    def mrows(cond):
        if cond not in market_cache:
            market_cache[cond]=market_trades(cond,1000)
        return market_cache[cond]
    def enter(asset, qty, px, ts, meta):
        nonlocal fills,cost
        if qty<=0: return
        old=inv[asset]; new=old+qty; avg[asset]=(avg[asset]*old+px*qty)/new if new else 0; inv[asset]=new; entry_ts.setdefault(asset,ts); fills+=1; cost+=px*qty
        if len(examples)<6: examples.append({'action':'ENTER','asset':asset[:10],'qty':round(qty,2),'px':round(px,4),**meta})
    def exit_pos(asset, qty, px, ts, reason, meta):
        nonlocal pnl,exit_count
        if inv[asset]<=0 or qty<=0: return
        q=min(inv[asset],qty); x=(px-avg[asset])*q; pnl+=x; inv[asset]-=q; exit_count+=1
        if len(examples)<6: examples.append({'action':'EXIT','asset':asset[:10],'qty':round(q,2),'px':round(px,4),'pnl':round(x,2),'reason':reason,**meta})
    for r in test:
        cond=str(r.get('conditionId') or '')
        if cond not in allowed_conditions: continue
        asset=str(r.get('asset')); side=str(r.get('side')); ts=int(r.get('timestamp') or 0); p=float(r.get('price') or 0); q=float(r.get('size') or 0)
        if q<=0 or not 0<p<1: continue
        c=category(r)
        # time/TP/stop exits on every market print for held same asset
        if inv[asset]>0:
            if strat.take_profit_mult and p >= avg[asset]*strat.take_profit_mult:
                fill=next_fill(mrows(cond),asset,'SELL',ts,max(0.001,p-strat.slippage),inv[asset],0.10,3600)
                if fill['qty']: exit_pos(asset,fill['qty'],fill['avg'],fill['ts'] or ts,'tp',{'title':str(r.get('title',''))[:60]})
            elif strat.stop_frac and p <= avg[asset]*strat.stop_frac:
                fill=next_fill(mrows(cond),asset,'SELL',ts,max(0.001,p-strat.slippage),inv[asset],0.10,3600)
                if fill['qty']: exit_pos(asset,fill['qty'],fill['avg'],fill['ts'] or ts,'stop',{'title':str(r.get('title',''))[:60]})
            elif strat.max_hold_s and ts-entry_ts.get(asset,ts)>=strat.max_hold_s:
                fill=next_fill(mrows(cond),asset,'SELL',ts,max(0.001,p-strat.slippage),inv[asset],0.10,3600)
                if fill['qty']: exit_pos(asset,fill['qty'],fill['avg'],fill['ts'] or ts,'time',{'title':str(r.get('title',''))[:60]})
        if strat.mode=='copy' and side=='SELL' and inv[asset]>0:
            fill=next_fill(mrows(cond),asset,'SELL',ts+strat.delay_s,max(0.001,p-strat.slippage),min(q,inv[asset]),0.10,6*3600)
            if fill['qty']: exit_pos(asset,fill['qty'],fill['avg'],fill['ts'] or ts,'source_sell',{'title':str(r.get('title',''))[:60]})
            continue
        # entries from source signals
        if strat.mode in ('copy','fade_buy') and side!='BUY': continue
        if strat.mode=='fade_sell' and side!='SELL': continue
        if strat.category_filter and c!=strat.category_filter: continue
        if q<min_size: continue
        if not (strat.price_min<=p<=strat.price_max): continue
        if strat.mode=='copy':
            prior_buys[asset]+=1
            if prior_buys[asset] < strat.accumulation_n: continue
            signals+=1; conditions.add(cond)
            limit=min(0.999,p+strat.max_chase+strat.slippage)
            fill=next_fill(mrows(cond),asset,'BUY',ts+strat.delay_s,limit,q,0.10,6*3600)
            if fill['qty']: enter(asset,fill['qty'],fill['avg']+strat.slippage,fill['ts'] or ts,{'signal':'copy','src_px':round(p,4),'title':str(r.get('title',''))[:60]})
            else: missed+=1
        else:
            signals+=1; rows=mrows(cond); opp=opposite_asset_hint(rows,asset)
            if not opp: missed+=1; continue
            # buy opposite side; approximate fair opposite price = 1-source price, allow chase
            limit=min(0.999,1-p+strat.max_chase+strat.slippage)
            fill=next_fill(rows,opp,'BUY',ts+strat.delay_s,limit,q,0.10,6*3600)
            if fill['qty']: enter(opp,fill['qty'],fill['avg']+strat.slippage,fill['ts'] or ts,{'signal':strat.mode,'src_px':round(p,4),'title':str(r.get('title',''))[:60]})
            else: missed+=1
    # mark open by latest market print less liquidation haircut
    open_value=0; open_cost=0; open_n=0
    for a,q in inv.items():
        if q<=1e-9: continue
        open_n+=1; open_cost+=avg[a]*q
        latest=None
        # search cached markets for latest asset print
        for rows in market_cache.values():
            for rr in reversed(rows):
                if str(rr.get('asset'))==a:
                    latest=float(rr.get('price') or 0); break
            if latest is not None: break
        lp=latest if latest is not None else avg[a]
        open_value += max(0.001, lp - strat.slippage - 0.04)*q
    mtm=pnl+open_value-open_cost
    fill_rate=fills/signals if signals else 0
    return {**asdict(strat),'wallet':wallet,'signals':signals,'fills':fills,'fill_rate':fill_rate,'missed':missed,'exits':exit_count,'conditions':len(conditions),'cost':cost,'realized_pnl':pnl,'open_positions':open_n,'mtm_pnl':mtm,'mtm_roi':mtm/cost if cost else 0,'realized_roi':pnl/cost if cost else 0,'examples':examples}


def score_result(r):
    if r['fills']<5 or r['cost']<=0: return -999
    score=0
    score += max(-30,min(40,r['mtm_roi']*100))
    score += max(-10,min(20,r['realized_roi']*100))*0.7
    score += min(15,math.log10(max(1,r['fills']))*8)
    score += min(10,math.log10(max(1,r['conditions']))*6)
    score += min(10,r['fill_rate']*10)
    if r['open_positions']>r['exits']*2+10: score -= 10
    return score


def main():
    OUT.mkdir(parents=True,exist_ok=True); CACHE.mkdir(parents=True,exist_ok=True)
    lbs=leaderboard(100)
    universe=[]
    for lb in lbs[:60]:
        w=(lb.get('proxyWallet') or '').lower()
        if not w.startswith('0x'): continue
        try: tr=wallet_trades(w,1200)
        except Exception as e:
            print('wallet fetch failed',w,e); continue
        if len(tr)<80: continue
        ts=sorted(int(t.get('timestamp') or 0) for t in tr)
        split=ts[int(len(ts)*0.70)]
        prof=wallet_profile(lb,tr,split)
        universe.append((prof,tr))
        print('profile',prof['leaderboard_rank'],prof['name'],prof['top_category'],len(tr),round(prof['score'],2))
    universe.sort(key=lambda x:x[0]['score'], reverse=True)
    selected=universe[:10]
    results=[]
    for prof,tr in selected:
        for s in strategies(prof['top_category']):
            try:
                r=simulate(prof['wallet'],tr,prof,s)
                r['wallet_name']=prof['name']; r['leaderboard_rank']=prof['leaderboard_rank']; r['top_category']=prof['top_category']; r['score']=score_result(r)
                results.append(r)
                print('bt',prof['leaderboard_rank'],s.name,'fills',r['fills'],'roi',round(r['mtm_roi'],3),'score',round(r['score'],1))
            except Exception as e:
                print('bt failed',prof['wallet'],s.name,e)
    results.sort(key=lambda r:r['score'], reverse=True)
    payload={'generated_at':int(time.time()),'method':'Comprehensive wallet discovery: current leaderboard used only as broad candidate universe; each wallet profile/strategy inferred on first 70% of its fetched trades and tested on latest 30%. Follower fills use market-wide /trades?market=conditionId after signal delay with 10% participation cap, max chase, slippage, and latest-print liquidation haircut for open positions. This is Tier-B trade-print simulation, not full L2 order-book replay.', 'universe_wallets':[p for p,_ in universe], 'selected_wallets':[p for p,_ in selected], 'results':results}
    (OUT/'comprehensive_wallet_backtest.json').write_text(json.dumps(payload,indent=2))
    # report
    md=[]; md.append('# Comprehensive alpha discovery / wallet strategy backtest')
    md.append('')
    md.append(payload['method'])
    md.append('')
    md.append(f"Universe: {len(universe)} leaderboard wallets with >=80 trades. Selected top 10 by train-period score. Strategies tested: {len(results)} ({len(selected)} wallets × 12 variants).")
    md.append('')
    md.append('## Top strategy variants by robustness score')
    md.append('| # | score | wallet | rank | style | strategy | fills/signals | fill | conds | MTM ROI | Realized ROI | MTM PnL | cost | open |')
    md.append('|---:|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|')
    for i,r in enumerate(results[:25],1):
        md.append(f"| {i} | {r['score']:.1f} | `{r['wallet'][:8]}…` {r['wallet_name']} | {r['leaderboard_rank']} | {r['top_category']} | {r['name']} | {r['fills']}/{r['signals']} | {r['fill_rate']*100:.0f}% | {r['conditions']} | {r['mtm_roi']*100:.1f}% | {r['realized_roi']*100:.1f}% | ${r['mtm_pnl']:,.0f} | ${r['cost']:,.0f} | {r['open_positions']} |")
    md.append('')
    md.append('## Selected wallet profiles')
    for p,_ in selected:
        md.append(f"### Rank {p['leaderboard_rank']} — {p['name']} `{p['wallet']}`")
        md.append(f"- Train/test trades: {p['train_trades']}/{p['test_trades']}; train style: **{p['top_category']}**; category mix: {p['category_mix']}.")
        md.append(f"- Train reconstructed realized ROI: {p['train_realized']['roi']*100:.1f}% on ${p['train_realized']['cost']:,.0f}; closed trades {p['train_realized']['closed_trades']}; median train buy px/size {p['median_train_price']:.3f}/{p['median_train_size']:.1f}.")
        wr=[r for r in results if r['wallet']==p['wallet']]
        wr.sort(key=lambda r:r['score'], reverse=True)
        for r in wr[:3]:
            md.append(f"  - Best: `{r['name']}` score {r['score']:.1f}, fills {r['fills']}/{r['signals']}, MTM ROI {r['mtm_roi']*100:.1f}%, realized ROI {r['realized_roi']*100:.1f}%, cost ${r['cost']:,.0f}.")
        md.append('')
    md.append('## Usability / next actions')
    md.append('- Results with high MTM but many open positions are **not** immediately tradable; promote only if realized ROI and market-wide fill-rate are acceptable.')
    md.append('- The best candidates must now be replayed against CLOB `prices-history`/recorded books and external market context. This run intentionally rejects source-fill fantasy by requiring later market-wide prints.')
    md.append('- No live trading or services were touched.')
    (OUT/'comprehensive_wallet_backtest.md').write_text('\n'.join(md))
    print('WROTE',OUT/'comprehensive_wallet_backtest.md')
    print(json.dumps({'report':str(OUT/'comprehensive_wallet_backtest.md'),'json':str(OUT/'comprehensive_wallet_backtest.json'),'top':results[:5]},indent=2)[:5000])

if __name__=='__main__': main()
