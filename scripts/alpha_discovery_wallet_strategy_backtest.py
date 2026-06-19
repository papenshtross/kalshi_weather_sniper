#!/usr/bin/env python3
from __future__ import annotations

import json, math, statistics, time, re
from collections import defaultdict, Counter
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
import requests

UA={'User-Agent':'Mozilla/5.0 (Hermes alpha discovery wallet backtest)','Accept':'application/json'}
DATA='https://data-api.polymarket.com'
OUT=Path('reports/alpha_discovery_wallets')

def jget(url, params=None, tries=3):
    last=None
    for i in range(tries):
        try:
            r=requests.get(url,params=params,headers=UA,timeout=30)
            r.raise_for_status(); return r.json()
        except Exception as e:
            last=e; time.sleep(0.5*(i+1))
    raise RuntimeError(f"request failed: {url}") from last

def cat_of(t):
    s=' '.join(str(t.get(k) or '').lower() for k in ('title','slug','eventSlug'))
    if any(x in s for x in ['bitcoin','btc','ethereum','eth','solana','sol','xrp','doge','up or down','updown','crypto']): return 'crypto'
    if any(x in s for x in ['nba','nfl','nhl','mlb','soccer','fifa','tennis','cricket','champions league','ufc','f1','esports','lol','dota','cs2']): return 'sports'
    if any(x in s for x in ['trump','election','president','senate','iran','ukraine','russia','israel','ceasefire','peace','china','taiwan','congress','politic']): return 'politics/geopolitics'
    if any(x in s for x in ['fed','oil','wti','inflation','recession','market cap','ipo','stock','rate cut']): return 'macro/finance'
    if any(x in s for x in ['temperature','weather','rain','snow','hurricane','degree','celsius']): return 'weather'
    if any(x in s for x in ['spacex','starship','ai','openai','tesla','apple','google']): return 'tech/science'
    if any(x in s for x in ['album','movie','box office','elon musk','tweet','gta','celebrity']): return 'culture/media'
    return 'other'

def fetch_leaderboard(limit=25):
    rows=jget(DATA+'/v1/leaderboard', {'limit':limit})
    return rows

def fetch_trades(wallet, max_rows=1500):
    out=[]
    for off in range(0,max_rows,500):
        rows=jget(DATA+'/trades', {'user':wallet,'limit':500,'offset':off})
        if not rows: break
        out.extend(rows)
        if len(rows)<500: break
        time.sleep(0.08)
    # dedupe tx+asset+side+ts+price+size
    seen=set(); clean=[]
    for r in out:
        key=(r.get('transactionHash'),r.get('asset'),r.get('side'),r.get('timestamp'),round(float(r.get('price') or 0),8),round(float(r.get('size') or 0),6))
        if key in seen: continue
        seen.add(key); clean.append(r)
    return sorted(clean, key=lambda x:int(x.get('timestamp') or 0))

def curve_from_source(trades):
    inv=defaultdict(float); avg=defaultdict(float); pnl=0; cost=0; wins=losses=0
    for r in trades:
        asset=str(r.get('asset')); side=str(r.get('side')); q=float(r.get('size') or 0); p=float(r.get('price') or 0)
        if q<=0 or not (0<p<1): continue
        if side=='BUY':
            old=inv[asset]; new=old+q; avg[asset]=((avg[asset]*old)+(p*q))/new if new else 0; inv[asset]=new; cost+=p*q
        elif side=='SELL' and inv[asset]>0:
            q2=min(q,inv[asset]); tradep=(p-avg[asset])*q2; pnl+=tradep; wins+= tradep>0; losses+=tradep<0; inv[asset]-=q2
    return {'realized_pnl':pnl,'cost':cost,'roi':pnl/cost if cost else 0,'closed_wins':wins,'closed_losses':losses}

def summarize_wallet(lb, trades):
    cats=Counter(cat_of(t) for t in trades)
    buy=[t for t in trades if t.get('side')=='BUY']
    sell=[t for t in trades if t.get('side')=='SELL']
    prices=[float(t.get('price') or 0) for t in buy if 0<float(t.get('price') or 0)<1]
    sizes=[float(t.get('size') or 0) for t in buy if float(t.get('size') or 0)>0]
    source=curve_from_source(trades)
    topcat=cats.most_common(1)[0][0] if cats else 'unknown'
    return {
        'wallet': (lb.get('proxyWallet') or '').lower(), 'name': lb.get('userName') or lb.get('name') or '',
        'leaderboard_rank': lb.get('rank'), 'leaderboard_pnl': float(lb.get('pnl') or 0), 'leaderboard_volume': float(lb.get('vol') or 0),
        'trades_loaded': len(trades), 'buy_count': len(buy), 'sell_count': len(sell), 'top_category': topcat,
        'category_mix': dict(cats.most_common(6)), 'median_buy_price': statistics.median(prices) if prices else 0,
        'median_buy_size': statistics.median(sizes) if sizes else 0, 'source_reconstructed': source,
    }

@dataclass
class Variant:
    name: str; category: str|None=None; delay_s:int=120; slippage:float=0.015; min_size_mult:float=0; price_min:float=0.0; price_max:float=1.0; accumulation:bool=False; exit_mode:str='mirror'; hold_s:int=0; stop_cents:float=0.0

def wallet_variants(topcat):
    return [
        Variant('specialist_fast_mirror',topcat,30,0.010),
        Variant('specialist_2m_mirror',topcat,120,0.015),
        Variant('specialist_10m_mirror',topcat,600,0.025),
        Variant('all_markets_fast_mirror',None,30,0.012),
        Variant('large_trades_specialist',topcat,120,0.015,min_size_mult=1.0),
        Variant('cheap_convex_specialist',topcat,120,0.020,price_max=0.25),
        Variant('mid_prob_specialist',topcat,120,0.015,price_min=0.25,price_max=0.75),
        Variant('high_conf_specialist',topcat,120,0.010,price_min=0.75),
        Variant('accumulation_confirmed',topcat,120,0.015,accumulation=True),
        Variant('specialist_24h_time_exit',topcat,120,0.018,exit_mode='time',hold_s=24*3600),
    ]

def backtest_variant(trades, v:Variant, median_size:float, split_q=0.70):
    if len(trades)<20: return None
    ts=[int(t.get('timestamp') or 0) for t in trades]
    split=sorted(ts)[int(len(ts)*split_q)]
    train=[t for t in trades if int(t.get('timestamp') or 0)<split]
    test=[t for t in trades if int(t.get('timestamp') or 0)>=split]
    # historical same-asset buy count available before each trade, for accumulation variants
    buy_seen=Counter(str(t.get('asset')) for t in train if t.get('side')=='BUY')
    inv=defaultdict(float); avg=defaultdict(float); entry_ts={}; pnl=0; cost=0; sells=0; buys=0; wins=0; losses=0; skipped=0
    by_asset_latest={}
    examples=[]
    min_size=median_size*v.min_size_mult if v.min_size_mult else 0
    def maybe_sell(asset, q, px, reason, meta):
        nonlocal pnl,sells,wins,losses
        if inv[asset]<=0: return
        q2=min(q,inv[asset]); px=max(0.001,min(0.999,px-v.slippage))
        tradep=(px-avg[asset])*q2; pnl+=tradep; sells+=1; wins+=tradep>0; losses+=tradep<0; inv[asset]-=q2
        if len(examples)<5 and abs(tradep)>0:
            examples.append({'asset':asset,'reason':reason,'pnl':round(tradep,2),'exit_px':round(px,4), **meta})
    for r in test:
        asset=str(r.get('asset')); side=str(r.get('side')); p=float(r.get('price') or 0); q=float(r.get('size') or 0); curts=int(r.get('timestamp') or 0)
        if not (q>0 and 0<p<1): continue
        by_asset_latest[asset]=(p,curts,r)
        # time exits at any later print for held assets
        if v.exit_mode=='time':
            for a in list(inv.keys()):
                if inv[a]>0 and curts-entry_ts.get(a,curts)>=v.hold_s and a==asset:
                    maybe_sell(a, inv[a], p, 'time_exit', {'title':r.get('title','')[:80]})
        if side=='SELL':
            if v.exit_mode=='mirror': maybe_sell(asset,q,p,'mirror_source_sell',{'title':r.get('title','')[:80]})
            continue
        if side!='BUY': continue
        buy_seen[asset]+=1
        c=cat_of(r)
        if v.category and c!=v.category: skipped+=1; continue
        if q<min_size: skipped+=1; continue
        if not (v.price_min<=p<=v.price_max): skipped+=1; continue
        if v.accumulation and buy_seen[asset]<2: skipped+=1; continue
        # follower cannot get source fill; pay slippage plus tiny delay decay
        entry=min(0.999,p+v.slippage+min(0.01, v.delay_s/3600*0.005))
        old=inv[asset]; new=old+q; avg[asset]=((avg[asset]*old)+(entry*q))/new if new else 0; inv[asset]=new; entry_ts.setdefault(asset,curts)
        cost += entry*q; buys+=1
    # mark residual at last seen print less haircut; only for assets with a later/latest test print
    open_value=0; open_cost=0; open_n=0
    for a,q in inv.items():
        if q<=0: continue
        open_n+=1; open_cost += avg[a]*q
        lp=by_asset_latest.get(a,(avg[a],0,None))[0]
        open_value += max(0.001, lp - v.slippage - 0.03)*q
    mtm_pnl=pnl + open_value - open_cost
    roi=mtm_pnl/cost if cost else 0
    closed_roi=pnl/cost if cost else 0
    return {**asdict(v),'test_trades':len(test),'buys':buys,'sells':sells,'skipped':skipped,'cost':cost,'realized_pnl':pnl,'mtm_pnl':mtm_pnl,'roi':roi,'closed_roi':closed_roi,'wins':wins,'losses':losses,'open_positions':open_n,'examples':examples}

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    lb=fetch_leaderboard(25)
    wallets=[]; all_results=[]
    for row in lb[:18]:
        w=(row.get('proxyWallet') or '').lower()
        if not w.startswith('0x'): continue
        try: trades=fetch_trades(w,1500)
        except Exception as e:
            print('fetch failed',w,e); continue
        if len(trades)<25: continue
        summ=summarize_wallet(row,trades)
        wallets.append(summ)
        vars=wallet_variants(summ['top_category'])
        res=[]
        for v in vars:
            bt=backtest_variant(trades,v,summ['median_buy_size'])
            if bt: res.append(bt)
        res.sort(key=lambda x:(x['roi'],x['mtm_pnl']), reverse=True)
        all_results.append({'wallet':summ,'variants':res})
        print('done',summ['leaderboard_rank'],summ['name'],summ['top_category'],len(res))
        if len(all_results)>=10: break
    # global rank: require >=3 buys and positive cost
    flat=[]
    for item in all_results:
        for r in item['variants']:
            if r['buys']>=3 and r['cost']>0:
                flat.append({'wallet':item['wallet']['wallet'],'wallet_name':item['wallet']['name'],'rank':item['wallet']['leaderboard_rank'],'top_category':item['wallet']['top_category'],**{k:v for k,v in r.items() if k!='examples'},'examples':r['examples']})
    flat.sort(key=lambda x:(x['roi'],x['mtm_pnl']), reverse=True)
    payload={'generated_at':int(time.time()),'method':'Data API leaderboard + trades. Strategy inferred on first 70% chronological trades; variants tested on latest 30%. Follower execution pays conservative slippage/delay haircut; price-history/L2 books not used, so results are alpha triage not deploy-grade.', 'wallets':wallets,'wallet_results':all_results,'global_top_variants':flat[:25]}
    (OUT/'wallet_strategy_backtest.json').write_text(json.dumps(payload,indent=2))
    # Markdown summary
    md=[]
    md.append('# Alpha discovery: top Polymarket wallets strategy reconstruction/backtest')
    md.append('')
    md.append('**Provenance:** Data API `/v1/leaderboard` and `/trades?user=`. Top wallets by leaderboard PnL, then first 10 with enough public trade rows. Backtest is 70/30 chronological: infer wallet style from older trades, test 10 copy variants on latest trades. Execution is pessimistic source-fill copy proxy (slippage/delay haircut), not historical L2 replay.')
    md.append('')
    md.append('## Best variant ranking')
    md.append('| # | wallet | rank | style | variant | MTM ROI | closed ROI | MTM PnL | cost | buys/sells | open |')
    md.append('|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|')
    for i,r in enumerate(flat[:15],1):
        md.append(f"| {i} | `{r['wallet'][:8]}…` {r['wallet_name']} | {r['rank']} | {r['top_category']} | {r['name']} | {r['roi']*100:.1f}% | {r['closed_roi']*100:.1f}% | ${r['mtm_pnl']:,.0f} | ${r['cost']:,.0f} | {r['buys']}/{r['sells']} | {r['open_positions']} |")
    md.append('')
    md.append('## Wallet reconstructions + best of 10 variants')
    for item in all_results:
        w=item['wallet']; best=item['variants'][0] if item['variants'] else None
        md.append(f"### Rank {w['leaderboard_rank']} — {w['name']} `{w['wallet']}`")
        md.append(f"- Leaderboard PnL/vol: ${w['leaderboard_pnl']:,.0f} / ${w['leaderboard_volume']:,.0f}; trades loaded: {w['trades_loaded']} ({w['buy_count']} buys / {w['sell_count']} sells).")
        md.append(f"- Inferred style: **{w['top_category']}** specialist; category mix: {w['category_mix']}; median buy px/size: {w['median_buy_price']:.3f} / {w['median_buy_size']:.1f} shares.")
        if best:
            md.append(f"- Best tested variant: **{best['name']}** — ROI {best['roi']*100:.1f}%, MTM PnL ${best['mtm_pnl']:,.0f}, cost ${best['cost']:,.0f}, buys/sells {best['buys']}/{best['sells']}, open {best['open_positions']}.")
            md.append('- All 10 variant ROI: '+', '.join([f"{v['name']} {v['roi']*100:.1f}%" for v in item['variants']]))
        md.append('')
    md.append('## Selection')
    md.append('Prioritize variants with positive latest-period ROI, multiple copied buys, non-trivial cost base, and category concentration that can be externally modeled. Do **not** deploy any from this alone; next step is Tier-B/Tier-C replay with observed trade-print volume or recorded books and fixed rules.')
    (OUT/'wallet_strategy_backtest.md').write_text('\n'.join(md))
    print('WROTE',OUT/'wallet_strategy_backtest.md')
    print(json.dumps({'top':flat[:5], 'report':str(OUT/'wallet_strategy_backtest.md')},indent=2)[:4000])
if __name__=='__main__': main()
