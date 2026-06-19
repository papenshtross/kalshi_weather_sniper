#!/usr/bin/env python3
"""Test top BTC 5m strategies against one PMXT public L2 archive hour.

This is local-authored code. It reads PMXT Parquet directly and does not execute
third-party repository scripts.
"""
from __future__ import annotations

import argparse, bisect, json, sys, urllib.request, time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean, median
from typing import Any

import pyarrow.dataset as ds
import pyarrow.compute as pc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.search_strict_polymarket_btc5m_strategy import Candidate, load_market_inputs, replay_candidate, fee_per_share  # noqa: E402

@dataclass
class Snapshot:
    ts: float | None
    asks: dict[float, float]
    bids: dict[float, float]


def cand(d: dict[str, Any]) -> Candidate:
    return Candidate(name=d['name'], windows=tuple(d['windows']), min_consensus=d['min_consensus'], entry_rule=d['entry_rule'], entry_seconds_before_close=d.get('entry_seconds_before_close'), ask_buffer=float(d['ask_buffer']), max_trade_age=int(d['max_trade_age']), max_ask=float(d['max_ask']), hedge_buffer=d.get('hedge_buffer'), hedge_trigger_consensus=d.get('hedge_trigger_consensus'), fee_rate=float(d.get('fee_rate',0.072)), stake_usd=float(d.get('stake_usd',1.0)))


def token_maps(slugs: list[str], cache: Path) -> dict[str, dict[str, str]]:
    data=json.loads(cache.read_text()) if cache.exists() else {}
    changed=False
    for slug in slugs:
        if slug in data and not data[slug].get('_error'): continue
        req=urllib.request.Request(f'https://gamma-api.polymarket.com/events?slug={slug}', headers={'User-Agent':'Mozilla/5.0 Hermes PMXT L2 tester'})
        try:
            ev=json.load(urllib.request.urlopen(req, timeout=20))
            m=ev[0]['markets'][0]
            outs=json.loads(m['outcomes']) if isinstance(m.get('outcomes'), str) else m['outcomes']
            toks=json.loads(m['clobTokenIds']) if isinstance(m.get('clobTokenIds'), str) else m['clobTokenIds']
            data[slug]={str(tok): str(out).lower() for out,tok in zip(outs,toks)}
        except Exception as e:
            data[slug]={'_error':repr(e)}
        changed=True; time.sleep(0.05)
    if changed:
        cache.parent.mkdir(parents=True, exist_ok=True); cache.write_text(json.dumps(data,indent=2,sort_keys=True))
    return data


def parse_levels(s: str | None) -> dict[float, float]:
    if not s: return {}
    arr=json.loads(s)
    out={}
    for row in arr:
        p=float(row[0]); z=float(row[1])
        if z>0: out[p]=z
    return out


def fill(snap: Snapshot, limit: float, shares: float) -> dict[str, Any]:
    asks=sorted(snap.asks.items())
    best=asks[0][0] if asks else None
    depth=sum(z for p,z in asks if p <= limit + 1e-12)
    rem=shares; cost=0.0; levels=0
    for p,z in asks:
        if p > limit + 1e-12: break
        take=min(rem,z)
        if take>0:
            cost += take*p; rem -= take; levels += 1
        if rem <= 1e-12: break
    ok=rem <= 1e-12
    return {'filled':ok, 'best_ask':best, 'depth_at_limit':depth, 'avg_price': cost/shares if ok else None, 'levels_used':levels}


def pnl_unhedged(side: str, winner: str, avg: float, shares: float, fee_rate: float):
    fee=shares*fee_per_share(avg, fee_rate)
    payout=shares if side==winner else 0.0
    return payout - shares*avg - fee, fee


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--pmxt', type=Path, default=ROOT/'data/l2/pmxt/polymarket_orderbook_2026-04-16T07.parquet')
    ap.add_argument('--market-cache', type=Path, default=ROOT/'data/backtests/cache/btc_5m_market_inputs_30d.json')
    ap.add_argument('--search-json', type=Path, default=ROOT/'data/backtests/strict_polymarket_strategy_search_30d.json')
    ap.add_argument('--token-cache', type=Path, default=ROOT/'data/backtests/cache/pmxt_l2_token_maps.json')
    ap.add_argument('--output', type=Path, default=ROOT/'data/backtests/l2_pmxt_2026-04-16T07_top_strategies_report.json')
    ap.add_argument('--top', type=int, default=44)
    args=ap.parse_args()

    _, all_markets=load_market_inputs(args.market_cache)
    # One PMXT UTC hour. Markets starting from 07:10 through 07:55 have entry/hedge times inside this file.
    start=1776323400; end=1776326400
    markets=[m for m in all_markets if start <= m.start_ts < end]
    slugs=[m.market_slug for m in markets]
    maps=token_maps(slugs, args.token_cache)
    side_to_token={slug:{side:aid for aid,side in maps.get(slug,{}).items() if side in {'up','down'}} for slug in slugs}
    token_to_slug_side={aid:(slug,side) for slug, mp in maps.items() if slug in slugs for aid,side in mp.items() if side in {'up','down'}}
    tokens=list(token_to_slug_side)

    search=json.loads(args.search_json.read_text())
    tops=search['top_ranked'][:args.top]
    cands=[cand(x['candidate']) for x in tops]

    # First compute proxy decisions and required L2 observation times.
    proxy: dict[tuple[str,str], dict[str, Any]]={}
    targets_by_asset: dict[str, list[tuple[float, str]]]=defaultdict(list)
    for c in cands:
        for m in markets:
            r=replay_candidate(m,c,'chainlink')
            key=(c.name,m.market_slug); proxy[key]=r
            if not r.get('executed'): continue
            side=str(r['side']).lower(); aid=side_to_token.get(m.market_slug,{}).get(side)
            if aid: targets_by_asset[aid].append((float(r['entry_ts']), key[0]+'|'+key[1]+'|entry'))
            if r.get('hedged') and r.get('hedge_ts') is not None:
                opp='down' if side=='up' else 'up'; haid=side_to_token.get(m.market_slug,{}).get(opp)
                if haid: targets_by_asset[haid].append((float(r['hedge_ts']), key[0]+'|'+key[1]+'|hedge'))
    for aid in targets_by_asset:
        targets_by_asset[aid].sort()

    # Scan PMXT relevant token events and snapshot book state at target times.
    dataset=ds.dataset(str(args.pmxt), format='parquet')
    table=dataset.to_table(columns=['timestamp','event_type','asset_id','bids','asks','price','size','side'], filter=pc.field('asset_id').isin(tokens))
    states={aid:Snapshot(None,{}, {}) for aid in tokens}
    idx={aid:0 for aid in tokens}
    observations: dict[str, dict[str, Any]]={}
    event_counts=Counter()

    def record_until(aid: str, t: float):
        arr=targets_by_asset.get(aid, [])
        st=states[aid]
        while idx[aid] < len(arr) and arr[idx[aid]][0] < t:
            target_ts, tid=arr[idx[aid]]
            observations[tid]={'target_ts':target_ts, 'book_ts':st.ts, 'asks':dict(st.asks), 'bids':dict(st.bids)}
            idx[aid]+=1

    # Table should be timestamp ordered from PMXT. If not, pyarrow preserves parquet row order; docs imply event stream dumps.
    cols=table.to_pydict()
    for ts,event,aid,bids,asks,price,size,side in zip(cols['timestamp'], cols['event_type'], cols['asset_id'], cols['bids'], cols['asks'], cols['price'], cols['size'], cols['side']):
        t=ts.timestamp()
        record_until(aid, t)
        st=states[aid]
        if event=='book':
            st.bids=parse_levels(bids); st.asks=parse_levels(asks); st.ts=t; event_counts['book']+=1
        elif event=='price_change':
            p=float(price); z=float(size); book=st.bids if side=='BUY' else st.asks
            if z<=0: book.pop(p, None)
            else: book[p]=z
            st.ts=t; event_counts['price_change']+=1
        else:
            event_counts[event]+=1
    for aid in tokens:
        record_until(aid, float('inf'))

    rows_by_cand=defaultdict(list)
    for c in cands:
        for m in markets:
            r=proxy[(c.name,m.market_slug)]
            row={'candidate':c.name,'market_slug':m.market_slug,'proxy_executed':bool(r.get('executed')),'proxy_reason':r.get('reason')}
            if not r.get('executed'):
                rows_by_cand[c.name].append(row); continue
            side=str(r['side']).lower(); winner=str(r['winner']).lower(); shares=float(r['shares']); limit=float(r['entry_ask'])
            oid=f'{c.name}|{m.market_slug}|entry'; obs=observations.get(oid)
            row.update({'side':side,'winner':winner,'entry_ts':r['entry_ts'],'entry_limit':limit,'proxy_pnl':r.get('pnl'),'hedged_by_proxy':bool(r.get('hedged'))})
            if not obs or obs['book_ts'] is None:
                row.update({'entry_l2_filled':False,'strict_l2_filled':False,'l2_reason':'no_pmxt_book_before_entry'}); rows_by_cand[c.name].append(row); continue
            snap=Snapshot(obs['book_ts'], obs['asks'], obs['bids']); f=fill(snap,limit,shares)
            row.update({'entry_book_ts':obs['book_ts'],'entry_book_age_s':round(float(r['entry_ts'])-obs['book_ts'],6),'entry_best_ask':f['best_ask'],'entry_depth_at_limit':round(f['depth_at_limit'],8),'entry_l2_filled':f['filled'],'entry_avg_price':f['avg_price']})
            if not f['filled']:
                row.update({'strict_l2_filled':False,'l2_reason':'insufficient_entry_depth_at_limit'}); rows_by_cand[c.name].append(row); continue
            entry_avg=float(f['avg_price'])
            if r.get('hedged') and r.get('hedge_ts') is not None and r.get('hedge_ask') is not None:
                oid=f'{c.name}|{m.market_slug}|hedge'; hobs=observations.get(oid)
                if not hobs or hobs['book_ts'] is None:
                    p,fee=pnl_unhedged(side,winner,entry_avg,shares,c.fee_rate)
                    row.update({'strict_l2_filled':False,'l2_reason':'entry_filled_but_no_hedge_book','l2_pnl_unhedged_if_no_hedge':p}); rows_by_cand[c.name].append(row); continue
                hsnap=Snapshot(hobs['book_ts'],hobs['asks'],hobs['bids']); hf=fill(hsnap,float(r['hedge_ask']),shares)
                row.update({'hedge_ts':r['hedge_ts'],'hedge_limit':r['hedge_ask'],'hedge_book_ts':hobs['book_ts'],'hedge_book_age_s':round(float(r['hedge_ts'])-hobs['book_ts'],6),'hedge_best_ask':hf['best_ask'],'hedge_depth_at_limit':round(hf['depth_at_limit'],8),'hedge_l2_filled':hf['filled'],'hedge_avg_price':hf['avg_price']})
                if not hf['filled']:
                    p,fee=pnl_unhedged(side,winner,entry_avg,shares,c.fee_rate)
                    row.update({'strict_l2_filled':False,'l2_reason':'entry_filled_but_insufficient_hedge_depth','l2_pnl_unhedged_if_no_hedge':p}); rows_by_cand[c.name].append(row); continue
                hedge_avg=float(hf['avg_price']); entry_fee=shares*fee_per_share(entry_avg,c.fee_rate); hedge_fee=shares*fee_per_share(hedge_avg,c.fee_rate)
                pnl=shares*(1-entry_avg-hedge_avg)-entry_fee-hedge_fee
                row.update({'strict_l2_filled':True,'l2_reason':'filled_entry_and_hedge','l2_pnl':pnl,'entry_fee':entry_fee,'hedge_fee':hedge_fee})
            else:
                pnl,fee=pnl_unhedged(side,winner,entry_avg,shares,c.fee_rate)
                row.update({'strict_l2_filled':True,'l2_reason':'filled_entry','l2_pnl':pnl,'entry_fee':fee})
            rows_by_cand[c.name].append(row)

    def summ(rows):
        ex=[r for r in rows if r['proxy_executed']]; ef=[r for r in ex if r.get('entry_l2_filled')]; sf=[r for r in ex if r.get('strict_l2_filled')]
        pnls=[r['l2_pnl'] for r in sf if r.get('l2_pnl') is not None]; ages=[r['entry_book_age_s'] for r in ef if r.get('entry_book_age_s') is not None]
        return {'markets_total':len(rows),'proxy_executed':len(ex),'entry_l2_filled':len(ef),'strict_l2_filled':len(sf),'entry_fill_rate_vs_proxy':round(len(ef)/len(ex),6) if ex else 0,'strict_fill_rate_vs_proxy':round(len(sf)/len(ex),6) if ex else 0,'l2_total_pnl':round(sum(pnls),6) if pnls else 0,'l2_avg_pnl':round(mean(pnls),6) if pnls else 0,'l2_win_rate':round(sum(1 for p in pnls if p>0)/len(pnls),6) if pnls else 0,'median_entry_book_age_s':round(median(ages),3) if ages else None,'p95_entry_book_age_s':round(sorted(ages)[int(.95*(len(ages)-1))],3) if len(ages)>1 else (round(ages[0],3) if ages else None),'l2_fail_reasons':dict(Counter(r.get('l2_reason') for r in ex if not r.get('strict_l2_filled')))}

    ranked=[]
    for c in cands:
        base=next((x for x in tops if x['candidate']['name']==c.name),{})
        ranked.append({'candidate':c.to_dict(),'original_30d_chainlink':base.get('chainlink_reference'),'pmxt_l2_sample':summ(rows_by_cand[c.name])})
    ranked.sort(key=lambda r:(r['pmxt_l2_sample']['l2_total_pnl'], r['pmxt_l2_sample']['strict_l2_filled']), reverse=True)
    payload={'generated_at':datetime.now(timezone.utc).isoformat(),'data_source':{'repo':'https://archive.pmxt.dev/Polymarket/v2','file':str(args.pmxt),'markets_in_sample':len(markets),'sample_start_ts':start,'sample_end_ts':end,'tokens':len(tokens),'pmxt_rows_for_tokens':table.num_rows,'event_counts':dict(event_counts),'note':'One UTC hour PMXT true L2 replay sample; much stronger than Cronos side-incomplete sample but still too small for final acceptance.'},'method':{'candidates_tested':len(cands),'third_party_code_execution':'none','fill_rule':'latest reconstructed ask book at/before strategy ts, $1/share limit buy, enough ask depth at <= strategy limit required; proxy hedges require hedge fill too.'},'ranked_results':ranked,'details_by_candidate':rows_by_cand}
    args.output.write_text(json.dumps(payload,indent=2))
    print(json.dumps({'output':str(args.output),'markets':len(markets),'rows':table.num_rows,'event_counts':dict(event_counts),'top_12':[{ 'name':r['candidate']['name'], **r['pmxt_l2_sample']} for r in ranked[:12]]}, indent=2))

if __name__=='__main__': main()
