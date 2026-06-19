#!/usr/bin/env python3
"""Backtest selected strict BTC 5m strategy.

Selected after strict proxy search:
- evaluate 3s before close
- require 5/5 Binance momentum consensus over 5,10,20,40,80s
- side = direction of consensus
- require chosen contract conservative ask in [0.08, 0.95]
- conservative ask = latest same-side Polymarket trade print + 5c
- latest same-side print must be <=20s old
- require absolute 80s Binance return >= 0.01%
- actual outcome = Chainlink/Gamma final_price >= price_to_beat
- PnL uses fixed $1 stake and dynamic Polymarket crypto taker fee

Limitation: historical cache has trade prints, not CLOB depth. This is a
stress-tested conservative trade-print ask proxy, not exact historical FOK replay.
"""
from __future__ import annotations

import argparse, json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

from scripts.search_strict_polymarket_btc5m_strategy import load_market_inputs, conservative_ask, fee_per_share
from scripts.backtest_momentum_requested_variations import consensus_score_from_map, winner_for_reference
from polybot.backtest.binance_strategy_lab import SideSignal

WINDOWS = (5, 10, 20, 40, 80)


def day(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ex = [r for r in rows if r.get('executed')]
    pnls = [r['pnl'] for r in ex]
    wins = [p for p in pnls if p > 0]
    eq = peak = 0.0; dd = 0.0
    for r in rows:
        if r.get('executed'):
            eq += r['pnl']; peak = max(peak, eq); dd = min(dd, eq - peak)
    return {
        'markets_total': len(rows),
        'executed_trades': len(ex),
        'coverage': len(ex)/len(rows) if rows else 0.0,
        'win_rate': len(wins)/len(ex) if ex else 0.0,
        'total_pnl_usd_per_1usd_stake': round(sum(pnls), 6),
        'avg_pnl_usd_per_trade': round(mean(pnls), 6) if pnls else 0.0,
        'max_drawdown_usd_per_1usd_stake': round(dd, 6),
        'skip_reasons': dict(Counter(r.get('reason') for r in rows if not r.get('executed'))),
    }


def run(markets, ask_buffer: float, min_ask: float, max_ask: float, min_abs_ret80: float, reference: str):
    rows=[]
    for m in markets:
        ts = m.end_ts - 3
        close = {c.ts: c.close for c in m.candles}
        score = consensus_score_from_map(close, ts, WINDOWS)
        if score not in (5, -5):
            rows.append({'market_slug': m.market_slug, 'executed': False, 'reason': 'no_5of5_consensus'})
            continue
        side = SideSignal.UP if score == 5 else SideSignal.DOWN
        spot = close.get(ts); base = close.get(ts-80)
        if spot is None or base is None or base <= 0:
            rows.append({'market_slug': m.market_slug, 'executed': False, 'reason': 'missing_binance_context'})
            continue
        abs_ret80 = abs((spot - base) / base)
        if abs_ret80 < min_abs_ret80:
            rows.append({'market_slug': m.market_slug, 'executed': False, 'reason': 'weak_80s_move'})
            continue
        pts = m.up_points if side == SideSignal.UP else m.down_points
        ask, price_ts, reason = conservative_ask(pts, ts, 20, ask_buffer)
        if ask is None:
            rows.append({'market_slug': m.market_slug, 'executed': False, 'reason': reason or 'missing_price'})
            continue
        if ask < min_ask:
            rows.append({'market_slug': m.market_slug, 'executed': False, 'reason': 'ask_below_min'})
            continue
        if ask > max_ask:
            rows.append({'market_slug': m.market_slug, 'executed': False, 'reason': 'ask_above_cap'})
            continue
        shares = 1.0 / ask
        fee = shares * fee_per_share(ask, 0.072)
        winner = winner_for_reference(m, reference)
        pnl = (shares if side == winner else 0.0) - 1.0 - fee
        rows.append({
            'market_slug': m.market_slug, 'executed': True, 'reference': reference,
            'side': side.value, 'winner': winner.value, 'entry_ts': ts,
            'entry_ask': ask, 'entry_price_ts': price_ts, 'score': score,
            'abs_ret80': abs_ret80, 'shares': shares, 'fee': fee, 'pnl': pnl,
            'day': day(m.end_ts),
        })
    return rows


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--market-input-cache', type=Path, default=Path('data/backtests/cache/btc_5m_market_inputs_30d.json'))
    ap.add_argument('--output', type=Path, default=Path('data/backtests/selected_strict_fixed3_5of5_strategy_30d.json'))
    args=ap.parse_args()
    meta, markets=load_market_inputs(args.market_input_cache)
    params={'entry_seconds_before_close':3,'windows':list(WINDOWS),'min_consensus':5,'ask_buffer':0.05,'min_ask':0.08,'max_ask':0.95,'max_trade_age_seconds':20,'min_abs_ret80':0.0001,'stake_usd':1.0,'fee_rate':0.072}
    chain=run(markets, reference='chainlink', **{k:params[k] for k in ['ask_buffer','min_ask','max_ask','min_abs_ret80']})
    bnb=run(markets, reference='binance', **{k:params[k] for k in ['ask_buffer','min_ask','max_ask','min_abs_ret80']})
    daily=[]
    by=defaultdict(list)
    for r in chain: by[r.get('day','skip')].append(r)
    for d, rs in sorted((k,v) for k,v in by.items() if k!='skip'):
        daily.append({'day':d, **summarize(rs)})
    payload={'generated_at':datetime.now(timezone.utc).isoformat(),'source_cache':str(args.market_input_cache),'source_meta':meta,'strategy':params,'execution_model':'conservative trade-print ask proxy; no historical CLOB depth in cache','chainlink_reference':summarize(chain),'binance_reference':summarize(bnb),'profitable_days':sum(1 for d in daily if d['total_pnl_usd_per_1usd_stake']>0),'day_count':len(daily),'daily':daily,'top_wins':sorted([r for r in chain if r.get('executed')], key=lambda r:r['pnl'], reverse=True)[:10],'top_losses':sorted([r for r in chain if r.get('executed')], key=lambda r:r['pnl'])[:10]}
    args.output.write_text(json.dumps(payload, indent=2))
    print(json.dumps({k:payload[k] for k in ['strategy','chainlink_reference','binance_reference','profitable_days','day_count']}, indent=2))

if __name__=='__main__': main()
