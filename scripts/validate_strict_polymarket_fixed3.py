#!/usr/bin/env python3
"""Validate selected strict BTC 5m Polymarket strategy by time splits/stress."""
from __future__ import annotations

import argparse, json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.search_strict_polymarket_btc5m_strategy import Candidate, load_market_inputs, replay_candidate, summarize


def bucket_day(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')


def candidate(buffer: float) -> Candidate:
    return Candidate(
        name=f"fixed3s_momo_5_10_20_40_80_5of5_buf{int(buffer*100)}c_cap95_age20",
        windows=(5,10,20,40,80),
        min_consensus=5,
        entry_rule="fixed",
        entry_seconds_before_close=3,
        ask_buffer=buffer,
        max_trade_age=20,
        max_ask=0.95,
        hedge_buffer=None,
        hedge_trigger_consensus=None,
    )


def validate(markets, buffer: float) -> dict[str, Any]:
    c = candidate(buffer)
    chain = [replay_candidate(m, c, 'chainlink') for m in markets]
    bnb = [replay_candidate(m, c, 'binance') for m in markets]
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m, r in zip(markets, chain, strict=False):
        by_day[bucket_day(m.end_ts)].append(r)
    day_rows = []
    for day, rows in sorted(by_day.items()):
        s = summarize(rows)
        day_rows.append({"day": day, **s})
    profitable_days = sum(1 for d in day_rows if d['total_pnl_usd_per_1usd_stake'] > 0)
    return {
        "candidate": c.to_dict(),
        "chainlink_reference": summarize(chain),
        "binance_reference": summarize(bnb),
        "profitable_days": profitable_days,
        "day_count": len(day_rows),
        "profitable_day_rate": profitable_days / len(day_rows) if day_rows else 0.0,
        "worst_days": sorted(day_rows, key=lambda x: x['total_pnl_usd_per_1usd_stake'])[:5],
        "best_days": sorted(day_rows, key=lambda x: x['total_pnl_usd_per_1usd_stake'], reverse=True)[:5],
        "daily": day_rows,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--market-input-cache', type=Path, default=Path('data/backtests/cache/btc_5m_market_inputs_30d.json'))
    ap.add_argument('--output', type=Path, default=Path('data/backtests/strict_polymarket_fixed3_validation_30d.json'))
    args = ap.parse_args()
    meta, markets = load_market_inputs(args.market_input_cache)
    runs = {f"buffer_{int(b*100)}c": validate(markets, b) for b in [0.0,0.01,0.02,0.03,0.05]}
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "source_meta": meta, "runs": runs}
    args.output.write_text(json.dumps(payload, indent=2))
    print(json.dumps({k: {"chain": v['chainlink_reference'], "binance": v['binance_reference'], "profitable_day_rate": v['profitable_day_rate'], "worst_days": v['worst_days'][:2]} for k,v in runs.items()}, indent=2))

if __name__ == '__main__':
    main()
