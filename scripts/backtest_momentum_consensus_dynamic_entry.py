from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from polybot.backtest.binance_strategy_lab import BinanceLabConfig, backtest_market, summarize_results
from polybot.backtest.momentum_consensus_dynamic_entry import backtest_market_first_consensus, top_momentum_consensus_no_price_cap
from scripts.backtest_binance_strategy_lab import build_market_inputs, load_btc_5m_markets, result_to_dict


TOP_FIXED_NAME = "momentum_consensus_07"


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest dynamic-entry momentum consensus strategy")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--max-workers", type=int, default=12)
    parser.add_argument("--output", type=Path, default=Path("data/backtests/momentum_consensus_dynamic_entry_30d.json"))
    args = parser.parse_args()

    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = end_ts - (args.days * 86400)

    print(f"Loading BTC 5m markets for last {args.days} days...")
    markets = load_btc_5m_markets(start_ts, end_ts)
    print(f"Loaded {len(markets)} markets")

    print("Fetching Polymarket trades + Binance 1s candles market-by-market...")
    market_inputs = build_market_inputs(markets, max_workers=args.max_workers)
    print(f"Prepared {len(market_inputs)} market inputs")

    config = BinanceLabConfig(assumed_fee_per_share=0.0)
    dynamic_spec = top_momentum_consensus_no_price_cap()

    print("Running dynamic first-consensus backtest...")
    dynamic_chainlink = [backtest_market_first_consensus(m, dynamic_spec, reference="chainlink", config=config) for m in market_inputs]
    dynamic_binance = [backtest_market_first_consensus(m, dynamic_spec, reference="binance", config=config) for m in market_inputs]
    dynamic_summary = {
        "strategy": {
            "name": dynamic_spec.name + "_first_consensus_no_price_cap",
            "family": dynamic_spec.family,
            "windows": list(dynamic_spec.windows),
            "entry_rule": "first_timestamp_with_consensus",
            "min_consensus": dynamic_spec.min_consensus,
            "price_cap": None,
        },
        "chainlink_reference": summarize_results(dynamic_chainlink),
        "binance_reference": summarize_results(dynamic_binance),
        "top_chainlink_wins": [result_to_dict(r) for r in sorted(dynamic_chainlink, key=lambda item: item.net_pnl, reverse=True)[:10]],
        "top_chainlink_losses": [result_to_dict(r) for r in sorted(dynamic_chainlink, key=lambda item: item.net_pnl)[:10]],
    }

    fixed_chainlink = [backtest_market(m, dynamic_spec, config=config, reference="chainlink") for m in market_inputs]
    fixed_binance = [backtest_market(m, dynamic_spec, config=config, reference="binance") for m in market_inputs]
    fixed_summary = {
        "strategy": {
            "name": dynamic_spec.name + "_fixed_5s_no_price_cap",
            "family": dynamic_spec.family,
            "windows": list(dynamic_spec.windows),
            "entry_rule": "fixed_5s_before_close",
            "min_consensus": dynamic_spec.min_consensus,
            "price_cap": None,
        },
        "chainlink_reference": summarize_results(fixed_chainlink),
        "binance_reference": summarize_results(fixed_binance),
    }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": args.days,
        "market_count": len(markets),
        "prepared_market_count": len(market_inputs),
        "dynamic_first_consensus": dynamic_summary,
        "fixed_5s_no_price_cap": fixed_summary,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {args.output}")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
