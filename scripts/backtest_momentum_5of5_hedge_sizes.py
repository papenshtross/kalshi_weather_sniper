from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from polybot.backtest.binance_strategy_lab import BinanceLabConfig, CandlePoint, MarketBacktestInput, PricePoint
from scripts.backtest_momentum_requested_variations import (
    WINDOWS,
    agreement,
    backtest_first_consensus_with_optional_hedge,
    summarize_dict_results,
)


def load_market_inputs(path: Path) -> tuple[int, list[MarketBacktestInput]]:
    raw = json.loads(path.read_text())
    market_inputs = [
        MarketBacktestInput(
            market_id=item["market_id"],
            market_slug=item["market_slug"],
            start_ts=item["start_ts"],
            end_ts=item["end_ts"],
            price_to_beat=item["price_to_beat"],
            final_price=item["final_price"],
            binance_start_price=item["binance_start_price"],
            binance_end_price=item["binance_end_price"],
            candles=[CandlePoint(**c) for c in item["candles"]],
            up_points=[PricePoint(**p) for p in item["up_points"]],
            down_points=[PricePoint(**p) for p in item["down_points"]],
        )
        for item in raw["market_inputs"]
    ]
    return raw.get("market_count", len(market_inputs)), market_inputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest 5/5 first-consensus momentum with varying hedge profit buffers")
    parser.add_argument("--market-input-cache", type=Path, default=Path("data/backtests/cache/btc_5m_market_inputs_30d.json"))
    parser.add_argument("--output", type=Path, default=Path("data/backtests/momentum_5of5_hedge_sizes_30d.json"))
    parser.add_argument("--hedge-cents", type=int, nargs="+", default=[1, 5, 15, 20, 25])
    parser.add_argument("--hedge-trigger-consensus", type=int, default=None, help="Original-side consensus score at or below which hedge search begins; e.g. 3 means 5/5 entry hedges after weakening to 3/5")
    parser.add_argument("--polymarket-fee-rate", type=float, default=0.072, help="Polymarket dynamic taker fee rate; crypto markets default to 0.072")
    args = parser.parse_args()

    print(f"Loading prepared market inputs cache {args.market_input_cache}")
    market_count, market_inputs = load_market_inputs(args.market_input_cache)
    print(f"Prepared market inputs: {len(market_inputs)}")

    config = BinanceLabConfig(assumed_fee_per_share=0.0, contract_trade_staleness_seconds=20)
    runs = {}
    min_consensus = 5
    for cents in args.hedge_cents:
        buffer = cents / 100.0
        run_key = f"first_consensus_5_of_5_then_hedge_{cents}c"
        print(f"Running {run_key}...")
        runs[run_key] = {
            "strategy": {
                "windows": list(WINDOWS),
                "entry_rule": "first_consensus",
                "min_consensus": min_consensus,
                "hedge_rule": f"if original-side consensus weakens to {args.hedge_trigger_consensus if args.hedge_trigger_consensus is not None else min_consensus - 1}/5 or lower, buy opposite side if net locked PnL after dynamic fees >= {buffer:.2f}",
                "hedge_profit_buffer": buffer,
                "hedge_trigger_consensus": args.hedge_trigger_consensus if args.hedge_trigger_consensus is not None else min_consensus - 1,
                "polymarket_dynamic_fees": True,
                "polymarket_fee_rate": args.polymarket_fee_rate,
                "price_cap": None,
                "volatility_filter": False,
            }
        }
        for reference in ("chainlink", "binance"):
            results = [
                backtest_first_consensus_with_optional_hedge(
                    m,
                    min_consensus,
                    buffer,
                    reference,
                    config,
                    hedge_trigger_consensus=args.hedge_trigger_consensus,
                    polymarket_fee_rate=args.polymarket_fee_rate,
                )
                for m in market_inputs
            ]
            runs[run_key][f"{reference}_reference"] = summarize_dict_results(results)
            runs[run_key][f"{reference}_top_wins"] = sorted([r for r in results if r.get("executed")], key=lambda r: r["pnl"], reverse=True)[:5]
            runs[run_key][f"{reference}_top_losses"] = sorted([r for r in results if r.get("executed")], key=lambda r: r["pnl"])[:5]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market_count": market_count,
        "prepared_market_count": len(market_inputs),
        "source_model": {
            "signal": "Binance BTCUSDT 1-second candles",
            "entry_price_proxy": "Polymarket Data API trade prints by conditionId/outcome",
            "actual_reference": "Chainlink/Polymarket Gamma eventMetadata finalPrice vs priceToBeat",
            "diagnostic_reference": "Binance market end close vs start close",
        },
        "chainlink_vs_binance_winner_agreement": agreement(market_inputs),
        "hedge_size_variations": runs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {args.output}")
    print(json.dumps({
        "generated_at": payload["generated_at"],
        "market_count": payload["market_count"],
        "prepared_market_count": payload["prepared_market_count"],
        "chainlink_vs_binance_winner_agreement": payload["chainlink_vs_binance_winner_agreement"],
        "hedge_size_variations": {k: {ref: v[ref] for ref in ("chainlink_reference", "binance_reference")} for k, v in runs.items()},
    }, indent=2))


if __name__ == "__main__":
    main()
