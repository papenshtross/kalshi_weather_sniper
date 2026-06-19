from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from polybot.backtest.binance_strategy_lab import BinanceLabConfig
from scripts.backtest_momentum_5of5_hedge_sizes import load_market_inputs
from scripts.backtest_momentum_requested_variations import (
    WINDOWS,
    agreement,
    backtest_first_consensus_with_optional_hedge,
    summarize_dict_results,
)

MARKET_INPUT_CACHE = Path("data/backtests/cache/btc_5m_market_inputs_30d.json")
OUTPUT = Path("data/backtests/momentum_5of5_hedge_triggers_1_5_dynamic_fees_30d.json")
HEDGE_CENTS = [0, 1, 2, 3, 4, 5, 10, 15, 25]
LIVE_TRIGGERS = [1, 2, 3, 4, 5]
MIN_CONSENSUS = 5
POLYMARKET_FEE_RATE = 0.072


def main() -> None:
    market_count, market_inputs = load_market_inputs(MARKET_INPUT_CACHE)
    config = BinanceLabConfig(assumed_fee_per_share=0.0, contract_trade_staleness_seconds=20)
    runs = {}
    summary_rows = []

    for live_trigger in LIVE_TRIGGERS:
        absolute_trigger = max(0, MIN_CONSENSUS - live_trigger)
        trigger_key = f"live_trigger_{live_trigger}_absolute_{absolute_trigger}of5"
        print(f"Running {trigger_key}...")
        runs[trigger_key] = {
            "live_hedge_consensus_trigger_votes_lost": live_trigger,
            "absolute_backtest_hedge_trigger_consensus": absolute_trigger,
            "meaning": f"5/5 entry hedges when original-side consensus weakens to {absolute_trigger}/5 or lower",
            "hedge_size_variations": {},
        }
        for cents in HEDGE_CENTS:
            buffer = cents / 100.0
            run_key = f"hedge_{cents}c"
            runs[trigger_key]["hedge_size_variations"][run_key] = {
                "strategy": {
                    "windows": list(WINDOWS),
                    "entry_rule": "first_consensus",
                    "min_consensus": MIN_CONSENSUS,
                    "live_hedge_consensus_trigger_votes_lost": live_trigger,
                    "absolute_backtest_hedge_trigger_consensus": absolute_trigger,
                    "hedge_profit_buffer": buffer,
                    "polymarket_dynamic_fees": True,
                    "polymarket_fee_rate": POLYMARKET_FEE_RATE,
                }
            }
            for reference in ("chainlink", "binance"):
                results = [
                    backtest_first_consensus_with_optional_hedge(
                        m,
                        MIN_CONSENSUS,
                        buffer,
                        reference,
                        config,
                        hedge_trigger_consensus=absolute_trigger,
                        polymarket_fee_rate=POLYMARKET_FEE_RATE,
                    )
                    for m in market_inputs
                ]
                ref_summary = summarize_dict_results(results)
                runs[trigger_key]["hedge_size_variations"][run_key][f"{reference}_reference"] = ref_summary
            chain = runs[trigger_key]["hedge_size_variations"][run_key]["chainlink_reference"]
            bina = runs[trigger_key]["hedge_size_variations"][run_key]["binance_reference"]
            summary_rows.append({
                "live_trigger": live_trigger,
                "absolute_trigger": absolute_trigger,
                "hedge_cents": cents,
                "chainlink_pnl": chain["total_pnl"],
                "chainlink_win_rate": chain["win_rate"],
                "chainlink_hedged": chain.get("hedged_trades", 0),
                "chainlink_hedge_rate": chain.get("hedge_rate", 0),
                "binance_pnl": bina["total_pnl"],
                "binance_win_rate": bina["win_rate"],
                "binance_hedged": bina.get("hedged_trades", 0),
                "binance_hedge_rate": bina.get("hedge_rate", 0),
                "trades": chain["executed_trades"],
            })
            print(summary_rows[-1])

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market_count": market_count,
        "prepared_market_count": len(market_inputs),
        "chainlink_vs_binance_winner_agreement": agreement(market_inputs),
        "min_consensus": MIN_CONSENSUS,
        "live_trigger_mapping": "live trigger = votes lost from 5/5; absolute backtest threshold = 5 - live_trigger",
        "hedge_cents": HEDGE_CENTS,
        "polymarket_fee_rate": POLYMARKET_FEE_RATE,
        "runs": runs,
        "summary_rows": summary_rows,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {OUTPUT}")
    print(json.dumps({
        "generated_at": payload["generated_at"],
        "market_count": market_count,
        "prepared_market_count": len(market_inputs),
        "agreement": payload["chainlink_vs_binance_winner_agreement"],
        "top_by_chainlink": sorted(summary_rows, key=lambda r: r["chainlink_pnl"], reverse=True)[:15],
        "best_per_live_trigger": [
            max([r for r in summary_rows if r["live_trigger"] == t], key=lambda r: r["chainlink_pnl"])
            for t in LIVE_TRIGGERS
        ],
    }, indent=2))


if __name__ == "__main__":
    main()
