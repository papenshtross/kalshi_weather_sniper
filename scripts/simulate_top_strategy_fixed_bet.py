from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from polybot.backtest.binance_strategy_lab import BinanceLabConfig, build_strategy_universe, backtest_market
from scripts.backtest_binance_strategy_lab import build_market_inputs, load_btc_5m_markets


ARTIFACT = Path("data/backtests/binance_strategy_lab_30d.json")
FIXED_BET = 100.0


def main() -> None:
    payload = json.loads(ARTIFACT.read_text())
    generated_at = datetime.fromisoformat(payload["generated_at"])
    end_ts = int(generated_at.timestamp())
    start_ts = end_ts - (payload["days"] * 86400)
    top_name = payload["top5"][0]["strategy"]["name"]

    strategies = {strategy.name: strategy for strategy in build_strategy_universe()}
    strategy = strategies[top_name]
    config = BinanceLabConfig(assumed_fee_per_share=0.0)

    markets = load_btc_5m_markets(start_ts, end_ts)
    market_inputs = build_market_inputs(markets, max_workers=12)

    if len(markets) != payload["market_count"] or len(market_inputs) != payload["prepared_market_count"]:
        raise SystemExit(
            f"dataset mismatch: expected markets={payload['market_count']} prepared={payload['prepared_market_count']}, "
            f"got markets={len(markets)} prepared={len(market_inputs)}"
        )

    executed = []
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    wins = 0
    losses = 0
    total_staked = 0.0
    for market in market_inputs:
        result = backtest_market(market, strategy, config=config, reference="chainlink")
        if not result.executed or result.entry_price is None:
            continue
        shares = FIXED_BET / result.entry_price
        payout = shares if result.payout > 0 else 0.0
        pnl = payout - FIXED_BET
        equity += pnl
        total_staked += FIXED_BET
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
        if pnl > 0:
            wins += 1
        else:
            losses += 1
        executed.append(
            {
                "market_slug": result.market_slug,
                "side": result.side,
                "entry_ts": result.entry_ts,
                "entry_price": result.entry_price,
                "shares": shares,
                "pnl": pnl,
                "equity": equity,
            }
        )

    avg_entry_price = sum(item["entry_price"] for item in executed) / len(executed)
    avg_pnl = equity / len(executed)
    best_trade = max(executed, key=lambda item: item["pnl"])
    worst_trade = min(executed, key=lambda item: item["pnl"])
    summary = {
        "strategy": top_name,
        "fixed_bet": FIXED_BET,
        "days": payload["days"],
        "market_count": len(markets),
        "prepared_market_count": len(market_inputs),
        "executed_trades": len(executed),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(executed) if executed else 0.0,
        "total_staked": total_staked,
        "total_pnl": equity,
        "roi_on_total_staked": equity / total_staked if total_staked else 0.0,
        "avg_pnl_per_trade": avg_pnl,
        "avg_entry_price": avg_entry_price,
        "max_drawdown_dollars": abs(max_drawdown),
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "final_equity_if_start_10000": 10000.0 + equity,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
