from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from polybot.backtest.binance_strategy_lab import (
    BinanceLabConfig,
    CandlePoint,
    MarketBacktestInput,
    MarketBacktestResult,
    PricePoint,
    SideSignal,
    StrategySpec,
    price_at_or_before,
    price_point_at_or_before,
    summarize_results,
)
from scripts.backtest_binance_strategy_lab import build_market_inputs, load_btc_5m_markets

WINDOWS = (5, 10, 20, 40, 80)


def _close_at(candles: list[CandlePoint], ts: int) -> float | None:
    latest: float | None = None
    for candle in candles:
        if candle.ts <= ts:
            latest = candle.close
        else:
            break
    return latest


def consensus_score(candles: list[CandlePoint], ts: int, windows: tuple[int, ...] = WINDOWS, epsilon: float = 1e-9) -> int | None:
    close_by_ts = {candle.ts: candle.close for candle in candles}
    return consensus_score_from_map(close_by_ts, ts, windows, epsilon)


def consensus_score_from_map(close_by_ts: dict[int, float], ts: int, windows: tuple[int, ...] = WINDOWS, epsilon: float = 1e-9) -> int | None:
    current = close_by_ts.get(ts)
    if current is None:
        return None
    score = 0
    for window in windows:
        base = close_by_ts.get(ts - window)
        if base is None:
            return None
        delta = current - base
        if delta > epsilon:
            score += 1
        elif delta < -epsilon:
            score -= 1
    return score


def winner_for_reference(market: MarketBacktestInput, reference: str) -> SideSignal:
    if reference == "binance":
        return SideSignal.UP if market.binance_end_price >= market.binance_start_price else SideSignal.DOWN
    return SideSignal.UP if market.final_price >= market.price_to_beat else SideSignal.DOWN


def side_from_score(score: int, min_consensus: int) -> SideSignal:
    if score >= min_consensus:
        return SideSignal.UP
    if score <= -min_consensus:
        return SideSignal.DOWN
    return SideSignal.SKIP


def fresh_price_at_or_before(points: list[PricePoint], ts: int, max_age: int | None) -> tuple[float | None, int | None]:
    point = price_point_at_or_before(points, ts)
    if point is None:
        return None, None
    if max_age is not None and ts - point.ts > max_age:
        return None, point.ts
    return point.price, point.ts


def polymarket_dynamic_fee_per_share(price: float, fee_rate: float = 0.072) -> float:
    """Return Polymarket taker fee per share for a binary contract trade.

    Formula is feeRate * price * (1 - price). BTC 5m markets are crypto, so
    the default Polymarket taker fee rate is 7.2%.
    """
    try:
        p = float(price)
        r = float(fee_rate)
    except Exception:
        return 0.0
    if p < 0.0 or p > 1.0 or r <= 0.0:
        return 0.0
    return r * p * (1.0 - p)


def max_hedge_price_for_net_buffer(entry_price: float, hedge_profit_buffer: float, fee_rate: float = 0.072) -> float:
    """Highest hedge price that preserves the requested per-share net buffer.

    A fully hedged binary position pays 1.0 regardless of winner, so net locked
    PnL per share is:
        1 - entry_price - hedge_price - entry_fee - hedge_fee
    The hedge is allowed only when that value is at least hedge_profit_buffer.
    """
    entry_fee = polymarket_dynamic_fee_per_share(entry_price, fee_rate)
    budget = 1.0 - float(entry_price) - float(hedge_profit_buffer) - entry_fee
    if budget <= 0.0:
        return 0.0
    # Solve p + fee_rate*p*(1-p) <= budget. The left-hand side is monotonic on
    # [0,1] for Polymarket fee rates, so bisection gives a stable price cap.
    lo = 0.0
    hi = min(1.0, budget)
    for _ in range(64):
        mid = (lo + hi) / 2.0
        cost = mid + polymarket_dynamic_fee_per_share(mid, fee_rate)
        if cost <= budget:
            lo = mid
        else:
            hi = mid
    return max(0.0, min(0.99, lo))


def backtest_first_consensus_with_optional_hedge(
    market: MarketBacktestInput,
    min_consensus: int,
    hedge_profit_buffer: float,
    reference: str,
    config: BinanceLabConfig,
    hedge_trigger_consensus: int | None = None,
    polymarket_fee_rate: float = 0.072,
) -> dict[str, Any]:
    max_window = max(WINDOWS)
    first_ts = market.start_ts + max_window
    last_ts = market.end_ts - 1

    entry_side = SideSignal.SKIP
    entry_ts: int | None = None
    entry_score: int | None = None
    entry_price: float | None = None
    entry_price_ts: int | None = None

    close_by_ts = {candle.ts: candle.close for candle in market.candles}

    for ts in range(first_ts, last_ts + 1):
        score = consensus_score_from_map(close_by_ts, ts, WINDOWS, config.epsilon)
        if score is None:
            continue
        side = side_from_score(score, min_consensus)
        if side == SideSignal.SKIP:
            continue
        points = market.up_points if side == SideSignal.UP else market.down_points
        price, price_ts = fresh_price_at_or_before(points, ts, config.contract_trade_staleness_seconds)
        if price is None:
            continue
        entry_side = side
        entry_ts = ts
        entry_score = score
        entry_price = price
        entry_price_ts = price_ts
        break

    if entry_side == SideSignal.SKIP or entry_ts is None or entry_price is None:
        return {
            "market_slug": market.market_slug,
            "executed": False,
            "reference": reference,
            "reason": "no_consensus",
            "pnl": 0.0,
            "net_pnl": 0.0,
            "hedged": False,
        }

    opposite_side = SideSignal.DOWN if entry_side == SideSignal.UP else SideSignal.UP
    opposite_points = market.down_points if entry_side == SideSignal.UP else market.up_points
    entry_fee = polymarket_dynamic_fee_per_share(entry_price, polymarket_fee_rate)
    hedge_target = max_hedge_price_for_net_buffer(entry_price, hedge_profit_buffer, polymarket_fee_rate)
    trigger_consensus = min_consensus - 1 if hedge_trigger_consensus is None else int(hedge_trigger_consensus)
    hedge_price: float | None = None
    hedge_ts: int | None = None
    hedge_trigger_score: int | None = None
    seeking_hedge = False

    for ts in range(entry_ts + 1, last_ts + 1):
        score = consensus_score_from_map(close_by_ts, ts, WINDOWS, config.epsilon)
        if score is None:
            continue
        # If the consensus behind the original side weakens to the configured
        # hedge trigger, start looking for a guaranteed-profit opposite leg.
        if entry_side == SideSignal.UP:
            weakened = score <= trigger_consensus
        else:
            weakened = score >= -trigger_consensus
        if weakened:
            seeking_hedge = True
        if not seeking_hedge:
            continue
        price, price_ts = fresh_price_at_or_before(opposite_points, ts, config.contract_trade_staleness_seconds)
        if price is not None and price <= hedge_target + 1e-12:
            hedge_price = price
            hedge_ts = ts
            hedge_trigger_score = score
            break

    winner = winner_for_reference(market, reference)
    hedge_fee = polymarket_dynamic_fee_per_share(hedge_price, polymarket_fee_rate) if hedge_price is not None else 0.0
    if hedge_price is not None:
        gross_pnl = 1.0 - entry_price - hedge_price
        pnl = gross_pnl - entry_fee - hedge_fee - config.assumed_fee_per_share
    else:
        gross_pnl = (1.0 if entry_side == winner else 0.0) - entry_price
        pnl = gross_pnl - entry_fee - config.assumed_fee_per_share
    return {
        "market_slug": market.market_slug,
        "executed": True,
        "reference": reference,
        "side": entry_side.value,
        "score": float(entry_score or 0),
        "entry_ts": entry_ts,
        "entry_price": entry_price,
        "entry_price_ts": entry_price_ts,
        "winner": winner.value,
        "hedged": hedge_price is not None,
        "hedge_side": opposite_side.value if hedge_price is not None else None,
        "hedge_ts": hedge_ts,
        "hedge_price": hedge_price,
        "hedge_target": hedge_target,
        "hedge_trigger_score": hedge_trigger_score,
        "gross_pnl": gross_pnl,
        "entry_fee": entry_fee,
        "hedge_fee": hedge_fee,
        "total_fees": entry_fee + hedge_fee + config.assumed_fee_per_share,
        "fee_rate": polymarket_fee_rate,
        "pnl": pnl,
        "net_pnl": pnl,
    }


def backtest_fixed_entry_no_risk_filter(
    market: MarketBacktestInput,
    min_consensus: int,
    entry_seconds_before_close: int,
    reference: str,
    config: BinanceLabConfig,
) -> MarketBacktestResult:
    entry_ts = market.end_ts - entry_seconds_before_close
    close_by_ts = {candle.ts: candle.close for candle in market.candles}
    score = consensus_score_from_map(close_by_ts, entry_ts, WINDOWS, config.epsilon)
    if score is None:
        reason = "insufficient_history"
        side = SideSignal.SKIP
    else:
        side = side_from_score(score, min_consensus)
        reason = "no_consensus" if side == SideSignal.SKIP else None
    if side == SideSignal.SKIP:
        return MarketBacktestResult(market.market_id, market.market_slug, False, side, float(score or 0), entry_ts, None, 0.0, 0.0, 0.0, reference=reference, reason=reason)
    points = market.up_points if side == SideSignal.UP else market.down_points
    entry_price, _ = fresh_price_at_or_before(points, entry_ts, config.contract_trade_staleness_seconds)
    if entry_price is None:
        return MarketBacktestResult(market.market_id, market.market_slug, False, side, float(score or 0), entry_ts, None, 0.0, 0.0, 0.0, reference=reference, reason="missing_or_stale_contract_entry")
    winner = winner_for_reference(market, reference)
    payout = 1.0 if side == winner else 0.0
    pnl = payout - entry_price
    return MarketBacktestResult(market.market_id, market.market_slug, True, side, float(score or 0), entry_ts, entry_price, payout, pnl, pnl - config.assumed_fee_per_share, reference=reference)


def summarize_dict_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    executed = [r for r in results if r.get("executed")]
    wins = [r for r in executed if float(r.get("pnl", 0.0)) > 0]
    hedged = [r for r in executed if r.get("hedged")]
    reasons = Counter(str(r.get("reason")) for r in results if not r.get("executed") and r.get("reason"))
    total = round(sum(float(r.get("pnl", 0.0)) for r in executed), 6)
    total_gross = round(sum(float(r.get("gross_pnl", r.get("pnl", 0.0))) for r in executed), 6)
    total_fees = round(sum(float(r.get("total_fees", 0.0)) for r in executed), 6)
    return {
        "markets_total": len(results),
        "executed_trades": len(executed),
        "skipped_trades": len(results) - len(executed),
        "win_rate": len(wins) / len(executed) if executed else 0.0,
        "hedged_trades": len(hedged),
        "hedge_rate": len(hedged) / len(executed) if executed else 0.0,
        "total_pnl": total,
        "avg_pnl": round(total / len(executed), 6) if executed else 0.0,
        "total_gross_pnl": total_gross,
        "total_fees": total_fees,
        "avg_fees": round(total_fees / len(executed), 6) if executed else 0.0,
        "skip_reasons": dict(reasons),
    }


def result_to_dict(result: MarketBacktestResult) -> dict[str, Any]:
    return asdict(result)


def agreement(markets: list[MarketBacktestInput]) -> dict[str, Any]:
    comparable = 0
    agree = 0
    for m in markets:
        c = winner_for_reference(m, "chainlink")
        b = winner_for_reference(m, "binance")
        comparable += 1
        agree += int(c == b)
    return {"markets_compared": comparable, "agreement_rate": agree / comparable if comparable else 0.0}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--max-workers", type=int, default=12)
    parser.add_argument("--output", type=Path, default=Path("data/backtests/momentum_requested_variations_30d.json"))
    parser.add_argument("--market-input-cache", type=Path, default=Path("data/backtests/cache/btc_5m_market_inputs_30d.json"))
    args = parser.parse_args()

    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = end_ts - args.days * 86400
    config = BinanceLabConfig(assumed_fee_per_share=0.0, contract_trade_staleness_seconds=20)

    if args.market_input_cache.exists():
        print(f"Loading prepared market inputs cache {args.market_input_cache}")
        raw = json.loads(args.market_input_cache.read_text())
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
        markets_count = raw.get("market_count", len(market_inputs))
    else:
        print(f"Loading BTC 5m markets for last {args.days} days...")
        markets = load_btc_5m_markets(start_ts, end_ts)
        markets_count = len(markets)
        print(f"Loaded {markets_count} markets")
        print("Fetching Polymarket trades + Binance 1s candles market-by-market...")
        market_inputs = build_market_inputs(markets, max_workers=args.max_workers)
        print(f"Prepared {len(market_inputs)} market inputs")
        args.market_input_cache.parent.mkdir(parents=True, exist_ok=True)
        args.market_input_cache.write_text(json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "days": args.days,
            "market_count": markets_count,
            "market_inputs": [
                {
                    **{k: v for k, v in asdict(m).items() if k not in {"candles", "up_points", "down_points"}},
                    "candles": [asdict(c) for c in m.candles],
                    "up_points": [asdict(p) for p in m.up_points],
                    "down_points": [asdict(p) for p in m.down_points],
                }
                for m in market_inputs
            ],
        }))

    hedge_runs: dict[str, Any] = {}
    for min_consensus in (5, 4, 3):
        run_key = f"first_consensus_{min_consensus}_of_5_then_hedge_10c"
        hedge_runs[run_key] = {
            "strategy": {
                "windows": list(WINDOWS),
                "entry_rule": "first_consensus",
                "min_consensus": min_consensus,
                "hedge_rule": "if original-side consensus weakens below threshold, buy opposite side if entry+opposite <= 0.90",
                "hedge_profit_buffer": 0.10,
                "price_cap": None,
                "volatility_filter": False,
            }
        }
        for reference in ("chainlink", "binance"):
            results = [backtest_first_consensus_with_optional_hedge(m, min_consensus, 0.10, reference, config) for m in market_inputs]
            hedge_runs[run_key][f"{reference}_reference"] = summarize_dict_results(results)
            hedge_runs[run_key][f"{reference}_top_wins"] = sorted([r for r in results if r.get("executed")], key=lambda r: r["pnl"], reverse=True)[:5]
            hedge_runs[run_key][f"{reference}_top_losses"] = sorted([r for r in results if r.get("executed")], key=lambda r: r["pnl"])[:5]

    fixed_runs: dict[str, Any] = {}
    for min_consensus in (5, 4, 3):
        for entry_seconds in (5, 15, 30):
            run_key = f"fixed_entry_{entry_seconds}s_{min_consensus}_of_5"
            spec = StrategySpec(
                name=f"momentum_consensus_07_fixed_{entry_seconds}s_{min_consensus}_of_5",
                family="momentum_consensus",
                windows=WINDOWS,
                entry_seconds_before_close=entry_seconds,
                min_consensus=min_consensus,
                price_cap=None,
            )
            fixed_runs[run_key] = {
                "strategy": {
                    **spec.to_dict(),
                    "volatility_filter": False,
                    "entry_rule": f"fixed_{entry_seconds}s_before_close",
                }
            }
            for reference in ("chainlink", "binance"):
                results = [backtest_fixed_entry_no_risk_filter(m, min_consensus, entry_seconds, reference, config) for m in market_inputs]
                fixed_runs[run_key][f"{reference}_reference"] = summarize_results(results)
                fixed_runs[run_key][f"{reference}_top_wins"] = [result_to_dict(r) for r in sorted([r for r in results if r.executed], key=lambda r: r.pnl, reverse=True)[:5]]
                fixed_runs[run_key][f"{reference}_top_losses"] = [result_to_dict(r) for r in sorted([r for r in results if r.executed], key=lambda r: r.pnl)[:5]]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": args.days,
        "market_count": markets_count,
        "prepared_market_count": len(market_inputs),
        "source_model": {
            "signal": "Binance BTCUSDT 1-second candles",
            "entry_price_proxy": "Polymarket Data API trade prints by conditionId/outcome",
            "actual_reference": "Chainlink/Polymarket Gamma eventMetadata finalPrice vs priceToBeat",
            "diagnostic_reference": "Binance market end close vs start close",
        },
        "chainlink_vs_binance_winner_agreement": agreement(market_inputs),
        "hedge_variations": hedge_runs,
        "fixed_entry_variations": fixed_runs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {args.output}")
    print(json.dumps({
        "generated_at": payload["generated_at"],
        "days": payload["days"],
        "market_count": payload["market_count"],
        "prepared_market_count": payload["prepared_market_count"],
        "chainlink_vs_binance_winner_agreement": payload["chainlink_vs_binance_winner_agreement"],
        "hedge_variations": {k: {ref: v[ref] for ref in ("chainlink_reference", "binance_reference")} for k, v in hedge_runs.items()},
        "fixed_entry_variations": {k: {ref: v[ref] for ref in ("chainlink_reference", "binance_reference")} for k, v in fixed_runs.items()},
    }, indent=2))


if __name__ == "__main__":
    main()
