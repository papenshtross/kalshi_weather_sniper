#!/usr/bin/env python3
"""Strict-ish BTC 5m Polymarket strategy search.

This is intentionally more conservative than the older momentum lab:
- one entry decision per market
- actual Polymarket/Chainlink label: final_price >= price_to_beat
- side-specific cached Polymarket trade print with freshness guard
- conservative executable ask proxy = latest trade + ask_buffer
- dynamic Polymarket crypto taker fee: 0.072 * p * (1-p)
- fixed $1 stake accounting: shares = stake / ask
- optional same-share hedge with conservative opposite ask proxy + dynamic fees
- parameter sweeps include stress buffers; headline candidates must survive fees/buffers

Limitation: the 30d cache does not contain historical CLOB ask depth. Results are
"conservative trade-print ask proxy", not exact FOK orderbook replay.
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from polybot.backtest.binance_strategy_lab import CandlePoint, MarketBacktestInput, PricePoint, SideSignal
from scripts.backtest_momentum_requested_variations import consensus_score_from_map, winner_for_reference

WINDOW_SETS: dict[str, tuple[int, ...]] = {
    # Start with families repeatedly found strongest in prior 30d sweeps.
    "momo_5_10_20_40_80": (5, 10, 20, 40, 80),
    "momo_3_8_21_55": (3, 8, 21, 55),
    "momo_5_15_30_60": (5, 15, 30, 60),
}


@dataclass(frozen=True)
class Candidate:
    name: str
    windows: tuple[int, ...]
    min_consensus: int
    entry_rule: str  # first_consensus or fixed
    entry_seconds_before_close: int | None
    ask_buffer: float
    max_trade_age: int
    max_ask: float
    hedge_buffer: float | None
    hedge_trigger_consensus: int | None
    fee_rate: float = 0.072
    stake_usd: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["windows"] = list(self.windows)
        return d


def load_market_inputs(path: Path) -> tuple[dict[str, Any], list[MarketBacktestInput]]:
    raw = json.loads(path.read_text())
    markets = [
        MarketBacktestInput(
            market_id=item["market_id"],
            market_slug=item["market_slug"],
            start_ts=int(item["start_ts"]),
            end_ts=int(item["end_ts"]),
            price_to_beat=float(item["price_to_beat"]),
            final_price=float(item["final_price"]),
            binance_start_price=float(item["binance_start_price"]),
            binance_end_price=float(item["binance_end_price"]),
            candles=[CandlePoint(**c) for c in item["candles"]],
            up_points=[PricePoint(**p) for p in item["up_points"]],
            down_points=[PricePoint(**p) for p in item["down_points"]],
        )
        for item in raw["market_inputs"]
    ]
    meta = {k: raw.get(k) for k in ("generated_at", "days", "market_count")}
    return meta, markets


def point_at_or_before(points: list[PricePoint], ts: int) -> PricePoint | None:
    latest = None
    for p in points:
        if p.ts <= ts:
            latest = p
        else:
            break
    return latest


def conservative_ask(points: list[PricePoint], ts: int, max_age: int, buffer: float) -> tuple[float | None, int | None, str | None]:
    p = point_at_or_before(points, ts)
    if p is None:
        return None, None, "missing_price"
    age = ts - p.ts
    if age > max_age:
        return None, p.ts, "stale_price"
    ask = min(0.99, max(0.01, float(p.price) + buffer))
    return ask, p.ts, None


def fee_per_share(price: float, fee_rate: float) -> float:
    if not (0 <= price <= 1) or fee_rate <= 0:
        return 0.0
    return fee_rate * price * (1.0 - price)


def max_hedge_ask(entry_ask: float, hedge_buffer: float, fee_rate: float) -> float:
    entry_fee = fee_per_share(entry_ask, fee_rate)
    budget = 1.0 - entry_ask - entry_fee - hedge_buffer
    if budget <= 0:
        return 0.0
    lo, hi = 0.0, min(0.99, budget)
    for _ in range(64):
        mid = (lo + hi) / 2
        if mid + fee_per_share(mid, fee_rate) <= budget:
            lo = mid
        else:
            hi = mid
    return lo


def side_from_score(score: int, min_consensus: int) -> SideSignal:
    if score >= min_consensus:
        return SideSignal.UP
    if score <= -min_consensus:
        return SideSignal.DOWN
    return SideSignal.SKIP


def trade_pnl_usd(side: SideSignal, winner: SideSignal, ask: float, stake_usd: float, fee_rate: float) -> tuple[float, float, float]:
    shares = stake_usd / ask
    fee = shares * fee_per_share(ask, fee_rate)
    payout = shares if side == winner else 0.0
    pnl = payout - stake_usd - fee
    return pnl, shares, fee


def replay_candidate(m: MarketBacktestInput, c: Candidate, reference: str) -> dict[str, Any]:
    close_by_ts = {x.ts: x.close for x in m.candles}
    max_window = max(c.windows)
    first_ts = m.start_ts + max_window
    last_ts = m.end_ts - 1
    if c.entry_rule == "fixed":
        if c.entry_seconds_before_close is None:
            return {"executed": False, "reason": "bad_candidate", "pnl": 0.0}
        candidate_ts_iter: Iterable[int] = (m.end_ts - c.entry_seconds_before_close,)
    else:
        candidate_ts_iter = range(first_ts, last_ts + 1)

    entry: dict[str, Any] | None = None
    for ts in candidate_ts_iter:
        if ts < first_ts or ts > last_ts:
            continue
        score = consensus_score_from_map(close_by_ts, ts, c.windows)
        if score is None:
            continue
        side = side_from_score(score, c.min_consensus)
        if side == SideSignal.SKIP:
            if c.entry_rule == "fixed":
                return {"executed": False, "reason": "no_consensus", "pnl": 0.0}
            continue
        points = m.up_points if side == SideSignal.UP else m.down_points
        ask, price_ts, reason = conservative_ask(points, ts, c.max_trade_age, c.ask_buffer)
        if ask is None:
            if c.entry_rule == "fixed":
                return {"executed": False, "reason": reason or "no_price", "pnl": 0.0}
            continue
        if ask > c.max_ask:
            if c.entry_rule == "fixed":
                return {"executed": False, "reason": "ask_above_cap", "pnl": 0.0}
            continue
        entry = {"ts": ts, "side": side, "score": score, "ask": ask, "price_ts": price_ts}
        break

    if entry is None:
        return {"executed": False, "reason": "no_entry", "pnl": 0.0}

    winner = winner_for_reference(m, reference)
    entry_side: SideSignal = entry["side"]
    entry_ask = float(entry["ask"])
    hedge_ask = None
    hedge_ts = None
    hedge_score = None
    hedge_fee = 0.0
    hedged = False

    if c.hedge_buffer is not None and c.hedge_trigger_consensus is not None:
        target = max_hedge_ask(entry_ask, c.hedge_buffer, c.fee_rate)
        opp_side = SideSignal.DOWN if entry_side == SideSignal.UP else SideSignal.UP
        opp_points = m.down_points if entry_side == SideSignal.UP else m.up_points
        seeking = False
        for ts in range(int(entry["ts"]) + 1, last_ts + 1):
            score = consensus_score_from_map(close_by_ts, ts, c.windows)
            if score is None:
                continue
            weakened = score <= c.hedge_trigger_consensus if entry_side == SideSignal.UP else score >= -c.hedge_trigger_consensus
            if weakened:
                seeking = True
            if not seeking:
                continue
            ask, _, _ = conservative_ask(opp_points, ts, c.max_trade_age, c.ask_buffer)
            if ask is not None and ask <= target:
                hedge_ask = ask
                hedge_ts = ts
                hedge_score = score
                hedged = True
                break

    if hedged and hedge_ask is not None:
        shares = c.stake_usd / entry_ask
        entry_fee = shares * fee_per_share(entry_ask, c.fee_rate)
        # Same-share hedge to lock binary payout. In live, hedge may need to size up for $1 min notional;
        # this reports same-share economic PnL and flags historical depth as unavailable.
        hedge_fee = shares * fee_per_share(hedge_ask, c.fee_rate)
        pnl = shares * (1.0 - entry_ask - hedge_ask) - entry_fee - hedge_fee
    else:
        pnl, shares, entry_fee = trade_pnl_usd(entry_side, winner, entry_ask, c.stake_usd, c.fee_rate)

    return {
        "executed": True,
        "market_slug": m.market_slug,
        "side": entry_side.value,
        "winner": winner.value,
        "entry_ts": entry["ts"],
        "entry_ask": entry_ask,
        "entry_score": entry["score"],
        "entry_price_ts": entry["price_ts"],
        "shares": shares,
        "hedged": hedged,
        "hedge_ask": hedge_ask,
        "hedge_ts": hedge_ts,
        "hedge_score": hedge_score,
        "entry_fee": entry_fee,
        "hedge_fee": hedge_fee,
        "pnl": pnl,
        "reference": reference,
    }


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    executed = [r for r in results if r.get("executed")]
    pnl = [float(r.get("pnl", 0.0)) for r in executed]
    wins = [x for x in pnl if x > 0]
    hedged = [r for r in executed if r.get("hedged")]
    reasons = Counter(str(r.get("reason")) for r in results if not r.get("executed"))
    # market/day-ish drawdown in market order
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in results:
        eq += float(r.get("pnl", 0.0)) if r.get("executed") else 0.0
        peak = max(peak, eq)
        max_dd = min(max_dd, eq - peak)
    return {
        "markets_total": len(results),
        "executed_trades": len(executed),
        "coverage": len(executed) / len(results) if results else 0.0,
        "win_rate": len(wins) / len(executed) if executed else 0.0,
        "hedged_trades": len(hedged),
        "hedge_rate": len(hedged) / len(executed) if executed else 0.0,
        "total_pnl_usd_per_1usd_stake": round(sum(pnl), 6),
        "avg_pnl_usd_per_trade": round(mean(pnl), 6) if pnl else 0.0,
        "median_pnl_usd_per_trade": round(sorted(pnl)[len(pnl)//2], 6) if pnl else 0.0,
        "max_drawdown_usd_per_1usd_stake": round(max_dd, 6),
        "skip_reasons": dict(reasons),
    }


def candidate_grid() -> list[Candidate]:
    candidates: list[Candidate] = []
    # Focused iteration around historically strongest dynamic consensus + hedge,
    # plus a few fixed-entry baselines. This keeps strict iterations fast enough
    # to run repeatedly while we adjust execution assumptions.
    ask_buffers = [0.00, 0.01, 0.02, 0.03]
    max_asks = [0.95]
    ages = [20]
    family = "momo_5_10_20_40_80"
    windows = WINDOW_SETS[family]
    min_consensus = len(windows)
    for ask_buffer in ask_buffers:
        for max_ask in max_asks:
            for age in ages:
                candidates.append(Candidate(
                    name=f"first_{family}_5of5_buf{int(ask_buffer*100)}c_cap{int(max_ask*100)}_age{age}",
                    windows=windows,
                    min_consensus=min_consensus,
                    entry_rule="first_consensus",
                    entry_seconds_before_close=None,
                    ask_buffer=ask_buffer,
                    max_trade_age=age,
                    max_ask=max_ask,
                    hedge_buffer=None,
                    hedge_trigger_consensus=None,
                ))
                for hedge_buffer in [0.00, 0.01, 0.03, 0.05, 0.10, 0.25]:
                    for trigger in [3]:
                        candidates.append(Candidate(
                            name=f"first_{family}_5of5_buf{int(ask_buffer*100)}c_cap{int(max_ask*100)}_age{age}_hedge{int(hedge_buffer*100)}c_trig{trigger}",
                            windows=windows,
                            min_consensus=min_consensus,
                            entry_rule="first_consensus",
                            entry_seconds_before_close=None,
                            ask_buffer=ask_buffer,
                            max_trade_age=age,
                            max_ask=max_ask,
                            hedge_buffer=hedge_buffer,
                            hedge_trigger_consensus=trigger,
                        ))
    for entry_s in [3, 5, 10, 15]:
        for ask_buffer in ask_buffers:
            for max_ask in max_asks:
                candidates.append(Candidate(
                    name=f"fixed{entry_s}s_{family}_5of5_buf{int(ask_buffer*100)}c_cap{int(max_ask*100)}",
                    windows=windows,
                    min_consensus=min_consensus,
                    entry_rule="fixed",
                    entry_seconds_before_close=entry_s,
                    ask_buffer=ask_buffer,
                    max_trade_age=20,
                    max_ask=max_ask,
                    hedge_buffer=None,
                    hedge_trigger_consensus=None,
                ))
    return candidates


def evaluate_candidate(markets: list[MarketBacktestInput], c: Candidate) -> dict[str, Any]:
    chain = [replay_candidate(m, c, "chainlink") for m in markets]
    # Reuse decisions for binance by replaying; small enough.
    bnb = [replay_candidate(m, c, "binance") for m in markets]
    cs = summarize(chain)
    bs = summarize(bnb)
    # Strict acceptance: positive under actual Chainlink/Polymarket and diagnostic Binance transferability.
    accepted = (
        cs["executed_trades"] >= 500
        and cs["total_pnl_usd_per_1usd_stake"] >= 100.0
        and cs["avg_pnl_usd_per_trade"] >= 0.02
        and cs["max_drawdown_usd_per_1usd_stake"] > -150.0
        and bs["total_pnl_usd_per_1usd_stake"] > 0.0
    )
    return {
        "candidate": c.to_dict(),
        "chainlink_reference": cs,
        "binance_reference": bs,
        "accepted_strict_gate": accepted,
        "score": cs["total_pnl_usd_per_1usd_stake"] + 0.25 * bs["total_pnl_usd_per_1usd_stake"] - max(0, -cs["max_drawdown_usd_per_1usd_stake"] - 100),
        "chainlink_top_wins": sorted([r for r in chain if r.get("executed")], key=lambda r: r["pnl"], reverse=True)[:5],
        "chainlink_top_losses": sorted([r for r in chain if r.get("executed")], key=lambda r: r["pnl"])[:5],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market-input-cache", type=Path, default=Path("data/backtests/cache/btc_5m_market_inputs_30d.json"))
    ap.add_argument("--output", type=Path, default=Path("data/backtests/strict_polymarket_strategy_search_30d.json"))
    ap.add_argument("--top", type=int, default=50)
    args = ap.parse_args()

    meta, markets = load_market_inputs(args.market_input_cache)
    candidates = candidate_grid()
    print(f"Loaded {len(markets)} markets. Evaluating {len(candidates)} candidates...")
    evaluated = []
    for i, c in enumerate(candidates, start=1):
        res = evaluate_candidate(markets, c)
        evaluated.append(res)
        if i % 100 == 0:
            best = max(evaluated, key=lambda x: x["chainlink_reference"]["total_pnl_usd_per_1usd_stake"])
            print(f"{i}/{len(candidates)} best={best['candidate']['name']} pnl={best['chainlink_reference']['total_pnl_usd_per_1usd_stake']} accepted={sum(x['accepted_strict_gate'] for x in evaluated)}")
    ranked = sorted(evaluated, key=lambda x: x["score"], reverse=True)
    accepted = [x for x in ranked if x["accepted_strict_gate"]]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_cache": str(args.market_input_cache),
        "source_meta": meta,
        "prepared_market_count": len(markets),
        "limitation": "No 30d historical CLOB ask-depth exists in cache. Entry/hedge prices are conservative latest-trade ask proxies, not exact FOK replay.",
        "strict_gate": {
            "min_executed_trades": 500,
            "min_chainlink_pnl_usd_per_1usd_stake": 100.0,
            "min_avg_pnl_usd_per_trade": 0.02,
            "chainlink_max_drawdown_must_be_above": -150.0,
            "binance_reference_pnl_must_be_positive": True,
        },
        "candidate_count": len(candidates),
        "accepted_count": len(accepted),
        "accepted": accepted[:args.top],
        "top_ranked": ranked[:args.top],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print(json.dumps({
        "output": str(args.output),
        "candidate_count": len(candidates),
        "accepted_count": len(accepted),
        "best": ranked[0]["candidate"]["name"],
        "best_chainlink": ranked[0]["chainlink_reference"],
        "best_binance": ranked[0]["binance_reference"],
        "best_accepted": ranked[0]["accepted_strict_gate"],
    }, indent=2))


if __name__ == "__main__":
    main()
