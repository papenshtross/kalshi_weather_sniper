#!/usr/bin/env python3
"""Backtest Crypto_Arbitrage_v1 against real observed Polymarket wallet fills.

This is an execution-realistic proxy: use the target wallet's public Polymarket
trade fills as the set of real executable prices, pair opposite outcomes only
when they were bought within a tight decision window, and apply the same edge +
short-horizon fair-value gate used by the live runner. It intentionally rejects
asynchronous inventory accumulation because that is directional trading, not a
simultaneous YES/NO arbitrage.
"""
from __future__ import annotations

import argparse
import json
import math
import time
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from pathlib import Path
from statistics import mean
from typing import Any

from polybot.crypto.fair_price import CryptoFairPriceModel, fair_edge_accepts_pair

DEFAULT_WALLET = "0x04b6d7e930cf9e493c5e6ef24b496294f95594c8"


def fetch_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def get_trades(wallet: str, max_rows: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for offset in range(0, min(max_rows, 5000) + 1, 500):
        url = f"https://data-api.polymarket.com/trades?user={wallet}&limit=500&offset={offset}"
        rows = fetch_json(url)
        if not rows:
            break
        out.extend(rows)
        if len(rows) < 500 or len(out) >= max_rows:
            break
    return out[:max_rows]


def slug_start_ts(slug: str) -> int | None:
    try:
        tail = slug.rsplit("-", 1)[1]
        return int(tail) if len(tail) == 10 else None
    except Exception:
        return None


def binance_symbol(slug: str) -> str:
    if slug.startswith("eth-updown"):
        return "ETHUSDT"
    if slug.startswith("sol-updown"):
        return "SOLUSDT"
    return "BTCUSDT"


def binance_1s_prices(symbol: str, start_ts: int, seconds: int, cache: dict[tuple[str, int, int], list[float]]) -> list[float]:
    seconds = max(1, min(1000, int(seconds)))
    key = (symbol, int(start_ts), seconds)
    if key in cache:
        return cache[key]
    url = "https://api.binance.com/api/v3/klines?" + urllib.parse.urlencode(
        {"symbol": symbol, "interval": "1s", "startTime": int(start_ts) * 1000, "limit": seconds}
    )
    try:
        rows = fetch_json(url)
        prices = [float(r[4]) for r in rows if float(r[4]) > 0]
    except Exception:
        prices = []
    cache[key] = prices
    return prices


def binance_price(symbol: str, ts: int, cache: dict[tuple[str, int, int], list[float]]) -> float | None:
    prices = binance_1s_prices(symbol, ts, 1, cache)
    return prices[0] if prices else None


def pair_near_simultaneous(rows: list[dict[str, Any]], max_gap_seconds: int) -> tuple[list[dict[str, Any]], float]:
    """FIFO pair opposite fills only if timestamps are close enough."""
    queues: dict[str, deque[dict[str, float]]] = {"Up": deque(), "Down": deque()}
    pairs: list[dict[str, Any]] = []
    unmatched_notional = 0.0
    for r in sorted(rows, key=lambda x: int(x.get("timestamp") or 0)):
        out = str(r.get("outcome"))
        if out not in queues:
            continue
        opp = "Down" if out == "Up" else "Up"
        size = float(r.get("size") or 0)
        price = float(r.get("price") or 0)
        ts = int(r.get("timestamp") or 0)
        if size <= 0 or price <= 0 or ts <= 0:
            continue
        cur = {"size": size, "price": price, "ts": ts}
        while cur["size"] > 1e-9 and queues[opp]:
            old = queues[opp][0]
            if abs(cur["ts"] - old["ts"]) > max_gap_seconds:
                expired = queues[opp].popleft()
                unmatched_notional += expired["size"] * expired["price"]
                continue
            matched = min(cur["size"], old["size"])
            up = cur if out == "Up" else old
            down = old if out == "Up" else cur
            decision_ts = max(int(up["ts"]), int(down["ts"]))
            pairs.append({"size": matched, "up_price": float(up["price"]), "down_price": float(down["price"]), "decision_ts": decision_ts})
            cur["size"] -= matched
            old["size"] -= matched
            if old["size"] <= 1e-9:
                queues[opp].popleft()
        if cur["size"] > 1e-9:
            queues[out].append(cur)
    for q in queues.values():
        for r in q:
            unmatched_notional += r["size"] * r["price"]
    return pairs, unmatched_notional


def evaluate(
    wallet: str,
    max_rows: int,
    min_pair_edge: float,
    min_model_edge: float,
    fee_rate: float,
    max_leg_gap_seconds: int,
    vol_window_seconds: int,
    ewma_lambda: float,
    winsor_sigma: float,
    latency_buffer_seconds: float,
) -> dict[str, Any]:
    trades = [t for t in get_trades(wallet, max_rows) if str(t.get("side")) == "BUY"]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        slug = str(t.get("slug") or "")
        if slug_start_ts(slug) is None:
            continue
        grouped[slug].append(t)
    model = CryptoFairPriceModel(
        fallback_sigma=0.80,
        vol_floor=0.05,
        vol_cap=5.0,
        ewma_lambda=ewma_lambda,
        winsor_sigma=winsor_sigma,
        latency_buffer_seconds=latency_buffer_seconds,
    )
    px_cache: dict[tuple[str, int, int], list[float]] = {}
    evaluated = []
    unmatched_notional = 0.0
    paired_fills = 0
    for slug, rows in grouped.items():
        pairs, unmatched = pair_near_simultaneous(rows, max_leg_gap_seconds)
        unmatched_notional += unmatched
        if not pairs:
            continue
        start_ts = slug_start_ts(slug)
        timeframe = 900 if "-15m-" in slug else 300
        symbol = binance_symbol(slug)
        start_px = binance_price(symbol, int(start_ts), px_cache) if start_ts else None
        if not start_px:
            continue
        for pair in pairs:
            paired_fills += 1
            up_avg = float(pair["up_price"])
            dn_avg = float(pair["down_price"])
            size = float(pair["size"])
            avg_sum = up_avg + dn_avg
            fee = fee_rate * up_avg * (1 - up_avg) + fee_rate * dn_avg * (1 - dn_avg)
            pair_edge = 1.0 - avg_sum - fee
            trade_ts = int(pair["decision_ts"])
            cur_px = binance_price(symbol, trade_ts, px_cache)
            if not cur_px:
                continue
            seconds_left = max(0, int(start_ts) + timeframe - trade_ts)
            recent_start = max(0, trade_ts - vol_window_seconds + 1)
            recent = binance_1s_prices(symbol, recent_start, vol_window_seconds, px_cache)
            fair = model.price(start_price=start_px, current_price=cur_px, seconds_to_expiry=seconds_left, recent_prices=recent)
            model_ok = fair_edge_accepts_pair(yes_avg=up_avg, no_avg=dn_avg, fair_up=fair.fair_up, min_model_edge=min_model_edge)
            edge_ok = pair_edge >= min_pair_edge
            evaluated.append({
                "slug": slug,
                "size": size,
                "up_avg": up_avg,
                "down_avg": dn_avg,
                "avg_sum": avg_sum,
                "pair_edge_after_fee": pair_edge,
                "fair_up": fair.fair_up,
                "sigma": fair.sigma_annualized,
                "vol_observations": fair.vol_observations,
                "vol_source": fair.vol_source,
                "z_score": fair.z_score,
                "seconds_left": seconds_left,
                "decision_ts": trade_ts,
                "model_ok": model_ok,
                "edge_ok": edge_ok,
                "accepted": model_ok and edge_ok,
                "implied_pair_pnl": size * pair_edge,
            })
            time.sleep(0.01)
    accepted = [x for x in evaluated if x["accepted"]]
    positive_edge = [x for x in evaluated if x["pair_edge_after_fee"] > 0]
    threshold_edge = [x for x in evaluated if x["edge_ok"]]
    return {
        "strategy": "Crypto_Arbitrage_v1 observed-fill simultaneous-pair backtest",
        "wallet": wallet,
        "trades_loaded": len(trades),
        "markets_grouped": len(grouped),
        "paired_fills": paired_fills,
        "markets_evaluated": len({x["slug"] for x in evaluated}),
        "pairs_evaluated": len(evaluated),
        "accepted_pairs": len(accepted),
        "max_leg_gap_seconds": max_leg_gap_seconds,
        "vol_window_seconds": vol_window_seconds,
        "ewma_lambda": ewma_lambda,
        "winsor_sigma": winsor_sigma,
        "latency_buffer_seconds": latency_buffer_seconds,
        "min_pair_edge": min_pair_edge,
        "min_model_edge": min_model_edge,
        "fee_rate": fee_rate,
        "unmatched_notional_ignored": unmatched_notional,
        "all_positive_edge_pairs": len(positive_edge),
        "all_positive_edge_implied_pnl": sum(x["implied_pair_pnl"] for x in positive_edge),
        "threshold_edge_pairs": len(threshold_edge),
        "threshold_edge_implied_pnl": sum(x["implied_pair_pnl"] for x in threshold_edge),
        "accepted_implied_pnl": sum(x["implied_pair_pnl"] for x in accepted),
        "accepted_matched_shares": sum(x["size"] for x in accepted),
        "accepted_avg_edge": mean([x["pair_edge_after_fee"] for x in accepted]) if accepted else 0.0,
        "top_accepted": sorted(accepted, key=lambda x: x["implied_pair_pnl"], reverse=True)[:25],
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--wallet", default=DEFAULT_WALLET)
    p.add_argument("--max-rows", type=int, default=1500)
    p.add_argument("--min-pair-edge", type=float, default=0.003)
    p.add_argument("--min-model-edge", type=float, default=0.001)
    p.add_argument("--fee-rate", type=float, default=0.072)
    p.add_argument("--max-leg-gap-seconds", type=int, default=5)
    p.add_argument("--vol-window-seconds", type=int, default=180)
    p.add_argument("--ewma-lambda", type=float, default=0.94)
    p.add_argument("--winsor-sigma", type=float, default=6.0)
    p.add_argument("--latency-buffer-seconds", type=float, default=0.25)
    p.add_argument("--output", type=Path, default=Path("data/backtests/crypto_arbitrage_v1_observed_wallet.json"))
    args = p.parse_args()
    result = evaluate(
        args.wallet,
        args.max_rows,
        args.min_pair_edge,
        args.min_model_edge,
        args.fee_rate,
        args.max_leg_gap_seconds,
        args.vol_window_seconds,
        args.ewma_lambda,
        args.winsor_sigma,
        args.latency_buffer_seconds,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
