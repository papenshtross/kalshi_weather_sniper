#!/usr/bin/env python3
"""Backtest the crypto fair-price model on cached real BTC 5m market data."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any

from polybot.crypto.fair_price import CryptoFairPriceModel


def logloss(p: float, y: int) -> float:
    p = min(1 - 1e-9, max(1e-9, p))
    return -(y * math.log(p) + (1 - y) * math.log(1 - p))


def load_markets(cache: Path, days: int, max_markets: int = 0) -> list[dict[str, Any]]:
    payload = json.loads(cache.read_text())
    markets = payload.get("market_inputs", [])
    if days > 0:
        max_end = max(int(m["end_ts"]) for m in markets)
        cutoff = max_end - days * 86400
        markets = [m for m in markets if int(m["end_ts"]) >= cutoff]
    if max_markets > 0:
        markets = markets[-max_markets:]
    return markets


def evaluate(cache: Path, days: int, eval_seconds: list[int], vol_window: int, max_markets: int = 0, price_source: str = "binance") -> dict[str, Any]:
    model = CryptoFairPriceModel(fallback_sigma=0.80, vol_floor=0.05, vol_cap=5.0)
    rows = []
    markets = load_markets(cache, days, max_markets)
    use_settlement = price_source == "settlement"
    for m in markets:
        # Live parity defaults to Binance 1s start/end because the deployed gate
        # uses Binance start-of-window and websocket reference prices. Settlement
        # mode remains available for oracle/Polymarket-resolution diagnostics.
        start = float((m.get("price_to_beat") if use_settlement else m.get("binance_start_price")) or m.get("binance_start_price") or 0)
        final = float((m.get("final_price") if use_settlement else m.get("binance_end_price")) or m.get("binance_end_price") or 0)
        if start <= 0 or final <= 0:
            continue
        y = 1 if final > start else 0
        candles = m.get("candles") or []
        by_ts = {int(c["ts"]): float(c["close"]) for c in candles if c.get("close")}
        for sec_left in eval_seconds:
            ts = int(m["end_ts"]) - int(sec_left)
            px = by_ts.get(ts)
            if not px or px <= 0:
                continue
            hist = [float(c["close"]) for c in candles if int(c["ts"]) <= ts][-vol_window:]
            snap = model.price(start_price=start, current_price=px, seconds_to_expiry=sec_left, recent_prices=hist)
            rows.append({"sec_left": sec_left, "p": snap.fair_up, "y": y, "sigma": snap.sigma_annualized})
    by_sec: dict[int, dict[str, Any]] = {}
    for sec in eval_seconds:
        r = [x for x in rows if x["sec_left"] == sec]
        if not r:
            continue
        brier = mean((x["p"] - x["y"]) ** 2 for x in r)
        ll = mean(logloss(x["p"], x["y"]) for x in r)
        acc = mean((x["p"] >= 0.5) == bool(x["y"]) for x in r)
        by_sec[sec] = {"n": len(r), "brier": brier, "logloss": ll, "directional_accuracy": acc, "avg_sigma": mean(x["sigma"] for x in r)}
    return {
        "strategy": "Crypto_Arbitrage_v1 fair model validation",
        "cache": str(cache),
        "days": days,
        "markets": len(markets),
        "observations": len(rows),
        "eval_seconds": eval_seconds,
        "vol_window_seconds": vol_window,
        "price_source": price_source,
        "metrics_by_seconds_left": by_sec,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--cache", type=Path, default=Path("data/backtests/cache/btc_5m_market_inputs_30d.json"))
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--eval-seconds", default="240,180,120,90,60,45,30,15,5")
    p.add_argument("--vol-window", type=int, default=180)
    p.add_argument("--max-markets", type=int, default=0)
    p.add_argument("--price-source", choices=["binance", "settlement"], default="binance")
    p.add_argument("--output", type=Path, default=Path("data/backtests/crypto_arbitrage_v1_fair_model.json"))
    args = p.parse_args()
    eval_seconds = [int(x) for x in args.eval_seconds.split(",") if x.strip()]
    result = evaluate(args.cache, args.days, eval_seconds, args.vol_window, args.max_markets, args.price_source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
