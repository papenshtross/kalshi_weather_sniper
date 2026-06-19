from __future__ import annotations

import argparse
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from polybot.backtest.binance_multiframe_trend_5m import (
    BinanceTrendConfig,
    MarketBacktestInput,
    PricePoint,
    backtest_market,
    summarize_results,
)

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
GOLDSKY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
POLYMARKET_DATA_TRADES_URL = "https://data-api.polymarket.com/trades"
USDC = "0"

POLY_SESSION = requests.Session()
BINANCE_SESSION = requests.Session()


def parse_iso_timestamp(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def iter_market_start_timestamps(start_ts: int, end_ts: int):
    current = start_ts - (start_ts % 300)
    while current <= end_ts:
        yield current
        current += 300


def batched(values: list[str], size: int):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def load_btc_5m_markets(start_ts: int, end_ts: int) -> list[dict[str, Any]]:
    slugs = [f"btc-updown-5m-{ts}" for ts in iter_market_start_timestamps(start_ts, end_ts)]
    session = requests.Session()
    markets: list[dict[str, Any]] = []
    for slug_batch in batched(slugs, 100):
        params: list[tuple[str, str]] = [("slug", slug) for slug in slug_batch]
        params.extend([("closed", "true"), ("limit", str(len(slug_batch)))])
        response = session.get(GAMMA_EVENTS_URL, params=params, timeout=60)
        response.raise_for_status()
        page = response.json()
        for event in page:
            slug = str(event.get("slug") or "")
            if not slug.startswith("btc-updown-5m-"):
                continue
            event_meta = event.get("eventMetadata") or {}
            markets_data = event.get("markets") or []
            if not markets_data:
                continue
            market = markets_data[0]
            outcomes = market.get("outcomes") or []
            token_ids = market.get("clobTokenIds") or []
            outcome_prices = market.get("outcomePrices") or []
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)
            if isinstance(outcome_prices, str):
                outcome_prices = json.loads(outcome_prices)
            if len(token_ids) < 2 or len(outcomes) < 2:
                continue
            start_market_ts = int(slug.rsplit("-", 1)[-1])
            end_market_ts = parse_iso_timestamp(event.get("endDate")) or start_market_ts + 300
            if end_market_ts < start_ts or start_market_ts > end_ts:
                continue
            try:
                up_index = outcomes.index("Up")
                down_index = outcomes.index("Down")
            except ValueError:
                continue
            final_price = event_meta.get("finalPrice")
            price_to_beat = event_meta.get("priceToBeat")
            if final_price is None or price_to_beat is None:
                continue
            markets.append(
                {
                    "event_id": str(event.get("id")),
                    "market_id": str(market.get("id")),
                    "condition_id": str(market.get("conditionId")),
                    "slug": slug,
                    "title": event.get("title") or market.get("question") or slug,
                    "start_ts": start_market_ts,
                    "end_ts": end_market_ts,
                    "price_to_beat": float(price_to_beat),
                    "final_price": float(final_price),
                    "up_token": str(token_ids[up_index]),
                    "down_token": str(token_ids[down_index]),
                    "outcome_prices": outcome_prices,
                }
            )
        time.sleep(0.1)
    markets.sort(key=lambda row: row["start_ts"])
    return markets


def fetch_market_trade_points(condition_id: str) -> tuple[list[PricePoint], list[PricePoint]]:
    response = None
    for attempt in range(6):
        response = POLY_SESSION.get(
            POLYMARKET_DATA_TRADES_URL,
            params={"market": condition_id, "limit": 500},
            timeout=30,
        )
        if response.status_code == 429:
            time.sleep(min(2 ** attempt, 20))
            continue
        response.raise_for_status()
        break
    if response is None:
        raise RuntimeError(f"failed to fetch trades for {condition_id}")
    rows = response.json()
    up_points: list[PricePoint] = []
    down_points: list[PricePoint] = []
    for row in rows:
        outcome = str(row.get("outcome") or "")
        point = PricePoint(ts=int(row["timestamp"]), price=float(row["price"]))
        if outcome == "Up":
            up_points.append(point)
        elif outcome == "Down":
            down_points.append(point)
    up_points.sort(key=lambda point: point.ts)
    down_points.sort(key=lambda point: point.ts)
    return up_points, down_points


def fetch_binance_points_for_market(start_ts: int, end_ts: int) -> list[PricePoint]:
    response = None
    for attempt in range(6):
        response = BINANCE_SESSION.get(
            BINANCE_KLINES_URL,
            params={
                "symbol": "BTCUSDT",
                "interval": "1s",
                "startTime": start_ts * 1000,
                "endTime": (end_ts + 1) * 1000,
                "limit": 1000,
            },
            timeout=30,
        )
        if response.status_code == 429:
            time.sleep(min(2 ** attempt, 20))
            continue
        response.raise_for_status()
        break
    if response is None:
        raise RuntimeError(f"failed to fetch Binance points for {start_ts}-{end_ts}")
    klines = response.json()
    return [PricePoint(ts=int(kline[0]) // 1000, price=float(kline[4])) for kline in klines]


def build_market_inputs(markets: list[dict[str, Any]], max_workers: int = 16) -> list[MarketBacktestInput]:
    market_inputs: list[MarketBacktestInput] = []

    def build_one(market: dict[str, Any]) -> MarketBacktestInput | None:
        up_points, down_points = fetch_market_trade_points(market["condition_id"])
        if not up_points or not down_points:
            return None
        binance_points = fetch_binance_points_for_market(market["start_ts"], market["end_ts"])
        if not binance_points:
            return None
        return MarketBacktestInput(
            market_id=market["market_id"],
            market_slug=market["slug"],
            start_ts=market["start_ts"],
            end_ts=market["end_ts"],
            price_to_beat=market["price_to_beat"],
            final_price=market["final_price"],
            binance_start_price=binance_points[0].price,
            binance_end_price=binance_points[-1].price,
            binance_points=binance_points,
            up_points=up_points,
            down_points=down_points,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(build_one, market): market["slug"] for market in markets}
        for future in as_completed(futures):
            slug = futures[future]
            try:
                item = future.result()
            except Exception as exc:
                print(f"warning: skipped {slug}: {exc}")
                continue
            if item is not None:
                market_inputs.append(item)
    market_inputs.sort(key=lambda item: item.start_ts)
    return market_inputs


def evaluate_configs(market_inputs: list[MarketBacktestInput], configs: list[BinanceTrendConfig]) -> dict[str, Any]:
    runs: list[dict[str, Any]] = []
    best_run: dict[str, Any] | None = None
    for config in configs:
        chainlink_results = [backtest_market(market, config, reference="chainlink") for market in market_inputs]
        binance_results = [backtest_market(market, config, reference="binance") for market in market_inputs]
        chainlink_summary = summarize_results(chainlink_results)
        binance_summary = summarize_results(binance_results)
        run = {
            "config": asdict(config),
            "chainlink_reference": chainlink_summary,
            "binance_reference": binance_summary,
            "top_chainlink_wins": [asdict(result) for result in sorted(chainlink_results, key=lambda item: item.net_pnl, reverse=True)[:10]],
            "top_chainlink_losses": [asdict(result) for result in sorted(chainlink_results, key=lambda item: item.net_pnl)[:10]],
        }
        runs.append(run)
        if best_run is None or run["chainlink_reference"]["total_net_pnl"] > best_run["chainlink_reference"]["total_net_pnl"]:
            best_run = run
    return {"best_run": best_run, "runs": runs}


def default_config_grid() -> list[BinanceTrendConfig]:
    configs: list[BinanceTrendConfig] = []
    for entry_seconds in (5, 10, 15):
        for min_consensus in (2, 3, 4):
            for price_cap in (0.9, 0.93, 0.95, 0.97):
                configs.append(
                    BinanceTrendConfig(
                        entry_seconds_before_close=entry_seconds,
                        trend_windows=(5, 15, 30, 60),
                        min_consensus=min_consensus,
                        volatility_window_seconds=60,
                        price_cap=price_cap,
                        assumed_fee_per_share=0.0,
                    )
                )
    return configs


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest Binance multi-timeframe BTC trend strategy on Polymarket 5m markets")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--output", type=Path, default=Path("data/backtests/binance_multiframe_trend_5m_30d.json"))
    parser.add_argument("--max-workers", type=int, default=16)
    args = parser.parse_args()

    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = end_ts - (args.days * 86400)

    print(f"Loading BTC 5m markets for last {args.days} days...")
    markets = load_btc_5m_markets(start_ts, end_ts)
    print(f"Loaded {len(markets)} markets")

    print("Fetching Polymarket trade history + Binance 1s windows market-by-market...")
    market_inputs = build_market_inputs(markets, max_workers=args.max_workers)
    print(f"Prepared {len(market_inputs)} market inputs")

    configs = default_config_grid()
    print(f"Evaluating {len(configs)} config combinations...")
    evaluation = evaluate_configs(market_inputs, configs)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": args.days,
        "market_count": len(markets),
        "prepared_market_count": len(market_inputs),
        "strategy": {
            "name": "binance_multiframe_trend_5m",
            "trend_windows": [5, 15, 30, 60],
            "signal_source": "Binance BTCUSDT 1s klines",
            "contract_entry_source": "Polymarket Goldsky orderFilled events",
            "references": ["chainlink", "binance"],
        },
        "best_run": evaluation["best_run"],
        "runs": evaluation["runs"],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {args.output}")
    if evaluation["best_run"]:
        print(json.dumps(evaluation["best_run"], indent=2))


if __name__ == "__main__":
    main()
