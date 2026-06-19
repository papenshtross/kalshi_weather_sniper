from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from polybot.backtest.binance_strategy_lab import (
    BinanceLabConfig,
    CandlePoint,
    MarketBacktestInput,
    MarketBacktestResult,
    PricePoint,
    StrategySpec,
    backtest_market,
    build_strategy_universe,
    replay_live_decision,
    summarize_results,
)

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
POLYMARKET_DATA_TRADES_URL = "https://data-api.polymarket.com/trades"

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
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)
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
                    "market_id": str(market.get("id")),
                    "condition_id": str(market.get("conditionId")),
                    "slug": slug,
                    "start_ts": start_market_ts,
                    "end_ts": end_market_ts,
                    "price_to_beat": float(price_to_beat),
                    "final_price": float(final_price),
                    "up_token": str(token_ids[up_index]),
                    "down_token": str(token_ids[down_index]),
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



def fetch_binance_candles_for_market(start_ts: int, end_ts: int, padding_seconds: int = 180) -> list[CandlePoint]:
    response = None
    padded_start = max(0, start_ts - padding_seconds)
    for attempt in range(6):
        response = BINANCE_SESSION.get(
            BINANCE_KLINES_URL,
            params={
                "symbol": "BTCUSDT",
                "interval": "1s",
                "startTime": padded_start * 1000,
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
        raise RuntimeError(f"failed to fetch Binance candles for {start_ts}-{end_ts}")
    klines = response.json()
    candles: list[CandlePoint] = []
    for kline in klines:
        candles.append(
            CandlePoint(
                ts=int(kline[0]) // 1000,
                open=float(kline[1]),
                high=float(kline[2]),
                low=float(kline[3]),
                close=float(kline[4]),
                volume=float(kline[5]),
                taker_buy_volume=float(kline[9]),
            )
        )
    return candles



def build_market_inputs(markets: list[dict[str, Any]], max_workers: int = 16) -> list[MarketBacktestInput]:
    market_inputs: list[MarketBacktestInput] = []

    def build_one(market: dict[str, Any]) -> MarketBacktestInput | None:
        up_points, down_points = fetch_market_trade_points(market["condition_id"])
        if not up_points or not down_points:
            return None
        candles = fetch_binance_candles_for_market(market["start_ts"], market["end_ts"])
        if len(candles) < 30:
            return None
        return MarketBacktestInput(
            market_id=market["market_id"],
            market_slug=market["slug"],
            start_ts=market["start_ts"],
            end_ts=market["end_ts"],
            price_to_beat=market["price_to_beat"],
            final_price=market["final_price"],
            binance_start_price=candles[0].close,
            binance_end_price=candles[-1].close,
            candles=candles,
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



def validate_strategy_parity(market_inputs: list[MarketBacktestInput], strategy: StrategySpec, config: BinanceLabConfig, sample_size: int = 25) -> dict[str, Any]:
    checked = 0
    mismatches: list[dict[str, Any]] = []
    checked_markets: list[dict[str, Any]] = []
    for market in market_inputs:
        if checked >= sample_size:
            break
        result = backtest_market(market, strategy, config=config, reference="chainlink")
        replay = replay_live_decision(market, strategy, config)
        checked += 1
        checked_markets.append(
            {
                "market_slug": market.market_slug,
                "entry_ts": result.entry_ts,
                "last_candle_ts_used": max(candle.ts for candle in market.candles if candle.ts <= result.entry_ts),
                "last_up_trade_ts_used": max((point.ts for point in market.up_points if point.ts <= result.entry_ts), default=None),
                "last_down_trade_ts_used": max((point.ts for point in market.down_points if point.ts <= result.entry_ts), default=None),
                "backtest_side": result.side,
                "replay_side": replay.side,
                "backtest_entry_price": result.entry_price,
                "replay_entry_price": replay.entry_price,
            }
        )
        if result.side != replay.side or result.entry_price != replay.entry_price or result.entry_ts != replay.entry_ts:
            mismatches.append(
                {
                    "market_slug": market.market_slug,
                    "backtest": {
                        "side": result.side,
                        "entry_ts": result.entry_ts,
                        "entry_price": result.entry_price,
                    },
                    "replay": {
                        "side": replay.side,
                        "entry_ts": replay.entry_ts,
                        "entry_price": replay.entry_price,
                    },
                }
            )
    return {
        "strategy": strategy.to_dict(),
        "checked_market_count": checked,
        "parity_ok": len(mismatches) == 0,
        "mismatch_count": len(mismatches),
        "sampled_markets": checked_markets,
        "mismatches": mismatches,
    }



def evaluate_strategies(market_inputs: list[MarketBacktestInput], strategies: list[StrategySpec], config: BinanceLabConfig) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for strategy in strategies:
        chainlink_results = [backtest_market(market, strategy, config=config, reference="chainlink") for market in market_inputs]
        binance_results = [backtest_market(market, strategy, config=config, reference="binance") for market in market_inputs]
        chainlink_summary = summarize_results(chainlink_results)
        binance_summary = summarize_results(binance_results)
        run = {
            "strategy": strategy.to_dict(),
            "chainlink_reference": chainlink_summary,
            "binance_reference": binance_summary,
            "top_chainlink_wins": [result_to_dict(result) for result in sorted(chainlink_results, key=lambda item: item.net_pnl, reverse=True)[:10]],
            "top_chainlink_losses": [result_to_dict(result) for result in sorted(chainlink_results, key=lambda item: item.net_pnl)[:10]],
        }
        runs.append(run)
    runs.sort(key=lambda item: (item["chainlink_reference"]["total_net_pnl"], item["chainlink_reference"]["win_rate"]), reverse=True)
    return runs



def result_to_dict(result: MarketBacktestResult) -> dict[str, Any]:
    return {
        "market_id": result.market_id,
        "market_slug": result.market_slug,
        "executed": result.executed,
        "side": result.side,
        "score": result.score,
        "entry_ts": result.entry_ts,
        "entry_price": result.entry_price,
        "payout": result.payout,
        "pnl": result.pnl,
        "net_pnl": result.net_pnl,
        "breakout_probability": result.breakout_probability,
        "opposite_implied_probability": result.opposite_implied_probability,
        "reference": result.reference,
        "reason": result.reason,
        "diagnostics": result.diagnostics,
    }



def main() -> None:
    parser = argparse.ArgumentParser(description="Run 50 Binance BTC 5m strategy backtests on Polymarket BTC 5m markets")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--output", type=Path, default=Path("data/backtests/binance_strategy_lab_30d.json"))
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--fee", type=float, default=0.0)
    parser.add_argument("--parity-sample-size", type=int, default=25)
    args = parser.parse_args()

    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = end_ts - (args.days * 86400)
    config = BinanceLabConfig(assumed_fee_per_share=args.fee)

    print(f"Loading BTC 5m markets for last {args.days} days...")
    markets = load_btc_5m_markets(start_ts, end_ts)
    print(f"Loaded {len(markets)} markets")

    print("Fetching Polymarket trades + Binance 1s candles market-by-market...")
    market_inputs = build_market_inputs(markets, max_workers=args.max_workers)
    print(f"Prepared {len(market_inputs)} market inputs")

    strategies = build_strategy_universe()
    print(f"Built {len(strategies)} strategies")

    validation = validate_strategy_parity(market_inputs, strategies[0], config, sample_size=args.parity_sample_size)
    print(json.dumps({
        "parity_ok": validation["parity_ok"],
        "checked_market_count": validation["checked_market_count"],
        "mismatch_count": validation["mismatch_count"],
    }, indent=2))
    if not validation["parity_ok"]:
        raise SystemExit("Parity validation failed; refusing full sweep")

    print("Evaluating strategies...")
    runs = evaluate_strategies(market_inputs, strategies, config)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": args.days,
        "market_count": len(markets),
        "prepared_market_count": len(market_inputs),
        "config": {
            "fee": args.fee,
            "volatility_window_seconds": config.volatility_window_seconds,
            "contract_trade_staleness_seconds": config.contract_trade_staleness_seconds,
        },
        "validation": validation,
        "top5": runs[:5],
        "runs": runs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {args.output}")
    print(json.dumps(payload["top5"], indent=2))


if __name__ == "__main__":
    main()
