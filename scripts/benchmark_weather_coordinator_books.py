#!/usr/bin/env python3
"""Read-only benchmark for the global weather /books coordinator.

This resolves the active weather token universe from the existing per-city config
files, then polls CLOB ``POST /books`` at the requested coordinator cadence with a
no-op fanout.  It never creates execution clients and never submits/cancels
orders.
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import httpx
import yaml

from polybot.live.arb_sniper import Book, resolve_event_pairs
from polybot.live.weather_outlier_coordinator import CoordinatedMarket, WeatherBookCoordinator, fetch_books_resilient


def _config_paths(values: Sequence[Path] | None) -> list[Path]:
    if values:
        return list(values)
    return [Path(p) for p in sorted(glob.glob("config/weather-outlier-sniper-*-live.yaml"))]


async def resolve_markets(paths: Sequence[Path]) -> list[CoordinatedMarket]:
    markets: list[CoordinatedMarket] = []
    for path in paths:
        cfg: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
        slug = str(cfg.get("market_slug") or cfg.get("market") or cfg.get("event_slug") or "")
        if not slug:
            continue
        try:
            pairs = await resolve_event_pairs(slug, all_markets=bool(cfg.get("monitor_all_markets") or cfg.get("event_all_markets") or cfg.get("market_mode") == "event_all_binary_markets"))
        except Exception as exc:
            print(f"WARN unresolved config={path} slug={slug}: {type(exc).__name__}: {exc}")
            continue
        tokens: list[str] = []
        for pair in pairs:
            tokens.extend([str(pair["yes_token"]), str(pair["no_token"])])
        markets.append(CoordinatedMarket(str(cfg.get("id") or path.stem), str(slug), tokens, {"config": str(path), "pairs": len(pairs)}))
    return markets


async def main_async() -> None:
    parser = argparse.ArgumentParser(description="Read-only benchmark for global weather /books polling")
    parser.add_argument("--config", action="append", type=Path)
    parser.add_argument("--target-hz", type=float, default=1.2)
    parser.add_argument("--duration-seconds", type=float, default=10.0)
    parser.add_argument("--max-tokens-per-request", type=int, default=500)
    parser.add_argument("--max-planned-request-rate", type=float, default=4.5)
    parser.add_argument("--sequential", action="store_true", help="Await each cycle before starting the next; default is production-like pipelined issuance")
    parser.add_argument("--max-in-flight-cycles", type=int, default=8)
    args = parser.parse_args()

    paths = _config_paths(args.config)
    markets = await resolve_markets(paths)
    if not markets:
        raise SystemExit("no markets resolved")

    latencies: list[float] = []
    cycle_ms: list[float] = []
    applied_counts: list[int] = []
    timeout = httpx.Timeout(2.0, connect=0.5, read=1.5, write=0.5, pool=0.5)
    limits = httpx.Limits(max_keepalive_connections=8, max_connections=16, keepalive_expiry=30.0)

    async with httpx.AsyncClient(timeout=timeout, limits=limits, http2=True) as client:
        async def fetch(batch: Sequence[str]) -> dict[str, Book]:
            t0 = time.perf_counter()
            out = await fetch_books_resilient(client, list(batch))
            latencies.append((time.perf_counter() - t0) * 1000.0)
            return out

        async def apply(_market: CoordinatedMarket, snapshots: dict[str, Book]) -> None:
            applied_counts.append(len(snapshots))

        coordinator = WeatherBookCoordinator(
            markets=markets,
            fetch_books=fetch,
            apply_snapshots=apply,
            target_hz=args.target_hz,
            max_tokens_per_request=args.max_tokens_per_request,
            max_planned_request_rate=args.max_planned_request_rate,
            require_complete_market_snapshot=True,
        )
        cycles = max(1, int(args.duration_seconds * args.target_hz))
        print(
            f"resolved strategies={len(markets)} tokens={coordinator.token_count} batches={coordinator.requests_per_cycle} "
            f"target_hz={coordinator.target_hz:.2f} planned_req_s={coordinator.planned_request_rate:.2f} cycles={cycles}"
        )
        start = time.perf_counter()
        if args.sequential:
            for _ in range(cycles):
                t0 = time.perf_counter()
                await coordinator.run_cycles(cycles=1)
                cycle_ms.append((time.perf_counter() - t0) * 1000.0)
        else:
            stop = asyncio.Event()

            async def stopper() -> None:
                await asyncio.sleep(args.duration_seconds)
                stop.set()

            await asyncio.gather(coordinator.run_forever(stop, log_interval_seconds=0, max_in_flight_cycles=args.max_in_flight_cycles), stopper())
            cycle_ms.append(coordinator.stats.last_cycle_ms)
        elapsed = time.perf_counter() - start

    def pct(vals: list[float], p: float) -> float:
        if not vals:
            return 0.0
        vals = sorted(vals)
        idx = min(len(vals) - 1, max(0, int(round((p / 100.0) * (len(vals) - 1)))))
        return vals[idx]

    stats = coordinator.stats
    print(f"elapsed_s={elapsed:.3f}")
    print(f"cycles={stats.cycles} effective_hz={stats.cycles / elapsed:.3f} target_hz={args.target_hz:.3f}")
    print(f"book_requests={stats.book_requests} effective_req_s={stats.book_requests / elapsed:.3f} planned_req_s={coordinator.planned_request_rate:.3f}")
    print(f"request_errors={stats.request_errors} tokens_received={stats.tokens_received} tokens_requested={stats.tokens_requested}")
    print(f"markets_applied={stats.markets_applied} incomplete_markets_skipped={stats.incomplete_markets_skipped}")
    print(f"cycle_ms_median={statistics.median(cycle_ms):.1f} p90={pct(cycle_ms, 90):.1f} p99={pct(cycle_ms, 99):.1f} max={max(cycle_ms):.1f}")
    if latencies:
        print(f"request_ms_median={statistics.median(latencies):.1f} p90={pct(latencies, 90):.1f} p99={pct(latencies, 99):.1f} max={max(latencies):.1f}")
    print(f"applied_snapshot_tokens_per_market_median={statistics.median(applied_counts) if applied_counts else 0}")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
