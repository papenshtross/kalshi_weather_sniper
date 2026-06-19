#!/usr/bin/env python3
"""Offline hot-path benchmark for the staged NWWS-OI weather-lock engine.

No network, no DB, no signing, no live orders.  It seeds an in-memory CLOB book,
feeds a recorded METAR stanza, and measures parser + threshold + book sweep +
circuit-breaker decision latency.
"""
from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polybot.live.nwws_weather_execution import (
    InMemoryBookStore,
    StationTarget,
    WeatherLockExecutionEngine,
)

RAW = b"SAUS70 KWBC 151500\nMETAR KLGA 151451Z 18007KT 10SM FEW050 28/14 A2992 RMK AO2"


def pct(vals: list[int], p: float) -> int:
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(len(vals) * p))]


async def main() -> None:
    target = StationTarget(
        b"KLGA",
        "nyc",
        "new-york-high-temp-demo",
        "YES_KLGA_28C_DEMO",
        28,
        price_ceiling=0.98,
        max_notional_usdc=5.0,
    )
    books = InMemoryBookStore()
    books.update_ws_message({
        "asset_id": "YES_KLGA_28C_DEMO",
        "asks": [
            {"price": "0.40", "size": "3"},
            {"price": "0.50", "size": "10"},
            {"price": "0.99", "size": "100"},
        ],
    })
    engine = WeatherLockExecutionEngine([target], {}, books=books, max_book_age_ms=10_000)
    try:
        # warmup
        for _ in range(1_000):
            await engine.on_stanza(RAW)
        samples: list[int] = []
        last = None
        for _ in range(20_000):
            t0 = time.perf_counter_ns()
            last = await engine.on_stanza(RAW)
            samples.append(time.perf_counter_ns() - t0)
        report = {
            "samples": len(samples),
            "p50_ns": int(statistics.median(samples)),
            "p90_ns": pct(samples, 0.90),
            "p99_ns": pct(samples, 0.99),
            "p999_ns": pct(samples, 0.999),
            "last_decision": last,
        }
        print(json.dumps(report, indent=2, default=str))
    finally:
        await engine.close()


if __name__ == "__main__":
    asyncio.run(main())
