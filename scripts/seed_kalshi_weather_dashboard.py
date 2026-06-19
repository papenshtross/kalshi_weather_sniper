#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from polybot.persistence.writer import PolybotWriter
from polybot.live.kalshi_weather_sniper import load_config


async def main() -> None:
    cfg_path = Path(os.getenv("KALSHI_WEATHER_CONFIG", "config/kalshi-weather-sniper-live.yaml"))
    cfg = load_config(cfg_path)
    writer = PolybotWriter(os.getenv("KALSHI_WEATHER_DB_URL") or os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL"))
    await writer.connect()
    try:
        async with writer._pool.acquire() as con:  # type: ignore[attr-defined]
            await con.execute(
                """
                INSERT INTO strategies(id, name, kind, market, status, config, mode)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                ON CONFLICT (id) DO UPDATE
                SET name=$2, kind=$3, market=$4, config=$6::jsonb, mode=$7, updated_at=now()
                """,
                cfg["id"], cfg["name"], cfg["kind"], "Kalshi daily high-temperature markets",
                cfg.get("status", "stopped"), json.dumps(cfg, default=str), cfg.get("mode", "live"),
            )
        print(f"seeded {cfg['id']} ({cfg['kind']}) with order_size_usd={cfg.get('order_size_usd')} dry_run={cfg.get('dry_run')}")
    finally:
        await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
