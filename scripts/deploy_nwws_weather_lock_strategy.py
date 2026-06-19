#!/usr/bin/env python3
"""Stage the disabled Prism 3 NWWS-OI weather-lock strategy in dashboard DB."""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import asyncpg

SID = "live_nwws_weather_lock_prism3_v1"

CONFIG = {
    "dashboard_enabled": True,
    "strategy_family": "weather_lock_nwws_oi",
    "wallet_id": "prism3",
    "wallet_name": "Prism 3",
    "wallet_proxy": "0x333df4c8d4d4403c89ee253b1b5cf5b1361ba8b1",
    "wallet_eoa": "0xE0295DE5Cb6bEd26f5774474Fd75B8a5f75ec7a1",
    "signature_type": 3,
    "data_mode": "nwws_oi_xmpp_push_plus_clob_l2_ws",
    "nwws_host": "nwws-oi.weather.gov",
    "nwws_port": 5222,
    "nwws_pubsub_node": "/products",
    "station_filter": "KLGA",
    "active_markets_source": "dashboard/manual until market mapper is wired",
    "price_ceiling": 0.98,
    "max_notional_usdc": 0,
    "max_book_age_ms": 50,
    "presign_refresh_seconds": 30,
    "ring_buffer_size": 4096,
    "pre_sign_orders": True,
    "critical_path_no_db": True,
    "critical_path_no_rest_market_state": True,
    "critical_path_no_signing": True,
    "dry_run": True,
    "live_launch_armed": False,
    "live_trading": False,
    "activation_blockers": [
        "NWWS-OI credentials not configured in service env",
        "active weather market/token threshold mapper not wired",
        "pre-signed CLOB payload cache not populated",
        "tiny Prism 3 live order probe not run for this execution path",
        "standalone VPS latency benchmark not performed",
    ],
    "implementation_path": "/home/administrator/projects/polybot/polybot/live/nwws_weather_execution.py",
    "future_vps_ready": True,
    "deployed_at": datetime.now(timezone.utc).isoformat(),
}


async def main() -> None:
    dsn = os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("set NAUTILUS_DB_URL/POSTGRES_URL/DATABASE_URL")
    con = await asyncpg.connect(dsn)
    try:
        await con.execute(
            """
            INSERT INTO strategies(id, name, kind, market, status, config, mode)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            ON CONFLICT (id) DO UPDATE SET
              name=EXCLUDED.name,
              kind=EXCLUDED.kind,
              market=EXCLUDED.market,
              status='stopped',
              config=strategies.config || EXCLUDED.config,
              mode=EXCLUDED.mode,
              updated_at=now()
            """,
            SID,
            "Live · NWWS-OI Weather Lock Sniper · Prism 3 (staged)",
            "nwws_weather_lock",
            "NWWS-OI weather sensor lock → Polymarket weather buckets (disabled)",
            "stopped",
            json.dumps(CONFIG),
            "live",
        )
        await con.execute(
            """
            INSERT INTO strategy_logs(strategy_id, level, message)
            VALUES ($1, 'WARN', $2)
            """,
            SID,
            "NWWS-OI weather-lock strategy staged for Prism 3 with live_trading=false, live_launch_armed=false, dry_run=true, and dashboard start disabled. No orders can be submitted from this staged deployment.",
        )
        row = await con.fetchrow("select id, name, status, mode, kind, config from strategies where id=$1", SID)
        safe = dict(row)
        cfg = row["config"]
        if isinstance(cfg, str):
            cfg = json.loads(cfg)
        safe["config"] = {
            k: v for k, v in dict(cfg).items()
            if "key" not in k.lower() and "secret" not in k.lower()
        }
        print(json.dumps(safe, indent=2, default=str))
    finally:
        await con.close()


if __name__ == "__main__":
    asyncio.run(main())
