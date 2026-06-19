"""Postgres/Timescale writer for the polybot dashboard.

The dashboard's /api/metrics endpoint expects this exact schema. Run the
DDL once on your Postgres (it's idempotent), then call PolybotWriter.start()
from the live runner — it will subscribe to the Nautilus message bus and
upsert equity snapshots, positions, fills, and orders.

DDL is executed automatically on first connect.
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Any

from loguru import logger

try:
    import asyncpg  # type: ignore
except ImportError:  # pragma: no cover
    asyncpg = None  # type: ignore


DDL = """
CREATE TABLE IF NOT EXISTS strategies (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL,
    market      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running',
    config      JSONB NOT NULL DEFAULT '{}'::jsonb,
    mode        TEXT NOT NULL DEFAULT 'paper',
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS equity_snapshots (
    strategy_id TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    equity      NUMERIC NOT NULL
);
CREATE INDEX IF NOT EXISTS equity_snapshots_sid_ts ON equity_snapshots (strategy_id, ts DESC);

CREATE TABLE IF NOT EXISTS positions (
    strategy_id TEXT NOT NULL,
    market      TEXT NOT NULL,
    side        TEXT NOT NULL,
    size        NUMERIC NOT NULL,
    entry       NUMERIC NOT NULL,
    last        NUMERIC NOT NULL,
    pnl         NUMERIC NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (strategy_id, market)
);

CREATE TABLE IF NOT EXISTS fills (
    strategy_id TEXT NOT NULL,
    id          BIGINT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    market      TEXT NOT NULL,
    side        TEXT NOT NULL,
    px          NUMERIC NOT NULL,
    size        NUMERIC NOT NULL,
    kind        TEXT DEFAULT 'MM',
    PRIMARY KEY (strategy_id, id)
);
CREATE INDEX IF NOT EXISTS fills_sid_ts ON fills (strategy_id, ts DESC);

CREATE TABLE IF NOT EXISTS price_ticks (
    strategy_id TEXT NOT NULL,
    token       TEXT NOT NULL,
    label       TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    best_bid    NUMERIC NOT NULL,
    best_ask    NUMERIC NOT NULL
);
CREATE INDEX IF NOT EXISTS price_ticks_sid_ts ON price_ticks (strategy_id, ts DESC);

CREATE TABLE IF NOT EXISTS book_latest (
    strategy_id TEXT NOT NULL,
    token       TEXT NOT NULL,
    label       TEXT NOT NULL,
    bids        JSONB NOT NULL DEFAULT '[]'::jsonb,
    asks        JSONB NOT NULL DEFAULT '[]'::jsonb,
    best_bid    NUMERIC NOT NULL DEFAULT 0,
    best_ask    NUMERIC NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (strategy_id, token)
);

CREATE TABLE IF NOT EXISTS strategy_logs (
    strategy_id TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    level       TEXT NOT NULL DEFAULT 'INFO',
    message     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS strategy_logs_sid_ts ON strategy_logs (strategy_id, ts DESC);

CREATE TABLE IF NOT EXISTS market_observations (
    id              BIGSERIAL PRIMARY KEY,
    strategy_id     TEXT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    market_slug     TEXT NOT NULL,
    market_title    TEXT NOT NULL DEFAULT '',
    market_start_ts BIGINT,
    market_end_ts   BIGINT,
    price_to_beat   NUMERIC,
    final_price     NUMERIC,
    up_token        TEXT NOT NULL DEFAULT '',
    down_token      TEXT NOT NULL DEFAULT '',
    up_bid          NUMERIC,
    up_ask          NUMERIC,
    down_bid        NUMERIC,
    down_ask        NUMERIC,
    up_bids         JSONB NOT NULL DEFAULT '[]'::jsonb,
    up_asks         JSONB NOT NULL DEFAULT '[]'::jsonb,
    down_bids       JSONB NOT NULL DEFAULT '[]'::jsonb,
    down_asks       JSONB NOT NULL DEFAULT '[]'::jsonb,
    binance         JSONB NOT NULL DEFAULT '{}'::jsonb,
    signal          JSONB NOT NULL DEFAULT '{}'::jsonb,
    config          JSONB NOT NULL DEFAULT '{}'::jsonb,
    state           JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS market_observations_sid_ts ON market_observations (strategy_id, ts DESC);
CREATE INDEX IF NOT EXISTS market_observations_slug_ts ON market_observations (market_slug, ts DESC);

CREATE TABLE IF NOT EXISTS order_attempts (
    id          BIGSERIAL PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    market_slug TEXT NOT NULL,
    token       TEXT NOT NULL DEFAULT '',
    outcome     TEXT NOT NULL DEFAULT '',
    side        TEXT NOT NULL DEFAULT '',
    order_type  TEXT NOT NULL DEFAULT '',
    price       NUMERIC,
    size        NUMERIC,
    stake_usd   NUMERIC,
    status      TEXT NOT NULL DEFAULT '',
    response    JSONB NOT NULL DEFAULT '{}'::jsonb,
    error       TEXT,
    signal      JSONB NOT NULL DEFAULT '{}'::jsonb,
    config      JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS order_attempts_sid_ts ON order_attempts (strategy_id, ts DESC);
CREATE INDEX IF NOT EXISTS order_attempts_slug_ts ON order_attempts (market_slug, ts DESC);

CREATE TABLE IF NOT EXISTS weather_safety_filter_latest (
    strategy_id                 TEXT PRIMARY KEY,
    city_slug                   TEXT NOT NULL,
    city                        TEXT NOT NULL DEFAULT '',
    station                     TEXT NOT NULL DEFAULT '',
    source                      TEXT NOT NULL DEFAULT '',
    gate                        TEXT NOT NULL DEFAULT 'GREEN',
    reason                      TEXT NOT NULL DEFAULT '',
    expected_temp_fluctuation_c NUMERIC,
    weather_codes               JSONB NOT NULL DEFAULT '[]'::jsonb,
    weather_code_names          JSONB NOT NULL DEFAULT '[]'::jsonb,
    size_multiplier             NUMERIC NOT NULL DEFAULT 1,
    enabled                     BOOLEAN NOT NULL DEFAULT false,
    event_slug                  TEXT NOT NULL DEFAULT '',
    metrics                     JSONB NOT NULL DEFAULT '{}'::jsonb,
    reasons                     JSONB NOT NULL DEFAULT '[]'::jsonb,
    warnings                    JSONB NOT NULL DEFAULT '[]'::jsonb,
    checked_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS weather_safety_filter_latest_gate ON weather_safety_filter_latest (gate, updated_at DESC);
CREATE INDEX IF NOT EXISTS weather_safety_filter_latest_city ON weather_safety_filter_latest (city_slug, updated_at DESC);
"""


class PolybotWriter:
    """Persistence writer.

    Usage:
        writer = PolybotWriter(os.environ["NAUTILUS_DB_URL"])
        await writer.connect()
        # Hook into Nautilus events:
        node.kernel.msgbus.subscribe("events.*", writer.on_event)
        # Or call directly from the strategy:
        await writer.snapshot_equity(decimal_value)
    """

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or os.getenv("NAUTILUS_DB_URL", "postgresql://polybot:polybot@localhost:5432/polybot")
        self._pool: Any = None

    async def connect(self) -> None:
        if asyncpg is None:
            raise RuntimeError("asyncpg not installed — pip install asyncpg")
        self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=4)
        async with self._pool.acquire() as con:
            await con.execute(DDL)
        logger.info("PolybotWriter connected, schema ready")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    # ------------------------------------------------------------------ writes

    async def register_strategy(self, strategy_id: str, name: str, kind: str,
                                market: str, config: dict) -> None:
        import json as _json
        async with self._pool.acquire() as con:
            await con.execute(
                """INSERT INTO strategies(id, name, kind, market, status, config)
                   VALUES ($1, $2, $3, $4, 'running', $5::jsonb)
                   ON CONFLICT (id) DO UPDATE
                   SET name=$2,
                       kind=$3,
                       market=$4,
                       -- preserve dashboard-controlled status; don't force running on every refresh/roll
                       status=strategies.status,
                       -- preserve dashboard-saved config; never overwrite with file defaults on refresh/roll
                       config=strategies.config,
                       updated_at=now()""",
                strategy_id, name, kind, market, _json.dumps(config),
            )

    async def set_strategy_status(self, strategy_id: str, status: str) -> None:
        async with self._pool.acquire() as con:
            await con.execute(
                "UPDATE strategies SET status=$2, updated_at=now() WHERE id=$1",
                strategy_id, status,
            )

    async def get_strategy_status(self, strategy_id: str) -> str | None:
        async with self._pool.acquire() as con:
            return await con.fetchval(
                "SELECT status FROM strategies WHERE id=$1", strategy_id,
            )

    async def get_strategy_config(self, strategy_id: str) -> dict[str, Any]:
        import json as _json
        async with self._pool.acquire() as con:
            row = await con.fetchrow("SELECT config FROM strategies WHERE id=$1", strategy_id)
            if not row:
                return {}
            cfg = row["config"]
            if cfg is None:
                return {}
            if isinstance(cfg, dict):
                return dict(cfg)
            if isinstance(cfg, str):
                try:
                    parsed = _json.loads(cfg)
                    return dict(parsed) if isinstance(parsed, dict) else {}
                except Exception:
                    return {}
            try:
                return dict(cfg)
            except Exception:
                return {}

    async def count_fills(self, strategy_id: str) -> int:
        async with self._pool.acquire() as con:
            val = await con.fetchval("SELECT COUNT(*) FROM fills WHERE strategy_id=$1", strategy_id)
            return int(val or 0)

    async def snapshot_equity(self, strategy_id: str, equity: float) -> None:
        async with self._pool.acquire() as con:
            await con.execute(
                "INSERT INTO equity_snapshots(strategy_id, ts, equity) VALUES ($1, now(), $2)",
                strategy_id, equity,
            )

    async def upsert_position(self, strategy_id: str, market: str, side: str,
                              size: float, entry: float, last: float, pnl: float) -> None:
        async with self._pool.acquire() as con:
            await con.execute(
                """INSERT INTO positions(strategy_id, market, side, size, entry, last, pnl, updated_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7, now())
                   ON CONFLICT (strategy_id, market) DO UPDATE
                   SET side=$3, size=$4, entry=$5, last=$6, pnl=$7, updated_at=now()""",
                strategy_id, market, side, size, entry, last, pnl,
            )

    async def record_fill(self, strategy_id: str, fill_id: int, market: str,
                          side: str, px: float, size: float, kind: str = "MM") -> None:
        async with self._pool.acquire() as con:
            await con.execute(
                """INSERT INTO fills(strategy_id, id, ts, market, side, px, size, kind)
                   VALUES ($1, $2, now(), $3, $4, $5, $6, $7)
                   ON CONFLICT (strategy_id, id) DO NOTHING""",
                strategy_id, fill_id, market, side, px, size, kind,
            )

    async def log_strategy_event(self, strategy_id: str, message: str, level: str = "INFO") -> None:
        async with self._pool.acquire() as con:
            await con.execute(
                "INSERT INTO strategy_logs(strategy_id, ts, level, message) VALUES ($1, now(), $2, $3)",
                strategy_id, level, message,
            )

    async def record_tick(self, strategy_id: str, token: str, label: str,
                          best_bid: float, best_ask: float) -> None:
        async with self._pool.acquire() as con:
            await con.execute(
                """INSERT INTO price_ticks(strategy_id, token, label, ts, best_bid, best_ask)
                   VALUES ($1, $2, $3, now(), $4, $5)""",
                strategy_id, token, label, best_bid, best_ask,
            )

    async def upsert_book(self, strategy_id: str, token: str, label: str,
                          bids: list, asks: list, best_bid: float, best_ask: float) -> None:
        await self.upsert_books([
            {
                "strategy_id": strategy_id,
                "token": token,
                "label": label,
                "bids": bids,
                "asks": asks,
                "best_bid": best_bid,
                "best_ask": best_ask,
            }
        ])

    async def upsert_books(self, rows: list[dict[str, Any]]) -> None:
        """Bulk upsert latest books with one DB timestamp for all rows.

        Weather arb snipers monitor 22+ tokens per worker. Updating those rows
        one-at-a-time against the remote dashboard DB can take several seconds,
        which makes the dashboard display artificial 5-10s quote ages even while
        the live in-memory hot path is fresh. A single JSONB recordset upsert
        keeps dashboard freshness close to the worker's write cadence and avoids
        holding the status loop on per-token round trips.
        """
        if not rows:
            return
        import json as _json
        payload = _json.dumps(rows)
        async with self._pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO book_latest(strategy_id, token, label, bids, asks, best_bid, best_ask, updated_at)
                SELECT strategy_id, token, label,
                       COALESCE(bids, '[]'::jsonb),
                       COALESCE(asks, '[]'::jsonb),
                       COALESCE(best_bid, 0),
                       COALESCE(best_ask, 0),
                       now()
                FROM jsonb_to_recordset($1::jsonb) AS x(
                    strategy_id text,
                    token text,
                    label text,
                    bids jsonb,
                    asks jsonb,
                    best_bid numeric,
                    best_ask numeric
                )
                ON CONFLICT (strategy_id, token) DO UPDATE
                SET label=EXCLUDED.label,
                    bids=EXCLUDED.bids,
                    asks=EXCLUDED.asks,
                    best_bid=EXCLUDED.best_bid,
                    best_ask=EXCLUDED.best_ask,
                    updated_at=EXCLUDED.updated_at
                """,
                payload,
            )

    async def record_market_observation(
        self,
        strategy_id: str,
        market_slug: str,
        market_title: str,
        market_start_ts: int | None,
        market_end_ts: int | None,
        price_to_beat: float | None,
        final_price: float | None,
        up_token: str,
        down_token: str,
        up_bid: float | None,
        up_ask: float | None,
        down_bid: float | None,
        down_ask: float | None,
        up_bids: list,
        up_asks: list,
        down_bids: list,
        down_asks: list,
        binance: dict[str, Any],
        signal: dict[str, Any],
        config: dict[str, Any],
        state: dict[str, Any],
    ) -> None:
        """Persist a historical market-data/signal snapshot for replay/backtests."""
        import json as _json
        async with self._pool.acquire() as con:
            await con.execute(
                """INSERT INTO market_observations(
                       strategy_id, market_slug, market_title, market_start_ts, market_end_ts,
                       price_to_beat, final_price, up_token, down_token,
                       up_bid, up_ask, down_bid, down_ask,
                       up_bids, up_asks, down_bids, down_asks,
                       binance, signal, config, state)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                           $14::jsonb,$15::jsonb,$16::jsonb,$17::jsonb,
                           $18::jsonb,$19::jsonb,$20::jsonb,$21::jsonb)""",
                strategy_id, market_slug, market_title, market_start_ts, market_end_ts,
                price_to_beat, final_price, up_token, down_token,
                up_bid, up_ask, down_bid, down_ask,
                _json.dumps(up_bids), _json.dumps(up_asks),
                _json.dumps(down_bids), _json.dumps(down_asks),
                _json.dumps(binance), _json.dumps(signal), _json.dumps(config), _json.dumps(state),
            )

    async def record_order_attempt(
        self,
        strategy_id: str,
        market_slug: str,
        token: str,
        outcome: str,
        side: str,
        order_type: str,
        price: float | None,
        size: float | None,
        stake_usd: float | None,
        status: str,
        response: dict[str, Any] | None = None,
        error: str | None = None,
        signal: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Persist every live order intent/result, including rejects/errors."""
        import json as _json
        async with self._pool.acquire() as con:
            await con.execute(
                """INSERT INTO order_attempts(
                       strategy_id, market_slug, token, outcome, side, order_type,
                       price, size, stake_usd, status, response, error, signal, config)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13::jsonb,$14::jsonb)""",
                strategy_id, market_slug, token, outcome, side, order_type,
                price, size, stake_usd, status,
                _json.dumps(response or {}), error,
                _json.dumps(signal or {}), _json.dumps(config or {}),
            )

    async def update_order_attempt_by_order_id(
        self,
        strategy_id: str,
        order_id: str,
        *,
        status: str,
        price: float | None = None,
        size: float | None = None,
        stake_usd: float | None = None,
        response_patch: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> bool:
        """Update a delayed CLOB attempt after reconciliation by order id."""
        if not order_id:
            return False
        import json as _json
        sets = ["status=$3"]
        params: list[Any] = [strategy_id, order_id, status]
        if price is not None:
            params.append(price)
            sets.append(f"price=${len(params)}")
        if size is not None:
            params.append(size)
            sets.append(f"size=${len(params)}")
        if stake_usd is not None:
            params.append(stake_usd)
            sets.append(f"stake_usd=${len(params)}")
        if response_patch is not None:
            params.append(_json.dumps(response_patch or {}))
            sets.append(f"response = response || ${len(params)}::jsonb")
        if error is not None:
            params.append(error)
            sets.append(f"error=${len(params)}")
        async with self._pool.acquire() as con:
            tag = await con.execute(
                f"""UPDATE order_attempts
                    SET {', '.join(sets)}
                    WHERE strategy_id=$1
                      AND (response->>'orderID'=$2 OR response->>'order_id'=$2)""",
                *params,
            )
        try:
            return int(str(tag).split()[-1]) > 0
        except Exception:
            return False

    async def pending_order_attempts_by_response_order_id(
        self,
        strategy_id: str,
        *,
        order_type: str | None = None,
        side: str | None = None,
        status: str = "submitted",
        max_age_seconds: int = 600,
    ) -> list[dict[str, Any]]:
        clauses = ["strategy_id=$1", "status=$2", "COALESCE(response->>'orderID', response->>'order_id', '') <> ''", f"ts > now() - interval '{int(max_age_seconds)} seconds'"]
        params: list[Any] = [strategy_id, status]
        if order_type is not None:
            params.append(order_type)
            clauses.append(f"order_type=${len(params)}")
        if side is not None:
            params.append(side)
            clauses.append(f"side=${len(params)}")
        async with self._pool.acquire() as con:
            rows = await con.fetch(
                f"""SELECT id, ts, market_slug, token, outcome, side, order_type, price, size, stake_usd, status, response, signal
                    FROM order_attempts
                    WHERE {' AND '.join(clauses)}
                    ORDER BY ts ASC, id ASC""",
                *params,
            )
        return [dict(r) for r in rows]

    async def count_order_attempts(self, strategy_id: str, market_slug: str) -> int:
        async with self._pool.acquire() as con:
            return int(
                await con.fetchval(
                    "SELECT COUNT(*) FROM order_attempts WHERE strategy_id=$1 AND market_slug=$2",
                    strategy_id, market_slug,
                )
                or 0
            )

    async def count_successful_order_attempts(self, strategy_id: str, market_slug: str) -> int:
        async with self._pool.acquire() as con:
            return int(
                await con.fetchval(
                    """SELECT COUNT(*)
                       FROM order_attempts
                       WHERE strategy_id=$1 AND market_slug=$2
                         AND (status IN ('filled','submitted') OR response->>'success' = 'true')""",
                    strategy_id, market_slug,
                )
                or 0
            )

    async def successful_order_stake_usd(self, strategy_id: str, market_slug: str, *, side: str | None = None, token: str | None = None) -> float:
        clauses = ["strategy_id=$1", "market_slug=$2", "(status IN ('filled','submitted') OR response->>'success' = 'true')"]
        params: list[Any] = [strategy_id, market_slug]
        if side is not None:
            params.append(side)
            clauses.append(f"side=${len(params)}")
        if token is not None:
            params.append(token)
            clauses.append(f"token=${len(params)}")
        async with self._pool.acquire() as con:
            return float(
                await con.fetchval(
                    f"SELECT COALESCE(SUM(stake_usd), 0) FROM order_attempts WHERE {' AND '.join(clauses)}",
                    *params,
                )
                or 0.0
            )

    async def successful_order_stake_usd_many(self, strategy_id: str, slug_tokens: list[tuple[str, str]], *, side: str | None = None) -> dict[tuple[str, str], float]:
        if not slug_tokens:
            return {}
        slugs = [slug for slug, _token in slug_tokens]
        tokens = [token for _slug, token in slug_tokens]
        clauses = ["strategy_id=$1", "market_slug = ANY($2::text[])", "token = ANY($3::text[])", "(status IN ('filled','submitted') OR response->>'success' = 'true')"]
        params: list[Any] = [strategy_id, slugs, tokens]
        if side is not None:
            params.append(side)
            clauses.append(f"side=${len(params)}")
        async with self._pool.acquire() as con:
            rows = await con.fetch(
                f"SELECT market_slug, token, COALESCE(SUM(stake_usd), 0) AS stake FROM order_attempts WHERE {' AND '.join(clauses)} GROUP BY market_slug, token",
                *params,
            )
        return {(str(r['market_slug']), str(r['token'])): float(r['stake'] or 0.0) for r in rows}

    async def successful_buy_market_slugs(self, strategy_id: str) -> set[str]:
        """Return market slugs with successful/submitted BUY exposure for a strategy."""
        async with self._pool.acquire() as con:
            rows = await con.fetch(
                """SELECT DISTINCT market_slug FROM order_attempts
                   WHERE strategy_id=$1 AND side='BUY'
                     AND market_slug IS NOT NULL AND market_slug <> ''
                     AND (status IN ('filled','submitted') OR response->>'success' = 'true')""",
                strategy_id,
            )
        return {str(r['market_slug']) for r in rows if r['market_slug']}

    async def first_successful_buy_outlier_signal_by_event(self, strategy_id: str) -> dict[str, dict[str, Any]]:
        """Return earliest successful BUY signal per weather event/date for direction locks."""
        async with self._pool.acquire() as con:
            rows = await con.fetch(
                """SELECT DISTINCT ON (regexp_replace(market_slug, '^((highest-temperature-in-[a-z0-9-]+-on-[a-z]+-[0-9]{1,2}-[0-9]{4})).*$', '\\1'))
                          regexp_replace(market_slug, '^((highest-temperature-in-[a-z0-9-]+-on-[a-z]+-[0-9]{1,2}-[0-9]{4})).*$', '\\1') AS event_key,
                          market_slug, signal, ts
                   FROM order_attempts
                   WHERE strategy_id=$1 AND side='BUY'
                     AND market_slug IS NOT NULL AND market_slug <> ''
                     AND (status IN ('filled','submitted') OR response->>'success' = 'true')
                   ORDER BY regexp_replace(market_slug, '^((highest-temperature-in-[a-z0-9-]+-on-[a-z]+-[0-9]{1,2}-[0-9]{4})).*$', '\\1'), ts ASC, id ASC""",
                strategy_id,
            )
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            sig = r["signal"] or {}
            if isinstance(sig, str):
                try:
                    sig = json.loads(sig)
                except Exception:
                    sig = {}
            out[str(r["event_key"] or r["market_slug"])] = {
                "market_slug": str(r["market_slug"] or ""),
                "temp_value": sig.get("temp_value") if isinstance(sig, dict) else None,
                "winning_temp": sig.get("winning_temp") if isinstance(sig, dict) else None,
                "ts": r["ts"],
            }
        return out

    async def net_filled_order_size(self, strategy_id: str, market_slug: str, token: str) -> float:
        async with self._pool.acquire() as con:
            return float(
                await con.fetchval(
                    """SELECT COALESCE(SUM(CASE WHEN side='BUY' THEN size WHEN side='SELL' THEN -size ELSE 0 END), 0)
                       FROM order_attempts
                       WHERE strategy_id=$1 AND market_slug=$2 AND token=$3
                         AND status='filled'""",
                    strategy_id, market_slug, token,
                )
                or 0.0
            )

    async def count_filled_order_attempts(self, strategy_id: str, market_slug: str) -> int:
        async with self._pool.acquire() as con:
            return int(
                await con.fetchval(
                    "SELECT COUNT(*) FROM order_attempts WHERE strategy_id=$1 AND market_slug=$2 AND status='filled'",
                    strategy_id, market_slug,
                )
                or 0
            )

    async def latest_order_attempt(self, strategy_id: str, market_slug: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as con:
            row = await con.fetchrow(
                """SELECT status, response, error, signal, config
                   FROM order_attempts
                   WHERE strategy_id=$1 AND market_slug=$2
                   ORDER BY ts DESC, id DESC
                   LIMIT 1""",
                strategy_id, market_slug,
            )
            return dict(row) if row else None

    async def has_order_attempt(self, strategy_id: str, market_slug: str) -> bool:
        return (await self.count_order_attempts(strategy_id, market_slug)) > 0

    async def purge_old_ticks(self, strategy_id: str, keep_minutes: int = 60) -> None:
        async with self._pool.acquire() as con:
            await con.execute(
                f"DELETE FROM price_ticks WHERE strategy_id=$1 AND ts < now() - interval '{keep_minutes} minutes'",
                strategy_id,
            )

    async def upsert_weather_safety_filter(self, strategy_id: str, result: dict[str, Any], *, enabled: bool = False) -> bool:
        """Insert/update the latest weather safety row.

        Returns True only when the dashboard-visible row was inserted or changed.
        Routine checks always advance checked_at so freshness is truthful, while
        unchanged gate/reason/metrics leave updated_at untouched so dashboard
        polling can distinguish real filter changes from heartbeat checks.
        """
        import json as _json
        from datetime import datetime

        metrics = dict(result.get("metrics") or {})
        # Volatile clock-derived values change on every refresh and would make
        # routine checks look like dashboard changes. Keep dashboard rows focused
        # on gate/reason/model inputs that matter to trading/display state.
        metrics.pop("obs_age_min", None)
        checked_at = result.get("checked_at")
        if isinstance(checked_at, str):
            try:
                checked_at = datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
            except ValueError:
                checked_at = None
        async with self._pool.acquire() as con:
            changed = await con.fetchval(
                """WITH upsert AS (
                   INSERT INTO weather_safety_filter_latest(
                       strategy_id, city_slug, city, station, source, gate, reason,
                       expected_temp_fluctuation_c, weather_codes, weather_code_names,
                       size_multiplier, enabled, event_slug, metrics, reasons, warnings,
                       checked_at, updated_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11,$12,$13,$14::jsonb,$15::jsonb,$16::jsonb,COALESCE($17::timestamptz, now()),now())
                   ON CONFLICT (strategy_id) DO UPDATE SET
                       city_slug=$2, city=$3, station=$4, source=$5, gate=$6, reason=$7,
                       expected_temp_fluctuation_c=$8, weather_codes=$9::jsonb,
                       weather_code_names=$10::jsonb, size_multiplier=$11, enabled=$12,
                       event_slug=$13, metrics=$14::jsonb, reasons=$15::jsonb, warnings=$16::jsonb,
                       checked_at=COALESCE($17::timestamptz, now()),
                       updated_at=CASE WHEN (
                           weather_safety_filter_latest.city_slug,
                           weather_safety_filter_latest.city,
                           weather_safety_filter_latest.station,
                           weather_safety_filter_latest.source,
                           weather_safety_filter_latest.gate,
                           weather_safety_filter_latest.reason,
                           weather_safety_filter_latest.expected_temp_fluctuation_c,
                           weather_safety_filter_latest.weather_codes,
                           weather_safety_filter_latest.weather_code_names,
                           weather_safety_filter_latest.size_multiplier,
                           weather_safety_filter_latest.enabled,
                           weather_safety_filter_latest.event_slug,
                           weather_safety_filter_latest.metrics,
                           weather_safety_filter_latest.reasons,
                           weather_safety_filter_latest.warnings)
                          IS DISTINCT FROM
                          ($2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11,$12,$13,$14::jsonb,$15::jsonb,$16::jsonb)
                       THEN now() ELSE weather_safety_filter_latest.updated_at END
                   RETURNING CASE WHEN xmax = 0 OR updated_at = now() THEN 1 ELSE 0 END
                )
                SELECT COALESCE((SELECT 1 FROM upsert), 0)""",
                strategy_id,
                str(result.get("city_slug") or ""),
                str(result.get("city") or ""),
                str(result.get("station") or ""),
                str(result.get("source") or ""),
                str(result.get("gate") or "GREEN"),
                str(result.get("reason") or ""),
                result.get("expected_temp_fluctuation_c"),
                _json.dumps(result.get("weather_codes") or []),
                _json.dumps(result.get("weather_code_names") or []),
                float(result.get("size_multiplier") if result.get("size_multiplier") is not None else 1.0),
                bool(enabled),
                str(result.get("event_slug") or ""),
                _json.dumps(metrics),
                _json.dumps(result.get("reasons") or []),
                _json.dumps(result.get("warnings") or []),
                checked_at,
            )
            return bool(changed)

    # ------------------------------------------------------------------ msgbus glue

    def on_event(self, event: Any) -> None:
        """Bridge Nautilus events into async writes via a background task."""
        cls = type(event).__name__
        if cls == "OrderFilled":
            asyncio.create_task(self.record_fill(
                fill_id=int(getattr(event, "trade_id", str(id(event)))),
                market=str(event.instrument_id),
                side=str(event.order_side),
                px=float(event.last_px),
                size=float(event.last_qty),
            ))
        elif cls in ("OrderAccepted", "OrderUpdated", "OrderCanceled", "OrderRejected"):
            asyncio.create_task(self.upsert_order(
                order_id=str(event.client_order_id),
                market=str(event.instrument_id),
                side=str(getattr(event, "order_side", "")),
                px=float(getattr(event, "price", 0) or 0),
                size=float(getattr(event, "quantity", 0) or 0),
                status=cls.replace("Order", "").upper(),
            ))
