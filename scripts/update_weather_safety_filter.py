#!/usr/bin/env python3
"""Refresh the live weather safety filter snapshot without running a cron job.

This is a standalone updater for the configured 4°C-change weather safety filter.
It evaluates every configured live weather-outlier city and upserts
`weather_safety_filter_latest` only when a dashboard-visible value changes. JSON/MD
reports are optional so routine runs do not spam service logs.

It deliberately does not toggle `weather_safety_filter_enabled`, start/stop any
strategy, or place/cancel orders. Live enforcement remains controlled only by the
saved strategy config/dashboard checkbox.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from polybot.live.weather_safety_filter import STATIONS, analyze_city_safety  # noqa: E402
from scripts.weather_safety_filter_report import make_markdown  # noqa: E402


@dataclass(frozen=True)
class WeatherShard:
    strategy_id: str
    city_slug: str
    config_path: Path
    enabled: bool
    dashboard_enabled: bool
    event_slug: str | None = None


def _slug(raw: Any) -> str:
    return str(raw or "").strip().lower().replace("_", "-").replace(" ", "-")


def _safe_bool(raw: Any, default: bool = False) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _shard_from_cfg(strategy_id: str, cfg: dict[str, Any], *, config_path: Path, latest_event_slug: str | None = None) -> WeatherShard | None:
    city = _slug(cfg.get("weather_city"))
    market_slug = str(cfg.get("market_slug") or "")
    prefix = "auto:weather-high-temp:"
    if not city and market_slug.startswith(prefix):
        city = _slug(market_slug[len(prefix):])
    if not city:
        return None
    event_slug = latest_event_slug or (market_slug if market_slug and not market_slug.startswith("auto:") else None)
    return WeatherShard(
        strategy_id=strategy_id,
        city_slug=city,
        config_path=config_path,
        enabled=_safe_bool(cfg.get("weather_safety_filter_enabled"), False),
        dashboard_enabled=_safe_bool(cfg.get("weather_outlier_dashboard_enabled"), True),
        event_slug=event_slug,
    )


def load_shards(config_dir: Path) -> list[WeatherShard]:
    """Discover weather-outlier live shard configs and their DB strategy IDs."""
    shards: list[WeatherShard] = []
    for p in sorted(config_dir.glob("weather-outlier-sniper-*-live.yaml")):
        try:
            cfg = yaml.safe_load(p.read_text()) or {}
        except Exception as exc:
            print(f"WARN: skipping unreadable config {p}: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue

        city = _slug(cfg.get("weather_city"))
        if not city:
            market_slug = str(cfg.get("market_slug") or "")
            prefix = "auto:weather-high-temp:"
            if market_slug.startswith(prefix):
                city = _slug(market_slug[len(prefix):])
        if not city:
            continue

        strategy_id = str(cfg.get("id") or f"live_weather_outlier_sniper_{city.replace('-', '_')}_auto_v1")
        shard = _shard_from_cfg(strategy_id, cfg, config_path=p)
        if shard:
            shards.append(shard)
    return shards


async def load_running_shards_from_db(dsn: str, config_dir: Path) -> list[WeatherShard]:
    """Load currently running dashboard/live weather shards from DB state.

    For auto-roll shards, configs contain only `auto:weather-high-temp:<city>`.
    The connected concrete event date comes from the latest live filter row
    written by each running shard (`weather_safety_filter_latest.event_slug`).
    """
    con = await asyncpg.connect(dsn)
    try:
        rows = await con.fetch(
            """
            SELECT s.id, s.config, w.event_slug AS latest_event_slug
            FROM strategies s
            LEFT JOIN weather_safety_filter_latest w ON w.strategy_id = s.id
            WHERE s.status='running'
              AND s.id LIKE 'live_weather_outlier_sniper_%'
            ORDER BY s.id
            """
        )
    finally:
        await con.close()
    shards: list[WeatherShard] = []
    for row in rows:
        cfg = row["config"] or {}
        if isinstance(cfg, str):
            try:
                cfg = json.loads(cfg)
            except Exception:
                cfg = {}
        if not isinstance(cfg, dict):
            cfg = {}
        city = _slug(cfg.get("weather_city"))
        config_path = config_dir / f"weather-outlier-sniper-{city}-live.yaml" if city else config_dir
        shard = _shard_from_cfg(str(row["id"]), cfg, config_path=config_path, latest_event_slug=row["latest_event_slug"])
        if shard:
            shards.append(shard)
    return shards


async def analyze_shards(shards: list[WeatherShard], *, concurrency: int) -> list[tuple[WeatherShard, dict[str, Any]]]:
    sem = asyncio.Semaphore(max(1, concurrency))

    async def wrapped(shard: WeatherShard) -> tuple[WeatherShard, dict[str, Any]]:
        async with sem:
            if shard.city_slug not in STATIONS:
                result = {
                    "city_slug": shard.city_slug,
                    "city": shard.city_slug,
                    "station": "",
                    "source": "",
                    "gate": "RED",
                    "reason": "city missing from station map",
                    "reasons": ["city missing from station map"],
                    "warnings": [],
                    "weather_codes": [],
                    "weather_code_names": [],
                    "expected_temp_fluctuation_c": None,
                    "size_multiplier": 0.0,
                    "metrics": {},
                    "event_slug": shard.event_slug,
                }
            else:
                result = await analyze_city_safety(shard.city_slug, event_slug=shard.event_slug)

            # Match live strategy sizing semantics, without changing enforcement state.
            gate = str(result.get("gate") or "GREEN").upper()
            if gate == "RED":
                result["size_multiplier"] = 0.0
            else:
                # GREEN and YELLOW use normal one-shot order size; YELLOW only
                # disables same-market re-buy ladders in live enforcement.
                result["size_multiplier"] = 1.0
            return shard, result

    rows = await asyncio.gather(*(wrapped(s) for s in shards))
    order = {"RED": 0, "YELLOW": 1, "GREEN": 2}
    return sorted(rows, key=lambda x: (order.get(str(x[1].get("gate") or ""), 9), x[0].city_slug))


def write_reports(rows: list[tuple[WeatherShard, dict[str, Any]]], json_path: Path, markdown_path: Path) -> None:
    results = [r for _, r in rows]
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    markdown_path.write_text(make_markdown(results))


async def upsert_db(rows: list[tuple[WeatherShard, dict[str, Any]]], *, dsn: str | None) -> int:
    # Import lazily so report-only refreshes still work in minimal environments
    # that do not have the live persistence dependencies installed.
    from polybot.persistence.writer import PolybotWriter

    writer = PolybotWriter(dsn)
    await writer.connect()
    try:
        count = 0
        for shard, result in rows:
            if not shard.dashboard_enabled:
                continue
            db_result = dict(result)
            # analyze_city_safety uses ISO strings for report JSON; PolybotWriter's
            # timestamptz argument expects a datetime object or None. Let the DB
            # stamp checked_at=now() for this standalone refresh.
            if isinstance(db_result.get("checked_at"), str):
                db_result["checked_at"] = None
            if await writer.upsert_weather_safety_filter(shard.strategy_id, db_result, enabled=shard.enabled):
                count += 1
        return count
    finally:
        await writer.close()


def print_summary(rows: list[tuple[WeatherShard, dict[str, Any]]], *, db_count: int | None, json_path: Path, markdown_path: Path, wrote_reports: bool, verbose: bool) -> None:
    counts = {g: 0 for g in ["GREEN", "YELLOW", "RED"]}
    for _, r in rows:
        gate = str(r.get("gate") or "GREEN").upper()
        if gate in counts:
            counts[gate] += 1
    red_yellow = [f"{r.get('city_slug')}={r.get('gate')}" for _, r in rows if str(r.get("gate") or "").upper() in {"RED", "YELLOW"}]
    if not verbose and not wrote_reports:
        return
    print(
        "Weather safety filter refreshed: "
        f"GREEN={counts['GREEN']} YELLOW={counts['YELLOW']} RED={counts['RED']} total={len(rows)}"
    )
    if wrote_reports:
        print(f"Reports: {json_path} · {markdown_path}")
    if db_count is None:
        print("Dashboard update: skipped")
    else:
        print(f"Dashboard update: {db_count} changed rows in weather_safety_filter_latest")
    if verbose and red_yellow:
        print("RED/YELLOW: " + ", ".join(red_yellow))


async def main() -> None:
    ap = argparse.ArgumentParser(description="Refresh configured live weather safety filter reports and dashboard DB rows.")
    ap.add_argument("--config-dir", type=Path, default=ROOT / "config")
    ap.add_argument("--json", type=Path, default=ROOT / "reports" / "weather_safety_filter_latest.json")
    ap.add_argument("--markdown", type=Path, default=ROOT / "reports" / "weather_safety_filter_latest.md")
    ap.add_argument("--write-reports", action="store_true", help="Also write JSON/MD report files. Default only updates the dashboard DB.")
    ap.add_argument("--verbose", action="store_true", help="Print a summary even when no dashboard rows changed.")
    ap.add_argument("--concurrency", type=int, default=2, help="Keep low to avoid Open-Meteo 429s during full-universe refreshes")
    ap.add_argument("--limit", type=int, default=0, help="Test mode: only refresh the first N discovered shards")
    ap.add_argument("--no-db", action="store_true", help="Only write report files; do not upsert dashboard DB")
    ap.add_argument("--configured-shards", action="store_true", help="Use all configured YAML shards instead of currently running DB shards")
    ap.add_argument("--dsn", default=os.getenv("NAUTILUS_DB_URL") or os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL"), help="Postgres DSN; defaults to NAUTILUS_DB_URL/DATABASE_URL/POSTGRES_URL")
    args = ap.parse_args()
    if not args.verbose:
        # Keep routine updater runs out of service logs unless the dashboard row
        # actually changes or the operator asks for verbose diagnostics.
        try:
            from loguru import logger as _loguru_logger

            _loguru_logger.remove()
        except Exception:
            pass

    if args.dsn and not args.configured_shards:
        shards = await load_running_shards_from_db(args.dsn, args.config_dir)
    else:
        shards = load_shards(args.config_dir)
    if args.limit:
        shards = shards[: args.limit]
    if not shards:
        source = "running DB strategies" if args.dsn and not args.configured_shards else f"configs in {args.config_dir}"
        raise SystemExit(f"No weather outlier shards found from {source}")

    rows = await analyze_shards(shards, concurrency=args.concurrency)
    if args.write_reports:
        write_reports(rows, args.json, args.markdown)

    db_count: int | None = None
    if not args.no_db:
        if not args.dsn:
            raise SystemExit("No DB DSN found. Set NAUTILUS_DB_URL or pass --dsn, or use --no-db.")
        db_count = await upsert_db(rows, dsn=args.dsn)

    print_summary(rows, db_count=db_count, json_path=args.json, markdown_path=args.markdown, wrote_reports=args.write_reports, verbose=args.verbose)


if __name__ == "__main__":
    asyncio.run(main())
