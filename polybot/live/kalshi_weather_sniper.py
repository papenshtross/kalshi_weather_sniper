from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from loguru import logger

from polybot.adapters.kalshi.client import KalshiHttpClient, KalshiMarket, parse_market
from polybot.live.kalshi_weather_universe import (
    ALL_KALSHI_HIGH_TEMP_SERIES,
    boundary_veto_reason,
    nws_daily_high,
)
from polybot.live.weather_safety_filter import STATIONS
from polybot.persistence.writer import PolybotWriter

STARTING_CASH = 10_000.0


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _safe_bool(x: Any, default: bool = False) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        return x.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(x)


def load_config(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text()) or {}
    data.setdefault("id", "live_kalshi_weather_sniper_v1")
    data.setdefault("name", "Live · Kalshi Weather Sniper")
    data.setdefault("kind", "kalshi_weather_sniper")
    data.setdefault("mode", "live")
    data.setdefault("status", "stopped")
    data.setdefault("order_size_usd", 1.0)
    data.setdefault("outlier_order_usd", data.get("order_size_usd", 1.0))
    data.setdefault("order_limit_usd", data.get("order_size_usd", 1.0))
    data.setdefault("outlier_temperature_offset_degrees", 4)
    data.setdefault("min_edge", 0.01)
    data.setdefault("outlier_take_profit_price", 0.999)
    data.setdefault("weather_outlier_rebuy_tiers_enabled", False)
    data.setdefault("weather_outlier_rebuy_tiers", "1:1,2:2,3:3")
    data.setdefault("weather_outlier_blacklist", [])
    data.setdefault("weather_safety_filter_report_enabled", True)
    data.setdefault("weather_safety_filter_enabled", False)
    data.setdefault("weather_safety_filter_yellow_size_multiplier", 0.2)
    data.setdefault("weather_safety_filter_refresh_seconds", 900)
    data.setdefault("max_orders_per_market", 1)
    data.setdefault("dry_run", True)
    data.setdefault("nws_boundary_veto_enabled", True)
    data.setdefault("nws_boundary_veto_degrees_f", 3.6)
    data.setdefault("series", ALL_KALSHI_HIGH_TEMP_SERIES)
    return data


def market_label(m: KalshiMarket) -> str:
    return m.title or m.ticker


def kalshi_date_code(target: date | None = None) -> str:
    target = target or datetime.now(timezone.utc).date()
    return target.strftime("%y%b%d").upper()


def filter_markets_for_date(markets: list[KalshiMarket], target: date | None = None) -> list[KalshiMarket]:
    code = kalshi_date_code(target)
    return [m for m in markets if f"-{code}-" in m.ticker.upper()]


def best_candidate(markets: list[KalshiMarket], forecast_high_f: float | None, threshold_f: float) -> tuple[KalshiMarket | None, str]:
    # Pick an outlier bracket away from NWS high and with a known ask. This is a
    # scanner/plan path; order placement remains dry-run unless enabled.
    viable: list[tuple[float, KalshiMarket]] = []
    for m in markets:
        temp = m.temp_mid_f
        ask = m.yes_ask
        if temp is None or ask is None:
            continue
        veto = boundary_veto_reason(temp, forecast_high_f, threshold_f)
        if veto:
            continue
        distance = abs(temp - forecast_high_f) if forecast_high_f is not None else 0.0
        viable.append((distance, m))
    if not viable:
        return None, "no candidate passed NWS boundary veto and quote checks"
    viable.sort(key=lambda x: (x[0], -(x[1].yes_ask or 0)), reverse=True)
    return viable[0][1], "selected farthest quoted bracket from NWS high"


class KalshiWeatherSniper:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.cfg = load_config(config_path)
        self.strategy_id = str(self.cfg["id"])
        self.writer = PolybotWriter(os.getenv("KALSHI_WEATHER_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("NAUTILUS_DB_URL"))
        self.client = KalshiHttpClient()

    async def connect(self) -> None:
        await self.writer.connect()
        await self.ensure_strategy_row(self.cfg)

    async def close(self) -> None:
        await self.client.aclose()
        await self.writer.close()

    async def ensure_strategy_row(self, cfg: dict[str, Any]) -> None:
        import json as _json
        market = f"{len(cfg.get('series') or {})} Kalshi daily high-temperature city markets"
        async with self.writer._pool.acquire() as con:  # type: ignore[attr-defined]
            await con.execute(
                """
                INSERT INTO strategies(id, name, kind, market, status, config, mode)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                ON CONFLICT (id) DO UPDATE
                SET name=$2, kind=$3, market=$4, config=$6::jsonb, mode=$7, updated_at=now()
                """,
                self.strategy_id,
                str(cfg["name"]),
                str(cfg["kind"]),
                market,
                str(cfg.get("status") or "stopped"),
                _json.dumps(cfg, default=str),
                str(cfg.get("mode") or "live"),
            )

    async def current_state(self) -> tuple[dict[str, Any], str]:
        async with self.writer._pool.acquire() as con:  # type: ignore[attr-defined]
            row = await con.fetchrow("SELECT config, status FROM strategies WHERE id=$1", self.strategy_id)
        if not row:
            return dict(self.cfg), str(self.cfg.get("status", "stopped"))
        cfg = load_config(self.config_path)
        cfg.update(dict(row["config"] or {}))
        return cfg, str(row["status"] or "stopped")

    async def discover_city_markets(self, city_slug: str, spec: dict[str, Any]) -> list[KalshiMarket]:
        raw = await self.client.list_markets(series_ticker=spec["series_ticker"], status="open", limit=200, limit_pages=3)
        return filter_markets_for_date([parse_market(m) for m in raw if parse_market(m).ticker])

    async def scan_once(self, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        cfg = dict(cfg or self.cfg)
        order_size = min(1.0, _safe_float(cfg.get("outlier_order_usd", cfg.get("order_size_usd")), 1.0))
        threshold_f = _safe_float(cfg.get("nws_boundary_veto_degrees_f"), 3.6)
        rows: list[dict[str, Any]] = []
        plans: list[dict[str, Any]] = []
        for city_slug, spec in (cfg.get("series") or {}).items():
            markets = await self.discover_city_markets(city_slug, spec)
            forecast = None
            forecast_error = None
            try:
                forecast = await nws_daily_high(float(spec["lat"]), float(spec["lon"]))
            except Exception as e:
                forecast_error = str(e)
            candidate, reason = best_candidate(markets, forecast.high_f if forecast else None, threshold_f)
            station = STATIONS.get(city_slug)
            inherited_risk = bool(spec.get("inherited_polymarket_risk_city")) and station is not None
            gate = "GREEN"
            warnings = []
            if forecast_error:
                gate = "YELLOW"
                warnings.append(f"NWS forecast unavailable: {forecast_error}")
            if not inherited_risk:
                warnings.append("No matching Polymarket city risk mapping; Kalshi-only city")
            await self.writer.upsert_weather_safety_filter(
                f"{self.strategy_id}_{city_slug}",
                {
                    "city_slug": city_slug,
                    "city": spec.get("city", city_slug),
                    "station": spec.get("station", ""),
                    "source": "NWS forecast + Kalshi high-temp series",
                    "gate": gate,
                    "reason": forecast_error or "GREEN: NWS forecast available; Kalshi daily high-temp market scanned",
                    "expected_temp_fluctuation_c": None,
                    "weather_codes": [],
                    "weather_code_names": [],
                    "size_multiplier": 1.0,
                    "event_slug": spec.get("series_ticker", ""),
                    "metrics": {
                        "filter_model": "kalshi_weather_nws_forecast_v1",
                        "forecast_high_f": forecast.high_f if forecast else None,
                        "market_count": len(markets),
                        "candidate": candidate.ticker if candidate else None,
                        "candidate_yes_ask": candidate.yes_ask if candidate else None,
                        "inherited_polymarket_risk_city": inherited_risk,
                    },
                    "reasons": [reason] if reason else [],
                    "warnings": warnings,
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                },
                enabled=_safe_bool(cfg.get("weather_safety_filter_enabled"), False),
            )
            row = {
                "city_slug": city_slug,
                "city": spec.get("city", city_slug),
                "series_ticker": spec.get("series_ticker"),
                "market_count": len(markets),
                "nws_high_f": forecast.high_f if forecast else None,
                "gate": gate,
                "inherited_polymarket_risk_city": inherited_risk,
                "warnings": warnings,
                "candidate": candidate.ticker if candidate else None,
                "candidate_title": candidate.title if candidate else None,
                "candidate_yes_ask": candidate.yes_ask if candidate else None,
                "reason": reason,
            }
            rows.append(row)
            if candidate:
                plans.append({**row, "order_size_usd": order_size, "dry_run": _safe_bool(cfg.get("dry_run"), True)})
                await self.writer.upsert_book(
                    self.strategy_id,
                    candidate.ticker,
                    f"{spec.get('city')} · {candidate.title}",
                    [],
                    [{"price": candidate.yes_ask or 0, "size": order_size}],
                    candidate.yes_bid or 0,
                    candidate.yes_ask or 0,
                )
        await self.writer.snapshot_equity(self.strategy_id, STARTING_CASH)
        await self.writer.upsert_position(self.strategy_id, "Kalshi weather scan", "SCANNING", 0, 0, 0, 0)
        await self.writer.log_strategy_event(self.strategy_id, f"Kalshi weather scan: {len(rows)} cities, {len(plans)} dry-run candidate(s)")
        return {"checked_at": datetime.now(timezone.utc).isoformat(), "strategy_id": self.strategy_id, "order_size_usd": order_size, "plans": plans, "cities": rows}

    async def run_forever(self) -> None:
        await self.connect()
        try:
            while True:
                cfg, status = await self.current_state()
                if status == "running":
                    result = await self.scan_once(cfg)
                    logger.info("scan complete: {} plans", len(result["plans"]))
                await asyncio.sleep(_safe_float(cfg.get("poll_seconds"), 300))
        finally:
            await self.close()


async def _amain() -> None:
    load_dotenv()
    load_dotenv(".env.live", override=False)
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/kalshi-weather-sniper-live.yaml")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    bot = KalshiWeatherSniper(Path(args.config))
    if args.once:
        await bot.connect()
        try:
            cfg, _status = await bot.current_state()
            print(json.dumps(await bot.scan_once(cfg), indent=2, default=str))
        finally:
            await bot.close()
    else:
        await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(_amain())
