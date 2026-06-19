#!/usr/bin/env python3
"""Dry-run weather safety filter report for current live weather outlier cities."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from polybot.live.weather_safety_filter import STATIONS, analyze_city_safety  # noqa: E402


def load_cities(config_dir: Path) -> list[str]:
    out: list[str] = []
    for p in sorted(config_dir.glob("weather-outlier-sniper-*-live.yaml")):
        try:
            cfg = yaml.safe_load(p.read_text()) or {}
        except Exception:
            continue
        city = str(cfg.get("weather_city") or "").strip().lower().replace("_", "-").replace(" ", "-")
        if not city:
            slug = str(cfg.get("market_slug") or "")
            prefix = "auto:weather-high-temp:"
            if slug.startswith(prefix):
                city = slug[len(prefix):]
        if city:
            out.append(city)
    return sorted(set(out))


def make_markdown(results: list[dict[str, Any]]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    counts = {g: sum(1 for r in results if r.get("gate") == g) for g in ["GREEN", "YELLOW", "RED"]}
    lines = [
        f"# Weather safety filter dry run — {now}",
        "",
        "Filter is **not activated** by this report. Live enforcement requires `weather_safety_filter_enabled=true`.",
        "Rule if enabled: RED blocks new BUYs; YELLOW disables same-market re-buy ladder while preserving the normal single order; GREEN unchanged. RED=snow/winter, rain+empirical_bad_city, or empirical_watch_city+snow/heavy-rain/heavy-wind. YELLOW=empirical_watch_city, or non-watch city+heavy-rain/heavy-wind during 10:00-16:00 local. Ordinary rain alone is context only. Static city risk is informational only. Take-profit SELLs are never blocked.",
        f"Summary: GREEN={counts['GREEN']} · YELLOW={counts['YELLOW']} · RED={counts['RED']} · total={len(results)}",
        "",
    ]
    for gate in ["RED", "YELLOW", "GREEN"]:
        group = [r for r in results if r.get("gate") == gate]
        if not group:
            continue
        lines.append(f"## {gate}")
        for r in sorted(group, key=lambda x: x.get("city_slug", "")):
            metrics = r.get("metrics") or {}
            codes = ",".join(str(x) for x in (r.get("weather_codes") or [])) or "none"
            names = ", ".join(r.get("weather_code_names") or []) or "none"
            reason = r.get("reason") or "passes safety filter"
            lines.append(
                f"- **{r.get('city')}** ({r.get('city_slug')}, {r.get('station')}) — {reason}. "
                f"expected_fluctuation={r.get('expected_temp_fluctuation_c')}°C · codes={codes} ({names}) · "
                f"size_if_enabled={int(float(r.get('size_multiplier') or 0)*100)}% · "
                f"forecast_high={metrics.get('forecast_high_c')}°C · ens_spread={metrics.get('ens_spread_c')}°C · "
                f"run_delta={metrics.get('high_run_delta_c')}°C · gust={metrics.get('gust_peak_kmh')}km/h · CAPE={metrics.get('cape_peak')} · PoP={metrics.get('max_pop_peak_pct')}%"
            )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config-dir", type=Path, default=ROOT / "config")
    ap.add_argument("--json", type=Path, default=ROOT / "reports" / "weather_safety_filter_latest.json")
    ap.add_argument("--markdown", type=Path, default=ROOT / "reports" / "weather_safety_filter_latest.md")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--concurrency", type=int, default=2, help="Keep low to avoid Open-Meteo 429s during 51-city dry runs")
    args = ap.parse_args()
    cities = load_cities(args.config_dir)
    if args.limit:
        cities = cities[: args.limit]
    sem = asyncio.Semaphore(max(1, args.concurrency))
    async def wrapped(city: str) -> dict[str, Any]:
        async with sem:
            if city not in STATIONS:
                return {"city_slug": city, "city": city, "station": None, "gate": "RED", "reason": "city missing from station map", "weather_codes": [], "weather_code_names": [], "expected_temp_fluctuation_c": None, "size_multiplier": 0.0, "metrics": {}}
            return await analyze_city_safety(city)
    results = await asyncio.gather(*(wrapped(c) for c in cities))
    order = {"RED": 0, "YELLOW": 1, "GREEN": 2}
    results = sorted(results, key=lambda r: (order.get(r.get("gate"), 9), r.get("city_slug", "")))
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    md = make_markdown(results)
    args.markdown.write_text(md)
    print(md)


if __name__ == "__main__":
    asyncio.run(main())
