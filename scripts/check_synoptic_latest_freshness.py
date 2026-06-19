#!/usr/bin/env python3
"""Pull Synoptic Data latest observations and assess feed freshness.

This intentionally uses Synoptic's REST pull API (``/v2/stations/latest``), not a
Synoptic push/webhook stream. It reports the age of the observation timestamp
returned by the API relative to local receipt time.

Example:
    SYNOPTIC_API_TOKEN=... python scripts/check_synoptic_latest_freshness.py \
        --stations KLGA,KJFK,KEWR --vars air_temp --max-age-sec 1800
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

DEFAULT_BASE_URL = "https://api.synopticdata.com/v2/stations/latest"


def _parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Synoptic sometimes exposes epochs in seconds; tolerate ms too.
        ts = float(value)
        if ts > 10_000_000_000:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        # Synoptic API examples are ISO; keep one compact fallback for common
        # YYYY-mm-dd HH:MM UTC-ish payloads.
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_latest_observation(station: dict[str, Any], preferred_vars: list[str]) -> dict[str, Any]:
    obs = station.get("OBSERVATIONS") or {}
    candidates: list[tuple[str, Any, datetime]] = []

    def add_candidate(name: str, node: Any) -> None:
        if isinstance(node, dict):
            dt = _parse_time(node.get("date_time") or node.get("datetime") or node.get("time") or node.get("timestamp"))
            val = node.get("value")
        else:
            dt = None
            val = node
        if dt is not None:
            candidates.append((name, val, dt))

    for var in preferred_vars:
        # Common Synoptic latest keys look like air_temp_value_1.
        for key, node in obs.items():
            if key == var or key.startswith(f"{var}_"):
                add_candidate(key, node)

    if not candidates:
        for key, node in obs.items():
            add_candidate(key, node)

    if not candidates:
        return {"variable": None, "value": None, "observed_at": None}

    name, value, observed_at = max(candidates, key=lambda item: item[2])
    return {"variable": name, "value": value, "observed_at": observed_at.isoformat().replace("+00:00", "Z")}


def assess_payload(payload: dict[str, Any], received_at: datetime, preferred_vars: list[str], max_age_sec: float) -> dict[str, Any]:
    rows = []
    for station in payload.get("STATION") or []:
        latest = _extract_latest_observation(station, preferred_vars)
        observed_at = _parse_time(latest.get("observed_at"))
        age_sec = None if observed_at is None else (received_at - observed_at).total_seconds()
        rows.append(
            {
                "station": station.get("STID") or station.get("stid"),
                "name": station.get("NAME") or station.get("name"),
                "variable": latest.get("variable"),
                "value": latest.get("value"),
                "observed_at": latest.get("observed_at"),
                "received_at": received_at.isoformat().replace("+00:00", "Z"),
                "age_sec": age_sec,
                "fresh": bool(age_sec is not None and age_sec <= max_age_sec and age_sec >= -60),
            }
        )
    return {
        "received_at": received_at.isoformat().replace("+00:00", "Z"),
        "max_age_sec": max_age_sec,
        "station_count": len(rows),
        "fresh_count": sum(1 for row in rows if row["fresh"]),
        "stale_count": sum(1 for row in rows if not row["fresh"]),
        "stations": rows,
    }


def fetch_latest(base_url: str, token: str, stations: str, variables: str, timeout_sec: float) -> tuple[dict[str, Any], datetime, float]:
    params = {
        "token": token,
        "stid": stations,
        "vars": variables,
        "units": "temp|C",
        "output": "json",
    }
    url = base_url + "?" + urllib.parse.urlencode(params)
    started = time.perf_counter()
    with urllib.request.urlopen(url, timeout=timeout_sec) as resp:
        body = resp.read()
    latency_ms = (time.perf_counter() - started) * 1000.0
    received_at = datetime.now(timezone.utc)
    return json.loads(body), received_at, latency_ms


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stations", default="KLGA", help="Comma-separated station IDs, e.g. KLGA,KJFK,KEWR")
    parser.add_argument("--vars", default="air_temp", help="Comma-separated Synoptic variables to request")
    parser.add_argument("--max-age-sec", type=float, default=1800.0, help="Freshness threshold for observation age")
    parser.add_argument("--timeout-sec", type=float, default=10.0)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--token", default=os.getenv("SYNOPTIC_API_TOKEN") or os.getenv("SYNOPTIC_TOKEN"))
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)

    if not args.token:
        print("Missing Synoptic token. Set SYNOPTIC_API_TOKEN or pass --token.", file=sys.stderr)
        return 2

    try:
        payload, received_at, latency_ms = fetch_latest(args.base_url, args.token, args.stations, args.vars, args.timeout_sec)
    except urllib.error.HTTPError as exc:
        print(f"Synoptic API HTTP {exc.code}: {exc.reason}", file=sys.stderr)
        try:
            print(exc.read().decode("utf-8", "replace")[:1000], file=sys.stderr)
        except Exception:
            pass
        return 1
    except Exception as exc:
        print(f"Synoptic API request failed: {exc}", file=sys.stderr)
        return 1

    report = assess_payload(payload, received_at, [v.strip() for v in args.vars.split(",") if v.strip()], args.max_age_sec)
    report["http_latency_ms"] = round(latency_ms, 1)
    report["source"] = "synoptic_pull_api:/v2/stations/latest"
    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True))
    return 0 if report["fresh_count"] == report["station_count"] and report["station_count"] > 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
