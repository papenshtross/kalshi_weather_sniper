#!/usr/bin/env python3
"""Preflight validator for the staged NWWS-OI weather-lock bot.

This is intentionally conservative: it reports every blocker and exits nonzero
unless the deployment remains safely disabled or all explicit live prerequisites
are satisfied. It performs no CLOB orders and no signing.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polybot.live.nwws_weather_execution import load_targets

PLACEHOLDER_MARKERS = ("PLACEHOLDER", "DEMO", "TODO", "")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets-json", default="config/nwws-weather-lock-prism3.targets.example.json")
    ap.add_argument("--expect-live", action="store_true", help="require all live prerequisites instead of validating safe staged mode")
    args = ap.parse_args()

    blockers: list[str] = []
    targets = load_targets(args.targets_json)
    if not targets:
        blockers.append("no targets configured")
    for t in targets:
        token = t.yes_token.upper()
        if any(m in token for m in PLACEHOLDER_MARKERS):
            blockers.append(f"{t.icao.decode()}: yes_token is placeholder/empty")
        if t.max_notional_usdc <= 0:
            blockers.append(f"{t.icao.decode()}: max_notional_usdc <= 0")
        if not (0 < t.price_ceiling <= 0.99):
            blockers.append(f"{t.icao.decode()}: invalid price_ceiling {t.price_ceiling}")

    live_env = os.getenv("POLYBOT_NWWS_LIVE_UNLOCK") == "I_UNDERSTAND_THIS_IS_LIVE"
    nwws_env = bool(os.getenv("NWWS_JID") and os.getenv("NWWS_PASSWORD"))
    if args.expect_live:
        if not live_env:
            blockers.append("missing POLYBOT_NWWS_LIVE_UNLOCK")
        if not nwws_env:
            blockers.append("missing NWWS_JID/NWWS_PASSWORD")

    result = {
        "targets_json": args.targets_json,
        "target_count": len(targets),
        "expect_live": args.expect_live,
        "live_unlock_env": live_env,
        "nwws_credentials_env": nwws_env,
        "blockers": blockers,
        "safe_staged": (not args.expect_live and bool(blockers)),
    }
    print(json.dumps(result, indent=2))
    return 1 if args.expect_live and blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
