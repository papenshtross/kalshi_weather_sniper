#!/usr/bin/env bash
# Backfill N days of Polymarket trades for a given market condition_id.
set -euo pipefail
MARKET="${1:?usage: backfill.sh <condition_id> [days]}"
DAYS="${2:-30}"
python -m polybot.data.goldsky --market "$MARKET" --days "$DAYS" --out data/parquet/
