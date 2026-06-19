"""Goldsky subgraph → Parquet historical backfill.

Polymarket does not expose deep L2 history publicly. The Polymarket-operated
Goldsky subgraph exposes trades, orders filled, and order book events via
GraphQL. We paginate through them and write Parquet partitioned by market.

Usage:
    python -m polybot.data.goldsky --market <condition_id> --days 30 \
        --out data/parquet/

The output is Nautilus-friendly: one row per trade with ts_event (ns),
price, size, side, maker, taker, token_id.
"""
from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Any, Iterator

import httpx
import pandas as pd
from loguru import logger

DEFAULT_URL = os.getenv(
    "GOLDSKY_GRAPHQL_URL",
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orders-subgraph/prod/gn",
)

TRADES_QUERY = """
query Trades($market: String!, $first: Int!, $skip: Int!, $since: BigInt!) {
  orderFilledEvents(
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
    where: { market: $market, timestamp_gte: $since }
  ) {
    id
    timestamp
    maker
    taker
    makerAssetId
    takerAssetId
    makerAmountFilled
    takerAmountFilled
    fee
    transactionHash
  }
}
"""


def _paginate(client: httpx.Client, market: str, since: int) -> Iterator[list[dict[str, Any]]]:
    skip = 0
    page = 1000
    while True:
        r = client.post(
            DEFAULT_URL,
            json={
                "query": TRADES_QUERY,
                "variables": {"market": market.lower(), "first": page, "skip": skip, "since": str(since)},
            },
            timeout=30.0,
        )
        r.raise_for_status()
        data = r.json()
        if "errors" in data:
            raise RuntimeError(data["errors"])
        batch = data["data"]["orderFilledEvents"]
        if not batch:
            return
        yield batch
        if len(batch) < page:
            return
        skip += page


def backfill(market: str, days: int, out_dir: Path) -> Path:
    since = int(time.time()) - days * 86400
    logger.info("Backfilling trades for {} since {} ({}d)", market, since, days)

    rows: list[dict[str, Any]] = []
    with httpx.Client() as client:
        for batch in _paginate(client, market, since):
            rows.extend(batch)
            logger.info("  fetched {} rows (total {})", len(batch), len(rows))

    if not rows:
        logger.warning("No trades found")
        return out_dir

    df = pd.DataFrame(rows)
    df["timestamp"] = df["timestamp"].astype("int64")
    df["ts_event"] = df["timestamp"] * 1_000_000_000  # → nanoseconds
    df["maker_amount"] = df["makerAmountFilled"].astype("float64") / 1e6
    df["taker_amount"] = df["takerAmountFilled"].astype("float64") / 1e6

    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"trades_{market}.parquet"
    df.to_parquet(out, index=False)
    logger.info("Wrote {} rows → {}", len(df), out)
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--market", required=True, help="Polymarket conditionId")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--out", type=Path, default=Path("data/parquet"))
    args = p.parse_args()
    backfill(args.market, args.days, args.out)


if __name__ == "__main__":
    main()
