"""Live runner — real Polymarket feed → real paper strategies → dashboard.

What runs:
- Gamma API: pick N active markets by 24h volume
- Polymarket public CLOB websocket: subscribe to each market's YES token_id
- For each market, instantiate a PaperMM strategy. Every book event feeds the
  strategy, which simulates fills when the real touch crosses its virtual
  quotes and maintains its own position, avg entry, realized & unrealized pnl.
- Writer upserts position state and fills into Neon Postgres, snapshots
  strategy-summed equity every 5s.

No synthetic prices, no fake positions — every number on the dashboard is
derived from a real market event processed by a real strategy.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from pathlib import Path

import httpx
import websockets
import yaml
from dotenv import load_dotenv
from loguru import logger

from polybot.persistence.writer import PolybotWriter
from polybot.strategies.paper_mm import PaperMM

GAMMA = "https://gamma-api.polymarket.com/markets"
WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
STARTING_CASH = 10_000.0


async def pick_markets(n: int) -> list[tuple[str, str, str]]:
    """Return [(question, token_id, slug)] for the top-volume active markets."""
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(GAMMA, params={
            "active": "true", "closed": "false",
            "limit": n * 20, "order": "volume24hr", "ascending": "false",
        })
        r.raise_for_status()
        markets = r.json()

    picks: list[tuple[str, str, str]] = []
    for m in markets:
        tokens = m.get("clobTokenIds")
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        if not tokens:
            continue
        q = (m.get("question") or "").strip()
        if not q:
            continue
        # Skip degenerate markets (near 0 or 1) — PaperMM needs room to quote
        try:
            last = float(m.get("lastTradePrice") or m.get("bestBid") or 0)
        except Exception:
            last = 0
        if not (0.1 < last < 0.9):
            continue
        picks.append((q, str(tokens[0]), m.get("slug", "")))
        if len(picks) >= n:
            break
    return picks


def parse_book(msg: dict) -> tuple[float, float] | None:
    bids = msg.get("bids") or msg.get("buys") or []
    asks = msg.get("asks") or msg.get("sells") or []
    try:
        best_bid = max(float(b["price"]) for b in bids) if bids else None
        best_ask = min(float(a["price"]) for a in asks) if asks else None
    except Exception:
        return None
    if best_bid and best_ask and best_bid < best_ask:
        return best_bid, best_ask
    return None


async def run(config_path: Path) -> None:
    load_dotenv()
    cfg = yaml.safe_load(config_path.read_text()) or {}
    n_markets = int(cfg.get("n_markets", 5))

    writer = PolybotWriter(os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL"))
    await writer.connect()

    # Wipe stale positions/fills so only real strategy output shows
    async with writer._pool.acquire() as con:  # type: ignore[attr-defined]
        await con.execute("TRUNCATE positions")
        await con.execute("TRUNCATE fills")
    logger.info("cleared old positions/fills")

    picks = await pick_markets(n_markets)
    logger.info("selected {} markets", len(picks))
    strategies: dict[str, PaperMM] = {}

    # Seed initial mid from REST book so strategies have state before first ws event
    async with httpx.AsyncClient(timeout=15) as c:
        for q, tok, _ in picks:
            mid = 0.5
            try:
                rb = await c.get("https://clob.polymarket.com/book", params={"token_id": tok})
                if rb.status_code == 200:
                    book = rb.json()
                    bb = max(float(b["price"]) for b in book.get("bids", [])) if book.get("bids") else 0
                    ba = min(float(a["price"]) for a in book.get("asks", [])) if book.get("asks") else 0
                    if 0 < bb < ba < 1:
                        mid = (bb + ba) / 2
            except Exception:
                pass
            s = PaperMM(market=q[:60], token_id=tok,
                        half_spread=0.015, order_size=50, max_inventory=500)
            s.last_mid = mid
            strategies[tok] = s
            logger.info("  strategy → {} | initial mid {}", q[:50], round(mid, 3))

    stop = asyncio.Event()

    def _stop(*_):
        stop.set()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    fill_seq = int(time.time() * 10)

    async def periodic_equity():
        while not stop.is_set():
            total = STARTING_CASH + sum(s.total_pnl for s in strategies.values())
            await writer.snapshot_equity(round(total, 2))
            try:
                await asyncio.wait_for(stop.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    async def flush_state():
        for s in strategies.values():
            st = s.state()
            await writer.upsert_position(
                st["market"], st["side"], st["size"], st["entry"], st["last"], st["pnl"],
            )

    async def periodic_flush():
        while not stop.is_set():
            for s in strategies.values():
                st = s.state()
                await writer.upsert_position(
                    st["market"], st["side"], st["size"], st["entry"], st["last"], st["pnl"],
                )
            try:
                await asyncio.wait_for(stop.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    # Prime dashboard with initial strategy state immediately
    for s in strategies.values():
        st = s.state()
        await writer.upsert_position(
            st["market"], st["side"], st["size"], st["entry"], st["last"], st["pnl"],
        )

    eq_task = asyncio.create_task(periodic_equity())
    flush_task = asyncio.create_task(periodic_flush())

    asset_ids = list(strategies.keys())
    logger.info("connecting Polymarket websocket for {} assets", len(asset_ids))

    while not stop.is_set():
        try:
            async with websockets.connect(WS_MARKET, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"type": "Market", "assets_ids": asset_ids}))
                logger.info("subscribed")
                msg_count = 0
                async for raw in ws:
                    if stop.is_set():
                        break
                    try:
                        msgs = json.loads(raw)
                    except Exception:
                        continue
                    if not isinstance(msgs, list):
                        msgs = [msgs]
                    msg_count += len(msgs)
                    if msg_count <= 5 or msg_count % 50 == 0:
                        logger.info("ws msgs received: {}", msg_count)
                    dirty: dict[str, PaperMM] = {}

                    def _handle(tok: str, bb: float, ba: float) -> None:
                        nonlocal fill_seq
                        strat = strategies.get(tok)
                        if not strat:
                            return
                        new_fills = strat.on_book(bb, ba, time.time())
                        dirty[tok] = strat
                        for f in new_fills:
                            fill_seq += 1
                            asyncio.create_task(writer.record_fill(
                                fill_id=fill_seq,
                                market=strat.market[:40],
                                side=f.side,
                                px=f.px,
                                size=f.size,
                            ))
                            logger.info("FILL {} {} {}@{} → pos {:.0f}",
                                        strat.market[:30], f.side, f.size, f.px, strat.position)

                    for m in msgs:
                        # Shape A: full book snapshot — has bids/asks + asset_id
                        if "bids" in m and "asks" in m:
                            tok = str(m.get("asset_id", ""))
                            book = parse_book(m)
                            if book:
                                _handle(tok, *book)
                            continue
                        # Shape B: price_changes — array of per-asset updates with best_bid/best_ask
                        pcs = m.get("price_changes")
                        if isinstance(pcs, list):
                            latest: dict[str, tuple[float, float]] = {}
                            for pc in pcs:
                                tok = str(pc.get("asset_id", ""))
                                try:
                                    bb = float(pc.get("best_bid", 0) or 0)
                                    ba = float(pc.get("best_ask", 0) or 0)
                                except Exception:
                                    continue
                                if 0 < bb < ba < 1:
                                    latest[tok] = (bb, ba)
                            for tok, (bb, ba) in latest.items():
                                _handle(tok, bb, ba)
                            continue
                        # Shape C: last_trade_price
                        if m.get("event_type") == "last_trade_price" or "price" in m and "side" in m:
                            tok = str(m.get("asset_id", ""))
                            try:
                                px = float(m.get("price", 0))
                                sz = float(m.get("size", 0))
                            except Exception:
                                continue
                            strat = strategies.get(tok)
                            if strat and px > 0:
                                # Treat trade as a micro-book tick: bid=ask=px (paper fills still use quotes from last book)
                                _handle(tok, px, px)
                    for s in dirty.values():
                        st = s.state()
                        await writer.upsert_position(
                            st["market"], st["side"], st["size"], st["entry"], st["last"], st["pnl"],
                        )
        except Exception as e:
            logger.warning("ws disconnected: {} — reconnecting", e)
            try:
                await asyncio.wait_for(stop.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass

    eq_task.cancel()
    flush_task.cancel()
    await flush_state()
    await writer.close()
    logger.info("stopped")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, required=True)
    args = p.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
