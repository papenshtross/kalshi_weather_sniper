"""Live runner — BinaryArbMM on a single binary Polymarket market.

Designed for 2-outcome markets (Up/Down, Yes/No). Subscribes to both token
order books, runs the BinaryArbMM strategy, writes real state and fills to
Postgres so the dashboard reflects it.

Config (YAML):
    event_slug: bitcoin-up-or-down-on-april-9-2026   # Gamma event slug
    threshold: 0.95
    pair_size: 20
    mm_spread: 0.02
    mm_size: 30
    max_inventory: 500

Usage:
    python -m polybot.live.run_binary_arb --config config/binary_arb.yaml
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
from polybot.strategies.binary_arb_mm import BinaryArbMM

WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
STARTING_CASH = 10_000.0


async def resolve_event(slug: str) -> dict:
    """Fetch a binary event by slug and return a dict with yes/no tokens + question."""
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get("https://gamma-api.polymarket.com/events", params={"slug": slug})
        r.raise_for_status()
        events = r.json()
    if not events:
        raise SystemExit(f"no event found for slug={slug}")
    ev = events[0]
    markets = ev.get("markets", [])
    if not markets:
        raise SystemExit("event has no markets")
    m = markets[0]
    toks = m.get("clobTokenIds")
    outs = m.get("outcomes")
    if isinstance(toks, str):
        toks = json.loads(toks)
    if isinstance(outs, str):
        outs = json.loads(outs)
    if not toks or len(toks) < 2:
        raise SystemExit("market is not binary (needs 2 outcomes)")
    return {
        "title": ev.get("title") or m.get("question", slug),
        "yes_token": str(toks[0]),
        "no_token": str(toks[1]),
        "yes_label": outs[0] if outs else "YES",
        "no_label": outs[1] if outs else "NO",
    }


async def rest_book(client: httpx.AsyncClient, token_id: str) -> tuple[float, float] | None:
    try:
        r = await client.get("https://clob.polymarket.com/book", params={"token_id": token_id}, timeout=10)
        if r.status_code != 200:
            return None
        b = r.json()
        bb = max(float(x["price"]) for x in b.get("bids", [])) if b.get("bids") else 0.0
        ba = min(float(x["price"]) for x in b.get("asks", [])) if b.get("asks") else 0.0
        if 0 < bb < ba < 1:
            return bb, ba
    except Exception:
        pass
    return None


async def run(config_path: Path) -> None:
    load_dotenv()
    cfg = yaml.safe_load(config_path.read_text()) or {}

    writer = PolybotWriter(os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL"))
    await writer.connect()

    async with writer._pool.acquire() as con:  # type: ignore[attr-defined]
        await con.execute("TRUNCATE positions")
        await con.execute("TRUNCATE fills")
        await con.execute("TRUNCATE equity_snapshots")
    logger.info("cleared dashboard state")

    ev = await resolve_event(cfg["event_slug"])
    logger.info("market: {}", ev["title"])
    logger.info("  {} token = {}", ev["yes_label"], ev["yes_token"][:20])
    logger.info("  {} token = {}", ev["no_label"], ev["no_token"][:20])

    strat = BinaryArbMM(
        market=ev["title"][:60],
        yes_token=ev["yes_token"],
        no_token=ev["no_token"],
        threshold=float(cfg.get("threshold", 0.95)),
        pair_size=float(cfg.get("pair_size", 20)),
        mm_spread=float(cfg.get("mm_spread", 0.02)),
        mm_size=float(cfg.get("mm_size", 30)),
        max_inventory=float(cfg.get("max_inventory", 500)),
    )
    logger.info("strategy: threshold={} pair_size={} mm_size={}",
                strat.threshold, strat.pair_size, strat.mm_size)

    # Seed with REST book snapshots
    async with httpx.AsyncClient() as c:
        y = await rest_book(c, strat.yes_token)
        n = await rest_book(c, strat.no_token)
    now = time.time()
    if y:
        strat.on_book(strat.yes_token, *y, now)
    if n:
        strat.on_book(strat.no_token, *n, now)
    logger.info("seeded: {} best={}/{}  {} best={}/{}  sum={}",
                ev["yes_label"], strat.yes_bid, strat.yes_ask,
                ev["no_label"], strat.no_bid, strat.no_ask, strat.last_sum)

    stop = asyncio.Event()

    def _stop(*_):
        stop.set()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    fill_seq = int(time.time() * 10)

    async def write_state():
        st = strat.state()
        await writer.upsert_position(
            st["market"], st["side"], st["size"], st["entry"], st["last"], st["pnl"],
        )

    async def periodic_equity():
        while not stop.is_set():
            total = STARTING_CASH + strat.total_pnl
            await writer.snapshot_equity(round(total, 2))
            await write_state()
            try:
                await asyncio.wait_for(stop.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    await write_state()  # immediately show in dashboard
    eq_task = asyncio.create_task(periodic_equity())

    asset_ids = [strat.yes_token, strat.no_token]
    logger.info("connecting websocket…")

    while not stop.is_set():
        try:
            async with websockets.connect(WS_MARKET, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"type": "Market", "assets_ids": asset_ids}))
                logger.info("subscribed to {} + {}", ev["yes_label"], ev["no_label"])
                msg_n = 0

                async for raw in ws:
                    if stop.is_set():
                        break
                    try:
                        msgs = json.loads(raw)
                    except Exception:
                        continue
                    if not isinstance(msgs, list):
                        msgs = [msgs]
                    msg_n += len(msgs)

                    any_update = False
                    for m in msgs:
                        # Full book snapshot
                        if "bids" in m and "asks" in m:
                            tok = str(m.get("asset_id", ""))
                            try:
                                bb = max(float(b["price"]) for b in m["bids"]) if m["bids"] else 0.0
                                ba = min(float(a["price"]) for a in m["asks"]) if m["asks"] else 0.0
                            except Exception:
                                continue
                            if 0 < bb < ba < 1:
                                new_fills = strat.on_book(tok, bb, ba, time.time())
                                any_update = True
                                for f in new_fills:
                                    fill_seq += 1
                                    label = f"{strat.market[:30]} [{f.kind}]"
                                    if f.kind == "ARB":
                                        await writer.record_fill(fill_seq, f"{label} YES", "BUY", f.yes_px, f.size)
                                        fill_seq += 1
                                        await writer.record_fill(fill_seq, f"{label} NO ", "BUY", f.no_px, f.size)
                                        logger.info("★ ARB sum={:.3f} size={} profit=${:.2f} (YES@{} NO@{})",
                                                    strat.last_sum, f.size, f.profit, f.yes_px, f.no_px)
                                    else:
                                        px = f.yes_px if f.yes_px else f.no_px
                                        lbl = "YES" if f.yes_px else "NO"
                                        await writer.record_fill(fill_seq, f"{label} {lbl}", "BUY", px, f.size)
                                        logger.info("MM fill {} {}@{}", lbl, f.size, px)
                            continue

                        # price_changes array
                        pcs = m.get("price_changes")
                        if isinstance(pcs, list):
                            for pc in pcs:
                                tok = str(pc.get("asset_id", ""))
                                try:
                                    bb = float(pc.get("best_bid", 0) or 0)
                                    ba = float(pc.get("best_ask", 0) or 0)
                                except Exception:
                                    continue
                                if 0 < bb < ba < 1:
                                    new_fills = strat.on_book(tok, bb, ba, time.time())
                                    any_update = True
                                    for f in new_fills:
                                        fill_seq += 1
                                        if f.kind == "ARB":
                                            await writer.record_fill(fill_seq, f"{strat.market[:28]} [ARB] YES", "BUY", f.yes_px, f.size)
                                            fill_seq += 1
                                            await writer.record_fill(fill_seq, f"{strat.market[:28]} [ARB] NO ", "BUY", f.no_px, f.size)
                                            logger.info("★ ARB sum={:.3f} size={} profit=${:.2f}", strat.last_sum, f.size, f.profit)
                                        else:
                                            px = f.yes_px if f.yes_px else f.no_px
                                            lbl = "YES" if f.yes_px else "NO"
                                            await writer.record_fill(fill_seq, f"{strat.market[:28]} [MM] {lbl}", "BUY", px, f.size)

                    if any_update:
                        await write_state()
                    if msg_n <= 5 or msg_n % 100 == 0:
                        logger.info("ws msgs={} | YES {}/{}  NO {}/{}  sum={}",
                                    msg_n, strat.yes_bid, strat.yes_ask,
                                    strat.no_bid, strat.no_ask, strat.last_sum)
        except Exception as e:
            logger.warning("ws disconnected: {} — reconnecting", e)
            try:
                await asyncio.wait_for(stop.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass

    eq_task.cancel()
    await write_state()
    await writer.close()
    logger.info("stopped | arbs={} mm_fills={} cash=${:.2f} total_pnl=${:.2f}",
                strat.arb_count, strat.mm_count, strat.cash, strat.total_pnl)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, required=True)
    args = p.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
