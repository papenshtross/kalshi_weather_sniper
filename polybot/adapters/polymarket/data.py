"""Polymarket market data client.

STUB. Wire to nautilus_trader.live.data_client.LiveMarketDataClient once you pin
a Nautilus version. The interface contract below is what Nautilus expects.

Responsibilities:
- subscribe_order_book_deltas(instrument_id)  → push L2 deltas as OrderBookDelta
- subscribe_trade_ticks(instrument_id)        → push PolymarketTrade as TradeTick
- subscribe_quote_ticks(instrument_id)        → derived top-of-book quotes
- request_bars / request_trade_ticks          → historical (delegate to data/goldsky.py)

Transport: Polymarket exposes a public websocket at wss://ws-subscriptions-clob.polymarket.com/ws/market
Auth: not required for public market data; required for user channel (fills/orders).
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

import httpx
from loguru import logger

from polybot.adapters.polymarket.client import PolymarketHttpClient

WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_USER = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


class PolymarketDataClient:
    """Framework-agnostic market data pump.

    Nautilus integration: a thin LiveMarketDataClient subclass will instantiate
    this and translate events into Nautilus OrderBookDelta / TradeTick messages.
    """

    def __init__(self, http: PolymarketHttpClient | None = None) -> None:
        self.http = http or PolymarketHttpClient()
        self._tasks: list[asyncio.Task] = []
        self._handlers: dict[str, list[Callable[[dict[str, Any]], None]]] = {
            "book": [],
            "price_change": [],
            "trade": [],
        }

    # ------------------------------------------------------------------ pub/sub

    def on(self, event: str, handler: Callable[[dict[str, Any]], None]) -> None:
        self._handlers.setdefault(event, []).append(handler)

    def _emit(self, event: str, payload: dict[str, Any]) -> None:
        for h in self._handlers.get(event, []):
            try:
                h(payload)
            except Exception:
                logger.exception("handler error on %s", event)

    # ------------------------------------------------------------------ subscribe

    async def subscribe_markets(self, asset_ids: list[str]) -> None:
        """Open the market websocket and subscribe to the given token ids."""
        import websockets  # local import so tests without ws work

        async def _run() -> None:
            while True:
                try:
                    async with websockets.connect(WS_MARKET, ping_interval=20) as ws:
                        await ws.send(json.dumps({"type": "Market", "assets_ids": asset_ids}))
                        logger.info("Subscribed to %d Polymarket assets", len(asset_ids))
                        async for raw in ws:
                            msgs = json.loads(raw)
                            if isinstance(msgs, dict):
                                msgs = [msgs]
                            for m in msgs:
                                ev = m.get("event_type") or m.get("type", "book").lower()
                                self._emit(ev, m)
                except Exception as e:
                    logger.warning("market ws disconnected: %s — reconnecting", e)
                    await asyncio.sleep(2)

        self._tasks.append(asyncio.create_task(_run()))

    # ------------------------------------------------------------------ REST helpers

    async def get_book(self, token_id: str) -> dict[str, Any]:
        r = await httpx.AsyncClient().get(
            f"{self.http.cfg.host}/book", params={"token_id": token_id}, timeout=10.0
        )
        r.raise_for_status()
        return r.json()

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
