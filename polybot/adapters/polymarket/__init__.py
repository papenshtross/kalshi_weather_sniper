"""Polymarket adapter for Nautilus Trader.

Exposes:
    PolymarketDataClient     — market data (books, trades, quotes)
    PolymarketExecutionClient — order submission / cancellation / fill routing
    PolymarketInstrumentProvider — market discovery and instrument mapping

The adapter wraps py-clob-client-v2 and py-order-utils. I/O primitives (websocket
subscription loop, order signing, rate limiting) follow the patterns used in
warproxxx/poly-maker.
"""
from polybot.adapters.polymarket.client import PolymarketHttpClient
from polybot.adapters.polymarket.data import PolymarketDataClient
from polybot.adapters.polymarket.execution import PolymarketExecutionClient
from polybot.adapters.polymarket.instruments import PolymarketInstrumentProvider

__all__ = [
    "PolymarketHttpClient",
    "PolymarketDataClient",
    "PolymarketExecutionClient",
    "PolymarketInstrumentProvider",
]
