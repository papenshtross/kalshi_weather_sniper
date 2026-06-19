"""Polymarket market discovery + mapping to Nautilus Instruments.

A Polymarket "market" = a condition with typically 2 outcomes (YES/NO),
each outcome is an ERC-1155 token with its own token_id, traded on the CLOB
as a 0..1 priced asset with tick size 0.01 (default) or 0.001 (enhanced).

We model each outcome token as one Nautilus Instrument with:
    instrument_id = InstrumentId(symbol=f"{slug}-{outcome}", venue="POLYMARKET")
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from polybot.adapters.polymarket.client import PolymarketHttpClient


@dataclass
class PolyInstrument:
    condition_id: str
    token_id: str
    question: str
    slug: str
    outcome: str  # "YES" / "NO"
    tick_size: float
    min_order_size: float

    @property
    def symbol(self) -> str:
        return f"{self.slug}-{self.outcome}"


class PolymarketInstrumentProvider:
    def __init__(self, http: PolymarketHttpClient | None = None) -> None:
        self.http = http or PolymarketHttpClient()
        self._cache: dict[str, PolyInstrument] = {}

    async def load_all(self, active_only: bool = True, limit: int = 500) -> list[PolyInstrument]:
        markets = await self.http.gamma_markets(active=active_only, limit=limit, closed=False)
        out: list[PolyInstrument] = []
        for m in markets:
            tokens = m.get("clobTokenIds")
            outcomes = m.get("outcomes")
            if not tokens or not outcomes:
                continue
            if isinstance(tokens, str):
                import json
                tokens = json.loads(tokens)
                outcomes = json.loads(outcomes)
            for tok, oc in zip(tokens, outcomes):
                inst = PolyInstrument(
                    condition_id=m["conditionId"],
                    token_id=str(tok),
                    question=m.get("question", ""),
                    slug=m.get("slug", ""),
                    outcome=oc,
                    tick_size=float(m.get("orderPriceMinTickSize", 0.01)),
                    min_order_size=float(m.get("orderMinSize", 5)),
                )
                self._cache[inst.symbol] = inst
                out.append(inst)
        return out

    def get(self, symbol: str) -> PolyInstrument | None:
        return self._cache.get(symbol)
