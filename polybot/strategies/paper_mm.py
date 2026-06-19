"""Paper market-maker strategy.

A self-contained strategy that runs against a real Polymarket L2 feed in
paper mode:

- Quotes symmetric ±half_spread around mid.
- Simulates fills when the real touch price crosses a virtual quote.
- Tracks its own position, average entry, realized and unrealized pnl, cash.
- Reports state via get_state() for the persistence writer.

This is the SAME kind of strategy you'd ship to live — the only difference is
submit() writes to the local book instead of the CLOB.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


Side = Literal["BUY", "SELL"]


@dataclass
class PaperFill:
    ts: float
    side: Side
    px: float
    size: float


@dataclass
class PaperMM:
    market: str
    token_id: str
    half_spread: float = 0.015      # 1.5 cents each side
    order_size: float = 50.0        # shares per quote
    max_inventory: float = 500.0

    # state
    position: float = 0.0
    avg_entry: float = 0.0
    cash: float = 0.0               # realized pnl tally
    bid_quote: float | None = None
    ask_quote: float | None = None
    last_mid: float = 0.0
    fills: list[PaperFill] = field(default_factory=list)

    # ------------------------------------------------------------------ tick

    def on_book(self, best_bid: float, best_ask: float, ts: float) -> list[PaperFill]:
        """Update strategy on a fresh top-of-book. Return any new fills."""
        if not (0 < best_bid < best_ask < 1):
            return []
        mid = (best_bid + best_ask) / 2
        self.last_mid = mid

        new_fills: list[PaperFill] = []

        # ---- simulate fills: if the real market crossed our quote, we're filled ----
        if self.bid_quote is not None and best_ask <= self.bid_quote and self.position < self.max_inventory:
            f = self._fill("BUY", self.bid_quote, self.order_size, ts)
            new_fills.append(f)
        if self.ask_quote is not None and best_bid >= self.ask_quote and self.position > -self.max_inventory:
            f = self._fill("SELL", self.ask_quote, self.order_size, ts)
            new_fills.append(f)

        # ---- re-quote ----
        self.bid_quote = round(mid - self.half_spread, 4)
        self.ask_quote = round(mid + self.half_spread, 4)
        # One-sided if hitting inventory cap
        if self.position >= self.max_inventory:
            self.bid_quote = None
        if self.position <= -self.max_inventory:
            self.ask_quote = None

        return new_fills

    # ------------------------------------------------------------------ accounting

    def _fill(self, side: Side, px: float, size: float, ts: float) -> PaperFill:
        if side == "BUY":
            new_pos = self.position + size
            if self.position >= 0:
                # adding to long
                total = self.position * self.avg_entry + size * px
                self.avg_entry = total / new_pos if new_pos else 0
            else:
                # covering short
                closed = min(size, -self.position)
                self.cash += (self.avg_entry - px) * closed
                remainder = size - closed
                if remainder > 0:
                    self.avg_entry = px
            self.position = new_pos
        else:  # SELL
            new_pos = self.position - size
            if self.position <= 0:
                total = (-self.position) * self.avg_entry + size * px
                self.avg_entry = total / (-new_pos) if new_pos else 0
            else:
                closed = min(size, self.position)
                self.cash += (px - self.avg_entry) * closed
                remainder = size - closed
                if remainder > 0:
                    self.avg_entry = px
            self.position = new_pos

        f = PaperFill(ts=ts, side=side, px=px, size=size)
        self.fills.append(f)
        return f

    # ------------------------------------------------------------------ reports

    @property
    def unrealized(self) -> float:
        if self.position == 0 or self.last_mid == 0:
            return 0.0
        if self.position > 0:
            return (self.last_mid - self.avg_entry) * self.position
        return (self.avg_entry - self.last_mid) * (-self.position)

    @property
    def total_pnl(self) -> float:
        return self.cash + self.unrealized

    def state(self) -> dict:
        if self.position == 0:
            side = "FLAT"
        elif self.position > 0:
            side = "LONG"
        else:
            side = "SHORT"
        return {
            "market": self.market,
            "side": side,
            "size": round(abs(self.position), 2),
            "entry": round(self.avg_entry, 4) if self.avg_entry else round(self.last_mid, 4),
            "last": round(self.last_mid, 4),
            "pnl": round(self.total_pnl, 2),
        }
