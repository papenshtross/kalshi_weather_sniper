"""Minimal market-making example strategy.

Quotes both sides around mid at a configurable half-spread, with a max
inventory limit. Same class is loaded by backtest, paper, and live runners.

Written against the nautilus_trader.trading.strategy.Strategy base class.
Kept deliberately small so you can copy it as a template for real strategies.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

try:
    from nautilus_trader.trading.strategy import Strategy, StrategyConfig
    from nautilus_trader.model.data import QuoteTick
    from nautilus_trader.model.enums import OrderSide, TimeInForce
    from nautilus_trader.model.identifiers import InstrumentId
    from nautilus_trader.model.objects import Price, Quantity
except ImportError:  # pragma: no cover — allow import without nautilus installed
    class _FallbackNautilusBase:
        def __init_subclass__(cls, **kwargs: Any) -> None:
            # Nautilus config classes accept dataclass-style keywords such as
            # `frozen=True`. Plain `object` does not, but smoke imports should
            # still work on machines without nautilus installed.
            super().__init_subclass__()

    Strategy = _FallbackNautilusBase  # type: ignore
    StrategyConfig = _FallbackNautilusBase  # type: ignore
    QuoteTick = Any  # type: ignore


class ExampleMMConfig(StrategyConfig, frozen=True):  # type: ignore[misc]
    instrument_id: str
    half_spread_bps: int = 50         # 0.5%
    order_size: Decimal = Decimal("10")
    max_inventory: Decimal = Decimal("100")
    refresh_secs: int = 5


class ExampleMM(Strategy):  # type: ignore[misc]
    """Quotes symmetric ±half_spread around mid, caps inventory."""

    def __init__(self, config: ExampleMMConfig) -> None:
        super().__init__(config)
        self.instrument_id = InstrumentId.from_str(config.instrument_id)  # type: ignore[name-defined]
        self._hs = Decimal(config.half_spread_bps) / Decimal(10_000)

    # ------------------------------------------------------------------ lifecycle

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.instrument_id)
        self.log.info(f"ExampleMM started on {self.instrument_id}")

    def on_stop(self) -> None:
        self.cancel_all_orders(self.instrument_id)

    # ------------------------------------------------------------------ signal

    def on_quote_tick(self, tick: QuoteTick) -> None:
        mid = (Decimal(str(tick.bid_price)) + Decimal(str(tick.ask_price))) / 2
        bid = mid * (Decimal(1) - self._hs)
        ask = mid * (Decimal(1) + self._hs)

        pos = self.portfolio.net_position(self.instrument_id)
        if pos >= self.config.max_inventory:
            self._quote_one_side(OrderSide.SELL, ask)
        elif pos <= -self.config.max_inventory:
            self._quote_one_side(OrderSide.BUY, bid)
        else:
            self._quote_two_sided(bid, ask)

    # ------------------------------------------------------------------ helpers

    def _quote_two_sided(self, bid: Decimal, ask: Decimal) -> None:
        self.cancel_all_orders(self.instrument_id)
        self.submit_order(
            self.order_factory.limit(
                instrument_id=self.instrument_id,
                order_side=OrderSide.BUY,
                quantity=Quantity.from_str(str(self.config.order_size)),
                price=Price.from_str(f"{bid:.4f}"),
                time_in_force=TimeInForce.GTC,
            )
        )
        self.submit_order(
            self.order_factory.limit(
                instrument_id=self.instrument_id,
                order_side=OrderSide.SELL,
                quantity=Quantity.from_str(str(self.config.order_size)),
                price=Price.from_str(f"{ask:.4f}"),
                time_in_force=TimeInForce.GTC,
            )
        )

    def _quote_one_side(self, side: "OrderSide", px: Decimal) -> None:
        self.cancel_all_orders(self.instrument_id)
        self.submit_order(
            self.order_factory.limit(
                instrument_id=self.instrument_id,
                order_side=side,
                quantity=Quantity.from_str(str(self.config.order_size)),
                price=Price.from_str(f"{px:.4f}"),
                time_in_force=TimeInForce.GTC,
            )
        )
