"""Polymarket execution client.

STUB. Wire to nautilus_trader.live.execution_client.LiveExecutionClient.

Responsibilities (Nautilus contract):
- submit_order     → place LIMIT/MARKET order via CLOB
- modify_order     → cancel+replace (CLOB doesn't support amend)
- cancel_order     → CLOB cancel
- cancel_all_orders
- generate_fill_reports / generate_order_status_reports → reconciliation

Uses py-clob-client-v2 (L2 auth) under the hood. Signing is handled inside the SDK
via py-order-utils.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Any

from loguru import logger

from polybot.adapters.polymarket.client import PolymarketHttpClient

try:
    from py_clob_client_v2.clob_types import (
        MarketOrderArgs,
        OrderArgs,
        OrderType,
        PartialCreateOrderOptions,
        PostOrdersV2Args,
        OpenOrderParams,
        OrderPayload,
        TradeParams,
    )
    from py_clob_client_v2.order_builder.constants import BUY, SELL
except ImportError:  # pragma: no cover
    MarketOrderArgs = None  # type: ignore
    OrderArgs = None  # type: ignore
    OrderType = None  # type: ignore
    PartialCreateOrderOptions = None  # type: ignore
    PostOrdersV2Args = None  # type: ignore
    OpenOrderParams = None  # type: ignore
    OrderPayload = None  # type: ignore
    TradeParams = None  # type: ignore
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class PolyOrder:
    token_id: str
    side: str           # "BUY" | "SELL"
    price: Decimal      # 0 < p < 1
    size: Decimal       # in shares (= pUSD when p=1)
    order_type: str = "GTC"  # GTC, FOK, GTD, FAK
    post_only: bool = False
    # Default False preserves existing live strategies: BUY FOK/FAK uses the SDK
    # market-order path with an explicit pUSD amount. Set True for arb/sniper
    # flows that intentionally submit share-sized limit FOK orders, which can be
    # below $1 notional when the outcome price is low if the CLOB accepts the
    # market's orderMinSize in shares.
    use_limit_order: bool = False
    # CLOB V2 signing validates price increments with per-market metadata.
    # Always pass Gamma/CLOB market tick_size and neg_risk into orders when known.
    tick_size: str = "0.01"
    neg_risk: bool = False
    # V2 order attribution fields. The SDK defaults to bytes32 zero when omitted.
    builder_code: str | None = None
    metadata: str | None = None
    # SDK field name is still user_usdc_balance, but CLOB V2 collateral is pUSD.
    user_pusd_balance: Decimal | None = None


class PolymarketExecutionClient:
    def __init__(self, http: PolymarketHttpClient | None = None) -> None:
        self.http = http or PolymarketHttpClient()

    def _price_tick(self, order: PolyOrder) -> Decimal:
        try:
            tick = Decimal(str(order.tick_size or "0.01"))
        except Exception:
            tick = Decimal("0.01")
        if tick not in {Decimal("0.1"), Decimal("0.01"), Decimal("0.001"), Decimal("0.0001")}:
            logger.warning("Unsupported CLOB V2 tick_size={} for {}; falling back to 0.01", order.tick_size, order.token_id)
            tick = Decimal("0.01")
        return tick

    def _normalize_order(self, order: PolyOrder) -> PolyOrder:
        tick = self._price_tick(order)
        order_type_name = str(order.order_type or "").upper()
        # Marketable BUY limit/FOK orders are validated by CLOB like market buys:
        # maker/notional must be cents-denominated. Force cent-or-coarser limit
        # prices for this path; the arb planner also uses integer shares so
        # price*size has at most 2 decimals.
        if order.side.upper() == "BUY" and order.use_limit_order and order_type_name in {"FOK", "FAK"}:
            tick = max(tick, Decimal("0.01"))
        rounding = ROUND_UP if order.side.upper() == "BUY" else ROUND_DOWN
        price = Decimal(str(order.price)).quantize(tick, rounding=rounding)
        price = max(Decimal("0.001"), min(Decimal("0.999"), price))
        size_quant = Decimal("1") if order.side.upper() == "BUY" and order.use_limit_order and order_type_name in {"FOK", "FAK"} else Decimal("0.0001")
        size = Decimal(str(order.size)).quantize(size_quant, rounding=ROUND_DOWN)

        return PolyOrder(
            token_id=order.token_id,
            side=order.side,
            price=price,
            size=size,
            order_type=order.order_type,
            post_only=order.post_only,
            use_limit_order=order.use_limit_order,
            tick_size=str(tick),
            neg_risk=order.neg_risk,
            builder_code=order.builder_code,
            metadata=order.metadata,
            user_pusd_balance=order.user_pusd_balance,
        )

    def _order_options(self, order: PolyOrder) -> Any:
        if PartialCreateOrderOptions is None:
            raise RuntimeError("py-clob-client-v2 order options not installed")
        return PartialCreateOrderOptions(tick_size=order.tick_size, neg_risk=order.neg_risk)

    def _market_buy_amount(self, order: PolyOrder) -> Decimal:
        """Return the pUSD maker amount for a FOK/FAK BUY market order.

        Polymarket validates market BUY orders with maker/notional precision <= 2
        decimals and taker/share precision <= 4 decimals. The py-clob limit-order
        builder can emit a 3-5 decimal maker amount for BUY FOK orders at prices
        such as 0.427, which the CLOB rejects. Use the SDK market-order builder
        for live taker buys so the amount is an explicit cents-denominated stake.
        """
        amount = (Decimal(str(order.price)) * Decimal(str(order.size))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        return amount

    def _market_buy_price(self, order: PolyOrder) -> Decimal:
        """Return a tick-valid worst price for a market BUY.

        CLOB V2 validates the limit against the supplied market tick. Round BUY
        limits up so a best-ask signal remains crossable while avoiding extra
        slippage from always forcing a 1-cent tick on 0.001-tick markets.
        """
        tick = self._price_tick(order)
        return min(
            Decimal("0.999"),
            Decimal(str(order.price)).quantize(tick, rounding=ROUND_UP),
        )

    # ------------------------------------------------------------------ place

    def _signed_order(self, order: PolyOrder) -> tuple[Any, Any, bool]:
        if OrderArgs is None:
            raise RuntimeError("py-clob-client-v2 not installed")
        normalized = self._normalize_order(order)
        order_type_name = normalized.order_type.upper()
        order_type = getattr(OrderType, order_type_name, order_type_name)

        if normalized.side.upper() == "BUY" and order_type_name in {"FOK", "FAK"} and not normalized.use_limit_order:
            if MarketOrderArgs is None or PartialCreateOrderOptions is None:
                raise RuntimeError("py-clob-client-v2 market order types not installed")
            amount = self._market_buy_amount(order)
            if amount <= 0:
                raise ValueError(f"BUY order amount floors to zero: {order}")
            market_price = self._market_buy_price(normalized)
            kwargs = {
                "token_id": normalized.token_id,
                "amount": float(amount),
                "price": float(market_price),
                "side": BUY,
                "order_type": order_type,
            }
            if normalized.builder_code:
                kwargs["builder_code"] = normalized.builder_code
            if normalized.metadata:
                kwargs["metadata"] = normalized.metadata
            if normalized.user_pusd_balance is not None:
                # py-clob-client-v2 names this field user_usdc_balance even though
                # CLOB V2 collateral is pUSD. Passing it lets the SDK do fee-aware
                # market-buy sizing when the caller knows the balance.
                kwargs["user_usdc_balance"] = float(normalized.user_pusd_balance)
            market_args = MarketOrderArgs(**kwargs)
            signed = self.http.clob.create_market_order(market_args, self._order_options(normalized))
            logger.info("sign market-buy {} -> {} amount={}", order, normalized, amount)
            return signed, order_type, normalized.post_only

        kwargs = {
            "token_id": normalized.token_id,
            "price": float(normalized.price),
            "size": float(normalized.size),
            "side": BUY if normalized.side.upper() == "BUY" else SELL,
        }
        if normalized.builder_code:
            kwargs["builder_code"] = normalized.builder_code
        if normalized.metadata:
            kwargs["metadata"] = normalized.metadata
        args = OrderArgs(**kwargs)
        signed = self.http.clob.create_order(args, self._order_options(normalized))
        return signed, order_type, normalized.post_only

    def submit_batch(self, orders: list[PolyOrder]) -> list[dict[str, Any]]:
        """Sign and post multiple orders through CLOB postOrders.

        This reduces YES/NO leg latency but is not atomic; callers must handle mixed
        success/failure responses.
        """
        if PostOrdersV2Args is None:
            raise RuntimeError("py-clob-client-v2 batch order type not installed")
        post_args = []
        for order in orders:
            signed, order_type, post_only = self._signed_order(order)
            if post_only:
                logger.warning("CLOB V2 post_orders applies post_only at request level; per-order post_only requested for {}", order)
            post_args.append(PostOrdersV2Args(order=signed, orderType=order_type))
        resp = self.http.clob.post_orders(post_args, post_only=any(o.post_only for o in orders))
        logger.info("submit batch {} orders: {}", len(orders), resp)
        return resp

    def submit(self, order: PolyOrder) -> dict[str, Any]:
        """Sign and post an order. Returns the CLOB response dict."""
        signed, order_type, post_only = self._signed_order(order)
        resp = self.http.clob.post_order(signed, order_type, post_only=post_only)
        logger.info("submit {}: {}", order, resp)
        return resp

    def cancel(self, order_id: str) -> dict[str, Any]:
        if OrderPayload is None:
            raise RuntimeError("py-clob-client-v2 order payload type not installed")
        return self.http.clob.cancel_order(OrderPayload(orderID=order_id))

    def cancel_all(self) -> dict[str, Any]:
        return self.http.clob.cancel_all()

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self.http.clob.get_order(order_id)

    def open_orders(self, market: str | None = None) -> list[dict[str, Any]]:
        if market:
            if OpenOrderParams is None:
                raise RuntimeError("py-clob-client-v2 open order params type not installed")
            return self.http.clob.get_open_orders(OpenOrderParams(market=market))
        return self.http.clob.get_open_orders()

    def get_trades(self, **params: Any) -> list[dict[str, Any]]:
        if TradeParams is None:
            raise RuntimeError("py-clob-client-v2 not installed")
        return self.http.clob.get_trades(TradeParams(**params))
