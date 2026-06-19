from decimal import Decimal

from polybot.adapters.polymarket.client import PolymarketConfig
from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient


class DummyHttp:
    clob = None


class DummyClob:
    def __init__(self):
        self.market_orders = []
        self.limit_orders = []
        self.posted = []

    def create_market_order(self, args, options=None):
        self.market_orders.append((args, options))
        return {"kind": "market", "args": args, "options": options}

    def create_order(self, args, options=None):
        self.limit_orders.append((args, options))
        return {"kind": "limit", "args": args, "options": options}

    def post_order(self, signed, order_type, post_only=False):
        self.posted.append((signed, order_type, post_only))
        return {"success": True, "signed": signed, "order_type": order_type, "post_only": post_only}

    def post_orders(self, args, post_only=False):
        self.posted.append((args, post_only))
        return [{"success": True, "order": x.order, "order_type": x.orderType, "post_only": post_only} for x in args]


class DummyHttpWithClob:
    def __init__(self):
        self.clob = DummyClob()


def test_normalize_order_uses_clob_v2_tick_size_and_share_precision():
    client = PolymarketExecutionClient(http=DummyHttp())
    buy = PolyOrder(
        token_id="tok",
        side="BUY",
        price=Decimal("0.3149"),
        size=Decimal("4.54559"),
        order_type="FOK",
        tick_size="0.01",
    )
    fine_tick_buy = PolyOrder(
        token_id="tok",
        side="BUY",
        price=Decimal("0.3149"),
        size=Decimal("4.54559"),
        order_type="FOK",
        tick_size="0.001",
    )

    normalized = client._normalize_order(buy)
    fine_normalized = client._normalize_order(fine_tick_buy)

    assert normalized.price == Decimal("0.32")
    assert fine_normalized.price == Decimal("0.315")
    assert normalized.size == Decimal("4.5455")


def test_buy_limit_fok_precision_normalizes_to_cent_price_and_integer_shares():
    client = PolymarketExecutionClient(http=DummyHttp())
    order = PolyOrder(
        token_id="tok",
        side="BUY",
        price=Decimal("0.427"),
        size=Decimal("5.0276"),
        order_type="FOK",
        use_limit_order=True,
        tick_size="0.001",
    )

    normalized = client._normalize_order(order)

    assert normalized.price == Decimal("0.43")
    assert normalized.size == Decimal("5")
    assert normalized.price * normalized.size == Decimal("2.15")


def test_market_buy_amount_is_cents_denominated():
    client = PolymarketExecutionClient(http=DummyHttp())
    order = PolyOrder(
        token_id="tok",
        side="BUY",
        price=Decimal("0.427"),
        size=Decimal("2.3419"),
        order_type="FOK",
        post_only=False,
    )

    assert client._market_buy_amount(order) == Decimal("1.00")
    assert client._market_buy_price(client._normalize_order(order)) == Decimal("0.43")
    fine_tick_order = PolyOrder(token_id="tok", side="BUY", price=Decimal("0.427"), size=Decimal("2.3419"), order_type="FOK", tick_size="0.001")
    assert client._market_buy_price(client._normalize_order(fine_tick_order)) == Decimal("0.427")


def test_buy_fok_uses_market_order_builder_with_cent_amount_and_cent_tick():
    http = DummyHttpWithClob()
    client = PolymarketExecutionClient(http=http)

    resp = client.submit(
        PolyOrder(
            token_id="tok",
            side="BUY",
            price=Decimal("0.427"),
            size=Decimal("2.3419"),
            order_type="FOK",
            post_only=False,
        )
    )

    assert resp["success"] is True
    assert len(http.clob.market_orders) == 1
    assert len(http.clob.limit_orders) == 0
    args, options = http.clob.market_orders[0]
    assert args.token_id == "tok"
    assert args.side == "BUY"
    assert args.amount == 1.0
    assert args.price == 0.43
    assert args.order_type == "FOK"
    assert options.tick_size == "0.01"
    assert options.neg_risk is False


def test_buy_fok_can_force_share_sized_limit_order_for_sub_dollar_notional():
    http = DummyHttpWithClob()
    client = PolymarketExecutionClient(http=http)

    resp = client.submit(
        PolyOrder(
            token_id="tok",
            side="BUY",
            price=Decimal("0.05"),
            size=Decimal("5"),
            order_type="FOK",
            post_only=False,
            use_limit_order=True,
        )
    )

    assert resp["success"] is True
    assert len(http.clob.market_orders) == 0
    assert len(http.clob.limit_orders) == 1
    args, options = http.clob.limit_orders[0]
    assert args.token_id == "tok"
    assert args.side == "BUY"
    assert args.price == 0.05
    assert args.size == 5.0
    assert options.tick_size == "0.01"
    assert options.neg_risk is False


def test_submit_batch_posts_two_share_sized_fok_orders_together():
    http = DummyHttpWithClob()
    client = PolymarketExecutionClient(http=http)

    resp = client.submit_batch([
        PolyOrder(token_id="yes", side="BUY", price=Decimal("0.40"), size=Decimal("5"), order_type="FOK", use_limit_order=True),
        PolyOrder(token_id="no", side="BUY", price=Decimal("0.55"), size=Decimal("5"), order_type="FOK", use_limit_order=True),
    ])

    assert len(resp) == 2
    assert len(http.clob.limit_orders) == 2
    assert len(http.clob.market_orders) == 0
    posted, post_only = http.clob.posted[-1]
    assert len(posted) == 2
    assert all(x.orderType == "FOK" for x in posted)
    assert post_only is False


def test_sell_order_uses_limit_builder_and_preserves_size_except_basic_quantization():
    http = DummyHttpWithClob()
    client = PolymarketExecutionClient(http=http)
    order = PolyOrder(
        token_id="tok",
        side="SELL",
        price=Decimal("0.67891"),
        size=Decimal("5.123456"),
        order_type="FOK",
        post_only=False,
    )

    normalized = client._normalize_order(order)
    resp = client.submit(order)

    assert normalized.price == Decimal("0.67")
    assert normalized.size == Decimal("5.1234")
    assert resp["success"] is True
    assert len(http.clob.limit_orders) == 1
    assert len(http.clob.market_orders) == 0


def test_signed_orders_pass_v2_tick_size_and_neg_risk_options():
    http = DummyHttpWithClob()
    client = PolymarketExecutionClient(http=http)

    client.submit(
        PolyOrder(
            token_id="tok",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("2"),
            order_type="GTC",
            tick_size="0.001",
            neg_risk=True,
        )
    )

    args, options = http.clob.limit_orders[0]
    assert args.token_id == "tok"
    assert options.tick_size == "0.001"
    assert options.neg_risk is True


def test_v2_builder_code_is_attached_to_limit_and_market_orders():
    http = DummyHttpWithClob()
    client = PolymarketExecutionClient(http=http)
    builder = "0x" + "a" * 64

    client.submit(PolyOrder(token_id="limit", side="BUY", price=Decimal("0.50"), size=Decimal("2"), order_type="GTC", builder_code=builder))
    client.submit(PolyOrder(token_id="market", side="BUY", price=Decimal("0.50"), size=Decimal("2"), order_type="FOK", builder_code=builder, user_pusd_balance=Decimal("100")))

    limit_args, _ = http.clob.limit_orders[0]
    market_args, _ = http.clob.market_orders[0]
    assert limit_args.builder_code == builder
    assert market_args.builder_code == builder
    assert getattr(market_args, "user_usdc_balance") == 100.0


def test_polymarket_retry_on_error_can_be_disabled_for_latency_sensitive_sniper(monkeypatch):
    monkeypatch.setenv("POLYMARKET_RETRY_ON_ERROR", "false")
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.delenv("NAUTILUS_DB_URL", raising=False)
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    cfg = PolymarketConfig.from_env()

    assert cfg.retry_on_error is False
