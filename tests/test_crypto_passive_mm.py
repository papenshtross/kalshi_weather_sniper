import pytest

from polybot.crypto.fair_price import FairPriceSnapshot
from polybot.live.crypto_passive_mm import (
    PASSIVE_MM_CONFIG_VERSION,
    PassiveCryptoMMRunner,
    RestingOrder,
    _merge_passive_cfg,
    _order_id,
    _passive_cfg_issues,
    _round_price,
    _size_for_notional,
)


def _runner(filled=None, ask=0.99, fill_cost=None):
    runner = PassiveCryptoMMRunner.__new__(PassiveCryptoMMRunner)
    runner.pair = {"tick_size": "0.01", "order_min_size": 5.0}
    runner.books = {"YES": type("Book", (), {"ask": ask})(), "NO": type("Book", (), {"ask": ask})()}
    runner.filled = filled or {"YES": 0.0, "NO": 0.0}
    runner.fill_cost = fill_cost or {leg: runner.filled.get(leg, 0.0) * 0.40 for leg in ("YES", "NO")}
    return runner


def _fair(up=0.60, down=0.60):
    return FairPriceSnapshot(
        fair_up=up,
        fair_down=down,
        sigma_annualized=1.0,
        z_score=0.0,
        seconds_to_expiry=300.0,
        start_price=100.0,
        current_price=99.0,
    )


def test_round_price_floors_buy_quote_to_tick():
    assert _round_price(0.437, "0.01") == pytest.approx(0.43)
    assert _round_price(0.437, "0.001") == pytest.approx(0.437)
    assert _round_price(1.2, "0.01") == pytest.approx(0.99)
    assert _round_price(-1, "0.01") == pytest.approx(0.01)


def test_size_for_one_dollar_notional_respects_exchange_share_floor():
    assert _size_for_notional(1.0, 0.25, 5.0) == pytest.approx(5.0)
    assert _size_for_notional(1.0, 0.80, 5.0) == pytest.approx(5.0)
    assert _size_for_notional(1.0, 0.10, 5.0) == pytest.approx(10.0)
    assert _size_for_notional(1.0, 0.46, 5.0) * 0.46 >= 1.0


def test_order_id_accepts_common_clob_shapes():
    assert _order_id({"orderID": "a"}) == "a"
    assert _order_id({"order_id": "b"}) == "b"
    assert _order_id({"data": {"id": "c"}}) == "c"
    assert _order_id({"success": False}) is None


def test_stale_dashboard_config_cannot_override_passive_mm_critical_file_keys():
    file_cfg = {
        "id": "crypto_passive_mm_btc_15m",
        "passive_mm_config_version": PASSIVE_MM_CONFIG_VERSION,
        "quote_notional_usd": 0.0,
        "quote_size_shares": 5.0,
        "max_quote_shares": 5.0,
        "shares_max_per_market": 10.0,
        "max_market_exposure_shares": 10.0,
        "quote_update_interval_seconds": 0.5,
    }
    stale_db_cfg = {
        "id": "crypto_passive_mm_btc_15m",
        "quote_notional_usd": 1.0,
        "quote_size_shares": 1.0,
        "max_quote_shares": 1.0,
        "shares_max_per_market": 5.0,
        "max_market_exposure_shares": 5.0,
        "quote_update_interval_seconds": 2.0,
        "name": "dashboard name is still allowed",
    }

    merged, warnings = _merge_passive_cfg(file_cfg, stale_db_cfg)

    assert merged["quote_notional_usd"] == pytest.approx(0.0)
    assert merged["quote_size_shares"] == pytest.approx(5.0)
    assert merged["max_quote_shares"] == pytest.approx(5.0)
    assert merged["shares_max_per_market"] == pytest.approx(10.0)
    assert merged["max_market_exposure_shares"] == pytest.approx(10.0)
    assert merged["quote_update_interval_seconds"] == pytest.approx(0.5)
    assert merged["name"] == "dashboard name is still allowed"
    assert merged["passive_mm_config_version"] == PASSIVE_MM_CONFIG_VERSION
    assert warnings and "stale DB config" in warnings[0]
    assert _passive_cfg_issues(merged, order_min_size=5.0) == []


def test_current_version_dashboard_config_may_override_passive_mm_critical_file_keys():
    file_cfg = {"passive_mm_config_version": PASSIVE_MM_CONFIG_VERSION, "quote_size_shares": 5.0, "shares_max_per_market": 10.0}
    db_cfg = {"passive_mm_config_version": PASSIVE_MM_CONFIG_VERSION, "quote_size_shares": 7.0, "shares_max_per_market": 14.0}

    merged, warnings = _merge_passive_cfg(file_cfg, db_cfg)

    assert warnings == []
    assert merged["quote_size_shares"] == pytest.approx(7.0)
    assert merged["shares_max_per_market"] == pytest.approx(14.0)


def test_config_checker_flags_per_side_cap_below_exchange_min_size():
    issues = _passive_cfg_issues({"shares_max_per_market": 5.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0}, order_min_size=5.0)

    assert issues == ["per-side cap 2.5000 is below exchange min size 5.0000"]


def test_target_quotes_use_minimal_exchange_valid_size_and_skip_if_cap_too_low():
    runner = PassiveCryptoMMRunner.__new__(PassiveCryptoMMRunner)
    runner.pair = {"tick_size": "0.01", "order_min_size": 5.0}
    runner.books = {"YES": type("Book", (), {"ask": 0.02})(), "NO": type("Book", (), {"ask": 0.99})()}
    runner.filled = {"YES": 0.0, "NO": 0.0}
    fair = FairPriceSnapshot(
        fair_up=0.08,
        fair_down=0.92,
        sigma_annualized=1.0,
        z_score=0.0,
        seconds_to_expiry=300.0,
        start_price=100.0,
        current_price=99.0,
    )
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0}

    targets = runner._target_quotes(fair, cfg)

    assert targets["YES"] == pytest.approx((0.01, 5.0, "base"))
    assert targets["NO"] == pytest.approx((0.85, 5.0, "base"))


def test_target_quotes_cap_hedge_to_one_five_share_clip():
    runner = _runner(
        filled={"YES": 108.66, "NO": 39.997776},
        fill_cost={"YES": 108.66 * 0.01, "NO": 39.997776 * 0.40},
    )
    fair = _fair(up=0.01, down=0.99)
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0, "max_market_exposure_shares": 300.0}

    targets = runner._target_quotes(fair, cfg)

    assert targets["NO"] == pytest.approx((0.92, 5.0, "hedge"))


def test_target_quotes_stop_when_market_exposure_cap_is_filled_balanced():
    runner = _runner(filled={"YES": 5.0, "NO": 5.0})
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0, "max_market_exposure_shares": 10.0}

    targets = runner._target_quotes(_fair(), cfg)

    assert targets["YES"] == pytest.approx((0.01, 0.0, "awaiting_hedge"))
    assert targets["NO"] == pytest.approx((0.01, 0.0, "awaiting_hedge"))


def test_target_quotes_support_shares_max_per_market_alias_for_minimal_size_per_side():
    runner = _runner(filled={"YES": 5.0, "NO": 0.0})
    cfg = {
        "quote_edge_cents": 7.0,
        "quote_size_shares": 1.0,
        "max_quote_shares": 1.0,
        "quote_notional_usd": 1.0,
        "min_order_notional_usd": 1.0,
        "shares_max_per_market": 10.0,
    }

    targets = runner._target_quotes(_fair(), cfg)

    assert targets["YES"] == pytest.approx((0.01, 0.0, "awaiting_hedge"))
    assert targets["NO"] == pytest.approx((0.53, 5.0, "hedge"))


def test_target_quotes_one_share_desired_clip_is_raised_to_minimal_exchange_valid_size():
    runner = _runner(filled={"YES": 0.0, "NO": 0.0})
    cfg = {
        "quote_edge_cents": 7.0,
        "quote_size_shares": 1.0,
        "max_quote_shares": 1.0,
        "quote_notional_usd": 0.0,
        "min_order_notional_usd": 1.0,
        "shares_max_per_market": 10.0,
    }

    targets = runner._target_quotes(_fair(), cfg)

    assert targets["YES"] == pytest.approx((0.53, 5.0, "base"))
    assert targets["NO"] == pytest.approx((0.53, 5.0, "base"))


def test_partial_fill_below_min_residual_does_not_overhedge_past_five_share_side_cap():
    runner = _runner(filled={"YES": 4.44, "NO": 0.0})
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0, "shares_max_per_market": 10.0}

    targets = runner._target_quotes(_fair(), cfg)

    assert targets["YES"] == pytest.approx((0.01, 0.0, "awaiting_hedge"))
    assert targets["NO"] == pytest.approx((0.53, 0.0, "residual_below_min"))


def test_exact_five_each_filled_stops_all_quotes_for_market():
    runner = _runner(filled={"YES": 5.0, "NO": 5.0})
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "shares_max_per_market": 10.0}

    targets = runner._target_quotes(_fair(), cfg)

    assert all(size == pytest.approx(0.0) for _price, size, _purpose in targets.values())
    assert {purpose for _price, _size, purpose in targets.values()} == {"awaiting_hedge"}


def test_existing_five_share_fill_prevents_same_side_base_requote():
    runner = _runner(filled={"YES": 0.0, "NO": 5.0}, ask=0.99)
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 1.0, "max_quote_shares": 1.0, "min_order_notional_usd": 1.0, "shares_max_per_market": 10.0}

    targets = runner._target_quotes(_fair(up=0.25, down=0.75), cfg)

    # NO filled at the default 40c test avg, so fixed hedge-min is
    # 1 - 0.40 - 0.07 = 0.53. It should not trail lower with fair value.
    assert targets["YES"] == pytest.approx((0.53, 5.0, "hedge"))


def test_hedge_quote_is_capped_to_keep_pair_edge_positive():
    runner = _runner(filled={"YES": 0.0, "NO": 5.0}, fill_cost={"YES": 0.0, "NO": 5.0 * 0.53})
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0, "shares_max_per_market": 10.0}

    targets = runner._target_quotes(_fair(up=0.90, down=0.10), cfg)

    # Fair-edge alone would allow 0.83, but the existing NO fill was 0.53.
    # Cap YES at 1 - 0.53 - 0.07 = 0.40 so the hedge cannot lock a negative pair.
    assert targets["YES"] == pytest.approx((0.40, 5.0, "hedge"))


def test_hedge_quote_skips_when_positive_pair_edge_is_below_min_quote_price():
    runner = _runner(filled={"YES": 0.0, "NO": 5.0}, fill_cost={"YES": 0.0, "NO": 5.0 * 0.95})
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0, "shares_max_per_market": 10.0}

    targets = runner._target_quotes(_fair(up=0.90, down=0.10), cfg)

    assert targets["YES"] == pytest.approx((0.01, 0.0, "hedge_edge"))


def test_low_price_hedge_uses_five_shares_not_one_dollar_notional():
    runner = _runner(filled={"YES": 0.0, "NO": 5.0}, fill_cost={"YES": 0.0, "NO": 5.0 * 0.90})
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0, "shares_max_per_market": 10.0}

    targets = runner._target_quotes(_fair(up=0.90, down=0.10), cfg)

    # Hedge price = 1 - 0.90 - 0.07 = 0.03. Previous logic inflated to
    # 33.3334 shares for a $1 notional floor; live BTC15m CLOB accepts 5-share
    # limit orders below $1, so hedges should work down exposure in 5-share clips.
    assert targets["YES"] == pytest.approx((0.03, 5.0, "hedge"))


def test_hedge_price_stays_fixed_at_min_when_fair_moves_lower():
    runner = _runner(filled={"YES": 0.0, "NO": 5.0}, fill_cost={"YES": 0.0, "NO": 5.0 * 0.53})
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0, "shares_max_per_market": 10.0}

    high_fair = runner._target_quotes(_fair(up=0.90, down=0.10), cfg)
    low_fair = runner._target_quotes(_fair(up=0.25, down=0.75), cfg)

    assert high_fair["YES"] == pytest.approx((0.40, 5.0, "hedge"))
    assert low_fair["YES"] == pytest.approx((0.40, 5.0, "hedge"))


def test_target_quotes_allow_only_underfilled_side_until_ten_share_market_cap():
    runner = _runner(filled={"YES": 5.0, "NO": 0.0})
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0, "max_market_exposure_shares": 10.0}

    targets = runner._target_quotes(_fair(), cfg)

    assert targets["YES"] == pytest.approx((0.01, 0.0, "awaiting_hedge"))
    assert targets["NO"] == pytest.approx((0.53, 5.0, "hedge"))


def test_existing_fills_near_total_cap_do_not_place_min_size_over_cap():
    runner = _runner(filled={"YES": 8.0, "NO": 0.0})
    cfg = {"quote_edge_cents": 7.0, "quote_size_shares": 5.0, "max_quote_shares": 5.0, "min_order_notional_usd": 1.0, "shares_max_per_market": 10.0}

    targets = runner._target_quotes(_fair(), cfg)

    # Only 2 total shares remain before the 10-share market cap, below exchange
    # 5-share minimum. Do not over-hedge to 13 total shares.
    assert targets["YES"] == pytest.approx((0.01, 0.0, "awaiting_hedge"))
    assert targets["NO"] == pytest.approx((0.53, 0.0, "market_filled_cap"))


@pytest.mark.asyncio
async def test_quote_once_does_not_log_or_place_zero_size_cap_targets():
    class Writer:
        async def get_strategy_status(self, strategy_id):
            return "running"

        async def log_strategy_event(self, *args, **kwargs):
            raise AssertionError(f"unexpected strategy log: {args} {kwargs}")

    runner = _runner(filled={"YES": 5.0, "NO": 5.0})
    runner.strategy_id = "s"
    runner.writer = Writer()
    runner.open = {"YES": None, "NO": None}
    runner.last_quote_log_ts = 10**12  # suppress periodic health log in this unit test
    placed = []

    async def noop(*args, **kwargs):
        return None

    async def fake_place(*args, **kwargs):
        placed.append(args)

    runner._roll_market_if_needed = noop
    runner._refresh_current_price = noop
    runner._refresh_books = noop
    runner._reconcile_orders = noop
    runner._fair = lambda cfg: _fair()
    runner._place = fake_place

    await runner._quote_once({"quote_size_shares": 5.0, "max_quote_shares": 5.0, "shares_max_per_market": 10.0})

    assert placed == []


@pytest.mark.asyncio
async def test_place_allows_five_share_maker_quote_below_one_dollar_notional():
    class Writer:
        def __init__(self):
            self.attempts = []
            self.logs = []

        async def record_order_attempt(self, *args, **kwargs):
            self.attempts.append((args, kwargs))

        async def log_strategy_event(self, *args, **kwargs):
            self.logs.append((args, kwargs))

    runner = PassiveCryptoMMRunner.__new__(PassiveCryptoMMRunner)
    runner.strategy_id = "s"
    runner.pair = {"title": "Bitcoin Up or Down - test", "slug": "btc-updown-15m-test", "yes_token": "yes", "no_token": "no", "tick_size": "0.01"}
    runner.books = {"YES": type("Book", (), {"ask": 0.02})(), "NO": type("Book", (), {"ask": 0.99})()}
    runner.writer = Writer()
    runner.exec_client = None
    runner.open = {"YES": None, "NO": None}

    await runner._place("YES", 0.01, 5.0, "base", {"dry_run": True, "min_order_notional_usd": 1.0})

    assert runner.open["YES"] is not None
    assert runner.open["YES"].size == pytest.approx(5.0)
    assert runner.writer.attempts[0][0][8] == pytest.approx(0.05)
    assert runner.writer.attempts[0][0][9] == "submitted"


@pytest.mark.asyncio
async def test_reconcile_full_fills_prevents_any_more_orders_for_market():
    class Writer:
        def __init__(self):
            self.fills = []
            self.logs = []

        async def record_fill(self, *args, **kwargs):
            self.fills.append((args, kwargs))

        async def log_strategy_event(self, *args, **kwargs):
            self.logs.append((args, kwargs))

    class Exec:
        def get_order(self, order_id):
            return {"id": order_id, "status": "MATCHED", "size_matched": "5"}

    runner = PassiveCryptoMMRunner.__new__(PassiveCryptoMMRunner)
    runner.strategy_id = "s"
    runner.pair = {"title": "Bitcoin Up or Down - test", "tick_size": "0.01", "order_min_size": 5.0}
    runner.books = {"YES": type("Book", (), {"ask": 0.99})(), "NO": type("Book", (), {"ask": 0.99})()}
    runner.writer = Writer()
    runner.exec_client = Exec()
    runner.open = {
        "YES": RestingOrder("YES", "yes", "yes-oid", 0.43, 5.0, 0.0),
        "NO": RestingOrder("NO", "no", "no-oid", 0.44, 5.0, 0.0),
    }
    runner.filled = {"YES": 0.0, "NO": 0.0}
    runner.fill_cost = {"YES": 0.0, "NO": 0.0}
    runner.fill_seq = 100

    await runner._reconcile_orders({})
    targets = runner._target_quotes(_fair(), {"quote_size_shares": 5.0, "max_quote_shares": 5.0, "shares_max_per_market": 10.0})

    assert runner.filled == pytest.approx({"YES": 5.0, "NO": 5.0})
    assert runner.fill_cost == pytest.approx({"YES": 2.15, "NO": 2.20})
    assert runner.open == {"YES": None, "NO": None}
    assert len(runner.writer.fills) == 2
    assert all(size == pytest.approx(0.0) for _price, size, _purpose in targets.values())


@pytest.mark.asyncio
async def test_restart_loads_existing_five_by_five_fills_and_blocks_requote():
    class Conn:
        async def fetch(self, query, strategy_id, market_like):
            assert strategy_id == "s"
            assert market_like == "Bitcoin Up or Down - test [PASSIVE] %"
            return [
                {"market": "Bitcoin Up or Down - test [PASSIVE] YES", "size": 5.0, "cost": 2.0},
                {"market": "Bitcoin Up or Down - test [PASSIVE] NO", "size": 5.0, "cost": 2.25},
            ]

    class Acquire:
        async def __aenter__(self):
            return Conn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class Pool:
        def acquire(self):
            return Acquire()

    class Writer:
        def __init__(self):
            self._pool = Pool()
            self.logs = []

        async def log_strategy_event(self, *args, **kwargs):
            self.logs.append((args, kwargs))

    runner = PassiveCryptoMMRunner.__new__(PassiveCryptoMMRunner)
    runner.strategy_id = "s"
    runner.pair = {"title": "Bitcoin Up or Down - test", "tick_size": "0.01", "order_min_size": 5.0}
    runner.books = {"YES": type("Book", (), {"ask": 0.99})(), "NO": type("Book", (), {"ask": 0.99})()}
    runner.writer = Writer()
    runner.filled = {"YES": 0.0, "NO": 0.0}
    runner.fill_cost = {"YES": 0.0, "NO": 0.0}

    await runner._load_existing_passive_fills()
    targets = runner._target_quotes(_fair(), {"quote_size_shares": 5.0, "max_quote_shares": 5.0, "shares_max_per_market": 10.0})

    assert runner.filled == pytest.approx({"YES": 5.0, "NO": 5.0})
    assert runner.fill_cost == pytest.approx({"YES": 2.0, "NO": 2.25})
    assert all(size == pytest.approx(0.0) for _price, size, _purpose in targets.values())
    assert "Loaded existing passive fills" in runner.writer.logs[0][0][1]


@pytest.mark.asyncio
async def test_cancel_records_partial_fill_before_successful_cancel():
    class Writer:
        def __init__(self):
            self.fills = []
            self.logs = []

        async def record_fill(self, *args, **kwargs):
            self.fills.append((args, kwargs))

        async def log_strategy_event(self, *args, **kwargs):
            self.logs.append((args, kwargs))

    class Exec:
        def __init__(self):
            self.get_calls = 0

        def get_order(self, order_id):
            self.get_calls += 1
            return {"id": order_id, "status": "LIVE", "size_matched": "4.44"}

        def cancel(self, order_id):
            return {"canceled": [order_id], "not_canceled": {}}

    runner = PassiveCryptoMMRunner.__new__(PassiveCryptoMMRunner)
    runner.strategy_id = "s"
    runner.pair = {"title": "Bitcoin Up or Down - test"}
    runner.writer = Writer()
    runner.exec_client = Exec()
    runner.open = {"YES": RestingOrder("YES", "token", "oid", 0.43, 5.0, 0.0), "NO": None}
    runner.filled = {"YES": 0.0, "NO": 0.0}
    runner.fill_cost = {"YES": 0.0, "NO": 0.0}
    runner.fill_seq = 100

    delta = await runner._cancel("YES", "requote")

    assert delta == pytest.approx(4.44)
    assert runner.filled["YES"] == pytest.approx(4.44)
    assert runner.fill_cost["YES"] == pytest.approx(0.43 * 4.44)
    assert runner.open["YES"] is None
    assert len(runner.writer.fills) == 1
