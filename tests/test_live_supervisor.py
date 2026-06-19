import pytest

from polybot.live.supervisor import (
    LiveExecutionPlan,
    _apply_runtime_config,
    _build_live_execution_plan,
    _execute_live_pair,
    _sell_price_for_size,
)
from polybot.strategies.binary_arb_mm import BinaryArbMM


class DummyWriter:
    def __init__(self):
        self.logs = []
        self.fills = []
        self.statuses = []

    async def log_strategy_event(self, strategy_id, message, level="INFO"):
        self.logs.append((strategy_id, level, message))

    async def record_fill(self, strategy_id, fill_id, market, side, px, size, kind="MM"):
        self.fills.append({
            "strategy_id": strategy_id,
            "fill_id": fill_id,
            "market": market,
            "side": side,
            "px": px,
            "size": size,
            "kind": kind,
        })

    async def set_strategy_status(self, strategy_id, status):
        self.statuses.append((strategy_id, status))


class DummyExecClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.orders = []

    def submit(self, order):
        self.orders.append(order)
        if not self.responses:
            raise AssertionError("No more stub responses configured")
        return self.responses.pop(0)


@pytest.fixture()
def strat():
    s = BinaryArbMM(
        market="BTC test",
        yes_token="yes-token",
        no_token="no-token",
        threshold=0.97,
        fee_per_share=0.0,
        min_edge=0.0,
        pair_size=10.0,
        max_inventory=50.0,
    )
    s.yes_bid = 0.60
    s.yes_ask = 0.70
    s.no_bid = 0.20
    s.no_ask = 0.22
    return s


def test_sell_price_for_size_walks_bid_depth():
    bids = [
        {"price": 0.68, "size": 2},
        {"price": 0.67, "size": 2},
        {"price": 0.66, "size": 2},
    ]

    limit_px, cumulative = _sell_price_for_size(bids, 5.0)

    assert limit_px == pytest.approx(0.66)
    assert cumulative == pytest.approx(6.0)


def test_build_live_execution_plan_is_depth_aware(strat):
    cfg = {
        "threshold": 0.97,
        "pair_size": 10,
        "max_order_size": 10,
        "max_position_size": 10,
        "max_executed_orders": 0,
    }
    yes_asks = [{"price": 0.70, "size": 10}]
    no_asks = [{"price": 0.22, "size": 6}, {"price": 0.27, "size": 4}, {"price": 0.30, "size": 100}]

    plan, reason, details = _build_live_execution_plan(strat, cfg, executed_orders=0, yes_asks=yes_asks, no_asks=no_asks)

    assert reason == "ok"
    assert isinstance(plan, LiveExecutionPlan)
    assert details["first_limit"] == pytest.approx(0.70)
    assert details["second_limit"] == pytest.approx(0.27)
    assert plan.first_leg == "YES"
    assert plan.second_leg == "NO"
    assert plan.first_limit == pytest.approx(0.70)
    assert plan.second_limit == pytest.approx(0.27)
    assert plan.pair_size == pytest.approx(10.0)
    assert plan.trigger_cap == pytest.approx(0.97)


def test_build_live_execution_plan_rejects_if_position_cap_too_small(strat):
    cfg = {
        "threshold": 0.97,
        "pair_size": 10,
        "max_order_size": 10,
        "max_position_size": 2,
        "max_executed_orders": 0,
    }
    yes_asks = [{"price": 0.70, "size": 100}]
    no_asks = [{"price": 0.22, "size": 100}, {"price": 0.27, "size": 100}]

    plan, reason, details = _build_live_execution_plan(strat, cfg, executed_orders=0, yes_asks=yes_asks, no_asks=no_asks)

    assert plan is None
    assert reason == "max_position_size"
    assert details["min_required_size"] == pytest.approx(1 / 0.27)
    assert details["max_position_cap"] < details["min_required_size"]


def test_build_live_execution_plan_reports_second_leg_budget_exhausted(strat):
    strat.yes_ask = 0.6996
    strat.no_ask = 0.0001
    cfg = {
        "threshold": 0.6997,
        "pair_size": 10,
        "max_order_size": 10,
        "max_position_size": 10,
        "max_executed_orders": 0,
    }
    yes_asks = [{"price": 0.6996, "size": 100}]
    no_asks = [{"price": 0.0001, "size": 100}]

    plan, reason, details = _build_live_execution_plan(strat, cfg, executed_orders=0, yes_asks=yes_asks, no_asks=no_asks)

    assert plan is None
    assert reason == "second_leg_budget"
    assert details["second_limit"] <= 0


def test_apply_runtime_config_overrides_active_strategy_fields(strat):
    _apply_runtime_config(strat, {
        "threshold": 0.95,
        "pair_size": 7,
        "max_position_size": 12,
        "slow_offset": 0.02,
        "max_wait_seconds": 33,
    })

    assert strat.threshold == pytest.approx(0.95)
    assert strat.pair_size == pytest.approx(7)
    assert strat.max_inventory == pytest.approx(12)
    assert strat.slow_offset == pytest.approx(0.02)
    assert strat.max_wait_seconds == pytest.approx(33)


@pytest.mark.asyncio
async def test_execute_live_pair_recovers_if_second_leg_fails(strat):
    writer = DummyWriter()
    exec_client = DummyExecClient([
        {"success": True, "orderID": "yes-1"},
        {"success": False, "error": "no liquidity"},
        {"success": True, "orderID": "yes-unwind"},
    ])
    plan = LiveExecutionPlan(
        first_leg="YES",
        second_leg="NO",
        first_token=strat.yes_token,
        second_token=strat.no_token,
        first_limit=0.70,
        second_limit=0.27,
        pair_size=5.0,
        trigger_cap=0.97,
        estimated_pair_cost=4.85,
    )
    books = {
        strat.yes_token: {
            "bids": [{"price": 0.68, "size": 2}, {"price": 0.67, "size": 3}],
            "asks": [{"price": 0.70, "size": 5}],
        },
        strat.no_token: {
            "bids": [{"price": 0.20, "size": 5}],
            "asks": [{"price": 0.22, "size": 5}],
        },
    }
    fill_seq = [100]
    executed_orders = [0]

    ok = await _execute_live_pair(exec_client, writer, "s1", strat, {"threshold": 0.97}, fill_seq, executed_orders, plan, books)

    assert ok is False
    assert [o.side for o in exec_client.orders] == ["BUY", "BUY", "SELL"]
    assert [float(o.price) for o in exec_client.orders] == [0.70, 0.27, 0.67]
    assert executed_orders[0] == 2
    assert strat.yes_pos == pytest.approx(0.0)
    assert strat.no_pos == pytest.approx(0.0)
    assert strat.cash == pytest.approx(-0.15)
    assert not writer.statuses
    assert any("Recovery unwind filled" in msg for _, _, msg in writer.logs)


@pytest.mark.asyncio
async def test_execute_live_pair_stops_strategy_if_recovery_fails(strat):
    writer = DummyWriter()
    exec_client = DummyExecClient([
        {"success": True, "orderID": "yes-1"},
        {"success": False, "error": "no liquidity"},
        {"success": False, "error": "cannot unwind"},
    ])
    plan = LiveExecutionPlan(
        first_leg="YES",
        second_leg="NO",
        first_token=strat.yes_token,
        second_token=strat.no_token,
        first_limit=0.70,
        second_limit=0.27,
        pair_size=5.0,
        trigger_cap=0.97,
        estimated_pair_cost=4.85,
    )
    books = {
        strat.yes_token: {
            "bids": [{"price": 0.68, "size": 5}],
            "asks": [{"price": 0.70, "size": 5}],
        },
        strat.no_token: {
            "bids": [{"price": 0.20, "size": 5}],
            "asks": [{"price": 0.22, "size": 5}],
        },
    }
    fill_seq = [200]
    executed_orders = [0]

    ok = await _execute_live_pair(exec_client, writer, "s2", strat, {"threshold": 0.97}, fill_seq, executed_orders, plan, books)

    assert ok is False
    assert executed_orders[0] == 1
    assert strat.yes_pos == pytest.approx(5.0)
    assert strat.no_pos == pytest.approx(0.0)
    assert writer.statuses == [("s2", "stopped")]
    assert any("Residual exposure remains" in msg for _, _, msg in writer.logs)
