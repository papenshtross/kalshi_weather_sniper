from decimal import Decimal
import importlib.util
from pathlib import Path


MODULE_PATH = Path("scripts/tmp_event_outlier_exit_only_once.py")


def load_module():
    spec = importlib.util.spec_from_file_location("event_outlier_exit_only_once", MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_distressed_stop_exit_waits_for_liquidity_by_default():
    mod = load_module()

    allowed, details = mod.exit_liquidity_allowed(
        reason="stop",
        bid=Decimal("0.003"),
        entry=Decimal("0.01"),
        size=Decimal("424"),
        cfg={},
    )

    assert allowed is False
    assert details["action"] == "wait_for_liquidity"
    assert details["min_exit_bid"] == "0.0100"


def test_take_profit_exit_is_not_blocked_by_entry_floor():
    mod = load_module()

    allowed, details = mod.exit_liquidity_allowed(
        reason="take_profit",
        bid=Decimal("0.15"),
        entry=Decimal("0.01"),
        size=Decimal("424"),
        cfg={},
    )

    assert allowed is True
    assert details["action"] == "exit_allowed"


def test_distressed_exit_can_be_explicitly_allowed_by_config():
    mod = load_module()

    allowed, details = mod.exit_liquidity_allowed(
        reason="stop",
        bid=Decimal("0.003"),
        entry=Decimal("0.01"),
        size=Decimal("424"),
        cfg={"allow_distressed_exit": True},
    )

    assert allowed is True
    assert details["action"] == "exit_allowed"
