from datetime import datetime, timezone

import pytest

from polybot.live.supervisor import (
    fast_roll_retry_sleep_seconds,
    should_select_btc_15m_event,
)


def test_15m_market_picker_ignores_expired_but_not_near_expiry_live_markets():
    now = datetime.fromtimestamp(1_000, tz=timezone.utc)

    assert should_select_btc_15m_event(datetime.fromtimestamp(999, tz=timezone.utc), now) is False
    assert should_select_btc_15m_event(datetime.fromtimestamp(1_001, tz=timezone.utc), now) is True
    assert should_select_btc_15m_event(datetime.fromtimestamp(1_030, tz=timezone.utc), now) is True


def test_auto_roll_retry_sleep_is_subsecond_to_catch_next_market_immediately():
    assert fast_roll_retry_sleep_seconds() == pytest.approx(0.25)
