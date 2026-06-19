import json
from datetime import datetime, timezone

import pytest

from polybot.persistence.writer import PolybotWriter


class FakeAcquire:
    def __init__(self, con):
        self.con = con

    async def __aenter__(self):
        return self.con

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, con=None):
        self.con = con or FakeConnection()

    def acquire(self):
        return FakeAcquire(self.con)


class FakeConnection:
    def __init__(self):
        self.calls = []
        self.fetchval_return = None

    async def execute(self, sql, *args):
        self.calls.append((sql, args))

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return self.fetchval_return


@pytest.mark.asyncio
async def test_record_market_observation_persists_books_signal_config_and_state_as_json():
    writer = PolybotWriter("postgresql://example")
    writer._pool = FakePool()

    await writer.record_market_observation(
        strategy_id="live_momentum_consensus_07_dynamic",
        market_slug="btc-updown-5m-123",
        market_title="Bitcoin Up or Down",
        market_start_ts=100,
        market_end_ts=400,
        price_to_beat=64000.5,
        final_price=None,
        up_token="up-token",
        down_token="down-token",
        up_bid=0.48,
        up_ask=0.49,
        down_bid=0.51,
        down_ask=0.52,
        up_bids=[{"price": 0.48, "size": 10}],
        up_asks=[{"price": 0.49, "size": 11}],
        down_bids=[{"price": 0.51, "size": 12}],
        down_asks=[{"price": 0.52, "size": 13}],
        binance={"latest_close": 64002.0, "candles": [{"ts": 399, "close": 64002.0}]},
        signal={"side": "UP", "score": 3.0, "reason": None},
        config={"max_order_size": 1.0},
        state={"side": "FLAT", "pnl": 0.0},
    )

    sql, args = writer._pool.con.calls[-1]
    assert "INSERT INTO market_observations" in sql
    assert args[:11] == (
        "live_momentum_consensus_07_dynamic",
        "btc-updown-5m-123",
        "Bitcoin Up or Down",
        100,
        400,
        64000.5,
        None,
        "up-token",
        "down-token",
        0.48,
        0.49,
    )
    assert json.loads(args[13]) == [{"price": 0.48, "size": 10}]
    assert json.loads(args[17])["latest_close"] == 64002.0
    assert json.loads(args[18]) == {"side": "UP", "score": 3.0, "reason": None}
    assert json.loads(args[19]) == {"max_order_size": 1.0}
    assert json.loads(args[20]) == {"side": "FLAT", "pnl": 0.0}


@pytest.mark.asyncio
async def test_has_order_attempt_checks_market_slug():
    con = FakeConnection()
    con.fetchval_return = True
    writer = PolybotWriter("postgresql://unused")
    writer._pool = FakePool(con)

    assert await writer.has_order_attempt("strat", "slug") is True
    sql, args = con.calls[0]
    assert "order_attempts" in sql
    assert args == ("strat", "slug")


@pytest.mark.asyncio
async def test_record_order_attempt_persists_response_and_signal_json():
    writer = PolybotWriter("postgresql://example")
    writer._pool = FakePool()

    await writer.record_order_attempt(
        strategy_id="live_momentum_consensus_07_dynamic",
        market_slug="btc-updown-5m-123",
        token="up-token",
        outcome="UP",
        side="BUY",
        order_type="FOK",
        price=0.49,
        size=2.0408,
        stake_usd=1.0,
        status="rejected",
        response={"success": False},
        error="invalid signature",
        signal={"side": "UP", "score": 3.0},
        config={"max_order_size": 1.0},
    )

    sql, args = writer._pool.con.calls[-1]
    assert "INSERT INTO order_attempts" in sql
    assert args[:10] == (
        "live_momentum_consensus_07_dynamic",
        "btc-updown-5m-123",
        "up-token",
        "UP",
        "BUY",
        "FOK",
        0.49,
        2.0408,
        1.0,
        "rejected",
    )
    assert json.loads(args[10]) == {"success": False}
    assert args[11] == "invalid signature"
    assert json.loads(args[12]) == {"side": "UP", "score": 3.0}
    assert json.loads(args[13]) == {"max_order_size": 1.0}


@pytest.mark.asyncio
async def test_upsert_weather_safety_filter_accepts_iso_checked_at_string():
    writer = PolybotWriter("postgresql://example")
    writer._pool = FakePool()

    await writer.upsert_weather_safety_filter(
        "live_weather_outlier_sniper_chicago_auto_v1",
        {
            "city_slug": "chicago",
            "city": "Chicago",
            "station": "KORD",
            "source": "test",
            "gate": "YELLOW",
            "reason": "heavy wind 10-16 local",
            "expected_temp_fluctuation_c": 3.51,
            "weather_codes": [0, 1],
            "weather_code_names": ["clear", "mainly clear"],
            "size_multiplier": 1.0,
            "event_slug": "highest-temperature-in-chicago-on-may-4-2026",
            "metrics": {"filter_model": "empirical_weather_chart_risk_v3", "obs_age_min": 12.3},
            "reasons": ["heavy wind 10-16 local"],
            "warnings": [],
            "checked_at": "2026-05-04T05:14:56.163549+00:00",
        },
        enabled=True,
    )

    _sql, args = writer._pool.con.calls[-1]
    assert isinstance(args[16], datetime)
    assert args[16] == datetime(2026, 5, 4, 5, 14, 56, 163549, tzinfo=timezone.utc)
    assert json.loads(args[13]) == {"filter_model": "empirical_weather_chart_risk_v3"}
