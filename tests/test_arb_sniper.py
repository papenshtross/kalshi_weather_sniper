from datetime import datetime, timedelta, timezone

import pytest

from polybot.live.arb_sniper import Book, _auto_crypto_updown, _event_end_dt, build_arb_plan, diagnose_arb_plan_skip, is_auto_roll_slug, pick_weather_high_temp_event


def test_build_arb_plan_accounts_for_fees_and_min_edge():
    now = 1000.0
    yes = Book(bid=0.47, ask=0.48, asks=[{"price": 0.48, "size": 10}], updated_ts=now)
    no = Book(bid=0.48, ask=0.49, asks=[{"price": 0.49, "size": 10}], updated_ts=now)

    plan = build_arb_plan(
        yes,
        no,
        order_limit_usd=5.0,
        min_edge=0.02,
        fee_per_share=0.005,
        now=now,
    )

    assert plan is not None
    # 1 - (0.48 + 0.49) - 2*0.005 = 0.02 exactly
    assert plan.edge_per_pair == 0.02
    assert plan.size <= 5.0 / (0.48 + 0.49 + 0.01)


def test_build_arb_plan_rejects_when_fee_adjusted_edge_too_small():
    now = 1000.0
    yes = Book(bid=0.48, ask=0.49, asks=[{"price": 0.49, "size": 10}], updated_ts=now)
    no = Book(bid=0.48, ask=0.49, asks=[{"price": 0.49, "size": 10}], updated_ts=now)

    plan = build_arb_plan(
        yes,
        no,
        order_limit_usd=10,
        min_edge=0.03,
        fee_per_share=0.005,
        now=now,
    )
    assert plan is None


def test_build_arb_plan_requires_depth_for_live_fixed_dollar_orders():
    now = 1000.0
    yes = Book(bid=0.09, ask=0.10, asks=None, updated_ts=now)
    no = Book(bid=0.84, ask=0.85, asks=[{"price": 0.85, "size": 10}], updated_ts=now)

    plan = build_arb_plan(
        yes,
        no,
        order_limit_usd=2,
        min_edge=0.01,
        first_leg_order_value_usd=1.0,
        second_leg_max_order_value_usd=1.0,
        require_full_depth_for_fixed_dollar=True,
        now=now,
    )
    assert plan is None


def test_build_arb_plan_can_use_top_price_fallback_when_depth_not_required():
    now = 1000.0
    yes = Book(bid=0.09, ask=0.10, asks=None, updated_ts=now)
    no = Book(bid=0.84, ask=0.85, asks=[{"price": 0.85, "size": 10}], updated_ts=now)

    plan = build_arb_plan(
        yes,
        no,
        order_limit_usd=2,
        min_edge=0.01,
        first_leg_order_value_usd=1.0,
        second_leg_max_order_value_usd=1.0,
        now=now,
    )
    assert plan is not None


def test_build_arb_plan_allows_sub_dollar_notional_when_min_is_shares():
    now = 1000.0
    yes = Book(bid=0.04, ask=0.05, asks=[{"price": 0.05, "size": 5}], updated_ts=now)
    no = Book(bid=0.39, ask=0.40, asks=[{"price": 0.40, "size": 5}], updated_ts=now)

    plan = build_arb_plan(
        yes,
        no,
        order_limit_usd=0.50,
        min_edge=0.02,
        fee_per_share=0.0,
        min_order_size_shares=1.0,
        now=now,
    )

    assert plan is not None
    assert plan.total_cost_est <= 0.50
    assert plan.size >= 1.0
    assert plan.yes_cost_est < 1.0



def test_build_arb_plan_uses_visible_depth_through_limit():
    now = 1000.0
    yes = Book(
        bid=0.40,
        ask=0.40,
        asks=[{"price": 0.40, "size": 1}, {"price": 0.41, "size": 10}],
        updated_ts=now,
    )
    no = Book(bid=0.55, ask=0.55, asks=[{"price": 0.55, "size": 10}], updated_ts=now)

    plan = build_arb_plan(
        yes,
        no,
        order_limit_usd=10.0,
        min_edge=0.03,
        fee_per_share=0.0,
        now=now,
    )

    assert plan is not None
    assert plan.yes_limit == 0.41
    assert plan.no_limit == 0.55
    assert plan.avg_sum_est <= 0.97


def test_weather_auto_roll_slug_is_recognized():
    assert is_auto_roll_slug("auto:weather-high-temp:hong-kong") is True
    assert is_auto_roll_slug("auto:weather-high-temp:new_york") is True
    assert is_auto_roll_slug("btc-updown-15m-auto") is True
    assert is_auto_roll_slug("auto:crypto-updown:btc:5m") is True
    assert is_auto_roll_slug("auto:crypto-updown:eth:15m") is True
    assert is_auto_roll_slug("auto:crypto-updown:sol:5m") is True
    assert _auto_crypto_updown("auto:crypto-updown:sol:15m") == ("sol", "15m")
    assert is_auto_roll_slug("highest-temperature-in-hong-kong-on-april-29-2026") is False


class _FakeGammaResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200
        self.headers = {"content-type": "application/json"}
        self.text = "[]"

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeGammaClient:
    def __init__(self, payload):
        self.payload = payload
        self.requests = []

    async def get(self, url, params=None, **kwargs):
        self.requests.append((url, params or {}))
        return _FakeGammaResponse(self.payload)


class _SlugAwareFakeGammaClient:
    def __init__(self, by_slug):
        self.by_slug = by_slug
        self.requests = []

    async def get(self, url, params=None, **kwargs):
        params = params or {}
        self.requests.append((url, params))
        if params.get("slug"):
            return _FakeGammaResponse(self.by_slug.get(params["slug"], []))
        return _FakeGammaResponse([])


@pytest.mark.asyncio
async def test_pick_weather_high_temp_event_rolls_to_next_unexpired_city_market():
    now = datetime.now(timezone.utc)
    payload = [
        {
            "slug": "highest-temperature-in-test-city-on-april-30-2026",
            "endDate": (now - timedelta(minutes=1)).isoformat(),
            "volume": 999,
        },
        {
            "slug": "highest-temperature-in-test-city-on-may-1-2026",
            "endDate": (now + timedelta(hours=5)).isoformat(),
            "volume": 10,
        },
        {
            "slug": "highest-temperature-in-mexico-city-on-may-1-2026",
            "endDate": (now + timedelta(hours=1)).isoformat(),
            "volume": 100,
        },
    ]

    picked = await pick_weather_high_temp_event(_FakeGammaClient(payload), "test-city")

    assert picked == "highest-temperature-in-test-city-on-may-1-2026"


@pytest.mark.asyncio
async def test_pick_weather_high_temp_event_includes_next_local_day_beyond_36h():
    now = datetime.now(timezone.utc)
    payload = [
        {
            "slug": "highest-temperature-in-late-city-on-may-2-2026",
            "endDate": (now + timedelta(hours=40)).isoformat(),
            "volume": 10,
        }
    ]

    picked = await pick_weather_high_temp_event(_FakeGammaClient(payload), "late-city")

    assert picked == "highest-temperature-in-late-city-on-may-2-2026"


@pytest.mark.asyncio
async def test_pick_weather_high_temp_event_falls_back_to_exact_city_slug_when_tag_page_omits_city():
    now = datetime.now(timezone.utc)
    d = (now + timedelta(days=1)).date()
    slug = f"highest-temperature-in-fallback-city-on-{d.strftime('%B').lower()}-{d.day}-{d.year}"
    client = _SlugAwareFakeGammaClient(
        {
            slug: [
                {
                    "slug": slug,
                    "endDate": (now + timedelta(hours=24)).isoformat(),
                    "active": True,
                    "closed": False,
                    "archived": False,
                    "volume": 10,
                }
            ]
        }
    )

    picked = await pick_weather_high_temp_event(client, "fallback-city")

    assert picked == slug
    assert any(params.get("slug") == slug for _url, params in client.requests)


def test_diagnose_arb_skip_logs_spotted_but_below_min_order_size():
    now = 1000.0
    yes = Book(bid=0.29, ask=0.30, asks=[{"price": 0.30, "size": 1.0}], updated_ts=now)
    no = Book(bid=0.64, ask=0.65, asks=[{"price": 0.65, "size": 1.0}], updated_ts=now)

    diag = diagnose_arb_plan_skip(
        yes,
        no,
        order_limit_usd=10.0,
        min_edge=0.03,
        min_order_size_shares=2.0,
        max_book_age_ms=150,
        now=now,
    )

    assert diag.opportunity_spotted is True
    assert diag.reason == "min_order_size"
    assert round(diag.edge, 4) == 0.05
    assert diag.details["max_feasible_before_profit_shares"] == 1.0


def test_diagnose_arb_skip_identifies_stale_quotes_even_when_edge_exists():
    yes = Book(bid=0.29, ask=0.30, asks=[{"price": 0.30, "size": 10.0}], updated_ts=900.0)
    no = Book(bid=0.64, ask=0.65, asks=[{"price": 0.65, "size": 10.0}], updated_ts=900.0)

    diag = diagnose_arb_plan_skip(
        yes,
        no,
        order_limit_usd=10.0,
        min_edge=0.03,
        max_book_age_ms=150,
        now=1000.0,
    )

    assert diag.opportunity_spotted is True
    assert diag.reason == "stale_quotes"
    assert diag.details["yes_age_ms"] > 150


def test_event_end_dt_parses_gamma_z_timestamp():
    assert _event_end_dt({"endDate": "2026-04-29T12:00:00Z"}) == datetime(2026, 4, 29, 12, tzinfo=timezone.utc)
