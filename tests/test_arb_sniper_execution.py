import asyncio
import time
from pathlib import Path

import pytest

import polybot.live.arb_sniper as arb_sniper_module
from polybot.live.arb_sniper import (
    ArbPlan,
    ArbSniperRunner,
    Book,
    LatencyStats,
    _weather_outlier_blacklist,
    _weather_outlier_city_from_cfg,
    _weather_outlier_is_blacklisted,
    _weather_outlier_is_higher_bracket_only,
    build_arb_plan,
    clob_response_indicates_fill,
    deterministic_rest_poll_phase_ms,
    market_websocket_enabled,
    polymarket_fee_per_share,
    rest_books_full,
    weather_prediction_price,
    weather_temperature_value,
)


class DummyWriter:
    def __init__(self):
        self.events = []
        self.order_attempts = []
        self.updated_attempts = []
        self.fills = []
        self.positions = []
        self.book_rows = []
        self.book_calls = 0
        self.ticks = []
        self.status = None

    async def upsert_books(self, rows):
        self.book_calls += 1
        self.book_rows.extend(rows)

    async def upsert_book(self, *args, **kwargs):
        self.book_calls += 1
        self.book_rows.append(args)

    async def record_tick(self, *args, **kwargs):
        self.ticks.append((args, kwargs))

    async def record_order_attempt(self, *args, **kwargs):
        self.order_attempts.append((args, kwargs))

    async def record_fill(self, *args, **kwargs):
        self.fills.append((args, kwargs))

    async def log_strategy_event(self, *args, **kwargs):
        self.events.append((args, kwargs))

    async def upsert_position(self, *args, **kwargs):
        self.positions.append((args, kwargs))

    async def snapshot_equity(self, *args, **kwargs):
        pass

    async def set_strategy_status(self, strategy_id, status):
        self.status = status

    async def count_order_attempts(self, strategy_id, market_slug):
        return sum(1 for args, _kwargs in self.order_attempts if args[0] == strategy_id and args[1] == market_slug)

    async def count_successful_order_attempts(self, strategy_id, market_slug):
        return sum(
            1
            for args, kwargs in self.order_attempts
            if args[0] == strategy_id
            and args[1] == market_slug
            and (len(args) > 9 and args[9] in {"filled", "submitted"} or (kwargs.get("response") or {}).get("success") is True)
        )

    async def successful_buy_market_slugs(self, strategy_id):
        slugs = set()
        for args, kwargs in self.order_attempts:
            if args[0] != strategy_id or args[4] != "BUY":
                continue
            response = kwargs.get("response") or {}
            status = args[9] if len(args) > 9 else ""
            if status in {"filled", "submitted"} or response.get("success") is True:
                slugs.add(args[1])
        return slugs

    async def first_successful_buy_outlier_signal_by_event(self, strategy_id):
        out = {}
        for args, kwargs in self.order_attempts:
            if args[0] != strategy_id or args[4] != "BUY":
                continue
            response = kwargs.get("response") or {}
            status = args[9] if len(args) > 9 else ""
            if status not in {"filled", "submitted"} and response.get("success") is not True:
                continue
            event_key = arb_sniper_module._weather_market_event_key(args[1])
            out.setdefault(event_key, {"market_slug": args[1], **(kwargs.get("signal") or {})})
        return out

    async def successful_order_stake_usd(self, strategy_id, market_slug, *, side=None, token=None):
        total = 0.0
        for args, kwargs in self.order_attempts:
            if args[0] != strategy_id or args[1] != market_slug:
                continue
            if side is not None and args[4] != side:
                continue
            if token is not None and args[2] != token:
                continue
            response = kwargs.get("response") or {}
            status = args[9] if len(args) > 9 else ""
            if status in {"filled", "submitted"} or response.get("success") is True:
                total += float(args[8] or 0.0)
        return total

    async def net_filled_order_size(self, strategy_id, market_slug, token):
        total = 0.0
        for args, _kwargs in self.order_attempts:
            if args[0] == strategy_id and args[1] == market_slug and args[2] == token and len(args) > 9 and args[9] == "filled":
                total += float(args[7] or 0.0) if args[4] == "BUY" else -float(args[7] or 0.0)
        return total

    async def count_filled_order_attempts(self, strategy_id, market_slug):
        return sum(
            1
            for args, _kwargs in self.order_attempts
            if args[0] == strategy_id and args[1] == market_slug and len(args) > 9 and args[9] == "filled"
        )

    async def has_order_attempt(self, strategy_id, market_slug):
        return await self.count_order_attempts(strategy_id, market_slug) > 0

    async def pending_order_attempts_by_response_order_id(self, strategy_id, *, order_type=None, side=None, status="submitted", max_age_seconds=600):
        rows = []
        for idx, (args, kwargs) in enumerate(self.order_attempts, start=1):
            response = kwargs.get("response") or {}
            if args[0] != strategy_id or args[9] != status:
                continue
            if order_type is not None and args[5] != order_type:
                continue
            if side is not None and args[4] != side:
                continue
            if not (response.get("orderID") or response.get("order_id")):
                continue
            rows.append({
                "id": idx,
                "ts": kwargs.get("ts") or __import__("datetime").datetime.fromtimestamp(time.time() - max_age_seconds - 1, tz=__import__("datetime").timezone.utc),
                "market_slug": args[1],
                "token": args[2],
                "outcome": args[3],
                "side": args[4],
                "order_type": args[5],
                "price": args[6],
                "size": args[7],
                "stake_usd": args[8],
                "status": args[9],
                "response": response,
                "signal": kwargs.get("signal") or {},
            })
        return rows

    async def update_order_attempt_by_order_id(self, strategy_id, order_id, *, status, price=None, size=None, stake_usd=None, response_patch=None, error=None):
        for idx, (args, kwargs) in enumerate(self.order_attempts):
            response = kwargs.get("response") or {}
            if args[0] == strategy_id and (response.get("orderID") == order_id or response.get("order_id") == order_id):
                args = list(args)
                if price is not None:
                    args[6] = price
                if size is not None:
                    args[7] = size
                if stake_usd is not None:
                    args[8] = stake_usd
                args[9] = status
                response.update(response_patch or {})
                kwargs["response"] = response
                if error is not None:
                    kwargs["error"] = error
                self.order_attempts[idx] = (tuple(args), kwargs)
                self.updated_attempts.append((order_id, status, price, size, stake_usd, response_patch))
                return True
        return False


class FakeBooksResponse:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeBooksClient:
    def __init__(self, payload):
        self.payload = payload
        self.requests = []

    async def post(self, url, json=None, timeout=None):
        self.requests.append({"url": url, "json": json, "timeout": timeout})
        return FakeBooksResponse(self.payload)


class SequencedExec:
    def __init__(self, batch_responses=None, single_responses=None, trades=None):
        self.batch_responses = list(batch_responses or [])
        self.single_responses = list(single_responses or [])
        self.trades = list(trades or [])
        self.batches = []
        self.singles = []
        self.http = type("Http", (), {"cfg": type("Cfg", (), {"proxy_address": "0xproxy"})()})()

    def submit_batch(self, orders):
        self.batches.append(orders)
        resp = self.batch_responses.pop(0)
        if isinstance(resp, list):
            resp = [({**x, "status": x.get("status") or "matched"} if isinstance(x, dict) and x.get("success") is True and (x.get("orderID") or x.get("order_id")) else x) for x in resp]
        return resp

    def submit(self, order):
        self.singles.append(order)
        resp = self.single_responses.pop(0)
        if isinstance(resp, BaseException):
            raise resp
        if isinstance(resp, dict) and resp.get("success") is True and (resp.get("orderID") or resp.get("order_id")) and not resp.get("status"):
            resp = {**resp, "status": "matched"}
        return resp

    def get_trades(self, **params):
        return list(self.trades)


def make_runner(exec_client):
    r = ArbSniperRunner(Path("config/arb-sniper-live.yaml"))
    r.writer = DummyWriter()
    r.exec_client = exec_client
    r.ev = {"slug": "test-market", "title": "Test Market", "yes_token": "YES_TOKEN", "no_token": "NO_TOKEN", "tick_size": "0.001", "neg_risk": True, "order_min_size": 5.0}
    return r


def plan():
    return ArbPlan(
        yes_size=5,
        no_size=5,
        size=5,
        yes_limit=0.40,
        no_limit=0.55,
        yes_cost_est=2.0,
        no_cost_est=2.75,
        total_cost_est=4.75,
        avg_sum_est=0.95,
        edge_per_pair=0.05,
        first_leg="YES",
        second_leg="NO",
    )


def make_weather_outlier_runner():
    r = make_runner(SequencedExec())
    pairs = []
    now = time.time()
    for temp, yes_ask, no_ask in [(17, 0.04, 0.96), (21, 0.45, 0.55), (25, 0.03, 0.99), (26, 0.02, 0.995)]:
        pair = {
            "slug": f"weather-temp-{temp}c",
            "title": f"High temperature {temp}C",
            "yes_token": f"YES_{temp}",
            "no_token": f"NO_{temp}",
            "tick_size": "0.01",
            "neg_risk": True,
        }
        pairs.append(pair)
        r.books_by_slug[pair["slug"]] = {
            "YES": Book(bid=max(0, yes_ask - 0.01), ask=yes_ask, asks=[{"price": yes_ask, "size": 500}], updated_ts=now),
            "NO": Book(bid=max(0, no_ask - 0.01), ask=no_ask, asks=[{"price": no_ask, "size": 500}], updated_ts=now),
        }
    r.market_pairs = pairs
    r.ev = pairs[0]
    r.books = r.books_by_slug[pairs[0]["slug"]]
    return r


def test_weather_temperature_value_normalizes_fahrenheit_to_celsius():
    assert weather_temperature_value({"title": "Will the highest temperature be 77°F?", "slug": "weather-77f"}) == pytest.approx(25.0)
    assert weather_temperature_value({"title": "Will the high be between 68 and 86F?", "slug": "weather-range"}) == pytest.approx(25.0)
    assert weather_temperature_value({"title": "Will the highest temperature be 25°C?", "slug": "weather-25c"}) == pytest.approx(25.0)


def test_weather_prediction_price_prefers_best_bid_over_ask():
    assert weather_prediction_price(Book(bid=0.48, ask=0.60)) == pytest.approx(0.48)
    assert weather_prediction_price(Book(bid=0.0, ask=0.60)) == pytest.approx(0.60)


def test_weather_outlier_blacklist_normalizes_city_names():
    cfg = {"market_slug": "auto:weather-high-temp:qingdao", "weather_outlier_blacklist": "Qingdao, New York"}
    assert _weather_outlier_city_from_cfg(cfg) == "qingdao"
    assert _weather_outlier_blacklist(cfg) == {"qingdao", "new-york"}
    assert _weather_outlier_is_blacklisted(cfg) is True


def test_weather_outlier_higher_bracket_only_city_category_normalizes_defaults_and_config():
    assert _weather_outlier_is_higher_bracket_only({"weather_city": "Qingdao"}) is True
    assert _weather_outlier_is_higher_bracket_only({"market_slug": "auto:weather-high-temp:shenzhen"}) is True
    assert _weather_outlier_is_higher_bracket_only({"weather_city": "San Francisco"}) is False
    assert _weather_outlier_is_higher_bracket_only({"weather_city": "Austin", "weather_outlier_higher_bracket_only_cities": "austin"}) is True


def test_weather_nws_heat_alert_blocks_higher_no_only_while_in_effect():
    active = {
        "active": True,
        "event": "Heat Advisory",
        "onset": "2026-06-11T12:00:00-07:00",
        "ends": "2026-06-11T23:00:00-07:00",
    }
    now = __import__("datetime").datetime.fromisoformat("2026-06-11T20:00:00+00:00")
    before = __import__("datetime").datetime.fromisoformat("2026-06-11T18:00:00+00:00")
    after = __import__("datetime").datetime.fromisoformat("2026-06-12T07:00:00+00:00")
    assert arb_sniper_module._weather_nws_heat_alert_blocks_higher_no(31.0, 28.0, active, now) is True
    assert arb_sniper_module._weather_nws_heat_alert_blocks_higher_no(31.0, 28.0, active, now, event_slug="highest-temperature-in-san-francisco-on-june-11-2026-88-89f") is True
    assert arb_sniper_module._weather_nws_heat_alert_blocks_higher_no(31.0, 28.0, active, now, event_slug="highest-temperature-in-san-francisco-on-june-12-2026-88-89f") is False
    assert arb_sniper_module._weather_nws_heat_alert_blocks_higher_no(25.0, 28.0, active, now) is False
    assert arb_sniper_module._weather_nws_heat_alert_blocks_higher_no(31.0, 28.0, active, before) is False
    assert arb_sniper_module._weather_nws_heat_alert_blocks_higher_no(31.0, 28.0, active, after) is False


def test_weather_outlier_nws_heat_alert_blocks_higher_no_plan_but_allows_lower_no():
    r = make_weather_outlier_runner()
    # Make both lower (17C) and higher (25C) outliers executable; active NWS heat
    # alert should block only the higher NO candidate.
    r.books_by_slug["weather-temp-17c"]["NO"].ask = 0.95
    r.books_by_slug["weather-temp-17c"]["NO"].asks = [{"price": 0.95, "size": 500}]
    r.books_by_slug["weather-temp-25c"]["NO"].ask = 0.95
    r.books_by_slug["weather-temp-25c"]["NO"].asks = [{"price": 0.95, "size": 500}]
    r.weather_nws_heat_alert_status = {
        "active": True,
        "event": "Heat Advisory",
        "onset": "2000-01-01T00:00:00+00:00",
        "ends": "2999-01-01T00:00:00+00:00",
    }
    diagnostics = []
    p = r._weather_outlier_plan_from_cfg({"kind": "weather_outlier_sniper", "outlier_order_usd": 10, "min_edge": 0.01}, diagnostics=diagnostics)
    assert p is not None
    assert p.pair["slug"] == "weather-temp-17c"
    assert any(d.get("reason") == "nws_heat_alert_higher_no" and d.get("candidate") == "weather-temp-25c" for d in diagnostics)


@pytest.mark.asyncio
async def test_weather_nws_heat_alert_exit_sells_pre_alert_higher_no_at_entry_price(monkeypatch):
    exec_client = SequencedExec(single_responses=[{"success": True, "orderID": "0xexit", "status": "matched"}])
    r = make_weather_outlier_runner()
    r.exec_client = exec_client
    pair = r.market_pairs[2]  # 25C higher NO token
    slug = "highest-temperature-in-san-francisco-on-june-11-2026-88-89f"
    pair["slug"] = slug
    r.books_by_slug[slug] = r.books_by_slug.pop("weather-temp-25c")
    token = pair["no_token"]
    r.books_by_slug[slug]["NO"].bid = 0.97
    r.books_by_slug[slug]["NO"].bids = [{"price": 0.97, "size": 25}]
    r.weather_nws_heat_alert_status = {
        "active": True,
        "event": "Heat Advisory",
        "sent": "2026-06-10T12:54:00+00:00",
        "onset": "2000-01-01T00:00:00+00:00",
        "ends": "2999-01-01T00:00:00+00:00",
    }

    async def positions(_status):
        return [{"slug": slug, "token": token, "open_size": 10.0, "entry_price": 0.97}]

    async def open_size(_pair):
        return 10.0

    monkeypatch.setattr(r, "_weather_outlier_pre_alert_higher_no_positions", positions)
    monkeypatch.setattr(r, "_weather_outlier_open_size", open_size)
    ok = await r.maybe_exit_weather_outlier_nws_heat_alert({"kind": "weather_outlier_sniper", "min_order_notional_usd": 1})
    assert ok is True
    assert exec_client.singles[0].side == "SELL"
    assert float(exec_client.singles[0].price) == pytest.approx(0.97)
    args, kwargs = r.writer.order_attempts[-1]
    assert args[4] == "SELL"
    assert args[5] == "FOK_NWS_HEAT_ALERT_EXIT"
    assert kwargs["signal"]["nws_heat_alert_exit"] is True




def test_weather_outlier_scan_max_age_is_independent_from_execution_guard():
    r = make_weather_outlier_runner()
    now = time.time()
    for books in r.books_by_slug.values():
        books["YES"].updated_ts = now - 2.0
        books["NO"].updated_ts = now - 2.0
    r.books_by_slug["weather-temp-25c"]["NO"].ask = 0.84
    r.books_by_slug["weather-temp-25c"]["NO"].asks = [{"price": 0.84, "size": 50}]

    p = r._weather_outlier_plan_from_cfg({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 10,
        "min_edge": 0.04,
        "max_book_age_ms": 1500,
        "scan_max_book_age_ms": 3000,
        "execution_max_book_age_ms": 1000,
    })

    assert p is not None
    assert p.pair["slug"] in {"weather-temp-17c", "weather-temp-25c"}


def test_weather_outlier_execution_allows_old_candidate_book_because_limit_price_caps_execution():
    r = make_weather_outlier_runner()
    now = time.time()
    r.books_by_slug["weather-temp-25c"]["NO"].ask = 0.84
    r.books_by_slug["weather-temp-25c"]["NO"].asks = [{"price": 0.84, "size": 50}]
    for books in r.books_by_slug.values():
        books["YES"].updated_ts = now - 2.0
        books["NO"].updated_ts = now - 2.0
    plan = r._weather_outlier_plan_from_cfg({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 10,
        "min_edge": 0.04,
        "scan_max_book_age_ms": 3000,
    })

    assert plan is not None
    ok, reason = r._weather_outlier_plan_fresh_for_execution(plan, {"execution_max_book_age_ms": 1000})

    assert ok is True
    assert reason == "ok"


def test_weather_outlier_hot_watchlist_tracks_candidate_tokens():
    r = make_weather_outlier_runner()
    pair = r.market_pairs[2]

    r._mark_weather_outlier_hot_pair(pair, {"weather_outlier_hot_poll_seconds": 30}, reason="candidate")

    assert set(r._weather_outlier_hot_token_ids()) == {pair["yes_token"], pair["no_token"]}


@pytest.mark.asyncio
async def test_weather_outlier_revalidation_refreshes_candidate_with_get_book(monkeypatch):
    r = make_weather_outlier_runner()
    pair = r.market_pairs[0]
    stale = time.time() - 5.0
    r.books_by_slug[pair["slug"]]["YES"].updated_ts = stale
    r.books_by_slug[pair["slug"]]["NO"].updated_ts = stale
    r.books_by_slug[pair["slug"]]["NO"].ask = 0.96
    r.books_by_slug[pair["slug"]]["NO"].asks = [{"price": 0.96, "size": 50}]
    plan = r._weather_outlier_plan_from_cfg({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 10,
        "min_edge": 0.04,
        "scan_max_book_age_ms": 10_000,
    })
    assert plan is not None
    calls = []

    async def fake_rest_book_full(_client, token_id):
        calls.append(token_id)
        if token_id == pair["yes_token"]:
            return Book(bid=0.03, ask=0.04, asks=[{"price": 0.04, "size": 50}], updated_ts=time.time())
        if token_id == pair["no_token"]:
            return Book(bid=0.95, ask=0.96, asks=[{"price": 0.96, "size": 50}], updated_ts=time.time())
        return None

    monkeypatch.setattr(arb_sniper_module, "rest_book_full", fake_rest_book_full)

    ok, reason = await r._revalidate_weather_outlier_candidate_books(plan, {"execution_max_book_age_ms": 1000, "execution_revalidate_books": True})

    assert ok is True
    assert reason == "ok"
    assert set(calls) == {pair["yes_token"], pair["no_token"]}
    ok, _ = r._weather_outlier_plan_fresh_for_execution(plan, {"execution_max_book_age_ms": 1000})
    assert ok is True

def test_weather_outlier_higher_bracket_only_blocks_lower_outlier_for_qingdao():
    r = make_weather_outlier_runner()
    p = r._weather_outlier_plan_from_cfg({
        "kind": "weather_outlier_sniper",
        "weather_city": "qingdao",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 10,
        "min_edge": 0.04,
        "max_book_age_ms": 10_000,
    })
    assert p is None


def test_weather_outlier_higher_bracket_only_allows_higher_outlier_for_shenzhen():
    r = make_weather_outlier_runner()
    r.books_by_slug["weather-temp-25c"]["NO"].ask = 0.84
    r.books_by_slug["weather-temp-25c"]["NO"].asks = [{"price": 0.84, "size": 50}]
    p = r._weather_outlier_plan_from_cfg({
        "kind": "weather_outlier_sniper",
        "weather_city": "shenzhen",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 10,
        "min_edge": 0.04,
        "max_book_age_ms": 10_000,
    })
    assert p is not None
    assert p.pair["slug"] == "weather-temp-25c"
    assert p.temp_value > p.winning_temp


@pytest.mark.asyncio
async def test_weather_outlier_blacklist_blocks_new_buys_but_not_runner():
    r = make_weather_outlier_runner()
    r.file_cfg = {"kind": "weather_outlier_sniper", "weather_city": "qingdao"}
    await r.maybe_execute_weather_outlier({
        "kind": "weather_outlier_sniper",
        "weather_city": "qingdao",
        "weather_outlier_blacklist": ["Qingdao"],
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 1,
        "min_edge": 0.01,
        "max_book_age_ms": 10_000,
    })

    assert r.exec_client.singles == []
    assert r.writer.order_attempts == []


def test_weather_outlier_plans_no_liquidity_with_min_edge_cap():
    r = make_weather_outlier_runner()

    p = r._weather_outlier_plan_from_cfg({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 1,
        "min_edge": 0.01,
        "max_book_age_ms": 10_000,
    })

    assert p is not None
    assert p.winning_temp == pytest.approx(21)
    assert p.temp_value == pytest.approx(25)
    assert p.token == "NO_25"
    assert p.ask == pytest.approx(0.99)
    assert p.max_no_price == pytest.approx(0.99)
    assert p.notional >= 1.0


def test_weather_outlier_sweeps_all_visible_capped_liquidity_even_below_min_notional():
    r = make_weather_outlier_runner()
    r.books_by_slug["weather-temp-17c"]["NO"].ask = 0
    r.books_by_slug["weather-temp-17c"]["NO"].asks = []
    r.books_by_slug["weather-temp-25c"]["NO"].ask = 0.84
    r.books_by_slug["weather-temp-25c"]["NO"].asks = [{"price": 0.84, "size": 0.5}]

    p = r._weather_outlier_plan_from_cfg({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 200,
        "min_edge": 0.04,
        "max_book_age_ms": 10_000,
        "min_order_notional_usd": 1.0,
    })

    assert p is not None
    assert p.token == "NO_25"
    assert p.size == pytest.approx(0.5)
    assert p.notional == pytest.approx(0.42)


def test_weather_outlier_boundary_veto_blocks_candidate_within_2c_of_forecast_high():
    r = make_weather_outlier_runner()
    r.books_by_slug["weather-temp-25c"]["NO"].ask = 0.84
    r.books_by_slug["weather-temp-25c"]["NO"].asks = [{"price": 0.84, "size": 50}]
    r.books_by_slug["weather-temp-17c"]["NO"].ask = 0.99
    r.books_by_slug["weather-temp-17c"]["NO"].asks = [{"price": 0.99, "size": 50}]
    diagnostics = []

    p = r._weather_outlier_plan_from_cfg(
        {
            "kind": "weather_outlier_sniper",
            "outlier_temperature_offset_degrees": 4,
            "outlier_order_usd": 10,
            "min_edge": 0.04,
            "max_book_age_ms": 10_000,
        },
        safety_result={"metrics": {"forecast_high_c": 24.0, "forecast_high_f": 75.2, "forecast_provider_used": "wunderground"}},
        diagnostics=diagnostics,
    )

    assert p is None
    assert any(d["reason"] == "forecast_boundary_veto" and d["temp"] == 25 for d in diagnostics)
    assert any("threshold 2" in d["detail"] for d in diagnostics if d["reason"] == "forecast_boundary_veto")


@pytest.mark.asyncio
async def test_weather_outlier_logs_blocking_criteria_when_plan_is_rejected_by_forecast_veto():
    r = make_weather_outlier_runner()
    r.books_by_slug["weather-temp-25c"]["NO"].ask = 0.84
    r.books_by_slug["weather-temp-25c"]["NO"].asks = [{"price": 0.84, "size": 50}]
    r.books_by_slug["weather-temp-17c"]["NO"].ask = 0.99
    r.books_by_slug["weather-temp-17c"]["NO"].asks = [{"price": 0.99, "size": 50}]

    async def safety_allows(_cfg):
        return True, 1.0, {"gate": "GREEN", "metrics": {"forecast_high_c": 24.0, "forecast_provider_used": "wunderground"}}

    r._weather_safety_allows_new_buy = safety_allows

    await r.maybe_execute_weather_outlier({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 10,
        "min_edge": 0.04,
        "max_book_age_ms": 10_000,
        "weather_outlier_block_log_interval_seconds": 0,
    })

    messages = [args[1] for args, _kwargs in r.writer.events]
    assert any("Weather outlier entry blocked" in msg for msg in messages)
    assert any("forecast_boundary_veto" in msg and "candidate=weather-temp-25c" in msg for msg in messages)
    assert any("winning_temp=21" in msg and "safety_gate=GREEN" in msg for msg in messages)


def test_weather_outlier_boundary_veto_allows_candidate_outside_2c_and_records_fahrenheit_context():
    r = make_weather_outlier_runner()
    r.books_by_slug["weather-temp-25c"]["NO"].ask = 0.84
    r.books_by_slug["weather-temp-25c"]["NO"].asks = [{"price": 0.84, "size": 50}]
    r.books_by_slug["weather-temp-17c"]["NO"].ask = 0.99
    r.books_by_slug["weather-temp-17c"]["NO"].asks = [{"price": 0.99, "size": 50}]

    p = r._weather_outlier_plan_from_cfg(
        {
            "kind": "weather_outlier_sniper",
            "outlier_temperature_offset_degrees": 4,
            "outlier_order_usd": 10,
            "min_edge": 0.04,
            "max_book_age_ms": 10_000,
        },
        safety_result={"metrics": {"forecast_high_c": 22.5, "forecast_high_f": 72.5, "forecast_provider_used": "wunderground"}},
    )

    assert p is not None
    assert p.temp_value == pytest.approx(25)
    assert p.boundary_forecast_high_c == pytest.approx(22.5)
    assert p.boundary_distance_c == pytest.approx(2.5)


def test_weather_outlier_uses_highest_yes_bid_as_winning_prediction():
    r = make_weather_outlier_runner()
    now = time.time()
    # Simulate Shanghai-like books: 25C is the highest priced prediction by
    # executable YES bid, while a farther market has a stray/high ask. With a
    # 4C offset, 28C is only 3C away from 25C and must not be eligible.
    custom = [(24, 0.25, 0.26, 0.74), (25, 0.48, 0.50, 0.50), (28, 0.01, 0.60, 0.94), (29, 0.005, 0.01, 0.94)]
    pairs = []
    r.books_by_slug = {}
    for temp, yes_bid, yes_ask, no_ask in custom:
        pair = {"slug": f"weather-temp-{temp}c", "title": f"High temperature {temp}°C", "yes_token": f"YES_{temp}", "no_token": f"NO_{temp}", "tick_size": "0.01", "neg_risk": True}
        pairs.append(pair)
        r.books_by_slug[pair["slug"]] = {
            "YES": Book(bid=yes_bid, ask=yes_ask, asks=[{"price": yes_ask, "size": 500}], updated_ts=now),
            "NO": Book(bid=max(0, no_ask - 0.01), ask=no_ask, asks=[{"price": no_ask, "size": 500}], updated_ts=now),
        }
    r.market_pairs = pairs
    r.ev = pairs[0]
    r.books = r.books_by_slug[pairs[0]["slug"]]

    p = r._weather_outlier_plan_from_cfg({"kind": "weather_outlier_sniper", "outlier_temperature_offset_degrees": 4, "outlier_order_usd": 1, "min_edge": 0.05, "max_book_age_ms": 10_000})

    assert p is not None
    assert p.winning_temp == pytest.approx(25)
    assert p.temp_value == pytest.approx(29)
    assert p.distance_degrees == pytest.approx(4)


def test_weather_outlier_rejects_no_price_above_min_edge_cap():
    r = make_weather_outlier_runner()

    p = r._weather_outlier_plan_from_cfg({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 1,
        "min_edge": 0.02,
        "max_book_age_ms": 10_000,
    })

    assert p is not None
    assert p.token == "NO_17"
    assert p.ask == pytest.approx(0.96)
    assert p.max_no_price == pytest.approx(0.98)


def test_weather_outlier_uses_worst_limit_needed_for_fixed_dollar_fok():
    r = make_weather_outlier_runner()
    now = time.time()
    # Thin top level: there is only 1.01 shares at 0.971, so a $1
    # FOK sized at the best ask requires walking to 0.976. Submitting the full
    # size at 0.971 gets killed even though enough capped liquidity exists.
    pair = r.market_pairs[0]
    pair.update({"slug": "highest-temperature-in-qingdao-on-may-1-2026-28corbelow", "title": "Will the highest temperature in Qingdao be 28°C or below on May 1?"})
    r.market_pairs = [
        pair,
        {"slug": "qingdao-34c", "title": "Will the highest temperature in Qingdao be 34°C on May 1?", "yes_token": "YES_34", "no_token": "NO_34", "tick_size": "0.01", "neg_risk": True},
    ]
    r.books_by_slug = {
        pair["slug"]: {
            "YES": Book(bid=0.029, ask=0.030, asks=[{"price": 0.030, "size": 2.56}], updated_ts=now),
            "NO": Book(bid=0.970, ask=0.971, asks=[{"price": 0.971, "size": 1.01}, {"price": 0.976, "size": 30}], updated_ts=now),
        },
        "qingdao-34c": {
            "YES": Book(bid=0.310, ask=0.320, asks=[{"price": 0.320, "size": 500}], updated_ts=now),
            "NO": Book(bid=0.680, ask=0.690, asks=[{"price": 0.690, "size": 500}], updated_ts=now),
        },
    }
    r.ev = pair
    r.books = r.books_by_slug[pair["slug"]]

    p = r._weather_outlier_plan_from_cfg({"kind": "weather_outlier_sniper", "outlier_temperature_offset_degrees": 4, "outlier_order_usd": 1, "min_edge": 0.01, "max_book_age_ms": 10_000})

    assert p is not None
    assert p.temp_value == pytest.approx(28)
    assert p.winning_temp == pytest.approx(34)
    assert p.ask == pytest.approx(0.976)
    assert p.notional == pytest.approx(1.0, abs=0.0002)
    assert p.size > 1.01


def test_weather_outlier_sweeps_partial_liquidity_under_order_cap():
    r = make_weather_outlier_runner()
    now = time.time()
    pair = r.market_pairs[0]
    pair.update({"slug": "thin-outlier-17c", "title": "Will the highest temperature be 17°C?"})
    r.market_pairs = [
        pair,
        {"slug": "weather-21c", "title": "Will the highest temperature be 21°C?", "yes_token": "YES_21", "no_token": "NO_21", "tick_size": "0.01", "neg_risk": True},
    ]
    r.books_by_slug = {
        pair["slug"]: {
            "YES": Book(bid=0.02, ask=0.03, asks=[{"price": 0.03, "size": 500}], updated_ts=now),
            "NO": Book(
                bid=0.94,
                ask=0.95,
                asks=[{"price": 0.95, "size": 4.0}, {"price": 0.96, "size": 2.0}, {"price": 0.97, "size": 2.35}, {"price": 0.98, "size": 500}],
                updated_ts=now,
            ),
        },
        "weather-21c": {
            "YES": Book(bid=0.50, ask=0.51, asks=[{"price": 0.51, "size": 500}], updated_ts=now),
            "NO": Book(bid=0.48, ask=0.49, asks=[{"price": 0.49, "size": 500}], updated_ts=now),
        },
    }
    r.ev = pair
    r.books = r.books_by_slug[pair["slug"]]

    p = r._weather_outlier_plan_from_cfg({"kind": "weather_outlier_sniper", "outlier_temperature_offset_degrees": 4, "outlier_order_usd": 10, "min_edge": 0.03, "max_book_age_ms": 10_000})

    assert p is not None
    assert p.token == "NO_17"
    assert p.ask == pytest.approx(0.97)
    assert p.notional == pytest.approx(8.0, abs=0.001)
    assert p.notional < 10


@pytest.mark.asyncio
async def test_weather_outlier_top_up_until_order_cap():
    r = make_weather_outlier_runner()
    await r.writer.record_order_attempt(r.strategy_id, "weather-temp-25c", "NO_25", "NO", "BUY", "FOK_MARKET", 0.99, 8.0808, 8.0, "filled")

    remaining = await r._weather_outlier_remaining_usd_by_slug({"kind": "weather_outlier_sniper", "outlier_order_usd": 10, "min_order_notional_usd": 1})
    p = r._weather_outlier_plan_from_cfg({"kind": "weather_outlier_sniper", "outlier_temperature_offset_degrees": 4, "outlier_order_usd": 10, "min_edge": 0.01, "max_book_age_ms": 10_000}, remaining_usd_by_slug=remaining)

    assert p is not None
    assert p.pair["slug"] == "weather-temp-25c"
    assert p.notional == pytest.approx(2.0, abs=0.01)


@pytest.mark.asyncio
async def test_weather_outlier_entry_uses_fak_limit_at_tier_threshold_and_records_partial_fill():
    r = make_weather_outlier_runner()
    ex = SequencedExec(single_responses=[{"success": True, "status": "matched", "orderID": "fak-1", "matched_size": "7", "matched_amount": "5.60"}])
    r.exec_client = ex
    book = r.books_by_slug["weather-temp-17c"]["NO"]
    book.ask = 0.84
    book.asks = [{"price": 0.84, "size": 100}]

    await r.maybe_execute_weather_outlier({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 100,
        "min_edge": 0.04,
        "max_book_age_ms": 10_000,
        "weather_outlier_rebuy_tiers_enabled": True,
    })

    assert len(ex.singles) == 1
    order = ex.singles[0]
    assert order.order_type == "FAK"
    assert float(order.price) == pytest.approx(0.88)
    assert float(order.price) * float(order.size) == pytest.approx(84.0, abs=0.01)
    args, kwargs = r.writer.order_attempts[-1]
    assert args[5] == "FAK_LIMIT"
    assert args[6] == pytest.approx(0.88)
    assert args[7] == pytest.approx(7.0)
    assert args[8] == pytest.approx(5.60)
    assert args[9] == "filled"
    assert kwargs["signal"]["execution_order_type"] == "FAK"
    assert kwargs["signal"]["planned_worst_book_price"] == pytest.approx(0.84)
    fill_args, _fill_kwargs = r.writer.fills[-1]
    assert fill_args[4] == pytest.approx(0.80)
    assert fill_args[5] == pytest.approx(7.0)


@pytest.mark.asyncio
async def test_weather_outlier_reconciles_delayed_fak_buy_from_trades():
    r = make_weather_outlier_runner()
    ex = SequencedExec(trades=[{
        "taker_order_id": "delayed-buy-1",
        "asset_id": "NO_17",
        "side": "BUY",
        "size": "5.051658",
        "price": "0.871",
        "status": "CONFIRMED",
        "outcome": "No",
    }])
    r.exec_client = ex
    await r.writer.record_order_attempt(
        r.strategy_id,
        "weather-temp-17c",
        "NO_17",
        "NO",
        "BUY",
        "FAK_LIMIT",
        0.88,
        5.0,
        4.40,
        "submitted",
        response={"success": True, "status": "delayed", "orderID": "delayed-buy-1"},
        signal={"strategy": "weather_outlier_sniper"},
    )

    await r._reconcile_weather_outlier_delayed_entries({"kind": "weather_outlier_sniper"})

    args, kwargs = r.writer.order_attempts[-1]
    assert args[9] == "filled"
    assert args[6] == pytest.approx(0.871)
    assert args[7] == pytest.approx(5.051658)
    assert args[8] == pytest.approx(5.051658 * 0.871)
    assert kwargs["response"]["reconciled_trade"]["taker_order_id"] == "delayed-buy-1"
    assert r.writer.fills[-1][0][4] == pytest.approx(0.871)
    assert r.writer.fills[-1][0][5] == pytest.approx(5.051658)


@pytest.mark.asyncio
async def test_weather_outlier_reconciles_old_delayed_fak_no_fill():
    r = make_weather_outlier_runner()
    r.exec_client = SequencedExec(trades=[])
    await r.writer.record_order_attempt(
        r.strategy_id,
        "weather-temp-17c",
        "NO_17",
        "NO",
        "BUY",
        "FAK_LIMIT",
        0.88,
        5.0,
        4.40,
        "submitted",
        response={"success": True, "status": "delayed", "orderID": "delayed-no-fill"},
    )

    await r._reconcile_weather_outlier_delayed_entries({"kind": "weather_outlier_sniper", "weather_outlier_delayed_reconcile_grace_seconds": 1})

    args, kwargs = r.writer.order_attempts[-1]
    assert args[9] == "no_fill"
    assert args[7] == pytest.approx(0.0)
    assert args[8] == pytest.approx(0.0)
    assert kwargs["response"]["reconciled_no_fill"] is True
    assert r.weather_outlier_market_cooldown_until["weather-temp-17c"] > time.time()


@pytest.mark.asyncio
async def test_weather_outlier_fak_no_fill_is_info_and_sets_market_cooldown():
    r = make_weather_outlier_runner()
    ex = SequencedExec(single_responses=[{"success": True, "status": "canceled"}, {"success": True, "status": "matched", "orderID": "should-not-submit"}])
    r.exec_client = ex
    book = r.books_by_slug["weather-temp-17c"]["NO"]
    book.ask = 0.84
    book.asks = [{"price": 0.84, "size": 100}]
    cfg = {
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 100,
        "min_edge": 0.04,
        "max_book_age_ms": 10_000,
        "weather_outlier_rebuy_tiers_enabled": True,
        "weather_outlier_fak_no_fill_cooldown_seconds": 30,
    }

    await r.maybe_execute_weather_outlier(cfg)
    await r.maybe_execute_weather_outlier(cfg)

    assert len(ex.singles) == 1
    args, _kwargs = r.writer.order_attempts[-1]
    assert args[9] == "no_fill"
    assert r.weather_outlier_market_cooldown_until["weather-temp-17c"] > time.time()
    _event_args, event_kwargs = r.writer.events[-1]
    assert event_kwargs["level"] == "INFO"



def test_weather_outlier_rebuy_tiers_allow_same_market_at_deeper_edges():
    r = make_weather_outlier_runner()
    now = time.time()
    pair = {"slug": "weather-temp-17c", "title": "Will the highest temperature be 17°C?", "yes_token": "YES_17", "no_token": "NO_17", "tick_size": "0.01", "neg_risk": True}
    winning = {"slug": "weather-temp-21c", "title": "Will the highest temperature be 21°C?", "yes_token": "YES_21", "no_token": "NO_21", "tick_size": "0.01", "neg_risk": True}
    r.market_pairs = [pair, winning]
    r.ev = pair
    r.books_by_slug = {
        pair["slug"]: {
            "YES": Book(bid=0.02, ask=0.03, asks=[{"price": 0.03, "size": 500}], updated_ts=now),
            "NO": Book(bid=0.89, ask=0.90, asks=[{"price": 0.90, "size": 500}], updated_ts=now),
        },
        winning["slug"]: {
            "YES": Book(bid=0.50, ask=0.51, asks=[{"price": 0.51, "size": 500}], updated_ts=now),
            "NO": Book(bid=0.48, ask=0.49, asks=[{"price": 0.49, "size": 500}], updated_ts=now),
        },
    }
    r.books = r.books_by_slug[pair["slug"]]
    cfg = {
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 1,
        "min_edge": 0.05,
        "weather_outlier_rebuy_tiers_enabled": True,
        "max_book_age_ms": 10_000,
    }

    p = r._weather_outlier_plan_from_cfg(cfg, remaining_usd_by_slug={pair["slug"]: 2.0, winning["slug"]: 3.0})

    assert p is not None
    assert p.token == "NO_17"
    assert p.max_no_price == pytest.approx(0.90)
    assert p.tier_edge_multiplier == pytest.approx(2.0)
    assert p.tier_notional_multiplier == pytest.approx(2.0)
    assert p.notional == pytest.approx(1.0, abs=0.001)


def test_weather_outlier_direction_lock_blocks_opposite_lower_after_higher_buy():
    r = make_weather_outlier_runner()
    now = time.time()
    high = {"slug": "highest-temperature-in-test-city-on-may-5-2026-28c", "title": "High temperature 28C", "yes_token": "YES_28", "no_token": "NO_28", "tick_size": "0.01", "neg_risk": True}
    low = {"slug": "highest-temperature-in-test-city-on-may-5-2026-20c", "title": "High temperature 20C", "yes_token": "YES_20", "no_token": "NO_20", "tick_size": "0.01", "neg_risk": True}
    fav = {"slug": "highest-temperature-in-test-city-on-may-5-2026-24c", "title": "High temperature 24C", "yes_token": "YES_24", "no_token": "NO_24", "tick_size": "0.01", "neg_risk": True}
    r.market_pairs = [high, low, fav]
    r.ev = high
    r.books_by_slug = {
        high["slug"]: {"YES": Book(bid=0.04, ask=0.05, asks=[{"price": 0.05, "size": 500}], updated_ts=now), "NO": Book(bid=0.95, ask=0.96, asks=[{"price": 0.96, "size": 500}], updated_ts=now)},
        low["slug"]: {"YES": Book(bid=0.04, ask=0.05, asks=[{"price": 0.05, "size": 500}], updated_ts=now), "NO": Book(bid=0.95, ask=0.96, asks=[{"price": 0.96, "size": 500}], updated_ts=now)},
        fav["slug"]: {"YES": Book(bid=0.49, ask=0.50, asks=[{"price": 0.50, "size": 500}], updated_ts=now), "NO": Book(bid=0.49, ask=0.50, asks=[{"price": 0.50, "size": 500}], updated_ts=now)},
    }

    p = r._weather_outlier_plan_from_cfg(
        {"kind": "weather_outlier_sniper", "outlier_temperature_offset_degrees": 4, "outlier_order_usd": 10, "min_edge": 0.03, "max_book_age_ms": 10_000},
        direction_lock_by_event={"highest-temperature-in-test-city-on-may-5-2026": "higher"},
    )

    assert p is not None
    assert p.pair["slug"] == high["slug"]


def test_weather_outlier_direction_lock_blocks_opposite_higher_after_lower_buy():
    r = make_weather_outlier_runner()
    now = time.time()
    high = {"slug": "highest-temperature-in-test-city-on-may-5-2026-28c", "title": "High temperature 28C", "yes_token": "YES_28", "no_token": "NO_28", "tick_size": "0.01", "neg_risk": True}
    low = {"slug": "highest-temperature-in-test-city-on-may-5-2026-20c", "title": "High temperature 20C", "yes_token": "YES_20", "no_token": "NO_20", "tick_size": "0.01", "neg_risk": True}
    fav = {"slug": "highest-temperature-in-test-city-on-may-5-2026-24c", "title": "High temperature 24C", "yes_token": "YES_24", "no_token": "NO_24", "tick_size": "0.01", "neg_risk": True}
    r.market_pairs = [high, low, fav]
    r.ev = high
    r.books_by_slug = {
        high["slug"]: {"YES": Book(bid=0.04, ask=0.05, asks=[{"price": 0.05, "size": 500}], updated_ts=now), "NO": Book(bid=0.95, ask=0.96, asks=[{"price": 0.96, "size": 500}], updated_ts=now)},
        low["slug"]: {"YES": Book(bid=0.04, ask=0.05, asks=[{"price": 0.05, "size": 500}], updated_ts=now), "NO": Book(bid=0.95, ask=0.96, asks=[{"price": 0.96, "size": 500}], updated_ts=now)},
        fav["slug"]: {"YES": Book(bid=0.49, ask=0.50, asks=[{"price": 0.50, "size": 500}], updated_ts=now), "NO": Book(bid=0.49, ask=0.50, asks=[{"price": 0.50, "size": 500}], updated_ts=now)},
    }

    p = r._weather_outlier_plan_from_cfg(
        {"kind": "weather_outlier_sniper", "outlier_temperature_offset_degrees": 4, "outlier_order_usd": 10, "min_edge": 0.03, "max_book_age_ms": 10_000},
        direction_lock_by_event={"highest-temperature-in-test-city-on-may-5-2026": "lower"},
    )

    assert p is not None
    assert p.pair["slug"] == low["slug"]


def test_weather_outlier_direction_lock_can_be_disabled_by_config():
    r = make_weather_outlier_runner()
    now = time.time()
    high = {"slug": "highest-temperature-in-test-city-on-may-5-2026-28c", "title": "High temperature 28C", "yes_token": "YES_28", "no_token": "NO_28", "tick_size": "0.01", "neg_risk": True}
    low = {"slug": "highest-temperature-in-test-city-on-may-5-2026-20c", "title": "High temperature 20C", "yes_token": "YES_20", "no_token": "NO_20", "tick_size": "0.01", "neg_risk": True}
    fav = {"slug": "highest-temperature-in-test-city-on-may-5-2026-24c", "title": "High temperature 24C", "yes_token": "YES_24", "no_token": "NO_24", "tick_size": "0.01", "neg_risk": True}
    r.market_pairs = [high, low, fav]
    r.ev = high
    r.books_by_slug = {
        high["slug"]: {"YES": Book(bid=0.04, ask=0.05, asks=[{"price": 0.05, "size": 500}], updated_ts=now), "NO": Book(bid=0.95, ask=0.96, asks=[{"price": 0.96, "size": 500}], updated_ts=now)},
        low["slug"]: {"YES": Book(bid=0.03, ask=0.04, asks=[{"price": 0.04, "size": 500}], updated_ts=now), "NO": Book(bid=0.96, ask=0.97, asks=[{"price": 0.97, "size": 500}], updated_ts=now)},
        fav["slug"]: {"YES": Book(bid=0.49, ask=0.50, asks=[{"price": 0.50, "size": 500}], updated_ts=now), "NO": Book(bid=0.49, ask=0.50, asks=[{"price": 0.50, "size": 500}], updated_ts=now)},
    }

    p = r._weather_outlier_plan_from_cfg(
        {
            "kind": "weather_outlier_sniper",
            "outlier_temperature_offset_degrees": 4,
            "outlier_order_usd": 10,
            "min_edge": 0.03,
            "max_book_age_ms": 10_000,
            "weather_outlier_direction_lock_enabled": False,
        },
        direction_lock_by_event={"highest-temperature-in-test-city-on-may-5-2026": "higher"},
    )

    assert p is not None
    assert p.pair["slug"] == low["slug"]


@pytest.mark.asyncio
async def test_weather_safety_cache_refreshes_when_event_target_date_changes(monkeypatch):
    r = make_weather_outlier_runner()
    calls = []

    async def fake_analyze_city_safety(city, event_slug=None):
        calls.append(event_slug)
        forecast = 23.4 if "may-27" in str(event_slug) else 27.2
        return {"city_slug": city, "gate": "GREEN", "reason": "test green", "metrics": {"forecast_high_c": forecast}}

    monkeypatch.setattr(arb_sniper_module, "analyze_city_safety", fake_analyze_city_safety)
    cfg = {
        "kind": "weather_outlier_sniper",
        "weather_city": "qingdao",
        "weather_safety_filter_report_enabled": True,
        "weather_safety_filter_refresh_seconds": 900,
    }

    r.ev = {"slug": "highest-temperature-in-qingdao-on-may-27-2026-26c"}
    first = await r._refresh_weather_safety_filter(cfg)
    assert first is not None
    cached = await r._refresh_weather_safety_filter(cfg)
    assert cached is not None
    assert first is cached
    assert len(calls) == 1
    assert cached["metrics"]["forecast_high_c"] == pytest.approx(23.4)

    r.ev = {"slug": "highest-temperature-in-qingdao-on-may-28-2026-31c"}
    refreshed = await r._refresh_weather_safety_filter(cfg)
    assert refreshed is not None
    assert len(calls) == 2
    assert refreshed["metrics"]["forecast_high_c"] == pytest.approx(27.2)


@pytest.mark.asyncio
async def test_weather_safety_yellow_uses_normal_single_order_not_rebuy_ladder(monkeypatch):
    r = make_weather_outlier_runner()
    r.exec_client = SequencedExec(single_responses=[{"status": "matched", "success": True}])
    pair = r.market_pairs[2]  # 25C NO at 0.99 is eligible with min_edge=0.01
    other_pair = r.market_pairs[0]
    r.books_by_slug[other_pair["slug"]]["NO"].ask = 0.995
    r.books_by_slug[other_pair["slug"]]["NO"].asks = [{"price": 0.995, "size": 500}]

    async def fake_analyze_city_safety(city, event_slug=None):
        return {"city_slug": city, "gate": "YELLOW", "reason": "test yellow", "size_multiplier": 0.2}

    monkeypatch.setattr(arb_sniper_module, "analyze_city_safety", fake_analyze_city_safety)
    await r.writer.record_order_attempt(r.strategy_id, pair["slug"], pair["no_token"], "NO", "BUY", "FOK_MARKET", 0.99, 1.0101, 1.0, "filled")

    await r.maybe_execute_weather_outlier({
        "kind": "weather_outlier_sniper",
        "weather_city": "test-city",
        "weather_safety_filter_enabled": True,
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 1,
        "min_edge": 0.01,
        "weather_outlier_rebuy_tiers_enabled": True,
        "max_book_age_ms": 10_000,
    })

    buy_attempts = [args for args, _kwargs in r.writer.order_attempts if args[4] == "BUY"]
    assert len(buy_attempts) == 1
    assert r.exec_client.singles == []
    assert r.weather_safety_status["size_multiplier"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_weather_safety_green_keeps_rebuy_ladder(monkeypatch):
    r = make_weather_outlier_runner()
    r.exec_client = SequencedExec(single_responses=[{"status": "matched", "success": True}])
    pair = r.market_pairs[2]
    other_pair = r.market_pairs[0]
    r.books_by_slug[other_pair["slug"]]["NO"].ask = 0.995
    r.books_by_slug[other_pair["slug"]]["NO"].asks = [{"price": 0.995, "size": 500}]
    r.books_by_slug[pair["slug"]]["NO"].ask = 0.98
    r.books_by_slug[pair["slug"]]["NO"].asks = [{"price": 0.98, "size": 500}]

    async def fake_analyze_city_safety(city, event_slug=None):
        return {"city_slug": city, "gate": "GREEN", "reason": "test green"}

    monkeypatch.setattr(arb_sniper_module, "analyze_city_safety", fake_analyze_city_safety)
    await r.writer.record_order_attempt(r.strategy_id, pair["slug"], pair["no_token"], "NO", "BUY", "FOK_MARKET", 0.99, 1.0101, 1.0, "filled")

    await r.maybe_execute_weather_outlier({
        "kind": "weather_outlier_sniper",
        "weather_city": "test-city",
        "weather_safety_filter_enabled": True,
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 1,
        "min_edge": 0.01,
        "weather_outlier_rebuy_tiers_enabled": True,
        "max_book_age_ms": 10_000,
    })

    buy_attempts = [args for args, _kwargs in r.writer.order_attempts if args[4] == "BUY"]
    assert len(buy_attempts) == 2
    assert r.exec_client.singles
    assert r.writer.order_attempts[-1][1]["signal"]["tier_edge_multiplier"] == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_weather_safety_red_blocks_new_buys(monkeypatch):
    r = make_weather_outlier_runner()
    r.exec_client = SequencedExec(single_responses=[{"status": "matched", "success": True}])

    async def fake_analyze_city_safety(city, event_slug=None):
        return {"city_slug": city, "gate": "RED", "reason": "test red"}

    monkeypatch.setattr(arb_sniper_module, "analyze_city_safety", fake_analyze_city_safety)
    await r.maybe_execute_weather_outlier({
        "kind": "weather_outlier_sniper",
        "weather_city": "test-city",
        "weather_safety_filter_enabled": True,
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 1,
        "min_edge": 0.01,
        "weather_outlier_rebuy_tiers_enabled": True,
        "max_book_age_ms": 10_000,
    })

    assert r.exec_client.singles == []
    assert r.writer.order_attempts == []


def test_weather_outlier_rebuy_tiers_still_require_temperature_offset():
    r = make_weather_outlier_runner()
    now = time.time()
    near = {"slug": "weather-temp-18c", "title": "Will the highest temperature be 18°C?", "yes_token": "YES_18", "no_token": "NO_18", "tick_size": "0.01", "neg_risk": True}
    winning = {"slug": "weather-temp-21c", "title": "Will the highest temperature be 21°C?", "yes_token": "YES_21", "no_token": "NO_21", "tick_size": "0.01", "neg_risk": True}
    r.market_pairs = [near, winning]
    r.ev = near
    r.books_by_slug = {
        near["slug"]: {
            "YES": Book(bid=0.02, ask=0.03, asks=[{"price": 0.03, "size": 500}], updated_ts=now),
            "NO": Book(bid=0.84, ask=0.85, asks=[{"price": 0.85, "size": 500}], updated_ts=now),
        },
        winning["slug"]: {
            "YES": Book(bid=0.50, ask=0.51, asks=[{"price": 0.51, "size": 500}], updated_ts=now),
            "NO": Book(bid=0.48, ask=0.49, asks=[{"price": 0.49, "size": 500}], updated_ts=now),
        },
    }
    r.books = r.books_by_slug[near["slug"]]

    p = r._weather_outlier_plan_from_cfg({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 1,
        "min_edge": 0.05,
        "weather_outlier_rebuy_tiers_enabled": True,
        "max_book_age_ms": 10_000,
    })

    assert p is None


@pytest.mark.asyncio
async def test_weather_outlier_take_profit_sells_open_no_position():
    r = make_weather_outlier_runner()
    r.exec_client = SequencedExec(single_responses=[{"status": "matched", "success": True}])
    pair = r.market_pairs[2]
    pair["tick_size"] = "0.001"
    r.weather_outlier_local_positions[pair["no_token"]] = 3.25
    r.books_by_slug[pair["slug"]]["NO"].bid = 0.999
    r.books_by_slug[pair["slug"]]["NO"].bids = [{"price": 0.999, "size": 10}]

    executed = await r.maybe_take_profit_weather_outlier({"kind": "weather_outlier_sniper", "outlier_take_profit_price": 0.999, "min_order_notional_usd": 1, "tick_size": "0.001"})

    assert executed is True
    assert len(r.exec_client.singles) == 1
    order = r.exec_client.singles[0]
    assert order.side == "SELL"
    assert float(order.price) == pytest.approx(0.999)
    assert float(order.size) == pytest.approx(3.25)
    args, kwargs = r.writer.order_attempts[-1]
    assert args[4] == "SELL"
    assert args[5] == "FOK_TAKE_PROFIT"
    assert kwargs["signal"]["take_profit"] is True
    assert kwargs["signal"]["tick_size"] == "0.001"


@pytest.mark.asyncio
async def test_weather_outlier_take_profit_prefers_pair_tick_over_stale_config():
    r = make_weather_outlier_runner()
    r.exec_client = SequencedExec(single_responses=[{"status": "matched", "success": True}])
    pair = r.market_pairs[2]
    pair["tick_size"] = "0.001"
    r.weather_outlier_local_positions[pair["no_token"]] = 3.25
    r.books_by_slug[pair["slug"]]["NO"].bid = 0.999
    r.books_by_slug[pair["slug"]]["NO"].bids = [{"price": 0.999, "size": 10}]

    executed = await r.maybe_take_profit_weather_outlier({"kind": "weather_outlier_sniper", "outlier_take_profit_price": 0.999, "min_order_notional_usd": 1, "tick_size": "0.01"})

    assert executed is True
    order = r.exec_client.singles[-1]
    assert float(order.price) == pytest.approx(0.999)
    assert order.tick_size == "0.001"


@pytest.mark.asyncio
async def test_weather_outlier_take_profit_refuses_lower_tick_floor_price():
    r = make_weather_outlier_runner()
    r.exec_client = SequencedExec(single_responses=[{"status": "matched", "success": True}])
    pair = r.market_pairs[2]
    r.weather_outlier_local_positions[pair["no_token"]] = 3.25
    r.books_by_slug[pair["slug"]]["NO"].bid = 0.999
    r.books_by_slug[pair["slug"]]["NO"].bids = [{"price": 0.999, "size": 10}]

    executed = await r.maybe_take_profit_weather_outlier({"kind": "weather_outlier_sniper", "outlier_take_profit_price": 0.999, "min_order_notional_usd": 1, "tick_size": "0.01"})

    assert executed is False
    assert r.exec_client.singles == []
    assert r.writer.order_attempts == []
    assert any("refusing to sell lower" in event[0][1] for event in r.writer.events)


@pytest.mark.asyncio
async def test_weather_outlier_legacy_take_profit_uses_hydrated_neg_risk_metadata(monkeypatch):
    r = make_weather_outlier_runner()
    r.exec_client = SequencedExec(single_responses=[{"status": "matched", "success": True}])
    legacy = {
        "slug": "legacy-weather-28c",
        "title": "legacy-weather-28c",
        "no_token": "LEGACY_NO",
        "db_open_size": 20.7469,
        "filled_buy_size": 20.7469,
    }

    async def fake_legacy_positions():
        return [legacy]

    async def fake_enrich(positions):
        return [{**positions[0], "tick_size": "0.001", "neg_risk": True, "title": "Legacy 28C"}]

    async def fake_rest_books_full(_client, tokens):
        assert tokens == ["LEGACY_NO"]
        return {"LEGACY_NO": Book(bid=0.999, bids=[{"price": 0.999, "size": 50}], updated_ts=time.time(), tick_size="0.001", neg_risk=True)}

    r._weather_outlier_legacy_open_positions = fake_legacy_positions
    r._weather_outlier_enrich_legacy_positions = fake_enrich
    r._conditional_balance_shares = lambda token: 20.7469 if token == "LEGACY_NO" else 0.0
    monkeypatch.setattr(arb_sniper_module, "rest_books_full", fake_rest_books_full)

    executed = await r.maybe_take_profit_weather_outlier({"kind": "weather_outlier_sniper", "outlier_take_profit_price": 0.999, "min_order_notional_usd": 1})

    assert executed is True
    order = r.exec_client.singles[-1]
    assert order.token_id == "LEGACY_NO"
    assert order.side == "SELL"
    assert order.tick_size == "0.001"
    assert order.neg_risk is True
    args, kwargs = r.writer.order_attempts[-1]
    assert args[1] == "legacy-weather-28c"
    assert kwargs["signal"]["legacy_position"] is True


@pytest.mark.asyncio
async def test_weather_outlier_honors_max_orders_per_market():
    r = make_weather_outlier_runner()
    r.exec_client = SequencedExec(single_responses=[{"status": "matched", "success": True}])
    # Pretend the best outlier market already had a fill; planner should use
    # the next qualifying outlier instead of buying the same market again.
    await r.writer.record_order_attempt(
        r.strategy_id,
        "weather-temp-25c",
        "NO_25",
        "NO",
        "BUY",
        "FOK_MARKET",
        0.99,
        1.0102,
        1.0,
        "filled",
    )

    await r.maybe_execute_weather_outlier({
        "kind": "weather_outlier_sniper",
        "outlier_temperature_offset_degrees": 4,
        "outlier_order_usd": 1,
        "min_edge": 0.04,
        "max_orders_per_market": 1,
        "max_book_age_ms": 10_000,
    })

    assert len(r.writer.order_attempts) == 2
    args, _kwargs = r.writer.order_attempts[-1]
    assert args[1] == "weather-temp-17c"
    assert args[2] == "NO_17"
    assert args[3] == "NO"


def test_fee_formula_is_price_dependent():
    assert polymarket_fee_per_share(0.50, fee_rate=0.072) == pytest.approx(0.018)
    assert polymarket_fee_per_share(0.90, fee_rate=0.072) == pytest.approx(0.00648)


@pytest.mark.asyncio
async def test_rest_books_full_keeps_fresh_ask_snapshot_when_bid_side_empty():
    client = FakeBooksClient([
        {
            "asset_id": "NO_TOKEN",
            "bids": [],
            "asks": [{"price": "0.02", "size": "500"}],
        }
    ])

    books = await rest_books_full(client, ["NO_TOKEN"])

    assert "NO_TOKEN" in books
    assert books["NO_TOKEN"].ask == pytest.approx(0.02)
    assert books["NO_TOKEN"].bid == pytest.approx(0.0)
    assert books["NO_TOKEN"].asks == [{"price": 0.02, "size": 500.0}]
    assert books["NO_TOKEN"].updated_ts > 0


@pytest.mark.asyncio
async def test_rest_books_full_keeps_bid_only_snapshot_for_take_profit_sells():
    client = FakeBooksClient([
        {
            "asset_id": "NO_TOKEN",
            "bids": [{"price": "0.999", "size": "342.96"}],
            "asks": [],
            "tick_size": "0.001",
            "neg_risk": True,
            "min_order_size": "5",
        }
    ])

    books = await rest_books_full(client, ["NO_TOKEN"])

    assert "NO_TOKEN" in books
    assert books["NO_TOKEN"].bid == pytest.approx(0.999)
    assert books["NO_TOKEN"].ask == pytest.approx(0.0)
    assert books["NO_TOKEN"].bids == [{"price": 0.999, "size": 342.96}]
    assert books["NO_TOKEN"].asks == []
    assert books["NO_TOKEN"].tick_size == "0.001"
    assert books["NO_TOKEN"].neg_risk is True
    assert books["NO_TOKEN"].order_min_size == pytest.approx(5.0)
    assert books["NO_TOKEN"].updated_ts > 0


class _BadTokenBooksResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or []
        self.text = text

    def json(self):
        return self._payload


class _BadTokenBooksClient:
    def __init__(self):
        self.requests = []

    async def post(self, url, json=None, timeout=None):
        tokens = [row["token_id"] for row in json]
        self.requests.append(tokens)
        if "BAD_TOKEN" in tokens:
            return _BadTokenBooksResponse(400, text="bad token")
        return _BadTokenBooksResponse(
            200,
            [{"asset_id": token, "bids": [{"price": "0.1", "size": "1"}], "asks": [{"price": "0.2", "size": "1"}]} for token in tokens],
        )


@pytest.mark.asyncio
async def test_rest_books_full_bisects_failed_batch_and_keeps_valid_tokens():
    client = _BadTokenBooksClient()

    books = await rest_books_full(client, ["GOOD_1", "BAD_TOKEN", "GOOD_2"])

    assert set(books) == {"GOOD_1", "GOOD_2"}
    assert ["GOOD_1", "BAD_TOKEN", "GOOD_2"] in client.requests
    assert ["BAD_TOKEN"] in client.requests


def test_build_arb_plan_subtracts_price_dependent_leg_fees_gas_and_buffer():
    now = 1000.0
    yes = Book(bid=0.47, ask=0.48, asks=[{"price": 0.48, "size": 10}], updated_ts=now)
    no = Book(bid=0.48, ask=0.49, asks=[{"price": 0.49, "size": 10}], updated_ts=now)

    accepted = build_arb_plan(
        yes,
        no,
        order_limit_usd=5.0,
        min_edge=0.001,
        fee_rate=0.001,
        gas_per_share=0.001,
        stale_quote_buffer=0.001,
        now=now,
    )
    rejected = build_arb_plan(
        yes,
        no,
        order_limit_usd=5.0,
        min_edge=0.028,
        fee_rate=0.001,
        gas_per_share=0.001,
        stale_quote_buffer=0.001,
        now=now,
    )

    assert accepted is not None
    assert accepted.edge_per_pair < 0.03
    assert rejected is None


def test_build_arb_plan_requires_per_leg_min_notional_and_integer_sizes_for_limit_fok():
    now = 1000.0
    yes = Book(bid=0.89, ask=0.90, asks=[{"price": 0.90, "size": 20}], updated_ts=now)
    no = Book(bid=0.07, ask=0.08, asks=[{"price": 0.08, "size": 20}], updated_ts=now)

    too_small = build_arb_plan(
        yes,
        no,
        order_limit_usd=5.0,
        min_edge=0.001,
        min_order_notional_usd=1.0,
        share_size_increment=1.0,
        max_book_age_ms=0,
        now=now,
    )
    enough_cap = build_arb_plan(
        yes,
        no,
        order_limit_usd=13.0,
        min_edge=0.001,
        min_order_notional_usd=1.0,
        share_size_increment=1.0,
        max_book_age_ms=0,
        now=now,
    )

    assert too_small is None
    assert enough_cap is not None
    assert enough_cap.size == 13.0
    assert enough_cap.yes_cost_est >= 1.0
    assert enough_cap.no_cost_est >= 1.0
    assert enough_cap.first_leg == "YES"
    assert enough_cap.second_leg == "NO"


def test_build_arb_plan_caps_smaller_second_leg_value_and_sends_larger_first():
    now = 1000.0
    yes = Book(bid=0.89, ask=0.90, asks=[{"price": 0.90, "size": 50}], updated_ts=now)
    no = Book(bid=0.07, ask=0.08, asks=[{"price": 0.08, "size": 50}], updated_ts=now)

    capped = build_arb_plan(
        yes,
        no,
        order_limit_usd=50.0,
        min_edge=0.001,
        share_size_increment=1.0,
        second_leg_max_order_value_usd=1.0,
        max_book_age_ms=0,
        now=now,
    )

    assert capped is not None
    assert capped.first_leg == "YES"
    assert capped.second_leg == "NO"
    assert capped.no_cost_est <= 1.0
    assert capped.no_cost_est > 0.80
    assert capped.size == 11.0
    assert capped.total_cost_est <= 50.0


def test_build_arb_plan_sizes_from_first_leg_dollar_value():
    now = 1000.0
    yes = Book(bid=0.89, ask=0.90, asks=[{"price": 0.90, "size": 50}], updated_ts=now)
    no = Book(bid=0.07, ask=0.08, asks=[{"price": 0.08, "size": 50}], updated_ts=now)

    capped = build_arb_plan(
        yes,
        no,
        order_limit_usd=3.0,
        min_edge=0.001,
        share_size_increment=0.0001,
        first_leg_order_value_usd=1.0,
        second_leg_max_order_value_usd=1.0,
        max_book_age_ms=0,
        now=now,
    )

    assert capped is not None
    assert capped.first_leg == "YES"
    assert capped.yes_cost_est <= 1.0
    assert capped.yes_cost_est == pytest.approx(1.0, abs=0.001)
    assert capped.no_cost_est <= 1.0
    assert capped.total_cost_est <= 3.0


def test_build_arb_plan_fixed_dollar_mode_satisfies_each_leg_min_notional_with_unequal_shares():
    now = 1000.0
    yes = Book(bid=0.02, ask=0.03, asks=[{"price": 0.03, "size": 200}], updated_ts=now)
    no = Book(bid=0.95, ask=0.96, asks=[{"price": 0.96, "size": 200}], updated_ts=now)

    plan = build_arb_plan(
        yes,
        no,
        order_limit_usd=3.0,
        min_edge=0.001,
        min_order_notional_usd=1.0,
        share_size_increment=1.0,
        first_leg_order_value_usd=1.0,
        second_leg_max_order_value_usd=1.0,
        max_book_age_ms=0,
        now=now,
    )

    assert plan is not None
    assert plan.first_leg == "NO"
    assert plan.second_leg == "YES"
    assert plan.no_cost_est == pytest.approx(1.0, abs=0.001)
    assert plan.yes_cost_est == pytest.approx(1.0, abs=0.001)
    assert plan.no_size != plan.yes_size


def test_build_arb_plan_rejects_when_smaller_second_leg_cap_exhausts_total_order_limit():
    now = 1000.0
    yes = Book(bid=0.89, ask=0.90, asks=[{"price": 0.90, "size": 50}], updated_ts=now)
    no = Book(bid=0.07, ask=0.08, asks=[{"price": 0.08, "size": 50}], updated_ts=now)

    impossible = build_arb_plan(
        yes,
        no,
        order_limit_usd=5.0,
        min_edge=0.001,
        min_order_notional_usd=1.0,
        share_size_increment=1.0,
        second_leg_max_order_value_usd=1.0,
        max_book_age_ms=0,
        now=now,
    )

    assert impossible is None


def test_runner_default_rejects_sub_dollar_legs_before_submit():
    now = 1000.0
    r = make_runner(SequencedExec())
    r.books["YES"] = Book(bid=0.89, ask=0.90, asks=[{"price": 0.90, "size": 20}], updated_ts=now)
    r.books["NO"] = Book(bid=0.07, ask=0.08, asks=[{"price": 0.08, "size": 20}], updated_ts=now)

    plan = r._plan_from_cfg({
        "order_limit_usd": 5.0,
        "min_edge": 0.001,
        "min_order_notional_usd": 1.0,
        "share_size_increment": 1.0,
        "max_book_age_ms": 0,
    })

    assert plan is None


def test_runner_fixed_dollar_sizing_buys_one_dollar_of_each_leg_without_equal_share_offset():
    now = 1000.0
    r = make_runner(SequencedExec())
    r.books["YES"] = Book(bid=0.89, ask=0.90, asks=[{"price": 0.90, "size": 20}], updated_ts=now)
    r.books["NO"] = Book(bid=0.07, ask=0.08, asks=[{"price": 0.08, "size": 20}], updated_ts=now)

    plan = r._plan_from_cfg({
        "order_limit_usd": 3.0,
        "first_leg_order_value_usd": 1.0,
        "second_leg_max_order_value_usd": 1.0,
        "min_edge": 0.001,
        "share_size_increment": 1.0,
        "min_order_notional_usd": 1.0,
        "max_book_age_ms": 0,
    })

    assert plan is not None
    assert plan.first_leg == "YES"
    assert plan.second_leg == "NO"
    assert plan.yes_size == pytest.approx(1.1111, abs=0.0001)
    assert plan.no_size == pytest.approx(12.5, abs=0.001)
    assert plan.yes_cost_est == pytest.approx(1.0, abs=0.001)
    assert plan.no_cost_est == pytest.approx(1.0, abs=0.001)
    assert plan.total_cost_est <= 3.0


def test_runner_explicit_false_sub_min_notional_flag_still_rejects_sub_dollar_legs():
    now = 1000.0
    r = make_runner(SequencedExec())
    r.books["YES"] = Book(bid=0.89, ask=0.90, asks=[{"price": 0.90, "size": 20}], updated_ts=now)
    r.books["NO"] = Book(bid=0.07, ask=0.08, asks=[{"price": 0.08, "size": 20}], updated_ts=now)

    plan = r._plan_from_cfg({
        "order_limit_usd": 5.0,
        "min_edge": 0.001,
        "min_order_notional_usd": 1.0,
        "share_size_increment": 1.0,
        "allow_sub_min_order_notional": False,
        "max_book_age_ms": 0,
    })

    assert plan is None


def test_clob_response_with_error_msg_is_not_a_fill_even_when_success_true():
    resp = {
        "success": True,
        "status": "",
        "orderID": "",
        "errorMsg": "invalid amounts, the market buy orders maker amount supports a max accuracy of 2 decimals",
    }

    assert clob_response_indicates_fill(resp) is False


def test_clob_response_with_delayed_status_is_not_a_fill_even_with_order_id():
    resp = {
        "success": True,
        "status": "delayed",
        "orderID": "0xeb2ec887912fee60ca3ab6b0e3f707833b1a7f9b3fd6dd8144d8d8064b07b6dd",
        "errorMsg": "",
    }

    assert clob_response_indicates_fill(resp) is False


def test_submit_pair_does_not_record_error_msg_responses_as_filled_legs():
    ex = SequencedExec(batch_responses=[[
        {"success": True, "status": "", "orderID": "", "errorMsg": "invalid amounts"},
        {"success": True, "status": "", "orderID": "", "errorMsg": "invalid amounts"},
    ]])
    r = make_runner(ex)

    ok, pnl, filled, responses, state = r._submit_pair(plan(), {"use_limit_fok": True, "arb_execution_mode": "batch_fok"})

    assert ok is False
    assert pnl == 0.0
    assert filled == []
    assert state == "FLAT_NO_FILL"


def test_submit_pair_test_mode_buys_only_lower_leg_sized_to_one_dollar_minimum():
    ex = SequencedExec(single_responses=[{"success": True, "orderID": "no-test"}])
    r = make_runner(ex)
    skewed = ArbPlan(
        yes_size=8,
        no_size=8,
        size=8,
        yes_limit=0.490,
        no_limit=0.054,
        yes_cost_est=3.92,
        no_cost_est=0.432,
        total_cost_est=4.352,
        avg_sum_est=0.544,
        edge_per_pair=0.4333,
        first_leg="NO",
        second_leg="YES",
    )

    ok, pnl, filled, responses, state = r._submit_pair(skewed, {
        "use_limit_fok": True,
        "arb_test_mode": True,
        "test_mode_min_notional_usd": 1.0,
        "share_size_increment": 1.0,
    })

    assert ok is False
    assert pnl == 0.0
    assert state == "TEST_LOWER_LEG_FILLED"
    assert filled == ["NO"]
    assert len(ex.batches) == 0
    assert len(ex.singles) == 1
    order = ex.singles[0]
    assert order.token_id == "NO_TOKEN"
    assert order.side == "BUY"
    assert float(order.price) == pytest.approx(0.054)
    assert float(order.size) == pytest.approx(19.0)
    assert float(order.price * order.size) >= 1.0
    assert r.residual_inventory["NO"] == pytest.approx(19.0)
    assert responses["NO"]["_attempt_size"] == pytest.approx(19.0)


@pytest.mark.asyncio
async def test_hot_path_test_mode_buys_lower_leg_when_paired_plan_blocked_by_order_limit():
    ex = SequencedExec(single_responses=[{"success": True, "orderID": "cheap-no"}])
    r = make_runner(ex)
    now = 1000.0
    r.books = {
        "YES": Book(bid=0.92, ask=0.94, asks=[{"price": 0.94, "size": 100.0}], updated_ts=now),
        "NO": Book(bid=0.0, ask=0.01, asks=[{"price": 0.01, "size": 500.0}], updated_ts=now),
    }
    r.books_by_slug = {"test-market": r.books}
    r.market_pairs = [r.ev]

    original_time = time.time
    try:
        import polybot.live.arb_sniper as arb_sniper
        arb_sniper.time.time = lambda: now
        await r.maybe_execute_hot_path({
            "use_limit_fok": True,
            "arb_test_mode": True,
            "test_mode_min_notional_usd": 1.0,
            "share_size_increment": 1.0,
            "order_limit_usd": 3.0,
            "min_edge": 0.003,
            "max_book_age_ms": 150,
        })
    finally:
        arb_sniper.time.time = original_time

    assert len(ex.singles) == 1
    order = ex.singles[0]
    assert order.token_id == "NO_TOKEN"
    assert float(order.price) == pytest.approx(0.01)
    assert float(order.size) == pytest.approx(100.0)
    assert r.writer.order_attempts
    args, kwargs = r.writer.order_attempts[0]
    assert args[3] == "NO"
    assert args[8] == pytest.approx(1.0)
    assert kwargs["signal"]["state"] == "TEST_LOWER_LEG_FILLED"


def test_submit_pair_test_mode_does_not_interfere_when_both_legs_already_clear_minimum():
    ex = SequencedExec(single_responses=[{"success": True, "orderID": "yes-1"}, {"success": True, "orderID": "no-1"}])
    r = make_runner(ex)

    ok, pnl, filled, responses, state = r._submit_pair(plan(), {
        "use_limit_fok": True,
        "arb_test_mode": True,
        "test_mode_min_notional_usd": 1.0,
        "share_size_increment": 1.0,
    })

    assert ok is True
    assert filled == ["YES", "NO"]
    assert state == "SEQUENTIAL_HEDGED"
    assert len(ex.batches) == 0
    assert len(ex.singles) == 2


def test_submit_pair_default_is_sequential_budgeted_and_second_leg_uses_profit_cap():
    ex = SequencedExec(
        single_responses=[{"success": True, "orderID": "yes-1"}, {"success": True, "orderID": "no-1"}],
    )
    r = make_runner(ex)

    ok, pnl, filled, responses, state = r._submit_pair(plan(), {"use_limit_fok": True, "fee_per_share": 0.0, "min_edge": 0.005})

    assert ok is True
    assert filled == ["YES", "NO"]
    assert state == "SEQUENTIAL_HEDGED"
    assert len(ex.batches) == 0
    assert len(ex.singles) == 2
    assert ex.singles[0].token_id == "YES_TOKEN"
    assert float(ex.singles[0].price) == pytest.approx(0.40)
    assert ex.singles[1].token_id == "NO_TOKEN"
    assert float(ex.singles[1].price) == pytest.approx(0.595)
    assert all(o.order_type == "FOK" and o.use_limit_order for o in ex.singles)
    assert all(o.tick_size == "0.001" and o.neg_risk is True for o in ex.singles)
    assert responses["NO"]["_attempt_price"] == pytest.approx(0.595)


def test_submit_pair_fixed_dollar_uses_market_buy_path_to_preserve_one_dollar_notional():
    ex = SequencedExec(single_responses=[{"success": True, "orderID": "no-1"}, {"success": True, "orderID": "yes-1"}])
    r = make_runner(ex)
    fixed = ArbPlan(
        yes_size=2.2727,
        no_size=2.0833,
        size=2.0833,
        yes_limit=0.440,
        no_limit=0.480,
        yes_cost_est=1.0,
        no_cost_est=1.0,
        total_cost_est=2.0,
        avg_sum_est=0.920,
        edge_per_pair=0.0433,
        first_leg="NO",
        second_leg="YES",
    )

    ok, pnl, filled, responses, state = r._submit_pair(fixed, {"use_limit_fok": True, "fee_per_share": 0.0, "min_edge": 0.005})

    assert ok is True
    assert filled == ["NO", "YES"]
    assert state == "SEQUENTIAL_HEDGED"
    assert len(ex.singles) == 2
    assert [o.token_id for o in ex.singles] == ["NO_TOKEN", "YES_TOKEN"]
    assert all(o.order_type == "FOK" and not o.use_limit_order for o in ex.singles)
    # Fixed-dollar first leg can cross up to the profit-preserving cap to avoid
    # FOK kills after tiny book moves, while synthetic size shrinks to keep the
    # market-buy notional at the planned $1.
    assert float(ex.singles[0].price) == pytest.approx(0.480)
    assert float(ex.singles[0].size) == pytest.approx(2.0833)
    assert float(ex.singles[0].size * ex.singles[0].price) == pytest.approx(1.0, abs=0.0001)
    # The second fixed-dollar FOK is allowed to cross deeper while preserving the
    # pair's profitability cap; synthetic size shrinks so price*size still sends
    # exactly the planned $1 market-buy amount.
    assert float(ex.singles[1].price) >= 0.440 - 1e-9
    assert float(ex.singles[1].size * ex.singles[1].price) == pytest.approx(1.0, abs=0.0001)
    assert responses["NO"]["_attempt_size"] == pytest.approx(float(ex.singles[0].size))
    assert responses["YES"]["_attempt_size"] == pytest.approx(float(ex.singles[1].size))


def test_submit_pair_fixed_dollar_second_leg_crosses_to_profitability_threshold_for_extreme_crypto_skew():
    ex = SequencedExec(single_responses=[{"success": True, "orderID": "yes-1"}, {"success": True, "orderID": "no-1"}])
    r = make_runner(ex)
    fixed = ArbPlan(
        yes_size=1.0526,
        no_size=100.0,
        size=1.0526,
        yes_limit=0.950,
        no_limit=0.010,
        yes_cost_est=1.0,
        no_cost_est=1.0,
        total_cost_est=2.0,
        avg_sum_est=0.960,
        edge_per_pair=0.0348,
        first_leg="YES",
        second_leg="NO",
    )

    ok, pnl, filled, responses, state = r._submit_pair(fixed, {
        "use_limit_fok": True,
        "fee_per_share": 0.0,
        "polymarket_taker_fee_rate": 0.0,
        "min_edge": 0.003,
        "stale_quote_buffer": 0.0,
        "profit_slippage_first_leg": False,
    })

    assert ok is True
    assert filled == ["YES", "NO"]
    assert state == "SEQUENTIAL_HEDGED"
    assert float(ex.singles[0].price) == pytest.approx(0.950)
    assert float(ex.singles[1].price) == pytest.approx(0.047)
    assert float(ex.singles[1].size * ex.singles[1].price) == pytest.approx(1.0, abs=0.0001)
    assert responses["NO"]["_attempt_price"] == pytest.approx(0.047)


def test_submit_pair_fixed_dollar_first_leg_retry_crosses_after_flat_rejection_for_crypto_race():
    ex = SequencedExec(single_responses=[
        RuntimeError("order couldn't be fully filled. FOK orders are fully filled or killed"),
        {"success": True, "orderID": "no-retry"},
        {"success": True, "orderID": "yes-1"},
    ])
    r = make_runner(ex)
    fixed = ArbPlan(
        yes_size=4.7619,
        no_size=1.4085,
        size=1.4085,
        yes_limit=0.210,
        no_limit=0.710,
        yes_cost_est=1.0,
        no_cost_est=1.0,
        total_cost_est=2.0,
        avg_sum_est=0.920,
        edge_per_pair=0.0522,
        first_leg="NO",
        second_leg="YES",
    )

    ok, pnl, filled, responses, state = r._submit_pair(fixed, {
        "use_limit_fok": True,
        "fee_per_share": 0.0,
        "polymarket_taker_fee_rate": 0.072,
        "min_edge": 0.003,
        "stale_quote_buffer": 0.001,
    })

    assert ok is True
    assert filled == ["NO", "YES"]
    assert state == "SEQUENTIAL_HEDGED"
    # The production failure was a FOK kill at NO@0.710 despite ~5c edge.
    # We now spend the available edge as slippage room on the first leg too.
    assert len(ex.singles) == 3
    assert float(ex.singles[0].price) == pytest.approx(0.710)
    assert float(ex.singles[1].price) > 0.710
    assert float(ex.singles[1].price) < 0.770
    assert float(ex.singles[1].size * ex.singles[1].price) == pytest.approx(1.0, abs=0.0001)
    assert "_initial_attempt" in responses["NO"]
    assert responses["NO"]["_attempt_price"] == pytest.approx(float(ex.singles[1].price))


def test_update_top_of_book_accepts_zero_bid_with_live_ask_to_prevent_stale_quote():
    r = make_runner(SequencedExec())
    r.books_by_slug = {"test-market": r.books}
    r.market_pairs = [r.ev]

    assert r._update_top_of_book("NO_TOKEN", 0.0, 0.99) is True

    assert r.books["NO"].bid == pytest.approx(0.0)
    assert r.books["NO"].ask == pytest.approx(0.99)
    assert r.books["NO"].updated_ts > 0


def test_submit_pair_sequential_does_not_submit_second_when_first_rejects():
    ex = SequencedExec(single_responses=[{"success": False, "error": "first missed"}])
    r = make_runner(ex)

    ok, pnl, filled, responses, state = r._submit_pair(plan(), {"use_limit_fok": True, "fee_per_share": 0.0, "min_edge": 0.005})

    assert ok is False
    assert pnl == 0.0
    assert filled == []
    assert state == "FLAT_NO_FILL"
    assert len(ex.batches) == 0
    assert len(ex.singles) == 1
    assert "NO" not in responses


def test_submit_pair_sequential_treats_first_leg_fok_exception_as_flat_rejection():
    ex = SequencedExec(single_responses=[RuntimeError("order couldn't be fully filled. FOK orders are fully filled or killed")])
    r = make_runner(ex)

    ok, pnl, filled, responses, state = r._submit_pair(plan(), {"use_limit_fok": True, "fee_per_share": 0.0, "min_edge": 0.005})

    assert ok is False
    assert pnl == 0.0
    assert filled == []
    assert state == "FLAT_NO_FILL"
    assert len(ex.singles) == 1
    assert "FOK orders" in responses["YES"]["error"]
    assert "NO" not in responses


def test_submit_pair_legacy_batch_fok_orders_when_configured():
    ex = SequencedExec(batch_responses=[[{"success": True, "orderID": "yes-1"}, {"success": True, "orderID": "no-1"}]])
    r = make_runner(ex)

    ok, pnl, filled, responses, state = r._submit_pair(plan(), {"use_limit_fok": True, "arb_execution_mode": "batch_fok"})

    assert ok is True
    assert filled == ["YES", "NO"]
    assert state == "HEDGED"
    assert len(ex.batches) == 1
    assert len(ex.batches[0]) == 2
    assert all(o.order_type == "FOK" and o.use_limit_order for o in ex.batches[0])
    assert all(o.tick_size == "0.001" and o.neg_risk is True for o in ex.batches[0])


def test_submit_pair_allows_config_to_override_v2_order_metadata():
    ex = SequencedExec(single_responses=[{"success": True, "orderID": "yes-1"}, {"success": True, "orderID": "no-1"}])
    r = make_runner(ex)

    r._submit_pair(plan(), {"use_limit_fok": True, "tick_size": "0.01", "neg_risk": False, "builder_code": "0x" + "b" * 64})

    assert all(o.tick_size == "0.01" and o.neg_risk is False for o in ex.singles)
    assert all(o.builder_code == "0x" + "b" * 64 for o in ex.singles)


def test_one_batch_leg_filled_rescues_by_buying_missing_side():
    ex = SequencedExec(
        batch_responses=[[{"success": True, "orderID": "yes-1"}, {"success": False, "error": "miss"}]],
        single_responses=[{"success": True, "orderID": "no-rescue"}],
    )
    r = make_runner(ex)
    r.books["NO"] = Book(bid=0.53, ask=0.54, asks=[{"price": 0.54, "size": 5}], updated_ts=1000)

    ok, pnl, filled, responses, state = r._submit_pair(
        plan(),
        {"use_limit_fok": True, "arb_execution_mode": "batch_fok", "fee_per_share": 0.0, "min_edge": 0.005, "max_rescue_slippage": 0.01},
    )

    assert ok is True
    assert state == "RESCUED_HEDGED"
    assert filled == ["YES", "NO"]
    assert len(ex.singles) == 1
    assert ex.singles[0].token_id == "NO_TOKEN"
    assert ex.singles[0].side == "BUY"


def test_one_batch_leg_filled_and_rescue_fails_holds_residual_without_stopping():
    ex = SequencedExec(
        batch_responses=[[{"success": True, "orderID": "yes-1"}, {"success": False, "error": "miss"}]],
        single_responses=[{"success": False, "error": "rescue rejected"}],
    )
    r = make_runner(ex)
    r.books["NO"] = Book(bid=0.90, ask=0.91, asks=[{"price": 0.91, "size": 5}], updated_ts=1000)

    ok, pnl, filled, responses, state = r._submit_pair(
        plan(),
        {"use_limit_fok": True, "arb_execution_mode": "batch_fok", "fee_per_share": 0.0, "min_edge": 0.005, "max_rescue_slippage": 0.01},
    )

    assert ok is False
    assert state == "HOLD_RESIDUAL"
    assert filled == ["YES"]
    assert r.residual_inventory["YES"] == pytest.approx(5)
    assert r.residual_inventory["NO"] == pytest.approx(0)
    assert r.writer.status is None


def test_rescue_exception_after_partial_fill_still_records_filled_leg_and_residual():
    ex = SequencedExec(
        batch_responses=[[{"success": True, "status": "matched", "orderID": "yes-1"}, {"success": False, "error": "miss"}]],
        single_responses=[RuntimeError("invalid amount for a marketable BUY order ($0.15), min size: $1")],
    )
    r = make_runner(ex)
    r.books["NO"] = Book(bid=0.53, ask=0.54, asks=[{"price": 0.54, "size": 5}], updated_ts=1000)

    ok, pnl, filled, responses, state = r._submit_pair(
        plan(),
        {"use_limit_fok": True, "arb_execution_mode": "batch_fok", "fee_per_share": 0.0, "min_edge": 0.005, "max_rescue_slippage": 0.01},
    )
    asyncio.run(r._record_execution(r.ev, plan(), ok, pnl, filled, responses, {"use_limit_fok": True}, state))

    assert ok is False
    assert state == "HOLD_RESIDUAL"
    assert filled == ["YES"]
    assert "min size: $1" in responses["NO"]["error"]
    assert r.residual_inventory["YES"] == pytest.approx(5)
    statuses = [args[9] for args, _kwargs in r.writer.order_attempts]
    assert statuses == ["filled", "rejected"]
    assert len(r.writer.fills) == 1
    assert r.writer.fills[0][1]["kind"] == "ARB_RESIDUAL"


def test_residual_inventory_blocks_new_entries_until_resolved():
    ex = SequencedExec(batch_responses=[[{"success": True, "orderID": "yes-1"}, {"success": True, "orderID": "no-1"}]])
    r = make_runner(ex)
    r.residual_inventory["YES"] = 2.0
    r.books["YES"] = Book(bid=0.40, ask=0.41, asks=[{"price": 0.41, "size": 10}], updated_ts=1000)
    r.books["NO"] = Book(bid=0.50, ask=0.51, asks=[{"price": 0.51, "size": 10}], updated_ts=1000)

    asyncio.run(r.maybe_execute_hot_path({"order_limit_usd": 5, "min_edge": 0.01, "max_book_age_ms": 999999, "cooldown_ms": 0}))

    assert ex.batches == []


def test_quote_health_summary_uses_in_memory_quote_ages_not_dashboard_write_time():
    r = make_runner(SequencedExec())
    r.books["YES"] = Book(bid=0.45, ask=0.46, asks=[{"price": 0.46, "size": 10}], updated_ts=1000.0)
    r.books["NO"] = Book(bid=0.49, ask=0.50, asks=[{"price": 0.50, "size": 10}], updated_ts=999.7)

    health = r.quote_health_summary({"max_book_age_ms": 150, "min_edge": 0.001}, now=1000.0)

    assert health["tokens"] == 2
    assert health["fresh_pairs"] == 0
    assert health["stale_pairs"] == 1
    assert health["max_age_ms"] == pytest.approx(300.0)
    assert health["opportunity_now"] is False


def test_health_message_documents_opportunity_and_freshness_for_dashboard_logs():
    r = make_runner(SequencedExec())
    r.cached_status = "running"
    import time
    now = time.time()
    r.books["YES"] = Book(bid=0.45, ask=0.46, asks=[{"price": 0.46, "size": 10}], updated_ts=now)
    r.books["NO"] = Book(bid=0.48, ask=0.49, asks=[{"price": 0.49, "size": 10}], updated_ts=now)

    msg, level = r._health_message({"max_book_age_ms": 150, "min_edge": 0.001, "order_limit_usd": 10})

    assert level == "INFO"
    assert "Data health" in msg
    assert "fresh_pairs=1/1" in msg
    assert "opportunity_now=True" in msg




def test_weather_outlier_health_message_identifies_oldest_tokens_and_missing_last_poll():
    r = make_weather_outlier_runner()
    r.cached_status = "running"
    now = time.time()
    for books in r.books_by_slug.values():
        books["YES"].updated_ts = now
        books["NO"].updated_ts = now
    stale_pair = r.market_pairs[1]
    r.books_by_slug[stale_pair["slug"]]["NO"].updated_ts = now - 65.0
    r.books_by_slug[stale_pair["slug"]]["YES"].updated_ts = now - 12.0
    r.last_rest_missing_tokens = [stale_pair["no_token"], stale_pair["yes_token"]]
    r.last_rest_requested_tokens = 22
    r.last_rest_returned_tokens = 20

    msg, level = r._weather_outlier_health_message({
        "kind": "weather_outlier_sniper",
        "max_book_age_ms": 5000,
        "scan_max_book_age_ms": 5000,
    })

    assert level == "WARNING"
    assert "oldest_tokens=" in msg
    assert stale_pair["slug"] in msg
    assert f"NO:{stale_pair['no_token']}" in msg
    assert "missing_last_poll=2/22" in msg
    assert stale_pair["no_token"] in msg


def test_weather_outlier_health_message_warns_on_extreme_oldest_age():
    r = make_weather_outlier_runner()
    r.cached_status = "running"
    now = time.time()
    stale_pair = r.market_pairs[0]
    r.books_by_slug[stale_pair["slug"]]["NO"].updated_ts = now - 11.0

    msg, level = r._weather_outlier_health_message({
        "kind": "weather_outlier_sniper",
        "max_book_age_ms": 5000,
        "scan_max_book_age_ms": 5000,
        "weather_outlier_diag_warn_age_ms": 10000,
    })

    assert level == "WARNING"
    assert "oldest_tokens=" in msg

def test_maybe_log_health_allows_zero_interval_to_disable_dashboard_status_logs():
    r = make_runner(SequencedExec())
    r.last_health_log_ts = 0

    asyncio.run(r.maybe_log_health({"health_log_interval_seconds": 0}))

    assert r.writer.events == []
    assert r.last_health_log_ts == 0


def test_record_execution_dashboard_log_says_opportunity_spotted():
    r = make_runner(SequencedExec())
    p = plan()

    asyncio.run(r._record_execution(r.ev, p, True, 0.25, ["YES", "NO"], {"YES": {"success": True}, "NO": {"success": True}}, {}, "HEDGED"))

    messages = [args[1] for args, _kwargs in r.writer.events]
    assert any("Opportunity spotted" in message and "market=test-market" in message for message in messages)


def test_record_execution_dashboard_log_includes_attempt_status_and_error_details():
    r = make_runner(SequencedExec())
    p = plan()
    responses = {
        "YES": {"success": False, "error": "invalid amount for a marketable BUY order ($0.40), min size: $1"},
        "NO": {"success": False, "error": "invalid amount for a marketable BUY order ($0.55), min size: $1"},
    }

    asyncio.run(r._record_execution(r.ev, p, False, 0.0, [], responses, {}, "FLAT_NO_FILL"))

    messages = [args[1] for args, _kwargs in r.writer.events]
    assert any("attempts=[" in message and "YES=rejected" in message and "min size: $1" in message for message in messages)


def test_market_websocket_can_be_disabled_for_rest_polled_weather_shards():
    assert market_websocket_enabled({}) is True
    assert market_websocket_enabled({"market_websocket_enabled": False}) is False
    assert market_websocket_enabled({"websocket_enabled": "false"}) is False
    assert market_websocket_enabled({"disable_market_websocket": True}) is False



def test_websocket_subscription_enables_custom_best_bid_ask_events():
    r = make_runner(SequencedExec())

    payload = r._subscription_payload(["YES_TOKEN", "NO_TOKEN"], {})

    assert payload == {
        "type": "market",
        "assets_ids": ["YES_TOKEN", "NO_TOKEN"],
        "custom_feature_enabled": True,
    }


def test_top_of_book_update_drops_old_depth_until_batched_rest_refresh():
    r = make_runner(SequencedExec())
    r.books["YES"] = Book(bid=0.40, ask=0.41, asks=[{"price": 0.41, "size": 10}], bids=[{"price": 0.40, "size": 10}], updated_ts=1000)

    assert r._update_top_of_book("YES_TOKEN", 0.42, 0.43) is True

    assert r.books["YES"].bid == pytest.approx(0.42)
    assert r.books["YES"].ask == pytest.approx(0.43)
    assert r.books["YES"].asks is None
    assert r.books["YES"].bids is None


def test_apply_full_book_restores_depth_after_top_of_book_update():
    r = make_runner(SequencedExec())
    r._update_top_of_book("YES_TOKEN", 0.42, 0.43)

    ok = r._apply_full_book("YES_TOKEN", Book(bid=0.41, ask=0.42, bids=[{"price": 0.41, "size": 5}], asks=[{"price": 0.42, "size": 5}], updated_ts=1234))

    assert ok is True
    assert r.books["YES"].bid == pytest.approx(0.41)
    assert r.books["YES"].ask == pytest.approx(0.42)
    assert r.books["YES"].asks == [{"price": 0.42, "size": 5}]
    assert r.books["YES"].updated_ts == pytest.approx(1234)


def test_latency_stats_reports_bounded_window_percentiles():
    stats = LatencyStats(maxlen=3)
    for value in [100.0, 10.0, 20.0, 30.0]:
        stats.add(value)

    summary = stats.summary()

    assert summary["count"] == 3
    assert summary["min_ms"] == pytest.approx(10.0)
    assert summary["median_ms"] == pytest.approx(20.0)
    assert summary["max_ms"] == pytest.approx(30.0)


def test_deterministic_rest_poll_phase_spreads_shards_without_changing_interval():
    interval_ms = 225
    phases = [deterministic_rest_poll_phase_ms(f"live_weather_arb_sniper_city_{i}", interval_ms) for i in range(10)]

    assert phases == [deterministic_rest_poll_phase_ms(f"live_weather_arb_sniper_city_{i}", interval_ms) for i in range(10)]
    assert all(0 <= p < interval_ms for p in phases)
    assert len(set(phases)) >= 8


def test_configured_rest_poll_phase_overrides_hash_and_wraps_to_interval():
    assert deterministic_rest_poll_phase_ms("strategy", 225, configured_phase=500) == pytest.approx(50.0)


def test_health_message_includes_rest_and_submit_latency_when_available():
    r = make_runner(SequencedExec())
    r.cached_status = "running"
    import time
    now = time.time()
    r.books["YES"] = Book(bid=0.45, ask=0.46, asks=[{"price": 0.46, "size": 10}], updated_ts=now)
    r.books["NO"] = Book(bid=0.48, ask=0.49, asks=[{"price": 0.49, "size": 10}], updated_ts=now)
    r.rest_book_latency.add(12.0)
    r.submit_latency.add(85.0)

    msg, level = r._health_message({"max_book_age_ms": 50, "min_edge": 0.001, "order_limit_usd": 10})

    assert level == "INFO"
    assert "rest_books_median=12ms" in msg
    assert "submit_median=85ms" in msg


def test_write_book_rows_bulk_upserts_all_monitored_tokens_in_one_db_roundtrip():
    r = make_runner(SequencedExec())
    r.market_pairs = [
        {
            "slug": "m1",
            "title": "Market 1",
            "yes_token": "YES1",
            "no_token": "NO1",
            "yes_label": "Yes",
            "no_label": "No",
        },
        {
            "slug": "m2",
            "title": "Market 2",
            "yes_token": "YES2",
            "no_token": "NO2",
            "yes_label": "Yes",
            "no_label": "No",
        },
    ]
    r.books_by_slug = {
        "m1": {
            "YES": Book(bid=0.10, ask=0.11, bids=[{"price": 0.10, "size": 1}], asks=[{"price": 0.11, "size": 2}]),
            "NO": Book(bid=0.88, ask=0.89, bids=[{"price": 0.88, "size": 3}], asks=[{"price": 0.89, "size": 4}]),
        },
        "m2": {
            "YES": Book(bid=0.20, ask=0.21, bids=[{"price": 0.20, "size": 5}], asks=[{"price": 0.21, "size": 6}]),
            "NO": Book(bid=0.78, ask=0.79, bids=[{"price": 0.78, "size": 7}], asks=[{"price": 0.79, "size": 8}]),
        },
    }

    asyncio.run(r._write_book_rows())

    assert r.writer.book_calls == 1
    assert [row["token"] for row in r.writer.book_rows] == ["YES1", "NO1", "YES2", "NO2"]
    assert r.writer.book_rows[0]["bids"] == [{"px": 0.10, "sz": 1}]
    assert r.writer.book_rows[0]["asks"] == [{"px": 0.11, "sz": 2}]
    assert len(r.writer.ticks) == 4
