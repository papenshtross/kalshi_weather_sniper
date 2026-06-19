from polybot.live.nwws_weather_execution import (
    ExecutionCircuitBreaker,
    InMemoryBookStore,
    PreparedOrderCache,
    StationTarget,
    TargetIndex,
    WeatherLockExecutionEngine,
    extract_metar_temperature_c,
    parse_nwws_metar,
    threshold_wins,
)


def _targets():
    return TargetIndex([
        StationTarget(b"KLGA", "nyc", "new-york-high", "token", 28),
        StationTarget(b"KORD", "chicago", "chicago-high", "token2", 30),
    ])


def test_parse_routine_metar_positive_temp():
    raw = b"SAUS70 KWBC 151500\nMETAR KLGA 151451Z 18007KT 10SM FEW050 28/14 A2992 RMK AO2"
    hit = parse_nwws_metar(raw, _targets(), received_ns=123)
    assert hit is not None
    assert hit.icao == "KLGA"
    assert hit.temp_c == 28
    assert hit.bulletin_kind == "SAUS/METAR"


def test_parse_special_metar_negative_temp():
    raw = b"SPUS70 KWBC 151505\nSPECI KLGA 151501Z 18007KT 10SM FEW050 M02/M05 A2992 RMK AO2"
    hit = parse_nwws_metar(raw, _targets(), received_ns=123)
    assert hit is not None
    assert hit.icao == "KLGA"
    assert hit.temp_c == -2
    assert hit.bulletin_kind == "SPUS/SPECI"


def test_ignores_non_target_station():
    raw = b"SAUS70 KWBC 151500\nMETAR KJFK 151451Z 18007KT 10SM FEW050 28/14 A2992 RMK AO2"
    assert parse_nwws_metar(raw, _targets(), received_ns=123) is None


def test_extract_skips_missing_temp_group():
    raw = b"SAUS70 KWBC 151500\nMETAR KLGA 151451Z 18007KT 10SM FEW050 //// A2992 RMK AO2"
    pos = raw.find(b"KLGA")
    assert extract_metar_temperature_c(raw, b"KLGA", pos) is None


def test_threshold_directions():
    assert threshold_wins(28, StationTarget(b"KLGA", "nyc", "m", "t", 28, "gte")) is True
    assert threshold_wins(27, StationTarget(b"KLGA", "nyc", "m", "t", 28, "gte")) is False
    assert threshold_wins(27, StationTarget(b"KLGA", "nyc", "m", "t", 28, "lte")) is True
    assert threshold_wins(28, StationTarget(b"KLGA", "nyc", "m", "t", 28, "eq")) is True


def test_in_memory_book_sweep_sorts_and_caps():
    books = InMemoryBookStore()
    book = books.update_ws_message({
        "asset_id": "YES",
        "asks": [{"price": "0.99", "size": "100"}, {"price": "0.40", "size": "3"}, {"price": "0.50", "size": "10"}],
        "bids": [{"price": "0.39", "size": "4"}],
    }, now_ns=100)
    assert book is not None
    assert book.asks == [(0.4, 3.0), (0.5, 10.0), (0.99, 100.0)]
    assert book.ask_sweep(0.98, 3.2) == [(0.4, 3.0), (0.5, 4.0)]


async def test_engine_uses_in_memory_book_before_dry_run_execute():
    raw = b"SAUS70 KWBC 151500\nMETAR KLGA 151451Z 18007KT 10SM FEW050 28/14 A2992 RMK AO2"
    target = StationTarget(b"KLGA", "nyc", "new-york-high", "YES", 28, price_ceiling=0.98, max_notional_usdc=2)
    books = InMemoryBookStore()
    books.update_ws_message({"asset_id": "YES", "asks": [{"price": "0.50", "size": "10"}]})
    engine = WeatherLockExecutionEngine([target], {}, dry_run=True, books=books, max_book_age_ms=1000)
    try:
        decision = await engine.on_stanza(raw)
        assert decision is not None
        assert decision["action"] == "blocked_circuit_breaker"
        assert decision["block_reason"] == "not_armed"
        assert decision["sweep_levels"] == 1
        assert decision["sweep_notional"] == 2
    finally:
        await engine.close()


def test_circuit_breaker_requires_explicit_env_unlock(monkeypatch):
    cb = ExecutionCircuitBreaker(max_event_notional_usdc=10)
    assert cb.can_submit(armed=True, dry_run=False, planned_notional=1) is False
    assert cb.tripped_reason.startswith("missing_env_unlock")
    monkeypatch.setenv("POLYBOT_NWWS_LIVE_UNLOCK", "I_UNDERSTAND_THIS_IS_LIVE")
    assert cb.can_submit(armed=True, dry_run=False, planned_notional=11) is False
    assert cb.tripped_reason == "event_notional_cap"
    assert cb.can_submit(armed=True, dry_run=False, planned_notional=1) is True


def test_prepared_order_cache_is_memory_only_and_rejects_empty_payloads():
    station = StationTarget(b"KLGA", "nyc", "new-york-high", "YES", 28)
    cache = PreparedOrderCache()
    assert cache.get("KLGA") is None
    plan = cache.upsert_presigned(station, [{"order": "already-signed"}])
    assert plan.ready is True
    assert cache.get("KLGA") is plan
    try:
        cache.upsert_presigned(station, [])
    except ValueError as exc:
        assert "non-empty" in str(exc)
    else:
        raise AssertionError("empty payload cache insert should fail")


async def test_engine_submits_only_presigned_payloads_needed_for_sweep(monkeypatch):
    class FakeResponse:
        status_code = 200
        text = "ok"

    class FakeHttp:
        def __init__(self):
            self.posts = []

        async def post(self, path, json):
            self.posts.append((path, json))
            return FakeResponse()

        async def aclose(self):
            pass

    monkeypatch.setenv("POLYBOT_NWWS_LIVE_UNLOCK", "I_UNDERSTAND_THIS_IS_LIVE")
    raw = b"SAUS70 KWBC 151500\nMETAR KLGA 151451Z 18007KT 10SM FEW050 28/14 A2992 RMK AO2"
    target = StationTarget(b"KLGA", "nyc", "new-york-high", "YES", 28, price_ceiling=0.98, max_notional_usdc=1.0)
    books = InMemoryBookStore()
    books.update_ws_message({"asset_id": "YES", "asks": [{"price": "0.50", "size": "2"}, {"price": "0.60", "size": "2"}]})
    cache = PreparedOrderCache()
    cache.upsert_presigned(target, [
        {"order": "signed-a", "__sweep_price": "0.50", "__sweep_size": "1.0"},
        {"order": "signed-b", "__sweep_price": "0.50", "__sweep_size": "1.0"},
        {"order": "signed-c", "__sweep_price": "0.60", "__sweep_size": "2.0"},
    ])
    engine = WeatherLockExecutionEngine([target], cache, armed=True, dry_run=False, books=books, max_book_age_ms=1000)
    fake = FakeHttp()
    engine.http = fake
    try:
        decision = await engine.on_stanza(raw)
        assert decision is not None
        assert decision["action"] == "submitted"
        assert decision["submitted_payloads"] == 2
        assert [payload["order"] for _, payload in fake.posts] == ["signed-a", "signed-b"]
    finally:
        await engine.close()
