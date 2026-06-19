import asyncio

import pytest

from polybot.live.arb_sniper import Book
from polybot.live.weather_outlier_coordinator import (
    CoordinatedMarket,
    WeatherBookCoordinator,
    build_token_batches,
    fetch_books_resilient,
    parse_books_payload,
)


def test_build_token_batches_deduplicates_and_caps_batch_size():
    markets = [
        CoordinatedMarket("s1", "m1", ["a", "b", "c"]),
        CoordinatedMarket("s2", "m2", ["c", "d", "e"]),
        CoordinatedMarket("s3", "m3", ["f"]),
    ]

    batches = build_token_batches(markets, max_tokens_per_request=3)

    assert batches == [["a", "b", "c"], ["d", "e", "f"]]


def test_build_token_batches_rejects_invalid_limits():
    with pytest.raises(ValueError, match="max_tokens_per_request"):
        build_token_batches([], max_tokens_per_request=0)


def test_parse_books_payload_keeps_ask_only_weather_books():
    parsed = parse_books_payload([
        {"asset_id": "tok1", "bids": [], "asks": [{"price": "0.999", "size": "12.5"}]},
        {"asset_id": "tok2", "bids": [], "asks": []},
    ])

    assert set(parsed) == {"tok1"}
    assert parsed["tok1"].ask == 0.999
    assert parsed["tok1"].bid == 0.0


class _FakeBooksResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or []
        self.text = text

    def json(self):
        return self._payload


class _SplittingFakeClient:
    def __init__(self, *, status_with_bad=400):
        self.calls = []
        self.status_with_bad = status_with_bad

    async def post(self, _url, json, timeout=None):
        tokens = [row["token_id"] for row in json]
        self.calls.append(tokens)
        if "bad" in tokens and len(tokens) > 1:
            return _FakeBooksResponse(self.status_with_bad, text="bad token")
        if tokens == ["bad"]:
            return _FakeBooksResponse(self.status_with_bad, text="bad token")
        return _FakeBooksResponse(200, [{"asset_id": token, "bids": [{"price": "0.1", "size": "1"}], "asks": [{"price": "0.2", "size": "1"}]} for token in tokens])


@pytest.mark.asyncio
async def test_resilient_fetch_bisects_failed_batch_and_keeps_valid_tokens():
    client = _SplittingFakeClient()

    snapshots = await fetch_books_resilient(client, ["good1", "bad", "good2"])

    assert set(snapshots) == {"good1", "good2"}
    assert ["good1", "bad", "good2"] in client.calls
    assert ["bad"] in client.calls


@pytest.mark.asyncio
async def test_resilient_fetch_does_not_split_on_rate_limit():
    client = _SplittingFakeClient(status_with_bad=429)

    snapshots = await fetch_books_resilient(client, ["good1", "bad", "good2"])

    assert snapshots == {}
    assert client.calls == [["good1", "bad", "good2"]]


@pytest.mark.asyncio
async def test_coordinator_refreshes_all_markets_at_same_10hz_cadence():
    calls = []
    applied = []

    async def fetch(batch):
        calls.append(list(batch))
        return {token: Book(bid=0.10, ask=0.20, updated_ts=1000.0) for token in batch}

    async def apply(market, snapshots):
        applied.append((market.strategy_id, sorted(snapshots)))

    coordinator = WeatherBookCoordinator(
        markets=[
            CoordinatedMarket("s1", "city-one", ["a", "b"]),
            CoordinatedMarket("s2", "city-two", ["c", "d"]),
        ],
        fetch_books=fetch,
        apply_snapshots=apply,
        target_hz=10.0,
        max_tokens_per_request=3,
        max_planned_request_rate=45.0,
    )

    stats = await coordinator.run_cycles(cycles=2)

    assert calls == [["a", "b", "c"], ["d"], ["a", "b", "c"], ["d"]]
    assert applied == [
        ("s1", ["a", "b"]),
        ("s2", ["c", "d"]),
        ("s1", ["a", "b"]),
        ("s2", ["c", "d"]),
    ]
    assert stats.cycles == 2
    assert stats.book_requests == 4
    assert stats.tokens_requested == 8
    assert stats.target_hz == 10.0


@pytest.mark.asyncio
async def test_coordinator_applies_only_complete_market_snapshots():
    applied = []

    async def fetch(batch):
        return {
            "a": Book(bid=0.10, ask=0.20, updated_ts=1000.0),
            "b": Book(bid=0.11, ask=0.21, updated_ts=1000.0),
            "c": Book(bid=0.12, ask=0.22, updated_ts=1000.0),
        }

    async def apply(market, snapshots):
        applied.append((market.strategy_id, sorted(snapshots)))

    coordinator = WeatherBookCoordinator(
        markets=[
            CoordinatedMarket("complete", "city-one", ["a", "b"]),
            CoordinatedMarket("partial", "city-two", ["c", "d"]),
        ],
        fetch_books=fetch,
        apply_snapshots=apply,
        target_hz=10.0,
        max_tokens_per_request=10,
        max_planned_request_rate=45.0,
        require_complete_market_snapshot=True,
    )

    stats = await coordinator.run_cycles(cycles=1)

    assert applied == [("complete", ["a", "b"])]
    assert stats.markets_applied == 1
    assert stats.incomplete_markets_skipped == 1


def test_coordinator_estimates_safe_request_rate_for_full_universe():
    markets = [CoordinatedMarket(f"s{i}", f"city-{i}", [f"{i}-{j}" for j in range(22)]) for i in range(50)]
    coordinator = WeatherBookCoordinator(
        markets=markets,
        fetch_books=lambda batch: {},
        apply_snapshots=lambda market, snapshots: None,
        target_hz=10.0,
        max_tokens_per_request=500,
        max_planned_request_rate=45.0,
    )

    assert coordinator.token_count == 1100
    assert coordinator.requests_per_cycle == 3
    assert coordinator.planned_request_rate == pytest.approx(30.0)
    assert coordinator.planned_request_rate < 50.0


def test_coordinator_rejects_10hz_full_universe_under_live_books_limit():
    markets = [CoordinatedMarket(f"s{i}", f"city-{i}", [f"{i}-{j}" for j in range(22)]) for i in range(50)]

    with pytest.raises(ValueError, match="planned /books rate"):
        WeatherBookCoordinator(
            markets=markets,
            fetch_books=lambda batch: {},
            apply_snapshots=lambda market, snapshots: None,
            target_hz=10.0,
            max_tokens_per_request=500,
            max_planned_request_rate=4.5,
        )


def test_coordinator_accepts_safe_full_universe_rate_under_live_books_limit():
    markets = [CoordinatedMarket(f"s{i}", f"city-{i}", [f"{i}-{j}" for j in range(22)]) for i in range(50)]

    coordinator = WeatherBookCoordinator(
        markets=markets,
        fetch_books=lambda batch: {},
        apply_snapshots=lambda market, snapshots: None,
        target_hz=1.5,
        max_tokens_per_request=500,
        max_planned_request_rate=4.5,
    )

    assert coordinator.requests_per_cycle == 3
    assert coordinator.planned_request_rate == pytest.approx(4.5)
