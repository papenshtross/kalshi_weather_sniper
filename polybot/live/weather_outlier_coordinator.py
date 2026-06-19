#!/usr/bin/env python3
"""Global weather-outlier book coordinator.

This is a forked execution path for weather outlier snipers.  It does not change
``ArbSniperRunner.run`` or the currently deployed per-city systemd units.

The coordinator replaces per-city REST polling with one global ``POST /books``
poller that batches the complete token universe, then fans snapshots out to the
existing city strategy evaluators in memory.
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import inspect
import math
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Sequence

import httpx
from dotenv import load_dotenv
from loguru import logger

from polybot.live.arb_sniper import ArbSniperRunner, Book, CLOB_REST, _parse_levels

FetchBooks = Callable[[Sequence[str]], Awaitable[dict[str, Book]] | dict[str, Book]]
ApplySnapshots = Callable[["CoordinatedMarket", dict[str, Book]], Awaitable[None] | None]


@dataclass(frozen=True)
class CoordinatedMarket:
    """Token set owned by one weather outlier city/strategy evaluator."""

    strategy_id: str
    market_key: str
    token_ids: Sequence[str]
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class CoordinatorStats:
    target_hz: float
    cycles: int = 0
    book_requests: int = 0
    request_errors: int = 0
    tokens_requested: int = 0
    tokens_received: int = 0
    markets_applied: int = 0
    incomplete_markets_skipped: int = 0
    slow_cycles: int = 0
    max_cycle_ms: float = 0.0
    last_cycle_ms: float = 0.0

    @property
    def effective_hz(self) -> float:
        if self.last_cycle_ms <= 0:
            return 0.0
        return 1000.0 / self.last_cycle_ms


def _dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        token = str(raw or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def build_token_batches(markets: Sequence[CoordinatedMarket], *, max_tokens_per_request: int = 500) -> list[list[str]]:
    """Return de-duplicated token batches capped for CLOB ``POST /books``.

    Polymarket currently accepts 500-token ``/books`` requests and rejects larger
    observed requests.  Keep the cap explicit so tests and runtime safety checks
    can reason about planned request rate before deployment.
    """

    if max_tokens_per_request <= 0:
        raise ValueError("max_tokens_per_request must be > 0")
    all_tokens = _dedupe_preserve_order(token for market in markets for token in market.token_ids)
    return [all_tokens[i : i + max_tokens_per_request] for i in range(0, len(all_tokens), max_tokens_per_request)]


def parse_books_payload(payload: Any) -> dict[str, Book]:
    """Parse a CLOB ``/books`` JSON response into token -> Book snapshots."""

    out: dict[str, Book] = {}
    now = time.time()
    if not isinstance(payload, list):
        return out
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        tok = str(raw.get("asset_id") or raw.get("token_id") or "")
        bids = _parse_levels(raw.get("bids", []))
        asks = _parse_levels(raw.get("asks", []))
        if not tok or (not bids and not asks):
            continue
        out[tok] = Book(
            bid=max((x["price"] for x in bids), default=0.0),
            ask=min((x["price"] for x in asks), default=0.0),
            bids=bids,
            asks=asks,
            updated_ts=now,
        )
    return out


async def fetch_books_resilient(
    client: Any,
    token_ids: Sequence[str],
    *,
    split_on_failure: bool = True,
) -> dict[str, Book]:
    """Fetch CLOB books and bisect failed batches to isolate bad/expired tokens.

    A daily weather universe can contain recently expired or otherwise rejected
    tokens.  One bad token must not make a whole 500-token coordinator batch stale.
    On any non-200 response or transport exception, recursively split the batch;
    single-token failures are skipped and logged.
    """

    tokens = _dedupe_preserve_order(token_ids)
    if not tokens:
        return {}
    status_code: int | None = None
    try:
        response = await client.post(f"{CLOB_REST}/books", json=[{"token_id": str(t)} for t in tokens], timeout=2.5)
        status_code = int(response.status_code)
        if status_code == 200:
            return parse_books_payload(response.json())
        reason = f"HTTP {status_code}: {response.text[:200]}"
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
    # Never split on rate-limit responses. Splitting a 429 multiplies requests
    # exactly when the upstream has told us to slow down, creating a request storm.
    if status_code == 429:
        logger.warning("weather coordinator /books rate limited batch size={} reason={}", len(tokens), reason)
        return {}
    if not split_on_failure or len(tokens) <= 1:
        logger.warning("weather coordinator skipped /books batch size={} reason={}", len(tokens), reason)
        return {}
    mid = max(1, len(tokens) // 2)
    left, right = await asyncio.gather(
        fetch_books_resilient(client, tokens[:mid], split_on_failure=split_on_failure),
        fetch_books_resilient(client, tokens[mid:], split_on_failure=split_on_failure),
    )
    merged = dict(left)
    merged.update(right)
    return merged


class WeatherBookCoordinator:
    """Poll all weather books in global batches and fan out to city evaluators."""

    def __init__(
        self,
        *,
        markets: Sequence[CoordinatedMarket],
        fetch_books: FetchBooks,
        apply_snapshots: ApplySnapshots,
        target_hz: float = 1.2,
        max_tokens_per_request: int = 500,
        require_complete_market_snapshot: bool = True,
        max_planned_request_rate: float = 4.5,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.perf_counter,
    ) -> None:
        if target_hz <= 0:
            raise ValueError("target_hz must be > 0")
        self.markets = list(markets)
        self.fetch_books = fetch_books
        self.apply_snapshots = apply_snapshots
        self.target_hz = float(target_hz)
        self.max_tokens_per_request = int(max_tokens_per_request)
        self.require_complete_market_snapshot = bool(require_complete_market_snapshot)
        self.max_planned_request_rate = float(max_planned_request_rate)
        self.sleep = sleep
        self.monotonic = monotonic
        self._apply_lock = asyncio.Lock()
        self.batches = build_token_batches(self.markets, max_tokens_per_request=self.max_tokens_per_request)
        self.stats = CoordinatorStats(target_hz=self.target_hz)
        if self.planned_request_rate > self.max_planned_request_rate + 1e-9:
            raise ValueError(
                f"planned /books rate {self.planned_request_rate:.2f} req/s exceeds safety cap "
                f"{self.max_planned_request_rate:.2f}; lower target_hz or increase safe batching"
            )

    @property
    def requests_per_cycle(self) -> int:
        return len(self.batches)

    @property
    def token_count(self) -> int:
        return sum(len(batch) for batch in self.batches)

    @property
    def planned_request_rate(self) -> float:
        return self.requests_per_cycle * self.target_hz

    async def _maybe_await(self, value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value

    async def _fetch_batch(self, batch: Sequence[str]) -> dict[str, Book]:
        self.stats.book_requests += 1
        self.stats.tokens_requested += len(batch)
        try:
            got = await self._maybe_await(self.fetch_books(batch))
        except Exception as exc:
            self.stats.request_errors += 1
            logger.warning("weather coordinator /books batch failed size={}: {}", len(batch), exc)
            return {}
        if got:
            self.stats.tokens_received += len(got)
            return dict(got)
        return {}

    async def run_cycle(self) -> CoordinatorStats:
        start = self.monotonic()
        batch_results = await asyncio.gather(*(self._fetch_batch(batch) for batch in self.batches))
        snapshots: dict[str, Book] = {}
        for got in batch_results:
            if got:
                snapshots.update(got)

        async with self._apply_lock:
            for market in self.markets:
                market_tokens = [str(t) for t in market.token_ids]
                market_snapshots = {token: snapshots[token] for token in market_tokens if token in snapshots}
                if self.require_complete_market_snapshot and len(market_snapshots) < len(set(market_tokens)):
                    self.stats.incomplete_markets_skipped += 1
                    continue
                if not market_snapshots:
                    self.stats.incomplete_markets_skipped += 1
                    continue
                await self._maybe_await(self.apply_snapshots(market, market_snapshots))
                self.stats.markets_applied += 1

        elapsed_ms = (self.monotonic() - start) * 1000.0
        self.stats.cycles += 1
        self.stats.last_cycle_ms = elapsed_ms
        self.stats.max_cycle_ms = max(self.stats.max_cycle_ms, elapsed_ms)
        if elapsed_ms > (1000.0 / self.target_hz):
            self.stats.slow_cycles += 1
        return self.stats

    async def run_cycles(self, *, cycles: int) -> CoordinatorStats:
        if cycles < 0:
            raise ValueError("cycles must be >= 0")
        interval = 1.0 / self.target_hz
        for _ in range(cycles):
            started = self.monotonic()
            await self.run_cycle()
            sleep_for = interval - (self.monotonic() - started)
            if sleep_for > 0:
                await self.sleep(sleep_for)
        return self.stats

    async def run_forever(self, stop: asyncio.Event, *, log_interval_seconds: float = 10.0, max_in_flight_cycles: int = 8) -> None:
        interval = 1.0 / self.target_hz
        last_log = 0.0
        in_flight: set[asyncio.Task[CoordinatorStats]] = set()

        def _discard_done(task: asyncio.Task[CoordinatorStats]) -> None:
            in_flight.discard(task)
            try:
                task.result()
            except Exception as exc:
                self.stats.request_errors += 1
                logger.warning("weather coordinator cycle task failed: {}", exc)

        while not stop.is_set():
            started = self.monotonic()
            if len(in_flight) < max(1, int(max_in_flight_cycles)):
                task = asyncio.create_task(self.run_cycle())
                in_flight.add(task)
                task.add_done_callback(_discard_done)
            else:
                self.stats.slow_cycles += 1
                logger.warning("weather coordinator skipped cycle: in_flight={} cap={}", len(in_flight), max_in_flight_cycles)
            now = time.time()
            if log_interval_seconds > 0 and now - last_log >= log_interval_seconds:
                last_log = now
                logger.info(
                    "weather coordinator health target_hz={:.2f} planned_req_s={:.2f} cycles={} in_flight={} req={} errors={} "
                    "tokens={}/{} markets_applied={} skipped={} last_cycle_ms={:.1f} max_cycle_ms={:.1f} slow_cycles={}",
                    self.target_hz,
                    self.planned_request_rate,
                    self.stats.cycles,
                    len(in_flight),
                    self.stats.book_requests,
                    self.stats.request_errors,
                    self.stats.tokens_received,
                    self.stats.tokens_requested,
                    self.stats.markets_applied,
                    self.stats.incomplete_markets_skipped,
                    self.stats.last_cycle_ms,
                    self.stats.max_cycle_ms,
                    self.stats.slow_cycles,
                )
            sleep_for = interval - (self.monotonic() - started)
            if sleep_for > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=sleep_for)
                except asyncio.TimeoutError:
                    pass
        if in_flight:
            await asyncio.gather(*in_flight, return_exceptions=True)


class RunnerFanout:
    """Adapter from global book snapshots to existing ArbSniperRunner instances."""

    def __init__(self, runners: Sequence[ArbSniperRunner], *, shadow: bool = False, state_refresh_seconds: float = 1.0) -> None:
        self.runners_by_strategy_id = {runner.strategy_id: runner for runner in runners}
        self.shadow = bool(shadow)
        self.state_refresh_seconds = max(0.05, float(state_refresh_seconds))
        self._state_cache: dict[str, tuple[float, dict[str, Any], str]] = {}

    def markets(self) -> list[CoordinatedMarket]:
        out: list[CoordinatedMarket] = []
        for runner in self.runners_by_strategy_id.values():
            tokens: list[str] = []
            for pair in runner.market_pairs or ([] if runner.ev is None else [runner.ev]):
                tokens.extend([str(pair["yes_token"]), str(pair["no_token"])])
            out.append(CoordinatedMarket(runner.strategy_id, str((runner.ev or {}).get("event_slug") or runner.strategy_id), tokens))
        return out

    async def _cached_state(self, runner: ArbSniperRunner) -> tuple[dict[str, Any], str]:
        now = time.time()
        cached = self._state_cache.get(runner.strategy_id)
        if cached and now - cached[0] < self.state_refresh_seconds:
            return cached[1], cached[2]
        cfg, status = await runner.current_state()
        runner.cached_status = status
        self._state_cache[runner.strategy_id] = (now, cfg, status)
        return cfg, status

    async def apply(self, market: CoordinatedMarket, snapshots: dict[str, Book]) -> None:
        runner = self.runners_by_strategy_id[market.strategy_id]
        applied = 0
        for token, book in snapshots.items():
            if runner._apply_full_book(token, book):
                applied += 1
        if applied:
            runner.rest_book_refresh_count += 1
            cfg, status = await self._cached_state(runner)
            runner._schedule_dashboard_book_write(cfg)
            if status == "running" and not self.shadow:
                await runner.maybe_execute_hot_path(cfg)


async def load_weather_runners(config_paths: Sequence[Path]) -> list[ArbSniperRunner]:
    runners: list[ArbSniperRunner] = []
    for path in config_paths:
        runner = ArbSniperRunner(path)
        try:
            await runner.setup()
            cfg, status = await runner.current_state()
            runner.cached_status = status
            await runner.load_market(cfg)
        except Exception as exc:
            logger.warning("skipping weather runner config={} during coordinator startup: {}", path, exc)
            try:
                await runner.writer.close()
            except Exception:
                pass
            continue
        runners.append(runner)
        logger.info("loaded weather runner strategy={} status={} markets={}", runner.strategy_id, status, len(runner.market_pairs))
    return runners


async def close_weather_runners(runners: Sequence[ArbSniperRunner]) -> None:
    for runner in runners:
        try:
            if runner.dashboard_book_write_task is not None and not runner.dashboard_book_write_task.done():
                runner.dashboard_book_write_task.cancel()
            await runner.writer.close()
        except Exception as exc:
            logger.debug("runner close failed for {}: {}", runner.strategy_id, exc)


async def run_from_configs(
    config_paths: Sequence[Path],
    *,
    target_hz: float = 1.2,
    max_tokens_per_request: int = 500,
    max_planned_request_rate: float = 4.5,
    shadow: bool = False,
) -> None:
    load_dotenv()
    load_dotenv(".env.live", override=True)
    runners = await load_weather_runners(config_paths)
    fanout = RunnerFanout(runners, shadow=shadow)
    markets = fanout.markets()
    if not markets:
        raise RuntimeError("no weather markets loaded")
    timeout = httpx.Timeout(2.0, connect=0.5, read=1.5, write=0.5, pool=0.5)
    limits = httpx.Limits(max_keepalive_connections=8, max_connections=16, keepalive_expiry=30.0)
    stop = asyncio.Event()

    def _stop(*_: Any) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    try:
        async with httpx.AsyncClient(timeout=timeout, limits=limits, http2=True) as client:
            async def fetch(batch: Sequence[str]) -> dict[str, Book]:
                return await fetch_books_resilient(client, list(batch))

            coordinator = WeatherBookCoordinator(
                markets=markets,
                fetch_books=fetch,
                apply_snapshots=fanout.apply,
                target_hz=target_hz,
                max_tokens_per_request=max_tokens_per_request,
                max_planned_request_rate=max_planned_request_rate,
                require_complete_market_snapshot=True,
            )
            logger.info(
                "weather coordinator starting strategies={} tokens={} batches={} target_hz={:.2f} planned_req_s={:.2f} shadow={}",
                len(markets),
                coordinator.token_count,
                coordinator.requests_per_cycle,
                coordinator.target_hz,
                coordinator.planned_request_rate,
                shadow,
            )
            await coordinator.run_forever(stop)
    finally:
        await close_weather_runners(runners)


def _default_config_paths() -> list[Path]:
    return [Path(p) for p in sorted(glob.glob("config/weather-outlier-sniper-*-live.yaml"))]


def main() -> None:
    parser = argparse.ArgumentParser(description="Global batched weather outlier coordinator")
    parser.add_argument("--config", action="append", type=Path, help="Weather outlier config path; repeatable. Defaults to config/weather-outlier-sniper-*-live.yaml")
    parser.add_argument("--target-hz", type=float, default=1.2)
    parser.add_argument("--max-tokens-per-request", type=int, default=500)
    parser.add_argument("--max-planned-request-rate", type=float, default=4.5)
    parser.add_argument("--shadow", action="store_true", help="Apply snapshots and dashboard writes but never submit orders")
    args = parser.parse_args()
    paths = args.config or _default_config_paths()
    if not paths:
        raise SystemExit("no config files found")
    asyncio.run(
        run_from_configs(
            paths,
            target_hz=args.target_hz,
            max_tokens_per_request=args.max_tokens_per_request,
            max_planned_request_rate=args.max_planned_request_rate,
            shadow=args.shadow,
        )
    )


if __name__ == "__main__":
    main()
