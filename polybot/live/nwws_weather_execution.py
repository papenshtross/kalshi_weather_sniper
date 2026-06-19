"""NWWS-OI weather-lock execution scaffold.

This module intentionally defaults to DRY/DISABLED.  It provides the hot-path
building blocks for an event-driven weather execution engine without enabling
live orders by itself:

* persistent NWWS-OI XMPP listener (slixmpp-backed, optional dependency)
* allocation-light METAR/SPECI parser for target ICAO stations
* lock-free-ish asyncio Queue handoff from ingest to execution loop
* in-memory threshold/token/order-plan objects

Production note: Python cannot guarantee sub-millisecond end-to-end execution,
and Polymarket REST/Cloudflare latency is tens of milliseconds even on warm
connections.  The parser below avoids regex and performs bounded byte scans so
it can be ported directly to Rust/PyO3 if this strategy is moved to a colocated
standalone VPS.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import ssl
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Mapping

import httpx
import websockets
from loguru import logger

try:  # Optional compiled parser: `python setup.py build_ext --inplace`.
    from polybot.live import _nwws_fast  # type: ignore
except Exception:  # pragma: no cover
    _nwws_fast = None

SAUS = b"SAUS"
SPUS = b"SPUS"
METAR = b"METAR"
SPECI = b"SPECI"


@dataclass(slots=True, frozen=True)
class StationTarget:
    """Active target mapped from a Polymarket weather market."""

    icao: bytes
    city_slug: str
    market_slug: str
    yes_token: str
    threshold_c: int
    direction: str = "gte"  # gte/lte/eq; daily high buckets should use gte.
    price_ceiling: float = 0.98
    max_notional_usdc: float = 0.0


@dataclass(slots=True, frozen=True)
class MetarHit:
    icao: str
    temp_c: int
    raw_start: int
    raw_end: int
    bulletin_kind: str
    received_ns: int


@dataclass(slots=True)
class PreparedOrderPlan:
    """Pre-computed execution payload placeholder.

    Fill `signed_payloads` from the existing py-clob-client-v2 signing path during
    a warmup phase.  The event loop never signs or fetches metadata on the hot path.
    """

    station: StationTarget
    signed_payloads: list[dict[str, Any]] = field(default_factory=list)
    last_prepared_ns: int = 0

    @property
    def ready(self) -> bool:
        return bool(self.signed_payloads)

    def select_payloads_for_sweep(self, sweep: list[tuple[float, float]]) -> list[dict[str, Any]]:
        """Select pre-signed payloads that cover the planned in-memory sweep.

        Signing remains out of band.  Warmup code should attach lightweight
        metadata to each signed payload:

        * `__sweep_price`: limit/match price represented by the pre-signed order
        * `__sweep_size`: share size represented by the pre-signed order

        If metadata is missing we conservatively return all payloads for backward
        compatibility with externally-built signed batches.  The execution hot
        path never mutates order size or signs a replacement order.
        """
        required_notional = sum(px * sz for px, sz in sweep)
        if required_notional <= 0:
            return []
        selected: list[dict[str, Any]] = []
        covered = 0.0
        for payload in self.signed_payloads:
            try:
                px = float(Decimal(str(payload["__sweep_price"])))
                sz = float(Decimal(str(payload["__sweep_size"])))
            except Exception:
                return list(self.signed_payloads)
            if px <= 0 or sz <= 0:
                continue
            selected.append(payload)
            covered += px * sz
            if covered + 1e-9 >= required_notional:
                break
        return selected


class PreparedOrderCache:
    """Memory-only holder for pre-signed CLOB payloads.

    This object deliberately does not know how to sign. Signing/warmup must be
    performed before the event loop starts; the METAR hot path can only read an
    already-populated payload list from memory.
    """

    __slots__ = ("plans",)

    def __init__(self, plans: Iterable[PreparedOrderPlan] = ()) -> None:
        self.plans: dict[str, PreparedOrderPlan] = {
            p.station.icao.decode("ascii"): p for p in plans
        }

    def get(self, icao: str) -> PreparedOrderPlan | None:
        return self.plans.get(icao)

    def upsert_presigned(self, station: StationTarget, payloads: list[dict[str, Any]]) -> PreparedOrderPlan:
        if not payloads:
            raise ValueError("payloads must be non-empty")
        plan = PreparedOrderPlan(
            station=station,
            signed_payloads=list(payloads),
            last_prepared_ns=time.perf_counter_ns(),
        )
        self.plans[station.icao.decode("ascii")] = plan
        return plan


@dataclass(slots=True)
class ExecutionCircuitBreaker:
    """Non-hot-path safety gate for any future live execution.

    The staged dashboard deployment intentionally leaves live orders impossible.
    Even if code is started with armed=True/dry_run=False, this breaker requires
    an explicit environment unlock and nonzero bounded notional before `/orders`.
    """

    env_var: str = "POLYBOT_NWWS_LIVE_UNLOCK"
    required_value: str = "I_UNDERSTAND_THIS_IS_LIVE"
    max_event_notional_usdc: float = 0.0
    tripped_reason: str = ""

    def can_submit(self, *, armed: bool, dry_run: bool, planned_notional: float) -> bool:
        if not armed:
            self.tripped_reason = "not_armed"
            return False
        if dry_run:
            self.tripped_reason = "dry_run"
            return False
        if os.getenv(self.env_var) != self.required_value:
            self.tripped_reason = f"missing_env_unlock:{self.env_var}"
            return False
        if planned_notional <= 0:
            self.tripped_reason = "zero_planned_notional"
            return False
        if self.max_event_notional_usdc <= 0 or planned_notional > self.max_event_notional_usdc:
            self.tripped_reason = "event_notional_cap"
            return False
        self.tripped_reason = ""
        return True


class AsyncDecisionLogger:
    """Best-effort async logger; never awaited on the hot decision path."""

    __slots__ = ("queue", "task")

    def __init__(self, maxsize: int = 8192) -> None:
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=maxsize)
        self.task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self.task is None:
            self.task = asyncio.create_task(self._run())

    def emit_nowait(self, item: dict[str, Any]) -> None:
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            pass

    async def _run(self) -> None:
        while True:
            item = await self.queue.get()
            logger.info("NWWS weather-lock decision {}", item)


@dataclass(slots=True)
class L2Book:
    token_id: str
    bids: list[tuple[float, float]] = field(default_factory=list)
    asks: list[tuple[float, float]] = field(default_factory=list)
    updated_ns: int = 0

    def update(self, msg: Mapping[str, Any], now_ns: int | None = None) -> None:
        bids = msg.get("bids") or []
        asks = msg.get("asks") or []
        if bids:
            self.bids = _levels_desc(bids)
        if asks:
            self.asks = _levels_asc(asks)
        self.updated_ns = now_ns or time.perf_counter_ns()

    def ask_sweep(self, price_ceiling: float, max_notional: float) -> list[tuple[float, float]]:
        remaining = max(0.0, float(max_notional))
        out: list[tuple[float, float]] = []
        if remaining <= 0:
            return out
        for px, sz in self.asks:
            if px > price_ceiling or sz <= 0:
                break
            take = min(sz, remaining / px)
            if take <= 0:
                break
            out.append((px, take))
            remaining -= px * take
            if remaining <= 1e-9:
                break
        return out


class InMemoryBookStore:
    __slots__ = ("books",)

    def __init__(self) -> None:
        self.books: dict[str, L2Book] = {}

    def update_ws_message(self, item: Mapping[str, Any], now_ns: int | None = None) -> L2Book | None:
        token = str(item.get("asset_id") or item.get("token_id") or "")
        if not token:
            return None
        book = self.books.get(token)
        if book is None:
            book = self.books[token] = L2Book(token)
        book.update(item, now_ns=now_ns)
        return book

    def get(self, token_id: str) -> L2Book | None:
        return self.books.get(token_id)


class TargetIndex:
    """Byte-keyed target index used by the parser."""

    __slots__ = ("_by_icao", "_needles", "station_tuple")

    def __init__(self, targets: Iterable[StationTarget]):
        self._by_icao = {t.icao.upper(): t for t in targets}
        # Include leading space to avoid matching ICAO inside a longer token.
        self._needles = tuple(b" " + k + b" " for k in self._by_icao)
        self.station_tuple = tuple(self._by_icao.keys())

    def find_station(self, buf: bytes, start: int = 0, end: int | None = None) -> tuple[StationTarget, int] | None:
        stop = len(buf) if end is None else end
        best: tuple[StationTarget, int] | None = None
        for needle in self._needles:
            pos = buf.find(needle, start, stop)
            if pos >= 0 and (best is None or pos < best[1]):
                target = self._by_icao[needle[1:-1]]
                best = (target, pos + 1)
        return best


def _level_pair(level: Any) -> tuple[float, float] | None:
    if isinstance(level, Mapping):
        px = level.get("price") or level.get("px")
        sz = level.get("size") or level.get("shares") or level.get("quantity")
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        px, sz = level[0], level[1]
    else:
        return None
    try:
        return float(Decimal(str(px))), float(Decimal(str(sz)))
    except Exception:
        return None


def _levels_asc(levels: Iterable[Any]) -> list[tuple[float, float]]:
    parsed = [x for x in (_level_pair(l) for l in levels) if x and x[1] > 0]
    return sorted(parsed, key=lambda x: x[0])


def _levels_desc(levels: Iterable[Any]) -> list[tuple[float, float]]:
    parsed = [x for x in (_level_pair(l) for l in levels) if x and x[1] > 0]
    return sorted(parsed, key=lambda x: x[0], reverse=True)


def _is_digit(c: int) -> bool:
    return 48 <= c <= 57


def _parse_signed_two_digits(buf: bytes, i: int, end: int) -> tuple[int, int] | None:
    """Parse METAR signed two-digit temperature at buf[i:].

    Handles 28, M02, 02.  Returns (value_c, next_index).
    """
    neg = False
    if i < end and buf[i] == 77:  # M
        neg = True
        i += 1
    if i + 1 >= end or not _is_digit(buf[i]) or not _is_digit(buf[i + 1]):
        return None
    value = (buf[i] - 48) * 10 + (buf[i + 1] - 48)
    return ((-value if neg else value), i + 2)


def extract_metar_temperature_c(raw: bytes, icao: bytes, station_pos: int) -> int | None:
    """Extract the METAR temperature group for a specific station.

    The target group is the first token after the station/time block that looks
    like `TT/DD`, `M02/M05`, `28/14`, `28/`, or `////` is skipped.  We scan only
    the first ~220 bytes after the ICAO because the temp/dewpoint group appears
    early in routine METAR/SPECI lines.
    """
    n = len(raw)
    end = min(n, station_pos + 220)
    i = station_pos + len(icao)
    # token scan: tokens are whitespace-delimited inside the raw bulletin text.
    while i < end:
        while i < end and raw[i] <= 32:
            i += 1
        tok_start = i
        while i < end and raw[i] > 32:
            i += 1
        tok_end = i
        if tok_end <= tok_start:
            continue
        slash = raw.find(b"/", tok_start, tok_end)
        if slash <= tok_start:
            continue
        parsed = _parse_signed_two_digits(raw, tok_start, slash)
        if parsed is None:
            continue
        temp_c, next_i = parsed
        # require slash immediately after the temp digits/Mdd.  This rejects wind
        # and altimeter tokens while accepting missing dewpoint forms like 28/.
        if next_i == slash:
            return temp_c
    return None


def parse_nwws_metar(buf: bytes, targets: TargetIndex, received_ns: int | None = None) -> MetarHit | None:
    """Return a target METAR/SPECI hit or None.

    This is intentionally regex-free.  It first checks for SAUS/SPUS or METAR/SPECI
    markers, then finds only configured ICAO codes, then extracts temp.
    """
    if _nwws_fast is not None:
        parsed = _nwws_fast.parse_any(buf, targets.station_tuple)
        if parsed is None:
            return None
        icao_b, temp_c, raw_start, raw_end, kind = parsed
        return MetarHit(
            icao=icao_b.decode("ascii"),
            temp_c=int(temp_c),
            raw_start=int(raw_start),
            raw_end=int(raw_end),
            bulletin_kind=str(kind),
            received_ns=received_ns or time.perf_counter_ns(),
        )
    if not (SAUS in buf or SPUS in buf or METAR in buf or SPECI in buf):
        return None
    found = targets.find_station(buf)
    if not found:
        return None
    target, station_pos = found
    temp_c = extract_metar_temperature_c(buf, target.icao, station_pos)
    if temp_c is None:
        return None
    kind = "SPUS/SPECI" if (SPUS in buf or SPECI in buf[: station_pos + 16]) else "SAUS/METAR"
    return MetarHit(
        icao=target.icao.decode("ascii"),
        temp_c=temp_c,
        raw_start=max(0, station_pos - 64),
        raw_end=min(len(buf), station_pos + 220),
        bulletin_kind=kind,
        received_ns=received_ns or time.perf_counter_ns(),
    )


def threshold_wins(temp_c: int, target: StationTarget) -> bool:
    if target.direction == "gte":
        return temp_c >= target.threshold_c
    if target.direction == "lte":
        return temp_c <= target.threshold_c
    if target.direction == "eq":
        return temp_c == target.threshold_c
    raise ValueError(f"unknown direction: {target.direction}")


async def clob_l2_websocket_loop(
    asset_ids: list[str],
    books: InMemoryBookStore,
    *,
    url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    stop: asyncio.Event | None = None,
) -> None:
    """Maintain in-memory L2 books from Polymarket market websocket."""
    stop = stop or asyncio.Event()
    while not stop.is_set():
        try:
            async with websockets.connect(
                url,
                ping_interval=10,
                ping_timeout=10,
                close_timeout=1,
                max_queue=4096,
                compression=None,
            ) as ws:
                await ws.send(json.dumps({"type": "market", "assets_ids": asset_ids, "custom_feature_enabled": True}))
                while not stop.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    parsed = json.loads(raw)
                    now_ns = time.perf_counter_ns()
                    for item in parsed if isinstance(parsed, list) else [parsed]:
                        if isinstance(item, Mapping) and item.get("event_type") in (None, "book", "price_change"):
                            books.update_ws_message(item, now_ns=now_ns)
        except asyncio.TimeoutError:
            continue
        except Exception:
            await asyncio.sleep(1)


class NwwsXmppClient:
    """NWWS-OI XMPP PubSub listener.

    Requires `slixmpp` at runtime. Credentials are intentionally read from env,
    not stored in strategy config:

    * NWWS_JID
    * NWWS_PASSWORD
    * NWWS_HOST default nwws-oi.weather.gov
    * NWWS_PORT default 5222
    * NWWS_PUBSUB_JID default nwws-oi.weather.gov
    * NWWS_NODE default /products
    """

    def __init__(self, queue: asyncio.Queue[bytes], *, jid: str, password: str, host: str = "nwws-oi.weather.gov", port: int = 5222, pubsub_jid: str = "nwws-oi.weather.gov", node: str = "/products") -> None:
        self.queue = queue
        self.jid = jid
        self.password = password
        self.host = host
        self.port = port
        self.pubsub_jid = pubsub_jid
        self.node = node

    async def run(self) -> None:
        try:
            import slixmpp  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional runtime dep
            raise RuntimeError("NWWS XMPP listener requires optional package: pip install slixmpp") from exc

        queue = self.queue
        pubsub_jid = self.pubsub_jid
        node = self.node

        class Client(slixmpp.ClientXMPP):  # type: ignore[misc]
            def __init__(self, jid: str, password: str):
                super().__init__(jid, password)
                self.register_plugin("xep_0030")
                self.register_plugin("xep_0060")
                self.add_event_handler("session_start", self.session_start)
                self.add_event_handler("pubsub_publish", self.pubsub_publish)

            async def session_start(self, _event: Any) -> None:
                self.send_presence()
                await self.get_roster()
                # Subscribe to live pushes only; no historical query/replay.
                await self.plugin["xep_0060"].subscribe(pubsub_jid, node)
                logger.info("NWWS-OI subscribed pubsub_jid={} node={}", pubsub_jid, node)

            def pubsub_publish(self, msg: Any) -> None:
                # Convert stanza once at the boundary. Hot parser consumes bytes.
                try:
                    payload = bytes(str(msg), "utf-8", "ignore")
                    queue.put_nowait(payload)
                except asyncio.QueueFull:
                    logger.warning("NWWS queue full; dropping stanza")

        xmpp = Client(self.jid, self.password)
        xmpp.ssl_context = ssl.create_default_context()
        xmpp.connect((self.host, self.port), use_ssl=False, force_starttls=True)
        await xmpp.disconnected


class WeatherLockExecutionEngine:
    """Critical-path coordinator.

    In live mode this receives parsed hits and posts already signed CLOB order
    payloads.  Default `armed=False` guarantees dry deployment to dashboard.
    """

    def __init__(self, targets: list[StationTarget], plans: Mapping[str, PreparedOrderPlan] | PreparedOrderCache, *, armed: bool = False, clob_url: str = "https://clob.polymarket.com", dry_run: bool = True, books: InMemoryBookStore | None = None, max_book_age_ms: int = 50, circuit_breaker: ExecutionCircuitBreaker | None = None, decision_logger: AsyncDecisionLogger | None = None) -> None:
        self.targets = TargetIndex(targets)
        self.target_by_icao = {t.icao.decode("ascii"): t for t in targets}
        self.plans = plans if isinstance(plans, PreparedOrderCache) else PreparedOrderCache(plans.values())
        self.armed = armed
        self.dry_run = dry_run
        self.books = books or InMemoryBookStore()
        self.max_book_age_ns = int(max_book_age_ms * 1_000_000)
        max_event_notional = max((t.max_notional_usdc for t in targets), default=0.0)
        self.circuit_breaker = circuit_breaker or ExecutionCircuitBreaker(max_event_notional_usdc=max_event_notional)
        self.decision_logger = decision_logger
        self.http = httpx.AsyncClient(base_url=clob_url, http2=True, timeout=1.0)

    async def on_stanza(self, stanza: bytes) -> dict[str, Any] | None:
        t0 = time.perf_counter_ns()
        hit = parse_nwws_metar(stanza, self.targets, received_ns=t0)
        if hit is None:
            return None
        target = self.target_by_icao[hit.icao]
        win = threshold_wins(hit.temp_c, target)
        decision = {
            "icao": hit.icao,
            "temp_c": hit.temp_c,
            "market_slug": target.market_slug,
            "threshold_c": target.threshold_c,
            "wins": win,
            "armed": self.armed,
            "dry_run": self.dry_run,
            "latency_parse_us": (time.perf_counter_ns() - t0) / 1000.0,
        }
        if not win:
            return decision
        book = self.books.get(target.yes_token)
        if book is None:
            decision["action"] = "blocked_no_l2_book"
            return decision
        book_age_ns = time.perf_counter_ns() - book.updated_ns
        decision["book_age_ms"] = book_age_ns / 1_000_000.0
        if book_age_ns > self.max_book_age_ns:
            decision["action"] = "blocked_stale_l2_book"
            return decision
        sweep = book.ask_sweep(target.price_ceiling, target.max_notional_usdc)
        decision["sweep_levels"] = len(sweep)
        decision["sweep_notional"] = sum(px * sz for px, sz in sweep)
        if not sweep:
            decision["action"] = "blocked_no_liquidity_under_ceiling"
            if self.decision_logger:
                self.decision_logger.emit_nowait(decision)
            return decision
        if not self.circuit_breaker.can_submit(armed=self.armed, dry_run=self.dry_run, planned_notional=decision["sweep_notional"]):
            decision["action"] = "blocked_circuit_breaker"
            decision["block_reason"] = self.circuit_breaker.tripped_reason
            if self.decision_logger:
                self.decision_logger.emit_nowait(decision)
            return decision
        plan = self.plans.get(hit.icao)
        if not plan or not plan.signed_payloads:
            decision["action"] = "blocked_no_presigned_payloads"
            if self.decision_logger:
                self.decision_logger.emit_nowait(decision)
            return decision
        payloads = plan.select_payloads_for_sweep(sweep)
        if not payloads:
            decision["action"] = "blocked_no_presigned_payloads_for_sweep"
            if self.decision_logger:
                self.decision_logger.emit_nowait(decision)
            return decision
        # Hot path: no DB, no metadata fetch, no signing.  Send selected prebuilt payloads.
        responses = []
        for payload in payloads:
            r = await self.http.post("/orders", json=payload)
            responses.append({"status": r.status_code, "text": r.text[:300]})
        decision["action"] = "submitted"
        decision["submitted_payloads"] = len(payloads)
        decision["responses"] = responses
        decision["latency_total_us"] = (time.perf_counter_ns() - t0) / 1000.0
        if self.decision_logger:
            self.decision_logger.emit_nowait(decision)
        return decision

    async def close(self) -> None:
        await self.http.aclose()


def load_targets(path: str) -> list[StationTarget]:
    raw = json.loads(open(path, "r", encoding="utf-8").read())
    targets = raw.get("targets", raw)
    return [
        StationTarget(
            icao=str(t["icao"]).upper().encode("ascii"),
            city_slug=str(t.get("city_slug") or t["icao"]).lower(),
            market_slug=str(t.get("market_slug") or ""),
            yes_token=str(t.get("yes_token") or ""),
            threshold_c=int(t["threshold_c"]),
            direction=str(t.get("direction") or "gte"),
            price_ceiling=float(t.get("price_ceiling", 0.98)),
            max_notional_usdc=float(t.get("max_notional_usdc", 0)),
        )
        for t in targets
    ]


def _demo() -> None:
    targets = TargetIndex([StationTarget(b"KLGA", "nyc", "demo", "YES", 28)])
    samples = [
        b"SAUS70 KWBC 151500\nMETAR KLGA 151451Z 18007KT 10SM FEW050 28/14 A2992 RMK AO2",
        b"SPUS70 KWBC 151505\nSPECI KLGA 151501Z 18007KT 10SM FEW050 M02/M05 A2992 RMK AO2",
    ]
    for s in samples:
        print(parse_nwws_metar(s, targets))


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets-json")
    ap.add_argument("--demo", action="store_true")
    ap.add_argument("--armed", action="store_true")
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--disable-clob-ws", action="store_true")
    ap.add_argument("--max-book-age-ms", type=int, default=50)
    ap.add_argument("--validate-runtime-only", action="store_true")
    args = ap.parse_args()
    if args.demo:
        _demo(); return
    if not args.targets_json:
        raise SystemExit("--targets-json required unless --demo")
    queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=4096)
    targets = load_targets(args.targets_json)
    if args.validate_runtime_only:
        print(json.dumps({"ok": True, "targets": len(targets), "asset_ids": [t.yes_token for t in targets]}))
        return
    if not (os.getenv("NWWS_JID") and os.getenv("NWWS_PASSWORD")):
        raise SystemExit("NWWS_JID and NWWS_PASSWORD are required unless --validate-runtime-only or --demo")
    books = InMemoryBookStore()
    decision_logger = AsyncDecisionLogger()
    decision_logger.start()
    engine = WeatherLockExecutionEngine(
        targets,
        PreparedOrderCache(),
        armed=args.armed,
        dry_run=args.dry_run,
        books=books,
        max_book_age_ms=args.max_book_age_ms,
        decision_logger=decision_logger,
    )
    client = NwwsXmppClient(
        queue,
        jid=os.environ["NWWS_JID"],
        password=os.environ["NWWS_PASSWORD"],
        host=os.getenv("NWWS_HOST", "nwws-oi.weather.gov"),
        port=int(os.getenv("NWWS_PORT", "5222")),
        pubsub_jid=os.getenv("NWWS_PUBSUB_JID", "nwws-oi.weather.gov"),
        node=os.getenv("NWWS_NODE", "/products"),
    )
    listener = asyncio.create_task(client.run())
    clob_ws = None
    asset_ids = [t.yes_token for t in targets if t.yes_token and "PLACEHOLDER" not in t.yes_token.upper()]
    if asset_ids and not args.disable_clob_ws:
        clob_ws = asyncio.create_task(clob_l2_websocket_loop(asset_ids, books))
    try:
        while True:
            stanza = await queue.get()
            decision = await engine.on_stanza(stanza)
            if decision and not decision_logger.task:
                logger.info("NWWS weather-lock decision {}", decision)
    finally:
        listener.cancel()
        if clob_ws:
            clob_ws.cancel()
        await engine.close()


if __name__ == "__main__":
    asyncio.run(main())
