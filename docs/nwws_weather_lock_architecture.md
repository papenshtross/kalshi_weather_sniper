# NWWS-OI Weather-Lock Execution Engine (staged / disabled)

## Safety state

This implementation is staged for research/paper operation only.

Live submission is blocked unless all independent gates are changed deliberately:

- dashboard strategy status is running
- `live_launch_armed=true`
- `dry_run=false`
- `max_notional_usdc>0`
- pre-signed order payload cache is populated
- `POLYBOT_NWWS_LIVE_UNLOCK=I_UNDERSTAND_THIS_IS_LIVE`
- Prism 3 auth/order probe has passed for this execution path

The current dashboard row `live_nwws_weather_lock_prism3_v1` is stopped, unarmed, dry-run, and max notional 0.

## Architecture

```text
NWWS-OI Openfire/XMPP
  nwws-oi.weather.gov:5222 STARTTLS
  XEP-0060 PubSub live node only
        │
        ▼
NwwsXmppClient
  receives pushed stanzas
  converts boundary XML stanza to bytes
  put_nowait() into bounded queue
        │
        ▼
Compiled hot parser
  polybot.live._nwws_fast.parse_any(bytes, station_tuple)
  no regex
  byte scan for SAUS/SPUS/METAR/SPECI
  target ICAO only, e.g. KLGA
  extracts TT from TT/DD, e.g. 28/14
        │
        ▼
WeatherLockExecutionEngine
  threshold check against in-memory StationTarget
  no DB/Gamma/REST market data/signing on critical path
        │
        ├── Polymarket CLOB websocket state
        │     wss://ws-subscriptions-clob.polymarket.com/ws/market
        │     InMemoryBookStore[token_id] -> sorted L2Book
        │
        ▼
Sweep planner
  checks fresh L2 book age
  computes ask sweep under price ceiling and notional cap
        │
        ▼
Circuit breaker
  dry_run / armed / env unlock / notional cap
        │
        ▼
Pre-signed payload dispatch
  POST https://clob.polymarket.com/orders
  HTTP/2 warm client
```

## Hot-path invariants

The METAR-triggered decision path must not perform:

- database reads/writes
- Gamma API calls
- CLOB book REST calls
- EIP-712 signing
- synchronous logging
- filesystem writes

Allowed hot-path state:

- `TargetIndex`
- `InMemoryBookStore`
- `PreparedOrderPlan.signed_payloads`
- bounded queues / memory only

## Current implementation files

- `polybot/live/nwws_weather_execution.py`
- `polybot/live/_nwws_fast.c`
- `tests/test_nwws_weather_execution.py`
- `scripts/deploy_nwws_weather_lock_strategy.py`
- `polybot-dash/app/page.js`
- `polybot-dash/app/api/strategies/route.js`

## Python runtime dependencies

Already used in polybot:

- `asyncio`
- `httpx` with HTTP/2
- `websockets`
- `loguru`

Optional NWWS runtime dependency:

- `slixmpp` for XMPP + XEP-0060 PubSub

Build dependency for compiled parser:

- CPython headers
- GCC/Clang
- `setuptools`

Build command:

```bash
cd /home/administrator/projects/polybot
python3 setup.py build_ext --inplace
python3 -m pytest tests/test_nwws_weather_execution.py -q
```

## Future Rust/PyO3 standalone VPS stack

For a standalone VPS version, replace the Python orchestration hot path with Rust and expose only control/telemetry to Python if needed.

Recommended crates:

- `tokio` — async runtime
- `tokio-tungstenite` — CLOB websocket
- `reqwest` — warmed HTTP/2 REST submit client
- `quick-xml` — if XML fields must be parsed structurally
- `bytes` — zero-copy byte buffers
- `rtrb` or `crossbeam-queue` — lock-free/fast producer-consumer channel
- `serde`, `serde_json` — CLOB websocket/order payloads
- `tracing`, `tracing-appender` — non-blocking telemetry
- `pyo3` — optional Python extension boundary
- `zeroize` — secret memory hygiene if signing/pre-signing moves into Rust

Rust thread model:

```text
thread 1: NWWS XMPP TCP/TLS reader
thread 2: CLOB websocket book maintainer
thread 3: execution worker with pre-signed order cache
thread 4: async logger/telemetry drain
```

Communication:

- NWWS thread sends parsed `MetarHit` over lock-free ring buffer.
- CLOB book thread updates atomically swapped book snapshots or single-writer shared state.
- Execution worker consumes `MetarHit`, reads latest book snapshot, and submits pre-signed payload.

## XMPP details

- host: `nwws-oi.weather.gov`
- port: `5222`
- transport: TCP + STARTTLS
- auth: NWWS-OI JID/password from environment
- PubSub: XEP-0060
- subscription mode: live pushes only; do not request historical replay

Environment variables:

```bash
NWWS_JID='...'
NWWS_PASSWORD='...'
NWWS_HOST='nwws-oi.weather.gov'
NWWS_PORT='5222'
NWWS_PUBSUB_JID='nwws-oi.weather.gov'
NWWS_NODE='/products'
```

## CLOB websocket details

URL:

```text
wss://ws-subscriptions-clob.polymarket.com/ws/market
```

Subscribe message:

```json
{
  "type": "market",
  "assets_ids": ["<YES_TOKEN_ID>", "..."],
  "custom_feature_enabled": true
}
```

Handled event types:

- `book`
- `price_change`
- absent/legacy event type if payload contains `asset_id`/`token_id` + `bids`/`asks`

## Activation blockers

Do not activate until these are complete:

1. NWWS-OI credentials are installed in a service environment.
2. Active market mapper creates `StationTarget` rows from live Polymarket weather markets.
3. Token metadata includes `neg_risk`, `tick_size`, and Prism 3 signing params.
4. Pre-signed payload cache is wired to `py-clob-client-v2` or Rust signer warmup.
5. Tiny Prism 3 roundtrip probe passes on this specific path.
6. VPS region latency benchmark selects a host by warm p90/p99.
7. Dry-run replay using recorded METAR + recorded CLOB book snapshots proves threshold/sweep behavior.
