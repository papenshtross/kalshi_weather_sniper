# polybot

Polymarket trading bot built on [Nautilus Trader](https://github.com/nautechsystems/nautilus_trader).

Designed so you can **drop in new strategies without touching the core engine, execution, or backtest code**.

## Architecture

```
polybot/
├── adapters/polymarket/    # Polymarket CLOB adapter (DataClient + ExecutionClient)
│   ├── client.py           # thin wrapper around py-clob-client
│   ├── data.py             # MarketDataClient: books, trades, quotes → Nautilus events
│   ├── execution.py        # ExecutionClient: order submit/cancel/fill routing
│   └── instruments.py      # Polymarket market → Nautilus Instrument mapping
├── strategies/             # ← PLUGGABLE. Add new files here, no core changes.
│   └── example_mm.py       # simple market-making example
├── data/                   # historical data loaders
│   └── goldsky.py          # Goldsky subgraph → Parquet backfill
├── backtest/               # backtest runners (same strategy code as live)
│   └── run.py
├── live/                   # live / paper trading runners
│   └── run.py
└── config/                 # YAML configs for nodes, strategies, risk
```

Same `Strategy` class runs in:
- **backtest** — `python -m polybot.backtest.run --config config/backtest.yaml`
- **paper**   — `python -m polybot.live.run --config config/paper.yaml`
- **live**    — `python -m polybot.live.run --config config/live.yaml`

## Why Nautilus (vs Hummingbot)

- Strict separation DataEngine / ExecutionEngine / RiskEngine / Strategy
- Backtest ≡ paper ≡ live — identical event flow, fills, order lifecycle
- Rust core, nanosecond timestamps, L2 replay
- Strategies are standalone plug-in classes; live in their own files/repo
- Hummingbot is more mature as a *market-maker product* but its backtesting is weak and strategies are tightly coupled to connectors

## Dashboard

No built-in UI. Use the included Grafana stack (`infra/docker-compose.yml`) which
reads from the Nautilus Postgres persistence layer. See `infra/README.md`.

## Historical data

Polymarket does not expose deep L2 history publicly. We backfill trades and order
events from the official [Goldsky subgraph](https://github.com/Polymarket/goldsky-subgraph)
into Parquet. See `polybot/data/goldsky.py` and `scripts/backfill.sh`.

## Quickstart

```bash
git clone https://github.com/papenshtross/polybot.git
cd polybot
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Backfill 30 days of history for a market
python -m polybot.data.goldsky --market <condition_id> --days 30 \
    --out data/parquet/

# Backtest the example strategy
python -m polybot.backtest.run --config config/backtest.yaml

# Paper trade against live Polymarket feed
cp .env.example .env  # fill in POLYMARKET_PRIVATE_KEY etc.
python -m polybot.live.run --config config/paper.yaml
```

## Status

🚧 **Scaffold.** Adapter stubs are in place but not yet wired to a running Nautilus
`TradingNode`. The Polymarket I/O primitives are lifted in spirit from
[warproxxx/poly-maker](https://github.com/warproxxx/poly-maker) — the only
production-quality open-source Polymarket bot I could find.

## License

MIT
