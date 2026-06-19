# Restoring and Launching Polybot from GitHub

This document is the handoff for another Hermes Agent or operator to restore the current runnable project state from GitHub. It intentionally excludes machine-local secrets, logs, PID files, virtualenvs, and bulky API/backtest caches.

## Current restore point

- Branch: `main`
- Commit: see the `snapshot-20260503-live-recovery` tag (or run `git rev-parse HEAD` after checkout)
- Restore tag: `snapshot-20260503-live-recovery`
- Repository: `https://github.com/papenshtross/polybot.git`

Restore source/config/tests:

```bash
git clone https://github.com/papenshtross/polybot.git
cd polybot
git checkout snapshot-20260503-live-recovery
python3.11 -m venv .venv
. .venv/bin/activate
pip install -U pip
pip install -e '.[live,dev]'
python -m pytest -q tests/test_polymarket_execution.py tests/test_arb_sniper_execution.py tests/test_crypto_passive_mm.py tests/test_live_momentum_5m.py
```

If Python 3.11 is unavailable, use the closest supported 3.11/3.12 interpreter and rerun the targeted tests before launching live.

## What is included in Git

The snapshot includes the pieces needed to relaunch or inspect the live system:

- Source code under `polybot/`, including live arb sniper, crypto passive MM, weather safety filter, Polymarket adapters, persistence helpers, and momentum runners.
- Live strategy configs under `config/`, including per-city weather outlier shards, crypto arb sniper shards, supervisor configs, and passive MM configs.
- Operational scripts under `scripts/`, including weather safety/risk updaters, backtests, watchdogs, rescue/roundtrip scripts, and diagnostics.
- Tests under `tests/` for execution, live momentum, weather outlier, crypto fair model, passive MM, watchdog, and persistence behavior.
- Small backtest/report result files under `data/backtests/` and `reports/` that are useful for validating strategy choices.
- `uv.lock`, `pyproject.toml`, README, infrastructure docs, and this restore guide.

## What is intentionally excluded

These are intentionally not committed because they are unsafe, machine-specific, or too large for a normal GitHub restore snapshot:

- `.env`, `.env.live`: live keys, wallet settings, and database URLs. Recreate from `.env.example` and `.env.live.example` on the target machine.
- `.venv/`, `__pycache__/`, `.pytest_cache/`, `polybot.egg-info/`: local build/runtime caches.
- `*.log`, `*.pid`: local process artifacts; may contain trading traces or only apply to this boot session.
- `data/runtime/`: live Gamma/runtime cache, regenerated automatically.
- `data/backtests/cache/`: large raw market-input cache. Keep a separate artifact backup if exact raw replay data is needed.
- `reports/**/cache/`: large third-party API response caches. Reports and compact result JSON/CSV files are kept instead.
- `tmp_*.py`: scratch/debug scripts.

If a future operator needs the excluded raw caches, copy them from a separate encrypted backup or regenerate them with the scripts in `scripts/`.

## Required live secrets and environment

Create `.env` and `.env.live` locally after cloning. Do **not** commit real values.

Minimum live variables used by the current code paths:

```bash
# .env
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_PROXY_ADDRESS=0x...
POLYMARKET_SIGNATURE_TYPE=1
# Optional if not stored in the dashboard DB wallet_config:
POLYMARKET_API_KEY=...
POLYMARKET_API_SECRET=...
POLYMARKET_API_PASSPHRASE=...
POLYMARKET_BUILDER_CODE=...

# .env.live
export NAUTILUS_DB_URL=postgresql://polybot:...@host:5432/polybot
export POSTGRES_URL=postgresql://polybot:...@host:5432/polybot
```

The Polymarket client can also load wallet credentials from the dashboard `wallet_config` row when `POSTGRES_URL`/`NAUTILUS_DB_URL` points to the live dashboard database.

## Database restore/bring-up

The live writer creates/uses the required Polybot tables on connect. On a new machine:

1. Provision Postgres and create a database/user.
2. Put the connection string in `.env.live` as both `NAUTILUS_DB_URL` and `POSTGRES_URL`.
3. Run a smoke test that imports and connects the writer:

```bash
set -a
. .env
. .env.live
set +a
. .venv/bin/activate
python - <<'PY'
import asyncio, os
from polybot.persistence.writer import PolybotWriter

async def main():
    w = PolybotWriter(os.environ['POSTGRES_URL'])
    await w.connect()
    await w.close()
    print('DB writer connect OK')

asyncio.run(main())
PY
```

If restoring a production dashboard, restore the dashboard DB dump separately before starting live trading so strategy rows, wallet_config, and historical attempts are available.

## Launch options

### Option A: foreground smoke run

Use this first on a new machine. It validates imports/config/env without creating persistent services.

```bash
set -a
. .env
[ -f .env.live ] && . .env.live
set +a
. .venv/bin/activate
python -m polybot.live.arb_sniper --config config/weather-outlier-sniper-jakarta-live.yaml
```

Stop with `Ctrl-C` after it has loaded markets and written logs/DB state.

### Option B: legacy supervisor script

```bash
set -a
. .env
[ -f .env.live ] && . .env.live
set +a
. .venv/bin/activate
./scripts/live_supervisor_ctl.sh start
./scripts/live_supervisor_ctl.sh status
```

Logs are local and ignored by Git:

```bash
tail -f live-supervisor.log supervisor-live.log
```

### Option C: user-level systemd services for one-process-per-shard live ops

The current live machine uses user services like `polybot-weather-outlier-sniper-jakarta.service`, one config file per city/shard. On a restored Linux/WSL machine with systemd user services enabled:

```bash
mkdir -p ~/.config/systemd/user
PROJECT_DIR="$PWD"
PY="$PROJECT_DIR/.venv/bin/python"

for cfg in config/weather-outlier-sniper-*-live.yaml; do
  city="${cfg#config/weather-outlier-sniper-}"
  city="${city%-live.yaml}"
  unit="$HOME/.config/systemd/user/polybot-weather-outlier-sniper-${city}.service"
  cat > "$unit" <<EOF
[Unit]
Description=Polybot weather outlier sniper ${city}
After=network-online.target

[Service]
Type=simple
WorkingDirectory=${PROJECT_DIR}
EnvironmentFile=${PROJECT_DIR}/.env
EnvironmentFile=-${PROJECT_DIR}/.env.live
ExecStart=${PY} -m polybot.live.arb_sniper --config ${PROJECT_DIR}/${cfg}
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF
done

systemctl --user daemon-reload
systemctl --user enable --now polybot-weather-outlier-sniper-*.service
systemctl --user list-units 'polybot-weather-outlier-sniper*' --no-pager
```

For crypto arb/passive-MM shards, create equivalent units using the matching config files, for example:

```ini
ExecStart=/path/to/polybot/.venv/bin/python -m polybot.live.arb_sniper --config /path/to/polybot/config/arb-sniper-crypto-btc5m-live.yaml
```

Do not start live trading services on a clone until the wallet, proxy address, DB, and strategy settings have been verified.

## Verification after launch

Run these checks before considering the restore successful:

```bash
# Code/import/test verification
. .venv/bin/activate
python -m py_compile polybot/live/arb_sniper.py polybot/live/crypto_passive_mm.py polybot/live/momentum_5m_runner.py polybot/adapters/polymarket/execution.py
python -m pytest -q tests/test_polymarket_execution.py tests/test_arb_sniper_execution.py tests/test_crypto_passive_mm.py tests/test_live_momentum_5m.py

# Service verification, if systemd was used
systemctl --user list-units 'polybot-*' --no-pager
journalctl --user -u 'polybot-weather-outlier-sniper-*' -n 100 --no-pager
```

For live DB verification:

```bash
set -a; . .env; [ -f .env.live ] && . .env.live; set +a
python - <<'PY'
import asyncio, os, asyncpg
async def main():
    con = await asyncpg.connect(os.environ['POSTGRES_URL'])
    print(await con.fetchval("select count(*) from strategies"))
    print(await con.fetchval("select max(ts) from order_attempts"))
    await con.close()
asyncio.run(main())
PY
```

## Restore/rollback commands

To restore exactly this GitHub snapshot on another machine:

```bash
git clone https://github.com/papenshtross/polybot.git
cd polybot
git checkout snapshot-20260503-live-recovery
```

To reset an existing clone to the snapshot:

```bash
git fetch --all --tags
git checkout main
git reset --hard snapshot-20260503-live-recovery
```

Only then recreate local `.env`/`.env.live`, install dependencies, run tests, and start services.
