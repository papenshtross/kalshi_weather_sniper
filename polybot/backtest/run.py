"""Backtest runner.

Loads a YAML config, instantiates a Nautilus BacktestEngine, registers the
strategy specified in config, and streams Parquet historical data.

Usage:
    python -m polybot.backtest.run --config config/backtest.yaml
"""
from __future__ import annotations

import argparse
import importlib
from pathlib import Path
from typing import Any

import yaml
from loguru import logger


def load_class(path: str) -> type:
    """e.g. 'polybot.strategies.example_mm:ExampleMM'."""
    mod_name, cls_name = path.split(":")
    return getattr(importlib.import_module(mod_name), cls_name)


def run(config_path: Path) -> None:
    cfg: dict[str, Any] = yaml.safe_load(config_path.read_text())
    logger.info("Backtest config loaded: {}", cfg.get("name", "<unnamed>"))

    # ------------------------------------------------------------------ Nautilus wiring
    # Deferred import so this file is importable without nautilus installed yet.
    try:
        from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
        from nautilus_trader.model.identifiers import Venue
        from nautilus_trader.model.currencies import USD
        from nautilus_trader.model.enums import AccountType, OMSType
    except ImportError as e:
        raise SystemExit(
            f"nautilus_trader not installed — pip install -e '.[dev]'. ({e})"
        )

    engine = BacktestEngine(config=BacktestEngineConfig(trader_id="POLYBOT-001"))

    venue = Venue("POLYMARKET")
    engine.add_venue(
        venue=venue,
        oms_type=OMSType.NETTING,
        account_type=AccountType.CASH,
        base_currency=USD,
        starting_balances=[f"{cfg.get('starting_cash', 10_000)} USD"],
    )

    # TODO: load instruments from config['instruments']
    # TODO: stream data from config['data']['parquet_glob'] into engine.add_data(...)

    strategy_cls = load_class(cfg["strategy"]["class"])
    params = cfg["strategy"].get("params", {})
    strategy = strategy_cls(config=strategy_cls.config_cls(**params)) if hasattr(strategy_cls, "config_cls") else strategy_cls(**params)
    engine.add_strategy(strategy)

    logger.info("Running backtest…")
    engine.run()

    logger.info("=== Results ===")
    for report in engine.trader.generate_account_report(venue):
        logger.info(report)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, required=True)
    args = p.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
