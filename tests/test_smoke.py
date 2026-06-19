"""Smoke tests — importable without nautilus_trader installed."""
import importlib


def test_package_imports():
    assert importlib.import_module("polybot").__version__


def test_adapter_imports():
    mod = importlib.import_module("polybot.adapters.polymarket")
    assert hasattr(mod, "PolymarketHttpClient")
    assert hasattr(mod, "PolymarketDataClient")
    assert hasattr(mod, "PolymarketExecutionClient")
    assert hasattr(mod, "PolymarketInstrumentProvider")


def test_strategy_importable():
    importlib.import_module("polybot.strategies.example_mm")


def test_goldsky_cli_importable():
    importlib.import_module("polybot.data.goldsky")
