# Binance BTC 5m Strategy Lab Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Build a reusable strategy-lab backtester that evaluates 50 Binance-derived BTC direction strategies for Polymarket BTC 5m markets, validates backtest/live signal parity, and reports the top 5 strategies by PnL.

**Architecture:** Add a shared signal engine that operates on Binance 1-second candles plus Polymarket per-outcome trade points. The backtest runner will fetch/prepare market inputs once, run 50 named strategies through the same signal engine used for parity validation, and persist ranked results to JSON for dashboard or Telegram reporting.

**Tech Stack:** Python 3.11, requests, pytest, dataclasses, existing polybot backtest modules.

---

### Task 1: Inspect the current BTC 5m backtest pipeline
- Read `polybot/backtest/binance_multiframe_trend_5m.py`
- Read `scripts/backtest_binance_multiframe_trend_5m.py`
- Read `tests/test_binance_multiframe_trend_5m.py`
- Identify reusable pieces and parity gaps

### Task 2: Add failing tests for richer signal families and parity
- Create tests for:
  - drift/vol signal
  - EMA slope consensus
  - VWAP acceptance / taker imbalance confirmation
  - breakout signal
  - strategy-spec generation count = 50
  - replay/live evaluator parity against backtest evaluator
- Run targeted pytest and verify failure first

### Task 3: Implement shared signal-engine module
- Add a new backtest module with:
  - candle dataclass carrying OHLCV and taker-buy volume
  - signal families and helpers
  - `StrategySpec`
  - `evaluate_strategy_signal(...)`
  - `backtest_market(...)`
  - `build_strategy_universe()` returning 50 named strategies

### Task 4: Implement runner with data prep, validation, and ranking
- Add script to:
  - fetch Gamma market metadata
  - fetch Polymarket Data API trades
  - fetch Binance 1s klines including OHLCV/taker-buy data
  - prepare market inputs once
  - run the first strategy validation checks on sampled markets
  - evaluate all 50 strategies
  - rank and save JSON artifact

### Task 5: Verify correctness and parity
- Run focused pytest
- Run a small-sample dry run
- Run a sampled validation command showing decision timestamps, contract prices, and parity checks
- Run the 30-day full sweep

### Task 6: Summarize top 5 strategies
- Extract top 5 by Chainlink net PnL
- Include win rate, trade count, avg pnl, skip reasons, and key parameters
- Note limitations around fill model and live-execution assumptions
