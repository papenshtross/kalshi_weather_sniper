from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from typing import Any

from polybot.backtest.binance_strategy_lab import (
    BinanceLabConfig,
    PricePoint,
    SideSignal,
    StrategySpec,
    TrendSignal,
    build_strategy_universe,
    estimate_breakout_probability,
)


TOP_STRATEGY_NAME = "momentum_consensus_07"


def top_momentum_strategy_spec() -> StrategySpec:
    for spec in build_strategy_universe():
        if spec.name == TOP_STRATEGY_NAME:
            return spec
    raise RuntimeError(f"strategy {TOP_STRATEGY_NAME} not found")



def top_dynamic_momentum_strategy_spec() -> StrategySpec:
    return replace(top_momentum_strategy_spec(), price_cap=None)


def dynamic_momentum_strategy_spec_from_config(cfg: dict[str, Any] | None = None) -> StrategySpec:
    """Build the live dynamic momentum spec, allowing DB/YAML config overrides.

    The live dashboard/DB config is the source of truth for operational knobs.
    Keeping min_consensus configurable lets us tighten the live 5-window strategy
    from the backtest default (3/5) to stricter variants such as 5/5 without
    editing the shared backtest strategy universe.
    """
    spec = top_dynamic_momentum_strategy_spec()
    cfg = cfg or {}
    if "min_consensus" not in cfg or cfg.get("min_consensus") is None:
        return spec
    min_consensus = int(cfg["min_consensus"])
    if min_consensus < 1 or min_consensus > len(spec.windows):
        raise ValueError(f"min_consensus must be between 1 and {len(spec.windows)}, got {min_consensus}")
    return replace(spec, min_consensus=min_consensus)



def _close_at(candles: list[Any], ts: int) -> float | None:
    latest = None
    for candle in candles:
        if candle.ts <= ts:
            latest = candle.close
        else:
            break
    return latest



def build_live_trade_signal(
    candles: list[Any],
    up_ask: float,
    down_ask: float,
    market_end_ts: int,
    price_to_beat: float,
    spec: StrategySpec | None = None,
    config: BinanceLabConfig | None = None,
):
    strategy = spec or top_momentum_strategy_spec()
    lab_cfg = config or BinanceLabConfig()
    entry_ts = market_end_ts - strategy.entry_seconds_before_close
    from polybot.backtest.binance_strategy_lab import evaluate_strategy_signal
    return evaluate_strategy_signal(
        candles=candles,
        up_points=[PricePoint(ts=entry_ts, price=up_ask)],
        down_points=[PricePoint(ts=entry_ts, price=down_ask)],
        market_end_ts=market_end_ts,
        price_to_beat=price_to_beat,
        config=lab_cfg,
        strategy=strategy,
    )



def build_live_now_signal(
    candles: list[Any],
    up_ask: float,
    down_ask: float,
    current_ts: int,
    price_to_beat: float,
    spec: StrategySpec | None = None,
    config: BinanceLabConfig | None = None,
) -> TrendSignal:
    strategy = spec or top_dynamic_momentum_strategy_spec()
    lab_cfg = config or BinanceLabConfig()
    entry_close = _close_at(candles, current_ts)
    if entry_close is None:
        return TrendSignal(side=SideSignal.SKIP, score=0.0, entry_ts=current_ts, entry_price=None, reason="missing_binance_entry")

    score = 0
    for window in strategy.windows:
        base_price = _close_at(candles, current_ts - window)
        if base_price is None:
            return TrendSignal(side=SideSignal.SKIP, score=0.0, entry_ts=current_ts, entry_price=None, reason="insufficient_history")
        delta = entry_close - base_price
        if delta > lab_cfg.epsilon:
            score += 1
        elif delta < -lab_cfg.epsilon:
            score -= 1

    if score >= strategy.min_consensus:
        side = SideSignal.UP
        entry_price = up_ask
    elif score <= -strategy.min_consensus:
        side = SideSignal.DOWN
        entry_price = down_ask
    else:
        return TrendSignal(side=SideSignal.SKIP, score=float(score), entry_ts=current_ts, entry_price=None, reason="no_consensus")

    breakout_probability = estimate_breakout_probability(
        candles=candles,
        entry_ts=current_ts,
        entry_price=entry_close,
        market_end_ts=current_ts + 1,
        config=lab_cfg,
        breakout_level=price_to_beat,
    )
    opposite_implied_probability = max(0.0, min(1.0, 1.0 - entry_price))
    return TrendSignal(
        side=side,
        score=float(score),
        entry_ts=current_ts,
        entry_price=entry_price,
        breakout_probability=breakout_probability,
        opposite_implied_probability=opposite_implied_probability,
    )



def build_fixed_stake_order(best_ask: float, top_ask_size: float, stake_usd: float) -> dict[str, float] | None:
    if best_ask <= 0 or top_ask_size <= 0 or stake_usd <= 0:
        return None
    shares = float((Decimal(str(stake_usd)) / Decimal(str(best_ask))).quantize(Decimal("0.0001"), rounding=ROUND_DOWN))
    if shares <= 0:
        return None
    if top_ask_size + 1e-9 < shares:
        return None
    return {
        "shares": shares,
        "limit_price": best_ask,
        "stake_usd": stake_usd,
    }


def build_fixed_stake_order_from_asks(
    asks: list[dict[str, float]],
    stake_usd: float,
    slippage_ticks: int = 1,
) -> dict[str, float] | None:
    """Build a taker BUY order using cumulative ask depth through a cent-tick cap.

    Polymarket FOK market buys are all-or-kill for the notional amount. Checking
    only the best ask level is fragile: the best level can disappear between the
    REST book snapshot and order arrival, producing intermittent FOK kills. Use
    visible cumulative depth through a small configurable price buffer instead.
    """
    if not asks or stake_usd <= 0:
        return None

    levels = sorted(
        (
            {"price": float(level.get("price", 0.0)), "size": float(level.get("size", 0.0))}
            for level in asks
        ),
        key=lambda level: level["price"],
    )
    levels = [level for level in levels if 0 < level["price"] < 1 and level["size"] > 0]
    if not levels:
        return None

    tick = Decimal("0.01")
    best_ask = Decimal(str(levels[0]["price"]))
    ticks = max(0, int(slippage_ticks or 0))
    limit_price = min(
        Decimal("0.99"),
        best_ask.quantize(tick, rounding=ROUND_CEILING) + (tick * ticks),
    )

    available_notional = Decimal("0")
    for level in levels:
        price = Decimal(str(level["price"]))
        if price > limit_price:
            break
        available_notional += price * Decimal(str(level["size"]))
        if available_notional + Decimal("0.0000001") >= Decimal(str(stake_usd)):
            shares = (Decimal(str(stake_usd)) / limit_price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
            if shares <= 0:
                return None
            return {
                "shares": float(shares),
                "limit_price": float(limit_price),
                "stake_usd": stake_usd,
                "available_notional": float(available_notional),
            }

    return None


@dataclass
class MomentumLiveState:
    market: str
    market_slug: str
    end_dt: datetime | None
    cash: float = 0.0
    side: str = "FLAT"
    token_id: str | None = None
    size: float = 0.0
    stake_usd: float = 0.0
    entry: float = 0.0
    last: float = 0.0
    filled_ts: int | None = None
    resolved: bool = False
    hedge_side: str | None = None
    hedge_token_id: str | None = None
    hedge_size: float = 0.0
    hedge_stake_usd: float = 0.0
    hedge_entry: float = 0.0
    hedge_filled_ts: int | None = None

    def has_position(self) -> bool:
        return self.size > 0 and self.side in {"UP", "DOWN"}

    def is_hedged(self) -> bool:
        return self.side == "HEDGED" and self.size > 0 and self.hedge_size > 0

    def apply_fill(self, side: str, token_id: str, entry_price: float, stake_usd: float, shares: float, ts: int) -> None:
        self.side = side
        self.token_id = token_id
        self.entry = entry_price
        self.last = entry_price
        self.stake_usd = stake_usd
        self.size = shares
        self.filled_ts = ts
        self.resolved = False
        self.hedge_side = None
        self.hedge_token_id = None
        self.hedge_size = 0.0
        self.hedge_stake_usd = 0.0
        self.hedge_entry = 0.0
        self.hedge_filled_ts = None

    def apply_hedge(self, side: str, token_id: str, hedge_price: float, stake_usd: float, shares: float, ts: int) -> None:
        if not self.has_position():
            return
        self.hedge_side = side
        self.hedge_token_id = token_id
        self.hedge_entry = hedge_price
        self.hedge_stake_usd = stake_usd
        self.hedge_size = shares
        self.hedge_filled_ts = ts
        self.last = hedge_price
        self.side = "HEDGED"

    def locked_pnl(self) -> float:
        if not self.is_hedged():
            return 0.0
        matched = min(self.size, self.hedge_size)
        return matched * (1.0 - self.entry - self.hedge_entry)

    def mark_price(self, best_bid: float) -> None:
        if self.has_position():
            self.last = best_bid

    def unrealized_pnl(self) -> float:
        if self.is_hedged():
            return self.locked_pnl()
        if not self.has_position():
            return 0.0
        return (self.last - self.entry) * self.size

    def settle(self, winner: SideSignal, ts: int) -> float:
        if self.is_hedged():
            realized = self.locked_pnl()
            self.cash += realized
            self.side = "FLAT"
            self.token_id = None
            self.size = 0.0
            self.stake_usd = 0.0
            self.entry = 0.0
            self.last = 0.0
            self.hedge_side = None
            self.hedge_token_id = None
            self.hedge_size = 0.0
            self.hedge_stake_usd = 0.0
            self.hedge_entry = 0.0
            self.hedge_filled_ts = ts
            self.filled_ts = ts
            self.resolved = True
            return realized
        if not self.has_position():
            return 0.0
        is_win = (self.side == "UP" and winner == SideSignal.UP) or (self.side == "DOWN" and winner == SideSignal.DOWN)
        payout = self.size if is_win else 0.0
        realized = payout - self.stake_usd
        self.cash += realized
        self.side = "FLAT"
        self.token_id = None
        self.size = 0.0
        self.stake_usd = 0.0
        self.entry = 0.0
        self.last = 0.0
        self.filled_ts = ts
        self.resolved = True
        return realized

    def state_dict(self) -> dict[str, float | str]:
        return {
            "market": self.market,
            "side": self.side,
            "size": self.size,
            "entry": self.entry,
            "last": self.last,
            "pnl": self.unrealized_pnl(),
            "cash": self.cash,
            "hedge_side": self.hedge_side or "",
            "hedge_size": self.hedge_size,
            "hedge_entry": self.hedge_entry,
            "hedge_stake_usd": self.hedge_stake_usd,
            "locked_pnl": self.locked_pnl(),
        }
