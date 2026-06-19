"""Binary-market arbitrage with slow-side resting limit + reactive-IOC hedge.

Problem solved
--------------
Polymarket's CLOB has no atomic 2-leg primitive. When one outcome of a binary
market (YES/NO) dumps, the combined best-ask sum drops below 1.00 and there is
free money. But you cannot atomically buy both legs — REST calls land 50–200ms
apart, and FOK-on-both has ~50% race failure during fast moves.

Design
------
State machine (option 1 from the plan):
    SCANNING:
        Every book tick, recompute which leg is "slow" (wider spread / more
        illiquid top). Check the cold-hedge invariant:
            best_ask_fast + p_slow_limit  ≤  1 − 2*fee − min_edge
        If we can place a passive BID on the slow side at p_slow_limit such
        that the fast side's CURRENT best_ask still leaves room for the
        min_edge after a worst-case walk-up, place the limit. Go to
        WAITING_SLOW with a recorded max_p_fast budget.

    WAITING_SLOW:
        Update max_p_fast continuously. If best_ask_fast walks above the
        budget, CANCEL the slow limit and go back to SCANNING — no fill, no
        risk.
        On slow-side simulated fill (touch price ≤ p_slow_limit), transition
        to HEDGING.

    HEDGING:
        Immediately fire a fast-side IOC BUY at best_ask_fast with max price
        = max_p_fast. If best_ask_fast ≤ max_p_fast: fill, realize
        (1 − p_slow_fill − p_fast_fill − 2*fee) * size. Log success.
        If IOC rejects (book moved past budget in the microseconds since the
        slow fill): residual unhedged slow-side position, schedule urgent
        rebalance on next tick. Return to SCANNING.

Invariants
----------
- A slow-side limit is only placed when the fast-side CURRENT book supports
  profitable completion.
- A fast-side buy is only triggered AFTER a slow-side fill (never
  opportunistically alone) — the condition "we bought one side" gates the
  other.
- Paper mode: the "fill" happens when real best_bid_slow ≤ our p_slow_limit
  (the market's resting interest would have hit our order) OR when real
  best_ask_slow ≤ p_slow_limit (a crossing trade). We use the crossing-ask
  model because it's more conservative and matches live semantics.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Leg = Literal["YES", "NO"]


@dataclass
class ArbFill:
    ts: float
    kind: str              # "ARB" | "RESIDUAL"
    yes_px: float
    no_px: float
    size: float
    profit: float


@dataclass
class BinaryArbMM:
    market: str
    yes_token: str
    no_token: str

    # --- arb trigger (after fees) ---
    threshold: float = 0.99        # soft cap on sum(best_ask)
    fee_per_share: float = 0.0     # round-trip fee per leg
    min_edge: float = 0.002        # required net profit per share pair

    # --- sizing ---
    pair_size: float = 20.0
    max_inventory: float = 500.0

    # --- slow-side limit placement ---
    slow_offset: float = 0.01      # how far below best_bid we rest our slow-side limit
    max_wait_seconds: float = 60.0 # cancel if unfilled this long (avoid stale orders)

    # --- book state ---
    yes_bid: float = 0.0
    yes_ask: float = 0.0
    no_bid: float = 0.0
    no_ask: float = 0.0
    last_sum: float = 0.0

    # --- resting slow-side order (paper) ---
    state: str = "SCANNING"        # SCANNING | WAITING_SLOW | HEDGING
    slow_leg: Leg | None = None
    slow_limit_px: float = 0.0
    slow_budget_fast: float = 0.0  # max we'll pay on the fast side after slow fills
    slow_order_ts: float = 0.0

    # --- positions ---
    yes_pos: float = 0.0
    no_pos: float = 0.0
    yes_avg: float = 0.0
    no_avg: float = 0.0
    cash: float = 0.0              # realized lock-in

    fills: list[ArbFill] = field(default_factory=list)
    arb_count: int = 0
    residual_count: int = 0

    # ------------------------------------------------------------------ main tick

    def on_book(self, token: str, best_bid: float, best_ask: float, ts: float, simulate_fills: bool = True) -> list[ArbFill]:
        if not (0 < best_bid <= best_ask < 1):
            return []
        if token == self.yes_token:
            self.yes_bid, self.yes_ask = best_bid, best_ask
        elif token == self.no_token:
            self.no_bid, self.no_ask = best_bid, best_ask
        else:
            return []

        if self.yes_ask == 0 or self.no_ask == 0:
            return []

        self.last_sum = round(self.yes_ask + self.no_ask, 4)

        if self.state == "STOPPED":
            return []
        if self.state == "SCANNING":
            return self._tick_scanning(ts)
        elif self.state == "WAITING_SLOW":
            return self._tick_waiting(ts, simulate_fills=simulate_fills)
        return []  # HEDGING is instantaneous; never lingers across ticks

    # ------------------------------------------------------------------ SCANNING

    def _tick_scanning(self, ts: float) -> list[ArbFill]:
        # Pick slow leg = wider (best_ask - best_bid) spread — that's the one
        # most likely to drift unfilled. Ties broken by taking the cheaper leg
        # so resting there is cheap.
        yes_spread = self.yes_ask - self.yes_bid
        no_spread = self.no_ask - self.no_bid
        slow: Leg = "YES" if yes_spread >= no_spread else "NO"

        if slow == "YES":
            slow_bid, slow_ask, fast_ask = self.yes_bid, self.yes_ask, self.no_ask
        else:
            slow_bid, slow_ask, fast_ask = self.no_bid, self.no_ask, self.yes_ask

        # Compute the deepest profitable slow-side limit.
        # We want:  p_slow_limit + fast_ask + 2*fee + min_edge ≤ 1
        max_p_slow = 1.0 - fast_ask - 2 * self.fee_per_share - self.min_edge
        if max_p_slow <= 0.01:
            return []  # no room for profit even at the deepest tick

        # We rest slow_offset below best_bid so only a fast adverse move gets us filled.
        desired = round(max(0.01, slow_bid - self.slow_offset), 3)
        p_slow_limit = min(desired, round(max_p_slow, 3))
        if p_slow_limit <= 0.01:
            return []

        # Also honor the soft threshold cap: only play if sum(best_ask) is not
        # wildly above 1. (During quiet markets sum~1.01, we still want to rest
        # slow limits for adverse flow — so this is advisory.)
        if self.last_sum > self.threshold + 0.05:
            return []

        # Inventory caps
        if self.yes_pos + self.pair_size > self.max_inventory:
            return []
        if self.no_pos + self.pair_size > self.max_inventory:
            return []

        # Enter WAITING_SLOW
        self.state = "WAITING_SLOW"
        self.slow_leg = slow
        self.slow_limit_px = p_slow_limit
        self.slow_budget_fast = round(1.0 - p_slow_limit - 2 * self.fee_per_share - self.min_edge, 4)
        self.slow_order_ts = ts
        return []

    # ------------------------------------------------------------------ WAITING_SLOW

    def _tick_waiting(self, ts: float, simulate_fills: bool = True) -> list[ArbFill]:
        assert self.slow_leg is not None
        if self.slow_leg == "YES":
            slow_ask, fast_ask, fast_bid = self.yes_ask, self.no_ask, self.no_bid
        else:
            slow_ask, fast_ask, fast_bid = self.no_ask, self.yes_ask, self.yes_bid

        # Cancel stale orders
        if ts - self.slow_order_ts > self.max_wait_seconds:
            self._cancel_slow()
            return []

        # Cancel if fast side drifted past our budget — completing would lose money
        if fast_ask > self.slow_budget_fast:
            self._cancel_slow()
            return []

        # In live mode the supervisor owns actual execution/reconciliation and we
        # only maintain market-state transitions here.
        if not simulate_fills:
            return []

        # Did the slow side get filled? Paper model: if crossing ask reaches our
        # limit (best_ask_slow ≤ p_slow_limit), we're filled at p_slow_limit.
        if slow_ask <= self.slow_limit_px:
            return self._execute_arb(ts, fast_ask)

        return []

    def _cancel_slow(self) -> None:
        self.state = "SCANNING"
        self.slow_leg = None
        self.slow_limit_px = 0.0
        self.slow_budget_fast = 0.0

    def cancel_all(self) -> None:
        """Cancel any resting (paper) orders. Called when the strategy is
        stopped from the dashboard so no orders linger."""
        self._cancel_slow()
        self.state = "STOPPED"

    def apply_live_pair(self, ts: float, yes_px: float, no_px: float, size: float) -> ArbFill:
        """Apply an already-executed live YES+NO pair into local accounting."""
        self._buy("YES", yes_px, size)
        self._buy("NO", no_px, size)
        profit = round((1.0 - yes_px - no_px - 2 * self.fee_per_share) * size, 4)
        self.cash += profit
        self.arb_count += 1
        self.last_sum = round((self.yes_ask or yes_px) + (self.no_ask or no_px), 4)
        f = ArbFill(ts=ts, kind="ARB", yes_px=yes_px, no_px=no_px, size=size, profit=profit)
        self.fills.append(f)
        return f

    def apply_live_recovery_loss(self, buy_leg: Leg, buy_px: float, sell_px: float, size: float) -> float:
        """Apply realized PnL for a buy-then-emergency-unwind round-trip that ends flat."""
        realized = round((sell_px - buy_px) * size, 4)
        self.cash += realized
        self.last_sum = round((self.yes_ask or 0.0) + (self.no_ask or 0.0), 4)
        return realized

    def apply_live_residual(self, ts: float, leg: Leg, px: float, size: float) -> ArbFill:
        """Apply a one-sided live fill that could not be hedged or unwound."""
        self._buy(leg, px, size)
        self.residual_count += 1
        self.last_sum = round((self.yes_ask or 0.0) + (self.no_ask or 0.0), 4)
        f = ArbFill(
            ts=ts,
            kind="RESIDUAL",
            yes_px=(px if leg == "YES" else 0.0),
            no_px=(px if leg == "NO" else 0.0),
            size=size,
            profit=0.0,
        )
        self.fills.append(f)
        return f

    # ------------------------------------------------------------------ HEDGING

    def _execute_arb(self, ts: float, fast_ask_now: float) -> list[ArbFill]:
        assert self.slow_leg is not None
        slow = self.slow_leg
        size = self.pair_size

        # Slow fill locked in at our limit
        slow_px = self.slow_limit_px
        self._buy(slow, slow_px, size)

        # Reactive IOC hedge on fast leg. Budget = slow_budget_fast (precomputed
        # when we placed the slow order, enforced every tick in WAITING_SLOW,
        # so it's valid here unless there's a race).
        fast = "NO" if slow == "YES" else "YES"
        fast_px = fast_ask_now

        out: list[ArbFill] = []
        if fast_px <= self.slow_budget_fast:
            self._buy(fast, fast_px, size)
            profit = round((1.0 - slow_px - fast_px - 2 * self.fee_per_share) * size, 4)
            self.cash += profit
            self.arb_count += 1
            f = ArbFill(ts=ts, kind="ARB",
                        yes_px=(slow_px if slow == "YES" else fast_px),
                        no_px=(slow_px if slow == "NO" else fast_px),
                        size=size, profit=profit)
            self.fills.append(f); out.append(f)
        else:
            # Race lost: slow side filled but fast ask ran away. We carry
            # unhedged slow-leg inventory — record as residual for rebalancing.
            self.residual_count += 1
            f = ArbFill(ts=ts, kind="RESIDUAL",
                        yes_px=(slow_px if slow == "YES" else 0.0),
                        no_px=(slow_px if slow == "NO" else 0.0),
                        size=size, profit=0.0)
            self.fills.append(f); out.append(f)

        # Back to scanning
        self._cancel_slow()
        return out

    # ------------------------------------------------------------------ accounting

    def _buy(self, leg: Leg, px: float, size: float) -> None:
        if leg == "YES":
            new_pos = self.yes_pos + size
            total = self.yes_pos * self.yes_avg + size * px
            self.yes_avg = total / new_pos if new_pos else 0.0
            self.yes_pos = new_pos
        else:
            new_pos = self.no_pos + size
            total = self.no_pos * self.no_avg + size * px
            self.no_avg = total / new_pos if new_pos else 0.0
            self.no_pos = new_pos

    @property
    def unrealized(self) -> float:
        yes_val = self.yes_pos * (self.yes_bid or 0.0)
        no_val = self.no_pos * (self.no_bid or 0.0)
        cost = self.yes_pos * self.yes_avg + self.no_pos * self.no_avg
        # Paired inventory is also worth exactly $1 per pair at resolution —
        # mark it conservatively at the lower of bid-sum and 1.0.
        paired = min(self.yes_pos, self.no_pos)
        paired_bonus = paired * max(0.0, 1.0 - (self.yes_bid + self.no_bid or 0.0))
        return round((yes_val + no_val) - cost + paired_bonus, 4)

    @property
    def total_pnl(self) -> float:
        return round(self.cash + self.unrealized, 4)

    # ------------------------------------------------------------------ dashboard row

    def state_dict(self) -> dict:
        if self.yes_pos > 0 and self.no_pos > 0:
            side = "ARB"
        elif self.yes_pos > 0:
            side = "LONG-YES"
        elif self.no_pos > 0:
            side = "LONG-NO"
        else:
            side = self.state
        return {
            "market": self.market,
            "side": side,
            "size": round(self.yes_pos + self.no_pos, 2),
            "entry": round(self.threshold, 4),
            "last": round(self.last_sum, 4),
            "pnl": self.total_pnl,
        }
