#!/usr/bin/env python3
"""Live-vs-backtest parity diagnostics for BTC 5m LightGBM strategies.

Read-only script. It uses persisted live observations/order attempts to quantify
where a LightGBM probability-edge backtest can diverge from live Polymarket
execution: repeated samples vs first fills, trade-print proxy vs live ask/FOK,
fees, stake sizing, order_version_mismatch, and missing exact book timestamps.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg
import httpx

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
FEE_RATE = 0.072
DEFAULT_STRATEGIES = (
    "live_lightgbm_btc5m_v1",
    "live_lightgbm_probability_edge_btc5m_v1",
    "live_lightgbm_probability_edge_btc5m_v1_filtered",
    "live_lightgbm_probability_edge_btc5m_v2",
)


def _json(x: Any) -> Any:
    if x is None:
        return {}
    if isinstance(x, (dict, list)):
        return x
    try:
        return json.loads(x)
    except Exception:
        return {}


def _f(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _fee_per_share(price: float) -> float:
    return FEE_RATE * price * (1.0 - price)


def _is_fok_error(error: str | None, response: Any) -> bool:
    text = (error or "") + " " + json.dumps(_json(response), sort_keys=True)
    return "fully filled" in text or "FOK" in text


def _is_version_mismatch(error: str | None, response: Any) -> bool:
    text = (error or "") + " " + json.dumps(_json(response), sort_keys=True)
    return "order_version_mismatch" in text


def _outcome_won(outcome: str, up_wins: bool) -> bool:
    side = str(outcome or "").upper()
    return (side in {"UP", "YES"} and up_wins) or (side in {"DOWN", "NO"} and not up_wins)


def _best_depth_fillable(asks: list[dict[str, Any]], *, limit_price: float, requested_shares: float) -> tuple[bool, float]:
    depth = 0.0
    for level in sorted(asks or [], key=lambda x: float(x.get("price", 0.0))):
        px = float(level.get("price", 0.0) or 0.0)
        if px <= limit_price + 1e-12:
            depth += float(level.get("size", 0.0) or 0.0)
    return depth + 1e-9 >= requested_shares, depth


async def _fetch_gamma_outcomes(slugs: set[str]) -> dict[str, dict[str, float]]:
    """Fetch finalPrice/priceToBeat by exact repeated slug batches."""
    out: dict[str, dict[str, float]] = {}
    if not slugs:
        return out
    async with httpx.AsyncClient(timeout=20) as client:
        ordered = sorted(slugs)
        for i in range(0, len(ordered), 40):
            batch = ordered[i : i + 40]
            params: list[tuple[str, str]] = [("slug", s) for s in batch]
            params.append(("limit", str(len(batch))))
            try:
                resp = await client.get(GAMMA_EVENTS_URL, params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                continue
            events = data if isinstance(data, list) else data.get("events", []) if isinstance(data, dict) else []
            for ev in events:
                slug = str(ev.get("slug") or "")
                md = ((ev.get("events") or [{}])[0].get("eventMetadata") if isinstance(ev.get("events"), list) else None) or ev.get("eventMetadata") or {}
                if isinstance(md, str):
                    md = _json(md)
                ptb = _f(md.get("priceToBeat") or ev.get("priceToBeat"))
                final = _f(md.get("finalPrice") or ev.get("finalPrice"))
                if slug and ptb is not None and final is not None:
                    out[slug] = {"price_to_beat": ptb, "final_price": final, "up_wins": final >= ptb}
    return out


@dataclass
class StrategyDiag:
    strategy_id: str
    observation_count: int = 0
    observed_markets: int = 0
    first_observation_ts: str | None = None
    last_observation_ts: str | None = None
    signal_counts: dict[str, int] | None = None
    skip_reasons: dict[str, int] | None = None
    attempts: int = 0
    fills: int = 0
    rejected: int = 0
    fok_kills: int = 0
    order_version_mismatch: int = 0
    other_rejections: int = 0
    resolved_fills: int = 0
    resolved_wins: int = 0
    gross_stake_usd: float = 0.0
    net_pnl_usd_est: float = 0.0
    avg_fill_price: float | None = None
    avg_selected_edge: float | None = None
    avg_seconds_remaining_at_attempt: float | None = None
    live_book_replay_matches_order: int = 0
    live_book_replay_mismatches: int = 0
    visible_depth_fillable_at_nearest_obs: int = 0
    visible_depth_unfillable_at_nearest_obs: int = 0
    median_nearest_obs_lag_s: float | None = None
    notes: list[str] | None = None


async def build_report(postgres_url: str, strategies: tuple[str, ...]) -> dict[str, Any]:
    con = await asyncpg.connect(postgres_url)
    try:
        report: dict[str, Any] = {
            "generated_at": datetime.now(UTC).isoformat(),
            "fee_model": "Polymarket taker fee per share = 0.072 * price * (1 - price)",
            "scope": list(strategies),
            "diagnostics": {},
            "global_findings": [],
        }
        # Observation summaries.
        obs_rows = await con.fetch(
            """
            select strategy_id, count(*) obs, count(distinct market_slug) markets, min(ts) first_ts, max(ts) last_ts
            from market_observations where strategy_id=any($1::text[]) group by 1
            """,
            list(strategies),
        )
        obs_summary = {r["strategy_id"]: r for r in obs_rows}
        # Signal distribution.
        signal_rows = await con.fetch(
            """
            select strategy_id,
                   coalesce(signal->>'side', signal->>'action', 'missing') side,
                   coalesce(signal->>'reason','') reason,
                   count(*) n
            from market_observations
            where strategy_id=any($1::text[])
            group by 1,2,3
            """,
            list(strategies),
        )
        signal_counts: dict[str, Counter[str]] = defaultdict(Counter)
        skip_reasons: dict[str, Counter[str]] = defaultdict(Counter)
        for r in signal_rows:
            signal_counts[r["strategy_id"]][r["side"]] += int(r["n"])
            if r["side"] in {"SKIP", "NO_TRADE"} or r["reason"]:
                skip_reasons[r["strategy_id"]][r["reason"] or r["side"]] += int(r["n"])

        attempts = await con.fetch(
            """
            select * from order_attempts
            where strategy_id=any($1::text[])
            order by strategy_id, ts
            """,
            list(strategies),
        )
        attempt_slugs = {str(a["market_slug"]) for a in attempts if a["market_slug"]}
        gamma_outcomes = await _fetch_gamma_outcomes(attempt_slugs)

        # Nearest-observation rows for replay. Pull all observations for attempted markets only.
        obs_for_attempts = await con.fetch(
            """
            select id, strategy_id, ts, market_slug, up_asks, down_asks, signal, config
            from market_observations
            where strategy_id=any($1::text[]) and market_slug=any($2::text[])
            order by strategy_id, market_slug, ts
            """,
            list(strategies),
            list(attempt_slugs) if attempt_slugs else ["__none__"],
        )
        obs_by_key: dict[tuple[str, str], list[Any]] = defaultdict(list)
        for o in obs_for_attempts:
            obs_by_key[(o["strategy_id"], o["market_slug"])].append(o)

        by_strategy: dict[str, StrategyDiag] = {sid: StrategyDiag(strategy_id=sid, signal_counts={}, skip_reasons={}, notes=[]) for sid in strategies}
        for sid, r in obs_summary.items():
            d = by_strategy[sid]
            d.observation_count = int(r["obs"])
            d.observed_markets = int(r["markets"])
            d.first_observation_ts = r["first_ts"].isoformat() if r["first_ts"] else None
            d.last_observation_ts = r["last_ts"].isoformat() if r["last_ts"] else None
        for sid in strategies:
            by_strategy[sid].signal_counts = dict(signal_counts.get(sid, {}))
            by_strategy[sid].skip_reasons = dict(skip_reasons.get(sid, {}))

        fill_prices: dict[str, list[float]] = defaultdict(list)
        selected_edges: dict[str, list[float]] = defaultdict(list)
        seconds_remaining: dict[str, list[float]] = defaultdict(list)
        nearest_lags: dict[str, list[float]] = defaultdict(list)

        for a in attempts:
            sid = a["strategy_id"]
            d = by_strategy[sid]
            d.attempts += 1
            status = str(a["status"] or "").lower()
            resp = _json(a["response"])
            err = str(a["error"] or "")
            if status == "filled" or resp.get("success") is True or resp.get("status") == "matched":
                d.fills += 1
            else:
                d.rejected += 1
                if _is_fok_error(err, resp):
                    d.fok_kills += 1
                elif _is_version_mismatch(err, resp):
                    d.order_version_mismatch += 1
                else:
                    d.other_rejections += 1
            price = _f(a["price"])
            size = _f(a["size"])
            stake = _f(a["stake_usd"])
            if price is not None:
                fill_prices[sid].append(price)
            sig = _json(a["signal"])
            diag = _json(sig.get("diagnostics")) if isinstance(sig, dict) else {}
            edge = _f(diag.get("selected_edge")) if isinstance(diag, dict) else None
            if edge is not None:
                selected_edges[sid].append(edge)
            sr = _f(diag.get("seconds_remaining") or sig.get("seconds_remaining") if isinstance(sig, dict) else None)
            if sr is not None:
                seconds_remaining[sid].append(sr)
            # Resolved PnL from Gamma outcome if available.
            outcome = gamma_outcomes.get(str(a["market_slug"]))
            if (status == "filled" or resp.get("success") is True or resp.get("status") == "matched") and outcome and price is not None and size is not None:
                won = _outcome_won(str(a["outcome"]), bool(outcome["up_wins"]))
                d.resolved_fills += 1
                d.resolved_wins += int(won)
                d.gross_stake_usd += stake if stake is not None else price * size
                d.net_pnl_usd_est += (size if won else 0.0) - (price * size) - (_fee_per_share(price) * size)
            # Nearest observation visible-depth replay.
            candidates = obs_by_key.get((sid, str(a["market_slug"])), [])
            if candidates and price is not None and size is not None:
                nearest = min(candidates, key=lambda o: abs((o["ts"] - a["ts"]).total_seconds()))
                lag = (nearest["ts"] - a["ts"]).total_seconds()
                nearest_lags[sid].append(lag)
                outcome_side = str(a["outcome"] or "").upper()
                asks = _json(nearest["up_asks"] if outcome_side in {"UP", "YES"} else nearest["down_asks"])
                fillable, _depth = _best_depth_fillable(asks, limit_price=price, requested_shares=size)
                if fillable:
                    d.visible_depth_fillable_at_nearest_obs += 1
                    if status == "filled" or resp.get("success") is True or resp.get("status") == "matched":
                        d.live_book_replay_matches_order += 1
                    else:
                        d.live_book_replay_mismatches += 1
                else:
                    d.visible_depth_unfillable_at_nearest_obs += 1
                    if status != "filled" and resp.get("success") is not True and resp.get("status") != "matched":
                        d.live_book_replay_matches_order += 1
                    else:
                        d.live_book_replay_mismatches += 1

        def avg(xs: list[float]) -> float | None:
            return sum(xs) / len(xs) if xs else None

        def median(xs: list[float]) -> float | None:
            if not xs:
                return None
            s = sorted(xs)
            mid = len(s) // 2
            return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2.0

        for sid, d in by_strategy.items():
            d.avg_fill_price = avg(fill_prices[sid])
            d.avg_selected_edge = avg(selected_edges[sid])
            d.avg_seconds_remaining_at_attempt = avg(seconds_remaining[sid])
            d.median_nearest_obs_lag_s = median(nearest_lags[sid])
            if d.fok_kills:
                d.notes.append("FOK kills prove trade-print/backtest executable-price assumptions are optimistic unless arrival-book depth is modeled.")
            if d.order_version_mismatch:
                d.notes.append("order_version_mismatch requires book-version/latency logging or wider retry policy; historical trade prints cannot model it.")
            if d.resolved_fills == 0:
                d.notes.append("No resolved fills could be priced from Gamma yet; rerun after markets resolve or backfill final_price in observations.")
            report["diagnostics"][sid] = asdict(d)

        report["global_findings"] = [
            "Current historical V2 artifact is not deployment-grade: its own report rejects the expanded candidate and the trade-print+fee backtest is negative.",
            "The live-vs-backtest failure mode is primarily execution/parity: live buys ask-book/FOK with $1 fixed-stake orders, while historical LightGBM reports used repeated samples and/or last trade + 2c price proxies.",
            "A valid V2 gate must replay one decision/order per market with exact stake sizing, fees, CLOB ask depth at order arrival, latency, FOK kills, order_version_mismatch, and min-notional constraints.",
            "Persisted market_observations are sufficient for approximate visible-depth replay, but not exact exchange replay because they lack book fetch timestamp, sequence/version, order_attempt observation_id, submit/ack latency, and raw model request/response.",
        ]
        return report
    finally:
        await con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--postgres-url", default=os.environ.get("POSTGRES_URL") or os.environ.get("NAUTILUS_DB_URL"))
    ap.add_argument("--out", type=Path, default=Path("reports/lightgbm_live_parity_diagnostics.json"))
    ap.add_argument("--strategies", nargs="*", default=list(DEFAULT_STRATEGIES))
    args = ap.parse_args()
    if not args.postgres_url:
        raise SystemExit("POSTGRES_URL or NAUTILUS_DB_URL is required")
    report = asyncio.run(build_report(args.postgres_url, tuple(args.strategies)))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps({
        "out": str(args.out),
        "generated_at": report["generated_at"],
        "strategies": {sid: {
            "attempts": report["diagnostics"][sid]["attempts"],
            "fills": report["diagnostics"][sid]["fills"],
            "fok_kills": report["diagnostics"][sid]["fok_kills"],
            "order_version_mismatch": report["diagnostics"][sid]["order_version_mismatch"],
            "visible_depth_replay_mismatches": report["diagnostics"][sid]["live_book_replay_mismatches"],
        } for sid in report["diagnostics"]},
    }, indent=2))


if __name__ == "__main__":
    main()
