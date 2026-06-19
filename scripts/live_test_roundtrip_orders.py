#!/usr/bin/env python3
"""Place tiny live round-trip test orders for arb sniper strategies.

Buys ~$1 of YES and ~$1 of NO on a selected pair, then immediately sells any
filled shares. Records strategy_logs, order_attempts, and fills for dashboard
visibility. Intended for explicit operator-requested smoke tests only.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.live.arb_sniper import Book, resolve_event_pairs, rest_books_full
from polybot.persistence.writer import PolybotWriter


@dataclass
class LegPlan:
    leg: str
    token: str
    label: str
    buy_px: Decimal
    sell_px: Decimal
    buy_size: Decimal
    buy_stake: Decimal
    tick_size: str
    neg_risk: bool


def D(x: Any) -> Decimal:
    return Decimal(str(x))


def tick_dec(tick: str) -> Decimal:
    try:
        t = Decimal(str(tick or "0.01"))
    except Exception:
        t = Decimal("0.01")
    return t if t in {Decimal("0.1"), Decimal("0.01"), Decimal("0.001"), Decimal("0.0001")} else Decimal("0.01")


def round_buy(px: float, tick: str) -> Decimal:
    return min(Decimal("0.999"), D(px).quantize(tick_dec(tick), rounding=ROUND_UP))


def round_sell(px: float, tick: str) -> Decimal:
    return max(Decimal("0.001"), D(px).quantize(tick_dec(tick), rounding=ROUND_DOWN))


def top_depth(levels: list[dict[str, float]] | None, px: float, side: str) -> float:
    if not levels:
        return 0.0
    vals = []
    for x in levels:
        p = float(x.get("price") or 0)
        s = float(x.get("size") or 0)
        if side == "ask" and p <= px + 1e-12:
            vals.append(s)
        if side == "bid" and p >= px - 1e-12:
            vals.append(s)
    return sum(vals)


def resp_success(resp: dict[str, Any]) -> bool:
    return bool(resp and resp.get("success") and str(resp.get("status", "")).lower() in {"matched", "", "success"})


def bought_shares(resp: dict[str, Any], fallback_size: Decimal) -> Decimal:
    # For BUY responses, CLOB returns takingAmount as shares and makingAmount as pUSD.
    for k in ("takingAmount", "size", "matchedAmount"):
        v = resp.get(k)
        if v is not None:
            try:
                q = D(v)
                if q > 0:
                    return q.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
            except Exception:
                pass
    return fallback_size.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def stake_from_resp(resp: dict[str, Any], fallback: Decimal) -> Decimal:
    for k in ("makingAmount", "amount", "matchedAmount"):
        v = resp.get(k)
        if v is not None:
            try:
                q = D(v)
                if q > 0:
                    return q
            except Exception:
                pass
    return fallback


def choose_pair(pairs: list[dict[str, Any]], snapshots: dict[str, Book], stake: Decimal) -> tuple[dict[str, Any], dict[str, LegPlan]]:
    candidates = []
    for pair in pairs:
        tick = str(pair.get("tick_size") or "0.01")
        neg = bool(pair.get("neg_risk") or False)
        y = snapshots.get(str(pair["yes_token"]))
        n = snapshots.get(str(pair["no_token"]))
        if not y or not n or not (0 < y.ask < 1 and 0 < n.ask < 1 and 0 < y.bid < 1 and 0 < n.bid < 1):
            continue
        legs = {}
        ok = True
        for leg, token, book, label in [
            ("YES", str(pair["yes_token"]), y, str(pair.get("yes_label") or "YES")),
            ("NO", str(pair["no_token"]), n, str(pair.get("no_label") or "NO")),
        ]:
            buy_px = round_buy(book.ask, tick)
            sell_px = round_sell(book.bid, tick)
            size = (stake / buy_px).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
            if size <= 0:
                ok = False
                break
            # Need visible depth for buying and immediate unwind.
            if top_depth(book.asks, float(buy_px), "ask") + 1e-9 < float(size):
                ok = False
                break
            if top_depth(book.bids, float(sell_px), "bid") + 1e-9 < float(size):
                ok = False
                break
            legs[leg] = LegPlan(leg, token, label, buy_px, sell_px, size, stake, tick, neg)
        if ok:
            spread_loss = float((legs["YES"].buy_px - legs["YES"].sell_px) * legs["YES"].buy_size + (legs["NO"].buy_px - legs["NO"].sell_px) * legs["NO"].buy_size)
            sum_ask = float(y.ask + n.ask)
            candidates.append((spread_loss, abs(sum_ask - 1.0), pair, legs))
    if not candidates:
        raise RuntimeError("no pair has enough visible top-depth to buy ~$1 YES + ~$1 NO and sell both immediately")
    candidates.sort(key=lambda x: (x[0], x[1]))
    return candidates[0][2], candidates[0][3]


async def record_attempt(writer: PolybotWriter, strategy_id: str, pair: dict[str, Any], lp: LegPlan, side: str, px: Decimal, size: Decimal, status: str, resp: dict[str, Any], error: str | None, tag: str) -> None:
    await writer.record_order_attempt(
        strategy_id,
        str(pair.get("slug") or ""),
        lp.token,
        lp.leg,
        side,
        "TEST_ROUNDTRIP_FOK",
        float(px),
        float(size),
        float((px * size).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)),
        status,
        response=resp,
        error=error,
        signal={"manual_test": True, "tag": tag, "market_title": pair.get("title")},
        config={"stake_per_side_usd": 1.0, "sell_immediately": True, "tick_size": lp.tick_size, "neg_risk": lp.neg_risk},
    )


async def run_one(kind: str, strategy_id: str, slug: str, all_markets: bool, exec_client: PolymarketExecutionClient, writer: PolybotWriter, stake: Decimal) -> dict[str, Any]:
    pairs = await resolve_event_pairs(slug, all_markets=all_markets)
    asset_ids: list[str] = []
    for p in pairs:
        asset_ids.extend([str(p["yes_token"]), str(p["no_token"])])
    import httpx
    async with httpx.AsyncClient(timeout=5) as c:
        snapshots = await rest_books_full(c, asset_ids)
    pair, legs = choose_pair(pairs, snapshots, stake)
    title = str(pair.get("title") or pair.get("slug") or "")
    tag = f"manual_roundtrip_{kind}_{int(time.time())}"
    await writer.log_strategy_event(strategy_id, f"MANUAL TEST {tag}: starting $1/side BUY then immediate SELL on {pair.get('slug')} · {title}", level="INFO")

    results: dict[str, Any] = {"kind": kind, "strategy_id": strategy_id, "market_slug": pair.get("slug"), "title": title, "tag": tag, "legs": {}}

    # Batch buy both legs to mimic the arb sniper two-leg submit path.
    buy_orders = []
    for leg in ("YES", "NO"):
        lp = legs[leg]
        buy_orders.append(PolyOrder(
            token_id=lp.token,
            side="BUY",
            price=lp.buy_px,
            size=lp.buy_size,
            order_type="FOK",
            post_only=False,
            use_limit_order=False,  # market-buy amount path; lp.buy_size chosen so amount ~= $1
            tick_size=lp.tick_size,
            neg_risk=lp.neg_risk,
        ))
    buy_resps = exec_client.submit_batch(buy_orders)
    if not isinstance(buy_resps, list):
        buy_resps = [buy_resps]

    fill_seq = int(time.time() * 1000) % 1_000_000_000
    filled: dict[str, Decimal] = {}
    total_buy = Decimal("0")
    total_sell = Decimal("0")
    for idx, leg in enumerate(("YES", "NO")):
        lp = legs[leg]
        resp = buy_resps[idx] if idx < len(buy_resps) and isinstance(buy_resps[idx], dict) else {"raw": buy_resps[idx] if idx < len(buy_resps) else None}
        ok = resp_success(resp)
        err = None if ok else str(resp.get("error") or resp.get("errorMsg") or resp)
        status = "filled" if ok else "rejected"
        await record_attempt(writer, strategy_id, pair, lp, "BUY", lp.buy_px, lp.buy_size, status, resp, err, tag)
        leg_result = {"buy_status": status, "buy_price": str(lp.buy_px), "buy_size_requested": str(lp.buy_size), "buy_response": resp}
        if ok:
            shares = bought_shares(resp, lp.buy_size)
            spent = stake_from_resp(resp, lp.buy_stake)
            filled[leg] = shares
            total_buy += spent
            fill_seq += 1
            await writer.record_fill(strategy_id, fill_seq, f"{title[:40]} [MANUAL_TEST] {leg}", "BUY", float(lp.buy_px), float(shares), kind="MANUAL_TEST_BUY")
            leg_result.update({"bought_shares": str(shares), "spent_est": str(spent)})
        results["legs"][leg] = leg_result

    # Immediately sell every leg that filled.
    for leg in ("YES", "NO"):
        if leg not in filled:
            continue
        lp = legs[leg]
        shares = filled[leg]
        sell_order = PolyOrder(
            token_id=lp.token,
            side="SELL",
            price=lp.sell_px,
            size=shares,
            order_type="FOK",
            post_only=False,
            use_limit_order=True,
            tick_size=lp.tick_size,
            neg_risk=lp.neg_risk,
        )
        try:
            sell_resp = exec_client.submit(sell_order)
        except Exception as e:
            sell_resp = {"success": False, "error": repr(e)}
        ok = resp_success(sell_resp)
        err = None if ok else str(sell_resp.get("error") or sell_resp.get("errorMsg") or sell_resp)
        status = "filled" if ok else "rejected"
        await record_attempt(writer, strategy_id, pair, lp, "SELL", lp.sell_px, shares, status, sell_resp, err, tag)
        proceeds = stake_from_resp({"makingAmount": None, "amount": None}, lp.sell_px * shares)
        # For SELL, takingAmount is pUSD received in typical CLOB response.
        if ok and sell_resp.get("takingAmount") is not None:
            try:
                proceeds = D(sell_resp["takingAmount"])
            except Exception:
                pass
        if ok:
            total_sell += proceeds
            fill_seq += 1
            await writer.record_fill(strategy_id, fill_seq, f"{title[:40]} [MANUAL_TEST] {leg}", "SELL", float(lp.sell_px), float(shares), kind="MANUAL_TEST_SELL")
        results["legs"][leg].update({"sell_status": status, "sell_price": str(lp.sell_px), "sell_size": str(shares), "sell_response": sell_resp, "proceeds_est": str(proceeds)})

    pnl = total_sell - total_buy
    results["spent_est"] = str(total_buy)
    results["proceeds_est"] = str(total_sell)
    results["pnl_est"] = str(pnl)
    residual = {leg: str(shares) for leg, shares in filled.items() if results["legs"].get(leg, {}).get("sell_status") != "filled"}
    results["residual"] = residual
    level = "INFO" if not residual else "ERROR"
    await writer.log_strategy_event(
        strategy_id,
        f"MANUAL TEST {tag}: completed roundtrip on {pair.get('slug')} spent≈${total_buy:.4f} proceeds≈${total_sell:.4f} pnl≈${pnl:.4f} residual={residual or 'none'}",
        level=level,
    )
    return results


async def main() -> None:
    for f in [".env", ".env.live", "/home/administrator/projects/polybot-dash/.env.local"]:
        if Path(f).exists():
            load_dotenv(f, override=False)
    writer = PolybotWriter(os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL"))
    await writer.connect()
    exec_client = PolymarketExecutionClient()
    # Warm credentials/client outside timing-critical section.
    _ = exec_client.http.clob
    try:
        out = []
        out.append(await run_one(
            "weather",
            "live_weather_arb_sniper_austin_20260429_v1",
            "highest-temperature-in-austin-on-april-29-2026",
            True,
            exec_client,
            writer,
            Decimal("1.00"),
        ))
        out.append(await run_one(
            "btc15m",
            "live_arb_sniper_btc15m_v1",
            "btc-updown-15m-auto",
            False,
            exec_client,
            writer,
            Decimal("1.00"),
        ))
        print(json.dumps(out, indent=2, default=str))
    finally:
        await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
