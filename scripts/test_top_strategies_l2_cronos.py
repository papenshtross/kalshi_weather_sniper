#!/usr/bin/env python3
"""Test existing top BTC 5m strategy candidates against public L2 books.

Uses CronosVirus00/polymarket-BTC5min-database Parquet files as a small true-L2
sample and the existing strict_polymarket_strategy_search_30d candidate list.
Does not execute any third-party repo code.
"""
from __future__ import annotations

import argparse
import bisect
import json
import sys
import time
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.search_strict_polymarket_btc5m_strategy import (  # noqa: E402
    Candidate,
    load_market_inputs,
    replay_candidate,
    fee_per_share,
)


@dataclass
class Book:
    ts: float
    asks: list[tuple[float, float]]
    bids: list[tuple[float, float]]


def candidate_from_dict(d: dict[str, Any]) -> Candidate:
    return Candidate(
        name=d["name"],
        windows=tuple(int(x) for x in d["windows"]),
        min_consensus=int(d["min_consensus"]),
        entry_rule=d["entry_rule"],
        entry_seconds_before_close=d.get("entry_seconds_before_close"),
        ask_buffer=float(d["ask_buffer"]),
        max_trade_age=int(d["max_trade_age"]),
        max_ask=float(d["max_ask"]),
        hedge_buffer=d.get("hedge_buffer"),
        hedge_trigger_consensus=d.get("hedge_trigger_consensus"),
        fee_rate=float(d.get("fee_rate", 0.072)),
        stake_usd=float(d.get("stake_usd", 1.0)),
    )


def fetch_token_maps(slugs: list[str], cache_path: Path) -> dict[str, dict[str, str]]:
    if cache_path.exists():
        cached = json.loads(cache_path.read_text())
    else:
        cached = {}
    changed = False
    for slug in slugs:
        if slug in cached:
            continue
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 Hermes L2 tester"})
        try:
            data = json.load(urllib.request.urlopen(req, timeout=20))
            if not data or not data[0].get("markets"):
                cached[slug] = {}
            else:
                m = data[0]["markets"][0]
                outcomes = json.loads(m["outcomes"]) if isinstance(m.get("outcomes"), str) else m.get("outcomes", [])
                toks = json.loads(m["clobTokenIds"]) if isinstance(m.get("clobTokenIds"), str) else m.get("clobTokenIds", [])
                cached[slug] = {str(tok): str(outcome).lower() for outcome, tok in zip(outcomes, toks)}
            changed = True
            time.sleep(0.08)
        except Exception as e:
            cached[slug] = {"_error": repr(e)}
            changed = True
    if changed:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(cached, indent=2, sort_keys=True))
    return cached


def load_books(data_dir: Path, token_maps: dict[str, dict[str, str]]) -> tuple[dict[tuple[str, str], list[Book]], dict[str, Any]]:
    books: dict[tuple[str, str], list[Book]] = {}
    stats = Counter()
    unknown_assets = Counter()
    for f in sorted(data_dir.glob("*.parquet")):
        table = pq.read_table(f, columns=["timestamp", "slug", "asset_id", "bids", "asks"])
        d = table.to_pydict()
        for ts, slug, aid, bids_s, asks_s in zip(d["timestamp"], d["slug"], d["asset_id"], d["bids"], d["asks"]):
            side = token_maps.get(slug, {}).get(str(aid))
            if side not in {"up", "down"}:
                unknown_assets[(slug, str(aid))] += 1
                stats["unknown_asset_rows"] += 1
                continue
            try:
                asks_raw = json.loads(asks_s) if isinstance(asks_s, str) else asks_s
                bids_raw = json.loads(bids_s) if isinstance(bids_s, str) else bids_s
                asks = sorted((float(x["price"]), float(x["size"])) for x in asks_raw if float(x.get("size", 0)) > 0)
                bids = sorted(((float(x["price"]), float(x["size"])) for x in bids_raw if float(x.get("size", 0)) > 0), reverse=True)
            except Exception:
                stats["bad_json_rows"] += 1
                continue
            books.setdefault((slug, side), []).append(Book(float(ts), asks, bids))
            stats["book_rows"] += 1
    for k in list(books):
        books[k].sort(key=lambda b: b.ts)
    meta = {
        "book_rows": stats["book_rows"],
        "unknown_asset_rows": stats["unknown_asset_rows"],
        "bad_json_rows": stats["bad_json_rows"],
        "book_keys": len(books),
        "unknown_asset_examples": [list(k) + [v] for k, v in unknown_assets.most_common(10)],
    }
    return books, meta


def latest_book(books: dict[tuple[str, str], list[Book]], slug: str, side: str, ts: float) -> Book | None:
    arr = books.get((slug, side))
    if not arr:
        return None
    times = [b.ts for b in arr]
    i = bisect.bisect_right(times, ts) - 1
    if i < 0:
        return None
    return arr[i]


def fill_from_asks(book: Book, limit_price: float, shares_needed: float) -> dict[str, Any]:
    rem = shares_needed
    cost = 0.0
    levels_used = 0
    best_ask = book.asks[0][0] if book.asks else None
    depth_at_limit = 0.0
    for price, size in book.asks:
        if price <= limit_price + 1e-12:
            depth_at_limit += size
    for price, size in book.asks:
        if price > limit_price + 1e-12:
            break
        take = min(rem, size)
        if take > 0:
            cost += take * price
            rem -= take
            levels_used += 1
        if rem <= 1e-12:
            break
    filled = rem <= 1e-12
    avg_price = cost / shares_needed if filled and shares_needed > 0 else None
    return {
        "filled": filled,
        "best_ask": best_ask,
        "depth_at_limit": depth_at_limit,
        "shares_needed": shares_needed,
        "avg_price": avg_price,
        "levels_used": levels_used,
    }


def pnl_unhedged(side: str, winner: str, avg_price: float, shares: float, fee_rate: float) -> tuple[float, float]:
    fee = shares * fee_per_share(avg_price, fee_rate)
    payout = shares if side == winner else 0.0
    return payout - shares * avg_price - fee, fee


def summarize(rows: list[dict[str, Any]], total_markets: int) -> dict[str, Any]:
    executed = [r for r in rows if r["proxy_executed"]]
    entry_filled = [r for r in executed if r.get("entry_l2_filled")]
    strict_filled = [r for r in executed if r.get("strict_l2_filled")]
    pnls = [r["l2_pnl"] for r in strict_filled if r.get("l2_pnl") is not None]
    ages = [r["entry_book_age_s"] for r in entry_filled if r.get("entry_book_age_s") is not None]
    skip = Counter(r.get("l2_reason") for r in executed if not r.get("strict_l2_filled"))
    return {
        "markets_total": total_markets,
        "proxy_executed": len(executed),
        "entry_l2_filled": len(entry_filled),
        "strict_l2_filled": len(strict_filled),
        "entry_fill_rate_vs_proxy": round(len(entry_filled) / len(executed), 6) if executed else 0.0,
        "strict_fill_rate_vs_proxy": round(len(strict_filled) / len(executed), 6) if executed else 0.0,
        "l2_total_pnl": round(sum(pnls), 6) if pnls else 0.0,
        "l2_avg_pnl": round(mean(pnls), 6) if pnls else 0.0,
        "l2_win_rate": round(sum(1 for p in pnls if p > 0) / len(pnls), 6) if pnls else 0.0,
        "median_entry_book_age_s": round(median(ages), 3) if ages else None,
        "p95_entry_book_age_s": round(sorted(ages)[int(0.95 * (len(ages)-1))], 3) if len(ages) > 1 else (round(ages[0], 3) if ages else None),
        "l2_fail_reasons": dict(skip),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cronos-dir", type=Path, default=Path("/home/administrator/poly_search/repos/polymarket-BTC5min-database/market_data"))
    ap.add_argument("--search-json", type=Path, default=ROOT / "data/backtests/strict_polymarket_strategy_search_30d.json")
    ap.add_argument("--market-cache", type=Path, default=ROOT / "data/backtests/cache/btc_5m_market_inputs_30d.json")
    ap.add_argument("--token-cache", type=Path, default=ROOT / "data/backtests/cache/cronos_btc5m_token_maps.json")
    ap.add_argument("--output", type=Path, default=ROOT / "data/backtests/l2_cronos_top_strategies_report.json")
    ap.add_argument("--top", type=int, default=44)
    args = ap.parse_args()

    slugs = sorted({p.name.split("_")[0] for p in args.cronos_dir.glob("*.parquet")})
    token_maps = fetch_token_maps(slugs, args.token_cache)
    books, book_meta = load_books(args.cronos_dir, token_maps)

    _, all_markets = load_market_inputs(args.market_cache)
    markets = [m for m in all_markets if m.market_slug in slugs]
    market_by_slug = {m.market_slug: m for m in markets}

    search = json.loads(args.search_json.read_text())
    top = search.get("top_ranked", [])[: args.top]
    candidates = [candidate_from_dict(x["candidate"]) for x in top]

    reports = []
    details_by_candidate = {}
    for c in candidates:
        rows = []
        for m in markets:
            r = replay_candidate(m, c, "chainlink")
            row = {
                "market_slug": m.market_slug,
                "candidate": c.name,
                "proxy_executed": bool(r.get("executed")),
                "proxy_reason": r.get("reason"),
            }
            if not r.get("executed"):
                rows.append(row); continue
            side = str(r["side"]).lower()
            winner = str(r["winner"]).lower()
            entry_ts = float(r["entry_ts"])
            entry_limit = float(r["entry_ask"])
            shares = float(r["shares"])
            b = latest_book(books, m.market_slug, side, entry_ts)
            row.update({
                "side": side,
                "winner": winner,
                "entry_ts": entry_ts,
                "entry_limit": entry_limit,
                "proxy_pnl": r.get("pnl"),
                "hedged_by_proxy": bool(r.get("hedged")),
            })
            if b is None:
                row.update({"entry_l2_filled": False, "strict_l2_filled": False, "l2_reason": "no_book_for_side_before_entry"})
                rows.append(row); continue
            fill = fill_from_asks(b, entry_limit, shares)
            row.update({
                "entry_book_ts": b.ts,
                "entry_book_age_s": round(entry_ts - b.ts, 6),
                "entry_best_ask": fill["best_ask"],
                "entry_depth_at_limit": round(fill["depth_at_limit"], 8),
                "entry_shares_needed": round(shares, 8),
                "entry_l2_filled": fill["filled"],
                "entry_avg_price": fill["avg_price"],
            })
            if not fill["filled"]:
                row.update({"strict_l2_filled": False, "l2_reason": "insufficient_entry_depth_at_limit"})
                rows.append(row); continue
            entry_avg = float(fill["avg_price"])
            if r.get("hedged") and r.get("hedge_ts") is not None and r.get("hedge_ask") is not None:
                opp = "down" if side == "up" else "up"
                hb = latest_book(books, m.market_slug, opp, float(r["hedge_ts"]))
                if hb is None:
                    # Entry filled, but proxy hedge could not be verified/fillable from captured opposite book.
                    pnl, fee = pnl_unhedged(side, winner, entry_avg, shares, c.fee_rate)
                    row.update({"strict_l2_filled": False, "l2_reason": "entry_filled_but_no_hedge_book", "l2_pnl_unhedged_if_no_hedge": pnl})
                    rows.append(row); continue
                hfill = fill_from_asks(hb, float(r["hedge_ask"]), shares)
                row.update({
                    "hedge_side": opp,
                    "hedge_ts": r["hedge_ts"],
                    "hedge_limit": r["hedge_ask"],
                    "hedge_book_ts": hb.ts,
                    "hedge_book_age_s": round(float(r["hedge_ts"]) - hb.ts, 6),
                    "hedge_best_ask": hfill["best_ask"],
                    "hedge_depth_at_limit": round(hfill["depth_at_limit"], 8),
                    "hedge_l2_filled": hfill["filled"],
                    "hedge_avg_price": hfill["avg_price"],
                })
                if not hfill["filled"]:
                    pnl, fee = pnl_unhedged(side, winner, entry_avg, shares, c.fee_rate)
                    row.update({"strict_l2_filled": False, "l2_reason": "entry_filled_but_insufficient_hedge_depth", "l2_pnl_unhedged_if_no_hedge": pnl})
                    rows.append(row); continue
                hedge_avg = float(hfill["avg_price"])
                entry_fee = shares * fee_per_share(entry_avg, c.fee_rate)
                hedge_fee = shares * fee_per_share(hedge_avg, c.fee_rate)
                pnl = shares * (1.0 - entry_avg - hedge_avg) - entry_fee - hedge_fee
                row.update({"strict_l2_filled": True, "l2_reason": "filled_entry_and_hedge", "l2_pnl": pnl, "entry_fee": entry_fee, "hedge_fee": hedge_fee})
            else:
                pnl, fee = pnl_unhedged(side, winner, entry_avg, shares, c.fee_rate)
                row.update({"strict_l2_filled": True, "l2_reason": "filled_entry", "l2_pnl": pnl, "entry_fee": fee})
            rows.append(row)
        summ = summarize(rows, len(markets))
        baseline = next((x for x in top if x["candidate"]["name"] == c.name), None)
        reports.append({
            "candidate": c.to_dict(),
            "original_30d_chainlink": baseline.get("chainlink_reference") if baseline else None,
            "cronos_l2_sample": summ,
        })
        details_by_candidate[c.name] = rows

    reports.sort(key=lambda x: (x["cronos_l2_sample"]["l2_total_pnl"], x["cronos_l2_sample"]["strict_l2_filled"]), reverse=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_source": {
            "repo": "https://github.com/CronosVirus00/polymarket-BTC5min-database",
            "dir": str(args.cronos_dir),
            "markets_in_sample": len(markets),
            "sample_start_ts": min((m.start_ts for m in markets), default=None),
            "sample_end_ts": max((m.end_ts for m in markets), default=None),
            "book_meta": book_meta,
            "note": "True L2 bids/asks from GitHub-hosted Parquet, but narrow BTC 5m sample; many markets have only one side captured.",
        },
        "method": {
            "candidates_tested": len(candidates),
            "strategy_source": str(args.search_json),
            "entry_fill_rule": "$1/share limit buy simulated against latest captured ask book at or before strategy entry_ts; fill requires enough ask depth at <= strategy limit price.",
            "hedge_fill_rule": "For proxy-hedged strategies, strict L2 pass requires both entry and hedge book fills for the same share size.",
            "third_party_code_execution": "none; only Parquet data read with local PyArrow and locally-authored tester code.",
        },
        "ranked_results": reports,
        "details_by_candidate": details_by_candidate,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print(json.dumps({
        "output": str(args.output),
        "markets": len(markets),
        "book_meta": book_meta,
        "top_10": [
            {
                "name": r["candidate"]["name"],
                **r["cronos_l2_sample"],
            }
            for r in reports[:10]
        ],
    }, indent=2))


if __name__ == "__main__":
    main()
