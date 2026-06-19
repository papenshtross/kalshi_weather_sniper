#!/usr/bin/env python3
from __future__ import annotations

import argparse, asyncio, json, os, time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient

DEFAULT_STRATEGIES = [
    "live_like_more_v35_tp15_stop35_d0_day3_city1_bid0",
    "live_like_more_v21_tp20_stop35_d0_day3_city1_bid0",
    "live_like_more_v21_tp20_stop35_d0_day3_city2_bid0",
]


def D(x: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal(default)


def q4(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def parse_jsonish(v, default):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return default
    return v if v is not None else default


def order_id(resp: dict | None) -> str | None:
    if not isinstance(resp, dict):
        return None
    for k in ("orderID", "order_id", "id"):
        if resp.get(k):
            return str(resp[k])
    nested = resp.get("order") if isinstance(resp.get("order"), dict) else None
    return order_id(nested) if nested else None


def status_from(resp: dict | None) -> str:
    if not isinstance(resp, dict):
        return "unknown"
    raw = str(resp.get("status") or "").lower()
    if raw in {"matched", "filled"}:
        return "filled"
    if raw in {"delayed", "pending", "live"}:
        return "submitted"
    if resp.get("success") is True and order_id(resp):
        return "submitted"
    if resp.get("success") is False:
        return "rejected"
    return raw or "submitted"


def matched_size_from_order(order: dict | None) -> Decimal:
    if not isinstance(order, dict):
        return Decimal("0")
    for k in ("size_matched", "sizeMatched", "matched_size", "matchedSize"):
        if order.get(k) not in (None, ""):
            return D(order.get(k))
    return Decimal("0")


def level_lists(book: dict):
    bids=[]; asks=[]
    for x in book.get("bids", []) or []:
        px=D(x.get("price")); sz=D(x.get("size"))
        if px > 0 and sz > 0: bids.append((px, sz))
    for x in book.get("asks", []) or []:
        px=D(x.get("price")); sz=D(x.get("size"))
        if px > 0 and sz > 0: asks.append((px, sz))
    return bids, asks


def choose_candidate(stake: Decimal) -> dict:
    markets = requests.get(
        "https://gamma-api.polymarket.com/markets",
        params={"active": "true", "closed": "false", "limit": 300, "order": "volume24hr", "ascending": "false"},
        timeout=25,
    ).json()
    req=[]; meta={}
    for m in markets:
        if not m.get("acceptingOrders", True):
            continue
        toks=parse_jsonish(m.get("clobTokenIds"), [])
        outs=parse_jsonish(m.get("outcomes"), ["Yes", "No"])
        if not toks or len(toks)<2:
            continue
        for i,tok in enumerate(toks[:2]):
            tok=str(tok)
            req.append({"token_id": tok})
            meta[tok]={
                "token": tok,
                "outcome": outs[i] if i < len(outs) else ("Yes" if i==0 else "No"),
                "slug": m.get("slug"),
                "question": m.get("question"),
                "tick_size": str(m.get("orderPriceMinTickSize") or m.get("minimumTickSize") or m.get("tickSize") or "0.01"),
                "order_min_size": D(m.get("orderMinSize") or 5),
                "neg_risk": bool(m.get("negRisk") or m.get("neg_risk") or False),
            }
    candidates=[]
    for i in range(0, len(req), 100):
        r=requests.post("https://clob.polymarket.com/books", json=req[i:i+100], timeout=15)
        r.raise_for_status()
        for b in r.json() or []:
            tok=str(b.get("asset_id") or b.get("token_id") or "")
            if tok not in meta: continue
            bids,asks=level_lists(b)
            if not bids or not asks: continue
            bid=max(p for p,s in bids); ask=min(p for p,s in asks)
            best_ask_depth=sum(s for p,s in asks if p == ask)
            best_bid_depth=sum(s for p,s in bids if p == bid)
            spread=ask-bid
            shares=stake / ask if ask > 0 else Decimal("0")
            # Prefer cheap-ish, tight-spread tokens where $1 can buy >= min shares and the best bid can sell it back.
            if Decimal("0.02") <= ask <= Decimal("0.30") and spread <= Decimal("0.01") and shares >= meta[tok]["order_min_size"] and best_ask_depth >= shares and best_bid_depth >= shares:
                candidates.append((spread, ask, -min(best_ask_depth,best_bid_depth), tok, bid, ask, best_bid_depth, best_ask_depth, meta[tok]))
    if not candidates:
        raise RuntimeError("no liquid $1 roundtrip candidate found")
    spread, ask_sort, neg_depth, tok, bid, ask, bid_depth, ask_depth, m = sorted(candidates)[0]
    m.update({"bid": bid, "ask": ask, "bid_depth": bid_depth, "ask_depth": ask_depth, "spread": spread})
    return m


async def wait_order(ex: PolymarketExecutionClient, oid: str | None, tries=8) -> dict | None:
    if not oid: return None
    last=None
    for _ in range(tries):
        try:
            last=ex.get_order(oid)
            if matched_size_from_order(last) > 0 or str((last or {}).get("status") or "").lower() in {"matched","filled"}:
                return last
        except Exception as e:
            last={"error": repr(e)}
        await asyncio.sleep(1)
    return last


async def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--stake", default="1.00")
    ap.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES))
    ap.add_argument("--out", default="reports/deployment/manual_multi_strategy_roundtrip.json")
    args=ap.parse_args()
    for f in ["/home/administrator/projects/polybot/.env", "/home/administrator/projects/polybot/.env.live", "/home/administrator/projects/polybot-dash/.env.local"]:
        if Path(f).exists(): load_dotenv(f, override=True)
    import asyncpg
    dsn=os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
    conn=await asyncpg.connect(dsn)
    ex=PolymarketExecutionClient(); clob=ex.http.clob
    stake=D(args.stake, "1.00")
    strategies=[s.strip() for s in args.strategies.split(',') if s.strip()]
    candidate=choose_candidate(stake)
    tick=D(candidate["tick_size"], "0.01")
    before_collateral=clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    report={"tag": f"manual_multi_roundtrip_{int(time.time())}", "stake": str(stake), "candidate": {k: str(v) if isinstance(v, Decimal) else v for k,v in candidate.items() if k != 'neg_risk'}, "strategy_results": [], "before_collateral": before_collateral}
    for sid in strategies:
        before_token=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate["token"])) or {}).get("balance","0")) / Decimal(10)**6
        # Refresh book for this individual attempt.
        book=requests.get("https://clob.polymarket.com/book", params={"token_id": candidate["token"]}, timeout=10).json()
        bids,asks=level_lists(book)
        bid=max(p for p,s in bids); ask=min(p for p,s in asks)
        buy_px=min(Decimal("0.999"), (ask + max(tick, Decimal("0.001"))).quantize(tick, rounding=ROUND_UP))
        buy_size=q4(stake / buy_px)
        await conn.execute("INSERT INTO strategy_logs(strategy_id,level,message,ts) VALUES($1,'WARN',$2,now())", sid, f"MANUAL ROUNDTRIP TEST {report['tag']}: BUY then immediate SELL token={candidate['token'][:12]} market={candidate['slug']} outcome={candidate['outcome']} buy_px<={buy_px} approx_stake=${stake}")
        buy_resp=ex.submit(PolyOrder(token_id=candidate["token"], side="BUY", price=buy_px, size=buy_size, order_type="FAK", post_only=False, use_limit_order=False, tick_size=str(tick), neg_risk=bool(candidate["neg_risk"])))
        buy_oid=order_id(buy_resp); buy_status=status_from(buy_resp); buy_order=await wait_order(ex,buy_oid)
        bought=Decimal("0")
        for _ in range(15):
            cur=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate["token"])) or {}).get("balance","0")) / Decimal(10)**6
            bought=max(Decimal("0"), cur-before_token)
            if bought >= candidate["order_min_size"]: break
            await asyncio.sleep(1)
        buy_stake=(bought * buy_px).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
        await conn.execute("""
          INSERT INTO order_attempts(strategy_id, market_slug, token, outcome, side, order_type, price, size, stake_usd, status, response, signal, config)
          VALUES($1,$2,$3,$4,'BUY','MANUAL_TEST_FAK',$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb)
        """, sid, candidate['slug'], candidate['token'], candidate['outcome'], buy_px, bought if bought>0 else buy_size, buy_stake if bought>0 else stake, 'filled' if bought>0 else buy_status, json.dumps({'submit':buy_resp,'get_order':buy_order,'order_id':buy_oid}, default=str), json.dumps({'manual_roundtrip':True,'tag':report['tag'],'candidate':report['candidate']}, default=str), json.dumps({'source':'manual_multi_strategy_roundtrip','stake':str(stake)}, default=str))
        sell_result={"status":"skipped"}
        residual=None
        if bought > 0:
            book=requests.get("https://clob.polymarket.com/book", params={"token_id": candidate["token"]}, timeout=10).json()
            bids,asks=level_lists(book)
            bid=max(p for p,s in bids) if bids else Decimal("0")
            sell_px=max(Decimal("0.001"), bid.quantize(tick, rounding=ROUND_DOWN))
            sell_size=q4(bought)
            sell_resp=ex.submit(PolyOrder(token_id=candidate["token"], side="SELL", price=sell_px, size=sell_size, order_type="FOK", post_only=False, use_limit_order=True, tick_size=str(tick), neg_risk=bool(candidate["neg_risk"])))
            sell_oid=order_id(sell_resp); sell_status=status_from(sell_resp); sell_order=await wait_order(ex,sell_oid)
            await asyncio.sleep(2)
            after_token=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate["token"])) or {}).get("balance","0")) / Decimal(10)**6
            residual=q4(after_token-before_token)
            await conn.execute("""
              INSERT INTO order_attempts(strategy_id, market_slug, token, outcome, side, order_type, price, size, stake_usd, status, response, signal, config)
              VALUES($1,$2,$3,$4,'SELL','MANUAL_TEST_FOK',$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb)
            """, sid, candidate['slug'], candidate['token'], candidate['outcome'], sell_px, sell_size, (sell_size*sell_px).quantize(Decimal('0.0001'), rounding=ROUND_DOWN), 'filled' if residual <= Decimal('0.0001') else sell_status, json.dumps({'submit':sell_resp,'get_order':sell_order,'order_id':sell_oid}, default=str), json.dumps({'manual_roundtrip':True,'tag':report['tag'],'candidate':report['candidate']}, default=str), json.dumps({'source':'manual_multi_strategy_roundtrip','stake':str(stake)}, default=str))
            sell_result={"limit_price":str(sell_px),"size":str(sell_size),"status":'filled' if residual <= Decimal('0.0001') else sell_status,"order_id":sell_oid,"response":sell_resp,"get_order":sell_order}
        else:
            after_token=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate["token"])) or {}).get("balance","0")) / Decimal(10)**6
            residual=q4(after_token-before_token)
        level='INFO' if residual is not None and residual <= Decimal('0.0001') and sell_result.get('status')=='filled' else 'ERROR'
        await conn.execute("INSERT INTO strategy_logs(strategy_id,level,message,ts) VALUES($1,$2,$3,now())", sid, level, f"MANUAL ROUNDTRIP TEST {report['tag']} complete: buy_status={'filled' if bought>0 else buy_status} buy_order={buy_oid} sell_status={sell_result.get('status')} sell_order={sell_result.get('order_id')} residual_delta={residual}")
        report['strategy_results'].append({"strategy_id":sid,"buy":{"limit_price":str(buy_px),"requested_size":str(buy_size),"bought_shares":str(q4(bought)),"status":'filled' if bought>0 else buy_status,"order_id":buy_oid,"response":buy_resp,"get_order":buy_order},"sell":sell_result,"residual_delta_shares":str(residual)})
    try:
        open_orders=ex.open_orders()
    except Exception as e:
        open_orders={"error":repr(e)}
    report['after_collateral']=clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    report['open_orders_count']=len(open_orders) if isinstance(open_orders,list) else None
    report['open_orders_sample']=open_orders[:5] if isinstance(open_orders,list) else open_orders
    out=Path(args.out); out.parent.mkdir(parents=True, exist_ok=True); out.write_text(json.dumps(report, indent=2, default=str))
    await conn.close()
    print(json.dumps(report, indent=2, default=str))

if __name__ == '__main__':
    asyncio.run(main())
