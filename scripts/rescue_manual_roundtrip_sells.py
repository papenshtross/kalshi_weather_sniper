#!/usr/bin/env python3
from __future__ import annotations
import asyncio, os, json, time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any
from dotenv import load_dotenv
from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.live.arb_sniper import rest_books_full
from polybot.persistence.writer import PolybotWriter

POSITIONS = [
    {
        "strategy_id":"live_weather_arb_sniper_austin_20260429_v1",
        "market_slug":"highest-temperature-in-austin-on-april-29-2026-82-83f",
        "title":"Will the highest temperature in Austin be between 82-83°F on April 29?",
        "leg":"YES",
        "token":"42724294385397295252976648821995165156903192090214173713687709370683725766470",
        "tick":"0.01",
        "neg_risk":True,
        "tag":"manual_roundtrip_weather_1777395660",
        "bought_shares":"3.3333",
    },
    {
        "strategy_id":"live_weather_arb_sniper_austin_20260429_v1",
        "market_slug":"highest-temperature-in-austin-on-april-29-2026-82-83f",
        "title":"Will the highest temperature in Austin be between 82-83°F on April 29?",
        "leg":"NO",
        "token":"45113131969732879589607964373796693337280031149001525970238557166644492249404",
        "tick":"0.01",
        "neg_risk":True,
        "tag":"manual_roundtrip_weather_1777395660",
        "bought_shares":"1.3888",
    },
    {
        "strategy_id":"live_arb_sniper_btc15m_v1",
        "market_slug":"btc-updown-15m-1777395600",
        "title":"Bitcoin Up or Down - April 28, 1:00PM-1:15PM ET",
        "leg":"NO",
        "token":"70882036078128554418013692156277760913823227728892214777048757053342501996661",
        "tick":"0.01",
        "neg_risk":False,
        "tag":"manual_roundtrip_btc15m_1777395663",
        "bought_shares":"3.1250",
    },
]
BTC_BAD_YES_ORDER_ID = "0xb9c784eb5087c924cf1bfc7fafa838b2915240592822b8dd7d9843f9bf5e24d0"


def D(x): return Decimal(str(x))

def q4(x: Decimal): return x.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

async def record_attempt(writer, p, side, px, size, status, resp, err):
    await writer.record_order_attempt(
        p["strategy_id"], p["market_slug"], p["token"], p["leg"], side, "TEST_ROUNDTRIP_FOK_RETRY",
        float(px), float(size), float((px*size).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)), status,
        response=resp, error=err,
        signal={"manual_test": True, "tag": p["tag"], "retry_sell_after_balance_settled": True, "market_title": p["title"]},
        config={"sell_immediately": True, "retry_after_initial_sell_reject": True, "tick_size": p["tick"], "neg_risk": p["neg_risk"]},
    )

async def main():
    for f in [".env", ".env.live", "/home/administrator/projects/polybot-dash/.env.local"]:
        if Path(f).exists(): load_dotenv(f, override=False)
    writer=PolybotWriter(os.getenv("NAUTILUS_DB_URL") or os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL"))
    await writer.connect()
    ex=PolymarketExecutionClient(); clob=ex.http.clob
    import httpx
    async with httpx.AsyncClient(timeout=5) as c:
        books=await rest_books_full(c,[p["token"] for p in POSITIONS])
    results=[]
    # Correct the one batch response that had success=true but errorMsg/FOK-killed/no status/no amounts.
    async with writer._pool.acquire() as con:  # type: ignore[attr-defined]
        await con.execute("""
            UPDATE order_attempts
            SET status='rejected', error='Corrected manual test: CLOB batch response had success=true but errorMsg said FOK killed and no matched amounts/status.'
            WHERE strategy_id='live_arb_sniper_btc15m_v1' AND response->>'orderID'=$1
        """, BTC_BAD_YES_ORDER_ID)
        await con.execute("""
            DELETE FROM fills
            WHERE strategy_id='live_arb_sniper_btc15m_v1'
              AND kind='MANUAL_TEST_BUY'
              AND side='BUY'
              AND market LIKE '%[MANUAL_TEST] YES'
              AND ts > now() - interval '30 minutes'
        """)
    await writer.log_strategy_event('live_arb_sniper_btc15m_v1', f"MANUAL TEST manual_roundtrip_btc15m_1777395663: corrected YES leg as rejected/FOK-killed; no YES fill existed, removed erroneous dashboard fill.", level='WARNING')

    for p in POSITIONS:
        bal = clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=p["token"]))
        balance_shares = D(bal.get('balance','0')) / Decimal(10) ** 6
        target = min(q4(D(p["bought_shares"])), q4(balance_shares))
        book=books.get(p["token"])
        if target <= 0 or not book or not book.bid:
            msg=f"MANUAL TEST {p['tag']}: cannot retry sell {p['leg']} token balance={balance_shares} book_bid={getattr(book,'bid',None)}"
            await writer.log_strategy_event(p['strategy_id'], msg, level='ERROR')
            results.append({**p,"balance":str(balance_shares),"status":"no_balance_or_book"})
            continue
        px = D(book.bid).quantize(D(p["tick"]), rounding=ROUND_DOWN)
        order=PolyOrder(token_id=p["token"], side="SELL", price=px, size=target, order_type="FOK", post_only=False, use_limit_order=True, tick_size=p["tick"], neg_risk=p["neg_risk"])
        try:
            resp=ex.submit(order)
        except Exception as e:
            resp={"success":False,"error":repr(e)}
        ok=bool(resp.get('success') and str(resp.get('status','')).lower() in {'matched','success'})
        status='filled' if ok else 'rejected'
        err=None if ok else str(resp.get('error') or resp.get('errorMsg') or resp)
        await record_attempt(writer,p,'SELL',px,target,status,resp,err)
        if ok:
            fill_id=int(time.time()*1000)%1_000_000_000
            await writer.record_fill(p['strategy_id'], fill_id, f"{p['title'][:40]} [MANUAL_TEST_RETRY] {p['leg']}", 'SELL', float(px), float(target), kind='MANUAL_TEST_SELL_RETRY')
        await writer.log_strategy_event(p['strategy_id'], f"MANUAL TEST {p['tag']}: retry SELL {p['leg']} {status} size={target} px={px} response={json.dumps(resp)[:500]}", level='INFO' if ok else 'ERROR')
        results.append({**p,"balance":str(balance_shares),"sell_px":str(px),"sell_size":str(target),"status":status,"response":resp})
    print(json.dumps(results, indent=2, default=str))
    await writer.close()

if __name__=='__main__': asyncio.run(main())
