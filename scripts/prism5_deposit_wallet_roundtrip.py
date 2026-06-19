#!/usr/bin/env python3
from __future__ import annotations
import asyncio, json, os, time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

import requests
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.security.wallet_registry import decrypt_registry

ROOT = Path('/home/administrator/projects/polybot')
REPORT_PATH = ROOT / 'reports/deployment/prism5_deposit_wallet_roundtrip_2026-06-04.json'
STAKE = Decimal('1.00')


def D(x: Any) -> Decimal: return Decimal(str(x))
def q4(x: Decimal) -> Decimal: return x.quantize(Decimal('0.0001'), rounding=ROUND_DOWN)

def parse_jsonish(v: Any, default: Any) -> Any:
    if isinstance(v, str):
        try: return json.loads(v)
        except Exception: return default
    return v if v is not None else default

def order_id(resp: dict[str, Any] | None) -> str | None:
    if not isinstance(resp, dict): return None
    for k in ('orderID','order_id','id'):
        if resp.get(k): return str(resp[k])
    nested = resp.get('order') if isinstance(resp.get('order'), dict) else None
    return order_id(nested) if nested else None

def status_from(resp: dict[str, Any] | None) -> str:
    if not isinstance(resp, dict): return 'unknown'
    raw = str(resp.get('status') or '').lower()
    if raw in {'matched','filled'}: return 'filled'
    if raw in {'delayed','pending','live'}: return 'submitted'
    if resp.get('success') is True and (order_id(resp) or raw in {'success',''}): return 'filled' if raw in {'matched','success',''} else 'submitted'
    if resp.get('success') is False: return 'rejected'
    return raw or 'submitted'

def levels(book: dict[str, Any]):
    bids=[(D(x['price']),D(x['size'])) for x in book.get('bids',[]) if D(x.get('price',0))>0 and D(x.get('size',0))>0]
    asks=[(D(x['price']),D(x['size'])) for x in book.get('asks',[]) if D(x.get('price',0))>0 and D(x.get('size',0))>0]
    return bids, asks

def choose_candidate() -> dict[str, Any]:
    markets=requests.get('https://gamma-api.polymarket.com/markets',params={'active':'true','closed':'false','limit':400,'order':'volume24hr','ascending':'false'},timeout=25).json()
    req=[]; meta={}
    for m in markets:
        if not m.get('acceptingOrders', True): continue
        toks=parse_jsonish(m.get('clobTokenIds'), [])
        outs=parse_jsonish(m.get('outcomes'), ['Yes','No'])
        if not toks or len(toks)<2: continue
        for i,tok in enumerate(toks[:2]):
            tok=str(tok); req.append({'token_id':tok})
            meta[tok]={'token':tok,'outcome':outs[i] if i < len(outs) else ('Yes' if i==0 else 'No'),'slug':m.get('slug'),'question':m.get('question'),'tick_size':str(m.get('orderPriceMinTickSize') or m.get('minimumTickSize') or m.get('tickSize') or '0.01'),'order_min_size':D(m.get('orderMinSize') or 5),'neg_risk':bool(m.get('negRisk') or m.get('neg_risk') or False)}
    cand=[]
    for i in range(0,len(req),100):
        r=requests.post('https://clob.polymarket.com/books',json=req[i:i+100],timeout=12); r.raise_for_status()
        for b in r.json() or []:
            tok=str(b.get('asset_id') or b.get('token_id') or '')
            if tok not in meta: continue
            bids,asks=levels(b)
            if not bids or not asks: continue
            bid=max(p for p,_ in bids); ask=min(p for p,_ in asks)
            ask_depth=sum(s for p,s in asks if p<=ask)
            bid_depth=sum(s for p,s in bids if p>=bid)
            shares=STAKE/ask; spread=ask-bid
            if Decimal('0.02') <= ask <= Decimal('0.20') and spread <= Decimal('0.01') and shares >= meta[tok]['order_min_size'] and ask_depth >= meta[tok]['order_min_size'] and bid_depth >= meta[tok]['order_min_size']:
                cand.append((spread, -min(ask_depth,bid_depth), ask,bid,tok,meta[tok],ask_depth,bid_depth))
    if not cand: raise RuntimeError('no cheap liquid candidate found')
    spread,_,ask,bid,tok,m,ad,bd=sorted(cand)[0]
    m.update({'ask':ask,'bid':bid,'spread':spread,'ask_depth':ad,'bid_depth':bd})
    return m

async def main():
    reg=decrypt_registry()['wallets']; w=reg['prism5']
    deposit_wallet = w.get('POLYMARKET_DEPOSIT_WALLET') or w.get('POLYMARKET_BUILDER_ADDRESS')
    old=dict(os.environ)
    os.environ['POLYMARKET_PRIVATE_KEY']=w['POLYMARKET_PRIVATE_KEY']
    os.environ['POLYMARKET_PROXY_ADDRESS']=deposit_wallet
    os.environ['POLYMARKET_SIGNATURE_TYPE']='3'
    os.environ['POLYMARKET_BUILDER_CODE']=w.get('POLYMARKET_BUILDER_CODE','')
    os.environ['POLYMARKET_BUILDER_ADDRESS']=w.get('POLYMARKET_BUILDER_ADDRESS','')
    # Existing stored CLOB API creds were rejected by CLOB; derive/use existing via L1 auth instead.
    for k in ['POLYMARKET_API_KEY','POLYMARKET_API_SECRET','POLYMARKET_API_PASSPHRASE','POLY_API_KEY','POLY_API_SECRET','POLY_PASSPHRASE']:
        os.environ.pop(k, None)
    result={'wallet':'prism5','owner_eoa':w.get('POLYMARKET_EOA_ADDRESS'),'deposit_wallet':deposit_wallet,'signature_type':3,'stake_usd':str(STAKE),'buy':{},'sell':{}}
    try:
        ex=PolymarketExecutionClient(); clob=ex.http.clob
        result['balance_before']=clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        try: result['update_balance_allowance']=clob.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        except Exception as e: result['update_balance_allowance_error']=repr(e)
        candidate=choose_candidate(); tick=D(candidate['tick_size'])
        result['candidate']={**candidate,'ask':str(candidate['ask']),'bid':str(candidate['bid']),'spread':str(candidate['spread']),'ask_depth':str(candidate['ask_depth']),'bid_depth':str(candidate['bid_depth'])}
        before_token=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance','0'))/Decimal(10)**6
        buy_cap=min(Decimal('0.999'),(candidate['ask']+max(tick,Decimal('0.001'))).quantize(tick,rounding=ROUND_UP))
        buy_size=q4(STAKE/buy_cap)
        buy_resp=ex.submit(PolyOrder(token_id=candidate['token'], side='BUY', price=buy_cap, size=buy_size, order_type='FAK', post_only=False, use_limit_order=False, tick_size=str(tick), neg_risk=bool(candidate['neg_risk']), builder_code=w.get('POLYMARKET_BUILDER_CODE')))
        result['buy']={'limit_price':str(buy_cap),'requested_size':str(buy_size),'response':buy_resp,'order_id':order_id(buy_resp),'status':status_from(buy_resp)}
        bought=Decimal('0')
        for _ in range(15):
            cur=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance','0'))/Decimal(10)**6
            bought=max(Decimal('0'),cur-before_token)
            if bought>0: break
            await asyncio.sleep(1)
        result['buy']['bought_shares_from_wallet_delta']=str(q4(bought))
        sell_attempts=[]
        if bought>0:
            # Try immediate FOK at best bid, then slightly lower FAK if needed to avoid residual.
            for attempt in range(3):
                book=requests.get('https://clob.polymarket.com/book',params={'token_id':candidate['token']},timeout=10).json()
                bids,_=levels(book); best_bid=max(p for p,_ in bids) if bids else Decimal('0')
                cur=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance','0'))/Decimal(10)**6
                remaining=q4(max(Decimal('0'),cur-before_token))
                if remaining <= Decimal('0.0001'): break
                sell_px=max(Decimal('0.001'), (best_bid - (tick*attempt)).quantize(tick, rounding=ROUND_DOWN))
                sell_type='FOK' if attempt == 0 else 'FAK'
                sell_resp=ex.submit(PolyOrder(token_id=candidate['token'], side='SELL', price=sell_px, size=remaining, order_type=sell_type, post_only=False, use_limit_order=True, tick_size=str(tick), neg_risk=bool(candidate['neg_risk']), builder_code=w.get('POLYMARKET_BUILDER_CODE')))
                sell_attempts.append({'attempt':attempt+1,'order_type':sell_type,'limit_price':str(sell_px),'size':str(remaining),'response':sell_resp,'order_id':order_id(sell_resp),'status':status_from(sell_resp)})
                await asyncio.sleep(3)
            result['sell']={'attempts':sell_attempts}
        else:
            result['sell']={'status':'skipped','reason':'no token balance appeared after BUY'}
        after_token=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance','0'))/Decimal(10)**6
        result['residual_delta_shares']=str(q4(after_token-before_token))
        result['balance_after']=clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    except Exception as e:
        result['error']=repr(e)
    finally:
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(result,indent=2,default=str))
        print(json.dumps(result,indent=2,default=str))
        os.environ.clear(); os.environ.update(old)

if __name__=='__main__': asyncio.run(main())
