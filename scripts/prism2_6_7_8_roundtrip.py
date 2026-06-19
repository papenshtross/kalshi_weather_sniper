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

ROOT=Path('/home/administrator/projects/polybot')
REPORT=ROOT/'reports/deployment/prism2_6_7_8_roundtrip_2026-06-04.json'
STAKE=Decimal('1.00')


def D(x: Any) -> Decimal: return Decimal(str(x))
def q4(x: Decimal) -> Decimal: return x.quantize(Decimal('0.0001'), rounding=ROUND_DOWN)
def parse_jsonish(v: Any, default: Any) -> Any:
    if isinstance(v,str):
        try: return json.loads(v)
        except Exception: return default
    return v if v is not None else default

def order_id(resp: dict[str,Any]|None)->str|None:
    if not isinstance(resp,dict): return None
    for k in ('orderID','order_id','id'):
        if resp.get(k): return str(resp[k])
    nested=resp.get('order') if isinstance(resp.get('order'),dict) else None
    return order_id(nested) if nested else None

def status_from(resp: dict[str,Any]|None)->str:
    if not isinstance(resp,dict): return 'unknown'
    raw=str(resp.get('status') or '').lower()
    if raw in {'matched','filled'}: return 'filled'
    if raw in {'delayed','pending','live'}: return 'submitted'
    if resp.get('success') is True and (order_id(resp) or raw in {'success',''}): return 'filled' if raw in {'matched','success',''} else 'submitted'
    if resp.get('success') is False: return 'rejected'
    return raw or 'submitted'

def levels(book:dict[str,Any]):
    bids=[(D(x['price']),D(x['size'])) for x in book.get('bids',[]) if D(x.get('price',0))>0 and D(x.get('size',0))>0]
    asks=[(D(x['price']),D(x['size'])) for x in book.get('asks',[]) if D(x.get('price',0))>0 and D(x.get('size',0))>0]
    return bids,asks

def choose_candidate()->dict[str,Any]:
    markets=requests.get('https://gamma-api.polymarket.com/markets',params={'active':'true','closed':'false','limit':500,'order':'volume24hr','ascending':'false'},timeout=25).json()
    req=[]; meta={}
    for m in markets:
        if not m.get('acceptingOrders', True): continue
        toks=parse_jsonish(m.get('clobTokenIds'), [])
        outs=parse_jsonish(m.get('outcomes'), ['Yes','No'])
        if not toks or len(toks)<2: continue
        for i,tok in enumerate(toks[:2]):
            tok=str(tok); req.append({'token_id':tok})
            meta[tok]={'token':tok,'outcome':outs[i] if i<len(outs) else ('Yes' if i==0 else 'No'),'slug':m.get('slug'),'question':m.get('question'),'tick_size':str(m.get('orderPriceMinTickSize') or m.get('minimumTickSize') or m.get('tickSize') or '0.01'),'order_min_size':D(m.get('orderMinSize') or 5),'neg_risk':bool(m.get('negRisk') or m.get('neg_risk') or False)}
    cand=[]
    for i in range(0,len(req),100):
        r=requests.post('https://clob.polymarket.com/books',json=req[i:i+100],timeout=12); r.raise_for_status()
        for b in r.json() or []:
            tok=str(b.get('asset_id') or b.get('token_id') or '')
            if tok not in meta: continue
            bids,asks=levels(b)
            if not bids or not asks: continue
            bid=max(p for p,_ in bids); ask=min(p for p,_ in asks)
            ask_depth=sum(s for p,s in asks if p<=ask); bid_depth=sum(s for p,s in bids if p>=bid)
            shares=STAKE/ask; spread=ask-bid
            if Decimal('0.02') <= ask <= Decimal('0.20') and spread <= Decimal('0.01') and shares >= meta[tok]['order_min_size'] and ask_depth >= meta[tok]['order_min_size'] and bid_depth >= meta[tok]['order_min_size']:
                cand.append((spread,-min(ask_depth,bid_depth),ask,bid,tok,meta[tok],ask_depth,bid_depth))
    if not cand: raise RuntimeError('no cheap liquid candidate found')
    spread,_,ask,bid,tok,m,ad,bd=sorted(cand)[0]
    m.update({'ask':ask,'bid':bid,'spread':spread,'ask_depth':ad,'bid_depth':bd})
    return m

async def run_one(wallet_id:str, candidate:dict[str,Any], registry:dict[str,Any])->dict[str,Any]:
    old=dict(os.environ)
    try:
        os.environ['POLYBOT_WALLET_ID']=wallet_id
        os.environ['POLYMARKET_WALLET_ID']=wallet_id
        # prevent unrelated shell env overrides
        for k in ['POLYMARKET_PRIVATE_KEY','POLYMARKET_PROXY_ADDRESS','POLYMARKET_API_KEY','POLYMARKET_API_SECRET','POLYMARKET_API_PASSPHRASE','POLYMARKET_SIGNATURE_TYPE']:
            os.environ.pop(k,None)
        w=registry['wallets'][wallet_id]
        result={'wallet_id':wallet_id,'owner_eoa':w.get('POLYMARKET_EOA_ADDRESS'),'deposit_wallet':w.get('POLYMARKET_DEPOSIT_WALLET') or w.get('POLYMARKET_PROXY_ADDRESS'),'signature_type':w.get('POLYMARKET_SIGNATURE_TYPE'),'buy':{},'sell':{}}
        ex=PolymarketExecutionClient(); clob=ex.http.clob
        bal_before=clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        result['balance_before']=bal_before
        raw_bal=D(bal_before.get('balance','0'))/Decimal(10)**6
        if raw_bal < Decimal('1.05'):
            result['skipped']='insufficient_balance_for_$1_roundtrip'
            return result
        try: result['update_balance_allowance']=clob.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        except Exception as e: result['update_balance_allowance_error']=repr(e)
        tick=D(candidate['tick_size'])
        result['candidate']={**candidate,'ask':str(candidate['ask']),'bid':str(candidate['bid']),'spread':str(candidate['spread']),'ask_depth':str(candidate['ask_depth']),'bid_depth':str(candidate['bid_depth'])}
        before_token=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance','0'))/Decimal(10)**6
        buy_cap=min(Decimal('0.999'),(candidate['ask']+max(tick,Decimal('0.001'))).quantize(tick,rounding=ROUND_UP))
        buy_size=q4(STAKE/buy_cap)
        buy_resp=ex.submit(PolyOrder(token_id=candidate['token'],side='BUY',price=buy_cap,size=buy_size,order_type='FAK',post_only=False,use_limit_order=False,tick_size=str(tick),neg_risk=bool(candidate['neg_risk']),builder_code=(registry['wallets'].get('prism5') or {}).get('POLYMARKET_BUILDER_CODE')))
        result['buy']={'limit_price':str(buy_cap),'requested_size':str(buy_size),'response':buy_resp,'order_id':order_id(buy_resp),'status':status_from(buy_resp)}
        bought=Decimal('0')
        for _ in range(15):
            cur=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance','0'))/Decimal(10)**6
            bought=max(Decimal('0'),cur-before_token)
            if bought>0: break
            await asyncio.sleep(1)
        result['buy']['bought_shares_from_wallet_delta']=str(q4(bought))
        attempts=[]
        if bought>0:
            for attempt in range(3):
                book=requests.get('https://clob.polymarket.com/book',params={'token_id':candidate['token']},timeout=10).json()
                bids,_=levels(book); best_bid=max(p for p,_ in bids) if bids else Decimal('0')
                cur=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance','0'))/Decimal(10)**6
                remaining=q4(max(Decimal('0'),cur-before_token))
                if remaining <= Decimal('0.0001'): break
                sell_px=max(Decimal('0.001'),(best_bid-(tick*attempt)).quantize(tick,rounding=ROUND_DOWN))
                sell_type='FOK' if attempt==0 else 'FAK'
                sell_resp=ex.submit(PolyOrder(token_id=candidate['token'],side='SELL',price=sell_px,size=remaining,order_type=sell_type,post_only=False,use_limit_order=True,tick_size=str(tick),neg_risk=bool(candidate['neg_risk']),builder_code=(registry['wallets'].get('prism5') or {}).get('POLYMARKET_BUILDER_CODE')))
                attempts.append({'attempt':attempt+1,'order_type':sell_type,'limit_price':str(sell_px),'size':str(remaining),'response':sell_resp,'order_id':order_id(sell_resp),'status':status_from(sell_resp)})
                await asyncio.sleep(3)
        else:
            result['sell']={'status':'skipped','reason':'no token balance appeared after BUY'}
        if attempts: result['sell']={'attempts':attempts}
        after_token=D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance','0'))/Decimal(10)**6
        result['residual_delta_shares']=str(q4(after_token-before_token))
        result['balance_after']=clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        # authenticated order verification
        for key,oid in [('buy_get_order',result.get('buy',{}).get('order_id')),('sell_get_order',(attempts[0].get('order_id') if attempts else None))]:
            if oid:
                try: result[key]=clob.get_order(oid)
                except Exception as e: result[key+'_error']=repr(e)
        return result
    except Exception as e:
        return {'wallet_id':wallet_id,'error':repr(e)}
    finally:
        os.environ.clear(); os.environ.update(old)

async def main():
    registry=decrypt_registry()
    candidate=choose_candidate()
    results=[]
    for wid in ['prism2','prism6','prism7','prism8']:
        results.append(await run_one(wid,candidate,registry))
    payload={'generated_at':int(time.time()),'stake_usd':str(STAKE),'candidate':{**candidate,'ask':str(candidate['ask']),'bid':str(candidate['bid']),'spread':str(candidate['spread']),'ask_depth':str(candidate['ask_depth']),'bid_depth':str(candidate['bid_depth'])},'results':results}
    REPORT.parent.mkdir(parents=True,exist_ok=True); REPORT.write_text(json.dumps(payload,indent=2,default=str))
    print(json.dumps(payload,indent=2,default=str))

if __name__=='__main__': asyncio.run(main())
