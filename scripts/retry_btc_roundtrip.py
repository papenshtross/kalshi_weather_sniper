#!/usr/bin/env python3
from __future__ import annotations
import asyncio, os, json, time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from dotenv import load_dotenv
from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.live.arb_sniper import resolve_event_pairs, rest_books_full
from polybot.persistence.writer import PolybotWriter

SID='live_arb_sniper_btc15m_v1'
SLUG='btc-updown-15m-auto'
STAKE=Decimal('1.00')

def D(x): return Decimal(str(x))
def q4(x): return x.quantize(Decimal('0.0001'), rounding=ROUND_DOWN)
def tick(t): return Decimal(str(t or '0.01'))
def buy_px(ask,t): return min(Decimal('0.999'), (D(ask)+Decimal('0.03')).quantize(tick(t), rounding=ROUND_UP))
def sell_px(bid,t): return max(Decimal('0.001'), D(bid).quantize(tick(t), rounding=ROUND_DOWN))
def ok(resp): return bool(resp.get('success') and str(resp.get('status','')).lower()=='matched' and resp.get('takingAmount') and resp.get('makingAmount'))
async def rec(writer,pair,leg,token,side,otype,px,size,status,resp,err,tag):
 await writer.record_order_attempt(SID,str(pair['slug']),token,leg,side,otype,float(px),float(size),float((px*size).quantize(Decimal('0.0001'),rounding=ROUND_DOWN)),status,response=resp,error=err,signal={'manual_test':True,'tag':tag,'market_title':pair.get('title'),'btc_retry':True},config={'stake_per_side_usd':1.0,'sell_immediately':True,'buy_cap_extra':0.03})
async def main():
 for f in ['.env','.env.live','/home/administrator/projects/polybot-dash/.env.local']:
  if Path(f).exists(): load_dotenv(f, override=False)
 writer=PolybotWriter(os.getenv('NAUTILUS_DB_URL') or os.getenv('POSTGRES_URL') or os.getenv('DATABASE_URL')); await writer.connect()
 ex=PolymarketExecutionClient(); clob=ex.http.clob
 pairs=await resolve_event_pairs(SLUG, all_markets=False); pair=pairs[0]
 async with __import__('httpx').AsyncClient(timeout=5) as c:
  books=await rest_books_full(c,[pair['yes_token'],pair['no_token']])
 t=str(pair.get('tick_size') or '0.01'); neg=bool(pair.get('neg_risk') or False)
 plans={}
 for leg,token in [('YES',pair['yes_token']),('NO',pair['no_token'])]:
  b=books[str(token)]
  px=buy_px(b.ask,t)
  size=q4(STAKE/px)
  plans[leg]={'token':str(token),'buy_px':px,'sell_px':sell_px(b.bid,t),'size':size}
 tag=f'manual_roundtrip_btc15m_retry_{int(time.time())}'
 await writer.log_strategy_event(SID,f"MANUAL TEST {tag}: retrying BTC $1/side BUY batch then immediate SELL on {pair['slug']} with 3c buy cap buffer",level='INFO')
 orders=[PolyOrder(token_id=plans[leg]['token'],side='BUY',price=plans[leg]['buy_px'],size=plans[leg]['size'],order_type='FOK',post_only=False,use_limit_order=False,tick_size=t,neg_risk=neg) for leg in ['YES','NO']]
 resps=ex.submit_batch(orders)
 filled={}; results={'tag':tag,'market_slug':pair['slug'],'legs':{}}
 for i,leg in enumerate(['YES','NO']):
  p=plans[leg]; resp=resps[i] if i<len(resps) and isinstance(resps[i],dict) else {'raw':resps[i] if i<len(resps) else None}
  status='filled' if ok(resp) else 'rejected'; err=None if status=='filled' else str(resp.get('error') or resp.get('errorMsg') or resp)
  await rec(writer,pair,leg,p['token'],'BUY','TEST_ROUNDTRIP_FOK_RETRY2',p['buy_px'],p['size'],status,resp,err,tag)
  results['legs'][leg]={'buy_status':status,'buy_resp':resp,'buy_px':str(p['buy_px']),'req_size':str(p['size'])}
  if status=='filled':
   shares=q4(D(resp['takingAmount'])); filled[leg]=shares
   await writer.record_fill(SID,int(time.time()*1000)%1_000_000_000,f"{str(pair.get('title'))[:40]} [MANUAL_TEST_RETRY2] {leg}",'BUY',float(p['buy_px']),float(shares),kind='MANUAL_TEST_BUY_RETRY2')
 # Wait until conditional token balances are visible, then sell.
 for leg,shares in filled.items():
  p=plans[leg]
  bal_shares=Decimal('0')
  for _ in range(10):
   bal=clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=p['token']))
   bal_shares=D(bal.get('balance','0'))/(Decimal(10)**6)
   if bal_shares >= shares: break
   await asyncio.sleep(1)
  sell_size=min(shares,q4(bal_shares))
  if sell_size<=0:
   await writer.log_strategy_event(SID,f"MANUAL TEST {tag}: no visible balance to sell {leg}, balance={bal_shares}",level='ERROR'); continue
  # Refresh book for sell px.
  async with __import__('httpx').AsyncClient(timeout=5) as c:
   b=(await rest_books_full(c,[p['token']]))[p['token']]
  spx=sell_px(b.bid,t)
  try: sresp=ex.submit(PolyOrder(token_id=p['token'],side='SELL',price=spx,size=sell_size,order_type='FOK',post_only=False,use_limit_order=True,tick_size=t,neg_risk=neg))
  except Exception as e: sresp={'success':False,'error':repr(e)}
  sstatus='filled' if ok(sresp) else 'rejected'; serr=None if sstatus=='filled' else str(sresp.get('error') or sresp.get('errorMsg') or sresp)
  await rec(writer,pair,leg,p['token'],'SELL','TEST_ROUNDTRIP_FOK_RETRY2',spx,sell_size,sstatus,sresp,serr,tag)
  if sstatus=='filled': await writer.record_fill(SID,int(time.time()*1000)%1_000_000_000,f"{str(pair.get('title'))[:40]} [MANUAL_TEST_RETRY2] {leg}",'SELL',float(spx),float(sell_size),kind='MANUAL_TEST_SELL_RETRY2')
  results['legs'][leg].update({'sell_status':sstatus,'sell_resp':sresp,'sell_px':str(spx),'sell_size':str(sell_size)})
 residual={leg:str(shares) for leg,shares in filled.items() if results['legs'][leg].get('sell_status')!='filled'}
 await writer.log_strategy_event(SID,f"MANUAL TEST {tag}: BTC retry completed residual={residual or 'none'} results={json.dumps(results)[:900]}",level='INFO' if not residual else 'ERROR')
 print(json.dumps(results,indent=2,default=str))
 await writer.close()
if __name__=='__main__': asyncio.run(main())
