#!/usr/bin/env python3
from __future__ import annotations
import os,json
from pathlib import Path
from dotenv import load_dotenv
from eth_account import Account
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import TradeParams, OpenOrderParams
ROOT=Path('/home/administrator/projects/polybot')
for k in list(os.environ):
    if k.startswith('POLYMARKET_') or k.startswith('POLY_') or k=='PROXY_ADDRESS': os.environ.pop(k,None)
for f in [ROOT/'.env', ROOT/'.env.live', Path('/home/administrator/projects/polybot-dash/.env.local'), ROOT/'config/wallets/prism2.env']:
    if f.exists(): load_dotenv(f, override=True)
pk=os.environ['POLYMARKET_PRIVATE_KEY']; proxy=os.getenv('POLYMARKET_PROXY_ADDRESS'); eoa=Account.from_key(pk).address
for funder_name,funder in [('none',None),('eoa',eoa),('proxy',proxy)]:
    c=ClobClient('https://clob.polymarket.com', key=pk, chain_id=137, signature_type=1, funder=funder)
    creds=c.create_or_derive_api_key(); c.set_api_creds(creds)
    out={'funder':funder_name,'api_key_prefix':getattr(creds,'api_key', '')[:8] if creds else None}
    for name,fn in [
      ('api_keys', lambda: c.get_api_keys()),
      ('open_orders', lambda: c.get_open_orders(OpenOrderParams())),
      ('trades', lambda: c.get_trades(TradeParams())),
    ]:
      try:
        v=fn()
        if isinstance(v,list):
          out[name+'_count']=len(v); out[name+'_sample']=v[:3]
        else: out[name]=v
      except Exception as e: out[name+'_error']=f'{type(e).__name__}:{e}'
    print(json.dumps(out, default=str, indent=2))
