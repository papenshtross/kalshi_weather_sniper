#!/usr/bin/env python3
from __future__ import annotations
import os, json
from pathlib import Path
from dotenv import load_dotenv
from eth_account import Account
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

ROOT=Path('/home/administrator/projects/polybot')
for k in list(os.environ):
    if k.startswith('POLYMARKET_') or k.startswith('POLY_') or k=='PROXY_ADDRESS': os.environ.pop(k,None)
for f in [ROOT/'.env', ROOT/'.env.live', Path('/home/administrator/projects/polybot-dash/.env.local'), ROOT/'config/wallets/prism2.env']:
    if f.exists(): load_dotenv(f, override=True)
pk=os.environ['POLYMARKET_PRIVATE_KEY']
proxy=os.getenv('POLYMARKET_PROXY_ADDRESS') or os.getenv('PROXY_ADDRESS')
eoa=Account.from_key(pk).address
print(json.dumps({'env_proxy':proxy,'env_eoa':os.getenv('POLYMARKET_EOA_ADDRESS'), 'derived_eoa':eoa, 'env_sig_type':os.getenv('POLYMARKET_SIGNATURE_TYPE')}, indent=2))
for sig in [0,1,2]:
  for funder_name,funder in [('none',None),('eoa',eoa),('proxy',proxy)]:
    try:
      c=ClobClient('https://clob.polymarket.com', key=pk, chain_id=137, signature_type=sig, funder=funder)
      try:
        creds=c.create_or_derive_api_key(); c.set_api_creds(creds); auth='ok'
      except Exception as e:
        auth=f'auth_error:{type(e).__name__}:{e}'
      try:
        bal=c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
      except Exception as e:
        bal={'error':f'{type(e).__name__}:{e}'}
      print(json.dumps({'sig':sig,'funder':funder_name,'auth':auth,'collateral':bal}, default=str))
    except Exception as e:
      print(json.dumps({'sig':sig,'funder':funder_name,'init_error':repr(e)}))
