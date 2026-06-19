#!/usr/bin/env python3
from __future__ import annotations
import json, os, time
from decimal import Decimal
from pathlib import Path
import requests
from dotenv import load_dotenv

ROOT=Path('/home/administrator/projects/polybot')
OUT=ROOT/'research'/'manual_close_all_positions'

def D(x):
    try: return Decimal(str(x if x is not None else '0'))
    except Exception: return Decimal('0')

def load_envs(files):
    old=dict(os.environ)
    for f in [ROOT/'.env', ROOT/'.env.live', Path('/home/administrator/projects/polybot-dash/.env.local'), *files]:
        if Path(f).exists(): load_dotenv(f, override=True)
    return old

def restore(old): os.environ.clear(); os.environ.update(old)

def fetch(user):
    rows=[]
    for off in range(0,5000,500):
        r=requests.get('https://data-api.polymarket.com/positions',params={'user':user,'limit':500,'offset':off},timeout=30); r.raise_for_status(); b=r.json()
        if not b: break
        rows+=b
        if len(b)<500: break
    return rows

def main():
    from polybot.adapters.polymarket.execution import PolymarketExecutionClient
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
    wallets=[('default',[ROOT/'.env', ROOT/'.env.live', Path('/home/administrator/projects/polybot-dash/.env.local')]),('prism2',[ROOT/'.env', ROOT/'config/wallets/prism2.env']),('prism3',[ROOT/'.env', ROOT/'config/wallets/prism3.env'])]
    seen=set(); rep={'ts':time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),'wallets':[]}
    for name,files in wallets:
        old=load_envs(files)
        proxy=os.getenv('POLYMARKET_PROXY_ADDRESS') or os.getenv('PROXY_ADDRESS') or os.getenv('POLYMARKET_ADDRESS')
        if not proxy or proxy.lower() in seen:
            restore(old); continue
        seen.add(proxy.lower())
        w={'name':name,'proxy':proxy,'open_orders':None,'positions':[],'counts':{}}
        ex=PolymarketExecutionClient(); clob=ex.http.clob
        try: w['open_orders']=len(ex.open_orders())
        except Exception as e: w['open_orders_error']=repr(e)
        for p in fetch(proxy):
            token=str(p.get('asset') or '')
            if not token: continue
            size=D(p.get('size')); val=D(p.get('currentValue')); cur=D(p.get('curPrice'))
            if size<=0 or (val<=0 and cur<=0): continue
            rec={'asset_tail':token[-8:],'title':p.get('title'),'outcome':p.get('outcome'),'data_size':str(size),'curPrice':str(p.get('curPrice')),'currentValue':str(p.get('currentValue')),'redeemable':bool(p.get('redeemable'))}
            try:
                bal=clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token)) or {}
                shares=D(bal.get('balance'))/(Decimal(10)**6)
                rec['conditional_shares']=str(shares)
            except Exception as e: rec['balance_error']=repr(e)
            if rec.get('redeemable'): kind='redeemable'
            elif D(rec.get('conditional_shares')) <= Decimal('0.0001'): kind='zero_balance'
            elif val < Decimal('0.05'): kind='dust_unresolved'
            else: kind='unresolved'
            rec['kind']=kind
            w['counts'][kind]=w['counts'].get(kind,0)+1
            w['positions'].append(rec)
        rep['wallets'].append(w)
        restore(old)
    OUT.mkdir(parents=True, exist_ok=True)
    out=OUT/'verify_after_close_latest.json'; out.write_text(json.dumps(rep,indent=2,default=str))
    print(json.dumps({'out':str(out),'summary':[{'wallet':w['name'],'proxy_tail':w['proxy'][-6:],'open_orders':w.get('open_orders'),'counts':w['counts']} for w in rep['wallets']]},indent=2))
if __name__=='__main__': main()
