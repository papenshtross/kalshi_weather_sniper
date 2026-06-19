#!/usr/bin/env python3
"""Close all live Polymarket positions for configured wallets.

Stops/closing script used only on explicit user instruction. It:
- loads default/prism2/prism3 wallet envs without printing secrets;
- cancels all CLOB open orders first;
- fetches data-api positions for each proxy wallet;
- verifies conditional token balance through CLOB;
- submits SELL FAK orders crossing available bid depth for each non-redeemable position.

Resolved/redeemable/no-bid/dust positions are reported but not fabricated as closed.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

ROOT = Path('/home/administrator/projects/polybot')
OUT = ROOT / 'research' / 'manual_close_all_positions'
DATA_API = 'https://data-api.polymarket.com/positions'


def D(x: Any) -> Decimal:
    try:
        return Decimal(str(x if x is not None else '0'))
    except Exception:
        return Decimal('0')


def level_price(level: Any) -> Decimal:
    return D(getattr(level, 'price', None) if not isinstance(level, dict) else level.get('price'))


def level_size(level: Any) -> Decimal:
    return D(getattr(level, 'size', None) if not isinstance(level, dict) else level.get('size'))


def q4(x: Decimal) -> Decimal:
    return x.quantize(Decimal('0.0001'), rounding=ROUND_DOWN)


@dataclass
class WalletSpec:
    name: str
    env_files: list[Path]
    proxy: str


def env_from_files(files: list[Path]) -> dict[str, str]:
    old = dict(os.environ)
    for k in list(os.environ):
        if k.startswith('POLYMARKET_') or k.startswith('POLY_') or k in {'PROXY_ADDRESS'}:
            os.environ.pop(k, None)
    for f in files:
        if f.exists():
            load_dotenv(f, override=True)
    env = {k: v for k, v in os.environ.items() if k.startswith('POLYMARKET_') or k.startswith('POLY_') or k in {'PROXY_ADDRESS'}}
    os.environ.clear(); os.environ.update(old)
    return env


def discover_wallets() -> list[WalletSpec]:
    candidates = [
        ('default', [ROOT/'.env', ROOT/'.env.live', Path('/home/administrator/projects/polybot-dash/.env.local')]),
        ('prism2', [ROOT/'.env', ROOT/'config/wallets/prism2.env']),
        ('prism3', [ROOT/'.env', ROOT/'config/wallets/prism3.env']),
    ]
    out=[]; seen=set()
    for name, files in candidates:
        env=env_from_files(files)
        proxy=env.get('POLYMARKET_PROXY_ADDRESS') or env.get('PROXY_ADDRESS') or env.get('POLYMARKET_ADDRESS')
        pk=env.get('POLYMARKET_PRIVATE_KEY')
        if not proxy or not pk:
            continue
        if proxy.lower() in seen:
            continue
        seen.add(proxy.lower())
        out.append(WalletSpec(name, files, proxy))
    return out


def fetch_positions(user: str) -> list[dict[str, Any]]:
    rows=[]
    for offset in range(0, 5000, 500):
        r=requests.get(DATA_API, params={'user': user, 'limit': 500, 'offset': offset}, timeout=30)
        r.raise_for_status()
        batch=r.json()
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < 500:
            break
        time.sleep(0.1)
    return rows




_META_CACHE: dict[str, dict[str, Any]] = {}

def market_meta_for_token(token: str) -> dict[str, Any]:
    if token in _META_CACHE:
        return _META_CACHE[token]
    meta: dict[str, Any] = {"tick_size": "0.01", "neg_risk": False, "order_min_size": None}
    try:
        r = requests.get('https://gamma-api.polymarket.com/markets', params={'clob_token_ids': token}, timeout=20)
        r.raise_for_status()
        rows = r.json()
        if rows:
            m = rows[0]
            meta = {
                "tick_size": str(m.get('orderPriceMinTickSize') or '0.01'),
                "neg_risk": bool(m.get('negRisk')),
                "order_min_size": m.get('orderMinSize'),
                "slug": m.get('slug'),
            }
    except Exception as e:
        meta["meta_error"] = repr(e)
    _META_CACHE[token] = meta
    return meta


def env_context(files: list[Path]):
    class Ctx:
        def __enter__(self):
            self.old=dict(os.environ)
            for f in [ROOT/'.env', ROOT/'.env.live', Path('/home/administrator/projects/polybot-dash/.env.local')]:
                if f.exists(): load_dotenv(f, override=True)
            for f in files:
                if f.exists(): load_dotenv(f, override=True)
            return self
        def __exit__(self, *exc):
            os.environ.clear(); os.environ.update(self.old)
    return Ctx()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    from polybot.adapters.polymarket.execution import PolymarketExecutionClient, PolyOrder
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

    report={'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'wallets': []}
    for wallet in discover_wallets():
        wrep={'name': wallet.name, 'proxy': wallet.proxy, 'cancel_all': None, 'open_orders_before': None, 'positions_count': 0, 'actions': [], 'errors': []}
        with env_context(wallet.env_files):
            ex=PolymarketExecutionClient()
            clob=ex.http.clob
            try:
                oo=ex.open_orders()
                wrep['open_orders_before']=len(oo) if isinstance(oo, list) else oo
            except Exception as e:
                wrep['errors'].append({'stage':'open_orders_before','error':repr(e)})
            try:
                wrep['cancel_all']=ex.cancel_all()
            except Exception as e:
                wrep['errors'].append({'stage':'cancel_all','error':repr(e)})
            positions=fetch_positions(wallet.proxy)
            # keep positions with positive size/current value; data-api sometimes includes historical tiny/resolved rows
            live=[]
            for p in positions:
                asset=str(p.get('asset') or '')
                if not asset:
                    continue
                size=D(p.get('size'))
                cur=D(p.get('curPrice'))
                val=D(p.get('currentValue'))
                if size > 0 and (val > 0 or cur > 0):
                    live.append(p)
            wrep['positions_count']=len(live)
            for p in sorted(live, key=lambda x: D(x.get('currentValue')), reverse=True):
                token=str(p.get('asset'))
                item={'asset':token, 'asset_tail':token[-8:], 'title':p.get('title'), 'outcome':p.get('outcome'), 'data_size':str(p.get('size')), 'curPrice':p.get('curPrice'), 'currentValue':p.get('currentValue'), 'redeemable':p.get('redeemable')}
                if p.get('redeemable'):
                    item['action']='skip_redeemable'
                    wrep['actions'].append(item); continue
                meta = market_meta_for_token(token)
                item['market_meta'] = meta
                try:
                    balraw=clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token)) or {}
                    shares=D(balraw.get('balance'))/(Decimal(10)**6)
                    item['conditional_shares']=str(shares)
                except Exception as e:
                    item['action']='error_balance'; item['error']=repr(e); wrep['actions'].append(item); continue
                if shares <= Decimal('0.0001'):
                    item['action']='skip_zero_balance'; wrep['actions'].append(item); continue
                try:
                    book=clob.get_order_book(token)
                    bids=getattr(book, 'bids', None) or (book.get('bids') if isinstance(book, dict) else []) or []
                    bids=sorted(bids, key=level_price, reverse=True)
                    item['bids_len']=len(bids)
                    item['top_bid']=str(level_price(bids[0])) if bids else None
                except Exception as e:
                    item['action']='error_book'; item['error']=repr(e); wrep['actions'].append(item); continue
                if not bids:
                    item['action']='skip_no_bids'; wrep['actions'].append(item); continue
                need=shares
                cum=Decimal('0'); worst=None
                for lvl in bids:
                    px=level_price(lvl); sz=level_size(lvl)
                    if px <= 0 or sz <= 0: continue
                    cum += sz; worst=px
                    if cum >= need: break
                if worst is None:
                    item['action']='skip_no_positive_bids'; wrep['actions'].append(item); continue
                min_size = D(meta.get('order_min_size'))
                sell_size=q4(min(shares, cum))
                if sell_size <= Decimal('0.0001'):
                    item['action']='skip_dust_below_quant'; item['available_bid_shares']=str(cum); wrep['actions'].append(item); continue
                if min_size > 0 and sell_size < min_size:
                    item['action']='skip_below_order_min_size'; item['available_bid_shares']=str(cum); item['order_min_size']=str(min_size); wrep['actions'].append(item); continue
                # FAK crosses the visible bid depth and cancels residual automatically.
                tick = D(meta.get('tick_size') or '0.01')
                if tick <= 0 or tick < Decimal('0.01'):
                    # The SDK/order builder rejects SELL prices below 0.01 even on 0.001-tick markets.
                    tick = Decimal('0.01')
                worst = max(worst, Decimal('0.01'))
                order=PolyOrder(token_id=token, side='SELL', price=worst, size=sell_size, order_type='FAK', use_limit_order=True, tick_size=str(tick), neg_risk=bool(meta.get('neg_risk')))
                item['sell_attempt']={'size':str(sell_size),'limit_price':str(worst),'visible_bid_shares_to_limit':str(cum)}
                try:
                    resp=ex.submit(order)
                    item['action']='sell_fak_submitted'
                    item['response']=resp
                except Exception as e:
                    item['action']='error_submit'; item['error']=repr(e)
                wrep['actions'].append(item)
                time.sleep(0.35)
            try:
                wrep['open_orders_after']=len(ex.open_orders())
            except Exception as e:
                wrep['errors'].append({'stage':'open_orders_after','error':repr(e)})
        report['wallets'].append(wrep)
    report['finished_at']=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    out=OUT/f'close_all_positions_{time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())}.json'
    latest=OUT/'close_all_positions_latest.json'
    out.write_text(json.dumps(report, indent=2, default=str))
    latest.write_text(json.dumps(report, indent=2, default=str))
    summary=[]
    for w in report['wallets']:
        counts={}
        for a in w['actions']:
            counts[a.get('action','?')]=counts.get(a.get('action','?'),0)+1
        summary.append({'wallet':w['name'],'proxy_tail':w['proxy'][-6:],'positions':w['positions_count'],'open_orders_before':w.get('open_orders_before'),'open_orders_after':w.get('open_orders_after'),'counts':counts,'errors':w['errors']})
    print(json.dumps({'report':str(out),'summary':summary}, indent=2, default=str))

if __name__ == '__main__':
    main()
