#!/usr/bin/env python3
from __future__ import annotations

import asyncio, json, os, time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.persistence.writer import PolybotWriter

ROOT = Path('/home/administrator/projects/polybot')
REPORT_PATH = ROOT / 'reports/deployment/prism2_test_order_roundtrip.json'
WALLET_ENV = ROOT / 'config/wallets/prism2.env'
STRATEGY_ID = 'manual_prism2_wallet_test_roundtrip'
STAKE = Decimal('1.00')


def D(x: Any) -> Decimal:
    return Decimal(str(x))


def q4(x: Decimal) -> Decimal:
    return x.quantize(Decimal('0.0001'), rounding=ROUND_DOWN)


def parse_jsonish(v: Any, default: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return default
    return v if v is not None else default


def order_id(resp: dict[str, Any] | None) -> str | None:
    if not isinstance(resp, dict):
        return None
    for k in ('orderID', 'order_id', 'id'):
        if resp.get(k):
            return str(resp[k])
    nested = resp.get('order') if isinstance(resp.get('order'), dict) else None
    return order_id(nested) if nested else None


def status_from(resp: dict[str, Any] | None) -> str:
    if not isinstance(resp, dict):
        return 'unknown'
    raw = str(resp.get('status') or '').lower()
    if raw in {'matched', 'filled'}:
        return 'filled'
    if raw in {'delayed', 'pending', 'live'}:
        return 'submitted'
    if resp.get('success') is True and order_id(resp):
        return 'submitted'
    if resp.get('success') is True and raw in {'success', ''}:
        return 'filled'
    if resp.get('success') is False:
        return 'rejected'
    return raw or 'submitted'


def is_matched(resp: dict[str, Any] | None) -> bool:
    if not isinstance(resp, dict):
        return False
    raw = str(resp.get('status') or '').lower()
    return raw in {'matched', 'filled'} or (resp.get('success') is True and raw in {'matched', 'success', ''})


def level_lists(book: dict[str, Any]):
    bids = [(D(x['price']), D(x['size'])) for x in book.get('bids', []) if D(x.get('price', 0)) > 0 and D(x.get('size', 0)) > 0]
    asks = [(D(x['price']), D(x['size'])) for x in book.get('asks', []) if D(x.get('price', 0)) > 0 and D(x.get('size', 0)) > 0]
    return bids, asks


def choose_candidate() -> dict[str, Any]:
    markets = requests.get(
        'https://gamma-api.polymarket.com/markets',
        params={'active': 'true', 'closed': 'false', 'limit': 300, 'order': 'volume24hr', 'ascending': 'false'},
        timeout=25,
    ).json()
    req, meta = [], {}
    for m in markets:
        if not m.get('acceptingOrders', True):
            continue
        toks = parse_jsonish(m.get('clobTokenIds'), [])
        outs = parse_jsonish(m.get('outcomes'), ['Yes', 'No'])
        if not toks or len(toks) < 2:
            continue
        for i, tok in enumerate(toks[:2]):
            tok = str(tok)
            req.append({'token_id': tok})
            meta[tok] = {
                'token': tok,
                'outcome': outs[i] if i < len(outs) else ('Yes' if i == 0 else 'No'),
                'slug': m.get('slug'),
                'question': m.get('question'),
                'tick_size': str(m.get('orderPriceMinTickSize') or m.get('minimumTickSize') or m.get('tickSize') or '0.01'),
                'order_min_size': D(m.get('orderMinSize') or 5),
                'neg_risk': bool(m.get('negRisk') or m.get('neg_risk') or False),
            }
    candidates = []
    for i in range(0, len(req), 100):
        r = requests.post('https://clob.polymarket.com/books', json=req[i:i+100], timeout=12)
        r.raise_for_status()
        for b in r.json() or []:
            tok = str(b.get('asset_id') or b.get('token_id') or '')
            if tok not in meta:
                continue
            bids, asks = level_lists(b)
            if not bids or not asks:
                continue
            bid = max(p for p, _ in bids)
            ask = min(p for p, _ in asks)
            ask_depth = sum(s for p, s in asks if p <= ask)
            bid_depth = sum(s for p, s in bids if p >= bid)
            shares = STAKE / ask
            spread = ask - bid
            if Decimal('0.02') <= ask <= Decimal('0.20') and bid > 0 and spread <= Decimal('0.01') and shares >= meta[tok]['order_min_size'] and ask_depth >= meta[tok]['order_min_size'] and bid_depth >= meta[tok]['order_min_size']:
                candidates.append((spread, -min(ask_depth, bid_depth), ask, bid, tok, meta[tok], ask_depth, bid_depth))
    if not candidates:
        raise RuntimeError('no cheap liquid candidate found for $1 buy + immediate sell')
    spread, _, ask, bid, tok, m, ask_depth, bid_depth = sorted(candidates)[0]
    m.update({'ask': ask, 'bid': bid, 'spread': spread, 'ask_depth': ask_depth, 'bid_depth': bid_depth})
    return m


def load_prism2_env() -> None:
    # Clear wallet-specific vars so prism2.env cannot be contaminated by another wallet's process env.
    for k in list(os.environ):
        if k.startswith('POLYMARKET_') or k.startswith('POLY_') or k == 'PROXY_ADDRESS':
            os.environ.pop(k, None)
    for f in [ROOT / '.env', ROOT / '.env.live', Path('/home/administrator/projects/polybot-dash/.env.local'), WALLET_ENV]:
        if f.exists():
            load_dotenv(f, override=True)


async def main() -> None:
    load_prism2_env()
    proxy = os.getenv('POLYMARKET_PROXY_ADDRESS') or os.getenv('PROXY_ADDRESS') or os.getenv('POLYMARKET_ADDRESS')
    if not proxy:
        raise RuntimeError('missing prism2 proxy address')
    writer = None
    dsn = os.getenv('NAUTILUS_DB_URL') or os.getenv('POSTGRES_URL') or os.getenv('DATABASE_URL')
    if dsn:
        writer = PolybotWriter(dsn)
        await writer.connect()
    try:
        ex = PolymarketExecutionClient()
        clob = ex.http.clob
        open_before = ex.open_orders()
        candidate = choose_candidate()
        tick = D(candidate['tick_size'])
        buy_cap = min(Decimal('0.999'), (candidate['ask'] + max(tick, Decimal('0.001'))).quantize(tick, rounding=ROUND_UP))
        buy_size = q4(STAKE / buy_cap)
        before_collateral = clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        before_token = D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance', '0')) / Decimal(10) ** 6
        tag = f'prism2_test_roundtrip_{int(time.time())}'
        result: dict[str, Any] = {
            'tag': tag,
            'wallet': 'prism2',
            'proxy': proxy,
            'strategy_id': STRATEGY_ID,
            'candidate': {**candidate, 'ask': str(candidate['ask']), 'bid': str(candidate['bid']), 'spread': str(candidate['spread']), 'ask_depth': str(candidate['ask_depth']), 'bid_depth': str(candidate['bid_depth'])},
            'open_orders_before_count': len(open_before) if isinstance(open_before, list) else None,
            'balances': {'before_token_shares': str(before_token), 'before_collateral': before_collateral},
            'buy': {},
            'sell': {},
        }
        if writer:
            await writer.log_strategy_event(STRATEGY_ID, f'MANUAL TEST {tag}: Prism2 placing ~$1 BUY then immediate SELL on {candidate["slug"]} {candidate["outcome"]} ask={candidate["ask"]} bid={candidate["bid"]}', level='WARN')
        buy_resp = ex.submit(PolyOrder(token_id=candidate['token'], side='BUY', price=buy_cap, size=buy_size, order_type='FAK', post_only=False, use_limit_order=False, tick_size=str(tick), neg_risk=bool(candidate['neg_risk'])))
        result['buy'] = {'limit_price': str(buy_cap), 'requested_size': str(buy_size), 'response': buy_resp, 'order_id': order_id(buy_resp), 'status': status_from(buy_resp)}
        matched_buy_order = None
        if result['buy']['order_id']:
            for _ in range(8):
                try:
                    matched_buy_order = ex.get_order(result['buy']['order_id'])
                    if is_matched(matched_buy_order):
                        break
                except Exception as e:
                    matched_buy_order = {'error': repr(e)}
                await asyncio.sleep(1)
        bought_shares = Decimal('0')
        for _ in range(14):
            cur = D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance', '0')) / Decimal(10) ** 6
            bought_shares = max(Decimal('0'), cur - before_token)
            if bought_shares > 0:
                break
            await asyncio.sleep(1)
        result['buy']['get_order'] = matched_buy_order
        result['buy']['bought_shares_from_wallet_delta'] = str(q4(bought_shares))
        if bought_shares <= 0:
            result['sell'] = {'status': 'skipped', 'reason': 'no token balance appeared after BUY'}
        else:
            book = requests.get('https://clob.polymarket.com/book', params={'token_id': candidate['token']}, timeout=10).json()
            bids, _ = level_lists(book)
            best_bid = max(p for p, _ in bids) if bids else Decimal('0')
            sell_size = q4(bought_shares)
            sell_px = max(Decimal('0.001'), best_bid.quantize(tick, rounding=ROUND_DOWN))
            sell_resp = ex.submit(PolyOrder(token_id=candidate['token'], side='SELL', price=sell_px, size=sell_size, order_type='FOK', post_only=False, use_limit_order=True, tick_size=str(tick), neg_risk=bool(candidate['neg_risk'])))
            result['sell'] = {'limit_price': str(sell_px), 'size': str(sell_size), 'response': sell_resp, 'order_id': order_id(sell_resp), 'status': status_from(sell_resp)}
            matched_sell_order = None
            if result['sell']['order_id']:
                for _ in range(8):
                    try:
                        matched_sell_order = ex.get_order(result['sell']['order_id'])
                        if is_matched(matched_sell_order):
                            break
                    except Exception as e:
                        matched_sell_order = {'error': repr(e)}
                    await asyncio.sleep(1)
            result['sell']['get_order'] = matched_sell_order
        after_token = D((clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=candidate['token'])) or {}).get('balance', '0')) / Decimal(10) ** 6
        after_collateral = clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        open_after = ex.open_orders()
        result['balances'].update({'after_token_shares': str(after_token), 'residual_delta_shares': str(q4(after_token - before_token)), 'after_collateral': after_collateral})
        result['open_orders_after_count'] = len(open_after) if isinstance(open_after, list) else None
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(result, indent=2, default=str))
        ok = result['buy'].get('status') in {'filled', 'submitted'} and result['sell'].get('status') in {'filled', 'submitted'} and q4(after_token - before_token) <= Decimal('0.0001')
        if writer:
            await writer.log_strategy_event(STRATEGY_ID, f'MANUAL TEST {tag}: Prism2 completed buy_status={result["buy"].get("status")} sell_status={result["sell"].get("status")} residual_delta={result["balances"]["residual_delta_shares"]} report={REPORT_PATH}', level='INFO' if ok else 'ERROR')
        print(json.dumps(result, indent=2, default=str))
    finally:
        if writer:
            await writer.close()

if __name__ == '__main__':
    asyncio.run(main())
