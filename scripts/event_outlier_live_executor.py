#!/usr/bin/env python3
"""Strict guarded live CLOB executor for event-outlier weather pump scanner signals.

This script is intentionally small and conservative:
- reads scanner-exported candidate signals;
- requires DB status=running plus config.live_trading=true and live_launch_armed=true;
- places at most max_live_orders_per_scan capped FAK/IOC BUY YES orders;
- records every attempt in order_attempts and strategy_logs;
- does not synthesize fills unless CLOB/get_order reports matched size.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import re
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

POLYBOT_DIR = Path('/home/administrator/projects/polybot')
sys.path.insert(0, str(POLYBOT_DIR))

from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient  # noqa: E402
try:  # noqa: E402
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
except Exception:  # pragma: no cover
    AssetType = None
    BalanceAllowanceParams = None


def _load_env() -> str:
    load_dotenv(POLYBOT_DIR / '.env')
    load_dotenv(POLYBOT_DIR / '.env.live', override=True)
    load_dotenv('/home/administrator/projects/polybot-dash/.env.local', override=True)
    dsn = os.getenv('POSTGRES_URL') or os.getenv('DATABASE_URL') or os.getenv('NAUTILUS_DB_URL')
    if not dsn:
        raise SystemExit('missing POSTGRES_URL/DATABASE_URL')
    return dsn


def _dec(v: Any, default: str = '0') -> Decimal:
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal(default)


def _floor_size(size: Decimal) -> Decimal:
    return size.quantize(Decimal('0.0001'), rounding=ROUND_DOWN)


def _boolish(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _order_id(resp: dict[str, Any]) -> str | None:
    for k in ('orderID', 'order_id', 'id'):
        if resp.get(k):
            return str(resp[k])
    nested = resp.get('order') if isinstance(resp.get('order'), dict) else None
    if nested:
        return _order_id(nested)
    return None


def _status_from_response(resp: dict[str, Any]) -> str:
    raw = str(resp.get('status') or '').lower()
    if raw in {'matched', 'filled'}:
        return 'filled'
    if raw in {'delayed', 'pending', 'live'}:
        return 'submitted'
    if resp.get('success') is True and _order_id(resp):
        return 'submitted'
    if resp.get('success') is False:
        return 'rejected'
    return raw or 'submitted'


def _matched_size_from_order(order: dict[str, Any] | None) -> Decimal:
    if not order:
        return Decimal('0')
    for k in ('size_matched', 'sizeMatched', 'matched_size', 'matchedSize'):
        if order.get(k) not in (None, ''):
            return _dec(order.get(k))
    return Decimal('0')


def _slugify_city(value: Any) -> str:
    return re.sub(r'[^a-z0-9]+', '-', str(value or '').strip().lower().replace('_', '-')).strip('-')


def _city_from_signal(sig: dict[str, Any]) -> str:
    for key in ('city_slug', 'city', 'weather_city'):
        city = _slugify_city(sig.get(key))
        if city:
            return city
    slug = str(sig.get('event_slug') or sig.get('market_slug') or '')
    m = re.search(r'high(?:est)?-temperature-in-(.+?)-on-', slug)
    return _slugify_city(m.group(1) if m else '')


def _collateral_balance_usdc(client: PolymarketExecutionClient) -> Decimal | None:
    if AssetType is None or BalanceAllowanceParams is None:
        return None
    try:
        bal = client.http.clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)) or {}
        raw = bal.get('balance')
        return _dec(raw) / Decimal('1000000') if raw is not None else None
    except Exception:
        return None


async def main() -> None:
    import asyncpg

    parser = argparse.ArgumentParser()
    parser.add_argument('--strategy-id', default='live_event_outlier_weather_pump_v1')
    parser.add_argument('--signals', required=True)
    args = parser.parse_args()

    dsn = _load_env()
    conn = await asyncpg.connect(dsn)
    try:
        row = await conn.fetchrow('SELECT status, config FROM strategies WHERE id=$1', args.strategy_id)
        if not row:
            raise SystemExit(f'strategy missing: {args.strategy_id}')
        cfg = row['config']
        if isinstance(cfg, str):
            cfg = json.loads(cfg)
        if row['status'] != 'running':
            print(json.dumps({'executed': 0, 'reason': f"status={row['status']}"}))
            return
        if cfg.get('live_trading') is not True or cfg.get('live_launch_armed') is not True:
            print(json.dumps({'executed': 0, 'reason': 'live_trading/live_launch_armed not both true'}))
            return

        signals_path = Path(args.signals)
        data = json.loads(signals_path.read_text()) if signals_path.exists() else {}
        signals = [s for s in data.get('signals', []) if s.get('candidate') and s.get('paper_order')]
        if not signals:
            print(json.dumps({'executed': 0, 'reason': 'no candidate signals'}))
            return

        max_orders = max(0, int(cfg.get('max_live_orders_per_scan', 1)))
        max_orders_24h = max(0, int(cfg.get('max_live_orders_per_24h', 50)))
        recent_attempts_24h = int(await conn.fetchval("SELECT count(*) FROM order_attempts WHERE strategy_id=$1 AND side='BUY' AND ts > now() - interval '24 hours'", args.strategy_id) or 0)
        remaining_24h = max(0, max_orders_24h - recent_attempts_24h)
        max_orders = min(max_orders, remaining_24h)
        if max_orders <= 0:
            print(json.dumps({'executed': 0, 'reason': '24h order cap reached', 'recent_attempts_24h': recent_attempts_24h, 'max_orders_per_24h': max_orders_24h}))
            return
        max_notional = _dec(cfg.get('max_notional_usdc', 2), '2')
        max_notional_24h = _dec(cfg.get('max_live_notional_per_24h', cfg.get('daily_limit_usd', 0)), '0')
        recent_notional_24h = _dec(await conn.fetchval(
            """
            SELECT COALESCE(SUM(COALESCE(stake_usd,0)),0)::text
            FROM order_attempts
            WHERE strategy_id=$1 AND side='BUY' AND ts > now() - interval '24 hours'
              AND (status IN ('filled','submitted','matched','delayed') OR response->>'success'='true')
            """,
            args.strategy_id,
        ), '0')
        remaining_notional_24h = max(Decimal('0'), max_notional_24h - recent_notional_24h) if max_notional_24h > 0 else None
        if remaining_notional_24h is not None and remaining_notional_24h < _dec(cfg.get('min_market_buy_usdc', 1), '1'):
            print(json.dumps({'executed': 0, 'reason': '24h notional cap reached', 'recent_notional_24h': str(recent_notional_24h), 'max_live_notional_per_24h': str(max_notional_24h)}))
            return
        min_market_buy_usdc = _dec(cfg.get('min_market_buy_usdc', 1), '1')
        max_entry_price = _dec(cfg.get('max_entry_price', 0.02), '0.02')
        min_shares = _dec(cfg.get('min_order_size_shares', 5), '5')
        order_type = str(cfg.get('order_type') or 'FAK').upper()
        if order_type not in {'FAK', 'FOK', 'IOC'}:
            order_type = 'FAK'
        if order_type == 'IOC':  # py-clob names immediate partial fill as FAK.
            order_type = 'FAK'

        client = PolymarketExecutionClient()
        balance = _collateral_balance_usdc(client)
        executed = 0
        attempted = 0
        skipped: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []

        for sig in signals:
            # max_live_orders_per_scan is a hard submission-attempt cap, not only
            # a success cap. If CLOB rejects one candidate (e.g. auth/signature),
            # do not spray every other candidate in the same scan.
            if attempted >= max_orders:
                break
            po = sig['paper_order']
            token = str(po.get('token_id') or sig.get('yes_token_id') or '')
            market_slug = str(sig.get('market_slug') or '')
            city_slug = _city_from_signal(sig)
            if not token or not market_slug:
                skipped.append({'market_slug': market_slug, 'reason': 'missing token/slug'})
                continue
            if int(cfg.get('max_active_trades_per_city', 0) or 0) == 1 and city_slug:
                open_city_size = await conn.fetchval(
                    """
                    SELECT COALESCE(SUM(CASE
                      WHEN side='BUY' AND (status IN ('filled','submitted','matched','delayed') OR response->>'success'='true') THEN COALESCE(size,0)
                      WHEN side='SELL' AND (status IN ('filled','submitted','matched','delayed') OR response->>'success'='true') THEN -COALESCE(size,0)
                      ELSE 0 END), 0)::float
                    FROM order_attempts
                    WHERE strategy_id=$1
                      AND COALESCE(signal->>'city_slug', signal->>'city', signal->>'weather_city', regexp_replace(market_slug, '^.*temperature-in-([^/]+?)-on-.*$', '\\1')) = $2
                    """,
                    args.strategy_id,
                    city_slug,
                )
                if float(open_city_size or 0.0) > 0.000001:
                    skipped.append({'market_slug': market_slug, 'city': city_slug, 'reason': 'city already has active/open trade'})
                    continue
            prev = await conn.fetchval(
                """
                SELECT count(*) FROM order_attempts
                WHERE strategy_id=$1 AND token=$2 AND side='BUY'
                  AND status IN ('submitted','filled','matched','delayed')
                  AND ts > now() - interval '48 hours'
                """,
                args.strategy_id, token,
            )
            if prev and int(prev) > 0:
                skipped.append({'market_slug': market_slug, 'reason': 'already has recent submitted/filled BUY'})
                continue

            price = min(_dec(po.get('limit_price'), '0'), max_entry_price)
            if price <= 0 or price > max_entry_price:
                skipped.append({'market_slug': market_slug, 'reason': f'bad price {price}'})
                continue
            depth_shares = _dec(sig.get('eligible_depth_shares'), '0')
            depth_usdc = _dec(sig.get('eligible_depth_usdc'), '0')
            notional = min(max_notional, _dec(po.get('max_notional_usdc'), str(max_notional)), depth_usdc)
            if remaining_notional_24h is not None:
                notional = min(notional, remaining_notional_24h)
            size = _floor_size(notional / price) if price > 0 else Decimal('0')
            if depth_shares > 0:
                size = min(size, _floor_size(depth_shares))
            if size < min_shares:
                skipped.append({'market_slug': market_slug, 'reason': f'size {size} below min {min_shares}'})
                continue
            notional = (size * price).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            if notional < min_market_buy_usdc:
                skipped.append({'market_slug': market_slug, 'reason': f'notional {notional} below market-buy minimum {min_market_buy_usdc}'})
                continue
            if balance is not None and notional > balance:
                skipped.append({'market_slug': market_slug, 'reason': f'insufficient pUSD balance {balance} for {notional}'})
                continue

            attempted += 1
            order = PolyOrder(
                token_id=token,
                side='BUY',
                price=price,
                size=size,
                order_type=order_type,
                post_only=False,
                use_limit_order=False,
                tick_size=str(sig.get('tick_size') or cfg.get('tick_size') or '0.001'),
                neg_risk=_boolish(sig.get('neg_risk'), _boolish(cfg.get('neg_risk'), False)),
                builder_code=str(cfg.get('builder_code') or os.getenv('POLYMARKET_BUILDER_CODE') or os.getenv('POLY_BUILDER_CODE') or '') or None,
            )
            attempt_cfg = {
                'source': 'event_outlier_live_executor',
                'max_orders_per_scan': max_orders,
                'max_orders_per_24h': max_orders_24h,
                'recent_attempts_24h_before_submit': recent_attempts_24h,
                'max_entry_price': str(max_entry_price),
                'max_notional_usdc': str(max_notional),
                'min_market_buy_usdc': str(min_market_buy_usdc),
                'live_trading': True,
                'live_launch_armed': True,
            }
            try:
                resp = client.submit(order)
                oid = _order_id(resp if isinstance(resp, dict) else {})
                status = _status_from_response(resp if isinstance(resp, dict) else {'raw': resp})
                get_order_resp = None
                matched = Decimal('0')
                if oid:
                    time.sleep(2)
                    try:
                        get_order_resp = client.get_order(oid)
                        matched = _matched_size_from_order(get_order_resp)
                        if matched > 0:
                            status = 'filled'
                    except Exception as exc:
                        get_order_resp = {'error': str(exc)}
                response = {'submit': resp, 'order_id': oid, 'get_order': get_order_resp}
                await conn.execute(
                    """
                    INSERT INTO order_attempts(strategy_id, market_slug, token, outcome, side, order_type, price, size, stake_usd, status, response, signal, config)
                    VALUES($1,$2,$3,'YES','BUY',$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb)
                    """,
                    args.strategy_id, market_slug, token, order_type, price, size, notional, status,
                    json.dumps(response), json.dumps(sig), json.dumps(attempt_cfg),
                )
                if matched > 0:
                    px = price
                    next_fill_id = await conn.fetchval("SELECT coalesce(max(id), 0) + 1 FROM fills")
                    await conn.execute(
                        """
                        INSERT INTO fills(strategy_id, id, ts, market, side, px, size, kind)
                        VALUES($1, $2, now(), $3, 'BUY', $4, $5, 'EVENT_OUTLIER_WEATHER')
                        """,
                        args.strategy_id, int(next_fill_id), market_slug, px, matched,
                    )
                await conn.execute(
                    "INSERT INTO strategy_logs(strategy_id, level, message, ts) VALUES($1,'WARN',$2,now())",
                    args.strategy_id,
                    f"LIVE CLOB BUY submitted market={market_slug} token={token[:12]}... price<={price} size={size} notional≈{notional} status={status} order_id={oid}",
                )
                executed += 1
                if remaining_notional_24h is not None:
                    remaining_notional_24h = max(Decimal('0'), remaining_notional_24h - notional)
                results.append({'market_slug': market_slug, 'city': city_slug, 'order_id': oid, 'status': status, 'size': str(size), 'price': str(price), 'notional': str(notional)})
            except Exception as exc:
                await conn.execute(
                    """
                    INSERT INTO order_attempts(strategy_id, market_slug, token, outcome, side, order_type, price, size, stake_usd, status, response, error, signal, config)
                    VALUES($1,$2,$3,'YES','BUY',$4,$5,$6,$7,'rejected','{}'::jsonb,$8,$9::jsonb,$10::jsonb)
                    """,
                    args.strategy_id, market_slug, token, order_type, price, size, notional, str(exc)[:1000], json.dumps(sig), json.dumps(attempt_cfg),
                )
                await conn.execute(
                    "INSERT INTO strategy_logs(strategy_id, level, message, ts) VALUES($1,'ERROR',$2,now())",
                    args.strategy_id, f"LIVE CLOB BUY failed market={market_slug}: {str(exc)[:700]}",
                )
                results.append({'market_slug': market_slug, 'status': 'rejected', 'error': str(exc)[:300]})

        print(json.dumps({'executed': executed, 'attempted': attempted, 'results': results, 'skipped': skipped[:10]}, indent=2))
    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
