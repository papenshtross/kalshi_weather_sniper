#!/usr/bin/env python3
from __future__ import annotations
import argparse, asyncio, json, os, time, requests
from collections import defaultdict
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from dotenv import load_dotenv
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
from polybot.adapters.polymarket.execution import PolyOrder, PolymarketExecutionClient
from polybot.persistence.writer import PolybotWriter

DATA_API='https://data-api.polymarket.com/trades'
CLOB_BOOK='https://clob.polymarket.com/book'
SESSION=requests.Session(); SESSION.headers.update({'User-Agent':'Prism-live-hf-micro-flow/0.1'})

def D(x): return Decimal(str(x))
def q4(x): return D(x).quantize(Decimal('0.0001'), rounding=ROUND_DOWN)
def parse_jsonish(v, default):
    if isinstance(v,str):
        try: return json.loads(v)
        except Exception: return default
    return v if v is not None else default

def infer_category(title, slug=''):
    s=(title+' '+slug).lower(); sports=['nba','nfl','nhl','mlb','ncaab','ufc','tennis','soccer','football','vs.',' v ', 'fifa','baseball','basketball','hockey']
    crypto=['bitcoin','btc','ethereum','eth','solana',' sol ','xrp','doge','crypto','up or down']
    esports=['esports','league of legends','lol','valorant','counter-strike','cs2','dota']
    if any(x in s for x in crypto): return 'crypto'
    if any(x in s for x in esports): return 'esports'
    if any(x in s for x in sports): return 'sports'
    return 'other-liquid'

def fetch_recent(limit=500, pages=4):
    out=[]; seen=set()
    for offset in range(0, limit*pages, limit):
        try: rows=SESSION.get(DATA_API, params={'limit':limit,'offset':offset}, timeout=20).json()
        except Exception: break
        if not rows: break
        for r in rows:
            if not r.get('asset') or r.get('price') is None or not r.get('timestamp'): continue
            key=(r.get('transactionHash'),r.get('asset'),r.get('timestamp'),r.get('price'),r.get('size'))
            if key in seen: continue
            seen.add(key); out.append(r)
        time.sleep(0.05)
    out.sort(key=lambda x:int(x['timestamp']))
    return out

def signal_direction(rows, i, v):
    k=int(v['lookback_trades']); th=float(v['threshold']); mode=v['mode']
    prices=[float(r['price']) for r in rows]; sizes=[float(r.get('size') or 0) for r in rows]
    if sizes[i] < float(v.get('min_trade_size',1)): return 0
    p=prices[i]; prev=prices[i-k]
    if not (0.03 <= p <= 0.97): return 0
    delta=p-prev; vol=sum(sizes[max(0,i-k):i+1]); avgvol=sum(sizes[max(0,i-5*k):i+1])/max(1,min(i+1,5*k))
    if mode=='momentum' and delta >= th: return 1
    if mode=='volatility_breakout' and abs(delta) >= th and vol >= avgvol*k*float(v.get('vol_mult',1.2))*0.4 and delta > 0: return 1
    return 0

def levels(book):
    bids=[(D(x['price']),D(x['size'])) for x in book.get('bids',[]) if D(x.get('size',0))>0]
    asks=[(D(x['price']),D(x['size'])) for x in book.get('asks',[]) if D(x.get('size',0))>0]
    return bids, asks

def order_id(resp):
    if not isinstance(resp,dict): return None
    for k in ('orderID','order_id','id'):
        if resp.get(k): return str(resp[k])
    if isinstance(resp.get('order'),dict): return order_id(resp['order'])
    return None

def status_from(resp):
    raw=str((resp or {}).get('status') or '').lower()
    if raw in {'matched','filled'}: return 'filled'
    if raw in {'delayed','pending','live'}: return 'submitted'
    if (resp or {}).get('success') is True and order_id(resp): return 'submitted'
    if (resp or {}).get('success') is False: return 'rejected'
    return raw or 'submitted'

async def db_exec(writer, sql, *args):
    async with writer._pool.acquire() as con:
        return await con.execute(sql, *args)
async def db_fetchval(writer, sql, *args):
    async with writer._pool.acquire() as con:
        return await con.fetchval(sql, *args)
async def db_fetchrow(writer, sql, *args):
    async with writer._pool.acquire() as con:
        return await con.fetchrow(sql, *args)

async def record_attempt(writer, sid, market_slug, token, outcome, side, order_type, price, size, stake, status, response, signal, cfg, err=None):
    await db_exec(writer,"""
    INSERT INTO order_attempts(strategy_id,market_slug,token,outcome,side,order_type,price,size,stake_usd,status,response,error,signal,config)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13::jsonb,$14::jsonb)
    """,sid,market_slug,str(token),outcome,side,order_type,float(price),float(size),float(stake),status,json.dumps(response or {}),err,json.dumps(signal or {}),json.dumps(cfg or {}))

async def place_buy_sell_test(sid, writer, ex, cfg, tag='startup_test'):
    # Pick a cheap liquid active token to keep loss small, independent of strategy signal.
    markets=SESSION.get('https://gamma-api.polymarket.com/markets',params={'active':'true','closed':'false','limit':200,'order':'volume24hr','ascending':'false'},timeout=20).json()
    req=[]; meta={}
    for m in markets:
        if not m.get('acceptingOrders', True): continue
        toks=parse_jsonish(m.get('clobTokenIds'),[]); outs=parse_jsonish(m.get('outcomes'),['Yes','No'])
        for i,tok in enumerate(toks[:2]):
            tok=str(tok); req.append({'token_id':tok}); meta[tok]={'slug':m.get('slug'),'title':m.get('question') or m.get('title'),'outcome':outs[i] if i<len(outs) else str(i),'tick':str(m.get('orderPriceMinTickSize') or m.get('minimumTickSize') or '0.01'),'neg_risk':bool(m.get('negRisk') or False),'order_min_size':D(m.get('orderMinSize') or 5)}
    cand=[]
    for i in range(0,len(req),100):
        books=SESSION.post('https://clob.polymarket.com/books',json=req[i:i+100],timeout=10).json()
        for b in books or []:
            tok=str(b.get('asset_id') or b.get('token_id') or '')
            if tok not in meta: continue
            bids,asks=levels(b)
            if not bids or not asks: continue
            bid=max(p for p,s in bids); ask=min(p for p,s in asks); spread=ask-bid
            shares=D('1')/ask; ask_depth=sum(s for p,s in asks if p<=ask); bid_depth=sum(s for p,s in bids if p>=bid)
            if D('0.02')<=ask<=D('0.20') and spread<=D('0.01') and shares>=meta[tok]['order_min_size'] and ask_depth>=meta[tok]['order_min_size'] and bid_depth>=meta[tok]['order_min_size']:
                cand.append((spread,ask,tok,bid,meta[tok]))
    if not cand:
        await writer.log_strategy_event(sid, f'MANUAL TEST {tag}: no cheap liquid candidate found', 'ERROR'); return {'ok':False,'reason':'no_candidate'}
    spread,ask,tok,bid,m=sorted(cand)[0]; tick=D(m['tick']); cap=(ask+max(tick,D('0.001'))).quantize(tick, rounding=ROUND_UP); size=q4(D('1')/cap)
    await writer.log_strategy_event(sid, f'MANUAL TEST {tag}: BUY $1 then SELL {m["slug"]} {m["outcome"]} via {cfg.get("wallet_name")}', 'WARN')
    before=D((ex.http.clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tok)) or {}).get('balance','0'))/D(10)**6
    try:
        buy=ex.submit(PolyOrder(token_id=tok,side='BUY',price=cap,size=size,order_type='FAK',use_limit_order=False,tick_size=str(tick),neg_risk=bool(m['neg_risk'])))
    except Exception as e:
        err={'error':repr(e)}
        await record_attempt(writer,sid,m['slug'],tok,str(m['outcome']),'BUY','FAK',cap,size,D('1'),'rejected',err,{'tag':tag},cfg,repr(e))
        await writer.log_strategy_event(sid, f'MANUAL TEST {tag}: BUY rejected wallet={cfg.get("wallet_name")} error={repr(e)[:500]}', 'ERROR')
        return {'ok':False,'stage':'buy','error':repr(e)}
    await record_attempt(writer,sid,m['slug'],tok,str(m['outcome']),'BUY','FAK',cap,size,D('1'),status_from(buy),buy,{'tag':tag},cfg)
    await asyncio.sleep(6)
    after=D((ex.http.clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tok)) or {}).get('balance','0'))/D(10)**6
    bought=max(D('0'), after-before)
    if bought<=0:
        await writer.log_strategy_event(sid, f'MANUAL TEST {tag}: BUY did not change token balance order_id={order_id(buy)}', 'ERROR'); return {'ok':False,'buy':buy,'bought':str(bought)}
    b=SESSION.get(CLOB_BOOK, params={'token_id':tok}, timeout=10).json(); bids,asks=levels(b); best_bid=max(p for p,s in bids); sell_size=q4(bought); sell_px=best_bid.quantize(tick, rounding=ROUND_DOWN)
    sell=ex.submit(PolyOrder(token_id=tok,side='SELL',price=sell_px,size=sell_size,order_type='FOK',use_limit_order=True,tick_size=str(tick),neg_risk=bool(m['neg_risk'])))
    await record_attempt(writer,sid,m['slug'],tok,str(m['outcome']),'SELL','FOK',sell_px,sell_size,sell_size*sell_px,status_from(sell),sell,{'tag':tag},cfg)
    await writer.record_fill(sid, int(time.time()*1000)%9223372036854775807, f'{m["title"]} [TEST]', 'BUY', float(cap), float(bought), 'MANUAL_TEST_BUY')
    await writer.record_fill(sid, (int(time.time()*1000)+1)%9223372036854775807, f'{m["title"]} [TEST]', 'SELL', float(sell_px), float(sell_size), 'MANUAL_TEST_SELL')
    await writer.log_strategy_event(sid, f'MANUAL TEST {tag}: completed buy={status_from(buy)} sell={status_from(sell)} bought={q4(bought)} wallet={cfg.get("wallet_name")}', 'INFO')
    return {'ok':True,'buy_order_id':order_id(buy),'sell_order_id':order_id(sell),'bought':str(q4(bought))}

async def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--strategy-id',required=True); ap.add_argument('--wallet-env'); ap.add_argument('--test-once',action='store_true'); ap.add_argument('--loop',action='store_true')
    args=ap.parse_args()
    for f in ['/home/administrator/projects/polybot/.env','/home/administrator/projects/polybot/.env.live','/home/administrator/projects/polybot-dash/.env.local', args.wallet_env]:
        if f and Path(f).exists(): load_dotenv(f, override=True)
    dsn=os.getenv('NAUTILUS_DB_URL') or os.getenv('POSTGRES_URL') or os.getenv('DATABASE_URL')
    writer=PolybotWriter(dsn); await writer.connect()
    cfg=await writer.get_strategy_config(args.strategy_id)
    ex=PolymarketExecutionClient()
    if args.test_once:
        res=await place_buy_sell_test(args.strategy_id, writer, ex, cfg, 'requested_test'); print(json.dumps(res,indent=2,default=str)); await writer.close(); return
    await writer.set_strategy_status(args.strategy_id,'running')
    await writer.log_strategy_event(args.strategy_id, f"HF live runner started wallet={cfg.get('wallet_name')} order=${cfg.get('order_size_usd')} daily_limit=${cfg.get('daily_order_limit_usd')}", 'INFO')
    state_path=Path(cfg.get('state_path') or f'/home/administrator/projects/polybot/data/live_state_{args.strategy_id}.json'); state_path.parent.mkdir(parents=True, exist_ok=True)
    state=json.loads(state_path.read_text()) if state_path.exists() else {'open':{},'seen_signals':[]}
    while True:
        try:
            st=await db_fetchrow(writer, 'select status, config from strategies where id=$1', args.strategy_id)
            if not st or st['status']!='running':
                await asyncio.sleep(5); continue
            cfg=dict(st['config']) if isinstance(st['config'],dict) else json.loads(st['config'])
            daily=float(await db_fetchval(writer,"select coalesce(sum(stake_usd),0) from order_attempts where strategy_id=$1 and side='BUY' and status in ('filled','submitted') and ts>now()-interval '24 hours'",args.strategy_id) or 0)
            trades=fetch_recent(int(cfg.get('trade_fetch_page',500)), int(cfg.get('trade_fetch_pages',3)))
            by=defaultdict(list)
            for t in trades: by[str(t['asset'])].append(t)
            try:
                collateral = ex.http.clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=int(os.getenv('POLYMARKET_SIGNATURE_TYPE', cfg.get('signature_type', 1) or 1))))
                wallet_balance = float(D((collateral or {}).get('balance','0')) / D(10)**6)
            except Exception:
                wallet_balance = None
            placed=False
            allowed=set(cfg.get('category_filter') or [])
            for asset, rows in sorted(by.items(), key=lambda kv: int(kv[1][-1]['timestamp']), reverse=True):
                title=rows[-1].get('title',''); slug=rows[-1].get('slug') or rows[-1].get('eventSlug') or ''
                if allowed and infer_category(title,slug) not in allowed: continue
                k=int(cfg['lookback_trades']); hold=int(cfg['hold_trades'])
                if len(rows)<k+hold+5: continue
                i=len(rows)-hold-2
                if i<k or signal_direction(rows,i,cfg)!=1: continue
                sig_key=f"{asset}:{rows[i].get('transactionHash')}:{rows[i].get('timestamp')}:{rows[i].get('price')}"
                if sig_key in state.get('seen_signals',[]): continue
                if daily + float(cfg.get('order_size_usd',1)) > float(cfg.get('daily_order_limit_usd',30)):
                    continue
                book=SESSION.get(CLOB_BOOK, params={'token_id':asset}, timeout=8).json(); bids,asks=levels(book)
                if not bids or not asks: continue
                bid=max(p for p,s in bids); ask=min(p for p,s in asks); spread=ask-bid
                ask_depth=sum(p*s for p,s in asks if p<=ask+D('0.02')); bid_depth=sum(p*s for p,s in bids if p>=bid-D('0.02'))
                if spread>D(str(cfg.get('max_spread',0.03))) or min(ask_depth,bid_depth)<D(str(cfg.get('min_depth_usd',25))): continue
                stake=D(str(cfg.get('order_size_usd',1))); tick=D(str(cfg.get('tick_size','0.01'))); cap=(ask+max(tick,D('0.001'))).quantize(tick, rounding=ROUND_UP); size=q4(stake/cap)
                signal={'signal_key':sig_key,'title':title,'slug':slug,'asset':asset,'price':rows[i].get('price'),'mode':cfg.get('mode'),'wallet_name':cfg.get('wallet_name')}
                try:
                    resp=ex.submit(PolyOrder(token_id=asset,side='BUY',price=cap,size=size,order_type='FAK',use_limit_order=False,tick_size=str(tick),neg_risk=False))
                    status=status_from(resp); err=None
                except Exception as e:
                    resp={'error':repr(e)}; status='rejected'; err=repr(e)
                await record_attempt(writer,args.strategy_id,slug,asset,'', 'BUY','FAK',cap,size,stake,status,resp,signal,cfg,err)
                await writer.log_strategy_event(args.strategy_id, f"LIVE BUY signal {title[:80]} px={cap} stake=${stake} wallet={cfg.get('wallet_name')} status={status}{(' error='+err[:200]) if err else ''}", 'WARN')
                state.setdefault('seen_signals',[]).append(sig_key); state['seen_signals']=state['seen_signals'][-500:]
                state['open'][asset]={'asset':asset,'slug':slug,'title':title,'entry_ts':int(time.time()),'entry_price':str(cap),'size':str(size),'hold_trades':hold}
                placed=True; break
            # simple exit: sell any held balance after min hold seconds
            for asset,pos in list(state.get('open',{}).items()):
                if time.time()-float(pos.get('entry_ts',0)) < int(cfg.get('min_hold_seconds',15)): continue
                bal=D((ex.http.clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=asset)) or {}).get('balance','0'))/D(10)**6
                if bal<=0: state['open'].pop(asset,None); continue
                book=SESSION.get(CLOB_BOOK, params={'token_id':asset}, timeout=8).json(); bids,asks=levels(book)
                if not bids: continue
                tick=D(str(cfg.get('tick_size','0.01'))); px=max(D('0.001'), max(p for p,s in bids).quantize(tick, rounding=ROUND_DOWN)); size=q4(bal)
                try:
                    resp=ex.submit(PolyOrder(token_id=asset,side='SELL',price=px,size=size,order_type='FOK',use_limit_order=True,tick_size=str(tick),neg_risk=False))
                    status=status_from(resp); err=None
                except Exception as e:
                    resp={'error':repr(e)}; status='rejected'; err=repr(e)
                await record_attempt(writer,args.strategy_id,pos.get('slug',''),asset,'','SELL','FOK',px,size,px*size,status,resp,{'exit':'hold_elapsed'},cfg,err)
                await writer.log_strategy_event(args.strategy_id, f"LIVE SELL exit {pos.get('title','')[:80]} px={px} size={size} wallet={cfg.get('wallet_name')} status={status}{(' error='+err[:200]) if err else ''}", 'WARN')
                state['open'].pop(asset,None)
            state_path.write_text(json.dumps(state,indent=2))
            async with writer._pool.acquire() as con:
                await con.execute(
                    """
                    UPDATE strategies
                    SET config = jsonb_strip_nulls(config || $2::jsonb), updated_at=now()
                    WHERE id=$1
                    """,
                    args.strategy_id,
                    json.dumps({
                        'last_heartbeat_at': int(time.time() * 1000),
                        'last_daily_buy_usd': round(daily, 6),
                        'last_open_count': len(state.get('open',{})),
                        'last_scan_trade_count': len(trades),
                        'last_scan_asset_count': len(by),
                        'last_wallet_balance': wallet_balance,
                        'wallet_proxy': os.getenv('POLYMARKET_PROXY_ADDRESS') or cfg.get('wallet_proxy') or cfg.get('proxy_address'),
                    }),
                )
        except Exception as e:
            await writer.log_strategy_event(args.strategy_id, f"runner error: {repr(e)[:800]}", 'ERROR')
        await asyncio.sleep(float(cfg.get('poll_seconds',15)))

if __name__=='__main__': asyncio.run(main())
