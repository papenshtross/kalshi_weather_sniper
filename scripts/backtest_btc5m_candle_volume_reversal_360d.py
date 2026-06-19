#!/usr/bin/env python3
"""BTC 5m Polymarket-style candle succession + volume reversal sweep.

Research-only. Fetches Binance BTCUSDT 5m OHLCV for the last N days and treats each
closed 5m candle as one Polymarket BTC up/down market:
- price_to_beat = candle open
- resolution = UP if candle close > open, else DOWN
- entry signal is computed at the current candle open from previous candles only.

Base rule: previous candle green => bet UP; previous candle red => bet DOWN.
Exception: if previous candle has a volume spike relative to average of previous n
candles, flip to reversal. 1000 variants sweep n, spike multiple x, streak gates,
body/wick confirmation, and long-streak exhaustion gates while still issuing a call
for every eligible market.
"""
from __future__ import annotations

import argparse, csv, json, math, random, time, urllib.parse, urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import mean, median

BINANCE = "https://api.binance.com/api/v3/klines"
EPS = 1e-12

@dataclass(frozen=True)
class Variant:
    id: int
    vol_n: int
    spike_x: float
    min_body_pct: float
    spike_body_pct: float
    streak_min_for_reversal: int
    streak_max_continue: int
    require_spike_same_as_streak: bool
    wick_exhaustion: float
    exhaustion_reversal: bool
    use_taker_confirmation: bool

@dataclass
class Result:
    variant_id: int
    params: dict
    trades: int
    wins: int
    losses: int
    hit_rate: float
    train_trades: int
    train_hit_rate: float
    test_trades: int
    test_hit_rate: float
    flips: int
    flip_rate: float
    flip_hit_rate: float
    continuation_hit_rate: float
    avg_spike_x_on_flips: float
    score: float


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).isoformat()


def fetch_klines(days: int, out_csv: Path, refresh: bool = False) -> list[dict]:
    if out_csv.exists() and not refresh:
        return read_csv(out_csv)
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(days=days)
    rows = []
    cur = ms(start)
    end_ms = ms(end)
    while cur < end_ms:
        qs = urllib.parse.urlencode({"symbol":"BTCUSDT", "interval":"5m", "startTime":cur, "endTime":end_ms, "limit":1000})
        with urllib.request.urlopen(f"{BINANCE}?{qs}", timeout=20) as r:
            batch = json.loads(r.read().decode())
        if not batch:
            break
        for k in batch:
            rows.append({
                "open_time": int(k[0]), "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
                "close_time": int(k[6]), "quote_volume": float(k[7]), "trades": int(k[8]),
                "taker_buy_base": float(k[9]), "taker_buy_quote": float(k[10]),
            })
        nxt = int(batch[-1][0]) + 5*60*1000
        if nxt <= cur: break
        cur = nxt
        time.sleep(0.03)
    # drop still-open candle if any and dedupe
    dedup = {r["open_time"]: r for r in rows if r["close_time"] < end_ms}
    rows = [dedup[k] for k in sorted(dedup)]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    return rows


def read_csv(path: Path) -> list[dict]:
    out=[]
    with path.open() as f:
        for r in csv.DictReader(f):
            out.append({
                "open_time": int(r["open_time"]), "open": float(r["open"]), "high": float(r["high"]),
                "low": float(r["low"]), "close": float(r["close"]), "volume": float(r["volume"]),
                "close_time": int(r["close_time"]), "quote_volume": float(r["quote_volume"]), "trades": int(r["trades"]),
                "taker_buy_base": float(r["taker_buy_base"]), "taker_buy_quote": float(r["taker_buy_quote"]),
            })
    return out


def color(c: dict) -> int:
    return 1 if c["close"] > c["open"] else -1 if c["close"] < c["open"] else 0

def body_pct(c: dict) -> float:
    rng = max(EPS, c["high"] - c["low"])
    return abs(c["close"] - c["open"]) / rng

def upper_wick_pct(c: dict) -> float:
    rng=max(EPS,c["high"]-c["low"])
    return (c["high"] - max(c["open"], c["close"])) / rng

def lower_wick_pct(c: dict) -> float:
    rng=max(EPS,c["high"]-c["low"])
    return (min(c["open"], c["close"]) - c["low"]) / rng

def streak_len(rows: list[dict], idx: int) -> int:
    # streak ending at idx inclusive
    c = color(rows[idx]);
    if c == 0: return 0
    n=0; j=idx
    while j>=0 and color(rows[j]) == c:
        n += 1; j -= 1
    return n


def build_variants(total=1000, seed=42) -> list[Variant]:
    r=random.Random(seed)
    variants=[]
    vol_ns=[3,5,8,10,13,21,34,55,89]
    xs=[1.15,1.25,1.35,1.5,1.75,2.0,2.5,3.0,4.0]
    body=[0.0,0.10,0.20,0.30,0.45]
    spike_body=[0.0,0.20,0.35,0.50,0.65]
    streak_mins=[1,2,3,4,5]
    streak_max=[0,3,5,8,13] # 0 means disabled
    wicks=[0.0,0.45,0.55,0.65,0.75]
    seen=set()
    while len(variants) < total:
        v=Variant(
            id=len(variants),
            vol_n=r.choice(vol_ns), spike_x=r.choice(xs), min_body_pct=r.choice(body), spike_body_pct=r.choice(spike_body),
            streak_min_for_reversal=r.choice(streak_mins), streak_max_continue=r.choice(streak_max),
            require_spike_same_as_streak=r.choice([False, True]), wick_exhaustion=r.choice(wicks),
            exhaustion_reversal=r.choice([False, True]), use_taker_confirmation=r.choice([False, True])
        )
        key=tuple(asdict(v).items())
        if key not in seen:
            seen.add(key); variants.append(v)
    return variants


def signal(rows: list[dict], i: int, v: Variant) -> tuple[int, bool, float, str]:
    # signal for market/candle i, using previous candle i-1 and earlier only
    prev = rows[i-1]
    base = color(prev)
    if base == 0:
        # flat candle: use previous non-flat fallback
        j=i-2
        while j>=0 and color(rows[j])==0: j-=1
        base = color(rows[j]) if j>=0 else 1
    if body_pct(prev) < v.min_body_pct:
        # weak/noisy prior candle: still every-market; use close-to-close micro direction
        base = 1 if prev["close"] >= rows[i-2]["close"] else -1
    avg_vol = mean([rows[j]["volume"] for j in range(i-1-v.vol_n, i-1)]) if i-1-v.vol_n >= 0 else 0.0
    ratio = prev["volume"] / max(EPS, avg_vol)
    st = streak_len(rows, i-1)
    spike = avg_vol > 0 and ratio >= v.spike_x and body_pct(prev) >= v.spike_body_pct
    if v.use_taker_confirmation:
        taker_buy_share = prev["taker_buy_base"] / max(EPS, prev["volume"])
        # require the spike candle's aggressive flow to match its color; otherwise no reversal exception.
        if (base == 1 and taker_buy_share < 0.52) or (base == -1 and taker_buy_share > 0.48):
            spike = False
    flip = False; reason = "continuation"
    if spike and st >= v.streak_min_for_reversal:
        if (not v.require_spike_same_as_streak) or color(prev) == base:
            flip = True; reason = "volume_spike_reversal"
    if v.streak_max_continue and st >= v.streak_max_continue:
        # optional long-succession exhaustion rule; wick gate can make it selective.
        wick_ok = v.wick_exhaustion <= 0 or (base == 1 and upper_wick_pct(prev) >= v.wick_exhaustion) or (base == -1 and lower_wick_pct(prev) >= v.wick_exhaustion)
        if v.exhaustion_reversal and wick_ok:
            flip = True; reason = "streak_exhaustion_reversal"
    return (-base if flip else base), flip, ratio, reason


def eval_variant(rows: list[dict], v: Variant, split_idx: int) -> Result | None:
    start = max(v.vol_n + 2, 3)
    trades=wins=tw=tt=vw=vt=flips=fw=cw=ct=0
    spike_ratios=[]
    for i in range(start, len(rows)):
        pred, flip, ratio, _ = signal(rows, i, v)
        actual = color(rows[i])
        if actual == 0: continue
        win = pred == actual
        trades += 1; wins += int(win)
        if i < split_idx: tt += 1; tw += int(win)
        else: vt += 1; vw += int(win)
        if flip:
            flips += 1; fw += int(win); spike_ratios.append(ratio)
        else:
            ct += 1; cw += int(win)
    if not trades or not vt: return None
    hit=wins/trades; th=tw/max(1,tt); vh=vw/max(1,vt)
    flip_hr=fw/max(1,flips); cont_hr=cw/max(1,ct)
    # rank by test + stability + ability of reversal exception, with mild penalty for over-flipping
    score = vh + 0.10*hit - 0.20*abs(th-vh) + 0.03*flip_hr - 0.02*abs((flips/max(1,trades))-0.12)
    return Result(v.id, {k:val for k,val in asdict(v).items() if k!="id"}, trades, wins, trades-wins, hit,
                  tt, th, vt, vh, flips, flips/trades, flip_hr, cont_hr, mean(spike_ratios) if spike_ratios else 0.0, score)


def reversal_volume_stats(rows: list[dict], ns: list[int]) -> dict:
    out={}
    for n in ns:
        ratios_rev=[]; ratios_cont=[]
        for i in range(n+2, len(rows)):
            prev=color(rows[i-1]); cur=color(rows[i])
            if prev == 0 or cur == 0: continue
            avg=mean([rows[j]["volume"] for j in range(i-1-n, i-1)])
            ratio=rows[i-1]["volume"]/max(EPS, avg)
            (ratios_rev if cur == -prev else ratios_cont).append(ratio)
        def q(a,p):
            if not a: return 0
            b=sorted(a); return b[min(len(b)-1, int(round((len(b)-1)*p)))]
        out[str(n)]={
            "reversal_count":len(ratios_rev), "continuation_count":len(ratios_cont),
            "reversal_mean_ratio":mean(ratios_rev), "continuation_mean_ratio":mean(ratios_cont),
            "reversal_median_ratio":median(ratios_rev), "continuation_median_ratio":median(ratios_cont),
            "reversal_p75_ratio":q(ratios_rev,0.75), "reversal_p90_ratio":q(ratios_rev,0.90),
            "continuation_p90_ratio":q(ratios_cont,0.90),
        }
    return out


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=360)
    ap.add_argument("--variants", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--out-dir", default="/home/administrator/projects/polybot/reports/btc5m_candle_volume_reversal_360d")
    args=ap.parse_args()
    out_dir=Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    csv_path=out_dir/f"binance_btcusdt_5m_{args.days}d.csv"
    rows=fetch_klines(args.days, csv_path, args.refresh)
    rows=rows[-int(args.days*24*12)-5:]
    split=int(len(rows)*0.70)
    variants=build_variants(args.variants, args.seed)
    results=[eval_variant(rows,v,split) for v in variants]
    results=[r for r in results if r is not None]
    results.sort(key=lambda r:(r.score,r.test_hit_rate,r.hit_rate), reverse=True)
    ns=sorted({v.vol_n for v in variants})
    vol_stats=reversal_volume_stats(rows, ns)
    report={
        "assumptions":{
            "data":"Binance BTCUSDT 5m OHLCV; every closed 5m candle is treated as one BTC 5m up/down market.",
            "resolution":"UP if candle close > open; DOWN if close < open; flat candles skipped for scoring.",
            "entry":"signal uses only candles fully closed before the current 5m market opens; no Polymarket price/PnL included.",
            "base_rule":"previous candle green -> UP, previous candle red -> DOWN; volume/streak exhaustion can flip to reversal.",
            "stake_model":"directional hit-rate only, equivalent fixed-size every-market bet before fees/spread/market price."
        },
        "dataset":{"rows":len(rows),"markets_scored_approx":len(rows)-max(ns)-2,"start_utc":iso(rows[0]["open_time"]),"end_utc":iso(rows[-1]["open_time"]),"train_rows":split,"test_rows":len(rows)-split,"csv":str(csv_path)},
        "variants_evaluated":len(results),
        "volume_reversal_stats_by_n":vol_stats,
        "top_25":[asdict(r) for r in results[:25]],
    }
    (out_dir/"summary.json").write_text(json.dumps(report, indent=2, sort_keys=True))
    with (out_dir/"top_25.csv").open("w", newline="") as f:
        fieldnames=list(asdict(results[0]).keys())
        w=csv.DictWriter(f, fieldnames=fieldnames); w.writeheader(); w.writerows(asdict(r) for r in results[:25])
    print(json.dumps({"dataset":report["dataset"],"top_5":[asdict(r) for r in results[:5]],"summary":str(out_dir/"summary.json")}, indent=2, sort_keys=True))

if __name__ == "__main__":
    main()
