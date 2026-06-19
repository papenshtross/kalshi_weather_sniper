from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json" crossorigin="anonymous">(.*?)</script>'
)


@dataclass(frozen=True)
class CurvePoint:
    ts: int
    pnl: float


@dataclass(frozen=True)
class CurveMetrics:
    final_pnl: float
    history_days: float
    max_drawdown_abs: float
    max_drawdown_ratio: float
    up_step_ratio: float
    r2: float
    recent_30d_change: float
    worst_step_drop_abs: float


@dataclass(frozen=True)
class ProfileSnapshot:
    address: str
    name: str
    predictions: int
    largest_win: float
    volume_amount: float
    current_pnl: float
    curve_all: list[CurvePoint]


def extract_next_data(html: str) -> dict[str, Any]:
    match = NEXT_DATA_RE.search(html)
    if not match:
        raise ValueError("__NEXT_DATA__ payload not found")
    return json.loads(match.group(1))


def _iter_queries(next_data: dict[str, Any]) -> list[dict[str, Any]]:
    return (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("dehydratedState", {})
        .get("queries", [])
    )


def extract_leaderboard_rows(next_data: dict[str, Any]) -> list[dict[str, Any]]:
    for query in _iter_queries(next_data):
        key = query.get("queryKey")
        data = query.get("state", {}).get("data")
        if isinstance(key, list) and key and key[0] == "/leaderboard" and isinstance(data, list):
            return data
    return []


def extract_profile_snapshot(next_data: dict[str, Any], wallet: str) -> ProfileSnapshot:
    wallet = wallet.lower()
    name = wallet
    predictions = 0
    largest_win = 0.0
    volume_amount = 0.0
    current_pnl = 0.0
    curve_all: list[CurvePoint] = []

    for query in _iter_queries(next_data):
        key = query.get("queryKey")
        data = query.get("state", {}).get("data")
        if key == ["user-stats", wallet] and isinstance(data, dict):
            predictions = int(data.get("trades") or 0)
            largest_win = float(data.get("largestWin") or 0.0)
        elif key == ["/api/profile/volume", wallet, wallet] and isinstance(data, dict):
            volume_amount = float(data.get("amount") or 0.0)
            current_pnl = float(data.get("pnl") or 0.0)
        elif key == ["/api/profile/userData", wallet] and isinstance(data, dict):
            name = str(data.get("name") or data.get("pseudonym") or wallet)
        elif isinstance(key, list) and len(key) >= 4 and key[0] == "portfolio-pnl" and key[2] == wallet and key[-1] == "ALL":
            curve_all = [
                CurvePoint(ts=int(point["t"]), pnl=float(point["p"]))
                for point in data or []
                if isinstance(point, dict) and "t" in point and "p" in point
            ]

    return ProfileSnapshot(
        address=wallet,
        name=name,
        predictions=predictions,
        largest_win=largest_win,
        volume_amount=volume_amount,
        current_pnl=current_pnl,
        curve_all=sorted(curve_all, key=lambda point: point.ts),
    )


def _regression_r2(points: list[CurvePoint]) -> tuple[float, float]:
    count = len(points)
    if count < 3:
        return 0.0, 0.0
    ys = [point.pnl for point in points]
    xs = list(range(count))
    mean_x = (count - 1) / 2
    mean_y = sum(ys) / count
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if var_x <= 0:
        return 0.0, 0.0
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    slope = cov / var_x
    fitted = [mean_y + slope * (x - mean_x) for x in xs]
    total_var = sum((y - mean_y) ** 2 for y in ys)
    if total_var <= 0:
        return 1.0, slope
    residual_var = sum((y - prediction) ** 2 for y, prediction in zip(ys, fitted))
    return max(0.0, 1.0 - (residual_var / total_var)), slope


def compute_curve_metrics(points: list[CurvePoint]) -> CurveMetrics:
    if not points:
        return CurveMetrics(
            final_pnl=0.0,
            history_days=0.0,
            max_drawdown_abs=0.0,
            max_drawdown_ratio=0.0,
            up_step_ratio=0.0,
            r2=0.0,
            recent_30d_change=0.0,
            worst_step_drop_abs=0.0,
        )

    ordered = sorted(points, key=lambda point: point.ts)
    final_pnl = ordered[-1].pnl
    history_days = max(1.0, (ordered[-1].ts - ordered[0].ts) / 86400)

    peak = -float("inf")
    max_drawdown_abs = 0.0
    gains = 0
    nonflat_steps = 0
    worst_step_drop_abs = 0.0
    for index, point in enumerate(ordered):
        peak = max(peak, point.pnl)
        max_drawdown_abs = max(max_drawdown_abs, peak - point.pnl)
        if index == 0:
            continue
        delta = point.pnl - ordered[index - 1].pnl
        if abs(delta) <= 1e-9:
            continue
        nonflat_steps += 1
        if delta > 0:
            gains += 1
        else:
            worst_step_drop_abs = max(worst_step_drop_abs, abs(delta))

    up_step_ratio = gains / nonflat_steps if nonflat_steps else 0.5
    denom = max(1.0, abs(final_pnl))
    max_drawdown_ratio = max_drawdown_abs / denom
    r2, _ = _regression_r2(ordered)

    latest_ts = ordered[-1].ts
    recent_cutoff = latest_ts - (30 * 86400)
    recent_points = [point for point in ordered if point.ts >= recent_cutoff]
    recent_30d_change = recent_points[-1].pnl - recent_points[0].pnl if recent_points else 0.0

    return CurveMetrics(
        final_pnl=final_pnl,
        history_days=history_days,
        max_drawdown_abs=max_drawdown_abs,
        max_drawdown_ratio=max_drawdown_ratio,
        up_step_ratio=up_step_ratio,
        r2=r2,
        recent_30d_change=recent_30d_change,
        worst_step_drop_abs=worst_step_drop_abs,
    )


def score_profile(snapshot: ProfileSnapshot) -> float:
    metrics = compute_curve_metrics(snapshot.curve_all)
    score = 0.0
    if metrics.final_pnl > 0:
        score += math.log10(metrics.final_pnl + 1.0) * 2.5
    score += max(0.0, min(1.0, metrics.r2)) * 3.0
    score += max(0.0, min(1.0, metrics.up_step_ratio)) * 2.0
    score += math.log10(max(1.0, snapshot.predictions)) * 1.0
    score += math.log10(max(1.0, snapshot.volume_amount)) * 0.75
    score += math.tanh(metrics.recent_30d_change / 50_000.0) * 1.5
    score -= min(3.0, metrics.max_drawdown_ratio * 4.0)
    score -= min(2.0, metrics.worst_step_drop_abs / max(10_000.0, abs(metrics.final_pnl) * 0.2))
    return round(score, 4)


def build_watchlist(snapshots: list[ProfileSnapshot], top_n: int = 50) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for snapshot in snapshots:
        metrics = compute_curve_metrics(snapshot.curve_all)
        if metrics.final_pnl <= 0 or metrics.history_days < 14 or snapshot.predictions < 8:
            continue
        # User preference for copy-trading candidates: favor sustainable, smoother
        # curves and reject the biggest boom/bust profiles even if their terminal
        # pnl is very large.
        if metrics.max_drawdown_ratio > 0.35 or metrics.r2 < 0.4:
            continue
        ranked.append(
            {
                "address": snapshot.address,
                "name": snapshot.name,
                "score": score_profile(snapshot),
                "final_pnl": round(metrics.final_pnl, 2),
                "recent_30d_change": round(metrics.recent_30d_change, 2),
                "max_drawdown_ratio": round(metrics.max_drawdown_ratio, 4),
                "r2": round(metrics.r2, 4),
                "up_step_ratio": round(metrics.up_step_ratio, 4),
                "predictions": snapshot.predictions,
                "largest_win": round(snapshot.largest_win, 2),
                "volume_amount": round(snapshot.volume_amount, 2),
            }
        )
    ranked.sort(key=lambda row: row["score"], reverse=True)
    return ranked[:top_n]
