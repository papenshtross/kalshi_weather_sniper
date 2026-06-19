import json

from polybot.copytrade.ranking import (
    CurvePoint,
    ProfileSnapshot,
    compute_curve_metrics,
    extract_next_data,
    extract_profile_snapshot,
    score_profile,
)


def test_extract_next_data_reads_embedded_script_payload():
    payload = {"props": {"pageProps": {"hello": "world"}}}
    html = (
        '<html><body><script id="__NEXT_DATA__" type="application/json" crossorigin="anonymous">'
        + json.dumps(payload)
        + "</script></body></html>"
    )

    assert extract_next_data(html) == payload


def test_extract_profile_snapshot_reads_all_time_curve_and_stats():
    wallet = "0x1234567890abcdef1234567890abcdef12345678"
    next_data = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "queryKey": ["portfolio-pnl", "demo", wallet, "ALL"],
                            "state": {"data": [{"t": 1, "p": 10.0}, {"t": 2, "p": 15.0}]},
                        },
                        {
                            "queryKey": ["user-stats", wallet],
                            "state": {"data": {"trades": 21, "largestWin": 88.0}},
                        },
                        {
                            "queryKey": ["/api/profile/volume", wallet, wallet],
                            "state": {"data": {"amount": 3210.0, "pnl": 15.0}},
                        },
                        {
                            "queryKey": ["/api/profile/userData", wallet],
                            "state": {"data": {"name": "Demo Trader"}},
                        },
                    ]
                }
            }
        }
    }

    snapshot = extract_profile_snapshot(next_data, wallet)

    assert snapshot.address == wallet
    assert snapshot.name == "Demo Trader"
    assert snapshot.predictions == 21
    assert snapshot.largest_win == 88.0
    assert snapshot.volume_amount == 3210.0
    assert [point.pnl for point in snapshot.curve_all] == [10.0, 15.0]


def test_compute_curve_metrics_rewards_smoother_curves():
    smooth = [
        CurvePoint(ts=0, pnl=0.0),
        CurvePoint(ts=1, pnl=10.0),
        CurvePoint(ts=2, pnl=20.0),
        CurvePoint(ts=3, pnl=30.0),
        CurvePoint(ts=4, pnl=40.0),
    ]
    jagged = [
        CurvePoint(ts=0, pnl=0.0),
        CurvePoint(ts=1, pnl=25.0),
        CurvePoint(ts=2, pnl=5.0),
        CurvePoint(ts=3, pnl=35.0),
        CurvePoint(ts=4, pnl=40.0),
    ]

    smooth_metrics = compute_curve_metrics(smooth)
    jagged_metrics = compute_curve_metrics(jagged)

    assert smooth_metrics.r2 > jagged_metrics.r2
    assert smooth_metrics.max_drawdown_abs < jagged_metrics.max_drawdown_abs
    assert smooth_metrics.up_step_ratio > jagged_metrics.up_step_ratio


def test_score_profile_prefers_smoother_growth_for_same_terminal_pnl():
    smooth = ProfileSnapshot(
        address="0x1",
        name="smooth",
        predictions=50,
        largest_win=1000.0,
        volume_amount=100000.0,
        current_pnl=400.0,
        curve_all=[
            CurvePoint(ts=0, pnl=0.0),
            CurvePoint(ts=1, pnl=100.0),
            CurvePoint(ts=2, pnl=200.0),
            CurvePoint(ts=3, pnl=300.0),
            CurvePoint(ts=4, pnl=400.0),
        ],
    )
    jagged = ProfileSnapshot(
        address="0x2",
        name="jagged",
        predictions=50,
        largest_win=1000.0,
        volume_amount=100000.0,
        current_pnl=400.0,
        curve_all=[
            CurvePoint(ts=0, pnl=0.0),
            CurvePoint(ts=1, pnl=250.0),
            CurvePoint(ts=2, pnl=50.0),
            CurvePoint(ts=3, pnl=350.0),
            CurvePoint(ts=4, pnl=400.0),
        ],
    )

    assert score_profile(smooth) > score_profile(jagged)
