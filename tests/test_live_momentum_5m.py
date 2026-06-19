import pytest
from datetime import datetime, timezone

from polybot.backtest.binance_strategy_lab import CandlePoint, PricePoint, SideSignal
import polybot.live.momentum_5m_runner as momentum_runner
from polybot.live.momentum_5m import (
    MomentumLiveState,
    build_fixed_stake_order,
    build_fixed_stake_order_from_asks,
    build_live_now_signal,
    dynamic_momentum_strategy_spec_from_config,
    top_dynamic_momentum_strategy_spec,
)
from polybot.live.momentum_5m_runner import (
    active_market_loop_sleep_seconds,
    chainlink_data_streams_url,
    chainlink_rpc_url,
    decode_chainlink_latest_round_data,
    decode_chainlink_streams_latest_point,
    entry_max_attempts_per_market,
    entry_retry_cooldown_seconds,
    is_fok_liquidity_kill_error,
    market_discovery_sleep_seconds,
    momentum_resolution_wait_timeout,
    normalize_attempt_response,
    normalize_signal_source,
    pick_btc_updown_5m,
    should_switch_to_next_5m_event,
    seconds_until_next_5m_start,
    should_poll_market_when_status,
    hedge_consensus_trigger,
    hedge_arm_score_threshold,
    hedge_enabled,
    hedge_profit_buffer,
    polymarket_taker_fee_usdc,
    opposite_outcome,
    build_hedge_order_from_asks,
    should_arm_hedge,
    update_hedge_armed,
    should_retry_entry_attempt,
    signal_to_observation,
    signal_log_summary,
    lightgbm_predict_url,
    build_lightgbm_prediction_request,
    lightgbm_response_to_signal,
    lightgbm_market_state_filter_signal,
    maybe_reverse_signal_side,
    normalize_signal_filters,
    signal_source_is_lightgbm,
    signal_source_target_mode,
    signal_history_lookback_seconds,
    signal_candle_start_ts,
)


def _candles(values, start_ts=0):
    candles = []
    ts = start_ts
    for close, taker_ratio in values:
        open_price = candles[-1].close if candles else close - 0.1
        candles.append(
            CandlePoint(
                ts=ts,
                open=open_price,
                high=max(open_price, close) + 0.05,
                low=min(open_price, close) - 0.05,
                close=close,
                volume=10.0,
                taker_buy_volume=10.0 * taker_ratio,
            )
        )
        ts += 1
    return candles


def test_stopped_momentum_strategy_does_not_poll_live_market():
    assert should_poll_market_when_status("running") is True
    assert should_poll_market_when_status("stopped") is False
    assert should_poll_market_when_status("stop_requested") is False


def test_momentum_does_not_wait_for_resolution_by_default():
    assert momentum_resolution_wait_timeout({}) == 0
    assert momentum_resolution_wait_timeout({"resolution_wait_seconds": 0}) == 0
    assert momentum_resolution_wait_timeout({"resolution_wait_seconds": 12}) == 12


def test_top_dynamic_momentum_strategy_spec_matches_requested_variant():
    spec = top_dynamic_momentum_strategy_spec()
    assert spec.windows == (5, 10, 20, 40, 80)
    assert spec.min_consensus == 3
    assert spec.price_cap is None


def test_live_dynamic_momentum_strategy_can_require_5_of_5_consensus():
    spec = dynamic_momentum_strategy_spec_from_config({"min_consensus": 5})
    assert spec.windows == (5, 10, 20, 40, 80)
    assert spec.min_consensus == 5
    assert spec.price_cap is None


def test_momentum_market_discovery_polls_fast_around_5m_rollover():
    assert seconds_until_next_5m_start(299) == pytest.approx(1.0)
    assert seconds_until_next_5m_start(299.98) == pytest.approx(0.02)
    assert seconds_until_next_5m_start(300) == pytest.approx(0.0)
    assert market_discovery_sleep_seconds(299.2) == pytest.approx(0.02)
    assert market_discovery_sleep_seconds(300.2) == pytest.approx(0.02)
    assert market_discovery_sleep_seconds(304) == pytest.approx(0.25)
    assert market_discovery_sleep_seconds(450) == pytest.approx(2.0)


def test_active_market_loop_sleep_tightens_to_sub_50ms_at_rollover():
    assert active_market_loop_sleep_seconds(298.5) == pytest.approx(0.10)
    assert active_market_loop_sleep_seconds(299.2) == pytest.approx(0.02)
    assert active_market_loop_sleep_seconds(450) == pytest.approx(1.0)
    assert active_market_loop_sleep_seconds(450, retry_sleep_seconds=0.75) == pytest.approx(0.75)


def test_momentum_switches_to_preresolved_next_market_as_soon_as_it_starts():
    current = {"slug": "btc-updown-5m-600", "start_ts": 600, "end_dt": datetime.fromtimestamp(900, tz=timezone.utc)}
    next_ev = {"slug": "btc-updown-5m-900", "start_ts": 900, "end_dt": datetime.fromtimestamp(1200, tz=timezone.utc)}

    assert should_switch_to_next_5m_event(899.95, current, next_ev) is False
    assert should_switch_to_next_5m_event(900.0, current, next_ev) is True
    assert should_switch_to_next_5m_event(900.25, current, next_ev) is True
    assert should_switch_to_next_5m_event(900.25, current, None) is True


@pytest.mark.asyncio
async def test_pick_btc_updown_5m_uses_deterministic_slug_before_active_list(monkeypatch):
    calls = []

    async def fake_resolve(client, slug):
        calls.append(slug)
        return {"slug": slug} if slug == "btc-updown-5m-600" else None

    monkeypatch.setattr(momentum_runner.time, "time", lambda: 601)
    monkeypatch.setattr(momentum_runner, "resolve_5m_event", fake_resolve)

    picked = await pick_btc_updown_5m(client=object())

    assert picked == {"slug": "btc-updown-5m-600"}
    assert calls == ["btc-updown-5m-600"]


@pytest.mark.asyncio
async def test_pick_btc_updown_5m_preloads_next_slug_when_current_is_almost_over(monkeypatch):
    calls = []

    async def fake_resolve(client, slug):
        calls.append(slug)
        return {"slug": slug} if slug == "btc-updown-5m-900" else None

    monkeypatch.setattr(momentum_runner.time, "time", lambda: 890)
    monkeypatch.setattr(momentum_runner, "resolve_5m_event", fake_resolve)

    picked = await pick_btc_updown_5m(client=object())

    assert picked == {"slug": "btc-updown-5m-900"}
    assert calls == ["btc-updown-5m-900"]


def test_build_live_now_signal_uses_current_timestamp_not_fixed_5s_before_close():
    candles = _candles([(100 + i * 0.1, 0.6) for i in range(90)], start_ts=210)
    spec = top_dynamic_momentum_strategy_spec()

    signal = build_live_now_signal(
        candles=candles,
        up_ask=0.81,
        down_ask=0.19,
        current_ts=299,
        price_to_beat=103.0,
        spec=spec,
    )

    assert signal.side == SideSignal.UP
    assert signal.entry_ts == 299
    assert signal.entry_price == pytest.approx(0.81)


def test_signal_to_observation_keeps_decision_fields_for_backtest_replay():
    signal = build_live_now_signal(
        candles=_candles([(100 + i * 0.1, 0.6) for i in range(90)], start_ts=210),
        up_ask=0.81,
        down_ask=0.19,
        current_ts=299,
        price_to_beat=103.0,
        spec=top_dynamic_momentum_strategy_spec(),
    )

    payload = signal_to_observation(signal)

    assert payload["side"] == "UP"
    assert payload["score"] == pytest.approx(5.0)
    assert payload["entry_ts"] == 299
    assert payload["entry_price"] == pytest.approx(0.81)
    assert payload["reason"] is None
    assert "breakout_probability" in payload


def test_fok_liquidity_kill_detection_is_narrow():
    assert is_fok_liquidity_kill_error({"success": False, "error": "order couldn't be fully filled. FOK orders are fully filled or killed"}) is True
    assert is_fok_liquidity_kill_error({"success": False, "error": "invalid signature"}) is False
    assert is_fok_liquidity_kill_error({"success": False, "error": "invalid amounts, maker amount supports a max accuracy of 2 decimals"}) is False
    assert is_fok_liquidity_kill_error({"success": False, "error": "not enough balance / allowance"}) is False


def test_entry_retry_config_defaults_and_clamps():
    assert entry_max_attempts_per_market({}) == 1
    assert entry_max_attempts_per_market({"entry_retry_on_fok_kill": True}) == 3
    assert entry_max_attempts_per_market({"entry_retry_on_fok_kill": True, "entry_max_attempts_per_market": 0}) == 1
    assert entry_retry_cooldown_seconds({}) == pytest.approx(0.75)
    assert entry_retry_cooldown_seconds({"entry_retry_cooldown_seconds": -1}) == pytest.approx(0.0)


def test_signal_source_config_normalizes_binance_chainlink_and_lightgbm():
    assert normalize_signal_source({}) == "binance"
    assert normalize_signal_source({"signal_source": "Chainlink"}) == "chainlink"
    assert normalize_signal_source({"signal_source": "LightGBM"}) == "lightgbm"
    assert normalize_signal_source({"signal_source": "LightGBM_Probability"}) == "lightgbm_probability"
    assert normalize_signal_source({"signal_source": "LightGBM_Probability_V2"}) == "lightgbm_probability_v2"
    assert normalize_signal_source({"signal_source": "unknown"}) == "binance"

    assert lightgbm_predict_url({}) == "http://127.0.0.1:8787/predict"
    assert lightgbm_predict_url({"signal_source": "lightgbm_probability"}) == "http://127.0.0.1:8788/predict"
    assert lightgbm_predict_url({"signal_source": "lightgbm_probability_v2"}) == "http://127.0.0.1:8789/predict"
    assert lightgbm_predict_url({"lightgbm_predict_url": "http://signal.local/predict"}) == "http://signal.local/predict"
    assert signal_source_is_lightgbm("lightgbm") is True
    assert signal_source_is_lightgbm("lightgbm_probability") is True
    assert signal_source_is_lightgbm("lightgbm_probability_v2") is True
    assert signal_source_target_mode("lightgbm") == "current_5m_return"
    assert signal_source_target_mode("lightgbm_probability") == "polymarket_strike"
    assert signal_source_target_mode("lightgbm_probability_v2") == "polymarket_strike"
    assert chainlink_rpc_url({"chainlink_rpc_url": "https://rpc.example"}) == "https://rpc.example"


def test_lightgbm_signal_fetches_full_model_lookback_before_market_start():
    assert signal_history_lookback_seconds({"signal_source": "lightgbm_probability"}) == 330
    assert signal_history_lookback_seconds({"signal_source": "lightgbm_probability", "lightgbm_model_lookback_seconds": 450}) == 450
    assert signal_history_lookback_seconds({"signal_source": "binance"}) == 180

    # LightGBM must not clamp to market_start_ts; early in a 5m market it still
    # needs prior-market 1s candles to populate 300s rolling features.
    assert signal_candle_start_ts({"signal_source": "lightgbm_probability"}, market_start_ts=1_000, now_ts=1_030) == 700
    assert signal_candle_start_ts({"signal_source": "lightgbm_probability", "signal_history_lookback_seconds": 450}, market_start_ts=1_000, now_ts=1_030) == 580
    # Non-LightGBM momentum stays scoped to the active market.
    assert signal_candle_start_ts({"signal_source": "binance"}, market_start_ts=1_000, now_ts=1_030) == 1_000


def test_build_lightgbm_prediction_request_uses_binance_features_and_chainlink_quality_fields():
    candles = _candles([(100 + i * 0.1, 0.6) for i in range(130)], start_ts=1_000)

    req = build_lightgbm_prediction_request(
        candles=candles,
        current_ts=1_120,
        window_start_ts=900,
        window_end_ts=1_200,
        price_to_beat=105.0,
        chainlink_price=112.5,
        chainlink_ts=1_119,
        up_ask=0.62,
        down_ask=0.41,
        target_mode="current_5m_return",
    )

    assert req["ts_ms"] == 1_120_000
    assert req["window_start_ms"] == 900_000
    assert req["window_end_ms"] == 1_200_000
    assert req["price_to_beat"] == pytest.approx(105.0)
    assert req["chainlink_price"] == pytest.approx(112.5)
    assert req["chainlink_ts_ms"] == 1_119_000
    assert req["binance_spot_price"] == pytest.approx(112.0)
    assert req["binance_spot_ts_ms"] == 1_120_000
    assert req["polymarket"] == {"up_best_ask": 0.62, "down_best_ask": 0.41}
    assert req["target_mode"] == "current_5m_return"
    assert req["target_reference_price"] == pytest.approx(112.0)
    features = req["features"]
    assert features["binance_price"] == pytest.approx(112.0)
    assert features["seconds_elapsed"] == 220
    assert features["seconds_remaining"] == 80
    assert features["binance_distance_to_beat_bps"] == pytest.approx(10000.0 * (112.0 / 105.0 - 1.0))
    assert features["consensus_score_5_10_20_40_80"] == 5
    assert "realized_vol_120s" in features


def test_lightgbm_probability_request_targets_market_resolution_price_to_beat():
    candles = _candles([(100 + i * 0.1, 0.6) for i in range(130)], start_ts=1_000)

    req = build_lightgbm_prediction_request(
        candles=candles,
        current_ts=1_120,
        window_start_ts=900,
        window_end_ts=1_200,
        price_to_beat=105.0,
        chainlink_price=112.5,
        chainlink_ts=1_119,
        up_ask=0.50,
        down_ask=0.52,
        target_mode="polymarket_strike",
    )

    assert req["target_mode"] == "polymarket_strike"
    assert req["target_reference_price"] == pytest.approx(105.0)
    assert "target_mode" not in req["features"]
    assert all((value is None) or isinstance(value, (int, float)) for value in req["features"].values())


def test_build_lightgbm_prediction_request_uses_actual_latest_binance_candle_timestamp_for_staleness():
    candles = _candles([(100.0, 0.6), (101.0, 0.6)], start_ts=1_000)

    req = build_lightgbm_prediction_request(
        candles=candles,
        current_ts=1_120,
        window_start_ts=900,
        window_end_ts=1_200,
        price_to_beat=105.0,
        chainlink_price=112.5,
        chainlink_ts=1_119,
        up_ask=0.50,
        down_ask=0.52,
        target_mode="polymarket_strike",
    )

    assert req["binance_spot_price"] == pytest.approx(101.0)
    assert req["binance_spot_ts_ms"] == 1_001_000


def test_lightgbm_probability_edge_mode_respects_no_trade_action_and_data_quality():
    no_trade = lightgbm_response_to_signal(
        {
            "action": "NO_TRADE",
            "confidence": 0.99,
            "p_up": 0.99,
            "p_down": 0.01,
            "edge_up": 0.49,
            "edge_down": -0.51,
            "reason_codes": ["stale_binance"],
            "data_quality": {"ok": False},
        },
        current_ts=1_120,
        up_ask=0.50,
        down_ask=0.52,
        min_model_edge=0.20,
        edge_mode=True,
    )

    assert no_trade.side == SideSignal.SKIP
    assert no_trade.reason == "lightgbm_no_trade_stale_binance"

    bad_quality = lightgbm_response_to_signal(
        {
            "action": "UP",
            "confidence": 0.99,
            "p_up": 0.99,
            "p_down": 0.01,
            "edge_up": 0.49,
            "edge_down": -0.51,
            "data_quality": {"ok": False, "warnings": ["stale_binance"]},
        },
        current_ts=1_120,
        up_ask=0.50,
        down_ask=0.52,
        min_model_edge=0.20,
        edge_mode=True,
    )

    assert bad_quality.side == SideSignal.SKIP
    assert bad_quality.reason == "lightgbm_data_quality_not_ok"


def test_lightgbm_probability_edge_signal_buys_only_when_model_edge_exceeds_threshold():
    buy_up = lightgbm_response_to_signal(
        {"action": "UP", "confidence": 0.72, "p_up": 0.72, "p_down": 0.28, "edge_up": 0.22, "edge_down": -0.24, "target_mode": "polymarket_strike"},
        current_ts=1_120,
        up_ask=0.50,
        down_ask=0.52,
        min_model_edge=0.20,
        edge_mode=True,
    )
    assert buy_up.side == SideSignal.UP
    assert buy_up.entry_price == pytest.approx(0.50)
    assert buy_up.score == pytest.approx(0.22)
    assert buy_up.diagnostics["selected_edge"] == pytest.approx(0.22)

    skip = lightgbm_response_to_signal(
        {"action": "UP", "confidence": 0.60, "p_up": 0.60, "p_down": 0.40, "edge_up": 0.10, "edge_down": -0.12, "target_mode": "polymarket_strike"},
        current_ts=1_120,
        up_ask=0.50,
        down_ask=0.52,
        min_model_edge=0.20,
        edge_mode=True,
    )
    assert skip.side == SideSignal.SKIP
    assert skip.reason == "lightgbm_edge_below_min"


def test_active_market_loop_sleep_can_be_overridden_for_high_frequency_edge_monitoring():
    assert active_market_loop_sleep_seconds(450, cfg={"active_loop_sleep_seconds": 0.10}) == pytest.approx(0.10)


def test_normalize_signal_filters_accepts_dashboard_multiselect_shapes():
    assert normalize_signal_filters({}) == set()
    assert normalize_signal_filters({"signal_filters": ["lightgbm_market_state"]}) == {"lightgbm_market_state"}
    assert normalize_signal_filters({"signal_filters": "lightgbm_market_state, unknown"}) == {"lightgbm_market_state"}
    assert normalize_signal_filters({"enabled_signal_filters": ["market_state_confirmation"]}) == {"lightgbm_market_state"}


def test_lightgbm_market_state_filter_blocks_majority_opposed_trend_and_flow():
    candles = _candles([(100 - i * 0.03, 0.20) for i in range(130)], start_ts=1_000)
    raw = lightgbm_response_to_signal(
        {"action": "UP", "p_up": 0.72, "p_down": 0.28, "edge_up": 0.22, "edge_down": -0.24, "target_mode": "polymarket_strike"},
        current_ts=1_120,
        up_ask=0.50,
        down_ask=0.52,
        min_model_edge=0.20,
        edge_mode=True,
    )

    filtered = lightgbm_market_state_filter_signal(
        raw,
        candles=candles,
        current_ts=1_120,
        window_start_ts=900,
        price_to_beat=105.0,
        cfg={"signal_filters": ["lightgbm_market_state"]},
    )

    assert filtered.side == SideSignal.SKIP
    assert filtered.reason == "filtered_lightgbm_market_state"
    assert filtered.diagnostics["filter_result"]["passed"] is False
    assert "trend_majority_opposed" in filtered.diagnostics["filter_result"]["failed_reasons"]
    assert "taker_flow_opposed" in filtered.diagnostics["filter_result"]["failed_reasons"]


def test_lightgbm_market_state_filter_allows_confirmed_high_volume_trade():
    candles = _candles([(100 + i * 0.03, 0.70) for i in range(130)], start_ts=1_000)
    raw = lightgbm_response_to_signal(
        {"action": "UP", "p_up": 0.75, "p_down": 0.25, "edge_up": 0.25, "edge_down": -0.25, "target_mode": "polymarket_strike"},
        current_ts=1_020,
        up_ask=0.50,
        down_ask=0.50,
        min_model_edge=0.20,
        edge_mode=True,
    )

    filtered = lightgbm_market_state_filter_signal(
        raw,
        candles=candles,
        current_ts=1_020,
        window_start_ts=900,
        price_to_beat=99.0,
        cfg={"signal_filters": ["lightgbm_market_state"]},
    )

    assert filtered.side == SideSignal.UP
    assert filtered.diagnostics["filter_result"]["passed"] is True


def test_maybe_reverse_signal_side_flips_allowed_signal_after_filter_gate():
    signal = lightgbm_response_to_signal(
        {"action": "UP", "p_up": 0.75, "p_down": 0.25, "edge_up": 0.25, "edge_down": -0.25, "target_mode": "polymarket_strike"},
        current_ts=1_020,
        up_ask=0.44,
        down_ask=0.59,
        min_model_edge=0.20,
        edge_mode=True,
    )

    reversed_signal = maybe_reverse_signal_side(signal, {"reverse_signal_side": True}, up_ask=0.44, down_ask=0.59)

    assert reversed_signal.side == SideSignal.DOWN
    assert reversed_signal.score == pytest.approx(-0.25)
    assert reversed_signal.entry_price == pytest.approx(0.59)
    assert reversed_signal.diagnostics["reversed"] is True
    assert reversed_signal.diagnostics["original_side"] == "UP"
    assert "signal_reversed" in reversed_signal.reason


def test_maybe_reverse_signal_side_does_not_flip_skips():
    signal = lightgbm_response_to_signal(
        {"action": "UP", "p_up": 0.55, "p_down": 0.45, "edge_up": 0.05, "edge_down": -0.05},
        current_ts=1_020,
        up_ask=0.50,
        down_ask=0.50,
        min_model_edge=0.20,
        edge_mode=True,
    )

    assert signal.side == SideSignal.SKIP
    assert maybe_reverse_signal_side(signal, {"reverse_signal_side": True}, up_ask=0.50, down_ask=0.50) is signal


def test_lightgbm_market_state_filter_blocks_late_recovery_unless_strong_reversal_evidence():
    candles = _candles([(100 + i * 0.01, 0.52) for i in range(130)], start_ts=1_000)
    raw = lightgbm_response_to_signal(
        {"action": "UP", "p_up": 0.75, "p_down": 0.25, "edge_up": 0.25, "edge_down": -0.25, "target_mode": "polymarket_strike"},
        current_ts=1_120,
        up_ask=0.50,
        down_ask=0.50,
        min_model_edge=0.20,
        edge_mode=True,
    )

    filtered = lightgbm_market_state_filter_signal(
        raw,
        candles=candles,
        current_ts=1_120,
        window_start_ts=900,
        price_to_beat=105.0,
        cfg={"signal_filters": ["lightgbm_market_state"]},
    )

    assert filtered.side == SideSignal.SKIP
    assert "late_recovery_bet" in filtered.diagnostics["filter_result"]["failed_reasons"]


def test_lightgbm_response_to_signal_maps_actions_and_no_trade():
    up = lightgbm_response_to_signal(
        {"action": "UP", "confidence": 0.71, "p_up": 0.71, "p_down": 0.29, "reason_codes": ["lightgbm_model"]},
        current_ts=1_120,
        up_ask=0.62,
        down_ask=0.41,
    )
    assert up.side == SideSignal.UP
    assert up.score == pytest.approx(0.71)
    assert up.entry_price == pytest.approx(0.62)
    assert up.diagnostics["p_up"] == pytest.approx(0.71)

    skip = lightgbm_response_to_signal({"action": "NO_TRADE", "confidence": 0.54}, current_ts=1_120, up_ask=0.62, down_ask=0.41)
    assert skip.side == SideSignal.SKIP
    assert skip.reason == "lightgbm_no_trade"


def test_signal_log_summary_includes_lightgbm_reason_for_executed_trade():
    signal = lightgbm_response_to_signal(
        {
            "action": "UP",
            "confidence": 0.7139,
            "p_up": 0.7139,
            "p_down": 0.2861,
            "model_version": "lightgbm-polybot-cache-30d-20260427",
            "target_mode": "current_5m_return",
            "target_reference_price": 112.0,
            "reason_codes": ["target_current_price", "lightgbm_model"],
            "data_quality": {"ok": True, "warnings": [], "max_feed_staleness_ms": 1200},
        },
        current_ts=1_120,
        up_ask=0.62,
        down_ask=0.41,
    )

    summary = signal_log_summary(signal)

    assert "signal=UP" in summary
    assert "score=0.714" in summary
    assert "p_up=0.714" in summary
    assert "p_down=0.286" in summary
    assert "confidence=0.714" in summary
    assert "model=lightgbm-polybot-cache-30d-20260427" in summary
    assert "target=current_5m_return@112.00" in summary
    assert "quality=ok" in summary
    assert "reasons=target_current_price,lightgbm_model" in summary


def test_decode_chainlink_latest_round_data_decodes_answer_and_updated_timestamp():
    def word(value):
        return hex(value & ((1 << 256) - 1))[2:].rjust(64, "0")

    payload = "0x" + "".join([
        word(123),
        word(6400012345678),
        word(1700000000),
        word(1700000001),
        word(123),
    ])

    point = decode_chainlink_latest_round_data(payload)

    assert point.ts == 1700000001
    assert point.close == pytest.approx(64000.12345678)
    assert point.open == point.close


def test_decode_chainlink_streams_latest_point_uses_newest_benchmark_node():
    payload = {
        "data": {
            "allStreamValuesGenerics": {
                "nodes": [
                    {
                        "feedId": "0xfeed",
                        "valueNumeric": "77996.90492",
                        "validAfterTs": "2026-04-26T17:24:33.14534+00:00",
                        "attributeName": "benchmark",
                    },
                    {
                        "feedId": "0xfeed",
                        "valueNumeric": "77995.0",
                        "validAfterTs": "2026-04-26T17:24:32.009086+00:00",
                        "attributeName": "benchmark",
                    },
                ]
            }
        }
    }

    point = decode_chainlink_streams_latest_point(payload)

    assert point.ts == 1777224273
    assert point.close == pytest.approx(77996.90492)
    assert point.open == point.close


def test_chainlink_data_streams_url_defaults_to_btc_usd_cexprice_benchmark():
    url = chainlink_data_streams_url({})

    assert "live-data-engine-stream-data" in url
    assert "feedId=0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8" in url
    assert "attributeName=benchmark" in url


def test_should_retry_entry_attempt_only_for_bounded_fok_kills_with_consensus():
    cfg = {"entry_retry_on_fok_kill": True, "entry_max_attempts_per_market": 3}
    fok_kill = {"success": False, "error": "order couldn't be fully filled. FOK orders are fully filled or killed"}

    assert should_retry_entry_attempt(fok_kill, attempts_so_far=1, cfg=cfg, consensus_still_valid=True) is True
    assert should_retry_entry_attempt(fok_kill, attempts_so_far=3, cfg=cfg, consensus_still_valid=True) is False
    assert should_retry_entry_attempt(fok_kill, attempts_so_far=1, cfg=cfg, consensus_still_valid=False) is False
    assert should_retry_entry_attempt({"success": False, "error": "invalid signature"}, attempts_so_far=1, cfg=cfg, consensus_still_valid=True) is False
    assert should_retry_entry_attempt(fok_kill, attempts_so_far=1, cfg={"entry_retry_on_fok_kill": False, "entry_max_attempts_per_market": 3}, consensus_still_valid=True) is False


def test_normalize_attempt_response_accepts_json_string_attempt_rows():
    assert normalize_attempt_response('{"success": false, "error": "order killed"}') == {"success": False, "error": "order killed"}
    assert normalize_attempt_response("plain text") == {"message": "plain text"}


def test_build_fixed_stake_order_uses_exact_stake_and_requires_top_level_depth():
    order = build_fixed_stake_order(best_ask=0.40, top_ask_size=2.6, stake_usd=1.0)
    assert order is not None
    assert order["shares"] == pytest.approx(2.5)
    assert order["limit_price"] == pytest.approx(0.40)
    assert order["stake_usd"] == pytest.approx(1.0)

    insufficient = build_fixed_stake_order(best_ask=0.40, top_ask_size=2.4, stake_usd=1.0)
    assert insufficient is None


def test_build_fixed_stake_order_from_asks_uses_cumulative_depth_with_slippage_buffer():
    order = build_fixed_stake_order_from_asks(
        asks=[
            {"price": 0.40, "size": 1.0},
            {"price": 0.41, "size": 2.0},
        ],
        stake_usd=1.0,
        slippage_ticks=1,
    )

    assert order is not None
    assert order["limit_price"] == pytest.approx(0.41)
    assert order["shares"] == pytest.approx(2.4390)
    assert order["available_notional"] == pytest.approx(1.22)


def test_build_fixed_stake_order_from_asks_rejects_when_depth_inside_buffer_is_insufficient():
    order = build_fixed_stake_order_from_asks(
        asks=[
            {"price": 0.40, "size": 1.0},
            {"price": 0.43, "size": 10.0},
        ],
        stake_usd=1.0,
        slippage_ticks=1,
    )

    assert order is None


def test_build_fixed_stake_order_from_asks_cent_rounds_before_slippage():
    order = build_fixed_stake_order_from_asks(
        asks=[{"price": 0.401, "size": 3.0}],
        stake_usd=1.0,
        slippage_ticks=1,
    )

    assert order is not None
    assert order["limit_price"] == pytest.approx(0.42)


def test_hedge_config_defaults_and_trigger_semantics():
    assert hedge_enabled({}) is False
    assert hedge_enabled({"hedge_consensus_trigger": 0, "hedge_buffer_ticks": 10}) is False
    assert hedge_enabled({"hedge_consensus_trigger": "0", "hedge_profit_buffer": -0.01}) is False
    assert hedge_enabled({"hedge_consensus_trigger": 1, "hedge_buffer_ticks": 0}) is False
    assert hedge_enabled({"hedge_consensus_trigger": 1, "hedge_buffer_ticks": -1}) is False
    assert hedge_enabled({"hedge_consensus_trigger": 1, "hedge_profit_buffer": 0}) is False
    assert hedge_enabled({"hedge_consensus_trigger": 1, "hedge_profit_buffer": -0.01}) is False
    assert hedge_enabled({"hedge_consensus_trigger": 1, "hedge_buffer_ticks": 1}) is True
    assert hedge_profit_buffer({}) == pytest.approx(0.0)
    assert hedge_profit_buffer({"hedge_profit_buffer": 0.01}) == pytest.approx(0.01)
    assert hedge_profit_buffer({"hedge_profit_buffer": -0.01}) == pytest.approx(-0.01)
    assert hedge_profit_buffer({"hedge_buffer_ticks": 10}) == pytest.approx(0.10)
    assert hedge_profit_buffer({"hedge_buffer_ticks": 0}) == pytest.approx(0.0)
    assert hedge_profit_buffer({"hedge_buffer_ticks": -1}) == pytest.approx(-0.01)
    assert hedge_consensus_trigger({}) == 0
    assert hedge_consensus_trigger({"hedge_consensus_trigger": "1"}) == 1
    assert hedge_consensus_trigger({"hedge_consensus_trigger": "10"}) == 10

    assert should_arm_hedge(entry_side="UP", signal_score=-5, cfg={"hedge_consensus_trigger": 0, "hedge_buffer_ticks": -10}) is False
    assert should_arm_hedge(entry_side="DOWN", signal_score=5, cfg={"hedge_consensus_trigger": 0, "hedge_profit_buffer": -0.05}) is False

    # Trigger is subtracted directly from min_consensus.
    # With a 5/5 entry and trigger=2, hedge at 3/5 or weaker.
    cfg = {"min_consensus": 5, "hedge_consensus_trigger": 2, "hedge_buffer_ticks": 1}
    assert should_arm_hedge(entry_side="UP", signal_score=3, cfg=cfg) is True
    assert should_arm_hedge(entry_side="UP", signal_score=4, cfg=cfg) is False
    assert should_arm_hedge(entry_side="DOWN", signal_score=-3, cfg=cfg) is True
    assert should_arm_hedge(entry_side="DOWN", signal_score=-4, cfg=cfg) is False

    # Do not clamp the trigger to the 5-vote consensus range: 5 - 10 = -5,
    # so an UP entry only hedges at -5 and a DOWN entry only hedges at +5.
    cfg = {"min_consensus": 5, "hedge_consensus_trigger": 10, "hedge_buffer_ticks": 1}
    assert hedge_arm_score_threshold(cfg) == -5
    assert should_arm_hedge(entry_side="UP", signal_score=-5, cfg=cfg) is True
    assert should_arm_hedge(entry_side="UP", signal_score=-4, cfg=cfg) is False
    assert should_arm_hedge(entry_side="DOWN", signal_score=5, cfg=cfg) is True
    assert should_arm_hedge(entry_side="DOWN", signal_score=4, cfg=cfg) is False


def test_update_hedge_armed_tracks_current_weakened_consensus_not_latched_state():
    cfg = {"min_consensus": 5, "hedge_consensus_trigger": 2, "hedge_buffer_ticks": 1}

    assert update_hedge_armed(currently_armed=False, entry_side="DOWN", signal_score=-3, cfg=cfg) is True
    assert update_hedge_armed(currently_armed=True, entry_side="DOWN", signal_score=-4, cfg=cfg) is False
    assert update_hedge_armed(currently_armed=True, entry_side="UP", signal_score=4, cfg=cfg) is False
    assert update_hedge_armed(currently_armed=False, entry_side="UP", signal_score=3, cfg=cfg) is True


def test_hedge_order_buys_opposite_same_shares_when_profitable_and_min_notional_met():
    assert opposite_outcome("UP") == "DOWN"
    assert opposite_outcome("DOWN") == "UP"

    order = build_hedge_order_from_asks(
        asks=[{"price": 0.24, "size": 1.0}, {"price": 0.25, "size": 3.0}],
        entry_price=0.57,
        entry_shares=4.0,
        hedge_buffer=0.10,
        min_notional=1.0,
    )

    assert order is not None
    assert order["shares"] == pytest.approx(4.0)
    assert order["limit_price"] == pytest.approx(0.25)
    assert order["target_price"] == pytest.approx(0.2594112)
    assert order["entry_fee_usdc"] == pytest.approx(0.0705888)
    assert order["stake_usd"] == pytest.approx(1.0)
    assert order["locked_pnl"] == pytest.approx(4.0 * (1.0 - 0.57 - 0.25) - 0.0705888)


def test_hedge_order_buys_min_notional_when_same_share_hedge_is_too_small():
    order = build_hedge_order_from_asks(
        asks=[{"price": 0.064, "size": 20.0}],
        entry_price=0.92,
        entry_shares=1.0869,
        hedge_buffer=0.01,
        min_notional=1.0,
    )

    assert order is not None
    assert order["shares"] == pytest.approx(15.625)
    assert order["limit_price"] == pytest.approx(0.064)
    assert order["stake_usd"] == pytest.approx(1.0)
    assert order["sizing_mode"] == "min_notional"
    assert order["target_price"] == pytest.approx(0.06424029952)


def test_hedge_order_buys_min_notional_even_when_visible_depth_is_below_minimum():
    order = build_hedge_order_from_asks(
        asks=[{"price": 0.064, "size": 1.0}],
        entry_price=0.92,
        entry_shares=1.0869,
        hedge_buffer=0.01,
        min_notional=1.0,
    )

    assert order is not None
    assert order["shares"] == pytest.approx(15.625)
    assert order["limit_price"] == pytest.approx(0.064)
    assert order["stake_usd"] == pytest.approx(1.0)
    assert order["sizing_mode"] == "min_notional"


def test_hedge_order_subtracts_entry_taker_fee_from_target_price():
    assert polymarket_taker_fee_usdc(price=0.50, shares=2.0) == pytest.approx(0.036)

    order = build_hedge_order_from_asks(
        asks=[{"price": 0.36, "size": 2.0}],
        entry_price=0.50,
        entry_shares=2.0,
        hedge_buffer=0.10,
        min_notional=0.01,
    )

    assert order is not None
    assert order["target_price"] == pytest.approx(0.364)
    assert order["entry_fee_usdc"] == pytest.approx(0.036)
    assert order["locked_pnl"] == pytest.approx(2.0 * (1.0 - 0.50 - 0.36) - 0.036)

    too_expensive = build_hedge_order_from_asks(
        asks=[{"price": 0.37, "size": 2.0}],
        entry_price=0.50,
        entry_shares=2.0,
        hedge_buffer=0.10,
        min_notional=0.01,
    )
    assert too_expensive is None



def test_momentum_live_state_tracks_hedged_position_and_locked_pnl():
    end_dt = datetime.fromtimestamp(300, tz=timezone.utc)
    state = MomentumLiveState(market="BTC 5m", market_slug="btc-updown-5m-0", end_dt=end_dt)
    state.apply_fill(side="UP", token_id="yes-token", entry_price=0.57, stake_usd=1.0, shares=2.0, ts=100)
    state.apply_hedge(side="DOWN", token_id="no-token", hedge_price=0.31, stake_usd=0.62, shares=2.0, ts=150)

    snapshot = state.state_dict()
    assert snapshot["side"] == "HEDGED"
    assert snapshot["hedge_side"] == "DOWN"
    assert snapshot["hedge_entry"] == pytest.approx(0.31)
    assert snapshot["locked_pnl"] == pytest.approx(0.24)
    assert state.has_position() is False
    assert state.is_hedged() is True


def test_momentum_live_state_settles_win_and_clears_position():
    end_dt = datetime.fromtimestamp(300, tz=timezone.utc)
    state = MomentumLiveState(market="BTC 5m", market_slug="btc-updown-5m-0", end_dt=end_dt)
    state.apply_fill(side="UP", token_id="yes-token", entry_price=0.50, stake_usd=1.0, shares=2.0, ts=295)

    state.mark_price(0.49)
    assert state.state_dict()["pnl"] == pytest.approx(-0.02)

    realized = state.settle(winner=SideSignal.UP, ts=301)
    assert realized == pytest.approx(1.0)
    snapshot = state.state_dict()
    assert snapshot["side"] == "FLAT"
    assert snapshot["size"] == pytest.approx(0.0)
    assert snapshot["pnl"] == pytest.approx(0.0)
    assert state.cash == pytest.approx(1.0)


def test_supervisor_live_yaml_includes_requested_5of5_momentum_buffer_variants():
    import yaml

    with open("config/supervisor-live.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    strategies = {s["id"]: s for s in cfg["strategies"]}

    base = strategies["live_momentum_consensus_07_dynamic"]
    assert base["hedge_buffer_ticks"] == 0
    assert base["hedge_consensus_trigger"] == 0
    assert base["hedge_max_attempts_per_market"] == 0

    v2 = strategies["live_momentum_consensus_07_dynamic_v2"]
    assert v2["hedge_buffer_ticks"] == 0
    assert v2["hedge_consensus_trigger"] == 0
    assert v2["hedge_max_attempts_per_market"] == 0

    binance = strategies["live_momentum_consensus_07_dynamic_binance_5of5_hedge_1c"]
    assert binance["name"] == "Binance 5/5 first-consensus with hedge buffers 1c"
    assert binance["kind"] == "momentum_consensus_dynamic_entry_5m"
    assert binance["min_consensus"] == 5
    assert binance["signal_source"] == "binance"
    assert binance["entry_slippage_ticks"] == 0
    assert binance["hedge_buffer_ticks"] == 1
    assert binance["hedge_consensus_trigger"] == 2
    assert binance["max_executed_orders"] == 0

    chainlink = strategies["live_momentum_consensus_07_dynamic_chainlink_5of5_hedge_10c"]
    assert chainlink["name"] == "Chainlink 5/5 first-consensus with hedge buffers 10c"
    assert chainlink["kind"] == "momentum_consensus_dynamic_entry_5m"
    assert chainlink["min_consensus"] == 5
    assert chainlink["signal_source"] == "chainlink"
    assert chainlink["entry_slippage_ticks"] == 0
    assert chainlink["hedge_buffer_ticks"] == 10
    assert chainlink["hedge_consensus_trigger"] == 1
    assert chainlink["max_executed_orders"] == 0

    lightgbm = strategies["live_lightgbm_btc5m_v1"]
    assert lightgbm["name"] == "BTC 5m LightGBM module signal v1"
    assert lightgbm["kind"] == "momentum_consensus_dynamic_entry_5m"
    assert lightgbm["signal_source"] == "lightgbm"
    assert lightgbm["lightgbm_predict_url"] == "http://127.0.0.1:8787/predict"
    assert lightgbm["max_order_size"] == 1.0
    assert lightgbm["max_position_size"] == 1.0
    assert lightgbm["entry_slippage_ticks"] == 0
    assert lightgbm["hedge_buffer_ticks"] == 0
    assert lightgbm["hedge_consensus_trigger"] == 0


def test_supervisor_lightgbm_live_yaml_includes_model_v3_alias():
    import yaml

    with open("config/supervisor-lightgbm-live.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    strategies = {s["id"]: s for s in cfg["strategies"]}

    v3 = strategies["live_lightgbm_probability_edge_btc5m_v3"]
    assert v3["name"] == "5m BTC - Model v3"
    assert v3["kind"] == "momentum_consensus_dynamic_entry_5m"
    assert v3["signal_source"] == "lightgbm_probability_v2"
    assert v3["target_mode"] == "polymarket_strike"
    assert v3["lightgbm_predict_url"] == "http://127.0.0.1:8789/predict"
    assert v3["min_model_edge"] == 0.20
    assert v3["max_order_size"] == 1.0
    assert v3["max_position_size"] == 1.0
    assert v3["entry_slippage_ticks"] == 0
    assert v3["hedge_buffer_ticks"] == 0
    assert v3["hedge_consensus_trigger"] == 0
