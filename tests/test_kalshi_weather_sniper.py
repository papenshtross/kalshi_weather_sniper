from datetime import date

from polybot.adapters.kalshi.client import dollars_to_probability, parse_market
from polybot.live.kalshi_weather_sniper import best_candidate, filter_markets_for_date, kalshi_date_code, load_config
from polybot.live.kalshi_weather_universe import ALL_KALSHI_HIGH_TEMP_SERIES, KALSHI_HIGH_TEMP_SERIES, boundary_veto_reason


def test_dollars_to_probability_accepts_cents_and_dollars():
    assert dollars_to_probability(73) == 0.73
    assert dollars_to_probability("0.4200") == 0.42
    assert dollars_to_probability(None) is None


def test_parse_kalshi_temperature_market_shape():
    m = parse_market({
        "ticker": "KXHIGHLAX-26JUN19-B72.5",
        "event_ticker": "KXHIGHLAX-26JUN19",
        "series_ticker": "KXHIGHLAX",
        "title": "Will the high temp in LA be 72-73° on Jun 19, 2026?",
        "strike_type": "between",
        "floor_strike": "72",
        "cap_strike": "73",
        "yes_ask_dollars": "0.1200",
        "yes_bid_dollars": "0.0800",
    })
    assert m.ticker == "KXHIGHLAX-26JUN19-B72.5"
    assert m.temp_mid_f == 72.5
    assert m.yes_ask == 0.12


def test_nws_boundary_veto_rejects_near_forecast_boundary():
    assert boundary_veto_reason(72.5, 73.0, 3.6)
    assert boundary_veto_reason(80.0, 73.0, 3.6) is None


def test_best_candidate_picks_far_quoted_non_vetoed_market():
    near = parse_market({"ticker": "near", "title": "near", "floor_strike": 72, "cap_strike": 73, "yes_ask_dollars": "0.10"})
    far = parse_market({"ticker": "far", "title": "far", "floor_strike": 82, "cap_strike": 83, "yes_ask_dollars": "0.11"})
    chosen, reason = best_candidate([near, far], forecast_high_f=73.0, threshold_f=3.6)
    assert chosen is far
    assert "selected" in reason


def test_date_filter_keeps_only_target_daily_markets():
    assert kalshi_date_code(date(2026, 6, 19)) == "26JUN19"
    old = parse_market({"ticker": "KXHIGHLAX-26JUN18-T73", "title": "old", "floor_strike": 73, "yes_ask_dollars": "0.01"})
    cur = parse_market({"ticker": "KXHIGHLAX-26JUN19-T73", "title": "cur", "floor_strike": 73, "yes_ask_dollars": "0.01"})
    assert filter_markets_for_date([old, cur], date(2026, 6, 19)) == [cur]


def test_load_config_exposes_weather_outlier_dashboard_aliases(tmp_path):
    cfg_path = tmp_path / "kalshi.yaml"
    cfg_path.write_text("id: live_kalshi_weather_sniper_v1\norder_size_usd: 1.0\n")
    cfg = load_config(cfg_path)
    assert cfg["kind"] == "kalshi_weather_sniper"
    assert cfg["outlier_order_usd"] == 1.0
    assert cfg["order_limit_usd"] == 1.0
    assert cfg["outlier_temperature_offset_degrees"] == 4
    assert cfg["weather_outlier_rebuy_tiers"] == "1:1,2:2,3:3"
    assert cfg["weather_safety_filter_report_enabled"] is True
    assert len(cfg["series"]) == 20
    assert cfg["series"]["san-francisco"]["polymarket_boundary_veto_degrees_c"] == 3.5
    assert cfg["series"]["san-francisco"]["nws_boundary_veto_degrees_f"] == 6.3
    assert cfg["series"]["atlanta"]["polymarket_boundary_veto_degrees_c"] == 2.0
    assert cfg["series"]["phoenix"].get("polymarket_boundary_veto_degrees_c") is None


def test_copied_polymarket_risk_city_list_is_smaller_than_full_kalshi_config():
    assert len(ALL_KALSHI_HIGH_TEMP_SERIES) == 20
    assert len(KALSHI_HIGH_TEMP_SERIES) == 11
    assert {"los-angeles", "atlanta", "houston", "austin", "chicago", "dallas", "denver", "miami", "nyc", "san-francisco", "seattle"}.issubset(KALSHI_HIGH_TEMP_SERIES)
    assert ALL_KALSHI_HIGH_TEMP_SERIES["phoenix"]["inherited_polymarket_risk_city"] is False
