import pytest

import polybot.live.weather_safety_filter as weather_filter_module
from polybot.live.weather_safety_filter import STATIONS, _twc_to_open_meteo_shape, _wttr_to_open_meteo_shape, evaluate_weather_safety, event_target_date, forecast_provider_for_resolution_source, forecast_role_metadata, get_forecast_with_fallback


def forecast(code=0, rain=0, showers=0, snow=0, gust=10, wind=5, dates=None):
    dates = dates or ["2026-05-03"]
    times = [f"{day}T{h:02d}:00" for day in dates for h in range(24)]

    def hourly_values(value, default=0):
        if isinstance(value, dict):
            return [value.get(h, default) for _day in dates for h in range(24)]
        return [value] * len(times)

    rain_values = hourly_values(rain)
    shower_values = hourly_values(showers)
    snow_values = hourly_values(snow)
    return {
        "hourly": {
            "time": times,
            "temperature_2m": [20] * len(times),
            "precipitation": [r + s for r, s in zip(rain_values, shower_values)],
            "rain": rain_values,
            "showers": shower_values,
            "snowfall": snow_values,
            "wind_gusts_10m": hourly_values(gust, default=10),
            "wind_speed_10m": hourly_values(wind, default=5),
            "weather_code": hourly_values(code),
        },
        "daily": {
            "time": ["2026-05-02"] + dates,
            "temperature_2m_max": [19] + [20] * len(dates),
            "precipitation_sum": [0] + [sum(rain_values) + sum(shower_values)] * len(dates),
            "snowfall_sum": [0] + [sum(snow_values)] * len(dates),
        },
    }


def evaluate(city, fc, event_slug=None):
    return evaluate_weather_safety(STATIONS[city], None, [], fc, None, None, event_slug=event_slug)


def test_ordinary_rain_is_not_standalone_yellow_for_non_watch_city():
    result = evaluate("london", forecast(code=61, rain=0.5))

    assert result["gate"] == "GREEN"
    assert result["size_multiplier"] == 1.0
    assert result["metrics"]["rain_signal"] is True
    assert result["metrics"]["heavy_rain_peak_signal"] is False
    assert "ordinary rain alone is not a sizing trigger" in "; ".join(result["warnings"])


def test_heavy_rain_10_16_is_yellow_for_non_watch_city():
    result = evaluate("london", forecast(code=65, rain=3.0))

    assert result["gate"] == "YELLOW"
    assert result["size_multiplier"] == 1.0
    assert result["metrics"]["heavy_rain_peak_signal"] is True
    assert "heavy rain 10-16 local" in result["reason"]


def test_heavy_wind_10_16_is_yellow_for_non_watch_city():
    result = evaluate("london", forecast(gust=55, wind=20))

    assert result["gate"] == "YELLOW"
    assert result["metrics"]["heavy_wind_peak_signal"] is True
    assert "heavy wind 10-16 local" in result["reason"]


def test_heavy_rain_outside_10_16_does_not_trigger_yellow():
    result = evaluate("london", forecast(code={8: 65, 18: 65}, rain={8: 5.0, 18: 5.0}))

    assert result["gate"] == "GREEN"
    assert result["metrics"]["rain_signal"] is False
    assert result["metrics"]["heavy_rain_peak_signal"] is False


def test_heavy_wind_outside_10_16_does_not_trigger_yellow():
    result = evaluate("london", forecast(gust={8: 70, 18: 70}, wind={8: 40, 18: 40}))

    assert result["gate"] == "GREEN"
    assert result["metrics"]["heavy_wind_peak_signal"] is False


def test_qingdao_is_permanent_watch_city_yellow():
    result = evaluate("qingdao", forecast(code=0, rain=0.0, gust=5, wind=5))

    assert result["gate"] == "YELLOW"
    assert result["metrics"]["empirical_watch_city"] is True
    assert "empirical watch city" in result["reason"]


def test_shenzhen_is_permanent_watch_city_yellow():
    result = evaluate("shenzhen", forecast(code=0, rain=0.0, gust=5, wind=5))

    assert result["gate"] == "YELLOW"
    assert result["metrics"]["empirical_watch_city"] is True
    assert "empirical watch city" in result["reason"]


def test_guangzhou_is_permanent_watch_city_yellow():
    result = evaluate("guangzhou", forecast(code=0, rain=0, gust=5, wind=5))

    assert result["gate"] == "YELLOW"
    assert result["metrics"]["empirical_watch_city"] is True
    assert "empirical watch city" in result["reason"]


def test_istanbul_is_permanent_watch_city_yellow():
    result = evaluate("istanbul", forecast(code=0, rain=0, gust=5, wind=5))

    assert result["gate"] == "YELLOW"
    assert result["metrics"]["empirical_watch_city"] is True
    assert "empirical watch city" in result["reason"]


def test_watch_city_escalates_heavy_rain_to_red():
    result = evaluate("toronto", forecast(code=65, rain=3.0))

    assert result["gate"] == "RED"
    assert result["size_multiplier"] == 0.0
    assert "watch city + heavy rain 10-16 local" in result["reason"]


def test_watch_city_escalates_heavy_wind_to_red():
    result = evaluate("toronto", forecast(gust=55, wind=20))

    assert result["gate"] == "RED"
    assert result["size_multiplier"] == 0.0
    assert "watch city + heavy wind 10-16 local" in result["reason"]


def test_watch_city_snow_stays_red():
    result = evaluate("toronto", forecast(code=71, snow=0.1))

    assert result["gate"] == "RED"
    assert result["size_multiplier"] == 0.0
    assert "snow" in result["reason"].lower()


def test_event_target_date_parses_connected_weather_slug():
    assert event_target_date("highest-temperature-in-jakarta-on-may-5-2026") == "2026-05-05"
    assert event_target_date("highest-temperature-in-nyc-on-september-12-2026") == "2026-09-12"


def test_filter_uses_connected_market_target_date_not_default_today():
    fc = forecast(
        code={12: 65},
        rain={12: 4.0},
        dates=["2026-05-03", "2026-05-04", "2026-05-05"],
    )
    # Without a connected event date this test fixture's default local day is
    # 2026-05-04. Force the signal off on non-target dates and on only for May 5.
    idx = {t: i for i, t in enumerate(fc["hourly"]["time"])}
    for name in ("precipitation", "rain", "weather_code"):
        fc["hourly"][name] = [0] * len(fc["hourly"]["time"])
    may5_noon = idx["2026-05-05T12:00"]
    fc["hourly"]["precipitation"][may5_noon] = 4.0
    fc["hourly"]["rain"][may5_noon] = 4.0
    fc["hourly"]["weather_code"][may5_noon] = 65

    result = evaluate("london", fc, event_slug="highest-temperature-in-london-on-may-5-2026")

    assert result["metrics"]["event_target_date"] == "2026-05-05"
    assert result["metrics"]["local_date"] == "2026-05-05"
    assert result["gate"] == "YELLOW"
    assert "heavy rain 10-16 local" in result["reason"]


def test_wttr_fallback_shape_triggers_same_heavy_rain_gate():
    fc = _wttr_to_open_meteo_shape({
        "weather": [{
            "date": "2026-05-05",
            "maxtempC": "22",
            "totalSnow_cm": "0",
            "hourly": [{
                "time": "1200",
                "tempC": "20",
                "precipMM": "3.5",
                "chanceofsnow": "0",
                "chanceofrain": "80",
                "weatherCode": "296",
                "cloudcover": "90",
                "pressure": "1012",
                "windspeedKmph": "10",
                "winddirDegree": "180",
                "WindGustKmph": "20",
            }],
        }]
    })
    fc["_provider_errors"] = ["open_meteo=HTTPStatusError: 429"]

    result = evaluate("london", fc, event_slug="highest-temperature-in-london-on-may-5-2026")

    assert result["metrics"]["forecast_provider_used"] == "wttr"
    assert result["metrics"]["forecast_provider_errors"]
    assert result["gate"] == "YELLOW"
    assert "heavy rain 10-16 local" in result["reason"]


def test_forecast_high_reports_celsius_and_fahrenheit_for_boundary_veto():
    result = evaluate("london", forecast(dates=["2026-05-05"]), event_slug="highest-temperature-in-london-on-may-5-2026")

    assert result["metrics"]["forecast_high_c"] == 20
    assert result["metrics"]["forecast_high_f"] == 68


def test_resolution_source_uses_twc_primary_for_wunderground_markets():
    assert forecast_provider_for_resolution_source("Wunderground station history") == "twc_weather"
    assert forecast_provider_for_resolution_source("NOAA/Weather.gov WRH time series") == "twc_weather"
    assert forecast_role_metadata("open_meteo", "NOAA/Weather.gov WRH time series")["forecast_exact_resolution_service"] is False


@pytest.mark.asyncio
async def test_twc_primary_records_resolution_metadata(monkeypatch):
    async def fake_twc(*args, **kwargs):
        fc = forecast(dates=["2026-05-05"])
        fc["_provider"] = "twc_weather"
        fc["_twc_daily_highs_are_integer_c"] = True
        return fc

    monkeypatch.setattr(weather_filter_module, "get_twc_weather_forecast", fake_twc)

    fc, provider, errors = await get_forecast_with_fallback(
        object(),
        51.5,
        -0.1,
        target_date="2026-05-05",
        primary_provider="twc_weather",
        station_id="EGLC",
        resolution_source="Wunderground station history",
    )

    assert provider == "twc_weather"
    assert fc["_provider_fallback_from"] is None
    assert errors == []
    assert fc["_forecast_provider_role"] == "twc_weather_primary"
    assert fc["_forecast_exact_resolution_service"] is True
    assert fc["_resolution_source"] == "Wunderground station history"

    result = evaluate("london", fc, event_slug="highest-temperature-in-london-on-may-5-2026")
    assert result["metrics"]["forecast_provider_used"] == "twc_weather"
    assert result["metrics"]["forecast_provider_fallback_from"] is None
    assert result["metrics"]["forecast_provider_role"] == "twc_weather_primary"
    assert result["metrics"]["forecast_exact_resolution_service"] is True
    assert result["metrics"]["resolution_source"] == "Wunderground station history"
    assert result["metrics"]["forecast_daily_highs_are_integer_c"] is True


@pytest.mark.asyncio
async def test_twc_primary_falls_back_to_open_meteo_only(monkeypatch):
    async def fake_twc(*args, **kwargs):
        raise RuntimeError("twc unavailable")

    async def fake_open_meteo(*args, **kwargs):
        fc = forecast(dates=["2026-05-05"])
        fc["_provider"] = "open_meteo"
        return fc

    monkeypatch.setattr(weather_filter_module, "get_twc_weather_forecast", fake_twc)
    monkeypatch.setattr(weather_filter_module, "get_open_meteo", fake_open_meteo)

    fc, provider, errors = await get_forecast_with_fallback(
        object(), 51.5, -0.1, target_date="2026-05-05", primary_provider="twc_weather", station_id="EGLC", resolution_source="Wunderground station history"
    )

    assert provider == "open_meteo"
    assert fc["_provider_fallback_from"] == "twc_weather"
    assert any("twc_weather=RuntimeError" in e for e in errors)
    assert fc["_forecast_provider_role"] == "twc_weather_primary"


def test_twc_forecast_shape_uses_hourly_and_daily_highs():
    fc = _twc_to_open_meteo_shape(
        {
            "validTimeLocal": ["2026-05-05T10:00:00+0100", "2026-05-05T11:00:00+0100"],
            "temperature": [18, 19],
            "qpf": [0.1, 0.0],
            "qpfRain": [0.1, 0.0],
            "qpfSnow": [0.0, 0.0],
            "precipChance": [60, 10],
            "precipType": ["rain", "rain"],
            "windSpeed": [20, 21],
            "windGust": [30, 31],
        },
        {"validTimeLocal": ["2026-05-05T07:00:00+0100"], "calendarDayTemperatureMax": [20], "qpf": [0.1], "qpfRain": [0.1], "qpfSnow": [0.0]},
    )

    result = evaluate("london", fc, event_slug="highest-temperature-in-london-on-may-5-2026")
    assert result["metrics"]["forecast_provider_used"] == "twc_weather"
    assert result["metrics"]["forecast_high_c"] == 20
    assert result["metrics"]["forecast_high_source"] == "daily"
