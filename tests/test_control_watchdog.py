from pathlib import Path
import subprocess


def test_control_watchdog_maps_passive_crypto_mm_strategy_to_systemd_service():
    """Dashboard start/stop must control the passive MM service, not just DB status."""
    script = Path("scripts/arb_sniper_control_watchdog.js").read_text(encoding="utf-8")

    assert "crypto_passive_mm_btc_15m" in script
    assert "polybot-crypto-passive-mm-btc15m.service" in script


def test_control_watchdog_maps_event_outlier_weather_scanner_to_systemd_service():
    """Dashboard start/stop must run the event-outlier live-data scanner service."""
    script = Path("scripts/arb_sniper_control_watchdog.js").read_text(encoding="utf-8")

    assert "live_event_outlier_weather_pump_v1" in script
    assert "polybot-event-outlier-weather-pump-scanner.service" in script


def test_control_watchdog_covers_all_weather_outlier_service_configs():
    """Every per-city weather outlier service config needs dashboard start/stop coverage."""
    script = Path("scripts/arb_sniper_control_watchdog.js").read_text(encoding="utf-8")

    for config_path in Path("config").glob("weather-outlier-sniper-*-live.yaml"):
        text = config_path.read_text(encoding="utf-8")
        strategy_id = next(line.split(":", 1)[1].strip() for line in text.splitlines() if line.startswith("id:"))
        service_slug = config_path.name.removeprefix("weather-outlier-sniper-").removesuffix("-live.yaml")
        service = f"polybot-weather-outlier-sniper-{service_slug}.service"

        assert strategy_id in script
        assert service in script


def test_control_watchdog_systemd_timer_is_enabled_for_dashboard_bridge():
    """The control bridge must be scheduled; otherwise dashboard state never reaches systemd."""
    timer_path = Path.home() / ".config/systemd/user/polybot-arb-sniper-control-watchdog.timer"
    assert timer_path.exists()

    result = subprocess.run(
        ["systemctl", "--user", "is-enabled", "polybot-arb-sniper-control-watchdog.timer"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
