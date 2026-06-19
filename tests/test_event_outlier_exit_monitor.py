from pathlib import Path


def test_event_outlier_exit_monitor_uses_strategy_id_from_environment():
    """Each live Weather Pump variant must monitor exits for its own strategy row."""
    script = Path("scripts/tmp_event_outlier_exit_only_once.py").read_text(encoding="utf-8")

    first_assignment = script.split("STRATEGY_ID", 1)[1].split("\n", 1)[0]
    assert "os.getenv('EVENT_OUTLIER_STRATEGY_ID'" in script or 'os.getenv("EVENT_OUTLIER_STRATEGY_ID"' in script
    assert not first_assignment.strip().startswith("='live_event_outlier_weather_pump_v1'")


def test_event_outlier_scanner_passes_strategy_id_to_exit_monitor_child_process():
    """The scanner should propagate EVENT_OUTLIER_STRATEGY_ID when invoking exit-only monitor."""
    script = Path("scripts/event_outlier_weather_pump_scanner_loop.js").read_text(encoding="utf-8")

    assert "EVENT_OUTLIER_STRATEGY_ID: STRATEGY_ID" in script
    assert "env: childEnv" in script
