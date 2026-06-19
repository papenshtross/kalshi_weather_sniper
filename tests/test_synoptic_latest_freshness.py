from datetime import datetime, timezone
import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_synoptic_latest_freshness.py"
_SPEC = importlib.util.spec_from_file_location("check_synoptic_latest_freshness", _SCRIPT)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
assess_payload = _MODULE.assess_payload


def test_assess_synoptic_latest_payload_marks_fresh_observation():
    payload = {
        "STATION": [
            {
                "STID": "KLGA",
                "NAME": "La Guardia Airport",
                "OBSERVATIONS": {
                    "air_temp_value_1": {"value": 25.6, "date_time": "2026-06-05T12:00:00Z"}
                },
            }
        ]
    }
    report = assess_payload(
        payload,
        datetime(2026, 6, 5, 12, 3, 0, tzinfo=timezone.utc),
        ["air_temp"],
        max_age_sec=300,
    )

    assert report["station_count"] == 1
    assert report["fresh_count"] == 1
    row = report["stations"][0]
    assert row["station"] == "KLGA"
    assert row["value"] == 25.6
    assert row["age_sec"] == 180.0
    assert row["fresh"] is True


def test_assess_synoptic_latest_payload_marks_stale_observation():
    payload = {
        "STATION": [
            {
                "STID": "KLGA",
                "OBSERVATIONS": {
                    "air_temp_value_1": {"value": 25.6, "date_time": "2026-06-05T11:00:00Z"}
                },
            }
        ]
    }
    report = assess_payload(
        payload,
        datetime(2026, 6, 5, 12, 3, 0, tzinfo=timezone.utc),
        ["air_temp"],
        max_age_sec=300,
    )

    assert report["fresh_count"] == 0
    assert report["stale_count"] == 1
    assert report["stations"][0]["age_sec"] == 3780.0
    assert report["stations"][0]["fresh"] is False
