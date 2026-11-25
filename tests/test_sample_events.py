"""Contract tests for the sample events produced by the Arduino and the edge script."""
import json
from datetime import datetime
from pathlib import Path

import pytest

SAMPLE_FILE = Path(__file__).resolve().parents[1] / "sample_data" / "sample_events.jsonl"

REQUIRED_FIELDS = {
    "event": str,
    "id": int,
    "start_sensor": int,
    "direction": str,
    "elapsed_ms": int,
    "distance_cm": (int, float),
    "speed_kmh": (int, float),
    "received_at": str,
}
DIRECTION_BY_START_SENSOR = {1: "right_to_left", 2: "left_to_right"}


def load_events():
    with SAMPLE_FILE.open(encoding="utf-8") as file:
        return [json.loads(line) for line in file if line.strip()]


EVENTS = load_events()


def test_sample_file_is_not_empty():
    assert len(EVENTS) > 0


@pytest.mark.parametrize("event", EVENTS)
def test_required_fields_and_types(event):
    for field, expected_type in REQUIRED_FIELDS.items():
        assert field in event, f"missing field: {field}"
        assert isinstance(event[field], expected_type), f"wrong type for {field}"


@pytest.mark.parametrize("event", EVENTS)
def test_event_passes_silver_expectations(event):
    assert event["speed_kmh"] > 0
    assert event["elapsed_ms"] > 0
    assert event["direction"] in DIRECTION_BY_START_SENSOR.values()
    assert datetime.fromisoformat(event["received_at"]).tzinfo is not None


@pytest.mark.parametrize("event", EVENTS)
def test_direction_matches_first_sensor(event):
    assert event["direction"] == DIRECTION_BY_START_SENSOR[event["start_sensor"]]


@pytest.mark.parametrize("event", EVENTS)
def test_speed_matches_distance_over_time(event):
    # v [km/h] = (d [cm] / 100) / (t [ms] / 1000) * 3.6 = d / t * 36
    expected_kmh = event["distance_cm"] / event["elapsed_ms"] * 36
    assert event["speed_kmh"] == pytest.approx(expected_kmh, abs=0.01)
