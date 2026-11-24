import json
import re

from data_generator import generate_ride_event


def test_event_has_the_schema_the_spark_jobs_expect():
    event = generate_ride_event()
    assert set(event) == {"trip_id", "driver_id", "distance_km", "fare_amount", "timestamp"}


def test_event_values_are_in_range():
    for _ in range(200):
        e = generate_ride_event()
        assert 1 <= e["driver_id"] <= 100
        assert 1 <= e["distance_km"] <= 50
        assert 5 <= e["fare_amount"] <= 150


def test_timestamp_matches_the_format_the_jobs_parse():
    ts = generate_ride_event()["timestamp"]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", ts)


def test_event_serialises_to_one_json_line():
    line = json.dumps(generate_ride_event())
    assert "\n" not in line
    assert json.loads(line)["trip_id"]
