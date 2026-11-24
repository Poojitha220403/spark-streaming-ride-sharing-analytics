"""End-to-end tests: run each Structured Streaming job against the real socket
generator for a short time and check what it writes. They start a local Spark
session, so they are slow (about a minute each) and need Java 11 or 17."""
import csv
from pathlib import Path

import pytest

pytestmark = pytest.mark.spark


def read_csvs(folder: Path):
    rows = []
    for f in sorted(folder.rglob("part-*.csv")):
        with open(f, newline="") as fh:
            rows.extend(csv.DictReader(fh))
    return rows


def test_task1_writes_each_parsed_event_as_its_own_csv(run_stream_job):
    out, exit_code = run_stream_job("task1.py", seconds=40)
    assert exit_code is None, "task1 crashed"
    rows = read_csvs(out / "outputs" / "task1")
    assert len(rows) >= 3
    assert set(rows[0]) == {"trip_id", "driver_id", "distance_km", "fare_amount", "timestamp"}
    assert all(1 <= int(r["driver_id"]) <= 100 for r in rows)
    assert all(float(r["fare_amount"]) >= 5 for r in rows)


def test_task2_writes_per_driver_totals(run_stream_job):
    out, exit_code = run_stream_job("task2.py", seconds=45)
    assert exit_code is None, "task2 crashed"
    rows = read_csvs(out / "outputs" / "task2")
    assert rows, "no driver aggregates were written"
    assert set(rows[0]) == {"driver_id", "total_fare", "avg_distance"}
    assert all(float(r["total_fare"]) > 0 for r in rows)
    assert all(1 <= float(r["avg_distance"]) <= 50 for r in rows)


def test_task3_windowed_job_runs_and_checkpoints(run_stream_job):
    # Windows are only emitted after the watermark passes the 5-minute window
    # end, which takes minutes of data, so here we only check that the job
    # starts, stays up and creates its checkpoint.
    out, exit_code = run_stream_job("task3.py", seconds=40)
    assert exit_code is None, "task3 crashed"
    assert (out / "checkpoints" / "task3").exists()
