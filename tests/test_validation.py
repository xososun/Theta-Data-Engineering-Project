"""Unit tests for src/validation/checks.py.

These test the check logic in isolation (no Postgres, no staging Parquet).
The DB-writing wrapper is not exercised here - that path is proven by the
end-to-end run against real staging batches.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from validation import checks


CANONICAL = {
    "fields": [
        {"name": "crash_id", "nullable": False},
        {"name": "source_id", "nullable": False},
        {"name": "severity", "nullable": False},
        {"name": "country", "nullable": False},
    ]
}


def test_schema_check_pass():
    df = pd.DataFrame(columns=["crash_id", "source_id", "severity", "country"])
    status, _, failed, _ = checks.schema_check(df, CANONICAL)
    assert status == "pass"
    assert failed == 0


def test_schema_check_fails_on_missing_field():
    df = pd.DataFrame(columns=["crash_id", "source_id"])
    status, _, failed, details = checks.schema_check(df, CANONICAL)
    assert status == "fail"
    assert "severity" in details


def test_nullability_pass():
    df = pd.DataFrame({
        "crash_id": ["a", "b"], "source_id": ["x", "x"],
        "severity": ["fatal", "none"], "country": ["US", "US"],
    })
    status, _, failed, _ = checks.nullability_check(df, CANONICAL, "chicago_us")
    assert status == "pass"


def test_nullability_fails_on_null():
    df = pd.DataFrame({
        "crash_id": ["a", "b"], "source_id": ["x", "x"],
        "severity": ["fatal", None], "country": ["US", "US"],
    })
    status, _, failed, _ = checks.nullability_check(df, CANONICAL, "chicago_us")
    assert status == "fail"
    assert failed == 1


def test_nullability_skips_structural_gaps():
    # For uk_stats19, num_killed is null by design - should not count.
    canonical = {"fields": [{"name": "num_killed", "nullable": False}]}
    df = pd.DataFrame({"num_killed": [None, None]})
    status, _, _, _ = checks.nullability_check(df, canonical, "uk_stats19")
    assert status == "pass"


def test_uniqueness_pass():
    df = pd.DataFrame({
        "crash_id": ["a", "b", "c"], "source_record_id": ["1", "2", "3"],
    })
    status, _, failed, _ = checks.uniqueness_check(df, "chicago_us")
    assert status == "pass"


def test_uniqueness_fails_on_duplicate():
    df = pd.DataFrame({
        "crash_id": ["a", "a", "c"], "source_record_id": ["1", "1", "3"],
    })
    status, _, failed, _ = checks.uniqueness_check(df, "chicago_us")
    assert status == "fail"
    assert failed >= 1


def test_accepted_values_pass():
    df = pd.DataFrame({
        "severity": ["fatal", "minor", "none"], "country": ["US", "US", "GB"],
    })
    status, _, failed, _ = checks.accepted_values_check(df)
    assert status == "pass"


def test_accepted_values_fails_on_unknown_severity():
    df = pd.DataFrame({
        "severity": ["fatal", "bogus"], "country": ["US", "US"],
    })
    status, _, failed, _ = checks.accepted_values_check(df)
    assert status == "fail"
    assert failed == 1


def test_range_check_flags_speed_outside_band():
    df = pd.DataFrame({"posted_speed_limit_mph": ["30", "5", "1", "100"]})
    status, _, failed, details = checks.range_check(df, "chicago_us")
    # 1 is below 5; 100 is above 70 - both should warn
    assert status == "warn"
    assert failed >= 2


def test_date_logic_skips_structural_gap():
    df = pd.DataFrame({"police_notified_timestamp_utc": [None, None]})
    status, _, _, details = checks.date_logic_check(df, "uk_stats19")
    assert status == "pass"
    assert "Skipped" in details or "structural gap" in details


def test_date_logic_detects_inversion():
    df = pd.DataFrame({
        "crash_timestamp_utc": ["2024-03-01 10:00:00+00:00", "2024-03-02 10:00:00+00:00"],
        "police_notified_timestamp_utc": ["2024-03-01 11:00:00+00:00", "2024-03-02 09:00:00+00:00"],
    })
    status, _, failed, _ = checks.date_logic_check(df, "chicago_us")
    assert failed == 1
    # 1 inversion < tolerance(5) for chicago, so status is warn
    assert status == "warn"