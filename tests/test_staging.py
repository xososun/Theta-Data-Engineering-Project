"""Unit tests for src/transform/staging.py primitives.

These tests do NOT need Docker or a real raw batch. They exercise the
mapping logic in isolation using small in-memory DataFrames.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from transform.staging import _apply_field, _null_sentinel_coordinates
from transform import derivations


ADAPTER_STUB = {
    "bounding_box": {"latitude": [41.60, 42.05], "longitude": [-87.95, -87.50]},
}


def test_literal_field():
    df = pd.DataFrame({"x": ["a", "b", "c"]})
    spec = {"literal": "chicago_us"}
    result = _apply_field("source_id", spec, df, ADAPTER_STUB)
    assert list(result) == ["chicago_us", "chicago_us", "chicago_us"]


def test_from_field():
    df = pd.DataFrame({"CRASH_RECORD_ID": ["id1", "id2", "id3"]})
    spec = {"from": "CRASH_RECORD_ID"}
    result = _apply_field("source_record_id", spec, df, ADAPTER_STUB)
    assert list(result) == ["id1", "id2", "id3"]


def test_map_field_with_default():
    df = pd.DataFrame({"SEV": ["FATAL", "OTHER", "INCAPACITATING INJURY"]})
    spec = {
        "from": "SEV",
        "map": {"FATAL": "fatal", "INCAPACITATING INJURY": "serious"},
        "default": "unknown",
    }
    result = _apply_field("severity", spec, df, ADAPTER_STUB)
    assert list(result) == ["fatal", "unknown", "serious"]


def test_derive_fn_chicago_valid_coords():
    df = pd.DataFrame({
        "LATITUDE": ["41.80", "0.0", "43.0", None],
        "LONGITUDE": ["-87.60", "0.0", "-87.60", None],
    })
    spec = {"derive_fn": "chicago_has_valid_coordinates"}
    result = _apply_field("has_valid_coordinates", spec, df, ADAPTER_STUB)
    # inside bbox, sentinel (0,0), outside bbox (lat 43 > 42.05), null
    assert list(result) == [True, False, False, False]


def test_nyc_severity_from_raw_components():
    df = pd.DataFrame({
        "number_of_persons_injured": ["0", "2", "0", "1"],
        "number_of_pedestrians_killed": ["0", "0", "1", "0"],
        "number_of_cyclist_killed": ["0", "0", "0", "0"],
        "number_of_motorist_killed": ["0", "0", "0", "0"],
    })
    spec = {"derive_fn": "nyc_severity"}
    result = _apply_field("severity", spec, df, ADAPTER_STUB)
    assert list(result) == ["none", "minor", "fatal", "minor"]


def test_null_sentinel_coordinates_nulls_pairs():
    df = pd.DataFrame({
        "latitude": [41.8, 0.0, 42.0],
        "longitude": [-87.6, 0.0, -87.5],
    })
    out = _null_sentinel_coordinates(df)
    assert pd.isna(out.iloc[1]["latitude"])
    assert pd.isna(out.iloc[1]["longitude"])
    assert out.iloc[0]["latitude"] == 41.8
    assert out.iloc[2]["longitude"] == -87.5


def test_uk_null_sentinel_to_null():
    df = pd.DataFrame({"speed_limit": ["30", "-1", "40"]})
    adapter = {"null_sentinel_value": -1}
    spec = {"from": "speed_limit", "apply_null_sentinel": True}
    result = _apply_field("posted_speed_limit_mph", spec, df, adapter)
    assert result.iloc[0] == "30"
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == "40"


def test_uk_crash_id_format():
    df = pd.DataFrame({"collision_index": ["2024A1", "2024A2"]})
    spec = {"derive_fn": "uk_crash_id"}
    result = _apply_field("crash_id", spec, df, ADAPTER_STUB)
    assert list(result) == ["uk_stats19:2024A1", "uk_stats19:2024A2"]

def test_map_matches_int_keys_against_text_values():
    """UK severity codes arrive as text ("1") but the YAML keys are ints (1)."""
    df = pd.DataFrame({"collision_severity": ["1", "2", "3", None]})
    spec = {
        "from": "collision_severity",
        "map": {1: "fatal", 2: "serious", 3: "minor", None: "unknown"},
        "default": "unknown",
    }
    result = _apply_field("severity", spec, df, ADAPTER_STUB)
    assert list(result) == ["fatal", "serious", "minor", "unknown"]
