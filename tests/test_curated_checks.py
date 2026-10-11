"""Tests for the curated validation gate (src/validation/curated_checks.py)
and the write-audit-publish flow in src/transform/curated.py.

No Docker or database: small staged batches are written to a temp folder,
harmonized into a candidate, and audited with logging switched off. Each
fault test breaks one thing the staging checks cannot see and asserts the
gate catches it.
"""

import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from transform import curated
from validation import curated_checks as cc

CANONICAL = [f["name"] for f in yaml.safe_load(open(ROOT / "config" / "canonical_schema.yaml"))["fields"]]
RUN_TIME = datetime(2026, 10, 1, tzinfo=timezone.utc)


def _staged_frame(source_id: str, n: int, timestamps: list[str], batch_id: str) -> pd.DataFrame:
    """A staged batch shaped like staging.py output: canonical columns, text values."""
    rows = []
    for i in range(n):
        rows.append({
            "crash_id": f"{source_id}:{i}", "source_id": source_id, "source_record_id": str(i),
            "crash_timestamp_utc": timestamps[i % len(timestamps)], "police_notified_timestamp_utc": None,
            "source_local_timezone": curated.LOCAL_TZ[source_id], "city": "X", "country": "US",
            "latitude": "41.9", "longitude": "-87.6", "has_valid_coordinates": "true",
            "severity": "minor", "source_severity_raw": "x", "num_injured_total": "1",
            "num_killed": "0", "num_vehicles_involved": "2", "crash_type": None, "primary_cause": None,
            "weather_condition": None, "lighting_condition": None, "posted_speed_limit_mph": "30",
            "hit_and_run": "false", "ingested_at_utc": pd.Timestamp("2026-10-01", tz="UTC"),
            "batch_id": batch_id, "source_row_raw_ref": None,
        })
    return pd.DataFrame(rows)[CANONICAL]


@pytest.fixture
def staged(tmp_path, monkeypatch):
    """Three staged batches in a temp staging root; returns the batch map."""
    staging = tmp_path / "staging"
    monkeypatch.setattr(curated, "staging_batch_dir", lambda s, b: staging / s / b)
    data = {
        # Chicago's first row is a daylight-saving-ambiguous time (2025-11-02 01:30)
        "chicago_us": (["11/02/2025 01:30:00 AM"] + ["03/05/2024 02:15:00 PM", "07/20/2025 11:05:00 AM"] * 60, 120),
        "nyc_us": (["2026-04-01T00:00:00.000 14:50", "2026-05-11T00:00:00.000 6:00"], 80),
        "uk_stats19": (["14/02/2023 08:30", "30/06/2024 17:45"], 100),
    }
    batches = {}
    for source_id, (stamps, n) in data.items():
        batch_id = f"{source_id}_20261001T000000Z_test"
        path = staging / source_id / batch_id
        path.mkdir(parents=True)
        _staged_frame(source_id, n, stamps, batch_id).to_parquet(path / "fact_crash.parquet", index=False)
        batches[source_id] = batch_id
    return batches


def _harmonize(batches, tmp_path):
    return curated.harmonize(batches=batches, target=tmp_path / "candidate" / "fact_crash")


def _audit(result):
    return cc.run_curated_checks(result, run_time=RUN_TIME, log=False)


# ---------- Happy path ----------

def test_clean_candidate_passes_all_checks(staged, tmp_path):
    result = _harmonize(staged, tmp_path)
    summary = _audit(result)
    assert summary["failed"] == 0
    assert summary["checks_run"] >= 5


def test_harmonize_reports_per_source_reconciliation(staged, tmp_path):
    result = _harmonize(staged, tmp_path)
    chi = result["sources"]["chicago_us"]
    # Exactly one row is the DST-ambiguous time.
    assert chi["staged_rows"] == 120
    assert chi["excluded_rows"] == 1
    assert chi["curated_rows"] == 119
    assert result["sources"]["uk_stats19"]["excluded_rows"] == 0


def test_harmonize_reads_exactly_the_given_batches(staged, tmp_path):
    """A newer, unvalidated batch on disk must not be picked up."""
    newer = curated.staging_batch_dir("uk_stats19", "uk_stats19_20991231T000000Z_newer")
    newer.mkdir(parents=True)
    _staged_frame("uk_stats19", 5, ["01/01/2025 10:00"], "newer").to_parquet(newer / "fact_crash.parquet")
    result = _harmonize(staged, tmp_path)
    assert result["sources"]["uk_stats19"]["batch_id"] == staged["uk_stats19"]
    assert result["sources"]["uk_stats19"]["curated_rows"] == 100


# ---------- Faults the staging checks cannot catch ----------

def test_wrong_date_format_for_one_source_fails_reconciliation(staged, tmp_path, monkeypatch):
    """A cross-source transformation bug: UK parsed with the US date order.
    Strict parsing makes the rows fail instead of silently swapping day and
    month, and the reconciliation check stops the run."""
    monkeypatch.setitem(curated.DATE_FORMATS, "uk_stats19", "%m/%d/%Y %H:%M")
    result = _harmonize(staged, tmp_path)
    with pytest.raises(RuntimeError, match="curated_reconciliation_check \\(uk_stats19\\)"):
        _audit(result)


def test_row_in_wrong_partition_fails_partition_check(staged, tmp_path):
    result = _harmonize(staged, tmp_path)
    cand = Path(result["candidate_path"])
    src = next((cand / "source_id=uk_stats19" / "year=2023").rglob("*.parquet"))
    wrong = cand / "source_id=uk_stats19" / "year=2023" / "month=9"
    wrong.mkdir(parents=True)
    shutil.move(str(src), wrong / src.name)
    with pytest.raises(RuntimeError, match="curated_partition_check \\(uk_stats19\\)"):
        _audit(result)


def test_missing_partition_file_fails_file_count(staged, tmp_path):
    result = _harmonize(staged, tmp_path)
    result["partition_files"] += 1   # harmonize "wrote" a file that is not there
    with pytest.raises(RuntimeError, match="curated_partition_check \\(all sources\\)"):
        _audit(result)


def test_batch_mismatch_fails_reconciliation(staged, tmp_path):
    result = _harmonize(staged, tmp_path)
    result["sources"]["nyc_us"]["batch_id"] = "nyc_us_some_other_batch"
    with pytest.raises(RuntimeError, match="not from validated batch"):
        _audit(result)


# ---------- The check functions on their own ----------

def _rows(n=4):
    ts = pd.Series(pd.to_datetime(["2024-03-01 10:00"] * n, utc=True))
    return pd.DataFrame({
        "source_id": ["chicago_us"] * n, "year": [2024] * n, "month": [3] * n,
        "source_record_id": [str(i) for i in range(n)],
        "crash_id": [f"chicago_us:{i}" for i in range(n)],
        "batch_id": ["b1"] * n, "crash_timestamp_utc": ts, "crash_date_key": ts.dt.date,
    })


def test_uniqueness_check_catches_duplicates_and_bad_crash_id():
    rows = _rows()
    assert cc.uniqueness_check(rows)[0][1] == "pass"
    rows.loc[1, "source_record_id"] = "0"          # duplicate natural key, crash_id now wrong too
    status, _, failed, details = cc.uniqueness_check(rows)[0][1:]
    assert status == "fail" and failed >= 2 and "duplicate (source_id, source_record_id)=1" in details


def test_timestamp_check_catches_date_key_mismatch_and_future():
    rows = _rows()
    rows.loc[0, "crash_date_key"] = pd.Timestamp("2024-03-02").date()
    rows.loc[1, "crash_timestamp_utc"] = pd.Timestamp("2030-01-01", tz="UTC")
    status, _, failed, details = cc.timestamp_check(rows, RUN_TIME)[0][1:]
    assert status == "fail" and failed >= 2 and "after run time=1" in details


def test_timestamp_check_rejects_naive_timestamps():
    rows = _rows()
    rows["crash_timestamp_utc"] = rows["crash_timestamp_utc"].dt.tz_localize(None)
    assert cc.timestamp_check(rows, RUN_TIME)[0][1] == "fail"


def test_reconciliation_tolerance_has_a_floor_for_small_batches():
    rows = _rows(4)
    expected = {"chicago_us": {"batch_id": "b1", "staged_rows": 6}}   # 2 excluded
    assert cc.reconciliation_check(rows, expected)[0][1] == "pass"
    expected = {"chicago_us": {"batch_id": "b1", "staged_rows": 40}}  # 36 excluded > 25
    assert cc.reconciliation_check(rows, expected)[0][1] == "fail"


def test_reconciliation_fails_when_a_validated_source_is_missing():
    rows = _rows()
    expected = {"chicago_us": {"batch_id": "b1", "staged_rows": 4},
                "nyc_us": {"batch_id": "b2", "staged_rows": 10}}
    statuses = {r[0]: r[1] for r in cc.reconciliation_check(rows, expected)}
    assert statuses == {"chicago_us": "pass", "nyc_us": "fail"}


def test_schema_check_catches_missing_and_mistyped_fields(staged, tmp_path):
    result = _harmonize(staged, tmp_path)
    schema, rows, _ = cc.load_candidate(Path(result["candidate_path"]))
    canonical = yaml.safe_load(open(ROOT / "config" / "canonical_schema.yaml"))
    assert cc.schema_check(schema, canonical, len(rows))[0][1] == "pass"
    broken = schema.remove(schema.get_field_index("severity"))
    assert cc.schema_check(broken, canonical, len(rows))[0][1] == "fail"


# ---------- Publish ----------

def test_publish_swaps_candidate_into_live(staged, tmp_path):
    result = _harmonize(staged, tmp_path)
    live = tmp_path / "live" / "fact_crash"
    (live / "old").mkdir(parents=True)                # previous published data
    out = curated.publish(candidate=Path(result["candidate_path"]), live=live)
    assert not Path(result["candidate_path"]).exists()
    assert not (live / "old").exists()
    assert out["partition_files"] == result["partition_files"]


def test_all_null_police_timestamps_still_typed_as_utc_timestamp(staged, tmp_path):
    """No source in the test data has police times; the column must still be a timestamp."""
    import pyarrow as pa
    result = _harmonize(staged, tmp_path)
    schema, _, _ = cc.load_candidate(Path(result["candidate_path"]))
    t = schema.field("police_notified_timestamp_utc").type
    assert pa.types.is_timestamp(t) and t.tz == "UTC"
