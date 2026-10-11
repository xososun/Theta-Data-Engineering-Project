"""Tests for source refresh detection (src/utils/freshness.py) and its use
in the Chicago and UK extractors. No Docker or database needed."""

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils import freshness
from extract import chicago_extractor, uk_extractor

CHICAGO_YAML = ROOT / "config" / "adapters" / "chicago.yaml"
UK_YAML = ROOT / "config" / "adapters" / "uk.yaml"
NOW = datetime(2026, 10, 12, tzinfo=timezone.utc)


def _csv(path: Path, rows: int = 3, tag: str = "a") -> Path:
    path.write_text("col1,col2\n" + "".join(f"{tag}{i},{i}\n" for i in range(rows)), encoding="utf-8")
    return path


# ---------- Picking the incoming file ----------

def test_select_incoming_file_picks_newest_export_by_name(tmp_path):
    _csv(tmp_path / "Traffic_Crashes_-_Crashes_20260923.csv")
    time.sleep(0.01)
    _csv(tmp_path / "Traffic_Crashes_-_Crashes_20261010.csv")
    _csv(tmp_path / "unrelated.csv")
    chosen = freshness.select_incoming_file(tmp_path, "Traffic_Crashes_-_Crashes_*.csv")
    assert chosen.name == "Traffic_Crashes_-_Crashes_20261010.csv"


def test_select_incoming_file_fails_clearly_when_nothing_matches(tmp_path):
    with pytest.raises(FileNotFoundError, match="No file matching"):
        freshness.select_incoming_file(tmp_path, "Traffic_Crashes_-_Crashes_*.csv")


# ---------- Chicago: unchanged file is detected and reused ----------

def test_chicago_unchanged_file_reuses_previous_batch(tmp_path):
    incoming, raw = tmp_path / "incoming", tmp_path / "raw"
    incoming.mkdir()
    export = _csv(incoming / "Traffic_Crashes_-_Crashes_20261010.csv")

    first = chicago_extractor.run(str(export), str(CHICAGO_YAML), str(raw))
    assert first["content_changed"] is True and first["previous_batch_id"] is None

    second = chicago_extractor.run(str(export), str(CHICAGO_YAML), str(raw))
    assert second["content_changed"] is False
    assert second["batch_id"] == first["batch_id"]
    assert len(list((raw / "chicago_us").iterdir())) == 1        # no second raw copy


def test_chicago_replaced_file_creates_new_batch(tmp_path):
    incoming, raw = tmp_path / "incoming", tmp_path / "raw"
    incoming.mkdir()
    export = _csv(incoming / "Traffic_Crashes_-_Crashes_20261010.csv")
    first = chicago_extractor.run(str(export), str(CHICAGO_YAML), str(raw))

    _csv(export, rows=4, tag="b")                                 # someone replaced the export
    second = chicago_extractor.run(str(export), str(CHICAGO_YAML), str(raw))
    assert second["content_changed"] is True
    assert second["batch_id"] != first["batch_id"]
    assert second["previous_batch_id"] == first["batch_id"]
    manifest = json.loads(Path(second["manifest_path"]).read_text())
    assert manifest["extra_metadata"]["content_changed"] is True
    assert manifest["extra_metadata"]["previous_batch_id"] == first["batch_id"]


def test_force_new_batch_skips_reuse(tmp_path):
    incoming, raw = tmp_path / "incoming", tmp_path / "raw"
    incoming.mkdir()
    export = _csv(incoming / "Traffic_Crashes_-_Crashes_20261010.csv")
    chicago_extractor.run(str(export), str(CHICAGO_YAML), str(raw))
    again = chicago_extractor.run(str(export), str(CHICAGO_YAML), str(raw), reuse_unchanged=False)
    assert again["content_changed"] is True


# ---------- UK: all five files must match ----------

def test_uk_unchanged_files_reuse_batch_and_one_changed_year_does_not(tmp_path):
    incoming, raw = tmp_path / "incoming", tmp_path / "raw"
    incoming.mkdir()
    years = [2024, 2025]
    for y in years:
        _csv(incoming / f"dft-road-casualty-statistics-collision-{y}.csv", tag=str(y))

    first = uk_extractor.run(str(incoming), years, str(UK_YAML), str(raw))
    second = uk_extractor.run(str(incoming), years, str(UK_YAML), str(raw))
    assert second["content_changed"] is False and second["batch_id"] == first["batch_id"]

    _csv(incoming / "dft-road-casualty-statistics-collision-2025.csv", rows=5, tag="rev")
    third = uk_extractor.run(str(incoming), years, str(UK_YAML), str(raw))
    assert third["content_changed"] is True and third["batch_id"] != first["batch_id"]


def test_uk_missing_year_is_reported_not_reused(tmp_path):
    incoming, raw = tmp_path / "incoming", tmp_path / "raw"
    incoming.mkdir()
    _csv(incoming / "dft-road-casualty-statistics-collision-2024.csv")
    result = uk_extractor.run(str(incoming), [2024, 2025], str(UK_YAML), str(raw))
    assert [f["year"] for f in result["years_failed"]] == [2025]


# ---------- What gets recorded in dq_run_log ----------

LOCAL = {"method": "local_file", "max_unchanged_days": 14}


def test_freshness_new_content_passes():
    status, details = freshness.freshness_status(
        {"content_changed": True, "previous_batch_id": "b0", "source_files": ["x.csv"]}, LOCAL, NOW)
    assert status == "pass" and details.startswith("NEW content")


def test_freshness_unchanged_within_limit_passes_but_says_so():
    first = (NOW - timedelta(days=7)).isoformat()
    status, details = freshness.freshness_status(
        {"content_changed": False, "batch_id": "b0", "content_first_ingested_utc": first,
         "source_files": ["x.csv"]}, LOCAL, NOW)
    assert status == "pass" and details.startswith("UNCHANGED") and "7 days" in details


def test_freshness_unchanged_too_long_warns():
    first = (NOW - timedelta(days=30)).isoformat()
    status, details = freshness.freshness_status(
        {"content_changed": False, "batch_id": "b0", "content_first_ingested_utc": first,
         "source_files": ["x.csv"]}, LOCAL, NOW)
    assert status == "warn" and "14-day" in details


def test_freshness_api_reports_publication_lag():
    api = {"method": "api", "max_publication_lag_days": 180}
    ok = freshness.freshness_status({"latest_published_date": "2026-06-08", "window_since": "2026-03-10",
                                     "window_until": "2026-06-08"}, api, NOW)
    late = freshness.freshness_status({"latest_published_date": "2026-01-01"}, api, NOW)
    assert ok[0] == "pass" and "126 days" in ok[1]
    assert late[0] == "warn"
