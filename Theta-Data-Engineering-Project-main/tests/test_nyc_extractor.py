"""
test_nyc_extractor.py

Unit tests for nyc_extractor's error-handling logic, using mocked API
responses. This sandbox has no outbound network access, so these tests
substitute for a real end-to-end run against the live NYC API - they
verify the retry/backoff, permanent-vs-transient error handling, and
zero-rows detection actually behave as designed, rather than shipping
that logic unverified.

Run with: python -m pytest tests/test_nyc_extractor.py -v
       or: python tests/test_nyc_extractor.py   (falls back to plain asserts)
"""

import sys
import tempfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "extract"))

import nyc_extractor as nx

FAKE_CONFIG = {
    "source_id": "nyc_us",
    "provider": "NYC OpenData (Socrata)",
    "format": "json",
    "api_endpoint": "https://fake.test/resource/h9gi-nx95.json",
}


def _write_fake_config(tmp_path: Path) -> Path:
    import yaml
    p = tmp_path / "nyc.yaml"
    with open(p, "w") as f:
        yaml.safe_dump(FAKE_CONFIG, f)
    return p


def test_transient_error_retries_then_succeeds():
    """A 503 should be retried, not immediately fail the run."""
    call_count = {"n": 0}

    def flaky_fetch(base_url, limit, offset, since, until):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise nx.TransientAPIError("simulated 503")
        if offset == 0:
            return [{"collision_id": "1", "crash_date": "2025-01-01T00:00:00.000"}]
        return []  # second page: no more data

    with mock.patch("nyc_extractor.fetch_page", side_effect=flaky_fetch), \
         mock.patch("nyc_extractor.time.sleep", return_value=None):  # skip real backoff delay in test
        result = nx.fetch_page_with_retry("url", 1000, 0, "2025-01-01", "2025-01-02")

    assert call_count["n"] == 3, f"expected 3 attempts (2 failures + 1 success), got {call_count['n']}"
    assert result == [{"collision_id": "1", "crash_date": "2025-01-01T00:00:00.000"}]
    print("PASS: transient error retries then succeeds")


def test_permanent_error_does_not_retry():
    """A 400 (bad query) should NOT be retried - it'll never succeed."""
    call_count = {"n": 0}

    def bad_query_fetch(base_url, limit, offset, since, until):
        call_count["n"] += 1
        raise nx.PermanentAPIError("simulated 400 bad query")

    with mock.patch("nyc_extractor.fetch_page", side_effect=bad_query_fetch):
        try:
            nx.fetch_page_with_retry("url", 1000, 0, "2025-01-01", "2025-01-02")
            assert False, "expected PermanentAPIError to propagate"
        except nx.PermanentAPIError:
            pass

    assert call_count["n"] == 1, f"expected exactly 1 attempt (no retry on permanent error), got {call_count['n']}"
    print("PASS: permanent error does not retry")


def test_retries_exhausted_raises():
    """If every retry fails, the error should propagate after MAX_RETRIES attempts."""
    call_count = {"n": 0}

    def always_fails(base_url, limit, offset, since, until):
        call_count["n"] += 1
        raise nx.TransientAPIError("simulated persistent timeout")

    with mock.patch("nyc_extractor.fetch_page", side_effect=always_fails), \
         mock.patch("nyc_extractor.time.sleep", return_value=None):
        try:
            nx.fetch_page_with_retry("url", 1000, 0, "2025-01-01", "2025-01-02")
            assert False, "expected TransientAPIError after exhausting retries"
        except nx.TransientAPIError:
            pass

    assert call_count["n"] == nx.MAX_RETRIES, f"expected {nx.MAX_RETRIES} attempts, got {call_count['n']}"
    print("PASS: retries exhausted raises correctly")


def test_zero_rows_raises_runtime_error():
    """0 rows for a 5-year window should be treated as a failure, not a quiet success."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        config_path = _write_fake_config(tmp_path)
        raw_root = tmp_path / "raw"

        with mock.patch("nyc_extractor.fetch_page_with_retry", return_value=[]):
            try:
                nx.run("2021-01-01", "2025-12-31", str(config_path), str(raw_root), None)
                assert False, "expected RuntimeError for 0 total rows"
            except RuntimeError as e:
                assert "0 rows returned" in str(e)
    print("PASS: zero rows raises RuntimeError instead of silent success")


def test_successful_run_writes_raw_layer_and_manifest():
    """End-to-end happy path with mocked pages: raw files + manifest should land correctly."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        config_path = _write_fake_config(tmp_path)
        raw_root = tmp_path / "raw"

        call_count = {"n": 0}

        def two_pages_then_done(base_url, limit, offset, since, until):
            call_count["n"] += 1
            if offset == 0:
                return [{"collision_id": str(i), "crash_date": "2025-01-01T00:00:00.000"} for i in range(1000)]
            elif offset == 1000:
                return [{"collision_id": "1000", "crash_date": "2025-01-01T00:00:00.000"}]
            return []

        with mock.patch("nyc_extractor.fetch_page", side_effect=two_pages_then_done), \
             mock.patch("nyc_extractor.time.sleep", return_value=None):
            result = nx.run("2021-01-01", "2025-12-31", str(config_path), str(raw_root), None)

        assert result["total_rows"] == 1001, f"expected 1001 rows, got {result['total_rows']}"
        assert result["completed_fully"] is True
        manifest_path = Path(result["manifest_path"])
        assert manifest_path.exists(), "manifest.json was not written"

        raw_files = list((raw_root / "nyc_us" / result["batch_id"]).glob("*.json"))
        # 2 page files + 1 manifest.json
        assert len(raw_files) == 3, f"expected 3 JSON files (2 pages + manifest), found {len(raw_files)}"
    print("PASS: successful run writes raw layer and manifest correctly")


def test_max_rows_cap_respected():
    """--max-rows should stop the pull early rather than fetching everything."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        config_path = _write_fake_config(tmp_path)
        raw_root = tmp_path / "raw"

        def unlimited_fetch(base_url, limit, offset, since, until):
            return [{"collision_id": str(offset + i), "crash_date": "2025-01-01T00:00:00.000"} for i in range(limit)]

        with mock.patch("nyc_extractor.fetch_page", side_effect=unlimited_fetch), \
             mock.patch("nyc_extractor.time.sleep", return_value=None):
            result = nx.run("2021-01-01", "2025-12-31", str(config_path), str(raw_root), max_rows=1500)

        assert result["total_rows"] == 1500, f"expected exactly 1500 rows (cap), got {result['total_rows']}"
    print("PASS: --max-rows cap respected")


if __name__ == "__main__":
    test_transient_error_retries_then_succeeds()
    test_permanent_error_does_not_retry()
    test_retries_exhausted_raises()
    test_zero_rows_raises_runtime_error()
    test_successful_run_writes_raw_layer_and_manifest()
    test_max_rows_cap_respected()
    print("\nAll tests passed.")
