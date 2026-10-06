#!/usr/bin/env python3
"""
nyc_extractor.py

Production ingestion for the NYC Motor Vehicle Collisions - Crashes
Socrata API, scoped to the last 5 years (matching the UK and Chicago
windowing decisions). This replaces the earlier pull_nyc_sample.py, which
was a profiling tool, not a production extractor: it lacked retries,
raw-layer writing, and ingestion metadata.

Key differences from the sampling script:
  - Retries transient failures (timeouts, 5xx, connection errors) with
    exponential backoff, rather than giving up on the first error.
  - Writes each raw API page as its own JSON file via raw_writer (see
    that module's docstring for why: no parsing before the raw layer).
  - Uses the SAME compound sort key fix (crash_date DESC, collision_id
    DESC) discovered during profiling - a single sort key allowed
    duplicate rows across page boundaries on this high-volume source.
  - Treats "0 rows returned before the expected date range is covered"
    as a real failure, not silent success, since that likely means the
    API changed shape or the date filter is wrong - worth surfacing
    loudly rather than quietly ingesting less data than requested.

Usage:
    python nyc_extractor.py --since 2021-01-01 --until 2025-12-31
    python nyc_extractor.py --since 2021-01-01 --until 2025-12-31 --max-rows 500000
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.batch import new_batch_id
from utils.config import load_adapter_config
from utils.raw_writer import write_raw_json_pages, write_manifest

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "adapters" / "nyc.yaml"
DEFAULT_RAW_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw"
PAGE_SIZE = 1000
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2


class TransientAPIError(Exception):
    """Retryable: timeout, connection error, or 5xx server error."""


class PermanentAPIError(Exception):
    """Not retryable: 4xx client error (bad query, auth, etc.)."""


def fetch_page(base_url: str, limit: int, offset: int, since: str, until: str) -> list:
    params = {
        "$limit": limit,
        "$offset": offset,
        # Compound sort key: REQUIRED for stable pagination on this
        # source. Confirmed during profiling that crash_date alone lets
        # same-date rows straddle a page boundary and get returned twice.
        "$order": "crash_date DESC, collision_id DESC",
        "$where": f"crash_date >= '{since}T00:00:00.000' AND crash_date <= '{until}T23:59:59.999'",
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "data-eng-project-nyc-extractor/1.0"})

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if 500 <= e.code < 600:
            raise TransientAPIError(f"HTTP {e.code} from NYC API (server-side, retryable): {e.reason}") from e
        raise PermanentAPIError(f"HTTP {e.code} from NYC API (client-side, not retryable): {e.reason}") from e
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        raise TransientAPIError(f"Connection error reaching NYC API: {e}") from e
    except json.JSONDecodeError as e:
        raise TransientAPIError(f"NYC API returned invalid JSON (likely a transient server issue): {e}") from e


def latest_crash_date(base_url: str) -> str:
    """
    Return the newest crash_date the API currently publishes, as YYYY-MM-DD.

    NYC publishes collisions with a lag, so "today" is usually ahead of
    the newest available record. Scheduled runs anchor their trailing
    window on this date instead of today, otherwise the window can fall
    entirely in the not-yet-published period and return 0 rows.
    """
    params = {"$select": "max(crash_date) AS max_date"}
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "data-eng-project-nyc-extractor/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            rows = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise PermanentAPIError(f"HTTP {e.code} asking NYC API for its latest date: {e.reason}") from e
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        raise TransientAPIError(f"Connection error reaching NYC API: {e}") from e
    if not rows or not rows[0].get("max_date"):
        raise PermanentAPIError(f"NYC API returned no max(crash_date): {rows!r}")
    return rows[0]["max_date"][:10]


def fetch_page_with_retry(base_url: str, limit: int, offset: int, since: str, until: str) -> list:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fetch_page(base_url, limit, offset, since, until)
        except TransientAPIError as e:
            last_error = e
            if attempt < MAX_RETRIES:
                wait = BACKOFF_BASE_SECONDS ** attempt
                print(f"[nyc_extractor] attempt {attempt}/{MAX_RETRIES} failed "
                      f"({e}); retrying in {wait}s...", file=sys.stderr)
                time.sleep(wait)
            else:
                print(f"[nyc_extractor] all {MAX_RETRIES} attempts failed at offset {offset}", file=sys.stderr)
    raise last_error  # exhausted retries -> propagate, let caller decide (abort vs. keep partial data)


def run(since: str, until: str, config_path: str, raw_root: str, max_rows: int | None) -> dict:
    config = load_adapter_config(config_path)
    source_id = config["source_id"]
    base_url = config["api_endpoint"]

    batch_id = new_batch_id(source_id)
    raw_root_path = Path(raw_root)

    pages = []
    offset = 0
    total_rows = 0
    hit_permanent_error = False

    while True:
        if max_rows is not None and total_rows >= max_rows:
            print(f"[nyc_extractor] reached --max-rows cap ({max_rows}); stopping")
            break

        page_limit = PAGE_SIZE
        if max_rows is not None:
            page_limit = min(PAGE_SIZE, max_rows - total_rows)

        try:
            page = fetch_page_with_retry(base_url, page_limit, offset, since, until)
        except PermanentAPIError as e:
            # Not retryable - likely a bad query or config issue. Abort
            # rather than keep hammering an endpoint that will never
            # succeed with the same request.
            print(f"[nyc_extractor] FAILED (permanent): {e}", file=sys.stderr)
            hit_permanent_error = True
            break
        except TransientAPIError as e:
            # Retries exhausted. Keep whatever pages were already
            # successfully fetched rather than discarding a long partial
            # pull over one bad page - but this run is still a failure,
            # not a silent partial success.
            print(f"[nyc_extractor] FAILED (transient, retries exhausted): {e}", file=sys.stderr)
            hit_permanent_error = True
            break

        if not page:
            print(f"[nyc_extractor] no more rows at offset {offset}; date range exhausted")
            break

        pages.append(page)
        total_rows += len(page)
        offset += len(page)
        print(f"[nyc_extractor] fetched {total_rows:,} rows so far...")
        time.sleep(0.2)  # be polite to the public endpoint

    if total_rows == 0 and not hit_permanent_error:
        # Zero rows for a 5-year window on an active dataset is suspicious
        # enough to treat as a failure, not a quiet empty success - most
        # likely the date filter or endpoint shape changed.
        raise RuntimeError(
            f"0 rows returned for the requested window ({since} to {until}). "
            f"This almost certainly indicates a problem (wrong date format, "
            f"changed API schema, or an expired endpoint), not a real "
            f"absence of NYC crashes in that window."
        )

    entries = write_raw_json_pages(
        pages=pages,
        source_id=source_id,
        batch_id=batch_id,
        raw_root=raw_root_path,
    )

    manifest_path = write_manifest(
        raw_root=raw_root_path,
        source_id=source_id,
        batch_id=batch_id,
        entries=entries,
        extra_metadata={
            "provider": config["provider"],
            "api_endpoint": base_url,
            "requested_since": since,
            "requested_until": until,
            "max_rows_cap": max_rows,
            "completed_fully": not hit_permanent_error,
        },
    )

    print(f"\n[nyc_extractor] batch_id={batch_id}")
    print(f"[nyc_extractor] {total_rows:,} rows across {len(pages)} page(s) written to raw layer")
    print(f"[nyc_extractor] manifest: {manifest_path}")

    if hit_permanent_error:
        print("[nyc_extractor] NOTE: run did not complete fully; manifest "
              "reflects a partial pull. See completed_fully=false.", file=sys.stderr)

    return {
        "batch_id": batch_id,
        "total_rows": total_rows,
        "completed_fully": not hit_permanent_error,
        "manifest_path": str(manifest_path),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True, help="ISO date (YYYY-MM-DD), inclusive lower bound")
    parser.add_argument("--until", required=True, help="ISO date (YYYY-MM-DD), inclusive upper bound")
    parser.add_argument("--max-rows", type=int, default=None, help="Optional cap on total rows fetched")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--raw-root", default=str(DEFAULT_RAW_ROOT))
    args = parser.parse_args()

    try:
        result = run(args.since, args.until, args.config, args.raw_root, args.max_rows)
    except RuntimeError as e:
        print(f"[nyc_extractor] FAILED: {e}", file=sys.stderr)
        sys.exit(1)

    if not result["completed_fully"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
