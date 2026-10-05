#!/usr/bin/env python3
"""
uk_extractor.py

Automated ingestion for the UK DfT STATS19 road casualty collision data,
scoped to the last 5 published years (2021-2025) per the project's
documented decision to avoid pre-2015-era schema drift in the full
1979-present archive.

The DfT publishes one CSV file per year. This extractor takes a directory
containing those annual files (matched by a configurable filename pattern)
and ingests each year as its own raw-layer file copy - byte-for-byte, same
principle as the Chicago extractor - under a SINGLE batch_id covering the
whole 5-year pull, so the five years are traceable as one ingestion run.

A missing or unreadable year does NOT abort the whole batch: each year is
attempted independently, failures are collected and reported together at
the end, and the extractor exits non-zero only if at least one year failed
- a partial run (e.g. 4 of 5 years) completes and is clearly flagged,
rather than either silently succeeding or discarding 4 good years because
1 was missing.

Usage:
    python uk_extractor.py --input-dir /path/to/uk_annual_files
    python uk_extractor.py --input-dir /path/to/files --years 2021,2022,2023,2024,2025
"""

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.batch import new_batch_id
from utils.config import load_adapter_config
from utils.raw_writer import write_raw_file_copy, write_manifest

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "adapters" / "uk.yaml"
DEFAULT_RAW_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw"
DEFAULT_YEARS = [2021, 2022, 2023, 2024, 2025]
# DfT's real published filename pattern, e.g.
# dft-road-casualty-statistics-collision-2023.csv
FILENAME_PATTERN = "dft-road-casualty-statistics-collision-{year}.csv"


def validate_file_is_readable_csv(path: Path) -> int:
    """Same minimal sanity check as the Chicago extractor: openable, has a
    real header, is valid UTF-8. Not data validation - just catches a
    truncated/corrupt download before it enters the raw layer."""
    try:
        with open(path, "r", encoding="utf-8", errors="strict") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header or len(header) < 2:
                raise ValueError(f"File does not look like a valid CSV (header: {header!r})")
            row_count = sum(1 for _ in reader)
        return row_count
    except UnicodeDecodeError as e:
        raise ValueError(f"File is not valid UTF-8 ({e}).") from e


def find_year_file(input_dir: Path, year: int, pattern: str) -> Path:
    candidate = input_dir / pattern.format(year=year)
    if candidate.exists():
        return candidate
    # Be slightly lenient about casing/naming drift between DfT releases
    # without silently guessing wrong: only accept an unambiguous single
    # match, never pick among several.
    matches = list(input_dir.glob(f"*{year}*.csv"))
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise FileNotFoundError(
            f"Year {year}: expected '{candidate.name}' but found {len(matches)} "
            f"ambiguous candidates instead: {[m.name for m in matches]}. "
            f"Refusing to guess - rename the file or pass an exact pattern."
        )
    raise FileNotFoundError(
        f"Year {year}: no file found matching '{candidate.name}' in {input_dir}"
    )


def run(input_dir: str, years: list[int], config_path: str, raw_root: str) -> dict:
    config = load_adapter_config(config_path)
    source_id = config["source_id"]
    input_dir_path = Path(input_dir)

    if not input_dir_path.is_dir():
        raise NotADirectoryError(f"--input-dir is not a directory: {input_dir_path}")

    batch_id = new_batch_id(source_id)
    raw_root_path = Path(raw_root)

    entries = []
    failures = []
    total_rows = 0

    for year in years:
        try:
            year_file = find_year_file(input_dir_path, year, FILENAME_PATTERN)
            row_count = validate_file_is_readable_csv(year_file)

            entry = write_raw_file_copy(
                source_filepath=year_file,
                source_id=source_id,
                batch_id=batch_id,
                raw_root=raw_root_path,
                logical_name=f"collisions_{year}.csv",
            )
            entry["record_count"] = row_count
            entry["year"] = year
            entries.append(entry)
            total_rows += row_count
            print(f"[uk_extractor] {year}: {row_count:,} rows OK")

        except (FileNotFoundError, ValueError) as e:
            # Isolate the failure to this year; keep going so a single
            # missing file doesn't throw away years that ARE available.
            failures.append({"year": year, "error": str(e)})
            print(f"[uk_extractor] {year}: FAILED - {e}", file=sys.stderr)

    manifest_path = write_manifest(
        raw_root=raw_root_path,
        source_id=source_id,
        batch_id=batch_id,
        entries=entries,
        extra_metadata={
            "provider": config["provider"],
            "dataset_url": config.get("dataset_url"),
            "years_requested": years,
            "years_succeeded": [e["year"] for e in entries],
            "years_failed": [f["year"] for f in failures],
        },
    )

    print(f"\n[uk_extractor] batch_id={batch_id}")
    print(f"[uk_extractor] {len(entries)}/{len(years)} years ingested, {total_rows:,} total rows")
    print(f"[uk_extractor] manifest: {manifest_path}")

    if failures:
        print(f"[uk_extractor] {len(failures)} year(s) FAILED: "
              f"{[f['year'] for f in failures]}", file=sys.stderr)

    return {
        "batch_id": batch_id,
        "years_succeeded": [e["year"] for e in entries],
        "years_failed": failures,
        "total_rows": total_rows,
        "manifest_path": str(manifest_path),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, help="Directory containing the annual UK STATS19 CSV files")
    parser.add_argument("--years", default=",".join(str(y) for y in DEFAULT_YEARS),
                         help="Comma-separated years to ingest (default: 2021-2025)")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--raw-root", default=str(DEFAULT_RAW_ROOT))
    args = parser.parse_args()

    years = [int(y.strip()) for y in args.years.split(",")]

    try:
        result = run(args.input_dir, years, args.config, args.raw_root)
    except NotADirectoryError as e:
        print(f"[uk_extractor] FAILED: {e}", file=sys.stderr)
        sys.exit(1)

    # Partial success (some years failed) is still a pipeline failure signal
    # for Airflow to surface, even though the good years were kept.
    if result["years_failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
