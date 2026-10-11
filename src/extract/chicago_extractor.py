#!/usr/bin/env python3
"""
chicago_extractor.py

Automated ingestion for the City of Chicago "Traffic Crashes - Crashes"
bulk CSV export. This is the simplest of the three extractors: one file,
no pagination, no API. It still has to handle a missing/corrupt file
without crashing the whole pipeline, since Airflow should see a clean
task failure with a useful message, not a bare traceback.

The source file is copied into the raw layer byte-for-byte (see
raw_writer.py for why), never re-parsed before landing there.

Change detection: before copying, the file's SHA-256 is compared with the
latest raw batch. If the file has not been replaced since then, no new raw
copy is written and the previous batch_id is returned with
content_changed=False (see src/utils/freshness.py). --force-new-batch
turns this off.

Usage:
    python chicago_extractor.py --input /path/to/Traffic_Crashes_-_Crashes_*.csv
    python chicago_extractor.py --input /path/to/file.csv --raw-root /data/raw
"""

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # allow `src.utils` imports when run standalone

from utils.batch import new_batch_id
from utils.config import load_adapter_config
from utils.freshness import fingerprint, previous_batch_id, unchanged_batch
from utils.raw_writer import write_raw_file_copy, write_manifest

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "adapters" / "chicago.yaml"
DEFAULT_RAW_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw"


def validate_file_is_readable_csv(path: Path) -> int:
    """
    Minimal sanity check before committing a file to the raw layer: can it
    be opened and does it have a header row with at least one column?
    This is NOT data validation (that's the staging-layer job) - it only
    catches "this isn't actually a CSV / the download was truncated"
    before it silently becomes a raw-layer artifact.
    Returns the row count (excluding header) for the manifest.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="strict") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header or len(header) < 2:
                raise ValueError(
                    f"File does not look like a valid CSV (header: {header!r})"
                )
            row_count = sum(1 for _ in reader)
        return row_count
    except UnicodeDecodeError as e:
        raise ValueError(
            f"File is not valid UTF-8 ({e}). Chicago's export should be "
            f"UTF-8; re-download the file rather than ingesting as-is."
        ) from e


RAW_FILE_NAME = "crashes.csv"


def run(input_path: str, config_path: str, raw_root: str, reuse_unchanged: bool = True) -> dict:
    config = load_adapter_config(config_path)
    source_id = config["source_id"]
    source_file = Path(input_path)

    if not source_file.exists():
        raise FileNotFoundError(
            f"Chicago source file not found: {source_file}. Check the "
            f"--input path, or that the file was fully downloaded from "
            f"{config.get('dataset_url', '(no URL in config)')}."
        )

    raw_root_path = Path(raw_root)
    hashes = fingerprint({RAW_FILE_NAME: source_file})

    # Same bytes as the latest raw batch -> nothing new to ingest.
    reused = unchanged_batch(raw_root_path, source_id, hashes) if reuse_unchanged else None
    if reused:
        print(f"[chicago_extractor] {source_file.name} is unchanged since batch "
              f"{reused['batch_id']} (sha256 {hashes[RAW_FILE_NAME][:12]}); reusing it, "
              f"no new raw copy written")
        return {
            "batch_id": reused["batch_id"],
            "row_count": reused["record_count"],
            "manifest_path": reused["manifest_path"],
            "content_changed": False,
            "content_first_ingested_utc": reused["content_first_ingested_utc"],
            "source_files": [source_file.name],
        }

    row_count = validate_file_is_readable_csv(source_file)

    prior_batch = previous_batch_id(raw_root_path, source_id)
    batch_id = new_batch_id(source_id)

    entry = write_raw_file_copy(
        source_filepath=source_file,
        source_id=source_id,
        batch_id=batch_id,
        raw_root=raw_root_path,
        logical_name=RAW_FILE_NAME,
    )
    entry["record_count"] = row_count

    manifest_path = write_manifest(
        raw_root=raw_root_path,
        source_id=source_id,
        batch_id=batch_id,
        entries=[entry],
        extra_metadata={
            "provider": config["provider"],
            "dataset_url": config.get("dataset_url"),
            "original_filename": source_file.name,
            "content_changed": True,
            "previous_batch_id": prior_batch,
        },
    )

    print(f"[chicago_extractor] batch_id={batch_id}")
    print(f"[chicago_extractor] {row_count:,} rows copied to raw layer")
    print(f"[chicago_extractor] manifest: {manifest_path}")

    return {
        "batch_id": batch_id,
        "row_count": row_count,
        "manifest_path": str(manifest_path),
        "content_changed": True,
        "previous_batch_id": prior_batch,
        "source_files": [source_file.name],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Path to the Chicago bulk CSV export")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to chicago.yaml adapter config")
    parser.add_argument("--raw-root", default=str(DEFAULT_RAW_ROOT), help="Root of the raw data layer")
    parser.add_argument("--force-new-batch", action="store_true",
                        help="Write a new raw batch even if the file is unchanged")
    args = parser.parse_args()

    try:
        run(args.input, args.config, args.raw_root, reuse_unchanged=not args.force_new_batch)
    except (FileNotFoundError, ValueError) as e:
        # Known, expected failure modes -> clean message, non-zero exit,
        # no stack trace spam. Airflow surfaces this cleanly in task logs.
        print(f"[chicago_extractor] FAILED: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
