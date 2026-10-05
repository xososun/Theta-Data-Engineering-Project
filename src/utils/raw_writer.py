"""
raw_writer.py

Shared raw-layer writer used by every extractor (Chicago, NYC, UK).

Design principle (from the project guidelines): "Raw Layer: retain
source-faithful data and preserve traceability." That means:

  - File-based sources (Chicago bulk CSV, UK annual CSVs) are copied into
    the raw layer BYTE-FOR-BYTE. They are never round-tripped through
    pandas or any parser before landing in raw/, because re-serializing
    can silently change formatting, numeric precision, or encoding -
    exactly the kind of "destructive manual preprocessing before
    ingestion" the guidelines warn against.

  - API-based sources (NYC) have no original file to copy, so the raw
    layer stores the exact JSON payload received from each page request,
    before any parsing into rows.

Every write also produces a manifest.json recording ingestion metadata:
source, batch_id, retrieval timestamp, and enough detail to trace each
raw file back to exactly how and when it was pulled.
"""

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _batch_dir(raw_root: Path, source_id: str, batch_id: str) -> Path:
    d = raw_root / source_id / batch_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_raw_file_copy(
    source_filepath: Path,
    source_id: str,
    batch_id: str,
    raw_root: Path,
    logical_name: str | None = None,
) -> dict:
    """
    Copy a source file into the raw layer byte-for-byte. Returns a
    manifest entry dict (not yet written to disk - caller batches these
    via write_manifest).
    """
    source_filepath = Path(source_filepath)
    if not source_filepath.exists():
        raise FileNotFoundError(f"Source file not found: {source_filepath}")

    dest_dir = _batch_dir(raw_root, source_id, batch_id)
    dest_name = logical_name or source_filepath.name
    dest_path = dest_dir / dest_name

    shutil.copy2(source_filepath, dest_path)

    return {
        "type": "file_copy",
        "original_path": str(source_filepath),
        "raw_path": str(dest_path),
        "size_bytes": dest_path.stat().st_size,
        "sha256": _sha256_of_file(dest_path),
    }


def write_raw_json_pages(
    pages: list,
    source_id: str,
    batch_id: str,
    raw_root: Path,
    page_prefix: str = "page",
) -> list[dict]:
    """
    Write each raw API response page as its own JSON file, exactly as
    received (no parsing/flattening). Returns a list of manifest entries.
    """
    dest_dir = _batch_dir(raw_root, source_id, batch_id)
    entries = []
    for i, page in enumerate(pages):
        dest_path = dest_dir / f"{page_prefix}_{i:05d}.json"
        with open(dest_path, "w", encoding="utf-8") as f:
            json.dump(page, f)
        entries.append({
            "type": "api_page",
            "raw_path": str(dest_path),
            "record_count": len(page) if isinstance(page, list) else 1,
            "size_bytes": dest_path.stat().st_size,
            "sha256": _sha256_of_file(dest_path),
        })
    return entries


def write_manifest(
    raw_root: Path,
    source_id: str,
    batch_id: str,
    entries: list[dict],
    extra_metadata: dict | None = None,
) -> Path:
    """
    Write the batch's manifest.json: what was ingested, when, and how.
    This is the ingestion-metadata record required by the guidelines
    ("record ingestion metadata such as source, retrieval timestamp, or
    batch identifier"), separate from the raw data files themselves so
    the raw files stay byte-identical to source.
    """
    dest_dir = _batch_dir(raw_root, source_id, batch_id)
    manifest = {
        "source_id": source_id,
        "batch_id": batch_id,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "file_count": len(entries),
        "total_records_or_files": sum(e.get("record_count", 1) for e in entries),
        "files": entries,
        "extra_metadata": extra_metadata or {},
    }
    manifest_path = dest_dir / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    return manifest_path
