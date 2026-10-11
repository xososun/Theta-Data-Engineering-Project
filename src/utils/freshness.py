"""
freshness.py

Detects whether a file-based source (Chicago, UK) actually has new data,
and describes how fresh each source is, so a scheduled run can say
precisely what it refreshed.

Why this exists
---------------
Chicago and UK files are supplied by a person: someone downloads the
export and puts it in data/incoming/. NYC is pulled from its API on every
run. So a weekly run can only bring in new Chicago/UK data after someone
replaces those files. Without detection, every run would copy the same
400 MB Chicago file into a new raw batch and nothing would say that the
data had not changed.

How detection works
-------------------
1. Before copying, the extractor computes the SHA-256 of each incoming
   file (the same hash the raw layer already records in manifest.json).
2. It compares those hashes with the files in the source's most recent
   raw batch.
3. Identical content -> no new raw batch is written; the run reuses the
   previous batch_id. Different content -> a new batch is written, and its
   manifest records `content_changed: true` and the previous batch_id.
4. The DAG writes one `source_freshness` row per source per run to
   dq_run_log (pass / warn), so every run's quality report states which
   sources were refreshed and which were not.

Everything here is file and manifest logic only (no database), so it can
be unit-tested without Docker.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------- Choosing the incoming file ----------

def select_incoming_file(incoming_dir: str | Path, pattern: str) -> Path:
    """
    Return the newest file in incoming_dir matching a glob pattern.

    "Newest" is by file name first, because the Chicago portal ends its
    export name with the export date (..._Crashes_20260923.csv), so a later
    export sorts last. Modification time breaks ties. This means replacing
    the export is enough: nobody has to edit the DAG or set CHICAGO_CSV.
    """
    incoming_dir = Path(incoming_dir)
    matches = [p for p in incoming_dir.iterdir()
               if p.is_file() and fnmatch.fnmatch(p.name, pattern)] if incoming_dir.is_dir() else []
    if not matches:
        raise FileNotFoundError(
            f"No file matching {pattern!r} in {incoming_dir}. Download the "
            f"source export into this folder (see README, 'What a scheduled "
            f"run refreshes')."
        )
    return max(matches, key=lambda p: (p.name, p.stat().st_mtime))


# ---------- Comparing with the previous raw batch ----------

def latest_batch_manifest(raw_root: str | Path, source_id: str) -> dict | None:
    """
    The manifest of the source's most recent raw batch, or None.

    Batch folders are named <source_id>_<UTC timestamp>_<id>, so sorting by
    name sorts by creation time.
    """
    source_dir = Path(raw_root) / source_id
    if not source_dir.is_dir():
        return None
    for batch_dir in sorted((d for d in source_dir.iterdir() if d.is_dir()), reverse=True):
        manifest_path = batch_dir / "manifest.json"
        if manifest_path.exists():
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            manifest["_manifest_path"] = str(manifest_path)
            return manifest
    return None


def fingerprint(files: dict[str, Path]) -> dict[str, str]:
    """{raw file name: sha256} for the files about to be ingested."""
    return {name: sha256_of_file(Path(path)) for name, path in files.items()}


def unchanged_batch(raw_root: str | Path, source_id: str,
                    incoming_hashes: dict[str, str]) -> dict | None:
    """
    If the incoming files are byte-identical to the latest raw batch,
    return that batch's details; otherwise None.

    "Identical" means the same set of raw file names with the same SHA-256
    each. A batch whose raw files have since gone missing is not reused.
    """
    manifest = latest_batch_manifest(raw_root, source_id)
    if not manifest:
        return None
    previous = {}
    for entry in manifest.get("files", []):
        raw_path = Path(entry.get("raw_path", ""))
        if entry.get("type") != "file_copy" or not raw_path.exists():
            return None
        previous[raw_path.name] = entry.get("sha256")
    if previous != incoming_hashes:
        return None
    return {
        "batch_id": manifest["batch_id"],
        "record_count": manifest.get("total_records_or_files"),
        "content_first_ingested_utc": manifest.get("retrieved_at_utc"),
        "manifest_path": manifest["_manifest_path"],
    }


def previous_batch_id(raw_root: str | Path, source_id: str) -> str | None:
    manifest = latest_batch_manifest(raw_root, source_id)
    return manifest["batch_id"] if manifest else None


# ---------- Describing freshness for dq_run_log ----------

def _days_between(earlier_iso: str | None, now: datetime) -> int | None:
    if not earlier_iso:
        return None
    earlier = datetime.fromisoformat(earlier_iso)
    if earlier.tzinfo is None:
        earlier = earlier.replace(tzinfo=timezone.utc)
    return (now - earlier).days


def freshness_status(extract_result: dict, refresh_cfg: dict,
                     now: datetime | None = None) -> tuple[str, str]:
    """
    Turn an extractor result into a (status, details) pair for the
    `source_freshness` row in dq_run_log.

    local_file sources:
      new content                      -> pass
      unchanged, within max age        -> pass  (says so explicitly)
      unchanged, older than max age    -> warn  (input file needs replacing)
    api sources:
      newest published date within lag -> pass
      older than max_publication_lag   -> warn
    """
    now = now or datetime.now(timezone.utc)
    method = refresh_cfg.get("method", "local_file")

    if method == "api":
        latest = extract_result.get("latest_published_date")
        lag = (now.date() - date.fromisoformat(latest)).days if latest else None
        max_lag = refresh_cfg.get("max_publication_lag_days")
        details = (
            f"API pull: window {extract_result.get('window_since')} to "
            f"{extract_result.get('window_until')}; newest published crash date "
            f"{latest} ({lag} days before this run)."
        )
        if lag is not None and max_lag is not None and lag > max_lag:
            return "warn", details + f" Publication lag exceeds {max_lag} days."
        return "pass", details

    files = ", ".join(extract_result.get("source_files", [])) or "(unknown file)"
    if extract_result.get("content_changed", True):
        prev = extract_result.get("previous_batch_id") or "none"
        return "pass", (f"NEW content ingested from {files} as a new raw batch "
                        f"(previous batch: {prev}).")

    age = _days_between(extract_result.get("content_first_ingested_utc"), now)
    max_age = refresh_cfg.get("max_unchanged_days")
    details = (
        f"UNCHANGED: {files} is byte-identical to batch {extract_result.get('batch_id')} "
        f"(first ingested {age} days ago). No new raw copy written; the batch is "
        f"re-staged and re-validated. New data needs a replaced file in data/incoming/."
    )
    if age is not None and max_age is not None and age > max_age:
        return "warn", details + f" Older than the {max_age}-day refresh expectation."
    return "pass", details
