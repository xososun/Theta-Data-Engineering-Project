#!/usr/bin/env python3
"""
curated_checks.py

Validation gate on the CURATED candidate: runs after harmonization and
before anything is published or loaded.

Why a second gate
-----------------
checks.py validates each source's staging batch on its own. Harmonization
then combines all sources and transforms them again: it parses each
source's timestamp format, converts local time to UTC, derives
crash_date_key, casts types and splits the data into partitions. A mistake
in that cross-source step (a wrong date format for one source, a type that
casts to null, a source silently missing, a partition written to the wrong
month) would pass every staging check and still corrupt the curated table.
These checks look at the harmonized result itself.

The five checks
---------------
1. curated_schema_check          every canonical field present with the
                                 expected Parquet type; partition keys present
2. curated_uniqueness_check      (source_id, source_record_id) and crash_id
                                 unique across ALL sources; crash_id built
                                 from them
3. curated_reconciliation_check  per source: every validated batch present,
                                 curated rows come from that exact batch,
                                 staged = curated + excluded, and exclusions
                                 within tolerance
4. curated_timestamp_check       crash timestamps present, in UTC, within a
                                 plausible range; crash_date_key equals the
                                 UTC date of the timestamp
5. curated_partition_check       each row sits in the year/month partition of
                                 its timestamp; one partition folder per
                                 source-year-month present in the data; no
                                 empty files; file count matches what
                                 harmonize reported

Each result is written to dq_run_log (check names start with "curated_").
Per-source results carry that source's validated batch_id; cross-source
results carry the harmonize run id and no source_id. Any `fail` raises,
which fails the Airflow task, so publish and load never run and the
previous live curated data stays untouched.

Usage:
    python src/validation/curated_checks.py     # audits data/curated/_candidate/
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

REPO_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_SCHEMA = REPO_ROOT / "config" / "canonical_schema.yaml"

# Rows harmonize may exclude per source (timestamps with no valid UTC
# value, e.g. daylight-saving transition hours). The real run excludes
# 0.017% of Chicago and 0.013% of UK rows; 0.1% leaves headroom while a
# real parsing regression (a wrong date format drops most rows) fails.
# The floor keeps a very small batch from failing on one DST row.
MAX_EXCLUDED_FRACTION = 0.001
MIN_EXCLUDED_ALLOWANCE = 25

# Earliest plausible crash time (canonical_schema.yaml: min_plausible).
MIN_PLAUSIBLE_UTC = pd.Timestamp("2000-01-01", tz="UTC")
# Slack for clock differences when checking "not in the future".
FUTURE_SLACK = timedelta(days=1)

PARTITION_KEYS = ("source_id", "year", "month")

# Columns the checks read (not the whole table, to keep memory down).
CHECK_COLUMNS = [
    "source_id", "year", "month", "source_record_id", "crash_id", "batch_id",
    "crash_timestamp_utc", "crash_date_key",
]


# ---------- Expected Parquet types ----------

def _type_ok(canonical_type: str, arrow_type: pa.DataType) -> bool:
    if canonical_type in ("string", "enum"):
        return pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type) \
            or pa.types.is_dictionary(arrow_type)
    if canonical_type == "timestamp":
        return pa.types.is_timestamp(arrow_type) and arrow_type.tz in ("UTC", "+00:00", "Etc/UTC")
    if canonical_type == "integer":
        return pa.types.is_integer(arrow_type)
    if canonical_type == "float":
        return pa.types.is_floating(arrow_type)
    if canonical_type == "boolean":
        return pa.types.is_boolean(arrow_type)
    return True  # unknown canonical type: not enforced


def _load_canonical() -> dict:
    with open(CANONICAL_SCHEMA, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------- Reading the candidate ----------

def load_candidate(candidate_dir: Path) -> tuple[pa.Schema, pd.DataFrame, pd.DataFrame]:
    """
    Returns (schema, rows, files):
      schema  the unified Arrow schema, including partition keys
      rows    the CHECK_COLUMNS for every row
      files   one row per Parquet file: source_id, year, month, num_rows
    """
    candidate_dir = Path(candidate_dir)
    if not candidate_dir.exists():
        raise FileNotFoundError(f"No curated candidate at {candidate_dir}. Run harmonize first.")
    dataset = ds.dataset(candidate_dir, format="parquet", partitioning="hive")
    columns = [c for c in CHECK_COLUMNS if c in dataset.schema.names]
    rows = dataset.to_table(columns=columns).to_pandas()

    file_rows = []
    for path in dataset.files:
        parts = dict(p.split("=", 1) for p in Path(path).relative_to(candidate_dir).parts[:-1] if "=" in p)
        file_rows.append({
            "path": path,
            "source_id": parts.get("source_id"),
            "year": int(parts["year"]) if parts.get("year", "").lstrip("-").isdigit() else None,
            "month": int(parts["month"]) if parts.get("month", "").lstrip("-").isdigit() else None,
            "num_rows": pq.ParquetFile(path).metadata.num_rows,
        })
    files = pd.DataFrame(file_rows, columns=["path", "source_id", "year", "month", "num_rows"])
    return dataset.schema, rows, files


# ---------- The checks ----------
# Each returns a list of (source_id or None, status, rows_checked, rows_failed, details).

def schema_check(schema: pa.Schema, canonical: dict, n_rows: int) -> list[tuple]:
    problems = []
    for field in canonical["fields"]:
        name = field["name"]
        if name not in schema.names:
            problems.append(f"{name}: missing")
        elif not _type_ok(field.get("type", ""), schema.field(name).type):
            problems.append(f"{name}: {schema.field(name).type} is not {field.get('type')}")
    for name in ("crash_date_key",):
        if name not in schema.names:
            problems.append(f"{name}: missing")
        elif not pa.types.is_date(schema.field(name).type):
            problems.append(f"{name}: {schema.field(name).type} is not a date")
    for key in PARTITION_KEYS:
        if key not in schema.names:
            problems.append(f"partition key {key}: missing")
    if problems:
        return [(None, "fail", n_rows, n_rows, "Schema problems: " + "; ".join(problems))]
    return [(None, "pass", n_rows, 0,
             f"All {len(canonical['fields'])} canonical fields + crash_date_key present "
             f"with expected types; partitioned by {'/'.join(PARTITION_KEYS)}")]


def uniqueness_check(rows: pd.DataFrame) -> list[tuple]:
    dup_key = int(rows.duplicated(subset=["source_id", "source_record_id"]).sum())
    dup_id = int(rows["crash_id"].duplicated().sum())
    expected_id = rows["source_id"].astype(str) + ":" + rows["source_record_id"].astype(str)
    bad_id = int((rows["crash_id"].astype(str) != expected_id).sum())
    total = dup_key + dup_id + bad_id
    details = (f"Across all sources: duplicate (source_id, source_record_id)={dup_key:,}, "
               f"duplicate crash_id={dup_id:,}, crash_id not '<source_id>:<source_record_id>'={bad_id:,}")
    return [(None, "fail" if total else "pass", len(rows), total, details)]


def reconciliation_check(rows: pd.DataFrame, expected: dict,
                         max_excluded_fraction: float = MAX_EXCLUDED_FRACTION,
                         min_allowance: int = MIN_EXCLUDED_ALLOWANCE) -> list[tuple]:
    """
    expected: {source_id: {"batch_id": ..., "staged_rows": int}} for the
    batches validated in this run. Staged row counts are re-read from the
    staged Parquet metadata by run_curated_checks, not trusted from harmonize.
    """
    results = []
    curated_counts = rows["source_id"].value_counts()

    unexpected = sorted(set(curated_counts.index) - set(expected))
    if unexpected:
        results.append((None, "fail", len(rows), int(curated_counts[unexpected].sum()),
                        f"Curated data contains sources that were not validated in this run: {unexpected}"))

    for source_id, info in sorted(expected.items()):
        staged = int(info["staged_rows"])
        src_rows = rows[rows["source_id"] == source_id]
        curated = len(src_rows)
        problems = []
        if curated == 0:
            problems.append("source missing from curated output")
        batch_ids = set(src_rows["batch_id"].dropna().astype(str).unique())
        wrong_batch = int((src_rows["batch_id"].astype(str) != info["batch_id"]).sum())
        if wrong_batch:
            problems.append(f"{wrong_batch:,} rows not from validated batch {info['batch_id']} "
                            f"(found {sorted(batch_ids)[:3]})")
        if curated > staged:
            problems.append(f"more curated rows ({curated:,}) than staged ({staged:,})")
        excluded = staged - curated
        fraction = excluded / staged if staged else 0.0
        allowed = max(max_excluded_fraction * staged, min_allowance)
        if excluded > allowed:
            problems.append(f"{excluded:,} rows ({fraction:.3%}) excluded, above the tolerance "
                            f"of {int(allowed):,} ({max_excluded_fraction:.1%}, minimum {min_allowance})")
        details = (f"staged {staged:,} = curated {curated:,} + excluded {max(excluded, 0):,} "
                   f"({fraction:.3%}, no valid UTC timestamp); batch {info['batch_id']}")
        if problems:
            results.append((source_id, "fail", staged, abs(excluded) if excluded else staged,
                            "; ".join(problems) + " | " + details))
        else:
            results.append((source_id, "pass", staged, 0, details))
    return results


def timestamp_check(rows: pd.DataFrame, run_time: datetime) -> list[tuple]:
    results = []
    ts_all = rows["crash_timestamp_utc"]
    tz = getattr(ts_all.dtype, "tz", None)
    if tz is None or str(tz) not in ("UTC", "+00:00", "Etc/UTC"):
        return [(None, "fail", len(rows), len(rows),
                 f"crash_timestamp_utc is not UTC-aware (dtype {ts_all.dtype})")]
    latest_ok = pd.Timestamp(run_time).tz_convert("UTC") + FUTURE_SLACK
    for source_id, src in rows.groupby("source_id", observed=True):
        ts = src["crash_timestamp_utc"]
        null = int(ts.isna().sum())
        too_early = int((ts < MIN_PLAUSIBLE_UTC).sum())
        future = int((ts > latest_ok).sum())
        date_key = pd.to_datetime(src["crash_date_key"]).dt.date
        mismatch = int((date_key != ts.dt.date).sum())
        failed = null + too_early + future + mismatch
        details = (f"null={null:,}, before {MIN_PLAUSIBLE_UTC.date()}={too_early:,}, "
                   f"after run time={future:,}, crash_date_key != UTC date={mismatch:,}; "
                   f"range {ts.min()} to {ts.max()}")
        results.append((str(source_id), "fail" if failed else "pass", len(src), failed, details))
    return results


def partition_check(rows: pd.DataFrame, files: pd.DataFrame,
                    reported_files: int | None) -> list[tuple]:
    results = []
    ts = rows["crash_timestamp_utc"]
    misplaced_all = (rows["year"].astype(int) != ts.dt.year) | (rows["month"].astype(int) != ts.dt.month)
    for source_id, src_files in files.groupby("source_id"):
        src_mask = rows["source_id"] == source_id
        src = rows[src_mask]
        misplaced = int(misplaced_all[src_mask].sum())
        data_months = set(zip(src["year"].astype(int), src["month"].astype(int)))
        folder_months = set(zip(src_files["year"], src_files["month"]))
        empty = int((src_files["num_rows"] == 0).sum())
        problems = []
        if misplaced:
            problems.append(f"{misplaced:,} rows in a partition that does not match their timestamp")
        if data_months != folder_months:
            problems.append(f"partition folders ({len(folder_months)}) do not match "
                            f"source-year-months in the data ({len(data_months)})")
        if empty:
            problems.append(f"{empty} empty partition file(s)")
        details = (f"{len(src_files)} files in {len(folder_months)} year/month partitions, "
                   f"{int(src_files['num_rows'].sum()):,} rows")
        results.append((str(source_id), "fail" if problems else "pass", len(src),
                        misplaced + empty, ("; ".join(problems) + " | " if problems else "") + details))

    total_files = len(files)
    if reported_files is not None and total_files != reported_files:
        results.append((None, "fail", len(rows), abs(total_files - reported_files),
                        f"{total_files} partition files on disk, harmonize reported {reported_files}"))
    else:
        results.append((None, "pass", len(rows), 0,
                        f"{total_files} partition files across {files['source_id'].nunique()} sources "
                        f"(matches harmonize)"))
    return results


# ---------- Orchestration ----------

def _staged_rows(path: str) -> int:
    """Row count straight from the staged Parquet footer, independent of harmonize."""
    return pq.ParquetFile(path).metadata.num_rows


def run_curated_checks(harmonize_result: dict, run_time: datetime | None = None,
                       log: bool = True) -> dict:
    """
    Audit the curated candidate described by harmonize_result (the dict
    harmonize() returns). Logs every result to dq_run_log, then raises
    RuntimeError if any check failed.
    """
    run_time = run_time or datetime.now(timezone.utc)
    candidate = Path(harmonize_result["candidate_path"])
    run_id = harmonize_result["curated_run_id"]
    expected = {
        sid: {"batch_id": info["batch_id"], "staged_rows": _staged_rows(info["staged_path"])}
        for sid, info in harmonize_result["sources"].items()
    }
    print(f"[curated_checks] auditing {candidate} (run {run_id})")

    schema, rows, files = load_candidate(candidate)
    print(f"[curated_checks] {len(rows):,} rows in {len(files)} files")

    checks = [
        ("curated_schema_check",         lambda: schema_check(schema, _load_canonical(), len(rows))),
        ("curated_uniqueness_check",     lambda: uniqueness_check(rows)),
        ("curated_reconciliation_check", lambda: reconciliation_check(
            rows, expected, MAX_EXCLUDED_FRACTION, MIN_EXCLUDED_ALLOWANCE)),
        ("curated_timestamp_check",      lambda: timestamp_check(rows, run_time)),
        ("curated_partition_check",      lambda: partition_check(rows, files, harmonize_result.get("partition_files"))),
    ]

    if log:
        from validation.checks import log_check

    results, failures = [], []
    for name, fn in checks:
        try:
            outcomes = fn()
        except Exception as e:  # a check that crashes counts as a failure
            outcomes = [(None, "fail", len(rows), len(rows), f"Check raised: {e}")]
        for source_id, status, checked, failed, details in outcomes:
            batch_id = expected[source_id]["batch_id"] if source_id in expected else run_id
            if log:
                log_check(source_id, batch_id, name, status, checked, failed, details)
            results.append({"check": name, "source_id": source_id, "status": status, "details": details})
            marker = {"pass": "OK", "warn": "WARN", "fail": "FAIL"}[status]
            print(f"[curated_checks] {marker:4} {name:30} {source_id or 'all sources':12} {details[:140]}")
            if status == "fail":
                failures.append(f"{name} ({source_id or 'all sources'}): {details}")

    summary = {
        "curated_run_id": run_id,
        "checks_run": len(results),
        "passed": sum(r["status"] == "pass" for r in results),
        "warned": sum(r["status"] == "warn" for r in results),
        "failed": len(failures),
        "rows_checked": len(rows),
    }
    if failures:
        raise RuntimeError(f"{len(failures)} curated check(s) failed: " + " || ".join(failures))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--summary", help="harmonize summary JSON (default: the one beside the candidate)")
    args = parser.parse_args()

    from transform.curated import CANDIDATE_SUMMARY
    summary_path = Path(args.summary) if args.summary else CANDIDATE_SUMMARY
    if not summary_path.exists():
        print(f"[curated_checks] FAILED: no harmonize summary at {summary_path}; run "
              f"src/transform/curated.py first", file=sys.stderr)
        sys.exit(1)
    try:
        result = run_curated_checks(json.loads(summary_path.read_text(encoding="utf-8")))
    except Exception as e:
        print(f"[curated_checks] FAILED: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"[curated_checks] done: {result}")


if __name__ == "__main__":
    main()
