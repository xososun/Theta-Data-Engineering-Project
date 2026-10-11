#!/usr/bin/env python3
"""
curated.py

Staging -> curated. Reads one staged batch per source, concatenates
into one canonical `fact_crash` dataset, casts columns to canonical
types, parses timestamps into UTC, and writes partitioned Parquet.

Write-audit-publish: harmonize() never overwrites the live curated data.
It writes a CANDIDATE:

    data/curated/_candidate/fact_crash/source_id=<id>/year=<YYYY>/month=<MM>/

src/validation/curated_checks.py then audits the candidate, and only if
every check passes does publish() swap it into the live location that
consumers (load_postgres.py, the notebooks) read:

    data/curated/fact_crash/source_id=<id>/year=<YYYY>/month=<MM>/

So a harmonization bug can never reach the published Parquet or Postgres.

Also parses each source's timestamp strings into real UTC timestamps.
This is the layer where parsing happens (not staging), so staging stays
source-faithful and the date-format logic lives in one place.

Usage:
    python src/transform/curated.py                      # harmonize -> candidate
    python src/validation/curated_checks.py              # audit the candidate
    python src/transform/curated.py --publish            # candidate -> live
    python src/transform/curated.py --read-source chicago_us --read-year 2024 --read-month 3
"""

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.paths import STAGING_ROOT, CURATED_ROOT, staging_batch_dir

LIVE_DIR = CURATED_ROOT / "fact_crash"
CANDIDATE_DIR = CURATED_ROOT / "_candidate" / "fact_crash"
CANDIDATE_SUMMARY = CURATED_ROOT / "_candidate" / "harmonize_summary.json"


# ---------- Canonical column types ----------

INT_COLS = ["num_injured_total", "num_killed", "num_vehicles_involved", "posted_speed_limit_mph"]
# Cast explicitly so the Parquet type is a UTC timestamp even when every
# value in a run is null (e.g. police_notified without Chicago rows).
TIMESTAMP_COLS = ["crash_timestamp_utc", "police_notified_timestamp_utc", "ingested_at_utc"]
FLOAT_COLS = ["latitude", "longitude"]
BOOL_COLS = ["has_valid_coordinates", "hit_and_run"]
TEXT_COLS = [
    "crash_id", "source_id", "source_record_id", "city", "country",
    "source_local_timezone", "source_severity_raw", "severity",
    "crash_type", "primary_cause", "weather_condition", "lighting_condition",
    "batch_id", "source_row_raw_ref",
]


# ---------- Timestamp parsing ----------

DATE_FORMATS = {
    "chicago_us": "%m/%d/%Y %I:%M:%S %p",   # 08/20/2026 01:29:00 PM
    "uk_stats19":  "%d/%m/%Y %H:%M",        # 20/08/2026 13:29
    "nyc_us":      None,                    # ISO 8601 - pandas handles it
}

LOCAL_TZ = {
    "chicago_us": "America/Chicago",
    "uk_stats19":  "Europe/London",
    "nyc_us":      "America/New_York",
}


def _parse_timestamps(df: pd.DataFrame, source_id: str) -> pd.DataFrame:
    """
    Parse string timestamps into UTC-aware datetimes.
    Source-local time -> UTC via pandas tz_localize + tz_convert.
    """
    fmt = DATE_FORMATS.get(source_id)
    tz = LOCAL_TZ.get(source_id)

    # Crash timestamp (required). A source with an explicit format is parsed
    # strictly: if the format is wrong, rows fail to parse and the curated
    # reconciliation check fails the run. Falling back to pandas' guessing
    # would read UK DD/MM dates as MM/DD and silently swap day and month.
    crash = pd.to_datetime(df["crash_timestamp_utc"], format=fmt, errors="coerce")
    if tz:
        crash = crash.dt.tz_localize(tz, ambiguous="NaT", nonexistent="NaT")
        crash = crash.dt.tz_convert("UTC")
    df["crash_timestamp_utc"] = crash

    # Police notified timestamp (optional; UK/NYC are all-null)
    police_col = "police_notified_timestamp_utc"
    if police_col in df.columns and df[police_col].notna().any():
        police = pd.to_datetime(df[police_col], format=fmt, errors="coerce")
        if tz:
            police = police.dt.tz_localize(tz, ambiguous="NaT", nonexistent="NaT")
            police = police.dt.tz_convert("UTC")
        df[police_col] = police

    return df


# ---------- Type casting ----------

def _to_bool(v):
    """Coerce common representations to a nullable Python bool."""
    if v is None:
        return pd.NA
    if isinstance(v, float) and pd.isna(v):
        return pd.NA
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "y", "yes", "t"):
        return True
    if s in ("false", "0", "n", "no", "f", ""):
        return False
    return pd.NA


def _cast_canonical_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce columns to canonical types before writing Parquet. Staging
    keeps everything as strings (source-faithful); curated casts to the
    real types so the Parquet schema is stable and Postgres load can
    insert without per-column coercion.
    """
    for col in TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = df[col].map(_to_bool).astype("boolean")
    for col in TEXT_COLS:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


# ---------- Reading staging ----------

def _newest_staged_batches() -> dict[str, str]:
    """Newest staged batch per source. Only for manual CLI runs; the DAG
    passes the exact batches that passed validation in the same run."""
    if not STAGING_ROOT.exists():
        raise FileNotFoundError(f"No staging root at {STAGING_ROOT}")
    batches = {}
    for source_dir in sorted(STAGING_ROOT.iterdir()):
        if not source_dir.is_dir():
            continue
        staged = sorted(d.name for d in source_dir.iterdir()
                        if d.is_dir() and (d / "fact_crash.parquet").exists())
        if staged:
            batches[source_dir.name] = staged[-1]
    return batches


def _read_staging(batches: dict[str, str]) -> tuple[pd.DataFrame, dict]:
    """
    Read exactly one staged batch per source and concatenate.

    Returns the combined frame and per-source bookkeeping (batch_id, path,
    staged row count) that the curated checks reconcile against.
    """
    if not batches:
        raise RuntimeError("No staging batches found. Run staging first.")
    frames, sources = [], {}
    for source_id, batch_id in sorted(batches.items()):
        parquet = staging_batch_dir(source_id, batch_id) / "fact_crash.parquet"
        if not parquet.exists():
            raise FileNotFoundError(f"Staged batch missing for {source_id}: {parquet}")
        df = pd.read_parquet(parquet)
        print(f"[curated] reading {source_id} batch {batch_id}: {len(df):,} rows")
        frames.append(df)
        sources[source_id] = {"batch_id": batch_id, "staged_path": str(parquet),
                              "staged_rows": len(df)}
    combined = pd.concat(frames, ignore_index=True)
    print(f"[curated] concatenated {len(combined):,} rows from {len(frames)} sources")
    return combined, sources


# ---------- Harmonization ----------

def _add_crash_date_key(df: pd.DataFrame) -> pd.DataFrame:
    if "crash_timestamp_utc" not in df.columns:
        raise KeyError("crash_timestamp_utc missing; cannot derive crash_date_key")
    df["crash_date_key"] = pd.to_datetime(df["crash_timestamp_utc"], utc=True).dt.date
    return df


def _add_partition_columns(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["crash_timestamp_utc"], utc=True, errors="coerce")
    df["_partition_year"] = ts.dt.year.astype("Int64")
    df["_partition_month"] = ts.dt.month.astype("Int64")
    return df


# ---------- Writing partitioned Parquet ----------

def _write_partitioned(df: pd.DataFrame, target: Path = CANDIDATE_DIR) -> Path:
    """
    Write partitioned Parquet to `target` (the candidate by default):
        <target>/source_id=<id>/year=<YYYY>/month=<MM>/<file>.parquet
    The target is replaced, never appended to, so reruns are safe.
    """
    target = Path(target)
    if target.exists():
        print(f"[curated] removing previous output at {target}")
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    # Drop rows with unparseable crash_date_key (can't be inserted anyway)
    before = len(df)
    df = df[df["crash_date_key"].notna()].copy()
    after = len(df)
    if before != after:
        print(f"[curated] dropped {before - after:,} rows with unparseable crash_timestamp_utc")

    # Cast canonical types BEFORE writing
    df = _cast_canonical_types(df)

    # Partition columns must not duplicate data columns
    partition_df = df.drop(columns=["_partition_year", "_partition_month"], errors="ignore")
    partition_df = partition_df.assign(
        year=df["_partition_year"],
        month=df["_partition_month"],
    )

    partition_df.to_parquet(
        target,
        partition_cols=["source_id", "year", "month"],
        index=False,
    )
    return target


# ---------- Partition-pruned read (demo) ----------

def read_partition(source_id: str, year: int, month: int | None = None) -> pd.DataFrame:
    """
    Read exactly one partition's Parquet file - demonstrating that reading
    a single source-year (or source-year-month) does NOT scan the whole
    dataset. Uses pyarrow.dataset with a filter for partition pruning.
    """
    import pyarrow.dataset as ds

    target = LIVE_DIR
    if not target.exists():
        raise FileNotFoundError(f"No curated output at {target}")
    dataset = ds.dataset(target, format="parquet", partitioning="hive")
    filter_expr = (ds.field("source_id") == source_id) & (ds.field("year") == year)
    if month is not None:
        filter_expr = filter_expr & (ds.field("month") == month)
    table = dataset.to_table(filter=filter_expr)
    return table.to_pandas()


# ---------- Public entry point ----------

def harmonize(batches: dict[str, str] | None = None, target: Path = CANDIDATE_DIR) -> dict:
    """
    Harmonize one staged batch per source into a candidate curated dataset.

    batches: {source_id: batch_id}. The DAG passes the batches validated in
    this run; when omitted (manual run), the newest staged batch per source.
    """
    if batches is None:
        batches = _newest_staged_batches()
    print(f"[curated] harmonizing batches: {batches}")
    combined, sources = _read_staging(batches)

    print("[curated] parsing timestamps per source")
    frames = []
    for source_id, group in combined.groupby("source_id", dropna=False):
        group = group.copy()
        group = _parse_timestamps(group, source_id)
        frames.append(group)
    df = pd.concat(frames, ignore_index=True)

    df = _add_crash_date_key(df)
    df = _add_partition_columns(df)

    # Drop rows whose timestamp could not be converted to UTC: unparseable
    # values, and local times in a daylight-saving transition hour (1:00-1:59
    # happens twice in autumn; 2:00-2:59 does not exist in spring). Counted
    # per source so the curated checks can reconcile staged vs curated rows.
    keep = df["_partition_year"].notna() & df["_partition_month"].notna()
    for source_id, n_excluded in df.loc[~keep, "source_id"].value_counts().items():
        print(f"[curated] {source_id}: excluded {n_excluded:,} rows with no valid UTC timestamp")
    df = df[keep].copy()
    if (~keep).any():
        print(f"[curated] dropped {int((~keep).sum()):,} rows with no partition year/month")

    curated_counts = df["source_id"].value_counts()
    for source_id, info in sources.items():
        info["curated_rows"] = int(curated_counts.get(source_id, 0))
        info["excluded_rows"] = info["staged_rows"] - info["curated_rows"]

    print(f"[curated] writing partitioned Parquet under {target}")
    target = _write_partitioned(df, target)

    n_files = sum(1 for _ in target.rglob("*.parquet"))
    print(f"[curated] wrote {n_files} partition files")

    summary = {
        "curated_run_id": "curated_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "rows_written": len(df),
        "partition_files": n_files,
        "candidate_path": str(target),
        "sources": sources,
    }
    # Saved beside the candidate so manual runs of curated_checks.py can
    # pick it up; the DAG passes it directly.
    if Path(target) == CANDIDATE_DIR:
        CANDIDATE_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def publish(candidate: Path = CANDIDATE_DIR, live: Path = LIVE_DIR) -> dict:
    """
    Swap the audited candidate into the live location.

    Called only after src/validation/curated_checks.py has passed. The swap
    is two directory renames on the same filesystem, so readers see either
    the old dataset or the new one, never a half-written mix. The previous
    live dataset is removed only after the new one is in place.
    """
    candidate, live = Path(candidate), Path(live)
    if not candidate.exists():
        raise FileNotFoundError(
            f"No candidate at {candidate}. Run harmonize and curated_checks first."
        )
    previous = live.parent / "_previous_fact_crash"
    if previous.exists():
        shutil.rmtree(previous)
    if live.exists():
        live.rename(previous)
    candidate.rename(live)
    if previous.exists():
        shutil.rmtree(previous)
    if CANDIDATE_SUMMARY.exists() and candidate == CANDIDATE_DIR:
        CANDIDATE_SUMMARY.unlink()
    if candidate.parent.exists() and not any(candidate.parent.iterdir()):
        candidate.parent.rmdir()
    n_files = sum(1 for _ in live.rglob("*.parquet"))
    print(f"[curated] published {n_files} partition files to {live}")
    return {"published_path": str(live), "partition_files": n_files}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--read-source")
    parser.add_argument("--read-year", type=int)
    parser.add_argument("--read-month", type=int)
    parser.add_argument("--publish", action="store_true",
                        help="Swap the audited candidate into the live curated location")
    args = parser.parse_args()

    if args.publish:
        try:
            print(f"[curated] done: {publish()}")
        except Exception as e:
            print(f"[curated] FAILED: {e}", file=sys.stderr)
            sys.exit(1)
        return

    if args.read_source and args.read_year:
        df = read_partition(args.read_source, args.read_year, args.read_month)
        print(f"[curated] read_partition({args.read_source}, year={args.read_year}, month={args.read_month}) -> {len(df):,} rows")
        if len(df) > 0:
            cols = [c for c in ["crash_id", "source_id", "crash_date_key", "severity"] if c in df.columns]
            print(df[cols].head(3).to_string(index=False))
        return

    try:
        summary = harmonize()
    except Exception as e:
        print(f"[curated] FAILED: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"[curated] done: {summary}")


if __name__ == "__main__":
    main()