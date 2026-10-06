#!/usr/bin/env python3
"""
curated.py

Staging -> curated. Reads every staged batch (all sources), concatenates
into one canonical `fact_crash` dataset, casts columns to canonical
types, parses timestamps into UTC, and writes partitioned Parquet:

    data/curated/fact_crash/source_id=<id>/year=<YYYY>/month=<MM>/part-0.parquet

Also parses each source's timestamp strings into real UTC timestamps.
This is the layer where parsing happens (not staging), so staging stays
source-faithful and the date-format logic lives in one place.

Usage:
    python -m src.transform.curated
    python -m src.transform.curated --read-source chicago_us --read-year 2024 --read-month 3
"""

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.paths import STAGING_ROOT, CURATED_ROOT


# ---------- Canonical column types ----------

INT_COLS = ["num_injured_total", "num_killed", "num_vehicles_involved", "posted_speed_limit_mph"]
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

    # Crash timestamp (required)
    crash = pd.to_datetime(df["crash_timestamp_utc"], format=fmt, errors="coerce")
    if crash.isna().all() and df["crash_timestamp_utc"].notna().any():
        crash = pd.to_datetime(df["crash_timestamp_utc"], errors="coerce")
    if tz:
        crash = crash.dt.tz_localize(tz, ambiguous="NaT", nonexistent="NaT")
        crash = crash.dt.tz_convert("UTC")
    df["crash_timestamp_utc"] = crash

    # Police notified timestamp (optional; UK/NYC are all-null)
    police_col = "police_notified_timestamp_utc"
    if police_col in df.columns and df[police_col].notna().any():
        police = pd.to_datetime(df[police_col], format=fmt, errors="coerce")
        if police.isna().all() and df[police_col].notna().any():
            police = pd.to_datetime(df[police_col], errors="coerce")
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

def _read_all_staging() -> pd.DataFrame:
    """
    Walk every data/staging/<source_id>/<batch_id>/fact_crash.parquet and
    concatenate. Uses the newest batch per source (replace-per-batch
    semantics at the staging layer).
    """
    if not STAGING_ROOT.exists():
        raise FileNotFoundError(f"No staging root at {STAGING_ROOT}")

    per_source = {}
    for source_dir in sorted(STAGING_ROOT.iterdir()):
        if not source_dir.is_dir():
            continue
        source_id = source_dir.name
        batch_dirs = sorted(
            [d for d in source_dir.iterdir() if d.is_dir()],
            key=lambda p: p.name,
        )
        if not batch_dirs:
            continue
        latest = batch_dirs[-1]
        parquet = latest / "fact_crash.parquet"
        if not parquet.exists():
            print(f"[curated] warning: no fact_crash.parquet in {latest}, skipping")
            continue
        print(f"[curated] reading {source_id} from batch {latest.name}")
        df = pd.read_parquet(parquet)
        per_source[source_id] = df

    if not per_source:
        raise RuntimeError("No staging batches found. Run staging first.")

    combined = pd.concat(per_source.values(), ignore_index=True)
    print(f"[curated] concatenated {len(combined):,} rows from {len(per_source)} sources")
    return combined


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

def _write_partitioned(df: pd.DataFrame) -> Path:
    """
    Replace the curated output then write partitioned Parquet:
        fact_crash/source_id=<id>/year=<YYYY>/month=<MM>/part-0.parquet
    Replace-partition semantics for rerun safety.
    """
    target = CURATED_ROOT / "fact_crash"
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

    target = CURATED_ROOT / "fact_crash"
    if not target.exists():
        raise FileNotFoundError(f"No curated output at {target}")
    dataset = ds.dataset(target, format="parquet", partitioning="hive")
    filter_expr = (ds.field("source_id") == source_id) & (ds.field("year") == year)
    if month is not None:
        filter_expr = filter_expr & (ds.field("month") == month)
    table = dataset.to_table(filter=filter_expr)
    return table.to_pandas()


# ---------- Public entry point ----------

def harmonize() -> dict:
    print(f"[curated] reading staging from {STAGING_ROOT}")
    combined = _read_all_staging()

    print(f"[curated] parsing timestamps per source")
    frames = []
    for source_id, group in combined.groupby("source_id", dropna=False):
        group = group.copy()
        group = _parse_timestamps(group, source_id)
        frames.append(group)
    df = pd.concat(frames, ignore_index=True)

    df = _add_crash_date_key(df)
    df = _add_partition_columns(df)

    # Drop rows where partition columns are null (unparseable timestamps)
    before = len(df)
    df = df[df["_partition_year"].notna() & df["_partition_month"].notna()].copy()
    if before != len(df):
        print(f"[curated] dropped {before - len(df):,} rows with no partition year/month")

    print(f"[curated] writing partitioned Parquet under {CURATED_ROOT / 'fact_crash'}")
    target = _write_partitioned(df)

    n_files = sum(1 for _ in target.rglob("*.parquet"))
    print(f"[curated] wrote {n_files} partition files")

    return {
        "rows_written": len(df),
        "partition_files": n_files,
        "curated_path": str(target),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--read-source")
    parser.add_argument("--read-year", type=int)
    parser.add_argument("--read-month", type=int)
    args = parser.parse_args()

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