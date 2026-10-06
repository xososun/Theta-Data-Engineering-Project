#!/usr/bin/env python3
"""
load_postgres.py

Curated -> PostgreSQL. Reads the curated Parquet partitions, ensures
dim_date is populated for every crash_date_key present, and UPSERTs
fact_crash rows using (source_id, source_record_id) as the conflict
target.

Idempotency: repeated runs converge to the same database state. The
UNIQUE constraint on (source_id, source_record_id) is the conflict
target, and ON CONFLICT DO UPDATE overwrites fields with the newer
values. Running this twice produces identical row counts.

Usage:
    python -m src.load.load_postgres
    python -m src.load.load_postgres --source chicago_us   # one source only
    python -m src.load.load_postgres --dry-run             # build rows, don't insert
"""

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.db import get_connection
from utils.paths import CURATED_ROOT


CURATED_DIR = CURATED_ROOT / "fact_crash"

# Column order MUST match the INSERT statement's column list.
FACT_COLUMNS = [
    "crash_id",
    "source_id",
    "source_record_id",
    "crash_timestamp_utc",
    "crash_date_key",
    "police_notified_timestamp_utc",
    "city",
    "country",
    "latitude",
    "longitude",
    "has_valid_coordinates",
    "severity",
    "source_severity_raw",
    "num_injured_total",
    "num_killed",
    "num_vehicles_involved",
    "crash_type",
    "primary_cause",
    "weather_condition",
    "lighting_condition",
    "posted_speed_limit_mph",
    "hit_and_run",
    "batch_id",
    "source_row_raw_ref",
]


# ---------- Reading curated Parquet ----------

def _read_curated(source_id: str | None = None) -> pd.DataFrame:
    if not CURATED_DIR.exists():
        raise FileNotFoundError(
            f"No curated output at {CURATED_DIR}. Run src.transform.curated first."
        )
    import pyarrow.dataset as ds
    dataset = ds.dataset(CURATED_DIR, format="parquet", partitioning="hive")
    if source_id:
        table = dataset.to_table(filter=ds.field("source_id") == source_id)
    else:
        table = dataset.to_table()
    df = table.to_pandas()
    # Drop the hive partition columns - they're not part of fact_crash
    df = df.drop(columns=[c for c in ("year", "month") if c in df.columns])
    return df


# ---------- dim_date ----------

def _populate_dim_date(dates: list[date]) -> int:
    """
    Idempotent: insert missing dates, do nothing for existing ones.
    Returns the number of NEW rows actually inserted.
    """
    if not dates:
        return 0
    rows = []
    for d in dates:
        if d is None or pd.isna(d):
            continue
        # Python weekday: Monday=0 ... Sunday=6 (matches our canonical day_of_week)
        dow = d.weekday()
        rows.append((d, d.year, d.month, d.day, dow, dow >= 5))
    if not rows:
        return 0

    keys = [r[0] for r in rows]
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Count existing so we can report exactly how many we inserted.
            cur.execute(
                "SELECT COUNT(*) FROM dim_date WHERE date_key = ANY(%s)",
                (keys,),
            )
            existing = cur.fetchone()[0]

            execute_values(
                cur,
                """
                INSERT INTO dim_date (date_key, year, month, day, day_of_week, is_weekend)
                VALUES %s
                ON CONFLICT (date_key) DO NOTHING
                """,
                rows,
                page_size=1000,
            )
    return len(rows) - existing


# ---------- fact_crash upsert ----------

UPSERT_SQL = """
INSERT INTO fact_crash (
    crash_id, source_id, source_record_id,
    crash_timestamp_utc, crash_date_key, police_notified_timestamp_utc,
    city, country, latitude, longitude, has_valid_coordinates,
    severity, source_severity_raw,
    num_injured_total, num_killed, num_vehicles_involved,
    crash_type, primary_cause, weather_condition, lighting_condition,
    posted_speed_limit_mph, hit_and_run,
    batch_id, source_row_raw_ref
) VALUES %s
ON CONFLICT (source_id, source_record_id) DO UPDATE SET
    crash_id                        = EXCLUDED.crash_id,
    crash_timestamp_utc             = EXCLUDED.crash_timestamp_utc,
    crash_date_key                  = EXCLUDED.crash_date_key,
    police_notified_timestamp_utc   = EXCLUDED.police_notified_timestamp_utc,
    city                            = EXCLUDED.city,
    country                         = EXCLUDED.country,
    latitude                        = EXCLUDED.latitude,
    longitude                       = EXCLUDED.longitude,
    has_valid_coordinates           = EXCLUDED.has_valid_coordinates,
    severity                        = EXCLUDED.severity,
    source_severity_raw             = EXCLUDED.source_severity_raw,
    num_injured_total               = EXCLUDED.num_injured_total,
    num_killed                      = EXCLUDED.num_killed,
    num_vehicles_involved           = EXCLUDED.num_vehicles_involved,
    crash_type                      = EXCLUDED.crash_type,
    primary_cause                   = EXCLUDED.primary_cause,
    weather_condition               = EXCLUDED.weather_condition,
    lighting_condition              = EXCLUDED.lighting_condition,
    posted_speed_limit_mph          = EXCLUDED.posted_speed_limit_mph,
    hit_and_run                     = EXCLUDED.hit_and_run,
    ingested_at_utc                 = now(),
    batch_id                        = EXCLUDED.batch_id,
    source_row_raw_ref              = EXCLUDED.source_row_raw_ref
"""


def _to_pg_val(v):
    """
    Coerce a pandas cell value into something psycopg2 can send as a
    parameter. Catches every flavour of pandas missing value up front
    (None, NaN, pd.NA, pd.NaT, numpy.nan) so 'NaT' never reaches Postgres
    as a literal string.
    """
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        # pd.isna can raise on some container types; fall through.
        pass

    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if hasattr(v, "to_pydatetime"):
        try:
            return v.to_pydatetime()
        except Exception:
            pass
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    return v


def _upsert_facts(df: pd.DataFrame, page_size: int = 5000) -> int:
    if df.empty:
        return 0

    missing = [c for c in FACT_COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(f"Curated data missing columns: {missing}")

    df = df[FACT_COLUMNS]
    records = [tuple(_to_pg_val(v) for v in row) for row in df.itertuples(index=False)]

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, records, page_size=page_size)
            return len(records)


# ---------- Public entry point ----------

def load(source_id: str | None = None, dry_run: bool = False) -> dict:
    print(f"[load] reading curated Parquet from {CURATED_DIR}")
    df = _read_curated(source_id=source_id)
    print(f"[load] {len(df):,} rows to load"
          + (f" (source_id={source_id})" if source_id else " (all sources)"))

    if dry_run:
        print("[load] dry run: skipping all writes")
        return {"rows_read": len(df), "rows_written": 0, "dim_date_inserted": 0}

    unique_dates = sorted({d for d in df["crash_date_key"].dropna().unique()})
    print(f"[load] populating dim_date for {len(unique_dates):,} unique dates")
    dim_inserted = _populate_dim_date(unique_dates)
    print(f"[load] dim_date inserted {dim_inserted:,} new rows")

    print(f"[load] UPSERTing {len(df):,} fact_crash rows (page_size=5000)")
    n = _upsert_facts(df)
    print(f"[load] fact_crash processed {n:,} rows")

    return {
        "rows_read": len(df),
        "rows_written": n,
        "dim_date_inserted": dim_inserted,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", help="Load only one source_id")
    parser.add_argument("--dry-run", action="store_true", help="Build rows, don't insert")
    args = parser.parse_args()

    try:
        result = load(source_id=args.source, dry_run=args.dry_run)
    except Exception as e:
        print(f"[load] FAILED: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[load] done: {result}")


if __name__ == "__main__":
    main()