#!/usr/bin/env python3
"""
checks.py

Automated data-quality checks against a staging batch. Each check
writes one row to `dq_run_log` in Postgres, so results are queryable
and visible in task logs (per the project's data-quality requirement
of 5+ automated checks).

Design notes
------------
- Checks run against the STAGING Parquet (`fact_crash.parquet`), not
  raw. Staging is the first layer where the canonical schema applies.
- Source-specific structural gaps are encoded in STRUCTURAL_GAPS and
  checked against -- e.g. `num_killed` is null for uk_stats19 by
  design, not a failure. This mirrors the matrix in
  docs/data_dictionary.md.
- `fail` writes its dq_run_log row first, then raises, so Airflow
  marks the task failed but the audit trail survives.
- `warn` writes and continues; used for soft signals (e.g. speed
  limits outside plausible range).
- date_logic_check tolerates a small number of inverted timestamps for
  sources with a documented anomaly (chicago_us has 1 known row).

Usage:
    python -m src.validation.checks --source chicago_us --batch <batch_id>
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.db import get_connection
from utils.paths import staging_batch_dir


REPO_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_SCHEMA = REPO_ROOT / "config" / "canonical_schema.yaml"
ADAPTERS_DIR = REPO_ROOT / "config" / "adapters"


# ---------- Source-specific structural gaps ----------

STRUCTURAL_GAPS = {
    "chicago_us": set(),
    "uk_stats19": {
        "police_notified_timestamp_utc",
        "city",
        "num_killed",
        "crash_type",
        "primary_cause",
    },
    "nyc_us": {
        "police_notified_timestamp_utc",
        "crash_type",
        "weather_condition",
        "lighting_condition",
        "posted_speed_limit_mph",
    },
}

# Documented anomaly tolerance: number of inverted date-logic rows
# allowed before the check fails.
DATE_LOGIC_TOLERANCE = {
    "chicago_us": 5,
    "nyc_us": 0,
    "uk_stats19": 0,
}

# Numeric range bounds. (min, max) or None to skip a bound.
NUMERIC_RANGES = {
    "num_injured_total": (0, None),
    "num_killed": (0, None),
    "num_vehicles_involved": (0, None),
    "posted_speed_limit_mph": (0, 200),
}

# Speed limits outside this band produce a WARN, not a FAIL.
SPEED_FLAG_BAND = (5, 70)

# Accepted-value constraints mirrored from the DDL CHECK constraints.
ACCEPTED_VALUES = {
    "severity": {"fatal", "serious", "minor", "none", "unknown"},
    "country": {"US", "GB"},
}


# ---------- Helpers ----------

def _load_canonical_schema() -> dict:
    with open(CANONICAL_SCHEMA, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_adapter(source_id: str) -> dict:
    for yaml_path in sorted(ADAPTERS_DIR.glob("*.yaml")):
        with open(yaml_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        if config.get("source_id") == source_id:
            return config
    raise FileNotFoundError(
        f"No adapter in {ADAPTERS_DIR} has source_id={source_id!r}"
    )


def _read_staging(source_id: str, batch_id: str) -> pd.DataFrame:
    path = staging_batch_dir(source_id, batch_id) / "fact_crash.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"No staging Parquet at {path}. Run src.transform.staging first."
        )
    return pd.read_parquet(path)


def _log_check(
    source_id: str,
    batch_id: str,
    check_name: str,
    status: str,
    rows_checked: int,
    rows_failed: int,
    details: str,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dq_run_log
                    (batch_id, source_id, check_name, status,
                     rows_checked, rows_failed, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (batch_id, source_id, check_name, status,
                 int(rows_checked), int(rows_failed), details[:1000]),
            )


# ---------- Individual checks ----------

def schema_check(df: pd.DataFrame, canonical: dict) -> tuple[str, int, int, str]:
    expected = {f["name"] for f in canonical["fields"]}
    present = set(df.columns)
    missing = expected - present
    if missing:
        return ("fail", len(df), len(df),
                f"Missing canonical fields: {sorted(missing)}")
    return ("pass", len(df), 0, f"All {len(expected)} canonical fields present")


def nullability_check(
    df: pd.DataFrame, canonical: dict, source_id: str
) -> tuple[str, int, int, str]:
    gaps = STRUCTURAL_GAPS.get(source_id, set())
    nullable_by_design = {f["name"] for f in canonical["fields"] if not f.get("nullable", True)}
    enforced = nullable_by_design - gaps

    failures = {}
    for col in enforced:
        if col not in df.columns:
            continue
        n_null = int(df[col].isna().sum())
        if n_null > 0:
            failures[col] = n_null

    if failures:
        total = sum(failures.values())
        return ("fail", len(df), total,
                f"Non-nullable fields with nulls: {failures}")
    return ("pass", len(df), 0,
            f"No nulls in {len(enforced)} non-nullable fields")


def uniqueness_check(df: pd.DataFrame, source_id: str) -> tuple[str, int, int, str]:
    if "source_record_id" not in df.columns or "crash_id" not in df.columns:
        return ("fail", len(df), len(df), "Missing source_record_id or crash_id")
    dup_src = int(df["source_record_id"].duplicated().sum())
    dup_cid = int(df["crash_id"].duplicated().sum())
    total = dup_src + dup_cid
    if total > 0:
        return ("fail", len(df), total,
                f"Duplicates: source_record_id={dup_src}, crash_id={dup_cid}")
    return ("pass", len(df), 0, "All keys unique within batch")


def accepted_values_check(df: pd.DataFrame) -> tuple[str, int, int, str]:
    failures = {}
    for col, allowed in ACCEPTED_VALUES.items():
        if col not in df.columns:
            continue
        bad = ~df[col].isin(allowed) & df[col].notna()
        n = int(bad.sum())
        if n > 0:
            sample = df.loc[bad, col].dropna().unique()[:5].tolist()
            failures[col] = (n, sample)
    if failures:
        total = sum(n for n, _ in failures.values())
        return ("fail", len(df), total,
                f"Values outside accepted sets: {failures}")
    return ("pass", len(df), 0, "All accepted-value constraints satisfied")


def range_check(df: pd.DataFrame, source_id: str) -> tuple[str, int, int, str]:
    fails = {}
    warns = {}

    for col, (lo, hi) in NUMERIC_RANGES.items():
        if col not in df.columns:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        bad = pd.Series(False, index=df.index)
        if lo is not None:
            bad |= numeric < lo
        if hi is not None:
            bad |= numeric > hi
        bad &= numeric.notna()
        if bad.sum() > 0:
            fails[col] = int(bad.sum())

    if "posted_speed_limit_mph" in df.columns:
        numeric = pd.to_numeric(df["posted_speed_limit_mph"], errors="coerce")
        lo, hi = SPEED_FLAG_BAND
        out_of_band = ((numeric < lo) | (numeric > hi)) & numeric.notna()
        if out_of_band.sum() > 0:
            warns["posted_speed_limit_mph"] = int(out_of_band.sum())

    if "latitude" in df.columns and "longitude" in df.columns:
        adapter = _load_adapter(source_id)
        bbox = adapter.get("bounding_box", {})
        lat_lo, lat_hi = bbox.get("latitude", [None, None])
        lon_lo, lon_hi = bbox.get("longitude", [None, None])
        lat = pd.to_numeric(df["latitude"], errors="coerce")
        lon = pd.to_numeric(df["longitude"], errors="coerce")
        if lat_lo is not None:
            bad_lat = ((lat < lat_lo) | (lat > lat_hi)) & lat.notna()
            if bad_lat.sum() > 0:
                fails["latitude_out_of_bbox"] = int(bad_lat.sum())
        if lon_lo is not None:
            bad_lon = ((lon < lon_lo) | (lon > lon_hi)) & lon.notna()
            if bad_lon.sum() > 0:
                fails["longitude_out_of_bbox"] = int(bad_lon.sum())

    if fails:
        total = sum(fails.values())
        return ("fail", len(df), total, f"Out-of-range values: {fails}")
    if warns:
        total = sum(warns.values())
        return ("warn", len(df), total, f"Flagged (soft): {warns}")
    return ("pass", len(df), 0, "All numeric ranges within bounds")


def date_logic_check(
    df: pd.DataFrame, source_id: str
) -> tuple[str, int, int, str]:
    gaps = STRUCTURAL_GAPS.get(source_id, set())
    if "police_notified_timestamp_utc" in gaps:
        return ("pass", len(df), 0,
                "Skipped: police_notified_timestamp_utc is a structural gap")

    if ("crash_timestamp_utc" not in df.columns
            or "police_notified_timestamp_utc" not in df.columns):
        return ("warn", len(df), 0, "One or both timestamp columns missing")

    crash = pd.to_datetime(df["crash_timestamp_utc"], errors="coerce", utc=True)
    police = pd.to_datetime(df["police_notified_timestamp_utc"], errors="coerce", utc=True)
    both = crash.notna() & police.notna()
    inverted = both & (police < crash)
    n = int(inverted.sum())

    tolerance = DATE_LOGIC_TOLERANCE.get(source_id, 0)
    if n > tolerance:
        return ("fail", len(df), n,
                f"{n} rows have police_notified < crash_timestamp (tolerance={tolerance})")
    if n > 0:
        return ("warn", len(df), n,
                f"{n} rows with inverted timestamps (within tolerance={tolerance}; documented anomaly)")
    return ("pass", len(df), 0,
            f"Date logic holds ({int(both.sum()):,} rows with both timestamps)")


def referential_check(df: pd.DataFrame, source_id: str) -> tuple[str, int, int, str]:
    if "source_id" not in df.columns:
        return ("fail", len(df), len(df), "Missing source_id column")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT source_id FROM dim_source")
            known = {row[0] for row in cur.fetchall()}
    distinct = set(df["source_id"].dropna().unique())
    unknown = distinct - known
    if unknown:
        return ("fail", len(df), len(df),
                f"source_id values not in dim_source: {sorted(unknown)}")
    return ("pass", len(df), 0, f"All source_id values ({sorted(distinct)}) known")


def row_count_check(df: pd.DataFrame, source_id: str, batch_id: str) -> tuple[str, int, int, str]:
    import json
    n = len(df)
    if n == 0:
        return ("fail", 0, 0, "Staging produced zero rows")
    raw_dir = Path("/opt/airflow/data/raw") / source_id / batch_id
    manifest_path = raw_dir / "manifest.json"
    raw_total = None
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        raw_total = manifest.get("total_records_or_files")
    detail = f"Staged rows: {n:,}"
    if raw_total:
        detail += f" | raw manifest total: {raw_total:,}"
        if n > raw_total:
            return ("warn", n, 0, detail + " | staged > raw (unexpected)")
    return ("pass", n, 0, detail)


# ---------- Orchestration ----------

def run_checks(source_id: str, batch_id: str) -> dict:
    print(f"[checks] source_id={source_id} batch_id={batch_id}")
    canonical = _load_canonical_schema()
    df = _read_staging(source_id, batch_id)
    print(f"[checks] loaded {len(df):,} staged rows, {len(df.columns)} columns")

    results = []

    checks = [
        ("schema_check",           lambda: schema_check(df, canonical)),
        ("nullability_check",      lambda: nullability_check(df, canonical, source_id)),
        ("uniqueness_check",       lambda: uniqueness_check(df, source_id)),
        ("accepted_values_check",  lambda: accepted_values_check(df)),
        ("range_check",            lambda: range_check(df, source_id)),
        ("date_logic_check",       lambda: date_logic_check(df, source_id)),
        ("referential_check",      lambda: referential_check(df, source_id)),
        ("row_count_check",        lambda: row_count_check(df, source_id, batch_id)),
    ]

    failures = []
    for name, fn in checks:
        try:
            status, checked, failed, details = fn()
        except Exception as e:
            status, checked, failed, details = ("fail", len(df), len(df),
                                                f"Check raised: {e}")
        _log_check(source_id, batch_id, name, status, checked, failed, details)
        results.append((name, status, failed, details))
        marker = {"pass": "OK", "warn": "WARN", "fail": "FAIL"}[status]
        print(f"[checks] {marker:4} {name:24} failed={failed:>10,}  {details[:120]}")
        if status == "fail":
            failures.append((name, details))

    summary = {
        "source_id": source_id,
        "batch_id": batch_id,
        "checks_run": len(results),
        "passed": sum(1 for _, s, _, _ in results if s == "pass"),
        "warned": sum(1 for _, s, _, _ in results if s == "warn"),
        "failed": sum(1 for _, s, _, _ in results if s == "fail"),
    }

    if failures:
        msg = "; ".join(f"{n}: {d}" for n, d in failures)
        raise RuntimeError(f"{len(failures)} check(s) failed: {msg}")

    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--batch", required=True)
    args = parser.parse_args()

    try:
        summary = run_checks(args.source, args.batch)
    except Exception as e:
        print(f"[checks] FAILED: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[checks] done: {summary}")


if __name__ == "__main__":
    main()