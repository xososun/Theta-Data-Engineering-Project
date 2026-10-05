#!/usr/bin/env python3
"""
profile_crashes.py

Reusable data-profiling script for the City of Chicago "Traffic Crashes -
Crashes" dataset (and, by extension, any CSV export that shares its schema).

This is meant to satisfy the "Source Inventory & Data Profiling" requirement
of the course project: it produces row/column counts, dtypes, missingness,
duplicates, date range, category distributions, and known data-quality
findings (mixed date formats, sentinel 0,0 coordinates, out-of-range speed
limits, near-empty flag columns) discovered from sample files.

Usage:
    python profile_crashes.py path/to/crashes.csv
    python profile_crashes.py path/to/crashes.csv --out report.json
    python profile_crashes.py path/to/crashes.csv --sample 50000

The script does not modify or clean the input file. It only reads and
reports. Cleaning/transformation belongs in the staging layer of the
pipeline, not here.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Chicago's approximate bounding box. Used only to flag rows for review;
# does not reject anything.
CHICAGO_LAT_RANGE = (41.60, 42.05)
CHICAGO_LON_RANGE = (-87.95, -87.50)

# Columns that are only ever populated with "Y" in the source system.
# A blank/NaN in these columns means "N or unknown," not "missing data"
# in the usual sense, so they are reported separately from other nulls.
Y_ONLY_FLAG_COLUMNS = [
    "INTERSECTION_RELATED_I",
    "NOT_RIGHT_OF_WAY_I",
    "HIT_AND_RUN_I",
    "PHOTOS_TAKEN_I",
    "STATEMENTS_TAKEN_I",
    "DOORING_I",
    "WORK_ZONE_I",
    "WORKERS_PRESENT_I",
    "CRASH_DATE_EST_I",
]

DATE_COLUMNS = ["CRASH_DATE", "DATE_POLICE_NOTIFIED"]

EXPECTED_COLUMNS = [
    "CRASH_RECORD_ID", "CRASH_DATE_EST_I", "CRASH_DATE", "POSTED_SPEED_LIMIT",
    "TRAFFIC_CONTROL_DEVICE", "DEVICE_CONDITION", "WEATHER_CONDITION",
    "LIGHTING_CONDITION", "FIRST_CRASH_TYPE", "TRAFFICWAY_TYPE", "LANE_CNT",
    "ALIGNMENT", "ROADWAY_SURFACE_COND", "ROAD_DEFECT", "REPORT_TYPE",
    "CRASH_TYPE", "INTERSECTION_RELATED_I", "NOT_RIGHT_OF_WAY_I",
    "HIT_AND_RUN_I", "DAMAGE", "DATE_POLICE_NOTIFIED", "PRIM_CONTRIBUTORY_CAUSE",
    "SEC_CONTRIBUTORY_CAUSE", "STREET_NO", "STREET_DIRECTION", "STREET_NAME",
    "BEAT_OF_OCCURRENCE", "PHOTOS_TAKEN_I", "STATEMENTS_TAKEN_I", "DOORING_I",
    "WORK_ZONE_I", "WORK_ZONE_TYPE", "WORKERS_PRESENT_I", "NUM_UNITS",
    "CRASH_MONTH", "MOST_SEVERE_INJURY", "INJURIES_TOTAL", "INJURIES_FATAL",
    "INJURIES_INCAPACITATING", "INJURIES_NON_INCAPACITATING",
    "INJURIES_REPORTED_NOT_EVIDENT", "INJURIES_NO_INDICATION",
    "INJURIES_UNKNOWN", "CRASH_HOUR", "CRASH_DAY_OF_WEEK", "IDOT_CONTROL_NO",
    "LATITUDE", "LONGITUDE", "LOCATION",
]


def load(path: str, sample: int | None) -> pd.DataFrame:
    if sample:
        return pd.read_csv(path, nrows=sample, low_memory=False)
    return pd.read_csv(path, low_memory=False)


def schema_report(df: pd.DataFrame) -> dict:
    present = set(df.columns)
    expected = set(EXPECTED_COLUMNS)
    return {
        "n_rows": len(df),
        "n_columns": len(df.columns),
        "missing_expected_columns": sorted(expected - present),
        "unexpected_extra_columns": sorted(present - expected),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
    }


def missingness_report(df: pd.DataFrame) -> dict:
    na_pct = (df.isna().sum() / len(df) * 100).round(2)
    ordinary = na_pct.drop(labels=[c for c in Y_ONLY_FLAG_COLUMNS if c in na_pct.index],
                            errors="ignore")
    flags = na_pct[[c for c in Y_ONLY_FLAG_COLUMNS if c in na_pct.index]]
    return {
        "ordinary_columns_pct_null": ordinary[ordinary > 0].sort_values(ascending=False).to_dict(),
        "y_only_flag_columns_pct_blank": flags.sort_values(ascending=False).to_dict(),
        "note": "Blank in a Y-only flag column means N/unknown, not missing data.",
    }


def duplicate_report(df: pd.DataFrame) -> dict:
    result = {"fully_duplicate_rows": int(df.duplicated().sum())}
    if "CRASH_RECORD_ID" in df.columns:
        result["duplicate_crash_record_ids"] = int(df["CRASH_RECORD_ID"].duplicated().sum())
    return result


def parse_dates_mixed(series: pd.Series) -> pd.Series:
    """
    CRASH_DATE / DATE_POLICE_NOTIFIED appear in at least two formats in
    real exports:
      - MM/DD/YYYY hh:mm:ss AM/PM
      - MM/DD/YYYY HH:MM  (24-hour, no seconds)
    pandas' 'mixed' format inference handles both without raising, whereas
    a single strptime format string will fail on a meaningful share of rows.
    """
    return pd.to_datetime(series, format="mixed", errors="coerce")


def date_report(df: pd.DataFrame) -> dict:
    out = {}
    for col in DATE_COLUMNS:
        if col not in df.columns:
            continue
        raw = df[col].dropna().astype(str)
        has_ampm = raw.str.contains("AM|PM", case=False, regex=True).sum()
        parsed = parse_dates_mixed(df[col])
        unparseable = int(parsed.isna().sum() - df[col].isna().sum())
        out[col] = {
            "min": str(parsed.min()) if parsed.notna().any() else None,
            "max": str(parsed.max()) if parsed.notna().any() else None,
            "rows_with_ampm_format": int(has_ampm),
            "rows_without_ampm_format": int(len(raw) - has_ampm),
            "rows_that_failed_to_parse": max(unparseable, 0),
            "future_dated_rows": int((parsed > pd.Timestamp.now()).sum()),
        }
    return out


def yearly_coverage_report(df: pd.DataFrame) -> dict:
    """
    Row counts by year, to check the documented claim that citywide coverage
    only starts ~Sept 2017 and that some districts have data from 2015.
    A big ramp-up around 2017 is expected; a flat pre-2017 count close to
    the ramped-up years would contradict the "partial" claim.
    """
    if "CRASH_DATE" not in df.columns:
        return {}
    parsed = parse_dates_mixed(df["CRASH_DATE"])
    counts = parsed.dt.year.value_counts().sort_index()
    return {int(y): int(c) for y, c in counts.items()}


def district_coverage_report(df: pd.DataFrame, cutoff: str = "2017-09-01") -> dict:
    """
    Per-district (BEAT_OF_OCCURRENCE) earliest crash date. Used to check the
    documentation's claim that some districts report from 2015 while
    citywide coverage only starts in Sept 2017. Reports how many distinct
    districts have their first record before the citywide cutoff, since
    that would confirm early data is real but partial (not every district
    reporting yet), rather than a data error.
    """
    if not {"BEAT_OF_OCCURRENCE", "CRASH_DATE"}.issubset(df.columns):
        return {}
    tmp = df[["BEAT_OF_OCCURRENCE", "CRASH_DATE"]].copy()
    tmp["parsed_date"] = parse_dates_mixed(tmp["CRASH_DATE"])
    tmp = tmp.dropna(subset=["parsed_date", "BEAT_OF_OCCURRENCE"])
    # BEAT_OF_OCCURRENCE encodes a police beat, not a district directly;
    # district is conventionally the first 1-2 digits of the beat number.
    tmp["district"] = (tmp["BEAT_OF_OCCURRENCE"] // 100).astype(int)
    first_by_district = tmp.groupby("district")["parsed_date"].min()
    cutoff_ts = pd.Timestamp(cutoff)
    before_cutoff = first_by_district[first_by_district < cutoff_ts]
    n_missing_beat = int(df["BEAT_OF_OCCURRENCE"].isna().sum())
    return {
        "rows_missing_beat_of_occurrence": n_missing_beat,
        "n_districts": int(first_by_district.shape[0]),
        "n_districts_with_data_before_cutoff": int(before_cutoff.shape[0]),
        "cutoff_used": cutoff,
        "earliest_date_overall": str(first_by_district.min()),
        "districts_before_cutoff_and_their_start_date": {
            int(d): str(dt) for d, dt in before_cutoff.sort_values().items()
        },
    }


def date_logic_report(df: pd.DataFrame) -> dict:
    """
    DATE_POLICE_NOTIFIED should never be earlier than CRASH_DATE. Rows
    where it is are a business-rule violation worth flagging in
    validation, not just a formatting issue.
    """
    if not {"CRASH_DATE", "DATE_POLICE_NOTIFIED"}.issubset(df.columns):
        return {}
    crash = parse_dates_mixed(df["CRASH_DATE"])
    notified = parse_dates_mixed(df["DATE_POLICE_NOTIFIED"])
    both_present = crash.notna() & notified.notna()
    violation = both_present & (notified < crash)
    gap_days = (notified - crash).dt.total_seconds() / 86400
    return {
        "rows_notified_before_crash": int(violation.sum()),
        "max_gap_days_notified_after_crash": float(gap_days[both_present].max()) if both_present.any() else None,
    }


def coordinate_report(df: pd.DataFrame) -> dict:
    if not {"LATITUDE", "LONGITUDE"}.issubset(df.columns):
        return {}
    total = len(df)
    has_coords = df.dropna(subset=["LATITUDE", "LONGITUDE"])
    sentinel_zero = has_coords[(has_coords["LATITUDE"] == 0) | (has_coords["LONGITUDE"] == 0)]
    valid = has_coords[(has_coords["LATITUDE"] != 0) & (has_coords["LONGITUDE"] != 0)]
    out_of_bbox = valid[
        ~valid["LATITUDE"].between(*CHICAGO_LAT_RANGE)
        | ~valid["LONGITUDE"].between(*CHICAGO_LON_RANGE)
    ]
    return {
        "rows_missing_coordinates": int(total - len(has_coords)),
        "pct_missing_coordinates": round((total - len(has_coords)) / total * 100, 2) if total else None,
        "rows_with_sentinel_zero_zero": int(len(sentinel_zero)),
        "rows_outside_chicago_bounding_box": int(len(out_of_bbox)),
        "valid_lat_range": [float(valid["LATITUDE"].min()), float(valid["LATITUDE"].max())] if len(valid) else None,
        "valid_lon_range": [float(valid["LONGITUDE"].min()), float(valid["LONGITUDE"].max())] if len(valid) else None,
    }


def speed_limit_report(df: pd.DataFrame) -> dict:
    if "POSTED_SPEED_LIMIT" not in df.columns:
        return {}
    s = df["POSTED_SPEED_LIMIT"]
    suspect = s[(s < 5) | (s > 70)]
    return {
        "min": float(s.min()),
        "max": float(s.max()),
        "rows_below_5_or_above_70": int(len(suspect)),
        "value_counts": s.value_counts().sort_index().to_dict(),
    }


def category_report(df: pd.DataFrame, columns: list[str], top_n: int = 15) -> dict:
    out = {}
    for col in columns:
        if col not in df.columns:
            continue
        out[col] = df[col].value_counts(dropna=False).head(top_n).to_dict()
    return out


def injury_report(df: pd.DataFrame) -> dict:
    injury_cols = [c for c in df.columns if c.startswith("INJURIES_")]
    out = {"totals": {c: float(df[c].sum(skipna=True)) for c in injury_cols}}
    if "INJURIES_FATAL" in df.columns:
        out["fatal_crash_count"] = int((df["INJURIES_FATAL"].fillna(0) > 0).sum())
    if {"INJURIES_TOTAL", "MOST_SEVERE_INJURY"}.issubset(df.columns):
        both_null = df["INJURIES_TOTAL"].isna() & df["MOST_SEVERE_INJURY"].isna()
        out["rows_with_no_injury_data_at_all"] = int(both_null.sum())
    return out


def near_empty_columns(df: pd.DataFrame, threshold_pct: float = 95.0) -> list[str]:
    na_pct = df.isna().sum() / len(df) * 100
    return sorted(na_pct[na_pct >= threshold_pct].index.tolist())


def build_report(df: pd.DataFrame) -> dict:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema": schema_report(df),
        "missingness": missingness_report(df),
        "duplicates": duplicate_report(df),
        "dates": date_report(df),
        "date_logic": date_logic_report(df),
        "yearly_coverage": yearly_coverage_report(df),
        "district_coverage": district_coverage_report(df),
        "coordinates": coordinate_report(df),
        "speed_limit": speed_limit_report(df),
        "injuries": injury_report(df),
        "top_categories": category_report(
            df,
            ["CRASH_TYPE", "MOST_SEVERE_INJURY", "FIRST_CRASH_TYPE",
             "WEATHER_CONDITION", "LIGHTING_CONDITION", "TRAFFIC_CONTROL_DEVICE",
             "PRIM_CONTRIBUTORY_CAUSE", "REPORT_TYPE"],
        ),
        "near_empty_columns_ge_95pct_null": near_empty_columns(df),
    }


def print_summary(report: dict) -> None:
    s = report["schema"]
    print(f"Rows: {s['n_rows']:,}  Columns: {s['n_columns']}")
    if s["missing_expected_columns"]:
        print(f"⚠ Missing expected columns: {s['missing_expected_columns']}")
    if s["unexpected_extra_columns"]:
        print(f"⚠ Unexpected extra columns: {s['unexpected_extra_columns']}")

    dup = report["duplicates"]
    print(f"Fully duplicate rows: {dup['fully_duplicate_rows']}  "
          f"Duplicate CRASH_RECORD_IDs: {dup.get('duplicate_crash_record_ids', 'n/a')}")

    coords = report.get("coordinates", {})
    if coords:
        print(f"Missing coordinates: {coords['rows_missing_coordinates']:,} "
              f"({coords['pct_missing_coordinates']}%)  "
              f"Sentinel (0,0) rows: {coords['rows_with_sentinel_zero_zero']}  "
              f"Outside Chicago bbox: {coords['rows_outside_chicago_bounding_box']}")

    for col, d in report["dates"].items():
        print(f"{col}: {d['min']} to {d['max']}  "
              f"(AM/PM format: {d['rows_with_ampm_format']}, "
              f"24h format: {d['rows_without_ampm_format']}, "
              f"unparseable: {d['rows_that_failed_to_parse']}, "
              f"future-dated: {d['future_dated_rows']})")

    logic = report.get("date_logic", {})
    if logic:
        print(f"Rows notified before crash occurred (invalid): {logic['rows_notified_before_crash']}  "
              f"Max notify gap: {logic['max_gap_days_notified_after_crash']:.1f} days")

    yearly = report.get("yearly_coverage", {})
    if yearly:
        years_str = ", ".join(f"{y}: {c:,}" for y, c in sorted(yearly.items()))
        print(f"Rows by year: {years_str}")

    district = report.get("district_coverage", {})
    if district:
        print(f"Rows missing BEAT_OF_OCCURRENCE: {district['rows_missing_beat_of_occurrence']}  "
              f"Districts: {district['n_districts']}  "
              f"Districts with data before {district['cutoff_used']}: "
              f"{district['n_districts_with_data_before_cutoff']}  "
              f"Earliest date overall: {district['earliest_date_overall']}")
        if district["districts_before_cutoff_and_their_start_date"]:
            print(f"  Early-reporting districts: {district['districts_before_cutoff_and_their_start_date']}")

    inj = report["injuries"]
    print(f"Fatal crashes: {inj.get('fatal_crash_count', 'n/a')}  "
          f"Rows with no injury data at all: {inj.get('rows_with_no_injury_data_at_all', 'n/a')}")

    near_empty = report["near_empty_columns_ge_95pct_null"]
    if near_empty:
        print(f"Columns ≥95% null: {near_empty}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", help="Path to the crashes CSV file")
    parser.add_argument("--out", help="Write full JSON report to this path")
    parser.add_argument("--sample", type=int, default=None,
                         help="Only read the first N rows (useful for very large files)")
    args = parser.parse_args()

    if not Path(args.csv_path).exists():
        sys.exit(f"File not found: {args.csv_path}")

    df = load(args.csv_path, args.sample)
    report = build_report(df)
    print_summary(report)

    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nFull report written to {args.out}")


if __name__ == "__main__":
    main()
