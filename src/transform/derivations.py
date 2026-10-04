"""
derivations.py

Named helper functions for per-source derived fields that are too
complex to express as inline YAML expressions.

Each function takes a row (dict) and returns the derived value. Adapter
YAML references them by name via `derive_fn:`.

Kept separate from staging.py so the business rules are testable,
readable, and reusable from curated.py or analytics code.
"""

import pandas as pd


# ---------- Shared helpers ----------

def _is_missing(value) -> bool:
    """True if value is None, NaN, or an empty/whitespace string."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _to_float(value):
    """Try to convert to float; return None on failure or missing."""
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    """Try to convert to int; return None on failure or missing."""
    f = _to_float(value)
    if f is None:
        return None
    try:
        return int(f)
    except (TypeError, ValueError):
        return None


def combine_date_and_time(date_val, time_val):
    """Combine a date string and a time string into one 'DATE TIME' string."""
    if _is_missing(date_val) or _is_missing(time_val):
        return None
    return f"{date_val} {time_val}"


# ---------- Chicago ----------

def chicago_crash_id(row: dict) -> str:
    return f"chicago_us:{row.get('CRASH_RECORD_ID', '')}"


def chicago_has_valid_coordinates(row: dict, bbox: dict) -> bool:
    lat = _to_float(row.get("LATITUDE"))
    lon = _to_float(row.get("LONGITUDE"))
    if lat is None or lon is None:
        return False
    if lat == 0 and lon == 0:
        return False
    lat_min, lat_max = bbox["latitude"]
    lon_min, lon_max = bbox["longitude"]
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


# ---------- UK ----------

def uk_crash_id(row: dict) -> str:
    return f"uk_stats19:{row.get('collision_index', '')}"


def uk_crash_timestamp(row: dict) -> str:
    return combine_date_and_time(row.get("date"), row.get("time"))


def uk_has_valid_coordinates(row: dict, bbox: dict) -> bool:
    lat = _to_float(row.get("latitude"))
    lon = _to_float(row.get("longitude"))
    if lat is None or lon is None:
        return False
    if lat == 0 and lon == 0:
        return False
    lat_min, lat_max = bbox["latitude"]
    lon_min, lon_max = bbox["longitude"]
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


# ---------- NYC ----------

def nyc_crash_id(row: dict) -> str:
    return f"nyc_us:{row.get('collision_id', '')}"


def nyc_crash_timestamp(row: dict) -> str:
    return combine_date_and_time(row.get("crash_date"), row.get("crash_time"))


def nyc_has_valid_coordinates(row: dict, bbox: dict) -> bool:
    lat = _to_float(row.get("latitude"))
    lon = _to_float(row.get("longitude"))
    if lat is None or lon is None:
        return False
    if lat == 0 and lon == 0:
        return False
    lat_min, lat_max = bbox["latitude"]
    lon_min, lon_max = bbox["longitude"]
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


def nyc_num_killed(row: dict) -> int:
    """NYC aggregate field is ~88% null; sum the pedestrian/cyclist/motorist components."""
    parts = [
        row.get("number_of_pedestrians_killed"),
        row.get("number_of_cyclist_killed"),
        row.get("number_of_motorist_killed"),
    ]
    total = 0
    for p in parts:
        v = _to_int(p)
        if v is not None:
            total += v
    return total


def nyc_num_vehicles(row: dict) -> int:
    """NYC has no direct vehicle count; count non-null vehicle_type_code slots."""
    slots = [
        row.get("vehicle_type_code1"),
        row.get("vehicle_type_code2"),
        row.get("vehicle_type_code_3"),
        row.get("vehicle_type_code_4"),
        row.get("vehicle_type_code_5"),
    ]
    return sum(1 for s in slots if not _is_missing(s))


def nyc_severity(row: dict) -> str:
    """
    NYC has no categorical severity field; derive from injury/fatality
    counts. Reads the RAW source fields directly (not the mapped
    num_killed / num_injured_total) to avoid depending on field order
    inside the adapter's field_mapping.
    """
    injured = _to_int(row.get("number_of_persons_injured"))

    killed_parts = [
        row.get("number_of_pedestrians_killed"),
        row.get("number_of_cyclist_killed"),
        row.get("number_of_motorist_killed"),
    ]
    killed_total = 0
    has_killed_data = False
    for p in killed_parts:
        v = _to_int(p)
        if v is not None:
            killed_total += v
            has_killed_data = True

    if has_killed_data and killed_total > 0:
        return "fatal"
    if injured is not None and injured > 0:
        return "minor"
    if (injured == 0 or injured is None) and (has_killed_data and killed_total == 0):
        return "none"
    return "unknown"


def nyc_source_severity_raw(row: dict) -> str:
    """Audit-friendly string: killed=N,injured=N."""
    killed = row.get("number_of_persons_killed")
    injured = row.get("number_of_persons_injured")
    k = "0" if _is_missing(killed) else str(killed)
    i = "0" if _is_missing(injured) else str(injured)
    return f"killed={k},injured={i}"