"""
comparison_frames.py

One shared definition of "comparing like with like", used by both
notebooks so the EDA and the clustering compare the same things.

A cross-source comparison is only made inside a COMPARISON FRAME:

1. Same kind of place. City-scale geographies only:
     Chicago        chicago_us  (city limits; the dataset's own scope)
     New York City  nyc_us      (five boroughs; the dataset's own scope)
     London         uk_stats19  rows with city = "London", i.e. Greater
                                London (Metropolitan + City of London
                                police areas). The rest of Great Britain
                                is never compared with a city.

2. Same kind of event. Injury crashes only (severity fatal / serious /
   minor). STATS19 records only collisions in which someone was injured;
   Chicago and NYC also record damage-only crashes. Comparing all
   records would compare different reporting rules, not different roads.

3. Same period. Each frame uses the dates BOTH (or all) cities cover,
   computed from the data, so it stays correct when the data changes:
   with the current load, Chicago vs London overlap 2021-2025, Chicago vs
   NYC overlap only inside NYC's 90-day window, and NYC and London do not
   overlap at all (NYC 2026, UK up to 2025), so no three-way comparison
   is made. A NYC backfill (DAG run conf nyc_since/nyc_until) would make
   a three-way frame appear automatically.

4. An exposure denominator. Counts are turned into rates per 100,000
   residents per year. Residents are a coarse measure of exposure (they
   ignore commuters, visitors and how far people travel), but they are
   available for all three cities on a consistent basis; vehicle-miles
   travelled would be better and is listed as future work.
"""

from __future__ import annotations

import pandas as pd

CITY_OF_SOURCE = {"chicago_us": "Chicago", "nyc_us": "New York City", "uk_stats19": "London"}

# Census counts: US Census Bureau 2020 Census (QuickFacts, April 1 2020);
# ONS Census 2021 for the London region (Greater London).
POPULATION = {"Chicago": 2_746_388, "New York City": 8_804_190, "London": 8_799_800}
POPULATION_NOTE = "residents: 2020 US Census (Chicago, NYC); 2021 ONS Census (Greater London)"

# Land area for density-relative hotspot thresholds. US Census QuickFacts
# 2020 land area (227.73 and 300.45 sq mi); Greater London total area.
AREA_KM2 = {"Chicago": 589.8, "New York City": 778.2, "London": 1572.0}

INJURY_SEVERITIES = ("fatal", "serious", "minor")


def add_local_time(df: pd.DataFrame) -> pd.DataFrame:
    """Add local_date, local_hour, local_dow (0=Mon), local_year, is_weekend.

    Each source is converted to its own timezone. crash_date_key is the
    UTC date (an evening crash in Chicago is already 'tomorrow' in UTC),
    so every calendar comparison uses local time instead.
    """
    parts = []
    for tz, g in df.groupby("source_local_timezone"):
        local = g["crash_timestamp_utc"].dt.tz_convert(tz)
        parts.append(pd.DataFrame({
            "local_date": local.dt.date,
            "local_hour": local.dt.hour,
            "local_dow": local.dt.dayofweek,
            "local_year": local.dt.year,
        }, index=g.index))
    out = df.join(pd.concat(parts))
    out["is_weekend"] = out["local_dow"] >= 5
    return out


def comparable_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Rows that may enter a cross-source comparison: the source's city
    geography and injury crashes only. Adds a `city_area` column."""
    expected_city = df["source_id"].map(CITY_OF_SOURCE)
    keep = (df["city"] == expected_city) & df["severity"].isin(INJURY_SEVERITIES)
    out = df[keep].copy()
    out["city_area"] = out["source_id"].map(CITY_OF_SOURCE)
    return out


def coverage(comparable: pd.DataFrame) -> pd.DataFrame:
    """First and last local date each city covers in the comparable rows."""
    return (comparable.groupby("city_area")["local_date"]
            .agg(first="min", last="max", injury_crashes="size"))


def overlap(cov: pd.DataFrame, cities: list[str]):
    """The (start, end) both/all cities cover, or None if they don't overlap."""
    if not set(cities) <= set(cov.index):
        return None
    start, end = cov.loc[cities, "first"].max(), cov.loc[cities, "last"].min()
    return (start, end) if start <= end else None


def build_frames(comparable: pd.DataFrame) -> list[dict]:
    """
    The comparison frames the data supports, each with a shared window:
      Chicago vs London, Chicago vs New York City, and all three cities
      (only if their windows overlap).
    """
    cov = coverage(comparable)
    candidates = [
        ("Chicago vs London", ["Chicago", "London"]),
        ("Chicago vs New York City", ["Chicago", "New York City"]),
        ("All three cities", ["Chicago", "New York City", "London"]),
    ]
    frames = []
    for name, cities in candidates:
        window = overlap(cov, cities)
        frames.append({
            "frame": name, "cities": cities,
            "start": window[0] if window else None,
            "end": window[1] if window else None,
            "days": (window[1] - window[0]).days + 1 if window else 0,
        })
    return frames


def in_frame(comparable: pd.DataFrame, frame: dict) -> pd.DataFrame:
    """Comparable rows for this frame's cities inside its shared window."""
    if not frame["days"]:
        return comparable.iloc[0:0]
    return comparable[comparable["city_area"].isin(frame["cities"])
                      & (comparable["local_date"] >= frame["start"])
                      & (comparable["local_date"] <= frame["end"])]


def rates(rows: pd.DataFrame, frame: dict) -> pd.DataFrame:
    """Injury and fatal crashes per 100,000 residents per year, per city."""
    years = frame["days"] / 365.25
    out = rows.groupby("city_area").agg(
        injury_crashes=("severity", "size"),
        fatal_crashes=("severity", lambda s: int((s == "fatal").sum())),
    ).reindex(frame["cities"]).fillna(0).astype(int)
    out["residents"] = [POPULATION[c] for c in out.index]
    out["injury_per_100k_per_year"] = out["injury_crashes"] / years / out["residents"] * 1e5
    out["fatal_per_100k_per_year"] = out["fatal_crashes"] / years / out["residents"] * 1e5
    out["fatal_share_of_injury_pct"] = out["fatal_crashes"] / out["injury_crashes"].where(out["injury_crashes"] > 0) * 100
    out.insert(0, "window", f"{frame['start']} to {frame['end']} ({frame['days']} days)")
    return out.round(3)
