# Data Dictionary: `fact_crash` (Curated Layer)

Covers the canonical, cross-source crash table defined in `canonical_schema.yaml`
and implemented in `sql/schema.sql`. Source-specific raw fields are documented
separately in each adapter file (`adapters/chicago.yaml`, `adapters/nyc.yaml`,
`adapters/uk.yaml`); this dictionary covers only the curated, harmonized output.

**Sources:** `chicago_us` (City of Chicago, full history), `nyc_us` (NYC OpenData,
last 5 years planned), `uk_stats19` (UK DfT, 2021–2025, 5 published years).

## Field Reference

| Field | Type | Nullable | Description | Chicago | NYC | UK |
|---|---|---|---|---|---|---|
| `crash_id` | TEXT (PK) | No | `{source_id}:{source_record_id}`. Globally unique surrogate key. | ✅ | ✅ | ✅ |
| `source_id` | TEXT (FK) | No | Which source adapter produced this row: `chicago_us`, `nyc_us`, or `uk_stats19`. | ✅ | ✅ | ✅ |
| `source_record_id` | TEXT | No | Original natural key from the source, kept verbatim for traceability. | ✅ | ✅ | ✅ |
| `crash_timestamp_utc` | TIMESTAMPTZ | No | Crash date/time normalized to UTC from the source's local timezone. | ✅ | ✅ | ✅ |
| `crash_date_key` | DATE (FK → `dim_date`) | No | Date portion of `crash_timestamp_utc`, for date-dimension joins. | ✅ | ✅ | ✅ |
| `police_notified_timestamp_utc` | TIMESTAMPTZ | Yes | When police were notified, UTC. | ✅ | ❌ no equivalent field | ❌ no equivalent field |
| `city` | TEXT | Yes | City name. Null for national-scope sources. | ✅ "Chicago" | ✅ "New York City" | ❌ national dataset, not city-scoped |
| `country` | CHAR(2) | No | ISO 3166-1 alpha-2. | ✅ "US" | ✅ "US" | ✅ "GB" |
| `latitude` / `longitude` | DOUBLE PRECISION | Yes | Coordinates. Null if source had none, or a `(0,0)` sentinel was rejected. | ⚠ 0.78% missing | ⚠ 0.7% missing, but 2.3% of present coords are sentinel `(0,0)` | ✅ ~0.01% missing, 0 sentinel rows |
| `has_valid_coordinates` | BOOLEAN | No | True only if lat/lon are non-null, non-sentinel, and inside the source's bounding box. | ✅ derived | ✅ derived | ✅ derived |
| `severity` | TEXT, enum (`fatal`/`serious`/`minor`/`none`/`unknown`) | No | Canonical severity. | ✅ full 5-value range from `MOST_SEVERE_INJURY` | ⚠ only `fatal`/`minor`/`none` — **cannot produce `serious`**, no categorical field in source | ✅ full range from `collision_severity` codes 1/2/3 |
| `source_severity_raw` | TEXT | Yes | Original source value or derivation string, for audit. | ✅ | ✅ | ✅ |
| `num_injured_total` | INTEGER | Yes | Total injured. | ✅ `INJURIES_TOTAL` | ✅ `number_of_persons_injured` | ✅ `number_of_casualties` |
| `num_killed` | INTEGER | Yes | Total killed. | ✅ `INJURIES_FATAL` | ✅ derived: sum of pedestrian+cyclist+motorist killed (aggregate field is 88% null) | ❌ null — casualty-level fatality detail lives in a separate STATS19 table not included in this pipeline |
| `num_vehicles_involved` | INTEGER | Yes | Vehicles involved. | ✅ `NUM_UNITS` | ⚠ approximated: count of non-null vehicle-type slots (undercounts if a vehicle has no type recorded) | ✅ `number_of_vehicles` |
| `crash_type` | TEXT | Yes | Physical crash type (rear-end, angle, sideswipe, etc.). | ✅ `FIRST_CRASH_TYPE` | ❌ not collected by source | ❌ not collected in this file |
| `primary_cause` | TEXT | Yes | Primary contributory cause. | ✅ `PRIM_CONTRIBUTORY_CAUSE` | ✅ `contributing_factor_vehicle_1` | ❌ contributory-factor data lives in a separate, unjoined STATS19 table |
| `weather_condition` | TEXT | Yes | Weather at time of crash. | ✅ | ❌ not collected by source | ✅, with `-1` sentinel translated to null |
| `lighting_condition` | TEXT | Yes | Lighting at time of crash. | ✅ | ❌ not collected by source | ✅, with `-1` sentinel translated to null |
| `posted_speed_limit_mph` | INTEGER | Yes | Posted speed limit, mph. | ⚠ present but includes implausible outliers (observed range 0–60) | ❌ not collected by source | ✅, with `-1` sentinel translated to null; clean discrete set {20,30,40,50,60,70} otherwise |
| `hit_and_run` | BOOLEAN | No | Whether the crash was a hit-and-run. | ✅ reliable — explicit `HIT_AND_RUN_I` source flag | ❌ always `false` — no reliable source signal | ❌ always `false` — no source field |
| `ingested_at_utc` | TIMESTAMPTZ | No | When this row was loaded by the pipeline. | ✅ | ✅ | ✅ |
| `batch_id` | TEXT | No | Pipeline run identifier, for rerun-safety and `dq_run_log` joins. | ✅ | ✅ | ✅ |
| `source_row_raw_ref` | TEXT | Yes | Pointer back to the raw-layer file/row, for lineage. | ✅ | ✅ | ✅ |

**Legend:** ✅ reliably populated · ⚠ populated with a caveat (see note) · ❌ structurally null for this source (not a bug)

## Cross-Source Gaps at a Glance

These are the fields where at least one source cannot populate real data — worth knowing before writing any cross-source analysis, since a `GROUP BY source_id` on these fields will look like an uneven pipeline rather than a documented source limitation:

- **`severity` "serious" bucket** — NYC can never produce this value. Any severity breakdown by source will show NYC missing a category, not zero crashes of that severity.
- **`crash_type`** — null for NYC and UK; only Chicago can support "what kind of collision" analysis.
- **`primary_cause`** — null for UK only.
- **`weather_condition` / `lighting_condition` / `posted_speed_limit_mph`** — null for NYC only.
- **`num_killed`** — null for UK only.
- **`hit_and_run`** — meaningful only for Chicago; always `false` elsewhere, so a "hit-and-run rate by city" query would misleadingly show 0% for NYC and UK.
- **`city`** — null for UK, since it's a national, not city-scoped, dataset.

## Known Per-Source Data-Quality Notes

Full detail lives in each adapter's `known_issues` section; summarized here for quick reference.

| Issue | Source(s) | Handling |
|---|---|---|
| Sentinel `(0,0)` coordinates | Chicago (0.007%), NYC (2.3% of coordinate-bearing rows) | Treated as missing, not a real location, via `has_valid_coordinates` |
| `-1` used as "not recorded" across many coded fields, including `speed_limit` | UK only | Translated to null before any validation or use |
| Mixed date field order (`DD/MM/YYYY` vs `MM/DD/YYYY`) across sources | UK vs. Chicago/NYC | Each adapter sets an explicit date format; never inferred or defaulted |
| Aggregate injury/fatality field far less complete than its components | NYC | `num_killed` derived from component sums, not the aggregate field |
| Pagination duplicate risk on high-volume, date-tied sources | NYC (API) | Compound sort key (`crash_date DESC, collision_id DESC`) required for stable paging |
| Partial pre-2017 coverage by police district | Chicago | Recommend windowing trend analysis from 2018 onward for citywide comparability |
| Implausible speed-limit outliers (e.g. 0, 3 mph) | Chicago | Flagged by validation (`flag_if_outside [5,70]`), not rejected — may be legitimate signage |

## Change Log

| Date | Change |
|---|---|
| Initial | Schema defined from full-scale profiling of Chicago (1,096,581 rows), NYC (10,000-row API sample), and UK (513,801-row, 5-year file) |
