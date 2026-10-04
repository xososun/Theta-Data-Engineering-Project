# Cross-City Road Crash Data Pipeline

DSS150P Data Engineering project. Full problem statement, objectives, and
architecture: see `docs/project_proposal.md`.

## Status

This repository currently contains the **design and profiling phase**
artifacts, plus the **full transformation, validation, and load
pipeline** built by Member B. The automated ingestion/transformation/orchestration
pipeline itself has been built for ingestion (Member A) and
transformation/validation/load (Member B); Airflow DAGs, diagrams, and
analytics remain (Member C).

### What's actually done
- **Source profiling**, run against full or near-full real data for all
  three sources (not just samples): Chicago (1,096,581 rows), NYC (10,000-row
  API sample), UK STATS19 (513,801 rows, 2021–2025). Findings are captured in
  each adapter's `known_issues` section and in `docs/data_dictionary.md`.
- **Canonical schema** (`config/canonical_schema.yaml`) that all three
  sources map into.
- **Source adapter configs** for Chicago, NYC, and UK
  (`config/adapters/*.yaml`) — field mappings, value translations, and
  documented per-source data-quality issues and structural gaps.
- **PostgreSQL DDL** (`sql/schema.sql`) implementing the canonical schema as
  `dim_source` / `dim_date` / `fact_crash` / `dq_run_log`, with CHECK
  constraints enforcing the canonical enums. Applies cleanly on Postgres
  startup via the Docker init scripts (see "Docker environment" below);
  a manual `psql` check of the resulting tables is still worth doing.
- **Data dictionary** (`docs/data_dictionary.md`), including a per-field,
  per-source null/availability matrix so cross-source analysis doesn't
  accidentally misread a structural gap as a real finding.
- **Reusable profiling script** (`src/profiling/profile_crashes.py`) for the
  Chicago source — schema drift, missingness, date-format issues,
  coordinate sentinel/bounding-box checks, district coverage, date-logic
  validation.
- **Automated ingestion for all three sources** (`src/extract/`):
  - `chicago_extractor.py` — bulk CSV, byte-for-byte raw-layer copy, rejects
    missing/corrupt files with a clean error rather than a stack trace.
  - `uk_extractor.py` — loops over the 5 configured years independently;
    a missing or ambiguous year fails that year only (clearly reported,
    non-zero exit) without discarding the years that succeeded.
  - `nyc_extractor.py` — production API extractor (supersedes the earlier
    `pull_nyc_sample.py` profiling tool). Retries transient failures
    (timeouts, 5xx) with backoff, fails fast on permanent errors (4xx),
    treats 0 rows as a failure rather than a silent empty success, and
    uses the compound sort key fix found during NYC profiling. Verified
    with mocked-API unit tests (`tests/test_nyc_extractor.py`, 6 tests,
    all passing) since this sandbox has no live network access to test
    against the real endpoint.
  - Shared helpers in `src/utils/`: `batch.py` (collision-safe batch IDs),
    `raw_writer.py` (byte-for-byte file copies / raw JSON pages + a
    `manifest.json` per batch recording source, batch ID, and retrieval
    timestamp), `config.py` (adapter YAML loader).
  - **All three extractors verified end-to-end against real, full-scale
    data, through the actual Docker/Airflow image** (not just locally or
    mocked): Chicago (1,096,581 rows, real bulk CSV), UK (513,801 rows
    across all 5 real annual files), NYC (live Socrata API call, 1,793
    rows for a real 7-day window — confirms the compound-sort-key
    pagination fix and retry logic against the actual endpoint, not just
    the mocked tests in `tests/test_nyc_extractor.py`). Error-handling
    paths (missing file, malformed file, missing year, ambiguous
    filename) were verified separately against realistic bad inputs.
- **Staging transformation for all three sources** (`src/transform/`):
  - `staging.py` — config-driven raw→staging transform. Reads each
    adapter YAML's `field_mapping` and executes it. Mapping primitives:
    `from`, `literal`, `map`, `derive_fn` (named function from
    `derivations.py` for non-trivial per-source logic).
  - `derivations.py` — named helper functions for per-source derived
    fields (Chicago/NYC/UK `has_valid_coordinates`, NYC severity and
    vehicle counting from components, crash-id construction).
  - Sentinel `(0,0)` coordinates are nulled at staging time (74 Chicago
    rows, 34 NYC rows) rather than left as fake points.
  - All three adapters include `source_id` and `source_local_timezone`
    literals so those required canonical fields are populated.
  - Verified end-to-end: Chicago 1,096,581, UK 513,801, NYC 1,793 rows;
    raw→staged counts match exactly.
- **8 automated data-quality checks** (`src/validation/checks.py`):
  schema, nullability, uniqueness, accepted values, ranges, date logic,
  referential integrity, row counts. Every check writes a row to
  `dq_run_log`. Source-specific structural gaps are encoded so documented
  nulls do not register as failures. A single documented Chicago anomaly
  (1 row with inverted police-notification timestamp) is tolerated via a
  per-source tolerance. Verified: 22 pass, 2 warn, 0 fail across all
  three sources.
- **Curated harmonization + partitioned Parquet**
  (`src/transform/curated.py`): reads all three staging batches, casts
  columns to canonical types, parses per-source timestamp strings into
  UTC-aware datetimes, adds `crash_date_key` (FK to `dim_date`), writes
  partitioned Parquet at
  `data/curated/fact_crash/source_id=<id>/year=<YYYY>/month=<MM>/`.
  `read_partition(source_id, year, month)` uses pyarrow.dataset with a
  filter, demonstrating partition-pruned reads. Verified: 1,611,920 rows
  across 208 partition files; `read_partition("chicago_us", 2024, 3)`
  returns 8,924 rows without scanning the full dataset.
- **Postgres load with idempotent UPSERT** (`src/load/load_postgres.py`):
  populates `dim_date` (ON CONFLICT DO NOTHING), UPSERTs `fact_crash`
  on `(source_id, source_record_id)` using
  `psycopg2.extras.execute_values`. A `_to_pg_val` coercion catches all
  pandas missing-value flavours (`None`, `NaN`, `pd.NA`, `pd.NaT`)
  before send. Verified: 1,611,920 rows loaded; re-running leaves row
  counts unchanged.
- **CSV / JSON / Parquet benchmark** (`src/transform/format_compare.py`):
  writes the same curated slice as all three formats, measures size and
  read/write time, emits a Markdown report to
  `data/curated/format_benchmark/format_benchmark.md`. Parquet is
  smallest (2.48 MB) and fastest on write and read; CSV 2.0× larger;
  JSON 3.8× larger.
- **Unit tests** (`tests/test_staging.py`, `tests/test_validation.py`):
  8 tests for the staging mapping primitives and sentinel handling;
  12 tests for the DQ check logic including structural gap skipping and
  anomaly tolerance.
- **Docker environment**: `Dockerfile` (extends the official Airflow image
  with this project's code/dependencies, also usable standalone for manual
  extractor runs), `docker-compose.yml` (Postgres + Airflow webserver/
  scheduler + a `pipeline` one-off runner service), `.env.example`,
  `.gitignore`. **Fully tested and working**: `schema.sql` and a second
  init script (`sql/00_airflow_metadata.sql`, for Airflow's own metadata
  DB) both auto-apply cleanly on Postgres startup; Airflow webserver/
  scheduler run healthy; the `pipeline` runner service executes all three
  extractors successfully against real data. One real bug was found and
  fixed along the way: the `pipeline` service's `entrypoint: /bin/bash`
  (with no `-c`) caused bash to treat any command — `python`, `uname`,
  anything — as a script FILE to interpret rather than a program to run,
  failing with a misleading "cannot execute binary file" that initially
  looked like a Windows/WSL2/architecture issue but wasn't. Fixed by
  setting `entrypoint: []` so `docker compose run`'s command executes
  directly.

### What's NOT built yet
- Airflow DAGs (`dags/` is currently an empty placeholder — Member C's
  workstream).
- Data contract document (distinct from the data dictionary).
- Architecture, data-flow/lineage, and ERD diagrams.
- Analytics/EDA/clustering notebooks.
- `fact_person` and `fact_vehicle` tables (mentioned in the proposal;
  not implemented in v1; the canonical schema and DDL focus on
  `fact_crash`).

## Data Layers

- **`data/incoming/`** — a local drop zone, NOT a pipeline layer. Put
  downloaded source files here (the Chicago bulk CSV, UK annual CSVs)
  before running an extractor. Gitignored; never commit the actual
  datasets.
- **`data/raw/`** — pipeline-MANAGED output only. Extractors write here
  themselves, organized as `data/raw/<source_id>/<batch_id>/`, each with
  a `manifest.json` recording what was ingested and when. Never manually
  drop files in here, and never point an extractor's `--input` /
  `--input-dir` at this folder — doing so risks the UK extractor's
  filename matching picking up pipeline-generated files instead of
  genuine source files, and defeats the point of raw-layer traceability
  (raw/ is supposed to mean "an extractor produced this, with a
  manifest," not "a human put a file somewhere").
- **`data/staging/`** — pipeline-managed Parquet produced by
  `src/transform/staging.py`. One `fact_crash.parquet` per source per
  batch. Cleaned, typed, standardized to the canonical schema.
- **`data/curated/`** — pipeline-managed partitioned Parquet produced by
  `src/transform/curated.py`. Layout:
  `fact_crash/source_id=<id>/year=<YYYY>/month=<MM>/part-0.parquet`.
  Also contains `format_benchmark/` for the format comparison report.

## Running the Extractors

New to the project? Read `STARTUP_GUIDE.md` first for environment setup.

All commands run inside Docker through the `pipeline` service. Paths are
**container** paths (`/opt/airflow/...`), not host paths. Examples are single-line
commands so they work unchanged in PowerShell, bash, and cmd.

```bash
# Chicago: put the downloaded CSV in data/incoming/ on your host first
docker compose run --rm pipeline python src/extract/chicago_extractor.py --input /opt/airflow/data/incoming/Traffic_Crashes_-_Crashes_20260923.csv --raw-root /opt/airflow/data/raw

# UK: reads all 5 annual STATS19 files from the drop zone
docker compose run --rm pipeline python src/extract/uk_extractor.py --input-dir /opt/airflow/data/incoming --raw-root /opt/airflow/data/raw

# NYC: pulls a date window live from the Socrata API (no local file needed)
docker compose run --rm pipeline python src/extract/nyc_extractor.py --since 2025-09-01 --until 2025-09-07 --raw-root /opt/airflow/data/raw

# Inspect a batch's manifest (substitute your real batch folder name)
docker compose run --rm pipeline cat /opt/airflow/data/raw/chicago_us/<batch_id>/manifest.json