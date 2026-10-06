# Cross-City Road Crash Data Pipeline

DSS150P Data Engineering project (Team THETA). An end-to-end, Dockerized,
Airflow-orchestrated pipeline that ingests road-crash records from three
independent public sources (Chicago, New York City, and the UK's STATS19),
validates them, harmonizes them into one canonical schema, and publishes
the result as partitioned Parquet and a PostgreSQL star schema.

Full problem statement, objectives, stakeholders, and design rationale:
`docs/project_proposal.md`.

## Problem and Objectives

Road-crash data is published by each city or country in its own format,
with its own field names, severity scales, date conventions, and
missing-data codes. Comparing crash patterns across cities therefore
requires repeatable integration work, not a one-off analysis: the sources
update over time, and every refresh would otherwise mean redoing the same
cleaning by hand.

This project builds that integration once, as a pipeline that:

- ingests each source automatically and keeps a source-faithful raw copy;
- translates each source into one shared schema through per-source YAML
  adapters, so adding a city means adding a config file, not new code;
- enforces automated data-quality checks and records every result;
- produces a curated, cross-city crash table that analysts can query
  without knowing each source's quirks; and
- can be rerun safely without duplicating or corrupting data.

## Team and Roles

| Member  | Workstream |
|---------|---|
| Santos  | Ingestion (all three extractors), source profiling, canonical schema, adapters, PostgreSQL DDL, Docker environment |
| Hermoso | Staging transformation, data-quality checks, curated harmonization and partitioning, PostgreSQL load, format benchmark, unit tests |
| Leyte   | Airflow DAG, architecture / lineage / ERD diagrams, data contract, EDA and clustering notebooks |

## Data Sources

| Source | Provider | Format / retrieval | Scope | Raw rows | In `fact_crash` |
|---|---|---|---|---:|---:|
| `chicago_us` | City of Chicago Data Portal (Socrata) | Bulk CSV export | Full export as of 2026-09-23 | 1,096,581 | 1,096,395 |
| `nyc_us` | NYC Open Data (Socrata) | JSON via REST API, paginated | 90 days ending at the newest published date | Varies per run | 20,537 |
| `uk_stats19` | UK Department for Transport | Annual CSV files | 2021–2025, Collisions file only | 513,801 | 513,732 |
| **Total** | | | | | **1,630,664** |

The gap between raw and loaded rows is the daylight-saving exclusion
described under Known Limitations. NYC publishes with a lag of several
months, so its window ends at the newest date the API has, not today.

Provenance, licence, and known issues for each source are recorded in its
adapter (`config/adapters/*.yaml`) and in `docs/data_dictionary.md`. The
NYC source is the programmatically retrieved one.

## Architecture

```
Chicago CSV ─┐
NYC API ─────┼─► extract ─► data/raw/ ─► stage ─► data/staging/ ─► validate ─┐
UK CSVs ─────┘   (per source, in parallel)                    (writes dq_run_log)
                                                                              │
          ┌───────────────────────────────────────────────────────────────────┘
          ▼
      harmonize ─► data/curated/ (partitioned Parquet) ─► load ─► PostgreSQL ─► quality report
      (all sources at once)                                       (UPSERT)
```

Airflow schedules and monitors every stage; Docker Compose runs Postgres,
Airflow, and the pipeline code in one reproducible environment.

Full diagrams (Mermaid source in `docs/diagrams/`, rendered together in
`docs/diagrams.md`):

- **Architecture** (`architecture.mmd`): sources → ingestion → raw →
  staging → validation → harmonization → curated → Postgres + Parquet →
  consumption, with Airflow, Docker Compose, YAML config, and Git shown as
  orchestration / runtime / configuration / versioning layers.
- **Data flow / lineage** (`lineage.mmd`): each canonical `fact_crash`
  field traced back to its source column(s) in all three sources,
  including structural gaps as dashed edges and the `batch_id` → raw
  batch folder → `manifest.json` traceability chain.
- **ERD** (`erd.mmd`): derived from `sql/schema.sql`: `dim_source`,
  `dim_date`, `fact_crash`, `dq_run_log`, with foreign keys, the
  natural-key UNIQUE constraint, and CHECK constraints.

**Technology stack:** Python 3.11 (pandas, pyarrow, psycopg2, PyYAML),
PostgreSQL 16, Apache Airflow 2.9.3 (LocalExecutor), Docker / Docker
Compose, Git/GitHub.

## What's Built

### Ingestion (`src/extract/`)

- `chicago_extractor.py`: bulk CSV, byte-for-byte raw-layer copy; rejects
  missing or corrupt files with a clean error rather than a stack trace.
- `uk_extractor.py`: loops over the 5 configured years independently. A
  missing or ambiguous year fails that year only (clearly reported,
  non-zero exit) without discarding the years that succeeded.
- `nyc_extractor.py`: paginated REST API extractor. Retries transient
  failures (timeouts, 5xx) with backoff, fails fast on permanent errors
  (4xx), treats 0 rows as a failure rather than a silent empty success,
  and uses a compound sort key (`crash_date DESC, collision_id DESC`) to
  avoid duplicate rows at page boundaries.
- Shared helpers in `src/utils/`: `batch.py` (collision-safe batch IDs),
  `raw_writer.py` (raw-layer writes plus a `manifest.json` per batch
  recording source, batch ID, and retrieval timestamp), `config.py`
  (adapter YAML loader).
- Verified end-to-end through the Docker/Airflow image against real data:
  Chicago 1,096,581 rows, UK 513,801 rows (all 5 annual files), NYC live
  API pull (1,793 rows for a 7-day window). Error paths (missing file,
  malformed file, missing year, ambiguous filename) were verified
  separately against realistic bad inputs.

### Source profiling

- Run against full or near-full real data: Chicago (1,096,581 rows), NYC
  (10,000-row API sample), UK (513,801 rows). Findings are recorded in
  each adapter's `known_issues` and in `docs/data_dictionary.md`.
- `src/profiling/profile_crashes.py`: reusable profiling script for the
  Chicago source (schema drift, missingness, date formats, coordinate
  sentinels and bounding box, district coverage, date logic).

### Staging transformation (`src/transform/`)

- `staging.py`: config-driven raw → staging transform. Reads each
  adapter's `field_mapping` and executes it using four primitives:
  `from`, `literal`, `map`, and `derive_fn` (a named function in
  `derivations.py` for non-trivial per-source logic).
- `derivations.py`: per-source derived fields (crash-ID construction,
  `has_valid_coordinates`, NYC severity, killed and vehicle counts built
  from component fields).
- Sentinel `(0,0)` coordinates are nulled at staging (74 Chicago rows, 34
  NYC rows) rather than kept as fake points; UK `-1` "not recorded" codes
  are nulled on every field marked `apply_null_sentinel`.
- Verified: Chicago 1,096,581, UK 513,801, NYC 1,793 rows; raw → staged
  counts match exactly.

### Data-quality validation (`src/validation/checks.py`)

Eight automated checks run on every staged batch: schema, nullability,
uniqueness, accepted values, ranges, date logic, referential integrity,
and row counts. Each check writes one row to `dq_run_log` with its
status (`pass` / `warn` / `fail`), rows checked, rows failed, and
details. Any `fail` raises an exception, so the Airflow task fails
visibly and downstream stages do not run.

Documented structural gaps (for example, NYC has no posted speed limit)
are encoded per source so they do not register as failures. One
documented Chicago anomaly (a single row whose police-notification time
precedes the crash) is tolerated explicitly. Implausible speed limits are
flagged as `warn`, not rejected. Verified across all three sources: 22
pass, 2 warn, 0 fail.

### Curated layer and partitioning (`src/transform/curated.py`)

- Reads the newest staging batch for every source, parses each source's
  timestamp format, converts local time to UTC, casts columns to
  canonical types, and adds `crash_date_key` (foreign key to `dim_date`).
- Writes partitioned Parquet at
  `data/curated/fact_crash/source_id=<id>/year=<YYYY>/month=<MM>/`.
  Rerun-safe through replace semantics: the previous curated output is
  removed and rewritten, never appended to.
- **Partition key rationale:** `source_id` first, because most analyses
  compare or isolate cities; then `year` / `month`, because crash
  analysis is time-windowed (trends, seasonality) and monthly files stay
  a manageable size.
- `read_partition(source_id, year, month)` uses `pyarrow.dataset` with a
  filter, so only the matching partition is read. For example,
  `read_partition("chicago_us", 2024, 3)` returns 8,924 rows without
  scanning the full dataset.

### PostgreSQL storage (`sql/schema.sql`, `src/load/load_postgres.py`)

- Star schema: `fact_crash` with `dim_source` and `dim_date`, plus
  `dq_run_log`. CHECK constraints enforce canonical enums and
  non-negative counts.
- `load_postgres.py` populates `dim_date` (`ON CONFLICT DO NOTHING`) and
  UPSERTs `fact_crash` on `(source_id, source_record_id)` with
  `psycopg2.extras.execute_values`. Verified: 1,630,664 rows loaded across 211 partitions,
  and a second full DAG run left every per-source count unchanged.

### CSV / JSON / Parquet comparison (`src/transform/format_compare.py`)

Writes the same curated slice in all three formats, measures file size
and read/write time, and emits a Markdown report to
`data/curated/format_benchmark/format_benchmark.md`. Result: Parquet is
smallest (2.48 MB) and fastest to write and read; CSV is 2.0× larger and
JSON 3.8× larger. CSV is still used where it fits (source exports), JSON
where the source speaks it (the NYC API), and Parquet for everything the
pipeline produces, because it preserves column types and supports
partition pruning.

### Orchestration (`dags/crash_pipeline.py`)

One DAG, `crash_pipeline`:

- **Per source, in parallel** (dynamic task mapping over `chicago_us`,
  `nyc_us`, `uk_stats19`): `extract → stage → validate`.
- **Once, after all three sources succeed:** `harmonize → load`.
- **Always last:** `quality_report` (trigger rule `ALL_DONE`), which
  summarizes this run's `dq_run_log` rows into
  `data/quality_reports/quality_report_<timestamp>.md` even when an
  earlier stage failed. Airflow judges a run by its final task, so
  `quality_report` then fails itself if any upstream task failed;
  otherwise a run with a failed validation would show as green.
- Weekly schedule (`0 6 * * 1`), `catchup=False`, `max_active_runs=1`.
  Tasks retry twice with a 2-minute delay, except `validate` and
  `quality_report`: a failed data-quality check is deterministic, so
  retrying would only fail again and duplicate its `dq_run_log` rows.

`harmonize` deliberately keeps the default `all_success` trigger rule.
Staging output is written before validation runs, so a batch that failed
its checks still sits in `data/staging/` as the newest batch; letting
harmonize run anyway would publish data that failed validation.

The DAG contains no transformation logic: each task calls a function from
`src/`, so every stage can also be run by hand (see "Running Stages
Manually").

### Documentation

- `docs/data_dictionary.md`: field-level reference for `fact_crash`,
  including a per-field, per-source availability matrix so a structural
  gap is not misread as a real finding.
- `docs/data_contract.md`: producer guarantees (row-level rules, rerun
  safety, raw-layer traceability, layer boundaries), structural gaps
  consumers must account for, per-source quirks, and explicit
  non-guarantees.
- `docs/diagrams.md` and `docs/diagrams/`: the three diagrams above.

### Tests (`tests/`)

- `test_nyc_extractor.py`: 6 mocked-API tests for retry and error
  handling.
- `test_staging.py`: 8 tests for mapping primitives and sentinel handling.
- `test_validation.py`: 12 tests for check logic, structural-gap skipping,
  and anomaly tolerance.
- `test_dag_imports.py`: structural tests for the DAG (imports
  cleanly; expected tasks, mapping, fan-in dependencies, trigger rules,
  retries, schedule). Needs Airflow, so run it inside the container.

### Analytics (bonus)

`notebooks/eda.ipynb` and `notebooks/clustering.ipynb` run against the
curated Parquet output. Analytics is supplementary; the production
pipeline lives entirely in `src/` and `dags/`.

## Data Layers

- **`data/incoming/`**: a local drop zone, NOT a pipeline layer. Put
  downloaded source files here (the Chicago bulk CSV, UK annual CSVs)
  before running. Gitignored; never commit the datasets.
- **`data/raw/`**: pipeline-managed only. Extractors write here as
  `data/raw/<source_id>/<batch_id>/`, each batch with a `manifest.json`
  recording what was ingested and when. Never drop files here by hand,
  and never point an extractor's `--input` / `--input-dir` at this
  folder; raw/ means "an extractor produced this, with a manifest".
- **`data/staging/`**: one `fact_crash.parquet` per source per batch,
  produced by `staging.py`. Mapped to the canonical schema, sentinels
  nulled; timestamps still in source-local string form.
- **`data/curated/`**: partitioned Parquet produced by `curated.py`
  (`fact_crash/source_id=<id>/year=<YYYY>/month=<MM>/`), plus
  `format_benchmark/`.

## Repository Structure

```
.
├── README.md
├── STARTUP_GUIDE.md            # first-time setup, start here if new
├── TASKS.md                    # task breakdown and ownership
├── Dockerfile                  # extends apache/airflow with project code/deps
├── docker-compose.yml          # Postgres + Airflow + one-off pipeline runner
├── .env.example                # configuration template, no real secrets
├── .gitignore
├── requirements.txt
├── config/
│   ├── canonical_schema.yaml   # the shared schema every source maps into
│   └── adapters/               # one YAML per source: chicago, nyc, uk
├── dags/
│   └── crash_pipeline.py       # the Airflow DAG
├── data/                       # incoming/, raw/, staging/, curated/ (contents gitignored)
├── docs/
│   ├── project_proposal.md
│   ├── data_dictionary.md
│   ├── data_contract.md
│   ├── diagrams.md
│   └── diagrams/               # architecture.mmd, lineage.mmd, erd.mmd
├── notebooks/                  # eda.ipynb, clustering.ipynb (bonus analytics)
├── sql/
│   ├── 00_airflow_metadata.sql # creates Airflow's metadata DB on first start
│   └── schema.sql              # project DDL, auto-applied on first start
├── src/
│   ├── extract/                # chicago, nyc, uk extractors
│   ├── transform/              # staging, derivations, curated, format_compare
│   ├── validation/             # checks.py, quality_report.py
│   ├── load/                   # load_postgres.py
│   ├── profiling/              # profile_crashes.py
│   └── utils/                  # batch, config, db, paths, raw_writer
└── tests/
```

`src/profiling/` is an addition to the course's suggested layout. It
keeps one-off source-profiling scripts separate from the validation
checks that run inside the DAG.

## Setup

Prerequisites: Docker Desktop (on Windows, with the WSL2 backend) and
Git. Python 3.10+ is needed only to run things outside Docker.

```bash
git clone <repo-url> && cd <repo-folder>
cp .env.example .env
# Generate a Fernet key and paste it into AIRFLOW_FERNET_KEY in .env:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Replace the changeme_* passwords in .env (POSTGRES_PASSWORD, AIRFLOW_ADMIN_PASSWORD)
docker compose up -d --build
```

Then put the source files in `data/incoming/`:

- Chicago: `Traffic_Crashes_-_Crashes_20260923.csv` (or set `CHICAGO_CSV`).
- UK: the five STATS19 annual Collision files, 2021–2025.
- NYC: nothing; it is pulled live from the API.

See `STARTUP_GUIDE.md` for a step-by-step walkthrough.

### Configuration

All configuration is externalized to `.env` (template: `.env.example`).
Never commit `.env`.

| Variable | Purpose |
|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT` | Project database |
| `AIRFLOW_DB` | Airflow's own metadata database (same Postgres instance, separate DB) |
| `AIRFLOW_FERNET_KEY` | Airflow encryption key |
| `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_EMAIL` | Airflow UI login |
| `AIRFLOW_WEBSERVER_PORT` | Airflow UI port (default 8080) |

Docker Compose derives the `PIPELINE_POSTGRES_*` and `PIPELINE_*_DATA_ROOT`
variables that the pipeline code reads, so the code has no hard-coded
hosts, credentials, or machine-specific paths. Non-secret source settings
(endpoints, bounding boxes, mappings) live in `config/adapters/`.

### Starting and stopping

```bash
docker compose up -d --build   # start Postgres, Airflow webserver + scheduler
docker compose ps              # check health
docker compose down            # stop (data and database kept)
docker compose down -v         # stop AND wipe the database volume (forces schema re-init)
```

### PostgreSQL initialization

`sql/00_airflow_metadata.sql` and `sql/schema.sql` are mounted into
Postgres's init directory and run automatically on the **first** start
with an empty volume. To re-apply them, run `docker compose down -v`, then
`up` again. To inspect the database:

```bash
docker compose exec postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB>
```

## Running the Pipeline

### With Airflow (normal operation)

Open `http://localhost:8080`, log in with the admin credentials from
`.env`, and unpause or trigger `crash_pipeline`. From the command line:

```bash
docker compose exec airflow-scheduler airflow dags trigger crash_pipeline
```

Each task's log (Airflow UI → task → Logs) shows its stage prefix
(`[staging]`, `[checks]`, `[curated]`, `[load]`), row counts, and the full
exception on failure. A failed validation lists each failing check and
its details in the log and in `dq_run_log`.

### Running stages manually

Every stage can also be run outside Airflow through the `pipeline`
service. Paths are **container** paths (`/opt/airflow/...`). The commands
are single-line, so they work unchanged in PowerShell, bash, and cmd.

```bash
# 1. Extract (writes data/raw/<source_id>/<batch_id>/)
docker compose run --rm pipeline python src/extract/chicago_extractor.py --input /opt/airflow/data/incoming/Traffic_Crashes_-_Crashes_20260923.csv --raw-root /opt/airflow/data/raw
docker compose run --rm pipeline python src/extract/uk_extractor.py --input-dir /opt/airflow/data/incoming --raw-root /opt/airflow/data/raw
docker compose run --rm pipeline python src/extract/nyc_extractor.py --since 2025-09-01 --until 2025-09-07 --raw-root /opt/airflow/data/raw

# 2. Stage and validate one batch (batch folder name from step 1)
docker compose run --rm pipeline python src/transform/staging.py --source chicago_us --batch <batch_id>
docker compose run --rm pipeline python src/validation/checks.py --source chicago_us --batch <batch_id>

# 3. Harmonize all sources into partitioned curated Parquet
docker compose run --rm pipeline python src/transform/curated.py

# 4. Load into PostgreSQL (add --source <id> for one source, --dry-run to skip writes)
docker compose run --rm pipeline python src/load/load_postgres.py

# Partition-pruned read demo
docker compose run --rm pipeline python src/transform/curated.py --read-source chicago_us --read-year 2024 --read-month 3

# Data-quality report (default: checks from the last 24 hours)
docker compose run --rm pipeline python src/validation/quality_report.py

# Format benchmark
docker compose run --rm pipeline python src/transform/format_compare.py --source chicago_us --year 2024 --month 3

# Inspect a raw batch's manifest
docker compose run --rm pipeline cat /opt/airflow/data/raw/chicago_us/<batch_id>/manifest.json

# Tests
docker compose run --rm pipeline python -m pytest tests -q
```

Batch folders are named `<source_id>_<UTC timestamp>_<short id>`, for
example `chicago_us_20261002T092840Z_9b8ac00a`. Use `--help` on any script
for the full option list.

### Representative queries

```sql
-- Crashes and fatal crashes per source per year
SELECT f.source_id, d.year,
       COUNT(*) AS crashes,
       COUNT(*) FILTER (WHERE f.severity = 'fatal') AS fatal_crashes
FROM fact_crash f
JOIN dim_date d ON d.date_key = f.crash_date_key
GROUP BY f.source_id, d.year
ORDER BY f.source_id, d.year;

-- Latest data-quality results
SELECT source_id, check_name, status, rows_failed, details
FROM dq_run_log
ORDER BY run_timestamp DESC
LIMIT 24;
```

## Rerun Safety

Each layer has its own strategy, so rerunning the DAG or any single stage
never duplicates data:

| Layer | Strategy |
|---|---|
| Raw | Every run writes a new batch folder with a unique batch ID; batches are never overwritten, which preserves history and lineage. |
| Staging | Written per `(source_id, batch_id)`; rerunning a batch overwrites that batch's file. |
| Curated | Replace semantics: the curated output is removed and rewritten from the newest batch per source. |
| PostgreSQL | UPSERT on `(source_id, source_record_id)`; `dim_date` uses `ON CONFLICT DO NOTHING`. |

## Expected Outputs

| Output | Location |
|---|---|
| Raw batches + manifests | `data/raw/<source_id>/<batch_id>/` |
| Staged batches | `data/staging/<source_id>/<batch_id>/fact_crash.parquet` |
| Curated partitioned dataset | `data/curated/fact_crash/source_id=*/year=*/month=*/` |
| Format benchmark report | `data/curated/format_benchmark/format_benchmark.md` |
| Curated tables | PostgreSQL: `fact_crash`, `dim_source`, `dim_date` |
| Validation results | PostgreSQL: `dq_run_log` |
| Data-quality reports | `data/quality_reports/quality_report_<timestamp>.md` |

## Known Limitations and Assumptions

- **Uneven field coverage across sources.** NYC has no categorical
  severity (it can never be `serious`), crash type, weather, lighting, or
  speed limit. UK has no `num_killed`, crash type, or primary cause in the
  Collision file. Gaps are left null, never approximated; see the
  availability matrix in `docs/data_dictionary.md`.
- **`hit_and_run`** is a real signal only for Chicago; NYC and UK default
  to `false`.
- **UK is national, not a city.** `city` is null for `uk_stats19`, so
  city-level comparisons must exclude it or use a different granularity.
- **Chicago 2013–2017** is partial coverage (districts were onboarded
  gradually); citywide trends are most comparable from 2018 onward.
- **NYC is a rolling window** in scheduled runs (90 days), not the full
  history.
- **Out of scope for v1:** `fact_person`, `fact_vehicle`, and
  `dim_location`, which were mentioned in the proposal draft.
- **Daylight-saving exclusion.** Crashes timestamped in a DST transition
  hour cannot be converted to UTC unambiguously (in autumn 1:00–1:59 am
  happens twice; in spring 2:00–2:59 am does not exist), so harmonization
  excludes them: 186 Chicago and 69 UK rows in the current load. The
  harmonize task log reports the count each run.
- **Curated Parquet holds the latest batch only.** It is rebuilt each run,
  while `fact_crash` accumulates. For NYC, Parquet has the newest 90-day
  window and Postgres has every window ever loaded.
- **No row-level quarantine.** A failing data-quality check blocks the
  whole run rather than removing individual rows; rows flagged `warn`
  are loaded.
- **`source_row_raw_ref` is not populated.** Rows are traced to their raw
  batch through `batch_id` instead.
- One Postgres container hosts both the project database and Airflow's
  metadata database (separate databases), a deliberate simplification
  for a course-scale project.
- `crash_date_key` and `dim_date` use the UTC date, so evening crashes in 
  Chicago and NYC fall on the next day. Local-time analysis converts from 
  `crash_timestamp_utc`.

## Troubleshooting

- **`cannot execute binary file` from `docker compose run pipeline ...`.**
  The `pipeline` service must keep `entrypoint: []`. A bare
  `entrypoint: /bin/bash` makes bash treat `python` as a script file.
- **Tables missing in Postgres.** The init scripts only run on an empty
  volume. Run `docker compose down -v`, then `up`.
- **Chicago extract fails with "source file not found".** The CSV is not
  in `data/incoming/`, or the file name differs (set `CHICAGO_CSV`).
- **`No adapter ... has source_id=...`.** The adapter YAML's top-level
  `source_id` must match the source ID used by the DAG.
- **`harmonize` shows `upstream_failed`.** At least one source failed
  validation. Check that source's `validate` task log, the newest file in
  `data/quality_reports/`, or `dq_run_log`.
- **Host paths in commands.** Inside containers, always use
  `/opt/airflow/...` paths, not Windows or host paths.

## Future Improvements

- Retrieve Chicago through its Socrata API instead of a manual bulk
  download, making all three sources fully automated.
- Add the STATS19 Casualties and contributory-factors tables as sources
  to fill the UK gaps (`num_killed`, `primary_cause`).
- Have `curated.py` read date formats and time zones from the adapter
  YAMLs instead of its own lookup table, so a new source needs config
  changes only.
- Make harmonization incremental (rewrite only the affected partitions)
  rather than rebuilding the whole curated layer.
- Resolve DST-ambiguous timestamps (assume the first occurrence, shift
  nonexistent times forward) instead of excluding them.
- Populate `source_row_raw_ref` so each row points to its exact raw line.
- Implement `fact_person` and `fact_vehicle`.
