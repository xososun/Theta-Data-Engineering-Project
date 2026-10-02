# Cross-City Road Crash Data Pipeline

DSS150P Data Engineering project. Full problem statement, objectives, and
architecture: see `docs/project_proposal.md`.

## Status

This repository currently contains the **design and profiling phase**
artifacts. The automated ingestion/transformation/orchestration pipeline
itself has not been built yet — see "What's not built yet" below before
assuming anything beyond what's listed as done.

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
- Raw → staging → curated transformation code implementing the adapter
  mappings (the YAML configs describe the mapping; nothing executes it yet).
- The 5+ automated data-quality checks as running code (the *rules* are
  documented in the adapters and schema; `dq_run_log` exists as a table with
  no writer yet).
- Airflow DAGs (`dags/` is currently an empty placeholder).
- Partitioning implementation (Parquet output, partition-pruned reads).
- Data contract document (distinct from the data dictionary).
- Architecture, data-flow/lineage, and ERD diagrams.
- Tests (`tests/` is an empty placeholder).
- Analytics/EDA/clustering notebooks.

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
- **`data/staging/`**, **`data/curated/`** — not yet populated (transformation
  code not yet built; see Member B's workstream in `TASKS.md`).

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
```

Batch folders are named like `chicago_us_20261002T092840Z_9b8ac00a`
(`<source_id>_<UTC timestamp>_<short id>`). Use `--help` on any extractor
for the full list of options.

## Repository Structure

```
project/
├── README.md                  # this file
├── STARTUP_GUIDE.md           # setup + how to run everything (start here)
├── Dockerfile                  # extends apache/airflow with this project's code/deps
├── docker-compose.yml          # Postgres + Airflow + pipeline runner (tested end-to-end)
├── .env.example                 # config template, no real secrets
├── .gitignore
├── requirements.txt
├── config/
│   ├── canonical_schema.yaml   # the shared schema every source maps into
│   └── adapters/
│       ├── chicago.yaml
│       ├── nyc.yaml
│       └── uk.yaml
├── docs/
│   ├── project_proposal.md     # problem statement, objectives, architecture
│   └── data_dictionary.md      # field-level reference incl. per-source gaps
├── sql/
│   └── schema.sql              # PostgreSQL DDL (auto-applied by Docker on first Postgres start)
├── src/
│   ├── extract/
│   │   ├── chicago_extractor.py  # bulk CSV → raw layer
│   │   ├── uk_extractor.py       # 5-year multi-file → raw layer, per-year failure isolation
│   │   └── nyc_extractor.py      # paginated API → raw layer, retries + backoff
│   ├── profiling/
│   │   └── profile_crashes.py   # Chicago source profiling script
│   ├── utils/
│   │   ├── batch.py              # batch ID generation
│   │   ├── raw_writer.py         # byte-for-byte raw-layer writer + manifest.json
│   │   └── config.py             # adapter YAML loader
│   ├── transform/                # empty — not yet built
│   ├── load/                     # empty — not yet built
│   └── validation/               # empty — not yet built
├── data/
│   ├── incoming/                 # LOCAL drop zone for downloaded source files (gitignored)
│   ├── raw/                     # pipeline-managed only (gitignored contents) — see "Data Layers" above
│   ├── staging/                  # empty — not yet populated
│   └── curated/                  # empty — not yet populated
├── dags/                         # empty — Airflow DAGs not yet written
├── notebooks/                    # empty — analytics not yet started
└── tests/
    └── test_nyc_extractor.py     # mocked-API tests for retry/error-handling logic
```

`src/profiling/` is a deviation from the course's suggested layout (which
lists `extract/transform/load/validation/utils` under `src/`) added to keep
one-off source-profiling scripts separate from pipeline-integrated
validation checks that will run inside the DAG. Documented here since the
guidelines ask for an explanation wherever the structure differs.

## Next Steps

Member A's workstream (ingestion + environment) is complete. The next
planned items are Member B's: build the staging transformation that
executes the adapter mappings, then the curated layer and automated
data-quality checks. Member C can start the DAG skeleton, diagrams, and
data contract in parallel. See `TASKS.md` for the full breakdown and
ownership.
