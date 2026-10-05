# Startup Guide — Cross-City Road Crash Data Pipeline

For Members B and C picking up from Member A's work (ingestion + environment). Read this first, then `README.md` and `TASKS.md`.

## 1. What's already working

- All three extractors (Chicago, UK, NYC) run end-to-end against real data inside the Docker/Airflow image.
- The raw layer is written by the pipeline itself, with a `manifest.json` per batch.
- Postgres starts with `sql/schema.sql` and `sql/00_airflow_metadata.sql` applied automatically.
- Airflow webserver and scheduler run healthy.

**Not built yet:** staging/curated transformation, DQ checks, DAGs, Parquet partitioning, data contract, diagrams, tests beyond NYC, notebooks. See `TASKS.md` for who owns what.

## 2. Prerequisites

- Docker Desktop (on Windows: with the WSL2 backend)
- Git
- Python 3.10+ only if you want to run things outside Docker (`pip install -r requirements.txt`)

## 3. First-time setup

```bash
# 1. Clone and enter the repo
git clone <repo-url> && cd <repo-folder>

# 2. Create your local env file (never commit .env)
cp .env.example .env

# 3. Generate a Fernet key and paste it into AIRFLOW_FERNET_KEY in .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 4. Replace the changeme_* passwords in .env
#    (POSTGRES_PASSWORD, AIRFLOW_ADMIN_PASSWORD)

# 5. Build and start the stack
docker compose up -d --build
```

Then check:

- Airflow UI: `http://localhost:8080` (login with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from your `.env`)
- Postgres: port `5432` by default (`POSTGRES_PORT`)

**.env variables at a glance**

| Variable | Purpose |
|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT` | Curated crash-data database |
| `AIRFLOW_DB` | Airflow's own metadata DB. Must differ from `POSTGRES_DB`, though both live in the same Postgres container |
| `AIRFLOW_FERNET_KEY` | Must be generated; do not leave the placeholder |
| `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_EMAIL` | Airflow login |
| `AIRFLOW_WEBSERVER_PORT` | Defaults to 8080 |
| `NYC_SOCRATA_APP_TOKEN` | Optional; raises the NYC API rate limit |

## 4. Get the source data

Datasets are **not** in the repo (gitignored). Each person downloads them locally.

- **Chicago:** the bulk "Traffic Crashes - Crashes" CSV
- **UK STATS19:** the 5 annual files, 2021–2025 (Collisions only)
- **NYC:** pulled live from the Socrata API, so no download is needed

Put downloaded files in `data/incoming/` (host machine). This is the local drop zone.

## 5. Run the extractors

All commands run through the one-off `pipeline` service. They are written as **single-line commands** so they work unchanged in PowerShell, bash, or cmd (don't use `\` line continuation in PowerShell; it uses a backtick).

Paths are **container** paths (`/opt/airflow/...`), not Windows/host paths.

```bash
# Chicago (use the file name you actually downloaded)
docker compose run --rm pipeline python src/extract/chicago_extractor.py --input /opt/airflow/data/incoming/Traffic_Crashes_-_Crashes_20260923.csv --raw-root /opt/airflow/data/raw

# UK: reads the 5 annual files from the drop zone
docker compose run --rm pipeline python src/extract/uk_extractor.py --input-dir /opt/airflow/data/incoming --raw-root /opt/airflow/data/raw

# NYC: live API pull for a date window
docker compose run --rm pipeline python src/extract/nyc_extractor.py --since 2025-09-01 --until 2025-09-07 --raw-root /opt/airflow/data/raw
```

Use `--help` on any extractor to see all options.

Each successful run creates `data/raw/<source_id>/<batch_id>/` containing the raw data and a `manifest.json` (source, batch ID, retrieval timestamp). Batch folders look like `chicago_us_20261002T092840Z_9b8ac00a`.

**Check a batch's manifest** (replace with your real batch folder):

```bash
docker compose run --rm pipeline cat /opt/airflow/data/raw/chicago_us/chicago_us_20261002T092840Z_9b8ac00a/manifest.json
```

Quick sanity numbers from the real runs: Chicago 1,096,581 rows; UK 513,801 rows across 5 files; NYC 1,793 rows for a 7-day window.

## 6. Rules that will save you pain

1. **Never put files in `data/raw/` by hand**, and never point `--input` / `--input-dir` at it. Raw means "an extractor produced this, with a manifest." Use `data/incoming/` for your own downloads.
2. **Config lives in YAML, not code.** Field mappings, value translations and known data-quality issues are in `config/adapters/*.yaml`. Transformation code should read them, not hardcode per-source logic.
3. **Don't commit `.env` or datasets.**
4. **UK extractor fails per year, not all-or-nothing.** If a year is missing or ambiguous, the other years still succeed and the run exits non-zero. Read the output, don't just trust the exit code.
5. **NYC returning 0 rows is treated as a failure**, on purpose.
6. **Structural nulls are not findings.** Some fields don't exist in some sources. Check the per-source gap table in `docs/data_dictionary.md` before interpreting any cross-city null pattern (especially Member C's EDA).

## 7. Handy reusable pieces

| File | What it gives you |
|---|---|
| `src/utils/config.py` | Adapter YAML loader. Use it for the staging transform |
| `src/utils/batch.py` | Collision-safe batch IDs |
| `src/utils/raw_writer.py` | Raw-layer writer + `manifest.json` |
| `src/profiling/profile_crashes.py` | Chicago profiling (missingness, sentinels, coordinate checks) |
| `tests/test_nyc_extractor.py` | Example of the mocked-API test style |

## 8. Where each person starts

**Member B (Transformation, Validation & DB)**
1. Run Chicago and UK extractors once to get real raw batches.
2. `sql/schema.sql` is already applied automatically on Postgres startup. Confirm the tables with `psql` (or the container logs), then tick that box in `TASKS.md`.
3. Build the shared staging transform that executes each adapter's `field_mapping`.

**Member C (Orchestration, Diagrams & Analytics)**
1. Diagrams, the DAG skeleton and the data contract can start now from `canonical_schema.yaml`, the adapters and `schema.sql`.
2. `dags/` is empty and is mounted into Airflow, so a skeleton DAG should appear in the UI quickly.
3. The EDA and clustering notebooks need Member B's curated output.

## 9. Troubleshooting

- **"cannot execute binary file" from the `pipeline` service:** this was a real bug, already fixed by `entrypoint: []` in `docker-compose.yml`. If it reappears, check that the entrypoint was not changed. It is not a Windows/WSL2 problem.
- **Airflow won't start or errors on DB:** confirm `AIRFLOW_DB` differs from `POSTGRES_DB`, and that the Fernet key is set. Init scripts only run on a *fresh* Postgres volume; after changing them, reset with `docker compose down -v` (this deletes DB data).
- **Extractor says file missing/corrupt:** check the path is the **container** path (`/opt/airflow/data/incoming/...`), not your host path.
- **Anything else:** `docker compose logs <service>`.

## 10. Housekeeping

- Tick off items in `TASKS.md` with initials and date as you finish them.
- Update `README.md` as each workstream lands.
- Anyone may be asked about any part in the technical defense, so skim the other members' sections.
