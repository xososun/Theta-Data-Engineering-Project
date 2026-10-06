# Running Member C's Branch (Orchestration)

For anyone who wants to pull the `member-c/orchestration` branch and see
the Airflow DAG, diagrams, data contract, and DAG structural tests.

**Important:** this branch does **not** run the pipeline end-to-end. The
DAG is complete in structure but fails by design at the `raw_validate`
stage, because the transformation modules it depends on are not part of
this branch. That failure is the point — it proves the DAG is wired
correctly up to the boundary where those modules plug in. See Section 5
for what "success" means here.

---

## 0. What You'll End Up With

After following this guide, you will have:

- The Airflow DAG `crash_pipeline` visible and healthy in the Airflow UI.
- One triggered DAG run showing `extract` tasks **succeeding** (real
  extractors) and `raw_validate` failing with `NotImplementedError` (from
  the stubs) — the intended, correct outcome.
- Three Mermaid diagrams rendering: architecture, data flow / lineage, ERD.
- The data contract document (`docs/data_contract.md`).
- `tests/test_dag_imports.py` runnable and (once inside the Airflow image)
  passing 11/11.
- The two notebook scaffolds openable in VS Code.

**Time:** ~20–30 minutes, mostly waiting for Docker to build.

---

## 1. Prerequisites

Before anything else:

- **Docker Desktop** installed and running (whale icon steady in the
  system tray).
- **Git** installed.
- The project folder is **not** inside OneDrive or Downloads. Use
  `C:\Users\<you>\dev\` or similar.
- **VS Code** installed with the Mermaid preview extensions (see Section 6).
- **Node.js** (optional) — only needed if you want to export diagrams to PNG.

Verify in a terminal:

```powershell
docker --version
docker compose version
git --version
```

All three should print version numbers. If `docker ps` errors with
"Cannot connect to the Docker daemon," open Docker Desktop and wait for
the whale to stop animating.

---

## 2. Clone and Switch to the Branch

If you already have the repo cloned:

```powershell
cd <path-to>/Theta-Data-Engineering-Project
git fetch origin
git checkout member-c/orchestration
git pull
```

If you don't have the repo yet:

```powershell
cd <parent-folder>
git clone https://github.com/xososun/Theta-Data-Engineering-Project.git
cd Theta-Data-Engineering-Project
git checkout member-c/orchestration
```

Verify you're on the right branch:

```powershell
git branch
```

Expected:

```
  main
* member-c/orchestration
```

The `*` should be next to `member-c/orchestration`.

Verify the files exist:

```powershell
Get-ChildItem dags, docs, notebooks, tests
```

Expected key files:

```
dags/crash_pipeline.py
dags/_stubs.py

docs/diagrams.md
docs/diagrams/erd.mmd
docs/diagrams/architecture.mmd
docs/diagrams/lineage.mmd
docs/data_contract.md

notebooks/eda.ipynb
notebooks/clustering.ipynb

tests/test_dag_imports.py
```

If any are missing, `git pull` again.

---

## 3. Set Up `.env`

The repo ships with `.env.example` but not `.env` (it's gitignored). This
is mandatory — the Docker stack will not start without it.

```powershell
Copy-Item .env.example .env
```

Open `.env` in VS Code. Set three values:

### 3a — Fernet key

Generate one:

```powershell
docker run --rm apache/airflow:2.9.3-python3.11 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the 44-character output into `.env`:

```
AIRFLOW_FERNET_KEY=<your-44-char-key-ending-in-=>
```

### 3b — Postgres password

Find `POSTGRES_PASSWORD=changeme_use_a_real_password` and change to
anything you like. **Avoid `$`, `#`, `!`, and backticks** unless you know
how to escape them for Compose:

```
POSTGRES_PASSWORD=local_dev_pw_2026
```

### 3c — Airflow admin password

```
AIRFLOW_ADMIN_PASSWORD=admin_local_2026
```

Save `.env`.

Confirm `.env` isn't tracked:

```powershell
git status --short
```

`.env` should **not** appear. If it does, stop — `.gitignore` is broken.

---

## 4. Start the Docker Stack

```powershell
docker compose up -d --build
```

First run: 5–30 minutes (downloads the Airflow base image ~400 MB,
installs deps, builds the pipeline image). Subsequent runs: ~10 seconds.

Wait ~30 seconds after the command returns, then:

```powershell
docker compose ps
```

Expected:

| NAME | STATUS |
|---|---|
| `crash_pipeline_postgres` | `Up ... (healthy)` |
| `crash_pipeline_airflow_init` | `Exited (0)` |
| `crash_pipeline_airflow_webserver` | `Up ... (healthy)` |
| `crash_pipeline_airflow_scheduler` | `Up` |

The critical signals are **`Exited (0)`** for `airflow-init` (it's a
one-shot job, not a long-running service) and **`(healthy)`** for
`postgres`.

If postgres shows `starting` for more than 2 minutes:

```powershell
docker compose logs postgres --tail=50
```

If tables weren't created (fresh DB but init scripts failed):

```powershell
docker compose down -v
docker compose up -d --build
```

**Warning:** `down -v` deletes the Postgres volume. Only use it when you
deliberately want to reset the database.

Access the Airflow UI: **http://localhost:8080** — log in with
`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env` (in the
example above, `admin` / `admin_local_2026`).

Access Postgres directly:

```powershell
docker compose exec postgres psql -U pipeline_user -d crash_pipeline -c "\dt"
```

Expected: 4 tables — `dim_source`, `dim_date`, `fact_crash`, `dq_run_log`.

---

## 5. Verify the DAG and Trigger a Run

This is the section that's specific to this branch. What you're verifying
is not that the pipeline "works end-to-end" — it's that the DAG's
**structure** is correct, and that it fails at exactly the boundary that
was designed.

### 5a — Confirm the DAG registered

```powershell
docker compose exec airflow-scheduler airflow dags list
```

Expected: a line including `crash_pipeline`.

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected: **no** row for `crash_pipeline`. If there is one, the error
message names the file and line that failed to import.

### 5b — Trigger a run

```powershell
docker compose exec airflow-scheduler airflow dags trigger crash_pipeline
```

Expected output: a line like

```
Created <DagRun crash_pipeline @ 2026-10-06T00:00:00+00:00: manual__2026-10-06T00:00:00+00:00, externally triggered: True>
```

### 5c — Watch the run

```powershell
docker compose exec airflow-scheduler airflow dags list-runs -d crash_pipeline
```

Repeat every ~15 seconds until the `state` column shows `failed`. Or open
the Airflow UI at http://localhost:8080, click `crash_pipeline`, and watch
the **Graph** view.

### 5d — What "success" looks like here

This is the point of the whole exercise. Look at the task colors in the
Graph view:

| Task | Expected outcome |
|---|---|
| `extract` (per source) | **green** — calls the real extractors |
| `raw_validate` | **red** — `NotImplementedError` from `dags/_stubs.py` |
| `stage` | grey — `upstream_failed` |
| `validate` | grey — `upstream_failed` |
| `harmonize` | grey — `upstream_failed` |
| `load_postgres_task` | grey — `upstream_failed` |
| `publish_parquet_task` | grey — `upstream_failed` |
| `quality_report` | red — `TriggerRule.ALL_DONE` makes it run regardless; it calls a stub too |

**All three source branches should show the same pattern.** A Chicago
failure does not cancel the NYC or UK branch — that's the per-source
isolation the DAG was designed for.

### 5e — Confirm the failure is the stub, not a real bug

Click the red `raw_validate` task in the UI → **Logs**. Near the bottom you
should see:

```
NotImplementedError: raw_validate stage not yet implemented. Member B's
workstream provides this. Until then, DAG runs will fail here by design -
the DAG itself parses and appears in the Airflow UI.
```

If you see a **different** error (`FileNotFoundError`, `ModuleNotFoundError`,
etc.), that's a wiring bug, not the intended stub failure. Paste the
traceback and file a ticket.

### 5f — Confirm the extract stage actually ran

Because `extract` succeeded, at least one source should have written a
real batch to disk:

```powershell
Get-ChildItem .\data\raw -Recurse -Directory
```

Expected: at least one folder per source that had data available. The
Chicago and UK extractors need source CSVs in `data/incoming/`; NYC pulls
live. Sources whose inputs are missing will show red at `extract` instead,
and no batch folder — that's also a valid outcome.

---

## 6. View the Diagrams

All three diagrams are in `docs/diagrams.md`, embedded as Mermaid and
rendered by VS Code's Markdown Preview.

### 6a — Install the Mermaid extensions (once)

Open the Extensions panel (`Ctrl+Shift+X`) and install:

| Extension ID | Purpose |
|---|---|
| `bierner.markdown-mermaid` | Renders Mermaid in Markdown Preview |
| `bpruitt-goddard.mermaid-markdown-syntax-highlighting` | Syntax highlighting for `.mmd` files |

### 6b — Preview the diagrams

Open `docs/diagrams.md` and press `Ctrl+Shift+V`.

You should see:

1. **Architecture** — sources → ingestion → raw → validation → staging →
   harmonization → curated → Postgres + Parquet → consumption, with an
   "Orchestration & Runtime" band wrapping the whole thing.
2. **Data flow / lineage** — each canonical `fact_crash` field traced back
   to its source column(s); structural gaps shown as dashed red "cannot
   populate" edges.
3. **ERD** — four entities (`dim_source`, `dim_date`, `fact_crash`,
   `dq_run_log`) with foreign keys and CHECK constraints.

To view a `.mmd` file standalone, open it and use Command Palette →
"Mermaid: Open Preview".

### 6c — Export to PNG (optional)

If the final paper needs image files:

```powershell
New-Item -ItemType Directory -Force docs\diagrams\exported
npx -y @mermaid-js/mermaid-cli -i docs/diagrams/erd.mmd -o docs/diagrams/exported/erd.png
npx -y @mermaid-js/mermaid-cli -i docs/diagrams/architecture.mmd -o docs/diagrams/exported/architecture.png
npx -y @mermaid-js/mermaid-cli -i docs/diagrams/lineage.mmd -o docs/diagrams/exported/lineage.png
```

Requires Node.js. `docs/diagrams/exported/` is not gitignored — add it to
`.gitignore` if you don't want the PNGs committed.

---

## 7. Read the Data Contract

Open `docs/data_contract.md`. It's distinct from `docs/data_dictionary.md`:

- **Dictionary** = *what the fields are* (types, descriptions, per-source availability).
- **Contract** = *what each producer guarantees and each consumer can rely on*.

Sections worth knowing:

| Section | What it says |
|---|---|
| 3 | Row-level, rerun-safety, raw-layer, and layer-boundary guarantees the producer makes. |
| 4 | Structural gaps (fields null for some sources by design, not by bug). |
| 5 | Per-source quirks (UK `-1` sentinel, Chicago / NYC `(0,0)` sentinel, NYC aggregate-vs-component kill counts). |
| 6 | Explicit *non*-guarantees — the assumptions a consumer might otherwise make. |
| 7 | Out-of-scope for v1: `dim_location`, `fact_person`, `fact_vehicle` are in the proposal draft but not implemented. |
| 9 | Open items pending the transformation layer. |

---

## 8. Run the DAG Structural Tests

`tests/test_dag_imports.py` verifies the DAG without needing a running
Airflow instance — but Airflow itself is only inside the Docker image, so
the tests must be run there, not on the host.

### 8a — On the host (expected partial failure)

```powershell
python -m pytest tests/test_dag_imports.py -v
```

Expected: 3 tests pass (the `_stubs.py` checks and the file-exists check),
8 tests fail with `ModuleNotFoundError: No module named 'airflow'`. That
failure is **expected** — Airflow is not installed on the host, by design
(see `requirements.txt`).

### 8b — Inside the container (full pass)

```powershell
docker compose run --rm -v "${PWD}\tests:/opt/airflow/tests" pipeline python -m pytest /opt/airflow/tests/test_dag_imports.py -v
```

Expected: **11 tests passing.**

If it fails with `FileNotFoundError` on the test path, the volume mount
didn't take — check the `${PWD}` expansion by running
`Get-Location` first and substituting the literal path.

---

## 9. Where Everything Lives

| Layer | Path | Produced by |
|---|---|---|
| Airflow DAG | `dags/crash_pipeline.py` | Member C |
| Stub modules | `dags/_stubs.py` | Member C |
| DAG tests | `tests/test_dag_imports.py` | Member C |
| Diagrams | `docs/diagrams/*.mmd` | Member C |
| Diagram index | `docs/diagrams.md` | Member C |
| Data contract | `docs/data_contract.md` | Member C |
| Notebook scaffolds | `notebooks/eda.ipynb`, `notebooks/clustering.ipynb` | Member C |

Everything under `data/` is gitignored — it doesn't travel between
machines.

---

## 10. Useful Inspection Commands

**Confirm the DAG is registered and importable:**

```powershell
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```

**See run history for the DAG:**

```powershell
docker compose exec airflow-scheduler airflow dags list-runs -d crash_pipeline
```

**See task states for the most recent run (replace `<run_id>`):**

```powershell
docker compose exec airflow-scheduler airflow tasks states-for-dag-run crash_pipeline <run_id>
```

**View a batch's manifest (from the extract stage that ran):**

```powershell
docker compose run --rm pipeline cat /opt/airflow/data/raw/chicago_us/<batch_id>/manifest.json
```

**Confirm the stub still raises:**

```powershell
docker compose run --rm pipeline python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); import _stubs; _stubs.validate_raw_batch('chicago_us', 'x', '/tmp')"
```

Expected: `NotImplementedError` traceback.

---

## 11. Troubleshooting

| Symptom | Fix |
|---|---|
| `docker ps` errors with "Cannot connect" | Start Docker Desktop; wait for the whale to steady |
| `docker compose up` fails on Postgres healthcheck | `.env` is missing or has placeholder values — see Section 3 |
| `airflow-scheduler is not running` | Run `docker compose ps -a` and `docker compose logs airflow-scheduler --tail=60`. Usually `airflow-init` didn't finish — check its logs too. |
| `crash_pipeline` not in `airflow dags list` | Run `airflow dags list-import-errors` — the message names the file and line |
| DAG Graph view is empty | The `per_source_branch.expand(...)` line in `crash_pipeline.py` may be missing or malformed |
| `ModuleNotFoundError: No module named 'airflow'` when running pytest on host | Expected — run inside the container per Section 8b |
| `ModuleNotFoundError: extract.chicago_extractor` inside the container | `sys.path.insert` lines in `crash_pipeline.py` were moved or removed |
| Diagram renders as raw code block in preview | Mermaid extension not installed — see Section 6a |
| Port 8080 or 5432 in use | Change `AIRFLOW_WEBSERVER_PORT` / `POSTGRES_PORT` in `.env`, then `docker compose down && docker compose up -d` |
| Anything else | `docker compose logs <service> --tail=100` |

---

## 12. Day-to-Day Git Workflow

Once you're on `member-c/orchestration`:

**Before starting work:**

```powershell
git checkout member-c/orchestration
git pull
```

**After making changes:**

```powershell
git add <files>
git commit -m "clear message"
git push
```

**To get updates from `main` while keeping your branch:**

```powershell
git fetch origin
git merge origin/main
```

Resolve conflicts, then push.

**Never commit:**

- `.env`
- `data/incoming/*.csv` (or any large dataset)
- `data/raw/`, `data/staging/`, `data/curated/` contents
- `__pycache__/`, `.pytest_cache/`, `.ipynb_checkpoints/`

The `.gitignore` already excludes these.

---

## 13. One-Line Quick Reference

For someone who just wants the commands:

```powershell
git clone https://github.com/xososun/Theta-Data-Engineering-Project.git
cd Theta-Data-Engineering-Project
git checkout member-c/orchestration

Copy-Item .env.example .env
# Edit .env: set AIRFLOW_FERNET_KEY, POSTGRES_PASSWORD, AIRFLOW_ADMIN_PASSWORD

docker compose up -d --build

# Verify DAG registered
docker compose exec airflow-scheduler airflow dags list

# Trigger a run (will fail at raw_validate — by design)
docker compose exec airflow-scheduler airflow dags trigger crash_pipeline
docker compose exec airflow-scheduler airflow dags list-runs -d crash_pipeline

# Run DAG structural tests inside the container
docker compose run --rm -v "${PWD}\tests:/opt/airflow/tests" pipeline python -m pytest /opt/airflow/tests/test_dag_imports.py -v

# View the diagrams
# Open docs/diagrams.md in VS Code and press Ctrl+Shift+V
```

---

## 14. If Something Breaks

1. **Read the error.** Most are self-explanatory.
2. **Check you're on the right branch:** `git branch` should show `* member-c/orchestration`.
3. **Check `.env`:** the three values must be set — no `changeme_*` placeholders.
4. **Check Docker:** `docker compose ps` — services should be healthy.
5. **Re-pull:** `git pull`.
6. **Ping Member C** with the error output.

### What's *not* a bug

- `raw_validate` failing with `NotImplementedError`. That is the stub
  boundary; it's how the DAG signals that the transformation modules are
  not loaded yet. Nothing to fix.
- `stage`, `validate`, `harmonize`, `load_postgres_task`,
  `publish_parquet_task` showing `upstream_failed`. They can't run until
  `raw_validate` succeeds.
- pytest on the host failing with `ModuleNotFoundError: No module named
  'airflow'`. Airflow is not installed on the host; run the tests inside
  the container per Section 8b.
- Notebooks producing no output when you "Run All." All their code cells
  are commented out; they are scaffolds, waiting for curated Parquet output.
