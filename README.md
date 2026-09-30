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
  constraints enforcing the canonical enums. **Not yet executed against a
  real PostgreSQL instance** — written and manually reviewed, but needs a
  real `psql` run to confirm it's error-free.
- **Data dictionary** (`docs/data_dictionary.md`), including a per-field,
  per-source null/availability matrix so cross-source analysis doesn't
  accidentally misread a structural gap as a real finding.
- **Reusable profiling script** (`src/profiling/profile_crashes.py`) for the
  Chicago source — schema drift, missingness, date-format issues,
  coordinate sentinel/bounding-box checks, district coverage, date-logic
  validation.
- **NYC API pull script** (`src/extract/pull_nyc_sample.py`) — paginated
  Socrata API extraction with a fixed compound sort key (an earlier version
  had a pagination bug that produced duplicate rows; documented and fixed).

### What's NOT built yet
- Automated ingestion for Chicago (bulk file) and UK (multi-year file) — only
  NYC has an extraction script so far, and it's a sampling tool, not the
  production extractor.
- Raw → staging → curated transformation code implementing the adapter
  mappings (the YAML configs describe the mapping; nothing executes it yet).
- The 5+ automated data-quality checks as running code (the *rules* are
  documented in the adapters and schema; `dq_run_log` exists as a table with
  no writer yet).
- Airflow DAGs (`dags/` is currently an empty placeholder).
- Docker / Docker Compose environment.
- `.env.example` / configuration management.
- Partitioning implementation (Parquet output, partition-pruned reads).
- Data contract document (distinct from the data dictionary).
- Architecture, data-flow/lineage, and ERD diagrams.
- Tests (`tests/` is an empty placeholder).
- Analytics/EDA/clustering notebooks.

## Repository Structure

```
project/
├── README.md                 # this file
├── config/
│   ├── canonical_schema.yaml  # the shared schema every source maps into
│   └── adapters/
│       ├── chicago.yaml
│       ├── nyc.yaml
│       └── uk.yaml
├── docs/
│   ├── project_proposal.md    # problem statement, objectives, architecture
│   └── data_dictionary.md     # field-level reference incl. per-source gaps
├── sql/
│   └── schema.sql             # PostgreSQL DDL (NOT YET run against real Postgres)
├── src/
│   ├── extract/
│   │   └── pull_nyc_sample.py # NYC API sampling/pull script
│   ├── profiling/
│   │   └── profile_crashes.py # Chicago source profiling script
│   ├── transform/              # empty — not yet built
│   ├── load/                   # empty — not yet built
│   ├── validation/              # empty — not yet built
│   └── utils/                   # empty — not yet built
├── data/
│   ├── raw/                    # empty — not yet populated
│   ├── staging/                 # empty — not yet populated
│   └── curated/                 # empty — not yet populated
├── dags/                        # empty — Airflow DAGs not yet written
├── notebooks/                   # empty — analytics not yet started
└── tests/                       # empty — not yet written
```

`src/profiling/` is a deviation from the course's suggested layout (which
lists `extract/transform/load/validation/utils` under `src/`) added to keep
one-off source-profiling scripts separate from pipeline-integrated
validation checks that will run inside the DAG. Documented here since the
guidelines ask for an explanation wherever the structure differs.

## Next Steps

See the conversation/project log for the agreed order of work. As of this
commit, the next planned items are: verify `sql/schema.sql` against a real
PostgreSQL instance, then build the extraction/transformation code that
actually executes the adapter mappings.