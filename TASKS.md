# Task Tracker

Three workstreams, split along the pipeline's actual data flow so each
person's output unblocks the next, not by even file count. Shared artifacts
(proposal, data dictionary, canonical schema, adapter configs) are already
done and apply to everyone — see `README.md` for current repo status.

Update this file directly as items are finished (check the box, add initials
and date). Anyone can be asked to explain any part during the technical
defense, so skim the other two sections even if they aren't yours.

---

## Member A — Ingestion & Environment
*No dependencies — can start immediately.*

- [x] Chicago bulk-file extractor (reads `config/adapters/chicago.yaml`, does not hardcode mapping logic) — `src/extract/chicago_extractor.py`
- [x] UK multi-year extractor (2021–2025, one file per year, loop + concatenate) — `src/extract/uk_extractor.py`; per-year failure isolation so one missing year doesn't discard the rest
- [x] Turn `pull_nyc_sample.py` into the real production NYC extractor — `src/extract/nyc_extractor.py`; retries + backoff, keeps the compound-sort-key pagination fix, treats 0 rows as a failure. Old sampling script removed (superseded).
- [x] Raw-layer storage: preserve source-faithful data, add ingestion metadata — `src/utils/raw_writer.py` (byte-for-byte file copies / raw JSON pages) + `src/utils/batch.py` (collision-safe batch IDs) + a `manifest.json` per batch
- [x] Error handling for each extractor: missing file, bad API response, connection failure — tested against real malformed/missing files (Chicago, UK) and mocked API failures (NYC, `tests/test_nyc_extractor.py`, 6/6 passing)
- [x] Dockerfile + `docker-compose.yml` (Postgres, Airflow, pipeline image) — **fully tested end-to-end** (Postgres init scripts, Airflow webserver/scheduler healthy, all 3 extractors run successfully against real data via the `pipeline` service). One real bug found & fixed: `pipeline`'s entrypoint — see README "What's actually done" for the full explanation.
- [x] `.env.example` with all config externalized (DB host/name/port, paths, API settings) — no real secrets committed

## Member B — Transformation, Validation & Database
*Needs Member A's raw layer (even a stub/sample) to transform against.*

- [ ] Staging transformation code that executes each adapter YAML's `field_mapping` (shared logic across sources, not copy-pasted per source)
- [ ] Curated-layer harmonization into the canonical schema
- [X] Run `sql/schema.sql` against a real PostgreSQL instance
- [ ] Implement 5+ automated data-quality checks as running code, writing results to `dq_run_log`:
  - [ ] Schema check
  - [ ] Nullability check
  - [ ] Uniqueness / duplicate check
  - [ ] Accepted-values check (severity, etc.)
  - [ ] Range check (coordinates, speed limit, dates not in future)
- [ ] Rerun-safety: UPSERT on `(source_id, source_record_id)` into Postgres
- [ ] Partitioning: write curated output to Parquet, partitioned by `source_id/year/month`
- [ ] Demonstrate reading a single partition without a full scan

## Member C — Orchestration, Diagrams & Analytics
*Can start diagrams/DAG skeleton immediately from existing schema/adapters; needs real data for the notebook.*

- [ ] Airflow DAG: `extract → raw_validate → stage → validate → harmonize → load_postgres → publish_parquet → quality_report`
- [ ] Task dependencies, scheduling, retries, failure handling
- [ ] Architecture diagram (sources → ingestion → raw → validation → staging → curated → Postgres → orchestration → consumption)
- [ ] Data flow / lineage diagram
- [ ] ERD from `sql/schema.sql`
- [ ] Data contract document (distinct from `docs/data_dictionary.md` — expected fields/types/nullability/constraints per producer/consumer)
- [ ] EDA notebook (temporal/severity patterns across cities, using the gap table in `data_dictionary.md` to avoid misreading structural nulls as findings)
- [ ] Clustering notebook (hotspot detection, run per city with identical code)

---

## Shared / Already Done
- [x] Problem statement & objectives — `docs/project_proposal.md`
- [x] Source profiling (Chicago full-scale, NYC sample, UK 5-year full) — findings folded into adapter `known_issues` sections
- [x] Canonical schema — `config/canonical_schema.yaml`
- [x] Source adapters (Chicago, NYC, UK) — `config/adapters/*.yaml`
- [x] PostgreSQL DDL (written, not yet executed) — `sql/schema.sql`
- [x] Data dictionary with per-source null/gap matrix — `docs/data_dictionary.md`
- [x] Chicago profiling script — `src/profiling/profile_crashes.py`
- [x] All three production extractors (Chicago, NYC, UK) + shared raw-layer/batch utils + Docker environment — Member A, see section above

## Not Yet Assigned
- [ ] README updates as each workstream lands
- [ ] Final technical report
- [ ] Presentation slides
- [ ] Individual Q&A prep (everyone, not delegable)
