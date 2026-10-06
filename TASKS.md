# Task Tracker

Three workstreams, split along the pipeline's actual data flow so each
person's output unblocks the next, not by even file count. Shared artifacts
(proposal, data dictionary, canonical schema, adapter configs) are already
done and apply to everyone — see `README.md` for current repo status.

Update this file directly as items are finished (check the box, add initials
and date). Anyone can be asked to explain any part during the technical
defense, so skim the other two sections even if they aren't yours.

---

## Member A (Santos) — Ingestion & Environment
*No dependencies — can start immediately.*

- [x] Chicago bulk-file extractor (reads `config/adapters/chicago.yaml`, does not hardcode mapping logic) — `src/extract/chicago_extractor.py`
- [x] UK multi-year extractor (2021–2025, one file per year, loop + concatenate) — `src/extract/uk_extractor.py`; per-year failure isolation so one missing year doesn't discard the rest
- [x] Turn `pull_nyc_sample.py` into the real production NYC extractor — `src/extract/nyc_extractor.py`; retries + backoff, keeps the compound-sort-key pagination fix, treats 0 rows as a failure. Old sampling script removed (superseded).
- [x] Raw-layer storage: preserve source-faithful data, add ingestion metadata — `src/utils/raw_writer.py` (byte-for-byte file copies / raw JSON pages) + `src/utils/batch.py` (collision-safe batch IDs) + a `manifest.json` per batch
- [x] Error handling for each extractor: missing file, bad API response, connection failure — tested against real malformed/missing files (Chicago, UK) and mocked API failures (NYC, `tests/test_nyc_extractor.py`, 6/6 passing)
- [x] Dockerfile + `docker-compose.yml` (Postgres, Airflow, pipeline image) — **fully tested end-to-end** (Postgres init scripts, Airflow webserver/scheduler healthy, all 3 extractors run successfully against real data via the `pipeline` service). One real bug found & fixed: `pipeline`'s entrypoint — see README "What's actually done" for the full explanation.
- [x] `.env.example` with all config externalized (DB host/name/port, paths, API settings) — no real secrets committed

## Member B (Hermoso) — Transformation, Validation & Database
*Needs Member A's raw layer (even a stub/sample) to transform against.*

- [x] Staging transformation code that executes each adapter YAML's `field_mapping` (shared logic across sources, not copy-pasted per source) — (MB, 2026-10-05)
- [x] Curated-layer harmonization into the canonical schema — (MB, 2026-10-05)
- [x] Run `sql/schema.sql` against a real PostgreSQL instance — (MB, 2026-10-04)
- [x] Implement 8 automated data-quality checks as running code, writing results to `dq_run_log`:
  - [x] Schema check
  - [x] Nullability check
  - [x] Uniqueness / duplicate check
  - [x] Accepted-values check (severity, etc.)
  - [x] Range check (coordinates, speed limit, dates not in future)
  - [x] Date-logic check (with per-source tolerance for documented anomalies)
  - [x] Referential-integrity check
  - [x] Row-count reconciliation check
- [x] Rerun-safety: UPSERT on `(source_id, source_record_id)` into Postgres — (MB, 2026-10-05)
- [x] Partitioning: write curated output to Parquet, partitioned by `source_id/year/month` — (MB, 2026-10-05)
- [x] Demonstrate reading a single partition without a full scan — (MB, 2026-10-05)
- [x] CSV / JSON / Parquet handling + size and performance comparison — (MB, 2026-10-05)
- [x] Postgres load of 1,611,920 rows with verified idempotency — (MB, 2026-10-05)

## Member C (Leyte) — Orchestration, Diagrams & Analytics
*Can start diagrams/DAG skeleton immediately from existing schema/adapters; needs real data for the notebook.*

- [x] Airflow DAG: `extract → raw_validate → stage → validate → harmonize → load_postgres → publish_parquet → quality_report` — `dags/crash_pipeline.py` (2026-10-06)
- [x] Task dependencies, scheduling, retries, failure handling — in `dags/crash_pipeline.py`: per-source task groups via dynamic task mapping; weekly schedule; 2 retries with 2-min delay; `quality_report` uses `TriggerRule.ALL_DONE` so partial runs still report (2026-10-06)
- [x] Architecture diagram (sources → ingestion → raw → validation → staging → curated → Postgres → orchestration → consumption) — `docs/diagrams/architecture.mmd`, embedded in `docs/diagrams.md` §1 (2026-10-06)
- [x] Data flow / lineage diagram — `docs/diagrams/lineage.mmd`, embedded in `docs/diagrams.md` §2; field-level provenance plus the `source_row_raw_ref` → raw-file → manifest traceability chain (2026-10-06)
- [x] ERD from `sql/schema.sql` — `docs/diagrams/erd.mmd`, embedded in `docs/diagrams.md` §3 (2026-10-06)
- [x] Data contract document (distinct from `docs/data_dictionary.md` — expected fields/types/nullability/constraints per producer/consumer) — `docs/data_contract.md` (2026-10-06)
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
- [x] Diagrams index — `docs/diagrams.md`, with all three diagrams embedded and rendered via Mermaid — Member C (2026-10-06)

## Not Yet Assigned
- [ ] README updates as each workstream lands
- [ ] Final technical report
- [ ] Presentation slides
- [ ] Individual Q&A prep (everyone, not delegable)