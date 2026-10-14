# AI Acknowledgment Summary

Prepared for inclusion in the Final Technical Report (Section 14), per the
course's Academic Integrity and Responsible Use of AI policy. This section
applies project-wide; All members' workstream also has a detailed
standalone record (`docs/ai_acknowledgment_member_[a/b/c].md`) that this summary
draws from and is consistent with.

---

## Statement of AI Tool Use

This project made use of Claude (Anthropic) as a coding and design
assistant across multiple workstreams: source data profiling, canonical
schema and source-adapter design, PostgreSQL schema design, data
ingestion and refresh detection, transformation, validation, and database 
loading (raw → staging → curated transform, 8 DQ checks, idempotent Postgres load, 
partitioned Parquet), and orchestration, diagrams and analytics 
(the Airflow DAG's task graph, trigger rules and retry policy; 
the Mermaid source for the architecture, lineage and ERD diagrams; 
the comparison-frame module and the DBSCAN clustering notebook; 
the data contract document; and the analytics prose and section summaries).

Consistent with the course policy, AI assistance does not remove
individual responsibility for understanding, verifying, and being able to
defend all submitted work. Each member's section below states what was
AI-assisted, what was independently verified and how, and what remains a
human decision the AI did not make.

## Principles Followed Across All Workstreams

- **AI output was treated as a draft, not a deliverable.** Code, config,
  and documentation produced with AI assistance were run, tested, and
  checked against real data or a real environment before being considered
  finished, not accepted on the basis that it looked correct.
- **Failures and corrections are documented, not hidden.** Where an
  AI-proposed approach was wrong or incomplete, the project's
  documentation records what went wrong and how it was actually fixed,
  rather than presenting only the final, working state as if it were the
  first attempt.
- **Source-of-truth artifacts (the canonical schema, adapter configs, DDL)
  were treated as decisions team members made and reviewed**, not
  accepted from the AI without evaluation — profiling findings that
  informed them were produced by running real code against real data.
- No fabricated data, results, or test outcomes appear in this project.
  Where something has not been verified, the project's documentation
  (`README.md`, `TASKS.md`) says so explicitly.

---

## Member A (Santos) — Ingestion & Environment

**Full detail:** `docs/ai_acknowledgment_member_a.md`

AI-assisted: source profiling script, the three source extractors
(Chicago, NYC, UK), shared ingestion utilities, the Docker/Airflow
environment, the associated tests, and the first drafts of the adapters,
canonical schema, DDL and data dictionary, which the team then reviewed and
decided. Refresh detection (SHA-256 comparison and `source_freshness`) was
added in the instructor-feedback round; the detailed record states how AI
was involved in it.

Independently verified: every extractor was run against real, full-scale
or live data (not just reviewed) — 1,096,581 real Chicago rows, 513,801
real UK rows across 5 annual files, and a live call to the real NYC
Socrata API (1,793 rows for a 7-day window). Error-handling paths were
tested against deliberately bad input (missing/malformed files, missing
years, ambiguous filenames). Batch IDs were confirmed collision-safe by
actually re-running extractors. Chicago and the UK now reuse the previous
batch when a file's SHA-256 is unchanged, and NYC writes a new batch each
run; this behaviour is covered by tests in `tests/test_freshness.py`.

Notable issues found and fixed: one AI-proposed fix (a Docker entrypoint
workaround) was found, through testing, not to have addressed the real
cause of a bug; the actual root cause was identified and fixed afterward.
Separately, reading the DAG against the NYC extractor showed that a
partial NYC pull would not have failed the `extract` task; the task now
raises when the manifest says the pull was not complete. See the linked
document for the full account.

## Member B (Hermoso) — Transformation, Validation & Database

Full detail: docs/ai_acknowledgment_member_b.md

AI-assisted: the config-driven staging transform and its per-source derivation 
functions, the curated harmonization and partitioned Parquet writer, the eight 
staging data-quality checks writing to dq_run_log, the Postgres loader with idempotent 
UPSERT, the CSV/JSON/Parquet benchmark, the path and database helpers, the associated 
unit tests, and the adapter changes needed to support the transform (source_id and 
timezone literals, and moving complex derive blocks into named derive_fn functions). 
The partition key order, the 5-year UK and NYC date windows, and the scope decision to 
leave fact_person and fact_vehicle out of v1 were team decisions the AI did not make; 
the detailed record lists these.

Independently verified: every module was run against the real raw batches, not just 
reviewed. Raw-to-staged row counts match exactly (Chicago 1,096,581; UK 513,801; 
NYC 1,793 in that run), and the eight checks across the three sources gave 22 pass, 
2 warn, 0 fail, with both warnings being documented Chicago anomalies handled by 
explicit configuration rather than by loosening the check. Re-running the load, and a 
second full DAG run, left per-source row counts unchanged, which is the evidence for 
idempotency. A filtered read of Chicago, March 2024 returned 8,924 rows without scanning 
the full dataset. The curated gate was fault-tested by injecting a wrong UK date format: 
all staging checks passed, the curated reconciliation check failed for the UK, and publish 
and load did not run. The full Docker re-run after the instructor-feedback changes is 
still open, so these figures come from the earlier full run plus the unit tests.

Notable issues found and fixed: several AI-written first drafts failed on real data 
and were root-caused during execution. The transform assumed adapter filenames 
matched source IDs (chicago_us.yaml versus the real chicago.yaml). The adapters’ 
pseudo-English derive expressions could not be evaluated, which led to the derive_fn 
primitive. (0,0) sentinel coordinates (74 Chicago, 34 NYC) were first left as real points.
pd.NaT reached Postgres as the string “NaT”. The first Parquet write failed on 
mixed-type num_killed values. The date-logic check failed Chicago on one documented 
anomaly until a per-source tolerance was added. The benchmark first showed Parquet
reading slower than CSV, a cold-read artifact of Docker’s filesystem, and the report 
was corrected after a warm re-run instead of being left mismatched to the numbers. 
After instructor feedback, curated parsing was also made strict so a wrong date format 
fails the run instead of silently swapping day and month, and 255 crashes inside 
daylight-saving transition hours are now excluded and counted each run rather than 
dropped silently.


## Member C (Leyte) — Orchestration, Diagrams & Analytics

Full detail: docs/ai_acknowledgment_member_c.md

AI-assisted: the Mermaid source code for the architecture, lineage and ERD diagrams, 
and the draft of the data contract document, the analytics write-up, and the summary of 
the sections. AI was also used as an aid while I was in a fix to generate a list of things 
that needed to be done for my sections and to help me in writing the documentation. 
All the artifacts that belonged to Leyte were created and decided by me, including the 
Airflow DAG, the logic of the comparison-frame, the clustering notebooks, and the 
content of the final diagram.

Independently verified: the DAG's task graph, dependencies, retries, trigger rules 
and schedule were verified against `tests/test_dag_imports.py`, which checks for 
the order of tasks `harmonize → validate_curated → publish → load`, the dynamic mapping 
of tasks per source, the `all_success` trigger rule on the curated gate, and the `ALL_DONE` 
trigger rule on `quality_report`. The write-audit-publish workflow was verified from 
the latest quality report, not just from looking at the code: Each source's 
`curated_reconciliation_check` shows the batch validated in that run and the 
precise staged = curated + excluded numbers; Chicago 1,096,581 = 1,096,395 + 186; 
UK 513,801 = 513,732 + 69; NYC 20,537 = 20,537 + 0; and `curated_partition_check` 
verifies 211 partition files among three sources, just as harmonize found. 
The architecture and lineage diagrams were compared line by line to the code 
they represent; `dags/crash_pipeline.py` and `src/transform/derivations.py`; 
and the ERD to `sql/schema.sql`, not against an idealized form. 
The analytics figures were re-created from the curated Parquet in `notebooks/clustering.ipynb`: 
in the Chicago vs London, 2025 case, 26.3% of injury crashes are in hotspots 
for London while 8.8% in Chicago; in the Chicago vs NYC, 2026 case, 16.5% for Chicago 
compared to 6.2% in NYC. Stability of the hotspots was verified by running DBSCAN on 
80% subsamples (three seeds per city): London 27.6-28.6%, Chicago 9.1-9.3% in the London case, 
Chicago 16.9-17.7% in the NYC case, and NYC 8.2-9.5%.

Notable issues found and fixed: while the AI-drafted lineage diagram depicts the NYC `severity` 
attribute as a direct mapping from a source column, NYC does not publish a categorical severity 
attribute; analyzing the diagram based on `src/transform/derivations.py` revealed the mistake, 
and I revised the diagram to depict `severity` as a derived attribute (`nyc_severity()`), 
which is calculated based on the number of injuries and deaths. 
The hotspots method section explains why DBSCAN should be used instead of k-means, 
in that k-means presumes spherical clustering and *k* selection, 
but the road network creates elongated hotspots along arterial roads, and most crashes 
do not occur in hotspots. The comparison-frame rule defined in `comparison_frames.py` 
(the same location, the same event type, the same period, and rate per 100,000 residents per year) 
is responsible for excluding total numbers and Great Britain versus one city from the analysis, 
and the notebook's Finding cell highlights that the two frames yield different shares 
for Chicago and that shares are not comparable between the frames.

---
