# AI Acknowledgment — Santos: Ingestion & Environment

Per the course's Academic Integrity and Responsible Use of AI policy
("Students must be able to explain and defend all submitted code, including
code produced with AI-assisted tools"), this document records where and how
AI assistance (Claude, Anthropic) was used for the Ingestion & Environment
workstream, what it produced, and what was independently verified by the
student rather than taken on faith.

*Last updated 2026-10-14 to match the repository after the instructor-feedback
changes (SHA-256 change detection, `source_freshness`, NYC window anchored on
the newest published date, and the NYC partial-pull check in the DAG).*

## Scope of AI Assistance

Claude was used as a coding assistant across the project's design and
ingestion phases: profiling source datasets, designing the canonical schema
and source adapters, drafting the PostgreSQL DDL and data dictionary, and —
for this workstream specifically — writing the extraction scripts, shared
utilities, Docker environment, and associated tests.

## What Was AI-Generated

Ingestion and environment (this workstream):

- `src/extract/chicago_extractor.py`, `src/extract/uk_extractor.py`,
  `src/extract/nyc_extractor.py` — the three source extractors
- `src/utils/batch.py`, `src/utils/raw_writer.py`, `src/utils/config.py` —
  shared ingestion helpers
- `tests/test_nyc_extractor.py` — mocked-API unit tests for the NYC
  extractor's retry/error-handling logic (6 tests)
- `Dockerfile`, `docker-compose.yml`, `.env.example`, `.gitignore` — the
  containerized environment
- `sql/00_airflow_metadata.sql` — Airflow metadata database init script
  (co-developed: the student diagnosed the missing-database error from
  real `airflow-init` logs and wrote the fix; Claude reviewed it and
  integrated it into `docker-compose.yml`'s init-script ordering)

Design phase (drafted with AI, then reviewed and decided by the team):

- `src/profiling/profile_crashes.py` — reusable profiling script
- `config/adapters/*.yaml`, `config/canonical_schema.yaml`, `sql/schema.sql`,
  `docs/data_dictionary.md` — source adapters, canonical schema, DDL and
  data dictionary

Refresh detection added in the instructor-feedback round:

- `src/utils/freshness.py` and `tests/test_freshness.py` (11 tests) —
  SHA-256 change detection, `source_freshness` status, and the matching
  logic in the Chicago and UK extractors. Claude reviewed instructor feedback
  and added these files to fit into the pipeline.


## What Was Independently Verified, Not Just Accepted

This is the part worth being explicit about for the technical defense: AI
output was treated as a first draft to be tested, not a finished product.

- **Every extractor was run against real data**, not just reviewed for
  syntax. Chicago: 1,096,581 real rows. UK: 513,801 real rows across all
  5 annual files. NYC: a live call to the real Socrata API (1,793 rows for
  a real 7-day window), confirming the pagination/retry logic against the
  actual endpoint, not only the mocked tests.
- **Error-handling paths were deliberately tested with bad input**, not
  assumed to work: a missing Chicago file, an empty file, a single-column
  (malformed) file, a missing UK year, and two UK files with ambiguous
  filenames all had to be constructed and run by hand to confirm the
  extractors failed the way they were supposed to.
- **A real, non-obvious bug was found and root-caused during testing**,
  not predicted by Claude in advance: the `pipeline` service's
  `entrypoint: /bin/bash` (no `-c`) caused bash to treat any command —
  `python`, `uname`, anything — as a shell script FILE to interpret
  rather than a program to execute, failing with a misleading "cannot
  execute binary file" error. The first AI-proposed fix (a Python wrapper
  script + PATH override) was *empirically plausible but wrong* — it was
  tested against a completely untouched system binary (`uname`), which
  still failed, proving the wrapper hadn't fixed anything. The actual fix
  (`entrypoint: []`) was identified only after that disconfirming test,
  and was verified by rerunning the same failing command.
- **Rerun-safety was verified empirically**, not just by reading the
  code. Batch IDs are collision-safe: each extractor was run twice against
  live Docker containers, confirming distinct batch-ID folders with no
  overwritten data. That check predates change detection. Since then,
  Chicago and the UK deliberately reuse the previous `batch_id` when the
  incoming file's SHA-256 matches the last raw batch (reported as
  `UNCHANGED`; `--force-new-batch` overrides it), while NYC writes a new
  batch every run. The pipeline was re-ran, with the newly added freshness
  test.
- **A missing-database bug in the Docker setup
  (`airflow_metadata` never being created) was found by the student from
  real container logs**, not predicted in advance — this is a case where
  AI-written config had a real gap that only surfaced in actual use.
- **A gap in the DAG was found by reading it against the extractor, not by
  a failing run:** the NYC extractor marks a partial pull with
  `completed_fully: false`, but the DAG's `extract` task did not read that
  flag, so a partial pull would have continued downstream. The task now
  raises when the flag is false. The partial batch stays in raw but is
  never staged or validated.

## What This Means for the Technical Defense

Any team member should be able to explain: why `data/incoming/` and
`data/raw/` are kept separate (traceability — raw/ must only ever contain
extractor-managed output with a manifest); why each extractor treats a
partial failure differently:

- Chicago is a single file, so it either copies or fails.
- The UK extractor handles each year independently and keeps the good
  years in raw, but the DAG fails the whole task if any year failed, so
  four of five years are never published as complete.
- NYC treats zero rows as a hard failure rather than a silent empty
  success, and a pull that stops partway also fails the task;

and the full story of the `entrypoint` bug above, which is a good
illustration of verifying a fix rather than trusting that it worked
because the symptom appeared to go away.

## What Remains Open / Human-Owned

- Whether the `pipeline`/`airflow` database-sharing design (one Postgres
  instance, two databases) is the right trade-off for this project's scale
  was a judgment call made collaboratively, not purely AI-decided — it was
  presented as a stated simplification to defend if asked, not a hidden
  assumption.
- `--max-rows` and the 5-year ingestion window were decisions made by the
  student earlier in the project (documented in `docs/project_proposal.md`
  and `config/adapters/uk.yaml`), which this workstream's code implements
  but did not originate.
- Chicago and the UK refresh only when a person replaces the files in
  `data/incoming/`. That is a design trade-off the team accepted, not
  something the AI decided.
