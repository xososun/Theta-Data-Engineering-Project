# AI Acknowledgment — Hermoso: Transformation, Validation & Database

Per the course's Academic Integrity and Responsible Use of AI policy
("Students must be able to explain and defend all submitted code, including
code produced with AI-assisted tools"), this document records where and how
AI assistance (Claude, Anthropic) was used for the Transformation,
Validation & Database workstream, what it produced, and what was
independently verified by the student rather than taken on faith.

*Last updated 2026-10-14 to match the repository after the
instructor-feedback changes (schema/type casting in curated, sentinel
coordinate nulling, per-source date-logic tolerance, and the pandas
`NaT` coercion in the Postgres loader).*

## Scope of AI Assistance

Claude was used as a coding assistant across the Transformation, Validation
& Database workstream: designing the staging transform and its mapping
primitives, drafting the validation checks and their source-specific
tolerances, writing the curated harmonization and partitioned Parquet
writer, writing the Postgres loader with idempotent UPSERT, and building
the CSV/JSON/Parquet benchmark and the accompanying unit tests.

## What Was AI-Generated

Transformation, validation & database (this workstream):

- `src/transform/staging.py` — the config-driven raw→staging transform
  (mapping primitives: `from`, `literal`, `map`, `derive_fn`; sentinel
  coordinate nulling; canonical-field alignment)
- `src/transform/derivations.py` — named helper functions for per-source
  derived fields (Chicago/NYC/UK `has_valid_coordinates`, NYC severity
  and vehicle counting from components, crash-id construction)
- `src/transform/curated.py` — staging→curated harmonization (canonical
  type casting, per-source timestamp parsing to UTC, `crash_date_key`,
  partitioned Parquet by `source_id/year/month`, partition-pruned
  reader)
- `src/transform/format_compare.py` — CSV / JSON / Parquet size and
  read/write benchmark plus its Markdown report generator
- `src/validation/checks.py` — eight automated data-quality checks
  writing to `dq_run_log`, with per-source structural gaps and a
  documented-anomaly tolerance
- `src/load/load_postgres.py` — `dim_date` population and idempotent
  `fact_crash` UPSERT on `(source_id, source_record_id)`, with a
  `_to_pg_val` coercion that catches all pandas missing-value flavours
- `src/utils/paths.py`, `src/utils/db.py` — env-var-driven path and
  connection helpers used by the pipeline stages
- `tests/test_staging.py`, `tests/test_validation.py` — unit tests for
  the mapping primitives, sentinel handling, and DQ check logic

Adapter changes made to support the transform (the adapters themselves
were originally authored in the shared design phase by Member A):

- Added `source_id` and `source_local_timezone` literals to
  `config/adapters/chicago.yaml`, `config/adapters/nyc.yaml`, and
  `config/adapters/uk.yaml`.
- Reworked complex `derive:` blocks in each adapter into `derive_fn:`
  references, moving the per-source business rules into named functions
  in `derivations.py`.

## What Was Independently Verified, Not Just Accepted

This is the part worth being explicit about for the technical defense: AI
output was treated as a first draft to be tested, not a finished product.

- **Every module was run against real full-scale data**, not just
  reviewed for syntax. Chicago: 1,096,581 raw rows → 1,096,581 staged
  rows. UK: 513,801 → 513,801. NYC: 1,793 → 1,793. Row counts match
  exactly across the raw→staging boundary, verifying no silent row loss.

- **The staging transform was validated by a full DQ pass**, not just by
  successful execution: 24 checks (8 checks × 3 sources) run against the
  staged batches produced 22 pass, 2 warn, 0 fail. The two warnings are
  documented Chicago anomalies (speed limits outside the plausible band;
  one inverted police-notification timestamp) — both are recorded in
  `chicago.yaml`'s `known_issues` and handled by explicit
  configuration, not by loosening the check.

- **Real runtime bugs were found and root-caused during execution**, not
  predicted by Claude in advance. Examples:
  - *Adapter lookup mismatch.* The transform initially assumed the
    adapter filename matched the source's `source_id`
    (`chicago_us.yaml`). The actual file is `chicago.yaml`. Fixed by
    reading the `source_id` field from each YAML to locate the correct
    adapter — matching the pattern Member A's extractors already used.
  - *`derive:` expression incompatibility.* The adapters' inline
    `derive:` blocks were written in pseudo-English
    (`X is not null`, `X between bounds`) that could not be evaluated as
    Python. Fixed by introducing the `derive_fn` primitive, keeping the
    adapters config-driven for simple fields while moving complex
    per-source logic into named functions.
  - *Sentinel `(0,0)` coordinates.* Chicago (74 rows) and NYC (34 rows)
    use `(0,0)` as a "no coordinates recorded" sentinel. The first
    staging run left them as real points in the Gulf of Guinea; the fix
    nulls the pair at staging time so downstream spatial analysis does
    not silently include them.
  - *`pd.NaT` reaching Postgres as a literal string.* The first load
    attempt failed with `invalid input syntax for type timestamp:
    "NaT"`. Fixed by extending `_to_pg_val` to catch all pandas
    missing-value flavours (`None`, `NaN`, `pd.NA`, `pd.NaT`) before
    sending to psycopg2.
  - *Mixed-type Parquet write failure.* The first curated write failed
    with `Expected bytes, got a 'int' object` because `num_killed` held
    mixed `str`/`int`/`None` values. Fixed by casting all canonical
    numeric, boolean, and text columns to explicit pandas dtypes before
    the Parquet write.
  - *Date-logic check too strict.* The check initially failed Chicago on
    the single documented inverted timestamp. Fixed by introducing a
    per-source tolerance (5 for Chicago, 0 elsewhere) so documented
    anomalies warn rather than fail.

- **Idempotency was verified empirically**, not just by reading the
  `ON CONFLICT` clause. Re-running the load against one source produced
  the same row count in `fact_crash` — no duplicates, no drift.

- **Partition-pruned reads were verified with an actual query**, not by
  inspection: `read_partition("chicago_us", 2024, 3)` returned 8,924
  rows using `pyarrow.dataset` with a filter. This is the demo that
  reads a single `source_id/year/month` partition without scanning the
  full 1.6M-row dataset.

- **Unit tests exercise the primitives in isolation** (26 total across
  the repo: 6 from Member A's NYC extractor tests, 8 for the staging
  primitives, 12 for the validation logic). The staging tests cover each
  mapping primitive, sentinel handling, and the UK `-1` sentinel; the
  validation tests cover pass/fail/warn behaviour, structural-gap
  skipping, and the documented-anomaly tolerance.

- **The format benchmark was re-run after an unexpected first result**,
  not accepted at face value. The initial run showed Parquet reading
  slower than CSV (an artifact of Docker's virtualized filesystem on a
  cold first read). Re-running in a warm container showed Parquet
  winning on all three metrics (smallest, fastest write, fastest read).
  The report was updated to reflect the honest measurement, and the
  interpretation text was corrected rather than left mismatched to the
  numbers.

## What This Means for the Technical Defense

Any team member should be able to explain:

- **Why the pipeline is config-driven, not per-source code.** Each
  adapter YAML (`config/adapters/*.yaml`) carries that source's field
  mapping, value translations, date format, timezone, and structural
  gaps. `staging.py` reads the YAML and executes the mapping; it
  contains no `if source_id == "chicago_us"` branches. Adding a fourth
  city means writing a new YAML, not editing Python.

- **Why `derive_fn` exists alongside `derive`.** Simple inline
  expressions (`'uk_stats19:' + collision_index`) stay in the YAML.
  Non-trivial business rules — NYC severity derivation from injury
  components, coordinate bounding-box checks — live in named functions
  in `derivations.py`, callable and testable in isolation. This avoids
  fragile expression parsing while keeping the pipeline config-driven.

- **Why structural gaps are handled per-source, not as failures.** NYC
  cannot produce a `serious` severity (no categorical severity field in
  the source) and cannot populate `crash_type`, `weather_condition`,
  `lighting_condition`, or `posted_speed_limit_mph`. UK cannot populate
  `num_killed`, `primary_cause`, or `crash_type`. These are documented
  in `docs/data_dictionary.md` and encoded in
  `src/validation/checks.py`'s `STRUCTURAL_GAPS` map, so a null in those
  columns for that source is *by design*, not a data-quality failure.

- **Why the load is idempotent.** `fact_crash` has a
  `UNIQUE (source_id, source_record_id)` constraint;
  `src/load/load_postgres.py` uses `INSERT ... ON CONFLICT
  (source_id, source_record_id) DO UPDATE`. Re-running the loader
  overwrites fields with the newer values and never duplicates rows.
  This was verified by re-running against a real source and comparing
  counts.

- **Why Parquet is the curated storage format.** The benchmark shows
  Parquet smallest and fastest on write and read for a real 8,924-row
  curated slice. Combined with partition pruning and column projection,
  it's the correct choice for analytical consumption. CSV remains only
  as the source-interchange format; JSON remains only as the raw-API
  format.

- **Why the 255 unparseable timestamps were dropped, not "fixed".**
  Chicago's `POSTED_SPEED_LIMIT` range, one inverted police-notification
  timestamp, and NYC's `number_of_persons_killed` aggregate being ~88%
  null are all real source limitations, documented in the adapters'
  `known_issues`. The correct handling is to preserve those anomalies
  through staging (source-faithful), drop only genuinely unparseable
  timestamps at the curated boundary, and log the drop count — not to
  silently normalize them.

## What Remains Open / Human-Owned

- **The `source_id/year/month` partition key** was a team design
  decision made during the project proposal phase, matching the access
  patterns in the DDL's indexes (`idx_fact_crash_source_date`). This
  workstream's code implements that decision; it did not originate it.

- **The 5-year UK window and NYC date window** were decisions made by
  the team earlier in the project (documented in
  `docs/project_proposal.md` and `config/adapters/uk.yaml`). This
  workstream's staging and curated code consume those decisions; it did
  not make them.

- **The `fact_person` and `fact_vehicle` tables** mentioned in the
  original proposal are **not** implemented in v1. The canonical schema
  and DDL focus on `fact_crash`; the proposal describes a future state.
  This is a scope decision, documented in the README's "What's NOT
  built yet" section.

- **The choice to keep staging Parquet per batch rather than per source
  in a single consolidated file** is a trade-off. It makes reruns and
  backfills cleaner (replace one batch folder), at the cost of having
  multiple Parquet files per source. The curated layer consolidates
  across batches, so downstream consumers see one partitioned dataset.

- **The 0.016% row drop rate at the curated boundary** (255 of
  1,612,175 rows due to malformed or missing source timestamps) is an
  accepted limitation. Those rows cannot be inserted into a Postgres
  table whose `crash_timestamp_utc` column is `NOT NULL`; the
  alternative would be to fabricate a timestamp, which is worse. The
  drop count is logged, and the loss rate is documented for the report.