# Data Contract: `fact_crash` (Curated Layer)

**Version:** 1.1
**Status:** Active. Every guarantee below has been checked against the
implementation (`sql/schema.sql`, `src/transform/`, `src/validation/checks.py`,
`src/load/load_postgres.py`, `dags/crash_pipeline.py`).

This document is the **contract** between the pipeline (producer) and any
downstream consumer (analyst, notebook, dashboard, external query). It is
distinct from `docs/data_dictionary.md`: the dictionary describes what each
field *is*; this contract describes what each consumer is *guaranteed*.

Where the contract and the implementation disagree, the implementation is
wrong and must be fixed, or this contract must be explicitly renegotiated
(see "Change Process" below). Silent divergence is the failure mode this
document exists to prevent.

---

## 1. Scope

**Producer:** The cross-city crash pipeline (`extract → raw → staging →
validation → curated → PostgreSQL`), orchestrated by the Airflow DAG
`crash_pipeline`, running inside the Docker Compose stack defined in
`docker-compose.yml`.

**Consumers in scope:**
- Analytical queries against the `fact_crash` PostgreSQL table
  (`sql/schema.sql`).
- The partitioned Parquet curated output at
  `data/curated/fact_crash/source_id=<id>/year=<YYYY>/month=<MM>/`.
- The EDA notebook (`notebooks/eda.ipynb`) and clustering notebook
  (`notebooks/clustering.ipynb`).
- Any future dashboard or export built on the curated layer.

**Consumers out of scope:**
- The raw layer (`data/raw/`) — pipeline-internal. Only extractors and the
  staging transform read it. No external consumer should depend on
  raw-layer file paths or formats.
- The staging layer (`data/staging/`) — also pipeline-internal, and not
  part of this contract.

---

## 2. Sources Covered

| `source_id` | Provider | Scope | Update cadence |
|---|---|---|---|
| `chicago_us` | City of Chicago Data Portal (Socrata) | City of Chicago, full published history | Bulk CSV export; refreshed by the portal on its own schedule, ingested on demand |
| `nyc_us` | NYC OpenData (Socrata) | New York City, a 90-day window per run ending at the newest date the API has published | Live API pull; NYC publishes with a lag of several months, and crash reports can be amended after first publication |
| `uk_stats19` | UK Department for Transport (data.gov.uk) | Great Britain (England, Scotland, Wales), published years 2021–2025 | One CSV per year; the DfT publishes annual files, and revisions to past years are occasionally released |

Adding a source means adding a new adapter YAML (`config/adapters/*.yaml`)
**and** a new section in this contract. A new source without a contract
section is a contract violation, not a feature.

---

## 3. What the Producer Guarantees

These are unconditional guarantees — every row in `fact_crash` satisfies
them, and any row that doesn't is a producer bug.

### 3.1 Row-level guarantees

| Guarantee | Enforced by | Notes |
|---|---|---|
| `crash_id` is globally unique across all sources | `fact_crash.crash_id` PRIMARY KEY | Constructed as `'{source_id}:{source_record_id}'`; collision across sources is impossible by construction, not by convention |
| `(source_id, source_record_id)` is unique | `uq_fact_crash_source_natural_key` UNIQUE constraint + `uniqueness` check | This is the dedup key at load time; UPSERT relies on it |
| `crash_timestamp_utc` is non-null and in UTC | NOT NULL; local time converted to UTC in `src/transform/curated.py` | Rows whose local time cannot be converted are excluded (see section 6) |
| `severity` is one of `fatal`, `serious`, `minor`, `none`, `unknown` | CHECK constraint + `accepted_values` check | This is the canonical scale; source values are preserved separately in `source_severity_raw` |
| `country` is `US` or `GB` | `CHAR(2) NOT NULL` + `accepted_values` check | Set by each adapter as a literal, never read from source data |
| `source_id` matches a row in `dim_source` | FOREIGN KEY + `referential` check | Every row's source is registered |
| `crash_date_key` matches a row in `dim_date` | FOREIGN KEY | `load_postgres.py` populates `dim_date` before inserting any crash row |
| Counts (`num_injured_total`, `num_killed`, `num_vehicles_involved`) are non-negative | CHECK constraints + `range` check | |
| `batch_id` is non-empty | NOT NULL | The batch that most recently wrote the row; joins to `dq_run_log.batch_id` for that batch's check results |

### 3.2 Batch-level validation guarantee

Every staged batch passes through 8 automated checks (`schema`,
`nullability`, `uniqueness`, `accepted_values`, `range`, `date_logic`,
`referential`, `row_count`) before it can reach the curated layer. Each
result is written to `dq_run_log`.

| Check outcome | Effect |
|---|---|
| `pass` | Batch proceeds |
| `warn` | Batch proceeds; the flagged rows **are loaded**. Used for documented anomalies (e.g. implausible speed limits, one known Chicago timestamp inversion) |
| `fail` | The validate task fails, and the **whole run stops before harmonize**. No row from any source reaches curated Parquet or `fact_crash` in that run; the previous load stays in place |

Validation is batch-level, not row-level: there is no quarantine. A single
failing check blocks the entire batch rather than removing individual
rows.

### 3.3 Rerun-safety guarantees

| Guarantee | Mechanism |
|---|---|
| Re-running the pipeline does not duplicate rows | UPSERT on `(source_id, source_record_id)` — the natural key, not the surrogate `crash_id`. Verified: two consecutive full DAG runs both left 1,630,664 rows |
| Late amendments to a source record are reflected, not duplicated | NYC re-pulls a trailing 90-day window each run and UPSERTs it; amended records overwrite their earlier version |
| A crash row is never deleted from `fact_crash` | The load step only inserts or updates; no pipeline code issues `DELETE` or `TRUNCATE` against `fact_crash` |

### 3.4 Raw-layer traceability guarantees

| Guarantee | Mechanism |
|---|---|
| Every `fact_crash` row can be traced back to the raw batch it came from | `batch_id` on the row → `data/raw/<source_id>/<batch_id>/` → that batch's `manifest.json`; `source_record_id` then locates the record within the batch's files |
| Every raw batch has an ingestion manifest | `src/utils/raw_writer.py` writes a `manifest.json` per batch, recording `source_id`, `batch_id`, `retrieved_at_utc`, file list, SHA-256 per file |
| Raw files are byte-for-byte source-faithful | `raw_writer.write_raw_file_copy` uses `shutil.copy2`, never re-parses; API pages are written as received |

### 3.5 Layer-boundary guarantees

| Layer | What's guaranteed | What's explicitly *not* guaranteed |
|---|---|---|
| `data/raw/` | Byte-for-byte copy of source; manifest present; batches never overwritten | No schema stability. File names and layouts are internal and may change |
| `data/staging/` | Each source mapped to canonical column names; sentinels nulled | Timestamps are still source-local strings; types not yet cast. Pipeline-internal |
| `data/curated/` (Parquet) | Canonical schema (`config/canonical_schema.yaml`); UTC timestamps; partitioned by `source_id/year/month` | Holds **only the newest batch per source** — rebuilt from scratch each run. For NYC that means only the latest 90-day window |
| `fact_crash` (Postgres) | Everything in sections 3.1–3.4; accumulates every batch ever loaded | No query-latency SLA; this is a course project, not a production service |

Because Parquet is rebuilt per run but Postgres accumulates, the two can
hold different NYC row counts. Use `fact_crash` for history and the
Parquet output for the current snapshot.

---

## 4. Known Structural Gaps (Not Violations)

These are fields that are **legitimately null** for certain sources because
the source doesn't collect the data. They are documented here so a consumer
does not read a structural null as a data-quality problem — which would
mislead any cross-city comparison.

| Field | Structurally null for | Reason | Consumer guidance |
|---|---|---|---|
| `police_notified_timestamp_utc` | `nyc_us`, `uk_stats19` | Neither source has an equivalent field | Do not compute "time to police notification" across all sources; Chicago-only |
| `city` | `uk_stats19` | National (GB-wide) dataset, not city-scoped | Any query grouping by `city` must either exclude `uk_stats19` or accept NULL as a legitimate category |
| `severity = 'serious'` bucket | `nyc_us` | NYC has no categorical severity field, only injury/kill counts. It can derive `fatal`, `minor`, `none` — but not the serious/minor distinction Chicago and UK provide | A severity breakdown by source will show NYC missing the `serious` row entirely; this is not zero serious crashes |
| `num_killed` | `uk_stats19` | STATS19 fatality detail lives in a separate Casualties table not ingested in this pipeline | Do not compute "fatal crash rate by killed-count" for UK; use `severity = 'fatal'` instead |
| `crash_type` | `nyc_us`, `uk_stats19` | Neither source classifies crashes by physical type (rear-end, angle, sideswipe) | Only Chicago supports crash-type analysis in v1 |
| `primary_cause` | `uk_stats19` | STATS19 contributory factors live in a separate, unjoined table | |
| `weather_condition` | `nyc_us` | Not collected by source | |
| `lighting_condition` | `nyc_us` | Not collected by source | |
| `posted_speed_limit_mph` | `nyc_us` | Not collected by source | |
| `hit_and_run` | `nyc_us`, `uk_stats19` | No reliable source signal; defaults to `false` | **A "hit-and-run rate by city" query will show 0% for NYC and UK. This is a data gap, not a finding.** |
| `source_row_raw_ref` | all sources | Column exists in the schema but is not yet populated | Trace rows through `batch_id` instead (section 3.4) |

The full per-field, per-source availability matrix is in
`docs/data_dictionary.md`, section "Field Reference". This table is the
*contract-relevant* subset: the gaps a consumer is most likely to
misinterpret as findings.

---

## 5. Known Per-Source Quirks the Consumer Must Account For

These are not guarantees the producer makes, but characteristics of the
data a consumer should know:

| Quirk | Source | Consumer impact |
|---|---|---|
| `-1` is a sentinel for "not recorded" across many coded fields, including `speed_limit` | `uk_stats19` | Translated to NULL at staging before it reaches `fact_crash`. If you see `-1` in a UK row, it's a pipeline bug, not a data value |
| Sentinel `(0,0)` coordinates | `chicago_us`, `nyc_us` | Nulled at staging, and `has_valid_coordinates` is `FALSE`. 2.3% of NYC coordinate-bearing rows (profiled sample) are sentinel — much higher than Chicago's 0.007%. Filter on `has_valid_coordinates = TRUE` for any spatial analysis |
| Aggregate kill count far less complete than its components | `nyc_us` | `num_killed` is derived from the pedestrian + cyclist + motorist killed components, not the aggregate field (which is ~88% null). Do not re-derive from the aggregate field |
| Partial pre-2017 coverage by police district | `chicago_us` | Recommend windowing trend analysis from 2018-01-01 onward for citywide comparability |
| `speed_limit` includes implausible values (0, 3 mph) | `chicago_us` | The `range` check flags (does not reject) values outside `[5, 70]` as `warn`, so they are loaded. Apply a plausibility filter for any speed-limit analysis |
| A small number of police-notification times precede the crash time | `chicago_us` | Tolerated by the `date_logic` check (up to 5 rows) as a documented anomaly, not corrected |
| Data ends months before the run date | `nyc_us` | Publication lag, not missing data. The pipeline anchors its window on the newest published date |

---

## 6. Guarantees the Producer Explicitly Does *Not* Make

Consumers should not assume these. Any code relying on them is fragile:

- **Not every raw record reaches `fact_crash`.** Crashes timestamped in a
  daylight-saving transition hour are excluded during harmonization,
  because their UTC time cannot be determined: in autumn the 1:00–1:59 am
  hour occurs twice (ambiguous), and in spring the 2:00–2:59 am hour does
  not exist. In the current load this excludes 186 Chicago rows and 69 UK
  rows. The harmonize task logs the count on every run.
- **No row-count stability between runs.** A source that adds backfilled
  records will produce more rows next run; that's correct behavior, not a
  regression. Compare against a specific `batch_id` if you need a stable
  snapshot.
- **No cross-source severity equivalence beyond the canonical scale.** A
  "serious" injury in Chicago and a "serious" injury in the UK follow
  different legal definitions. The canonical scale maps them into one
  bucket, but they are not identical events.
- **No data-quality certification for individual rows.** `dq_run_log`
  records which checks passed at the batch level. Rows flagged by a `warn`
  are loaded. `fact_crash` is not "verified correct" per row; it is
  "belongs to a batch that passed the automated checks in place at
  ingestion time."
- **No stable row identity across schema changes.** If `source_record_id`
  ever changes format for a source, `crash_id` changes, and UPSERT will
  treat the row as new. Sources' natural keys are treated as immutable;
  the pipeline has no migration mechanism if one changes.
- **No support for back-dated schema changes.** If a source retroactively
  redefines a field's meaning (e.g. `severity` scale changes), the
  pipeline will ingest both interpretations into the same column. This
  would require an explicit schema migration and reingestion.

---

## 7. Out of Scope for v1 (Documented Gap vs. Proposal)

The project proposal (section 7) drafts a fuller data model than
`schema.sql` implements. The following are **not** part of v1's contract:

| Proposal item | Status in v1 | Reason |
|---|---|---|
| `dim_location` | Not implemented | Location is denormalized onto `fact_crash` (lat/lon + `has_valid_coordinates`). A separate dimension was deferred — coordinate cardinality is high and not currently a join target |
| `fact_person` | Not implemented | STATS19 casualty detail lives in a separate file not ingested. Adding it means a new source file + adapter + contract section |
| `fact_vehicle` | Not implemented | Same as above for STATS19; NYC has partial vehicle data via `vehicle_type_code_N` fields, kept in staging but not promoted |
| Row-level quarantine | Not implemented | Validation blocks whole batches instead (section 3.2) |
| `source_local_timezone` on `fact_crash` | Not implemented; it's on `dim_source` | Local timezone is a property of the source, not per-crash |

Any consumer needing person- or vehicle-level analysis must wait for a
future contract version. The proposal's draft model should be read as
"what a fuller version might include," not as a current guarantee.

---

## 8. Change Process

Any change to the guarantees in this document requires:

1. **A change to the DDL or the transformation code** — the contract
   describes reality, not intention. A contract edit alone is not a
   contract change.
2. **A version bump** at the top of this document, with a changelog entry
   at the bottom.
3. **A note in `README.md`** under "What's Built" if the change adds or
   removes a deliverable.
4. **For a new source:** a new adapter YAML, a new section in the sources
   table (section 2), and any new structural gaps added to section 4.

Consumers that depend on a guarantee being removed must be updated in the
same change. "The contract changed, and the notebook broke" is not an
acceptable outcome — the notebook is part of the change.

---

## 9. Open Items

- **Populate `source_row_raw_ref`.** The column exists but is always null.
  A documented format such as
  `{source_id}/{batch_id}/{filename}#row={n}` would let a consumer jump
  from a row to its exact raw line, rather than searching the batch by
  `source_record_id`.
- **Resolve DST-ambiguous timestamps instead of excluding them** (section
  6), e.g. by assuming the first occurrence for ambiguous times and
  shifting nonexistent times forward.

---

## Changelog

| Date | Version | Change |
|---|---|---|
| (initial) | 1.0 | Draft written from `sql/schema.sql`, `config/canonical_schema.yaml`, `config/adapters/*.yaml`, `docs/data_dictionary.md`, and `docs/project_proposal.md`. Open items marked ⚠ pending the transformation layer. |
| 2026-10-06 | 1.1 | Confirmed against the merged implementation. Replaced the row-level quarantine claim with the actual batch-level validation behavior (3.2). Removed the unimplemented "not in the future" rule. Corrected traceability to use `batch_id` (`source_row_raw_ref` is not populated). Updated the NYC window to "90 days ending at the newest published date". Documented the DST exclusion, the Parquet-vs-Postgres history difference, and verified idempotency. Resolved the rerun-window, quarantine, and partitioning open items. |
