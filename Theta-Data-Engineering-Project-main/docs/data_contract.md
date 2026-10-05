# Data Contract: `fact_crash` (Curated Layer)

**Version:** 1.0
**Status:** Draft — awaiting implementation of the transformation layer
(Member B's workstream). Contract terms marked ⚠ are *proposed* and must
be confirmed against the transformation code once it exists.

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
curated`), orchestrated by Airflow, running inside the Docker Compose
stack defined in `docker-compose.yml`.

**Consumers in scope:**
- Analytical queries against the `fact_crash` PostgreSQL table
  (`sql/schema.sql`).
- The EDA notebook (`notebooks/eda.ipynb`) and clustering notebook
  (`notebooks/clustering.ipynb`).
- The partitioned Parquet curated output ⚠ (not yet implemented — Member B).
- Any future dashboard or export built on the curated layer.

**Consumers out of scope:**
- The raw layer (`data/raw/`) — that is pipeline-internal. Only extractors
  and the (future) staging transform read it. No external consumer should
  depend on raw-layer file paths or formats.
- The staging layer — also pipeline-internal. Its schema is source-specific
  and not part of this contract.

---

## 2. Sources Covered

| `source_id` | Provider | Scope | Update cadence |
|---|---|---|---|
| `chicago_us` | City of Chicago Data Portal (Socrata) | City of Chicago, full published history | Bulk CSV export; refreshed by the portal on its own schedule, ingested on demand |
| `nyc_us` | NYC OpenData (Socrata) | New York City, last 5 years per run window | Live API pull; crash reports can be amended after first publication |
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
| `(source_id, source_record_id)` is unique | `uq_fact_crash_source_natural_key` UNIQUE constraint | This is the dedup key at load time; UPSERT relies on it |
| `crash_timestamp_utc` is non-null and not in the future | NOT NULL + `not_future` validation rule | "Future" is defined relative to the pipeline run time, not to the consumer's clock |
| `severity` is one of `fatal`, `serious`, `minor`, `none`, `unknown` | CHECK constraint + validation layer | This is the canonical scale; source values are preserved separately in `source_severity_raw` |
| `country` is a valid ISO 3166-1 alpha-2 code | CHECK (via `CHAR(2)` and controlled insert) | Currently only `US` and `GB` |
| `source_id` matches a row in `dim_source` | FOREIGN KEY | Every row's source is registered |
| `crash_date_key` matches a row in `dim_date` | FOREIGN KEY | The date dimension is populated before any crash row is inserted |
| `batch_id` is non-empty | NOT NULL | Ties the row to exactly one pipeline run; see `dq_run_log` joins |

### 3.2 Rerun-safety guarantees

| Guarantee | Mechanism |
|---|---|
| Re-running the pipeline for the same source/window does not duplicate rows | UPSERT on `(source_id, source_record_id)` — the natural key, not the surrogate `crash_id` |
| Late amendments to a source record are reflected, not duplicated | ⚠ A trailing re-pull window (proposal section 9: "last 90 days") re-ingests and UPSERTs. The exact window per source is TBD pending Member B's implementation |
| A crash row is never silently deleted | The pipeline UPSERTs; it does not `DELETE`. Removing a row requires an explicit, logged operation |

### 3.3 Raw-layer traceability guarantees

| Guarantee | Mechanism |
|---|---|
| Every `fact_crash` row can be traced back to the exact raw file it came from | `source_row_raw_ref` on the row → the raw-layer file at that path → the file's `manifest.json` |
| Every raw file has an ingestion manifest | `src/utils/raw_writer.py` writes a `manifest.json` per batch, recording `source_id`, `batch_id`, `retrieved_at_utc`, file list, SHA-256 per file |
| Raw files are byte-for-byte source-faithful | `raw_writer.write_raw_file_copy` uses `shutil.copy2`, never re-parses; API pages are written as received |

### 3.4 Layer-boundary guarantees

| Layer | What's guaranteed | What's explicitly *not* guaranteed |
|---|---|---|
| `data/raw/` | Byte-for-byte copy of source; manifest present | No schema stability. File names and layouts are internal and may change |
| `data/staging/` | ⚠ Source-specific cleaned/typed data | No cross-source schema. Each source's staging shape is its own |
| `data/curated/` | Canonical schema (`config/canonical_schema.yaml`); stable | ⚠ Not yet implemented (Member B) — Parquet partitioning by `source_id/year/month` per proposal section 9 |
| `fact_crash` (Postgres) | Everything in sections 3.1–3.3 | No query-latency SLA; this is a course project, not a production service |

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
| `-1` is a sentinel for "not recorded" across many coded fields, including `speed_limit` | `uk_stats19` | Translated to NULL by the adapter before it reaches `fact_crash`. If you see `-1` in a UK row, it's a pipeline bug, not a data value |
| Sentinel `(0,0)` coordinates | `chicago_us`, `nyc_us` | Rejected by `has_valid_coordinates`. 2.3% of NYC coordinate-bearing rows are sentinel — much higher than Chicago's 0.007%. Filter on `has_valid_coordinates = TRUE` for any spatial analysis |
| Aggregate kill count far less complete than its components | `nyc_us` | `num_killed` is derived from the pedestrian + cyclist + motorist killed components, not the aggregate field (which is ~88% null). Do not re-derive from the aggregate field |
| Partial pre-2017 coverage by police district | `chicago_us` | Recommend windowing trend analysis from 2018-01-01 onward for citywide comparability |
| `speed_limit` includes implausible values (0, 3 mph) | `chicago_us` | The pipeline flags (does not reject) values outside `[5, 70]` — check `posted_speed_limit_mph` against a plausibility filter for any speed-limit analysis |

---

## 6. Guarantees the Producer Explicitly Does *Not* Make

Consumers should not assume these. Any code relying on them is fragile:

- **No row-count stability between runs.** A source that adds backfilled
  records will produce more rows next run; that's correct behavior, not a
  regression. Compare against a specific `batch_id` if you need a stable
  snapshot.
- **No cross-source severity equivalence beyond the canonical scale.** A
  "serious" injury in Chicago and a "serious" injury in the UK follow
  different legal definitions. The canonical scale maps them into one
  bucket, but they are not identical events.
- **No data-quality certification for individual rows.** `dq_run_log`
  records which checks passed at the batch level. Rows that failed a check
  go to quarantine (⚠ not yet implemented — Member B) and are excluded
  from `fact_crash`. But `fact_crash` is not "verified correct" per row;
  it is "passed the automated checks in place at ingestion time."
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
3. **A note in `README.md`** under "What's actually done" if the change
   adds or removes a deliverable.
4. **For a new source:** a new adapter YAML, a new section in the sources
   table (section 2), and any new structural gaps added to section 4.

Consumers that depend on a guarantee being removed must be updated in the
same change. "The contract changed, and the notebook broke" is not an
acceptable outcome — the notebook is part of the change.

---

## 9. Open Items (⚠ To Be Resolved)

These are known unknowns; each will be resolved by Member B's work and
this contract updated accordingly:

- ⚠ **Exact rerun window per source.** Proposal says "last 90 days" for
  late amendments. Actual value depends on how often each source revises
  past records.
- ⚠ **Quarantine mechanism.** Proposal section 8 says failing rows go to
  "a quarantine table or file with a reason." Not yet built. This
  contract currently says failed rows are excluded; if quarantine lands
  differently, update section 6.
- ⚠ **Partitioning scheme for Parquet.** Proposal says `source_id/year/month`.
  Not yet implemented. The Postgres `fact_crash` table is not partitioned
  (it relies on indexes); the Parquet output will be. Confirm the
  partitioning matches the proposal before finalizing.
- ⚠ **`source_row_raw_ref` format.** Currently unconstrained. Should be a
  documented format (e.g. `{raw_root}/{source_id}/{batch_id}/{filename}#row={n}`)
  so a consumer can actually follow it. Settle this with Member B.

---

## Changelog

| Date | Version | Change |
|---|---|---|
| (initial) | 1.0 | Draft written from `sql/schema.sql`, `config/canonical_schema.yaml`, `config/adapters/*.yaml`, `docs/data_dictionary.md`, and `docs/project_proposal.md`. Open items marked ⚠ pending Member B's transformation layer. |