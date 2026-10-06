# Project Proposal: A City-Agnostic Road Crash Data Pipeline

**Course:** DSS150P Data Engineering
**Status:** Draft v1. Items marked ⚠ need verification or a team decision.

***Superseded where it differs from the implementation; see README***

---

## 1. Problem Statement

Cities publish traffic crash data, but every city publishes it differently. Chicago's Traffic Crashes dataset has its own column names, injury categories, date formats, and coordinate conventions. New York, London, and other cities each use a different structure. Because of this, any analysis of crash patterns (where crashes cluster, when they happen, what factors are involved) has to be rebuilt from scratch for each city. The raw data is also messy. In our Chicago sample of 80 rows, about 41% of records had no coordinates, dates appeared in two different formats, and several flag columns were populated only when the value was "Y".

This project builds a **reusable, automated pipeline that ingests crash data from any supported city, maps it to one canonical schema, validates it, and produces a clean, analysis-ready dataset.** Chicago is the first source and the reference implementation. It is not a limit on what the pipeline can handle.

**Why this needs a pipeline and not a one-time analysis:** The sources update continuously, and crash reports are amended after they are first published. Sources also arrive in different formats and schemas. The value comes from repeatable ingestion, validation, and harmonization, not from a single notebook run.

## 2. Stakeholders and Downstream Users

| Stakeholder | Need |
|---|---|
| Transportation and road-safety analysts | Comparable crash data across cities without per-city cleaning |
| Urban planners and researchers | Hotspot and temporal-pattern analysis on a consistent schema |
| Data scientists | A trusted, documented curated layer for modelling |
| Future contributors | A way to add a new city by writing configuration, not new pipeline code |

## 3. Objectives

**Primary objective: build the pipeline.**
1. Ingest crash data automatically from multiple heterogeneous sources (bulk CSV, REST API JSON).
2. Preserve source-faithful raw data with ingestion metadata (source, batch ID, retrieval timestamp).
3. Validate data with at least five automated check types, with failures visible in logs and Airflow.
4. Transform every source into one **canonical crash schema** through per-source configuration files.
5. Load a modelled PostgreSQL schema and publish partitioned Parquet outputs.
6. Make the pipeline rerun-safe (no duplicates on repeated runs) and fully reproducible with Docker Compose.

**Secondary objective: EDA and clustering on the curated layer (bonus analytics).**
7. Explore temporal and severity patterns across cities.
8. Cluster crash locations (for example DBSCAN or HDBSCAN on coordinates, optionally with severity weighting) to identify hotspots, using the same code for every city.

## 4. Data Sources

The guidelines award full credit for **3 or more independent sources and 3 or more formats**. The plan below is designed to meet that.

| # | Source | Format / Retrieval | Role |
|---|---|---|---|
| 1 | Chicago Traffic Crashes (Crashes, plus People and Vehicles tables) | Bulk CSV export and Socrata API | Reference source. Confirmed structure from the sample (49 columns). |
| 2 | ⚠ NYC Motor Vehicle Collisions (NYC Open Data) | Socrata REST API → JSON | Second city with a different schema, which proves the pipeline generalizes |
| 3 | ⚠ UK road safety data (STATS19: collisions, vehicles, casualties) | Annual CSV files from data.gov.uk | Third country, different severity scale and coordinate system |
| 4 | ⚠ Open-Meteo historical weather API | REST API → JSON | Objective weather enrichment by location and hour, available for any city |

**Formats demonstrated:** CSV (raw bulk sources), JSON (API responses), Parquet (staging and curated layers). This covers the required CSV/JSON/Parquet comparison of size and read/write speed.

⚠ I have not yet verified the endpoints, schemas, licenses, or update frequencies for sources 2 to 4. That should be the first task: profile each one before committing. If any source proves unsuitable, other open city datasets (Los Angeles, San Francisco, Seattle, and others) can be substituted without changing the architecture. Please also confirm with the instructor that multiple tables from one portal (Chicago's Crashes, People, Vehicles) count as one source and not three.

## 5. Making It Work for Any City

This is the core design decision. Instead of writing Chicago-specific code, the pipeline treats each city as a **configuration file**:

- **Source adapter config (YAML)** per city: endpoint or file location, format, column mappings to the canonical schema, date format and timezone, coordinate reference system, and value mappings (for example, mapping each city's injury categories to a common severity scale).
- **Canonical severity scale:** fatal, serious, minor, none, unknown. Chicago's injury classes and the UK's fatal/serious/slight categories both map into this, with the original value kept alongside.
- **Shared transformation modules** that only ever see the canonical schema.
- **Adding a new city means adding one YAML file.** The Airflow DAG uses dynamic task mapping to run one branch per configured source.

**Canonical crash fields (draft):** crash_id, source_id, crash_timestamp_utc, local_timezone, latitude, longitude, severity, num_vehicles, num_injured, num_killed, crash_type, primary_cause, speed_limit (nullable), lighting, weather, plus ingestion metadata.

## 6. Architecture

```
Sources (CSV, API JSON)
   → Ingestion (Python, per-source adapter)  → RAW layer (files as received + batch metadata)
   → Validation (5+ check types)             → STAGING layer (cleaned, typed, Parquet)
   → Harmonization to canonical schema       → CURATED layer (Parquet, partitioned + PostgreSQL)
   → Consumption: SQL queries, EDA, clustering notebook

Orchestration: Apache Airflow  |  Runtime: Docker Compose  |  Config: .env + YAML  |  Version control: Git/GitHub
```

| Layer | What is allowed |
|---|---|
| Raw | No changes to content. Add only ingestion metadata (source, batch ID, timestamp). |
| Staging | Type conversion, date parsing, null handling, deduplication, standardizing categories. |
| Curated | Canonical schema, joins (crash ↔ people ↔ vehicles, weather enrichment), derived fields, and analysis-ready tables. |

**Note on tooling:** You listed Python, Git, Docker, and PostgreSQL. The course guidelines also make **Apache Airflow a required core component**, and a missing core technology triggers substantial deductions. This proposal includes Airflow. If your instructor has approved an exception, tell me and I'll adjust.

## 7. Data Model (PostgreSQL, draft)

- `dim_source` (source_id, city, country, provider, license, url)
- `dim_date` (date_key, year, month, day_of_week, is_weekend)
- `dim_location` (location_key, latitude, longitude, grid or cell ID, city)
- `fact_crash` (crash_id PK, source_id FK, date_key FK, location_key FK, severity, num_vehicles, num_injured, num_killed, crash_type, primary_cause, weather, lighting, batch_id)
- `fact_person` and `fact_vehicle` (FK to `fact_crash`), where source data supports it
- `dq_run_log` (batch_id, check_name, status, rows_checked, rows_failed, run_timestamp)

Natural key: (source_id, source_record_id). Deliverables: DDL scripts, ERD, and representative SQL queries.

## 8. Data Quality Checks (at least 5)

1. **Schema check:** expected columns present and types correct.
2. **Nullability:** required fields such as timestamp and source ID are never null.
3. **Uniqueness:** no duplicate natural keys within a batch.
4. **Accepted values:** severity and crash type fall within the allowed sets after mapping.
5. **Range checks:** latitude and longitude fall within the city's bounding box, speed limits are plausible, and dates are not in the future.
6. **Row-count reconciliation:** raw rows equal staged rows plus rejected rows.
7. **Referential integrity:** every person and vehicle row links to a crash.

Failing rows go to a quarantine table or file with a reason. Check results are written to `dq_run_log` and surface as Airflow task status.

## 9. Partitioning, Idempotency, and Reliability

- **Partition key:** `source_id / year / month` for Parquet in staging and curated. This suits time-window analysis and per-city processing, and the demo will show reading a single city-month without a full scan.
- **Rerun safety:** UPSERT on the natural key into PostgreSQL, and replace-partition semantics for Parquet, so repeating a run never duplicates data.
- **Late amendments:** Crash reports can be amended after publication, so each incremental run re-pulls a trailing window (for example, the last 90 days) and upserts it. ⚠ Confirm the best incremental field for each source during profiling.
- **Logging and errors:** Structured logs per stage with record counts. Retries with backoff on API calls. Clear failure messages for missing files, bad responses, and schema drift.

## 10. Orchestration and Environment

- **Airflow DAG:** `extract → raw_validate → stage → validate → harmonize → load_postgres → publish_parquet → quality_report`, with a daily or weekly schedule, retries, and one dynamically mapped branch per configured source.
- **Docker Compose services:** PostgreSQL, Airflow (webserver, scheduler), and the pipeline image.
- **Configuration:** `.env.example` for credentials and ports, YAML files for source adapters, and no secrets or absolute paths in the repository.

## 11. Analytics (Bonus)

- **EDA:** hour-of-day and day-of-week profiles, severity distributions, and city-to-city comparison using the canonical schema.
- **Clustering:** hotspot detection with a density-based method, run per city using identical code. Evaluate cluster stability and compare hotspot characteristics (time of day, severity mix).
- **Caveat to state in the report:** Crash counts depend on each city's reporting thresholds and practices, so cross-city comparisons are of patterns, not raw totals.

## 12. Known Risks and Limitations

- Reporting thresholds and coverage differ by city, which limits direct comparability.
- Severity scales don't map perfectly. The original values are kept next to the harmonized ones.
- Missing coordinates were common in the Chicago sample (about 41%). The plan is to keep those rows for non-spatial analysis and exclude them only from clustering, and to test geocoding from street address as a stretch goal.
- Officer-recorded conditions such as weather and road surface are subjective. Weather API data adds an objective second measure.
- Each added source multiplies profiling and mapping effort, so scope should be set at three or four sources.

## 13. Mapping to the Course Rubric

| Rubric area | How this proposal addresses it |
|---|---|
| Problem and use case | Cross-city crash analysis with named stakeholders and a real need for automation |
| Sources (3+ sources, 3+ formats) | Chicago, NYC, UK, plus weather, across CSV, JSON, Parquet |
| Architecture, layers | Raw → staging → curated with defined rules per layer |
| Ingestion | Automated, config-driven, raw-preserving, with metadata and error handling |
| Transformation | Shared canonical-schema logic, with per-source YAML mappings |
| Data quality (5+ checks) | 7 planned checks integrated into the DAG |
| Storage and modeling | Star-style PostgreSQL model with keys, ERD, and SQL |
| Orchestration | Airflow with dynamic per-source tasks, retries, and schedule |
| Rerun safety and reproducibility | UPSERT and replace-partition, Docker Compose, `.env.example` |
| Documentation | README, data dictionary, data contract on `fact_crash`, lineage diagram, ERD |

## 14. Proposed Timeline

| Phase | Work |
|---|---|
| 1 | Verify and profile all sources, settle the canonical schema |
| 2 | Docker Compose environment, PostgreSQL DDL, ingestion and raw layer for Chicago |
| 3 | Staging and validation, canonical harmonization for Chicago |
| 4 | Add source adapters for the additional cities and weather |
| 5 | Airflow DAG, idempotency tests, partitioning and format benchmarks |
| 6 | Documentation, diagrams, data dictionary, data contract, optional analytics |
| Final | Demo rehearsal and individual Q&A preparation before the presentation window (October 6–8, 2026) |

The presentation document is due **October 2, 2026** and the final paper on **October 14, 2026**.
