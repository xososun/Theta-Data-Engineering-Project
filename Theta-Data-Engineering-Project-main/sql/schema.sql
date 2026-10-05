-- schema.sql
--
-- PostgreSQL DDL for the curated layer of the cross-city crash pipeline.
-- Implements the canonical schema defined in canonical_schema.yaml, fed by
-- the chicago_us / nyc_us / uk_stats19 source adapters.
--
-- Design notes:
--   - fact_crash uses (source_id, source_record_id) as its natural key,
--     enforced with a UNIQUE constraint, and is loaded with UPSERT
--     (INSERT ... ON CONFLICT ... DO UPDATE) for rerun safety, per the
--     idempotency strategy in the project proposal.
--   - Nullability below matches what profiling actually found per source,
--     not an idealized schema: several columns are nullable because at
--     least one real source (NYC, UK) cannot populate them, as documented
--     in each adapter's known_issues section.
--   - CHECK constraints enforce the canonical enums/ranges from
--     canonical_schema.yaml directly in the database, so a bad row is
--     rejected at load time even if the validation layer upstream is
--     ever bypassed or has a bug.

BEGIN;

-- ============================================================
-- dim_source: one row per configured source adapter
-- ============================================================
CREATE TABLE dim_source (
    source_id           TEXT PRIMARY KEY,
    provider             TEXT NOT NULL,
    dataset_url           TEXT,
    city                  TEXT,               -- NULL for national sources (e.g. uk_stats19)
    country               CHAR(2) NOT NULL,    -- ISO 3166-1 alpha-2
    source_local_timezone TEXT NOT NULL,       -- IANA tz name
    license               TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE dim_source IS
    'One row per source adapter config (chicago_us, nyc_us, uk_stats19, ...). '
    'city is NULL for national-scope sources such as uk_stats19.';

-- ============================================================
-- dim_date: standard date dimension, generated once and reused
-- ============================================================
CREATE TABLE dim_date (
    date_key      DATE PRIMARY KEY,
    year          SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    day           SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,          -- 0=Monday .. 6=Sunday (ISO-ish, defined at load time)
    is_weekend    BOOLEAN NOT NULL
);

COMMENT ON TABLE dim_date IS
    'Standard date dimension. Populate once for the full range covered by '
    'all configured sources (2013-2026 as of the current sources), not '
    'incrementally per batch.';

-- ============================================================
-- fact_crash: the canonical, cross-source curated table
-- ============================================================
CREATE TABLE fact_crash (
    crash_id                    TEXT PRIMARY KEY,     -- '{source_id}:{source_record_id}'
    source_id                   TEXT NOT NULL REFERENCES dim_source(source_id),
    source_record_id            TEXT NOT NULL,        -- original natural key, verbatim

    -- Time
    crash_timestamp_utc         TIMESTAMPTZ NOT NULL,
    crash_date_key               DATE NOT NULL REFERENCES dim_date(date_key),
    police_notified_timestamp_utc TIMESTAMPTZ,          -- NULL for nyc_us, uk_stats19 (no source field)

    -- Location
    city                         TEXT,                  -- NULL for uk_stats19 (national dataset)
    country                      CHAR(2) NOT NULL,
    latitude                     DOUBLE PRECISION,
    longitude                    DOUBLE PRECISION,
    has_valid_coordinates        BOOLEAN NOT NULL DEFAULT FALSE,

    -- Severity
    severity                     TEXT NOT NULL
        CHECK (severity IN ('fatal', 'serious', 'minor', 'none', 'unknown')),
    source_severity_raw          TEXT,                  -- original value/derivation, for audit
    num_injured_total            INTEGER CHECK (num_injured_total >= 0),
    num_killed                   INTEGER CHECK (num_killed >= 0),  -- NULL for uk_stats19 (see adapter notes)
    num_vehicles_involved        INTEGER CHECK (num_vehicles_involved >= 0),

    -- Circumstances (structurally absent for some sources - see data dictionary)
    crash_type                   TEXT,                  -- NULL for nyc_us, uk_stats19
    primary_cause                TEXT,                  -- NULL for uk_stats19
    weather_condition            TEXT,                  -- NULL for nyc_us
    lighting_condition           TEXT,                  -- NULL for nyc_us
    posted_speed_limit_mph       INTEGER,                -- NULL for nyc_us
    hit_and_run                  BOOLEAN NOT NULL DEFAULT FALSE,  -- always FALSE for nyc_us, uk_stats19 (no source signal)

    -- Ingestion metadata
    ingested_at_utc               TIMESTAMPTZ NOT NULL DEFAULT now(),
    batch_id                      TEXT NOT NULL,
    source_row_raw_ref             TEXT,                 -- pointer back to raw-layer file/row, for lineage

    CONSTRAINT uq_fact_crash_source_natural_key UNIQUE (source_id, source_record_id)
);

COMMENT ON TABLE fact_crash IS
    'Canonical, cross-source curated crash table. One row per crash, '
    'harmonized from chicago_us / nyc_us / uk_stats19 via their adapters. '
    'Several columns are legitimately NULL depending on source - see '
    'data_dictionary.md for the full per-field, per-source availability matrix.';

COMMENT ON COLUMN fact_crash.crash_id IS
    'Surrogate key: source_id || '':'' || source_record_id. Globally unique '
    'across sources by construction.';
COMMENT ON COLUMN fact_crash.num_killed IS
    'NULL for uk_stats19: STATS19 casualty-level fatality counts live in a '
    'separate, unjoined table not included in this pipeline.';
COMMENT ON COLUMN fact_crash.hit_and_run IS
    'Reliable only for chicago_us, which has an explicit source flag. '
    'Always FALSE for nyc_us and uk_stats19 due to no source signal - '
    'do not compare hit-and-run rates across sources without accounting '
    'for this.';

-- Indexes supporting the pipeline's actual access patterns: partition-style
-- reads by source + time window, and spatial filtering for clustering/EDA.
CREATE INDEX idx_fact_crash_source_date ON fact_crash (source_id, crash_date_key);
CREATE INDEX idx_fact_crash_date ON fact_crash (crash_date_key);
CREATE INDEX idx_fact_crash_coords ON fact_crash (latitude, longitude)
    WHERE has_valid_coordinates;
CREATE INDEX idx_fact_crash_severity ON fact_crash (severity);
CREATE INDEX idx_fact_crash_batch ON fact_crash (batch_id);

-- ============================================================
-- dq_run_log: validation outcomes per batch, per check
-- ============================================================
CREATE TABLE dq_run_log (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    source_id       TEXT REFERENCES dim_source(source_id),
    check_name      TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pass', 'fail', 'warn')),
    rows_checked    BIGINT NOT NULL DEFAULT 0,
    rows_failed     BIGINT NOT NULL DEFAULT 0,
    details         TEXT,                        -- free-text summary or sample of failures
    run_timestamp   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE dq_run_log IS
    'One row per data-quality check executed per pipeline batch. Airflow '
    'tasks write here so validation results are queryable and visible in '
    'task logs, per the project''s data-quality requirement (5+ checks).';

CREATE INDEX idx_dq_run_log_batch ON dq_run_log (batch_id);
CREATE INDEX idx_dq_run_log_status ON dq_run_log (status) WHERE status != 'pass';

-- ============================================================
-- Seed dim_source with the three configured adapters
-- ============================================================
INSERT INTO dim_source (source_id, provider, dataset_url, city, country, source_local_timezone, license) VALUES
    ('chicago_us', 'City of Chicago Data Portal (Socrata)',
     'https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if',
     'Chicago', 'US', 'America/Chicago', 'Public domain / open data'),
    ('nyc_us', 'NYC OpenData (Socrata)',
     'https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95',
     'New York City', 'US', 'America/New_York', 'Public domain / open data'),
    ('uk_stats19', 'UK Department for Transport (data.gov.uk)',
     'https://www.data.gov.uk/dataset/road-accidents-safety-data',
     NULL, 'GB', 'Europe/London', 'Open Government Licence v3.0');

COMMIT;

-- ============================================================
-- Representative queries
-- ============================================================

-- 1. Crashes per source per year (basic volume check across sources)
-- SELECT source_id, EXTRACT(YEAR FROM crash_date_key) AS year, COUNT(*)
-- FROM fact_crash
-- GROUP BY source_id, year
-- ORDER BY source_id, year;

-- 2. Fatal crash rate by source, coordinates-only subset (for mapping)
-- SELECT source_id,
--        COUNT(*) FILTER (WHERE severity = 'fatal') AS fatal_count,
--        COUNT(*) AS total_count,
--        ROUND(100.0 * COUNT(*) FILTER (WHERE severity = 'fatal') / COUNT(*), 3) AS fatal_pct
-- FROM fact_crash
-- WHERE has_valid_coordinates
-- GROUP BY source_id;

-- 3. Most recent data-quality failures across all sources
-- SELECT batch_id, source_id, check_name, rows_failed, run_timestamp
-- FROM dq_run_log
-- WHERE status = 'fail'
-- ORDER BY run_timestamp DESC
-- LIMIT 50;

-- 4. Crashes usable for spatial clustering (has_valid_coordinates only),
--    scoped to a single source and year - demonstrates partition-style
--    access via the composite index rather than a full table scan.
-- SELECT crash_id, latitude, longitude, severity
-- FROM fact_crash
-- WHERE source_id = 'chicago_us'
--   AND crash_date_key BETWEEN '2024-01-01' AND '2024-12-31'
--   AND has_valid_coordinates;
