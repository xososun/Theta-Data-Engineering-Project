-- =============================================================================
-- queries.sql
--
-- Representative queries against the curated PostgreSQL schema
-- (sql/schema.sql). Each query states what it answers and any structural
-- gap it has to respect (docs/data_contract.md, section 4).
--
-- Run one query at a time, or the whole file:
--   docker compose exec -T postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB> < sql/queries.sql
--
-- Time zones: crash_timestamp_utc is UTC, and crash_date_key / dim_date
-- use the UTC date. Queries about the time of day or day of week convert to
-- each source's local time with AT TIME ZONE dim_source.source_local_timezone.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Load verification: rows per source, and how many batches they came from.
--    The totals should match the pipeline's load log.
-- -----------------------------------------------------------------------------
SELECT
    f.source_id,
    COUNT(*)                   AS crashes,
    COUNT(DISTINCT f.batch_id) AS batches_loaded,
    MIN(f.crash_timestamp_utc) AS earliest_crash_utc,
    MAX(f.crash_timestamp_utc) AS latest_crash_utc
FROM fact_crash f
GROUP BY f.source_id
ORDER BY f.source_id;


-- -----------------------------------------------------------------------------
-- 2. Rerun safety: no natural key appears twice.
--    Expected result: zero rows. The UNIQUE constraint already guarantees
--    this; the query is the evidence to show after rerunning the DAG.
-- -----------------------------------------------------------------------------
SELECT source_id, source_record_id, COUNT(*) AS copies
FROM fact_crash
GROUP BY source_id, source_record_id
HAVING COUNT(*) > 1;


-- -----------------------------------------------------------------------------
-- 3. Crashes per year per source (join to dim_date).
--    Chicago before 2018 is partial coverage (districts were onboarded
--    gradually), so start citywide trends at 2018.
-- -----------------------------------------------------------------------------
SELECT
    d.year,
    COUNT(*) FILTER (WHERE f.source_id = 'chicago_us') AS chicago_us,
    COUNT(*) FILTER (WHERE f.source_id = 'nyc_us')     AS nyc_us,
    COUNT(*) FILTER (WHERE f.source_id = 'uk_stats19') AS uk_stats19
FROM fact_crash f
JOIN dim_date d ON d.date_key = f.crash_date_key
GROUP BY d.year
ORDER BY d.year;


-- -----------------------------------------------------------------------------
-- 4. Hour-of-day profile in LOCAL time, as a share of each source's crashes.
--    Converts with each source's own timezone from dim_source, and uses a
--    window function so the three very different volumes are comparable.
-- -----------------------------------------------------------------------------
SELECT
    f.source_id,
    EXTRACT(HOUR FROM f.crash_timestamp_utc AT TIME ZONE s.source_local_timezone)::int AS local_hour,
    COUNT(*) AS crashes,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY f.source_id), 2) AS pct_of_source
FROM fact_crash f
JOIN dim_source s ON s.source_id = f.source_id
GROUP BY f.source_id, local_hour
ORDER BY f.source_id, local_hour;


-- -----------------------------------------------------------------------------
-- 5. Day-of-week profile in LOCAL time (ISO: 1 = Monday ... 7 = Sunday).
--    dim_date.day_of_week is not used here because it is based on the UTC
--    date, which moves evening crashes in Chicago and NYC to the next day.
-- -----------------------------------------------------------------------------
SELECT
    f.source_id,
    EXTRACT(ISODOW FROM f.crash_timestamp_utc AT TIME ZONE s.source_local_timezone)::int AS local_isodow,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY f.source_id), 2) AS pct_of_source
FROM fact_crash f
JOIN dim_source s ON s.source_id = f.source_id
GROUP BY f.source_id, local_isodow
ORDER BY f.source_id, local_isodow;


-- -----------------------------------------------------------------------------
-- 6. Severity mix per source.
--    GAP: nyc_us cannot produce 'serious' (no categorical severity field), and
--    uk_stats19 has no 'none' (STATS19 records injury collisions only). These
--    shares describe what each source records, not comparable danger levels.
-- -----------------------------------------------------------------------------
SELECT
    source_id,
    severity,
    COUNT(*) AS crashes,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY source_id), 2) AS pct_of_source
FROM fact_crash
GROUP BY source_id, severity
ORDER BY source_id,
         ARRAY_POSITION(ARRAY['fatal', 'serious', 'minor', 'none', 'unknown'], severity);


-- -----------------------------------------------------------------------------
-- 7. Fatal share among INJURY crashes only: the closest like-for-like rule.
--    Restricting every source to crashes with an injury puts Chicago and NYC
--    on the same footing as STATS19, which only records injury collisions.
-- -----------------------------------------------------------------------------
SELECT
    source_id,
    COUNT(*)                                             AS injury_crashes,
    COUNT(*) FILTER (WHERE severity = 'fatal')           AS fatal_crashes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE severity = 'fatal') / COUNT(*), 3) AS pct_fatal
FROM fact_crash
WHERE severity IN ('fatal', 'serious', 'minor')
GROUP BY source_id
ORDER BY pct_fatal DESC;


-- -----------------------------------------------------------------------------
-- 8. Chicago: most common primary causes of fatal and serious crashes.
--    primary_cause and the 'serious' level both exist for Chicago; UK has no
--    primary_cause and NYC has no 'serious', so this is single-source.
-- -----------------------------------------------------------------------------
SELECT
    primary_cause,
    COUNT(*) AS fatal_or_serious_crashes,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM fact_crash
WHERE source_id = 'chicago_us'
  AND severity IN ('fatal', 'serious')
GROUP BY primary_cause
ORDER BY fatal_or_serious_crashes DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 9. Chicago monthly trend with a 3-month rolling average (window function).
--    Starts at 2018 to skip the partial-coverage ramp-up years.
-- -----------------------------------------------------------------------------
WITH monthly AS (
    SELECT DATE_TRUNC('month', f.crash_date_key)::date AS month,
           COUNT(*)                                   AS crashes
    FROM fact_crash f
    WHERE f.source_id = 'chicago_us'
      AND f.crash_date_key >= DATE '2018-01-01'
    GROUP BY 1
)
SELECT
    month,
    crashes,
    ROUND(AVG(crashes) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0)
        AS rolling_3_month_avg
FROM monthly
ORDER BY month;


-- -----------------------------------------------------------------------------
-- 10. Coordinate coverage, and a spatial bounding-box query.
--     Always filter on has_valid_coordinates, never on "latitude IS NOT NULL":
--     the (0,0) sentinel rows are nulled at staging and flagged FALSE here.
--     The box below covers downtown Chicago (around the Loop).
-- -----------------------------------------------------------------------------
SELECT
    source_id,
    ROUND(100.0 * AVG(has_valid_coordinates::int), 2) AS pct_valid_coordinates
FROM fact_crash
GROUP BY source_id
ORDER BY source_id;

SELECT severity, COUNT(*) AS crashes
FROM fact_crash
WHERE source_id = 'chicago_us'
  AND has_valid_coordinates
  AND latitude  BETWEEN 41.870 AND 41.890
  AND longitude BETWEEN -87.640 AND -87.620
GROUP BY severity
ORDER BY crashes DESC;


-- -----------------------------------------------------------------------------
-- 11. Selective read: one source, one month.
--     The EXPLAIN shows the planner using idx_fact_crash_source_date (on
--     source_id, crash_date_key) instead of scanning the whole table: the
--     Postgres counterpart of reading a single Parquet partition.
-- -----------------------------------------------------------------------------
EXPLAIN
SELECT COUNT(*)
FROM fact_crash
WHERE source_id = 'chicago_us'
  AND crash_date_key >= DATE '2024-03-01'
  AND crash_date_key <  DATE '2024-04-01';


-- -----------------------------------------------------------------------------
-- 12. Trace one record back through the pipeline.
--     Shows the row, the raw batch folder it came from, and the data-quality
--     results for that batch. Replace the LIMIT 1 sample with a specific
--     crash_id (e.g. WHERE crash_id = 'chicago_us:<CRASH_RECORD_ID>') to trace
--     a chosen record.
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT crash_id, source_id, source_record_id, batch_id,
           crash_timestamp_utc, severity, ingested_at_utc
    FROM fact_crash
    WHERE source_id = 'chicago_us'
    ORDER BY crash_id
    LIMIT 1
)
SELECT
    t.crash_id,
    t.source_record_id,
    t.crash_timestamp_utc,
    t.severity,
    t.batch_id,
    'data/raw/' || t.source_id || '/' || t.batch_id || '/manifest.json' AS raw_manifest_path,
    q.check_name,
    q.status,
    q.rows_failed
FROM target t
LEFT JOIN dq_run_log q ON q.batch_id = t.batch_id
ORDER BY q.check_name;


-- -----------------------------------------------------------------------------
-- 13. Latest data-quality result per source and check (DISTINCT ON).
--     Anything other than 'pass' is listed with its details.
-- -----------------------------------------------------------------------------
SELECT DISTINCT ON (source_id, check_name)
    source_id,
    check_name,
    status,
    rows_checked,
    rows_failed,
    CASE WHEN status <> 'pass' THEN LEFT(details, 120) END AS details,
    run_timestamp
FROM dq_run_log
ORDER BY source_id, check_name, run_timestamp DESC;
