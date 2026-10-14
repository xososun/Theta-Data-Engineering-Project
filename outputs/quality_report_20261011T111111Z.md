# Data Quality Report

Checks run since 2026-10-11T10:45:50.384053+00:00

**37 pass, 2 warn, 0 fail** across 3 source(s).

| Source | Batch | Check | Status | Rows checked | Rows failed | Details |
|---|---|---|---|---:|---:|---|
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | source_freshness | PASS | 1,096,581 | 0 | UNCHANGED: Traffic_Crashes_-_Crashes_20260923.csv is byte-identical to batch chicago_us_20261006T132750Z_a20a6dd8 (first ingested 4 days ago). No new raw copy written; the batch is re-staged and re-va |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | schema_check | PASS | 1,096,581 | 0 | All 25 canonical fields present |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | nullability_check | PASS | 1,096,581 | 0 | No nulls in 12 non-nullable fields |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | uniqueness_check | PASS | 1,096,581 | 0 | All keys unique within batch |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | accepted_values_check | PASS | 1,096,581 | 0 | All accepted-value constraints satisfied |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | range_check | WARN | 1,096,581 | 8,320 | Flagged (soft): {'posted_speed_limit_mph': 8320} |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | date_logic_check | WARN | 1,096,581 | 1 | 1 rows with inverted timestamps (within tolerance=5; documented anomaly) |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | referential_check | PASS | 1,096,581 | 0 | All source_id values (['chicago_us']) known |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | row_count_check | PASS | 1,096,581 | 0 | Staged rows: 1,096,581 \| raw manifest total: 1,096,581 |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | curated_reconciliation_check | PASS | 1,096,581 | 0 | staged 1,096,581 = curated 1,096,395 + excluded 186 (0.017%, no valid UTC timestamp); batch chicago_us_20261006T132750Z_a20a6dd8 |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | curated_timestamp_check | PASS | 1,096,395 | 0 | null=0, before 2000-01-01=0, after run time=0, crash_date_key != UTC date=0; range 2013-03-03 22:48:00+00:00 to 2026-09-22 07:55:00+00:00 |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | curated_partition_check | PASS | 1,096,395 | 0 | 147 files in 147 year/month partitions, 1,096,395 rows |
| nyc_us | nyc_us_20261011T104553Z_30081487 | source_freshness | PASS | 20,537 | 0 | API pull: window 2026-03-13 to 2026-06-11; newest published crash date 2026-06-11 (122 days before this run). |
| nyc_us | nyc_us_20261011T104553Z_30081487 | schema_check | PASS | 20,537 | 0 | All 25 canonical fields present |
| nyc_us | nyc_us_20261011T104553Z_30081487 | nullability_check | PASS | 20,537 | 0 | No nulls in 12 non-nullable fields |
| nyc_us | nyc_us_20261011T104553Z_30081487 | uniqueness_check | PASS | 20,537 | 0 | All keys unique within batch |
| nyc_us | nyc_us_20261011T104553Z_30081487 | accepted_values_check | PASS | 20,537 | 0 | All accepted-value constraints satisfied |
| nyc_us | nyc_us_20261011T104553Z_30081487 | range_check | PASS | 20,537 | 0 | All numeric ranges within bounds |
| nyc_us | nyc_us_20261011T104553Z_30081487 | date_logic_check | PASS | 20,537 | 0 | Skipped: police_notified_timestamp_utc is a structural gap |
| nyc_us | nyc_us_20261011T104553Z_30081487 | referential_check | PASS | 20,537 | 0 | All source_id values (['nyc_us']) known |
| nyc_us | nyc_us_20261011T104553Z_30081487 | row_count_check | PASS | 20,537 | 0 | Staged rows: 20,537 \| raw manifest total: 20,537 |
| nyc_us | nyc_us_20261011T104553Z_30081487 | curated_reconciliation_check | PASS | 20,537 | 0 | staged 20,537 = curated 20,537 + excluded 0 (0.000%, no valid UTC timestamp); batch nyc_us_20261011T104553Z_30081487 |
| nyc_us | nyc_us_20261011T104553Z_30081487 | curated_timestamp_check | PASS | 20,537 | 0 | null=0, before 2000-01-01=0, after run time=0, crash_date_key != UTC date=0; range 2026-03-13 04:00:00+00:00 to 2026-06-11 17:08:00+00:00 |
| nyc_us | nyc_us_20261011T104553Z_30081487 | curated_partition_check | PASS | 20,537 | 0 | 4 files in 4 year/month partitions, 20,537 rows |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | source_freshness | PASS | 513,801 | 0 | UNCHANGED: dft-road-casualty-statistics-collision-2021.csv, dft-road-casualty-statistics-collision-2022.csv, dft-road-casualty-statistics-collision-2023.csv, dft-road-casualty-statistics-collision-202 |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | schema_check | PASS | 513,801 | 0 | All 25 canonical fields present |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | nullability_check | PASS | 513,801 | 0 | No nulls in 11 non-nullable fields |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | uniqueness_check | PASS | 513,801 | 0 | All keys unique within batch |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | accepted_values_check | PASS | 513,801 | 0 | All accepted-value constraints satisfied |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | range_check | PASS | 513,801 | 0 | All numeric ranges within bounds |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | date_logic_check | PASS | 513,801 | 0 | Skipped: police_notified_timestamp_utc is a structural gap |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | referential_check | PASS | 513,801 | 0 | All source_id values (['uk_stats19']) known |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | row_count_check | PASS | 513,801 | 0 | Staged rows: 513,801 \| raw manifest total: 513,801 |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | curated_reconciliation_check | PASS | 513,801 | 0 | staged 513,801 = curated 513,732 + excluded 69 (0.013%, no valid UTC timestamp); batch uk_stats19_20261006T132630Z_d683af3f |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | curated_timestamp_check | PASS | 513,732 | 0 | null=0, before 2000-01-01=0, after run time=0, crash_date_key != UTC date=0; range 2021-01-01 00:17:00+00:00 to 2025-12-31 23:59:00+00:00 |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | curated_partition_check | PASS | 513,732 | 0 | 60 files in 60 year/month partitions, 513,732 rows |
| all sources | curated_20261011T110042Z | curated_schema_check | PASS | 1,630,664 | 0 | All 25 canonical fields + crash_date_key present with expected types; partitioned by source_id/year/month |
| all sources | curated_20261011T110042Z | curated_uniqueness_check | PASS | 1,630,664 | 0 | Across all sources: duplicate (source_id, source_record_id)=0, duplicate crash_id=0, crash_id not '<source_id>:<source_record_id>'=0 |
| all sources | curated_20261011T110042Z | curated_partition_check | PASS | 1,630,664 | 0 | 211 partition files across 3 sources (matches harmonize) |
