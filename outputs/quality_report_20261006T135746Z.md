# Data Quality Report

Checks run since 2026-10-06T13:26:26.592589+00:00

**22 pass, 2 warn, 0 fail** across 3 source(s).

| Source | Batch | Check | Status | Rows checked | Rows failed | Details |
|---|---|---|---|---:|---:|---|
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | schema_check | PASS | 1,096,581 | 0 | All 25 canonical fields present |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | nullability_check | PASS | 1,096,581 | 0 | No nulls in 12 non-nullable fields |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | uniqueness_check | PASS | 1,096,581 | 0 | All keys unique within batch |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | accepted_values_check | PASS | 1,096,581 | 0 | All accepted-value constraints satisfied |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | range_check | WARN | 1,096,581 | 8,320 | Flagged (soft): {'posted_speed_limit_mph': 8320} |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | date_logic_check | WARN | 1,096,581 | 1 | 1 rows with inverted timestamps (within tolerance=5; documented anomaly) |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | referential_check | PASS | 1,096,581 | 0 | All source_id values (['chicago_us']) known |
| chicago_us | chicago_us_20261006T132750Z_a20a6dd8 | row_count_check | PASS | 1,096,581 | 0 | Staged rows: 1,096,581 \| raw manifest total: 1,096,581 |
| nyc_us | nyc_us_20261006T132635Z_59787a95 | schema_check | PASS | 20,537 | 0 | All 25 canonical fields present |
| nyc_us | nyc_us_20261006T132635Z_59787a95 | nullability_check | PASS | 20,537 | 0 | No nulls in 12 non-nullable fields |
| nyc_us | nyc_us_20261006T132635Z_59787a95 | uniqueness_check | PASS | 20,537 | 0 | All keys unique within batch |
| nyc_us | nyc_us_20261006T132635Z_59787a95 | accepted_values_check | PASS | 20,537 | 0 | All accepted-value constraints satisfied |
| nyc_us | nyc_us_20261006T132635Z_59787a95 | range_check | PASS | 20,537 | 0 | All numeric ranges within bounds |
| nyc_us | nyc_us_20261006T132635Z_59787a95 | date_logic_check | PASS | 20,537 | 0 | Skipped: police_notified_timestamp_utc is a structural gap |
| nyc_us | nyc_us_20261006T132635Z_59787a95 | referential_check | PASS | 20,537 | 0 | All source_id values (['nyc_us']) known |
| nyc_us | nyc_us_20261006T132635Z_59787a95 | row_count_check | PASS | 20,537 | 0 | Staged rows: 20,537 \| raw manifest total: 20,537 |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | schema_check | PASS | 513,801 | 0 | All 25 canonical fields present |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | nullability_check | PASS | 513,801 | 0 | No nulls in 11 non-nullable fields |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | uniqueness_check | PASS | 513,801 | 0 | All keys unique within batch |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | accepted_values_check | PASS | 513,801 | 0 | All accepted-value constraints satisfied |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | range_check | PASS | 513,801 | 0 | All numeric ranges within bounds |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | date_logic_check | PASS | 513,801 | 0 | Skipped: police_notified_timestamp_utc is a structural gap |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | referential_check | PASS | 513,801 | 0 | All source_id values (['uk_stats19']) known |
| uk_stats19 | uk_stats19_20261006T132630Z_d683af3f | row_count_check | PASS | 513,801 | 0 | Staged rows: 513,801 \| raw manifest total: 513,801 |
