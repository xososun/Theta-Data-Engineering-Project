"""
crash_pipeline.py

Airflow DAG for the cross-city road crash data pipeline.

Implements the 8-stage chain from the project proposal (section 10):

    extract -> raw_validate -> stage -> validate -> harmonize
    -> load_postgres -> publish_parquet -> quality_report

One branch per configured source (chicago_us, nyc_us, uk_stats19), using
Airflow's dynamic task mapping. A failure in one source's branch does not
cancel the other sources' branches; failures are visible per-source in
the Airflow UI and in dq_run_log.

Status of each stage as of this file's writing:

- extract:            REAL — calls Member A's extractors under src/extract/.
- raw_validate:       STUB — raises NotImplementedError until Member B's code lands.
- stage:              STUB
- validate:           STUB
- harmonize:          STUB
- load_postgres:      STUB
- publish_parquet:    STUB
- quality_report:     STUB

The DAG imports stubs from dags/_stubs.py so it parses without Member B's
modules existing. When B's real modules land, swap the import block below
to point at them directly (and delete _stubs.py, or keep it as a local-dev
fallback). Do NOT edit the task definitions to add logic — the DAG is a
scheduler; transformation logic belongs in the imported modules.

Paths are CONTAINER paths (/opt/airflow/...) because the DAG runs inside
the Airflow image defined by Dockerfile. Do not change them to host paths.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.trigger_rule import TriggerRule

# The Airflow image mounts ./src and ./config into /opt/airflow/, and copies
# them in at build time (see Dockerfile). Ensure they're importable.
sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(0, "/opt/airflow/dags")

# Member A's extractors (real, verified).
from extract.chicago_extractor import run as run_chicago_extract
from extract.nyc_extractor import run as run_nyc_extract
from extract.uk_extractor import run as run_uk_extract

# Member B's stages — currently stubs. Swap this import when B lands.
from _stubs import (
    validate_raw_batch,
    stage_source,
    validate_staged,
    harmonize_to_canonical,
    load_postgres,
    publish_parquet,
    build_quality_report,
    cleanup_failed_batch,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# The three configured sources. Adding a source means adding its source_id
# here AND a matching config/adapters/<source_id>.yaml — no DAG code changes.
SOURCES: list[str] = ["chicago_us", "nyc_us", "uk_stats19"]

# Container-side roots, matching docker-compose.yml's volume mounts.
RAW_ROOT = "/opt/airflow/data/raw"
STAGING_ROOT = "/opt/airflow/data/staging"
CURATED_ROOT = "/opt/airflow/data/curated"
INCOMING_ROOT = "/opt/airflow/data/incoming"
CONFIG_DIR = "/opt/airflow/config/adapters"
CANONICAL_SCHEMA_PATH = "/opt/airflow/config/canonical_schema.yaml"

# NYC extraction window. The proposal (section 9) says incremental runs
# re-pull a trailing window for late amendments; 90 days is the proposed
# value. Adjust here, not in the extractor.
NYC_TRAILING_WINDOW_DAYS = 90

# Data-quality checks to run in the validate stage. The check *names* match
# proposal section 8's seven checks; Member B's validate_staged is
# responsible for implementing them.
DQ_CHECKS = [
    "schema",
    "nullability",
    "uniqueness",
    "accepted_values",
    "range",
    "row_count_reconciliation",
    "referential_integrity",
]

# Postgres connection details for the curated database, read from env vars
# set in docker-compose.yml (PIPELINE_POSTGRES_*). Read at task-execution
# time, not at DAG-parse time, so a missing env var does not break parsing.
def _db_conn() -> dict:
    return {
        "host": os.environ.get("PIPELINE_POSTGRES_HOST", "postgres"),
        "port": int(os.environ.get("PIPELINE_POSTGRES_PORT", "5432")),
        "dbname": os.environ.get("PIPELINE_POSTGRES_DB", "crash_pipeline"),
        "user": os.environ.get("PIPELINE_POSTGRES_USER", "pipeline_user"),
        "password": os.environ.get("PIPELINE_POSTGRES_PASSWORD", ""),
    }


# ---------------------------------------------------------------------------
# DAG defaults
# ---------------------------------------------------------------------------

default_args = {
    "owner": "member-c",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    # NYC's extractor already implements its own retry+backoff for
    # transient API errors; a short DAG-level retry covers everything else
    # (transient infra hiccups on stage/load).
    "retry_delay": timedelta(minutes=2),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="crash_pipeline",
    description="Cross-city road crash ingestion, validation, and curation",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * 1",  # weekly, Mondays at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["crash", "pipeline", "cross-city"],
) as dag:

    # -----------------------------------------------------------------
    # Per-source branch. Everything below is defined once and mapped
    # across SOURCES by dynamic task mapping — not copy-pasted per source.
    # -----------------------------------------------------------------

    @task
    def extract(source_id: str) -> dict:
        """
        Dispatch to the correct extractor for this source. Runs for real —
        Member A's extractors are verified end-to-end.

        Returns a dict with batch_id and row_count; downstream tasks take
        source_id as their first argument and pull the rest from config /
        environment, so the XCom payload stays small.
        """
        if source_id == "chicago_us":
            # Chicago requires a downloaded CSV in data/incoming/. The
            # extractor fails with a clean error if not present — that's
            # the intended behavior; the DAG should not silently skip a
            # missing source.
            chicago_csv = os.environ.get(
                "CHICAGO_CSV",
                f"{INCOMING_ROOT}/Traffic_Crashes_-_Crashes_20260923.csv",
            )
            result = run_chicago_extract(
                input_path=chicago_csv,
                config_path=f"{CONFIG_DIR}/chicago.yaml",
                raw_root=RAW_ROOT,
            )
        elif source_id == "nyc_us":
            # Trailing window for late amendments — see proposal section 9.
            today = datetime.utcnow().date()
            since = (today - timedelta(days=NYC_TRAILING_WINDOW_DAYS)).isoformat()
            until = today.isoformat()
            result = run_nyc_extract(
                since=since,
                until=until,
                config_path=f"{CONFIG_DIR}/nyc.yaml",
                raw_root=RAW_ROOT,
                max_rows=None,
            )
        elif source_id == "uk_stats19":
            result = run_uk_extract(
                input_dir=INCOMING_ROOT,
                years=[2021, 2022, 2023, 2024, 2025],
                config_path=f"{CONFIG_DIR}/uk.yaml",
                raw_root=RAW_ROOT,
            )
        else:
            raise ValueError(f"Unknown source_id: {source_id}")

        # Normalize the per-extractor return shape to a common payload.
        # Extractor returns differ: chicago has row_count, uk has
        # total_rows, nyc has total_rows.
        row_count = (
            result.get("row_count")
            or result.get("total_rows")
            or result.get("total_rows", 0)
        )
        return {
            "source_id": source_id,
            "batch_id": result["batch_id"],
            "row_count": row_count,
            "manifest_path": result.get("manifest_path"),
        }

    @task
    def raw_validate(source_id: str, extract_result: dict) -> dict:
        return validate_raw_batch(
            source_id=source_id,
            batch_id=extract_result["batch_id"],
            raw_root=RAW_ROOT,
        )

    @task
    def stage(source_id: str, raw_validate_result: dict, extract_result: dict) -> dict:
        return stage_source(
            source_id=source_id,
            batch_id=extract_result["batch_id"],
            raw_root=RAW_ROOT,
            staging_root=STAGING_ROOT,
            config_path=f"{CONFIG_DIR}/{source_id}.yaml",
        )

    @task
    def validate(source_id: str, stage_result: dict, extract_result: dict) -> dict:
        return validate_staged(
            source_id=source_id,
            batch_id=extract_result["batch_id"],
            staging_root=STAGING_ROOT,
            checks=DQ_CHECKS,
            dq_run_log_conn=_db_conn(),
        )

    @task
    def harmonize(source_id: str, validate_result: dict, extract_result: dict) -> dict:
        return harmonize_to_canonical(
            source_id=source_id,
            batch_id=extract_result["batch_id"],
            staging_root=STAGING_ROOT,
            curated_root=CURATED_ROOT,
            canonical_schema_path=CANONICAL_SCHEMA_PATH,
            config_path=f"{CONFIG_DIR}/{source_id}.yaml",
        )

    @task
    def load_postgres_task(source_id: str, harmonize_result: dict, extract_result: dict) -> dict:
        return load_postgres(
            source_id=source_id,
            batch_id=extract_result["batch_id"],
            curated_root=CURATED_ROOT,
            db_conn=_db_conn(),
        )

    @task
    def publish_parquet_task(source_id: str, load_result: dict, extract_result: dict) -> dict:
        return publish_parquet(
            source_id=source_id,
            batch_id=extract_result["batch_id"],
            curated_root=CURATED_ROOT,
            publish_root=CURATED_ROOT,
        )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def quality_report(source_id: str, extract_result: dict) -> dict:
        """
        Summarize dq_run_log for this batch. Runs even if an earlier stage
        failed (ALL_DONE), so a partial run still produces a report — this
        is the stage that tells a human what went wrong.
        """
        return build_quality_report(
            source_id=source_id,
            batch_id=extract_result["batch_id"],
            dq_run_log_conn=_db_conn(),
            report_root="/opt/airflow/data/quality_reports",
        )

    # Per-source task groups. Explicit task groups (rather than bare
    # mapped tasks) so the Airflow UI groups each source's stages into a
    # collapsible unit — important when 3 sources x 8 stages = 24 tasks
    # are visible at once.
    @task_group(group_id="per_source")
    def per_source_branch(source_id: str):
        e = extract(source_id)
        rv = raw_validate(source_id, e)
        s = stage(source_id, rv, e)
        v = validate(source_id, s, e)
        h = harmonize(source_id, v, e)
        lp = load_postgres_task(source_id, h, e)
        pp = publish_parquet_task(source_id, lp, e)
        qr = quality_report(source_id, e)
        # quality_report is ALL_DONE, so it isn't chained on pp — it runs
        # once extract's batch_id is known, regardless of upstream success.
        pp >> qr  # declare this edge for UI clarity; ALL_DONE makes it non-blocking

    # Fan out across sources using dynamic task mapping.
    per_source_branch.expand(source_id=SOURCES)