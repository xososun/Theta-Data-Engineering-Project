"""
crash_pipeline.py

Airflow DAG for the cross-city road crash data pipeline.

Shape:

    per source, in parallel (dynamic task mapping over SOURCES):
        extract -> stage -> validate
                              |
                  (all three sources must succeed)
                              v
    once:          harmonize -> load -> quality_report (ALL_DONE)

Why the fan-in: stage() and run_checks() work on one source's batch, but
harmonize() reads the newest staged batch of EVERY source and rewrites the
whole curated layer, and load() loads all of curated. Running those two
per source would have three tasks rewriting the same folder at once.

The DAG is a scheduler only. Every task calls a function from src/, so
each stage can also be run by hand (see README "Running stages manually").
Heavy imports (pandas, pyarrow, psycopg2) happen inside the tasks rather
than at the top of the file, so the scheduler can parse this file quickly
and a missing library fails one task with a clear error instead of making
the whole DAG vanish from the UI.

Paths are CONTAINER paths (/opt/airflow/...) because the DAG runs inside
the Airflow image defined by the Dockerfile.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

# docker-compose mounts ./src into /opt/airflow/src; make it importable.
sys.path.insert(0, "/opt/airflow/src")

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Adding a source means adding its source_id here, a matching
# config/adapters/*.yaml with that top-level source_id, an extractor branch
# in extract() below, and a dim_source row in sql/schema.sql.
SOURCES: list[str] = ["chicago_us", "nyc_us", "uk_stats19"]

# Extractor config files are named by city, not by source_id.
ADAPTER_FILES = {
    "chicago_us": "chicago.yaml",
    "nyc_us": "nyc.yaml",
    "uk_stats19": "uk.yaml",
}

# Same environment variables src/utils/paths.py reads, so the DAG and the
# pipeline modules always agree on where each layer lives.
RAW_ROOT = os.environ.get("PIPELINE_RAW_DATA_ROOT", "/opt/airflow/data/raw")
INCOMING_ROOT = "/opt/airflow/data/incoming"
CONFIG_DIR = "/opt/airflow/config/adapters"

# NYC incremental runs re-pull a trailing window to pick up late
# amendments (proposal section 9). Adjust here, not in the extractor.
NYC_TRAILING_WINDOW_DAYS = 90

UK_YEARS = [2021, 2022, 2023, 2024, 2025]


# ---------------------------------------------------------------------------
# DAG defaults
# ---------------------------------------------------------------------------

default_args = {
    "owner": "theta",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Retries cover transient problems (a network blip on the NYC API, a
    # Postgres connection drop). NYC's extractor also has its own
    # retry + backoff for API errors.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="crash_pipeline",
    description="Cross-city road crash ingestion, validation, and curation",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * 1",  # weekly, Mondays at 06:00 UTC
    catchup=False,
    max_active_runs=1,  # overlapping runs would race on curated/ and the UPSERT
    tags=["crash", "pipeline", "cross-city"],
) as dag:

    # -----------------------------------------------------------------
    # Per-source tasks (mapped across SOURCES)
    # -----------------------------------------------------------------

    @task
    def extract(source_id: str) -> dict:
        """Run this source's extractor; returns its batch_id for later tasks."""
        config_path = f"{CONFIG_DIR}/{ADAPTER_FILES[source_id]}"

        if source_id == "chicago_us":
            from extract.chicago_extractor import run
            # Fails with a clear error if the CSV is not in data/incoming/.
            # Deliberate: a missing source should fail visibly, not be skipped.
            result = run(
                input_path=os.environ.get(
                    "CHICAGO_CSV",
                    f"{INCOMING_ROOT}/Traffic_Crashes_-_Crashes_20260923.csv",
                ),
                config_path=config_path,
                raw_root=RAW_ROOT,
            )
        elif source_id == "nyc_us":
            from extract.nyc_extractor import run
            today = datetime.utcnow().date()
            result = run(
                since=(today - timedelta(days=NYC_TRAILING_WINDOW_DAYS)).isoformat(),
                until=today.isoformat(),
                config_path=config_path,
                raw_root=RAW_ROOT,
                max_rows=None,
            )
        elif source_id == "uk_stats19":
            from extract.uk_extractor import run
            result = run(
                input_dir=INCOMING_ROOT,
                years=UK_YEARS,
                config_path=config_path,
                raw_root=RAW_ROOT,
            )
        else:
            raise ValueError(f"Unknown source_id: {source_id}")

        # Extractors name the count differently (row_count vs total_rows).
        row_count = result.get("row_count", result.get("total_rows", 0))
        log.info("[extract] %s batch=%s rows=%s", source_id, result["batch_id"], row_count)
        return {
            "source_id": source_id,
            "batch_id": result["batch_id"],
            "row_count": row_count,
        }

    @task
    def stage(extract_result: dict) -> dict:
        """Raw -> staging: apply the adapter's field_mapping to this batch."""
        from transform.staging import stage as run_stage
        return run_stage(extract_result["source_id"], extract_result["batch_id"])

    # No retries: a failed data-quality check is deterministic, so retrying
    # would only fail again and add duplicate rows to dq_run_log.
    @task(retries=0)
    def validate(stage_result: dict) -> dict:
        """Run the 8 data-quality checks; raises (fails the task) on any 'fail'."""
        from validation.checks import run_checks
        return run_checks(stage_result["source_id"], stage_result["batch_id"])

    @task_group(group_id="per_source")
    def per_source_branch(source_id: str):
        validate(stage(extract(source_id)))

    # -----------------------------------------------------------------
    # Fan-in tasks (run once, across all sources)
    # -----------------------------------------------------------------

    # Default trigger rule (all_success), on purpose. stage() writes to
    # data/staging/ BEFORE validate() runs, so a batch that failed its
    # checks is still the newest batch on disk. A looser rule would let
    # harmonize publish data that failed validation.
    @task
    def harmonize() -> dict:
        """Staging -> curated: combine all sources into partitioned Parquet."""
        from transform.curated import harmonize as run_harmonize
        return run_harmonize()

    @task
    def load() -> dict:
        """Curated -> PostgreSQL: idempotent UPSERT of fact_crash + dim_date."""
        from load.load_postgres import load as run_load
        return run_load()

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def quality_report(**context) -> dict:
        """
        Summarize this run's dq_run_log rows. Runs even if earlier stages
        failed (ALL_DONE), so a broken run still produces a report.

        Airflow marks a DAG run successful or failed from its LAST task.
        Because this task runs regardless, it must fail itself when any
        upstream task failed; otherwise a run with a failed validation
        would show as green.
        """
        from validation.quality_report import build_quality_report

        dag_run = context["dag_run"]
        summary = build_quality_report(since=dag_run.start_date)

        bad = [
            ti.task_id + (f"[{ti.map_index}]" if ti.map_index >= 0 else "")
            for ti in dag_run.get_task_instances(
                state=[TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]
            )
            if ti.task_id != context["ti"].task_id
        ]
        if bad:
            raise AirflowException(
                f"Pipeline run had failed tasks: {sorted(bad)}. "
                f"Quality report written to {summary['report_path']}."
            )
        return summary

    per_source = per_source_branch.expand(source_id=SOURCES)
    per_source >> harmonize() >> load() >> quality_report()
