"""
crash_pipeline.py

Airflow DAG for the cross-city road crash data pipeline.

Shape:

    per source, in parallel (dynamic task mapping over SOURCES):
        extract -> stage -> validate
                              |
                  (all three sources must succeed)
                              v
    once:   harmonize -> validate_curated -> publish -> load -> quality_report
                                                               (ALL_DONE)

Why the fan-in: stage() and run_checks() work on one source's batch, but
harmonize() combines the validated batch of EVERY source and rewrites the
whole curated layer, and load() loads all of curated. Running those two
per source would have three tasks rewriting the same folder at once.

Write-audit-publish: harmonize writes a CANDIDATE curated dataset,
validate_curated audits it (schema, unique keys, per-source row
reconciliation, timestamps/date keys, partitions), and only then does
publish swap it into the live location and load copy it into Postgres. A
bug in the cross-source transformation fails validate_curated and never
reaches the published Parquet or Postgres.

What a scheduled run refreshes: NYC is pulled from its API every run.
Chicago and UK are read from files a person places in data/incoming/; if
those files have not been replaced, extract detects it (SHA-256 against
the last raw batch), reuses the previous batch instead of copying the
same file again, and records it in a source_freshness row in dq_run_log.

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
from airflow.exceptions import AirflowException, AirflowFailException
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
# amendments (proposal section 9). The window ends at the newest date the
# API has published (it lags behind today). Adjust here, not in the extractor.
# A one-off backfill can override the window when triggering the DAG:
#   airflow dags trigger crash_pipeline --conf '{"nyc_since": "2021-01-01", "nyc_until": "2025-12-31"}'
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
    def extract(source_id: str, **context) -> dict:
        """
        Run this source's extractor; returns its batch_id for later tasks.

        Also records whether new data actually arrived, as a
        `source_freshness` row in dq_run_log (see src/utils/freshness.py).
        """
        from utils.config import load_adapter_config
        from utils.freshness import freshness_status, select_incoming_file

        config_path = f"{CONFIG_DIR}/{ADAPTER_FILES[source_id]}"
        refresh_cfg = load_adapter_config(config_path).get("refresh", {})
        conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}

        if source_id == "chicago_us":
            from extract.chicago_extractor import run
            # Newest export in data/incoming/ (by the date in its file name),
            # so replacing the export is all a refresh needs. CHICAGO_CSV
            # still overrides. No matching file fails the task visibly.
            input_path = os.environ.get("CHICAGO_CSV") or select_incoming_file(
                INCOMING_ROOT, refresh_cfg.get("incoming_pattern", "Traffic_Crashes_-_Crashes_*.csv"))
            result = run(
                input_path=str(input_path),
                config_path=config_path,
                raw_root=RAW_ROOT,
            )
        elif source_id == "nyc_us":
            from extract.nyc_extractor import latest_crash_date, run
            # NYC publishes with a lag, so anchor the window on the newest
            # date the API actually has, not on today.
            api = load_adapter_config(config_path)["api_endpoint"]
            latest = datetime.strptime(latest_crash_date(api), "%Y-%m-%d").date()
            until = datetime.strptime(conf["nyc_until"], "%Y-%m-%d").date() if conf.get("nyc_until") else latest
            since = (datetime.strptime(conf["nyc_since"], "%Y-%m-%d").date() if conf.get("nyc_since")
                     else until - timedelta(days=NYC_TRAILING_WINDOW_DAYS))
            log.info("[extract] nyc_us latest published date %s; window %s to %s%s",
                     latest, since, until, " (backfill from run conf)" if conf.get("nyc_since") else "")
            result = run(
                since=since.isoformat(),
                until=until.isoformat(),
                config_path=config_path,
                raw_root=RAW_ROOT,
                max_rows=None,
            )
            result.update(latest_published_date=latest.isoformat(),
                          window_since=since.isoformat(), window_until=until.isoformat())
        elif source_id == "uk_stats19":
            from extract.uk_extractor import run
            result = run(
                input_dir=INCOMING_ROOT,
                years=UK_YEARS,
                config_path=config_path,
                raw_root=RAW_ROOT,
            )
            # A missing or unreadable year must fail the run: otherwise the
            # pipeline would publish 4 of 5 years as if they were complete.
            # AirflowFailException skips retries: a missing file won't appear
            # in two minutes.
            if result["years_failed"]:
                raise AirflowFailException(
                    f"UK extract incomplete, years failed: {result['years_failed']}. "
                    f"Check the files in data/incoming/."
                )
        else:
            raise ValueError(f"Unknown source_id: {source_id}")

        # Extractors name the count differently (row_count vs total_rows).
        row_count = result.get("row_count", result.get("total_rows", 0)) or 0

        # Record what this run actually refreshed.
        from validation.checks import log_check
        status, details = freshness_status(result, refresh_cfg)
        log_check(source_id, result["batch_id"], "source_freshness", status, row_count, 0, details)
        log.info("[extract] %s batch=%s rows=%s freshness=%s: %s",
                 source_id, result["batch_id"], row_count, status, details)
        return {
            "source_id": source_id,
            "batch_id": result["batch_id"],
            "row_count": row_count,
            "content_changed": result.get("content_changed", True),
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
        # The group returns validate()'s result, so harmonize receives the
        # exact batch each source validated in this run.
        return validate(stage(extract(source_id)))

    # -----------------------------------------------------------------
    # Fan-in tasks (run once, across all sources)
    # -----------------------------------------------------------------

    # Default trigger rule (all_success), on purpose: a failed source
    # validation must stop the run. harmonize also reads exactly the
    # batches validated in this run (not "newest on disk"), so a batch that
    # failed validation in an earlier run can never be picked up.
    @task
    def harmonize(validated: list[dict]) -> dict:
        """Staging -> curated CANDIDATE: combine the validated batches."""
        from transform.curated import harmonize as run_harmonize
        batches = {v["source_id"]: v["batch_id"] for v in validated}
        return run_harmonize(batches=batches)

    # No retries, same reason as validate: a failed check is deterministic.
    @task(retries=0)
    def validate_curated(harmonize_result: dict) -> dict:
        """Audit the curated candidate; raises (fails the task) on any 'fail'."""
        from validation.curated_checks import run_curated_checks
        return run_curated_checks(harmonize_result)

    @task
    def publish(curated_checks_result: dict) -> dict:
        """Swap the audited candidate into the live curated location."""
        from transform.curated import publish as run_publish
        return run_publish()

    @task
    def load(publish_result: dict) -> dict:
        """Published curated -> PostgreSQL: idempotent UPSERT of fact_crash + dim_date."""
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

    validated = per_source_branch.expand(source_id=SOURCES)
    load(publish(validate_curated(harmonize(validated)))) >> quality_report()
