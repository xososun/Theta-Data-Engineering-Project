"""
test_dag_imports.py

Structural tests for dags/crash_pipeline.py. Verifies the DAG file imports
cleanly and produces a well-formed DAG with the expected tasks, dependencies,
and failure-handling settings, without needing a running Airflow scheduler
or the Docker stack's database.

These catch the failure mode that Airflow itself catches late and quietly:
a DAG file with a Python error silently disappears from the UI, with no
visible alert.

Requires Airflow to be importable (true inside the project's Docker image).

Run:
    python -m pytest tests/test_dag_imports.py -v
"""

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "dags"
sys.path.insert(0, str(REPO_ROOT / "src"))


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    d = dagbag.get_dag("crash_pipeline")
    assert d is not None, f"crash_pipeline not found. Import errors: {dagbag.import_errors}"
    return d


def test_dag_file_exists():
    assert (DAGS_DIR / "crash_pipeline.py").exists()


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_stubs_are_gone():
    """The placeholder module was replaced by the real src/ functions."""
    assert not (DAGS_DIR / "_stubs.py").exists()
    assert "_stubs" not in (DAGS_DIR / "crash_pipeline.py").read_text(encoding="utf-8")


def test_declares_all_three_sources():
    """SOURCES must match the adapter YAMLs and the dim_source seed rows."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("crash_pipeline_src", DAGS_DIR / "crash_pipeline.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert set(module.SOURCES) == {"chicago_us", "nyc_us", "uk_stats19"}


def test_expected_tasks(dag):
    assert {t.task_id for t in dag.tasks} == {
        "per_source.extract",
        "per_source.stage",
        "per_source.validate",
        "harmonize",
        "load",
        "quality_report",
    }


def test_per_source_chain_is_mapped(dag):
    """extract/stage/validate run once per source via dynamic task mapping."""
    for tid in ("per_source.extract", "per_source.stage", "per_source.validate"):
        assert dag.get_task(tid).get_closest_mapped_task_group() is not None, f"{tid} is not mapped"


def test_dependencies(dag):
    """per-source chain, then fan-in to a single harmonize -> load -> report."""
    up = lambda tid: dag.get_task(tid).upstream_task_ids  # noqa: E731
    assert up("per_source.extract") == set()
    assert up("per_source.stage") == {"per_source.extract"}
    assert up("per_source.validate") == {"per_source.stage"}
    assert up("harmonize") == {"per_source.validate"}
    assert up("load") == {"harmonize"}
    assert up("quality_report") == {"load"}


def test_harmonize_requires_all_sources_to_pass(dag):
    """A failed validation must block harmonize; the failed batch is still in staging."""
    assert dag.get_task("harmonize").trigger_rule == "all_success"


def test_quality_report_always_runs(dag):
    assert dag.get_task("quality_report").trigger_rule == "all_done"


def test_retries(dag):
    """Transient-failure tasks retry; deterministic DQ failures do not."""
    assert dag.get_task("per_source.extract").retries == 2
    assert dag.get_task("load").retries == 2
    assert dag.get_task("per_source.validate").retries == 0


def test_schedule(dag):
    assert "0 6 * * 1" in str(dag.schedule_interval)


def test_max_active_runs_is_one(dag):
    """Overlapping runs would race on the curated layer and the UPSERT."""
    assert dag.max_active_runs == 1


def test_catchup_disabled(dag):
    assert dag.catchup is False
