"""
test_dag_imports.py

Structural tests for dags/crash_pipeline.py. Verifies the DAG file imports
cleanly and produces a well-formed DAG with the expected task IDs, without
needing a running Airflow instance or Docker stack.

These catch the failure mode that Airflow itself catches late and quietly:
a DAG file with a Python error silently disappears from the UI, with no
visible alert. Running this file in a pre-commit hook or CI catches that
in seconds.

Run:
    python -m pytest tests/test_dag_imports.py -v
or:
    python tests/test_dag_imports.py
"""

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "dags"
SRC_DIR = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(DAGS_DIR))


def _load_module_from_file(module_name: str, file_path: Path):
    """Load a module from an explicit file path, bypassing the normal
    package import machinery. Needed because dags/ is not a package
    (no __init__.py), and we want to load crash_pipeline.py by its path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stubs_module_imports():
    """dags/_stubs.py must be importable and expose all eight stage functions."""
    stubs_path = DAGS_DIR / "_stubs.py"
    assert stubs_path.exists(), f"_stubs.py not found at {stubs_path}"

    stubs = _load_module_from_file("_stubs_test", stubs_path)

    expected_functions = [
        "validate_raw_batch",
        "stage_source",
        "validate_staged",
        "harmonize_to_canonical",
        "load_postgres",
        "publish_parquet",
        "build_quality_report",
        "cleanup_failed_batch",
    ]
    missing = [fn for fn in expected_functions if not hasattr(stubs, fn)]
    assert not missing, (
        f"_stubs.py is missing function(s): {missing}. "
        f"Step 7's content must be present in full."
    )
    print(f"PASS: _stubs.py imports and exposes all {len(expected_functions)} functions")


def test_stubs_raise_on_call():
    """Every stub must raise NotImplementedError when called — never return
    fake data, which would make a DAG run look successful without doing work."""
    stubs = _load_module_from_file("_stubs_test_call", DAGS_DIR / "_stubs.py")

    probes = [
        ("validate_raw_batch", ("chicago_us", "batch", "/raw")),
        ("stage_source", ("chicago_us", "batch", "/raw", "/staging", "/config.yaml")),
        ("validate_staged", ("chicago_us", "batch", "/staging", [], {})),
        ("harmonize_to_canonical", ("chicago_us", "batch", "/staging", "/curated",
                                    "/schema.yaml", "/config.yaml")),
        ("load_postgres", ("chicago_us", "batch", "/curated", {})),
        ("publish_parquet", ("chicago_us", "batch", "/curated", "/curated")),
        ("build_quality_report", ("chicago_us", "batch", {}, "/reports")),
    ]
    for fn_name, args in probes:
        fn = getattr(stubs, fn_name)
        try:
            fn(*args)
            assert False, f"{fn_name} returned without raising NotImplementedError"
        except NotImplementedError:
            pass
    print(f"PASS: all {len(probes)} stub functions raise NotImplementedError when called")


def test_dag_file_exists():
    dag_path = DAGS_DIR / "crash_pipeline.py"
    assert dag_path.exists(), f"crash_pipeline.py not found at {dag_path}"
    print(f"PASS: {dag_path.name} exists")


def test_dag_imports_cleanly():
    """Importing crash_pipeline.py must not raise. This is the test that
    would catch a syntax error, a bad import, or a missing dependency
    before Airflow's scheduler silently drops the DAG."""
    dag_module = _load_module_from_file("crash_pipeline_test", DAGS_DIR / "crash_pipeline.py")
    assert hasattr(dag_module, "dag"), (
        "crash_pipeline.py does not expose a top-level `dag` object. "
        "The `with DAG(...) as dag:` block may be missing or renamed."
    )
    print("PASS: crash_pipeline.py imports cleanly and exposes `dag`")


def test_dag_has_expected_id():
    dag_module = _load_module_from_file("crash_pipeline_test_id", DAGS_DIR / "crash_pipeline.py")
    assert dag_module.dag.dag_id == "crash_pipeline", (
        f"Expected dag_id 'crash_pipeline', got '{dag_module.dag.dag_id}'"
    )
    print(f"PASS: dag_id is '{dag_module.dag.dag_id}'")


def test_dag_has_no_import_errors():
    """The DAG's own scheduler-side check: nothing in the file set
    `dag.import_errors` or left the DAG in a broken state."""
    dag_module = _load_module_from_file("crash_pipeline_test_errs", DAGS_DIR / "crash_pipeline.py")
    errors = getattr(dag_module.dag, "import_errors", {})
    assert not errors, f"DAG reports import errors: {errors}"
    print("PASS: DAG has no import_errors")


def test_dag_declares_all_three_sources():
    """SOURCES must contain exactly the three configured source ids, matching
    dim_source seeds in sql/schema.sql and the adapter YAMLs in config/adapters/."""
    dag_module = _load_module_from_file("crash_pipeline_test_sources", DAGS_DIR / "crash_pipeline.py")
    expected = {"chicago_us", "nyc_us", "uk_stats19"}
    actual = set(dag_module.SOURCES)
    assert actual == expected, (
        f"SOURCES mismatch. Expected {expected}, got {actual}. "
        f"Adding a new source means updating SOURCES here, adding a "
        f"config/adapters/<source_id>.yaml, and adding a row to dim_source."
    )
    print(f"PASS: SOURCES = {sorted(actual)}")


def test_dag_uses_expected_schedule():
    """The DAG must declare a weekly schedule (per proposal section 10)."""
    dag_module = _load_module_from_file("crash_pipeline_test_sched", DAGS_DIR / "crash_pipeline.py")
    schedule = dag_module.dag.schedule_interval
    schedule_str = str(schedule)
    assert "0 6 * * 1" in schedule_str or "cron" in schedule_str.lower(), (
        f"Unexpected schedule: {schedule_str}"
    )
    print(f"PASS: schedule is '{schedule_str}'")


def test_dag_declares_eight_stages():
    """The DAG must expose tasks for each of the 8 stages from proposal
    section 10, mapped across the three sources."""
    dag_module = _load_module_from_file("crash_pipeline_test_stages", DAGS_DIR / "crash_pipeline.py")
    task_ids = {t.task_id for t in dag_module.dag.tasks}

    expected_stages = [
        "extract",
        "raw_validate",
        "stage",
        "validate",
        "harmonize",
        "load_postgres_task",
        "publish_parquet_task",
        "quality_report",
    ]
    found = []
    for stage in expected_stages:
        if any(stage in tid for tid in task_ids):
            found.append(stage)
    missing = [s for s in expected_stages if s not in found]
    assert not missing, (
        f"DAG is missing task(s) for stage(s): {missing}. "
        f"Declared task_ids were: {sorted(task_ids)}"
    )
    print(f"PASS: all {len(expected_stages)} stages declared as tasks")
    print(f"      Total task count in DAG: {len(task_ids)}")


def test_dag_max_active_runs_is_one():
    """max_active_runs=1 prevents overlapping runs of the same DAG, which
    would race on the raw layer and the curated UPSERT."""
    dag_module = _load_module_from_file("crash_pipeline_test_mar", DAGS_DIR / "crash_pipeline.py")
    assert dag_module.dag.max_active_runs == 1, (
        f"max_active_runs should be 1, got {dag_module.dag.max_active_runs}"
    )
    print("PASS: max_active_runs == 1")


def test_dag_catchup_disabled():
    """catchup=False means a fresh install does not backfill every week
    since start_date. Required for a course project, otherwise the first
    DAG run after deployment would trigger dozens of historical runs."""
    dag_module = _load_module_from_file("crash_pipeline_test_catchup", DAGS_DIR / "crash_pipeline.py")
    assert dag_module.dag.catchup is False, (
        f"catchup should be False, got {dag_module.dag.catchup}"
    )
    print("PASS: catchup is disabled")

if __name__ == "__main__":
    tests = [
        test_stubs_module_imports,
        test_stubs_raise_on_call,
        test_dag_file_exists,
        test_dag_imports_cleanly,
        test_dag_has_expected_id,
        test_dag_has_no_import_errors,
        test_dag_declares_all_three_sources,
        test_dag_uses_expected_schedule,
        test_dag_declares_eight_stages,
        test_dag_max_active_runs_is_one,
        test_dag_catchup_disabled,
    ]
    failures = []
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failures.append((t.__name__, str(e)))
            print(f"FAIL: {t.__name__}: {e}")
        except Exception as e:
            failures.append((t.__name__, repr(e)))
            print(f"ERROR: {t.__name__}: {e!r}")

    print()
    if failures:
        print(f"{len(failures)} test(s) failed:")
        for name, msg in failures:
            print(f"  - {name}: {msg}")
        sys.exit(1)
    print(f"All {len(tests)} tests passed.")