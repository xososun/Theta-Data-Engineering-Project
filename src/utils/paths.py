"""
paths.py

Centralizes filesystem paths for the pipeline. Every stage (staging,
curated, load) reads its root directory from here, so Airflow and
manual `docker compose run` invocations resolve to the same locations.

Inside the container (where Airflow runs), the env vars
PIPELINE_RAW_DATA_ROOT, PIPELINE_STAGING_DATA_ROOT, and
PIPELINE_CURATED_DATA_ROOT are set by docker-compose.yml. Outside
Docker (local unit tests), they fall back to the repo's ./data/...
folders so tests can run without a container.
"""

import os
from pathlib import Path


def _env_path(var: str, fallback: str) -> Path:
    value = os.environ.get(var)
    if value:
        return Path(value)
    # Fallback: resolve relative to the repo root (two levels up from this file)
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / fallback


RAW_ROOT = _env_path("PIPELINE_RAW_DATA_ROOT", "data/raw")
STAGING_ROOT = _env_path("PIPELINE_STAGING_DATA_ROOT", "data/staging")
CURATED_ROOT = _env_path("PIPELINE_CURATED_DATA_ROOT", "data/curated")


def raw_batch_dir(source_id: str, batch_id: str) -> Path:
    return RAW_ROOT / source_id / batch_id


def staging_batch_dir(source_id: str, batch_id: str) -> Path:
    return STAGING_ROOT / source_id / batch_id


def curated_root() -> Path:
    return CURATED_ROOT