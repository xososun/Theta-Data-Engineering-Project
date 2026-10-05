"""
config.py

Thin loader for the per-source adapter YAML configs under config/adapters/.
Extractors read source_id, provider, and source-specific settings
(date_format, pagination, bounding_box, etc.) from these files rather than
hardcoding them, so the config stays the single source of truth across
profiling, extraction, and (later) transformation code.
"""

from pathlib import Path

import yaml


def load_adapter_config(path: str | Path) -> dict:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"Adapter config not found: {path}. Expected one YAML file per "
            f"source under config/adapters/ (see chicago.yaml for the "
            f"expected shape)."
        )
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    required_keys = ["source_id", "provider", "format"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise ValueError(
            f"Adapter config {path} is missing required key(s): {missing}"
        )
    return config
