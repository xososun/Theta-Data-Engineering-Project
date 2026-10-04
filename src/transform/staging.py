#!/usr/bin/env python3
"""
staging.py

Raw -> staging transform. Reads a raw batch produced by an extractor,
loads the matching adapter YAML, applies the adapter's `field_mapping`,
and writes a Parquet file whose columns match the canonical schema.

Mapping primitives (per field in the adapter's field_mapping):

  from: <COLUMN>            -> read a source column directly
  literal: <VALUE>          -> constant for every row
  map: {raw: canon, ...}    -> translate values (with optional `default:`)
  derive: "<expr>"          -> inline Python expression (simple cases)
  derive_fn: <name>         -> named function from derivations.py

The transform is config-driven: no `if source_id == "chicago_us"` branches.
Every source-specific decision lives in config/adapters/<source>.yaml.

Adapter lookup: file names (chicago.yaml) do not match source_id values
(chicago_us); we find the adapter by reading every YAML under
config/adapters/ and matching on the `source_id` field.

Usage:
    python -m src.transform.staging --source chicago_us --batch <batch_id>
"""

import argparse
import inspect
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.config import load_adapter_config
from utils.paths import raw_batch_dir, staging_batch_dir
from transform import derivations

REPO_ROOT = Path(__file__).resolve().parents[2]
ADAPTERS_DIR = REPO_ROOT / "config" / "adapters"
CANONICAL_SCHEMA = REPO_ROOT / "config" / "canonical_schema.yaml"


# ---------- Adapter loading ----------

def _load_canonical_field_names() -> list[str]:
    with open(CANONICAL_SCHEMA, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)
    return [field["name"] for field in schema["fields"]]


def _load_adapter(source_id: str) -> dict:
    """Find adapter YAML whose `source_id:` equals the requested source_id."""
    candidates = []
    for yaml_path in sorted(ADAPTERS_DIR.glob("*.yaml")):
        config = load_adapter_config(yaml_path)
        if config.get("source_id") == source_id:
            return config
        candidates.append((yaml_path.name, config.get("source_id")))
    raise FileNotFoundError(
        f"No adapter in {ADAPTERS_DIR} has source_id={source_id!r}. Found: {candidates}"
    )


# ---------- Raw reading ----------

def _read_raw(source_id: str, batch_id: str) -> pd.DataFrame:
    batch_dir = raw_batch_dir(source_id, batch_id)
    manifest_path = batch_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"No manifest.json at {manifest_path}. Raw batches must be "
            f"produced by an extractor, not placed by hand."
        )

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    files = manifest.get("files", [])
    if not files:
        raise ValueError(f"Manifest at {manifest_path} lists zero files.")

    frames = []
    for entry in files:
        raw_path = Path(entry["raw_path"])
        if not raw_path.exists():
            raise FileNotFoundError(f"Manifest lists {raw_path} but the file is missing.")
        suffix = raw_path.suffix.lower()
        if suffix == ".csv":
            frames.append(pd.read_csv(raw_path, dtype=str, keep_default_na=False, na_values=[""]))
        elif suffix == ".json":
            with open(raw_path, "r", encoding="utf-8") as f:
                page = json.load(f)
            frames.append(pd.DataFrame(page))
        else:
            raise ValueError(f"Unsupported raw file type: {raw_path.suffix}")

    df = pd.concat(frames, ignore_index=True)
    df.attrs["manifest"] = manifest
    return df


# ---------- Inline derive helpers ----------

def _apply_derive(expr: str, row: dict, adapter: dict):
    bbox = adapter.get("bounding_box", {})
    lat_bounds = tuple(bbox.get("latitude", [None, None]))
    lon_bounds = tuple(bbox.get("longitude", [None, None]))

    def between(value, bounds):
        if value is None or pd.isna(value):
            return False
        try:
            v = float(value)
        except (TypeError, ValueError):
            return False
        lo, hi = bounds
        if lo is None or hi is None:
            return False
        return lo <= v <= hi

    expr = expr.replace("bounding_box.latitude", "lat_bounds")
    expr = expr.replace("bounding_box.longitude", "lon_bounds")

    scope = {
        "combine_date_and_time": derivations.combine_date_and_time,
        "between": between,
        "lat_bounds": lat_bounds,
        "lon_bounds": lon_bounds,
    }
    scope.update(row)

    try:
        return eval(expr, {"__builtins__": {}}, scope)
    except Exception as e:
        raise ValueError(f"Failed to evaluate derive {expr!r} on row: {e}")


def _apply_map(value, mapping: dict, default):
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        value = None
    if value in mapping:
        return mapping[value]
    return default


# ---------- Field application ----------

def _apply_field(field_name: str, spec: dict, df: pd.DataFrame, adapter: dict) -> pd.Series:
    # Literal only
    if "literal" in spec and "from" not in spec and "derive" not in spec and "derive_fn" not in spec:
        return pd.Series([spec["literal"]] * len(df), index=df.index)

    # Named derive function
    if "derive_fn" in spec:
        fn_name = spec["derive_fn"]
        if not hasattr(derivations, fn_name):
            raise ValueError(f"derivations.py has no function {fn_name!r}")
        fn = getattr(derivations, fn_name)
        bbox = adapter.get("bounding_box", {})
        takes_bbox = "bbox" in inspect.signature(fn).parameters
        if takes_bbox:
            results = [fn(row.to_dict(), bbox) for _, row in df.iterrows()]
        else:
            results = [fn(row.to_dict()) for _, row in df.iterrows()]
        return pd.Series(results, index=df.index)

    # Inline derive expression
    if "derive" in spec and "from" not in spec:
        expr = spec["derive"]
        results = [_apply_derive(expr, row.to_dict(), adapter) for _, row in df.iterrows()]
        return pd.Series(results, index=df.index)

    # From a source column
    if "from" in spec:
        col = spec["from"]
        if col not in df.columns:
            raise KeyError(
                f"Adapter maps {field_name!r} from column {col!r}, but raw "
                f"data has no such column. First 15 present: {list(df.columns)[:15]}"
            )
        series = df[col].copy()

        if spec.get("apply_null_sentinel") and adapter.get("null_sentinel_value") is not None:
            sentinel = adapter["null_sentinel_value"]
            series = series.replace(str(sentinel), pd.NA)
            try:
                series = series.replace(sentinel, pd.NA)
            except TypeError:
                pass

        if "map" in spec:
            mapping = spec["map"]
            default = spec.get("default")
            series = series.apply(lambda v: _apply_map(v, mapping, default))

        return series

    raise ValueError(f"Field {field_name!r} has no recognised mapping spec: {spec}")


# ---------- Public entry point ----------

def stage(source_id: str, batch_id: str) -> dict:
    adapter = _load_adapter(source_id)
    canonical_fields = _load_canonical_field_names()

    print(f"[staging] source_id={source_id} batch_id={batch_id}")
    raw_df = _read_raw(source_id, batch_id)
    print(f"[staging] read {len(raw_df):,} raw rows, {len(raw_df.columns)} columns")

    mapping = adapter["field_mapping"]
    out = pd.DataFrame(index=raw_df.index)

    for field_name, spec in mapping.items():
        if field_name not in canonical_fields:
            continue
        out[field_name] = _apply_field(field_name, spec, raw_df, adapter)

    for field_name in canonical_fields:
        if field_name not in out.columns:
            out[field_name] = pd.NA

    out = out[canonical_fields]

    if "batch_id" not in mapping:
        out["batch_id"] = batch_id
    if "ingested_at_utc" not in mapping:
        out["ingested_at_utc"] = pd.Timestamp.now("UTC")
    if "source_row_raw_ref" not in mapping:
        out["source_row_raw_ref"] = pd.NA

    dest_dir = staging_batch_dir(source_id, batch_id)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / "fact_crash.parquet"
    out.to_parquet(dest_file, index=False)

    print(f"[staging] wrote {len(out):,} rows to {dest_file}")

    return {
        "source_id": source_id,
        "batch_id": batch_id,
        "rows_staged": len(out),
        "staging_path": str(dest_file),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--batch", required=True)
    args = parser.parse_args()

    try:
        result = stage(args.source, args.batch)
    except Exception as e:
        print(f"[staging] FAILED: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[staging] done: {result}")


if __name__ == "__main__":
    main()