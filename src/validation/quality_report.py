#!/usr/bin/env python3
"""
quality_report.py

Summarizes the data-quality results that checks.py wrote to dq_run_log
during one pipeline run, and writes them as a Markdown report under
data/quality_reports/. Read-only: never modifies fact_crash or dq_run_log.

Called by the final task of the Airflow DAG (which runs even when earlier
stages failed), and runnable by hand:

    python src/validation/quality_report.py                      # last 24 hours
    python src/validation/quality_report.py --since 2026-10-06T00:00:00+00:00
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.db import get_connection

log = logging.getLogger(__name__)

REPORT_ROOT = Path(
    os.environ.get("PIPELINE_QUALITY_REPORT_ROOT")
    or Path(os.environ.get("PIPELINE_CURATED_DATA_ROOT", "data/curated")).parent / "quality_reports"
)

QUERY = """
    SELECT source_id, batch_id, check_name, status,
           rows_checked, rows_failed, details, run_timestamp
    FROM dq_run_log
    WHERE run_timestamp >= %s
    ORDER BY source_id, batch_id, run_timestamp, check_name
"""


def _fetch_results(since: datetime) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(QUERY, (since,))
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _render_markdown(rows: list[dict], since: datetime, counts: dict) -> str:
    lines = [
        "# Data Quality Report",
        "",
        f"Checks run since {since.isoformat()}",
        "",
        f"**{counts['pass']} pass, {counts['warn']} warn, {counts['fail']} fail** "
        f"across {len({r['source_id'] for r in rows})} source(s).",
        "",
    ]
    if not rows:
        lines.append("No checks were recorded in this window. If this was a pipeline "
                     "run, validation did not execute; check upstream task logs.")
        return "\n".join(lines) + "\n"

    lines += [
        "| Source | Batch | Check | Status | Rows checked | Rows failed | Details |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for r in rows:
        details = (r["details"] or "").replace("|", "\\|").replace("\n", " ")[:200]
        lines.append(
            f"| {r['source_id']} | {r['batch_id']} | {r['check_name']} | "
            f"{r['status'].upper()} | {r['rows_checked']:,} | {r['rows_failed']:,} | {details} |"
        )
    return "\n".join(lines) + "\n"


def build_quality_report(since: datetime, report_root: Path = REPORT_ROOT) -> dict:
    """
    Summarize every dq_run_log row written at or after `since`.

    Returns {"report_path", "checks_run", "pass", "warn", "fail", "sources"}.
    """
    rows = _fetch_results(since)
    counts = {s: sum(1 for r in rows if r["status"] == s) for s in ("pass", "warn", "fail")}

    report_root = Path(report_root)
    report_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = report_root / f"quality_report_{stamp}.md"
    report_path.write_text(_render_markdown(rows, since, counts), encoding="utf-8")

    for r in rows:
        if r["status"] != "pass":
            log.warning("[quality] %s %s %s: %s", r["source_id"], r["check_name"],
                        r["status"].upper(), (r["details"] or "")[:200])
    log.info("[quality] %d checks: %d pass, %d warn, %d fail -> %s",
             len(rows), counts["pass"], counts["warn"], counts["fail"], report_path)

    return {
        "report_path": str(report_path),
        "checks_run": len(rows),
        "sources": sorted({r["source_id"] for r in rows if r["source_id"]}),
        **counts,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", help="ISO timestamp; default is 24 hours ago")
    args = parser.parse_args()

    since = (datetime.fromisoformat(args.since) if args.since
             else datetime.now(timezone.utc) - timedelta(hours=24))
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)

    summary = build_quality_report(since)
    print(f"[quality] done: {summary}")


if __name__ == "__main__":
    main()
