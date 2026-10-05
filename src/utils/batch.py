"""
batch.py

Generates batch identifiers used across every extractor. A batch_id ties
together: the raw files written by one ingestion run, the rows loaded from
that run (fact_crash.batch_id), and the dq_run_log entries for that run.

Format: {source_id}_{utc_timestamp}_{short_uuid}
Example: chicago_us_20260930T114500Z_a1b2c3d4

This is deliberately NOT just a timestamp: two runs of the same source
seconds apart (e.g. a manual retry right after a failure) must never
collide, which a timestamp-only ID risks if run twice in the same second.
"""

import uuid
from datetime import datetime, timezone


def new_batch_id(source_id: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    short_uuid = uuid.uuid4().hex[:8]
    return f"{source_id}_{ts}_{short_uuid}"
