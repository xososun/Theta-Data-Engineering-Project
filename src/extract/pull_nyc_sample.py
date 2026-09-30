#!/usr/bin/env python3
"""
pull_nyc_sample.py

Pulls a large sample from the NYC Motor Vehicle Collisions - Crashes
Socrata API (https://data.cityofnewyork.us/resource/h9gi-nx95.json) and
saves it to CSV for profiling.

The API caps each request at 1,000 rows by default (and won't exceed
50,000 even with a larger $limit), so this pages through with $limit/
$offset, ordered by crash_date, until it has the requested row count or
runs out of data.

Usage:
    python pull_nyc_sample.py --rows 10000 --out nyc_sample.csv
    python pull_nyc_sample.py --rows 10000 --since 2020-01-01 --out nyc_sample.csv
"""

import argparse
import csv
import sys
import time
import urllib.request
import urllib.parse
import json

BASE_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
PAGE_SIZE = 1000  # Socrata's safe per-request page size


def fetch_page(limit: int, offset: int, since: str | None) -> list[dict]:
    params = {
        "$limit": limit,
        "$offset": offset,
        # A secondary sort key is required for stable pagination: when many
        # rows share the same crash_date, ordering by crash_date alone lets
        # a page boundary fall mid-tie, causing Socrata to return the same
        # row(s) again on the next page (confirmed: 4 duplicate collision_id
        # rows appeared, all exactly at page boundaries, before this fix).
        "$order": "crash_date DESC, collision_id DESC",
    }
    if since:
        params["$where"] = f"crash_date >= '{since}T00:00:00.000'"
    url = f"{BASE_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "data-eng-project/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000, help="Target number of rows to pull")
    parser.add_argument("--since", default=None, help="Optional ISO date (YYYY-MM-DD) lower bound on crash_date")
    parser.add_argument("--out", default="nyc_sample.csv", help="Output CSV path")
    args = parser.parse_args()

    all_rows: list[dict] = []
    offset = 0

    while len(all_rows) < args.rows:
        page_limit = min(PAGE_SIZE, args.rows - len(all_rows))
        try:
            page = fetch_page(page_limit, offset, args.since)
        except Exception as e:
            print(f"Request failed at offset {offset}: {e}", file=sys.stderr)
            break

        if not page:
            print(f"No more rows returned at offset {offset}; source may be exhausted.")
            break

        all_rows.extend(page)
        offset += len(page)
        print(f"Fetched {len(all_rows)} / {args.rows} rows...")
        time.sleep(0.2)  # be polite to the public endpoint

    if not all_rows:
        sys.exit("No rows fetched. Check connectivity and the endpoint URL.")

    # Defensive check: even with a stable sort order, don't silently trust
    # pagination. Flag duplicates rather than assume the fix above is
    # sufficient forever (e.g. if collision_id itself is ever non-unique
    # upstream).
    seen_ids = set()
    duplicate_ids = set()
    for row in all_rows:
        cid = row.get("collision_id")
        if cid in seen_ids:
            duplicate_ids.add(cid)
        seen_ids.add(cid)
    if duplicate_ids:
        print(f"WARNING: {len(duplicate_ids)} duplicate collision_id(s) still "
              f"found after pagination: {sorted(duplicate_ids)[:10]}"
              f"{' ...' if len(duplicate_ids) > 10 else ''}", file=sys.stderr)

    # Union of all keys seen, since Socrata omits fields that are null/absent
    # per row rather than sending them as empty strings.
    fieldnames: list[str] = []
    seen = set()
    for row in all_rows:
        for k in row.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            # Flatten the nested "location" dict into its own text form to
            # keep this readable as plain CSV; latitude/longitude columns
            # already carry the same values individually.
            row = dict(row)
            if "location" in row and isinstance(row["location"], dict):
                row["location"] = json.dumps(row["location"])
            writer.writerow(row)

    print(f"\nWrote {len(all_rows)} rows, {len(fieldnames)} columns to {args.out}")


if __name__ == "__main__":
    main()
