#!/usr/bin/env python3
"""Export compact same-universe inputs for the treemap weighting-bias visual."""

from __future__ import annotations

import csv
import json
import subprocess
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROFILE = "matt.hindman@researchaccelerator.org"
WAREHOUSE_ID = "86100da4e1fe8713"
ALLOCATIONS = "dev_sean.matt.yt_treemap_full_corpus_lid_v3_20260715_v1_allocations_family_balanced_raw"
OUTPUT_DIR = ROOT / "outputs" / "youtube_topic_treemap_weighting_bias_20260716_v1" / "databricks_export"

QUERY = f"""
SELECT
  language_display,
  yt_family,
  yt_leaf,
  SUM(CAST(allocation_weight AS DOUBLE)) AS allocated_channel_mass,
  SUM(COALESCE(CAST(allocated_views_4wk AS DOUBLE), 0.0)) AS allocated_view_mass,
  COUNT(DISTINCT channel_id) AS channel_memberships,
  COUNT(DISTINCT CASE WHEN view_count_4wk > 0 THEN channel_id END) AS positive_view_channel_memberships
FROM {ALLOCATIONS}
GROUP BY language_display, yt_family, yt_leaf
ORDER BY language_display, yt_family, yt_leaf
"""


def databricks_api(method: str, path: str, *extra_args: str) -> dict:
    command = [
        "env",
        "DATABRICKS_AUTH_STORAGE=plaintext",
        "databricks",
        "api",
        method,
        path,
        "--profile",
        PROFILE,
        "--output",
        "json",
        *extra_args,
    ]
    return json.loads(subprocess.check_output(command, cwd=ROOT, text=True))


def execute_sql(statement: str) -> tuple[dict, list[list[object]]]:
    body = {
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
        "disposition": "INLINE",
        "statement": statement.strip(),
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(body, handle)
        request_path = Path(handle.name)
    try:
        response = databricks_api(
            "post", "/api/2.0/sql/statements", "--json", f"@{request_path}"
        )
    finally:
        request_path.unlink(missing_ok=True)

    statement_id = response["statement_id"]
    state = response.get("status", {}).get("state")
    while state in {"PENDING", "RUNNING"}:
        time.sleep(3)
        response = databricks_api("get", f"/api/2.0/sql/statements/{statement_id}")
        state = response.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(json.dumps(response.get("status", response), indent=2))

    rows: list[list[object]] = []
    chunk_count = int(response.get("manifest", {}).get("total_chunk_count", 1))
    for chunk_index in range(chunk_count):
        if chunk_index == 0 and response.get("result", {}).get("data_array") is not None:
            chunk = response["result"]
        else:
            chunk = databricks_api(
                "get", f"/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}"
            ).get("result", {})
        rows.extend(chunk.get("data_array", []))
    return response, rows


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    response, rows = execute_sql(QUERY)
    columns = [column["name"] for column in response["manifest"]["schema"]["columns"]]
    records = [dict(zip(columns, row)) for row in rows]
    csv_path = OUTPUT_DIR / "language_family_leaf_weighting_masses.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)

    manifest = {
        "statement_id": response["statement_id"],
        "profile": PROFILE,
        "warehouse_id": WAREHOUSE_ID,
        "source_table": ALLOCATIONS,
        "source_universe": "2026-06-15 subscriber cohort with current_subscriber_count >= 10000",
        "allocation_method": "raw family-balanced allocations; no named-channel display overrides",
        "rows": len(records),
        "columns": columns,
        "csv": str(csv_path.relative_to(ROOT)),
    }
    manifest_path = OUTPUT_DIR / "export_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"WROTE {csv_path}: {len(records):,} rows")
    print(f"STATEMENT ID: {response['statement_id']}")


if __name__ == "__main__":
    main()
