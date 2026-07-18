#!/usr/bin/env python3
"""Export bounded inputs for the below-10K treemap sensitivity analysis."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROFILE = "matt.hindman@researchaccelerator.org"
WAREHOUSE_ID = "86100da4e1fe8713"
OUTPUT_DIR = ROOT / "outputs" / "banded_lt10k_full_corpus_sensitivity_20260716" / "inputs"

STATS = "dev_sean.default.yt_channel_stats_full"
PILOT = "dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_treemap_pilot_channel_base"
PROJECTION = "dev_sean.matt.yt_treemap_full_corpus_lid_v3_20260715_v1_topic_projection"

DELTA_CTE = f"""
ranked AS (
  SELECT canonical_id, subscriber_count, total_view_count, collected_at, collected_date,
         ROW_NUMBER() OVER (
           PARTITION BY canonical_id, collected_date
           ORDER BY collected_at DESC, total_view_count DESC, subscriber_count DESC
         ) AS rn
  FROM {STATS}
  WHERE collected_date IN (DATE '2026-06-15', DATE '2026-07-13')
),
snapshots AS (
  SELECT canonical_id, subscriber_count, total_view_count, collected_date
  FROM ranked
  WHERE rn = 1
),
deltas AS (
  SELECT a.canonical_id,
         CAST(a.subscriber_count AS BIGINT) AS subscriber_count_t0,
         CAST(b.subscriber_count AS BIGINT) AS subscriber_count_t1,
         CAST(b.total_view_count - a.total_view_count AS DOUBLE) AS raw_4wk_views,
         CAST(CASE WHEN b.total_view_count >= a.total_view_count
                   THEN b.total_view_count - a.total_view_count END AS DOUBLE) AS view_count_4wk
  FROM snapshots a
  LEFT JOIN snapshots b
    ON a.canonical_id = b.canonical_id
   AND b.collected_date = DATE '2026-07-13'
  WHERE a.collected_date = DATE '2026-06-15'
)
"""

QUERIES = {
    "band_margins_1k": f"""
      WITH {DELTA_CTE}
      SELECT CAST(FLOOR(subscriber_count_t0 / 1000) AS INT) AS sample_band,
             COUNT(*) AS frame_channels,
             COUNT_IF(subscriber_count_t1 IS NULL) AS missing_current_channels,
             COUNT_IF(view_count_4wk > 0) AS positive_delta_channels,
             COUNT_IF(view_count_4wk = 0) AS zero_delta_channels,
             COUNT_IF(raw_4wk_views < 0) AS negative_delta_channels,
             SUM(COALESCE(view_count_4wk, 0)) AS positive_view_change
      FROM deltas
      WHERE subscriber_count_t0 >= 0 AND subscriber_count_t0 < 10000
      GROUP BY CAST(FLOOR(subscriber_count_t0 / 1000) AS INT)
      ORDER BY sample_band
    """,
    "baseline_language": f"""
      WITH {DELTA_CTE}
      SELECT COALESCE(p.language_display, 'Unlabelled >=10K') AS language_display,
             COUNT(*) AS positive_channels,
             SUM(d.view_count_4wk) AS positive_view_change
      FROM deltas d
      INNER JOIN {PROJECTION} p ON d.canonical_id = p.channel_id
      WHERE d.subscriber_count_t0 >= 10000 AND d.view_count_4wk > 0
      GROUP BY COALESCE(p.language_display, 'Unlabelled >=10K')
      ORDER BY positive_view_change DESC
    """,
    "baseline_language_channels": f"""
      SELECT language_display, COUNT(*) AS allocated_channel_count
      FROM {PROJECTION}
      GROUP BY language_display
      ORDER BY allocated_channel_count DESC
    """,
    "baseline_family_leaf": f"""
      WITH {DELTA_CTE},
      exploded AS (
        SELECT p.channel_id, di.yt_family, di.yt_leaf
        FROM {PROJECTION} p
        LATERAL VIEW EXPLODE(p.display_items) ex AS di
      ),
      deduped AS (
        SELECT DISTINCT channel_id, yt_family, yt_leaf FROM exploded
      ),
      family_counts AS (
        SELECT channel_id, COUNT(*) AS families_per_channel
        FROM (SELECT DISTINCT channel_id, yt_family FROM deduped)
        GROUP BY channel_id
      ),
      leaf_counts AS (
        SELECT channel_id, yt_family, COUNT(*) AS leaves_in_family
        FROM deduped
        GROUP BY channel_id, yt_family
      ),
      counts AS (
        SELECT d.channel_id, d.yt_family, d.yt_leaf,
               l.leaves_in_family, f.families_per_channel
        FROM deduped d
        INNER JOIN leaf_counts l USING (channel_id, yt_family)
        INNER JOIN family_counts f USING (channel_id)
      )
      SELECT c.yt_family, c.yt_leaf,
             COUNT(*) AS positive_channel_memberships,
             SUM(d.view_count_4wk / c.families_per_channel / c.leaves_in_family)
               AS positive_view_change
      FROM deltas d
      INNER JOIN counts c ON d.canonical_id = c.channel_id
      WHERE d.subscriber_count_t0 >= 10000 AND d.view_count_4wk > 0
      GROUP BY c.yt_family, c.yt_leaf
      ORDER BY positive_view_change DESC
    """,
    "baseline_family_leaf_channels": f"""
      WITH exploded AS (
        SELECT p.channel_id, di.yt_family, di.yt_leaf
        FROM {PROJECTION} p
        LATERAL VIEW EXPLODE(p.display_items) ex AS di
      ),
      deduped AS (
        SELECT DISTINCT channel_id, yt_family, yt_leaf FROM exploded
      ),
      family_counts AS (
        SELECT channel_id, COUNT(*) AS families_per_channel
        FROM (SELECT DISTINCT channel_id, yt_family FROM deduped)
        GROUP BY channel_id
      ),
      leaf_counts AS (
        SELECT channel_id, yt_family, COUNT(*) AS leaves_in_family
        FROM deduped
        GROUP BY channel_id, yt_family
      )
      SELECT d.yt_family, d.yt_leaf,
             SUM(CAST(1.0 AS DOUBLE) / f.families_per_channel / l.leaves_in_family)
               AS allocated_channel_count
      FROM deduped d
      INNER JOIN family_counts f USING (channel_id)
      INNER JOIN leaf_counts l USING (channel_id, yt_family)
      GROUP BY d.yt_family, d.yt_leaf
      ORDER BY allocated_channel_count DESC
    """,
    "pilot_rows": f"""
      SELECT channel_id, sampled_subscriber_count, sample_band, channel_language,
             is_language_classified, language_label_source, is_mixed_language,
             is_script_ambiguous, prior_subscriber_count, current_subscriber_count,
             raw_4wk_views, view_count_4wk, has_valid_4wk_views,
             has_invalid_negative_delta, raw_topic_categories, topic_row_present,
             has_nonempty_topic_categories
      FROM {PILOT}
      ORDER BY sample_band, channel_id
    """,
}


def databricks_api(method: str, path: str, *extra_args: str) -> dict:
    cmd = [
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
    return json.loads(subprocess.check_output(cmd, cwd=ROOT, text=True))


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
        response = databricks_api("post", "/api/2.0/sql/statements", "--json", f"@{request_path}")
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

    chunk_count = int(response.get("manifest", {}).get("total_chunk_count", 1))
    rows: list[list[object]] = []
    for chunk_index in range(chunk_count):
        if chunk_index == 0 and response.get("result", {}).get("data_array") is not None:
            chunk = response["result"]
        else:
            chunk = databricks_api(
                "get", f"/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}"
            ).get("result", {})
        rows.extend(chunk.get("data_array", []))
    return response, rows


def write_result(name: str, response: dict, rows: list[list[object]]) -> None:
    columns = [column["name"] for column in response["manifest"]["schema"]["columns"]]
    records = [dict(zip(columns, row)) for row in rows]
    payload = {
        "query": name,
        "statement_id": response["statement_id"],
        "columns": columns,
        "rows": records,
    }
    (OUTPUT_DIR / f"{name}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    with (OUTPUT_DIR / f"{name}.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for record in records:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value
                for key, value in record.items()
            })
    print(f"WROTE {name}: {len(records):,} rows ({response['statement_id']})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("queries", nargs="*", choices=sorted(QUERIES))
    args = parser.parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    selected = args.queries or list(QUERIES)
    for name in selected:
        statement = QUERIES[name]
        response, rows = execute_sql(statement)
        write_result(name, response, rows)


if __name__ == "__main__":
    main()
