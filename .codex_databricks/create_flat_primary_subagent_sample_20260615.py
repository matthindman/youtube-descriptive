#!/usr/bin/env python3
"""Create a blind 1,000-case evidence sample for flat-topic subagent validation."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROFILE = os.environ.get("DATABRICKS_PROFILE", "matt.hindman@researchaccelerator.org")
WAREHOUSE_ID = "86100da4e1fe8713"
OUT_DIR = ROOT / "artifacts" / "category_taxonomy_estimation_20260612" / "flat_primary_subagent_validation_20260615"
SAMPLE_SEED = "flat_primary_subagent_validation_20260615_v1"
SAMPLE_SIZE = 1000


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


def execute_sql(statement: str) -> dict:
    body = {
        "warehouse_id": WAREHOUSE_ID,
        "catalog": "dev_sean",
        "schema": "matt",
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
        "statement": statement,
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(body, handle)
        temp_path = Path(handle.name)
    try:
        response = databricks_api("post", "/api/2.0/sql/statements", "--json", f"@{temp_path}")
    finally:
        temp_path.unlink(missing_ok=True)
    statement_id = response["statement_id"]
    state = response.get("status", {}).get("state")
    while state in {"PENDING", "RUNNING"}:
        time.sleep(5)
        response = databricks_api("get", f"/api/2.0/sql/statements/{statement_id}")
        state = response.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(json.dumps(response.get("status", response), indent=2))
    if "manifest" in response and "result" not in response:
        response["result"] = databricks_api("get", f"/api/2.0/sql/statements/{statement_id}/result/chunks/0")
    return response


def query_df(statement: str) -> pd.DataFrame:
    response = execute_sql(statement)
    columns = [col["name"] for col in response["manifest"]["schema"]["columns"]]
    rows = response.get("result", {}).get("data_array", [])
    df = pd.DataFrame(rows, columns=columns)
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (TypeError, ValueError):
            pass
    return df


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sql = f"""
    WITH eligible AS (
      SELECT
        f.channel_id,
        f.topic_categories,
        f.topic_category_count,
        f.primary_flat_label,
        f.primary_rule_id,
        f.candidate_flat_labels,
        f.specific_candidate_flat_labels,
        f.n_specific_candidate_flat_labels,
        c.channel_name,
        c.language_code,
        c.total_records
      FROM dev_sean.matt.yt_channel_topic_flat_primary_draft_20260615 f
      LEFT JOIN prod_tads.youtube_too.yt_sl_channels c
        ON f.channel_id = c.channel_id
      WHERE f.category_status = 'nonempty_topic_categories'
        AND f.topic_category_count > 0
    ),
    sampled AS (
      SELECT
        ROW_NUMBER() OVER (ORDER BY xxhash64(channel_id, '{SAMPLE_SEED}'), channel_id) AS case_id,
        *
      FROM eligible
      ORDER BY xxhash64(channel_id, '{SAMPLE_SEED}'), channel_id
      LIMIT {SAMPLE_SIZE}
    ),
    video_ranked AS (
      SELECT
        s.case_id,
        s.channel_id,
        ROW_NUMBER() OVER (
          PARTITION BY s.channel_id
          ORDER BY v.published_at DESC NULLS LAST, v.video_id ASC NULLS LAST
        ) AS rn,
        concat(
          '[',
          CAST(ROW_NUMBER() OVER (
            PARTITION BY s.channel_id
            ORDER BY v.published_at DESC NULLS LAST, v.video_id ASC NULLS LAST
          ) AS STRING),
          '] Title: ',
          substring(regexp_replace(coalesce(v.video_title, ''), '[\\r\\n\\t]+', ' '), 1, 260),
          CASE WHEN length(coalesce(v.description, '')) > 0
            THEN concat(' | Description: ', substring(regexp_replace(coalesce(v.description, ''), '[\\r\\n\\t]+', ' '), 1, 220))
            ELSE ''
          END
        ) AS video_line
      FROM sampled s
      LEFT JOIN prod_tads.youtube_too.yt_sl_videos v
        ON s.channel_id = v.channel_id
    ),
    video_agg AS (
      SELECT
        channel_id,
        array_join(transform(array_sort(collect_list(named_struct('rn', rn, 'line', video_line))), x -> x.line), '\\n') AS recent_video_evidence,
        COUNT(*) AS n_recent_videos
      FROM video_ranked
      WHERE rn <= 8
      GROUP BY channel_id
    )
    SELECT
      s.case_id,
      s.channel_id,
      s.channel_name,
      s.language_code,
      s.total_records,
      coalesce(v.n_recent_videos, 0) AS n_recent_videos,
      coalesce(v.recent_video_evidence, '') AS recent_video_evidence,
      s.topic_categories,
      s.topic_category_count,
      s.primary_flat_label,
      s.primary_rule_id,
      s.candidate_flat_labels,
      s.specific_candidate_flat_labels,
      s.n_specific_candidate_flat_labels
    FROM sampled s
    LEFT JOIN video_agg v
      ON s.channel_id = v.channel_id
    ORDER BY s.case_id
    """
    df = query_df(sql)
    reference_cols = [
        "case_id",
        "channel_id",
        "channel_name",
        "language_code",
        "total_records",
        "n_recent_videos",
        "recent_video_evidence",
        "topic_categories",
        "topic_category_count",
        "primary_flat_label",
        "primary_rule_id",
        "candidate_flat_labels",
        "specific_candidate_flat_labels",
        "n_specific_candidate_flat_labels",
    ]
    blind_cols = [
        "case_id",
        "channel_id",
        "channel_name",
        "language_code",
        "total_records",
        "n_recent_videos",
        "recent_video_evidence",
    ]
    df[reference_cols].to_csv(OUT_DIR / "flat_primary_subagent_reference_1000.csv", index=False)
    df[blind_cols].to_csv(OUT_DIR / "flat_primary_subagent_blind_evidence_1000.csv", index=False)
    summary = {
        "sample_size": len(df),
        "seed": SAMPLE_SEED,
        "blind_evidence_path": str(OUT_DIR / "flat_primary_subagent_blind_evidence_1000.csv"),
        "reference_path": str(OUT_DIR / "flat_primary_subagent_reference_1000.csv"),
        "primary_label_counts": df["primary_flat_label"].value_counts().to_dict(),
    }
    (OUT_DIR / "sample_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
