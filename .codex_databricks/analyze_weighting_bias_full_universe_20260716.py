#!/usr/bin/env python3
"""Read-only census analysis of channel-weighted versus view-weighted YouTube composition."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROFILE = os.environ.get("DATABRICKS_PROFILE", "matt.hindman@researchaccelerator.org")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "86100da4e1fe8713")
OUTPUT_DIR = ROOT / "artifacts" / "weighting_bias_full_universe_20260716"

BASE = "dev_sean.matt.yt_treemap_full_corpus_lid_v3_20260715_v1_channel_base"
ALLOC = "dev_sean.matt.yt_treemap_full_corpus_lid_v3_20260715_v1_allocations_family_balanced_raw"

BASE_COMPLETE = f"""
base_complete AS (
  SELECT channel_id, channel_language, language_display,
         CAST(raw_4wk_views AS DOUBLE) AS net_views,
         GREATEST(CAST(raw_4wk_views AS DOUBLE), 0.0) AS positive_views,
         GREATEST(-CAST(raw_4wk_views AS DOUBLE), 0.0) AS negative_correction_mass
  FROM {BASE}
  WHERE in_subscriber_cohort
    AND raw_4wk_views IS NOT NULL
)
"""


QUERIES = {
    "universe_qa": f"""
    WITH cohort AS (
      SELECT channel_id, raw_4wk_views, has_current_snapshot, has_prior_snapshot
      FROM {BASE} WHERE in_subscriber_cohort
    ),
    complete AS (
      SELECT channel_id, CAST(raw_4wk_views AS DOUBLE) AS net_views,
             GREATEST(CAST(raw_4wk_views AS DOUBLE), 0.0) AS positive_views,
             GREATEST(-CAST(raw_4wk_views AS DOUBLE), 0.0) AS negative_correction_mass
      FROM cohort WHERE raw_4wk_views IS NOT NULL
    ),
    ranked AS (
      SELECT *, ROW_NUMBER() OVER (ORDER BY positive_views DESC, channel_id) AS view_rank
      FROM complete
    ),
    negative_ranked AS (
      SELECT *, ROW_NUMBER() OVER (ORDER BY negative_correction_mass DESC, channel_id) AS correction_rank
      FROM complete
    ),
    totals AS (
      SELECT COUNT(*) AS n_complete,
             SUM(net_views) AS total_net_views,
             SUM(positive_views) AS total_positive_views,
             SUM(GREATEST(-net_views, 0.0)) AS total_negative_correction_mass,
             SUM(POWER(positive_views, 2)) AS sum_squared_positive_views,
             COUNT_IF(net_views < 0) AS n_negative,
             COUNT_IF(net_views = 0) AS n_zero,
             COUNT_IF(net_views > 0) AS n_positive,
             percentile_approx(positive_views,
               array(0.5, 0.9, 0.99, 0.999, 0.9999, 1.0), 10000) AS positive_view_quantiles
             ,percentile_approx(negative_correction_mass,
               array(0.95, 0.99, 0.999, 0.9999, 1.0), 10000) AS negative_correction_quantiles
      FROM complete
    ),
    concentration AS (
      SELECT SUM(CASE WHEN view_rank <= 1 THEN positive_views ELSE 0 END) AS top1_views,
             SUM(CASE WHEN view_rank <= 10 THEN positive_views ELSE 0 END) AS top10_views,
             SUM(CASE WHEN view_rank <= 100 THEN positive_views ELSE 0 END) AS top100_views,
             SUM(CASE WHEN view_rank <= 1000 THEN positive_views ELSE 0 END) AS top1000_views,
             SUM(CASE WHEN view_rank <= 10000 THEN positive_views ELSE 0 END) AS top10000_views
      FROM ranked
    ),
    correction_concentration AS (
      SELECT SUM(CASE WHEN correction_rank <= 1 THEN negative_correction_mass ELSE 0 END) AS top1_correction,
             SUM(CASE WHEN correction_rank <= 10 THEN negative_correction_mass ELSE 0 END) AS top10_correction,
             SUM(CASE WHEN correction_rank <= 100 THEN negative_correction_mass ELSE 0 END) AS top100_correction,
             SUM(CASE WHEN correction_rank <= 1000 THEN negative_correction_mass ELSE 0 END) AS top1000_correction
      FROM negative_ranked
    ),
    cohort_counts AS (
      SELECT COUNT(*) AS n_cohort,
             COUNT_IF(has_current_snapshot) AS n_current,
             COUNT_IF(has_prior_snapshot) AS n_prior,
             COUNT_IF(raw_4wk_views IS NOT NULL) AS n_both
      FROM cohort
    )
    SELECT c.n_cohort, c.n_current, c.n_prior, c.n_both,
           t.n_complete, t.n_negative, t.n_zero, t.n_positive,
           t.total_net_views, t.total_positive_views, t.total_negative_correction_mass,
           t.positive_view_quantiles, t.negative_correction_quantiles,
           POWER(t.total_positive_views, 2) / t.sum_squared_positive_views
             AS census_weight_effective_sample_size,
           t.n_complete * t.sum_squared_positive_views / POWER(t.total_positive_views, 2)
             AS kish_design_effect,
           k.top1_views / t.total_positive_views AS top1_share,
           k.top10_views / t.total_positive_views AS top10_share,
           k.top100_views / t.total_positive_views AS top100_share,
           k.top1000_views / t.total_positive_views AS top1000_share,
           k.top10000_views / t.total_positive_views AS top10000_share,
           n.top1_correction / t.total_negative_correction_mass AS correction_top1_share,
           n.top10_correction / t.total_negative_correction_mass AS correction_top10_share,
           n.top100_correction / t.total_negative_correction_mass AS correction_top100_share,
           n.top1000_correction / t.total_negative_correction_mass AS correction_top1000_share
    FROM cohort_counts c CROSS JOIN totals t CROSS JOIN concentration k
         CROSS JOIN correction_concentration n
    """,

    "language_bias": f"""
    WITH {BASE_COMPLETE},
    totals AS (
      SELECT COUNT(*) AS n, SUM(net_views) AS total_net,
             SUM(positive_views) AS total_positive,
             SUM(POWER(positive_views, 2)) AS total_w2,
             AVG(positive_views) AS mean_positive
      FROM base_complete
    ),
    grouped AS (
      SELECT channel_language, language_display,
             COUNT(*) AS n_channels,
             SUM(net_views) AS net_views,
             SUM(positive_views) AS positive_views,
             SUM(negative_correction_mass) AS negative_correction_mass,
             SUM(POWER(positive_views, 2)) AS group_w2
      FROM base_complete
      GROUP BY channel_language, language_display
    )
    SELECT g.channel_language, g.language_display, g.n_channels,
           CAST(g.n_channels AS DOUBLE) / t.n AS channel_share,
           g.net_views / t.total_net AS net_view_share,
           g.positive_views / t.total_positive AS positive_view_share,
           (CAST(g.n_channels AS DOUBLE) / t.n) - (g.net_views / t.total_net)
             AS unweighted_minus_net_bias,
           (CAST(g.n_channels AS DOUBLE) / t.n) - (g.positive_views / t.total_positive)
             AS unweighted_minus_positive_bias,
           (g.positive_views / t.total_positive)
             / NULLIF(CAST(g.n_channels AS DOUBLE) / t.n, 0) AS audience_multiplier,
           g.negative_correction_mass,
           SQRT(
             ((1 - g.positive_views / t.total_positive) *
               (1 - g.positive_views / t.total_positive) * g.group_w2
              + POWER(g.positive_views / t.total_positive, 2) * (t.total_w2 - g.group_w2))
             / t.n / 1000
           ) / t.mean_positive AS weighted_se_srs_n1000,
           SQRT(
             (CAST(g.n_channels AS DOUBLE) / t.n) *
             (1 - CAST(g.n_channels AS DOUBLE) / t.n) / 1000
           ) AS unweighted_channel_share_se_n1000
    FROM grouped g CROSS JOIN totals t
    ORDER BY positive_view_share DESC
    """,

    "topic_bias": f"""
    WITH {BASE_COMPLETE},
    family_alloc AS (
      SELECT channel_id, yt_family, SUM(allocation_weight) AS allocation_weight
      FROM {ALLOC} GROUP BY channel_id, yt_family
    ),
    covered AS (
      SELECT b.* FROM base_complete b
      JOIN (SELECT channel_id FROM family_alloc GROUP BY channel_id) a USING (channel_id)
    ),
    totals AS (
      SELECT COUNT(*) AS n, SUM(net_views) AS total_net,
             SUM(positive_views) AS total_positive,
             SUM(POWER(positive_views, 2)) AS total_w2,
             AVG(positive_views) AS mean_positive
      FROM covered
    ),
    grouped AS (
      SELECT a.yt_family,
             COUNT(DISTINCT a.channel_id) AS member_channels,
             SUM(a.allocation_weight) AS channel_equivalents,
             SUM(b.net_views * a.allocation_weight) AS net_views,
             SUM(b.positive_views * a.allocation_weight) AS positive_views,
             SUM(b.negative_correction_mass * a.allocation_weight) AS negative_correction_mass,
             SUM(POWER(a.allocation_weight, 2)) AS sum_a2,
             SUM(POWER(b.positive_views, 2) * a.allocation_weight) AS sum_w2_a,
             SUM(POWER(b.positive_views * a.allocation_weight, 2)) AS sum_w2_a2
      FROM covered b JOIN family_alloc a USING (channel_id)
      GROUP BY a.yt_family
    )
    SELECT 'topic' AS topic_level, g.yt_family, CAST(NULL AS STRING) AS yt_leaf,
           g.member_channels, g.channel_equivalents,
           g.channel_equivalents / t.n AS channel_share,
           g.net_views / t.total_net AS net_view_share,
           g.positive_views / t.total_positive AS positive_view_share,
           g.channel_equivalents / t.n - g.net_views / t.total_net AS unweighted_minus_net_bias,
           g.channel_equivalents / t.n - g.positive_views / t.total_positive
             AS unweighted_minus_positive_bias,
           (g.positive_views / t.total_positive)
             / NULLIF(g.channel_equivalents / t.n, 0) AS audience_multiplier,
           g.negative_correction_mass,
           SQRT(
             (g.sum_w2_a2
              - 2 * (g.positive_views / t.total_positive) * g.sum_w2_a
              + POWER(g.positive_views / t.total_positive, 2) * t.total_w2)
             / t.n / 1000
           ) / t.mean_positive AS weighted_se_srs_n1000,
           SQRT(
             (g.sum_a2 / t.n - POWER(g.channel_equivalents / t.n, 2)) / 1000
           ) AS unweighted_channel_share_se_n1000
    FROM grouped g CROSS JOIN totals t
    ORDER BY positive_view_share DESC
    """,

    "subtopic_bias": f"""
    WITH {BASE_COMPLETE},
    leaf_alloc AS (
      SELECT channel_id, yt_family, yt_leaf, SUM(allocation_weight) AS allocation_weight
      FROM {ALLOC} GROUP BY channel_id, yt_family, yt_leaf
    ),
    covered AS (
      SELECT b.* FROM base_complete b
      JOIN (SELECT channel_id FROM leaf_alloc GROUP BY channel_id) a USING (channel_id)
    ),
    totals AS (
      SELECT COUNT(*) AS n, SUM(net_views) AS total_net,
             SUM(positive_views) AS total_positive,
             SUM(POWER(positive_views, 2)) AS total_w2,
             AVG(positive_views) AS mean_positive
      FROM covered
    ),
    grouped AS (
      SELECT a.yt_family, a.yt_leaf,
             COUNT(DISTINCT a.channel_id) AS member_channels,
             SUM(a.allocation_weight) AS channel_equivalents,
             SUM(b.net_views * a.allocation_weight) AS net_views,
             SUM(b.positive_views * a.allocation_weight) AS positive_views,
             SUM(b.negative_correction_mass * a.allocation_weight) AS negative_correction_mass,
             SUM(POWER(a.allocation_weight, 2)) AS sum_a2,
             SUM(POWER(b.positive_views, 2) * a.allocation_weight) AS sum_w2_a,
             SUM(POWER(b.positive_views * a.allocation_weight, 2)) AS sum_w2_a2
      FROM covered b JOIN leaf_alloc a USING (channel_id)
      GROUP BY a.yt_family, a.yt_leaf
    )
    SELECT 'subtopic' AS topic_level, g.yt_family, g.yt_leaf,
           g.member_channels, g.channel_equivalents,
           g.channel_equivalents / t.n AS channel_share,
           g.net_views / t.total_net AS net_view_share,
           g.positive_views / t.total_positive AS positive_view_share,
           g.channel_equivalents / t.n - g.net_views / t.total_net AS unweighted_minus_net_bias,
           g.channel_equivalents / t.n - g.positive_views / t.total_positive
             AS unweighted_minus_positive_bias,
           (g.positive_views / t.total_positive)
             / NULLIF(g.channel_equivalents / t.n, 0) AS audience_multiplier,
           g.negative_correction_mass,
           SQRT(
             (g.sum_w2_a2
              - 2 * (g.positive_views / t.total_positive) * g.sum_w2_a
              + POWER(g.positive_views / t.total_positive, 2) * t.total_w2)
             / t.n / 1000
           ) / t.mean_positive AS weighted_se_srs_n1000,
           SQRT(
             (g.sum_a2 / t.n - POWER(g.channel_equivalents / t.n, 2)) / 1000
           ) AS unweighted_channel_share_se_n1000
    FROM grouped g CROSS JOIN totals t
    ORDER BY positive_view_share DESC
    """,

    "language_repeated_samples": f"""
    WITH {BASE_COMPLETE},
    bucketed AS (
      SELECT *, PMOD(XXHASH64(channel_id), 4779) AS sample_id
      FROM base_complete
    ),
    census AS (
      SELECT channel_language, language_display,
             COUNT(*) / (SELECT COUNT(*) FROM base_complete) AS channel_share,
             SUM(positive_views) / (SELECT SUM(positive_views) FROM base_complete) AS view_share
      FROM base_complete GROUP BY channel_language, language_display
    ),
    sample_totals AS (
      SELECT sample_id, COUNT(*) AS sample_n, SUM(positive_views) AS sample_views
      FROM bucketed GROUP BY sample_id
    ),
    sample_group AS (
      SELECT sample_id, channel_language, language_display,
             COUNT(*) AS group_n, SUM(positive_views) AS group_views
      FROM bucketed
      GROUP BY sample_id, channel_language, language_display
    ),
    draws AS (
      SELECT c.channel_language, c.language_display, c.channel_share, c.view_share,
             t.sample_id, t.sample_n,
             COALESCE(g.group_n, 0) / t.sample_n AS sample_channel_share,
             COALESCE(g.group_views, 0) / t.sample_views AS sample_view_share
      FROM census c CROSS JOIN sample_totals t
      LEFT JOIN sample_group g
        ON c.channel_language = g.channel_language
       AND c.language_display = g.language_display
       AND t.sample_id = g.sample_id
    )
    SELECT channel_language, language_display, channel_share, view_share,
           COUNT(*) AS n_replicates, AVG(sample_n) AS mean_sample_n,
           AVG(sample_channel_share) AS mean_sample_channel_share,
           STDDEV_SAMP(sample_channel_share) AS sd_sample_channel_share,
           AVG(sample_view_share) AS mean_sample_view_share,
           STDDEV_SAMP(sample_view_share) AS sd_sample_view_share,
           percentile_approx(sample_view_share, array(0.025,0.5,0.975), 10000)
             AS sample_view_share_p025_p50_p975
    FROM draws
    GROUP BY channel_language, language_display, channel_share, view_share
    ORDER BY view_share DESC
    """,

    "topic_repeated_samples": f"""
    WITH {BASE_COMPLETE},
    family_alloc AS (
      SELECT channel_id, yt_family, SUM(allocation_weight) AS allocation_weight
      FROM {ALLOC} GROUP BY channel_id, yt_family
    ),
    covered AS (
      SELECT b.*, PMOD(XXHASH64(b.channel_id), 4779) AS sample_id
      FROM base_complete b
      JOIN (SELECT channel_id FROM family_alloc GROUP BY channel_id) a USING (channel_id)
    ),
    census AS (
      SELECT a.yt_family,
             SUM(a.allocation_weight) / (SELECT COUNT(*) FROM covered) AS channel_share,
             SUM(b.positive_views * a.allocation_weight)
               / (SELECT SUM(positive_views) FROM covered) AS view_share
      FROM covered b JOIN family_alloc a USING (channel_id)
      GROUP BY a.yt_family
    ),
    sample_totals AS (
      SELECT sample_id, COUNT(*) AS sample_n, SUM(positive_views) AS sample_views
      FROM covered GROUP BY sample_id
    ),
    sample_group AS (
      SELECT b.sample_id, a.yt_family,
             SUM(a.allocation_weight) AS group_channel_equivalents,
             SUM(b.positive_views * a.allocation_weight) AS group_views
      FROM covered b JOIN family_alloc a USING (channel_id)
      GROUP BY b.sample_id, a.yt_family
    ),
    draws AS (
      SELECT c.yt_family, c.channel_share, c.view_share, t.sample_id, t.sample_n,
             COALESCE(g.group_channel_equivalents, 0) / t.sample_n AS sample_channel_share,
             COALESCE(g.group_views, 0) / t.sample_views AS sample_view_share
      FROM census c CROSS JOIN sample_totals t
      LEFT JOIN sample_group g ON c.yt_family = g.yt_family AND t.sample_id = g.sample_id
    )
    SELECT yt_family, channel_share, view_share, COUNT(*) AS n_replicates,
           AVG(sample_n) AS mean_sample_n,
           AVG(sample_channel_share) AS mean_sample_channel_share,
           STDDEV_SAMP(sample_channel_share) AS sd_sample_channel_share,
           AVG(sample_view_share) AS mean_sample_view_share,
           STDDEV_SAMP(sample_view_share) AS sd_sample_view_share,
           percentile_approx(sample_view_share, array(0.025,0.5,0.975), 10000)
             AS sample_view_share_p025_p50_p975
    FROM draws GROUP BY yt_family, channel_share, view_share
    ORDER BY view_share DESC
    """,

    "subtopic_repeated_samples": f"""
    WITH {BASE_COMPLETE},
    leaf_alloc AS (
      SELECT channel_id, yt_family, yt_leaf, SUM(allocation_weight) AS allocation_weight
      FROM {ALLOC} GROUP BY channel_id, yt_family, yt_leaf
    ),
    covered AS (
      SELECT b.*, PMOD(XXHASH64(b.channel_id), 4779) AS sample_id
      FROM base_complete b
      JOIN (SELECT channel_id FROM leaf_alloc GROUP BY channel_id) a USING (channel_id)
    ),
    census AS (
      SELECT a.yt_family, a.yt_leaf,
             SUM(a.allocation_weight) / (SELECT COUNT(*) FROM covered) AS channel_share,
             SUM(b.positive_views * a.allocation_weight)
               / (SELECT SUM(positive_views) FROM covered) AS view_share
      FROM covered b JOIN leaf_alloc a USING (channel_id)
      GROUP BY a.yt_family, a.yt_leaf
    ),
    sample_totals AS (
      SELECT sample_id, COUNT(*) AS sample_n, SUM(positive_views) AS sample_views
      FROM covered GROUP BY sample_id
    ),
    sample_group AS (
      SELECT b.sample_id, a.yt_family, a.yt_leaf,
             SUM(a.allocation_weight) AS group_channel_equivalents,
             SUM(b.positive_views * a.allocation_weight) AS group_views
      FROM covered b JOIN leaf_alloc a USING (channel_id)
      GROUP BY b.sample_id, a.yt_family, a.yt_leaf
    ),
    draws AS (
      SELECT c.yt_family, c.yt_leaf, c.channel_share, c.view_share,
             t.sample_id, t.sample_n,
             COALESCE(g.group_channel_equivalents, 0) / t.sample_n AS sample_channel_share,
             COALESCE(g.group_views, 0) / t.sample_views AS sample_view_share
      FROM census c CROSS JOIN sample_totals t
      LEFT JOIN sample_group g
        ON c.yt_family = g.yt_family AND c.yt_leaf = g.yt_leaf
       AND t.sample_id = g.sample_id
    )
    SELECT yt_family, yt_leaf, channel_share, view_share, COUNT(*) AS n_replicates,
           AVG(sample_n) AS mean_sample_n,
           AVG(sample_channel_share) AS mean_sample_channel_share,
           STDDEV_SAMP(sample_channel_share) AS sd_sample_channel_share,
           AVG(sample_view_share) AS mean_sample_view_share,
           STDDEV_SAMP(sample_view_share) AS sd_sample_view_share,
           percentile_approx(sample_view_share, array(0.025,0.5,0.975), 10000)
             AS sample_view_share_p025_p50_p975
    FROM draws GROUP BY yt_family, yt_leaf, channel_share, view_share
    ORDER BY view_share DESC
    """,
}

# Preserve the exhaustive n≈1,000 partition outputs while making a directly
# comparable n≈10,000 specification (4,778,580 complete channels / 478 buckets).
for _dimension in ("language", "topic", "subtopic"):
    _source = f"{_dimension}_repeated_samples"
    QUERIES[f"{_source}_n10000"] = QUERIES[_source].replace(
        "PMOD(XXHASH64(channel_id), 4779)",
        "PMOD(XXHASH64(channel_id), 478)",
    ).replace(
        "PMOD(XXHASH64(b.channel_id), 4779)",
        "PMOD(XXHASH64(b.channel_id), 478)",
    )


def databricks_api(method: str, path: str, *extra_args: str) -> dict:
    cmd = [
        "env", "DATABRICKS_AUTH_STORAGE=plaintext", "databricks", "api", method, path,
        "--profile", PROFILE, "--output", "json", *extra_args,
    ]
    return json.loads(subprocess.check_output(cmd, cwd=ROOT, text=True))


def execute_sql(statement: str) -> dict:
    body = {
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
        "disposition": "INLINE",
        "statement": statement.strip(),
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
        time.sleep(3)
        response = databricks_api("get", f"/api/2.0/sql/statements/{statement_id}")
        state = response.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(json.dumps(response.get("status", response), indent=2))
    if "manifest" in response and "result" not in response:
        response["result"] = databricks_api(
            "get", f"/api/2.0/sql/statements/{statement_id}/result/chunks/0"
        )
    return response


def normalize(name: str, response: dict) -> dict:
    columns = [c["name"] for c in response["manifest"]["schema"]["columns"]]
    rows = response.get("result", {}).get("data_array", [])
    return {
        "query": name,
        "statement_id": response.get("statement_id"),
        "columns": columns,
        "rows": [dict(zip(columns, row)) for row in rows],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("query", choices=sorted(QUERIES))
    args = parser.parse_args()
    result = normalize(args.query, execute_sql(QUERIES[args.query]))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{args.query}.json"
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"query": args.query, "rows": len(result["rows"]), "output": str(path)}))


if __name__ == "__main__":
    main()
