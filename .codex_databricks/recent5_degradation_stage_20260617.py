# Databricks notebook source
"""Stage the fixed all-random 1k sample for recent-5 vs recent-10 degradation testing."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import Window


dbutils.widgets.text("catalog", "dev_sean")
dbutils.widgets.text("schema", "matt")
dbutils.widgets.text("baseline_run_id", "too_full_20260609")
dbutils.widgets.text("baseline_requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
dbutils.widgets.text("baseline_raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
dbutils.widgets.text("baseline_verdicts_table", "yt_lid_v3_too_full_20260609_llm_validation_verdicts")
dbutils.widgets.text("baseline_segments_input_table", "yt_lid_v3_too_full_20260609_segments_input")
dbutils.widgets.text("source_channels_fqtn", "prod_tads.youtube_too.yt_sl_channels")
dbutils.widgets.text("source_videos_fqtn", "prod_tads.youtube_too.yt_sl_videos")
dbutils.widgets.text("output_prefix", "yt_lid_v3_recent5_degradation_20260617")
dbutils.widgets.text("stage_videos_per_channel", "5")


CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BASELINE_RUN_ID = dbutils.widgets.get("baseline_run_id")
REQUESTS_TABLE = dbutils.widgets.get("baseline_requests_table")
RAW_RESULTS_TABLE = dbutils.widgets.get("baseline_raw_results_table")
VERDICTS_TABLE = dbutils.widgets.get("baseline_verdicts_table")
BASELINE_SEGMENTS_INPUT_TABLE = dbutils.widgets.get("baseline_segments_input_table")
SOURCE_CHANNELS_FQTN = dbutils.widgets.get("source_channels_fqtn")
SOURCE_VIDEOS_FQTN = dbutils.widgets.get("source_videos_fqtn")
OUTPUT_PREFIX = dbutils.widgets.get("output_prefix")
STAGE_VIDEOS_PER_CHANNEL = int(dbutils.widgets.get("stage_videos_per_channel"))

RANK_CANDIDATES = [
    "published_at",
    "publish_time",
    "published_time",
    "upload_date",
    "created_time",
    "created_at",
    "first_capture_time",
    "ingestion_timestamp",
    "capture_date",
]


def fqtn(table_name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"


def table_exists(table_name: str) -> bool:
    try:
        spark.table(fqtn(table_name)).limit(1).count()
        return True
    except Exception:
        return False


def first_present_column(columns: list[str], candidates: list[str]) -> str | None:
    by_lower = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in by_lower:
            return by_lower[candidate.lower()]
    return None


sample_table = f"{OUTPUT_PREFIX}_sample_channels"
staged_channels_table = f"{OUTPUT_PREFIX}_source_channels"
staged_videos_table = f"{OUTPUT_PREFIX}_source_videos"
preflight_table = f"{OUTPUT_PREFIX}_preflight"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")

requests = spark.table(fqtn(REQUESTS_TABLE)).where(F.col("run_id") == F.lit(BASELINE_RUN_ID))
sample = requests.select("channel_id").where(F.col("channel_id").isNotNull()).distinct()

sample_count = sample.count()
if sample_count != 1000:
    raise ValueError(
        f"Expected exactly 1,000 distinct baseline sample channels for {BASELINE_RUN_ID}; found {sample_count}."
    )
sample_channel_ids = [row["channel_id"] for row in sample.select("channel_id").collect()]
if len(sample_channel_ids) != sample_count:
    raise ValueError(
        f"Collected {len(sample_channel_ids)} channel IDs after counting {sample_count} distinct sample channels."
    )

if "lid_iso_disagree" in BASELINE_RUN_ID:
    raise ValueError(f"Refusing focused hard-case run_id as baseline: {BASELINE_RUN_ID}")

request_cols = set(requests.columns)
if "route_reason" in request_cols:
    bad_routes = (
        requests.where(F.lower(F.col("route_reason")).contains("lid_iso_disagreement"))
        .select("route_reason")
        .distinct()
        .collect()
    )
    if bad_routes:
        raise ValueError(f"Baseline request table appears to contain hard-case routing reasons: {bad_routes}")

video_cols = spark.table(SOURCE_VIDEOS_FQTN).columns
video_col_lc = {c.lower(): c for c in video_cols}
recency_col = None
for candidate in RANK_CANDIDATES:
    if candidate.lower() in video_col_lc:
        recency_col = video_col_lc[candidate.lower()]
        break

if recency_col is None:
    raise ValueError(
        "No video recency column found in "
        f"{SOURCE_VIDEOS_FQTN}; checked {', '.join(RANK_CANDIDATES)}. "
        "The recent-5 experiment must not fall back to hash ordering."
    )

raw_coverage = {}
if table_exists(RAW_RESULTS_TABLE):
    raw_results = spark.table(fqtn(RAW_RESULTS_TABLE)).where(F.col("run_id") == F.lit(BASELINE_RUN_ID))
    raw_coverage = {
        "baseline_raw_channels": raw_results.select("channel_id").distinct().count(),
        "baseline_raw_rows": raw_results.count(),
    }

verdict_coverage = {}
if table_exists(VERDICTS_TABLE):
    verdicts = spark.table(fqtn(VERDICTS_TABLE)).where(F.col("run_id") == F.lit(BASELINE_RUN_ID))
    panel_status_col = "panel_status" if "panel_status" in verdicts.columns else None
    panel_majority = verdicts.where(F.col(panel_status_col) == F.lit("panel_majority")) if panel_status_col else verdicts
    verdict_coverage = {
        "baseline_verdict_channels": verdicts.select("channel_id").distinct().count(),
        "baseline_panel_majority_channels": panel_majority.select("channel_id").distinct().count(),
    }

sample.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqtn(sample_table))

sample_channels = spark.table(fqtn(sample_table))
source_channels_native = (
    spark.table(SOURCE_CHANNELS_FQTN)
    .where(F.col("channel_id").isin(sample_channel_ids))
    .join(F.broadcast(sample_channels), on="channel_id", how="inner")
)
native_channel_name_col = first_present_column(
    source_channels_native.columns,
    ["channel_name", "title", "name", "display_name"],
)
native_channel_description_col = first_present_column(
    source_channels_native.columns,
    ["channel_description", "description", "about", "bio", "channel_about", "profile_description", "channel_text"],
)
native_channel_text = source_channels_native.select(
    "channel_id",
    (
        F.col(native_channel_name_col).cast("string")
        if native_channel_name_col
        else F.lit(None).cast("string")
    ).alias("_native_channel_name"),
    (
        F.col(native_channel_description_col).cast("string")
        if native_channel_description_col
        else F.lit(None).cast("string")
    ).alias("_native_channel_description"),
)

baseline_segments = spark.table(fqtn(BASELINE_SEGMENTS_INPUT_TABLE)).where(
    (F.col("run_id") == F.lit(BASELINE_RUN_ID))
    & F.col("segment_type").isin("channel_name", "channel_description")
)
baseline_channel_text = baseline_segments.groupBy("channel_id").agg(
    F.max(F.when(F.col("segment_type") == F.lit("channel_name"), F.col("text").cast("string"))).alias(
        "_baseline_channel_name"
    ),
    F.max(F.when(F.col("segment_type") == F.lit("channel_description"), F.col("text").cast("string"))).alias(
        "_baseline_channel_description"
    ),
)
source_channels = (
    sample_channels
    .join(native_channel_text, on="channel_id", how="left")
    .join(baseline_channel_text, on="channel_id", how="left")
    .select(
        "channel_id",
        F.coalesce(F.col("_native_channel_name"), F.col("_baseline_channel_name")).alias("channel_name"),
        F.coalesce(F.col("_native_channel_description"), F.col("_baseline_channel_description")).alias(
            "channel_description"
        ),
    )
)
source_videos_all = (
    spark.table(SOURCE_VIDEOS_FQTN)
    .where(F.col("channel_id").isin(sample_channel_ids))
    .join(F.broadcast(sample_channels), on="channel_id", how="inner")
    .withColumn("__recent5_rank_ts", F.col(recency_col))
)
if STAGE_VIDEOS_PER_CHANNEL > 0:
    tie_break_cols = []
    if "video_id" in {c.lower() for c in source_videos_all.columns}:
        video_id_source_col = next(c for c in source_videos_all.columns if c.lower() == "video_id")
        tie_break_cols.append(F.col(video_id_source_col).asc_nulls_last())
    else:
        tie_break_cols.append(F.xxhash64(*[F.col(c).cast("string") for c in source_videos_all.columns]).asc())
    source_videos = (
        source_videos_all
        .withColumn(
            "__recent5_stage_video_rank",
            F.row_number().over(
                Window.partitionBy("channel_id").orderBy(
                    F.col("__recent5_rank_ts").desc_nulls_last(),
                    *tie_break_cols,
                )
            ),
        )
        .where(F.col("__recent5_stage_video_rank") <= F.lit(STAGE_VIDEOS_PER_CHANNEL))
    )
else:
    source_videos = source_videos_all.withColumn("__recent5_stage_video_rank", F.lit(None).cast("int"))

source_channels.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fqtn(staged_channels_table)
)
source_videos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fqtn(staged_videos_table)
)

staged_channel_count = spark.table(fqtn(staged_channels_table)).select("channel_id").distinct().count()
if staged_channel_count != 1000:
    raise ValueError(f"Staged source channels should contain 1,000 distinct channels; found {staged_channel_count}.")

staged_video_channels = spark.table(fqtn(staged_videos_table)).select("channel_id").distinct().count()
staged_video_rows = spark.table(fqtn(staged_videos_table)).count()

metrics = {
    "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    "baseline_run_id": BASELINE_RUN_ID,
    "sample_channels": sample_count,
    "source_channels_fqtn": SOURCE_CHANNELS_FQTN,
    "source_videos_fqtn": SOURCE_VIDEOS_FQTN,
    "discovered_recency_column": recency_col,
    "canonical_recency_column": "__recent5_rank_ts",
    "stage_videos_per_channel": STAGE_VIDEOS_PER_CHANNEL,
    "staging_filter_mode": "channel_id_in_locked_sample",
    "native_source_channel_rows": source_channels_native.select("channel_id").distinct().count(),
    "staged_source_channels": staged_channel_count,
    "staged_video_channels": staged_video_channels,
    "staged_video_rows": staged_video_rows,
    **raw_coverage,
    **verdict_coverage,
}

preflight_df = spark.createDataFrame([(k, json.dumps(v, default=str)) for k, v in metrics.items()], ["metric", "value"])
preflight_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqtn(preflight_table))

print(json.dumps(metrics, indent=2, sort_keys=True, default=str))
