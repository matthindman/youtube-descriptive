# Databricks notebook source
# ruff: noqa: F821
"""Stage and dual-LID classify the 2,000-channel below-10K banded sample."""

import json

from pyspark.sql import functions as F


RUN_ID = "banded_lt10k_20260716"
PREFIX = "yt_lid_v3_banded_lt10k_20260716"
HASH_BUCKETS = 128
EXPECTED_CHANNELS = 2000
EXPECTED_BANDS = 10
EXPECTED_PER_BAND = 200
EXPECTED_CHANNELS_WITH_DESCRIPTION_ROWS = 1971
EXPECTED_CHANNELS_WITH_NONEMPTY_DESCRIPTIONS = 1488
EXPECTED_CHANNELS_WITH_VIDEOS = 1857

SAMPLE_TABLE = "dev_sean.matt.yt_banded_sample"
DESCRIPTIONS_TABLE = "dev_sean.matt.yt_banded_channel_descriptions"
VIDEOS_TABLE = "dev_sean.matt.yt_banded_channel_videos"
SOURCE_CHANNELS_TABLE = f"dev_sean.matt.{PREFIX}_source_channels"
SOURCE_VIDEOS_TABLE = f"dev_sean.matt.{PREFIX}_source_videos"
SUMMARY_TABLE = f"dev_sean.matt.{PREFIX}_lid_run_summary"
LID_NOTEBOOK = (
    "/Users/matt.hindman@researchaccelerator.org/"
    "banded_lt10k_language_20260716/01_language_openlid_v3_databricks"
)


def write_table(df, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


sample = (
    spark.table(SAMPLE_TABLE)
    .where(F.col("subscriber_count") < F.lit(10000))
    .select(
        F.col("canonical_id").alias("channel_id"),
        F.col("subscriber_count"),
        F.col("band"),
        F.col("run_id").alias("sample_run_id"),
    )
)
descriptions = spark.table(DESCRIPTIONS_TABLE).select(
    F.col("canonical_id").alias("channel_id"),
    "channel_name",
    "channel_description",
    "uploads_playlist_id",
    F.col("collected_at").alias("channel_collected_at"),
    F.col("collected_date").alias("channel_collected_date"),
)
source_channels = sample.join(descriptions, "channel_id", "left")

sample_count = sample.count()
sample_distinct = sample.select("channel_id").distinct().count()
source_count = source_channels.count()
source_distinct = source_channels.select("channel_id").distinct().count()
description_row_coverage = source_channels.where(
    F.col("channel_collected_at").isNotNull()
).count()
nonempty_description_coverage = source_channels.where(
    F.length(F.trim(F.coalesce(F.col("channel_description"), F.lit("")))) > 0
).count()
band_counts = {
    int(row["band"]): int(row["count"])
    for row in sample.groupBy("band").count().orderBy("band").collect()
}
if not (
    sample_count == sample_distinct == source_count == source_distinct == EXPECTED_CHANNELS
):
    raise AssertionError(
        "Below-10K sample cardinality mismatch: "
        f"sample={sample_count}, sample_distinct={sample_distinct}, "
        f"source={source_count}, source_distinct={source_distinct}"
    )
if len(band_counts) != EXPECTED_BANDS or any(
    count != EXPECTED_PER_BAND for count in band_counts.values()
):
    raise AssertionError(f"Expected 10 bands of 200 channels; got {band_counts}")
if description_row_coverage != EXPECTED_CHANNELS_WITH_DESCRIPTION_ROWS:
    raise AssertionError(
        f"Description-row coverage={description_row_coverage}; "
        f"expected={EXPECTED_CHANNELS_WITH_DESCRIPTION_ROWS}"
    )
if nonempty_description_coverage != EXPECTED_CHANNELS_WITH_NONEMPTY_DESCRIPTIONS:
    raise AssertionError(
        f"Non-empty description coverage={nonempty_description_coverage}; "
        f"expected={EXPECTED_CHANNELS_WITH_NONEMPTY_DESCRIPTIONS}"
    )

source_videos = (
    spark.table(VIDEOS_TABLE)
    .select(
        F.col("canonical_id").alias("channel_id"),
        "video_id",
        "video_title",
        "video_description",
        "published_at",
        "position",
        F.col("collected_at").alias("video_collected_at"),
        F.col("collected_date").alias("video_collected_date"),
    )
    .join(sample.select("channel_id"), "channel_id", "inner")
)
video_rows = source_videos.count()
video_channels = source_videos.select("channel_id").distinct().count()
if video_channels != EXPECTED_CHANNELS_WITH_VIDEOS:
    raise AssertionError(
        f"Video channel coverage={video_channels}; expected={EXPECTED_CHANNELS_WITH_VIDEOS}"
    )

write_table(source_channels, SOURCE_CHANNELS_TABLE)
write_table(source_videos, SOURCE_VIDEOS_TABLE)

output_suffixes = {
    "segments_input": "segments_input",
    "openlid_segments": "openlid_segments",
    "glotlid_segments": "glotlid_segments",
    "glotlid_native_segments": "glotlid_native_segments",
    "openlid_compact": "openlid_predictions_compact",
    "glotlid_compact": "glotlid_predictions_compact",
    "glotlid_native_compact": "glotlid_native_predictions_compact",
    "channel_text_features": "channel_text_features",
    "segment_model_comparison": "segment_model_comparison",
    "channel_votes": "channel_votes",
    "channel_model_aggregation": "channel_model_aggregation",
    "channel_model_comparison": "channel_model_comparison",
    "channels": "channels",
    "language_summary_full": "language_summary_full",
    "language_summary_rollup": "language_summary_rollup",
    "model_agreement_summary": "model_agreement_summary",
    "mixed_language_candidates": "mixed_language_candidates",
    "hindi_indic_audit": "hindi_indic_audit_candidates",
    "suspect_tail_audit": "suspect_tail_audit_sample",
    "high_risk_redirect": "high_risk_redirect_diagnostic",
    "manual_validation_sample": "manual_validation_sample",
    "unclassified_audit": "unclassified_audit",
    "source_language_confusion": "source_language_confusion",
    "dedupe_qa": "dedupe_qa",
    "preflight_estimate": "preflight_estimate",
    "ablation_summary": "ablation_summary",
    "run_progress": "run_progress",
}

lid_args = {
    "catalog": "dev_sean",
    "schema": "matt",
    "channels_table": f"{PREFIX}_source_channels",
    "videos_table": f"{PREFIX}_source_videos",
    "output_catalog": "dev_sean",
    "output_schema": "matt",
    "run_id": RUN_ID,
    "inference_hash_buckets": str(HASH_BUCKETS),
    "bucket_start": "0",
    "bucket_end": str(HASH_BUCKETS - 1),
    "channel_id_column": "channel_id",
    "video_id_column": "video_id",
    "channel_name_column": "channel_name",
    "channel_description_column": "channel_description",
    "video_title_column": "video_title",
    "video_description_column": "video_description",
    "video_rank_column": "position",
    "videos_per_channel": "10",
    "enable_openlid": "true",
    "enable_glotlid": "true",
    "glotlid_mode": "all_valid_segments",
    "glotlid_preprocessing_mode": "match_openlid",
    "prediction_output_mode": "compact",
    "production_mode": "true",
    "run_heavy_qa": "true",
    "enable_notebook_displays": "false",
    "create_validation_samples": "true",
    "run_ablation_aggregations": "false",
    "optimize_after_write": "false",
    "download_model_if_missing": "false",
    "min_num_partitions": "32",
    "max_num_partitions": "512",
    "target_segments_per_partition": "25000",
    "checkpoint_dir": f"dbfs:/tmp/yt_lid_v3/{RUN_ID}/checkpoints",
    "update_source_detected_language": "false",
}
for widget_suffix, table_suffix in output_suffixes.items():
    lid_args[f"output_{widget_suffix}_table"] = f"{PREFIX}_{table_suffix}"

print("Invoking dual-LID notebook with arguments:")
print(json.dumps(lid_args, indent=2, sort_keys=True))
lid_result = dbutils.notebook.run(LID_NOTEBOOK, 0, lid_args)
print("Dual-LID result:", lid_result)

channels_table = f"dev_sean.matt.{PREFIX}_channels"
segments_table = f"dev_sean.matt.{PREFIX}_segments_input"
comparison_table = f"dev_sean.matt.{PREFIX}_channel_model_comparison"
channels = spark.table(channels_table).where(
    (F.col("run_id") == F.lit(RUN_ID))
    & (F.col("inference_hash_buckets") == F.lit(HASH_BUCKETS))
)
segments = spark.table(segments_table).where(
    (F.col("run_id") == F.lit(RUN_ID))
    & (F.col("inference_hash_buckets") == F.lit(HASH_BUCKETS))
)
comparison = spark.table(comparison_table).where(
    (F.col("run_id") == F.lit(RUN_ID))
    & (F.col("inference_hash_buckets") == F.lit(HASH_BUCKETS))
)

post_counts = {
    "channels": channels.count(),
    "distinct_channels": channels.select("channel_id").distinct().count(),
    "segments": segments.count(),
    "valid_segments": segments.where(F.col("is_valid_text_for_lid")).count(),
    "comparison_channels": comparison.select("channel_id").distinct().count(),
}
if post_counts["channels"] != EXPECTED_CHANNELS or post_counts["distinct_channels"] != EXPECTED_CHANNELS:
    raise AssertionError(f"Final LID channel QA failed: {post_counts}")

status_distribution = [
    row.asDict(recursive=True)
    for row in channels.groupBy("consensus_status")
    .count()
    .orderBy(F.desc("count"))
    .collect()
]
summary = {
    "run_id": RUN_ID,
    "prefix": PREFIX,
    "source_tables": {
        "sample": SAMPLE_TABLE,
        "descriptions": DESCRIPTIONS_TABLE,
        "videos": VIDEOS_TABLE,
        "staged_channels": SOURCE_CHANNELS_TABLE,
        "staged_videos": SOURCE_VIDEOS_TABLE,
    },
    "source_counts": {
        "channels": source_count,
        "description_row_channels": description_row_coverage,
        "nonempty_description_channels": nonempty_description_coverage,
        "video_rows": video_rows,
        "video_channels": video_channels,
        "band_counts": band_counts,
    },
    "post_counts": post_counts,
    "status_distribution": status_distribution,
    "lid_notebook_result": lid_result,
}
summary_rows = [(RUN_ID, key, json.dumps(value, sort_keys=True, default=str)) for key, value in summary.items()]
write_table(
    spark.createDataFrame(summary_rows, "run_id string, metric string, value_json string"),
    SUMMARY_TABLE,
)
print("BANDED_LT10K_LID_SUMMARY=" + json.dumps(summary, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(summary, sort_keys=True, default=str))
