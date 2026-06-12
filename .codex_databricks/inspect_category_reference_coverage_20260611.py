# Databricks notebook source
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

CHANNELS = "prod_tads.youtube_too.yt_sl_channels"
VIDEOS = "prod_tads.youtube_too.yt_sl_videos"
BRONZE_INGEST = "prod_tads.youtube_too.yt_bz_ingest"
BACKFILL_CHANNELS = "dev_sean.default.backfill_channels"

spark.conf.set("spark.databricks.remoteFiltering.blockSelfJoins", "false")

YT_CATEGORIES = [
    ("1", "Film & Animation"),
    ("2", "Autos & Vehicles"),
    ("10", "Music"),
    ("15", "Pets & Animals"),
    ("17", "Sports"),
    ("19", "Travel & Events"),
    ("20", "Gaming"),
    ("22", "People & Blogs"),
    ("23", "Comedy"),
    ("24", "Entertainment"),
    ("25", "News & Politics"),
    ("26", "Howto & Style"),
    ("27", "Education"),
    ("28", "Science & Technology"),
    ("29", "Nonprofits & Activism"),
]


def normalize_label_key(col):
    return F.regexp_replace(F.lower(F.trim(col.cast("string"))), "&", "and")


category_map_rows = []
for category_id, category_name in YT_CATEGORIES:
    name_key = category_name.lower()
    normalized_name_key = name_key.replace("&", "and")
    category_map_rows.append((category_id, category_id))
    category_map_rows.append((name_key, category_id))
    category_map_rows.append((normalized_name_key, category_id))

category_map = spark.createDataFrame(
    category_map_rows,
    ["label_key", "category_id"],
).dropDuplicates(["label_key"])

channels = (
    spark.table(CHANNELS)
    .select(F.col("channel_id").cast("string").alias("channel_id"))
    .where(F.col("channel_id").isNotNull())
    .dropDuplicates(["channel_id"])
)

videos = spark.table(VIDEOS).select(
    F.col("channel_id").cast("string").alias("channel_id"),
    F.col("video_id").cast("string").alias("video_id"),
    F.col("ai_label").cast("string").alias("ai_label"),
)

video_counts = (
    videos.where(F.col("channel_id").isNotNull())
    .groupBy("channel_id")
    .agg(
        F.count("*").alias("n_videos"),
        F.sum(
            F.when(F.col("ai_label").isNotNull() & (F.length(F.trim(F.col("ai_label"))) > 0), 1).otherwise(0)
        ).alias("n_ai_label_videos"),
    )
)

raw_reference_labels = (
    videos.where(
        F.col("channel_id").isNotNull()
        & F.col("ai_label").isNotNull()
        & (F.length(F.trim(F.col("ai_label"))) > 0)
    )
    .select("channel_id", normalize_label_key(F.col("ai_label")).alias("label_key"))
)

mapped_reference_labels = (
    raw_reference_labels.join(category_map, on="label_key", how="inner")
    .select("channel_id", "category_id")
)

bronze = spark.table(BRONZE_INGEST).select(
    F.col("channel_id").cast("string").alias("channel_id"),
    F.col("ai_label").cast("string").alias("ai_label"),
)

bronze_counts = (
    bronze.where(F.col("channel_id").isNotNull())
    .groupBy("channel_id")
    .agg(
        F.sum(
            F.when(F.col("ai_label").isNotNull() & (F.length(F.trim(F.col("ai_label"))) > 0), 1).otherwise(0)
        ).alias("n_bronze_ai_label_rows"),
    )
)

bronze_raw_reference_labels = (
    bronze.where(
        F.col("channel_id").isNotNull()
        & F.col("ai_label").isNotNull()
        & (F.length(F.trim(F.col("ai_label"))) > 0)
    )
    .select("channel_id", normalize_label_key(F.col("ai_label")).alias("label_key"))
)

bronze_mapped_reference_labels = (
    bronze_raw_reference_labels.join(category_map, on="label_key", how="inner")
    .select("channel_id", "category_id")
)

bronze_totals = bronze_mapped_reference_labels.groupBy("channel_id").agg(F.count("*").alias("reference_total_label_count"))
bronze_votes = (
    bronze_mapped_reference_labels.groupBy("channel_id", "category_id")
    .agg(F.count("*").alias("reference_label_vote_count"))
)
bronze_winners = (
    bronze_votes.join(bronze_totals, on="channel_id", how="inner")
    .withColumn("reference_label_agreement_fraction", F.col("reference_label_vote_count") / F.col("reference_total_label_count"))
)
w = Window.partitionBy("channel_id").orderBy(
    F.desc("reference_label_vote_count"),
    F.desc("reference_label_agreement_fraction"),
    F.asc(F.col("category_id").cast("int")),
)
bronze_reference_channel_labels = (
    bronze_winners.withColumn("reference_label_rank", F.row_number().over(w))
    .where(F.col("reference_label_rank") == 1)
    .where((F.col("reference_label_vote_count") >= 3) & (F.col("reference_label_agreement_fraction") >= 0.50))
    .select("channel_id", "category_id", "reference_label_vote_count", "reference_total_label_count", "reference_label_agreement_fraction")
)

totals = mapped_reference_labels.groupBy("channel_id").agg(F.count("*").alias("reference_total_label_count"))
votes = (
    mapped_reference_labels.groupBy("channel_id", "category_id")
    .agg(F.count("*").alias("reference_label_vote_count"))
)

winners = (
    votes.join(totals, on="channel_id", how="inner")
    .withColumn("reference_label_agreement_fraction", F.col("reference_label_vote_count") / F.col("reference_total_label_count"))
)
reference_channel_labels = (
    winners.withColumn("reference_label_rank", F.row_number().over(w))
    .where(F.col("reference_label_rank") == 1)
    .where((F.col("reference_label_vote_count") >= 3) & (F.col("reference_label_agreement_fraction") >= 0.50))
    .select("channel_id", "category_id", "reference_label_vote_count", "reference_total_label_count", "reference_label_agreement_fraction")
)

backfill_categories = (
    spark.table(BACKFILL_CHANNELS)
    .where(F.lower(F.trim(F.col("status"))) == F.lit("done"))
    .select(
        F.col("canonical_id").cast("string").alias("channel_id"),
        F.explode_outer(F.col("topic_categories")).cast("string").alias("heldout_topic_category"),
    )
    .where(
        F.col("channel_id").isNotNull()
        & F.col("heldout_topic_category").isNotNull()
        & (F.length(F.trim(F.col("heldout_topic_category"))) > 0)
    )
    .dropDuplicates(["channel_id", "heldout_topic_category"])
)

backfill_categories_any_status = (
    spark.table(BACKFILL_CHANNELS)
    .select(
        F.col("canonical_id").cast("string").alias("channel_id"),
        F.lower(F.trim(F.col("status"))).alias("status"),
        F.explode_outer(F.col("topic_categories")).cast("string").alias("heldout_topic_category"),
    )
    .where(
        F.col("channel_id").isNotNull()
        & F.col("heldout_topic_category").isNotNull()
        & (F.length(F.trim(F.col("heldout_topic_category"))) > 0)
    )
    .dropDuplicates(["channel_id", "heldout_topic_category"])
)

backfill_status_counts = (
    spark.table(BACKFILL_CHANNELS)
    .select(
        F.lower(F.trim(F.col("status"))).alias("status"),
        F.size(F.col("topic_categories")).alias("topic_category_count"),
    )
    .groupBy("status")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("topic_category_count") > 0, 1).otherwise(0)).alias("n_rows_with_topic_categories"),
    )
    .orderBy(F.desc("n_rows"))
)

sample_order = F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.lit("20260611_category_validation")))
sample_1000 = channels.orderBy(sample_order, F.col("channel_id")).limit(1000)

summary = (
    channels.join(video_counts, on="channel_id", how="left")
    .join(bronze_counts, on="channel_id", how="left")
    .join(reference_channel_labels.select("channel_id").withColumn("has_reference_label", F.lit(True)), on="channel_id", how="left")
    .join(bronze_reference_channel_labels.select("channel_id").withColumn("has_bronze_reference_label", F.lit(True)), on="channel_id", how="left")
    .join(backfill_categories.select("channel_id").dropDuplicates(["channel_id"]).withColumn("has_backfill_topic_category", F.lit(True)), on="channel_id", how="left")
    .join(backfill_categories_any_status.select("channel_id").dropDuplicates(["channel_id"]).withColumn("has_backfill_topic_category_any_status", F.lit(True)), on="channel_id", how="left")
    .agg(
        F.count("*").alias("n_channels"),
        F.sum(F.when(F.col("n_videos") > 0, 1).otherwise(0)).alias("n_channels_with_videos"),
        F.sum(F.when(F.col("n_ai_label_videos") > 0, 1).otherwise(0)).alias("n_channels_with_any_ai_label"),
        F.sum(F.when(F.col("n_ai_label_videos") >= 3, 1).otherwise(0)).alias("n_channels_with_3_ai_label_videos"),
        F.sum(F.when(F.col("has_reference_label") == F.lit(True), 1).otherwise(0)).alias("n_channels_passing_reference_threshold"),
        F.sum(F.when(F.col("n_bronze_ai_label_rows") > 0, 1).otherwise(0)).alias("n_channels_with_any_bronze_ai_label"),
        F.sum(F.when(F.col("n_bronze_ai_label_rows") >= 3, 1).otherwise(0)).alias("n_channels_with_3_bronze_ai_label_rows"),
        F.sum(F.when(F.col("has_bronze_reference_label") == F.lit(True), 1).otherwise(0)).alias("n_channels_passing_bronze_reference_threshold"),
        F.sum(F.when(F.col("has_backfill_topic_category") == F.lit(True), 1).otherwise(0)).alias("n_channels_with_backfill_topic_category"),
        F.sum(F.when(F.col("has_backfill_topic_category_any_status") == F.lit(True), 1).otherwise(0)).alias("n_channels_with_backfill_topic_category_any_status"),
    )
)

sample_summary = (
    sample_1000.join(video_counts, on="channel_id", how="left")
    .join(bronze_counts, on="channel_id", how="left")
    .join(reference_channel_labels.select("channel_id").withColumn("has_reference_label", F.lit(True)), on="channel_id", how="left")
    .join(bronze_reference_channel_labels.select("channel_id").withColumn("has_bronze_reference_label", F.lit(True)), on="channel_id", how="left")
    .join(backfill_categories.select("channel_id").dropDuplicates(["channel_id"]).withColumn("has_backfill_topic_category", F.lit(True)), on="channel_id", how="left")
    .join(backfill_categories_any_status.select("channel_id").dropDuplicates(["channel_id"]).withColumn("has_backfill_topic_category_any_status", F.lit(True)), on="channel_id", how="left")
    .agg(
        F.count("*").alias("sample_n_channels"),
        F.sum(F.when(F.col("n_ai_label_videos") > 0, 1).otherwise(0)).alias("sample_n_channels_with_any_ai_label"),
        F.sum(F.when(F.col("has_reference_label") == F.lit(True), 1).otherwise(0)).alias("sample_n_channels_passing_reference_threshold"),
        F.sum(F.when(F.col("n_bronze_ai_label_rows") > 0, 1).otherwise(0)).alias("sample_n_channels_with_any_bronze_ai_label"),
        F.sum(F.when(F.col("has_bronze_reference_label") == F.lit(True), 1).otherwise(0)).alias("sample_n_channels_passing_bronze_reference_threshold"),
        F.sum(F.when(F.col("has_backfill_topic_category") == F.lit(True), 1).otherwise(0)).alias("sample_n_channels_with_backfill_topic_category"),
        F.sum(F.when(F.col("has_backfill_topic_category_any_status") == F.lit(True), 1).otherwise(0)).alias("sample_n_channels_with_backfill_topic_category_any_status"),
    )
)

top_backfill_categories = (
    channels.join(backfill_categories, on="channel_id", how="inner")
    .groupBy("heldout_topic_category")
    .agg(F.count("*").alias("n_channels"))
    .orderBy(F.desc("n_channels"), "heldout_topic_category")
    .limit(20)
)

summary_row = summary.collect()[0].asDict(recursive=True)
sample_row = sample_summary.collect()[0].asDict(recursive=True)

for key in [
    "n_channels_with_videos",
    "n_channels_with_any_ai_label",
    "n_channels_with_3_ai_label_videos",
    "n_channels_passing_reference_threshold",
    "n_channels_with_any_bronze_ai_label",
    "n_channels_with_3_bronze_ai_label_rows",
    "n_channels_passing_bronze_reference_threshold",
    "n_channels_with_backfill_topic_category",
    "n_channels_with_backfill_topic_category_any_status",
]:
    summary_row[f"pct_{key.removeprefix('n_channels_')}"] = round(100.0 * summary_row[key] / summary_row["n_channels"], 2)

for key in [
    "sample_n_channels_with_any_ai_label",
    "sample_n_channels_passing_reference_threshold",
    "sample_n_channels_with_any_bronze_ai_label",
    "sample_n_channels_passing_bronze_reference_threshold",
    "sample_n_channels_with_backfill_topic_category",
    "sample_n_channels_with_backfill_topic_category_any_status",
]:
    sample_row[f"pct_{key.removeprefix('sample_n_channels_')}"] = round(100.0 * sample_row[key] / sample_row["sample_n_channels"], 2)

result = {
    "source_tables": {"channels": CHANNELS, "videos": VIDEOS, "bronze_ingest": BRONZE_INGEST, "backfill_channels": BACKFILL_CHANNELS},
    "reference_rule": {
        "label_source": "prod_tads.youtube_too.yt_sl_videos.ai_label",
        "min_reference_labeled_videos": 3,
        "min_reference_agreement_fraction": 0.50,
    },
    "heldout_topic_rule": {
        "label_source": "dev_sean.default.backfill_channels.topic_categories exploded array",
        "status_filter": "status = 'done'",
        "array_rule": "topic_categories is multi-label; coverage is channel-level nonempty array, top labels are exploded elements",
    },
    "full_universe": summary_row,
    "deterministic_random_1000_preview": sample_row,
    "top_backfill_topic_categories": [row.asDict(recursive=True) for row in top_backfill_categories.collect()],
    "backfill_status_counts": [row.asDict(recursive=True) for row in backfill_status_counts.collect()],
}

print(json.dumps(result, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
