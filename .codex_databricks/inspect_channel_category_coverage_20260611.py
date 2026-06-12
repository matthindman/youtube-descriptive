# Databricks notebook source
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

CHANNELS = "prod_tads.youtube_too.yt_sl_channels"
CATEGORY = "dev_sean.default.channel_category"

spark.conf.set("spark.databricks.remoteFiltering.blockSelfJoins", "false")

channels = (
    spark.table(CHANNELS)
    .select(F.col("channel_id").cast("string").alias("channel_id"))
    .where(F.col("channel_id").isNotNull())
    .dropDuplicates(["channel_id"])
)

raw_categories = (
    spark.table(CATEGORY)
    .select(
        F.col("canonical_id").cast("string").alias("channel_id"),
        F.col("topic_categories"),
        F.col("collected_at"),
        F.col("collected_date"),
    )
    .where(F.col("canonical_id").isNotNull())
)

joined = raw_categories.join(F.broadcast(channels), on="channel_id", how="inner")
w = Window.partitionBy("channel_id").orderBy(F.desc("collected_at"))
latest = (
    joined.withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("topic_category_count", F.size(F.col("topic_categories")))
    .withColumn(
        "topic_slugs",
        F.expr("transform(topic_categories, x -> case when x rlike '/wiki/' then regexp_extract(x, '/wiki/(.*)$', 1) else x end)"),
    )
)

sample_order = F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.lit("20260611_category_validation")))
sample_1000 = channels.orderBy(sample_order, F.col("channel_id")).limit(1000)

summary = (
    channels.join(
        latest.select("channel_id", "topic_category_count", "collected_at"),
        on="channel_id",
        how="left",
    )
    .agg(
        F.count("*").alias("n_channels"),
        F.sum(F.when(F.col("collected_at").isNotNull(), 1).otherwise(0)).alias("n_channels_with_category_row"),
        F.sum(F.when(F.col("topic_category_count") > 0, 1).otherwise(0)).alias("n_channels_with_nonempty_topic_categories"),
        F.max("collected_at").alias("max_collected_at"),
        F.min("collected_at").alias("min_collected_at_in_overlap"),
    )
    .collect()[0]
    .asDict(recursive=True)
)

sample_summary = (
    sample_1000.join(
        latest.select("channel_id", "topic_category_count", "collected_at"),
        on="channel_id",
        how="left",
    )
    .agg(
        F.count("*").alias("sample_n_channels"),
        F.sum(F.when(F.col("collected_at").isNotNull(), 1).otherwise(0)).alias("sample_n_channels_with_category_row"),
        F.sum(F.when(F.col("topic_category_count") > 0, 1).otherwise(0)).alias("sample_n_channels_with_nonempty_topic_categories"),
    )
    .collect()[0]
    .asDict(recursive=True)
)

for key in ["n_channels_with_category_row", "n_channels_with_nonempty_topic_categories"]:
    summary[f"pct_{key.removeprefix('n_channels_')}"] = round(100.0 * summary[key] / summary["n_channels"], 2)

for key in ["sample_n_channels_with_category_row", "sample_n_channels_with_nonempty_topic_categories"]:
    sample_summary[f"pct_{key.removeprefix('sample_n_channels_')}"] = round(100.0 * sample_summary[key] / sample_summary["sample_n_channels"], 2)

top_topic_categories = [
    row.asDict(recursive=True)
    for row in (
        latest.where(F.col("topic_category_count") > 0)
        .select(F.explode_outer("topic_slugs").alias("topic_category"))
        .where(F.col("topic_category").isNotNull() & (F.length(F.trim("topic_category")) > 0))
        .groupBy("topic_category")
        .agg(F.count("*").alias("n_channels"))
        .orderBy(F.desc("n_channels"), F.asc("topic_category"))
        .limit(20)
        .collect()
    )
]

result = {
    "channels_table": CHANNELS,
    "category_table": CATEGORY,
    "category_rule": "latest row per canonical_id; topic_categories is a multi-label array; order is not treated as truth",
    "full_universe": summary,
    "deterministic_random_1000_preview": sample_summary,
    "top_topic_categories_exploded": top_topic_categories,
}

print(json.dumps(result, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(result, sort_keys=True, default=str))
