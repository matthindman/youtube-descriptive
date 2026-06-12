# Databricks notebook source
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

CHANNELS = "prod_tads.youtube_too.yt_sl_channels"
CATEGORY = "dev_sean.default.channel_category"
OUTPUT_PREFIX = "dev_sean.matt.yt_channel_category_histogram_20260612"
SAMPLE_SEED = "20260611_category_validation"

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
w = Window.partitionBy("channel_id").orderBy(F.desc("collected_at"), F.desc("collected_date"))

latest = (
    joined.withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn(
        "topic_category_urls",
        F.when(F.col("topic_categories").isNull(), F.array().cast("array<string>")).otherwise(F.col("topic_categories")),
    )
    .withColumn(
        "topic_category_urls",
        F.expr("array_distinct(filter(topic_category_urls, x -> x is not null and length(trim(x)) > 0))"),
    )
    .withColumn("topic_category_count", F.size(F.col("topic_category_urls")))
    .withColumn(
        "topic_slugs",
        F.expr(
            """
            transform(
              topic_category_urls,
              x -> case
                when x rlike '/wiki/' then regexp_replace(regexp_extract(x, '/wiki/(.*)$', 1), '%20', '_')
                else x
              end
            )
            """
        ),
    )
)

latest_with_missing = channels.join(
    latest.select("channel_id", "topic_category_urls", "topic_slugs", "topic_category_count", "collected_at"),
    on="channel_id",
    how="left",
).withColumn(
    "topic_category_count",
    F.when(F.col("topic_category_count").isNull(), F.lit(0)).otherwise(F.col("topic_category_count")),
)

sample_order = F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.lit(SAMPLE_SEED)))
sample_1000 = channels.orderBy(sample_order, F.col("channel_id")).limit(1000)
sample_latest = sample_1000.join(latest_with_missing, on="channel_id", how="left")


def frequency_table(base_df, universe_name):
    universe_count = base_df.count()
    exploded = (
        base_df.where(F.col("topic_category_count") > 0)
        .select(
            "channel_id",
            F.arrays_zip(F.col("topic_category_urls"), F.col("topic_slugs")).alias("topic_pairs"),
        )
        .select("channel_id", F.explode("topic_pairs").alias("topic_pair"))
        .select(
            "channel_id",
            F.col("topic_pair.topic_category_urls").cast("string").alias("topic_category_url"),
            F.col("topic_pair.topic_slugs").cast("string").alias("topic_category"),
        )
        .where(F.col("topic_category").isNotNull() & (F.length(F.trim("topic_category")) > 0))
        .dropDuplicates(["channel_id", "topic_category"])
    )

    return (
        exploded.groupBy("topic_category", "topic_category_url")
        .agg(F.countDistinct("channel_id").alias("n_channels"))
        .withColumn("universe", F.lit(universe_name))
        .withColumn("universe_n_channels", F.lit(universe_count))
        .withColumn("pct_channels", F.round(100.0 * F.col("n_channels") / F.lit(universe_count), 4))
        .select("universe", "topic_category", "topic_category_url", "n_channels", "universe_n_channels", "pct_channels")
        .orderBy(F.desc("n_channels"), F.asc("topic_category"))
    )


def array_length_table(base_df, universe_name):
    universe_count = base_df.count()
    return (
        base_df.groupBy("topic_category_count")
        .agg(F.countDistinct("channel_id").alias("n_channels"))
        .withColumn("universe", F.lit(universe_name))
        .withColumn("universe_n_channels", F.lit(universe_count))
        .withColumn("pct_channels", F.round(100.0 * F.col("n_channels") / F.lit(universe_count), 4))
        .select("universe", "topic_category_count", "n_channels", "universe_n_channels", "pct_channels")
        .orderBy("topic_category_count")
    )


full_freq = frequency_table(latest_with_missing, "full_youtube_too")
sample_freq = frequency_table(sample_latest, "sample_1000")
freq = full_freq.unionByName(sample_freq)

full_lengths = array_length_table(latest_with_missing, "full_youtube_too")
sample_lengths = array_length_table(sample_latest, "sample_1000")
lengths = full_lengths.unionByName(sample_lengths)

full_distinct_topic_categories = full_freq.select("topic_category").distinct().count()
sample_distinct_topic_categories = sample_freq.select("topic_category").distinct().count()

(
    freq.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{OUTPUT_PREFIX}_frequency")
)

(
    lengths.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{OUTPUT_PREFIX}_array_lengths")
)

summary = {
    "channels_table": CHANNELS,
    "category_table": CATEGORY,
    "frequency_table": f"{OUTPUT_PREFIX}_frequency",
    "array_length_table": f"{OUTPUT_PREFIX}_array_lengths",
    "array_rule": "topic_categories is handled as an array; histogram explodes all distinct non-empty elements per channel.",
    "full_youtube_too": latest_with_missing.agg(
        F.countDistinct("channel_id").alias("n_channels"),
        F.sum(F.when(F.col("collected_at").isNotNull(), 1).otherwise(0)).alias("n_channels_with_category_row"),
        F.sum(F.when(F.col("topic_category_count") > 0, 1).otherwise(0)).alias("n_channels_with_nonempty_topic_categories"),
        F.max("collected_at").alias("max_collected_at"),
        F.min("collected_at").alias("min_collected_at_in_overlap"),
    ).collect()[0].asDict(recursive=True),
    "sample_1000": sample_latest.agg(
        F.countDistinct("channel_id").alias("n_channels"),
        F.sum(F.when(F.col("collected_at").isNotNull(), 1).otherwise(0)).alias("n_channels_with_category_row"),
        F.sum(F.when(F.col("topic_category_count") > 0, 1).otherwise(0)).alias("n_channels_with_nonempty_topic_categories"),
    ).collect()[0].asDict(recursive=True),
    "full_frequency": [row.asDict(recursive=True) for row in full_freq.collect()],
    "sample_frequency": [row.asDict(recursive=True) for row in sample_freq.collect()],
    "full_array_lengths": [row.asDict(recursive=True) for row in full_lengths.collect()],
    "sample_array_lengths": [row.asDict(recursive=True) for row in sample_lengths.collect()],
}

summary["full_youtube_too"]["distinct_topic_categories"] = full_distinct_topic_categories
summary["sample_1000"]["distinct_topic_categories"] = sample_distinct_topic_categories

for scope in ["full_youtube_too", "sample_1000"]:
    total = summary[scope]["n_channels"]
    for key in ["n_channels_with_category_row", "n_channels_with_nonempty_topic_categories"]:
        summary[scope][f"pct_{key.removeprefix('n_channels_')}"] = round(100.0 * summary[scope][key] / total, 4)

print(json.dumps(summary, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(summary, sort_keys=True, default=str))
