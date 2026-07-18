# Databricks notebook source
# ruff: noqa: F821
"""Build an analysis-ready pilot base for below-10K treemap sensitivity work."""

import json

from pyspark.sql import Window
from pyspark.sql import functions as F


SAMPLE_TABLE = "dev_sean.matt.yt_banded_sample"
LANGUAGE_TABLE = "dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_channel_language_current"
TOPIC_TABLE = "dev_sean.default.channel_category"
STATS_TABLE = "dev_sean.default.yt_channel_stats_full"
OUTPUT_TABLE = "dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_treemap_pilot_channel_base"
SUMMARY_TABLE = "dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_treemap_pilot_summary"
LABEL_VERSION = "banded_lt10k_20260716_lid_deepseek_v1"
CURRENT_SNAPSHOT = "2026-07-13"
PRIOR_SNAPSHOT = "2026-06-15"


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
        F.col("canonical_id").cast("string").alias("channel_id"),
        F.col("subscriber_count").cast("long").alias("sampled_subscriber_count"),
        F.col("band").cast("int").alias("sample_band"),
        F.col("run_id").alias("sample_run_id"),
    )
)
language = spark.table(LANGUAGE_TABLE).drop("sample_run_id", "subscriber_count", "band")

stats_window = Window.partitionBy("canonical_id", F.to_date("collected_at")).orderBy(
    F.col("collected_at").desc()
)
stats = (
    spark.table(STATS_TABLE)
    .join(sample.select(F.col("channel_id").alias("canonical_id")), "canonical_id", "inner")
    .where(F.to_date("collected_at").isin(CURRENT_SNAPSHOT, PRIOR_SNAPSHOT))
    .select(
        F.col("canonical_id").cast("string").alias("canonical_id"),
        F.col("channel_name").cast("string").alias("stats_channel_name"),
        F.col("subscriber_count").cast("long").alias("stats_subscriber_count"),
        F.col("total_view_count").cast("double").alias("stats_total_view_count"),
        F.col("collected_at").cast("timestamp").alias("collected_at"),
    )
    .withColumn("_rn", F.row_number().over(stats_window))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)
current = stats.where(F.to_date("collected_at") == F.to_date(F.lit(CURRENT_SNAPSHOT))).select(
    F.col("canonical_id").alias("channel_id"),
    F.col("stats_channel_name").alias("current_channel_name"),
    F.col("stats_subscriber_count").alias("current_subscriber_count"),
    F.col("stats_total_view_count").alias("current_lifetime_views"),
    F.col("collected_at").alias("current_collected_at"),
)
prior = stats.where(F.to_date("collected_at") == F.to_date(F.lit(PRIOR_SNAPSHOT))).select(
    F.col("canonical_id").alias("channel_id"),
    F.col("stats_subscriber_count").alias("prior_subscriber_count"),
    F.col("stats_total_view_count").alias("prior_lifetime_views"),
    F.col("collected_at").alias("prior_collected_at"),
)

topic_window = Window.partitionBy("channel_id").orderBy(
    F.col("_topic_timestamp").desc_nulls_last(),
    F.col("_topic_date").desc_nulls_last(),
)
topics = (
    spark.table(TOPIC_TABLE)
    .select(
        F.col("canonical_id").cast("string").alias("channel_id"),
        F.col("topic_categories").cast("array<string>").alias("raw_topic_categories_source"),
        F.to_timestamp("collected_at").alias("_topic_timestamp"),
        F.to_date("collected_date").alias("_topic_date"),
    )
    .join(sample.select("channel_id"), "channel_id", "inner")
    .withColumn("_topic_rn", F.row_number().over(topic_window))
    .where(F.col("_topic_rn") == 1)
    .drop("_topic_rn")
)

joined = (
    sample
    .join(language, "channel_id", "left")
    .join(current, "channel_id", "left")
    .join(prior, "channel_id", "left")
    .join(topics, "channel_id", "left")
)
raw_delta = F.col("current_lifetime_views") - F.col("prior_lifetime_views")
pilot = (
    joined
    .withColumn("raw_4wk_views", raw_delta)
    .withColumn(
        "view_count_4wk",
        F.when(
            F.col("prior_lifetime_views").isNotNull()
            & (F.col("current_lifetime_views") >= F.col("prior_lifetime_views")),
            raw_delta,
        ).cast("double"),
    )
    .withColumn("avg_weekly_view_count", F.col("view_count_4wk") / F.lit(4.0))
    .withColumn("has_current_snapshot", F.col("current_collected_at").isNotNull())
    .withColumn("has_prior_snapshot", F.col("prior_collected_at").isNotNull())
    .withColumn("has_invalid_negative_delta", F.col("raw_4wk_views") < F.lit(0.0))
    .withColumn("has_valid_4wk_views", F.col("view_count_4wk").isNotNull())
    .withColumn("has_positive_4wk_views", F.col("view_count_4wk") > F.lit(0.0))
    .withColumn("below_10k_at_current_snapshot", F.col("current_subscriber_count") < F.lit(10000))
    .withColumn(
        "topic_row_present",
        F.col("_topic_timestamp").isNotNull()
        | F.col("_topic_date").isNotNull()
        | F.col("raw_topic_categories_source").isNotNull(),
    )
    .withColumn(
        "raw_topic_categories",
        F.coalesce(F.col("raw_topic_categories_source"), F.array().cast("array<string>")),
    )
    .withColumn("has_nonempty_topic_categories", F.size("raw_topic_categories") > 0)
    .withColumn("traffic_current_snapshot", F.lit(CURRENT_SNAPSHOT).cast("date"))
    .withColumn("traffic_prior_snapshot", F.lit(PRIOR_SNAPSHOT).cast("date"))
    .withColumn("pilot_base_version", F.lit("banded_lt10k_treemap_pilot_20260716_v1"))
    .drop("raw_topic_categories_source", "_topic_timestamp", "_topic_date")
)

pilot = pilot.cache()
qa = {
    "rows": pilot.count(),
    "distinct_channels": pilot.select("channel_id").distinct().count(),
    "language_rows": pilot.where(F.col("label_version") == F.lit(LABEL_VERSION)).count(),
    "classified_languages": pilot.where(F.col("is_language_classified")).count(),
    "und_languages": pilot.where(F.col("channel_language") == F.lit("und")).count(),
    "topic_rows": pilot.where(F.col("topic_row_present")).count(),
    "nonempty_topic_channels": pilot.where(F.col("has_nonempty_topic_categories")).count(),
    "current_snapshot_channels": pilot.where(F.col("has_current_snapshot")).count(),
    "prior_snapshot_channels": pilot.where(F.col("has_prior_snapshot")).count(),
    "both_snapshot_channels": pilot.where(F.col("has_current_snapshot") & F.col("has_prior_snapshot")).count(),
    "valid_nonnegative_delta_channels": pilot.where(F.col("has_valid_4wk_views")).count(),
    "positive_delta_channels": pilot.where(F.col("has_positive_4wk_views")).count(),
    "zero_delta_channels": pilot.where(F.col("view_count_4wk") == F.lit(0.0)).count(),
    "negative_delta_channels": pilot.where(F.col("has_invalid_negative_delta")).count(),
    "below_10k_at_current_snapshot": pilot.where(F.col("below_10k_at_current_snapshot")).count(),
    "at_or_above_10k_at_current_snapshot": pilot.where(F.col("current_subscriber_count") >= F.lit(10000)).count(),
}
expected = {
    "rows": 2000,
    "distinct_channels": 2000,
    "language_rows": 2000,
    "classified_languages": 1838,
    "und_languages": 162,
    "topic_rows": 1972,
    "nonempty_topic_channels": 1763,
    "current_snapshot_channels": 1967,
    "prior_snapshot_channels": 1972,
    "both_snapshot_channels": 1967,
    "valid_nonnegative_delta_channels": 1889,
    "positive_delta_channels": 1661,
    "zero_delta_channels": 228,
    "negative_delta_channels": 78,
    "below_10k_at_current_snapshot": 1913,
    "at_or_above_10k_at_current_snapshot": 54,
}
for metric, expected_value in expected.items():
    if qa[metric] != expected_value:
        raise AssertionError(f"{metric}={qa[metric]}; expected={expected_value}")

write_table(pilot, OUTPUT_TABLE)

language_views = [
    row.asDict(recursive=True)
    for row in pilot.where(F.col("has_valid_4wk_views"))
    .groupBy("channel_language")
    .agg(
        F.count(F.lit(1)).alias("channels_with_valid_delta"),
        F.sum("view_count_4wk").alias("sample_4wk_views"),
    )
    .orderBy(F.desc("sample_4wk_views"), F.asc("channel_language"))
    .limit(50)
    .collect()
]
band_views = [
    row.asDict(recursive=True)
    for row in pilot.groupBy("sample_band")
    .agg(
        F.count(F.lit(1)).alias("sample_channels"),
        F.sum(F.col("has_valid_4wk_views").cast("int")).alias("valid_delta_channels"),
        F.sum("view_count_4wk").alias("sample_4wk_views"),
    )
    .orderBy("sample_band")
    .collect()
]
summary = {
    "output_table": OUTPUT_TABLE,
    "sample_table": SAMPLE_TABLE,
    "language_table": LANGUAGE_TABLE,
    "topic_table": TOPIC_TABLE,
    "stats_table": STATS_TABLE,
    "current_snapshot": CURRENT_SNAPSHOT,
    "prior_snapshot": PRIOR_SNAPSHOT,
    "qa": qa,
    "language_views_unweighted_sample": language_views,
    "band_views_unweighted_sample": band_views,
    "sampling_warning": (
        "Equal 200-per-band pilot with no stored frame sizes or inclusion probabilities; "
        "do not extrapolate unweighted view or language shares to the below-10K population."
    ),
}
summary_rows = [
    ("banded_lt10k_treemap_pilot_20260716_v1", key, json.dumps(value, sort_keys=True, default=str))
    for key, value in summary.items()
]
write_table(
    spark.createDataFrame(summary_rows, "pilot_base_version string, metric string, value_json string"),
    SUMMARY_TABLE,
)

comment = (
    "Analysis-ready one-row-per-channel pilot joining the stratified below-10K sample to "
    "language labels, latest topics, and 2026-06-15 to 2026-07-13 traffic. Not population weighted."
)
spark.sql(f"COMMENT ON TABLE {OUTPUT_TABLE} IS '{comment}'")
spark.sql(
    f"ALTER TABLE {OUTPUT_TABLE} SET TBLPROPERTIES ("
    "'quality'='analysis_pilot', "
    f"'language.label_version'='{LABEL_VERSION}', "
    f"'traffic.current_snapshot'='{CURRENT_SNAPSHOT}', "
    f"'traffic.prior_snapshot'='{PRIOR_SNAPSHOT}', "
    "'sampling.population_weighted'='false')"
)

print("BANDED_LT10K_TREEMAP_PILOT_SUMMARY=" + json.dumps(summary, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(summary, sort_keys=True, default=str))
