# Databricks notebook source
import json

from pyspark.sql import functions as F

CHANNELS_RAW = "dev_sean.default.channels"

df = spark.table(CHANNELS_RAW).select(
    F.col("channel_id").cast("string").alias("channel_id"),
    F.col("raw_json").cast("string").alias("raw_json"),
)

path_checks = [
    ("topic_details_0", "$.topicDetails.topicCategories[0]"),
    ("items_topic_details_0", "$.items[0].topicDetails.topicCategories[0]"),
    ("snippet_category_id", "$.snippet.categoryId"),
    ("items_snippet_category_id", "$.items[0].snippet.categoryId"),
    ("kind", "$.kind"),
    ("items_kind", "$.items[0].kind"),
]

exprs = []
for name, path in path_checks:
    exprs.append(
        F.sum(
            F.when(
                F.get_json_object(F.col("raw_json"), path).isNotNull()
                & (F.length(F.trim(F.get_json_object(F.col("raw_json"), path))) > 0),
                1,
            ).otherwise(0)
        ).alias(f"n_{name}")
    )

summary = df.agg(
    F.count("*").alias("n_rows"),
    F.countDistinct("channel_id").alias("n_distinct_channels"),
    F.sum(F.when(F.col("raw_json").rlike("(?i)topic"), 1).otherwise(0)).alias("n_raw_json_contains_topic"),
    F.sum(F.when(F.col("raw_json").rlike("(?i)categor"), 1).otherwise(0)).alias("n_raw_json_contains_categor"),
    F.sum(F.when(F.col("raw_json").rlike("(?i)genre"), 1).otherwise(0)).alias("n_raw_json_contains_genre"),
    *exprs,
).collect()[0].asDict(recursive=True)

samples = [
    row.asDict(recursive=True)
    for row in (
        df.where(F.col("raw_json").rlike("(?i)topic|categor|genre"))
        .select("channel_id", F.substring("raw_json", 1, 1800).alias("raw_json_prefix"))
        .limit(5)
        .collect()
    )
]

result = {"table": CHANNELS_RAW, "summary": summary, "samples": samples}
print(json.dumps(result, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
