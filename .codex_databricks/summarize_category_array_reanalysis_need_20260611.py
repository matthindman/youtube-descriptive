# Databricks notebook source
import json
import os
import re

from pyspark.sql import functions as F


def _create_text_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default, name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return os.environ.get(name.upper(), default)


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "category_topic_random_1000_20260611")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "category_topic_random_1000_20260611")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def out_table(suffix: str) -> str:
    return fqtn(f"{OUTPUT_PREFIX}_{suffix}")


prompt = spark.table(out_table("prompt_inputs")).where(F.col("run_id") == F.lit(RUN_ID))
summary = spark.table(out_table("agreement_summary")).where(F.col("run_id") == F.lit(RUN_ID))
channel_agreement = spark.table(out_table("channel_agreement")).where(F.col("run_id") == F.lit(RUN_ID))

prompt_versions = [r.asDict(recursive=True) for r in prompt.groupBy("prompt_version").count().orderBy("prompt_version").collect()]

primary_labels = prompt.where(F.col("primary_topic_slug").isNotNull()).select(F.col("primary_topic_slug").alias("label")).dropDuplicates()
array_labels = prompt.select(F.explode_outer("topic_slugs").alias("label")).where(F.col("label").isNotNull()).dropDuplicates()
array_not_primary = array_labels.join(primary_labels, on="label", how="left_anti")
channels_with_array_only_labels = (
    prompt
    .withColumn("label", F.explode_outer("topic_slugs"))
    .join(array_not_primary, on="label", how="inner")
    .select("channel_id")
    .dropDuplicates()
    .count()
)

first_prompt = prompt.select("system_prompt").where(F.col("system_prompt").isNotNull()).limit(1).collect()
prompt_allowed_labels = []
if first_prompt:
    text = first_prompt[0]["system_prompt"] or ""
    prompt_allowed_labels = sorted(set(re.findall(r"^- ([^:]+):", text, flags=re.MULTILINE)))

array_label_values = {r["label"] for r in array_labels.collect()}
primary_label_values = {r["label"] for r in primary_labels.collect()}
prompt_allowed_values = set(prompt_allowed_labels)

agreement_rows = [
    r.asDict(recursive=True)
    for r in (
        summary
        .select(
            "provider",
            "model",
            "n_with_any_reference",
            "n_valid_predictions",
            "n_agree_any_topic",
            "agreement_any_topic_strict",
            "agreement_any_topic_valid_only",
            "n_agree_primary",
            "agreement_primary_strict",
        )
        .orderBy(F.desc("agreement_any_topic_strict"), "provider", "model")
        .collect()
    )
]

issue_counts = [
    r.asDict(recursive=True)
    for r in (
        channel_agreement
        .groupBy("issue_type", "severity")
        .count()
        .orderBy(F.desc("count"), "issue_type")
        .collect()
    )
]

result = {
    "run_id": RUN_ID,
    "prompt_versions": prompt_versions,
    "n_prompt_channels": prompt.count(),
    "distinct_primary_labels": len(primary_label_values),
    "distinct_array_labels": len(array_label_values),
    "distinct_prompt_allowed_labels_parsed": len(prompt_allowed_values),
    "array_labels_not_primary_count": len(array_label_values - primary_label_values),
    "array_labels_not_in_prompt_allowlist_count": len(array_label_values - prompt_allowed_values),
    "channels_with_array_only_labels": channels_with_array_only_labels,
    "array_labels_not_in_prompt_allowlist": sorted(array_label_values - prompt_allowed_values),
    "reanalyze_existing_is_clean": bool((array_label_values - prompt_allowed_values) == set()),
    "agreement_summary": agreement_rows,
    "issue_counts": issue_counts,
}

print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
