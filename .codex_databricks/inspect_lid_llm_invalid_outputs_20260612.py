# Databricks notebook source
import json
import os

from pyspark.sql import functions as F


def _create_text_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default, name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None and value != "" else default
    except Exception:
        return os.environ.get(name.upper(), default)


def _get_int_widget(name: str, default: int) -> int:
    try:
        return int(_get_widget(name, str(default)))
    except Exception:
        return default


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_focused2_20260612")
_create_text_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
_create_text_widget("max_examples", "30")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_focused2_20260612")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
MAX_EXAMPLES = _get_int_widget("max_examples", 30)

raw_full = f"`{CATALOG}`.`{SCHEMA}`.`{RAW_RESULTS_TABLE}`"
raw = spark.table(raw_full).where(F.col("run_id") == F.lit(RUN_ID))

invalid = raw.where(F.col("is_valid_panel_vote") != F.lit(True))

reason_counts = (
    invalid
    .groupBy(
        "provider",
        "model",
        F.coalesce(F.col("result_status").cast("string"), F.lit("null")).alias("result_status"),
        F.coalesce(F.col("status").cast("string"), F.lit("null")).alias("prediction_status"),
        F.coalesce(F.col("parse_error").cast("string"), F.lit("null")).alias("parse_error"),
        F.coalesce(F.col("prediction_parse_error").cast("string"), F.lit("null")).alias("prediction_parse_error"),
    )
    .agg(F.count(F.lit(1)).alias("n"))
    .orderBy(F.desc("n"), "provider", "model")
)

examples = (
    invalid
    .withColumn(
        "example_priority",
        F.when(F.col("parse_error").isNotNull(), F.lit(0))
        .when(F.col("prediction_parse_error").isNotNull(), F.lit(1))
        .when(F.col("status") == F.lit("insufficient_text"), F.lit(3))
        .otherwise(F.lit(2)),
    )
    .select(
        "example_priority",
        "provider",
        "model",
        "channel_id",
        "result_status",
        "status",
        "primary_language_label",
        "primary_language_iso639_3",
        "primary_language_script",
        "parse_error",
        "prediction_parse_error",
        F.substring(F.col("raw_text"), 1, 700).alias("raw_text_prefix"),
    )
    .orderBy("example_priority", "provider", "model", "channel_id")
    .limit(MAX_EXAMPLES)
)

payload = {
    "run_id": RUN_ID,
    "n_invalid": invalid.count(),
    "reason_counts": [row.asDict(recursive=True) for row in reason_counts.collect()],
    "examples": [row.asDict(recursive=True) for row in examples.collect()],
}

print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(payload, ensure_ascii=False, sort_keys=True))
