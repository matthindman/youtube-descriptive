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


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "too_full_20260609")
_create_text_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")

table_full = f"`{CATALOG}`.`{SCHEMA}`.`{RAW_RESULTS_TABLE}`"

counts = (
    spark.table(table_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .groupBy("provider", "model", "model_tier")
    .agg(
        F.count(F.lit(1)).alias("n_results"),
        F.sum(F.when(F.col("is_valid_panel_vote") == F.lit(True), 1).otherwise(0)).alias("n_valid_panel_votes"),
        F.sum(F.when(F.col("parse_error").isNotNull() | F.col("prediction_parse_error").isNotNull(), 1).otherwise(0)).alias("n_parse_errors"),
        F.sum(F.when(F.col("result_status").cast("string").rlike("^[45][0-9][0-9]$"), 1).otherwise(0)).alias("n_http_errors"),
    )
    .orderBy("provider", "model")
)

rows = [row.asDict(recursive=True) for row in counts.collect()]
print(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(rows, ensure_ascii=False, sort_keys=True))
