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
_create_text_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
_create_text_widget("original_run_id", "too_full_20260609")
_create_text_widget("retry_run_id", "too_full_20260609_retry_incomplete_20260611")
_create_text_widget("exclude_providers", "openai")
_create_text_widget("retry_providers", "gemini,deepseek")
_create_text_widget("min_shared_classified", "31")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
ORIGINAL_RUN_ID = _get_widget("original_run_id", "too_full_20260609")
RETRY_RUN_ID = _get_widget("retry_run_id", "too_full_20260609_retry_incomplete_20260611")
EXCLUDE_PROVIDERS = {p.strip().lower() for p in _get_widget("exclude_providers", "openai").split(",") if p.strip()}
RETRY_PROVIDERS = {p.strip().lower() for p in _get_widget("retry_providers", "gemini,deepseek").split(",") if p.strip()}
MIN_SHARED_CLASSIFIED = int(_get_widget("min_shared_classified", "31"))


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


raw = spark.table(fqtn(RAW_RESULTS_TABLE))

selected = raw.where(
    (
        (F.col("run_id") == F.lit(RETRY_RUN_ID))
        & F.lower(F.col("provider")).isin(*sorted(RETRY_PROVIDERS))
    )
    | (
        (F.col("run_id") == F.lit(ORIGINAL_RUN_ID))
        & (~F.lower(F.col("provider")).isin(*sorted(RETRY_PROVIDERS | EXCLUDE_PROVIDERS)))
    )
)
if EXCLUDE_PROVIDERS:
    selected = selected.where(~F.lower(F.col("provider")).isin(*sorted(EXCLUDE_PROVIDERS)))

model_counts = (
    selected.groupBy("provider", "model", "model_tier", "run_id")
    .agg(
        F.count(F.lit(1)).alias("n_results"),
        F.sum(F.when(F.col("is_valid_panel_vote") == F.lit(True), 1).otherwise(0)).alias("n_valid_panel_votes"),
        F.sum(F.when(F.col("parse_error").isNotNull() | F.col("prediction_parse_error").isNotNull(), 1).otherwise(0)).alias("n_parse_errors"),
    )
    .orderBy("provider", "model", "run_id")
)

votes = (
    selected.where(F.col("is_valid_panel_vote") == F.lit(True))
    .select(
        "channel_id",
        "provider",
        "model",
        "model_tier",
        F.col("primary_language_label").alias("language_label"),
        F.col("pred_base_iso").alias("base_iso"),
    )
    .withColumn("model_key", F.concat_ws(":", F.col("provider"), F.col("model")))
)

a = votes.alias("a")
b = votes.alias("b")
agreement = (
    a.join(b, on="channel_id", how="inner")
    .where(F.col("a.model_key") < F.col("b.model_key"))
    .groupBy(
        F.col("a.provider").alias("provider_a"),
        F.col("a.model").alias("model_a"),
        F.col("a.model_tier").alias("model_tier_a"),
        F.col("b.provider").alias("provider_b"),
        F.col("b.model").alias("model_b"),
        F.col("b.model_tier").alias("model_tier_b"),
    )
    .agg(
        F.count(F.lit(1)).alias("n_both_classified"),
        F.sum(F.when(F.col("a.base_iso") == F.col("b.base_iso"), 1).otherwise(0)).alias("n_base_iso_agree"),
        F.sum(F.when(F.col("a.language_label") == F.col("b.language_label"), 1).otherwise(0)).alias("n_full_label_agree"),
    )
    .withColumn("base_iso_agreement_rate", F.round(F.col("n_base_iso_agree") / F.col("n_both_classified"), 4))
    .withColumn("full_label_agreement_rate", F.round(F.col("n_full_label_agree") / F.col("n_both_classified"), 4))
    .withColumn("same_provider", F.col("provider_a") == F.col("provider_b"))
    .withColumn("same_tier", F.col("model_tier_a") == F.col("model_tier_b"))
    .where(F.col("n_both_classified") >= F.lit(MIN_SHARED_CLASSIFIED))
    .orderBy(F.desc("n_both_classified"), "provider_a", "model_a", "provider_b", "model_b")
)

result = {
    "source": {
        "original_run_id": ORIGINAL_RUN_ID,
        "retry_run_id": RETRY_RUN_ID,
        "retry_providers": sorted(RETRY_PROVIDERS),
        "exclude_providers": sorted(EXCLUDE_PROVIDERS),
        "min_shared_classified": MIN_SHARED_CLASSIFIED,
    },
    "model_counts": [row.asDict(recursive=True) for row in model_counts.collect()],
    "agreement": [row.asDict(recursive=True) for row in agreement.collect()],
}

print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, sort_keys=True))
