# Databricks notebook source
import json

from pyspark.sql import functions as F


RUN_ID = "too_full_20260609_deepseek_nothinking_20260611"
REFERENCE_RUN_ID = "too_full_20260609"
CATALOG = "dev_sean"
SCHEMA = "matt"


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


requests_table = fqtn("yt_lid_v3_too_full_20260609_llm_validation_requests")
batch_jobs_table = fqtn("yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
raw_results_table = fqtn("yt_lid_v3_too_full_20260609_llm_validation_raw_results")
verdicts_table = fqtn("yt_lid_v3_too_full_20260609_llm_validation_verdicts")
agreement_table = fqtn("yt_lid_v3_too_full_20260609_llm_validation_model_agreement")


def rows_to_dicts(df):
    return [row.asDict(recursive=True) for row in df.collect()]


summary = {
    "run_id": RUN_ID,
    "sample_overlap_with_reference": {},
    "requests_by_model": rows_to_dicts(
        spark.table(requests_table)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .groupBy("provider", "model")
        .agg(F.count(F.lit(1)).alias("n_requests"))
        .orderBy("provider", "model")
    ),
    "batch_jobs": rows_to_dicts(
        spark.table(batch_jobs_table)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .select("provider", "model", "provider_status", "submission_status", "submission_error")
        .orderBy("provider", "model")
    ),
    "raw_results_by_model": rows_to_dicts(
        spark.table(raw_results_table)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .groupBy("provider", "model")
        .agg(
            F.count(F.lit(1)).alias("n_raw_results"),
            F.sum(F.when(F.col("parse_error").isNull(), 1).otherwise(0)).alias("n_extracted_text"),
        )
        .orderBy("provider", "model")
    ),
    "valid_votes_by_model": rows_to_dicts(
        spark.table(raw_results_table)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .groupBy("provider", "model")
        .agg(
            F.sum(F.when(F.col("is_valid_panel_vote") == F.lit(True), 1).otherwise(0)).alias("n_valid_panel_votes"),
            F.sum(F.when(F.col("prediction_parse_error").isNotNull(), 1).otherwise(0)).alias("n_prediction_parse_errors"),
        )
        .orderBy("provider", "model")
    ),
    "verdict_status": rows_to_dicts(
        spark.table(verdicts_table)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .groupBy("panel_status")
        .agg(F.count(F.lit(1)).alias("n_channels"))
        .orderBy("panel_status")
    ),
    "agreement": rows_to_dicts(
        spark.table(agreement_table)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .select(
            "provider_a",
            "model_a",
            "provider_b",
            "model_b",
            "n_both_classified",
            "n_base_iso_agree",
            "base_iso_agreement_rate",
            "n_normalized_base_iso_agree",
            "normalized_base_iso_agreement_rate",
        )
        .orderBy("provider_a", "model_a", "provider_b", "model_b")
    ),
}

new_channels = (
    spark.table(requests_table)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .select("channel_id")
    .distinct()
)
reference_channels = (
    spark.table(requests_table)
    .where(F.col("run_id") == F.lit(REFERENCE_RUN_ID))
    .select("channel_id")
    .distinct()
)
summary["sample_overlap_with_reference"] = {
    "reference_run_id": REFERENCE_RUN_ID,
    "reference_distinct_channels": reference_channels.count(),
    "new_distinct_channels": new_channels.count(),
    "overlap_channels": new_channels.join(reference_channels, on="channel_id", how="inner").count(),
    "new_minus_reference": new_channels.join(reference_channels, on="channel_id", how="left_anti").count(),
    "reference_minus_new": reference_channels.join(new_channels, on="channel_id", how="left_anti").count(),
}

dbutils.notebook.exit(json.dumps(summary, sort_keys=True))
