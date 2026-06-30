# Databricks notebook source
"""Read-only status probe for the DeepSeek Flash full fallback run."""

import json

from pyspark.sql import functions as F


def _widget(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except Exception:
        return default


CATALOG = _widget("catalog", "dev_sean")
SCHEMA = _widget("schema", "matt")
BASE_RUN_ID = _widget(
    "base_run_id",
    "channel_crawl_full_20260623_deepseek_flash_full_fallback_20260630",
)
BASE_OUTPUT_PREFIX = _widget(
    "base_output_prefix",
    "yt_lid_v3_channel_crawl_full_20260623_deepseek_flash_full_fallback_20260630",
)
PHASE = _widget("phase", "smoke_5k").strip().lower()

PHASE_SUFFIX = {
    "preflight_only": "",
    "smoke_5k": "_smoke_5k",
    "pilot_50k": "_pilot_50k",
    "full": "",
}.get(PHASE)
if PHASE_SUFFIX is None:
    raise ValueError(f"Unsupported phase: {PHASE}")

RUN_ID = _widget("run_id", f"{BASE_RUN_ID}{PHASE_SUFFIX}")
OUTPUT_PREFIX = _widget("output_prefix", f"{BASE_OUTPUT_PREFIX}{PHASE_SUFFIX}")


def fqtn(table: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{CATALOG}.{table}"
    return f"{CATALOG}.{SCHEMA}.{table}"


def table_exists(table: str) -> bool:
    try:
        return spark.catalog.tableExists(fqtn(table))
    except Exception:
        return False


def count_run(table: str, run_col: str = "run_id"):
    if not table_exists(table):
        return None
    df = spark.table(fqtn(table))
    if run_col in df.columns:
        df = df.where(F.col(run_col) == F.lit(RUN_ID))
    return df.count()


requests_table = f"{OUTPUT_PREFIX}_llm_requests"
batch_files_table = f"{requests_table}_batch_files"
batch_jobs_table = f"{OUTPUT_PREFIX}_llm_batch_jobs"
raw_results_table = f"{OUTPUT_PREFIX}_llm_raw_results"
verdicts_table = f"{OUTPUT_PREFIX}_llm_verdicts"
model_agreement_table = f"{OUTPUT_PREFIX}_llm_model_agreement"
progress_table = f"{OUTPUT_PREFIX}_llm_run_progress"

summary = {
    "phase": PHASE,
    "run_id": RUN_ID,
    "output_prefix": OUTPUT_PREFIX,
    "tables": {
        "requests": fqtn(requests_table),
        "batch_files": fqtn(batch_files_table),
        "batch_jobs": fqtn(batch_jobs_table),
        "raw_results": fqtn(raw_results_table),
        "verdicts": fqtn(verdicts_table),
        "model_agreement": fqtn(model_agreement_table),
        "progress": fqtn(progress_table),
    },
    "counts": {
        "requests": count_run(requests_table),
        "batch_files": count_run(batch_files_table),
        "batch_jobs": count_run(batch_jobs_table),
        "raw_results": count_run(raw_results_table),
        "verdicts": count_run(verdicts_table),
        "model_agreement": count_run(model_agreement_table),
        "progress_rows": count_run(progress_table),
    },
}

if table_exists(progress_table):
    progress = spark.table(fqtn(progress_table)).where(F.col("run_id") == F.lit(RUN_ID))
    latest_rows = [
        row.asDict()
        for row in (
            progress
            .orderBy(F.col("event_timestamp").desc())
            .select("event_timestamp", "stage", "status", "metric", "value")
            .limit(20)
            .collect()
        )
    ]
    by_stage = [
        row.asDict()
        for row in (
            progress
            .groupBy("stage", "status")
            .count()
            .orderBy("stage", "status")
            .collect()
        )
    ]
    summary["latest_progress"] = latest_rows
    summary["progress_by_stage"] = by_stage

if table_exists(batch_jobs_table):
    jobs = spark.table(fqtn(batch_jobs_table)).where(F.col("run_id") == F.lit(RUN_ID))
    status_cols = [col for col in ["provider", "model", "provider_status"] if col in jobs.columns]
    if status_cols:
        summary["batch_job_status_counts"] = [
            row.asDict()
            for row in jobs.groupBy(*status_cols).count().orderBy(*status_cols).collect()
        ]

if table_exists(raw_results_table):
    raw = spark.table(fqtn(raw_results_table)).where(F.col("run_id") == F.lit(RUN_ID))
    status_cols = [col for col in ["provider", "model", "result_status", "status"] if col in raw.columns]
    if status_cols:
        summary["raw_status_counts"] = [
            row.asDict()
            for row in raw.groupBy(*status_cols).count().orderBy(*status_cols).collect()
        ]

print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False, default=str))
