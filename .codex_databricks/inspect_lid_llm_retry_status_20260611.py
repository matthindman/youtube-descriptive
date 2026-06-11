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
_create_text_widget("run_id", "too_full_20260609_retry_incomplete_20260611")
_create_text_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
_create_text_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
_create_text_widget("batch_files_table", "yt_lid_v3_too_full_20260609_llm_validation_requests_batch_files")
_create_text_widget("status_snapshot_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_retry_incomplete_20260611")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
REQUESTS_TABLE = _get_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
BATCH_FILES_TABLE = _get_widget("batch_files_table", "yt_lid_v3_too_full_20260609_llm_validation_requests_batch_files")
STATUS_SNAPSHOT_TABLE = _get_widget("status_snapshot_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


batch_jobs = (
    spark.table(fqtn(BATCH_JOBS_TABLE))
    .where(F.col("run_id") == F.lit(RUN_ID))
    .select(
        "provider", "model", "chunk_id", "n_requests", "provider_batch_id",
        "provider_file_id", "provider_status", "submission_status", "submission_error",
    )
    .orderBy("provider", "model", "chunk_id")
)

requests = (
    spark.table(fqtn(REQUESTS_TABLE))
    .where(F.col("run_id") == F.lit(RUN_ID))
    .groupBy("provider", "model", "model_tier")
    .agg(F.count(F.lit(1)).alias("n_requests"))
    .orderBy("provider", "model")
)

batch_files = (
    spark.table(fqtn(BATCH_FILES_TABLE))
    .where(F.col("run_id") == F.lit(RUN_ID))
    .select("provider", "model", "chunk_id", "local_jsonl_path", "n_requests", "n_bytes")
    .orderBy("provider", "model", "chunk_id")
)

try:
    status = (
        spark.table(fqtn(STATUS_SNAPSHOT_TABLE))
        .where(F.col("run_id") == F.lit(RUN_ID))
        .select(
            "provider", "model", "chunk_id", "n_requests", "provider_batch_id",
            "provider_status", "request_counts_json", "result_counts_json", "output_ref_json", "status_error",
        )
        .orderBy("provider", "model", "chunk_id")
    )
    status_rows = [row.asDict(recursive=True) for row in status.collect()]
except Exception as exc:
    status_rows = [{"error": repr(exc)[:2000]}]

result = {
    "requests": [row.asDict(recursive=True) for row in requests.collect()],
    "batch_files": [row.asDict(recursive=True) for row in batch_files.collect()],
    "batch_jobs": [row.asDict(recursive=True) for row in batch_jobs.collect()],
    "status": status_rows,
}
print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, sort_keys=True))
