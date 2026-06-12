# Databricks notebook source
import json
import os


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
_create_text_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
_create_text_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
_create_text_widget("provider_batch_id", "batches/ghlsuvkqdjxjidmfx1252yfbs1iftu6tdtht")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
PROVIDER_BATCH_ID = _get_widget("provider_batch_id", "batches/ghlsuvkqdjxjidmfx1252yfbs1iftu6tdtht")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


table_full = fqtn(BATCH_JOBS_TABLE)
spark.sql(
    f"""
    UPDATE {table_full}
    SET
      provider_batch_id = {_sql_string(PROVIDER_BATCH_ID)},
      provider_file_id = CAST(NULL AS STRING),
      provider_status = 'JOB_STATE_RUNNING',
      submission_status = 'submitted',
      submission_error = CAST(NULL AS STRING)
    WHERE run_id = {_sql_string(RUN_ID)}
      AND provider = 'gemini'
      AND model = 'gemini-3.5-flash'
      AND chunk_id = 0
    """
)

row = (
    spark.table(table_full)
    .where(f"run_id = {_sql_string(RUN_ID)} AND provider = 'gemini' AND model = 'gemini-3.5-flash' AND chunk_id = 0")
    .select("provider", "model", "chunk_id", "provider_batch_id", "provider_status", "submission_status", "submission_error")
    .collect()
)

result = {"restored": [r.asDict(recursive=True) for r in row]}
print(json.dumps(result, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
