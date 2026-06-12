# Databricks notebook source
# MAGIC %md
# MAGIC # Repair Topic Multi-label Batch Job Registry
# MAGIC
# MAGIC Rebuilds known `batch_jobs` rows after a failed/incomplete upsert, using the generated
# MAGIC `batch_files` table for model metadata and known provider batch/result ids captured during the run.

# COMMAND ----------
import json
import os
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


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


DEFAULT_KNOWN_RECORDS_JSON = json.dumps([
    {"provider": "deepseek", "model": "deepseek-v4-flash", "provider_batch_id": "deepseek_direct:/dbfs/FileStore/youtube_category_topic_multilabel_batches/results/category_topic_multilabel_random_1000_20260612/deepseek/deepseek-v4-flash/chunk_00000_results.jsonl", "provider_status": "completed"},
    {"provider": "deepseek", "model": "deepseek-v4-pro", "provider_batch_id": "deepseek_direct:/dbfs/FileStore/youtube_category_topic_multilabel_batches/results/category_topic_multilabel_random_1000_20260612/deepseek/deepseek-v4-pro/chunk_00000_results.jsonl", "provider_status": "completed"},
    {"provider": "gemini", "model": "gemini-3.1-pro-preview", "provider_batch_id": "batches/1p7zlen8m7lnr2wkvaxaa709hdr8z3y89fvf", "provider_status": "JOB_STATE_PENDING"},
    {"provider": "gemini", "model": "gemini-3.5-flash", "provider_batch_id": "batches/qrzaji6sug4xie435307y8w6xkgk66yqk0fz", "provider_status": "JOB_STATE_PENDING"},
    {"provider": "openai", "model": "gpt-5-nano", "provider_batch_id": "batch_6a2c70b692b48190a2b5a29244a5178d", "provider_status": "validating"},
    {"provider": "openai", "model": "gpt-5.4-mini", "provider_batch_id": "batch_6a2c726d44dc8190b1a62c2e5ed62e1e", "provider_status": "validating"},
    {"provider": "openai", "model": "gpt-5.4-nano", "provider_batch_id": "batch_6a2c726f0d788190bc7da5f57cc8083f", "provider_status": "validating"},
    {"provider": "openai", "model": "gpt-5.5", "provider_batch_id": "batch_6a2c726bd59c81909b4067fb5cbbbbdf", "provider_status": "validating"}
])

_create_text_widget("run_id", "category_topic_multilabel_random_1000_20260612")
_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("batch_files_table", "yt_category_topic_multilabel_1000_batch_files")
_create_text_widget("batch_jobs_table", "yt_category_topic_multilabel_1000_batch_jobs")
_create_text_widget("known_records_json", DEFAULT_KNOWN_RECORDS_JSON)

RUN_ID = _get_widget("run_id", "category_topic_multilabel_random_1000_20260612")
CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
BATCH_FILES_TABLE = _get_widget("batch_files_table", "yt_category_topic_multilabel_1000_batch_files")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_category_topic_multilabel_1000_batch_jobs")
KNOWN_RECORDS = json.loads(_get_widget("known_records_json", DEFAULT_KNOWN_RECORDS_JSON))


def table_ref(name: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in name.split("."))


def fqtn(table_name: str) -> str:
    return table_ref(table_name) if "." in table_name else table_ref(f"{CATALOG}.{SCHEMA}.{table_name}")


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def write_run_scoped(df, table_full: str):
    if "run_id" not in df.columns:
        df = df.withColumn("run_id", F.lit(RUN_ID))
    if not _table_exists_full(table_full):
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("run_id").saveAsTable(table_full)
        return
    existing = spark.table(table_full)
    for field in existing.schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    df = df.select(*existing.columns)
    spark.sql(f"DELETE FROM {table_full} WHERE run_id = {_sql_string(RUN_ID)}")
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_full)


batch_files_full = fqtn(BATCH_FILES_TABLE)
batch_jobs_full = fqtn(BATCH_JOBS_TABLE)

batch_job_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("model_tier", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("local_jsonl_path", StringType(), True),
    StructField("provider_file_id", StringType(), True),
    StructField("provider_batch_id", StringType(), True),
    StructField("provider_status", StringType(), True),
    StructField("submission_status", StringType(), True),
    StructField("submission_error", StringType(), True),
    StructField("submitted_at_utc", StringType(), True),
])

known_by_key = {
    (r["provider"], r["model"]): r
    for r in KNOWN_RECORDS
}
records = []
for row in (
    spark.table(batch_files_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .orderBy("provider", "model", "chunk_id")
    .collect()
):
    known = known_by_key.get((row["provider"], row["model"]))
    if not known:
        continue
    records.append((
        RUN_ID,
        row["provider"],
        row["model"],
        row["model_tier"],
        int(row["chunk_id"]),
        row["local_jsonl_path"],
        known.get("provider_file_id"),
        known.get("provider_batch_id"),
        known.get("provider_status"),
        "submitted",
        None,
        datetime.utcnow().isoformat(),
    ))

if not records:
    raise RuntimeError("No known batch job records to repair.")

repair_df = spark.createDataFrame(records, batch_job_schema)
write_run_scoped(repair_df, batch_jobs_full)

payload = {
    "run_id": RUN_ID,
    "batch_jobs_table": batch_jobs_full,
    "repaired_rows": len(records),
    "models": [f"{r[1]}:{r[2]}" for r in records],
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True))
