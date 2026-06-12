# Databricks notebook source
# MAGIC %md
# MAGIC # Topic/Genre 1k Batch Status Check

# COMMAND ----------
# MAGIC %pip install "openai>=2.0.0" anthropic "google-genai>=1.51.0" pandas pyarrow

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
from datetime import datetime, timezone
from typing import Any

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
        return value if value else default
    except Exception:
        return default


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "category_topic_random_1000_20260611")
_create_text_widget("batch_jobs_table", "yt_category_topic_random_1000_batch_jobs")
_create_text_widget("batch_files_table", "yt_category_topic_random_1000_batch_files")
_create_text_widget("status_snapshot_table", "yt_category_topic_random_1000_batch_status_check")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("openai_secret_key", "openai-api-key")
_create_text_widget("anthropic_secret_key", "anthropic-api-key")
_create_text_widget("gemini_secret_key", "gemini-api-key")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "category_topic_random_1000_20260611")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_category_topic_random_1000_batch_jobs")
BATCH_FILES_TABLE = _get_widget("batch_files_table", "yt_category_topic_random_1000_batch_files")
STATUS_SNAPSHOT_TABLE = _get_widget("status_snapshot_table", "yt_category_topic_random_1000_batch_status_check")
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai-api-key")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic-api-key")
GEMINI_SECRET_KEY = _get_widget("gemini_secret_key", "gemini-api-key")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def as_jsonable(value: Any):
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "to_json_dict"):
        return value.to_json_dict()
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): as_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [as_jsonable(v) for v in value]
    return str(value)


def enum_name(value: Any):
    if value is None:
        return None
    return getattr(value, "name", None) or getattr(value, "value", None) or str(value)


def get_secret(key: str) -> str:
    return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def write_run_scoped(df, table_full: str):
    if not _table_exists_full(table_full):
        df.write.format("delta").mode("overwrite").partitionBy("run_id").saveAsTable(table_full)
        return

    existing = spark.table(table_full)
    for field in existing.schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    df = df.select(*existing.columns)
    spark.sql(f"DELETE FROM {table_full} WHERE run_id = {_sql_string(RUN_ID)}")
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_full)


jobs = (
    spark.table(fqtn(BATCH_JOBS_TABLE)).alias("j")
    .where(F.col("j.run_id") == F.lit(RUN_ID))
    .where(F.col("j.submission_status") == F.lit("submitted"))
    .where(F.col("j.provider").isin("anthropic", "gemini", "openai"))
    .where(F.col("j.provider_batch_id").isNotNull())
    .join(
        spark.table(fqtn(BATCH_FILES_TABLE)).alias("f").where(F.col("f.run_id") == F.lit(RUN_ID)),
        on=["run_id", "provider", "model", "chunk_id", "local_jsonl_path"],
        how="left",
    )
    .select("provider", "model", "chunk_id", "n_requests", "provider_batch_id", "provider_file_id")
    .dropDuplicates(["provider", "model", "chunk_id", "provider_batch_id"])
    .orderBy("provider", "model", "chunk_id")
    .collect()
)

print(f"Checking {len(jobs)} submitted OpenAI/Anthropic/Gemini batches for run_id={RUN_ID}")

checked_at = datetime.now(timezone.utc).isoformat()
records = []
openai_client = None
anthropic_client = None
gemini_client = None

for row in jobs:
    provider = row["provider"]
    model = row["model"]
    chunk_id = int(row["chunk_id"])
    provider_batch_id = row["provider_batch_id"]
    provider_file_id = row["provider_file_id"]
    n_requests = int(row["n_requests"] or 0)
    provider_status = None
    request_counts_json = None
    result_counts_json = None
    output_ref_json = None
    error = None

    try:
        if provider == "openai":
            from openai import OpenAI

            if openai_client is None:
                openai_client = OpenAI(api_key=get_secret(OPENAI_SECRET_KEY))
            batch = openai_client.batches.retrieve(provider_batch_id)
            provider_status = getattr(batch, "status", None)
            request_counts_json = json.dumps(as_jsonable({
                "request_counts": getattr(batch, "request_counts", None),
                "created_at": getattr(batch, "created_at", None),
                "in_progress_at": getattr(batch, "in_progress_at", None),
                "finalizing_at": getattr(batch, "finalizing_at", None),
                "completed_at": getattr(batch, "completed_at", None),
                "failed_at": getattr(batch, "failed_at", None),
                "expired_at": getattr(batch, "expired_at", None),
                "cancelled_at": getattr(batch, "cancelled_at", None),
            }), ensure_ascii=False, sort_keys=True)
            output_ref_json = json.dumps(as_jsonable({
                "output_file_id": getattr(batch, "output_file_id", None),
                "error_file_id": getattr(batch, "error_file_id", None),
                "errors": getattr(batch, "errors", None),
            }), ensure_ascii=False, sort_keys=True)
        elif provider == "anthropic":
            import anthropic

            if anthropic_client is None:
                anthropic_client = anthropic.Anthropic(api_key=get_secret(ANTHROPIC_SECRET_KEY))
            batch = anthropic_client.messages.batches.retrieve(provider_batch_id)
            provider_status = getattr(batch, "processing_status", None)
            request_counts_json = json.dumps(as_jsonable(getattr(batch, "request_counts", None)), ensure_ascii=False, sort_keys=True)
            if provider_status == "ended":
                counts = {}
                for item in anthropic_client.messages.batches.results(provider_batch_id):
                    d = as_jsonable(item)
                    result_type = (((d or {}).get("result") or {}).get("type")) or "unknown"
                    counts[result_type] = counts.get(result_type, 0) + 1
                result_counts_json = json.dumps(counts, ensure_ascii=False, sort_keys=True)
        elif provider == "gemini":
            from google import genai

            if gemini_client is None:
                gemini_client = genai.Client(api_key=get_secret(GEMINI_SECRET_KEY))
            try:
                batch = gemini_client.batches.get(name=provider_batch_id)
            except TypeError:
                batch = gemini_client.batches.get(provider_batch_id)
            provider_status = enum_name(getattr(batch, "state", None))
            request_counts_json = json.dumps(as_jsonable({
                "create_time": getattr(batch, "create_time", None),
                "update_time": getattr(batch, "update_time", None),
                "end_time": getattr(batch, "end_time", None),
            }), ensure_ascii=False, sort_keys=True)
            output_ref_json = json.dumps(as_jsonable({
                "dest": getattr(batch, "dest", None),
                "output": getattr(batch, "output", None),
                "output_file": getattr(batch, "output_file", None),
                "error": getattr(batch, "error", None),
            }), ensure_ascii=False, sort_keys=True)
    except Exception as exc:
        error = repr(exc)[:2000]

    records.append((
        RUN_ID,
        checked_at,
        provider,
        model,
        chunk_id,
        n_requests,
        provider_batch_id,
        provider_file_id,
        provider_status,
        request_counts_json,
        result_counts_json,
        output_ref_json,
        error,
    ))
    print(provider, model, provider_batch_id, provider_status, error or "")

schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("checked_at_utc", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("n_requests", IntegerType(), True),
    StructField("provider_batch_id", StringType(), True),
    StructField("provider_file_id", StringType(), True),
    StructField("provider_status", StringType(), True),
    StructField("request_counts_json", StringType(), True),
    StructField("result_counts_json", StringType(), True),
    StructField("output_ref_json", StringType(), True),
    StructField("status_error", StringType(), True),
])

status_df = spark.createDataFrame(records, schema)
write_run_scoped(status_df, fqtn(STATUS_SNAPSHOT_TABLE))
display(status_df.orderBy("provider", "model", "chunk_id"))

summary = {
    "run_id": RUN_ID,
    "status_snapshot_table": fqtn(STATUS_SNAPSHOT_TABLE),
    "n_checked": len(records),
    "statuses": [
        {
            "provider": r[2],
            "model": r[3],
            "provider_status": r[8],
            "status_error": r[12],
        }
        for r in records
    ],
}
dbutils.notebook.exit(json.dumps(summary, sort_keys=True))
