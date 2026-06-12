# Databricks notebook source
# MAGIC %md
# MAGIC # Retry OpenAI Topic Multi-label Batches with OpenAI-Compatible JSON Schema
# MAGIC
# MAGIC Rewrites the existing OpenAI batch JSONL requests after removing unsupported JSON Schema
# MAGIC keywords, then submits replacement OpenAI batch jobs and updates `batch_jobs`.

# COMMAND ----------
import json
import os
import re
from datetime import datetime
from typing import Any, List

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


_create_text_widget("run_id", "category_topic_multilabel_random_1000_20260612")
_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("requests_table", "yt_category_topic_multilabel_1000_requests")
_create_text_widget("batch_jobs_table", "yt_category_topic_multilabel_1000_batch_jobs")
_create_text_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("openai_secret_key", "openai-api-key")

RUN_ID = _get_widget("run_id", "category_topic_multilabel_random_1000_20260612")
CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
REQUESTS_TABLE = _get_widget("requests_table", "yt_category_topic_multilabel_1000_requests")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_category_topic_multilabel_1000_batch_jobs")
BATCH_OUTPUT_DIR = _get_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches").rstrip("/")
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai-api-key")


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


def safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", model)


def strip_unsupported_schema_keywords(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: strip_unsupported_schema_keywords(item)
            for key, item in value.items()
            if key not in {"uniqueItems"}
        }
    if isinstance(value, list):
        return [strip_unsupported_schema_keywords(item) for item in value]
    return value


def openai_batch_endpoint_for_line(line_obj: dict) -> str:
    return line_obj.get("url") or "/v1/responses"


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


def upsert_batch_job_records(records: List[tuple]) -> None:
    if not records:
        return
    table_full = fqtn(BATCH_JOBS_TABLE)
    columns = [field.name for field in batch_job_schema.fields]
    if _table_exists_full(table_full):
        replace_keys = {(r[1], r[2], int(r[4])) for r in records}
        preserved = []
        for row in spark.table(table_full).where(F.col("run_id") == F.lit(RUN_ID)).collect():
            key = (row["provider"], row["model"], int(row["chunk_id"]))
            if key not in replace_keys:
                preserved.append(tuple(row[c] for c in columns))
        write_run_scoped(spark.createDataFrame(preserved + records, batch_job_schema), table_full)
        return
    write_run_scoped(spark.createDataFrame(records, batch_job_schema), table_full)


from openai import OpenAI

client = OpenAI(api_key=dbutils.secrets.get(scope=SECRET_SCOPE, key=OPENAI_SECRET_KEY))
requests_full = fqtn(REQUESTS_TABLE)
run_dir = os.path.join(BATCH_OUTPUT_DIR, RUN_ID, "openai_fixed_schema_20260612")
os.makedirs(run_dir, exist_ok=True)

groups = (
    spark.table(requests_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("provider") == F.lit("openai"))
    .select("provider", "model", "model_tier", "chunk_id")
    .distinct()
    .orderBy("model", "chunk_id")
    .collect()
)

records = []
for group in groups:
    model = group["model"]
    chunk_id = int(group["chunk_id"])
    provider_dir = os.path.join(run_dir, safe_model_dir(model))
    os.makedirs(provider_dir, exist_ok=True)
    local_path = os.path.join(provider_dir, f"chunk_{chunk_id:05d}.jsonl")
    endpoint = None
    n_lines = 0
    with open(local_path, "w", encoding="utf-8") as out:
        rows = (
            spark.table(requests_full)
            .where(F.col("run_id") == F.lit(RUN_ID))
            .where((F.col("provider") == F.lit("openai")) & (F.col("model") == F.lit(model)) & (F.col("chunk_id") == F.lit(chunk_id)))
            .select("batch_line")
            .toLocalIterator()
        )
        for row in rows:
            obj = json.loads(row["batch_line"])
            obj = strip_unsupported_schema_keywords(obj)
            endpoint = endpoint or openai_batch_endpoint_for_line(obj)
            out.write(json.dumps(obj, ensure_ascii=False) + "\n")
            n_lines += 1
    try:
        with open(local_path, "rb") as f:
            uploaded = client.files.create(file=f, purpose="batch")
        batch = client.batches.create(
            input_file_id=uploaded.id,
            endpoint=endpoint or "/v1/responses",
            completion_window="24h",
            metadata={"run_id": RUN_ID, "task": "youtube_topic_categories_multilabel_openai_fixed_schema", "model": model},
        )
        record = (
            RUN_ID,
            "openai",
            model,
            group["model_tier"],
            chunk_id,
            local_path,
            uploaded.id,
            batch.id,
            getattr(batch, "status", None),
            "submitted",
            None,
            datetime.utcnow().isoformat(),
        )
        print("Submitted fixed OpenAI batch", model, chunk_id, batch.id, getattr(batch, "status", None), f"n={n_lines}")
    except Exception as exc:
        record = (
            RUN_ID,
            "openai",
            model,
            group["model_tier"],
            chunk_id,
            local_path,
            None,
            None,
            None,
            "error",
            repr(exc)[:2000],
            datetime.utcnow().isoformat(),
        )
        print("OpenAI fixed-schema submission error", model, chunk_id, repr(exc))
    records.append(record)
    upsert_batch_job_records([record])

payload = {
    "run_id": RUN_ID,
    "attempted": len(records),
    "submitted": sum(1 for r in records if r[9] == "submitted"),
    "errors": sum(1 for r in records if r[9] == "error"),
    "batch_jobs_table": fqtn(BATCH_JOBS_TABLE),
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True))
