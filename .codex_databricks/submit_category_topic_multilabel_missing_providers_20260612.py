# Databricks notebook source
# MAGIC %md
# MAGIC # Submit Missing Provider Batches for Topic Multi-label Validation
# MAGIC
# MAGIC Reuses previously generated batch JSONL files and submits only provider/model rows that do
# MAGIC not already have `submission_status = 'submitted'` in `batch_jobs`.

# COMMAND ----------
# MAGIC %pip install anthropic "google-genai>=1.51.0"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List

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
_create_text_widget("batch_files_table", "yt_category_topic_multilabel_1000_batch_files")
_create_text_widget("batch_jobs_table", "yt_category_topic_multilabel_1000_batch_jobs")
_create_text_widget("provider_filter", "anthropic,gemini")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("anthropic_secret_key", "anthropic-api-key")
_create_text_widget("gemini_secret_key", "gemini-api-key")

RUN_ID = _get_widget("run_id", "category_topic_multilabel_random_1000_20260612")
CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
BATCH_FILES_TABLE = _get_widget("batch_files_table", "yt_category_topic_multilabel_1000_batch_files")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_category_topic_multilabel_1000_batch_jobs")
PROVIDER_FILTER = {p.strip().lower() for p in _get_widget("provider_filter", "anthropic,gemini").split(",") if p.strip()}
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic-api-key")
GEMINI_SECRET_KEY = _get_widget("gemini_secret_key", "gemini-api-key")


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


def get_secret(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


def safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", model)


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


def upsert_batch_job_records(records: List[tuple]) -> None:
    if not records:
        return
    batch_job_columns = [field.name for field in batch_job_schema.fields]
    if _table_exists_full(batch_jobs_full):
        replace_keys = {(r[1], r[2], int(r[4])) for r in records}
        preserved = []
        for row in spark.table(batch_jobs_full).where(F.col("run_id") == F.lit(RUN_ID)).collect():
            key = (row["provider"], row["model"], int(row["chunk_id"]))
            if key not in replace_keys:
                preserved.append(tuple(row[c] for c in batch_job_columns))
        write_run_scoped(spark.createDataFrame(preserved + records, batch_job_schema), batch_jobs_full)
        return
    write_run_scoped(spark.createDataFrame(records, batch_job_schema), batch_jobs_full)


def submit_anthropic_batch(local_jsonl_path: str) -> Dict[str, Any]:
    import anthropic
    client = anthropic.Anthropic(api_key=get_secret(SECRET_SCOPE, ANTHROPIC_SECRET_KEY))
    requests_payload = []
    with open(local_jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                requests_payload.append(json.loads(line))
    batch = client.messages.batches.create(requests=requests_payload)
    return {
        "provider_file_id": None,
        "provider_batch_id": batch.id,
        "provider_status": getattr(batch, "processing_status", None),
    }


def submit_gemini_batch(local_jsonl_path: str, model: str) -> Dict[str, Any]:
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=get_secret(SECRET_SCOPE, GEMINI_SECRET_KEY))
    uploaded_file = client.files.upload(
        file=local_jsonl_path,
        config=types.UploadFileConfig(display_name=f"{RUN_ID}_{safe_model_dir(model)}", mime_type="jsonl"),
    )
    batch = client.batches.create(model=model, src=uploaded_file.name, config={"display_name": f"{RUN_ID}_{safe_model_dir(model)}"})
    return {
        "provider_file_id": getattr(uploaded_file, "name", None),
        "provider_batch_id": getattr(batch, "name", None),
        "provider_status": getattr(getattr(batch, "state", None), "name", None) or getattr(batch, "state", None),
    }


files_to_submit = (
    spark.table(batch_files_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("provider").isin(*sorted(PROVIDER_FILTER)))
)
if _table_exists_full(batch_jobs_full):
    existing_submitted = (
        spark.table(batch_jobs_full)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .where(F.col("submission_status") == F.lit("submitted"))
        .select("provider", "model", "chunk_id")
        .dropDuplicates()
    )
    files_to_submit = files_to_submit.join(existing_submitted, on=["provider", "model", "chunk_id"], how="left_anti")

files_to_submit = files_to_submit.orderBy("provider", "model", "chunk_id")
records = []
for row in files_to_submit.collect():
    provider = row["provider"]
    model = row["model"]
    path = row["local_jsonl_path"]
    try:
        if provider == "anthropic":
            result = submit_anthropic_batch(path)
        elif provider == "gemini":
            result = submit_gemini_batch(path, model)
        else:
            raise ValueError(f"Unsupported provider for missing-provider submitter: {provider}")
        record = (
            RUN_ID,
            provider,
            model,
            row["model_tier"],
            int(row["chunk_id"]),
            path,
            result.get("provider_file_id"),
            result.get("provider_batch_id"),
            result.get("provider_status"),
            "submitted",
            None,
            datetime.utcnow().isoformat(),
        )
        print("Submitted", provider, model, row["chunk_id"], result)
    except Exception as exc:
        record = (
            RUN_ID,
            provider,
            model,
            row["model_tier"],
            int(row["chunk_id"]),
            path,
            None,
            None,
            None,
            "error",
            repr(exc)[:2000],
            datetime.utcnow().isoformat(),
        )
        print("Submission error", provider, model, row["chunk_id"], repr(exc))
    records.append(record)
    upsert_batch_job_records([record])

payload = {
    "run_id": RUN_ID,
    "provider_filter": sorted(PROVIDER_FILTER),
    "attempted": len(records),
    "submitted": sum(1 for r in records if r[9] == "submitted"),
    "errors": sum(1 for r in records if r[9] == "error"),
    "batch_jobs_table": batch_jobs_full,
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True, default=str))
