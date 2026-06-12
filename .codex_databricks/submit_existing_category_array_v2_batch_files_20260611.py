# Databricks notebook source
# MAGIC %md
# MAGIC # Submit Existing Corrected Array-v2 Batch Files
# MAGIC
# MAGIC Submits already-materialized JSONL files for selected providers and updates only those provider rows in
# MAGIC the batch job registry.

# COMMAND ----------
# MAGIC %pip install "openai>=2.0.0" anthropic "google-genai>=1.51.0"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import re
from datetime import datetime
from typing import Any, Dict

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
        return os.environ.get(name.upper(), default)


_create_text_widget("run_id", "category_topic_random_1000_array_v2_20260611")
_create_text_widget("output_catalog", "dev_sean")
_create_text_widget("output_schema", "matt")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")
_create_text_widget("submit_provider_filter", "anthropic,gemini")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("openai_secret_key", "openai-api-key")
_create_text_widget("anthropic_secret_key", "anthropic-api-key")
_create_text_widget("gemini_secret_key", "gemini-api-key")
_create_text_widget("openai_reasoning_effort", "minimal")

RUN_ID = _get_widget("run_id", "category_topic_random_1000_array_v2_20260611")
OUTPUT_CATALOG = _get_widget("output_catalog", "dev_sean")
OUTPUT_SCHEMA = _get_widget("output_schema", "matt")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000")
SUBMIT_PROVIDER_FILTER = {p.strip().lower() for p in _get_widget("submit_provider_filter", "anthropic,gemini").split(",") if p.strip()}
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai-api-key")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic-api-key")
GEMINI_SECRET_KEY = _get_widget("gemini_secret_key", "gemini-api-key")
OPENAI_REASONING_EFFORT = _get_widget("openai_reasoning_effort", "minimal").strip().lower()


def table_ref(name: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in name.split("."))


def out_table(suffix: str) -> str:
    return table_ref(f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{OUTPUT_PREFIX}_{suffix}")


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


def get_secret(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


def safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", model)


def is_openai_reasoning_or_gpt5_model(model: str) -> bool:
    m = (model or "").lower()
    return m.startswith("gpt-5") or m.startswith("o1") or m.startswith("o3") or m.startswith("o4") or m.startswith("o-")


def openai_batch_endpoint_for_model(model: str) -> str:
    return "/v1/responses" if is_openai_reasoning_or_gpt5_model(model) else "/v1/chat/completions"


batch_files_full = out_table("batch_files")
batch_jobs_full = out_table("batch_jobs")


def submit_openai_batch(local_jsonl_path: str, model: str) -> Dict[str, Any]:
    from openai import OpenAI
    client = OpenAI(api_key=get_secret(SECRET_SCOPE, OPENAI_SECRET_KEY))
    with open(local_jsonl_path, "rb") as handle:
        uploaded = client.files.create(file=handle, purpose="batch")
    batch = client.batches.create(
        input_file_id=uploaded.id,
        endpoint=openai_batch_endpoint_for_model(model),
        completion_window="24h",
        metadata={"run_id": RUN_ID, "task": "youtube_topic_category_array_v2", "model": model},
    )
    return {"provider_file_id": uploaded.id, "provider_batch_id": batch.id, "provider_status": getattr(batch, "status", None)}


def submit_anthropic_batch(local_jsonl_path: str, model: str) -> Dict[str, Any]:
    import anthropic
    client = anthropic.Anthropic(api_key=get_secret(SECRET_SCOPE, ANTHROPIC_SECRET_KEY))
    requests_payload = []
    with open(local_jsonl_path, "r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                requests_payload.append(json.loads(line))
    batch = client.messages.batches.create(requests=requests_payload)
    return {"provider_file_id": None, "provider_batch_id": batch.id, "provider_status": getattr(batch, "processing_status", None)}


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


batch_job_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("local_jsonl_path", StringType(), True),
    StructField("provider_file_id", StringType(), True),
    StructField("provider_batch_id", StringType(), True),
    StructField("provider_status", StringType(), True),
    StructField("submission_status", StringType(), True),
    StructField("submission_error", StringType(), True),
    StructField("submitted_at_utc", StringType(), True),
])

batch_file_rows = (
    spark.table(batch_files_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("provider").isin(*sorted(SUBMIT_PROVIDER_FILTER)))
    .orderBy("provider", "model", "chunk_id")
    .collect()
)
if not batch_file_rows:
    raise RuntimeError(f"No batch_files rows for run_id={RUN_ID}, providers={sorted(SUBMIT_PROVIDER_FILTER)}")

job_records = []
for row in batch_file_rows:
    provider = row["provider"]
    model = row["model"]
    chunk_id = int(row["chunk_id"])
    local_path = row["local_jsonl_path"]
    try:
        if provider == "openai":
            result = submit_openai_batch(local_path, model)
        elif provider == "anthropic":
            result = submit_anthropic_batch(local_path, model)
        elif provider == "gemini":
            result = submit_gemini_batch(local_path, model)
        else:
            raise ValueError(f"Unsupported provider for this retry notebook: {provider}")
        status = "submitted"
        error = None
    except Exception as exc:
        result = {"provider_file_id": None, "provider_batch_id": None, "provider_status": None}
        status = "error"
        error = repr(exc)[:2000]
    job_records.append((RUN_ID, provider, model, chunk_id, local_path, result.get("provider_file_id"), result.get("provider_batch_id"), result.get("provider_status"), status, error, datetime.utcnow().isoformat()))
    print(provider, model, chunk_id, status, result, error)

jobs_df = spark.createDataFrame(job_records, batch_job_schema)

if _table_exists_full(batch_jobs_full):
    existing = spark.table(batch_jobs_full)
    current = existing.where(F.col("run_id") == F.lit(RUN_ID))
    replace_keys = jobs_df.select("provider", "model", "chunk_id").distinct()
    keep_current_rows = [
        row.asDict(recursive=True)
        for row in current.join(replace_keys, on=["provider", "model", "chunk_id"], how="left_anti").collect()
    ]
    keep_current = spark.createDataFrame(keep_current_rows, schema=existing.schema) if keep_current_rows else spark.createDataFrame([], schema=existing.schema)
    jobs_to_write = keep_current.unionByName(jobs_df, allowMissingColumns=True)
    for field in existing.schema.fields:
        if field.name not in jobs_to_write.columns:
            jobs_to_write = jobs_to_write.withColumn(field.name, F.lit(None).cast(field.dataType))
    jobs_to_write = jobs_to_write.select(*existing.columns)
    spark.sql(f"DELETE FROM {batch_jobs_full} WHERE run_id = {_sql_string(RUN_ID)}")
    jobs_to_write.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(batch_jobs_full)
else:
    jobs_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("run_id").saveAsTable(batch_jobs_full)

result = {
    "run_id": RUN_ID,
    "providers": sorted(SUBMIT_PROVIDER_FILTER),
    "n_batch_files": len(batch_file_rows),
    "job_records": [
        {
            "provider": r[1],
            "model": r[2],
            "submission_status": r[8],
            "provider_status": r[7],
            "submission_error": r[9],
        }
        for r in job_records
    ],
}
print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, sort_keys=True))
