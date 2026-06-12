# Databricks notebook source
# MAGIC %md
# MAGIC # Materialize and Submit Corrected Array-v2 Topic Requests
# MAGIC
# MAGIC Reuses corrected `prompt_inputs` rows and materializes provider JSONL requests in driver-side Python.
# MAGIC This avoids the all-in-one verification notebook path that stalled after prompt materialization.

# COMMAND ----------
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, Optional

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


def _get_int_widget(name: str, default: int) -> int:
    raw = _get_widget(name, str(default)).strip()
    return int(raw) if raw else default


DEFAULT_MODELS_JSON = json.dumps([
    {"provider": "openai", "model": "gpt-5.5", "tier": "frontier"},
    {"provider": "openai", "model": "gpt-5.4-mini", "tier": "small"},
    {"provider": "openai", "model": "gpt-5.4-nano", "tier": "nano"},
    {"provider": "openai", "model": "gpt-5-nano", "tier": "nano_low_cost"},
    {"provider": "anthropic", "model": "claude-opus-4-8", "tier": "frontier"},
    {"provider": "anthropic", "model": "claude-sonnet-4-6", "tier": "mid"},
    {"provider": "anthropic", "model": "claude-haiku-4-5", "tier": "small"},
    {"provider": "gemini", "model": "gemini-3.1-pro-preview", "tier": "frontier"},
    {"provider": "gemini", "model": "gemini-3.5-flash", "tier": "mid"},
    {"provider": "gemini", "model": "gemini-3.1-flash-lite", "tier": "small"},
    {"provider": "deepseek", "model": "deepseek-v4-pro", "tier": "frontier"},
    {"provider": "deepseek", "model": "deepseek-v4-flash", "tier": "small"},
], ensure_ascii=False)


_create_text_widget("run_id", "category_topic_random_1000_array_v2_20260611")
_create_text_widget("output_catalog", "dev_sean")
_create_text_widget("output_schema", "matt")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")
_create_text_widget("models_json", DEFAULT_MODELS_JSON)
_create_text_widget("submit_provider_filter", "anthropic,gemini,openai")
_create_text_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_batches")
_create_text_widget("max_requests_per_file", "10000")
_create_text_widget("max_output_tokens", "700")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("openai_secret_key", "openai-api-key")
_create_text_widget("anthropic_secret_key", "anthropic-api-key")
_create_text_widget("gemini_secret_key", "gemini-api-key")
_create_text_widget("openai_reasoning_effort", "minimal")
_create_text_widget("gemini_thinking_level", "low")

RUN_ID = _get_widget("run_id", "category_topic_random_1000_array_v2_20260611")
OUTPUT_CATALOG = _get_widget("output_catalog", "dev_sean")
OUTPUT_SCHEMA = _get_widget("output_schema", "matt")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000")
MODELS = json.loads(_get_widget("models_json", DEFAULT_MODELS_JSON))
SUBMIT_PROVIDER_FILTER = {p.strip().lower() for p in _get_widget("submit_provider_filter", "anthropic,gemini,openai").split(",") if p.strip()}
BATCH_OUTPUT_DIR = _get_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_batches").rstrip("/")
MAX_REQUESTS_PER_FILE = _get_int_widget("max_requests_per_file", 10000)
MAX_OUTPUT_TOKENS = _get_int_widget("max_output_tokens", 700)
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai-api-key")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic-api-key")
GEMINI_SECRET_KEY = _get_widget("gemini_secret_key", "gemini-api-key")
OPENAI_REASONING_EFFORT = _get_widget("openai_reasoning_effort", "minimal").strip().lower()
GEMINI_THINKING_LEVEL = _get_widget("gemini_thinking_level", "low").strip()


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


def get_secret(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


def stable_request_id(provider: str, model: str, channel_id: str) -> str:
    import hashlib
    h = hashlib.sha256(f"{RUN_ID}||{provider}||{model}||{channel_id}".encode("utf-8")).hexdigest()
    return f"ytc_{h[:60]}"


def is_openai_reasoning_or_gpt5_model(model: Optional[str]) -> bool:
    m = (model or "").lower()
    return m.startswith("gpt-5") or m.startswith("o1") or m.startswith("o3") or m.startswith("o4") or m.startswith("o-")


def openai_batch_endpoint_for_model(model: Optional[str]) -> str:
    return "/v1/responses" if is_openai_reasoning_or_gpt5_model(model) else "/v1/chat/completions"


prompt_inputs_full = out_table("prompt_inputs")
requests_full = out_table("requests")
batch_files_full = out_table("batch_files")
batch_jobs_full = out_table("batch_jobs")

prompt_rows = [
    r.asDict(recursive=True)
    for r in (
        spark.table(prompt_inputs_full)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .select("run_id", "channel_id", "system_prompt", "prompt_user", "topic_slugs")
        .orderBy("channel_id")
        .collect()
    )
]
if not prompt_rows:
    raise RuntimeError(f"No prompt_inputs rows found for run_id={RUN_ID}")

allowed_slugs = sorted({
    str(slug)
    for row in prompt_rows
    for slug in (row.get("topic_slugs") or [])
    if slug is not None and str(slug).strip()
})
if not allowed_slugs:
    raise RuntimeError("No held-out topic labels found in prompt_inputs.topic_slugs.")

topic_response_json_schema = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "category_id": {"type": "string", "enum": allowed_slugs},
        "category_name": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "ambiguous": {"type": "boolean"},
        "rationale_short": {"type": "string", "maxLength": 180},
    },
    "required": ["category_id", "category_name", "confidence", "ambiguous", "rationale_short"],
}

models = [m for m in MODELS if m["provider"].lower() in SUBMIT_PROVIDER_FILTER]
print("prompt_rows", len(prompt_rows), "allowed_labels", len(allowed_slugs), "providers", sorted(SUBMIT_PROVIDER_FILTER))


def make_batch_line(provider: str, model: str, request_id: str, system_prompt: str, user_prompt: str, max_output_tokens: int) -> str:
    provider = (provider or "").lower()
    if provider == "openai":
        if is_openai_reasoning_or_gpt5_model(model):
            body = {
                "model": model,
                "instructions": system_prompt,
                "input": user_prompt,
                "max_output_tokens": max_output_tokens,
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "youtube_topic_category_prediction",
                        "schema": topic_response_json_schema,
                        "strict": True,
                    },
                    "verbosity": "low",
                },
            }
            if OPENAI_REASONING_EFFORT:
                body["reasoning"] = {"effort": OPENAI_REASONING_EFFORT}
            return json.dumps({"custom_id": request_id, "method": "POST", "url": "/v1/responses", "body": body}, ensure_ascii=False)
        body = {
            "model": model,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            "max_tokens": max_output_tokens,
        }
        return json.dumps({"custom_id": request_id, "method": "POST", "url": "/v1/chat/completions", "body": body}, ensure_ascii=False)
    if provider == "anthropic":
        return json.dumps({"custom_id": request_id, "params": {"model": model, "max_tokens": max_output_tokens, "system": system_prompt, "messages": [{"role": "user", "content": user_prompt}]}}, ensure_ascii=False)
    if provider == "gemini":
        generation_config = {"max_output_tokens": max_output_tokens, "response_mime_type": "application/json"}
        if GEMINI_THINKING_LEVEL:
            generation_config["thinking_config"] = {"thinking_level": GEMINI_THINKING_LEVEL}
        return json.dumps({"key": request_id, "request": {"system_instruction": {"parts": [{"text": system_prompt}]}, "contents": [{"role": "user", "parts": [{"text": user_prompt}]}], "generation_config": generation_config}}, ensure_ascii=False)
    if provider == "deepseek":
        body = {
            "model": model,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            "max_tokens": max_output_tokens,
            "stream": False,
            "extra_body": {"thinking": {"type": "disabled"}},
        }
        return json.dumps({"custom_id": request_id, "method": "POST", "url": "/chat/completions", "body": body}, ensure_ascii=False)
    raise ValueError(f"Unsupported provider: {provider}")


# COMMAND ----------
request_records = []
for model_cfg in models:
    provider = model_cfg["provider"].lower()
    model = model_cfg["model"]
    tier = model_cfg.get("tier", "unspecified")
    for row in prompt_rows:
        request_id = stable_request_id(provider, model, row["channel_id"])
        batch_line = make_batch_line(provider, model, request_id, row["system_prompt"], row["prompt_user"], MAX_OUTPUT_TOKENS)
        request_records.append({
            "run_id": RUN_ID,
            "provider": provider,
            "model": model,
            "model_tier": tier,
            "channel_id": row["channel_id"],
            "request_id": request_id,
            "max_output_tokens": MAX_OUTPUT_TOKENS,
            "chunk_id": 0,
            "batch_line": batch_line,
        })

request_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("model_tier", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("max_output_tokens", IntegerType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("batch_line", StringType(), True),
])
requests_df = spark.createDataFrame(request_records, schema=request_schema)
write_run_scoped(requests_df, requests_full)
print("wrote requests", len(request_records), requests_full)

# COMMAND ----------
run_dir = os.path.join(BATCH_OUTPUT_DIR, RUN_ID)
os.makedirs(run_dir, exist_ok=True)

batch_file_records = []
for model_cfg in models:
    provider = model_cfg["provider"].lower()
    model = model_cfg["model"]
    tier = model_cfg.get("tier", "unspecified")
    provider_dir = os.path.join(run_dir, provider, safe_model_dir(model))
    os.makedirs(provider_dir, exist_ok=True)
    local_path = os.path.join(provider_dir, "chunk_00000.jsonl")
    rows_for_model = [r for r in request_records if r["provider"] == provider and r["model"] == model]
    n_lines = 0
    n_bytes = 0
    with open(local_path, "w", encoding="utf-8") as handle:
        for rec in rows_for_model:
            line = rec["batch_line"]
            handle.write(line + "\n")
            n_lines += 1
            n_bytes += len(line.encode("utf-8")) + 1
    batch_file_records.append((RUN_ID, provider, model, tier, 0, local_path, n_lines, n_bytes, datetime.utcnow().isoformat()))
    print("wrote", provider, model, n_lines, local_path)

batch_file_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("model_tier", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("local_jsonl_path", StringType(), True),
    StructField("n_requests", IntegerType(), True),
    StructField("n_bytes", IntegerType(), True),
    StructField("created_at_utc", StringType(), True),
])
batch_files_df = spark.createDataFrame(batch_file_records, batch_file_schema)
write_run_scoped(batch_files_df, batch_files_full)

# COMMAND ----------
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

job_records = []
for rec in batch_file_records:
    _, provider, model, _tier, chunk_id, local_path, _n_lines, _n_bytes, _created_at = rec
    try:
        if provider == "openai":
            result = submit_openai_batch(local_path, model)
        elif provider == "anthropic":
            result = submit_anthropic_batch(local_path, model)
        elif provider == "gemini":
            result = submit_gemini_batch(local_path, model)
        else:
            raise ValueError(f"Direct provider {provider} intentionally not submitted by this split batch notebook.")
        status = "submitted"
        error = None
    except Exception as exc:
        result = {"provider_file_id": None, "provider_batch_id": None, "provider_status": None}
        status = "error"
        error = repr(exc)[:2000]
    job_records.append((RUN_ID, provider, model, int(chunk_id), local_path, result.get("provider_file_id"), result.get("provider_batch_id"), result.get("provider_status"), status, error, datetime.utcnow().isoformat()))
    print(provider, model, status, result, error)

jobs_df = spark.createDataFrame(job_records, batch_job_schema)
write_run_scoped(jobs_df, batch_jobs_full)

result = {
    "run_id": RUN_ID,
    "prompt_rows": len(prompt_rows),
    "allowed_topic_label_count": len(allowed_slugs),
    "providers_submitted": sorted(SUBMIT_PROVIDER_FILTER),
    "requests_written": len(request_records),
    "batch_files_written": len(batch_file_records),
    "batch_jobs_table": batch_jobs_full,
    "request_table": requests_full,
    "batch_output_dir": run_dir,
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
