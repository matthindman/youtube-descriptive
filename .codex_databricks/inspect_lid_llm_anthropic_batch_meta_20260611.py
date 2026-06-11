# Databricks notebook source
# MAGIC %pip install anthropic

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
from typing import Any

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
        return default


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "too_full_20260609")
_create_text_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("anthropic_secret_key", "anthropic-api-key")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic-api-key")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def as_jsonable(value: Any):
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        return as_jsonable(value.model_dump())
    if hasattr(value, "to_json_dict"):
        return as_jsonable(value.to_json_dict())
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): as_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [as_jsonable(v) for v in value]
    return str(value)


jobs = (
    spark.table(fqtn(BATCH_JOBS_TABLE))
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("provider") == F.lit("anthropic"))
    .where(F.col("provider_batch_id").isNotNull())
    .select("run_id", "provider", "model", "chunk_id", "n_requests", "provider_batch_id", "submitted_at_utc")
    .orderBy("model", "chunk_id")
    .collect()
)

import anthropic

client = anthropic.Anthropic(api_key=dbutils.secrets.get(scope=SECRET_SCOPE, key=ANTHROPIC_SECRET_KEY))
rows = []
for row in jobs:
    try:
        batch = client.messages.batches.retrieve(row["provider_batch_id"])
        meta = as_jsonable(batch)
        result_counts = {}
        input_tokens = 0
        output_tokens = 0
        cache_read_input_tokens = 0
        cache_creation_input_tokens = 0
        n_with_usage = 0
        for item in client.messages.batches.results(row["provider_batch_id"]):
            item_obj = as_jsonable(item)
            result = item_obj.get("result") if isinstance(item_obj, dict) else None
            result_type = result.get("type") if isinstance(result, dict) else "unknown"
            result_counts[result_type] = result_counts.get(result_type, 0) + 1
            message = result.get("message") if isinstance(result, dict) and isinstance(result.get("message"), dict) else {}
            usage = message.get("usage") if isinstance(message.get("usage"), dict) else {}
            if usage:
                n_with_usage += 1
                input_tokens += int(usage.get("input_tokens") or 0)
                output_tokens += int(usage.get("output_tokens") or 0)
                cache_read_input_tokens += int(usage.get("cache_read_input_tokens") or 0)
                cache_creation_input_tokens += int(usage.get("cache_creation_input_tokens") or 0)
        rows.append({
            "run_id": row["run_id"],
            "provider": row["provider"],
            "model": row["model"],
            "chunk_id": row["chunk_id"],
            "n_requests": row["n_requests"],
            "provider_batch_id": row["provider_batch_id"],
            "submitted_at_utc": row["submitted_at_utc"],
            "metadata": meta,
            "result_counts": result_counts,
            "n_with_usage": n_with_usage,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_read_input_tokens": cache_read_input_tokens,
            "cache_creation_input_tokens": cache_creation_input_tokens,
            "error": None,
        })
    except Exception as exc:
        rows.append({
            "run_id": row["run_id"],
            "provider": row["provider"],
            "model": row["model"],
            "chunk_id": row["chunk_id"],
            "n_requests": row["n_requests"],
            "provider_batch_id": row["provider_batch_id"],
            "submitted_at_utc": row["submitted_at_utc"],
            "metadata": None,
            "result_counts": None,
            "n_with_usage": None,
            "input_tokens": None,
            "output_tokens": None,
            "cache_read_input_tokens": None,
            "cache_creation_input_tokens": None,
            "error": repr(exc)[:1000],
        })

print(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str))
