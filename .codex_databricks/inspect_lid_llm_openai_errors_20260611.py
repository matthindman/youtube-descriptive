# Databricks notebook source
import json
import os
from collections import Counter, defaultdict

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
_create_text_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
_create_text_widget("status_snapshot_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("openai_secret_key", "openai-api-key")
_create_text_widget("max_lines_per_model", "5")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
STATUS_SNAPSHOT_TABLE = _get_widget("status_snapshot_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai-api-key")
MAX_LINES_PER_MODEL = int(_get_widget("max_lines_per_model", "5"))


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def openai_file_content_text(file_content) -> str:
    text = getattr(file_content, "text", None)
    if isinstance(text, str):
        return text
    if isinstance(file_content, str):
        return file_content
    if isinstance(file_content, bytes):
        return file_content.decode("utf-8")
    if hasattr(file_content, "read"):
        data = file_content.read()
        return data.decode("utf-8") if isinstance(data, bytes) else str(data)
    try:
        return bytes(file_content).decode("utf-8")
    except Exception:
        return str(file_content)


from openai import OpenAI

client = OpenAI(api_key=dbutils.secrets.get(scope=SECRET_SCOPE, key=OPENAI_SECRET_KEY))

rows = (
    spark.table(fqtn(STATUS_SNAPSHOT_TABLE))
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("provider") == F.lit("openai"))
    .select("model", "chunk_id", "provider_batch_id", "provider_status", "request_counts_json", "output_ref_json")
    .orderBy("model", "chunk_id")
    .collect()
)

summary = []
for row in rows:
    output_ref = {}
    request_counts = {}
    try:
        output_ref = json.loads(row["output_ref_json"] or "{}")
    except Exception:
        output_ref = {}
    try:
        request_counts = json.loads(row["request_counts_json"] or "{}")
    except Exception:
        request_counts = {}

    error_file_id = output_ref.get("error_file_id")
    if not error_file_id:
        try:
            batch = client.batches.retrieve(row["provider_batch_id"])
            error_file_id = getattr(batch, "error_file_id", None)
        except Exception:
            error_file_id = None

    error_counts = Counter()
    examples = []
    n_error_lines = 0
    if error_file_id:
        text = openai_file_content_text(client.files.content(error_file_id))
        for line in text.splitlines():
            if not line.strip():
                continue
            n_error_lines += 1
            try:
                obj = json.loads(line)
            except Exception:
                error_counts["<json_parse_error>"] += 1
                if len(examples) < MAX_LINES_PER_MODEL:
                    examples.append({"raw": line[:1000]})
                continue
            err = obj.get("error") or obj.get("response", {}).get("body", {}).get("error") or {}
            code = err.get("code") or err.get("type") or "<no_code>"
            message = err.get("message") or json.dumps(err, ensure_ascii=False)[:500]
            key = f"{code}: {message[:220]}"
            error_counts[key] += 1
            if len(examples) < MAX_LINES_PER_MODEL:
                examples.append({
                    "custom_id": obj.get("custom_id"),
                    "code": code,
                    "message": message[:1000],
                })

    summary.append({
        "model": row["model"],
        "chunk_id": int(row["chunk_id"]),
        "provider_batch_id": row["provider_batch_id"],
        "provider_status": row["provider_status"],
        "request_counts": request_counts.get("request_counts", request_counts),
        "error_file_id": error_file_id,
        "n_error_lines": n_error_lines,
        "top_errors": [{"error": key, "n": n} for key, n in error_counts.most_common(5)],
        "examples": examples,
    })

print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False, sort_keys=True))
