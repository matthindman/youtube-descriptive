# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube Topic Verification: Random 1,000 Channel LLM Run
# MAGIC
# MAGIC **Deprecated for `dev_sean.default.channel_category.topic_categories` validation.** This notebook asks
# MAGIC for exactly one topic label and is retained only for comparison with prior runs. Use
# MAGIC `run_category_topic_multilabel_1000_20260612.py` for the current exact observed YouTube
# MAGIC `topic_categories` multi-label target.
# MAGIC
# MAGIC Randomly samples channels from `prod_tads.youtube_too.yt_sl_channels`, joins held-out topic labels from
# MAGIC `dev_sean.default.channel_category`, prompts LLMs without per-channel labels, and submits provider jobs.

# COMMAND ----------
# MAGIC %pip install "openai>=2.0.0" anthropic "google-genai>=1.51.0" requests

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _create_text_widget(name: str, default: str, label: Optional[str] = None) -> None:
    try:
        dbutils.widgets.text(name, default, label or name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None and value != "" else default
    except Exception:
        return os.environ.get(name.upper(), default)


def _get_bool_widget(name: str, default: bool) -> bool:
    raw = _get_widget(name, str(default)).strip().lower()
    return raw in {"1", "true", "t", "yes", "y"}


def _get_int_widget(name: str, default: int) -> int:
    raw = _get_widget(name, str(default)).strip()
    return int(raw) if raw else default


def _get_float_widget(name: str, default: float) -> float:
    raw = _get_widget(name, str(default)).strip()
    return float(raw) if raw else default


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

_create_text_widget("run_id", "category_topic_random_1000_20260611")
_create_text_widget("channels_table", "prod_tads.youtube_too.yt_sl_channels")
_create_text_widget("videos_table", "prod_tads.youtube_too.yt_sl_videos")
_create_text_widget("category_table", "dev_sean.default.channel_category")
_create_text_widget("language_table", "dev_sean.matt.yt_lid_v3_channels")
_create_text_widget("output_catalog", "dev_sean")
_create_text_widget("output_schema", "matt")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")
_create_text_widget("sample_size", "1000")
_create_text_widget("random_seed", "20260611")
_create_text_widget("videos_per_channel", "10")
_create_text_widget("prompt_max_video_description_chars", "300")
_create_text_widget("prompt_max_chars", "9000")
_create_text_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_batches")
_create_text_widget("max_requests_per_file", "10000")
_create_text_widget("max_output_tokens", "700")
_create_text_widget("models_json", DEFAULT_MODELS_JSON)
_create_text_widget("submit_batches", "true")
_create_text_widget("submit_provider_filter", "anthropic,gemini,openai,deepseek")
_create_text_widget("skip_existing_submitted_batches", "true")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("openai_secret_key", "openai-api-key")
_create_text_widget("anthropic_secret_key", "anthropic-api-key")
_create_text_widget("gemini_secret_key", "gemini-api-key")
_create_text_widget("deepseek_secret_key", "deepseek-api-key")
_create_text_widget("openai_reasoning_effort", "minimal")
_create_text_widget("gemini_thinking_level", "low")
_create_text_widget("deepseek_thinking_type", "disabled")
_create_text_widget("deepseek_max_output_tokens", "700")
_create_text_widget("deepseek_max_workers", "16")
_create_text_widget("deepseek_request_timeout_seconds", "60")
_create_text_widget("deepseek_max_retries", "1")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_batches/results")

RUN_ID = _get_widget("run_id", "category_topic_random_1000_20260611").strip()
CHANNELS_TABLE = _get_widget("channels_table", "prod_tads.youtube_too.yt_sl_channels").strip()
VIDEOS_TABLE = _get_widget("videos_table", "prod_tads.youtube_too.yt_sl_videos").strip()
CATEGORY_TABLE = _get_widget("category_table", "dev_sean.default.channel_category").strip()
LANGUAGE_TABLE = _get_widget("language_table", "dev_sean.matt.yt_lid_v3_channels").strip()
OUTPUT_CATALOG = _get_widget("output_catalog", "dev_sean").strip()
OUTPUT_SCHEMA = _get_widget("output_schema", "matt").strip()
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000").strip()
SAMPLE_SIZE = _get_int_widget("sample_size", 1000)
RANDOM_SEED = _get_int_widget("random_seed", 20260611)
VIDEOS_PER_CHANNEL = _get_int_widget("videos_per_channel", 10)
PROMPT_MAX_VIDEO_DESCRIPTION_CHARS = _get_int_widget("prompt_max_video_description_chars", 300)
PROMPT_MAX_CHARS = _get_int_widget("prompt_max_chars", 9000)
BATCH_OUTPUT_DIR = _get_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_batches").rstrip("/")
MAX_REQUESTS_PER_FILE = _get_int_widget("max_requests_per_file", 10000)
MAX_OUTPUT_TOKENS = _get_int_widget("max_output_tokens", 700)
MODELS = json.loads(_get_widget("models_json", DEFAULT_MODELS_JSON))
SUBMIT_BATCHES = _get_bool_widget("submit_batches", True)
SUBMIT_PROVIDER_FILTER = {p.strip().lower() for p in _get_widget("submit_provider_filter", "anthropic,gemini,openai,deepseek").split(",") if p.strip()}
SKIP_EXISTING_SUBMITTED_BATCHES = _get_bool_widget("skip_existing_submitted_batches", True)
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai-api-key")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic-api-key")
GEMINI_SECRET_KEY = _get_widget("gemini_secret_key", "gemini-api-key")
DEEPSEEK_SECRET_KEY = _get_widget("deepseek_secret_key", "deepseek-api-key")
OPENAI_REASONING_EFFORT = _get_widget("openai_reasoning_effort", "minimal").strip().lower()
GEMINI_THINKING_LEVEL = _get_widget("gemini_thinking_level", "low").strip()
DEEPSEEK_THINKING_TYPE = _get_widget("deepseek_thinking_type", "disabled").strip().lower()
DEEPSEEK_MAX_OUTPUT_TOKENS = _get_int_widget("deepseek_max_output_tokens", 700)
DEEPSEEK_MAX_WORKERS = _get_int_widget("deepseek_max_workers", 16)
DEEPSEEK_REQUEST_TIMEOUT_SECONDS = _get_float_widget("deepseek_request_timeout_seconds", 60.0)
DEEPSEEK_MAX_RETRIES = _get_int_widget("deepseek_max_retries", 1)
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_batches/results").rstrip("/")

spark.conf.set("spark.databricks.remoteFiltering.blockSelfJoins", "false")


def table_ref(name: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in name.split("."))


def out_table(suffix: str) -> str:
    return table_ref(f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{OUTPUT_PREFIX}_{suffix}")


def safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", model)


def stable_hash_order_col(*cols: str):
    exprs = [F.lit(str(RANDOM_SEED))]
    for c in cols:
        exprs.append(F.coalesce(F.col(c).cast("string"), F.lit("")))
    return F.sha2(F.concat_ws("||", *exprs), 256)


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


def topic_slug_expr(col):
    return F.when(col.rlike("/wiki/"), F.regexp_extract(col, r"/wiki/(.*)$", 1)).otherwise(col)


def topic_name_expr(slug_col):
    return F.regexp_replace(slug_col, "_", " ")


prompt_inputs_full = out_table("prompt_inputs")
requests_full = out_table("requests")
batch_files_full = out_table("batch_files")
batch_jobs_full = out_table("batch_jobs")

print("RUN_ID:", RUN_ID)
print("outputs:", prompt_inputs_full, requests_full, batch_files_full, batch_jobs_full)

# COMMAND ----------
channels = (
    spark.table(table_ref(CHANNELS_TABLE))
    .select(
        F.col("channel_id").cast("string").alias("channel_id"),
        F.col("channel_name").cast("string").alias("channel_name"),
        F.col("language_code").cast("string").alias("source_language_code"),
    )
    .where(F.col("channel_id").isNotNull())
    .dropDuplicates(["channel_id"])
)

category_raw = (
    spark.table(table_ref(CATEGORY_TABLE))
    .select(
        F.col("canonical_id").cast("string").alias("channel_id"),
        F.col("topic_categories"),
        F.col("collected_at"),
    )
    .where(F.col("canonical_id").isNotNull())
)

latest_category = (
    category_raw.join(F.broadcast(channels.select("channel_id")), on="channel_id", how="inner")
    .withColumn("_rn", F.row_number().over(Window.partitionBy("channel_id").orderBy(F.desc("collected_at"))))
    .where(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("topic_category_count", F.size("topic_categories"))
    .withColumn("primary_topic_url", F.element_at("topic_categories", 1).cast("string"))
    .withColumn("primary_topic_slug", topic_slug_expr(F.col("primary_topic_url")))
    .withColumn("primary_topic_name", topic_name_expr(F.col("primary_topic_slug")))
    .withColumn(
        "topic_slugs",
        F.expr("transform(topic_categories, x -> case when x rlike '/wiki/' then regexp_extract(x, '/wiki/(.*)$', 1) else x end)"),
    )
)

sample_ids = (
    channels
    .orderBy(stable_hash_order_col("channel_id"), F.col("channel_id"))
    .limit(SAMPLE_SIZE)
    .select("channel_id")
)

sampled = (
    sample_ids
    .join(channels, on="channel_id", how="inner")
    .join(
        latest_category.select("channel_id", "topic_categories", "topic_slugs", "primary_topic_url", "primary_topic_slug", "primary_topic_name", "topic_category_count", "collected_at"),
        on="channel_id",
        how="left",
    )
)

try:
    lang = spark.table(table_ref(LANGUAGE_TABLE))
    lang_cols = ["channel_id"]
    for c in ["primary_language_label", "primary_language_iso639_3", "primary_language_confidence", "language_status"]:
        if c in lang.columns:
            lang_cols.append(c)
    sampled = sampled.join(lang.select(*lang_cols).dropDuplicates(["channel_id"]), on="channel_id", how="left")
except Exception as exc:
    print(f"Language join skipped: {exc}")
    sampled = sampled.withColumn("primary_language_label", F.lit(None).cast("string")).withColumn("primary_language_iso639_3", F.lit(None).cast("string"))

sampled = (
    sampled
    .withColumn("primary_language_label", F.coalesce(F.col("primary_language_label"), F.col("source_language_code"), F.lit("unknown")))
    .withColumn("primary_language_iso639_3", F.coalesce(F.col("primary_language_iso639_3"), F.lit("unknown")))
)

allowed_categories_df = (
    sampled
    .select(F.explode_outer("topic_slugs").alias("topic_slug"))
    .where(F.col("topic_slug").isNotNull() & (F.length(F.trim("topic_slug")) > 0))
    .dropDuplicates(["topic_slug"])
    .withColumn("topic_name", topic_name_expr(F.col("topic_slug")))
    .orderBy("topic_slug")
)
allowed_categories = [
    row.asDict(recursive=True)
    for row in allowed_categories_df.collect()
]
allowed_slugs = [row["topic_slug"] for row in allowed_categories]
if not allowed_slugs:
    raise RuntimeError("No held-out topic labels found in the random sample.")

allowed_text = "\n".join([f"- {row['topic_slug']}: {row['topic_name']}" for row in allowed_categories])
print(f"Allowed topic labels in sample topic arrays: {len(allowed_slugs):,}")

TOPIC_RESPONSE_JSON_SCHEMA = {
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

SYSTEM_PROMPT = f"""You are classifying YouTube channels for an academic study. Choose exactly one topic/genre category_id from the allowed label list below. Use only the channel name, detected language, and recent video titles/descriptions. The held-out category labels for each channel are not shown to you.

Allowed category_id values:
{allowed_text}

Return one compact minified JSON object on one line, with no markdown and no text outside JSON:
{{"category_id":"one allowed category_id exactly","category_name":"human readable category name","confidence":0.0,"ambiguous":false,"rationale_short":"brief evidence, max 160 chars"}}
""".strip()

# COMMAND ----------
videos = spark.table(table_ref(VIDEOS_TABLE))
video_title_col = "video_title" if "video_title" in videos.columns else "title"
video_desc_col = "description" if "description" in videos.columns else ("video_description" if "video_description" in videos.columns else None)
rank_col = "published_at" if "published_at" in videos.columns else ("ingestion_timestamp" if "ingestion_timestamp" in videos.columns else None)

video_base = (
    videos.join(F.broadcast(sample_ids), on="channel_id", how="inner")
    .select(
        F.col("channel_id").cast("string").alias("channel_id"),
        F.col("video_id").cast("string").alias("video_id") if "video_id" in videos.columns else F.lit(None).cast("string").alias("video_id"),
        F.col(video_title_col).cast("string").alias("video_title"),
        F.col(video_desc_col).cast("string").alias("video_description") if video_desc_col else F.lit("").alias("video_description"),
        F.col(rank_col).alias("_video_rank_value") if rank_col else F.lit(None).alias("_video_rank_value"),
    )
)

if rank_col:
    vw = Window.partitionBy("channel_id").orderBy(F.col("_video_rank_value").desc_nulls_last(), F.col("video_id").asc_nulls_last())
else:
    vw = Window.partitionBy("channel_id").orderBy(F.xxhash64(F.col("video_id")).asc())

video_lines = (
    video_base
    .withColumn("_video_rank", F.row_number().over(vw))
    .where(F.col("_video_rank") <= VIDEOS_PER_CHANNEL)
    .withColumn("video_title_clean", F.substring(F.regexp_replace(F.coalesce(F.col("video_title"), F.lit("")), r"[\r\n\t]+", " "), 1, 320))
    .withColumn("video_desc_clean", F.substring(F.regexp_replace(F.coalesce(F.col("video_description"), F.lit("")), r"[\r\n\t]+", " "), 1, PROMPT_MAX_VIDEO_DESCRIPTION_CHARS))
    .withColumn(
        "video_line",
        F.concat(
            F.lit("["),
            F.col("_video_rank").cast("string"),
            F.lit("] Title: "),
            F.col("video_title_clean"),
            F.when(F.length(F.col("video_desc_clean")) > 0, F.concat(F.lit(" | Description: "), F.col("video_desc_clean"))).otherwise(F.lit("")),
        ),
    )
    .groupBy("channel_id")
    .agg(
        F.array_sort(F.collect_list(F.struct(F.col("_video_rank").alias("rank"), F.col("video_line").alias("line")))).alias("video_lines_struct"),
        F.count("*").alias("n_videos_in_prompt"),
    )
    .withColumn("recent_videos_text", F.expr("array_join(transform(video_lines_struct, x -> x.line), '\\n')"))
    .drop("video_lines_struct")
)

prompt_inputs = (
    sampled.join(video_lines, on="channel_id", how="left")
    .withColumn("n_videos_in_prompt", F.coalesce(F.col("n_videos_in_prompt"), F.lit(0)))
    .withColumn("recent_videos_text", F.coalesce(F.col("recent_videos_text"), F.lit("")))
    .withColumn(
        "prompt_user",
        F.substring(
            F.concat(
                F.lit("Classify this YouTube channel into exactly one allowed topic/genre category_id. Return strict JSON only.\n\n"),
                F.lit("Channel name: "), F.coalesce(F.col("channel_name"), F.lit("")), F.lit("\n"),
                F.lit("Detected language: "), F.coalesce(F.col("primary_language_label"), F.lit("unknown")),
                F.lit(" ("), F.coalesce(F.col("primary_language_iso639_3"), F.lit("unknown")), F.lit(")\n\n"),
                F.lit("Recent videos:\n"), F.coalesce(F.col("recent_videos_text"), F.lit("")), F.lit("\n"),
            ),
            1,
            PROMPT_MAX_CHARS,
        ),
    )
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("sample_mode", F.lit("random_full_youtube_too"))
    .withColumn("prompt_version", F.lit("topic_category_v2_array_allowlist"))
    .withColumn("system_prompt", F.lit(SYSTEM_PROMPT))
    .withColumn("created_at", F.current_timestamp())
)

write_run_scoped(prompt_inputs, prompt_inputs_full)
summary = prompt_inputs.agg(
    F.count("*").alias("n_channels"),
    F.sum(F.when(F.col("primary_topic_slug").isNotNull(), 1).otherwise(0)).alias("n_with_primary_reference"),
    F.sum(F.when(F.col("topic_category_count") > 0, 1).otherwise(0)).alias("n_with_any_reference"),
    F.avg(F.col("n_videos_in_prompt")).alias("mean_videos_in_prompt"),
).collect()[0].asDict(recursive=True)
print("prompt_input_summary:", json.dumps(summary, sort_keys=True, default=str))

# COMMAND ----------
models_df = spark.createDataFrame(
    [(m["provider"].lower(), m["model"], m.get("tier", "unspecified")) for m in MODELS],
    ["provider", "model", "model_tier"],
).where(F.col("provider").isin(*sorted(SUBMIT_PROVIDER_FILTER)))


def _is_openai_reasoning_or_gpt5_model(model: Optional[str]) -> bool:
    m = (model or "").lower()
    return m.startswith("gpt-5") or m.startswith("o1") or m.startswith("o3") or m.startswith("o4") or m.startswith("o-")


def _openai_batch_endpoint_for_model(model: Optional[str]) -> str:
    return "/v1/responses" if _is_openai_reasoning_or_gpt5_model(model) else "/v1/chat/completions"


@F.udf(StringType())
def make_batch_line(provider: str, model: str, request_id: str, system_prompt: str, user_prompt: str, max_output_tokens: int) -> str:
    provider = (provider or "").lower()
    max_out = int(max_output_tokens or MAX_OUTPUT_TOKENS)
    if provider == "openai":
        if _is_openai_reasoning_or_gpt5_model(model):
            body = {
                "model": model,
                "instructions": system_prompt,
                "input": user_prompt,
                "max_output_tokens": max_out,
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "youtube_topic_category_prediction",
                        "schema": TOPIC_RESPONSE_JSON_SCHEMA,
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
            "max_tokens": max_out,
        }
        return json.dumps({"custom_id": request_id, "method": "POST", "url": "/v1/chat/completions", "body": body}, ensure_ascii=False)
    if provider == "anthropic":
        return json.dumps({"custom_id": request_id, "params": {"model": model, "max_tokens": max_out, "system": system_prompt, "messages": [{"role": "user", "content": user_prompt}]}}, ensure_ascii=False)
    if provider == "gemini":
        generation_config = {"max_output_tokens": max_out, "response_mime_type": "application/json"}
        if GEMINI_THINKING_LEVEL:
            generation_config["thinking_config"] = {"thinking_level": GEMINI_THINKING_LEVEL}
        return json.dumps({"key": request_id, "request": {"system_instruction": {"parts": [{"text": system_prompt}]}, "contents": [{"role": "user", "parts": [{"text": user_prompt}]}], "generation_config": generation_config}}, ensure_ascii=False)
    if provider == "deepseek":
        body = {
            "model": model,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            "max_tokens": int(DEEPSEEK_MAX_OUTPUT_TOKENS or max_out),
            "stream": False,
        }
        if DEEPSEEK_THINKING_TYPE:
            body["extra_body"] = {"thinking": {"type": DEEPSEEK_THINKING_TYPE}}
        return json.dumps({"custom_id": request_id, "method": "POST", "url": "/chat/completions", "body": body}, ensure_ascii=False)
    raise ValueError(f"Unsupported provider: {provider}")


requests = (
    prompt_inputs.crossJoin(models_df)
    .withColumn("request_id", F.concat(F.lit("ytc_"), F.substring(F.sha2(F.concat_ws("||", F.col("run_id"), F.col("provider"), F.col("model"), F.col("channel_id")), 256), 1, 60)))
    .withColumn("max_output_tokens", F.lit(MAX_OUTPUT_TOKENS))
    .withColumn("batch_line", make_batch_line(F.col("provider"), F.col("model"), F.col("request_id"), F.col("system_prompt"), F.col("prompt_user"), F.col("max_output_tokens")))
)
rw = Window.partitionBy("provider", "model").orderBy("request_id")
requests = (
    requests
    .withColumn("_request_n", F.row_number().over(rw))
    .withColumn("chunk_id", F.floor((F.col("_request_n") - F.lit(1)) / F.lit(MAX_REQUESTS_PER_FILE)).cast("int"))
    .drop("_request_n")
)

write_run_scoped(requests, requests_full)
display(requests.groupBy("provider", "model", "chunk_id").count().orderBy("provider", "model", "chunk_id"))

# COMMAND ----------
os.makedirs(BATCH_OUTPUT_DIR, exist_ok=True)
run_dir = os.path.join(BATCH_OUTPUT_DIR, RUN_ID)
os.makedirs(run_dir, exist_ok=True)

request_groups = (
    spark.table(requests_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .select("provider", "model", "model_tier", "chunk_id")
    .distinct()
    .orderBy("provider", "model", "chunk_id")
    .collect()
)

batch_file_records = []
for g in request_groups:
    provider = g["provider"]
    model = g["model"]
    chunk_id = int(g["chunk_id"])
    provider_dir = os.path.join(run_dir, provider, safe_model_dir(model))
    os.makedirs(provider_dir, exist_ok=True)
    local_path = os.path.join(provider_dir, f"chunk_{chunk_id:05d}.jsonl")
    subset = (
        spark.table(requests_full)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .where((F.col("provider") == provider) & (F.col("model") == model) & (F.col("chunk_id") == chunk_id))
        .select("batch_line")
    )
    n_lines = 0
    n_bytes = 0
    with open(local_path, "w", encoding="utf-8") as f:
        for row in subset.toLocalIterator():
            line = row["batch_line"]
            f.write(line + "\n")
            n_lines += 1
            n_bytes += len(line.encode("utf-8")) + 1
    batch_file_records.append((RUN_ID, provider, model, g["model_tier"], chunk_id, local_path, n_lines, n_bytes, datetime.utcnow().isoformat()))
    print(f"Wrote {n_lines:,} requests: {local_path}")

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
    with open(local_jsonl_path, "rb") as f:
        uploaded = client.files.create(file=f, purpose="batch")
    batch = client.batches.create(
        input_file_id=uploaded.id,
        endpoint=_openai_batch_endpoint_for_model(model),
        completion_window="24h",
        metadata={"run_id": RUN_ID, "task": "youtube_topic_category", "model": model},
    )
    return {"provider_file_id": uploaded.id, "provider_batch_id": batch.id, "provider_status": getattr(batch, "status", None)}


def submit_anthropic_batch(local_jsonl_path: str, model: str) -> Dict[str, Any]:
    import anthropic
    client = anthropic.Anthropic(api_key=get_secret(SECRET_SCOPE, ANTHROPIC_SECRET_KEY))
    requests_payload = []
    with open(local_jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
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


def submit_deepseek_direct(local_jsonl_path: str, model: str) -> Dict[str, Any]:
    import requests
    api_key = get_secret(SECRET_SCOPE, DEEPSEEK_SECRET_KEY)
    thread_state = threading.local()
    result_dir = os.path.join(RESULTS_INPUT_DIR, RUN_ID, "deepseek", safe_model_dir(model))
    os.makedirs(result_dir, exist_ok=True)
    result_path = os.path.join(result_dir, os.path.basename(local_jsonl_path).replace(".jsonl", "_results.jsonl"))

    def _session():
        session = getattr(thread_state, "session", None)
        if session is None:
            session = requests.Session()
            thread_state.session = session
        return session

    def _call_line(line: str):
        req = json.loads(line)
        body = dict(req["body"])
        extra_body = body.pop("extra_body", None)
        if isinstance(extra_body, dict):
            body.update(extra_body)
        custom_id = req.get("custom_id")
        last_error = None
        for attempt in range(DEEPSEEK_MAX_RETRIES + 1):
            try:
                response = _session().post(
                    "https://api.deepseek.com/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json=body,
                    timeout=DEEPSEEK_REQUEST_TIMEOUT_SECONDS,
                )
                try:
                    response_body = response.json()
                except Exception:
                    response_body = {"text": response.text[:4000]}
                out = {"custom_id": custom_id, "response": {"status_code": response.status_code, "body": response_body}}
                if 200 <= response.status_code < 300:
                    return out, True
                last_error = response.text[:2000]
                if response.status_code not in {408, 409, 429, 500, 502, 503, 504}:
                    out["error"] = last_error
                    return out, False
            except Exception as exc:
                last_error = repr(exc)[:2000]
            time.sleep(min(2 ** attempt, 8))
        return {"custom_id": custom_id, "response": {"status_code": 500, "error": last_error}, "error": last_error}, False

    with open(local_jsonl_path, "r", encoding="utf-8") as src:
        lines = [line for line in src if line.strip()]
    n_ok = 0
    n_error = 0
    with open(result_path, "w", encoding="utf-8") as dst:
        with ThreadPoolExecutor(max_workers=DEEPSEEK_MAX_WORKERS) as pool:
            futures = [pool.submit(_call_line, line) for line in lines]
            for i, fut in enumerate(as_completed(futures), start=1):
                out, ok = fut.result()
                n_ok += int(ok)
                n_error += int(not ok)
                dst.write(json.dumps(out, ensure_ascii=False) + "\n")
                if i % 100 == 0 or i == len(lines):
                    dst.flush()
                    print(f"DeepSeek {model}: {i:,}/{len(lines):,}; ok={n_ok:,}; error={n_error:,}")
    status = "completed" if n_error == 0 else "completed_with_errors"
    return {
        "provider_file_id": result_path,
        "provider_batch_id": f"deepseek-direct:{RUN_ID}:{safe_model_dir(model)}:{os.path.basename(local_jsonl_path)}",
        "provider_status": f"{status}; ok={n_ok}; error={n_error}",
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

if SUBMIT_BATCHES:
    already_submitted = set()
    if SKIP_EXISTING_SUBMITTED_BATCHES and _table_exists_full(batch_jobs_full):
        already_rows = (
            spark.table(batch_jobs_full)
            .where((F.col("run_id") == F.lit(RUN_ID)) & (F.col("submission_status") == F.lit("submitted")) & F.col("provider_batch_id").isNotNull())
            .select("provider", "model", "chunk_id")
            .collect()
        )
        already_submitted = {(r["provider"], r["model"], int(r["chunk_id"])) for r in already_rows}
    job_records = []
    for row in spark.table(batch_files_full).where(F.col("run_id") == F.lit(RUN_ID)).orderBy("provider", "model", "chunk_id").collect():
        provider = row["provider"]
        model = row["model"]
        chunk_id = int(row["chunk_id"])
        path = row["local_jsonl_path"]
        if provider not in SUBMIT_PROVIDER_FILTER:
            continue
        if (provider, model, chunk_id) in already_submitted:
            print(provider, model, chunk_id, "already submitted; skipping")
            continue
        try:
            if provider == "openai":
                result = submit_openai_batch(path, model)
            elif provider == "anthropic":
                result = submit_anthropic_batch(path, model)
            elif provider == "gemini":
                result = submit_gemini_batch(path, model)
            elif provider == "deepseek":
                result = submit_deepseek_direct(path, model)
            else:
                raise ValueError(f"Unsupported provider {provider}")
            status = "submitted"
            error = None
        except Exception as exc:
            result = {"provider_file_id": None, "provider_batch_id": None, "provider_status": None}
            status = "error"
            error = repr(exc)[:2000]
        job_records.append((RUN_ID, provider, model, chunk_id, path, result.get("provider_file_id"), result.get("provider_batch_id"), result.get("provider_status"), status, error, datetime.utcnow().isoformat()))
        print(provider, model, chunk_id, status, result, error)
    jobs_df = spark.createDataFrame(job_records, batch_job_schema) if job_records else spark.createDataFrame([], batch_job_schema)
    if _table_exists_full(batch_jobs_full):
        current = spark.table(batch_jobs_full).where(F.col("run_id") == F.lit(RUN_ID))
        replace_keys = jobs_df.select("provider", "model", "chunk_id").distinct()
        jobs_to_write = current.join(replace_keys, on=["provider", "model", "chunk_id"], how="left_anti").unionByName(jobs_df, allowMissingColumns=True)
    else:
        jobs_to_write = jobs_df
    write_run_scoped(jobs_to_write, batch_jobs_full)
    display(jobs_df.orderBy("provider", "model", "chunk_id"))
else:
    print("submit_batches=false; generated prompt/request/batch files only.")

result = {
    "run_id": RUN_ID,
    "sample_size": SAMPLE_SIZE,
    "allowed_topic_label_count": len(allowed_slugs),
    "prompt_inputs_table": prompt_inputs_full,
    "requests_table": requests_full,
    "batch_files_table": batch_files_full,
    "batch_jobs_table": batch_jobs_full,
    "batch_output_dir": run_dir,
}
print(json.dumps(result, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
