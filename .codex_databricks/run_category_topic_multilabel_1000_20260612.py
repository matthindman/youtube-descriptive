# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube Topic Categories: Multi-label 1,000 Channel LLM Validation
# MAGIC
# MAGIC This notebook samples channels from `prod_tads.youtube_too.yt_sl_channels`, joins held-out
# MAGIC YouTube API `topic_categories` from `dev_sean.default.channel_category`, and asks LLMs to predict
# MAGIC the full multi-label topic set from channel/video evidence only.
# MAGIC
# MAGIC The target is the exact observed YouTube topic-category array. The per-channel category array is
# MAGIC never included in model prompts.

# COMMAND ----------
# Provider SDKs are imported lazily inside the submission helpers below. Do not
# run `%pip` here: the prompt/sample tables should be materialized before any
# optional provider-client setup can slow or fail the notebook.

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
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


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


# Defaults reflect the 2026-06-12 validation run: include only models that
# completed reliably and produced useful heldout metrics. OpenAI aliases and
# Gemini Pro preview are opt-in until their provider errors/latency are resolved.
DEFAULT_MODELS_JSON = json.dumps([
    {"provider": "anthropic", "model": "claude-opus-4-8", "tier": "frontier"},
    {"provider": "anthropic", "model": "claude-sonnet-4-6", "tier": "mid"},
    {"provider": "anthropic", "model": "claude-haiku-4-5", "tier": "small"},
    {"provider": "gemini", "model": "gemini-3.5-flash", "tier": "mid"},
    {"provider": "gemini", "model": "gemini-3.1-flash-lite", "tier": "small"},
    {"provider": "deepseek", "model": "deepseek-v4-pro", "tier": "frontier"},
    {"provider": "deepseek", "model": "deepseek-v4-flash", "tier": "small"},
], ensure_ascii=False)


_create_text_widget("run_id", "category_topic_multilabel_random_1000_20260612")
_create_text_widget("channels_table", "prod_tads.youtube_too.yt_sl_channels")
_create_text_widget("videos_table", "prod_tads.youtube_too.yt_sl_videos")
_create_text_widget("category_table", "dev_sean.default.channel_category")
_create_text_widget("language_table", "dev_sean.matt.yt_lid_v3_channels")
_create_text_widget("output_catalog", "dev_sean")
_create_text_widget("output_schema", "matt")
_create_text_widget("output_prefix", "yt_category_topic_multilabel_1000")
_create_text_widget("sample_size", "1000")
_create_text_widget("random_seed", "20260612")
_create_text_widget("calibration_pct", "40")
_create_text_widget("videos_per_channel", "10")
_create_text_widget("prompt_max_channel_description_chars", "900")
_create_text_widget("prompt_max_video_description_chars", "260")
_create_text_widget("prompt_max_chars", "11000")
_create_text_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches")
_create_text_widget("max_requests_per_file", "10000")
_create_text_widget("max_output_tokens", "4500")
_create_text_widget("models_json", DEFAULT_MODELS_JSON)
_create_text_widget("submit_batches", "false")
_create_text_widget("submit_provider_filter", "anthropic,gemini,deepseek")
_create_text_widget("skip_existing_submitted_batches", "true")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("openai_secret_key", "openai-api-key")
_create_text_widget("anthropic_secret_key", "anthropic-api-key")
_create_text_widget("gemini_secret_key", "gemini-api-key")
_create_text_widget("deepseek_secret_key", "deepseek-api-key")
_create_text_widget("openai_reasoning_effort", "minimal")
_create_text_widget("gemini_thinking_level", "low")
_create_text_widget("deepseek_thinking_type", "disabled")
_create_text_widget("deepseek_max_output_tokens", "4500")
_create_text_widget("deepseek_max_workers", "12")
_create_text_widget("deepseek_request_timeout_seconds", "90")
_create_text_widget("deepseek_max_retries", "1")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches/results")

RUN_ID = _get_widget("run_id", "category_topic_multilabel_random_1000_20260612").strip()
CHANNELS_TABLE = _get_widget("channels_table", "prod_tads.youtube_too.yt_sl_channels").strip()
VIDEOS_TABLE = _get_widget("videos_table", "prod_tads.youtube_too.yt_sl_videos").strip()
CATEGORY_TABLE = _get_widget("category_table", "dev_sean.default.channel_category").strip()
LANGUAGE_TABLE = _get_widget("language_table", "dev_sean.matt.yt_lid_v3_channels").strip()
OUTPUT_CATALOG = _get_widget("output_catalog", "dev_sean").strip()
OUTPUT_SCHEMA = _get_widget("output_schema", "matt").strip()
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_multilabel_1000").strip()
SAMPLE_SIZE = _get_int_widget("sample_size", 1000)
RANDOM_SEED = _get_int_widget("random_seed", 20260612)
CALIBRATION_PCT = _get_int_widget("calibration_pct", 40)
VIDEOS_PER_CHANNEL = _get_int_widget("videos_per_channel", 10)
PROMPT_MAX_CHANNEL_DESCRIPTION_CHARS = _get_int_widget("prompt_max_channel_description_chars", 900)
PROMPT_MAX_VIDEO_DESCRIPTION_CHARS = _get_int_widget("prompt_max_video_description_chars", 260)
PROMPT_MAX_CHARS = _get_int_widget("prompt_max_chars", 11000)
BATCH_OUTPUT_DIR = _get_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches").rstrip("/")
MAX_REQUESTS_PER_FILE = _get_int_widget("max_requests_per_file", 10000)
MAX_OUTPUT_TOKENS = _get_int_widget("max_output_tokens", 4500)
MODELS = json.loads(_get_widget("models_json", DEFAULT_MODELS_JSON))
SUBMIT_BATCHES = _get_bool_widget("submit_batches", False)
SUBMIT_PROVIDER_FILTER = {p.strip().lower() for p in _get_widget("submit_provider_filter", "anthropic,gemini,deepseek").split(",") if p.strip()}
SKIP_EXISTING_SUBMITTED_BATCHES = _get_bool_widget("skip_existing_submitted_batches", True)
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai-api-key")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic-api-key")
GEMINI_SECRET_KEY = _get_widget("gemini_secret_key", "gemini-api-key")
DEEPSEEK_SECRET_KEY = _get_widget("deepseek_secret_key", "deepseek-api-key")
OPENAI_REASONING_EFFORT = _get_widget("openai_reasoning_effort", "minimal").strip().lower()
GEMINI_THINKING_LEVEL = _get_widget("gemini_thinking_level", "low").strip()
DEEPSEEK_THINKING_TYPE = _get_widget("deepseek_thinking_type", "disabled").strip().lower()
DEEPSEEK_MAX_OUTPUT_TOKENS = _get_int_widget("deepseek_max_output_tokens", 4500)
DEEPSEEK_MAX_WORKERS = _get_int_widget("deepseek_max_workers", 12)
DEEPSEEK_REQUEST_TIMEOUT_SECONDS = _get_float_widget("deepseek_request_timeout_seconds", 90.0)
DEEPSEEK_MAX_RETRIES = _get_int_widget("deepseek_max_retries", 1)
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches/results").rstrip("/")

if not 1 <= CALIBRATION_PCT <= 99:
    raise ValueError("calibration_pct must be between 1 and 99")

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
    return F.when(col.rlike("/wiki/"), F.regexp_replace(F.regexp_extract(col, r"/wiki/(.*)$", 1), "%20", "_")).otherwise(col)


def topic_name_expr(slug_col):
    return F.regexp_replace(slug_col, "_", " ")


def first_existing_col(df, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


prompt_inputs_full = out_table("prompt_inputs")
requests_full = out_table("requests")
batch_files_full = out_table("batch_files")
batch_jobs_full = out_table("batch_jobs")

print("RUN_ID:", RUN_ID)
print("outputs:", prompt_inputs_full, requests_full, batch_files_full, batch_jobs_full)

# COMMAND ----------
channels_src = spark.table(table_ref(CHANNELS_TABLE))
channel_desc_col = first_existing_col(channels_src, ["channel_description", "description", "about", "channel_about", "long_description"])
channel_handle_col = first_existing_col(channels_src, ["handle", "channel_handle", "custom_url"])
channel_country_col = first_existing_col(channels_src, ["country", "country_code", "channel_country"])

channels_select = [
    F.col("channel_id").cast("string").alias("channel_id"),
    F.col("channel_name").cast("string").alias("channel_name") if "channel_name" in channels_src.columns else F.lit(None).cast("string").alias("channel_name"),
    F.col("language_code").cast("string").alias("source_language_code") if "language_code" in channels_src.columns else F.lit(None).cast("string").alias("source_language_code"),
    F.col(channel_desc_col).cast("string").alias("channel_description") if channel_desc_col else F.lit("").cast("string").alias("channel_description"),
    F.col(channel_handle_col).cast("string").alias("channel_handle") if channel_handle_col else F.lit(None).cast("string").alias("channel_handle"),
    F.col(channel_country_col).cast("string").alias("channel_country") if channel_country_col else F.lit(None).cast("string").alias("channel_country"),
]

channels = (
    channels_src
    .select(*channels_select)
    .where(F.col("channel_id").isNotNull())
    .dropDuplicates(["channel_id"])
)

category_raw = (
    spark.table(table_ref(CATEGORY_TABLE))
    .select(
        F.col("canonical_id").cast("string").alias("channel_id"),
        F.col("topic_categories"),
        F.col("collected_at"),
        F.col("collected_date") if "collected_date" in spark.table(table_ref(CATEGORY_TABLE)).columns else F.lit(None).alias("collected_date"),
    )
    .where(F.col("canonical_id").isNotNull())
)

latest_category = (
    category_raw.join(F.broadcast(channels.select("channel_id")), on="channel_id", how="inner")
    .withColumn("_rn", F.row_number().over(Window.partitionBy("channel_id").orderBy(F.desc("collected_at"), F.desc("collected_date"))))
    .where(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn(
        "topic_category_urls",
        F.expr("array_distinct(filter(coalesce(topic_categories, cast(array() as array<string>)), x -> x is not null and length(trim(x)) > 0))"),
    )
    .withColumn("topic_category_count", F.size("topic_category_urls"))
    .withColumn("primary_topic_url", F.expr("get(topic_category_urls, 0)").cast("string"))
    .withColumn("primary_topic_slug", topic_slug_expr(F.col("primary_topic_url")))
    .withColumn("primary_topic_name", topic_name_expr(F.col("primary_topic_slug")))
    .withColumn(
        "topic_slugs",
        F.expr("transform(topic_category_urls, x -> case when x rlike '/wiki/' then regexp_replace(regexp_extract(x, '/wiki/(.*)$', 1), '%20', '_') else x end)"),
    )
)

allowed_categories_df = (
    latest_category
    .select(F.explode_outer("topic_slugs").alias("topic_slug"))
    .where(F.col("topic_slug").isNotNull() & (F.length(F.trim("topic_slug")) > 0))
    .dropDuplicates(["topic_slug"])
    .withColumn("topic_name", topic_name_expr(F.col("topic_slug")))
    .orderBy("topic_slug")
)
allowed_categories = [row.asDict(recursive=True) for row in allowed_categories_df.collect()]
allowed_slugs = [row["topic_slug"] for row in allowed_categories]
if not allowed_slugs:
    raise RuntimeError("No observed YouTube API topic labels found.")
if len(allowed_slugs) < 40:
    raise RuntimeError(f"Only {len(allowed_slugs)} labels found; expected the full observed topicCategories vocabulary.")

allowed_text = "\n".join([f"- {row['topic_slug']}: {row['topic_name']}" for row in allowed_categories])
print(f"Allowed observed topic labels: {len(allowed_slugs):,}")

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
        latest_category.select(
            "channel_id",
            "topic_category_urls",
            "topic_slugs",
            "primary_topic_url",
            "primary_topic_slug",
            "primary_topic_name",
            "topic_category_count",
            "collected_at",
        ),
        on="channel_id",
        how="left",
    )
    .withColumn("topic_category_urls", F.coalesce(F.col("topic_category_urls"), F.array().cast("array<string>")))
    .withColumn("topic_slugs", F.coalesce(F.col("topic_slugs"), F.array().cast("array<string>")))
    .withColumn("topic_category_count", F.coalesce(F.col("topic_category_count"), F.lit(0)))
)

split_score = F.pmod(F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.lit(str(RANDOM_SEED)), F.lit("topic_multilabel_eval_split"))), F.lit(100))
sampled = sampled.withColumn("eval_split", F.when(split_score < F.lit(CALIBRATION_PCT), F.lit("calibration")).otherwise(F.lit("heldout_test")))

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

# COMMAND ----------
probability_properties = {
    slug: {"type": "number", "minimum": 0, "maximum": 1}
    for slug in allowed_slugs
}

MULTILABEL_RESPONSE_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "label_probabilities": {
            "type": "object",
            "additionalProperties": False,
            "properties": probability_properties,
            "required": allowed_slugs,
        },
        "predicted_positive_labels": {
            "type": "array",
            "items": {"type": "string", "enum": allowed_slugs},
        },
        "uncertain_labels": {
            "type": "array",
            "items": {"type": "string", "enum": allowed_slugs},
        },
        "rationale_short": {"type": "string", "maxLength": 240},
    },
    "required": ["label_probabilities", "predicted_positive_labels", "uncertain_labels", "rationale_short"],
}

SYSTEM_PROMPT = f"""You are predicting the current YouTube Data API channel topicCategories labels for an academic validation study.

Important target definition:
- Predict the exact set of Wikipedia-derived topic category labels that YouTube's API would return for this channel.
- This is not human gold-standard coding and not a single best category task.
- Multiple labels may be present, and an empty set is possible.
- The order of YouTube topicCategories is not meaningful; do not infer primary/secondary order.
- Broad labels such as Music, Entertainment, Lifestyle_(sociology), Sport, Society, and Video_game_culture may co-occur with specific labels when YouTube's labeling conventions suggest they would both be returned.
- Use only the channel/video evidence in the user prompt. The held-out YouTube labels are not shown to you.

Allowed label ids:
{allowed_text}

Return one compact JSON object only, with no markdown and no text outside JSON.
Required format:
{{"label_probabilities":{{"LABEL_ID":0.00}},"predicted_positive_labels":["LABEL_ID"],"uncertain_labels":["LABEL_ID"],"rationale_short":"brief evidence, max 220 chars"}}

Rules for the JSON:
- label_probabilities must contain every allowed label id exactly once.
- Probabilities must be numbers from 0 to 1, rounded to two decimals when possible.
- predicted_positive_labels should contain every label you think YouTube would return after considering the full evidence.
- uncertain_labels should contain labels where the probability is near the decision boundary.
""".strip()

# COMMAND ----------
videos = spark.table(table_ref(VIDEOS_TABLE))
video_title_col = "video_title" if "video_title" in videos.columns else ("title" if "title" in videos.columns else None)
if video_title_col is None:
    raise RuntimeError(f"No video title column found in {VIDEOS_TABLE}")
video_desc_col = "description" if "description" in videos.columns else ("video_description" if "video_description" in videos.columns else None)
rank_col = "published_at" if "published_at" in videos.columns else ("ingestion_timestamp" if "ingestion_timestamp" in videos.columns else None)

video_base = (
    videos.join(F.broadcast(sample_ids), on="channel_id", how="inner")
    .select(
        F.col("channel_id").cast("string").alias("channel_id"),
        F.col("video_id").cast("string").alias("video_id") if "video_id" in videos.columns else F.lit(None).cast("string").alias("video_id"),
        F.col(video_title_col).cast("string").alias("video_title"),
        F.col(video_desc_col).cast("string").alias("video_description") if video_desc_col else F.lit("").cast("string").alias("video_description"),
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
    .withColumn("channel_description_clean", F.substring(F.regexp_replace(F.coalesce(F.col("channel_description"), F.lit("")), r"[\r\n\t]+", " "), 1, PROMPT_MAX_CHANNEL_DESCRIPTION_CHARS))
    .withColumn(
        "prompt_user",
        F.substring(
            F.concat(
                F.lit("Predict the YouTube API topicCategories label set for this channel. Return strict JSON only.\n\n"),
                F.lit("Channel name: "), F.coalesce(F.col("channel_name"), F.lit("")), F.lit("\n"),
                F.when(F.col("channel_handle").isNotNull(), F.concat(F.lit("Channel handle/custom URL: "), F.col("channel_handle"), F.lit("\n"))).otherwise(F.lit("")),
                F.when(F.col("channel_country").isNotNull(), F.concat(F.lit("Channel country: "), F.col("channel_country"), F.lit("\n"))).otherwise(F.lit("")),
                F.lit("Detected language: "), F.coalesce(F.col("primary_language_label"), F.lit("unknown")),
                F.lit(" ("), F.coalesce(F.col("primary_language_iso639_3"), F.lit("unknown")), F.lit(")\n"),
                F.when(F.length(F.col("channel_description_clean")) > 0, F.concat(F.lit("Channel description: "), F.col("channel_description_clean"), F.lit("\n"))).otherwise(F.lit("")),
                F.lit("\nRecent videos:\n"), F.coalesce(F.col("recent_videos_text"), F.lit("")), F.lit("\n"),
            ),
            1,
            PROMPT_MAX_CHARS,
        ),
    )
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("sample_mode", F.lit("random_full_youtube_too"))
    .withColumn("target_definition", F.lit("exact_observed_youtube_topic_categories_array"))
    .withColumn("prompt_version", F.lit("topic_categories_multilabel_probabilities_v1"))
    .withColumn("allowed_topic_labels", F.array(*[F.lit(slug) for slug in allowed_slugs]))
    .withColumn("system_prompt", F.lit(SYSTEM_PROMPT))
    .withColumn("created_at", F.current_timestamp())
)

write_run_scoped(prompt_inputs, prompt_inputs_full)
persisted_prompt_inputs = (
    spark.table(prompt_inputs_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .cache()
)
persisted_prompt_inputs.count()

summary = persisted_prompt_inputs.agg(
    F.count("*").alias("n_channels"),
    F.sum(F.when(F.col("topic_category_count") > 0, 1).otherwise(0)).alias("n_with_nonempty_reference_set"),
    F.sum(F.when(F.col("topic_category_count") == 0, 1).otherwise(0)).alias("n_with_empty_reference_set"),
    F.sum(F.when(F.col("eval_split") == "calibration", 1).otherwise(0)).alias("n_calibration"),
    F.sum(F.when(F.col("eval_split") == "heldout_test", 1).otherwise(0)).alias("n_heldout_test"),
    F.avg(F.col("topic_category_count")).alias("mean_reference_label_count"),
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
                        "name": "youtube_topic_categories_multilabel_prediction",
                        "schema": MULTILABEL_RESPONSE_JSON_SCHEMA,
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
    persisted_prompt_inputs.crossJoin(models_df)
    .withColumn("request_id", F.concat(F.lit("ytcm_"), F.substring(F.sha2(F.concat_ws("||", F.col("run_id"), F.col("provider"), F.col("model"), F.col("channel_id")), 256), 1, 59)))
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
display(
    spark.table(requests_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .groupBy("provider", "model", "chunk_id")
    .count()
    .orderBy("provider", "model", "chunk_id")
)

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
        metadata={"run_id": RUN_ID, "task": "youtube_topic_categories_multilabel", "model": model},
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
            started = time.time()
            try:
                response = _session().post(
                    "https://api.deepseek.com/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json=body,
                    timeout=DEEPSEEK_REQUEST_TIMEOUT_SECONDS,
                )
                duration = time.time() - started
                if response.status_code < 500:
                    if response.ok:
                        return {
                            "custom_id": custom_id,
                            "response": {"status_code": response.status_code, "body": response.json()},
                            "_deepseek_direct_metadata": {"duration_seconds": duration, "attempt": attempt + 1},
                        }
                    return {
                        "custom_id": custom_id,
                        "response": {"status_code": response.status_code},
                        "error": {"message": response.text[:2000]},
                        "_deepseek_direct_metadata": {"duration_seconds": duration, "attempt": attempt + 1},
                    }
                last_error = response.text[:2000]
            except Exception as exc:
                last_error = repr(exc)
            time.sleep(min(2 ** attempt, 8))
        return {"custom_id": custom_id, "error": {"message": last_error or "unknown_error"}}

    with open(local_jsonl_path, "r", encoding="utf-8") as f:
        lines = [line for line in f if line.strip()]
    with ThreadPoolExecutor(max_workers=DEEPSEEK_MAX_WORKERS) as pool:
        futures = [pool.submit(_call_line, line) for line in lines]
        with open(result_path, "w", encoding="utf-8") as out:
            for fut in as_completed(futures):
                out.write(json.dumps(fut.result(), ensure_ascii=False) + "\n")
    return {"provider_file_id": None, "provider_batch_id": f"deepseek_direct:{result_path}", "provider_status": "completed"}


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
    if SKIP_EXISTING_SUBMITTED_BATCHES and _table_exists_full(batch_jobs_full):
        replace_keys = {(r[1], r[2], int(r[4])) for r in records}
        preserved = []
        for row in spark.table(batch_jobs_full).where(F.col("run_id") == F.lit(RUN_ID)).collect():
            key = (row["provider"], row["model"], int(row["chunk_id"]))
            if key not in replace_keys:
                preserved.append(tuple(row[c] for c in batch_job_columns))
        write_run_scoped(spark.createDataFrame(preserved + records, batch_job_schema), batch_jobs_full)
        return
    write_run_scoped(spark.createDataFrame(records, batch_job_schema), batch_jobs_full)


job_records = []
if SUBMIT_BATCHES:
    existing = None
    if SKIP_EXISTING_SUBMITTED_BATCHES and _table_exists_full(batch_jobs_full):
        existing = (
            spark.table(batch_jobs_full)
            .where(F.col("run_id") == F.lit(RUN_ID))
            .where(F.col("submission_status") == F.lit("submitted"))
            .select("provider", "model", "chunk_id")
            .dropDuplicates()
        )
    files_to_submit = spark.table(batch_files_full).where(F.col("run_id") == F.lit(RUN_ID))
    if existing is not None:
        files_to_submit = files_to_submit.join(existing, on=["provider", "model", "chunk_id"], how="left_anti")
    files_to_submit = (
        files_to_submit
        .withColumn("_submit_priority", F.when(F.col("provider") == F.lit("deepseek"), F.lit(99)).otherwise(F.lit(0)))
        .orderBy("_submit_priority", "provider", "model", "chunk_id")
        .drop("_submit_priority")
    )
    for row in files_to_submit.collect():
        provider = row["provider"]
        model = row["model"]
        path = row["local_jsonl_path"]
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
                raise ValueError(f"Unsupported provider: {provider}")
            record = (RUN_ID, provider, model, row["model_tier"], int(row["chunk_id"]), path, result.get("provider_file_id"), result.get("provider_batch_id"), result.get("provider_status"), "submitted", None, datetime.utcnow().isoformat())
            job_records.append(record)
            upsert_batch_job_records([record])
            print("Submitted", provider, model, row["chunk_id"], result)
        except Exception as exc:
            record = (RUN_ID, provider, model, row["model_tier"], int(row["chunk_id"]), path, None, None, None, "error", repr(exc)[:2000], datetime.utcnow().isoformat())
            job_records.append(record)
            upsert_batch_job_records([record])
            print("Submission error", provider, model, row["chunk_id"], repr(exc))
else:
    print("submit_batches=false; wrote prompt inputs and batch JSONL files only.")

if job_records:
    upsert_batch_job_records(job_records)

payload = {
    "run_id": RUN_ID,
    "prompt_inputs_table": prompt_inputs_full,
    "requests_table": requests_full,
    "batch_files_table": batch_files_full,
    "batch_jobs_table": batch_jobs_full,
    "allowed_topic_label_count": len(allowed_slugs),
    "prompt_input_summary": summary,
    "submitted": bool(SUBMIT_BATCHES),
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True, default=str))
