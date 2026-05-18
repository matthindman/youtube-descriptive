# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube Census: LLM-based YouTube category classification bake-off
# MAGIC
# MAGIC This notebook prepares channel-level prompts, generates provider-specific batch request files for OpenAI, Anthropic, and Gemini, optionally submits batches, imports batch results, and evaluates model agreement against existing YouTube/category labels.
# MAGIC
# MAGIC **Run language classification first.** This notebook stratifies the bake-off by detected channel language from `yt_lid_openlid_v3_channels`.
# MAGIC
# MAGIC **Default mode:** `labeled_validation`. It samples from channels with existing labels so model predictions can be benchmarked before any full-corpus run.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Install notebook-scoped dependencies

# COMMAND ----------
# MAGIC %pip install "openai>=2.0.0" anthropic "google-genai>=1.51.0" pandas pyarrow tenacity

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------
import json
import os
import re
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------
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


def _get_optional_float_widget(name: str, default: Optional[float] = None) -> Optional[float]:
    raw = _get_widget(name, "" if default is None else str(default)).strip()
    if raw == "" or raw.lower() in {"none", "null", "omit", "default"}:
        return None
    return float(raw)


_create_text_widget("catalog", "prod_tads")
_create_text_widget("schema", "youtube")
_create_text_widget("channels_table", "yt_sl_channels")
_create_text_widget("videos_table", "yt_sl_videos")
_create_text_widget("language_table", "yt_lid_openlid_v3_channels")

# Existing label source. The screenshot shows ai_label in yt_sl_videos; override if your observed category column differs.
_create_text_widget("existing_label_col", "ai_label")
_create_text_widget("existing_label_source", "videos")  # videos, channels, or expert_table
_create_text_widget("expert_label_table", "")
_create_text_widget("expert_label_channel_id_col", "channel_id")
_create_text_widget("expert_label_category_col", "category_id")
_create_text_widget("min_reference_labeled_videos", "3")
_create_text_widget("min_reference_agreement_fraction", "0.50")

# Output tables.
_create_text_widget("prompt_inputs_table", "yt_category_llm_prompt_inputs")
_create_text_widget("request_table", "yt_category_llm_requests")
_create_text_widget("batch_files_table", "yt_category_llm_batch_files")
_create_text_widget("batch_jobs_table", "yt_category_llm_batch_jobs")
_create_text_widget("raw_results_table", "yt_category_llm_raw_results")
_create_text_widget("parsed_predictions_table", "yt_category_llm_predictions")
_create_text_widget("eval_metrics_table", "yt_category_llm_eval_metrics")
_create_text_widget("agreement_table", "yt_category_llm_model_agreement")

# Sampling and prompt construction.
_create_text_widget("run_id", "")
_create_text_widget("run_mode", "labeled_validation")  # labeled_validation, unlabeled_pilot, full_unlabeled, all_channels
_create_text_widget("n_per_language_category", "25")
_create_text_widget("n_per_language_unlabeled", "100")
_create_text_widget("max_total_channels", "50000")
_create_text_widget("videos_per_channel", "10")
_create_text_widget("video_rank_column", "")
_create_text_widget("cap_videos_without_rank_column", "true")
_create_text_widget("prompt_max_channel_description_chars", "1200")
_create_text_widget("prompt_max_video_description_chars", "350")
_create_text_widget("prompt_max_chars", "7000")
_create_text_widget("random_seed", "20260511")

# Batch generation.
_create_text_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_batches")
_create_text_widget("max_requests_per_file", "10000")
_create_text_widget("max_output_tokens", "300")
_create_text_widget("temperature", "")  # blank/omit = provider default; do not set 0.0 for GPT-5/Gemini-3 unless explicitly tested

# Model list. Replace display IDs with exact API model IDs used by the three providers.
DEFAULT_MODELS_JSON = json.dumps([
    {"provider": "openai", "model": "gpt-5.5", "tier": "frontier"},
    {"provider": "openai", "model": "gpt-5-nano", "tier": "small"},
    {"provider": "anthropic", "model": "claude-opus-4-7", "tier": "frontier"},
    {"provider": "anthropic", "model": "claude-haiku-4-5", "tier": "small"},
    {"provider": "gemini", "model": "gemini-3.1-pro-preview", "tier": "frontier"},
    {"provider": "gemini", "model": "gemini-3.1-flash-lite-preview", "tier": "small"},
], ensure_ascii=False)
_create_text_widget("models_json", DEFAULT_MODELS_JSON)
_create_text_widget("openai_endpoint_mode", "auto")  # auto, responses, or chat_completions
_create_text_widget("openai_reasoning_effort", "minimal")  # blank to omit; useful for GPT-5 Responses API
_create_text_widget("gemini_thinking_level", "low")  # blank to omit; low/minimal can reduce latency/cost for Gemini 3

# Optional submission. Leave false to only write JSONL files for manual/provider-side submission.
_create_text_widget("submit_batches", "false")
_create_text_widget("secret_scope", "llm-api-keys")
_create_text_widget("openai_secret_key", "openai_api_key")
_create_text_widget("anthropic_secret_key", "anthropic_api_key")
_create_text_widget("gemini_secret_key", "gemini_api_key")

# Result import. Place downloaded JSONL result files under this directory and set import_results=true.
_create_text_widget("import_results", "false")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_category_batches/results")

# COMMAND ----------
CATALOG = _get_widget("catalog", "prod_tads")
SCHEMA = _get_widget("schema", "youtube")
CHANNELS_TABLE = _get_widget("channels_table", "yt_sl_channels")
VIDEOS_TABLE = _get_widget("videos_table", "yt_sl_videos")
LANGUAGE_TABLE = _get_widget("language_table", "yt_lid_openlid_v3_channels")

EXISTING_LABEL_COL = _get_widget("existing_label_col", "ai_label")
EXISTING_LABEL_SOURCE = _get_widget("existing_label_source", "videos").strip().lower()
EXPERT_LABEL_TABLE = _get_widget("expert_label_table", "").strip()
EXPERT_LABEL_CHANNEL_ID_COL = _get_widget("expert_label_channel_id_col", "channel_id").strip()
EXPERT_LABEL_CATEGORY_COL = _get_widget("expert_label_category_col", "category_id").strip()
MIN_REFERENCE_LABELED_VIDEOS = _get_int_widget("min_reference_labeled_videos", 3)
MIN_REFERENCE_AGREEMENT_FRACTION = _get_float_widget("min_reference_agreement_fraction", 0.50)

PROMPT_INPUTS_TABLE = _get_widget("prompt_inputs_table", "yt_category_llm_prompt_inputs")
REQUEST_TABLE = _get_widget("request_table", "yt_category_llm_requests")
BATCH_FILES_TABLE = _get_widget("batch_files_table", "yt_category_llm_batch_files")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_category_llm_batch_jobs")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_category_llm_raw_results")
PARSED_PREDICTIONS_TABLE = _get_widget("parsed_predictions_table", "yt_category_llm_predictions")
EVAL_METRICS_TABLE = _get_widget("eval_metrics_table", "yt_category_llm_eval_metrics")
AGREEMENT_TABLE = _get_widget("agreement_table", "yt_category_llm_model_agreement")

RUN_ID = _get_widget("run_id", "").strip() or datetime.utcnow().strftime("ytcat_%Y%m%d_%H%M%S")
RUN_MODE = _get_widget("run_mode", "labeled_validation").strip().lower()
N_PER_LANGUAGE_CATEGORY = _get_int_widget("n_per_language_category", 25)
N_PER_LANGUAGE_UNLABELED = _get_int_widget("n_per_language_unlabeled", 100)
MAX_TOTAL_CHANNELS = _get_int_widget("max_total_channels", 50000)
VIDEOS_PER_CHANNEL = _get_int_widget("videos_per_channel", 10)
VIDEO_RANK_COLUMN = _get_widget("video_rank_column", "").strip()
CAP_VIDEOS_WITHOUT_RANK_COLUMN = _get_bool_widget("cap_videos_without_rank_column", True)
PROMPT_MAX_CHANNEL_DESCRIPTION_CHARS = _get_int_widget("prompt_max_channel_description_chars", 1200)
PROMPT_MAX_VIDEO_DESCRIPTION_CHARS = _get_int_widget("prompt_max_video_description_chars", 350)
PROMPT_MAX_CHARS = _get_int_widget("prompt_max_chars", 7000)
RANDOM_SEED = _get_int_widget("random_seed", 20260511)

BATCH_OUTPUT_DIR = _get_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_batches").rstrip("/")
MAX_REQUESTS_PER_FILE = _get_int_widget("max_requests_per_file", 10000)
MAX_OUTPUT_TOKENS = _get_int_widget("max_output_tokens", 300)
TEMPERATURE = _get_optional_float_widget("temperature", None)
MODELS = json.loads(_get_widget("models_json", DEFAULT_MODELS_JSON))
OPENAI_ENDPOINT_MODE = _get_widget("openai_endpoint_mode", "auto").strip().lower()
OPENAI_REASONING_EFFORT = _get_widget("openai_reasoning_effort", "minimal").strip().lower()
GEMINI_THINKING_LEVEL = _get_widget("gemini_thinking_level", "low").strip().lower()

SUBMIT_BATCHES = _get_bool_widget("submit_batches", False)
SECRET_SCOPE = _get_widget("secret_scope", "llm-api-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai_api_key")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic_api_key")
GEMINI_SECRET_KEY = _get_widget("gemini_secret_key", "gemini_api_key")

IMPORT_RESULTS = _get_bool_widget("import_results", False)
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_category_batches/results").rstrip("/")

if RUN_MODE not in {"labeled_validation", "unlabeled_pilot", "full_unlabeled", "all_channels"}:
    raise ValueError("run_mode must be one of: labeled_validation, unlabeled_pilot, full_unlabeled, all_channels")
if EXISTING_LABEL_SOURCE not in {"videos", "channels", "expert_table"}:
    raise ValueError("existing_label_source must be one of: videos, channels, expert_table")
if OPENAI_ENDPOINT_MODE not in {"auto", "responses", "chat_completions"}:
    raise ValueError("openai_endpoint_mode must be one of: auto, responses, chat_completions")

# COMMAND ----------
def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def table_ref(name: str) -> str:
    """Return a SQL table reference. Unqualified names are interpreted in the configured catalog/schema."""
    if "." not in name:
        return fqtn(name)
    return ".".join(f"`{part.strip('`')}`" for part in name.split("."))


def safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", model)


channels_full = fqtn(CHANNELS_TABLE)
videos_full = fqtn(VIDEOS_TABLE)
language_full = fqtn(LANGUAGE_TABLE)
prompt_inputs_full = fqtn(PROMPT_INPUTS_TABLE)
request_full = fqtn(REQUEST_TABLE)
batch_files_full = fqtn(BATCH_FILES_TABLE)
batch_jobs_full = fqtn(BATCH_JOBS_TABLE)
raw_results_full = fqtn(RAW_RESULTS_TABLE)
parsed_predictions_full = fqtn(PARSED_PREDICTIONS_TABLE)
eval_metrics_full = fqtn(EVAL_METRICS_TABLE)
agreement_full = fqtn(AGREEMENT_TABLE)

print("RUN_ID:", RUN_ID)
print("RUN_MODE:", RUN_MODE)
print("Models:", json.dumps(MODELS, indent=2))
print("Prompt input table:", prompt_inputs_full)
print("Request table:", request_full)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Category taxonomy
# MAGIC
# MAGIC The prompt forces exactly one category from the 15 standard YouTube-style categories below. If your source labels use a different 15-class taxonomy, edit this mapping before running.

# COMMAND ----------
YT_CATEGORIES = [
    {"category_id": "1", "category_name": "Film & Animation", "definition": "Movies, film clips, animation, trailers, cinematic shorts, film criticism."},
    {"category_id": "2", "category_name": "Autos & Vehicles", "definition": "Cars, trucks, motorcycles, vehicle reviews, repairs, driving, automotive culture."},
    {"category_id": "10", "category_name": "Music", "definition": "Songs, music videos, performances, artists, DJs, lyrics, instruments, music commentary."},
    {"category_id": "15", "category_name": "Pets & Animals", "definition": "Pets, wildlife, animal care, animal behavior, zoos, farms, animal rescue."},
    {"category_id": "17", "category_name": "Sports", "definition": "Sports highlights, matches, athletes, coaching, fitness competition, sporting news."},
    {"category_id": "19", "category_name": "Travel & Events", "definition": "Travel guides, tourism, destinations, events, festivals, hotels, transportation for trips."},
    {"category_id": "20", "category_name": "Gaming", "definition": "Video games, gameplay, streaming, esports, walkthroughs, game reviews, game culture."},
    {"category_id": "22", "category_name": "People & Blogs", "definition": "Vlogs, personal updates, lifestyle diaries, creator commentary, general personal content."},
    {"category_id": "23", "category_name": "Comedy", "definition": "Sketches, stand-up, jokes, humorous commentary, satire primarily intended as comedy."},
    {"category_id": "24", "category_name": "Entertainment", "definition": "Celebrity, variety, pop culture, TV-style entertainment, general entertainment not otherwise classified."},
    {"category_id": "25", "category_name": "News & Politics", "definition": "News, public affairs, political commentary, elections, geopolitics, journalism, current events."},
    {"category_id": "26", "category_name": "Howto & Style", "definition": "Tutorials, beauty, fashion, DIY, cooking, home, repair, practical how-to instruction."},
    {"category_id": "27", "category_name": "Education", "definition": "Teaching, lectures, explainers, academic content, training, language learning, documentary education."},
    {"category_id": "28", "category_name": "Science & Technology", "definition": "Science, engineering, software, gadgets, AI, computing, medicine, technical reviews."},
    {"category_id": "29", "category_name": "Nonprofits & Activism", "definition": "NGOs, charities, advocacy, activism, social movements, public-interest campaigns."},
]

category_map_rows = []
for c in YT_CATEGORIES:
    category_name_key = c["category_name"].lower()
    category_name_key_normalized = category_name_key.replace("&", "and")
    category_map_rows.append((c["category_id"], c["category_name"], c["definition"], c["category_id"].lower(), category_name_key_normalized))
    category_map_rows.append((c["category_id"], c["category_name"], c["definition"], category_name_key, category_name_key_normalized))
    category_map_rows.append((c["category_id"], c["category_name"], c["definition"], category_name_key_normalized, category_name_key_normalized))
# Common aliases.
aliases = {
    "film": "1", "film and animation": "1", "autos": "2", "auto": "2", "vehicles": "2", "music": "10",
    "animals": "15", "pets": "15", "sports": "17", "travel": "19", "events": "19", "gaming": "20",
    "games": "20", "people": "22", "blogs": "22", "vlog": "22", "vlogs": "22", "comedy": "23",
    "entertainment": "24", "news": "25", "politics": "25", "news and politics": "25", "howto": "26",
    "how-to": "26", "style": "26", "howto and style": "26", "education": "27", "educational": "27",
    "science": "28", "technology": "28", "science and technology": "28", "nonprofits": "29", "activism": "29",
    "nonprofits and activism": "29",
}
by_id = {c["category_id"]: c for c in YT_CATEGORIES}
for alias, cid in aliases.items():
    c = by_id[cid]
    category_map_rows.append((c["category_id"], c["category_name"], c["definition"], alias.lower(), c["category_name"].lower()))

category_map_df = spark.createDataFrame(
    category_map_rows,
    ["category_id", "category_name", "definition", "label_key", "canonical_label_key"],
).dropDuplicates(["label_key"])
category_map_df.createOrReplaceTempView("_youtube_category_map")

CATEGORY_LIST_FOR_PROMPT = "\n".join([
    f"- {c['category_id']}: {c['category_name']} — {c['definition']}"
    for c in YT_CATEGORIES
])
VALID_CATEGORY_IDS = set(c["category_id"] for c in YT_CATEGORIES)
CATEGORY_IDS_SORTED = sorted(VALID_CATEGORY_IDS, key=lambda x: int(x))
CATEGORY_NAME_BY_ID = {c["category_id"]: c["category_name"] for c in YT_CATEGORIES}
CATEGORY_ID_BY_NAME = {c["category_name"].lower(): c["category_id"] for c in YT_CATEGORIES}

CATEGORY_RESPONSE_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "category_id": {"type": "string", "enum": CATEGORY_IDS_SORTED},
        "category_name": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "ambiguous": {"type": "boolean"},
        "rationale_short": {"type": "string", "maxLength": 220},
    },
    "required": ["category_id", "category_name", "confidence", "ambiguous", "rationale_short"],
}

# COMMAND ----------
SYSTEM_PROMPT = f"""You are classifying YouTube channels for an academic study. Choose exactly one category from the allowed YouTube category list. Use the channel name, description, detected language, and recent video titles/descriptions. Do not infer from subscriber count or popularity. If the evidence is mixed, choose the category that best describes the channel's main recurring content.

Allowed categories:
{CATEGORY_LIST_FOR_PROMPT}

Return strict JSON only, with no markdown and no explanatory text outside JSON. The JSON schema is:
{{
  "category_id": "one of: {', '.join(sorted(VALID_CATEGORY_IDS, key=lambda x: int(x)))}",
  "category_name": "exact category name",
  "confidence": 0.0,
  "ambiguous": false,
  "rationale_short": "brief evidence, maximum 25 words"
}}
""".strip()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Build channel-level prompt inputs

# COMMAND ----------
def columns_lower_map(df) -> Dict[str, str]:
    return {c.lower(): c for c in df.columns}


def first_existing_column(df, candidates: Iterable[str], override: str = "") -> Optional[str]:
    cmap = columns_lower_map(df)
    if override:
        if override.lower() in cmap:
            return cmap[override.lower()]
        raise ValueError(f"Requested override column `{override}` not found. Available columns: {df.columns}")
    for c in candidates:
        if c.lower() in cmap:
            return cmap[c.lower()]
    return None


def stable_hash_order_col(*cols: str):
    """Stable pseudo-random order independent of Spark partition layout."""
    exprs = [F.lit(str(RANDOM_SEED))]
    for c in cols:
        exprs.append(F.coalesce(F.col(c).cast("string"), F.lit("")))
    return F.sha2(F.concat_ws("||", *exprs), 256)


def normalize_label_key(col):
    return F.lower(F.trim(F.regexp_replace(F.coalesce(col.cast("string"), F.lit("")), "&", "and")))


def normalize_reference_label_frame(raw_df, channel_col: str, label_col: str, source_name: str):
    """Map raw reference labels into the 15-category taxonomy."""
    return (
        raw_df
        .select(
            F.col(channel_col).cast("string").alias("channel_id"),
            F.col(label_col).cast("string").alias("reference_raw_label"),
        )
        .where(F.col("reference_raw_label").isNotNull() & (F.length(F.trim(F.col("reference_raw_label"))) > 0))
        .withColumn("label_key", normalize_label_key(F.col("reference_raw_label")))
        .join(category_map_df.select("label_key", "category_id", "category_name"), on="label_key", how="left")
        .where(F.col("category_id").isNotNull())
        .withColumn("reference_label_source", F.lit(source_name))
    )


def aggregate_reference_labels(norm_df, min_count: int, min_fraction: float):
    """Collapse row/video labels to one channel-level reference label with deterministic numeric tiebreaks."""
    totals = norm_df.groupBy("channel_id").agg(F.count("*").alias("reference_total_label_count"))
    votes = (
        norm_df
        .groupBy("channel_id", "category_id", "category_name")
        .agg(
            F.count("*").alias("reference_label_vote_count"),
            F.first("reference_raw_label", ignorenulls=True).alias("reference_raw_label"),
            F.first("reference_label_source", ignorenulls=True).alias("reference_label_source"),
        )
        .join(totals, on="channel_id", how="inner")
        .withColumn("reference_label_agreement_fraction", F.col("reference_label_vote_count") / F.col("reference_total_label_count"))
        .withColumn("category_id_int", F.col("category_id").cast("int"))
    )
    lw = Window.partitionBy("channel_id").orderBy(
        F.desc("reference_label_vote_count"),
        F.desc("reference_label_agreement_fraction"),
        F.asc("category_id_int"),
    )
    return (
        votes
        .withColumn("reference_label_rank", F.row_number().over(lw))
        .where(F.col("reference_label_rank") == 1)
        .where(
            (F.col("reference_label_vote_count") >= F.lit(int(min_count))) &
            (F.col("reference_label_agreement_fraction") >= F.lit(float(min_fraction)))
        )
        .select(
            "channel_id",
            "reference_raw_label",
            F.col("category_id").alias("reference_category_id"),
            F.col("category_name").alias("reference_category_name"),
            "reference_label_vote_count",
            "reference_total_label_count",
            "reference_label_agreement_fraction",
            "reference_label_source",
        )
    )


channels = spark.table(channels_full)
videos = spark.table(videos_full)
try:
    languages = spark.table(language_full)
except Exception:
    print(f"Language table {language_full} not found; continuing without detected-language stratification.")
    languages = None

channel_id_col = first_existing_column(channels, ["channel_id"])
video_channel_id_col = first_existing_column(videos, ["channel_id"])
video_id_col = first_existing_column(videos, ["video_id", "id"])
channel_name_col = first_existing_column(channels, ["channel_name", "title", "name", "display_name"])
channel_desc_col = first_existing_column(channels, ["channel_description", "description", "about", "bio", "channel_about", "profile_description"])
video_title_col = first_existing_column(videos, ["video_title", "title", "name"])
video_desc_col = first_existing_column(videos, ["description", "video_description", "text", "body"])

if not channel_id_col or not video_channel_id_col:
    raise ValueError("Could not identify channel_id in channels and videos tables.")
if not channel_name_col:
    raise ValueError("Could not identify channel name column.")
if not video_title_col and not video_desc_col:
    raise ValueError("Could not identify video title or description columns.")

print("channel_name_col:", channel_name_col)
print("channel_desc_col:", channel_desc_col)
print("video_title_col:", video_title_col)
print("video_desc_col:", video_desc_col)
print("label source/col:", EXISTING_LABEL_SOURCE, EXISTING_LABEL_COL)

# COMMAND ----------
# Existing/reference labels: normalize to the YouTube taxonomy where possible.
# These labels are a benchmark for the bake-off, not necessarily expert-coded ground truth.
if EXISTING_LABEL_SOURCE == "videos":
    if EXISTING_LABEL_COL not in videos.columns:
        raise ValueError(f"existing_label_col `{EXISTING_LABEL_COL}` not found in videos table.")
    raw_reference_labels = normalize_reference_label_frame(
        videos,
        channel_col=video_channel_id_col,
        label_col=EXISTING_LABEL_COL,
        source_name=f"videos.{EXISTING_LABEL_COL}",
    )
    reference_channel_labels = aggregate_reference_labels(
        raw_reference_labels,
        min_count=MIN_REFERENCE_LABELED_VIDEOS,
        min_fraction=MIN_REFERENCE_AGREEMENT_FRACTION,
    )
elif EXISTING_LABEL_SOURCE == "channels":
    if EXISTING_LABEL_COL not in channels.columns:
        raise ValueError(f"existing_label_col `{EXISTING_LABEL_COL}` not found in channels table.")
    raw_reference_labels = normalize_reference_label_frame(
        channels,
        channel_col=channel_id_col,
        label_col=EXISTING_LABEL_COL,
        source_name=f"channels.{EXISTING_LABEL_COL}",
    )
    reference_channel_labels = aggregate_reference_labels(raw_reference_labels, min_count=1, min_fraction=0.0)
elif EXISTING_LABEL_SOURCE == "expert_table":
    if not EXPERT_LABEL_TABLE:
        raise ValueError("existing_label_source=expert_table requires expert_label_table to be set.")
    expert_df = spark.table(table_ref(EXPERT_LABEL_TABLE))
    if EXPERT_LABEL_CHANNEL_ID_COL not in expert_df.columns:
        raise ValueError(f"expert_label_channel_id_col `{EXPERT_LABEL_CHANNEL_ID_COL}` not found in {EXPERT_LABEL_TABLE}.")
    if EXPERT_LABEL_CATEGORY_COL not in expert_df.columns:
        raise ValueError(f"expert_label_category_col `{EXPERT_LABEL_CATEGORY_COL}` not found in {EXPERT_LABEL_TABLE}.")
    raw_reference_labels = normalize_reference_label_frame(
        expert_df,
        channel_col=EXPERT_LABEL_CHANNEL_ID_COL,
        label_col=EXPERT_LABEL_CATEGORY_COL,
        source_name=EXPERT_LABEL_TABLE,
    )
    reference_channel_labels = aggregate_reference_labels(raw_reference_labels, min_count=1, min_fraction=0.0)
else:
    raise ValueError("existing_label_source must be videos, channels, or expert_table")

print("Reference category coverage after normalization and channel-level thresholds")
display(
    reference_channel_labels
    .groupBy("reference_label_source", "reference_category_id", "reference_category_name")
    .agg(
        F.count("*").alias("n_channels"),
        F.avg("reference_label_vote_count").alias("mean_reference_label_vote_count"),
        F.avg("reference_label_agreement_fraction").alias("mean_reference_label_agreement_fraction"),
    )
    .orderBy(F.desc("n_channels"))
)

# COMMAND ----------
# Select videos per channel.
rank_candidates = [
    "published_at", "publish_time", "published_time", "upload_date", "created_time", "created_at",
    "first_capture_time", "ingestion_timestamp", "capture_date",
]
if VIDEO_RANK_COLUMN:
    if VIDEO_RANK_COLUMN not in videos.columns:
        raise ValueError(f"video_rank_column `{VIDEO_RANK_COLUMN}` not present in videos table")
    rank_col = VIDEO_RANK_COLUMN
else:
    rank_col = first_existing_column(videos, rank_candidates)

video_base = videos.select(
    F.col(video_channel_id_col).cast("string").alias("channel_id"),
    F.col(video_id_col).cast("string").alias("video_id") if video_id_col else F.lit(None).cast("string").alias("video_id"),
    F.col(video_title_col).cast("string").alias("video_title") if video_title_col else F.lit("").alias("video_title"),
    F.col(video_desc_col).cast("string").alias("video_description") if video_desc_col else F.lit("").alias("video_description"),
    F.col(rank_col).alias("_video_rank_value") if rank_col else F.lit(None).alias("_video_rank_value"),
)

if VIDEOS_PER_CHANNEL > 0:
    if rank_col:
        w = Window.partitionBy("channel_id").orderBy(F.col("_video_rank_value").desc_nulls_last(), F.col("video_id").asc_nulls_last())
    elif CAP_VIDEOS_WITHOUT_RANK_COLUMN and video_id_col:
        w = Window.partitionBy("channel_id").orderBy(F.xxhash64(F.col("video_id")).asc())
    else:
        w = None
    if w is not None:
        video_base = video_base.withColumn("_video_rank", F.row_number().over(w)).where(F.col("_video_rank") <= VIDEOS_PER_CHANNEL)
    else:
        video_base = video_base.withColumn("_video_rank", F.lit(1))
else:
    video_base = video_base.withColumn("_video_rank", F.lit(1))

video_lines = (
    video_base
    .withColumn("video_title_clean", F.substring(F.regexp_replace(F.coalesce(F.col("video_title"), F.lit("")), r"[\r\n\t]+", " "), 1, 300))
    .withColumn("video_desc_clean", F.substring(F.regexp_replace(F.coalesce(F.col("video_description"), F.lit("")), r"[\r\n\t]+", " "), 1, PROMPT_MAX_VIDEO_DESCRIPTION_CHARS))
    .withColumn(
        "video_line",
        F.concat(
            F.lit("["), F.col("_video_rank").cast("string"), F.lit("] Title: "), F.col("video_title_clean"),
            F.when(F.length(F.col("video_desc_clean")) > 0, F.concat(F.lit(" | Description: "), F.col("video_desc_clean"))).otherwise(F.lit("")),
        )
    )
    .groupBy("channel_id")
    .agg(
        F.array_sort(F.collect_list(F.struct(F.col("_video_rank").alias("rank"), F.col("video_line").alias("line")))).alias("video_lines_struct"),
        F.count("*").alias("n_videos_in_prompt"),
    )
    .withColumn("recent_videos_text", F.expr("array_join(transform(video_lines_struct, x -> x.line), '\\n')"))
    .drop("video_lines_struct")
)

# COMMAND ----------
channel_base = channels.select(
    F.col(channel_id_col).cast("string").alias("channel_id"),
    F.col(channel_name_col).cast("string").alias("channel_name"),
    F.substring(F.regexp_replace(F.col(channel_desc_col).cast("string"), r"[\r\n\t]+", " "), 1, PROMPT_MAX_CHANNEL_DESCRIPTION_CHARS).alias("channel_description") if channel_desc_col else F.lit("").alias("channel_description"),
)

if languages is not None and "channel_id" in languages.columns:
    lang_cols = ["channel_id"]
    for c in ["primary_language_label", "primary_language_iso639_3", "primary_language_confidence", "language_status"]:
        if c in languages.columns:
            lang_cols.append(c)
    lang_df = languages.select(*lang_cols)
else:
    lang_df = spark.createDataFrame([], StructType([StructField("channel_id", StringType(), True)]))

prompt_input = (
    channel_base
    .join(video_lines, on="channel_id", how="left")
    .join(reference_channel_labels, on="channel_id", how="left")
    .join(lang_df, on="channel_id", how="left")
    .withColumn("primary_language_label", F.coalesce(F.col("primary_language_label"), F.lit("unknown")))
    .withColumn("primary_language_iso639_3", F.coalesce(F.col("primary_language_iso639_3"), F.lit("unknown")))
    .withColumn("n_videos_in_prompt", F.coalesce(F.col("n_videos_in_prompt"), F.lit(0)))
    .withColumn("recent_videos_text", F.coalesce(F.col("recent_videos_text"), F.lit("")))
)

# Filter by run mode.
if RUN_MODE == "labeled_validation":
    candidates = prompt_input.where(F.col("reference_category_id").isNotNull())
    stratum_cols = ["primary_language_iso639_3", "reference_category_id"]
    n_per_stratum = N_PER_LANGUAGE_CATEGORY
elif RUN_MODE == "unlabeled_pilot":
    candidates = prompt_input.where(F.col("reference_category_id").isNull())
    stratum_cols = ["primary_language_iso639_3"]
    n_per_stratum = N_PER_LANGUAGE_UNLABELED
elif RUN_MODE == "full_unlabeled":
    candidates = prompt_input.where(F.col("reference_category_id").isNull())
    stratum_cols = []
    n_per_stratum = None
else:
    candidates = prompt_input
    stratum_cols = []
    n_per_stratum = None

if stratum_cols:
    w = Window.partitionBy(*stratum_cols).orderBy(stable_hash_order_col("channel_id"))
    sampled = candidates.withColumn("_sample_rank", F.row_number().over(w)).where(F.col("_sample_rank") <= n_per_stratum)
else:
    sampled = candidates.withColumn("_sample_rank", F.lit(1))

if MAX_TOTAL_CHANNELS > 0 and RUN_MODE != "full_unlabeled":
    sampled = sampled.orderBy(stable_hash_order_col("channel_id")).limit(MAX_TOTAL_CHANNELS)

# Build prompt text without leaking reference category.
prompt_prefix = F.lit(
    "Classify the following YouTube channel into exactly one allowed category. "
    "Return strict JSON only.\n\n"
)

sampled = (
    sampled
    .withColumn(
        "prompt_user",
        F.substring(
            F.concat(
                prompt_prefix,
                F.lit("Channel name: "), F.coalesce(F.col("channel_name"), F.lit("")), F.lit("\n"),
                F.lit("Detected language: "), F.coalesce(F.col("primary_language_label"), F.lit("unknown")),
                F.lit(" ("), F.coalesce(F.col("primary_language_iso639_3"), F.lit("unknown")), F.lit(")\n"),
                F.lit("Channel description: "), F.coalesce(F.col("channel_description"), F.lit("")), F.lit("\n\n"),
                F.lit("Recent videos:\n"), F.coalesce(F.col("recent_videos_text"), F.lit("")), F.lit("\n"),
            ),
            1,
            PROMPT_MAX_CHARS,
        )
    )
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("run_mode", F.lit(RUN_MODE))
    .withColumn("prompt_version", F.lit("yt_category_v1"))
    .withColumn("created_at", F.current_timestamp())
)

(
    sampled
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(prompt_inputs_full)
)

print("Wrote prompt inputs to", prompt_inputs_full)
display(spark.table(prompt_inputs_full).groupBy("run_mode", "primary_language_iso639_3", "reference_category_id", "reference_category_name").count().orderBy(F.desc("count")).limit(100))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Generate provider-specific batch request files
# MAGIC
# MAGIC The request table maps every provider/model/custom_id to `channel_id`, so returned batch results can be joined back without relying on row order.

# COMMAND ----------
models_df = spark.createDataFrame(
    [(m["provider"].lower(), m["model"], m.get("tier", "unspecified")) for m in MODELS],
    ["provider", "model", "model_tier"],
)

prompt_inputs = spark.table(prompt_inputs_full)
requests = (
    prompt_inputs
    .crossJoin(models_df)
    .withColumn(
        "request_id",
        F.concat(F.lit("yc_"), F.substring(F.sha2(F.concat_ws("||", F.col("run_id"), F.col("provider"), F.col("model"), F.col("channel_id")), 256), 1, 61)),
    )
    .withColumn("system_prompt", F.lit(SYSTEM_PROMPT))
    .withColumn("temperature", F.lit(float(TEMPERATURE)).cast("double") if TEMPERATURE is not None else F.lit(None).cast("double"))
    .withColumn("max_output_tokens", F.lit(int(MAX_OUTPUT_TOKENS)))
)

def _is_openai_reasoning_or_gpt5_model(model: Optional[str]) -> bool:
    m = (model or "").lower()
    return m.startswith("gpt-5") or m.startswith("o1") or m.startswith("o3") or m.startswith("o4") or m.startswith("o-")


def _openai_uses_responses_api(model: Optional[str]) -> bool:
    if OPENAI_ENDPOINT_MODE == "responses":
        return True
    if OPENAI_ENDPOINT_MODE == "chat_completions":
        return False
    return _is_openai_reasoning_or_gpt5_model(model)


def _openai_batch_endpoint_for_model(model: Optional[str]) -> str:
    return "/v1/responses" if _openai_uses_responses_api(model) else "/v1/chat/completions"


@F.udf(StringType())
def make_batch_line(provider: str, model: str, request_id: str, system_prompt: str, user_prompt: str, temperature: Optional[float], max_output_tokens: int) -> str:
    provider = (provider or "").lower()
    temp = None if temperature is None else float(temperature)
    max_out = int(max_output_tokens or MAX_OUTPUT_TOKENS)

    if provider == "openai":
        if _openai_uses_responses_api(model):
            body = {
                "model": model,
                "instructions": system_prompt,
                "input": user_prompt,
                "max_output_tokens": max_out,
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "youtube_category_prediction",
                        "schema": CATEGORY_RESPONSE_JSON_SCHEMA,
                        "strict": True,
                    },
                    "verbosity": "low",
                },
            }
            if OPENAI_REASONING_EFFORT:
                body["reasoning"] = {"effort": OPENAI_REASONING_EFFORT}
            # Leave temperature omitted by default for GPT-5/reasoning models.
            if temp is not None:
                body["temperature"] = temp
            obj = {"custom_id": request_id, "method": "POST", "url": "/v1/responses", "body": body}
        else:
            body = {
                "model": model,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            }
            token_field = "max_completion_tokens" if _is_openai_reasoning_or_gpt5_model(model) else "max_tokens"
            body[token_field] = max_out
            if temp is not None:
                body["temperature"] = temp
            obj = {"custom_id": request_id, "method": "POST", "url": "/v1/chat/completions", "body": body}

    elif provider == "anthropic":
        params = {
            "model": model,
            "max_tokens": max_out,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        if temp is not None:
            params["temperature"] = temp
        obj = {"custom_id": request_id, "params": params}

    elif provider == "gemini":
        generation_config = {
            "max_output_tokens": max_out,
            "response_format": {
                "text": {
                    "mime_type": "application/json",
                    "schema": CATEGORY_RESPONSE_JSON_SCHEMA,
                }
            },
        }
        if temp is not None:
            generation_config["temperature"] = temp
        if GEMINI_THINKING_LEVEL:
            generation_config["thinking_config"] = {"thinking_level": GEMINI_THINKING_LEVEL}
        obj = {
            "key": request_id,
            "request": {
                "system_instruction": {"parts": [{"text": system_prompt}]},
                "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                "generation_config": generation_config,
            },
        }
    else:
        raise ValueError(f"Unsupported provider: {provider}")
    return json.dumps(obj, ensure_ascii=False)

requests = (
    requests
    .withColumn(
        "batch_line",
        make_batch_line(
            F.col("provider"),
            F.col("model"),
            F.col("request_id"),
            F.col("system_prompt"),
            F.col("prompt_user"),
            F.col("temperature"),
            F.col("max_output_tokens"),
        ),
    )
)

# Chunk separately by provider/model so files respect provider batch limits and are easier to submit.
rw = Window.partitionBy("provider", "model").orderBy("request_id")
requests = (
    requests
    .withColumn("_request_n", F.row_number().over(rw))
    .withColumn("chunk_id", F.floor((F.col("_request_n") - F.lit(1)) / F.lit(MAX_REQUESTS_PER_FILE)).cast("int"))
    .drop("_request_n")
)

(
    requests
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(request_full)
)

print("Wrote request table to", request_full)
display(spark.table(request_full).groupBy("provider", "model", "chunk_id").count().orderBy("provider", "model", "chunk_id"))

# COMMAND ----------
# Write JSONL files to driver-visible DBFS path.
os.makedirs(BATCH_OUTPUT_DIR, exist_ok=True)
run_dir = os.path.join(BATCH_OUTPUT_DIR, RUN_ID)
os.makedirs(run_dir, exist_ok=True)

request_groups = (
    spark.table(request_full)
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
    model_dir = safe_model_dir(model)
    provider_dir = os.path.join(run_dir, provider, model_dir)
    os.makedirs(provider_dir, exist_ok=True)
    local_path = os.path.join(provider_dir, f"chunk_{chunk_id:05d}.jsonl")

    subset = (
        spark.table(request_full)
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
    print(f"Wrote {n_lines:,} requests: {local_path} ({n_bytes:,} bytes)")

batch_files_df = spark.createDataFrame(
    batch_file_records,
    ["run_id", "provider", "model", "model_tier", "chunk_id", "local_jsonl_path", "n_requests", "n_bytes", "created_at_utc"],
)
(
    batch_files_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(batch_files_full)
)

print("Wrote batch file registry to", batch_files_full)
display(spark.table(batch_files_full).orderBy("provider", "model", "chunk_id"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Optional: submit batch jobs
# MAGIC
# MAGIC Leave `submit_batches=false` if your colleague will submit batch files externally. If enabled, this cell reads API keys from Databricks Secrets.
# MAGIC
# MAGIC Gemini submission uses the native Gemini Batch API JSONL path. Test one small Gemini batch before running a large job.

# COMMAND ----------
def get_secret(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


def submit_openai_batch(local_jsonl_path: str, model: str) -> Dict[str, Any]:
    from openai import OpenAI
    client = OpenAI(api_key=get_secret(SECRET_SCOPE, OPENAI_SECRET_KEY))
    with open(local_jsonl_path, "rb") as f:
        uploaded = client.files.create(file=f, purpose="batch")
    batch = client.batches.create(
        input_file_id=uploaded.id,
        endpoint=_openai_batch_endpoint_for_model(model),
        completion_window="24h",
        metadata={"run_id": RUN_ID, "task": "youtube_category", "model": model},
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
        config=types.UploadFileConfig(
            display_name=f"{RUN_ID}_{safe_model_dir(model)}",
            mime_type="jsonl",
        ),
    )
    batch = client.batches.create(
        model=model,
        src=uploaded_file.name,
        config={"display_name": f"{RUN_ID}_{safe_model_dir(model)}"},
    )
    return {
        "provider_file_id": getattr(uploaded_file, "name", None),
        "provider_batch_id": getattr(batch, "name", None),
        "provider_status": getattr(getattr(batch, "state", None), "name", None) or getattr(batch, "state", None),
    }


if SUBMIT_BATCHES:
    job_records = []
    for row in spark.table(batch_files_full).orderBy("provider", "model", "chunk_id").collect():
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
            else:
                raise ValueError(f"Unsupported provider {provider}")
            status = "submitted"
            error = None
        except Exception as e:
            result = {"provider_file_id": None, "provider_batch_id": None, "provider_status": None}
            status = "error"
            error = repr(e)[:2000]
        job_records.append((RUN_ID, provider, model, int(row["chunk_id"]), path, result.get("provider_file_id"), result.get("provider_batch_id"), result.get("provider_status"), status, error, datetime.utcnow().isoformat()))
        print(provider, model, row["chunk_id"], status, result, error)

    jobs_df = spark.createDataFrame(
        job_records,
        ["run_id", "provider", "model", "chunk_id", "local_jsonl_path", "provider_file_id", "provider_batch_id", "provider_status", "submission_status", "submission_error", "submitted_at_utc"],
    )
    (
        jobs_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(batch_jobs_full)
    )
    display(jobs_df)
else:
    print("Batch submission skipped. Set submit_batches=true to submit OpenAI/Anthropic/Gemini batches from this notebook.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Import and parse provider result files
# MAGIC
# MAGIC Put downloaded result JSONL files under `results_input_dir`. The parser is tolerant of OpenAI, Anthropic, and Gemini-like result shapes. It joins results back to `yt_category_llm_requests` using `custom_id`/`key`.

# COMMAND ----------
parse_schema = StructType([
    StructField("request_id", StringType(), True),
    StructField("provider_result_model", StringType(), True),
    StructField("raw_text", StringType(), True),
    StructField("result_status", StringType(), True),
    StructField("input_tokens", IntegerType(), True),
    StructField("output_tokens", IntegerType(), True),
    StructField("parse_error", StringType(), True),
])


def _dig(obj: Any, path: List[Any], default=None):
    cur = obj
    for p in path:
        try:
            if isinstance(p, int):
                cur = cur[p]
            else:
                cur = cur[p]
        except Exception:
            return default
    return cur


def _collect_openai_response_text(body: Dict[str, Any]) -> Optional[str]:
    if not isinstance(body, dict):
        return None
    if body.get("output_text"):
        return body.get("output_text")
    chat_text = _dig(body, ["choices", 0, "message", "content"])
    if chat_text:
        return chat_text
    chunks = []
    for item in body.get("output", []) or []:
        for part in item.get("content", []) or []:
            if isinstance(part, dict) and part.get("type") in {"output_text", "text"} and part.get("text"):
                chunks.append(part.get("text"))
    return "\n".join(chunks) if chunks else None


def extract_provider_text(line: str) -> Dict[str, Any]:
    try:
        obj = json.loads(line)
    except Exception as e:
        return {"request_id": None, "provider_result_model": None, "raw_text": None, "result_status": "json_load_error", "input_tokens": None, "output_tokens": None, "parse_error": repr(e)[:500]}

    request_id = obj.get("custom_id") or obj.get("key") or obj.get("id")
    text = None
    model = None
    status = None
    input_tokens = None
    output_tokens = None

    # OpenAI Batch: supports both Responses API and Chat Completions result bodies.
    body = _dig(obj, ["response", "body"])
    if body:
        status = str(_dig(obj, ["response", "status_code"], body.get("status", "succeeded")))
        model = body.get("model")
        text = _collect_openai_response_text(body)
        usage = body.get("usage", {}) or {}
        input_tokens = usage.get("input_tokens") or usage.get("prompt_tokens")
        output_tokens = usage.get("output_tokens") or usage.get("completion_tokens")
        if text is None and obj.get("error"):
            status = "error"

    # Anthropic Message Batches shape.
    if text is None and obj.get("result"):
        r = obj.get("result", {})
        status = r.get("type")
        message = r.get("message", {}) if isinstance(r, dict) else {}
        model = message.get("model")
        content = message.get("content", [])
        if content and isinstance(content, list):
            text = "\n".join([c.get("text", "") for c in content if isinstance(c, dict) and c.get("type") == "text"])
        usage = message.get("usage", {})
        input_tokens = usage.get("input_tokens")
        output_tokens = usage.get("output_tokens")

    # Native Gemini Batch shape: tolerate several possible nesting conventions.
    if text is None:
        request_id = request_id or obj.get("key")
        status = status or obj.get("status") or _dig(obj, ["response", "status"]) or "unknown"
        model = obj.get("model") or obj.get("modelVersion") or _dig(obj, ["response", "modelVersion"])
        text = (
            _dig(obj, ["response", "candidates", 0, "content", "parts", 0, "text"])
            or _dig(obj, ["response", "candidates", 0, "content", 0, "parts", 0, "text"])
            or _dig(obj, ["candidates", 0, "content", "parts", 0, "text"])
            or _dig(obj, ["inlineResponse", "candidates", 0, "content", "parts", 0, "text"])
            or _dig(obj, ["response", "text"])
        )
        input_tokens = _dig(obj, ["response", "usageMetadata", "promptTokenCount"]) or _dig(obj, ["usageMetadata", "promptTokenCount"])
        output_tokens = _dig(obj, ["response", "usageMetadata", "candidatesTokenCount"]) or _dig(obj, ["usageMetadata", "candidatesTokenCount"])

    if text is None:
        err = obj.get("error") or _dig(obj, ["response", "error"]) or _dig(obj, ["result", "error"])
        return {"request_id": request_id, "provider_result_model": model, "raw_text": None, "result_status": status, "input_tokens": input_tokens, "output_tokens": output_tokens, "parse_error": (json.dumps(err)[:500] if err else "could_not_extract_text")}

    return {"request_id": request_id, "provider_result_model": model, "raw_text": text, "result_status": status or "succeeded", "input_tokens": input_tokens, "output_tokens": output_tokens, "parse_error": None}


@F.udf(parse_schema)
def extract_provider_text_udf(line: str):
    d = extract_provider_text(line)
    return tuple(d.get(field.name) for field in parse_schema.fields)


prediction_json_schema = StructType([
    StructField("category_id", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("ambiguous", BooleanType(), True),
    StructField("rationale_short", StringType(), True),
    StructField("prediction_parse_error", StringType(), True),
])


def extract_first_json_object(text: Optional[str]) -> Optional[Dict[str, Any]]:
    if text is None:
        return None
    text = text.strip()
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    decoder = json.JSONDecoder()
    # Scan for the first valid JSON object rather than taking first "{" through last "}".
    for idx, ch in enumerate(text):
        if ch != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[idx:])
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    return None


def normalize_category_prediction(text: Optional[str]) -> Dict[str, Any]:
    obj = extract_first_json_object(text)
    if not obj:
        return {"category_id": None, "category_name": None, "confidence": None, "ambiguous": None, "rationale_short": None, "prediction_parse_error": "invalid_or_missing_json"}

    cid = str(obj.get("category_id", "")).strip()
    cname = str(obj.get("category_name", "")).strip()
    conf_raw = obj.get("confidence")
    ambiguous_raw = obj.get("ambiguous")
    rationale = obj.get("rationale_short")

    # Fix name-only outputs.
    if cid not in VALID_CATEGORY_IDS and cname:
        cid = CATEGORY_ID_BY_NAME.get(cname.lower(), cid)
    if cid in VALID_CATEGORY_IDS:
        cname = CATEGORY_NAME_BY_ID[cid]
    else:
        return {"category_id": None, "category_name": cname or None, "confidence": None, "ambiguous": None, "rationale_short": str(rationale)[:500] if rationale else None, "prediction_parse_error": f"invalid_category_id:{cid}"}

    try:
        conf = float(conf_raw) if conf_raw is not None else None
        if conf is not None:
            conf = max(0.0, min(1.0, conf))
    except Exception:
        conf = None

    if isinstance(ambiguous_raw, bool):
        ambiguous = ambiguous_raw
    elif isinstance(ambiguous_raw, str):
        ambiguous = ambiguous_raw.strip().lower() in {"true", "1", "yes", "y"}
    else:
        ambiguous = None

    return {"category_id": cid, "category_name": cname, "confidence": conf, "ambiguous": ambiguous, "rationale_short": str(rationale)[:500] if rationale else None, "prediction_parse_error": None}


@F.udf(prediction_json_schema)
def normalize_category_prediction_udf(raw_text: str):
    d = normalize_category_prediction(raw_text)
    return tuple(d.get(field.name) for field in prediction_json_schema.fields)

# COMMAND ----------
if IMPORT_RESULTS:
    if not os.path.exists(RESULTS_INPUT_DIR):
        raise FileNotFoundError(f"results_input_dir does not exist: {RESULTS_INPUT_DIR}")
    print("Importing result JSONL files from", RESULTS_INPUT_DIR)
    result_lines = spark.read.text(RESULTS_INPUT_DIR)
    raw_results = (
        result_lines
        .withColumn("parsed", extract_provider_text_udf(F.col("value")))
        .select("value", "parsed.*")
        .withColumn("run_id", F.lit(RUN_ID))
        .withColumn("imported_at", F.current_timestamp())
    )
    (
        raw_results
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(raw_results_full)
    )
    print("Wrote raw results to", raw_results_full)
else:
    print("Result import skipped. Set import_results=true after placing provider result JSONL files in results_input_dir.")

# COMMAND ----------
# Build parsed prediction table if raw results exist.
try:
    raw_results_loaded = spark.table(raw_results_full).where(F.col("run_id") == RUN_ID)
    have_results = raw_results_loaded.limit(1).count() > 0
except Exception:
    have_results = False

if have_results:
    req_map = spark.table(request_full).select(
        "run_id", "request_id", "provider", "model", "model_tier", "channel_id", "reference_category_id", "reference_category_name",
        "reference_raw_label", "reference_label_source", "reference_label_vote_count", "reference_total_label_count",
        "reference_label_agreement_fraction", "primary_language_iso639_3", "primary_language_label", "n_videos_in_prompt",
    )
    parsed_predictions = (
        raw_results_loaded
        .withColumn("pred", normalize_category_prediction_udf(F.col("raw_text")))
        .select("run_id", "request_id", "provider_result_model", "result_status", "input_tokens", "output_tokens", "raw_text", "parse_error", "pred.*", "imported_at")
        .join(req_map, on=["run_id", "request_id"], how="left")
        .withColumn("correct_vs_reference", F.when(F.col("reference_category_id").isNotNull() & F.col("category_id").isNotNull(), F.col("reference_category_id") == F.col("category_id")))
        .withColumn("prediction_timestamp", F.current_timestamp())
    )
    (
        parsed_predictions
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(parsed_predictions_full)
    )
    print("Wrote parsed predictions to", parsed_predictions_full)
    display(parsed_predictions.groupBy("provider", "model", "result_status", "parse_error", "prediction_parse_error").count().orderBy(F.desc("count")))
else:
    print("No raw results found for this RUN_ID yet; skipping parsed prediction/evaluation tables.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Evaluation against labeled data and inter-model agreement

# COMMAND ----------
def write_evaluation_tables() -> None:
    preds = spark.table(parsed_predictions_full).where(F.col("run_id") == RUN_ID)
    labeled = preds.where(F.col("reference_category_id").isNotNull() & F.col("category_id").isNotNull())
    if labeled.limit(1).count() == 0:
        print("No labeled parsed predictions available for evaluation.")
        return

    # Overall accuracy and coverage.
    overall = (
        preds
        .groupBy("run_id", "provider", "model", "model_tier")
        .agg(
            F.count("*").alias("n_results"),
            F.sum(F.when(F.col("category_id").isNotNull(), 1).otherwise(0)).alias("n_valid_predictions"),
            F.sum(F.when(F.col("reference_category_id").isNotNull(), 1).otherwise(0)).alias("n_with_reference"),
            F.avg(F.when(F.col("reference_category_id").isNotNull() & F.col("category_id").isNotNull(), F.col("correct_vs_reference").cast("double"))).alias("accuracy_vs_reference"),
            F.avg("confidence").alias("mean_reported_confidence"),
            F.sum(F.when(F.col("ambiguous") == True, 1).otherwise(0)).alias("n_ambiguous"),
            F.sum(F.when(F.col("parse_error").isNotNull() | F.col("prediction_parse_error").isNotNull(), 1).otherwise(0)).alias("n_parse_errors"),
        )
    )

    # Per-class precision/recall/F1. Use a full model x class grid so missed classes count as F1=0.
    model_keys = preds.select("run_id", "provider", "model").distinct()
    class_df = spark.createDataFrame([(cid,) for cid in CATEGORY_IDS_SORTED], ["category_id"])
    full_grid = model_keys.crossJoin(class_df)
    tp = labeled.where(F.col("category_id") == F.col("reference_category_id")).groupBy("run_id", "provider", "model", "category_id").agg(F.count("*").alias("tp"))
    pred_counts = labeled.groupBy("run_id", "provider", "model", "category_id").agg(F.count("*").alias("pred_n"))
    reference_counts = labeled.groupBy("run_id", "provider", "model", F.col("reference_category_id").alias("category_id")).agg(F.count("*").alias("reference_n"))
    per_class = (
        full_grid
        .join(pred_counts, on=["run_id", "provider", "model", "category_id"], how="left")
        .join(reference_counts, on=["run_id", "provider", "model", "category_id"], how="left")
        .join(tp, on=["run_id", "provider", "model", "category_id"], how="left")
        .withColumn("tp", F.coalesce(F.col("tp"), F.lit(0)))
        .withColumn("pred_n", F.coalesce(F.col("pred_n"), F.lit(0)))
        .withColumn("reference_n", F.coalesce(F.col("reference_n"), F.lit(0)))
        .withColumn("precision", F.when(F.col("pred_n") > 0, F.col("tp") / F.col("pred_n")).otherwise(F.lit(0.0)))
        .withColumn("recall", F.when(F.col("reference_n") > 0, F.col("tp") / F.col("reference_n")).otherwise(F.lit(0.0)))
        .withColumn("f1", F.when((F.col("precision") + F.col("recall")) > 0, 2 * F.col("precision") * F.col("recall") / (F.col("precision") + F.col("recall"))).otherwise(F.lit(0.0)))
    )
    macro = per_class.groupBy("run_id", "provider", "model").agg(F.avg("f1").alias("macro_f1"), F.avg("precision").alias("macro_precision"), F.avg("recall").alias("macro_recall"))

    # By language.
    by_language = (
        labeled
        .groupBy("run_id", "provider", "model", "model_tier", "primary_language_iso639_3", "primary_language_label")
        .agg(
            F.count("*").alias("n_labeled"),
            F.avg(F.col("correct_vs_reference").cast("double")).alias("accuracy_vs_reference"),
            F.avg("confidence").alias("mean_reported_confidence"),
        )
    )

    eval_metrics = (
        overall
        .join(macro, on=["run_id", "provider", "model"], how="left")
        .withColumn("metric_level", F.lit("overall"))
        .withColumn("primary_language_iso639_3", F.lit(None).cast("string"))
        .withColumn("primary_language_label", F.lit(None).cast("string"))
        .select(
            "run_id", "provider", "model", "model_tier", "metric_level", "primary_language_iso639_3", "primary_language_label",
            "n_results", "n_valid_predictions", "n_with_reference", "accuracy_vs_reference", "macro_f1", "macro_precision", "macro_recall",
            "mean_reported_confidence", "n_ambiguous", "n_parse_errors",
        )
        .unionByName(
            by_language
            .withColumn("metric_level", F.lit("language"))
            .withColumn("n_results", F.col("n_labeled"))
            .withColumn("n_valid_predictions", F.col("n_labeled"))
            .withColumn("n_with_reference", F.col("n_labeled"))
            .withColumn("macro_f1", F.lit(None).cast("double"))
            .withColumn("macro_precision", F.lit(None).cast("double"))
            .withColumn("macro_recall", F.lit(None).cast("double"))
            .withColumn("n_ambiguous", F.lit(None).cast("long"))
            .withColumn("n_parse_errors", F.lit(None).cast("long"))
            .select(
                "run_id", "provider", "model", "model_tier", "metric_level", "primary_language_iso639_3", "primary_language_label",
                "n_results", "n_valid_predictions", "n_with_reference", "accuracy_vs_reference", "macro_f1", "macro_precision", "macro_recall",
                "mean_reported_confidence", "n_ambiguous", "n_parse_errors",
            )
        )
    )

    (
        eval_metrics
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(eval_metrics_full)
    )

    # Pairwise model agreement on all channels with valid predictions.
    p1 = preds.where(F.col("category_id").isNotNull()).select(
        "channel_id", F.concat_ws("/", "provider", "model").alias("model_a"), F.col("category_id").alias("category_a")
    )
    p2 = preds.where(F.col("category_id").isNotNull()).select(
        "channel_id", F.concat_ws("/", "provider", "model").alias("model_b"), F.col("category_id").alias("category_b")
    )
    agreement = (
        p1.join(p2, on="channel_id", how="inner")
        .where(F.col("model_a") < F.col("model_b"))
        .groupBy("model_a", "model_b")
        .agg(
            F.count("*").alias("n_overlap"),
            F.avg((F.col("category_a") == F.col("category_b")).cast("double")).alias("pairwise_agreement"),
        )
        .withColumn("run_id", F.lit(RUN_ID))
    )
    (
        agreement
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(agreement_full)
    )

    print("Wrote evaluation metrics to", eval_metrics_full)
    print("Wrote model agreement to", agreement_full)
    display(eval_metrics.where(F.col("metric_level") == "overall").orderBy(F.desc("accuracy_vs_reference"), F.desc("macro_f1")))
    display(agreement.orderBy(F.desc("pairwise_agreement")))


try:
    if have_results:
        write_evaluation_tables()
    else:
        print("Evaluation skipped because parsed results are not available yet.")
except Exception as e:
    print("Evaluation failed:", repr(e))
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Consensus labels after bake-off
# MAGIC
# MAGIC Use this only after you have reviewed model accuracy, macro-F1, language-stratified performance, and pairwise agreement.

# COMMAND ----------
try:
    preds = spark.table(parsed_predictions_full).where(F.col("run_id") == RUN_ID).where(F.col("category_id").isNotNull())
    if preds.limit(1).count() > 0:
        consensus_votes = (
            preds
            .groupBy("channel_id", "category_id", "category_name")
            .agg(
                F.count("*").alias("n_model_votes"),
                F.avg("confidence").alias("mean_model_confidence"),
                F.collect_list(F.concat_ws("/", "provider", "model")).alias("voting_models"),
            )
        )
        cw = Window.partitionBy("channel_id").orderBy(F.desc("n_model_votes"), F.desc("mean_model_confidence"), F.asc(F.col("category_id").cast("int")))
        consensus = (
            consensus_votes
            .withColumn("consensus_rank", F.row_number().over(cw))
            .where(F.col("consensus_rank") == 1)
            .withColumn("run_id", F.lit(RUN_ID))
            .withColumn("prediction_timestamp", F.current_timestamp())
        )
        consensus.createOrReplaceTempView("yt_category_consensus_preview")
        print("Consensus preview available as temp view: yt_category_consensus_preview")
        display(consensus.orderBy(F.desc("n_model_votes"), F.desc("mean_model_confidence")).limit(100))
except Exception as e:
    print("Consensus preview skipped:", repr(e))
