# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube LID v3 — LLM adjudication panel (companion to notebook 01)
# MAGIC
# MAGIC **Run order:** run `01_language_openlid_v3_databricks` first (writes the `yt_lid_v3_*` tables),
# MAGIC then run this notebook. This notebook does **not** re-run the fastText models.
# MAGIC
# MAGIC **What it does:** by default, routes the small subset of channels where the two fastText models
# MAGIC *disagree* (plus a tiny blind *audit* sample of the agreement bucket) to a multi-model LLM panel.
# MAGIC For API/secrets validation, set `routing_mode=random_validation` to classify a reproducible random
# MAGIC sample from the notebook 01 comparison table. The panel adjudicates written-metadata language and
# MAGIC writes per-model outputs, a majority verdict, and an all-model agreement matrix.
# MAGIC
# MAGIC **Inputs (from notebook 01):** `yt_lid_v3_channel_model_comparison`, `yt_lid_v3_segments_input`,
# MAGIC optionally `yt_lid_v3_channel_text_features`.
# MAGIC **Outputs:** `yt_lid_v3_llm_panel_requests`, batch JSONL files on DBFS,
# MAGIC `yt_lid_v3_llm_panel_requests_batch_files`, `yt_lid_v3_llm_panel_batch_jobs`,
# MAGIC `yt_lid_v3_llm_panel_raw_results`, `yt_lid_v3_llm_panel_verdicts`.
# MAGIC
# MAGIC **Spec:** the per-channel classifier instructions mirror
# MAGIC `youtube_descriptive/validation/llm_panel_classifier_prompt.md`, adapted for batch (the model judges
# MAGIC from supplied metadata instead of fetching live). See the validation report §10 (P0/D) for routing
# MAGIC scope and reconciliation rules.

# COMMAND ----------
# MAGIC %pip install "openai>=2.0.0" anthropic "google-genai>=1.51.0" pandas pyarrow requests tenacity
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import re
import time
import unicodedata
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Widgets & configuration

# COMMAND ----------
def _create_text_widget(name: str, default: str, label: Optional[str] = None) -> None:
    try:
        dbutils.widgets.text(name, default, label or name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        v = dbutils.widgets.get(name)
        return v if v is not None and v != "" else default
    except Exception:
        return os.environ.get(name.upper(), default)


def _get_bool_widget(name: str, default: bool) -> bool:
    return _get_widget(name, str(default)).strip().lower() in {"1", "true", "t", "yes", "y"}


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


# Source tables (must match notebook 01's output location).
_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("comparison_table", "yt_lid_v3_channel_model_comparison")
_create_text_widget("segments_input_table", "yt_lid_v3_segments_input")
_create_text_widget("channel_text_features_table", "yt_lid_v3_channel_text_features")
_create_text_widget("hindi_indic_audit_table", "yt_lid_v3_hindi_indic_audit_candidates")
_create_text_widget("run_id", "default")
_create_text_widget("source_run_id", "")  # blank = use run_id; set for retry/output-only runs
_create_text_widget("inference_hash_buckets", "4096")

# Output tables.
_create_text_widget("panel_requests_table", "yt_lid_v3_llm_panel_requests")
_create_text_widget("panel_batch_jobs_table", "yt_lid_v3_llm_panel_batch_jobs")
_create_text_widget("panel_raw_results_table", "yt_lid_v3_llm_panel_raw_results")
_create_text_widget("panel_verdicts_table", "yt_lid_v3_llm_panel_verdicts")
_create_text_widget("panel_model_agreement_table", "yt_lid_v3_llm_panel_model_agreement")

# --- Routing controls ---
_create_text_widget("routing_mode", "residual_panel")  # residual_panel | random_validation
_create_text_widget("random_validation_sample_size", "1000")
_create_text_widget("random_validation_seed", "20260610")
# Residual-panel routes. Ignored when routing_mode=random_validation.
# Disagreement buckets (always routed).
_create_text_widget("route_disagreement", "true")
# Unresolved high-risk tail (consensus label NULL). Confident mutual-agreement tails keep their label and
# are NOT routed (see report B5).
_create_text_widget("route_unresolved_tail", "true")
# Targeted shared-bias route (D3): exact English consensus WITH contradicting Indic evidence.
_create_text_widget("route_shared_bias_english_indic", "true")
# Blind audit sample (E3): a small uniform-random slice of the AGREEMENT bucket, to measure accuracy/bias.
_create_text_widget("route_agreement_audit", "true")
_create_text_widget("agreement_audit_fraction", "0.005")
_create_text_widget("agreement_audit_seed", "20260526")
# Skip within-Arabic-family disagreements (taxonomy artifact handled deterministically upstream by B1).
_create_text_widget("exclude_arabic_family_pairs", "true")
_create_text_widget("max_routed_channels", "0")  # 0 = no cap

# Prompt construction.
_create_text_widget("max_video_titles", "12")
_create_text_widget("max_video_descriptions", "4")
_create_text_widget("max_segment_chars", "350")
_create_text_widget("prompt_max_chars", "6000")
_create_text_widget("strip_prompt_boilerplate", "true")
_create_text_widget("dedupe_prompt_segments", "true")

# Models: mix frontier/mid/small providers by default for validation agreement matrices.
DEFAULT_MODELS_JSON = json.dumps([
    {"provider": "openai", "model": "gpt-5.5", "tier": "frontier"},
    {"provider": "openai", "model": "gpt-5.4", "tier": "mid"},
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
_create_text_widget("models_json", DEFAULT_MODELS_JSON)
_create_text_widget("max_output_tokens", "2000")
_create_text_widget("temperature", "")  # blank = provider default
_create_text_widget("openai_endpoint_mode", "auto")
_create_text_widget("openai_reasoning_effort", "minimal")  # blank = omit reasoning controls
_create_text_widget("gemini_thinking_level", "low")  # blank = omit thinking controls
_create_text_widget("deepseek_thinking_type", "disabled")  # disabled | enabled | blank to omit
_create_text_widget("deepseek_reasoning_effort", "")  # high | max; only used with enabled thinking
_create_text_widget("deepseek_max_output_tokens", "600")
_create_text_widget("deepseek_max_workers", "16")
_create_text_widget("deepseek_request_timeout_seconds", "60")
_create_text_widget("deepseek_max_retries", "1")

# Batch I/O.
_create_text_widget("batch_output_dir", "/dbfs/FileStore/youtube_lid_panel_batches")
_create_text_widget("max_requests_per_file", "10000")
_create_text_widget("submit_batches", "false")
_create_text_widget("submit_provider_filter", "")  # blank = all; comma-separated provider names
_create_text_widget("skip_existing_submitted_batches", "true")
_create_text_widget("import_results", "false")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results")
_create_text_widget("panel_majority_mode", "reached_models")  # reached_models | configured_models
_create_text_widget("min_panel_votes_for_majority", "2")
_create_text_widget("panel_majority_vote_basis", "normalized_base_iso")  # normalized_base_iso | raw_base_iso
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("openai_secret_key", "openai-api-key")
_create_text_widget("anthropic_secret_key", "anthropic-api-key")
_create_text_widget("gemini_secret_key", "gemini-api-key")
_create_text_widget("deepseek_secret_key", "deepseek-api-key")

# COMMAND ----------
CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
COMPARISON_TABLE = _get_widget("comparison_table", "yt_lid_v3_channel_model_comparison")
SEGMENTS_INPUT_TABLE = _get_widget("segments_input_table", "yt_lid_v3_segments_input")
CHANNEL_TEXT_FEATURES_TABLE = _get_widget("channel_text_features_table", "yt_lid_v3_channel_text_features")
HINDI_INDIC_AUDIT_TABLE = _get_widget("hindi_indic_audit_table", "yt_lid_v3_hindi_indic_audit_candidates")
RUN_ID = _get_widget("run_id", "default").strip() or "default"
SOURCE_RUN_ID = _get_widget("source_run_id", "").strip() or RUN_ID
INFERENCE_HASH_BUCKETS = _get_int_widget("inference_hash_buckets", 4096)

PANEL_REQUESTS_TABLE = _get_widget("panel_requests_table", "yt_lid_v3_llm_panel_requests")
PANEL_BATCH_JOBS_TABLE = _get_widget("panel_batch_jobs_table", "yt_lid_v3_llm_panel_batch_jobs")
PANEL_RAW_RESULTS_TABLE = _get_widget("panel_raw_results_table", "yt_lid_v3_llm_panel_raw_results")
PANEL_VERDICTS_TABLE = _get_widget("panel_verdicts_table", "yt_lid_v3_llm_panel_verdicts")
PANEL_MODEL_AGREEMENT_TABLE = _get_widget("panel_model_agreement_table", "yt_lid_v3_llm_panel_model_agreement")

ROUTING_MODE = _get_widget("routing_mode", "residual_panel").strip().lower()
RANDOM_VALIDATION_SAMPLE_SIZE = _get_int_widget("random_validation_sample_size", 1000)
RANDOM_VALIDATION_SEED = _get_widget("random_validation_seed", "20260610").strip()
ROUTE_DISAGREEMENT = _get_bool_widget("route_disagreement", True)
ROUTE_UNRESOLVED_TAIL = _get_bool_widget("route_unresolved_tail", True)
ROUTE_SHARED_BIAS = _get_bool_widget("route_shared_bias_english_indic", True)
ROUTE_AGREEMENT_AUDIT = _get_bool_widget("route_agreement_audit", True)
AGREEMENT_AUDIT_FRACTION = _get_float_widget("agreement_audit_fraction", 0.005)
AGREEMENT_AUDIT_SEED = _get_widget("agreement_audit_seed", "20260526")
EXCLUDE_ARABIC_FAMILY_PAIRS = _get_bool_widget("exclude_arabic_family_pairs", True)
MAX_ROUTED_CHANNELS = _get_int_widget("max_routed_channels", 0)

MAX_VIDEO_TITLES = _get_int_widget("max_video_titles", 12)
MAX_VIDEO_DESCRIPTIONS = _get_int_widget("max_video_descriptions", 4)
MAX_SEGMENT_CHARS = _get_int_widget("max_segment_chars", 350)
PROMPT_MAX_CHARS = _get_int_widget("prompt_max_chars", 6000)
STRIP_PROMPT_BOILERPLATE = _get_bool_widget("strip_prompt_boilerplate", True)
DEDUPE_PROMPT_SEGMENTS = _get_bool_widget("dedupe_prompt_segments", True)

MODELS = json.loads(_get_widget("models_json", DEFAULT_MODELS_JSON))
MAX_OUTPUT_TOKENS = _get_int_widget("max_output_tokens", 2000)
TEMPERATURE = _get_optional_float_widget("temperature", None)
OPENAI_ENDPOINT_MODE = _get_widget("openai_endpoint_mode", "auto").strip().lower()
OPENAI_REASONING_EFFORT = _get_widget("openai_reasoning_effort", "minimal").strip()
GEMINI_THINKING_LEVEL = _get_widget("gemini_thinking_level", "low").strip()
DEEPSEEK_THINKING_TYPE = _get_widget("deepseek_thinking_type", "disabled").strip().lower()
DEEPSEEK_REASONING_EFFORT = _get_widget("deepseek_reasoning_effort", "").strip().lower()
DEEPSEEK_MAX_OUTPUT_TOKENS = _get_int_widget("deepseek_max_output_tokens", 600)
DEEPSEEK_MAX_WORKERS = _get_int_widget("deepseek_max_workers", 16)
DEEPSEEK_REQUEST_TIMEOUT_SECONDS = _get_float_widget("deepseek_request_timeout_seconds", 60.0)
DEEPSEEK_MAX_RETRIES = _get_int_widget("deepseek_max_retries", 1)

BATCH_OUTPUT_DIR = _get_widget("batch_output_dir", "/dbfs/FileStore/youtube_lid_panel_batches")
MAX_REQUESTS_PER_FILE = _get_int_widget("max_requests_per_file", 10000)
SUBMIT_BATCHES = _get_bool_widget("submit_batches", False)
SUBMIT_PROVIDER_FILTER_RAW = _get_widget("submit_provider_filter", "").strip().lower()
SUBMIT_PROVIDER_FILTER = {p.strip() for p in SUBMIT_PROVIDER_FILTER_RAW.split(",") if p.strip()}
SKIP_EXISTING_SUBMITTED_BATCHES = _get_bool_widget("skip_existing_submitted_batches", True)
IMPORT_RESULTS = _get_bool_widget("import_results", False)
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results")
PANEL_MAJORITY_MODE = _get_widget("panel_majority_mode", "reached_models").strip().lower()
MIN_PANEL_VOTES_FOR_MAJORITY = _get_int_widget("min_panel_votes_for_majority", 2)
PANEL_MAJORITY_VOTE_BASIS = _get_widget("panel_majority_vote_basis", "normalized_base_iso").strip().lower()
DEFAULT_SECRET_SCOPE = "youtube-llm-keys"
DEFAULT_SECRET_KEYS = {
    "openai": "openai-api-key",
    "anthropic": "anthropic-api-key",
    "gemini": "gemini-api-key",
    "deepseek": "deepseek-api-key",
}


def _normalize_secret_scope(raw: str) -> str:
    value = (raw or "").strip()
    if value in {"", "llm-api-keys"}:
        return DEFAULT_SECRET_SCOPE
    return value


def _normalize_secret_key(provider: str, raw: str) -> str:
    value = (raw or "").strip()
    legacy_default = f"{provider}_api_key"
    if value in {"", legacy_default}:
        return DEFAULT_SECRET_KEYS[provider]
    return value


SECRET_SCOPE = _normalize_secret_scope(_get_widget("secret_scope", DEFAULT_SECRET_SCOPE))
OPENAI_SECRET_KEY = _normalize_secret_key("openai", _get_widget("openai_secret_key", DEFAULT_SECRET_KEYS["openai"]))
ANTHROPIC_SECRET_KEY = _normalize_secret_key("anthropic", _get_widget("anthropic_secret_key", DEFAULT_SECRET_KEYS["anthropic"]))
GEMINI_SECRET_KEY = _normalize_secret_key("gemini", _get_widget("gemini_secret_key", DEFAULT_SECRET_KEYS["gemini"]))
DEEPSEEK_SECRET_KEY = _normalize_secret_key("deepseek", _get_widget("deepseek_secret_key", DEFAULT_SECRET_KEYS["deepseek"]))


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", model or "model")


def spark_path(path: str) -> str:
    path = (path or "").rstrip("/")
    if path.startswith("/dbfs/"):
        return "dbfs:/" + path[len("/dbfs/"):]
    return path


def local_fs_path(path: str) -> str:
    path = (path or "").rstrip("/")
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/"):]
    return path


comparison_full = fqtn(COMPARISON_TABLE)
segments_input_full = fqtn(SEGMENTS_INPUT_TABLE)
channel_text_features_full = fqtn(CHANNEL_TEXT_FEATURES_TABLE)
hindi_indic_audit_full = fqtn(HINDI_INDIC_AUDIT_TABLE)
panel_requests_full = fqtn(PANEL_REQUESTS_TABLE)
panel_batch_jobs_full = fqtn(PANEL_BATCH_JOBS_TABLE)
panel_raw_results_full = fqtn(PANEL_RAW_RESULTS_TABLE)
panel_verdicts_full = fqtn(PANEL_VERDICTS_TABLE)
panel_model_agreement_full = fqtn(PANEL_MODEL_AGREEMENT_TABLE)
panel_batch_files_full = fqtn(PANEL_REQUESTS_TABLE + "_batch_files")

# D4: idempotent, run-scoped writes — re-running the same run_id overwrites only its own partition,
# never the whole table, so prior runs are preserved.
try:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
except Exception:
    pass


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


def _table_partition_columns(table_full: str):
    try:
        row = spark.sql(f"DESCRIBE DETAIL {table_full}").select("partitionColumns").collect()[0]
        return list(row["partitionColumns"] or [])
    except Exception:
        return []


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def write_run_scoped(df, table_full, extra_partitions=None):
    if "run_id" not in df.columns:
        df = df.withColumn("run_id", F.lit(RUN_ID))
    parts = ["run_id"] + list(extra_partitions or [])
    if not _table_exists_full(table_full):
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy(*parts)
            .saveAsTable(table_full)
        )
        return

    actual_partitions = _table_partition_columns(table_full)
    if actual_partitions != parts:
        raise RuntimeError(
            f"{table_full} partition columns are {actual_partitions}, expected {parts}. "
            "Recreate or migrate the table before running a scoped panel overwrite."
        )

    existing = spark.table(table_full)
    if "run_id" not in existing.columns:
        raise RuntimeError(f"{table_full} has no run_id column and cannot be safely overwritten by run scope.")

    existing_cols = set(existing.columns)
    write_cols = set(df.columns)
    missing_write_cols = sorted(write_cols - existing_cols)
    if missing_write_cols:
        print(f"Evolving {table_full} schema with new output columns {missing_write_cols}.")
        (
            df.limit(0)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_full)
        )
        existing = spark.table(table_full)
        existing_cols = set(existing.columns)

    unknown_write_cols = sorted(set(df.columns) - existing_cols)
    if unknown_write_cols:
        raise RuntimeError(f"{table_full} schema did not accept new output columns {unknown_write_cols}.")

    write_df = df
    for field in existing.schema.fields:
        if field.name not in write_df.columns:
            write_df = write_df.withColumn(field.name, F.lit(None).cast(field.dataType))
    write_df = write_df.select(*existing.columns)

    (
        write_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"run_id = {_sql_string(RUN_ID)}")
        .partitionBy(*parts)
        .saveAsTable(table_full)
    )

# Arabic macrolanguage + dialects collapsed to one family for the "exclude taxonomy artifact" filter.
ARABIC_FAMILY_ISO = {"ara", "arb", "ary", "arz", "apc", "ars", "ajp", "aeb", "acm", "acq", "aec", "afb", "ayl", "ayn"}
# South-Asian source language codes used to flag the romanized-Indic shared-bias route (D3).
SOURCE_INDIC_CODES = {"hi", "hi-in", "hin", "ne", "ne-np", "nep", "npi", "bho", "ur", "ur-pk", "pa", "gu", "mr", "bn", "ta", "te", "kn", "ml", "or", "si"}
CANONICAL_BASE_ISO = {
    # Project-level taxonomy aliases surfaced in the 1k LLM validation disagreement audit.
    "zho": "cmn",
    "cmn": "cmn",
    "fil": "fil",
    "tgl": "fil",
    "ori": "ory",
    "ory": "ory",
    "uzn": "uzb",
    "uzb": "uzb",
    "msa": "zsm",
    "zsm": "zsm",
    "nep": "npi",
    "npi": "npi",
}


def canonical_base_iso_expr(col):
    iso = F.lower(F.trim(col.cast("string")))
    iso = F.when(iso.isin("", "null", "none"), F.lit(None).cast("string")).otherwise(iso)
    iso = F.when(iso.isin(*sorted(ARABIC_FAMILY_ISO)), F.lit("ara")).otherwise(iso)
    mapping = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in CANONICAL_BASE_ISO.items()], []))
    return F.coalesce(F.element_at(mapping, iso), iso)


def script_from_label_expr(label_col):
    script = F.element_at(F.split(label_col.cast("string"), "_"), 2)
    return F.when(script.isin("", "null", "none"), F.lit(None).cast("string")).otherwise(script)


def script_family_expr(script_col):
    raw = F.trim(script_col.cast("string"))
    raw_l = F.lower(raw)
    return (
        F.when(raw_l.isin("", "null", "none"), F.lit(None).cast("string"))
        .when(raw_l.isin("hani", "hans", "hant", "han"), F.lit("Hani"))
        .when(raw_l.isin("arab", "arabic"), F.lit("Arab"))
        .when(raw_l.isin("latn", "latin"), F.lit("Latn"))
        .when(raw_l.isin("cyrl", "cyrillic"), F.lit("Cyrl"))
        .when(raw_l.isin("deva", "devanagari"), F.lit("Deva"))
        .when(raw_l.isin("orya", "odia"), F.lit("Orya"))
        .when(raw_l.isin("thai"), F.lit("Thai"))
        .when(raw_l.isin("jpan", "kana"), F.lit("Jpan"))
        .when(raw_l.isin("hang", "korean"), F.lit("Hang"))
        .otherwise(raw)
    )


def normalized_language_label_expr(iso_col, script_col):
    iso = canonical_base_iso_expr(iso_col)
    script = script_family_expr(script_col)
    return (
        F.when(iso.isNull(), F.lit(None).cast("string"))
        .when(script.isNull(), iso)
        .otherwise(F.concat_ws("_", iso, script))
    )

if ROUTING_MODE not in {"residual_panel", "random_validation"}:
    raise ValueError("routing_mode must be residual_panel or random_validation")
if RANDOM_VALIDATION_SAMPLE_SIZE < 1:
    raise ValueError("random_validation_sample_size must be positive")
if DEEPSEEK_THINKING_TYPE not in {"", "enabled", "disabled"}:
    raise ValueError("deepseek_thinking_type must be blank, enabled, or disabled")
if DEEPSEEK_REASONING_EFFORT and DEEPSEEK_REASONING_EFFORT not in {"high", "max"}:
    raise ValueError("deepseek_reasoning_effort must be blank, high, or max")
if DEEPSEEK_REASONING_EFFORT and DEEPSEEK_THINKING_TYPE != "enabled":
    raise ValueError("deepseek_reasoning_effort requires deepseek_thinking_type=enabled")
if DEEPSEEK_MAX_OUTPUT_TOKENS < 1:
    raise ValueError("deepseek_max_output_tokens must be at least 1")
if DEEPSEEK_MAX_WORKERS < 1:
    raise ValueError("deepseek_max_workers must be at least 1")
if DEEPSEEK_REQUEST_TIMEOUT_SECONDS <= 0:
    raise ValueError("deepseek_request_timeout_seconds must be positive")
if DEEPSEEK_MAX_RETRIES < 0:
    raise ValueError("deepseek_max_retries must be non-negative")
if not SUBMIT_PROVIDER_FILTER.issubset({"openai", "anthropic", "gemini", "deepseek"}):
    raise ValueError("submit_provider_filter may only contain openai, anthropic, gemini, and/or deepseek")
if PANEL_MAJORITY_MODE not in {"reached_models", "configured_models"}:
    raise ValueError("panel_majority_mode must be reached_models or configured_models")
if MIN_PANEL_VOTES_FOR_MAJORITY < 1:
    raise ValueError("min_panel_votes_for_majority must be positive")
if PANEL_MAJORITY_VOTE_BASIS not in {"normalized_base_iso", "raw_base_iso"}:
    raise ValueError("panel_majority_vote_basis must be normalized_base_iso or raw_base_iso")

print("Source comparison table:", comparison_full, "| source_run_id:", SOURCE_RUN_ID, "| output run_id:", RUN_ID)
print("Panel models:", ", ".join(f"{m['provider']}:{m['model']}[{m.get('tier', 'unspecified')}]" for m in MODELS))
print("Panel majority vote basis:", PANEL_MAJORITY_VOTE_BASIS)
print("Prompt cleanup: strip_boilerplate=", STRIP_PROMPT_BOILERPLATE, "| dedupe_segments=", DEDUPE_PROMPT_SEGMENTS)
print(
    "DeepSeek direct controls:",
    f"thinking_type={DEEPSEEK_THINKING_TYPE or 'omitted'}",
    f"max_output_tokens={DEEPSEEK_MAX_OUTPUT_TOKENS}",
    f"workers={DEEPSEEK_MAX_WORKERS}",
    f"timeout_seconds={DEEPSEEK_REQUEST_TIMEOUT_SECONDS}",
    f"max_retries={DEEPSEEK_MAX_RETRIES}",
)
print("Routing mode:", ROUTING_MODE)
if ROUTING_MODE == "random_validation":
    print(f"Random validation sample: n={RANDOM_VALIDATION_SAMPLE_SIZE:,}, seed={RANDOM_VALIDATION_SEED}")
else:
    print("Routes -> disagreement:", ROUTE_DISAGREEMENT, "| unresolved_tail:", ROUTE_UNRESOLVED_TAIL,
          "| shared_bias_english_indic:", ROUTE_SHARED_BIAS, "| agreement_audit:", ROUTE_AGREEMENT_AUDIT,
          f"({AGREEMENT_AUDIT_FRACTION:.4f})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. System prompt (batch-adapted classifier spec)
# MAGIC
# MAGIC Mirrors `validation/llm_panel_classifier_prompt.md`, but the model judges from the metadata supplied
# MAGIC in the user prompt rather than fetching live (batch APIs cannot browse).

# COMMAND ----------
SYSTEM_PROMPT = """You are an independent, evidence-driven language classifier for YouTube channels. You are one member of a panel that adjudicates cases where a two-model machine pipeline (OpenLID-v3 + GlotLID) disagrees. Judge ONLY from the channel metadata supplied below; do not assume what the channel "probably" is, and do not consider any other model's guess.

OBJECTIVE: determine the dominant WRITTEN-METADATA language — the language of the channel name, description, and video titles/descriptions provided. This is NOT the spoken language and NOT the creator's nationality. A channel filmed in Hindi can have English-written metadata; classify the WRITING.

LABEL FORMAT: a "<ISO 639-3>_<ISO 15924 script>" tag, e.g. eng_Latn, spa_Latn, hin_Deva, ara_Arab, cmn_Hani, tha_Thai, kor_Hang. Always include the script. If a non-Latin language is written in Latin letters (romanization), label it with _Latn and set is_romanized=true (e.g. romanized Hindi = hin_Latn).

WEIGH the evidence by field, highest first: video_title (2.0), video_description (1.0), channel_description (1.0), channel_name (0.25). A field is decisive only with enough clean letters (>=40 Latin / >=12 non-Latin), but repeated short titles can still be strong evidence. Treat generic provider metadata, release metadata, URLs, social links, and English scaffolding like "Official Video" as weak evidence.

USE SUMMARIES CAREFULLY: FIELD SUMMARY, SEGMENT SCRIPT SUMMARY, TEXT SCRIPT SUMMARY, and LANGUAGE HINTS describe the supplied prompt after cleanup. Use them to notice mixed-script or romanized evidence, but do not classify from a single hint, hashtag, location, artist name, or channel name without supporting natural-language title/description text.

GUARD against known failure modes:
- LATIN-NAME TRAP: do not let an English/Latin channel NAME override video titles that are mostly non-Latin. If titles are mostly Thai/Korean/Arabic/etc., that is the language even when the brand name is Latin.
- ROMANIZED NON-LATIN: detect romanized Hindi/Urdu/Punjabi/Arabic/Bengali/Tamil/Telugu/Malayalam/Bhojpuri/Haryanvi; label the underlying language with _Latn, is_romanized=true; do not default to English when the title phrases are clearly non-English.
- SPARSE CUES: do not let a single channel name, one short non-English item, hashtags, locations, artist names, or topic labels override repeated English natural-language titles/descriptions. Preserve recurring secondary evidence with secondary_language_label/is_mixed_language instead.
- ENGLISH vs CREOLE: standard English is eng_Latn; only use jam_Latn/pcm_Latn with genuine creole grammar/lexis.
- MINORITY OVER-PREDICTION: be conservative with rare Romance/minority tail labels (srd, ast, vec, gug, lim, scn, glg, eus); a few ambiguous Latin words are usually Spanish/Italian/Portuguese/English. Set is_high_risk_tail=true if you do assign one.

NORMALIZE TAXONOMY: report Arabic as the macrolanguage ara_Arab (put a known dialect in dialect_or_variant); use cmn for Chinese/Mandarin rather than zho; use fil_Latn for broad Filipino/Tagalog unless there is a specific reason to report tgl; use ory rather than ori for Odia; use uzb rather than uzn for broad Uzbek; use zsm rather than msa for Standard Malay/Malay; use npi rather than nep for Nepali; distinguish ind vs zsm only with clear evidence.

MIXED LANGUAGE: if a second language recurs across multiple fields, set secondary_language_label, is_mixed_language=true, and list mixed_languages. Do not force a single-language call when the supplied metadata is genuinely bilingual; choose the dominant written metadata language and preserve the recurring secondary language.

ABSTAIN rather than guess: if the supplied metadata has no usable text, status="insufficient_text" and leave labels null. Otherwise status="classified".

Base the judgment ONLY on the supplied text. NEVER invent content. Return ONE compact, minified JSON object
on one line, nothing else. Keep evidence <=160 characters and quote only the shortest decisive text:
{"status":"classified|insufficient_text","primary_language_label":"iso_Script|null","primary_language_iso639_3":"iso|null","primary_language_script":"Script|null","is_romanized":true|false,"dialect_or_variant":"iso|null","is_high_risk_tail":true|false,"secondary_language_label":"iso_Script|null","is_mixed_language":true|false,"mixed_languages":["iso_Script"],"confidence":"high|medium|low","evidence":"<=160 chars"}"""

# Response JSON schema for providers that enforce structured output. Gemini requests JSON MIME type only.
LANG_RESPONSE_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "status": {"type": "string", "enum": ["classified", "insufficient_text"]},
        "primary_language_label": {"type": ["string", "null"]},
        "primary_language_iso639_3": {"type": ["string", "null"]},
        "primary_language_script": {"type": ["string", "null"]},
        "is_romanized": {"type": "boolean"},
        "dialect_or_variant": {"type": ["string", "null"]},
        "is_high_risk_tail": {"type": "boolean"},
        "secondary_language_label": {"type": ["string", "null"]},
        "is_mixed_language": {"type": "boolean"},
        "mixed_languages": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "evidence": {"type": "string", "maxLength": 180},
    },
    "required": ["status", "primary_language_label", "is_romanized", "is_high_risk_tail",
                 "is_mixed_language", "confidence", "evidence"],
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Routing — select residual-panel cases or a random validation sample from notebook 01's output

# COMMAND ----------
cmp_df = spark.table(comparison_full).where(
    (F.col("run_id") == F.lit(SOURCE_RUN_ID)) & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
)

ol_iso = F.col("openlid_primary_language_iso639_3")
gl_iso = F.col("glotlid_primary_language_iso639_3")
both_arabic = ol_iso.isin(*sorted(ARABIC_FAMILY_ISO)) & gl_iso.isin(*sorted(ARABIC_FAMILY_ISO))

DISAGREEMENT_STATUSES = [
    "model_disagreement_needs_review",
    "glotlid_fallback_openlid_low_confidence",
    "openlid_high_confidence_glotlid_missing_or_error",
]
AGREEMENT_STATUSES = [
    "exact_model_agreement",
    "iso_or_script_variant_agreement",
    "cluster_model_agreement",
    "taxonomy_normalized_agreement",
    "high_risk_tail_exact_agreement",
]

route_frames = []

if ROUTING_MODE == "random_validation":
    # Seeded, reproducible sample from the full notebook-01 comparison table. This is intended for
    # API/secrets smoke validation, not adjudication of only the residual-review queue.
    sample_order = F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.lit(RANDOM_VALIDATION_SEED)))
    route_frames.append(
        cmp_df
        .orderBy(sample_order, F.col("channel_id"))
        .limit(RANDOM_VALIDATION_SAMPLE_SIZE)
        .withColumn("route_reason", F.lit("random_validation"))
    )
else:
    if ROUTE_DISAGREEMENT:
        d = cmp_df.where(F.col("consensus_status").isin(*DISAGREEMENT_STATUSES))
        if EXCLUDE_ARABIC_FAMILY_PAIRS:
            d = d.where(~F.coalesce(both_arabic, F.lit(False)))
        route_frames.append(d.withColumn("route_reason", F.lit("disagreement")))

    if ROUTE_UNRESOLVED_TAIL:
        # Unresolved tail only: confident mutual-agreement tails already carry a consensus label (kept final).
        t = cmp_df.where(
            (F.col("consensus_status") == F.lit("high_risk_tail_label_needs_review"))
            & F.col("consensus_language_label").isNull()
        )
        route_frames.append(t.withColumn("route_reason", F.lit("unresolved_tail")))

    if ROUTE_SHARED_BIAS:
        # D3: both models agree on English, but Indic evidence contradicts. Reuse channel_text_features
        # for metadata signals and hindi_indic_audit for source-language evidence when available.
        text_feat_cols = set(spark.table(channel_text_features_full).columns) if _table_exists_full(channel_text_features_full) else set()
        hindi_audit_cols = set(spark.table(hindi_indic_audit_full).columns) if _table_exists_full(hindi_indic_audit_full) else set()
        sig = cmp_df.where(F.col("consensus_language_iso639_3") == F.lit("eng"))
        indic_signal = F.lit(False)
        if text_feat_cols:
            # D4: run-scope the auxiliary join (channel_text_features is per-run partitioned) and dedupe to one
            # row per channel, so we never pull rows from another run or fan out the comparison rows.
            tf = spark.table(channel_text_features_full)
            if "run_id" in text_feat_cols:
                tf = tf.where(F.col("run_id") == F.lit(SOURCE_RUN_ID))
            if "inference_hash_buckets" in text_feat_cols:
                tf = tf.where(F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
            tf_select = ["channel_id"]
            if "contains_devanagari_metadata" in text_feat_cols:
                tf_select.append(F.col("contains_devanagari_metadata").alias("tf_contains_devanagari_metadata"))
            if "romanized_indic_keyword_count" in text_feat_cols:
                tf_select.append(F.col("romanized_indic_keyword_count").alias("tf_romanized_indic_keyword_count"))
            tf = tf.select(*tf_select).dropDuplicates(["channel_id"])
            sig = sig.join(tf, on="channel_id", how="left")
            if "contains_devanagari_metadata" in text_feat_cols:
                indic_signal = indic_signal | F.coalesce(F.col("tf_contains_devanagari_metadata"), F.lit(False))
            if "romanized_indic_keyword_count" in text_feat_cols:
                indic_signal = indic_signal | (F.coalesce(F.col("tf_romanized_indic_keyword_count"), F.lit(0)) > 0)
        if hindi_audit_cols:
            # D4: run-scope the Hindi/Indic audit join too; source_language_value is written there, not in
            # channel_text_features, so this preserves the D3 source-code trigger.
            hi = spark.table(hindi_indic_audit_full)
            if "run_id" in hindi_audit_cols:
                hi = hi.where(F.col("run_id") == F.lit(SOURCE_RUN_ID))
            if "inference_hash_buckets" in hindi_audit_cols:
                hi = hi.where(F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
            hi_select = ["channel_id"]
            if "contains_devanagari_metadata" in hindi_audit_cols:
                hi_select.append(F.col("contains_devanagari_metadata").alias("hi_contains_devanagari_metadata"))
            if "romanized_indic_keyword_count" in hindi_audit_cols:
                hi_select.append(F.col("romanized_indic_keyword_count").alias("hi_romanized_indic_keyword_count"))
            if "source_language_value" in hindi_audit_cols:
                hi_select.append(F.lower(F.trim(F.col("source_language_value").cast("string"))).alias("hi_source_language_value"))
            hi = hi.select(*hi_select).dropDuplicates(["channel_id"])
            sig = sig.join(hi, on="channel_id", how="left")
            if "contains_devanagari_metadata" in hindi_audit_cols:
                indic_signal = indic_signal | F.coalesce(F.col("hi_contains_devanagari_metadata"), F.lit(False))
            if "romanized_indic_keyword_count" in hindi_audit_cols:
                indic_signal = indic_signal | (F.coalesce(F.col("hi_romanized_indic_keyword_count"), F.lit(0)) > 0)
            if "source_language_value" in hindi_audit_cols:
                indic_signal = indic_signal | F.col("hi_source_language_value").isin(*sorted(SOURCE_INDIC_CODES))
        sig = sig.where(indic_signal)
        route_frames.append(sig.select(*cmp_df.columns, F.lit("shared_bias_english_indic").alias("route_reason")))

    if ROUTE_AGREEMENT_AUDIT:
        # E3: uniform-random blind sample of the agreement bucket (deterministic hash) to measure accuracy/bias.
        audit_threshold = int(max(0.0, min(1.0, AGREEMENT_AUDIT_FRACTION)) * 1_000_000)
        a = (
            cmp_df.where(F.col("consensus_status").isin(*AGREEMENT_STATUSES))
            .where(F.pmod(F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.lit(AGREEMENT_AUDIT_SEED))), F.lit(1_000_000)) < F.lit(audit_threshold))
            .withColumn("route_reason", F.lit("agreement_audit"))
        )
        route_frames.append(a)

if not route_frames:
    raise ValueError("No channels routed. Check routing_mode and route_* widgets.")

# Union; if a channel matches multiple routes, keep the highest-priority reason.
_priority = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in {
    "random_validation": 0, "disagreement": 1, "unresolved_tail": 2, "shared_bias_english_indic": 3, "agreement_audit": 4,
}.items()], []))
routed = route_frames[0]
for rf in route_frames[1:]:
    routed = routed.unionByName(rf, allowMissingColumns=True)
w = Window.partitionBy("channel_id").orderBy(F.element_at(_priority, F.col("route_reason")).asc())
routed = (
    routed.withColumn("_rk", F.row_number().over(w)).where(F.col("_rk") == 1).drop("_rk")
)
if MAX_ROUTED_CHANNELS > 0:
    routed = routed.orderBy(F.xxhash64(F.col("channel_id"))).limit(MAX_ROUTED_CHANNELS)

_routed_select = [
    "channel_id", "channel_hash_bucket", "route_reason", "consensus_status", "consensus_language_label",
]
if "consensus_source" in routed.columns:
    _routed_select.append(F.col("consensus_source").alias("fasttext_consensus_source"))
else:
    _routed_select.append(F.lit(None).cast("string").alias("fasttext_consensus_source"))
_routed_select += ["openlid_primary_language_label", "glotlid_primary_language_label"]
routed = routed.select(*_routed_select).persist()

n_routed = routed.count()
print(f"Routed channels: {n_routed:,}")
display(routed.groupBy("route_reason").count().orderBy(F.desc("count")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Assemble per-channel metadata and build the user prompt

# COMMAND ----------
# D4: build prompts from ALL segment rows (not only is_valid_text_for_lid). The fastText 40-char validity
# rule discards short channel names/titles that an LLM can still use; we keep them (flagged) but stay
# bounded by the per-type count and total-char caps below so cost/noise don't balloon.
segments_tbl = spark.table(segments_input_full)
segment_cols = set(segments_tbl.columns)
seg = (
    segments_tbl
    .where((F.col("run_id") == F.lit(SOURCE_RUN_ID)) & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS)))
    .join(routed.select("channel_id"), on="channel_id", how="inner")
    .select(
        "channel_id", "segment_type",
        F.substring(F.col("text").cast("string"), 1, MAX_SEGMENT_CHARS).alias("text"),
        F.coalesce(F.col("is_valid_text_for_lid"), F.lit(False)).alias("is_valid"),
        (F.col("short_text_reason") if "short_text_reason" in segment_cols else F.lit(None).cast("string")).alias("short_text_reason"),
        (F.col("clean_letter_count") if "clean_letter_count" in segment_cols else F.lit(None).cast("int")).alias("clean_letter_count"),
        (F.col("clean_text_len") if "clean_text_len" in segment_cols else F.lit(None).cast("int")).alias("clean_text_len"),
        (F.col("dominant_script") if "dominant_script" in segment_cols else F.lit(None).cast("string")).alias("dominant_script"),
        (F.col("dominant_script_share") if "dominant_script_share" in segment_cols else F.lit(None).cast("double")).alias("dominant_script_share"),
    )
)

seg_by_channel = seg.groupBy("channel_id").agg(
    F.collect_list(F.struct(
        "segment_type", "text", "is_valid", "short_text_reason", "clean_letter_count",
        "clean_text_len", "dominant_script", "dominant_script_share",
    )).alias("segments")
)

_prompt_max = PROMPT_MAX_CHARS
_max_titles = MAX_VIDEO_TITLES
_max_descs = MAX_VIDEO_DESCRIPTIONS
_strip_prompt_boilerplate = STRIP_PROMPT_BOILERPLATE
_dedupe_prompt_segments = DEDUPE_PROMPT_SEGMENTS

_PROMPT_BOILERPLATE_LINE_PATTERNS = [
    r"^\W*provided to youtube by\b",
    r"^\W*auto-generated by youtube\b",
    r"^released on\s*:",
    r"^(song credits?|music credits?|audio production)\b",
    r"^(main artist|producer|composer|lyricist|arranger|associated performer|music publisher|cast)\s*:",
    r"^\W*(official site|facebook|twitter|instagram|tiktok|website|discord)\b",
    r"^(click here to subscribe|make sure to subscribe|subscribe\b|for more such videos|get ready to witness)\b",
    r"^(download link|download mp3|download song|follow (us|me)|join (my|our)|support (the stream|a creator|us|me)|superchat)\b",
    r"^(copyright disclaimer|under section 107|allowance is made for fair use|fair use is a use permitted|non-profit, educational or personal use)\b",
    r"^music video by\b.*\bofficial video\b",
    r"^[\u2117\u00a9]\s*\d{4}\b",
]
_PROMPT_BOILERPLATE_PHRASE_PATTERNS = [
    r"\bcopyright disclaimer under section 107\b.*$",
    r"\bprovided to youtube by\b.*$",
    r"\bauto-generated by youtube\b.*$",
    r"\bdownload link\s*[-:]\s*\S+",
]
_PROMPT_GENERIC_HASHTAGS = {
    "shorts", "ytshorts", "shortvideo", "viral", "trending", "fyp", "explore", "motivation",
    "officialvideo", "musicvideo", "video", "song", "subscribe", "youtube", "youtubeshorts",
    "feedshorts", "shortsfeed", "reels", "foryou", "duet", "status", "newrelease", "latest",
}
_URL_RE = re.compile(r"(https?://\S+|www\.\S+)", flags=re.IGNORECASE)
_OFFICIAL_VIDEO_RE = re.compile(r"\(?\bofficial(?:\s+music)?\s+video\b\)?", flags=re.IGNORECASE)
_HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")
_PROMPT_LANGUAGE_HINT_PATTERNS = {
    "ara": [r"\barabic\b", r"\bquran\b", r"\ballah\b", r"\bazan\b"],
    "ben": [r"\bbangla\b", r"\bbengali\b", r"\bnatok\b"],
    "bho": [r"\bbhojpuri\b"],
    "ell": [r"\bgreek\b"],
    "hin": [r"\bhindi\b", r"\bbollywood\b", r"\bdesi\b", r"\bharyanvi\b"],
    "ind": [r"\bindonesian\b", r"\bindonesia\b"],
    "jav": [r"\bjavanese\b"],
    "kor": [r"\bkorean\b", r"\bk[- ]?pop\b"],
    "lao": [r"\blao\b"],
    "mal": [r"\bmalayalam\b"],
    "pan": [r"\bpunjabi\b", r"\bgurmukhi\b"],
    "por": [r"\bportuguese\b", r"\bportugu[e\u00ea]s\b"],
    "rus": [r"\brussian\b"],
    "spa": [r"\bspanish\b", r"\bespa[n\u00f1]ol\b"],
    "tam": [r"\btamil\b"],
    "tel": [r"\btelugu\b"],
    "tha": [r"\bthai\b"],
    "urd": [r"\burdu\b", r"\bshayari\b", r"\bghazal\b"],
    "wol": [r"\bwolof\b"],
    "zsm": [r"\bmalay\b", r"\bbahasa malaysia\b"],
}


def _char_script_family(ch: str) -> Optional[str]:
    if not ch or not ch.isalpha():
        return None
    name = unicodedata.name(ch, "")
    for token, script in [
        ("DEVANAGARI", "devanagari"),
        ("ARABIC", "arabic"),
        ("GURMUKHI", "gurmukhi"),
        ("BENGALI", "bengali"),
        ("TAMIL", "tamil"),
        ("TELUGU", "telugu"),
        ("MALAYALAM", "malayalam"),
        ("KANNADA", "kannada"),
        ("GUJARATI", "gujarati"),
        ("ORIYA", "odia"),
        ("SINHALA", "sinhala"),
        ("THAI", "thai"),
        ("LAO", "lao"),
        ("HANGUL", "hangul"),
        ("HIRAGANA", "japanese"),
        ("KATAKANA", "japanese"),
        ("CJK", "han"),
        ("GREEK", "greek"),
        ("CYRILLIC", "cyrillic"),
        ("HEBREW", "hebrew"),
    ]:
        if token in name:
            return script
    if name.startswith("LATIN"):
        return "latin"
    return "other"


def _text_script_counts(text: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for ch in str(text or ""):
        script = _char_script_family(ch)
        if script:
            counts[script] = counts.get(script, 0) + 1
    return counts


def _language_hint_counts(text: str) -> Dict[str, int]:
    lowered = str(text or "").lower()
    counts: Dict[str, int] = {}
    for iso, patterns in _PROMPT_LANGUAGE_HINT_PATTERNS.items():
        hits = sum(len(re.findall(pattern, lowered, flags=re.IGNORECASE)) for pattern in patterns)
        if hits:
            counts[iso] = hits
    return counts


def _remove_generic_hashtags(text: str) -> str:
    def _sub(match):
        tag = match.group(1).lower()
        return "" if tag in _PROMPT_GENERIC_HASHTAGS else match.group(0)

    return _HASHTAG_RE.sub(_sub, text)


def _clean_prompt_text(text: str, segment_type: str) -> str:
    lines = []
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = _URL_RE.sub("", line).strip()
        if not line:
            continue
        if _strip_prompt_boilerplate:
            line_l = line.lower()
            if any(re.search(pattern, line_l, flags=re.IGNORECASE) for pattern in _PROMPT_BOILERPLATE_LINE_PATTERNS):
                continue
            for pattern in _PROMPT_BOILERPLATE_PHRASE_PATTERNS:
                line = re.sub(pattern, "", line, flags=re.IGNORECASE)
            line = _OFFICIAL_VIDEO_RE.sub("", line)
            line = re.sub(r"\bauto-generated by youtube\b", "", line, flags=re.IGNORECASE)
            line = _remove_generic_hashtags(line)
        line = re.sub(r"\s+", " ", line).strip(" -|\u00b7:;")
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def _prompt_dedupe_key(text: str) -> str:
    key = _URL_RE.sub("", str(text or "").lower())
    key = re.sub(r"[\W_]+", "", key, flags=re.UNICODE)
    return key[:500]


@F.udf(StringType())
def build_user_prompt(segments) -> str:
    if not segments:
        return "No channel metadata was found."
    # collect_list has no inherent order; sort deterministically so the per-type caps (and thus the
    # batch files / verdicts) are reproducible across reruns of the same run_id.
    type_priority = {"video_title": 0, "channel_description": 1, "video_description": 2, "channel_name": 3}
    segments = sorted(
        segments,
        key=lambda s: (
            type_priority.get((s["segment_type"] or "").lower(), 9),
            0 if s["is_valid"] else 1,
            -(s["clean_letter_count"] or 0),
            (s["text"] or ""),
        ),
    )
    name, titles, descs, other = [], [], [], []
    invalid_marker = " [lid-invalid:"
    seen = set()
    script_stats = {}
    text_script_stats = {}
    field_stats = {}
    language_hint_stats = {}

    def _invalid_tag(s) -> str:
        if s["is_valid"]:
            return ""
        details = []
        if s["short_text_reason"]:
            details.append(f"reason={s['short_text_reason']}")
        if s["clean_letter_count"] is not None:
            details.append(f"letters={s['clean_letter_count']}")
        if s["clean_text_len"] is not None:
            details.append(f"clean_len={s['clean_text_len']}")
        if s["dominant_script"]:
            script = f"script={s['dominant_script']}"
            if s["dominant_script_share"] is not None:
                try:
                    script += f":{float(s['dominant_script_share']):.2f}"
                except Exception:
                    pass
            details.append(script)
        return f"{invalid_marker} {', '.join(details or ['below_fasttext_threshold'])}]"

    def _record_script(bucket: str, s) -> None:
        script = (s["dominant_script"] or "unknown").lower()
        bucket_stats = script_stats.setdefault(bucket, {})
        stats = bucket_stats.setdefault(script, {"n": 0, "valid": 0})
        stats["n"] += 1
        if s["is_valid"]:
            stats["valid"] += 1

    def _record_text_stats(bucket: str, txt: str) -> None:
        stats = field_stats.setdefault(bucket, {"n": 0, "letters": 0})
        stats["n"] += 1
        script_counts = _text_script_counts(txt)
        stats["letters"] += sum(script_counts.values())
        bucket_script_stats = text_script_stats.setdefault(bucket, {})
        for script, n_chars in script_counts.items():
            bucket_script_stats[script] = bucket_script_stats.get(script, 0) + n_chars
        for iso, n_hits in _language_hint_counts(txt).items():
            language_hint_stats[iso] = language_hint_stats.get(iso, 0) + n_hits

    def _bucket_for_segment_type(st: str) -> str:
        if st == "channel_name":
            return "channel_name"
        if st == "video_title":
            return "video_title"
        if st in ("video_description", "channel_description"):
            return "description"
        return "other"

    for s in segments:
        st = (s["segment_type"] or "").lower()
        txt = _clean_prompt_text(s["text"], st)
        if not txt:
            continue
        if _dedupe_prompt_segments:
            key = f"{st}:{_prompt_dedupe_key(txt)}"
            if key in seen:
                continue
            if len(key) > len(st) + 8:
                seen.add(key)
        entry = f"{txt}{_invalid_tag(s)}"
        bucket = _bucket_for_segment_type(st)
        _record_text_stats(bucket, txt)
        if st == "channel_name":
            name.append(entry)
            _record_script("channel_name", s)
        elif st == "video_title":
            titles.append(entry)
            _record_script("video_title", s)
        elif st in ("video_description", "channel_description"):
            descs.append(entry)
            _record_script("description", s)
        else:
            other.append(entry)
            _record_script("other", s)
    # Prioritize valid (untagged) entries, then fall back to short ones, within the per-type caps.
    def _order(items):
        return [x for x in items if invalid_marker not in x] + [x for x in items if invalid_marker in x]

    def _select_diverse(items, max_items):
        items = _order(items)
        if len(items) <= max_items:
            return items
        selected = []
        selected_ids = set()
        by_script = {}
        for idx, item in enumerate(items):
            text_only = item.split(invalid_marker, 1)[0]
            for script, n_chars in _text_script_counts(text_only).items():
                if script != "latin" and n_chars >= 6:
                    by_script.setdefault(script, []).append((idx, item, n_chars))
        for script, candidates in sorted(by_script.items(), key=lambda kv: (-sum(x[2] for x in kv[1]), kv[0])):
            for idx, item, _ in candidates[:2]:
                if len(selected) >= max_items:
                    break
                if idx not in selected_ids:
                    selected.append(item)
                    selected_ids.add(idx)
            if len(selected) >= max_items:
                break
        for idx, item in enumerate(items):
            if len(selected) >= max_items:
                break
            if idx not in selected_ids:
                selected.append(item)
                selected_ids.add(idx)
        return selected

    titles = _select_diverse(titles, _max_titles)
    descs = _select_diverse(descs, _max_descs)
    other = _select_diverse(other, _max_titles)
    lines = []
    if field_stats:
        field_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in field_stats:
                continue
            stats = field_stats[bucket]
            field_parts.append(f"{bucket}: n={stats['n']} letters={stats['letters']}")
        if field_parts:
            lines.append("FIELD SUMMARY (after cleanup): " + " | ".join(field_parts))
    if script_stats:
        summary_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in script_stats:
                continue
            scripts = sorted(script_stats[bucket].items(), key=lambda kv: (-kv[1]["n"], kv[0]))
            script_part = ", ".join(f"{script}={stats['n']} valid={stats['valid']}" for script, stats in scripts[:4])
            summary_parts.append(f"{bucket}: {script_part}")
        if summary_parts:
            lines.append("SEGMENT SCRIPT SUMMARY (metadata dominant script): " + " | ".join(summary_parts))
    if text_script_stats:
        text_script_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in text_script_stats:
                continue
            scripts = sorted(text_script_stats[bucket].items(), key=lambda kv: (-kv[1], kv[0]))
            script_part = ", ".join(f"{script}={n_chars}" for script, n_chars in scripts[:5])
            text_script_parts.append(f"{bucket}: {script_part}")
        if text_script_parts:
            lines.append("TEXT SCRIPT SUMMARY (letter counts after cleanup): " + " | ".join(text_script_parts))
    if language_hint_stats:
        hints = sorted(language_hint_stats.items(), key=lambda kv: (-kv[1], kv[0]))
        hint_part = ", ".join(f"{iso}={n_hits}" for iso, n_hits in hints[:8])
        lines.append("LANGUAGE HINTS (non-decisive cue counts): " + hint_part)
    if name:
        lines.append(f"CHANNEL NAME: {name[0]}")
    if titles:
        lines.append("VIDEO TITLES:")
        lines += [f"- {t}" for t in titles]
    if descs:
        lines.append("DESCRIPTIONS:")
        lines += [f"- {d}" for d in descs]
    if other and not (titles or descs):
        lines += [f"- {o}" for o in other]
    lines.append("(Provider metadata, generic URLs, duplicate segments, and generic hashtags may have been removed before this prompt. Items tagged [lid-invalid: ...] failed the fastText eligibility rule; repeated short items can still be meaningful evidence.)")
    prompt = "Channel metadata to classify:\n" + "\n".join(lines)
    return prompt[:_prompt_max]


prompts = seg_by_channel.withColumn("prompt_user", build_user_prompt(F.col("segments"))).select("channel_id", "prompt_user")
routed_prompts = routed.join(prompts, on="channel_id", how="left").withColumn(
    "prompt_user", F.coalesce(F.col("prompt_user"), F.lit("No usable channel metadata was found."))
)

# Fan out to one request per (channel, model).
models_df = spark.createDataFrame(
    [(m["provider"], m["model"], m.get("tier", "unspecified")) for m in MODELS],
    ["provider", "model", "model_tier"],
)
requests = (
    routed_prompts.crossJoin(models_df)
    # D4: run-scope the request identity so results from other runs can't collide on import.
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn(
        "request_id",
        F.concat(F.lit("yl_"), F.substring(F.sha2(F.concat_ws("||", F.col("run_id"), F.col("provider"), F.col("model"), F.col("channel_id")), 256), 1, 61)),
    )
    .withColumn("system_prompt", F.lit(SYSTEM_PROMPT))
    .withColumn("temperature", F.lit(TEMPERATURE).cast("double") if TEMPERATURE is not None else F.lit(None).cast("double"))
    .withColumn("max_output_tokens", F.lit(MAX_OUTPUT_TOKENS))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Build provider request lines

# COMMAND ----------
def _is_openai_reasoning_or_gpt5_model(model: Optional[str]) -> bool:
    m = (model or "").lower()
    return m.startswith("gpt-5") or m.startswith("o1") or m.startswith("o3") or m.startswith("o4")


def _openai_uses_responses_api(model: Optional[str]) -> bool:
    if OPENAI_ENDPOINT_MODE == "responses":
        return True
    if OPENAI_ENDPOINT_MODE == "chat_completions":
        return False
    return _is_openai_reasoning_or_gpt5_model(model)


def _openai_batch_endpoint_for_model(model: Optional[str]) -> str:
    return "/v1/responses" if _openai_uses_responses_api(model) else "/v1/chat/completions"


@F.udf(StringType())
def make_batch_line(provider: str, model: str, request_id: str, system_prompt: str, user_prompt: str,
                    temperature: Optional[float], max_output_tokens: int) -> str:
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
                "text": {"format": {"type": "json_schema", "name": "lid_panel_prediction",
                                    "schema": LANG_RESPONSE_JSON_SCHEMA, "strict": False}, "verbosity": "low"},
            }
            if OPENAI_REASONING_EFFORT:
                body["reasoning"] = {"effort": OPENAI_REASONING_EFFORT}
            if temp is not None:
                body["temperature"] = temp
            obj = {"custom_id": request_id, "method": "POST", "url": "/v1/responses", "body": body}
        else:
            body = {
                "model": model,
                "response_format": {"type": "json_object"},
                "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            }
            body["max_completion_tokens" if _is_openai_reasoning_or_gpt5_model(model) else "max_tokens"] = max_out
            if temp is not None:
                body["temperature"] = temp
            obj = {"custom_id": request_id, "method": "POST", "url": "/v1/chat/completions", "body": body}
    elif provider == "anthropic":
        params = {"model": model, "max_tokens": max_out, "system": system_prompt,
                  "messages": [{"role": "user", "content": user_prompt}]}
        if temp is not None:
            params["temperature"] = temp
        obj = {"custom_id": request_id, "params": params}
    elif provider == "gemini":
        generation_config = {"max_output_tokens": max_out, "response_mime_type": "application/json"}
        if temp is not None:
            generation_config["temperature"] = temp
        if GEMINI_THINKING_LEVEL:
            generation_config["thinking_config"] = {"thinking_level": GEMINI_THINKING_LEVEL}
        obj = {"key": request_id, "request": {"system_instruction": {"parts": [{"text": system_prompt}]},
               "contents": [{"role": "user", "parts": [{"text": user_prompt}]}], "generation_config": generation_config}}
    elif provider == "deepseek":
        deepseek_max_out = int(DEEPSEEK_MAX_OUTPUT_TOKENS or max_out)
        body = {
            "model": model,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            "max_tokens": deepseek_max_out,
            "stream": False,
        }
        if DEEPSEEK_THINKING_TYPE:
            body.setdefault("extra_body", {})["thinking"] = {"type": DEEPSEEK_THINKING_TYPE}
        if DEEPSEEK_REASONING_EFFORT and DEEPSEEK_THINKING_TYPE == "enabled":
            body.setdefault("extra_body", {})["reasoning_effort"] = DEEPSEEK_REASONING_EFFORT
        if temp is not None:
            body["temperature"] = temp
        # DeepSeek is OpenAI-compatible for chat completions, but does not use the provider Batch API here.
        # The submit step calls these request bodies directly and writes result JSONL for the common parser.
        obj = {"custom_id": request_id, "method": "POST", "url": "/chat/completions", "body": body}
    else:
        raise ValueError(f"Unsupported provider: {provider}")
    return json.dumps(obj, ensure_ascii=False)


requests = requests.withColumn(
    "batch_line",
    make_batch_line(F.col("provider"), F.col("model"), F.col("request_id"), F.col("system_prompt"),
                    F.col("prompt_user"), F.col("temperature"), F.col("max_output_tokens")),
)
rw = Window.partitionBy("provider", "model").orderBy("request_id")
requests = requests.withColumn("_n", F.row_number().over(rw)) \
    .withColumn("chunk_id", F.floor((F.col("_n") - F.lit(1)) / F.lit(MAX_REQUESTS_PER_FILE)).cast("int")).drop("_n")

write_run_scoped(requests, panel_requests_full)
print("Wrote request table to", panel_requests_full)
display(spark.table(panel_requests_full).where(F.col("run_id") == F.lit(RUN_ID)).groupBy("provider", "model").count())

# COMMAND ----------
# Write JSONL batch files to DBFS (one per provider/model/chunk).
os.makedirs(BATCH_OUTPUT_DIR, exist_ok=True)
run_dir = os.path.join(BATCH_OUTPUT_DIR, RUN_ID)
os.makedirs(run_dir, exist_ok=True)
_run_requests = spark.table(panel_requests_full).where(F.col("run_id") == F.lit(RUN_ID))
groups = _run_requests.select("provider", "model", "chunk_id").distinct().orderBy("provider", "model", "chunk_id").collect()
batch_file_records = []
for g in groups:
    provider, model, chunk_id = g["provider"], g["model"], int(g["chunk_id"])
    provider_dir = os.path.join(run_dir, provider, safe_model_dir(model))
    os.makedirs(provider_dir, exist_ok=True)
    local_path = os.path.join(provider_dir, f"chunk_{chunk_id:05d}.jsonl")
    subset = _run_requests.where(
        (F.col("provider") == provider) & (F.col("model") == model) & (F.col("chunk_id") == chunk_id)
    ).select("batch_line")
    n = 0
    n_bytes = 0
    with open(local_path, "w", encoding="utf-8") as f:
        for row in subset.toLocalIterator():
            line = row["batch_line"]
            f.write(line + "\n")
            n += 1
            n_bytes += len(line.encode("utf-8")) + 1
    batch_file_records.append((RUN_ID, provider, model, chunk_id, local_path, n, n_bytes, datetime.utcnow().isoformat()))
    print(f"Wrote {n:,} requests: {local_path} ({n_bytes:,} bytes)")

# D4: persist a batch-file registry (run-scoped, idempotent) so submission/import are auditable.
if batch_file_records:
    batch_files_df = spark.createDataFrame(
        batch_file_records,
        ["run_id", "provider", "model", "chunk_id", "local_jsonl_path", "n_requests", "n_bytes", "created_at_utc"],
    )
    write_run_scoped(batch_files_df, panel_batch_files_full)
    print("Wrote batch-file registry to", panel_batch_files_full)
print("Batch files written under", run_dir)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Optional: submit batches/direct requests (set submit_batches=true; reads API keys from Databricks Secrets)

# COMMAND ----------
def get_secret(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


def submit_openai_batch(path: str, model: str) -> Dict[str, Any]:
    from openai import OpenAI
    client = OpenAI(api_key=get_secret(SECRET_SCOPE, OPENAI_SECRET_KEY))
    with open(path, "rb") as f:
        uploaded = client.files.create(file=f, purpose="batch")
    batch = client.batches.create(input_file_id=uploaded.id, endpoint=_openai_batch_endpoint_for_model(model),
                                  completion_window="24h", metadata={"run_id": RUN_ID, "task": "yt_lid_panel", "model": model})
    return {"provider_file_id": uploaded.id, "provider_batch_id": batch.id, "provider_status": getattr(batch, "status", None)}


def submit_anthropic_batch(path: str, model: str) -> Dict[str, Any]:
    import anthropic
    client = anthropic.Anthropic(api_key=get_secret(SECRET_SCOPE, ANTHROPIC_SECRET_KEY))
    payload = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
    batch = client.messages.batches.create(requests=payload)
    return {"provider_file_id": None, "provider_batch_id": batch.id, "provider_status": getattr(batch, "processing_status", None)}


def submit_gemini_batch(path: str, model: str) -> Dict[str, Any]:
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=get_secret(SECRET_SCOPE, GEMINI_SECRET_KEY))
    uploaded = client.files.upload(file=path, config=types.UploadFileConfig(display_name=f"{RUN_ID}_{safe_model_dir(model)}", mime_type="jsonl"))
    batch = client.batches.create(model=model, src=uploaded.name, config={"display_name": f"{RUN_ID}_{safe_model_dir(model)}"})
    batch_state = getattr(batch, "state", None)
    return {
        "provider_file_id": getattr(uploaded, "name", None),
        "provider_batch_id": getattr(batch, "name", None),
        "provider_status": getattr(batch_state, "name", None) or (str(batch_state) if batch_state is not None else None),
    }


def submit_deepseek_direct(path: str, model: str) -> Dict[str, Any]:
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import requests

    api_key = get_secret(SECRET_SCOPE, DEEPSEEK_SECRET_KEY)
    thread_state = threading.local()
    result_dir = os.path.join(RESULTS_INPUT_DIR, RUN_ID, "deepseek", safe_model_dir(model))
    os.makedirs(result_dir, exist_ok=True)
    result_path = os.path.join(result_dir, os.path.basename(path).replace(".jsonl", "_results.jsonl"))
    n_ok = 0
    n_error = 0

    def _session():
        session = getattr(thread_state, "session", None)
        if session is None:
            session = requests.Session()
            thread_state.session = session
        return session

    def _parse_response_body(response):
        try:
            return response.json()
        except Exception:
            return {"text": response.text[:4000]}

    def _call_line(line: str):
        req = {}
        request_started_perf = time.perf_counter()
        attempt_records = []
        try:
            req = json.loads(line)
            custom_id = req.get("custom_id") or req.get("key")
            body = dict(req["body"])
            extra_body = body.pop("extra_body", None)
            if isinstance(extra_body, dict):
                body.update(extra_body)
            last_error = None
            for attempt in range(DEEPSEEK_MAX_RETRIES + 1):
                attempt_started_perf = time.perf_counter()
                try:
                    response = _session().post(
                        "https://api.deepseek.com/chat/completions",
                        headers={
                            "Authorization": f"Bearer {api_key}",
                            "Content-Type": "application/json",
                        },
                        json=body,
                        timeout=DEEPSEEK_REQUEST_TIMEOUT_SECONDS,
                    )
                    elapsed_ms = round((time.perf_counter() - attempt_started_perf) * 1000, 1)
                    response_body = _parse_response_body(response)
                    attempt_records.append({
                        "attempt": attempt + 1,
                        "status_code": response.status_code,
                        "duration_ms": elapsed_ms,
                    })
                    out = {
                        "custom_id": custom_id,
                        "response": {
                            "status_code": response.status_code,
                            "body": response_body,
                        },
                        "_deepseek_direct_metadata": {
                            "attempts": len(attempt_records),
                            "duration_ms": round((time.perf_counter() - request_started_perf) * 1000, 1),
                            "attempts_detail": attempt_records,
                            "thinking_type": DEEPSEEK_THINKING_TYPE or None,
                            "max_tokens": body.get("max_tokens"),
                        },
                    }
                    if 200 <= response.status_code < 300:
                        return out, True
                    last_error = response.text[:2000]
                    if response.status_code not in {408, 409, 429, 500, 502, 503, 504} or attempt >= DEEPSEEK_MAX_RETRIES:
                        out["error"] = last_error
                        return out, False
                except Exception as e:
                    last_error = repr(e)[:2000]
                    attempt_records.append({
                        "attempt": attempt + 1,
                        "status_code": 500,
                        "duration_ms": round((time.perf_counter() - attempt_started_perf) * 1000, 1),
                        "error": last_error,
                    })
                    if attempt >= DEEPSEEK_MAX_RETRIES:
                        out = {
                            "custom_id": custom_id,
                            "response": {"status_code": 500, "error": last_error},
                            "_deepseek_direct_metadata": {
                                "attempts": len(attempt_records),
                                "duration_ms": round((time.perf_counter() - request_started_perf) * 1000, 1),
                                "attempts_detail": attempt_records,
                                "thinking_type": DEEPSEEK_THINKING_TYPE or None,
                                "max_tokens": body.get("max_tokens"),
                            },
                            "error": last_error,
                        }
                        return out, False
                time.sleep(min(2 ** attempt, 8))
            out = {
                "custom_id": custom_id,
                "response": {"status_code": 500, "error": last_error},
                "_deepseek_direct_metadata": {
                    "attempts": len(attempt_records),
                    "duration_ms": round((time.perf_counter() - request_started_perf) * 1000, 1),
                    "attempts_detail": attempt_records,
                    "thinking_type": DEEPSEEK_THINKING_TYPE or None,
                    "max_tokens": body.get("max_tokens"),
                },
                "error": last_error,
            }
            return out, False
        except Exception as e:
            custom_id = None
            try:
                custom_id = req.get("custom_id") or req.get("key")
            except Exception:
                pass
            out = {
                "custom_id": custom_id,
                "response": {"status_code": 500, "error": repr(e)[:2000]},
                "_deepseek_direct_metadata": {
                    "attempts": len(attempt_records),
                    "duration_ms": round((time.perf_counter() - request_started_perf) * 1000, 1),
                    "attempts_detail": attempt_records,
                    "thinking_type": DEEPSEEK_THINKING_TYPE or None,
                },
                "error": repr(e)[:2000],
            }
            return out, False

    def _successful_existing_result(line: str):
        try:
            obj = json.loads(line)
            custom_id = obj.get("custom_id")
            status_code = int(obj.get("response", {}).get("status_code", 500))
            if custom_id and not obj.get("error") and 200 <= status_code < 300:
                return custom_id, line if line.endswith("\n") else line + "\n"
        except Exception:
            return None, None
        return None, None

    with open(path, "r", encoding="utf-8") as src:
        lines = [line for line in src if line.strip()]

    completed_ids = set()
    existing_success_lines = []
    if os.path.exists(result_path):
        with open(result_path, "r", encoding="utf-8") as existing:
            for existing_line in existing:
                if not existing_line.strip():
                    continue
                custom_id, normalized_line = _successful_existing_result(existing_line)
                if custom_id:
                    completed_ids.add(custom_id)
                    existing_success_lines.append(normalized_line)

    pending_lines = []
    for line in lines:
        try:
            req = json.loads(line)
            custom_id = req.get("custom_id") or req.get("key")
        except Exception:
            custom_id = None
        if custom_id not in completed_ids:
            pending_lines.append(line)

    total = len(lines)
    print(
        f"DeepSeek direct {model}: {len(pending_lines):,}/{total:,} pending requests "
        f"with {DEEPSEEK_MAX_WORKERS} workers; preserved_success={len(existing_success_lines):,}"
    )
    with open(result_path, "w", encoding="utf-8") as dst:
        for line in existing_success_lines:
            dst.write(line)
        dst.flush()
        with ThreadPoolExecutor(max_workers=DEEPSEEK_MAX_WORKERS) as pool:
            futures = [pool.submit(_call_line, line) for line in pending_lines]
            for i, fut in enumerate(as_completed(futures), start=1):
                out, ok = fut.result()
                if ok:
                    n_ok += 1
                else:
                    n_error += 1
                dst.write(json.dumps(out, ensure_ascii=False) + "\n")
                if i % 100 == 0 or i == len(pending_lines):
                    dst.flush()
                    print(f"DeepSeek direct {model}: {i:,}/{len(pending_lines):,} pending done; ok={n_ok:,}; error={n_error:,}")

    total_rows = 0
    total_errors = 0
    with open(result_path, "r", encoding="utf-8") as final:
        for line in final:
            if not line.strip():
                continue
            total_rows += 1
            try:
                obj = json.loads(line)
                status_code = int(obj.get("response", {}).get("status_code", 500))
                if obj.get("error") or not (200 <= status_code < 300):
                    total_errors += 1
            except Exception:
                total_errors += 1

    status = "completed" if total_errors == 0 and total_rows == total else "partial_or_errors"
    return {
        "provider_file_id": result_path,
        "provider_batch_id": f"deepseek-direct:{RUN_ID}:{safe_model_dir(model)}:{os.path.basename(path)}",
        "provider_status": f"{status}; ok={total_rows - total_errors}; error={total_errors}",
    }


batch_job_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("local_jsonl_path", StringType(), True),
    StructField("n_requests", IntegerType(), True),
    StructField("n_bytes", IntegerType(), True),
    StructField("provider_file_id", StringType(), True),
    StructField("provider_batch_id", StringType(), True),
    StructField("provider_status", StringType(), True),
    StructField("submission_status", StringType(), True),
    StructField("submitted_at_utc", StringType(), True),
    StructField("recorded_at_utc", StringType(), True),
    StructField("submission_error", StringType(), True),
])

def persist_batch_job_records(records):
    if records:
        batch_jobs_df = spark.createDataFrame(records, batch_job_schema)
        write_run_scoped(batch_jobs_df, panel_batch_jobs_full)
        print(f"Wrote {len(records):,} batch-job records to", panel_batch_jobs_full)


def existing_batch_job_records():
    if not (SKIP_EXISTING_SUBMITTED_BATCHES and _table_exists_full(panel_batch_jobs_full)):
        return []
    existing = (
        spark.table(panel_batch_jobs_full)
        .where(F.col("run_id") == F.lit(RUN_ID))
    )
    records = []
    for row in existing.collect():
        records.append(tuple(row[f.name] if f.name in row.asDict(recursive=False) else None for f in batch_job_schema.fields))
    if records:
        print(f"Found {len(records):,} existing batch-job records for this run.")
    return records


def provider_status_is_successful(provider_status: str) -> bool:
    status_l = (provider_status or "").lower()
    if re.search(r"failed|failure|partial_or_errors|completed_with_errors|error=[1-9]", status_l):
        return False
    if status_l.strip() in {"error", "errored"}:
        return False
    return True


if SUBMIT_BATCHES:
    batch_job_records = existing_batch_job_records()
    already_submitted = set()
    for r in batch_job_records:
        provider = str(r[1])
        provider_status = str(r[9] or "")
        submission_status = str(r[10] or "")
        if (
            provider in {"anthropic", "gemini", "openai", "deepseek"}
            and submission_status == "submitted"
            and r[8] is not None
            and provider_status_is_successful(provider_status)
        ):
            already_submitted.add((provider, str(r[2]), int(r[3])))
    if already_submitted:
        print(f"Skipping {len(already_submitted):,} already submitted provider-batch chunks.")

    def replace_batch_job_record(record):
        nonlocal_records = []
        for existing_record in batch_job_records:
            if (
                str(existing_record[1]) == str(record[1])
                and str(existing_record[2]) == str(record[2])
                and int(existing_record[3]) == int(record[3])
            ):
                continue
            nonlocal_records.append(existing_record)
        nonlocal_records.append(record)
        return nonlocal_records

    # Submit asynchronous batch providers before direct providers so long direct calls do not delay batch
    # provider processing, and keep each provider/model in a stable order for reproducibility.
    _submit_priority = {"openai": 0, "anthropic": 1, "gemini": 2, "deepseek": 3}
    submission_records = sorted(
        batch_file_records,
        key=lambda r: (_submit_priority.get(str(r[1]), 99), str(r[2]), int(r[3])),
    )

    for rec in submission_records:
        _, provider, model, chunk_id, path, n, n_bytes, _ = rec
        if SUBMIT_PROVIDER_FILTER and str(provider) not in SUBMIT_PROVIDER_FILTER:
            print(provider, model, chunk_id, "not in submit_provider_filter; skipping")
            continue
        if (str(provider), str(model), int(chunk_id)) in already_submitted:
            print(provider, model, chunk_id, "already submitted; skipping")
            continue
        submitted_at = datetime.utcnow().isoformat()
        batch_job_records = replace_batch_job_record((
            RUN_ID, provider, model, int(chunk_id), path, int(n), int(n_bytes), None,
            None, "running", "running", submitted_at, datetime.utcnow().isoformat(), None,
        ))
        persist_batch_job_records(batch_job_records)
        try:
            submitter = {
                "openai": submit_openai_batch,
                "anthropic": submit_anthropic_batch,
                "gemini": submit_gemini_batch,
                "deepseek": submit_deepseek_direct,
            }[provider]
            res = submitter(path, model)
            print(provider, model, chunk_id, "submitted", res)
            batch_job_records = replace_batch_job_record((
                RUN_ID, provider, model, int(chunk_id), path, int(n), int(n_bytes), res.get("provider_file_id"),
                res.get("provider_batch_id"), res.get("provider_status"), "submitted",
                submitted_at, datetime.utcnow().isoformat(), None,
            ))
            persist_batch_job_records(batch_job_records)
            already_submitted.add((str(provider), str(model), int(chunk_id)))
        except Exception as e:
            err = repr(e)[:500]
            print(provider, model, chunk_id, "ERROR", err)
            batch_job_records = replace_batch_job_record((
                RUN_ID, provider, model, int(chunk_id), path, int(n), int(n_bytes), None,
                None, None, "error", submitted_at, datetime.utcnow().isoformat(), err,
            ))
            persist_batch_job_records(batch_job_records)
else:
    print("submit_batches=false — JSONL files written for external/colleague submission.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Import + parse provider results, then reconcile the panel verdict
# MAGIC
# MAGIC Put downloaded result JSONL files anywhere under `results_input_dir`, set `import_results=true`, re-run.

# COMMAND ----------
parse_schema = StructType([
    StructField("request_id", StringType(), True),
    StructField("provider_result_model", StringType(), True),
    StructField("raw_text", StringType(), True),
    StructField("result_status", StringType(), True),
    StructField("parse_error", StringType(), True),
])


def _dig(obj, path, default=None):
    cur = obj
    for p in path:
        try:
            cur = cur[p]
        except Exception:
            return default
    return cur


def _openai_text(body):
    if not isinstance(body, dict):
        return None
    if body.get("output_text"):
        return body["output_text"]
    chat = _dig(body, ["choices", 0, "message", "content"])
    if chat:
        return chat
    chunks = []
    for item in body.get("output", []) or []:
        for part in item.get("content", []) or []:
            if isinstance(part, dict) and part.get("type") in {"output_text", "text"} and part.get("text"):
                chunks.append(part["text"])
    return "\n".join(chunks) if chunks else None


def extract_provider_text(line: str) -> Dict[str, Any]:
    try:
        obj = json.loads(line)
    except Exception as e:
        return {"request_id": None, "provider_result_model": None, "raw_text": None, "result_status": "json_load_error", "parse_error": repr(e)[:300]}
    rid = obj.get("custom_id") or obj.get("key") or obj.get("id")
    text = model = status = None
    body = _dig(obj, ["response", "body"])
    if body:
        status = str(_dig(obj, ["response", "status_code"], body.get("status", "succeeded")))
        model = body.get("model")
        text = _openai_text(body)
    if text is None and obj.get("result"):
        r = obj["result"]
        status = r.get("type")
        msg = r.get("message", {}) if isinstance(r, dict) else {}
        model = msg.get("model")
        content = msg.get("content", [])
        if isinstance(content, list):
            text = "\n".join(c.get("text", "") for c in content if isinstance(c, dict) and c.get("type") == "text")
    if text is None:
        rid = rid or obj.get("key")
        status = status or obj.get("status") or "unknown"
        model = model or obj.get("modelVersion") or _dig(obj, ["response", "modelVersion"])
        text = (_dig(obj, ["response", "candidates", 0, "content", "parts", 0, "text"])
                or _dig(obj, ["candidates", 0, "content", "parts", 0, "text"])
                or _dig(obj, ["response", "text"]))
    if text is None:
        err = obj.get("error") or _dig(obj, ["response", "error"]) or _dig(obj, ["result", "error"])
        return {"request_id": rid, "provider_result_model": model, "raw_text": None, "result_status": status, "parse_error": (json.dumps(err)[:300] if err else "could_not_extract_text")}
    return {"request_id": rid, "provider_result_model": model, "raw_text": text, "result_status": status or "succeeded", "parse_error": None}


@F.udf(parse_schema)
def extract_provider_text_udf(line: str):
    d = extract_provider_text(line)
    return tuple(d.get(f.name) for f in parse_schema.fields)


pred_schema = StructType([
    StructField("primary_language_label", StringType(), True),
    StructField("primary_language_iso639_3", StringType(), True),
    StructField("primary_language_script", StringType(), True),
    StructField("status", StringType(), True),
    StructField("is_romanized", BooleanType(), True),
    StructField("is_high_risk_tail", BooleanType(), True),
    StructField("is_mixed_language", BooleanType(), True),
    StructField("secondary_language_label", StringType(), True),
    StructField("dialect_or_variant", StringType(), True),
    StructField("mixed_languages", ArrayType(StringType()), True),
    StructField("confidence", StringType(), True),
    StructField("evidence", StringType(), True),
    StructField("prediction_parse_error", StringType(), True),
])


def extract_first_json_object(text: Optional[str]) -> Optional[Dict[str, Any]]:
    if text is None:
        return None
    text = text.strip()
    try:
        o = json.loads(text)
        return o if isinstance(o, dict) else None
    except Exception:
        pass
    dec = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch == "{":
            try:
                o, _ = dec.raw_decode(text[i:])
                if isinstance(o, dict):
                    return o
            except Exception:
                continue
    return None


def _extract_jsonish_value(text: str, key: str):
    # Conservative recovery for provider outputs truncated after the label fields but before the
    # closing JSON brace. This only returns values explicitly present in the text.
    pattern = r'"' + re.escape(key) + r'"\s*:\s*(null|true|false|"((?:\\.|[^"\\])*)"|\[[^\]]*\])'
    match = re.search(pattern, text or "", flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    token = match.group(1)
    if token == "null":
        return None
    if token == "true":
        return True
    if token == "false":
        return False
    try:
        return json.loads(token)
    except Exception:
        if token.startswith('"') and token.endswith('"'):
            return token[1:-1]
    return None


def extract_partial_prediction_object(text: Optional[str]) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    keys = [
        "status", "primary_language_label", "primary_language_iso639_3", "primary_language_script",
        "is_romanized", "dialect_or_variant", "is_high_risk_tail", "secondary_language_label",
        "is_mixed_language", "mixed_languages", "confidence", "evidence",
    ]
    recovered = {k: _extract_jsonish_value(text, k) for k in keys}
    has_label = bool(recovered.get("primary_language_label") or recovered.get("primary_language_iso639_3"))
    if not has_label:
        return None
    if recovered.get("status") is None:
        recovered["status"] = "classified"
    if recovered.get("confidence") is None:
        recovered["confidence"] = "low"
    return recovered


def _base_iso(label, iso):
    if iso:
        return str(iso).split("_")[0].lower()
    if label:
        return str(label).split("_")[0].lower()
    return None


def _to_nullable_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "t", "yes", "y"}:
            return True
        if v in {"0", "false", "f", "no", "n"}:
            return False
    return None


def _string_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None and str(v).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


@F.udf(pred_schema)
def normalize_prediction_udf(raw_text: str):
    o = extract_first_json_object(raw_text)
    if not o:
        o = extract_partial_prediction_object(raw_text)
    if not o:
        return (None, None, None, None, None, None, None, None, None, [], None, None, "no_json_object")
    label = o.get("primary_language_label")
    iso = o.get("primary_language_iso639_3") or _base_iso(label, None)
    script = o.get("primary_language_script") or (str(label).split("_")[1] if label and "_" in str(label) else None)
    return (
        label, iso, script, o.get("status"),
        _to_nullable_bool(o.get("is_romanized")),
        _to_nullable_bool(o.get("is_high_risk_tail")),
        _to_nullable_bool(o.get("is_mixed_language")),
        o.get("secondary_language_label"),
        o.get("dialect_or_variant"),
        _string_list(o.get("mixed_languages")),
        o.get("confidence"),
        (o.get("evidence") or "")[:500],
        None,
    )


if IMPORT_RESULTS:
    # D4: recurse through this run's result subtree when available. Falling back to RESULTS_INPUT_DIR keeps
    # older manual layouts importable while avoiding stale cross-run files in normal runs.
    if not os.path.exists(local_fs_path(RESULTS_INPUT_DIR)):
        raise FileNotFoundError(f"results_input_dir does not exist: {RESULTS_INPUT_DIR}")
    run_results_input_dir = os.path.join(RESULTS_INPUT_DIR.rstrip("/"), RUN_ID)
    results_read_dir = run_results_input_dir if os.path.exists(local_fs_path(run_results_input_dir)) else RESULTS_INPUT_DIR
    print("Importing result JSONL files from", results_read_dir)
    raw = (
        spark.read.option("recursiveFileLookup", "true").text(spark_path(results_read_dir))
        .withColumnRenamed("value", "line").where(F.length("line") > 2)
    )
    parsed = raw.withColumn("p", extract_provider_text_udf(F.col("line"))).select("p.*")
    parsed = parsed.withColumn("pred", normalize_prediction_udf(F.col("raw_text"))).select("*", "pred.*").drop("pred")
    # Keep ONLY this run's results by joining to the exact request registry. Do not parse request_id:
    # run/model/channel IDs may contain the delimiter used in request_id.
    request_map = (
        spark.table(panel_requests_full)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .select(
            "request_id",
            F.col("run_id").alias("_request_run_id"),
            F.col("provider").alias("_request_provider"),
            F.col("model").alias("_request_model"),
            F.col("model_tier").alias("_request_model_tier"),
            F.col("channel_id").alias("_request_channel_id"),
        )
        .dropDuplicates(["request_id"])
    )
    parsed = (
        parsed
        .join(request_map, on="request_id", how="inner")
        .withColumn("result_run_id", F.col("_request_run_id"))
        .withColumn("provider", F.col("_request_provider"))
        .withColumn("model", F.col("_request_model"))
        .withColumn("model_tier", F.col("_request_model_tier"))
        .withColumn("channel_id", F.col("_request_channel_id"))
        .drop("_request_run_id", "_request_provider", "_request_model", "_request_model_tier", "_request_channel_id")
        .withColumn("_pred_iso_raw", F.lower(F.trim(F.col("primary_language_iso639_3"))))
        .withColumn("_pred_iso_from_label", F.lower(F.trim(F.split("primary_language_label", "_").getItem(0))))
        .withColumn("_pred_iso_raw", F.when(F.col("_pred_iso_raw").isin("", "null", "none"), F.lit(None)).otherwise(F.col("_pred_iso_raw")))
        .withColumn("_pred_iso_from_label", F.when(F.col("_pred_iso_from_label").isin("", "null", "none"), F.lit(None)).otherwise(F.col("_pred_iso_from_label")))
        .withColumn("pred_base_iso", F.coalesce(F.col("_pred_iso_raw"), F.col("_pred_iso_from_label")))
        .withColumn("_pred_script_from_label", script_from_label_expr(F.col("primary_language_label")))
        .withColumn("pred_script_family", script_family_expr(F.coalesce(F.col("primary_language_script"), F.col("_pred_script_from_label"))))
        .withColumn("pred_normalized_base_iso", canonical_base_iso_expr(F.col("pred_base_iso")))
        .withColumn("pred_normalized_language_label", normalized_language_label_expr(F.col("pred_base_iso"), F.col("pred_script_family")))
        .drop("_pred_iso_raw", "_pred_iso_from_label", "_pred_script_from_label")
    )
    imported_at_utc = datetime.utcnow().isoformat()
    result_status_l = F.lower(F.coalesce(F.col("result_status").cast("string"), F.lit("")))
    failed_result_status = (
        result_status_l.rlike("^[45][0-9][0-9]$")
        | result_status_l.isin(
            "error", "failed", "failure", "errored", "expired", "cancelled", "canceled",
            "json_load_error", "rate_limited", "timeout",
        )
    )
    parsed = (
        parsed.withColumn("run_id", F.lit(RUN_ID))
        .withColumn("imported_at_utc", F.lit(imported_at_utc))
        .withColumn(
            "is_valid_panel_vote",
            (F.col("pred_normalized_base_iso").isNotNull())
            & (F.lower(F.coalesce(F.col("status").cast("string"), F.lit(""))) == F.lit("classified"))
            & F.col("parse_error").isNull()
            & F.col("prediction_parse_error").isNull()
            & (~failed_result_status)
        )
    )
    # D4: dedupe to one result per request_id so duplicate result files can't inflate the panel vote count
    # (one vote per model). Prefer a valid classified prediction if duplicated result files disagree.
    w_request = Window.partitionBy("request_id").orderBy(
        F.desc(F.col("is_valid_panel_vote").cast("int")),
        F.desc(F.col("parse_error").isNull().cast("int")),
        F.desc(F.col("raw_text").isNotNull().cast("int")),
        F.asc(F.coalesce(F.col("result_status").cast("string"), F.lit(""))),
        F.asc(F.coalesce(F.col("provider_result_model").cast("string"), F.lit(""))),
    )
    parsed = (
        parsed.withColumn("_request_rank", F.row_number().over(w_request))
        .where(F.col("_request_rank") == 1)
        .drop("_request_rank")
    )
    write_run_scoped(parsed, panel_raw_results_full)
    print("Wrote parsed per-model predictions to", panel_raw_results_full)

    valid_model_votes = (
        parsed.where(F.col("is_valid_panel_vote") == F.lit(True))
        .select(
            "channel_id",
            F.col("provider").alias("provider"),
            F.col("model").alias("model"),
            F.col("model_tier").alias("model_tier"),
            F.col("primary_language_label").alias("language_label"),
            F.col("pred_base_iso").alias("base_iso"),
            F.col("pred_normalized_base_iso").alias("normalized_base_iso"),
            F.col("pred_normalized_language_label").alias("normalized_language_label"),
        )
        .withColumn("model_key", F.concat_ws(":", F.col("provider"), F.col("model")))
    )
    if valid_model_votes.limit(1).count() > 0:
        a = valid_model_votes.alias("a")
        b = valid_model_votes.alias("b")
        pairwise_agreement = (
            a.join(b, on="channel_id", how="inner")
            .where(F.col("a.model_key") < F.col("b.model_key"))
            .groupBy(
                F.col("a.provider").alias("provider_a"),
                F.col("a.model").alias("model_a"),
                F.col("a.model_tier").alias("model_tier_a"),
                F.col("b.provider").alias("provider_b"),
                F.col("b.model").alias("model_b"),
                F.col("b.model_tier").alias("model_tier_b"),
            )
            .agg(
                F.count(F.lit(1)).alias("n_both_classified"),
                F.sum(F.when(F.col("a.base_iso") == F.col("b.base_iso"), 1).otherwise(0)).alias("n_base_iso_agree"),
                F.sum(F.when(F.col("a.normalized_base_iso") == F.col("b.normalized_base_iso"), 1).otherwise(0)).alias("n_normalized_base_iso_agree"),
                F.sum(F.when(F.col("a.language_label") == F.col("b.language_label"), 1).otherwise(0)).alias("n_full_label_agree"),
                F.sum(F.when(F.col("a.normalized_language_label") == F.col("b.normalized_language_label"), 1).otherwise(0)).alias("n_normalized_label_agree"),
            )
            .withColumn("base_iso_agreement_rate", F.col("n_base_iso_agree") / F.col("n_both_classified"))
            .withColumn("normalized_base_iso_agreement_rate", F.col("n_normalized_base_iso_agree") / F.col("n_both_classified"))
            .withColumn("full_label_agreement_rate", F.col("n_full_label_agree") / F.col("n_both_classified"))
            .withColumn("normalized_label_agreement_rate", F.col("n_normalized_label_agree") / F.col("n_both_classified"))
            .withColumn("same_provider", F.col("provider_a") == F.col("provider_b"))
            .withColumn("same_tier", F.col("model_tier_a") == F.col("model_tier_b"))
            .withColumn("run_id", F.lit(RUN_ID))
            .withColumn("computed_at_utc", F.lit(imported_at_utc))
            .select(
                "run_id", "provider_a", "model_a", "model_tier_a", "provider_b", "model_b", "model_tier_b",
                "same_provider", "same_tier", "n_both_classified", "n_base_iso_agree", "base_iso_agreement_rate",
                "n_normalized_base_iso_agree", "normalized_base_iso_agreement_rate",
                "n_full_label_agree", "full_label_agreement_rate",
                "n_normalized_label_agree", "normalized_label_agreement_rate", "computed_at_utc",
            )
        )
        write_run_scoped(pairwise_agreement, panel_model_agreement_full)
        print("Wrote all-model pairwise agreement matrix to", panel_model_agreement_full)
        display(pairwise_agreement.orderBy("provider_a", "model_a", "provider_b", "model_b"))
    else:
        print("No valid per-model votes available; skipping all-model agreement matrix.")

    # --- Reconcile: majority vote on base ISO, but PRESERVE the full winning label/script + side fields. ---
    n_models = len(MODELS)
    configured_majority_threshold = max(MIN_PANEL_VOTES_FOR_MAJORITY, (n_models // 2) + 1)
    _panel_vote_iso_source = "pred_normalized_base_iso" if PANEL_MAJORITY_VOTE_BASIS == "normalized_base_iso" else "pred_base_iso"
    votes = parsed.where(F.col("is_valid_panel_vote") == F.lit(True)).withColumn("_panel_vote_iso", F.col(_panel_vote_iso_source))
    per_iso = votes.groupBy("channel_id", "_panel_vote_iso").agg(F.count(F.lit(1)).alias("n_votes"))
    vote_dist = (
        per_iso.groupBy("channel_id")
        .agg(
            F.sort_array(
                F.collect_list(F.struct(F.col("n_votes"), F.col("_panel_vote_iso").alias("iso"))),
                asc=False,
            ).alias("panel_vote_distribution")
        )
        .withColumn("n_distinct_panel_vote_iso", F.size(F.col("panel_vote_distribution")))
        .withColumn("panel_second_votes", F.coalesce(F.expr("get(panel_vote_distribution, 1).n_votes"), F.lit(0)))
        .withColumn("panel_vote_margin", F.expr("get(panel_vote_distribution, 0).n_votes") - F.col("panel_second_votes"))
        .withColumn("panel_vote_distribution_json", F.to_json(F.col("panel_vote_distribution")))
    )
    w_iso = Window.partitionBy("channel_id").orderBy(F.desc("n_votes"), F.asc("_panel_vote_iso"))
    top_iso = (per_iso.withColumn("_rk", F.row_number().over(w_iso)).where(F.col("_rk") == 1)
               .select("channel_id", F.col("_panel_vote_iso").alias("panel_majority_vote_iso"), "n_votes"))
    # Full winning label among the winning-ISO voters (mode; tie-break by confidence). Preserves script
    # (e.g. hin_Deva vs hin_Latn) and the side fields, not just the base ISO.
    _conf_rank = F.when(F.col("confidence") == "high", 3).when(F.col("confidence") == "medium", 2).when(F.col("confidence") == "low", 1).otherwise(0)
    _empty_string_array = F.from_json(F.lit("[]"), ArrayType(StringType()))
    winners = votes.join(top_iso, on="channel_id", how="inner").where(F.col("_panel_vote_iso") == F.col("panel_majority_vote_iso"))
    lbl = winners.groupBy("channel_id", "primary_language_label").agg(
        F.count(F.lit(1)).alias("lbl_n"),
        F.max(_conf_rank).alias("conf_rank"),
        F.first("primary_language_script", ignorenulls=True).alias("panel_language_script_from_model"),
        F.first("pred_normalized_language_label", ignorenulls=True).alias("panel_normalized_language_label_from_model"),
        F.first("secondary_language_label", ignorenulls=True).alias("panel_secondary_language_label"),
        F.first("dialect_or_variant", ignorenulls=True).alias("panel_dialect_or_variant"),
        F.array_distinct(F.flatten(F.collect_list(F.coalesce(F.col("mixed_languages"), _empty_string_array)))).alias("panel_mixed_languages"),
        F.max(F.col("is_mixed_language").cast("int")).alias("_mixed_int"),
        F.max(F.col("is_romanized").cast("int")).alias("_romanized_int"),
        F.first("evidence", ignorenulls=True).alias("panel_evidence"),
    )
    w_lbl = Window.partitionBy("channel_id").orderBy(F.desc("lbl_n"), F.desc("conf_rank"), F.asc("primary_language_label"))
    full = (lbl.withColumn("_rk", F.row_number().over(w_lbl)).where(F.col("_rk") == 1)
            .withColumn("panel_confidence", F.when(F.col("conf_rank") == 3, F.lit("high"))
                        .when(F.col("conf_rank") == 2, F.lit("medium"))
                        .when(F.col("conf_rank") == 1, F.lit("low")))
            .select("channel_id", F.col("primary_language_label").alias("panel_language_label"),
                    "panel_language_script_from_model", "panel_normalized_language_label_from_model",
                    "panel_secondary_language_label",
                    "panel_dialect_or_variant", "panel_mixed_languages", "panel_confidence",
                    "_mixed_int", "_romanized_int", "panel_evidence"))
    # Per-provider labels + reach (full predictions preserved per provider).
    prov = parsed.groupBy("channel_id").agg(
        F.first(F.when((F.col("provider") == "openai") & (F.col("is_valid_panel_vote") == F.lit(True)), F.col("primary_language_label")), ignorenulls=True).alias("openai_label"),
        F.first(F.when((F.col("provider") == "anthropic") & (F.col("is_valid_panel_vote") == F.lit(True)), F.col("primary_language_label")), ignorenulls=True).alias("anthropic_label"),
        F.first(F.when((F.col("provider") == "gemini") & (F.col("is_valid_panel_vote") == F.lit(True)), F.col("primary_language_label")), ignorenulls=True).alias("gemini_label"),
        F.first(F.when((F.col("provider") == "deepseek") & (F.col("is_valid_panel_vote") == F.lit(True)), F.col("primary_language_label")), ignorenulls=True).alias("deepseek_label"),
        F.sum(F.when(F.col("is_valid_panel_vote") == F.lit(True), 1).otherwise(0)).alias("n_reached"),
        F.collect_set(F.when(F.col("is_valid_panel_vote") == F.lit(True), F.col("model"))).alias("panel_models"),
    )
    verdict = (
        routed
        .join(top_iso, on="channel_id", how="left")
        .join(vote_dist, on="channel_id", how="left")
        .join(full, on="channel_id", how="left")
        .join(prov, on="channel_id", how="left")
        .withColumn("_panel_label_iso", F.lower(F.trim(F.split(F.col("panel_language_label"), "_").getItem(0))))
        .withColumn("_panel_label_iso", F.when(F.col("_panel_label_iso").isin("", "null", "none"), F.lit(None)).otherwise(F.col("_panel_label_iso")))
        .withColumn("panel_language_iso", F.coalesce(F.col("_panel_label_iso"), F.col("panel_majority_vote_iso")))
        .withColumn("panel_language_iso639_3", F.col("panel_language_iso"))
        .withColumn("panel_language_script", F.coalesce(F.col("panel_language_script_from_model"), F.element_at(F.split("panel_language_label", "_"), 2)))
        .withColumn("panel_language_script_family", script_family_expr(F.col("panel_language_script")))
        .withColumn("panel_normalized_language_iso639_3", canonical_base_iso_expr(F.col("panel_language_iso639_3")))
        .withColumn(
            "panel_normalized_language_label",
            F.coalesce(
                F.col("panel_normalized_language_label_from_model"),
                normalized_language_label_expr(F.col("panel_language_iso639_3"), F.col("panel_language_script")),
            ),
        )
        .withColumn("panel_is_mixed_language", F.coalesce(F.col("_mixed_int") == 1, F.lit(False)))
        .withColumn("panel_is_romanized", F.coalesce(F.col("_romanized_int") == 1, F.lit(False)))
        .withColumn(
            "panel_majority_threshold",
            F.when(
                F.lit(PANEL_MAJORITY_MODE) == F.lit("reached_models"),
                F.greatest(
                    F.lit(MIN_PANEL_VOTES_FOR_MAJORITY),
                    (F.floor(F.coalesce(F.col("n_reached"), F.lit(0)) / F.lit(2)) + F.lit(1)).cast("int"),
                ),
            ).otherwise(F.lit(configured_majority_threshold)),
        )
        .withColumn("panel_majority_mode", F.lit(PANEL_MAJORITY_MODE))
        .withColumn("panel_majority_vote_basis", F.lit(PANEL_MAJORITY_VOTE_BASIS))
        .withColumn("panel_status", F.when(F.col("panel_majority_vote_iso").isNull(), F.lit("no_panel_result"))
                    .when(
                        (F.coalesce(F.col("n_reached"), F.lit(0)) >= F.lit(MIN_PANEL_VOTES_FOR_MAJORITY))
                        & (F.col("n_votes") >= F.col("panel_majority_threshold")),
                        F.lit("panel_majority"),
                    )
                    .otherwise(F.lit("needs_human_review")))
        .withColumn("audit_sample", F.col("route_reason") == F.lit("agreement_audit"))
        # Audit rows are measurements: never overwrite consensus unless explicitly promoted later.
        .withColumn("consensus_source", F.when(F.col("audit_sample"), F.lit("audit_sample"))
                    .when(F.col("panel_status") == F.lit("panel_majority"), F.lit("llm_panel"))
                    .otherwise(F.lit("human_review")))
        .withColumn("prediction_timestamp", F.current_timestamp())
        .withColumn("run_id", F.lit(RUN_ID))
        .drop("_mixed_int", "_romanized_int", "_panel_label_iso", "panel_language_script_from_model", "panel_normalized_language_label_from_model")
    )
    write_run_scoped(verdict, panel_verdicts_full)
    print("Wrote panel verdicts to", panel_verdicts_full)

    # D4 acceptance: exactly one verdict row per routed channel (no fan-out from joins, none dropped).
    n_verdict = spark.table(panel_verdicts_full).where(F.col("run_id") == F.lit(RUN_ID)).count()
    print(f"Coverage: routed={n_routed:,}  verdict_rows={n_verdict:,}")
    assert n_verdict == n_routed, "Verdict rows must equal routed channels (one row per routed channel)."

    display(verdict.groupBy("route_reason", "panel_status").count().orderBy("route_reason", "panel_status"))

    # Audit read-out: blind agreement sample — how often does the panel disagree with the fastText consensus?
    audit = verdict.where(F.col("audit_sample"))
    if audit.limit(1).count() > 0:
        audit_eval = audit.withColumn(
            "panel_agrees_consensus",
            canonical_base_iso_expr(F.lower(F.split(F.coalesce(F.col("consensus_language_label"), F.lit("")), "_").getItem(0)))
            == F.col("panel_normalized_language_iso639_3"),
        )
        print("Agreement-bucket audit (panel vs fastText consensus):")
        display(audit_eval.groupBy("panel_agrees_consensus").count())
else:
    print("import_results=false — set it true after downloading provider result JSONL files into results_input_dir.")
