# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube LID v3 — LLM adjudication panel (companion to notebook 01)
# MAGIC
# MAGIC **Run order:** run `01_language_openlid_v3_databricks` first (writes the `yt_lid_v3_*` tables),
# MAGIC then run this notebook. This notebook does **not** re-run the fastText models.
# MAGIC
# MAGIC **What it does:** by default, routes the small subset of channels where the two fastText models
# MAGIC *disagree* (plus a tiny blind *audit* sample of the agreement bucket) to a multi-model LLM panel.
# MAGIC Set `route_unclassified=true` when the LLM should act as the final fallback for channels that LID
# MAGIC left unclassified; on full-crawl runs this can be hundreds of thousands of channels, so it is explicit.
# MAGIC For API/secrets validation, set `routing_mode=random_validation` to classify a reproducible random
# MAGIC sample from the notebook 01 comparison table. Set `random_validation_scope=lid_iso_disagreement` to
# MAGIC draw that sample only from cases where OpenLID and GlotLID disagree after ISO normalization.
# MAGIC The panel adjudicates written-metadata language and
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
import sys
import time
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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
_create_text_widget("channels_table", "yt_lid_v3_channels")
_create_text_widget("channel_text_features_table", "yt_lid_v3_channel_text_features")
_create_text_widget("hindi_indic_audit_table", "yt_lid_v3_hindi_indic_audit_candidates")
_create_text_widget("source_channels_table", "")  # optional raw fallback for routed channels missing segments
_create_text_widget("source_videos_table", "")  # optional raw fallback for routed channels missing segments
_create_text_widget("run_id", "default")
_create_text_widget("source_run_id", "")  # blank = use run_id; set for retry/output-only runs
_create_text_widget("inference_hash_buckets", "4096")

# Output tables.
_create_text_widget("panel_requests_table", "yt_lid_v3_llm_panel_requests")
_create_text_widget("panel_batch_jobs_table", "yt_lid_v3_llm_panel_batch_jobs")
_create_text_widget("panel_raw_results_table", "yt_lid_v3_llm_panel_raw_results")
_create_text_widget("panel_verdicts_table", "yt_lid_v3_llm_panel_verdicts")
_create_text_widget("panel_model_agreement_table", "yt_lid_v3_llm_panel_model_agreement")
_create_text_widget("panel_run_progress_table", "yt_lid_v3_llm_panel_run_progress")

# --- Routing controls ---
_create_text_widget("routing_mode", "residual_panel")  # residual_panel | random_validation
_create_text_widget("random_validation_scope", "all")  # all | lid_iso_disagreement
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
# Final-arbiter route: channels left unclassified by the LID pass. This can be large on full-crawl runs,
# so keep it explicit; set true when DeepSeek/LLM is intended to be the final fallback.
_create_text_widget("route_unclassified", "false")
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
_create_text_widget("prompt_best_guess_mode", "true")
_create_text_widget("prompt_version", "llm_fallback_final_guardrails_post_review_20260630")
_create_text_widget("apply_llm_calibration", "true")

# Models: mix frontier/mid/small providers by default for validation agreement matrices.
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
_create_text_widget("models_json", DEFAULT_MODELS_JSON)
_create_text_widget("max_output_tokens", "2000")
_create_text_widget("temperature", "")  # blank = provider default
_create_text_widget("openai_endpoint_mode", "auto")
_create_text_widget("openai_reasoning_effort", "none")  # blank = omit reasoning controls; none = thinking off
_create_text_widget("openai_reasoning_effort_by_model_json", "{}")
_create_text_widget("gemini_thinking_level", "low")  # blank = omit thinking controls
_create_text_widget("deepseek_thinking_type", "disabled")  # disabled | enabled | blank to omit
_create_text_widget("deepseek_reasoning_effort", "")  # low | medium | high | max | xhigh; only used with enabled thinking
_create_text_widget("deepseek_max_output_tokens", "600")
_create_text_widget("deepseek_max_workers", "16")
_create_text_widget("deepseek_request_timeout_seconds", "60")
_create_text_widget("deepseek_max_retries", "1")
_create_text_widget("deepseek_direct_streaming", "false")
_create_text_widget("deepseek_delete_request_jsonl_after_submit", "false")

# Batch I/O.
_create_text_widget("batch_output_dir", "/dbfs/FileStore/youtube_lid_panel_batches")
_create_text_widget("max_requests_per_file", "10000")
_create_text_widget("submit_batches", "false")
_create_text_widget("submit_provider_filter", "")  # blank = all; comma-separated provider names
_create_text_widget("submit_model_filter", "")  # blank = all; comma-separated model names
_create_text_widget("skip_existing_submitted_batches", "true")
_create_text_widget("import_results", "false")
_create_text_widget("reuse_existing_requests_on_import", "true")
_create_text_widget("reuse_existing_requests_on_submit", "true")
_create_text_widget("refresh_request_provider_filter", "")  # blank = none; comma-separated providers to rebuild batch_line from stored prompts
_create_text_widget("refresh_request_model_filter", "")  # blank = all models for refreshed providers
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
CHANNELS_TABLE = _get_widget("channels_table", "yt_lid_v3_channels")
CHANNEL_TEXT_FEATURES_TABLE = _get_widget("channel_text_features_table", "yt_lid_v3_channel_text_features")
HINDI_INDIC_AUDIT_TABLE = _get_widget("hindi_indic_audit_table", "yt_lid_v3_hindi_indic_audit_candidates")
SOURCE_CHANNELS_TABLE = _get_widget("source_channels_table", "").strip()
SOURCE_VIDEOS_TABLE = _get_widget("source_videos_table", "").strip()
RUN_ID = _get_widget("run_id", "default").strip() or "default"
SOURCE_RUN_ID = _get_widget("source_run_id", "").strip() or RUN_ID
INFERENCE_HASH_BUCKETS = _get_int_widget("inference_hash_buckets", 4096)

PANEL_REQUESTS_TABLE = _get_widget("panel_requests_table", "yt_lid_v3_llm_panel_requests")
PANEL_BATCH_JOBS_TABLE = _get_widget("panel_batch_jobs_table", "yt_lid_v3_llm_panel_batch_jobs")
PANEL_RAW_RESULTS_TABLE = _get_widget("panel_raw_results_table", "yt_lid_v3_llm_panel_raw_results")
PANEL_VERDICTS_TABLE = _get_widget("panel_verdicts_table", "yt_lid_v3_llm_panel_verdicts")
PANEL_MODEL_AGREEMENT_TABLE = _get_widget("panel_model_agreement_table", "yt_lid_v3_llm_panel_model_agreement")
PANEL_RUN_PROGRESS_TABLE = _get_widget("panel_run_progress_table", "yt_lid_v3_llm_panel_run_progress")

ROUTING_MODE = _get_widget("routing_mode", "residual_panel").strip().lower()
RANDOM_VALIDATION_SCOPE = _get_widget("random_validation_scope", "all").strip().lower()
RANDOM_VALIDATION_SAMPLE_SIZE = _get_int_widget("random_validation_sample_size", 1000)
RANDOM_VALIDATION_SEED = _get_widget("random_validation_seed", "20260610").strip()
ROUTE_DISAGREEMENT = _get_bool_widget("route_disagreement", True)
ROUTE_UNRESOLVED_TAIL = _get_bool_widget("route_unresolved_tail", True)
ROUTE_SHARED_BIAS = _get_bool_widget("route_shared_bias_english_indic", True)
ROUTE_UNCLASSIFIED = _get_bool_widget("route_unclassified", False)
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
PROMPT_BEST_GUESS_MODE = _get_bool_widget("prompt_best_guess_mode", True)
PROMPT_VERSION = _get_widget("prompt_version", "llm_fallback_final_guardrails_post_review_20260630").strip()
APPLY_LLM_CALIBRATION = _get_bool_widget("apply_llm_calibration", True)

MODELS = json.loads(_get_widget("models_json", DEFAULT_MODELS_JSON))
MAX_OUTPUT_TOKENS = _get_int_widget("max_output_tokens", 2000)
TEMPERATURE = _get_optional_float_widget("temperature", None)
OPENAI_ENDPOINT_MODE = _get_widget("openai_endpoint_mode", "auto").strip().lower()
OPENAI_REASONING_EFFORT = _get_widget("openai_reasoning_effort", "none").strip().lower()
OPENAI_REASONING_EFFORT_BY_MODEL_JSON = _get_widget("openai_reasoning_effort_by_model_json", "{}").strip()
GEMINI_THINKING_LEVEL = _get_widget("gemini_thinking_level", "low").strip()
DEEPSEEK_THINKING_TYPE = _get_widget("deepseek_thinking_type", "disabled").strip().lower()
DEEPSEEK_REASONING_EFFORT = _get_widget("deepseek_reasoning_effort", "").strip().lower()
DEEPSEEK_MAX_OUTPUT_TOKENS = _get_int_widget("deepseek_max_output_tokens", 600)
DEEPSEEK_MAX_WORKERS = _get_int_widget("deepseek_max_workers", 16)
DEEPSEEK_REQUEST_TIMEOUT_SECONDS = _get_float_widget("deepseek_request_timeout_seconds", 60.0)
DEEPSEEK_MAX_RETRIES = _get_int_widget("deepseek_max_retries", 1)
DEEPSEEK_DIRECT_STREAMING = _get_bool_widget("deepseek_direct_streaming", False)
DEEPSEEK_DELETE_REQUEST_JSONL_AFTER_SUBMIT = _get_bool_widget("deepseek_delete_request_jsonl_after_submit", False)

BATCH_OUTPUT_DIR = _get_widget("batch_output_dir", "/dbfs/FileStore/youtube_lid_panel_batches")
MAX_REQUESTS_PER_FILE = _get_int_widget("max_requests_per_file", 10000)
SUBMIT_BATCHES = _get_bool_widget("submit_batches", False)
SUBMIT_PROVIDER_FILTER_RAW = _get_widget("submit_provider_filter", "").strip().lower()
SUBMIT_PROVIDER_FILTER = {p.strip() for p in SUBMIT_PROVIDER_FILTER_RAW.split(",") if p.strip()}
SUBMIT_MODEL_FILTER_RAW = _get_widget("submit_model_filter", "").strip()
SUBMIT_MODEL_FILTER = {p.strip() for p in SUBMIT_MODEL_FILTER_RAW.split(",") if p.strip()}
SKIP_EXISTING_SUBMITTED_BATCHES = _get_bool_widget("skip_existing_submitted_batches", True)
IMPORT_RESULTS = _get_bool_widget("import_results", False)
REUSE_EXISTING_REQUESTS_ON_IMPORT = _get_bool_widget("reuse_existing_requests_on_import", True)
REUSE_EXISTING_REQUESTS_ON_SUBMIT = _get_bool_widget("reuse_existing_requests_on_submit", True)
REFRESH_REQUEST_PROVIDER_FILTER_RAW = _get_widget("refresh_request_provider_filter", "").strip().lower()
REFRESH_REQUEST_PROVIDER_FILTER = {
    p.strip() for p in REFRESH_REQUEST_PROVIDER_FILTER_RAW.split(",") if p.strip()
}
REFRESH_REQUEST_MODEL_FILTER_RAW = _get_widget("refresh_request_model_filter", "").strip()
REFRESH_REQUEST_MODEL_FILTER = {
    p.strip() for p in REFRESH_REQUEST_MODEL_FILTER_RAW.split(",") if p.strip()
}
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


def _quote_table_part(part: str) -> str:
    cleaned = str(part or "").strip().strip("`")
    return "`" + cleaned.replace("`", "``") + "`"


def fqtn(table: str) -> str:
    value = str(table or "").strip()
    if not value:
        raise ValueError("Table name cannot be blank.")
    if "`.`" in value and value.startswith("`") and value.endswith("`"):
        return value
    parts = [p for p in value.split(".") if p]
    if len(parts) == 3:
        return ".".join(_quote_table_part(p) for p in parts)
    if len(parts) == 2:
        return ".".join([_quote_table_part(CATALOG)] + [_quote_table_part(p) for p in parts])
    if len(parts) == 1:
        return ".".join(_quote_table_part(p) for p in [CATALOG, SCHEMA, parts[0]])
    raise ValueError(f"Unsupported table identifier: {table!r}")


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
channels_full = fqtn(CHANNELS_TABLE)
channel_text_features_full = fqtn(CHANNEL_TEXT_FEATURES_TABLE)
hindi_indic_audit_full = fqtn(HINDI_INDIC_AUDIT_TABLE)
source_channels_full = fqtn(SOURCE_CHANNELS_TABLE) if SOURCE_CHANNELS_TABLE else None
source_videos_full = fqtn(SOURCE_VIDEOS_TABLE) if SOURCE_VIDEOS_TABLE else None
panel_requests_full = fqtn(PANEL_REQUESTS_TABLE)
panel_batch_jobs_full = fqtn(PANEL_BATCH_JOBS_TABLE)
panel_raw_results_full = fqtn(PANEL_RAW_RESULTS_TABLE)
panel_verdicts_full = fqtn(PANEL_VERDICTS_TABLE)
panel_model_agreement_full = fqtn(PANEL_MODEL_AGREEMENT_TABLE)
panel_batch_files_full = fqtn(PANEL_REQUESTS_TABLE + "_batch_files")
panel_progress_full = fqtn(PANEL_RUN_PROGRESS_TABLE)

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


_PANEL_PROGRESS_STATE = {"stage": "initializing", "status": "starting"}
if not hasattr(sys, "_yt_lid_panel_original_excepthook"):
    sys._yt_lid_panel_original_excepthook = sys.excepthook


def _progress_value(value: object) -> Optional[str]:
    if value is None:
        return None
    return str(value)[:4000]


def record_panel_progress(stage: str, status: str = "ok", metrics: Optional[Dict[str, object]] = None) -> None:
    _PANEL_PROGRESS_STATE["stage"] = stage
    _PANEL_PROGRESS_STATE["status"] = status
    rows = [
        (
            RUN_ID,
            SOURCE_RUN_ID,
            INFERENCE_HASH_BUCKETS,
            stage,
            status,
            str(k),
            _progress_value(v),
        )
        for k, v in (metrics or {"event": "1"}).items()
    ]
    progress_df = (
        spark.createDataFrame(
            rows,
            "run_id string, source_run_id string, inference_hash_buckets int, "
            "stage string, status string, metric string, value string",
        )
        .withColumn("event_timestamp", F.current_timestamp())
    )
    try:
        (
            progress_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(panel_progress_full)
        )
        print(f"Panel progress: {stage} [{status}] -> {panel_progress_full}")
    except Exception as exc:  # noqa: BLE001 - progress logging must not be able to kill the run
        print(f"WARNING: failed to record panel progress stage={stage!r} status={status!r}: {exc}")


def _record_uncaught_panel_failure(exc_type, exc_value, exc_traceback) -> None:
    if getattr(sys, "_yt_lid_panel_recording_uncaught_failure", False):
        sys._yt_lid_panel_original_excepthook(exc_type, exc_value, exc_traceback)
        return
    sys._yt_lid_panel_recording_uncaught_failure = True
    try:
        record_panel_progress(
            "notebook_failed",
            status="error",
            metrics={
                "last_progress_stage": _PANEL_PROGRESS_STATE.get("stage"),
                "last_progress_status": _PANEL_PROGRESS_STATE.get("status"),
                "exception_type": getattr(exc_type, "__name__", str(exc_type)),
                "exception_message": str(exc_value),
            },
        )
    except Exception as progress_exc:  # noqa: BLE001 - never mask the original notebook failure
        print(f"WARNING: failed to persist panel uncaught-failure progress marker: {progress_exc}")
    finally:
        sys._yt_lid_panel_recording_uncaught_failure = False
        sys._yt_lid_panel_original_excepthook(exc_type, exc_value, exc_traceback)


sys.excepthook = _record_uncaught_panel_failure


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
        record_panel_progress(
            "delta_write_committed",
            metrics={"table": table_full, "replace_where": "<new_table>", "partition_cols": ",".join(parts)},
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
    record_panel_progress(
        "delta_write_committed",
        metrics={"table": table_full, "replace_where": f"run_id = {RUN_ID}", "partition_cols": ",".join(parts)},
    )


record_panel_progress(
    "configured",
    metrics={
        "comparison_table": comparison_full,
        "segments_input_table": segments_input_full,
        "channels_table": channels_full,
        "source_channels_table": source_channels_full,
        "source_videos_table": source_videos_full,
        "source_run_id": SOURCE_RUN_ID,
        "run_id": RUN_ID,
        "routing_mode": ROUTING_MODE,
        "route_unclassified": ROUTE_UNCLASSIFIED,
        "prompt_version": PROMPT_VERSION,
        "apply_llm_calibration": APPLY_LLM_CALIBRATION,
        "panel_requests_table": panel_requests_full,
        "panel_raw_results_table": panel_raw_results_full,
        "panel_verdicts_table": panel_verdicts_full,
    },
)

# Arabic macrolanguage + dialects collapsed to one family for the "exclude taxonomy artifact" filter.
ARABIC_FAMILY_ISO = {"ara", "arb", "ary", "arz", "arq", "apc", "ars", "ajp", "aeb", "acm", "acq", "aec", "afb", "ayl", "ayn"}
# Codes such as und/zxx/mul are abstentions for this panel. `inc` is a collective
# Indo-Aryan family code, not a specific channel-language label.
NON_LANGUAGE_BASE_ISO = {"und", "zxx", "mul", "mis", "inc"}
# South-Asian source language codes used to flag the romanized-Indic shared-bias route (D3).
SOURCE_INDIC_CODES = {"hi", "hi-in", "hin", "ne", "ne-np", "nep", "npi", "bho", "ur", "ur-pk", "pa", "gu", "mr", "bn", "ta", "te", "kn", "ml", "or", "si"}
CANONICAL_BASE_ISO = {
    # Project-level taxonomy aliases surfaced in the 1k LLM validation disagreement audit.
    "ar": "ara",
    "arabic": "ara",
    "bengali": "ben",
    "bosnian": "bos",
    "braj": "bra",
    "brij": "bra",
    "bundeli": "bns",
    "bundelkhandi": "bns",
    "bundleli": "bns",
    "cantonese": "yue",
    "chinese": "cmn",
    "croatian": "hrv",
    "deutsch": "deu",
    "english": "eng",
    "filipino": "fil",
    "french": "fra",
    "german": "deu",
    "hindi": "hin",
    "haryanvi": "bgc",
    "gujarati": "guj",
    "guj": "guj",
    "hindko": "hnd",
    "hnd": "hnd",
    "indonesian": "ind",
    "javanese": "jav",
    "jv": "jav",
    "jw": "jav",
    "japanese": "jpn",
    "kannada": "kan",
    "kashmiri": "kas",
    "kas": "kas",
    "khasi": "kha",
    "korean": "kor",
    "kumaoni": "kfy",
    "garhwali": "gbm",
    "kutchi": "kfr",
    "kachchi": "kfr",
    "kutch": "kfr",
    "kfr": "kfr",
    "marwari": "mwr",
    "malay": "zsm",
    "mandarin": "cmn",
    "marathi": "mar",
    "nagpuri": "sck",
    "odia": "ory",
    "pashtun": "pus",
    "sadani": "sck",
    "sadri": "sck",
    "pashto": "pus",
    "portuguese": "por",
    "punjabi": "pan",
    "rajasthani": "raj",
    "russian": "rus",
    "serbian": "srp",
    "serbo-croatian": "hbs",
    "serbocroatian": "hbs",
    "bcs": "hbs",
    "spanish": "spa",
    "tamil": "tam",
    "telugu": "tel",
    "tulu": "tcy",
    "tcy": "tcy",
    "ukrainian": "ukr",
    "urdu": "urd",
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
    "pas": "pus",
    "zh": "cmn",
    "ku": "kmr",
    "kur": "kmr",
    "kmr": "kmr",
}

SCRIPT_FAMILY_CANONICAL = {
    "arab": "Arab",
    "arabic": "Arab",
    "beng": "Beng",
    "bengali": "Beng",
    "cyrl": "Cyrl",
    "cyrillic": "Cyrl",
    "deva": "Deva",
    "devanagari": "Deva",
    "greek": "Grek",
    "grek": "Grek",
    "gujr": "Gujr",
    "gujarati": "Gujr",
    "guru": "Guru",
    "gurmukhi": "Guru",
    "han": "Hani",
    "hani": "Hani",
    "hans": "Hani",
    "hant": "Hani",
    "hang": "Hang",
    "hangul": "Hang",
    "hebr": "Hebr",
    "hebrew": "Hebr",
    "japanese": "Jpan",
    "jpan": "Jpan",
    "kana": "Jpan",
    "kannada": "Knda",
    "khmer": "Khmr",
    "khmr": "Khmr",
    "knda": "Knda",
    "korean": "Hang",
    "lao": "Laoo",
    "laoo": "Laoo",
    "latin": "Latn",
    "latn": "Latn",
    "malayalam": "Mlym",
    "mlym": "Mlym",
    "odia": "Orya",
    "oriya": "Orya",
    "orya": "Orya",
    "sinh": "Sinh",
    "sinhala": "Sinh",
    "tamil": "Taml",
    "taml": "Taml",
    "telugu": "Telu",
    "telu": "Telu",
    "thai": "Thai",
}


def canonical_base_iso_expr(col):
    iso = F.lower(F.trim(col.cast("string")))
    iso = F.when(iso.isin("", "null", "none"), F.lit(None).cast("string")).otherwise(iso)
    iso = F.when(iso.isin(*sorted(ARABIC_FAMILY_ISO)), F.lit("ara")).otherwise(iso)
    mapping = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in CANONICAL_BASE_ISO.items()], []))
    mapped = F.coalesce(F.element_at(mapping, iso), iso)
    return (
        F.when(mapped.isin(*sorted(NON_LANGUAGE_BASE_ISO)), F.lit(None).cast("string"))
        .when(mapped.rlike("^[a-z]{3}$"), mapped)
        .otherwise(F.lit(None).cast("string"))
    )


def safe_split_get_expr(col, pattern: str, zero_based_index: int):
    padding = F.array(*[F.lit(None).cast("string") for _ in range(zero_based_index + 1)])
    parts = F.concat(F.split(col.cast("string"), pattern), padding)
    return parts.getItem(zero_based_index)


def script_from_label_expr(label_col):
    script = safe_split_get_expr(label_col, "_", 1)
    return F.when(script.isin("", "null", "none"), F.lit(None).cast("string")).otherwise(script)


def script_family_expr(script_col):
    raw = F.trim(script_col.cast("string"))
    raw_l = F.lower(raw)
    mapping = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in SCRIPT_FAMILY_CANONICAL.items()], []))
    mapped = F.element_at(mapping, raw_l)
    return (
        F.when(raw_l.isin("", "null", "none"), F.lit(None).cast("string"))
        .when(mapped.isNotNull(), mapped)
        .when(raw.rlike("^[A-Z][a-z]{3}$"), raw)
        .otherwise(F.lit(None).cast("string"))
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
if RANDOM_VALIDATION_SCOPE not in {"all", "lid_iso_disagreement"}:
    raise ValueError("random_validation_scope must be all or lid_iso_disagreement")
if RANDOM_VALIDATION_SAMPLE_SIZE < 1:
    raise ValueError("random_validation_sample_size must be positive")
OPENAI_REASONING_EFFORT_VALUES = {"none", "minimal", "low", "medium", "high", "xhigh"}
try:
    OPENAI_REASONING_EFFORT_BY_MODEL = {
        str(k): str(v).strip().lower()
        for k, v in json.loads(OPENAI_REASONING_EFFORT_BY_MODEL_JSON or "{}").items()
        if str(v).strip()
    }
except Exception as exc:
    raise ValueError(f"openai_reasoning_effort_by_model_json must be a JSON object: {exc}") from exc
if OPENAI_REASONING_EFFORT and OPENAI_REASONING_EFFORT not in OPENAI_REASONING_EFFORT_VALUES:
    raise ValueError("openai_reasoning_effort must be blank, none, minimal, low, medium, high, or xhigh")
bad_openai_model_efforts = {
    model: effort for model, effort in OPENAI_REASONING_EFFORT_BY_MODEL.items()
    if effort not in OPENAI_REASONING_EFFORT_VALUES
}
if bad_openai_model_efforts:
    raise ValueError(f"Unsupported openai_reasoning_effort_by_model_json values: {bad_openai_model_efforts}")
if DEEPSEEK_THINKING_TYPE not in {"", "enabled", "disabled"}:
    raise ValueError("deepseek_thinking_type must be blank, enabled, or disabled")
DEEPSEEK_REASONING_EFFORT_VALUES = {"low", "medium", "high", "max", "xhigh"}
if DEEPSEEK_REASONING_EFFORT and DEEPSEEK_REASONING_EFFORT not in DEEPSEEK_REASONING_EFFORT_VALUES:
    raise ValueError("deepseek_reasoning_effort must be blank, low, medium, high, max, or xhigh")
if DEEPSEEK_REASONING_EFFORT and DEEPSEEK_THINKING_TYPE != "enabled":
    raise ValueError("deepseek_reasoning_effort requires deepseek_thinking_type=enabled")
if DEEPSEEK_MAX_OUTPUT_TOKENS < 1:
    raise ValueError("deepseek_max_output_tokens must be at least 1")
DEEPSEEK_MODELS_IN_PANEL = [
    str(m.get("model", "")).strip()
    for m in MODELS
    if str(m.get("provider", "")).strip().lower() == "deepseek"
]
if DEEPSEEK_THINKING_TYPE == "enabled":
    deepseek_thinking_min_tokens = 4000 if any("pro" in m.lower() for m in DEEPSEEK_MODELS_IN_PANEL) else 2000
    if DEEPSEEK_MAX_OUTPUT_TOKENS < deepseek_thinking_min_tokens:
        raise ValueError(
            f"deepseek_max_output_tokens must be at least {deepseek_thinking_min_tokens} when "
            "deepseek_thinking_type=enabled for the selected DeepSeek models; reasoning tokens share "
            "the output cap and lower caps truncate the final JSON."
        )
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
print(
    "Prompt cleanup: strip_boilerplate=", STRIP_PROMPT_BOILERPLATE,
    "| dedupe_segments=", DEDUPE_PROMPT_SEGMENTS,
    "| best_guess_mode=", PROMPT_BEST_GUESS_MODE,
    "| prompt_version=", PROMPT_VERSION,
    "| apply_llm_calibration=", APPLY_LLM_CALIBRATION,
)
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
    print(f"Random validation sample: n={RANDOM_VALIDATION_SAMPLE_SIZE:,}, seed={RANDOM_VALIDATION_SEED}, scope={RANDOM_VALIDATION_SCOPE}")
else:
    print("Routes -> disagreement:", ROUTE_DISAGREEMENT, "| unresolved_tail:", ROUTE_UNRESOLVED_TAIL,
          "| shared_bias_english_indic:", ROUTE_SHARED_BIAS, "| unclassified:", ROUTE_UNCLASSIFIED,
          "| agreement_audit:", ROUTE_AGREEMENT_AUDIT,
          f"({AGREEMENT_AUDIT_FRACTION:.4f})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. System prompt (batch-adapted classifier spec)
# MAGIC
# MAGIC Mirrors `validation/llm_panel_classifier_prompt.md`, but the model judges from the metadata supplied
# MAGIC in the user prompt rather than fetching live (batch APIs cannot browse).

# COMMAND ----------
SYSTEM_PROMPT = """EXECUTION CONTEXT: You cannot browse, search, or retrieve pages. Classify only from metadata supplied in this prompt. If no channel-level metadata text is supplied, return status="insufficient_text" with null language fields and confidence=null; never infer from the channel ID. The only valid statuses are classified and insufficient_text.

ROLE: You are an independent, evidence-driven language classifier for YouTube channels. You are one member of a panel that adjudicates cases where a two-model machine pipeline (OpenLID-v3 + GlotLID) disagrees. Judge ONLY from the channel metadata supplied below; do not assume what the channel "probably" is, and do not consider any other model's guess.

OBJECTIVE: determine the dominant WRITTEN-METADATA language — the language of the channel name, description, and video titles/descriptions provided. This is NOT the spoken language and NOT the creator's nationality. A channel filmed in Hindi can have English-written metadata; classify the WRITING.

DECISION ORDER:
1. The label script is the script of the highest-tier decisive evidence. This works both ways: coherent Devanagari/Cyrillic/etc. prose uses the native script label (e.g. hin_Deva, uzb_Cyrl), while romanized text with no decisive native script stays _Latn with is_romanized=true. Numeric field weights are tie-breakers only and never override the tier hierarchy. Generic English channel/about text, contact/support text, upload/category descriptions, and media scaffolding do not outrank repeated non-generic native-script title phrases or native-script description phrases.
2. Apply the minimum-evidence gate before guessing. Running prose in a language can support a low-confidence label; repeated or field-level short real English phrases can support eng_Latn with low confidence. Only names, handles, dates, single words, generic hashtags, topic/language names, or CTA/SEO boilerplate means insufficient_text.
3. Hindi-belt regional codes (bho, bgc, hne, sck, raj, mwr) require real lect-specific phrase markers; region/genre/artist names or hashtags are not enough, so default to hin_Deva/hin_Latn or the actual phrase language.
4. A language name or region tag such as "Tamil", "Bhojpuri", "Urdu translation", or "Telugu songs" is topic routing metadata, never a primary label by itself; use only as secondary/low-confidence support when another supplied cue agrees.

LABEL FORMAT: use the pipeline's internal "<ISO 639-3>_<ISO 15924>" format, e.g. eng_Latn, spa_Latn, hin_Deva, ara_Arab, cmn_Hani, tha_Thai, kor_Hang. This is not standard BCP-47: always use the three-letter code and underscore. primary_language_label is the full label (hin_Deva), primary_language_iso639_3 is the code only (hin), and primary_language_script is the script only (Deva). Always include the script for classified rows. If a non-Latin language is written in Latin letters (romanization), label it with _Latn and set is_romanized=true (e.g. romanized Hindi = hin_Latn). Use ISO codes only; never output English names such as hindi_Deva, korean_Hangul, or punjabi_Latn. For insufficient_text, set language fields and confidence to null.

WEIGH evidence quality before field weights. Use this priority order: (1) substantive non-boilerplate description prose about the channel's actual content/message, (2) coherent title phrases, (3) repeated non-generic phrases, (4) localized date/month cues, (5) non-generic hashtags, (6) channel name, (7) generic English/SEO/CTA/channel-about boilerplate. Lower tiers do not override higher tiers. The label's script is set by the highest-tier decisive evidence; numeric field weights never promote romanized titles over coherent native-script description prose. If high-quality tiers strongly conflict, choose the dominant written metadata language and preserve the other as secondary/mixed. Use field weights only as tie-breakers among comparable-quality evidence: video_title (2.0), video_description (1.0), channel_description (1.0), channel_name (0.25). A single field is decisive only with enough clean letters (>=40 Latin / >=12 non-Latin), but short grammatical sentences, repeated short titles, repeated localized date/month strings, repeated non-generic hashtags, repeated short English noun/verb phrases, and repeated short non-Latin snippets can collectively support a low- or medium-confidence channel-language guess. Treat generic provider metadata, release metadata, production credits, URLs, social links, query/tag lists, repeated near-duplicate template descriptions, title translations, proper-name credit blocks, episode/review/fancam/game/cartoon shell labels, and English scaffolding like "Official Video", "Full Natok", "Clip Officiel", "Presenting the new drama", "Cast", or "Produced by" as weak evidence. Count repeated boilerplate/template descriptions once, not once per video. A substantive channel or video description with sentence-like prose about the actual content/message should outweigh noisy repeated hashtags, dates, language-name tags, and SEO lists. A grammatical description is not Tier 1 if it is only welcome/support/contact/business, upload schedule, channel-purpose/category, or CTA text; treat that as lower-tier boilerplate.

USE SUMMARIES CAREFULLY: EVIDENCE PRIORITY SUMMARY, FIELD SUMMARY, SEGMENT SCRIPT SUMMARY, TEXT SCRIPT SUMMARY, SHORT SENTENCE/PHRASE CUES, COHERENT DESCRIPTION PROSE, CTA/CHANNEL BOILERPLATE, ROMANIZED SOUTH ASIAN CUES, ARABIC-SCRIPT URDU/PUNJABI CUES, TOPIC/LANGUAGE-NAME MENTIONS, LANGUAGE HINTS, NON-GENERIC HASHTAGS, LOCALIZED DATE/MONTH CUES, and REPEATED PATTERNS describe the supplied metadata after cleanup. Use them to notice mixed-script, romanized, hashtag-locked, date-only, or repeated short-title evidence. Do not classify from a single hint, hashtag, location, artist name, or channel name, but if multiple weak cues point to the same language or mutually intelligible family, make the best low-confidence guess rather than treating the channel as textless.

GUARD against known failure modes:
- LATIN-NAME TRAP: do not let an English/Latin channel NAME override video titles that are mostly non-Latin. If titles are mostly Thai/Korean/Arabic/etc., that is the language even when the brand name is Latin.
- FASTTEXT-INELIGIBLE IS NOT TEXTLESS: snippets tagged [fasttext-ineligible-visible-text: ...] failed a short-text/fastText eligibility rule, not a language-evidence rule. Read the visible words yourself. A short complete sentence ("Disfruta de nuestro contenido hecho para ti", "Offizieller YouTube Channel von...", "If you're here...") or repeated short phrase can justify a classified low/medium-confidence label. Do not return insufficient_text solely because every snippet is fastText-ineligible.
- SHORT ENGLISH PHRASE RESCUE: do not abstain from eng_Latn when supplied titles/descriptions contain repeated real English phrases or a short field-level English phrase with ordinary word order, such as "Robot vs human", "water vs coconut water", "Holy Quran recitation", "Digital News Portal", "Live Stream", "one more chance", or "Free Fire New Wishlist". Use low confidence when evidence is short. This does not apply to names/handles alone, bare dates, single words, generic hashtags, language/topic tags by themselves, or CTA/SEO boilerplate such as "please support me", "subscribe", "viral shorts", "official video", "full video", "new song", "edit", or "lyrics".
- SCRIPT CONSISTENCY: the script in primary_language_label must match the highest-tier decisive written evidence you cite. If a non-Latin language is written mostly in Latin characters, label it _Latn and set is_romanized=true; do not output hin_Deva, urd_Arab, mar_Deva, kan_Knda, or similar native-script labels when the decisive text is romanized Latin. Conversely, do not output hin_Latn, uzb_Latn, or similar Latin-script labels when the decisive cited evidence is coherent Devanagari, Cyrillic, Arabic, etc.; use hin_Deva, uzb_Cyrl, urd_Arab, etc. If romanized titles and a native-script description both recur, choose the primary script from the higher-tier decisive evidence and set is_mixed_language/secondary_language_label when appropriate.
- NATIVE-SCRIPT DECISIVENESS: do not let generic English channel/about descriptions, contact/support text, upload/category descriptions, or media scaffolding override repeated non-generic native-script title phrases or native-script description phrases. If the native-script evidence is coherent and the English evidence is only channel-purpose, welcome/support/contact/business, SEO, or format text, use the native-script label and cite the native-script phrases.
- DESCRIPTION PROSE VS BOILERPLATE: do not treat every grammatical channel description as Tier 1. Tier 1 description prose must say something substantive in the written language about the channel's content, message, story, claims, instructions, or topic. Descriptions limited to "welcome to my channel", "please subscribe/support", contact or promotion lines, upload/category summaries, business inquiries, social links, or generic "we make videos about..." boilerplate are lower-tier evidence and should not override stronger title/phrase evidence.
- ROMANIZED NON-LATIN: detect romanized Hindi/Urdu/Punjabi/Pashto/Arabic/Bengali/Tamil/Telugu/Malayalam/Bhojpuri/Haryanvi/Bundeli; label the underlying language with _Latn, is_romanized=true; do not default to English when the title phrases are clearly non-English. Hindi/Hinglish cues include "ke", "ki", "ka", "me/mei/main", "hai", "hoga/hogi", "hone", "ne", "se", "par", "ye/yeh", "kya", "kyu/kyun", "kaise/kase", "apka/aapka", "dil", and "sabko"; these are not Bengali. Urdu-in-Latin cues include "ki/ka/main", "duniya", "subse/sabse", "pyari", "awaz", "kase/kaise", "hoi/hui", "tabdil", "dua", "wazifa", "ishq", "naat", and "tilawat"; "Urdu translation" is a topic/label cue, not Urdu evidence by itself. Punjabi cues include "da/di/de", "sanu", "sade", "noo/nu", "ni", "ae/aiy", "wich", "mola", "ishq/ishqa", "maawan", "tayari", "wazifa", "wird"; Pakistani naat/manqabat or Lahore/Pakistan context supports pnb_Latn only with Punjabi/Shahmukhi grammar or repeated Punjabi lexical cues, not from religious genre/geography alone. Pashto cues include repeated grammar/phrases such as "da ... jwand", "sta", "zama", "sara", "kho", "peghor", or explicit Pashto/Pakhto; do not relabel Pashto-like Pakistani vines as English just because the script is Latin. Use pan_Guru/pan_Latn for Indian/Eastern Punjabi or explicit Gurmukhi/India context. Chhattisgarhi running-text markers include repeated "Mor", "Mola", "Tor", or "Ka Hoge"; labels like "Cg Song" or "Chhattisgarhi Gana" alone are genre metadata, not enough to override ordinary Hindi. Bundeli/Bundelkhand cues require direct Bundeli phrase evidence; region names, "Bundeli" labels, or "Bundelkhand" alone are topic metadata. Bhojpuri running-text markers include repeated grammar such as "ba", "bani", "badu", "tohar", "hamar", "rauwa", "saiyan", or "ka ho"; a Bhojpuri/Bhojpuriya label, #bhojpuri tag, or artist/genre cue alone is not enough when phrase text is generic Hindi/English, so keep the phrase language primary and record Bhojpuri as secondary/low confidence.
- ROMANIZED SOUTH ASIAN AMBIGUITY: script-blind Hindi/Urdu/Punjabi/Bhojpuri/Nepali evidence often deserves a best guess, but not high confidence from particles alone. If the cues distinguish only a mutually intelligible cluster, choose the most directly evidenced ISO label, use low/medium confidence, and preserve plausible close varieties in secondary_language_label, dialect_or_variant, mixed_languages, or evidence. Use npi_Latn for Nepali, not nep_Latn.
- SPARSE CUES: hashtag-only, mostly-hashtag, emoji-heavy, title-template-only, or proper-noun-only channels do not contain enough written-language evidence for a confident classification. Do not let a single channel name, handle, brand, proper name, game/media title, one short non-English item, hashtags, locations, artist names, or topic labels override repeated natural-language titles/descriptions. But if weak cues recur across several titles/descriptions/tags or localized dates and consistently point to one language/family, classify with confidence="low" or "medium" and quote the cue. Use status="insufficient_text" only when there is truly minimal language evidence after considering repeated weak signals.
- PROPER-NOUN/LITURGY TRAP: religious titles, temple/person/place names, transliterated chants, or topic labels such as "Gita", "Darbar", "Puje", "Bhagavatha", "Matha", "Teertaru", "Pravachana", "Allah", "Quran/Qur'an", "Surah", "Yasin", "Rahman", "Tilawat", "Naat", "Azan", "Islamic Knowledge", or "Masha Allah" are weak evidence by themselves. Quran/Surah/Naat/Tilawat labels describe religious subject matter unless grammar-bearing prose accompanies them. Arabic religious words in Latin script do not imply Arabic metadata unless there is Arabic-script text or grammatical Arabic phrasing. Arabic-script text may be Urdu, Punjabi/Shahmukhi, Persian, or another language; Urdu/Persian letterforms and markers such as "ک", "ی", "ے", "ہ", "گ", "کو", "کی", "کا", "کے", "میں", "والا", "والے", "ہے", "ہیں", "دینے" point away from Arabic toward Urdu/Persian-family scripts, while "ساڈی", "اے", "دا", "دی", "دے", "وچ", or "نوں" point toward Punjabi/Shahmukhi. For Islamic metadata with Urdu/Hindi connective text such as "ki", "main", "duniya", "sabse/subse", "pyari", "awaz", or "translation", classify the connective language (often urd_Latn/hin_Latn) rather than Arabic. If only names/topics are present, use insufficient_text or low/medium confidence and preserve secondary cues.
- LANGUAGE-NAME / REGION / AD-VARIANT TRAP: product/ad titles can list variants such as "Hindi 20 Sec", "Bengali 6 Sec", or "Punjabi 6 Sec" while the natural text is mostly English. Treat language names, regions, ethnicities, music/genre labels, title suffixes, hashtags, topic labels, and query lists such as "Bhojpuri", "Kashmiri funny video", "Tamil Edit", "Punjabi Status", "Urdu translation", "Telugu songs", or "Chaoui Algerian" as topic routing metadata. They are never primary-label evidence by themselves; use them only as secondary or low-confidence tie-break support when another supplied phrase/script cue agrees.
- SEO-TEMPLATE TRAP: English category words like "lyrics", "recipe", "mukbang", "ASMR", "official video", "full video", "new song", "listen/stream", and "subscribe" are often boilerplate around non-English phrase text. Do not let these terms automatically dominate repeated Hindi/Korean/Telugu/etc. phrase text; if the only non-English signal is a language name or proper noun, keep English primary and record the other cue as secondary or low confidence.
- MIXED-SCRIPT TITLE TRAP: many titles combine English media scaffolding with the real title phrase, e.g. "ASMR ... 먹방 MUKBANG, EATING", "Raghu Tarang II Quotes for Healthy Living: వండని వంటలు", or "OFFICIAL 4K VIDEO". Downweight the generic English scaffolding and classify from the recurring natural-language phrase/script across titles. Repeated English description templates should not override repeated non-English title phrases.
- TRANSLATED-TITLE TRAP: titles often pair a source-language title with an English translation after a colon/pipe, e.g. Korean/Russian/Chinese text followed by an English gloss. If the same non-English script or romanized source-language phrases recur across titles, do not count the English gloss or credit shell as equal primary-language evidence.
- MEDIA-SHELL TRAP: words such as "fancam", "behind", "performance ver.", "full episode", "promo", "preview", "review", "reaction", "cartoon", "gameplay", "nursery rhymes", "kids", and "toy" describe the video format or audience. Treat them as weak category labels unless there is enough surrounding natural-language text in English.
- TEMPLATE-DESCRIPTION TRAP: duplicated descriptions, query lists, "related tags", "your query solved", "listen here", shopping/booking blocks, and social-link blocks in any language often repeat across every video. They should not multiply the weight of English or SEO terms. Use the varied title text and the first natural-language description as stronger evidence than repeated templates.
- CTA BOILERPLATE TRAP: phrases such as "Please support me", "welcome to my channel", "thanks for watching", "subscribe to my channel", and "my new channel for live" are not enough to infer English by themselves. Treat them as boilerplate unless there is other coherent English prose.
- REGIONAL ISO TRAP: use Hindi-belt regional ISO codes only when running title/description/name text contains genuine lect-specific lexical or grammatical markers: Haryanvi=bgc; Bhojpuri=bho; Chhattisgarhi=hne; Rajasthani=raj and explicit Marwari=mwr; Nagpuri/Sadri/Sadani=sck. A region, artist/channel name, genre tag, hashtag, language name, or music label such as "Haryanvi Swad", "bhojpuri masala", "Rajasthan", "Khunti Public", "CG Song", "Sadri/Nagpuri", or "Bhojpuri" is not enough if the phrase evidence is ordinary Hindi/Hinglish/English; default to hin_Deva/hin_Latn or the actual phrase language and preserve the regional cue as dialect_or_variant or secondary evidence. Other regional cues still require direct text evidence: Bundeli/Bundelkhandi=bns; Braj/Brij/Braj Bhasha=bra; Kumaoni=kfy (not kum, which is Kumyk); Garhwali=gbm; Kashmiri=kas (not ksh, which is Kolsch); Tulu=tcy; Hindko=hnd; Kutchi/Kachchi/Kutch=kfr; Gujarati=guj; Pashto=pus (not pas); Western/Shahmukhi Punjabi=pnb and Eastern/Gurmukhi Punjabi=pan.
- BOSNIAN/CROATIAN/SERBIAN AMBIGUITY: if Latin-script Bosnian/Croatian/Serbian/Serbo-Croatian text is mutually intelligible and the supplied metadata has no decisive country, orthography, or explicit-language cue, use hbs_Latn. Use bos_Latn, hrv_Latn, or srp_Latn only when direct metadata evidence supports that specific variety, such as explicit "bosanski", "hrvatski", "srpski", "Srbija", "Hrvatska", "BiH", or clear Cyrillic Serbian. Sports/news metadata from regional outlets without direct country/language cues should normally be hbs_Latn.
- ENGLISH vs CREOLE: standard English is eng_Latn; only use jam_Latn/pcm_Latn with genuine creole grammar/lexis.
- FRENCH vs CREOLE: standard French is fra_Latn; only use gcf_Latn for Guadeloupean/Caribbean French Creole with genuine creole grammar/lexis, not merely Caribbean artists, Zouk/Kassav references, or French proper nouns.
- MINORITY OVER-PREDICTION: be conservative with rare Romance/minority tail labels (srd, ast, vec, gug, lim, scn, glg, eus); a few ambiguous Latin words are usually Spanish/Italian/Portuguese/English. Set is_high_risk_tail=true if you do assign one.

NORMALIZE TAXONOMY: report Arabic as the macrolanguage ara_Arab (put a known dialect in dialect_or_variant); use kmr_Latn for broad Kurmanji/Northern Kurdish rather than ku/kur; use cmn for Chinese/Mandarin rather than zho; use hbs_Latn for unresolved Bosnian/Croatian/Serbian text; use fil_Latn for broad Filipino/Tagalog unless there is a specific reason to report tgl; use ory rather than ori for Odia; use uzb rather than uzn for broad Uzbek; use zsm rather than msa for Standard Malay/Malay; use npi rather than nep for Nepali; use pnb for Western/Pakistani Punjabi, pan for Eastern/Standard Punjabi, bns for Bundeli, bra for Braj, raj for Rajasthani, mwr for explicit Marwari, sck for Nagpuri/Sadri/Sadani, kas for Kashmiri, tcy for Tulu, hnd for Hindko, kfr for Kutchi/Kachchi, guj for Gujarati, hne for Chhattisgarhi, bho for Bhojpuri, mag for Magahi, and hif only for Fiji Hindi; distinguish ind vs zsm only with clear evidence.

MIXED LANGUAGE: if a second language recurs across multiple fields, set secondary_language_label, is_mixed_language=true, and list mixed_languages. Do not force a single-language call when the supplied metadata is genuinely bilingual; choose the dominant written metadata language and preserve the recurring secondary language.

CONFIDENCE CAPS: substantive non-boilerplate description prose or repeated coherent title phrases can support high confidence when language/script are clear. Generic about-channel, welcome/support/contact/business, upload/category, or CTA descriptions should not by themselves justify high confidence. A single coherent title/description phrase or repeated non-generic phrases usually supports medium confidence. Localized dates, non-generic hashtags, channel name, language-name/topic tags, or mostly boilerplate should usually be low confidence and must not override higher-quality phrase/prose evidence. Use at most confidence="medium" for script-blind romanized Hindi/Urdu/Punjabi/Bhojpuri/Nepali unless there is repeated clear phrase evidence. If English dominates the character count but a few romanized South Asian cues recur, keep English primary with the South Asian language secondary unless the non-English phrase evidence is clearly dominant. For insufficient_text, confidence must be null.

ABSTAIN only when evidence is genuinely minimal: if the supplied metadata has no usable text, no repeated localized date/month signal, no repeated real English phrase evidence, no repeated non-generic hashtag signal, and no recognizable script/lexical cue, status="insufficient_text" and leave labels null. Do not abstain solely because the visible evidence is short or fastText-ineligible; do abstain for names-only, handles-only, proper-noun-only, topic-only, or religious-icon-only metadata. Otherwise make the best reasonable language guess and use low confidence when the signal is weak.
Do not return und, zxx, mul, inc, or other family/collective codes as classified language labels; use status="insufficient_text" with null labels instead when the text is not classifiable or only a broad family is known.

FINAL CHECK BEFORE OUTPUT: silently verify that (1) quoted evidence is real supplied metadata, not inferred identity/topic/nationality; (2) the label's script matches the decisive cited evidence in both directions; (3) any Tier 1 description evidence is substantive content/message prose, not generic about/channel/contact/category/CTA boilerplate; (4) generic English about/channel/contact/category text did not override repeated coherent native-script phrase evidence; (5) no language name, region, religious term, hashtag, artist/game title, or channel suffix alone set the label; (6) any Hindi-belt regional code has real lect markers, else prefer Hindi or the phrase language; (7) names/dates/one-word/generic-tags/CTA-SEO only means insufficient_text; (8) recurring short real evidence, including short real English phrases, can justify a low-confidence guess, not textless; (9) the response is valid JSON only.

Base the judgment ONLY on the supplied text. NEVER invent content. Return ONE compact, minified JSON object
on one line, nothing else. Keep evidence <=160 characters and quote only the shortest decisive text:
{"status":"classified|insufficient_text","primary_language_label":"iso_Script|null","primary_language_iso639_3":"iso|null","primary_language_script":"Script|null","is_romanized":true|false,"dialect_or_variant":"iso|null","is_high_risk_tail":true|false,"secondary_language_label":"iso_Script|null","is_mixed_language":true|false,"mixed_languages":["iso_Script"],"confidence":"high|medium|low|null","evidence":"<=160 chars"}"""

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
        "confidence": {"type": ["string", "null"], "enum": ["high", "medium", "low", None]},
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
    # Seeded, reproducible sample from notebook-01 comparison output. Scope defaults to the full comparison
    # table for API smoke validation; the LID-disagreement scope targets hard cases where the two fastText
    # detectors disagree after project-level ISO normalization.
    random_sample_base = cmp_df
    route_reason = "random_validation"
    if RANDOM_VALIDATION_SCOPE == "lid_iso_disagreement":
        random_sample_base = random_sample_base.where(
            canonical_base_iso_expr(F.col("openlid_primary_language_iso639_3")).isNotNull()
            & canonical_base_iso_expr(F.col("glotlid_primary_language_iso639_3")).isNotNull()
            & (
                canonical_base_iso_expr(F.col("openlid_primary_language_iso639_3"))
                != canonical_base_iso_expr(F.col("glotlid_primary_language_iso639_3"))
            )
        )
        route_reason = "random_validation_lid_iso_disagreement"
    n_random_sample_base = random_sample_base.count()
    print(f"Random validation eligible channels ({RANDOM_VALIDATION_SCOPE}): {n_random_sample_base:,}")
    if n_random_sample_base < RANDOM_VALIDATION_SAMPLE_SIZE:
        raise ValueError(
            f"random_validation_scope={RANDOM_VALIDATION_SCOPE} has only {n_random_sample_base:,} eligible "
            f"channels, fewer than requested sample size {RANDOM_VALIDATION_SAMPLE_SIZE:,}."
        )
    sample_order = F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.lit(RANDOM_VALIDATION_SEED)))
    route_frames.append(
        random_sample_base
        .orderBy(sample_order, F.col("channel_id"))
        .limit(RANDOM_VALIDATION_SAMPLE_SIZE)
        .withColumn("route_reason", F.lit(route_reason))
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

    if ROUTE_UNCLASSIFIED:
        # Final-arbiter route: channel_model_comparison is built from channels with valid model aggregation
        # rows, so zero-valid channels can be absent from cmp_df. Use the final channel table, which is
        # joined back to the full source universe and carries language_status/consensus_status.
        if not _table_exists_full(channels_full):
            raise ValueError(
                f"route_unclassified=true but channels_table does not exist: {channels_full}. "
                "Point channels_table at the final notebook-01 channel output."
            )
        ch_src = spark.table(channels_full)
        ch_cols = set(ch_src.columns)
        if "run_id" in ch_cols:
            ch_src = ch_src.where(F.col("run_id") == F.lit(SOURCE_RUN_ID))
        if "inference_hash_buckets" in ch_cols:
            ch_src = ch_src.where(F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))

        unclassified_condition = F.lit(False)
        saw_unclassified_signal = False
        both_labels_null_condition = None
        if "language_status" in ch_cols:
            saw_unclassified_signal = True
            unclassified_condition = unclassified_condition | (
                F.col("language_status") == F.lit("insufficient_text_or_unclassified")
            )
        if "consensus_status" in ch_cols:
            saw_unclassified_signal = True
            unclassified_condition = unclassified_condition | (F.col("consensus_status") == F.lit("insufficient_text"))
        if {"openlid_primary_language_label", "glotlid_primary_language_label"}.issubset(ch_cols):
            saw_unclassified_signal = True
            both_labels_null_condition = (
                F.col("openlid_primary_language_label").isNull()
                & F.col("glotlid_primary_language_label").isNull()
            )
            unclassified_condition = unclassified_condition | both_labels_null_condition
        if "valid_language_segment_count" in ch_cols:
            saw_unclassified_signal = True
            zero_valid_condition = F.coalesce(F.col("valid_language_segment_count"), F.lit(0)) == F.lit(0)
            if both_labels_null_condition is not None:
                zero_valid_condition = zero_valid_condition & both_labels_null_condition
            unclassified_condition = unclassified_condition | zero_valid_condition
        if not saw_unclassified_signal:
            raise ValueError(
                f"route_unclassified=true but {channels_full} lacks language_status, consensus_status, "
                "valid_language_segment_count, and OpenLID/GlotLID label fields."
            )

        def _ch_col(name: str, data_type: str = "string"):
            return F.col(name) if name in ch_cols else F.lit(None).cast(data_type)

        u = (
            ch_src.where(unclassified_condition)
            .select(
                "channel_id",
                _ch_col("channel_hash_bucket", "int").alias("channel_hash_bucket"),
                _ch_col("consensus_status").alias("consensus_status"),
                _ch_col("consensus_language_label").alias("consensus_language_label"),
                _ch_col("consensus_source").alias("consensus_source"),
                _ch_col("openlid_primary_language_label").alias("openlid_primary_language_label"),
                _ch_col("glotlid_primary_language_label").alias("glotlid_primary_language_label"),
            )
            .withColumn("route_reason", F.lit("unclassified"))
        )
        route_frames.append(u)

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
    "random_validation": 0, "random_validation_lid_iso_disagreement": 0,
    "disagreement": 1, "unresolved_tail": 2, "shared_bias_english_indic": 3, "unclassified": 4,
    "agreement_audit": 5,
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

def _first_existing_col(cols, candidates):
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None


def _source_text_segment_frame(df, segment_type: str, text_col: str):
    text_expr = F.trim(F.coalesce(F.col(text_col).cast("string"), F.lit("")))
    return (
        df.where(F.length(text_expr) > 0)
        .select(
            "channel_id",
            F.lit(segment_type).alias("segment_type"),
            F.substring(text_expr, 1, MAX_SEGMENT_CHARS).alias("text"),
            F.lit(False).alias("is_valid"),
            F.lit("source_fallback_unsegmented").cast("string").alias("short_text_reason"),
            F.length(F.regexp_replace(text_expr, r"[^\p{L}]", "")).cast("int").alias("clean_letter_count"),
            F.length(text_expr).cast("int").alias("clean_text_len"),
            F.lit(None).cast("string").alias("dominant_script"),
            F.lit(None).cast("double").alias("dominant_script_share"),
        )
    )


source_fallback_frames = []
if source_channels_full or source_videos_full:
    channels_with_segments = seg.select("channel_id").distinct()
    routed_without_segments = routed.select("channel_id").join(channels_with_segments, on="channel_id", how="left_anti").persist()
    n_without_segments = routed_without_segments.count()
    print(f"Routed channels without segments_input rows: {n_without_segments:,}")

    if n_without_segments > 0 and source_channels_full:
        if not _table_exists_full(source_channels_full):
            raise ValueError(f"source_channels_table was set but does not exist: {source_channels_full}")
        src_ch = spark.table(source_channels_full).join(routed_without_segments, on="channel_id", how="inner")
        src_ch_cols = set(src_ch.columns)
        ch_name_col = _first_existing_col(src_ch_cols, ["channel_name", "title", "name"])
        ch_desc_col = _first_existing_col(src_ch_cols, ["channel_description", "description", "about", "channel_about"])
        if ch_name_col:
            source_fallback_frames.append(_source_text_segment_frame(src_ch, "channel_name", ch_name_col))
        if ch_desc_col:
            source_fallback_frames.append(_source_text_segment_frame(src_ch, "channel_description", ch_desc_col))

    if n_without_segments > 0 and source_videos_full:
        if not _table_exists_full(source_videos_full):
            raise ValueError(f"source_videos_table was set but does not exist: {source_videos_full}")
        src_v = spark.table(source_videos_full).join(routed_without_segments, on="channel_id", how="inner")
        src_v_cols = set(src_v.columns)
        video_title_col = _first_existing_col(src_v_cols, ["video_title", "title"])
        video_desc_col = _first_existing_col(src_v_cols, ["video_description", "description"])
        order_cols = []
        if "position" in src_v_cols:
            order_cols.append(F.col("position").asc_nulls_last())
        if "published_at" in src_v_cols:
            order_cols.append(F.col("published_at").desc_nulls_last())
        elif "publish_time" in src_v_cols:
            order_cols.append(F.col("publish_time").desc_nulls_last())
        if "video_id" in src_v_cols:
            order_cols.append(F.col("video_id").asc_nulls_last())
        if not order_cols:
            order_cols.append(F.monotonically_increasing_id())
        video_limit = max(MAX_VIDEO_TITLES, MAX_VIDEO_DESCRIPTIONS, 10)
        src_v_ranked = (
            src_v.withColumn("_video_rank", F.row_number().over(Window.partitionBy("channel_id").orderBy(*order_cols)))
            .where(F.col("_video_rank") <= F.lit(video_limit))
            .drop("_video_rank")
        )
        if video_title_col:
            source_fallback_frames.append(_source_text_segment_frame(src_v_ranked, "video_title", video_title_col))
        if video_desc_col:
            source_fallback_frames.append(_source_text_segment_frame(src_v_ranked, "video_description", video_desc_col))

    if source_fallback_frames:
        source_fallback = source_fallback_frames[0]
        for sf in source_fallback_frames[1:]:
            source_fallback = source_fallback.unionByName(sf, allowMissingColumns=True)
        seg = seg.unionByName(source_fallback, allowMissingColumns=True)
        print("Added raw source fallback segments for routed channels missing segments_input rows.")
    routed_without_segments.unpersist()

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
_prompt_best_guess_mode = PROMPT_BEST_GUESS_MODE

_PROMPT_BOILERPLATE_LINE_PATTERNS = [
    r"^\W*provided to youtube by\b",
    r"^\W*auto-generated by youtube\b",
    r"^released on\s*:",
    r"^(song credits?|music credits?|audio production)\b",
    r"^\W*(album|song|spng|singer|lyrics?|chorus|music composition)\s*[:：;-]",
    r"^\W*(main artist|artist|producer|executive producer|composer|lyricist|arranger|associated performer|performed by|music publisher|cast|genre)\s*[:：;-]",
    r"^\W*(script|dialogue|direction|directed by|writer|written by|written and produced by|story|screenplay|starring|staring|guest appearance|edit|editor|video edited|color|colorist|dop|director of photography|cinematography|camera(?:\s+man)?|shot by|bgm|poster design|pr|label|banner|production|mix(?:ed)?(?:\s+and)?\s+master(?:ed)?|mastered by|beat prod(?:uced)?|set design|choreography|makeup)\s*[:：;-]",
    r"^\W*(digital media(?: creator| planner)?|presents?|presented by|publisher|trade enqu?ir(?:y|ies)|trade enqu?ary|copyright|related artists?|other re?lated tags?|other re?lated hashtags?)\s*[:：;-]",
    r"^\W*(eid ul fitr\s*\d+\s*[—-]\s*)?presenting\b.*\b(drama|natok|film|movie)\b",
    r"^\W*(presenting|watch)\s+the\s+(new|latest|valentine special|eid special)?\s*(drama|natok|film|movie)\b",
    r"^\W*watch\s+.*\b(?:movie|episode|part|clip|show)\b",
    r"^\W*(welcome to|you are watching|movie synopsis|synopsis|storyline)\b",
    r"^\W*(enjoy\s*&\s*stay connected|watch more videos?|for partnership|partnership/collaboration)\b",
    r"^\W*(official site|facebook|twitter|instagram|tiktok|website|discord)\b",
    r"^\W*(related tags?|related tag|your quer(?:y|ies) solved|query solved|search terms?|keywords?|tags?)\s*(?:[:：-]+\s*)?$",
    r"^\W*(for booking|booking|bookings|business enquiries?|business inquiries?|business promotion|sponsorship|collaboration)\s*[:：-]?\b",
    r"^\W*(listen|stream)(?:/download)?\s+(here|now|on|via|\S+\s+via)\b",
    r"^\W*discover similar songs\b",
    r"^\W*be sure to\s+(subscribe|like|follow)\b",
    r"^\W*(we are on|pre-?save link|shop)\b",
    r"^\W*follow\s+[@A-Za-z0-9_.-]+\s*:?\b",
    r"^\W*(click here to subscribe|make sure to subscribe|subscribe\b|like us on|for more such videos|get ready to witness)\b",
    r"^\W*(download link|download mp3|download song|follow (us|me)|join (my|our)|support\b|sponsor\b|superchat)\b",
    r"^\W*(upi id|e-?mail id|email id|e-?mail|email|whats?app|phone|contact)\b",
    r"^\W*(if you like|follow us on|free home delivery|publisher|copyright)\b",
    r"^(copyright disclaimer|under section 107|allowance is made for fair use|fair use is a use permitted|non-profit, educational or personal use)\b",
    r"^music video by\b.*\bofficial video\b",
    r"^[\u2117\u00a9]\s*\d{4}\b",
]
_PROMPT_BOILERPLATE_PHRASE_PATTERNS = [
    r"\bcopyright disclaimer under section 107\b.*$",
    r"\bprovided to youtube by\b.*$",
    r"\bauto-generated by youtube\b.*$",
    r"\b(?:related tags?|your quer(?:y|ies) solved|query solved|search terms?|keywords?)\s*[:：-].*$",
    r"\b(?:stream|listen)(?:/download)?\s+\S+\s+via\b.*$",
    r"\bmake reels on instagram\b.*$",
    r"\b(?:album|song|singer|lyrics?)\s*[:：-].*$",
    r"\bdownload link\s*[-:]\s*\S+",
    r"\b(?:performed by|written and produced by|produced\s*&\s*written by|directed by|mastered by|mixed and mastered|mix and mastered|video edited|camera(?:\s+man)?|shot by|beat prod(?:uced)?|executive producer|starring|staring|set design|choreography|makeup|digital media(?: creator| planner)?|presented by|presents?|publisher|trade enqu?ir(?:y|ies)|trade enqu?ary)\b.*$",
    r"\bproduced by\b.*$",
    r"\bunder the banner of\b.*$",
]
_PROMPT_DESCRIPTION_BOILERPLATE_PHRASE_PATTERNS = [
    r"\b(?:facebook|instagram|tiktok|twitter|telegram|snapchat|youtube)\s*[:：/@].*$",
    r"\b(?:more socials|redes sociales|social medias?|suis[- ]?moi|abonne[- ]toi|"
    r"folgt? (?:uns|mir)|folge (?:uns|mir)|segui(?:ci)?|pratite me|"
    r"kanal jetzt abonnieren|channel abonnieren|channel abonnieren|subscribe(?: to)?|"
    r"follow (?:me|us|[A-Z][A-Za-z0-9_.-]+))\b.*$",
    r"\b(?:download/stream|stream/download|download mp3|download now|download here|"
    r"ascolta|escucha|écoute|ecoute)\b.*$",
    r"\b(?:bookings? via|label, management|management, booking|booking & interviewanfragen|"
    r"business inquiries?|business enquiries?|contact|mail\s*:|e-?mail\s*:)\b.*$",
    r"\b(?:join the trend|use my code)\b.*$",
]
_PROMPT_BOILERPLATE_SECTION_PATTERNS = [
    r"^\W*(related tags?|related tag|your quer(?:y|ies) solved|query solved|search terms?|keywords?)\s*(?:[:：-]+\s*)?$",
]
_PROMPT_GENERIC_HASHTAGS = {
    "shorts", "ytshorts", "shortvideo", "viral", "viralshorts", "trending", "trend", "fyp",
    "explore", "motivation", "officialvideo", "musicvideo", "video", "song", "subscribe",
    "youtube", "youtubeshorts", "feedshorts", "shortsfeed", "reels", "foryou", "foryoupage",
    "duet", "status", "newrelease", "latest", "funny", "comedy", "dance", "dancesong",
    "partymusic", "love", "emotional", "islamic", "islamicprayer", "islamicritual",
    "quran", "surah", "yasin", "rahman", "naat", "tilawat", "juma", "allah", "azan",
}
_URL_RE = re.compile(r"(https?://\S+|www\.\S+)", flags=re.IGNORECASE)
_OFFICIAL_VIDEO_RE = re.compile(r"\(?\bofficial(?:\s+music)?\s+video\b\)?", flags=re.IGNORECASE)
_GENERIC_TITLE_SCAFFOLD_RE = re.compile(
    r"\b(?:official(?:\s+\w+){0,2}\s+video|remastered|lyrics?|lyric\s+video|jukebox|"
    r"mukbang|eating|asmr|shorts?|reels?|vlogs?|mini\s*vlog|daily\s*vlog|"
    r"music\s+video|full\s+video|full\s+movie|new\s+song|old\s+songs?|songs?|"
    r"official\s+trailer|trailer|teaser|web\s+series|audio\s+jukebox|audio|"
    r"visuali[sz]er|clip\s+officiel|cover|remix|recipe|whatsapp\s+status|status|"
    r"comedy\s+scene|dance\s+choreo|choreo|dance|movie|fancam|behind|"
    r"performance\s+ver(?:sion)?|full\s+episode|episodes?|promo|preview|review|reaction|"
    r"gameplay|minecraft|roblox|lego|cartoons?|nursery\s+rhymes?|rhymes?|toys?|"
    r"compilations?|subscribe|hd|4k)\b",
    flags=re.IGNORECASE,
)
_HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")
_PROMPT_LANGUAGE_HINT_PATTERNS = {
    "ara": [r"\barabic\b"],
    "ben": [r"\bbangla\b", r"\bbengali\b"],
    "bgc": [r"\bharyanvi\b"],
    "bho": [r"\bbhojpuri\b"],
    "bns": [r"\bbundeli\b", r"\bbundelkhand(?:i)?\b"],
    "bra": [r"\bbraj\b", r"\bbrij\b"],
    "ell": [r"\bgreek\b"],
    "hbs": [r"\bserbo[- ]?croatian\b", r"\bbcs\b"],
    "hin": [r"\bhindi\b", r"\bhinglish\b"],
    "hnd": [r"\bhindko\b"],
    "hne": [r"\bchh?attisgarh(?:i)?\b", r"\bchh?attisarhi\b", r"\bcg\s+(song|gana)\b"],
    "guj": [r"\bgujarati\b"],
    "ind": [r"\bindonesian\b", r"\bindonesia\b"],
    "jav": [r"\bjavanese\b"],
    "kas": [r"\bkashmiri\b"],
    "kfr": [r"\bkutchi\b", r"\bkachchi\b", r"\bkutch\b"],
    "kha": [r"\bkhasi\b"],
    "kor": [r"\bkorean\b", r"\bk[- ]?pop\b"],
    "lao": [r"\blao\b"],
    "mal": [r"\bmalayalam\b"],
    "mag": [r"\bmagahi\b", r"\bmaghi\b", r"\bmaghisong\b"],
    "mwr": [r"\bmarwari\b"],
    "pan": [r"\bpunjabi\b", r"\bgurmukhi\b"],
    "pnb": [r"\bwestern punjabi\b", r"\bpakistani punjabi\b", r"\bshahmukhi\b"],
    "por": [r"\bportuguese\b", r"\bportugu[e\u00ea]s\b"],
    "pus": [r"\bpashto\b", r"\bpakhto\b"],
    "raj": [r"\brajasthani\b"],
    "rus": [r"\brussian\b"],
    "sck": [r"\bnagpuri\b", r"\bsadri\b", r"\bsadani\b"],
    "spa": [r"\bspanish\b", r"\bespa[n\u00f1]ol\b"],
    "tam": [r"\btamil\b"],
    "tcy": [r"\btulu\b"],
    "tel": [r"\btelugu\b"],
    "tha": [r"\bthai\b"],
    "urd": [r"\burdu\b"],
    "wol": [r"\bwolof\b"],
    "zsm": [r"\bmalay\b", r"\bbahasa malaysia\b"],
}
_PROMPT_LOCALIZED_MONTH_PATTERNS = {
    # Exclude English-identical names such as April/August/September/November; they created noisy German cues.
    "deu": [r"\bjanuar\b", r"\bfebruar\b", r"\bmärz\b", r"\bmaerz\b", r"\bmai\b", r"\bjuni\b", r"\bjuli\b", r"\boktober\b", r"\bdezember\b"],
    "fra": [r"\bjanvier\b", r"\bfévrier\b", r"\bfevrier\b", r"\bmars\b", r"\bavril\b", r"\bmai\b", r"\bjuin\b", r"\bjuillet\b", r"\baoût\b", r"\baout\b", r"\bseptembre\b", r"\boctobre\b", r"\bnovembre\b", r"\bdécembre\b", r"\bdecembre\b"],
    "ind": [r"\bjanuari\b", r"\bfebruari\b", r"\bmaret\b", r"\bmei\b", r"\bjuni\b", r"\bjuli\b", r"\bagustus\b", r"\bseptember\b", r"\boktober\b", r"\bnovember\b", r"\bdesember\b"],
    "ita": [r"\bgennaio\b", r"\bfebbraio\b", r"\bmarzo\b", r"\baprile\b", r"\bmaggio\b", r"\bgiugno\b", r"\bluglio\b", r"\bagosto\b", r"\bsettembre\b", r"\bottobre\b", r"\bnovembre\b", r"\bdicembre\b"],
    "por": [r"\bjaneiro\b", r"\bfevereiro\b", r"\bmarço\b", r"\bmarco\b", r"\babril\b", r"\bmaio\b", r"\bjunho\b", r"\bjulho\b", r"\bagosto\b", r"\bsetembro\b", r"\boutubro\b", r"\bnovembro\b", r"\bdezembro\b"],
    "rus": [r"\bянваря\b", r"\bфевраля\b", r"\bмарта\b", r"\bапреля\b", r"\bмая\b", r"\bиюня\b", r"\bиюля\b", r"\bавгуста\b", r"\bсентября\b", r"\bоктября\b", r"\bноября\b", r"\bдекабря\b"],
    "spa": [r"\benero\b", r"\bfebrero\b", r"\bmarzo\b", r"\babril\b", r"\bmayo\b", r"\bjunio\b", r"\bjulio\b", r"\bagosto\b", r"\bseptiembre\b", r"\bsetiembre\b", r"\boctubre\b", r"\bnoviembre\b", r"\bdiciembre\b"],
    "tur": [r"\bocak\b", r"\bşubat\b", r"\bsubat\b", r"\bmart\b", r"\bnisan\b", r"\bmayıs\b", r"\bmayis\b", r"\bhaziran\b", r"\btemmuz\b", r"\bağustos\b", r"\bagustos\b", r"\beylül\b", r"\beylul\b", r"\bekim\b", r"\bkasım\b", r"\bkasim\b", r"\baralık\b", r"\baralik\b"],
    "vie": [r"\bngày\b", r"\bngay\b", r"\btháng\b", r"\bthang\b"],
}

_PROMPT_ROMANIZED_SOUTH_ASIAN_PATTERNS = {
    "hin_urd_shared": [
        r"\b(ki|ka|ke|ko|se|hai|hain|mei|mein|main|ye|yeh|kya|kyu|kyun|kaise|kase|bhai|dil|aap|aapka|sabko|rasta|raasta|samne|saamne)\b",
    ],
    "hin": [
        r"\b(namaste|dhokha|dhadi|sada\s+bahaar|nagme|bach(?:a|cha)|aaft|hoga|hogi|hone)\b",
    ],
    "urd": [
        r"\b(dua|wazifa|naat|tilawat|darood|duniya|subse|sabse|pyari|awaz|ishq|rasool|tabdil)\b",
    ],
    "pnb": [
        r"\b(saadi|sadi|sadda|sade|sanu|noo|nu|ae|aiy|wich|chany|maawan|mola|ishqa)\b",
    ],
    "bho": [
        r"\b(ba|bani|badu|tohar|hamar|rauwa|saiyan)\b",
        r"\bka\s+ho\b",
    ],
    "npi": [
        r"\b(nepal\s+ghumgham|ghumgham)\b",
    ],
    "hne": [
        r"\b(mor|mola|tor|ka\s+hoge)\b",
    ],
}

_PROMPT_ARABIC_SCRIPT_SOUTH_ASIAN_PATTERNS = {
    "urd": [
        r"کو", r"کی", r"کا", r"کے", r"میں", r"نہیں", r"والا", r"والے", r"والوں",
        r"دینے", r"ہے", r"ہیں", r"سکون", r"ک", r"ی", r"ے", r"ہ", r"گ",
    ],
    "fas_or_urd_letterforms": [r"ک", r"ی", r"ے", r"ہ", r"گ"],
    "pnb": [
        r"ساڈی", r"ساڈا", r"ساڈے", r"اے", r"دا", r"دی", r"دے", r"وچ", r"نوں", r"مینوں",
    ],
}

_PROMPT_TOPIC_LANGUAGE_MENTION_PATTERNS = {
    "religious_topic_not_language": [
        r"\b(allah|masha\s*allah|mashaallah|insha\s*allah|bismillah|quran|qur'?an|surah|ayat|yasin|rahman|tilawat|naat|azan|darood|islamic\s+knowledge|makkah|madina|kaaba|gita|pravachana)\b",
        r"ﷺ",
    ],
    "language_or_region_tag_not_phrase": [
        r"\b(hindi|urdu|punjabi|bhojpuri|nepali|arabic|bangla|bengali|tamil|telugu|malayalam|korean|thai|russian|spanish|portuguese|english|kashmiri|chaoui|algerian|marathi|kannada|bodo)\b",
        r"\b(tamil|punjabi|bhojpuri|telugu|kashmiri)\s+(edit|status|song|songs|funny|video|shorts?)\b",
    ],
    "media_topic_not_language": [
        r"\b(roblox|minecraft|pubg|free\s*fire|gameplay|shorts|vlog|reaction|status|lyrics|song|movie|film|drama|cartoon|anime|fancam)\b",
    ],
    "geography_or_ethnicity_not_language": [
        r"\b(karachites?|lahoris?|pakistan(?:i)?|india(?:n)?|bangladesh(?:i)?|kashmir(?:i)?|algeria(?:n)?|chaoui|desi)\b",
    ],
}

_PROMPT_SHORT_SENTENCE_CUE_PATTERNS = {
    "eng": [r"\b(if|you|your|you're|the|this|that|for|with|from|to|is|are|was|were|i'?m|boys|feeling)\b"],
    "spa": [r"\b(disfruta|nuestro|nuestra|contenido|para|hecho|ti|que|con|los|las|del|de|el|la)\b"],
    "deu": [r"\b(offiziell(?:er|e|es)?|youtube|channel|kanal|von|und|für|fuer|mit|der|die|das)\b"],
    "fra": [r"\b(cha[iî]ne|officielle|pour|avec|dans|les|des|une|vous)\b"],
    "por": [r"\b(conte[uú]do|para|com|dos|das|uma|voce|voc[êe])\b"],
    "ind": [r"\b(kumpulan|lucu|ngakak|abis|yang|untuk|dengan|terbaru|kok\s+bisa|kalo|kalau|jarang|euy|ganteng)\b"],
    "tur": [r"\b(hay[ıi]r|ke[şs]fet|evet|de[ğg]il|benim|senin|i[çc]in|ile)\b"],
    "ara_latn": [r"\b(3ala|3arabi|7abib(?:i)?|7obi|2albi|9albi|salam|shukran|yalla)\b"],
}

_PROMPT_CTA_BOILERPLATE_PATTERNS = [
    r"\b(please\s+support\s+me|support\s+my\s+channel|welcome\s+to\s+my\s+channel|thanks?\s+for\s+watching|subscribe\s+(to\s+)?my\s+channel|like\s+share\s+(and\s+)?subscribe|my\s+new\s+channel\s+for\s+live)\b",
]


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


def _letter_count(text: str) -> int:
    return sum(1 for ch in str(text or "") if ch.isalpha())


def _language_hint_counts(text: str) -> Dict[str, int]:
    lowered = str(text or "").lower()
    counts: Dict[str, int] = {}
    for iso, patterns in _PROMPT_LANGUAGE_HINT_PATTERNS.items():
        hits = sum(len(re.findall(pattern, lowered, flags=re.IGNORECASE)) for pattern in patterns)
        if hits:
            counts[iso] = hits
    return counts


def _localized_month_hint_counts(text: str) -> Dict[str, int]:
    lowered = str(text or "").lower()
    counts: Dict[str, int] = {}
    for iso, patterns in _PROMPT_LOCALIZED_MONTH_PATTERNS.items():
        hits = sum(len(re.findall(pattern, lowered, flags=re.IGNORECASE)) for pattern in patterns)
        if hits:
            counts[iso] = hits
    return counts


def _pattern_hint_counts(text: str, patterns_by_label: Dict[str, List[str]]) -> Dict[str, int]:
    value = str(text or "")
    lowered = value.lower()
    counts: Dict[str, int] = {}
    for label, patterns in patterns_by_label.items():
        hits = sum(len(re.findall(pattern, lowered, flags=re.IGNORECASE)) for pattern in patterns)
        if hits:
            counts[label] = hits
    return counts


def _romanized_south_asian_hint_counts(text: str) -> Dict[str, int]:
    return _pattern_hint_counts(text, _PROMPT_ROMANIZED_SOUTH_ASIAN_PATTERNS)


def _arabic_script_south_asian_marker_counts(text: str) -> Dict[str, int]:
    counts = _text_script_counts(text)
    if counts.get("arabic", 0) < 2:
        return {}
    value = str(text or "")
    marker_counts: Dict[str, int] = {}
    for label, patterns in _PROMPT_ARABIC_SCRIPT_SOUTH_ASIAN_PATTERNS.items():
        hits = sum(len(re.findall(pattern, value)) for pattern in patterns)
        if hits:
            marker_counts[label] = hits
    return marker_counts


def _topic_language_mention_counts(text: str) -> Dict[str, int]:
    return _pattern_hint_counts(text, _PROMPT_TOPIC_LANGUAGE_MENTION_PATTERNS)


def _short_sentence_cue_counts(text: str) -> Dict[str, int]:
    return _pattern_hint_counts(text, _PROMPT_SHORT_SENTENCE_CUE_PATTERNS)


def _cta_boilerplate_count(text: str) -> int:
    lowered = str(text or "").lower()
    return sum(len(re.findall(pattern, lowered, flags=re.IGNORECASE)) for pattern in _PROMPT_CTA_BOILERPLATE_PATTERNS)


def _looks_like_short_sentence_or_phrase(text: str, segment_type: str) -> bool:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    if not cleaned:
        return False
    if _cta_boilerplate_count(cleaned):
        return False
    letters = _letter_count(cleaned)
    if letters < 10 or letters > 180:
        return False
    words = re.findall(r"[^\W\d_]+", cleaned, flags=re.UNICODE)
    if len(words) < 2:
        return False
    scripts = _text_script_counts(cleaned)
    non_latin_letters = sum(n for script, n in scripts.items() if script != "latin")
    if non_latin_letters >= 8:
        return True
    if (segment_type or "").lower() == "channel_name" and letters < 30:
        return False
    if _short_sentence_cue_counts(cleaned) or _romanized_south_asian_hint_counts(cleaned):
        return True
    if re.search(r"[.!?]", cleaned) and letters >= 18:
        return True
    return False


def _looks_like_coherent_description(text: str, segment_type: str) -> bool:
    st = (segment_type or "").lower()
    if st not in {"channel_description", "video_description"}:
        return False
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    letters = _letter_count(cleaned)
    if letters < 35 or letters > 500:
        return False
    if _cta_boilerplate_count(cleaned):
        return False
    if _is_hashtag_dominated_line(cleaned):
        return False
    words = re.findall(r"[^\W\d_]+", cleaned, flags=re.UNICODE)
    if len(words) < 6:
        return False
    scripts = _text_script_counts(cleaned)
    non_latin_letters = sum(n for script, n in scripts.items() if script != "latin")
    if non_latin_letters >= 12:
        return True
    return bool(_short_sentence_cue_counts(cleaned) or re.search(r"[.!?]", cleaned))


def _split_hashtag_tag(raw_tag: str) -> str:
    tag = str(raw_tag or "").strip("_")
    tag = re.sub(r"[_-]+", " ", tag)
    tag = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", tag)
    tag = re.sub(r"(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])", " ", tag)
    return re.sub(r"\s+", " ", tag).strip()


def _extract_non_generic_hashtags(text: str):
    tags = []
    for raw_tag in _HASHTAG_RE.findall(str(text or "")):
        tag = raw_tag.strip("_").lower()
        if tag and tag not in _PROMPT_GENERIC_HASHTAGS:
            tags.append(tag)
    return tags


def _expand_non_generic_hashtags(text: str) -> str:
    def _sub(match):
        raw_tag = match.group(1)
        tag = raw_tag.strip("_").lower()
        if not tag or tag in _PROMPT_GENERIC_HASHTAGS:
            return ""
        return " " + _split_hashtag_tag(raw_tag) + " "

    return _HASHTAG_RE.sub(_sub, text)


def _is_hashtag_dominated_line(text: str) -> bool:
    raw = _URL_RE.sub("", str(text or ""))
    tags = _HASHTAG_RE.findall(raw)
    if len(tags) < 2:
        return False
    total_letters = _letter_count(raw)
    non_hashtag_letters = _letter_count(_HASHTAG_RE.sub("", raw))
    if total_letters == 0:
        return True
    return non_hashtag_letters < 12 or (non_hashtag_letters / total_letters) < 0.35


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
        non_generic_tags = _extract_non_generic_hashtags(line)
        hashtag_dominated = _is_hashtag_dominated_line(line)
        line = _URL_RE.sub("", line).strip()
        if not line:
            continue
        if _strip_prompt_boilerplate:
            line_l = line.lower()
            if any(re.search(pattern, line_l, flags=re.IGNORECASE) for pattern in _PROMPT_BOILERPLATE_SECTION_PATTERNS):
                break
            if any(re.search(pattern, line_l, flags=re.IGNORECASE) for pattern in _PROMPT_BOILERPLATE_LINE_PATTERNS):
                continue
            for pattern in _PROMPT_BOILERPLATE_PHRASE_PATTERNS:
                line = re.sub(pattern, "", line, flags=re.IGNORECASE)
            if (segment_type or "").lower() in {"video_description", "channel_description"}:
                for pattern in _PROMPT_DESCRIPTION_BOILERPLATE_PHRASE_PATTERNS:
                    line = re.sub(pattern, "", line, flags=re.IGNORECASE)
            line = _OFFICIAL_VIDEO_RE.sub("", line)
            if (segment_type or "").lower() == "video_title":
                line = _GENERIC_TITLE_SCAFFOLD_RE.sub("", line)
            line = re.sub(r"\bauto-generated by youtube\b", "", line, flags=re.IGNORECASE)
            line = _remove_generic_hashtags(line)
            line = _expand_non_generic_hashtags(line)
        line = re.sub(r"\s+", " ", line).strip(" -|\u00b7:;►•*")
        if hashtag_dominated and _letter_count(line) < 20 and not non_generic_tags:
            continue
        if line and any(ch.isalpha() for ch in line):
            lines.append(line)
    return "\n".join(lines).strip()


def _prompt_dedupe_key(text: str) -> str:
    key = _URL_RE.sub("", str(text or "").lower())
    key = re.sub(r"\b(?:episode|ep|part|pt|day|vol|volume|quote|quotes)\s*[-:#]?\s*\d+\b", "", key)
    key = re.sub(r"\d+", "#", key)
    key = _HASHTAG_RE.sub("", key)
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
    invalid_marker = " [fasttext-ineligible-visible-text:"
    seen = set()
    script_stats = {}
    text_script_stats = {}
    field_stats = {}
    language_hint_stats = {}
    weak_hashtag_hint_stats = {}
    hashtag_stats = {}
    localized_month_stats = {}
    short_sentence_stats = {}
    coherent_description_stats = {}
    cta_boilerplate_stats = {}
    romanized_south_asian_stats = {}
    arabic_script_south_asian_stats = {}
    topic_language_mention_stats = {}
    repeated_pattern_stats = {}

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

    def _record_weak_hashtag_hints(bucket: str, raw_text: str) -> None:
        tags = _extract_non_generic_hashtags(raw_text)
        if not tags:
            return
        tag_text = " ".join(tags)
        for iso, n_hits in _language_hint_counts(tag_text).items():
            bucket_stats = weak_hashtag_hint_stats.setdefault(bucket, {})
            bucket_stats[iso] = bucket_stats.get(iso, 0) + n_hits

    def _record_hashtag_stats(bucket: str, raw_text: str) -> None:
        tags = _extract_non_generic_hashtags(raw_text)
        if not tags:
            return
        bucket_stats = hashtag_stats.setdefault(bucket, {})
        for tag in tags:
            display_tag = _split_hashtag_tag(tag) or tag
            bucket_stats[display_tag] = bucket_stats.get(display_tag, 0) + 1

    def _record_localized_month_hints(bucket: str, raw_text: str) -> None:
        hints = _localized_month_hint_counts(raw_text)
        if not hints:
            return
        bucket_stats = localized_month_stats.setdefault(bucket, {})
        for iso, n_hits in hints.items():
            bucket_stats[iso] = bucket_stats.get(iso, 0) + n_hits

    def _merge_bucket_counts(store, bucket: str, counts: Dict[str, int]) -> None:
        if not counts:
            return
        bucket_stats = store.setdefault(bucket, {})
        for label, n_hits in counts.items():
            bucket_stats[label] = bucket_stats.get(label, 0) + n_hits

    def _record_short_sentence_candidate(bucket: str, st: str, txt: str) -> None:
        if not _looks_like_short_sentence_or_phrase(txt, st):
            return
        stats = short_sentence_stats.setdefault(bucket, {"n": 0, "cues": {}, "samples": []})
        stats["n"] += 1
        for label, n_hits in _short_sentence_cue_counts(txt).items():
            stats["cues"][label] = stats["cues"].get(label, 0) + n_hits
        sample = re.sub(r"\s+", " ", str(txt or "")).strip()[:110]
        if sample and sample not in stats["samples"] and len(stats["samples"]) < 4:
            stats["samples"].append(sample)

    def _record_coherent_description_candidate(bucket: str, st: str, txt: str) -> None:
        if not _looks_like_coherent_description(txt, st):
            return
        stats = coherent_description_stats.setdefault(bucket, {"n": 0, "cues": {}, "samples": []})
        stats["n"] += 1
        for label, n_hits in _short_sentence_cue_counts(txt).items():
            stats["cues"][label] = stats["cues"].get(label, 0) + n_hits
        sample = re.sub(r"\s+", " ", str(txt or "")).strip()[:140]
        if sample and sample not in stats["samples"] and len(stats["samples"]) < 3:
            stats["samples"].append(sample)

    def _record_cta_boilerplate(bucket: str, txt: str) -> None:
        n_hits = _cta_boilerplate_count(txt)
        if not n_hits:
            return
        cta_boilerplate_stats[bucket] = cta_boilerplate_stats.get(bucket, 0) + n_hits

    def _record_repeated_pattern(bucket: str, key: str, txt: str) -> None:
        if not key or len(key) <= len(bucket) + 8:
            return
        bucket_stats = repeated_pattern_stats.setdefault(bucket, {})
        stats = bucket_stats.setdefault(key, {"n": 0, "sample": txt[:120]})
        stats["n"] += 1

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
        bucket = _bucket_for_segment_type(st)
        _record_hashtag_stats(bucket, s["text"])
        _record_weak_hashtag_hints(bucket, s["text"])
        _record_localized_month_hints(bucket, s["text"])
        txt = _clean_prompt_text(s["text"], st)
        if not txt:
            continue
        _record_text_stats(bucket, txt)
        _record_short_sentence_candidate(bucket, st, txt)
        _record_coherent_description_candidate(bucket, st, txt)
        _record_cta_boilerplate(bucket, txt)
        _merge_bucket_counts(romanized_south_asian_stats, bucket, _romanized_south_asian_hint_counts(txt))
        _merge_bucket_counts(arabic_script_south_asian_stats, bucket, _arabic_script_south_asian_marker_counts(txt))
        _merge_bucket_counts(topic_language_mention_stats, bucket, _topic_language_mention_counts(txt))
        _record_script(bucket, s)
        key = f"{st}:{_prompt_dedupe_key(txt)}"
        if _dedupe_prompt_segments:
            _record_repeated_pattern(bucket, key, txt)
        if _dedupe_prompt_segments:
            if key in seen:
                continue
            if len(key) > len(st) + 8:
                seen.add(key)
        entry = f"{txt}{_invalid_tag(s)}"
        if st == "channel_name":
            name.append(entry)
        elif st == "video_title":
            titles.append(entry)
        elif st in ("video_description", "channel_description"):
            descs.append(entry)
        else:
            other.append(entry)
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
    if _prompt_best_guess_mode:
        lines.append("FINAL FALLBACK MODE: use low confidence for a reasonable best guess from repeated weak cues; use insufficient_text only when language evidence is truly minimal.")
    if not field_stats:
        lines.append("NO USABLE NATURAL-LANGUAGE TITLE/DESCRIPTION TEXT REMAINED AFTER CLEANUP.")
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

    def _priority_sample_text(samples, limit=2, char_limit=80):
        values = []
        for sample in samples or []:
            cleaned = re.sub(r"\s+", " ", str(sample or "")).strip()
            if not cleaned or cleaned in values:
                continue
            values.append(cleaned[:char_limit])
            if len(values) >= limit:
                break
        return "; ".join(values)

    def _priority_cue_text(cues, limit=3):
        if not cues:
            return ""
        top = sorted(cues.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]
        return " cues=" + ",".join(f"{label}={n}" for label, n in top)

    def _priority_sample_stats(stats_by_bucket, buckets, sample_limit=2):
        parts = []
        for bucket in buckets:
            if bucket not in stats_by_bucket:
                continue
            stats = stats_by_bucket[bucket]
            samples = _priority_sample_text(stats.get("samples", []), sample_limit)
            sample_part = f" examples={samples}" if samples else ""
            parts.append(f"{bucket}: n={stats.get('n', 0)}{_priority_cue_text(stats.get('cues', {}))}{sample_part}")
        return " | ".join(parts)

    def _priority_count_stats(stats_by_bucket, buckets, limit=4):
        parts = []
        for bucket in buckets:
            if bucket not in stats_by_bucket:
                continue
            top = sorted(stats_by_bucket[bucket].items(), key=lambda kv: (-kv[1], kv[0]))[:limit]
            if top:
                parts.append(f"{bucket}: " + ",".join(f"{label}={n}" for label, n in top))
        return " | ".join(parts)

    def _is_priority_repeated_phrase(sample: str) -> bool:
        cleaned = re.sub(r"\s+", " ", str(sample or "")).strip()
        if _letter_count(cleaned) < 10:
            return False
        if _cta_boilerplate_count(cleaned) or _is_hashtag_dominated_line(cleaned):
            return False
        scripts = _text_script_counts(cleaned)
        non_latin_letters = sum(n for script, n in scripts.items() if script != "latin")
        if non_latin_letters >= 8 or _romanized_south_asian_hint_counts(cleaned):
            return True
        cue_counts = _short_sentence_cue_counts(cleaned)
        if _GENERIC_TITLE_SCAFFOLD_RE.search(cleaned) and set(cue_counts).issubset({"eng"}):
            return False
        return bool(cue_counts) or _letter_count(cleaned) >= 25

    def _priority_repeated_phrase_stats():
        parts = []
        for bucket in ["video_title", "description", "other"]:
            if bucket not in repeated_pattern_stats:
                continue
            repeats = sorted(
                (
                    stats for stats in repeated_pattern_stats[bucket].values()
                    if stats["n"] > 1 and _is_priority_repeated_phrase(stats.get("sample", ""))
                ),
                key=lambda stats: (-stats["n"], stats["sample"]),
            )
            if repeats:
                parts.append(
                    f"{bucket}: "
                    + "; ".join(f"x{stats['n']} {str(stats.get('sample', ''))[:80]}" for stats in repeats[:3])
                )
        return " | ".join(parts)

    def _priority_boilerplate_stats():
        parts = []
        if cta_boilerplate_stats:
            cta_parts = [
                f"{bucket}=cta_or_channel_boilerplate:{cta_boilerplate_stats[bucket]}"
                for bucket in ["video_title", "description", "channel_name", "other"]
                if bucket in cta_boilerplate_stats
            ]
            if cta_parts:
                parts.append("cta=" + ",".join(cta_parts))
        topic_part = _priority_count_stats(topic_language_mention_stats, ["video_title", "description", "channel_name", "other"], 3)
        if topic_part:
            parts.append("topic_or_language_name=" + topic_part)
        return " | ".join(parts)

    priority_parts = []
    t1 = _priority_sample_stats(coherent_description_stats, ["description"], 2)
    if t1:
        priority_parts.append(f"T1 substantive_description_prose_if_not_boilerplate={t1}")
    t2 = _priority_sample_stats(short_sentence_stats, ["video_title"], 3)
    if t2:
        priority_parts.append(f"T2 coherent_title_phrases={t2}")
    t3 = _priority_repeated_phrase_stats()
    if t3:
        priority_parts.append(f"T3 repeated_non_generic_phrases={t3}")
    t4 = _priority_count_stats(localized_month_stats, ["video_title", "description", "channel_name", "other"], 4)
    if t4:
        priority_parts.append(f"T4 localized_date_month_cues={t4}")
    t5 = _priority_count_stats(hashtag_stats, ["video_title", "description", "channel_name", "other"], 4)
    if t5:
        priority_parts.append(f"T5 non_generic_hashtags={t5}")
    t6 = _priority_sample_text(name, 1, 80)
    if t6:
        priority_parts.append(f"T6 channel_name={t6}")
    t7 = _priority_boilerplate_stats()
    if t7:
        priority_parts.append(f"T7 generic_english_seo_boilerplate={t7}")
    if priority_parts:
        priority_line = (
            "EVIDENCE PRIORITY SUMMARY (higher tiers outrank lower tiers; field weights break ties within comparable tiers): "
            + " || ".join(priority_parts)
        )
        if len(priority_line) > 1400:
            priority_line = priority_line[:1397].rstrip() + "..."
        lines.append(priority_line)
    if short_sentence_stats:
        short_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in short_sentence_stats:
                continue
            stats = short_sentence_stats[bucket]
            cue_part = ""
            if stats.get("cues"):
                cues = sorted(stats["cues"].items(), key=lambda kv: (-kv[1], kv[0]))
                cue_part = " cues=" + ",".join(f"{label}={n}" for label, n in cues[:5])
            samples = "; ".join(stats.get("samples", [])[:3])
            short_parts.append(f"{bucket}: n={stats['n']}{cue_part} examples={samples}")
        if short_parts:
            lines.append("SHORT SENTENCE/PHRASE CUES (Tier 2 when from titles; lower-tier support otherwise): " + " | ".join(short_parts))
    if coherent_description_stats:
        desc_parts = []
        for bucket in ["description", "video_title", "channel_name", "other"]:
            if bucket not in coherent_description_stats:
                continue
            stats = coherent_description_stats[bucket]
            cue_part = ""
            if stats.get("cues"):
                cues = sorted(stats["cues"].items(), key=lambda kv: (-kv[1], kv[0]))
                cue_part = " cues=" + ",".join(f"{label}={n}" for label, n in cues[:5])
            samples = "; ".join(stats.get("samples", [])[:2])
            desc_parts.append(f"{bucket}: n={stats['n']}{cue_part} examples={samples}")
        if desc_parts:
            lines.append(
                "COHERENT DESCRIPTION PROSE (Tier 1 only when substantive content/message; "
                "generic about/support/contact/upload/category text is lower-tier boilerplate): "
                + " | ".join(desc_parts)
            )
    if cta_boilerplate_stats:
        cta_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket in cta_boilerplate_stats:
                cta_parts.append(f"{bucket}: cta_or_channel_boilerplate={cta_boilerplate_stats[bucket]}")
        if cta_parts:
            lines.append("CTA/CHANNEL BOILERPLATE (Tier 7; generic about/support/contact/upload/category text is not Tier 1): " + " | ".join(cta_parts))
    if romanized_south_asian_stats:
        sa_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in romanized_south_asian_stats:
                continue
            hints = sorted(romanized_south_asian_stats[bucket].items(), key=lambda kv: (-kv[1], kv[0]))
            sa_parts.append(f"{bucket}: " + ", ".join(f"{label}={n_hits}" for label, n_hits in hints[:6]))
        if sa_parts:
            lines.append("ROMANIZED SOUTH ASIAN CUES (weak; use low/medium confidence unless phrase evidence is clear): " + " | ".join(sa_parts))
    if arabic_script_south_asian_stats:
        arabic_sa_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in arabic_script_south_asian_stats:
                continue
            hints = sorted(arabic_script_south_asian_stats[bucket].items(), key=lambda kv: (-kv[1], kv[0]))
            arabic_sa_parts.append(f"{bucket}: " + ", ".join(f"{label}={n_hits}" for label, n_hits in hints[:5]))
        if arabic_sa_parts:
            lines.append("ARABIC-SCRIPT URDU/PUNJABI CUES (these argue against naive ara_Arab if Arabic grammar is absent): " + " | ".join(arabic_sa_parts))
    if topic_language_mention_stats:
        topic_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in topic_language_mention_stats:
                continue
            hints = sorted(topic_language_mention_stats[bucket].items(), key=lambda kv: (-kv[1], kv[0]))
            topic_parts.append(f"{bucket}: " + ", ".join(f"{label}={n_hits}" for label, n_hits in hints[:5]))
        if topic_parts:
            lines.append("TOPIC/LANGUAGE-NAME MENTIONS (routing/topic cues; not phrase evidence by themselves): " + " | ".join(topic_parts))
    if localized_month_stats:
        date_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in localized_month_stats:
                continue
            hints = sorted(localized_month_stats[bucket].items(), key=lambda kv: (-kv[1], kv[0]))
            date_parts.append(f"{bucket}: " + ", ".join(f"{iso}={n_hits}" for iso, n_hits in hints[:5]))
        if date_parts:
            lines.append("LOCALIZED DATE/MONTH CUES (Tier 4; weak but usable if repeated): " + " | ".join(date_parts))
    if language_hint_stats:
        hints = sorted(language_hint_stats.items(), key=lambda kv: (-kv[1], kv[0]))
        hint_part = ", ".join(f"{iso}={n_hits}" for iso, n_hits in hints[:8])
        lines.append("LANGUAGE HINTS (non-decisive cue counts): " + hint_part)
    if hashtag_stats:
        tag_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in hashtag_stats:
                continue
            tags = sorted(hashtag_stats[bucket].items(), key=lambda kv: (-kv[1], kv[0]))
            tag_parts.append(f"{bucket}: " + ", ".join(f"{tag}={n}" for tag, n in tags[:8]))
        if tag_parts:
            lines.append("NON-GENERIC HASHTAGS (Tier 5; weak cues that do not override prose/phrases): " + " | ".join(tag_parts))
    if weak_hashtag_hint_stats:
        weak_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in weak_hashtag_hint_stats:
                continue
            hints = sorted(weak_hashtag_hint_stats[bucket].items(), key=lambda kv: (-kv[1], kv[0]))
            weak_parts.append(f"{bucket}: " + ", ".join(f"{iso}={n_hits}" for iso, n_hits in hints[:5]))
        if weak_parts:
            lines.append("WEAK HASHTAG LANGUAGE CUES (not decisive alone; usable as low-confidence support if repeated and not contradicted): " + " | ".join(weak_parts))
    if repeated_pattern_stats:
        repeat_parts = []
        for bucket in ["video_title", "description", "channel_name", "other"]:
            if bucket not in repeated_pattern_stats:
                continue
            repeats = sorted(
                (stats for stats in repeated_pattern_stats[bucket].values() if stats["n"] > 1),
                key=lambda stats: (-stats["n"], stats["sample"]),
            )
            if repeats:
                repeat_parts.append(
                    f"{bucket}: "
                    + " | ".join(f"x{stats['n']} {stats['sample']}" for stats in repeats[:4])
                )
        if repeat_parts:
            lines.append("REPEATED PATTERNS (Tier 3 only when non-boilerplate; generic templates remain Tier 7): " + " || ".join(repeat_parts))
    if name:
        lines.append(f"CHANNEL NAME (Tier 6; lower priority than prose/phrases/tags): {name[0]}")
    if titles:
        lines.append("VIDEO TITLES:")
        lines += [f"- {t}" for t in titles]
    if descs:
        lines.append("DESCRIPTIONS:")
        lines += [f"- {d}" for d in descs]
    if other and not (titles or descs):
        lines += [f"- {o}" for o in other]
    lines.append("(Provider metadata, generic URLs, and generic hashtags may have been removed before this prompt. Apply the evidence priority hierarchy before field weights: prose and phrases outrank repeated dates, hashtags, channel names, and boilerplate. Items tagged [fasttext-ineligible-visible-text: ...] were too short or otherwise ineligible for fastText; they are visible text, not invalid language evidence.)")
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
    .withColumn("prompt_version", F.lit(PROMPT_VERSION))
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


def _openai_reasoning_effort_for_model(model: Optional[str]) -> str:
    effort = OPENAI_REASONING_EFFORT_BY_MODEL.get(str(model or ""), OPENAI_REASONING_EFFORT)
    return "" if str(effort or "").strip().lower() == "none" else effort


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
            reasoning_effort = _openai_reasoning_effort_for_model(model)
            if reasoning_effort:
                body["reasoning"] = {"effort": reasoning_effort}
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

_using_existing_requests = False
_reuse_existing_requests_reason = None
if IMPORT_RESULTS and REUSE_EXISTING_REQUESTS_ON_IMPORT:
    _reuse_existing_requests_reason = "import_results=true"
elif SUBMIT_BATCHES and REUSE_EXISTING_REQUESTS_ON_SUBMIT:
    _reuse_existing_requests_reason = "submit_batches=true"

if _reuse_existing_requests_reason and _table_exists_full(panel_requests_full):
    existing_run_requests = spark.table(panel_requests_full).where(F.col("run_id") == F.lit(RUN_ID))
    if existing_run_requests.limit(1).count() > 0:
        existing_request_cols = set(existing_run_requests.columns)
        prompt_version_ok_for_submit = True
        if _reuse_existing_requests_reason == "submit_batches=true":
            if "prompt_version" not in existing_request_cols:
                prompt_version_ok_for_submit = False
            else:
                prompt_version_ok_for_submit = (
                    existing_run_requests
                    .where(F.coalesce(F.col("prompt_version"), F.lit("")) != F.lit(PROMPT_VERSION))
                    .limit(1)
                    .count()
                    == 0
                )
            if not prompt_version_ok_for_submit:
                print(
                    "Existing request table prompt_version does not match current prompt_version; "
                    "regenerating request rows for submit:",
                    panel_requests_full,
                )

        if prompt_version_ok_for_submit and REFRESH_REQUEST_PROVIDER_FILTER:
            refresh_filter = F.col("provider").isin(*sorted(REFRESH_REQUEST_PROVIDER_FILTER))
            if REFRESH_REQUEST_MODEL_FILTER:
                refresh_filter = refresh_filter & F.col("model").isin(*sorted(REFRESH_REQUEST_MODEL_FILTER))
            refreshed_requests = existing_run_requests.where(refresh_filter).withColumn(
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
            kept_requests = existing_run_requests.where(~refresh_filter)
            requests = kept_requests.unionByName(refreshed_requests, allowMissingColumns=True)
            print(
                "Refreshing stored request batch lines for providers",
                sorted(REFRESH_REQUEST_PROVIDER_FILTER),
                "models",
                sorted(REFRESH_REQUEST_MODEL_FILTER) if REFRESH_REQUEST_MODEL_FILTER else "ALL",
                "while preserving stored prompts.",
            )
        elif prompt_version_ok_for_submit:
            requests = existing_run_requests
            _using_existing_requests = True
            print(f"Reusing existing request table for {_reuse_existing_requests_reason}:", panel_requests_full)

if not _using_existing_requests:
    write_run_scoped(requests, panel_requests_full)
    print("Wrote request table to", panel_requests_full)
display(spark.table(panel_requests_full).where(F.col("run_id") == F.lit(RUN_ID)).groupBy("provider", "model").count())

# COMMAND ----------
batch_file_records = []
run_dir = os.path.join(BATCH_OUTPUT_DIR, RUN_ID)
streaming_deepseek_records = []


def write_request_chunk_jsonl(provider: str, model: str, chunk_id: int, local_path: str):
    subset = (
        spark.table(panel_requests_full)
        .where(
            (F.col("run_id") == F.lit(RUN_ID))
            & (F.col("provider") == F.lit(provider))
            & (F.col("model") == F.lit(model))
            & (F.col("chunk_id") == F.lit(int(chunk_id)))
        )
        .select("batch_line")
    )
    n = 0
    n_bytes = 0
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        for row in subset.toLocalIterator():
            line = row["batch_line"]
            f.write(line + "\n")
            n += 1
            n_bytes += len(line.encode("utf-8")) + 1
    print(f"Wrote {n:,} requests: {local_path} ({n_bytes:,} bytes)")
    record_panel_progress(
        "batch_jsonl_chunk_written",
        metrics={
            "provider": provider,
            "model": model,
            "chunk_id": int(chunk_id),
            "local_jsonl_path": local_path,
            "n_requests": n,
            "n_bytes": n_bytes,
        },
    )
    return n, n_bytes


if _using_existing_requests and not SUBMIT_BATCHES:
    print("Skipping batch JSONL rewrite while reusing existing requests for import_results=true.")
else:
    # Write JSONL batch files to DBFS (one per provider/model/chunk).
    os.makedirs(BATCH_OUTPUT_DIR, exist_ok=True)
    os.makedirs(run_dir, exist_ok=True)
    _run_requests = spark.table(panel_requests_full).where(F.col("run_id") == F.lit(RUN_ID))
    groups = _run_requests.select("provider", "model", "chunk_id").distinct().orderBy("provider", "model", "chunk_id").collect()
    for g in groups:
        provider, model, chunk_id = g["provider"], g["model"], int(g["chunk_id"])
        provider_dir = os.path.join(run_dir, provider, safe_model_dir(model))
        os.makedirs(provider_dir, exist_ok=True)
        local_path = os.path.join(provider_dir, f"chunk_{chunk_id:05d}.jsonl")
        if (
            SUBMIT_BATCHES
            and DEEPSEEK_DIRECT_STREAMING
            and str(provider).lower() == "deepseek"
        ):
            streaming_deepseek_records.append((RUN_ID, provider, model, chunk_id, local_path, None, None, datetime.utcnow().isoformat()))
            continue
        n, n_bytes = write_request_chunk_jsonl(provider, model, chunk_id, local_path)
        batch_file_records.append((RUN_ID, provider, model, chunk_id, local_path, n, n_bytes, datetime.utcnow().isoformat()))

    # D4: persist a batch-file registry (run-scoped, idempotent) so submission/import are auditable.
    if batch_file_records:
        batch_files_df = spark.createDataFrame(
            batch_file_records,
            ["run_id", "provider", "model", "chunk_id", "local_jsonl_path", "n_requests", "n_bytes", "created_at_utc"],
        )
        write_run_scoped(batch_files_df, panel_batch_files_full)
        print("Wrote batch-file registry to", panel_batch_files_full)
    if streaming_deepseek_records:
        print(
            f"DeepSeek direct streaming enabled; {len(streaming_deepseek_records):,} DeepSeek chunks "
            "will be written immediately before submission instead of staged up front."
        )
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
                            "reasoning_effort": DEEPSEEK_REASONING_EFFORT or None,
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
                                "reasoning_effort": DEEPSEEK_REASONING_EFFORT or None,
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
                    "reasoning_effort": DEEPSEEK_REASONING_EFFORT or None,
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
                    "reasoning_effort": DEEPSEEK_REASONING_EFFORT or None,
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
    record_panel_progress(
        "deepseek_direct_started",
        metrics={
            "model": model,
            "chunk_file": path,
            "result_path": result_path,
            "total_requests": total,
            "pending_requests": len(pending_lines),
            "preserved_success": len(existing_success_lines),
        },
    )
    if not pending_lines:
        print(f"DeepSeek direct {model}: all {total:,} requests already have successful results.")
    with open(result_path, "a", encoding="utf-8", buffering=1) as dst:
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

    success_ids = set()
    seen_ids = set()
    malformed_rows = 0
    with open(result_path, "r", encoding="utf-8") as final:
        for line in final:
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                custom_id = obj.get("custom_id")
                if custom_id:
                    seen_ids.add(custom_id)
                status_code = int(obj.get("response", {}).get("status_code", 500))
                if custom_id and not obj.get("error") and 200 <= status_code < 300:
                    success_ids.add(custom_id)
            except Exception:
                malformed_rows += 1

    n_success = len(success_ids)
    n_missing_success = max(0, total - n_success)
    status = "completed" if n_success == total else "partial_or_errors"
    record_panel_progress(
        "deepseek_direct_completed",
        status=status,
        metrics={
            "model": model,
            "chunk_file": path,
            "result_path": result_path,
            "total_requests": total,
            "successful_request_ids": n_success,
            "missing_successful_request_ids": n_missing_success,
            "seen_request_ids": len(seen_ids),
            "malformed_result_rows": malformed_rows,
        },
    )
    return {
        "provider_file_id": result_path,
        "provider_batch_id": f"deepseek-direct:{RUN_ID}:{safe_model_dir(model)}:{os.path.basename(path)}",
        "provider_status": f"{status}; ok={n_success}; error={n_missing_success}; malformed_rows={malformed_rows}",
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
    if not _table_exists_full(panel_batch_jobs_full):
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
    if SKIP_EXISTING_SUBMITTED_BATCHES:
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
    else:
        print("skip_existing_submitted_batches=false — existing batch-job records will be preserved but not skipped.")

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
        batch_file_records + streaming_deepseek_records,
        key=lambda r: (_submit_priority.get(str(r[1]), 99), str(r[2]), int(r[3])),
    )

    for rec in submission_records:
        _, provider, model, chunk_id, path, n, n_bytes, _ = rec
        if SUBMIT_PROVIDER_FILTER and str(provider) not in SUBMIT_PROVIDER_FILTER:
            print(provider, model, chunk_id, "not in submit_provider_filter; skipping")
            continue
        if SUBMIT_MODEL_FILTER and str(model) not in SUBMIT_MODEL_FILTER:
            print(provider, model, chunk_id, "not in submit_model_filter; skipping")
            continue
        if (str(provider), str(model), int(chunk_id)) in already_submitted:
            print(provider, model, chunk_id, "already submitted; skipping")
            continue
        streaming_deepseek_chunk = (
            DEEPSEEK_DIRECT_STREAMING
            and str(provider).lower() == "deepseek"
            and (n is None or n_bytes is None)
        )
        if streaming_deepseek_chunk:
            n, n_bytes = write_request_chunk_jsonl(str(provider), str(model), int(chunk_id), path)
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
            if streaming_deepseek_chunk and DEEPSEEK_DELETE_REQUEST_JSONL_AFTER_SUBMIT:
                try:
                    os.remove(local_fs_path(path))
                    print(f"Deleted streamed DeepSeek request JSONL after successful submit: {path}")
                    record_panel_progress(
                        "batch_jsonl_chunk_deleted",
                        metrics={
                            "provider": provider,
                            "model": model,
                            "chunk_id": int(chunk_id),
                            "local_jsonl_path": path,
                        },
                    )
                except FileNotFoundError:
                    pass
                except Exception as delete_exc:  # noqa: BLE001
                    print(f"WARNING: could not delete streamed DeepSeek request JSONL {path}: {delete_exc}")
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


def _split_language_label(label) -> Tuple[Optional[str], Optional[str]]:
    if not label:
        return None, None
    parts = str(label).strip().replace("-", "_").split("_")
    base = parts[0].strip().lower() if parts and parts[0].strip() else None
    script = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
    return base, script


def _clean_base_iso_value(value) -> Optional[str]:
    if not value:
        return None
    raw = str(value).strip().lower()
    if raw in {"", "null", "none"}:
        return None
    if raw in ARABIC_FAMILY_ISO:
        raw = "ara"
    raw = CANONICAL_BASE_ISO.get(raw, raw)
    if raw in NON_LANGUAGE_BASE_ISO:
        return None
    return raw if re.fullmatch(r"[a-z]{3}", raw) else None


def _clean_script_value(value) -> Optional[str]:
    if not value:
        return None
    raw = str(value).strip()
    raw_l = raw.lower()
    if raw_l in {"", "null", "none"}:
        return None
    mapped = SCRIPT_FAMILY_CANONICAL.get(raw_l)
    if mapped:
        return mapped
    return raw if re.fullmatch(r"[A-Z][a-z]{3}", raw) else None


def _clean_language_label_value(value) -> Optional[str]:
    base_raw, script_raw = _split_language_label(value)
    base = _clean_base_iso_value(base_raw)
    if not base:
        return None
    script = _clean_script_value(script_raw)
    return f"{base}_{script}" if script else base


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


def _language_label_list(value):
    labels = []
    for item in _string_list(value):
        cleaned = _clean_language_label_value(item)
        if cleaned:
            labels.append(cleaned)
    return labels


@F.udf(pred_schema)
def normalize_prediction_udf(raw_text: str):
    o = extract_first_json_object(raw_text)
    if not o:
        o = extract_partial_prediction_object(raw_text)
    if not o:
        return (None, None, None, None, None, None, None, None, None, [], None, None, "no_json_object")
    raw_label = o.get("primary_language_label")
    label_iso, label_script = _split_language_label(raw_label)
    # Prefer the constrained iso_Script label over separately emitted component fields when they conflict.
    # Some small models return internally inconsistent JSON such as primary_language_label=tel_Latn with
    # primary_language_iso639_3=hin; using the label keeps panel votes aligned with the requested output.
    iso = _clean_base_iso_value(label_iso) or _clean_base_iso_value(o.get("primary_language_iso639_3"))
    script = _clean_script_value(label_script) or _clean_script_value(o.get("primary_language_script"))
    label = f"{iso}_{script}" if iso and script else iso
    status = o.get("status")
    prediction_parse_error = None
    if str(status or "").strip().lower() == "classified" and not iso:
        prediction_parse_error = "invalid_language_label"
    return (
        label, iso, script, status,
        _to_nullable_bool(o.get("is_romanized")),
        _to_nullable_bool(o.get("is_high_risk_tail")),
        _to_nullable_bool(o.get("is_mixed_language")),
        _clean_language_label_value(o.get("secondary_language_label")),
        o.get("dialect_or_variant"),
        _language_label_list(o.get("mixed_languages")),
        o.get("confidence"),
        (o.get("evidence") or "")[:500],
        prediction_parse_error,
    )


_PROMPT_SCRIPT_SUMMARY_RE = re.compile(
    r"\b(latin|arabic|devanagari|gurmukhi|bengali|tamil|telugu|malayalam|kannada|gujarati|odia|sinhala|thai|lao|hangul|japanese|han|greek|cyrillic|hebrew)=(\d+)\b",
    flags=re.IGNORECASE,
)


def _prompt_line(prompt_user: str, prefix: str) -> str:
    for line in str(prompt_user or "").splitlines():
        if line.startswith(prefix):
            return line
    return ""


def _prompt_text_script_family_counts(prompt_user: str) -> Dict[str, int]:
    line = _prompt_line(prompt_user, "TEXT SCRIPT SUMMARY")
    counts: Dict[str, int] = {}
    for script_raw, n_raw in _PROMPT_SCRIPT_SUMMARY_RE.findall(line):
        family = SCRIPT_FAMILY_CANONICAL.get(script_raw.lower())
        if not family:
            continue
        try:
            counts[family] = counts.get(family, 0) + int(n_raw)
        except Exception:
            pass
    return counts


def _summary_label_counts(line: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for label, n_raw in re.findall(r"\b([a-z_]+)=([0-9]+)\b", str(line or ""), flags=re.IGNORECASE):
        try:
            counts[label.lower()] = counts.get(label.lower(), 0) + int(n_raw)
        except Exception:
            pass
    return counts


@F.udf(ArrayType(StringType()))
def prediction_quality_flags_udf(
    prompt_user: str,
    status: str,
    pred_base_iso: str,
    pred_script_family: str,
    confidence: str,
) -> List[str]:
    flags: List[str] = []
    status_l = str(status or "").strip().lower()
    iso = str(pred_base_iso or "").strip().lower()
    script = str(pred_script_family or "").strip()
    confidence_l = str(confidence or "").strip().lower()
    prompt = str(prompt_user or "")

    short_line = _prompt_line(prompt, "SHORT SENTENCE/PHRASE CUES")
    coherent_desc_line = _prompt_line(prompt, "COHERENT DESCRIPTION CUES")
    romanized_sa_line = _prompt_line(prompt, "ROMANIZED SOUTH ASIAN CUES")
    arabic_sa_line = _prompt_line(prompt, "ARABIC-SCRIPT URDU/PUNJABI CUES")
    topic_line = _prompt_line(prompt, "TOPIC/LANGUAGE-NAME MENTIONS")

    if status_l == "insufficient_text":
        if short_line:
            flags.append("insufficient_with_short_sentence_cues")
        if romanized_sa_line:
            flags.append("insufficient_with_romanized_south_asian_cues")
        if arabic_sa_line:
            flags.append("insufficient_with_arabic_script_south_asian_cues")
        return flags

    if status_l != "classified":
        return flags

    script_counts = _prompt_text_script_family_counts(prompt)
    if script and script_counts:
        predicted_count = script_counts.get(script, 0)
        top_script, top_count = max(script_counts.items(), key=lambda kv: kv[1])
        if predicted_count == 0 and top_count >= 12:
            flags.append("predicted_script_absent_from_prompt_text")
        elif script != top_script and top_count >= 40 and predicted_count < max(6, int(top_count * 0.15)):
            flags.append("review_predicted_script_is_minor_prompt_script")

    if iso == "ara" and arabic_sa_line:
        flags.append("review_arabic_prediction_with_urdu_punjabi_markers")

    romanized_sa_counts = _summary_label_counts(romanized_sa_line)
    south_asian_iso = {"hin", "urd", "pnb", "pan", "bho", "npi", "hne", "bns", "bra", "bgc", "raj", "mwr"}
    if iso == "eng" and sum(romanized_sa_counts.values()) >= 3:
        flags.append("review_english_prediction_with_repeated_south_asian_romanized_cues")
    if iso in south_asian_iso and confidence_l == "high" and romanized_sa_counts.get("hin_urd_shared", 0) >= 2:
        flags.append("review_high_confidence_script_blind_south_asian_prediction")

    topic_counts = _summary_label_counts(topic_line)
    if topic_counts and not short_line and not romanized_sa_line and not arabic_sa_line:
        flags.append("review_classified_from_topic_or_language_mentions_only_possible")
    if iso == "ara" and topic_counts.get("religious_topic_not_language", 0) and not coherent_desc_line and not arabic_sa_line:
        flags.append("review_religious_topic_only_language_inference_possible")

    return flags


_ROMANIZABLE_BASE_ISO = {
    "asm", "ben", "bgc", "bho", "bns", "bra", "brx", "guj", "hin", "hne", "kan",
    "kas", "mag", "mal", "mar", "mni", "npi", "ori", "ory", "pan", "pnb", "raj",
    "sat", "snd", "tam", "tel", "urd",
}

_BASE_ISO_COMPATIBLE_NATIVE_SCRIPTS = {
    "ara": {"Arab"},
    "asm": {"Beng"},
    "ben": {"Beng"},
    "bgc": {"Deva"},
    "bho": {"Deva"},
    "bns": {"Deva"},
    "bra": {"Deva"},
    "brx": {"Deva"},
    "bul": {"Cyrl"},
    "cmn": {"Hani"},
    "ell": {"Grek"},
    "fas": {"Arab"},
    "guj": {"Gujr"},
    "heb": {"Hebr"},
    "hin": {"Deva"},
    "hne": {"Deva"},
    "jpn": {"Jpan"},
    "kan": {"Knda"},
    "kas": {"Arab", "Deva"},
    "kaz": {"Cyrl"},
    "khm": {"Khmr"},
    "kir": {"Cyrl"},
    "kor": {"Hang"},
    "lao": {"Laoo"},
    "mag": {"Deva"},
    "mal": {"Mlym"},
    "mar": {"Deva"},
    "mkd": {"Cyrl"},
    "mni": {"Beng"},
    "mon": {"Cyrl"},
    "npi": {"Deva"},
    "ory": {"Orya"},
    "pan": {"Guru"},
    "pes": {"Arab"},
    "pnb": {"Arab"},
    "raj": {"Deva"},
    "rus": {"Cyrl"},
    "sat": {"Beng"},
    "snd": {"Arab", "Deva"},
    "sin": {"Sinh"},
    "srp": {"Cyrl"},
    "tam": {"Taml"},
    "tel": {"Telu"},
    "tgk": {"Cyrl"},
    "tha": {"Thai"},
    "urd": {"Arab"},
    "ukr": {"Cyrl"},
    "uzb": {"Cyrl"},
    "yue": {"Hani"},
}

_HINDI_BELT_REGIONAL_BASE_ISO = {"bgc", "bho", "hne", "mwr", "raj", "sck"}

_REGIONAL_HINDI_BELT_STRONG_MARKER_PATTERNS = {
    "bho": [
        r"\b(ba|bani|badu|tohar|hamar|rauwa|bhail|bhailu)\b",
    ],
    "hne": [
        r"\b(mor|mola|tor)\b",
    ],
    "mwr": [
        r"\b(mharo|mhari|mhare|tharo|thari|thare|ghani)\b",
        r"(म्हारो|म्हारी|म्हारे|थारो|थारी|थारे|घणी)",
    ],
    "raj": [
        r"\b(mharo|mhari|mhare|tharo|thari|thare|ghani)\b",
        r"(म्हारो|म्हारी|म्हारे|थारो|थारी|थारे|घणी)",
    ],
}

_REGIONAL_HINDI_BELT_STRONG_PHRASE_PATTERNS = {
    "bho": [r"\bka\s+ho\b"],
    "hne": [r"\bka\s+hoge\b"],
    "mwr": [r"\bpadharo\b", r"पधारो"],
    "raj": [r"\bpadharo\b", r"पधारो"],
}

calibration_schema = StructType([
    StructField("calibrated_status", StringType(), True),
    StructField("calibrated_language_label", StringType(), True),
    StructField("calibrated_base_iso", StringType(), True),
    StructField("calibrated_script", StringType(), True),
    StructField("calibrated_is_romanized", BooleanType(), True),
    StructField("calibrated_confidence", StringType(), True),
    StructField("calibration_flags", ArrayType(StringType()), True),
])


def _label_from_parts(base_iso: Optional[str], script: Optional[str]) -> Optional[str]:
    if base_iso and script:
        return f"{base_iso}_{script}"
    return base_iso or None


def _cap_confidence_value(confidence: str, max_confidence: str) -> str:
    order = {"low": 1, "medium": 2, "high": 3}
    inv = {1: "low", 2: "medium", 3: "high"}
    current = order.get(str(confidence or "").lower(), 1)
    cap = order.get(str(max_confidence or "").lower(), current)
    return inv[min(current, cap)]


def _dominant_non_latin_script(script_counts: Dict[str, int]) -> Optional[str]:
    counts = {script: int(n or 0) for script, n in (script_counts or {}).items() if int(n or 0) > 0}
    total = sum(counts.values())
    if total <= 0:
        return None
    native_counts = {script: n for script, n in counts.items() if script != "Latn"}
    if not native_counts:
        return None
    top_script, top_count = max(native_counts.items(), key=lambda kv: kv[1])
    return top_script if (top_count / float(total)) > 0.5 else None


def _compatible_native_script(base_iso: Optional[str], native_script: Optional[str]) -> bool:
    if not base_iso or not native_script:
        return False
    return native_script in _BASE_ISO_COMPATIBLE_NATIVE_SCRIPTS.get(base_iso, set())


def _prompt_evidence_text(prompt_user: str) -> str:
    summary_prefixes = (
        "FINAL FALLBACK MODE:",
        "NO USABLE NATURAL-LANGUAGE",
        "FIELD SUMMARY",
        "SEGMENT SCRIPT SUMMARY",
        "TEXT SCRIPT SUMMARY",
        "SHORT SENTENCE/PHRASE CUES",
        "COHERENT DESCRIPTION CUES",
        "CTA/CHANNEL BOILERPLATE",
        "ROMANIZED SOUTH ASIAN CUES",
        "ARABIC-SCRIPT URDU/PUNJABI CUES",
        "TOPIC/LANGUAGE-NAME MENTIONS",
        "LANGUAGE HINTS",
        "NON-GENERIC HASHTAGS",
        "LOCALIZED DATE/MONTH CUES",
        "REPEATED SHORT/TEMPLATE PATTERNS",
    )
    lines = []
    for line in str(prompt_user or "").splitlines():
        if line.startswith(summary_prefixes):
            continue
        if line.startswith("(Provider metadata,"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _has_regional_hindi_belt_markers(base_iso: Optional[str], prompt_user: str) -> bool:
    if base_iso not in _HINDI_BELT_REGIONAL_BASE_ISO:
        return False
    evidence = _prompt_evidence_text(prompt_user)
    for pattern in _REGIONAL_HINDI_BELT_STRONG_PHRASE_PATTERNS.get(base_iso, []):
        if re.search(pattern, evidence, flags=re.IGNORECASE):
            return True
    marker_hits = 0
    for pattern in _REGIONAL_HINDI_BELT_STRONG_MARKER_PATTERNS.get(base_iso, []):
        marker_hits += len(re.findall(pattern, evidence, flags=re.IGNORECASE))
    return marker_hits >= 2


def _hindi_belt_fallback_script(script_counts: Dict[str, int], current_script: Optional[str]) -> str:
    if script_counts.get("Deva", 0) >= 12 or current_script == "Deva":
        return "Deva"
    return "Latn"


@F.udf(calibration_schema)
def calibrate_llm_prediction_udf(
    prompt_user: str,
    status: str,
    pred_base_iso: str,
    pred_script_family: str,
    is_romanized: bool,
    confidence: str,
    prediction_quality_flags,
    apply_calibration: bool,
):
    status_l = str(status or "").strip().lower()
    base_iso = _clean_base_iso_value(pred_base_iso)
    script = _clean_script_value(pred_script_family)
    cal_status = status_l if status_l in {"classified", "insufficient_text"} else status
    cal_base_iso = base_iso
    cal_script = script
    cal_is_romanized = bool(is_romanized) if is_romanized is not None else False
    cal_confidence = str(confidence or "").strip().lower() or None
    cal_flags = []
    quality_flags = [str(x) for x in (prediction_quality_flags or []) if x]
    quality_flag_set = set(quality_flags)
    prompt = str(prompt_user or "")

    if not cal_confidence and cal_status == "classified":
        cal_confidence = "low"

    if not apply_calibration or cal_status != "classified" or not cal_base_iso:
        return (
            cal_status,
            _label_from_parts(cal_base_iso, cal_script) if cal_status == "classified" else None,
            cal_base_iso if cal_status == "classified" else None,
            cal_script if cal_status == "classified" else None,
            cal_is_romanized if cal_status == "classified" else False,
            cal_confidence,
            cal_flags,
        )

    script_counts = _prompt_text_script_family_counts(prompt)
    top_script = None
    if script_counts:
        top_script = max(script_counts.items(), key=lambda kv: kv[1])[0]
    dominant_native_script = _dominant_non_latin_script(script_counts)
    should_calibrate_latn_to_native = (
        cal_script == "Latn"
        and _compatible_native_script(cal_base_iso, dominant_native_script)
    )
    short_line = _prompt_line(prompt, "SHORT SENTENCE/PHRASE CUES")
    coherent_desc_line = _prompt_line(prompt, "COHERENT DESCRIPTION CUES")
    romanized_sa_line = _prompt_line(prompt, "ROMANIZED SOUTH ASIAN CUES")
    arabic_sa_line = _prompt_line(prompt, "ARABIC-SCRIPT URDU/PUNJABI CUES")
    topic_line = _prompt_line(prompt, "TOPIC/LANGUAGE-NAME MENTIONS")
    cta_line = _prompt_line(prompt, "CTA/CHANNEL BOILERPLATE")
    topic_only = bool(topic_line) and not (short_line or coherent_desc_line or romanized_sa_line or arabic_sa_line)

    if "predicted_script_absent_from_prompt_text" in quality_flag_set:
        if should_calibrate_latn_to_native:
            cal_script = dominant_native_script
            cal_is_romanized = False
            cal_confidence = _cap_confidence_value(cal_confidence, "medium")
            cal_flags.append("calibrated_latn_to_native_script")
            cal_flags.append("calibrated_confidence_cap_medium_script_correction")
        elif topic_only:
            cal_status = "insufficient_text"
            cal_base_iso = None
            cal_script = None
            cal_is_romanized = False
            cal_confidence = None
            cal_flags.append("calibrated_to_insufficient_topic_only_script_absent")
        elif (
            top_script == "Latn"
            and script_counts.get("Latn", 0) >= 12
            and cal_base_iso in _ROMANIZABLE_BASE_ISO
        ):
            cal_script = "Latn"
            cal_is_romanized = True
            cal_confidence = _cap_confidence_value(cal_confidence, "medium")
            cal_flags.append("calibrated_script_absent_to_latn")
            cal_flags.append("calibrated_confidence_cap_medium_script_correction")
        else:
            cal_flags.append("review_script_absent_calibration_not_applied")

    if cal_status == "classified":
        if should_calibrate_latn_to_native and cal_script == "Latn":
            cal_script = dominant_native_script
            cal_is_romanized = False
            cal_confidence = _cap_confidence_value(cal_confidence, "medium")
            cal_flags.append("calibrated_latn_to_native_script")
            cal_flags.append("calibrated_confidence_cap_medium_script_correction")
        if (
            cal_base_iso in _HINDI_BELT_REGIONAL_BASE_ISO
            and not _has_regional_hindi_belt_markers(cal_base_iso, prompt)
        ):
            fallback_script = _hindi_belt_fallback_script(script_counts, cal_script)
            cal_base_iso = "hin"
            cal_script = fallback_script
            cal_is_romanized = fallback_script == "Latn"
            cal_confidence = _cap_confidence_value(cal_confidence, "medium")
            cal_flags.append("calibrated_regional_hindi_belt_to_hin")
            cal_flags.append("calibrated_confidence_cap_medium_regional_hindi_belt")
        if "review_high_confidence_script_blind_south_asian_prediction" in quality_flag_set:
            new_confidence = _cap_confidence_value(cal_confidence, "medium")
            if new_confidence != cal_confidence:
                cal_flags.append("calibrated_confidence_cap_medium_script_blind")
            cal_confidence = new_confidence
        if "review_classified_from_topic_or_language_mentions_only_possible" in quality_flag_set and not coherent_desc_line:
            new_confidence = _cap_confidence_value(cal_confidence, "low")
            if new_confidence != cal_confidence:
                cal_flags.append("calibrated_confidence_cap_low_topic_only")
            cal_confidence = new_confidence
        if cta_line and not (coherent_desc_line or romanized_sa_line or arabic_sa_line):
            new_confidence = _cap_confidence_value(cal_confidence, "low")
            if new_confidence != cal_confidence:
                cal_flags.append("calibrated_confidence_cap_low_cta_boilerplate")
            cal_confidence = new_confidence

    return (
        cal_status,
        _label_from_parts(cal_base_iso, cal_script) if cal_status == "classified" else None,
        cal_base_iso if cal_status == "classified" else None,
        cal_script if cal_status == "classified" else None,
        cal_is_romanized if cal_status == "classified" else False,
        cal_confidence,
        cal_flags,
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
            F.col("prompt_user").alias("_request_prompt_user"),
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
        .withColumn("_pred_iso_raw", F.lower(F.trim(F.col("primary_language_iso639_3"))))
        .withColumn("_pred_iso_from_label", F.lower(F.trim(F.split("primary_language_label", "_").getItem(0))))
        .withColumn("_pred_iso_raw", F.when(F.col("_pred_iso_raw").isin("", "null", "none"), F.lit(None)).otherwise(F.col("_pred_iso_raw")))
        .withColumn("_pred_iso_from_label", F.when(F.col("_pred_iso_from_label").isin("", "null", "none"), F.lit(None)).otherwise(F.col("_pred_iso_from_label")))
        .withColumn("pred_base_iso", F.coalesce(F.col("_pred_iso_raw"), F.col("_pred_iso_from_label")))
        .withColumn("_pred_script_from_label", script_from_label_expr(F.col("primary_language_label")))
        .withColumn("pred_script_family", script_family_expr(F.coalesce(F.col("primary_language_script"), F.col("_pred_script_from_label"))))
        .withColumn("pred_normalized_base_iso", canonical_base_iso_expr(F.col("pred_base_iso")))
        .withColumn("pred_normalized_language_label", normalized_language_label_expr(F.col("pred_base_iso"), F.col("pred_script_family")))
        .withColumn(
            "prediction_quality_flags",
            prediction_quality_flags_udf(
                F.col("_request_prompt_user"),
                F.col("status"),
                F.col("pred_base_iso"),
                F.col("pred_script_family"),
                F.col("confidence"),
            ),
        )
        .withColumn(
            "_calibrated_prediction",
            calibrate_llm_prediction_udf(
                F.col("_request_prompt_user"),
                F.col("status"),
                F.col("pred_base_iso"),
                F.col("pred_script_family"),
                F.col("is_romanized"),
                F.col("confidence"),
                F.col("prediction_quality_flags"),
                F.lit(APPLY_LLM_CALIBRATION),
            ),
        )
        .select("*", "_calibrated_prediction.*")
        .drop("_calibrated_prediction")
        .withColumn("calibrated_normalized_base_iso", canonical_base_iso_expr(F.col("calibrated_base_iso")))
        .withColumn("calibrated_normalized_language_label", normalized_language_label_expr(F.col("calibrated_base_iso"), F.col("calibrated_script")))
        .drop(
            "_request_run_id", "_request_provider", "_request_model", "_request_model_tier",
            "_request_channel_id", "_request_prompt_user",
            "_pred_iso_raw", "_pred_iso_from_label", "_pred_script_from_label",
        )
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
            (F.col("calibrated_normalized_base_iso").isNotNull())
            & (F.lower(F.coalesce(F.col("calibrated_status").cast("string"), F.lit(""))) == F.lit("classified"))
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
            F.col("calibrated_language_label").alias("language_label"),
            F.col("calibrated_base_iso").alias("base_iso"),
            F.col("calibrated_normalized_base_iso").alias("normalized_base_iso"),
            F.col("calibrated_normalized_language_label").alias("normalized_language_label"),
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
    _panel_vote_iso_source = "calibrated_normalized_base_iso" if PANEL_MAJORITY_VOTE_BASIS == "normalized_base_iso" else "calibrated_base_iso"
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
    _conf_rank = F.when(F.col("calibrated_confidence") == "high", 3).when(F.col("calibrated_confidence") == "medium", 2).when(F.col("calibrated_confidence") == "low", 1).otherwise(0)
    _empty_string_array = F.from_json(F.lit("[]"), ArrayType(StringType()))
    winners = votes.join(top_iso, on="channel_id", how="inner").where(F.col("_panel_vote_iso") == F.col("panel_majority_vote_iso"))
    lbl = winners.groupBy("channel_id", "calibrated_language_label").agg(
        F.count(F.lit(1)).alias("lbl_n"),
        F.max(_conf_rank).alias("conf_rank"),
        F.first("calibrated_script", ignorenulls=True).alias("panel_language_script_from_model"),
        F.first("calibrated_normalized_language_label", ignorenulls=True).alias("panel_normalized_language_label_from_model"),
        F.first("secondary_language_label", ignorenulls=True).alias("panel_secondary_language_label"),
        F.first("dialect_or_variant", ignorenulls=True).alias("panel_dialect_or_variant"),
        F.array_distinct(F.flatten(F.collect_list(F.coalesce(F.col("mixed_languages"), _empty_string_array)))).alias("panel_mixed_languages"),
        F.max(F.col("is_mixed_language").cast("int")).alias("_mixed_int"),
        F.max(F.col("calibrated_is_romanized").cast("int")).alias("_romanized_int"),
        F.first("evidence", ignorenulls=True).alias("panel_evidence"),
    )
    w_lbl = Window.partitionBy("channel_id").orderBy(F.desc("lbl_n"), F.desc("conf_rank"), F.asc("calibrated_language_label"))
    full = (lbl.withColumn("_rk", F.row_number().over(w_lbl)).where(F.col("_rk") == 1)
            .withColumn("panel_confidence", F.when(F.col("conf_rank") == 3, F.lit("high"))
                        .when(F.col("conf_rank") == 2, F.lit("medium"))
                        .when(F.col("conf_rank") == 1, F.lit("low")))
            .select("channel_id", F.col("calibrated_language_label").alias("panel_language_label"),
                    "panel_language_script_from_model", "panel_normalized_language_label_from_model",
                    "panel_secondary_language_label",
                    "panel_dialect_or_variant", "panel_mixed_languages", "panel_confidence",
                    "_mixed_int", "_romanized_int", "panel_evidence"))
    # Per-provider labels + reach (full predictions preserved per provider).
    prov = parsed.groupBy("channel_id").agg(
        F.first(F.when((F.col("provider") == "openai") & (F.col("is_valid_panel_vote") == F.lit(True)), F.col("calibrated_language_label")), ignorenulls=True).alias("openai_label"),
        F.first(F.when((F.col("provider") == "anthropic") & (F.col("is_valid_panel_vote") == F.lit(True)), F.col("calibrated_language_label")), ignorenulls=True).alias("anthropic_label"),
        F.first(F.when((F.col("provider") == "gemini") & (F.col("is_valid_panel_vote") == F.lit(True)), F.col("calibrated_language_label")), ignorenulls=True).alias("gemini_label"),
        F.first(F.when((F.col("provider") == "deepseek") & (F.col("is_valid_panel_vote") == F.lit(True)), F.col("calibrated_language_label")), ignorenulls=True).alias("deepseek_label"),
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
        .withColumn("panel_language_script", F.coalesce(F.col("panel_language_script_from_model"), safe_split_get_expr(F.col("panel_language_label"), "_", 1)))
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
