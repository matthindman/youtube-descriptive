# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube LID v3 — LLM adjudication panel (companion to notebook 01)
# MAGIC
# MAGIC **Run order:** run `01_language_openlid_v3_databricks` first (writes the `yt_lid_v3_*` tables),
# MAGIC then run this notebook. This notebook does **not** re-run the fastText models.
# MAGIC
# MAGIC **What it does:** routes the small subset of channels where the two fastText models *disagree*
# MAGIC (plus a tiny blind *audit* sample of the agreement bucket) to a three-LLM panel — OpenAI `gpt-5.5`,
# MAGIC Anthropic `claude-opus-4-7`, Gemini `gemini-3.1-pro-preview` — adjudicates the written-metadata
# MAGIC language, and reconciles a panel verdict by majority vote. It runs the panel **only on disagreement
# MAGIC or audit cases**, never on the whole population.
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
# MAGIC %pip install "openai>=2.0.0" anthropic "google-genai>=1.51.0" pandas pyarrow tenacity
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
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
_create_text_widget("run_id", "default")
_create_text_widget("inference_hash_buckets", "4096")

# Output tables.
_create_text_widget("panel_requests_table", "yt_lid_v3_llm_panel_requests")
_create_text_widget("panel_batch_jobs_table", "yt_lid_v3_llm_panel_batch_jobs")
_create_text_widget("panel_raw_results_table", "yt_lid_v3_llm_panel_raw_results")
_create_text_widget("panel_verdicts_table", "yt_lid_v3_llm_panel_verdicts")

# --- Routing controls (ONLY disagreement + audit cases) ---
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

# Models (three frontier panelists by default).
DEFAULT_MODELS_JSON = json.dumps([
    {"provider": "openai", "model": "gpt-5.5"},
    {"provider": "anthropic", "model": "claude-opus-4-7"},
    {"provider": "gemini", "model": "gemini-3.1-pro-preview"},
], ensure_ascii=False)
_create_text_widget("models_json", DEFAULT_MODELS_JSON)
_create_text_widget("max_output_tokens", "400")
_create_text_widget("temperature", "")  # blank = provider default
_create_text_widget("openai_endpoint_mode", "auto")
_create_text_widget("openai_reasoning_effort", "minimal")
_create_text_widget("gemini_thinking_level", "low")

# Batch I/O.
_create_text_widget("batch_output_dir", "/dbfs/FileStore/youtube_lid_panel_batches")
_create_text_widget("max_requests_per_file", "10000")
_create_text_widget("submit_batches", "false")
_create_text_widget("import_results", "false")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results")
_create_text_widget("secret_scope", "llm-api-keys")
_create_text_widget("openai_secret_key", "openai_api_key")
_create_text_widget("anthropic_secret_key", "anthropic_api_key")
_create_text_widget("gemini_secret_key", "gemini_api_key")

# COMMAND ----------
CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
COMPARISON_TABLE = _get_widget("comparison_table", "yt_lid_v3_channel_model_comparison")
SEGMENTS_INPUT_TABLE = _get_widget("segments_input_table", "yt_lid_v3_segments_input")
CHANNEL_TEXT_FEATURES_TABLE = _get_widget("channel_text_features_table", "yt_lid_v3_channel_text_features")
RUN_ID = _get_widget("run_id", "default").strip() or "default"
INFERENCE_HASH_BUCKETS = _get_int_widget("inference_hash_buckets", 4096)

PANEL_REQUESTS_TABLE = _get_widget("panel_requests_table", "yt_lid_v3_llm_panel_requests")
PANEL_BATCH_JOBS_TABLE = _get_widget("panel_batch_jobs_table", "yt_lid_v3_llm_panel_batch_jobs")
PANEL_RAW_RESULTS_TABLE = _get_widget("panel_raw_results_table", "yt_lid_v3_llm_panel_raw_results")
PANEL_VERDICTS_TABLE = _get_widget("panel_verdicts_table", "yt_lid_v3_llm_panel_verdicts")

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

MODELS = json.loads(_get_widget("models_json", DEFAULT_MODELS_JSON))
MAX_OUTPUT_TOKENS = _get_int_widget("max_output_tokens", 400)
TEMPERATURE = _get_optional_float_widget("temperature", None)
OPENAI_ENDPOINT_MODE = _get_widget("openai_endpoint_mode", "auto").strip().lower()
OPENAI_REASONING_EFFORT = _get_widget("openai_reasoning_effort", "minimal").strip()
GEMINI_THINKING_LEVEL = _get_widget("gemini_thinking_level", "low").strip()

BATCH_OUTPUT_DIR = _get_widget("batch_output_dir", "/dbfs/FileStore/youtube_lid_panel_batches")
MAX_REQUESTS_PER_FILE = _get_int_widget("max_requests_per_file", 10000)
SUBMIT_BATCHES = _get_bool_widget("submit_batches", False)
IMPORT_RESULTS = _get_bool_widget("import_results", False)
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results")
SECRET_SCOPE = _get_widget("secret_scope", "llm-api-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai_api_key")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic_api_key")
GEMINI_SECRET_KEY = _get_widget("gemini_secret_key", "gemini_api_key")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", model or "model")


comparison_full = fqtn(COMPARISON_TABLE)
segments_input_full = fqtn(SEGMENTS_INPUT_TABLE)
channel_text_features_full = fqtn(CHANNEL_TEXT_FEATURES_TABLE)
panel_requests_full = fqtn(PANEL_REQUESTS_TABLE)
panel_batch_jobs_full = fqtn(PANEL_BATCH_JOBS_TABLE)
panel_raw_results_full = fqtn(PANEL_RAW_RESULTS_TABLE)
panel_verdicts_full = fqtn(PANEL_VERDICTS_TABLE)
panel_batch_files_full = fqtn(PANEL_REQUESTS_TABLE + "_batch_files")

# D4: idempotent, run-scoped writes — re-running the same run_id overwrites only its own partition,
# never the whole table, so prior runs are preserved.
try:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
except Exception:
    pass


def write_run_scoped(df, table_full, extra_partitions=None):
    if "run_id" not in df.columns:
        df = df.withColumn("run_id", F.lit(RUN_ID))
    parts = ["run_id"] + list(extra_partitions or [])
    writer = df.write.format("delta").mode("overwrite").partitionBy(*parts)
    if not spark.catalog.tableExists(table_full.replace("`", "")):
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(table_full)

# Arabic macrolanguage + dialects collapsed to one family for the "exclude taxonomy artifact" filter.
ARABIC_FAMILY_ISO = {"ara", "arb", "ary", "arz", "apc", "ars", "ajp", "aeb", "acm", "acq", "aec", "afb", "ayl", "ayn"}
# South-Asian source language codes used to flag the romanized-Indic shared-bias route (D3).
SOURCE_INDIC_CODES = {"hi", "hi-in", "hin", "ne", "ne-np", "npi", "bho", "ur", "ur-pk", "pa", "gu", "mr", "bn", "ta", "te", "kn", "ml", "or", "si"}

print("Source comparison table:", comparison_full, "| run_id:", RUN_ID)
print("Panel models:", ", ".join(f"{m['provider']}:{m['model']}" for m in MODELS))
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

WEIGH the evidence by field, highest first: video_title (2.0), video_description (1.0), channel_description (1.0), channel_name (0.25). A field is decisive only with enough clean letters (>=40 Latin / >=12 non-Latin).

GUARD against known failure modes:
- LATIN-NAME TRAP: do not let an English/Latin channel NAME override video titles that are mostly non-Latin. If titles are mostly Thai/Korean/Arabic/etc., that is the language even when the brand name is Latin.
- ROMANIZED NON-LATIN: detect romanized Hindi/Urdu/Punjabi/Arabic; label the underlying language with _Latn, is_romanized=true; do not default to English.
- ENGLISH vs CREOLE: standard English is eng_Latn; only use jam_Latn/pcm_Latn with genuine creole grammar/lexis.
- MINORITY OVER-PREDICTION: be conservative with rare Romance/minority tail labels (srd, ast, vec, gug, lim, scn, glg, eus); a few ambiguous Latin words are usually Spanish/Italian/Portuguese/English. Set is_high_risk_tail=true if you do assign one.

NORMALIZE TAXONOMY: report Arabic as the macrolanguage ara_Arab (put a known dialect in dialect_or_variant); use cmn for Mandarin with the script in the tag; distinguish ind vs zsm only with clear evidence.

MIXED LANGUAGE: if a second language recurs across multiple fields, set secondary_language_label, is_mixed_language=true, and list mixed_languages.

ABSTAIN rather than guess: if the supplied metadata has no usable text, status="insufficient_text" and leave labels null. Otherwise status="classified".

Base the judgment ONLY on the supplied text; quote the specific evidence. NEVER invent content. Return ONE JSON object, nothing else:
{"status":"classified|insufficient_text","primary_language_label":"iso_Script|null","primary_language_iso639_3":"iso|null","primary_language_script":"Script|null","is_romanized":true|false,"dialect_or_variant":"iso|null","is_high_risk_tail":true|false,"secondary_language_label":"iso_Script|null","is_mixed_language":true|false,"mixed_languages":["iso_Script"],"confidence":"high|medium|low","evidence":"1-2 sentences quoting the text that drove the decision"}"""

# Response JSON schema for providers that enforce structured output (OpenAI Responses / Gemini).
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
        "evidence": {"type": "string"},
    },
    "required": ["status", "primary_language_label", "is_romanized", "is_high_risk_tail",
                 "is_mixed_language", "confidence", "evidence"],
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Routing — select ONLY disagreement + audit channels from notebook 01's output

# COMMAND ----------
cmp_df = spark.table(comparison_full).where(
    (F.col("run_id") == F.lit(RUN_ID)) & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
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
]

route_frames = []

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
    # D3: both models agree on English, but Indic evidence contradicts. Reuse channel_text_features signals
    # (Devanagari metadata, romanized-Indic keywords) when available; fall back to source language code.
    text_feat_cols = set(spark.table(channel_text_features_full).columns) if spark.catalog.tableExists(channel_text_features_full.replace("`", "")) else set()
    sig = cmp_df.where(F.col("consensus_language_iso639_3") == F.lit("eng"))
    indic_signal = F.lit(False)
    if text_feat_cols:
        # D4: run-scope the auxiliary join (channel_text_features is per-run partitioned) and dedupe to one
        # row per channel, so we never pull rows from another run or fan out the comparison rows.
        tf = spark.table(channel_text_features_full)
        if "run_id" in text_feat_cols:
            tf = tf.where(F.col("run_id") == F.lit(RUN_ID))
        if "inference_hash_buckets" in text_feat_cols:
            tf = tf.where(F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
        tf = tf.select(
            "channel_id",
            *[c for c in ["contains_devanagari_metadata", "romanized_indic_keyword_count", "source_language_value"] if c in text_feat_cols],
        ).dropDuplicates(["channel_id"])
        sig = sig.join(tf, on="channel_id", how="left")
        if "contains_devanagari_metadata" in text_feat_cols:
            indic_signal = indic_signal | F.coalesce(F.col("contains_devanagari_metadata"), F.lit(False))
        if "romanized_indic_keyword_count" in text_feat_cols:
            indic_signal = indic_signal | (F.coalesce(F.col("romanized_indic_keyword_count"), F.lit(0)) > 0)
        if "source_language_value" in text_feat_cols:
            indic_signal = indic_signal | F.lower(F.col("source_language_value")).isin(*sorted(SOURCE_INDIC_CODES))
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
    raise ValueError("No routes enabled. Enable at least one route_* widget.")

# Union; if a channel matches multiple routes, keep the highest-priority reason.
_priority = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in {
    "disagreement": 0, "unresolved_tail": 1, "shared_bias_english_indic": 2, "agreement_audit": 3,
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
    .where((F.col("run_id") == F.lit(RUN_ID)) & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS)))
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


@F.udf(StringType())
def build_user_prompt(segments) -> str:
    if not segments:
        return "No channel metadata was found."
    # collect_list has no inherent order; sort deterministically so the per-type caps (and thus the
    # batch files / verdicts) are reproducible across reruns of the same run_id.
    segments = sorted(segments, key=lambda s: ((s["segment_type"] or ""), (s["text"] or "")))
    name, titles, descs, other = [], [], [], []
    invalid_marker = " [lid-invalid:"

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

    for s in segments:
        st = (s["segment_type"] or "").lower()
        txt = (s["text"] or "").strip()
        if not txt:
            continue
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
    titles, descs, other = _order(titles), _order(descs), _order(other)
    lines = []
    if name:
        lines.append(f"CHANNEL NAME: {name[0]}")
    if descs:
        lines.append("DESCRIPTIONS:")
        lines += [f"- {d}" for d in descs[:_max_descs]]
    if titles:
        lines.append("VIDEO TITLES:")
        lines += [f"- {t}" for t in titles[:_max_titles]]
    if other and not (titles or descs):
        lines += [f"- {o}" for o in other[:_max_titles]]
    lines.append("(Items tagged [lid-invalid: ...] failed the fastText eligibility rule; use the reason/letter/script diagnostics and weigh them as weak evidence.)")
    prompt = "Channel metadata to classify:\n" + "\n".join(lines)
    return prompt[:_prompt_max]


prompts = seg_by_channel.withColumn("prompt_user", build_user_prompt(F.col("segments"))).select("channel_id", "prompt_user")
routed_prompts = routed.join(prompts, on="channel_id", how="left").withColumn(
    "prompt_user", F.coalesce(F.col("prompt_user"), F.lit("No usable channel metadata was found."))
)

# Fan out to one request per (channel, model).
models_df = spark.createDataFrame([(m["provider"], m["model"]) for m in MODELS], ["provider", "model"])
requests = (
    routed_prompts.crossJoin(models_df)
    # D4: run-scope the request identity so results from other runs can't collide on import.
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("request_id", F.concat_ws("__", F.lit(RUN_ID), F.col("provider"), F.col("model"), F.col("channel_id")))
    .withColumn("system_prompt", F.lit(SYSTEM_PROMPT))
    .withColumn("temperature", F.lit(TEMPERATURE).cast("double") if TEMPERATURE is not None else F.lit(None).cast("double"))
    .withColumn("max_output_tokens", F.lit(MAX_OUTPUT_TOKENS))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Build provider batch lines (reuses notebook 02's request format)

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
        generation_config = {"max_output_tokens": max_out,
                             "response_format": {"text": {"mime_type": "application/json", "schema": LANG_RESPONSE_JSON_SCHEMA}}}
        if temp is not None:
            generation_config["temperature"] = temp
        if GEMINI_THINKING_LEVEL:
            generation_config["thinking_config"] = {"thinking_level": GEMINI_THINKING_LEVEL}
        obj = {"key": request_id, "request": {"system_instruction": {"parts": [{"text": system_prompt}]},
               "contents": [{"role": "user", "parts": [{"text": user_prompt}]}], "generation_config": generation_config}}
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
    with open(local_path, "w", encoding="utf-8") as f:
        for row in subset.toLocalIterator():
            f.write(row["batch_line"] + "\n")
            n += 1
    batch_file_records.append((RUN_ID, provider, model, chunk_id, local_path, n, datetime.utcnow().isoformat()))
    print(f"Wrote {n:,} requests: {local_path}")

# D4: persist a batch-file registry (run-scoped, idempotent) so submission/import are auditable.
if batch_file_records:
    batch_files_df = spark.createDataFrame(
        batch_file_records,
        ["run_id", "provider", "model", "chunk_id", "local_jsonl_path", "n_requests", "created_at_utc"],
    )
    write_run_scoped(batch_files_df, panel_batch_files_full)
    print("Wrote batch-file registry to", panel_batch_files_full)
print("Batch files written under", run_dir)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Optional: submit batches (set submit_batches=true; reads API keys from Databricks Secrets)

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
    return {"provider_batch_id": batch.id, "provider_status": getattr(batch, "status", None)}


def submit_anthropic_batch(path: str, model: str) -> Dict[str, Any]:
    import anthropic
    client = anthropic.Anthropic(api_key=get_secret(SECRET_SCOPE, ANTHROPIC_SECRET_KEY))
    payload = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
    batch = client.messages.batches.create(requests=payload)
    return {"provider_batch_id": batch.id, "provider_status": getattr(batch, "processing_status", None)}


def submit_gemini_batch(path: str, model: str) -> Dict[str, Any]:
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=get_secret(SECRET_SCOPE, GEMINI_SECRET_KEY))
    uploaded = client.files.upload(file=path, config=types.UploadFileConfig(display_name=f"{RUN_ID}_{safe_model_dir(model)}", mime_type="jsonl"))
    batch = client.batches.create(model=model, src=uploaded.name, config={"display_name": f"{RUN_ID}_{safe_model_dir(model)}"})
    return {"provider_batch_id": getattr(batch, "name", None), "provider_status": getattr(getattr(batch, "state", None), "name", None)}


batch_job_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("local_jsonl_path", StringType(), True),
    StructField("n_requests", IntegerType(), True),
    StructField("provider_batch_id", StringType(), True),
    StructField("provider_status", StringType(), True),
    StructField("submitted_at_utc", StringType(), True),
    StructField("recorded_at_utc", StringType(), True),
    StructField("submit_error", StringType(), True),
])

if SUBMIT_BATCHES:
    batch_job_records = []
    for rec in batch_file_records:
        _, provider, model, chunk_id, path, n, _ = rec
        submitted_at = datetime.utcnow().isoformat()
        try:
            res = {"openai": submit_openai_batch, "anthropic": submit_anthropic_batch, "gemini": submit_gemini_batch}[provider](path, model)
            print(provider, model, chunk_id, "submitted", res)
            batch_job_records.append((
                RUN_ID, provider, model, int(chunk_id), path, int(n), res.get("provider_batch_id"),
                res.get("provider_status"), submitted_at, datetime.utcnow().isoformat(), None,
            ))
        except Exception as e:
            err = repr(e)[:500]
            print(provider, model, chunk_id, "ERROR", err)
            batch_job_records.append((
                RUN_ID, provider, model, int(chunk_id), path, int(n), None,
                "submit_error", submitted_at, datetime.utcnow().isoformat(), err,
            ))
    if batch_job_records:
        batch_jobs_df = spark.createDataFrame(batch_job_records, batch_job_schema)
        write_run_scoped(batch_jobs_df, panel_batch_jobs_full)
        print("Wrote batch-job registry to", panel_batch_jobs_full)
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
    StructField("status", StringType(), True),
    StructField("is_romanized", BooleanType(), True),
    StructField("is_high_risk_tail", BooleanType(), True),
    StructField("is_mixed_language", BooleanType(), True),
    StructField("secondary_language_label", StringType(), True),
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


def _base_iso(label, iso):
    if iso:
        return str(iso).split("_")[0].lower()
    if label:
        return str(label).split("_")[0].lower()
    return None


@F.udf(pred_schema)
def normalize_prediction_udf(raw_text: str):
    o = extract_first_json_object(raw_text)
    if not o:
        return (None, None, None, None, None, None, None, None, None, "no_json_object")
    label = o.get("primary_language_label")
    iso = o.get("primary_language_iso639_3") or _base_iso(label, None)
    return (
        label, iso, o.get("status"),
        bool(o.get("is_romanized")) if o.get("is_romanized") is not None else None,
        bool(o.get("is_high_risk_tail")) if o.get("is_high_risk_tail") is not None else None,
        bool(o.get("is_mixed_language")) if o.get("is_mixed_language") is not None else None,
        o.get("secondary_language_label"),
        o.get("confidence"),
        (o.get("evidence") or "")[:500],
        None,
    )


if IMPORT_RESULTS:
    # D4: recurse into results_input_dir (downloaded provider result files may be nested).
    raw = (
        spark.read.option("recursiveFileLookup", "true").text(RESULTS_INPUT_DIR)
        .withColumnRenamed("value", "line").where(F.length("line") > 2)
    )
    parsed = raw.withColumn("p", extract_provider_text_udf(F.col("line"))).select("p.*")
    parsed = parsed.withColumn("pred", normalize_prediction_udf(F.col("raw_text"))).select("*", "pred.*").drop("pred")
    # request_id = run_id__provider__model__channel_id -> recover parts; keep ONLY this run's results.
    rid = F.split("request_id", "__")
    parsed = (
        parsed
        .withColumn("result_run_id", rid.getItem(0))
        .withColumn("provider", rid.getItem(1))
        .withColumn("model", rid.getItem(2))
        .withColumn("channel_id", F.element_at(rid, -1))
        .withColumn("pred_base_iso", F.lower(F.coalesce(F.col("primary_language_iso639_3"), F.split("primary_language_label", "_").getItem(0))))
        .where(F.col("result_run_id") == F.lit(RUN_ID))
    )
    # D4: drop stale/orphan result lines by inner-joining to THIS run's request registry, and dedupe to one
    # result per request_id so duplicate result files can't inflate the panel vote count (one vote per model).
    run_request_ids = spark.table(panel_requests_full).where(F.col("run_id") == F.lit(RUN_ID)).select("request_id").distinct()
    parsed = (
        parsed.join(run_request_ids, on="request_id", how="inner")
        .withColumn("run_id", F.lit(RUN_ID))
        .dropDuplicates(["request_id"])
    )
    write_run_scoped(parsed, panel_raw_results_full)
    print("Wrote parsed per-model predictions to", panel_raw_results_full)

    # --- Reconcile: majority vote on base ISO, but PRESERVE the full winning label/script + side fields. ---
    n_models = len(MODELS)
    votes = parsed.where(F.col("pred_base_iso").isNotNull())
    per_iso = votes.groupBy("channel_id", "pred_base_iso").agg(F.count(F.lit(1)).alias("n_votes"))
    w_iso = Window.partitionBy("channel_id").orderBy(F.desc("n_votes"), F.asc("pred_base_iso"))
    top_iso = (per_iso.withColumn("_rk", F.row_number().over(w_iso)).where(F.col("_rk") == 1)
               .select("channel_id", F.col("pred_base_iso").alias("panel_language_iso"), "n_votes"))
    # Full winning label among the winning-ISO voters (mode; tie-break by confidence). Preserves script
    # (e.g. hin_Deva vs hin_Latn) and the side fields, not just the base ISO.
    _conf_rank = F.when(F.col("confidence") == "high", 3).when(F.col("confidence") == "medium", 2).when(F.col("confidence") == "low", 1).otherwise(0)
    winners = votes.join(top_iso, on="channel_id", how="inner").where(F.col("pred_base_iso") == F.col("panel_language_iso"))
    lbl = winners.groupBy("channel_id", "primary_language_label").agg(
        F.count(F.lit(1)).alias("lbl_n"),
        F.max(_conf_rank).alias("conf_rank"),
        F.first("secondary_language_label", ignorenulls=True).alias("panel_secondary_language_label"),
        F.max(F.col("is_mixed_language").cast("int")).alias("_mixed_int"),
        F.max(F.col("is_romanized").cast("int")).alias("_romanized_int"),
        F.first("evidence", ignorenulls=True).alias("panel_evidence"),
    )
    w_lbl = Window.partitionBy("channel_id").orderBy(F.desc("lbl_n"), F.desc("conf_rank"), F.asc("primary_language_label"))
    full = (lbl.withColumn("_rk", F.row_number().over(w_lbl)).where(F.col("_rk") == 1)
            .select("channel_id", F.col("primary_language_label").alias("panel_language_label"),
                    "panel_secondary_language_label", "_mixed_int", "_romanized_int", "panel_evidence"))
    # Per-provider labels + reach (full predictions preserved per provider).
    prov = parsed.groupBy("channel_id").agg(
        F.first(F.when(F.col("provider") == "openai", F.col("primary_language_label")), ignorenulls=True).alias("openai_label"),
        F.first(F.when(F.col("provider") == "anthropic", F.col("primary_language_label")), ignorenulls=True).alias("anthropic_label"),
        F.first(F.when(F.col("provider") == "gemini", F.col("primary_language_label")), ignorenulls=True).alias("gemini_label"),
        F.sum(F.when(F.col("pred_base_iso").isNotNull(), 1).otherwise(0)).alias("n_reached"),
        F.collect_set(F.when(F.col("pred_base_iso").isNotNull(), F.col("model"))).alias("panel_models"),
    )
    verdict = (
        routed
        .join(top_iso, on="channel_id", how="left")
        .join(full, on="channel_id", how="left")
        .join(prov, on="channel_id", how="left")
        .withColumn("panel_language_iso639_3", F.col("panel_language_iso"))
        .withColumn("panel_language_script", F.element_at(F.split("panel_language_label", "_"), 2))
        .withColumn("panel_is_mixed_language", F.coalesce(F.col("_mixed_int") == 1, F.lit(False)))
        .withColumn("panel_is_romanized", F.coalesce(F.col("_romanized_int") == 1, F.lit(False)))
        .withColumn("panel_status", F.when(F.col("panel_language_iso").isNull(), F.lit("no_panel_result"))
                    .when(F.col("n_votes") >= F.lit(max(2, (n_models // 2) + 1)), F.lit("panel_majority"))
                    .otherwise(F.lit("needs_human_review")))
        .withColumn("audit_sample", F.col("route_reason") == F.lit("agreement_audit"))
        # Audit rows are measurements: never overwrite consensus unless explicitly promoted later.
        .withColumn("consensus_source", F.when(F.col("audit_sample"), F.lit("audit_sample"))
                    .when(F.col("panel_status") == F.lit("panel_majority"), F.lit("llm_panel"))
                    .otherwise(F.lit("human_review")))
        .withColumn("prediction_timestamp", F.current_timestamp())
        .withColumn("run_id", F.lit(RUN_ID))
        .drop("_mixed_int", "_romanized_int")
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
            F.lower(F.split(F.coalesce(F.col("consensus_language_label"), F.lit("")), "_").getItem(0)) == F.col("panel_language_iso"),
        )
        print("Agreement-bucket audit (panel vs fastText consensus):")
        display(audit_eval.groupBy("panel_agrees_consensus").count())
else:
    print("import_results=false — set it true after downloading provider result JSONL files into results_input_dir.")
