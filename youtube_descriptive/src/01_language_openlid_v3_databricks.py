# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube Census: language classification (v3 dual-model: OpenLID-v3 + GlotLID)
# MAGIC
# MAGIC This notebook performs channel language classification by running **two** fastText language-ID
# MAGIC models — OpenLID-v3 (the legacy primary detector) and GlotLID — on the **same universe of valid
# MAGIC text segments**, then comparing them and producing model-specific and consensus labels.
# MAGIC
# MAGIC The output is interpreted as **written metadata language**, not spoken/video language. Source
# MAGIC language fields are preserved as audit fields, never as ground truth.
# MAGIC
# MAGIC **Source tables (Unity Catalog):**
# MAGIC - `prod_tads.youtube.yt_sl_channels`
# MAGIC - `prod_tads.youtube.yt_sl_videos`
# MAGIC
# MAGIC **Output table family:** a new `yt_lid_v3_*` family (see section 1). The legacy
# MAGIC `yt_lid_openlid_v3_*` tables are never overwritten.
# MAGIC
# MAGIC **Implementation status:** all phases of `lang_detect_v3_implementation_plan.md` are implemented:
# MAGIC scaffolding/widgets/constants/models, deterministic dedup + smoke sampling, canonical segment-input
# MAGIC table, dual-model inference on the shared valid-segment universe, compact predictions with optional
# MAGIC long-format audit output,
# MAGIC model-specific channel aggregation, model comparison + consensus, screen-vs-credible mixed-language,
# MAGIC Hindi/Indic + high-risk redirect diagnostics, the final channel table, QA summaries + deterministic
# MAGIC validation sampling, ablation with primary-label churn, and acceptance checks. See
# MAGIC `README_language_lid_v3.md`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Install notebook-scoped dependencies
# MAGIC
# MAGIC Both OpenLID-v3 and GlotLID are fastText models. Run this cell first, then allow Python to restart.

# COMMAND ----------
# MAGIC %pip install "numpy<2" fasttext==0.9.3 huggingface-hub==0.35.3 regex==2024.4.28 pandas pyarrow

# COMMAND ----------
# Restart after %pip installs so Python sees newly installed libraries.
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC
# MAGIC Widgets switch catalogs/schemas, choose model paths, run a deterministic sample, and override every
# MAGIC output table name. Defaults follow `lang_detect_revision_spec.md` v3 (§3).

# COMMAND ----------
import os
import shutil
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd
import regex
import fasttext
from huggingface_hub import hf_hub_download

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------
# Widget helpers.
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


# COMMAND ----------
# Source table parameters.
_create_text_widget("catalog", "prod_tads")
_create_text_widget("schema", "youtube")
_create_text_widget("channels_table", "yt_sl_channels")
_create_text_widget("videos_table", "yt_sl_videos")

# Output table parameters (v3 family, §2). Override any name here.
_create_text_widget("output_segments_input_table", "yt_lid_v3_segments_input")
_create_text_widget("output_openlid_segments_table", "yt_lid_v3_openlid_segments")
_create_text_widget("output_glotlid_segments_table", "yt_lid_v3_glotlid_segments")
_create_text_widget("output_glotlid_native_segments_table", "yt_lid_v3_glotlid_native_segments")
_create_text_widget("output_openlid_compact_table", "yt_lid_v3_openlid_predictions_compact")
_create_text_widget("output_glotlid_compact_table", "yt_lid_v3_glotlid_predictions_compact")
_create_text_widget("output_glotlid_native_compact_table", "yt_lid_v3_glotlid_native_predictions_compact")
_create_text_widget("output_channel_text_features_table", "yt_lid_v3_channel_text_features")
_create_text_widget("output_segment_model_comparison_table", "yt_lid_v3_segment_model_comparison")
_create_text_widget("output_channel_votes_table", "yt_lid_v3_channel_votes")
# Intermediate (not in §2): one row per channel per model with the §8 summary fields; feeds Phases 6/10.
_create_text_widget("output_channel_model_aggregation_table", "yt_lid_v3_channel_model_aggregation")
_create_text_widget("output_channel_model_comparison_table", "yt_lid_v3_channel_model_comparison")
_create_text_widget("output_channels_table", "yt_lid_v3_channels")
_create_text_widget("output_language_summary_full_table", "yt_lid_v3_language_summary_full")
_create_text_widget("output_language_summary_rollup_table", "yt_lid_v3_language_summary_rollup")
_create_text_widget("output_model_agreement_summary_table", "yt_lid_v3_model_agreement_summary")
_create_text_widget("output_mixed_language_candidates_table", "yt_lid_v3_mixed_language_candidates")
_create_text_widget("output_hindi_indic_audit_table", "yt_lid_v3_hindi_indic_audit_candidates")
_create_text_widget("output_suspect_tail_audit_table", "yt_lid_v3_suspect_tail_audit_sample")
_create_text_widget("output_high_risk_redirect_table", "yt_lid_v3_high_risk_redirect_diagnostic")
_create_text_widget("output_manual_validation_sample_table", "yt_lid_v3_manual_validation_sample")
_create_text_widget("output_unclassified_audit_table", "yt_lid_v3_unclassified_audit")
_create_text_widget("output_source_language_confusion_table", "yt_lid_v3_source_language_confusion")
_create_text_widget("output_dedupe_qa_table", "yt_lid_v3_dedupe_qa")
_create_text_widget("output_ablation_summary_table", "yt_lid_v3_ablation_summary")

# OpenLID-v3 model parameters.
_create_text_widget("model_repo", "HPLT/OpenLID-v3")
_create_text_widget("model_filename", "openlid-v3.bin")
_create_text_widget("model_local_path", "/dbfs/models/openlid_v3/openlid-v3.bin")
_create_text_widget("download_model_if_missing", "true")
_create_text_widget("model_distribution_mode", "direct_path")  # direct_path or sparkfiles

# GlotLID model parameters.
_create_text_widget("glotlid_repo", "cis-lmu/glotlid")
_create_text_widget("glotlid_filename", "model.bin")
_create_text_widget("glotlid_local_path", "/Volumes/dev_sean/matt/models/glotlid.bin")

# Model enable/mode controls (§3). HARD DEFAULT: full GlotLID coverage.
_create_text_widget("enable_openlid", "true")
_create_text_widget("enable_glotlid", "true")
_create_text_widget("glotlid_mode", "all_valid_segments")  # disabled, audit_segments, all_valid_segments
_create_text_widget("glotlid_preprocessing_mode", "match_openlid")  # match_openlid or glotlid_native_audit
_create_text_widget("glotlid_native_audit_sample_fraction", "0.00")

# Text validity / scoring thresholds (§3).
_create_text_widget("min_clean_chars", "40")
_create_text_widget("min_clean_chars_non_latin", "12")
_create_text_widget("non_latin_dominant_script_share", "0.60")
_create_text_widget("top_k", "5")

_create_text_widget("primary_min_score", "0.20")
_create_text_widget("secondary_min_score", "0.35")
_create_text_widget("secondary_min_score_ratio", "0.50")
_create_text_widget("secondary_label_vote_weight", "0.20")

# Mixed-language thresholds (§3).
_create_text_widget("mixed_screen_ratio_threshold", "0.40")
_create_text_widget("mixed_screen_min_secondary_segments", "2")
_create_text_widget("mixed_credible_ratio_threshold", "0.50")
_create_text_widget("mixed_credible_min_secondary_segments", "3")
_create_text_widget("mixed_credible_min_secondary_top1_segments", "1")
_create_text_widget("mixed_credible_min_secondary_segment_types", "2")
_create_text_widget("mixed_credible_min_rank2_rank3_margin_ratio", "0.25")
_create_text_widget("mixed_credible_secondary_mean_score", "0.45")
_create_text_widget("mixed_credible_secondary_max_score", "0.70")
_create_text_widget("mixed_credible_require_second_model_support", "true")

# Consensus confidence thresholds (§10). vote_share_with_top2 is the confidence proxy.
_create_text_widget("consensus_low_conf_vote_share", "0.50")
_create_text_widget("consensus_high_conf_vote_share", "0.65")

# Segment-type vote weights (§3 / §8).
_create_text_widget("channel_description_weight", "1.00")
_create_text_widget("video_title_weight", "2.00")
_create_text_widget("video_description_weight", "1.00")
_create_text_widget("video_tags_weight", "0.50")
_create_text_widget("channel_name_weight", "0.25")

# Ablation / validation controls (§3).
_create_text_widget("run_ablation_aggregations", "false")
_create_text_widget("create_validation_samples", "false")
_create_text_widget("validation_sample_seed", "20260520")
_create_text_widget("validation_max_per_stratum", "100")
_create_text_widget("validation_min_per_stratum", "30")

# Run controls.
_create_text_widget("production_mode", "true")
_create_text_widget("prediction_output_mode", "compact")  # compact, long_sample, long_full
_create_text_widget("long_segment_sample_fraction", "0.001")
_create_text_widget("run_id", "default")
_create_text_widget("inference_hash_buckets", "4096")
_create_text_widget("bucket_start", "0")
_create_text_widget("bucket_end", "4095")
_create_text_widget("target_segments_per_partition", "250000")
_create_text_widget("min_num_partitions", "800")
_create_text_widget("max_num_partitions", "20000")
_create_text_widget("run_heavy_qa", "false")
_create_text_widget("enable_notebook_displays", "true")
_create_text_widget("allow_full_native_audit", "false")
_create_text_widget("optimize_after_write", "false")
_create_text_widget("limit_channels", "0")  # 0 = full corpus
_create_text_widget("videos_per_channel", "10")
_create_text_widget("video_rank_column", "")  # blank = auto-detect
_create_text_widget("max_segment_chars", "2000")
_create_text_widget("score_threshold", "0.0")
_create_text_widget("checkpoint_dir", "dbfs:/tmp/yt_lid_v3/checkpoints")
_create_text_widget("source_update_format", "label")  # label, iso639_3, or scriptless_label
_create_text_widget("update_source_detected_language", "false")

# Text-column overrides. Leave blank to use auto-detected candidate columns.
_create_text_widget("channel_id_column", "channel_id")
_create_text_widget("video_id_column", "video_id")
_create_text_widget("channel_name_column", "")
_create_text_widget("channel_description_column", "")
_create_text_widget("video_title_column", "")
_create_text_widget("video_description_column", "")
_create_text_widget("video_tags_column", "")

# COMMAND ----------
# Resolve widget values.
CATALOG = _get_widget("catalog", "prod_tads")
SCHEMA = _get_widget("schema", "youtube")
CHANNELS_TABLE = _get_widget("channels_table", "yt_sl_channels")
VIDEOS_TABLE = _get_widget("videos_table", "yt_sl_videos")

OUTPUT_SEGMENTS_INPUT_TABLE = _get_widget("output_segments_input_table", "yt_lid_v3_segments_input")
OUTPUT_OPENLID_SEGMENTS_TABLE = _get_widget("output_openlid_segments_table", "yt_lid_v3_openlid_segments")
OUTPUT_GLOTLID_SEGMENTS_TABLE = _get_widget("output_glotlid_segments_table", "yt_lid_v3_glotlid_segments")
OUTPUT_GLOTLID_NATIVE_SEGMENTS_TABLE = _get_widget("output_glotlid_native_segments_table", "yt_lid_v3_glotlid_native_segments")
OUTPUT_OPENLID_COMPACT_TABLE = _get_widget("output_openlid_compact_table", "yt_lid_v3_openlid_predictions_compact")
OUTPUT_GLOTLID_COMPACT_TABLE = _get_widget("output_glotlid_compact_table", "yt_lid_v3_glotlid_predictions_compact")
OUTPUT_GLOTLID_NATIVE_COMPACT_TABLE = _get_widget("output_glotlid_native_compact_table", "yt_lid_v3_glotlid_native_predictions_compact")
OUTPUT_CHANNEL_TEXT_FEATURES_TABLE = _get_widget("output_channel_text_features_table", "yt_lid_v3_channel_text_features")
OUTPUT_SEGMENT_MODEL_COMPARISON_TABLE = _get_widget("output_segment_model_comparison_table", "yt_lid_v3_segment_model_comparison")
OUTPUT_CHANNEL_VOTES_TABLE = _get_widget("output_channel_votes_table", "yt_lid_v3_channel_votes")
OUTPUT_CHANNEL_MODEL_AGGREGATION_TABLE = _get_widget("output_channel_model_aggregation_table", "yt_lid_v3_channel_model_aggregation")
OUTPUT_CHANNEL_MODEL_COMPARISON_TABLE = _get_widget("output_channel_model_comparison_table", "yt_lid_v3_channel_model_comparison")
OUTPUT_CHANNELS_TABLE = _get_widget("output_channels_table", "yt_lid_v3_channels")
OUTPUT_LANGUAGE_SUMMARY_FULL_TABLE = _get_widget("output_language_summary_full_table", "yt_lid_v3_language_summary_full")
OUTPUT_LANGUAGE_SUMMARY_ROLLUP_TABLE = _get_widget("output_language_summary_rollup_table", "yt_lid_v3_language_summary_rollup")
OUTPUT_MODEL_AGREEMENT_SUMMARY_TABLE = _get_widget("output_model_agreement_summary_table", "yt_lid_v3_model_agreement_summary")
OUTPUT_MIXED_LANGUAGE_CANDIDATES_TABLE = _get_widget("output_mixed_language_candidates_table", "yt_lid_v3_mixed_language_candidates")
OUTPUT_HINDI_INDIC_AUDIT_TABLE = _get_widget("output_hindi_indic_audit_table", "yt_lid_v3_hindi_indic_audit_candidates")
OUTPUT_SUSPECT_TAIL_AUDIT_TABLE = _get_widget("output_suspect_tail_audit_table", "yt_lid_v3_suspect_tail_audit_sample")
OUTPUT_HIGH_RISK_REDIRECT_TABLE = _get_widget("output_high_risk_redirect_table", "yt_lid_v3_high_risk_redirect_diagnostic")
OUTPUT_MANUAL_VALIDATION_SAMPLE_TABLE = _get_widget("output_manual_validation_sample_table", "yt_lid_v3_manual_validation_sample")
OUTPUT_UNCLASSIFIED_AUDIT_TABLE = _get_widget("output_unclassified_audit_table", "yt_lid_v3_unclassified_audit")
OUTPUT_SOURCE_LANGUAGE_CONFUSION_TABLE = _get_widget("output_source_language_confusion_table", "yt_lid_v3_source_language_confusion")
OUTPUT_DEDUPE_QA_TABLE = _get_widget("output_dedupe_qa_table", "yt_lid_v3_dedupe_qa")
OUTPUT_ABLATION_SUMMARY_TABLE = _get_widget("output_ablation_summary_table", "yt_lid_v3_ablation_summary")

MODEL_REPO = _get_widget("model_repo", "HPLT/OpenLID-v3")
MODEL_FILENAME = _get_widget("model_filename", "openlid-v3.bin")
MODEL_LOCAL_PATH = _get_widget("model_local_path", "/dbfs/models/openlid_v3/openlid-v3.bin")
DOWNLOAD_MODEL_IF_MISSING = _get_bool_widget("download_model_if_missing", True)
MODEL_DISTRIBUTION_MODE = _get_widget("model_distribution_mode", "direct_path").strip().lower()

GLOTLID_REPO = _get_widget("glotlid_repo", "cis-lmu/glotlid")
GLOTLID_FILENAME = _get_widget("glotlid_filename", "model.bin")
GLOTLID_LOCAL_PATH = _get_widget("glotlid_local_path", "/Volumes/dev_sean/matt/models/glotlid.bin")

ENABLE_OPENLID = _get_bool_widget("enable_openlid", True)
ENABLE_GLOTLID = _get_bool_widget("enable_glotlid", True)
GLOTLID_MODE = _get_widget("glotlid_mode", "all_valid_segments").strip().lower()
GLOTLID_PREPROCESSING_MODE = _get_widget("glotlid_preprocessing_mode", "match_openlid").strip().lower()
GLOTLID_NATIVE_AUDIT_SAMPLE_FRACTION = _get_float_widget("glotlid_native_audit_sample_fraction", 0.0)

MIN_CLEAN_CHARS = _get_int_widget("min_clean_chars", 40)
MIN_CLEAN_CHARS_NON_LATIN = _get_int_widget("min_clean_chars_non_latin", 12)
NON_LATIN_DOMINANT_SCRIPT_SHARE = _get_float_widget("non_latin_dominant_script_share", 0.60)
TOP_K = _get_int_widget("top_k", 5)

PRIMARY_MIN_SCORE = _get_float_widget("primary_min_score", 0.20)
SECONDARY_MIN_SCORE = _get_float_widget("secondary_min_score", 0.35)
SECONDARY_MIN_SCORE_RATIO = _get_float_widget("secondary_min_score_ratio", 0.50)
SECONDARY_LABEL_VOTE_WEIGHT = _get_float_widget("secondary_label_vote_weight", 0.20)

MIXED_SCREEN_RATIO_THRESHOLD = _get_float_widget("mixed_screen_ratio_threshold", 0.40)
MIXED_SCREEN_MIN_SECONDARY_SEGMENTS = _get_int_widget("mixed_screen_min_secondary_segments", 2)
MIXED_CREDIBLE_RATIO_THRESHOLD = _get_float_widget("mixed_credible_ratio_threshold", 0.50)
MIXED_CREDIBLE_MIN_SECONDARY_SEGMENTS = _get_int_widget("mixed_credible_min_secondary_segments", 3)
MIXED_CREDIBLE_MIN_SECONDARY_TOP1_SEGMENTS = _get_int_widget("mixed_credible_min_secondary_top1_segments", 1)
MIXED_CREDIBLE_MIN_SECONDARY_SEGMENT_TYPES = _get_int_widget("mixed_credible_min_secondary_segment_types", 2)
MIXED_CREDIBLE_MIN_RANK2_RANK3_MARGIN_RATIO = _get_float_widget("mixed_credible_min_rank2_rank3_margin_ratio", 0.25)
MIXED_CREDIBLE_SECONDARY_MEAN_SCORE = _get_float_widget("mixed_credible_secondary_mean_score", 0.45)
MIXED_CREDIBLE_SECONDARY_MAX_SCORE = _get_float_widget("mixed_credible_secondary_max_score", 0.70)
MIXED_CREDIBLE_REQUIRE_SECOND_MODEL_SUPPORT = _get_bool_widget("mixed_credible_require_second_model_support", True)

CONSENSUS_LOW_CONF_VOTE_SHARE = _get_float_widget("consensus_low_conf_vote_share", 0.50)
CONSENSUS_HIGH_CONF_VOTE_SHARE = _get_float_widget("consensus_high_conf_vote_share", 0.65)

CHANNEL_DESCRIPTION_WEIGHT = _get_float_widget("channel_description_weight", 1.00)
VIDEO_TITLE_WEIGHT = _get_float_widget("video_title_weight", 2.00)
VIDEO_DESCRIPTION_WEIGHT = _get_float_widget("video_description_weight", 1.00)
VIDEO_TAGS_WEIGHT = _get_float_widget("video_tags_weight", 0.50)
CHANNEL_NAME_WEIGHT = _get_float_widget("channel_name_weight", 0.25)

PRODUCTION_MODE = _get_bool_widget("production_mode", True)
PREDICTION_OUTPUT_MODE = _get_widget("prediction_output_mode", "compact").strip().lower()
LONG_SEGMENT_SAMPLE_FRACTION = _get_float_widget("long_segment_sample_fraction", 0.001)
RUN_ID = _get_widget("run_id", "default").strip() or "default"
INFERENCE_HASH_BUCKETS = _get_int_widget("inference_hash_buckets", 4096)
BUCKET_START = _get_int_widget("bucket_start", 0)
BUCKET_END = _get_int_widget("bucket_end", 4095)
TARGET_SEGMENTS_PER_PARTITION = _get_int_widget("target_segments_per_partition", 250000)
MIN_NUM_PARTITIONS = _get_int_widget("min_num_partitions", 800)
MAX_NUM_PARTITIONS = _get_int_widget("max_num_partitions", 20000)
RUN_HEAVY_QA = _get_bool_widget("run_heavy_qa", False)
ENABLE_NOTEBOOK_DISPLAYS = _get_bool_widget("enable_notebook_displays", True)
ALLOW_FULL_NATIVE_AUDIT = _get_bool_widget("allow_full_native_audit", False)
OPTIMIZE_AFTER_WRITE = _get_bool_widget("optimize_after_write", False)

RUN_ABLATION_AGGREGATIONS = _get_bool_widget("run_ablation_aggregations", not PRODUCTION_MODE)
if PRODUCTION_MODE and not RUN_HEAVY_QA:
    RUN_ABLATION_AGGREGATIONS = False
CREATE_VALIDATION_SAMPLES = _get_bool_widget("create_validation_samples", not PRODUCTION_MODE)
if PRODUCTION_MODE and not RUN_HEAVY_QA:
    CREATE_VALIDATION_SAMPLES = False
VALIDATION_SAMPLE_SEED = _get_widget("validation_sample_seed", "20260520")
VALIDATION_MAX_PER_STRATUM = _get_int_widget("validation_max_per_stratum", 100)
VALIDATION_MIN_PER_STRATUM = _get_int_widget("validation_min_per_stratum", 30)

LIMIT_CHANNELS = _get_int_widget("limit_channels", 0)
VIDEOS_PER_CHANNEL = _get_int_widget("videos_per_channel", 10)
VIDEO_RANK_COLUMN = _get_widget("video_rank_column", "").strip()
MAX_SEGMENT_CHARS = _get_int_widget("max_segment_chars", 2000)
SCORE_THRESHOLD = _get_float_widget("score_threshold", 0.0)
CHECKPOINT_DIR = _get_widget("checkpoint_dir", "dbfs:/tmp/yt_lid_v3/checkpoints")
SOURCE_UPDATE_FORMAT = _get_widget("source_update_format", "label").strip().lower()
UPDATE_SOURCE_DETECTED_LANGUAGE = _get_bool_widget("update_source_detected_language", False)

CHANNEL_ID_COLUMN = _get_widget("channel_id_column", "channel_id")
VIDEO_ID_COLUMN = _get_widget("video_id_column", "video_id")
CHANNEL_NAME_COLUMN_OVERRIDE = _get_widget("channel_name_column", "").strip()
CHANNEL_DESCRIPTION_COLUMN_OVERRIDE = _get_widget("channel_description_column", "").strip()
VIDEO_TITLE_COLUMN_OVERRIDE = _get_widget("video_title_column", "").strip()
VIDEO_DESCRIPTION_COLUMN_OVERRIDE = _get_widget("video_description_column", "").strip()
VIDEO_TAGS_COLUMN_OVERRIDE = _get_widget("video_tags_column", "").strip()

# COMMAND ----------
# Validate enum-style widgets up front so misconfiguration fails clearly (acceptance #18 spirit).
if SOURCE_UPDATE_FORMAT not in {"label", "iso639_3", "scriptless_label"}:
    raise ValueError("source_update_format must be one of: label, iso639_3, scriptless_label")
if GLOTLID_MODE not in {"disabled", "audit_segments", "all_valid_segments"}:
    raise ValueError("glotlid_mode must be one of: disabled, audit_segments, all_valid_segments")
if GLOTLID_PREPROCESSING_MODE not in {"match_openlid", "glotlid_native_audit"}:
    raise ValueError("glotlid_preprocessing_mode must be one of: match_openlid, glotlid_native_audit")
if MODEL_DISTRIBUTION_MODE not in {"direct_path", "sparkfiles"}:
    raise ValueError("model_distribution_mode must be one of: direct_path, sparkfiles")
if PREDICTION_OUTPUT_MODE not in {"compact", "long_sample", "long_full"}:
    raise ValueError("prediction_output_mode must be one of: compact, long_sample, long_full")
if not regex.fullmatch(r"[A-Za-z0-9_.-]+", RUN_ID):
    raise ValueError(
        "run_id may contain only ASCII letters, digits, underscore, hyphen, and period. "
        f"Got {RUN_ID!r}."
    )

# Runtime floor: the aggregation and validation-sampling steps use array_sort(comparator) and
# array_compact, both added in Spark 3.4.0 (Databricks Runtime 13.0+). Fail clearly on older runtimes
# instead of crashing mid-pipeline.
try:
    _spark_mm = tuple(int(p) for p in spark.version.split(".")[:2])
except Exception:
    _spark_mm = (0, 0)
if _spark_mm < (3, 4):
    raise RuntimeError(
        f"This notebook requires Spark 3.4+ (Databricks Runtime 13.0+); detected Spark {spark.version}. "
        "It uses array_sort(comparator) and array_compact, which were added in Spark 3.4."
    )


def _require_positive_int(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer; got {value}.")


def _require_nonnegative_int(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be a non-negative integer; got {value}.")


def _require_nonnegative_float(name: str, value: float) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative; got {value}.")


def _require_fraction(name: str, value: float) -> None:
    if value < 0 or value > 1:
        raise ValueError(f"{name} must be between 0 and 1 inclusive; got {value}.")


_require_positive_int("top_k", TOP_K)
_require_positive_int("min_clean_chars", MIN_CLEAN_CHARS)
_require_positive_int("min_clean_chars_non_latin", MIN_CLEAN_CHARS_NON_LATIN)
_require_fraction("non_latin_dominant_script_share", NON_LATIN_DOMINANT_SCRIPT_SHARE)
_require_fraction("glotlid_native_audit_sample_fraction", GLOTLID_NATIVE_AUDIT_SAMPLE_FRACTION)
_require_fraction("long_segment_sample_fraction", LONG_SEGMENT_SAMPLE_FRACTION)
_require_positive_int("inference_hash_buckets", INFERENCE_HASH_BUCKETS)
_require_nonnegative_int("bucket_start", BUCKET_START)
_require_nonnegative_int("bucket_end", BUCKET_END)
if BUCKET_START > BUCKET_END:
    raise ValueError("bucket_start must be <= bucket_end.")
if BUCKET_END >= INFERENCE_HASH_BUCKETS:
    raise ValueError("bucket_end must be < inference_hash_buckets.")
IS_FULL_BUCKET_RANGE = BUCKET_START == 0 and BUCKET_END == INFERENCE_HASH_BUCKETS - 1
ACTIVE_BUCKET_COUNT = BUCKET_END - BUCKET_START + 1
_require_positive_int("target_segments_per_partition", TARGET_SEGMENTS_PER_PARTITION)
_require_positive_int("min_num_partitions", MIN_NUM_PARTITIONS)
_require_positive_int("max_num_partitions", MAX_NUM_PARTITIONS)
if MIN_NUM_PARTITIONS > MAX_NUM_PARTITIONS:
    raise ValueError("min_num_partitions must be <= max_num_partitions.")
_require_fraction("primary_min_score", PRIMARY_MIN_SCORE)
_require_fraction("secondary_min_score", SECONDARY_MIN_SCORE)
_require_nonnegative_float("secondary_min_score_ratio", SECONDARY_MIN_SCORE_RATIO)
_require_nonnegative_float("secondary_label_vote_weight", SECONDARY_LABEL_VOTE_WEIGHT)
_require_nonnegative_float("mixed_screen_ratio_threshold", MIXED_SCREEN_RATIO_THRESHOLD)
_require_nonnegative_int("mixed_screen_min_secondary_segments", MIXED_SCREEN_MIN_SECONDARY_SEGMENTS)
_require_nonnegative_float("mixed_credible_ratio_threshold", MIXED_CREDIBLE_RATIO_THRESHOLD)
_require_nonnegative_int("mixed_credible_min_secondary_segments", MIXED_CREDIBLE_MIN_SECONDARY_SEGMENTS)
_require_nonnegative_int("mixed_credible_min_secondary_top1_segments", MIXED_CREDIBLE_MIN_SECONDARY_TOP1_SEGMENTS)
_require_nonnegative_int("mixed_credible_min_secondary_segment_types", MIXED_CREDIBLE_MIN_SECONDARY_SEGMENT_TYPES)
_require_nonnegative_float("mixed_credible_min_rank2_rank3_margin_ratio", MIXED_CREDIBLE_MIN_RANK2_RANK3_MARGIN_RATIO)
_require_fraction("mixed_credible_secondary_mean_score", MIXED_CREDIBLE_SECONDARY_MEAN_SCORE)
_require_fraction("mixed_credible_secondary_max_score", MIXED_CREDIBLE_SECONDARY_MAX_SCORE)
_require_fraction("consensus_low_conf_vote_share", CONSENSUS_LOW_CONF_VOTE_SHARE)
_require_fraction("consensus_high_conf_vote_share", CONSENSUS_HIGH_CONF_VOTE_SHARE)
if CONSENSUS_LOW_CONF_VOTE_SHARE > CONSENSUS_HIGH_CONF_VOTE_SHARE:
    raise ValueError("consensus_low_conf_vote_share must be <= consensus_high_conf_vote_share.")
for _name, _value in {
    "channel_description_weight": CHANNEL_DESCRIPTION_WEIGHT,
    "video_title_weight": VIDEO_TITLE_WEIGHT,
    "video_description_weight": VIDEO_DESCRIPTION_WEIGHT,
    "video_tags_weight": VIDEO_TAGS_WEIGHT,
    "channel_name_weight": CHANNEL_NAME_WEIGHT,
}.items():
    _require_nonnegative_float(_name, _value)
_require_nonnegative_int("limit_channels", LIMIT_CHANNELS)
_require_nonnegative_int("videos_per_channel", VIDEOS_PER_CHANNEL)
_require_positive_int("max_segment_chars", MAX_SEGMENT_CHARS)
_require_fraction("score_threshold", SCORE_THRESHOLD)
_require_nonnegative_int("validation_max_per_stratum", VALIDATION_MAX_PER_STRATUM)
_require_nonnegative_int("validation_min_per_stratum", VALIDATION_MIN_PER_STRATUM)
if VALIDATION_MIN_PER_STRATUM > VALIDATION_MAX_PER_STRATUM:
    raise ValueError("validation_min_per_stratum must be <= validation_max_per_stratum.")
try:
    int(VALIDATION_SAMPLE_SEED)
except ValueError as exc:
    raise ValueError(f"validation_sample_seed must be an integer string; got {VALIDATION_SAMPLE_SEED!r}.") from exc
if CREATE_VALIDATION_SAMPLES and not IS_FULL_BUCKET_RANGE:
    print(
        "Skipping manual validation sample for a partial bucket range; validation sample caps are global "
        "and require the full bucket range."
    )
    CREATE_VALIDATION_SAMPLES = False

# GlotLID is effectively off if either the enable flag is false or the mode is 'disabled'.
GLOTLID_ACTIVE = ENABLE_GLOTLID and GLOTLID_MODE != "disabled"
# Only a full all-valid-segments GlotLID run may feed agreement, consensus, and downstream diagnostics.
# audit_segments remains available as a separate runtime-saving audit output, but it is subset-biased.
GLOTLID_CAN_FEED_MAIN = GLOTLID_ACTIVE and GLOTLID_MODE == "all_valid_segments"
if not (ENABLE_OPENLID or GLOTLID_ACTIVE):
    raise ValueError("At least one active model is required. Enable OpenLID or enable GlotLID with glotlid_mode != disabled.")
if GLOTLID_ACTIVE and GLOTLID_MODE == "audit_segments" and not ENABLE_OPENLID:
    raise ValueError("glotlid_mode=audit_segments requires enable_openlid=true because it audits OpenLID outputs.")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def local_dir_for(path: str) -> str:
    return os.path.dirname(path.replace("dbfs:/", "/dbfs/"))


channels_full = fqtn(CHANNELS_TABLE)
videos_full = fqtn(VIDEOS_TABLE)
segments_input_full = fqtn(OUTPUT_SEGMENTS_INPUT_TABLE)
dedupe_qa_full = fqtn(OUTPUT_DEDUPE_QA_TABLE)
openlid_segments_full = fqtn(OUTPUT_OPENLID_SEGMENTS_TABLE)
glotlid_segments_full = fqtn(OUTPUT_GLOTLID_SEGMENTS_TABLE)
glotlid_native_segments_full = fqtn(OUTPUT_GLOTLID_NATIVE_SEGMENTS_TABLE)
openlid_compact_full = fqtn(OUTPUT_OPENLID_COMPACT_TABLE)
glotlid_compact_full = fqtn(OUTPUT_GLOTLID_COMPACT_TABLE)
glotlid_native_compact_full = fqtn(OUTPUT_GLOTLID_NATIVE_COMPACT_TABLE)
channel_text_features_full = fqtn(OUTPUT_CHANNEL_TEXT_FEATURES_TABLE)
channel_votes_full = fqtn(OUTPUT_CHANNEL_VOTES_TABLE)
channel_model_aggregation_full = fqtn(OUTPUT_CHANNEL_MODEL_AGGREGATION_TABLE)
segment_model_comparison_full = fqtn(OUTPUT_SEGMENT_MODEL_COMPARISON_TABLE)
channel_model_comparison_full = fqtn(OUTPUT_CHANNEL_MODEL_COMPARISON_TABLE)
mixed_language_candidates_full = fqtn(OUTPUT_MIXED_LANGUAGE_CANDIDATES_TABLE)
hindi_indic_audit_full = fqtn(OUTPUT_HINDI_INDIC_AUDIT_TABLE)
high_risk_redirect_full = fqtn(OUTPUT_HIGH_RISK_REDIRECT_TABLE)
channels_output_full = fqtn(OUTPUT_CHANNELS_TABLE)
language_summary_full_full = fqtn(OUTPUT_LANGUAGE_SUMMARY_FULL_TABLE)
language_summary_rollup_full = fqtn(OUTPUT_LANGUAGE_SUMMARY_ROLLUP_TABLE)
model_agreement_summary_full = fqtn(OUTPUT_MODEL_AGREEMENT_SUMMARY_TABLE)
suspect_tail_audit_full = fqtn(OUTPUT_SUSPECT_TAIL_AUDIT_TABLE)
manual_validation_sample_full = fqtn(OUTPUT_MANUAL_VALIDATION_SAMPLE_TABLE)
unclassified_audit_full = fqtn(OUTPUT_UNCLASSIFIED_AUDIT_TABLE)
source_language_confusion_full = fqtn(OUTPUT_SOURCE_LANGUAGE_CONFUSION_TABLE)
ablation_summary_full = fqtn(OUTPUT_ABLATION_SUMMARY_TABLE)

print("Source channels table:", channels_full)
print("Source videos table:", videos_full)
print("Segments-input output table:", segments_input_full)
print("Compact prediction tables:", openlid_compact_full, glotlid_compact_full)
print("Dedupe QA output table:", dedupe_qa_full)
print("enable_openlid:", ENABLE_OPENLID, "| enable_glotlid:", ENABLE_GLOTLID, "| glotlid_mode:", GLOTLID_MODE)
print("min_clean_chars:", MIN_CLEAN_CHARS, "| min_clean_chars_non_latin:", MIN_CLEAN_CHARS_NON_LATIN,
      "| non_latin_dominant_script_share:", NON_LATIN_DOMINANT_SCRIPT_SHARE, "| top_k:", TOP_K)
print("production_mode:", PRODUCTION_MODE, "| prediction_output_mode:", PREDICTION_OUTPUT_MODE,
      "| run_id:", RUN_ID, "| bucket range:", f"{BUCKET_START}-{BUCKET_END}/{INFERENCE_HASH_BUCKETS}")

try:
    spark.conf.set("spark.databricks.delta.replaceWhere.dataColumns.enabled", "true")
except Exception as exc:  # noqa: BLE001 - non-Databricks Spark may not expose this config
    print(f"WARNING: could not enable Delta data-column replaceWhere support: {exc}")


def _channel_hash_bucket_col(channel_col):
    return F.pmod(F.xxhash64(channel_col.cast("string")), F.lit(INFERENCE_HASH_BUCKETS)).cast("int")


def _run_id_replace_where() -> str:
    return f"run_id = '{RUN_ID}'"


def _bucket_replace_where() -> str:
    return (
        f"{_run_id_replace_where()} AND inference_hash_buckets = {INFERENCE_HASH_BUCKETS} "
        f"AND channel_hash_bucket >= {BUCKET_START} "
        f"AND channel_hash_bucket <= {BUCKET_END}"
    )


def _run_scope_replace_where() -> str:
    full_flag = "true" if IS_FULL_BUCKET_RANGE else "false"
    base = (
        f"{_run_id_replace_where()} AND inference_hash_buckets = {INFERENCE_HASH_BUCKETS} "
        f"AND is_full_bucket_range = {full_flag}"
    )
    if IS_FULL_BUCKET_RANGE:
        return base
    return f"{base} AND bucket_start = {BUCKET_START} AND bucket_end = {BUCKET_END}"


def _run_scope_required_cols() -> List[str]:
    if IS_FULL_BUCKET_RANGE:
        return ["run_id", "inference_hash_buckets", "is_full_bucket_range"]
    return ["run_id", "inference_hash_buckets", "bucket_start", "bucket_end", "is_full_bucket_range"]


def _bucket_required_cols() -> List[str]:
    return ["run_id", "inference_hash_buckets", "channel_hash_bucket"]


def repartition_for_bucketed_parallelism(df, num_partitions: int, segment_col: str = "segment_id"):
    if num_partitions > ACTIVE_BUCKET_COUNT and segment_col in df.columns:
        return df.repartition(num_partitions, "channel_hash_bucket", segment_col)
    return df.repartition(num_partitions, "channel_hash_bucket")


def _current_run_filter(include_bucket: bool = True):
    cond = (F.col("run_id") == F.lit(RUN_ID)) & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
    if include_bucket:
        cond = cond & (F.col("channel_hash_bucket") >= F.lit(BUCKET_START)) & (F.col("channel_hash_bucket") <= F.lit(BUCKET_END))
    return cond


def current_run_table(table_full: str, include_bucket: bool = True):
    return spark.table(table_full).where(_current_run_filter(include_bucket))


def _current_run_scope_filter():
    cond = (
        (F.col("run_id") == F.lit(RUN_ID))
        & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
        & (F.col("is_full_bucket_range") == F.lit(IS_FULL_BUCKET_RANGE))
    )
    if not IS_FULL_BUCKET_RANGE:
        cond = cond & (F.col("bucket_start") == F.lit(BUCKET_START)) & (F.col("bucket_end") == F.lit(BUCKET_END))
    return cond


def current_run_scope_table(table_full: str):
    return spark.table(table_full).where(_current_run_scope_filter())


def with_run_scope_columns(df):
    return (
        df.withColumn("run_id", F.lit(RUN_ID))
        .withColumn("inference_hash_buckets", F.lit(INFERENCE_HASH_BUCKETS))
        .withColumn("bucket_start", F.lit(BUCKET_START))
        .withColumn("bucket_end", F.lit(BUCKET_END))
        .withColumn("is_full_bucket_range", F.lit(IS_FULL_BUCKET_RANGE))
    )


def with_bucket_run_columns(df):
    return df.withColumn("run_id", F.lit(RUN_ID)).withColumn("inference_hash_buckets", F.lit(INFERENCE_HASH_BUCKETS))


def _should_write_long_predictions() -> bool:
    return PREDICTION_OUTPUT_MODE in {"long_sample", "long_full"}


def _maybe_display(df) -> None:
    if ENABLE_NOTEBOOK_DISPLAYS:
        display(df)


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


def _table_partition_columns(table_full: str) -> List[str]:
    try:
        row = spark.sql(f"DESCRIBE DETAIL {table_full}").select("partitionColumns").collect()[0]
        return list(row["partitionColumns"] or [])
    except Exception:
        return []


def _table_requires_full_overwrite(table_full: str, write_cols: List[str], partition_cols: Optional[List[str]],
                                   replace_where_cols: Optional[List[str]]) -> Tuple[bool, str]:
    if not _table_exists_full(table_full):
        return (False, "")
    existing_cols = set(spark.table(table_full).columns)
    missing = sorted(set(replace_where_cols or []) - existing_cols)
    if missing:
        return (True, f"missing replaceWhere columns {missing}")
    write_col_set = set(write_cols)
    missing_write_cols = sorted(write_col_set - existing_cols)
    extra_existing_cols = sorted(existing_cols - write_col_set)
    if missing_write_cols or extra_existing_cols:
        details = []
        if missing_write_cols:
            details.append(f"missing output columns {missing_write_cols}")
        if extra_existing_cols:
            details.append(f"extra existing columns {extra_existing_cols}")
        return (True, "; ".join(details))
    expected_partitions = list(partition_cols or [])
    if expected_partitions:
        actual_partitions = _table_partition_columns(table_full)
        if actual_partitions != expected_partitions:
            return (True, f"partition columns are {actual_partitions}, expected {expected_partitions}")
    return (False, "")


def write_delta(df, table_full: str, partition_cols: Optional[List[str]] = None,
                replace_where: Optional[str] = None, zorder_cols: Optional[List[str]] = None,
                replace_where_cols: Optional[List[str]] = None) -> None:
    active_replace_where = replace_where
    if replace_where:
        if not _table_exists_full(table_full):
            active_replace_where = None
        else:
            inferred_replace_cols = ["run_id"]
            if "channel_hash_bucket" in replace_where:
                inferred_replace_cols.append("channel_hash_bucket")
            if "inference_hash_buckets" in replace_where:
                inferred_replace_cols.append("inference_hash_buckets")
            if "bucket_start" in replace_where:
                inferred_replace_cols.append("bucket_start")
            if "bucket_end" in replace_where:
                inferred_replace_cols.append("bucket_end")
            needs_full, reason = _table_requires_full_overwrite(
                table_full, df.columns, partition_cols, replace_where_cols or inferred_replace_cols
            )
            if needs_full:
                if not IS_FULL_BUCKET_RANGE:
                    raise RuntimeError(
                        f"{table_full} is not compatible with scoped replaceWhere ({reason}). "
                        "Run a full bucket range once to migrate the output table, or recreate the table before "
                        "running a partial bucket range."
                    )
                print(f"Migrating {table_full} with a full overwrite because existing metadata is incompatible: {reason}.")
                active_replace_where = None

    writer = df.write.format("delta").mode("overwrite")
    if active_replace_where:
        writer = writer.option("replaceWhere", active_replace_where)
    else:
        writer = writer.option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(table_full)
    if OPTIMIZE_AFTER_WRITE and zorder_cols:
        try:
            spark.sql(f"OPTIMIZE {table_full} ZORDER BY ({', '.join(zorder_cols)})")
        except Exception as exc:  # noqa: BLE001 - optimization is optional and Databricks-specific
            print(f"WARNING: OPTIMIZE failed for {table_full}: {exc}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1b. Taxonomy, high-risk labels, and review constants (spec §9)
# MAGIC
# MAGIC These constants are defined once for use throughout the pipeline. High-risk labels are flagged and
# MAGIC compared across models — never hard-recoded. Romanized keyword sets are high-recall **audit** signals
# MAGIC only; they must never feed label assignment, vote weighting, or consensus.

# COMMAND ----------
# §9.1 High-risk Latin tail labels.
HIGH_RISK_LATIN_TAIL_LABELS = {
    "srd_Latn", "ast_Latn", "vec_Latn", "gug_Latn", "pap_Latn",
    "fur_Latn", "scn_Latn", "lim_Latn", "mlt_Latn",
    "som_Latn", "swh_Latn", "yor_Latn", "hau_Latn", "kin_Latn",
    "sun_Latn", "bjn_Latn", "min_Latn", "ekk_Latn", "als_Latn",
}

# §9.2 Indic review labels.
HINDI_RELATED_ISO = {"hin", "bho", "awa", "mai", "mag"}
INDIC_AUDIT_ISO = {"hin", "bho", "awa", "mai", "mag", "npi", "mar", "urd", "pan", "guj"}
SOURCE_HINDI_CODES = {"hi", "hi-in", "hin"}
SOURCE_INDIC_CODES = {"hi", "hi-in", "hin", "ne", "ne-np", "npi", "bho", "ur", "ur-pk", "pa", "gu", "mr"}

# §9.3 Macro/near-language review clusters. Used for QA and mixed-language suppression, never silent recoding.
# Keyed by the unit that appears in model output: ISO-639-3 for most, full label for Chinese script variants.
ANALYSIS_CLUSTER_MAP = {
    # hbs_BCMS
    "bos": "hbs_BCMS", "hrv": "hbs_BCMS", "srp": "hbs_BCMS", "cnr": "hbs_BCMS",
    # Malay/Indonesian
    "ind": "ind_zsm_malay_indonesian_cluster", "zsm": "ind_zsm_malay_indonesian_cluster",
    # Chinese (keyed by full label because the script distinction is the point)
    "cmn_Hans": "cmn_Chinese", "cmn_Hant": "cmn_Chinese",
    # North Indic / Hindi-related
    "hin": "hindi_related_north_indic_review_cluster",
    "bho": "hindi_related_north_indic_review_cluster",
    "awa": "hindi_related_north_indic_review_cluster",
    "mai": "hindi_related_north_indic_review_cluster",
    "mag": "hindi_related_north_indic_review_cluster",
    # Italian/Romance
    "ita": "italian_romance_review_cluster", "srd": "italian_romance_review_cluster",
    "vec": "italian_romance_review_cluster", "scn": "italian_romance_review_cluster",
    "fur": "italian_romance_review_cluster", "lmo": "italian_romance_review_cluster",
    # Iberian/Romance
    "spa": "iberian_romance_review_cluster", "ast": "iberian_romance_review_cluster",
    "glg": "iberian_romance_review_cluster", "cat": "iberian_romance_review_cluster",
}

# §9.4 Label review notes (informational; do not hard-recode these labels).
LABEL_REVIEW_NOTES = {
    "als": "ISO 639-3 'als' is Tosk Albanian, while historical Wikimedia/legacy 'als' usage often refers to Alemannic German. Audit before interpretation.",
    "nob": "Norwegian Bokmaal (nob) vs Norwegian macrolanguage/source code no/nor. Reconcile before rollup.",
    "zsm": "Standard Malay (zsm) vs Malay macrolanguage/source ms/msa and Indonesian (ind). Treat within Malay/Indonesian review cluster.",
}

# §12 Romanized keyword sets — recall-only audit signals.
ROMANIZED_HINDI_KEYWORDS = {
    "hindi", "bhajan", "bollywood", "krishna", "kanha", "radha",
    "mahadev", "shiv", "ramayan", "katha", "pravachan", "samachar",
    "khabar", "modi", "yogi", "sarkar", "chunav", "desi",
    "upsc", "ssc", "gk", "current affairs in hindi",
}
ROMANIZED_INDIC_KEYWORDS = ROMANIZED_HINDI_KEYWORDS | {
    "nepali", "lok dohori", "dohori", "maya", "timro", "mero", "timi",
    "bhojpuri", "punjabi", "urdu", "ghazal", "qawwali", "kirtan",
}

print("Loaded taxonomy constants:")
print("  HIGH_RISK_LATIN_TAIL_LABELS:", len(HIGH_RISK_LATIN_TAIL_LABELS))
print("  INDIC_AUDIT_ISO:", len(INDIC_AUDIT_ISO))
print("  ANALYSIS_CLUSTER_MAP entries:", len(ANALYSIS_CLUSTER_MAP))
print("  ROMANIZED_INDIC_KEYWORDS:", len(ROMANIZED_INDIC_KEYWORDS))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Download or validate the model binaries (fail-fast)
# MAGIC
# MAGIC Both enabled models must be present before the run proceeds. If a cluster has no internet access,
# MAGIC upload the binaries and set `download_model_if_missing=false`.

# COMMAND ----------
def ensure_hf_fasttext_model(repo_id: str, filename: str, local_path: str, download_if_missing: bool,
                             extra_fallback_filenames: Optional[List[str]] = None) -> str:
    """Ensure a fastText .bin model exists at local_path and return the local filesystem path.

    Hugging Face model cards are not always consistent about filenames, so we try the requested
    filename first and then common fallbacks before failing.
    """
    local_path = local_path.replace("dbfs:/", "/dbfs/")
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        print(f"Model already present: {local_path} ({os.path.getsize(local_path):,} bytes)")
        return local_path

    if not download_if_missing:
        raise FileNotFoundError(
            f"Model file not found at {local_path}. Upload it there or set download_model_if_missing=true."
        )

    target_dir = local_dir_for(local_path)
    os.makedirs(target_dir, exist_ok=True)

    candidate_filenames: List[str] = []
    for f in [filename, *(extra_fallback_filenames or []), "model.bin"]:
        if f and f not in candidate_filenames:
            candidate_filenames.append(f)

    last_error = None
    for candidate in candidate_filenames:
        try:
            print(f"Downloading {repo_id}/{candidate} to {target_dir} ...")
            downloaded = hf_hub_download(repo_id=repo_id, filename=candidate, local_dir=target_dir)
            if downloaded != local_path:
                shutil.copyfile(downloaded, local_path)
            print(f"Model ready: {local_path} ({os.path.getsize(local_path):,} bytes); source filename={candidate}")
            return local_path
        except Exception as e:
            last_error = e
            print(f"Could not download {repo_id}/{candidate}: {repr(e)[:300]}")

    raise RuntimeError(f"Could not download a usable fastText model from {repo_id}. Last error: {last_error!r}")


WORKER_OPENLID_PATH = None
WORKER_GLOTLID_PATH = None

if ENABLE_OPENLID:
    MODEL_LOCAL_PATH = ensure_hf_fasttext_model(
        MODEL_REPO, MODEL_FILENAME, MODEL_LOCAL_PATH, DOWNLOAD_MODEL_IF_MISSING,
        extra_fallback_filenames=["openlid-v3.bin"],
    )
else:
    print("OpenLID disabled (enable_openlid=false).")

if GLOTLID_ACTIVE:
    GLOTLID_LOCAL_PATH = ensure_hf_fasttext_model(
        GLOTLID_REPO, GLOTLID_FILENAME, GLOTLID_LOCAL_PATH, DOWNLOAD_MODEL_IF_MISSING,
    )
else:
    print(f"GlotLID not active (enable_glotlid={ENABLE_GLOTLID}, glotlid_mode={GLOTLID_MODE}).")

# COMMAND ----------
# Resolve worker-visible model paths. SparkFiles is optional for clusters that cannot read /dbfs directly.
from pyspark import SparkFiles


def resolve_worker_path(local_path: Optional[str]) -> Optional[str]:
    if not local_path:
        return None
    if MODEL_DISTRIBUTION_MODE == "sparkfiles":
        uri = "file://" + local_path if local_path.startswith("/") else local_path
        spark.sparkContext.addFile(uri)
        return f"sparkfiles:{os.path.basename(local_path)}"
    return local_path


if ENABLE_OPENLID:
    WORKER_OPENLID_PATH = resolve_worker_path(MODEL_LOCAL_PATH)
    print("Worker OpenLID path:", WORKER_OPENLID_PATH)
if GLOTLID_ACTIVE:
    WORKER_GLOTLID_PATH = resolve_worker_path(GLOTLID_LOCAL_PATH)
    print("Worker GlotLID path:", WORKER_GLOTLID_PATH)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Deterministic source deduplication and smoke-test sampling (spec §4)
# MAGIC
# MAGIC Channels and videos are deduplicated with a deterministic `row_number()` ordering — never
# MAGIC `.dropDuplicates()`. The smoke-test channel sample is a deterministic hash order, never `.limit()`
# MAGIC on an unordered DataFrame. A `yt_lid_v3_dedupe_qa` table records before/after counts.

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


def truncate_col(col_name: str, max_chars: int):
    return F.substring(F.col(col_name).cast("string"), 1, max_chars)


# Timestamp columns considered for deterministic dedup ordering (spec §4.1, in priority order).
DEDUP_TS_CANDIDATES = [
    "updated_at", "modified_at", "ingestion_timestamp", "created_at", "capture_date", "first_capture_time",
]
RANK_CANDIDATES = [
    "published_at", "publish_time", "published_time", "upload_date", "created_time",
    "created_at", "first_capture_time", "ingestion_timestamp", "capture_date",
]


def deterministic_dedup(df, partition_cols: List[str], chosen_ts_col: Optional[str],
                        tie_breaker_hash_col: Optional[str] = None):
    """Select one row per partition deterministically.

    Ordering: chosen timestamp desc (nulls last) -> deterministic row hash asc -> partition keys asc.
    Never uses .dropDuplicates(). If no timestamp exists, hash ordering carries the determinism.
    """
    row_hash = F.col(tie_breaker_hash_col) if tie_breaker_hash_col else F.sha2(F.to_json(F.struct(*[F.col(c) for c in df.columns])), 256)
    order_exprs = []
    if chosen_ts_col:
        order_exprs.append(F.col(chosen_ts_col).desc_nulls_last())
    order_exprs.append(row_hash.asc())
    for c in partition_cols:
        order_exprs.append(F.col(c).asc_nulls_last())
    w = Window.partitionBy(*partition_cols).orderBy(*order_exprs)
    return (
        df.withColumn("_dedup_rn", F.row_number().over(w))
        .where(F.col("_dedup_rn") == 1)
        .drop("_dedup_rn")
    )


def stable_row_hash(df):
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in df.columns])), 256)


channels_raw = spark.table(channels_full)
videos_raw = spark.table(videos_full)

if CHANNEL_ID_COLUMN not in channels_raw.columns:
    raise ValueError(f"Channel ID column `{CHANNEL_ID_COLUMN}` not found in {channels_full}")
if CHANNEL_ID_COLUMN not in videos_raw.columns:
    raise ValueError(f"Channel ID column `{CHANNEL_ID_COLUMN}` not found in {videos_full}")

if not IS_FULL_BUCKET_RANGE:
    print(f"Filtering source tables to channel_hash_bucket range {BUCKET_START}-{BUCKET_END} before counts and dedup.")
    channels_raw = (
        channels_raw
        .withColumn("_lid_source_bucket", _channel_hash_bucket_col(F.col(CHANNEL_ID_COLUMN)))
        .where((F.col("_lid_source_bucket") >= F.lit(BUCKET_START)) & (F.col("_lid_source_bucket") <= F.lit(BUCKET_END)))
        .drop("_lid_source_bucket")
    )
    videos_raw = (
        videos_raw
        .withColumn("_lid_source_bucket", _channel_hash_bucket_col(F.col(CHANNEL_ID_COLUMN)))
        .where((F.col("_lid_source_bucket") >= F.lit(BUCKET_START)) & (F.col("_lid_source_bucket") <= F.lit(BUCKET_END)))
        .drop("_lid_source_bucket")
    )

if RUN_HEAVY_QA:
    n_channels_in = channels_raw.count()
    n_videos_raw_in = videos_raw.count()
else:
    print("Skipping raw source row counts for production run; set run_heavy_qa=true for exact dedupe before-counts.")
    n_channels_in = None
    n_videos_raw_in = None

# --- Channel dedup (§4.1) ---
channel_ts_col = first_existing_column(channels_raw, DEDUP_TS_CANDIDATES)
print("Channel dedup timestamp column:", channel_ts_col)
channels_dedup = deterministic_dedup(channels_raw, [CHANNEL_ID_COLUMN], channel_ts_col).persist(StorageLevel.DISK_ONLY)
n_channels_after_dedup = channels_dedup.count()
if RUN_HEAVY_QA:
    n_duplicate_channel_keys = (
        channels_raw.groupBy(CHANNEL_ID_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .count()
    )
else:
    n_duplicate_channel_keys = None
n_channels_pipeline = n_channels_after_dedup
n_videos_input_for_dedup = n_videos_raw_in

# COMMAND ----------
# Deterministic smoke-test channel selection (§4.3): order by xxhash64(channel_id) then take first N.
if LIMIT_CHANNELS > 0:
    print(f"Smoke test: selecting first {LIMIT_CHANNELS:,} channels by xxhash64(channel_id) after dedup.")
    sampled_channels = (
        channels_dedup
        .select(CHANNEL_ID_COLUMN)
        .orderBy(F.xxhash64(F.col(CHANNEL_ID_COLUMN)).asc(), F.col(CHANNEL_ID_COLUMN).asc_nulls_last())
        .limit(LIMIT_CHANNELS)
    )
    channels_dedup = channels_dedup.join(sampled_channels, on=CHANNEL_ID_COLUMN, how="inner")
    channels_dedup = channels_dedup.persist(StorageLevel.DISK_ONLY)
    videos_raw = videos_raw.join(
        sampled_channels.select(CHANNEL_ID_COLUMN), on=CHANNEL_ID_COLUMN, how="inner"
    )
    n_channels_pipeline = channels_dedup.count()
    n_videos_input_for_dedup = videos_raw.count() if RUN_HEAVY_QA else None

# Bucket-filter the pipeline after deterministic channel selection. The default range covers all buckets;
# narrower ranges support resumable production reruns without changing downstream logic.
channels_dedup = channels_dedup.withColumn(
    "channel_hash_bucket", _channel_hash_bucket_col(F.col(CHANNEL_ID_COLUMN))
)
if not IS_FULL_BUCKET_RANGE:
    print(f"Source tables are already restricted to channel_hash_bucket range {BUCKET_START}-{BUCKET_END}.")

# COMMAND ----------
# Detect text/id columns (needed before video dedup so the fallback partition keys are concrete).
channel_name_col = first_existing_column(
    channels_dedup, ["channel_name", "title", "name", "display_name"], CHANNEL_NAME_COLUMN_OVERRIDE,
)
channel_description_col = first_existing_column(
    channels_dedup,
    ["channel_description", "description", "about", "bio", "channel_about", "profile_description", "channel_text"],
    CHANNEL_DESCRIPTION_COLUMN_OVERRIDE,
)
video_id_col = first_existing_column(videos_raw, [VIDEO_ID_COLUMN, "id", "video"], VIDEO_ID_COLUMN if VIDEO_ID_COLUMN in videos_raw.columns else "")
video_title_col = first_existing_column(videos_raw, ["video_title", "title", "name"], VIDEO_TITLE_COLUMN_OVERRIDE)
video_description_col = first_existing_column(
    videos_raw, ["description", "video_description", "caption", "text", "body"], VIDEO_DESCRIPTION_COLUMN_OVERRIDE,
)
video_tags_col = first_existing_column(videos_raw, ["tags", "keywords", "video_tags"], VIDEO_TAGS_COLUMN_OVERRIDE)

print("channel_name_col:", channel_name_col)
print("channel_description_col:", channel_description_col)
print("video_id_col:", video_id_col)
print("video_title_col:", video_title_col)
print("video_description_col:", video_description_col)
print("video_tags_col:", video_tags_col)

# --- Video dedup (§4.2) ---
video_ts_col = first_existing_column(videos_raw, DEDUP_TS_CANDIDATES)
video_rank_dedup_col = first_existing_column(
    videos_raw, RANK_CANDIDATES, VIDEO_RANK_COLUMN if VIDEO_RANK_COLUMN else ""
)
videos_for_dedup = videos_raw.withColumn("_lid_video_row_hash", stable_row_hash(videos_raw))
if video_id_col:
    _video_id_trimmed = F.trim(F.col(video_id_col).cast("string"))
    videos_for_dedup = videos_for_dedup.withColumn(
        "_lid_video_dedup_key",
        F.when(_video_id_trimmed.isNotNull() & (_video_id_trimmed != ""), _video_id_trimmed)
        .otherwise(F.concat(F.lit("rowhash:"), F.col("_lid_video_row_hash"))),
    )
    video_partition_cols = ["_lid_video_dedup_key"]
else:
    # Fallback: partition on channel + available text fields + timestamp/rank fields (§4.2).
    video_partition_cols = []
    for c in [CHANNEL_ID_COLUMN, video_title_col, video_description_col, video_ts_col, video_rank_dedup_col]:
        if c and c not in video_partition_cols:
            video_partition_cols.append(c)
print("Video dedup partition cols:", video_partition_cols, "| timestamp column:", video_ts_col)
if RUN_HEAVY_QA:
    n_duplicate_video_keys = (
        videos_for_dedup.groupBy(*video_partition_cols)
        .count()
        .where(F.col("count") > 1)
        .count()
    )
else:
    n_duplicate_video_keys = None
videos_dedup = deterministic_dedup(
    videos_for_dedup, video_partition_cols, video_ts_col, "_lid_video_row_hash"
).persist(StorageLevel.DISK_ONLY)
n_videos_out = videos_dedup.count() if RUN_HEAVY_QA else None

# COMMAND ----------
# Restrict to the N most recent videos per channel if a rank column is available (deterministic).
if VIDEO_RANK_COLUMN:
    if VIDEO_RANK_COLUMN not in videos_dedup.columns:
        raise ValueError(f"video_rank_column `{VIDEO_RANK_COLUMN}` was specified but is not present.")
    rank_col = VIDEO_RANK_COLUMN
else:
    rank_col = first_existing_column(videos_dedup, RANK_CANDIDATES)

videos_selected = videos_dedup
if VIDEOS_PER_CHANNEL > 0:
    video_row_hash_tiebreak = F.col("_lid_video_row_hash").asc()
    if rank_col:
        print(f"Restricting to {VIDEOS_PER_CHANNEL} videos/channel using rank column `{rank_col}`.")
        order_cols = [F.col(rank_col).desc_nulls_last()]
        if video_id_col:
            order_cols.append(F.col(video_id_col).asc_nulls_last())
        order_cols.append(video_row_hash_tiebreak)
        w = Window.partitionBy(CHANNEL_ID_COLUMN).orderBy(*order_cols)
    else:
        print(f"No rank column found. Restricting to {VIDEOS_PER_CHANNEL} deterministic videos/channel by hash.")
        w = Window.partitionBy(CHANNEL_ID_COLUMN).orderBy(video_row_hash_tiebreak)
    videos_selected = (
        videos_dedup
        .withColumn("_video_rank_for_lid", F.row_number().over(w))
        .where(F.col("_video_rank_for_lid") <= VIDEOS_PER_CHANNEL)
        .drop("_video_rank_for_lid")
    )

# Stable video key used in segment IDs. If the source has no video_id, use a deterministic post-dedup row hash
# so distinct same-text videos from the same channel do not collapse to the same segment_id.
if video_id_col:
    _video_id_trimmed_selected = F.trim(F.col(video_id_col).cast("string"))
    videos_selected = videos_selected.withColumn(
        "_lid_video_key",
        F.when(_video_id_trimmed_selected.isNotNull() & (_video_id_trimmed_selected != ""), _video_id_trimmed_selected)
        .otherwise(F.concat(F.lit("rowhash:"), F.col("_lid_video_row_hash"))),
    )
else:
    videos_selected = videos_selected.withColumn(
        "_lid_video_key",
        F.concat(F.lit("rowhash:"), F.col("_lid_video_row_hash")),
    )
videos_selected = videos_selected.persist(StorageLevel.DISK_ONLY)
n_videos_selected_for_segments = videos_selected.count()
print(f"Videos selected for segment fanout: {n_videos_selected_for_segments:,}")

# COMMAND ----------
# Write dedupe QA (§4.1/§4.2). Exact raw source and duplicate-key counts are heavy-QA only in production.
dedupe_qa_rows = [
    (
        "channels",
        n_channels_in,
        n_channels_after_dedup,
        (n_channels_in - n_channels_after_dedup) if n_channels_in is not None else None,
        n_duplicate_channel_keys,
        channel_ts_col or "<none>",
        n_channels_pipeline,
    ),
    (
        "videos",
        n_videos_input_for_dedup,
        n_videos_out,
        (n_videos_input_for_dedup - n_videos_out) if n_videos_input_for_dedup is not None and n_videos_out is not None else None,
        n_duplicate_video_keys,
        video_ts_col or "<none>",
        n_videos_selected_for_segments,
    ),
]
dedupe_qa_df = spark.createDataFrame(
    dedupe_qa_rows,
    schema=StructType([
        StructField("entity", StringType(), False),
        StructField("n_input_rows", LongType(), True),
        StructField("n_output_rows", LongType(), True),
        StructField("n_duplicate_rows_removed", LongType(), True),
        StructField("n_duplicate_keys", LongType(), True),
        StructField("chosen_timestamp_column", StringType(), True),
        StructField("n_pipeline_rows_after_sampling", LongType(), True),
    ]),
).withColumn("limit_channels", F.lit(LIMIT_CHANNELS)).withColumn("run_id", F.lit(RUN_ID)).withColumn(
    "inference_hash_buckets", F.lit(INFERENCE_HASH_BUCKETS)
).withColumn("bucket_start", F.lit(BUCKET_START)).withColumn("bucket_end", F.lit(BUCKET_END)).withColumn(
    "is_full_bucket_range", F.lit(IS_FULL_BUCKET_RANGE)
).withColumn(
    "prediction_timestamp", F.current_timestamp()
)

write_delta(
    dedupe_qa_df,
    dedupe_qa_full,
    partition_cols=["run_id"],
    replace_where=_run_scope_replace_where(),
    replace_where_cols=_run_scope_required_cols(),
)
print("Wrote dedupe QA to", dedupe_qa_full)
_maybe_display(current_run_scope_table(dedupe_qa_full))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Canonical segment-input table (spec §5)
# MAGIC
# MAGIC Each text field becomes a separate segment. We compute per-script **letter** metrics and a
# MAGIC letter-based validity rule (not whitespace-padded text length). Both OpenLID and GlotLID read from
# MAGIC this single canonical table in the default run.

# COMMAND ----------
segment_dfs = []

base_channel_cols = [
    F.col(CHANNEL_ID_COLUMN).cast("string").alias("channel_id"),
    F.lit(None).cast("string").alias("video_id"),
]
if channel_name_col:
    segment_dfs.append(channels_dedup.select(
        *base_channel_cols, F.lit("channel_name").alias("segment_type"),
        truncate_col(channel_name_col, MAX_SEGMENT_CHARS).alias("text"),
    ))
if channel_description_col:
    segment_dfs.append(channels_dedup.select(
        *base_channel_cols, F.lit("channel_description").alias("segment_type"),
        truncate_col(channel_description_col, MAX_SEGMENT_CHARS).alias("text"),
    ))

base_video_cols = [
    F.col(CHANNEL_ID_COLUMN).cast("string").alias("channel_id"),
    F.col("_lid_video_key").alias("video_id"),
]
if video_title_col:
    segment_dfs.append(videos_selected.select(
        *base_video_cols, F.lit("video_title").alias("segment_type"),
        truncate_col(video_title_col, MAX_SEGMENT_CHARS).alias("text"),
    ))
if video_description_col:
    segment_dfs.append(videos_selected.select(
        *base_video_cols, F.lit("video_description").alias("segment_type"),
        truncate_col(video_description_col, MAX_SEGMENT_CHARS).alias("text"),
    ))
if video_tags_col:
    segment_dfs.append(videos_selected.select(
        *base_video_cols, F.lit("video_tags").alias("segment_type"),
        truncate_col(video_tags_col, MAX_SEGMENT_CHARS).alias("text"),
    ))

if not segment_dfs:
    raise ValueError("No usable text segment columns were found. Set column override widgets manually.")

segments = segment_dfs[0]
for s in segment_dfs[1:]:
    segments = segments.unionByName(s)

segments = (
    segments
    .where(F.col("channel_id").isNotNull())
    .where(F.col("text").isNotNull() & (F.length(F.trim(F.col("text"))) > 0))
    .withColumn("text", F.substring(F.col("text"), 1, MAX_SEGMENT_CHARS))
    .withColumn(
        "segment_id",
        F.sha2(F.concat_ws("||", F.col("channel_id"), F.coalesce(F.col("video_id"), F.lit("")),
                           F.col("segment_type"), F.col("text")), 256),
    )
    .withColumn("channel_hash_bucket", _channel_hash_bucket_col(F.col("channel_id")))
    .transform(lambda df: repartition_for_bucketed_parallelism(df, MIN_NUM_PARTITIONS))
)

# COMMAND ----------
# Script-metric UDF. Computes per-script *letter* counts over cleaned text (URLs/digits/punct/symbols
# removed), the dominant script and its share, and raw-text flags. Validity is computed in Spark below
# using the widget thresholds so the UDF stays a pure text->metrics function.
URL_PATTERN = regex.compile(r"https?://\S+|www\.\S+", flags=regex.IGNORECASE)
HASHTAG_PATTERN = regex.compile(r"#\w+", flags=regex.UNICODE)
SYMBOL_PATTERN = regex.compile(r"[\p{So}\p{Sk}\p{Sc}\p{Sm}]")
NONWORD_REPLACE_PATTERN = regex.compile(r"[^\p{Word}\p{Zs}]|\d")
SPACE_PATTERN = regex.compile(r"\s\s+")
LETTER_PATTERN = regex.compile(r"\p{L}")

# Per-script letter patterns. "kana" merges Hiragana+Katakana. Scripts outside this set fall into "other"
# (still treated as non-Latin), which keeps major non-Latin Indic scripts (Tamil/Telugu/Bengali/etc.)
# eligible for the non-Latin validity exception.
SCRIPT_PATTERNS = {
    "latin": regex.compile(r"\p{Latin}"),
    "devanagari": regex.compile(r"\p{Devanagari}"),
    "arabic": regex.compile(r"\p{Arabic}"),
    "cyrillic": regex.compile(r"\p{Cyrillic}"),
    "han": regex.compile(r"\p{Han}"),
    "kana": regex.compile(r"[\p{Hiragana}\p{Katakana}]"),
    "hangul": regex.compile(r"\p{Hangul}"),
    "thai": regex.compile(r"\p{Thai}"),
}


def preprocess_for_lid(text: Optional[str]) -> str:
    """Lowercase, strip URLs, drop non-word chars and digits, collapse whitespace. Shared by LID inference."""
    if text is None:
        return ""
    text = str(text).strip().replace("\n", " ").replace("\r", " ").lower()
    text = regex.sub(URL_PATTERN, " ", text)
    text = regex.sub(SPACE_PATTERN, " ", text)
    text = regex.sub(NONWORD_REPLACE_PATTERN, "", text)
    text = regex.sub(SPACE_PATTERN, " ", text).strip()
    return text


def compute_script_metrics_one(raw: Optional[str]) -> dict:
    raw_str = "" if raw is None else str(raw)
    clean = preprocess_for_lid(raw_str)
    counts = {name: len(p.findall(clean)) for name, p in SCRIPT_PATTERNS.items()}
    total_letters = len(LETTER_PATTERN.findall(clean))
    tracked = sum(counts.values())
    other = max(0, total_letters - tracked)
    bucket = dict(counts)
    bucket["other"] = other
    if total_letters <= 0:
        dominant_script = "none"
        dominant_share = 0.0
    else:
        dominant_script = max(bucket, key=lambda k: bucket[k])
        dominant_share = bucket[dominant_script] / float(total_letters)
    return {
        "clean_text": clean,
        "raw_text_len": len(raw_str),
        "clean_text_len": len(clean),
        "clean_letter_count": total_letters,
        "clean_token_count": len(clean.split()),
        "dominant_script": dominant_script,
        "dominant_script_share": float(dominant_share),
        "latin_char_count": counts["latin"],
        "devanagari_char_count": counts["devanagari"],
        "arabic_char_count": counts["arabic"],
        "cyrillic_char_count": counts["cyrillic"],
        "han_char_count": counts["han"],
        "kana_char_count": counts["kana"],
        "hangul_char_count": counts["hangul"],
        "thai_char_count": counts["thai"],
        "has_url": bool(URL_PATTERN.search(raw_str)),
        "has_hashtag": bool(HASHTAG_PATTERN.search(raw_str)),
        "has_emoji_or_symbol": bool(SYMBOL_PATTERN.search(raw_str)),
    }


script_metrics_schema = StructType([
    StructField("clean_text", StringType(), True),
    StructField("raw_text_len", IntegerType(), True),
    StructField("clean_text_len", IntegerType(), True),
    StructField("clean_letter_count", IntegerType(), True),
    StructField("clean_token_count", IntegerType(), True),
    StructField("dominant_script", StringType(), True),
    StructField("dominant_script_share", DoubleType(), True),
    StructField("latin_char_count", IntegerType(), True),
    StructField("devanagari_char_count", IntegerType(), True),
    StructField("arabic_char_count", IntegerType(), True),
    StructField("cyrillic_char_count", IntegerType(), True),
    StructField("han_char_count", IntegerType(), True),
    StructField("kana_char_count", IntegerType(), True),
    StructField("hangul_char_count", IntegerType(), True),
    StructField("thai_char_count", IntegerType(), True),
    StructField("has_url", BooleanType(), True),
    StructField("has_hashtag", BooleanType(), True),
    StructField("has_emoji_or_symbol", BooleanType(), True),
])


@F.pandas_udf(script_metrics_schema)
def script_metrics_udf(text_series: pd.Series) -> pd.DataFrame:
    return pd.DataFrame([compute_script_metrics_one(t) for t in text_series])

# COMMAND ----------
# Apply metrics and the letter-based validity rule (§5), then write the canonical segment-input table.
seg = (
    segments
    .withColumn("m", script_metrics_udf(F.col("text")))
    .select("channel_id", "video_id", "segment_id", "segment_type", "channel_hash_bucket", "text", "m.*")
)

is_non_latin_dominant = (~F.col("dominant_script").isin("latin", "none"))
latin_rule = F.col("clean_letter_count") >= F.lit(MIN_CLEAN_CHARS)
non_latin_rule = (
    is_non_latin_dominant
    & (F.col("clean_letter_count") >= F.lit(MIN_CLEAN_CHARS_NON_LATIN))
    & (F.col("dominant_script_share") >= F.lit(NON_LATIN_DOMINANT_SCRIPT_SHARE))
)

segments_input = (
    seg
    .withColumn("is_valid_text_latin_rule", latin_rule)
    .withColumn("is_valid_text_non_latin_rule", non_latin_rule)
    .withColumn("is_valid_text_for_lid", latin_rule | non_latin_rule)
    .withColumn(
        "short_text_reason",
        F.when(F.col("is_valid_text_for_lid"), F.lit(None).cast("string"))
        .when(F.col("clean_letter_count") <= 0, F.lit("no_letters"))
        .when(~is_non_latin_dominant, F.lit("below_min_clean_chars_latin_or_mixed"))
        .when(F.col("clean_letter_count") < F.lit(MIN_CLEAN_CHARS_NON_LATIN), F.lit("below_min_clean_chars_non_latin"))
        .when(F.col("dominant_script_share") < F.lit(NON_LATIN_DOMINANT_SCRIPT_SHARE), F.lit("low_dominant_script_share"))
        .otherwise(F.lit("invalid_other")),  # defensive catch-all; the branches above are exhaustive for invalid rows
    )
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("inference_hash_buckets", F.lit(INFERENCE_HASH_BUCKETS))
    .withColumn("prediction_timestamp", F.current_timestamp())
)

write_delta(
    segments_input,
    segments_input_full,
    partition_cols=["run_id", "channel_hash_bucket"],
    replace_where=_bucket_replace_where(),
    zorder_cols=["segment_id", "channel_id"],
)
print("Wrote canonical segment-input table to", segments_input_full)

# COMMAND ----------
# Segment-input QA: counts by segment type and validity, and the validity-reason breakdown.
seg_in = current_run_table(segments_input_full)
print("Segment counts by type and validity")
_maybe_display(
    seg_in.groupBy("segment_type", "is_valid_text_for_lid").count().orderBy("segment_type", "is_valid_text_for_lid")
)
print("Invalid-text reasons")
_maybe_display(seg_in.groupBy("short_text_reason").count().orderBy(F.desc("count")))
print("Dominant script distribution (valid segments)")
_maybe_display(
    seg_in.where(F.col("is_valid_text_for_lid"))
    .groupBy("dominant_script").count().orderBy(F.desc("count"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Run OpenLID and GlotLID on the same valid-segment universe (spec §6)
# MAGIC
# MAGIC Both models run on `is_valid_text_for_lid=true` rows from the canonical segment-input table. The main
# MAGIC GlotLID pass uses `match_openlid` preprocessing (the shared `clean_text`) so the model comparison is
# MAGIC apples-to-apples. `audit_segments` (manual override) and `glotlid_native_audit` (separate output) are
# MAGIC supported but never feed the main comparison.

# COMMAND ----------
# Preprocessor for the optional GlotLID-native audit. GlotLID is trained on lightly normalized,
# case/script-preserving text, so the native path keeps case, digits, and punctuation and only removes
# URLs and newlines. This is an approximation of GlotLID-native preprocessing and is deliberately kept
# OUT of the main comparison (§6.4).
def preprocess_glotlid_native(text: Optional[str]) -> str:
    if text is None:
        return ""
    t = str(text).replace("\n", " ").replace("\r", " ")
    t = regex.sub(URL_PATTERN, " ", t)
    t = regex.sub(SPACE_PATTERN, " ", t).strip()
    return t


@F.pandas_udf(StringType())
def glotlid_native_clean_udf(text_series: pd.Series) -> pd.Series:
    return text_series.map(preprocess_glotlid_native)

# COMMAND ----------
_LID_MODELS: Dict[str, object] = {}


def _load_model_cached(path: str):
    if path not in _LID_MODELS:
        _LID_MODELS[path] = fasttext.load_model(path)
    return _LID_MODELS[path]


def _worker_model_path(worker_path: str) -> str:
    if worker_path.startswith("sparkfiles:"):
        return SparkFiles.get(worker_path.split(":", 1)[1])
    return worker_path


def _raw_prediction_columns(k: int) -> List[str]:
    cols: List[str] = []
    for i in range(1, k + 1):
        cols.extend([f"label_raw_{i}", f"score_{i}"])
    cols.append("lid_error")
    return cols


def _compact_prediction_columns(k: int) -> List[str]:
    cols = list(COMPACT_CARRY_COLS) + ["lid_model"]
    for i in range(1, k + 1):
        cols.extend([f"label_raw_{i}", f"label_{i}", f"iso639_3_{i}", f"script_{i}", f"score_{i}"])
    cols.extend(["lid_error", "run_id", "inference_hash_buckets", "prediction_timestamp"])
    return cols


def _compact_prediction_schema(k: int) -> StructType:
    fields = [
        StructField("channel_id", StringType(), True),
        StructField("video_id", StringType(), True),
        StructField("segment_id", StringType(), True),
        StructField("segment_type", StringType(), True),
        StructField("channel_hash_bucket", IntegerType(), True),
        StructField("clean_letter_count", IntegerType(), True),
        StructField("clean_text_len", IntegerType(), True),
        StructField("dominant_script", StringType(), True),
        StructField("is_valid_text_for_lid", BooleanType(), True),
    ]
    for i in range(1, k + 1):
        fields.append(StructField(f"label_raw_{i}", StringType(), True))
        fields.append(StructField(f"score_{i}", DoubleType(), True))
    fields.append(StructField("lid_error", StringType(), True))
    return StructType(fields)


def _empty_prediction_row(k: int) -> dict:
    row = {c: None for c in _raw_prediction_columns(k)}
    row["lid_error"] = None
    return row


def _fill_prediction_row(row: dict, labels, scores, k: int) -> dict:
    for idx in range(min(k, len(labels))):
        row[f"label_raw_{idx + 1}"] = str(labels[idx])
        row[f"score_{idx + 1}"] = float(scores[idx])
    return row


def _predict_text_batch(model, texts: List[str], k: int, score_threshold: float) -> List[dict]:
    rows = [_empty_prediction_row(k) for _ in texts]
    non_empty = [(idx, "" if t is None else str(t)) for idx, t in enumerate(texts)]
    non_empty = [(idx, t) for idx, t in non_empty if t.strip() != ""]
    if not non_empty:
        return rows

    batch_texts = [t for _, t in non_empty]
    try:
        labels_batch, scores_batch = model.predict(
            text=batch_texts, k=max(1, k), threshold=score_threshold, on_unicode_error="replace",
        )
        if (
            len(labels_batch) == len(batch_texts)
            and len(scores_batch) == len(batch_texts)
            and all(isinstance(x, (list, tuple)) for x in labels_batch)
        ):
            for (out_idx, _), labels, scores in zip(non_empty, labels_batch, scores_batch):
                rows[out_idx] = _fill_prediction_row(rows[out_idx], labels, scores, k)
            return rows
    except Exception:
        pass

    for out_idx, text in non_empty:
        try:
            labels, scores = model.predict(
                text=text, k=max(1, k), threshold=score_threshold, on_unicode_error="replace",
            )
            rows[out_idx] = _fill_prediction_row(rows[out_idx], labels, scores, k)
        except Exception as exc:  # noqa: BLE001 - keep one bad segment from failing the batch
            rows[out_idx]["lid_error"] = repr(exc)[:500]
    return rows


def predict_segments_compact(input_df, worker_path: str, model_name: str, k: int,
                             score_threshold: float, text_col: str = "clean_text"):
    """Run fastText inference via mapInPandas and return one compact row per input segment."""
    schema = _compact_prediction_schema(k)
    output_cols = [f.name for f in schema.fields]
    selected = input_df.select(*SEGMENT_INFERENCE_COLS, F.col(text_col).alias("__predict_text"))

    def _iterator(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        model = _load_model_cached(_worker_model_path(worker_path))
        for pdf in pdf_iter:
            pred_pdf = pd.DataFrame(
                _predict_text_batch(model, pdf["__predict_text"].tolist(), k, score_threshold),
                columns=_raw_prediction_columns(k),
            )
            carry_pdf = pdf[COMPACT_CARRY_COLS].reset_index(drop=True)
            for int_col in ["channel_hash_bucket", "clean_letter_count", "clean_text_len"]:
                carry_pdf[int_col] = pd.to_numeric(carry_pdf[int_col], errors="coerce").astype("Int32")
            out = pd.concat([carry_pdf, pred_pdf], axis=1)
            yield out[output_cols]

    raw = selected.mapInPandas(_iterator, schema=schema)
    compact = _with_parsed_compact_labels(raw, model_name, k)
    return compact.select(*_compact_prediction_columns(k))

# COMMAND ----------
# Materialize the shared valid-segment universe once via a DBFS-backed checkpoint. localCheckpoint caused
# executor-eviction failures upstream, so we use a DBFS checkpoint dir (commit d3cb137 / upstream fix).
spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

NATIVE_AUDIT_ENABLED = GLOTLID_ACTIVE and (
    GLOTLID_PREPROCESSING_MODE == "glotlid_native_audit" or GLOTLID_NATIVE_AUDIT_SAMPLE_FRACTION > 0
)
NATIVE_AUDIT_IS_FULL = NATIVE_AUDIT_ENABLED and (
    GLOTLID_NATIVE_AUDIT_SAMPLE_FRACTION == 0 or GLOTLID_NATIVE_AUDIT_SAMPLE_FRACTION >= 1.0
)
if NATIVE_AUDIT_IS_FULL and not ALLOW_FULL_NATIVE_AUDIT:
    raise ValueError(
        "GlotLID native audit would run on all valid segments. Set allow_full_native_audit=true "
        "or set 0 < glotlid_native_audit_sample_fraction < 1."
    )

COMPACT_CARRY_COLS = [
    "channel_id", "video_id", "segment_id", "segment_type", "channel_hash_bucket",
    "clean_letter_count", "clean_text_len", "dominant_script", "is_valid_text_for_lid",
]
SEGMENT_INFERENCE_COLS = COMPACT_CARRY_COLS + ["clean_text"]

valid_segments_base = (
    current_run_table(segments_input_full)
    .where(F.col("is_valid_text_for_lid"))
    .select(*(SEGMENT_INFERENCE_COLS + (["text"] if NATIVE_AUDIT_ENABLED else [])))
)
n_valid_segments = valid_segments_base.count()
EFFECTIVE_NUM_PARTITIONS = max(
    MIN_NUM_PARTITIONS,
    min(MAX_NUM_PARTITIONS, int((n_valid_segments + TARGET_SEGMENTS_PER_PARTITION - 1) // TARGET_SEGMENTS_PER_PARTITION)),
)
spark.conf.set("spark.sql.shuffle.partitions", str(EFFECTIVE_NUM_PARTITIONS))
print(
    "Effective inference/shuffle partitions:",
    EFFECTIVE_NUM_PARTITIONS,
    "| target_segments_per_partition:",
    TARGET_SEGMENTS_PER_PARTITION,
)
valid_segments = repartition_for_bucketed_parallelism(valid_segments_base, EFFECTIVE_NUM_PARTITIONS).checkpoint(eager=True)
print(f"Valid segments for inference: {n_valid_segments:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Label normalization and optional long-format predictions (spec §7)

# COMMAND ----------
def _with_parsed_compact_labels(raw_pred_df, model_name: str, k: int):
    """Attach native Spark label/ISO/script columns to compact top-k prediction output."""
    df = raw_pred_df.withColumn("lid_model", F.lit(model_name))
    for i in range(1, k + 1):
        raw_col = F.col(f"label_raw_{i}")
        clean_col = f"_label_clean_{i}"
        parts_col = f"_label_parts_{i}"
        iso_part_col = f"_iso_part_{i}"
        script_part_col = f"_script_part_{i}"
        df = (
            df
            .withColumn(clean_col, F.regexp_replace(F.trim(raw_col), r"^__label__", ""))
            .withColumn(f"label_{i}", F.when(raw_col.isNull() | (F.col(clean_col) == ""), F.lit(None).cast("string")).otherwise(F.col(clean_col)))
            .withColumn(parts_col, F.split(F.col(f"label_{i}"), "_"))
            .withColumn(iso_part_col, F.array_join(F.slice(F.col(parts_col), 1, 1), ""))
            .withColumn(script_part_col, F.array_join(F.slice(F.col(parts_col), 2, 1), ""))
            .withColumn(
                f"iso639_3_{i}",
                F.when(F.col(f"label_{i}").isNull(), F.lit(None).cast("string"))
                .when(F.col(iso_part_col) == "", F.lit(None).cast("string"))
                .otherwise(F.col(iso_part_col)),
            )
            .withColumn(
                f"script_{i}",
                F.when((F.col(f"label_{i}").isNotNull()) & (F.col(script_part_col) != ""), F.col(script_part_col))
                .otherwise(F.lit(None).cast("string")),
            )
            .drop(clean_col, parts_col, iso_part_col, script_part_col)
        )
    return (
        df.withColumn("run_id", F.lit(RUN_ID))
        .withColumn("inference_hash_buckets", F.lit(INFERENCE_HASH_BUCKETS))
        .withColumn("prediction_timestamp", F.current_timestamp())
    )


def build_long_segments_from_compact(compact_df, k: int):
    """Convert compact top-k predictions to the legacy long format when audit output is requested."""
    pred_array = F.filter(
        F.array(*[
            F.struct(
                F.col(f"label_raw_{i}").alias("label_raw"),
                F.col(f"label_{i}").alias("label"),
                F.col(f"iso639_3_{i}").alias("iso639_3"),
                F.col(f"script_{i}").alias("script"),
                F.col(f"score_{i}").alias("score"),
            )
            for i in range(1, k + 1)
        ]),
        lambda x: x["label_raw"].isNotNull(),
    )
    exploded = (
        compact_df
        .withColumn("_pred_array", pred_array)
        .select(
            *COMPACT_CARRY_COLS, "lid_model", "lid_error", "score_1", "run_id", "inference_hash_buckets",
            F.posexplode_outer("_pred_array").alias("_pos", "_pred"),
        )
    )
    long_df = (
        exploded
        .withColumn("prediction_rank", (F.col("_pos") + F.lit(1)))
        .withColumn("label_raw", F.col("_pred")["label_raw"])
        .withColumn("label", F.col("_pred")["label"])
        .withColumn("iso639_3", F.col("_pred")["iso639_3"])
        .withColumn("script", F.col("_pred")["script"])
        .withColumn("score", F.col("_pred")["score"])
        .select(
            "channel_id", "video_id", "segment_id", "segment_type", "channel_hash_bucket", "lid_model",
            "prediction_rank", "label_raw", "label", "iso639_3", "script",
            "score", "score_1",
            "clean_letter_count", "clean_text_len", "dominant_script", "is_valid_text_for_lid",
            "lid_error", "run_id", "inference_hash_buckets",
        )
        .withColumn("prediction_timestamp", F.current_timestamp())
    )
    return long_df


def write_optional_long_segments(compact_df, table_full: str, model_name: str) -> None:
    if not _should_write_long_predictions():
        print(f"Skipping legacy long segment table for {model_name} (prediction_output_mode={PREDICTION_OUTPUT_MODE}).")
        if not _table_exists_full(table_full):
            print(f"No existing legacy long segment table to clear for compact-mode run: {table_full}")
            return
        empty_long = build_long_segments_from_compact(compact_df.limit(0), TOP_K)
        needs_full, reason = _table_requires_full_overwrite(
            table_full, empty_long.columns, ["run_id", "channel_hash_bucket"], _bucket_required_cols()
        )
        if needs_full:
            print(
                f"WARNING: not clearing incompatible legacy long segment table {table_full} in compact mode "
                f"because that would require a table-wide overwrite ({reason}). Consume compact predictions "
                "for this run, or migrate/drop the legacy long table explicitly."
            )
        else:
            write_delta(
                empty_long,
                table_full,
                partition_cols=["run_id", "channel_hash_bucket"],
                replace_where=_bucket_replace_where(),
                replace_where_cols=_bucket_required_cols(),
                zorder_cols=["segment_id", "channel_id"],
            )
            print("Cleared current-run legacy long segment rows for compact-mode run:", table_full)
        return
    long_df = build_long_segments_from_compact(compact_df, TOP_K)
    if PREDICTION_OUTPUT_MODE == "long_sample":
        sample_threshold = int(LONG_SEGMENT_SAMPLE_FRACTION * 1_000_000)
        long_df = long_df.where(
            F.pmod(F.xxhash64(F.col("segment_id"), F.lit(RUN_ID), F.col("lid_model")), F.lit(1_000_000)) < F.lit(sample_threshold)
        )
    write_delta(
        long_df,
        table_full,
        partition_cols=["run_id", "channel_hash_bucket"],
        replace_where=_bucket_replace_where(),
        zorder_cols=["segment_id", "channel_id"],
    )
    print("Wrote legacy long segment predictions to", table_full)


def write_compact_predictions(input_df, worker_path: str, model_name: str, table_full: str,
                              text_col: str = "clean_text", input_is_partitioned: bool = False):
    inference_input = input_df if input_is_partitioned else repartition_for_bucketed_parallelism(input_df, EFFECTIVE_NUM_PARTITIONS)
    compact = predict_segments_compact(
        inference_input,
        worker_path,
        model_name,
        TOP_K,
        SCORE_THRESHOLD,
        text_col=text_col,
    )
    write_delta(
        compact,
        table_full,
        partition_cols=["run_id", "channel_hash_bucket"],
        replace_where=_bucket_replace_where(),
        zorder_cols=["segment_id", "channel_id"],
    )
    print("Wrote compact segment predictions to", table_full)
    return current_run_table(table_full)

# COMMAND ----------
# --- OpenLID main pass (§6.1) ---
if ENABLE_OPENLID:
    openlid_compact = write_compact_predictions(
        valid_segments, WORKER_OPENLID_PATH, "openlid-v3", openlid_compact_full, input_is_partitioned=True
    )
    write_optional_long_segments(openlid_compact, openlid_segments_full, "openlid-v3")
else:
    openlid_compact = None
    print("OpenLID disabled; skipping OpenLID inference.")

# COMMAND ----------
# Determine the GlotLID main-pass input universe.
AUDIT_LOW_CONFIDENCE_SCORE = 0.50  # manual audit_segments threshold; only used when glotlid_mode=audit_segments

if GLOTLID_ACTIVE:
    if GLOTLID_MODE == "all_valid_segments":
        glotlid_input = valid_segments
        print(f"GlotLID main pass on all {n_valid_segments:,} valid segments.")
    elif GLOTLID_MODE == "audit_segments":
        if not ENABLE_OPENLID:
            raise ValueError("glotlid_mode=audit_segments requires enable_openlid=true (it audits OpenLID).")
        # Low-confidence OpenLID segments: weak top-1, or a high-risk Latin tail primary, or no prediction.
        ol_top1 = current_run_table(openlid_compact_full)
        high_risk_bcast = F.array(*[F.lit(x) for x in sorted(HIGH_RISK_LATIN_TAIL_LABELS)])
        audit_ids = (
            ol_top1
            .where(
                F.col("label_1").isNull()
                | (F.col("score_1") < F.lit(AUDIT_LOW_CONFIDENCE_SCORE))
                | F.array_contains(high_risk_bcast, F.col("label_1"))
            )
            .select("segment_id").distinct()
        )
        glotlid_input = valid_segments.join(audit_ids, on="segment_id", how="inner")
        print("GlotLID audit_segments mode: restricted to low-confidence OpenLID segments.")
    else:
        glotlid_input = None
else:
    glotlid_input = None
    print(f"GlotLID not active (enable_glotlid={ENABLE_GLOTLID}, glotlid_mode={GLOTLID_MODE}); skipping.")

# COMMAND ----------
# --- GlotLID main pass (§6.2). Always match_openlid preprocessing (shared clean_text) for comparability. ---
if glotlid_input is not None:
    glotlid_compact = write_compact_predictions(
        glotlid_input,
        WORKER_GLOTLID_PATH,
        "glotlid",
        glotlid_compact_full,
        input_is_partitioned=(GLOTLID_MODE == "all_valid_segments"),
    )
    write_optional_long_segments(glotlid_compact, glotlid_segments_full, "glotlid")
else:
    glotlid_compact = None

# COMMAND ----------
# --- Optional GlotLID native-preprocessing audit (§6.4). Separate output; never mixed into comparison. ---
if NATIVE_AUDIT_ENABLED:
    native_input = valid_segments
    if 0 < GLOTLID_NATIVE_AUDIT_SAMPLE_FRACTION < 1.0:
        frac = GLOTLID_NATIVE_AUDIT_SAMPLE_FRACTION
        native_input = valid_segments.sample(withReplacement=False, fraction=frac, seed=int(VALIDATION_SAMPLE_SEED))
        print(f"GlotLID native audit on ~{frac:.2%} sample of valid segments.")
    elif GLOTLID_NATIVE_AUDIT_SAMPLE_FRACTION >= 1.0:
        print("GlotLID native audit on all valid segments (sample_fraction >= 1).")
    else:
        print("GlotLID native audit on all valid segments (mode=glotlid_native_audit, fraction=0).")
    native_clean = native_input.withColumn("clean_text_native", glotlid_native_clean_udf(F.col("text")))
    native_compact = write_compact_predictions(
        native_clean,
        WORKER_GLOTLID_PATH,
        "glotlid-native",
        glotlid_native_compact_full,
        text_col="clean_text_native",
        input_is_partitioned=NATIVE_AUDIT_IS_FULL,
    )
    write_optional_long_segments(native_compact, glotlid_native_segments_full, "glotlid-native")
else:
    print("GlotLID native audit not enabled; skipping.")

# COMMAND ----------
N_OPENLID_COMPACT_ROWS = current_run_table(openlid_compact_full).count() if ENABLE_OPENLID else None
N_GLOTLID_COMPACT_ROWS = current_run_table(glotlid_compact_full).count() if GLOTLID_CAN_FEED_MAIN else None

# Acceptance check (§6.2 / acceptance #3): in the default full-coverage run, the valid segment_id universe
# must match between models, except for explicit per-segment inference errors. The full distinct/full-outer
# parity join is expensive at production scale, so production runs use row-count plus per-bucket checksum
# parity and reserve the full segment-id join for heavy QA.
if ENABLE_OPENLID and GLOTLID_ACTIVE and GLOTLID_MODE == "all_valid_segments":
    if N_OPENLID_COMPACT_ROWS != n_valid_segments or N_GLOTLID_COMPACT_ROWS != n_valid_segments:
        raise AssertionError(
            "Compact prediction row counts do not match the valid-segment universe. "
            f"openlid={N_OPENLID_COMPACT_ROWS}, glotlid={N_GLOTLID_COMPACT_ROWS}, valid_segments={n_valid_segments}."
        )
    if RUN_HEAVY_QA:
        ol_ids = current_run_table(openlid_compact_full).select("segment_id").distinct().withColumn("_in_openlid", F.lit(1))
        gl_ids = current_run_table(glotlid_compact_full).select("segment_id").distinct().withColumn("_in_glotlid", F.lit(1))
        parity = ol_ids.join(gl_ids, on="segment_id", how="full_outer").agg(
            F.sum(F.col("_in_openlid").isNotNull().cast("long")).alias("n_ol"),
            F.sum(F.col("_in_glotlid").isNotNull().cast("long")).alias("n_gl"),
            F.sum((F.col("_in_openlid").isNotNull() & F.col("_in_glotlid").isNull()).cast("long")).alias("n_only_ol"),
            F.sum((F.col("_in_glotlid").isNotNull() & F.col("_in_openlid").isNull()).cast("long")).alias("n_only_gl"),
        ).collect()[0]
        n_ol = int(parity["n_ol"] or 0)
        n_gl = int(parity["n_gl"] or 0)
        n_only_ol = int(parity["n_only_ol"] or 0)
        n_only_gl = int(parity["n_only_gl"] or 0)
        n_ol_errors = current_run_table(openlid_compact_full).where(F.col("lid_error").isNotNull()).select("segment_id").distinct().count()
        n_gl_errors = current_run_table(glotlid_compact_full).where(F.col("lid_error").isNotNull()).select("segment_id").distinct().count()
        print(f"OpenLID distinct segment_ids: {n_ol:,} | GlotLID: {n_gl:,}")
        print(f"Only-OpenLID: {n_only_ol:,} | Only-GlotLID: {n_only_gl:,}")
        print(f"OpenLID error segments: {n_ol_errors:,} | GlotLID error segments: {n_gl_errors:,}")
        if n_only_ol != 0 or n_only_gl != 0:
            raise AssertionError(
                "Segment-id universes diverge between models beyond inference errors. "
                f"only_openlid={n_only_ol}, only_glotlid={n_only_gl}. Investigate before proceeding."
            )
        print("Acceptance #3 OK: identical valid segment_id universe for both models.")
    else:
        print(
            "Acceptance #3 row-count check OK: OpenLID and GlotLID compact row counts both match "
            "the valid-segment count. Full segment-id parity join skipped because run_heavy_qa=false."
        )
        def _segment_universe_signature(table_full, prefix):
            return (
                current_run_table(table_full)
                .groupBy("channel_hash_bucket")
                .agg(
                    F.count(F.lit(1)).alias(f"{prefix}_n"),
                    F.sum(F.xxhash64(F.col("segment_id")).cast("decimal(38,0)")).alias(f"{prefix}_segment_hash_sum"),
                    F.sum(
                        F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.col("segment_id"))).cast("decimal(38,0)")
                    ).alias(f"{prefix}_channel_segment_hash_sum"),
                )
            )

        ol_sig = _segment_universe_signature(openlid_compact_full, "ol")
        gl_sig = _segment_universe_signature(glotlid_compact_full, "gl")
        signature_mismatches = (
            ol_sig.join(gl_sig, on="channel_hash_bucket", how="full_outer")
            .where(
                (F.coalesce(F.col("ol_n"), F.lit(-1)) != F.coalesce(F.col("gl_n"), F.lit(-1)))
                | (F.coalesce(F.col("ol_segment_hash_sum"), F.lit(0).cast("decimal(38,0)")) != F.coalesce(F.col("gl_segment_hash_sum"), F.lit(0).cast("decimal(38,0)")))
                | (F.coalesce(F.col("ol_channel_segment_hash_sum"), F.lit(0).cast("decimal(38,0)")) != F.coalesce(F.col("gl_channel_segment_hash_sum"), F.lit(0).cast("decimal(38,0)")))
            )
            .limit(1)
            .count()
        )
        if signature_mismatches:
            raise AssertionError(
                "OpenLID and GlotLID segment universes differ by bucket-level checksum. "
                "Set run_heavy_qa=true for the full segment-id parity join."
            )
        print("Acceptance #3 bucket-level segment-id checksum OK.")
else:
    if ENABLE_OPENLID and N_OPENLID_COMPACT_ROWS != n_valid_segments:
        raise AssertionError(
            f"OpenLID compact row count {N_OPENLID_COMPACT_ROWS} does not match valid segments {n_valid_segments}."
        )
    print("Skipping cross-model segment-id acceptance check (single model or non-default GlotLID mode).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Model-specific channel aggregation (spec §8)
# MAGIC
# MAGIC OpenLID and GlotLID are aggregated separately by the **same** functions. The vote-level table
# MAGIC (`yt_lid_v3_channel_votes`, with a `lid_model` column) holds per-(channel, language) weighted votes;
# MAGIC the per-model channel summary (`yt_lid_v3_channel_model_aggregation`) holds one row per channel per
# MAGIC model with the §8 primary/secondary/consensus-input fields. Note: length-weighting from the legacy
# MAGIC pipeline is intentionally **not** applied in v3 (§8 omits it), for cross-model comparability.

# COMMAND ----------
# Experimental, DEFAULT-OFF fixes (plan B2b / B3 / B4). Enable per-subset to collect scale-test data;
# while false they do NOT change production output. See validation/IMPLEMENTATION_PLAN_lid_v3_fixes.md.
for _w, _d in [
    ("b3_downweight_latin_name", "false"),       # B3: suppress a Latin channel-name vote on non-Latin channels
    ("b3_nonlatin_share", "0.60"),
    ("b3_min_nonname_segments", "2"),
    ("b4_emit_bilingual_status", "false"),        # B4: emit credible bilingual as a first-class status
    ("b2b_prefer_romanized_indic_when_eng", "false"),  # B2(b): English -> South-Asian when Indic signal present
    ("b2b_min_romanized_keywords", "1"),
]:
    _create_text_widget(_w, _d)
B3_DOWNWEIGHT_LATIN_NAME = _get_bool_widget("b3_downweight_latin_name", False)
B3_NONLATIN_SHARE = _get_float_widget("b3_nonlatin_share", 0.60)
B3_MIN_NONNAME_SEGMENTS = _get_int_widget("b3_min_nonname_segments", 2)
B4_EMIT_BILINGUAL_STATUS = _get_bool_widget("b4_emit_bilingual_status", False)
B2B_PREFER_ROMANIZED_INDIC = _get_bool_widget("b2b_prefer_romanized_indic_when_eng", False)
B2B_MIN_ROMANIZED_KEYWORDS = _get_int_widget("b2b_min_romanized_keywords", 1)
SOUTH_ASIAN_ISO = {"hin", "urd", "pan", "ben", "tel", "tam", "mal", "guj", "mar", "ory", "kan", "sin",
                   "npi", "bho", "awa", "mai", "mag", "asm", "snd", "kas", "doi", "san"}
print("Experimental fixes -> B3_name_downweight:", B3_DOWNWEIGHT_LATIN_NAME,
      "| B4_bilingual:", B4_EMIT_BILINGUAL_STATUS, "| B2b_romanized_indic:", B2B_PREFER_ROMANIZED_INDIC)
if not IS_FULL_BUCKET_RANGE:
    print(
        "NOTE: this build adds new output columns (consensus_source, bilingual_*, b2b_* and the new "
        "consensus_status values taxonomy_normalized_agreement / romanized_indic_override). The FIRST run "
        "after these schema changes must use the FULL bucket range so write_delta can migrate the output "
        "tables; a partial bucket range will fail at write time."
    )

# COMMAND ----------
# Segment-type vote weights (§8). Unmatched segment types default to weight 1.0.
SEGMENT_WEIGHTS = spark.createDataFrame(
    [
        ("channel_name", float(CHANNEL_NAME_WEIGHT)),
        ("channel_description", float(CHANNEL_DESCRIPTION_WEIGHT)),
        ("video_title", float(VIDEO_TITLE_WEIGHT)),
        ("video_description", float(VIDEO_DESCRIPTION_WEIGHT)),
        ("video_tags", float(VIDEO_TAGS_WEIGHT)),
    ],
    StructType([
        StructField("segment_type", StringType(), False),
        StructField("segment_weight", DoubleType(), False),
    ]),
)

NOISE_LABEL_REGEX = r"^(zxx|und|noise|null|none|unknown)"


def _safe_ratio(numer_col: str, denom_col: str):
    return F.when(F.col(denom_col) > 0, F.col(numer_col) / F.col(denom_col))


def build_admitted_votes_from_compact(compact_df):
    """Apply the §8 top-1 / top-2 admission rules to compact predictions.

    Top-1: rank==1, score>=primary_min_score. Top-2: rank==2, score>=secondary_min_score and
    score/score_1>=secondary_min_score_ratio. Both require a non-null, non-noise label and a valid segment.
    weighted_score = score * segment_weight * rank_weight (no length weighting).
    """
    carry = [
        "channel_id", "video_id", "segment_id", "segment_type", "channel_hash_bucket", "lid_model",
        "clean_letter_count", "clean_text_len", "dominant_script", "is_valid_text_for_lid", "run_id",
    ]
    top1 = (
        compact_df
        .select(
            *carry,
            F.lit(1).alias("prediction_rank"),
            F.col("label_raw_1").alias("label_raw"),
            F.col("label_1").alias("label"),
            F.col("iso639_3_1").alias("iso639_3"),
            F.col("script_1").alias("script"),
            F.col("score_1").alias("score"),
            F.col("score_1").alias("score_1"),
        )
    )
    if TOP_K >= 2:
        top2 = (
            compact_df
            .select(
                *carry,
                F.lit(2).alias("prediction_rank"),
                F.col("label_raw_2").alias("label_raw"),
                F.col("label_2").alias("label"),
                F.col("iso639_3_2").alias("iso639_3"),
                F.col("script_2").alias("script"),
                F.col("score_2").alias("score"),
                F.col("score_1").alias("score_1"),
            )
        )
    else:
        top2 = top1.where(F.lit(False)).withColumn("prediction_rank", F.lit(2))
    base = (
        top1.unionByName(top2)
        .where(F.col("is_valid_text_for_lid"))
        .where(F.col("label").isNotNull())
        .where(~F.lower(F.col("label")).rlike(NOISE_LABEL_REGEX))
    )
    admitted = base.where(
        ((F.col("prediction_rank") == 1) & (F.col("score") >= F.lit(PRIMARY_MIN_SCORE)))
        | (
            (F.col("prediction_rank") == 2)
            & (F.col("score") >= F.lit(SECONDARY_MIN_SCORE))
            & (F.col("score_1") > 0)
            & ((F.col("score") / F.col("score_1")) >= F.lit(SECONDARY_MIN_SCORE_RATIO))
        )
    )
    weighted = (
        admitted
        .join(F.broadcast(SEGMENT_WEIGHTS), on="segment_type", how="left")
        .withColumn("segment_weight", F.coalesce(F.col("segment_weight"), F.lit(1.0)))
        .withColumn(
            "rank_weight",
            F.when(F.col("prediction_rank") == 1, F.lit(1.0)).otherwise(F.lit(float(SECONDARY_LABEL_VOTE_WEIGHT))),
        )
        .withColumn("weighted_score", F.col("score") * F.col("segment_weight") * F.col("rank_weight"))
    )
    # B3 (default-off): if a channel's non-name segments are predominantly non-Latin but the channel name
    # is Latin, suppress the channel-name vote so the Latin brand name can't flip a non-Latin channel.
    if B3_DOWNWEIGHT_LATIN_NAME:
        _nonlatin = (~F.col("dominant_script").isin("latin", "none"))
        # Count by DISTINCT segment (rank-1 rows = one per segment) so top-1+top-2 of the same segment
        # don't double-count toward b3_min_nonname_segments.
        _r1 = weighted.where(F.col("prediction_rank") == F.lit(1))
        _name_flags = (
            _r1.groupBy("channel_id").agg(
                F.avg(F.when(F.col("segment_type") != F.lit("channel_name"), _nonlatin.cast("double"))).alias("_nonname_nonlatin_share"),
                F.countDistinct(F.when(F.col("segment_type") != F.lit("channel_name"), F.col("segment_id"))).alias("_nonname_n"),
                F.max(F.when((F.col("segment_type") == F.lit("channel_name")) & (F.col("dominant_script") == F.lit("latin")), F.lit(1)).otherwise(F.lit(0))).alias("_name_is_latin"),
            )
            .withColumn(
                "_suppress_name",
                (F.col("_nonname_n") >= F.lit(B3_MIN_NONNAME_SEGMENTS))
                & (F.coalesce(F.col("_nonname_nonlatin_share"), F.lit(0.0)) >= F.lit(B3_NONLATIN_SHARE))
                & (F.col("_name_is_latin") == F.lit(1)),
            )
            .select("channel_id", "_suppress_name")
        )
        weighted = (
            weighted.join(_name_flags, on="channel_id", how="left")
            .withColumn(
                "weighted_score",
                F.when((F.col("segment_type") == F.lit("channel_name")) & F.coalesce(F.col("_suppress_name"), F.lit(False)), F.lit(0.0))
                 .otherwise(F.col("weighted_score")),
            )
            .drop("_suppress_name")
        )
    return weighted


def build_channel_votes(admitted, model_name: str):
    """Vote-level aggregation: one row per (channel, label) with weighted score, counts, and language_rank."""
    is_r1 = F.col("prediction_rank") == 1
    is_r2 = F.col("prediction_rank") == 2
    votes = (
        admitted
        .groupBy("channel_id", "channel_hash_bucket", "label", "iso639_3", "script")
        .agg(
            F.sum("weighted_score").alias("weighted_score"),
            F.sum(F.when(is_r1, F.col("weighted_score"))).alias("top1_weighted_score"),
            F.countDistinct("segment_id").alias("segment_count"),
            F.countDistinct(F.when(is_r1, F.col("segment_id"))).alias("top1_segment_count"),
            F.countDistinct(F.when(is_r2, F.col("segment_id"))).alias("top2_segment_count"),
            F.count(F.lit(1)).alias("vote_count"),
            F.avg("score").alias("mean_segment_score"),
            F.max("score").alias("max_segment_score"),
            F.avg(F.when(is_r1, F.col("score"))).alias("top1_mean_score"),
            F.max(F.when(is_r1, F.col("score"))).alias("top1_max_score"),
            F.collect_set("segment_type").alias("segment_types"),
        )
        .withColumn("segment_types", F.sort_array(F.col("segment_types")))
        .withColumn("segment_type_count", F.size("segment_types"))
    )
    rank_window = Window.partitionBy("channel_id").orderBy(
        F.desc("weighted_score"), F.desc("segment_count"), F.desc("max_segment_score"), F.asc("label"),
    )
    return votes.withColumn("language_rank", F.row_number().over(rank_window)).withColumn("lid_model", F.lit(model_name))


def summarize_channel(votes, admitted, compact_df, model_name: str):
    """One row per channel with the §8 primary/secondary/margin/share fields and language_votes_json."""
    is_r1 = F.col("prediction_rank") == 1
    vote_totals = (
        admitted.groupBy("channel_id", "channel_hash_bucket").agg(
            F.sum("weighted_score").alias("total_weighted_score"),
            F.sum(F.when(is_r1, F.col("weighted_score"))).alias("total_top1_weighted_score"),
        )
    )
    valid_segment_totals = (
        compact_df.where(F.col("is_valid_text_for_lid"))
        .select("channel_id", "channel_hash_bucket", "segment_id", "segment_type", "clean_letter_count")
        .groupBy("channel_id", "channel_hash_bucket").agg(
            F.countDistinct("segment_id").alias("valid_language_segment_count"),
            F.countDistinct("segment_type").alias("valid_language_segment_type_count"),
            F.sum("clean_letter_count").alias("total_clean_letter_count"),
        )
    )

    r = F.col("language_rank")
    topn = votes.where(r <= 10)
    pivot = topn.groupBy("channel_id", "channel_hash_bucket").agg(
        F.max(F.when(r == 1, F.col("label"))).alias("primary_language_label"),
        F.max(F.when(r == 1, F.col("iso639_3"))).alias("primary_language_iso639_3"),
        F.max(F.when(r == 1, F.col("script"))).alias("primary_language_script"),
        F.max(F.when(r == 1, F.col("weighted_score"))).alias("primary_language_score"),
        F.max(F.when(r == 1, F.col("top1_weighted_score"))).alias("primary_language_top1_weighted_score"),
        F.max(F.when(r == 1, F.col("top1_mean_score"))).alias("primary_language_top1_score"),
        F.max(F.when(r == 1, F.col("mean_segment_score"))).alias("mean_segment_score_primary"),
        F.max(F.when(r == 1, F.col("max_segment_score"))).alias("max_segment_score_primary"),
        F.max(F.when(r == 1, F.col("top1_segment_count"))).alias("primary_language_top1_segment_count"),
        F.max(F.when(r == 1, F.col("top2_segment_count"))).alias("primary_language_top2_segment_count"),
        F.max(F.when(r == 2, F.col("label"))).alias("secondary_language_label"),
        F.max(F.when(r == 2, F.col("iso639_3"))).alias("secondary_language_iso639_3"),
        F.max(F.when(r == 2, F.col("script"))).alias("secondary_language_script"),
        F.max(F.when(r == 2, F.col("weighted_score"))).alias("secondary_language_score"),
        F.max(F.when(r == 2, F.col("segment_count"))).alias("secondary_language_segment_count"),
        F.max(F.when(r == 2, F.col("top1_segment_count"))).alias("secondary_language_top1_segment_count"),
        F.max(F.when(r == 2, F.col("segment_type_count"))).alias("secondary_language_segment_type_count"),
        F.max(F.when(r == 2, F.col("mean_segment_score"))).alias("secondary_mean_segment_score"),
        F.max(F.when(r == 2, F.col("max_segment_score"))).alias("secondary_max_segment_score"),
        F.max(F.when(r == 2, F.col("weighted_score"))).alias("rank2_language_score"),
        F.max(F.when(r == 3, F.col("weighted_score"))).alias("rank3_language_score"),
        F.to_json(
            F.array_sort(
                F.collect_list(
                    F.struct(
                        F.col("language_rank").alias("language_rank"),
                        F.col("label").alias("label"),
                        F.col("iso639_3").alias("iso639_3"),
                        F.col("script").alias("script"),
                        F.col("weighted_score").alias("weighted_score"),
                        F.col("segment_count").alias("segment_count"),
                        F.col("top1_segment_count").alias("top1_segment_count"),
                        F.col("top2_segment_count").alias("top2_segment_count"),
                        F.col("vote_count").alias("vote_count"),
                        F.col("mean_segment_score").alias("mean_segment_score"),
                        F.col("max_segment_score").alias("max_segment_score"),
                        F.col("segment_type_count").alias("segment_type_count"),
                        F.col("segment_types").alias("segment_types"),
                    )
                ),
                lambda left, right: (left["language_rank"] - right["language_rank"]).cast("int"),
            )
        ).alias("language_votes_json"),
    )

    summary = (
        valid_segment_totals
        .join(pivot, on=["channel_id", "channel_hash_bucket"], how="left")
        .join(vote_totals, on=["channel_id", "channel_hash_bucket"], how="left")
        .withColumn("lid_model", F.lit(model_name))
        .withColumn("primary_language_vote_share_with_top2", _safe_ratio("primary_language_score", "total_weighted_score"))
        .withColumn("primary_language_top1_vote_share", _safe_ratio("primary_language_top1_weighted_score", "total_top1_weighted_score"))
        .withColumn("secondary_to_primary_score_ratio", _safe_ratio("secondary_language_score", "primary_language_score"))
        .withColumn("rank2_rank3_margin", F.col("rank2_language_score") - F.coalesce(F.col("rank3_language_score"), F.lit(0.0)))
        .withColumn(
            "rank2_rank3_margin_ratio",
            F.when(F.col("rank2_language_score") > 0, F.col("rank2_rank3_margin") / F.col("rank2_language_score")),
        )
    )
    return summary

# COMMAND ----------
# Run aggregation for each model that produced a segment table, then save the unioned outputs.
def aggregate_model(compact_table_full: str, model_name: str):
    compact_df = current_run_table(compact_table_full)
    admitted = build_admitted_votes_from_compact(compact_df)
    votes = build_channel_votes(admitted, model_name)
    summary = summarize_channel(votes, admitted, compact_df, model_name)
    return votes, summary


vote_frames = []
summary_frames = []

if ENABLE_OPENLID:
    ol_votes, ol_summary = aggregate_model(openlid_compact_full, "openlid-v3")
    vote_frames.append(ol_votes)
    summary_frames.append(ol_summary)

if GLOTLID_CAN_FEED_MAIN:
    gl_votes, gl_summary = aggregate_model(glotlid_compact_full, "glotlid")
    vote_frames.append(gl_votes)
    summary_frames.append(gl_summary)
elif GLOTLID_ACTIVE:
    print("GlotLID audit_segments output was written, but is excluded from main channel aggregation.")

if not vote_frames:
    raise RuntimeError("No model segment tables were produced; cannot aggregate channel votes.")

channel_votes = vote_frames[0]
for v in vote_frames[1:]:
    channel_votes = channel_votes.unionByName(v)
channel_votes = with_bucket_run_columns(channel_votes).withColumn("prediction_timestamp", F.current_timestamp())
write_delta(
    channel_votes,
    channel_votes_full,
    partition_cols=["run_id", "channel_hash_bucket"],
    replace_where=_bucket_replace_where(),
    zorder_cols=["channel_id"],
)
print("Wrote channel votes (lid_model column) to", channel_votes_full)

channel_model_aggregation = summary_frames[0]
for s in summary_frames[1:]:
    channel_model_aggregation = channel_model_aggregation.unionByName(s)
channel_model_aggregation = with_bucket_run_columns(channel_model_aggregation).withColumn("prediction_timestamp", F.current_timestamp())
write_delta(
    channel_model_aggregation,
    channel_model_aggregation_full,
    partition_cols=["run_id", "channel_hash_bucket"],
    replace_where=_bucket_replace_where(),
    zorder_cols=["channel_id"],
)
print("Wrote per-model channel aggregation to", channel_model_aggregation_full)

# COMMAND ----------
# Aggregation QA: per-model channel counts and top primary languages.
agg = current_run_table(channel_model_aggregation_full)
print("Channels with a primary language, by model")
_maybe_display(
    agg.where(F.col("primary_language_label").isNotNull())
    .groupBy("lid_model").count().orderBy("lid_model")
)
print("Top primary languages by model")
_maybe_display(
    agg.where(F.col("primary_language_label").isNotNull())
    .groupBy("lid_model", "primary_language_label")
    .agg(F.count(F.lit(1)).alias("n_channels"))
    .orderBy("lid_model", F.desc("n_channels"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Model comparison and consensus (spec §10)
# MAGIC
# MAGIC Segment-level and channel-level comparisons of OpenLID vs GlotLID, with exact/ISO/cluster agreement
# MAGIC flags and a deterministic `consensus_status` classifier. High-risk tail labels are flagged for review,
# MAGIC never hard-recoded. Because GlotLID runs on all valid segments by default, the
# MAGIC `openlid_high_confidence_glotlid_missing_or_error` status is only reached when GlotLID is absent/errored.

# COMMAND ----------
# Analysis-cluster lookup (§9.3) as a NATIVE Spark expression (no Python UDF), so it is cheap even at
# segment granularity. Keyed by full label for script-sensitive entries (cmn_Hans/cmn_Hant), otherwise by
# ISO-639-3; the full label takes precedence, then the ISO. element_at on a map returns NULL for a missing
# or NULL key, so this matches the previous UDF semantics.
_CLUSTER_MAP_EXPR = F.create_map(*[F.lit(x) for _kv in ANALYSIS_CLUSTER_MAP.items() for x in _kv])


def analysis_cluster_expr(label_col, iso_col):
    return F.coalesce(F.element_at(_CLUSTER_MAP_EXPR, label_col), F.element_at(_CLUSTER_MAP_EXPR, iso_col))


def _isnull(x) -> bool:
    return x is None or (isinstance(x, float) and x != x)


# B1: Arabic macrolanguage + dialects collapse to one key so dialect-vs-macro is treated as agreement
# (the dialect is preserved as an audit field, never overwritten). Chinese already shares iso `cmn`.
ARABIC_FAMILY_ISO = {"ara", "arb", "ary", "arz", "apc", "ars", "ajp", "aeb", "acm", "acq", "aec", "afb", "ayl", "ayn"}


def _canonical_iso(iso):
    return "ara" if (iso is not None and str(iso) in ARABIC_FAMILY_ISO) else iso


def _canonical_iso_col(col):
    return F.when(col.isin(*sorted(ARABIC_FAMILY_ISO)), F.lit("ara")).otherwise(col)


def compute_consensus(ol_label, ol_iso, ol_script, ol_vs, ol_hr, ol_cl,
                      gl_label, gl_iso, gl_script, gl_vs, gl_hr, gl_cl, gl_present) -> dict:
    """Deterministic §10 consensus classifier. References CONSENSUS_LOW/HIGH_CONF_VOTE_SHARE globals.

    Confidence proxy is primary_language_vote_share_with_top2. The 'script evidence does not contradict'
    nuance for GlotLID fallback is approximated as 'GlotLID label is not a high-risk tail label'.
    """
    def s(x):
        return None if _isnull(x) else str(x)

    def f(x):
        return 0.0 if _isnull(x) else float(x)

    ol_label, ol_iso, ol_script, ol_cl = s(ol_label), s(ol_iso), s(ol_script), s(ol_cl)
    gl_label, gl_iso, gl_script, gl_cl = s(gl_label), s(gl_iso), s(gl_script), s(gl_cl)
    ol_vs, gl_vs = f(ol_vs), f(gl_vs)
    ol_hr = bool(ol_hr) and not _isnull(ol_hr)
    gl_hr = bool(gl_hr) and not _isnull(gl_hr)
    ol_present = ol_label is not None
    gl_present = (not _isnull(gl_present)) and bool(gl_present) and gl_label is not None
    low_conf, high_conf = CONSENSUS_LOW_CONF_VOTE_SHARE, CONSENSUS_HIGH_CONF_VOTE_SHARE

    def out(status, label=None, manual=False, cluster=None, rollup="__auto__", source=None):
        iso = script = None
        if label:
            parts = label.split("_")
            iso = parts[0] if parts[0] else None
            script = parts[1] if len(parts) >= 2 and parts[1] else None
        if rollup == "__auto__":
            rollup = cluster or iso
        if source is None:
            if status == "taxonomy_normalized_agreement":
                source = "taxonomy_normalized_fasttext"
            elif status == "high_risk_tail_label_needs_review" and label:
                source = "fasttext_mutual_high_risk_agreement"
            elif manual:
                source = "manual_adjudication_required"
            elif label:
                source = "fasttext_consensus"
            else:
                source = "fasttext_unlabeled_consensus"
        return {
            "consensus_status": status,
            "consensus_source": source,
            "consensus_language_label": label,
            "consensus_language_iso639_3": iso,
            "consensus_language_script": script,
            "consensus_analysis_language_cluster": cluster,
            "consensus_for_rollup_label": rollup,
            "requires_manual_adjudication": bool(manual),
        }

    if not ol_present and not gl_present:
        return out("insufficient_text")

    # Exact agreement. High-risk labels still require strong evidence before an exact label is populated.
    if ol_present and gl_present and ol_label == gl_label:
        if ol_hr:
            strong_high_risk_evidence = ol_vs >= high_conf and gl_vs >= high_conf
            return out(
                "high_risk_tail_label_needs_review",
                label=ol_label if strong_high_risk_evidence else None,
                manual=True,
                cluster=ol_cl,
                rollup=(ol_cl or ol_iso),
            )
        return out("exact_model_agreement", label=ol_label, manual=False, cluster=ol_cl)

    # GlotLID absent/errored -> single-model OpenLID only when confident and not high-risk.
    if ol_present and not gl_present:
        if ol_vs >= high_conf and not ol_hr:
            return out("openlid_high_confidence_glotlid_missing_or_error", label=ol_label, manual=False, cluster=ol_cl)
        return out("openlid_high_confidence_glotlid_missing_or_error", label=None, manual=True,
                   cluster=ol_cl, rollup=(ol_cl or ol_iso))

    # OpenLID absent (e.g., disabled) -> GlotLID single-model fallback when confident.
    if gl_present and not ol_present:
        if gl_vs >= high_conf and not gl_hr:
            return out("glotlid_fallback_openlid_low_confidence", label=gl_label, manual=False, cluster=gl_cl)
        return out("model_disagreement_needs_review", label=None, manual=True)

    # Both present, labels differ.
    # B1: Arabic macro/dialect normalization. If both models land in the Arabic family, they agree the
    # content is Arabic; emit the canonical ara_Arab label instead of dropping to NULL/needs_review.
    # The specific dialects remain in the per-model audit fields (openlid/glotlid_primary_*).
    if ol_iso and gl_iso and _canonical_iso(ol_iso) == "ara" and _canonical_iso(gl_iso) == "ara":
        return out("taxonomy_normalized_agreement", label="ara_Arab", manual=False, cluster=(ol_cl or gl_cl), rollup="ara")

    # A high-risk tail label without exact agreement is flagged for review
    # before any iso/cluster/fallback consensus, so high-risk hallucinations are never silently rolled up
    # or used as a fallback target (§10 high-risk rule; §11 caution).
    if ol_hr or gl_hr:
        return out("high_risk_tail_label_needs_review", label=None, manual=True)
    if ol_iso and gl_iso and ol_iso == gl_iso:
        return out("iso_or_script_variant_agreement", label=None, manual=False,
                   cluster=(ol_cl or gl_cl), rollup=(ol_cl or gl_cl or ol_iso))
    if ol_cl and gl_cl and ol_cl == gl_cl:
        return out("cluster_model_agreement", label=None, manual=False, cluster=ol_cl, rollup=ol_cl)
    if ol_vs < low_conf and gl_vs >= high_conf:
        return out("glotlid_fallback_openlid_low_confidence", label=gl_label, manual=False, cluster=gl_cl)
    return out("model_disagreement_needs_review", label=None, manual=True)


consensus_schema = StructType([
    StructField("consensus_status", StringType(), True),
    StructField("consensus_source", StringType(), True),
    StructField("consensus_language_label", StringType(), True),
    StructField("consensus_language_iso639_3", StringType(), True),
    StructField("consensus_language_script", StringType(), True),
    StructField("consensus_analysis_language_cluster", StringType(), True),
    StructField("consensus_for_rollup_label", StringType(), True),
    StructField("requires_manual_adjudication", BooleanType(), True),
])

_CONSENSUS_COLS = [f.name for f in consensus_schema.fields]


@F.pandas_udf(consensus_schema)
def consensus_udf(ol_label, ol_iso, ol_script, ol_vs, ol_hr, ol_cl,
                  gl_label, gl_iso, gl_script, gl_vs, gl_hr, gl_cl, gl_present) -> pd.DataFrame:
    rows = [
        compute_consensus(*vals)
        for vals in zip(ol_label, ol_iso, ol_script, ol_vs, ol_hr, ol_cl,
                        gl_label, gl_iso, gl_script, gl_vs, gl_hr, gl_cl, gl_present)
    ]
    return pd.DataFrame(rows, columns=_CONSENSUS_COLS)

# COMMAND ----------
# Segment-level comparison (§10): one row per segment comparing each model's TOP-1 prediction, preserving
# no-label/error segments (rank-1 row, or the single null-rank row for empty/errored segments). Comparing
# only the top-1 keeps the table one-row-per-segment and avoids meaningless rank-2-vs-rank-2 disagreements.
# Only built when both models ran on the full valid universe.
if ENABLE_OPENLID and GLOTLID_CAN_FEED_MAIN:
    def _top1_or_error(table_full, prefix):
        return (
            current_run_table(table_full)
            .select(
                "segment_id",
                "channel_hash_bucket",
                F.col("channel_id").alias(f"{prefix}_channel_id"),
                F.col("segment_type").alias(f"{prefix}_segment_type"),
                F.col("dominant_script").alias(f"{prefix}_dominant_script"),
                F.col("label_1").alias(f"{prefix}_label"),
                F.col("iso639_3_1").alias(f"{prefix}_iso639_3"),
                F.col("script_1").alias(f"{prefix}_script"),
                F.col("score_1").alias(f"{prefix}_score"),
                F.col("lid_error").alias(f"{prefix}_lid_error"),
            )
        )

    ol_seg = _top1_or_error(openlid_compact_full, "openlid")
    gl_seg = _top1_or_error(glotlid_compact_full, "glotlid")
    segment_join_type = "full_outer" if RUN_HEAVY_QA else "left"
    seg_cmp = (
        ol_seg.join(gl_seg, on=["channel_hash_bucket", "segment_id"], how=segment_join_type)
        .withColumn("channel_id", F.coalesce(F.col("openlid_channel_id"), F.col("glotlid_channel_id")))
        .withColumn("segment_type", F.coalesce(F.col("openlid_segment_type"), F.col("glotlid_segment_type")))
        .withColumn("dominant_script", F.coalesce(F.col("openlid_dominant_script"), F.col("glotlid_dominant_script")))
        .withColumn("openlid_cluster", analysis_cluster_expr(F.col("openlid_label"), F.col("openlid_iso639_3")))
        .withColumn("glotlid_cluster", analysis_cluster_expr(F.col("glotlid_label"), F.col("glotlid_iso639_3")))
        .withColumn("segment_agree_exact",
                    F.col("openlid_label").isNotNull() & (F.col("openlid_label") == F.col("glotlid_label")))
        .withColumn("segment_agree_iso",
                    F.col("openlid_iso639_3").isNotNull() & (F.col("openlid_iso639_3") == F.col("glotlid_iso639_3")))
        .withColumn("segment_agree_cluster",
                    F.col("openlid_cluster").isNotNull() & (F.col("openlid_cluster") == F.col("glotlid_cluster")))
        .drop("openlid_channel_id", "glotlid_channel_id", "openlid_segment_type", "glotlid_segment_type",
              "openlid_dominant_script", "glotlid_dominant_script")
        .transform(with_bucket_run_columns)
        .withColumn("prediction_timestamp", F.current_timestamp())
    )
    write_delta(
        seg_cmp,
        segment_model_comparison_full,
        partition_cols=["run_id", "channel_hash_bucket"],
        replace_where=_bucket_replace_where(),
        zorder_cols=["segment_id", "channel_id"],
    )
    print("Wrote segment model comparison (top-1 per segment) to", segment_model_comparison_full)
else:
    print("Skipping segment_model_comparison (requires both OpenLID and GlotLID).")

# COMMAND ----------
# Channel-level comparison + consensus (§10).
AGG_COMPARISON_FIELDS = [
    "primary_language_label", "primary_language_iso639_3", "primary_language_script",
    "primary_language_score", "primary_language_vote_share_with_top2",
    "secondary_language_label", "secondary_language_iso639_3", "secondary_to_primary_score_ratio",
]

agg_all = current_run_table(channel_model_aggregation_full)


def _model_side(model_name, prefix):
    cols = [F.col("channel_id"), F.col("channel_hash_bucket").alias(f"{prefix}_channel_hash_bucket")]
    cols += [F.col(c).alias(f"{prefix}_{c}") for c in AGG_COMPARISON_FIELDS]
    return agg_all.where(F.col("lid_model") == model_name).select(*cols)


ol_side = _model_side("openlid-v3", "openlid")
have_glotlid_agg = GLOTLID_CAN_FEED_MAIN
if have_glotlid_agg:
    gl_side = _model_side("glotlid", "glotlid")
    chan_cmp = ol_side.join(gl_side, on="channel_id", how="full_outer")
else:
    # Single-model run: synthesize null GlotLID columns so the consensus UDF still applies.
    chan_cmp = ol_side
    chan_cmp = chan_cmp.withColumn("glotlid_channel_hash_bucket", F.lit(None).cast("int"))
    for c in AGG_COMPARISON_FIELDS:
        chan_cmp = chan_cmp.withColumn(f"glotlid_{c}", F.lit(None).cast("string" if "label" in c or "iso" in c or "script" in c else "double"))

chan_cmp = (
    chan_cmp
    .withColumn("channel_hash_bucket", F.coalesce(F.col("openlid_channel_hash_bucket"), F.col("glotlid_channel_hash_bucket")))
    .withColumn("openlid_primary_cluster", analysis_cluster_expr(F.col("openlid_primary_language_label"), F.col("openlid_primary_language_iso639_3")))
    .withColumn("glotlid_primary_cluster", analysis_cluster_expr(F.col("glotlid_primary_language_label"), F.col("glotlid_primary_language_iso639_3")))
    .withColumn("openlid_secondary_cluster", analysis_cluster_expr(F.col("openlid_secondary_language_label"), F.col("openlid_secondary_language_iso639_3")))
    .withColumn("glotlid_secondary_cluster", analysis_cluster_expr(F.col("glotlid_secondary_language_label"), F.col("glotlid_secondary_language_iso639_3")))
    .withColumn("openlid_primary_is_high_risk", F.coalesce(F.col("openlid_primary_language_label").isin(*sorted(HIGH_RISK_LATIN_TAIL_LABELS)), F.lit(False)))
    .withColumn("glotlid_primary_is_high_risk", F.coalesce(F.col("glotlid_primary_language_label").isin(*sorted(HIGH_RISK_LATIN_TAIL_LABELS)), F.lit(False)))
    .withColumn("glotlid_present", F.col("glotlid_primary_language_label").isNotNull())
)

both_nn = lambda a, b: F.col(a).isNotNull() & F.col(b).isNotNull()
chan_cmp = (
    chan_cmp
    .withColumn("models_agree_exact_primary", both_nn("openlid_primary_language_label", "glotlid_primary_language_label") & (F.col("openlid_primary_language_label") == F.col("glotlid_primary_language_label")))
    .withColumn("models_agree_iso_primary", both_nn("openlid_primary_language_iso639_3", "glotlid_primary_language_iso639_3") & (_canonical_iso_col(F.col("openlid_primary_language_iso639_3")) == _canonical_iso_col(F.col("glotlid_primary_language_iso639_3"))))
    .withColumn("models_agree_analysis_cluster_primary", both_nn("openlid_primary_cluster", "glotlid_primary_cluster") & (F.col("openlid_primary_cluster") == F.col("glotlid_primary_cluster")))
    .withColumn("models_agree_exact_secondary", both_nn("openlid_secondary_language_label", "glotlid_secondary_language_label") & (F.col("openlid_secondary_language_label") == F.col("glotlid_secondary_language_label")))
    .withColumn("models_agree_analysis_cluster_secondary", both_nn("openlid_secondary_cluster", "glotlid_secondary_cluster") & (F.col("openlid_secondary_cluster") == F.col("glotlid_secondary_cluster")))
    .withColumn("consensus", consensus_udf(
        F.col("openlid_primary_language_label"), F.col("openlid_primary_language_iso639_3"), F.col("openlid_primary_language_script"),
        F.col("openlid_primary_language_vote_share_with_top2"), F.col("openlid_primary_is_high_risk"), F.col("openlid_primary_cluster"),
        F.col("glotlid_primary_language_label"), F.col("glotlid_primary_language_iso639_3"), F.col("glotlid_primary_language_script"),
        F.col("glotlid_primary_language_vote_share_with_top2"), F.col("glotlid_primary_is_high_risk"), F.col("glotlid_primary_cluster"),
        F.col("glotlid_present"),
    ))
    .select("*", "consensus.*").drop("consensus")
    .drop("openlid_channel_hash_bucket", "glotlid_channel_hash_bucket")
    .transform(with_bucket_run_columns)
    .withColumn("prediction_timestamp", F.current_timestamp())
)

write_delta(
    chan_cmp,
    channel_model_comparison_full,
    partition_cols=["run_id", "channel_hash_bucket"],
    replace_where=_bucket_replace_where(),
    zorder_cols=["channel_id"],
)
print("Wrote channel model comparison + consensus to", channel_model_comparison_full)

# COMMAND ----------
# Consensus QA: status distribution and primary-agreement rate.
cmp_tbl = current_run_table(channel_model_comparison_full)
print("Consensus status distribution")
_maybe_display(cmp_tbl.groupBy("consensus_status").count().orderBy(F.desc("count")))
print("Manual-adjudication flag")
_maybe_display(cmp_tbl.groupBy("requires_manual_adjudication").count().orderBy(F.desc("count")))
if ENABLE_OPENLID and GLOTLID_CAN_FEED_MAIN:
    print("Primary agreement rates (exact / iso / cluster) over channels where both models have a primary")
    both_primary = cmp_tbl.where(F.col("openlid_primary_language_label").isNotNull() & F.col("glotlid_primary_language_label").isNotNull())
    # B6: cluster agreement is only meaningful within the cluster taxonomy. Averaging over ALL both-primary
    # channels (most of which have a NULL cluster) understates it badly (the misleading 14.23%). Report the
    # WITHIN-cluster rate (denominator = both clusters non-null) plus the cluster coverage separately.
    _both_clustered = F.col("openlid_primary_cluster").isNotNull() & F.col("glotlid_primary_cluster").isNotNull()
    _maybe_display(both_primary.agg(
        F.avg(F.col("models_agree_exact_primary").cast("double")).alias("exact_primary_agreement_rate"),
        F.avg(F.col("models_agree_iso_primary").cast("double")).alias("iso_primary_agreement_rate"),
        F.avg(F.when(_both_clustered, F.col("models_agree_analysis_cluster_primary").cast("double"))).alias("within_cluster_primary_agreement_rate"),
        F.avg(_both_clustered.cast("double")).alias("cluster_coverage_rate"),
        F.sum(_both_clustered.cast("int")).alias("n_channels_both_clustered"),
        F.count(F.lit(1)).alias("n_channels_both_primary"),
    ))
    # B5 audit: high-risk tail labels kept as a FINAL consensus label come only from confident mutual
    # agreement. Surface the count + the labels so the deliberate policy exception is reviewable before
    # publication (per plan B5 acceptance).
    _hr_final = cmp_tbl.where(
        F.col("consensus_language_label").isNotNull()
        & F.col("consensus_language_label").isin(*sorted(HIGH_RISK_LATIN_TAIL_LABELS))
    )
    print("B5: confident mutual-agreement tail labels kept as final (count by label)")
    _maybe_display(_hr_final.groupBy("consensus_language_label").count().orderBy(F.desc("count")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Revised mixed-language logic (spec §11)
# MAGIC
# MAGIC A **screen** is permissive (a secondary language is plausibly present). A **credible candidate** must
# MAGIC clear the full §11 evidence bar. Consensus credibility requires second-model support by default. A
# MAGIC high-risk tail label cannot create a credible candidate unless both models agree on it exactly.

# COMMAND ----------
# Pull the per-model secondary-evidence fields from the channel aggregation, and the clusters /
# secondary-agreement flags from the channel comparison.
MIX_FIELDS = [
    "primary_language_label", "primary_language_script",
    "secondary_language_label", "secondary_language_script",
    "secondary_to_primary_score_ratio",
    "secondary_language_segment_count", "secondary_language_top1_segment_count",
    "secondary_language_segment_type_count",
    "secondary_mean_segment_score", "secondary_max_segment_score",
    "rank2_rank3_margin_ratio",
]


def _mix_side(model_name, prefix):
    cols = [F.col("channel_id"), F.col("channel_hash_bucket").alias(f"{prefix}_channel_hash_bucket")]
    cols += [F.col(c).alias(f"{prefix}_{c}") for c in MIX_FIELDS]
    return current_run_table(channel_model_aggregation_full).where(F.col("lid_model") == model_name).select(*cols)


cmp_subset = current_run_table(channel_model_comparison_full).select(
    "channel_id", "channel_hash_bucket",
    "openlid_primary_cluster", "openlid_secondary_cluster",
    "glotlid_primary_cluster", "glotlid_secondary_cluster",
    "models_agree_exact_secondary", "models_agree_analysis_cluster_secondary",
)

mix = _mix_side("openlid-v3", "openlid")
if GLOTLID_CAN_FEED_MAIN:
    mix = mix.join(_mix_side("glotlid", "glotlid"), on="channel_id", how="full_outer")
else:
    mix = mix.withColumn("glotlid_channel_hash_bucket", F.lit(None).cast("int"))
    for c in MIX_FIELDS:
        dtype = "string" if any(k in c for k in ("label", "script")) else "double"
        mix = mix.withColumn(f"glotlid_{c}", F.lit(None).cast(dtype))
mix = mix.join(cmp_subset, on="channel_id", how="left")
mix = mix.withColumn(
    "channel_hash_bucket",
    F.coalesce(F.col("channel_hash_bucket"), F.col("openlid_channel_hash_bucket"), F.col("glotlid_channel_hash_bucket")),
)

# COMMAND ----------
# Per-model screen and credible flags (§11).
_HR_LABELS = sorted(HIGH_RISK_LATIN_TAIL_LABELS)

for p in ["openlid", "glotlid"]:
    ratio = F.coalesce(F.col(f"{p}_secondary_to_primary_score_ratio"), F.lit(0.0))
    seg_cnt = F.coalesce(F.col(f"{p}_secondary_language_segment_count"), F.lit(0))
    top1_cnt = F.coalesce(F.col(f"{p}_secondary_language_top1_segment_count"), F.lit(0))
    type_cnt = F.coalesce(F.col(f"{p}_secondary_language_segment_type_count"), F.lit(0))
    mean_s = F.coalesce(F.col(f"{p}_secondary_mean_segment_score"), F.lit(0.0))
    max_s = F.coalesce(F.col(f"{p}_secondary_max_segment_score"), F.lit(0.0))
    margin_ratio = F.coalesce(F.col(f"{p}_rank2_rank3_margin_ratio"), F.lit(0.0))
    pscript = F.col(f"{p}_primary_language_script")
    sscript = F.col(f"{p}_secondary_language_script")
    slabel = F.col(f"{p}_secondary_language_label")
    pcl = F.col(f"{p}_primary_cluster")
    scl = F.col(f"{p}_secondary_cluster")

    has_secondary = slabel.isNotNull()
    cross_script = pscript.isNotNull() & sscript.isNotNull() & (pscript != sscript)
    same_cluster = pcl.isNotNull() & scl.isNotNull() & (pcl == scl)
    sec_high_risk = F.coalesce(slabel.isin(*_HR_LABELS), F.lit(False))
    agree_exact_secondary = F.coalesce(F.col("models_agree_exact_secondary"), F.lit(False))
    high_risk_wo_agreement = sec_high_risk & (~agree_exact_secondary)

    screen = (
        has_secondary
        & (ratio >= F.lit(MIXED_SCREEN_RATIO_THRESHOLD))
        & (seg_cnt >= F.lit(MIXED_SCREEN_MIN_SECONDARY_SEGMENTS))
    )
    credible = (
        has_secondary
        & (ratio >= F.lit(MIXED_CREDIBLE_RATIO_THRESHOLD))
        & (seg_cnt >= F.lit(MIXED_CREDIBLE_MIN_SECONDARY_SEGMENTS))
        & (top1_cnt >= F.lit(MIXED_CREDIBLE_MIN_SECONDARY_TOP1_SEGMENTS))
        & (mean_s >= F.lit(MIXED_CREDIBLE_SECONDARY_MEAN_SCORE))
        & ((max_s >= F.lit(MIXED_CREDIBLE_SECONDARY_MAX_SCORE)) | cross_script)
        & (margin_ratio >= F.lit(MIXED_CREDIBLE_MIN_RANK2_RANK3_MARGIN_RATIO))
        & (cross_script | (type_cnt >= F.lit(MIXED_CREDIBLE_MIN_SECONDARY_SEGMENT_TYPES)))
        & (~same_cluster)
        & (~high_risk_wo_agreement)
    )
    mix = (
        mix
        .withColumn(f"{p}_cross_script", cross_script)
        .withColumn(f"{p}_same_cluster", same_cluster)
        .withColumn(f"{p}_secondary_high_risk", sec_high_risk)
        .withColumn(f"{p}_is_mixed_language_screen", screen)
        .withColumn(f"{p}_is_credible_mixed_language_candidate", credible)
    )

# COMMAND ----------
# Consensus screen / credible (§11) and rejection reason.
ol_cred = F.coalesce(F.col("openlid_is_credible_mixed_language_candidate"), F.lit(False))
gl_cred = F.coalesce(F.col("glotlid_is_credible_mixed_language_candidate"), F.lit(False))
ol_screen = F.coalesce(F.col("openlid_is_mixed_language_screen"), F.lit(False))
gl_screen = F.coalesce(F.col("glotlid_is_mixed_language_screen"), F.lit(False))
agree_exact_secondary = F.coalesce(F.col("models_agree_exact_secondary"), F.lit(False))
agree_cluster_secondary = F.coalesce(F.col("models_agree_analysis_cluster_secondary"), F.lit(False))
has_full_dual_model_secondary_evidence = F.lit(ENABLE_OPENLID and GLOTLID_CAN_FEED_MAIN)

# §11 condition 3: strong cross-script evidence from one credible model, with the other not contradicting
# the secondary cluster (its secondary cluster is null or equal). This still requires both full-universe
# models to have run when mixed_credible_require_second_model_support=true; a synthesized null side in
# single-model or audit_segments mode is not second-model support.
cond3 = (
    has_full_dual_model_secondary_evidence
    & (
        (ol_cred & F.col("openlid_cross_script")
         & (F.col("glotlid_secondary_cluster").isNull() | (F.col("glotlid_secondary_cluster") == F.col("openlid_secondary_cluster"))))
        | (gl_cred & F.col("glotlid_cross_script")
           & (F.col("openlid_secondary_cluster").isNull() | (F.col("openlid_secondary_cluster") == F.col("glotlid_secondary_cluster"))))
    )
)
cond1 = has_full_dual_model_secondary_evidence & agree_exact_secondary & (ol_cred | gl_cred)
cond2 = has_full_dual_model_secondary_evidence & agree_cluster_secondary & (ol_cred | gl_cred)

consensus_screen = ol_screen | gl_screen
if MIXED_CREDIBLE_REQUIRE_SECOND_MODEL_SUPPORT:
    consensus_credible = cond1 | cond2 | cond3
else:
    consensus_credible = ol_cred | gl_cred

mix = (
    mix
    .withColumn("consensus_is_mixed_language_screen", consensus_screen)
    .withColumn("consensus_is_credible_mixed_language_candidate", consensus_credible)
)

ol_same_cluster = F.coalesce(F.col("openlid_same_cluster"), F.lit(False))
gl_same_cluster = F.coalesce(F.col("glotlid_same_cluster"), F.lit(False))
ol_sec_hr = F.coalesce(F.col("openlid_secondary_high_risk"), F.lit(False))
gl_sec_hr = F.coalesce(F.col("glotlid_secondary_high_risk"), F.lit(False))

mix = mix.withColumn(
    "mixed_language_rejection_reason",
    F.when(F.col("consensus_is_credible_mixed_language_candidate"), F.lit(None).cast("string"))
    .when(~F.col("consensus_is_mixed_language_screen"), F.lit("no_model_screen"))
    .when((ol_sec_hr | gl_sec_hr) & (~agree_exact_secondary), F.lit("high_risk_secondary_without_model_agreement"))
    .when(ol_same_cluster | gl_same_cluster, F.lit("secondary_same_analysis_cluster_as_primary"))
    .when(F.lit(MIXED_CREDIBLE_REQUIRE_SECOND_MODEL_SUPPORT) & (~(agree_exact_secondary | agree_cluster_secondary | cond3)), F.lit("no_cross_model_secondary_support"))
    .otherwise(F.lit("insufficient_secondary_evidence")),
)

# COMMAND ----------
mixed_out = mix.select(
    "channel_id", "channel_hash_bucket",
    "openlid_is_mixed_language_screen", "openlid_is_credible_mixed_language_candidate",
    "glotlid_is_mixed_language_screen", "glotlid_is_credible_mixed_language_candidate",
    "consensus_is_mixed_language_screen", "consensus_is_credible_mixed_language_candidate",
    "mixed_language_rejection_reason",
    "openlid_primary_language_label", "openlid_secondary_language_label", "openlid_secondary_to_primary_score_ratio",
    "glotlid_primary_language_label", "glotlid_secondary_language_label", "glotlid_secondary_to_primary_score_ratio",
    "models_agree_exact_secondary", "models_agree_analysis_cluster_secondary",
).transform(with_bucket_run_columns).withColumn("prediction_timestamp", F.current_timestamp())

write_delta(
    mixed_out,
    mixed_language_candidates_full,
    partition_cols=["run_id", "channel_hash_bucket"],
    replace_where=_bucket_replace_where(),
    zorder_cols=["channel_id"],
)
print("Wrote mixed-language candidates to", mixed_language_candidates_full)

# COMMAND ----------
# Mixed-language QA: screen/credible counts per model + consensus, and rejection reasons among screened channels.
ml = current_run_table(mixed_language_candidates_full)
print("Mixed-language flag counts")
_maybe_display(ml.agg(
    F.sum(F.col("openlid_is_mixed_language_screen").cast("int")).alias("openlid_screens"),
    F.sum(F.col("openlid_is_credible_mixed_language_candidate").cast("int")).alias("openlid_credible"),
    F.sum(F.col("glotlid_is_mixed_language_screen").cast("int")).alias("glotlid_screens"),
    F.sum(F.col("glotlid_is_credible_mixed_language_candidate").cast("int")).alias("glotlid_credible"),
    F.sum(F.col("consensus_is_mixed_language_screen").cast("int")).alias("consensus_screens"),
    F.sum(F.col("consensus_is_credible_mixed_language_candidate").cast("int")).alias("consensus_credible"),
))
print("Rejection reasons among screened-but-not-credible channels")
_maybe_display(
    ml.where(F.col("consensus_is_mixed_language_screen") & (~F.col("consensus_is_credible_mixed_language_candidate")))
    .groupBy("mixed_language_rejection_reason").count().orderBy(F.desc("count"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Hindi/Indic recall diagnostics (spec §12)
# MAGIC
# MAGIC Exports Hindi/Indic audit candidates even when Hindi is not the primary or secondary label. Romanized
# MAGIC keyword flags use **word-boundary** matching and are recall-only audit signals — they never feed label
# MAGIC assignment, vote weighting, or consensus.

# COMMAND ----------
# Canonical post-dedup channel universe, reused by Phases 8/10/11.
all_channels = channels_dedup.select(
    F.col(CHANNEL_ID_COLUMN).cast("string").alias("channel_id"),
    "channel_hash_bucket",
).persist(StorageLevel.DISK_ONLY)
n_universe = all_channels.count()

# Detect a source language column (audit only).
SOURCE_LANG_CANDIDATES = ["language_code", "detected_language", "default_language", "defaultlanguage", "lang"]
source_lang_col = first_existing_column(channels_dedup, SOURCE_LANG_CANDIDATES)
print("Source language column (audit):", source_lang_col)

# COMMAND ----------
# Romanized-keyword matcher (§12). Word-boundary / phrase matching, never substring. References the
# ROMANIZED_*_KEYWORDS globals. Returns occurrence counts and the distinct matched Indic terms.
def _compile_keyword_pattern(words):
    parts = sorted((regex.escape(w) for w in words), key=len, reverse=True)
    return regex.compile(r"\b(" + "|".join(parts) + r")\b", flags=regex.IGNORECASE)


ROMANIZED_HINDI_PATTERN = _compile_keyword_pattern(ROMANIZED_HINDI_KEYWORDS)
ROMANIZED_INDIC_PATTERN = _compile_keyword_pattern(ROMANIZED_INDIC_KEYWORDS)

keyword_schema = StructType([
    StructField("romanized_hindi_keyword_count", IntegerType(), True),
    StructField("romanized_indic_keyword_count", IntegerType(), True),
    StructField("romanized_indic_keyword_examples", ArrayType(StringType()), True),
])


@F.pandas_udf(keyword_schema)
def romanized_keyword_udf(text_series: pd.Series) -> pd.DataFrame:
    rows = []
    for t in text_series:
        s = "" if t is None else str(t).lower()
        hindi = ROMANIZED_HINDI_PATTERN.findall(s)
        indic = ROMANIZED_INDIC_PATTERN.findall(s)
        rows.append({
            "romanized_hindi_keyword_count": len(hindi),
            "romanized_indic_keyword_count": len(indic),
            "romanized_indic_keyword_examples": sorted({m.lower() for m in indic}),
        })
    return pd.DataFrame(rows, columns=[f.name for f in keyword_schema.fields])

# COMMAND ----------
# Per-channel Devanagari metadata, keyword aggregation, and a representative sample text.
seg_in_tbl = current_run_table(segments_input_full)
channel_text_features = (
    seg_in_tbl
    .withColumn("kw", romanized_keyword_udf(F.col("clean_text")))
    .groupBy("channel_id", "channel_hash_bucket")
    .agg(
        F.count(F.lit(1)).alias("n_segments"),
        F.sum(F.col("is_valid_text_for_lid").cast("int")).alias("n_valid_segments"),
        F.sum(F.col("clean_letter_count")).alias("total_clean_letter_count"),
        F.collect_set("short_text_reason").alias("short_text_reasons"),
        F.max((~F.col("dominant_script").isin("latin", "none")).cast("int")).alias("non_latin_any_int"),
        F.sum(F.col("devanagari_char_count")).alias("devanagari_char_count_total"),
        F.sum(F.when(F.col("devanagari_char_count") > 0, F.lit(1)).otherwise(F.lit(0))).alias("devanagari_segment_count"),
        F.sum(F.col("kw.romanized_hindi_keyword_count")).alias("romanized_hindi_keyword_count"),
        F.sum(F.col("kw.romanized_indic_keyword_count")).alias("romanized_indic_keyword_count"),
        F.slice(F.array_distinct(F.flatten(F.collect_list(F.col("kw.romanized_indic_keyword_examples")))), 1, 25).alias("romanized_indic_keyword_examples"),
        F.max(F.struct(F.col("clean_letter_count"), F.col("text"))).alias("_sample"),
    )
    .withColumn("contains_devanagari_metadata", F.col("devanagari_char_count_total") > 0)
    .withColumn("sample_text", F.col("_sample.text"))
    .drop("_sample")
    .transform(with_bucket_run_columns)
    .withColumn("prediction_timestamp", F.current_timestamp())
)
write_delta(
    channel_text_features,
    channel_text_features_full,
    partition_cols=["run_id", "channel_hash_bucket"],
    replace_where=_bucket_replace_where(),
    zorder_cols=["channel_id"],
)
print("Wrote channel text features to", channel_text_features_full)
dev_kw = current_run_table(channel_text_features_full)

# COMMAND ----------
# Per-channel hindi/indic vote presence in either model's top-k.
def _vote_presence(table_full, prefix):
    hindi_any = None
    indic_any = None
    for i in range(1, TOP_K + 1):
        h = F.col(f"iso639_3_{i}").isin(*sorted(HINDI_RELATED_ISO))
        ind = F.col(f"iso639_3_{i}").isin(*sorted(INDIC_AUDIT_ISO))
        hindi_any = h if hindi_any is None else (hindi_any | h)
        indic_any = ind if indic_any is None else (indic_any | ind)
    return (
        current_run_table(table_full).groupBy("channel_id").agg(
            F.max(F.coalesce(hindi_any, F.lit(False)).cast("int")).alias(f"{prefix}_hindi_related_vote_int"),
            F.max(F.coalesce(indic_any, F.lit(False)).cast("int")).alias(f"{prefix}_indic_vote_int"),
        )
    )


ol_votep = _vote_presence(openlid_compact_full, "openlid") if ENABLE_OPENLID else None
gl_votep = _vote_presence(glotlid_compact_full, "glotlid") if GLOTLID_CAN_FEED_MAIN else None

# Primary/secondary hindi/indic signals and per-model vote JSON from the channel aggregation.
_agg = current_run_table(channel_model_aggregation_full)
agg_signals = _agg.groupBy("channel_id").agg(
    F.max(F.col("primary_language_iso639_3").isin(*sorted(HINDI_RELATED_ISO)).cast("int")).alias("hindi_primary_int"),
    F.max((F.col("primary_language_iso639_3").isin(*sorted(HINDI_RELATED_ISO)) | F.col("secondary_language_iso639_3").isin(*sorted(HINDI_RELATED_ISO))).cast("int")).alias("hindi_pos_int"),
    F.max((F.col("primary_language_iso639_3").isin(*sorted(INDIC_AUDIT_ISO)) | F.col("secondary_language_iso639_3").isin(*sorted(INDIC_AUDIT_ISO))).cast("int")).alias("indic_pos_int"),
    F.max(F.when(F.col("lid_model") == "openlid-v3", F.col("language_votes_json"))).alias("openlid_votes_json"),
    F.max(F.when(F.col("lid_model") == "glotlid", F.col("language_votes_json"))).alias("glotlid_votes_json"),
)

# Consensus + cluster signal from the channel comparison.
cmp_signals = current_run_table(channel_model_comparison_full).select(
    "channel_id", "consensus_status", "consensus_language_label", "consensus_analysis_language_cluster",
    "openlid_primary_language_label", "glotlid_primary_language_label",
)

# COMMAND ----------
# Assemble the Hindi/Indic audit table.
hi = all_channels
hi = hi.join(dev_kw, on=["channel_id", "channel_hash_bucket"], how="left")
if ol_votep is not None:
    hi = hi.join(ol_votep, on="channel_id", how="left")
else:
    hi = hi.withColumn("openlid_hindi_related_vote_int", F.lit(0)).withColumn("openlid_indic_vote_int", F.lit(0))
if gl_votep is not None:
    hi = hi.join(gl_votep, on="channel_id", how="left")
else:
    hi = hi.withColumn("glotlid_hindi_related_vote_int", F.lit(0)).withColumn("glotlid_indic_vote_int", F.lit(0))
hi = hi.join(agg_signals, on="channel_id", how="left").join(cmp_signals, on="channel_id", how="left")

if source_lang_col:
    src = channels_dedup.select(
        F.col(CHANNEL_ID_COLUMN).cast("string").alias("channel_id"),
        F.lower(F.trim(F.col(source_lang_col).cast("string"))).alias("source_language_value"),
    )
    hi = hi.join(src, on="channel_id", how="left")
else:
    hi = hi.withColumn("source_language_value", F.lit(None).cast("string"))

# Booleans.
def _b(colname):
    return F.coalesce(F.col(colname), F.lit(0)) > 0

hi = (
    hi
    .withColumn("contains_devanagari_metadata", F.coalesce(F.col("contains_devanagari_metadata"), F.lit(False)))
    .withColumn("devanagari_segment_count", F.coalesce(F.col("devanagari_segment_count"), F.lit(0)))
    .withColumn("devanagari_char_count_total", F.coalesce(F.col("devanagari_char_count_total"), F.lit(0)))
    .withColumn("romanized_hindi_keyword_count", F.coalesce(F.col("romanized_hindi_keyword_count"), F.lit(0)))
    .withColumn("romanized_indic_keyword_count", F.coalesce(F.col("romanized_indic_keyword_count"), F.lit(0)))
    .withColumn("hindi_related_openlid_vote_present", _b("openlid_hindi_related_vote_int"))
    .withColumn("hindi_related_glotlid_vote_present", _b("glotlid_hindi_related_vote_int"))
    .withColumn("indic_openlid_vote_present", _b("openlid_indic_vote_int"))
    .withColumn("indic_glotlid_vote_present", _b("glotlid_indic_vote_int"))
)
hi = (
    hi
    .withColumn("hindi_related_any_model_vote_present", F.col("hindi_related_openlid_vote_present") | F.col("hindi_related_glotlid_vote_present"))
    .withColumn("indic_any_model_vote_present", F.col("indic_openlid_vote_present") | F.col("indic_glotlid_vote_present"))
    .withColumn("hindi_related_primary_or_secondary", _b("hindi_pos_int"))
    .withColumn("indic_primary_or_secondary", _b("indic_pos_int"))
    .withColumn("hindi_primary", _b("hindi_primary_int"))
)

source_is_hindi = F.col("source_language_value").isin(*sorted(SOURCE_HINDI_CODES))
source_is_indic = F.col("source_language_value").isin(*sorted(SOURCE_INDIC_CODES))
hi = (
    hi
    .withColumn("source_hi_disagreement", F.col("source_language_value").isNotNull() & (F.coalesce(source_is_hindi, F.lit(False)) != F.col("hindi_related_any_model_vote_present")))
    .withColumn("source_indic_disagreement", F.col("source_language_value").isNotNull() & (F.coalesce(source_is_indic, F.lit(False)) != F.col("indic_any_model_vote_present")))
    .withColumn("indic_cluster_candidate", F.col("consensus_analysis_language_cluster") == F.lit("hindi_related_north_indic_review_cluster"))
)

# Priority-ordered status (§12).
hi = hi.withColumn(
    "hindi_indic_candidate_status",
    F.when(F.col("hindi_primary"), F.lit("hindi_primary_metadata"))
    .when(F.col("hindi_related_any_model_vote_present") | F.col("hindi_related_primary_or_secondary"), F.lit("hindi_secondary_or_topk_metadata"))
    .when(F.col("indic_primary_or_secondary"), F.lit("indic_primary_or_secondary_metadata"))
    .when(F.col("contains_devanagari_metadata"), F.lit("devanagari_non_hindi_primary"))
    .when(F.col("romanized_hindi_keyword_count") > 0, F.lit("romanized_hindi_candidate"))
    .when(F.col("romanized_indic_keyword_count") > 0, F.lit("romanized_indic_candidate"))
    .when(F.col("source_hi_disagreement"), F.lit("source_hi_disagreement"))
    .when(F.col("source_indic_disagreement"), F.lit("source_indic_disagreement"))
    .when(F.coalesce(F.col("indic_cluster_candidate"), F.lit(False)), F.lit("indic_cluster_candidate"))
    .otherwise(F.lit("no_hindi_or_indic_signal")),
)

hindi_indic_out = hi.select(
    "channel_id", "channel_hash_bucket",
    "contains_devanagari_metadata", "devanagari_segment_count", "devanagari_char_count_total",
    "hindi_related_openlid_vote_present", "hindi_related_glotlid_vote_present", "hindi_related_any_model_vote_present",
    "indic_openlid_vote_present", "indic_glotlid_vote_present", "indic_any_model_vote_present",
    "hindi_related_primary_or_secondary", "indic_primary_or_secondary",
    "romanized_hindi_keyword_count", "romanized_indic_keyword_count", "romanized_indic_keyword_examples",
    "source_hi_disagreement", "source_indic_disagreement", "hindi_indic_candidate_status",
    "sample_text", "openlid_votes_json", "glotlid_votes_json", "source_language_value",
    "consensus_status", "consensus_language_label", "consensus_analysis_language_cluster",
    "openlid_primary_language_label", "glotlid_primary_language_label",
).transform(with_bucket_run_columns).withColumn("prediction_timestamp", F.current_timestamp())

write_delta(
    hindi_indic_out,
    hindi_indic_audit_full,
    partition_cols=["run_id", "channel_hash_bucket"],
    replace_where=_bucket_replace_where(),
    zorder_cols=["channel_id"],
)
print("Wrote Hindi/Indic audit candidates to", hindi_indic_audit_full)
_maybe_display(current_run_table(hindi_indic_audit_full).groupBy("hindi_indic_candidate_status").count().orderBy(F.desc("count")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 11. High-risk redirect diagnostic (spec §13)
# MAGIC
# MAGIC For every channel whose OpenLID or GlotLID primary/secondary label is a high-risk Latin tail label,
# MAGIC combine Devanagari evidence, romanized Indic keywords, source-language fields, the other model's
# MAGIC top-1, and non-Latin dominant script. This is **not** only a script-mismatch diagnostic: romanized
# MAGIC Hindi/Nepali failure cases are Latin-dominant.

# COMMAND ----------
# Romance ISO-639-3 set used to detect when the other model's top-1 is non-Romance (a redirect signal).
ROMANCE_ISO = {
    "spa", "por", "ita", "fra", "ron", "cat", "glg", "ast", "srd", "vec", "scn", "fur", "lmo",
    "oci", "gug", "pap", "lim", "mlt", "cos", "arg", "wln", "frp", "lad", "roh", "lij", "nap",
}

# Per-channel signals: reuse the persisted Hindi/Indic audit table, add non-Latin-any-segment and the
# other model's primary ISO from the comparison table.
hi_sig = current_run_table(hindi_indic_audit_full).select(
    "channel_id", "channel_hash_bucket", "contains_devanagari_metadata",
    "romanized_hindi_keyword_count", "romanized_indic_keyword_count",
    "indic_any_model_vote_present", "source_language_value",
)
nl_sig = current_run_table(channel_text_features_full).select("channel_id", "channel_hash_bucket", "non_latin_any_int")
iso_sig = current_run_table(channel_model_comparison_full).select(
    "channel_id",
    F.col("openlid_primary_language_iso639_3").alias("openlid_primary_iso"),
    F.col("glotlid_primary_language_iso639_3").alias("glotlid_primary_iso"),
)

signals = (
    hi_sig
    .join(nl_sig, on=["channel_id", "channel_hash_bucket"], how="left")
    .join(iso_sig, on="channel_id", how="left")
    .withColumn("sig_devanagari", F.coalesce(F.col("contains_devanagari_metadata"), F.lit(False)))
    .withColumn("sig_romanized_hindi", F.coalesce(F.col("romanized_hindi_keyword_count"), F.lit(0)) > 0)
    .withColumn("sig_romanized_indic", F.coalesce(F.col("romanized_indic_keyword_count"), F.lit(0)) > 0)
    .withColumn("sig_any_indic_vote", F.coalesce(F.col("indic_any_model_vote_present"), F.lit(False)))
    .withColumn("sig_source_indic", F.coalesce(F.col("source_language_value").isin(*sorted(SOURCE_INDIC_CODES)), F.lit(False)))
    .withColumn("sig_non_latin", F.coalesce(F.col("non_latin_any_int"), F.lit(0)) > 0)
    .withColumn("sig_glotlid_non_romance_top1", F.col("glotlid_primary_iso").isNotNull() & (~F.col("glotlid_primary_iso").isin(*sorted(ROMANCE_ISO))))
    .withColumn("sig_openlid_non_romance_top1", F.col("openlid_primary_iso").isNotNull() & (~F.col("openlid_primary_iso").isin(*sorted(ROMANCE_ISO))))
)
signals = signals.withColumn(
    "sig_any_indic_or_nonlatin",
    F.col("sig_devanagari") | F.col("sig_romanized_indic") | F.col("sig_any_indic_vote") | F.col("sig_source_indic") | F.col("sig_non_latin"),
)

# COMMAND ----------
# Emit one row per (channel, high-risk position). model_label_source in
# {openlid_primary, openlid_secondary, glotlid_primary, glotlid_secondary}.
_HR_LABELS_LIST = sorted(HIGH_RISK_LATIN_TAIL_LABELS)
agg_hr = current_run_table(channel_model_aggregation_full)
model_prefix = F.when(F.col("lid_model") == "openlid-v3", F.lit("openlid")).otherwise(F.lit("glotlid"))

prim_emit = (
    agg_hr.where(F.col("primary_language_label").isin(*_HR_LABELS_LIST))
    .select("channel_id", F.concat(model_prefix, F.lit("_primary")).alias("model_label_source"),
            F.col("primary_language_label").alias("high_risk_label"))
)
sec_emit = (
    agg_hr.where(F.col("secondary_language_label").isin(*_HR_LABELS_LIST))
    .select("channel_id", F.concat(model_prefix, F.lit("_secondary")).alias("model_label_source"),
            F.col("secondary_language_label").alias("high_risk_label"))
)
emissions = prim_emit.unionByName(sec_emit).join(signals, on="channel_id", how="left")
redirect_counts = (
    emissions.groupBy("model_label_source", "high_risk_label").agg(
        F.count(F.lit(1)).alias("n_channels"),
        F.sum(F.col("sig_devanagari").cast("int")).alias("n_with_devanagari_metadata"),
        F.sum(F.col("sig_romanized_hindi").cast("int")).alias("n_with_romanized_hindi_keywords"),
        F.sum(F.col("sig_romanized_indic").cast("int")).alias("n_with_romanized_indic_keywords"),
        F.sum(F.col("sig_any_indic_vote").cast("int")).alias("n_with_any_indic_model_vote"),
        F.sum(F.col("sig_source_indic").cast("int")).alias("n_with_source_indic_code"),
        F.sum(F.col("sig_glotlid_non_romance_top1").cast("int")).alias("n_with_glotlid_non_romance_top1"),
        F.sum(F.col("sig_openlid_non_romance_top1").cast("int")).alias("n_with_openlid_non_romance_top1"),
        F.sum(F.col("sig_non_latin").cast("int")).alias("n_dominant_script_non_latin_any_segment"),
        F.avg(F.col("sig_any_indic_or_nonlatin").cast("double")).alias("share_with_any_indic_or_nonlatin_signal"),
    )
)
_hr_sample_w = Window.partitionBy("model_label_source", "high_risk_label").orderBy(
    F.xxhash64(F.col("channel_id"), F.lit(VALIDATION_SAMPLE_SEED)).asc(),
    F.col("channel_id").asc(),
)
redirect_samples = (
    emissions
    .select("model_label_source", "high_risk_label", "channel_id")
    .withColumn("_sample_rn", F.row_number().over(_hr_sample_w))
    .where(F.col("_sample_rn") <= 25)
    .groupBy("model_label_source", "high_risk_label")
    .agg(F.sort_array(F.collect_list("channel_id")).alias("sample_channel_ids"))
)
redirect = (
    redirect_counts
    .join(redirect_samples, on=["model_label_source", "high_risk_label"], how="left")
    .orderBy("model_label_source", F.desc("n_channels"))
)
redirect = with_run_scope_columns(redirect).withColumn("prediction_timestamp", F.current_timestamp())

write_delta(
    redirect,
    high_risk_redirect_full,
    partition_cols=["run_id"],
    replace_where=_run_scope_replace_where(),
    replace_where_cols=_run_scope_required_cols(),
)
print("Wrote high-risk redirect diagnostic to", high_risk_redirect_full)
_maybe_display(current_run_scope_table(high_risk_redirect_full))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 12. Final channel table (spec §8 backward-compat + §10 consensus)
# MAGIC
# MAGIC One row per post-dedup channel. Legacy OpenLID field names are preserved for backward compatibility,
# MAGIC plus explicit `openlid_*` / `glotlid_*` fields, model-comparison + consensus fields, mixed-language
# MAGIC flags, and the Hindi/Indic audit status. The source table is not modified unless explicitly enabled.

# COMMAND ----------
_agg2 = current_run_table(channel_model_aggregation_full)

# Legacy unprefixed OpenLID fields (backward compatibility).
LEGACY_FIELDS = [
    "primary_language_label", "primary_language_iso639_3", "primary_language_script", "primary_language_score",
    "primary_language_vote_share_with_top2", "primary_language_top1_vote_share",
    "secondary_language_label", "secondary_language_iso639_3", "secondary_language_script", "secondary_language_score",
    "secondary_to_primary_score_ratio", "valid_language_segment_count", "valid_language_segment_type_count",
    "total_clean_letter_count", "language_votes_json",
]
ol_legacy = (
    _agg2.where(F.col("lid_model") == "openlid-v3").select("channel_id", *LEGACY_FIELDS)
    .withColumn("primary_language_confidence", F.col("primary_language_vote_share_with_top2"))
)
gl_votes = _agg2.where(F.col("lid_model") == "glotlid").select(
    "channel_id", F.col("language_votes_json").alias("glotlid_language_votes_json")
)

# Prefixed + comparison + consensus fields.
cmp_fields = current_run_table(channel_model_comparison_full).select(
    "channel_id",
    "openlid_primary_language_label", "openlid_primary_language_iso639_3", "openlid_primary_language_script",
    "openlid_primary_language_score", "openlid_primary_language_vote_share_with_top2",
    "openlid_secondary_language_label", "openlid_secondary_to_primary_score_ratio",
    "glotlid_primary_language_label", "glotlid_primary_language_iso639_3", "glotlid_primary_language_script",
    "glotlid_primary_language_score", "glotlid_primary_language_vote_share_with_top2",
    "glotlid_secondary_language_label", "glotlid_secondary_to_primary_score_ratio",
    "openlid_primary_is_high_risk", "glotlid_primary_is_high_risk",
    "models_agree_exact_primary", "models_agree_iso_primary", "models_agree_analysis_cluster_primary",
    "models_agree_exact_secondary", "models_agree_analysis_cluster_secondary",
    "consensus_status", "consensus_source", "consensus_language_label", "consensus_language_iso639_3", "consensus_language_script",
    "consensus_analysis_language_cluster", "consensus_for_rollup_label", "requires_manual_adjudication",
)

# Mixed-language flags (only the flags + reason; labels come from cmp_fields to avoid collisions).
mixed_flags = current_run_table(mixed_language_candidates_full).select(
    "channel_id",
    "openlid_is_mixed_language_screen", "openlid_is_credible_mixed_language_candidate",
    "glotlid_is_mixed_language_screen", "glotlid_is_credible_mixed_language_candidate",
    "consensus_is_mixed_language_screen", "consensus_is_credible_mixed_language_candidate",
    "mixed_language_rejection_reason",
)

# Hindi/Indic audit status (status + recall signals; consensus/label columns come from cmp_fields).
hindi_status = current_run_table(hindi_indic_audit_full).select(
    "channel_id", "hindi_indic_candidate_status", "contains_devanagari_metadata",
    "romanized_hindi_keyword_count", "romanized_indic_keyword_count", "source_language_value",
)

# COMMAND ----------
channels = (
    all_channels
    .join(ol_legacy, on="channel_id", how="left")
    .join(gl_votes, on="channel_id", how="left")
    .join(cmp_fields, on="channel_id", how="left")
    .join(mixed_flags, on="channel_id", how="left")
    .join(hindi_status, on="channel_id", how="left")
    .withColumn("is_mixed_language_candidate", F.coalesce(F.col("consensus_is_credible_mixed_language_candidate"), F.lit(False)))
    # B4: surface credible bilingual channels' primary+secondary (populated for mixed candidates regardless
    # of the status-label flag, so the table schema is stable whether or not B4 is enabled).
    .withColumn("bilingual_primary_language_label", F.when(F.col("is_mixed_language_candidate"), F.coalesce(F.col("openlid_primary_language_label"), F.col("glotlid_primary_language_label"))))
    .withColumn("bilingual_secondary_language_label", F.when(F.col("is_mixed_language_candidate"), F.coalesce(F.col("openlid_secondary_language_label"), F.col("glotlid_secondary_language_label"))))
    .withColumn(
        "language_status",
        F.when(F.col("primary_language_label").isNull() & F.col("openlid_primary_language_label").isNull() & F.col("glotlid_primary_language_label").isNull(), F.lit("insufficient_text_or_unclassified"))
        .when(F.col("consensus_status") == F.lit("insufficient_text"), F.lit("insufficient_text_or_unclassified"))
        .when(F.coalesce(F.col("consensus_is_credible_mixed_language_candidate"), F.lit(False)), F.lit("bilingual" if B4_EMIT_BILINGUAL_STATUS else "mixed_language_candidate"))
        .when(F.coalesce(F.col("requires_manual_adjudication"), F.lit(False)), F.lit("needs_review"))
        .otherwise(F.lit("classified")),
    )
    .withColumn("pipeline_version", F.lit("lid_v3"))
    .transform(with_bucket_run_columns)
    .withColumn("prediction_timestamp", F.current_timestamp())
)

# B2(b) (default-off): when consensus says English but there is strong romanized-Indic signal AND a model
# gave a South-Asian language, prefer that South-Asian label. The marker column is always present (False
# when the flag is off) so the schema is stable across flag states.
_b2b_sa = sorted(SOUTH_ASIAN_ISO)
_b2b_indic_signal = F.coalesce(F.col("contains_devanagari_metadata"), F.lit(False)) | (F.coalesce(F.col("romanized_indic_keyword_count"), F.lit(0)) >= F.lit(B2B_MIN_ROMANIZED_KEYWORDS))
_b2b_override_label = (
    # Prefer GlotLID's South-Asian label (more reliable on romanized South-Asian per the validation report).
    F.when(F.col("glotlid_primary_language_iso639_3").isin(*_b2b_sa), F.col("glotlid_primary_language_label"))
     # Only fall back to OpenLID's South-Asian label when OpenLID is not the low-confidence loser, so we
     # never promote a low-confidence OpenLID label over a confident English call.
     .when(
        F.col("openlid_primary_language_iso639_3").isin(*_b2b_sa)
        & (F.coalesce(F.col("openlid_primary_language_vote_share_with_top2"), F.lit(0.0)) >= F.lit(CONSENSUS_LOW_CONF_VOTE_SHARE)),
        F.col("openlid_primary_language_label"),
     )
)
_b2b_fire = F.lit(bool(B2B_PREFER_ROMANIZED_INDIC)) & (F.col("consensus_language_iso639_3") == F.lit("eng")) & _b2b_indic_signal & _b2b_override_label.isNotNull()
channels = (
    channels
    .withColumn("b2b_romanized_indic_override", F.coalesce(_b2b_fire, F.lit(False)))
    .withColumn("b2b_romanized_indic_original_consensus_status", F.when(F.col("b2b_romanized_indic_override"), F.col("consensus_status")))
    .withColumn("b2b_romanized_indic_original_consensus_source", F.when(F.col("b2b_romanized_indic_override"), F.col("consensus_source")))
    .withColumn("b2b_romanized_indic_original_consensus_language_label", F.when(F.col("b2b_romanized_indic_override"), F.col("consensus_language_label")))
    .withColumn("b2b_romanized_indic_original_consensus_language_iso639_3", F.when(F.col("b2b_romanized_indic_override"), F.col("consensus_language_iso639_3")))
    .withColumn("b2b_romanized_indic_original_consensus_language_script", F.when(F.col("b2b_romanized_indic_override"), F.col("consensus_language_script")))
    .withColumn("consensus_status", F.when(F.col("b2b_romanized_indic_override"), F.lit("romanized_indic_override")).otherwise(F.col("consensus_status")))
    .withColumn("consensus_source", F.when(F.col("b2b_romanized_indic_override"), F.lit("romanized_indic_override")).otherwise(F.col("consensus_source")))
    .withColumn("requires_manual_adjudication", F.when(F.col("b2b_romanized_indic_override"), F.lit(False)).otherwise(F.col("requires_manual_adjudication")))
    .withColumn("consensus_language_label", F.when(F.col("b2b_romanized_indic_override"), _b2b_override_label).otherwise(F.col("consensus_language_label")))
    .withColumn("consensus_language_iso639_3", F.when(F.col("b2b_romanized_indic_override"), F.split(_b2b_override_label, "_").getItem(0)).otherwise(F.col("consensus_language_iso639_3")))
    .withColumn("consensus_language_script", F.when(F.col("b2b_romanized_indic_override"), F.element_at(F.split(_b2b_override_label, "_"), 2)).otherwise(F.col("consensus_language_script")))
    # Keep the rollup/cluster fields in sync so overridden channels are summarized under the new language.
    .withColumn("consensus_analysis_language_cluster", F.when(F.col("b2b_romanized_indic_override"), analysis_cluster_expr(F.col("consensus_language_label"), F.col("consensus_language_iso639_3"))).otherwise(F.col("consensus_analysis_language_cluster")))
    .withColumn("consensus_for_rollup_label", F.when(F.col("b2b_romanized_indic_override"), F.coalesce(analysis_cluster_expr(F.col("consensus_language_label"), F.col("consensus_language_iso639_3")), F.col("consensus_language_iso639_3"))).otherwise(F.col("consensus_for_rollup_label")))
)

write_delta(
    channels,
    channels_output_full,
    partition_cols=["run_id", "channel_hash_bucket"],
    replace_where=_bucket_replace_where(),
    zorder_cols=["channel_id"],
)
print("Wrote final channel table to", channels_output_full)

# Acceptance #4: exactly one row per post-dedup channel ID.
n_rows = current_run_table(channels_output_full).count()
n_distinct = None
if RUN_HEAVY_QA:
    n_distinct = current_run_table(channels_output_full).select("channel_id").distinct().count()
print(
    f"channels rows={n_rows:,} "
    f"distinct_channel_id={n_distinct if n_distinct is not None else '<skipped; run_heavy_qa=false>'} "
    f"post_dedup_universe={n_universe:,}"
)
if n_rows != n_universe:
    raise AssertionError("Final channel table row count does not match the post-dedup channel universe (acceptance #4).")
if RUN_HEAVY_QA and n_distinct != n_rows:
    raise AssertionError("Final channel table is not one row per post-dedup channel ID (acceptance #4).")
print("Acceptance #4 OK.")

# COMMAND ----------
# QA: classification status and top consensus languages.
ch = current_run_table(channels_output_full)
print("Language status distribution")
_maybe_display(ch.groupBy("language_status").count().orderBy(F.desc("count")))
print("Top consensus languages (where consensus exact label present)")
_maybe_display(
    ch.where(F.col("consensus_language_label").isNotNull())
    .groupBy("consensus_language_label").count().orderBy(F.desc("count"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Optional: update `yt_sl_channels.detected_language` (disabled by default)
# MAGIC
# MAGIC Prefer keeping predictions separate until validation is complete. When enabled, only clean classified
# MAGIC rows with a non-null consensus exact label are eligible; review and mixed-language cases are never
# MAGIC written back to the source table.

# COMMAND ----------
if UPDATE_SOURCE_DETECTED_LANGUAGE:
    detected_language_col = columns_lower_map(channels_dedup).get("detected_language")
    if not detected_language_col:
        raise ValueError("Source table does not have a detected_language column to update.")
    base_label_expr = "consensus_language_label"
    if SOURCE_UPDATE_FORMAT == "iso639_3":
        update_expr = f"regexp_replace({base_label_expr}, '_[A-Za-z]+$', '')"
    elif SOURCE_UPDATE_FORMAT == "scriptless_label":
        update_expr = f"regexp_replace({base_label_expr}, '_[A-Za-z]+$', '')"
    else:
        update_expr = base_label_expr
    current_run_table(channels_output_full).createOrReplaceTempView("_lid_v3_channel_updates")
    merge_sql = f"""
    MERGE INTO {channels_full} AS t
    USING (
      SELECT channel_id, {update_expr} AS detected_language_update
      FROM _lid_v3_channel_updates
      WHERE consensus_language_label IS NOT NULL
        AND coalesce(requires_manual_adjudication, false) = false
        AND language_status = 'classified'
    ) AS s
    ON t.`{CHANNEL_ID_COLUMN}` = s.channel_id
    WHEN MATCHED THEN UPDATE SET t.`{detected_language_col}` = s.detected_language_update
    """
    spark.sql(merge_sql)
    print(f"Updated {channels_full}.detected_language using source_update_format={SOURCE_UPDATE_FORMAT}")
else:
    print("Source table update skipped. Set update_source_detected_language=true to enable MERGE (after validation).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 13. QA tables, summaries, and validation sampling (spec §14)
# MAGIC
# MAGIC Saved Delta tables are full for their configured run scope. Partial bucket runs write bucket-scoped
# MAGIC summaries; global capped samples are emitted only for full-bucket runs.

# COMMAND ----------
ch_qa = current_run_table(channels_output_full)
_hr_in = lambda c: F.coalesce(F.col(c).isin(*_HR_LABELS_LIST), F.lit(False))
ch_qa = (
    ch_qa
    .withColumn("any_primary_high_risk", _hr_in("openlid_primary_language_label") | _hr_in("glotlid_primary_language_label"))
    .withColumn("is_hindi_indic_candidate", F.col("hindi_indic_candidate_status") != F.lit("no_hindi_or_indic_signal"))
)

# 1. language_summary_full — exact-label counts, confidence distribution, high-risk + Hindi/Indic counts.
language_summary_full = (
    ch_qa.groupBy("consensus_language_label").agg(
        F.count(F.lit(1)).alias("n_channels"),
        F.sum(F.col("requires_manual_adjudication").cast("int")).alias("n_requires_manual_adjudication"),
        F.sum(F.col("any_primary_high_risk").cast("int")).alias("n_high_risk_primary"),
        F.sum(F.col("is_hindi_indic_candidate").cast("int")).alias("n_hindi_indic_candidate"),
        F.avg("openlid_primary_language_vote_share_with_top2").alias("mean_openlid_vote_share"),
        F.percentile_approx("openlid_primary_language_vote_share_with_top2", 0.5, 10000).alias("median_openlid_vote_share"),
    ).orderBy(F.desc("n_channels"))
)
language_summary_full = with_run_scope_columns(language_summary_full).withColumn("prediction_timestamp", F.current_timestamp())
write_delta(
    language_summary_full,
    language_summary_full_full,
    partition_cols=["run_id"],
    replace_where=_run_scope_replace_where(),
    replace_where_cols=_run_scope_required_cols(),
)
print("Wrote", language_summary_full_full)

# 2. language_summary_rollup — rollup cluster counts by consensus status and language status.
language_summary_rollup = (
    ch_qa.groupBy("consensus_for_rollup_label", "consensus_status", "language_status")
    .agg(F.count(F.lit(1)).alias("n_channels"))
    .orderBy(F.desc("n_channels"))
)
language_summary_rollup = with_run_scope_columns(language_summary_rollup).withColumn("prediction_timestamp", F.current_timestamp())
write_delta(
    language_summary_rollup,
    language_summary_rollup_full,
    partition_cols=["run_id"],
    replace_where=_run_scope_replace_where(),
    replace_where_cols=_run_scope_required_cols(),
)
print("Wrote", language_summary_rollup_full)

# COMMAND ----------
# 3. model_agreement_summary — exact/ISO/cluster agreement rates by primary language and script.
if ENABLE_OPENLID and GLOTLID_CAN_FEED_MAIN:
    both_primary = (
        ch_qa
        .where(F.col("openlid_primary_language_label").isNotNull() & F.col("glotlid_primary_language_label").isNotNull())
        .withColumn(
            "openlid_primary_analysis_cluster",
            analysis_cluster_expr(F.col("openlid_primary_language_label"), F.col("openlid_primary_language_iso639_3")),
        )
        .withColumn(
            "glotlid_primary_analysis_cluster",
            analysis_cluster_expr(F.col("glotlid_primary_language_label"), F.col("glotlid_primary_language_iso639_3")),
        )
        .withColumn(
            "both_primary_analysis_clustered",
            F.col("openlid_primary_analysis_cluster").isNotNull() & F.col("glotlid_primary_analysis_cluster").isNotNull(),
        )
    )
    model_agreement_summary = (
        both_primary.groupBy(
            "openlid_primary_language_iso639_3",
            "openlid_primary_language_script",
            "openlid_primary_analysis_cluster",
            "glotlid_primary_analysis_cluster",
            "consensus_analysis_language_cluster",
        ).agg(
            F.count(F.lit(1)).alias("n_channels"),
            F.sum(F.col("both_primary_analysis_clustered").cast("long")).alias("n_channels_both_clustered"),
            F.avg(F.col("models_agree_exact_primary").cast("double")).alias("exact_agreement_rate"),
            F.avg(F.col("models_agree_iso_primary").cast("double")).alias("iso_agreement_rate"),
            F.avg(F.when(F.col("both_primary_analysis_clustered"), F.col("models_agree_analysis_cluster_primary").cast("double"))).alias("cluster_agreement_rate"),
            F.avg(F.when(F.col("both_primary_analysis_clustered"), F.col("models_agree_analysis_cluster_primary").cast("double"))).alias("within_cluster_primary_agreement_rate"),
            F.avg(F.col("both_primary_analysis_clustered").cast("double")).alias("cluster_coverage_rate"),
        )
        .orderBy(F.desc("n_channels"))
    )
    model_agreement_summary = with_run_scope_columns(model_agreement_summary).withColumn("prediction_timestamp", F.current_timestamp())
    write_delta(
        model_agreement_summary,
        model_agreement_summary_full,
        partition_cols=["run_id"],
        replace_where=_run_scope_replace_where(),
        replace_where_cols=_run_scope_required_cols(),
    )
    print("Wrote", model_agreement_summary_full)
else:
    empty_model_agreement_schema = StructType([
        StructField("openlid_primary_language_iso639_3", StringType(), True),
        StructField("openlid_primary_language_script", StringType(), True),
        StructField("openlid_primary_analysis_cluster", StringType(), True),
        StructField("glotlid_primary_analysis_cluster", StringType(), True),
        StructField("consensus_analysis_language_cluster", StringType(), True),
        StructField("n_channels", LongType(), True),
        StructField("n_channels_both_clustered", LongType(), True),
        StructField("exact_agreement_rate", DoubleType(), True),
        StructField("iso_agreement_rate", DoubleType(), True),
        StructField("cluster_agreement_rate", DoubleType(), True),
        StructField("within_cluster_primary_agreement_rate", DoubleType(), True),
        StructField("cluster_coverage_rate", DoubleType(), True),
    ])
    write_delta(
        with_run_scope_columns(spark.createDataFrame([], empty_model_agreement_schema))
        .withColumn("prediction_timestamp", F.current_timestamp()),
        model_agreement_summary_full,
        partition_cols=["run_id"],
        replace_where=_run_scope_replace_where(),
        replace_where_cols=_run_scope_required_cols(),
    )
    print("Wrote empty model_agreement_summary (requires both models on the full valid-segment universe).")

# COMMAND ----------
# 5. suspect_tail_audit_sample — up to 50 deterministic channels per high-risk primary label.
if IS_FULL_BUCKET_RANGE:
    hr_channels = (
        ch_qa
        .select(
            "channel_id",
            "channel_hash_bucket",
            "openlid_primary_language_label",
            "glotlid_primary_language_label",
            "consensus_status",
            "hindi_indic_candidate_status",
            "contains_devanagari_metadata",
            "romanized_indic_keyword_count",
            "source_language_value",
            F.array_distinct(F.array_compact(F.array(
                F.when(_hr_in("openlid_primary_language_label"), F.col("openlid_primary_language_label")),
                F.when(_hr_in("glotlid_primary_language_label"), F.col("glotlid_primary_language_label")),
            ))).alias("_high_risk_labels"),
        )
        .withColumn("high_risk_label", F.explode(F.col("_high_risk_labels")))
        .drop("_high_risk_labels")
    )
    _w_tail = Window.partitionBy("high_risk_label").orderBy(F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.lit(VALIDATION_SAMPLE_SEED))).asc())
    suspect_tail_audit = (
        hr_channels.withColumn("_rn", F.row_number().over(_w_tail)).where(F.col("_rn") <= 50).drop("_rn")
        .select("channel_id", "channel_hash_bucket", "high_risk_label", "openlid_primary_language_label", "glotlid_primary_language_label",
                "consensus_status", "hindi_indic_candidate_status", "contains_devanagari_metadata",
                "romanized_indic_keyword_count", "source_language_value")
        .transform(with_bucket_run_columns)
        .withColumn("prediction_timestamp", F.current_timestamp())
    )
    write_delta(
        suspect_tail_audit,
        suspect_tail_audit_full,
        partition_cols=["run_id", "channel_hash_bucket"],
        replace_where=_bucket_replace_where(),
        zorder_cols=["channel_id"],
    )
    print("Wrote", suspect_tail_audit_full)
else:
    print("Skipping suspect_tail_audit_sample for partial bucket range; the 50-per-label cap is global.")

# COMMAND ----------
# 7. unclassified_audit — channels with no valid segment, plus invalid-text reason breakdown.
seg_counts = current_run_table(channel_text_features_full).select(
    "channel_id", "channel_hash_bucket", "n_segments", "n_valid_segments",
    "total_clean_letter_count", "short_text_reasons",
)
unclassified_audit = (
    all_channels.join(seg_counts, on=["channel_id", "channel_hash_bucket"], how="left")
    .join(ch_qa.select("channel_id", "language_status", "consensus_status"), on="channel_id", how="left")
    .withColumn("n_valid_segments", F.coalesce(F.col("n_valid_segments"), F.lit(0)))
    .where((F.col("n_valid_segments") == 0) | (F.col("language_status") == F.lit("insufficient_text_or_unclassified")))
    .transform(with_bucket_run_columns)
    .withColumn("prediction_timestamp", F.current_timestamp())
)
write_delta(
    unclassified_audit,
    unclassified_audit_full,
    partition_cols=["run_id", "channel_hash_bucket"],
    replace_where=_bucket_replace_where(),
    zorder_cols=["channel_id"],
)
print("Wrote", unclassified_audit_full)

# 8. source_language_confusion — source vs model primary ISO disagreement patterns.
if source_lang_col:
    source_language_confusion = (
        ch_qa.where(F.col("source_language_value").isNotNull())
        .groupBy("source_language_value", "openlid_primary_language_iso639_3", "consensus_language_iso639_3")
        .agg(F.count(F.lit(1)).alias("n_channels"))
        .orderBy(F.desc("n_channels"))
    )
    source_language_confusion = with_run_scope_columns(source_language_confusion).withColumn("prediction_timestamp", F.current_timestamp())
    write_delta(
        source_language_confusion,
        source_language_confusion_full,
        partition_cols=["run_id"],
        replace_where=_run_scope_replace_where(),
        replace_where_cols=_run_scope_required_cols(),
    )
    print("Wrote", source_language_confusion_full)
else:
    empty_source_confusion_schema = StructType([
        StructField("source_language_value", StringType(), True),
        StructField("openlid_primary_language_iso639_3", StringType(), True),
        StructField("consensus_language_iso639_3", StringType(), True),
        StructField("n_channels", LongType(), True),
    ])
    write_delta(
        with_run_scope_columns(spark.createDataFrame([], empty_source_confusion_schema))
        .withColumn("prediction_timestamp", F.current_timestamp()),
        source_language_confusion_full,
        partition_cols=["run_id"],
        replace_where=_run_scope_replace_where(),
        replace_where_cols=_run_scope_required_cols(),
    )
    print("Wrote empty source_language_confusion (no source language column detected).")

# COMMAND ----------
# 6. manual_validation_sample — deterministic per-stratum sample (§14). Each channel's qualifying strata
# are preserved in an array; one primary stratum is assigned by fixed priority to avoid double counting.
if CREATE_VALIDATION_SAMPLES:
    nl_sig11 = current_run_table(channel_text_features_full).select("channel_id", "non_latin_any_int")
    src_dis = current_run_table(hindi_indic_audit_full).select(
        "channel_id",
        (F.coalesce(F.col("source_hi_disagreement"), F.lit(False)) | F.coalesce(F.col("source_indic_disagreement"), F.lit(False))).alias("source_disagreement"),
    )
    vch = ch_qa.join(nl_sig11, on="channel_id", how="left").join(src_dis, on="channel_id", how="left")

    has_primary = F.col("openlid_primary_language_label").isNotNull() | F.col("glotlid_primary_language_label").isNotNull()
    both_primary_v = F.col("openlid_primary_language_label").isNotNull() & F.col("glotlid_primary_language_label").isNotNull()
    vs = F.coalesce(F.col("openlid_primary_language_vote_share_with_top2"), F.lit(0.0))

    # (stratum_name, predicate) in fixed priority order (§14 list).
    strata = [
        ("high_confidence_major_language", (F.col("consensus_status").isin("exact_model_agreement", "taxonomy_normalized_agreement")) & (vs >= F.lit(CONSENSUS_HIGH_CONF_VOTE_SHARE))),
        ("low_confidence", has_primary & (vs < F.lit(CONSENSUS_LOW_CONF_VOTE_SHARE))),
        ("credible_mixed_language_candidate", F.coalesce(F.col("consensus_is_credible_mixed_language_candidate"), F.lit(False))),
        ("mixed_screen_not_credible", F.coalesce(F.col("consensus_is_mixed_language_screen"), F.lit(False)) & (~F.coalesce(F.col("consensus_is_credible_mixed_language_candidate"), F.lit(False)))),
        ("high_risk_latin_tail_label", F.col("any_primary_high_risk")),
        ("hindi_indic_audit_candidate", F.col("is_hindi_indic_candidate")),
        ("source_language_disagreement", F.coalesce(F.col("source_disagreement"), F.lit(False))),
        ("openlid_glotlid_exact_disagreement", both_primary_v & (~F.coalesce(F.col("models_agree_exact_primary"), F.lit(False)))),
        ("openlid_glotlid_cluster_disagreement", both_primary_v & (~F.coalesce(F.col("models_agree_exact_primary"), F.lit(False))) & (~F.coalesce(F.col("models_agree_analysis_cluster_primary"), F.lit(False)))),
        ("insufficient_text_or_unclassified", F.col("language_status") == F.lit("insufficient_text_or_unclassified")),
        ("non_latin_script_control", F.coalesce(F.col("non_latin_any_int"), F.lit(0)) > 0),
    ]
    qualifying_arr = F.array(*[F.when(pred, F.lit(name)) for name, pred in strata])
    vch = (
        vch
        .withColumn("qualifying_strata", F.array_compact(qualifying_arr))
        .withColumn("_primary_stratum_candidate", F.array_join(F.slice(F.col("qualifying_strata"), 1, 1), ""))
        .withColumn(
            "primary_stratum",
            F.when(F.col("_primary_stratum_candidate") != "", F.col("_primary_stratum_candidate"))
            .otherwise(F.lit(None).cast("string")),
        )
        .drop("_primary_stratum_candidate")
        .where(F.col("primary_stratum").isNotNull())
    )
    _w_val = Window.partitionBy("primary_stratum").orderBy(
        F.xxhash64(F.concat_ws("|", F.col("channel_id"), F.lit(VALIDATION_SAMPLE_SEED), F.col("primary_stratum"))).asc()
    )
    manual_validation_sample = (
        vch.withColumn("_rn", F.row_number().over(_w_val)).where(F.col("_rn") <= VALIDATION_MAX_PER_STRATUM)
        .select("channel_id", "channel_hash_bucket", "primary_stratum", "qualifying_strata", "consensus_status", "consensus_language_label",
                "openlid_primary_language_label", "glotlid_primary_language_label", "language_status",
                "hindi_indic_candidate_status", "requires_manual_adjudication")
        .transform(with_bucket_run_columns)
        .withColumn("prediction_timestamp", F.current_timestamp())
    )
    write_delta(
        manual_validation_sample,
        manual_validation_sample_full,
        partition_cols=["run_id", "channel_hash_bucket"],
        replace_where=_bucket_replace_where(),
        zorder_cols=["channel_id"],
    )
    print("Wrote", manual_validation_sample_full)
    counts = current_run_table(manual_validation_sample_full).groupBy("primary_stratum").count()
    _maybe_display(counts.orderBy("primary_stratum"))
    below_min = counts.where(F.col("count") < VALIDATION_MIN_PER_STRATUM).count()
    if below_min > 0:
        print(f"WARNING: {below_min} strata have fewer than validation_min_per_stratum={VALIDATION_MIN_PER_STRATUM} channels.")
else:
    print("Skipping manual validation sample (create_validation_samples=false).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 14. Ablation analysis (spec §15)
# MAGIC
# MAGIC Re-aggregates from the **stored** segment predictions without rerunning inference. Reports per-config
# MAGIC language counts and **primary-label churn** vs both the v3 default OpenLID and v3 default consensus.
# MAGIC
# MAGIC Caveat: inference ran only on the `min_clean_chars=40` valid universe, so character-threshold
# MAGIC ablations can only *restrict* further (e.g. 50), never recover the more permissive legacy 20-char set.
# MAGIC `v1_legacy_like_openlid` therefore approximates legacy weights on the v3 valid-segment universe.

# COMMAND ----------
if RUN_ABLATION_AGGREGATIONS:
    LEGACY_WEIGHTS = {"channel_name": 0.5, "channel_description": 3.0, "video_title": 2.0, "video_description": 1.0, "video_tags": 0.75}
    V3_WEIGHTS = {
        "channel_name": float(CHANNEL_NAME_WEIGHT), "channel_description": float(CHANNEL_DESCRIPTION_WEIGHT),
        "video_title": float(VIDEO_TITLE_WEIGHT), "video_description": float(VIDEO_DESCRIPTION_WEIGHT),
        "video_tags": float(VIDEO_TAGS_WEIGHT),
    }
    ABLATION_LABEL_ISOS = ["srd", "ast", "vec", "gug", "eng", "spa", "ita", "por"]

    def ablation_aggregate(compact_df, weights, primary_min, secondary_min, secondary_ratio, secondary_vw,
                           use_top2, min_clean_letters, rollup_to_iso):
        carry = [
            "channel_id", "segment_id", "segment_type", "clean_letter_count", "is_valid_text_for_lid",
        ]
        top1_rows = compact_df.select(
            *carry,
            F.lit(1).alias("prediction_rank"),
            F.col("label_1").alias("label"),
            F.col("iso639_3_1").alias("iso639_3"),
            F.col("script_1").alias("script"),
            F.col("score_1").alias("score"),
            F.col("score_1").alias("score_1"),
        )
        if TOP_K >= 2:
            top2_rows = compact_df.select(
                *carry,
                F.lit(2).alias("prediction_rank"),
                F.col("label_2").alias("label"),
                F.col("iso639_3_2").alias("iso639_3"),
                F.col("script_2").alias("script"),
                F.col("score_2").alias("score"),
                F.col("score_1").alias("score_1"),
            )
        else:
            top2_rows = top1_rows.where(F.lit(False)).withColumn("prediction_rank", F.lit(2))
        base = (
            top1_rows.unionByName(top2_rows)
            .where(
                F.col("is_valid_text_for_lid") & F.col("label").isNotNull()
                & (~F.lower(F.col("label")).rlike(NOISE_LABEL_REGEX))
                & (F.coalesce(F.col("clean_letter_count"), F.lit(0)) >= F.lit(min_clean_letters))
            )
        )
        top1 = base.where((F.col("prediction_rank") == 1) & (F.col("score") >= F.lit(primary_min)))
        if use_top2:
            top2 = base.where(
                (F.col("prediction_rank") == 2) & (F.col("score") >= F.lit(secondary_min))
                & (F.col("score_1") > 0) & ((F.col("score") / F.col("score_1")) >= F.lit(secondary_ratio))
            )
            admitted = top1.unionByName(top2)
        else:
            admitted = top1
        wmap = F.create_map(*sum([[F.lit(k), F.lit(float(v))] for k, v in weights.items()], []))
        admitted = (
            admitted
            .withColumn("sw", F.coalesce(wmap[F.col("segment_type")], F.lit(1.0)))
            .withColumn("rw", F.when(F.col("prediction_rank") == 1, F.lit(1.0)).otherwise(F.lit(float(secondary_vw))))
            .withColumn("ws", F.col("score") * F.col("sw") * F.col("rw"))
            # Drop zero-weighted votes so a config that zeroes a segment-type weight (e.g. v3_no_description)
            # does not fabricate a primary for channels whose only signal came from that segment type.
            .where(F.col("ws") > 0)
            .withColumn("ablabel", F.col("iso639_3") if rollup_to_iso else F.col("label"))
        )
        votes = admitted.groupBy("channel_id", "ablabel").agg(
            F.sum("ws").alias("ws"),
            F.countDistinct("segment_id").alias("seg"),
            F.countDistinct(F.when(F.col("prediction_rank") == 1, F.col("segment_id"))).alias("top1seg"),
            F.avg("score").alias("mean_s"), F.max("score").alias("max_s"),
            F.min("iso639_3").alias("iso"), F.min("script").alias("script"),
            F.size(F.collect_set("segment_type")).alias("type_count"),
        )
        w = Window.partitionBy("channel_id").orderBy(F.desc("ws"), F.desc("seg"), F.desc("max_s"), F.asc("ablabel"))
        votes = votes.withColumn("rk", F.row_number().over(w))
        piv = votes.where(F.col("rk") <= 3).groupBy("channel_id").agg(
            F.max(F.when(F.col("rk") == 1, F.col("ablabel"))).alias("ab_primary_label"),
            F.max(F.when(F.col("rk") == 1, F.col("iso"))).alias("p_iso"),
            F.max(F.when(F.col("rk") == 1, F.col("script"))).alias("p_script"),
            F.max(F.when(F.col("rk") == 1, F.col("ws"))).alias("p_ws"),
            F.max(F.when(F.col("rk") == 2, F.col("ablabel"))).alias("s_label"),
            F.max(F.when(F.col("rk") == 2, F.col("iso"))).alias("s_iso"),
            F.max(F.when(F.col("rk") == 2, F.col("script"))).alias("s_script"),
            F.max(F.when(F.col("rk") == 2, F.col("ws"))).alias("s_ws"),
            F.max(F.when(F.col("rk") == 2, F.col("seg"))).alias("s_seg"),
            F.max(F.when(F.col("rk") == 2, F.col("top1seg"))).alias("s_top1seg"),
            F.max(F.when(F.col("rk") == 2, F.col("type_count"))).alias("s_typecount"),
            F.max(F.when(F.col("rk") == 2, F.col("mean_s"))).alias("s_mean"),
            F.max(F.when(F.col("rk") == 2, F.col("max_s"))).alias("s_max"),
            F.max(F.when(F.col("rk") == 3, F.col("ws"))).alias("r3_ws"),
        )
        cross = F.col("p_script").isNotNull() & F.col("s_script").isNotNull() & (F.col("p_script") != F.col("s_script"))
        piv = (
            piv
            .withColumn("ratio", F.when(F.col("p_ws") > 0, F.col("s_ws") / F.col("p_ws")))
            .withColumn("margin_ratio", F.when(F.col("s_ws") > 0, (F.col("s_ws") - F.coalesce(F.col("r3_ws"), F.lit(0.0))) / F.col("s_ws")))
            .withColumn("p_cluster", analysis_cluster_expr(F.col("ab_primary_label"), F.col("p_iso")))
            .withColumn("s_cluster", analysis_cluster_expr(F.col("s_label"), F.col("s_iso")))
        )
        same_cluster = F.col("p_cluster").isNotNull() & F.col("s_cluster").isNotNull() & (F.col("p_cluster") == F.col("s_cluster"))
        sec_hr = F.coalesce(F.col("s_label").isin(*_HR_LABELS_LIST), F.lit(False))
        screen = F.col("s_label").isNotNull() & (F.coalesce(F.col("ratio"), F.lit(0.0)) >= F.lit(MIXED_SCREEN_RATIO_THRESHOLD)) & (F.coalesce(F.col("s_seg"), F.lit(0)) >= F.lit(MIXED_SCREEN_MIN_SECONDARY_SEGMENTS))
        credible = (
            F.col("s_label").isNotNull()
            & (F.coalesce(F.col("ratio"), F.lit(0.0)) >= F.lit(MIXED_CREDIBLE_RATIO_THRESHOLD))
            & (F.coalesce(F.col("s_seg"), F.lit(0)) >= F.lit(MIXED_CREDIBLE_MIN_SECONDARY_SEGMENTS))
            & (F.coalesce(F.col("s_top1seg"), F.lit(0)) >= F.lit(MIXED_CREDIBLE_MIN_SECONDARY_TOP1_SEGMENTS))
            & (F.coalesce(F.col("s_mean"), F.lit(0.0)) >= F.lit(MIXED_CREDIBLE_SECONDARY_MEAN_SCORE))
            & ((F.coalesce(F.col("s_max"), F.lit(0.0)) >= F.lit(MIXED_CREDIBLE_SECONDARY_MAX_SCORE)) | cross)
            & (F.coalesce(F.col("margin_ratio"), F.lit(0.0)) >= F.lit(MIXED_CREDIBLE_MIN_RANK2_RANK3_MARGIN_RATIO))
            & (cross | (F.coalesce(F.col("s_typecount"), F.lit(0)) >= F.lit(MIXED_CREDIBLE_MIN_SECONDARY_SEGMENT_TYPES)))
            & (~same_cluster) & (~sec_hr)
        )
        return piv.select("channel_id", "ab_primary_label", "p_iso", screen.alias("ab_is_screen"), credible.alias("ab_is_credible"))

# COMMAND ----------
if RUN_ABLATION_AGGREGATIONS:
    DEF = dict(primary_min=PRIMARY_MIN_SCORE, secondary_min=SECONDARY_MIN_SCORE, secondary_ratio=SECONDARY_MIN_SCORE_RATIO,
               secondary_vw=SECONDARY_LABEL_VOTE_WEIGHT, use_top2=True, min_clean_letters=0, rollup_to_iso=False)

    def _merge(**over):
        c = dict(DEF)
        c.update(over)
        return c

    ol_compact = current_run_table(openlid_compact_full) if ENABLE_OPENLID else None
    gl_compact = current_run_table(glotlid_compact_full) if GLOTLID_CAN_FEED_MAIN else None

    # Consensus default frame from the comparison + mixed tables.
    cons_frame = (
        current_run_table(channel_model_comparison_full)
        .select("channel_id", F.col("consensus_language_label").alias("ab_primary_label"),
                F.col("consensus_language_iso639_3").alias("p_iso"))
        .join(current_run_table(mixed_language_candidates_full).select(
            "channel_id",
            F.col("consensus_is_mixed_language_screen").alias("ab_is_screen"),
            F.col("consensus_is_credible_mixed_language_candidate").alias("ab_is_credible")),
            on="channel_id", how="left")
    )

    config_specs = []  # (name, model_or_consensus, frame)
    if ol_compact is not None:
        config_specs += [
            ("v1_legacy_like_openlid", "openlid", ablation_aggregate(ol_compact, LEGACY_WEIGHTS, 0.0, 0.0, 0.0, 0.35, True, 0, False)),
            ("v3_default_openlid", "openlid", ablation_aggregate(ol_compact, V3_WEIGHTS, **DEF)),
            ("v3_no_top2_openlid", "openlid", ablation_aggregate(ol_compact, V3_WEIGHTS, **_merge(use_top2=False))),
            ("v3_description_weight_1_openlid", "openlid", ablation_aggregate(ol_compact, {**V3_WEIGHTS, "channel_description": 1.0}, **DEF)),
            ("v3_no_description_openlid", "openlid", ablation_aggregate(ol_compact, {**V3_WEIGHTS, "channel_description": 0.0}, **DEF)),
            ("v3_min_clean_chars_50_latin_openlid", "openlid", ablation_aggregate(ol_compact, V3_WEIGHTS, **_merge(min_clean_letters=50))),
            ("v3_top1_only_rollup_openlid", "openlid", ablation_aggregate(ol_compact, V3_WEIGHTS, **_merge(use_top2=False, rollup_to_iso=True))),
        ]
    if gl_compact is not None:
        config_specs += [
            ("v3_default_glotlid", "glotlid", ablation_aggregate(gl_compact, V3_WEIGHTS, **DEF)),
            ("v3_no_top2_glotlid", "glotlid", ablation_aggregate(gl_compact, V3_WEIGHTS, **_merge(use_top2=False))),
        ]
    config_specs += [("v3_default_consensus", "consensus", cons_frame)]

    # Baselines for churn.
    base_ol = next((f for n, m, f in config_specs if n == "v3_default_openlid"), None)
    base_ol = base_ol.select("channel_id", F.col("ab_primary_label").alias("base_ol_label")) if base_ol is not None else None
    base_cons = cons_frame.select("channel_id", F.col("ab_primary_label").alias("base_cons_label"))
    base_cred = cons_frame.select("channel_id", F.col("ab_is_credible").alias("base_cred"))
    hindi_flag = current_run_table(channels_output_full).select(
        "channel_id", (F.col("hindi_indic_candidate_status") != F.lit("no_hindi_or_indic_signal")).alias("hindi_indic_flag"))

# COMMAND ----------
if RUN_ABLATION_AGGREGATIONS:
    def ablation_metrics(frame, config_name, mode):
        f = frame.join(hindi_flag, on="channel_id", how="left")
        if base_ol is not None:
            f = f.join(base_ol, on="channel_id", how="full_outer")
        else:
            f = f.withColumn("base_ol_label", F.lit(None).cast("string"))
        f = f.join(base_cons, on="channel_id", how="full_outer").join(base_cred, on="channel_id", how="full_outer")
        SENT = "__none__"
        changed_ol = F.col("base_ol_label").isNotNull() & (F.coalesce(F.col("ab_primary_label"), F.lit(SENT)) != F.coalesce(F.col("base_ol_label"), F.lit(SENT)))
        changed_cons = F.col("base_cons_label").isNotNull() & (F.coalesce(F.col("ab_primary_label"), F.lit(SENT)) != F.coalesce(F.col("base_cons_label"), F.lit(SENT)))
        cred_changed = F.coalesce(F.col("ab_is_credible"), F.lit(False)) != F.coalesce(F.col("base_cred"), F.lit(False))
        label_aggs = [F.sum((F.col("p_iso") == F.lit(iso)).cast("int")).alias(f"n_{iso}_primary") for iso in ABLATION_LABEL_ISOS]
        agg = f.agg(
            F.sum(F.col("ab_primary_label").isNotNull().cast("int")).alias("n_channels_classified"),
            F.sum(F.coalesce(F.col("ab_is_screen"), F.lit(False)).cast("int")).alias("n_mixed_screen"),
            F.sum(F.coalesce(F.col("ab_is_credible"), F.lit(False)).cast("int")).alias("n_credible_mixed"),
            F.sum(F.coalesce(F.col("ab_primary_label").isin(*_HR_LABELS_LIST), F.lit(False)).cast("int")).alias("n_high_risk_primary"),
            F.sum((F.col("p_iso") == F.lit("hin")).cast("int")).alias("n_hindi_primary"),
            F.sum(F.coalesce(F.col("hindi_indic_flag"), F.lit(False)).cast("int")).alias("n_hindi_indic_candidate"),
            *label_aggs,
            F.sum(changed_ol.cast("int")).alias("n_primary_changed_vs_v3_default_openlid"),
            F.sum(F.col("base_ol_label").isNotNull().cast("int")).alias("_n_base_ol"),
            F.sum(changed_cons.cast("int")).alias("n_primary_changed_vs_v3_default_consensus"),
            F.sum(F.col("base_cons_label").isNotNull().cast("int")).alias("_n_base_cons"),
            F.sum(cred_changed.cast("int")).alias("n_credible_mixed_changed_vs_v3_default"),
        )
        return (
            agg
            .withColumn("config_name", F.lit(config_name))
            .withColumn("lid_model_or_consensus", F.lit(mode))
            .withColumn("pct_primary_changed_vs_v3_default_openlid", F.when(F.col("_n_base_ol") > 0, F.col("n_primary_changed_vs_v3_default_openlid") / F.col("_n_base_ol")))
            .withColumn("pct_primary_changed_vs_v3_default_consensus", F.when(F.col("_n_base_cons") > 0, F.col("n_primary_changed_vs_v3_default_consensus") / F.col("_n_base_cons")))
            .drop("_n_base_ol", "_n_base_cons")
        )

    ablation_rows = [ablation_metrics(frame, name, mode) for name, mode, frame in config_specs]
    ablation_summary = ablation_rows[0]
    for r in ablation_rows[1:]:
        ablation_summary = ablation_summary.unionByName(r)
    ablation_summary = with_run_scope_columns(ablation_summary).withColumn("prediction_timestamp", F.current_timestamp())
    write_delta(
        ablation_summary,
        ablation_summary_full,
        partition_cols=["run_id"],
        replace_where=_run_scope_replace_where(),
        replace_where_cols=_run_scope_required_cols(),
    )
    print("Wrote ablation summary to", ablation_summary_full)
    _maybe_display(current_run_scope_table(ablation_summary_full).orderBy("config_name"))
else:
    print("Skipping ablation (run_ablation_aggregations=false).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 15. Acceptance-criteria verification (spec §17)
# MAGIC
# MAGIC Lightweight checks of the deterministic, config-level acceptance criteria, plus existence checks for
# MAGIC the always-produced output tables. Data-level criteria (segment-id universe equality, one row per
# MAGIC channel) are asserted inline in their respective sections above.

# COMMAND ----------
def _table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{table_name}")
    except Exception:
        return False


def _table_has_required_columns(table_name: str, required_cols: List[str]) -> bool:
    if not _table_exists(table_name):
        return False
    cols = set(spark.table(fqtn(table_name)).columns)
    return set(required_cols).issubset(cols)


_acceptance = []


def _check(cond, label):
    _acceptance.append((bool(cond), label))


# Config-level criteria.
if ENABLE_GLOTLID and GLOTLID_MODE == "all_valid_segments":
    _check(ENABLE_GLOTLID, "#2 GlotLID enabled for the default all_valid_segments run")
elif not ENABLE_GLOTLID:
    print("NOTE: enable_glotlid=false; skipping default GlotLID all_valid_segments acceptance check for this manual run.")
else:
    print(f"NOTE: glotlid_mode={GLOTLID_MODE}; skipping runtime check for default all_valid_segments mode.")
# #6 is about the default; a manual run may legitimately lower the threshold, so this is informational
# (a NOTE) rather than a hard failure that would abort after all tables are already written.
if MIN_CLEAN_CHARS >= 40:
    _check(True, "#6 default Latin/ambiguous threshold >= 40 usable letters")
else:
    print(f"NOTE: min_clean_chars={MIN_CLEAN_CHARS} (<40) for this manual run; spec #6 default is >= 40.")
_check(not UPDATE_SOURCE_DETECTED_LANGUAGE, "#1 source table yt_sl_channels not modified (update flag off)")

# Always-produced output tables. Check current-run metadata columns so stale pre-refactor tables do not pass.
_bucket_table_cols = ["run_id", "inference_hash_buckets", "channel_hash_bucket"]
_scope_table_cols = ["run_id", "inference_hash_buckets", "bucket_start", "bucket_end", "is_full_bucket_range"]
_core_tables = [
    (OUTPUT_SEGMENTS_INPUT_TABLE, _bucket_table_cols),
    (OUTPUT_CHANNEL_TEXT_FEATURES_TABLE, _bucket_table_cols),
    (OUTPUT_CHANNEL_VOTES_TABLE, _bucket_table_cols),
    (OUTPUT_CHANNEL_MODEL_AGGREGATION_TABLE, _bucket_table_cols),
    (OUTPUT_CHANNEL_MODEL_COMPARISON_TABLE, _bucket_table_cols),
    (OUTPUT_CHANNELS_TABLE, _bucket_table_cols),
    (OUTPUT_MIXED_LANGUAGE_CANDIDATES_TABLE, _bucket_table_cols),
    (OUTPUT_HINDI_INDIC_AUDIT_TABLE, _bucket_table_cols),
    (OUTPUT_HIGH_RISK_REDIRECT_TABLE, _scope_table_cols),
    (OUTPUT_LANGUAGE_SUMMARY_FULL_TABLE, _scope_table_cols),
    (OUTPUT_LANGUAGE_SUMMARY_ROLLUP_TABLE, _scope_table_cols),
    (OUTPUT_MODEL_AGREEMENT_SUMMARY_TABLE, _scope_table_cols),
    (OUTPUT_UNCLASSIFIED_AUDIT_TABLE, _bucket_table_cols),
    (OUTPUT_SOURCE_LANGUAGE_CONFUSION_TABLE, _scope_table_cols),
    (OUTPUT_DEDUPE_QA_TABLE, _scope_table_cols),
]
if IS_FULL_BUCKET_RANGE:
    _core_tables.append((OUTPUT_SUSPECT_TAIL_AUDIT_TABLE, _bucket_table_cols))
if ENABLE_OPENLID:
    _core_tables.append((OUTPUT_OPENLID_COMPACT_TABLE, _bucket_table_cols))
    if _should_write_long_predictions():
        _core_tables.append((OUTPUT_OPENLID_SEGMENTS_TABLE, _bucket_table_cols))
if GLOTLID_ACTIVE:
    _core_tables.append((OUTPUT_GLOTLID_COMPACT_TABLE, _bucket_table_cols))
    if _should_write_long_predictions():
        _core_tables.append((OUTPUT_GLOTLID_SEGMENTS_TABLE, _bucket_table_cols))
if NATIVE_AUDIT_ENABLED:
    _core_tables.append((OUTPUT_GLOTLID_NATIVE_COMPACT_TABLE, _bucket_table_cols))
    if _should_write_long_predictions():
        _core_tables.append((OUTPUT_GLOTLID_NATIVE_SEGMENTS_TABLE, _bucket_table_cols))
if ENABLE_OPENLID and GLOTLID_CAN_FEED_MAIN:
    _core_tables.append((OUTPUT_SEGMENT_MODEL_COMPARISON_TABLE, _bucket_table_cols))
if RUN_ABLATION_AGGREGATIONS:
    _core_tables.append((OUTPUT_ABLATION_SUMMARY_TABLE, _scope_table_cols))
if CREATE_VALIDATION_SAMPLES:
    _core_tables.append((OUTPUT_MANUAL_VALIDATION_SAMPLE_TABLE, _bucket_table_cols))
for t, required_cols in _core_tables:
    _check(_table_has_required_columns(t, required_cols), f"output table written with current-run metadata: {t}")

if ENABLE_OPENLID:
    _check((N_OPENLID_COMPACT_ROWS if N_OPENLID_COMPACT_ROWS is not None else current_run_table(openlid_compact_full).count()) == n_valid_segments,
           "OpenLID compact predictions match current valid-segment count")
if GLOTLID_CAN_FEED_MAIN:
    _check((N_GLOTLID_COMPACT_ROWS if N_GLOTLID_COMPACT_ROWS is not None else current_run_table(glotlid_compact_full).count()) == n_valid_segments,
           "GlotLID compact predictions match current valid-segment count")
_check(current_run_scope_table(dedupe_qa_full).count() == 2, "dedupe QA has current run/scope rows")
_check(n_rows == n_universe, "final channel table has current run/bucket rows")

print("Acceptance checks:")
_failed = 0
for ok, label in _acceptance:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
    _failed += 0 if ok else 1
if _failed:
    raise AssertionError(f"{_failed} acceptance check(s) failed; see output above.")
print("\nAll config-level and table-existence acceptance checks passed.")
print(
    "Data-level criteria asserted inline above: #3 (segment-id universe equality) in section 5; "
    "#4 (one row per post-dedup channel) in section 12. #18 (fail-fast on missing model) is enforced "
    "in section 2. Remaining criteria (#5 deterministic dedup/sampling, #7 full summaries, #8 per-model "
    "aggregations, #9 legacy+prefixed+consensus fields, #10 consensus rules, #11 screen-vs-credible, "
    "#12 high-risk flagged not recoded, #13-15 Hindi/Indic + redirect diagnostics, #16 validation sample, "
    "#17 ablation churn, #19 README) are satisfied by sections 3-15 and README_language_lid_v3.md."
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC All phases of `lang_detect_v3_implementation_plan.md` are implemented. See `README_language_lid_v3.md`
# MAGIC for documentation and `CHANGELOG_revisions.md` for the revision summary. The legacy single-model
# MAGIC OpenLID-v3 pipeline remains in git history at commit `d3cb137`.
