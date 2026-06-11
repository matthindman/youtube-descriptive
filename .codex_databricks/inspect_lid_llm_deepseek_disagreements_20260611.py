# Databricks notebook source
import json
import os

from pyspark.sql import Window
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
_create_text_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
_create_text_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
_create_text_widget("segments_input_table", "yt_lid_v3_too_full_20260609_segments_input")
_create_text_widget("comparison_table", "yt_lid_v3_too_full_20260609_channel_model_comparison")
_create_text_widget("lid_channel_model_aggregation_table", "yt_lid_v3_too_full_20260609_channel_model_aggregation")
_create_text_widget("original_run_id", "too_full_20260609")
_create_text_widget("gemini_run_id", "too_full_20260609_retry_incomplete_20260611")
_create_text_widget("deepseek_run_id", "too_full_20260609_deepseek_nothinking_20260611")
_create_text_widget("source_run_id", "too_full_20260609")
_create_text_widget("inference_hash_buckets", "4096")
_create_text_widget("exclude_providers", "openai")
_create_text_widget("max_segments_per_channel", "28")
_create_text_widget("max_segment_chars", "650")
_create_text_widget("prompt_chars", "12000")
_create_text_widget("output_path", "/dbfs/FileStore/youtube_lid_panel_batches/analysis/deepseek_disagreements_20260611.json")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
REQUESTS_TABLE = _get_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
SEGMENTS_INPUT_TABLE = _get_widget("segments_input_table", "yt_lid_v3_too_full_20260609_segments_input")
COMPARISON_TABLE = _get_widget("comparison_table", "yt_lid_v3_too_full_20260609_channel_model_comparison")
LID_CHANNEL_MODEL_AGGREGATION_TABLE = _get_widget(
    "lid_channel_model_aggregation_table",
    "yt_lid_v3_too_full_20260609_channel_model_aggregation",
)
ORIGINAL_RUN_ID = _get_widget("original_run_id", "too_full_20260609")
GEMINI_RUN_ID = _get_widget("gemini_run_id", "too_full_20260609_retry_incomplete_20260611")
DEEPSEEK_RUN_ID = _get_widget("deepseek_run_id", "too_full_20260609_deepseek_nothinking_20260611")
SOURCE_RUN_ID = _get_widget("source_run_id", "too_full_20260609")
INFERENCE_HASH_BUCKETS = int(_get_widget("inference_hash_buckets", "4096"))
EXCLUDE_PROVIDERS = {p.strip().lower() for p in _get_widget("exclude_providers", "openai").split(",") if p.strip()}
MAX_SEGMENTS_PER_CHANNEL = int(_get_widget("max_segments_per_channel", "28"))
MAX_SEGMENT_CHARS = int(_get_widget("max_segment_chars", "650"))
PROMPT_CHARS = int(_get_widget("prompt_chars", "12000"))
OUTPUT_PATH = _get_widget("output_path", "/dbfs/FileStore/youtube_lid_panel_batches/analysis/deepseek_disagreements_20260611.json")

ARABIC_FAMILY_ISO = {"ara", "arb", "ary", "arz", "apc", "ars", "ajp", "aeb", "acm", "acq", "aec", "afb", "ayl", "ayn"}
CANONICAL_BASE_ISO = {
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


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def rows_to_dicts(df):
    return [row.asDict(recursive=True) for row in df.collect()]


def canonical_base_iso_expr(col):
    iso = F.lower(F.trim(col.cast("string")))
    iso = F.when(iso.isin("", "null", "none"), F.lit(None).cast("string")).otherwise(iso)
    iso = F.when(iso.isin(*sorted(ARABIC_FAMILY_ISO)), F.lit("ara")).otherwise(iso)
    mapping = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in CANONICAL_BASE_ISO.items()], []))
    return F.coalesce(F.element_at(mapping, iso), iso)


raw = spark.table(fqtn(RAW_RESULTS_TABLE))
deepseek_sample = (
    raw.where((F.col("run_id") == F.lit(DEEPSEEK_RUN_ID)) & (F.lower(F.col("provider")) == F.lit("deepseek")))
    .select("channel_id")
    .distinct()
)

selected_llm_raw = raw.where(
    (
        (F.col("run_id") == F.lit(DEEPSEEK_RUN_ID))
        & (F.lower(F.col("provider")) == F.lit("deepseek"))
    )
    | (
        (F.col("run_id") == F.lit(GEMINI_RUN_ID))
        & (F.lower(F.col("provider")) == F.lit("gemini"))
    )
    | (
        (F.col("run_id") == F.lit(ORIGINAL_RUN_ID))
        & (~F.lower(F.col("provider")).isin("deepseek", "gemini"))
    )
)
if EXCLUDE_PROVIDERS:
    selected_llm_raw = selected_llm_raw.where(~F.lower(F.col("provider")).isin(*sorted(EXCLUDE_PROVIDERS)))

llm_votes = (
    selected_llm_raw.join(deepseek_sample, on="channel_id", how="inner")
    .where(F.col("is_valid_panel_vote") == F.lit(True))
    .select(
        "channel_id",
        F.lower(F.col("provider")).alias("provider"),
        "model",
        "model_tier",
        F.col("primary_language_label").alias("language_label"),
        F.lower(F.col("pred_base_iso")).alias("base_iso"),
        "primary_language_script",
        "secondary_language_label",
        "dialect_or_variant",
        "is_mixed_language",
        "mixed_languages",
        "is_romanized",
        "confidence",
        "evidence",
    )
    .where(F.col("base_iso").isNotNull())
)

lid_tbl = spark.table(fqtn(LID_CHANNEL_MODEL_AGGREGATION_TABLE))
lid_cols = set(lid_tbl.columns)
if "inference_hash_buckets" in lid_cols:
    lid_tbl = lid_tbl.where(F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
lid_confidence_expr = (
    F.col("confidence_bucket").cast("string")
    if "confidence_bucket" in lid_cols
    else F.col("primary_language_confidence").cast("string")
    if "primary_language_confidence" in lid_cols
    else F.lit(None).cast("string")
)

lid_votes = (
    lid_tbl.where(F.col("run_id") == F.lit(SOURCE_RUN_ID))
    .where(F.col("lid_model").isin("openlid-v3", "glotlid"))
    .join(deepseek_sample, on="channel_id", how="inner")
    .select(
        "channel_id",
        F.lit("lid").alias("provider"),
        F.col("lid_model").alias("model"),
        F.lit("deterministic-lid").alias("model_tier"),
        F.col("primary_language_label").alias("language_label"),
        F.lower(F.col("primary_language_iso639_3")).alias("base_iso"),
        F.col("primary_language_script").alias("primary_language_script"),
        F.lit(None).cast("string").alias("secondary_language_label"),
        F.lit(None).cast("string").alias("dialect_or_variant"),
        F.lit(None).cast("boolean").alias("is_mixed_language"),
        F.from_json(F.lit("[]"), "array<string>").alias("mixed_languages"),
        F.lit(None).cast("boolean").alias("is_romanized"),
        lid_confidence_expr.alias("confidence"),
        F.lit(None).cast("string").alias("evidence"),
    )
    .where(F.col("base_iso").isNotNull())
)

votes = (
    llm_votes.unionByName(lid_votes)
    .withColumn("normalized_base_iso", canonical_base_iso_expr(F.col("base_iso")))
    .withColumn("model_key", F.concat_ws(":", F.col("provider"), F.col("model")))
)

other_votes = votes.where(F.col("provider") != F.lit("deepseek"))
other_counts = (
    other_votes.groupBy("channel_id", "normalized_base_iso")
    .agg(
        F.countDistinct("model_key").alias("n_votes"),
        F.concat_ws(", ", F.sort_array(F.collect_set("model"))).alias("models"),
    )
)
dist = (
    other_counts.groupBy("channel_id")
    .agg(
        F.sum("n_votes").alias("n_other_votes"),
        F.sort_array(
            F.collect_list(F.struct(F.col("n_votes"), F.col("normalized_base_iso"), F.col("models"))),
            asc=False,
        ).alias("other_vote_distribution"),
    )
    .withColumn("other_majority_votes", F.expr("get(other_vote_distribution, 0).n_votes"))
    .withColumn("other_majority_iso", F.expr("get(other_vote_distribution, 0).normalized_base_iso"))
    .withColumn("other_second_votes", F.coalesce(F.expr("get(other_vote_distribution, 1).n_votes"), F.lit(0)))
    .withColumn("other_has_strict_majority", F.col("other_majority_votes") > (F.col("n_other_votes") / F.lit(2.0)))
)

deepseek_votes = (
    votes.where(F.col("provider") == F.lit("deepseek"))
    .select(
        "channel_id",
        "model",
        F.col("language_label").alias("deepseek_language_label"),
        F.col("base_iso").alias("deepseek_base_iso"),
        F.col("normalized_base_iso").alias("deepseek_normalized_base_iso"),
        "primary_language_script",
        "secondary_language_label",
        "dialect_or_variant",
        "is_mixed_language",
        "mixed_languages",
        "is_romanized",
        "confidence",
        "evidence",
    )
)

deepseek_case_flags = (
    deepseek_votes.join(dist, on="channel_id", how="inner")
    .withColumn("deepseek_outside_other_majority", F.col("deepseek_normalized_base_iso") != F.col("other_majority_iso"))
)
case_summary = (
    deepseek_case_flags.groupBy("channel_id", "n_other_votes", "other_majority_iso", "other_majority_votes", "other_second_votes", "other_has_strict_majority", "other_vote_distribution")
    .agg(
        F.countDistinct("model").alias("n_deepseek_votes"),
        F.sum(F.when(F.col("deepseek_outside_other_majority"), 1).otherwise(0)).alias("n_deepseek_outside_other_majority"),
        F.countDistinct("deepseek_normalized_base_iso").alias("n_distinct_deepseek_iso"),
        F.collect_list(F.struct(
            "model",
            "deepseek_language_label",
            "deepseek_base_iso",
            "deepseek_normalized_base_iso",
            "primary_language_script",
            "secondary_language_label",
            "dialect_or_variant",
            "is_mixed_language",
            "mixed_languages",
            "is_romanized",
            "confidence",
            "deepseek_outside_other_majority",
            "evidence",
        )).alias("deepseek_votes"),
    )
    .where((F.col("other_has_strict_majority") == F.lit(True)) & (F.col("n_deepseek_outside_other_majority") > F.lit(0)))
    .withColumn(
        "deepseek_disagreement_pattern",
        F.when(F.col("n_deepseek_outside_other_majority") == F.col("n_deepseek_votes"), F.lit("both_or_all_deepseek_outside_other_majority"))
        .when(F.col("n_distinct_deepseek_iso") > 1, F.lit("deepseek_models_split_one_outside_other_majority"))
        .otherwise(F.lit("one_deepseek_outside_other_majority")),
    )
    .persist()
)

case_ids = case_summary.select("channel_id").distinct()

votes_for_cases = (
    votes.join(case_ids, on="channel_id", how="inner")
    .join(dist.select("channel_id", "other_majority_iso"), on="channel_id", how="left")
    .withColumn("outside_other_majority", F.col("normalized_base_iso") != F.col("other_majority_iso"))
)
votes_agg = votes_for_cases.groupBy("channel_id").agg(
    F.collect_list(F.struct(
        "provider",
        "model",
        "model_tier",
        "language_label",
        "base_iso",
        "normalized_base_iso",
        "primary_language_script",
        "secondary_language_label",
        "dialect_or_variant",
        "is_mixed_language",
        "mixed_languages",
        "is_romanized",
        "confidence",
        "outside_other_majority",
        "evidence",
    )).alias("model_votes")
)

request_w = Window.partitionBy("channel_id").orderBy(F.asc("model"))
prompts = (
    spark.table(fqtn(REQUESTS_TABLE))
    .where((F.col("run_id") == F.lit(DEEPSEEK_RUN_ID)) & (F.lower(F.col("provider")) == F.lit("deepseek")))
    .join(case_ids, on="channel_id", how="inner")
    .select("channel_id", "model", F.substring(F.col("prompt_user"), 1, PROMPT_CHARS).alias("prompt_user"))
    .withColumn("_rk", F.row_number().over(request_w))
    .where(F.col("_rk") == 1)
    .drop("_rk")
)

seg_tbl = spark.table(fqtn(SEGMENTS_INPUT_TABLE))
seg_cols = set(seg_tbl.columns)
seg_select = [
    "channel_id",
    (F.col("segment_id") if "segment_id" in seg_cols else F.sha2(F.concat_ws("||", F.col("channel_id"), F.col("segment_type"), F.col("text")), 256)).alias("segment_id"),
    "segment_type",
    F.substring(F.col("text").cast("string"), 1, MAX_SEGMENT_CHARS).alias("text"),
    F.coalesce(F.col("is_valid_text_for_lid"), F.lit(False)).alias("is_valid_text_for_lid"),
]
for col_name, alias, dtype in [
    ("short_text_reason", "short_text_reason", "string"),
    ("clean_letter_count", "clean_letter_count", "int"),
    ("clean_text_len", "clean_text_len", "int"),
    ("dominant_script", "dominant_script", "string"),
    ("dominant_script_share", "dominant_script_share", "double"),
]:
    if col_name in seg_cols:
        seg_select.append(F.col(col_name).cast(dtype).alias(alias))
    else:
        seg_select.append(F.lit(None).cast(dtype).alias(alias))

seg_rank = Window.partitionBy("channel_id").orderBy(
    F.desc(F.col("is_valid_text_for_lid").cast("int")),
    F.when(F.col("segment_type") == "video_title", 0)
     .when(F.col("segment_type").isin("channel_description", "video_description"), 1)
     .when(F.col("segment_type") == "channel_name", 2)
     .otherwise(3),
    F.desc(F.coalesce(F.col("clean_letter_count"), F.lit(0))),
    F.asc("segment_id"),
)
seg_scope = seg_tbl.where(F.col("run_id") == F.lit(SOURCE_RUN_ID))
if "inference_hash_buckets" in seg_cols:
    seg_scope = seg_scope.where(F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
segments = (
    seg_scope
    .join(case_ids, on="channel_id", how="inner")
    .select(*seg_select)
    .withColumn("_segment_rank", F.row_number().over(seg_rank))
    .where(F.col("_segment_rank") <= F.lit(MAX_SEGMENTS_PER_CHANNEL))
)
segments_agg = segments.groupBy("channel_id").agg(
    F.sort_array(F.collect_list(F.struct(
        "_segment_rank",
        "segment_type",
        "text",
        "is_valid_text_for_lid",
        "short_text_reason",
        "clean_letter_count",
        "clean_text_len",
        "dominant_script",
        "dominant_script_share",
    ))).alias("segments")
)
script_summary = (
    segments.groupBy("channel_id", "dominant_script")
    .agg(
        F.count(F.lit(1)).alias("n_segments"),
        F.sum(F.when(F.col("is_valid_text_for_lid"), 1).otherwise(0)).alias("n_valid_segments"),
    )
    .groupBy("channel_id")
    .agg(F.sort_array(F.collect_list(F.struct("dominant_script", "n_segments", "n_valid_segments"))).alias("segment_script_summary"))
)

cmp_tbl = spark.table(fqtn(COMPARISON_TABLE))
cmp_cols = set(cmp_tbl.columns)
cmp_select = [
    "channel_id",
    "consensus_status",
    "consensus_language_label",
    "openlid_primary_language_label",
    "glotlid_primary_language_label",
]
for col_name in [
    "openlid_primary_language_confidence",
    "glotlid_primary_language_confidence",
    "openlid_secondary_language_label",
    "glotlid_secondary_language_label",
]:
    if col_name in cmp_cols:
        cmp_select.append(col_name)
cmp_scope = cmp_tbl.where(F.col("run_id") == F.lit(SOURCE_RUN_ID))
if "inference_hash_buckets" in cmp_cols:
    cmp_scope = cmp_scope.where(F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
comparison = (
    cmp_scope
    .join(case_ids, on="channel_id", how="inner")
    .select(*cmp_select)
    .dropDuplicates(["channel_id"])
)

cases = (
    case_summary
    .join(votes_agg, on="channel_id", how="left")
    .join(prompts, on="channel_id", how="left")
    .join(segments_agg, on="channel_id", how="left")
    .join(script_summary, on="channel_id", how="left")
    .join(comparison, on="channel_id", how="left")
    .orderBy(F.desc("n_deepseek_outside_other_majority"), F.asc("other_majority_iso"), F.asc("channel_id"))
)

summary_by_pattern = rows_to_dicts(
    case_summary.groupBy("deepseek_disagreement_pattern")
    .agg(F.count(F.lit(1)).alias("n_channels"))
    .orderBy("deepseek_disagreement_pattern")
)
summary_by_majority_iso = rows_to_dicts(
    case_summary.groupBy("other_majority_iso")
    .agg(F.count(F.lit(1)).alias("n_channels"))
    .orderBy(F.desc("n_channels"), "other_majority_iso")
)

result = {
    "source": {
        "original_run_id": ORIGINAL_RUN_ID,
        "gemini_run_id": GEMINI_RUN_ID,
        "deepseek_run_id": DEEPSEEK_RUN_ID,
        "source_run_id": SOURCE_RUN_ID,
        "exclude_providers": sorted(EXCLUDE_PROVIDERS),
        "definition": "case where at least one DeepSeek normalized base ISO differs from a strict majority of non-DeepSeek models on the same 1,000-channel sample",
    },
    "n_cases": case_summary.count(),
    "summary_by_pattern": summary_by_pattern,
    "summary_by_other_majority_iso": summary_by_majority_iso,
    "cases": rows_to_dicts(cases),
}

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2, sort_keys=True)

dbutils.notebook.exit(json.dumps({
    "output_path": OUTPUT_PATH,
    "n_cases": result["n_cases"],
    "summary_by_pattern": summary_by_pattern,
    "summary_by_other_majority_iso": summary_by_majority_iso,
}, ensure_ascii=False, sort_keys=True))
