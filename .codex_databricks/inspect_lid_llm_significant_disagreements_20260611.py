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
_create_text_widget("original_run_id", "too_full_20260609")
_create_text_widget("retry_run_id", "too_full_20260609_retry_incomplete_20260611")
_create_text_widget("source_run_id", "too_full_20260609")
_create_text_widget("exclude_providers", "openai")
_create_text_widget("retry_providers", "gemini,deepseek")
_create_text_widget("min_valid_votes", "5")
_create_text_widget("min_dissenting_models", "2")
_create_text_widget("max_segments_per_channel", "24")
_create_text_widget("max_segment_chars", "500")
_create_text_widget("prompt_chars", "9000")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
REQUESTS_TABLE = _get_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
SEGMENTS_INPUT_TABLE = _get_widget("segments_input_table", "yt_lid_v3_too_full_20260609_segments_input")
COMPARISON_TABLE = _get_widget("comparison_table", "yt_lid_v3_too_full_20260609_channel_model_comparison")
ORIGINAL_RUN_ID = _get_widget("original_run_id", "too_full_20260609")
RETRY_RUN_ID = _get_widget("retry_run_id", "too_full_20260609_retry_incomplete_20260611")
SOURCE_RUN_ID = _get_widget("source_run_id", "too_full_20260609")
EXCLUDE_PROVIDERS = {p.strip().lower() for p in _get_widget("exclude_providers", "openai").split(",") if p.strip()}
RETRY_PROVIDERS = {p.strip().lower() for p in _get_widget("retry_providers", "gemini,deepseek").split(",") if p.strip()}
MIN_VALID_VOTES = int(_get_widget("min_valid_votes", "5"))
MIN_DISSENTING_MODELS = int(_get_widget("min_dissenting_models", "2"))
MAX_SEGMENTS_PER_CHANNEL = int(_get_widget("max_segments_per_channel", "24"))
MAX_SEGMENT_CHARS = int(_get_widget("max_segment_chars", "500"))
PROMPT_CHARS = int(_get_widget("prompt_chars", "9000"))


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


raw = spark.table(fqtn(RAW_RESULTS_TABLE))

selected = raw.where(
    (
        (F.col("run_id") == F.lit(RETRY_RUN_ID))
        & F.lower(F.col("provider")).isin(*sorted(RETRY_PROVIDERS))
    )
    | (
        (F.col("run_id") == F.lit(ORIGINAL_RUN_ID))
        & (~F.lower(F.col("provider")).isin(*sorted(RETRY_PROVIDERS | EXCLUDE_PROVIDERS)))
    )
)
if EXCLUDE_PROVIDERS:
    selected = selected.where(~F.lower(F.col("provider")).isin(*sorted(EXCLUDE_PROVIDERS)))

votes = (
    selected.where(F.col("is_valid_panel_vote") == F.lit(True))
    .select(
        "channel_id",
        F.lower(F.col("provider")).alias("provider"),
        "model",
        "model_tier",
        F.col("primary_language_label").alias("language_label"),
        F.lower(F.col("pred_base_iso")).alias("base_iso"),
        "primary_language_script",
        "secondary_language_label",
        "is_mixed_language",
        "is_romanized",
        "confidence",
        "evidence",
    )
    .where(F.col("base_iso").isNotNull())
    .withColumn("model_key", F.concat_ws(":", F.col("provider"), F.col("model")))
)

channel_vote_counts = votes.groupBy("channel_id").agg(F.countDistinct("model_key").alias("n_valid_votes"))

base_counts = votes.groupBy("channel_id", "base_iso").agg(F.countDistinct("model_key").alias("n"))
base_w = Window.partitionBy("channel_id").orderBy(F.desc("n"), F.asc("base_iso"))
base_majority = (
    base_counts.withColumn("_rk", F.row_number().over(base_w))
    .where(F.col("_rk") == 1)
    .select("channel_id", F.col("base_iso").alias("majority_base_iso"), F.col("n").alias("majority_base_votes"))
)

label_counts = votes.groupBy("channel_id", "language_label").agg(F.countDistinct("model_key").alias("n"))
label_w = Window.partitionBy("channel_id").orderBy(F.desc("n"), F.asc("language_label"))
label_majority = (
    label_counts.withColumn("_rk", F.row_number().over(label_w))
    .where(F.col("_rk") == 1)
    .select("channel_id", F.col("language_label").alias("majority_language_label"), F.col("n").alias("majority_label_votes"))
)

summary = (
    channel_vote_counts
    .join(base_majority, on="channel_id", how="inner")
    .join(label_majority, on="channel_id", how="inner")
    .withColumn("base_dissenting_models", F.col("n_valid_votes") - F.col("majority_base_votes"))
    .withColumn("label_dissenting_models", F.col("n_valid_votes") - F.col("majority_label_votes"))
)

significant = (
    summary.where(F.col("n_valid_votes") >= F.lit(MIN_VALID_VOTES))
    .where(
        (F.col("base_dissenting_models") >= F.lit(MIN_DISSENTING_MODELS))
        | (F.col("label_dissenting_models") >= F.lit(MIN_DISSENTING_MODELS))
    )
    .persist()
)

sig_ids = significant.select("channel_id").distinct()

votes_for_cases = (
    votes.join(significant.select("channel_id", "majority_base_iso", "majority_language_label"), on="channel_id", how="inner")
    .withColumn("outside_base_majority", F.col("base_iso") != F.col("majority_base_iso"))
    .withColumn("outside_label_majority", F.col("language_label") != F.col("majority_language_label"))
)

votes_agg = votes_for_cases.groupBy("channel_id").agg(
    F.sort_array(F.collect_list(F.struct(
        "provider",
        "model",
        "model_tier",
        "language_label",
        "base_iso",
        "primary_language_script",
        "secondary_language_label",
        "is_mixed_language",
        "is_romanized",
        "confidence",
        "outside_base_majority",
        "outside_label_majority",
        "evidence",
    ))).alias("model_votes")
)

requests = spark.table(fqtn(REQUESTS_TABLE))
prompt_w = Window.partitionBy("channel_id").orderBy(
    F.when(F.col("run_id") == F.lit(ORIGINAL_RUN_ID), 0).otherwise(1),
    F.asc("provider"),
    F.asc("model"),
)
prompts = (
    requests
    .where(F.col("run_id").isin(ORIGINAL_RUN_ID, RETRY_RUN_ID))
    .join(sig_ids, on="channel_id", how="inner")
    .select("channel_id", "run_id", "provider", "model", F.substring(F.col("prompt_user"), 1, PROMPT_CHARS).alias("prompt_user"))
    .withColumn("_rk", F.row_number().over(prompt_w))
    .where(F.col("_rk") == 1)
    .drop("_rk")
)

seg_tbl = spark.table(fqtn(SEGMENTS_INPUT_TABLE))
seg_cols = set(seg_tbl.columns)
seg_select = [
    "channel_id",
    "segment_id",
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

seg = (
    seg_tbl
    .where(F.col("run_id") == F.lit(SOURCE_RUN_ID))
    .join(sig_ids, on="channel_id", how="inner")
    .select(*seg_select)
    .withColumn(
        "_segment_rank",
        F.row_number().over(
            Window.partitionBy("channel_id").orderBy(
                F.desc(F.col("is_valid_text_for_lid").cast("int")),
                F.when(F.col("segment_type") == "video_title", 0)
                 .when(F.col("segment_type").isin("video_description", "channel_description"), 1)
                 .when(F.col("segment_type") == "channel_name", 2)
                 .otherwise(3),
                F.desc(F.coalesce(F.col("clean_letter_count"), F.lit(0))),
                F.asc("segment_id"),
            )
        )
    )
    .where(F.col("_segment_rank") <= F.lit(MAX_SEGMENTS_PER_CHANNEL))
    .drop("_segment_rank")
)

segments_agg = seg.groupBy("channel_id").agg(
    F.collect_list(F.struct(
        "segment_type",
        "text",
        "is_valid_text_for_lid",
        "short_text_reason",
        "clean_letter_count",
        "clean_text_len",
        "dominant_script",
        "dominant_script_share",
    )).alias("segments")
)

script_summary = (
    seg.groupBy("channel_id", "dominant_script")
    .agg(
        F.count(F.lit(1)).alias("n_segments"),
        F.sum(F.when(F.col("is_valid_text_for_lid"), 1).otherwise(0)).alias("n_valid_segments"),
    )
    .groupBy("channel_id")
    .agg(F.sort_array(F.collect_list(F.struct("dominant_script", "n_segments", "n_valid_segments"))).alias("segment_script_summary"))
)

cmp_cols = set(spark.table(fqtn(COMPARISON_TABLE)).columns)
cmp_select = [
    "channel_id",
    "consensus_status",
    "consensus_language_label",
    "openlid_primary_language_label",
    "glotlid_primary_language_label",
]
optional_cmp = [
    "openlid_primary_language_confidence",
    "glotlid_primary_language_confidence",
    "openlid_secondary_language_label",
    "glotlid_secondary_language_label",
]
cmp_df = spark.table(fqtn(COMPARISON_TABLE)).where(F.col("run_id") == F.lit(SOURCE_RUN_ID))
for col_name in optional_cmp:
    if col_name in cmp_cols:
        cmp_select.append(col_name)
cmp = cmp_df.join(sig_ids, on="channel_id", how="inner").select(*cmp_select).dropDuplicates(["channel_id"])

cases = (
    significant
    .join(votes_agg, on="channel_id", how="left")
    .join(prompts, on="channel_id", how="left")
    .join(segments_agg, on="channel_id", how="left")
    .join(script_summary, on="channel_id", how="left")
    .join(cmp, on="channel_id", how="left")
    .orderBy(F.desc("base_dissenting_models"), F.desc("label_dissenting_models"), "channel_id")
)

case_rows = [row.asDict(recursive=True) for row in cases.collect()]
overall = {
    "n_channels_with_valid_votes": channel_vote_counts.count(),
    "n_significant_cases": len(case_rows),
    "selection_rule": {
        "min_valid_votes": MIN_VALID_VOTES,
        "min_dissenting_models": MIN_DISSENTING_MODELS,
        "base_or_full_label": "base_dissenting_models >= threshold OR label_dissenting_models >= threshold",
    },
    "run_selection": {
        "original_run_id": ORIGINAL_RUN_ID,
        "retry_run_id": RETRY_RUN_ID,
        "retry_providers": sorted(RETRY_PROVIDERS),
        "exclude_providers": sorted(EXCLUDE_PROVIDERS),
    },
}

result = {"overall": overall, "cases": case_rows}
print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, sort_keys=True))
