# Databricks notebook source
import json
import os

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
_create_text_widget("lid_channel_model_aggregation_table", "yt_lid_v3_too_full_20260609_channel_model_aggregation")
_create_text_widget("original_run_id", "too_full_20260609")
_create_text_widget("gemini_run_id", "too_full_20260609_retry_incomplete_20260611")
_create_text_widget("deepseek_run_id", "too_full_20260609_deepseek_nothinking_20260611")
_create_text_widget("lid_run_id", "too_full_20260609")
_create_text_widget("exclude_providers", "openai")
_create_text_widget("min_shared_classified", "31")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
LID_CHANNEL_MODEL_AGGREGATION_TABLE = _get_widget(
    "lid_channel_model_aggregation_table",
    "yt_lid_v3_too_full_20260609_channel_model_aggregation",
)
ORIGINAL_RUN_ID = _get_widget("original_run_id", "too_full_20260609")
GEMINI_RUN_ID = _get_widget("gemini_run_id", "too_full_20260609_retry_incomplete_20260611")
DEEPSEEK_RUN_ID = _get_widget("deepseek_run_id", "too_full_20260609_deepseek_nothinking_20260611")
LID_RUN_ID = _get_widget("lid_run_id", "too_full_20260609")
EXCLUDE_PROVIDERS = {p.strip().lower() for p in _get_widget("exclude_providers", "openai").split(",") if p.strip()}
MIN_SHARED_CLASSIFIED = int(_get_widget("min_shared_classified", "31"))

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


def canonical_base_iso_expr(col):
    iso = F.lower(F.trim(col.cast("string")))
    iso = F.when(iso.isin("", "null", "none"), F.lit(None).cast("string")).otherwise(iso)
    iso = F.when(iso.isin(*sorted(ARABIC_FAMILY_ISO)), F.lit("ara")).otherwise(iso)
    mapping = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in CANONICAL_BASE_ISO.items()], []))
    return F.coalesce(F.element_at(mapping, iso), iso)


raw = spark.table(fqtn(RAW_RESULTS_TABLE))
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

sample_channels = (
    raw.where(F.col("run_id") == F.lit(DEEPSEEK_RUN_ID))
    .select("channel_id")
    .distinct()
)

llm_votes = (
    selected_llm_raw.join(sample_channels, on="channel_id", how="inner")
    .where(F.col("is_valid_panel_vote") == F.lit(True))
    .select(
        "channel_id",
        "provider",
        "model",
        "model_tier",
        F.lower(F.col("pred_base_iso")).alias("base_iso"),
        F.col("primary_language_label").alias("language_label"),
    )
    .where(F.col("base_iso").isNotNull())
)

lid_votes = (
    spark.table(fqtn(LID_CHANNEL_MODEL_AGGREGATION_TABLE))
    .where(F.col("run_id") == F.lit(LID_RUN_ID))
    .where(F.col("lid_model").isin("openlid-v3", "glotlid"))
    .join(sample_channels, on="channel_id", how="inner")
    .select(
        "channel_id",
        F.lit("lid").alias("provider"),
        F.col("lid_model").alias("model"),
        F.lit("deterministic-lid").alias("model_tier"),
        F.lower(F.col("primary_language_iso639_3")).alias("base_iso"),
        F.col("primary_language_label").alias("language_label"),
    )
    .where(F.col("base_iso").isNotNull())
)

votes = (
    llm_votes.unionByName(lid_votes)
    .withColumn("normalized_base_iso", canonical_base_iso_expr(F.col("base_iso")))
    .withColumn("model_key", F.concat_ws(":", F.col("provider"), F.col("model")))
)

model_counts = (
    votes.groupBy("provider", "model", "model_tier")
    .agg(F.countDistinct("channel_id").alias("n_valid_votes"))
    .orderBy("provider", "model")
)

a = votes.alias("a")
b = votes.alias("b")
pairwise = (
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
        F.sum(F.when(F.col("a.base_iso") == F.col("b.base_iso"), 1).otherwise(0)).alias("n_raw_base_iso_agree"),
        F.sum(F.when(F.col("a.normalized_base_iso") == F.col("b.normalized_base_iso"), 1).otherwise(0)).alias("n_normalized_base_iso_agree"),
    )
    .withColumn("raw_base_iso_agreement_rate", F.round(F.col("n_raw_base_iso_agree") / F.col("n_both_classified"), 4))
    .withColumn("normalized_base_iso_agreement_rate", F.round(F.col("n_normalized_base_iso_agree") / F.col("n_both_classified"), 4))
    .where(F.col("n_both_classified") >= F.lit(MIN_SHARED_CLASSIFIED))
    .orderBy("provider_a", "model_a", "provider_b", "model_b")
)

overall = (
    pairwise.agg(
        F.sum("n_both_classified").alias("pair_channel_comparisons"),
        F.sum("n_normalized_base_iso_agree").alias("normalized_base_iso_agree"),
    )
    .withColumn("normalized_base_iso_agreement_rate", F.round(F.col("normalized_base_iso_agree") / F.col("pair_channel_comparisons"), 4))
)

result = {
    "source": {
        "original_run_id": ORIGINAL_RUN_ID,
        "gemini_run_id": GEMINI_RUN_ID,
        "deepseek_run_id": DEEPSEEK_RUN_ID,
        "lid_run_id": LID_RUN_ID,
        "exclude_providers": sorted(EXCLUDE_PROVIDERS),
        "min_shared_classified": MIN_SHARED_CLASSIFIED,
        "normalization": {
            "aliases": CANONICAL_BASE_ISO,
            "arabic_family_to": "ara",
        },
    },
    "sample_channel_count": sample_channels.count(),
    "model_counts": [row.asDict(recursive=True) for row in model_counts.collect()],
    "overall": [row.asDict(recursive=True) for row in overall.collect()][0],
    "pairwise": [row.asDict(recursive=True) for row in pairwise.collect()],
}

dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, sort_keys=True))
