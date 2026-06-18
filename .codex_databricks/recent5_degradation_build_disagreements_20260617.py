# Databricks notebook source
"""Build recent-5 degradation comparison and route table for DeepSeek adjudication."""

from __future__ import annotations

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "dev_sean")
dbutils.widgets.text("schema", "matt")
dbutils.widgets.text("baseline_run_id", "too_full_20260609")
dbutils.widgets.text("recent5_run_id", "too_full_20260609_recent5_degradation_20260617")
dbutils.widgets.text("baseline_prefix", "yt_lid_v3_too_full_20260609")
dbutils.widgets.text("recent5_prefix", "yt_lid_v3_recent5_degradation_20260617")
dbutils.widgets.text("baseline_verdicts_table", "yt_lid_v3_too_full_20260609_llm_validation_verdicts")
dbutils.widgets.text("output_prefix", "yt_lid_v3_recent5_degradation_20260617")


CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BASELINE_RUN_ID = dbutils.widgets.get("baseline_run_id")
RECENT5_RUN_ID = dbutils.widgets.get("recent5_run_id")
BASELINE_PREFIX = dbutils.widgets.get("baseline_prefix")
RECENT5_PREFIX = dbutils.widgets.get("recent5_prefix")
BASELINE_VERDICTS_TABLE = dbutils.widgets.get("baseline_verdicts_table")
OUTPUT_PREFIX = dbutils.widgets.get("output_prefix")

ARABIC_MACRO_ISO = [
    "acm",
    "acq",
    "aeb",
    "afb",
    "ajp",
    "apc",
    "arb",
    "arq",
    "ars",
    "ary",
    "arz",
    "shu",
]

CANONICAL_ISO = {
    "ar": "ara",
    "ara": "ara",
    "zh": "cmn",
    "zho": "cmn",
    "chi": "cmn",
    "cmn": "cmn",
    "iw": "heb",
    "he": "heb",
    "in": "ind",
    "id": "ind",
    "jw": "jav",
    "jv": "jav",
    "ku": "kmr",
    "kur": "kmr",
    "ne": "npi",
    "nep": "npi",
    "or": "ory",
    "ori": "ory",
    "ms": "zsm",
    "msa": "zsm",
    "tgl": "fil",
    "tl": "fil",
    "uzn": "uzb",
}

DISAGREEMENT_STATUSES = [
    "model_disagreement_needs_review",
    "mixed_language_candidate",
    "manual_adjudication_required",
]

UNCLASSIFIED_OR_REVIEW_STATUSES = [
    "insufficient_text_or_unclassified",
    "insufficient_text",
    "needs_review",
    "model_disagreement_needs_review",
    "mixed_language_candidate",
    "manual_adjudication_required",
]


def fqtn(table_name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"


def col_or_null(df, name: str):
    return F.col(name) if name in df.columns else F.lit(None)


def first_existing_col(df, *names: str):
    for name in names:
        if name in df.columns:
            return F.col(name)
    return F.lit(None)


def canon_iso(col):
    iso = F.lower(F.trim(col.cast("string")))
    iso = F.when(iso.isin("", "null", "none", "und", "zxx", "mul", "mis", "inc"), F.lit(None)).otherwise(iso)
    iso = F.when(iso.isin(*ARABIC_MACRO_ISO), F.lit("ara")).otherwise(iso)
    mapping = F.create_map(*[x for item in CANONICAL_ISO.items() for x in (F.lit(item[0]), F.lit(item[1]))])
    iso = F.coalesce(F.element_at(mapping, iso), iso)
    return F.when(iso.rlike("^[a-z]{3}$"), iso)


def cmp_projection(df, prefix: str):
    return df.select(
        "channel_id",
        F.col("run_id").alias(f"{prefix}_run_id"),
        col_or_null(df, "consensus_status").alias(f"{prefix}_consensus_status"),
        col_or_null(df, "consensus_language_label").alias(f"{prefix}_consensus_language_label"),
        col_or_null(df, "consensus_language_iso639_3").alias(f"{prefix}_consensus_iso639_3"),
        canon_iso(col_or_null(df, "consensus_language_iso639_3")).alias(f"{prefix}_consensus_base_iso"),
        col_or_null(df, "openlid_primary_language_label").alias(f"{prefix}_openlid_label"),
        col_or_null(df, "openlid_primary_language_iso639_3").alias(f"{prefix}_openlid_iso639_3"),
        canon_iso(col_or_null(df, "openlid_primary_language_iso639_3")).alias(f"{prefix}_openlid_base_iso"),
        col_or_null(df, "glotlid_primary_language_label").alias(f"{prefix}_glotlid_label"),
        col_or_null(df, "glotlid_primary_language_iso639_3").alias(f"{prefix}_glotlid_iso639_3"),
        canon_iso(col_or_null(df, "glotlid_primary_language_iso639_3")).alias(f"{prefix}_glotlid_base_iso"),
        col_or_null(df, "channel_hash_bucket").alias(f"{prefix}_channel_hash_bucket"),
    )


def segment_counts(table_name: str, run_id: str, prefix: str, sample_channels):
    df = spark.table(fqtn(table_name)).where(F.col("run_id") == F.lit(run_id))
    df = df.join(F.broadcast(sample_channels), on="channel_id", how="inner")
    video_id_col = col_or_null(df, "video_id")
    text_col = col_or_null(df, "segment_text")
    return df.groupBy("channel_id").agg(
        F.count(F.lit(1)).alias(f"{prefix}_segment_rows"),
        F.sum(F.when(F.length(F.trim(text_col.cast("string"))) > 0, 1).otherwise(0)).alias(f"{prefix}_valid_text_segments"),
        F.countDistinct(video_id_col).alias(f"{prefix}_selected_video_count"),
    )


recent5_cmp = spark.table(fqtn(f"{RECENT5_PREFIX}_channel_model_comparison")).where(
    F.col("run_id") == F.lit(RECENT5_RUN_ID)
)
sample_channels = (
    spark.table(fqtn(f"{RECENT5_PREFIX}_sample_channels"))
    .select("channel_id")
    .where(F.col("channel_id").isNotNull())
    .distinct()
    .persist()
)
sample_count = sample_channels.count()
if sample_count != 1000:
    raise ValueError(f"Locked recent-5 sample should contain exactly 1,000 channels; found {sample_count}.")

baseline_cmp = (
    spark.table(fqtn(f"{BASELINE_PREFIX}_channel_model_comparison"))
    .where(F.col("run_id") == F.lit(BASELINE_RUN_ID))
    .join(F.broadcast(sample_channels), on="channel_id", how="inner")
)

baseline_segments = segment_counts(f"{BASELINE_PREFIX}_segments_input", BASELINE_RUN_ID, "baseline10", sample_channels)
recent5_segments = segment_counts(f"{RECENT5_PREFIX}_segments_input", RECENT5_RUN_ID, "recent5", sample_channels)

verdicts = (
    spark.table(fqtn(BASELINE_VERDICTS_TABLE))
    .where(F.col("run_id") == F.lit(BASELINE_RUN_ID))
    .join(F.broadcast(sample_channels), on="channel_id", how="inner")
)
llm_reference = verdicts.where(col_or_null(verdicts, "panel_language_iso639_3").isNotNull()).select(
    "channel_id",
    F.col("panel_status").alias("llm_panel_status"),
    col_or_null(verdicts, "panel_language_label").alias("llm_panel_language_label"),
    col_or_null(verdicts, "panel_language_iso639_3").alias("llm_panel_iso639_3"),
    col_or_null(verdicts, "panel_normalized_language_iso639_3").alias("llm_panel_normalized_iso639_3"),
    canon_iso(F.coalesce(col_or_null(verdicts, "panel_normalized_language_iso639_3"), col_or_null(verdicts, "panel_language_iso639_3"))).alias(
        "llm_panel_base_iso"
    ),
    col_or_null(verdicts, "panel_vote_margin").alias("llm_panel_vote_margin"),
    first_existing_col(verdicts, "panel_votes_for_winner", "n_votes").alias("llm_panel_votes_for_winner"),
    first_existing_col(verdicts, "panel_models_reached", "n_votes").alias("llm_panel_models_reached"),
)

analysis = (
    sample_channels
    .join(cmp_projection(baseline_cmp, "baseline10"), on="channel_id", how="left")
    .join(cmp_projection(recent5_cmp, "recent5"), on="channel_id", how="left")
    .join(llm_reference, on="channel_id", how="left")
    .join(baseline_segments, on="channel_id", how="left")
    .join(recent5_segments, on="channel_id", how="left")
)

recent5_lid_disagree = (
    F.col("recent5_consensus_status").isin(DISAGREEMENT_STATUSES)
    | (
        F.col("recent5_openlid_base_iso").isNotNull()
        & F.col("recent5_glotlid_base_iso").isNotNull()
        & (F.col("recent5_openlid_base_iso") != F.col("recent5_glotlid_base_iso"))
    )
)
recent5_vs_baseline_changed = (
    F.coalesce(F.col("recent5_consensus_base_iso"), F.lit("__NULL__"))
    != F.coalesce(F.col("baseline10_consensus_base_iso"), F.lit("__NULL__"))
) | (
    F.coalesce(F.col("recent5_consensus_status"), F.lit("__NULL__"))
    != F.coalesce(F.col("baseline10_consensus_status"), F.lit("__NULL__"))
)
recent5_vs_llm_changed = F.col("llm_panel_base_iso").isNotNull() & (
    F.coalesce(F.col("recent5_consensus_base_iso"), F.lit("__NULL__")) != F.col("llm_panel_base_iso")
)
recent5_newly_unclassified = (
    (F.col("recent5_consensus_base_iso").isNull() | F.col("recent5_consensus_status").isin(UNCLASSIFIED_OR_REVIEW_STATUSES))
    & (F.col("baseline10_consensus_base_iso").isNotNull() | F.col("llm_panel_base_iso").isNotNull())
)

analysis = (
    analysis.withColumn("recent5_internal_lid_disagreement", recent5_lid_disagree)
    .withColumn("recent5_vs_baseline_lid_change", recent5_vs_baseline_changed)
    .withColumn("recent5_vs_llm_reference_change", recent5_vs_llm_changed)
    .withColumn("recent5_newly_unclassified_or_review", recent5_newly_unclassified)
    .withColumn(
        "_degradation_disagreement_reasons_raw",
        F.array(
            F.when(F.col("recent5_internal_lid_disagreement"), F.lit("recent5_openlid_glotlid_disagreement")),
            F.when(F.col("recent5_vs_baseline_lid_change"), F.lit("recent5_vs_10video_lid_change")),
            F.when(F.col("recent5_vs_llm_reference_change"), F.lit("recent5_vs_llm_panel_change")),
            F.when(F.col("recent5_newly_unclassified_or_review"), F.lit("recent5_newly_unclassified_or_review")),
        ),
    )
    .withColumn("degradation_disagreement_reasons", F.expr("filter(_degradation_disagreement_reasons_raw, x -> x is not null)"))
    .drop("_degradation_disagreement_reasons_raw")
    .withColumn(
        "degradation_disagreement_reason",
        F.when(F.size(F.col("degradation_disagreement_reasons")) > 0, F.concat_ws("|", F.col("degradation_disagreement_reasons"))),
    )
    .withColumn("has_degradation_disagreement", F.size(F.col("degradation_disagreement_reasons")) > 0)
)

analysis_table = f"{OUTPUT_PREFIX}_channel_analysis"
disagreement_table = f"{OUTPUT_PREFIX}_disagreement_channels"
deepseek_input_table = f"{OUTPUT_PREFIX}_deepseek_comparison_input"

analysis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqtn(analysis_table))

disagreements = analysis.where(F.col("has_degradation_disagreement")).select(
    "channel_id",
    "degradation_disagreement_reason",
    "degradation_disagreement_reasons",
    "recent5_internal_lid_disagreement",
    "recent5_vs_baseline_lid_change",
    "recent5_vs_llm_reference_change",
    "recent5_newly_unclassified_or_review",
)
disagreements.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fqtn(disagreement_table)
)

route_defaults = analysis.select(
    "channel_id",
    F.coalesce(F.col("recent5_channel_hash_bucket"), F.col("baseline10_channel_hash_bucket")).alias(
        "_route_channel_hash_bucket"
    ),
)
deepseek_input = recent5_cmp.join(
    F.broadcast(disagreements.select("channel_id", "degradation_disagreement_reason")),
    on="channel_id",
    how="right",
).join(
    route_defaults,
    on="channel_id",
    how="left",
)
if "run_id" in deepseek_input.columns:
    deepseek_input = deepseek_input.withColumn("run_id", F.coalesce(F.col("run_id"), F.lit(RECENT5_RUN_ID)))
if "channel_hash_bucket" in deepseek_input.columns:
    deepseek_input = deepseek_input.withColumn(
        "channel_hash_bucket",
        F.coalesce(F.col("channel_hash_bucket"), F.col("_route_channel_hash_bucket")),
    )
deepseek_input = deepseek_input.drop("_route_channel_hash_bucket")
if "consensus_status" in deepseek_input.columns:
    deepseek_input = deepseek_input.withColumnRenamed("consensus_status", "original_recent5_consensus_status")
if "consensus_source" in deepseek_input.columns:
    deepseek_input = deepseek_input.withColumnRenamed("consensus_source", "original_recent5_consensus_source")

deepseek_input = (
    deepseek_input.withColumn("consensus_status", F.lit("model_disagreement_needs_review"))
    .withColumn("consensus_source", F.lit("recent5_degradation_disagreement"))
    .withColumn("degradation_disagreement_reason", F.col("degradation_disagreement_reason"))
)
deepseek_input.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fqtn(deepseek_input_table)
)

summary = {
    "analysis_channels": analysis.select("channel_id").distinct().count(),
    "disagreement_channels": disagreements.select("channel_id").distinct().count(),
    "deepseek_input_channels": deepseek_input.select("channel_id").distinct().count(),
    "recent5_internal_lid_disagreement": analysis.where(F.col("recent5_internal_lid_disagreement")).count(),
    "recent5_vs_baseline_lid_change": analysis.where(F.col("recent5_vs_baseline_lid_change")).count(),
    "recent5_vs_llm_reference_change": analysis.where(F.col("recent5_vs_llm_reference_change")).count(),
    "recent5_newly_unclassified_or_review": analysis.where(F.col("recent5_newly_unclassified_or_review")).count(),
}
print(summary)
