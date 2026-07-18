# Databricks notebook source
# ruff: noqa: F821
"""DeepSeek fallback and final language publication for the below-10K sample."""

import json

from pyspark.sql import functions as F


SOURCE_RUN_ID = "banded_lt10k_20260716"
LLM_RUN_ID = "banded_lt10k_20260716_deepseek_flash_fallback"
PREFIX = "yt_lid_v3_banded_lt10k_20260716"
LLM_PREFIX = f"{PREFIX}_deepseek_flash_fallback"
LABEL_VERSION = "banded_lt10k_20260716_lid_deepseek_v1"
PROMPT_VERSION = "llm_fallback_final_guardrails_post_review_20260630"
HASH_BUCKETS = 128
EXPECTED_CHANNELS = 2000

SAMPLE_TABLE = "dev_sean.matt.yt_banded_sample"
SOURCE_CHANNELS_TABLE = f"dev_sean.matt.{PREFIX}_source_channels"
SOURCE_VIDEOS_TABLE = f"dev_sean.matt.{PREFIX}_source_videos"
LID_CHANNELS_TABLE = f"dev_sean.matt.{PREFIX}_channels"
LID_COMPARISON_TABLE = f"dev_sean.matt.{PREFIX}_channel_model_comparison"
LID_SEGMENTS_TABLE = f"dev_sean.matt.{PREFIX}_segments_input"
LID_TEXT_FEATURES_TABLE = f"dev_sean.matt.{PREFIX}_channel_text_features"
LID_HINDI_AUDIT_TABLE = f"dev_sean.matt.{PREFIX}_hindi_indic_audit_candidates"
ROUTING_COMPARISON_TABLE = f"dev_sean.matt.{PREFIX}_llm_routing_comparison"

REQUESTS_TABLE = f"dev_sean.matt.{LLM_PREFIX}_llm_requests"
BATCH_JOBS_TABLE = f"dev_sean.matt.{LLM_PREFIX}_llm_batch_jobs"
RAW_RESULTS_TABLE = f"dev_sean.matt.{LLM_PREFIX}_llm_raw_results"
VERDICTS_TABLE = f"dev_sean.matt.{LLM_PREFIX}_llm_verdicts"
MODEL_AGREEMENT_TABLE = f"dev_sean.matt.{LLM_PREFIX}_llm_model_agreement"
PROGRESS_TABLE = f"dev_sean.matt.{LLM_PREFIX}_llm_run_progress"

CURRENT_TABLE = f"dev_sean.matt.{PREFIX}_channel_language_current"
LABELS_TABLE = f"dev_sean.matt.{PREFIX}_channel_language_labels"
BAND_SUMMARY_TABLE = f"dev_sean.matt.{PREFIX}_channel_language_band_summary"
RUN_SUMMARY_TABLE = f"dev_sean.matt.{PREFIX}_channel_language_run_summary"

LLM_NOTEBOOK = (
    "/Users/matt.hindman@researchaccelerator.org/"
    "banded_lt10k_language_20260716/03_language_llm_panel_databricks"
)


def write_table(df, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def run_scope(df, run_id: str):
    return df.where(
        (F.col("run_id") == F.lit(run_id))
        & (F.col("inference_hash_buckets") == F.lit(HASH_BUCKETS))
    )


lid = run_scope(spark.table(LID_CHANNELS_TABLE), SOURCE_RUN_ID)
comparison = run_scope(spark.table(LID_COMPARISON_TABLE), SOURCE_RUN_ID)

lid_count = lid.count()
lid_distinct = lid.select("channel_id").distinct().count()
if lid_count != EXPECTED_CHANNELS or lid_distinct != EXPECTED_CHANNELS:
    raise AssertionError(
        f"LID source must contain 2,000 unique channels; rows={lid_count}, distinct={lid_distinct}"
    )

same_lid_iso = (
    F.col("openlid_primary_language_iso639_3").isNotNull()
    & F.col("glotlid_primary_language_iso639_3").isNotNull()
    & (
        F.lower(F.col("openlid_primary_language_iso639_3"))
        == F.lower(F.col("glotlid_primary_language_iso639_3"))
    )
)
lid_base_resolved = F.col("consensus_language_iso639_3").isNotNull() | same_lid_iso

# Preserve the original LID state, but expose one routing status that means exactly
# "base language unresolved". This keeps script-only disagreements out of the LLM fallback.
routing_comparison = (
    comparison
    .withColumn("source_consensus_status", F.col("consensus_status"))
    .withColumn("source_consensus_language_label", F.col("consensus_language_label"))
    .withColumn("lid_base_language_resolved", lid_base_resolved)
    .withColumn(
        "consensus_status",
        F.when(lid_base_resolved, F.lit("exact_model_agreement"))
        .otherwise(F.lit("model_disagreement_needs_review")),
    )
    .withColumn(
        "consensus_language_label",
        F.when(lid_base_resolved, F.col("consensus_language_label"))
        .otherwise(F.lit(None).cast("string")),
    )
)
write_table(routing_comparison, ROUTING_COMPARISON_TABLE)

unresolved_comparison_channels = (
    routing_comparison
    .where(~F.col("lid_base_language_resolved"))
    .select("channel_id")
    .distinct()
    .count()
)

models_json = json.dumps([
    {"provider": "deepseek", "model": "deepseek-v4-flash", "tier": "small"}
])
llm_args = {
    "catalog": "dev_sean",
    "schema": "matt",
    "comparison_table": ROUTING_COMPARISON_TABLE,
    "segments_input_table": LID_SEGMENTS_TABLE,
    "channels_table": LID_CHANNELS_TABLE,
    "channel_text_features_table": LID_TEXT_FEATURES_TABLE,
    "hindi_indic_audit_table": LID_HINDI_AUDIT_TABLE,
    "source_channels_table": SOURCE_CHANNELS_TABLE,
    "source_videos_table": SOURCE_VIDEOS_TABLE,
    "run_id": LLM_RUN_ID,
    "source_run_id": SOURCE_RUN_ID,
    "inference_hash_buckets": str(HASH_BUCKETS),
    "panel_requests_table": REQUESTS_TABLE,
    "panel_batch_jobs_table": BATCH_JOBS_TABLE,
    "panel_raw_results_table": RAW_RESULTS_TABLE,
    "panel_verdicts_table": VERDICTS_TABLE,
    "panel_model_agreement_table": MODEL_AGREEMENT_TABLE,
    "panel_run_progress_table": PROGRESS_TABLE,
    "routing_mode": "residual_panel",
    "route_disagreement": "true",
    "route_unresolved_tail": "false",
    "route_shared_bias_english_indic": "false",
    "route_unclassified": "true",
    "route_agreement_audit": "false",
    "exclude_arabic_family_pairs": "false",
    "max_routed_channels": "0",
    "models_json": models_json,
    "max_output_tokens": "2000",
    "temperature": "",
    "prompt_version": PROMPT_VERSION,
    "apply_llm_calibration": "true",
    "deepseek_thinking_type": "disabled",
    "deepseek_reasoning_effort": "",
    "deepseek_max_output_tokens": "600",
    "deepseek_max_workers": "16",
    "deepseek_request_timeout_seconds": "60",
    "deepseek_max_retries": "2",
    "deepseek_pending_batch_size": "500",
    "deepseek_direct_streaming": "true",
    "deepseek_delete_request_jsonl_after_submit": "true",
    "deepseek_direct_submit_from_requests_table": "true",
    "submit_batches": "true",
    "submit_provider_filter": "deepseek",
    "submit_model_filter": "deepseek-v4-flash",
    "skip_existing_submitted_batches": "true",
    "import_results": "true",
    "reuse_existing_requests_on_submit": "true",
    "reuse_existing_requests_on_import": "true",
    "panel_majority_mode": "reached_models",
    "min_panel_votes_for_majority": "1",
    "panel_majority_vote_basis": "normalized_base_iso",
    "secret_scope": "youtube-llm-keys",
    "deepseek_secret_key": "deepseek-api-key",
}

print("Invoking DeepSeek fallback notebook with arguments:")
print(json.dumps(llm_args, indent=2, sort_keys=True))
llm_result = dbutils.notebook.run(LLM_NOTEBOOK, 0, llm_args)
print("DeepSeek fallback result:", llm_result)

requests = spark.table(REQUESTS_TABLE).where(F.col("run_id") == F.lit(LLM_RUN_ID))
raw = spark.table(RAW_RESULTS_TABLE).where(F.col("run_id") == F.lit(LLM_RUN_ID))
verdicts = spark.table(VERDICTS_TABLE).where(F.col("run_id") == F.lit(LLM_RUN_ID))

request_count = requests.count()
request_distinct = requests.select("request_id").distinct().count()
request_channels = requests.select("channel_id").distinct().count()
raw_count = raw.count()
raw_distinct = raw.select("request_id").distinct().count()
verdict_count = verdicts.count()
verdict_distinct = verdicts.select("channel_id").distinct().count()
if not (
    request_count
    == request_distinct
    == request_channels
    == raw_count
    == raw_distinct
    == verdict_count
    == verdict_distinct
):
    raise AssertionError(
        "DeepSeek coverage mismatch: "
        f"requests={request_count}, request_ids={request_distinct}, request_channels={request_channels}, "
        f"raw={raw_count}, raw_ids={raw_distinct}, verdicts={verdict_count}, "
        f"verdict_channels={verdict_distinct}"
    )

failed_result_status = (
    F.lower(F.coalesce(F.col("result_status").cast("string"), F.lit(""))).rlike("^[45][0-9][0-9]$")
    | F.lower(F.coalesce(F.col("result_status").cast("string"), F.lit(""))).isin(
        "error", "failed", "failure", "errored", "expired", "cancelled", "canceled",
        "json_load_error", "rate_limited", "timeout",
    )
)
resolved_prediction = (
    F.coalesce(F.col("is_valid_panel_vote"), F.lit(False))
    | (
        (F.lower(F.coalesce(F.col("calibrated_status"), F.lit(""))) == F.lit("insufficient_text"))
        & F.col("parse_error").isNull()
        & F.col("prediction_parse_error").isNull()
        & (~failed_result_status)
    )
)
technical_failures = raw.where(~resolved_prediction).count()
if technical_failures:
    raise AssertionError(
        f"DeepSeek returned {technical_failures} unresolved technical/parse failures; repair before publication"
    )

sample = (
    spark.table(SAMPLE_TABLE)
    .where(F.col("subscriber_count") < F.lit(10000))
    .select(
        F.col("canonical_id").alias("channel_id"),
        "subscriber_count",
        "band",
        F.col("run_id").alias("sample_run_id"),
    )
)
source_channels = spark.table(SOURCE_CHANNELS_TABLE).select(
    "channel_id", "channel_name", "channel_description", "channel_collected_at"
)
video_coverage = (
    spark.table(SOURCE_VIDEOS_TABLE)
    .groupBy("channel_id")
    .agg(F.count(F.lit(1)).alias("recent_video_count"))
)

lid_projection = lid.select(
    "channel_id", "channel_hash_bucket", "consensus_status",
    "consensus_language_label", "consensus_language_iso639_3", "consensus_language_script",
    "consensus_is_credible_mixed_language_candidate",
    "openlid_primary_language_label", "openlid_primary_language_iso639_3",
    "openlid_primary_language_script", "glotlid_primary_language_label",
    "glotlid_primary_language_iso639_3", "glotlid_primary_language_script",
    F.col("prediction_timestamp").alias("lid_prediction_timestamp"),
)
llm_projection = verdicts.select(
    "channel_id", "route_reason", "panel_status", "panel_language_iso639_3",
    "panel_language_label", "panel_language_script", "panel_is_mixed_language",
    "panel_is_romanized", "panel_confidence", "panel_evidence",
    F.col("prediction_timestamp").alias("llm_prediction_timestamp"),
)

joined = (
    sample
    .join(source_channels, "channel_id", "left")
    .join(video_coverage, "channel_id", "left")
    .join(lid_projection, "channel_id", "left")
    .join(llm_projection, "channel_id", "left")
)
has_llm = F.col("panel_status").isNotNull()
llm_classified = (
    (F.col("panel_status") == F.lit("panel_majority"))
    & F.col("panel_language_iso639_3").isNotNull()
)
same_lid_iso_final = (
    F.col("openlid_primary_language_iso639_3").isNotNull()
    & F.col("glotlid_primary_language_iso639_3").isNotNull()
    & (
        F.lower(F.col("openlid_primary_language_iso639_3"))
        == F.lower(F.col("glotlid_primary_language_iso639_3"))
    )
)
lid_consensus = (~has_llm) & F.col("consensus_language_iso639_3").isNotNull()
lid_base_agreement = (~has_llm) & (~lid_consensus) & same_lid_iso_final

channel_language = (
    F.when(llm_classified, F.lower(F.trim(F.col("panel_language_iso639_3"))))
    .when(has_llm, F.lit("und"))
    .when(lid_consensus, F.lower(F.trim(F.col("consensus_language_iso639_3"))))
    .when(lid_base_agreement, F.lower(F.trim(F.col("openlid_primary_language_iso639_3"))))
    .otherwise(F.lit("und"))
)
same_lid_script = (
    F.col("openlid_primary_language_script").isNotNull()
    & F.col("glotlid_primary_language_script").isNotNull()
    & (F.col("openlid_primary_language_script") == F.col("glotlid_primary_language_script"))
)
source_script = (
    F.when(llm_classified, F.col("panel_language_script"))
    .when(has_llm, F.lit(None).cast("string"))
    .when(lid_consensus, F.col("consensus_language_script"))
    .when(lid_base_agreement & same_lid_script, F.col("openlid_primary_language_script"))
    .otherwise(F.lit(None).cast("string"))
)
normalized_script = (
    F.when(source_script == F.lit("Japn"), F.lit("Jpan"))
    .when(source_script == F.lit("Myan"), F.lit("Mymr"))
    .when(source_script == F.lit("Trad"), F.lit("Hant"))
    .when(source_script.isin("Sant", "Syrl"), F.lit(None).cast("string"))
    .otherwise(source_script)
)
label_source = (
    F.when(llm_classified, F.lit("deepseek_flash_fallback"))
    .when(has_llm, F.lit("deepseek_flash_insufficient_text"))
    .when(lid_consensus, F.lit("lid_consensus"))
    .when(lid_base_agreement, F.lit("lid_base_iso_agreement"))
    .otherwise(F.lit("unresolved"))
)
script_ambiguous = (
    lid_base_agreement
    & F.col("openlid_primary_language_script").isNotNull()
    & F.col("glotlid_primary_language_script").isNotNull()
    & (~same_lid_script)
)
mixed = (
    F.when(llm_classified, F.coalesce(F.col("panel_is_mixed_language"), F.lit(False)))
    .when(has_llm, F.lit(None).cast("boolean"))
    .when(lid_consensus | lid_base_agreement,
          F.coalesce(F.col("consensus_is_credible_mixed_language_candidate"), F.lit(False)))
    .otherwise(F.lit(None).cast("boolean"))
)
published_at = F.current_timestamp()

final = joined.select(
    "channel_id", "subscriber_count", "band", "sample_run_id", "channel_name",
    F.col("channel_collected_at").isNotNull().alias("has_channel_metadata_row"),
    (F.length(F.trim(F.coalesce(F.col("channel_description"), F.lit("")))) > 0)
    .alias("has_nonempty_channel_description"),
    (F.coalesce(F.col("recent_video_count"), F.lit(0)) > 0).alias("has_recent_videos"),
    F.coalesce(F.col("recent_video_count"), F.lit(0)).cast("long").alias("recent_video_count"),
    "channel_hash_bucket",
    channel_language.alias("channel_language"),
    source_script.alias("source_language_script"),
    normalized_script.alias("channel_language_script"),
    F.when(
        (channel_language != F.lit("und")) & normalized_script.isNotNull(),
        F.concat_ws("_", channel_language, normalized_script),
    ).alias("channel_language_script_label"),
    (channel_language != F.lit("und")).alias("is_language_classified"),
    mixed.alias("is_mixed_language"),
    F.when(llm_classified, F.col("panel_is_romanized"))
    .otherwise(F.lit(None).cast("boolean")).alias("is_romanized"),
    F.coalesce(script_ambiguous, F.lit(False)).alias("is_script_ambiguous"),
    label_source.alias("language_label_source"),
    F.when(llm_classified, F.col("panel_confidence"))
    .otherwise(F.lit(None).cast("string")).alias("language_confidence_level"),
    F.col("consensus_status").alias("lid_consensus_status"),
    F.col("consensus_language_label").alias("lid_consensus_language_label"),
    "openlid_primary_language_label", "glotlid_primary_language_label",
    F.when(has_llm, F.col("panel_status")).otherwise(F.lit(None).cast("string")).alias("llm_status"),
    F.when(has_llm, F.col("route_reason")).otherwise(F.lit(None).cast("string")).alias("llm_route_reason"),
    F.when(has_llm, F.lit("deepseek-v4-flash")).otherwise(F.lit(None).cast("string")).alias("llm_model"),
    F.when(has_llm, F.lit(False)).otherwise(F.lit(None).cast("boolean")).alias("llm_thinking_enabled"),
    F.when(has_llm, F.lit(PROMPT_VERSION)).otherwise(F.lit(None).cast("string")).alias("llm_prompt_version"),
    F.when(has_llm, F.col("panel_evidence")).otherwise(F.lit(None).cast("string")).alias("llm_evidence"),
    F.when(has_llm, F.lit(VERDICTS_TABLE)).otherwise(F.lit(LID_CHANNELS_TABLE)).alias("classification_source_table"),
    F.when(has_llm, F.lit(LLM_RUN_ID)).otherwise(F.lit(SOURCE_RUN_ID)).alias("classification_source_run_id"),
    F.when(has_llm, F.col("llm_prediction_timestamp")).otherwise(F.col("lid_prediction_timestamp"))
    .alias("source_prediction_timestamp"),
    F.lit(LABEL_VERSION).alias("label_version"),
    published_at.alias("published_at"),
)

final = final.cache()
final_count = final.count()
final_distinct = final.select("channel_id").distinct().count()
invalid_codes = final.where(~F.col("channel_language").rlike("^[a-z]{3}$")).count()
invalid_scripts = final.where(
    F.col("channel_language_script").isNotNull()
    & (~F.col("channel_language_script").rlike("^[A-Z][a-z]{3}$"))
).count()
if final_count != EXPECTED_CHANNELS or final_distinct != EXPECTED_CHANNELS:
    raise AssertionError(f"Final cardinality failed: rows={final_count}, distinct={final_distinct}")
if invalid_codes:
    raise AssertionError(f"Found {invalid_codes} invalid base language codes")
if invalid_scripts:
    raise AssertionError(f"Found {invalid_scripts} invalid normalized script codes")

write_table(final, CURRENT_TABLE)
write_table(final, LABELS_TABLE)

band_summary = (
    final.groupBy("band")
    .agg(
        F.min("subscriber_count").alias("sample_min_subscribers"),
        F.max("subscriber_count").alias("sample_max_subscribers"),
        F.count(F.lit(1)).alias("channels"),
        F.sum(F.col("is_language_classified").cast("int")).alias("classified_channels"),
        F.sum((F.col("channel_language") == F.lit("und")).cast("int")).alias("und_channels"),
        F.sum(F.col("has_nonempty_channel_description").cast("int")).alias("nonempty_description_channels"),
        F.sum(F.col("has_recent_videos").cast("int")).alias("video_channels"),
        F.sum(F.col("is_mixed_language").cast("int")).alias("mixed_channels"),
    )
    .withColumn("classified_share", F.col("classified_channels") / F.col("channels"))
    .withColumn("label_version", F.lit(LABEL_VERSION))
    .orderBy("band")
)
write_table(band_summary, BAND_SUMMARY_TABLE)

post = spark.table(CURRENT_TABLE)
post_counts = {
    "rows": post.count(),
    "distinct_channels": post.select("channel_id").distinct().count(),
    "classified": post.where(F.col("is_language_classified")).count(),
    "und": post.where(F.col("channel_language") == F.lit("und")).count(),
    "mixed": post.where(F.col("is_mixed_language") == F.lit(True)).count(),
    "script_ambiguous": post.where(F.col("is_script_ambiguous")).count(),
}
source_distribution = [
    row.asDict(recursive=True)
    for row in post.groupBy("language_label_source").count().orderBy(F.desc("count")).collect()
]
top_languages = [
    row.asDict(recursive=True)
    for row in post.groupBy("channel_language").count()
    .orderBy(F.desc("count"), F.asc("channel_language")).limit(50).collect()
]
status_distribution = [
    row.asDict(recursive=True)
    for row in post.groupBy("lid_consensus_status", "llm_status").count()
    .orderBy(F.desc("count")).collect()
]
band_distribution = [row.asDict(recursive=True) for row in band_summary.collect()]

summary = {
    "source_run_id": SOURCE_RUN_ID,
    "llm_run_id": LLM_RUN_ID,
    "label_version": LABEL_VERSION,
    "sample_table": SAMPLE_TABLE,
    "source_channels_table": SOURCE_CHANNELS_TABLE,
    "source_videos_table": SOURCE_VIDEOS_TABLE,
    "lid_channels_table": LID_CHANNELS_TABLE,
    "lid_comparison_table": LID_COMPARISON_TABLE,
    "routing_comparison_table": ROUTING_COMPARISON_TABLE,
    "llm_requests_table": REQUESTS_TABLE,
    "llm_raw_results_table": RAW_RESULTS_TABLE,
    "llm_verdicts_table": VERDICTS_TABLE,
    "current_table": CURRENT_TABLE,
    "labels_table": LABELS_TABLE,
    "band_summary_table": BAND_SUMMARY_TABLE,
    "unresolved_comparison_channels_before_llm": unresolved_comparison_channels,
    "deepseek_requests": request_count,
    "deepseek_technical_failures": technical_failures,
    "post_counts": post_counts,
    "source_distribution": source_distribution,
    "top_languages": top_languages,
    "status_distribution": status_distribution,
    "band_distribution": band_distribution,
    "llm_notebook_result": llm_result,
}
summary_rows = [
    (LABEL_VERSION, key, json.dumps(value, sort_keys=True, default=str))
    for key, value in summary.items()
]
write_table(
    spark.createDataFrame(summary_rows, "label_version string, metric string, value_json string"),
    RUN_SUMMARY_TABLE,
)

for table_name, comment in {
    CURRENT_TABLE: (
        "Current one-row-per-channel language labels for the 2,000-channel stratified below-10K sample. "
        "channel_language is the default base ISO 639-3 analysis variable; script is separate."
    ),
    LABELS_TABLE: (
        "Versioned language labels for the 2,000-channel stratified below-10K sample, label version "
        f"{LABEL_VERSION}."
    ),
    BAND_SUMMARY_TABLE: "Language-label coverage by the original subscriber-count sample band.",
}.items():
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{comment.replace(chr(39), chr(39) * 2)}'")
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
        "'quality' = 'silver', "
        f"'language.label_version' = '{LABEL_VERSION}', "
        f"'language.source_run_id' = '{SOURCE_RUN_ID}'"
        ")"
    )

print("BANDED_LT10K_CHANNEL_LANGUAGE_SUMMARY=" + json.dumps(summary, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(summary, sort_keys=True, default=str))
