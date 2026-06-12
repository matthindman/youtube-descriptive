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
_create_text_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
_create_text_widget("source_run_id", "too_full_20260609")
_create_text_widget("inference_hash_buckets", "4096")
_create_text_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
_create_text_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
_create_text_widget("segments_input_table", "yt_lid_v3_too_full_20260609_segments_input")
_create_text_widget("comparison_table", "yt_lid_v3_too_full_20260609_channel_model_comparison")
_create_text_widget("output_table", "yt_lid_v3_too_full_20260609_llm_hardcase_disagreement_audit")
_create_text_widget("min_valid_votes", "4")
_create_text_widget("max_segments_per_channel", "24")
_create_text_widget("max_segment_chars", "500")
_create_text_widget("max_output_cases", "50")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
SOURCE_RUN_ID = _get_widget("source_run_id", "too_full_20260609")
INFERENCE_HASH_BUCKETS = int(_get_widget("inference_hash_buckets", "4096"))
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
REQUESTS_TABLE = _get_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
SEGMENTS_INPUT_TABLE = _get_widget("segments_input_table", "yt_lid_v3_too_full_20260609_segments_input")
COMPARISON_TABLE = _get_widget("comparison_table", "yt_lid_v3_too_full_20260609_channel_model_comparison")
OUTPUT_TABLE = _get_widget("output_table", "yt_lid_v3_too_full_20260609_llm_hardcase_disagreement_audit")
MIN_VALID_VOTES = int(_get_widget("min_valid_votes", "4"))
MAX_SEGMENTS_PER_CHANNEL = int(_get_widget("max_segments_per_channel", "24"))
MAX_SEGMENT_CHARS = int(_get_widget("max_segment_chars", "500"))
MAX_OUTPUT_CASES = int(_get_widget("max_output_cases", "50"))


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def _table_exists(table_full: str) -> bool:
    try:
        return spark.catalog.tableExists(table_full.replace("`", ""))
    except Exception:
        try:
            spark.table(table_full).limit(1).count()
            return True
        except Exception:
            return False


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def write_run_scoped(df, table_full: str) -> None:
    if not _table_exists(table_full):
        df.write.format("delta").mode("overwrite").partitionBy("run_id").saveAsTable(table_full)
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"run_id = {_sql_string(RUN_ID)}")
        .saveAsTable(table_full)
    )


raw = spark.table(fqtn(RAW_RESULTS_TABLE)).where(F.col("run_id") == F.lit(RUN_ID))

votes = (
    raw.where(F.col("is_valid_panel_vote") == F.lit(True))
    .select(
        "channel_id",
        F.lower(F.col("provider")).alias("provider"),
        "model",
        "model_tier",
        F.col("primary_language_label").alias("language_label"),
        F.col("primary_language_iso639_3").alias("primary_language_iso639_3"),
        F.col("pred_base_iso").alias("base_iso"),
        F.col("pred_normalized_base_iso").alias("normalized_base_iso"),
        F.col("pred_normalized_language_label").alias("normalized_language_label"),
        "primary_language_script",
        "secondary_language_label",
        "dialect_or_variant",
        "is_mixed_language",
        "is_romanized",
        "confidence",
        "evidence",
    )
    .where(F.col("normalized_base_iso").isNotNull())
    .withColumn("model_key", F.concat_ws(":", F.col("provider"), F.col("model")))
)

channel_vote_counts = votes.groupBy("channel_id").agg(F.countDistinct("model_key").alias("n_valid_votes"))

base_counts = (
    votes.groupBy("channel_id", "normalized_base_iso")
    .agg(
        F.countDistinct("model_key").alias("n"),
        F.sort_array(F.collect_set("provider")).alias("providers"),
        F.sort_array(F.collect_set("model_key")).alias("models"),
    )
)
base_counts = base_counts.withColumn("_max_n", F.max("n").over(Window.partitionBy("channel_id")))
base_lead_meta = base_counts.groupBy("channel_id").agg(
    F.max("_max_n").alias("majority_base_votes"),
    F.sum(F.when(F.col("n") == F.col("_max_n"), 1).otherwise(0)).alias("n_base_labels_tied_for_lead"),
    F.max(F.when(F.col("n") < F.col("_max_n"), F.col("n")).otherwise(F.lit(0))).alias("base_second_votes"),
)
base_w = Window.partitionBy("channel_id").orderBy(F.desc("n"), F.asc("normalized_base_iso"))
base_majority = (
    base_counts.withColumn("_rk", F.row_number().over(base_w))
    .where(F.col("_rk") == 1)
    .join(base_lead_meta, on="channel_id", how="inner")
    .select(
        "channel_id",
        F.col("normalized_base_iso").alias("majority_normalized_base_iso"),
        "majority_base_votes",
        "n_base_labels_tied_for_lead",
        "base_second_votes",
    )
)
base_dist = base_counts.groupBy("channel_id").agg(
    F.count(F.lit(1)).alias("n_distinct_normalized_base_iso"),
    F.to_json(F.sort_array(F.collect_list(F.struct("normalized_base_iso", "n", "providers", "models")))).alias("base_vote_distribution_json"),
)

label_counts = (
    votes.groupBy("channel_id", "normalized_language_label")
    .agg(
        F.countDistinct("model_key").alias("n"),
        F.sort_array(F.collect_set("provider")).alias("providers"),
        F.sort_array(F.collect_set("model_key")).alias("models"),
    )
)
label_counts = label_counts.withColumn("_max_n", F.max("n").over(Window.partitionBy("channel_id")))
label_lead_meta = label_counts.groupBy("channel_id").agg(
    F.max("_max_n").alias("majority_label_votes"),
    F.sum(F.when(F.col("n") == F.col("_max_n"), 1).otherwise(0)).alias("n_label_labels_tied_for_lead"),
    F.max(F.when(F.col("n") < F.col("_max_n"), F.col("n")).otherwise(F.lit(0))).alias("label_second_votes"),
)
label_w = Window.partitionBy("channel_id").orderBy(F.desc("n"), F.asc("normalized_language_label"))
label_majority = (
    label_counts.withColumn("_rk", F.row_number().over(label_w))
    .where(F.col("_rk") == 1)
    .join(label_lead_meta, on="channel_id", how="inner")
    .select(
        "channel_id",
        F.col("normalized_language_label").alias("majority_normalized_language_label"),
        "majority_label_votes",
        "n_label_labels_tied_for_lead",
        "label_second_votes",
    )
)
label_dist = label_counts.groupBy("channel_id").agg(
    F.count(F.lit(1)).alias("n_distinct_normalized_language_label"),
    F.to_json(F.sort_array(F.collect_list(F.struct("normalized_language_label", "n", "providers", "models")))).alias("label_vote_distribution_json"),
)

summary = (
    channel_vote_counts
    .join(base_majority, on="channel_id", how="inner")
    .join(base_dist, on="channel_id", how="inner")
    .join(label_majority, on="channel_id", how="inner")
    .join(label_dist, on="channel_id", how="inner")
    .withColumn("base_dissenting_models", F.col("n_valid_votes") - F.col("majority_base_votes"))
    .withColumn("label_dissenting_models", F.col("n_valid_votes") - F.col("majority_label_votes"))
    .withColumn("has_unique_base_leader", F.col("n_base_labels_tied_for_lead") == F.lit(1))
    .withColumn("has_unique_label_leader", F.col("n_label_labels_tied_for_lead") == F.lit(1))
)

cases = (
    summary.where(F.col("n_valid_votes") >= F.lit(MIN_VALID_VOTES))
    .where(
        (F.col("n_distinct_normalized_base_iso") > 1)
        | (F.col("n_distinct_normalized_language_label") > 1)
    )
    .persist()
)

case_ids = cases.select("channel_id").distinct().persist()

votes_for_cases = (
    votes.join(
        cases.select(
            "channel_id",
            "majority_normalized_base_iso",
            "majority_normalized_language_label",
            "has_unique_base_leader",
            "has_unique_label_leader",
        ),
        on="channel_id",
        how="inner",
    )
    .withColumn(
        "outside_base_majority",
        F.col("has_unique_base_leader") & (F.col("normalized_base_iso") != F.col("majority_normalized_base_iso")),
    )
    .withColumn(
        "outside_label_majority",
        F.col("has_unique_label_leader") & (F.col("normalized_language_label") != F.col("majority_normalized_language_label")),
    )
)

votes_agg = votes_for_cases.groupBy("channel_id").agg(
    F.to_json(F.sort_array(F.collect_list(F.struct(
        "provider",
        "model",
        "model_tier",
        "language_label",
        "primary_language_iso639_3",
        "base_iso",
        "normalized_base_iso",
        "normalized_language_label",
        "primary_language_script",
        "secondary_language_label",
        "dialect_or_variant",
        "is_mixed_language",
        "is_romanized",
        "confidence",
        "outside_base_majority",
        "outside_label_majority",
        "evidence",
    )))).alias("model_votes_json"),
    F.max(F.coalesce(F.col("is_mixed_language"), F.lit(False)).cast("int")).alias("any_model_mixed_language"),
    F.countDistinct("provider").alias("n_providers_with_valid_votes"),
)

requests = spark.table(fqtn(REQUESTS_TABLE))
prompt_w = Window.partitionBy("channel_id").orderBy(F.asc("provider"), F.asc("model"))
prompts = (
    requests.where(F.col("run_id") == F.lit(RUN_ID))
    .join(case_ids, on="channel_id", how="inner")
    .select("channel_id", "provider", "model", F.substring(F.col("prompt_user"), 1, 9000).alias("prompt_user"))
    .withColumn("_rk", F.row_number().over(prompt_w))
    .where(F.col("_rk") == 1)
    .drop("_rk", "provider", "model")
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

seg_filter = F.col("run_id") == F.lit(SOURCE_RUN_ID)
if "inference_hash_buckets" in seg_cols:
    seg_filter = seg_filter & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))

seg = (
    seg_tbl.where(seg_filter)
    .join(case_ids, on="channel_id", how="inner")
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
    F.count(F.lit(1)).alias("n_segments_shown"),
    F.sum(F.when(F.col("is_valid_text_for_lid"), 1).otherwise(0)).alias("n_valid_segments_shown"),
    F.countDistinct("dominant_script").alias("n_segment_scripts_shown"),
    F.sum(F.coalesce(F.col("clean_letter_count"), F.lit(0))).alias("shown_clean_letter_count"),
    F.to_json(F.collect_list(F.struct(
        "segment_type",
        "text",
        "is_valid_text_for_lid",
        "short_text_reason",
        "clean_letter_count",
        "clean_text_len",
        "dominant_script",
        "dominant_script_share",
    ))).alias("top_segments_json"),
)

script_summary = (
    seg.groupBy("channel_id", "dominant_script")
    .agg(
        F.count(F.lit(1)).alias("n_segments"),
        F.sum(F.when(F.col("is_valid_text_for_lid"), 1).otherwise(0)).alias("n_valid_segments"),
    )
    .groupBy("channel_id")
    .agg(F.to_json(F.sort_array(F.collect_list(F.struct("dominant_script", "n_segments", "n_valid_segments")))).alias("segment_script_summary_json"))
)

cmp_tbl = spark.table(fqtn(COMPARISON_TABLE))
cmp_cols = set(cmp_tbl.columns)
cmp_filter = F.col("run_id") == F.lit(SOURCE_RUN_ID)
if "inference_hash_buckets" in cmp_cols:
    cmp_filter = cmp_filter & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
cmp_select = [
    "channel_id",
    "consensus_status",
    "consensus_language_label",
    "openlid_primary_language_label",
    "openlid_primary_language_iso639_3",
    "glotlid_primary_language_label",
    "glotlid_primary_language_iso639_3",
]
for col_name in [
    "openlid_primary_language_confidence",
    "glotlid_primary_language_confidence",
    "openlid_secondary_language_label",
    "glotlid_secondary_language_label",
]:
    if col_name in cmp_cols:
        cmp_select.append(col_name)
cmp = (
    cmp_tbl.where(cmp_filter)
    .join(case_ids, on="channel_id", how="inner")
    .select(*cmp_select)
    .dropDuplicates(["channel_id"])
)

case_details = (
    cases
    .join(votes_agg, on="channel_id", how="left")
    .join(segments_agg, on="channel_id", how="left")
    .join(script_summary, on="channel_id", how="left")
    .join(cmp, on="channel_id", how="left")
    .join(prompts, on="channel_id", how="left")
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("source_run_id", F.lit(SOURCE_RUN_ID))
    .withColumn("audit_created_at_utc", F.current_timestamp())
    .withColumn(
        "_issue_flags_raw",
        F.array(
            F.when(
                (F.col("n_distinct_normalized_base_iso") == 1)
                & (F.col("n_distinct_normalized_language_label") > 1),
                F.lit("script_or_full_label_only_split"),
            ),
            F.when(~F.col("has_unique_base_leader"), F.lit("no_unique_base_leader")),
            F.when(~F.col("has_unique_label_leader"), F.lit("no_unique_label_leader")),
            F.when(F.coalesce(F.col("any_model_mixed_language"), F.lit(0)) > 0, F.lit("mixed_language_cue")),
            F.when(F.coalesce(F.col("n_segment_scripts_shown"), F.lit(0)) > 1, F.lit("multi_script_metadata")),
            F.when(F.coalesce(F.col("n_valid_segments_shown"), F.lit(0)) <= 2, F.lit("sparse_valid_lid_text")),
            F.when(
                (F.col("majority_normalized_base_iso") == F.col("openlid_primary_language_iso639_3"))
                | (F.col("majority_normalized_base_iso") == F.col("glotlid_primary_language_iso639_3")),
                F.lit("llm_majority_matches_one_lid_model"),
            ),
            F.when(F.col("base_dissenting_models") == 1, F.lit("single_model_base_outlier")),
            F.when(F.col("base_dissenting_models") >= 2, F.lit("multi_model_base_split")),
        ),
    )
    .withColumn("probable_issue_flags", F.expr("filter(_issue_flags_raw, x -> x is not null)"))
    .drop("_issue_flags_raw")
    .select(
        "run_id",
        "source_run_id",
        "channel_id",
        "n_valid_votes",
        "n_providers_with_valid_votes",
        "majority_normalized_base_iso",
        "majority_base_votes",
        "base_dissenting_models",
        "n_distinct_normalized_base_iso",
        "majority_normalized_language_label",
        "majority_label_votes",
        "label_dissenting_models",
        "n_distinct_normalized_language_label",
        "base_vote_distribution_json",
        "label_vote_distribution_json",
        "model_votes_json",
        "consensus_status",
        "consensus_language_label",
        "openlid_primary_language_label",
        "openlid_primary_language_iso639_3",
        "glotlid_primary_language_label",
        "glotlid_primary_language_iso639_3",
        *[c for c in [
            "openlid_primary_language_confidence",
            "glotlid_primary_language_confidence",
            "openlid_secondary_language_label",
            "glotlid_secondary_language_label",
        ] if c in cmp.columns],
        "n_segments_shown",
        "n_valid_segments_shown",
        "n_segment_scripts_shown",
        "shown_clean_letter_count",
        "segment_script_summary_json",
        "top_segments_json",
        "prompt_user",
        "any_model_mixed_language",
        "probable_issue_flags",
        "audit_created_at_utc",
    )
)

write_run_scoped(case_details, fqtn(OUTPUT_TABLE))

flag_counts = [
    row.asDict(recursive=True)
    for row in (
        case_details.select(F.explode_outer("probable_issue_flags").alias("flag"))
        .where(F.col("flag").isNotNull())
        .groupBy("flag")
        .agg(F.count(F.lit(1)).alias("n_cases"))
        .orderBy(F.desc("n_cases"), "flag")
        .collect()
    )
]

top_cases = [
    row.asDict(recursive=True)
    for row in (
        case_details.orderBy(
            F.desc("base_dissenting_models"),
            F.desc("label_dissenting_models"),
            F.desc("n_valid_votes"),
            "channel_id",
        )
        .select(
            "channel_id",
            "n_valid_votes",
            "majority_normalized_base_iso",
            "majority_base_votes",
            "base_dissenting_models",
            "n_distinct_normalized_base_iso",
            "majority_normalized_language_label",
            "label_dissenting_models",
            "n_distinct_normalized_language_label",
            "consensus_status",
            "openlid_primary_language_label",
            "glotlid_primary_language_label",
            "probable_issue_flags",
            "base_vote_distribution_json",
            "label_vote_distribution_json",
            "model_votes_json",
            "segment_script_summary_json",
            "top_segments_json",
        )
        .limit(MAX_OUTPUT_CASES)
        .collect()
    )
]

overall = {
    "run_id": RUN_ID,
    "source_run_id": SOURCE_RUN_ID,
    "output_table": f"{CATALOG}.{SCHEMA}.{OUTPUT_TABLE}",
    "n_channels_with_valid_votes": channel_vote_counts.count(),
    "n_disagreement_cases": case_details.count(),
    "min_valid_votes": MIN_VALID_VOTES,
    "flag_counts": flag_counts,
    "top_cases": top_cases,
}

print(json.dumps(overall, ensure_ascii=False, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(overall, ensure_ascii=False, sort_keys=True, default=str))
