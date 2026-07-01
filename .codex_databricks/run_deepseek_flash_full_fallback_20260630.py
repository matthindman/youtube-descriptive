# Databricks notebook source
"""Run DeepSeek Flash on the full LID residual fallback cohort.

This driver targets channels from the 2026-06-23 full crawl where the two LID
models disagree or where the final LID table did not produce a language
classification. It keeps preflight, smoke, pilot, and full phases in separate
run/table namespaces while using the same routing and prompt configuration.
"""

import json
from typing import Dict, Iterable, List, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def _widget(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except Exception:
        return default


CATALOG = _widget("catalog", "dev_sean")
SCHEMA = _widget("schema", "matt")
SOURCE_RUN_ID = _widget("source_run_id", "channel_crawl_full_20260623")
INFERENCE_HASH_BUCKETS = int(_widget("inference_hash_buckets", "4096"))
BASE_PREFIX = _widget("base_prefix", "yt_lid_v3_channel_crawl_full_20260623")
BASE_RUN_ID = _widget(
    "base_run_id",
    "channel_crawl_full_20260623_deepseek_flash_full_fallback_20260630",
)
BASE_OUTPUT_PREFIX = _widget(
    "base_output_prefix",
    "yt_lid_v3_channel_crawl_full_20260623_deepseek_flash_full_fallback_20260630",
)
PHASE = _widget("phase", "preflight_only").strip().lower()
PROMPT_VERSION = _widget("prompt_version", "llm_fallback_final_guardrails_post_review_20260630")
LLM_NOTEBOOK_PATH = _widget(
    "llm_notebook_path",
    "/Users/matt.hindman@researchaccelerator.org/lid_v3_channel_crawl_full_20260623/03_language_llm_panel_databricks_post_review_20260630",
)

ROUTE_DISAGREEMENT = _widget("route_disagreement", "true").strip().lower() == "true"
ROUTE_UNCLASSIFIED = _widget("route_unclassified", "true").strip().lower() == "true"
EXCLUDE_ARABIC_FAMILY_PAIRS = _widget("exclude_arabic_family_pairs", "true").strip().lower() == "true"
USE_SOURCE_FALLBACK = _widget("use_source_fallback", "true").strip().lower() == "true"

SOURCE_CHANNELS_TABLE = _widget("source_channels_table", f"{BASE_PREFIX}_source_channels")
SOURCE_VIDEOS_TABLE = _widget("source_videos_table", f"{BASE_PREFIX}_source_videos")

SUBMIT_BATCHES = _widget("submit_batches", "true").strip().lower() == "true"
IMPORT_RESULTS = _widget("import_results", "true").strip().lower() == "true"
DEEPSEEK_MAX_WORKERS = _widget("deepseek_max_workers", "16")
DEEPSEEK_REQUEST_TIMEOUT_SECONDS = _widget("deepseek_request_timeout_seconds", "60")
DEEPSEEK_MAX_RETRIES = _widget("deepseek_max_retries", "1")
DEEPSEEK_DIRECT_STREAMING = _widget("deepseek_direct_streaming", "true")
DEEPSEEK_DELETE_REQUEST_JSONL_AFTER_SUBMIT = _widget("deepseek_delete_request_jsonl_after_submit", "true")
DEEPSEEK_DIRECT_SUBMIT_FROM_REQUESTS_TABLE = _widget("deepseek_direct_submit_from_requests_table", "true")

DEFAULT_PHASE_LIMITS = {
    "preflight_only": 0,
    "smoke_5k": 5000,
    "pilot_50k": 50000,
    "full": 0,
}
if PHASE not in DEFAULT_PHASE_LIMITS:
    raise ValueError(f"Unsupported phase={PHASE!r}; expected one of {sorted(DEFAULT_PHASE_LIMITS)}")
MAX_ROUTED_CHANNELS = int(_widget("max_routed_channels", str(DEFAULT_PHASE_LIMITS[PHASE])))

PHASE_SUFFIX = {
    "preflight_only": "",
    "smoke_5k": "_smoke_5k",
    "pilot_50k": "_pilot_50k",
    "full": "",
}[PHASE]
RUN_ID = _widget("run_id", f"{BASE_RUN_ID}{PHASE_SUFFIX}")
OUTPUT_PREFIX = _widget("output_prefix", f"{BASE_OUTPUT_PREFIX}{PHASE_SUFFIX}")


ARABIC_FAMILY_ISO = {"ara", "arb", "ary", "arz", "arq", "apc", "ars", "ajp", "aeb", "acm", "acq", "aec", "afb", "ayl", "ayn"}
DISAGREEMENT_STATUSES = [
    "model_disagreement_needs_review",
    "glotlid_fallback_openlid_low_confidence",
    "openlid_high_confidence_glotlid_missing_or_error",
]


def fqtn(table: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{CATALOG}.{table}"
    return f"{CATALOG}.{SCHEMA}.{table}"


def _table_exists(table: str) -> bool:
    try:
        return spark.catalog.tableExists(fqtn(table))
    except Exception:
        return False


def _scoped(df: DataFrame) -> DataFrame:
    cols = set(df.columns)
    if "run_id" in cols:
        df = df.where(F.col("run_id") == F.lit(SOURCE_RUN_ID))
    if "inference_hash_buckets" in cols:
        df = df.where(F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
    return df


def write_table(df: DataFrame, table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqtn(table))
    )


def _maybe_col(cols: Iterable[str], name: str, data_type: str = "string"):
    return F.col(name) if name in cols else F.lit(None).cast(data_type)


def _route_frame(df: DataFrame, route_reason: str, cols: Iterable[str]) -> DataFrame:
    cols = set(cols)
    return df.select(
        "channel_id",
        _maybe_col(cols, "channel_hash_bucket", "int").alias("channel_hash_bucket"),
        _maybe_col(cols, "consensus_status").alias("consensus_status"),
        _maybe_col(cols, "consensus_language_label").alias("consensus_language_label"),
        _maybe_col(cols, "consensus_source").alias("consensus_source"),
        _maybe_col(cols, "openlid_primary_language_label").alias("openlid_primary_language_label"),
        _maybe_col(cols, "openlid_primary_language_iso639_3").alias("openlid_primary_language_iso639_3"),
        _maybe_col(cols, "glotlid_primary_language_label").alias("glotlid_primary_language_label"),
        _maybe_col(cols, "glotlid_primary_language_iso639_3").alias("glotlid_primary_language_iso639_3"),
        F.lit(route_reason).alias("route_reason"),
    )


comparison_table = f"{BASE_PREFIX}_channel_model_comparison"
segments_input_table = f"{BASE_PREFIX}_segments_input"
channels_table = f"{BASE_PREFIX}_channels"
channel_text_features_table = f"{BASE_PREFIX}_channel_text_features"
hindi_indic_audit_table = f"{BASE_PREFIX}_hindi_indic_audit_candidates"

preflight_table = f"{BASE_OUTPUT_PREFIX}_preflight_qa"
eligible_routes_table = f"{BASE_OUTPUT_PREFIX}_eligible_routes"

requests_table = f"{OUTPUT_PREFIX}_llm_requests"
batch_jobs_table = f"{OUTPUT_PREFIX}_llm_batch_jobs"
raw_results_table = f"{OUTPUT_PREFIX}_llm_raw_results"
verdicts_table = f"{OUTPUT_PREFIX}_llm_verdicts"
model_agreement_table = f"{OUTPUT_PREFIX}_llm_model_agreement"
progress_table = f"{OUTPUT_PREFIX}_llm_run_progress"

for required_table in [comparison_table, channels_table, segments_input_table]:
    if not _table_exists(required_table):
        raise ValueError(f"Required table does not exist: {fqtn(required_table)}")

source_channels_for_panel = SOURCE_CHANNELS_TABLE if USE_SOURCE_FALLBACK and _table_exists(SOURCE_CHANNELS_TABLE) else ""
source_videos_for_panel = SOURCE_VIDEOS_TABLE if USE_SOURCE_FALLBACK and _table_exists(SOURCE_VIDEOS_TABLE) else ""

cmp_df = _scoped(spark.table(fqtn(comparison_table)))
ch_src = _scoped(spark.table(fqtn(channels_table)))
cmp_cols = set(cmp_df.columns)
ch_cols = set(ch_src.columns)

route_frames: List[DataFrame] = []
if ROUTE_DISAGREEMENT:
    d = cmp_df.where(F.col("consensus_status").isin(*DISAGREEMENT_STATUSES))
    if EXCLUDE_ARABIC_FAMILY_PAIRS and {"openlid_primary_language_iso639_3", "glotlid_primary_language_iso639_3"}.issubset(cmp_cols):
        both_arabic = (
            F.col("openlid_primary_language_iso639_3").isin(*sorted(ARABIC_FAMILY_ISO))
            & F.col("glotlid_primary_language_iso639_3").isin(*sorted(ARABIC_FAMILY_ISO))
        )
        d = d.where(~F.coalesce(both_arabic, F.lit(False)))
    route_frames.append(_route_frame(d, "disagreement", cmp_cols))

if ROUTE_UNCLASSIFIED:
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
            f"{fqtn(channels_table)} lacks the fields needed for route_unclassified=true"
        )
    route_frames.append(_route_frame(ch_src.where(unclassified_condition), "unclassified", ch_cols))

if not route_frames:
    raise ValueError("No route frames were enabled.")

priority = F.create_map(
    F.lit("disagreement"), F.lit(1),
    F.lit("unclassified"), F.lit(4),
)
routed = route_frames[0]
for frame in route_frames[1:]:
    routed = routed.unionByName(frame, allowMissingColumns=True)

routed = (
    routed
    .withColumn("_priority", F.element_at(priority, F.col("route_reason")))
    .withColumn("_rk", F.row_number().over(Window.partitionBy("channel_id").orderBy(F.col("_priority").asc(), F.col("route_reason").asc())))
    .where(F.col("_rk") == 1)
    .drop("_priority", "_rk")
    .persist()
)

write_table(routed, eligible_routes_table)

channels_total = ch_src.select("channel_id").distinct().count()
comparison_total = cmp_df.select("channel_id").distinct().count()
routed_total = routed.select("channel_id").distinct().count()
route_counts = [
    row.asDict()
    for row in routed.groupBy("route_reason").count().orderBy("route_reason").collect()
]

seg = _scoped(spark.table(fqtn(segments_input_table))).select("channel_id").dropDuplicates(["channel_id"])
routed_without_segments = routed.select("channel_id").join(seg, on="channel_id", how="left_anti").persist()
routed_without_segments_count = routed_without_segments.count()

source_channels_covered = None
source_videos_covered = None
source_channels_columns: List[str] = []
source_videos_columns: List[str] = []
if source_channels_for_panel:
    src_ch = spark.table(fqtn(source_channels_for_panel))
    source_channels_columns = src_ch.columns
    source_channels_covered = routed_without_segments.join(
        src_ch.select("channel_id").dropDuplicates(["channel_id"]), on="channel_id", how="inner"
    ).count()
if source_videos_for_panel:
    src_v = spark.table(fqtn(source_videos_for_panel))
    source_videos_columns = src_v.columns
    source_videos_covered = routed_without_segments.join(
        src_v.select("channel_id").dropDuplicates(["channel_id"]), on="channel_id", how="inner"
    ).count()

preflight_summary = {
    "phase": PHASE,
    "run_id": RUN_ID,
    "output_prefix": OUTPUT_PREFIX,
    "source_run_id": SOURCE_RUN_ID,
    "prompt_version": PROMPT_VERSION,
    "comparison_table": fqtn(comparison_table),
    "channels_table": fqtn(channels_table),
    "segments_input_table": fqtn(segments_input_table),
    "source_channels_table": fqtn(source_channels_for_panel) if source_channels_for_panel else "",
    "source_videos_table": fqtn(source_videos_for_panel) if source_videos_for_panel else "",
    "eligible_routes_table": fqtn(eligible_routes_table),
    "preflight_table": fqtn(preflight_table),
    "routes": {
        "route_disagreement": ROUTE_DISAGREEMENT,
        "route_unclassified": ROUTE_UNCLASSIFIED,
        "exclude_arabic_family_pairs": EXCLUDE_ARABIC_FAMILY_PAIRS,
    },
    "counts": {
        "channels_total": channels_total,
        "comparison_total": comparison_total,
        "routed_total": routed_total,
        "routed_fraction_of_channels": (float(routed_total) / float(channels_total)) if channels_total else None,
        "routed_without_segments": routed_without_segments_count,
        "source_channels_covered_missing_segments": source_channels_covered,
        "source_videos_covered_missing_segments": source_videos_covered,
    },
    "route_counts": route_counts,
    "source_columns": {
        "source_channels": source_channels_columns,
        "source_videos": source_videos_columns,
    },
}

qa_rows: List[Tuple[str, str, str, str]] = []
for key, value in preflight_summary["counts"].items():
    qa_rows.append((RUN_ID, PHASE, key, json.dumps(value, ensure_ascii=False)))
qa_rows.append((RUN_ID, PHASE, "route_counts", json.dumps(route_counts, ensure_ascii=False)))
qa_rows.append((RUN_ID, PHASE, "summary", json.dumps(preflight_summary, ensure_ascii=False, sort_keys=True)))
qa_df = spark.createDataFrame(qa_rows, "run_id string, phase string, metric string, value_json string")
write_table(qa_df, preflight_table)

print(json.dumps(preflight_summary, indent=2, ensure_ascii=False, sort_keys=True))

if PHASE == "preflight_only":
    routed_without_segments.unpersist()
    routed.unpersist()
    dbutils.notebook.exit(json.dumps(preflight_summary, ensure_ascii=False))

panel_limit = str(MAX_ROUTED_CHANNELS)
models_json = json.dumps([{"provider": "deepseek", "model": "deepseek-v4-flash", "tier": "small"}])
llm_args: Dict[str, str] = {
    "catalog": CATALOG,
    "schema": SCHEMA,
    "comparison_table": comparison_table,
    "segments_input_table": segments_input_table,
    "channels_table": channels_table,
    "channel_text_features_table": channel_text_features_table,
    "hindi_indic_audit_table": hindi_indic_audit_table,
    "source_channels_table": source_channels_for_panel,
    "source_videos_table": source_videos_for_panel,
    "run_id": RUN_ID,
    "source_run_id": SOURCE_RUN_ID,
    "inference_hash_buckets": str(INFERENCE_HASH_BUCKETS),
    "panel_requests_table": requests_table,
    "panel_batch_jobs_table": batch_jobs_table,
    "panel_raw_results_table": raw_results_table,
    "panel_verdicts_table": verdicts_table,
    "panel_model_agreement_table": model_agreement_table,
    "panel_run_progress_table": progress_table,
    "routing_mode": "residual_panel",
    "route_disagreement": str(ROUTE_DISAGREEMENT).lower(),
    "route_unresolved_tail": "false",
    "route_shared_bias_english_indic": "false",
    "route_unclassified": str(ROUTE_UNCLASSIFIED).lower(),
    "route_agreement_audit": "false",
    "exclude_arabic_family_pairs": str(EXCLUDE_ARABIC_FAMILY_PAIRS).lower(),
    "max_routed_channels": panel_limit,
    "models_json": models_json,
    "max_output_tokens": "2000",
    "temperature": "",
    "prompt_version": PROMPT_VERSION,
    "apply_llm_calibration": "true",
    "deepseek_thinking_type": "disabled",
    "deepseek_reasoning_effort": "",
    "deepseek_max_output_tokens": "600",
    "deepseek_max_workers": DEEPSEEK_MAX_WORKERS,
    "deepseek_request_timeout_seconds": DEEPSEEK_REQUEST_TIMEOUT_SECONDS,
    "deepseek_max_retries": DEEPSEEK_MAX_RETRIES,
    "deepseek_direct_streaming": DEEPSEEK_DIRECT_STREAMING,
    "deepseek_delete_request_jsonl_after_submit": DEEPSEEK_DELETE_REQUEST_JSONL_AFTER_SUBMIT,
    "deepseek_direct_submit_from_requests_table": DEEPSEEK_DIRECT_SUBMIT_FROM_REQUESTS_TABLE,
    "submit_batches": str(SUBMIT_BATCHES).lower(),
    "submit_provider_filter": "deepseek",
    "submit_model_filter": "deepseek-v4-flash",
    "skip_existing_submitted_batches": "true",
    "import_results": str(IMPORT_RESULTS).lower(),
    "reuse_existing_requests_on_submit": "true",
    "reuse_existing_requests_on_import": "true",
    "panel_majority_mode": "reached_models",
    "min_panel_votes_for_majority": "1",
    "panel_majority_vote_basis": "normalized_base_iso",
    "secret_scope": "youtube-llm-keys",
    "deepseek_secret_key": "deepseek-api-key",
}

print("Invoking LLM notebook:", LLM_NOTEBOOK_PATH)
print("Panel arguments:", json.dumps(llm_args, indent=2, sort_keys=True))
llm_result = dbutils.notebook.run(LLM_NOTEBOOK_PATH, 0, llm_args)
print("LLM notebook result:", llm_result)

requests_count = spark.table(fqtn(requests_table)).where(F.col("run_id") == F.lit(RUN_ID)).count()
raw_count = spark.table(fqtn(raw_results_table)).where(F.col("run_id") == F.lit(RUN_ID)).count()
verdicts_count = spark.table(fqtn(verdicts_table)).where(F.col("run_id") == F.lit(RUN_ID)).count()

expected_routed = MAX_ROUTED_CHANNELS if MAX_ROUTED_CHANNELS > 0 else routed_total
run_summary = dict(preflight_summary)
run_summary["llm_notebook_result"] = llm_result
run_summary["counts"] = dict(preflight_summary["counts"])
run_summary["counts"].update(
    {
        "max_routed_channels": MAX_ROUTED_CHANNELS,
        "expected_routed_for_phase": expected_routed,
        "requests": requests_count,
        "raw_results": raw_count,
        "verdicts": verdicts_count,
    }
)
run_summary["output_tables"] = {
    "requests": fqtn(requests_table),
    "batch_jobs": fqtn(batch_jobs_table),
    "raw_results": fqtn(raw_results_table),
    "verdicts": fqtn(verdicts_table),
    "model_agreement": fqtn(model_agreement_table),
    "progress": fqtn(progress_table),
}

if requests_count != expected_routed:
    raise RuntimeError(f"Request count mismatch: requests={requests_count:,}, expected={expected_routed:,}")
if raw_count != expected_routed:
    raise RuntimeError(f"Raw result count mismatch: raw_results={raw_count:,}, expected={expected_routed:,}")
if verdicts_count != expected_routed:
    raise RuntimeError(f"Verdict count mismatch: verdicts={verdicts_count:,}, expected={expected_routed:,}")

qa_rows.append((RUN_ID, PHASE, "run_summary", json.dumps(run_summary, ensure_ascii=False, sort_keys=True)))
write_table(spark.createDataFrame(qa_rows, "run_id string, phase string, metric string, value_json string"), preflight_table)

routed_without_segments.unpersist()
routed.unpersist()
print(json.dumps(run_summary, indent=2, ensure_ascii=False, sort_keys=True))
dbutils.notebook.exit(json.dumps(run_summary, ensure_ascii=False))
