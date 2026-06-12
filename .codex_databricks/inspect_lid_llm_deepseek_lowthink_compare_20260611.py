# Databricks notebook source
import json
import os
from typing import Any

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


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
_create_text_widget("baseline_run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
_create_text_widget("thinking_run_id", "too_full_20260609_lid_iso_disagree_1k_deepseek_lowthink_20260611")
_create_text_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
_create_text_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
_create_text_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results")
_create_text_widget("min_majority_votes", "2")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
BASELINE_RUN_ID = _get_widget("baseline_run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
THINKING_RUN_ID = _get_widget("thinking_run_id", "too_full_20260609_lid_iso_disagree_1k_deepseek_lowthink_20260611")
REQUESTS_TABLE = _get_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results").rstrip("/")
MIN_MAJORITY_VOTES = int(_get_widget("min_majority_votes", "2"))

DEEPSEEK_PRICES_PER_1M = {
    "deepseek-v4-flash": {"input_cache_hit": 0.0028, "input_cache_miss": 0.14, "output": 0.28},
    "deepseek-v4-pro": {"input_cache_hit": 0.003625, "input_cache_miss": 0.435, "output": 0.87},
}

ARABIC_FAMILY_ISO = {"ara", "arb", "ary", "arz", "arq", "apc", "ars", "ajp", "aeb", "acm", "acq", "aec", "afb", "ayl", "ayn"}
CANONICAL_BASE_ISO = {
    "arabic": "ara",
    "arb": "ara",
    "ary": "ara",
    "arz": "ara",
    "arq": "ara",
    "apc": "ara",
    "ars": "ara",
    "ajp": "ara",
    "aeb": "ara",
    "acm": "ara",
    "acq": "ara",
    "aec": "ara",
    "afb": "ara",
    "ayl": "ara",
    "ayn": "ara",
    "chinese": "cmn",
    "mandarin": "cmn",
    "zho": "cmn",
    "cmn": "cmn",
    "tagalog": "fil",
    "filipino": "fil",
    "tgl": "fil",
    "fil": "fil",
    "odia": "ory",
    "ori": "ory",
    "ory": "ory",
    "uzbek": "uzb",
    "uzn": "uzb",
    "uzb": "uzb",
    "malay": "zsm",
    "msa": "zsm",
    "zsm": "zsm",
    "nepali": "npi",
    "nep": "npi",
    "npi": "npi",
    "kurdish": "kmr",
    "kur": "kmr",
    "ku": "kmr",
    "hindi": "hin",
    "korean": "kor",
    "punjabi": "pan",
    "cantonese": "yue",
}


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def _path_to_spark(path: str) -> str:
    if path.startswith("/dbfs/"):
        return "dbfs:/" + path[len("/dbfs/"):]
    return path


def _dig(obj: Any, path, default=None):
    cur = obj
    for part in path:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def _int_or_none(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _float_or_none(value):
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def canonical_base_iso_expr(col):
    iso = F.lower(F.trim(col.cast("string")))
    iso = F.when(iso.isin("", "null", "none"), F.lit(None).cast("string")).otherwise(iso)
    iso = F.when(iso.isin(*sorted(ARABIC_FAMILY_ISO)), F.lit("ara")).otherwise(iso)
    mapping = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in CANONICAL_BASE_ISO.items()], []))
    return F.coalesce(F.element_at(mapping, iso), iso)


usage_schema = StructType([
    StructField("request_id", StringType(), True),
    StructField("provider_result_model", StringType(), True),
    StructField("status_code", StringType(), True),
    StructField("thinking_type", StringType(), True),
    StructField("reasoning_effort", StringType(), True),
    StructField("duration_ms", DoubleType(), True),
    StructField("attempts", LongType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("deepseek_cache_hit_tokens", LongType(), True),
    StructField("deepseek_cache_miss_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("reasoning_output_tokens", LongType(), True),
    StructField("total_tokens", LongType(), True),
    StructField("has_usage", BooleanType(), True),
    StructField("parse_error", StringType(), True),
])


@F.udf(usage_schema)
def parse_result_line_udf(line: str):
    try:
        obj = json.loads(line)
    except Exception as exc:
        return (None, None, None, None, None, None, None, None, None, None, None, None, None, False, repr(exc)[:300])

    request_id = obj.get("custom_id") or obj.get("key") or obj.get("id")
    body = _dig(obj, ["response", "body"])
    status_code = _dig(obj, ["response", "status_code"])
    provider_result_model = body.get("model") if isinstance(body, dict) else None
    usage = body.get("usage") if isinstance(body, dict) else None
    metadata = obj.get("_deepseek_direct_metadata") if isinstance(obj.get("_deepseek_direct_metadata"), dict) else {}

    if not isinstance(usage, dict):
        return (
            request_id,
            provider_result_model,
            str(status_code) if status_code is not None else None,
            metadata.get("thinking_type"),
            metadata.get("reasoning_effort"),
            _float_or_none(metadata.get("duration_ms")),
            _int_or_none(metadata.get("attempts")),
            None,
            None,
            None,
            None,
            None,
            None,
            False,
            None,
        )

    hit = _int_or_none(usage.get("prompt_cache_hit_tokens"))
    miss = _int_or_none(usage.get("prompt_cache_miss_tokens"))
    input_tokens = _int_or_none(usage.get("prompt_tokens")) or _int_or_none(usage.get("input_tokens"))
    if input_tokens is None and hit is not None and miss is not None:
        input_tokens = hit + miss
    output_tokens = _int_or_none(usage.get("completion_tokens")) or _int_or_none(usage.get("output_tokens"))
    reasoning_output_tokens = (
        _int_or_none(_dig(usage, ["completion_tokens_details", "reasoning_tokens"]))
        or _int_or_none(_dig(usage, ["output_tokens_details", "reasoning_tokens"]))
    )
    total_tokens = _int_or_none(usage.get("total_tokens"))

    return (
        request_id,
        provider_result_model,
        str(status_code) if status_code is not None else None,
        metadata.get("thinking_type"),
        metadata.get("reasoning_effort"),
        _float_or_none(metadata.get("duration_ms")),
        _int_or_none(metadata.get("attempts")),
        input_tokens,
        hit,
        miss,
        output_tokens,
        reasoning_output_tokens,
        total_tokens,
        True,
        None,
    )


def rows(df):
    return [row.asDict(recursive=True) for row in df.collect()]


requests = (
    spark.table(fqtn(REQUESTS_TABLE))
    .where(F.col("run_id").isin(BASELINE_RUN_ID, THINKING_RUN_ID))
    .select("run_id", "request_id", "provider", "model", "model_tier", "channel_id")
)

sample_overlap = (
    requests.where(F.col("run_id") == F.lit(BASELINE_RUN_ID)).select("channel_id").distinct()
    .intersect(requests.where(F.col("run_id") == F.lit(THINKING_RUN_ID)).select("channel_id").distinct())
    .persist()
)

raw = spark.table(fqtn(RAW_RESULTS_TABLE)).where(F.col("run_id").isin(BASELINE_RUN_ID, THINKING_RUN_ID))
votes = (
    raw.join(sample_overlap, on="channel_id", how="inner")
    .where(F.col("is_valid_panel_vote") == F.lit(True))
    .select(
        "run_id",
        "channel_id",
        F.lower(F.col("provider")).alias("provider"),
        "model",
        "model_tier",
        canonical_base_iso_expr(F.coalesce(F.col("pred_normalized_base_iso"), F.col("pred_base_iso"))).alias("normalized_base_iso"),
    )
    .where(F.col("normalized_base_iso").isNotNull())
    .withColumn(
        "setting",
        F.when(F.col("run_id") == F.lit(THINKING_RUN_ID), F.lit("low_thinking"))
        .when((F.col("run_id") == F.lit(BASELINE_RUN_ID)) & (F.col("provider") == F.lit("deepseek")), F.lit("no_thinking"))
        .otherwise(F.lit("baseline")),
    )
    .withColumn("model_key", F.concat_ws(":", F.col("provider"), F.col("model"), F.col("setting")))
)

valid_counts = (
    raw.join(sample_overlap, on="channel_id", how="inner")
    .groupBy("run_id", F.lower(F.col("provider")).alias("provider"), "model", "model_tier")
    .agg(
        F.count(F.lit(1)).alias("n_results"),
        F.sum(F.when(F.col("is_valid_panel_vote") == F.lit(True), 1).otherwise(0)).alias("n_valid"),
        F.sum(F.when(F.col("parse_error").isNotNull() | F.col("prediction_parse_error").isNotNull(), 1).otherwise(0)).alias("n_parse_errors"),
        F.sum(F.when(F.col("result_status").cast("string").rlike("^[45][0-9][0-9]$"), 1).otherwise(0)).alias("n_http_errors"),
    )
    .orderBy("run_id", "provider", "model")
)

deepseek_baseline = votes.where((F.col("run_id") == F.lit(BASELINE_RUN_ID)) & (F.col("provider") == F.lit("deepseek"))).alias("base")
deepseek_thinking = votes.where((F.col("run_id") == F.lit(THINKING_RUN_ID)) & (F.col("provider") == F.lit("deepseek"))).alias("think")
deepseek_same_model = (
    deepseek_baseline.join(deepseek_thinking, on=["channel_id", "provider", "model"], how="inner")
    .groupBy("provider", "model")
    .agg(
        F.count(F.lit(1)).alias("n_shared_valid"),
        F.sum(F.when(F.col("base.normalized_base_iso") == F.col("think.normalized_base_iso"), 1).otherwise(0)).alias("n_same_iso"),
    )
    .withColumn("same_iso_rate", F.round(F.col("n_same_iso") / F.col("n_shared_valid"), 4))
    .withColumn("n_changed_iso", F.col("n_shared_valid") - F.col("n_same_iso"))
    .orderBy("model")
)

baseline_non_deepseek = votes.where((F.col("run_id") == F.lit(BASELINE_RUN_ID)) & (F.col("provider") != F.lit("deepseek"))).persist()
label_counts = baseline_non_deepseek.groupBy("channel_id", "normalized_base_iso").agg(F.count(F.lit(1)).alias("n_votes"))
channel_model_counts = baseline_non_deepseek.groupBy("channel_id").agg(F.countDistinct("model_key").alias("n_models"))
w_majority = Window.partitionBy("channel_id").orderBy(F.desc("n_votes"), F.asc("normalized_base_iso"))
top_labels = label_counts.withColumn("top_rank", F.dense_rank().over(w_majority)).where(F.col("top_rank") == 1)
majority = (
    top_labels.groupBy("channel_id")
    .agg(
        F.count(F.lit(1)).alias("n_top_labels"),
        F.first("normalized_base_iso").alias("majority_iso"),
        F.first("n_votes").alias("n_majority_votes"),
    )
    .join(channel_model_counts, on="channel_id", how="left")
    .where((F.col("n_top_labels") == F.lit(1)) & (F.col("n_majority_votes") >= F.lit(MIN_MAJORITY_VOTES)))
    .select("channel_id", "majority_iso", "n_majority_votes", "n_models")
    .persist()
)

deepseek_to_majority = (
    votes.where(F.col("provider") == F.lit("deepseek"))
    .join(majority, on="channel_id", how="inner")
    .groupBy("run_id", "model", "setting")
    .agg(
        F.count(F.lit(1)).alias("n_with_non_deepseek_majority"),
        F.sum(F.when(F.col("normalized_base_iso") == F.col("majority_iso"), 1).otherwise(0)).alias("n_agree_majority"),
        F.avg("n_majority_votes").alias("avg_majority_votes"),
        F.avg("n_models").alias("avg_non_deepseek_models_voting"),
    )
    .withColumn("majority_agreement_rate", F.round(F.col("n_agree_majority") / F.col("n_with_non_deepseek_majority"), 4))
    .orderBy("model", "setting")
)

thinking_vs_baseline = (
    deepseek_thinking.alias("think")
    .join(votes.where(F.col("run_id") == F.lit(BASELINE_RUN_ID)).alias("base"), on="channel_id", how="inner")
    .where(F.col("think.model_key") != F.col("base.model_key"))
    .groupBy(
        F.col("think.model").alias("thinking_model"),
        F.col("base.provider").alias("baseline_provider"),
        F.col("base.model").alias("baseline_model"),
        F.col("base.setting").alias("baseline_setting"),
    )
    .agg(
        F.count(F.lit(1)).alias("n_shared_valid"),
        F.sum(F.when(F.col("think.normalized_base_iso") == F.col("base.normalized_base_iso"), 1).otherwise(0)).alias("n_agree"),
    )
    .withColumn("agreement_rate", F.round(F.col("n_agree") / F.col("n_shared_valid"), 4))
)

nothinking_vs_baseline = (
    deepseek_baseline.alias("ds")
    .join(votes.where((F.col("run_id") == F.lit(BASELINE_RUN_ID)) & (F.col("provider") != F.lit("deepseek"))).alias("base"), on="channel_id", how="inner")
    .groupBy(
        F.col("ds.model").alias("deepseek_model"),
        F.col("base.provider").alias("baseline_provider"),
        F.col("base.model").alias("baseline_model"),
    )
    .agg(
        F.count(F.lit(1)).alias("n_shared_valid"),
        F.sum(F.when(F.col("ds.normalized_base_iso") == F.col("base.normalized_base_iso"), 1).otherwise(0)).alias("n_agree"),
    )
    .withColumn("agreement_rate", F.round(F.col("n_agree") / F.col("n_shared_valid"), 4))
)

agreement_delta = (
    thinking_vs_baseline.where(F.col("baseline_provider") != F.lit("deepseek")).alias("t")
    .join(
        nothinking_vs_baseline.alias("n"),
        (F.col("t.thinking_model") == F.col("n.deepseek_model"))
        & (F.col("t.baseline_provider") == F.col("n.baseline_provider"))
        & (F.col("t.baseline_model") == F.col("n.baseline_model")),
        how="left",
    )
    .select(
        F.col("t.thinking_model").alias("deepseek_model"),
        F.col("t.baseline_provider"),
        F.col("t.baseline_model"),
        F.col("t.n_shared_valid").alias("lowthink_shared_valid"),
        F.col("t.agreement_rate").alias("lowthink_agreement_rate"),
        F.col("n.n_shared_valid").alias("nothinking_shared_valid"),
        F.col("n.agreement_rate").alias("nothinking_agreement_rate"),
        F.round(F.col("t.agreement_rate") - F.col("n.agreement_rate"), 4).alias("lowthink_minus_nothinking"),
    )
    .orderBy("deepseek_model", "baseline_provider", "baseline_model")
)


def read_result_lines(run_id: str):
    path = _path_to_spark(f"{RESULTS_INPUT_DIR}/{run_id}/deepseek")
    try:
        return (
            spark.read.option("recursiveFileLookup", "true").text(path)
            .withColumnRenamed("value", "line")
            .where(F.length(F.trim(F.col("line"))) > 2)
        )
    except Exception as exc:
        print(f"Could not read {path}: {exc}")
        return None


raw_lines = None
for run_id in [BASELINE_RUN_ID, THINKING_RUN_ID]:
    part = read_result_lines(run_id)
    if part is not None:
        part = part.withColumn("source_run_id", F.lit(run_id))
        raw_lines = part if raw_lines is None else raw_lines.unionByName(part)

if raw_lines is None:
    usage_cost = spark.createDataFrame([], StructType([
        StructField("run_id", StringType(), True),
        StructField("model", StringType(), True),
        StructField("n_usage_rows", LongType(), True),
    ]))
else:
    parsed = raw_lines.withColumn("u", parse_result_line_udf(F.col("line"))).select("source_run_id", "u.*")
    usage_joined = (
        parsed
        .join(requests.where(F.lower(F.col("provider")) == F.lit("deepseek")), on="request_id", how="inner")
        .dropDuplicates(["request_id"])
        .withColumn("setting", F.when(F.col("run_id") == F.lit(THINKING_RUN_ID), F.lit("low_thinking")).otherwise(F.lit("no_thinking")))
    )
    usage_cost = (
        usage_joined.groupBy("run_id", "model", "setting", "thinking_type", "reasoning_effort")
        .agg(
            F.count(F.lit(1)).alias("n_usage_rows"),
            F.sum(F.when(F.col("has_usage"), 1).otherwise(0)).alias("n_rows_with_usage"),
            F.sum("input_tokens").alias("input_tokens"),
            F.sum(F.coalesce(F.col("deepseek_cache_hit_tokens"), F.lit(0))).alias("cache_hit_input_tokens"),
            F.sum(F.coalesce(F.col("deepseek_cache_miss_tokens"), F.lit(0))).alias("cache_miss_input_tokens"),
            F.sum("output_tokens").alias("output_tokens"),
            F.sum(F.coalesce(F.col("reasoning_output_tokens"), F.lit(0))).alias("reasoning_output_tokens"),
            F.sum("total_tokens").alias("total_tokens"),
            F.avg("duration_ms").alias("avg_request_duration_ms"),
            F.expr("percentile_approx(duration_ms, 0.50)").alias("p50_request_duration_ms"),
            F.expr("percentile_approx(duration_ms, 0.95)").alias("p95_request_duration_ms"),
            F.max("duration_ms").alias("max_request_duration_ms"),
            F.avg("attempts").alias("avg_attempts"),
        )
    )

    price_rows = spark.createDataFrame(
        [(model, p["input_cache_hit"], p["input_cache_miss"], p["output"]) for model, p in DEEPSEEK_PRICES_PER_1M.items()],
        ["model", "price_input_cache_hit_per_1m", "price_input_cache_miss_per_1m", "price_output_per_1m"],
    )
    usage_cost = (
        usage_cost.join(price_rows, on="model", how="left")
        .withColumn(
            "estimated_cost_usd",
            (
                F.coalesce(F.col("cache_hit_input_tokens"), F.lit(0)) * F.col("price_input_cache_hit_per_1m")
                + F.coalesce(F.col("cache_miss_input_tokens"), F.lit(0)) * F.col("price_input_cache_miss_per_1m")
                + F.coalesce(F.col("output_tokens"), F.lit(0)) * F.col("price_output_per_1m")
            ) / F.lit(1000000.0),
        )
        .withColumn("cost_per_1000_usage_rows_usd", F.round(F.col("estimated_cost_usd") * F.lit(1000.0) / F.col("n_usage_rows"), 6))
        .withColumn("avg_request_duration_ms", F.round("avg_request_duration_ms", 1))
        .withColumn("p50_request_duration_ms", F.round("p50_request_duration_ms", 1))
        .withColumn("p95_request_duration_ms", F.round("p95_request_duration_ms", 1))
        .withColumn("max_request_duration_ms", F.round("max_request_duration_ms", 1))
        .withColumn("avg_attempts", F.round("avg_attempts", 3))
        .orderBy("model", "setting")
    )


def parse_ts_expr(col):
    return F.coalesce(
        F.to_timestamp(col),
        F.to_timestamp(F.regexp_replace(col, "Z$", "+00:00")),
        F.to_timestamp(F.regexp_replace(col, r"\\+00:00$", "")),
    )


job_timing = (
    spark.table(fqtn(BATCH_JOBS_TABLE))
    .where((F.col("run_id").isin(BASELINE_RUN_ID, THINKING_RUN_ID)) & (F.lower(F.col("provider")) == F.lit("deepseek")))
    .withColumn("setting", F.when(F.col("run_id") == F.lit(THINKING_RUN_ID), F.lit("low_thinking")).otherwise(F.lit("no_thinking")))
    .withColumn("_submitted_ts", parse_ts_expr(F.col("submitted_at_utc")))
    .withColumn("_recorded_ts", parse_ts_expr(F.col("recorded_at_utc")))
    .withColumn("observed_wall_seconds", F.unix_timestamp("_recorded_ts") - F.unix_timestamp("_submitted_ts"))
    .groupBy("run_id", "model", "setting")
    .agg(
        F.sum("n_requests").alias("n_job_requests"),
        F.max("observed_wall_seconds").alias("observed_wall_seconds"),
        F.collect_set("provider_status").alias("provider_statuses"),
        F.collect_set("submission_status").alias("submission_statuses"),
    )
    .withColumn("wall_seconds_per_1000_requests", F.round(F.col("observed_wall_seconds") * F.lit(1000.0) / F.col("n_job_requests"), 1))
    .orderBy("model", "setting")
)

result = {
    "source": {
        "baseline_run_id": BASELINE_RUN_ID,
        "thinking_run_id": THINKING_RUN_ID,
        "sample_overlap_channels": sample_overlap.count(),
        "min_majority_votes": MIN_MAJORITY_VOTES,
        "deepseek_price_source": "https://api-docs.deepseek.com/quick_start/pricing",
        "deepseek_prices_per_1m_tokens_usd": DEEPSEEK_PRICES_PER_1M,
        "thinking_mode_note": "DeepSeek docs accept reasoning_effort=low for compatibility but map low/medium to high.",
    },
    "valid_counts": rows(valid_counts),
    "deepseek_lowthinking_vs_nothinking_same_model": rows(deepseek_same_model),
    "deepseek_vs_non_deepseek_majority": rows(deepseek_to_majority),
    "deepseek_vs_other_models_agreement_delta": rows(agreement_delta),
    "deepseek_usage_cost": rows(usage_cost),
    "deepseek_job_timing": rows(job_timing),
}

print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
