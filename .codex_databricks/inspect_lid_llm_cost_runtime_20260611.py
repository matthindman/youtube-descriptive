# Databricks notebook source
import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
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
_create_text_widget("run_ids", "too_full_20260609,too_full_20260609_retry_incomplete_20260611")
_create_text_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
_create_text_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
_create_text_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
_create_text_widget("batch_status_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")
_create_text_widget("result_files_table", "yt_lid_v3_too_full_20260609_llm_validation_result_files")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_IDS = [r.strip() for r in _get_widget("run_ids", "").split(",") if r.strip()]
REQUESTS_TABLE = _get_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
BATCH_STATUS_TABLE = _get_widget("batch_status_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")
RESULT_FILES_TABLE = _get_widget("result_files_table", "yt_lid_v3_too_full_20260609_llm_validation_result_files")
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results").rstrip("/")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def _table_exists(table: str) -> bool:
    try:
        spark.table(fqtn(table)).limit(0)
        return True
    except Exception:
        return False


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


usage_schema = StructType([
    StructField("request_id", StringType(), True),
    StructField("provider_result_model", StringType(), True),
    StructField("status_code", StringType(), True),
    StructField("usage_json", StringType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("cached_input_tokens", LongType(), True),
    StructField("cache_creation_input_tokens", LongType(), True),
    StructField("deepseek_cache_hit_tokens", LongType(), True),
    StructField("deepseek_cache_miss_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("reasoning_output_tokens", LongType(), True),
    StructField("total_tokens", LongType(), True),
    StructField("has_usage", BooleanType(), True),
    StructField("usage_parse_error", StringType(), True),
])


def _int_or_none(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


@F.udf(usage_schema)
def parse_usage_udf(line: str):
    try:
        obj = json.loads(line)
    except Exception as exc:
        return (None, None, None, None, None, None, None, None, None, None, None, None, False, repr(exc)[:300])

    rid = obj.get("custom_id") or obj.get("key") or obj.get("id")
    body = _dig(obj, ["response", "body"])
    status_code = _dig(obj, ["response", "status_code"])
    provider_result_model = None
    usage = None

    if isinstance(body, dict):
        provider_result_model = body.get("model") or body.get("modelVersion")
        usage = body.get("usage") or body.get("usageMetadata")

    if usage is None and isinstance(obj.get("result"), dict):
        result = obj["result"]
        msg = result.get("message") if isinstance(result.get("message"), dict) else {}
        provider_result_model = provider_result_model or msg.get("model")
        usage = msg.get("usage") or result.get("usage")
        status_code = status_code or result.get("type")

    if usage is None:
        usage = obj.get("usage") or obj.get("usageMetadata") or _dig(obj, ["response", "usageMetadata"])
        provider_result_model = provider_result_model or obj.get("modelVersion") or _dig(obj, ["response", "modelVersion"])
        status_code = status_code or obj.get("status")

    if not isinstance(usage, dict):
        return (rid, provider_result_model, str(status_code) if status_code is not None else None, None,
                None, None, None, None, None, None, None, None, False, None)

    input_tokens = (
        _int_or_none(usage.get("input_tokens"))
        or _int_or_none(usage.get("prompt_tokens"))
        or _int_or_none(usage.get("promptTokenCount"))
    )
    output_tokens = (
        _int_or_none(usage.get("output_tokens"))
        or _int_or_none(usage.get("completion_tokens"))
        or _int_or_none(usage.get("candidatesTokenCount"))
    )
    reasoning_output_tokens = (
        _int_or_none(_dig(usage, ["output_tokens_details", "reasoning_tokens"]))
        or _int_or_none(_dig(usage, ["completion_tokens_details", "reasoning_tokens"]))
        or _int_or_none(usage.get("thoughtsTokenCount"))
    )
    if usage.get("thoughtsTokenCount") is not None:
        output_tokens = int(output_tokens or 0) + int(usage.get("thoughtsTokenCount") or 0)

    total_tokens = (
        _int_or_none(usage.get("total_tokens"))
        or _int_or_none(usage.get("totalTokenCount"))
    )
    cached_input_tokens = (
        _int_or_none(usage.get("cache_read_input_tokens"))
        or _int_or_none(_dig(usage, ["input_tokens_details", "cached_tokens"]))
        or _int_or_none(_dig(usage, ["prompt_tokens_details", "cached_tokens"]))
    )
    cache_creation_input_tokens = _int_or_none(usage.get("cache_creation_input_tokens"))
    deepseek_cache_hit_tokens = _int_or_none(usage.get("prompt_cache_hit_tokens"))
    deepseek_cache_miss_tokens = _int_or_none(usage.get("prompt_cache_miss_tokens"))
    if input_tokens is None and deepseek_cache_hit_tokens is not None and deepseek_cache_miss_tokens is not None:
        input_tokens = deepseek_cache_hit_tokens + deepseek_cache_miss_tokens

    return (
        rid,
        provider_result_model,
        str(status_code) if status_code is not None else None,
        json.dumps(usage, ensure_ascii=False, sort_keys=True),
        input_tokens,
        cached_input_tokens,
        cache_creation_input_tokens,
        deepseek_cache_hit_tokens,
        deepseek_cache_miss_tokens,
        output_tokens,
        reasoning_output_tokens,
        total_tokens,
        True,
        None,
    )


requests = (
    spark.table(fqtn(REQUESTS_TABLE))
    .where(F.col("run_id").isin(RUN_IDS))
    .select(
        "run_id", "request_id", "provider", "model", "model_tier", "channel_id",
        F.length("system_prompt").alias("system_prompt_chars"),
        F.length("prompt_user").alias("user_prompt_chars"),
        "max_output_tokens",
    )
)

raw_results = (
    spark.table(fqtn(RAW_RESULTS_TABLE))
    .where(F.col("run_id").isin(RUN_IDS))
    .groupBy("run_id", "provider", "model", "model_tier")
    .agg(
        F.count(F.lit(1)).alias("n_results"),
        F.sum(F.when(F.col("is_valid_panel_vote") == F.lit(True), 1).otherwise(0)).alias("n_valid_codes"),
        F.sum(F.when(F.col("parse_error").isNotNull() | F.col("prediction_parse_error").isNotNull(), 1).otherwise(0)).alias("n_parse_errors"),
        F.sum(F.when(F.col("result_status").cast("string").rlike("^[45][0-9][0-9]$"), 1).otherwise(0)).alias("n_http_errors"),
    )
)

request_summary = (
    requests.groupBy("run_id", "provider", "model", "model_tier")
    .agg(
        F.count(F.lit(1)).alias("n_requested"),
        F.countDistinct("channel_id").alias("n_requested_channels"),
        F.sum("system_prompt_chars").alias("system_prompt_chars"),
        F.sum("user_prompt_chars").alias("user_prompt_chars"),
        F.sum(F.col("system_prompt_chars") + F.col("user_prompt_chars")).alias("total_prompt_chars"),
        F.avg(F.col("system_prompt_chars") + F.col("user_prompt_chars")).alias("avg_prompt_chars"),
    )
)

result_dirs = [_path_to_spark(f"{RESULTS_INPUT_DIR}/{run_id}") for run_id in RUN_IDS]
raw_lines = None
for path in result_dirs:
    try:
        part = (
            spark.read.option("recursiveFileLookup", "true").text(path)
            .withColumnRenamed("value", "line")
            .where(F.length(F.trim(F.col("line"))) > 2)
        )
        raw_lines = part if raw_lines is None else raw_lines.unionByName(part)
    except Exception as exc:
        print(f"Could not read result path {path}: {exc}")

if raw_lines is None:
    usage_by_model = requests.limit(0).select("run_id", "provider", "model", "model_tier").withColumn("n_usage_rows", F.lit(0))
    usage_examples = []
else:
    usage_rows = raw_lines.withColumn("u", parse_usage_udf(F.col("line"))).select("u.*")
    usage_joined = (
        usage_rows
        .join(requests.select("run_id", "request_id", "provider", "model", "model_tier", "channel_id"), on="request_id", how="inner")
        .dropDuplicates(["request_id"])
    )
    usage_by_model = (
        usage_joined.groupBy("run_id", "provider", "model", "model_tier")
        .agg(
            F.count(F.lit(1)).alias("n_usage_rows"),
            F.sum(F.when(F.col("has_usage"), 1).otherwise(0)).alias("n_rows_with_usage"),
            F.sum("input_tokens").alias("input_tokens"),
            F.sum(F.coalesce(F.col("cached_input_tokens"), F.lit(0))).alias("cached_input_tokens"),
            F.sum(F.coalesce(F.col("cache_creation_input_tokens"), F.lit(0))).alias("cache_creation_input_tokens"),
            F.sum(F.coalesce(F.col("deepseek_cache_hit_tokens"), F.lit(0))).alias("deepseek_cache_hit_tokens"),
            F.sum(F.coalesce(F.col("deepseek_cache_miss_tokens"), F.lit(0))).alias("deepseek_cache_miss_tokens"),
            F.sum("output_tokens").alias("output_tokens"),
            F.sum(F.coalesce(F.col("reasoning_output_tokens"), F.lit(0))).alias("reasoning_output_tokens"),
            F.sum("total_tokens").alias("total_tokens"),
            F.avg("input_tokens").alias("avg_input_tokens"),
            F.avg("output_tokens").alias("avg_output_tokens"),
        )
    )
    usage_examples = [
        row.asDict(recursive=True)
        for row in usage_joined.where(F.col("has_usage")).select(
            "run_id", "provider", "model", "usage_json"
        ).dropDuplicates(["run_id", "provider", "model"]).orderBy("run_id", "provider", "model").collect()
    ]

jobs = (
    spark.table(fqtn(BATCH_JOBS_TABLE))
    .where(F.col("run_id").isin(RUN_IDS))
    .select(
        "run_id", "provider", "model", "chunk_id", "n_requests", "n_bytes",
        "provider_file_id", "provider_batch_id", "provider_status", "submission_status",
        "submitted_at_utc", "recorded_at_utc", "submission_error",
    )
)

status_rows = None
if _table_exists(BATCH_STATUS_TABLE):
    status_raw = spark.table(fqtn(BATCH_STATUS_TABLE)).where(F.col("run_id").isin(RUN_IDS))
    w_status = Window.partitionBy("run_id", "provider", "model", "chunk_id", "provider_batch_id").orderBy(F.desc("checked_at_utc"))
    status_rows = (
        status_raw
        .withColumn("_rank", F.row_number().over(w_status))
        .where(F.col("_rank") == 1)
        .drop("_rank")
        .select(
            "run_id", "provider", "model", "chunk_id", "provider_batch_id", "checked_at_utc",
            F.col("provider_status").alias("latest_provider_status"),
            "request_counts_json", "result_counts_json", "output_ref_json", "status_error",
        )
    )


def _parse_ts_expr(col):
    return F.coalesce(
        F.to_timestamp(col),
        F.to_timestamp(F.regexp_replace(col, "Z$", "+00:00")),
        F.to_timestamp(F.regexp_replace(col, r"\\+00:00$", "")),
    )


timing = jobs
if status_rows is not None:
    timing = timing.join(
        status_rows,
        on=["run_id", "provider", "model", "chunk_id", "provider_batch_id"],
        how="left",
    )
else:
    timing = timing.withColumn("checked_at_utc", F.lit(None).cast("string")).withColumn("request_counts_json", F.lit(None).cast("string"))

timing = (
    timing
    .withColumn("_submitted_ts", _parse_ts_expr(F.col("submitted_at_utc")))
    .withColumn("_recorded_ts", _parse_ts_expr(F.col("recorded_at_utc")))
    .withColumn("_checked_ts", _parse_ts_expr(F.col("checked_at_utc")))
    .withColumn("_request_counts", F.from_json("request_counts_json", "map<string,string>"))
    .withColumn("_openai_created_at", F.element_at("_request_counts", "created_at").cast("double"))
    .withColumn("_openai_completed_at", F.element_at("_request_counts", "completed_at").cast("double"))
    .withColumn("_gemini_create_time", _parse_ts_expr(F.element_at("_request_counts", "create_time")))
    .withColumn("_gemini_end_time", _parse_ts_expr(F.element_at("_request_counts", "end_time")))
    .withColumn(
        "observed_wall_seconds",
        F.when(
            F.col("provider") == F.lit("openai"),
            F.col("_openai_completed_at") - F.col("_openai_created_at"),
        ).when(
            F.col("provider") == F.lit("gemini"),
            F.unix_timestamp("_gemini_end_time") - F.unix_timestamp("_gemini_create_time"),
        ).when(
            F.col("provider") == F.lit("deepseek"),
            F.unix_timestamp("_recorded_ts") - F.unix_timestamp("_submitted_ts"),
        ).otherwise(
            F.unix_timestamp("_checked_ts") - F.unix_timestamp("_submitted_ts"),
        ),
    )
)

timing_by_model = (
    timing.groupBy("run_id", "provider", "model")
    .agg(
        F.count(F.lit(1)).alias("n_job_chunks"),
        F.sum("n_requests").alias("n_job_requests"),
        F.sum("n_bytes").alias("request_jsonl_bytes"),
        F.min("submitted_at_utc").alias("first_submitted_at_utc"),
        F.max("recorded_at_utc").alias("last_recorded_at_utc"),
        F.max("checked_at_utc").alias("latest_checked_at_utc"),
        F.collect_set("provider_status").alias("submitted_provider_statuses"),
        F.collect_set("latest_provider_status").alias("latest_provider_statuses"),
        F.max("observed_wall_seconds").alias("observed_wall_seconds"),
    )
)

summary = (
    request_summary
    .join(raw_results, on=["run_id", "provider", "model", "model_tier"], how="left")
    .join(usage_by_model, on=["run_id", "provider", "model", "model_tier"], how="left")
    .join(timing_by_model, on=["run_id", "provider", "model"], how="left")
    .orderBy("run_id", "provider", "model")
)

result = {
    "run_ids": RUN_IDS,
    "tables": {
        "requests_table": REQUESTS_TABLE,
        "raw_results_table": RAW_RESULTS_TABLE,
        "batch_jobs_table": BATCH_JOBS_TABLE,
        "batch_status_table": BATCH_STATUS_TABLE,
        "result_files_table": RESULT_FILES_TABLE,
        "results_input_dir": RESULTS_INPUT_DIR,
    },
    "summary": [row.asDict(recursive=True) for row in summary.collect()],
    "usage_examples": usage_examples,
    "batch_timing_rows": [row.asDict(recursive=True) for row in timing.orderBy("run_id", "provider", "model", "chunk_id").collect()],
}

print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
