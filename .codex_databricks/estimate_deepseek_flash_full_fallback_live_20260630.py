# Databricks notebook source
"""Live cost/runtime estimator for the DeepSeek Flash full fallback run."""

import json
import math
import os
from datetime import datetime, timezone
from statistics import mean, median

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
RUN_ID = _widget(
    "run_id",
    "channel_crawl_full_20260623_deepseek_flash_full_fallback_20260630",
)
OUTPUT_PREFIX = _widget(
    "output_prefix",
    "yt_lid_v3_channel_crawl_full_20260623_deepseek_flash_full_fallback_20260630",
)
FULL_CHANNELS = int(_widget("full_channels", "645865"))

# Official DeepSeek V4 Flash pricing per 1M tokens as of 2026-06-30.
PRICE_INPUT_CACHE_HIT = float(_widget("price_input_cache_hit_per_million", "0.0028"))
PRICE_INPUT_CACHE_MISS = float(_widget("price_input_cache_miss_per_million", "0.14"))
PRICE_OUTPUT = float(_widget("price_output_per_million", "0.28"))


def fqtn(table: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{CATALOG}.{table}"
    return f"{CATALOG}.{SCHEMA}.{table}"


def table_exists(table: str) -> bool:
    try:
        return spark.catalog.tableExists(fqtn(table))
    except Exception:
        return False


def local_path(path: str) -> str:
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/") :]
    return path


def read_jsonl(path: str):
    resolved = local_path(path)
    if not os.path.exists(resolved):
        return
    with open(resolved, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except Exception:
                    continue


def stats(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    values_sorted = sorted(values)
    return {
        "sum": sum(values),
        "mean": mean(values),
        "median": median(values),
        "p90": values_sorted[min(len(values_sorted) - 1, math.ceil(0.90 * len(values_sorted)) - 1)],
        "p95": values_sorted[min(len(values_sorted) - 1, math.ceil(0.95 * len(values_sorted)) - 1)],
        "max": max(values),
    }


progress_table = f"{OUTPUT_PREFIX}_llm_run_progress"
batch_jobs_table = f"{OUTPUT_PREFIX}_llm_batch_jobs"
requests_table = f"{OUTPUT_PREFIX}_llm_requests"

if not table_exists(progress_table):
    raise RuntimeError(f"Progress table not found: {fqtn(progress_table)}")

progress = spark.table(fqtn(progress_table)).where(F.col("run_id") == F.lit(RUN_ID))

progress_rows = [row.asDict() for row in progress.select("event_timestamp", "stage", "status", "metric", "value").collect()]
configured_times = [row["event_timestamp"] for row in progress_rows if row["stage"] == "configured"]
started_times = [row["event_timestamp"] for row in progress_rows if row["stage"] == "deepseek_direct_started"]
completed_times = [row["event_timestamp"] for row in progress_rows if row["stage"] == "deepseek_direct_completed"]

start_time = min(started_times or configured_times) if (started_times or configured_times) else None
latest_completion_time = max(completed_times) if completed_times else None
now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

completed_metrics = {}
started_metrics = {}
for row in progress_rows:
    if row["stage"] == "deepseek_direct_started":
        key = (row["event_timestamp"], row["status"])
        started_metrics.setdefault(key, {})[row["metric"]] = row["value"]
    if row["stage"] != "deepseek_direct_completed":
        continue
    key = (row["event_timestamp"], row["status"])
    completed_metrics.setdefault(key, {})[row["metric"]] = row["value"]

completed_chunks = []
for (event_timestamp, status), metrics in completed_metrics.items():
    total_requests = int(metrics.get("total_requests") or 0)
    successful_request_ids = int(metrics.get("successful_request_ids") or 0)
    completed_chunks.append(
        {
            "event_timestamp": str(event_timestamp),
            "status": status,
            "chunk_file": metrics.get("chunk_file"),
            "result_path": metrics.get("result_path"),
            "total_requests": total_requests,
            "successful_request_ids": successful_request_ids,
            "missing_successful_request_ids": int(metrics.get("missing_successful_request_ids") or 0),
            "seen_request_ids": int(metrics.get("seen_request_ids") or 0),
        }
    )
completed_chunks = sorted(completed_chunks, key=lambda r: r["event_timestamp"])

started_result_paths = sorted({metrics.get("result_path") for metrics in started_metrics.values() if metrics.get("result_path")})
completed_result_paths = sorted({row["result_path"] for row in completed_chunks if row.get("result_path")})
result_paths = sorted(set(started_result_paths + completed_result_paths))
usage_rows = []
status_counts = {}
missing_usage = 0
for path in result_paths:
    for obj in read_jsonl(path):
        response = obj.get("response") or {}
        status_code = int(response.get("status_code") or 0)
        status_counts[str(status_code)] = status_counts.get(str(status_code), 0) + 1
        body = response.get("body") or {}
        usage = body.get("usage") or {}
        if not usage:
            missing_usage += 1
        prompt_tokens = int(usage.get("prompt_tokens") or 0)
        completion_tokens = int(usage.get("completion_tokens") or 0)
        prompt_cache_hit_tokens = int(
            usage.get("prompt_cache_hit_tokens")
            or usage.get("prompt_cache_hit")
            or usage.get("cache_hit_tokens")
            or 0
        )
        prompt_cache_miss_tokens = int(
            usage.get("prompt_cache_miss_tokens")
            or usage.get("prompt_cache_miss")
            or usage.get("cache_miss_tokens")
            or max(0, prompt_tokens - prompt_cache_hit_tokens)
        )
        usage_rows.append(
            {
                "request_id": obj.get("custom_id"),
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": int(usage.get("total_tokens") or (prompt_tokens + completion_tokens)),
                "prompt_cache_hit_tokens": prompt_cache_hit_tokens,
                "prompt_cache_miss_tokens": prompt_cache_miss_tokens,
            }
        )

completed_success = sum(row["successful_request_ids"] for row in completed_chunks)
completed_seen = len(usage_rows)
requests_count = spark.table(fqtn(requests_table)).where(F.col("run_id") == F.lit(RUN_ID)).count() if table_exists(requests_table) else FULL_CHANNELS

elapsed_wall_seconds = None
elapsed_completed_seconds = None
if start_time is not None:
    elapsed_wall_seconds = max(0.0, (now_utc - start_time).total_seconds())
if start_time is not None and latest_completion_time is not None:
    elapsed_completed_seconds = max(0.0, (latest_completion_time - start_time).total_seconds())

throughput_per_hour = None
eta_remaining_hours = None
estimated_finish_utc = None
if completed_success > 0 and elapsed_completed_seconds and elapsed_completed_seconds > 0:
    throughput_per_hour = completed_success / (elapsed_completed_seconds / 3600.0)
    eta_remaining_hours = max(0, requests_count - completed_success) / throughput_per_hour
    estimated_finish_utc = (now_utc.timestamp() + eta_remaining_hours * 3600.0)

partial_throughput_per_hour = None
partial_eta_remaining_hours = None
partial_estimated_finish_utc = None
if completed_seen > 0 and elapsed_wall_seconds and elapsed_wall_seconds > 0:
    partial_throughput_per_hour = completed_seen / (elapsed_wall_seconds / 3600.0)
    partial_eta_remaining_hours = max(0, requests_count - completed_seen) / partial_throughput_per_hour
    partial_estimated_finish_utc = now_utc.timestamp() + partial_eta_remaining_hours * 3600.0

input_hit_sum = sum(row["prompt_cache_hit_tokens"] for row in usage_rows)
input_miss_sum = sum(row["prompt_cache_miss_tokens"] for row in usage_rows)
output_sum = sum(row["completion_tokens"] for row in usage_rows)
observed_cost = (
    input_hit_sum / 1_000_000 * PRICE_INPUT_CACHE_HIT
    + input_miss_sum / 1_000_000 * PRICE_INPUT_CACHE_MISS
    + output_sum / 1_000_000 * PRICE_OUTPUT
)
scale = requests_count / completed_seen if completed_seen else None
scaled_total_cost = observed_cost * scale if scale else None
remaining_cost = max(0.0, scaled_total_cost - observed_cost) if scaled_total_cost is not None else None

summary = {
    "run_id": RUN_ID,
    "output_prefix": OUTPUT_PREFIX,
    "tables": {
        "requests": fqtn(requests_table),
        "batch_jobs": fqtn(batch_jobs_table),
        "progress": fqtn(progress_table),
    },
    "pricing_per_million_tokens": {
        "input_cache_hit": PRICE_INPUT_CACHE_HIT,
        "input_cache_miss": PRICE_INPUT_CACHE_MISS,
        "output": PRICE_OUTPUT,
    },
    "completion": {
        "requests_total": requests_count,
        "completed_chunks": len(completed_chunks),
        "completed_successful_request_ids": completed_success,
        "result_rows_read_from_started_or_completed_files": completed_seen,
        "missing_usage_rows": missing_usage,
        "status_counts": status_counts,
        "started_result_paths": len(started_result_paths),
        "completed_result_paths": len(completed_result_paths),
        "first_started_at": str(start_time) if start_time is not None else None,
        "latest_completed_at": str(latest_completion_time) if latest_completion_time is not None else None,
        "snapshot_utc": str(now_utc),
    },
    "runtime_estimate": {
        "elapsed_wall_seconds": elapsed_wall_seconds,
        "elapsed_completed_seconds": elapsed_completed_seconds,
        "throughput_requests_per_hour_from_completed_chunks": throughput_per_hour,
        "eta_remaining_hours_from_completed_chunks": eta_remaining_hours,
        "estimated_finish_unix_utc": estimated_finish_utc,
        "partial_throughput_requests_per_hour_from_result_rows": partial_throughput_per_hour,
        "partial_eta_remaining_hours_from_result_rows": partial_eta_remaining_hours,
        "partial_estimated_finish_unix_utc": partial_estimated_finish_utc,
    },
    "usage_summary": {
        "prompt_tokens": stats([row["prompt_tokens"] for row in usage_rows]),
        "completion_tokens": stats([row["completion_tokens"] for row in usage_rows]),
        "total_tokens": stats([row["total_tokens"] for row in usage_rows]),
        "prompt_cache_hit_tokens": stats([row["prompt_cache_hit_tokens"] for row in usage_rows]),
        "prompt_cache_miss_tokens": stats([row["prompt_cache_miss_tokens"] for row in usage_rows]),
    },
    "cost_estimates_usd": {
        "observed_completed_cost": observed_cost,
        "scaled_total_cost_from_completed_results": scaled_total_cost,
        "scaled_remaining_cost_from_completed_results": remaining_cost,
    },
    "completed_chunks": completed_chunks[-5:],
}

print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False, default=str))
