# Databricks notebook source
"""Estimate full DeepSeek Flash fallback cost from the completed smoke run."""

import json
import math
import os
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
BASE_RUN_ID = _widget(
    "base_run_id",
    "channel_crawl_full_20260623_deepseek_flash_full_fallback_20260630",
)
BASE_OUTPUT_PREFIX = _widget(
    "base_output_prefix",
    "yt_lid_v3_channel_crawl_full_20260623_deepseek_flash_full_fallback_20260630",
)
SMOKE_RUN_ID = f"{BASE_RUN_ID}_smoke_5k"
SMOKE_OUTPUT_PREFIX = f"{BASE_OUTPUT_PREFIX}_smoke_5k"
FULL_RUN_ID = BASE_RUN_ID
FULL_OUTPUT_PREFIX = BASE_OUTPUT_PREFIX
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


def local_path(path: str) -> str:
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/") :]
    return path


def read_jsonl(path: str):
    with open(local_path(path), "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


batch_files_table = fqtn(f"{SMOKE_OUTPUT_PREFIX}_llm_requests_batch_files")
batch_jobs_table = fqtn(f"{SMOKE_OUTPUT_PREFIX}_llm_batch_jobs")
requests_table = fqtn(f"{SMOKE_OUTPUT_PREFIX}_llm_requests")

batch_file_paths = [
    row["local_jsonl_path"]
    for row in (
        spark.table(batch_files_table)
        .where(F.col("run_id") == F.lit(SMOKE_RUN_ID))
        .select("local_jsonl_path")
        .collect()
    )
]
result_paths = [
    row["provider_file_id"]
    for row in (
        spark.table(batch_jobs_table)
        .where(F.col("run_id") == F.lit(SMOKE_RUN_ID))
        .select("provider_file_id")
        .collect()
    )
]

if not batch_file_paths:
    raise RuntimeError(f"No smoke batch-file rows found in {batch_files_table}")
if not result_paths:
    raise RuntimeError(f"No smoke result paths found in {batch_jobs_table}")

request_rows = []
for path in batch_file_paths:
    for obj in read_jsonl(path):
        body = obj.get("body") or {}
        messages = body.get("messages") or []
        request_rows.append(
            {
                "request_id": obj.get("custom_id") or obj.get("key"),
                "request_chars": len(json.dumps(body, ensure_ascii=False)),
                "message_chars": sum(len(str(m.get("content") or "")) for m in messages if isinstance(m, dict)),
                "max_tokens": body.get("max_tokens"),
            }
        )

usage_rows = []
missing_usage = 0
for path in result_paths:
    for obj in read_jsonl(path):
        body = ((obj.get("response") or {}).get("body") or {})
        usage = body.get("usage") or {}
        if not usage:
            missing_usage += 1
            usage = {}
        prompt_tokens = int(usage.get("prompt_tokens") or 0)
        completion_tokens = int(usage.get("completion_tokens") or 0)
        total_tokens = int(usage.get("total_tokens") or (prompt_tokens + completion_tokens))
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
                "total_tokens": total_tokens,
                "prompt_cache_hit_tokens": prompt_cache_hit_tokens,
                "prompt_cache_miss_tokens": prompt_cache_miss_tokens,
                "usage_keys": sorted(usage.keys()),
            }
        )


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


n_smoke = len(usage_rows)
scale = FULL_CHANNELS / n_smoke if n_smoke else None
usage_summary = {
    "n_smoke_results": n_smoke,
    "n_smoke_requests": len(request_rows),
    "missing_usage_rows": missing_usage,
    "full_channels": FULL_CHANNELS,
    "scale_factor": scale,
    "usage_key_sets": sorted({tuple(row["usage_keys"]) for row in usage_rows})[:20],
    "request_chars": stats([row["request_chars"] for row in request_rows]),
    "message_chars": stats([row["message_chars"] for row in request_rows]),
    "prompt_tokens": stats([row["prompt_tokens"] for row in usage_rows]),
    "completion_tokens": stats([row["completion_tokens"] for row in usage_rows]),
    "total_tokens": stats([row["total_tokens"] for row in usage_rows]),
    "prompt_cache_hit_tokens": stats([row["prompt_cache_hit_tokens"] for row in usage_rows]),
    "prompt_cache_miss_tokens": stats([row["prompt_cache_miss_tokens"] for row in usage_rows]),
}

smoke_cost = (
    usage_summary["prompt_cache_hit_tokens"]["sum"] / 1_000_000 * PRICE_INPUT_CACHE_HIT
    + usage_summary["prompt_cache_miss_tokens"]["sum"] / 1_000_000 * PRICE_INPUT_CACHE_MISS
    + usage_summary["completion_tokens"]["sum"] / 1_000_000 * PRICE_OUTPUT
)
full_estimated_cost = smoke_cost * scale

all_miss_smoke_cost = (
    usage_summary["prompt_tokens"]["sum"] / 1_000_000 * PRICE_INPUT_CACHE_MISS
    + usage_summary["completion_tokens"]["sum"] / 1_000_000 * PRICE_OUTPUT
)
all_miss_full_estimated_cost = all_miss_smoke_cost * scale

summary = {
    "run_ids": {
        "smoke": SMOKE_RUN_ID,
        "full": FULL_RUN_ID,
    },
    "tables": {
        "smoke_requests": requests_table,
        "smoke_batch_files": batch_files_table,
        "smoke_batch_jobs": batch_jobs_table,
    },
    "pricing_per_million_tokens": {
        "input_cache_hit": PRICE_INPUT_CACHE_HIT,
        "input_cache_miss": PRICE_INPUT_CACHE_MISS,
        "output": PRICE_OUTPUT,
    },
    "usage_summary": usage_summary,
    "cost_estimates_usd": {
        "smoke_actual_from_usage": smoke_cost,
        "full_scaled_from_smoke_with_observed_cache": full_estimated_cost,
        "full_scaled_from_smoke_all_input_cache_miss": all_miss_full_estimated_cost,
    },
}

print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True))
dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False))
