# Databricks notebook source
# MAGIC %md
# MAGIC # Channel topics (`topicDetails`) for the >=10k universe
# MAGIC
# MAGIC Fetches each channel's YouTube topics from `channels.list` (`part=topicDetails`, 50 IDs per
# MAGIC call, 1 quota unit per call), so ~5M channels cost ~100k units.
# MAGIC
# MAGIC Used by Figure 2 (`12_fig2_platform_map_databricks`): a channel's category is derived from its
# MAGIC topic categories. This replaces the 2026-06-11 `dev_sean.default.channel_category` snapshot,
# MAGIC which misses channels found later.
# MAGIC
# MAGIC Output `target_table` (append-only, one row per channel per collection):
# MAGIC `canonical_id, topic_ids, topic_categories, status ('ok' | 'not_found'), collected_at, collected_date`.
# MAGIC A channel YouTube returns without `topicDetails` is `ok` with empty arrays.
# MAGIC
# MAGIC **Resumable:** IDs already in `target_table` with status ok/not_found are skipped, so a
# MAGIC rerun only fetches what is missing. Calls that fail after retries, or that hit the daily
# MAGIC quota, are not written and are picked up by the next run. An expired or invalid key fails
# MAGIC the run immediately.
# MAGIC
# MAGIC **Key:** the `api_key` job parameter if given, else the `youtube/api-key` secret.

# COMMAND ----------
import os
import random
import time
from datetime import datetime, timezone

import requests
from pyspark.sql import Row, functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType


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


DEFAULTS = {
    "source_table": "dev_sean.matt.fig1_discovery_channels_v4",
    "id_column": "canonical_id",
    "target_table": "dev_sean.matt.yt_channel_topics",
    "global_rps": "500",
    "api_key": "",
}
for k, v in DEFAULTS.items():
    _create_text_widget(k, v)
P = {k: _get_widget(k, v) for k, v in DEFAULTS.items()}
API_KEY = P["api_key"] or dbutils.secrets.get("youtube", "api-key")
SOURCE, ID_COL, TARGET = P["source_table"], P["id_column"], P["target_table"]
GLOBAL_RPS = float(P["global_rps"])

CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"
BATCH_SIZE = 50
MAX_RETRIES = 8
TIMEOUT = 30
print({k: v for k, v in P.items() if k != "api_key"})

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
  canonical_id STRING, topic_ids ARRAY<STRING>, topic_categories ARRAY<STRING>, status STRING,
  collected_at TIMESTAMP, collected_date DATE
) USING DELTA PARTITIONED BY (collected_date)
""")

todo = (spark.table(SOURCE).select(F.col(ID_COL).alias("canonical_id")).distinct()
        .join(spark.table(TARGET).where("status IN ('ok', 'not_found')").select("canonical_id"),
              "canonical_id", "left_anti"))
n_todo = todo.count()
print(f"channels to fetch: {n_todo:,}  (~{-(-n_todo // BATCH_SIZE):,} quota units)")

# COMMAND ----------
OUT_SCHEMA = StructType([
    StructField("canonical_id", StringType()), StructField("topic_ids", ArrayType(StringType())),
    StructField("topic_categories", ArrayType(StringType())), StructField("status", StringType()),
])


def fetch_partition(rows):
    """One channels.list call per 50 IDs. Yields ok / not_found / error / deferred rows.
    Never raises on rate or transient errors (a failed task would re-spend quota);
    raises only on a rejected key."""
    ids = [r.canonical_id for r in rows]
    if not ids:
        return
    session = requests.Session()
    min_interval = 1.0 / RATE_PER_TASK
    last = 0.0
    stopped = False
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i:i + BATCH_SIZE]
        if stopped:
            for cid in batch:
                yield (cid, None, None, "deferred")
            continue
        result, delay = None, 1.0
        for _ in range(MAX_RETRIES + 1):
            wait = last + min_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            last = time.monotonic()
            try:
                resp = session.get(CHANNELS_URL, timeout=TIMEOUT, params={
                    "part": "topicDetails", "id": ",".join(batch), "maxResults": 50, "key": API_KEY})
                if resp.status_code == 200:
                    result = resp.json()
                    break
                if resp.status_code == 400 and "API key" in resp.text:
                    raise RuntimeError("API key rejected (expired or invalid)")
                if resp.status_code == 403:
                    reasons = {e.get("reason") for e in resp.json().get("error", {}).get("errors", [])}
                    if reasons & {"quotaExceeded", "dailyLimitExceeded"}:
                        stopped = True
                        break
                    if reasons & {"keyInvalid", "accessNotConfigured", "forbidden"}:
                        raise RuntimeError(f"permanent 403 {reasons}")
                elif resp.status_code not in (429, 500, 503):
                    break  # other 4xx: give up on this batch, retry next run
            except requests.RequestException:
                pass
            time.sleep(delay + random.random())
            delay = min(delay * 2, 60.0)
        if result is None:
            status = "deferred" if stopped else "error"
            for cid in batch:
                yield (cid, None, None, status)
            continue
        found = {}
        for item in result.get("items", []):
            td = item.get("topicDetails", {})
            found[item["id"]] = (td.get("topicIds", []), td.get("topicCategories", []))
        for cid in batch:
            if cid in found:
                yield (cid, found[cid][0], found[cid][1], "ok")
            else:
                yield (cid, None, None, "not_found")


# COMMAND ----------
slots = max(1, spark.sparkContext.defaultParallelism)
RATE_PER_TASK = GLOBAL_RPS / slots
num_partitions = max(slots, -(-n_todo // 50_000))
print(f"{num_partitions} partitions, {RATE_PER_TASK:.1f} req/s per task (~{GLOBAL_RPS:.0f} req/s global)")

if n_todo:
    fetched = spark.createDataFrame(
        todo.repartition(num_partitions).rdd.mapPartitions(fetch_partition), OUT_SCHEMA)
    fetched = fetched.cache()
    counts = {r.status: r.n for r in fetched.groupBy("status").agg(F.count("*").alias("n")).collect()}
    print("status counts:", counts)
    now = datetime.now(timezone.utc)
    (fetched.where("status IN ('ok', 'not_found')")
            .withColumn("collected_at", F.lit(now).cast("timestamp"))
            .withColumn("collected_date", F.lit(now.date()).cast("date"))
            .write.mode("append").saveAsTable(TARGET))
    fetched.unpersist()
    left = counts.get("error", 0) + counts.get("deferred", 0)
    if left:
        print(f"{left:,} channels not fetched (error/deferred); rerun to finish.")

# COMMAND ----------
display(spark.sql(f"""
SELECT status, count(*) n, count_if(size(topic_categories) > 0) with_topics
FROM {TARGET} GROUP BY status"""))
