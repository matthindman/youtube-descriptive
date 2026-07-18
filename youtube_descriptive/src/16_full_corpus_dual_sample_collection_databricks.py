# Databricks notebook source
# ruff: noqa: F821
# MAGIC %run ./full_corpus_dual_sample_design

# COMMAND ----------
"""Resumable source-text collection for the frozen analysis-union queue."""

from __future__ import annotations

import concurrent.futures
import json
import threading
import time
from datetime import datetime, timezone

import requests
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def _widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass


def _get(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name).strip() or default
    except Exception:
        return default


_widget("stage", "collect_channels")
_widget(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
_widget("youtube_api_secret_scope", "")
_widget("youtube_api_secret_key", "")
STAGE = _get("stage", "collect_channels")
CONFIG_PATH = _get(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
SECRET_SCOPE = _get("youtube_api_secret_scope", "")
SECRET_KEY = _get("youtube_api_secret_key", "")
CONFIG = json.loads(dbutils.fs.head(CONFIG_PATH, 1024 * 1024))
validate_design_config(CONFIG)
COLLECTION = CONFIG["collection"]
DESIGN_VERSION = CONFIG["design_version"]
FRAME_VERSION = CONFIG["frame_version"]
RUN_ID = COLLECTION["run_id"]
PREFIX = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_prefix']}"

TABLES = {
    "queue": f"{PREFIX}_collection_queue",
    "channel_raw": f"{PREFIX}_collection_channel_raw",
    "video_raw": f"{PREFIX}_collection_video_raw",
    "video_items": f"{PREFIX}_collection_video_items_raw",
    "descriptions": f"{PREFIX}_collected_channel_descriptions",
    "videos": f"{PREFIX}_collected_channel_videos",
    "dispositions": f"{PREFIX}_collection_dispositions",
    "summary": f"{PREFIX}_collection_summary",
}

CHANNEL_SCHEMA = (
    "run_id string, request_id string, channel_id string, http_status int, found boolean, "
    "channel_name string, channel_description string, uploads_playlist_id string, response_body string, "
    "error string, attempts int, completed_at timestamp"
)
VIDEO_RAW_SCHEMA = (
    "run_id string, request_id string, channel_id string, uploads_playlist_id string, http_status int, "
    "item_count int, response_body string, error string, attempts int, completed_at timestamp"
)
VIDEO_ITEM_SCHEMA = (
    "run_id string, request_id string, channel_id string, video_id string, video_title string, "
    "video_description string, published_at timestamp, position int, completed_at timestamp"
)


def require_table(table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required table does not exist: {table_name}")


def append_table(frame: DataFrame, table_name: str) -> None:
    mode = "append" if spark.catalog.tableExists(table_name) else "overwrite"
    writer = frame.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(table_name)


def overwrite_table(frame: DataFrame, table_name: str, comment: str) -> None:
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    escaped = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{escaped}'")
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
        f"'design.version'='{DESIGN_VERSION}', "
        f"'frame.version'='{FRAME_VERSION}', "
        f"'collection.run_id'='{RUN_ID}')"
    )


def api_key() -> str:
    if not SECRET_SCOPE or not SECRET_KEY:
        raise RuntimeError(
            "YouTube API secret location is required through youtube_api_secret_scope and "
            "youtube_api_secret_key widgets; secret values are never stored in config or tables"
        )
    return dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)


thread_state = threading.local()


def session() -> requests.Session:
    value = getattr(thread_state, "session", None)
    if value is None:
        value = requests.Session()
        thread_state.session = value
    return value


def get_json(url: str, params: dict[str, str | int]) -> tuple[int | None, str | None, dict | None, str | None, int]:
    last_error = None
    for attempt in range(int(COLLECTION["max_retries"]) + 1):
        try:
            response = session().get(
                url,
                params=params,
                timeout=float(COLLECTION["request_timeout_seconds"]),
            )
            body = response.text
            parsed = response.json() if body else {}
            if response.status_code == 429 or response.status_code >= 500:
                last_error = f"HTTP {response.status_code}: {body[:500]}"
                if attempt < int(COLLECTION["max_retries"]):
                    time.sleep(min(30.0, 2.0**attempt))
                    continue
            return int(response.status_code), body, parsed, None, attempt + 1
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < int(COLLECTION["max_retries"]):
                time.sleep(min(30.0, 2.0**attempt))
    return None, None, None, last_error, int(COLLECTION["max_retries"]) + 1


def latest_raw(table_name: str) -> DataFrame:
    return (
        spark.table(table_name)
        .where(F.col("run_id") == RUN_ID)
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("channel_id").orderBy(
                    F.col("completed_at").desc_nulls_last(), F.col("request_id").desc()
                )
            ),
        )
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def collect_channels() -> dict[str, int]:
    require_table(TABLES["queue"])
    queue = spark.table(TABLES["queue"]).select("channel_id").distinct()
    if spark.catalog.tableExists(TABLES["channel_raw"]):
        completed = latest_raw(TABLES["channel_raw"]).where(
            F.col("http_status").between(200, 299)
        ).select("channel_id")
        pending = queue.join(completed, "channel_id", "left_anti")
    else:
        pending = queue
    key = api_key()
    batch_size = int(COLLECTION["channel_batch_size"])
    flush_count = int(COLLECTION["flush_channel_count"])
    workers = int(COLLECTION["max_workers"])
    pending_n = pending.count()

    def call(channel_ids: list[str]) -> list[tuple]:
        request_id = "ytc_" + sha256_order_key(
            ",".join(channel_ids), FRAME_VERSION, f"{RUN_ID}_channels"
        )[:60]
        status, body, parsed, error, attempts = get_json(
            "https://www.googleapis.com/youtube/v3/channels",
            {
                "part": "snippet,contentDetails",
                "id": ",".join(channel_ids),
                "maxResults": batch_size,
                "key": key,
            },
        )
        items = {str(item.get("id")): item for item in (parsed or {}).get("items", [])}
        completed_at = datetime.now(timezone.utc)
        rows = []
        for index, channel_id in enumerate(channel_ids):
            item = items.get(channel_id)
            snippet = (item or {}).get("snippet") or {}
            uploads = ((item or {}).get("contentDetails") or {}).get("relatedPlaylists", {}).get("uploads")
            rows.append(
                (
                    RUN_ID,
                    request_id,
                    channel_id,
                    status,
                    item is not None,
                    snippet.get("title"),
                    snippet.get("description"),
                    uploads,
                    body if index == 0 else None,
                    error,
                    attempts,
                    completed_at,
                )
            )
        return rows

    id_buffer: list[str] = []
    request_batches: list[list[str]] = []
    submitted = 0
    succeeded = 0

    def flush() -> None:
        nonlocal request_batches, submitted, succeeded
        if not request_batches:
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            result_groups = list(executor.map(call, request_batches))
        rows = [row for group in result_groups for row in group]
        append_table(spark.createDataFrame(rows, CHANNEL_SCHEMA), TABLES["channel_raw"])
        submitted += len(rows)
        succeeded += sum(1 for row in rows if row[3] is not None and 200 <= row[3] < 300)
        print(f"CHANNEL COLLECTION PROGRESS: submitted={submitted:,} successful_dispositions={succeeded:,}")
        request_batches = []

    for row in pending.orderBy("channel_id").toLocalIterator():
        id_buffer.append(str(row["channel_id"]))
        if len(id_buffer) == batch_size:
            request_batches.append(id_buffer)
            id_buffer = []
        if len(request_batches) * batch_size >= flush_count:
            flush()
    if id_buffer:
        request_batches.append(id_buffer)
    flush()
    result = {"pending_at_start": pending_n, "submitted": submitted, "successful_dispositions": succeeded}
    print("CHANNEL COLLECTION:", json.dumps(result, sort_keys=True))
    return result


def collect_videos() -> dict[str, int]:
    require_table(TABLES["channel_raw"])
    channels = latest_raw(TABLES["channel_raw"]).where(
        F.col("found") & F.col("uploads_playlist_id").isNotNull()
    ).select("channel_id", "uploads_playlist_id")
    if spark.catalog.tableExists(TABLES["video_raw"]):
        completed = latest_raw(TABLES["video_raw"]).where(
            F.col("http_status").between(200, 299)
        ).select("channel_id")
        pending = channels.join(completed, "channel_id", "left_anti")
    else:
        pending = channels
    key = api_key()
    workers = int(COLLECTION["max_workers"])
    flush_count = int(COLLECTION["flush_channel_count"])
    recent_n = int(COLLECTION["recent_videos_per_channel"])
    pending_n = pending.count()

    def call(row) -> tuple[tuple, list[tuple]]:
        channel_id = str(row["channel_id"])
        playlist_id = str(row["uploads_playlist_id"])
        request_id = "ytv_" + sha256_order_key(channel_id, FRAME_VERSION, f"{RUN_ID}_videos")[:60]
        status, body, parsed, error, attempts = get_json(
            "https://www.googleapis.com/youtube/v3/playlistItems",
            {
                "part": "snippet",
                "playlistId": playlist_id,
                "maxResults": recent_n,
                "key": key,
            },
        )
        completed_at = datetime.now(timezone.utc)
        items = (parsed or {}).get("items", [])
        video_rows = []
        for item in items:
            snippet = item.get("snippet") or {}
            resource = snippet.get("resourceId") or {}
            published = snippet.get("publishedAt")
            published_at = datetime.fromisoformat(published.replace("Z", "+00:00")) if published else None
            video_rows.append(
                (
                    RUN_ID,
                    request_id,
                    channel_id,
                    resource.get("videoId"),
                    snippet.get("title"),
                    snippet.get("description"),
                    published_at,
                    int(snippet.get("position")) if snippet.get("position") is not None else None,
                    completed_at,
                )
            )
        raw_row = (
            RUN_ID,
            request_id,
            channel_id,
            playlist_id,
            status,
            len(items),
            body,
            error,
            attempts,
            completed_at,
        )
        return raw_row, video_rows

    buffer = []
    submitted = 0
    succeeded = 0
    video_items = 0

    def flush() -> None:
        nonlocal buffer, submitted, succeeded, video_items
        if not buffer:
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(call, buffer))
        raw_rows = [result[0] for result in results]
        item_rows = [item for result in results for item in result[1]]
        append_table(spark.createDataFrame(raw_rows, VIDEO_RAW_SCHEMA), TABLES["video_raw"])
        if item_rows:
            append_table(spark.createDataFrame(item_rows, VIDEO_ITEM_SCHEMA), TABLES["video_items"])
        submitted += len(raw_rows)
        succeeded += sum(1 for row in raw_rows if row[4] is not None and 200 <= row[4] < 300)
        video_items += len(item_rows)
        print(
            f"VIDEO COLLECTION PROGRESS: submitted={submitted:,} "
            f"successful_dispositions={succeeded:,} video_items={video_items:,}"
        )
        buffer = []

    for row in pending.orderBy("channel_id").toLocalIterator():
        buffer.append(row)
        if len(buffer) >= flush_count:
            flush()
    flush()
    result = {
        "pending_at_start": pending_n,
        "submitted": submitted,
        "successful_dispositions": succeeded,
        "video_items": video_items,
    }
    print("VIDEO COLLECTION:", json.dumps(result, sort_keys=True))
    return result


def publish() -> dict[str, int]:
    for name in ("queue", "channel_raw", "video_raw"):
        require_table(TABLES[name])
    queue = spark.table(TABLES["queue"]).select("channel_id").distinct()
    channels = latest_raw(TABLES["channel_raw"])
    video_raw = latest_raw(TABLES["video_raw"])
    descriptions = channels.where(F.col("found")).select(
        F.col("channel_id").alias("canonical_id"),
        "channel_name",
        "channel_description",
        "uploads_playlist_id",
        F.col("completed_at").alias("collected_at"),
        F.to_date("completed_at").alias("collected_date"),
        "request_id",
        F.lit(RUN_ID).alias("collection_run_id"),
    )
    overwrite_table(
        descriptions,
        TABLES["descriptions"],
        "Run-scoped YouTube Data API channel descriptions for the frozen collection queue.",
    )
    if spark.catalog.tableExists(TABLES["video_items"]):
        items = spark.table(TABLES["video_items"]).where(F.col("run_id") == RUN_ID)
        videos = items.join(
            video_raw.select("channel_id", "request_id", "completed_at"),
            ["channel_id", "request_id", "completed_at"],
            "inner",
        ).dropDuplicates(["channel_id", "video_id", "position"]).select(
            F.col("channel_id").alias("canonical_id"),
            "video_id",
            "video_title",
            "video_description",
            "published_at",
            "position",
            F.col("completed_at").alias("collected_at"),
            F.to_date("completed_at").alias("collected_date"),
            "request_id",
            F.lit(RUN_ID).alias("collection_run_id"),
        )
    else:
        videos = spark.createDataFrame([], VIDEO_ITEM_SCHEMA).select(
            F.col("channel_id").alias("canonical_id"),
            "video_id",
            "video_title",
            "video_description",
            "published_at",
            "position",
            F.col("completed_at").alias("collected_at"),
            F.to_date("completed_at").alias("collected_date"),
            "request_id",
            F.lit(RUN_ID).alias("collection_run_id"),
        )
    overwrite_table(
        videos,
        TABLES["videos"],
        "Run-scoped recent upload text for the frozen collection queue.",
    )
    dispositions = (
        queue.join(
            channels.select(
                "channel_id",
                F.col("http_status").alias("channel_http_status"),
                "found",
                F.col("error").alias("channel_error"),
                "uploads_playlist_id",
            ),
            "channel_id",
            "left",
        )
        .join(
            video_raw.select(
                "channel_id",
                F.col("http_status").alias("video_http_status"),
                "item_count",
                F.col("error").alias("video_error"),
            ),
            "channel_id",
            "left",
        )
        .withColumn(
            "collection_disposition",
            F.when(F.col("channel_http_status").isNull(), "channel_request_missing")
            .when(~F.col("channel_http_status").between(200, 299), "channel_api_failure")
            .when(~F.col("found"), "not_found_or_terminated")
            .when(F.col("uploads_playlist_id").isNull(), "channel_found_without_uploads_playlist")
            .when(F.col("video_http_status").isNull(), "video_request_missing")
            .when(~F.col("video_http_status").between(200, 299), "video_api_failure")
            .when(F.col("item_count") == 0, "channel_found_no_recent_videos")
            .otherwise("collection_success"),
        )
        .withColumn("design_version", F.lit(DESIGN_VERSION))
        .withColumn("collection_run_id", F.lit(RUN_ID))
    )
    overwrite_table(
        dispositions,
        TABLES["dispositions"],
        "One terminal or retryable source-text collection disposition per queued channel.",
    )
    counts = {
        row["collection_disposition"]: int(row["count"])
        for row in dispositions.groupBy("collection_disposition").count().collect()
    }
    summary_rows = [
        (DESIGN_VERSION, RUN_ID, key, value, datetime.now(timezone.utc))
        for key, value in sorted(counts.items())
    ]
    overwrite_table(
        spark.createDataFrame(
            summary_rows,
            "design_version string, collection_run_id string, disposition string, count long, recorded_at timestamp",
        ),
        TABLES["summary"],
        "Collection completion summary for the frozen dual-sample queue.",
    )
    print("COLLECTION PUBLICATION: PASS")
    print(json.dumps(counts, sort_keys=True))
    return counts


STAGES = {
    "collect_channels": collect_channels,
    "collect_videos": collect_videos,
    "publish": publish,
}
if STAGE not in STAGES:
    raise ValueError(f"Unknown collection stage {STAGE!r}; expected one of {sorted(STAGES)}")
print(f"RUNNING COLLECTION STAGE: {STAGE}")
RESULT = STAGES[STAGE]()
dbutils.notebook.exit(json.dumps({"stage": STAGE, "result": RESULT}, sort_keys=True, default=str))
