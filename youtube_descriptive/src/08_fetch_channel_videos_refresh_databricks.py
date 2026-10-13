# Databricks notebook source
"""
08_fetch_channel_videos_refresh — repo copy of the workspace notebook
"YouTube Update and Testing/fetch_channels_videos_backfill_20260820", parametrised so the
source ID table is a job widget (`source_table`) and the API key comes from the
`youtube/api-key` secret instead of being written into the code. Fetch logic is unchanged;
results append into the same production tables. Run with a fresh run_id (a YYYY-MM-DD
string) so this run's map/queue/not-found bookkeeping can't collide with prior runs'.

First use (2026-10-13): refresh recent uploads for the Figure 1 unknown + inactive channels,
source_table = dev_sean.matt.fig1_discovery_refresh_ids_v2.

For every distinct channel in SOURCE_TABLE, fetch:
  * the channel name and description                     (channels.list)
  * the N most recent video IDs                          (playlistItems.list)
  * each video's untruncated title + description         (videos.list)
where N is supplied at runtime via the `num_recent_videos` job widget.

playlistItems.snippet.description is truncated; videos.list returns the full
description, title, and authoritative publish time — so a third batched pass
enriches the recent videos before they are written.

Output is split across two tables in dev_sean.matt:
  * yt_channel_descriptions  — one row per channel (name + description)
  * yt_channel_videos        — one row per (channel, recent video)

DESIGN (mirrors all_update/all_update.py — the project's proven pattern):
  * Distributed fetch via mapPartitions; no .collect() of the channel universe.
  * Resumable: IDs are frozen into a stable batch/chunk map for the run. The
    CHUNK is the unit of work AND of completion — completed chunk_ids are
    recorded in a queue table, so a restart simply skips done chunks.
  * Append-only snapshots, partitioned by collected_date. At-least-once: a crash
    (or daily-quota stop) mid-chunk leaves the chunk unmarked, so the whole chunk
    is re-fetched on the next run. Duplicate snapshots are acceptable — reads take
    the latest collected_at per id.
  * SAFETY: workers NEVER raise on rate/quota/transient errors. A raised error
    fails the Spark task, and Spark re-runs the WHOLE partition, re-charging quota
    for batches that already succeeded — which is what blew the 10M daily budget
    before. Workers instead return a per-call outcome (ok / deferred / error /
    skipped); the chunk is only marked done when nothing was deferred or errored.

QUOTA:
  channels.list  = 1 unit per call of up to 50 IDs   → ~ Nchannels / 50 units
  playlistItems  = 1 unit per call (≤50 items)        → ~ Nchannels * ceil(N/50)
  videos.list    = 1 unit per call of up to 50 IDs   → ~ Nchannels * N / 50 units
  The per-channel playlistItems pass dominates; videos.list adds ~ that pass / 50.
  Daily budget = 10,000,000 units.
"""

from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# ── Configuration ───────────────────────────────────────────────────────────

API_KEY            = dbutils.secrets.get("youtube", "api-key")  # type: ignore[name-defined]  # noqa: F821
API_BASE           = "https://www.googleapis.com/youtube/v3"
CHANNELS_API_URL   = f"{API_BASE}/channels"
PLAYLISTITEMS_URL  = f"{API_BASE}/playlistItems"
VIDEOS_API_URL     = f"{API_BASE}/videos"

SOURCE_TABLE   = None  # set from the source_table widget below
ID_COLUMN      = "canonical_id"

CHANNELS_TABLE = "dev_sean.matt.yt_channel_descriptions"  # one row per channel
VIDEOS_TABLE   = "dev_sean.matt.yt_channel_videos"        # one row per recent video
QUEUE_TABLE    = "dev_sean.matt.yt_cd_queue"              # completed chunks
MAP_TABLE      = "dev_sean.matt.yt_cd_map"                # canonical_id -> batch/chunk
NOTFOUND_TABLE = "dev_sean.matt.yt_cd_not_found"          # audit of missing channel IDs

BATCH_SIZE       = 50       # channels.list max IDs per call
THREADS_PER_TASK = 128      # in-flight requests per task; the limiter caps actual rate
PARTITION_FACTOR = 3        # partitions = PARTITION_FACTOR × concurrent task slots
CHUNK_BATCHES    = 1_000    # batches per chunk/round (~50k channels). Smaller than
                            # all_update's chunk because each channel here also drives
                            # per-channel video calls — less work re-fetched on a stop.
MAX_RETRIES      = 8        # per-minute / transient retries per call (in-task)
REQUEST_TIMEOUT  = 30       # seconds

# Per-user-per-minute quota for the key's project caps request rate; the daily
# budget (10M units) is the hard wall (see SAFETY note in the module docstring).
TARGET_RPM    = 180_000
SAFETY_FACTOR = 0.5

# 403 classification (see all_update):
#   _DAILY_403     → daily budget gone. Stop the run cleanly; never retry/raise.
#   _PERMANENT_403 → auth/config error. Fail fast (rare; negligible quota).
#   anything else  → per-minute / transient: back off and retry in-task.
_DAILY_403     = {"quotaExceeded", "dailyLimitExceeded"}
_PERMANENT_403 = {"keyInvalid", "accessNotConfigured", "forbidden"}

# ── Runtime parameters (job widgets) ─────────────────────────────────────────

def _widget(name, default):
    try:
        v = dbutils.widgets.get(name)  # type: ignore[name-defined]  # noqa: F821
        return v if v else default
    except Exception:
        return default

# N most recent videos per channel — defined at runtime.
try:
    NUM_RECENT_VIDEOS = int(_widget("num_recent_videos", "10"))
except (ValueError, TypeError):
    NUM_RECENT_VIDEOS = 10
if NUM_RECENT_VIDEOS < 1:
    raise ValueError("num_recent_videos must be >= 1")

# One run_id per run. Override via job parameter when backfilling.
RUN_ID = _widget("run_id", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# Channel IDs to fetch (one `canonical_id` column). Required — no default, so a run can
# never silently refetch the wrong population.
SOURCE_TABLE = _widget("source_table", "")
if not SOURCE_TABLE:
    raise ValueError("source_table widget is required")

# ── Output schema for the distributed fetch ──────────────────────────────────
# A single wide row type discriminated by `row_type`; the driver projects the
# 'channel' and 'video' subsets into their respective tables.

_FETCH_SCHEMA = StructType([
    StructField("canonical_id",        StringType(),    False),  # the channel id
    StructField("row_type",            StringType(),    False),  # 'channel' | 'video'
    StructField("channel_name",        StringType(),    True),
    StructField("channel_description", StringType(),    True),
    StructField("uploads_playlist_id", StringType(),    True),
    StructField("video_id",            StringType(),    True),
    StructField("video_title",         StringType(),    True),
    StructField("video_description",   StringType(),    True),
    StructField("published_at",        TimestampType(), True),
    StructField("position",            IntegerType(),   True),   # 0 = most recent
    StructField("collected_at",        TimestampType(), False),
    StructField("batch_id",            LongType(),      False),
    StructField("status",              StringType(),    False),
])

# Statuses that mean a call did not complete — their presence blocks the chunk
# from being marked done (it is re-fetched next run). Everything else is terminal.
_BLOCKING = {"deferred", "error"}


# ── Table DDL (create-if-absent; never overwrite) ────────────────────────────

def ensure_tables():
    spark.sql("CREATE SCHEMA IF NOT EXISTS dev_sean.matt")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CHANNELS_TABLE} (
            canonical_id        STRING,
            channel_name        STRING,
            channel_description STRING,
            uploads_playlist_id STRING,
            collected_at        TIMESTAMP,
            collected_date      DATE
        ) USING DELTA PARTITIONED BY (collected_date)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {VIDEOS_TABLE} (
            canonical_id      STRING,
            video_id          STRING,
            video_title       STRING,
            video_description STRING,
            published_at      TIMESTAMP,
            position          INT,
            collected_at      TIMESTAMP,
            collected_date    DATE
        ) USING DELTA PARTITIONED BY (collected_date)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUEUE_TABLE} (
            run_id       STRING,
            chunk_id     BIGINT,
            completed_at TIMESTAMP
        ) USING DELTA PARTITIONED BY (run_id)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {MAP_TABLE} (
            canonical_id STRING,
            batch_id     BIGINT,
            run_id       STRING,
            chunk_id     BIGINT
        ) USING DELTA PARTITIONED BY (run_id, chunk_id)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {NOTFOUND_TABLE} (
            run_id       STRING,
            canonical_id STRING,
            collected_at TIMESTAMP
        ) USING DELTA PARTITIONED BY (run_id)
    """)


# ── Build the deduplicated canonical_id -> batch/chunk map for this run ───────

def ensure_map():
    """Freeze a stable, DEDUPLICATED canonical_id -> batch_id/chunk_id assignment
    for RUN_ID. `.distinct()` is the deduplication of SOURCE_TABLE.

    Idempotent: if the map already exists for this run (e.g. on restart) it is
    reused, guaranteeing identical batching across restarts. Returns max chunk_id.
    """
    existing = (
        spark.table(MAP_TABLE).where(F.col("run_id") == RUN_ID).limit(1).count()
    )
    if not existing:
        print(f"Building deduplicated batch map for run_id={RUN_ID} "
              f"from {SOURCE_TABLE}…")
        # Sort then zipWithIndex gives a deterministic, contiguous index without a
        # single-partition window; batch_id = idx // 50 caps each batch at 50 IDs.
        ids_rdd = (
            spark.table(SOURCE_TABLE)
            .select(F.col(ID_COLUMN).alias("canonical_id"))
            .where(F.col("canonical_id").isNotNull())
            .distinct()                      # <-- dedup of new_threshold_channels
            .orderBy("canonical_id")
            .rdd.map(lambda r: r.canonical_id)
        )
        map_rdd = ids_rdd.zipWithIndex().map(
            lambda x: (x[0],
                       x[1] // BATCH_SIZE,
                       RUN_ID,
                       (x[1] // BATCH_SIZE) // CHUNK_BATCHES)
        )
        map_df = spark.createDataFrame(
            map_rdd,
            "canonical_id string, batch_id long, run_id string, chunk_id long",
        )
        map_df.write.mode("append").partitionBy("run_id", "chunk_id") \
              .saveAsTable(MAP_TABLE)

        # Defensive: zipWithIndex order is relied upon for the ≤50/batch contract.
        max_sz = (
            spark.table(MAP_TABLE).where(F.col("run_id") == RUN_ID)
            .groupBy("batch_id").count().agg(F.max("count")).collect()[0][0]
        )
        if max_sz > BATCH_SIZE:
            raise AssertionError(
                f"batch larger than {BATCH_SIZE} ({max_sz}) — batching is broken")

    run_map = spark.table(MAP_TABLE).where(F.col("run_id") == RUN_ID)
    n = run_map.count()
    max_chunk = run_map.agg(F.max("chunk_id")).collect()[0][0]
    print(f"  {n:,} distinct channels mapped; {max_chunk + 1} chunks of "
          f"≤{CHUNK_BATCHES:,} batches; {NUM_RECENT_VIDEOS} recent videos/channel.")
    return max_chunk


# ── Distributed fetch (runs on workers) ──────────────────────────────────────

def make_fetch_partition(threads, rate_per_task, num_recent):
    """Return a per-partition generator that, for the channels in its partition:
      Stage 1 — batches IDs (≤50) and calls channels.list for name+description
                and the uploads playlist id.
      Stage 2 — for each found channel, calls playlistItems.list on its uploads
                playlist for the `num_recent` most recent video IDs.
      Stage 3 — batches the collected video IDs (≤50) through videos.list for the
                untruncated title, description, and publish time of each video.
    All stages share one rate limiter and one daily-quota stop signal. Emits one
    'channel' row per requested id and one 'video' row per recent video, plus
    marker rows carrying non-terminal statuses so the driver can detect an
    incomplete chunk. NEVER raises on rate/quota/transient errors."""

    max_pages = (num_recent + 49) // 50  # playlistItems caps maxResults at 50

    def fetch_partition(rows):
        import math  # noqa: F401  (kept for parity / future use)
        import random
        import threading
        import time
        from concurrent.futures import ThreadPoolExecutor
        from datetime import datetime, timezone

        import requests  # present on cluster workers (default on DBR)

        batches = {}
        for r in rows:
            batches.setdefault(r.batch_id, []).append(r.canonical_id)
        if not batches:
            return

        session = requests.Session()

        # Token bucket: this task's share of the global per-minute budget. All of
        # the task's threads share one limiter, so summed across ≤ S concurrent
        # slots the cluster stays under the per-minute cap.
        class _RateLimiter:
            def __init__(self, rate):
                self.rate = rate
                self.capacity = max(1.0, rate)  # allow ~1s burst
                self.tokens = self.capacity
                self.last = time.monotonic()
                self.lock = threading.Lock()

            def acquire(self):
                while True:
                    with self.lock:
                        now = time.monotonic()
                        self.tokens = min(
                            self.capacity,
                            self.tokens + (now - self.last) * self.rate,
                        )
                        self.last = now
                        if self.tokens >= 1.0:
                            self.tokens -= 1.0
                            return
                        sleep_for = (1.0 - self.tokens) / self.rate
                    time.sleep(sleep_for)

        limiter = _RateLimiter(rate_per_task)
        # Set once the daily quota is exhausted; short-circuits the rest of the
        # partition so we stop spending against an empty budget.
        stop = threading.Event()
        now = datetime.now(timezone.utc)

        def _parse_403_reason(resp):
            try:
                errs = resp.json().get("error", {}).get("errors", [])
                return errs[0].get("reason") if errs else None
            except Exception:
                return None

        def _parse_ts(s):
            if not s:
                return None
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return None

        def _do_request(url, params):
            """Single throttled GET with in-task backoff. Returns one of:
              ('ok', json) | ('deferred', None) | ('client_error', code)
              | ('error', None).
            Raises RuntimeError ONLY on a permanent auth/config 403 (real fatal).
            Never raises on rate/quota/transient/other-4xx — keeps the Spark task
            successful so each call is charged at most once."""
            if stop.is_set():
                return ("deferred", None)
            delay = 1.0
            for _ in range(MAX_RETRIES + 1):
                limiter.acquire()  # throttle every attempt, retries included
                try:
                    resp = session.get(url, params=params,
                                       timeout=REQUEST_TIMEOUT)
                    if resp.status_code == 200:
                        return ("ok", resp.json())
                    if resp.status_code == 403:
                        reason = _parse_403_reason(resp)
                        if reason in _DAILY_403:
                            stop.set()
                            return ("deferred", None)
                        if reason in _PERMANENT_403:
                            raise RuntimeError(f"permanent 403 ({reason})")
                        # rateLimitExceeded / userRateLimitExceeded → retry
                    elif resp.status_code == 404:
                        # Missing channel/playlist — soft miss, do not retry.
                        return ("client_error", 404)
                    elif resp.status_code not in (429, 500, 503):
                        # Other non-retryable 4xx (e.g. 400 bad playlist id).
                        # Soft per-call skip rather than fail the whole partition.
                        return ("client_error", resp.status_code)
                except requests.RequestException:
                    pass  # network blip: fall through to backoff
                if stop.is_set():
                    return ("deferred", None)
                time.sleep(delay + random.random())
                delay = min(delay * 2, 60.0)
            # Transient retries exhausted — return 'error' (do NOT raise).
            return ("error", None)

        # Row builders (keep the 13-field tuple order in one place).
        def _chan_row(cid, status, batch_id,
                      name=None, desc=None, uploads=None):
            return (cid, "channel", name, desc, uploads,
                    None, None, None, None, None, now, batch_id, status)

        def _vid_row(cid, batch_id, status,
                     video_id=None, title=None, desc=None,
                     published=None, position=None):
            return (cid, "video", None, None, None,
                    video_id, title, desc, published, position,
                    now, batch_id, status)

        # ── Stage 1: channels.list (batched) ─────────────────────────────────
        def fetch_channels(item):
            batch_id, ids = item
            kind, body = _do_request(
                CHANNELS_API_URL,
                {"part": "snippet,contentDetails",
                 "id": ",".join(ids), "key": API_KEY},
            )
            items = (body.get("items") or []) if kind == "ok" else None
            return batch_id, ids, items, kind

        # cid -> (uploads_playlist_id, batch_id) for channels with an uploads list
        video_targets = {}

        with ThreadPoolExecutor(max_workers=threads) as ex:
            for batch_id, ids, items, kind in ex.map(fetch_channels,
                                                      list(batches.items())):
                if kind != "ok":
                    # 'deferred' (daily quota) / 'error' (transient exhausted)
                    # block the chunk; 'client_error' on a whole channel batch is
                    # unexpected (ids are real) — treat as 'error' so it retries.
                    mark = "deferred" if kind == "deferred" else "error"
                    for cid in ids:
                        yield _chan_row(cid, mark, batch_id)
                    continue
                found = set()
                for it in items:
                    cid = it["id"]
                    found.add(cid)
                    snip = it.get("snippet") or {}
                    cd = it.get("contentDetails") or {}
                    uploads = (cd.get("relatedPlaylists") or {}).get("uploads")
                    yield _chan_row(cid, "found", batch_id,
                                    name=snip.get("title"),
                                    desc=snip.get("description"),
                                    uploads=uploads)
                    if uploads:
                        video_targets[cid] = (uploads, batch_id)
                for cid in ids:
                    if cid not in found:
                        yield _chan_row(cid, "not_found", batch_id)

            # ── Stage 2: playlistItems.list (per channel) ────────────────────
            def fetch_videos(target):
                cid, (uploads, batch_id) = target
                if stop.is_set():
                    return cid, batch_id, "deferred", []
                collected = []
                page_token = None
                for _ in range(max_pages):
                    want = num_recent - len(collected)
                    params = {"part": "snippet,contentDetails",
                              "playlistId": uploads,
                              "maxResults": min(50, want),
                              "key": API_KEY}
                    if page_token:
                        params["pageToken"] = page_token
                    kind, body = _do_request(PLAYLISTITEMS_URL, params)
                    if kind == "client_error":
                        # Deleted/empty/private uploads playlist — terminal, not a
                        # failure. Keep whatever we already collected.
                        return cid, batch_id, "skipped", collected
                    if kind != "ok":
                        return cid, batch_id, kind, collected  # deferred / error
                    for it in (body.get("items") or []):
                        snip = it.get("snippet") or {}
                        cd = it.get("contentDetails") or {}
                        rid = snip.get("resourceId") or {}
                        collected.append((
                            rid.get("videoId") or cd.get("videoId"),
                            snip.get("title"),
                            snip.get("description"),
                            _parse_ts(cd.get("videoPublishedAt")
                                      or snip.get("publishedAt")),
                        ))
                        if len(collected) >= num_recent:
                            break
                    page_token = body.get("nextPageToken")
                    if not page_token or len(collected) >= num_recent:
                        break
                return cid, batch_id, "ok", collected

            # Collect recent-video records (carrying the playlistItems title/desc/
            # publish as fallbacks); emit only failure/skip markers now. The
            # untruncated values come from the videos.list pass below.
            pending = []  # (cid, batch_id, pos, vid, fb_title, fb_desc, fb_pub)
            for cid, batch_id, kind, vids in ex.map(fetch_videos,
                                                    list(video_targets.items())):
                if kind in _BLOCKING:        # 'deferred' | 'error'
                    yield _vid_row(cid, batch_id, kind)
                    continue
                if kind == "skipped":        # no accessible uploads playlist
                    yield _vid_row(cid, batch_id, "video_skipped")
                    continue
                for pos, (vid, title, desc, published) in enumerate(vids):
                    if not vid:
                        # No resolvable video id — emit the playlist record as-is.
                        yield _vid_row(cid, batch_id, "video_ok",
                                       video_id=vid, title=title, desc=desc,
                                       published=published, position=pos)
                        continue
                    pending.append((cid, batch_id, pos, vid, title, desc, published))

            # ── Stage 3: videos.list (batched) for untruncated descriptions ──
            def fetch_video_meta(id_batch):
                kind, body = _do_request(
                    VIDEOS_API_URL,
                    {"part": "snippet", "id": ",".join(id_batch), "key": API_KEY},
                )
                items = (body.get("items") or []) if kind == "ok" else None
                return id_batch, items, kind

            full_meta = {}    # video_id -> (title, desc, published) from videos.list
            failed_ids = {}   # video_id -> blocking status ('deferred' | 'error')
            unique_ids = list({p[3] for p in pending})
            id_batches = [unique_ids[i:i + 50]
                          for i in range(0, len(unique_ids), 50)]
            for id_batch, items, kind in ex.map(fetch_video_meta, id_batches):
                if kind == "ok":
                    for it in items:
                        snip = it.get("snippet") or {}
                        full_meta[it["id"]] = (
                            snip.get("title"),
                            snip.get("description"),
                            _parse_ts(snip.get("publishedAt")),
                        )
                    # IDs absent from the response (deleted/private since stage 2)
                    # fall back to their playlistItems values below — not an error.
                elif kind == "client_error":
                    # Whole-batch client error is unexpected for real video ids —
                    # treat as transient so the chunk retries.
                    for vid in id_batch:
                        failed_ids[vid] = "error"
                else:                          # 'deferred' | 'error'
                    for vid in id_batch:
                        failed_ids[vid] = kind

            # Emit enriched video rows: prefer videos.list values, fall back to the
            # playlistItems values for any video dropped from the response.
            for cid, batch_id, pos, vid, fb_title, fb_desc, fb_pub in pending:
                if vid in failed_ids:
                    yield _vid_row(cid, batch_id, failed_ids[vid])  # blocks chunk
                    continue
                full = full_meta.get(vid)
                if full is None:
                    title, desc, published = fb_title, fb_desc, fb_pub
                else:
                    title     = full[0] if full[0] is not None else fb_title
                    desc      = full[1] if full[1] is not None else fb_desc
                    published = full[2] if full[2] is not None else fb_pub
                yield _vid_row(cid, batch_id, "video_ok",
                               video_id=vid, title=title, desc=desc,
                               published=published, position=pos)

    return fetch_partition


# ── Round processing (one chunk per round) ───────────────────────────────────

def process_round(chunk_id, threads, rate_per_task, num_partitions):
    work = (
        spark.table(MAP_TABLE)
        .where((F.col("run_id") == RUN_ID) & (F.col("chunk_id") == chunk_id))
        .select("canonical_id", "batch_id")
        .repartition(num_partitions, "batch_id")  # key: all 50 rows of a batch must
        # land in the same partition, or channels.list is split into partial calls,
        # multiplying quota usage. (Video calls are per-channel, partition-agnostic.)
    )

    fetched = spark.createDataFrame(
        work.rdd.mapPartitions(
            make_fetch_partition(threads, rate_per_task, NUM_RECENT_VIDEOS)),
        _FETCH_SCHEMA,
    )
    # Persist to disk and force the HTTP work exactly once, so the downstream
    # writes/counts can't re-trigger the fetch.
    fetched = fetched.persist(StorageLevel.DISK_ONLY)

    counts = {r["status"]: r["n"] for r in
              fetched.groupBy("status").count()
              .withColumnRenamed("count", "n").collect()}
    channels_found = counts.get("found", 0)
    not_found      = counts.get("not_found", 0)
    videos_ok      = counts.get("video_ok", 0)
    skipped        = counts.get("video_skipped", 0)
    deferred       = counts.get("deferred", 0)   # daily quota exhausted mid-chunk
    errored        = counts.get("error", 0)      # transient retries exhausted

    collected_date = F.lit(RUN_ID).cast("date")

    # Always persist the rows we did successfully fetch (append-only, valid data).
    (
        fetched.where("row_type = 'channel' AND status = 'found'")
        .select(
            "canonical_id", "channel_name", "channel_description",
            "uploads_playlist_id", "collected_at",
        )
        .withColumn("collected_date", collected_date)
        .write.mode("append").saveAsTable(CHANNELS_TABLE)
    )
    (
        fetched.where("row_type = 'video' AND status = 'video_ok'")
        .select(
            "canonical_id", "video_id", "video_title", "video_description",
            "published_at", "position", "collected_at",
        )
        .withColumn("collected_date", collected_date)
        .write.mode("append").saveAsTable(VIDEOS_TABLE)
    )
    (
        fetched.where("row_type = 'channel' AND status = 'not_found'")
        .select(F.lit(RUN_ID).alias("run_id"), "canonical_id", "collected_at")
        .write.mode("append").partitionBy("run_id").saveAsTable(NOTFOUND_TABLE)
    )

    # Mark the chunk done ONLY if every call completed. If anything was deferred
    # or errored, leave it unmarked so a future run reprocesses the whole chunk.
    complete = (deferred == 0 and errored == 0)
    if complete:
        (
            spark.createDataFrame([(RUN_ID, int(chunk_id))],
                                  "run_id string, chunk_id long")
            .withColumn("completed_at", F.current_timestamp())
            .write.mode("append").partitionBy("run_id").saveAsTable(QUEUE_TABLE)
        )

    fetched.unpersist()
    return {"channels_found": channels_found, "not_found": not_found,
            "videos_ok": videos_ok, "skipped": skipped,
            "deferred": deferred, "errored": errored, "complete": complete}


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    import time

    print(f"=== matt_channel_video_fetch run_id={RUN_ID} "
          f"num_recent_videos={NUM_RECENT_VIDEOS} ===")
    ensure_tables()
    max_chunk = ensure_map()

    # Split the per-minute budget across concurrent task slots so the aggregate
    # request rate stays under the cap.
    slots = max(1, spark.sparkContext.defaultParallelism)
    target_rps = TARGET_RPM * SAFETY_FACTOR / 60.0
    rate_per_task = target_rps / slots
    num_partitions = PARTITION_FACTOR * slots
    print(f"Throttle: {target_rps:,.0f} req/s global across ~{slots} slots "
          f"= {rate_per_task:,.1f} req/s/task; {num_partitions} partitions/chunk.")

    done = {
        r.chunk_id for r in
        spark.table(QUEUE_TABLE).where(F.col("run_id") == RUN_ID)
        .select("chunk_id").distinct().collect()
    }
    if done:
        print(f"Resuming: {len(done)} of {max_chunk + 1} chunks already done.")

    started = time.time()
    quota_stop = False
    incomplete = 0
    for c in range(max_chunk + 1):
        if c in done:
            continue
        print(f"Chunk {c}/{max_chunk}…")
        r = process_round(c, THREADS_PER_TASK, rate_per_task, num_partitions)
        flag = "" if r["complete"] else "  [INCOMPLETE — will retry next run]"
        print(f"  channels={r['channels_found']:,} not_found={r['not_found']:,} "
              f"videos={r['videos_ok']:,} skipped={r['skipped']:,} "
              f"deferred={r['deferred']:,} error={r['errored']:,}{flag}; "
              f"{(time.time() - started) / 60:.1f} min elapsed.")
        if not r["complete"]:
            incomplete += 1
        if r["deferred"] > 0:
            # Daily quota exhausted — stop cleanly; re-run after midnight PT to
            # resume from the completed chunks.
            quota_stop = True
            print("  DAILY QUOTA EXHAUSTED — stopping run; re-run tomorrow to resume.")
            break

    done_chunks = (
        spark.table(QUEUE_TABLE).where(F.col("run_id") == RUN_ID)
        .select("chunk_id").distinct().count()
    )
    status = ("STOPPED on daily quota" if quota_stop
              else "COMPLETE" if done_chunks >= max_chunk + 1
              else f"FINISHED with {incomplete} incomplete chunk(s) — re-run to finish")
    print(f"=== {status}. run_id={RUN_ID}: {done_chunks}/{max_chunk + 1} chunks done, "
          f"{(time.time() - started) / 60:.1f} min. ===")


if __name__ == "__main__":
    main()


# ── Post-flight verification (run AFTER a completed run) ──────────────────────
# SELECT * keeps the schema-resolution ordering bug out of play.
#
#   -- channels with no duplicate canonical_id within the run's latest snapshot:
#   SELECT canonical_id, COUNT(*) AS n
#   FROM dev_sean.matt.yt_channel_descriptions
#   WHERE collected_date = current_date()
#   GROUP BY canonical_id HAVING COUNT(*) > 1 LIMIT 20;   -- expect 0 rows
#
#   -- video counts per channel never exceed num_recent_videos:
#   SELECT MAX(n) FROM (
#     SELECT canonical_id, COUNT(*) AS n
#     FROM dev_sean.matt.yt_channel_videos
#     WHERE collected_date = current_date()
#     GROUP BY canonical_id);                             -- expect <= num_recent_videos
#
#   SELECT * FROM dev_sean.matt.yt_channel_descriptions LIMIT 5;
#   SELECT * FROM dev_sean.matt.yt_channel_videos        LIMIT 5;
