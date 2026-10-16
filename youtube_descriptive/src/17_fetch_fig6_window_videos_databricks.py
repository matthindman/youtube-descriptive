# Databricks notebook source
"""
17_fetch_fig6_window_videos — Figure 6 video-level collection, DRY-RUN edition (<= ~1,000 channels).

Adapted from 08_fetch_channel_videos_refresh_databricks.py (see PLAN_fig6_collection.md §5).
Design notes and run log: README_fig6_dryrun.md.

What it does
  Sample : `sample_n` channels from dev_sean.matt.fig1_discovery_channels_v4
           (activity_panel='active' AND NOT is_topic), stratified into weekly_views deciles
           (ntile(10), ties broken by id), fixed-seed hash order within decile.
  Pass A : playlistItems.list on the uploads playlist ('UU' + id[2:]), part=snippet,contentDetails,
           maxResults=50, newest first. Stop paging at the first item published before
           `window_start`, or after `page_cap` pages (channel flagged `censored`). Items from
           `window_start` on are recorded; items after `window_end` are kept with after_window=true.
  Pass B : videos.list part=contentDetails,statistics,snippet, 50 ids/call, for every recorded video.

Quota safety (kept from 08)
  * Calls never raise on rate/quota/transient errors; every attempt returns an outcome
    (ok / retry / deferred / error / client_error / skipped) and is logged to the calls table.
  * One global token-bucket rate limiter (`rpm`, default 2000/min).
  * Clean stop on quotaExceeded / dailyLimitExceeded (remaining work is 'skipped').
  * Permanent auth/config errors (keyInvalid, keyExpired, API_KEY_INVALID, accessNotConfigured,
    forbidden, 400 invalid key) stop the whole run immediately; the run fails AFTER the logs
    are flushed. The first channel is processed serially as a canary before any concurrency.
  * Chunk-level resumability: a chunk is recorded in the queue table only when none of its calls
    was deferred / errored / skipped.
  * HARD UNIT CAP `max_units`: every HTTP attempt (retries included) reserves 1 unit from a global
    counter BEFORE it is sent; once the cap is reached every further call is 'skipped'. The fetch
    runs driver-side on a thread pool (acceptable at 1k channels), so the cap is exact and global.
    A full-scale run needs a distributed version with per-task budgets (see README).

Key handling: read ONLY via dbutils.secrets.get("youtube", "api-key"); sent in the
X-Goog-Api-Key header, so no URL ever contains it. No api_key widget. Exception text and
response bodies are never printed or stored — only HTTP status codes and Google's `reason` code.

Outputs (append-only; `table_suffix` default v1; the smoke run uses suffix `smoke`):
  dev_sean.matt.fig6_dryrun_sample_<sfx>      sampled channels with stratum + stratum population size
  dev_sean.matt.fig6_dryrun_uploads_<sfx>     one row per recorded upload
  dev_sean.matt.fig6_dryrun_video_stats_<sfx> one row per video returned by videos.list
  dev_sean.matt.fig6_dryrun_calls_<sfx>       one row per HTTP attempt (no key, no URL)
  dev_sean.matt.fig6_dryrun_channels_<sfx>    one row per channel: pages, censored, n_window, status
  dev_sean.matt.fig6_dryrun_queue_<sfx>       completed chunks
Pre-flight: refuses to run if any output exists, unless overwrite=true (drops only these tables)
or resume=true (reuse sample, skip queued chunks).
"""

import re
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType,
    BooleanType, DoubleType,
)

spark = SparkSession.builder.getOrCreate()

API_BASE          = "https://www.googleapis.com/youtube/v3"
PLAYLISTITEMS_URL = f"{API_BASE}/playlistItems"
VIDEOS_API_URL    = f"{API_BASE}/videos"
DISCOVERY_URL     = "https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest"  # no key, no quota

SRC_TABLE = "dev_sean.matt.fig1_discovery_channels_v4"
SCHEMA    = "dev_sean.matt"
PREFIX    = "fig6_dryrun_"

THREADS         = 16
MAX_RETRIES     = 4      # transient retries per call (each attempt costs 1 unit)
REQUEST_TIMEOUT = 30

_DAILY_403     = {"quotaExceeded", "dailyLimitExceeded"}
_PERMANENT_403 = {"keyInvalid", "keyExpired", "accessNotConfigured", "forbidden"}
_KEY_BAD_MARKERS = ("api_key_invalid", "api key not valid", "api key expired", "keyinvalid", "keyexpired")


def _widget(name, default):
    try:
        v = dbutils.widgets.get(name)  # type: ignore[name-defined]  # noqa: F821
        return v if v not in (None, "") else default
    except Exception:
        return default


def _bool(v):
    return str(v).strip().lower() in ("1", "true", "yes", "y")


SAMPLE_N     = int(_widget("sample_n", "1000"))
SEED         = int(_widget("seed", "20261016"))
WINDOW_START = datetime.fromisoformat(_widget("window_start", "2026-08-31")).replace(tzinfo=timezone.utc)
WINDOW_END   = datetime.fromisoformat(_widget("window_end", "2026-10-05")).replace(tzinfo=timezone.utc)
PAGE_CAP     = int(_widget("page_cap", "20"))
MAX_UNITS    = int(_widget("max_units", "5000"))
RPM          = int(_widget("rpm", "2000"))
CHUNK_SIZE   = int(_widget("chunk_size", "250"))
SFX          = _widget("table_suffix", "v1")
OVERWRITE    = _bool(_widget("overwrite", "false"))
RESUME       = _bool(_widget("resume", "false"))
RUN_ID       = _widget("run_id", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%S"))

if RPM > 2000:
    raise ValueError("rpm is capped at 2000 for the dry run")
if SAMPLE_N > 1000:
    raise ValueError("sample_n is capped at 1000 for the dry run")
if MAX_UNITS > 5000:
    raise ValueError("max_units is capped at 5000 for the dry run")

T = {k: f"{SCHEMA}.{PREFIX}{k}_{SFX}"
     for k in ("sample", "uploads", "video_stats", "calls", "channels", "queue")}

# ── Schemas ──────────────────────────────────────────────────────────────────

UPLOADS_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("channel_id", StringType()),
    StructField("video_id", StringType()), StructField("published_at", TimestampType()),
    StructField("page", IntegerType()), StructField("after_window", BooleanType()),
    StructField("censored", BooleanType()), StructField("collected_at", TimestampType()),
])
STATS_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("video_id", StringType()),
    StructField("channel_id", StringType()), StructField("published_at", TimestampType()),
    StructField("duration_iso", StringType()), StructField("duration_s", LongType()),
    StructField("view_count", LongType()), StructField("like_count", LongType()),
    StructField("comment_count", LongType()),
    StructField("default_audio_language", StringType()), StructField("default_language", StringType()),
    StructField("title", StringType()), StructField("live_broadcast_content", StringType()),
    StructField("collected_at", TimestampType()),
])
CALLS_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("call_ts", TimestampType()),
    StructField("pass", StringType()), StructField("outcome", StringType()),
    StructField("units", IntegerType()), StructField("http_status", IntegerType()),
    StructField("reason", StringType()), StructField("channel_id", StringType()),
    StructField("page_no", IntegerType()), StructField("n_ids", IntegerType()),
    StructField("attempt", IntegerType()), StructField("latency_ms", IntegerType()),
    StructField("chunk_id", IntegerType()),
])
CHANNELS_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("channel_id", StringType()),
    StructField("chunk_id", IntegerType()),
    StructField("pages", IntegerType()), StructField("censored", BooleanType()),
    StructField("n_window", IntegerType()), StructField("n_after_window", IntegerType()),
    StructField("status", StringType()), StructField("collected_at", TimestampType()),
])

# ── Shared state ─────────────────────────────────────────────────────────────

class Budget:
    """Global hard unit cap. reserve() is called before every HTTP attempt."""
    def __init__(self, cap):
        self.cap, self.used, self.lock = cap, 0, threading.Lock()

    def reserve(self):
        with self.lock:
            if self.used >= self.cap:
                return False
            self.used += 1
            return True


class Limiter:
    def __init__(self, rpm):
        self.rate = rpm / 60.0
        self.capacity = max(1.0, self.rate / 4)   # <= 0.25 s burst
        self.tokens, self.last, self.lock = self.capacity, time.monotonic(), threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait = (1.0 - self.tokens) / self.rate
            time.sleep(wait)


class Ctx:
    def __init__(self, key):
        self.key = key
        self.budget = Budget(MAX_UNITS)
        self.limiter = Limiter(RPM)
        self.stop = threading.Event()     # daily quota gone
        self.fatal = threading.Event()    # permanent auth/config error
        self.fatal_reason = None
        self.cap_hit = threading.Event()
        self.calls, self.lock = [], threading.Lock()
        self.chunk_id = -1
        import requests
        self.session = requests.Session()

    def log(self, pass_, outcome, units, http=None, reason=None, channel=None,
            page=None, n_ids=None, attempt=None, latency=None):
        row = (RUN_ID, datetime.now(timezone.utc), pass_, outcome, units, http, reason, channel,
               page, n_ids, attempt, latency, self.chunk_id)
        with self.lock:
            self.calls.append(row)

    def drain(self):
        with self.lock:
            rows, self.calls = self.calls, []
        return rows


def _reason(resp):
    """Google's machine-readable error reason only; never the message text."""
    try:
        errs = resp.json().get("error", {}).get("errors", [])
        return errs[0].get("reason") if errs else None
    except Exception:
        return None


def api_call(ctx, pass_, url, params, channel=None, page=None, n_ids=None):
    """One logical call with in-task backoff. Returns (kind, body, http_status);
    kind in ok | deferred | error | client_error | skipped. Never raises."""
    import requests
    delay = 1.0
    last_http = None
    for attempt in range(1, MAX_RETRIES + 2):
        if ctx.fatal.is_set() or ctx.stop.is_set():
            ctx.log(pass_, "skipped", 0, reason="fatal" if ctx.fatal.is_set() else "daily_quota_stop",
                    channel=channel, page=page, n_ids=n_ids, attempt=attempt)
            return ("skipped", None, last_http)
        if not ctx.budget.reserve():
            ctx.cap_hit.set()
            ctx.log(pass_, "skipped", 0, reason="unit_cap", channel=channel, page=page,
                    n_ids=n_ids, attempt=attempt)
            return ("skipped", None, last_http)
        ctx.limiter.acquire()
        t0 = time.monotonic()
        http, reason, body, outcome = None, None, None, None
        try:
            resp = ctx.session.get(url, params=params, headers={"X-Goog-Api-Key": ctx.key},
                                   timeout=REQUEST_TIMEOUT)
            http = resp.status_code
            last_http = http
            if http == 200:
                body, outcome = resp.json(), "ok"
            else:
                reason = _reason(resp)
                low = (resp.text or "").lower()
                if (http in (400, 403)) and (reason in ("keyInvalid", "keyExpired")
                                             or any(m in low for m in _KEY_BAD_MARKERS)):
                    outcome, reason = "fatal", reason or "key_rejected"
                elif http == 403 and reason in _DAILY_403:
                    outcome = "deferred"
                elif http == 403 and reason in _PERMANENT_403:
                    outcome = "fatal"
                elif http in (429, 500, 502, 503, 504) or (http == 403):
                    outcome = "retry"      # rate limit / transient
                else:
                    outcome = "client_error"   # 404 playlistNotFound, other 4xx: terminal, soft
        except requests.RequestException as e:
            outcome, reason = "retry", type(e).__name__      # class only: str(e) may contain the URL
        except Exception as e:                                # bad JSON etc.
            outcome, reason = "retry", type(e).__name__
        lat = int((time.monotonic() - t0) * 1000)
        final = outcome if outcome != "retry" else ("retry" if attempt <= MAX_RETRIES else "error")
        ctx.log(pass_, final, 1, http=http, reason=reason, channel=channel, page=page,
                n_ids=n_ids, attempt=attempt, latency=lat)
        if outcome == "ok":
            return ("ok", body, http)
        if outcome == "fatal":
            ctx.fatal_reason = f"HTTP {http} {reason}"
            ctx.fatal.set()
            return ("error", None, http)
        if outcome == "deferred":
            ctx.stop.set()
            return ("deferred", None, http)
        if outcome == "client_error":
            return ("client_error", None, http)
        if attempt > MAX_RETRIES:
            return ("error", None, http)
        time.sleep(delay + random.random())
        delay = min(delay * 2, 30.0)
    return ("error", None, last_http)


def _parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


_DUR = re.compile(r"^P(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")


def parse_duration(s):
    """ISO 8601 duration -> seconds (None if unparseable)."""
    if not s:
        return None
    m = _DUR.match(s)
    if not m:
        return None
    w, d, h, mi, sec = (int(x) if x else 0 for x in m.groups())
    return ((w * 7 + d) * 24 + h) * 3600 + mi * 60 + sec


def _int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


# ── Pass A ───────────────────────────────────────────────────────────────────

def pass_a(ctx, cid):
    """Returns (uploads_rows, channel_row). Blocking status = not in ('ok','client_error')."""
    now = datetime.now(timezone.utc)
    if not cid or not cid.startswith("UC"):
        return [], (RUN_ID, cid, ctx.chunk_id, 0, False, 0, 0, "bad_id", now)
    uploads_id = "UU" + cid[2:]
    pages, token, reached_old, status = 0, None, False, "ok"
    recs = []
    while pages < PAGE_CAP:
        params = {"part": "snippet,contentDetails", "playlistId": uploads_id, "maxResults": 50}
        if token:
            params["pageToken"] = token
        kind, body, _ = api_call(ctx, "A", PLAYLISTITEMS_URL, params, channel=cid, page=pages + 1)
        if kind != "ok":
            status = kind
            break
        pages += 1
        for it in (body.get("items") or []):
            snip, cd = it.get("snippet") or {}, it.get("contentDetails") or {}
            vid = cd.get("videoId") or (snip.get("resourceId") or {}).get("videoId")
            ts = _parse_ts(cd.get("videoPublishedAt") or snip.get("publishedAt"))
            if not vid or ts is None:
                continue        # private/deleted placeholder with no date
            if ts < WINDOW_START:
                reached_old = True
                break
            recs.append((vid, ts, pages))
        token = body.get("nextPageToken")
        if reached_old or not token:
            break
    censored = (status == "ok" and pages >= PAGE_CAP and not reached_old and bool(token))
    up_rows = [(RUN_ID, cid, v, ts, pg, ts > WINDOW_END, censored, now) for v, ts, pg in recs]
    n_after = sum(1 for r in up_rows if r[5])
    ch_row = (RUN_ID, cid, ctx.chunk_id, pages, censored, len(up_rows) - n_after, n_after, status, now)
    return up_rows, ch_row


# ── Pass B ───────────────────────────────────────────────────────────────────

def pass_b(ctx, id_batch):
    kind, body, _ = api_call(ctx, "B", VIDEOS_API_URL,
                             {"part": "contentDetails,statistics,snippet", "id": ",".join(id_batch)},
                             n_ids=len(id_batch))
    if kind != "ok":
        return kind, []
    now = datetime.now(timezone.utc)
    rows = []
    for it in (body.get("items") or []):
        snip, cd, st = it.get("snippet") or {}, it.get("contentDetails") or {}, it.get("statistics") or {}
        rows.append((RUN_ID, it.get("id"), snip.get("channelId"), _parse_ts(snip.get("publishedAt")),
                     cd.get("duration"), parse_duration(cd.get("duration")),
                     _int(st.get("viewCount")), _int(st.get("likeCount")), _int(st.get("commentCount")),
                     snip.get("defaultAudioLanguage"), snip.get("defaultLanguage"),
                     snip.get("title"), snip.get("liveBroadcastContent"), now))
    return "ok", rows


# ── Tables / sample ──────────────────────────────────────────────────────────

def preflight():
    existing = [t for t in T.values() if spark.catalog.tableExists(t)]
    if existing and not (OVERWRITE or RESUME):
        raise RuntimeError(f"output tables already exist (set overwrite=true or resume=true): {existing}")
    if existing and OVERWRITE and not RESUME:
        for t in existing:
            assert t.startswith(f"{SCHEMA}.{PREFIX}"), t
            spark.sql(f"DROP TABLE {t}")
        print(f"overwrite=true: dropped {len(existing)} dry-run tables")


def build_sample():
    if RESUME and spark.catalog.tableExists(T["sample"]):
        print("resume=true: reusing existing sample table")
        return
    src = (spark.table(SRC_TABLE)
           .where("activity_panel = 'active' AND NOT is_topic AND canonical_id IS NOT NULL")
           .select("canonical_id", "subscriber_count",
                   F.coalesce(F.col("weekly_views").cast("double"), F.lit(0.0)).alias("weekly_views")))
    w = Window.orderBy("weekly_views", "canonical_id")
    strata = src.withColumn("stratum", F.ntile(10).over(w))
    pop = strata.groupBy("stratum").agg(F.count("*").alias("stratum_n"))
    base, rem = divmod(SAMPLE_N, 10)
    quota = spark.createDataFrame(
        [(d, base + (1 if d <= rem else 0)) for d in range(1, 11)], "stratum int, quota int")
    w2 = Window.partitionBy("stratum").orderBy(F.xxhash64("canonical_id", F.lit(SEED)), "canonical_id")
    samp = (strata.join(F.broadcast(pop), "stratum").join(F.broadcast(quota), "stratum")
            .withColumn("rk", F.row_number().over(w2)).where("rk <= quota")
            .withColumn("run_id", F.lit(RUN_ID)).withColumn("seed", F.lit(SEED))
            .withColumn("sample_n", F.lit(SAMPLE_N))
            .withColumn("sampled_at", F.current_timestamp())
            .select("run_id", "canonical_id", "stratum", "stratum_n", "weekly_views",
                    "subscriber_count", "seed", "sample_n", "sampled_at")
            .orderBy("stratum", "canonical_id"))
    samp.write.mode("append").saveAsTable(T["sample"])
    n = spark.table(T["sample"]).count()
    print(f"sample saved: {n} channels -> {T['sample']}")


def append(rows, schema, table):
    if rows:
        spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(table)


# ── Main ─────────────────────────────────────────────────────────────────────

def connectivity_check():
    import requests
    try:
        r = requests.get(DISCOVERY_URL, timeout=20)   # unauthenticated, no quota
        print(f"connectivity: googleapis.com reachable (HTTP {r.status_code})")
        if r.status_code != 200:
            raise RuntimeError(f"googleapis.com returned HTTP {r.status_code}")
    except requests.RequestException as e:
        raise RuntimeError(f"cluster cannot reach googleapis.com ({type(e).__name__})")


def main():
    print(f"=== fig6 dry run run_id={RUN_ID} sample_n={SAMPLE_N} max_units={MAX_UNITS} rpm={RPM} "
          f"page_cap={PAGE_CAP} window={WINDOW_START.date()}..{WINDOW_END.date()} sfx={SFX} ===")
    connectivity_check()
    key = dbutils.secrets.get("youtube", "api-key")  # type: ignore[name-defined]  # noqa: F821
    preflight()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {T['queue']} (run_id STRING, chunk_id INT, completed_at TIMESTAMP) USING DELTA")
    build_sample()

    ids = [r.canonical_id for r in spark.table(T["sample"]).select("canonical_id")
           .orderBy("canonical_id").toLocalIterator()]
    chunks = [ids[i:i + CHUNK_SIZE] for i in range(0, len(ids), CHUNK_SIZE)]
    done = {r.chunk_id for r in spark.table(T["queue"]).select("chunk_id").distinct().toLocalIterator()}
    ctx = Ctx(key)
    del key
    started = time.time()
    first = True

    for c, chunk in enumerate(chunks):
        if c in done:
            continue
        ctx.chunk_id = c
        if ctx.fatal.is_set() or ctx.stop.is_set() or ctx.cap_hit.is_set():
            for cid in chunk:
                ctx.log("A", "skipped", 0, reason="run_stopped", channel=cid)
            continue
        up_rows, ch_rows = [], []

        def work_a(cid):
            return pass_a(ctx, cid)

        todo = list(chunk)
        if first:   # serial canary: an auth failure shows up on exactly one call
            r0 = work_a(todo.pop(0))
            up_rows += r0[0]; ch_rows.append(r0[1])
            first = False
        if todo and not ctx.fatal.is_set():
            with ThreadPoolExecutor(max_workers=THREADS) as ex:
                for u, ch in ex.map(work_a, todo):
                    up_rows += u; ch_rows.append(ch)
        elif todo:
            for cid in todo:
                ctx.log("A", "skipped", 0, reason="fatal", channel=cid)

        # Pass B on every recorded video of this chunk
        vids = sorted({r[2] for r in up_rows})
        batches = [vids[i:i + 50] for i in range(0, len(vids), 50)]
        stat_rows, b_blocked = [], 0
        if batches and not ctx.fatal.is_set():
            with ThreadPoolExecutor(max_workers=THREADS) as ex:
                for kind, rows in ex.map(lambda b: pass_b(ctx, b), batches):
                    stat_rows += rows
                    if kind not in ("ok",):
                        b_blocked += 1

        a_blocked = sum(1 for ch in ch_rows if ch[7] not in ("ok", "client_error", "bad_id"))
        append(up_rows, UPLOADS_SCHEMA, T["uploads"])
        append(stat_rows, STATS_SCHEMA, T["video_stats"])
        append(ch_rows, CHANNELS_SCHEMA, T["channels"])
        append(ctx.drain(), CALLS_SCHEMA, T["calls"])
        complete = (a_blocked == 0 and b_blocked == 0)
        if complete:
            spark.createDataFrame([(RUN_ID, c)], "run_id string, chunk_id int") \
                 .withColumn("completed_at", F.current_timestamp()).write.mode("append").saveAsTable(T["queue"])
        print(f"chunk {c}: channels={len(ch_rows)} uploads={len(up_rows)} video_rows={len(stat_rows)} "
              f"a_blocked={a_blocked} b_blocked={b_blocked} units_used={ctx.budget.used}/{MAX_UNITS} "
              f"complete={complete} {(time.time() - started) / 60:.1f} min")

    append(ctx.drain(), CALLS_SCHEMA, T["calls"])
    mins = (time.time() - started) / 60
    print(f"units_used={ctx.budget.used} cap_hit={ctx.cap_hit.is_set()} daily_stop={ctx.stop.is_set()} "
          f"runtime_min={mins:.2f}")
    if ctx.fatal.is_set():
        raise RuntimeError(f"STOPPED: permanent API error ({ctx.fatal_reason}). Key not echoed. "
                           f"Do not retry with another key without PI approval.")
    status = ("STOPPED on unit cap" if ctx.cap_hit.is_set() else
              "STOPPED on daily quota" if ctx.stop.is_set() else "FINISHED")
    print(f"=== {status}. run_id={RUN_ID} ===")


if __name__ == "__main__":
    main()

# Post-flight (run after; SELECT * keeps schema-resolution ordering out of play):
#   SELECT * FROM dev_sean.matt.fig6_dryrun_calls_<sfx> LIMIT 5;
#   SELECT pass, outcome, http_status, reason, count(*), sum(units) FROM ...calls_<sfx> GROUP BY ALL;
#   SELECT * FROM dev_sean.matt.fig6_dryrun_video_stats_<sfx> LIMIT 5;
