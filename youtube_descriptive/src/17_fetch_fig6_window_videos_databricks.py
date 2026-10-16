# Databricks notebook source
"""
17_fetch_fig6_window_videos — Figure 6 video-level collection, DRY-RUN edition (<= ~1,000 channels).

Adapted from 08_fetch_channel_videos_refresh_databricks.py (see PLAN_fig6_collection.md §5).
Design notes, re-run procedure and run log: README_fig6_dryrun.md.

What it does
  Sample : `sample_n` channels from dev_sean.matt.fig1_discovery_channels_v4
           (activity_panel='active' AND NOT is_topic), stratified into weekly_views deciles
           (ntile(10), ties broken by id), fixed-seed hash order within decile. The sample table
           freezes stratum, stratum_n (population), n_h (quota), chunk_id and chunk_size.
  Pass A : playlistItems.list on the uploads playlist ('UU' + id[2:]), part=snippet,contentDetails,
           maxResults=50, newest first. PAGING STOPS on the playlist-position key snippet.publishedAt
           (the order of the uploads list) at the first item before `window_start`, or after
           `page_cap` pages (channel flagged `censored`). RECORDING is filtered on
           contentDetails.videoPublishedAt: window_start <= t < window_end (end EXCLUSIVE; the window
           is exactly 5 weeks, [2026-08-31 00:00, 2026-10-05 00:00) UTC). Items at or after
           window_end are kept with after_window=true. Items with no videoPublishedAt (private /
           deleted placeholders) are skipped and counted in channels.n_undated.
           Known remaining miss: a video added to the uploads playlist before window_start but whose
           videoPublishedAt falls inside the window (e.g. scheduled/premiered later) sits past the
           stop point and is not seen. Rare; documented as a caveat.
  Pass B : videos.list part=contentDetails,statistics,snippet, 50 ids/call, for every recorded video.
           Live/upcoming videos stay in the raw table but `countable` = false; analysis excludes them
           from counts and from the Shorts proxy.

Quota safety (kept from 08, tightened)
  * Calls never raise on rate/quota/transient errors; every attempt returns an outcome
    (ok / retry / deferred / error / client_error / fatal / skipped) and is logged to the calls table.
  * One global token-bucket rate limiter (`rpm`, default 2000/min, max 2000).
  * HARD UNIT CAP, cumulative: budget = min(max_units, AUTH_TOTAL - sum(ledger units)). Every HTTP
    attempt (retries included) reserves 1 unit BEFORE it is sent; at the cap calls are 'skipped'.
    The ledger dev_sean.matt.fig6_dryrun_unit_ledger is never dropped by pre-flight; it is created
    and seeded (smoke run 394512642345342 = 1 unit) at first start, and units are written to it per
    chunk and in a finally block. A hard kill (cluster loss) can lose at most one chunk's units.
  * Retry policy: 429/5xx/network errors and 403 rateLimitExceeded / userRateLimitExceeded are retried
    with backoff. 403 quotaExceeded / dailyLimitExceeded = clean stop. 404 is a soft per-call miss.
    EVERYTHING ELSE is fatal and stops the run with no retry: any 400 or 401, any other 403
    (forbidden, ipRefererBlocked, accessNotConfigured, SERVICE_DISABLED, ...), any other 4xx.
  * Canary: the first channel runs serially and must return ok or 404 before any concurrency.
    Breaker: more than 20 of the first 50 Pass A calls not ok -> stop. A whole-batch Pass B failure
    (other than cap/quota stop) is fatal.
  * Chunk-level resumability: a chunk is recorded in the queue table only if none of its calls was
    deferred / errored / skipped. Chunk assignment is frozen in the sample table.

Key handling: read ONLY via dbutils.secrets.get("youtube", "api-key"); sent in the X-Goog-Api-Key
header, so no URL contains it. No api_key widget. Only structured codes are logged
(HTTP status, error.errors[].reason, error.details[].reason, error.status) — never message text,
exception text or response bodies. Key-fatal is decided from details reason API_KEY_INVALID or errors
reason keyInvalid / keyExpired, not from message wording.

Outputs (append-only; `table_suffix` default v1):
  dev_sean.matt.fig6_dryrun_sample_<sfx>, _uploads_<sfx>, _video_stats_<sfx>, _calls_<sfx>,
  _channels_<sfx>, _queue_<sfx>;  plus the shared ledger dev_sean.matt.fig6_dryrun_unit_ledger.
Pre-flight: refuses to run if any <sfx> output exists, unless overwrite=true (drops only those
tables, never the ledger) or resume=true (reuse sample, skip queued chunks).
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
    BooleanType,
)

spark = SparkSession.builder.getOrCreate()

API_BASE          = "https://www.googleapis.com/youtube/v3"
PLAYLISTITEMS_URL = f"{API_BASE}/playlistItems"
VIDEOS_API_URL    = f"{API_BASE}/videos"
DISCOVERY_URL     = "https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest"  # no key, no quota

SRC_TABLE = "dev_sean.matt.fig1_discovery_channels_v4"
SCHEMA    = "dev_sean.matt"
PREFIX    = "fig6_dryrun_"
LEDGER    = f"{SCHEMA}.{PREFIX}unit_ledger"
SEED_LEDGER_ROWS = [("394512642345342", "smoke", 1)]   # smoke run 2026-10-16: 1 unit spent

THREADS         = 16
MAX_RETRIES     = 4      # transient retries per call (each attempt costs 1 unit)
REQUEST_TIMEOUT = 30
BREAKER_WINDOW, BREAKER_MAX_BAD = 50, 20

_DAILY_403 = {"quotaExceeded", "dailyLimitExceeded"}
_RATE_403  = {"rateLimitExceeded", "userRateLimitExceeded"}   # the only retryable 403s
# (permanent 403s incl. forbidden, ipRefererBlocked, accessNotConfigured: anything not daily/rate is fatal)
_KEY_ERR_REASONS = {"keyInvalid", "keyExpired"}
_KEY_DETAIL_REASONS = {"API_KEY_INVALID"}


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
WINDOW_END   = datetime.fromisoformat(_widget("window_end", "2026-10-05")).replace(tzinfo=timezone.utc)  # exclusive
PAGE_CAP     = int(_widget("page_cap", "20"))
MAX_UNITS    = int(_widget("max_units", "5000"))
AUTH_TOTAL   = int(_widget("auth_total", "5000"))
RPM          = int(_widget("rpm", "2000"))
CHUNK_SIZE   = int(_widget("chunk_size", "250"))
SFX          = _widget("table_suffix", "v1")
OVERWRITE    = _bool(_widget("overwrite", "false"))
RESUME       = _bool(_widget("resume", "false"))
RUN_ID       = _widget("run_id", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%S"))

if not (10 <= RPM <= 2000):
    raise ValueError("rpm must be in [10, 2000] for the dry run")
if not (10 <= SAMPLE_N <= 1000):
    raise ValueError("sample_n must be in [10, 1000] (>=1 channel per decile)")
if not (1 <= MAX_UNITS <= 5000):
    raise ValueError("max_units must be in [1, 5000]")
if not (1 <= AUTH_TOTAL <= 5000):
    raise ValueError("auth_total must be in [1, 5000]")
if not (10 <= CHUNK_SIZE):
    raise ValueError("chunk_size must be >= 10")
if not (1 <= PAGE_CAP <= 20):
    raise ValueError("page_cap must be in [1, 20]")
if not (WINDOW_START < WINDOW_END):
    raise ValueError("window_start must precede window_end")
if not re.fullmatch(r"[A-Za-z0-9_]{1,24}", SFX):
    raise ValueError("table_suffix must be alphanumeric/underscore")

T = {k: f"{SCHEMA}.{PREFIX}{k}_{SFX}"
     for k in ("sample", "uploads", "video_stats", "calls", "channels", "queue")}

# ── Schemas ──────────────────────────────────────────────────────────────────

UPLOADS_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("channel_id", StringType()),
    StructField("video_id", StringType()), StructField("published_at", TimestampType()),   # videoPublishedAt
    StructField("playlist_added_at", TimestampType()),                                      # snippet.publishedAt
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
    StructField("countable", BooleanType()),      # false for live / upcoming
    StructField("collected_at", TimestampType()),
])
CALLS_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("call_ts", TimestampType()),
    StructField("pass", StringType()), StructField("outcome", StringType()),
    StructField("units", IntegerType()), StructField("http_status", IntegerType()),
    StructField("reason", StringType()),            # error.errors[].reason, comma-joined
    StructField("detail_reasons", StringType()),    # error.details[].reason, comma-joined
    StructField("error_status", StringType()),      # error.status
    StructField("channel_id", StringType()),
    StructField("page_no", IntegerType()), StructField("n_ids", IntegerType()),
    StructField("attempt", IntegerType()), StructField("latency_ms", IntegerType()),
    StructField("chunk_id", IntegerType()),
])
CHANNELS_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("channel_id", StringType()),
    StructField("chunk_id", IntegerType()),
    StructField("pages", IntegerType()), StructField("censored", BooleanType()),
    StructField("n_window", IntegerType()), StructField("n_after_window", IntegerType()),
    StructField("n_undated", IntegerType()),
    StructField("status", StringType()), StructField("collected_at", TimestampType()),
])
LEDGER_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("suffix", StringType()),
    StructField("units", IntegerType()), StructField("ts", TimestampType()),
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
    def __init__(self, key, cap):
        self.key = key
        self.budget = Budget(cap)
        self.limiter = Limiter(RPM)
        self.stop = threading.Event()     # daily quota gone
        self.fatal = threading.Event()    # permanent error / breaker / canary failure
        self.fatal_reason = None
        self.cap_hit = threading.Event()
        self.calls, self.lock = [], threading.Lock()
        self.chunk_id = -1
        self.ledgered = 0
        self.a_seen, self.a_bad = 0, 0    # breaker over the first BREAKER_WINDOW Pass A calls
        import requests
        self.session = requests.Session()

    def set_fatal(self, why):
        with self.lock:
            if not self.fatal_reason:
                self.fatal_reason = why
        self.fatal.set()

    def log(self, pass_, outcome, units, http=None, reason=None, details=None, status=None,
            channel=None, page=None, n_ids=None, attempt=None, latency=None):
        row = (RUN_ID, datetime.now(timezone.utc), pass_, outcome, units, http, reason, details,
               status, channel, page, n_ids, attempt, latency, self.chunk_id)
        with self.lock:
            self.calls.append(row)

    def drain(self):
        with self.lock:
            rows, self.calls = self.calls, []
        return rows

    def breaker_record(self, ok):
        with self.lock:
            if self.a_seen >= BREAKER_WINDOW:
                return
            self.a_seen += 1
            if not ok:
                self.a_bad += 1
            trip = self.a_bad > BREAKER_MAX_BAD
        if trip:
            self.set_fatal(f"breaker: {self.a_bad} of first {self.a_seen} Pass A calls not ok")


def parse_error(resp):
    """Structured codes only: (errors[].reason list, details[].reason list, error.status). No message text."""
    try:
        e = resp.json().get("error", {}) or {}
    except Exception:
        return [], [], None
    reasons = [x.get("reason") for x in (e.get("errors") or []) if isinstance(x, dict) and x.get("reason")]
    details = [x.get("reason") for x in (e.get("details") or []) if isinstance(x, dict) and x.get("reason")]
    return reasons, details, e.get("status")


def classify(http, reasons, details):
    """-> outcome: ok | retry | deferred | client_error | fatal  (+ key_bad flag)."""
    if http == 200:
        return "ok", False
    key_bad = bool(set(reasons) & _KEY_ERR_REASONS or set(details) & _KEY_DETAIL_REASONS)
    if http == 404:
        return "client_error", False
    if http == 403:
        if set(reasons) & _DAILY_403:
            return "deferred", False
        if set(reasons) & _RATE_403:
            return "retry", False
        return "fatal", key_bad
    if http in (400, 401):
        return "fatal", key_bad
    if http == 429 or (http is not None and http >= 500):
        return "retry", False
    return "fatal", key_bad          # any other 4xx


def api_call(ctx, pass_, url, params, channel=None, page=None, n_ids=None):
    """One logical call with in-task backoff. Returns (kind, body, http_status);
    kind in ok | deferred | error | client_error | skipped. Never raises."""
    import requests
    delay, last_http = 1.0, None
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
        http, reasons, details, status, body = None, [], [], None, None
        key_bad = False
        try:
            resp = ctx.session.get(url, params=params, headers={"X-Goog-Api-Key": ctx.key},
                                   timeout=REQUEST_TIMEOUT)
            http = last_http = resp.status_code
            if http == 200:
                body, outcome = resp.json(), "ok"
            else:
                reasons, details, status = parse_error(resp)
                outcome, key_bad = classify(http, reasons, details)
        except requests.RequestException as e:
            outcome, reasons = "retry", [type(e).__name__]     # class only: str(e) may contain the URL
        except Exception as e:                                  # bad JSON etc.
            outcome, reasons = "retry", [type(e).__name__]
        lat = int((time.monotonic() - t0) * 1000)
        final = outcome if outcome != "retry" else ("retry" if attempt <= MAX_RETRIES else "error")
        ctx.log(pass_, final, 1, http=http, reason=",".join(reasons) or None,
                details=",".join(details) or None, status=status, channel=channel, page=page,
                n_ids=n_ids, attempt=attempt, latency=lat)
        if outcome == "ok":
            return ("ok", body, http)
        if outcome == "fatal":
            ctx.set_fatal(f"HTTP {http} {','.join(reasons) or '-'} {','.join(details) or '-'} "
                          f"{status or '-'}{' [key rejected]' if key_bad else ''}")
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

def pass_a(ctx, cid, record_breaker=True):
    """Returns (uploads_rows, channel_row). Blocking status = not in ('ok','client_error','bad_id')."""
    now = datetime.now(timezone.utc)
    if not cid or not cid.startswith("UC"):
        return [], (RUN_ID, cid, ctx.chunk_id, 0, False, 0, 0, 0, "bad_id", now)
    uploads_id = "UU" + cid[2:]
    pages, token, reached_old, status, n_undated = 0, None, False, "ok", 0
    recs = []
    while pages < PAGE_CAP:
        params = {"part": "snippet,contentDetails", "playlistId": uploads_id, "maxResults": 50}
        if token:
            params["pageToken"] = token
        kind, body, _ = api_call(ctx, "A", PLAYLISTITEMS_URL, params, channel=cid, page=pages + 1)
        if record_breaker:
            ctx.breaker_record(kind == "ok")
        if kind != "ok":
            status = kind
            break
        pages += 1
        for it in (body.get("items") or []):
            snip, cd = it.get("snippet") or {}, it.get("contentDetails") or {}
            added = _parse_ts(snip.get("publishedAt"))       # playlist-position key (list order)
            if added is not None and added < WINDOW_START:
                reached_old = True                            # stop paging on list order
                break
            vid = cd.get("videoId") or (snip.get("resourceId") or {}).get("videoId")
            vpa = _parse_ts(cd.get("videoPublishedAt"))      # actual publish time: window filter
            if not vid:
                continue
            if vpa is None:
                n_undated += 1                                # private/deleted placeholder
                continue
            if vpa < WINDOW_START:
                continue                                      # added late, published earlier
            recs.append((vid, vpa, added, pages))
        token = body.get("nextPageToken")
        if reached_old or not token:
            break
    censored = (status == "ok" and pages >= PAGE_CAP and not reached_old and bool(token))
    up_rows = [(RUN_ID, cid, v, vpa, added, pg, vpa >= WINDOW_END, censored, now)
               for v, vpa, added, pg in recs]
    n_after = sum(1 for r in up_rows if r[6])
    ch_row = (RUN_ID, cid, ctx.chunk_id, pages, censored, len(up_rows) - n_after, n_after,
              n_undated, status, now)
    return up_rows, ch_row


# ── Pass B ───────────────────────────────────────────────────────────────────

def pass_b(ctx, id_batch):
    kind, body, _ = api_call(ctx, "B", VIDEOS_API_URL,
                             {"part": "contentDetails,statistics,snippet", "id": ",".join(id_batch)},
                             n_ids=len(id_batch))
    if kind in ("error", "client_error"):
        ctx.set_fatal(f"whole-batch Pass B failure ({kind}, {len(id_batch)} ids)")
    if kind != "ok":
        return kind, []
    now = datetime.now(timezone.utc)
    rows = []
    for it in (body.get("items") or []):
        snip, cd, st = it.get("snippet") or {}, it.get("contentDetails") or {}, it.get("statistics") or {}
        live = snip.get("liveBroadcastContent")
        rows.append((RUN_ID, it.get("id"), snip.get("channelId"), _parse_ts(snip.get("publishedAt")),
                     cd.get("duration"), parse_duration(cd.get("duration")),
                     _int(st.get("viewCount")), _int(st.get("likeCount")), _int(st.get("commentCount")),
                     snip.get("defaultAudioLanguage"), snip.get("defaultLanguage"),
                     snip.get("title"), live, live in (None, "none"), now))
    return "ok", rows


# ── Tables / sample / ledger ─────────────────────────────────────────────────

def preflight():
    existing = [t for t in T.values() if spark.catalog.tableExists(t)]
    if existing and not (OVERWRITE or RESUME):
        raise RuntimeError(f"output tables already exist (set overwrite=true or resume=true): {existing}")
    if existing and OVERWRITE and not RESUME:
        for t in existing:
            assert t.startswith(f"{SCHEMA}.{PREFIX}") and t != LEDGER, t
            spark.sql(f"DROP TABLE {t}")
        print(f"overwrite=true: dropped {len(existing)} dry-run tables (ledger untouched)")


def ensure_ledger_and_budget():
    """Create + seed the ledger on first start (never dropped), return the remaining budget."""
    fresh = not spark.catalog.tableExists(LEDGER)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {LEDGER} "
              f"(run_id STRING, suffix STRING, units INT, ts TIMESTAMP) USING DELTA")
    if fresh:
        now = datetime.now(timezone.utc)
        spark.createDataFrame([(r, s, u, now) for r, s, u in SEED_LEDGER_ROWS], LEDGER_SCHEMA) \
             .write.mode("append").saveAsTable(LEDGER)
        print(f"ledger created and seeded with {SEED_LEDGER_ROWS}")
    spent = spark.table(LEDGER).agg(F.coalesce(F.sum("units"), F.lit(0))).collect()[0][0]
    cap = min(MAX_UNITS, AUTH_TOTAL - int(spent))
    print(f"ledger: {spent} units spent so far; this run's budget = min({MAX_UNITS}, {AUTH_TOTAL} - {spent}) = {cap}")
    if cap <= 0:
        raise RuntimeError("no authorised units left in the ledger; refusing to start")
    return cap


def ledger_flush(ctx):
    delta = ctx.budget.used - ctx.ledgered
    if delta > 0:
        spark.createDataFrame([(RUN_ID, SFX, int(delta), datetime.now(timezone.utc))], LEDGER_SCHEMA) \
             .write.mode("append").saveAsTable(LEDGER)
        ctx.ledgered += delta


def build_sample():
    if RESUME and spark.catalog.tableExists(T["sample"]):
        stored = spark.table(T["sample"]).select("chunk_size").distinct().collect()
        if len(stored) != 1 or stored[0][0] != CHUNK_SIZE:
            raise RuntimeError(f"resume refused: chunk_size {CHUNK_SIZE} != stored {[r[0] for r in stored]}")
        print("resume=true: reusing existing sample table (chunk assignment frozen)")
        return
    src = (spark.table(SRC_TABLE)
           .where("activity_panel = 'active' AND NOT is_topic AND canonical_id IS NOT NULL")
           .select("canonical_id", "subscriber_count",
                   F.coalesce(F.col("weekly_views").cast("double"), F.lit(0.0)).alias("weekly_views")))
    src = src.persist()
    n_all, n_dist = src.count(), src.select("canonical_id").distinct().count()
    if n_all != n_dist:
        raise RuntimeError(f"duplicate canonical_id in source: {n_all} rows, {n_dist} distinct")
    w = Window.orderBy("weekly_views", "canonical_id")
    strata = src.withColumn("stratum", F.ntile(10).over(w))
    pop = strata.groupBy("stratum").agg(F.count("*").alias("stratum_n"))
    base, rem = divmod(SAMPLE_N, 10)
    quota = spark.createDataFrame(
        [(d, base + (1 if d <= rem else 0)) for d in range(1, 11)], "stratum int, n_h int")
    w2 = Window.partitionBy("stratum").orderBy(F.xxhash64("canonical_id", F.lit(SEED)), "canonical_id")
    picked = (strata.join(F.broadcast(pop), "stratum").join(F.broadcast(quota), "stratum")
              .withColumn("rk", F.row_number().over(w2)).where("rk <= n_h"))
    w3 = Window.orderBy("canonical_id")
    samp = (picked.withColumn("pos", F.row_number().over(w3) - 1)
            .withColumn("chunk_id", (F.col("pos") / CHUNK_SIZE).cast("int"))
            .withColumn("chunk_size", F.lit(CHUNK_SIZE))
            .withColumn("run_id", F.lit(RUN_ID)).withColumn("seed", F.lit(SEED))
            .withColumn("sample_n", F.lit(SAMPLE_N))
            .withColumn("sampled_at", F.current_timestamp())
            .select("run_id", "canonical_id", "stratum", "stratum_n", "n_h", "weekly_views",
                    "subscriber_count", "seed", "sample_n", "chunk_id", "chunk_size", "sampled_at")
            .orderBy("canonical_id"))
    samp.write.mode("append").saveAsTable(T["sample"])
    src.unpersist()
    n = spark.table(T["sample"]).count()
    print(f"sample saved: {n} channels -> {T['sample']} (source {n_all:,} active non-Topic channels, no duplicates)")


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


def run_chunk(ctx, c, chunk, canary_pending):
    ctx.chunk_id = c
    if ctx.fatal.is_set() or ctx.stop.is_set() or ctx.cap_hit.is_set():
        for cid in chunk:
            ctx.log("A", "skipped", 0, reason="run_stopped", channel=cid)
        return False, canary_pending
    up_rows, ch_rows = [], []
    todo = list(chunk)
    if canary_pending:   # serial canary: must be ok or 404 before any concurrency
        r0 = pass_a(ctx, todo.pop(0), record_breaker=True)
        up_rows += r0[0]; ch_rows.append(r0[1])
        canary_pending = False
        if r0[1][8] not in ("ok", "client_error"):
            ctx.set_fatal(f"canary failed (status {r0[1][8]})")
    if todo and not ctx.fatal.is_set():
        with ThreadPoolExecutor(max_workers=THREADS) as ex:
            for u, ch in ex.map(lambda cid: pass_a(ctx, cid), todo):
                up_rows += u; ch_rows.append(ch)
    else:
        for cid in todo:
            ctx.log("A", "skipped", 0, reason="fatal", channel=cid)

    vids = sorted({r[2] for r in up_rows})
    batches = [vids[i:i + 50] for i in range(0, len(vids), 50)]
    stat_rows, b_blocked = [], 0
    if batches and not ctx.fatal.is_set():
        with ThreadPoolExecutor(max_workers=THREADS) as ex:
            for kind, rows in ex.map(lambda b: pass_b(ctx, b), batches):
                stat_rows += rows
                if kind != "ok":
                    b_blocked += 1
    elif batches:
        b_blocked = len(batches)

    a_blocked = sum(1 for ch in ch_rows if ch[8] not in ("ok", "client_error", "bad_id"))
    append(up_rows, UPLOADS_SCHEMA, T["uploads"])
    append(stat_rows, STATS_SCHEMA, T["video_stats"])
    append(ch_rows, CHANNELS_SCHEMA, T["channels"])
    append(ctx.drain(), CALLS_SCHEMA, T["calls"])
    ledger_flush(ctx)
    complete = (a_blocked == 0 and b_blocked == 0 and not ctx.fatal.is_set())
    if complete:
        spark.createDataFrame([(RUN_ID, c)], "run_id string, chunk_id int") \
             .withColumn("completed_at", F.current_timestamp()).write.mode("append").saveAsTable(T["queue"])
    print(f"chunk {c}: channels={len(ch_rows)} uploads={len(up_rows)} video_rows={len(stat_rows)} "
          f"a_blocked={a_blocked} b_blocked={b_blocked} units_used={ctx.budget.used}/{ctx.budget.cap} "
          f"complete={complete}")
    return complete, canary_pending


def main():
    print(f"=== fig6 dry run run_id={RUN_ID} sample_n={SAMPLE_N} max_units={MAX_UNITS} rpm={RPM} "
          f"page_cap={PAGE_CAP} window=[{WINDOW_START.date()}, {WINDOW_END.date()}) sfx={SFX} ===")
    connectivity_check()
    preflight()
    cap = ensure_ledger_and_budget()
    key = dbutils.secrets.get("youtube", "api-key")  # type: ignore[name-defined]  # noqa: F821
    spark.sql(f"CREATE TABLE IF NOT EXISTS {T['queue']} (run_id STRING, chunk_id INT, completed_at TIMESTAMP) USING DELTA")
    build_sample()

    by_chunk = {}
    for r in spark.table(T["sample"]).select("canonical_id", "chunk_id").orderBy("canonical_id").toLocalIterator():
        by_chunk.setdefault(r.chunk_id, []).append(r.canonical_id)
    done = {r.chunk_id for r in spark.table(T["queue"]).select("chunk_id").distinct().toLocalIterator()}
    ctx = Ctx(key, cap)
    del key
    started = time.time()
    canary_pending = True
    try:
        for c in sorted(by_chunk):
            if c in done:
                continue
            _, canary_pending = run_chunk(ctx, c, by_chunk[c], canary_pending)
    finally:
        # always: flush the call log and the unit ledger, report units, even on error/cancel
        try:
            append(ctx.drain(), CALLS_SCHEMA, T["calls"])
        except Exception as e:
            print(f"WARNING: could not flush call log ({type(e).__name__})")
        try:
            ledger_flush(ctx)
        except Exception as e:
            print(f"WARNING: could not write ledger ({type(e).__name__}); units to record manually: "
                  f"{ctx.budget.used - ctx.ledgered}")
        print(f"units_used={ctx.budget.used} cap_hit={ctx.cap_hit.is_set()} daily_stop={ctx.stop.is_set()} "
              f"fatal={ctx.fatal.is_set()} runtime_min={(time.time() - started) / 60:.2f}")

    if ctx.fatal.is_set():
        raise RuntimeError(f"STOPPED: {ctx.fatal_reason}. Do not retry with another key without PI approval.")
    status = ("STOPPED on unit cap" if ctx.cap_hit.is_set() else
              "STOPPED on daily quota" if ctx.stop.is_set() else "FINISHED")
    print(f"=== {status}. run_id={RUN_ID} ===")


if __name__ == "__main__":
    main()

# Post-flight (run after; SELECT * keeps schema-resolution ordering out of play):
#   SELECT * FROM dev_sean.matt.fig6_dryrun_calls_<sfx> LIMIT 5;
#   SELECT pass, outcome, http_status, reason, detail_reasons, count(*), sum(units) FROM ...calls_<sfx> GROUP BY ALL;
#   SELECT * FROM dev_sean.matt.fig6_dryrun_unit_ledger;
#   SELECT * FROM dev_sean.matt.fig6_dryrun_video_stats_<sfx> LIMIT 5;
# Analysis: de-duplicate uploads/stats on (channel_id, video_id) / video_id keeping the latest run_id;
# restrict to countable = true and after_window = false.
