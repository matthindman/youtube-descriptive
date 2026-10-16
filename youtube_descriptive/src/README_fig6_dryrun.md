# Figure 6 dry run (notebook 17)

Status 2026-10-16: **code revised after review; no collection yet.** The smoke run (job run 394512642345342) stopped on its first API call: the key in `youtube/api-key` was rejected (HTTP 400). 1 unit was spent. See `PLAN_fig6_collection.md` for the plan and quota math.

## Design

`17_fetch_fig6_window_videos_databricks.py`, adapted from 08. Widgets / job `base_parameters`: `sample_n` (10..1000, default 1000), `seed`, `window_start`, `window_end`, `page_cap` (1..20, default 20), `max_units` (1..5000), `auth_total` (1..5000, default 5000), `rpm` (10..2000), `chunk_size` (>=10, default 250), `table_suffix`, `overwrite`, `resume`, `run_id`. There is no key parameter.

- **Window:** `[2026-08-31 00:00, 2026-10-05 00:00)` UTC, exactly 5 weeks. The end is exclusive: a video published at 2026-10-05 00:00:00 is `after_window`.
- **Sample:** `fig1_discovery_channels_v4`, `activity_panel='active' AND NOT is_topic`; a duplicate-`canonical_id` check on the source aborts the run. Strata are `ntile(10)` over `weekly_views` (null as 0), ties by id; `n_h = sample_n/10` per stratum, ordered by `xxhash64(id, seed)`. The sample table freezes `stratum`, `stratum_n` (population), `n_h`, `chunk_id` and `chunk_size`. `resume=true` is refused if `chunk_size` differs from the stored value.
- **Pass A:** `playlistItems.list` on `UU` + `id[2:]`, `snippet,contentDetails`, 50 per page.
  - *Stop rule:* paging stops on the playlist-position key `snippet.publishedAt` (the order of the uploads list) at the first item before `window_start`, or after `page_cap` pages (`censored`).
  - *Window filter:* recording uses `contentDetails.videoPublishedAt`, `window_start <= t`. Items at or after `window_end` are kept with `after_window=true`.
  - *Undated items* (no `videoPublishedAt`: private/deleted placeholders) are skipped and counted in `channels.n_undated`.
  - *Items added to the playlist late* (list key in window, publish time before the window) are skipped.
- **Pass B:** `videos.list`, `contentDetails,statistics,snippet`, 50 ids per call, for every recorded video. Duration is parsed to seconds. Live and upcoming videos stay in the raw table with `countable=false`; analysis excludes them from counts and from the Shorts proxy.
- **Quota safety:**
  - *Cumulative cap:* `budget = min(max_units, auth_total - sum(ledger.units))`. The ledger `dev_sean.matt.fig6_dryrun_unit_ledger` is created and seeded at first start (smoke run 394512642345342 = 1 unit) and is never dropped by pre-flight. Units are written to it per chunk and in a `finally` block, so a clean error or cancel is always recorded; a hard kill (cluster loss) can lose up to one chunk of units.
  - *Hard unit cap:* every HTTP attempt, retries included, reserves one unit before it is sent; at the cap calls are `skipped`.
  - *Retries:* 429, 5xx, network errors and 403 `rateLimitExceeded` / `userRateLimitExceeded`. 403 `quotaExceeded` / `dailyLimitExceeded` is a clean stop. 404 is a soft per-call miss.
  - *Fatal, no retry:* any 400 or 401, any other 403 (`forbidden`, `ipRefererBlocked`, `accessNotConfigured`, `SERVICE_DISABLED`, ...), any other 4xx.
  - *Canary and breaker:* the first channel runs serially and must return ok or 404 before any concurrency. The run stops if more than 20 of the first 50 Pass A calls are not ok. A whole-batch Pass B failure is fatal.
  - *Chunk queue:* a chunk is marked done only if no call in it was deferred, errored or skipped.
  - *finally block:* call log flush, ledger write and the `units_used` print always run.
- **Key:** `dbutils.secrets.get("youtube","api-key")`, sent in the `X-Goog-Api-Key` header, never in a URL. Only structured codes are logged: HTTP status, every `error.errors[].reason`, every `error.details[].reason`, `error.status`. Never message text, exception text or response bodies. A key rejection is recognised from `API_KEY_INVALID` / `keyInvalid` / `keyExpired`, not from message wording.
- **Outputs** (`<sfx>` = table suffix): `fig6_dryrun_sample_<sfx>`, `_uploads_<sfx>`, `_video_stats_<sfx>`, `_calls_<sfx>` (one row per attempt), `_channels_<sfx>`, `_queue_<sfx>`, plus the shared ledger. Pre-flight refuses existing `<sfx>` tables unless `overwrite=true` (drops only those tables) or `resume=true`.
- **Analysis rules:** de-duplicate uploads on `(channel_id, video_id)` and stats on `video_id`, keeping the latest `run_id`. Use `countable = true` and `after_window = false`. Weight by `stratum_n / n_h`.

## Caveats

- A video added to the uploads playlist before `window_start` whose `videoPublishedAt` falls inside the window (for example scheduled or premiered later) lies past the stop point and is missed. Expected to be rare.
- A video can be missed if it was deleted or made private before collection.
- The ledger is written per chunk, so a hard kill can under-count by up to one chunk.

## Re-run procedure

1. A valid key must be in the `youtube/api-key` secret (renewed by someone with access).
2. Smoke: suffix `smoke2`, `overwrite=false`, `resume=false`, `sample_n=20`, `max_units=100`. Check the calls table (all `ok`, no `fatal`), the uploads and stats rows, and the ledger.
3. Main: suffix `v1`, `sample_n=1000`, `max_units=4800`. The ledger enforces the 5,000 total across all runs.
4. Do not run on a Monday (see plan §4).
5. The full collection needs a distributed `mapPartitions` version (per-task unit budgets, shared stop flag, as in 08). **Not built yet.**

## Run log

| Run | Parameters | Result |
|---|---|---|
| 394512642345342 (smoke) | sample_n=20, max_units=200, suffix=smoke | googleapis.com reachable (unauthenticated discovery call, 0 quota). Sample of 20 saved. The canary `playlistItems` call returned HTTP 400 and the run stopped: 1 unit attempted, 19 channels logged `skipped`, one `error` row in `channels_smoke`. |

Units spent so far: 1 of 5,000 (seeded into the ledger on its first start). Tables `fig6_dryrun_{sample,calls,channels,queue}_smoke` exist from that run (`uploads_smoke` and `video_stats_smoke` were never created because there was nothing to append).
