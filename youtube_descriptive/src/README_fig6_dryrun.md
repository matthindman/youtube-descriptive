# Figure 6 dry run (notebook 17)

Status 2026-10-16: **dry run complete** (1,000 channels, 1,991 units in total). The first smoke run (394512642345342) was stopped by a rejected key in `youtube/api-key` (1 unit); the key from `youtube-llm-keys/youtube-api-key` then ran clean. See `PLAN_fig6_collection.md` for the plan and quota math.

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
- **Key:** `dbutils.secrets.get(secret_scope, secret_key)`; the two widgets hold names only (defaults `youtube-llm-keys` / `youtube-api-key`, the PI-designated working key; the old `youtube/api-key` was rejected). Sent in the `X-Goog-Api-Key` header, never in a URL. Only structured codes are logged: HTTP status, every `error.errors[].reason`, every `error.details[].reason`, `error.status`. Never message text, exception text or response bodies. A key rejection is recognised from `API_KEY_INVALID` / `keyInvalid` / `keyExpired`, not from message wording.
- **Outputs** (`<sfx>` = table suffix): `fig6_dryrun_sample_<sfx>`, `_uploads_<sfx>`, `_video_stats_<sfx>`, `_calls_<sfx>` (one row per attempt), `_channels_<sfx>`, `_queue_<sfx>`, plus the shared ledger. Pre-flight refuses existing `<sfx>` tables unless `overwrite=true` (drops only those tables) or `resume=true`.
- **Analysis rules:** de-duplicate uploads on `(channel_id, video_id)` and stats on `video_id`, keeping the latest `run_id`. Use `countable = true` and `after_window = false`. Weight by `stratum_n / n_h`.

## Caveats

- A video added to the uploads playlist before `window_start` whose `videoPublishedAt` falls inside the window (for example scheduled or premiered later) lies past the stop point and is missed. Expected to be rare.
- A video can be missed if it was deleted or made private before collection.
- The ledger is written per chunk, so a hard kill can under-count by up to one chunk.

## Re-run procedure

1. The key is read from secret `youtube-llm-keys` / `youtube-api-key` (widgets `secret_scope`, `secret_key`).
2. Smoke: suffix `smoke2`, `overwrite=false`, `resume=false`, `sample_n=20`, `max_units=100`. Check the calls table (all `ok`, no `fatal`), the uploads and stats rows, and the ledger.
3. Main: suffix `v1`, `sample_n=1000`, `max_units=4800`. The ledger enforces the 5,000 total across all runs.
4. Do not run on a Monday (see plan §4).
5. The full collection needs a distributed `mapPartitions` version (per-task unit budgets, shared stop flag, as in 08). **Not built yet.**

## Run log

| Run | Parameters | Result |
|---|---|---|
| 394512642345342 (smoke, old secret) | sample_n=20, max_units=200, suffix=smoke | Key rejected on the canary call (HTTP 400); stopped. 1 unit. |
| 383858104778152 (smoke2) | sample_n=20, max_units=100, suffix=smoke2 | 27 Pass A + 11 Pass B calls, all HTTP 200; 38 units; 538 uploads and 538 video rows. |
| 983494926244636 (main) | sample_n=1000, max_units=4800, suffix=v1 | 1,372 Pass A units (4 were 404 playlistNotFound), 580 Pass B units, all other calls 200; 1,952 units; 126 s job time; no censored channel, no cap or quota stop. |

Ledger total: 1 + 38 + 1,952 = **1,991 units** of 5,000.

## Results (v1, 1,000 channels, 100 per weekly-views decile)

- Pages per channel: mean 1.37; median 1, p90 2, p99 9, max 17. 858 channels need 1 page. Censored: 0.
- Channels with at least one window upload: 62.4%. Window videos per channel: mean 21.6, median 3, p90 57, p99 284, max 634. 21,572 window videos plus 7,340 after 2026-10-05 (28,912 recorded; every one got stats; no duplicates).
- Countable window videos: 69.95% last 180 s or less (53.4% 60 s or less); 56.5% have `defaultAudioLanguage`; `defaultLanguage` is set on 100%. Top audio languages (all recorded videos): hi 3,730, en 2,922, en-US 1,389, ar 711, th 697, vi 654, es-419 589, bn 585, pt-BR 402, ja 393.
- Views at collection by video age (weeks): median 1.8k-2.5k and p90 about 44k at every age 1-6 weeks, i.e. no visible growth with age in this snapshot; p10 about 110-150.
- Rate: 1,952 calls in 1.68 min, about 1,160 calls/min (below the 2,000 cap; 16 threads were the limit).
- Stratified extrapolation to 3,469,842 channels (95%): Pass A 4.76M units (4.49-5.03M); window videos 74.9M (64.2-85.5M); Pass B 1.50M units (1.28-1.71M) for window videos only, 2.01M (1.73-2.29M) if after-window videos are fetched too.
