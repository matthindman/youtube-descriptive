# Figure 6 dry run (notebook 17)

Status 2026-10-16: **code written and imported; the dry run did not get past its first API call.** The key in the `youtube/api-key` secret was rejected (HTTP 400 `badRequest`, classified as an invalid/expired key). No data was collected. Per the run rules, no other key was sought and nothing was retried. See `PLAN_fig6_collection.md` for the plan and quota math.

## Design

`17_fetch_fig6_window_videos_databricks.py`, adapted from 08. Parameters (widgets / job `base_parameters`): `sample_n` (1000, max 1000), `seed`, `window_start` (2026-08-31), `window_end` (2026-10-05), `page_cap` (20), `max_units` (5000, max 5000), `rpm` (2000, max 2000), `chunk_size` (250), `table_suffix` (v1), `overwrite`, `resume`, `run_id`. There is no key parameter.

- **Sample:** `fig1_discovery_channels_v4`, `activity_panel='active' AND NOT is_topic`. Strata are `ntile(10)` over `weekly_views` (null treated as 0), ties broken by id. Each stratum gets `sample_n/10` channels, ordered by `xxhash64(id, seed)`. The sample table stores `stratum` and `stratum_n` (population per stratum) so the extrapolation needs no re-query.
- **Pass A:** `playlistItems.list`, uploads playlist `UU` + `id[2:]`, `snippet,contentDetails`, 50 per page. Stops at the first item before `window_start` or at `page_cap` pages (censored). Keeps items from `window_start` on, with `after_window` set after `window_end`. Dates come from `videoPublishedAt`; items without a date (private/deleted placeholders) are skipped.
- **Pass B:** `videos.list`, `contentDetails,statistics,snippet`, 50 ids per call, for every recorded video. Duration is parsed to seconds; stats also keep comment count and `liveBroadcastContent`.
- **Quota safety carried over from 08:** calls never raise, and every attempt is logged with an outcome. One global token-bucket limiter. A daily-quota 403 stops the run cleanly, and permanent auth errors fail fast. The first channel is processed serially as a canary. A chunk is marked done in the queue table only if no call in it was deferred, errored or skipped.
- **Hard unit cap:** each HTTP attempt (retries included) reserves one unit from a global counter before it is sent. After the cap every call is `skipped`. The fetch runs driver-side on 16 threads (sufficient at 1k channels), so the cap is exact.
- **Key:** `dbutils.secrets.get("youtube","api-key")`, sent in the `X-Goog-Api-Key` header, never in a URL. Exception text and response bodies are neither printed nor stored, only the status code and Google's `reason`.
- **Outputs** (suffix `v1`; the smoke run used `smoke`): `fig6_dryrun_sample_*`, `_uploads_*`, `_video_stats_*`, `_calls_*` (one row per attempt: pass, outcome, units, http_status, reason, channel, page), plus `_channels_*` (per-channel pages / censored / n_window) and `_queue_*`. Pre-flight refuses existing tables unless `overwrite=true` (drops only `fig6_dryrun_*` tables) or `resume=true`.

## Run log

| Run | Parameters | Result |
|---|---|---|
| 394512642345342 (smoke) | sample_n=20, max_units=200, suffix=smoke | googleapis.com reachable from the cluster (unauthenticated discovery call, 0 quota). Sample of 20 saved. The first `playlistItems` call returned **HTTP 400 `badRequest`**, matched the invalid-key markers, and stopped the run: 1 unit attempted, 19 channels logged as `skipped`. Notebook raised after flushing logs. |

Units spent in total: **1** (of the 5,000 allowed). The 1,000-channel run was not launched.

## Needs from the PI before the dry run can continue

A valid key in the `youtube/api-key` secret (the 2026-10-13 note in 08 says that secret held an expired key then). Whoever renews it should also confirm the key's GCP project quota (plan section 1). The smoke tables `dev_sean.matt.fig6_dryrun_*_smoke` exist; a re-run with suffix `smoke` needs `overwrite=true`.

## Changes the full run will need

- Distributed version: the driver-side pool cannot reach 60k req/min or millions of channels. Move to `mapPartitions` as in 08, with a per-task share of the unit budget (global cap = sum of task budgets) and a shared stop flag per task.
- Keep the chunk queue; size chunks so Pass B batches fill 50 ids.
- Also log the structured `details[].reason` (e.g. `API_KEY_INVALID`) next to `reason`. The 400 was classified as a key rejection because the response text matched a key-invalid marker; Google reports `badRequest` as the top-level reason for that case, so the stored reason alone does not show which marker fired.
