# Figure 6 — video-level collection plan (not launched)

Status: **plan only, 2026-10-16.** Nothing has been run.

Figure 6 ("Production and attention diverge by format") needs per-video upload lists, durations and views. A data audit on 2026-10-16 found no readable table that covers the ≥10k universe in the 2026-08-31 → 2026-10-05 window (see the audit notes at the end).

## 1. Quota: what we know and what still needs confirming

| Item | Value | Evidence |
|---|---|---|
| Daily quota (project behind the key) | **10,000,000 units/day** (stated) | Every fetcher states it: `08_…`, `09_…`, `11_…`, and `all_update.py`, which also records an earlier run that "blew the 10M daily budget". |
| Lower bound actually achieved | ≥ 2.43M units in one day | The weekly `all_update` job runs `channels.list` on ~121.4M channels at 50 IDs/call every Monday 21:00 UTC, about 1.1 h. Its last 4 runs succeeded. |
| Per-minute cap | 180,000 queries/min, project-wide | `all_update.py` notes. `all_update` takes 90k/min on Mondays. |
| Quota reset | midnight Pacific (07:00 UTC in PDT, 08:00 UTC in PST) | YouTube Data API standard. |

**Not verified:** the actual quota page. There is no Google Cloud CLI or console access from this machine, so the 10M figure comes from the code, not from Google Cloud.

**Before launch:** someone with access to the key's GCP project checks *APIs & Services → YouTube Data API v3 → Quotas* for "Queries per day", and today's usage.

**Other consumers of the same quota:**
- `all_update` uses about 2.43M units every Monday (job 500542207936927, 17:00 ET).
- `[prod_tads/youtube_random] YouTube Unified Ingestion` runs Mondays 06:00 UTC. Whether it uses the same key or project is unknown. Check before scheduling.
- `view_policy_change` is finished; its schedule window was 2026-08-22..25.

**Before launch:** confirm that the key in the `youtube/api-key` Databricks secret is valid. The fetch notebooks read it from there; no key goes in code or job parameters.

## 2. Who to collect

Universe: `dev_sean.matt.fig1_discovery_channels_v4` (4,990,312 channels with ≥10k subscribers).

| Group | Channels | Collect? | Why |
|---|---|---|---|
| active, non-Topic | 3,469,842 | **yes** | The Fig 6 population. Matches Fig 1's active panel. |
| active, Topic (auto-generated) | 20,971 | no | Auto-generated music channels; no production decisions. |
| inactive (no upload in the year to 2025-10-05) | 1,495,824 | **no, count as 0 uploads** | Of the 1.36M inactive channels re-crawled on 2026-10-13, 0 had uploaded since 2026-08-31. |
| not_found | 3,675 | no | |

## 3. Quota estimate

Estimates come from the 389,050 active, non-Topic channels whose last-10-uploads snapshot (`dev_sean.matt.yt_channel_videos`) was refreshed on 2026-10-13. That is the only fresh subset; the other active channels were last refreshed 2026-06-17.

In that subset:
- 75.3% uploaded since 2026-08-31.
- 44.2% had all 10 stored uploads after 2026-08-31, i.e. they hit the 10-upload cap.
- Extrapolating the capped channels' upload rate, capped at 1,000 videos per channel, gives:
  - **1.69 `playlistItems` pages per channel** (50 items per page, max 20 pages);
  - **46 videos per channel** since 2026-08-31, about 6.6 weeks.

Representativeness is unknown: why those 389k were refreshed first is not documented. So every row below has a range.

| Pass | Calls | Units | Notes |
|---|---|---|---|
| A. Upload lists: `playlistItems.list` on each channel's uploads playlist (`UU` + channel id suffix; no `channels.list` needed) | 3.47M channels × 1.4–2.0 pages | **4.9M–6.9M** (central 5.9M) | Newest first. Stop paging at the first item published before 2026-08-31. Hard cap 20 pages (1,000 items); flag channels that hit it as censored. |
| B. Video details: `videos.list`, `part=contentDetails,statistics,snippet` (cost is per call, not per part) | 25M–120M window videos ÷ 50 | **0.5M–2.4M** | Duration (Shorts proxy), views, `snippet.defaultAudioLanguage` / `defaultLanguage` and titles. The language fields also help the India Hindi/English question at no extra cost. The video count is uncertain because it is extrapolated from bursty last-10 spans. |
| C. Second views snapshot: `videos.list` on the same IDs about 14 days later | as B | **0.5M–2.4M** | Gives each window video a two-point growth measure at known ages. |
| D. Calibration cohort, discovery: `playlistItems.list` page 1, daily for 7 days, on a stratified sample of 100k active channels (5k per traffic block) | 100k × ~1.7 × 7 | **≈1.2M over 7 days** (~0.17M/day) | Captures fresh uploads within 24 h of publication. |
| E. Calibration cohort, polling: `videos.list` on cohort videos at ages 1, 2, 3, 5, 7, 10, 14, 21, 28, 42 days | ~0.3–0.7M videos ÷ 50 × ~10 | **≈0.06M–0.14M total** | Cumulative-views-by-age curves by format. These give the lookback calibration (ED Fig. 6). |
| **Total** | | **≈7.2M–13M over ~7 weeks** | **Peak single day ≈6–7M** (pass A). |

## 4. Schedule (assumes 10M/day is confirmed)

| Day | Work | Units that day |
|---|---|---|
| Tue D0 | Pass A, split as 50% today | ≈3.0–3.5M |
| Wed D1 | Pass A, remaining 50% | ≈3.0–3.5M |
| Thu D2 | Pass B; start cohort discovery (D, day 1 of 7) | ≈0.7–2.6M |
| D2–D8 | Cohort discovery D (daily) + polling E | ≈0.2M/day |
| D16 | Pass C (second snapshot) | ≈0.5–2.4M |
| D2–D44 | Cohort polling E (staggered ages) | <0.02M/day |

- Never schedule passes A, B or C on a Monday, when `all_update` takes about 2.4M units and 90k/min.
- Rate: give these jobs ≤60,000 req/min, the spare slice of the 180k/min project cap. Pass A then takes about 100 min per day-half.
- If the confirmed daily quota is lower than 10M, scale pass A across more days. Pass A is resumable by chunk, see §5.

## 5. Implementation

- **New notebook `17_fetch_fig6_window_videos_databricks.py`**, adapted from `08_fetch_channel_videos_refresh_databricks.py`. Keep 08's quota-safety design:
  - workers never raise on rate or quota errors;
  - chunk-level resumability through a queue table;
  - clean stop on `quotaExceeded` / `dailyLimitExceeded`.
- **Changes from 08:**
  - page until `published_at < window_start`, not a fixed N;
  - 20-page cap with a `censored` flag;
  - `videos.list` parts `contentDetails,statistics,snippet`.
- **Outputs:** append-only tables in `dev_sean.matt`, partitioned by `collected_date`:
  - `fig6_window_uploads`: channel_id, video_id, published_at, page, censored;
  - `fig6_window_video_stats`: video_id, duration_s, view_count, like_count, default_audio_language, default_language, title, collected_at.
- **New notebook `18_fetch_fig6_calibration_cohort_databricks.py`**:
  - draws the stratified 100k-channel sample once (seeded) and saves it;
  - runs daily discovery and age-scheduled polling;
  - outputs `fig6_cohort_channels`, `fig6_cohort_videos`, `fig6_cohort_polls`.
- **Then the figure notebook `16_fig6_format_databricks.py`** (Fig 3/4 pattern):
  - Shorts rule: duration ≤ 180 s, documented as a proxy;
  - lookback window chosen from the cohort curves;
  - panels a–c as specified in the paper outline.
- **Dry run first:** 1,000 channels through passes A+B, to measure real pages per channel and videos per channel before the full pass. That is about 3k units.

## 6. Caveats to carry into the paper

- Views come from snapshots at collection time, not window-end values. Window videos are 1–7 weeks old at pass B, so the cohort calibration is needed to put them on a common age basis.
- All views are post-2026-08-24 counting regime: a regular-video view counts at the first frame.
- Shorts views have been counted at first frame since 2025-03-31.
- Duration ≤ 180 s is a proxy for Shorts: not every short video is a vertical Short, and the reverse.
- Upload lists miss videos deleted or privated before collection.
- Censored channels (more than 1,000 uploads in the window) undercount production for the heaviest uploaders. Report how many there are.

## Audit notes (2026-10-16)

- `dev_sean.matt.yt_channel_videos`:
  - last 10 uploads per channel, with no views, duration or Shorts flag;
  - only 1.75M universe channels refreshed on 2026-10-13; 3.1M last refreshed 2026-06-17.
- Per-video metrics tables (`prod_tads.youtube_too|youtube|youtube_random.yt_sl_videos_metrics`, `qa_catalog.*`, `dev_tads.*`, `dev_kai.*`): all end by 2026-05 except `youtube_random`, which is a ~1k-videos/day random draw ending 2026-09-10 and is too thin.
- Unreadable without extra permissions, not checked: `dev_alex_v.youtube.yt_videos_metrics_ts`, `dev_lilly.youtube.yt_videos_metrics(_ts)`, `qa_catalog.youtube.yt_videos_metrics_ts`.
- `dev_sean.common_data.yt_vsw_ranked` is a channel-level ranking (weights pipeline), not video-level.
