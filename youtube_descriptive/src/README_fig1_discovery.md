# Figure 1 — discovery saturation (all vs active channels)

Figure 1 of *Attention on YouTube* shows that discovery of channels with ≥10k subscribers
saturated during the 13-step public-subscription snowball (ledger claim C2). Requirements and
decisions are in the Claude doc "Figure 1 Requirements — Discovery Saturation (All vs Active
Channels)".

## Pipeline

| Step | Code | Output |
|---|---|---|
| 1. Refresh recent uploads | `08_fetch_channel_videos_refresh_databricks.py` | appends to `dev_sean.matt.yt_channel_videos`, `yt_channel_descriptions`, `yt_cd_not_found` |
| 2. Channel creation dates | `09_fetch_channel_created_at_databricks.py` | `dev_sean.matt.yt_channel_created_at` |
| 3. Classify + aggregate + plot | `07_fig1_discovery_saturation_databricks.py` | `dev_sean.matt.fig1_discovery_{channels,exhaustion,totals,trending,activity,cohorts,refresh_ids}_<version>` |
| 4. Render locally | `../scripts/render_fig1_local.py` | `../figs/fig1/*.png`, `*.pdf`, and CSV source data |

`fig1_discovery_plot.py` draws every variant. The Databricks notebook and the local renderer both
import it. All notebooks are also in the workspace under
`/Users/sean.norton@researchaccelerator.org/youtube-descriptive/youtube_descriptive/src/`.

Both fetchers take an optional `api_key` job parameter; otherwise they read the `youtube/api-key`
secret. **On 2026-10-13 that secret held an expired key.** The working key was passed as a job
parameter and is never committed. Renew the secret before relying on it.

## Definitions

- **Universe:** channels with ≥10k subscribers in the 2026-10-05 snapshot of
  `dev_sean.default.yt_channel_stats_full` (4,990,312 channels).
- **Weekly views:** the 2026-09-28 → 2026-10-05 lifetime-view delta, normalised by elapsed days and
  floored at 0. This pair avoids the 2026-08-24 stats stall.
- **Discovery step:**
  - Step 0 is the 262,761-channel seed list (`new_yt_top_list`).
  - Steps 1–13 are `pub_subs_batches` batches 1–13.
  - Channels not reached by step 13 fall into `later_sources` (restart, full pass, 1k/5k threshold
    crawls, extension batches) or `trending`.
  - Batch numbers collide across sources, so joins key on (`batch_source`, `batch_number`).
- **Activity:**
  - Source: each channel's latest `yt_channel_videos` collection (last 10 uploads).
  - `active_panel`: newest upload on or after 2025-10-05.
  - `active_crawl`: an upload between 2025-03-04 and 2026-03-04. A channel whose 10 stored uploads
    all post-date 2026-03-04 also counts as active.
  - Reached by the fetcher but no videos returned: `inactive`. Reported missing by the API:
    `not_found`.
- **Topic channels:** any channel whose name ends in " - Topic" (case-insensitive). They are
  excluded from both active variants by name, because they can show auto-generated uploads.
- **Incumbent / entrant:**
  - A channel is an `entrant` if it was created after 2026-03-04 (`snippet.publishedAt`), or had
    under 10k subscribers at its earliest discovery record. Seeds are always incumbents.
  - `entry_reason` records which rule fired.
  - There is no March snapshot, so subscribers-at-first-record stands in for size at crawl time.
- **Panels:**
  - Panel a plots the cumulative share of each bin discovered by step k, for the chosen cohort.
    Solid lines are channels; dashed lines are weekly views.
  - Panels b–c plot channels first found by trending, per week, for every cohort.

## Figure variants (`RENDER_SPECS`)

| Variant | Panel a cohort | Bins | Use |
|---|---|---|---|
| `active_panel` | incumbent | subscribers | main Fig 1 |
| `all` | incumbent | subscribers | ED Fig 1 |
| `active_panel` | incumbent | weekly views | ED Fig 1 |
| `active_crawl` | incumbent | subscribers | ED Fig 1 sensitivity |
| `active_panel` | all | subscribers | comparison without the incumbent filter |

## Results (v4, 2026-10-13)

Share of each bin discovered by snowball step 13:

| Bin | Active, all channels | Active, incumbents | All channels, incumbents |
|---|---|---|---|
| 10k–100k subscribers | 81.8% | 85.5% | 86.5% |
| 100k–1M | 94.6% | 96.1% | 96.6% |
| 1M–10M | 96.9% | 97.3% | 97.4% |
| 10M+ | 100% | 100% | 100% |
| Top 1% by weekly views | 76.9% | 86.5% | 86.6% |

**Active channels**
- 3,469,842 active channels hold 97.6% of all weekly views (98.4% of non-Topic views); 3,107,858 of them are incumbents.

**Entrants**
- 381,616 entrants hold 14.0% of weekly views:
  - 348,796 were under 10k subscribers when first recorded;
  - 32,820 were created after 2026-03-04, holding 4.7% of weekly views.

**Top 1% by weekly views**
- Of the 41,961 incumbents in the top 1%, the snowball missed 5,639. The threshold crawls found
  3,688 of them and trending found 1,951.
- These look like genuine reach limits of a snowball seeded only from channels with ≥10k
  subscribers. Being found by the 1k/5k crawls means they are linked mainly from smaller channels.

**Activity and creation-date coverage**
- 1,486,703 channels are inactive, holding 1.4% of weekly views. 3,674 are not found.
- 30,093 Topic channels are excluded; after the refresh, 20,971 of them would have passed the activity rule.
- Creation dates cover all 4,990,312 channels: 4,984,800 have a date and 5,512 were not found.
  Two dates are the 1970-01-01 placeholder.

## Run log

| Date | Run | Databricks run_id | Outcome |
|---|---|---|---|
| 2026-10-13 | Fig 1 v1 | 866275067493901 | Tables built; plot step failed (DECIMAL → object dtype), rendered locally |
| 2026-10-13 | Fig 1 v2 | 292031529171075 | Fetched-but-empty channels reclassified as inactive; 1,888,518 refresh IDs |
| 2026-10-13 | Upload refresh | 1008878050030280 | No data: the `youtube/api-key` secret is expired; every call errored and the run still reported success |
| 2026-10-13 | Upload refresh | 518074753922217 | 38/38 chunks; 1,887,059 channels reached, 1,749,127 with videos, 1,459 not found |
| 2026-10-13 | Fig 1 v3 | 782899868506699 | Failed: missing comma between CTEs |
| 2026-10-13 | Fig 1 v3 | 411019945153851 | Success; incumbents defined by subscribers at discovery only |
| 2026-10-13 | Creation dates | 735569832398830 | 4,984,800 ok, 5,512 not found (~100k quota units) |
| 2026-10-13 | Fig 1 v4 | 803091634682123 | Success; entrant rule adds created-after-crawl |

## Rerunning

1. Optional: refresh uploads. Run `08_…` with `source_table` = the latest
   `fig1_discovery_refresh_ids_<v>`, a new `run_id` (YYYY-MM-DD) and `num_recent_videos=10`.
2. Optional: run `09_…` to fetch creation dates for channels not yet in `yt_channel_created_at`.
   It is resumable.
3. Run `07_…` with a new `version` (or `overwrite=true`).
4. Render locally:

   ```bash
   python scripts/render_fig1_local.py --profile accelerator-db --warehouse <id> --version <v>
   ```

## Known limits

- Panel b's log axis shows only one labelled tick (10³).
- Weekly-view bins use percent ranks. Ties at zero make the bottom bin slightly larger than 25%.
- Some discovery dates are lower bounds: unlabeled batch 21 → 2026-04-30, and `pub_subs_full`
  batch 2 → 2026-03-11. This affects only the ordering among later sources.
