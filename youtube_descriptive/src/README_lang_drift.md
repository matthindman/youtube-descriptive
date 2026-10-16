# Language drift in weekly views (Extended Data Fig. 4f candidate)

Do language shares of weekly YouTube views shift from week to week? This panel tracks them over the weekly panel, mid-June to mid-October 2026, for channels with ≥10k subscribers.

- **Panel a:** each language's share of weekly views, indexed to its June level (= 100), as a centred 3-week mean.
  - Thin lines are the raw weekly values.
  - Purple lines rose by more than 7.5%; teal lines fell by more than 7.5%; grey lines changed less.
- **Panel b:** relative change in share from June (the first four clean weeks) to late September–October (the last four).

## Pipeline

| Step | Code | Output |
|---|---|---|
| 1. Aggregate (SQL warehouse) | `../scripts/sql/lang_drift_weekly_hwm.sql` | one row per week × language |
| 2. Render | `../scripts/render_lang_drift_local.py` (+ `lang_drift_plot.py`) | `../figs/lang_drift/lang_drift_weekly_v1.csv`, `lang_drift_summary_v1.csv`, `lang_drift_v1.{png,pdf}` |

```bash
python scripts/render_lang_drift_local.py --profile accelerator-db --warehouse 86100da4e1fe8713
python scripts/render_lang_drift_local.py --from-csv
```

Only aggregates leave Databricks.

## Definitions

- **Universe:** `dev_sean.matt.fig1_discovery_channels_v4` (4,990,312 channels with ≥10k subscribers on 2026-10-05), with the channel set held fixed across weeks.
- **Language:** `dev_sean.matt.fig2_map_channels_v2.language` (Fig 2 labels, written metadata, ISO 639-3). Each channel's label is held fixed across weeks.
- **Snapshots:** every Monday snapshot of `dev_sean.default.yt_channel_stats_full` from 2026-06-01, deduplicated per channel and date with `max_by(total_view_count, collected_at)`.
  - Weeks are kept only when consecutive snapshots are exactly 7 days apart.
  - Elapsed time between collections is 7.00 days ±0.003 for every channel checked.
- **Weekly views:** each channel's gain above its own **prior high-water mark**, `greatest(views_t − max(views_<t), 0)`, divided by elapsed days and multiplied by 7, then summed by language.
  - This differs from Figs 3–5, which use a five-week delta and exclude channels whose total fell.
  - Why the high-water mark is needed: channel totals sometimes drop and are later restored. A plain weekly delta that skips the falling week counts the restoration as new viewing (see Caveats).
- **Week ending 2026-06-08 is dropped:** it has only one prior snapshot, so no high-water mark.
- **Baselines:**
  - "June" is the first four clean weeks (2026-06-15 to 07-06).
  - "Late Sep–Oct" is the last four (2026-09-21 to 10-12).
  - The stall and catch-up weeks ending 2026-08-17, 08-24 and 08-31 are excluded from both baselines but still plotted.
- **Plotted languages:** the 13 largest by total views, excluding `und`.

## Results (v1, 2026-10-16)

- **Week to week the ranking is stable.**
  - English holds 47.1–50.1% of weekly views every week (coefficient of variation 1.5%).
  - The other large languages vary by about 4–9% of their own share.
- **Hindi and Spanish trade second place.** Hindi was #2 through early July; Spanish has been #2 in most weeks since mid-July.
- **Drift, June → late Sep–Oct (relative change in share):**

  | Language | June share | Late share | Change |
  |---|---|---|---|
  | Japanese | 2.21% | 2.73% | +23.7% |
  | Russian | 2.12% | 2.39% | +12.7% |
  | Portuguese | 3.82% | 4.25% | +11.3% |
  | Korean | 2.59% | 2.87% | +10.7% |
  | Chinese | 1.04% | 1.11% | +6.4% |
  | Bengali | 1.55% | 1.64% | +6.0% |
  | Spanish | 6.31% | 6.51% | +3.2% |
  | Indonesian | 5.77% | 5.67% | −1.8% |
  | Turkish | 2.00% | 1.96% | −2.0% |
  | English | 49.54% | 48.00% | −3.1% |
  | Arabic | 4.58% | 4.22% | −7.9% |
  | Hindi | 6.81% | 6.19% | −9.1% |
  | Vietnamese | 2.04% | 1.75% | −14.1% |

- **Hindi's decline starts in July**, before the 2026-08-24 view-counting change, so that change does not explain it alone. Four months of data cannot separate trend from seasonality.

## Caveats

- **Refresh artefacts in the weekly totals:**
  - the stall week ending 2026-08-24, with its dip and catch-up;
  - another dip and catch-up pair: 0.98T views in the week ending 09-07, then 1.68T in the week ending 09-14.
  - Shares are less affected than totals, but those weeks are shaded and kept out of the baselines.
- **Drop-and-restore events:**
  - In the week ending 10-05, 41,510 English-labelled channels lost a net 420B lifetime views. In the week ending 10-12 they regained 209B.
  - With plain weekly deltas, English appears to jump to 54.8% of the 10-12 week. The high-water-mark rule removes this.
  - Cost of the rule: after a drop, a channel's genuinely new views are not counted until its total passes the old peak.
- **Fixed universe and labels:** the channel set is the ≥10k universe as of October, and the language labels are written metadata held fixed. Channels that crossed 10k during the period are included in the early weeks too.
- **View-counting change on 2026-08-24:** since then, a regular video's view counts at the first frame. Shorts already counted this way. This can shift shares between Shorts-heavy and long-form-heavy languages after that date.

## Draft paper text (staged for the Monday 2026-10-19 status check; not yet in the paper)

**Extended Data Fig. 4, new panel f (legend text):**

> **f**, Share of weekly views by language over the weekly panel (week ending 2026-06-15 to 2026-10-12), indexed to each language's June level (= 100); thick lines are centred three-week means, thin lines weekly values; purple and teal mark languages whose share rose or fell by more than 7.5% between June and late September–October (right), grey the rest. Weekly views are each channel's gain above its prior peak lifetime view count, scaled to seven days, for channels with ≥10,000 subscribers on 2026-10-05; language is the channel's written-metadata language. Shaded weeks contain data-refresh artefacts and are excluded from the baselines; the dashed line marks YouTube's 2026-08-24 view-counting change.

**Talking points (for Results 6):**

- Language shares are stable week to week (English 47–50% every week). Single-week figures such as Fig 2 are therefore representative.
- Over four months some shares drift: Japanese +24%, Russian +13%, Portuguese +11%, Korean +11%; Vietnamese −14%, Hindi −9%, Arabic −8% (relative change in share).
- Hindi and Spanish swap second place in mid-July.
- Hindi's decline predates the 2026-08-24 view-counting change. Four months cannot separate trend from seasonality.
- Method note for Methods: weekly gains are measured above each channel's prior high-water mark, so restored views are not double-counted.

## Run log

| Date | Run | Outcome |
|---|---|---|
| 2026-10-16 | `lang_drift_weekly_hwm.sql` on warehouse 86100da4e1fe8713 | 11,696 rows (19 weeks × 616 languages) → `lang_drift_weekly_v1.csv` |
| 2026-10-16 | `render_lang_drift_local.py --from-csv` | `lang_drift_v1.{png,pdf}`, `lang_drift_summary_v1.csv` |
