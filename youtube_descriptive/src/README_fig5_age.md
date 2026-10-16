# Figure 5 — recently founded channels capture a disproportionate share of attention

Figure 5 of *Attention on YouTube* (§2.5) relates channel founding year to attention. Purely descriptive.

- Panel a: share of channels and share of weekly views by founding year (2005–2026).
- Panel b: founding-year distribution within each of 20 traffic blocks (equal weekly-view mass; block 1 = channels holding the top 5% of weekly views), with the block median.
- Panel c: median founding year by block, channel-weighted (with interquartile band) and weighted by views.

**Headline (v1): the data do not support "older channels dominate the highest-attention ranks"; the clearest finding is a vintage skew.** Channels founded in 2023 or later are 24.5% of channels but 42.4% of weekly views. Across blocks the median founding year is an inverted U (block 1: 2021.3, peak 2022.9 at block 11, block 20: 2019.8).

Figure: `../figs/fig5/fig5_abc_v1.png` / `.pdf` (7.2 in wide).

### Caption (draft)

\textbf{Channel founding year and weekly views.} \textbf{a}, Share of channels and share of weekly views by founding year. \textbf{b}, Distribution of founding years within each of 20 traffic blocks (rows; block 1 holds the channels with the top 5\% of weekly views, block 20 the bottom 5\%); shading is the share of the block's channels founded in that year and the line marks the block median. \textbf{c}, Median founding year by traffic block for channels (band: interquartile range) and weighted by weekly views; the dotted line is the median across all channels. \emph{Notes:} Universe: channels with $\geq$10,000 subscribers on 2026-10-05, excluding channels whose lifetime views fell or that were absent on 2026-08-31 (4{,}770{,}835 channels). Weekly views are the 2026-08-31 to 2026-10-05 change in lifetime views, scaled to seven days. Traffic blocks rank channels by weekly views and cut the ranking into 20 blocks of equal total views. Founding year is the channel creation date reported by the YouTube Data API (99.9\% of channels and views have one; earlier-than-launch dates are treated as missing). 2026 is a partial year (hatched or lighter). Channels in this universe reached 10,000 subscribers by 2026-10-05, so founding years are conditional on that.

## Pipeline

| Step | Code | Output |
|---|---|---|
| 1. Aggregate | `15_fig5_age_databricks.py` | `dev_sean.matt.fig5_age_{channels,status,blocks,year_dist,reconcile}_<version>` |
| 2. Render locally | `../scripts/render_fig5_local.py` | `../figs/fig5/*.png`, `*.pdf`, and CSV source data |

- `fig5_age_plot.py` draws the three-panel figure. The view-weighted medians in panel c are computed from `year_dist` (per-block `wv_sum`, linear interpolation inside the calendar year), so they are approximate to a few hundredths of a year.
- Only aggregates leave Databricks: `channels` (4.77M rows) stays there.

```bash
python scripts/render_fig5_local.py --profile accelerator-db --warehouse 86100da4e1fe8713 --version v1
```

Add `--from-csv` to re-render from the saved CSVs (no Databricks needed).

## Definitions

- **Universe:** the ≥10k-subscriber channels, `dev_sean.matt.fig1_discovery_channels_v4` (4,990,312 distinct channels), as in Figs 1 and 3.
- **Weekly views:** as in Figs 3–4. 2026-08-31 → 2026-10-05 lifetime-view delta from `dev_sean.default.yt_channel_stats_full` (latest collection per channel and date), divided by elapsed days × 7.
  - `positive` 4,556,505; `zero` 214,330 (kept, they fall in block 20); together the 4,770,835 *measurable* channels (equal to Fig 3's universe and to Fig 4's ≥10k measurable slice).
  - Excluded: `total_fell` 209,749 (lifetime total fell) and `missing_start` 9,728. No channel is missing from the end snapshot.
- **Traffic blocks:** measurable channels ranked by weekly views (ties by `canonical_id`), cut into 20 blocks of equal view mass with exact running sums (the Fig 2 rule: block = floor(views ranked above / total × 20) + 1). Block 1 has 229 channels, block 20 has 4,099,993. Blocks are assigned before, and independently of, the founding date.
- **Founding date:** `snippet.publishedAt` from `dev_sean.matt.yt_channel_created_at` (notebook 09), latest `status = 'ok'` row per channel. Dates before YouTube's launch (2005-02-14; includes the API's 1970-01-01 placeholder) are nulled, the Fig 1 rule (2 channels).
- **Decimal founding year:** 1970 + days since 1970-01-01 / 365.2425. Medians and quartiles are exact (`percentile`). "Founded in or before Y" uses the calendar year.

## Results (v1, 2026-10-16)

**Coverage.** 4,984,800 of 4,990,312 universe channels (99.89%) have an `ok` founding date; the other 5,512 are `not_found` (deleted/terminated). 4,766,219 of the 4,770,835 measurable channels (99.90%) have a usable date (99.85% of weekly views). Per block, 99.75%–100% of channels and of views are dated, so no block is distorted by missing dates. No new data collection was needed.

**Median founding year by block** (decimal year; full table in `figs/fig5/fig5_blocks_v1.csv`):

| Block | Channels | Median | Q25–Q75 | ≤2010 | ≤2015 |
|---|---|---|---|---|---|
| 1 (top 5% of views) | 229 | 2021.3 | 2016.6–2023.4 | 10.0% | 20.1% |
| 2 | 579 | 2021.6 | 2016.6–2024.3 | 5.7% | 21.1% |
| 5 | 2,315 | 2022.1 | 2017.2–2024.9 | 5.1% | 19.9% |
| 10 | 9,338 | 2022.7 | 2018.2–2025.3 | 4.0% | 16.6% |
| 15 | 36,596 | 2022.4 | 2018.0–2025.0 | 3.6% | 17.0% |
| 20 (bottom 5%) | 4,099,993 | 2019.8 | 2015.8–2022.6 | 6.2% | 26.0% |
| All measurable | 4,770,835 | 2020.1 | 2016.0–2022.9 | 5.9% | 25.0% |

- The median is an inverted U: it rises from 2021.3 (block 1) to a peak of 2022.9 (block 11), then falls to 2019.8 (block 20). Block 1 is older than blocks 2–19 by median and younger only than block 20. Its 2023-or-later share of views (24.5%) is the lowest of blocks 1–19 (block 20: 26.7%; blocks 2–19: 35%–49%).
- Weighted by views, medians are earlier at the ends and the same elsewhere: block 1 2021.0 (channels: 2021.3), block 20 2020.4 (2019.8), block 11 2022.9 (2022.9). Overall, the view-weighted median founding year is 2022.0 against 2020.1 channel-weighted.
- Block 1 is small (229 channels). Bootstrap 95% CI for its median: about 2020.5–2021.7 (from the review of this figure; not in the exported tables). Its ≤2010 share (10.0%, about 23 channels) has SE ≈ 2 pp, 95% CI ≈ ±4 pp.
- Block 1 has the highest share of very old channels (founded ≤2010: 10.0% versus 3.6–6.2% in other blocks), but its share founded ≤2015 (20.1%) is below block 20's (26.0%).
- By vintage (all measurable dated channels): channels founded in 2023 or later are 24.5% of channels but 42.4% of weekly views; channels founded in 2010 or earlier are 5.9% of channels and 5.0% of views; in 2015 or earlier 25.1% and 19.0%. 2025 alone: 7.6% of channels, 15.8% of views.

## Caveats

- **Selection on the ≥10k universe.** Channels enter the universe by having reached 10k subscribers by 2026-10-05, so a young channel in it is a fast grower and a median of 2020 is not representative of all channels. Older channels have had more time to cross 10k; the age distribution is conditional on that. Block 20 (86% of channels, near-zero views each) is mostly older channels that crossed 10k subscribers long ago and now draw little attention, which is why its median is earlier than blocks 2–19.
- **Survivorship.** The founding table only holds channels that still exist (5,512 `not_found`, 0.11%, are missing).
- **Creation date is not first upload or last activity** and can predate a channel's real activity or reflect re-purposed channels.
- **Partial 2026.** Measurable channels must exist on 2026-08-31 (the start snapshot), so the latest founding month in the figure is August 2026. The 2026 column (hatched in b, lighter in a) covers about eight months.
- **Weekly views are one 5-week window**; a viral burst can move a young channel into the top block. `total_fell` channels (209,749; 4.2%) are excluded as in Figs 3–4, so a channel whose total dropped can be absent from every block.
- **Block widths:** because blocks hold equal view mass, block 20 contains 86% of channels and includes 214,330 zero-view channels.
- **Founding date times are UTC calendar dates**; decimal year ignores time of day.

## Run log

| Date | Run | Databricks run_id | Outcome |
|---|---|---|---|
| 2026-10-16 | Fig 5 v1 | 1052195798034156 (task run 1119759341281876) | Success on cluster `0818-185002-anfay17g`, first attempt |
| 2026-10-16 | v1 render | local | `render_fig5_local.py --version v1`; CSVs + PNG/PDF in `figs/fig5` |
