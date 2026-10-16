# Figure 5 — channel founding year across traffic blocks

Figure 5 of *Attention on YouTube* (§2.5) relates channel age to attention rank. Purely descriptive.

- Panel a: founding-year distribution within each of 20 traffic blocks (equal weekly-view mass; block 1 = channels holding the top 5% of weekly views).
- Panel b: median founding year (with interquartile band) and the share founded in or before 2010 / 2015, by block.

**Headline (v1): the data do not show older channels concentrated at the top.** Block 1's median founding year (2021.3) is later than block 20's (2019.8) and than the all-channel median (2020.1); the median is non-monotone (see Results). The planned title "Older channels dominate the highest-attention ranks" is not supported by these aggregates (see Caveats and open decision D2).

## Pipeline

| Step | Code | Output |
|---|---|---|
| 1. Aggregate | `15_fig5_age_databricks.py` | `dev_sean.matt.fig5_age_{channels,status,blocks,year_dist,reconcile}_<version>` |
| 2. Render locally | `../scripts/render_fig5_local.py` | `../figs/fig5/*.png`, `*.pdf`, and CSV source data |

- `fig5_age_plot.py` draws the variants (`ab` main, `a`, `b`).
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

- The median is not monotone: it rises from 2021.3 (block 1) to a peak of 2022.9 (block 11), then falls to 2019.8 (block 20). The top blocks are younger than the bottom block, and the middle blocks are youngest.
- Block 1's median is 1.5 years *later* than block 20's. Block 1 has the highest share of very old channels (founded ≤2010: 10.0%, about 23 of 229 channels, versus 3.6–6.2% elsewhere), but its share founded ≤2015 (20.1%) is below block 20's (26.0%).
- Block 1 is small (229 channels); its shares have wide sampling uncertainty (± ~2 pp on a share of 10%).
- By vintage (all measurable dated channels): channels founded in 2023 or later are 24.5% of channels but 42.4% of weekly views; channels founded in 2010 or earlier are 5.9% of channels and 5.0% of views; in 2015 or earlier 25.1% and 19.0%. 2025 alone: 7.6% of channels, 15.8% of views.

## Caveats

- **Selection on the ≥10k universe.** Channels enter the universe by having reached 10k subscribers by 2026-10-05, so a young channel in it is a fast grower and a median of 2020 is not representative of all channels. Older channels have had more time to cross 10k; the age distribution is conditional on that.
- **Survivorship.** The founding table only holds channels that still exist (5,512 `not_found`, 0.11%, are missing).
- **Creation date is not first upload or last activity** and can predate a channel's real activity or reflect re-purposed channels.
- **Partial 2026.** The 2026 column covers only 2026-01-01 to ~2026-10-01 (latest date 2026-10-01).
- **Weekly views are one 5-week window**; a viral burst can move a young channel into the top block. `total_fell` channels (209,749; 4.2%) are excluded as in Figs 3–4, so a channel whose total dropped can be absent from every block.
- **Block widths:** because blocks hold equal view mass, block 20 contains 86% of channels and includes 214,330 zero-view channels.
- **Founding date times are UTC calendar dates**; decimal year ignores time of day.

## Run log

| Date | Run | Databricks run_id | Outcome |
|---|---|---|---|
| 2026-10-16 | Fig 5 v1 | 1052195798034156 (task run 1119759341281876) | Success on cluster `0818-185002-anfay17g`, first attempt |
| 2026-10-16 | v1 render | local | `render_fig5_local.py --version v1`; CSVs + PNG/PDF in `figs/fig5` |
