# Figure 3 — subscribers are an imprecise proxy for attention

Figure 3 of *Attention on YouTube* shows that subscriber counts predict weekly attention only
loosely. Panel a plots weekly views against subscribers (log-log). Panel b plots weekly views per
subscriber against subscribers, with a flat overall-mean reference line. Panel c (main figure)
plots median views per subscriber by YouTube topic. The variance-decomposition tables (for
ED Fig. 2 and prose) measure how much of the spread category and language explain.

## Pipeline

| Step | Code | Output |
|---|---|---|
| 1. Aggregate + preview | `13_fig3_subscriber_proxy_databricks.py` | `dev_sean.matt.fig3_subproxy_{channels,snapshots,negatives,window_check,bins,hist2d,fit,decomp}_<version>` |
| 2. Render locally | `../scripts/render_fig3_local.py` | `../figs/fig3/*.png`, `*.pdf`, and CSV source data |

`fig3_proxy_plot.py` draws every variant; the notebook and the local renderer both import it
(it reuses the topic colours from `fig2_map_plot.py`). Only aggregates leave Databricks. The
channel-level frame stays in `fig3_subproxy_channels_<version>`. `hist2d` drops cells with fewer
than 10 channels.

```bash
python scripts/render_fig3_local.py --profile accelerator-db --warehouse 86100da4e1fe8713 --version v2
```

Add `--from-csv` to re-render from the saved CSVs without querying.

## Step 1: which weekly-views window?

`fig3_subproxy_snapshots_v2` audits every Monday snapshot of
`dev_sean.default.yt_channel_stats_full` from 2026-06-01 to 2026-10-05, restricted to the Fig 1
universe.

- **Snapshots:** 19 clean weekly snapshots (2026-06-01 … 2026-10-05).
  - Every pass ran 21:13–22:10 UTC (17:13–18:10 ET), and consecutive passes are 7.00 ± 0.01 days apart.
  - Coverage grows from 4,964,334 to 4,990,312 universe channels.
  - Each week, 92–94% of channels show a changed lifetime total and about 2% show a lower one.
- **Stall weeks.** The 2026-08-24 pass ran inside the known 14 h stats stall:
  - The week ending 08-24 records 0.92T views; the catch-up week ending 08-31 records 1.13T.
  - The weeks ending 08-10 (0.73T) and 08-17 (1.21T) show a similar dip-and-catch-up.
  - Other weeks sit between 0.86T and 1.14T.
- **Window chosen: 2026-08-31 → 2026-10-05**, five weeks.
  - Neither endpoint touches a stall or catch-up pass.
  - Weekly views = (total on 10-05 − total on 08-31) / elapsed days × 7.
- **Is a multi-week window better than the single 09-28 → 10-05 week?** Yes, slightly
  (`window_check`):
  - Per channel, the single-week value differs from the 5-week mean by a median of 0.09 dex
    (~±23%) for 10k–1M-subscriber channels. That error shrinks to 0.06–0.07 dex above 30M.
  - The single week's 10th–90th percentile log ratio is about −0.31 to +0.22 dex for small
    channels: refresh timing makes a single week a noisy measure.
  - The log correlation between the two measures is 0.97–0.98.
  - The spread of log views per subscriber falls only from 1.33–1.44 dex (single week) to
    1.31–1.40 dex (window). So the dispersion in Fig 3 is real cross-channel variation, not
    step-function noise.
  - The longer window is used because it is the better measure and costs nothing.

### Channels whose lifetime total fell

209,749 channels (4.2%; 183,385 of the active variant) had a *lower* lifetime total on 10-05 than
on 08-31, summing to −1.93T views.

- `negatives` shows these are one-off drops: most have a single negative week, and the median
  drop is 12× the channel's gains over the window.
- That pattern fits video deletions or privatisations, or count corrections. It is not
  oscillation: the 126k channels with negative weeks but a rising window have drops of only
  ~20% of their gains.
- For these channels attention is not measurable from channel totals. v2 sets their weekly views
  to null and excludes them (`fit.n_total_fell`); true zero deltas stay at 0.
- v1 floored them at 0, as Fig 1 does. That added 210k spurious zero-view channels and fattened
  the lower tail at large subscriber counts.
- **Note for Figs 1–2:** both floor weekly views at 0. They therefore count these channels as
  zero, and their platform total (1.53T for the single week) exceeds the week's net change (0.91T).

## Definitions

- **Universe:** `dev_sean.matt.fig1_discovery_channels_v4`: ≥10k subscribers on 2026-10-05
  (4,990,312 channels).
  - Variant `all` keeps every channel. Variant `active` keeps Fig 1's `active_panel`, non-Topic channels.
  - 9,728 channels missing from the 08-31 snapshot and the 209,749 with a falling total are excluded.
  - That leaves 4,770,835 channels (`all`) and 3,277,498 (`active`).
- **Subscribers:** `subscriber_count` on 2026-10-05. The API rounds it to 3 significant figures.
- **Views per subscriber (VPS):** weekly views / subscribers.
- **Overall mean (panel b line):** pooled Σ weekly views / Σ subscribers (2.47 for `all`, 2.87 for
  `active`). If subscribers were a proportional proxy, every channel would sit on this line.
- **Subscriber bins:** 0.05-dex bins of log10(subscribers), merged from the top down until each
  holds ≥500 channels (≥300 for per-group curves). This gives 65 bins for `all`.
  - Panels plot the exact binned percentiles at each bin's geometric-mean subscriber count,
    smoothed by a centred 5-bin rolling mean in log space (`SMOOTH_BINS`).
  - The CSV holds the unsmoothed values.
- **Panel a:**
  - Density layer: count of channels per 0.05 × 0.1-dex cell.
  - Lines: median, middle 50% band, and 5th–95th percentile lines (the prediction envelope).
  - Fit: log-log OLS on channels with positive weekly views.
  - Values below 10 views are clipped to the axis.
- **Language and category:** from `dev_sean.matt.fig2_map_channels_v2` (Fig 2's language
  cascade and YouTube parent topics). Panel c omits `Unclassified`.
- **Subscriber growth (ED variant):** (subscribers on 10-05 − subscribers on 08-31) / subscribers
  on 08-31, grouped as flat or falling / 0–2% / 2–10% / >10%. Because of 3-significant-figure
  rounding, 62% of channels read as flat, and the flat curve shows a sawtooth at rounding
  boundaries (100k, 1M).
- **Variance decomposition (`decomp`):**
  - Outcomes: log10 weekly views and log10 VPS, on channels with positive weekly views.
  - R² comes from cell-means models: 1 − within-cell SS / total SS.
  - Cells:
    - 0.1-dex subscriber bins (capped at 10^7.5);
    - YouTube topic (8);
    - language (78 cells: languages with ≥1,000 channels, plus `other` and `und`);
    - their crossings.
  - `within_p90_p10`: the channel-weighted mean of each cell's 10th–90th percentile range
    (cells with ≥30 channels), next to the same range for all channels.

## Figure variants (`RENDER_SPECS`)

| File | Variant | Panel c | Use |
|---|---|---|---|
| `fig3_all_category_v2` | all | median VPS by topic | **main Fig 3** |
| `fig3_all_ab_v2` | all | none | two-panel fallback if c crowds the page |
| `fig3_active_category_v2` | active | by topic | ED sensitivity (active, non-Topic channels) |
| `fig3_all_growth_v2` | all | by subscriber growth | ED |

Panel c earns its place: within every topic, VPS still rises with subscribers. Topic shifts the
curve only by about 2–4×, while the 10th–90th band spans about 3.5 orders of magnitude.

## Results (v2, 2026-10-15)

**Subscribers predict weekly views only loosely** (`fit`):

| | all (4.77M) | active (3.28M) |
|---|---|---|
| Log-log OLS slope | 1.17 | 1.09 |
| R² (log weekly views on log subscribers) | 0.15 | 0.16 |
| Residual SD | 1.40 dex | 1.30 dex |
| Spearman ρ (subscribers, weekly views) | 0.35 | 0.39 |
| Zero weekly views over the 5 weeks | 214,330 (4.5%) | 25,166 (0.8%) |
| Median VPS | 0.16 | 0.38 |
| Overall mean VPS (Σ views / Σ subs) | 2.47 | 2.87 |

**Spread at a given size** (`bins`, variant `all`):

| Subscribers | 10th pct weekly views | median | 90th pct | 90th / 10th ratio | median VPS |
|---|---|---|---|---|---|
| ~10.6k | 2 | 1,252 | 36,106 | ~15,000× | 0.12 |
| ~106k | 45 | 21,158 | 653,627 | ~15,000× | 0.20 |
| ~1.06M | 2,904 | 398,425 | 4.55M | ~1,600× | 0.38 |
| ~10.6M | 262,643 | 6.85M | 42.4M | ~160× | 0.64 |

- For active channels the 90th/10th ratio is ~1,800× at 10k, ~440× at 1M and ~85× at 10M.
- VPS rises with size (slope > 1): the median channel is far below the overall mean at every size.
  - The mean is carried by a heavy upper tail. The 90th percentile of VPS is about 3–7 at every size.
  - The median at 10k subscribers is 0.12; at 10M it is 0.64.
- **Median VPS by topic** (bin-weighted): Music 0.31, Sports 0.26, Lifestyle 0.19, Society 0.18,
  Entertainment 0.18, Knowledge 0.14, Gaming 0.08.
- **Recent subscriber growth separates VPS far more than topic** (ED variant). Channels that grew
  >10% in the five weeks have a median VPS of ~15–25; flat channels sit at ~0.02–0.4. Growth and
  views are jointly driven by new uploads, so this is descriptive, not causal.

**Variance decomposition** (`decomp`, variant `all`, positive weekly views, n = 4,556,505):

| Model (cell means) | R², log weekly views | R², log VPS | within-cell p90−p10, log VPS (dex) |
|---|---|---|---|
| none (total) | — | — | 3.61 |
| subscribers (0.1-dex bins) | 0.145 | 0.004 | 3.60 |
| language (78) | 0.017 | 0.020 | 3.56 |
| topic (8) | 0.028 | 0.031 | 3.53 |
| topic × language (622) | 0.052 | 0.058 | 3.47 |
| subscribers × topic | 0.172 | 0.035 | 3.52 |
| subscribers × language | 0.163 | 0.025 | 3.55 |
| subscribers × topic × language (13,185) | 0.197 | 0.065 | 3.47 |

- Topic, language and their crossing explain at most 6.5% of the variance in log VPS, even
  within subscriber bins.
- Within a single subscriber × topic × language cell, VPS still spans 3.5 orders of magnitude
  from the 10th to 90th percentile (total: 3.6).
- For `active`, the full-cell R² is 0.203 (log views) and 0.053 (log VPS); the within-cell
  p90−p10 of log VPS is 3.17 dex vs 3.27 total.

## Run log

| Date | Run | Databricks run_id | Outcome |
|---|---|---|---|
| 2026-10-15 | Fig 3 v1 | 657336416975082 | Failed: DIVIDE_BY_ZERO in the snapshot audit (first snapshot has no predecessor); switched to `try_divide` |
| 2026-10-15 | Fig 3 v1 | 592329854621263 | Success (~8 min). Falling-total channels floored at 0; superseded |
| 2026-10-15 | Fig 3 v2 | 249961857115890 | Success (~13 min). Falling-total channels excluded; adds `negatives` audit |
| 2026-10-15 | v2 render | local | `render_fig3_local.py --version v2`; CSVs + PNG/PDF in `figs/fig3` |

The v1 tables remain in `dev_sean.matt` (`fig3_subproxy_*_v1`). Only v2 outputs are committed.

## Rerunning

Run `13_…` with a new `version` (or `overwrite=true`). Change the window with
`snapshot_start` / `snapshot_end` (both must be Monday snapshots), then run the local renderer.
