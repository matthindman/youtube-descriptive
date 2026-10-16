# Figure 4 — subscriber thresholds capture unequal shares of channels and views

Figure 4 of *Attention on YouTube* shows how much of the platform's weekly attention lies above
the subscriber thresholds studies commonly use (1k, 10k, 100k, 1M), and how little lies beyond
everything we collected.

- Panel a: cumulative share of weekly views captured by the top-ranked channels, split by subscriber tier.
- Panel b: share of channels and of weekly views at or above each subscriber threshold.
- Panel c: the composition of channels and of weekly views by tier, with the modelled missing mass.

Unlike Figs 1–3 (≥10k universe), this figure covers **every collected channel** (~121M).

## Pipeline

| Step | Code | Output |
|---|---|---|
| 1. Aggregate + tail fit | `14_fig4_thresholds_databricks.py` | `dev_sean.matt.fig4_thresholds_{snapshots,channels,status,thresholds,sbins,vbins,ranked,tail,reconcile}_<version>` |
| 2. Render locally | `../scripts/render_fig4_local.py` | `../figs/fig4/*.png`, `*.pdf`, and CSV source data |

- `fig4_thresholds_plot.py` draws both variants.
- `tail_model.py` is copied verbatim from `Accelerator/common_data_weights`. It is the missing-mass estimator, and its methods write-up goes in the paper's appendix. Keep the two copies in sync.
- Only aggregates leave Databricks: `channels` (121M rows) and `ranked` (73.6M rows) stay there.

```bash
python scripts/render_fig4_local.py --profile accelerator-db --warehouse 86100da4e1fe8713 --version v1
```

Add `--from-csv` to re-render from the saved CSVs without querying.

## Definitions

- **Universe:** every channel in the 2026-10-05 partition of `dev_sean.default.yt_channel_stats_full` (121,416,981 channels; no duplicate rows in either snapshot).
- **Weekly views:** the same five-week window as Fig 3, 2026-08-31 → 2026-10-05, chosen because neither endpoint touches the 2026-08-24 stats stall.
  - Formula: the lifetime-view delta divided by elapsed days between the two collections, × 7.
  - Statuses:
    - `missing_start` (28,102): absent from 08-31, so excluded.
    - `total_fell` (2,563,220): the lifetime total fell, so excluded as in Fig 3.
    - `zero` (45,269,719).
    - `positive` (73,555,940).
  - Shares use the 118,825,659 measurable (zero + positive) channels as the denominator.
- **Subscribers:** `subscriber_count` on 2026-10-05.
  - Tiers: <1k, 1k–10k, 10k–100k, 100k–1M, ≥1M.
  - 2,159 measurable channels hide their count (`hidden`). They count in denominators but in no threshold's numerator.
- **Thresholds** are "at or above" (`subs >= T`).
- **Capture curve (panel a):** positive channels in 0.02-dex weekly-view bins × tier, cumulated from the top. Rank order is weekly-view order, so the curve is exact at bin edges.
- **Missing mass:** `tail_model.py` fitted on the ranked positive weekly views at r0 = 1M, 5M and 50M, with models PL (D0) ≥ D1 ≥ D2 ≥ D3.
  - Platform total = observed + max(modelled mass beyond r0 − observed mass beyond r0, 0), as in the weights pipeline.
  - Headline: r0 = 50M, D3. Shares in panels a–c divide by this total.
- **Reconciliation:** the ≥10k slice has 4,990,312 channels, identical to the Fig 1 universe (`fig1_discovery_channels_v4`), with 0 channels on either side missing from the other. Its 4,770,835 measurable channels equal Fig 3's universe.

## Results (v1, 2026-10-16)

Observed weekly views: 1.452T, on 73.6M channels with positive views.

**Shares at or above each threshold** (`thresholds`):

| Threshold | Channels at or above | Share of channels | Share of weekly views |
|---|---|---|---|
| 100 | 58.8M | 49.5% | 99.9% |
| 1k | 23.0M | 19.3% | 98.7% |
| 10k | 4.77M | 4.0% | 91.1% |
| 100k | 787k | 0.66% | 64.0% |
| 1M | 77.5k | 0.065% | 29.0% |

**By tier** (`status`, measurable channels):

| Tier | Share of channels | Share of weekly views |
|---|---|---|
| ≥1M | 0.065% | 29.0% |
| 100k–1M | 0.60% | 35.0% |
| 10k–100k | 3.35% | 27.1% |
| 1k–10k | 15.3% | 7.5% |
| <1k | 80.7% | 1.3% |

**Concentration:** 48,852 channels (0.04% of measurable channels) hold half of weekly views; 742k hold 90%; 6.45M hold 99%.

**Missing mass** (`tail`; % of the platform total):

| r0 | PL (D0) | D1 | D2 | D3 |
|---|---|---|---|---|
| 1M | 0.86% | 0 | 0 | 0 |
| 5M | 0.97% | 0.97% | 0.97% | 0.46% |
| 50M (headline) | 0.0011% | 0.00082% | 0.00082% | 0.00082% |

- The headline fit adds 11.9M weekly views, which is negligible.
- The most generous fit (PL at r0 = 5M) bounds unseen views at under 1% of the total.
- D1 = D2 at each r0 because the fitted curvature terms (eta1, eta2) are zero there.
- This matches the 2026-09-28 weights release, where no model predicted more views among small channels than were already observed.

## Caveats

- **Channels whose lifetime total fell are excluded.** That is 2.56M channels, whose combined drop of −2.09T views exceeds the window's observed positive views.
  - These are one-off drops (video deletions or privatisations, count corrections; see the Fig 3 README), not negative attention.
  - Exclusion is mildly uneven by tier: about 4% of channels in each ≥1k tier are excluded but only 1.6% below 1k. That slightly understates the larger tiers' view shares.
- **The tail model is a fit to the rank–views curve.** It assumes unfound channels continue that curve, so it cannot see attention on channels unlike any we found.
- **Weights pipeline differences:** it uses raw window deltas and floors falls at 0. Here we normalise by elapsed days and exclude falls, matching Fig 3.

## Run log

| Date | Run | Databricks run_id | Outcome |
|---|---|---|---|
| 2026-10-16 | Fig 4 v1 | 614377496030415 | Success (~17 min on cluster `0818-185002-anfay17g`) |
| 2026-10-16 | v1 render | local | `render_fig4_local.py --version v1`; CSVs + PNG/PDF in `figs/fig4` |
