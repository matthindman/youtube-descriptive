# Below-10K Full-Corpus Treemap Sensitivity Analysis

**Analysis date:** 2026-07-16
**Frame snapshot:** 2026-06-15
**Traffic snapshot:** 2026-07-13
**Traffic window:** 28 days
**Pilot:** 2,000 channels, 200 sampled independently within each 1,000-subscriber band below 10,000
**Language labels:** `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_channel_language_current`

## Executive Assessment

The best estimate is that collecting the full below-10K universe would change
the *number of channels* in the corpus dramatically but would change the
*view-area treemap* much less.

- Channels below 10K are 117,235,838 of the 122,124,193 channels with known
  subscriber counts in the aligned frame, or **96.00%**.
- They contribute 670.27 billion of 6.135 trillion accepted positive four-week
  views, or **10.93%**.
- The point estimate makes the full view distribution more English and more
  music-oriented. It makes Spanish, Portuguese, Entertainment, and Sports
  smaller. English's estimated increase is not quite stable at the 95% level;
  the Music increase and the Spanish, Portuguese, Entertainment, and Sports
  declines are stable under the sampling model.
- At the family level, the full view map is estimated as 32.97% Lifestyle,
  32.85% Entertainment, 12.66% Music, 8.14% Gaming, 6.53% Society, 3.47%
  Sports, 1.85% Unlabeled, and 1.54% Knowledge.
- The largest subtopic point increases are Hip hop music, Rock music, Music of
  Asia, Action-adventure games, and Action games. Only Music of Asia is clearly
  positive at 95% among those large increases. Hip hop and Rock are plausible
  increases, but their intervals narrowly include zero.

These are design-based sensitivity estimates, not a census reconstruction.
Traffic is extremely heavy-tailed. The 2,000 sampled channels have an overall
calibrated view-weight effective sample size of only **24.7**. One sampled
channel supplies **12.36%** of the estimated below-10K view composition. The
confidence intervals below are therefore more informative than the nominal
sample size.

## Target and Alignment

The target is the language/topic composition of positive accepted 28-day view
growth for all channels with a known subscriber count on 2026-06-15. A channel
is below or above 10K according to its 2026-06-15 subscriber count. Traffic is:

```text
view_count_4wk = total_view_count_2026_07_13 - total_view_count_2026_06_15
```

Negative revisions and missing deltas contribute zero positive view mass.
This matches the treemap area estimand, but it is not signed net traffic.

The existing >=10K projection and the below-10K pilot are both evaluated on
this shifted 2026-06-15 to 2026-07-13 window. The older production treemap's
2026-05-18 to 2026-06-15 traffic is not mixed into this analysis.

## Exact Calibration Margins

The equal 200-per-band sample must not be pooled without weighting. The
0-999 band is 83.88% of below-10K channels, while the 9,000-9,999 band is only
0.33%. Exact frame counts and positive-view totals were reconstructed from
`dev_sean.default.yt_channel_stats_full`.

| Subscriber band | Frame channels | % below-10K channels | Positive 4-week views | % below-10K views | View effective n | Largest sampled view share |
|---|---:|---:|---:|---:|---:|---:|
| 0-999 | 98,342,040 | 83.88% | 114.62B | 17.10% | 1.87 | 72.28% |
| 1,000-1,999 | 8,631,046 | 7.36% | 96.18B | 14.35% | 1.70 | 75.71% |
| 2,000-2,999 | 3,626,215 | 3.09% | 78.17B | 11.66% | 13.14 | 19.66% |
| 3,000-3,999 | 2,070,529 | 1.77% | 68.25B | 10.18% | 22.13 | 12.14% |
| 4,000-4,999 | 1,372,621 | 1.17% | 62.94B | 9.39% | 2.30 | 64.60% |
| 5,000-5,999 | 991,331 | 0.85% | 55.25B | 8.24% | 3.56 | 45.98% |
| 6,000-6,999 | 749,286 | 0.64% | 53.03B | 7.91% | 6.01 | 35.29% |
| 7,000-7,999 | 588,632 | 0.50% | 50.47B | 7.53% | 1.85 | 73.20% |
| 8,000-8,999 | 475,919 | 0.41% | 47.04B | 7.02% | 8.24 | 22.74% |
| 9,000-9,999 | 388,219 | 0.33% | 44.31B | 6.61% | 5.19 | 37.15% |

## Estimation

For channel prevalence, the analysis post-stratifies the within-band sample
share `p_h` to the exact channel count `N_h`:

```text
p_below = sum_h (N_h / N_below) * p_h
```

For view composition, it estimates a within-band ratio `r_h` and calibrates it
to the exact positive-view total `V_h`:

```text
r_h = sum_i (views_i * allocation_i) / sum_i views_i
q_below = sum_h (V_h / V_below) * r_h
```

The full-corpus composition combines the estimated below-10K composition with
the observed same-window >=10K composition using exact positive-view or
channel-count margins. Topic allocation uses the production normalization,
parent suppression, remaps, and family-balanced allocation. Every channel has
total topic weight one; absent topics are retained as `Unlabeled`.

The reported standard error is the standard deviation across 5,000
within-band multinomial bootstrap replicates (seed 20260716). The 95% interval
is the percentile interval. A finite-population-corrected linearized standard
error is also present in the CSVs. Current >=10K composition is treated as
fixed, so the projected-full and change standard errors are the same in
percentage-point units.

## Language: View-Area Treemap

`SE` is the bootstrap standard error for the projected full share, in
percentage points. `Change CI` is the 95% interval for the change from the
current >=10K view composition.

| Language | Below-10K share (SE) | Current >=10K | Projected full | Change | SE | Change 95% CI |
|---|---:|---:|---:|---:|---:|---:|
| English | 68.26% (7.17) | 49.05% | 51.15% | +2.10 pp | 0.78 | [-0.09, +2.88] |
| Spanish | 2.50% (0.97) | 6.58% | 6.14% | -0.45 pp | 0.11 | [-0.60, -0.19] |
| Hindi | 2.62% (1.60) | 6.49% | 6.06% | -0.42 pp | 0.17 | [-0.53, +0.14] |
| Indonesian | 4.53% (2.26) | 5.82% | 5.68% | -0.14 pp | 0.25 | [-0.51, +0.35] |
| Arabic | 2.38% (3.27) | 4.72% | 4.47% | -0.26 pp | 0.36 | [-0.46, +0.72] |
| Portuguese | 0.71% (0.61) | 3.93% | 3.58% | -0.35 pp | 0.07 | [-0.39, -0.14] |
| Japanese | 2.87% (1.64) | 2.27% | 2.34% | +0.07 pp | 0.18 | [-0.19, +0.51] |
| Korean | 0.18% (0.13) | 2.52% | 2.26% | -0.26 pp | 0.01 | [-0.27, -0.21] |
| Russian | 1.45% (1.13) | 2.18% | 2.10% | -0.08 pp | 0.12 | [-0.22, +0.27] |
| Vietnamese | 1.14% (1.31) | 2.06% | 1.96% | -0.10 pp | 0.14 | [-0.20, +0.34] |
| Chinese | 6.24% (2.92) | 1.08% | 1.64% | +0.56 pp | 0.32 | [-0.09, +0.79] |
| French | 1.98% (2.54) | 0.80% | 0.93% | +0.13 pp | 0.28 | [-0.06, +0.99] |
| Undetermined | 0.40% (0.35) | 1.04% | 0.97% | -0.07 pp | 0.04 | [-0.10, +0.04] |

The point estimate says English gains about 2.1 points and Chinese about 0.6,
while Spanish, Hindi, Portuguese, and Arabic lose share. The strongest
language conclusions under the stated sampling model are the Spanish and
Portuguese declines. The English and Chinese increases are plausible but not
unambiguous at 95%, and the Chinese estimate rests on only 17 positive-view
pilot channels. Korean, Turkish, and Thai also have statistically clear point
declines, but their absolute changes are smaller and they are supported by
small pilot cells.

## Language: Channel Counts

This is a different estimand from treemap area. Because 96% of channels are
below 10K, the projected full channel-count distribution is dominated by the
post-stratified 0-999 sample.

| Language | Current >=10K | Projected full | Change | Projected SE | Change 95% CI |
|---|---:|---:|---:|---:|---:|
| English | 41.15% | 30.13% | -11.02 pp | 2.58 | [-15.98, -5.93] |
| Undetermined | 3.22% | 25.97% | +22.75 pp | 2.67 | [+17.50, +28.15] |
| Portuguese | 5.51% | 7.08% | +1.57 pp | 1.54 | [-1.15, +4.73] |
| Spanish | 6.83% | 5.56% | -1.27 pp | 1.29 | [-3.70, +1.39] |
| Hindi | 6.56% | 4.57% | -1.99 pp | 1.13 | [-4.01, +0.40] |
| Indonesian | 4.15% | 3.90% | -0.25 pp | 1.05 | [-2.10, +1.96] |
| Arabic | 5.38% | 3.31% | -2.07 pp | 0.90 | [-3.66, -0.11] |
| French | 1.58% | 2.74% | +1.16 pp | 0.98 | [-0.52, +3.18] |
| Russian | 3.71% | 2.65% | -1.06 pp | 0.89 | [-2.64, +0.90] |
| Bengali | 2.11% | 2.03% | -0.08 pp | 0.79 | [-1.38, +1.59] |

The 25.97% projected `Undetermined` channel share is not a substantive
language finding. Language classification is only 69% in the 0-999 sample,
and that band is 83.88% of all below-10K channels. By contrast,
`Undetermined` contributes only 0.40% of estimated below-10K view mass. Any
publication about channel-count language diversity needs better low-band text
coverage or a principled model for unresolved cases.

## Topic Families: View-Area Treemap

| Family | Below-10K share (SE) | Current >=10K | Projected full | Change | SE | Change 95% CI |
|---|---:|---:|---:|---:|---:|---:|
| Lifestyle | 25.34% (6.05) | 33.91% | 32.97% | -0.94 pp | 0.66 | [-1.43, +0.92] |
| Entertainment | 26.77% (4.07) | 33.60% | 32.85% | -0.75 pp | 0.44 | [-1.94, -0.30] |
| Music | 25.54% (4.01) | 11.08% | 12.66% | +1.58 pp | 0.44 | [+0.42, +2.12] |
| Gaming | 13.65% (3.87) | 7.46% | 8.14% | +0.68 pp | 0.42 | [-0.17, +1.46] |
| Society | 5.53% (1.49) | 6.65% | 6.53% | -0.12 pp | 0.16 | [-0.46, +0.14] |
| Sports | 1.48% (0.80) | 3.71% | 3.47% | -0.24 pp | 0.09 | [-0.38, -0.04] |
| Unlabeled | 0.37% (0.27) | 2.04% | 1.85% | -0.18 pp | 0.03 | [-0.21, -0.09] |
| Knowledge | 1.33% (0.69) | 1.56% | 1.54% | -0.03 pp | 0.08 | [-0.12, +0.15] |

The clearest family result is Music: it is estimated at 25.5% of below-10K
positive views and rises from 11.1% to 12.7% in the full map. Entertainment
and Sports decline. Gaming's point increase is meaningful but uncertain, and
Lifestyle's apparent decline is not stable because a few large pilot channels
move it substantially.

Topic absence is severe for channel counts but not for traffic: `Unlabeled`
is an estimated 33.54% of below-10K channels yet only 0.37% of their views.
This indicates that the channels missing topics are mostly low-traffic. The
view treemap is comparatively robust to missing topics; a channel-count topic
analysis is not.

## Subtopics

The largest point changes are shown below. `Positive pilot n` counts sampled
channels with both the subtopic and positive accepted traffic.

| Subtopic | Positive pilot n | Current >=10K | Projected full | Change | SE | Change 95% CI |
|---|---:|---:|---:|---:|---:|---:|
| Hip hop music | 41 | 0.70% | 1.33% | +0.63 pp | 0.36 | [-0.07, +0.84] |
| Rock music | 30 | 0.24% | 0.85% | +0.61 pp | 0.31 | [-0.00, +0.76] |
| Music of Asia | 300 | 4.34% | 4.77% | +0.43 pp | 0.23 | [+0.03, +0.89] |
| Action-adventure game | 118 | 1.23% | 1.54% | +0.30 pp | 0.16 | [-0.07, +0.46] |
| Action game | 133 | 2.00% | 2.29% | +0.29 pp | 0.20 | [-0.11, +0.52] |
| Health | 39 | 0.68% | 0.96% | +0.28 pp | 0.16 | [-0.05, +0.38] |
| Technology | 59 | 1.17% | 1.35% | +0.18 pp | 0.15 | [-0.08, +0.51] |
| Lifestyle unspecified | 423 | 18.92% | 18.35% | -0.57 pp | 0.45 | [-0.98, +0.67] |
| Hobby | 135 | 5.67% | 5.36% | -0.31 pp | 0.16 | [-0.45, +0.15] |
| Politics | 21 | 2.20% | 1.97% | -0.23 pp | 0.00 | [-0.24, -0.22] |
| Movies | 198 | 16.00% | 15.78% | -0.22 pp | 0.32 | [-1.04, +0.22] |
| Humor | 26 | 3.03% | 2.85% | -0.18 pp | 0.07 | [-0.31, -0.04] |
| TV shows | 29 | 1.71% | 1.55% | -0.16 pp | 0.02 | [-0.18, -0.12] |
| Football | 21 | 1.15% | 1.06% | -0.09 pp | 0.03 | [-0.12, -0.00] |

Music of Asia is the strongest well-supported positive leaf result. The large
Hip hop and Rock point increases are visually consequential but sensitive to
the sampled traffic tail. Politics, Humor, and TV shows decline under the
sampling model. The apparently very narrow intervals for small leaves must be
read cautiously: a bootstrap cannot represent unobserved rare below-10K
channels or topics.

## What the Standard Errors Do and Do Not Cover

The standard errors cover random sampling under these assumptions:

1. Each set of 200 was a simple random sample from its original subscriber
   band.
2. The reconstructed 2026-06-15 band margins adequately approximate the frame
   used for the 2026-06-18 sample.
3. The target is this one 28-day positive-view window.
4. The sampled values are observed without language/topic classification
   error.
5. Missing/negative traffic contributes zero positive mass as specified.

They do **not** cover:

- three-day frame drift between the 2026-06-15 margin and 2026-06-18 sampling;
- month-to-month traffic variability;
- LID, DeepSeek, or topic taxonomy misclassification;
- nonrandom missing text or topic arrays;
- YouTube counter revisions;
- rare languages/topics absent from the 2,000 sampled channels;
- failure of the assumed within-band simple random sampling design; or
- the proportional-composition assumption used to scale the labeled >=10K
  projection to exact >=10K margins.

There is observable frame drift: 173 sampled channels fall into a different
1,000-subscriber band when measured at the 2026-06-15 prior snapshot. The
largest drift is in bands 8 and 9. This is not added to the reported SE.

The >=10K projection covers 96.19% of exact >=10K positive view mass and
97.92% of exact >=10K channels. Its observed composition is scaled
proportionally to the exact >=10K totals. Any systematic composition of the
uncovered portion is another unquantified uncertainty.

## Reproduction

Export exact margins and bounded row-level inputs:

```bash
env DATABRICKS_AUTH_STORAGE=plaintext \
  python3 scripts/export_banded_lt10k_sensitivity_inputs.py
```

Run the estimator:

```bash
python3 scripts/analyze_banded_lt10k_full_corpus_sensitivity.py
```

Primary outputs (ignored by git):

- `outputs/banded_lt10k_full_corpus_sensitivity_20260716/language_estimates.csv`
- `outputs/banded_lt10k_full_corpus_sensitivity_20260716/family_estimates.csv`
- `outputs/banded_lt10k_full_corpus_sensitivity_20260716/leaf_estimates.csv`
- `outputs/banded_lt10k_full_corpus_sensitivity_20260716/band_diagnostics.csv`
- `outputs/banded_lt10k_full_corpus_sensitivity_20260716/analysis_manifest.json`

SQL statement IDs are recorded in the manifest. The primary IDs are:

| Export | Statement ID |
|---|---|
| 1K band margins | `01f18132-d3ad-1115-aa56-a5883f049d45` |
| >=10K language views | `01f18133-7180-1edc-bfd3-550435907988` |
| >=10K family/leaf views | `01f18133-96ce-1b87-a8c8-ecb427e38ae2` |
| Pilot rows | `01f18133-d5ff-13c0-8c11-d14c299a3bf7` |
| >=10K language channels | `01f18134-010c-1b20-9662-bbabdf3b19b8` |
| >=10K family/leaf channels | `01f18134-0de8-1aa9-9113-3357152c6b37` |

Unknown-subscriber channels are excluded: 2,201 channels and 17.40 million
positive four-week views. All language, family, and leaf projected view shares
sum to one within floating-point tolerance.
