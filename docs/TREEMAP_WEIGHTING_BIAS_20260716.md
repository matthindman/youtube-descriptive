# Treemap Weighting-Bias Comparison

**Completed:** 2026-07-16
**Analysis version:** `youtube_topic_treemap_weighting_bias_20260716_v1`
**Cohort:** 4,786,690 channels with at least 10,000 subscribers on 2026-06-15
**Traffic:** 5,466,594,900,717 accepted positive views from 2026-05-18 to 2026-06-15

## Purpose

This analysis isolates the deterministic distortion produced when every
YouTube channel receives equal weight, even though the substantive target is
what viewers see. It compares two distributions on the identical full census:

1. **Equal-channel distribution:** every channel contributes total mass one.
2. **View-weighted distribution:** every channel contributes its accepted
   positive four-week view delta.

Both use the same language labels, topic taxonomy, and family-balanced topic
allocation. Named-channel display overrides are excluded. The only changed
quantity is the channel-level weight.

## Main Result

The language-family total-variation distance is **25.342 percentage points**.
In other words, 25.342% of probability mass must move between language-family
cells to turn the equal-channel distribution into the view-weighted exposure
distribution.

| Resolution | Total-variation distance |
|---|---:|
| Language | 13.302 pp |
| Topic family | 21.345 pp |
| Language x family | 25.342 pp |
| Language x family x leaf | 28.280 pp |

The signed differences conserve exactly:

- Equal weighting overstates cells by a combined **25.341984 pp**.
- Equal weighting understates cells by a combined **25.341984 pp**.
- `CONSERVATION: PASS`.

## Interpretation

Equal weighting describes the inventory of channels, not the distribution of
audience exposure. The most consequential family distortion is Entertainment:

| Family | Equal-channel share | View-weighted share | Equal minus view | View exposure multiplier |
|---|---:|---:|---:|---:|
| Entertainment | 17.41% | 34.09% | -16.69 pp | 1.96x |
| Music | 17.44% | 11.25% | +6.19 pp | 0.65x |
| Society | 11.92% | 6.75% | +5.17 pp | 0.57x |
| Unlabeled | 5.61% | 1.58% | +4.03 pp | 0.28x |
| Lifestyle | 30.58% | 34.33% | -3.75 pp | 1.12x |
| Knowledge | 4.63% | 1.56% | +3.07 pp | 0.34x |
| Gaming | 10.14% | 7.25% | +2.88 pp | 0.72x |
| Sports | 2.28% | 3.18% | -0.91 pp | 1.40x |

The largest language distortion is English. Equal weighting estimates English
at 41.15% of channels, compared with 50.70% of view exposure, an understatement
of 9.55 points. It overstates Undetermined by 2.22 points, Portuguese by 1.63,
Russian by 1.57, Arabic by 0.91, and French by 0.84. It understates Indonesian
by 1.21 points and Turkish by 0.68.

The largest joint cells make the mechanism clearer:

| Language-family cell | Equal share | View share | Bias | Exposure multiplier |
|---|---:|---:|---:|---:|
| English x Entertainment | 8.25% | 18.00% | -9.75 pp | 2.18x |
| English x Lifestyle | 13.31% | 18.09% | -4.78 pp | 1.36x |
| English x Music | 6.93% | 5.52% | +1.40 pp | 0.80x |
| Hindi x Entertainment | 1.15% | 2.49% | -1.34 pp | 2.17x |
| Indonesian x Entertainment | 0.67% | 1.90% | -1.23 pp | 2.84x |
| English x Unlabeled | 1.50% | 0.29% | +1.21 pp | 0.19x |
| English x Knowledge | 1.93% | 0.78% | +1.14 pp | 0.41x |
| English x Society | 2.92% | 1.85% | +1.07 pp | 0.63x |

## Visual Design

### Paired composition treemap

`treemap_equal_vs_view_weighted_v1` shows the two estimands on the same
language-family hierarchy. The display retains the union of the top 12
languages under either weighting, keeps `Undetermined` separate, and pools the
remaining language tail without dropping its mass.

This is the direct counterfactual: the left panel is the alternate treemap that
would result from treating all channels equally; the right panel is what the
same corpus looks like when area represents audience exposure.

### Bias-balance treemap

`treemap_weighting_bias_balance_v1` is the primary explanatory figure. It
decomposes the equal-minus-view difference into two treemaps:

- Left: cells that equal weighting makes too large.
- Right: cells that equal weighting makes too small.

Each panel contains exactly 25.342 points of displaced mass. Tile area is the
absolute percentage-point error. Family color remains fixed, so direction is
communicated by panel placement and headings rather than replacing the topic
palette.

This representation reveals an asymmetry hidden by ordinary side-by-side
treemaps: overstatement is diffuse across many Music, Society, Knowledge,
Gaming, and Unlabeled cells, while understatement is concentrated heavily in
English Entertainment and English Lifestyle.

### Interactive bias lens

The self-contained HTML supplies three modes:

- `Equal channels`
- `View weighted`
- `Distortion`

Hover reports equal-channel share, view-weighted share, percentage-point bias,
and the view-exposure multiplier. The Distortion mode splits the tree first by
direction, then language, then family.

## Data and Method

Source table:

`dev_sean.matt.yt_treemap_full_corpus_lid_v3_20260715_v1_allocations_family_balanced_raw`

Export statement ID:

`01f18162-6a5c-14e0-b238-45fd64303d7a`

For language-family cell `j`:

```text
channel_share_j = sum(allocation_weight_j) / 4,786,690

view_share_j = sum(allocation_weight_j * view_count_4wk)
               / 5,466,594,900,717

bias_j = channel_share_j - view_share_j

exposure_multiplier_j = view_share_j / channel_share_j

total_variation = 0.5 * sum_j(abs(bias_j))
```

Channels with missing, zero, or invalid negative deltas remain in the
equal-channel census but contribute no positive view mass. This is intentional:
the figure compares channel prevalence with the established positive-view
treemap estimand.

## Reproduction

Use the registered Databricks profile and SQL warehouse:

```bash
python3 scripts/export_unweighted_treemap_bias_inputs.py

MPLBACKEND=Agg MPLCONFIGDIR=/tmp/treemap-mpl \
  python3 scripts/render_unweighted_treemap_bias.py
```

Generated artifacts are under
`outputs/youtube_topic_treemap_weighting_bias_20260716_v1/`:

- `treemap_equal_vs_view_weighted_v1.png` and `.svg`
- `treemap_weighting_bias_balance_v1.png` and `.svg`
- `treemap_weighting_bias_lens_v1.html`
- `weighting_bias_language_summary_v1.csv`
- `weighting_bias_family_summary_v1.csv`
- `weighting_bias_language_family_summary_v1.csv`
- `treemap_weighting_bias_cells_v1.csv`
- `bias_manifest.json`
- `render_log_weighting_bias_v1.txt`

Static dimensions are 3300 x 1936. The paired comparison contains 110
equal-weight and 97 view-weighted rendered cells. The bias balance contains 63
overstatement and 29 understatement cells. Small tiles remain represented but
are unlabeled when text does not fit.

## Relation to Repeated-Sample Variance

This figure uses the complete >=10K census, so it measures estimand bias with
no channel-sampling error. It does not estimate the colleague's separate
repeated-sample variance quantity.

The appropriate follow-up is to draw repeated probability samples from the
row-level allocation table and compare, for each sample size and design:

1. unweighted channel-share estimates;
2. view-weighted ratio estimates;
3. empirical bias against the census view target;
4. empirical standard error and RMSE;
5. interval coverage; and
6. design effect or effective sample size caused by heavy-tailed views.

That simulation should compare simple random channel samples with at least one
stratified or probability-proportional-to-size design. The present census
comparison supplies the frozen population targets for that exercise.
