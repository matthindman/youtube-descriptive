# Full-Corpus Weighted Treemap Runbook

**Implementation date:** 2026-07-18

**Design version:** `full_corpus_dual_sample_20260717_v1`

**Status (2026-07-21):** Complete. PPS and SRS/remainder language publication,
the combined one-row-per-channel language table, paired attention/channel
estimation, static and interactive rendering, conservation QA, and manual
legibility review all passed.

## Purpose

This workflow turns the frozen-frame census plus the two registered below-10K
samples into additive language-by-topic cells for publication. It produces:

1. an **attention ecology** whose area estimates four-week positive view mass;
2. a **channel ecology** whose area estimates equal-channel prevalence;
3. self-contained interactive language -> family -> subtopic explorers; and
4. a coefficient-style comparison of view share minus channel share with
   design-based standard errors.

The code does not append the old 2,000-channel banded pilot to the >=10K
treemap. It uses the registered 1,000,000-channel Poisson PPS sample for view
mass, the separate 1,000,000-channel SRS for equal-channel prevalence, and the
>=10K universe as an exact census. Subscriber-unknown channels remain an exact
certainty stratum in the primary `all_retrievable` scope.

## Core Files

| File | Role |
|---|---|
| `config/full_corpus_dual_sample_20260717_v1.json` | Frozen sample settings plus post-sampling treemap publication settings |
| `youtube_descriptive/src/14_full_corpus_dual_sample_analysis_databricks.py` | Allocations, estimators, exact topic margins, publication cells, and QA |
| `scripts/build_full_corpus_dual_sample_analysis_job.py` | Ordered existing-cluster job: allocate -> estimate -> qa -> publish_treemap |
| `scripts/run_full_corpus_dual_sample_analysis.sh` | Uploads code/config and runs the four analysis stages |
| `scripts/render_full_corpus_weighted_treemaps.py` | Local static, interactive, and coefficient rendering |
| `scripts/render_full_corpus_expansion_changes.py` | Text and coefficient-style absolute/proportional comparisons of the non-tail baseline with the PPS-expanded platform estimate |
| `scripts/render_treemap_v3.py` | Accepted squarified layout, family palette, typography, and label-fit code reused by the aggregate renderer |
| `scripts/run_full_corpus_weighted_treemaps.sh` | Downloads compact publication exports and renders all artifacts |

Generated images, HTML, Parquet, CSV, JSON, and logs belong under `outputs/`
and must not be committed.

## Required Upstream State

The final paired analysis requires all of the following. They were complete for
the production execution recorded below:

- the two registered sample selections and the >=10K census union;
- channel-description and recent-video text backfill for unresolved sample
  channels, with a terminal disposition for every request;
- dual OpenLID-v3/GlotLID inference and DeepSeek Flash fallback;
- final one-row-per-channel language table
  `dev_sean.matt.yt_dual_sample_20260717_v1_channel_language_current`;
- platform topic arrays in the frozen frame;
- human-validation-gated calibration for the optional model-completed topic
  variant.

The primary paper treemap is `platform_only`. The model-completed variant is a
robustness analysis and cannot be substituted for it.

The separate `attention_pps` mode remains available for historical replication
because it uses the finalized PPS language publication and does not estimate the
equal-channel ecology. It writes isolated `_pps_attention` tables and cannot
overwrite final outputs.

## Estimands And Weights

For channel `i`, let:

- `V_i` be accepted nonnegative four-week view growth;
- `a_ilt` be its allocation to language `l` and platform topic leaf `t`;
- `pi_Pi` be its Poisson PPS inclusion probability;
- `pi_S` be the constant SRS inclusion probability in the below-10K frame.

The allocation conserves each channel:

```text
sum_(l,t) a_ilt = 1
```

Language is one-valued. Topic allocation is family-balanced, then balanced
within family, so a channel with many platform tags does not receive more total
mass than a channel with one tag. `Undetermined`, `Unlabeled`, and
`Other / Unmapped YouTube topic` remain explicit cells.

### Raw view estimate

For each language-topic cell:

```text
V_hat_lt = sum_(i in exact head) V_i a_ilt
           + sum_(i in PPS sample) V_i a_ilt / pi_Pi
```

The full-frame view denominator is known. Therefore the raw global share is:

```text
Q_hat_lt = V_hat_lt / V_full
```

Under the Poisson PPS design this is the primary design-unbiased estimate. Its
tail variance estimator is:

```text
Var_hat(V_hat_lt) =
  sum_(i in PPS sample) (1 - pi_Pi) (V_i a_ilt)^2 / pi_Pi^2
```

The share SE divides the square root by `V_full`.

### Raw equal-channel estimate

The channel ecology uses the separate SRS:

```text
N_hat_lt = sum_(i in exact head) a_ilt
           + N_tail * mean_(i in SRS)(a_ilt)
```

Its variance includes the finite-population correction. The global channel
share divides by the known frame channel count.

The >=10K census is exact, but it is not allowed to dominate the channel
ecology merely because it was fully enumerated. Each census channel contributes
one channel, just as each represented below-10K channel does; the SRS expansion
supplies the below-10K count mass, and the final denominator is the known full
frame size. Thus the >=10K census occupies only its actual share of all frame
channels (roughly 4%), rather than receiving one-half of the map or one-half of
the total weight. Subscriber-unknown certainty rows are reported separately
and enter only the registered `all_retrievable` scope.

The frozen `all_retrievable` frame contains 122,126,394 channels:
4,888,355 in the >=10K census (4.0027%), 117,235,838 in the below-10K
sampling frame (95.9955%), and 2,201 subscriber-unknown certainty rows
(0.0018%). These fixed frame shares, not the relative sizes of the realized
census and SRS files, determine their equal-channel treemap mass.

Publication QA computes the expected frame shares directly from
`subscriber_status`, compares them with the summed treemap geometry, and fails
if the exact-stratum, SRS-tail, or denominator discrepancy exceeds `1e-8`.
The local renderer prints `EQUAL-CHANNEL FRAME MIX` and
`EQUAL-CHANNEL STRATUM CALIBRATION: PASS` before writing the channel treemap.

## Calibration For Treemap Geometry

Raw Horvitz-Thompson cell estimates do not necessarily sum to the known total
in one realized sample. Plotting them directly can leave parent-child totals
incoherent. The renderer therefore uses calibrated totals for geometry while
retaining the raw estimates and SEs for inference.

For platform topics, the frame contains `raw_topic_categories` and `V_i` for
every channel. The pipeline computes exact below-10K family/leaf margins before
using any language result. For platform topic leaf `t`:

```text
c_view_t = known tail view total_t
           / sum_l(raw PPS tail estimate_lt)

c_channel_t = known tail allocated-channel total_t
              / sum_l(raw SRS tail estimate_lt)
```

The treemap geometry is:

```text
view_geometry_lt = exact_head_view_lt
                   + c_view_t * raw_PPS_tail_view_lt

channel_geometry_lt = exact_head_channel_lt
                      + c_channel_t * raw_SRS_tail_channel_lt
```

This procedure has four important properties:

- every platform family/leaf margin equals its exact frozen-frame total;
- every global set of leaf cells sums exactly to its known denominator;
- language-topic intersections retain the design-weighted language
  distribution within each topic leaf; and
- calibration never changes the raw HT/SRS estimates, SEs, or intervals.

If a positive exact topic margin has no sampled support, publication fails. It
does not silently assign that mass to a language or drop it.

The optional `model_completed` variant has no known full-frame model-topic
margin. Its geometry uses the registered single known-tail-total ratio factor,
not the exact platform-topic calibration.

## Published Cell Shares

For each complete language/family/leaf cell, the publication table includes:

```text
view_geometry_global_share
channel_geometry_global_share
view_within_language_share
channel_within_language_share
view_within_language_family_share
channel_within_language_family_share
```

All are calculated only after complete-cell estimation and calibration.
Pooling for the static figure happens later and cannot alter the underlying
estimates.

The global raw share has a design SE because its denominator is known. A
within-language share is a ratio with an estimated language denominator and
requires numerator-denominator covariance. The table therefore marks
conditional-share uncertainty as requiring Taylor linearization or joint
replicates; it does not report a fabricated conditional SE.

## Databricks Outputs

The four-stage analysis writes:

| Table | Contents |
|---|---|
| `dev_sean.matt.yt_dual_sample_20260717_v1_allocations` | Channel-level conserved language/family/leaf allocations for analysis-union channels |
| `dev_sean.matt.yt_dual_sample_20260717_v1_platform_topic_margins` | Exact frozen-frame platform family/leaf channel and view margins |
| `dev_sean.matt.yt_dual_sample_20260717_v1_estimates` | Raw and global-total-calibrated estimates at language, family, leaf, and intersection levels |
| `dev_sean.matt.yt_dual_sample_20260717_v1_weighting_differences` | View-minus-channel estimates and approximate independent-design SEs |
| `dev_sean.matt.yt_dual_sample_20260717_v1_publication_estimates` | Paired channel/view rollups over the union of SRS and PPS observed cell supports |
| `dev_sean.matt.yt_dual_sample_20260717_v1_treemap_cells` | Exact-margin-calibrated language/family/leaf geometry plus raw inference fields |
| `dev_sean.matt.yt_dual_sample_20260717_v1_treemap_qa` | Publication acceptance metrics |

Compact renderer inputs are exported to:

```text
dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1/treemap_publication/
  treemap_cells/
  publication_estimates/
  run_manifest.json
```

Each Parquet directory is coalesced to one part for deterministic local use.

## Execution

Use only the registered profile and existing cluster:

```text
Profile: matt.hindman@researchaccelerator.org
Cluster: 0601-203643-bkxsqffg (matt-research-gencompute)
```

Run the Databricks analysis after enrichment is complete:

```bash
bash scripts/run_full_corpus_dual_sample_analysis.sh
```

That job runs, in order:

```text
allocate -> estimate -> qa -> publish_treemap
```

It uses `existing_cluster_id`; it neither creates nor restarts compute.

Then download the compact exports and render locally:

```bash
bash scripts/run_full_corpus_weighted_treemaps.sh
```

The wrapper always invokes the CLI as:

```bash
env DATABRICKS_AUTH_STORAGE=plaintext \
  databricks -p matt.hindman@researchaccelerator.org ...
```

## Completed PPS Attention Expansion (2026-07-21)

The provisional attention product combines exact non-tail view mass with the
registered design-weighted below-10K PPS sample. It does not use SRS records or
fabricate equal-channel estimates.

### Inputs and label rule

- Frozen frame: `dev_sean.matt.yt_dual_sample_20260717_v1_frame`
- Frame topic companion: `dev_sean.matt.yt_dual_sample_20260717_v1_platform_topics`
- Analysis union: `dev_sean.matt.yt_dual_sample_20260717_v1_analysis_union`
- Final PPS labels:
  `dev_sean.matt.yt_dual_sample_20260717_v1_channel_language_pps_current`
- Existing exact-stratum labels:
  `dev_sean.matt.yt_dual_sample_20260717_v1_channel_language_current`
- Completed exact-stratum dual-LID agreements, where the existing exact label
  is absent:
  `dev_sean.matt.yt_dual_sample_20260717_v1_language_routing_comparison_remainder`

For this provisional product, the exact stratum reuses published labels first,
then completed OpenLID-v3/GlotLID agreements. Remaining exact-stratum channels
are explicitly `und`. The PPS tail always uses the finalized PPS publication.
This lookup is materialized as:

```text
dev_sean.matt.yt_dual_sample_20260717_v1_pps_attention_channel_language_current
```

The lookup has 5,890,700 rows and 220,334 `und` rows. That unresolved share is
a measurement limitation, not PPS sampling attrition. This provisional run is
superseded for substantive use by the completed paired execution below.

### Estimation and calibration

The exact component is the >=10K census plus subscriber-unknown certainty
rows. The tail component uses `V_i / pi_Pi` for each realized PPS channel.
Exact full-frame platform-topic view margins then calibrate the PPS
language-by-topic estimates within each topic leaf. Consequently:

- the tail contributes exactly 670,270,625,558 accepted four-week views;
- the calibrated tail is 10.9257% of final platform view mass;
- every family/leaf margin matches its exact frozen-frame view margin; and
- raw Horvitz-Thompson estimates and design SEs remain unchanged for
  inference.

The realized PPS sample has 1,000,144 channels. Its raw HT tail total is
670,248,334,181.7 views, so the global diagnostic ratio is 1.0000333 before
leaf-level calibration.

### Commands

Run the isolated Databricks mode:

```bash
ANALYSIS_MODE=attention_pps \
  bash scripts/run_full_corpus_dual_sample_analysis.sh
```

The launcher can resume without repeating successful stages:

```bash
ANALYSIS_MODE=attention_pps START_AT=estimate \
  bash scripts/run_full_corpus_dual_sample_analysis.sh

ANALYSIS_MODE=attention_pps START_AT=publish_treemap \
  bash scripts/run_full_corpus_dual_sample_analysis.sh
```

Download the compact provisional export and render the treemap plus expansion
comparisons:

```bash
ANALYSIS_MODE=attention_pps \
  bash scripts/run_full_corpus_weighted_treemaps.sh
```

The provisional Databricks outputs have the normal names with the suffix
`_pps_attention`. Compact renderer inputs are under:

```text
dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1/treemap_publication_pps_attention/
```

Local artifacts are under:

```text
outputs/full_corpus_dual_sample_20260717_v1/weighted_treemaps_pps_attention/
```

### Recorded runs and QA

- Allocation run `349093672850062`, task `901662360163990`: success.
- Estimation/QA run `73068724267084`, tasks `130518322688976` and
  `452921582411946`: success.
- Publication run `125352890555982`, task `744965842703036`: success.
- Analysis rows: 5,890,700; publication leaf cells: 18,389.
- Maximum channel-allocation error: `2.220446049250313e-16`.
- Maximum display-conservation error: `3.3306690738754696e-16`.
- Maximum topic-margin relative error: `1.0802478003587982e-15`.
- Maximum global-share error: `4.773959005888173e-15`.
- Maximum within-language error: `8.881784197001252e-16`.
- Maximum within-language-family error: `5.551115123125783e-16`.
- Unsampled positive margins, positive margins without support, and negative
  geometry rows: all zero.

The accepted 250-cell-budget static attention master has 217
language/family/subtopic cells, 0.307% minimum ordinary cell area, 0.382%
pooled view share, 83 labels, squarified packing, and 3000x1980-pixel
dimensions. The 0.3% leaf floor is binding before the 250-cell ceiling; the
renderer does not create smaller cells merely to exhaust the budget. Manual
inspection confirmed readable language, family, and subtopic regions with no
stack of thin slivers.

## Completed Paired Execution (2026-07-21)

The SRS/remainder fallback resumed without rerunning dual LID. Parent run
`1053350556385729` completed fallback task `1047774091941632` and publication
task `795902120766011`. All 245,925 routed requests received a successful API
response: DeepSeek classified 171,443 and returned an insufficient-language
verdict for 74,482. The published remainder table has 5,882,056 unique
channels, of which 5,511,908 are classified and 370,148 are `und`.

Combined-language run `851109646466619` published
`dev_sean.matt.yt_dual_sample_20260717_v1_channel_language_current` with
6,882,200 rows and distinct channel IDs. It contains 1,000,144 PPS rows and
5,882,056 nonoverlapping remainder rows; 6,462,938 channels are classified and
419,262 are `und`. Source-universe QA found zero missing or unexpected IDs.

Final analysis parent run `1056163018389473` completed allocation
(`388889144014481`), estimation (`494047300881882`), QA
(`175462060657441`), and treemap publication (`824909984690967`). Recorded
acceptance values are:

```text
ANALYSIS / LANGUAGE ROWS: 6,882,200 / 6,882,200
LANGUAGE JOIN MISSING / NULL: 0 / 0
SRS REALIZED N: 1,000,000
PPS REALIZED N: 1,000,144
PPS HT TAIL VIEWS: 670,248,334,181.7035
KNOWN TAIL VIEWS: 670,270,625,558
GLOBAL PPS RATIO DIAGNOSTIC: 1.0000332584
TREEMAP CELLS / UNIQUE CELLS: 20,233 / 20,233
NEGATIVE GEOMETRY ROWS: 0
POSITIVE TOPIC MARGINS WITHOUT SUPPORT: 0
MAX CHANNEL ALLOCATION ERROR: 2.220446049250313e-16
MAX DISPLAY SHARE ERROR: 6.661338147750939e-16
MAX GLOBAL SHARE ERROR: 2.375877272697835e-14
MAX TOPIC MARGIN ERROR: 3.1771103993192166e-15
TREEMAP PUBLICATION: PASS
TREEMAP CONSERVATION: PASS
```

Equal-channel geometry also passed an explicit stratum calibration. The known
122,126,394-channel denominator is decomposed as 4,888,355 >=10K census
channels (4.0027015%), 117,235,838 SRS-represented below-10K channels
(95.9954963%), and 2,201 subscriber-unknown certainty channels (0.0018022%).
The maximum observed-versus-expected share discrepancy was `2.34e-14`;
denominator error was zero. This is the required downweighting of the
enumerated census relative to the much larger channel tail.

The frozen 0.3% render remains the formal default. It produced 216 attention
cells (minimum ordinary area 0.309%, pooled view share 0.425%, 82 labels) and
199 equal-channel cells (minimum ordinary area 0.301%, pooled channel share
0.223%, 75 labels). Both are 3000x1980 pixels and use squarified packing.

The requested 0.1% minimum-area sensitivity was also rendered, without changing
the frozen config or estimators. It is the preferred detail comparison:

```text
ATTENTION STATIC CELLS: 227
ATTENTION MIN CELL AREA: 0.131%
ATTENTION POOLED VIEW SHARE: 0.425%
ATTENTION LABELED CELLS: 84
CHANNEL STATIC CELLS: 224
CHANNEL MIN CELL AREA: 0.109%
CHANNEL POOLED CHANNEL SHARE: 0.223%
CHANNEL LABELED CELLS: 73
FIGURE DIMENSIONS: 3000x1980
PACKING: squarify
CONSERVATION: PASS
EQUAL-CHANNEL STRATUM CALIBRATION: PASS
```

Both 0.1% PNGs were opened and manually inspected. **Legibility verdict:
PASS.** Language blocks and family/subtopic tiles are individually readable,
and no region is a stack of thin slivers. The lone family tile with an aspect
ratio above 8:1 is the isolated `Russian > Sports` tile (0.0436% of total
area), not a repeated sliver region. The large equal-channel
`Undetermined/Unlabeled` region is a substantive data result rather than a
layout failure.

The compact publication export is:

```text
dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1/treemap_publication/
```

The expansion summary and coefficient-style absolute/proportional change
figures are written with the paired treemaps under the local output directory.
The below-10K PPS expansion contributes 10.9257% of final view mass. The
largest family-level changes relative to the non-tail baseline are Music
(`+1.035` percentage points), Gaming (`+0.308`), and Entertainment
(`-1.485`).

## Rendered Artifacts

The local renderer writes parallel artifacts under
`outputs/full_corpus_dual_sample_20260717_v1/weighted_treemaps/`:

```text
treemap_static_master_attention_full_frame_weighted_v1.png
treemap_static_master_attention_full_frame_weighted_v1.svg
treemap_interactive_attention_full_frame_weighted_v1.html
treemap_static_master_channels_full_frame_weighted_v1.png
treemap_static_master_channels_full_frame_weighted_v1.svg
treemap_interactive_channels_full_frame_weighted_v1.html
weighting_difference_coefficients_full_frame_weighted_v1.png
weighting_difference_coefficients_full_frame_weighted_v1.svg
weighting_difference_coefficients_full_frame_weighted_v1.csv
weighting_difference_summary_full_frame_weighted_v1.json
artifact_manifest_full_frame_weighted_v1.json
treemap_static_master_attention_full_frame_weighted_min01_v1.png
treemap_static_master_attention_full_frame_weighted_min01_v1.svg
treemap_interactive_attention_full_frame_weighted_min01_v1.html
treemap_static_master_channels_full_frame_weighted_min01_v1.png
treemap_static_master_channels_full_frame_weighted_min01_v1.svg
treemap_interactive_channels_full_frame_weighted_min01_v1.html
```

The static master is language -> family -> subtopic, matching the accepted
full-corpus treemap. It uses the accepted v3 family palette and family-consistent
subtopic shades, white negative space instead of gray borders, squarified
packing, top-12 language pooling, family rescue rules, a 0.3% ordinary leaf
floor, and a hard 250-cell cap.
The interactive HTML retains the complete unpooled subtopic hierarchy for
drill-down.

The coefficient plot uses raw `view_share - channel_share`, not calibrated
geometry. Its interval treats the separate SRS and PPS sampling components as
independent. A joint replicate calculation remains necessary for final
measurement-error uncertainty.

## Acceptance Gates

The Databricks publication stage must print and satisfy:

```text
TREEMAP PUBLICATION: PASS
TREEMAP CONSERVATION: PASS
unsampled_positive_topic_margin_rows = 0
positive_topic_margins_without_sample_support = 0
negative_geometry_rows = 0
max_platform_topic_margin_relative_error <= 1e-6
max_global_share_conservation_error <= 1e-6
max_within_language_conservation_error <= 1e-6
max_within_language_family_conservation_error <= 1e-6
```

The local renderer must print:

```text
CONSERVATION: PASS
STATIC CELLS: <= 250
MIN CELL AREA: >= 0.300% for ordinary structural cells
PACKING: squarify
FIGURE DIMENSIONS: at least 2000x1200
INTERACTIVE: go.Treemap branchvalues=total maxdepth=2 packing=squarify sort=True
```

After rendering, open both static PNGs. Confirm that every language header is
readable, family tiles are distinguishable, and no region is a stack of thin
slivers. If either figure fails, raise pooling or reduce top-K and rerender;
never change estimates or drop residual mass to repair appearance.

## Interpretation Rules

- Rectangle area is a calibrated descriptive estimate, not a raw unbiased
  estimate.
- Hover and coefficient intervals are based on raw design-weighted estimates.
- A large rectangle does not imply adequate precision; use `headline_reliable`,
  effective contributing `n`, relative interval width, and largest weighted
  contribution.
- A zero estimate from an unobserved side of the SRS/PPS support union is kept
  and marked unreliable. It is never silently omitted.
- `V_i` is accepted nonnegative four-week view growth. The figure is not a
  signed net-change decomposition and not lifetime views.
- Equal-channel and view-weighted treemaps answer different questions. Their
  difference is the substantive weighting result, not an error in either
  estimator.
