# 122-Million-Channel Collected-Frame Dual-Sample Design

**Status:** DRAFT design protocol, 2026-07-17
**Target population:** Channels in the frozen 122-million-row collected frame
**Census stratum:** Channels with at least 10,000 subscribers at the frame date
**Probability-sampled stratum:** Channels below 10,000 subscribers at the frame date
**Base samples:** 1,000,000-channel SRS and target-1,000,000-channel PPS
**Primary uses:** Equal-channel, view-weighted, and language/topic intersection estimates

This document specifies a coordinated dual-sample design for extending the
existing `>=10K` YouTube channel census into the long tail. It is intended to
support a publication-quality comparison between:

1. the distribution of channels, where each population channel counts equally;
   and
2. the distribution of recorded views, where channels contribute in proportion
   to their recent view-count growth.

These are different estimands. Equal channel weighting is not intrinsically
biased when the question concerns creators or channels. It becomes a distorted
proxy when interpreted as the distribution of attention. The design estimates
both quantities directly and measures their difference with design-based
uncertainty.

This is not yet a registered or frozen protocol. Freeze the dates, frame,
sampling seeds, probability formulas, traffic treatment, taxonomy versions,
and model versions before drawing the samples. Do not tune those choices after
examining language or topic estimates.

## 1. Executive Design

Use three complementary components:

| Component | Approximate size | Inclusion probability | Primary role |
|---|---:|---:|---|
| `>=10K` census | 4.9 million | `1` | Complete collected-frame head stratum |
| Below-10K SRS | 1,000,000 | Constant within the tail | Channel/creator distribution |
| Below-10K PPS | Target 1,000,000 | Increases with prior four-week views | Recorded-view distribution |

Channels with hidden or otherwise unavailable subscriber counts cannot be
assigned to either subscriber stratum. Because the current aligned frame has
only about 2,200 such rows, include them as a separate certainty stratum and
always report them separately. The primary known-subscriber estimand excludes
this stratum; the all-retrievable-frame sensitivity adds it back with weight
one.

The SRS and PPS draws use independent, versioned random seeds. The complete
analysis union is the `>=10K` census, the subscriber-unknown certainty stratum,
and every channel selected by either tail design. Every row in this analysis
union receives the same language and topic processing. The tail-sample union
will contain slightly fewer than two million distinct channels because some
channels will enter through both designs.

The current language silver does not necessarily cover every channel in the
frozen `>=10K` census because its source crawl and snapshot cohort differ from
the proposed frame. Census channels lacking a label from the frozen language
version must be enriched through the same LID and fallback workflow as newly
sampled tail channels. Census status means `pi=1`; it does not mean that every
required analysis variable is already complete.

No rare-language supplement is part of the base design. The one-million draws
are deliberately large enough to characterize major long-tail domains without
requiring ex ante language expectations. Cells that remain underpowered are
pooled, suppressed, or described as exploratory rather than supplemented ad
hoc.

### 1.1 Two-phase interpretation

This is a two-phase measurement design:

1. **Phase one is the frozen frame census.** For every frame row it records
   subscriber status, current lifetime views, prior-endpoint availability,
   delta status, recent positive view growth when measurable, and platform-topic
   availability.
2. **Phase two is enrichment.** The census head and sampled tail channels are
   assigned language and semantic-topic measurements that are unavailable for
   most of the frame.

The PPS design therefore uses a phase-one auxiliary variable already observed
before enrichment. It is not selecting on an unobserved language or topic
outcome. For a cell allocation `a_ig` bounded in `[0,1]`, the unknown target
contribution is `V_i * a_ig`, where `V_i` is accepted phase-one positive view
mass. Sampling in proportion to known `V_i` is a natural near-optimal proxy
across many cells because `V_i` bounds the magnitude of that contribution. The
uniform component protects channels with zero,
negative, missing, or otherwise unusable deltas.

Phase one does not eliminate all outcome missingness: some frame channels lack
a usable endpoint pair. It makes that missingness known before sampling and
allows it to be retained as an explicit frame domain. The positive-view
denominator is exact for the declared valid-endpoint estimand; it is not an
imputation of unobserved view growth.

## 2. Current Empirical Basis

The most recent aligned analysis used the following snapshots from
`dev_sean.default.yt_channel_stats_full`:

- frame and threshold date `t0`: 2026-06-15;
- outcome endpoint `t1`: 2026-07-13; and
- elapsed interval: 28 days.

After deterministic endpoint deduplication, the observed `t0` frame contained:

| Domain | Channels |
|---|---:|
| Known subscriber count | 122,124,193 |
| Below 10,000 subscribers | 117,235,838 |
| At least 10,000 subscribers | 4,888,355 |
| Subscriber count unknown | 2,201 |
| Total frame | 122,126,394 |

In the aligned positive-view analysis, below-10K channels supplied about 670.27
billion of 6.135 trillion accepted positive four-week views, or 10.93%. They
were 96.00% of channels with known subscriber counts. The long tail therefore
matters far more for the creator ecology than for aggregate recorded view mass,
but it can still change particular languages, topic families, and subtopics.

The existing 2,000-channel pilot is not a substitute for the design specified
here. It used equal counts across subscriber bands and did not retain the frame
probabilities needed for population estimation. It also demonstrated the
importance of a better view design: its calibrated view-weight effective
sample size was only 24.7, and one channel contributed 12.36% of the estimated
below-10K view composition.

## 3. Target Population And Interpretation

The target is the frozen set of retrievable channels in this project's
122-million-row collected frame, not every channel ever created on YouTube.
There is no completed 150-million-channel comparison corpus and no known list
of approximately 28 million omitted channel IDs. Collection stopped before
that nominal scale because marginal discovery had fallen sharply and the final
work was yielding no additional channels above the subscriber threshold. It
would therefore be incorrect to invent an absent-ID sampling frame or treat
the numerical difference from 150 million as measured undercoverage.

Before freezing the analysis, preserve the discovery stopping-rule record:
source and traversal method, batches attempted, channels examined and added per
batch, subscriber-status coverage, yield of new `>=10K` channels, terminal
zero-yield runs, and the operational decision to stop. This evidence supports
the claim that discovery of additional above-threshold channels had saturated
under the implemented procedure. Because the discovery process was not a
probability sample of all possible YouTube channels, it does not establish
design-based coverage of the undiscovered long tail. Scope all estimates to the
frozen collected frame and discuss residual frame coverage as a limitation,
not as sampling variance.

The analysis describes channels and public channel view counters. It does not
observe viewer identities, unique viewers, impressions, watch time, viewer
language, or viewer geography. Language and topic results are ecological
descriptions of channels associated with recorded view-count growth.

A channel-counter difference includes growth on new uploads and back-catalog
videos, along with any public counter revisions. It measures which kinds of
channels accrued reported counter growth during the interval. It does not
identify which individual videos were watched during that month or attribute
the growth to newly published content.

Freeze threshold membership at `t0`. A channel that crosses 10,000 subscribers
during the 28-day interval remains in its `t0` stratum for that interval.

## 4. Observable Channel Values

For channel `i`, let:

```text
C_i0 = reported lifetime channel views at t0
C_i1 = reported lifetime channel views at t1
R_i  = C_i1 - C_i0
A_i  = R_i / 4
E_i  = 1 when both endpoints pass the frozen validity rule, else 0
Y_i  = max(R_i, 0) when E_i = 1, otherwise missing
V_i  = E_i * max(R_i, 0), with V_i = 0 when E_i = 0
m_i  = V_i / 4
```

`R_i` is the raw signed four-week change, and `A_i` is the recorded average net
change per week. `Y_i` remains missing when an endpoint is unusable. `V_i` is
the channel's contribution to the declared valid-endpoint positive-view
estimand; setting that contribution to zero when `E_i=0` is an accounting rule,
not an assertion that the channel had zero actual growth. Dividing by four does
not change PPS relative probabilities, but the weekly scale is easier to
interpret.

Persist all of the following rather than overwriting one with another:

```text
raw_4wk_net_views_i          = R_i when E_i = 1, otherwise null
avg_net_views_week_i         = A_i when E_i = 1, otherwise null
positive_4wk_views_i         = max(R_i, 0) when E_i = 1, otherwise null
positive_avg_views_week_i    = max(A_i, 0) when E_i = 1, otherwise null
accepted_positive_view_mass_i = coalesce(positive_4wk_views_i, 0)
pps_size_i                    = accepted_positive_view_mass_i / 4
has_prior_snapshot_i
has_current_snapshot_i
has_valid_endpoint_pair_i
has_positive_delta_i
has_zero_delta_i
has_negative_delta_i
```

Populate the two positive-view fields only when `E_i=1`. A missing endpoint is
not a measured zero-view month. It contributes no mass to the primary
valid-endpoint view estimand, remains a reported frame domain, and receives the
PPS uniform-floor probability through `m_i=0`. The primary view estimand is the
distribution of accepted positive four-week counter growth among frame
channels with a valid endpoint pair.

Negative changes are meaningful records of counter revisions or removals. They
cannot be used as a PPS size measure and cannot be rendered as treemap area.
The primary positive-area treemap uses `positive_4wk_views_i`. A signed-net
sensitivity analysis retains `R_i` and uses a visualization that supports
negative quantities.

Channels missing either endpoint, with zero changes, or with negative changes
remain eligible for both samples. In the PPS design they enter through the
uniform probability floor.

## 5. Core Estimands

Let `a_ig` be channel `i`'s allocation to analytic cell `g`. A cell may be a
language, topic family, family-leaf pair, language-family pair, or
language-family-leaf intersection. The allocation system must satisfy:

```text
a_ig >= 0
sum_g(a_ig) = 1 for a complete mutually exclusive partition
```

Language is one-hot, including `Undetermined`. Topics may be fractional under
the existing family-balanced allocation. `Unlabeled` and `Other / Unmapped`
are explicit cells so that no channel mass disappears.

### 5.1 Equal-channel distribution

For population size `N` and cell `g`:

```text
P_g = sum_i(a_ig) / N
```

`P_g` answers: What share of channels belongs to or is allocated to this cell?

### 5.2 View-weighted distribution

For accepted nonnegative analysis view mass `V_i`:

```text
Q_g = sum_i(V_i * a_ig) / sum_i(V_i)
```

`Q_g` answers: What share of accepted recorded view growth is allocated to this
cell?

### 5.3 Weighting distortion

The primary contrast is:

```text
D_g = Q_g - P_g
```

For a channel drawn uniformly from the population, the same difference can be
written as:

```text
D_g = Cov(V_i, a_ig) / E(V_i)
```

The identity supplies the substantive interpretation. A cell gains area in the
view-weighted distribution when its channels systematically receive more views
than the average channel. Report both percentage-point differences and the
ratio `Q_g / P_g` where `P_g` is not too small. Because `V_i=0` is an accounting
extension for unusable endpoint pairs, also report an endpoint-balanced
sensitivity `D_g^E = Q_g - P_g^E`, where `P_g^E` is the equal-channel share
restricted to `E_i=1`. This separates the effect of view weighting from any
association between classification and endpoint availability.

## 6. Frozen Frame Construction

Use `dev_sean.default.yt_channel_stats_full` and deduplicate each endpoint with:

```sql
ROW_NUMBER() OVER (
  PARTITION BY canonical_id, collected_date
  ORDER BY collected_at DESC,
           stable_tie_key ASC
)
```

Define `stable_tie_key` from a source ingest or row identifier when one exists.
Otherwise derive it from a cryptographic hash of a canonical serialization of
the raw source row. Do not
break an exact timestamp tie by preferring a larger view counter or subscriber
count, because those are analysis variables. If duplicate rows are identical
on all analytic fields, their tie order is immaterial.

The versioned frame must contain one row per `canonical_id` and at least:

```text
frame_version
frame_date
channel_id
channel_name
subscriber_count_t0
subscriber_status
current_lifetime_views
prior_lifetime_views
raw_4wk_net_views
avg_net_views_week
positive_4wk_views
positive_avg_views_week
accepted_positive_view_mass
pps_size
current_collected_at
prior_collected_at
delta_status
```

Classify frame rows as:

```text
census_ge10k
sample_frame_lt10k
subscriber_unknown_or_hidden
```

Do not draw from a changing table. Materialize the frame, record its Delta table
version or snapshot timestamp, and use that same materialization for both
samples and all denominator totals.

Before treating the approximately 2,200 subscriber-unknown rows as complete,
audit frame construction against raw channel responses and collection
dispositions. Distinguish explicitly hidden subscriber counts from nulls caused
by failed retrieval, parse errors, stale records, or upstream filtering. If
hidden-count channels were excluded before the weekly panel was written, treat
that as a frame-scope and coverage problem rather than placing only the
surviving null rows in the small certainty stratum.

### 6.1 Deterministic hash construction

Define one SHA-256 construction for both sample routes:

```text
payload = UTF8(channel_id || "\x1f" || frame_version || "\x1f" || seed)
digest  = SHA-256(payload)
srs_order_key = all 32 digest bytes, compared lexicographically as unsigned bytes
k       = unsigned integer represented by the first 8 digest bytes
u       = k / 2^64
```

Use `srs_order_key` for the exact fixed-size SRS ordering and `u` for the PPS
threshold. Implement the unsigned conversion explicitly. If the execution
engine cannot compare a 64-bit unsigned integer without lossy conversion, use
the first 53 bits divided by `2^53` and record that choice; do not silently cast
an unsigned value through a signed integer. Use distinct frozen seeds for SRS
and PPS. Break the negligible possibility of a hash collision with `channel_id`.
Record the byte encoding, separator, digest algorithm, selected digest bytes,
unsigned conversion, and floating or decimal conversion in code and table
properties. Test empirical uniformity, duplicate rates, and cross-seed
correlation before selection.

## 7. One-Million Equal-Probability Sample

### 7.1 Purpose

The SRS is optimized for channel prevalence and creator-ecology quantities. It
also supplies an intentionally inefficient but unbiased cross-check of the
view-weighted estimates.

### 7.2 Selection

Let:

```text
N_T = number of below-10K frame channels
n_S = 1,000,000
```

Assign an independent deterministic uniform random number:

```text
u_Si = hash_uniform(channel_id, frame_version, srs_seed)
```

Select the `n_S` smallest `(u_Si, channel_id)` values. This is an exact fixed-size
simple random sample without replacement if the hash behaves as a uniform
random permutation.

Every below-10K channel has:

```text
pi_Si = n_S / N_T
d_Si  = 1 / pi_Si = N_T / n_S
```

Store `u_Si`, the selection rank, `pi_Si`, and `d_Si`. Never reconstruct them
from row order.

### 7.3 Precision rationale

Ignoring the small finite-population correction, the worst-case 95% margin of
error for a simple proportion with one million completed channels is:

```text
1.96 * sqrt(0.5 * 0.5 / 1,000,000) = 0.00098
```

That is approximately 0.098 percentage points for a tail proportion. The
margin is about 0.059 percentage points for a 10% proportion. Classification
missingness, measurement error, and intersection-specific sample sizes remain
additional limitations.

## 8. Target-One-Million PPS Sample

### 8.1 Purpose

The PPS sample is optimized for the distribution of recent recorded views. It
must also retain a positive chance for zero-view, negative-change, and
missing-delta channels so that channel quantities and missingness remain
estimable.

### 8.2 Mixture size measure

Let:

```text
m_i   = pps_size_i
M     = sum_i(m_i) over the below-10K frame
alpha = 0.10
```

Define a normalized mixture size:

```text
q_i = alpha * (1 / N_T) + (1 - alpha) * (m_i / M)
```

The recommended `alpha=0.10` allocates approximately 10% of expected selections
through a uniform floor and 90% in proportion to positive weekly view growth.
The floor is essential because a pure view-PPS design would give zero
probability to most inactive or negatively revised channels.

Before selection, compare `alpha` values of 0.05, 0.10, and 0.20 against several
frozen pseudo-outcome sets. Use exact below-10K phase-one outcomes where they
exist, especially platform-topic families and leaves crossed with view and
subscriber bands. Use a reweighted bootstrap of the 2,000-channel pilot table
`dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_treemap_pilot_channel_base` for
language stress tests, while recognizing that its equal-band design and small
effective sample do not make it a population proxy. Use the `>=10K` census as
an implementation and high-concentration stress test, not as the only model of
tail behavior.

For each candidate report bias, RMSE, interval coverage, effective sample size,
weight dispersion, certainty-unit count, and maximum contribution for marginal
and intersection estimates. Freeze `alpha` before inspecting newly enriched
below-10K language or semantic-topic results. The default remains 0.10 unless
the tail-informed simulations demonstrate a material precision or
weight-stability advantage.

### 8.3 Inclusion probabilities

For target expected size `n_P = 1,000,000`, solve for scalar `c` such that:

```text
pi_Pi = min(1, c * q_i)
sum_i(pi_Pi) = n_P
```

Solve `c` by monotone binary search or an equivalent certainty-unit
water-filling algorithm. Channels with `c * q_i >= 1` are certainty selections.

The base PPS design weight is:

```text
d_Pi = 1 / pi_Pi
```

The design weight is not the inverse of views. Recent views determine the
inclusion probability; the inverse inclusion probability reconstructs the
population.

### 8.4 Poisson selection

Assign an independent deterministic uniform number:

```text
u_Pi = hash_uniform(channel_id, frame_version, pps_seed)
```

Select channel `i` when:

```text
u_Pi < pi_Pi
```

This gives an independent Poisson PPS sample with expected size exactly one
million. The realized size is random. Its standard deviation is at most about
1,000 rows, so a realized size near 998,000 to 1,002,000 is ordinary and must
not be rerolled.

Do not trim or top up the sample to force an exact row count. That changes the
design unless the second-stage probabilities are derived and stored. If an
exact one-million-row PPS sample is operationally mandatory, use a documented
conditional-Poisson/rejective design rather than an ad hoc adjustment.

### 8.5 Expected intersection coverage

With `alpha=0.10`, approximately 900,000 expected selections come from the
view-proportional component. Ignoring certainty capping and weight variation:

| Cell share of below-10K views | Expected PPS selections |
|---:|---:|
| 2.75% | 24,750 |
| 1.00% | 9,000 |
| 0.50% | 4,500 |
| 0.10% | 900 |
| 0.05% | 450 |

These are not effective sample sizes. Actual information is reduced by weight
dispersion, classification failures, and within-cell concentration. They show
why one million is appropriate for major language-family and selected subtopic
intersections without requiring a separate adaptive supplement.

### 8.6 Why PPS rather than fixed view-band stratification

A fixed-size SRS within frozen view bands, with allocation guided by each
band's view mass, is a valid alternative. It has familiar stratum estimators
and guarantees an exact total sample size. For a fair comparison, give the
zero, negative, and missing-delta strata the same expected uniform-floor budget
as PPS. Retain this design as the principal benchmark in simulation.

The proposed Poisson PPS design is preferred provisionally because it targets
the continuous, highly skewed size measure directly rather than introducing
arbitrary band boundaries. It also permits one distributed hash-threshold
selection over approximately 117 million rows, retains exact first-order
inclusion probabilities, and handles certainty channels without a separate
take-all rule. Calibration to the exactly known positive tail-view total and
other phase-one margins should absorb much of the efficiency loss associated
with random Poisson sample size.

This is a testable implementation choice, not a presumption. Simulate the
fixed-view-band alternative using the same expected enrichment budget and
outcomes. Switch designs before selection if it materially improves precision,
weight stability, or operational reliability across the registered headline
estimands.

## 9. Coordinating The Two Samples

Use independent SRS and PPS hashes. For every below-10K frame channel, persist:

```text
selected_srs
selected_pps
pi_srs
pi_pps
srs_seed
pps_seed
```

For independent invitations, the probability of appearing in the union is:

```text
pi_union_i = 1 - (1 - pi_Si) * (1 - pi_Pi)
d_union_i  = 1 / pi_union_i
```

Expected SRS-PPS overlap is approximately `n_S / N_T` of the PPS sample, or
roughly 8,500 channels under the current frame. Retain both selection routes on
overlap rows.

Use the samples as follows:

| Estimand | Primary source | Cross-check |
|---|---|---|
| Channel shares | SRS plus census | PPS with inverse-probability weights |
| View shares | PPS plus census | SRS HT estimate with known view denominator |
| Efficient combined estimates | Union with union weights/calibration | Compare with design-specific estimates |

The cross-checks are intentionally less efficient. Agreement within their
design-based intervals is evidence that sample construction and weighting are
working correctly.

## 10. Shared Language Measurement

Apply one versioned language pipeline to the complete analysis union: the
`>=10K` census, subscriber-unknown certainty rows, and the tail-sample union.

The reusable head lookup is
`dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_current`,
version `lid_v3_channel_crawl_full_20260623_deepseek_flash_20260715_v1`. It has
4,798,717 one-row-per-channel labels and uses `channel_language` as the primary
lowercase ISO 639-3 field, including `und`. Verify its exact join coverage
against the frozen 4.9-million-channel census; do not assume that the existing
lookup covers a later frame merely because its cohort is similar in size.

1. Reuse a current final LID silver label when the analysis-union channel
   already has a label from the required version.
2. Collect channel description and recent video titles/descriptions for
   analysis-union channels lacking adequate source text.
3. Run the established dual OpenLID-v3 and GlotLID pipeline.
4. Accept reliable model consensus under the frozen LID-v3 decision rules.
5. Route disagreement and insufficient-confidence cases with usable text to
   DeepSeek Flash fallback.
6. Retain `und` for channels with insufficient or no language evidence.
7. Preserve script, mixed-language status, model provenance, confidence, and
   source-run metadata separately from `channel_language`.

Do not force a language for channels without evidence. `Undetermined` is part
of the primary distribution. Report language-classification coverage by sample
route, subscriber band, view-size quantile, and topic-availability status.

### 10.1 Enrichment burden and version consistency

Before freezing the design, join the full frame to the exact frozen
language-label and source-text versions. Use census counts and the proposed
inclusion probabilities to estimate the enrichment burden by design route;
after selection, replace expected counts with realized counts. Report,
separately by route and threshold stratum:

```text
channels with an accepted current-version language label
channels with source text adequate for rerunning LID
channels requiring fresh text collection
channels expected to route to DeepSeek fallback
channels with no usable language evidence
channels with missing or empty platform-topic arrays
```

Convert these counts into expected collection, model, quota, and review burden.
If the feasible workflow cannot complete the registered analysis union, revise
the design before selection rather than making a budget-driven protocol change
after outcomes have been inspected.

Record source-text dates and label versions for reproducibility. Existing
project evidence indicates that language-label drift across the relevant text
vintages is minimal, so a dedicated census relabeling experiment is not part of
this protocol. Apply one frozen final-label interpretation across cohorts and
prioritize the larger validity threats: target-frame scope, endpoint coverage,
topic missingness, sparse text, and language/topic classification error. Reopen
the vintage question only if routine QA reveals a material version-specific
shift in `und`, script, mixed-language, or fallback rates.

## 11. Platform And Model-Completed Topics

Raw YouTube topic arrays are available broadly in
`dev_sean.default.channel_category`. The current 2026-06-11 snapshot has about
122.16 million rows and 86.71 million nonempty arrays, or 70.98%. Topic
missingness is therefore a major substantive feature of the channel-count
distribution, especially among small channels.

Produce three distinct topic analyses:

1. **Platform-observed primary:** Process the raw YouTube arrays through the
   existing hierarchy and retain empty arrays as `Unlabeled`.
2. **Platform complete-case diagnostic:** Restrict to nonempty arrays only and
   label this explicitly as a selected subset, not a population estimate.
3. **Model-completed robustness:** Use an LLM classifier to assign calibrated
   family and subtopic probabilities when platform topics are empty.

Do not silently replace `Unlabeled` in the primary platform-topic analysis.
Missingness itself demonstrates an important difference between the head and
long tail.

For the model-completed analysis, classify every platform-missing channel in
the complete analysis union and a probability validation subset of
platform-labeled channels from the census and both samples. If resources
permit, run the semantic classifier on the entire analysis union so that the
robustness analysis uses one measurement instrument rather than mixing
platform labels and model labels.

The LLM taxonomy must be frozen and match the analytic family/leaf hierarchy.
Require:

```text
family_probability >= 0
sum(family_probability) = 1
sum(leaf_probability within family) = family_probability
```

Include `Other`, `Insufficient evidence`, and `Unclassifiable`. Do not treat
self-reported LLM probabilities as calibrated without validation. Evaluate
macro-F1, Brier score, log loss, and calibration by language, subscriber band,
view band, text availability, and platform-topic missingness.

Use calibrated probabilities as fractional allocations. Propagate model
uncertainty with repeated draws or multiple imputation rather than treating
predicted probabilities as error-free.

## 12. Topic Projection And Conservation

Use the existing editable taxonomy assets:

- `config/youtube_topic_hierarchy_v2.yaml`;
- `config/topic_remap.yaml`;
- `config/language_normalization.yaml`; and
- `config/iso639_language_names.csv`.

For platform topics, apply the established rules:

1. choose the latest category row;
2. preserve the raw topic URL array;
3. normalize and deduplicate slugs;
4. suppress a parent when a child in the same family is present;
5. map every retained slug to family and leaf;
6. allocate equally across families and then across leaves within family;
7. assign empty arrays to `Unlabeled`; and
8. retain unmapped labels explicitly.

For every channel and every analysis variant, assert:

```text
sum_family_weights = 1
sum_leaf_weights = 1
sum_language_family_leaf_weights = 1
```

Named-channel display placement may be used for a visual sensitivity but must
not alter the primary statistical allocation.

## 13. Estimation With The Census And SRS

Let `H` denote the `>=10K` census, `U` the subscriber-unknown certainty
stratum, and `T` the below-10K population. Let `s_S` denote the SRS.

The equal-channel cell total is estimated by:

```text
N_hat_g = sum_{i in H}(a_ig)
          + sum_{i in s_S}(d_Si * a_ig)
```

This is the primary known-subscriber total. The all-retrievable-frame
sensitivity adds the exact certainty contribution `sum_{i in U}(a_ig)`.

Because each target population's total channel count is known, the shares are:

```text
P_hat_known_g = N_hat_g / (N_H + N_T)

P_hat_all_g =
  [N_hat_g + sum_{i in U}(a_ig)] / (N_H + N_T + N_U)
```

This estimator is design-unbiased for the channel share when classification
and response are complete or correctly adjusted.

For an SRS tail total `T_hat_g`, the standard finite-population variance is:

```text
Var(T_hat_g) = N_T^2 * (1 - n_S / N_T) * s_g^2 / n_S
```

where `s_g^2` is the sample variance of `a_ig`. Divide by the squared known
full channel count to obtain share variance. Use domain and replicate methods
when classification weights or calibration make the simple formula
insufficient.

## 14. Estimation With The Census And PPS

Let `s_P` denote the PPS sample and `V_i` the accepted positive treemap view
mass defined in Section 4.
The primary known-subscriber Horvitz-Thompson cell view total is:

```text
V_hat_g = sum_{i in H}(V_i * a_ig)
          + sum_{i in s_P}(d_Pi * V_i * a_ig)
```

The all-retrievable-frame sensitivity adds the exact certainty contribution
`sum_{i in U}(V_i * a_ig)`. The corresponding known-subscriber and all-frame
view denominators are known from the channel panel. Therefore:

```text
Q_hat_known_g = V_hat_g / V_known

Q_hat_all_g =
  [V_hat_g + sum_{i in U}(V_i * a_ig)] / V_all
```

With fixed known denominators, `Q_hat_known_g` and `Q_hat_all_g` are
design-unbiased for their respective cell view shares under Poisson PPS. The
estimated cell shares need not sum to exactly one in a realized sample because
the sampled Horvitz-Thompson tail view total fluctuates around its known total.

For coherent treemap rendering, calibrate the tail weights to the known tail
view total and known full-frame platform-topic margins. Render calibrated
totals, while retaining raw Horvitz-Thompson estimates as the primary
design-based benchmark. Calibration produces coherent, lower-variance display
shares but is not literally finite-sample unbiased in the same sense as the raw
cell total.

Under independent Poisson selection, the raw cell-total variance estimator is:

```text
Var_hat(V_hat_g) =
  sum_{i in s_P} (1 - pi_Pi) * (V_i * a_ig)^2 / pi_Pi^2
```

Divide by `V_known^2` or `V_all^2`, as appropriate, for the view-share variance.

## 15. Conditional And Intersection Estimates

A conditional quantity such as topic family `f` within language `l` is a ratio
of estimated totals:

```text
Q_hat(f | l) = V_hat_(l,f) / V_hat_l
```

Its denominator is not known for the full frame because language is sampled.
Use Taylor linearization or replicate weights. For a ratio `R_hat = X_hat /
Z_hat`, define the estimated linearized contribution:

```text
e_i = x_i - R_hat * z_i
```

Estimate the design variance of the weighted total of `e_i`, then divide by
`Z_hat^2`. Reproduce census, SRS, PPS, calibration, nonresponse adjustment, and
classification imputation in every replicate.

Do not infer precision from the nominal sample size. For every published
intersection report:

```text
sampled contributing channels
effective sample size
estimated channel or view mass
standard error
95% interval
coefficient of variation
largest weighted channel contribution
classification coverage
```

## 16. Estimating The Difference Between Weighting Regimes

Estimate:

```text
D_hat_g = Q_hat_g - P_hat_g
```

With independent SRS and PPS draws and exact census totals, the sampling
variance is approximately:

```text
Var(D_hat_g) = Var(Q_hat_g) + Var(P_hat_g)
```

Shared classification models and overlapping selected channels can create
additional covariance in the measurement-error component. A joint replicate
analysis over the union is the preferred final calculation.

Report:

- `P_hat_g` and its interval;
- `Q_hat_g` and its interval;
- `D_hat_g` in percentage points;
- endpoint-balanced `D_hat_g^E` as a missing-endpoint sensitivity;
- `Q_hat_g / P_hat_g` where stable;
- total-variation distance at language, family, and leaf levels; and
- rank changes between channel and view distributions.

The treemaps are descriptive summaries. Coefficient plots of `D_hat_g` with
intervals are the primary inferential display.

## 17. Calibration To Known Full-Frame Margins

The frame contains useful quantities known for every channel. Use them to
reduce variance and diagnose sample balance:

```text
full channel count
subscriber-band channel counts
positive/zero/negative/missing delta counts
positive-view totals by subscriber band
platform-topic availability counts
platform-topic family and leaf totals
```

Start from base weights `1/pi`. Calibrate only to margins frozen before viewing
language or model-completed topic results. Keep calibration factors bounded and
report their distribution. Preserve both base and calibrated weights.

Platform-topic margins can be calculated directly for the full frame because
raw topic arrays already exist broadly. The platform-topic marginal treemap
does not need sample estimation; the samples are needed for language,
model-completed topics, and their intersections.

## 18. Nonresponse And Retrieval Failure

Persist a disposition for every analysis-union channel, including:

```text
already_enriched
collection_success
not_found
private_or_terminated
temporary_api_failure
missing_prior_metrics
insufficient_language_text
language_undetermined
topic_row_missing
topic_array_empty
llm_insufficient_evidence
```

Do not replace selected failures with convenient channels. Retry under a
versioned protocol. If nonresponse remains, adjust within cells defined only by
full-frame auxiliary variables, and report unadjusted estimates plus bounds or
sensitivity analyses. A forced language or topic is not a response adjustment.

## 19. Classification And Measurement Uncertainty

Sampling uncertainty is only one error source. The final analysis must also
address:

- LID error and `und` rates;
- mixed-language channels;
- platform-topic missingness;
- LLM topic misclassification and probability calibration;
- missing or sparse source text;
- view-counter revisions; and
- residual coverage error from the nonprobability channel-discovery process.

Use a stratified independent human-validation sample spanning sample route,
subscriber band, view-size quantile, language, script, platform-topic status,
and text availability. Keep model outputs hidden until human labels are
recorded. Report measurement performance by channel count and by view mass.

Size validation from registered accuracy targets rather than an arbitrary row
count. For a simple proportion with anticipated value `p`, two-sided confidence
level critical value `z`, and desired absolute half-width `e`, begin with:

```text
n0 = z^2 * p * (1 - p) / e^2
```

Use `p=0.5` when no defensible prior value exists, then inflate for stratified
design effects, expected unresolved cases, dual-review disagreement, and
nonresponse. Register separate tolerances for overall accuracy/calibration and
for major language, script, subscriber, view, and topic-missingness domains.
Allocate a probability validation base for population-level performance plus
targeted oversamples for rare or high-risk domains, retaining validation
inclusion probabilities so oversampled cases do not distort aggregate error
rates. Specify adjudication, reviewer blinding, and the minimum completed count
per reported domain before model evaluation begins.

For the model-completed topic analysis, propagate classification uncertainty
with multiple imputation or a model bootstrap. Combine sampling and
classification variance rather than presenting survey intervals that treat
predicted labels as truth.

## 20. Temporal Design

The retrospective 2026-06-15 to 2026-07-13 distribution is the primary
descriptive estimate for that completed interval. It is valid when inclusion
probabilities are known even though the PPS size measure comes from the same
period's phase-one view record.

For any confirmatory claim about stable attention structure, freeze the PPS
sample from period-A views and make a subsequent period-B view distribution the
primary confirmatory estimate. Apply the frozen population definition,
classifiers, taxonomy, allocation rules, weights, and reliability gates without
retuning. Retain the same channels for additional 28-day windows and report:

- same-period retrospective estimates;
- subsequent-period estimates;
- repeated-window pooled estimates;
- entry, exit, and attrition;
- view-rank churn and burstiness; and
- a fixed-composition panel sensitivity.

This separates stable category differences from one-month viral events.

## 21. Treemap Construction

Produce parallel artifacts with identical taxonomy, ordering, family colors,
and pruning rules:

1. **Channel ecology:** Area from the SRS/census equal-channel estimates.
2. **Attention ecology:** Area from the PPS/census positive-view estimates.
3. **Weighting distortion:** A signed difference or ratio visualization showing
   where channel weighting overstates or understates view share.

The static treemaps remain language -> family only. Subtopics and channels live
in the interactive explorer or detail figures. Apply pooling only after
estimating the complete hierarchy. Never drop `Undetermined`, `Unlabeled`, or
`Other / Unmapped` mass to improve appearance.

Use calibrated estimates for geometrically coherent display. Attach raw
design-based estimates and uncertainty in the accompanying tables. A cell must
not appear inferentially certain merely because its rectangle is visually
large.

## 22. Reliability Rules

Do not assume every cell is adequately measured because each base sample is
large. Before publication, define and apply cell-level gates based on:

```text
effective contributing n
absolute 95% half-width
relative 95% half-width
coefficient of variation
largest weighted contribution
language and topic coverage
model-completion burden
```

Recommended starting diagnostics, to be frozen after design simulation, are:

- effective contributing `n >= 400` for a headline intersection;
- relative 95% half-width no larger than 15%;
- largest weighted channel contribution no larger than 10%; and
- classification coverage meeting the registered measurement gate.

Cells failing a gate remain in machine-readable outputs but are pooled,
suppressed, or marked exploratory in publication figures. Do not expand the
sample selectively after learning which substantive result would change.

## 23. Required Versioned Tables

Recommended Delta outputs under `dev_sean.matt` are:

```text
yt_dual_sample_20260717_v1_frame
yt_dual_sample_20260717_v1_frame_summary
yt_dual_sample_20260717_v1_frame_scope
yt_dual_sample_20260717_v1_srs
yt_dual_sample_20260717_v1_pps
yt_dual_sample_20260717_v1_union
yt_dual_sample_20260717_v1_design_simulation
yt_dual_sample_20260717_v1_repeated_simulation
yt_dual_sample_20260717_v1_dispositions
yt_dual_sample_20260717_v1_language
yt_dual_sample_20260717_v1_platform_topics
yt_dual_sample_20260717_v1_model_topics
yt_dual_sample_20260717_v1_topic_human_validation
yt_dual_sample_20260717_v1_topic_model_calibrated
yt_dual_sample_20260717_v1_topic_calibration_summary
yt_dual_sample_20260717_v1_validation
yt_dual_sample_20260717_v1_allocations
yt_dual_sample_20260717_v1_estimates
yt_dual_sample_20260717_v1_weighting_differences
yt_dual_sample_20260717_v1_qa
```

Every sample row must retain frame version, sample version, design route,
random seed, random uniform, inclusion probability, base weight, calibration
factor, final analysis weight, and collection disposition.

## 24. Databricks Execution Environment

Follow `youtube_descriptive/src/AGENT_DATA_CONTEXT.md`.

```bash
env DATABRICKS_AUTH_STORAGE=plaintext databricks \
  -p matt.hindman@researchaccelerator.org ...
```

- Host: `https://adb-1335559103600339.19.azuredatabricks.net`
- Existing cluster: `matt-research-gencompute`
- Cluster ID: `0601-203643-bkxsqffg`
- SQL warehouse for bounded probes: `86100da4e1fe8713`
- Output namespace: `dev_sean.matt`

Use the SQL warehouse for metadata and bounded validation. Use only the existing
all-purpose cluster for the 122-million-row frame build and sampling jobs. Do
not create or restart other compute unless explicitly authorized.

## 25. Frame And Sample Acceptance Checks

Print and persist at least:

```text
FRAME VERSION
FRAME ROWS
DISTINCT FRAME CHANNELS
DISCOVERY STOPPING-RULE RECORD
HIDDEN-SUBSCRIBER AUDIT
T0 DATE
T1 DATE
KNOWN / BELOW10K / GE10K / UNKNOWN COUNTS
VALID ENDPOINT PAIRS
MISSING / POSITIVE / ZERO / NEGATIVE DELTA COUNTS
POSITIVE VIEW TOTAL BY STRATUM
CENSUS LANGUAGE LABEL GAP
HASH UNIFORMITY / CROSS-SEED CORRELATION
ALPHA SIMULATION SOURCES AND RESULT
SRS TARGET N
SRS SUM PI
PPS EXPECTED N
PPS SUM PI
PPS CERTAINTY CHANNELS
MIN AND MAX NONCERTAINTY PI
EXPECTED SRS-PPS OVERLAP
```

Required gates:

- frame rows equal distinct channel IDs;
- the discovery stopping rule, terminal high-subscriber yield, and target-frame
  scope are documented without asserting a nonexistent 150-million-ID corpus;
- hidden and unknown subscriber statuses reconcile to documented source
  dispositions, with any upstream exclusions handled as a frame-scope and
  coverage limitation;
- every frame channel is assigned exactly one threshold status;
- valid endpoint rules reconcile with the positive-view denominator and no
  missing endpoint is silently treated as observed zero growth;
- the exact census burden and expected tail enrichment burden have been
  budgeted and reconciled with the realized selected samples;
- empirical hash tests support uniformity and independence of the frozen seeds;
- PPS `alpha` and the PPS-versus-view-band choice are supported by the frozen
  tail-informed design simulation;
- SRS sample contains exactly 1,000,000 distinct below-10K IDs;
- PPS `sum(pi)` equals 1,000,000 within numerical tolerance;
- every PPS `pi` is in `(0, 1]`;
- every certainty PPS channel is selected;
- no selected ID is duplicated within a route;
- rerunning with the same frame and seeds reproduces the same samples; and
- every selected row retains its original probability and route.

## 26. Post-Enrichment Acceptance Checks

Print and persist:

```text
SRS / PPS / UNION ROWS
OVERLAP ROWS
COLLECTION DISPOSITION BY ROUTE
LANGUAGE CLASSIFIED / UND BY ROUTE
PLATFORM TOPIC ROW / NONEMPTY / EMPTY BY ROUTE
LLM TOPIC CLASSIFIED / INSUFFICIENT BY ROUTE
BASE AND CALIBRATED WEIGHT QUANTILES
WEIGHT CV AND EFFECTIVE N
MAX WEIGHTED CHANNEL CONTRIBUTION BY HEADLINE CELL
CHANNEL ALLOCATION CONSERVATION
VIEW ALLOCATION CONSERVATION
```

The analysis cannot be declared complete unless channel and view allocations
conserve the corresponding weighted totals and all failures remain visible in
the QA record.

## 27. Repeated-Sample Design Evaluation

Before headline analysis, run repeated-sample evaluation on multiple frozen
pseudo-populations. Use the existing `>=10K` census to test exact recovery and
high-concentration behavior. Use below-10K phase-one platform-topic outcomes to
test the actual tail view distribution. Use a clearly labeled reweighted or
semi-synthetic version of the 2,000-channel pilot only to stress-test language
patterns not observed frame-wide. Repeatedly draw SRS, PPS, and fixed-view-band
samples at the same expected enrichment budget and compare estimates with the
corresponding known pseudo-population quantities.

For each design and outcome, report:

```text
bias
empirical standard error
mean reported standard error
RMSE
95% interval coverage
design effect
effective sample size
weight dispersion
maximum influence
```

Use at least 5,000 replicates for the final design comparison. Five thousand
replicates give about 2% relative Monte Carlo error for a variance estimate and
about 1% for its standard error under approximately normal behavior.

No pseudo-population can prove performance for unobserved below-10K semantic
labels. Together, these evaluations test implementation, weighting, variance
formulas, tail concentration, the uniform PPS floor, and the choice between PPS
and view-band stratification before expensive enrichment begins.

## 28. Decision Register To Freeze

Before drawing either sample, record:

```text
frame table and Delta version
discovery stopping-rule record and target-frame scope
t0 and t1 definitions
threshold date and threshold rule
hidden- and unknown-subscriber treatment
endpoint validity and missing-delta estimand
SRS seed and exact target n
PPS seed and expected target n
exact hash construction and uniformity tests
PPS alpha and size measure
alpha simulation inputs and PPS-versus-strata decision
language pipeline and label version
language enrichment burden, source-text dates, and label versions
topic hierarchy and remap versions
LLM topic model, prompt, and probability schema
human-validation tolerances, allocation, and adjudication
calibration margins and weight bounds
nonresponse retry and adjustment rules
period-A descriptive and period-B confirmatory roles
cell reliability gates
treemap pruning rules
```

Any later change creates a new sample or analysis version. Do not silently
overwrite the registered design.

## 29. Final Rationale

The `>=10K` census captures about 89% of observed positive four-week view mass
but only about 4% of known-subscriber channels. Keeping it as a census avoids
discarding already classified, high-influence observations.

The one-million-channel SRS estimates the creator ecology with approximately
0.1 percentage-point worst-case marginal precision before classification error.
It supplies the correct baseline for statements about a typical channel and
reveals the prevalence of small, inactive, unlabeled, and low-text channels.

The target-one-million PPS concentrates collection on the long-tail channels
that contribute recorded views while preserving a uniform floor for the rest.
It provides much stronger precision for view-weighted language/topic cells and
selected intersections than an SRS of the same size.

Together, the census, SRS, and PPS make the weighting comparison an explicit
comparison of population estimands rather than an artifact of one convenience
sample. Shared measurement, exact inclusion probabilities, complete failure
tracking, model-completion sensitivity, and design-based uncertainty are what
make the resulting treemaps suitable for a high-scrutiny research article.

## Related Documents

- [Dual-sample implementation runbook](FULL_CORPUS_DUAL_SAMPLE_IMPLEMENTATION_20260717.md)
- [Full-corpus treemap runbook](TREEMAP_FULL_CORPUS_RUNBOOK.md)
- [Below-10K sensitivity analysis](BANDED_LT10K_FULL_CORPUS_SENSITIVITY_20260716.md)
- [Treemap weighting-bias comparison](TREEMAP_WEIGHTING_BIAS_20260716.md)
- [Treemap visualization specification](TREEMAP_V3_SPEC.md)
- [Agent data context](../youtube_descriptive/src/AGENT_DATA_CONTEXT.md)
