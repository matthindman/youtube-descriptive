# Full-Corpus Dual-Sample Implementation Runbook

**Design version:** `full_corpus_dual_sample_20260717_v1`
**Frame version:** `yt_dual_sample_20260717_v1`
**Design specification:** [FULL_CORPUS_DUAL_SAMPLE_DESIGN_20260717.md](FULL_CORPUS_DUAL_SAMPLE_DESIGN_20260717.md)
**Target population:** channels in the frozen 2026-06-15 collected frame
**Traffic endpoint:** 2026-07-13, 28 elapsed days
**Status:** source collection, enrichment restage, and cutoff selection complete; PPS production dual LID running

## 1. Scope

This implementation creates:

1. the complete collected-frame phase-one table;
2. analytic comparisons of three PPS floor values and a fixed view-band SRS;
3. an exact one-million-channel below-10K SRS;
4. a target-one-million independent Poisson PPS;
5. the distinct tail union plus the complete `>=10K` frame census;
6. explicit source-text, language, and topic dispositions;
7. a gated dual-LID plus DeepSeek Flash language workflow; and
8. a model-completed family/leaf probability workflow for platform-topic-missing channels.

The implementation does not claim that the collected frame is every YouTube
channel. There is no completed 150-million-channel comparison corpus. Discovery
stopped after marginal yield fell sharply and terminal work returned no new
above-threshold channels. The discovery batch record is retained as scope
evidence, not converted into a fictional absent-ID sampling frame.

## 2. Registered Environment

Use only:

```text
Profile: matt.hindman@researchaccelerator.org
Host: https://adb-1335559103600339.19.azuredatabricks.net
Existing cluster: matt-research-gencompute
Cluster ID: 0601-203643-bkxsqffg
SQL warehouse: 86100da4e1fe8713
Output namespace: dev_sean.matt
```

Every CLI command must include the explicit profile and plaintext token storage:

```bash
env DATABRICKS_AUTH_STORAGE=plaintext databricks \
  -p matt.hindman@researchaccelerator.org ...
```

Do not use a default profile, the obsolete Gmail profile, or newly created
compute. Jobs in this runbook contain `existing_cluster_id` and no
`new_cluster` definition.

## 3. Replication Files

| File | Purpose |
|---|---|
| `config/full_corpus_dual_sample_20260717_v1.json` | Frozen dates, sources, seeds, alpha, model versions, and compute IDs |
| `youtube_descriptive/src/full_corpus_dual_sample_design.py` | Pure SHA-256, probability, water-filling test, and stratified-allocation helpers |
| `youtube_descriptive/src/11_full_corpus_dual_sample_databricks.py` | Frame, simulation, sample selection, and enrichment staging |
| `youtube_descriptive/src/12_full_corpus_dual_sample_language_databricks.py` | Language preflight, base-ISO routing, and final label publication |
| `youtube_descriptive/src/13_full_corpus_dual_sample_topic_model_databricks.py` | Missing-topic requests, DeepSeek probabilities, strict parse validation |
| `youtube_descriptive/src/14_full_corpus_dual_sample_analysis_databricks.py` | Topic allocations, SRS/PPS estimates and SEs, exact platform-topic calibration margins, publication cells, and conservation QA |
| `youtube_descriptive/src/15_full_corpus_dual_sample_repeated_simulation_databricks.py` | Frozen head/tail pseudo-populations and 5,000-replicate empirical design checks |
| `youtube_descriptive/src/16_full_corpus_dual_sample_collection_databricks.py` | Resumable YouTube Data API description/recent-video collection with complete dispositions |
| `youtube_descriptive/src/17_full_corpus_dual_sample_lid_cutoff_experiment_databricks.py` | Paired 100,000-PPS-channel experiment selecting the recent-video LID cap |
| `youtube_descriptive/src/17_full_corpus_dual_sample_topic_calibration_databricks.py` | Human-validation-gated weighted temperature calibration and QA |
| `scripts/run_full_corpus_dual_sample.sh` | Upload and run phase-one/sample stages |
| `scripts/run_full_corpus_dual_sample_language.sh` | Upload and run gated dual-LID/DeepSeek stages |
| `scripts/run_full_corpus_dual_sample_lid_cutoff.sh` | Upload and run the paired recent-video cutoff experiment |
| `scripts/run_full_corpus_dual_sample_topic.sh` | Upload and run model-completed topic stages |
| `scripts/run_full_corpus_dual_sample_analysis.sh` | Upload and run post-enrichment allocation, estimation, QA, and treemap publication stages |
| `scripts/render_full_corpus_weighted_treemaps.py` | Render weighted attention/channel treemaps, explorers, and coefficient plots from compact publication cells |
| `scripts/render_full_corpus_expansion_changes.py` | Compare the exact >=10K census with the PPS-expanded view distribution for languages, topics, subtopics, and their intersections |
| `scripts/run_full_corpus_weighted_treemaps.sh` | Download compact publication inputs and render all local artifacts |
| `scripts/run_full_corpus_dual_sample_simulation.sh` | Upload and run the registered repeated-sample design evaluation |
| `scripts/run_full_corpus_dual_sample_collection.sh` | Upload and run the source-text collector after secret names are supplied |
| `scripts/run_full_corpus_dual_sample_calibration.sh` | Fit and publish validated model-topic probabilities |
| `scripts/build_full_corpus_dual_sample_job.py` | Deterministic four-task Jobs payload |
| `scripts/build_full_corpus_dual_sample_language_job.py` | Deterministic five-task language Jobs payload |
| `scripts/build_full_corpus_dual_sample_lid_cutoff_job.py` | Existing-cluster cutoff-experiment Jobs payload |
| `scripts/build_full_corpus_dual_sample_topic_job.py` | Deterministic three-task topic Jobs payload |
| `scripts/build_full_corpus_dual_sample_analysis_job.py` | Deterministic four-task analysis Jobs payload |
| `scripts/build_full_corpus_dual_sample_simulation_job.py` | Existing-cluster Jobs payload for the 5,000-replicate evaluation |
| `scripts/build_full_corpus_dual_sample_collection_job.py` | Deterministic three-task source-collection Jobs payload |
| `scripts/build_full_corpus_dual_sample_calibration_job.py` | Existing-cluster Jobs payload for topic calibration |
| `youtube_descriptive/tests/test_full_corpus_dual_sample_*.py` | Local configuration, probability, hash, and no-new-compute tests |

The language job also uploads the established tracked notebooks
`01_language_openlid_v3_databricks.py` and
`03_language_llm_panel_databricks.py`. Their model decision rules remain the
source of truth; this implementation supplies the probability-selected source
cohort, registered parameters, gates, and publication join.

## 4. Authoritative Inputs

| Purpose | Table |
|---|---|
| Frozen frame and traffic endpoints | `dev_sean.default.yt_channel_stats_full` |
| Platform topic arrays | `dev_sean.default.channel_category` |
| Hidden-subscriber audit | `dev_sean.default.channels` |
| Discovery stopping record | `dev_sean.default.pub_subs_full_pass_batches` |
| Discovery channel detail | `dev_sean.default.new_channels` |
| Dual-sample channel descriptions | `dev_sean.matt.yt_dual_sample_20260717_v1_channel_descriptions` |
| Dual-sample recent-video text | `dev_sean.matt.yt_dual_sample_20260717_v1_channel_videos` |
| Attempted channels not found | `dev_sean.matt.yt_dual_sample_20260717_v1_cd_not_found` |
| Existing final language labels | `dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_current` |
| Tail stress-test pilot | `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_treemap_pilot_channel_base` |

Every source's latest Delta history entry is captured in
`yt_dual_sample_20260717_v1_frame_source_versions`. Publication manifests must
retain those versions; a rerun against later table versions is a new analysis
version even when dates and seeds are unchanged.

## 5. Phase-One And Sampling Run

Run from the repository root:

```bash
bash scripts/run_full_corpus_dual_sample.sh
```

After a documented downstream repair, resume against the same materialized
frame without rebuilding it:

```bash
START_STAGE=draw_samples bash scripts/run_full_corpus_dual_sample.sh
```

`START_STAGE` accepts only a registered stage and executes that stage plus its
ordered successors. It never changes the frozen frame, dates, seeds, or alpha.

The submitted tasks are strictly ordered:

```text
build_frame -> simulate_design -> draw_samples -> stage_enrichment
```

### 5.1 `build_frame`

- Reads only the frozen `2026-06-15` and `2026-07-13` partitions.
- Deduplicates channel/date rows by latest `collected_at`, then a canonical
  SHA-256 row key. View and subscriber values are never tie-breakers.
- Defines threshold membership at `t0`.
- Preserves signed counter change, accepted nonnegative traffic, missing
  endpoints, zero growth, and negative revisions separately.
- Uses missing/negative/zero traffic only through the PPS uniform floor.
- Joins latest platform-topic availability and retains raw arrays separately.
- Writes the discovery stopping record and hidden-subscriber audit.

### 5.2 `simulate_design`

For `alpha` in `0.05`, `0.10`, and `0.20`, it solves

```text
q_i = alpha / N + (1 - alpha) * m_i / M
pi_i = min(1, c * q_i)
sum(pi_i) = 1,000,000
```

using certainty-unit water filling. It computes exact Poisson HT variance for
known channel, endpoint, subscriber-band, topic-availability, and view-mass
outcomes. It also computes finite-population variance for an exact fixed-size
view-band SRS at the same enrichment budget.

The frozen `alpha=0.10` is retained unless another candidate reduces the worst
registered headline relative standard error by at least 10%. If that gate is
triggered, `draw_samples` fails before selecting any IDs and requires a new
config/design version.

### 5.3 `draw_samples`

The exact SRS orders the full below-10K frame by:

```text
SHA256(UTF8(channel_id || 0x1f || frame_version || 0x1f || srs_seed))
```

and takes the first one million `(hash, channel_id)` rows. PPS maps the first 64
hash bits to `(k + 0.5) / 2^64` and selects when `u_i < pi_i`.

Never top up, trim, or reroll the Poisson sample. A realized size different from
one million is expected. Every below-10K frame row retains its hashes,
probabilities, and route flags in the frame-probability table.

### 5.4 `stage_enrichment`

The analysis union contains the complete `>=10K` census, the audited
subscriber-unknown certainty domain, and every channel selected through either
tail route. It joins existing labels, descriptions, videos, and platform topics,
then writes explicit queues. A channel name alone never qualifies as adequate
language evidence.

## 6. Primary Delta Outputs

All tables are under `dev_sean.matt` with prefix
`yt_dual_sample_20260717_v1`:

```text
_frame
_frame_summary
_frame_scope
_frame_source_versions
_unknown_subscriber_audit
_platform_topics
_design_simulation
_design_simulation_summary
_frame_probabilities
_srs
_pps
_union
_sample_qa
_analysis_union
_dispositions
_collection_queue
_collection_channel_raw
_collection_video_raw
_collection_video_items_raw
_collected_channel_descriptions
_collected_channel_videos
_collection_dispositions
_collection_summary
_lid_source_channels
_lid_source_videos
_model_topic_queue
_model_topic_source_videos
_enrichment_inventory
```

## 7. Required Collection Boundary

`_collection_queue` is the only authorized source-text collection list. Run the
resumable collector with the names of an owner-approved YouTube Data API secret:

```bash
YOUTUBE_API_SECRET_SCOPE=<scope-name> \
YOUTUBE_API_SECRET_KEY=<key-name> \
  bash scripts/run_full_corpus_dual_sample_collection.sh
```

The secret value is read only inside Databricks and is never stored in the JSON
config, Jobs payload, raw-response table, or repository. The registered secret
scope/key names are the only unresolved external credential in this runbook;
the LLM secret scope does not imply a YouTube API credential.

The collector batches `channels.list` at 50 IDs, retrieves up to ten recent
uploads per found channel, retries only the same selected IDs, and publishes
run-scoped description/video tables plus one disposition per queued channel. It
does not mutate the established source tables and never replaces a failed ID.
After publication, rerun `stage_enrichment` with the same frozen config; that
stage unions the run-scoped evidence with the established sources and chooses
the latest deterministic row.

Do not run language inference until:

```text
collection_queue_rows = 0
missing_language_labels = distinct lid_source_channels + terminal_no_text_rows
analysis_union_rows = distinct analysis_union channels
```

The language preflight enforces these conditions. Terminal not-found/no-text
channels remain selected and publish as `und`; only retryable request failures
remain in `_collection_queue`.

### 7.1 Completed 2026-07-20 backfill

The completed external backfill wrote directly to the three authoritative
tables listed in section 4. It requested up to 50 playlist items per channel;
playlist `position=0` is the newest item. Exact SQL QA against the selected
SRS/PPS union (`statement_id=01f1847d-1aac-1108-8131-947cf060173d`) found:

```text
selected union                         1,991,644
retrieved selected channels           1,987,518
missing after attempted collection        4,126
nonempty channel descriptions         1,291,799
empty channel descriptions              695,719
channels with video text              1,760,633
channels with >=10 / >=50 videos      1,371,699 / 955,711
playlist position range               0..49
invalid/null playlist positions       0
PPS retrieved / with usable text      998,124 / 972,809
SRS retrieved / with usable text      997,872 / 861,643
```

The backfill audit table contains attempted IDs outside the tail sample union
because the complete analysis union also contains certainty-stratum channels.
The enrichment stage joins by `channel_id`, deduplicates repeated attempts,
uses retrieved descriptions/videos when present, and treats an audited
not-found result with no usable text as terminal `und`. It does not infer that
every not-found channel is permanently deleted; the stored disposition is
`not_found_or_unavailable_after_attempt`. A successfully retrieved channel with
an empty description and no usable text in any collected video is separately
recorded as `channel_retrieved_no_usable_text_after_50_videos` and also remains
in the analysis as `und`; repeating the same collection is not required.

## 8. Language Run

After collection and a successful enrichment restage, first select the evidence
cap from the paired PPS experiment:

```bash
bash scripts/run_full_corpus_dual_sample_lid_cutoff.sh
```

The deterministic experiment uses 100,000 PPS-selected, dual-LID-ready
channels. OpenLID-v3 and GlotLID run once on the newest 50 videos, after which
the stored segment predictions are reaggregated at cutoffs
`1, 2, 3, 5, 8, 10, 15, 20, 30, 40, 50` using the production segment weights,
top-two admission rules, and channel-level tie breakers. The primary outcome
is the unconditional share of sampled channels for which both models return
the same base ISO code. All cutoffs use the same channels, so the report gives
paired standard errors and p-values, model coverage, exact-label agreement,
and primary-label stability against the 50-video result.

The report evaluates both all experiment channels and the fixed subset with all
50 videos available. The cutoff is the smallest candidate whose best later
dual-resolution rate is less than **0.003 higher in both populations**, meaning
less than 0.3 percentage points of possible remaining improvement. This avoids
diluting the marginal-video signal with channels that have no later uploads.
The stricter materiality rule controls selection; paired significance and
relative gain remain diagnostics. Set
`collection.recent_videos_per_channel` to the recorded recommendation before
the production run. YouTube playlist position is ordered ascending throughout
because `position=0` is newest.

Run production language inference in nonoverlapping phases, starting with PPS:

```bash
SAMPLE_PHASE=pps bash scripts/run_full_corpus_dual_sample_language.sh
SAMPLE_PHASE=remainder bash scripts/run_full_corpus_dual_sample_language.sh
SAMPLE_PHASE=combine bash scripts/run_full_corpus_dual_sample_language.sh
```

For a deliberately gated launch, `RUN_THROUGH=lid` submits only preflight and
dual LID. After those outputs pass, `START_AT=routing RUN_THROUGH=full` resumes
from routing through DeepSeek and publication without rerunning detector
inference. This is the approved sequence when PPS fallback must finish before
SRS fallback:

```bash
SAMPLE_PHASE=remainder RUN_THROUGH=lid \
  bash scripts/run_full_corpus_dual_sample_language.sh
SAMPLE_PHASE=pps START_AT=routing RUN_THROUGH=full \
  bash scripts/run_full_corpus_dual_sample_language.sh
# Run only after PPS publication succeeds and remainder dual LID succeeds.
SAMPLE_PHASE=remainder START_AT=routing RUN_THROUGH=full \
  bash scripts/run_full_corpus_dual_sample_language.sh
SAMPLE_PHASE=combine bash scripts/run_full_corpus_dual_sample_language.sh
```

`pps` includes both `pps_only` and `srs_and_pps` channels. `remainder` excludes
those IDs and therefore covers SRS-only plus non-PPS certainty-stratum rows.
`combine` runs no inference; it unions the two phase publications and fails
unless row count, distinct channel count, missing IDs, and unexpected IDs all
match `_analysis_union` exactly.

Tasks:

```text
language_preflight
  -> dual_lid
  -> prepare_routing
  -> deepseek_fallback
  -> publish_language
```

Dual LID uses OpenLID-v3 and GlotLID with 2,048 deterministic channel buckets.
Exact base-ISO agreement is accepted even when scripts disagree; script remains
ambiguous and separate. Only unresolved base-language cases go to
`deepseek-v4-flash` with thinking disabled. Insufficient evidence remains
`und`. The final output is one row per analysis-union channel in
`_channel_language_current`.

The DeepSeek evidence builder consumes the segment universe produced from the
newest 50 videos. Script counts, cue counts, repeated-pattern counts, phrase
summaries, and evidence-quality summaries therefore use all retained segments.
To prevent context and cost from growing linearly with video count, the raw
prompt excerpts are deterministically deduplicated and limited to 12 diverse
titles and four descriptions, each at most 350 characters; the complete user
prompt is capped at 6,000 characters. The larger source window thus improves
evidence coverage without sending 50 full descriptions verbatim. Request IDs
include a SHA-256 fingerprint of the complete system/user prompt and generation
settings. Existing request rows are reused only when both the prompt version
and the full request-identity set match, preventing stale responses after a
prompt or evidence change.

## 9. Topic Robustness Run

After the final language table exists:

```bash
bash scripts/run_full_corpus_dual_sample_topic.sh
```

The job classifies every platform-topic-missing analysis channel plus a
deterministic 10,000-channel platform-labeled validation sample. The response
schema requires family probabilities summing to one and leaf probabilities
summing to the corresponding family probability. Invalid sums, unknown nodes,
HTTP errors, or parse errors fail the parse gate.

Outputs are intentionally marked:

```text
probabilities.calibrated = false
```

They are not valid treemap allocations until the registered human/platform
validation and calibration step is completed. `Insufficient evidence` remains
visible and is never redistributed among substantive topics.

The analysis notebook recognizes model-completed probabilities only from
`yt_dual_sample_20260717_v1_topic_model_calibrated`. That table is a deliberate
human-validation boundary and must contain one row per nonzero channel/leaf
probability with:

```text
channel_id, status, is_calibrated, family, leaf, probability
```

Every admitted row must have `is_calibrated=true`, and leaf probabilities must
sum to one per classified channel. The analysis code never relabels the raw
DeepSeek output as calibrated.

Human adjudication must first be written to
`yt_dual_sample_20260717_v1_topic_human_validation` with:

```text
channel_id, family, leaf, human_probability, validation_weight, adjudication_status
```

Completed channel probabilities must sum to one and have one positive
probability-sample weight per channel. After at least 2,000 completed channels
also have valid held-out model predictions, run:

```bash
bash scripts/run_full_corpus_dual_sample_calibration.sh
```

The registered weighted global-temperature fit minimizes validation log loss
within `[0.25, 4.0]`, reports pre/post weighted log loss and Brier score, and
publishes only coherent per-channel probabilities. This is a confidence
calibration, not evidence that taxonomy coverage or class-specific error has
vanished; stratified validation diagnostics remain required for publication.

## 10. Post-Enrichment Analysis Run

After final language publication and, if used, validated topic calibration:

```bash
bash scripts/run_full_corpus_dual_sample_analysis.sh
```

Tasks are ordered:

```text
allocate -> estimate -> qa -> publish_treemap
```

The primary `platform_only` allocation suppresses a parent when a child in the
same family exists, allocates equally across families and then leaves, and
retains `Unlabeled` and unmapped mass. The optional `model_completed` allocation
is added only when the calibrated-table gate passes.

The estimate stage writes raw Horvitz-Thompson shares and design-based standard
errors. Its general coherent display share applies the registered tail-total
ratio: the SRS channel factor is exactly one, while the PPS view factor
reconciles the realized tail HT view total to the known phase-one tail view
total. The publication stage further calibrates the primary `platform_only`
language-topic geometry to exact frozen-frame family/leaf channel and view
margins. Raw estimates and uncertainty remain unchanged. See
`docs/FULL_CORPUS_WEIGHTED_TREEMAP_RUNBOOK_20260718.md` for the equations,
published schema, artifact commands, and acceptance gates.
Raw estimates remain the inferential benchmark. The weighting-difference table
reports
`view_share - channel_share`; its initial SE uses the independent-design
approximation and explicitly flags that joint measurement-error replication is
still required for final inference.

After `publish_treemap` succeeds, download and render the complete local suite:

```bash
bash scripts/run_full_corpus_weighted_treemaps.sh
```

In addition to the static/interactive treemaps, this writes a census-expansion
CSV, Markdown summary, and coefficient-style absolute/proportional change plots
for language, family, leaf, language x family, and language x family x leaf.
For the registered `all_retrievable` artifact, the comparison baseline is the
exact view composition of the `>=10K` census plus the 2,201
subscriber-unknown certainty rows; the expanded point adds the below-10K tail
using the calibrated PPS platform composition. The `known_subscriber` variant
uses the `>=10K` census alone. Percentage-point change remains defined for cells
absent from the baseline and is primary.
Proportional rankings require a nonzero, non-negligible census baseline and the
registered reliability gate; zero-baseline cells are reported separately.
Language and language-by-topic comparisons use raw Horvitz-Thompson share SEs.
Topic-family and subtopic marginals are exact full-frame topic/view margins, so
their PPS sampling SE is zero; measurement error is not included. Raw HT fields
remain in the CSV for audit, while calibrated geometry defines the display.

## 11. Repeated-Sample Evaluation

Run after the frame exists; semantic enrichment is not required:

```bash
bash scripts/run_full_corpus_dual_sample_simulation.sh
```

The job freezes separate below-10K and `>=10K` pseudo-populations. Each contains
the 5,000 largest accepted-view channels in its source domain plus a
deterministic SHA-256 sample to a total of 100,000 rows. It then runs 5,000 exact
SRS draws and independent Poisson PPS draws at the full design's sampling
fraction. For channel count, accepted positive view mass, endpoint coverage,
positive-delta prevalence, and platform-topic coverage, it writes bias,
empirical and reported SE, RMSE, 95% coverage, design effect, effective sample
size, weight CV, and maximum weight share to
`yt_dual_sample_20260717_v1_repeated_simulation`.

These are implementation and concentration stress tests on explicitly labeled
pseudo-populations. They do not substitute for unavailable true below-10K
semantic labels and do not change the collected-frame coverage estimand.

## 12. Local Verification

Run without Databricks:

```bash
PYTHONPATH=. python3 -m unittest \
  youtube_descriptive.tests.test_full_corpus_dual_sample_design \
  youtube_descriptive.tests.test_full_corpus_dual_sample_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_language_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_lid_cutoff_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_topic_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_analysis_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_simulation_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_collection_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_calibration_job \
  youtube_descriptive.tests.test_full_corpus_expansion_changes \
  youtube_descriptive.tests.test_full_corpus_weighted_treemap_renderer -v

youtube_descriptive/.venv/bin/ruff check \
  youtube_descriptive/src/full_corpus_dual_sample_design.py \
  youtube_descriptive/src/11_full_corpus_dual_sample_databricks.py \
  youtube_descriptive/src/12_full_corpus_dual_sample_language_databricks.py \
  youtube_descriptive/src/13_full_corpus_dual_sample_topic_model_databricks.py \
  youtube_descriptive/src/14_full_corpus_dual_sample_analysis_databricks.py \
  youtube_descriptive/src/15_full_corpus_dual_sample_repeated_simulation_databricks.py \
  youtube_descriptive/src/16_full_corpus_dual_sample_collection_databricks.py \
  youtube_descriptive/src/17_full_corpus_dual_sample_lid_cutoff_experiment_databricks.py \
  youtube_descriptive/src/17_full_corpus_dual_sample_topic_calibration_databricks.py \
  scripts/build_full_corpus_dual_sample_job.py \
  scripts/build_full_corpus_dual_sample_language_job.py \
  scripts/build_full_corpus_dual_sample_lid_cutoff_job.py \
  scripts/build_full_corpus_dual_sample_topic_job.py \
  scripts/build_full_corpus_dual_sample_analysis_job.py \
  scripts/build_full_corpus_dual_sample_simulation_job.py \
  scripts/build_full_corpus_dual_sample_collection_job.py \
  scripts/build_full_corpus_dual_sample_calibration_job.py \
  scripts/render_full_corpus_weighted_treemaps.py \
  scripts/render_full_corpus_expansion_changes.py \
  scripts/render_treemap_v3.py
```

## 13. Acceptance Rules

The frame/sample run is complete only when its transcript and QA tables show:

```text
FRAME CONSERVATION: PASS
frame rows = distinct frame channels
threshold strata sum to frame rows
SRS rows = 1,000,000
PPS sum(pi) = 1,000,000 within 1e-4
all pi in (0, 1]
all certainty PPS rows selected
union rows = SRS + PPS - overlap
SAMPLE CONSERVATION: PASS
ENRICHMENT STAGING: PASS
```

Language and model-topic stages have separate conservation gates and cannot be
declared complete merely because API requests were submitted.

Post-enrichment acceptance additionally requires:

```text
CHANNEL ALLOCATION CONSERVATION: PASS
VIEW ALLOCATION CONSERVATION: PASS
DISPLAY SHARE CONSERVATION: PASS
```

## 14. Reruns And Failures

- Reuse the same design version only for an idempotent repair against identical
  source versions, dates, seeds, and rules.
- Never replace selected failures with other channels.
- Never change `alpha`, a hash seed, threshold membership, or endpoint rules in
  place. Create a new JSON config and output prefix.
- Preserve failed HTTP and parse results. Retry by deterministic `request_id`.
- Do not commit generated table exports, model responses, or rendered outputs.
- Record Databricks run IDs and final QA values in this document after each
  production execution.

## 15. Current Execution Log

| Date | Run ID | Stage | Status |
|---|---:|---|---|
| 2026-07-17 | `644840643258205` | Frame, design screen, draw, enrichment | Frame and screen passed; draw failed on duplicate version columns; enrichment skipped |
| 2026-07-17 | `35159108968698` | Repair from `draw_samples` | SUCCESS; draw and initial enrichment staging passed |
| 2026-07-17 | `896176326148446` | 5,000-replicate design evaluation | SUCCESS; 20 outcome/design/domain rows written |
| 2026-07-17 | `99262517647565` | Simulation schema repair rerun | SUCCESS; explicit `wald_normal` and variance-ratio fields written |
| 2026-07-20 | `771680734300337` | First post-collection enrichment refresh | CANCELED by this agent after QA showed retrieved empty-text channels needed an explicit terminal disposition |
| 2026-07-20 | `845387019641338` | Repaired post-collection enrichment refresh | SUCCESS; collection queue zero and language source conservation passed |
| 2026-07-20 | `98930401179212` | 100,000-PPS-channel recent-video cutoff experiment | SUCCESS; 50 videos selected under the 0.3-point rule |
| 2026-07-20 | `288009785189569` | Production PPS language phase | FAILED after dual LID succeeded; obsolete routing assertion treated intentional no-aggregation rows as lost |
| 2026-07-20 | `873172557781496` | Nonoverlapping SRS/remainder language phase, LID only | SUCCESS; both detectors and final channel outputs conserved |
| 2026-07-20 | `757710578516111` | PPS routing, DeepSeek fallback, and publication continuation | SUCCESS; all 150,853 routes received verdicts and the 1,000,144-row PPS language table was published |
| 2026-07-21 | `442856261733489` | First SRS/remainder fallback continuation | CANCELED after two 10,000-request chunks returned HTTP 402 `Insufficient Balance`; no successful SRS fallback responses were lost |

Successful results from the first two stages of `644840643258205`:

```text
FRAME ROWS / DISTINCT: 122,126,394 / 122,126,394
BELOW 10K / >=10K / UNKNOWN: 117,235,838 / 4,888,355 / 2,201
ACCEPTED POSITIVE 4WK VIEWS: 6,134,809,538,681
BELOW-10K POSITIVE 4WK VIEWS: 670,270,625,558
ALPHA SCORES (worst headline RSE): 0.05=0.5463%, 0.10=0.4349%, 0.20=0.4759%
RECOMMENDED / SELECTED ALPHA: 0.10 / 0.10
CONFIG CHANGE REQUIRED: false
```

The failed draw is retained in this log. Its Delta write rejected duplicate
`design_version` and `frame_version` columns after joining the already-written
probability frame to the tail payload. The repair drops only the duplicate
payload copies and resumes from the same probabilities and frozen inputs.

Successful sample and staging results from repair run `35159108968698`:

```text
SRS ROWS: 1,000,000
PPS REALIZED ROWS: 1,000,144
SRS/PPS OVERLAP: 8,500
TAIL UNION ROWS: 1,991,644
PPS SUM PI: 999,999.999996984
PPS CERTAINTY CHANNELS / UNSELECTED: 199,254 / 0
ANALYSIS UNION ROWS / DISTINCT: 6,882,200 / 6,882,200
CERTAINTY ROWS: 4,890,556
EXISTING LANGUAGE LABELS: 4,787,320
REQUIRES SOURCE-TEXT COLLECTION: 2,094,880
TOPIC ROWS / NONEMPTY: 6,880,708 / 6,227,663
REQUIRES MODEL-TOPIC ROBUSTNESS: 654,537
SAMPLE CONSERVATION: PASS
ENRICHMENT STAGING: PASS
```

Post-collection enrichment results from `845387019641338`:

```text
ANALYSIS UNION ROWS / DISTINCT: 6,882,200 / 6,882,200
CERTAINTY ROWS: 4,890,556
EXISTING LANGUAGE LABELS: 4,787,320
READY FOR DUAL LID: 1,926,208
TERMINAL NO-TEXT ASSIGN UND: 168,672
REQUIRES SOURCE-TEXT COLLECTION: 0
TOPIC ROWS / NONEMPTY: 6,880,708 / 6,227,663
REQUIRES MODEL-TOPIC ROBUSTNESS: 654,537
ENRICHMENT STAGING: PASS
```

Recent-video cutoff results from `98930401179212`:

```text
SAMPLE CHANNELS / DISTINCT: 100,000 / 100,000
SAMPLE VIDEO ROWS / CHANNELS: 4,155,729 / 98,877
FIXED 50-VIDEO COHORT: 73,198
VALID SEGMENTS PER MODEL: 4,249,034
COMPACT OPENLID / GLOTLID ROWS: 4,249,034 / 4,249,034
DUAL RESOLUTION AT 10 / 30 / 40 / 50: 75.877% / 78.880% / 79.499% / 79.860%
PAIRED 40-TO-50 GAIN: 0.361 percentage points (SE 0.0469; p=1.39e-14)
FIXED-50 COHORT 40-TO-50 GAIN: 0.481 percentage points (SE 0.0631; p=2.62e-14)
OPENLID / GLOTLID / BOTH COVERAGE AT 50: 94.168% / 94.359% / 94.089%
EXACT-LABEL AGREEMENT AT 50: 79.033%
RECOMMENDED RECENT VIDEOS PER CHANNEL: 50
CUTOFF EXPERIMENT CONSERVATION: PASS
```

The 40-to-50 improvement exceeds the registered 0.3-percentage-point stopping
threshold in both the complete experiment sample and the fixed 50-video cohort.
The config therefore freezes `recent_videos_per_channel=50`. These are
dual-model resolution/agreement diagnostics, not ground-truth language
accuracy estimates. Summary query statement ID:
`01f18485-5b2c-1e9f-a2c7-2dd2c401bcd8`.

Production PPS preflight from run `288009785189569`:

```text
PPS ANALYSIS ROWS / DISTINCT: 1,000,144 / 1,000,144
EXISTING LABELS: 276
MISSING LABELS: 999,868
DUAL-LID SOURCE CHANNELS / DISTINCT: 972,809 / 972,809
TERMINAL NO-TEXT UND: 27,059
COLLECTION QUEUE: 0
LID VIDEO ROWS / CHANNELS: 40,480,704 / 962,059
276 + 972,809 + 27,059 = 1,000,144: PASS
LANGUAGE PREFLIGHT: PASS
```

The expensive dual-LID task in that run (`694501391955531`) succeeded. The
following routing task failed only because the first orchestration gate required
the channel-model comparison table to contain every LID source channel. The LID
pipeline intentionally omits channels with no valid model aggregation from that
comparison and retains them in the complete channel table for
`route_unclassified=true`. The repaired continuation `757710578516111` proved:

```text
LID ROWS / DISTINCT: 972,809 / 972,809
COMPARISON ROUTING ROWS / DISTINCT: 920,175 / 920,175
MISSING COMPARISON ROWS: 52,634
MISSING COMPARISON ROWS ELIGIBLE AS UNCLASSIFIED: 52,634
UNEXPECTED COMPARISON ROWS: 0
BASE-LANGUAGE RESOLVED: 821,956
COMPARISON DISAGREEMENTS ROUTED TO DEEPSEEK: 98,219
TOTAL DEEPSEEK ROUTES: 150,853
LANGUAGE ROUTING: PASS
```

PPS request-table QA before API submission:

```text
REQUEST ROWS / DISTINCT CHANNELS: 150,853 / 150,853
USER PROMPT CHARACTERS MIN / MEAN / MAX: 666 / 4,054.75 / 6,000
PROMPTS AT THE 6,000-CHARACTER CAP: 25,015
NULL PROMPT FINGERPRINTS: 0
DISTINCT PROMPT FINGERPRINTS: 150,851
SYSTEM PROMPT CHARACTERS: 23,334
```

Two pairs of channels have identical prompt payloads, which is permitted;
request IDs remain unique because channel ID is also part of the identity. All
150,853 DeepSeek requests completed and PPS publication reported:

```text
PUBLISHED ROWS / DISTINCT CHANNELS: 1,000,144 / 1,000,144
CLASSIFIED / UND: 951,030 / 49,114
DEEPSEEK CLASSIFIED / INSUFFICIENT: 128,807 / 22,046
REUSED EXISTING SILVER LABELS: 276
PPS LANGUAGE PUBLICATION: SUCCESS
```

SRS/remainder preflight from run `873172557781496`:

```text
ANALYSIS ROWS / DISTINCT: 5,882,056 / 5,882,056
EXISTING LABELS: 4,787,044
MISSING LABELS: 1,095,012
DUAL-LID SOURCE CHANNELS / DISTINCT: 953,399 / 953,399
TERMINAL NO-TEXT UND: 141,613
COLLECTION QUEUE: 0
LID VIDEO ROWS / CHANNELS: 23,819,485 / 897,579
4,787,044 + 953,399 + 141,613 = 5,882,056: PASS
LANGUAGE PREFLIGHT: PASS
```

This remainder phase excludes all PPS-selected IDs, including the 8,500
SRS/PPS overlap. It therefore performs no duplicate inference while completing
the SRS-only and still-unlabeled census portion needed for the final conserved
analysis union.

Final SRS/remainder detector QA:

```text
SEGMENT INPUT ROWS: 37,231,133
VALID SEGMENTS PER MODEL: 21,431,571
INFERENCE PARTITIONS: 858
OPENLID COMPACT ROWS: 21,431,571
GLOTLID COMPACT ROWS: 21,431,571
FINAL CHANNEL ROWS / UNIVERSE: 953,399 / 953,399
DUAL-LID RUN: SUCCESS
```

SRS/remainder fallback run `442856261733489` resumed from those detector
outputs without rerunning LID. Routing conservation and request-table QA:

```text
LID ROWS / DISTINCT: 953,399 / 953,399
COMPARISON ROUTING ROWS / DISTINCT: 793,558 / 793,558
MISSING COMPARISON ROWS: 159,841
MISSING COMPARISON ROWS ELIGIBLE AS UNCLASSIFIED: 159,841
UNEXPECTED COMPARISON ROWS: 0
BASE-LANGUAGE RESOLVED: 707,474
COMPARISON DISAGREEMENTS ROUTED TO DEEPSEEK: 86,084
TOTAL DEEPSEEK ROUTES: 245,925
REQUEST ROWS / DISTINCT CHANNELS: 245,925 / 245,925
USER PROMPT CHARACTERS MIN / MEAN / MAX: 666 / 2,948.67 / 6,000
PROMPTS AT THE 6,000-CHARACTER CAP: 13,939
NULL PROMPT FINGERPRINTS: 0
DISTINCT PROMPT FINGERPRINTS: 245,826
LANGUAGE ROUTING: PASS
```

Repeated prompt payloads are allowed, while channel-scoped request IDs remain
unique. The request table was committed at `2026-07-21T04:33:37Z`; the first
10,000-request DeepSeek chunk began at `2026-07-21T04:34:11Z`. The first two
chunks each returned `ok=0; error=10,000` because the account behind
`youtube-llm-keys/deepseek-api-key` had insufficient balance. Run
`442856261733489` was canceled before the remaining requests were attempted.
The failed result rows are retained, but no successful SRS response was lost.

The direct runner now treats uniform HTTP 401/402/403 responses as fatal after
the first 500-request microbatch, rethrows after recording the failed chunk, and
halts all later chunks. On deterministic retry it rewrites an existing failed
result file while retaining only prior successes, rather than appending stale
failure rows. After the DeepSeek account is funded, rerun the same remainder
continuation command; the immutable request IDs and prompts will be reused.

Repeated-sample highlights from `896176326148446`:

```text
TAIL ACCEPTED-VIEW PPS DESIGN EFFECT VS SRS: 0.0168
HEAD ACCEPTED-VIEW PPS DESIGN EFFECT VS SRS: 0.0236
TAIL / HEAD PPS VIEW-MASS 95% COVERAGE: 95.18% / 95.38%
TAIL / HEAD SRS VIEW-MASS 95% COVERAGE: 91.56% / 92.00%
TAIL PPS MEAN REALIZED N / EFFECTIVE N: 852.9 / 93.0
HEAD PPS MEAN REALIZED N / EFFECTIVE N: 852.6 / 100.3
```

This supports the coordinated estimand-specific design: PPS is dramatically
more efficient and has materially better normal-interval behavior for the
concentrated view-mass outcome, while SRS is much more efficient for channel
prevalence and platform-topic coverage. Symmetric normal intervals performed
poorly for the near-one valid-endpoint proportion (70.0% tail and 83.0% head SRS
coverage) and are not approved for that boundary case; use a bounded/binomial
or replicate interval. The result is retained as a negative interval diagnostic,
not hidden or redefined as a pass.

The mean reported/empirical variance ratios are close to one for the view-mass
outcomes (`0.977` tail PPS, `1.025` head PPS, `1.010` tail SRS, `1.014` head
SRS). The coverage shortfall is therefore chiefly a shape/interval problem, not
a hidden variance collapse. Bounded result query statement ID:
`01f1825c-c2b5-11d1-ad3e-f73bf1e450f6`.

Run page:
`https://adb-1335559103600339.19.azuredatabricks.net/?o=1335559103600339#job/470109858178676/run/644840643258205`
