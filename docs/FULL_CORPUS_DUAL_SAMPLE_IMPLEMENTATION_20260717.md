# Full-Corpus Dual-Sample Implementation Runbook

**Design version:** `full_corpus_dual_sample_20260717_v1`
**Frame version:** `yt_dual_sample_20260717_v1`
**Design specification:** [FULL_CORPUS_DUAL_SAMPLE_DESIGN_20260717.md](FULL_CORPUS_DUAL_SAMPLE_DESIGN_20260717.md)
**Target population:** channels in the frozen 2026-06-15 collected frame
**Traffic endpoint:** 2026-07-13, 28 elapsed days
**Status:** frame, design screen, samples, and initial enrichment staging complete; source collection pending

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
| `youtube_descriptive/src/17_full_corpus_dual_sample_topic_calibration_databricks.py` | Human-validation-gated weighted temperature calibration and QA |
| `scripts/run_full_corpus_dual_sample.sh` | Upload and run phase-one/sample stages |
| `scripts/run_full_corpus_dual_sample_language.sh` | Upload and run gated dual-LID/DeepSeek stages |
| `scripts/run_full_corpus_dual_sample_topic.sh` | Upload and run model-completed topic stages |
| `scripts/run_full_corpus_dual_sample_analysis.sh` | Upload and run post-enrichment allocation, estimation, QA, and treemap publication stages |
| `scripts/render_full_corpus_weighted_treemaps.py` | Render weighted attention/channel treemaps, explorers, and coefficient plots from compact publication cells |
| `scripts/run_full_corpus_weighted_treemaps.sh` | Download compact publication inputs and render all local artifacts |
| `scripts/run_full_corpus_dual_sample_simulation.sh` | Upload and run the registered repeated-sample design evaluation |
| `scripts/run_full_corpus_dual_sample_collection.sh` | Upload and run the source-text collector after secret names are supplied |
| `scripts/run_full_corpus_dual_sample_calibration.sh` | Fit and publish validated model-topic probabilities |
| `scripts/build_full_corpus_dual_sample_job.py` | Deterministic four-task Jobs payload |
| `scripts/build_full_corpus_dual_sample_language_job.py` | Deterministic five-task language Jobs payload |
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
| Existing source descriptions | `dev_sean.matt.yt_channel_descriptions` |
| Existing recent-video text | `dev_sean.matt.yt_channel_videos` |
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

## 8. Language Run

After collection and a successful enrichment restage:

```bash
bash scripts/run_full_corpus_dual_sample_language.sh
```

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
  youtube_descriptive.tests.test_full_corpus_dual_sample_topic_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_analysis_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_simulation_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_collection_job \
  youtube_descriptive.tests.test_full_corpus_dual_sample_calibration_job \
  youtube_descriptive.tests.test_full_corpus_weighted_treemap_renderer -v

youtube_descriptive/.venv/bin/ruff check \
  youtube_descriptive/src/full_corpus_dual_sample_design.py \
  youtube_descriptive/src/11_full_corpus_dual_sample_databricks.py \
  youtube_descriptive/src/12_full_corpus_dual_sample_language_databricks.py \
  youtube_descriptive/src/13_full_corpus_dual_sample_topic_model_databricks.py \
  youtube_descriptive/src/14_full_corpus_dual_sample_analysis_databricks.py \
  youtube_descriptive/src/15_full_corpus_dual_sample_repeated_simulation_databricks.py \
  youtube_descriptive/src/16_full_corpus_dual_sample_collection_databricks.py \
  youtube_descriptive/src/17_full_corpus_dual_sample_topic_calibration_databricks.py \
  scripts/build_full_corpus_dual_sample_job.py \
  scripts/build_full_corpus_dual_sample_language_job.py \
  scripts/build_full_corpus_dual_sample_topic_job.py \
  scripts/build_full_corpus_dual_sample_analysis_job.py \
  scripts/build_full_corpus_dual_sample_simulation_job.py \
  scripts/build_full_corpus_dual_sample_collection_job.py \
  scripts/build_full_corpus_dual_sample_calibration_job.py \
  scripts/render_full_corpus_weighted_treemaps.py \
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
