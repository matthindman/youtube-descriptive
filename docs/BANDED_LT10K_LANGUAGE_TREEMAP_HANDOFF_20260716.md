# Below-10K Language and Treemap Pilot Handoff

**Completed:** 2026-07-16
**Databricks profile:** `matt.hindman@researchaccelerator.org`
**Workspace:** `dev_sean.matt`
**Sample run:** `2026-06-18`
**Language label version:** `banded_lt10k_20260716_lid_deepseek_v1`

## Purpose

This run classifies the 2,000 sampled channels below 10,000 subscribers and
prepares an analysis-ready pilot base for evaluating what a lower subscriber
threshold might do to the language/topic treemap.

The sample is **not population weighted**. It contains exactly 200 channels in
each 1,000-subscriber band from 0-999 through 9,000-9,999. The stored sample
does not include frame sizes, selection seeds, inclusion probabilities, or
nonresponse dispositions. Do not treat the pooled 2,000-channel distribution
as an estimate of the below-10K population.

## Primary Tables

Use these first:

| Purpose | Table |
|---|---|
| Final one-row-per-channel language lookup | `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_channel_language_current` |
| Versioned copy of the final labels | `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_channel_language_labels` |
| Analysis-ready language/topic/traffic pilot | `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_treemap_pilot_channel_base` |
| Language coverage by sample band | `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_channel_language_band_summary` |
| Language run summary | `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_channel_language_run_summary` |
| Pilot-base summary | `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_treemap_pilot_summary` |

In the final language table, use `channel_language` as the standard base ISO
639-3 analysis variable. `und` means unresolved. Keep
`channel_language_script`, `is_mixed_language`, `is_romanized`, and
`is_script_ambiguous` as separate attributes; do not split a base language in
the treemap merely because scripts differ.

## Language Run

### Source and staging

| Purpose | Table |
|---|---|
| Sample membership/subscribers/band | `dev_sean.matt.yt_banded_sample` |
| Raw channel metadata | `dev_sean.matt.yt_banded_channel_descriptions` |
| Raw recent videos | `dev_sean.matt.yt_banded_channel_videos` |
| Run-scoped channel staging | `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_source_channels` |
| Run-scoped video staging | `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_source_videos` |

Source coverage:

- 2,000 channels, 2,000 distinct IDs, 200 in each band.
- 1,971 have a channel-metadata row.
- 1,488 have a nonempty channel description.
- 1,857 have recent-video rows, totaling 16,863 videos.
- 29,732 text segments were built; 18,746 passed LID eligibility.

### Dual LID

- Source run ID: `banded_lt10k_20260716`
- Databricks submit run: `153258788085409`
- Successful task run: `53783707835957`
- Models: OpenLID-v3 and GlotLID.
- LID channel output: `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_channels`
- Model comparison: `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_channel_model_comparison`
- Segment input: `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_segments_input`
- Full table family prefix: `dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_`

The final LID table has 2,000 unique channels. The comparison table has 1,764
channels. Major statuses were 1,412 exact agreements, 144 model disagreements,
101 taxonomy-normalized agreements, 46 unresolved high-risk-tail cases, 23
ISO/script-variant agreements, 19 GlotLID fallbacks, and 15 cluster agreements.
Another 236 channels had no comparison row, usually because usable model text
was absent.

### DeepSeek fallback

- LLM run ID: `banded_lt10k_20260716_deepseek_flash_fallback`
- Databricks submit run: `120697250178571`
- Successful task run: `690187541454975`
- Model: `deepseek-v4-flash`
- Thinking: disabled.
- Prompt: `llm_fallback_final_guardrails_post_review_20260630`
- Routing: only channels without a usable LID base language; script-only LID
  disagreement did not trigger fallback.
- Requests: 441 unique channels.
- Classified by DeepSeek: 279.
- DeepSeek `insufficient_text`: 162.
- Technical/HTTP/parse failures: 0.

DeepSeek tables use prefix
`dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_deepseek_flash_fallback_`.
The most useful are `_llm_requests`, `_llm_raw_results`, and `_llm_verdicts`.

### Final label QA

- Rows: 2,000.
- Distinct `channel_id`: 2,000.
- Classified: 1,838 (91.9%).
- `und`: 162 (8.1%).
- Credible mixed-language flag: 119.
- Script ambiguous: 23.
- Label source: 1,535 LID consensus, 24 LID base-ISO agreement, 279 DeepSeek
  fallback, and 162 DeepSeek `insufficient_text`.

Largest language counts are English 788, Hindi 131, Spanish 112, Portuguese
109, Arabic 107, Indonesian 96, Russian 74, Bengali 44, Japanese 36, French
33, Vietnamese 28, Korean 23, Thai 20, Turkish 20, and Mandarin 19. These are
unweighted sample counts.

Classification coverage by sample band ranges from 69.0% in band 0 to 98.0%
in band 6. Bands 1-9 are all at least 91.5% classified. The 0-999 band has 62
of the 162 unresolved channels.

## Treemap Inputs and Feasibility

The existing full-corpus treemap is implemented in
`youtube_descriptive/src/youtube_topic_treemap_full_corpus.py` and documented
in `docs/TREEMAP_FULL_CORPUS_RUNBOOK.md`.

Current treemap inputs:

- Language: `dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_current`
- Topics: `dev_sean.default.channel_category`
- Traffic: `dev_sean.default.yt_channel_stats`
- Current/prior snapshots: 2026-06-15 and 2026-05-18.
- Subscriber floor: 10,000 at the current snapshot.
- Output prefix: `dev_sean.matt.yt_treemap_full_corpus_lid_v3_20260715_v1`

The below-10K sample overlaps the current treemap channel base in only one
channel because almost all of these sample labels were absent from the earlier
global language silver. Topic coverage is much stronger: 1,972 channels have a
topic row and 1,763 have nonempty YouTube topic arrays.

The existing `yt_channel_stats` source covers only one sample channel on the
treemap's 2026-06-15/2026-05-18 window. Therefore, do not append the pilot to
the existing treemap and claim a comparable change in four-week view mass.

The broader `dev_sean.default.yt_channel_stats_full` table supports a clean,
shifted 28-day window:

- Current: 2026-07-13, 1,967 sample channels.
- Prior: 2026-06-15, 1,972 sample channels.
- Both dates: 1,967.
- Valid nonnegative four-week deltas: 1,889.
- Positive deltas: 1,661; zero: 228; negative revisions: 78.
- At 2026-07-13, 1,913 were still below 10,000 and 54 were at or above 10,000;
  33 lacked a current snapshot.

The pilot channel-base table materializes these fields. Its key columns are:

- `channel_language`, script/mixed/provenance fields;
- `sampled_subscriber_count`, `sample_band`, `sample_run_id`;
- `current_subscriber_count`, `prior_subscriber_count`;
- `current_lifetime_views`, `prior_lifetime_views`;
- `raw_4wk_views`, `view_count_4wk`, `avg_weekly_view_count`;
- snapshot, negative-revision, and traffic-validity flags;
- `raw_topic_categories`, `topic_row_present`, and
  `has_nonempty_topic_categories`.

## Recommended Next Analysis

1. Use the pilot base for **descriptive sensitivity analysis**, stratified by
   `sample_band`. Report each band separately before showing any pooled result.
2. Reuse the current treemap's topic normalization and family-balanced
   allocation exactly. Do not use named-channel display overrides for inference.
3. For a traffic-weighted comparison, rerun both the at-least-10K baseline and
   the pilot sensitivity on the same 2026-06-15 to 2026-07-13 window using
   `yt_channel_stats_full`. Do not compare the shifted pilot month directly with
   the existing 2026-05-18 to 2026-06-15 production treemap.
4. Freeze threshold membership at a declared snapshot date. Do not use the
   June 18 sample subscriber count for a July 13 threshold without reporting
   crossings and missing current snapshots.
5. Match the production handling of negative revisions when making the direct
   sensitivity comparison: keep `raw_4wk_views`, but set accepted view mass to
   null when current lifetime views are lower than prior lifetime views.
6. Treat the pilot's raw view totals and language shares as diagnostics only.
   The pooled sample overweights low-population bands and underweights
   high-population bands whenever band frame sizes differ.
7. To estimate the treemap under a true lower-threshold full collection, first
   build a versioned lower-threshold frame and either collect it as a census or
   store `N_h`, inclusion probabilities, final weights, and nonresponse status.
   The present table cannot recover those values retrospectively.
8. Run threshold scenarios such as 10K, 5K, and 1K only after the language,
   topic, and traffic universes use the same channel frame and snapshot dates.
   Report added channel count, added valid view mass, language shares, topic
   shares, `und` share, and concentration at every threshold.

For orientation only, the unweighted pilot has about 160.2 million accepted
four-week views. English contributes about 101.4 million and Mandarin about
23.1 million. Band totals vary sharply, from 0.27 million in band 0 to 47.7
million in band 5, which is direct evidence that equal channel counts per band
must not be pooled as if they were population weights.

## Code and Workspace Locations

Local run drivers:

- `.codex_databricks/run_banded_lt10k_lid_20260716.py`
- `.codex_databricks/run_banded_lt10k_deepseek_publish_20260716.py`
- `.codex_databricks/build_banded_lt10k_treemap_pilot_base_20260716.py`

Databricks workspace folder:

`/Users/matt.hindman@researchaccelerator.org/banded_lt10k_language_20260716`

The active production notebook sources remain:

- `youtube_descriptive/src/01_language_openlid_v3_databricks.py`
- `youtube_descriptive/src/03_language_llm_panel_databricks.py`
- `youtube_descriptive/src/youtube_topic_treemap_full_corpus.py`

Operational audit notes:

- Initial LID submit run `600102235921346` stopped at preflight because the
  audit conflated 1,971 description-table rows with 1,488 nonempty description
  bodies. The metrics were separated before the successful LID run; no model
  inference from the failed attempt was accepted.
- Initial pilot-base submit run `959442929537874` passed its data QA but stopped
  before writing because `sample_run_id` appeared twice after the join. The
  redundant projection was removed before successful run `1116810601441868`.
- All accepted outputs are run-specific and passed the final 2,000-row,
  2,000-distinct-ID, zero-missing-ID checks.

Use cluster `0601-203643-bkxsqffg` only, per
`youtube_descriptive/src/AGENT_DATA_CONTEXT.md`. This workflow did not stop the
cluster; normal auto-termination applies.
