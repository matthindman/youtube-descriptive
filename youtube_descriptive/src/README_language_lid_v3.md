# README: YouTube language classification v3 (OpenLID-v3 + GlotLID) on Databricks

`01_language_openlid_v3_databricks.py` classifies YouTube channel language by running **two** fastText
language-ID models — OpenLID-v3 (the legacy primary detector) and GlotLID — on the **same universe of
valid text segments**, then comparing them and producing model-specific and consensus labels. It supersedes
the original single-model OpenLID first cut.

## Current validation status

The pipeline has completed a 10,000-channel Databricks validation run split evenly between prior
OpenLID/GlotLID top-of-ocean exact-agreement and exact-disagreement cases. The run wrote the expected core
tables, compact OpenLID/GlotLID predictions, analysis tables, table-backed SVG figures, and the preflight
estimate table under `dev_sean.matt` with prefix
`yt_lid_v3_validation_10k_20260608_161345_b10`.

The follow-up field-source ablation found no substantial overall advantage to changing the default
recent-video-metadata approach. Channel title/name contributed little in that run and can be omitted or kept
at very low weight; video titles alone reduced coverage and agreement; channel description could not be
evaluated because no valid `channel_description` segments were present in the validation source.

Treat this README and the current notebook code as the operational source of truth. Historical planning,
review, and validation-fix files should not be used as active runbooks.

## Production source and output defaults

The production LID source defaults are the top-of-ocean shaped tables:

- `prod_tads.youtube_too.yt_sl_channels`
- `prod_tads.youtube_too.yt_sl_videos`

The full TOO run should use the same deduplicated channel universe as the earlier big LID run, not a freshly
ranked subscriber cohort from `yt_sl_channels_metrics`. In the current workspace, the earlier big LID output
`dev_sean.matt.yt_lid_v3_channels` has `run_id='default'` with 105,638 channel rows. Subscriber metrics are
only auxiliary context unless a separate subscriber-cohort analysis run is explicitly requested.

Output defaults are separated from source defaults. The notebook reads from `prod_tads.youtube_too` but writes
the `yt_lid_v3_*` result family, preflight table, validation sample, ablation summary when enabled, and
progress table to `dev_sean.matt` by default. Use `catalog`/`schema` for source location and
`output_catalog`/`output_schema` for result location.

## Runtime requirements

Requires **Databricks Runtime 13.0+ (Apache Spark 3.4+)**. The channel aggregation and validation-sampling
steps use `array_sort` with a comparator and `array_compact`, both introduced in Spark 3.4; the notebook
asserts the Spark version up front and fails clearly on older runtimes. Notebook-scoped Python deps
(`numpy<2`, `fasttext`, `huggingface-hub`, `regex`, `pandas`, `pyarrow`) are installed by the first cell.
The production defaults avoid DBFS FUSE assumptions: model binaries default to Unity Catalog Volume paths,
and checkpointing falls back to persisted storage on shared clusters where direct `SparkContext` access is
blocked.

## 1. What the output measures

The output is **written metadata language** (channel name, channel description, video titles, video
descriptions, tags), **not** spoken/video language. YouTube source language fields (`defaultLanguage`,
`detected_language`, etc.) are preserved only as **audit** fields — never treated as ground truth. They are
sparse and themselves usually describe metadata rather than spoken content.

## 2. Two models, run by default on all valid segments

Both OpenLID-v3 and GlotLID run by default (`enable_openlid=true`, `enable_glotlid=true`,
`glotlid_mode=all_valid_segments`). Both read from the same canonical `yt_lid_v3_segments_input` table and
classify the **same** valid-segment universe, so model agreement, disagreement, fallback, and Hindi-recall
diagnostics are computed over an unbiased shared set rather than a low-confidence subset. Production
defaults verify the shared universe with row-count and per-bucket checksum parity; set `run_heavy_qa=true`
to run the full `segment_id` parity join. Per-segment inference errors are recorded in `lid_error`.

Production runs default to compact prediction storage (`production_mode=true`,
`prediction_output_mode=compact`). The compact tables keep one row per valid segment per model with
`label_raw_1..k`, parsed `label_1..k` / `iso639_3_1..k` / `script_1..k`, `score_1..k`, `run_id`,
`inference_hash_buckets`, and `channel_hash_bucket`. Compact tables intentionally do **not** duplicate
`clean_text`; downstream consumers should join to `yt_lid_v3_segments_input` when text is needed. This
avoids both the row explosion of always materializing long top-k predictions and repeated text storage.

## 3. `audit_segments` is a manual fallback only

`glotlid_mode=audit_segments` restricts GlotLID to low-confidence OpenLID segments to save runtime. It is a
manual override and **must not** be used to estimate overall model-agreement rates (the subset is biased).
The default is `all_valid_segments`. In audit mode, GlotLID segment predictions are written for review but
are excluded from the main channel aggregation, agreement, consensus, mixed-language, Hindi/Indic, redirect,
and ablation paths.

## 3b. Resumable bucketed production runs

Every large staged table includes `run_id` and `channel_hash_bucket`. The default bucket range covers the
whole corpus (`inference_hash_buckets=4096`, `bucket_start=0`, `bucket_end=4095`), but production can rerun a
smaller bucket range with the same `run_id`. Writes use `replaceWhere` for the current run/bucket range, so a
failed bucket can be retried without rewriting completed buckets. The source channel/video tables are also
filtered to the active bucket range before counts and deduplication, so a partial retry avoids full-table
input scans.

Run-level QA summaries carry `inference_hash_buckets`, `bucket_start`, `bucket_end`, and
`is_full_bucket_range`. Full-range runs replace the matching full-range summary scope; partial runs replace
only the matching run/bucket summary scope. Global capped samples, such as
`yt_lid_v3_suspect_tail_audit_sample`, are emitted only for full-range runs.

The notebook writes `yt_lid_v3_run_progress` throughout the run. Each successful Delta write appends a
`delta_write_committed` marker with the target table and scope, and uncaught notebook failures attempt to
append a `notebook_failed` marker with the last recorded stage and exception summary. Progress logging is
best-effort and nonfatal; completed Delta writes remain the authoritative saved output for retrying a bucket.

Partition sizing is computed from the number of valid segments:
`ceil(valid_segments / target_segments_per_partition)`, clamped by `min_num_partitions` and
`max_num_partitions`. The notebook also sets `spark.sql.shuffle.partitions` to this effective value. When
the effective partition count exceeds the active bucket count, inference repartitions by
`channel_hash_bucket` plus `segment_id` so bucket cardinality does not cap map task parallelism.

### Larger-run guardrails

Validation and cohort runs should keep `videos_per_channel` positive. The default is `10`; setting
`videos_per_channel=0` means "all selected source videos" and now fails unless
`allow_unbounded_videos_per_channel=true` is also set. This prevents a validation run from accidentally
materializing the full video history for sampled channels. Subscriber-cohort driver runs apply the same cap
before writing cohort video scratch tables and forward the same guard to the child LID notebook.

`video_rank_column` and `video_rank_ascending` must describe the source table's
ordering semantics together. Timestamp columns normally use descending order.
YouTube uploads-playlist `position` uses `0` for the newest item, so runs that
set `video_rank_column=position` must also set
`video_rank_ascending=true`. The dual-sample builders enforce this explicitly;
omitting it would select the oldest collected videos when a cap is active.

Every run writes `yt_lid_v3_preflight_estimate` after segment validity is computed and before model
inference starts. It records the selected channel count, selected video count, total segment rows, expected
valid segments, and projected compact prediction rows for the enabled model configuration. Check this table
before committing a larger run to inference.

## 4. Letter-based validity thresholds

Validity is decided on **usable letters**, not whitespace-padded text length:

```
is_valid_text_for_lid =
    clean_letter_count >= min_clean_chars            # default 40
 OR (dominant_script is non-Latin
     AND clean_letter_count >= min_clean_chars_non_latin      # default 12
     AND dominant_script_share >= non_latin_dominant_script_share)   # default 0.60
```

Per-script letter counts are computed over Unicode letters after URL/digit/punctuation/symbol removal.
`clean_letter_count` is the total Unicode-letter count; scripts outside the eight named buckets fall into
`other` and remain eligible for the non-Latin exception (so Tamil/Telugu/Bengali/etc. are not dropped).

## 5. Segment-level architecture

Each text field is classified **separately** and aggregated to a channel label. This avoids the common
failure where an English channel name or boilerplate description swamps the real language of the videos.
A single concatenated-text classifier is intentionally avoided.

## 6. Vote shares and confidence

Channel labels come from weighted votes across segments. Default segment weights: `video_title=2.0`,
`channel_description=1.0`, `video_description=1.0`, `video_tags=0.5`, `channel_name=0.25`. Top-1 votes carry
full weight; admitted top-2 votes carry `secondary_label_vote_weight=0.20`. `primary_language_confidence`
(= `primary_language_vote_share_with_top2`) is a **vote share**, not a calibrated probability — do not
interpret it as `P(language)`. Length-weighting from the legacy pipeline is intentionally not applied in v3,
for cross-model comparability.

## 7. High-risk tail labels are flagged, not recoded

`HIGH_RISK_LATIN_TAIL_LABELS` (srd, ast, vec, gug, pap, …) are languages the models tend to hallucinate for
English/Hindi/major-language content. They are **flagged** (`*_primary_is_high_risk`, the high-risk redirect
diagnostic, and a `high_risk_tail_label_needs_review` consensus status) and never hard-recoded. A high-risk
label does not produce a clean consensus unless both models agree exactly and both have strong channel-level
evidence. That narrow exception emits `high_risk_tail_exact_agreement` with
`consensus_source='fasttext_tail_agreement'`; all other high-risk cases keep a NULL exact label and
`high_risk_tail_label_needs_review` for panel or manual adjudication.

## 8. Hindi/Indic audit fields are high-recall, not classification

`yt_lid_v3_hindi_indic_audit_candidates` exports Hindi/Indic candidates even when Hindi is not the primary or
secondary label, using Devanagari evidence, Hindi/Indic votes in either model's top-k, source fields, and
**romanized keyword** flags. Romanized keyword matching uses word-boundary/phrase matching (never substring)
and is a **recall-only audit signal** — it never feeds label assignment, vote weighting, or consensus.

## 9. Mixed-language: screen vs. credible candidate

- A **screen** is permissive (a secondary language is plausibly present).
- A **credible candidate** must clear the full evidence bar (secondary score ratio, secondary segment count
  and top-1 count, mean/max scores or cross-script evidence, rank2/rank3 margin, segment-type diversity or
  cross-script, not the same analysis cluster as the primary, and not a high-risk secondary without
  agreement).
- **Consensus** credibility requires second-model support by default
  (`mixed_credible_require_second_model_support=true`).

## 10. Consensus statuses (including intentional NULLs)

`consensus_status` is assigned by deterministic rules: `exact_model_agreement`,
`iso_or_script_variant_agreement`, `cluster_model_agreement`,
`taxonomy_normalized_agreement`, `high_risk_tail_exact_agreement`,
`openlid_high_confidence_glotlid_missing_or_error`, `glotlid_fallback_openlid_low_confidence`,
`high_risk_tail_label_needs_review`, `model_disagreement_needs_review`, `insufficient_text`. For ISO/script
variant agreement, cluster agreement, disagreement, and most high-risk review cases,
`consensus_language_label` is intentionally **NULL** — only a rollup cluster (`consensus_for_rollup_label`)
and/or `requires_manual_adjudication=true` are populated. A NULL exact label is a deliberate "do not assert a
single label here" signal, not missing data.

`consensus_source` records the tier that produced the current consensus, including
`fasttext_agreement`, `fasttext_tail_agreement`, `taxonomy_normalized`, `reconciliation_rule`,
`manual_adjudication_required`, and panel/human-review sources from the LLM adjudication workflow.

## 10b. LLM adjudication panel secrets

`03_language_llm_panel_databricks.py` writes request files for residual language-disagreement adjudication
or for a reproducible random validation sample, and can optionally submit OpenAI, Anthropic, Gemini, and
DeepSeek requests from Databricks. The notebook defaults to the shared project scope:

```text
secret_scope = youtube-llm-keys
openai_secret_key = openai-api-key
anthropic_secret_key = anthropic-api-key
gemini_secret_key = gemini-api-key
deepseek_secret_key = deepseek-api-key
```

Default model coverage by family and size/cost bracket:

```text
OpenAI:    gpt-5.5, gpt-5.4-mini, gpt-5.4-nano, gpt-5-nano
Anthropic: claude-opus-4-8, claude-sonnet-4-6, claude-haiku-4-5
Gemini:    gemini-3.1-pro-preview, gemini-3.5-flash, gemini-3.1-flash-lite
DeepSeek:  deepseek-v4-pro, deepseek-v4-flash
```

OpenAI, Anthropic, and Gemini use their provider batch APIs. DeepSeek uses the OpenAI-compatible
`https://api.deepseek.com/chat/completions` path directly and writes parser-compatible result JSONL under
`results_input_dir`.

For production final adjudication, route both LID disagreements and LID-unclassified channels to the LLM
fallback. Set `route_unclassified=true` and point `channels_table` at the final notebook-01 channel output,
not just `channel_model_comparison`: zero-valid channels may have no model-aggregation row and therefore may
be absent from the comparison table. When staged raw source tables are available, also set
`source_channels_table` and `source_videos_table`; these are used only for routed channels with no
`segments_input` rows, so DeepSeek still sees channel names/descriptions and recent video titles/descriptions.

The LLM prompt is intentionally less abstention-biased than the fastText validity gate. It still downweights
generic boilerplate, names, brands, URLs, and generic hashtags, but preserves repeated weak cues such as
non-generic hashtags, localized month/date strings, short repeated titles, and short script-specific snippets.
The current prompt version (`llm_fallback_final_guardrails_post_review_20260630`) labels short visible snippets as
`fasttext-ineligible-visible-text` rather than `lid-invalid`, adds structured summaries for short sentence
cues, coherent description prose, repeated patterns, CTA/channel boilerplate, romanized South Asian cues,
Arabic-script Urdu/Punjabi markers, and topic/language-name/region mentions. Each prompt also includes an
`EVIDENCE PRIORITY SUMMARY` that orders evidence quality before field weights: substantive non-boilerplate
description prose about the actual content/message, coherent title phrases, repeated non-generic phrases,
localized date/month cues, non-generic hashtags, channel name, then generic English/SEO/CTA/channel-about
boilerplate. The prompt explicitly forbids browsing or inferring from channel IDs, makes script choice follow
the highest-tier decisive evidence in both directions, treats language/region/translation labels as topic
metadata rather than primary evidence, prevents generic English about/contact/category text from overriding
repeated coherent native-script phrase evidence, rescues repeated short real English phrases as low-confidence
`eng_Latn`, and uses `confidence=null` for `insufficient_text`. It should reserve `insufficient_text` for
cases where language evidence is truly minimal, such as names-only, handles-only, topic-only, or
religious-icon-only metadata.
Imported raw model
results include both exact provider outputs and calibrated fields. When `apply_llm_calibration=true`, the
panel vote uses conservative `calibrated_*` fields: high-precision script-absent cases can be corrected to
romanized `_Latn`; compatible `*_Latn` predictions can be corrected to the dominant visible non-Latin script
when that script is more than half of cleaned script-bearing prompt text; Hindi-belt regional codes
(`bgc`, `bho`, `hne`, `mwr`, `raj`, `sck`) can be corrected to `hin_Deva`/`hin_Latn` unless the
running text contains genuine lect-specific lexical or phrase markers; and topic-only script-absent
cases can be changed to `insufficient_text`. Other lower-precision Arabic/Urdu, South Asian,
topic-only, and incompatible script-minority signals remain `review_*` flags rather than automatic
relabels.

The panel notebook writes `yt_lid_v3_llm_panel_run_progress` with run-scoped Delta commit markers and
best-effort `notebook_failed` markers. DeepSeek direct results append to existing result JSONL rather than
truncating it, and reruns skip request IDs that already have successful responses; import deduplication keeps
one vote per stored request.

## 10c. Initial LLM secrets/API validation run

Use this before any large residual-adjudication or category run. It classifies a reproducible random 1,000
channels from the June 9, 2026 TOO LID output family and checks whether each provider key and parser path
works end to end.

Set the source widgets to the actual June 9 prefix written by notebook 01:

```text
catalog = dev_sean
schema = matt
comparison_table = yt_lid_v3_too_full_20260609_channel_model_comparison
segments_input_table = yt_lid_v3_too_full_20260609_segments_input
channel_text_features_table = yt_lid_v3_too_full_20260609_channel_text_features
hindi_indic_audit_table = yt_lid_v3_too_full_20260609_hindi_indic_audit_candidates
run_id = too_full_20260609
inference_hash_buckets = 4096
```

Then set the validation controls:

```text
routing_mode = random_validation
random_validation_scope = all
random_validation_sample_size = 1000
random_validation_seed = 20260610
max_output_tokens = 2000
openai_reasoning_effort = none
openai_reasoning_effort_by_model_json = {"gpt-5-nano":"minimal"}
gemini_thinking_level = low
submit_batches = false
import_results = false
panel_majority_mode = reached_models
min_panel_votes_for_majority = 2
panel_requests_table = yt_lid_v3_too_full_20260609_llm_validation_requests
panel_batch_jobs_table = yt_lid_v3_too_full_20260609_llm_validation_batch_jobs
panel_raw_results_table = yt_lid_v3_too_full_20260609_llm_validation_raw_results
panel_verdicts_table = yt_lid_v3_too_full_20260609_llm_validation_verdicts
panel_model_agreement_table = yt_lid_v3_too_full_20260609_llm_validation_model_agreement
```

Recommended sequence:

1. Run once with `submit_batches=false` to materialize the request table and JSONL files. Confirm each model
   has exactly 1,000 request rows.
2. Set `submit_batches=true` for the provider/API smoke. OpenAI, Anthropic, and Gemini will create provider
   batch jobs; DeepSeek will run direct requests and write result JSONL immediately under `results_input_dir`.
   Submit retries default to `reuse_existing_requests_on_submit=true`: if request rows already exist for
   the `run_id`, the notebook resubmits from those exact stored prompt rows instead of rebuilding prompts.
   This keeps all provider responses comparable within a run. Use a new `run_id` for prompt/cleaning changes.
3. Check provider batch status before import. The helper notebook
   `.codex_databricks/check_lid_llm_batch_status_20260610.py` writes the latest OpenAI/Anthropic/Gemini status
   snapshot to `yt_lid_v3_too_full_20260609_llm_validation_batch_status_check`.
4. Download completed OpenAI/Anthropic/Gemini result files with
   `.codex_databricks/download_lid_llm_batch_results_20260610.py`. It writes JSONL under
   `results_input_dir/<run_id>/<provider>/<model>/` and records file counts in
   `yt_lid_v3_too_full_20260609_llm_validation_result_files`.
5. Rerun `03_language_llm_panel_databricks.py` with `submit_batches=false` and `import_results=true`.
   Import reads `results_input_dir/<run_id>` when present, then writes raw parsed predictions, verdicts,
   and the all-model pairwise agreement matrix.
   The import path defaults to `reuse_existing_requests_on_import=true`; when request rows already exist
   for the `run_id`, the notebook reuses those rows instead of rewriting prompts or batch JSONL. This avoids
   mixing provider responses from an already-submitted prompt with a later local prompt edit. Set the reuse
   switches to `false` only when intentionally rebuilding a run before any provider submission has happened.
6. Review `yt_lid_v3_too_full_20260609_llm_validation_model_agreement` for all model-pair agreement and
   `yt_lid_v3_too_full_20260609_llm_validation_verdicts` for majority coverage and panel-vs-fastText
   top-line agreement.
7. For a visual agreement readout that includes OpenLID and GlotLID alongside the imported LLM models, run
   `.codex_databricks/inspect_lid_llm_hardcase_agreement_visual_20260611.py` and render the exported JSON
   with `.codex_databricks/render_lid_llm_agreement_heatmap_20260611.py`. The helper reports normalized
   base-ISO agreement and leaves missing provider results out of the matrix rather than imputing them.

For a harder validation pass focused on likely adjudication value rather than smoke testing, keep
`routing_mode=random_validation` and set `random_validation_scope=lid_iso_disagreement`. That scope samples
only channels where OpenLID and GlotLID have non-null, different primary ISO labels after the same
project-level normalization used by panel agreement (`zho`→`cmn`, `ku`/`kur`→`kmr`, Arabic dialects→`ara`,
etc.). The notebook
fails early if the requested sample is larger than the eligible hard-case universe for the selected source
run.

The language panel asks models for compact one-line JSON and defaults to `max_output_tokens=2000`,
`openai_reasoning_effort=none`, `gemini_thinking_level=low`, `deepseek_thinking_type=disabled`,
and `deepseek_max_output_tokens=600`. To test DeepSeek thinking mode, set
`deepseek_thinking_type=enabled` and `deepseek_reasoning_effort` to one of
`low`, `medium`, `high`, `max`, or `xhigh`; DeepSeek currently accepts `low`/`medium` only as
compatibility aliases that map to `high`, and `xhigh` maps to `max`. Thinking-enabled DeepSeek runs must
set `deepseek_max_output_tokens>=2000` for Flash-only tests and `>=4000` whenever Pro is selected;
reasoning tokens share the cap, and lower caps truncated final JSON on the June 11 low-thinking tests
(600 tokens failed badly; 2,000 still truncated many Pro rows). The larger global cap is intentional:
reasoning/thinking tokens
share the output budget on some providers, and the June 10 validation showed that Gemini frequently
truncated JSON at lower caps, reducing valid parsed votes even though the batch jobs themselves
succeeded. DeepSeek is handled separately because it runs through direct synchronous calls rather than a
provider batch API; keeping thinking disabled and capping its classification response avoids hidden
reasoning output dominating latency/cost while preserving enough budget for the required JSON.
OpenAI batch models currently use `none` rather than `minimal` for thinking off; some newer batch aliases
reject `minimal` with `unsupported_value`. The older `gpt-5-nano` batch alias is the exception observed in
the June 11 hard-case run: it rejects `none` and requires the lowest supported effort, `minimal`, so keep
the `openai_reasoning_effort_by_model_json` override unless that alias is retired.

DeepSeek direct result JSONL now includes `_deepseek_direct_metadata` with per-request duration,
attempt count, status codes, transport, thinking setting, reasoning-effort setting, and max-token setting. Use this metadata for
runtime diagnosis before inferring provider slowness from total job wall time. The focused DeepSeek
retry notebook also normalizes stale request JSONL at call time, so older request files with omitted
thinking controls or the global 2,000-token cap will still submit with the current DeepSeek defaults.

The June 11 hard-case disagreement audit showed that the highest model splits were driven mainly by
romanized South Asian close varieties, repeated English description templates, and mixed-script titles
where generic English media scaffolding competed with the actual title phrase. The prompt now explicitly
downweights mixed-script title scaffolding such as `ASMR`, `MUKBANG`, `Official Video`, `Lyrics`, and
series labels, and the prompt cleaner strips those generic title terms before request materialization.
The four-round audit over the top 120 hard-case splits added broader media-shell stripping for terms such
as `trailer`, `teaser`, `full movie`, `audio jukebox`, `visualizer`, `cover`, `remix`, `recipe`, `status`,
and `dance/choreo`, and added a translated-title guard for cases where a recurring non-English source title
is followed by an English gloss after a colon or pipe.
The next four 30-channel passes over ranks 121-240 found additional leakage from music-credit descriptions
(`Stream ... via`, `Performed by`, `Produced & Written by`, `Mixed and Mastered`, `Video Edited`, etc.) and
format/audience title shells (`fancam`, `behind`, `performance ver.`, `full episode`, `promo`, `review`,
`reaction`, `gameplay`, `cartoon`, `nursery rhymes`, `toy`). Those are now stripped or explicitly
downweighted before classification.
The ranks 361-480 pass mostly contained single-model outliers, but it surfaced repeated multilingual
social/download/booking boilerplate in descriptions (`More socials`, `Redes Sociales`, `Folgt uns`,
`Segui`, `Suis-moi`, `Channel abonnieren`, `Bookings via`, etc.). These patterns are now stripped only from
description fields so title text is not over-cleaned.
The cleaner also treats section headers such as `Related Tag :-` as the start of a query/tag block, so
the following SEO list does not enter the prompt. Parser normalization now prefers a complete
`primary_language_label` (`iso_Script`) over conflicting component fields when a model emits inconsistent
JSON, because the full label is the constrained field requested by the prompt. Malformed classified labels
such as `hmo?`, label-like ISO components such as `hye_latn`, or invalid script strings no longer count as
valid panel votes after alias normalization. Non-language outputs such as `und`, `zxx`, and `mul` are treated
as abstentions/null labels rather than classified panel votes.

Panel reconciliation now keeps both raw and normalized language judgments. Raw model labels are preserved,
but majority status defaults to `panel_majority_vote_basis=normalized_base_iso`, which collapses known
project-level taxonomy aliases (`zho`→`cmn`, `tgl`→`fil`, `ori`→`ory`, `uzn`→`uzb`, `msa`→`zsm`,
`nep`→`npi`, `ku`/`kur`→`kmr`, Arabic dialects→`ara`)
and treats Chinese `Hans`/`Hant`/`Hani` as the same script family for agreement diagnostics. The raw
agreement matrix still reports exact base-ISO and full-label agreement, and additionally reports normalized
base-ISO and normalized-label agreement. Use normalized base-language disagreement for human-review routing;
treat raw script/taxonomy-only splits as QA/taxonomy instability unless the exact script is itself the
question.

Prompt construction defaults to `strip_prompt_boilerplate=true` and `dedupe_prompt_segments=true`. The
panel prompt lists video titles before descriptions, removes common provider/release/link boilerplate such
as auto-generated music metadata, copyright/fair-use boilerplate, production-credit boilerplate
(`Presenting the new drama`, cast/script/producer/DOP/BGM lines), support/download/social boilerplate, and
generic URLs. It also strips common contact/donation boilerplate (`UPI ID`, email, WhatsApp/contact/support
lines) and warns models not to treat liturgical/proper-name-only strings (`Gita`, `Darbar`, `Puje`,
`Bhagavatha`, `Matha`, `Pravachana`, etc.) as decisive language evidence without grammatical connective text.
The prompt and parser also normalize common provider mistakes surfaced in hardcase validation: language-name
outputs such as `hindi_Deva` are mapped back to ISO (`hin_Deva`), script aliases such as `Hangul` are mapped
to ISO 15924 (`Hang`), and South Asian close-variety traps are called out explicitly (`pnb` vs `pan`,
`hne` Chhattisgarhi vs `hif` Fiji Hindi, and Bhojpuri/Magahi hashtag conflicts).
Music/link boilerplate such as `listen here`, `stream on`, `discover similar songs`, `pre-save link`,
`we are on`, `shop`, booking blocks, and artist follow lines is stripped before prompt assembly. Query/tag
sections such as `Related Tags`, `Your query solved`, `search terms`, and `keywords` stop the description
cleaner because later lines are usually SEO lists rather than channel-language evidence. The prompt warns
against letting English SEO-template words (`lyrics`, `recipe`, `mukbang`, `ASMR`, `official video`, etc.)
dominate repeated non-English phrase text.
The prompt collapses exact and near-duplicate segment text, normalizing volatile episode/part/day/quote
numbers in the dedupe key so repeated template descriptions count once instead of multiplying English
boilerplate weight. It includes compact field, segment-script, text-script, and
non-decisive language-hint summaries for candidate segments after cleanup. Prompt caps preserve representative
non-Latin-script examples before filling with the highest-priority remaining text. Short fields that failed
the fastText validity threshold are still shown with diagnostics because repeated short titles can be decisive
evidence for an LLM.

Panel verdicts include the winning vote count plus `panel_second_votes`, `panel_vote_margin`,
`n_distinct_panel_vote_iso`, and `panel_vote_distribution_json`. Use those columns to route close or genuinely
mixed channels to review without re-joining the raw model-result table.

## 11. GlotLID preprocessing caveat

The main GlotLID pass uses `match_openlid` preprocessing (the shared `clean_text`) so the comparison is
apples-to-apples. GlotLID is trained on lightly normalized, case/script-preserving text; an optional
**native-preprocessing audit** can be produced (`glotlid_preprocessing_mode=glotlid_native_audit` or
`glotlid_native_audit_sample_fraction>0`) and is written to a **separate** compact table
(`yt_lid_v3_glotlid_native_predictions_compact`). Native-preprocessed predictions are never mixed into the
main comparison. A full native audit over all valid segments now requires `allow_full_native_audit=true`;
otherwise the notebook fails fast unless `0 < glotlid_native_audit_sample_fraction < 1`.

## 12. QA, validation, and ablation outputs

Saved Delta tables are full for their configured run/bucket range. The deliberately partial outputs are
long-format segment audits when `prediction_output_mode=long_sample` and the global QA samples when they are
explicitly enabled. In compact mode, current-run/bucket rows in compatible legacy long segment tables are
cleared; incompatible pre-refactor long tables are left untouched with a warning rather than table-wide
overwritten. Displays are disabled by default in production. Outputs:

| Table | Contents |
|---|---|
| `yt_lid_v3_segments_input` | Canonical segments + script metrics + validity + run/bucket metadata |
| `yt_lid_v3_openlid_predictions_compact` / `yt_lid_v3_glotlid_predictions_compact` | Compact top-k predictions, one row per valid segment per model |
| `yt_lid_v3_openlid_segments` / `yt_lid_v3_glotlid_segments` | Optional long-format top-k predictions when `prediction_output_mode=long_sample` or `long_full` |
| `yt_lid_v3_glotlid_native_predictions_compact` / `yt_lid_v3_glotlid_native_segments` | Optional native-preprocessing audit, compact by default and long only when requested |
| `yt_lid_v3_channel_text_features` | Per-channel script, keyword, validity, and sample-text features reused by diagnostics |
| `yt_lid_v3_channel_votes` | Per-(channel, language) weighted votes, `lid_model` column |
| `yt_lid_v3_channel_model_aggregation` | Per-model channel summary (intermediate) |
| `yt_lid_v3_segment_model_comparison` / `yt_lid_v3_channel_model_comparison` | Model comparison + consensus |
| `yt_lid_v3_channels` | Final channel table (legacy + `openlid_*`/`glotlid_*` + consensus fields) |
| `yt_lid_v3_mixed_language_candidates` | Screen vs. credible flags + rejection reason |
| `yt_lid_v3_hindi_indic_audit_candidates` | Hindi/Indic recall audit |
| `yt_lid_v3_high_risk_redirect_diagnostic` | High-risk tail-label redirect signals, scoped by run/bucket metadata |
| `yt_lid_v3_language_summary_full` / `_rollup` | Exact-label and rollup summaries, scoped by run/bucket metadata |
| `yt_lid_v3_model_agreement_summary` | Exact/ISO/cluster agreement rates, scoped by run/bucket metadata |
| `yt_lid_v3_suspect_tail_audit_sample` | ≤50 channels per high-risk label for full-range runs |
| `yt_lid_v3_manual_validation_sample` | Deterministic stratified validation sample for full-range runs |
| `yt_lid_v3_unclassified_audit` | Text-sparse / invalid-text channels |
| `yt_lid_v3_source_language_confusion` | Source-vs-model disagreement patterns, scoped by run/bucket metadata |
| `yt_lid_v3_dedupe_qa` | Dedup and pipeline row counts; exact raw before/duplicate-key counts are populated when heavy QA is enabled, scoped by run/bucket metadata |
| `yt_lid_v3_preflight_estimate` | Pre-inference channel/video/segment fanout and projected compact prediction rows, scoped by run/bucket metadata |
| `yt_lid_v3_ablation_summary` | Per-config counts + primary-label churn, scoped by run/bucket metadata |

Notebook displays, validation samples, ablation, exact raw source before-counts, expensive duplicate-key
counts, and the full cross-model segment-id parity join are disabled by default in production mode. Set
`run_heavy_qa=true` plus the relevant explicit widget when a full QA notebook run is needed.

The **manual validation sample** is deterministic (seeded by `validation_sample_seed` + stratum), stratified
across high-confidence, low-confidence, credible/screen mixed, high-risk, Hindi/Indic, source disagreement,
exact/cluster model disagreement, insufficient-text, and a non-Latin control; each channel keeps all
qualifying strata in an array with one primary stratum assigned by fixed priority.
It is enabled by default because it is capped per stratum and cheap relative to inference. Ablations and
heavy QA remain opt-in for full production runs.

Validation-analysis figures should be treated as table artifacts, not filesystem artifacts. The companion
analysis notebook writes SVG text into `{output_prefix}_analysis_figures_svg`; optional DBFS/Volume copies
may be enabled only when the caller supplies a known-writable path.

The **ablation summary** re-aggregates from compact stored predictions (no re-inference) for the configs in
§15 of the spec and reports primary-label churn vs. both the v3 default OpenLID and v3 default consensus.
Caveat: because inference ran only on the `min_clean_chars=40` valid universe, character-threshold ablations
can only restrict further (e.g. 50), and `v1_legacy_like_openlid` approximates legacy weights on that
universe.

## 13. Determinism and source-table safety

Channel and video deduplication is deterministic (`row_number()` over timestamp → row-hash → key ordering;
never `.dropDuplicates()`), and the smoke-test sample is a deterministic `xxhash64(channel_id)` order (never
`.limit()` on unordered data). The notebook does **not** modify `yt_sl_channels.detected_language` unless
`update_source_detected_language=true` is set after validation. Even when that flag is enabled, only
classified rows with a non-null consensus exact label and `requires_manual_adjudication=false` are eligible
for write-back; review and mixed-language cases remain audit-only.

## 14. License and model-binary cautions

- OpenLID-v3 is distributed under GPL-3.0; review license implications before redistributing the binary.
- The model binaries are downloaded from Hugging Face (`HPLT/OpenLID-v3`, `cis-lmu/glotlid`) when missing.
  Production defaults point at uploaded Databricks Volume binaries:
  `/Volumes/dev_sean/matt/models/openlid-v3.bin` and `/Volumes/dev_sean/matt/models/glotlid.bin`. For
  air-gapped clusters, upload model binaries and set `download_model_if_missing=false`. The notebook fails
  clearly if a model is enabled but unavailable.
- Collapse model outputs to a project-level language taxonomy before publication; do not treat raw
  ISO/script labels (especially high-risk tail labels and macro/near-language clusters) as final.
