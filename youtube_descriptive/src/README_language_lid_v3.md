# README: YouTube language classification v3 (OpenLID-v3 + GlotLID) on Databricks

`01_language_openlid_v3_databricks.py` classifies YouTube channel language by running **two** fastText
language-ID models — OpenLID-v3 (the legacy primary detector) and GlotLID — on the **same universe of
valid text segments**, then comparing them and producing model-specific and consensus labels. It supersedes
the single-model first cut documented in `README_language_openlid_v3.md`.

## Runtime requirements

Requires **Databricks Runtime 13.0+ (Apache Spark 3.4+)**. The channel aggregation and validation-sampling
steps use `array_sort` with a comparator and `array_compact`, both introduced in Spark 3.4; the notebook
asserts the Spark version up front and fails clearly on older runtimes. Notebook-scoped Python deps
(`numpy<2`, `fasttext`, `huggingface-hub`, `regex`, `pandas`, `pyarrow`) are installed by the first cell.

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

Partition sizing is computed from the number of valid segments:
`ceil(valid_segments / target_segments_per_partition)`, clamped by `min_num_partitions` and
`max_num_partitions`. The notebook also sets `spark.sql.shuffle.partitions` to this effective value. When
the effective partition count exceeds the active bucket count, inference repartitions by
`channel_hash_bucket` plus `segment_id` so bucket cardinality does not cap map task parallelism.

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
label does not produce a clean consensus. The exact label remains NULL unless both models agree exactly and
both have strong channel-level evidence; even then the channel remains marked
`high_risk_tail_label_needs_review` for manual adjudication.

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
`openlid_high_confidence_glotlid_missing_or_error`, `glotlid_fallback_openlid_low_confidence`,
`high_risk_tail_label_needs_review`, `model_disagreement_needs_review`, `insufficient_text`. For ISO/script
variant agreement, cluster agreement, disagreement, and most high-risk review cases,
`consensus_language_label` is intentionally **NULL** — only a rollup cluster (`consensus_for_rollup_label`)
and/or `requires_manual_adjudication=true` are populated. A NULL exact label is a deliberate "do not assert a
single label here" signal, not missing data.

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
| `yt_lid_v3_ablation_summary` | Per-config counts + primary-label churn, scoped by run/bucket metadata |

Notebook displays, validation samples, ablation, exact raw source before-counts, expensive duplicate-key
counts, and the full cross-model segment-id parity join are disabled by default in production mode. Set
`run_heavy_qa=true` plus the relevant explicit widget when a full QA notebook run is needed.

The **manual validation sample** is deterministic (seeded by `validation_sample_seed` + stratum), stratified
across high-confidence, low-confidence, credible/screen mixed, high-risk, Hindi/Indic, source disagreement,
exact/cluster model disagreement, insufficient-text, and a non-Latin control; each channel keeps all
qualifying strata in an array with one primary stratum assigned by fixed priority.

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
- The model binaries are downloaded from Hugging Face (`HPLT/OpenLID-v3`, `cis-lmu/glotlid`); for air-gapped
  clusters upload them and set `download_model_if_missing=false`. The notebook fails clearly if a model is
  enabled but unavailable.
- Collapse model outputs to a project-level language taxonomy before publication; do not treat raw
  ISO/script labels (especially high-risk tail labels and macro/near-language clusters) as final.
