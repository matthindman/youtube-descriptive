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
diagnostics are computed over an unbiased shared set rather than a low-confidence subset. The notebook
asserts that the two models' valid `segment_id` universes are identical except for explicit per-segment
inference errors recorded in `lid_error`.

## 3. `audit_segments` is a manual fallback only

`glotlid_mode=audit_segments` restricts GlotLID to low-confidence OpenLID segments to save runtime. It is a
manual override and **must not** be used to estimate overall model-agreement rates (the subset is biased).
The default is `all_valid_segments`. In audit mode, GlotLID segment predictions are written for review but
are excluded from the main channel aggregation, agreement, consensus, mixed-language, Hindi/Indic, redirect,
and ablation paths.

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
`glotlid_native_audit_sample_fraction>0`) and is written to a **separate** table
(`yt_lid_v3_glotlid_native_segments`). Native-preprocessed predictions are never mixed into the main
comparison.

## 12. QA, validation, and ablation outputs

All saved Delta tables are full (no `.limit(100)`); only displays are truncated. Outputs:

| Table | Contents |
|---|---|
| `yt_lid_v3_segments_input` | Canonical segments + script metrics + validity |
| `yt_lid_v3_openlid_segments` / `yt_lid_v3_glotlid_segments` | Long-format top-k predictions per model |
| `yt_lid_v3_glotlid_native_segments` | Optional native-preprocessing audit |
| `yt_lid_v3_channel_votes` | Per-(channel, language) weighted votes, `lid_model` column |
| `yt_lid_v3_channel_model_aggregation` | Per-model channel summary (intermediate) |
| `yt_lid_v3_segment_model_comparison` / `yt_lid_v3_channel_model_comparison` | Model comparison + consensus |
| `yt_lid_v3_channels` | Final channel table (legacy + `openlid_*`/`glotlid_*` + consensus fields) |
| `yt_lid_v3_mixed_language_candidates` | Screen vs. credible flags + rejection reason |
| `yt_lid_v3_hindi_indic_audit_candidates` | Hindi/Indic recall audit |
| `yt_lid_v3_high_risk_redirect_diagnostic` | High-risk tail-label redirect signals |
| `yt_lid_v3_language_summary_full` / `_rollup` | Exact-label and rollup summaries |
| `yt_lid_v3_model_agreement_summary` | Exact/ISO/cluster agreement rates |
| `yt_lid_v3_suspect_tail_audit_sample` | ≤50 channels per high-risk label |
| `yt_lid_v3_manual_validation_sample` | Deterministic stratified validation sample |
| `yt_lid_v3_unclassified_audit` | Text-sparse / invalid-text channels |
| `yt_lid_v3_source_language_confusion` | Source-vs-model disagreement patterns |
| `yt_lid_v3_dedupe_qa` | Dedup before/after counts, duplicate key groups, and post-sampling pipeline counts |
| `yt_lid_v3_ablation_summary` | Per-config counts + primary-label churn |

The **manual validation sample** is deterministic (seeded by `validation_sample_seed` + stratum), stratified
across high-confidence, low-confidence, credible/screen mixed, high-risk, Hindi/Indic, source disagreement,
exact/cluster model disagreement, insufficient-text, and a non-Latin control; each channel keeps all
qualifying strata in an array with one primary stratum assigned by fixed priority.

The **ablation summary** re-aggregates from stored predictions (no re-inference) for the configs in §15 of
the spec and reports primary-label churn vs. both the v3 default OpenLID and v3 default consensus. Caveat:
because inference ran only on the `min_clean_chars=40` valid universe, character-threshold ablations can only
restrict further (e.g. 50), and `v1_legacy_like_openlid` approximates legacy weights on that universe.

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
