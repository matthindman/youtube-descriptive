# Codex specification v3: revise `01_language_openlid_v3_databricks.py` for the next language-classification run

## Status

This v3 spec supersedes the prior v2 draft. The v2 draft is mostly sound, but it makes one strategically wrong design choice: it changes the GlotLID default to `audit_segments`. For the next run, GlotLID must run on **all valid segments by default**, so model agreement, disagreement, fallback, and Hindi-recall diagnostics are computed over the full same segment universe rather than a biased subset.

Keep the useful v2 improvements: deterministic deduplication, stricter text thresholds, letter-based script metrics, full QA tables, high-risk tail-label flags, screen-versus-credible mixed-language distinction, consensus-status fields, Hindi audit candidates, manual validation sampling, and ablation summaries. Revise the sections below.

---

## 1. Core objective

Revise the Databricks notebook into a v2/v3 language-identification pipeline that:

1. Preserves OpenLID-v3 as the legacy primary metadata-language detector.
2. Runs GlotLID on the same **all-valid-segments** universe by default.
3. Saves model-specific segment predictions, model-specific channel aggregations, and model-comparison/consensus fields.
4. Reduces short-text false positives and tail-label hallucinations.
5. Adds Hindi/Indic recall diagnostics that do not depend on Hindi being the primary label.
6. Distinguishes loose mixed-language screens from credible mixed-language candidates.
7. Produces full QA, robustness, and validation-sampling outputs.
8. Does not overwrite `yt_sl_channels.detected_language` unless explicitly enabled after validation.

The output should be interpreted as **written metadata language**, not spoken/video language. Preserve source language fields as audit fields, not ground truth.

---

## 2. Required output tables

Use a new output family. Add widgets so every table name can be overridden.

```text
prod_tads.youtube.yt_lid_v3_segments_input
prod_tads.youtube.yt_lid_v3_openlid_segments
prod_tads.youtube.yt_lid_v3_glotlid_segments
prod_tads.youtube.yt_lid_v3_glotlid_native_segments          # optional, only if glotlid_preprocessing_mode/native audit is run
prod_tads.youtube.yt_lid_v3_segment_model_comparison
prod_tads.youtube.yt_lid_v3_channel_votes                    # one table with lid_model column
prod_tads.youtube.yt_lid_v3_channel_model_comparison
prod_tads.youtube.yt_lid_v3_channels
prod_tads.youtube.yt_lid_v3_language_summary_full
prod_tads.youtube.yt_lid_v3_language_summary_rollup
prod_tads.youtube.yt_lid_v3_model_agreement_summary
prod_tads.youtube.yt_lid_v3_mixed_language_candidates
prod_tads.youtube.yt_lid_v3_hindi_indic_audit_candidates
prod_tads.youtube.yt_lid_v3_suspect_tail_audit_sample
prod_tads.youtube.yt_lid_v3_high_risk_redirect_diagnostic
prod_tads.youtube.yt_lid_v3_manual_validation_sample
prod_tads.youtube.yt_lid_v3_unclassified_audit
prod_tads.youtube.yt_lid_v3_source_language_confusion
prod_tads.youtube.yt_lid_v3_dedupe_qa
prod_tads.youtube.yt_lid_v3_ablation_summary
```

If maintaining `v2` names is operationally easier, keep the `v2` table prefix but implement this v3 logic. The important point is to avoid overwriting the existing first-pass OpenLID-v3 tables.

---

## 3. Widget defaults

Replace the relevant defaults with the following.

```python
_create_text_widget("min_clean_chars", "40")
_create_text_widget("min_clean_chars_non_latin", "12")
_create_text_widget("non_latin_dominant_script_share", "0.60")
_create_text_widget("top_k", "5")

_create_text_widget("enable_openlid", "true")
_create_text_widget("enable_glotlid", "true")
# HARD DEFAULT FOR NEXT RUN: full GlotLID coverage.
_create_text_widget("glotlid_mode", "all_valid_segments")  # disabled, audit_segments, all_valid_segments
_create_text_widget("glotlid_preprocessing_mode", "match_openlid")  # match_openlid or glotlid_native_audit
_create_text_widget("glotlid_native_audit_sample_fraction", "0.00")  # 0 disables native audit; use sample only unless explicitly scheduled

_create_text_widget("primary_min_score", "0.20")
_create_text_widget("secondary_min_score", "0.35")
_create_text_widget("secondary_min_score_ratio", "0.50")
_create_text_widget("secondary_label_vote_weight", "0.20")

_create_text_widget("mixed_screen_ratio_threshold", "0.40")
_create_text_widget("mixed_screen_min_secondary_segments", "2")
_create_text_widget("mixed_credible_ratio_threshold", "0.50")
_create_text_widget("mixed_credible_min_secondary_segments", "3")
_create_text_widget("mixed_credible_min_secondary_top1_segments", "1")
_create_text_widget("mixed_credible_min_secondary_segment_types", "2")
_create_text_widget("mixed_credible_min_rank2_rank3_margin_ratio", "0.25")
_create_text_widget("mixed_credible_secondary_mean_score", "0.45")
_create_text_widget("mixed_credible_secondary_max_score", "0.70")
_create_text_widget("mixed_credible_require_second_model_support", "true")

_create_text_widget("channel_description_weight", "1.00")
_create_text_widget("video_title_weight", "2.00")
_create_text_widget("video_description_weight", "1.00")
_create_text_widget("video_tags_weight", "0.50")
_create_text_widget("channel_name_weight", "0.25")

_create_text_widget("run_ablation_aggregations", "true")
_create_text_widget("create_validation_samples", "true")
_create_text_widget("validation_sample_seed", "20260520")
_create_text_widget("validation_max_per_stratum", "100")
_create_text_widget("validation_min_per_stratum", "30")
```

Notes:

- `glotlid_mode="all_valid_segments"` is mandatory as the default. `audit_segments` remains available only as a manual cost-saving override.
- Because GlotLID runs on all valid segments, model-agreement, consensus, Hindi/Indic audit, and high-risk redirect diagnostics must not be selection-biased toward low-confidence OpenLID cases.
- `glotlid_preprocessing_mode="match_openlid"` is the default for comparability. A `glotlid_native_audit` mode may be added as an optional sample/audit run, but native-preprocessing GlotLID outputs should not be silently mixed into the main consensus table.

---

## 4. Deterministic source deduplication and smoke-test sampling

### 4.1 Channels

Before segment construction, deduplicate `channels_raw` deterministically.

1. Check uniqueness of `channel_id`.
2. Select one row per channel using `row_number()` over a deterministic ordering.
3. Order by:
   - best available timestamp descending, using the first existing column from:

```python
["updated_at", "modified_at", "ingestion_timestamp", "created_at", "capture_date", "first_capture_time"]
```

   - then by `sha2(to_json(struct(*all_columns)), 256)` ascending;
   - then by `channel_id` ascending.
4. Do **not** use `.dropDuplicates()` as the row-selection method, even when no timestamp exists. If no timestamp exists, use the hash ordering alone.
5. Write `yt_lid_v3_dedupe_qa` with before/after counts, duplicate-channel counts, and chosen timestamp column.

Define the canonical `all_channels` universe only from the post-dedup channel table.

### 4.2 Videos

Deduplicate `videos_raw` deterministically.

1. If `video_id` exists, partition by `video_id` and select by timestamp desc, row hash asc, `video_id` asc.
2. If `video_id` does not exist, partition by `channel_id`, `video_title`, `video_description`, and any available timestamp/rank fields; then select deterministically.
3. Record before/after counts in `yt_lid_v3_dedupe_qa`.

### 4.3 Smoke-test channel selection

If `limit_channels > 0`, never use `.limit()` on an unordered DataFrame. Select the first `limit_channels` channels after ordering by a deterministic hash such as:

```python
xxhash64(channel_id) ASC
```

Run the deterministic limit after channel deduplication. This makes the 10,000-channel smoke test reproducible.

---

## 5. Segment construction and preprocessing

Create and save a canonical `yt_lid_v3_segments_input` table before inference. Both OpenLID and GlotLID must read from this same table in the default run.

Required fields:

```text
channel_id
video_id
segment_id
segment_type
text
raw_text_len
clean_text
clean_text_len
clean_letter_count
clean_token_count
dominant_script
dominant_script_share
latin_char_count
devanagari_char_count
arabic_char_count
cyrillic_char_count
han_char_count
kana_char_count
hangul_char_count
thai_char_count
has_url
has_hashtag
has_emoji_or_symbol
is_valid_text_latin_rule
is_valid_text_non_latin_rule
is_valid_text_for_lid
short_text_reason
```

Define validity using **usable letters**, not whitespace-padded text length:

```text
is_valid_text_for_lid =
    clean_letter_count >= min_clean_chars
 OR (
      dominant_script is non-Latin
      AND clean_letter_count >= min_clean_chars_non_latin
      AND dominant_script_share >= non_latin_dominant_script_share
    )
```

Script metrics:

- Compute per-script counts over Unicode letters only after URL stripping, digit removal, punctuation removal, and symbol/emoji removal.
- Use Unicode script properties via the `regex` module where possible.
- `clean_letter_count` is the sum of per-script letter counts.
- `dominant_script_share = dominant_script_letter_count / clean_letter_count`.
- If `clean_letter_count == 0`, mark the segment invalid.
- Retain `clean_text_len` for diagnostics, but do not use it as the core validity threshold.

---

## 6. Run OpenLID and GlotLID on the same valid segment universe

### 6.1 OpenLID

Run OpenLID-v3 on all rows from `yt_lid_v3_segments_input` where `is_valid_text_for_lid=true`. Write `yt_lid_v3_openlid_segments`.

### 6.2 GlotLID

Run GlotLID on all rows from `yt_lid_v3_segments_input` where `is_valid_text_for_lid=true` when:

```text
enable_glotlid=true AND glotlid_mode="all_valid_segments"
```

Write `yt_lid_v3_glotlid_segments`.

Acceptance condition: the set of valid `segment_id`s in `yt_lid_v3_glotlid_segments` must equal the set in `yt_lid_v3_openlid_segments`, except for explicit per-segment inference errors recorded in `lid_error`.

### 6.3 Optional audit mode

Keep `glotlid_mode="audit_segments"` only as a manual override for emergency runtime constraints. Do not make it the default and do not use audit-only GlotLID outputs to estimate overall model agreement.

### 6.4 Optional native preprocessing audit

If `glotlid_preprocessing_mode="glotlid_native_audit"`, write separate outputs to `yt_lid_v3_glotlid_native_segments`. Do not mix native-preprocessed predictions into the main `yt_lid_v3_segment_model_comparison` unless a separate flag makes the mode explicit. The main comparison should use the same canonical segment-validity universe.

---

## 7. Label normalization and long-format predictions

Implement a robust parser:

```python
def parse_lid_label(raw_label: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Return (normalized_label, iso639_3, script).
    Handles:
    - __label__eng_Latn
    - eng_Latn
    - eng
    - malformed / missing labels
    """
```

Store raw and normalized labels for all top-k outputs.

Then convert each model’s top-k output to long format with:

```text
channel_id
video_id
segment_id
segment_type
lid_model
prediction_rank
label_raw
label
iso639_3
script
score
score_1
clean_letter_count
clean_text_len
dominant_script
is_valid_text_for_lid
```

Do not discard labels lacking a parsed script; set `script=NULL` and retain them for QA unless they are known noise/unknown labels.

---

## 8. Model-specific channel aggregation

Aggregate OpenLID and GlotLID separately using the same aggregation function. Save both sets of votes in `yt_lid_v3_channel_votes` with a `lid_model` column.

Top-1 votes are admitted only if:

```text
prediction_rank = 1
score >= primary_min_score
is_valid_text_for_lid = true
```

Top-2 votes are admitted only if:

```text
prediction_rank = 2
score >= secondary_min_score
score / score_1 >= secondary_min_score_ratio
is_valid_text_for_lid = true
```

Default weights:

```text
secondary_label_vote_weight = 0.20
channel_description_weight = 1.00
video_title_weight = 2.00
video_description_weight = 1.00
video_tags_weight = 0.50
channel_name_weight = 0.25
```

For each model, compute:

```text
primary_language_label
primary_language_iso639_3
primary_language_script
primary_language_score
primary_language_vote_share_with_top2
primary_language_top1_vote_share
primary_language_top1_score
secondary_language_label
secondary_language_iso639_3
secondary_language_script
secondary_language_score
secondary_to_primary_score_ratio
rank2_language_score
rank3_language_score
rank2_rank3_margin
rank2_rank3_margin_ratio
valid_language_segment_count
valid_language_segment_type_count
total_clean_letter_count
mean_segment_score_primary
max_segment_score_primary
primary_language_top1_segment_count
primary_language_top2_segment_count
language_votes_json
```

Keep the legacy OpenLID field names for backward compatibility in `yt_lid_v3_channels`, but add explicitly prefixed fields:

```text
openlid_primary_language_label
openlid_secondary_language_label
glotlid_primary_language_label
glotlid_secondary_language_label
```

---

## 9. Taxonomy, high-risk labels, and review categories

Separate three concepts that were previously conflated.

### 9.1 High-risk Latin tail labels

```python
HIGH_RISK_LATIN_TAIL_LABELS = {
    "srd_Latn", "ast_Latn", "vec_Latn", "gug_Latn", "pap_Latn",
    "fur_Latn", "scn_Latn", "lim_Latn", "mlt_Latn",
    "som_Latn", "swh_Latn", "yor_Latn", "hau_Latn", "kin_Latn",
    "sun_Latn", "bjn_Latn", "min_Latn", "ekk_Latn", "als_Latn"
}
```

### 9.2 Indic review labels

Do not treat Bhojpuri/Awadhi as generic tail hallucinations. They are often Hindi-adjacent signals and should be included in the Hindi/Indic audit pool.

```python
HINDI_RELATED_ISO = {"hin", "bho", "awa", "mai", "mag"}
INDIC_AUDIT_ISO = {"hin", "bho", "awa", "mai", "mag", "npi", "mar", "urd", "pan", "guj"}
SOURCE_HINDI_CODES = {"hi", "hi-in", "hin"}
SOURCE_INDIC_CODES = {"hi", "hi-in", "hin", "ne", "ne-np", "npi", "bho", "ur", "ur-pk", "pa", "gu", "mr"}
```

### 9.3 Macro/near-language review clusters

Use rollup clusters for QA and mixed-language suppression, not for silent recoding.

```text
bos, hrv, srp, cnr -> hbs_BCMS
ind, zsm -> ind_zsm_malay_indonesian_cluster
cmn_Hans, cmn_Hant -> cmn_Chinese
hin, bho, awa, mai, mag -> hindi_related_north_indic_review_cluster
ita, srd, vec, scn, fur, lmo -> italian_romance_review_cluster
spa, ast, glg, cat -> iberian_romance_review_cluster
```

### 9.4 Label review notes

Use a general `LABEL_REVIEW_NOTES` mapping rather than treating all entries as true ISO/Wikimedia ambiguity.

```python
LABEL_REVIEW_NOTES = {
    "als": "ISO 639-3 'als' is Tosk Albanian, while historical Wikimedia/legacy 'als' usage often refers to Alemannic German. Audit before interpretation.",
    "nob": "Norwegian Bokmaal (nob) vs Norwegian macrolanguage/source code no/nor. Reconcile before rollup.",
    "zsm": "Standard Malay (zsm) vs Malay macrolanguage/source ms/msa and Indonesian (ind). Treat within Malay/Indonesian review cluster.",
}
```

Do not hard-recode any of these labels.

---

## 10. Model comparison and consensus

Create `yt_lid_v3_segment_model_comparison` by joining OpenLID and GlotLID predictions on `segment_id`.

Create `yt_lid_v3_channel_model_comparison` by joining the model-specific channel aggregations on `channel_id`.

Channel-level comparison fields:

```text
channel_id
openlid_primary_language_label
openlid_primary_language_iso639_3
openlid_primary_language_script
openlid_primary_language_score
openlid_primary_language_vote_share_with_top2
openlid_secondary_language_label
openlid_secondary_to_primary_score_ratio
glotlid_primary_language_label
glotlid_primary_language_iso639_3
glotlid_primary_language_script
glotlid_primary_language_score
glotlid_primary_language_vote_share_with_top2
glotlid_secondary_language_label
glotlid_secondary_to_primary_score_ratio
models_agree_exact_primary
models_agree_iso_primary
models_agree_analysis_cluster_primary
models_agree_exact_secondary
models_agree_analysis_cluster_secondary
consensus_language_label
consensus_language_iso639_3
consensus_language_script
consensus_analysis_language_cluster
consensus_for_rollup_label
consensus_status
requires_manual_adjudication
```

Consensus mapping:

```text
exact_model_agreement
  -> consensus_language_label = OpenLID primary label; requires_manual_adjudication=false

iso_or_script_variant_agreement
  -> consensus_language_label = NULL if scripts differ materially;
     consensus_for_rollup_label and consensus_analysis_language_cluster populated;
     requires_manual_adjudication=false unless the distinction matters analytically

cluster_model_agreement
  -> consensus_language_label = NULL;
     consensus_analysis_language_cluster and consensus_for_rollup_label populated;
     requires_manual_adjudication=false for rollup analysis, true for exact-label analysis

openlid_high_confidence_glotlid_missing_or_error
  -> consensus_language_label = OpenLID primary label only if GlotLID failed to produce usable output;
     otherwise disagreement rules apply

glotlid_fallback_openlid_low_confidence
  -> consensus_language_label = GlotLID primary label only when OpenLID confidence is low,
     GlotLID confidence is high, the label is not high-risk, and script evidence does not contradict it

model_disagreement_needs_review
  -> consensus_language_label = NULL; requires_manual_adjudication=true

high_risk_tail_label_needs_review
  -> consensus_language_label = NULL unless both models agree exactly and evidence is strong;
     requires_manual_adjudication=true

insufficient_text
  -> NULL
```

Because GlotLID runs on all valid segments by default, `openlid_high_confidence_single_model` should not be used except when GlotLID inference fails or is manually disabled.

---

## 11. Revised mixed-language logic

Create model-specific and consensus mixed-language fields:

```text
openlid_is_mixed_language_screen
openlid_is_credible_mixed_language_candidate
glotlid_is_mixed_language_screen
glotlid_is_credible_mixed_language_candidate
consensus_is_mixed_language_screen
consensus_is_credible_mixed_language_candidate
mixed_language_rejection_reason
```

A model-specific screen remains permissive:

```text
secondary_to_primary_score_ratio >= mixed_screen_ratio_threshold
secondary_language_segment_count >= mixed_screen_min_secondary_segments
```

A model-specific credible candidate requires:

```text
secondary_to_primary_score_ratio >= mixed_credible_ratio_threshold
secondary_language_segment_count >= mixed_credible_min_secondary_segments
secondary_language_top1_segment_count >= mixed_credible_min_secondary_top1_segments
secondary_mean_segment_score >= mixed_credible_secondary_mean_score
(secondary_max_segment_score >= mixed_credible_secondary_max_score OR primary_script != secondary_script)
rank2_rank3_margin_ratio >= mixed_credible_min_rank2_rank3_margin_ratio
AND (primary_script != secondary_script OR secondary_language_segment_type_count >= mixed_credible_min_secondary_segment_types)
AND NOT same_analysis_language_cluster
AND NOT high_risk_secondary_without_model_agreement
```

A consensus credible mixed-language candidate requires one of:

1. OpenLID and GlotLID both identify the same secondary language exactly; or
2. OpenLID and GlotLID identify the same secondary analysis cluster, with high-quality evidence; or
3. There is strong cross-script evidence, at least one model has a high-confidence secondary language, and the other model does not contradict the secondary cluster.

High-risk tail labels may not create a credible mixed-language candidate unless both models support the high-risk label exactly or a manual review has validated it.

---

## 12. Hindi/Indic diagnostics

Create `yt_lid_v3_hindi_indic_audit_candidates`. Do not rely on `primary_language_label = hin_Deva`.

Add fields:

```text
contains_devanagari_metadata
devanagari_segment_count
devanagari_char_count_total
hindi_related_openlid_vote_present
hindi_related_glotlid_vote_present
hindi_related_any_model_vote_present
indic_openlid_vote_present
indic_glotlid_vote_present
indic_any_model_vote_present
hindi_related_primary_or_secondary
indic_primary_or_secondary
romanized_hindi_keyword_count
romanized_indic_keyword_count
romanized_indic_keyword_examples
source_hi_disagreement
source_indic_disagreement
hindi_indic_candidate_status
```

Use romanized keywords only as recall-oriented audit flags. They must never feed into primary/secondary label assignment, vote weighting, or consensus.

Use word-boundary/phrase matching, not substring matching. Store matched terms for review.

Start with two keyword sets:

```python
ROMANIZED_HINDI_KEYWORDS = {
    "hindi", "bhajan", "bollywood", "krishna", "kanha", "radha",
    "mahadev", "shiv", "ramayan", "katha", "pravachan", "samachar",
    "khabar", "modi", "yogi", "sarkar", "chunav", "desi",
    "upsc", "ssc", "gk", "current affairs in hindi"
}

ROMANIZED_INDIC_KEYWORDS = ROMANIZED_HINDI_KEYWORDS | {
    "nepali", "lok dohori", "dohori", "maya", "timro", "mero", "timi",
    "bhojpuri", "punjabi", "urdu", "ghazal", "qawwali", "kirtan"
}
```

Set `hindi_indic_candidate_status` using this priority order:

```text
hindi_primary_metadata
hindi_secondary_or_topk_metadata
indic_primary_or_secondary_metadata
devanagari_non_hindi_primary
romanized_hindi_candidate
romanized_indic_candidate
source_hi_disagreement
source_indic_disagreement
indic_cluster_candidate
no_hindi_or_indic_signal
```

Include raw sample text, OpenLID votes, GlotLID votes, source language fields, and consensus fields.

---

## 13. High-risk redirect diagnostic

Create `yt_lid_v3_high_risk_redirect_diagnostic` for every channel whose OpenLID or GlotLID primary/secondary label is in `HIGH_RISK_LATIN_TAIL_LABELS`.

Report:

```text
model_label_source                         # openlid_primary, openlid_secondary, glotlid_primary, glotlid_secondary
high_risk_label
n_channels
n_with_devanagari_metadata
n_with_romanized_hindi_keywords
n_with_romanized_indic_keywords
n_with_any_indic_model_vote
n_with_source_indic_code
n_with_glotlid_non_romance_top1            # for OpenLID high-risk cases
n_with_openlid_non_romance_top1            # for GlotLID high-risk cases
n_dominant_script_non_latin_any_segment
share_with_any_indic_or_nonlatin_signal
sample_channel_ids
```

Do not describe this only as a “script mismatch” diagnostic. Romanized Hindi/Nepali failure cases often have Latin dominant script, so script mismatch alone will miss them. The diagnostic must combine high-risk labels, romanized Indic keywords, source language fields, Devanagari presence, and the other model’s top-k votes.

---

## 14. QA tables and summaries

Save full QA tables without `.limit(100)`. Displays may be limited for readability, but saved Delta tables must not be.

Required summaries:

1. `yt_lid_v3_language_summary_full`: exact label counts, confidence distributions, high-risk counts, Hindi/Indic candidate counts.
2. `yt_lid_v3_language_summary_rollup`: rollup cluster counts by consensus status and language status.
3. `yt_lid_v3_model_agreement_summary`: exact, ISO, and cluster agreement rates by language/cluster and script.
4. `yt_lid_v3_mixed_language_candidates`: screens and credible candidates with rejection reasons.
5. `yt_lid_v3_suspect_tail_audit_sample`: 30–50 channels per high-risk label where available.
6. `yt_lid_v3_manual_validation_sample`: deterministic per-stratum sample.
7. `yt_lid_v3_unclassified_audit`: text-sparse and invalid-text cases.
8. `yt_lid_v3_source_language_confusion`: source-vs-model disagreement patterns.
9. `yt_lid_v3_high_risk_redirect_diagnostic`: tail-label redirect mechanism.

Manual validation strata:

```text
high_confidence_major_language
low_confidence
credible_mixed_language_candidate
mixed_screen_not_credible
high_risk_latin_tail_label
hindi_indic_audit_candidate
source_language_disagreement
openlid_glotlid_exact_disagreement
openlid_glotlid_cluster_disagreement
insufficient_text_or_unclassified
non_latin_script_control
```

Sample per stratum with deterministic seeds derived from `validation_sample_seed` and the stratum name. Preserve all qualifying strata in an array, but assign one primary stratum by fixed priority to avoid double-counting.

---

## 15. Ablation analysis

Run ablation aggregations without rerunning model inference. Include OpenLID, GlotLID, and consensus-sensitive ablations.

Minimum configurations:

```text
v1_legacy_like_openlid
v3_default_openlid
v3_default_glotlid
v3_default_consensus
v3_no_top2_openlid
v3_no_top2_glotlid
v3_description_weight_1_openlid
v3_no_description_openlid
v3_min_clean_chars_50_latin_openlid
v3_top1_only_rollup_openlid
```

Save `yt_lid_v3_ablation_summary` with:

```text
config_name
lid_model_or_consensus
n_channels_classified
n_mixed_screen
n_credible_mixed
n_high_risk_primary
n_hindi_primary
n_hindi_indic_candidate
n_srd_primary
n_ast_primary
n_vec_primary
n_gug_primary
n_eng_primary
n_spa_primary
n_ita_primary
n_por_primary
n_primary_changed_vs_v3_default_openlid
pct_primary_changed_vs_v3_default_openlid
n_primary_changed_vs_v3_default_consensus
pct_primary_changed_vs_v3_default_consensus
n_credible_mixed_changed_vs_v3_default
```

Primary-label churn is a required robustness metric.

---

## 16. README updates

Create or update `README_language_lid_v3.md`.

Document:

1. The output measures written metadata language, not spoken/video language.
2. OpenLID and GlotLID both run by default on all valid segments.
3. `audit_segments` is available only as a manual runtime fallback and should not be used for full model-agreement rates.
4. `min_clean_chars=40`, `min_clean_chars_non_latin=12`, and letter-based validity rules.
5. Segment-level architecture and why concatenated-text classification is avoided.
6. The meaning of vote shares and why `primary_language_confidence` is not a calibrated probability.
7. High-risk tail-label handling and the refusal to hard-recode labels.
8. Hindi/Indic audit fields and their high-recall, non-classification status.
9. Mixed-language screen versus credible mixed-language candidate.
10. Consensus-label statuses, including intentional NULL exact labels for disagreements/review cases.
11. GlotLID preprocessing caveat and the optional native-preprocessing audit output.
12. Full QA tables, manual validation sample, and ablation outputs.
13. License/model-binary cautions inherited from the original README.
14. Remove or resolve any dangling placeholder citations such as `[Sage Journals][2]`.

---

## 17. Acceptance criteria

The Codex revision is complete only if all are true:

1. A deterministic 10,000-channel smoke test runs without modifying `yt_sl_channels`.
2. `enable_glotlid=true` and `glotlid_mode="all_valid_segments"` are the defaults.
3. OpenLID and GlotLID segment tables are both written for the same valid `segment_id` universe, except for explicit inference errors.
4. The final channel table has one row per post-dedup channel ID.
5. Deduplication and smoke-test sampling are deterministic.
6. The default Latin/ambiguous threshold is at least 40 usable letters; the non-Latin exception is evaluated over `clean_letter_count`.
7. Full language summaries are saved without `.limit(100)`.
8. Model-specific channel aggregations are produced for both OpenLID and GlotLID.
9. The final channel table contains legacy OpenLID fields, GlotLID fields, model-comparison fields, and consensus fields.
10. Every `consensus_status` has deterministic rules for exact labels, rollup labels, and review flags.
11. Mixed-language output distinguishes screens from credible candidates and uses second-model support for credible candidate status by default.
12. High-risk labels are flagged, not silently recoded.
13. Hindi/Indic audit candidates are exported even when Hindi is not primary or secondary.
14. Romanized Hindi/Indic keyword flags use word-boundary/phrase matching and never feed into label assignment.
15. The high-risk redirect diagnostic combines high-risk labels, Devanagari evidence, broader romanized Indic keywords, source-language fields, and second-model votes; it is not only a script-mismatch diagnostic.
16. A deterministic stratified manual-validation sample is written.
17. Ablation summaries include primary-label churn versus both OpenLID default and consensus default.
18. The notebook fails clearly if either model is enabled but unavailable.
19. The README is updated and contains no unresolved placeholder references.

---

## 18. Codex instruction block

Use this concise instruction block when handing the task to Codex:

```text
Revise `01_language_openlid_v3_databricks.py` into a v3 Databricks language-ID notebook.

Keep OpenLID-v3 as the legacy primary detector, but run GlotLID on all valid segments by default. Do not default GlotLID to low-confidence/audit-only examples. Write new v3 output tables and do not overwrite source tables.

Implement deterministic channel/video deduplication, deterministic smoke-test sampling, canonical segment construction, `min_clean_chars=40` using usable letter counts, a 12-letter non-Latin exception, full OpenLID and GlotLID segment predictions, model-specific channel aggregation, channel-level model comparison, consensus fields, high-risk tail-label flags, Hindi/Indic audit candidates, high-risk redirect diagnostics, screen-vs-credible mixed-language flags, full QA tables, deterministic validation samples, and ablation summaries with primary-label churn.

High-risk labels must be flagged and compared across models, not hard-recoded. Romanized Hindi/Indic keywords are recall-only audit signals and must never affect label assignment. GlotLID native preprocessing may be supported only as a separate audit output; the main model comparison should use the same canonical valid-segment universe for both models.

Acceptance requires OpenLID and GlotLID outputs for the same valid `segment_id` universe, exactly one final row per post-dedup channel ID, saved full summaries without `.limit(100)`, and an updated README with no unresolved placeholder citations.
```
