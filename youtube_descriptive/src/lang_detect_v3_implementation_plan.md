# Implementation plan: revise `01_language_openlid_v3_databricks.py` to the v3 dual-model LID pipeline

Derived from `lang_detect_revision_spec.md` (v3). This plan maps every spec section to concrete
changes in the notebook, sequences the work into phases, and flags the engineering risks.

## Decisions taken (no clarification needed; spec is prescriptive)

1. **Revise in place.** Keep the filename `01_language_openlid_v3_databricks.py` (spec §18 names this
   file). The notebook is no longer OpenLID-only, but the filename is referenced throughout the spec, so
   it stays. Internally it becomes the "v3 dual-model" pipeline.
2. **New output table family `yt_lid_v3_*`** (spec §2). The legacy `yt_lid_openlid_v3_*` tables are never
   overwritten. Every table name is a widget so it can be overridden.
3. **Both models on by default**, `glotlid_mode="all_valid_segments"` (spec §3, acceptance #2).
4. **No source-table mutation** unless `update_source_detected_language=true` after validation
   (acceptance #1). Keep the existing MERGE guard.
5. **Refactor for reuse:** one preprocessing path, one inference-UDF factory, one channel-aggregation
   function — parameterized by model — rather than copy/pasting OpenLID logic for GlotLID.
6. **Extract pure functions** (`parse_lid_label`, script-metric computation, validity rule, consensus
   classifier) so they are unit-testable off-cluster (see Testing).

---

## Phase 0 — Scaffolding, widgets, constants, model loading

**Spec:** §3 (widgets), §9 (taxonomy constants), acceptance #18 (fail-fast).

- Rewrite the header markdown (cell 1) to describe the dual-model v3 pipeline and the new outputs.
- **Widgets:** replace the §3 defaults verbatim (`min_clean_chars=40`, `min_clean_chars_non_latin=12`,
  `non_latin_dominant_script_share=0.60`, `top_k=5`, `enable_openlid`, `enable_glotlid`,
  `glotlid_mode=all_valid_segments`, `glotlid_preprocessing_mode=match_openlid`,
  `glotlid_native_audit_sample_fraction=0.00`, the score thresholds, all `mixed_*` thresholds, segment
  weights, ablation/validation widgets). Add a widget for **every** output table in §2 (default to the
  `yt_lid_v3_*` names). Keep the existing source/model/run-control widgets.
- **Validate** `glotlid_mode ∈ {disabled, audit_segments, all_valid_segments}` and
  `glotlid_preprocessing_mode ∈ {match_openlid, glotlid_native_audit}`; raise on bad values.
- **Constants block** (new cell) — §9 sets verbatim: `HIGH_RISK_LATIN_TAIL_LABELS`, `HINDI_RELATED_ISO`,
  `INDIC_AUDIT_ISO`, `SOURCE_HINDI_CODES`, `SOURCE_INDIC_CODES`, the rollup-cluster map, `LABEL_REVIEW_NOTES`,
  `ROMANIZED_HINDI_KEYWORDS`, `ROMANIZED_INDIC_KEYWORDS`. Express the rollup map as a dict and build a
  small Spark mapping/`when` chain or a broadcast lookup DataFrame.
- **Model loading & fail-fast:** generalize `ensure_hf_fasttext_model` for both models. After resolving
  paths, if `enable_openlid` / `enable_glotlid` is true but the binary cannot be loaded, **raise** (not
  silently skip). GlotLID repo `cis-lmu/glotlid`, file `model.bin`. Keep `direct_path`/`sparkfiles` modes
  for both; compute `WORKER_OPENLID_PATH` and `WORKER_GLOTLID_PATH`.

---

## Phase 1 — Deterministic dedup + smoke sampling

**Spec:** §4, acceptance #4, #5. Produces `yt_lid_v3_dedupe_qa`.

- **Channels (§4.1):** detect the first existing timestamp column from
  `["updated_at","modified_at","ingestion_timestamp","created_at","capture_date","first_capture_time"]`.
  `row_number()` over `partitionBy(channel_id)` ordered by: timestamp desc → `sha2(to_json(struct(*cols)),256)` asc
  → `channel_id` asc. **No `.dropDuplicates()`.** If no timestamp, hash ordering only. Keep `row==1`.
- **Videos (§4.2):** if `video_id` exists, partition by it; else partition by
  `(channel_id, video_title, video_description, + any rank/timestamp)`. Same deterministic ordering.
- **Smoke sampling (§4.3):** when `limit_channels>0`, select the first N channels ordered by
  `xxhash64(channel_id) asc` **after** dedup — never `.limit()` on an unordered frame. Replace the current
  `.distinct().limit()` block (notebook lines ~325–329).
- **`yt_lid_v3_dedupe_qa`:** before/after row counts, duplicate-channel / duplicate-video counts, and the
  chosen timestamp column for each of channels and videos.
- Define the canonical `all_channels` universe from the **post-dedup** channel table (replaces line ~743).

---

## Phase 2 — Canonical segment-input table with script metrics

**Spec:** §5, acceptance #6. Produces `yt_lid_v3_segments_input`. Both models read from this table.

- Build segments as today (channel_name, channel_description, video_title, video_description, video_tags)
  from the **deduped** channels/videos, with deterministic `videos_per_channel` selection retained.
- **Script-metric computation** is the hard part. Implement a pandas UDF (or a single Python function
  reused by a UDF) that, given raw text, returns the struct of §5 fields. Steps inside:
  1. URL-strip, lowercase, collapse whitespace (reuse `preprocess_for_lid` logic).
  2. Remove digits, punctuation, symbols/emoji; keep only Unicode **letters** using `regex` Unicode
     properties (`\p{L}`, and per-script classes `\p{Latin}`, `\p{Devanagari}`, `\p{Arabic}`,
     `\p{Cyrillic}`, `\p{Han}`, `\p{Hiragana}`+`\p{Katakana}`→kana, `\p{Hangul}`, `\p{Thai}`).
  3. Per-script letter counts; `clean_letter_count = sum`; `dominant_script` = argmax; `dominant_script_share`.
  4. `has_url`, `has_hashtag`, `has_emoji_or_symbol` flags from the **raw** text.
  5. `clean_text` / `clean_text_len` retained for diagnostics only.
- **Validity rule (§5, the core change):**
  ```
  is_valid_text_for_lid =
      clean_letter_count >= min_clean_chars
   OR (dominant_script is non-Latin
       AND clean_letter_count >= min_clean_chars_non_latin
       AND dominant_script_share >= non_latin_dominant_script_share)
  ```
  Also set `is_valid_text_latin_rule`, `is_valid_text_non_latin_rule`, and a `short_text_reason` string.
  `clean_letter_count==0` ⇒ invalid. **Length-based validity is removed** from the threshold path.
- `segment_id` = sha2 over `(channel_id, video_id, segment_type, text)` (keep current scheme so it is
  stable). Write all §5 fields to `yt_lid_v3_segments_input`.

---

## Phase 3 — Run OpenLID and GlotLID on the same valid universe

**Spec:** §6, acceptance #3. Produces `yt_lid_v3_openlid_segments`, `yt_lid_v3_glotlid_segments`,
optional `yt_lid_v3_glotlid_native_segments`.

- **Inference UDF factory:** generalize the current `openlid_predict_udf` into a builder that takes a
  worker model path + preprocessing mode and returns top-k labels/scores. `top_k=5` now, so widen the
  prediction schema to `label_1..5` / `score_1..5` (or, preferably, return an **array** of (label, score)
  structs to avoid hard-coding k). Carry `lid_error` per segment.
- Input is `yt_lid_v3_segments_input` filtered to `is_valid_text_for_lid=true`. Inference no longer
  re-checks the length threshold (validity already decided in Phase 2).
- **OpenLID (§6.1):** run when `enable_openlid=true`; write `yt_lid_v3_openlid_segments`.
- **GlotLID (§6.2):** run when `enable_glotlid=true AND glotlid_mode="all_valid_segments"`; write
  `yt_lid_v3_glotlid_segments`. Default preprocessing `match_openlid` (same cleaning as OpenLID for
  comparability).
- **Audit mode (§6.3):** `glotlid_mode="audit_segments"` restricts GlotLID to low-confidence OpenLID
  segments — manual override only, must not feed agreement rates. Keep but do not default.
- **Native audit (§6.4):** if `glotlid_preprocessing_mode="glotlid_native_audit"` (or sample fraction>0),
  write separate `yt_lid_v3_glotlid_native_segments`; never mix into the main comparison.
- **Acceptance check (cell):** assert the valid `segment_id` set in glotlid_segments equals openlid_segments
  except rows with non-null `lid_error`. Print the diff counts; fail loudly if they diverge unexpectedly.
- **Checkpoint note:** the latest upstream commit switched `localCheckpoint` → DBFS checkpoint for executor
  eviction; reuse a DBFS-backed checkpoint/persist when materializing the shared segment frame so both
  model passes read a stable input.

---

## Phase 4 — Label normalization + long-format predictions

**Spec:** §7.

- Implement `parse_lid_label(raw) -> (normalized_label, iso639_3, script)` handling `__label__eng_Latn`,
  `eng_Latn`, `eng`, and malformed/missing. Pure function ⇒ unit-test it.
- Convert each model's top-k wide output to **long format** with the §7 columns
  (`prediction_rank`, `label_raw`, `label`, `iso639_3`, `script`, `score`, `score_1`, plus
  `clean_letter_count`, `clean_text_len`, `dominant_script`, `is_valid_text_for_lid`).
- Retain labels with `script=NULL`; only drop known noise/unknown (`zxx|und|noise|null|none|unknown`) for
  voting, but keep them in QA.

---

## Phase 5 — Model-specific channel aggregation

**Spec:** §8, acceptance #8. Produces `yt_lid_v3_channel_votes` (with `lid_model` column).

- One `aggregate_channel_votes(long_preds_df, model_name)` function, applied to OpenLID and GlotLID, union
  with a `lid_model` column.
- **Top-1 admission:** `rank=1 AND score>=primary_min_score AND is_valid_text_for_lid`.
- **Top-2 admission:** `rank=2 AND score>=secondary_min_score AND score/score_1>=secondary_min_score_ratio
  AND is_valid_text_for_lid`. Top-2 carries `secondary_label_vote_weight=0.20`.
- Apply segment-type weights (§8 defaults: title 2.0, description 1.0, channel_description 1.0, tags 0.5,
  channel_name 0.25). Note: these differ from the legacy weights (channel_description was 3.0) — use the
  new values.
- Compute **all** §8 channel fields (primary/secondary label/iso/script/score, vote shares with/without
  top2, rank2/rank3 scores + margin + margin ratio, segment counts/type counts, total letters, mean/max
  primary segment score, `language_votes_json`).
- Decide whether length-weighting carries over; spec §8 doesn't list it — keep it off by default for v3
  comparability unless a widget re-enables it (flag this for the user).

---

## Phase 6 — Model comparison + consensus

**Spec:** §10, acceptance #9, #10. Produces `yt_lid_v3_segment_model_comparison`,
`yt_lid_v3_channel_model_comparison`, and feeds `yt_lid_v3_channels`.

- **Segment comparison:** join OpenLID and GlotLID long preds on `segment_id`.
- **Channel comparison:** join the two per-model channel aggregations on `channel_id`; emit the §10 field
  list (openlid_* / glotlid_* primaries+secondaries, agreement booleans at exact/iso/cluster level,
  consensus_* fields, `consensus_status`, `requires_manual_adjudication`).
- **Consensus classifier** (pure function over the joined row → status + consensus label/cluster + flags),
  implementing the §10 status table exactly:
  `exact_model_agreement`, `iso_or_script_variant_agreement`, `cluster_model_agreement`,
  `openlid_high_confidence_glotlid_missing_or_error`, `glotlid_fallback_openlid_low_confidence`,
  `model_disagreement_needs_review`, `high_risk_tail_label_needs_review`, `insufficient_text`.
  Cluster membership comes from the §9.3 rollup map. Because GlotLID runs on all segments,
  `openlid_high_confidence_single_model` is used only when GlotLID errored/disabled (§10 note).

---

## Phase 7 — Screen vs. credible mixed-language

**Spec:** §11. Feeds the channel/comparison tables and `yt_lid_v3_mixed_language_candidates`.

- Per-model **screen** (permissive): `secondary_to_primary_score_ratio>=mixed_screen_ratio_threshold AND
  secondary_language_segment_count>=mixed_screen_min_secondary_segments`.
- Per-model **credible candidate:** the full §11 conjunction (ratio, secondary seg count, secondary top1
  seg count, secondary mean score, max-score-or-cross-script, rank2/rank3 margin ratio, segment-type
  diversity or cross-script, NOT same analysis cluster, NOT high-risk-secondary-without-agreement).
- **Consensus credible** requires one of the three §11 conditions (both models same secondary exactly /
  same cluster with quality / cross-script with one high-confidence model and no contradiction).
  `mixed_credible_require_second_model_support=true` by default.
- High-risk tail labels cannot create a credible candidate unless both models agree exactly or manual
  review validated it.
- Emit `openlid_/glotlid_/consensus_ is_mixed_language_screen` + `..._is_credible_mixed_language_candidate`
  and `mixed_language_rejection_reason`.

---

## Phase 8 — Hindi/Indic recall diagnostics

**Spec:** §12, acceptance #13, #14. Produces `yt_lid_v3_hindi_indic_audit_candidates`.

- Do **not** gate on `primary_language_label=hin_Deva`. Compute Devanagari presence/counts from the
  segment-input script metrics; check Hindi-related / Indic votes in **either** model's top-k.
- **Romanized keyword flags:** word-boundary/phrase matching (`regex` with `\b`), never substring; store
  matched terms (`romanized_indic_keyword_examples`). These are **recall-only audit signals** and must not
  feed votes/consensus.
- Source-field disagreement: `source_hi_disagreement`, `source_indic_disagreement` using `SOURCE_*` code
  sets vs. model output.
- `hindi_indic_candidate_status` assigned by the §12 priority order.
- Output includes raw sample text, both models' votes, source fields, consensus fields.

---

## Phase 9 — High-risk redirect diagnostic

**Spec:** §13, acceptance #15. Produces `yt_lid_v3_high_risk_redirect_diagnostic`.

- One row group per channel whose OpenLID/GlotLID primary **or** secondary label ∈
  `HIGH_RISK_LATIN_TAIL_LABELS`, broken out by `model_label_source`.
- Report the §13 counts: Devanagari metadata, romanized Hindi/Indic keywords, any Indic model vote, source
  Indic code, the *other* model's non-Romance top1, non-Latin dominant script in any segment, and
  `share_with_any_indic_or_nonlatin_signal`, plus `sample_channel_ids`.
- **Explicitly combine** label + Devanagari + romanized Indic keywords + source fields + other-model votes —
  not script-mismatch alone (romanized Hindi/Nepali cases are Latin-dominant).

---

## Phase 10 — Final `yt_lid_v3_channels` table

**Spec:** §8 backward-compat, §10 consensus, acceptance #4, #9.

- Left-join `all_channels` (post-dedup) with: OpenLID aggregation, GlotLID aggregation, channel model
  comparison, mixed-language flags, and source audit fields.
- Keep legacy field names (`primary_language_label`, etc.) for backward compatibility, **plus** the
  explicitly prefixed `openlid_*` / `glotlid_*` fields and all consensus fields.
- Exactly one row per post-dedup channel_id (assert).
- Do **not** touch `yt_sl_channels.detected_language` unless the existing guard widget is enabled.

---

## Phase 11 — QA tables, summaries, validation sampling

**Spec:** §14, acceptance #7, #16. Produces the summary/audit/sample tables in §2.

- All saved Delta tables **without `.limit(100)`** (displays may still be limited). Audit current §6 QA
  cells that use `.limit(100)` (notebook lines ~812, 827) and remove the limit from the *saved* paths.
- Build: `language_summary_full`, `language_summary_rollup`, `model_agreement_summary`,
  `mixed_language_candidates`, `suspect_tail_audit_sample` (30–50 channels/high-risk label),
  `manual_validation_sample`, `unclassified_audit`, `source_language_confusion`.
- **Manual validation sampling:** the §14 strata list; deterministic per-stratum seed derived from
  `validation_sample_seed` + stratum name; keep all qualifying strata in an array but assign one **primary
  stratum** by fixed priority. Respect `validation_max_per_stratum=100` / `validation_min_per_stratum=30`.
  Gate on `create_validation_samples`.

---

## Phase 12 — Ablation

**Spec:** §15, acceptance #17. Produces `yt_lid_v3_ablation_summary`. Gate on `run_ablation_aggregations`.

- Re-aggregate from stored segment predictions **without re-running inference**. Implement each §15 config
  by re-parameterizing the aggregation function (toggle top2, description weight, min_clean_chars=50,
  top1-only rollup, per-model vs consensus).
- Emit the §15 columns including **primary-label churn** vs. both `v3_default_openlid` and
  `v3_default_consensus` (`n_/pct_primary_changed_*`, `n_credible_mixed_changed_*`).

---

## Phase 13 — README + cleanup

**Spec:** §16, acceptance #19.

- Create `README_language_lid_v3.md` covering the 14 §16 points (written-metadata-language framing, both
  models default-on, audit_segments caveat, 40/12 letter thresholds, segment architecture, vote-share/
  non-calibrated-confidence note, high-risk no-recode policy, Hindi/Indic recall-only fields, screen vs
  credible, consensus statuses incl. intentional NULLs, GlotLID native-preprocessing caveat, full QA/
  validation/ablation outputs, license/binary cautions). Remove any dangling `[Sage Journals][2]`-style
  placeholders.
- Update `CHANGELOG_revisions.md`. Leave the old `README_language_openlid_v3.md` in place (legacy pipeline
  still documented) or mark it superseded.

---

## Phase 14 — Acceptance verification pass

Walk the §17 acceptance checklist (19 items) and confirm each is satisfied; add inline assertions in the
notebook where cheap (one-row-per-channel, segment_id set equality, defaults, no `.limit` on saved tables,
fail-fast on missing model).

---

## Testing strategy (this is a Databricks/Spark notebook — cannot fully run locally)

- **Off-cluster unit tests** for the extracted pure functions: `parse_lid_label`, the script-metric/
  validity computation, the consensus classifier, the mixed-language credible classifier, and the
  romanized-keyword word-boundary matcher. These cover the highest-logic-risk pieces without Spark.
- **On-cluster smoke test:** deterministic `limit_channels=10000`, both models on, confirm all tables
  write, segment_id universes match, one row per channel, no source mutation (acceptance #1).
- I cannot execute the notebook here (no Spark, no Unity Catalog, no fastText binaries). I will note any
  step that must be validated on the cluster.

## Key risks / things to confirm with you

1. **Performance:** GlotLID now runs on *all* valid segments (not a low-confidence subset). Roughly doubles
   inference cost vs. the audit-only design. Acceptable per spec, but worth confirming cluster sizing.
2. **Script-metric UDF cost:** per-segment Unicode-class counting in Python is the heaviest new component.
   Option to push some counts into Spark SQL `regexp`/`length` if the pandas UDF is too slow.
3. **Length-weighting in v3 aggregation:** spec §8 omits it; I plan to default it off for comparability —
   confirm you don't want it retained.
4. **Notebook size:** this will roughly double the file (~1800+ lines). I'll keep it as one notebook per
   spec, organized by the section headers above.
5. **`top_k=5`:** I'll switch the prediction schema to an array-of-structs so k isn't hard-coded to 3.

## Suggested execution order

Phases run in dependency order 0→14. Natural commit points: after Phase 2 (input table), Phase 5 (per-model
votes), Phase 10 (final channel table), Phase 13 (docs). I'd implement and self-review per phase rather than
in one pass.
