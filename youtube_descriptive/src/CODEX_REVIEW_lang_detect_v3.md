# Codex review handoff: language-detection v3 dual-model rewrite

This document explains every change made to implement `lang_detect_revision_spec.md` (the v3 spec) and
gives a reviewer everything needed to verify the work. It is written for Codex (or any reviewer) to audit
without re-deriving context.

## TL;DR

`01_language_openlid_v3_databricks.py` was rewritten from a **single-model OpenLID-v3** pipeline (927 lines,
3 output tables) into a **dual-model OpenLID-v3 + GlotLID consensus** pipeline (~2,960 lines, 20 output
tables + 1 intermediate) per the v3 spec. The 14-phase plan in `lang_detect_v3_implementation_plan.md` is
fully implemented.

**The code has NOT been run on a Databricks cluster.** It is verified by `python3 -m py_compile` and by
off-cluster unit tests of the pure-Python logic only. The Spark/Delta/fastText execution paths require an
on-cluster smoke test (see "What still needs cluster verification").

**Requires Databricks Runtime 13.0+ (Spark 3.4+)** — see "What still needs cluster verification".

### Revision history (since the initial rewrite)

- **Two review-fix batches** were applied. Highlights: separated `GLOTLID_ACTIVE` (writes the audit table)
  from `GLOTLID_CAN_FEED_MAIN` (only `all_valid_segments` feeds aggregation/consensus/diagnostics, per §6.3);
  robust video dedup + stable per-video segment key (null/empty `video_id` no longer collapse rows);
  native-audit now preprocesses raw `text`; high-risk exact agreement only populates a consensus label with
  strong dual-model evidence; consensus mixed-language `cond1/2/3` now require genuine dual-model evidence;
  source-table MERGE restricted to clean classified rows; skipped summaries write empty typed tables;
  deterministic ordering of `segment_types` and `language_votes_json`; full numeric-widget input validation.
- **A full audit** was then performed. Actionable items fixed: a Spark-3.4 version guard (this notebook now
  asserts `spark.version >= 3.4` up front); the `#6` acceptance check softened to a NOTE for manual runs that
  lower `min_clean_chars`; removed unused imports and a misleading dead `short_text_reason` branch; this
  handoff refreshed.
- **A second audit pass** then fixed the remaining items: the analysis-cluster lookup is now a native Spark
  `element_at(create_map(...))` expression (`analysis_cluster_expr`) instead of a Python pandas UDF, so it is
  cheap even at the segment scale used by `segment_model_comparison`; `segment_model_comparison` is now
  **top-1 per segment** (one row per segment, preserving error/empty rows) rather than a rank-by-rank join;
  `ablation_aggregate` drops zero-weighted votes so weight-zeroing configs (e.g. `v3_no_description`) no
  longer fabricate a primary for channels whose only signal was that segment type; and the
  `iso_or_script_variant_agreement` consensus rollup now prefers the cluster (consistent with the other
  branches). Documented-but-unfixed trade-offs: zero-valid-segment channels get `consensus_status` NULL
  (language_status still correct); `v3_description_weight_1_openlid` equals the v3 default (≈0 churn); some
  extra `.count()` passes. No run-blocking logic bug was found for a modern (DBR 13+) runtime.
- **A scalability remediation pass** converted production inference to compact, bucketed, resumable outputs.
  Full long-format top-k segment tables are no longer mandatory; `prediction_output_mode=compact` writes one
  row per valid segment per model, and long tables are written only for `long_sample` or `long_full`.
  Large staged tables carry `run_id` and `channel_hash_bucket`; inference, aggregation, diagnostics, and
  ablation now aggregate from compact predictions; heavyweight QA/default displays are gated for production.
- **A scalability hardening pass** fixed the follow-up audit findings: Delta writes no longer combine
  `overwriteSchema=true` with `replaceWhere`; incompatible pre-refactor output tables are migrated by a
  full-range overwrite or rejected for partial runs; source tables are bucket-filtered before counts and
  deduplication; run-level summaries carry run/bucket scope columns; global capped QA samples are skipped for
  partial bucket runs; compact full-range runs clear only compatible current-run legacy long rows to prevent
  stale reads without table-wide deletion.
- **A final scale pass** removed `clean_text` from persisted compact prediction tables, avoids redundant
  inference repartitions for already bucket-partitioned inputs, gates the expensive full segment-id parity
  join behind `run_heavy_qa`, reuses the precomputed video row hash during deduplication, and carries
  `inference_hash_buckets` in run metadata so rows from incompatible bucket schemes do not mix.

## Source-of-truth documents (read these first)

1. `lang_detect_revision_spec.md` — the v3 specification (the contract). Section numbers (§1–§18) below
   refer to this file.
2. `lang_detect_v3_implementation_plan.md` — the implementation plan (phases 0–14) I followed.
3. `README_language_lid_v3.md` — user-facing documentation of the new pipeline (new file).

The superseded v2 draft spec is intentionally not included in this branch; the v3 spec
(`lang_detect_revision_spec.md`) is the contract. The key v3 override to be aware of: GlotLID default is
`all_valid_segments`, not the v2 draft's `audit_segments`.

## Files changed

| File | Change |
|---|---|
| `01_language_openlid_v3_databricks.py` | Full rewrite (single-model → dual-model v3). Legacy code removed from working tree; preserved in git history at commit `d3cb137`. |
| `README_language_lid_v3.md` | New. Covers the 14 documentation points in §16 and the production scaling mode. |
| `CHANGELOG_revisions.md` | Added a v3 entry above the existing first-cut entry. |
| `lang_detect_v3_implementation_plan.md` | New. The plan (added at planning time). |
| `CODEX_REVIEW_lang_detect_v3.md` | This file. |

`lang_detect_revision_spec.md` was provided by the user; it is not my change. The superseded v2 draft spec
was removed from this branch.

## How to verify quickly

```bash
# 1. The notebook must parse as Python (the # MAGIC / # COMMAND lines are comments).
python3 -m py_compile 01_language_openlid_v3_databricks.py && echo OK

# 2. No null bytes (one was introduced during editing and stripped; guard against regressions).
python3 -c "print(open('01_language_openlid_v3_databricks.py','rb').read().count(b'\x00'))"   # expect 0

# 3. No leftover scaffolding.
grep -c "NOT YET IMPLEMENTED" 01_language_openlid_v3_databricks.py   # expect 0
```

The pure-logic unit tests I ran live in `/tmp` (not committed). A reviewer should re-extract and re-test
these functions directly from the notebook: `compute_script_metrics_one` + the validity rule,
the native Spark label-parsing expressions in `_with_parsed_compact_labels`, `compute_consensus`, the §11
mixed-language boolean conjunction, and the romanized keyword matcher. They are deterministic except where
they reference Spark expressions or module-level constants.

## Notebook structure (section → spec § → outputs)

| Notebook section | Spec § | Key functions | Output tables |
|---|---|---|---|
| 0–1 Params/widgets | §3 | widget helpers | (all §2 table names are widgets) |
| 1b Taxonomy constants | §9 | — | — |
| 2 Model binaries (fail-fast) | acc. #18 | `ensure_hf_fasttext_model` | — |
| 3 Dedup + smoke sampling | §4 | `deterministic_dedup` | `yt_lid_v3_dedupe_qa` |
| 4 Segment-input | §5 | `compute_script_metrics_one`, `script_metrics_udf` | `yt_lid_v3_segments_input` |
| 5 Inference | §6 | `predict_segments_compact`, DBFS checkpoint | `yt_lid_v3_openlid_predictions_compact`, `yt_lid_v3_glotlid_predictions_compact`, `*_glotlid_native_predictions_compact` |
| 6 Label/long-format | §7 | `_with_parsed_compact_labels`, `build_long_segments_from_compact` | optional long segment tables when requested |
| 7 Channel aggregation | §8 | `build_admitted_votes_from_compact`, `build_channel_votes`, `summarize_channel` | `yt_lid_v3_channel_votes`, `yt_lid_v3_channel_model_aggregation` |
| 8 Comparison + consensus | §10 | `analysis_cluster_expr`, `compute_consensus`, `consensus_udf` | `yt_lid_v3_segment_model_comparison`, `yt_lid_v3_channel_model_comparison` |
| 9 Mixed-language | §11 | (Spark boolean expressions) | `yt_lid_v3_mixed_language_candidates` |
| 10 Hindi/Indic | §12 | `romanized_keyword_udf` | `yt_lid_v3_channel_text_features`, `yt_lid_v3_hindi_indic_audit_candidates` |
| 11 High-risk redirect | §13 | (Spark aggregation) | `yt_lid_v3_high_risk_redirect_diagnostic` |
| 12 Final channels | §8+§10 | (joins) | `yt_lid_v3_channels` |
| 13 QA + validation | §14 | (Spark) | `language_summary_full/_rollup`, `model_agreement_summary`, `suspect_tail_audit_sample`, `manual_validation_sample`, `unclassified_audit`, `source_language_confusion` |
| 14 Ablation | §15 | `ablation_aggregate`, `ablation_metrics` | `yt_lid_v3_ablation_summary` |
| 15 Acceptance checks | §17 | `_check`, `_table_exists` | — |

## Design decisions, approximations, and deviations to scrutinize

These are the places where the spec was ambiguous or where a literal reading was impractical without
re-running inference. **Review these specifically.**

1. **`clean_letter_count` = total Unicode `\p{L}` letters**, not the sum of the 8 named per-script buckets
   that §5.3 literally describes. Reason: a literal sum would mark Tamil/Telugu/Bengali/etc. (untracked
   scripts) invalid, which is wrong for an India-heavy corpus. Untracked scripts fall into a `other`
   dominant-script bucket and remain eligible for the non-Latin validity exception. (Section 4 of notebook.)
2. **Inference writes compact predictions by default.** The production path stores `label_raw_1..k`,
   parsed `label_1..k` / `iso639_3_1..k` / `script_1..k`, `score_1..k`, and `lid_error` in one row per valid
   segment per model. Compact prediction tables do not persist `clean_text`; join to `segments_input` for
   text-level audits. Optional long-format tables are derived from compact predictions only when
   `prediction_output_mode=long_sample` or `long_full`. This avoids the previous top-k row explosion.
3. **Length-weighting from the legacy pipeline is dropped** in v3 aggregation (§8 omits it), for cross-model
   comparability. The widget that controlled it in the legacy notebook is gone.
4. **Consensus precedence:** a high-risk tail label without exact agreement is flagged
   (`high_risk_tail_label_needs_review`) **before** iso/cluster/fallback consensus is considered. Exact
   high-risk agreement only populates `consensus_language_label` when both models have high vote-share
   evidence; it still requires manual adjudication. This honors §10/§11's high-risk caution and acceptance
   #12. The "script evidence does not contradict" fallback nuance is approximated as "GlotLID label is not
   high-risk." (Section 8, `compute_consensus`.) Confidence proxy = `primary_language_vote_share_with_top2`
   with widget thresholds `consensus_low_conf_vote_share=0.50` / `consensus_high_conf_vote_share=0.65`.
5. **Mixed-language consensus cond3** ("other model does not contradict the secondary cluster") is
   implemented as "the other model's secondary cluster is null or equals this model's." (Section 9.)
6. **`yt_lid_v3_channel_model_aggregation`** is an intermediate table NOT in the §2 list. It holds the
   per-model channel summary that §8 requires ("model-specific channel aggregations produced for both
   models", acceptance #8) and feeds Phases 6/10. It is widgeted and clearly commented.
7. **Ablation caveat:** inference ran only on the `min_clean_chars=40` valid universe, so character-threshold
   ablations can only *restrict* (e.g. 50). `v1_legacy_like_openlid` therefore approximates legacy *weights*
   on the v3 valid universe; it does NOT reproduce the legacy 20-char threshold. The ablation credible-mixed
   flag is a per-model approximation of §11 (no cross-model agreement term except the high-risk block).
   (Section 14.)
8. **`v3_description_weight_1_openlid`** equals the v3 default (channel_description weight is already 1.0), so
   its churn vs default should be ~0. Implemented literally.
9. **`audit_segments` isolation:** when `glotlid_mode=audit_segments`, GlotLID segment predictions are
   written for manual review but excluded from the main aggregation, agreement, consensus, mixed-language,
   Hindi/Indic, redirect, and ablation paths because the subset is OpenLID-biased.
10. **`array_compact`** is used in the validation-sampling step (Section 13). It requires Databricks Runtime
    with Spark 3.4+. Flag if the target cluster is older.
11. **Bucketed resumability:** large production outputs include `run_id` and `channel_hash_bucket`; writes
    use `replaceWhere` for the active run/bucket range. The default range covers all buckets, but production
    can retry a failed bucket range with the same `run_id`. Source tables are filtered to the active bucket
    before counts and deduplication.
12. **Production QA defaults:** `production_mode=true` disables ablation, validation sampling, notebook
    displays, and expensive duplicate-key counts unless `run_heavy_qa=true` and the relevant explicit widget
    are set.
13. **Scoped QA summaries:** run-level summaries carry `inference_hash_buckets`, `bucket_start`,
    `bucket_end`, and `is_full_bucket_range`. Partial bucket runs write bucket-scoped summaries; full-range
    runs replace the matching full-range `run_id` summary scope. Global capped sample tables are full-range
    only.
14. **Delta write safety:** scoped writes use `replaceWhere` without `overwriteSchema`. If an existing output
    table is missing required current columns or has incompatible partition metadata, the notebook performs a
    full overwrite only for a full bucket range; partial bucket runs fail fast with migration instructions.

## What still needs cluster verification (I could NOT run these)

A reviewer with cluster access should confirm:

- **Runtime is DBR 13.0+ / Spark 3.4+.** The notebook asserts this up front (it uses `array_sort(comparator)`
  and `array_compact`, added in Spark 3.4). On an older runtime it now fails fast with a clear message
  instead of crashing mid-pipeline.
- The `mapInPandas` fastText inference path (`predict_segments_compact`) executes and the per-worker model
  cache works for two models on one executor. Confirm whether the installed fastText binding accepts batched
  list input; the code falls back to row-wise prediction if not.
- `valid_segments.checkpoint(eager=True)` against the DBFS `checkpoint_dir` works (this mirrors the upstream
  `localCheckpoint`→DBFS fix; verify the dir is writable).
- Compact prediction row counts equal the valid-segment count for each enabled full-coverage model.
- In `prediction_output_mode=compact`, full-range runs clear only current-run rows from compatible legacy
  long segment tables; incompatible pre-refactor long tables are left untouched with a warning. In
  `long_sample`, sampled long output is deterministic for a fixed `run_id` and sample fraction; in
  `long_full`, final channel outputs match compact mode on a 10k-channel smoke test.
- Bucket retry is idempotent: rerunning the same `run_id` and bucket range replaces rows rather than
  duplicating them.
- With `run_heavy_qa=true`, the full OpenLID/GlotLID segment-id parity join passes. With production defaults,
  the lightweight row-count parity check passes.
- Run a first full-range job after this refactor, or explicitly recreate output tables, before attempting a
  partial bucket job against pre-refactor Delta tables.
- Higher-order functions used: `F.filter(..., lambda)` (Section 6), `F.array_compact` (Section 13),
  `F.posexplode_outer`, `F.element_at`, `F.slice`, `F.create_map`.
- `F.col(...).isin(*python_set)` splat calls (used widely for high-risk / ISO membership).
- **Acceptance #3** (segment-id universes equal across models) — asserted inline in Section 5; only runs in
  the default `all_valid_segments` mode.
- **Acceptance #4** (one row per post-dedup channel) — asserted inline in Section 12.
- Run the **deterministic 10,000-channel smoke test**: `limit_channels=10000`, both models on,
  `update_source_detected_language=false`. Confirm all tables write and the two inline asserts pass.
- Confirm column auto-detection against the real `yt_sl_channels` / `yt_sl_videos` schemas (timestamp
  columns for dedup ordering, text columns, video rank column). If detection misses, set the override
  widgets.

## Acceptance criteria (§17) — where each is satisfied

1. Deterministic 10k smoke test w/o modifying `yt_sl_channels` → Section 3 sampling + Section 12 guard
   (update flag default false). **Needs cluster run to confirm.**
2. `enable_glotlid=true`, `glotlid_mode=all_valid_segments` defaults → Section 1 widgets; asserted Section 15.
3. Both models on same valid `segment_id` universe → Section 5 assert.
4. One row per post-dedup channel → Section 12 assert.
5. Deterministic dedup + sampling → Section 3 (`row_number`, `xxhash64`, no `.dropDuplicates`/`.limit`).
6. ≥40 usable-letter threshold, letter-based → Section 4; asserted Section 15.
7. Full summaries without `.limit(100)` → Section 13 (only `display` is limited).
8. Per-model aggregations for both models → Section 7 (`yt_lid_v3_channel_model_aggregation`).
9. Final table has legacy + `openlid_*`/`glotlid_*` + comparison + consensus fields → Section 12.
10. Deterministic `consensus_status` rules → Section 8 `compute_consensus`.
11. Screen vs. credible, second-model support default → Section 9.
12. High-risk flagged, not recoded → Sections 8/9/11 (consensus NULL exact label; flags only).
13. Hindi/Indic candidates exported even when not primary/secondary → Section 10.
14. Romanized keywords word-boundary, never feed labels → Section 10 (`romanized_keyword_udf`, recall-only).
15. Redirect diagnostic combines label+Devanagari+keywords+source+other-model votes → Section 11.
16. Deterministic stratified validation sample → Section 13.
17. Ablation churn vs OpenLID default and consensus default → Section 14.
18. Fails clearly if an enabled model is unavailable → Section 2 (`ensure_hf_fasttext_model` raises).
19. README updated, no placeholder citations → `README_language_lid_v3.md` (the `[Sage Journals][2]`
    placeholder exists only in the superseded draft spec, not in any README).

## Notes for the reviewer

- The `# MAGIC` and `# COMMAND ----------` lines are Databricks notebook markers (comments in plain Python).
- All UDFs reference module-level constants (e.g. `ANALYSIS_CLUSTER_MAP`, `HIGH_RISK_LATIN_TAIL_LABELS`,
  `CONSENSUS_*` thresholds) captured via cloudpickle — the same pattern the legacy notebook used for
  `MIN_CLEAN_CHARS`. Confirm this still serializes on the target runtime.
- During editing a single null byte was accidentally introduced in the ablation sentinel string and removed;
  the null-byte guard above catches regressions.
