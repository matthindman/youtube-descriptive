# Revision changelog after external review

## Language notebook — v3 dual-model revision (OpenLID-v3 + GlotLID)

Reworked `01_language_openlid_v3_databricks.py` into a v3 dual-model consensus pipeline per
`lang_detect_revision_spec.md`. The single-model first cut remains in git history at commit `d3cb137`;
see `README_language_lid_v3.md` for full documentation.

- GlotLID now runs as a peer model by default on **all valid segments** (not a low-confidence subset);
  `audit_segments` and `glotlid_native_audit` are optional non-default modes.
- New `yt_lid_v3_*` output family (segments input, per-model segment predictions, channel votes, per-model
  aggregation, segment/channel model comparison + consensus, final channels, mixed-language candidates,
  Hindi/Indic audit, high-risk redirect diagnostic, QA summaries, manual validation sample, dedupe QA,
  ablation summary). The legacy `yt_lid_openlid_v3_*` tables are never overwritten.
- Deterministic channel/video dedup (`row_number()`, never `.dropDuplicates()`) and deterministic
  `xxhash64` smoke sampling (never `.limit()` on unordered data); dedupe QA table.
- Letter-based validity (`min_clean_chars=40`, non-Latin exception at 12 letters / 0.60 dominant-script
  share) with per-script letter metrics, replacing whitespace-padded length thresholds.
- Robust `parse_lid_label`; long-format top-k predictions; `top_k=5`.
- Per-model channel aggregation with top-1/top-2 admission rules, segment-type + rank weights (length
  weighting dropped for comparability), vote shares, margins, and `language_votes_json`.
- Deterministic `consensus_status` classifier; high-risk tail labels flagged (and reviewed) but never
  hard-recoded.
- Screen vs. credible mixed-language distinction with second-model support by default.
- Hindi/Indic recall diagnostics with word-boundary romanized-keyword flags (recall-only; never feed
  labels), and a high-risk redirect diagnostic combining Devanagari/keywords/source/other-model signals.
- Deterministic stratified manual-validation sample and ablation summaries with primary-label churn vs.
  both the v3 default OpenLID and v3 default consensus.
- Fails clearly if an enabled model binary is unavailable; source table not modified unless explicitly
  enabled.

## Language notebook — first cut (single-model OpenLID-v3)

Accepted and fixed:

- Kept `openlid-v3.bin` as the default OpenLID-v3 filename, but added fallback download attempts for `openlid-v3.bin` and `model.bin` because the Hugging Face model card and file list are inconsistent.
- Added deterministic `video_id` tie-breaking when selecting recent videos.
- Added top-2 segment prediction voting with a lower weight for bilingual/mixed-language detection.
- Added optional segment-length weighting.
- Added ISO/script fields for top-2 and top-3 segment predictions.
- Added `source_update_format` to avoid silently changing downstream expectations for `yt_sl_channels.detected_language` if source-table update is enabled.
- Changed valid segment counting to use distinct segment IDs rather than summing per-language vote counts.

Left unchanged after review:

- Lowercasing and cleanup are retained because they are consistent with the OpenLID-v3 preprocessing pattern and acceptable for the first cut.
- GlotLID remains commented out for the first-cut analysis.

## Category notebook

Accepted and fixed:

- OpenAI GPT-5/o-series routing now defaults to the Responses API in `auto` mode.
- OpenAI Responses requests now use `max_output_tokens`; Chat Completions uses `max_tokens` or `max_completion_tokens` as appropriate.
- Temperature is omitted by default rather than forcing `temperature=0.0`.
- Gemini defaults now use `gemini-3.1-pro-preview` and `gemini-3.1-flash-lite-preview`; `google-genai>=1.51.0` is pinned.
- Gemini batch requests now use structured JSON output through `response_format`.
- `gold_*` names were replaced with `reference_*` names.
- Added support for an expert-coded reference table through `existing_label_source=expert_table`.
- Added channel-level reference-label thresholds for video-derived labels: minimum labeled videos and minimum winning-category agreement fraction.
- Replaced Spark `rand()` sampling with stable hash-based sampling.
- Added deterministic `video_id` tie-breaking when selecting videos for prompts.
- Fixed macro-F1 so missed classes count as zero rather than disappearing through null averaging.
- Fixed numeric category-ID tie-breaking.
- Made the Gemini parser less redundant and the JSON-object extractor more robust.

Left mostly unchanged after review:

- Anthropic batch submission still loads one JSONL chunk into memory. The default chunk size remains conservative at 10,000 requests.
- The notebook still treats existing labels as a benchmark, not as expert-coded validity.
