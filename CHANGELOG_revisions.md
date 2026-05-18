# Revision changelog after external review

## Language notebook

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
