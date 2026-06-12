# Multi-label YouTube Topic Validation: Lessons Learned

Run: `category_topic_multilabel_random_1000_20260612`  
Date: 2026-06-12  
Primary target: exact observed YouTube API `topic_categories` label set from `dev_sean.default.channel_category`

## Run Design

- Sampled 1,000 random channels from `prod_tads.youtube_too.yt_sl_channels`.
- Held out the observed YouTube `topic_categories` array from model prompts.
- Used the full observed 62-label vocabulary, not a sample-specific or single-label category list.
- Split sample into 401 calibration channels and 599 heldout-test channels.
- 971/1,000 sampled channels had a nonempty observed category set.
- Mean observed label count was 2.617 labels/channel; mean prompt evidence was 9.739 recent videos/channel.
- Evaluation treated this as multi-label prediction: model outputs were calibrated against held-out category arrays, not judged against human gold-standard topic codes.

## Scored Models

Eight model result sets were fully parsed and scored:

- Anthropic: `claude-haiku-4-5`, `claude-opus-4-8`, `claude-sonnet-4-6`
- DeepSeek: `deepseek-v4-flash`, `deepseek-v4-pro`
- Gemini: `gemini-3.1-flash-lite`, `gemini-3.5-flash`
- OpenAI: `gpt-5-nano`

Not included in the current scored set:

- `gemini-3.1-pro-preview` was still running at the final status check.
- `gpt-5.4-mini` and `gpt-5.4-nano` completed with 1,000 failed requests after the fixed-schema retry.
- `gpt-5.5` was still in progress and already had many failed requests at the final status check.

## Headline Findings

Best heldout performance came from calibrated probabilities, not the model-reported positive-label arrays.

| Model / variant | Exact set match | Mean Jaccard | Micro-F1 | Precision | Recall |
|---|---:|---:|---:|---:|---:|
| Gemini 3.5 Flash, label threshold + closure | 0.220 | 0.589 | 0.713 | 0.676 | 0.754 |
| Gemini 3.5 Flash, label threshold | 0.204 | 0.575 | 0.703 | 0.676 | 0.733 |
| Claude Opus 4.8, label threshold + closure | 0.257 | 0.562 | 0.692 | 0.710 | 0.676 |
| Claude Opus 4.8, label threshold | 0.240 | 0.552 | 0.686 | 0.714 | 0.660 |
| Claude Sonnet 4.6, label threshold + closure | 0.179 | 0.546 | 0.672 | 0.615 | 0.741 |

Best naive baseline was `topk_avg_cardinality`: micro-F1 0.373 and mean Jaccard 0.254. The top LLM result roughly doubled baseline micro-F1.

## Agreement Patterns

The highest cross-model predicted-set agreement was between Gemini 3.5 Flash and Claude Opus 4.8: mean predicted-set Jaccard 0.636 and exact predicted-set agreement 0.306. Gemini 3.5 Flash also aligned strongly with Claude Sonnet 4.6 and Claude Haiku 4.5. DeepSeek was less aligned with the top Anthropic/Gemini cluster, and OpenAI `gpt-5-nano` was both low-performing and weakly calibrated.

## Label-Level Issues

Weak labels were mostly broad or semantically overlapping categories:

- Broad labels: `Society`, `Entertainment`, `Film`, `Hobby`, `Knowledge`
- Music subtypes: `Music_of_Asia`, `Electronic_music`
- Game subtypes: `Action_game`, `Action-adventure_game`, `Role-playing_video_game`, `Video_game_culture`
- Style/function labels: `Humour`, `Television_program`

These are not necessarily “bad labels.” They are places where YouTube’s applied taxonomy is hard to infer from channel evidence alone, especially when broad and narrow labels co-occur.

## Visualizations

- Heldout model metrics: `heldout_model_metrics_bar.png`
- Prediction variant comparison: `prediction_variant_comparison.png`
- Pairwise model agreement heatmap: `model_pairwise_predicted_set_jaccard.png`
- Per-label F1 heatmap: `label_f1_heatmap_top_labels.png`
- Label-cardinality error histogram: `label_cardinality_error_histogram.png`
- Multi-label confusion matrix for best model/variant: `confusion_matrix_gemini_3_5_flash_closure.png`

## Operational Lessons

1. The main notebook should not start with unconditional `%pip`.
   The first attempt spent time before any inspectable output existed. The run now materializes prompt/request artifacts before optional provider SDK work.

2. Provider submission must be incremental and ordered.
   Direct providers such as DeepSeek should run after asynchronous batch providers. Batch job rows should be written after each provider submission so slow direct calls cannot hide already-submitted provider IDs.

3. Delta upsert patterns must not read and overwrite the same table lazily.
   The initial incremental upsert collapsed `batch_jobs` to the last row. The fix collects preserved small-table rows into Python and writes an independent replacement DataFrame.

4. OpenAI structured-output schemas reject `uniqueItems`.
   Removing `uniqueItems` fixed `gpt-5-nano`, which then completed 1,000/1,000 requests. The remaining OpenAI model aliases need separate provider-error inspection before future use.

5. Calibration is essential.
   The model-reported label arrays underperform calibrated probability thresholds. The strongest variant was per-label threshold calibration, with a small additional gain from empirical closure postprocessing.

## Implemented Changes

- Removed `uniqueItems` from the canonical OpenAI JSON schema.
- Added OpenAI fixed-schema retry notebook.
- Added OpenAI error-inspection notebook.
- Added missing-provider submission-only notebook for Anthropic/Gemini SDK-backed submissions.
- Added batch-job repair notebook.
- Fixed `batch_jobs` upsert to avoid lazy self-overwrite.
- Reordered future provider submission so direct providers run last.
- Changed the default validation model set to exclude failed/slow OpenAI aliases and `gemini-3.1-pro-preview`.
- Changed the default submit provider filter to `anthropic,gemini,deepseek`.
- Fixed OpenAI error-sample parsing for nested batch error records under `response.body.error`.
- Added multi-label confusion-matrix plotting notebook.
- Copied all generated plots into this local report directory.

## Recommended Next Run

Use the same 1,000-channel sample design and score at least:

- Gemini 3.5 Flash
- Claude Opus 4.8
- Claude Sonnet 4.6
- Claude Haiku 4.5
- DeepSeek v4 Pro

Treat OpenAI `gpt-5-nano` as low priority for this task unless cost is the primary constraint. Do not include `gpt-5.4-mini`, `gpt-5.4-nano`, or `gpt-5.5` again until their post-schema-fix provider errors are inspected and resolved.

The default runnable configuration has been updated to this stable set:

- `claude-opus-4-8`
- `claude-sonnet-4-6`
- `claude-haiku-4-5`
- `gemini-3.5-flash`
- `gemini-3.1-flash-lite`
- `deepseek-v4-pro`
- `deepseek-v4-flash`
