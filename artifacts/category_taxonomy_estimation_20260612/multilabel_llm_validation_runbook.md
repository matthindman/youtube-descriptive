# Multi-label YouTube Topic Category LLM Validation Runbook

Date: 2026-06-12

## Target

Predict the exact observed YouTube API `topic_categories` array from:

```text
dev_sean.default.channel_category
```

This is a 62-label multi-label target. The goal is to predict the current YouTube label-generating process, not to replace it with human gold-standard coding.

## Key Design Choices

- Ground truth is the raw observed category set after URL-to-slug normalization.
- `topic_categories[0]` is not used as primary truth.
- The prompt never includes the held-out category labels.
- Models return probabilities for every observed label plus their own positive set.
- Thresholds are calibrated on a deterministic calibration split.
- Final validation is on a deterministic heldout-test split.
- Co-label/closure rules are optional post-processing diagnostics, not substitute ground truth.

## Databricks Notebooks

```text
.codex_databricks/run_category_topic_multilabel_1000_20260612.py
.codex_databricks/submit_category_topic_multilabel_missing_providers_20260612.py
.codex_databricks/import_evaluate_category_topic_multilabel_20260612.py
.codex_databricks/analyze_category_topic_multilabel_20260612.py
.codex_databricks/plot_category_topic_multilabel_confusion_20260612.py
```

Repair/diagnostic notebooks from the 2026-06-12 run:

```text
.codex_databricks/repair_category_topic_multilabel_batch_jobs_20260612.py
.codex_databricks/retry_category_topic_multilabel_openai_fixed_schema_20260612.py
.codex_databricks/inspect_category_topic_multilabel_openai_errors_20260612.py
```

Reusable batch-status/download notebooks:

```text
.codex_databricks/check_category_topic_batch_status_20260611.py
.codex_databricks/download_category_topic_batch_results_20260611.py
```

## Job Configs

```text
.codex_databricks/job_category_topic_multilabel_1000_submit_20260612.json
.codex_databricks/job_category_topic_multilabel_status_20260612.json
.codex_databricks/job_category_topic_multilabel_download_20260612.json
.codex_databricks/job_category_topic_multilabel_import_evaluate_20260612.json
.codex_databricks/job_category_topic_multilabel_analysis_20260612.json
.codex_databricks/job_category_topic_multilabel_confusion_plot_20260612.json
.codex_databricks/job_category_topic_multilabel_submit_missing_providers_20260612.json
.codex_databricks/job_category_topic_multilabel_repair_batch_jobs_20260612.json
.codex_databricks/job_category_topic_multilabel_openai_retry_fixed_schema_20260612.json
.codex_databricks/job_category_topic_multilabel_openai_error_inspect_20260612.json
```

Default run id:

```text
category_topic_multilabel_random_1000_20260612
```

Default output prefix:

```text
yt_category_topic_multilabel_1000
```

## Run Order

1. Start cluster `0601-203643-bkxsqffg`.

2. Import the notebooks above to:

```text
/Users/matt.hindman@researchaccelerator.org/lid_v3_too_20260609/
```

3. Submit:

```text
databricks jobs submit --json @.codex_databricks/job_category_topic_multilabel_1000_submit_20260612.json --profile matt.hindman@researchaccelerator.org --no-wait
```

This creates prompt inputs, request rows, JSONL files, and provider batch/direct jobs. The default model set now excludes OpenAI aliases and `gemini-3.1-pro-preview`, because those models failed or were still running in the 2026-06-12 validation run. Override `models_json` only when intentionally re-testing those providers.

Default submitted models:

```text
claude-opus-4-8
claude-sonnet-4-6
claude-haiku-4-5
gemini-3.5-flash
gemini-3.1-flash-lite
deepseek-v4-pro
deepseek-v4-flash
```

If Anthropic/Gemini SDKs are unavailable on the cluster, run the submission-only notebook after prompt/request materialization:

```text
databricks jobs submit --json @.codex_databricks/job_category_topic_multilabel_submit_missing_providers_20260612.json --profile matt.hindman@researchaccelerator.org --no-wait
```

4. Poll status:

```text
databricks jobs submit --json @.codex_databricks/job_category_topic_multilabel_status_20260612.json --profile matt.hindman@researchaccelerator.org --no-wait
```

5. Download completed batch results:

```text
databricks jobs submit --json @.codex_databricks/job_category_topic_multilabel_download_20260612.json --profile matt.hindman@researchaccelerator.org --no-wait
```

DeepSeek direct results are written during submission and do not need provider download.

6. Import and evaluate:

```text
databricks jobs submit --json @.codex_databricks/job_category_topic_multilabel_import_evaluate_20260612.json --profile matt.hindman@researchaccelerator.org --no-wait
```

7. Run analysis/plots:

```text
databricks jobs submit --json @.codex_databricks/job_category_topic_multilabel_analysis_20260612.json --profile matt.hindman@researchaccelerator.org --no-wait
```

8. Render the best-model multi-label confusion matrix:

```text
databricks jobs submit --json @.codex_databricks/job_category_topic_multilabel_confusion_plot_20260612.json --profile matt.hindman@researchaccelerator.org --no-wait
```

Optional OpenAI diagnostics:

```text
databricks jobs submit --json @.codex_databricks/job_category_topic_multilabel_openai_error_inspect_20260612.json --profile matt.hindman@researchaccelerator.org --no-wait
```

Run the OpenAI fixed-schema retry only when deliberately re-testing OpenAI models. It removes schema keywords rejected by OpenAI structured outputs and writes replacement batch-job rows.

## Output Tables

Core run tables:

```text
dev_sean.matt.yt_category_topic_multilabel_1000_prompt_inputs
dev_sean.matt.yt_category_topic_multilabel_1000_requests
dev_sean.matt.yt_category_topic_multilabel_1000_batch_files
dev_sean.matt.yt_category_topic_multilabel_1000_batch_jobs
dev_sean.matt.yt_category_topic_multilabel_1000_batch_status_check
dev_sean.matt.yt_category_topic_multilabel_1000_result_files
```

Parsed/evaluation tables:

```text
dev_sean.matt.yt_category_topic_multilabel_1000_raw_results
dev_sean.matt.yt_category_topic_multilabel_1000_channel_predictions
dev_sean.matt.yt_category_topic_multilabel_1000_label_predictions
dev_sean.matt.yt_category_topic_multilabel_1000_thresholds
dev_sean.matt.yt_category_topic_multilabel_1000_model_metrics
dev_sean.matt.yt_category_topic_multilabel_1000_label_metrics
dev_sean.matt.yt_category_topic_multilabel_1000_channel_metrics
dev_sean.matt.yt_category_topic_multilabel_1000_baselines
dev_sean.matt.yt_category_topic_multilabel_1000_model_pairwise_set_agreement
```

Analysis outputs:

```text
dev_sean.matt.yt_category_topic_multilabel_1000_plot_artifacts
dev_sean.matt.yt_category_topic_multilabel_1000_high_error_cases
dev_sean.matt.yt_category_topic_multilabel_1000_openai_error_samples
```

Plot files:

```text
/dbfs/FileStore/youtube_category_topic_multilabel_batches/analysis/category_topic_multilabel_random_1000_20260612/
```

## Primary Metrics

Use heldout-test rows in:

```text
dev_sean.matt.yt_category_topic_multilabel_1000_model_metrics
```

Primary prediction variants:

- `model_reported`
- `prob_global_threshold`
- `prob_label_threshold`
- `prob_label_threshold_closure_postprocessed`

Primary metrics:

- `micro_precision`
- `micro_recall`
- `micro_f1`
- `macro_f1_present_labels`
- `exact_set_match_rate`
- `mean_jaccard_similarity`
- `hamming_loss`
- `mean_label_cardinality_error`
- `mean_abs_label_cardinality_error`

The preferred calibration-only variant is:

```text
prob_label_threshold
```

because it uses label-specific thresholds fit on calibration channels.

The preferred headline variant for the 2026-06-12 run was:

```text
prob_label_threshold_closure_postprocessed
```

because empirically strong co-label closure added a small heldout gain. Always report the non-closure `prob_label_threshold` result next to it, because closure is a post-processing ablation rather than a change to the reference labels.

## Baselines

Baselines are written to:

```text
dev_sean.matt.yt_category_topic_multilabel_1000_baselines
```

Included baselines:

- `baseline_empty_set`
- `baseline_topk_avg_cardinality`
- `baseline_prevalence_ge_05`

## Co-label Rules

The evaluator optionally applies strong empirical co-label rules from:

```text
dev_sean.matt.yt_channel_topic_taxonomy_inferred_edges_20260612
```

Only this variant uses them:

```text
prob_label_threshold_closure_postprocessed
```

This is a post-processing ablation scored against the raw exact YouTube label set. It does not expand or alter the reference labels.

## Expected Caveats

- A random 1,000-channel sample will estimate common-label performance better than rare-label performance.
- Rare labels may need a separate stratified supplement after the initial run.
- LLM self-reported probabilities may be miscalibrated; the evaluator fits thresholds but does not fully solve probability calibration.
- Agreement with YouTube labels is not the same as expert-coded semantic validity.

## Operational Guardrails

- Do not add unconditional `%pip` to the main materialization notebook. Prompt/request tables should be inspectable even if optional provider SDK setup fails.
- Direct providers should submit after asynchronous batch providers.
- Write `batch_jobs` incrementally after each provider submission.
- Do not lazily read and overwrite the same Delta table in a single replacement write. Collect small preserved registry rows first, then write an independent replacement DataFrame.
- Keep OpenAI `gpt-5.4-mini`, `gpt-5.4-nano`, `gpt-5.5`, and `gpt-5-nano` out of default scoring until provider errors and accuracy/cost tradeoffs are revalidated.
- Keep `gemini-3.1-pro-preview` opt-in until its latency/completion behavior is verified on this workflow.
