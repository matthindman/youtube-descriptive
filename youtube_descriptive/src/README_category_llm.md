# README: LLM-based YouTube category classification bake-off on Databricks

## Purpose

`02_category_llm_youtube_databricks.py` prepares and evaluates LLM-based classification of YouTube channels into a 15-category YouTube-style taxonomy. It is a validation-first bake-off across model families and model sizes. It should be run on labeled/reference data before any full-corpus classification.

The workflow is:

1. Run the OpenLID-v3 language notebook first.
2. Build a stratified validation sample within detected-language groups and reference category labels.
3. Create provider-specific request JSONL files for OpenAI, Anthropic, Gemini, and DeepSeek.
4. Optionally submit those files from Databricks Secrets, or hand the files to the colleague managing API access.
5. Import provider result JSONL files.
6. Evaluate against reference labels and compute model agreement.
7. Only after validation, generate full-unlabeled batch files for the selected model or model ensemble.

## Files and default outputs

- Notebook: `02_category_llm_youtube_databricks.py`
- Prompt inputs: `prod_tads.youtube.yt_category_llm_prompt_inputs`
- Request map: `prod_tads.youtube.yt_category_llm_requests`
- Batch file registry: `prod_tads.youtube.yt_category_llm_batch_files`
- Batch job registry: `prod_tads.youtube.yt_category_llm_batch_jobs`
- Raw provider results: `prod_tads.youtube.yt_category_llm_raw_results`
- Parsed predictions: `prod_tads.youtube.yt_category_llm_predictions`
- Evaluation metrics: `prod_tads.youtube.yt_category_llm_eval_metrics`
- Pairwise agreement: `prod_tads.youtube.yt_category_llm_model_agreement`

## Source tables expected

Defaults:

```text
channels_table = yt_sl_channels
videos_table = yt_sl_videos
language_table = yt_lid_openlid_v3_channels
```

The screenshots show `ai_label` in `yt_sl_videos`, so the default reference-label widgets are:

```text
existing_label_source = videos
existing_label_col = ai_label
```

The notebook now uses `reference_*` names rather than `gold_*`. These labels are benchmarks for the bake-off, not necessarily expert-coded truth.

## Reference-label options

### Video-level reference labels, default

```text
existing_label_source = videos
existing_label_col = ai_label
min_reference_labeled_videos = 3
min_reference_agreement_fraction = 0.50
```

A channel receives a reference label only when at least three labeled videos are available and the winning category accounts for at least 50% of mapped video labels. Adjust the thresholds if the source labels are sparser or cleaner than expected.

### Channel-level reference labels

```text
existing_label_source = channels
existing_label_col = <channel_label_column>
```

### Expert-coded labels

For the final paper, use a separate expert-coded table when available:

```text
existing_label_source = expert_table
expert_label_table = <catalog>.<schema>.<table>
expert_label_channel_id_col = channel_id
expert_label_category_col = category_id
```

This option uses the same downstream prompt generation and evaluation code.

## Category taxonomy

The notebook uses 15 YouTube-style categories:

| ID | Category |
|---:|---|
| 1 | Film & Animation |
| 2 | Autos & Vehicles |
| 10 | Music |
| 15 | Pets & Animals |
| 17 | Sports |
| 19 | Travel & Events |
| 20 | Gaming |
| 22 | People & Blogs |
| 23 | Comedy |
| 24 | Entertainment |
| 25 | News & Politics |
| 26 | Howto & Style |
| 27 | Education |
| 28 | Science & Technology |
| 29 | Nonprofits & Activism |

The label normalizer accepts IDs, exact names, ampersand/`and` variants, and common aliases.

## Python libraries

The notebook installs:

```python
%pip install "openai>=2.0.0" anthropic "google-genai>=1.51.0" pandas pyarrow tenacity
```

Then it calls:

```python
dbutils.library.restartPython()
```

## Model configuration

The `models_json` widget controls the bake-off. Replace defaults with exact model IDs available in the relevant API accounts.

Default configuration:

```json
[
  {"provider": "openai", "model": "gpt-5.5", "tier": "frontier"},
  {"provider": "openai", "model": "gpt-5.4", "tier": "mid"},
  {"provider": "openai", "model": "gpt-5.4-mini", "tier": "small"},
  {"provider": "openai", "model": "gpt-5.4-nano", "tier": "nano"},
  {"provider": "openai", "model": "gpt-5-nano", "tier": "nano_low_cost"},
  {"provider": "anthropic", "model": "claude-opus-4-8", "tier": "frontier"},
  {"provider": "anthropic", "model": "claude-sonnet-4-6", "tier": "mid"},
  {"provider": "anthropic", "model": "claude-haiku-4-5", "tier": "small"},
  {"provider": "gemini", "model": "gemini-3.1-pro-preview", "tier": "frontier"},
  {"provider": "gemini", "model": "gemini-3.5-flash", "tier": "mid"},
  {"provider": "gemini", "model": "gemini-3.1-flash-lite", "tier": "small"},
  {"provider": "deepseek", "model": "deepseek-v4-pro", "tier": "frontier"},
  {"provider": "deepseek", "model": "deepseek-v4-flash", "tier": "small"}
]
```

Leave `temperature` blank by default:

```text
run_id = default
temperature =
max_output_tokens = 2000
openai_reasoning_effort = minimal
gemini_thinking_level = low
deepseek_thinking_type = disabled
deepseek_max_output_tokens = 600
submit_batches = false
skip_existing_submitted_batches = true
```

The notebook omits temperature unless you explicitly set it. This avoids provider/model-specific failures and follows the current provider guidance more closely than forcing `temperature=0.0`. Category outputs are also requested as compact one-line JSON; the 2000-token global default gives enough headroom for providers whose reasoning/thinking tokens share the output budget. DeepSeek is capped separately because it runs as synchronous direct requests, and hidden reasoning output can dominate latency.

OpenAI-specific widgets:

```text
openai_endpoint_mode = auto          # auto, responses, chat_completions
openai_reasoning_effort = minimal    # blank to omit
```

In `auto` mode, models whose names start with `gpt-5` or common o-series prefixes are routed to `/v1/responses`; other OpenAI models use `/v1/chat/completions`. Responses API requests use `max_output_tokens`; Chat Completions requests use `max_tokens` or `max_completion_tokens` as appropriate.

Gemini-specific widget:

```text
gemini_thinking_level = low
```

The Gemini request file uses native Batch API JSONL with a `key` and a `request` object, plus JSON output through `response_mime_type=application/json`.

DeepSeek-specific widgets:

```text
deepseek_thinking_type = disabled
deepseek_reasoning_effort =
deepseek_max_output_tokens = 600
deepseek_max_workers = 16
```

DeepSeek uses the OpenAI-compatible `https://api.deepseek.com/chat/completions` path directly and writes
parser-compatible result JSONL under `results_input_dir`. Result rows include `_deepseek_direct_metadata`
with per-request duration, attempts, status codes, thinking setting, and max-token setting for runtime
diagnosis.

## Databricks secrets

If submitting batches from the notebook:

Use the shared Databricks scope configured for this project:

Notebook defaults:

```text
secret_scope = youtube-llm-keys
openai_secret_key = openai-api-key
anthropic_secret_key = anthropic-api-key
gemini_secret_key = gemini-api-key
deepseek_secret_key = deepseek-api-key
```

The notebook submits OpenAI, Anthropic, and Gemini through their provider batch APIs. DeepSeek is executed
as direct OpenAI-compatible chat-completion calls and imported through the same result parser. By default,
DeepSeek requests explicitly set non-thinking mode.

## Recommended first run

```text
run_id = category_labeled_validation_YYYYMMDD
run_mode = labeled_validation
n_per_language_category = 25
max_total_channels = 50000
videos_per_channel = 10
submit_batches = false
import_results = false
```

This writes prompt inputs and JSONL batch files without submitting them. Reuse the same `run_id` for
submission and import; request IDs include `run_id`, so changing it between sessions will orphan provider
results.

Batch files are written under:

```text
/dbfs/FileStore/youtube_category_batches/<RUN_ID>/<provider>/<model>/chunk_00000.jsonl
```

The batch registry table is:

```text
prod_tads.youtube.yt_category_llm_batch_files
```

Sampling is deterministic: the notebook uses a stable hash of `random_seed` and `channel_id`, not Spark’s partition-sensitive `rand()` ordering.

## Provider batch-file formats

- OpenAI: Batch API JSONL with `custom_id`, `method`, `url`, and `body`.
- Anthropic: Message Batches format with `custom_id` and `params`.
- Gemini: native Gemini Batch API JSONL with `key` and `request`.

The request table maps every request ID to `channel_id`, `reference_category_id`, detected language, model, and provider. Do not rely on provider result order.

For full-corpus work, keep:

```text
max_requests_per_file = 10000
```

unless your API owner has confirmed larger provider limits and file-size safety.

## Importing results

After provider jobs finish, place result JSONL files under:

```text
/dbfs/FileStore/youtube_category_batches/results
```

Then set:

```text
import_results = true
results_input_dir = /dbfs/FileStore/youtube_category_batches/results
```

The parser handles OpenAI Responses, OpenAI Chat Completions, Anthropic Message Batches, and common Gemini Batch output shapes. It joins results back through `request_id` / `custom_id` / `key` and drops rows that do not match the active run's request registry.

Category output tables are written by `run_id`. Re-running the same `run_id` replaces that run's prompt, request, raw-result, parsed-prediction, metric, and agreement rows without overwriting other runs. Parsed predictions also dedupe duplicate result files to one row per request.

## Evaluation outputs

The notebook computes:

- valid prediction rate;
- parse-error rate;
- accuracy against reference labels;
- macro-F1, macro-precision, and macro-recall;
- language-stratified accuracy;
- reported confidence;
- pairwise model agreement;
- consensus preview.

Macro-F1 uses a full model-by-15-category grid, so categories that are present in the reference labels but never predicted count as zero instead of being silently dropped.

## Decision rule before full-corpus classification

Do not classify the full corpus until the labeled validation bake-off shows:

1. high agreement with reference labels;
2. strong macro-F1, not just accuracy;
3. stable performance across major detected languages;
4. low parse-error rates;
5. high pairwise agreement between the best small model and frontier models;
6. category-specific error analysis for News & Politics, Education, Science & Technology, Entertainment, Gaming, and People & Blogs.

If no small model performs comparably to the frontier models, move to teacher-student distillation: use frontier models plus expert validation for a training set, then train a smaller classifier for full-corpus inference.

## Full unlabeled run

After validation:

```text
run_mode = full_unlabeled
max_total_channels = 0
models_json = [{"provider": "<provider>", "model": "<best_small_model>", "tier": "small"}]
```

Then regenerate batch files and submit using the same workflow.

## Important cautions

- The notebook does not include reference labels in the prompt.
- Agreement with `ai_label` or other platform-style labels is not the same as expert-coded validity.
- Use an expert-coded validation table for the final Nature/Science paper.
- Keep prompts, model IDs, provider settings, run IDs, and raw batch outputs for supplementary methods and reproducibility.
- Do not hard-code cost estimates in the paper; benchmark token counts and current provider pricing immediately before submission.
