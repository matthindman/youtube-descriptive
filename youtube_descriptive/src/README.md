# YouTube channel language and category classification on Databricks

This folder contains Databricks source-format notebooks and READMEs for the language and category
classification pipeline. For language detection, `README_language_lid_v3.md` is the current source of truth.
Historical implementation plans and review handoffs are not active runbooks.

## Deliverables

1. `01_language_openlid_v3_databricks.py`
   - Segment-level language classification with OpenLID-v3 and GlotLID.
   - Deterministic video selection.
   - Channel-level aggregation, model comparison, consensus labels, QA summaries, and audit outputs.

2. `01b_language_lid_v3_subscriber_cohort_analysis_databricks.py`
   - Builds top-100k-subscriber and 100k subscriber-band random cohorts by default.
   - Runs the v3 language notebook for each cohort with full diagnostics.
   - Writes combined analysis, summary, review-queue, and cohort metadata tables.

3. `README_language_lid_v3.md`
   - Current dual-model language-detection documentation.
   - Runtime requirements, widgets, output tables, QA/validation outputs, model-binary paths, and source-table update cautions.

4. `02_category_llm_youtube_databricks.py`
   - LLM bake-off for YouTube-style category classification.
   - Language-stratified validation sampling.
   - OpenAI, Anthropic, and Gemini batch JSONL generation.
   - Optional batch submission through Databricks Secrets.
   - Result parsing, reference-label evaluation, macro-F1, language-stratified metrics, and pairwise model agreement.

5. `README_category_llm.md`
   - API-key setup, reference-label options, model configuration, batch-file workflow, evaluation procedure, and full-corpus guidance.

## Historical language-detection records

- `lang_detect_revision_spec.md`: v3 design contract used for the rewrite.
- Earlier implementation plans, review handoffs, and single-model OpenLID docs are archived in git history or
  marked as historical if still present locally. Check current notebook code and `README_language_lid_v3.md`
  before treating older files as active work.

## Importing notebooks into Databricks

Workspace UI: import each `.py` file as a Databricks notebook.

Databricks CLI:

```bash
databricks workspace import ./01_language_openlid_v3_databricks.py /Users/<you>/youtube/01_language_openlid_v3_databricks.py --format SOURCE --language PYTHON --overwrite
databricks workspace import ./01b_language_lid_v3_subscriber_cohort_analysis_databricks.py /Users/<you>/youtube/01b_language_lid_v3_subscriber_cohort_analysis_databricks.py --format SOURCE --language PYTHON --overwrite
databricks workspace import ./02_category_llm_youtube_databricks.py /Users/<you>/youtube/02_category_llm_youtube_databricks.py --format SOURCE --language PYTHON --overwrite
```

Run order:

1. `01_language_openlid_v3_databricks.py` with a bounded `limit_channels` and positive `videos_per_channel`
   for smoke or validation runs.
2. Inspect `yt_lid_v3_preflight_estimate` before committing a larger run to inference.
3. Run the full language workflow after smoke/validation checks pass.
4. Run `01b_language_lid_v3_subscriber_cohort_analysis_databricks.py` only for subscriber-cohort analyses.
5. Run `02_category_llm_youtube_databricks.py` in `labeled_validation` mode with `submit_batches=false`.
6. Submit provider batches or hand JSONL files to the API owner.
7. Import results and evaluate before any full-unlabeled category run.
