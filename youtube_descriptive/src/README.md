# YouTube channel language and category classification on Databricks

This folder contains Databricks source-format notebooks and READMEs for the language and category classification pipeline.

## Deliverables

1. `01_language_openlid_v3_databricks.py`
   - Segment-level language classification with OpenLID-v3 and GlotLID.
   - Deterministic video selection.
   - Channel-level aggregation, model comparison, consensus labels, QA summaries, and audit outputs.

2. `01b_language_lid_v3_subscriber_cohort_analysis_databricks.py`
   - Builds top-100k-subscriber and 100k subscriber-band random cohorts by default.
   - Runs the v3 language notebook for each cohort with full diagnostics.
   - Writes combined analysis, summary, review-queue, and cohort metadata tables.

3. `README_language_openlid_v3.md`
   - Databricks setup, model upload/download, widget settings, output schema, validation notes, and source-table update cautions.

4. `02_category_llm_youtube_databricks.py`
   - LLM bake-off for YouTube-style category classification.
   - Language-stratified validation sampling.
   - OpenAI, Anthropic, and Gemini batch JSONL generation.
   - Optional batch submission through Databricks Secrets.
   - Result parsing, reference-label evaluation, macro-F1, language-stratified metrics, and pairwise model agreement.

5. `README_category_llm.md`
   - API-key setup, reference-label options, model configuration, batch-file workflow, evaluation procedure, and full-corpus guidance.

## Importing notebooks into Databricks

Workspace UI: import each `.py` file as a Databricks notebook.

Databricks CLI:

```bash
databricks workspace import ./01_language_openlid_v3_databricks.py /Users/<you>/youtube/01_language_openlid_v3_databricks.py --format SOURCE --language PYTHON --overwrite
databricks workspace import ./01b_language_lid_v3_subscriber_cohort_analysis_databricks.py /Users/<you>/youtube/01b_language_lid_v3_subscriber_cohort_analysis_databricks.py --format SOURCE --language PYTHON --overwrite
databricks workspace import ./02_category_llm_youtube_databricks.py /Users/<you>/youtube/02_category_llm_youtube_databricks.py --format SOURCE --language PYTHON --overwrite
```

Run order:

1. `01_language_openlid_v3_databricks.py` with `limit_channels=10000` for a smoke test.
2. Full language run after the smoke test succeeds.
3. `01b_language_lid_v3_subscriber_cohort_analysis_databricks.py` for the top-100k and random subscriber-band analysis cohorts.
4. `02_category_llm_youtube_databricks.py` in `labeled_validation` mode with `submit_batches=false`.
5. Submit provider batches or hand JSONL files to the API owner.
6. Import results and evaluate before any full-unlabeled run.
