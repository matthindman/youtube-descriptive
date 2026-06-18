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

6. `youtube_topic_treemap_v2.py`
   - Hierarchy-aware YouTube `topicCategories` treemap pipeline.
   - Reads `config/youtube_topic_hierarchy_v2.yaml`, writes channel-topic projection/allocation tables, diagnostics, and Plotly treemap HTML.

7. `treemap_spec.md`
   - Design contract for the hierarchy-aware top-of-ocean YouTube topic treemap.
   - Specifies topic hierarchy config, allocation rules, diagnostics, artifacts, and acceptance checks.

## Historical language-detection records

- `lang_detect_revision_spec.md`: v3 design contract used for the rewrite.
- Earlier implementation plans, review handoffs, and single-model OpenLID docs are archived in git history or
  marked as historical if still present locally. Check current notebook code and `README_language_lid_v3.md`
  before treating older files as active work.

## Importing notebooks into Databricks

Workspace UI: import each `.py` file as a Databricks notebook.

Databricks CLI:

```bash
databricks workspace import /Users/<you>/youtube/01_language_openlid_v3_databricks.py --file ./01_language_openlid_v3_databricks.py --format SOURCE --language PYTHON --overwrite
databricks workspace import /Users/<you>/youtube/01b_language_lid_v3_subscriber_cohort_analysis_databricks.py --file ./01b_language_lid_v3_subscriber_cohort_analysis_databricks.py --format SOURCE --language PYTHON --overwrite
databricks workspace import /Users/<you>/youtube/02_category_llm_youtube_databricks.py --file ./02_category_llm_youtube_databricks.py --format SOURCE --language PYTHON --overwrite
databricks workspace import /Users/<you>/youtube/youtube_topic_treemap_v2.py --file ./youtube_topic_treemap_v2.py --format SOURCE --language PYTHON --overwrite
```

For `youtube_topic_treemap_v2.py`, run from a Databricks Repo checkout when possible so
`config/youtube_topic_hierarchy_v2.yaml` is available. If importing the notebook by itself, import or upload the
YAML too and set the `hierarchy_config_path` widget to that readable workspace, `/dbfs`, or `dbfs:/` path.

One-command Databricks launch for the v2 treemap, using the current CLI syntax and existing project cluster:

```bash
bash .codex_databricks/run_youtube_topic_treemap_v2_20260617.sh
```

The helper runs all Databricks CLI calls with
`env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org ...`. It imports
`youtube_topic_treemap_v2.py`, uploads `config/youtube_topic_hierarchy_v2.yaml` to DBFS, submits the notebook
task on existing cluster `0601-203643-bkxsqffg`, waits up to three hours, and fetches the notebook acceptance
payload. Override `RUN_DATE`, `WORKSPACE_DIR`, `CLUSTER_ID`, `TOP_N_CHANNELS`, `SNAPSHOT_DATE`, or source-table
environment variables when needed.

Run order:

1. `01_language_openlid_v3_databricks.py` with a bounded `limit_channels` and positive `videos_per_channel`
   for smoke or validation runs.
2. Inspect `yt_lid_v3_preflight_estimate` before committing a larger run to inference.
3. Run the full language workflow after smoke/validation checks pass.
4. Run `01b_language_lid_v3_subscriber_cohort_analysis_databricks.py` only for subscriber-cohort analyses.
5. Run `02_category_llm_youtube_databricks.py` in `labeled_validation` mode with `submit_batches=false`.
6. Submit provider batches or hand JSONL files to the API owner.
7. Import results and evaluate before any full-unlabeled category run.
8. Run `youtube_topic_treemap_v2.py` after the metrics, channel, language, and YouTube `topicCategories` tables
   are readable; inspect the printed reconciliation metrics and the `diagnostics.md` artifact before using the HTML.
