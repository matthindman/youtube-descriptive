# YouTube channel language and category classification on Databricks

This folder contains Databricks source-format notebooks and READMEs for the first-cut language and category classification pipeline.

## Deliverables

1. `01_language_openlid_v3_databricks.py`
   - Segment-level language classification with OpenLID-v3.
   - Deterministic video selection.
   - Channel-level aggregation using top-1 and top-2 segment predictions.
   - GlotLID scaffold code remains commented out for follow-up audits.

2. `README_language_openlid_v3.md`
   - Databricks setup, model upload/download, widget settings, output schema, validation notes, and source-table update cautions.

3. `02_category_llm_youtube_databricks.py`
   - LLM bake-off for YouTube-style category classification.
   - Language-stratified validation sampling.
   - OpenAI, Anthropic, and Gemini batch JSONL generation.
   - Optional batch submission through Databricks Secrets.
   - Result parsing, reference-label evaluation, macro-F1, language-stratified metrics, and pairwise model agreement.

4. `README_category_llm.md`
   - API-key setup, reference-label options, model configuration, batch-file workflow, evaluation procedure, and full-corpus guidance.

## Importing notebooks into Databricks

Workspace UI: import each `.py` file as a Databricks notebook.

Databricks CLI:

```bash
databricks workspace import ./01_language_openlid_v3_databricks.py /Users/<you>/youtube/01_language_openlid_v3_databricks.py --format SOURCE --language PYTHON --overwrite
databricks workspace import ./02_category_llm_youtube_databricks.py /Users/<you>/youtube/02_category_llm_youtube_databricks.py --format SOURCE --language PYTHON --overwrite
```

Run order:

1. `01_language_openlid_v3_databricks.py` with `limit_channels=10000` for a smoke test.
2. Full language run after the smoke test succeeds.
3. `02_category_llm_youtube_databricks.py` in `labeled_validation` mode with `submit_batches=false`.
4. Submit provider batches or hand JSONL files to the API owner.
5. Import results and evaluate before any full-unlabeled run.
