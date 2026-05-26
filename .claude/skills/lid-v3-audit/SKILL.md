---
name: lid-v3-audit
description: Audit and repair the YouTube Descriptive language-detection v3 workflow. Use when reviewing or fixing changes to `01_language_openlid_v3_databricks.py`, `01b_language_lid_v3_subscriber_cohort_analysis_databricks.py`, `README_language_lid_v3.md`, `CODEX_REVIEW_lang_detect_v3.md`, residual-validation CSVs, or branches such as `lang-detect-v3-dual-model`.
---

# LID v3 Audit

## Purpose

Use this skill for high-risk review and repair of the YouTube language-detection v3 Databricks pipeline. The goal is to catch correctness, scalability, idempotency, and validation defects before the notebook is trusted on the full corpus.

## Start Here

1. Check branch and dirty state with `git status --short --branch`.
2. Read the review/spec files first:
   - `youtube_descriptive/src/CODEX_REVIEW_lang_detect_v3.md`
   - `youtube_descriptive/src/README_language_lid_v3.md`
   - `youtube_descriptive/src/README_language_openlid_v3.md` if present
3. Read the notebook exports:
   - `youtube_descriptive/src/01_language_openlid_v3_databricks.py`
   - `youtube_descriptive/src/01b_language_lid_v3_subscriber_cohort_analysis_databricks.py`
4. Read validation artifacts when the task touches residual disagreements:
   - `youtube_descriptive/validation/lid_v3_residual_disagreement_sample.csv`
   - `youtube_descriptive/validation/REPORT_lid_v3_top_cohort_validation.md`

## Audit Checklist

Prioritize hard behavioral issues over style.

- **Correctness:** schema assumptions, table names, checkpoint paths, output overwrite/append semantics, stale partial outputs, joins that can duplicate rows, language-code normalization, missing null handling, and mismatches between README/spec and code.
- **Scalability:** driver-side `collect()`, `toPandas()`, unbounded lists, per-row API/model calls, unpartitioned writes, avoidable full-table scans, missing repartitioning, and accidental cache/persist leaks.
- **Idempotency:** reruns must not silently mix old and new outputs. Confirm cleanup, overwrite mode, temp paths, and validation gates.
- **Databricks behavior:** widgets, `dbutils`, secrets, cluster assumptions, model artifact paths, MLflow or volume paths, and Python-export syntax.
- **Validation:** residual-disagreement samples, language/script labels, row counts, duplicate channel IDs, confidence/evidence columns, and whether a claimed gold label is actually present.

## Repair Rules

- Confirm every reported issue in the current code before editing, especially issues from another model.
- Prefer statistically and operationally correct fixes over matching older notebook patterns.
- Keep changes local to the LID v3 files unless the evidence requires docs or validation artifact updates.
- Preserve generated notebook-export readability. Do not introduce notebook-only magic that breaks `py_compile`.
- When updating validation CSVs, preserve existing columns unless the user explicitly asks to rename or replace them.

## Verification

Run the strongest cheap checks available:

```bash
python3 -m py_compile youtube_descriptive/src/01_language_openlid_v3_databricks.py
python3 -m py_compile youtube_descriptive/src/01b_language_lid_v3_subscriber_cohort_analysis_databricks.py
git diff --check
```

If the change touches CSV validation artifacts, also run a row-count and duplicate-key check with the Python standard library or the repo's existing validation code.

## Output

Lead with confirmed findings or fixes. Include:

- files changed,
- checks run and results,
- any remaining unverified risks,
- any exact questions needing human judgment, such as taxonomy choices or whether a validation column is authoritative.
