# youtube_descriptive

Databricks notebooks and support files for YouTube descriptive-analysis workflows, including channel
language detection, category classification, and manuscript-analysis notebooks.

## Repository layout

* `src/`: Databricks source-format notebooks and workflow documentation.
* `validation/`: Validation reports, prompt material, and audit samples.
* `tests/`: Unit tests for shared Python support code.
* `fixtures/`: Test fixtures.
* `.codex_databricks/`: Databricks validation helper notebooks used for LID v3 smoke, analysis, and ablation
  runs.

Generated local state such as `.venv/`, `.databricks/`, `__pycache__/`, and `.env.local` should remain
untracked.

## Current language-detection docs

Use `src/README_language_lid_v3.md` as the source of truth for the current dual-model OpenLID-v3 + GlotLID
pipeline. The original single-model OpenLID first cut is preserved in git history and should not be used as
the current runbook.

## Current sampling and treemap docs

Use these versioned records for the full collected-frame analysis:

* `../docs/FULL_CORPUS_DUAL_SAMPLE_DESIGN_20260717.md`: frozen census plus one-million-SRS and target-one-million-PPS design.
* `../docs/FULL_CORPUS_DUAL_SAMPLE_IMPLEMENTATION_20260717.md`: executable Databricks stages, table contracts, QA gates, and run log.
* `../docs/TREEMAP_FULL_CORPUS_RUNBOOK.md`: lineage and rendering instructions for the 4.8-million-channel language corpus and `>=10K` census.
* `../docs/TREEMAP_WEIGHTING_BIAS_20260716.md`: equal-channel versus view-weighted distortion analysis.
* `../docs/BANDED_LT10K_FULL_CORPUS_SENSITIVITY_20260716.md`: limitations and results from the earlier 2,000-channel banded pilot.

Generated Delta exports and rendered artifacts remain untracked. Commit code,
configuration, and design/runbook documents only.


## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

If you're developing with an IDE, dependencies for this project should be installed using uv:

*  Make sure you have the UV package manager installed.
   It's an alternative to tools like pip: https://docs.astral.sh/uv/getting-started/installation/.
*  Run `uv sync --dev` to install the project's dependencies.


# Using this project using the CLI

The Databricks workspace and IDE extensions provide a graphical interface for working
with this project. It's also possible to interact with it directly using the CLI:

1. Read `src/AGENT_DATA_CONTEXT.md` before authentication or data access. This
   project requires the named `matt.hindman@researchaccelerator.org` profile;
   do not use a default or legacy profile. CLI commands take this form:
    ```bash
    env DATABRICKS_AUTH_STORAGE=plaintext databricks \
      -p matt.hindman@researchaccelerator.org ...
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

4. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

5. Finally, to run tests locally, use `pytest`:
   ```
   $ uv run pytest
   ```
