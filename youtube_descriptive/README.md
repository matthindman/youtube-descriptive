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

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
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
