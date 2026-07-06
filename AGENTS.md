# youtube-descriptive — Agent Instructions (ChatGPT/Codex, Claude, and other agents)

Databricks-based YouTube descriptive-analysis workflows: channel language
detection (LID v3), category classification, and manuscript-analysis
notebooks. Part of the YouTube Census research program.

## Read first, by task

- **Any Databricks/data work:** `youtube_descriptive/src/AGENT_DATA_CONTEXT.md`
  — the agent-facing index (CLI profile + auth, cluster policy, query-safety
  contract). Machine-readable manifest:
  `youtube_descriptive/src/databricks_youtube_resources.agent.json`; long-form
  map: `youtube_descriptive/src/README_data_resources.md`.
- **Language-detection work:** `youtube_descriptive/src/README_language_lid_v3.md`
  is the source of truth for the current dual-model OpenLID-v3 + GlotLID
  pipeline. The single-model first cut in git history is NOT the current
  runbook.
- **Repo layout:** `youtube_descriptive/README.md`.

## Skills

Skills live in `.agents/skills/` (canonical for this repo) and are mirrored
to `.claude/skills/` so Claude Code loads them — keep the copies identical
when editing:

- `lid-v3-audit` — high-risk review/repair of the LID v3 Databricks pipeline.
- `youtube-channel-evidence-classifier` — independent, evidence-backed
  channel labeling for validation rows.

## Hard rules

- Follow the data-context Safety Contract: metadata first; no `count(*)`,
  broad `select *`, `distinct`, or unbounded scans just to understand
  structure; narrow filters and `LIMIT`.
- Use the registered CLI profile/cluster exactly as specified in
  AGENT_DATA_CONTEXT.md; do not create or restart other compute unless
  explicitly asked. The SQL warehouse can be started but not stopped from
  this side — treat starts as owner-visible actions.
- Publication-critical derived tables currently live in personal `dev_sean`
  schemas, not governed prod — verify semantics against code/docs before
  making publication-critical claims, and do not move/rename tables without
  the owner.
- Validation labels must be independent: when hand-labeling, ignore existing
  model/subagent/judgment columns until after your label is written (see the
  classifier skill).
- The working tree often carries other sessions' in-flight work (e.g.
  `.codex_databricks/` job scripts) — commit only files from your own task.
