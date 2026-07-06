# youtube-descriptive — Claude Instructions

Databricks-based YouTube descriptive analyses (LID v3 language detection,
category classification, manuscript notebooks) within the YouTube Census
program.

Read `AGENTS.md` (repo root) for the full orientation — read-first documents
by task, skill index, and hard rules. Key ones:

- Data/Databricks work starts at `youtube_descriptive/src/AGENT_DATA_CONTEXT.md`
  (CLI profile, cluster policy, query-safety contract: metadata first, no
  unbounded scans).
- LID work: `youtube_descriptive/src/README_language_lid_v3.md` is the
  current runbook (dual-model v3; the single-model first cut is history).
- Skills: `.claude/skills/{lid-v3-audit, youtube-channel-evidence-classifier}`
  (mirrored from `.agents/skills/` — keep copies identical when editing).
- Commit only your own task's files; the tree often has other sessions'
  in-flight work.
