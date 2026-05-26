---
name: youtube-channel-evidence-classifier
description: Classify or validate YouTube channels from direct evidence. Use for residual language-validation CSV rows, independent third-opinion channel labeling, screenshot-to-CSV channel ID checks, channel category slugs, language/script labels such as `eng_Latn`, or requests to ignore existing model/subagent columns and ground judgments in public channel evidence.
---

# YouTube Channel Evidence Classifier

## Purpose

Use this skill when channel rows need manual evidence-backed labels for validation. The output should be compact, auditable, and conservative enough to serve as a check on automated language or category pipelines.

## Evidence Order

1. Use the row's `channel_id` as the anchor. Do not infer identity from nearby rows.
2. Visit direct channel URLs: `https://www.youtube.com/channel/{channel_id}`.
3. Check the public RSS feed: `https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}`.
4. Use screenshots only to confirm row/channel IDs or visible text; do not let screenshots override a different direct channel ID.
5. Ignore existing machine, Gemini, Codex, `subagent_*`, or prior judgment columns until after the independent label is written.

The helper `scripts/fetch_channel_evidence.py` can gather page/RSS snippets for one or more channel IDs.

## Labeling Rules

- Prefer the repository's existing label vocabulary when one exists in code, CSV headers, or validation reports.
- For language validation, use language-script labels like `eng_Latn`, `tha_Thai`, `hin_Deva`, or combined labels like `ara_Arab+fra_Latn` when evidence clearly shows multiple languages/scripts.
- Use `und` for language when the channel is unreachable or evidence is too thin.
- For category validation, use the existing category slug set if present. If no slug set exists, use concise lowercase slugs and note uncertainty.
- Mark unreachable channels explicitly, such as `unreachable_no_evidence`, rather than guessing from stale metadata.
- Keep confidence conservative: high only when title/about/video evidence strongly agrees; medium for partial evidence; low for ambiguous or sparse evidence.

## Evidence Standard

For each row, capture:

- row number,
- channel ID,
- label,
- confidence,
- one or two short evidence snippets from channel title, about text, RSS title, or recent video titles.

Avoid long quotes. Use brief snippets such as video title fragments, channel title, or "This channel does not exist." If evidence conflicts, report the conflict instead of forcing a label.

## CSV Updates

Only edit CSVs when the user asks for it.

Before editing:

1. Read headers and determine the intended output columns.
2. Preserve existing rows and column order unless the user requests a schema change.
3. Treat row numbers carefully: distinguish CSV physical row numbers from data IDs.
4. If adding missing rows from screenshots, verify the channel IDs against the visible screenshot text or direct source.

After editing, validate:

```bash
python3 - <<'PY'
import csv
from collections import Counter
path = 'youtube_descriptive/validation/lid_v3_residual_disagreement_sample.csv'
with open(path, newline='') as f:
    rows = list(csv.DictReader(f))
ids = [r.get('channel_id', '') for r in rows]
print('rows', len(rows))
print('missing_channel_id', sum(not x for x in ids))
print('duplicate_channel_id', [k for k, v in Counter(ids).items() if k and v > 1][:20])
PY
```

## Output

For manual classification, return a compact table with row, channel ID, label, confidence, and evidence. For CSV edits, summarize columns changed, row count, duplicate/missing checks, and any rows left as `und` or unreachable.
