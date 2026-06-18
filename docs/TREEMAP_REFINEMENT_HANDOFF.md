# Treemap Refinement Handoff

Last updated: 2026-06-18  
Repository root: `/Users/hindman/Documents/GitHub/youtube-descriptive`

This document is for another model or agent refining the YouTube topic treemap
work. It includes the Databricks access pattern, relevant project files, data
assets, processing history, current artifacts, known caveats, and reproduction
commands.

## 1. Databricks Access Rules

Use only the Databricks profile and compute below unless the user explicitly
changes the instruction.

Correct Databricks auth/profile:

- Profile: `matt.hindman@researchaccelerator.org`
- Host: `https://adb-1335559103600339.19.azuredatabricks.net`
- Always run CLI commands like:

```bash
env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org ...
```

Do not use:

- `hindman.gmail.com@auth.researchaccelerator.org`
- any default profile
- any newly-created cluster unless explicitly asked

If auth is broken, refresh with:

```bash
env DATABRICKS_AUTH_STORAGE=plaintext databricks auth login \
  --profile matt.hindman@researchaccelerator.org \
  --host https://adb-1335559103600339.19.azuredatabricks.net \
  --timeout 10m
```

If the CLI prints an OAuth URL but does not open Safari correctly, open that
exact URL in Safari manually. The CLI may print
`/bin/bash: open -a Safari: command not found`; that is fine as long as the URL
is opened manually and the CLI later reports that the profile was saved.

Correct compute:

- Existing all-purpose cluster: `matt-research-gencompute`
- Cluster ID: `0601-203643-bkxsqffg`
- SQL Warehouse for lightweight SQL queries: `86100da4e1fe8713`

Namespace and secrets:

- Main catalog/schema for outputs: `dev_sean.matt`
- Source YouTube TOO tables: `prod_tads.youtube_too`
- LLM secret scope: `youtube-llm-keys`
- Secret keys, if other notebooks need them:
  - `openai-api-key`
  - `anthropic-api-key`
  - `gemini-api-key`
  - `deepseek-api-key`

The treemap rendering described here does not need LLM secrets.

## 2. Basic Project Information

Important local paths:

- Main spec: `docs/TREEMAP_SPEC.md`
- Source-copy spec: `youtube_descriptive/src/treemap_spec.md`
- Main Databricks treemap notebook/source: `youtube_descriptive/src/youtube_topic_treemap_v2.py`
- Editable hierarchy/seed map config: `config/youtube_topic_hierarchy_v2.yaml`
- Local v2b renderer: `scripts/render_treemap_v2b.py`
- Databricks traffic helper: `.codex_databricks/export_treemap_traffic_v2b_20260617.py`
- Databricks traffic job payload: `.codex_databricks/job_export_treemap_traffic_v2b_20260617.json`
- Original Databricks runner: `.codex_databricks/run_youtube_topic_treemap_v2_20260617.sh`

The worktree had many unrelated untracked/modified files when this handoff was
written. Do not assume everything in `git status` came from this treemap pass.
Do not revert unrelated changes.

## 3. Core Data Assets

### 3.1 Source Tables

Topic/category source:

- `dev_sean.default.channel_category`
- Provides raw `topic_categories` / `topicDetails.topicCategories[]` style
  arrays used to project YouTube topic labels.

TOO channel metadata/universe:

- `prod_tads.youtube_too.yt_sl_channels`
- Join key: `yt_sl_channels.channel_id`
- Use this table for TOO channel universe and channel metadata.

Weekly traffic snapshots:

- `dev_sean.default.yt_channel_stats`
- Join key: `yt_channel_stats.canonical_id`
- Important fields:
  - `canonical_id` = YouTube channel id
  - `channel_name`
  - `subscriber_count`
  - `total_view_count` = lifetime views at the snapshot
  - `collected_at` = snapshot timestamp
- Use latest available snapshot and the snapshot 4 weeks earlier.
- Current observed latest: `2026-06-15`
- Exact 4-week prior used here: `2026-05-18`
- Recent traffic measure:

```text
view_count_4wk = current.total_view_count - prior.total_view_count
```

Negative deltas are invalid/null, not real negative traffic.

Language table used by the original pipeline:

- `dev_sean.matt.yt_lid_v3_channels`
- The original Databricks notebook uses this to attach language codes.

Old comparison table:

- `dev_sean.matt.yt_channel_topic_flat_primary_draft_20260615`
- Do not use for the main treemap; it is for sensitivity/diagnostics only.

### 3.2 Delta Tables Written by the Original v2 Notebook

The original run date was `20260617`. The notebook writes tables in
`dev_sean.matt` with run-date suffixes. Relevant names from the code/spec:

- `dev_sean.matt.yt_channel_topic_projection_v2_20260617`
- `dev_sean.matt.yt_treemap_allocations_v2_20260617`
- `dev_sean.matt.yt_treemap_plot_rows_language_first_v2_20260617`
- `dev_sean.matt.yt_treemap_plot_rows_topic_first_v2_20260617`
- `dev_sean.matt.yt_treemap_diagnostics_v2_20260617`

The local v2b renderer currently reuses the projection/allocation Parquet
exports from this run rather than rerunning the whole Spark pipeline.

### 3.3 DBFS Artifact Directories

Original v2 artifact directory:

```text
dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617
```

Traffic extract directory for v2b:

```text
dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617_v2b/traffic_4wk.parquet
```

Local v2 artifact directory:

```text
outputs/youtube_topic_treemap_20260617
```

Local v2b artifact directory:

```text
outputs/youtube_topic_treemap_20260617_v2b
```

## 4. Local Data Files and Row Counts

Local v2b data files:

- `outputs/youtube_topic_treemap_20260617_v2b/channel_topic_projection.parquet`
  - 200,000 rows
  - 200,000 unique channels
  - 25 columns
  - Key columns include `channel_id`, `channel_title`, `language_code`,
    `latest_views`, `subscriber_count`, `snapshot_date`,
    `raw_topic_categories`, `normalized_slugs`, `canonical_slugs`,
    `mapped_nodes`, `display_families`, `display_leaves`, and diagnostic flags.
- `outputs/youtube_topic_treemap_20260617_v2b/channel_label_allocations.parquet`
  - 1,685,836 rows
  - 200,000 unique channels
  - Allocation-method row counts:
    - `dominant_display`: 200,000
    - `equal_leaf`: 371,459
    - `equal_raw_label_after_parent_prune`: 371,459
    - `family_balanced`: 371,459
    - `specificity_weighted`: 371,459
  - The v2b renderer uses `family_balanced`.
- `outputs/youtube_topic_treemap_20260617_v2b/traffic_4wk.parquet`
  - Raw rows: 108,067
  - Raw unique channels: 107,869
  - Contains duplicate channel rows from the TOO channel table join; the
    renderer dedupes by channel id, keeping the latest current/prior timestamps.
  - Raw valid non-null `view_count_4wk`: 104,611 rows before dedupe.
  - Raw negative deltas: 3,277 rows before dedupe.
  - Current snapshot date in raw extract: `2026-06-15`
  - Prior snapshot date in raw extract: `2026-05-18`

Renderer after traffic dedupe:

- Traffic extract rows: 108,067
- Traffic unique channels: 107,869
- Duplicate rows deduped: 198
- Channels with current snapshot: 107,777
- Channels with prior snapshot: 107,690
- Negative raw deltas: 3,266
- Valid 4-week traffic channels: 104,424
- Positive traffic channels plotted: 103,046
- Positive allocation rows plotted: 193,417
- Total 4-week views used after dedupe: 2,229,342,931,147

## 5. What Has Been Done So Far

### 5.1 Spec Placement and Updates

The treemap spec exists in two locations:

- `docs/TREEMAP_SPEC.md`
- `youtube_descriptive/src/treemap_spec.md`

Both were updated to include:

- The visualization patch requiring:
  - static master = language -> family only
  - top 12 languages
  - low-share family pooling
  - max 120 static rendered cells
  - min static cell area >= 0.3% of total
  - squarify packing
  - PNG and SVG static export
  - self-contained interactive HTML
  - top 15 channels plus `Other (N channels)` per leaf in the interactive view
- The weekly traffic source rule using `dev_sean.default.yt_channel_stats`
  instead of raw lifetime views as treemap area.
- The revised allocation formula:

```text
allocated_views = view_count_4wk * allocation_weight
```

The original Parquet allocation file still contains the old lifetime-derived
`allocated_views` from the original v2 notebook. The v2b local renderer does
not overwrite that raw column; it creates `allocated_views_4wk` in memory from
`view_count_4wk`.

### 5.2 Original v2 Databricks Run

The original hierarchy-aware treemap notebook:

```text
youtube_descriptive/src/youtube_topic_treemap_v2.py
```

was run through:

```text
.codex_databricks/run_youtube_topic_treemap_v2_20260617.sh
```

Known run information from the transcript:

- Submit run: `963598563804515`
- Task run: `546721819338670`
- Existing cluster: `0601-203643-bkxsqffg`
- Result: success
- Reported acceptance values included:
  - `RECONCILIATION: PASS`
  - `channels_processed = 200000`
  - `channel_allocation_rows = 1685836`
  - `plot_rows = 13088`
  - snapshot `2026-06-15`
  - total lifetime views about `134,199,668,079,123`
  - view mass coverage about `0.9911162660788841`

The original rendered PNG/HTML in `outputs/youtube_topic_treemap_20260617`
was not usable as a static figure. It rendered too many levels at once and
looked like a sliver storm.

### 5.3 Traffic Extraction for v2b

A small Databricks helper was added:

```text
.codex_databricks/export_treemap_traffic_v2b_20260617.py
```

It:

1. Reads the projection-channel universe from
   `dev_sean.matt.yt_channel_topic_projection_v2_20260617`.
2. Uses `prod_tads.youtube_too.yt_sl_channels` for channel universe/metadata.
3. Uses `dev_sean.default.yt_channel_stats` only for subscriber/current
   lifetime-view snapshots and the 4-week delta.
4. Resolves current and prior dates from `collected_at`:
   - current = max snapshot date
   - prior = max snapshot date <= current - 28 days
5. Dedupes stats rows by `(canonical_id, DATE(collected_at))`, keeping latest
   `collected_at`.
6. Computes:
   - `raw_4wk_views`
   - `view_count_4wk`
   - `avg_weekly_view_count`
7. Writes Parquet to:

```text
dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617_v2b/traffic_4wk.parquet
```

The successful traffic rerun:

- Submit run: `10866132594722`
- Task run: `384795401848230`
- Existing cluster: `0601-203643-bkxsqffg`
- Result: success
- Run page:
  `https://adb-1335559103600339.19.azuredatabricks.net/?o=1335559103600339#job/818765705261342/run/10866132594722`

There was also an earlier successful run that wrote to a `/dbfs/...` path that
the CLI did not see as a DBFS directory. That run was not used for local copy:

- Submit run: `199351191223507`
- Task run: `1056975900808353`

The corrected job payload is:

```text
.codex_databricks/job_export_treemap_traffic_v2b_20260617.json
```

### 5.4 Local v2b Renderer

The local renderer is:

```text
scripts/render_treemap_v2b.py
```

It expects these local inputs:

```text
outputs/youtube_topic_treemap_20260617_v2b/channel_label_allocations.parquet
outputs/youtube_topic_treemap_20260617_v2b/channel_topic_projection.parquet
outputs/youtube_topic_treemap_20260617_v2b/traffic_4wk.parquet
```

Main processing:

1. Reads allocation rows.
2. Filters to `allocation_method = family_balanced`.
3. Reads traffic extract.
4. Dedupes traffic by `channel_id`.
5. Joins traffic to allocations.
6. Computes `allocated_views_4wk = view_count_4wk * allocation_weight`.
7. Checks conservation on channels with valid non-null `view_count_4wk`:
   - per-channel weight sum = 1
   - per-channel allocated sum = channel `view_count_4wk`
   - total allocated = total `view_count_4wk`
8. Builds the static master:
   - levels: language -> family only
   - top 12 languages by allocated 4-week views
   - all other languages pooled under `Other languages`
   - families below threshold pooled into `Other (families)`
   - min rendered cell area >= 0.3%
   - squarify layout via Python `squarify` + matplotlib
9. Builds the interactive HTML:
   - Plotly `go.Treemap`
   - `branchvalues="total"`
   - `maxdepth=2`
   - `tiling.packing="squarify"`
   - `tiling.pad=2`
   - `sort=True`
   - full hierarchy: language -> family -> leaf -> channel
   - within each leaf: top 15 channels plus one `Other (N channels)` cell
   - hover includes channel name, allocated 4-week views, raw channel 4-week
     views, allocation weight, and raw topic slugs.

Renderer output printed:

```text
ALLOCATION METHOD: family_balanced
ROWS USED: 371,459
TRAFFIC SOURCE TABLE: dev_sean.default.yt_channel_stats
TOO UNIVERSE TABLE: prod_tads.youtube_too.yt_sl_channels
TRAFFIC CURRENT SNAPSHOT: 2026-06-15
TRAFFIC PRIOR SNAPSHOT: 2026-05-18
TRAFFIC EXTRACT ROWS: 108,067
TRAFFIC UNIQUE CHANNELS: 107,869
TRAFFIC DUPLICATE ROWS DEDUPED: 198
TRAFFIC CHANNELS WITH CURRENT: 107,777
TRAFFIC CHANNELS WITH PRIOR: 107,690
TRAFFIC NEGATIVE RAW DELTAS: 3,266
TRAFFIC CHANNELS WITH VALID 4WK: 104,424
VALID TRAFFIC CHANNELS IN ALLOCATIONS: 104,424
POSITIVE TRAFFIC CHANNELS PLOTTED: 103,046
POSITIVE ALLOCATION ROWS PLOTTED: 193,417
TOTAL 4WK VIEWS: 2,229,342,931,147
PACKING: squarify
STATIC METHOD: squarify library + matplotlib
INTERACTIVE METHOD: plotly.graph_objects.go.Treemap branchvalues=total maxdepth=2 tiling.packing=squarify
CONSERVATION: PASS
CONSERVATION TOTAL 4WK VIEWS: 2,229,342,931,147
STATIC CELLS: 73
MIN CELL AREA: 0.303%
POOLED VIEW SHARE: 26.157%
LABELED CELLS: 31
STATIC PRUNING: top_k_languages=12; family_pool_threshold=0.010
FIGURE DIMENSIONS: 4000x2400 px, 20x12 in, 200 DPI
INTERACTIVE CHANNEL LEAF CAP: top 15 + Other (N channels)
INTERACTIVE CHANNEL/OTHER NODES: 27,186
```

The static PNG was opened and visually inspected. Legibility verdict from the
transcript:

```text
LEGIBILITY: PASS - I opened the rendered static PNG. The language blocks are individually readable, the labeled family tiles are readable, and I do not see any region rendered as a stack of thin horizontal slivers; the remaining small unlabeled cells are pooled/legend-backed detail rather than a sliver storm.
```

## 6. Current Output Artifacts

Static master PNG:

```text
outputs/youtube_topic_treemap_20260617_v2b/treemap_static_master_v2b.png
```

- Size on disk: about 311 KB
- Dimensions: 4000 x 2400 px
- Rendered at 20 x 12 inches, 200 DPI

Static master SVG:

```text
outputs/youtube_topic_treemap_20260617_v2b/treemap_static_master_v2b.svg
```

- Size on disk: about 123 KB

Interactive explorer HTML:

```text
outputs/youtube_topic_treemap_20260617_v2b/treemap_interactive_explorer_v2b.html
```

- Size on disk: about 13 MB
- Self-contained: yes
- External `<script src=...>` tags: 0
- Contains required Plotly treemap settings:
  - `"branchvalues":"total"`
  - `"maxdepth":2`
  - `"packing":"squarify"`
  - `"pad":2`
  - `"sort":true`

Prior artifacts were kept:

```text
outputs/youtube_topic_treemap_20260617/
```

That directory contains the earlier all-level language-first HTML and PNG,
which should not be used as the static paper figure.

## 7. Reproduction Commands

Run from:

```bash
cd /Users/hindman/Documents/GitHub/youtube-descriptive
```

Install local Python dependencies if needed:

```bash
python3 -m pip install --user squarify plotly
```

If the original v2 Databricks artifacts need to be regenerated, use the existing
runner. It imports `youtube_descriptive/src/youtube_topic_treemap_v2.py`, uploads
`config/youtube_topic_hierarchy_v2.yaml`, submits to the existing cluster, and
writes the original DBFS artifacts.

```bash
env DATABRICKS_AUTH_STORAGE=plaintext \
  DATABRICKS_PROFILE=matt.hindman@researchaccelerator.org \
  CLUSTER_ID=0601-203643-bkxsqffg \
  bash .codex_databricks/run_youtube_topic_treemap_v2_20260617.sh
```

If only local copies of the original projection/allocation Parquet are missing,
copy them from DBFS:

```bash
mkdir -p outputs/youtube_topic_treemap_20260617_v2b

env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org \
  fs cp -r \
  dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617/channel_topic_projection.parquet \
  outputs/youtube_topic_treemap_20260617_v2b/channel_topic_projection.parquet

env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org \
  fs cp -r \
  dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617/channel_label_allocations.parquet \
  outputs/youtube_topic_treemap_20260617_v2b/channel_label_allocations.parquet
```

To rerun the v2b traffic extract:

```bash
env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org \
  workspace import \
  /Users/matt.hindman@researchaccelerator.org/lid_v3_too_20260609/export_treemap_traffic_v2b_20260617 \
  --file .codex_databricks/export_treemap_traffic_v2b_20260617.py \
  --format SOURCE \
  --language PYTHON \
  --overwrite

env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org \
  jobs submit \
  --json @.codex_databricks/job_export_treemap_traffic_v2b_20260617.json \
  --timeout 20m \
  --output json

env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org \
  fs cp -r \
  dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617_v2b/traffic_4wk.parquet \
  outputs/youtube_topic_treemap_20260617_v2b/traffic_4wk.parquet
```

Then rerender local artifacts:

```bash
python3 scripts/render_treemap_v2b.py
```

The renderer should print `CONSERVATION: PASS`, `PACKING: squarify`, static
cell metrics, static PNG/SVG paths, and the interactive HTML path.

## 8. Logs and Run Records

No durable local log file was written during this pass. The useful records are:

- Databricks traffic job JSON:
  `.codex_databricks/job_export_treemap_traffic_v2b_20260617.json`
- Databricks traffic notebook/source:
  `.codex_databricks/export_treemap_traffic_v2b_20260617.py`
- Databricks traffic rerun:
  - submit run `10866132594722`
  - task run `384795401848230`
  - run page:
    `https://adb-1335559103600339.19.azuredatabricks.net/?o=1335559103600339#job/818765705261342/run/10866132594722`
- To retrieve the traffic run output:

```bash
env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org \
  jobs get-run-output 384795401848230 --output json
```

The `get-run-output` response had empty `notebook_output` in this CLI session,
but the run state was `TERMINATED` / `SUCCESS`, and the DBFS Parquet directory
was visible and copied locally.

Original v2 treemap run records from the transcript:

- submit run `963598563804515`
- task run `546721819338670`

## 9. Important Caveats

1. The current v2b renderer is a local post-processing refinement. The full
   Spark notebook still produces lifetime-view allocation columns and old-style
   HTML. A cleaner next step is to integrate the 4-week traffic measure and the
   v2b visualization outputs into `youtube_descriptive/src/youtube_topic_treemap_v2.py`.
2. The local allocation file retains the original lifetime-derived
   `allocated_views` column. The renderer creates `allocated_views_4wk` in
   memory and uses that for all v2b plots.
3. The traffic extract contains duplicate `channel_id` rows because
   `yt_sl_channels` can duplicate channel IDs. The renderer dedupes by
   `channel_id`, keeping latest `current_collected_at`/`prior_collected_at`.
   A future Databricks helper should dedupe `yt_sl_channels` before writing
   the traffic extract.
4. Channels missing current/prior snapshots or having negative deltas are
   retained in diagnostics but do not contribute positive treemap area.
5. Some language codes are duplicates or variants (`en`, `eng`, `en-US`,
   `en-IN`, etc.). The current renderer keeps raw language codes separate,
   consistent with the "raw label/view columns unchanged" constraint. Any
   language-code consolidation should be explicit and documented.
6. The static figure labels language blocks and only large family tiles. Smaller
   family tiles are intentionally unlabeled and rely on the legend and
   interactive HTML.
7. The interactive HTML is large because it embeds Plotly JS and data for a
   self-contained file.

## 10. Recommended Refinement Work

High priority:

1. Move the 4-week traffic join into the main Databricks notebook so the Delta
   outputs and artifacts are generated in one reproducible pipeline.
2. Deduplicate `prod_tads.youtube_too.yt_sl_channels` by `channel_id` in the
   Databricks traffic helper/query, then compare row counts with the current
   local renderer dedupe.
3. Save local renderer output to a run log, e.g.
   `outputs/youtube_topic_treemap_20260617_v2b/render_log.txt`, so future
   validation does not depend on chat transcript text.
4. Add a small README in `outputs/youtube_topic_treemap_20260617_v2b/` with
   artifact provenance and metrics.

Medium priority:

1. Improve language labels using a controlled mapping file, while preserving raw
   `language_code` in data.
2. Consider a color-blind-safe family palette with similar contrast but less
   reliance on pale gray for pooled/unlabeled cells.
3. Add optional family-level small multiples:
   family -> leaf -> top channels, capped at about 60 cells each.
4. Add automated image checks for static PNG dimensions and a coarse sliver
   metric, but keep manual visual inspection as the final gate.

Lower priority:

1. Add notebook tests or local smoke tests for conservation and HTML settings.
2. Export a companion CSV with the final static cells and labels for paper
   caption/provenance.
3. Decide whether `Other languages` should be a parent block with family
   children, as currently implemented, or a single opaque pooled cell.

## 11. Acceptance Checklist for Future Refinements

Before declaring the treemap refined, require:

- `CONSERVATION: PASS`
- Static PNG and SVG written
- Static PNG at least 2000 x 1200 px and at least 200 DPI
- Static hierarchy only language -> family
- Static cells <= 120
- Minimum static cell area >= 0.3%
- Pooled view share printed
- Labeled cell count printed
- Squarify packing printed and verified
- Static PNG opened and visually inspected
- Legibility verdict printed
- Interactive HTML written
- Interactive HTML self-contained, no external `<script src=...>` tags
- Interactive trace has:
  - `branchvalues="total"`
  - `maxdepth=2`
  - `tiling.packing="squarify"`
  - `tiling.pad=2`
  - `sort=True`
- Channels in interactive leaves pooled to top 15 + `Other (N channels)`

## 12. Quick Orientation for Another Model

If you only have time to inspect three files, inspect these:

1. `docs/TREEMAP_SPEC.md`
2. `scripts/render_treemap_v2b.py`
3. `.codex_databricks/export_treemap_traffic_v2b_20260617.py`

If you only have time to rerun one command after dependencies are installed:

```bash
python3 scripts/render_treemap_v2b.py
```

If rerender succeeds and prints the expected metrics, open:

```text
outputs/youtube_topic_treemap_20260617_v2b/treemap_static_master_v2b.png
```

and inspect legibility before making any further claim about the figure.
