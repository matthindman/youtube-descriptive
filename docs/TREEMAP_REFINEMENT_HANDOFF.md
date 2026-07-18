# Treemap Refinement Handoff

**Last updated:** 2026-06-18
**Repository root:** `/Users/hindman/Documents/GitHub/youtube-descriptive`

This document is for another model or agent (currently Claude Code) refining the
YouTube topic treemap. It includes the Databricks access pattern, relevant
project files, data assets, processing history, current artifacts, known
caveats, and reproduction commands.

> **Current target: the v3 refinement.** The v2/v2b passes produced a
> conservation-correct, legible language → family static figure, but with four
> defects: (1) the English language is balkanized into `en`, `eng`, `en-IN`,
> `en-US`, `en-GB`; (2) some "languages" are LID review-cluster artifacts, not
> real languages; (3) the color palette is muddy and mis-assigned (gray is used
> for Society, a real family); and (4) it stops at language → family with no
> child topics and no named channels. v3 fixes all four. **The authoritative,
> detailed spec for v3 is `docs/TREEMAP_V3_SPEC.md`.** This handoff supersedes
> the v2/v2b acceptance checklist and the "static = language → family only"
> design described in the history below. Refine the existing renderer; do not
> rebuild the Spark pipeline.

---

## 1. Databricks Access Rules

Use only the Databricks profile and compute below unless the user explicitly
changes the instruction.

**Correct Databricks auth/profile:**

- Profile: `matt.hindman@researchaccelerator.org`
- Host: `https://adb-1335559103600339.19.azuredatabricks.net`

Always run CLI commands like:

```
env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org ...
```

**Do not use:**

- `hindman.gmail.com@auth.researchaccelerator.org`
- any default profile
- any newly-created cluster unless explicitly asked

If auth is broken, refresh with:

```
env DATABRICKS_AUTH_STORAGE=plaintext databricks auth login \
  --profile matt.hindman@researchaccelerator.org \
  --host https://adb-1335559103600339.19.azuredatabricks.net \
  --timeout 10m
```

If the CLI prints an OAuth URL but does not open Safari correctly, open that
exact URL in Safari manually. The CLI may print
`/bin/bash: open -a Safari: command not found`; that is fine as long as the URL
is opened manually and the CLI later reports that the profile was saved.

**Correct compute:**

- Existing all-purpose cluster: `matt-research-gencompute`
- Cluster ID: `0601-203643-bkxsqffg`
- SQL Warehouse for lightweight SQL queries: `86100da4e1fe8713`

**Namespace and secrets:**

- Main catalog/schema for outputs: `dev_sean.matt`
- Source YouTube TOO tables: `prod_tads.youtube_too`
- LLM secret scope: `youtube-llm-keys`
- Secret keys, if other notebooks need them: `openai-api-key`,
  `anthropic-api-key`, `gemini-api-key`, `deepseek-api-key`

The treemap rendering described here does **not** need LLM secrets. The
top-channel placement decisions were made by an LLM in a prior pass and are
delivered as a static CSV (see Section 3.4); rendering only reads that CSV.

---

## 2. Basic Project Information

**Important local paths:**

- Authoritative v3 spec: `docs/TREEMAP_V3_SPEC.md` *(new — read this first)*
- Prior spec: `docs/TREEMAP_SPEC.md`
- Source-copy spec: `youtube_descriptive/src/treemap_spec.md`
- Main Databricks treemap notebook/source: `youtube_descriptive/src/youtube_topic_treemap_v2.py`
- Editable hierarchy/seed map config: `config/youtube_topic_hierarchy_v2.yaml`
- Local v2b renderer: `scripts/render_treemap_v2b.py`
- **v3 renderer (extend v2b into this):** `scripts/render_treemap_v3.py` *(new)*
- **Language normalization config:** `config/language_normalization.yaml` *(new)*
- **Top-channel placement input:** `config/treemap_top_channel_placement.csv` *(new — see 3.4)*
- Databricks traffic helper: `.codex_databricks/export_treemap_traffic_v2b_20260617.py`
- Databricks traffic job payload: `.codex_databricks/job_export_treemap_traffic_v2b_20260617.json`
- Original Databricks runner: `.codex_databricks/run_youtube_topic_treemap_v2_20260617.sh`

The worktree had many unrelated untracked/modified files when this handoff was
written. Do not assume everything in `git status` came from this treemap pass.
Do not revert unrelated changes.

---

## 3. Core Data Assets

### 3.1 Source Tables

**Topic/category source:**
`dev_sean.default.channel_category`
Provides raw `topic_categories` / `topicDetails.topicCategories[]` style arrays
used to project YouTube topic labels.

**TOO channel metadata/universe:**
`prod_tads.youtube_too.yt_sl_channels`
Join key: `yt_sl_channels.channel_id`. Use this table for the TOO channel
universe and channel metadata. (Note: this table can duplicate channel IDs; the
renderer dedupes by channel id.)

**Weekly traffic snapshots:**
`dev_sean.default.yt_channel_stats`
Join key: `yt_channel_stats.canonical_id`. Important fields: `canonical_id`
(= YouTube channel id), `channel_name`, `subscriber_count`, `total_view_count`
(= lifetime views at the snapshot), `collected_at` (snapshot timestamp).

- Use latest available snapshot and the snapshot 4 weeks earlier.
- Current observed latest: `2026-06-15`
- Exact 4-week prior used here: `2026-05-18`
- Recent traffic measure: `view_count_4wk = current.total_view_count - prior.total_view_count`
- Negative deltas are invalid/null, not real negative traffic.

**Language table used by the original pipeline:**
`dev_sean.matt.yt_lid_v3_channels`
The original Databricks notebook uses this to attach language codes. **v3
applies a normalization map on top of these raw codes (see Section 5.5.1);
the raw `language_code` is preserved and a new `language_display` column is added.**

**Old comparison table:**
`dev_sean.matt.yt_channel_topic_flat_primary_draft_20260615`
Do not use for the main treemap; it is for sensitivity/diagnostics only.

### 3.2 Delta Tables Written by the Original v2 Notebook

The original run date was `20260617`. The notebook writes tables in
`dev_sean.matt` with run-date suffixes:

- `dev_sean.matt.yt_channel_topic_projection_v2_20260617`
- `dev_sean.matt.yt_treemap_allocations_v2_20260617`
- `dev_sean.matt.yt_treemap_plot_rows_language_first_v2_20260617`
- `dev_sean.matt.yt_treemap_plot_rows_topic_first_v2_20260617`
- `dev_sean.matt.yt_treemap_diagnostics_v2_20260617`

The local renderer reuses the projection/allocation Parquet exports from this
run rather than rerunning the whole Spark pipeline.

### 3.3 DBFS Artifact Directories

- Original v2 artifact directory: `dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617`
- Traffic extract directory for v2b: `dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617_v2b/traffic_4wk.parquet`
- Local v2 artifact directory: `outputs/youtube_topic_treemap_20260617`
- Local v2b artifact directory: `outputs/youtube_topic_treemap_20260617_v2b`
- **Local v3 artifact directory (write here):** `outputs/youtube_topic_treemap_20260617_v3` *(new)*

### 3.4 Top-Channel Placement Input *(new in v3)*

`config/treemap_top_channel_placement.csv` — the top ~100 channels by 4-week
views, each with a single LLM-curated primary placement. v3 uses this to place
named channels at their primary topic leaf (some channels carry multiple topic
tags; the LLM chose the primary one).

Columns (used columns in **bold**):

- `rank`, **`channel_id`**, **`channel_name_title`**, `language_code`,
  **`view_count_4wk`**, `pct_total_4wk_views`
- `original_canonical_slugs`, `original_display_families`, `original_display_leaves`
- `revised_primary_canonical_slug`, **`revised_primary_family`**,
  **`revised_primary_leaf`**, **`revised_primary_path`** (e.g.
  `Entertainment > [Entertainment] - unspecified`, `Music > Music of Asia`,
  `Sports > Football`, `Other / Unmapped YouTube topic > Unmapped: pet`)
- `primary_tag_level` (`family` / `child` / `unmapped_specific` / `none`)
- `decision_type` (`parent_only_fallback`, `child_tag_preferred`,
  `family_preferred_over_child`, `already_single_family`,
  `unmapped_specific_tag_preferred`, `no_topic_categories`)
- `non_primary_canonical_slugs`, **`non_primary_display_paths_to_retain_as_metadata`**
  (show in hover), `suppressed_child_paths_because_family_is_better`
- **`needs_manual_review`** (`True`/`False`; 36 of 100 are `True`),
  `manual_review_priority`, `decision_note`

The `revised_primary_path` leaf names match the renderer's leaf naming
convention exactly, so they join directly to `(revised_primary_family,
revised_primary_leaf)`.

**Open decision (default below, pending user confirmation):** for the 36
channels with `needs_manual_review = True`, v3 **places them at their
`revised_primary_path` anyway** but surfaces `needs_manual_review` in hover and
lists them in diagnostics. If the user prefers, hold flagged channels out of the
named layer until reviewed.

---

## 4. Local Data Files and Row Counts

**`outputs/youtube_topic_treemap_20260617_v2b/channel_topic_projection.parquet`**
- 200,000 rows; 200,000 unique channels; 25 columns
- Key columns: `channel_id`, `channel_title`, `language_code`, `latest_views`,
  `subscriber_count`, `snapshot_date`, `raw_topic_categories`,
  `normalized_slugs`, `canonical_slugs`, `mapped_nodes`, `display_families`,
  `display_leaves`, and diagnostic flags.

**`outputs/youtube_topic_treemap_20260617_v2b/channel_label_allocations.parquet`**
- 1,685,836 rows; 200,000 unique channels
- Allocation-method row counts:
  - `dominant_display`: 200,000
  - `equal_leaf`: 371,459
  - `equal_raw_label_after_parent_prune`: 371,459
  - `family_balanced`: 371,459
  - `specificity_weighted`: 371,459
- **v3 uses `family_balanced` for the long tail, then overrides the ~100 CSV
  channels with a single hard placement (see 5.5.3).**

**`outputs/youtube_topic_treemap_20260617_v2b/traffic_4wk.parquet`**
- Raw rows: 108,067; raw unique channels: 107,869 (contains duplicate channel
  rows from the TOO join; renderer dedupes by channel id)
- Raw valid non-null `view_count_4wk`: 104,611 before dedupe
- Raw negative deltas: 3,277 before dedupe
- Current snapshot: `2026-06-15`; prior snapshot: `2026-05-18`

**Renderer after traffic dedupe (v2b):**
- Duplicate rows deduped: 198
- Channels with current snapshot: 107,777; with prior snapshot: 107,690
- Negative raw deltas: 3,266; valid 4-week traffic channels: 104,424
- Positive traffic channels plotted: 103,046; positive allocation rows plotted: 193,417
- Total 4-week views used after dedupe: `2,229,342,931,147`

---

## 5. What Has Been Done So Far

### 5.1 Spec Placement and Updates

The treemap spec exists in two locations: `docs/TREEMAP_SPEC.md` and
`youtube_descriptive/src/treemap_spec.md`. The **v2b** visualization patch in
those files required:

- static master = language → family only
- top 12 languages; low-share family pooling; max 120 static cells;
  min static cell area ≥ 0.3% of total; squarify packing; PNG + SVG export
- self-contained interactive HTML; top 15 channels + Other (N channels) per leaf

> **Superseded by v3 (`docs/TREEMAP_V3_SPEC.md`):** the static master is now
> variable-depth (language → family → leaf → named channels) with merged
> languages and the new palette. See Section 5.5.

Also established in v2b: the weekly traffic source rule using
`dev_sean.default.yt_channel_stats` instead of raw lifetime views as treemap
area, and the allocation formula `allocated_views = view_count_4wk *
allocation_weight`. The original Parquet allocation file still contains the old
lifetime-derived `allocated_views`; the renderer creates `allocated_views_4wk`
in memory and does not overwrite the raw column.

### 5.2 Original v2 Databricks Run

Notebook `youtube_descriptive/src/youtube_topic_treemap_v2.py`, run via
`.codex_databricks/run_youtube_topic_treemap_v2_20260617.sh`.

- Submit run: `963598563804515`; task run: `546721819338670`
- Existing cluster: `0601-203643-bkxsqffg`; result: success
- Acceptance values: `RECONCILIATION: PASS`, channels_processed = 200000,
  channel_allocation_rows = 1685836, plot_rows = 13088, snapshot 2026-06-15,
  total lifetime views ≈ 134,199,668,079,123, view-mass coverage ≈ 0.9911

The original rendered PNG/HTML in `outputs/youtube_topic_treemap_20260617` was
not usable as a static figure (too many levels at once; sliver storm).

### 5.3 Traffic Extraction for v2b

Helper `.codex_databricks/export_treemap_traffic_v2b_20260617.py`:

- Reads the projection-channel universe from
  `dev_sean.matt.yt_channel_topic_projection_v2_20260617`.
- Uses `prod_tads.youtube_too.yt_sl_channels` for channel universe/metadata.
- Uses `dev_sean.default.yt_channel_stats` for subscriber/lifetime-view
  snapshots and the 4-week delta.
- Resolves current = max snapshot date; prior = max snapshot date ≤ current − 28 days.
- Dedupes stats rows by `(canonical_id, DATE(collected_at))`, keeping latest.
- Computes `raw_4wk_views`, `view_count_4wk`, `avg_weekly_view_count`.
- Writes Parquet to `dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617_v2b/traffic_4wk.parquet`.

Successful traffic rerun: submit run `10866132594722`; task run
`384795401848230`; cluster `0601-203643-bkxsqffg`; result success.
Run page: `https://adb-1335559103600339.19.azuredatabricks.net/?o=1335559103600339#job/818765705261342/run/10866132594722`

The corrected job payload is `.codex_databricks/job_export_treemap_traffic_v2b_20260617.json`.

### 5.4 Local v2b Renderer

`scripts/render_treemap_v2b.py` expects:
`channel_label_allocations.parquet`, `channel_topic_projection.parquet`,
`traffic_4wk.parquet` (all under `outputs/youtube_topic_treemap_20260617_v2b/`).

It filters to `family_balanced`, joins traffic, computes
`allocated_views_4wk = view_count_4wk * allocation_weight`, checks conservation,
builds a language → family static master (squarify + matplotlib) and a full
language → family → leaf → channel interactive HTML (Plotly `go.Treemap`,
`branchvalues="total"`, `maxdepth=2`, `tiling.packing="squarify"`).

Reported v2b metrics included: `CONSERVATION: PASS`; total 4wk views
`2,229,342,931,147`; `STATIC CELLS: 73`; `MIN CELL AREA: 0.303%`;
`POOLED VIEW SHARE: 26.157%`; `LABELED CELLS: 31`; figure 4000×2400 px,
20×12 in, 200 DPI; interactive channel/other nodes 27,186. The static PNG was
opened and visually inspected; legibility verdict: PASS (language blocks
readable, no sliver storm).

### 5.5 v3 Refinement Requirements *(new — the current task)*

The v2b figure is legible but has the four defects listed at the top. v3 fixes
them. Full detail is in `docs/TREEMAP_V3_SPEC.md`; summary below. Refine v2b into
`scripts/render_treemap_v3.py`, reuse the same parquet inputs, write to
`outputs/youtube_topic_treemap_20260617_v3/`, and keep all prior artifacts.

#### 5.5.1 Language normalization (new column `language_display`; keep raw `language_code`)

Build `config/language_normalization.yaml`:

- All English variants → one **"English"**: `en`, `eng`, `en-US`, `en-IN`,
  `en-GB`, and any `en-*`.
- Strip region subtags and merge ISO-639-2/3 aliases to one base for every
  language: `pt`/`pt-PT`/`pt-BR`/`por` → "Portuguese"; `es`/`es-*`/`spa` →
  "Spanish"; `zh`/`zh-*`/`cmn` → "Chinese"; `ar`/`ar-*`/`ara` → "Arabic";
  `hi`/`hin` → "Hindi"; `ru`/`rus` → "Russian"; etc. (general rule: lowercase,
  take base subtag, map 3-letter to 2-letter where applicable, then base →
  display name).
- LID review-cluster / non-ISO codes (e.g. `iberian_romance_review_cluster`,
  `hindi_related_north_indic_review_cluster`, anything matching `*_cluster` or
  not a valid ISO code) → pool into **"Other languages"**; print each with its
  view mass. Do not present them as real languages.
- Recompute the top-12 languages by allocated 4wk views **after** the merge;
  pool the rest into "Other languages". English will become a single large block
  (~1.16T+ 4wk views) and dominate.

#### 5.5.2 Color palette (color encodes FAMILY; gray = residuals only)

`FAMILY_COLOR_MAP` (fixed; editable config; assert it covers all families) —
the Okabe–Ito colorblind-safe palette:

- Entertainment = `#D55E00`
- Lifestyle = `#56B4E9`
- Music = `#CC79A7`
- Gaming = `#009E73`
- Society = `#0072B2`  *(a HUE — never gray)*
- Sports = `#E69F00`
- Knowledge = `#E6C700`

Residual buckets (gray; never a saturated family color):

- `[Family] - unspecified` = the family hue blended ~55% toward `#BBBBBB`
- `Other / Unmapped YouTube topic` = `#9E9E9E`
- `Unlabeled / No YouTube topicCategories` = `#CFCFCF`

Within a family: child leaves use a gentle lightness ramp of the family hue
(largest leaf = base hue, smaller leaves lighter, deterministic). Named channels
inherit their leaf hue, slightly lightened. No new hues below the family level.
White tile borders (`marker.line.color="white"`, width 1), `tiling.pad=2`,
inside-text auto-contrast. Verify grayscale + CVD legibility.

#### 5.5.3 Child leaves and named channels (variable depth, gated by legibility)

Static master path: `language_display → family → leaf → named top channels`,
deepening **only where cells stay legible**:

- Always show language (top 12 + "Other languages") and family.
- Break out a leaf as its own cell only if its area ≥ 0.5% of total; pool a
  family's remaining small leaves into `[Family] - other leaves`. Keep
  `[Family] - unspecified` as its own leaf only if ≥ 0.5%, else pool it.
- Min rendered cell area ≥ 0.3% of total. Soft cap ~200 static cells; the visual
  self-check is the real gate.

Named top channels (from `config/treemap_top_channel_placement.csv`):

- For each CSV channel: **override** `family_balanced` with a single allocation
  at `(language_display(channel), revised_primary_family, revised_primary_leaf)`
  with weight = 1 (100% of its `view_count_4wk`).
- For all other ~199,900 channels: keep `family_balanced` fractional allocation.
- Static: show a named channel box only where its leaf cell is large enough to
  carry labels (leaf area ≥ ~1.5%) and the channel's own area is visible; show
  top channels by `view_count_4wk` + `Other channels (N)`; smaller named
  channels fall into `Other channels`. Hover: channel title, `view_count_4wk`,
  `non_primary_display_paths_to_retain_as_metadata`, `needs_manual_review`.
- Interactive: full depth; within each leaf, top-15 placed channels +
  `Other (N channels)`.

#### 5.5.4 Conservation (re-verify after merge + hard-placement)

Each channel's full `view_count_4wk` is placed exactly once in aggregate (CSV
channels: one weight-1 row; others: `family_balanced` weights summing to 1).
Assert per-channel weight sum = 1 and total allocated = total `view_count_4wk`
within tolerance. Print `CONSERVATION: PASS`.

---

## 6. Current Output Artifacts

**v2b (keep; do not use the v2 directory's all-level figure as the paper figure):**

- Static master PNG: `outputs/youtube_topic_treemap_20260617_v2b/treemap_static_master_v2b.png`
  (~311 KB; 4000×2400 px; 20×12 in; 200 DPI)
- Static master SVG: `outputs/youtube_topic_treemap_20260617_v2b/treemap_static_master_v2b.svg` (~123 KB)
- Interactive explorer HTML: `outputs/youtube_topic_treemap_20260617_v2b/treemap_interactive_explorer_v2b.html`
  (~13 MB; self-contained; 0 external `<script src>` tags; contains
  `"branchvalues":"total"`, `"maxdepth":2`, `"packing":"squarify"`, `"pad":2`,
  `"sort":true`)
- Prior all-level language-first figure: `outputs/youtube_topic_treemap_20260617/` (do not use as the paper figure)

**v3 (to be written by `scripts/render_treemap_v3.py`):**

- `outputs/youtube_topic_treemap_20260617_v3/treemap_static_master_v3.png` (≥ 2000×1200 px, ≥ 200 DPI) + `.svg`
- `outputs/youtube_topic_treemap_20260617_v3/treemap_interactive_explorer_v3.html` (self-contained; full depth)
- `outputs/youtube_topic_treemap_20260617_v3/treemap_static_cells_v3.csv` (final cells + labels, for the figure caption/provenance)
- `outputs/youtube_topic_treemap_20260617_v3/render_log_v3.txt` (all printed metrics)

---

## 7. Reproduction Commands

Run from `cd /Users/hindman/Documents/GitHub/youtube-descriptive`.

Install local Python dependencies if needed:

```
python3 -m pip install --user squarify plotly
```

If the original v2 Databricks artifacts need to be regenerated, use the existing
runner (it imports `youtube_descriptive/src/youtube_topic_treemap_v2.py`, uploads
`config/youtube_topic_hierarchy_v2.yaml`, submits to the existing cluster, and
writes the original DBFS artifacts):

```
env DATABRICKS_AUTH_STORAGE=plaintext \
  DATABRICKS_PROFILE=matt.hindman@researchaccelerator.org \
  CLUSTER_ID=0601-203643-bkxsqffg \
  bash .codex_databricks/run_youtube_topic_treemap_v2_20260617.sh
```

If only local copies of the original projection/allocation Parquet are missing,
copy them from DBFS:

```
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

```
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

Then render. v2b (baseline):

```
python3 scripts/render_treemap_v2b.py
```

v3 (the current task; reuses the same parquet inputs):

```
python3 scripts/render_treemap_v3.py
```

The v3 renderer should print the language-normalization map, `ENGLISH IS ONE
BLOCK: PASS`, the palette line, leaf cell count, channels placed/labeled, static
cell metrics, `PACKING: squarify`, `CONSERVATION: PASS`, and the static PNG/SVG +
interactive HTML paths, then open and visually inspect the static PNG.

---

## 8. Logs and Run Records

No durable local log file was written during the v2/v2b passes (v3 must write
`render_log_v3.txt`). Useful records:

- Databricks traffic job JSON: `.codex_databricks/job_export_treemap_traffic_v2b_20260617.json`
- Databricks traffic notebook/source: `.codex_databricks/export_treemap_traffic_v2b_20260617.py`
- Traffic rerun: submit run `10866132594722`, task run `384795401848230`,
  run page `https://adb-1335559103600339.19.azuredatabricks.net/?o=1335559103600339#job/818765705261342/run/10866132594722`
- Retrieve traffic run output:
  ```
  env DATABRICKS_AUTH_STORAGE=plaintext databricks -p matt.hindman@researchaccelerator.org \
    jobs get-run-output 384795401848230 --output json
  ```
  (The `get-run-output` response had empty `notebook_output` in that CLI session,
  but the run state was TERMINATED / SUCCESS and the DBFS Parquet was copied.)
- Original v2 treemap run: submit run `963598563804515`, task run `546721819338670`.

---

## 9. Important Caveats

- **Language normalization is required in v3 (Section 5.5.1).** The v2b renderer
  kept raw language codes separate (so `en`, `eng`, `en-IN`, `en-US`, `en-GB`
  each became their own block). v3 merges them into one "English", strips region
  subtags for other languages, and pools LID review-cluster pseudo-codes into
  "Other languages". The raw `language_code` column is preserved; a new
  `language_display` column carries the merged value. Document the full map.
- **Named-channel placement (v3) hard-places the ~100 CSV channels** at their
  `revised_primary_path` while the rest stay fractional (`family_balanced`).
  This conserves views (each channel's full 4wk views placed exactly once) and
  is the intended head/tail treatment, but the top-100 placement is a curated
  override, not the uniform allocation rule. Note it in the figure provenance.
- The current renderer is local post-processing. The full Spark notebook still
  produces lifetime-view allocation columns and old-style HTML. A cleaner future
  step is to integrate the 4-week traffic measure, the language normalization,
  and the v3 visualization into `youtube_descriptive/src/youtube_topic_treemap_v2.py`.
- The local allocation file retains the original lifetime-derived
  `allocated_views`. The renderer creates `allocated_views_4wk` in memory.
- The traffic extract contains duplicate `channel_id` rows because
  `yt_sl_channels` can duplicate channel IDs. The renderer dedupes by
  `channel_id`, keeping latest current/prior timestamps. A future Databricks
  helper should dedupe `yt_sl_channels` before writing the traffic extract.
- Channels missing current/prior snapshots or with negative deltas are retained
  in diagnostics but do not contribute positive treemap area.
- Some language codes are LID artifacts (e.g. a Chinese-titled channel labeled
  `en-US`); v3 only normalizes code variants, it does not re-run LID. Any deeper
  language reassignment must be explicit and documented.
- The static figure labels language blocks, large family tiles, large leaf
  tiles, and named channels only where the cell is large enough. Smaller cells
  are intentionally unlabeled and rely on the legend and interactive HTML.
- The interactive HTML is large because it embeds Plotly JS and data for a
  self-contained file.

---

## 10. Recommended / Required Refinement Work

**Required for v3 (this pass):**

- Apply the language normalization map (Section 5.5.1): merge `en`/`eng`/`en-*`
  into one "English", strip region subtags for other languages, pool
  review-cluster codes into "Other languages".
- Apply the Okabe–Ito family palette with gray reserved for residual buckets
  only; Society must be a hue, not gray (Section 5.5.2).
- Show child leaves under families where legible, and place named top channels
  from `config/treemap_top_channel_placement.csv` at their primary leaf where
  legible (Section 5.5.3).
- Re-verify conservation after the merge and hard-placement (Section 5.5.4).
- Write `render_log_v3.txt` and `treemap_static_cells_v3.csv` so validation does
  not depend on chat transcript text. Add a short README in the v3 output
  directory with artifact provenance and metrics.

**High priority (next):**

- Move the 4-week traffic join and the language normalization into the main
  Databricks notebook so the Delta outputs and artifacts are generated in one
  reproducible pipeline.
- Deduplicate `prod_tads.youtube_too.yt_sl_channels` by `channel_id` in the
  Databricks traffic helper/query, then compare row counts with the local
  renderer dedupe.

**Medium priority:**

- Add optional family-level small multiples (`family → leaf → top channels`,
  capped at ~60 cells each).
- Add automated image checks for static PNG dimensions and a coarse sliver
  metric, but keep manual visual inspection as the final gate.
- Decide whether "Other languages" should remain a parent block with family
  children (as currently implemented) or a single opaque pooled cell.

**Lower priority:**

- Add notebook tests or local smoke tests for conservation and HTML settings.

---

## 11. Acceptance Checklist for v3

Before declaring the v3 treemap refined, require all of the following:

- `CONSERVATION: PASS` after language merge and named-channel hard-placement
  (per-channel weight sum = 1; total allocated = total `view_count_4wk`).
- **Language merge:** the full code → display map printed; all English variants
  (`en`, `eng`, `en-US`, `en-IN`, `en-GB`, `en-*`) collapsed into one "English"
  cell (`ENGLISH IS ONE BLOCK: PASS`); region subtags stripped for other
  languages; review-cluster / non-ISO codes pooled into "Other languages" and
  printed with view mass; top-12 recomputed after the merge.
- **Palette:** family color encodes the fixed Okabe–Ito map; gray used only for
  residual buckets (`[Family] - unspecified`, `Other / Unmapped`, `Unlabeled`);
  Society is a hue, not gray (`PALETTE: okabe-ito, residuals-only-gray`).
- **Child leaves** shown under families where legible (variable depth gated by
  cell area; small leaves pooled to `[Family] - other leaves`); leaf cell count
  printed.
- **Named top channels** from `config/treemap_top_channel_placement.csv`
  hard-placed at `revised_primary_family > revised_primary_leaf`, shown as
  labeled boxes where the leaf cell is large enough, with non-primary paths and
  `needs_manual_review` in hover; number placed and number labeled printed.
- Static PNG and SVG written; PNG ≥ 2000×1200 px and ≥ 200 DPI.
- Min rendered static cell area ≥ 0.3%; pooled view share printed; labeled cell
  count printed; soft cap ~200 static cells.
- `PACKING: squarify` printed and verified; white tile borders; `tiling.pad=2`.
- Interactive HTML written, self-contained, no external `<script src>` tags,
  full depth `language → family → leaf → channel`, with
  `branchvalues="total"`, `maxdepth=2`, `tiling.packing="squarify"`,
  `tiling.pad=2`, `sort=True`; channels in interactive leaves pooled to
  top-15 + `Other (N channels)`.
- Static PNG opened and visually inspected; legibility verdict printed
  confirming English is a single block, no sliver storm, family hues distinct,
  residual buckets visibly gray, and leaves/channels readable where shown.
- `render_log_v3.txt` and `treemap_static_cells_v3.csv` written; prior v2/v2b
  artifacts kept (timestamped v3 outputs).
- `go.Treemap` uses explicit unique `ids`/`parents` (not `px.treemap(path=...)`);
  raw `language_code` and raw view columns not mutated (new `language_display`
  and placement-override columns added).

---

## 12. Quick Orientation for Another Model

If you only have time to inspect a few files, inspect these:

- `docs/TREEMAP_V3_SPEC.md` *(authoritative v3 spec)*
- `scripts/render_treemap_v2b.py` *(baseline renderer to extend)*
- `config/treemap_top_channel_placement.csv` *(named-channel placements)*
- `.codex_databricks/export_treemap_traffic_v2b_20260617.py`

If you only have time to rerun one command after dependencies are installed and
`config/treemap_top_channel_placement.csv` is in place:

```
python3 scripts/render_treemap_v3.py
```

If rerender succeeds and prints the expected metrics
(`ENGLISH IS ONE BLOCK: PASS`, `PALETTE: okabe-ito, residuals-only-gray`,
`CONSERVATION: PASS`, static cell metrics), open
`outputs/youtube_topic_treemap_20260617_v3/treemap_static_master_v3.png`
and inspect legibility before making any further claim about the figure.