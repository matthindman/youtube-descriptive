# Full-Corpus Treemap Runbook

**Last updated:** 2026-07-16
**Pipeline version:** `full_corpus_lid_v3_20260715_20260716_v1`
**Language label version:** `lid_v3_channel_crawl_full_20260623_deepseek_flash_20260715_v1`

This is the execution and lineage record for expanding the YouTube language/topic
treemap from the former 202,985-channel TOO cohort to the current 4,798,717-channel
LID silver universe. The analysis cohort is the subset with at least 10,000
subscribers in the fixed 2026-06-15 channel snapshot.

The visual design remains governed by [TREEMAP_V3_SPEC.md](TREEMAP_V3_SPEC.md).
The original data and visualization requirements remain in
[TREEMAP_SPEC.md](TREEMAP_SPEC.md).

## Required Databricks Environment

Use only the registered profile and existing compute documented in
`youtube_descriptive/src/AGENT_DATA_CONTEXT.md`.

```bash
env DATABRICKS_AUTH_STORAGE=plaintext databricks \
  -p matt.hindman@researchaccelerator.org ...
```

- Host: `https://adb-1335559103600339.19.azuredatabricks.net`
- Existing all-purpose cluster: `matt-research-gencompute`
- Cluster ID: `0601-203643-bkxsqffg`
- SQL warehouse for bounded audits: `86100da4e1fe8713`
- Output namespace: `dev_sean.matt`

Do not use a default or legacy profile, create another cluster, or restart other
compute for this workflow.

## Authoritative Inputs

| Purpose | Table or file | Contract |
|---|---|---|
| Current language lookup | `dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_current` | One row per `channel_id`; analyze `channel_language` |
| Versioned language history | `dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_labels` | Provenance/history only; filter `label_version` |
| Full-crawl topic arrays | `dev_sean.default.channel_category` | Join `language.channel_id = topics.canonical_id`; use `topic_categories` |
| Weekly channel snapshots | `dev_sean.default.yt_channel_stats` | Join `channel_id = canonical_id`; fixed dates below |
| Topic hierarchy | `config/youtube_topic_hierarchy_v2.yaml` | Editable family/leaf taxonomy |
| Topic display remaps | `config/topic_remap.yaml` | Display-layer remaps; raw taxonomy is retained |
| Language merges | `config/language_normalization.yaml` | Small editorial merges such as `cmn`/`yue` to Chinese |
| ISO names | `config/iso639_language_names.csv` | Generated ISO 639-3, bibliographic-alias, and collection-code names |
| Named channel placement | `config/treemap_top_channel_placement.csv` | Editable display placement; does not alter raw allocations |

Do not use the old 202,985-row taxonomy derivative or `yt_sl_channels` as the
full-corpus topic universe. `yt_channel_descriptions` supplied LID text but has
no topic arrays.

## Traffic Definition

The pipeline deduplicates each channel/date to the latest `collected_at` row.

- Current snapshot: `2026-06-15`
- Prior snapshot: `2026-05-18`
- Raw four-week delta: `current.total_view_count - prior.total_view_count`
- Valid four-week delta: raw delta when prior exists and current is not lower
- Negative deltas: invalid and null, never negative traffic
- Cohort: `current_subscriber_count >= 10000`

The full enriched base retains all 4,798,717 language rows and marks cohort and
traffic eligibility explicitly. Projection and allocation tables use the
subscriber cohort. Zero, missing, and invalid deltas remain represented but do
not contribute visible area.

## Language Normalization

`channel_language` remains unchanged. `language_display` is derived in this
order:

1. `und` becomes `Undetermined`.
2. An editable override in `language_normalization.yaml` is applied.
3. The generated ISO catalog name is used.
4. A non-catalog result is retained as `Unregistered code (xxx)`.

No classified code is silently converted to `Other languages`. The renderer
selects the top 12 classified display languages by allocated four-week views,
keeps `Undetermined` separate, and pools the classified tail only at render time.

Regenerate the pinned ISO lookup with:

```bash
python3 -m pip install \
  --target /tmp/treemap_iso_packages \
  pycountry==24.6.1

PYTHONPATH=/tmp/treemap_iso_packages \
  python3 scripts/build_iso639_language_map.py
```

The generator was run with `pycountry==24.6.1`. The observed source contains 566
classified codes: 532 map through the pinned catalog and 34 non-catalog codes,
covering 135 channels, are retained and reported.

## Materialization Pipeline

The Databricks source notebook is:

`youtube_descriptive/src/youtube_topic_treemap_full_corpus.py`

The current language silver table is reproducibly assembled by
`.codex_databricks/publish_channel_language_silver_20260715.py` from the frozen
dual-LID and DeepSeek fallback outputs documented in
`youtube_descriptive/src/README_language_lid_v3.md`.

It performs the following operations in Spark:

1. Starts from the one-row-per-channel language silver current table.
2. Left-joins current/prior traffic and latest topic arrays.
3. Writes the full enriched channel base before applying the subscriber filter.
4. Normalizes raw topic URLs/slugs and applies aliases.
5. Projects parent, child, unmapped, and unlabeled topic nodes.
6. Prunes a parent label when a child in the same family is present.
7. Allocates equal mass across families, then equally across leaves within family.
8. Writes raw family-balanced allocations before any named-channel override.
9. Applies the editable placement CSV in a separate display allocation table.
10. Builds full language/family/leaf totals and top-15-plus-Other channel nodes.
11. Builds bounded renderer rows for top languages, `Undetermined`, and the tail pool.
12. Runs source, coverage, normalization, and conservation assertions.

## Versioned Delta Outputs

All outputs use prefix `dev_sean.matt.yt_treemap_full_corpus_lid_v3_20260715_v1`:

| Suffix | Contents |
|---|---|
| `_channel_base` | All language rows plus raw traffic/topics, display language, and eligibility flags |
| `_topic_projection` | One subscriber-cohort row per channel with raw arrays and projected display items |
| `_allocations_family_balanced_raw` | Pre-placement family-balanced allocations |
| `_allocations_display_v3` | Display allocations after editable named-channel placement |
| `_language_family_leaf` | Compact totals for every display language/family/leaf |
| `_top15_channels_per_leaf` | Full-language top 15 plus `Other (N channels)` per leaf |
| `_renderer_rows` | Bounded local static/interactive renderer input |
| `_qa` | Metrics and PASS/FAIL acceptance checks |

Every table receives run ID, language-label version, and snapshot table
properties. Raw topic arrays, `channel_language`, raw four-week delta, cleaned
delta, and pre-placement allocations are preserved.

## Run Command

From the repository root:

```bash
scripts/run_youtube_topic_treemap_full_corpus.sh
```

The runner imports the notebook, uploads all editable configs, submits it on the
existing cluster, and downloads only compact exports to:

`outputs/youtube_topic_treemap_full_corpus_20260716_v1/databricks_export/`

Full channel and allocation data remain in Delta and are not copied into local
pandas.

The runner replaces its generated `databricks_export/` directory before each
download. Databricks CLI `fs cp --overwrite` does not remove obsolete Spark part
files; downloading over an older export can otherwise duplicate rows. The local
renderer independently rejects a coalesced export directory unless it contains
exactly one `part-*.parquet` file.

## Renderer

The established v3.13 renderer now supports the compact Spark input while
retaining its legacy input mode:

```bash
python3 scripts/render_treemap_v3.py \
  --renderer-rows outputs/youtube_topic_treemap_full_corpus_20260716_v1/databricks_export/renderer_rows.parquet \
  --interactive-rows outputs/youtube_topic_treemap_full_corpus_20260716_v1/databricks_export/interactive_rows.parquet \
  --run-manifest outputs/youtube_topic_treemap_full_corpus_20260716_v1/databricks_export/run_manifest.json \
  --output-dir outputs/youtube_topic_treemap_full_corpus_20260716_v1 \
  --artifact-tag full_corpus_v1
```

Static output uses nested `squarify` geometry. The interactive output uses
`go.Treemap` with `branchvalues="total"`, `maxdepth=2`, squarify packing,
padding 2, and sort enabled. Channel hover includes allocated views, raw channel
views, allocation weight, raw topic slugs, and placement metadata.

The two Parquet inputs have different scopes by design. `renderer_rows` contains
the bounded top-12-plus-tail language set used only by the static master;
`interactive_rows` contains every display language and top 15 plus Other channels
for every full-language topic leaf. The interactive renderer must never substitute
the static tail-pooled input.

## Acceptance Gates

The Databricks run must prove all of the following before rendering:

- 4,798,717 language rows and distinct channel IDs
- 4,642,010 classified and 156,707 `und`
- Exactly the required language-label version
- Fixed current/prior snapshot coverage matches the bounded pre-run audit
- Exactly 4,786,690 channels satisfy the subscriber floor
- Negative traffic deltas are null
- Topic row and nonempty-array coverage match the bounded pre-run audit
- Every cohort channel has raw and display allocations
- Per-channel raw and display allocation weights sum to 1
- Per-channel allocated views equal valid channel views
- Raw and display grand totals equal valid source views
- `CONSERVATION: PASS`

The renderer must then print and pass:

- `STATIC CELLS` at or below the 200-cell design cap
- Minimum ordinary structural area at least 0.3%
- Pooled view share and labeled-cell count
- Squarify packing and dimensions at least 2000x1200
- Every displayed language has a category label
- No thin-sliver storm
- Self-contained interactive HTML
- Every full-language display block is retained in the interactive HTML; no
  `Other languages` pooling is allowed there
- Human inspection of the rendered PNG

## Executed Run and Results

The pipeline was executed successfully on the registered existing cluster on
2026-07-16.

- Databricks parent run ID: `631808860368581`
- Databricks task run ID: `45029214411644`
- Run ID: `full_corpus_lid_v3_20260715_20260716_v1`
- Databricks checks: 31 of 31 PASS
- Source rows/distinct channels: 4,798,717 / 4,798,717
- Classified / `und`: 4,642,010 / 156,707
- Subscriber cohort: 4,786,690
- Valid / positive four-week deltas: 4,588,277 / 4,343,842
- Invalid negative deltas: 190,303, all excluded from traffic area
- Channels with a topic row / nonempty topic array: 4,795,956 / 4,526,985
- Valid allocated four-week views: 5,466,594,900,717
- Maximum raw/display weight error: `2.220446049250313e-16`
- Maximum per-channel allocation error: `1.1920928955078125e-07`
- Conservation: PASS

The rendered static master reports:

- `STATIC CELLS: 199`
- `MIN STRUCTURAL CELL AREA (unforced): 0.308%`
- `POOLED FAMILY VIEW SHARE: 1.120%`
- `LABELED CELLS: 74`
- Figure dimensions: 3000 x 1980 at 300 DPI
- Packing: nested squarify
- Coverage: at least four of the five leading families in every displayed
  language block

The PNG was opened and inspected after rendering. All 14 displayed language
blocks are identifiable, major family/topic tiles are individually readable,
and no region is a stack of thin horizontal slivers. Small regions remain
compact rectangles; white separation and the restrained residual fill avoid
the prior busy gray-box appearance. The resulting legibility assessment is
`PASS`.

Final local artifacts are in
`outputs/youtube_topic_treemap_full_corpus_20260716_v1/`:

- `treemap_static_master_full_corpus_v1.png`
- `treemap_static_master_full_corpus_v1.svg`
- `treemap_interactive_explorer_full_corpus_v1.html`
- `treemap_static_cells_full_corpus_v1.csv`
- `render_log_full_corpus_v1.txt`

The interactive HTML contains 68,802 channel/Other nodes across all 543
positive-view display languages, is self-contained, and uses top-15-plus-Other
channel pooling within each of the 7,839 language/family/leaf paths. Both
full-language coverage checks pass, and `Other languages` does not appear in this
artifact. Node IDs are unique, every non-root parent exists, and child totals obey
the `branchvalues="total"` contract. Its semantic checks confirm `go.Treemap`,
`maxdepth=2`, squarify packing, padding 2, and sorting.

## Output Policy

Generated Parquet, PNG, SVG, HTML, CSV, and render-log artifacts belong under
`outputs/` and are not committed. Commit source, configuration, and documentation
only unless the repository owner explicitly changes that policy.
