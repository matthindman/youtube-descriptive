# YouTube Top-of-Ocean Topic Treemap v2

## Objective

Build a working treemap for the ~200,000 “top-of-ocean” YouTube channels, roughly the largest channels by subscriber count.

The figure represents YouTube’s own exposed channel topic metadata, not a researcher-defined content taxonomy.

The analytic path is:

language
  → YouTube topic family
    → YouTube topic leaf or “[family] — unspecified”
      → channel allocation

The value is allocated recent channel traffic: the latest available weekly
snapshot lifetime-view count minus the snapshot approximately four weeks
earlier. Lifetime snapshot fields are retained for diagnostics, but treemap
area uses the 4-week delta.

The output must preserve total view mass. It must not double-count parent and child labels.

## Core conceptual rules

1. The source field `topic_categories` / `topicDetails.topicCategories[]` is an array, not a scalar.
2. Array order must not be used as a primary-topic signal.
3. YouTube’s documented topic list is a scaffold, not a guarantee that current channel arrays form a clean tree.
4. Parent-child closure must be measured empirically.
5. Parent labels are removed from allocation only when a trusted child in the same family is also present.
6. Parent-only labels are retained as “[family] — unspecified.”
7. Cross-family labels are preserved and fractionally allocated.
8. Unknown labels are never dropped; they go to “Other / Unmapped YouTube topic” until mapped.
9. The old flat-primary table is only a sensitivity comparison, not the main method.
10. The treemap represents allocated view mass, not a claim about each channel’s single true content category.

## Data assumptions

Expected source fields, though exact table/column names may differ:

- channel_id
- channel_title or channel_name
- language or language_code
- topic_categories: array of YouTube topicDetails Wikipedia URLs
- current cumulative lifetime channel views from weekly snapshots
- prior cumulative lifetime channel views from the snapshot four weeks earlier
- recent 4-week view delta, with negative deltas treated as invalid/null
- optional current/prior snapshot dates

Known likely source table:

- dev_sean.default.channel_category

Known old comparison table:

- dev_sean.matt.yt_channel_topic_flat_primary_draft_20260615

Use existing repository code to discover actual table and column names.

## Weekly Traffic Source

Use `dev_sean.default.yt_channel_stats` as the weekly channel snapshot table for
traffic.

Important fields:

- `canonical_id` = YouTube channel id
- `channel_name`
- `subscriber_count`
- `total_view_count` = lifetime views at that snapshot
- `collected_at` = snapshot timestamp

Use the latest available snapshot and the snapshot four weeks earlier. The
current observed latest is `2026-06-15`; the exact four-week prior is
`2026-05-18`. Derive recent view count as:

`current.total_view_count - prior.total_view_count`

Treat negative deltas as invalid/null, not real negative traffic.

Join this traffic to the YouTube TOO channel universe using:

`prod_tads.youtube_too.yt_sl_channels.channel_id = dev_sean.default.yt_channel_stats.canonical_id`

Use `prod_tads.youtube_too.yt_sl_channels` for the TOO channel
metadata/universe, and use `dev_sean.default.yt_channel_stats` only for
subscriber/current lifetime-view snapshots and four-week view deltas.

Recommended SQL pattern:

```sql
WITH available_dates AS (
  SELECT DISTINCT DATE(collected_at) AS snapshot_date
  FROM dev_sean.default.yt_channel_stats
),
params AS (
  SELECT
    MAX(snapshot_date) AS current_date,
    MAX(CASE
      WHEN snapshot_date <= DATE_SUB((SELECT MAX(snapshot_date) FROM available_dates), 28)
      THEN snapshot_date
    END) AS prior_date
  FROM available_dates
),
stats_deduped AS (
  SELECT
    canonical_id,
    channel_name,
    subscriber_count,
    total_view_count,
    collected_at,
    DATE(collected_at) AS snapshot_date,
    ROW_NUMBER() OVER (
      PARTITION BY canonical_id, DATE(collected_at)
      ORDER BY collected_at DESC
    ) AS rn
  FROM dev_sean.default.yt_channel_stats
  WHERE DATE(collected_at) IN (
    (SELECT current_date FROM params),
    (SELECT prior_date FROM params)
  )
),
current_stats AS (
  SELECT *
  FROM stats_deduped
  WHERE snapshot_date = (SELECT current_date FROM params)
    AND rn = 1
),
prior_stats AS (
  SELECT *
  FROM stats_deduped
  WHERE snapshot_date = (SELECT prior_date FROM params)
    AND rn = 1
),
traffic AS (
  SELECT
    c.canonical_id AS channel_id,
    c.subscriber_count AS current_subscriber_count,
    c.total_view_count AS current_lifetime_views,
    p.total_view_count AS prior_lifetime_views,
    c.collected_at AS current_collected_at,
    p.collected_at AS prior_collected_at,
    c.total_view_count - p.total_view_count AS raw_4wk_views,
    CASE
      WHEN p.total_view_count IS NULL THEN NULL
      WHEN c.total_view_count >= p.total_view_count THEN c.total_view_count - p.total_view_count
      ELSE NULL
    END AS view_count_4wk,
    CASE
      WHEN p.total_view_count IS NULL THEN NULL
      WHEN c.total_view_count >= p.total_view_count THEN (c.total_view_count - p.total_view_count) / 4.0
      ELSE NULL
    END AS avg_weekly_view_count
  FROM current_stats c
  LEFT JOIN prior_stats p
    ON c.canonical_id = p.canonical_id
)
SELECT
  yt.*,
  t.current_subscriber_count,
  t.current_lifetime_views,
  t.prior_lifetime_views,
  t.current_collected_at,
  t.prior_collected_at,
  t.raw_4wk_views,
  t.view_count_4wk,
  t.avg_weekly_view_count
FROM prod_tads.youtube_too.yt_sl_channels yt
LEFT JOIN traffic t
  ON yt.channel_id = t.channel_id;
```

## Work plan before coding

1. Search the repo for:
   - treemap
   - topic_categories
   - topicDetails
   - channel_category
   - yt_channel_topic_flat_primary_draft_20260615
   - language
   - view_count
   - lifetime views
   - Plotly
2. Print or log the relevant table schemas.
3. Identify the existing treemap pipeline and reuse its loading/rendering conventions where possible.
4. Create a new v2 hierarchy-aware pipeline. Do not overwrite old outputs.
5. Write a short implementation plan before modifying code.

## Recommended implementation environment

Use the project’s existing conventions.

If the data live in Databricks/Delta:
- Use Spark for source discovery, joins, normalization, allocation, diagnostics, and Delta writes.
- Collect only the aggregated plotting table to pandas for Plotly.

If the repo already has a pandas/polars treemap pipeline:
- It is acceptable to use pandas/polars because ~200,000 channels should be manageable.
- Still write parquet/CSV diagnostics and a reproducible config.

## Required file structure

Create or update:

config/youtube_topic_hierarchy_v2.yaml
src or notebooks pipeline file:
  youtube_topic_treemap_v2.py
  or youtube_topic_treemap_v2.ipynb

Create timestamped artifacts:

artifacts/youtube_topic_treemap_top_ocean_YYYYMMDD/
  treemap_youtube_topics_language_first.html
  treemap_youtube_topics_topic_first.html
  channel_topic_projection.parquet
  channel_label_allocations.parquet
  treemap_plot_rows.parquet
  topic_slug_inventory.csv
  topic_node_map.csv
  messiness_dashboard.csv
  allocation_sensitivity_summary.csv
  colabel_intersections.csv
  visible_head_audit.csv
  diagnostics.md

If Delta tables are the project norm, also write timestamped or versioned tables:

dev_sean.matt.yt_topic_node_map_v2_YYYYMMDD
dev_sean.matt.yt_channel_topic_projection_v2_YYYYMMDD
dev_sean.matt.yt_treemap_allocations_v2_YYYYMMDD
dev_sean.matt.yt_treemap_plot_rows_v2_YYYYMMDD
dev_sean.matt.yt_treemap_diagnostics_v2_YYYYMMDD

## Config: canonical scaffold

The canonical hierarchy must live in config/youtube_topic_hierarchy_v2.yaml, not hardcoded in the pipeline logic.

Seed scaffold:

Music:
  parent_slugs:
    - music
  children:
    christian_music: Christian music
    classical_music: Classical music
    country: Country
    electronic_music: Electronic music
    hip_hop_music: Hip hop music
    independent_music: Independent music
    jazz: Jazz
    music_of_asia: Music of Asia
    music_of_latin_america: Music of Latin America
    pop_music: Pop music
    reggae: Reggae
    rhythm_and_blues: Rhythm and blues
    rock_music: Rock music
    soul_music: Soul music

Gaming:
  parent_slugs:
    - gaming
    - video_game_culture
  children:
    action_game: Action game
    action-adventure_game: Action-adventure game
    casual_game: Casual game
    music_video_game: Music video game
    puzzle_video_game: Puzzle video game
    racing_video_game: Racing video game
    role-playing_video_game: Role-playing video game
    simulation_video_game: Simulation video game
    sports_game: Sports game
    strategy_video_game: Strategy video game

Sports:
  parent_slugs:
    - sports
  children:
    american_football: American football
    baseball: Baseball
    basketball: Basketball
    boxing: Boxing
    cricket: Cricket
    football: Football
    association_football: Association football
    golf: Golf
    ice_hockey: Ice hockey
    mixed_martial_arts: Mixed martial arts
    motorsport: Motorsport
    tennis: Tennis
    volleyball: Volleyball

Entertainment:
  parent_slugs:
    - entertainment
  children:
    humor: Humor
    humour: Humor
    movies: Movies
    film: Movies
    movie: Movies
    performing_arts: Performing arts
    professional_wrestling: Professional wrestling
    tv_shows: TV shows
    television: TV shows
    television_program: TV shows

Lifestyle:
  parent_slugs:
    - lifestyle
    - lifestyle_(sociology)
  children:
    fashion: Fashion
    fitness: Fitness
    physical_fitness: Fitness
    food: Food
    hobby: Hobby
    pets: Pets
    physical_attractiveness: Physical attractiveness / Beauty
    beauty: Physical attractiveness / Beauty
    technology: Technology
    tourism: Tourism
    vehicles: Vehicles

Society:
  parent_slugs:
    - society
  children:
    business: Business
    health: Health
    military: Military
    politics: Politics
    religion: Religion

Knowledge:
  parent_slugs:
    - knowledge
  children: {}

Other:
  parent_slugs: []
  children: {}

Important mapping notes:
- Professional wrestling belongs under Entertainment in the canonical scaffold.
- Motorsport belongs under Sports.
- Vehicles belongs under Lifestyle.
- Technology belongs under Lifestyle.
- Health belongs under Society.
- Knowledge is a top-level “Other topics” node, not a child of Society.
- Do not move labels to more intuitive human categories; the goal is to represent YouTube’s scheme.

## Alias handling

The config must include an editable alias section:

aliases:
  association_football: football
  football: football
  humour: humor
  television_program: tv_shows
  television: tv_shows
  tv_show: tv_shows
  tv_shows: tv_shows
  lifestyle_(sociology): lifestyle
  physical_fitness: fitness
  physical_attractiveness: physical_attractiveness
  beauty: physical_attractiveness

Implementation rule:
- Apply aliases before mapping.
- Preserve raw slug and aliased canonical slug.
- If a slug is unknown, do not drop it.
- Unknown slugs go to family = “Other / Unmapped YouTube topic” and leaf = “Unmapped: <slug>”.
- Optionally compute empirical co-occurrence candidates for unknown slugs, but do not use them for pruning unless manually added to config.

## Normalize topicCategories

For each channel:

1. Use the latest available category row.
2. If multiple rows exist, choose the most recent by ingestion date or snapshot date.
3. Preserve raw topic URL array.
4. Drop null and empty entries.
5. Deduplicate within channel.
6. Normalize each Wikipedia URL:
   - take last path segment
   - URL-decode
   - replace spaces with underscores
   - lowercase
   - strip leading/trailing whitespace
   - remove URL anchors/query strings
7. Do not use original array order except for diagnostics.

Create `topic_slug_inventory.csv` with:

- raw_slug
- canonical_slug
- example_raw_url
- channel_count
- total_latest_views
- mapped_flag
- mapped_family
- mapped_leaf
- mapping_source
- mapping_confidence
- notes

Print the top 50 slugs by view mass before allocation.

## Channel projection

For each channel, produce a single projection row:

- channel_id
- channel_title
- language_code
- current_lifetime_views
- prior_lifetime_views
- view_count_4wk
- current_collected_at
- prior_collected_at
- raw_topic_categories
- normalized_slugs
- canonical_slugs
- mapped_nodes
- display_families
- display_leaves
- n_raw_labels
- n_canonical_labels
- n_display_families
- n_display_leaves
- has_no_topic_categories
- has_unmapped_labels
- has_parent_only_label
- has_same_family_multi_child
- has_cross_family_labels
- broken_closure_imputed
- projection_notes

Projection logic:

For each mapped slug:
- If it is a parent slug, map it to family with node_type = parent.
- If it is a child slug, map it to family + leaf with node_type = child.
- If child is present but its parent slug is absent in raw labels, still roll it up to the family and set broken_closure_imputed = true.
- If both parent and child in the same family are present, drop the parent from allocation.
- If parent is present and no child in that family is present, create leaf = “[family] — unspecified”.
- If multiple child leaves in the same family are present, keep all child leaves.
- If multiple families are present, keep all families.
- If no topic categories exist, create family = “Unlabeled” and leaf = “No YouTube topicCategories.”
- If unmapped labels exist, create family = “Other / Unmapped YouTube topic” and leaf = “Unmapped: <slug>.”

## Main allocation method: family_balanced

For each channel:

If no topic categories:
- one allocation row
- family = “Unlabeled”
- leaf = “No YouTube topicCategories”
- allocation_weight = 1

Otherwise:
1. Let F = number of display families after parent/child pruning.
2. Each family receives 1 / F of channel views.
3. Within family f, let L_f = number of display leaves.
4. Each leaf receives (1 / F) * (1 / L_f).
5. allocated_views = view_count_4wk * allocation_weight.
6. Channels with null `view_count_4wk` are retained in diagnostics but have no
   positive area in the traffic treemap.

Examples:

Raw:
  [Music, Pop music, Hip hop music]
Display:
  Music → Pop music
  Music → Hip hop music
Allocation:
  Pop music = 0.5
  Hip hop music = 0.5

Raw:
  [Pop music, Hip hop music, Religion]
Display:
  Music → Pop music
  Music → Hip hop music
  Society → Religion
Allocation:
  Pop music = 0.25
  Hip hop music = 0.25
  Religion = 0.50

Raw:
  [Lifestyle]
Display:
  Lifestyle → [Lifestyle] — unspecified
Allocation:
  [Lifestyle] — unspecified = 1.0

Raw:
  []
Display:
  Unlabeled → No YouTube topicCategories
Allocation:
  No YouTube topicCategories = 1.0

## Sensitivity methods

Also implement at least three sensitivity rules:

1. equal_leaf
   - Split equally across all display leaves, regardless of family.

2. equal_raw_label_after_parent_prune
   - Split equally across nonredundant canonical labels after dropping same-family parents when children exist.

3. dominant_display
   - Assign all views to one leaf using a deterministic display-only rule:
     a. If one display leaf, use it.
     b. Prefer mapped child leaf over parent-unspecified.
     c. Prefer non-unmapped over unmapped.
     d. Prefer family with largest family-level allocated view mass under family_balanced, if already computed.
     e. If still tied, use stable alphabetical order.
   - Clearly label this as display-only and not the analytic main method.

Optional:
4. specificity_weighted
   - Child leaves receive weight 1.0.
   - Parent-unspecified leaves receive weight 0.5.
   - Normalize weights to sum to 1.
   - Use only for sensitivity because it is more arbitrary.

## Allocation output

Create `channel_label_allocations.parquet` with one row per channel × allocation_method × display leaf:

- snapshot_date
- channel_id
- channel_title
- language_code
- latest_views
- allocation_method
- yt_family
- yt_leaf
- allocation_weight
- allocated_views
- raw_topic_categories
- normalized_slugs
- canonical_slugs
- display_families
- display_leaves
- has_no_topic_categories
- has_unmapped_labels
- has_parent_only_label
- has_same_family_multi_child
- has_cross_family_labels
- broken_closure_imputed

## Required reconciliation checks

These must be printed and must pass:

1. CHANNELS PROCESSED: <n>
   - n should be >150000 for the top-of-ocean sample.

2. Every source channel appears exactly once in the projection table.

3. Every source channel has at least one allocation row per allocation method.

4. For every channel × allocation_method:
   - abs(sum(allocation_weight) - 1) <= 1e-9

5. For every channel × allocation_method:
   - abs(sum(allocated_views) - view_count_4wk) <= max(1e-6, 1e-9 * view_count_4wk)

6. For each allocation method:
   - abs(sum(allocated_views) - sum(view_count_4wk)) / sum(view_count_4wk) <= 1e-9

7. Print:
   - RECONCILIATION: PASS
   - or RECONCILIATION: FAIL with the worst offending channel rows.

The previous suggested tolerance of 1e-3 is too loose for this application. Use 1e-9 relative tolerance unless the existing numeric stack forces a documented relaxation.

## Diagnostics

Create `messiness_dashboard.csv` and `diagnostics.md`.

Print these metrics:

Coverage:
- total channels
- total latest views
- channels with nonempty topicCategories
- view share with nonempty topicCategories
- channels with no topicCategories
- view share with no topicCategories
- VIEW-MASS COVERAGE: <pct>%

Mapping:
- distinct raw slugs
- mapped slugs
- unmapped slugs
- UNMAPPED-LABEL VIEW SHARE: <pct>%
- top 25 unmapped slugs by view mass
- top 25 unmapped slugs by channel count

Hierarchy:
- raw label cardinality distribution, channel-weighted
- raw label cardinality distribution, view-weighted
- display family cardinality distribution, channel-weighted
- display family cardinality distribution, view-weighted
- display leaf cardinality distribution, channel-weighted
- display leaf cardinality distribution, view-weighted
- parent-only channel share
- PARENT-ONLY VIEW SHARE by family
- same-family multi-child channel share
- same-family multi-child view share
- CROSS-FAMILY VIEW SHARE
- broken-closure channel share
- BROKEN-CLOSURE VIEW SHARE
- broken closure by family
- top 25 child-without-parent slugs by view mass

Co-label structure:
- top 50 raw co-label pairs by channel count
- top 50 raw co-label pairs by view mass
- top 50 display-family combinations by view mass
- top 50 display-leaf combinations by view mass
- write `colabel_intersections.csv`

Sensitivity:
For each allocation method:
- family view shares
- language × family view shares
- language × family × leaf view shares

Compare each sensitivity rule against family_balanced:
- L1 distance at family level
- L1 distance at language × family level
- L1 distance at language × family × leaf level
- Spearman rank correlation of family shares
- top 25 cells with largest absolute share change

Write:
- allocation_sensitivity_summary.csv

Risk flags:
- If unmapped-label view share > 1%, print WARNING.
- If unmapped-label view share > 5%, print HIGH RISK / PROVISIONAL FIGURE.
- If broken-closure view share > 5%, print HIGH DRIFT WARNING.
- Do not stop solely because of these warnings; still write outputs.

## Shorts / view-count caveat

If the latest snapshot date is after 2025-03-31, diagnostics.md must include:

“Starting March 31, 2025, YouTube changed Shorts view counting so channel viewCount includes starts and replays for Shorts. Channel lifetime viewCount series spanning this date may contain level shifts for Shorts-heavy channels.”

If producing weekly-difference flow treemaps:
- Do not compute a flow panel across the 2025-03-31 break without flagging it.
- If the previous and current snapshots straddle 2025-03-31, skip the flow panel or mark it as non-comparable.

## Treemap plotting table

Use allocation_method = family_balanced for the main treemap.

Create `treemap_plot_rows.parquet`.

Legibility defaults:

- TOP_K_LANGUAGES = 25
- TOP_N_CHANNELS_PER_LEAF = 10
- MAX_PLOT_ROWS = 20000
- language values outside top K become “Other languages.”
- within each language × family × leaf, show the top N channels by allocated views.
- aggregate the remaining channels into:
  - channel_display = “Other channels”
  - is_other_channel = true
  - other_channel_count = number of folded channels

If the plot exceeds MAX_PLOT_ROWS:
- reduce TOP_N_CHANNELS_PER_LEAF from 10 to 5.
- if still too large, reduce TOP_K_LANGUAGES from 25 to 20.
- print the final parameters used.

Required plotting columns:
- node_id
- parent_id
- label
- node_type: root / language / family / leaf / channel / other_channel
- language_display
- yt_family_display
- yt_leaf_display
- channel_display
- channel_id
- channel_title
- allocated_views
- raw_channel_views
- allocation_weight
- all_raw_topic_slugs
- all_display_leaves
- flags_display
- hover_text

## Plot construction

Prefer Plotly `go.Treemap` with explicit unique IDs and parents.

Construct IDs as full paths, not labels alone.

Examples:
- root
- lang::English
- lang::English/family::Music
- lang::English/family::Music/leaf::Pop music
- lang::English/family::Music/leaf::Pop music/channel::<channel_id>::<leaf_slug>
- lang::English/family::Music/leaf::Pop music/other

This is required because a channel may appear under multiple leaves. Repeated channel display names are allowed, but node IDs must be unique.

Use:
- branchvalues = "total"
- parent values = sum of descendant allocated views
- root value = total allocated views under the plotted language aggregation
- hovertemplate showing:
  - channel name
  - allocated views
  - raw channel views
  - allocation fraction
  - raw YouTube topic slugs
  - all display leaves
  - flags

If using Plotly Express instead:
- only use it if explicit ids and parents are supplied.
- verify that repeated channel labels do not collapse across different leaves.
- print DUPLICATE_ID_CHECK: PASS.

Write:
- treemap_youtube_topics_language_first.html

Also write, if feasible:
- treemap_youtube_topics_topic_first.html

Topic-first path:
YouTube topic family
  → YouTube topic leaf
    → language
      → channel allocation

## Visible head audit

Create `visible_head_audit.csv` for every non-other plotted channel rectangle above the final visibility threshold.

Columns:
- channel_id
- channel_title
- language_display
- latest_views
- allocated_views
- allocation_weight
- plotted_family
- plotted_leaf
- raw_topic_categories
- normalized_slugs
- canonical_slugs
- all_display_leaves
- has_cross_family_labels
- has_parent_only_label
- has_unmapped_labels
- broken_closure_imputed
- manual_review_status blank
- manual_review_notes blank

This audit is for verifying faithful display placement, not recoding YouTube labels.

## Optional flow treemap

If weekly snapshots are readily available:
- create latest-week delta views:
  latest_lifetime_views - previous_lifetime_views
- require nonnegative delta or flag negative deltas.
- skip if snapshots straddle the 2025-03-31 Shorts break.
- write treemap_youtube_topics_weekly_flow.html
- do not let this block the main cumulative treemap.

## Use of old flat-primary table

If available, join the old flat-primary table only for sensitivity diagnostics.

Do not use it in the main treemap.

Produce:
- flat_primary_family_comparison.csv
- L1 distance between old flat-primary category shares and new family_balanced family shares
- top categories whose mass moves most

Label all such outputs:
“single-label policy projection, not main analytic category.”

## Acceptance metrics to print

At the end of the run, print exactly:

CHANNELS PROCESSED: <n>
LATEST SNAPSHOT DATE: <date>
TOTAL LATEST VIEWS: <number>
VIEW-MASS COVERAGE: <pct>%
NO-LABEL VIEW SHARE: <pct>%
UNMAPPED-LABEL VIEW SHARE: <pct>%
BROKEN-CLOSURE VIEW SHARE: <pct>%
PARENT-ONLY VIEW SHARE: <pct>%
CROSS-FAMILY VIEW SHARE: <pct>%
RECONCILIATION: PASS/FAIL
MAIN TREEMAP HTML: <path>
CHANNEL ALLOCATION ROWS: <n>
PLOT ROWS: <n>
TOP 10 FAMILIES BY ALLOCATED VIEWS:
<printed table>
TOP 10 LANGUAGE × FAMILY CELLS:
<printed table>
TOP 15 UNMAPPED SLUGS BY VIEW MASS:
<printed table>
TOP 15 BROKEN-CLOSURE CHILD SLUGS BY VIEW MASS:
<printed table>
ALLOCATION SENSITIVITY SUMMARY:
<printed table>

## Do not do these things

- Do not mutate source tables.
- Do not overwrite the old flat-primary table.
- Do not classify channel content with an LLM.
- Do not build a new content taxonomy.
- Do not use array order as primary.
- Do not force one analytic label per channel.
- Do not drop parent-only labels.
- Do not drop unmapped labels.
- Do not use empirical co-occurrence edges to silently rewrite the hierarchy.
- Do not double-count parent and child labels.
- Do not call the output “ground truth content category.”
- Do not mark the goal complete unless reconciliation passes and the HTML exists.

## Final response required from the coding agent

When complete, report:

1. Files changed or created.
2. Tables written.
3. Artifact directory.
4. Source table names and resolved column names.
5. CHANNELS PROCESSED.
6. Allocation row count.
7. Plot row count.
8. Coverage statistics.
9. Unmapped-label view share.
10. Broken-closure view share.
11. Parent-only view share.
12. Cross-family view share.
13. Whether reconciliation passed.
14. HTML path.
15. Provisional assumptions and remaining risks.

## VISUALIZATION PATCH (replaces the plotting/output section)

Root cause of prior failure: one static frame, all 4 levels, ~all languages,
no minimum-area pooling, sliced (not squarified) tiles. Fix = two artifacts +
aggressive pruning + squarify + depth limit + legibility self-check.

### Artifact 1 — STATIC MASTER (the paper figure)

- Levels: language -> family ONLY. NO leaf, NO channel.
- Languages: top 12 by allocated views; pool the rest into one "Other
  languages" cell. Within each language, pool families < 1% of that language's
  views into "Other (families)".
- Hard cap: <= 120 rendered cells. If exceeded, reduce top-K languages or raise
  the family-pool threshold until under cap.
- Layout: squarified. Use the `squarify` library + matplotlib (preferred for
  print) OR Plotly go.Treemap with maxdepth=2 rendered at the top level.
  If Plotly: tiling.packing="squarify", tiling.pad=2, tiling.squarifyratio=1,
  sort=True.
- Min cell area must be >= 0.3% of total; verify and pool further if not.
- Labels: language name on each language block; family name only on family
  tiles whose area >= 1.5% of total; smaller tiles unlabeled (rely on the
  interactive figure / legend).
- Color: by FAMILY, fixed categorical palette, <= 9 hues (7 families + Unmapped
  + Unlabeled). Same family = same color everywhere. Legend + title + source.
- Export: SVG and PNG at >= 2000x1200, >= 200 DPI.

### Artifact 2 — INTERACTIVE EXPLORER (HTML supplement)

- Full hierarchy: language -> family -> leaf/"[Family]-unspecified" -> channel.
- go.Treemap, branchvalues="total", maxdepth=2 (initial view = language->family;
  click to drill), tiling.packing="squarify", tiling.pad=2, sort=True.
- Within each leaf: top 15 channels by allocated views + one "Other (N channels)"
  cell. Hover shows channel name, allocated views, raw channel views, allocation
  weight, and the channel's full raw topic-slug set.
- Single self-contained HTML.

### Artifact 3 — OPTIONAL detail small-multiples (appendix)

- One static pruned treemap per top family: family -> leaf -> top channels,
  each capped at ~60 cells. Skip if time-constrained.

### Pruning (apply to EVERY figure, before plotting)

- Pool any node below MIN_FRAC of its parent's value into a sibling "Other".
- Tune MIN_FRAC / top-K so each static figure is under its cell cap.
- Never render all levels of all languages in one static frame.

### Self-check (PRINT to transcript)

- "STATIC CELLS: <n>" (<=120); "MIN CELL AREA: <pct>%" (>=0.3);
  "POOLED VIEW SHARE: <pct>%"; "LABELED CELLS: <n>"; figure dimensions.
- OPEN the rendered static PNG and inspect it. Print a short legibility
  verdict: are language blocks individually readable? any region that is a
  stack of thin horizontal slivers? If yes -> raise pruning and re-render
  before declaring done.
- "CONSERVATION: PASS" must still hold.

### Do NOT

- Render a single static figure with leaf or channel levels across all
  languages. Channels and leaves live ONLY in the interactive (drill-down)
  figure or in the per-family small-multiples.
- Use slice/dice packing. Use squarify.
