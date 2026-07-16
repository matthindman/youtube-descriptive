## TREEMAP V3 — language merge, palette, child leaves, named channels

Read docs/TREEMAP_REFINEMENT_HANDOFF.md FIRST for Databricks auth/profile,
compute, source tables, the local v2b artifacts, and scripts/render_treemap_v2b.py.
This spec SUPERSEDES that handoff's acceptance checklist and its "static =
language->family only" design. Refine the existing renderer; reuse the
family_balanced allocation parquet (it already reconciles). Area metric stays
4-week traffic (view_count_4wk). Keep prior artifacts; write v3 outputs.

### 1. LANGUAGE NORMALIZATION (new column language_display; keep raw language_code)
Build an editable config map config/language_normalization.yaml:
- English: en, eng, en-US, en-IN, en-GB, and any en-* -> "English".
- Strip region subtags and merge ISO-639-2/3 aliases to one base for every
  language: pt/pt-PT/pt-BR/por -> "Portuguese"; es/es-*/spa -> "Spanish";
  zh/zh-*/cmn -> "Chinese"; ar/ar-*/ara -> "Arabic"; hi/hin -> "Hindi";
  ru/rus -> "Russian"; etc. (general rule: lowercase, take base subtag, map
  3-letter to 2-letter where applicable, then base -> display name).
- Review-cluster / non-ISO codes (e.g. iberian_romance_review_cluster,
  hindi_related_north_indic_review_cluster, anything matching *_cluster or not a
  valid ISO code) -> pool into "Other languages"; PRINT each with its view mass.
- Recompute top-12 languages by allocated 4wk views AFTER the merge; rest pooled
  to "Other languages". Print the full code->display map applied.

### 2. PALETTE (color encodes FAMILY; gray = residuals only)
FAMILY_COLOR_MAP (fixed; editable config; assert covers all families):
  Entertainment=#D55E00  Lifestyle=#56B4E9  Music=#CC79A7  Gaming=#009E73
  Society=#0072B2  Sports=#E69F00  Knowledge=#E6C700
Residual buckets (gray; NEVER a saturated family color):
  "[Family] - unspecified" = the family hue blended ~55% toward #BBBBBB
  "Other / Unmapped YouTube topic" = #9E9E9E
  "Unlabeled / No YouTube topicCategories" = #CFCFCF
Within a family: child leaves = gentle lightness ramp of the family hue (largest
leaf = base hue, smaller leaves lighter, deterministic). Named channels inherit
their leaf hue, slightly lightened. NO new hues below family level.
Society MUST be a hue (#0072B2), not gray. White tile borders
(marker.line.color="white", width=1), tiling.pad=2. Inside-text auto-contrast.
Verify grayscale + CVD legibility.

### 3. CHILD LEAVES (variable depth, gated by legibility)
Static master path: language_display -> family -> leaf -> named top channels,
but DEEPEN ONLY where cells stay legible:
- Always show language (top 12 + "Other languages") and family.
- Break out a leaf as its own cell only if its area >= 0.5% of total; pool a
  family's remaining small leaves into "[Family] - other leaves". Keep
  "[Family] - unspecified" as its own leaf only if >= 0.5%, else pool it too.
- Min rendered cell area >= 0.3% of total. Soft cap ~200 static cells; the
  visual self-check (sec 6) is the real gate.

### 4. NAMED TOP CHANNELS (config/treemap_top_channel_placement.csv)
Place the CSV (top ~100 channels) at config/treemap_top_channel_placement.csv.
Columns used: channel_id, channel_name_title, view_count_4wk,
revised_primary_family, revised_primary_leaf, revised_primary_path,
non_primary_display_paths_to_retain_as_metadata, needs_manual_review.
Placement:
- For each channel in the CSV: OVERRIDE family_balanced -> a single allocation
  at (language_display(channel), revised_primary_family, revised_primary_leaf)
  with weight=1 (100% of its view_count_4wk). The leaf names in
  revised_primary_path (e.g. "[Entertainment] - unspecified", "Music of Asia",
  "Football", "Unmapped: pet") map directly to family + leaf.
- For all other ~199,900 channels: keep family_balanced fractional allocation.
- Static: show a named channel box ONLY where its leaf cell is large enough to
  carry labels (leaf area >= ~1.5%) and the channel's own area is visible; show
  top channels by view_count_4wk + "Other channels (N)"; smaller named channels
  fall into "Other channels". Hover: channel title, view_count_4wk,
  non_primary paths, needs_manual_review.
- Interactive: full depth; within each leaf, top-15 named/placed channels +
  "Other (N channels)".
Print: number of CSV channels placed; number labeled in the static figure.

### 5. CONSERVATION (re-verify after merge + hard-placement)
Each channel's full view_count_4wk is placed exactly once in aggregate (CSV
channels: one weight-1 row; others: family_balanced weights summing to 1).
Assert per-channel weight sum = 1 and total allocated = total view_count_4wk
within tolerance. Print "CONSERVATION: PASS".

### 6. OUTPUTS + SELF-CHECK
Write to outputs/youtube_topic_treemap_20260617_v3/:
- treemap_static_master_v3.png (>=2000x1200, >=200 DPI) + .svg
- treemap_interactive_explorer_v3.html (self-contained; branchvalues="total",
  maxdepth=2, packing="squarify", pad=2, sort=True; full depth incl. channels)
- treemap_static_cells_v3.csv (final cells + labels, for the caption)
- render_log_v3.txt (all printed metrics; so validation doesn't depend on chat)
Print: language map; ENGLISH IS ONE BLOCK: PASS; PALETTE line; leaf cell count;
channels placed/labeled; static cells; min cell area; pooled view share;
PACKING: squarify; CONSERVATION: PASS.
Then OPEN the static PNG, inspect, and print a legibility verdict (English one
block; no sliver-storm; family hues distinct; residuals gray; leaves/channels
readable). If any fails, raise thresholds and RE-RENDER before declaring done.

### Do NOT
- Leave any separate English block (en/eng/en-* must merge).
- Show review-cluster pseudo-codes as real languages.
- Use gray for Society or any real family; gray = residuals only.
- Use a rainbow / per-leaf arbitrary colors.
- Mutate raw language_code or raw view columns (add new columns).
- Use px.treemap(path=...). Keep prior artifacts (timestamped v3).

## V3.8 visual refinement patch (2026-07-15)

This patch supersedes the v3 color treatment, border treatment, label fallback,
static cell cap, and output path above. It does not change allocation, traffic,
language normalization, topic remapping, hard placement, or conservation.

### Design rules

- Preserve the fixed seven-family qualitative hue map. Family-only `(main)`
  cells blend 48% toward white, not gray, so they remain visibly associated
  with their family. Residual buckets use low-chroma warm neutrals (`#E3DDD4`
  and `#F0EDE7`) rather than mid-gray.
- Do not draw language container fills, language outlines, header rules, tile
  outlines, text outlines, or legend-swatch outlines. Reserve a white header
  strip for each language and use graduated whitespace to express hierarchy.
- For subdivided same-hue families, paint the parent container white so its
  narrow padding becomes a clean internal gutter. This separates topics without
  boxing every tile.
- Use mixed-case language headers with a quiet view total. Use one concise title,
  one subtitle, one horizontal legend, and one short source/definition line.
- After ordinary label fitting, check every displayed language for at least one
  rendered real-family or leaf label. If a language has none, place its dominant
  family name in the largest available terminal tile; controlled two-line family
  labels are permitted only for this fallback. Print
  `LANGUAGES WITH NO CATEGORY LABEL: 0` or fail the visual gate.
- Hard static cap: 120 cells. Unforced structural minimum remains 0.30% of total.
  Continue using squarify and do not loosen either gate to expose tiny families.
  Pooled categories remain available in the interactive explorer.

### Research basis

- Squarified layouts avoid the thin elongated rectangles produced by slice/dice
  treemaps: https://doi.org/10.2312/VisSym/VisSym00/033-042
- Rectangular area judgment is sensitive to aspect ratio, so the map is an
  overview rather than a device for fine numerical comparison:
  https://hci.stanford.edu/publications/2010/crowd-perception/heer-chi2010.pdf
- Family is nominal data and therefore uses qualitative hue, with lightness
  changes only to show related descendants:
  https://colorbrewer2.org/learnmore/schemes_full.html
- Non-data decoration and excessive labels are removed in line with empirical
  decluttering guidance: https://doi.org/10.1109/TVCG.2021.3068337

### V3.8 outputs

Write preserved artifacts to `outputs/youtube_topic_treemap_20260715_v3_8/`:

- `treemap_static_master_v3_8.png` and `.svg`
- `treemap_interactive_explorer_v3_8.html`
- `treemap_static_cells_v3_8.csv`
- `render_log_v3_8.txt`

## V3.9 smaller-language topic inclusion patch (2026-07-15)

This patch supersedes the v3.8 120-cell cap and its universal 0.30% structural
floor only for a tightly controlled set of readable named topics.

- Ordinary full family subdivision remains unchanged: family >= 1.0% of total,
  leaf >= 0.5% of total, and ordinary structural cell >= 0.30%.
- For a medium family representing 0.5%-1.0% of total views, expose at most one
  leading specific topic when that topic is >= 0.18% of total. Pool every other
  leaf in that family into the sibling residual cell.
- Do not apply the exception to topic names longer than 14 characters. This
  avoids adding a colored rectangle whose canonical topic name cannot fit.
- Every priority topic must render its complete name. Horizontal labels are
  preferred; 90-degree rotation is permitted for narrow cells. Ordinary labels
  retain the 6 pt floor; these short priority labels alone may use 5.5 pt.
- Static cell cap is 150. Print the priority-topic count, labeled count, minimum
  priority area, and both label floors. Fail if any priority topic is unlabeled.
- Preserve v3.8. Write the new artifacts to
  `outputs/youtube_topic_treemap_20260715_v3_9/` with `_v3_9` filenames.

Final local-data result: 138 static cells, 71 leaf cells, 82 labels, and all
9/9 priority topics labeled. Priority topics add Politics in Hindi; Food and
Movies in Vietnamese; Movies in Russian; Food and Movies in Korean; and Movies
in Turkish, Japanese, and Bengali. The minimum priority-topic area is 0.192%;
the ordinary unforced structural minimum remains 0.306%.

## V3.10 geometry-first topic inclusion patch (2026-07-15)

This patch supersedes v3.9's label-required priority tier.

- Labels are optional for priority topics; inclusion is controlled by geometry,
  fidelity, and the cell budget.
- Allow a medium family down to the ordinary 0.30% family floor to expose one
  leading specific topic representing at least 0.10% of total views.
- Require the pooled sibling to remain >= 0.30% so the split never folds
  unrelated views into the named topic.
- Require every priority-topic rectangle to have aspect ratio <= 5:1.
- Static cap: 152 cells. Preserve v3.9 and write `_v3_10` artifacts under
  `outputs/youtube_topic_treemap_20260715_v3_10/`.

Final local-data result: 152 static cells, 85 leaf cells, 79 labels, 16 priority
topics (10 labeled), minimum priority area 0.102%, maximum priority aspect ratio
4.85:1, and no pooled remainder folded into a labeled leaf.

## V3.11 160-cell comparison (2026-07-15)

- Preserve all v3.10 rules: ordinary thresholds, optional labels, pooled sibling
  >= 0.30%, no folded remainder, and priority aspect ratio <= 5:1.
- Permit at most two priority topics per medium family. First-topic floor is
  0.094%; second-topic floor is 0.090%; re-check the pooled sibling after both.
- Static cap: 160. Preserve v3.10 and write `_v3_11` artifacts under
  `outputs/youtube_topic_treemap_20260715_v3_11/`.

Final local-data result: 160 static cells, 93 leaf cells, 23 priority topics
(10 labeled), minimum priority area 0.094%, maximum priority aspect ratio 3.81:1,
and no pooled remainder folded. The denser version improves rather than worsens
the maximum priority-cell aspect ratio relative to v3.10 (4.85:1 -> 3.81:1).

## V3.12 180-cell family-coverage comparison (2026-07-15)

This patch supersedes v3.11's cell cap and family-pooling rule only where needed
to improve cross-language family coverage. It does not change traffic,
allocation weights, family values, topic values, language normalization, or the
interactive hierarchy.

- Static cap: exactly 180 cells. Preserve v3.11 and write `_v3_12` artifacts
  under `outputs/youtube_topic_treemap_20260715_v3_12/`.
- For every displayed language, rank family totals before pooling and recover
  exact family cells until at least four of the language's true top five
  families are visible. Subtract each recovered value exactly from that
  language's `Other (families)` pool; never inflate, reweight, or copy values.
- Keep recovered families terminal in the static figure. Their lower-level
  topics remain available in the interactive explorer; do not turn a small
  recovered family into a stack of topic slivers.
- After every language reaches four-of-five, rank the remaining fifth-family
  candidates by view mass. Spend the six-cell residual budget on the largest
  candidates only when the resulting `Other (families)` cell remains at least
  0.1% of the full figure.
- Labels remain optional for recovered families. Color plus the fixed family
  legend carries identity when a label does not fit.
- Recovered family cells and their modified residual siblings are explicit
  exceptions to the ordinary 0.30% area floor, but every such rectangle must
  have aspect ratio <= 5:1. Print per-language before/after top-five coverage,
  rescued view share, minimum rescued-family area, and maximum exception aspect
  ratio.

The first 180-cell probe recovered all five families in the six languages that
started at two-of-five. It failed the geometry gate because the remaining
Russian residual was 7.43:1. The final data-ranked rule leaves Russian at
four-of-five and assigns discretionary fifth-family cells to larger,
geometry-safe candidates.

Final local-data result: 180 static cells; all 13 displayed language blocks show
at least four of their top five families; 20 exact family rescues; six languages
show all five after the rescue; pooled-family share 2.903%; rescued-family share
3.814%; minimum rescued-family area 0.067%; maximum coverage-exception aspect
ratio 3.81:1; and `CONSERVATION: PASS`. Full-resolution visual inspection found
no repeated stack of thin slivers; the language blocks and family-color tiles
remain individually distinguishable.

## V3.13 up-to-200-cell comparison (2026-07-15)

This is a density comparison, not a requirement to reach 200. Preserve every
v3.12 fidelity and geometry rule and stop below the ceiling when the next cell
would fail.

- Preserve the v3.12 top-five family guarantee and its six discretionary
  fifth-family selections.
- After that coverage pass, consider additional exact families in descending
  view order only when each is at least 0.03% of total views. Keep the remaining
  pooled sibling at least 0.03%, or eliminate the pool exactly when every
  remaining family clears the detail floor.
- Keep these additional families terminal and labels optional. Do not add topic
  leaves or channels merely to approach the cap.
- Lay out every candidate with squarify. Reject optional detail families
  iteratively whenever they or the modified residual produce an aspect ratio
  above 5:1. Restore rejected values to `Other (families)` exactly.
- Hard ceiling: 200 cells. Write preserved `_v3_13` artifacts under
  `outputs/youtube_topic_treemap_20260715_v3_13/`.

The geometry pass rejected four optional candidates: Portuguese Knowledge,
Portuguese Society, Russian Music, and Russian Sports. The final figure stops at
193 cells rather than violating the cap's other constraints.

### Comparison with v3.12

| Metric | v3.12 | v3.13 |
|---|---:|---:|
| Static cells | 180 | 193 |
| Family cells | 74 | 87 |
| Leaf cells | 93 | 93 |
| Labeled cells | 76 | 74 |
| Languages showing >=4 of top 5 | 13/13 | 13/13 |
| Languages showing all top 5 | 8/13 | 10/13 |
| Pooled family share | 2.903% | 1.005% |
| Maximum family-exception aspect | 3.81:1 | 2.34:1 |

Both versions pass conservation, geometry, and full-resolution visual checks.
V3.13 reveals materially more family mass, including English Knowledge and
additional families in Arabic, Hindi, Korean, Turkish, Japanese, and Other
languages. It also produces more small color-only swatches and two fewer fitted
labels. Use v3.12 as the paper static master because it has the cleaner visual
hierarchy; retain v3.13 as the higher-detail comparison or appendix candidate.
