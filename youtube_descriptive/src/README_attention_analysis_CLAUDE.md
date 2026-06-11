# Attention on YouTube  -  manuscript analysis notebook (Claude work product)

**Author:** Claude (Anthropic Opus 4.8). This is the *Claude* deliverable; a parallel notebook from
another model exists for comparison. Every artifact this code writes is tagged `author = "claude"` and
the filename stem `*_CLAUDE`, so the two are never confused.

## What this is

A Databricks-compatible notebook that produces the **core results, robustness checks, results tables, and
publication-grade figures** for Paper 1, *"Attention on YouTube"* (Hindman, Norton, Shapiro, Tucker).

| File | Role |
|---|---|
| `src/10_attention_manuscript_analysis_CLAUDE.py` | The notebook in Databricks source format (canonical; diff-friendly). |
| `notebooks/attention_manuscript_analysis_CLAUDE.ipynb` | The same notebook as a Jupyter `.ipynb` (generated; imports natively into Databricks). |
| `scripts/db_source_to_ipynb.py` | Zero-dependency converter; regenerates the `.ipynb` from the `.py`. |

Regenerate the notebook after editing the `.py`:

```bash
python scripts/db_source_to_ipynb.py \
  src/10_attention_manuscript_analysis_CLAUDE.py \
  notebooks/attention_manuscript_analysis_CLAUDE.ipynb
```

## Data-handling contract (enforced in code)

All heavy computation runs **inside Databricks**. Only *aggregates* (small summary tables) and rendered
figures leave the workspace. The export choke-point (`export_table`) refuses any frame above
`max_export_rows`, suppresses **only small positive** count cells (`0 < n < min_cell_count`, default 5;
NaN/zero structural rows like "not in panel" are kept), names every CSV `*_CLAUDE.csv`, **and persists
each aggregate as a Delta table** (`dev_sean.matt.claude_yt_attention_*`)  -  the manifest records a Delta
table only when the write actually succeeded. Each figure computes its aggregate, exports it through that
choke-point, then renders from it; no full channel-/video-level frame is collected to the driver
(concentration/threshold/Lorenz use Spark-side log-binning, traffic blocks via a value-binned broadcast
map). **One documented carve-out:** the Fig 2 treemap source lists named public head channels (the rest
pooled)  -  set `treemap_anonymize_labels=true` to hash those names.

**Production-run semantics.** Figure/robustness steps are failure-isolated (a missing input degrades to a
logged `_fail`), but a `core`/`full` run **raises at the end** if any required step failed (unless
`fail_on_missing_outputs=false`), so a manuscript run can't silently "succeed" with missing outputs; the
failures are listed in the manifest (`failed_steps`). The language `run_id` is validated against the
table's actual run_ids and falls back to the latest present one (with a warning) rather than collapsing
all labels to `und`. Video timing uses an explicit YouTube publish date only (never generic `created_at`).
An empty `channel_master` (bad smoke sample / empty anchor) reads as a readiness warning, not a
divide-by-zero crash.

## Safe by default

The notebook starts in **`manifest_only`** mode  -  it prints configuration and runs **no table scans**.
Set `execution_mode` to `smoke` (limited to `smoke_channel_limit` channels), or `core`/`full` (which
require `confirm_expensive=true`).

## Method, in one paragraph

"Attention" = **weekly views**, the **elapsed-day-normalized** change in a channel's *lifetime* views
between two snapshots: `(views_t - views_{t-k}) / k x 7`. The preferred panel is
**`dev_sean.default.yt_channel_stats_full`** (the current Sunday TOO snapshot job: `canonical_id`,
`subscriber_count`, `total_view_count`, `collected_date`); `yt_sl_channels_metrics` is *not* the weekly
job (it tops out at 2026-05-01). Universe = channels >= 10,000 subscribers ("Top of the Ocean"). Channels
join to consensus language labels (`dev_sean.matt.yt_lid_v3_channels`, `consensus_for_rollup_label`, gated
on valid-segment count) and to the native YouTube category (`backfill_channels.topic_categories`, primary
array element, normalized from the Wikipedia topic form; missing -> `uncategorized`, bounded in R7). The
analytical core is **recomputed from silver**; `dev_sean.diagnostics`/`validation` are cross-checks only.

## Reviewer-facing correctness guards

- **Anchor + prior completeness (strict in core/full).** Both the current and the prior snapshot are chosen
  from partitions whose row count is >= `anchor_min_fraction_of_max_partition` (default 0.90) x the largest
  partition. In `core`/`full`, a partial **current** anchor (from `anchor_selection_mode=latest` or a manual
  `snapshot_current_date`) **refuses to run** unless `allow_incomplete_anchor=true`, and a partial **prior**
  refuses unless `allow_incomplete_prior=true`; `smoke` proceeds with warnings. Per-partition counts go to
  `attention_anchor_snapshot_coverage_CLAUDE.csv`, and the manifest records the completeness of **both**
  chosen partitions (`snapshot_current_completeness`, `snapshot_prior_completeness`) regardless of how they
  were selected.
- **Concentration is labelled by denominator.** ED Lorenz/Gini and the robustness top-1% metrics are over
  **positive-attention (active) channels** (`weekly_views>0`), and are labelled/keyed as such
  (`gini_weekly_views_positive_attention_channels`, `top1pct_view_share_positive_attention`)  -  not implied to
  be over all observed channels.
- **Duplicate snapshot rows resolved by recency.** When the panel exposes an ingest/collected timestamp,
  duplicate (channel, date) rows are de-duplicated by that timestamp (then max views); the column used is
  recorded in `snapshot_dedupe_recency_column`.
- **Weeklyization.** Deltas are divided by the *actual* elapsed days and scaled to 7 (a weekly *rate*),
  never a raw off-cadence total; `snapshot_elapsed_days` is logged with a warning when != 7.
- **Missing prior = unmeasured, not zero.** Under every negative-delta policy (incl. `floor_zero`), channels
  absent from the prior snapshot get **null** weekly views, not a measured 0  -  so they don't bias
  concentration/threshold/ranking. They're counted in `attention_measurability_status_CLAUDE.csv`.
- **Genuinely bounded, deterministic smoke.** `smoke` samples `smoke_channel_limit` anchor-partition channel
  IDs (ordered by `channel_id`, so reproducible) **before** the prior-snapshot read and the delta join  -  a
  cheap bounded test. (The current anchor partition is scanned once to obtain the ID list.)
- **Table vs figure controls.** A main-figure cell **always** exports its source-data table when it runs
  (source exports are mandatory for reproducibility). `make_figures` controls *rendering*  -  when false,
  `save_fig` no-ops so no plots are written. `make_source_tables` lets the main-figure cells **run at all
  when `make_figures=false`**, giving a reviewer "tables-only" pass (`make_figures=false`,
  `make_source_tables=true`); `run_robustness` (default true) similarly controls the ED + robustness tables.
  (Setting `make_source_tables=false` while `make_figures=true` does not suppress a rendered figure's own
  source table  -  those are bundled.)
- **Treemap governance.** The treemap source export (`fig2_treemap_cells_CLAUDE.csv`) intentionally includes
  **individual labelled high-attention channel names and their weekly views** (plus pooled "other" cells).
  This is by manuscript design (the map labels public head channels); it is the one near-row-level export
  and is *not* a bulk channel dump  -  everything below the labeling threshold is pooled.
- **Threshold counts use the full observed universe.** Channel counts/shares in Table 1 / Fig 4 are computed
  over **all** observed >=10k channels (`channel_master`, inactive coalesced to 0 views), not the
  positive-weekly-views subset  -  so the =10k row is exactly 100% and "channels" means all observed channels.
  Shares are labelled "within the observed >=10k universe"; the =10k bar is dropped; the 1k row is a status
  placeholder.
- **Observed/design/bounded.** The tier panel draws the observed bar and marks design/bounded as hatched
  **"unknown (not in panel)"**  -  never zero.
- **Approximate blocks, audited.** Traffic blocks are *approximate* ~5% blocks (value-binned broadcast map,
  no global sort); the **actual** view share per block is exported to `fig4_block_actual_view_share_CLAUDE.csv`.
- **Platform-mass-conserving map.** Figure 2 adds an "other languages" top-level block (everything beyond the
  top languages, incl. undetermined) and an "other categories" cell per language, so the treemap represents
  the whole observed platform; `fig2_other_languages_view_share` is recorded.
- **Proxy-failure completeness.** Figure 3 adds an *inactive/unmeasured share* panel (zero/null weekly views
  by subscriber bin) so the missing mass the dispersion panels exclude is shown, not hidden.
- **Honest format measure.** Figure 6's format panel is labelled *cumulative views on recent uploads* (latest
  snapshot, videos published in the lookback)  -  **not** weekly attention and not all attention  -  pending
  video-level deltas.
- **Scale.** Concentration/Lorenz and the robustness top-1% shares use Spark-side log-binning (no global
  sorts), so the core scales toward the eventual ~100M+ universe.
- **Dry runs don't mutate.** With `write_outputs=false`, nothing is written to disk  -  CSVs, Delta tables,
  **and figures** are all suppressed.
- **Production intensity counts non-uploaders.** Figure 6 panel b fills zero-upload channels with 0 uploads
  (cross-join of ranked channels x formats), so it's uploads-per-channel-in-block, not per-uploader.
- **Honest format/lookback labels.** Figure 6's format panel and ED Fig 6 are labelled as *cumulative
  (lifetime) views on recent uploads*  -  not weekly attention and not a within-window recent-view capture
  (which needs video-level deltas).
- **Robustness schema.** `robustness_summary_CLAUDE.csv` is long-form (`check`/`variant`/`metric_name`/
  `metric_value`) since the rows hold different quantities (e.g. concentration vs language coverage).
- **Entry/exit.** Channels present now but absent from the prior snapshot get null weekly views, summarised
  in `attention_measurability_status_CLAUDE.csv`.
- **Optional multi-window cascade.** `attention_fallback_windows_days` (default off, e.g. `"14,28"`) lets a
  channel without a primary 7-day measure be filled from a longer *complete*-prior window, recording the
  window used per channel (`attention_window_used_days`, `attention_measure_status`) and summarising the mix
  in `attention_window_usage_summary_CLAUDE.csv` - so mixed-window measurement on the immature panel is
  explicit, never silent. Default off keeps the strict single-7-day path.
- **Schema-drift inventory.** Section 3.0 writes `metadata_table_inventory_CLAUDE.csv` (existence + column
  preview of every key table) before the expensive work, so drift/missing dependencies surface as one table.
- **Wider robustness / cross-checks.** Beyond R1/R3/R4/R5/R7 and ED concentration/lookback: **R8** correlates
  our annualised weekly attention against the diagnostics layer's `views_past_year` (a cross-check, not
  ground truth), and **R9** recomputes the format split at 60/configured/180s Shorts cutoffs.
- **Interpretation checklist + open items.** Sections 7-8 give a pre-manuscript checklist (window mix,
  composition coverage, threshold framing, concentration denominator) and the live open project decisions
  (first complete Sunday pair, 1k estimator, category coverage, creation date, governance).

## Figures (Nature/Science spec)

90 mm / 180 mm widths, 600-DPI PNG + vector PDF, sans-serif 5-7 pt, 8-pt bold panel letters, Okabe-Ito
colourblind-safe categorical palette, cividis/viridis sequential maps.

* **Fig 1** Discovery saturates (per-batch new-channel yield; high-subscriber head saturation  -  a
  subscriber-threshold proxy, not attention-weighted).
* **Fig 2** A map of public YouTube (full-width treemap language->category->channel; composition across 5%
  traffic blocks; per-language composition + Jensen-Shannon "one platform or many" statistic).
* **Fig 3** Subscribers are an imprecise proxy (weekly-views-vs-subscribers dispersion; views/subscriber).
* **Fig 4** Thresholds capture unequal shares (cumulative capture; threshold bars; observed/design/bounded)
   -  also writes manuscript **Table 1**.
* **Fig 5** Age structure **and** the JNS (5/22) alternative: language rank vs engagement & speaker population.
* **Fig 6** Production and attention diverge by format (supply-demand; production intensity; format share).
* **Extended Data + robustness:** Lorenz/Gini concentration, lookback calibration, and a robustness battery
  (negative-delta policy, suspicion-flag exclusion, language-confidence gate, weekly-vs-lifetime attention,
  category-missingness bounds), plus a cross-check of our Spearman against `too_run_summary.spearman_rho`.

## Running it

Import `src/10_attention_manuscript_analysis_CLAUDE.py` (or the `.ipynb`) into Databricks and attach to
`research-compute`. All inputs are widgets. It opens in `execution_mode=manifest_only` (no scans). For a
fast test set `execution_mode=smoke` (samples `smoke_channel_limit` channels); for the real run set
`execution_mode=core` (or `full`) **and** `confirm_expensive=true`. Outputs land in `figs/` and a dated
folder under `export_root` (each aggregate also persisted as a Delta table), alongside
`run_manifest_CLAUDE.json` (parameters, snapshot dates + `snapshot_elapsed_days`, coverage, headline
numbers, output checksums, and degradation warnings).

## Data dependencies  -  validated 2026-05-29 (cheap metadata/sampled queries)

A small SQL validation pass surfaced real data-readiness gaps. The notebook degrades gracefully around
each (logged to the manifest `warnings` + `data_readiness_2026_05_29`):

1. **Weekly panel is just starting to accumulate.** The default panel is now
   `dev_sean.default.yt_channel_stats_full` (the Sunday TOO job), which currently has only 2 adjacent-day
   partitions (05-27/05-28); `yt_sl_channels_metrics` tops out at 05-01 and is *not* the weekly job. Deltas
   are elapsed-day-normalized so the off-cadence gap is reported as a weekly *rate*, but headline numbers
   should await a true Sunday-to-Sunday pair. Panel source/key are widgets (`channel_panel_fqtn`,
   `channel_key_column`) supporting both schemas.
2. **Category genre-axis is not yet populated.** `ai_label`/`all_labels` in `yt_sl_videos` are empty; the
   native YouTube topics live in **`dev_sean.default.backfill_channels.topic_categories`** (array)  -  now the
   first auto-detect candidate, taking the **primary (first) element** normalized from the Wikipedia topic
   form  -  with `subsample_items.topic_top_k_json` as a secondary. Many rows are still `pending`. R7 bounds the
   uncategorized mass meanwhile.
3. **No platform-wide channel creation date.** Bronze `created_at` is ingest time and `published_at` is
   video publish; only `updated_sb_top50k.general.created_at` carries founding date (~top 50k). Fig 5a (age)
   self-skips and Fig 5 uses the JNS language-rank panel until a creation date is wired in.
4. **Threshold-collection strata** (`dev_sean.threshold_yt_1k` / `_5k`)  -  to populate the design-based /
   bounded tiers in Figure 4 (the panel currently covers the observed >=10k tier and labels the rest).
5. **Speaker-population table**  -  a small approximate lookup is bundled for the JNS language figure; swap in
   an authoritative source if that figure is promoted to the main text.
