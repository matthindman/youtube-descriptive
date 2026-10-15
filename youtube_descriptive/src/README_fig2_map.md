# Figure 2 — a map of public YouTube

Figure 2 of *Attention on YouTube* maps where weekly attention goes on public YouTube:

- **a:** a treemap of weekly views, nested language → category → channel.
- **b:** the category mix across 5% traffic blocks for the whole platform.
- **c:** the same within the three largest languages.

The spec is the Fig 2 row of the "Attention on YouTube — Analysis Plan & Data Audit" doc.

## Pipeline

| Step | Code | Output |
|---|---|---|
| 1. Channel topics | `11_fetch_channel_topics_databricks.py` | `dev_sean.matt.yt_channel_topics` |
| 1b. Language gap-fill (v2) | `01_language_openlid_v3_databricks.py` on the unlabelled channels | `dev_sean.matt.fig2_und_lid_v1_channel_language` |
| 2. Classify + aggregate | `12_fig2_platform_map_databricks.py` | `dev_sean.matt.fig2_map_{channels,cells,lang_cat,blocks,coverage}_<version>` |
| 3. Render locally | `../scripts/render_fig2_local.py` | `../figs/fig2/*.png`, `*.pdf`, and CSV source data |

`fig2_map_plot.py` draws the figure, and the local renderer imports it. The fetcher takes the same
optional `api_key` job parameter as the Figure 1 fetchers.

## Definitions

- **Universe and weekly views:**
  - Universe: the Figure 1 channel table (`fig1_discovery_channels_v4`), i.e. the 4,990,312 channels
    with ≥10k subscribers on 2026-10-05.
  - Weekly views are the 2026-09-28 → 2026-10-05 delta. Channels without a prior snapshot count as 0.
- **Language:**
  - Source: `channel_language` (ISO 639-3), taking the first non-`und` label from, in order: the LID v3
    silver table; `yt_dual_sample_20260717_v1_channel_language_current` (LID v3 + DeepSeek, dual
    sample); `fig2_und_lid_v1_channel_language` (the v2 gap-fill run: LID v3, then DeepSeek flash for unresolved cases, see below). Set by the
    `language_table` and `fallback_language_tables` widgets of step 2.
  - `und` in every source, or no row, is shown as "Unknown language".
  - The map shows the 8 largest languages by weekly views; the rest pool into "Other languages".
- **Category:** YouTube's own topic hierarchy, read from `topicDetails.topicCategories`.
  - Every topic maps to one of YouTube's seven parent topics: Music, Gaming, Entertainment, Lifestyle,
    Sports, Society, Knowledge.
  - A channel takes the parent of its first specific topic (e.g. Pop music → Music, Film →
    Entertainment).
  - A parent-level topic (e.g. "Lifestyle (sociology)") counts only when the channel has no specific
    topic.
  - Channels with no topics are "Unclassified".
- **Traffic blocks:**
  - Channels are ranked by weekly views and cut into 20 blocks of equal view mass, using exact running
    sums (block 1 holds the top 5% of views).
  - Panel c re-ranks channels within each language.
- **Labels:**
  - A channel gets its own outlined cell when it holds ≥0.02% of all weekly views (`label_min_share`).
    Its name is printed only where it fits.
  - All other channels pool into one cell per language × category.
  - `fig2_cells` is therefore the one export that names channels: these are public head channels only,
    by design.
- **One platform or many:**
  - The Jensen–Shannon divergence (JSD, in bits) is computed between the top languages' category mixes,
    and compared with the view-weighted JSD of each traffic block's mix from its own language's mix.
  - Unclassified views are excluded from this comparison.

## Results (v2, 2026-10-16)

v2 differs from v1 only in the language source. Weekly views, universe, topics and categories are unchanged.

**Label coverage**
- Category covers 98.1% of weekly views (unchanged; Unclassified is 1.9%).
- Language covers 98.8% of weekly views, up from 87.0% in v1. "Unknown language" holds 1.2%, down
  from 13.0%.
- Where the v1 gap was filled (the 280,752 ≥10k channels missing from the June crawl):
  - 173,345 were already labelled in the dual-sample table (171,539 with a real label);
  - 107,407 had no label anywhere and were run through LID v3 for this figure: 90,938 labelled
    (89,693 consensus, 1,245 ISO/script-variant agreement); 16,469 were left `und` (models disagree or
    too little text);
  - for 9,335 of those (7,183 model disagreement, 2,152 unresolved high-risk tail) DeepSeek
    `deepseek-v4-flash` classified the channel metadata (notebook `03_language_llm_panel_databricks.py`,
    single model, run_id `fig2_und_20261014_dsfb`, ~$1.5–3 at list price): 8,971 labelled, the rest
    left `und` (37 insufficient text, 264 low confidence, 62 high-risk tail labels, 1 malformed
    confidence); 7,498 gap-fill channels remain `und`, i.e. ~6,700 with no usable text, 419
    cluster-level agreements and the rejects above.
- The new labels also fill some channels the June crawl labelled `und`, which is why Unknown falls
  further than the 94% share attributed to missing channels would predict.

**Panel a: where weekly views go**

| Language | Share of views | | Category | Share of views |
|---|---|---|---|---|
| English | 49.4% | | Entertainment | 32.6% |
| Other languages | 17.9% | | Lifestyle | 32.6% |
| Spanish | 6.3% | | Music | 13.0% |
| Hindi | 6.1% | | Society | 8.2% |
| Indonesian | 5.4% | | Gaming | 7.2% |
| Portuguese | 4.2% | | Sports | 3.2% |
| Arabic | 4.1% | | Unclassified | 1.9% |
| Korean | 2.8% | | Knowledge | 1.3% |
| Japanese | 2.6% | | | |
| Unknown | 1.2% | | | |

- The language ranking is unchanged from v1 except that Unknown drops from third to last; English
  gains most (43.1% → 49.4%).
- No single channel dominates:
  - only 127 channels hold ≥0.02% of weekly views each, together 5.1% of all views;
  - only 7 channels hold ≥0.1%.

**Panel b: the category mix across traffic blocks**
- Block 1 (123 channels) is 44.2% Entertainment and 37.3% Lifestyle, but only 8.5% Music.
- Block 20 (4,400,383 channels) is 22.4% Music and 14.8% Society.

**Panel c: one platform or many**
- Category mixes differ more between languages than across traffic blocks within a language:
  - between languages: mean JSD 0.038 bits over the 28 pairs of the top 8 languages;
  - within a language: mean JSD 0.016 bits for English, Spanish and Hindi.
- The closest pair is English–Spanish (0.007 bits), and the furthest is Hindi–Japanese (0.143 bits).

v1 (language from silver only) had Unknown at 13.0%, English 43.1%, between-language JSD 0.039 and
within-language 0.019; its outputs remain as `fig2_map_*_v1`.

## Run log

| Date | Run | Databricks run_id | Outcome |
|---|---|---|---|
| 2026-10-14 | Channel topics | 1026080439438038 | 4,983,682 ok (4,703,168 with topics), 6,630 not found (~100k quota units) |
| 2026-10-14 | Fig 2 v1 | 397226955466533 | Success; label threshold 0.1% gave only 7 named channels |
| 2026-10-14 | Fig 2 v1 (overwrite) | 710882764670948 | Success; label threshold lowered to 0.02% (127 channels) |
| 2026-10-15 | LID v3 gap-fill (`run_id=fig2_und_20261014`) | 980202936593843 | Success; 107,407 channels, 1,309,535 valid segments, about 3 h on `sean-research-medium-compute`; outputs `fig2_und_lid_v1_lid_*` |
| 2026-10-15 | Fig 2 v2 (LID + dual-sample only) | n/a | Success; Unknown language 13.0% → 1.7% (superseded below) |
| 2026-10-16 | DeepSeek flash fallback requests | 735252650637058 | 9,335 requests built (`fig2_und_lid_v1_dsfb_llm_requests`), nothing sent |
| 2026-10-16 | DeepSeek flash submit | 1044533639912791 | Success; 9,298 classified, 37 insufficient text, no parse errors |
| 2026-10-16 | DeepSeek import | 796566519464698 | Success after two failed attempts (runs 139073521774852, 1050399855385108; array-index bugs in `03_…`, fixed with `try_element_at`) |
| 2026-10-16 | Fig 2 v2 (overwrite, with DeepSeek labels) | 474846533421417 | Success; Unknown language 1.2%, language coverage 98.8% |

## Rerunning

1. Optional: run `11_…` to fetch topics for channels not yet in `yt_channel_topics`. It is resumable.
2. Run `12_…` with a new `version` (or `overwrite=true`). Language labels come from the three tables
   described under Definitions; add new label tables to `fallback_language_tables`.
3. Render locally:

   ```bash
   python scripts/render_fig2_local.py --profile accelerator-db --warehouse <id> --version <v>
   ```

   Add `--from-csv` to redraw from the saved CSVs without querying Databricks.

## Known limits

- **Lifestyle is partly a catch-all.**
  - 35% of Lifestyle's views come from channels whose only topic is the generic "Lifestyle
    (sociology)".
  - The same holds for 22% of Entertainment's views and 13% of Society's.
- **Unknown language is now 1.2% of views.** It is mostly channels with no usable text, the 419
  cluster-level agreements (not routed to DeepSeek) and low-confidence or high-risk-tail DeepSeek
  answers (7,498 gap-fill channels), plus residual `und` in the other sources.
- **DeepSeek-labelled channels.** 8,971 gap-fill labels (and 20,424 in the dual-sample table) come from
  an LLM reading channel metadata, not from LID consensus; we kept only `high`/`medium` confidence,
  non-high-risk-tail answers, using the taxonomy-normalized base ISO. They carry
  `language_label_source='deepseek_flash_fallback'` and `llm_confidence`.
- **Gap-fill inputs.** The new run reuses `01_language_openlid_v3_databricks.py`
  unchanged, on scratch inputs `fig2_und_lid_v1_{targets,channels,videos}` built from
  `yt_channel_descriptions` and the 10 most recent videos in `yt_channel_videos`.
- **Topic channels are kept.** " - Topic" channels (0.8% of views) are real attention to auto-generated
  music uploads, so the map includes them; Figure 1's active variants drop them.
- **Panel c uses the three largest named languages.** Other languages and Unknown are not shown there.
