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
  - Source: `channel_language` from the LID v3 silver table (ISO 639-3).
  - `und`, or no row, is shown as "Unknown language".
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

## Results (v1, 2026-10-14)

**Label coverage**
- YouTube returned topics for 4,703,168 of 4,990,312 channels; 6,630 were not found. Category covers
  98.1% of weekly views, up from 93.1% with the June `channel_category` snapshot.
- Language covers 87.0% of weekly views. "Unknown language" (`und` or not in the LID v3 crawl)
  holds 13.0%. Of that, 94% comes from 280,752 channels missing from the 2026-06-23 LID v3 crawl. The
  rest comes from 150,696 crawled channels labelled `und`.

**Panel a: where weekly views go**

| Language | Share of views | | Category | Share of views |
|---|---|---|---|---|
| English | 43.1% | | Entertainment | 32.6% |
| Other languages | 15.8% | | Lifestyle | 32.6% |
| Unknown | 13.0% | | Music | 13.0% |
| Spanish | 5.7% | | Society | 8.2% |
| Hindi | 5.3% | | Gaming | 7.2% |
| Indonesian | 4.8% | | Sports | 3.2% |
| Portuguese | 3.8% | | Unclassified | 1.6% |
| Arabic | 3.7% | | Knowledge | 1.3% |
| Korean | 2.5% | | | |
| Japanese | 2.3% | | | |

- No single channel dominates:
  - only 127 channels hold ≥0.02% of weekly views each, together 5.1% of all views;
  - only 7 channels hold ≥0.1%.

**Panel b: the category mix across traffic blocks**
- Block 1 (123 channels) is 44.2% Entertainment and 37.3% Lifestyle, but only 8.5% Music.
- Block 20 (4,400,383 channels) is 22.4% Music and 14.8% Society.

**Panel c: one platform or many**
- Category mixes differ more between languages than across traffic blocks within a language:
  - between languages: mean JSD 0.039 bits over the 28 pairs of the top 8 languages;
  - within a language: mean JSD 0.019 bits for English, Spanish and Hindi.
- The closest pair is English–Spanish (0.009 bits), and the furthest is Hindi–Japanese (0.145 bits).

## Run log

| Date | Run | Databricks run_id | Outcome |
|---|---|---|---|
| 2026-10-14 | Channel topics | 1026080439438038 | 4,983,682 ok (4,703,168 with topics), 6,630 not found (~100k quota units) |
| 2026-10-14 | Fig 2 v1 | 397226955466533 | Success; label threshold 0.1% gave only 7 named channels |
| 2026-10-14 | Fig 2 v1 (overwrite) | 710882764670948 | Success; label threshold lowered to 0.02% (127 channels) |

## Rerunning

1. Optional: run `11_…` to fetch topics for channels not yet in `yt_channel_topics`. It is resumable.
2. Run `12_…` with a new `version` (or `overwrite=true`).
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
- **Unknown language is large (13% of views).** Most of it is channels the June LID v3 crawl never saw.
  Labelling them needs a LID v3 run on those 280,752 channels.
- **Topic channels are kept.** " - Topic" channels (0.8% of views) are real attention to auto-generated
  music uploads, so the map includes them; Figure 1's active variants drop them.
- **Panel c uses the three largest named languages.** Other languages and Unknown are not shown there.
