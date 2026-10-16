# Figure 5b (alternative Figure 5) — language rank vs. engagement and population

An alternative to the founding-year Figure 5 for *Attention on YouTube* (decision D2). It ranks the languages of
public YouTube (≥10k-subscriber channels) by weekly views and compares each with its number of speakers:

- **a:** speakers (log) vs weekly views (log), one point per language, with the line of views proportional to speakers.
- **b:** representation index (share of views ÷ share of speakers) by language rank.
- **c:** channels per million speakers vs weekly views per channel, with lines of equal views per speaker (0.1×, 1×, 10×).
  This splits a language's index into "more channels per speaker" and "more views per channel".

Descriptive only. The top 50 languages by weekly views that have a speaker figure are drawn (98.3% of all weekly views).

## Pipeline

| Step | Code | Output |
|---|---|---|
| 1. Speaker table | `../scripts/build_fig5b_speakers.py` (run with `python -I`) on `../data/cldr_release-48-2/` and `../data/fig5b_language_map.csv` | `../figs/fig5b/fig5b_speakers_cldr_release-48-2.csv` |
| 2. Aggregate | `19_fig5b_language_rank_databricks.py` | `dev_sean.matt.fig5b_langrank_{channels,languages,status,reconcile}_v1` |
| 3. Render locally | `../scripts/render_fig5b_local.py` (imports `fig5b_language_rank_plot.py`) | `../figs/fig5b/*.png`, `*.pdf`, CSV source data |

Only aggregates leave Databricks (`languages`, `status`, `reconcile`); `channels` stays in the workspace. The speaker
join and the pooling of labels into languages happen locally in the renderer. Re-draw with
`python scripts/render_fig5b_local.py --from-csv --version v1`. The ISO code `nan` (Min Nan) must not be read as a missing value: the
CSV readers use `keep_default_na=False`.

## Definitions

- **Universe:** `dev_sean.matt.fig1_discovery_channels_v4`, the 4,990,312 channels with ≥10k subscribers (as in Figures 3–5).
- **Weekly views:** the lifetime-view delta between the 2026-08-31 and 2026-10-05 collections of
  `dev_sean.default.yt_channel_stats_full` (latest collection per channel and date), divided by the elapsed days and
  multiplied by 7. Channels missing at the start (9,728) and channels whose lifetime total fell (209,749) are excluded,
  leaving 4,770,835 measurable channels (214,330 with zero views). This is the same SQL as Figure 5 (age), and the
  window is the 5-week window of Figures 3–5. It is not the one-week delta behind Figure 2.
- **Language label:** `language` of `dev_sean.matt.fig2_map_channels_v2` (ISO 639-3; Figure 2 v2 cascade: LID v3 silver,
  then dual-sample table, then the gap-fill run; see `README_fig2_map.md`). `und` = unknown. The label is the language
  of the channel's **written metadata** (title, description, tags), not the spoken language or the audience's language.
  **`und` holds 1.18% of measurable weekly views and 3.18% of measurable channels.** Every universe channel has a row in the
  language table (0 missing).
- **Analysis language:** labels are pooled by `data/fig5b_language_map.csv` (173 languages). Examples: `uzn` + `uzb`;
  `azj` + `aze`; `khk` + `mon`; `npi` + `nep`; `als` + `sqi`; `ekk` + `est`; `ron` + `rum`; `swh` + `swa`;
  `zsm` + `zlm` + `msa` (Malay; Indonesian `ind` stays separate); `fas` + `pes` + `prs` (Persian); `ory` + `odi`;
  `gaz` + `orm`; `lvs` + `lav`; `nob` + `nno` + `nor`; `fuv` + `ful` (Fula); `plt` + `mlg`; Arabic varieties
  (`ara`, `arb`, `apc`, `ary`, `arz`, …) into `ara`; and Serbian, Croatian, Bosnian, Montenegrin and `hbs`
  into one Serbo-Croatian group (the labels cannot be separated reliably). Mandarin (`cmn`) and Cantonese (`yue`)
  stay separate, as do Eastern Punjabi (`pan`, Gurmukhi) and Western Punjabi (`pnb`, `lah`), Northern (`kmr`) and
  Central (`ckb`) Kurdish, and Hindi and the Hindi-belt languages (`bho`, `mai`, `mag`, `hne`, `raj`, `awa`).
  Unmapped labels (401 of the 616 labels, 0.02% of all weekly views; `und` is separate) keep their own code and have no speaker figure.
- **Speakers:** Unicode CLDR, see below.
- **Representation index:** (language's share of weekly views) ÷ (language's share of speakers), both taken over the 172
  languages that have a speaker figure. 1 = views in proportion to speakers; 2 = twice as many views as its speakers imply.
  Equivalently, views per speaker ÷ the platform average of 137 weekly views per speaker. Colours: ≥2× orange,
  ≤0.5× blue, otherwise grey.
- **Other measures:** `channels_per_m` = measurable channels ÷ millions of speakers; `wv_per_channel` = mean weekly views per
  measurable channel. Index = (channels per million ÷ its platform average) × (views per channel ÷ its platform average).
- **Head concentration:** `top10_share` (the share of a language's views held by its ten largest channels) is in
  `fig5b_language_table_v1.csv`; it is 1.6% for English and ranges from 1.6% to 38% (Serbo-Croatian) among the top 50.

## Speaker-population data

- **Source:** Unicode CLDR `common/supplemental/supplementalData.xml`, `<territoryInfo>`, from the official
  `unicode-org/cldr` repository, **tag `release-48-2`** (latest non-beta tag on 2026-10-16; release 49 is beta only).
  `common/supplemental/supplementalMetadata.xml` from the same tag was used to check which ISO 639-3 labels CLDR treats
  as aliases of its codes (e.g. `cmn` → `zh`, `zsm` → `ms`, `uzn` → `uz`, `pnb` → `lah`). Both files are kept unmodified in
  `data/cldr_release-48-2/` (SHA-256 `cd2af39aef82fdbfba4d591c87548203350538ad2318486d104b3b38b8d62f1a` and `36e807ce72b15304dd993f132216f408351be1c4376edaa2fe9e9547e2efcac1`).
- **Method:** for each territory, speakers of a language = territory population × `populationPercent` ÷ 100, summed over
  territories (258 territories, 8.06 billion people). CLDR percentages are estimates of the population that uses the
  language, including second-language and literate users, and they overlap within a territory (Egypt: Arabic 94%, Egyptian
  Arabic 64%, English 35%). They are therefore **total speakers, L1 + L2**, and the language totals sum to 9.5 billion
  (more than the world population, because multilingual people count once per language).
- **Merging several CLDR codes into one analysis language** (column `combine` of the map):
  - `max` per territory for nested varieties and script variants (Arabic dialects; `zh` + `zh_Hant`; `yue` + `yue_Hans`;
    `lah` + `pa_Arab`), so no one is counted twice;
  - `sum`, capped at 100%, for distinct populations (Serbian + Croatian + Bosnian + Montenegrin).
- **Script/region variants** (`zh_Hant`, `uz_Cyrl`, `hi_Latn`, …) join the group of their base code unless another row claims
  that exact code: `pa_Arab` (Pakistan's Punjabi in Shahmukhi) goes to Western Punjabi, `az_Arab` (South Azerbaijani, Iran) to
  `azb`, not to Azerbaijani.
- **Coverage:** 172 of the 173 map rows have a CLDR figure (Mizo, `lus`, has none). The 172 languages with a figure hold
  **98.79% of all measurable weekly views** (99.97% of the views outside `und`; the 401 unmapped labels and Mizo hold the rest).
- **Uncertainty:** CLDR is a coarse, source-mixed estimate (national statistics, literacy and census data, often a single reference year)
  and is not a count of native speakers. Treat indices as order-of-magnitude, not point estimates. In particular,
  English (1.73 billion), French (333M), Swahili (194M) and Urdu (313M) are dominated by second-language users.

## Results (v1, 2026-10-16)

**Coverage and labels**
- 4,770,835 measurable channels, 1.3237 × 10¹² weekly views; 564 analysis languages have any views.
- `und`: 1.18% of views, 3.18% of measurable channels.
- 172 languages with a speaker figure hold 98.79% of views; the 50 plotted hold 98.3%.

**Does the share of attention track the speaker population?** Broadly yes, with large departures.
- On the log scale, weekly views and speakers correlate at r = 0.80 over the top 50 languages (Spearman 0.80) and r = 0.68
  over all 172 (Spearman 0.70). The log-log slope is 1.11 (top 50) and 0.96 (all): views grow about in proportion to speakers.
- But the index spans 0.02× to 3.4× among the top 50: 6 languages are ≥2×, 26 are ≤0.5×, 18 are in between.

**Largest languages by share of views vs share of speakers** (shares among the 172 languages with a figure; the views
share is of all views, so it differs slightly in the third decimal)

| Rank | Language | Share of views | Share of speakers | Index | Channels per M speakers | Views per channel |
|---|---|---|---|---|---|---|
| 1 | English | 48.3% | 18.2% | 2.7× | 1,132 | 327k |
| 2 | Spanish | 6.4% | 5.3% | 1.2× | 650 | 258k |
| 3 | Hindi | 6.1% | 6.1% | 1.0× | 514 | 271k |
| 4 | Indonesian | 5.5% | 1.9% | 2.9× | 1,100 | 365k |
| 5 | Arabic | 4.3% | 4.2% | 1.0× | 651 | 221k |
| 6 | Portuguese | 4.3% | 2.6% | 1.7× | 1,066 | 215k |
| 7 | Korean | 2.8% | 0.8% | 3.4× | 1,168 | 401k |
| 8 | Japanese | 2.7% | 1.2% | 2.2× | 827 | 364k |
| 9 | Russian | 2.3% | 2.1% | 1.1× | 880 | 175k |
| 10 | Turkish | 2.1% | 0.9% | 2.5× | 699 | 482k |

- The top 10 languages hold 84.9% of weekly views and 43.3% of speakers. English alone holds 48.3% of views and 18.2% of speakers.
- Six languages are over-represented (≥2×) among the top 50: Korean 3.4×, Indonesian 2.9×, English 2.7×, Turkish 2.5×, Japanese 2.2×
  and Thai 2.0× (2.006, on the threshold). Together they hold 62.5% of views and 23.6% of speakers.
- Under-represented (≤0.5×), largest first: Chinese (Mandarin) 0.08× (13.9% of speakers, 1.1% of views), French 0.24×,
  German 0.38×, Marathi 0.41×, Urdu 0.07×, Persian 0.17×, Dutch 0.16×. The lowest are Swahili 0.02×, Urdu 0.07×,
  Chinese 0.08×, Amharic 0.08×. The 26 under-represented languages hold 32.5% of speakers and 4.9% of views.
- Hindi (1.0×), Arabic (1.0×), Tamil (1.0×) and Spanish (1.2×) sit close to proportionality.
- Panel c: Korean, Indonesian, English, Turkish and Japanese have both more channels per speaker (about 700–1,170 per million speakers, vs 26–80 for
  Swahili, Chinese and Urdu) and higher views per channel (327k–482k). Thai is over-represented through channels alone: the most channels per speaker of any top-50 language
  (1,396 per million), but fewer views per channel (197k) than average. Chinese has few channels per speaker (35 per million) but ordinary
  views per channel (307k), so its gap is a gap of channels, not of audience per channel; Swahili and Urdu are low on both.
- Head concentration is low in the large languages (top-10 channels hold 1.6% of English views, 3–6% of Spanish, Hindi and
  Arabic), and higher in smaller ones (Serbo-Croatian 38%), so the small-language points rest on fewer channels.

## Caveats

- **Written-metadata language.** Labels come from channel text, not speech. A Hindi-speaking channel with English metadata counts as
  English; this probably inflates English and deflates languages whose creators write in English or Latin script.
- **English is a global second language.** Its CLDR figure (1.73 billion) is the largest, but such speakers watch in many languages. A
  2.7× English index is "views per CLDR speaker" and cannot be read as English-speaking users watching more.
- **Views are not viewers.** Weekly views count plays of channels in each language, wherever the viewers live, and are not a count of the
  speakers who watch. The index conflates reach (who has access and watches) with intensity (views per viewer) and with how many channels
  cater to the market. Diaspora viewing (Korean, Hindi, Arabic and Spanish speakers abroad) is included in views but not attributed
  to speakers by CLDR, which counts speakers by territory.
- **Access and platform reach.** YouTube is not available in mainland China, which holds most of CLDR's 1.3 billion Chinese speakers; this alone
  makes a large part of Chinese's low index structural. Persian and other languages with restricted access to YouTube are similarly affected.
- **Speaker counts are coarse.** CLDR figures mix first and second-language use and rest on territory-level estimates; they are not
  comparable across languages with different literacy or standard-language conventions (Arabic dialects, Hindi-belt languages, Serbo-Croatian).
- **Language-group choices.** Merging and splitting labels moves language totals (see the map). Mandarin and Cantonese are separate, while
  Arabic and Serbo-Croatian are pooled.
- **Subscriber threshold.** The universe is channels with ≥10k subscribers, so a language's channel counts depend on how many of its
  channels reach 10k.

## Figure 5b and the founding-year Figure 5

Figure 5 (age) shows a timing pattern in the data: channels founded 2023 or later are 24.5% of channels but 42.4% of weekly views. It rests only on
platform data and one field (`publishedAt`), so every number can be tied to the dataset. Figure 5b is a comparison with an external population and can say two
things the age figure cannot: which language communities hold more or less YouTube attention than their size would suggest, and whether
the pattern comes from the number of channels or from attention per channel. It cannot say why. The denominator is an approximate, partly
second-language speaker count by territory, the labels describe metadata, and the views are not viewers, so the index is a coarse comparison, not a measure
of how much a community watches. The age figure is the sturdier claim; Figure 5b is the more substantive question about whom the platform's attention serves.

## Caption (draft)

**Figure 5. Languages ranked by weekly views, compared with their speaker populations.** **a**, Speakers
(Unicode CLDR, first and second language) and weekly views for the 50 languages with the most weekly views; dashed line, views
proportional to speakers at the platform average. **b**, Each language's share of weekly views divided by its share of
speakers, by rank of weekly views (1× = proportional). **c**, Channels per million speakers and weekly views per channel; dashed and dotted
lines mark equal views per speaker (platform average, 0.1× and 10×). Orange, ≥2× the platform average; blue, ≤0.5×.
*Notes:* Universe: 4,990,312 channels with ≥10k subscribers on 2026-10-05; 4,770,835 with measurable views. Weekly views are the change in lifetime
views between 2026-08-31 and 2026-10-05, scaled to seven days. Language is that of the channel's written metadata (Figure 2 labels;
1.2% of weekly views are of unknown language). Speakers: CLDR release 48.2 territory population estimates, summed over territories;
172 languages with figures hold 98.8% of weekly views.

## Run log

| Date | Run | Databricks run_id | Outcome |
|---|---|---|---|
| 2026-10-16 | v1, first submit | 58331480445465 | Failed at start: notebook had been imported as a workspace file, not a notebook; deleted and re-imported (no tables written) |
| 2026-10-16 | v1 | 51060327728735 | Success on cluster `0818-185002-anfay17g`; wrote `dev_sean.matt.fig5b_langrank_{channels,languages,status,reconcile}_v1` |
