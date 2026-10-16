# Figure 5b (alternative Figure 5) — language rank vs. engagement and population

An alternative to the founding-year Figure 5 for *Attention on YouTube* (decision D2). It ranks the languages of
public YouTube (≥10k-subscriber channels) by weekly views and compares each with its number of speakers:

- **a:** speakers (log) vs weekly views (log), one point per language (size = measurable channels), with the line of views proportional to speakers.
- **b:** paired dots, share of weekly views vs share of speakers for the 18 largest languages. It needs no anchor.
- **c:** channels per million speakers vs weekly views per channel (size = weekly views), with lines of equal views per speaker (0.1×, 1×, 10×).
  This splits a language's index into "more channels per speaker" and "more views per channel".

Point colour (a, b line, c) is the continuous log of the representation index, in a teal–grey–purple scale chosen not to reuse the
blue/orange of Figures 3–5. The legend strip under the panels gives colour, point size and the hollow Chinese marker.

Descriptive only. The top 50 languages by weekly views that have a speaker figure are drawn (98.3% of all weekly views).

## Pipeline

| Step | Code | Output |
|---|---|---|
| 1. Speaker table | `../scripts/build_fig5b_speakers.py` (run with `python -I`) on `../data/cldr_release-48-2/` and `../data/fig5b_language_map.csv` | `../figs/fig5b/fig5b_speakers_cldr_release-48-2.csv` |
| 2. Aggregate | `19_fig5b_language_rank_databricks.py` | `dev_sean.matt.fig5b_langrank_{channels,languages,status,reconcile}_v1` |
| 3. Render locally | `../scripts/render_fig5b_local.py` (imports `fig5b_language_rank_plot.py`) | `../figs/fig5b/*.png`, `*.pdf`, CSV source data |

Sensitivities (China, Hindi+Urdu, Malay+Indonesian, anchor): `python scripts/fig5b_sensitivities.py`, which reads the committed
CLDR file and `figs/fig5b/fig5b_language_table_v1.csv` and writes `figs/fig5b/fig5b_sensitivities_v1.csv`.

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
  Equivalently, views per speaker ÷ the platform average of 137 weekly views per speaker. The "1×" anchor depends on the
  speaker total (9.52 billion, overlapping L1 + L2; see Sensitivities), so the figure uses a continuous colour by log index and shows both shares in panel b.
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

**Facts that need no speaker denominator**
- The top 10 languages hold 84.9% of weekly views. English alone holds 48.3%, followed by Spanish 6.4%, Hindi 6.1%, Indonesian 5.5% and Arabic 4.3%.
- Among the 172 languages with a speaker figure, the top 10 hold 43.3% of speakers.
- On the log scale, weekly views and speakers correlate at r = 0.80 over the top 50 languages (Spearman 0.80; log-log slope 1.11) and
  r = 0.68 over all 172 (Spearman 0.70; slope 0.96): views grow about in proportion to speakers.

**Departures from proportionality** (index = share of views ÷ share of speakers; the level of the index depends on the anchor, see
"Sensitivities", but the ordering and the shape of the spread do not)

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

- The index spans 0.02× to 3.4× among the top 50. The highest are Korean 3.4×, Indonesian 2.9×, English 2.7×, Turkish 2.5×,
  Japanese 2.2× and Thai 2.0× (2.006). The lowest are Swahili 0.02×, Urdu 0.07×, Chinese (Mandarin) 0.08×, Amharic 0.08×,
  Somali 0.15× and Dutch 0.16×. French is 0.24× and German 0.38×. Hindi, Arabic, Tamil (1.0×) and Spanish (1.2×) sit close to proportionality.
  (The figure no longer colours ≥2× and ≤0.5× classes: Thai sits on the 2× line and the 1× anchor is arbitrary, see below.)
- Chinese has 13.9% of speakers but 1.1% of views; see the China sensitivity.
- Panel c: Korean, Indonesian, English, Turkish and Japanese have both more channels per speaker (about 700–1,170 per million speakers, vs
  26–80 for Swahili, Chinese and Urdu) and higher views per channel (327k–482k). Thai is high through channels alone: the most channels per
  speaker of any top-50 language (1,396 per million) but fewer views per channel (197k) than average. Chinese has few channels per
  speaker (35 per million) but ordinary views per channel (307k); Swahili and Urdu are low on both.
- Head concentration is low in the large languages (the top-10 channels hold 1.6% of English views, 3–6% of Spanish, Hindi and Arabic) and
  higher in smaller ones (Serbo-Croatian 38%), so the small-language points rest on fewer channels.

## Sensitivities

Recomputed from the committed CLDR file and language table by `scripts/fig5b_sensitivities.py` (output `figs/fig5b/fig5b_sensitivities_v1.csv`).

- **Anchor (denominator).** Speaker shares use CLDR's overlapping L1 + L2 total of 9.52 billion. With the world population (8.06 billion)
  as the base, every index is multiplied by 0.847: English 2.7× becomes 2.3×, Thai 2.0× becomes 1.7×, Hindi and Arabic move to about 0.9×. Ratios between
  languages, the ordering, the correlation (r = 0.80) and the slope (1.11) are unchanged; only the level of "1×" moves. This is why the
  figure colours the index continuously and panel b shows both shares side by side, without an anchor.
- **China.** YouTube is blocked in mainland China. Removing the PRC (90% of 1.42 billion people speak `zh` in CLDR) from the Chinese speaker base
  cuts Chinese speakers from 1,325M to 51M and moves its index from 0.08× to **1.78×** (the speaker total reduced by the same amount;
  2.05× if the total is left unchanged). The low Chinese index is largely structural. The remaining Chinese-language views come from outside the PRC
  and from Taiwan, Hong Kong, Singapore and diaspora audiences, so 1.78× is itself not a clean estimate.
- **Hindi and Urdu pooled.** Hindi and Urdu are close spoken varieties written in different scripts, and LID may split them by script. Pooling them
  gives views of 6.3% and speakers of 822M (max per territory) and an index of **0.74×** (0.68× if speakers are simply summed to 893M), against
  1.0× for Hindi and 0.07× for Urdu alone. The Urdu deficit is therefore partly a labelling split.
- **Malay and Indonesian pooled.** Pooling Indonesian and Malay (speakers 208M with max per territory) gives **2.6×** (2.5× with summed speakers), against
  2.9× for Indonesian and 0.22× for Malay alone. CLDR gives Indonesian only 64% of Indonesia's population (Javanese 34%, Malay 3%), so the
  Indonesian denominator is probably low and its index high.
- **Other label artefacts.** Filipino channels often use Taglish metadata, which LID may label as English, which could lower Filipino and raise English. Other
  close pairs (Serbo-Croatian, Persian/Dari, Arabic dialects) are pooled; splitting them would change the language totals.

## Caveats

- **Written-metadata language.** Labels come from channel text, not speech. A Hindi-speaking channel with English metadata counts as
  English; this probably inflates English and deflates languages whose creators write in English or Latin script.
- **L2-heavy denominators.** English (1.73 billion), French (333M), Swahili (194M) and Urdu (313M) are dominated by second-language speakers in CLDR, and those
  speakers watch in other languages too. English at 2.7× is "views per CLDR speaker" and does not mean English speakers watch more; French,
  Swahili and Urdu are low partly because their denominators are L2-heavy.
- **Views are not viewers.** Weekly views count plays of channels in each language, wherever the viewers live, and are not a count of the
  speakers who watch. The index conflates reach (who has access and watches) with intensity (views per viewer) and with how many channels
  cater to the market. Diaspora viewing (Korean, Hindi, Arabic and Spanish speakers abroad) is in views but not attributed to speakers by CLDR, which
  counts speakers by territory.
- **Access and platform reach.** YouTube is blocked in mainland China (see the sensitivity); access to YouTube in Iran and elsewhere is also restricted or throttled, so Persian is similarly affected.
- **Speaker counts are coarse.** CLDR figures mix first and second-language use and rest on territory-level estimates; they are not
  comparable across languages with different literacy or standard-language conventions (Arabic dialects, Hindi-belt languages, Serbo-Croatian).
- **Language-group choices.** Merging and splitting labels moves language totals (see the map and the sensitivities). Mandarin and Cantonese are separate, while
  Arabic and Serbo-Croatian are pooled.
- **Subscriber threshold.** The universe is channels with ≥10k subscribers, so a language's channel counts depend on how many of its
  channels reach 10k.

## Figure 5b and the founding-year Figure 5

Figure 5 (age) shows a timing pattern in the data: channels founded 2023 or later are 24.5% of channels but 42.4% of weekly views. It rests only on
platform data and one field (`publishedAt`), so every number can be tied to the dataset. Figure 5b has two layers. Its denominator-free part (the top 10 languages hold
85% of views against 43% of speakers; English holds 48%; views grow about in proportion to speakers, r = 0.80) is as sturdy as the age figure. Its comparison with an external population can say
which language communities hold more or less YouTube attention than their size would suggest, and whether that comes from channel count or from attention per
channel, but only coarsely: the denominator is an approximate, partly second-language speaker count by territory, the labels describe metadata, views are not viewers,
and the level of the index depends on an arbitrary anchor. It cannot say why. The age figure is the sturdier single claim; Figure 5b is the richer question about whom the platform's attention serves.

## Caption (draft)

**Figure 5. Weekly views by language roughly track speaker numbers, but are concentrated in English, Korean and Indonesian.** **a**, Speakers
(Unicode CLDR, first and second language) and weekly views for the 50 languages with the most weekly views; dashed line, views proportional to
speakers; point size, measurable channels. **b**, Share of weekly views and share of speakers for the 18 largest languages; line colour, the ratio of the two. **c**, Channels
per million speakers and weekly views per channel; point size, weekly views; dashed and dotted lines mark equal views per speaker (platform average, 0.1× and 10×).
Colour is views share ÷ speaker share on a log scale. Hollow marker: Chinese (YouTube is blocked in mainland China). *Notes:* Universe: 4,990,312 channels with
≥10k subscribers on 2026-10-05; 4,770,835 with measurable views. Weekly views are the change in lifetime views between 2026-08-31 and 2026-10-05, scaled to
seven days. Language is that of the channel's written metadata, not spoken language (1.2% of weekly views are of unknown language). Speakers are
Unicode CLDR release 48.2 estimates of first- and second-language use summed over territories (172 languages with figures hold 98.8% of weekly views), so English, French, Swahili and Urdu rest mostly on
second-language speakers. Shares and ratios are taken over those 172 languages; the 1× anchor depends on that total (with world population as the base, every ratio scales by 0.85).

## Talking points

1. Denominator-free: the top 10 languages hold 85% of weekly views against 43% of speakers; English alone holds 48% (and 18% of speakers).
2. Across languages, views grow about in proportion to speakers: log-log r = 0.80 and slope 1.1 over the top 50 (r = 0.68 over all 172).
3. Around that trend the spread is wide (0.02× to 3.4× in the ratio), and the level of "1×" depends on the speaker total used: with world population it moves down by 15%.
4. Korean, Indonesian, Turkish, Japanese and Thai attract more views than their speaker counts imply, mostly through more channels per speaker; Thai is channel-driven only.
5. Chinese looks 13× below proportion, but YouTube is blocked in mainland China; without the PRC in the base it is about 1.8× above.
6. Label splits matter: pooling Hindi and Urdu gives 0.7× rather than 1.0× and 0.07×; pooling Indonesian and Malay gives about 2.6×.
7. Panel c separates "more channels" from "more views per channel", which a plain rank chart cannot.

## Run log

| Date | Run | Databricks run_id | Outcome |
|---|---|---|---|
| 2026-10-16 | v1, first submit | 58331480445465 | Failed at start: notebook had been imported as a workspace file, not a notebook; deleted and re-imported (no tables written) |
| 2026-10-16 | v1 | 51060327728735 | Success on cluster `0818-185002-anfay17g`; wrote `dev_sean.matt.fig5b_langrank_{channels,languages,status,reconcile}_v1` |
| 2026-10-16 | Review round 1 | — | Figure and README revised from the saved CSVs (no Databricks rerun) |
