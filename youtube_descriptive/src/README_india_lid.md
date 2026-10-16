# README: India Hindi vs English (IndicLID pilot, ED panel, validation set)

Question: the LID v3 labels 506,973 India-declared channels `eng` (219,956 India-declared channels are `hin`), but many
Hindi or Hinglish channels with English-written titles sit in `eng`. This work (a) runs AI4Bharat IndicLID on those
channels, (b) draws an Extended Data panel of category mix for Hindi-labelled channels versus IN x English channels
split by IndicLID, and (c) prepares a 300-channel human validation sample.

Code: `20_india_lid_pilot_databricks.py` (Databricks notebook, stage widget), `india_lid_plot.py`,
`../scripts/render_india_lid_local.py`. Outputs: `../figs/india_lid/` (aggregate CSVs, panels),
`../validation/india_lid_v1/`. Channel-level tables stay in `dev_sean.matt.india_lid_pilot_*_v1`.

## Headline results and what they mean

1. **IndicLID does not isolate Hindi on this material.** It labels 62-63% of the 10k IN x eng sample English, 33-34%
   "other romanised Indic" (a scatter across ben/kas/tel/mal/kan/ori/mni/kok/asm/urd/pan/guj/san Latin classes) and only
   0.4% (two-stage) or 0.1% (first stage) `hin_Latn`. Short English-written titles (hashtags, names, brand words) are
   routinely pushed to arbitrary Indic classes, especially by the IndicBERT stage. The "romanised Indic" group is
   therefore a **noisy** marker of Hindi/Hinglish, not a clean Hindi set. The coded validation sample is what turns it
   into a measured precision/recall.
2. The signal is nonetheless real: the share of a channel's titles IndicLID calls romanised-Indic is monotonically related
   to a Hindi function-word lexicon (lexicon flag rate 3% at 0% Indic titles, about 24% at 60-90%).
3. **Panel**: Hindi-labelled channels differ sharply from IN x English channels (Music 37% vs 14% of channels, Society
   25% vs 10%, Gaming 2% vs 12%, Lifestyle 12% vs 30%), and the IndicLID "romanised Indic" part of IN x English sits
   in between (Music 28%, Society 7%, Gaming 9%, Lifestyle 27%). Relabelling IN x English by IndicLID therefore does
   not move them toward the Hindi category profile (Society stays low); it mostly shifts Music up.

## Method

**Frame** (`..._frame_v1`). `dev_sean.matt.fig2_map_channels_v2` (>=10k-subscriber channels), non-topic (`is_topic`
excluded), language `hin` (any country) or `eng`, joined to `yt_channel_created_at` (latest `status='ok'` row's
self-declared `country`; `IN` = India-declared). Counts reproduce the scoping pass: IN x eng 506,973; IN x hin 219,956;
total `hin` 297,846 measurable (all countries).

**Weekly views.** The Figure 5 SQL (`fig5-age`, `15_fig5_age_databricks.py`), window 2026-08-31 to 2026-10-05: lifetime
view delta divided by elapsed days times 7, latest collection per channel and date. `missing_start`/`missing_end` and
`total_fell` channels have no weekly views and are excluded from the sample and panel (IN x eng: 833 missing_start,
32,261 total_fell; 458,607 positive; 15,272 zero). Zero-delta channels stay in (0 views).

**Sample** (`..._sample_v1`). 10,000 measurable IN x eng channels: 1,000 per weekly-views decile (ntile 10 over
`wv, canonical_id` within IN x eng; decile 10 = most views), taken in `xxhash64(canonical_id, 20261016)` order.
Stratum weight = decile size / 1,000 (about 47,388). Weights give population channel estimates; view-weighted shares
weight each channel by `weight * wv`.

**Text units** (`..._texts_v1`). Per channel: `channel_text` = LID v3 `channel_name` + `channel_description` (first 600
characters), from `..._segments_input.clean_text`, i.e. the metadata the LID v3 pipeline classified; `lid_title` = LID
v3 video-title segments (June 2026 crawl), de-duplicated against recent titles; `recent_title` and `recent_desc`
(first 300 characters) = up to 10 newest uploads from the latest `collected_date` snapshot per channel in
`yt_channel_videos`. URLs removed, whitespace collapsed. Snapshots are not all recent: the latest snapshot per sampled
channel is from June 2026 for 6,978 channels, August for 20 and October for 2,889 (113 sampled channels have no
video snapshot). Sample coverage: 98.9% of channels have at
least one title (mean 17.9 title units), 94.7% have channel text.

**IndicLID.** Models only from the official AI4Bharat GitHub release `v1.0` (2023-05-22):
`indiclid-ftn.zip` (sha256 `ececf588...2e7`), `indiclid-ftr.zip` (`683b3083...248`), `indiclid-bert.zip`
(`8515a50b...487`) from `https://github.com/AI4Bharat/IndicLID/releases/download/v1.0/`. Inference logic follows
`Inference/ai4bharat/IndicLID.py` (repository head `e4bfe42923c5ad581ccb64ca9feb38c0cd572ba8`, 2025-12-02) and is
re-implemented in the notebook: text with more than 50% Latin letters/digits goes to the roman path, else the native
fastText (FTN); roman text goes to FTR and, if FTR confidence is 0.6 or lower, to IndicBERT. The BERT tokenizer is
`ai4bharat/IndicBERTv2-MLM-only` from AI4Bharat's Hugging Face org (revision `8598f13f...c5c`), which the official code
loads. Packages: fasttext 0.9.3, torch 2.5.1+cpu, transformers 4.46.3, numpy 1.26.4, Python 3.12, installed with
notebook-scoped `%pip`. The pickled BERT model predates transformers 4.36, so the notebook sets
`attn_implementation="eager"` on it; this is a compatibility patch, not a model change. Two label variants are stored:
`2s` (two-stage, IndicBERT where it ran) and `ftr` (first stage only). In the pilot BERT ran on titles and channel
text, not on descriptions.

**Classes.** Unit label to class: `eng_Latn` english; `hin_Latn` or `urd_Latn` roman_hindi (roman Hindi and roman Urdu
are essentially one Hindustani register; IndicLID routes Hindi examples such as "Taarak Mehta Ka Ooltah Chashmah" to
`urd_Latn`); any other `*_Latn` roman_other_indic; `other`; all non-Latin labels native_script. Channel label =
plurality class over title units; ties or no titles fall back to the channel-text class; else `undetermined`.
Panel group "romanised Hindi/Indic" = roman_hindi + roman_other_indic.

**Lexicon.** Hindi function words in Latin script (about 140; list in the notebook, `HINDI_FW`; excludes collisions
with common English words such as the, main, par, do, log, kar, desi), tokenised with `[a-z]+` on lower-cased titles
plus channel text, computed in SQL. Score = hits / Latin word tokens. Flag = at least 3 hits and score at least
0.05. (A first version contained "the" and inflated flag rates; the final run uses the corrected list.)

**Panel** (`..._panel_v1`). Hindi groups and "IN x eng as labelled" come from the exact frame. The IndicLID split of
IN x eng uses the **full run** (first stage only; see Runtime) in the headline panel, and the weighted two-stage pilot
sample as a cross-check. Only measurable channels count.

## Pilot results (10,000-channel sample)

Channel label shares, two-stage (`2s`):

| class | channels (weighted) | weekly views (weighted) |
|---|---|---|
| english | 63.3% | 69.7% |
| roman_other_indic | 33.4% | 27.9% |
| roman_hindi | 0.43% | 0.20% |
| native_script | 1.4% | 0.83% |
| other | 1.3% | 0.83% |
| undetermined | 0.21% | 0.52% |

First stage only (`ftr`): english 57.5% / 63.9%, roman_other_indic 39.0% / 33.8%, roman_hindi 0.13% / 0.06%,
native 1.4% / 0.7%, other 1.7% / 0.95%.

By views decile (share of sampled channels, two-stage; decile 1 = fewest views, 10 = most):

| decile | english | roman_hindi | roman_other | native | other/undet. |
|---|---|---|---|---|---|
| 1 | 60.9% | 1.2% | 33.5% | 1.3% | 3.1% |
| 2 | 59.4% | 0.3% | 38.3% | 1.1% | 0.9% |
| 3 | 60.2% | 0.1% | 37.1% | 1.4% | 1.2% |
| 4 | 59.5% | 0.2% | 37.9% | 1.5% | 0.9% |
| 5 | 62.9% | 0.4% | 34.2% | 1.6% | 0.9% |
| 6 | 64.7% | 0.7% | 31.9% | 1.3% | 1.4% |
| 7 | 67.5% | 0.2% | 30.2% | 1.5% | 0.6% |
| 8 | 65.9% | 0.4% | 30.6% | 2.0% | 1.1% |
| 9 | 64.9% | 0.5% | 31.8% | 0.8% | 2.0% |
| 10 | 67.1% | 0.3% | 28.3% | 1.7% | 2.6% |

Roman-Indic share falls modestly with views; the top decile has the highest English share.

**Agreement with the lexicon.** Lexicon flag rate: 14.6% overall in the sample. Among channels IndicLID calls
romanised Hindi/Indic 19.4% are flagged (657 of 3,381) versus 12.1% of the rest (803 of 6,619); Cohen's kappa between
"IndicLID romanised Indic" and the lexicon flag is 0.085 (weak). Within IndicLID's `roman_hindi` class 60% are flagged
(26 of 43). The lexicon is a conservative marker of channels that write a lot of Hindi function words, so low flag rates
among Hinglish channels with few such words are expected; the two signals are weakly rather than strongly aligned.
First-stage and two-stage channel labels agree for 88.1% of channels (89.4% on the Indic-versus-not split).

## Runtime

Cluster `0818-185002-anfay17g` (driver 8 cores, CPU only; no GPU needed or used).

| run | units | model path | seconds |
|---|---|---|---|
| pilot (10k channels) | 249,314 | two-stage; 49,298 units (19.8%) through IndicBERT | 677 (first stage 4.5; IndicBERT 620, about 80 units/s) |
| full (506,819 channels with text) | 9,568,962 (titles and channel text; no descriptions) | first stage only | 899 (fastText 133; the rest Spark I/O) |

Extrapolating the pilot, two-stage IndicLID on the full set would need about 1.9M IndicBERT units, about 6.6 hours on
this driver, so IndicBERT was **not** run on the full set. The panel uses the first-stage full run, justified by
88% channel-level agreement with the two-stage labels, but first-stage and two-stage differ by about 6 points in the
English share (57% vs 63%): the first stage sends more channels to roman Indic classes. The panel numbers differ
correspondingly (see `figs/india_lid/*pilot_sample_weighted_2s*` vs `*full_run_ftr*`).

## Panel results (weekly views window 2026-08-31 to 2026-10-05)

Share of channels / share of weekly views (headline: full first-stage run):

| category | Hindi-labelled, all countries (n 297,846) | IN x eng, IndicLID romanised Indic (n 185,977) | IN x eng, IndicLID English (n 272,938) | IN x eng as labelled |
|---|---|---|---|---|
| Music | 37.5 / 25.4 | 27.8 / 21.2 | 13.6 / 11.8 | 19.3 / 15.1 |
| Gaming | 1.6 / 0.9 | 9.2 / 3.3 | 12.5 / 7.1 | 11.1 / 5.6 |
| Entertainment | 14.9 / 35.1 | 21.0 / 39.2 | 19.7 / 35.7 | 20.2 / 37.2 |
| Lifestyle | 12.4 / 17.8 | 27.0 / 29.2 | 30.4 / 33.6 | 28.9 / 31.8 |
| Society | 25.2 / 17.4 | 7.4 / 4.1 | 10.1 / 6.7 | 9.1 / 6.0 |
| Knowledge | 3.8 / 1.7 | 3.1 / 1.0 | 8.0 / 2.6 | 6.0 / 2.0 |
| Sports | 1.1 / 0.7 | 1.8 / 1.5 | 2.1 / 1.9 | 2.0 / 1.8 |
| Unclassified | 3.5 / 1.0 | 2.7 / 0.5 | 3.7 / 0.5 | 3.4 / 0.6 |

India-declared Hindi (sensitivity, n 202,180 measurable; `hin_IN`): Music 35.1 / 25.4, Society 26.6 / 19.4, Gaming 1.5 /
0.8, Lifestyle 12.9 / 16.5 (channels / views), essentially the same as all-country Hindi, as expected when 95% of
country-declaring Hindi channels declare IN. Variants in `figs/india_lid/`: `main`, `with_all` (adds the as-labelled
bar), `lexicon_sensitivity` (splits IN x eng by lexicon flag instead of IndicLID), `hin_in_sensitivity`.

## Validation set

`../validation/india_lid_v1/validation_sample.csv`: 300 channels from the pilot sample (channels with at least one title
and measurable views), strata = IndicLID two-stage label (english, roman_hindi, roman_other_indic, native_script,
other/undetermined) x views tercile (ntile 3 of weekly views over the sample), equal allocation of 22 per stratum
except roman_other_indic (21 each) and roman_hindi (13/12/14, all that exist), seed 20261016, shuffled. Strata with
few members (roman_hindi) are exhaustively sampled to the extent available. Coder columns are empty. Instructions:
`CODING_INSTRUCTIONS.md`. The CSV also carries the two `stratum_*` columns, which coders must hide.

## Caveats

- IndicLID was trained on sentence-length text; the YouTube titles are short, hashtag-heavy and often emoji, name or
  brand dominated. The "other romanised Indic" class is largely an artefact for such text.
- Written language is not spoken language: LID classifies metadata, the validation set codes speech.
- Channel text and titles come from snapshots of different dates (June crawl, August and October refreshes).
- 1.0% of sample channels have no title text and fall back to channel text; 154 IN x eng channels have no text at all
  (`undetermined`).
- Sample-based numbers carry sampling error (1,000 channels per decile); no confidence intervals are drawn.
- `lex_*` columns written per unit in the preds tables came from an earlier word list and are not used; the lexicon is
  recomputed in SQL.
- The BERT checkpoint is loaded with `torch.load(weights_only=False)` from the official release file (hash above).

## Run log (Databricks runs, cluster 0818-185002-anfay17g)

| run id | stage | result |
|---|---|---|
| 949326926478673 | probe | failed (BERT pickle vs transformers 4.46: `attn_implementation`); egress and model download fine |
| 1063968998433732 | probe | success; BERT benchmark about 165-190 short texts/s |
| 1088033029514625 | prep | success |
| 211374304715195 | pilot | success, 677 s |
| 329308073968602 | full (first stage only) | success, 899 s |
| 198273329824053 | aggregate | failed (`GROUP BY 0`), fixed |
| 739908555026536 | aggregate | success (pilot only, superseded) |
| 276297540959471 | aggregate | success (corrected lexicon) |
| 561779860230601 | panel + validation | success (pilot-only panel, panel superseded) |
| 1009166522208203 | aggregate + panel (with full run) | success; the final tables |

Tables written (all `dev_sean.matt.india_lid_pilot_<kind>_v1`): frame, sample, texts, preds, fullpreds, runlog_pilot,
runlog_full, channels, full_channels, label_shares, agreement, lexicon, panel, panel_sources, validation.
Render: `python scripts/render_india_lid_local.py --profile accelerator-db --warehouse 86100da4e1fe8713 --source full_run:ftr --validation`.
