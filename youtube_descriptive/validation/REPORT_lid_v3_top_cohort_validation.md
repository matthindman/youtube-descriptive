# LID v3 — Top-Subscriber Cohort Validation Report (WORK IN PROGRESS)

**Status:** Draft / living document — last updated 2026-05-22
**Run under audit:** `01_language_openlid_v3_databricks`, `run_id = default` ("top-of-the-ocean" / top-subscriber cohort)
**Random-100k subscriber-band cohort:** not yet run
**Data locations:** analysis tables → `dev_sean.matt.yt_lid_v3_*`; source channels → `prod_tads.youtube.yt_sl_channels`

---

## 0. How to read this report

This document reconstructs and validates a single production run of the dual-model
(OpenLID-v3 + GlotLID) YouTube language-ID pipeline. Each claim is tagged by evidence strength:

- **[CONFIRMED]** — directly queried from the analysis tables or printed in the notebook run log.
- **[DERIVED]** — computed by combining confirmed numbers (e.g. rate × base); precise to ±a few hundred.
- **[SAMPLE]** — observed in a manual/subagent sample; indicative, not population-level.
- **[OPEN]** — not yet answerable from available evidence; see §9.

There is **no ground-truth/gold label inside the pipeline run itself.** To get a truth signal we built
an external validation set (§8): three *independent* classifiers (Codex, Gemini, and Claude subagents)
that fetch live channel metadata and judge language, plus a 50-row `channel_language` column of
provisional gold labels. Where this report says "accuracy," it means accuracy against that external
set; where it says "agreement," it means classifier-vs-classifier concordance.

---

## 1. Provenance & run health

**Artifact audited.** The original `.dbc` export contained **only the child notebook**
(`01_language_openlid_v3_databricks`, 80 cells) — not the cohort driver
(`01b_language_lid_v3_subscriber_cohort_analysis`). The driver source does exist in the repo at
`youtube_descriptive/src/01b_language_lid_v3_subscriber_cohort_analysis_databricks.py`, but its
*run log* (cohort sizes, subscriber cutoff, the `Finished {cohort}` prints) was **not** in the export
and has not been audited.

**Run health [CONFIRMED].** All 80 cells finished; no cell carries an error/exception. No `WARNING`
lines. The only red text is a non-fatal pip dependency-resolver notice (install succeeded). All
acceptance checks printed `[PASS]`. The run completed cleanly.

**Cohort definition (confirmed by owner).** This run is the **entire "top of the ocean" population** —
the highest-subscriber channels, from the top down to a subscriber cutoff estimated to cover ~half of
all platform-wide views. It is **not** a 100k cohort: 105,638 channels is the intended full set, and
`run_id=default`, full `yt_sl_channels` source, and full bucket range are all correct for processing the
whole population (no per-cohort sub-selection applies). The separate `random_*_subscriber_band` sample
(channels below this cutoff) has not yet been run. Open item: record the exact subscriber cutoff value
and the basis for the ~50%-of-views estimate (§9).

**Display gap.** QA distributions in the notebook are gated behind `enable_notebook_displays`
(default `false`), so the run log captured only `print()` headers, not the distribution tables. All
numbers in §3–§5 were recovered by querying the persisted `yt_lid_v3_*` tables directly — not from
the export.

---

## 2. What the pipeline processed [CONFIRMED]

| Quantity | Value |
|---|---|
| Final channel universe (`yt_lid_v3_channels`) | **105,638** |
| Videos selected for segment fan-out (≤10/channel, ranked by `published_at`) | 1,035,054 |
| Valid text segments scored (both models) | **1,384,859** |
| OpenLID-v3 model | `/dbfs/models/openlid_v3/openlid-v3.bin` (1.23 GB) |
| GlotLID model | `/Volumes/dev_sean/matt/models/glotlid.bin` (1.69 GB) |

**Evidence base is thin and lopsided [CONFIRMED].** Of 1,384,859 valid segments:

| Segment type | Valid | Invalid |
|---|---|---|
| video_description | 786,022 | 52,149 |
| video_title | 595,338 | 439,716 |
| channel_name | 3,499 | 102,139 |
| channel_description | *(column absent)* | — |
| video_tags | *(column absent)* | — |

Two source columns (`channel_description`, `video_tags`) **did not resolve to any column** in
`yt_sl_channels`/`yt_sl_videos` and contributed **zero** segments. Channel name is 97% rejected
(too short). So the entire run rests on **video descriptions + video titles**. Most invalidation is
`below_min_clean_chars_latin_or_mixed` (567,767) — the 40-character Latin threshold.

> **Risk:** channel description is normally the richest written-metadata signal. Its absence (likely a
> schema/column-name mismatch, since the detector's candidate list includes `description`/`about`/`bio`)
> narrows the evidence base and should be verified against the live source schema. **[OPEN]**

---

## 3. Output distributions [CONFIRMED]

**Channel classification status** (`yt_lid_v3_channels`, n = 105,638):

| language_status | n | % |
|---|---|---|
| classified | 91,768 | 86.9% |
| needs_review | 9,088 | 8.6% |
| mixed_language_candidate | 2,615 | 2.5% |
| insufficient_text_or_unclassified | 2,167 | 2.1% |

**Consensus status** (`yt_lid_v3_channel_model_comparison`):

| consensus_status | n |
|---|---|
| exact_model_agreement | 91,974 |
| model_disagreement_needs_review | 8,165 |
| high_risk_tail_label_needs_review | 1,648 |
| iso_or_script_variant_agreement | 1,001 |
| cluster_model_agreement | 338 |
| glotlid_fallback_openlid_low_confidence | 323 |
| openlid_high_confidence_glotlid_missing_or_error | 31 |
| insufficient_text | 19 |

**Top consensus languages:** eng_Latn 45,321 · spa_Latn 10,789 · por_Latn 8,850 · rus_Cyrl 6,069 ·
fra_Latn 2,711 · tha_Thai 1,977 · jpn_Jpan 1,704 · deu_Latn 1,590 · tur_Latn 1,533 · kor_Hang 1,333 …
(146 distinct labels).

**Per-model coverage [CONFIRMED]:** GlotLID 103,449 channels with a primary; OpenLID-v3 103,369 —
near-identical. The two models diverge on *labels*, not on *coverage*.

---

## 4. Inter-model agreement — the "true" agreement story

Headline agreement rates over the 103,338 channels where both models produced a primary
[CONFIRMED]: **exact 89.25% · ISO 90.22% · cluster 14.23%.**

The cluster rate (0.1423) is **not** a real signal — `analysis_cluster` is NULL for all languages
outside the 23-entry confusable-cluster map (eng/spa/por/etc.), so those count as "disagreement."
Cluster agreement is only meaningful *within* clustered sets.

**Discounting taxonomy artifacts and understandable confusions [DERIVED]:**

| Level | % of both-primary |
|---|---|
| Exact label match | 89.3% |
| + script/orthography variants (mostly Chinese Hant/Hans/Hani; caught by ISO) | 90.2% |
| + Arabic macro↔dialect (`ara` vs `arb/ary/arz/…` — same language) | ~94.6% |
| + cluster-level & legit confusables (Malay/Indonesian, Iberian-Romance, Cantonese/Mandarin) | ~95.3% |
| **Residual genuine disagreement** | **~4.7% (~4,900 channels)** |

**Top-line: after discounting taxonomy and understandable confusions, the two models genuinely agree
on channel language for ~95% of channels.** The raw 89% understates real agreement by ~6 points,
almost entirely due to two labeling *conventions* (Arabic granularity, Chinese script tags), not model
error.

> ⚠️ This is inter-model agreement, **not** accuracy. Where both models share a bias they "agree" and
> inflate this number (see §6, the romanized-Hindi-as-English pattern).

---

## 5. The 4,526-channel Arabic consensus gap [CONFIRMED]

A direct query found **4,526 channels** with `consensus_language_label = NULL` where *both* models'
primary ISO is in the Arabic family (`ara/arb/ary/arz/apc/ars/ajp/aeb/…`). Both models agree the
content is Arabic; the consensus rule blanks the label only because OpenLID emits the macrolanguage
`ara` while GlotLID emits a specific dialect, so `models_agree_iso_primary = false`.

This is **4.3% of the entire run** dropped from classification over a taxonomy convention. It inflates
`model_disagreement_needs_review` and deflates the ISO agreement rate. Cross-checked against YouTube
metadata: `ar → NULL` for 4,506 channels (§6) — the same gap seen from the source side.

---

## 6. Cross-check vs. YouTube declared metadata language [CONFIRMED]

`yt_lid_v3_source_language_confusion` (`source_language_value` is a YouTube BCP-47-ish code;
`consensus_language_iso639_3` is ISO 639-3 — read as a pattern table, not a clean accuracy rate):

- **Strong diagonal** on agreed channels: `en→eng` 39,427 · `es→spa` 8,681 · `pt-pt→por` 5,915 ·
  `ru→rus` 5,867 · `fr→fra` 2,560 · `th→tha` 1,900 · `ja→jpn` 1,651 …
- **`ar → NULL` 4,506** — the Arabic gap (§5) seen from the metadata side.
- **Shared-bias signal:** `hi → eng` 307 — Hindi-declared channels detected as English (romanized
  Hindi / English-heavy titles). This is the failure class agreement *cannot* catch.

---

## 7. Source of the residual disagreement (~4,900 channels)

Defined as: both models have a primary, **not** exact-equal, **not** ISO-equal, **not** cluster-equal,
and **not** within the Arabic family. Archetype mix from a 243-row readable sample [SAMPLE]:

| Archetype | Share of sample |
|---|---|
| **English vs. another language** | ~67% (71% have `eng_Latn` on one side) |
| OpenLID tail-label over-prediction (srd/ast/gug/sun/vec/lim/…) | ~17% |
| Other cross-language | ~10% |
| GlotLID fallback (OpenLID low-confidence) | ~4% |
| Arabic vs. non-Arabic | ~2% |

**The dominant residual mechanism is "one model reads the metadata as English, the other as the local
language,"** in both directions — i.e. the two models split on short, Latin-script, code-mixed text.
Sub-clusters worth separating: romanized/mixed **Indic** (eng vs hin/mar/tel/pan/guj/urd); **English-based
creoles** (eng vs pcm/jam — arguably not errors); and **short European** text.

> Run the archetype SQL (in the team thread) for exact population counts rather than the sample ~67%.

---

## 8. Independent multi-classifier validation [SAMPLE]

We adjudicated the residual-disagreement sample with **three independent classifiers** that fetch live
channel metadata and judge written-metadata language *without* being told which pipeline model to favor,
plus a curated `channel_language` gold column. All per-channel verdicts live in
`lid_v3_residual_disagreement_sample.csv`.

**Classifiers & coverage (of 300 sampled residual channels):**

| Source | Column | Reached | Notes |
|---|---|---|---|
| **Codex** | `codex_primary_language_label` | 206 / 300 | run on all 300; the broadest reference |
| **Gemini** | `gemini_classification` | 65 (of ~120 attempted) | |
| **Claude subagents** | `subagent_judgment` | 32 (of 50 attempted) | first 50 only |
| **`channel_language` (gold)** | `channel_language` | 32 (50 filled, ~18 `und`) | first 50 only; provenance unconfirmed — see §9 |

> **On the `channel_language` column:** it was added to the CSV externally (not by this analysis),
> covers only rows 1–50, uses `und` for undetermined and multi-label `+` for bilingual channels. It
> matches **no single classifier** (closest is Codex at 22/50) and agrees with the *judge* cluster
> (Codex/Gemini/subagent, 83–91%) far more than with the pipeline (OpenLID/GlotLID, 41–44%) — i.e. it
> behaves like a high-quality human/LLM label set, not a fastText output. Treated here as **provisional
> gold** pending confirmation of its origin.

### 8.1 The judges are reliable; the pipeline models are not (on residual cases)

Primary-language accuracy against the 50-row gold column:

| Classifier | n scored | primary-correct |
|---|---|---|
| **Codex** | 32 | **91%** |
| **Gemini** | 25 | 84% |
| **Subagents** | 27 | 74% |
| OpenLID | 50 | **28%** |
| GlotLID | 50 | **26%** |

The three LLM judges cluster tightly (pairwise + vs-gold agreement 83–91%), so Codex is a sound
reference for the larger 206-row scale analysis. The two fastText models are the outliers.

### 8.2 Agreement matrix (primary base language; n = rows both classified)

| | OpenLID | GlotLID | gold | Subagent | Gemini | Codex |
|---|---|---|---|---|---|---|
| **OpenLID** | — | **0%** (300) | 44% (32) | 53% (32) | 48% (65) | 41% (206) |
| **GlotLID** | 0% (300) | — | 41% (32) | 31% (32) | 35% (65) | 34% (206) |
| **gold** | 44% | 41% | — | 83% (24) | 84% (25) | **91%** (32) |
| **Subagent** | 53% | 31% | 83% | — | **91%** (22) | 83% (29) |
| **Gemini** | 48% | 35% | 84% | 91% | — | 83% (64) |
| **Codex** | 41% | 34% | 91% | 83% | 83% | — |

OpenLID↔GlotLID = 0% **by construction** (this is the disagreement sample). The story: a tight
*judge cluster* at 83–91%, and the pipeline models agreeing with that cluster only 31–53%.

### 8.3 Head-to-head: which pipeline model is right on residual disagreements?

| Reference | OpenLID right | GlotLID right | **Neither** | Both (bilingual) |
|---|---|---|---|---|
| Gold (hardest 50) | 12 (24%) | 13 (26%) | **23 (46%)** | 2 |
| Codex (n = 206) | 85 (41%) | 71 (34%) | **50 (24%)** | 0 |

**This revises the earlier subagent-only finding** (which had OpenLID winning 17–8 on n=32). With real
coverage and gold labels it is close to a coin flip on *who* is right, and the dominant outcome is that
**neither pipeline model is right** — 24% at scale, 46% on the hardest cases. The residual pile is not
"one model is the good one"; it is "this metadata defeats both fastText models."

### 8.4 Real failure modes (ranked, vs Codex n = 206)

1. **OpenLID tail-label emission is essentially always wrong — 0 / 33.** Every channel whose OpenLID
   primary is a high-risk tail label (srd/ast/gug/sun/lim/vec…) is wrong; GlotLID's alternative is right
   23/33 (the other 10 are a third language both missed). Cleanest, most actionable signal in the data.

2. **The two models have *opposite, complementary* English biases.** Splitting the English-vs-other
   archetype (127/206) by whether the truth is actually English:
   - **Truth NOT English (n=100):** GlotLID wrongly said "English" **67×**, OpenLID 25×; OpenLID right 55,
     GlotLID 19. → **GlotLID over-collapses non-English *Latin-script* channels (Spanish/Portuguese/French/
     Italian/Czech/Indonesian/Tagalog…) to English.**
   - **Romanized South-Asian subset (n=27):** it flips — OpenLID wrongly said "English" **10×**, GlotLID 1×;
     GlotLID correctly fired `urd_Latn`/`pan_Latn`/`ben_Latn`. → **OpenLID over-collapses romanized
     Hindi/Urdu/Punjabi to English; GlotLID handles it.**
   - **Truth IS English (n=27):** coin flip (12 vs 13).

3. **Both miss a third language (the 50 "neither" cases):** 23 the truth is a mainstream Latin language
   neither picked (e.g. Javier Ferreira → `spa`, OpenLID `ast` / GlotLID `eus`); 15 a non-Latin script both
   missed (e.g. a Japanese channel → OpenLID `dan` / GlotLID `eng`); 12 romanized/minority both whiffed.

**Plausible explanations for the OpenLID↔GlotLID disagreement:**
- *Romanization coverage asymmetry.* GlotLID (~2000 langs, incl. low-resource/romanized) recognizes
  romanized South-Asian; OpenLID's narrower standard-orthography training treats it as generic Latin → English.
- *English as an attractor class in GlotLID* for short/brand-heavy Latin text → the 67/100 European→English collapse.
- *fastText minority-Romance tail artifact (OpenLID)* — rare classes (srd/ast/gug) win on sparse Latin evidence.
- *Genuine bilingual metadata* — gold is heavily multi-label (`urd_Latn+pnb_Latn`, `kor_Hang+eng_Latn`);
  some "disagreement" is two models each catching a different real language, not error.
- *Latin channel-name vs non-Latin video-titles* — on sparse channels the brand name competes with the titles.

### 8.5 Reachability / data-quality finding [SAMPLE]

The independent classifiers could not reach a large fraction of the sampled channels:

- **Codex (run on all 300): 94 unreachable (31%)** — its notes read "this channel does not exist / was removed."
- **First 50 (all three judges attempted): 15 (30%) reached by *none* of the three; 19 failed by ≥2 of 3.**

Two independent estimates converge at **~30% unreachable**, and the cause is **channel-ID transcription
error** (the 24-char IDs were OCR'd off screenshots and are off by a character — Codex/Gemini sometimes
*corrected* an ID and found a live channel, e.g. `UCincei…`→`UClncei…` = Dj Payback Garcia). This is a
data-collection artifact, not a 30% channel-death rate, and it caps the verifiable sample. **Pulling the
residual set via direct table export (exact IDs) is the single highest-value unblock for validation.**

### 8.6 Sizing the LLM-adjudicator load [DERIVED]

How many of the 105,638 channels would route to the LLM panel, net of the cheap deterministic fixes
(Arabic/Chinese normalization) that resolve before any LLM call. **Plan of record: send *all* tail cases
to the panel** (GlotLID's tail alternative is only ~70% right on a small sample — not safe to auto-resolve).

| Bucket | n | To LLM panel? |
|---|---|---|
| `exact_model_agreement` | 91,974 | No — agree |
| `iso_or_script_variant_agreement` + `cluster_model_agreement` | 1,339 | No — resolved |
| Arabic macro/dialect NULLs (inside `model_disagreement`) | ~4,526 | No — fixed by P0 normalization |
| `model_disagreement_needs_review` (after removing Arabic) | ~3,639 | **Yes** |
| `high_risk_tail_label_needs_review` (send all) | 1,648 | **Yes** |
| `glotlid_fallback_openlid_low_confidence` | 323 | **Yes** |
| `openlid_high_confidence_glotlid_missing` | 31 | Yes |

**Core adjudication load ≈ 5,641 channels ≈ 5.3% of the run.** ~95% of channels never touch the panel
(clean agreement + taxonomy-normalizable). This matches the independent §7 residual estimate (~4,900–5,300).

**Optional add-ons** if the panel also does more than tie-break:

| Add | n | Δ |
|---|---|---|
| Verify `mixed_language_candidate` (confirm bilingual) | 2,615 | +2.5% |
| Rescue `insufficient_text_or_unclassified` via live metadata fetch | 2,167 | +2.1% |

Including both → envelope **~10,400 channels ≈ ~10%**. (The rescue case is attractive: those channels
were unclassifiable partly because `channel_description` was missing from the pipeline input — §2 — which
a live fetch recovers.)

**Caveats that move the number:**
- **Top-cohort-specific.** Big mainstream channels have cleaner metadata and higher agreement. The
  random-100k band and the full census long tail likely have *more* code-mixed/sparse metadata → expect a
  **higher** panel fraction there. Re-estimate after the random cohort runs.
- **Reachability tax.** A share of routed channels are dead/removed and return no verdict (~5–15% with
  clean IDs; the 30% seen earlier was inflated by OCR'd IDs).

---

## 9. What we still cannot conclude (OPEN)

1. **Population-level accuracy** — gold exists for only 50 rows; the 206-row scale numbers use Codex as
   reference (itself ~91% vs gold, so ~9% reference noise). The pipeline's own `manual_validation_sample`
   step was skipped (`create_validation_samples=false`).
2. **Provenance of the `channel_language` column** — is it human gold or another tool's output? This
   determines whether §8.1 numbers are "accuracy" or "agreement." (It behaves like a judge, not the pipeline.)
3. **Subscriber cutoff value & the ~50%-of-views basis** — cohort identity/size are confirmed (entire
   top-of-ocean population, 105,638 channels); the exact cutoff and the view-share estimate are still to
   be recorded for documentation.
4. **The ~30% unreachable channels** — need correct IDs (export) to verify; until then ~1/3 of the sample is dark.
5. **Whether `channel_description` is genuinely absent** or a column-name mismatch (§2).
6. **The random-100k subscriber-band cohort** — not yet run; representativeness of the top cohort is unknown.

To close these: export the `yt_lid_v3_*` tables (and the residual sample with exact `channel_id`s)
filtered by cohort `run_id`, the driver run log, and the live `yt_sl_channels` schema.

---

## 10. Actionable findings — pipeline improvements

Prioritized by leverage. The consensus/aggregation fixes touch channel-level logic only (no re-inference);
the model-confidence fixes require re-scoring.

### P0 — Normalize Arabic macro/dialect and Chinese script tags *before* computing agreement
**Why:** recovers the 4,526 NULL-consensus Arabic channels (§5), shrinks `needs_review` by roughly half
the Arabic share, and raises the ISO agreement rate — without changing any segment prediction.
**How:** collapse `ara/arb/ary/arz/apc/ars/ajp/aeb/…` to a single Arabic key and `cmn_Hant/Hans/Hani` to a
single Chinese key, then evaluate `models_agree_iso_primary` on normalized keys; keep the dialect/script
as a secondary audit field.

### P0 — Route residual disagreements to a three-LLM adjudication panel
**Why:** §8.3 shows that on residual disagreements both fastText models are right only ~25–41% of the time
and **neither is right 24–46%** — while a single LLM judge (Codex) already hits ~91% vs gold, and the
independent judges agree with each other 83–91% (§8.2). No simple "prefer model X" heuristic works
(§8.4). This is the highest-impact *quality* change available.

**Design — three-LLM panel.** Adjudicate each routed channel with a panel of three independent frontier
models, voting on the written-metadata label:
- **Claude Opus** (Anthropic)
- **GPT-5.5** (OpenAI)
- **Gemini Pro** (Google)

All three run the shared spec in `llm_panel_classifier_prompt.md` (written-metadata language,
ISO 639-3 + script, romanization/bilingual handling, abstention). Resolution:
- **≥2 agree →** take the majority label (record vote split + per-model rationale).
- **3-way split / no majority →** mark `needs_human_review` and surface all three opinions.
- **A model returns `unreachable`/`insufficient_text` →** it abstains; decide on the remaining votes.

Using three independent models (not one) guards against any single model's bias — recall each base model
has its own systematic failure mode (§8.4), and the validation here showed the judges already triangulate
tightly. The panel's majority is the de-facto truth signal until a real gold set exists (§9, P3).

**Scope — what gets routed.** Per §8.6: the **core load is ~5% of channels (~5,600)** = non-Arabic
`model_disagreement` + **all** `high_risk_tail` cases (sent to the panel, *not* auto-resolved to GlotLID)
+ low-confidence/fallback buckets. Optionally extend to `mixed_language_candidate` (verify) and
`insufficient_text_or_unclassified` (rescue via live fetch) for an **upper envelope of ~10%**. The other
~95% (clean agreement + taxonomy-normalizable) never touch the panel, keeping cost bounded.

### P1 — Never emit OpenLID minority-Romance tail labels (they are ~0% correct)
**Why:** §8.4(1): OpenLID's tail emissions (srd/ast/gug/sun/lim/vec) were right **0/33**. GlotLID's
alternative is right ~70%, but the remaining ~30% are "neither" cases — so GlotLID-substitution is *not*
safe enough to auto-apply.
**How:** suppress the tail label as a final answer in all cases, and **route every tail channel to the
P0 panel** (this is why §8.6 sends all 1,648 tail cases, not just 30%). Keep the original OpenLID tail
label only as a flagged audit field.

### P1 — Fix the two opposite English-collapse biases
**Why:** §8.4(2): GlotLID collapses European/Latin-script non-English → English (67/100); OpenLID collapses
romanized South-Asian → English (10/27). These are systematic and predictable.
**How:** (a) when GlotLID says `eng` but OpenLID gives a specific non-English Latin language and the script
is Latin, distrust the GlotLID `eng`; (b) when OpenLID says `eng` but the channel has romanized-Indic
signals (existing keyword diagnostics) or GlotLID says a South-Asian language, prefer the South-Asian label.
Both are channel-level reconciliation rules.

### P1 — Investigate and restore the `channel_description` (and `video_tags`) source columns
**Why:** the run classified 105,638 channels on titles + descriptions only, with channel descriptions
entirely absent (§2) — the richest metadata field, and exactly the field LLM judges relied on most.
**How:** confirm the column name in `yt_sl_channels`, add it to the detector's candidate list (or set
`channel_description_column`), and re-measure the unclassified rate.

### P2 — Down-weight the Latin channel *name* vs non-Latin video-title evidence
**Why:** non-Latin→English errors (SMALLROOM→Thai, MOSTCONTENTS→Korean) trace to the English brand name
outvoting non-Latin titles on sparse channels.
**How:** when title/description segments are predominantly non-Latin but the channel name is Latin, suppress
the channel-name vote or require corroboration.

### P2 — Make mixed/bilingual a first-class output, not a forced single label
**Why:** the gold column is heavily multi-label; much "disagreement" is two models each catching a real
language of a bilingual channel.
**How:** lean on the existing mixed-language machinery to emit primary+secondary for credible bilingual
channels instead of forcing one label and flagging the other as disagreement.

### P3 — Build the real gold set and expand validation
**Why:** every accuracy number rests on 50 gold rows + an LLM reference.
**How:** run `manual_validation_sample` (`create_validation_samples=true`) on the full bucket range, and/or
extend the multi-classifier adjudication across the random-300 once **clean channel IDs** are exported.
Stratify by archetype (§7).

### Process note — eliminate the screenshot bottleneck (data quality)
~30% of the validation sample was unreachable purely because channel IDs were OCR'd from screenshots (§8.5).
Export the residual sample directly from `dev_sean.matt.yt_lid_v3_channel_model_comparison` so IDs are exact;
this single change roughly triples the verifiable sample and unblocks everything downstream.

---

## Appendix — companion files in this folder

- `lid_v3_residual_disagreement_sample.csv` — residual-disagreement channels with model labels, archetype,
  and per-channel judgments from Codex, Gemini, Claude subagents, and the `channel_language` gold column.
  Living file.
- `llm_panel_classifier_prompt.md` — the rigorous independent-classifier prompt; shared spec for the
  three-LLM adjudication panel (Claude Opus / GPT-5.5 / Gemini Pro) in the P0 recommendation.
