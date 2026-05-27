# LID v3 — Implementation Plan for Validation-Driven Fixes

**Status:** Draft plan — 2026-05-26 (rev. after external review: added A4, B5, B6, D3, D4; revised B2, C1, D2)
**Code landed (compile-checked, untested on cluster):** D4 (all 5 fixes in `03_…py`), B1 (Arabic normalization
+ canonical-iso metric, `01_…py`), B5 (kept confident-tail + added audit count), B6 (within-cluster metric),
C1 *driver pass-through* (`01b_…py` widgets + `COMMON_LID_ARGS`). B2(a) is satisfied by `03` routing
English-vs-X disagreements to the panel.
**Landed behind DEFAULT-OFF feature flags (enable per-subset for scale-test data; no production impact while
off):** B3 (`b3_downweight_latin_name`), B4 (`b4_emit_bilingual_status`), B2(b)
(`b2b_prefer_romanized_indic_when_eng`). All in `01_…py`; gated so the production run is unchanged until
validated on collected subsets.
**Not yet implemented:** C1 *child* candidate-list update (blocked on A2's confirmed column name),
D2 `consensus_source` merge-back into `yt_lid_v3_channels`, D3 routing sizing.
**Derived from:** `REPORT_lid_v3_top_cohort_validation.md` §10
**Primary code:** `youtube_descriptive/src/01_language_openlid_v3_databricks.py` (child, 3763 lines),
`…/01b_language_lid_v3_subscriber_cohort_analysis_databricks.py` (driver),
`…/02_category_llm_youtube_databricks.py` (existing multi-LLM batch infra — reused for the panel)

---

## 0. Cost model (read first)

Each fix is tagged by how expensive it is to land:

- **[recompute]** — changes only channel-level consensus/aggregation; re-run from existing segment
  prediction tables. Minutes, no fastText inference, no GPU.
- **[re-aggregate]** — changes vote weighting; re-run aggregation from existing segment predictions. No inference.
- **[re-inference]** — changes the segment universe (new input columns); requires a full pipeline
  re-run (both fastText models over all segments). Hours + GPU.
- **[new component]** — net-new code (LLM panel, validation harness).

Sequence the cheap [recompute] wins first (validate on the *existing* run), then bundle the one
[re-inference] change into a single clean re-run, then layer the panel.

---

## Workstream A — Data unblocks & investigations (no pipeline code) [recompute/none]

These gate everything else; do them first.

### A1. Export the residual sample with exact channel_ids
**Problem:** ~30% of the validation sample is unverifiable because IDs were OCR'd from screenshots (§8.5).
**Do:** export `dev_sean.matt.yt_lid_v3_channel_model_comparison` (residual filter from report §7) to CSV
with exact `channel_id`s; replace the screenshot-derived IDs in `lid_v3_residual_disagreement_sample.csv`.
**Done when:** the LLM-reachability on a re-pull is ≥85% (vs ~70% now).

### A2. Confirm the `yt_sl_channels` description column name
**Problem:** `channel_description_col` resolved to `None` (§2); the run used titles+descriptions only.
**Do:** `DESCRIBE prod_tads.youtube.yt_sl_channels`; identify the real about/description column.
**Done when:** we know the exact column name (feeds B4).

### A3. Confirm provenance of the `channel_language` gold column
**Problem:** §9 — is it human gold or another tool's output? Determines whether §8.1 numbers are
"accuracy" or "agreement."
**Do:** ask the owner; annotate the report and CSV header accordingly.

### A4. Document the top-of-ocean cohort definition (mostly resolved)
**Status:** cohort identity/size **confirmed by owner** — this run is the *entire* "top of the ocean"
population (highest-subscriber channels down to a cutoff ≈ half of all platform-wide views); 105,638 is
the intended full set and `run_id=default` is correct. Not a 100k cohort.
**Remaining (documentation only):** record the exact subscriber-cutoff value and the basis for the
~50%-of-views estimate; note that the `random_*_subscriber_band` sample (below the cutoff) is the
separate, not-yet-run population. No longer a gating blocker.

---

## Workstream B — Consensus / aggregation fixes (no re-inference) [recompute / re-aggregate]

All of these recompute channel-level tables from the **existing** segment predictions. Validate on the
current `run_id=default` before any re-inference.

### B1. Normalize Arabic macro/dialect before agreement  — **P0**  [recompute]
**Where:** `01_…py` `compute_consensus()` (lines ~2204–2285) and the `models_agree_iso_primary`
column (line ~2411).
**Change:** add a `_canonical_iso(iso)` map collapsing the Arabic family
(`ara/arb/ary/arz/apc/ars/ajp/aeb/acm/acq/aec/afb/ayl/ayn` → `ara`) and apply it at:
- the exact/iso comparison branches in `compute_consensus` (lines 2248, 2278);
- the `models_agree_iso_primary` derivation (line 2411).
Keep the original dialect-specific model labels/ISO codes in the existing `openlid_*` / `glotlid_*`
audit fields; never overwrite them with the canonical macro label.
Chinese (`cmn_Hant/Hans/Hani`) already shares iso `cmn`, so it needs no new map — verify it lands in
`iso_or_script_variant_agreement`.
**Acceptance:** the within-Arabic NULL-consensus query (report §5) returns ≈0 (was 4,526);
`model_disagreement_needs_review` drops by ~4.5k; ISO agreement rate rises.

### B2. Opposite English-collapse — routing features, NOT auto-resolve tie-breaks  — **P1**  [recompute]
**✅ B2(b) landed behind `b2b_prefer_romanized_indic_when_eng` (default off)** — a post-consensus override
in the final channels assembly: when `consensus_language_iso639_3 == 'eng'` AND (Devanagari metadata OR
`romanized_indic_keyword_count >= b2b_min_romanized_keywords`) AND a model's primary is South-Asian, the
consensus label/iso/script are replaced with that South-Asian label and `b2b_romanized_indic_override` is
flagged. B2(a) remains panel-routing (handled by `03`).
**Revised after external review.** The report's core finding is that neither model is reliably right on
residual cases, so a broad "prefer model X" tie-break is unsafe — e.g. a "when `gl_iso=='eng'` and `ol`
is a specific Latin language → take OpenLID" rule would mislabel the truth-*is*-English cases (§8.4 had
27 such), and nothing in the rule distinguishes them.
**Where:** `compute_consensus()` disagreement branch (lines ~2276–2285); panel routing in D1.
**Change:**
- (a) **GlotLID→English over-collapse** (`gl_iso=='eng'`, `ol_iso` a specific non-English Latin language,
  both Latin): emit a **routing feature** → send to the LLM panel (D). Do **not** auto-pick OpenLID.
- (b) **OpenLID→English on romanized South-Asian** (`ol_iso=='eng'` AND GlotLID gives a South-Asian
  language): keep as a deterministic preference **only with strong corroborating signal** —
  Devanagari metadata OR `romanized_indic_keyword_count` above threshold (reuse `ROMANIZED_INDIC_*`,
  line ~2676); otherwise route to the panel.
**Acceptance:** re-score against `lid_v3_residual_disagreement_sample.csv` after A1/E2 (clean IDs +
expanded gold); the corroborated romanized-Indic rule must not regress truth-is-English cases, and the
European→English signal must improve panel-routed precision. **Do not enable any auto-resolve branch at
population scale until validated on >50 gold rows.**

### B3. Down-weight Latin channel-name vs non-Latin titles  — **P2**  [re-aggregate]
**✅ Landed behind `b3_downweight_latin_name` (default off)** in `build_admitted_votes_from_compact` —
zeros the channel-name vote when ≥`b3_min_nonname_segments` non-name segments are ≥`b3_nonlatin_share`
non-Latin and the name segment is Latin.
**Where:** channel aggregation vote weights (`aggregate_model`, line ~2111; weight table line ~1911;
`CHANNEL_NAME_WEIGHT` line 309).
**Change:** when a channel's title/description segments are predominantly non-Latin but `channel_name`
is Latin, zero/suppress the channel-name vote (or require corroboration) so the brand name can't flip a
non-Latin channel to English.
**Acceptance:** SMALLROOM (Thai), MOSTCONTENTS (Korean), FHERO (Thai) type cases resolve to the
non-Latin language (validate against the CSV).

### B4. Make mixed/bilingual a first-class output  — **P2**  [recompute]
**✅ Landed behind `b4_emit_bilingual_status` (default off)** — credible mixed channels get
`bilingual_primary_language_label` + `bilingual_secondary_language_label` (always populated, stable
schema) and, when the flag is on, `language_status = "bilingual"` instead of `mixed_language_candidate`.
**Where:** mixed-language block (lines ~2550–2620) + final channels assembly (`language_status`).
**Change:** for credible bilingual channels emit primary+secondary rather than forcing one label and
flagging the other as disagreement. Add a `bilingual` `language_status` (or populate
`secondary_language_label` on `classified`).
**Acceptance:** the genuinely-bilingual gold rows (`urd_Latn+pnb_Latn`, `kor_Hang+eng_Latn`,
`ara_Arab+fra_Latn`) come out as multi-label, not `needs_review`.

### B5. Suppress high-risk tail labels EXCEPT on confident mutual agreement  — **P1**  [recompute]
**Owner decision (refines external-review finding #2):** confident *mutual* agreement on a tail label
is **acceptable as a final label** — two independently-trained models both clearing `high_conf` on a
rare language is a genuine signal (real `gsw`/`srd`/`hmn` channels exist), and our "tail ~0% correct"
evidence was only for the *disagreement* archetype, not agreement. Keep the existing
`strong_high_risk_evidence` guard. Volume is minimal either way.
**Important caveat:** this is a deliberate policy deviation from the report's literal P1 recommendation
("never emit OpenLID minority-Romance tail labels"). It is acceptable only if we explicitly audit the
exact-agreement tail population rather than assuming the disagreement-sample result applies or does not
apply.
**Where:** `compute_consensus()` — exact-agreement branch (`01_…py:2248–2258`) vs the
disagreement/single-model paths (2261–2285).
**Change (scope-limited):** **keep** emitting the tail label as a classified final label when both models
agree *and* both clear `high_conf` (`consensus_status='high_risk_tail_exact_agreement'`,
`consensus_source='fasttext_tail_agreement'`, `requires_manual_adjudication=false`). Ensure every *other* tail occurrence
(disagreement, single-model, or low-confidence agreement) emits `consensus_language_label=NULL`, keeps
the tail label as an audit field, and routes to the panel (D). This is already the intent today;
B5 just verifies it and adds a test.
**Acceptance:** no `yt_lid_v3_channels` row has a `consensus_language_label` in
`HIGH_RISK_LATIN_TAIL_LABELS` **unless** it came from confident mutual agreement (record
`consensus_source = fasttext_tail_agreement`) or from `llm_panel | human_review`. Also produce a count
and small audit sample of confident mutual-agreement tail rows (including both model vote shares and sample
metadata text) so the exception is reviewable before publication.

### B6. Fix the misleading cluster-agreement metric  — **P2**  [recompute]
**Added after external review.** §4: the reported cluster agreement (14.23%) divides by all
both-primary channels, including the majority whose `analysis_cluster` is NULL — so it reads far lower
than real cluster agreement.
**Where:** cluster-agreement-rate QA (lines ~2450).
**Change:** compute `cluster_primary_agreement_rate` only over channels where **both** clusters are
non-null; OR rename the existing field (e.g. `cluster_coverage_x_agreement`) so it is not interpreted as
within-cluster agreement. Surface the within-cluster rate alongside the coverage.
**Acceptance:** the report's §4 cluster figure is replaced by a within-cluster rate with its own
denominator stated.

---

## Workstream C — Restore missing input columns (requires re-inference)  — **P1**  [re-inference]

### C1. Add `channel_description` (and `video_tags`) to the segment universe — child **and driver**
**Where:** child column detection (lines ~1013–1026); `channel_description_col` candidate list (line
~1016). **Driver** `COMMON_LID_ARGS` (`01b_…py:592`) and the cohort source-table builder.
**Change:**
- *Child:* using A2's confirmed name, add it to the candidate list or set the
  `channel_description_column` widget; same for `video_tags`.
- *Driver (added after external review):* `COMMON_LID_ARGS` currently passes only `channel_id_column`
  and does **not** forward `channel_description_column` / `video_tags_column` / the title/description
  overrides — so production cohort runs would not pick up the fix. Add driver widgets and pass them
  through, **and** ensure the cohort *source* tables the driver writes (`_cohort_source_table_names`,
  in `dev_sean.matt`) actually SELECT the description/tags columns through the projection.
- This adds segments → **forces a full re-inference** (both models re-score the enlarged universe).
**Sequencing:** bundle with the validated B-series changes into a **single clean re-run** ("v3.1") so we
pay the GPU cost once. Do **not** re-run for B alone (B is validated by recompute on the existing run).
**Acceptance:** `channel_description_col != None` in **both** a standalone child run and a
driver-orchestrated cohort run; segment-input QA on the **cohort source tables** (not just
`prod_tads.youtube.yt_sl_channels`) shows `channel_description` rows; `insufficient_text_or_unclassified`
drops below the current 2.1%.

---

## Workstream D — Three-LLM adjudication panel  — **P0**  [new component, reuses 02_category_llm]

The hardest *quality* lever and the biggest single win (§8.3). **Reuse the existing batch infra in
`02_category_llm_youtube_databricks.py`** — it already targets `gpt-5.5`, `claude-opus-4-7`, and
`gemini-3.1-pro-preview` with batch generation/submission/import and secret handling.

### D1. Panel notebook — `src/03_language_llm_panel_databricks.py`  ✅ scaffold created
**Run order:** run `01_…` first (writes the `yt_lid_v3_*` tables), then a colleague with LLM access runs
`03_…`. The companion does **not** re-run fastText; it reads `01`'s output and runs the panel **only on
disagreement + audit cases**. Batch-adapted: the panel judges from supplied metadata (channel name +
sampled titles/descriptions from `yt_lid_v3_segments_input`), since batch LLM APIs can't browse.
Routing widgets implement `route_disagreement` / `route_unresolved_tail` (excludes confident-agreement
tails) / `route_shared_bias_english_indic` (D3) / `route_agreement_audit` (E3, default 0.5%), with
`exclude_arabic_family_pairs` so the B1 taxonomy artifact isn't sent. Reuses notebook 02's batch-line
format, submission, and result-parsing helpers. Implemented guardrails include:
- **Routing input:** the ~5% core load from report §8.6 — non-Arabic `model_disagreement` +
  `high_risk_tail` *except the confident-mutual-agreement subset kept as final by B5* (minimal) +
  low-confidence/fallback buckets — read from `yt_lid_v3_channel_model_comparison` (after B1–B2 so
  Arabic/rule-resolved cases are already gone). Optionally add `mixed_language_candidate` and
  `insufficient_text` for the ~10% envelope.
- **Prompt:** `llm_panel_classifier_prompt.md` (this folder), per channel, with the channel's metadata.
- **Providers:** fork `02_category_llm`'s batch-file generation; swap the category prompt for the
  language prompt; keep the 3 frontier providers.
- **Reconciliation:** ≥2 valid classified votes agree → majority label (record split + rationales);
  3-way split → `needs_human_review`; `unreachable`/`insufficient_text` → abstain, decide on the rest.
**Important:** validate D4's acceptance checks on Databricks before promoting panel verdicts into the
final consensus table.

### D2. Integrate panel verdicts as a consensus tier
- Add/use a `consensus_source` field (`fasttext_agreement` | `fasttext_tail_agreement` |
  `taxonomy_normalized` | `reconciliation_rule` | `manual_adjudication_required` |
  `audit_sample` | `llm_panel` | `human_review`) and write the panel
  label into `consensus_language_label` for the routed channels.
- The verdict table must preserve the **full winning panel label**, not only base ISO: at minimum
  `panel_language_label`, `panel_language_iso639_3`, `panel_language_script`, `panel_secondary_language_label`,
  `panel_dialect_or_variant`, `panel_mixed_languages`, `panel_is_mixed_language`, `panel_confidence`,
  `panel_evidence`, vote split, and per-provider raw/parsed labels. This is required to distinguish
  e.g. `hin_Deva` vs `hin_Latn`, Arabic dialect notes, and
  bilingual verdicts.
- Blind audit rows (`route_reason='agreement_audit'`) are measurement rows by default: they should not
  overwrite `consensus_language_label` unless explicitly promoted after review. Non-audit routed rows can
  overwrite only when `panel_status='panel_majority'`; no-majority rows become `human_review`.
**Acceptance:** routed channels (~5%) receive a panel label; panel-majority vs `channel_language` gold
≥ the single-judge ~91% baseline (§8.1).

### D3. Shared-bias audit route — exact English agreement with contradicting Indic evidence  — **P0**
**Added after external review (the review's #1 / biggest gap).** D1's routing only pulls *disagreements*,
so it misses the report's §6 shared-bias case: `hi → eng` (307) — channels where **both** fastText
models agree on `eng_Latn` (landing in `exact_model_agreement`, the "clean 95%") yet are wrong because
the metadata is romanized/Devanagari Hindi-Indic. Agreement cannot catch a shared bias.
**Where:** D1 routing selection; reuse existing signals — `source_language_value`
(`SOURCE_INDIC_CODES`), Devanagari-metadata flag, `romanized_indic_keyword_count`, and the
`yt_lid_v3_hindi_indic_audit_candidates` table.
**Change:** route to the panel any channel where `consensus_language_iso639_3 == 'eng'` **AND** any
contradicting Indic signal is present (source code in `SOURCE_INDIC_CODES`, OR Devanagari metadata,
OR romanized-Indic keyword count above threshold). This is a **targeted audit route**, not "panel all
English" — gating on contradicting evidence keeps it to ~low thousands, not the ~45k eng channels.
**Sizing:** estimate the triggered count from existing tables before enabling; add to the §8.6 routing
budget (incremental, on top of the ~5% core).
**Acceptance:** the `hi→eng`-type population is sampled/adjudicated; panel disagreement rate on this
bucket is measured (quantifies how much false confidence the "clean 95%" was hiding).

### D4. Harden the panel notebook before production use  — **P0**  [new component]
**Added after code review of `03_language_llm_panel_databricks.py`.** The panel notebook now implements
the required correctness/idempotency guardrails; keep this checklist as the Databricks smoke-test target
before using panel verdicts as the adjudication layer.

**Implemented guardrails:**
- **Run-scope every auxiliary join.** In the D3 route, `yt_lid_v3_channel_text_features` must be filtered
  by the same `run_id` and `inference_hash_buckets` as the comparison table before joining. Do the same for
  any future joins to segments, Hindi/Indic audit, or panel results. Reject or dedupe if a join would create
  more than one row per `channel_id`.
- **Use richer prompt context.** Build panel prompts from all segment rows, not only `is_valid_text_for_lid`
  rows. Include `segment_type`, raw/truncated text, validity flag, `short_text_reason`, clean-letter count,
  and dominant script/share. This keeps channel names and short metadata available as weak evidence while
  still warning the model not to over-weight them.
- **Make request/result identity run-scoped.** Include `run_id` (and preferably a hash of provider/model/
  channel/route) in `request_id`, persist a request map keyed by `run_id, request_id`, and import only result
  files for the intended run. Raw results should carry `run_id` and join back to the request table; stale
  files under a shared `results_input_dir` must not affect the current run.
- **Preserve complete panel predictions.** Reconciliation must retain the winning full label/script,
  secondary/mixed fields, confidence, evidence, and per-provider votes. Majority voting can use base ISO
  for the primary decision, but the stored panel verdict must keep the full normalized label chosen by the
  majority side.
- **Add batch/job registries and idempotent writes.** Persist JSONL file paths, request counts, byte counts,
  provider file/batch IDs, provider statuses, submission status/errors, and import timestamps. Use
  run-scoped `replaceWhere` semantics for normal reruns, with a preserving schema-migration path for older
  panel tables so reruns do not silently erase prior runs.

**Acceptance:** a dry run with `submit_batches=false` writes scoped requests + batch registries with stable
IDs; importing a small hand-made results file produces one verdict per routed channel with full labels and
no stale-run joins; re-running the same `run_id` is idempotent.

---

## Workstream E — Validation harness & gold set  — **P3**  [new component]

### E1. Turn on the pipeline's own validation sampling
**Where:** `create_validation_samples` widget (line 194, default `false`); `manual_validation_sample`
output (line 133).
**Do:** run with `create_validation_samples=true` on the full bucket range to emit the stratified sample.

### E2. Standing multi-classifier validation
**Do:** generalize the CSV workflow (Codex/Gemini/subagent + panel) into a repeatable scorer keyed on
exact channel_ids (A1), stratified by archetype (§7); expand gold beyond 50 rows. Re-run on the
random-100k cohort when available.
**Acceptance:** a reproducible accuracy table (per classifier, per archetype) we can regenerate each run.

### E3. Blind audit sample of the AGREEMENT bucket — measure accuracy & surface unknown bias  — **P1**
**Why:** the report's central caveat is *agreement ≠ accuracy* — where both fastText models share a
bias they "agree" and silently inflate the "clean ~95%." D3 catches the *known* `hi→eng` shared bias by
targeting it; E3 is the **blind control**: a random sample of agreement cases adjudicated by the panel,
to (a) put an actual accuracy number (with CI) on the agreement bucket — today assumed high-trust but
never measured — and (b) surface shared biases we don't yet know to look for.
**Sizing:** **~0.5% of the agreement bucket** (`exact_model_agreement` ≈ 91,974 plus the
iso/cluster-resolved buckets) ≈ **~460–520 channels** — a small panel batch, additive to the ~5% core
+ D3 routing budget.
**How:** deterministic hash sample (reuse `xxhash64(channel_id | seed)`, like the pipeline's existing
sampling) over `consensus_status ∈ {exact_model_agreement, iso_or_script_variant_agreement,
cluster_model_agreement, taxonomy_normalized_agreement, high_risk_tail_exact_agreement}`;
run the D panel; compare panel majority vs the fastText consensus label.
Keep it **uniform-random** for an unbiased headline accuracy estimate; optionally add a small
non-Latin/non-English stratified slice for bias coverage. Tag these `audit_sample=true` so they are
flagged as a measurement, not a correction.
**Acceptance:** an estimated agreement-bucket accuracy with a confidence interval, and a ranked list of
systematic panel-vs-consensus mismatches (candidate shared biases) — each one a candidate new targeted
route in the D3 style.

---

## Sequencing & dependencies

```
A1 ─┬───────────────────────────────────────► D1/D3 (clean routing IDs)   E2 (clean scoring IDs)
A2 ─┼─► C1 (child+driver) ─┐
A3 ─┤                      │
A4 ─┘                      │
B1 ─► B2(routing) ─┬───────┼─► (validate on current run) ─► C1 bundled re-run "v3.1" ─► D1/D4/D2/D3 ─► E1/E2
B4 ────────────────┤       │
B5 ────────────────┤       │
B6 ────────────────┘       │
B3 ────────────────────────┘   (re-aggregate)
```

- **Sprint 1 (no re-inference, days):** A1–A4, B1, B2, B3, B4, B5, B6 — land and validate on the existing
  `run_id=default` via recompute. Biggest cheap win = B1 (recovers ~4.5k Arabic channels); B5/B6 are
  small correctness/reporting fixes.
- **Sprint 2 (one re-inference):** C1 (child **and** driver) bundled with the B-series → clean "v3.1" run.
- **Sprint 3 (panel):** D1 scaffold, **D4 hardening**, D2 integration, **D3** (targeted shared-bias
  English→Indic route) on the v3.1 residual; then E1/E2 and **E3** (blind ~0.5% agreement-bucket audit —
  the control that finally measures the "clean 95%").

## Risk notes
- **B2 is now routing-only by default** (per external review) — the one retained auto-resolve branch
  (corroborated romanized-Indic) and B3 are heuristics tuned on a ≤300-row sample; do not enable at
  population scale until re-validated against the expanded gold set (E2) and clean IDs (A1).
- **D3 and E3 add to the panel budget** beyond the ~5% core — D3 (targeted, size the
  contradicting-evidence trigger from existing tables first) and E3 (blind ~0.5% agreement audit,
  ~500 channels). Both are small and bounded, but budget them explicitly.
- **D3 vs E3 are complementary, not redundant:** D3 is a *targeted* route for a known bias (corrects
  those channels); E3 is a *blind random* control that measures the agreement bucket's true accuracy and
  discovers *unknown* biases. Keep E3 uniform-random so its accuracy estimate stays unbiased.
- **Panel verdicts are not authoritative until D4 is complete.** In particular, do not merge panel outputs
  back into `yt_lid_v3_channels` until request/result identity is run-scoped, auxiliary joins are scoped,
  full labels/scripts/evidence are preserved, and batch/job registries exist.
- C1 changes the input universe, so all downstream distributions shift — re-baseline the report's §3–§7
  numbers after v3.1; it must be verified on **driver-orchestrated** cohort runs, not just the child.
- A4 is resolved (cohort = entire top-of-ocean population, 105,638 channels); only the exact cutoff
  value remains to document. Findings apply to this top-of-ocean population; the lower-subscriber
  `random_*_subscriber_band` sample is a separate, not-yet-run population.
- D depends on per-provider batch quotas/cost; the ~5% core routing keeps base volume bounded (~5k/cohort),
  plus the D3 increment.
- Everything here is on the top cohort; re-confirm thresholds on the random-100k cohort (§9).
