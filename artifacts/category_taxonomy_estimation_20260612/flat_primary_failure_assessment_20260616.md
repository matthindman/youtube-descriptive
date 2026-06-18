# Honest Assessment of the Flat Topic Classification Approach

Date: 2026-06-16

This document is a blunt assessment of why the current flat topic classification work has not produced the level of agreement we hoped for. It is intended as handoff context for another model or analyst. The important framing is that the project goal is not to replace YouTube's labels with a human gold-standard taxonomy. The near-term goal is to understand, replicate, and eventually predict the current label sets present in our data. That said, much of our validation so far has compared a deterministic projection of YouTube labels to an LLM's blind, human-intuitive classification from channel evidence. Those are related tasks, but they are not the same task. That mismatch is probably the central problem.

## Overview of What We Did Overall

The work did not begin with the current flat-label tree. It evolved through several stages as we discovered that the category source was structurally different from what a simple single-label validation workflow assumed.

### 1. We corrected the task and found the right source table

The initial discussion mistakenly referred to notebook numbering and language classification. We corrected the scope to the LLM notebooks that classify by topic/genre, not language. We then identified the relevant category source as:

```text
dev_sean.default.channel_category
```

The important discovery was that `topic_categories` is an array. That changed the whole problem. The categories are not a single scalar label, and any code that treats them as one category, a primary category, or a simple string is wrong.

Once the colleague's table population job finished, we confirmed that the category table covered essentially the full `youtube_too` channel universe. The relevant universe used in the later analysis was:

- Total `youtube_too` channels: 202,985.
- Channels with nonempty topic category arrays: 197,664.
- Distinct exploded topic labels in the full universe: 62.

### 2. We validated the YouTube API field and normalized the data

We checked the YouTube API documentation for the channel `topicDetails.topicCategories[]` field. The field is a list of Wikipedia URLs describing channel content. We normalized those URL strings into slugs, for example:

```text
https://en.wikipedia.org/wiki/Music_of_Asia -> Music_of_Asia
```

The normalization steps were:

1. Use the latest available category row per channel.
2. Drop null and empty array entries.
3. Deduplicate labels within channel.
4. Convert Wikipedia URLs to compact slugs.
5. Preserve array position only for diagnostics, not as evidence of rank.

This was an essential fix. Earlier confusion about 57 or more categories came from treating the exploded topic slugs as if they should be a small set of mutually exclusive categories. In reality the full table had 62 observed labels, and the 1,000-case validation sample happened to contain 57 of them.

### 3. We established that the field is multi-label and only partly hierarchical

We analyzed category frequencies, label cardinality, array order, parent-child co-occurrence, and association rules. The key findings were:

- Most channels have multiple topic labels.
- The labels have strong parent-like closure patterns, especially for music, video games, sports, entertainment, lifestyle, and some society-related labels.
- The field is not a clean tree. Some labels are cross-cutting or intermediate.
- Array order is not reliable as a primary/secondary ranking.

The array-order finding was especially important. If order had been meaningful, we might have used the first label as the primary category. It was not. Parent-before-child rates were often near 50%, and many specific labels appeared first. So we ruled out "first label wins" as a sound strategy.

### 4. We estimated empirical taxonomy structure with a holdout split

We treated each channel's label array as a transaction and estimated association-rule patterns using support, confidence, lift, directional asymmetry, and Wilson lower bounds. We used an 80/20 deterministic split by `channel_id` hash:

- Train channels: 162,321.
- Heldout channels: 40,664.
- Train nonempty arrays: 158,084.
- Heldout nonempty arrays: 39,580.

The strongest parent-like rules were very stable on heldout data. Strong empirical parent edges had weighted train confidence around 0.992 and weighted heldout confidence around 0.992. This told us that the co-label structure is internally reliable. But it did not tell us that a content-based model can infer those labels from channel evidence.

This was an important distinction that we probably underweighted later. Internal co-label closure is easy to estimate from the labels themselves. Predicting the labels from channel text is a harder and different task.

### 5. We visualized overlap in the parent-like taxonomy

We built overlap matrices for parent-like labels and then revised them to handle obvious lumping:

- Music labels were combined into a single music family.
- Video game labels were combined into a single video game family.
- Parent overlap was shown as joint prevalence, conditional prevalence, and lift, because raw overlap is misleading when parent prevalences vary heavily.
- We then zoomed into Lifestyle and Entertainment children, because those broad labels had substantial overlap and obvious ambiguity.

This showed that the broad labels are not just frequent; they overlap with many other labels and are difficult to interpret without their children. It also reinforced the later decision to treat `Lifestyle_(sociology)`, `Entertainment`, and `Hobby` as broad/fallback labels rather than concrete flat categories whenever possible.

### 6. We ran a multi-label LLM validation against held-out YouTube label sets

Before the current single-flat-label work, we ran a 1,000-channel multi-label validation task. The goal there was closer to the original data structure: predict the observed YouTube `topic_categories` set from channel evidence, with the observed labels held out from prompts.

Important details:

- 1,000 random channels were sampled.
- 971 had nonempty observed category sets.
- The sample was split into 401 calibration channels and 599 heldout-test channels.
- The full observed 62-label vocabulary was used.
- Evaluation treated this as multi-label prediction, not one-label classification.
- Eight model result sets were parsed and scored.

The best heldout result came from calibrated probabilities, not the models' self-reported positive arrays. The best reported result was Gemini 3.5 Flash with per-label thresholding plus closure postprocessing:

- Exact set match: 0.220.
- Mean Jaccard: 0.589.
- Micro-F1: 0.713.
- Precision: 0.676.
- Recall: 0.754.

This was actually a useful result, but it also showed how hard exact label-set replication is. Even the best model only exactly matched the full set 22% of the time. Calibration and closure helped, and the best LLM roughly doubled a naive baseline, but broad and overlapping labels remained weak.

### 7. We repaired operational problems in the LLM pipeline

That multi-label run exposed several implementation problems that were fixed:

- Prompt/request artifacts now materialize before optional provider SDK work.
- Provider submission was reordered so slow direct providers run after asynchronous batch providers.
- Batch job rows are written incrementally after each provider submission.
- A Delta upsert bug that lazily read and overwrote the same table was fixed.
- OpenAI structured-output schemas were fixed by removing unsupported `uniqueItems`.
- Error inspection and retry notebooks were added for failed OpenAI jobs.
- Default model/provider sets were revised based on observed failures and stable completions.
- Scoring was shifted toward calibrated probabilities and empirical closure postprocessing.

These fixes improved run reliability. They did not solve the conceptual issue that the labels are multi-label, broad, and sometimes not inferable from text evidence.

### 8. We then moved from multi-label prediction to a project-defined flat primary label

The project then needed a single flat one-level categorization system, where every channel receives one label. That was a separate requirement from reproducing the raw YouTube category array.

We created a deterministic decision tree to project observed YouTube label arrays into one flat label. Major design choices included:

- Lump all music labels into `Music`.
- Lump all video game labels into `Video games`, while later treating broad `Video_game_culture` as a fallback.
- Lump Film, Television, Humour, and later Performing Arts into `Film/TV/Humor`.
- Lump Health and Physical Fitness into `Health/Fitness`.
- Treat Hobby as a fallback unless no more specific mapped topic is available.
- Treat broad Lifestyle and Entertainment as fallback/general labels.
- Make Music primary whenever present.
- Make Vehicles/Motorsport outrank Sports.
- Fold Politics/News, Military, Business, and Society/General into `News/Society/Politics`.
- Rename the Knowledge-derived flat category to `Education/Explainers`.

This tree was always a policy layer over YouTube labels. It was not discovered as the true category system.

### 9. We tested rule variants using the earlier heldout multi-label model outputs

Before the 1,000 blind subagent validation, we tested flat decision-tree variants against heldout model predictions from the multi-label run. The primary scorer was:

```text
gemini / gemini-3.5-flash / prob_label_threshold_closure_postprocessed
```

Across 599 heldout channels, flat accuracy was only about 64% for the best early variants. Rule changes mostly moved accuracy by tiny amounts:

- Prior draft: 63.77%.
- Music/Vehicle priority: 63.94%.
- Film over video game: 63.77%.
- Motorsport to vehicles: 63.94%.
- Other tested variants were similarly flat.

Mean accuracy across all model/prediction-variant combinations was lower, around 57-58%. This was an early warning that tree tweaks were not likely to produce a large jump.

### 10. We inspected Film/TV/Humor versus Video Games collisions

Because Film/TV/Humor and Video Games overlapped in visible ways, we sampled 100 channels with both kinds of labels and did a keyword-assisted inspection using channel names and recent video evidence.

The sample split roughly as:

- 27% mostly film/TV/humor/adaptation.
- 26% mostly video game play or discussion.
- 16% mixed or ambiguous.
- 31% insufficient keyword evidence.

This did not support a simple priority flip. It showed that the collision is real and heterogeneous. Some channels are game content; some are media content based on games; some are ambiguous; some cannot be resolved from the available snippets.

### 11. We ran a fresh 1,000-case blind subagent validation for the flat labels

Finally, we created a 1,000-channel validation set for the flat primary labels. Subagents received channel evidence but did not receive:

- YouTube topic category arrays.
- The deterministic tree output.
- The rule definitions.

The goal was to compare the tree's flat label to an independent LLM's intuitive single-label topic assignment. This produced the current 68.8% agreement result.

This was useful because it gave a direct sense of whether the flat labels are human-intuitive from channel evidence. But it also introduced a target mismatch: the subagent was not predicting the YouTube label set or the deterministic tree. It was making its own best single-label judgment.

### 12. We iteratively revised the flat tree based on errors and user policy decisions

The later revisions included:

- Moving Film/TV/Humor after more concrete topical labels.
- Treating broad `Video_game_culture` as a fallback.
- Assigning Motorsport to Vehicles/Motorsport.
- Folding Performing Arts into Film/TV/Humor.
- Folding Politics/News, Military, Business, and Society/General into News/Society/Politics.
- Renaming Knowledge-derived output to Education/Explainers.
- Auditing pairwise label collapses to see what would improve exact agreement.

The current best tested variant after all this is still 68.8%, with an older variant at 69.0%. The remaining improvements mostly come from qualitatively unattractive merges, such as Film/TV/Humor + Lifestyle/General or Film/TV/Humor + Music.

## What We Tried, In Short

We tried all of the following:

- Documentation validation against the YouTube API field.
- Correct array handling for `topic_categories`.
- Full-universe frequency analysis.
- Label cardinality analysis.
- Array-position diagnostics.
- Parent/child co-occurrence estimation.
- Train/heldout association-rule validation.
- Parent overlap matrices using joint prevalence, conditional prevalence, and lift.
- Lumping music labels and video game labels.
- Lifestyle/Entertainment child overlap analysis.
- Multi-label LLM prediction of the full observed category set.
- Calibration of LLM probability outputs.
- Empirical closure postprocessing for predicted label sets.
- Provider/run reliability fixes for the LLM notebooks.
- A deterministic flat primary decision tree.
- Multiple flat tree priority variants.
- Film/TV/Humor versus Video Games collision inspection.
- A 1,000-case blind subagent single-label validation.
- Residual error analysis by tree label, matched raw topic label, subagent confidence, and candidate ambiguity.
- Pair-merge diagnostics to test whether further lumps would materially improve agreement.

The important result is not that we failed to try enough small fixes. The important result is that the small fixes plateaued. The remaining problem is structural.

## Current State

The current deterministic flat-label tree is materialized in:

```text
dev_sean.matt.yt_channel_topic_flat_primary_draft_20260615
```

The current validation artifacts are in:

```text
artifacts/category_taxonomy_estimation_20260612/flat_primary_subagent_validation_20260615/
artifacts/category_taxonomy_estimation_20260612/flat_primary_draft_20260615/
```

The latest 1,000-case blind LLM/subagent validation result is:

- Exact agreement between tree label and subagent label: 68.8% (688/1,000).
- Best tested decision-tree variant: 69.0%.
- Best tested improvement over the current tree: only +0.2 percentage points.
- Improvement over the earlier v1 tree: +1.2 percentage points.

This is not a catastrophic result for a noisy forced single-label task, but it is not strong enough to treat the current method as a reliable primary-topic classifier. The plateau across variants is especially concerning: after several plausible fixes, accuracy barely moves.

## The Core Conceptual Problem

We are trying to force three different objects into one slot:

1. YouTube's topic category array.
2. A single flat "main topic" label.
3. An LLM's intuitive reading of channel evidence.

Those objects do not naturally coincide.

The YouTube table returns arrays. The observed arrays often include broad parent-like labels and narrower child-like labels together. They are not a single mutually exclusive category. They are not obviously ordered by primary versus secondary topic. The first item is not reliable enough to use as "the" category. Treating this as a one-label classification problem is a lossy conversion before we even begin.

The LLM validation also does not directly validate our ability to reproduce YouTube's labels. It validates whether the flat label derived from YouTube's array matches what a model would infer from recent channel evidence. If YouTube labels a channel as `Music` because it contains music videos, but a human or LLM sees the channel as film, celebrity, religion, or lifestyle content, the LLM disagreement may say more about YouTube's labeling policy than our tree. Conversely, the LLM may simply be wrong or under-informed.

This means our current 68.8% agreement is a hybrid statistic. It is partly a measure of decision-tree quality, partly a measure of YouTube-label intuitiveness, partly a measure of LLM classification quality, and partly a measure of evidence sufficiency. It should not be interpreted as clean accuracy against ground truth.

## Decision Tree Failures

### 1. The tree converts a multi-label ontology into a single label

The source structure is multi-label. In the full nonempty population, only 35.3% of channels have exactly one mapped candidate flat label. Most have multiple mapped candidates:

| Mapped candidate labels | Share of nonempty channels |
|---:|---:|
| 1 | 35.3% |
| 2 | 40.4% |
| 3 | 19.9% |
| 4+ | 4.4% |

So most channels require a tie-break. The decision tree is therefore not just mapping labels; it is imposing a primary-topic policy on data that usually does not declare a primary topic.

### 2. The broad fallback categories are very weak

The most obvious failures are the broad/fallback labels. In the 1,000-case validation:

| Tree label | Cases | Agreement |
|---|---:|---:|
| Lifestyle/General | 94 | 28.7% |
| Entertainment/General | 19 | 26.3% |
| Hobby/General interests | 57 | 57.9% |
| Education/Explainers | 19 | 63.2% |

`Lifestyle_(sociology)`, `Entertainment`, `Hobby`, and `Knowledge` are not good flat end-user categories. They often function as broad parents or catch-alls. The current tree handles them as fallbacks, which is better than treating them as concrete labels, but that does not solve the underlying problem: many channels do not expose a more specific topic in the YouTube category array.

In the full nonempty population:

- 18.6% have zero specific candidate labels after excluding broad/fallback categories.
- 10.5% are assigned to `Lifestyle/General`.
- 5.5% are assigned to `Hobby/General interests`.
- 1.9% are assigned to `Entertainment/General`.
- 0.7% are assigned to broad `Video_game_culture` fallback.

This is not a small edge case. A large fraction of channels are being assigned from weak evidence.

### 3. The tree bakes in arbitrary priority decisions

The tree necessarily chooses one label when multiple candidate labels are present. Some current policy decisions are sensible, but still arbitrary:

- Music is primary whenever present.
- Specific video game genre labels outrank most other labels.
- `Vehicles/Motorsport` outranks `Sports`.
- `Film/TV/Humor` is delayed until after more concrete topical labels.
- Broad `Video_game_culture` is delayed as a fallback.
- `Hobby` is only used when no better topic is available.

These choices improved a few cases, but the validation shows that they mostly trade errors around. The current tree changed 44 assignments relative to the original v1 tree: 25 improved, 13 regressed, and 6 remained wrong under a different label. That is not a strong signal of a clean rule improvement. It is a sign that the problem is structurally ambiguous.

### 4. The biggest conflict pairs are not cleanly resolvable by a tree

The most common full-population candidate conflicts include:

| Candidate pair | Channels | Share of nonempty channels |
|---|---:|---:|
| Film/TV/Humor + Music | 7,217 | 3.65% |
| News/Society/Politics + Religion | 6,967 | 3.52% |
| Education/Explainers + News/Society/Politics | 2,950 | 1.49% |
| Music + Religion | 2,867 | 1.45% |
| Film/TV/Humor + News/Society/Politics | 2,388 | 1.21% |
| Music + News/Society/Politics | 2,038 | 1.03% |

These are not accidental edge cases. They reflect real multi-topic channels and/or YouTube's broad tagging. A single ordered rule list can only choose one side. That choice may be reasonable on average but will be visibly wrong in many individual cases.

### 5. The categories themselves do not have equal semantic granularity

The proposed flat labels mix different levels of abstraction:

- `Music`, `Video games`, and `Sports` are relatively concrete domains.
- `Lifestyle/General`, `Entertainment/General`, `Hobby/General interests`, and `Education/Explainers` are broad residual concepts.
- `News/Society/Politics` is an intentionally broad lump combining politics, news, military, business, and society.
- `Film/TV/Humor` mixes medium, format, and tone.
- `Health/Fitness` mixes medical/health content with exercise and bodybuilding.

This unevenness makes exact agreement harder. An LLM can often identify a more specific intuitive topic, but the tree may only have a broad residual category available. Or the tree may choose a concrete label from the YouTube array that does not feel like the channel's main topic.

### 6. Some accuracy gains require qualitatively bad lumping

Pair-merge diagnostics show that collapsing some labels would improve agreement:

| Collapse | Apparent agreement gain |
|---|---:|
| Film/TV/Humor + Lifestyle/General | +2.7 pp |
| Film/TV/Humor + Music | +2.7 pp |
| Entertainment/General + Lifestyle/General | +1.9 pp |
| Entertainment/General + Film/TV/Humor | +1.6 pp |
| Fashion/Beauty + Lifestyle/General | +1.2 pp |
| Film/TV/Humor + Video games | +1.1 pp |
| Food + Lifestyle/General | +1.0 pp |

These are not necessarily good taxonomy changes. Some are broad collapses that would make the categories less intuitive and less useful. The fact that the easiest accuracy gains come from ugly merges is itself evidence that the single-flat-label target is under strain.

### 7. Rare-label estimates are unstable

Some labels have very small validation counts. For example, `Travel` has only 5 cases in the 1,000-case sample. Its 40% agreement estimate is not stable. Even labels with 15 to 35 cases have wide uncertainty. The current sample is useful for finding major failure modes, but it is not enough to make high-confidence per-label judgments for every category.

## LLM-Aided Classification Failures

### 1. The LLM was not validating the exact target we care about

The LLM was asked to classify channels blind from evidence. It was not asked to predict YouTube's full category array. It did not see the YouTube labels, and it did not know the deterministic tree output. That blindness was intentional and useful for avoiding leakage, but it means the LLM output is an intuitive channel-topic judgment, not a direct prediction of the existing YouTube labels.

If the project goal is to reproduce current YouTube labels, then this is a target mismatch. The LLM may disagree with YouTube in ways that are perfectly reasonable. We should not automatically treat those disagreements as tree failures.

### 2. The LLM was forced into a single-label answer

The source labels are multi-label. The LLM was asked to choose one flat category. Many channels are genuinely multi-topic: religious music, political comedy, gaming film commentary, fitness sports, food lifestyle, vehicle sports, educational news, etc. A forced single-label LLM judgment throws away uncertainty and secondary topics in the same way the decision tree does.

The current validation therefore compares two lossy projections:

- YouTube multi-label array -> deterministic one-label tree output.
- Channel evidence -> LLM one-label judgment.

Two lossy projections will disagree even when both contain useful signal.

### 3. The LLM evidence was incomplete

The subagents received channel id, channel name, language code, rough record count, and recent video title/description snippets. They did not receive full channel histories, thumbnails, video transcripts, playlists, channel About text, structured video categories, or external context unless inferable from names/titles.

This matters because YouTube's topic labels may be based on a broader and different evidence base than our snippets. A channel's recent videos may not represent its long-run topic. Descriptions can be truncated, boilerplate-heavy, multilingual, promotional, or missing. Some titles are too short or culturally specific for confident classification.

### 4. Medium and low LLM confidence cases are very unreliable

Agreement by subagent confidence:

| Subagent confidence | Cases | Agreement |
|---|---:|---:|
| High | 713 | 81.9% |
| Medium | 256 | 37.9% |
| Low | 31 | 22.6% |

This is a major warning. The model's confidence signal is informative: the lower-confidence cases are where the LLM and tree diverge heavily. But it also means a single-pass LLM label is not a stable validation target for ambiguous channels. We need multiple judgments, adjudication, or probabilistic labels for these cases.

### 5. Broad-only and ambiguous candidate cases are much worse

Agreement by number of specific candidate labels:

| Specific candidate labels | Cases | Agreement |
|---:|---:|---:|
| 0 | 178 | 40.4% |
| 1 | 674 | 79.2% |
| 2 | 145 | 55.9% |
| 3 | 3 | 33.3% |

The best-performing cases are those with exactly one specific YouTube-derived candidate. The worst cases are broad-only or multi-candidate. This strongly suggests that the main problem is not just "the tree order is wrong." The underlying label evidence is weak or ambiguous in the cases where we most need help.

### 6. The LLM may classify "what the channel seems to be" rather than "what YouTube would label"

The LLM naturally tends toward an intuitive main-topic judgment. That may conflict with YouTube's category semantics. Examples of likely mismatch:

- Music labels attached to film/TV/celebrity or religious content.
- Film/TV/Humor labels attached to channels whose videos discuss politics, video games, or lifestyle.
- `Lifestyle_(sociology)` used as a broad parent where the LLM sees fashion, film, entertainment, food, or news.
- `Knowledge`/`Education/Explainers` used where the LLM sees news, society, health, or technology.
- `Health/Fitness` assigned by YouTube where the LLM sees competitive sport or athlete coverage.

These disagreements may be useful for understanding YouTube's labels, but they are not all model failures.

### 7. We have not measured LLM reliability independently

The current validation appears to use one subagent label per case. We do not have inter-rater agreement among multiple LLMs on the same channels, nor human adjudication. Without that, we cannot separate:

- tree error,
- YouTube label oddity,
- LLM error,
- ambiguous channel,
- insufficient evidence,
- taxonomy boundary ambiguity.

The LLM classifications are useful, but they should not be treated as ground truth.

### 8. Multilingual and culturally specific channels are likely harder

The sample includes non-English channels. The LLM can handle many languages, but performance is not uniform. Titles and descriptions may include slang, transliteration, named entities, local formats, or short snippets that are hard to interpret without cultural context. This likely creates both random error and systematic bias toward broad labels.

### 9. Label names influence the LLM

The flat labels are human-readable, but some are vague or unattractive choices:

- `Lifestyle/General`
- `Entertainment/General`
- `Hobby/General interests`
- `Education/Explainers`
- `News/Society/Politics`

The LLM may avoid broad "General" labels when it sees a more specific intuitive topic, or overuse them when uncertain. The label wording itself can affect validation outcomes.

## Why Agreement Has Plateaued

The failure to move past roughly 69% agreement is probably not one bug. It is likely the combined result of several ceilings:

1. The source YouTube category arrays are multi-label, not one-label.
2. The arrays mix broad parent-like labels and narrower child-like labels.
3. There is no reliable primary-label ordering in the arrays.
4. Many channels have broad-only labels that do not expose an intuitive main topic.
5. The decision tree can only use the labels, not the actual content evidence.
6. The LLM can use content evidence, but it is not predicting the same target as the tree.
7. Forced single-label classification is inappropriate for many channels.
8. Some intuitive accuracy gains would require taxonomic collapses we probably do not want.
9. The validation sample is too small for stable rare-label conclusions.
10. We lack independent human or multi-model adjudication to distinguish tree errors from LLM errors.

The numbers support this interpretation. Cases with one specific candidate are relatively strong at 79.2% agreement. High-confidence LLM cases are also strong at 81.9%. The failures concentrate where the source labels are broad, ambiguous, or multi-candidate, and where the LLM itself signals uncertainty.

## What We Should Not Conclude

We should not conclude that YouTube's labels are useless. They clearly contain signal.

We should not conclude that the LLM is bad at channel classification. It may often be making reasonable intuitive judgments that simply do not match YouTube's category arrays.

We should not conclude that the decision tree is pointless. It is a transparent, reproducible projection from YouTube labels to a single flat label. But it should be understood as a policy choice, not as a recovered ground truth.

We should not conclude that another small rule tweak will solve the problem. The tested variants show diminishing returns. The remaining disagreements are structural.

## Most Likely Failure Modes by Category

### Lifestyle/General

This is the clearest problem label. It accounts for 94 validation cases and only 28.7% agreement. In residual errors, `Lifestyle_(sociology)` alone accounts for 67 errors. This is not a reliable primary category. It is a broad parent/fallback that hides more specific intuitive topics.

### Entertainment/General

This is also weak, with 26.3% agreement in the validation sample. It overlaps heavily with film, TV, humor, music, lifestyle, and general creator content. It is probably more useful as a fallback flag than as a final category.

### Hobby/General interests

This is better than Lifestyle/General but still weak. It is a catch-all. We correctly moved Hobby later in the tree, but standalone Hobby remains hard to interpret.

### Music

Music is large and mostly useful, but it creates many high-impact conflicts. The top error pair is `Music` assigned by the tree but `Film/TV/Humor` assigned by the subagent (27 cases). Music also conflicts with entertainment, religion, lifestyle, and politics/news. The current policy "music is primary whenever present" is transparent, but it is not always intuitive.

### Film/TV/Humor

This label is relatively strong overall at 79.9% in the validation sample, but it overlaps with music, video games, entertainment, lifestyle, and politics. The label combines medium and tone, which makes some edge cases inevitable.

### Education/Explainers

Renaming from Knowledge was the right move semantically, but the underlying `Knowledge` label remains broad. It overlaps with news/society, health, technology, and general fact channels. The rename improves readability, not source-label specificity.

### Health/Fitness and Sports

Fitness, bodybuilding, combat sports, athlete news, and sports science blur together. Some channels look like sports to the LLM but are tagged Health/Fitness by the tree, or vice versa.

### Vehicles/Motorsport and Sports

Moving motorsport to Vehicles/Motorsport is conceptually defensible, but there will always be overlap. Motorsport is both a vehicle topic and a sport topic.

## Recommendations for the Next Phase

### 1. Split the work into two explicit tasks

Task A: Predict YouTube's current category label set from channel evidence.

Task B: Define and assign one intuitive flat primary topic for analysis.

These are different tasks. We should stop using one as a proxy for the other without explicitly modeling the mismatch.

### 2. For YouTube replication, use multi-label prediction metrics

If the goal is to reproduce the current labels, evaluate set prediction:

- exact set match,
- Jaccard similarity,
- micro precision/recall/F1,
- macro precision/recall/F1,
- per-label precision/recall,
- parent-level partial credit,
- calibration curves for label probabilities.

Do not force everything through a single primary label during this stage.

### 3. Ask LLMs to predict label sets, not only one label

The LLM should be asked for:

- all plausible YouTube-derived flat labels,
- a primary label only as a separate field,
- confidence per label,
- evidence for and against each label,
- uncertainty/ambiguity flags,
- whether the case is broad-only or genuinely multi-topic.

This aligns the LLM task with the array structure.

### 4. Use multiple independent labels per validation case

For a serious validation set, use at least two independent LLM calls or models per case, ideally plus human adjudication for disagreements. Measure inter-rater agreement. Do not treat one LLM pass as ground truth.

### 5. Stratify validation around the known hard cases

Oversample:

- broad-only cases with zero specific candidates,
- multiple-candidate cases,
- `Lifestyle/General`,
- `Entertainment/General`,
- `Hobby/General interests`,
- `Education/Explainers`,
- high-conflict pairs like Music + Film/TV/Humor and Religion + News/Society/Politics.

The current random sample finds global accuracy, but it is inefficient for diagnosing the failures that matter most.

### 6. Use a supervised model for YouTube-label replication

If the target is YouTube's existing labels, a supervised multi-label model is probably more appropriate than a hand-built tree. Inputs could include:

- channel title/name,
- channel description/about text,
- recent video titles/descriptions,
- historical video titles,
- language,
- view/subscriber/video-count features if available,
- embeddings of titles/descriptions,
- raw YouTube topic labels where the model is learning the flat projection.

The output should be label probabilities with calibrated thresholds. The deterministic tree can still be used as a transparent baseline and as a downstream policy layer.

### 7. Treat the flat decision tree as a policy layer

If we need one final label for analysis, define the tree as a policy choice:

- document the priority order,
- keep broad fallback flags,
- expose secondary candidate labels,
- keep uncertainty/ambiguity features,
- do not pretend the flat label fully represents the source label set.

Downstream analyses should be able to filter or sensitivity-test broad fallback labels.

### 8. Keep the raw and derived labels together

Every flat label should be stored with:

- raw topic category array,
- mapped candidate labels,
- specific candidate labels,
- number of candidates,
- primary rule id,
- whether the assignment came from a broad fallback,
- ambiguity/conflict flags.

This prevents analysts from treating a high-confidence specific label and a broad fallback label as equally informative.

### 9. Consider a hierarchy instead of one flat level

A single one-level system may be too blunt. A two-stage output might work better:

1. Broad parent family.
2. Specific child label when available.

For example, `Entertainment -> Film/TV/Humor`, `Lifestyle -> Food`, `Society -> News/Society/Politics`, etc. Even if the final analysis needs one flat label, retaining a hierarchy would make the source structure more transparent.

### 10. Define an abstain or low-confidence path

Some channels should not receive a confident single primary topic from the available labels. We can still assign them to a fallback label for completeness, but we should mark them as low-confidence. The current system assigns 100% of nonempty rows to a label, which is operationally convenient but analytically misleading.

## Bottom Line

The current approach is useful as a transparent first-pass projection from YouTube's category arrays to a one-label taxonomy. It is not yet a strong validated classifier of intuitive channel topic.

The main failure is not a single bad rule. The main failure is treating a multi-label, weakly hierarchical, partially broad/catch-all YouTube taxonomy as if it naturally contains one ordered primary category. The LLM validation then compounds the issue by comparing that deterministic projection to a single intuitive judgment from incomplete evidence.

The path forward should separate YouTube-label replication from intuitive primary-topic classification. For replication, move to multi-label prediction and evaluate against held-out YouTube label sets. For intuitive topic classification, build a separate validation set with multiple coders or models and adjudication. The decision tree can remain useful, but it should be treated as a documented policy layer with uncertainty flags, not as a discovered truth about channel topics.
