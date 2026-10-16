# Coding instructions: India Hindi vs English validation set (india_lid_v1)

**Purpose.** The LID v3 pipeline labels about 507k India-declared channels `eng`, but many of them are Hindi or
Hinglish channels whose titles are written in English letters. `validation_sample.csv` holds 300 of these channels
(drawn from the 10,000-channel IndicLID pilot). Your codes tell us what is actually spoken in the videos and what
language the titles are written in, so we can measure how well IndicLID and the lexicon score recover Hindi.

## What you code

For each row, fill four columns of `validation_sample.csv`: `spoken_language`, `title_language`, `coder`, `notes`.

**Blind coding.** Before you start, hide the columns `indiclid_label`, `lexicon_score`, `original_lid_label`,
`stratum_label` and `stratum_views_tercile` (or work from a copy without them). Do not look up what the models said
until all rows are coded. The file is shuffled; row order carries no information.

### Step 1. `spoken_language` (the main code)

Open the `channel_url`, go to the Videos tab, and watch roughly the **first 30 seconds of 2 recent videos** (the
newest two uploads; skip a video that is a Short only if a longer upload is available). If the channel has no
speech in the videos you watch (pure music, pure visuals), use `unclear`. Judge the language people actually speak,
not the title, description or captions.

| Code | Use when |
|---|---|
| `hin` | The speech is Hindi (including Urdu-register Hindi that a Hindi speaker would call Hindi), with at most a few borrowed English words or terms (names, brands, technical words) of the kind any Hindi speaker uses. |
| `eng` | The speech is English with at most a few borrowed Hindi words or phrases ("yaar", "namaste", "bhai" as a greeting). Indian-accented English is still `eng`. |
| `hinglish` | The speech visibly mixes the two: full Hindi clauses and full English clauses in the same video, or Hindi sentence frames with English content words throughout beyond ordinary loanwords (for example "aaj hum bataenge ki ye phone kitna fast hai aur battery backup kaisa hai, overall performance is really good"). |
| `other` | A clear third language: another Indian language (Tamil, Bengali, Marathi, Punjabi, Telugu, and so on), Urdu clearly distinct from Hindi, Nepali, or any non-Indian language. Write the language in `notes`. |
| `unclear` | No speech, instrumental music only, speech too short or too noisy to judge, videos unavailable or removed, or the two videos disagree with no dominant language and you cannot decide. |

**How Hinglish is defined.** Hinglish is code-mixed speech, not a title style. Use it when, across what you hear in
the 30 seconds, neither language is clearly dominant: you hear complete clauses of both, or you hear Hindi grammar
(verbs, postpositions, particles such as "hai", "ka", "ki", "ke", "mein", "kya") carrying English content throughout.
Apply this rule of thumb: if you would say "mostly Hindi with some English words", code `hin`; "mostly English with
some Hindi words", code `eng`; if you hesitate between those two sentences, code `hinglish`.

**Two videos that differ.** If one video is Hindi and the other English, code `hinglish` and say so in `notes`. If
one video is clearly speech and the other is music or silent, code from the one with speech.

### Step 2. `title_language` (the written language of the titles)

Look only at the written text of the `recent_video_title_1..3` columns (and the channel title if the titles are
empty). Use the same labels, applied to the writing:

- `hin`: Hindi, in Devanagari or romanised ("Aaj ka mausam kaisa rahega").
- `eng`: English.
- `hinglish`: Hindi and English words mixed within the same titles, or some titles Hindi and some English.
- `other`: another language (note which).
- `unclear`: too short, only names, numbers or emoji, or no titles.

A romanised Hindi title is `hin` here. The point of this column is to separate "what the title looks like" from
"what the video sounds like" (a channel can have English titles and Hindi speech).

### Step 3. `coder` and `notes`

- `coder`: your initials, the same on every row you code.
- `notes`: free text, mandatory for `other` (which language) and for `unclear` (why). Also use it for anything odd:
  channel appears to be a re-upload farm, auto-generated voice, speech in a different language from the title, and
  so on. Do not paste personal information about people in the videos; refer to what you hear, not who they are.

## Practical rules

- Use only the public YouTube pages linked in `channel_url`. Do not log in, subscribe or comment.
- Spend no more than about 3 minutes per channel. If you cannot decide after two videos, code `unclear`.
- Code the **spoken language of the channel's current videos**, not of old uploads or the channel banner.
- Do not code any channel twice. If a row seems wrong (channel removed, IDs mismatch), code `unclear` with a note.
- Aim for a second independent coder on at least 60 rows (every fifth row) so agreement can be reported.

## After coding

Return the CSV unchanged apart from the four coder columns. The analysis compares `spoken_language` with the IndicLID
label and the lexicon score by stratum (`stratum_label` x `stratum_views_tercile`), reporting for each IndicLID class
the share of channels that are actually `hin`, `hinglish`, `eng`, `other` or `unclear`.
