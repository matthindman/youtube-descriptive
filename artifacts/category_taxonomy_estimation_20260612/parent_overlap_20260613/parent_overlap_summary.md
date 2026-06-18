# Parent Category Overlap Matrix

Date: 2026-06-13

Source tables:

- `dev_sean.matt.yt_channel_topic_taxonomy_channel_labels_20260612`
- `dev_sean.matt.yt_channel_topic_taxonomy_label_roles_20260612`

Denominator: 197,664 channels with nonempty topic-category arrays.

Included labels: parent-like labels with empirical role `empirical_parent` or `intermediate_or_crosscutting_parent` and prevalence greater than 2% of nonempty channels.

## Included Parent-Like Labels

| Display label | Raw label | Role | Channels | Prevalence |
|---|---|---|---:|---:|
| Lifestyle | `Lifestyle_(sociology)` | empirical parent | 95,264 | 48.19% |
| Entertainment | `Entertainment` | empirical parent | 68,261 | 34.53% |
| Music | `Music` | empirical parent | 43,599 | 22.06% |
| Video game | `Video_game_culture` | empirical parent | 24,436 | 12.36% |
| Pop music | `Pop_music` | intermediate/cross-cutting | 20,419 | 10.33% |
| Action game | `Action_game` | intermediate/cross-cutting | 15,525 | 7.85% |
| RPG | `Role-playing_video_game` | intermediate/cross-cutting | 14,984 | 7.58% |
| Sport | `Sport` | empirical parent | 7,377 | 3.73% |
| Vehicle | `Vehicle` | intermediate/cross-cutting | 6,008 | 3.04% |

## Joint Overlap Matrix

Cells are the percent of all nonempty channels that have both the row and column parent-like labels. Diagonal cells are single-label prevalence.

| Row / column | Lifestyle | Entertainment | Music | Video game | Pop music | Action game | RPG | Sport | Vehicle |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Lifestyle | 48.19 | 14.79 | 4.38 | 1.30 | 1.20 | 0.31 | 0.24 | 1.86 | 3.00 |
| Entertainment | 14.79 | 34.53 | 5.12 | 1.61 | 1.34 | 0.40 | 0.28 | 0.54 | 0.22 |
| Music | 4.38 | 5.12 | 22.06 | 0.27 | 10.28 | 0.09 | 0.07 | 0.07 | 0.05 |
| Video game | 1.30 | 1.61 | 0.27 | 12.36 | 0.05 | 7.85 | 7.58 | 0.10 | 0.09 |
| Pop music | 1.20 | 1.34 | 10.28 | 0.05 | 10.33 | 0.02 | 0.01 | 0.01 | <0.01 |
| Action game | 0.31 | 0.40 | 0.09 | 7.85 | 0.02 | 7.85 | 6.26 | <0.01 | 0.02 |
| RPG | 0.24 | 0.28 | 0.07 | 7.58 | 0.01 | 6.26 | 7.58 | <0.01 | 0.02 |
| Sport | 1.86 | 0.54 | 0.07 | 0.10 | 0.01 | <0.01 | <0.01 | 3.73 | 0.32 |
| Vehicle | 3.00 | 0.22 | 0.05 | 0.09 | <0.01 | 0.02 | 0.02 | 0.32 | 3.04 |

## Conditional Matrix

Cells are `P(column label present | row label present)`, in percent. This is useful because parent prevalences vary from about 3% to 48%.

| Row / column | Lifestyle | Entertainment | Music | Video game | Pop music | Action game | RPG | Sport | Vehicle |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Lifestyle | 100.0 | 30.7 | 9.1 | 2.7 | 2.5 | 0.6 | 0.5 | 3.9 | 6.2 |
| Entertainment | 42.8 | 100.0 | 14.8 | 4.7 | 3.9 | 1.1 | 0.8 | 1.6 | 0.6 |
| Music | 19.9 | 23.2 | 100.0 | 1.2 | 46.6 | 0.4 | 0.3 | 0.3 | 0.2 |
| Video game | 10.5 | 13.1 | 2.2 | 100.0 | 0.4 | 63.5 | 61.3 | 0.8 | 0.7 |
| Pop music | 11.6 | 13.0 | 99.5 | 0.5 | 100.0 | 0.2 | 0.1 | 0.1 | 0.0 |
| Action game | 3.9 | 5.0 | 1.2 | 99.9 | 0.2 | 100.0 | 79.7 | 0.1 | 0.3 |
| RPG | 3.1 | 3.7 | 0.9 | 100.0 | 0.2 | 82.6 | 100.0 | 0.1 | 0.2 |
| Sport | 50.0 | 14.5 | 1.9 | 2.7 | 0.3 | 0.2 | 0.1 | 100.0 | 8.5 |
| Vehicle | 98.6 | 7.2 | 1.5 | 3.0 | 0.1 | 0.6 | 0.5 | 10.5 | 100.0 |

## Lift Matrix

Cells are observed pair prevalence divided by expected pair prevalence under independence. Values above 1 indicate positive association after accounting for base rates.

| Row / column | Lifestyle | Entertainment | Music | Video game | Pop music | Action game | RPG | Sport | Vehicle |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Lifestyle | 2.07 | 0.89 | 0.41 | 0.22 | 0.24 | 0.08 | 0.07 | 1.04 | 2.05 |
| Entertainment | 0.89 | 2.90 | 0.67 | 0.38 | 0.38 | 0.15 | 0.11 | 0.42 | 0.21 |
| Music | 0.41 | 0.67 | 4.53 | 0.10 | 4.51 | 0.05 | 0.04 | 0.09 | 0.07 |
| Video game | 0.22 | 0.38 | 0.10 | 8.09 | 0.04 | 8.08 | 8.09 | 0.22 | 0.25 |
| Pop music | 0.24 | 0.38 | 4.51 | 0.04 | 9.68 | 0.02 | 0.02 | 0.03 | 0.01 |
| Action game | 0.08 | 0.15 | 0.05 | 8.08 | 0.02 | 12.73 | 10.51 | 0.03 | 0.08 |
| RPG | 0.07 | 0.11 | 0.04 | 8.09 | 0.02 | 10.51 | 13.19 | 0.02 | 0.07 |
| Sport | 1.04 | 0.42 | 0.09 | 0.22 | 0.03 | 0.03 | 0.02 | 26.79 | 2.81 |
| Vehicle | 2.05 | 0.21 | 0.07 | 0.25 | 0.01 | 0.08 | 0.07 | 2.81 | 32.90 |

## Main Patterns

Highest absolute joint overlaps:
- Lifestyle + Entertainment: 14.79% (29,231 channels)
- Entertainment + Lifestyle: 14.79% (29,231 channels)
- Pop music + Music: 10.28% (20,324 channels)
- Music + Pop music: 10.28% (20,324 channels)
- Video game + Action game: 7.85% (15,513 channels)
- Action game + Video game: 7.85% (15,513 channels)
- Video game + RPG: 7.58% (14,980 channels)
- RPG + Video game: 7.58% (14,980 channels)

Strongest conditional overlaps:
- RPG + Video game: 99.97% (14,980 channels)
- Action game + Video game: 99.92% (15,513 channels)
- Pop music + Music: 99.53% (20,324 channels)
- Vehicle + Lifestyle: 98.64% (5,926 channels)
- RPG + Action game: 82.57% (12,373 channels)
- Action game + RPG: 79.70% (12,373 channels)
- Video game + Action game: 63.48% (15,513 channels)
- Video game + RPG: 61.30% (14,980 channels)

Strongest prevalence-adjusted overlaps among pairs with at least 100 shared channels:
- RPG + Action game: 10.51x (12,373 channels)
- Action game + RPG: 10.51x (12,373 channels)
- RPG + Video game: 8.09x (14,980 channels)
- Video game + RPG: 8.09x (14,980 channels)
- Video game + Action game: 8.08x (15,513 channels)
- Action game + Video game: 8.08x (15,513 channels)
- Music + Pop music: 4.51x (20,324 channels)
- Pop music + Music: 4.51x (20,324 channels)

## Interpretation

The absolute joint-prevalence matrix is the direct answer to “what portion of channels have both parents?” It should not be the only view, because common labels such as Lifestyle and Entertainment dominate absolute overlap. The conditional matrix shows near-nesting patterns, such as Pop music under Music and game subtypes under Video game culture. The lift matrix separates real association from marginal prevalence and shows that some small absolute overlaps are meaningful relative to expectation, while many broad cross-domain overlaps are lower than expected.
