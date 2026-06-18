# Lumped parent overlap

Date: 2026-06-13

Denominator: 197,664 nonempty channels.

Groups are included when prevalence is greater than 2% of nonempty channel-label arrays. Music and video game labels are collapsed into `Music_combined` and `Video_game_combined`.

## Included Labels

| Display label | Raw/group label | Role | Channels | Prevalence |
|---|---|---|---:|---:|
| Lifestyle | `Lifestyle_(sociology)` | `direct_parent` | 95,264 | 48.19% |
| Entertainment | `Entertainment` | `direct_parent` | 68,261 | 34.53% |
| Music | `Music_combined` | `lumped_music` | 44,371 | 22.45% |
| Video game | `Video_game_combined` | `lumped_video_game` | 24,507 | 12.40% |
| Sport | `Sport` | `direct_parent` | 7,377 | 3.73% |
| Vehicle | `Vehicle` | `direct_parent_like` | 6,008 | 3.04% |

## Joint Overlap Matrix

Cells are the percent of the denominator that have both row and column labels. Diagonal cells are single-label prevalence.

| Row / column | Lifestyle | Entertainment | Music | Video game | Sport | Vehicle |
|---|---:|---:|---:|---:|---:|---:|
| Lifestyle | 48.19 | 14.79 | 4.63 | 1.32 | 1.86 | 3.00 |
| Entertainment | 14.79 | 34.53 | 5.41 | 1.63 | 0.54 | 0.22 |
| Music | 4.63 | 5.41 | 22.45 | 0.28 | 0.08 | 0.05 |
| Video game | 1.32 | 1.63 | 0.28 | 12.40 | 0.11 | 0.09 |
| Sport | 1.86 | 0.54 | 0.08 | 0.11 | 3.73 | 0.32 |
| Vehicle | 3.00 | 0.22 | 0.05 | 0.09 | 0.32 | 3.04 |

## Conditional Matrix

Cells are `P(column label present | row label present)`, in percent.

| Row / column | Lifestyle | Entertainment | Music | Video game | Sport | Vehicle |
|---|---:|---:|---:|---:|---:|---:|
| Lifestyle | 100.0 | 30.7 | 9.6 | 2.7 | 3.9 | 6.2 |
| Entertainment | 42.8 | 100.0 | 15.7 | 4.7 | 1.6 | 0.6 |
| Music | 20.6 | 24.1 | 100.0 | 1.2 | 0.3 | 0.2 |
| Video game | 10.6 | 13.1 | 2.2 | 100.0 | 0.9 | 0.8 |
| Sport | 50.0 | 14.5 | 2.1 | 3.0 | 100.0 | 8.5 |
| Vehicle | 98.6 | 7.2 | 1.5 | 3.1 | 10.5 | 100.0 |

## Lift Matrix

Cells are observed pair prevalence divided by expected pair prevalence under independence.

| Row / column | Lifestyle | Entertainment | Music | Video game | Sport | Vehicle |
|---|---:|---:|---:|---:|---:|---:|
| Lifestyle | 2.07 | 0.89 | 0.43 | 0.22 | 1.04 | 2.05 |
| Entertainment | 0.89 | 2.90 | 0.70 | 0.38 | 0.42 | 0.21 |
| Music | 0.43 | 0.70 | 4.45 | 0.10 | 0.09 | 0.07 |
| Video game | 0.22 | 0.38 | 0.10 | 8.07 | 0.24 | 0.25 |
| Sport | 1.04 | 0.42 | 0.09 | 0.24 | 26.79 | 2.81 |
| Vehicle | 2.05 | 0.21 | 0.07 | 0.25 | 2.81 | 32.90 |

## Main Patterns

Highest absolute joint overlaps:
- Lifestyle + Entertainment: 14.79% (29,231 channels)
- Entertainment + Music: 5.41% (10,693 channels)
- Lifestyle + Music: 4.63% (9,157 channels)
- Lifestyle + Vehicle: 3.00% (5,926 channels)
- Lifestyle + Sport: 1.86% (3,686 channels)
- Entertainment + Video game: 1.63% (3,219 channels)
- Lifestyle + Video game: 1.32% (2,602 channels)
- Entertainment + Sport: 0.54% (1,072 channels)
- Sport + Vehicle: 0.32% (630 channels)
- Music + Video game: 0.28% (550 channels)

Strongest conditional overlaps:
- Vehicle + Lifestyle: 98.64% (5,926 channels)
- Sport + Lifestyle: 49.97% (3,686 channels)
- Entertainment + Lifestyle: 42.82% (29,231 channels)
- Lifestyle + Entertainment: 30.68% (29,231 channels)
- Music + Entertainment: 24.10% (10,693 channels)
- Music + Lifestyle: 20.64% (9,157 channels)
- Entertainment + Music: 15.66% (10,693 channels)
- Sport + Entertainment: 14.53% (1,072 channels)
- Video game + Entertainment: 13.14% (3,219 channels)
- Video game + Lifestyle: 10.62% (2,602 channels)

Strongest prevalence-adjusted overlaps among pairs with at least 100 shared channels:
- Sport + Vehicle: 2.81x (630 channels)
- Lifestyle + Vehicle: 2.05x (5,926 channels)
- Lifestyle + Sport: 1.04x (3,686 channels)
- Lifestyle + Entertainment: 0.89x (29,231 channels)
- Entertainment + Music: 0.70x (10,693 channels)
- Lifestyle + Music: 0.43x (9,157 channels)
- Entertainment + Sport: 0.42x (1,072 channels)
- Entertainment + Video game: 0.38x (3,219 channels)
- Video game + Vehicle: 0.25x (184 channels)
- Video game + Sport: 0.24x (222 channels)
