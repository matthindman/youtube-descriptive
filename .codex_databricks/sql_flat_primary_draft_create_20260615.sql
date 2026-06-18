CREATE OR REPLACE TABLE dev_sean.matt.yt_channel_topic_flat_primary_draft_20260615 AS
WITH base AS (
  SELECT
    channel_id,
    split,
    category_status,
    topic_categories,
    topic_category_count
  FROM dev_sean.matt.yt_channel_topic_taxonomy_channel_labels_20260612
),
candidate_raw AS (
  SELECT channel_id, 'Video games' AS flat_label, 20 AS priority, 'video_game_specific' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE size(array_intersect(topic_categories, array(
    'Action_game',
    'Action-adventure_game',
    'Casual_game',
    'Music_video_game',
    'Puzzle_video_game',
    'Racing_video_game',
    'Role-playing_video_game',
    'Simulation_video_game',
    'Sports_game',
    'Strategy_video_game'
  ))) > 0

  UNION ALL
  SELECT channel_id, 'Music' AS flat_label, 10 AS priority, 'music_any' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE size(array_intersect(topic_categories, array(
    'Music',
    'Christian_music',
    'Classical_music',
    'Country_music',
    'Electronic_music',
    'Hip_hop_music',
    'Independent_music',
    'Jazz',
    'Music_of_Asia',
    'Music_of_Latin_America',
    'Pop_music',
    'Reggae',
    'Rhythm_and_blues',
    'Rock_music',
    'Soul_music'
  ))) > 0

  UNION ALL
  SELECT channel_id, 'Film/TV/Humor' AS flat_label, 135 AS priority, 'film_tv_humor_performing_arts_lump' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE size(array_intersect(topic_categories, array('Film', 'Television_program', 'Humour', 'Performing_arts'))) > 0

  UNION ALL
  SELECT channel_id, 'Sports' AS flat_label, 40 AS priority, 'sports_any' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE size(array_intersect(topic_categories, array(
    'Sport',
    'Association_football',
    'American_football',
    'Baseball',
    'Basketball',
    'Boxing',
    'Cricket',
    'Golf',
    'Ice_hockey',
    'Mixed_martial_arts',
    'Professional_wrestling',
    'Tennis',
    'Volleyball'
  ))) > 0

  UNION ALL
  SELECT channel_id, 'Religion' AS flat_label, 50 AS priority, 'religion' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Religion')

  UNION ALL
  SELECT channel_id, 'News/Society/Politics' AS flat_label, 60 AS priority, 'news_society_politics_lump' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE size(array_intersect(topic_categories, array('Politics', 'Military', 'Business', 'Society'))) > 0

  UNION ALL
  SELECT channel_id, 'Food' AS flat_label, 70 AS priority, 'food' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Food')

  UNION ALL
  SELECT channel_id, 'Health/Fitness' AS flat_label, 80 AS priority, 'health_fitness_lump' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE size(array_intersect(topic_categories, array('Health', 'Physical_fitness'))) > 0

  UNION ALL
  SELECT channel_id, 'Technology' AS flat_label, 90 AS priority, 'technology' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Technology')

  UNION ALL
  SELECT channel_id, 'Vehicles/Motorsport' AS flat_label, 35 AS priority, 'vehicles_motorsport_lump' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE size(array_intersect(topic_categories, array('Vehicle', 'Motorsport'))) > 0

  UNION ALL
  SELECT channel_id, 'Pets/Animals' AS flat_label, 110 AS priority, 'pet' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Pet')

  UNION ALL
  SELECT channel_id, 'Fashion/Beauty' AS flat_label, 120 AS priority, 'fashion_beauty_lump' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE size(array_intersect(topic_categories, array('Fashion', 'Physical_attractiveness'))) > 0

  UNION ALL
  SELECT channel_id, 'Travel' AS flat_label, 130 AS priority, 'tourism' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Tourism')

  UNION ALL
  SELECT channel_id, 'Education/Explainers' AS flat_label, 170 AS priority, 'knowledge_explainers' AS rule_id, 'specific' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Knowledge')

  UNION ALL
  SELECT channel_id, 'Hobby/General interests' AS flat_label, 900 AS priority, 'hobby_fallback' AS rule_id, 'fallback' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Hobby')

  UNION ALL
  SELECT channel_id, 'Lifestyle/General' AS flat_label, 960 AS priority, 'lifestyle_broad_only' AS rule_id, 'broad' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Lifestyle_(sociology)')

  UNION ALL
  SELECT channel_id, 'Video games' AS flat_label, 965 AS priority, 'video_game_culture_fallback' AS rule_id, 'fallback' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Video_game_culture')

  UNION ALL
  SELECT channel_id, 'Entertainment/General' AS flat_label, 970 AS priority, 'entertainment_broad_only' AS rule_id, 'broad' AS candidate_type
  FROM base
  WHERE array_contains(topic_categories, 'Entertainment')
),
candidates AS (
  SELECT DISTINCT channel_id, flat_label, priority, rule_id, candidate_type
  FROM candidate_raw
),
candidate_agg AS (
  SELECT
    channel_id,
    sort_array(collect_set(flat_label)) AS candidate_flat_labels,
    sort_array(filter(collect_set(CASE WHEN candidate_type = 'specific' THEN flat_label ELSE NULL END), x -> x IS NOT NULL)) AS specific_candidate_flat_labels,
    COUNT(DISTINCT flat_label) AS n_candidate_flat_labels,
    COUNT(DISTINCT CASE WHEN candidate_type = 'specific' THEN flat_label END) AS n_specific_candidate_flat_labels
  FROM candidates
  GROUP BY channel_id
),
ranked_candidates AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY channel_id ORDER BY priority ASC, flat_label ASC) AS rn
  FROM candidates
),
primary_candidate AS (
  SELECT
    channel_id,
    flat_label AS primary_flat_label,
    priority AS primary_priority,
    rule_id AS primary_rule_id,
    candidate_type AS primary_candidate_type
  FROM ranked_candidates
  WHERE rn = 1
)
SELECT
  b.channel_id,
  b.split,
  b.category_status,
  b.topic_categories,
  b.topic_category_count,
  COALESCE(p.primary_flat_label, 'Uncategorized') AS primary_flat_label,
  COALESCE(p.primary_priority, 9999) AS primary_priority,
  COALESCE(p.primary_rule_id, 'no_topic_category') AS primary_rule_id,
  COALESCE(p.primary_candidate_type, 'uncategorized') AS primary_candidate_type,
  COALESCE(a.candidate_flat_labels, array()) AS candidate_flat_labels,
  COALESCE(a.specific_candidate_flat_labels, array()) AS specific_candidate_flat_labels,
  COALESCE(a.n_candidate_flat_labels, 0) AS n_candidate_flat_labels,
  COALESCE(a.n_specific_candidate_flat_labels, 0) AS n_specific_candidate_flat_labels,
  array_contains(b.topic_categories, 'Hobby') AS has_hobby_label,
  array_contains(b.topic_categories, 'Hobby') AND COALESCE(a.n_specific_candidate_flat_labels, 0) = 0 AS hobby_is_standalone_or_broad_only,
  COALESCE(p.primary_flat_label, 'Uncategorized') = 'Hobby/General interests' AS assigned_to_hobby_fallback
FROM base b
LEFT JOIN candidate_agg a
  ON b.channel_id = a.channel_id
LEFT JOIN primary_candidate p
  ON b.channel_id = p.channel_id
