WITH snap AS (
  SELECT s.canonical_id, s.collected_date,
         max_by(s.total_view_count, s.collected_at) AS views,
         max(s.collected_at) AS at
  FROM dev_sean.default.yt_channel_stats_full s
  JOIN dev_sean.matt.fig1_discovery_channels_v4 u ON u.canonical_id = s.canonical_id
  WHERE s.collected_date >= DATE'2026-06-01' AND dayofweek(s.collected_date) = 2
  GROUP BY 1, 2),
d AS (
  SELECT canonical_id, collected_date AS week_end, views,
         max(views) OVER (PARTITION BY canonical_id ORDER BY collected_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS hwm,
         (unix_timestamp(at) - unix_timestamp(lag(at) OVER w)) / 86400.0 AS days,
         datediff(collected_date, lag(collected_date) OVER w) AS gap
  FROM snap WINDOW w AS (PARTITION BY canonical_id ORDER BY collected_date)),
l AS (SELECT canonical_id, coalesce(language, 'und') AS language FROM dev_sean.matt.fig2_map_channels_v2)
SELECT d.week_end, l.language, count(*) AS n_channels,
       count_if(views < hwm) AS n_below_hwm,
       sum(greatest(views - hwm, 0) / days * 7) AS wv
FROM d JOIN l ON l.canonical_id = d.canonical_id
WHERE d.hwm IS NOT NULL AND d.gap = 7
GROUP BY 1, 2
