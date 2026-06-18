# Databricks notebook source
"""Export 4-week traffic deltas for the v2b treemap renderer."""

from pyspark.sql import functions as F


dbutils.widgets.text("projection_table", "dev_sean.matt.yt_channel_topic_projection_v2_20260617")
dbutils.widgets.text("channel_table", "prod_tads.youtube_too.yt_sl_channels")
dbutils.widgets.text("stats_table", "dev_sean.default.yt_channel_stats")
dbutils.widgets.text("output_path", "dbfs:/FileStore/youtube_topic_treemap_top_ocean_20260617_v2b/traffic_4wk.parquet")

PROJECTION_TABLE = dbutils.widgets.get("projection_table").strip()
CHANNEL_TABLE = dbutils.widgets.get("channel_table").strip()
STATS_TABLE = dbutils.widgets.get("stats_table").strip()
OUTPUT_PATH = dbutils.widgets.get("output_path").strip()

for table_name in [PROJECTION_TABLE, CHANNEL_TABLE, STATS_TABLE]:
    if not table_name:
        raise ValueError("All table parameters must be non-empty")

params = spark.sql(
    f"""
    WITH available_dates AS (
      SELECT DISTINCT DATE(collected_at) AS snapshot_date
      FROM {STATS_TABLE}
    )
    SELECT
      MAX(snapshot_date) AS current_date,
      MAX(CASE
        WHEN snapshot_date <= DATE_SUB((SELECT MAX(snapshot_date) FROM available_dates), 28)
        THEN snapshot_date
      END) AS prior_date
    FROM available_dates
    """
).first()

if params.current_date is None or params.prior_date is None:
    raise RuntimeError(f"Could not resolve current/prior snapshot dates from {STATS_TABLE}")

print(f"TRAFFIC CURRENT SNAPSHOT: {params.current_date}")
print(f"TRAFFIC PRIOR SNAPSHOT: {params.prior_date}")

traffic_df = spark.sql(
    f"""
    WITH stats_deduped AS (
      SELECT
        canonical_id,
        channel_name AS stats_channel_name,
        subscriber_count,
        total_view_count,
        collected_at,
        DATE(collected_at) AS snapshot_date,
        ROW_NUMBER() OVER (
          PARTITION BY canonical_id, DATE(collected_at)
          ORDER BY collected_at DESC
        ) AS rn
      FROM {STATS_TABLE}
      WHERE DATE(collected_at) IN (DATE('{params.current_date}'), DATE('{params.prior_date}'))
    ),
    current_stats AS (
      SELECT *
      FROM stats_deduped
      WHERE snapshot_date = DATE('{params.current_date}')
        AND rn = 1
    ),
    prior_stats AS (
      SELECT *
      FROM stats_deduped
      WHERE snapshot_date = DATE('{params.prior_date}')
        AND rn = 1
    ),
    traffic AS (
      SELECT
        c.canonical_id AS channel_id,
        c.stats_channel_name,
        c.subscriber_count AS current_subscriber_count,
        c.total_view_count AS current_lifetime_views,
        p.total_view_count AS prior_lifetime_views,
        c.collected_at AS current_collected_at,
        p.collected_at AS prior_collected_at,
        c.total_view_count - p.total_view_count AS raw_4wk_views,
        CASE
          WHEN p.total_view_count IS NULL THEN NULL
          WHEN c.total_view_count >= p.total_view_count THEN c.total_view_count - p.total_view_count
          ELSE NULL
        END AS view_count_4wk,
        CASE
          WHEN p.total_view_count IS NULL THEN NULL
          WHEN c.total_view_count >= p.total_view_count THEN (c.total_view_count - p.total_view_count) / 4.0
          ELSE NULL
        END AS avg_weekly_view_count
      FROM current_stats c
      LEFT JOIN prior_stats p
        ON c.canonical_id = p.canonical_id
    ),
    projection_channels AS (
      SELECT DISTINCT channel_id
      FROM {PROJECTION_TABLE}
    )
    SELECT
      yt.channel_id,
      COALESCE(yt.channel_name, t.stats_channel_name) AS channel_name,
      t.current_subscriber_count,
      t.current_lifetime_views,
      t.prior_lifetime_views,
      t.current_collected_at,
      t.prior_collected_at,
      t.raw_4wk_views,
      t.view_count_4wk,
      t.avg_weekly_view_count
    FROM projection_channels pc
    INNER JOIN {CHANNEL_TABLE} yt
      ON pc.channel_id = yt.channel_id
    LEFT JOIN traffic t
      ON yt.channel_id = t.channel_id
    """
)

summary = traffic_df.agg(
    F.count("*").alias("channels"),
    F.count(F.when(F.col("current_lifetime_views").isNotNull(), 1)).alias("channels_with_current"),
    F.count(F.when(F.col("prior_lifetime_views").isNotNull(), 1)).alias("channels_with_prior"),
    F.count(F.when(F.col("raw_4wk_views") < 0, 1)).alias("negative_raw_deltas"),
    F.count(F.when(F.col("view_count_4wk").isNotNull(), 1)).alias("channels_with_valid_4wk"),
    F.sum("view_count_4wk").alias("total_view_count_4wk"),
).first()

print(f"TRAFFIC CHANNELS: {summary.channels}")
print(f"TRAFFIC CHANNELS WITH CURRENT: {summary.channels_with_current}")
print(f"TRAFFIC CHANNELS WITH PRIOR: {summary.channels_with_prior}")
print(f"TRAFFIC NEGATIVE RAW DELTAS: {summary.negative_raw_deltas}")
print(f"TRAFFIC CHANNELS WITH VALID 4WK: {summary.channels_with_valid_4wk}")
print(f"TRAFFIC TOTAL 4WK VIEWS: {summary.total_view_count_4wk}")

traffic_df.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"TRAFFIC PARQUET: {OUTPUT_PATH}")
