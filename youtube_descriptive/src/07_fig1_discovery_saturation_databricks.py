# Databricks notebook source
# MAGIC %md
# MAGIC # Figure 1 - discovery saturation, all channels vs active channels
# MAGIC
# MAGIC Builds the channel-level classification and the aggregate tables behind Figure 1 of
# MAGIC *Attention on YouTube*, in three variants (requirements doc, 2026-10-13):
# MAGIC
# MAGIC | variant | population | destination |
# MAGIC |---|---|---|
# MAGIC | `active_panel` | >=10k subs, newest upload within 365 days of the panel snapshot, Topic channels excluded | main Fig 1 |
# MAGIC | `all` | every channel with >=10k subs (zombies included) | ED Fig 1 |
# MAGIC | `active_crawl` | as `active_panel`, but the 365 days end at the snowball's last step (2026-03-04) | ED Fig 1 sensitivity |
# MAGIC
# MAGIC Each variant is aggregated by subscriber bins and by weekly-view bins.
# MAGIC
# MAGIC **Discovery.** Step 0 = the 262,761-channel seed list (`new_yt_top_list`); steps 1-13 =
# MAGIC unlabeled `pub_subs_batches` batches 1-13 (each seeded by the previous one). A channel's
# MAGIC step is its earliest; channels not reached by step 13 are assigned to their earliest-dated
# MAGIC later source (`later_sources` or `trending`), else `untracked`. Batch numbers collide across
# MAGIC sources, so every join keys on (`batch_source`, `batch_number`).
# MAGIC
# MAGIC **Judgment calls (all from the requirements doc's defaults; change via widgets):**
# MAGIC - Bins use subscribers at the current snapshot, not at discovery.
# MAGIC - Weekly views = elapsed-day-normalised lifetime-view delta between two Monday snapshots,
# MAGIC   floored at 0; channels missing from the prior snapshot get null (counted as 0 views in
# MAGIC   the view-weighted curves, excluded from the weekly-view bins).
# MAGIC - Activity comes from each channel's latest `yt_channel_videos` collection (last 10
# MAGIC   uploads). Channels absent from it are `unknown` and excluded from the active variants.
# MAGIC - Crawl anchor: a channel whose 10 stored uploads all post-date the anchor counts as active.
# MAGIC - Topic channels = channel name ending in " - Topic" (case-insensitive); excluded from both
# MAGIC   active variants by name.
# MAGIC - Undated discovery batches get a lower-bound date (unlabeled batch 21 -> 2026-04-30;
# MAGIC   `pub_subs_full` batch 2 -> 2026-03-11). This only affects ordering among later sources.

# COMMAND ----------
import os
import sys


def _create_text_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default, name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None and value != "" else default
    except Exception:
        return os.environ.get(name.upper(), default)


DEFAULTS = {
    "stats_table": "dev_sean.default.yt_channel_stats_full",
    "snapshot_current": "2026-10-05",
    "snapshot_prior": "2026-09-28",
    "subscriber_threshold": "10000",
    "panel_anchor": "2026-10-05",
    "crawl_anchor": "2026-03-04",
    "output_prefix": "dev_sean.matt.fig1_discovery",
    "version": "v1",
    "overwrite": "false",
    "figure_dir": "",
}
for k, v in DEFAULTS.items():
    _create_text_widget(k, v)
P = {k: _get_widget(k, v) for k, v in DEFAULTS.items()}

STATS = P["stats_table"]
CUR, PRI = P["snapshot_current"], P["snapshot_prior"]
THR = int(P["subscriber_threshold"])
PANEL_ANCHOR, CRAWL_ANCHOR = P["panel_anchor"], P["crawl_anchor"]
OVERWRITE = P["overwrite"].lower() == "true"


def out(name: str) -> str:
    return f"{P['output_prefix']}_{name}_{P['version']}"


OUTPUTS = ["channels", "exhaustion", "totals", "trending", "activity"]
print({**P, "outputs": [out(n) for n in OUTPUTS]})

# COMMAND ----------
# Pre-flight: never silently overwrite.
schema, prefix = P["output_prefix"].rsplit(".", 1)
existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {schema} LIKE '{prefix}_*'").collect()}
clash = [out(n) for n in OUTPUTS if out(n).rsplit(".", 1)[1] in existing]
if clash and not OVERWRITE:
    raise RuntimeError(f"Output tables already exist: {clash}. Set overwrite=true or bump version.")

# COMMAND ----------
# 1. Universe: >=THR subscribers at the current snapshot, with weekly views.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW universe AS
WITH cur AS (
  SELECT canonical_id,
         max_by(channel_name, collected_at)     AS channel_name,
         max_by(subscriber_count, collected_at) AS subscriber_count,
         max_by(total_view_count, collected_at) AS total_view_count,
         max(collected_at)                      AS collected_at
  FROM {STATS} WHERE collected_date = DATE'{CUR}'
  GROUP BY canonical_id
),
pri AS (
  SELECT canonical_id,
         max_by(total_view_count, collected_at) AS total_view_count_prior,
         max(collected_at)                      AS collected_at_prior
  FROM {STATS} WHERE collected_date = DATE'{PRI}'
  GROUP BY canonical_id
)
SELECT c.canonical_id, c.channel_name, c.subscriber_count,
       CASE WHEN p.canonical_id IS NULL THEN NULL
            ELSE greatest(0, c.total_view_count - p.total_view_count_prior)
                 / ((unix_timestamp(c.collected_at) - unix_timestamp(p.collected_at_prior)) / 86400.0) * 7
       END AS weekly_views,
       coalesce(lower(trim(c.channel_name)) LIKE '% - topic', false) AS is_topic
FROM cur c LEFT JOIN pri p USING (canonical_id)
WHERE c.subscriber_count >= {THR}
""")

# COMMAND ----------
# 2. First discovery per channel.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW first_discovery AS
WITH raw AS (
  SELECT canonical_id, coalesce(batch_source, '') AS batch_source, batch_number
  FROM dev_sean.default.new_channels
  UNION ALL
  SELECT canonical_id, coalesce(batch_source, '') AS batch_source, batch_number
  FROM dev_sean.default.new_threshold_channels
),
events AS (
  SELECT r.canonical_id,
         CASE WHEN r.batch_source = '' AND r.batch_number BETWEEN 1 AND 13 THEN 'snowball'
              WHEN r.batch_source = 'trending' THEN 'trending'
              ELSE 'later_sources' END AS source,
         CASE WHEN r.batch_source = '' AND r.batch_number BETWEEN 1 AND 13 THEN r.batch_number END AS step,
         CASE WHEN r.batch_source = '' THEN coalesce(to_date(pb.run_date), DATE'2026-04-30')
              WHEN r.batch_source = 'trending' THEN to_date(tb.run_date)
              WHEN r.batch_source = 'pub_subs_full' THEN DATE'2026-03-11'
              WHEN r.batch_source = 'threshold_yt_5k' THEN DATE'2026-03-14'
              WHEN r.batch_source = 'threshold_yt_1k' THEN DATE'2026-04-02'
         END AS disc_date
  FROM raw r
  JOIN universe u ON u.canonical_id = r.canonical_id
  LEFT JOIN dev_sean.default.pub_subs_batches pb
         ON r.batch_source = '' AND pb.batch_number = r.batch_number
  LEFT JOIN dev_sean.default.trending_batches tb
         ON r.batch_source = 'trending' AND tb.batch_number = r.batch_number
),
ranked AS (
  SELECT *, row_number() OVER (
           PARTITION BY canonical_id
           ORDER BY CASE WHEN step IS NOT NULL THEN 0 ELSE 1 END, step, disc_date,
                    CASE source WHEN 'trending' THEN 1 ELSE 0 END) AS rn
  FROM events
)
SELECT canonical_id, source, step, disc_date FROM ranked WHERE rn = 1
""")

# COMMAND ----------
# 3. Upload activity from each channel's latest yt_channel_videos collection.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW activity AS
WITH v AS (
  SELECT canonical_id, published_at
  FROM dev_sean.matt.yt_channel_videos
  QUALIFY collected_date = max(collected_date) OVER (PARTITION BY canonical_id)
)
SELECT canonical_id,
       max(published_at) AS newest_upload,
       min(published_at) AS oldest_stored_upload,
       count(*)          AS n_stored_uploads,
       count_if(to_date(published_at) BETWEEN date_sub(DATE'{CRAWL_ANCHOR}', 365) AND DATE'{CRAWL_ANCHOR}')
                         AS n_in_crawl_window
FROM v GROUP BY canonical_id
""")

# COMMAND ----------
# 4. Channel-level classification (stays inside Databricks).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW channels AS
WITH base AS (
  SELECT u.*,
         CASE WHEN s.canonical_id IS NOT NULL THEN 0 ELSE f.step END AS step,
         CASE WHEN s.canonical_id IS NOT NULL THEN 'seed' ELSE coalesce(f.source, 'untracked') END AS first_source,
         f.disc_date,
         a.newest_upload, a.n_stored_uploads,
         CASE WHEN a.canonical_id IS NULL THEN 'unknown'
              WHEN to_date(a.newest_upload) >= date_sub(DATE'{PANEL_ANCHOR}', 365) THEN 'active'
              ELSE 'inactive' END AS activity_panel,
         CASE WHEN a.canonical_id IS NULL THEN 'unknown'
              WHEN a.n_in_crawl_window > 0 THEN 'active'
              WHEN a.n_stored_uploads >= 10 AND to_date(a.oldest_stored_upload) > DATE'{CRAWL_ANCHOR}' THEN 'active'
              ELSE 'inactive' END AS activity_crawl
  FROM universe u
  LEFT JOIN (SELECT DISTINCT canonical_id FROM dev_sean.default.new_yt_top_list) s USING (canonical_id)
  LEFT JOIN first_discovery f USING (canonical_id)
  LEFT JOIN activity a USING (canonical_id)
)
SELECT base.*,
       CASE WHEN subscriber_count < 100000 THEN 1 WHEN subscriber_count < 1000000 THEN 2
            WHEN subscriber_count < 10000000 THEN 3 ELSE 4 END AS sub_bin,
       r.views_pct_rank
FROM base
LEFT JOIN (SELECT canonical_id, percent_rank() OVER (ORDER BY weekly_views) AS views_pct_rank
           FROM universe WHERE weekly_views IS NOT NULL) r USING (canonical_id)
""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW channels_binned AS
SELECT *,
       CASE WHEN views_pct_rank IS NULL THEN NULL
            WHEN views_pct_rank < 0.25 THEN 1 WHEN views_pct_rank < 0.50 THEN 2
            WHEN views_pct_rank < 0.75 THEN 3 WHEN views_pct_rank < 0.99 THEN 4 ELSE 5 END AS view_bin,
       CASE WHEN step IS NOT NULL THEN step
            WHEN first_source = 'later_sources' THEN 14
            WHEN first_source = 'trending' THEN 15 ELSE 16 END AS step_order
FROM channels
""")
mode = "CREATE OR REPLACE TABLE" if OVERWRITE else "CREATE TABLE"
spark.sql(f"{mode} {out('channels')} AS SELECT * FROM channels_binned")
ch = out("channels")

# COMMAND ----------
# 5. Aggregates (the only things that leave the workspace).
VARIANTS = f"""
  SELECT 'all' AS variant, * FROM {ch}
  UNION ALL SELECT 'active_panel', * FROM {ch} WHERE activity_panel = 'active' AND NOT is_topic
  UNION ALL SELECT 'active_crawl', * FROM {ch} WHERE activity_crawl = 'active' AND NOT is_topic
"""
BINS = f"""
  SELECT variant, 'subscribers' AS bin_type, sub_bin AS bin_order,
         element_at(array('10k-100k','100k-1M','1M-10M','10M+'), sub_bin) AS bin_label,
         step_order, coalesce(weekly_views, 0) AS wv
  FROM ({VARIANTS})
  UNION ALL
  SELECT variant, 'weekly_views', view_bin,
         element_at(array('Bottom 25%','25-50%','50-75%','75-99%','Top 1%'), view_bin),
         step_order, weekly_views
  FROM ({VARIANTS}) WHERE view_bin IS NOT NULL
"""
spark.sql(f"""{mode} {out('exhaustion')} AS
SELECT variant, bin_type, bin_order, bin_label, step_order,
       CASE WHEN step_order <= 13 THEN CAST(step_order AS STRING)
            WHEN step_order = 14 THEN 'later_sources' WHEN step_order = 15 THEN 'trending'
            ELSE 'untracked' END AS step_label,
       count(*) AS n_channels, sum(wv) AS weekly_views
FROM ({BINS}) GROUP BY ALL""")
spark.sql(f"""{mode} {out('totals')} AS
SELECT variant, bin_type, bin_order, count(*) AS n_channels_total, sum(wv) AS weekly_views_total
FROM ({BINS}) GROUP BY ALL""")
spark.sql(f"""{mode} {out('trending')} AS
SELECT variant, date_trunc('week', disc_date) AS week_start,
       count(*) AS n_channels, sum(coalesce(weekly_views, 0)) AS weekly_views
FROM ({VARIANTS}) WHERE first_source = 'trending' GROUP BY ALL""")
spark.sql(f"""{mode} {out('activity')} AS
SELECT sub_bin, is_topic, activity_panel, activity_crawl,
       count(*) AS n_channels, sum(coalesce(weekly_views, 0)) AS weekly_views
FROM {ch} GROUP BY ALL""")

# COMMAND ----------
# 6. Checks.
n_universe = spark.sql("SELECT count(*) FROM universe").first()[0]
n_all = spark.sql(f"""SELECT sum(n_channels_total) FROM {out('totals')}
                      WHERE variant='all' AND bin_type='subscribers'""").first()[0]
assert n_universe == n_all, f"bin counts {n_all} != universe {n_universe}"
n_dupes = spark.sql(f"SELECT count(*) - count(DISTINCT canonical_id) FROM {ch}").first()[0]
assert n_dupes == 0, f"{n_dupes} duplicate channels in {ch}"
print("universe:", n_universe)
display(spark.sql(f"""SELECT step_label, sum(n_channels) n FROM {out('exhaustion')}
                      WHERE variant='all' AND bin_type='subscribers' GROUP BY ALL ORDER BY min(step_order)"""))
display(spark.sql(f"""SELECT is_topic, activity_panel, activity_crawl, sum(n_channels) n, sum(weekly_views) wv
                      FROM {out('activity')} GROUP BY ALL ORDER BY n DESC"""))

# COMMAND ----------
# 7. Render (same plotting module as the local renderer).
try:
    nb_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
    sys.path.insert(0, "/Workspace" + nb_dir)
except Exception:
    pass
from fig1_discovery_plot import plot_fig1  # noqa: E402

ex_pd = spark.table(out("exhaustion")).toPandas()
tr_pd = spark.table(out("trending")).toPandas()
tot_pd = spark.table(out("totals")).toPandas()
for variant, bin_type in [("active_panel", "subscribers"), ("all", "subscribers"),
                          ("active_panel", "weekly_views"), ("active_crawl", "subscribers")]:
    path = f"{P['figure_dir']}/fig1_{variant}_{bin_type}.png" if P["figure_dir"] else None
    display(plot_fig1(ex_pd, tr_pd, tot_pd, variant, bin_type, path=path))
