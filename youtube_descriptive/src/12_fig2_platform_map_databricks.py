# Databricks notebook source
# MAGIC %md
# MAGIC # Figure 2 — a map of public YouTube (language → category → channel)
# MAGIC
# MAGIC Builds the aggregates behind Figure 2 of *Attention on YouTube* and writes them to
# MAGIC `<output_prefix>_<name>_<version>`. Only aggregates leave the workspace, with one documented
# MAGIC carve-out: `cells` names the public head channels that are large enough to label on the map.
# MAGIC Every other channel is pooled into an "Other channels" cell per language × category.
# MAGIC
# MAGIC - **Universe and weekly views:** the Figure 1 channel table (`channels_table`): channels with
# MAGIC   ≥10k subscribers on 2026-10-05, weekly views from the 2026-09-28 → 2026-10-05 delta.
# MAGIC - **Language:** `channel_language` (ISO 639-3) from the LID v3 silver table. `und` or no row
# MAGIC   → `und` (shown as "Unknown").
# MAGIC - **Category:** YouTube's own topic hierarchy, from `topicDetails.topicCategories`
# MAGIC   (`11_fetch_channel_topics_databricks`). Each topic maps to one of YouTube's seven parent
# MAGIC   topics. A channel takes the parent of its first *specific* topic (e.g. Pop music, Action
# MAGIC   game); a generic parent-level topic (e.g. Lifestyle, Music) counts only when the channel has
# MAGIC   no specific one. No topics, or not fetched → `Unclassified`.
# MAGIC - **Traffic blocks:** channels ranked by weekly views and cut into `n_blocks` blocks of equal
# MAGIC   view mass (block 1 = the top 5% of views). Exact running sums, no binning. Panel c re-ranks
# MAGIC   within each of the largest languages.
# MAGIC
# MAGIC Outputs: `cells` (treemap), `lang_cat` (every language × category), `blocks` (category mix
# MAGIC per block, platform and per language), `coverage` (label coverage).

# COMMAND ----------
import os


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
    "channels_table": "dev_sean.matt.fig1_discovery_channels_v4",
    "language_table": "dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_current",
    "topics_table": "dev_sean.matt.yt_channel_topics",
    "n_blocks": "20",
    "top_languages": "8",
    "block_languages": "3",
    "label_min_share": "0.0002",
    "output_prefix": "dev_sean.matt.fig2_map",
    "version": "v1",
    "overwrite": "false",
}
for k, v in DEFAULTS.items():
    _create_text_widget(k, v)
P = {k: _get_widget(k, v) for k, v in DEFAULTS.items()}

N_BLOCKS = int(P["n_blocks"])
TOP_LANGS = int(P["top_languages"])
BLOCK_LANGS = int(P["block_languages"])
LABEL_MIN = float(P["label_min_share"])
OVERWRITE = P["overwrite"].lower() == "true"


def out(name: str) -> str:
    return f"{P['output_prefix']}_{name}_{P['version']}"


OUTPUTS = ["channels", "cells", "lang_cat", "blocks", "coverage"]
print({**P, "outputs": [out(n) for n in OUTPUTS]})

# COMMAND ----------
# Pre-flight: never silently overwrite.
schema, prefix = P["output_prefix"].rsplit(".", 1)
existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {schema} LIKE '{prefix}_*'").collect()}
clash = [out(n) for n in OUTPUTS if out(n).rsplit(".", 1)[1] in existing]
if clash and not OVERWRITE:
    raise RuntimeError(f"Output tables already exist: {clash}. Set overwrite=true or bump version.")

# COMMAND ----------
# 1. Topic → YouTube parent topic. `specific` = 0 for the parent-level topics themselves.
TOPIC_MAP = {
    "Music": [("Music", 0), "Music_of_Asia", "Music_of_Latin_America", "Pop_music", "Hip_hop_music",
              "Electronic_music", "Rock_music", "Christian_music", "Independent_music", "Soul_music",
              "Classical_music", "Rhythm_and_blues", "Jazz", "Country_music", "Reggae"],
    "Gaming": [("Video_game_culture", 0), "Action_game", "Role-playing_video_game", "Action-adventure_game",
               "Strategy_video_game", "Simulation_video_game", "Sports_game", "Racing_video_game",
               "Casual_game", "Puzzle_video_game", "Music_video_game"],
    "Sports": [("Sport", 0), "Association_football", "Motorsport", "Cricket", "Basketball",
               "Mixed_martial_arts", "Boxing", "American_football", "Baseball", "Volleyball", "Golf",
               "Ice_hockey", "Tennis"],
    "Entertainment": [("Entertainment", 0), "Film", "Television_program", "Humour", "Performing_arts",
                      "Professional_wrestling"],
    "Lifestyle": [("Lifestyle_(sociology)", 0), "Hobby", "Vehicle", "Food", "Technology", "Tourism", "Pet",
                  "Fashion", "Physical_fitness", "Physical_attractiveness"],
    "Society": [("Society", 0), "Religion", "Health", "Politics", "Business", "Military"],
    "Knowledge": [("Knowledge", 0)],
}
rows = []
for parent, topics in TOPIC_MAP.items():
    for t in topics:
        name, specific = t if isinstance(t, tuple) else (t, 1)
        rows.append((f"https://en.wikipedia.org/wiki/{name}", parent, specific))
spark.createDataFrame(rows, "topic STRING, category STRING, specific INT").createOrReplaceTempView("topic_map")

# COMMAND ----------
# 2. Channel-level frame (stays in Databricks).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW ch AS
WITH t AS (
  SELECT canonical_id, max_by(topic_categories, collected_at) AS topic_categories
  FROM {P['topics_table']} WHERE status = 'ok' GROUP BY canonical_id
),
tp AS (
  SELECT e.canonical_id, m.category, m.specific, e.pos
  FROM (SELECT canonical_id, posexplode(topic_categories) AS (pos, topic) FROM t) e
  JOIN topic_map m ON m.topic = e.topic
),
cat AS (
  SELECT canonical_id, max_by(category, struct(specific, -pos)) AS category FROM tp GROUP BY canonical_id
),
lang AS (
  SELECT channel_id AS canonical_id, max(channel_language) AS channel_language
  FROM {P['language_table']} GROUP BY channel_id
)
SELECT c.canonical_id, c.channel_name, c.is_topic,
       CAST(coalesce(c.weekly_views, 0) AS DOUBLE) AS wv,
       CASE WHEN l.channel_language IS NULL OR l.channel_language = '' THEN 'und'
            ELSE l.channel_language END AS language,
       coalesce(k.category, 'Unclassified') AS category,
       t.canonical_id IS NOT NULL AS topics_fetched
FROM {P['channels_table']} c
LEFT JOIN lang l USING (canonical_id)
LEFT JOIN cat k USING (canonical_id)
LEFT JOIN t USING (canonical_id)
""")

# Language groups for the treemap: top languages by weekly views, then "other", then "und".
top = [r.language for r in spark.sql(f"""
  SELECT language FROM ch WHERE language <> 'und' GROUP BY language
  ORDER BY sum(wv) DESC LIMIT {TOP_LANGS}""").collect()]
block_langs = top[:BLOCK_LANGS]
print("top languages:", top, "| block languages:", block_langs)
top_sql = ", ".join(f"'{x}'" for x in top)

total_wv = spark.sql("SELECT sum(wv) FROM ch").first()[0]
mode = "CREATE OR REPLACE TABLE" if OVERWRITE else "CREATE TABLE"
spark.sql(f"""
{mode} {out('channels')} AS
SELECT *,
       CASE WHEN language IN ({top_sql}) THEN language WHEN language = 'und' THEN 'und' ELSE 'other' END
         AS language_group,
       least({N_BLOCKS}, CAST(floor(coalesce(sum(wv) OVER (ORDER BY wv DESC, canonical_id
                                                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                     / {total_wv} * {N_BLOCKS}) AS INT) + 1) AS block,
       wv / {total_wv} AS share
FROM ch
""")
chn = out("channels")

# COMMAND ----------
# 3. Aggregates (the only things that leave the workspace).
spark.sql(f"""
{mode} {out('cells')} AS
SELECT language_group, category,
       CASE WHEN share >= {LABEL_MIN} THEN channel_name ELSE 'Other channels' END AS label,
       CASE WHEN share >= {LABEL_MIN} THEN canonical_id END AS canonical_id,
       count(*) AS n_channels, sum(wv) AS weekly_views
FROM {chn}
GROUP BY 1, 2, 3, 4
""")
spark.sql(f"""
{mode} {out('lang_cat')} AS
SELECT language, category, count(*) AS n_channels, sum(wv) AS weekly_views
FROM {chn} GROUP BY 1, 2
""")
# Platform blocks come from the channel table; language blocks re-rank within each language.
lang_list = ", ".join(f"'{x}'" for x in block_langs)
spark.sql(f"""
{mode} {out('blocks')} AS
WITH lb AS (
  SELECT language, category, wv,
         least({N_BLOCKS}, CAST(floor(coalesce(sum(wv) OVER (PARTITION BY language ORDER BY wv DESC, canonical_id
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                              / sum(wv) OVER (PARTITION BY language) * {N_BLOCKS}) AS INT) + 1) AS block
  FROM {chn} WHERE language IN ({lang_list})
)
SELECT 'all' AS scope, block, category, count(*) AS n_channels, sum(wv) AS weekly_views
FROM {chn} GROUP BY block, category
UNION ALL
SELECT language, block, category, count(*), sum(wv) FROM lb GROUP BY language, block, category
""")
spark.sql(f"""
{mode} {out('coverage')} AS
SELECT language_group, category <> 'Unclassified' AS has_category, language <> 'und' AS has_language,
       topics_fetched, is_topic, count(*) AS n_channels, sum(wv) AS weekly_views
FROM {chn} GROUP BY 1, 2, 3, 4, 5
""")

# COMMAND ----------
display(spark.sql(f"""
SELECT 'labelled channels' k, count(*) v FROM {out('cells')} WHERE canonical_id IS NOT NULL
UNION ALL SELECT 'category coverage (views)', sum(CASE WHEN has_category THEN weekly_views END) / sum(weekly_views)
  FROM {out('coverage')}
UNION ALL SELECT 'language coverage (views)', sum(CASE WHEN has_language THEN weekly_views END) / sum(weekly_views)
  FROM {out('coverage')}
UNION ALL SELECT 'block 1 channels', count(*) FROM {chn} WHERE block = 1
"""))
