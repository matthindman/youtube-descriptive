# Databricks notebook source
# MAGIC %md
# MAGIC # Figure 5b (alternative) - language rank, engagement and speaker population
# MAGIC
# MAGIC Builds the per-language aggregates behind the alternative Figure 5 of *Attention on YouTube*
# MAGIC ("language rank vs. engagement and population") and writes them to `<output_prefix>_<name>_<version>`.
# MAGIC Only aggregates leave the workspace; the channel-level frame (`channels`) stays in Databricks. The
# MAGIC speaker-population join (Unicode CLDR) happens locally in `scripts/render_fig5b_local.py`.
# MAGIC Purely descriptive.
# MAGIC
# MAGIC - **Universe:** the >=10k-subscriber channels of `fig1_channels_table` (the Figure 1 / 3 / 4 / 5 universe).
# MAGIC - **Weekly views:** as in Figures 3, 4 and 5. The lifetime-view delta between `snapshot_start` and
# MAGIC   `snapshot_end` (latest collection per channel and date), divided by the elapsed days between the two
# MAGIC   collections and multiplied by 7. Channels missing from either snapshot (`missing_start`,
# MAGIC   `missing_end`) and channels whose lifetime total fell (`total_fell`) are excluded. Zero deltas stay 0.
# MAGIC - **Language:** `language` of `language_table` (Figure 2 v2 cascade: LID v3, dual-sample table, gap-fill
# MAGIC   run; ISO 639-3, `und` = unknown). It is the language of the channel's written metadata, not the spoken
# MAGIC   or audience language. Universe channels with no row in `language_table` count as `und`. Labels are
# MAGIC   exported as they are; merging labels into analysis languages (e.g. `uzn` + `uzb`) happens locally.
# MAGIC
# MAGIC Outputs: `languages` (one row per label: channels, measurable channels, weekly views, per-channel
# MAGIC distribution and head concentration), `status` (status counts), `reconcile`.

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
    "stats_table": "dev_sean.default.yt_channel_stats_full",
    "fig1_channels_table": "dev_sean.matt.fig1_discovery_channels_v4",
    "language_table": "dev_sean.matt.fig2_map_channels_v2",
    "snapshot_start": "2026-08-31",
    "snapshot_end": "2026-10-05",
    "output_prefix": "dev_sean.matt.fig5b_langrank",
    "version": "v1",
    "overwrite": "false",
}
for k, v in DEFAULTS.items():
    _create_text_widget(k, v)
P = {k: _get_widget(k, v) for k, v in DEFAULTS.items()}

STATS, F1, LANG = P["stats_table"], P["fig1_channels_table"], P["language_table"]
START, END = P["snapshot_start"], P["snapshot_end"]
OVERWRITE = P["overwrite"].lower() == "true"


def out(name: str) -> str:
    return f"{P['output_prefix']}_{name}_{P['version']}"


OUTPUTS = ["channels", "languages", "status", "reconcile"]
print({**P, "outputs": [out(n) for n in OUTPUTS]})

# COMMAND ----------
# Pre-flight: never silently overwrite.
schema, prefix = P["output_prefix"].rsplit(".", 1)
existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {schema} LIKE '{prefix}_*'").collect()}
clash = [out(n) for n in OUTPUTS if out(n).rsplit(".", 1)[1] in existing]
if clash and not OVERWRITE:
    raise RuntimeError(f"Output tables already exist: {clash}. Set overwrite=true or bump version.")
mode = "CREATE OR REPLACE TABLE" if OVERWRITE else "CREATE TABLE"

# COMMAND ----------
# 1. Weekly views for the Figure 1 universe, one row per channel and date (latest collection wins).
# Same SQL as 15_fig5_age_databricks.py. Gotchas: LEFT JOIN from the universe so missing channels are
# counted, not dropped; status derived before wv; try_divide guards a zero elapsed time.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW f5b_wv AS
WITH snap AS (
  SELECT canonical_id, collected_date,
         max_by(total_view_count, collected_at) AS views,
         max(collected_at)                      AS collected_at
  FROM {STATS}
  WHERE collected_date IN (DATE'{START}', DATE'{END}')
    AND canonical_id IN (SELECT canonical_id FROM {F1})
  GROUP BY canonical_id, collected_date
),
e AS (SELECT * FROM snap WHERE collected_date = DATE'{END}'),
s AS (SELECT * FROM snap WHERE collected_date = DATE'{START}')
SELECT u.canonical_id,
       CASE WHEN e.canonical_id IS NULL THEN 'missing_end'
            WHEN s.canonical_id IS NULL THEN 'missing_start'
            WHEN e.views < s.views THEN 'total_fell'
            WHEN e.views = s.views THEN 'zero'
            ELSE 'positive' END AS status,
       CASE WHEN e.canonical_id IS NOT NULL AND s.canonical_id IS NOT NULL AND e.views >= s.views
            THEN try_divide(e.views - s.views,
                            (unix_timestamp(e.collected_at) - unix_timestamp(s.collected_at)) / 86400.0) * 7
       END AS wv
FROM (SELECT DISTINCT canonical_id FROM {F1}) u
LEFT JOIN e USING (canonical_id)
LEFT JOIN s USING (canonical_id)
""")

# 2. One language label per channel (the language table is one row per channel; guard with max_by anyway).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW f5b_lang AS
SELECT canonical_id, coalesce(nullif(max(language), ''), 'und') AS language
FROM {LANG} GROUP BY canonical_id
""")

# 3. Channel frame: every universe channel with its status, weekly views and label (missing label -> und).
spark.sql(f"""
{mode} {out('channels')} AS
SELECT w.canonical_id, w.status, w.wv, coalesce(l.language, 'und') AS language,
       l.canonical_id IS NULL AS no_language_row
FROM f5b_wv w LEFT JOIN f5b_lang l USING (canonical_id)
""")
spark.sql(f"CACHE TABLE f5b_ch AS SELECT * FROM {out('channels')}")

# COMMAND ----------
# 4. Aggregates (the only things that leave the workspace). Measurable = zero + positive.
spark.sql(f"""
{mode} {out('status')} AS
SELECT status, count(*) AS n_channels, sum(wv) AS wv_sum FROM f5b_ch GROUP BY status
""")
spark.sql(f"""
{mode} {out('languages')} AS
WITH m AS (
  SELECT language, wv, row_number() OVER (PARTITION BY language ORDER BY wv DESC, canonical_id) AS rk
  FROM f5b_ch WHERE status IN ('zero', 'positive')
),
agg AS (
  SELECT language, count(*) AS n_measurable, sum(CASE WHEN wv > 0 THEN 1 ELSE 0 END) AS n_positive,
         sum(wv) AS wv_sum, percentile(wv, 0.5) AS wv_median, percentile(wv, 0.9) AS wv_p90,
         max(wv) AS wv_max, sum(CASE WHEN rk <= 10 THEN wv END) AS wv_top10
  FROM m GROUP BY language
),
u AS (SELECT language, count(*) AS n_universe FROM f5b_ch GROUP BY language)
SELECT u.language, u.n_universe, coalesce(a.n_measurable, 0) AS n_measurable,
       coalesce(a.n_positive, 0) AS n_positive, coalesce(a.wv_sum, 0) AS wv_sum,
       a.wv_median, a.wv_p90, a.wv_max, a.wv_top10
FROM u LEFT JOIN agg a USING (language)
""")
display(spark.table(out("languages")).orderBy("wv_sum", ascending=False).limit(30))

# COMMAND ----------
# 5. Reconcile with the universe, Figures 3-5 and the language table.
spark.sql(f"""
{mode} {out('reconcile')} AS
SELECT 'fig1 universe' AS what, count(*) AS n FROM {F1}
UNION ALL SELECT 'fig1 universe distinct canonical_id', count(DISTINCT canonical_id) FROM {F1}
UNION ALL SELECT 'channels frame rows', count(*) FROM f5b_ch
UNION ALL SELECT 'measurable (zero/positive)', count(*) FROM f5b_ch WHERE status IN ('zero', 'positive')
UNION ALL SELECT 'measurable weekly views', sum(wv) FROM f5b_ch WHERE status IN ('zero', 'positive')
UNION ALL SELECT 'language table rows', count(*) FROM {LANG}
UNION ALL SELECT 'language table distinct canonical_id', count(DISTINCT canonical_id) FROM {LANG}
UNION ALL SELECT 'universe channels with no language-table row (counted und)', count(*)
  FROM f5b_ch WHERE no_language_row
UNION ALL SELECT 'language-table channels outside the universe', count(*) FROM {LANG} t
  LEFT ANTI JOIN (SELECT DISTINCT canonical_id FROM {F1}) f USING (canonical_id)
UNION ALL SELECT 'distinct language labels', count(DISTINCT language) FROM f5b_ch
UNION ALL SELECT 'measurable und channels', count(*) FROM f5b_ch
  WHERE language = 'und' AND status IN ('zero', 'positive')
UNION ALL SELECT 'measurable und weekly views', sum(wv) FROM f5b_ch
  WHERE language = 'und' AND status IN ('zero', 'positive')
""")
display(spark.table(out("reconcile")))
