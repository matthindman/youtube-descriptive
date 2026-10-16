# Databricks notebook source
# MAGIC %md
# MAGIC # Figure 5 - older channels dominate the highest-attention ranks
# MAGIC
# MAGIC Builds the aggregates behind Figure 5 of *Attention on YouTube* and writes them to
# MAGIC `<output_prefix>_<name>_<version>`. Only aggregates leave the workspace; the channel-level frame
# MAGIC (`channels`) stays in Databricks. Purely descriptive: channel age against attention rank.
# MAGIC
# MAGIC - **Universe:** the >=10k-subscriber channels of `fig1_channels_table` (the Figure 1 / 3 universe).
# MAGIC - **Weekly views:** as in Figures 3 and 4. The lifetime-view delta between `snapshot_start` and
# MAGIC   `snapshot_end` (latest collection per channel and date), divided by the elapsed days between the
# MAGIC   two collections and multiplied by 7. Channels missing from either snapshot (`missing_start`,
# MAGIC   `missing_end`) and channels whose lifetime total fell (`total_fell`) are excluded. Zero deltas
# MAGIC   stay 0 (they sit in the last block).
# MAGIC - **Traffic blocks:** measurable channels (zero + positive) ranked by weekly views (ties broken by
# MAGIC   `canonical_id`) and cut into `n_blocks` blocks of equal view mass (block 1 = the channels holding
# MAGIC   the top 5% of weekly views). Exact running sums, as in Figure 2.
# MAGIC - **Founding date:** `created_at_table` (`snippet.publishedAt`, from notebook 09): the latest
# MAGIC   `status = 'ok'` row per channel. Dates before YouTube launched (`launch_date`; 1970-01-01 is the
# MAGIC   API's placeholder) are set to null, as in Figure 1. Blocks are assigned before and independently of
# MAGIC   the founding date; channels without one are counted in `blocks` coverage only.
# MAGIC - **Decimal founding year:** `1970 + days since 1970-01-01 / 365.2425`. Medians and quartiles are exact.
# MAGIC
# MAGIC Outputs: `status` (status counts), `blocks` (per-block coverage, median / quartiles of founding
# MAGIC year, share founded by each benchmark year; `block = 0` is the whole universe), `year_dist`
# MAGIC (block x founding year), `reconcile`.

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
    "created_at_table": "dev_sean.matt.yt_channel_created_at",
    "snapshot_start": "2026-08-31",
    "snapshot_end": "2026-10-05",
    "n_blocks": "20",
    "launch_date": "2005-02-14",
    "benchmark_years": "2010,2015,2020",
    "output_prefix": "dev_sean.matt.fig5_age",
    "version": "v1",
    "overwrite": "false",
}
for k, v in DEFAULTS.items():
    _create_text_widget(k, v)
P = {k: _get_widget(k, v) for k, v in DEFAULTS.items()}

STATS, F1, CREATED = P["stats_table"], P["fig1_channels_table"], P["created_at_table"]
START, END = P["snapshot_start"], P["snapshot_end"]
N_BLOCKS = int(P["n_blocks"])
LAUNCH = P["launch_date"]
BENCH = [int(x) for x in P["benchmark_years"].split(",")]
OVERWRITE = P["overwrite"].lower() == "true"


def out(name: str) -> str:
    return f"{P['output_prefix']}_{name}_{P['version']}"


OUTPUTS = ["channels", "status", "blocks", "year_dist", "reconcile"]
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
# Gotchas: LEFT JOIN from the universe so missing channels are counted, not dropped; status is
# derived before wv; try_divide guards a zero elapsed time.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW f5_wv AS
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

# 2. Founding date: latest ok row per channel; pre-launch placeholders -> null (the Figure 1 rule).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW f5_born AS
SELECT canonical_id, to_date(created_at) AS founded
FROM (SELECT canonical_id, max_by(published_at, collected_at) AS created_at
      FROM {CREATED} WHERE status = 'ok' GROUP BY canonical_id)
WHERE to_date(created_at) >= DATE'{LAUNCH}'
""")

# 3. Channel frame: measurable channels get a block; the founding date is joined afterwards, so
# blocks do not depend on it.
spark.sql("""
CREATE OR REPLACE TEMP VIEW f5_meas AS
SELECT canonical_id, status, wv FROM f5_wv WHERE status IN ('zero', 'positive')
""")
total_wv = spark.sql("SELECT sum(wv) FROM f5_meas").first()[0]
print(f"measurable weekly views: {total_wv:,.0f}")
spark.sql(f"""
{mode} {out('channels')} AS
SELECT m.canonical_id, m.status, m.wv, b.founded,
       CASE WHEN b.founded IS NOT NULL
            THEN 1970 + datediff(b.founded, DATE'1970-01-01') / 365.2425 END AS founded_dec,
       year(b.founded) AS founded_year,
       least({N_BLOCKS}, CAST(floor(coalesce(sum(m.wv) OVER (ORDER BY m.wv DESC, m.canonical_id
                                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                    / {total_wv} * {N_BLOCKS}) AS INT) + 1) AS block
FROM f5_meas m LEFT JOIN f5_born b USING (canonical_id)
""")
spark.sql(f"CACHE TABLE f5_ch AS SELECT * FROM {out('channels')}")

# COMMAND ----------
# 4. Aggregates (the only things that leave the workspace).
spark.sql(f"""
{mode} {out('status')} AS
SELECT status, count(*) AS n_channels, sum(wv) AS wv_sum FROM f5_wv GROUP BY status
""")
bench_cols = ",\n       ".join(
    f"avg(CASE WHEN founded_year IS NULL THEN NULL WHEN founded_year <= {y} THEN 1.0 ELSE 0.0 END) "
    f"AS share_le_{y}" for y in BENCH)
agg = f"""count(*) AS n_channels, sum(wv) AS wv_sum,
       count(founded) AS n_dated, sum(CASE WHEN founded IS NOT NULL THEN wv END) AS wv_dated,
       min(wv) AS wv_min, max(wv) AS wv_max,
       percentile(founded_dec, 0.25) AS q25, percentile(founded_dec, 0.5) AS median,
       percentile(founded_dec, 0.75) AS q75, avg(founded_dec) AS mean,
       {bench_cols}"""
spark.sql(f"""
{mode} {out('blocks')} AS
SELECT block, {agg} FROM f5_ch GROUP BY block
UNION ALL
SELECT 0 AS block, {agg} FROM f5_ch
""")
spark.sql(f"""
{mode} {out('year_dist')} AS
SELECT block, founded_year, count(*) AS n_channels, sum(wv) AS wv_sum
FROM f5_ch WHERE founded_year IS NOT NULL GROUP BY block, founded_year
""")
display(spark.table(out("blocks")).orderBy("block"))

# COMMAND ----------
# 5. Reconcile with Figures 1 and 3 and with the founding-date table.
spark.sql(f"""
{mode} {out('reconcile')} AS
SELECT 'fig1 universe' AS what, count(*) AS n FROM {F1}
UNION ALL SELECT 'fig1 universe distinct canonical_id', count(DISTINCT canonical_id) FROM {F1}
UNION ALL SELECT 'measurable (zero/positive)', count(*) FROM f5_ch
UNION ALL SELECT 'measurable with founding date', count(*) FROM f5_ch WHERE founded IS NOT NULL
UNION ALL SELECT 'universe with ok founding row', count(*) FROM {F1} u
  LEFT SEMI JOIN (SELECT DISTINCT canonical_id FROM {CREATED} WHERE status = 'ok') c USING (canonical_id)
UNION ALL SELECT 'universe with not_found founding row only', count(*) FROM {F1} u
  LEFT SEMI JOIN (SELECT canonical_id FROM {CREATED} GROUP BY canonical_id
                  HAVING max(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) = 0) c USING (canonical_id)
UNION ALL SELECT 'ok founding dates before launch (nulled)', count(*) FROM
  (SELECT canonical_id, max_by(published_at, collected_at) AS pa FROM {CREATED}
   WHERE status = 'ok' GROUP BY canonical_id) WHERE to_date(pa) < DATE'{LAUNCH}'
UNION ALL SELECT 'blocks present', count(DISTINCT block) FROM f5_ch
""")
display(spark.table(out("reconcile")))
