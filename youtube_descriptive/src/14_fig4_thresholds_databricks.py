# Databricks notebook source
# MAGIC %md
# MAGIC # Figure 4 - common subscriber thresholds capture unequal shares of channels and views
# MAGIC
# MAGIC Builds the aggregates behind Figure 4 of *Attention on YouTube* and writes them to
# MAGIC `<output_prefix>_<name>_<version>`. Only aggregates leave the workspace; the channel-level frame
# MAGIC (`channels`) and the rank table the tail model fits on (`ranked`) stay in Databricks.
# MAGIC
# MAGIC - **Universe:** every channel in the `snapshot_end` partition of `stats_table` (all collected
# MAGIC   channels, not only >=10k subscribers).
# MAGIC - **Weekly views:** as in Figure 3. The lifetime-view delta between `snapshot_start` and
# MAGIC   `snapshot_end`, divided by the elapsed days between the two collections and multiplied by 7.
# MAGIC   Channels missing from the start snapshot get null weekly views (`missing_start`). Channels whose
# MAGIC   lifetime total fell (`total_fell`) are excluded, as in Figure 3. Zero deltas stay 0.
# MAGIC - **Subscribers:** `subscriber_count` at `snapshot_end`. A null count (hidden) is its own tier.
# MAGIC - **Tiers:** <1k, 1k-10k, 10k-100k, 100k-1M, >=1M, hidden.
# MAGIC - **Missing mass:** `tail_model.py` (copied from `common_data_weights`) fitted on the ranked positive
# MAGIC   weekly views at each `r0`, models PL (D0) >= D1 >= D2 >= D3. Platform total = all observed views
# MAGIC   + max(modelled mass beyond r0 - observed mass beyond r0, 0), as in the weights pipeline.
# MAGIC
# MAGIC Outputs: `snapshots` (row and duplicate audit of the two snapshots), `status` (tier x status),
# MAGIC `thresholds` (channel and view shares at or above each threshold), `sbins` (0.05-dex subscriber
# MAGIC bins, for continuous threshold curves), `vbins` (weekly-view bins x tier, for the cumulative
# MAGIC capture curve), `tail` (fits and totals per r0 x model), `reconcile` (>=10k counts vs Figs 1 and 3).

# COMMAND ----------
import os
import sys

# Make the sibling tail_model.py importable, also under jobs.
try:
    _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _nb_dir = os.path.dirname(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb)
except Exception:
    _nb_dir = os.getcwd()
if _nb_dir not in sys.path:
    sys.path.insert(0, _nb_dir)


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
    "snapshot_start": "2026-08-31",
    "snapshot_end": "2026-10-05",
    "thresholds": "100,1000,10000,100000,1000000",
    "r0s": "1000000,5000000,50000000",
    "headline_r0": "50000000",
    "headline_model": "d3",
    "sbin_dex": "0.05",
    "vbin_dex": "0.02",
    "output_prefix": "dev_sean.matt.fig4_thresholds",
    "version": "v1",
    "overwrite": "false",
}
for k, v in DEFAULTS.items():
    _create_text_widget(k, v)
P = {k: _get_widget(k, v) for k, v in DEFAULTS.items()}

STATS = P["stats_table"]
START, END = P["snapshot_start"], P["snapshot_end"]
THRESHOLDS = [int(x) for x in P["thresholds"].split(",")]
R0S = [int(x) for x in P["r0s"].split(",")]
HEADLINE_R0, HEADLINE_MODEL = int(P["headline_r0"]), P["headline_model"]
MODELS = ["pl", "d1", "d2", "d3"]
SBIN_DEX, VBIN_DEX = float(P["sbin_dex"]), float(P["vbin_dex"])
OVERWRITE = P["overwrite"].lower() == "true"
TIER_SQL = """CASE WHEN subs IS NULL THEN 'hidden'
                   WHEN subs >= 1000000 THEN '>=1M'
                   WHEN subs >= 100000 THEN '100k-1M'
                   WHEN subs >= 10000 THEN '10k-100k'
                   WHEN subs >= 1000 THEN '1k-10k'
                   ELSE '<1k' END"""


def out(name: str) -> str:
    return f"{P['output_prefix']}_{name}_{P['version']}"


OUTPUTS = ["snapshots", "channels", "status", "thresholds", "sbins", "vbins", "ranked", "tail", "reconcile"]
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
# 1. The two snapshots, one row per channel and date (latest collection wins, as in Figure 3).
spark.sql(f"""
{mode} {out('snapshots')} AS
SELECT collected_date, count(*) AS n_rows, count(DISTINCT canonical_id) AS n_channels,
       count_if(subscriber_count IS NULL) AS n_subs_null,
       min(collected_at) AS collected_min, max(collected_at) AS collected_max
FROM {STATS}
WHERE collected_date IN (DATE'{START}', DATE'{END}')
GROUP BY collected_date
""")
snaps = spark.table(out("snapshots")).orderBy("collected_date").collect()
display(spark.table(out("snapshots")))
assert len(snaps) == 2, f"Expected both snapshots {START} and {END}, found {[r.collected_date for r in snaps]}"

spark.sql(f"""
{mode} {out('channels')} AS
WITH snap AS (
  SELECT canonical_id, collected_date,
         max_by(subscriber_count, collected_at) AS subs,
         max_by(total_view_count, collected_at) AS views,
         max(collected_at)                      AS collected_at
  FROM {STATS}
  WHERE collected_date IN (DATE'{START}', DATE'{END}')
  GROUP BY canonical_id, collected_date
),
e AS (SELECT * FROM snap WHERE collected_date = DATE'{END}'),
s AS (SELECT * FROM snap WHERE collected_date = DATE'{START}'),
j AS (
  SELECT e.canonical_id, e.subs, e.views AS views_end, s.views AS views_start,
         CASE WHEN s.canonical_id IS NULL THEN 'missing_start'
              WHEN e.views < s.views THEN 'total_fell'
              WHEN e.views = s.views THEN 'zero'
              ELSE 'positive' END AS status,
         CASE WHEN s.canonical_id IS NOT NULL AND e.views >= s.views
              THEN try_divide(e.views - s.views,
                              (unix_timestamp(e.collected_at) - unix_timestamp(s.collected_at)) / 86400.0) * 7
         END AS wv
  FROM e LEFT JOIN s USING (canonical_id)
)
SELECT *, {TIER_SQL} AS tier FROM j
""")
spark.sql(f"CACHE TABLE ch_c AS SELECT * FROM {out('channels')}")

# COMMAND ----------
# 2. Tier x status counts and view sums.
spark.sql(f"""
{mode} {out('status')} AS
SELECT tier, status, count(*) AS n_channels, sum(wv) AS wv_sum,
       sum(CASE WHEN status = 'total_fell' THEN views_end - views_start END) AS fell_sum
FROM ch_c GROUP BY tier, status
""")
display(spark.table(out("status")).orderBy("tier", "status"))

# 3. Shares at or above each threshold. Denominators: measurable channels (zero + positive) and their
# weekly views, hidden-subscriber channels included in the denominators but in no numerator.
thr_values = ", ".join(f"({t})" for t in THRESHOLDS)
spark.sql(f"""
{mode} {out('thresholds')} AS
WITH m AS (SELECT subs, wv FROM ch_c WHERE status IN ('zero', 'positive')),
     tot AS (SELECT count(*) AS n_all, sum(wv) AS wv_all FROM m)
SELECT t.threshold,
       count_if(m.subs >= t.threshold) AS n_ge,
       sum(CASE WHEN m.subs >= t.threshold THEN m.wv END) AS wv_ge,
       first(tot.n_all) AS n_all, first(tot.wv_all) AS wv_all,
       count_if(m.subs >= t.threshold) / first(tot.n_all) AS share_channels,
       sum(CASE WHEN m.subs >= t.threshold THEN m.wv END) / first(tot.wv_all) AS share_views_observed
FROM (VALUES {thr_values}) AS t(threshold) CROSS JOIN m CROSS JOIN tot
GROUP BY t.threshold
""")
display(spark.table(out("thresholds")).orderBy("threshold"))

# 4. Subscriber bins (continuous threshold curves) and weekly-view bins x tier (capture curve).
spark.sql(f"""
{mode} {out('sbins')} AS
SELECT CAST(floor(log10(subs) / {SBIN_DEX}) AS INT) AS sbin, {SBIN_DEX} AS sbin_dex,
       count(*) AS n_channels, sum(wv) AS wv_sum, min(subs) AS subs_min, max(subs) AS subs_max
FROM ch_c WHERE status IN ('zero', 'positive') AND subs > 0
GROUP BY 1
""")
spark.sql(f"""
{mode} {out('vbins')} AS
SELECT CAST(floor(log10(wv) / {VBIN_DEX}) AS INT) AS vbin, {VBIN_DEX} AS vbin_dex, tier,
       count(*) AS n_channels, sum(wv) AS wv_sum, min(wv) AS wv_min, max(wv) AS wv_max
FROM ch_c WHERE status = 'positive'
GROUP BY 1, 3
""")

# COMMAND ----------
# 5. Rank positive weekly views (rank 1 = most views). Rows are split into 0.005-dex value buckets;
# rank = bucket offset + row_number within the bucket, which equals a global row_number ordered by
# weekly views (ties may be ordered either way, which nothing downstream depends on). One range
# shuffle also lays out each file over a contiguous rank range, so the fits' rank filters skip files.
from pyspark.sql import Window
from pyspark.sql import functions as F

BUCKET = "CAST(floor(log10(wv) * 200) AS INT)"
pos = spark.table("ch_c").where("status = 'positive'").selectExpr("wv AS view_count", f"{BUCKET} AS b")
bstats = pos.groupBy("b").agg(F.count(F.lit(1)).alias("n"), F.min("view_count").alias("vmin"),
                              F.max("view_count").alias("vmax")).collect()
offsets, cum, prev = [], 0, None
for r in sorted(bstats, key=lambda r: -r["b"]):
    if prev is not None and prev["vmin"] < r["vmax"]:
        raise RuntimeError(f"Bucket {prev['b']} min {prev['vmin']} < bucket {r['b']} max {r['vmax']}")
    offsets.append((int(r["b"]), cum))
    cum += int(r["n"])
    prev = r
N_POSITIVE = cum
print(f"{N_POSITIVE:,} positive channels in {len(offsets):,} buckets, "
      f"largest {max(int(r['n']) for r in bstats):,}")
offsets_df = spark.createDataFrame(offsets, "b int, rank_offset bigint")
ranked = (pos.repartitionByRange(256, F.desc("b"))
          .withColumn("rn", F.row_number().over(Window.partitionBy("b").orderBy(F.desc("view_count"))))
          .join(F.broadcast(offsets_df), "b")
          .select("view_count", (F.col("rank_offset") + F.col("rn")).alias("rank")))
if OVERWRITE:
    spark.sql(f"DROP TABLE IF EXISTS {out('ranked')}")
ranked.write.option("optimizeWrite", "false").saveAsTable(out("ranked"))

chk = spark.sql(f"""SELECT count(*) AS n, min(rank) AS lo, max(rank) AS hi, sum(rank) AS s,
                           sum(view_count) AS v FROM {out('ranked')}""").first()
assert chk["n"] == N_POSITIVE and chk["lo"] == 1 and chk["hi"] == N_POSITIVE \
    and chk["s"] == N_POSITIVE * (N_POSITIVE + 1) // 2, f"ranks are not 1..n: {chk.asDict()}"
OBS_ALL = float(chk["v"])

# COMMAND ----------
# 6. Tail fits and platform totals per r0 x model.
import importlib
import math

import tail_model
importlib.reload(tail_model)

spark.sql(f"CREATE OR REPLACE TEMP VIEW fig4_ranked AS SELECT view_count, rank FROM {out('ranked')}")
rows = []
for r0 in R0S:
    if r0 > N_POSITIVE:
        print(f"r0={r0:,} exceeds {N_POSITIVE:,} positive channels, skipped")
        continue
    obs_r0 = float(spark.sql(f"SELECT sum(view_count) AS s FROM fig4_ranked WHERE rank <= {r0}").first()["s"])
    est = tail_model.spark_fit_tail_estimates(spark, "fig4_ranked", float(r0), obs_r0)
    tails = [est.tail(m) for m in MODELS]
    ordering_ok = all(a >= b for a, b in zip(tails, tails[1:]))
    obs_beyond = OBS_ALL - obs_r0
    for m in MODELS:
        tail = float(est.tail(m))
        finite = math.isfinite(tail)
        total = float(est.total(m, OBS_ALL)) if finite else None
        rows.append((r0, m, r0 == HEADLINE_R0 and m == HEADLINE_MODEL,
                     float(est.y0), float(est.alpha0), float(est.eta0), float(est.eta1), float(est.eta2),
                     int(est.n_fit), obs_r0, obs_beyond, OBS_ALL, N_POSITIVE,
                     tail if finite else None, finite, ordering_ok,
                     max(tail - obs_beyond, 0.0) if finite else None, total,
                     OBS_ALL / total if finite else None,
                     float(tail_model.mass_between(est.y0, r0, est.alpha0, est.eta_coeffs(m), N_POSITIVE))))
    print(f"r0={r0:,}: alpha={est.alpha0:.3f} "
          + "  ".join(f"{m}={est.total(m, OBS_ALL):,.0f}" for m in MODELS) + f"  ordered={ordering_ok}")

TAIL_SCHEMA = """r0 bigint, model string, is_headline boolean, y0 double, alpha0 double, eta0 double,
    eta1 double, eta2 double, n_fit bigint, obs_total_r0 double, obs_beyond_r0 double, obs_total_all double,
    n_positive bigint, tail double, tail_is_finite boolean, tail_ordering_ok boolean, missing_mass double,
    total double, coverage double, pred_observed_segment double"""
tail_df = spark.createDataFrame(rows, TAIL_SCHEMA)
if OVERWRITE:
    spark.sql(f"DROP TABLE IF EXISTS {out('tail')}")
tail_df.write.saveAsTable(out("tail"))
display(spark.table(out("tail")).orderBy("r0", "model"))

# COMMAND ----------
# 7. Reconcile the >=10k slice with the Figure 1 universe and Figure 3's measurable channels.
spark.sql(f"""
{mode} {out('reconcile')} AS
SELECT 'stats >=10k on snapshot_end' AS what, count(*) AS n FROM ch_c WHERE subs >= 10000
UNION ALL SELECT 'stats >=10k measurable (zero/positive)', count(*) FROM ch_c
  WHERE subs >= 10000 AND status IN ('zero', 'positive')
UNION ALL SELECT 'fig1 universe', count(*) FROM {P['fig1_channels_table']}
UNION ALL SELECT 'fig1 universe missing from snapshot_end', count(*) FROM {P['fig1_channels_table']} f
  LEFT ANTI JOIN ch_c USING (canonical_id)
UNION ALL SELECT 'stats >=10k not in fig1 universe', count(*) FROM ch_c c
  LEFT ANTI JOIN {P['fig1_channels_table']} f USING (canonical_id) WHERE c.subs >= 10000
""")
display(spark.table(out("reconcile")))
