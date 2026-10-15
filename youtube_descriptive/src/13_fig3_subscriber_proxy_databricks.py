# Databricks notebook source
# MAGIC %md
# MAGIC # Figure 3 - subscribers are an imprecise proxy for attention
# MAGIC
# MAGIC Builds the aggregates behind Figure 3 of *Attention on YouTube* and writes them to
# MAGIC `<output_prefix>_<name>_<version>`. Only aggregates leave the workspace (binned quantiles,
# MAGIC fit statistics, a suppressed 2-D histogram, variance decompositions); the channel-level frame
# MAGIC stays in `<output_prefix>_channels_<version>`.
# MAGIC
# MAGIC - **Universe:** the Figure 1 channel table (`channels_table`): >=10k subscribers on 2026-10-05.
# MAGIC   Variant `all` keeps every channel; variant `active` keeps Fig 1's `active_panel`, non-Topic channels.
# MAGIC - **Weekly views:** the lifetime-view delta between `snapshot_start` and `snapshot_end`, divided by
# MAGIC   the elapsed days between the two collections and multiplied by 7, floored at 0. A multi-week
# MAGIC   window averages over YouTube's 12-26 h step-function refreshes of `total_view_count`. Channels
# MAGIC   missing from the start snapshot, or whose lifetime total *fell* over the window (video removals
# MAGIC   or count corrections, so attention is not measurable from totals), get null weekly views and
# MAGIC   are excluded (counted in `fit` as `n_missing_start` and `n_total_fell`). Zero deltas stay 0.
# MAGIC   The single-week Fig 1 value (2026-09-28 -> 2026-10-05) is kept as `wv_1w` for comparison.
# MAGIC - **Subscribers:** `subscriber_count` at `snapshot_end` (the API rounds it to 3 significant figures).
# MAGIC - **Views per subscriber (VPS):** weekly views / subscribers.
# MAGIC - **Language, category:** from the Figure 2 channel table (`map_table`).
# MAGIC - **Subscriber bins:** `bin_dex`-wide bins of log10(subscribers), merged from the top down until
# MAGIC   each holds at least `min_bin_n` channels (`min_group_bin_n` for per-group curves).
# MAGIC
# MAGIC Outputs: `snapshots` (panel snapshot audit), `window_check` (single-week vs window noise),
# MAGIC `bins` (binned quantiles), `hist2d`, `fit`, `decomp` (variance explained by subscribers,
# MAGIC category, language and their cells).

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
    "channels_table": "dev_sean.matt.fig1_discovery_channels_v4",
    "map_table": "dev_sean.matt.fig2_map_channels_v2",
    "stats_table": "dev_sean.default.yt_channel_stats_full",
    "snapshot_start": "2026-08-31",
    "snapshot_end": "2026-10-05",
    "audit_from": "2026-06-01",
    "bin_dex": "0.05",
    "min_bin_n": "500",
    "min_group_bin_n": "300",
    "min_hist_n": "10",
    "min_lang_n": "1000",
    "output_prefix": "dev_sean.matt.fig3_subproxy",
    "version": "v1",
    "overwrite": "false",
    "figure_dir": "",
}
for k, v in DEFAULTS.items():
    _create_text_widget(k, v)
P = {k: _get_widget(k, v) for k, v in DEFAULTS.items()}

STATS = P["stats_table"]
START, END = P["snapshot_start"], P["snapshot_end"]
BIN_DEX = float(P["bin_dex"])
MIN_BIN_N, MIN_GROUP_BIN_N = int(P["min_bin_n"]), int(P["min_group_bin_n"])
MIN_HIST_N, MIN_LANG_N = int(P["min_hist_n"]), int(P["min_lang_n"])
OVERWRITE = P["overwrite"].lower() == "true"
QS = [0.025, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.975]
QNAMES = ["p025", "p05", "p10", "p25", "p50", "p75", "p90", "p95", "p975"]


def out(name: str) -> str:
    return f"{P['output_prefix']}_{name}_{P['version']}"


OUTPUTS = ["channels", "snapshots", "negatives", "window_check", "bins", "hist2d", "fit", "decomp"]
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
# 1. Per-snapshot values for the universe, every Monday snapshot from audit_from to snapshot_end.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW snap AS
SELECT s.canonical_id, s.collected_date,
       max_by(s.subscriber_count, s.collected_at) AS subs,
       max_by(s.total_view_count, s.collected_at) AS views,
       max(s.collected_at)                        AS collected_at
FROM {STATS} s
JOIN (SELECT canonical_id FROM {P['channels_table']}) u USING (canonical_id)
WHERE s.collected_date BETWEEN DATE'{P['audit_from']}' AND DATE'{END}'
GROUP BY s.canonical_id, s.collected_date
""")
spark.sql("CACHE TABLE snap_c AS SELECT * FROM snap")

# Snapshot audit: coverage, collection timing, share of channels whose view total moved, and the
# elapsed-day-normalised weekly views versus the previous snapshot.
spark.sql(f"""
{mode} {out('snapshots')} AS
WITH d AS (
  SELECT *, lag(views) OVER w AS views_prev, lag(collected_at) OVER w AS at_prev,
            lag(collected_date) OVER w AS date_prev
  FROM snap_c WINDOW w AS (PARTITION BY canonical_id ORDER BY collected_date)
)
SELECT collected_date, count(*) AS n_channels,
       min(collected_at) AS collected_min, max(collected_at) AS collected_max,
       percentile_approx(unix_timestamp(collected_at), 0.5) AS collected_median_unix,
       count(views_prev) AS n_with_prev,
       try_divide(count_if(views_prev IS NOT NULL AND views <> views_prev), count(views_prev)) AS share_changed,
       try_divide(count_if(views < views_prev), count(views_prev)) AS share_negative,
       sum(CASE WHEN views_prev IS NOT NULL THEN views - views_prev END) AS delta_views,
       sum(CASE WHEN views_prev IS NOT NULL
                THEN try_divide(views - views_prev, (unix_timestamp(collected_at) - unix_timestamp(at_prev)) / 86400.0) * 7
           END) AS weekly_views_norm,
       percentile_approx(CASE WHEN views_prev IS NOT NULL
                THEN (unix_timestamp(collected_at) - unix_timestamp(at_prev)) / 86400.0 END, 0.5) AS median_gap_days
FROM d GROUP BY collected_date
""")
display(spark.table(out("snapshots")).orderBy("collected_date"))

# Falling totals inside the window: per channel, how many weekly deltas were negative and whether the
# window as a whole fell. Recovering drops (oscillation) show up as negative weeks with a rising window.
spark.sql(f"""
{mode} {out('negatives')} AS
WITH d AS (
  SELECT canonical_id, collected_date, views,
         views - lag(views) OVER (PARTITION BY canonical_id ORDER BY collected_date) AS dv
  FROM snap_c WHERE collected_date BETWEEN DATE'{START}' AND DATE'{END}'
),
c AS (
  SELECT canonical_id, count_if(dv < 0) AS n_neg_weeks, count(dv) AS n_weeks,
         sum(CASE WHEN dv > 0 THEN dv END) AS pos, sum(CASE WHEN dv < 0 THEN -dv END) AS neg,
         max_by(views, collected_date) - min_by(views, collected_date) AS net,
         min(collected_date) = DATE'{START}' AND max(collected_date) = DATE'{END}' AS has_both
  FROM d GROUP BY canonical_id
)
SELECT least(n_neg_weeks, 3) AS n_neg_weeks, net < 0 AS window_fell, count(*) AS n_channels,
       sum(pos) AS pos_views, sum(neg) AS neg_views, sum(net) AS net_views,
       percentile_approx(try_divide(neg, pos), 0.5) AS median_neg_over_pos
FROM c WHERE has_both GROUP BY ALL
""")
display(spark.table(out("negatives")).orderBy("n_neg_weeks", "window_fell"))

# COMMAND ----------
# 2. Channel-level frame (stays in Databricks).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW ch0 AS
WITH e AS (SELECT * FROM snap_c WHERE collected_date = DATE'{END}'),
     s AS (SELECT * FROM snap_c WHERE collected_date = DATE'{START}'),
     lang_n AS (SELECT language, count(*) AS n FROM {P['map_table']} GROUP BY language)
SELECT c.canonical_id, c.is_topic, c.activity_panel,
       c.subscriber_count AS subs,
       CAST(c.weekly_views AS DOUBLE) AS wv_1w,
       CASE WHEN s.canonical_id IS NULL OR e.canonical_id IS NULL OR e.views < s.views THEN NULL
            ELSE try_divide(e.views - s.views,
                            (unix_timestamp(e.collected_at) - unix_timestamp(s.collected_at)) / 86400.0) * 7
       END AS wv,
       s.subs AS subs_start,
       coalesce(e.views < s.views, false) AS total_fell,
       coalesce(m.language, 'und') AS language,
       coalesce(m.language_group, 'und') AS language_group,
       CASE WHEN m.language IS NULL THEN 'und' WHEN ln.n >= {MIN_LANG_N} THEN m.language ELSE 'other' END AS language_cell,
       coalesce(m.category, 'Unclassified') AS category
FROM {P['channels_table']} c
LEFT JOIN e USING (canonical_id)
LEFT JOIN s USING (canonical_id)
LEFT JOIN {P['map_table']} m USING (canonical_id)
LEFT JOIN lang_n ln ON ln.language = m.language
""")
spark.sql(f"""
{mode} {out('channels')} AS
SELECT *,
       log10(subs) AS lsub,
       CASE WHEN wv > 0 THEN log10(wv) END AS lwv,
       try_divide(wv, subs) AS vps,
       CASE WHEN wv > 0 THEN log10(wv / subs) END AS lvps,
       CAST(floor(log10(subs) / {BIN_DEX}) AS INT) AS sbin,
       CASE WHEN subs_start IS NULL OR subs_start = 0 THEN 'unknown'
            WHEN subs <= subs_start THEN 'flat or falling'
            WHEN try_divide(subs - subs_start, subs_start) < 0.02 THEN '0-2%'
            WHEN try_divide(subs - subs_start, subs_start) < 0.10 THEN '2-10%'
            ELSE '>10%' END AS growth,
       activity_panel = 'active' AND NOT is_topic AS is_active
FROM ch0
""")
chn = out("channels")
n_ch, n_dist = spark.sql(f"SELECT count(*), count(DISTINCT canonical_id) FROM {chn}").first()
assert n_ch == n_dist, f"{n_ch - n_dist} duplicate channels in {chn}"
print("channels:", n_ch)

# Variants x groups as a long view: one row per (channel, variant, group_type).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW long AS
SELECT v.variant, g.group_type, g.grp, c.*
FROM {chn} c
LATERAL VIEW explode(CASE WHEN c.is_active THEN array('all', 'active') ELSE array('all') END) v AS variant
LATERAL VIEW explode(array(
  named_struct('group_type', 'all',            'grp', 'all'),
  named_struct('group_type', 'category',       'grp', c.category),
  named_struct('group_type', 'language_group', 'grp', c.language_group),
  named_struct('group_type', 'growth',         'grp', c.growth))) gt AS g
WHERE c.wv IS NOT NULL
""")

# COMMAND ----------
# 3. Subscriber bins: fine log bins, merged from the top down until each has >= min n channels.
counts = spark.sql("""SELECT variant, group_type, grp, sbin, count(*) AS n FROM long
                      GROUP BY ALL""").toPandas()
rows = []
for (variant, gt, grp), g in counts.groupby(["variant", "group_type", "grp"]):
    min_n = MIN_BIN_N if gt == "all" else MIN_GROUP_BIN_N
    acc, members, bin_id = 0, [], 0
    for sbin, n in sorted(zip(g.sbin, g.n), reverse=True):
        acc += n
        members.append(int(sbin))
        if acc >= min_n:
            rows += [(variant, gt, grp, s, bin_id) for s in members]
            acc, members, bin_id = 0, [], bin_id + 1
    if members:  # leftover at the bottom joins the last full bin (or stands alone)
        rows += [(variant, gt, grp, s, max(bin_id - 1, 0)) for s in members]
spark.createDataFrame(rows, "variant STRING, group_type STRING, grp STRING, sbin INT, bin_id INT") \
     .createOrReplaceTempView("bin_map")

qarr = "array(" + ", ".join(str(q) for q in QS) + ")"
qcols = ",\n       ".join(
    [f"wvq[{i}] AS wv_{n}" for i, n in enumerate(QNAMES)] + [f"vpsq[{i}] AS vps_{n}" for i, n in enumerate(QNAMES)])
spark.sql(f"""
{mode} {out('bins')} AS
WITH b AS (
  SELECT l.variant, l.group_type, l.grp, m.bin_id,
         min(l.subs) AS subs_min, max(l.subs) AS subs_max,
         percentile(l.subs, 0.5) AS subs_median, power(10, avg(l.lsub)) AS subs_geomean,
         count(*) AS n_channels, count_if(l.wv = 0) AS n_zero,
         sum(l.wv) AS wv_sum, sum(l.subs) AS subs_sum, avg(l.vps) AS vps_mean,
         avg(l.lwv) AS lwv_mean, stddev_pop(l.lwv) AS lwv_sd, stddev_pop(l.lvps) AS lvps_sd,
         percentile(l.wv, {qarr}) AS wvq, percentile(l.vps, {qarr}) AS vpsq
  FROM long l JOIN bin_map m USING (variant, group_type, grp, sbin)
  GROUP BY ALL
)
SELECT variant, group_type, grp, bin_id, subs_min, subs_max, subs_median, subs_geomean,
       n_channels, n_zero, wv_sum, subs_sum, vps_mean, lwv_mean, lwv_sd, lvps_sd,
       {qcols}
FROM b
""")

# COMMAND ----------
# 4. 2-D histogram for panel a's density layer (cells under min_hist_n channels suppressed).
spark.sql(f"""
{mode} {out('hist2d')} AS
SELECT variant, sbin, CASE WHEN wv > 0 THEN CAST(floor(lwv / 0.1) AS INT) END AS vbin,
       {BIN_DEX} AS sbin_dex, 0.1 AS vbin_dex, count(*) AS n_channels
FROM long WHERE group_type = 'all'
GROUP BY ALL HAVING count(*) >= {MIN_HIST_N}
""")

# COMMAND ----------
# 5. Fit statistics per variant: log-log OLS on positive weekly views, Spearman (average ranks),
# pooled ratio, zero share, and coverage.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW base AS SELECT * FROM long WHERE group_type = 'all'
""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW ranks AS
WITH rs AS (
  SELECT variant, subs, count(*) AS n,
         sum(count(*)) OVER (PARTITION BY variant ORDER BY subs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS before
  FROM base GROUP BY variant, subs
),
rv AS (
  SELECT variant, wv, count(*) AS n,
         sum(count(*)) OVER (PARTITION BY variant ORDER BY wv ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS before
  FROM base GROUP BY variant, wv
)
SELECT b.variant, coalesce(rs.before, 0) + (rs.n + 1) / 2.0 AS r_subs,
       coalesce(rv.before, 0) + (rv.n + 1) / 2.0 AS r_wv
FROM base b JOIN rs USING (variant, subs) JOIN rv USING (variant, wv)
""")
spark.sql(f"""
{mode} {out('fit')} AS
WITH o AS (
  SELECT variant, count(*) AS n_channels, count_if(wv = 0) AS n_zero,
         count_if(wv > 0) AS n_positive,
         regr_slope(lwv, lsub) AS ols_slope, regr_intercept(lwv, lsub) AS ols_intercept,
         regr_r2(lwv, lsub) AS ols_r2,
         stddev_pop(lwv) AS lwv_sd, stddev_pop(lvps) AS lvps_sd,
         sqrt(var_pop(lwv) * (1 - regr_r2(lwv, lsub))) AS ols_resid_sd,
         try_divide(sum(wv), sum(subs)) AS pooled_vps, avg(vps) AS mean_vps, percentile(vps, 0.5) AS median_vps,
         sum(wv) AS wv_sum, sum(subs) AS subs_sum,
         corr(lwv, CASE WHEN wv_1w > 0 THEN log10(wv_1w) END) AS corr_log_wv_1w
  FROM base GROUP BY variant
),
sp AS (SELECT variant, corr(r_subs, r_wv) AS spearman FROM ranks GROUP BY variant),
miss AS (
  SELECT 'all' AS variant, count(*) AS n_universe, count_if(wv IS NULL AND NOT total_fell) AS n_missing_start,
         count_if(total_fell) AS n_total_fell FROM {chn}
  UNION ALL
  SELECT 'active', count(*), count_if(wv IS NULL AND NOT total_fell), count_if(total_fell) FROM {chn} WHERE is_active
)
SELECT * FROM o JOIN sp USING (variant) JOIN miss USING (variant)
""")
display(spark.table(out("fit")))

# COMMAND ----------
# 6. Single-week vs window noise, by 0.5-dex subscriber band: how much of the single-week spread
# is refresh timing rather than attention.
spark.sql(f"""
{mode} {out('window_check')} AS
SELECT variant, CAST(floor(lsub / 0.5) AS INT) AS band,
       min(subs) AS subs_min, max(subs) AS subs_max, count(*) AS n_channels,
       count_if(wv_1w = 0) AS n_zero_1w, count_if(wv = 0) AS n_zero_window,
       corr(log10(wv_1w), lwv) AS corr_log,
       percentile_approx(abs(log10(wv_1w) - lwv), 0.5) AS median_abs_log_ratio,
       percentile_approx(log10(wv_1w) - lwv, array(0.1, 0.9)) AS log_ratio_p10_p90,
       stddev_pop(log10(wv_1w / subs)) AS lvps_sd_1w,
       stddev_pop(lvps) AS lvps_sd_window
FROM long
WHERE group_type = 'all' AND wv > 0 AND wv_1w > 0
GROUP BY ALL
""")
display(spark.table(out("window_check")).orderBy("variant", "band"))

# COMMAND ----------
# 7. Variance decomposition of log weekly views and log VPS (positive weekly views only).
# R^2 of a cell-means model = 1 - (within-cell SS / total SS). Within-cell spread = channel-weighted
# mean of each cell's p90-p10 range (cells with >= 30 channels).
MODELS = {
    "subscribers": "CAST(least(floor(lsub * 10), 75) AS STRING)",
    "category": "category",
    "language": "language_cell",
    "category x language": "concat(category, '|', language_cell)",
    "subscribers x category": "concat(least(floor(lsub * 10), 75), '|', category)",
    "subscribers x language": "concat(least(floor(lsub * 10), 75), '|', language_cell)",
    "subscribers x category x language": "concat(least(floor(lsub * 10), 75), '|', category, '|', language_cell)",
}
model_arr = ",\n  ".join(f"named_struct('model', '{m}', 'cell', {k})" for m, k in MODELS.items())
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW dlong AS
SELECT b.variant, y.outcome, y.y, m.model, m.cell
FROM base b
LATERAL VIEW explode(array(named_struct('outcome', 'log10 weekly views', 'y', b.lwv),
                           named_struct('outcome', 'log10 views per subscriber', 'y', b.lvps))) t AS y
LATERAL VIEW explode(array(
  named_struct('model', 'none', 'cell', 'all'),
  {model_arr})) mm AS m
WHERE b.wv > 0
""")
spark.sql(f"""
{mode} {out('decomp')} AS
WITH cells AS (
  SELECT variant, outcome, model, cell, count(*) AS n, var_pop(y) * count(*) AS ssw,
         percentile_approx(y, 0.9, 2000) - percentile_approx(y, 0.1, 2000) AS p90_p10
  FROM dlong GROUP BY ALL
),
agg AS (
  SELECT variant, outcome, model, sum(n) AS n_channels, count(*) AS n_cells,
         count_if(n >= 30) AS n_cells_30, sum(ssw) AS ss_within,
         try_divide(sum(CASE WHEN n >= 30 THEN p90_p10 * n END), sum(CASE WHEN n >= 30 THEN n END)) AS within_p90_p10
  FROM cells GROUP BY ALL
),
tot AS (SELECT variant, outcome, ss_within AS ss_total, within_p90_p10 AS total_p90_p10
        FROM agg WHERE model = 'none')
SELECT a.variant, a.outcome, a.model, a.n_channels, a.n_cells, a.n_cells_30,
       1 - try_divide(a.ss_within, t.ss_total) AS r2,
       sqrt(a.ss_within / a.n_channels) AS within_sd,
       sqrt(t.ss_total / a.n_channels) AS total_sd,
       a.within_p90_p10, t.total_p90_p10
FROM agg a JOIN tot t USING (variant, outcome)
""")
display(spark.table(out("decomp")).orderBy("variant", "outcome", "r2"))

# COMMAND ----------
# 8. Checks.
n_universe = spark.sql(f"SELECT count(*) FROM {P['channels_table']}").first()[0]
assert n_ch == n_universe, f"channel frame {n_ch} != universe {n_universe}"
n_bins = spark.sql(f"""SELECT sum(n_channels) FROM {out('bins')}
                       WHERE variant = 'all' AND group_type = 'all'""").first()[0]
n_fit = spark.sql(f"SELECT n_channels FROM {out('fit')} WHERE variant = 'all'").first()[0]
assert n_bins == n_fit, f"bins {n_bins} != fit {n_fit}"
print("universe:", n_universe, "| with window views:", n_fit)
spark.sql("UNCACHE TABLE IF EXISTS snap_c")

# COMMAND ----------
# 9. Render (same plotting module as the local renderer).
try:
    nb_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
    sys.path.insert(0, "/Workspace" + nb_dir)
    from fig3_proxy_plot import RENDER_SPECS, plot_fig3  # noqa: E402

    tabs = {n: spark.table(out(n)).toPandas() for n in ("bins", "hist2d", "fit")}
    for variant, panel_c in RENDER_SPECS:
        path = f"{P['figure_dir']}/fig3_{variant}_{panel_c}.png" if P["figure_dir"] else None
        display(plot_fig3(tabs["bins"], tabs["hist2d"], tabs["fit"], variant, panel_c=panel_c, path=path))
except Exception as exc:  # the tables are the deliverable; rendering is a preview
    print("render skipped:", repr(exc))
