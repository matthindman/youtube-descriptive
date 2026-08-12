# Databricks notebook source
# MAGIC %md
# MAGIC # 100k channel sample, weighted by total views, subscribers > 10k
# MAGIC
# MAGIC Builds a probability-proportional-to-size (PPS) sample of `sample_size` channels drawn
# MAGIC **without replacement** from the population of channels whose most recent known
# MAGIC `subscriber_count` exceeds `subscriber_threshold`, with selection probability
# MAGIC proportional to each channel's most recent `total_view_count`.
# MAGIC
# MAGIC **Population source**: `yt_channel_stats_full` is a repeated-measures table (one row per
# MAGIC channel per crawl snapshot, ~1.35B rows / ~114M distinct channels as of 2026-08-12). We
# MAGIC take each channel's latest snapshot (`max_by(..., collected_at)`) as its current state.
# MAGIC
# MAGIC **Sampling method**: Efraimidis-Spirakis (A-Res) weighted sampling without replacement.
# MAGIC For each channel, draw `u ~ Uniform(0,1)` and compute `key = ln(u) / total_view_count`;
# MAGIC the `sample_size` channels with the largest key form the PPS sample. This is equivalent
# MAGIC to (and more numerically stable than) ranking by `u^(1/weight)`, and unlike naive
# MAGIC multinomial-with-replacement sampling it never produces duplicate channels.
# MAGIC
# MAGIC **Assumptions** (flagging since these are judgment calls, not gotchas with one right answer):
# MAGIC - "greater than 10k subscribers" is evaluated on each channel's *latest* snapshot, not
# MAGIC   "ever crossed 10k". A cheap pre-filter (`WHERE subscriber_count > threshold` on the raw
# MAGIC   rows before grouping) is applied first to cut shuffle volume; this is a safe superset
# MAGIC   filter since the latest-snapshot value can only appear if some row carries it.
# MAGIC - Channels with `total_view_count = 0` at their latest snapshot (177,703 of them, out of
# MAGIC   ~4.97M channels with subs > 10k) are excluded from the eligible population — a weight of
# MAGIC   zero means zero selection probability, so this is just being explicit about it.
# MAGIC - No explicit inclusion-probability / analysis-weight column is added to the output. If you
# MAGIC   need weights for reweighting the sample back to the population (e.g. Horvitz-Thompson
# MAGIC   style), the standard approximation is `pi_i ~= min(1, sample_size * total_view_count_i /
# MAGIC   SUM(total_view_count))`; happy to add it as a column if useful.

# COMMAND ----------
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


import os

_create_text_widget("source_table", "dev_sean.default.yt_channel_stats_full")
_create_text_widget("target_table", "dev_sean.default.channel_sample_100k_weighted_views")
_create_text_widget("subscriber_threshold", "10000")
_create_text_widget("sample_size", "100000")
_create_text_widget("random_seed", "20260812")

SOURCE_TABLE = _get_widget("source_table", "dev_sean.default.yt_channel_stats_full")
TARGET_TABLE = _get_widget("target_table", "dev_sean.default.channel_sample_100k_weighted_views")
SUBSCRIBER_THRESHOLD = int(_get_widget("subscriber_threshold", "10000"))
SAMPLE_SIZE = int(_get_widget("sample_size", "100000"))
RANDOM_SEED = int(_get_widget("random_seed", "20260812"))

# COMMAND ----------
# Pre-flight: confirm the target table doesn't already exist (never silently overwrite).
existing = spark.sql(
    f"SHOW TABLES IN {TARGET_TABLE.rsplit('.', 1)[0]} LIKE '{TARGET_TABLE.rsplit('.', 1)[1]}'"
).collect()
if existing:
    raise RuntimeError(
        f"{TARGET_TABLE} already exists. Drop it explicitly first if you intend to replace it, "
        "or choose a different target_table."
    )

# COMMAND ----------
# Pre-flight: confirm the eligible population is comfortably larger than the sample size.
population_check = spark.sql(f"""
    WITH pre_filtered AS (
        SELECT canonical_id, collected_at, subscriber_count, total_view_count
        FROM {SOURCE_TABLE}
        WHERE subscriber_count > {SUBSCRIBER_THRESHOLD}
    ),
    latest AS (
        SELECT
            canonical_id,
            max_by(subscriber_count, collected_at) AS subscriber_count,
            max_by(total_view_count, collected_at) AS total_view_count
        FROM pre_filtered
        GROUP BY canonical_id
    )
    SELECT
        COUNT(*) AS n_gt_threshold,
        SUM(CASE WHEN total_view_count > 0 THEN 1 ELSE 0 END) AS n_eligible
    FROM latest
    WHERE subscriber_count > {SUBSCRIBER_THRESHOLD}
""").collect()[0]

print(f"Channels with subscriber_count > {SUBSCRIBER_THRESHOLD}: {population_check['n_gt_threshold']:,}")
print(f"Eligible (also total_view_count > 0): {population_check['n_eligible']:,}")

if population_check["n_eligible"] < SAMPLE_SIZE:
    raise ValueError(
        f"Eligible population ({population_check['n_eligible']:,}) is smaller than "
        f"sample_size ({SAMPLE_SIZE:,}); cannot sample without replacement."
    )

# COMMAND ----------
# Build the sample and persist it as a new table.
spark.sql(f"""
    CREATE TABLE {TARGET_TABLE} AS
    WITH pre_filtered AS (
        SELECT canonical_id, collected_at, subscriber_count, total_view_count
        FROM {SOURCE_TABLE}
        WHERE subscriber_count > {SUBSCRIBER_THRESHOLD}
    ),
    latest AS (
        SELECT
            canonical_id,
            max_by(subscriber_count, collected_at) AS subscriber_count,
            max_by(total_view_count, collected_at) AS total_view_count,
            max(collected_at) AS stats_as_of
        FROM pre_filtered
        GROUP BY canonical_id
    ),
    population AS (
        SELECT *
        FROM latest
        WHERE subscriber_count > {SUBSCRIBER_THRESHOLD}
          AND total_view_count > 0
    ),
    keyed AS (
        SELECT
            *,
            ln(rand({RANDOM_SEED})) / total_view_count AS sample_key
        FROM population
    )
    SELECT canonical_id, subscriber_count, total_view_count, stats_as_of, sample_key
    FROM keyed
    ORDER BY sample_key DESC
    LIMIT {SAMPLE_SIZE}
""")

# COMMAND ----------
# Post-flight verification — run after the write.
verify = spark.sql(f"""
    SELECT
        COUNT(*) AS n_rows,
        COUNT(DISTINCT canonical_id) AS n_distinct_channels,
        MIN(subscriber_count) AS min_subs,
        MIN(total_view_count) AS min_views,
        MAX(total_view_count) AS max_views,
        SUM(total_view_count) AS sum_sample_views
    FROM {TARGET_TABLE}
""").collect()[0]
print(verify.asDict())

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY total_view_count DESC LIMIT 10"))
display(spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY total_view_count ASC LIMIT 10"))
