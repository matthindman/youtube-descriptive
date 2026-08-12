# Databricks notebook source
# MAGIC %md
# MAGIC # 1M channel simple random sample (without replacement)
# MAGIC
# MAGIC Draws a simple random sample (SRS) of `sample_size` channels, without replacement, from
# MAGIC the deduplicated full list of channels — no subscriber or view filtering.
# MAGIC
# MAGIC **Population source**: same as the weighted sample in
# MAGIC `04_channel_sample_100k_weighted_views_databricks.py` — `yt_channel_stats_full` (a
# MAGIC repeated-measures table, ~1.35B rows / ~114M distinct channels as of 2026-08-12),
# MAGIC deduplicated to each channel's latest snapshot via `max_by(..., collected_at)`. Unlike
# MAGIC the weighted sample, there is no subscriber or view threshold here — this is the full
# MAGIC population.
# MAGIC
# MAGIC **Sampling method**: assign each channel an iid `rand(seed)` key, take the `sample_size`
# MAGIC rows with the smallest key. This is a standard, unbiased way to draw an exact-size SRS
# MAGIC without replacement (equivalent to taking a prefix of a random permutation) — every
# MAGIC channel has equal selection probability, unlike the views-weighted PPS sample.
# MAGIC
# MAGIC **Note**: this query does a full aggregation over the ~1.35B-row source table (no
# MAGIC early filter is possible since the full population is in scope) — expect it to take
# MAGIC noticeably longer than the weighted-sample query.

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


_create_text_widget("source_table", "dev_sean.default.yt_channel_stats_full")
_create_text_widget("target_table", "dev_sean.default.channel_sample_1m_srs")
_create_text_widget("sample_size", "1000000")
_create_text_widget("random_seed", "20260812")

SOURCE_TABLE = _get_widget("source_table", "dev_sean.default.yt_channel_stats_full")
TARGET_TABLE = _get_widget("target_table", "dev_sean.default.channel_sample_1m_srs")
SAMPLE_SIZE = int(_get_widget("sample_size", "1000000"))
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
# Build the sample and persist it as a new table.
spark.sql(f"""
    CREATE TABLE {TARGET_TABLE} AS
    WITH latest AS (
        SELECT
            canonical_id,
            max_by(channel_name, collected_at) AS channel_name,
            max_by(subscriber_count, collected_at) AS subscriber_count,
            max_by(total_view_count, collected_at) AS total_view_count,
            max(collected_at) AS stats_as_of
        FROM {SOURCE_TABLE}
        GROUP BY canonical_id
    ),
    keyed AS (
        SELECT *, rand({RANDOM_SEED}) AS sample_key
        FROM latest
    )
    SELECT canonical_id, channel_name, subscriber_count, total_view_count, stats_as_of, sample_key
    FROM keyed
    ORDER BY sample_key
    LIMIT {SAMPLE_SIZE}
""")

# COMMAND ----------
# Post-flight verification — run after the write.
verify = spark.sql(f"""
    SELECT
        COUNT(*) AS n_rows,
        COUNT(DISTINCT canonical_id) AS n_distinct_channels,
        MIN(subscriber_count) AS min_subs,
        MAX(subscriber_count) AS max_subs,
        SUM(CASE WHEN subscriber_count IS NULL THEN 1 ELSE 0 END) AS null_subs,
        MIN(total_view_count) AS min_views,
        MAX(total_view_count) AS max_views
    FROM {TARGET_TABLE}
""").collect()[0]
print(verify.asDict())

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 10"))
