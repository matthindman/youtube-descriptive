# Databricks notebook source
# MAGIC %md
# MAGIC # Language LID v3 subscriber-cohort analysis driver
# MAGIC
# MAGIC This notebook builds two large subscriber-based evaluation cohorts and runs
# MAGIC `01_language_openlid_v3_databricks` against each cohort with compact prediction storage and QA diagnostics:
# MAGIC
# MAGIC 1. The channels with the highest subscriber counts (`cohort_size=100000` by default).
# MAGIC 2. A deterministic random sample of the same size with subscribers at or below the top-cohort cutoff and
# MAGIC    at least 10,000 total subscribers by default, excluding the exact top-cohort channel IDs.
# MAGIC
# MAGIC The source language notebook is left unchanged. This driver writes small cohort source tables, calls the
# MAGIC v3 LID notebook twice with separate `run_id`s, and writes combined result and summary tables for review.
# MAGIC Each run creates a timestamped scratch table family by default; set `analysis_run_suffix` to a stable value
# MAGIC when you intentionally want to overwrite/reuse a previous analysis table family.

# COMMAND ----------
from datetime import datetime, timezone
import os
import re
from typing import Dict, Iterable, Optional, Tuple

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------
# Widget helpers.
def _create_text_widget(name: str, default: str, label: Optional[str] = None) -> None:
    try:
        dbutils.widgets.text(name, default, label or name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None and value != "" else default
    except Exception:
        return os.environ.get(name.upper(), default)


def _get_bool_widget(name: str, default: bool) -> bool:
    raw = _get_widget(name, str(default)).strip().lower()
    return raw in {"1", "true", "t", "yes", "y"}


def _get_int_widget(name: str, default: int) -> int:
    raw = _get_widget(name, str(default)).strip()
    return int(raw) if raw else default


def _safe_token(raw: str, default: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "_", (raw or "").strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token or default


def _count_token(value: int) -> str:
    if value % 1_000_000 == 0:
        return f"{value // 1_000_000}m"
    if value % 1_000 == 0:
        return f"{value // 1_000}k"
    return str(value)


def _fqtn(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _append_path(base: str, *parts: str) -> str:
    clean_base = base.rstrip("/")
    clean_parts = [p.strip("/") for p in parts if p]
    return "/".join([clean_base, *clean_parts])


def _quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _qualified_col(alias: str, column: str):
    return F.col(f"{_quote_identifier(alias)}.{_quote_identifier(column)}")


def _overwrite_delta(df, table_full: str, partition_cols: Optional[Iterable[str]] = None) -> None:
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(table_full)


def _maybe_display(df) -> None:
    if ENABLE_DRIVER_DISPLAYS:
        display(df)


def _columns_lower_map(df) -> Dict[str, str]:
    return {c.lower(): c for c in df.columns}


def _first_existing_column(df, candidates: Iterable[str], override: str = "") -> Optional[str]:
    cmap = _columns_lower_map(df)
    if override:
        if override.lower() in cmap:
            return cmap[override.lower()]
        raise ValueError(f"Requested column `{override}` not found. Available columns: {df.columns}")
    for c in candidates:
        if c.lower() in cmap:
            return cmap[c.lower()]
    return None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------
_create_text_widget("source_catalog", "prod_tads")
_create_text_widget("source_schema", "youtube")
_create_text_widget("source_channels_table", "yt_sl_channels")
_create_text_widget("source_videos_table", "yt_sl_videos")
_create_text_widget("scratch_catalog", "dev_sean")
_create_text_widget("scratch_schema", "matt")

_create_text_widget("channel_id_column", "channel_id")
_create_text_widget("subscriber_column", "")  # blank = auto-detect
_create_text_widget("cohort_size", "100000")
_create_text_widget("random_lower_subscribers", "10000")
_create_text_widget("random_seed", "20260521")

_create_text_widget("analysis_table_prefix", "yt_lid_eval_subscriber")
_create_text_widget("analysis_run_suffix", "")  # blank = UTC timestamp
_create_text_widget("lid_notebook_path", "./01_language_openlid_v3_databricks")
_create_text_widget("notebook_timeout_seconds", "0")
_create_text_widget("run_lid_notebooks", "true")
_create_text_widget("enable_driver_displays", "false")
_create_text_widget("driver_shuffle_partitions", "800")

# Child LID notebook controls. Defaults keep prediction storage compact while
# enabling heavy QA, validation samples, and ablation summaries for careful analysis.
_create_text_widget("production_mode", "false")
_create_text_widget("prediction_output_mode", "compact")
_create_text_widget("run_heavy_qa", "true")
_create_text_widget("create_validation_samples", "true")
_create_text_widget("run_ablation_aggregations", "true")
_create_text_widget("enable_notebook_displays", "false")
_create_text_widget("videos_per_channel", "10")
_create_text_widget("top_k", "5")
_create_text_widget("inference_hash_buckets", "4096")
_create_text_widget("target_segments_per_partition", "250000")
_create_text_widget("min_num_partitions", "800")
_create_text_widget("max_num_partitions", "20000")

_create_text_widget("model_local_path", "/dbfs/models/openlid_v3/openlid-v3.bin")
_create_text_widget("glotlid_local_path", "/Volumes/dev_sean/matt/models/glotlid.bin")
_create_text_widget("download_model_if_missing", "true")
_create_text_widget("model_distribution_mode", "direct_path")
_create_text_widget("checkpoint_dir_base", "dbfs:/tmp/yt_lid_v3/subscriber_cohort_checkpoints")

# COMMAND ----------
SOURCE_CATALOG = _get_widget("source_catalog", "prod_tads")
SOURCE_SCHEMA = _get_widget("source_schema", "youtube")
SOURCE_CHANNELS_TABLE = _get_widget("source_channels_table", "yt_sl_channels")
SOURCE_VIDEOS_TABLE = _get_widget("source_videos_table", "yt_sl_videos")
SCRATCH_CATALOG = _get_widget("scratch_catalog", "dev_sean")
SCRATCH_SCHEMA = _get_widget("scratch_schema", "matt")

CHANNEL_ID_COLUMN = _get_widget("channel_id_column", "channel_id")
SUBSCRIBER_COLUMN_OVERRIDE = _get_widget("subscriber_column", "").strip()
COHORT_SIZE = _get_int_widget("cohort_size", 100000)
RANDOM_LOWER_SUBSCRIBERS = _get_int_widget("random_lower_subscribers", 10000)
RANDOM_SEED = _get_int_widget("random_seed", 20260521)

ANALYSIS_TABLE_PREFIX = _safe_token(_get_widget("analysis_table_prefix", "yt_lid_eval_subscriber"), "yt_lid_eval_subscriber")
RUN_SUFFIX = _safe_token(
    _get_widget("analysis_run_suffix", "") or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
    datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
)
LID_NOTEBOOK_PATH = _get_widget("lid_notebook_path", "./01_language_openlid_v3_databricks")
NOTEBOOK_TIMEOUT_SECONDS = _get_int_widget("notebook_timeout_seconds", 0)
RUN_LID_NOTEBOOKS = _get_bool_widget("run_lid_notebooks", True)
ENABLE_DRIVER_DISPLAYS = _get_bool_widget("enable_driver_displays", False)
DRIVER_SHUFFLE_PARTITIONS = _get_int_widget("driver_shuffle_partitions", 800)

PRODUCTION_MODE = _get_widget("production_mode", "false")
PREDICTION_OUTPUT_MODE = _get_widget("prediction_output_mode", "compact")
RUN_HEAVY_QA = _get_widget("run_heavy_qa", "true")
CREATE_VALIDATION_SAMPLES = _get_widget("create_validation_samples", "true")
RUN_ABLATION_AGGREGATIONS = _get_widget("run_ablation_aggregations", "true")
ENABLE_NOTEBOOK_DISPLAYS = _get_widget("enable_notebook_displays", "false")
VIDEOS_PER_CHANNEL = _get_widget("videos_per_channel", "10")
TOP_K = _get_widget("top_k", "5")
INFERENCE_HASH_BUCKETS = _get_widget("inference_hash_buckets", "4096")
TARGET_SEGMENTS_PER_PARTITION = _get_widget("target_segments_per_partition", "250000")
MIN_NUM_PARTITIONS = _get_widget("min_num_partitions", "800")
MAX_NUM_PARTITIONS = _get_widget("max_num_partitions", "20000")

MODEL_LOCAL_PATH = _get_widget("model_local_path", "/dbfs/models/openlid_v3/openlid-v3.bin")
GLOTLID_LOCAL_PATH = _get_widget("glotlid_local_path", "/Volumes/dev_sean/matt/models/glotlid.bin")
DOWNLOAD_MODEL_IF_MISSING = _get_widget("download_model_if_missing", "true")
MODEL_DISTRIBUTION_MODE = _get_widget("model_distribution_mode", "direct_path")
CHECKPOINT_DIR_BASE = _get_widget("checkpoint_dir_base", "dbfs:/tmp/yt_lid_v3/subscriber_cohort_checkpoints")

if COHORT_SIZE <= 0:
    raise ValueError("cohort_size must be positive.")
if RANDOM_LOWER_SUBSCRIBERS < 0:
    raise ValueError("random_lower_subscribers must be nonnegative.")
if DRIVER_SHUFFLE_PARTITIONS <= 0:
    raise ValueError("driver_shuffle_partitions must be positive.")

SOURCE_CHANNELS_FULL = _fqtn(SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_CHANNELS_TABLE)
SOURCE_VIDEOS_FULL = _fqtn(SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_VIDEOS_TABLE)
SCRATCH = f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}"

TABLE_PREFIX = f"{ANALYSIS_TABLE_PREFIX}_{RUN_SUFFIX}"
LID_OUTPUT_PREFIX = f"{TABLE_PREFIX}_lid_v3"
COHORT_SIZE_TOKEN = _count_token(COHORT_SIZE)
TOP_COHORT_NAME = f"top{COHORT_SIZE_TOKEN}_subscribers"
RANDOM_COHORT_NAME = f"random{COHORT_SIZE_TOKEN}_subscriber_band"

RUN_ID_TOP = f"{TABLE_PREFIX}_top{COHORT_SIZE_TOKEN}"
RUN_ID_RANDOM = f"{TABLE_PREFIX}_random{COHORT_SIZE_TOKEN}_band"

print("Source channels:", SOURCE_CHANNELS_FULL)
print("Source videos:", SOURCE_VIDEOS_FULL)
print("Scratch schema:", SCRATCH)
print("Analysis table prefix:", TABLE_PREFIX)
print("LID output table prefix:", LID_OUTPUT_PREFIX)
print("Top run_id:", RUN_ID_TOP)
print("Random-band run_id:", RUN_ID_RANDOM)
print("LID notebook path:", LID_NOTEBOOK_PATH)
if NOTEBOOK_TIMEOUT_SECONDS == 0:
    print("Notebook timeout: disabled (notebook_timeout_seconds=0).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Build deterministic subscriber cohorts

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCRATCH}")

channels_raw = spark.table(SOURCE_CHANNELS_FULL)
videos_raw = spark.table(SOURCE_VIDEOS_FULL)

if CHANNEL_ID_COLUMN not in channels_raw.columns:
    raise ValueError(f"Channel ID column `{CHANNEL_ID_COLUMN}` not found in {SOURCE_CHANNELS_FULL}.")
if CHANNEL_ID_COLUMN not in videos_raw.columns:
    raise ValueError(f"Channel ID column `{CHANNEL_ID_COLUMN}` not found in {SOURCE_VIDEOS_FULL}.")

SUBSCRIBER_CANDIDATES = [
    "subscriber_count",
    "subscribers_count",
    "subscribers",
    "subscriberCount",
    "subscriber_count_total",
    "subscriber_total",
    "total_subscribers",
    "channel_subscriber_count",
    "subscriberCountText",
]
subscriber_col = _first_existing_column(channels_raw, SUBSCRIBER_CANDIDATES, SUBSCRIBER_COLUMN_OVERRIDE)
if not subscriber_col:
    sub_like_cols = [c for c in channels_raw.columns if "sub" in c.lower()]
    raise ValueError(
        "Could not auto-detect a subscriber column. Set the `subscriber_column` widget. "
        f"Columns containing 'sub': {sub_like_cols}"
    )
print("Subscriber column:", subscriber_col)

# COMMAND ----------
DEDUP_TS_CANDIDATES = [
    "updated_at",
    "modified_at",
    "ingestion_timestamp",
    "created_at",
    "capture_date",
    "first_capture_time",
]
channel_ts_col = _first_existing_column(channels_raw, DEDUP_TS_CANDIDATES)
print("Channel dedup timestamp column:", channel_ts_col or "<none>")

SUBSCRIBER_RAW_COL = "__lid_eval_subscriber_raw"
SUBSCRIBER_NUMBER_STRING_COL = "__lid_eval_subscriber_number_string"
SUBSCRIBER_DIRECT_NUMBER_COL = "__lid_eval_subscriber_direct_number"
SUBSCRIBER_PARSED_NUMBER_COL = "__lid_eval_subscriber_parsed_number"

subscriber_multiplier = (
    F.when(F.col(SUBSCRIBER_RAW_COL).rlike(r"billion|[0-9]\s*b\b"), F.lit(1_000_000_000.0))
    .when(F.col(SUBSCRIBER_RAW_COL).rlike(r"million|[0-9]\s*m\b"), F.lit(1_000_000.0))
    .when(F.col(SUBSCRIBER_RAW_COL).rlike(r"thousand|[0-9]\s*k\b"), F.lit(1_000.0))
    .otherwise(F.lit(1.0))
)
subscriber_count_expr = F.coalesce(
    F.floor(F.col(SUBSCRIBER_DIRECT_NUMBER_COL)).cast("long"),
    F.floor(F.col(SUBSCRIBER_PARSED_NUMBER_COL) * subscriber_multiplier).cast("long"),
)

row_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in channels_raw.columns])), 256)
base_channels = (
    channels_raw
    .withColumn(SUBSCRIBER_RAW_COL, F.lower(F.trim(F.col(subscriber_col).cast("string"))))
    .withColumn(
        SUBSCRIBER_NUMBER_STRING_COL,
        F.regexp_extract(F.regexp_replace(F.col(SUBSCRIBER_RAW_COL), ",", ""), r"([0-9]+(?:\.[0-9]+)?)", 1),
    )
    .withColumn(SUBSCRIBER_DIRECT_NUMBER_COL, F.expr(f"try_cast({_quote_identifier(subscriber_col)} as double)"))
    .withColumn(SUBSCRIBER_PARSED_NUMBER_COL, F.expr(f"try_cast({_quote_identifier(SUBSCRIBER_NUMBER_STRING_COL)} as double)"))
    .withColumn("__lid_eval_channel_id", F.col(CHANNEL_ID_COLUMN).cast("string"))
    .withColumn("__lid_eval_subscriber_count", subscriber_count_expr)
    .withColumn("__lid_eval_row_hash", row_hash)
    .where(
        F.col("__lid_eval_channel_id").isNotNull()
        & F.col("__lid_eval_subscriber_count").isNotNull()
        & (F.col("__lid_eval_subscriber_count") >= F.lit(0))
    )
)

dedup_order_cols = []
if channel_ts_col:
    dedup_order_cols.append(F.col(channel_ts_col).desc_nulls_last())
dedup_order_cols.extend([
    F.col("__lid_eval_subscriber_count").desc_nulls_last(),
    F.col("__lid_eval_row_hash").asc(),
    F.col("__lid_eval_channel_id").asc(),
])

current_channels = (
    base_channels
    .withColumn("__lid_eval_rn", F.row_number().over(Window.partitionBy("__lid_eval_channel_id").orderBy(*dedup_order_cols)))
    .where(F.col("__lid_eval_rn") == 1)
    .drop(
        "__lid_eval_rn",
        "__lid_eval_row_hash",
        SUBSCRIBER_RAW_COL,
        SUBSCRIBER_NUMBER_STRING_COL,
        SUBSCRIBER_DIRECT_NUMBER_COL,
        SUBSCRIBER_PARSED_NUMBER_COL,
    )
)

channel_universe = current_channels.select(
    F.col("__lid_eval_channel_id").alias("channel_id"),
    F.col("__lid_eval_subscriber_count").alias("subscriber_count"),
).cache()

usable_channel_count = channel_universe.count()
print(f"Channels with usable subscriber counts after deterministic dedup: {usable_channel_count:,}")
if usable_channel_count < COHORT_SIZE:
    raise ValueError(f"Only {usable_channel_count:,} channels have usable subscriber counts; need {COHORT_SIZE:,}.")

# COMMAND ----------
# Use distributed top-k sorts first, then rank only the 100k-row materialized cohorts. A global row_number()
# over the full source universe would force all candidate rows through one Spark partition.
top_unranked = (
    channel_universe
    .orderBy(F.col("subscriber_count").desc(), F.col("channel_id").asc())
    .limit(COHORT_SIZE)
    .cache()
)
top_n = top_unranked.count()
if top_n != COHORT_SIZE:
    raise ValueError(f"Expected {COHORT_SIZE:,} top channels, got {top_n:,}.")

top_window = Window.orderBy(F.col("subscriber_count").desc(), F.col("channel_id").asc())
top_ids = (
    top_unranked
    .withColumn("selection_rank", F.row_number().over(top_window))
    .withColumn("cohort", F.lit(TOP_COHORT_NAME))
    .withColumn("run_id", F.lit(RUN_ID_TOP))
    .cache()
)

top_subscriber_cutoff = top_ids.agg(F.min("subscriber_count").alias("cutoff")).first()["cutoff"]
print(f"Top-{COHORT_SIZE:,} subscriber cutoff: {top_subscriber_cutoff:,}")

n_at_cutoff_total = channel_universe.where(F.col("subscriber_count") == F.lit(top_subscriber_cutoff)).count()
n_at_cutoff_in_top = top_ids.where(F.col("subscriber_count") == F.lit(top_subscriber_cutoff)).count()
n_at_cutoff_outside_top = n_at_cutoff_total - n_at_cutoff_in_top
print(
    f"Channels exactly at cutoff: total={n_at_cutoff_total:,}, "
    f"in top cohort={n_at_cutoff_in_top:,}, outside top cohort={n_at_cutoff_outside_top:,}"
)

random_pool = channel_universe.where(
    (F.col("subscriber_count") >= F.lit(RANDOM_LOWER_SUBSCRIBERS))
    & (F.col("subscriber_count") <= F.lit(top_subscriber_cutoff))
).join(
    top_ids.select("channel_id"),
    on="channel_id",
    how="left_anti",
)
random_pool_n = random_pool.count()
print(
    f"Random-band eligible pool: {random_pool_n:,} channels with "
    f"{RANDOM_LOWER_SUBSCRIBERS:,} <= subscribers <= {top_subscriber_cutoff:,}, "
    "excluding top-cohort channel IDs"
)
if random_pool_n < COHORT_SIZE:
    raise ValueError(f"Only {random_pool_n:,} random-band channels are eligible; need {COHORT_SIZE:,}.")

random_unranked = (
    random_pool
    .withColumn("sample_hash", F.xxhash64(F.col("channel_id"), F.lit(RANDOM_SEED)))
    .orderBy(F.col("sample_hash").asc(), F.col("channel_id").asc())
    .limit(COHORT_SIZE)
    .cache()
)

random_n = random_unranked.count()
if random_n != COHORT_SIZE:
    raise ValueError(f"Expected {COHORT_SIZE:,} random-band channels, got {random_n:,}.")

random_window = Window.orderBy(F.col("sample_hash").asc(), F.col("channel_id").asc())
random_ids = (
    random_unranked
    .withColumn("selection_rank", F.row_number().over(random_window))
    .drop("sample_hash")
    .withColumn("cohort", F.lit(RANDOM_COHORT_NAME))
    .withColumn("run_id", F.lit(RUN_ID_RANDOM))
    .cache()
)

# COMMAND ----------
cutoff_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_subscriber_cutoff")
cohort_ids_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_cohort_ids")

cutoff_df = spark.createDataFrame(
    [(
        int(COHORT_SIZE),
        int(top_subscriber_cutoff),
        int(RANDOM_LOWER_SUBSCRIBERS),
        int(RANDOM_SEED),
        int(top_n),
        int(random_pool_n),
        int(n_at_cutoff_total),
        int(n_at_cutoff_in_top),
        int(n_at_cutoff_outside_top),
        RUN_SUFFIX,
        RUN_ID_TOP,
        RUN_ID_RANDOM,
    )],
    """
    cohort_size long,
    top_subscriber_cutoff long,
    random_lower_subscribers long,
    random_seed long,
    top_cohort_n long,
    random_band_pool_n long,
    n_channels_at_top_cutoff long,
    n_channels_at_top_cutoff_in_top_cohort long,
    n_channels_at_top_cutoff_outside_top_cohort long,
    analysis_run_suffix string,
    top_run_id string,
    random_run_id string
    """,
).withColumn("created_at", F.current_timestamp())

cohort_ids = (
    top_ids.select("cohort", "run_id", "channel_id", "subscriber_count", "selection_rank")
    .unionByName(random_ids.select("cohort", "run_id", "channel_id", "subscriber_count", "selection_rank"))
    .withColumn("top_subscriber_cutoff", F.lit(int(top_subscriber_cutoff)))
    .withColumn("random_lower_subscribers", F.lit(int(RANDOM_LOWER_SUBSCRIBERS)))
    .withColumn("random_seed", F.lit(int(RANDOM_SEED)))
    .withColumn("analysis_run_suffix", F.lit(RUN_SUFFIX))
    .withColumn("selected_at", F.current_timestamp())
)

_overwrite_delta(cutoff_df, cutoff_table)
_overwrite_delta(cohort_ids, cohort_ids_table)

print("Wrote cutoff metadata:", cutoff_table)
print("Wrote cohort IDs:", cohort_ids_table)
_maybe_display(spark.table(cutoff_table))
_maybe_display(spark.table(cohort_ids_table).groupBy("cohort", "run_id").agg(
    F.count(F.lit(1)).alias("n_channels"),
    F.min("subscriber_count").alias("min_subscribers"),
    F.expr("percentile_approx(subscriber_count, 0.5, 10000)").alias("median_subscribers"),
    F.max("subscriber_count").alias("max_subscribers"),
).orderBy("cohort"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Write cohort source tables for the LID notebook

# COMMAND ----------
COHORT_SPECS = [
    (TOP_COHORT_NAME, RUN_ID_TOP),
    (RANDOM_COHORT_NAME, RUN_ID_RANDOM),
]


def _cohort_source_table_names(cohort_name: str) -> Tuple[str, str]:
    safe_cohort = _safe_token(cohort_name, "cohort")
    channels_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_{safe_cohort}_channels")
    videos_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_{safe_cohort}_videos")
    return channels_table, videos_table


combined_channels_source_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_selected_channels_all")
combined_videos_source_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_selected_videos_all")

selected_ids_all = (
    spark.table(cohort_ids_table)
    .select("cohort", F.col("channel_id").alias("__selected_channel_id"))
    .cache()
)
selected_id_count = selected_ids_all.count()
print(f"Selected cohort channel IDs across both cohorts: {selected_id_count:,}")

selected_channels_all = (
    current_channels.alias("c")
    .join(selected_ids_all.alias("i"), F.col("c.__lid_eval_channel_id") == F.col("i.__selected_channel_id"), "inner")
    .select(F.col("i.cohort").alias("__lid_eval_cohort"), "c.*")
    .drop("__lid_eval_channel_id", "__lid_eval_subscriber_count")
)
_overwrite_delta(selected_channels_all, combined_channels_source_table, partition_cols=["__lid_eval_cohort"])
print("Wrote combined selected channels source table:", combined_channels_source_table)

selected_videos_all = (
    videos_raw.alias("v")
    .join(selected_ids_all.alias("i"), _qualified_col("v", CHANNEL_ID_COLUMN).cast("string") == F.col("i.__selected_channel_id"), "inner")
    .select(F.col("i.cohort").alias("__lid_eval_cohort"), "v.*")
)
_overwrite_delta(selected_videos_all, combined_videos_source_table, partition_cols=["__lid_eval_cohort"])
print("Wrote combined selected videos source table:", combined_videos_source_table)
_maybe_display(spark.table(combined_channels_source_table).groupBy("__lid_eval_cohort").count().orderBy("__lid_eval_cohort"))
_maybe_display(spark.table(combined_videos_source_table).groupBy("__lid_eval_cohort").count().orderBy("__lid_eval_cohort"))

for cohort_name, run_id in COHORT_SPECS:
    channels_subset = (
        spark.table(combined_channels_source_table)
        .where(F.col("__lid_eval_cohort") == F.lit(cohort_name))
        .drop("__lid_eval_cohort")
    )
    videos_subset = (
        spark.table(combined_videos_source_table)
        .where(F.col("__lid_eval_cohort") == F.lit(cohort_name))
        .drop("__lid_eval_cohort")
    )
    channels_table, videos_table = _cohort_source_table_names(cohort_name)
    _overwrite_delta(channels_subset, channels_table)
    _overwrite_delta(videos_subset, videos_table)
    n_channels = spark.table(channels_table).count()
    n_videos = spark.table(videos_table).count()
    print(f"{cohort_name}: wrote {n_channels:,} channels to {channels_table}")
    print(f"{cohort_name}: wrote {n_videos:,} videos to {videos_table}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Run the LID v3 notebook for each cohort

# COMMAND ----------
def _lid_output_table_args(prefix: str) -> Dict[str, str]:
    return {
        "output_segments_input_table": f"{prefix}_segments_input",
        "output_openlid_segments_table": f"{prefix}_openlid_segments",
        "output_glotlid_segments_table": f"{prefix}_glotlid_segments",
        "output_glotlid_native_segments_table": f"{prefix}_glotlid_native_segments",
        "output_openlid_compact_table": f"{prefix}_openlid_predictions_compact",
        "output_glotlid_compact_table": f"{prefix}_glotlid_predictions_compact",
        "output_glotlid_native_compact_table": f"{prefix}_glotlid_native_predictions_compact",
        "output_channel_text_features_table": f"{prefix}_channel_text_features",
        "output_segment_model_comparison_table": f"{prefix}_segment_model_comparison",
        "output_channel_votes_table": f"{prefix}_channel_votes",
        "output_channel_model_aggregation_table": f"{prefix}_channel_model_aggregation",
        "output_channel_model_comparison_table": f"{prefix}_channel_model_comparison",
        "output_channels_table": f"{prefix}_channels",
        "output_language_summary_full_table": f"{prefix}_language_summary_full",
        "output_language_summary_rollup_table": f"{prefix}_language_summary_rollup",
        "output_model_agreement_summary_table": f"{prefix}_model_agreement_summary",
        "output_mixed_language_candidates_table": f"{prefix}_mixed_language_candidates",
        "output_hindi_indic_audit_table": f"{prefix}_hindi_indic_audit_candidates",
        "output_suspect_tail_audit_table": f"{prefix}_suspect_tail_audit_sample",
        "output_high_risk_redirect_table": f"{prefix}_high_risk_redirect_diagnostic",
        "output_manual_validation_sample_table": f"{prefix}_manual_validation_sample",
        "output_unclassified_audit_table": f"{prefix}_unclassified_audit",
        "output_source_language_confusion_table": f"{prefix}_source_language_confusion",
        "output_dedupe_qa_table": f"{prefix}_dedupe_qa",
        "output_ablation_summary_table": f"{prefix}_ablation_summary",
    }


def _cohort_lid_output_prefix(cohort_name: str) -> str:
    return f"{LID_OUTPUT_PREFIX}_{_safe_token(cohort_name, 'cohort')}"


COMMON_LID_ARGS = {
    "catalog": SCRATCH_CATALOG,
    "schema": SCRATCH_SCHEMA,
    "channel_id_column": CHANNEL_ID_COLUMN,
    "limit_channels": "0",
    "production_mode": PRODUCTION_MODE,
    "prediction_output_mode": PREDICTION_OUTPUT_MODE,
    "run_heavy_qa": RUN_HEAVY_QA,
    "create_validation_samples": CREATE_VALIDATION_SAMPLES,
    "run_ablation_aggregations": RUN_ABLATION_AGGREGATIONS,
    "enable_notebook_displays": ENABLE_NOTEBOOK_DISPLAYS,
    "videos_per_channel": VIDEOS_PER_CHANNEL,
    "top_k": TOP_K,
    "enable_openlid": "true",
    "enable_glotlid": "true",
    "glotlid_mode": "all_valid_segments",
    "glotlid_preprocessing_mode": "match_openlid",
    "glotlid_native_audit_sample_fraction": "0.00",
    "allow_full_native_audit": "false",
    "inference_hash_buckets": INFERENCE_HASH_BUCKETS,
    "bucket_start": "0",
    "bucket_end": str(int(INFERENCE_HASH_BUCKETS) - 1),
    "target_segments_per_partition": TARGET_SEGMENTS_PER_PARTITION,
    "min_num_partitions": MIN_NUM_PARTITIONS,
    "max_num_partitions": MAX_NUM_PARTITIONS,
    "model_local_path": MODEL_LOCAL_PATH,
    "glotlid_local_path": GLOTLID_LOCAL_PATH,
    "download_model_if_missing": DOWNLOAD_MODEL_IF_MISSING,
    "model_distribution_mode": MODEL_DISTRIBUTION_MODE,
}

child_run_log_rows = []
if RUN_LID_NOTEBOOKS:
    for cohort_name, run_id in COHORT_SPECS:
        channels_table, videos_table = _cohort_source_table_names(cohort_name)
        cohort_output_prefix = _cohort_lid_output_prefix(cohort_name)
        cohort_checkpoint_dir = _append_path(CHECKPOINT_DIR_BASE, RUN_SUFFIX, _safe_token(cohort_name, "cohort"))
        args = {
            **COMMON_LID_ARGS,
            **_lid_output_table_args(cohort_output_prefix),
            "run_id": run_id,
            "channels_table": channels_table.split(".")[-1],
            "videos_table": videos_table.split(".")[-1],
            "checkpoint_dir": cohort_checkpoint_dir,
        }
        print(f"Running {cohort_name}: run_id={run_id} output_prefix={cohort_output_prefix}")
        print(f"Checkpoint dir: {cohort_checkpoint_dir}")
        started_at = datetime.now(timezone.utc).isoformat()
        result = dbutils.notebook.run(LID_NOTEBOOK_PATH, NOTEBOOK_TIMEOUT_SECONDS, args)
        finished_at = datetime.now(timezone.utc).isoformat()
        child_run_log_rows.append((cohort_name, run_id, started_at, finished_at, result))
        print(f"Finished {cohort_name}: {result}")
else:
    print("run_lid_notebooks=false; cohort source tables were written, but LID notebook runs were skipped.")
    print("Cohort IDs:", cohort_ids_table)
    print("Cutoff metadata:", cutoff_table)
    for cohort_name, _ in COHORT_SPECS:
        channels_table, videos_table = _cohort_source_table_names(cohort_name)
        print(f"{cohort_name} channels:", channels_table)
        print(f"{cohort_name} videos:", videos_table)
    dbutils.notebook.exit("Cohort source tables written; LID notebook runs skipped.")

if child_run_log_rows:
    run_log_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_child_run_log")
    run_log = spark.createDataFrame(
        child_run_log_rows,
        "cohort string, run_id string, started_at_utc string, finished_at_utc string, notebook_result string",
    )
    _overwrite_delta(run_log, run_log_table)
    print("Wrote child run log:", run_log_table)

spark.conf.set("spark.sql.shuffle.partitions", str(DRIVER_SHUFFLE_PARTITIONS))
print(f"Reset driver spark.sql.shuffle.partitions to {DRIVER_SHUFFLE_PARTITIONS}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Materialize combined analysis outputs

# COMMAND ----------
def _cohort_lid_table(cohort_name: str, suffix: str) -> str:
    return _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{_cohort_lid_output_prefix(cohort_name)}_{suffix}")


def _union_all_by_name(frames):
    if not frames:
        raise ValueError("No DataFrames to union.")
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame, allowMissingColumns=True)
    return out


lid_output_channels_tables = [_cohort_lid_table(cohort_name, "channels") for cohort_name, _ in COHORT_SPECS]
lid_output_language_summary_tables = [_cohort_lid_table(cohort_name, "language_summary_full") for cohort_name, _ in COHORT_SPECS]
lid_output_validation_sample_tables = [_cohort_lid_table(cohort_name, "manual_validation_sample") for cohort_name, _ in COHORT_SPECS]
lid_output_high_risk_redirect_tables = [_cohort_lid_table(cohort_name, "high_risk_redirect_diagnostic") for cohort_name, _ in COHORT_SPECS]

result_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_language_results")
status_summary_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_language_status_summary")
language_review_summary_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_consensus_language_summary")
review_queue_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_review_queue")
model_agreement_by_cohort_table = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{TABLE_PREFIX}_model_agreement_by_cohort")

cohort_ids_for_join = spark.table(cohort_ids_table).select(
    "cohort",
    "run_id",
    "channel_id",
    "subscriber_count",
    "selection_rank",
    "top_subscriber_cutoff",
    "random_lower_subscribers",
    "random_seed",
)

lid_channels = _union_all_by_name([
    spark.table(_cohort_lid_table(cohort_name, "channels")).where(F.col("run_id") == F.lit(run_id))
    for cohort_name, run_id in COHORT_SPECS
])
results = (
    cohort_ids_for_join
    .join(lid_channels, on=["run_id", "channel_id"], how="left")
    .withColumn("analysis_run_suffix", F.lit(RUN_SUFFIX))
    .withColumn("materialized_at", F.current_timestamp())
)
_overwrite_delta(results, result_table)
print("Wrote combined channel-level results:", result_table)

status_summary = (
    spark.table(result_table)
    .groupBy("cohort", "run_id", "language_status", "consensus_status")
    .agg(
        F.count(F.lit(1)).alias("n_channels"),
        F.avg("subscriber_count").alias("mean_subscribers"),
        F.expr("percentile_approx(subscriber_count, 0.5, 10000)").alias("median_subscribers"),
        F.sum(F.coalesce(F.col("requires_manual_adjudication"), F.lit(False)).cast("int")).alias("n_requires_manual_adjudication"),
    )
    .orderBy("cohort", F.desc("n_channels"))
)
_overwrite_delta(status_summary, status_summary_table)
print("Wrote status summary:", status_summary_table)

language_review_summary = (
    spark.table(result_table)
    .groupBy(
        "cohort",
        "run_id",
        "consensus_language_label",
        "consensus_language_iso639_3",
        "consensus_for_rollup_label",
        "requires_manual_adjudication",
    )
    .agg(
        F.count(F.lit(1)).alias("n_channels"),
        F.avg("openlid_primary_language_vote_share_with_top2").alias("mean_openlid_vote_share"),
        F.avg("glotlid_primary_language_vote_share_with_top2").alias("mean_glotlid_vote_share"),
        F.sum(F.coalesce(F.col("is_mixed_language_candidate"), F.lit(False)).cast("int")).alias("n_mixed_language_candidate"),
        F.sum((F.col("hindi_indic_candidate_status") != F.lit("no_hindi_or_indic_signal")).cast("int")).alias("n_hindi_indic_candidate"),
    )
    .orderBy("cohort", F.desc("n_channels"))
)
_overwrite_delta(language_review_summary, language_review_summary_table)
print("Wrote consensus language summary:", language_review_summary_table)

review_queue = (
    spark.table(result_table)
    .where(
        F.coalesce(F.col("requires_manual_adjudication"), F.lit(False))
        | (F.col("language_status") != F.lit("classified"))
        | F.coalesce(F.col("is_mixed_language_candidate"), F.lit(False))
        | (F.col("hindi_indic_candidate_status") != F.lit("no_hindi_or_indic_signal"))
    )
    .select(
        "cohort",
        "run_id",
        "selection_rank",
        "channel_id",
        "subscriber_count",
        "language_status",
        "consensus_status",
        "consensus_language_label",
        "consensus_for_rollup_label",
        "requires_manual_adjudication",
        "is_mixed_language_candidate",
        "hindi_indic_candidate_status",
        "openlid_primary_language_label",
        "openlid_primary_language_vote_share_with_top2",
        "glotlid_primary_language_label",
        "glotlid_primary_language_vote_share_with_top2",
        "openlid_primary_is_high_risk",
        "glotlid_primary_is_high_risk",
    )
    .orderBy("cohort", "selection_rank")
)
_overwrite_delta(review_queue, review_queue_table)
print("Wrote review queue:", review_queue_table)

model_agreement_by_cohort = (
    _union_all_by_name([
        spark.table(_cohort_lid_table(cohort_name, "model_agreement_summary")).where(F.col("run_id") == F.lit(run_id))
        for cohort_name, run_id in COHORT_SPECS
    ])
    .join(spark.table(cohort_ids_table).select("cohort", "run_id").distinct(), on="run_id", how="left")
    .withColumn("analysis_run_suffix", F.lit(RUN_SUFFIX))
)
_overwrite_delta(model_agreement_by_cohort, model_agreement_by_cohort_table)
print("Wrote model agreement summary:", model_agreement_by_cohort_table)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Review displays

# COMMAND ----------
print("Primary analysis tables")
print("Cohort IDs:", cohort_ids_table)
print("Cutoff metadata:", cutoff_table)
print("Combined language results:", result_table)
print("Language status summary:", status_summary_table)
print("Consensus language summary:", language_review_summary_table)
print("Review queue:", review_queue_table)
print("Model agreement by cohort:", model_agreement_by_cohort_table)
print("LID output channels:", lid_output_channels_tables)
print("LID output language summaries:", lid_output_language_summary_tables)
print("LID output validation samples:", lid_output_validation_sample_tables)
print("LID output high-risk redirects:", lid_output_high_risk_redirect_tables)

_maybe_display(spark.table(cutoff_table))
_maybe_display(spark.table(status_summary_table))
_maybe_display(spark.table(language_review_summary_table).limit(100))
_maybe_display(spark.table(model_agreement_by_cohort_table).orderBy("run_id"))
_maybe_display(spark.table(review_queue_table).limit(200))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Use the printed table names above for detailed review. The two final channel-level cohorts are combined in
# MAGIC the `*_language_results` table, while each cohort's raw LID v3 outputs are preserved under its own
# MAGIC generated `*_lid_v3_<cohort>_*` table family.
