# Databricks notebook source
# MAGIC %md
# MAGIC # Build LID v3 10k validation cohort
# MAGIC
# MAGIC Creates a deterministic 10,000-channel scratch cohort split 50/50 between
# MAGIC prior top-of-ocean OpenLID/GlotLID exact primary agreement and exact primary
# MAGIC disagreement. This notebook does not run inference and does not mutate source
# MAGIC production tables.

# COMMAND ----------
from datetime import datetime, timezone
import json
import re
from typing import Dict, Iterable, Optional

from pyspark.sql import Window
from pyspark.sql import functions as F

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
        return default


def _get_int_widget(name: str, default: int) -> int:
    raw = _get_widget(name, str(default)).strip()
    return int(raw) if raw else default


def _get_bool_widget(name: str, default: bool) -> bool:
    raw = _get_widget(name, str(default)).strip().lower()
    if raw in {"true", "1", "yes", "y"}:
        return True
    if raw in {"false", "0", "no", "n"}:
        return False
    return default


def _safe_token(raw: str, default: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "_", (raw or "").strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token or default


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _fqtn(catalog: str, schema: str, table: str) -> str:
    return f"{_quote(catalog)}.{_quote(schema)}.{_quote(table)}"


def _table_exists(catalog: str, schema: str, table: str) -> bool:
    try:
        spark.table(_fqtn(catalog, schema, table)).limit(0).count()
        return True
    except Exception:
        return False


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


def _metric_rows(rows):
    return spark.createDataFrame(
        [(str(k), str(v), datetime.now(timezone.utc).isoformat()) for k, v in rows],
        "metric string, value string, recorded_at string",
    )

# COMMAND ----------
_create_text_widget("source_catalog", "prod_tads")
_create_text_widget("source_schema", "youtube")
_create_text_widget("source_channels_table", "yt_sl_channels")
_create_text_widget("source_videos_table", "yt_sl_videos")
_create_text_widget("prior_catalog", "dev_sean")
_create_text_widget("prior_schema", "matt")
_create_text_widget("prior_comparison_table", "yt_lid_v3_channel_model_comparison")
_create_text_widget("prior_run_id", "default")
_create_text_widget("scratch_catalog", "dev_sean")
_create_text_widget("scratch_schema", "matt")
_create_text_widget("output_prefix", "yt_lid_v3_validation_10k")
_create_text_widget("sample_per_stratum", "5000")
_create_text_widget("random_seed", "20260608")
_create_text_widget("channel_id_column", "channel_id")
_create_text_widget("source_videos_per_channel", "10")
_create_text_widget("allow_unbounded_source_videos", "false")
_create_text_widget("video_rank_column", "")
_create_text_widget("driver_shuffle_partitions", "256")

# COMMAND ----------
SOURCE_CATALOG = _get_widget("source_catalog", "prod_tads")
SOURCE_SCHEMA = _get_widget("source_schema", "youtube")
SOURCE_CHANNELS_TABLE = _get_widget("source_channels_table", "yt_sl_channels")
SOURCE_VIDEOS_TABLE = _get_widget("source_videos_table", "yt_sl_videos")
PRIOR_CATALOG = _get_widget("prior_catalog", "dev_sean")
PRIOR_SCHEMA = _get_widget("prior_schema", "matt")
PRIOR_COMPARISON_TABLE = _get_widget("prior_comparison_table", "yt_lid_v3_channel_model_comparison")
PRIOR_RUN_ID = _get_widget("prior_run_id", "default")
SCRATCH_CATALOG = _get_widget("scratch_catalog", "dev_sean")
SCRATCH_SCHEMA = _get_widget("scratch_schema", "matt")
OUTPUT_PREFIX = _safe_token(_get_widget("output_prefix", "yt_lid_v3_validation_10k"), "yt_lid_v3_validation_10k")
SAMPLE_PER_STRATUM = _get_int_widget("sample_per_stratum", 5000)
RANDOM_SEED = _get_int_widget("random_seed", 20260608)
CHANNEL_ID_COLUMN = _get_widget("channel_id_column", "channel_id")
SOURCE_VIDEOS_PER_CHANNEL = _get_int_widget("source_videos_per_channel", 10)
ALLOW_UNBOUNDED_SOURCE_VIDEOS = _get_bool_widget("allow_unbounded_source_videos", False)
VIDEO_RANK_COLUMN = _get_widget("video_rank_column", "").strip()
DRIVER_SHUFFLE_PARTITIONS = _get_int_widget("driver_shuffle_partitions", 256)

if SOURCE_VIDEOS_PER_CHANNEL < 0:
    raise ValueError("source_videos_per_channel must be non-negative.")
if SOURCE_VIDEOS_PER_CHANNEL == 0 and not ALLOW_UNBOUNDED_SOURCE_VIDEOS:
    raise ValueError(
        "source_videos_per_channel=0 would write all source videos for sampled channels. Set "
        "allow_unbounded_source_videos=true only for an intentional unbounded source-video run, or set a "
        "positive source_videos_per_channel cap."
    )

spark.conf.set("spark.sql.shuffle.partitions", str(DRIVER_SHUFFLE_PARTITIONS))

VIDEO_RANK_CANDIDATES = [
    "published_at", "publish_time", "published_time", "upload_date", "created_time",
    "created_at", "first_capture_time", "ingestion_timestamp", "capture_date",
]
VIDEO_ID_CANDIDATES = ["video_id", "id", "yt_video_id", "external_video_id"]

COHORT_TABLE = f"{OUTPUT_PREFIX}_cohort_sample"
CHANNELS_TABLE_OUT = f"{OUTPUT_PREFIX}_source_channels"
VIDEOS_TABLE_OUT = f"{OUTPUT_PREFIX}_source_videos"
PREFLIGHT_TABLE = f"{OUTPUT_PREFIX}_preflight_metrics"

print("source channels:", _fqtn(SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_CHANNELS_TABLE))
print("source videos:", _fqtn(SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_VIDEOS_TABLE))
print("prior comparison:", _fqtn(PRIOR_CATALOG, PRIOR_SCHEMA, PRIOR_COMPARISON_TABLE), "run_id=", PRIOR_RUN_ID)
print("scratch cohort:", _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, COHORT_TABLE))

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote(SCRATCH_CATALOG)}.{_quote(SCRATCH_SCHEMA)}")

for catalog, schema, table in [
    (SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_CHANNELS_TABLE),
    (SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_VIDEOS_TABLE),
    (PRIOR_CATALOG, PRIOR_SCHEMA, PRIOR_COMPARISON_TABLE),
]:
    if not _table_exists(catalog, schema, table):
        raise ValueError(f"Required table does not exist or is not readable: {catalog}.{schema}.{table}")

channels_src = spark.table(_fqtn(SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_CHANNELS_TABLE))
videos_src = spark.table(_fqtn(SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_VIDEOS_TABLE))
prior_cmp = spark.table(_fqtn(PRIOR_CATALOG, PRIOR_SCHEMA, PRIOR_COMPARISON_TABLE))

if CHANNEL_ID_COLUMN not in channels_src.columns:
    raise ValueError(f"Channel ID column `{CHANNEL_ID_COLUMN}` not found in source channels table.")
if CHANNEL_ID_COLUMN not in videos_src.columns:
    raise ValueError(f"Channel ID column `{CHANNEL_ID_COLUMN}` not found in source videos table.")
if "channel_id" not in prior_cmp.columns:
    raise ValueError("Prior comparison table does not contain `channel_id`.")
if "run_id" not in prior_cmp.columns:
    raise ValueError("Prior comparison table does not contain `run_id`.")

required_prior = [
    "openlid_primary_language_label",
    "glotlid_primary_language_label",
]
missing_prior = [c for c in required_prior if c not in prior_cmp.columns]
if missing_prior:
    raise ValueError(f"Prior comparison table missing required columns: {missing_prior}")

if "models_agree_exact_primary" in prior_cmp.columns:
    exact_agree_expr = F.coalesce(F.col("models_agree_exact_primary"), F.lit(False))
else:
    exact_agree_expr = (
        F.col("openlid_primary_language_label").isNotNull()
        & F.col("glotlid_primary_language_label").isNotNull()
        & (F.col("openlid_primary_language_label") == F.col("glotlid_primary_language_label"))
    )

prior_base = (
    prior_cmp
    .where(F.col("run_id") == F.lit(PRIOR_RUN_ID))
    .where(F.col("openlid_primary_language_label").isNotNull())
    .where(F.col("glotlid_primary_language_label").isNotNull())
    .select(
        F.col("channel_id").cast("string").alias("channel_id"),
        exact_agree_expr.alias("previous_models_agree_exact_primary"),
        "openlid_primary_language_label",
        "glotlid_primary_language_label",
        *([F.col("consensus_status")] if "consensus_status" in prior_cmp.columns else [F.lit(None).cast("string").alias("consensus_status")]),
        *([F.col("models_agree_iso_primary")] if "models_agree_iso_primary" in prior_cmp.columns else [F.lit(None).cast("boolean").alias("models_agree_iso_primary")]),
        *([F.col("models_agree_analysis_cluster_primary")] if "models_agree_analysis_cluster_primary" in prior_cmp.columns else [F.lit(None).cast("boolean").alias("models_agree_analysis_cluster_primary")]),
    )
)

available = (
    prior_base
    .groupBy("previous_models_agree_exact_primary")
    .count()
    .collect()
)
available_counts = {
    str(r["previous_models_agree_exact_primary"]).strip().lower(): int(r["count"])
    for r in available
    if r["previous_models_agree_exact_primary"] is not None
}
print("Prior both-primary availability:", available_counts)

if available_counts.get("true", 0) < SAMPLE_PER_STRATUM:
    raise ValueError(f"Only {available_counts.get('true', 0)} prior exact-agreement rows; need {SAMPLE_PER_STRATUM}.")
if available_counts.get("false", 0) < SAMPLE_PER_STRATUM:
    raise ValueError(f"Only {available_counts.get('false', 0)} prior exact-disagreement rows; need {SAMPLE_PER_STRATUM}.")

# COMMAND ----------
agreement = (
    prior_base
    .where(F.col("previous_models_agree_exact_primary"))
    .withColumn("validation_stratum", F.lit("previous_exact_agreement"))
)
disagreement = (
    prior_base
    .where(~F.col("previous_models_agree_exact_primary"))
    .withColumn("validation_stratum", F.lit("previous_exact_disagreement"))
)
cohort_candidates = agreement.unionByName(disagreement)

w = Window.partitionBy("validation_stratum").orderBy(
    F.xxhash64(F.col("channel_id"), F.lit(str(RANDOM_SEED)), F.col("validation_stratum")).asc(),
    F.col("channel_id").asc(),
)
cohort = (
    cohort_candidates
    .withColumn("selection_rank", F.row_number().over(w))
    .where(F.col("selection_rank") <= F.lit(SAMPLE_PER_STRATUM))
    .withColumn("validation_sample_seed", F.lit(RANDOM_SEED))
    .withColumn("prior_run_id", F.lit(PRIOR_RUN_ID))
    .withColumn("validation_created_at", F.current_timestamp())
)

cohort_counts = {r["validation_stratum"]: int(r["count"]) for r in cohort.groupBy("validation_stratum").count().collect()}
if cohort_counts.get("previous_exact_agreement", 0) != SAMPLE_PER_STRATUM:
    raise AssertionError(f"Agreement sample count mismatch: {cohort_counts}")
if cohort_counts.get("previous_exact_disagreement", 0) != SAMPLE_PER_STRATUM:
    raise AssertionError(f"Disagreement sample count mismatch: {cohort_counts}")

cohort_full = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, COHORT_TABLE)
_overwrite_delta(cohort, cohort_full, partition_cols=["validation_stratum"])
print("Wrote cohort sample:", cohort_full, cohort_counts)

# COMMAND ----------
cohort_ids = (
    cohort
    .select(
        F.col("channel_id").alias("_validation_channel_id"),
        "validation_stratum",
        "selection_rank",
    )
    .persist()
)

source_channel_id = F.col(CHANNEL_ID_COLUMN).cast("string")
channels_out = (
    channels_src
    .join(cohort_ids, source_channel_id == F.col("_validation_channel_id"), "inner")
    .drop("_validation_channel_id")
)
if CHANNEL_ID_COLUMN != "channel_id":
    channels_out = channels_out.withColumnRenamed(CHANNEL_ID_COLUMN, "channel_id")
else:
    channels_out = channels_out.withColumn("channel_id", F.col(CHANNEL_ID_COLUMN).cast("string"))

source_video_channel_id = F.col(CHANNEL_ID_COLUMN).cast("string")
videos_out = (
    videos_src
    .join(cohort_ids.select("_validation_channel_id"), source_video_channel_id == F.col("_validation_channel_id"), "inner")
    .drop("_validation_channel_id")
)
if CHANNEL_ID_COLUMN != "channel_id":
    videos_out = videos_out.withColumnRenamed(CHANNEL_ID_COLUMN, "channel_id")
else:
    videos_out = videos_out.withColumn("channel_id", F.col(CHANNEL_ID_COLUMN).cast("string"))

if SOURCE_VIDEOS_PER_CHANNEL > 0:
    video_rank_col = _first_existing_column(videos_out, VIDEO_RANK_CANDIDATES, VIDEO_RANK_COLUMN)
    video_id_col = _first_existing_column(videos_out, VIDEO_ID_CANDIDATES)
    videos_out = videos_out.withColumn(
        "_validation_video_row_hash",
        F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in videos_out.columns]),
    )
    order_cols = []
    if video_rank_col:
        print(f"Restricting source videos to {SOURCE_VIDEOS_PER_CHANNEL}/channel using rank column `{video_rank_col}`.")
        order_cols.append(F.col(video_rank_col).desc_nulls_last())
    else:
        print(f"No source video rank column found. Restricting to {SOURCE_VIDEOS_PER_CHANNEL}/channel by deterministic row hash.")
    if video_id_col:
        order_cols.append(F.col(video_id_col).asc_nulls_last())
    order_cols.append(F.col("_validation_video_row_hash").asc())
    video_window = Window.partitionBy("channel_id").orderBy(*order_cols)
    videos_out = (
        videos_out
        .withColumn("_validation_video_rank_for_lid", F.row_number().over(video_window))
        .where(F.col("_validation_video_rank_for_lid") <= F.lit(SOURCE_VIDEOS_PER_CHANNEL))
        .drop("_validation_video_rank_for_lid", "_validation_video_row_hash")
    )
else:
    video_rank_col = ""
    print("source_videos_per_channel=0 and allow_unbounded_source_videos=true; writing all source videos for sampled channels.")

channels_out_full = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, CHANNELS_TABLE_OUT)
videos_out_full = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, VIDEOS_TABLE_OUT)
_overwrite_delta(channels_out, channels_out_full)
_overwrite_delta(videos_out, videos_out_full)

scratch_channel_rows = spark.table(channels_out_full).count()
scratch_channel_distinct = spark.table(channels_out_full).select("channel_id").distinct().count()
scratch_video_rows = spark.table(videos_out_full).count()
scratch_video_distinct_channels = spark.table(videos_out_full).select("channel_id").distinct().count()

print("Wrote scratch channels:", channels_out_full, scratch_channel_rows, "rows;", scratch_channel_distinct, "distinct channels")
print("Wrote scratch videos:", videos_out_full, scratch_video_rows, "rows;", scratch_video_distinct_channels, "distinct channels with videos")

if scratch_channel_distinct != SAMPLE_PER_STRATUM * 2:
    raise AssertionError(f"Scratch channel distinct count should be {SAMPLE_PER_STRATUM * 2}; got {scratch_channel_distinct}.")

# COMMAND ----------
preflight_metrics = [
    ("spark_version", spark.version),
    ("source_channels_table", f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_CHANNELS_TABLE}"),
    ("source_videos_table", f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_VIDEOS_TABLE}"),
    ("prior_comparison_table", f"{PRIOR_CATALOG}.{PRIOR_SCHEMA}.{PRIOR_COMPARISON_TABLE}"),
    ("prior_run_id", PRIOR_RUN_ID),
    ("available_prior_exact_agreement", available_counts.get("true", 0)),
    ("available_prior_exact_disagreement", available_counts.get("false", 0)),
    ("sample_previous_exact_agreement", cohort_counts.get("previous_exact_agreement", 0)),
    ("sample_previous_exact_disagreement", cohort_counts.get("previous_exact_disagreement", 0)),
    ("scratch_channel_rows", scratch_channel_rows),
    ("scratch_distinct_channels", scratch_channel_distinct),
    ("scratch_video_rows", scratch_video_rows),
    ("scratch_video_distinct_channels", scratch_video_distinct_channels),
    ("source_videos_per_channel", SOURCE_VIDEOS_PER_CHANNEL),
    ("allow_unbounded_source_videos", ALLOW_UNBOUNDED_SOURCE_VIDEOS),
    ("source_video_rank_column", video_rank_col or "<deterministic_hash>"),
    ("cohort_table", f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{COHORT_TABLE}"),
    ("scratch_channels_table", f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{CHANNELS_TABLE_OUT}"),
    ("scratch_videos_table", f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{VIDEOS_TABLE_OUT}"),
]
preflight_full = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, PREFLIGHT_TABLE)
_overwrite_delta(_metric_rows(preflight_metrics), preflight_full)

result = {
    "status": "ok",
    "spark_version": spark.version,
    "cohort_table": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{COHORT_TABLE}",
    "scratch_channels_table": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{CHANNELS_TABLE_OUT}",
    "scratch_videos_table": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{VIDEOS_TABLE_OUT}",
    "preflight_metrics_table": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{PREFLIGHT_TABLE}",
    "cohort_counts": cohort_counts,
    "available_counts": available_counts,
    "scratch_channel_rows": scratch_channel_rows,
    "scratch_channel_distinct": scratch_channel_distinct,
    "scratch_video_rows": scratch_video_rows,
    "scratch_video_distinct_channels": scratch_video_distinct_channels,
    "source_videos_per_channel": SOURCE_VIDEOS_PER_CHANNEL,
    "allow_unbounded_source_videos": ALLOW_UNBOUNDED_SOURCE_VIDEOS,
    "source_video_rank_column": video_rank_col or "<deterministic_hash>",
}
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
