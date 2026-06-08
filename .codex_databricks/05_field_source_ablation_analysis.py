# Databricks notebook source
# MAGIC %md
# MAGIC # LID v3 field-source ablation analysis
# MAGIC
# MAGIC Reconstructs channel-level OpenLID/GlotLID labels from saved compact predictions for field subsets:
# MAGIC channel name, channel description, video titles, video metadata only, and default all fields.

# COMMAND ----------
from datetime import datetime, timezone
import json
import re
from typing import Dict, Iterable, List, Optional

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

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


def _get_float_widget(name: str, default: float) -> float:
    raw = _get_widget(name, str(default)).strip()
    return float(raw) if raw else default


def _safe_token(raw: str, default: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "_", (raw or "").strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token or default


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _fqtn(catalog: str, schema: str, table: str) -> str:
    return f"{_quote(catalog)}.{_quote(schema)}.{_quote(table)}"


def _overwrite_delta(df, table_full: str, partition_cols: Optional[Iterable[str]] = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(table_full)


def _rows(df, limit: int = 200):
    return [r.asDict(recursive=True) for r in df.limit(limit).collect()]

# COMMAND ----------
_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("output_prefix", "yt_lid_v3_validation_10k_20260608_161345_b10")
_create_text_widget("run_id", "codex_10k_20260608_161345_b10")
_create_text_widget("primary_min_score", "0.20")
_create_text_widget("secondary_min_score", "0.35")
_create_text_widget("secondary_min_score_ratio", "0.50")
_create_text_widget("secondary_label_vote_weight", "0.20")
_create_text_widget("mixed_screen_ratio_threshold", "0.40")
_create_text_widget("mixed_screen_min_secondary_segments", "2")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
OUTPUT_PREFIX = _safe_token(_get_widget("output_prefix", "yt_lid_v3_validation_10k_20260608_161345_b10"), "yt_lid_v3_validation_10k_20260608_161345_b10")
RUN_ID = _get_widget("run_id", "codex_10k_20260608_161345_b10")

PRIMARY_MIN_SCORE = _get_float_widget("primary_min_score", 0.20)
SECONDARY_MIN_SCORE = _get_float_widget("secondary_min_score", 0.35)
SECONDARY_MIN_SCORE_RATIO = _get_float_widget("secondary_min_score_ratio", 0.50)
SECONDARY_LABEL_VOTE_WEIGHT = _get_float_widget("secondary_label_vote_weight", 0.20)
MIXED_SCREEN_RATIO_THRESHOLD = _get_float_widget("mixed_screen_ratio_threshold", 0.40)
MIXED_SCREEN_MIN_SECONDARY_SEGMENTS = int(_get_float_widget("mixed_screen_min_secondary_segments", 2))

RUN_TS = datetime.now(timezone.utc).isoformat()

# COMMAND ----------
cohort = spark.table(_fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_cohort_sample")).select(
    "channel_id", "validation_stratum"
)
channels = spark.table(_fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_channels")).where(F.col("run_id") == F.lit(RUN_ID)).select(
    "channel_id",
    "language_status",
    "consensus_status",
    "requires_manual_adjudication",
    "models_agree_exact_primary",
    "models_agree_iso_primary",
)
segments = spark.table(_fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_segments_input")).where(F.col("run_id") == F.lit(RUN_ID)).select(
    "channel_id", "segment_id", "segment_type", "is_valid_text_for_lid"
)
openlid = spark.table(_fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_openlid_predictions_compact")).where(F.col("run_id") == F.lit(RUN_ID))
glotlid = spark.table(_fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_glotlid_predictions_compact")).where(F.col("run_id") == F.lit(RUN_ID))

cohort_n = cohort.select("channel_id").distinct().count()
if cohort_n == 0:
    raise ValueError("No cohort rows found.")

# COMMAND ----------
CONFIGS: List[Dict[str, object]] = [
    {
        "config_name": "default_all_fields_recomputed",
        "config_label": "Default: channel fields + recent video metadata",
        "segment_types": ["channel_name", "channel_description", "video_title", "video_description", "video_tags"],
        "channel_fields_included": True,
        "recent_video_fields_included": True,
    },
    {
        "config_name": "channel_name_only",
        "config_label": "Channel title/name only",
        "segment_types": ["channel_name"],
        "channel_fields_included": True,
        "recent_video_fields_included": False,
    },
    {
        "config_name": "channel_description_only",
        "config_label": "Channel description only",
        "segment_types": ["channel_description"],
        "channel_fields_included": True,
        "recent_video_fields_included": False,
    },
    {
        "config_name": "recent_video_titles_only",
        "config_label": "Recent video titles only",
        "segment_types": ["video_title"],
        "channel_fields_included": False,
        "recent_video_fields_included": True,
    },
    {
        "config_name": "recent_video_descriptions_only",
        "config_label": "Recent video descriptions only",
        "segment_types": ["video_description"],
        "channel_fields_included": False,
        "recent_video_fields_included": True,
    },
    {
        "config_name": "recent_video_metadata_only",
        "config_label": "Recent video title + description + tags only",
        "segment_types": ["video_title", "video_description", "video_tags"],
        "channel_fields_included": False,
        "recent_video_fields_included": True,
    },
    {
        "config_name": "no_channel_name",
        "config_label": "Drop channel title/name",
        "segment_types": ["channel_description", "video_title", "video_description", "video_tags"],
        "channel_fields_included": True,
        "recent_video_fields_included": True,
    },
    {
        "config_name": "no_channel_description",
        "config_label": "Drop channel description",
        "segment_types": ["channel_name", "video_title", "video_description", "video_tags"],
        "channel_fields_included": True,
        "recent_video_fields_included": True,
    },
]

weights = spark.createDataFrame(
    [
        ("channel_name", 0.25),
        ("channel_description", 1.0),
        ("video_title", 2.0),
        ("video_description", 1.0),
        ("video_tags", 0.5),
    ],
    "segment_type string, segment_weight double",
)
config_rows = [
    (
        c["config_name"],
        c["config_label"],
        list(c["segment_types"]),
        bool(c["channel_fields_included"]),
        bool(c["recent_video_fields_included"]),
        idx,
    )
    for idx, c in enumerate(CONFIGS)
]
config_df = spark.createDataFrame(
    config_rows,
    StructType([
        StructField("config_name", StringType(), False),
        StructField("config_label", StringType(), False),
        StructField("segment_types", ArrayType(StringType()), False),
        StructField("channel_fields_included", BooleanType(), False),
        StructField("recent_video_fields_included", BooleanType(), False),
        StructField("config_order", LongType(), False),
    ]),
)

# COMMAND ----------
def long_predictions(compact_df, model_name: str):
    r1 = compact_df.select(
        "channel_id", "segment_id", "segment_type", F.lit(model_name).alias("lid_model"),
        F.lit(1).alias("prediction_rank"), F.col("label_1").alias("label"),
        F.col("iso639_3_1").alias("iso639_3"), F.col("script_1").alias("script"),
        F.col("score_1").alias("score"), F.col("score_1").alias("score_1"),
    ).where(F.col("label").isNotNull() & (F.col("score") >= F.lit(PRIMARY_MIN_SCORE)))
    r2 = compact_df.select(
        "channel_id", "segment_id", "segment_type", F.lit(model_name).alias("lid_model"),
        F.lit(2).alias("prediction_rank"), F.col("label_2").alias("label"),
        F.col("iso639_3_2").alias("iso639_3"), F.col("script_2").alias("script"),
        F.col("score_2").alias("score"), F.col("score_1").alias("score_1"),
    ).where(
        F.col("label").isNotNull()
        & (F.col("score") >= F.lit(SECONDARY_MIN_SCORE))
        & (F.col("score_1") > F.lit(0.0))
        & ((F.col("score") / F.col("score_1")) >= F.lit(SECONDARY_MIN_SCORE_RATIO))
    )
    return r1.unionByName(r2)


predictions = long_predictions(openlid, "openlid").unionByName(long_predictions(glotlid, "glotlid"))

config_segments = (
    segments.where(F.col("is_valid_text_for_lid"))
    .join(config_df, F.array_contains(F.col("segment_types"), F.col("segment_type")), "inner")
    .select("config_name", "channel_id", "segment_id", "segment_type")
)

weighted = (
    predictions
    .join(config_segments, on=["channel_id", "segment_id", "segment_type"], how="inner")
    .join(F.broadcast(weights), on="segment_type", how="left")
    .withColumn("segment_weight", F.coalesce(F.col("segment_weight"), F.lit(1.0)))
    .withColumn("rank_weight", F.when(F.col("prediction_rank") == F.lit(1), F.lit(1.0)).otherwise(F.lit(SECONDARY_LABEL_VOTE_WEIGHT)))
    .withColumn("weighted_score", F.col("score") * F.col("segment_weight") * F.col("rank_weight"))
)

votes = (
    weighted
    .groupBy("config_name", "lid_model", "channel_id", "label", "iso639_3", "script")
    .agg(
        F.sum("weighted_score").alias("weighted_score"),
        F.sum(F.when(F.col("prediction_rank") == F.lit(1), F.col("weighted_score")).otherwise(F.lit(0.0))).alias("top1_weighted_score"),
        F.countDistinct("segment_id").alias("segment_count"),
        F.countDistinct(F.when(F.col("prediction_rank") == F.lit(1), F.col("segment_id"))).alias("top1_segment_count"),
        F.max("score").alias("max_segment_score"),
        F.collect_set("segment_type").alias("segment_types_observed"),
    )
)
rank_window = Window.partitionBy("config_name", "lid_model", "channel_id").orderBy(
    F.desc("weighted_score"), F.desc("segment_count"), F.desc("max_segment_score"), F.asc("label")
)
ranked = votes.withColumn("language_rank", F.row_number().over(rank_window))

total_scores = (
    votes
    .groupBy("config_name", "lid_model", "channel_id")
    .agg(F.sum("weighted_score").alias("total_weighted_score"))
)
top2 = (
    ranked.where(F.col("language_rank") <= F.lit(2))
    .groupBy("config_name", "lid_model", "channel_id")
    .agg(
        F.max(F.when(F.col("language_rank") == 1, F.col("label"))).alias("primary_label"),
        F.max(F.when(F.col("language_rank") == 1, F.col("iso639_3"))).alias("primary_iso"),
        F.max(F.when(F.col("language_rank") == 1, F.col("script"))).alias("primary_script"),
        F.max(F.when(F.col("language_rank") == 1, F.col("weighted_score"))).alias("primary_weighted_score"),
        F.max(F.when(F.col("language_rank") == 1, F.col("segment_count"))).alias("primary_segment_count"),
        F.max(F.when(F.col("language_rank") == 2, F.col("label"))).alias("secondary_label"),
        F.max(F.when(F.col("language_rank") == 2, F.col("iso639_3"))).alias("secondary_iso"),
        F.max(F.when(F.col("language_rank") == 2, F.col("weighted_score"))).alias("secondary_weighted_score"),
        F.max(F.when(F.col("language_rank") == 2, F.col("segment_count"))).alias("secondary_segment_count"),
    )
    .join(total_scores, on=["config_name", "lid_model", "channel_id"], how="left")
    .withColumn("primary_vote_share", F.col("primary_weighted_score") / F.col("total_weighted_score"))
    .withColumn("secondary_primary_ratio", F.col("secondary_weighted_score") / F.col("primary_weighted_score"))
    .withColumn(
        "mixed_screen",
        (F.col("secondary_label").isNotNull())
        & (F.col("secondary_primary_ratio") >= F.lit(MIXED_SCREEN_RATIO_THRESHOLD))
        & (F.col("secondary_segment_count") >= F.lit(MIXED_SCREEN_MIN_SECONDARY_SEGMENTS)),
    )
)

channel_config_coverage = (
    config_segments
    .groupBy("config_name", "channel_id")
    .agg(
        F.countDistinct("segment_id").alias("valid_segment_count"),
        F.countDistinct("segment_type").alias("valid_segment_type_count"),
        F.collect_set("segment_type").alias("valid_segment_types"),
    )
)

MODEL_COLS = [
    "primary_label", "primary_iso", "primary_script", "primary_vote_share",
    "secondary_label", "secondary_iso", "secondary_primary_ratio", "mixed_screen",
]

openlid_top2 = top2.where(F.col("lid_model") == "openlid").select("config_name", "channel_id", *MODEL_COLS)
for col in MODEL_COLS:
    openlid_top2 = openlid_top2.withColumnRenamed(col, f"openlid_{col}")

glotlid_top2 = top2.where(F.col("lid_model") == "glotlid").select("config_name", "channel_id", *MODEL_COLS)
for col in MODEL_COLS:
    glotlid_top2 = glotlid_top2.withColumnRenamed(col, f"glotlid_{col}")

wide = (
    cohort.crossJoin(config_df.select("config_name"))
    .join(channel_config_coverage, on=["config_name", "channel_id"], how="left")
    .join(openlid_top2, on=["config_name", "channel_id"], how="left")
    .join(glotlid_top2, on=["config_name", "channel_id"], how="left")
)

wide = (
    wide
    .join(channels, on="channel_id", how="left")
    .withColumn("both_models_classified", F.col("openlid_primary_label").isNotNull() & F.col("glotlid_primary_label").isNotNull())
    .withColumn("exact_agreement", F.col("both_models_classified") & (F.col("openlid_primary_label") == F.col("glotlid_primary_label")))
    .withColumn("iso_agreement", F.col("both_models_classified") & (F.col("openlid_primary_iso") == F.col("glotlid_primary_iso")))
    .withColumn("any_model_mixed_screen", F.coalesce(F.col("openlid_mixed_screen"), F.lit(False)) | F.coalesce(F.col("glotlid_mixed_screen"), F.lit(False)))
    .withColumn(
        "both_models_same_secondary_screen",
        F.coalesce(F.col("openlid_mixed_screen"), F.lit(False))
        & F.coalesce(F.col("glotlid_mixed_screen"), F.lit(False))
        & (F.col("openlid_secondary_iso") == F.col("glotlid_secondary_iso"))
        & F.col("openlid_secondary_iso").isNotNull(),
    )
    .withColumn("valid_segment_count", F.coalesce(F.col("valid_segment_count"), F.lit(0)))
    .withColumn("has_valid_segments", F.col("valid_segment_count") > F.lit(0))
)

# COMMAND ----------
def summarize(group_cols: List[str]):
    return (
        wide
        .groupBy(*group_cols)
        .agg(
            F.count(F.lit(1)).alias("n_channels"),
            F.sum(F.col("has_valid_segments").cast("long")).alias("channels_with_valid_segments"),
            F.sum(F.col("both_models_classified").cast("long")).alias("channels_both_models_classified"),
            F.avg(F.col("has_valid_segments").cast("double")).alias("valid_segment_coverage_rate"),
            F.avg(F.col("both_models_classified").cast("double")).alias("both_models_classified_rate"),
            F.avg(F.when(F.col("both_models_classified"), F.col("exact_agreement").cast("double"))).alias("exact_agreement_rate_among_classified"),
            F.avg(F.when(F.col("both_models_classified"), F.col("iso_agreement").cast("double"))).alias("iso_agreement_rate_among_classified"),
            F.avg(F.col("exact_agreement").cast("double")).alias("exact_agreement_rate_all_channels"),
            F.avg(F.col("iso_agreement").cast("double")).alias("iso_agreement_rate_all_channels"),
            F.avg(F.col("any_model_mixed_screen").cast("double")).alias("any_model_mixed_screen_rate"),
            F.avg(F.col("both_models_same_secondary_screen").cast("double")).alias("both_models_same_secondary_screen_rate"),
            F.avg(F.coalesce(F.col("models_agree_exact_primary").cast("double"), F.lit(0.0))).alias("saved_default_exact_agreement_rate"),
            F.avg(F.coalesce(F.col("models_agree_iso_primary").cast("double"), F.lit(0.0))).alias("saved_default_iso_agreement_rate"),
            F.avg(F.coalesce(F.col("requires_manual_adjudication").cast("double"), F.lit(0.0))).alias("saved_default_manual_adjudication_rate"),
            F.avg(F.coalesce((F.col("language_status") == F.lit("mixed_language_candidate")).cast("double"), F.lit(0.0))).alias("saved_default_mixed_language_rate"),
            F.avg("valid_segment_count").alias("mean_valid_segments_per_channel"),
        )
    )


summary_base = summarize(["config_name", "validation_stratum"])
overall = summarize(["config_name"]).withColumn("validation_stratum", F.lit("overall")).select(summary_base.columns)
summary = (
    summary_base.unionByName(overall)
    .join(config_df.drop("segment_types"), on="config_name", how="left")
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("output_prefix", F.lit(OUTPUT_PREFIX))
    .withColumn("analysis_recorded_at", F.lit(RUN_TS))
)

details = (
    wide.join(config_df.drop("segment_types"), on="config_name", how="left")
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("output_prefix", F.lit(OUTPUT_PREFIX))
    .withColumn("analysis_recorded_at", F.lit(RUN_TS))
)

field_segment_counts = (
    segments.where(F.col("is_valid_text_for_lid"))
    .groupBy("segment_type")
    .agg(
        F.countDistinct("channel_id").alias("channels_with_valid_segment_type"),
        F.countDistinct("segment_id").alias("valid_segments"),
    )
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("output_prefix", F.lit(OUTPUT_PREFIX))
    .withColumn("analysis_recorded_at", F.lit(RUN_TS))
)

summary_table = _fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_analysis_field_source_ablation_summary")
details_table = _fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_analysis_field_source_ablation_channels")
segment_counts_table = _fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_analysis_field_source_segment_counts")
_overwrite_delta(summary, summary_table)
_overwrite_delta(details, details_table, partition_cols=["config_name"])
_overwrite_delta(field_segment_counts, segment_counts_table)

result_rows = _rows(
    summary
    .where(F.col("validation_stratum") == F.lit("overall"))
    .orderBy(
        F.desc("both_models_classified_rate"),
        F.desc("iso_agreement_rate_all_channels"),
        F.asc("any_model_mixed_screen_rate"),
    ),
    limit=50,
)
stratum_rows = _rows(summary.orderBy("config_order", "validation_stratum"), limit=200)
segment_count_rows = _rows(field_segment_counts.orderBy("segment_type"), limit=20)

dbutils.notebook.exit(json.dumps({
    "status": "ok",
    "run_id": RUN_ID,
    "output_prefix": OUTPUT_PREFIX,
    "summary_table": f"{CATALOG}.{SCHEMA}.{OUTPUT_PREFIX}_analysis_field_source_ablation_summary",
    "details_table": f"{CATALOG}.{SCHEMA}.{OUTPUT_PREFIX}_analysis_field_source_ablation_channels",
    "segment_counts_table": f"{CATALOG}.{SCHEMA}.{OUTPUT_PREFIX}_analysis_field_source_segment_counts",
    "overall": result_rows,
    "by_stratum": stratum_rows,
    "segment_counts": segment_count_rows,
}, sort_keys=True))
