# Databricks notebook source
# ruff: noqa: F821
# MAGIC %run ./full_corpus_dual_sample_design

# COMMAND ----------
"""Choose the recent-video cap from paired OpenLID-v3/GlotLID evidence."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass


def _get(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name).strip() or default
    except Exception:
        return default


_widget("stage", "prepare")
_widget(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
STAGE = _get("stage", "prepare")
CONFIG_PATH = _get(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
CONFIG = json.loads(dbutils.fs.head(CONFIG_PATH, 1024 * 1024))
validate_design_config(CONFIG)

DESIGN_VERSION = CONFIG["design_version"]
LANGUAGE = CONFIG["language"]
CATALOG = CONFIG["output_catalog"]
SCHEMA = CONFIG["output_schema"]
PREFIX_NAME = CONFIG["output_prefix"]
PREFIX = f"{CATALOG}.{SCHEMA}.{PREFIX_NAME}"
EXPERIMENT_PREFIX_NAME = f"{PREFIX_NAME}_lid_video_cutoff"
EXPERIMENT_PREFIX = f"{CATALOG}.{SCHEMA}.{EXPERIMENT_PREFIX_NAME}"
EXPERIMENT_RUN_ID = f"{LANGUAGE['lid_run_id']}_video_cutoff_20260720_v1"
SAMPLE_N = int(LANGUAGE["video_cutoff_experiment_n"])
SAMPLE_SEED = LANGUAGE["video_cutoff_experiment_seed"]
CUTOFFS = sorted({int(value) for value in LANGUAGE["video_cutoff_candidates"]})
ABSOLUTE_GAIN_THRESHOLD = float(LANGUAGE["video_cutoff_absolute_gain_threshold"])
TEST_ALPHA = float(LANGUAGE["video_cutoff_test_alpha"])
MAX_CUTOFF = max(CUTOFFS)

TABLES = {
    "analysis_union": f"{PREFIX}_analysis_union",
    "lid_source_channels": f"{PREFIX}_lid_source_channels",
    "lid_source_videos": f"{PREFIX}_lid_source_videos",
    "sample_channels": f"{EXPERIMENT_PREFIX}_sample_channels",
    "sample_videos": f"{EXPERIMENT_PREFIX}_sample_videos",
    "segments": f"{EXPERIMENT_PREFIX}_lid_segments_input",
    "openlid": f"{EXPERIMENT_PREFIX}_lid_openlid_predictions_compact",
    "glotlid": f"{EXPERIMENT_PREFIX}_lid_glotlid_predictions_compact",
    "channel_detail": f"{EXPERIMENT_PREFIX}_channel_detail",
    "summary": f"{EXPERIMENT_PREFIX}_summary",
    "selection": f"{EXPERIMENT_PREFIX}_selection",
}


def require_table(table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required table does not exist: {table_name}")


def write_table(frame: DataFrame, table_name: str, comment: str) -> None:
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    escaped = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{escaped}'")
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
        f"'design.version'='{DESIGN_VERSION}', "
        f"'language.cutoff_run_id'='{EXPERIMENT_RUN_ID}')"
    )


def prepare() -> dict[str, int]:
    for name in ("analysis_union", "lid_source_channels", "lid_source_videos"):
        require_table(TABLES[name])
    analysis = spark.table(TABLES["analysis_union"])
    eligible = analysis.where(
        (~F.col("has_existing_language_label"))
        & (F.col("language_enrichment_disposition") == F.lit("ready_for_dual_lid"))
        & F.col("selection_route").isin("pps_only", "srs_and_pps")
    ).select("channel_id")
    source_channels = spark.table(TABLES["lid_source_channels"])
    eligible = eligible.join(source_channels.select("channel_id"), "channel_id", "inner")
    sample = (
        eligible.withColumn(
            "cutoff_sample_key",
            F.sha2(F.concat_ws("\x1f", F.col("channel_id"), F.lit(SAMPLE_SEED)), 256),
        )
        .orderBy("cutoff_sample_key", "channel_id")
        .limit(SAMPLE_N)
    )
    sample_channels = source_channels.join(sample, "channel_id", "inner")
    sample_videos = spark.table(TABLES["lid_source_videos"]).join(
        sample.select("channel_id"), "channel_id", "inner"
    )
    write_table(
        sample_channels,
        TABLES["sample_channels"],
        "Deterministic PPS-channel subsample for the recent-video LID cutoff experiment.",
    )
    write_table(
        sample_videos,
        TABLES["sample_videos"],
        "All collected recent videos for the paired recent-video LID cutoff experiment.",
    )
    counts = {
        "eligible_pps_channels": eligible.count(),
        "sample_channels": sample_channels.count(),
        "sample_distinct_channels": sample_channels.select("channel_id").distinct().count(),
        "sample_video_rows": sample_videos.count(),
        "sample_video_channels": sample_videos.select("channel_id").distinct().count(),
    }
    if counts["sample_channels"] != SAMPLE_N or counts["sample_distinct_channels"] != SAMPLE_N:
        raise AssertionError(f"Cutoff sample size or uniqueness failed: {counts}")
    print("CUTOFF EXPERIMENT PREPARE: PASS")
    print(json.dumps(counts, sort_keys=True))
    return counts


SEGMENT_WEIGHTS = {
    "channel_name": 0.25,
    "channel_description": 1.0,
    "video_title": 2.0,
    "video_description": 1.0,
    "video_tags": 0.5,
}
PRIMARY_MIN_SCORE = 0.20
SECONDARY_MIN_SCORE = 0.35
SECONDARY_MIN_SCORE_RATIO = 0.50
SECONDARY_LABEL_VOTE_WEIGHT = 0.20
NOISE_LABEL_REGEX = r"^(zxx|und|noise|null|none|unknown)"


def compact_for_run(table_name: str) -> DataFrame:
    return spark.table(table_name).where(F.col("run_id") == F.lit(EXPERIMENT_RUN_ID))


def build_primary(compact: DataFrame, segment_cutoffs: DataFrame, model_name: str) -> DataFrame:
    carry = [
        "channel_id",
        "segment_id",
        "segment_type",
        "is_valid_text_for_lid",
    ]
    top1 = compact.select(
        *carry,
        F.lit(1).alias("prediction_rank"),
        F.col("label_1").alias("label"),
        F.col("iso639_3_1").alias("iso639_3"),
        F.col("script_1").alias("script"),
        F.col("score_1").alias("score"),
        F.col("score_1").alias("score_1"),
    )
    top2 = compact.select(
        *carry,
        F.lit(2).alias("prediction_rank"),
        F.col("label_2").alias("label"),
        F.col("iso639_3_2").alias("iso639_3"),
        F.col("script_2").alias("script"),
        F.col("score_2").alias("score"),
        F.col("score_1").alias("score_1"),
    )
    admitted = (
        top1.unionByName(top2)
        .join(segment_cutoffs, ["channel_id", "segment_id"], "inner")
        .where(F.col("is_valid_text_for_lid"))
        .where(F.col("label").isNotNull())
        .where(~F.lower(F.col("label")).rlike(NOISE_LABEL_REGEX))
        .where(
            ((F.col("prediction_rank") == 1) & (F.col("score") >= PRIMARY_MIN_SCORE))
            | (
                (F.col("prediction_rank") == 2)
                & (F.col("score") >= SECONDARY_MIN_SCORE)
                & (F.col("score_1") > 0)
                & ((F.col("score") / F.col("score_1")) >= SECONDARY_MIN_SCORE_RATIO)
            )
        )
        .withColumn(
            "segment_weight",
            F.coalesce(
                F.create_map(
                    *[item for key, value in SEGMENT_WEIGHTS.items() for item in (F.lit(key), F.lit(value))]
                )[F.col("segment_type")],
                F.lit(1.0),
            ),
        )
        .withColumn(
            "rank_weight",
            F.when(F.col("prediction_rank") == 1, F.lit(1.0)).otherwise(
                F.lit(SECONDARY_LABEL_VOTE_WEIGHT)
            ),
        )
        .withColumn("weighted_score", F.col("score") * F.col("segment_weight") * F.col("rank_weight"))
    )
    votes = admitted.groupBy("cutoff", "channel_id", "label", "iso639_3", "script").agg(
        F.sum("weighted_score").alias("weighted_score"),
        F.countDistinct("segment_id").alias("segment_count"),
        F.max("score").alias("max_segment_score"),
    )
    rank_window = Window.partitionBy("cutoff", "channel_id").orderBy(
        F.desc("weighted_score"),
        F.desc("segment_count"),
        F.desc("max_segment_score"),
        F.asc("label"),
    )
    return (
        votes.withColumn("language_rank", F.row_number().over(rank_window))
        .where(F.col("language_rank") == 1)
        .select(
            "cutoff",
            "channel_id",
            F.col("label").alias(f"{model_name}_label"),
            F.lower(F.col("iso639_3")).alias(f"{model_name}_iso"),
            F.col("script").alias(f"{model_name}_script"),
        )
    )


def analyze() -> dict[str, object]:
    for name in ("sample_channels", "sample_videos", "segments", "openlid", "glotlid"):
        require_table(TABLES[name])
    sample_ids = spark.table(TABLES["sample_channels"]).select("channel_id").dropDuplicates()
    videos = spark.table(TABLES["sample_videos"])
    video_hash_values = [
        F.coalesce(F.col(name).cast("string"), F.lit("<NULL>")) for name in videos.columns
    ]
    video_dedup_window = Window.partitionBy("video_id").orderBy(
        F.col("_lid_video_row_hash").asc(),
        F.col("video_id").asc_nulls_last(),
    )
    videos = (
        videos.withColumn("_lid_video_row_hash", F.sha2(F.concat_ws("\x1e", *video_hash_values), 256))
        .withColumn("_dedup_rank", F.row_number().over(video_dedup_window))
        .where(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )
    video_rank_window = Window.partitionBy("channel_id").orderBy(
        F.col("position").asc_nulls_last(),
        F.col("video_id").asc_nulls_last(),
        F.col("_lid_video_row_hash").asc(),
    )
    video_ranks = (
        videos.select("channel_id", "video_id", "position", "_lid_video_row_hash")
        .withColumn("video_recency_rank", F.row_number().over(video_rank_window))
        .select("channel_id", "video_id", "video_recency_rank")
    )
    video_counts = video_ranks.groupBy("channel_id").agg(
        F.count(F.lit(1)).alias("available_video_count")
    )
    segments = compact_for_run(TABLES["segments"]).select(
        "channel_id", "video_id", "segment_id", "segment_type"
    )
    cutoff_frame = spark.createDataFrame([(value,) for value in CUTOFFS], "cutoff int")
    segment_cutoffs = (
        segments.join(video_ranks, ["channel_id", "video_id"], "left")
        .crossJoin(F.broadcast(cutoff_frame))
        .where(F.col("video_id").isNull() | (F.col("video_recency_rank") <= F.col("cutoff")))
        .select("channel_id", "segment_id", "cutoff")
    )
    openlid = build_primary(compact_for_run(TABLES["openlid"]), segment_cutoffs, "openlid")
    glotlid = build_primary(compact_for_run(TABLES["glotlid"]), segment_cutoffs, "glotlid")
    channel_grid = (
        sample_ids.join(video_counts, "channel_id", "left")
        .fillna({"available_video_count": 0})
        .withColumn("has_all_50_videos", F.col("available_video_count") >= F.lit(MAX_CUTOFF))
        .crossJoin(F.broadcast(cutoff_frame))
    )
    detail = (
        channel_grid.join(openlid, ["cutoff", "channel_id"], "left")
        .join(glotlid, ["cutoff", "channel_id"], "left")
        .withColumn("openlid_classified", F.col("openlid_iso").isNotNull())
        .withColumn("glotlid_classified", F.col("glotlid_iso").isNotNull())
        .withColumn("both_classified", F.col("openlid_iso").isNotNull() & F.col("glotlid_iso").isNotNull())
        .withColumn(
            "models_agree_iso",
            F.col("both_classified") & (F.col("openlid_iso") == F.col("glotlid_iso")),
        )
        .withColumn(
            "models_agree_exact_label",
            F.col("openlid_label").isNotNull()
            & F.col("glotlid_label").isNotNull()
            & (F.col("openlid_label") == F.col("glotlid_label")),
        )
    )
    full = detail.where(F.col("cutoff") == MAX_CUTOFF).select(
        "channel_id",
        F.col("models_agree_iso").alias("full_models_agree_iso"),
        F.col("openlid_iso").alias("full_openlid_iso"),
        F.col("glotlid_iso").alias("full_glotlid_iso"),
    )
    detail = (
        detail.join(full, "channel_id", "left")
        .withColumn(
            "paired_resolution_difference_vs_full",
            F.col("full_models_agree_iso").cast("int") - F.col("models_agree_iso").cast("int"),
        )
        .withColumn(
            "openlid_primary_stable_vs_full",
            F.col("openlid_iso").eqNullSafe(F.col("full_openlid_iso")),
        )
        .withColumn(
            "glotlid_primary_stable_vs_full",
            F.col("glotlid_iso").eqNullSafe(F.col("full_glotlid_iso")),
        )
    )
    write_table(
        detail,
        TABLES["channel_detail"],
        "Paired channel-level LID agreement and stability at every recent-video cutoff.",
    )
    detail = spark.table(TABLES["channel_detail"])
    base_summary = (
        detail.groupBy("cutoff")
        .agg(
            F.count(F.lit(1)).alias("n_channels"),
            F.avg(F.col("openlid_classified").cast("double")).alias("openlid_coverage"),
            F.avg(F.col("glotlid_classified").cast("double")).alias("glotlid_coverage"),
            F.avg(F.col("both_classified").cast("double")).alias("both_model_coverage"),
            F.avg(F.col("models_agree_iso").cast("double")).alias("dual_resolution_rate"),
            F.avg(F.col("models_agree_exact_label").cast("double")).alias("exact_label_agreement_rate"),
            F.avg(F.col("openlid_primary_stable_vs_full").cast("double")).alias("openlid_stability_vs_full"),
            F.avg(F.col("glotlid_primary_stable_vs_full").cast("double")).alias("glotlid_stability_vs_full"),
            F.avg("paired_resolution_difference_vs_full").alias("paired_gain_vs_full"),
            (
                F.stddev_samp("paired_resolution_difference_vs_full")
                / F.sqrt(F.count(F.lit(1)).cast("double"))
            ).alias("paired_gain_se"),
            F.sum((F.col("paired_resolution_difference_vs_full") == 1).cast("long")).alias("gained_by_full"),
            F.sum((F.col("paired_resolution_difference_vs_full") == -1).cast("long")).alias("lost_by_full"),
            F.sum(F.col("has_all_50_videos").cast("long")).alias("full50_n_channels"),
            F.avg(
                F.when(F.col("has_all_50_videos"), F.col("models_agree_iso").cast("double"))
            ).alias("full50_dual_resolution_rate"),
            F.avg(
                F.when(F.col("has_all_50_videos"), F.col("paired_resolution_difference_vs_full"))
            ).alias("full50_paired_gain_vs_full"),
            (
                F.stddev_samp(
                    F.when(F.col("has_all_50_videos"), F.col("paired_resolution_difference_vs_full"))
                )
                / F.sqrt(F.sum(F.col("has_all_50_videos").cast("long")).cast("double"))
            ).alias("full50_paired_gain_se"),
            F.sum(
                (
                    F.col("has_all_50_videos")
                    & (F.col("paired_resolution_difference_vs_full") == 1)
                ).cast("long")
            ).alias("full50_gained_by_full"),
            F.sum(
                (
                    F.col("has_all_50_videos")
                    & (F.col("paired_resolution_difference_vs_full") == -1)
                ).cast("long")
            ).alias("full50_lost_by_full"),
        )
        .orderBy("cutoff")
    )
    collected = [row.asDict() for row in base_summary.collect()]
    rates = {int(row["cutoff"]): float(row["dual_resolution_rate"]) for row in collected}
    full50_rates = {
        int(row["cutoff"]): float(row["full50_dual_resolution_rate"]) for row in collected
    }
    full_rate = rates[MAX_CUTOFF]
    full50_full_rate = full50_rates[MAX_CUTOFF]
    summary_rows = []
    for row in collected:
        cutoff = int(row["cutoff"])
        n = int(row["n_channels"])
        rate = float(row["dual_resolution_rate"])
        rate_se = math.sqrt(max(rate * (1.0 - rate), 0.0) / n)
        paired_se = float(row["paired_gain_se"] or 0.0)
        paired_gain = float(row["paired_gain_vs_full"] or 0.0)
        if paired_se > 0:
            paired_p = math.erfc(abs(paired_gain / paired_se) / math.sqrt(2.0))
        else:
            paired_p = 1.0 if paired_gain == 0 else 0.0
        full50_n = int(row["full50_n_channels"])
        full50_rate = float(row["full50_dual_resolution_rate"])
        full50_rate_se = math.sqrt(max(full50_rate * (1.0 - full50_rate), 0.0) / full50_n)
        full50_paired_gain = float(row["full50_paired_gain_vs_full"] or 0.0)
        full50_paired_se = float(row["full50_paired_gain_se"] or 0.0)
        if full50_paired_se > 0:
            full50_paired_p = math.erfc(
                abs(full50_paired_gain / full50_paired_se) / math.sqrt(2.0)
            )
        else:
            full50_paired_p = 1.0 if full50_paired_gain == 0 else 0.0
        later_best = max(rates[value] for value in CUTOFFS if value >= cutoff)
        full50_later_best = max(full50_rates[value] for value in CUTOFFS if value >= cutoff)
        max_later_absolute_gain = max(0.0, later_best - rate)
        full50_max_later_absolute_gain = max(0.0, full50_later_best - full50_rate)
        max_later_relative_gain = max_later_absolute_gain / rate if rate > 0 else float("inf")
        summary_rows.append(
            {
                **row,
                "dual_resolution_se": rate_se,
                "dual_resolution_ci_low": max(0.0, rate - 1.96 * rate_se),
                "dual_resolution_ci_high": min(1.0, rate + 1.96 * rate_se),
                "paired_gain_p_value": paired_p,
                "relative_gain_to_full": (full_rate - rate) / rate if rate > 0 else None,
                "full50_dual_resolution_se": full50_rate_se,
                "full50_dual_resolution_ci_low": max(0.0, full50_rate - 1.96 * full50_rate_se),
                "full50_dual_resolution_ci_high": min(1.0, full50_rate + 1.96 * full50_rate_se),
                "full50_paired_gain_p_value": full50_paired_p,
                "full50_relative_gain_to_full": (
                    (full50_full_rate - full50_rate) / full50_rate if full50_rate > 0 else None
                ),
                "max_later_absolute_gain": max_later_absolute_gain,
                "max_later_relative_gain": max_later_relative_gain,
                "full50_max_later_absolute_gain": full50_max_later_absolute_gain,
                "below_absolute_gain_threshold": (
                    max_later_absolute_gain < ABSOLUTE_GAIN_THRESHOLD
                    and full50_max_later_absolute_gain < ABSOLUTE_GAIN_THRESHOLD
                ),
                "paired_gain_nonsignificant": paired_p >= TEST_ALPHA,
            }
        )
    recommended = next(
        (int(row["cutoff"]) for row in summary_rows if row["below_absolute_gain_threshold"]),
        MAX_CUTOFF,
    )
    recorded_at = datetime.now(timezone.utc)
    summary_frame = spark.createDataFrame(summary_rows).withColumn(
        "recommended_cutoff", F.lit(recommended)
    ).withColumn("recorded_at", F.lit(recorded_at).cast("timestamp"))
    write_table(
        summary_frame,
        TABLES["summary"],
        "Paired recent-video cutoff experiment with material-gain and uncertainty diagnostics.",
    )
    selection = spark.createDataFrame(
        [
            (
                EXPERIMENT_RUN_ID,
                recommended,
                ABSOLUTE_GAIN_THRESHOLD,
                TEST_ALPHA,
                "smallest cutoff whose maximum later absolute dual-resolution gain is below 0.3 percentage points in both all channels and the fixed 50-video cohort",
                recorded_at,
            )
        ],
        "run_id string, recommended_cutoff int, absolute_gain_threshold double, test_alpha double, selection_rule string, recorded_at timestamp",
    )
    write_table(selection, TABLES["selection"], "Frozen recommendation from the paired LID cutoff experiment.")
    if detail.count() != SAMPLE_N * len(CUTOFFS):
        raise AssertionError("Cutoff detail did not conserve sample channels across candidates")
    print("VIDEO RANKING: position ASCENDING (0 = newest)")
    print("CUTOFF EXPERIMENT CONSERVATION: PASS")
    for row in summary_rows:
        print(json.dumps(row, sort_keys=True, default=str))
    print(f"RECOMMENDED RECENT VIDEOS PER CHANNEL: {recommended}")
    print(f"CUTOFF SUMMARY: {TABLES['summary']}")
    return {
        "sample_n": SAMPLE_N,
        "recommended_cutoff": recommended,
        "summary_table": TABLES["summary"],
        "selection_table": TABLES["selection"],
    }


STAGES = {"prepare": prepare, "analyze": analyze}
if STAGE not in STAGES:
    raise ValueError(f"Unknown stage {STAGE!r}; expected one of {sorted(STAGES)}")
print(f"RUNNING CUTOFF STAGE: {STAGE}")
RESULT = STAGES[STAGE]()
dbutils.notebook.exit(json.dumps({"stage": STAGE, "result": RESULT}, sort_keys=True, default=str))
