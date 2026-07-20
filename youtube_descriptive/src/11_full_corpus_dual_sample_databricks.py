# Databricks notebook source
# ruff: noqa: F821
# MAGIC %run ./full_corpus_dual_sample_design

# COMMAND ----------
"""Build, evaluate, draw, and stage the registered full-frame dual sample."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone

from pyspark import StorageLevel
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


_widget("stage", "build_frame")
_widget(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)

STAGE = _get("stage", "build_frame")
CONFIG_PATH = _get(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
CONFIG = json.loads(dbutils.fs.head(CONFIG_PATH, 1024 * 1024))
validate_design_config(CONFIG)

DESIGN_VERSION = CONFIG["design_version"]
FRAME_VERSION = CONFIG["frame_version"]
SOURCES = CONFIG["source_tables"]
PREFIX = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_prefix']}"
T0 = CONFIG["t0_date"]
T1 = CONFIG["t1_date"]
ELAPSED_DAYS = int(CONFIG["elapsed_days"])
SUBSCRIBER_THRESHOLD = int(CONFIG["subscriber_threshold"])
SAMPLES = CONFIG["samples"]
SIMULATION = CONFIG["simulation"]

TABLES = {
    "frame": f"{PREFIX}_frame",
    "frame_summary": f"{PREFIX}_frame_summary",
    "frame_scope": f"{PREFIX}_frame_scope",
    "frame_source_versions": f"{PREFIX}_frame_source_versions",
    "unknown_subscriber_audit": f"{PREFIX}_unknown_subscriber_audit",
    "platform_topics": f"{PREFIX}_platform_topics",
    "simulation": f"{PREFIX}_design_simulation",
    "simulation_summary": f"{PREFIX}_design_simulation_summary",
    "frame_probabilities": f"{PREFIX}_frame_probabilities",
    "srs": f"{PREFIX}_srs",
    "pps": f"{PREFIX}_pps",
    "union": f"{PREFIX}_union",
    "sample_qa": f"{PREFIX}_sample_qa",
    "analysis_union": f"{PREFIX}_analysis_union",
    "dispositions": f"{PREFIX}_dispositions",
    "collection_queue": f"{PREFIX}_collection_queue",
    "lid_source_channels": f"{PREFIX}_lid_source_channels",
    "lid_source_videos": f"{PREFIX}_lid_source_videos",
    "model_topic_queue": f"{PREFIX}_model_topic_queue",
    "model_topic_source_videos": f"{PREFIX}_model_topic_source_videos",
    "enrichment_inventory": f"{PREFIX}_enrichment_inventory",
    "collected_descriptions": f"{PREFIX}_collected_channel_descriptions",
    "collected_videos": f"{PREFIX}_collected_channel_videos",
    "collection_dispositions": f"{PREFIX}_collection_dispositions",
}


def write_table(
    frame: DataFrame,
    table_name: str,
    *,
    partition_by: list[str] | None = None,
    comment: str | None = None,
) -> None:
    writer = frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_name)
    if comment:
        escaped = comment.replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {table_name} IS '{escaped}'")
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
        f"'design.version'='{DESIGN_VERSION}', "
        f"'frame.version'='{FRAME_VERSION}')"
    )


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def require_table(table_name: str) -> None:
    if not table_exists(table_name):
        raise RuntimeError(f"Required table does not exist: {table_name}")


def stable_row_hash(columns: list[str]) -> F.Column:
    values = [F.coalesce(F.col(name).cast("string"), F.lit("<NULL>")) for name in columns]
    return F.sha2(F.concat_ws("\x1e", *values), 256)


def source_channel_id(frame: DataFrame) -> F.Column:
    """Normalize collection tables written by either crawler generation."""
    if "channel_id" in frame.columns:
        return F.col("channel_id").cast("string")
    if "canonical_id" in frame.columns:
        return F.col("canonical_id").cast("string")
    raise ValueError("Collection source must contain channel_id or canonical_id")


def sample_hash(channel_col: F.Column, seed: str) -> F.Column:
    return F.sha2(F.concat_ws("\x1f", channel_col.cast("string"), F.lit(FRAME_VERSION), F.lit(seed)), 256)


def uniform_from_hash(hash_col: F.Column) -> F.Column:
    unsigned_decimal = F.conv(F.substring(hash_col, 1, 16), 16, 10).cast(T.DecimalType(20, 0))
    return ((unsigned_decimal.cast("double") + F.lit(0.5)) / F.lit(float(2**64))).cast("double")


def view_stratum() -> F.Column:
    m = F.col("pps_size")
    status = F.col("delta_status")
    expression = (
        F.when(status == F.lit("missing_endpoint"), F.lit("missing_endpoint"))
        .when(status == F.lit("negative_revision"), F.lit("negative_revision"))
        .when(m == F.lit(0.0), F.lit("zero"))
    )
    breaks = [float(value) for value in SIMULATION["view_band_breaks_weekly"]]
    lower = 0.0
    for upper in breaks:
        label = f"positive_{lower:g}_{upper:g}"
        expression = expression.when((m > F.lit(lower)) & (m <= F.lit(upper)), F.lit(label))
        lower = upper
    return expression.otherwise(F.lit(f"positive_{lower:g}_plus"))


def table_version(table_name: str) -> dict[str, object]:
    try:
        row = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").first()
        return {
            "source_table": table_name,
            "delta_version": int(row["version"]),
            "history_timestamp": str(row["timestamp"]),
            "operation": row["operation"],
        }
    except Exception as exc:
        return {
            "source_table": table_name,
            "delta_version": None,
            "history_timestamp": None,
            "operation": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def latest_endpoint_rows() -> DataFrame:
    source = (
        spark.table(SOURCES["channel_stats"])
        .where(F.col("collected_date").isin(T0, T1))
        .where(F.col("canonical_id").isNotNull())
        .select(
            F.col("canonical_id").cast("string").alias("channel_id"),
            F.col("channel_name").cast("string").alias("channel_name"),
            F.col("subscriber_count").cast("long").alias("subscriber_count"),
            F.col("total_view_count").cast("long").alias("total_view_count"),
            F.col("collected_at").cast("timestamp").alias("collected_at"),
            F.col("collected_date").cast("date").alias("collected_date"),
        )
    )
    tie_columns = source.columns
    ranked = source.withColumn("_stable_tie_key", stable_row_hash(tie_columns)).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("channel_id", "collected_date").orderBy(
                F.col("collected_at").desc_nulls_last(),
                F.col("_stable_tie_key").asc(),
            )
        ),
    )
    return ranked.where(F.col("_rn") == 1).drop("_rn")


def latest_topics(frame_ids: DataFrame) -> DataFrame:
    raw = (
        spark.table(SOURCES["channel_topics"])
        .where(F.col("canonical_id").isNotNull())
        .select(
            F.col("canonical_id").cast("string").alias("channel_id"),
            F.col("topic_categories").cast("array<string>").alias("raw_topic_categories"),
            F.col("collected_at").cast("timestamp").alias("topic_collected_at"),
            F.col("collected_date").cast("date").alias("topic_collected_date"),
        )
        .join(frame_ids, "channel_id", "inner")
    )
    ranked = raw.withColumn("_stable_tie_key", stable_row_hash(raw.columns)).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("channel_id").orderBy(
                F.col("topic_collected_at").desc_nulls_last(),
                F.col("topic_collected_date").desc_nulls_last(),
                F.col("_stable_tie_key").asc(),
            )
        ),
    )
    return ranked.where(F.col("_rn") == 1).drop("_rn", "_stable_tie_key")


def build_discovery_scope() -> DataFrame:
    batches = spark.table(SOURCES["discovery_batches"])
    return batches.select(
        F.lit("public_subscriber_full_pass").alias("source"),
        F.col("batch_number").cast("int"),
        F.col("run_date").cast("string"),
        F.col("new_channels_count").cast("long"),
        F.col("new_threshold_count").cast("long"),
        F.col("seeds_top_count").cast("long"),
        F.col("seeds_thresh_count").cast("long"),
        F.col("seeds_total").cast("long"),
        F.current_timestamp().alias("recorded_at"),
        F.lit(DESIGN_VERSION).alias("design_version"),
    )


def build_unknown_subscriber_audit(frame: DataFrame) -> DataFrame:
    unknown = frame.where(F.col("subscriber_status") == F.lit("subscriber_unknown_or_hidden")).select(
        "channel_id", "channel_name", "prior_collected_at"
    )
    metadata_raw = spark.table(SOURCES["channel_metadata"]).select(
        F.col("channel_id").cast("string").alias("channel_id"),
        F.col("fetched_at").cast("string").alias("metadata_fetched_at"),
        F.col("hidden_subscriber_count").cast("boolean").alias("hidden_subscriber_count"),
        F.col("subscriber_count").cast("long").alias("metadata_subscriber_count"),
        F.col("raw_json").cast("string").alias("raw_json"),
    )
    filtered = metadata_raw.join(F.broadcast(unknown.select("channel_id")), "channel_id", "inner")
    ranked = filtered.withColumn("_stable_tie_key", stable_row_hash(filtered.columns)).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("channel_id").orderBy(
                F.to_timestamp("metadata_fetched_at").desc_nulls_last(),
                F.col("_stable_tie_key").asc(),
            )
        ),
    )
    latest = ranked.where(F.col("_rn") == 1).drop("_rn", "_stable_tie_key")
    return (
        unknown.join(latest, "channel_id", "left")
        .withColumn(
            "audit_disposition",
            F.when(F.col("hidden_subscriber_count") == F.lit(True), F.lit("explicitly_hidden"))
            .when(F.col("metadata_subscriber_count").isNotNull(), F.lit("stats_null_metadata_known"))
            .when(F.col("metadata_fetched_at").isNotNull(), F.lit("metadata_row_unresolved"))
            .otherwise(F.lit("no_metadata_row")),
        )
        .withColumn("design_version", F.lit(DESIGN_VERSION))
    )


def run_build_frame() -> dict[str, object]:
    endpoints = latest_endpoint_rows().persist(StorageLevel.DISK_ONLY)
    t0 = endpoints.where(F.col("collected_date") == F.lit(T0).cast("date")).select(
        "channel_id",
        F.col("channel_name").alias("channel_name_t0"),
        F.col("subscriber_count").alias("subscriber_count_t0"),
        F.col("total_view_count").alias("prior_lifetime_views"),
        F.col("collected_at").alias("prior_collected_at"),
        F.col("_stable_tie_key").alias("prior_stable_tie_key"),
    )
    t1 = endpoints.where(F.col("collected_date") == F.lit(T1).cast("date")).select(
        "channel_id",
        F.col("channel_name").alias("channel_name_t1"),
        F.col("subscriber_count").alias("subscriber_count_t1"),
        F.col("total_view_count").alias("current_lifetime_views"),
        F.col("collected_at").alias("current_collected_at"),
        F.col("_stable_tie_key").alias("current_stable_tie_key"),
    )
    base = t0.join(t1, "channel_id", "left")
    raw_delta = F.col("current_lifetime_views") - F.col("prior_lifetime_views")
    endpoint_pair = (
        F.col("prior_lifetime_views").isNotNull()
        & F.col("current_lifetime_views").isNotNull()
        & (F.col("prior_lifetime_views") >= F.lit(0))
        & (F.col("current_lifetime_views") >= F.lit(0))
    )
    delta_status = (
        F.when(~endpoint_pair, F.lit("missing_endpoint"))
        .when(raw_delta < F.lit(0), F.lit("negative_revision"))
        .when(raw_delta == F.lit(0), F.lit("zero"))
        .otherwise(F.lit("positive"))
    )
    subscriber_status = (
        F.when(F.col("subscriber_count_t0").isNull(), F.lit("subscriber_unknown_or_hidden"))
        .when(F.col("subscriber_count_t0") >= F.lit(SUBSCRIBER_THRESHOLD), F.lit("census_ge10k"))
        .otherwise(F.lit("sample_frame_lt10k"))
    )
    frame_without_topics = (
        base.withColumn("channel_name", F.coalesce("channel_name_t1", "channel_name_t0", "channel_id"))
        .withColumn("has_prior_snapshot", F.col("prior_collected_at").isNotNull())
        .withColumn("has_current_snapshot", F.col("current_collected_at").isNotNull())
        .withColumn("has_valid_endpoint_pair", endpoint_pair)
        .withColumn("raw_4wk_net_views", F.when(endpoint_pair, raw_delta).cast("double"))
        .withColumn("avg_net_views_week", F.col("raw_4wk_net_views") / F.lit(ELAPSED_DAYS / 7.0))
        .withColumn(
            "positive_4wk_views",
            F.when(endpoint_pair & (raw_delta >= F.lit(0)), raw_delta).cast("double"),
        )
        .withColumn("positive_avg_views_week", F.col("positive_4wk_views") / F.lit(ELAPSED_DAYS / 7.0))
        .withColumn("accepted_positive_view_mass", F.coalesce("positive_4wk_views", F.lit(0.0)))
        .withColumn("pps_size", F.col("accepted_positive_view_mass") / F.lit(ELAPSED_DAYS / 7.0))
        .withColumn("delta_status", delta_status)
        .withColumn("subscriber_status", subscriber_status)
        .withColumn("frame_version", F.lit(FRAME_VERSION))
        .withColumn("design_version", F.lit(DESIGN_VERSION))
        .withColumn("frame_date", F.lit(T0).cast("date"))
        .withColumn("outcome_date", F.lit(T1).cast("date"))
        .withColumn("elapsed_days", F.lit(ELAPSED_DAYS))
    )

    topics = latest_topics(frame_without_topics.select("channel_id"))
    platform_topics = (
        frame_without_topics.select("channel_id")
        .join(topics, "channel_id", "left")
        .withColumn(
            "topic_row_present",
            F.col("topic_collected_at").isNotNull() | F.col("topic_collected_date").isNotNull(),
        )
        .withColumn(
            "raw_topic_categories",
            F.coalesce(F.col("raw_topic_categories"), F.array().cast("array<string>")),
        )
        .withColumn("has_nonempty_topic_categories", F.size("raw_topic_categories") > F.lit(0))
        .withColumn("design_version", F.lit(DESIGN_VERSION))
    )
    write_table(
        platform_topics,
        TABLES["platform_topics"],
        comment="Latest available platform topic arrays for every frozen-frame channel.",
    )
    platform_topics = spark.table(TABLES["platform_topics"])
    frame = (
        frame_without_topics.join(
            platform_topics.select(
                "channel_id", "topic_row_present", "has_nonempty_topic_categories"
            ),
            "channel_id",
            "left",
        )
        .fillna({"topic_row_present": False, "has_nonempty_topic_categories": False})
        .withColumn("view_stratum", view_stratum())
    )
    write_table(
        frame,
        TABLES["frame"],
        partition_by=["subscriber_status"],
        comment=(
            "One row per channel in the frozen 2026-06-15 frame with deterministic 2026-07-13 "
            "endpoint matching, explicit delta status, phase-one topic availability, and PPS size."
        ),
    )
    frame = spark.table(TABLES["frame"])

    metrics = frame.agg(
        F.count(F.lit(1)).alias("frame_rows"),
        F.countDistinct("channel_id").alias("distinct_frame_channels"),
        F.sum((F.col("subscriber_status") == "sample_frame_lt10k").cast("long")).alias("below10k"),
        F.sum((F.col("subscriber_status") == "census_ge10k").cast("long")).alias("ge10k"),
        F.sum((F.col("subscriber_status") == "subscriber_unknown_or_hidden").cast("long")).alias("unknown"),
        F.sum((F.col("delta_status") == "missing_endpoint").cast("long")).alias("missing_delta"),
        F.sum((F.col("delta_status") == "negative_revision").cast("long")).alias("negative_delta"),
        F.sum((F.col("delta_status") == "zero").cast("long")).alias("zero_delta"),
        F.sum((F.col("delta_status") == "positive").cast("long")).alias("positive_delta"),
        F.sum(F.col("has_valid_endpoint_pair").cast("long")).alias("valid_endpoint_pairs"),
        F.sum("accepted_positive_view_mass").alias("accepted_positive_view_mass"),
        F.sum(F.when(F.col("subscriber_status") == "sample_frame_lt10k", F.col("accepted_positive_view_mass")).otherwise(0.0)).alias("below10k_positive_view_mass"),
        F.sum(F.col("topic_row_present").cast("long")).alias("topic_rows"),
        F.sum(F.col("has_nonempty_topic_categories").cast("long")).alias("nonempty_topic_rows"),
    ).first().asDict()
    if metrics["frame_rows"] != metrics["distinct_frame_channels"]:
        raise AssertionError(f"Frame uniqueness failed: {metrics}")
    if metrics["frame_rows"] != metrics["below10k"] + metrics["ge10k"] + metrics["unknown"]:
        raise AssertionError(f"Subscriber strata do not conserve frame rows: {metrics}")

    summary_rows = [
        (DESIGN_VERSION, str(key), json.dumps(value, default=str), datetime.now(timezone.utc))
        for key, value in metrics.items()
    ]
    write_table(
        spark.createDataFrame(
            summary_rows,
            "design_version string, metric string, value_json string, recorded_at timestamp",
        ),
        TABLES["frame_summary"],
        comment="Frozen-frame acceptance metrics.",
    )
    write_table(build_discovery_scope(), TABLES["frame_scope"], comment="Discovery stopping-rule record.")
    write_table(
        build_unknown_subscriber_audit(frame),
        TABLES["unknown_subscriber_audit"],
        comment="Audit of null subscriber counts against raw channel metadata.",
    )
    version_rows = [table_version(table_name) for table_name in SOURCES.values()]
    version_json = [
        (DESIGN_VERSION, row["source_table"], json.dumps(row, sort_keys=True, default=str))
        for row in version_rows
    ]
    write_table(
        spark.createDataFrame(version_json, "design_version string, source_table string, version_json string"),
        TABLES["frame_source_versions"],
        comment="Input Delta versions and latest-history metadata used by the design.",
    )
    endpoints.unpersist()
    print("FRAME VERSION", FRAME_VERSION)
    for key, value in metrics.items():
        print(f"{key.upper()}: {value}")
    print("FRAME CONSERVATION: PASS")
    return metrics


def tail_frame() -> DataFrame:
    require_table(TABLES["frame"])
    return spark.table(TABLES["frame"]).where(F.col("subscriber_status") == "sample_frame_lt10k")


def pps_q(alpha: float, population_n: int, size_total: float) -> F.Column:
    return F.lit(alpha / population_n) + F.lit((1.0 - alpha) / size_total) * F.col("pps_size")


def solve_spark_waterfill(tail: DataFrame, alpha: float, population_n: int, size_total: float) -> float:
    target_n = float(SAMPLES["pps_expected_n"])
    q = pps_q(alpha, population_n, size_total)
    c_value = target_n
    previous_certainty = -1
    for _ in range(25):
        row = tail.agg(
            F.sum((c_value * q >= F.lit(1.0)).cast("long")).alias("certainty"),
            F.sum(F.when(c_value * q < F.lit(1.0), q).otherwise(F.lit(0.0))).alias("q_remaining"),
        ).first()
        certainty = int(row["certainty"] or 0)
        q_remaining = float(row["q_remaining"] or 0.0)
        if certainty >= target_n or q_remaining <= 0:
            raise RuntimeError(f"Invalid water-filling state: certainty={certainty}, q_remaining={q_remaining}")
        next_c = (target_n - certainty) / q_remaining
        if certainty == previous_certainty and math.isclose(next_c, c_value, rel_tol=1e-12, abs_tol=1e-9):
            return next_c
        previous_certainty = certainty
        c_value = next_c
    raise RuntimeError(f"PPS water-filling failed to converge for alpha={alpha}")


def simulation_outcomes() -> list[tuple[str, F.Column, str, bool]]:
    subscriber = F.col("subscriber_count_t0")
    views = F.col("accepted_positive_view_mass")
    channel_outcomes = [
        ("channel_all", F.lit(1.0), "channel", False),
        ("channel_valid_endpoint", F.col("has_valid_endpoint_pair").cast("double"), "channel", True),
        ("channel_topic_row", F.col("topic_row_present").cast("double"), "channel", True),
        ("channel_topic_nonempty", F.col("has_nonempty_topic_categories").cast("double"), "channel", True),
        ("channel_subs_0", (subscriber == 0).cast("double"), "channel", False),
        ("channel_subs_1_99", subscriber.between(1, 99).cast("double"), "channel", True),
        ("channel_subs_100_999", subscriber.between(100, 999).cast("double"), "channel", True),
        ("channel_subs_1000_9999", subscriber.between(1000, 9999).cast("double"), "channel", True),
    ]
    view_outcomes = [
        ("view_all", views, "view", False),
        ("view_topic_row", views * F.col("topic_row_present").cast("double"), "view", True),
        ("view_topic_nonempty", views * F.col("has_nonempty_topic_categories").cast("double"), "view", True),
        ("view_subs_0_99", views * subscriber.between(0, 99).cast("double"), "view", True),
        ("view_subs_100_999", views * subscriber.between(100, 999).cast("double"), "view", True),
        ("view_subs_1000_9999", views * subscriber.between(1000, 9999).cast("double"), "view", True),
    ]
    return channel_outcomes + view_outcomes


def evaluate_poisson_design(
    tail: DataFrame,
    alpha: float,
    c_value: float,
    population_n: int,
    size_total: float,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    q = pps_q(alpha, population_n, size_total)
    probability = F.least(F.lit(1.0), F.lit(c_value) * q)
    factor = (F.lit(1.0) - probability) / probability
    outcomes = simulation_outcomes()
    aggregations = [
        F.sum(probability).alias("sum_pi"),
        F.sum((probability >= F.lit(1.0)).cast("long")).alias("certainty_channels"),
        F.min(probability).alias("min_pi"),
        F.max(probability).alias("max_pi"),
        F.sum(F.lit(1.0) / probability).alias("sum_inverse_pi"),
    ]
    for name, value, _, _ in outcomes:
        aggregations.append(F.sum(value).alias(f"{name}__total"))
        aggregations.append(F.sum(factor * value * value).alias(f"{name}__variance"))
    values = tail.agg(*aggregations).first().asDict()
    rows: list[dict[str, object]] = []
    for name, _, denominator_type, headline in outcomes:
        total = float(values[f"{name}__total"] or 0.0)
        variance = max(0.0, float(values[f"{name}__variance"] or 0.0))
        denominator = float(population_n if denominator_type == "channel" else size_total * 4.0)
        standard_error = math.sqrt(variance) / denominator if denominator else None
        rows.append(
            {
                "design": "poisson_pps",
                "alpha": alpha,
                "outcome": name,
                "denominator_type": denominator_type,
                "headline": headline,
                "population_total": total,
                "variance_total": variance,
                "standard_error_share": standard_error,
                "relative_standard_error": math.sqrt(variance) / total if total > 0 else None,
            }
        )
    summary = {
        "design": "poisson_pps",
        "alpha": alpha,
        "c_value": c_value,
        "sum_pi": float(values["sum_pi"]),
        "certainty_channels": int(values["certainty_channels"]),
        "min_pi": float(values["min_pi"]),
        "max_pi": float(values["max_pi"]),
        "expected_kish_n": float(population_n) ** 2 / float(values["sum_inverse_pi"]),
        "max_base_weight": 1.0 / float(values["min_pi"]),
    }
    return rows, summary


def evaluate_view_band_design(
    tail: DataFrame,
    alpha: float,
    population_n: int,
    size_total: float,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    outcomes = simulation_outcomes()
    aggregations = [
        F.count(F.lit(1)).alias("population_n"),
        F.sum("pps_size").alias("size_total"),
    ]
    for name, value, _, _ in outcomes:
        aggregations.append(F.sum(value).alias(f"{name}__sum"))
        aggregations.append(F.sum(value * value).alias(f"{name}__sumsq"))
    grouped = [row.asDict() for row in tail.groupBy("view_stratum").agg(*aggregations).collect()]
    allocation = allocate_stratified_counts(
        [
            {
                "stratum": row["view_stratum"],
                "population_n": row["population_n"],
                "size_total": row["size_total"] or 0.0,
            }
            for row in grouped
        ],
        int(SAMPLES["pps_expected_n"]),
        alpha,
    )
    rows: list[dict[str, object]] = []
    for name, _, denominator_type, headline in outcomes:
        total = 0.0
        variance = 0.0
        for stratum in grouped:
            population_h = int(stratum["population_n"])
            sample_h = int(allocation[str(stratum["view_stratum"])])
            sum_h = float(stratum[f"{name}__sum"] or 0.0)
            sumsq_h = float(stratum[f"{name}__sumsq"] or 0.0)
            total += sum_h
            if sample_h >= population_h or population_h <= 1:
                continue
            sample_variance = max(0.0, (sumsq_h - sum_h * sum_h / population_h) / (population_h - 1))
            variance += population_h**2 * (1.0 - sample_h / population_h) * sample_variance / sample_h
        denominator = float(population_n if denominator_type == "channel" else size_total * 4.0)
        rows.append(
            {
                "design": "view_band_srs",
                "alpha": alpha,
                "outcome": name,
                "denominator_type": denominator_type,
                "headline": headline,
                "population_total": total,
                "variance_total": variance,
                "standard_error_share": math.sqrt(variance) / denominator if denominator else None,
                "relative_standard_error": math.sqrt(variance) / total if total > 0 else None,
            }
        )
    min_pi = min(allocation[str(row["view_stratum"])] / int(row["population_n"]) for row in grouped)
    sum_inverse_pi = sum(
        int(row["population_n"]) ** 2 / allocation[str(row["view_stratum"])] for row in grouped
    )
    summary = {
        "design": "view_band_srs",
        "alpha": alpha,
        "c_value": None,
        "sum_pi": float(sum(allocation.values())),
        "certainty_channels": int(
            sum(
                int(row["population_n"])
                for row in grouped
                if allocation[str(row["view_stratum"])] == int(row["population_n"])
            )
        ),
        "min_pi": min_pi,
        "max_pi": max(allocation[str(row["view_stratum"])] / int(row["population_n"]) for row in grouped),
        "expected_kish_n": float(population_n) ** 2 / sum_inverse_pi,
        "max_base_weight": 1.0 / min_pi,
        "stratum_allocation_json": json.dumps(allocation, sort_keys=True),
    }
    return rows, summary


def run_simulation() -> dict[str, object]:
    tail = tail_frame().persist(StorageLevel.DISK_ONLY)
    totals = tail.agg(
        F.count(F.lit(1)).alias("population_n"),
        F.sum("pps_size").alias("size_total"),
    ).first()
    population_n = int(totals["population_n"])
    size_total = float(totals["size_total"])
    if population_n < int(SAMPLES["pps_expected_n"]) or size_total <= 0:
        raise AssertionError(f"Invalid tail totals: N={population_n}, M={size_total}")

    metric_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    for alpha in [float(value) for value in SAMPLES["alpha_candidates"]]:
        c_value = solve_spark_waterfill(tail, alpha, population_n, size_total)
        pps_rows, pps_summary = evaluate_poisson_design(tail, alpha, c_value, population_n, size_total)
        band_rows, band_summary = evaluate_view_band_design(tail, alpha, population_n, size_total)
        metric_rows.extend(pps_rows)
        metric_rows.extend(band_rows)
        summary_rows.append(pps_summary)
        summary_rows.append(band_summary)

    metric_pdf_rows = [
        (
            DESIGN_VERSION,
            row["design"],
            float(row["alpha"]),
            row["outcome"],
            row["denominator_type"],
            bool(row["headline"]),
            float(row["population_total"]),
            float(row["variance_total"]),
            float(row["standard_error_share"]) if row["standard_error_share"] is not None else None,
            float(row["relative_standard_error"]) if row["relative_standard_error"] is not None else None,
        )
        for row in metric_rows
    ]
    write_table(
        spark.createDataFrame(
            metric_pdf_rows,
            "design_version string, design string, alpha double, outcome string, denominator_type string, "
            "headline boolean, population_total double, variance_total double, standard_error_share double, "
            "relative_standard_error double",
        ),
        TABLES["simulation"],
        comment="Analytic finite-population comparison of PPS alpha candidates and fixed view-band SRS.",
    )

    selected_alpha = float(SAMPLES["selected_alpha"])
    pps_scores: dict[float, float] = {}
    for alpha in [float(value) for value in SAMPLES["alpha_candidates"]]:
        candidates = [
            float(row["relative_standard_error"])
            for row in metric_rows
            if row["design"] == "poisson_pps"
            and float(row["alpha"]) == alpha
            and row["headline"]
            and row["relative_standard_error"] is not None
        ]
        pps_scores[alpha] = max(candidates)
    best_alpha = min(pps_scores, key=lambda value: (pps_scores[value], value))
    selected_score = pps_scores[selected_alpha]
    improvement = max(0.0, (selected_score - pps_scores[best_alpha]) / selected_score)
    requires_change = best_alpha != selected_alpha and improvement >= float(
        SIMULATION["material_improvement_fraction"]
    )
    decision = {
        "selected_alpha": selected_alpha,
        "recommended_alpha": best_alpha,
        "selected_worst_headline_rse": selected_score,
        "recommended_worst_headline_rse": pps_scores[best_alpha],
        "relative_improvement": improvement,
        "material_improvement_fraction": float(SIMULATION["material_improvement_fraction"]),
        "requires_config_change": requires_change,
        "population_n": population_n,
        "size_total_weekly": size_total,
        "pps_scores": pps_scores,
    }
    summary_output = [
        (
            DESIGN_VERSION,
            row["design"],
            float(row["alpha"]),
            json.dumps(row, sort_keys=True, default=str),
        )
        for row in summary_rows
    ]
    summary_output.append((DESIGN_VERSION, "alpha_decision", selected_alpha, json.dumps(decision, sort_keys=True)))
    write_table(
        spark.createDataFrame(
            summary_output,
            "design_version string, record_type string, alpha double, summary_json string",
        ),
        TABLES["simulation_summary"],
        comment="Probability, weight, allocation, and registered alpha-decision diagnostics.",
    )
    tail.unpersist()
    print("ALPHA SIMULATION SOURCES: full below-10K frame phase-one outcomes")
    print("ALPHA DECISION:", json.dumps(decision, sort_keys=True))
    print("PPS VS VIEW-BAND SIMULATION: COMPLETE")
    return decision


def read_alpha_decision() -> dict[str, object]:
    require_table(TABLES["simulation_summary"])
    row = (
        spark.table(TABLES["simulation_summary"])
        .where(F.col("record_type") == F.lit("alpha_decision"))
        .orderBy(F.col("alpha").asc())
        .first()
    )
    if row is None:
        raise RuntimeError("The simulation summary has no alpha decision")
    return json.loads(row["summary_json"])


def run_draw_samples() -> dict[str, object]:
    decision = read_alpha_decision()
    if bool(decision["requires_config_change"]):
        raise RuntimeError(
            "The registered simulation found a material alpha improvement. "
            f"selected={decision['selected_alpha']}, recommended={decision['recommended_alpha']}. "
            "Revise the frozen config and design version before drawing samples."
        )
    alpha = float(SAMPLES["selected_alpha"])
    summary_row = (
        spark.table(TABLES["simulation_summary"])
        .where((F.col("record_type") == "poisson_pps") & (F.col("alpha") == F.lit(alpha)))
        .first()
    )
    if summary_row is None:
        raise RuntimeError(f"No simulation summary for selected alpha={alpha}")
    pps_summary = json.loads(summary_row["summary_json"])
    c_value = float(pps_summary["c_value"])

    tail = tail_frame().persist(StorageLevel.DISK_ONLY)
    totals = tail.agg(F.count(F.lit(1)).alias("n"), F.sum("pps_size").alias("m")).first()
    population_n = int(totals["n"])
    size_total = float(totals["m"])
    srs_pi = int(SAMPLES["srs_target_n"]) / population_n
    q = pps_q(alpha, population_n, size_total)
    pps_pi = F.least(F.lit(1.0), F.lit(c_value) * q)

    probability_frame = (
        tail.withColumn("srs_hash", sample_hash(F.col("channel_id"), SAMPLES["srs_seed"]))
        .withColumn("pps_hash", sample_hash(F.col("channel_id"), SAMPLES["pps_seed"]))
        .withColumn("pps_uniform", uniform_from_hash(F.col("pps_hash")))
        .withColumn("pi_srs", F.lit(srs_pi))
        .withColumn("pi_pps", pps_pi)
        .withColumn("selected_pps", F.col("pps_uniform") < F.col("pi_pps"))
    ).persist(StorageLevel.DISK_ONLY)

    srs_ids = (
        probability_frame.select("channel_id", "srs_hash")
        .orderBy(F.col("srs_hash").asc(), F.col("channel_id").asc())
        .limit(int(SAMPLES["srs_target_n"]))
        .withColumn(
            "srs_selection_rank",
            F.row_number().over(Window.orderBy(F.col("srs_hash").asc(), F.col("channel_id").asc())),
        )
    )
    srs_ids = srs_ids.persist(StorageLevel.DISK_ONLY)
    probability_with_srs = (
        probability_frame.join(srs_ids.select("channel_id", "srs_selection_rank"), "channel_id", "left")
        .withColumn("selected_srs", F.col("srs_selection_rank").isNotNull())
        .withColumn("design_version", F.lit(DESIGN_VERSION))
        .withColumn("frame_version", F.lit(FRAME_VERSION))
        .withColumn("srs_seed", F.lit(SAMPLES["srs_seed"]))
        .withColumn("pps_seed", F.lit(SAMPLES["pps_seed"]))
        .withColumn("pps_alpha", F.lit(alpha))
        .withColumn("pps_c", F.lit(c_value))
    )
    write_table(
        probability_with_srs.select(
            "channel_id",
            "frame_version",
            "design_version",
            "srs_hash",
            "srs_selection_rank",
            "selected_srs",
            "pi_srs",
            "pps_hash",
            "pps_uniform",
            "selected_pps",
            "pi_pps",
            "pps_alpha",
            "pps_c",
            "srs_seed",
            "pps_seed",
        ),
        TABLES["frame_probabilities"],
        comment="Registered first-order probabilities, hashes, and route selections for every below-10K frame row.",
    )
    probabilities = spark.table(TABLES["frame_probabilities"])
    tail_payload = tail.drop("design_version", "frame_version")
    srs = (
        probabilities.where(F.col("selected_srs"))
        .join(tail_payload, "channel_id", "inner")
        .withColumn("design_route", F.lit("srs"))
        .withColumn("base_weight", F.lit(1.0) / F.col("pi_srs"))
    )
    pps = (
        probabilities.where(F.col("selected_pps"))
        .join(tail_payload, "channel_id", "inner")
        .withColumn("design_route", F.lit("pps"))
        .withColumn("base_weight", F.lit(1.0) / F.col("pi_pps"))
    )
    write_table(srs, TABLES["srs"], comment="Exact fixed-size SRS from the below-10K frame.")
    write_table(pps, TABLES["pps"], comment="Independent Poisson PPS sample from the below-10K frame.")

    union = (
        probabilities.where(F.col("selected_srs") | F.col("selected_pps"))
        .withColumn("pi_union", F.lit(1.0) - (F.lit(1.0) - F.col("pi_srs")) * (F.lit(1.0) - F.col("pi_pps")))
        .withColumn("base_weight_union", F.lit(1.0) / F.col("pi_union"))
        .withColumn(
            "selection_route",
            F.when(F.col("selected_srs") & F.col("selected_pps"), F.lit("srs_and_pps"))
            .when(F.col("selected_srs"), F.lit("srs_only"))
            .otherwise(F.lit("pps_only")),
        )
    )
    write_table(union, TABLES["union"], comment="Distinct union of independently selected SRS and PPS tail channels.")

    qa = probabilities.agg(
        F.count(F.lit(1)).alias("tail_frame_rows"),
        F.countDistinct("channel_id").alias("tail_frame_distinct"),
        F.sum(F.col("selected_srs").cast("long")).alias("srs_rows"),
        F.sum(F.col("selected_pps").cast("long")).alias("pps_rows"),
        F.sum((F.col("selected_srs") & F.col("selected_pps")).cast("long")).alias("overlap_rows"),
        F.sum("pi_srs").alias("srs_sum_pi"),
        F.sum("pi_pps").alias("pps_sum_pi"),
        F.sum((F.col("pi_pps") >= F.lit(1.0)).cast("long")).alias("pps_certainty_channels"),
        F.sum(((F.col("pi_pps") >= F.lit(1.0)) & ~F.col("selected_pps")).cast("long")).alias("unselected_certainty_channels"),
        F.min("pi_pps").alias("min_pps_pi"),
        F.max("pi_pps").alias("max_pps_pi"),
    ).first().asDict()
    union_rows = spark.table(TABLES["union"]).count()
    qa["union_rows"] = union_rows
    qa["expected_overlap"] = int(SAMPLES["pps_expected_n"]) * srs_pi
    if qa["tail_frame_rows"] != qa["tail_frame_distinct"]:
        raise AssertionError(f"Tail frame uniqueness failed: {qa}")
    if qa["srs_rows"] != int(SAMPLES["srs_target_n"]):
        raise AssertionError(f"SRS target failed: {qa}")
    if abs(float(qa["pps_sum_pi"]) - int(SAMPLES["pps_expected_n"])) > 1e-4:
        raise AssertionError(f"PPS probability conservation failed: {qa}")
    if qa["unselected_certainty_channels"] != 0:
        raise AssertionError(f"A PPS certainty channel was not selected: {qa}")
    if union_rows != qa["srs_rows"] + qa["pps_rows"] - qa["overlap_rows"]:
        raise AssertionError(f"Union conservation failed: {qa}")

    qa_rows = [
        (DESIGN_VERSION, key, json.dumps(value, default=str), datetime.now(timezone.utc))
        for key, value in qa.items()
    ]
    write_table(
        spark.createDataFrame(
            qa_rows,
            "design_version string, metric string, value_json string, recorded_at timestamp",
        ),
        TABLES["sample_qa"],
        comment="Sample-selection acceptance metrics.",
    )
    probability_frame.unpersist()
    srs_ids.unpersist()
    tail.unpersist()
    print(f"SRS TARGET N: {SAMPLES['srs_target_n']}")
    print(f"PPS EXPECTED N: {SAMPLES['pps_expected_n']}")
    for key, value in qa.items():
        print(f"{key.upper()}: {value}")
    print("SAMPLE CONSERVATION: PASS")
    return qa


def latest_description(analysis_ids: DataFrame) -> DataFrame:
    def normalize(table_name: str) -> DataFrame:
        frame = spark.table(table_name)
        return frame.select(
            source_channel_id(frame).alias("channel_id"),
            F.col("channel_name").cast("string").alias("description_channel_name"),
            F.col("channel_description").cast("string").alias("channel_description"),
            F.col("uploads_playlist_id").cast("string").alias("uploads_playlist_id"),
            F.col("collected_at").cast("timestamp").alias("description_collected_at"),
            F.col("collected_date").cast("date").alias("description_collected_date"),
        )

    source_frames = [normalize(SOURCES["channel_descriptions"])]
    if table_exists(TABLES["collected_descriptions"]):
        source_frames.append(normalize(TABLES["collected_descriptions"]))
    raw = source_frames[0]
    for source_frame in source_frames[1:]:
        raw = raw.unionByName(source_frame)
    raw = raw.join(analysis_ids, "channel_id", "inner")
    ranked = raw.withColumn("_stable_tie_key", stable_row_hash(raw.columns)).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("channel_id").orderBy(
                F.col("description_collected_at").desc_nulls_last(),
                F.col("_stable_tie_key").asc(),
            )
        ),
    )
    return ranked.where(F.col("_rn") == 1).drop("_rn", "_stable_tie_key")


def available_video_source(analysis_ids: DataFrame) -> DataFrame:
    def normalize(table_name: str) -> DataFrame:
        frame = spark.table(table_name)
        return frame.select(
            source_channel_id(frame).alias("channel_id"),
            "video_id",
            "video_title",
            "video_description",
            "published_at",
            "position",
            F.col("collected_at").alias("video_collected_at"),
            F.col("collected_date").alias("video_collected_date"),
        )

    source_frames = [normalize(SOURCES["channel_videos"])]
    if table_exists(TABLES["collected_videos"]):
        source_frames.append(normalize(TABLES["collected_videos"]))
    result = source_frames[0]
    for source_frame in source_frames[1:]:
        result = result.unionByName(source_frame)
    return result.join(analysis_ids, "channel_id", "inner").dropDuplicates(
        ["channel_id", "video_id", "position", "video_collected_at"]
    )


def run_stage_enrichment() -> dict[str, object]:
    require_table(TABLES["frame"])
    require_table(TABLES["union"])
    frame = spark.table(TABLES["frame"])
    tail_union = spark.table(TABLES["union"])
    certainty = frame.where(F.col("subscriber_status") != "sample_frame_lt10k").select(
        "channel_id",
        F.lit(True).alias("certainty_stratum"),
        F.lit(1.0).alias("pi_union"),
        F.lit(1.0).alias("base_weight_union"),
        F.when(F.col("subscriber_status") == "census_ge10k", F.lit("census_ge10k"))
        .otherwise(F.lit("subscriber_unknown_certainty"))
        .alias("selection_route"),
    )
    sampled = tail_union.select(
        "channel_id",
        F.lit(False).alias("certainty_stratum"),
        "pi_union",
        "base_weight_union",
        "selection_route",
    )
    ids = certainty.unionByName(sampled).persist(StorageLevel.DISK_ONLY)
    if ids.count() != ids.select("channel_id").distinct().count():
        raise AssertionError("Analysis union IDs are not unique")

    language = spark.table(SOURCES["current_language"])
    expected_label_version = CONFIG["language"]["expected_existing_label_version"]
    language = language.where(F.col("label_version") == F.lit(expected_label_version))
    descriptions = latest_description(ids.select("channel_id"))
    topics = spark.table(TABLES["platform_topics"])
    collection_disposition_frames = []
    if table_exists(TABLES["collection_dispositions"]):
        collection_disposition_frames.append(
            spark.table(TABLES["collection_dispositions"]).select(
                "channel_id", "collection_disposition"
            )
        )
    collection_not_found_table = SOURCES.get("collection_not_found")
    if collection_not_found_table and table_exists(collection_not_found_table):
        not_found = spark.table(collection_not_found_table)
        collection_disposition_frames.append(
            not_found.select(
                source_channel_id(not_found).alias("channel_id"),
                F.lit("not_found_or_unavailable_after_attempt").alias("collection_disposition"),
            ).dropDuplicates(["channel_id"])
        )
    collection_dispositions = None
    if collection_disposition_frames:
        collection_dispositions = collection_disposition_frames[0]
        for disposition_frame in collection_disposition_frames[1:]:
            collection_dispositions = collection_dispositions.unionByName(disposition_frame)
        collection_dispositions = collection_dispositions.dropDuplicates(["channel_id"])

    unlabeled_ids = ids.join(language.select("channel_id"), "channel_id", "left_anti")
    all_video_source = available_video_source(ids.select("channel_id"))
    video_source = all_video_source.join(unlabeled_ids.select("channel_id"), "channel_id", "inner")
    video_coverage = video_source.groupBy("channel_id").agg(
        F.count(F.lit(1)).alias("recent_video_count"),
        F.sum(
            (
                (F.length(F.trim(F.coalesce(F.col("video_title"), F.lit("")))) > 0)
                | (F.length(F.trim(F.coalesce(F.col("video_description"), F.lit("")))) > 0)
            ).cast("long")
        ).alias("videos_with_text"),
    )

    joined = (
        ids.join(frame, "channel_id", "inner")
        .join(language, "channel_id", "left")
        .join(descriptions, "channel_id", "left")
        .join(video_coverage, "channel_id", "left")
        .join(
            topics.select("channel_id", "raw_topic_categories", "topic_collected_at", "topic_collected_date"),
            "channel_id",
            "left",
        )
        .withColumn("has_existing_language_label", F.col("channel_language").isNotNull())
        .withColumn(
            "has_nonempty_channel_description",
            F.length(F.trim(F.coalesce(F.col("channel_description"), F.lit("")))) > 0,
        )
        .withColumn("has_recent_video_text", F.coalesce("videos_with_text", F.lit(0)) > 0)
        .withColumn(
            "topic_enrichment_disposition",
            F.when(F.col("has_nonempty_topic_categories"), F.lit("platform_topic_available"))
            .otherwise(F.lit("requires_model_topic_robustness")),
        )
    )
    if collection_dispositions is not None:
        joined = joined.join(collection_dispositions, "channel_id", "left")
    else:
        joined = joined.withColumn("collection_disposition", F.lit(None).cast("string"))
    completed_without_text = (
        F.col("description_collected_at").isNotNull()
        & (~F.col("has_nonempty_channel_description"))
        & (~F.col("has_recent_video_text"))
    )
    joined = joined.withColumn(
        "collection_disposition",
        F.when(
            F.col("collection_disposition").isNull() & completed_without_text,
            F.lit("channel_retrieved_no_usable_text_after_50_videos"),
        ).otherwise(F.col("collection_disposition")),
    )
    terminal_collection = F.col("collection_disposition").isin(
        "not_found_or_terminated",
        "not_found_or_unavailable_after_attempt",
        "channel_found_without_uploads_playlist",
        "channel_found_no_recent_videos",
        "channel_retrieved_no_usable_text_after_50_videos",
    )
    joined = joined.withColumn(
        "language_enrichment_disposition",
        F.when(F.col("has_existing_language_label"), F.lit("reuse_frozen_label"))
        .when(
            F.col("has_nonempty_channel_description") | F.col("has_recent_video_text"),
            F.lit("ready_for_dual_lid"),
        )
        .when(terminal_collection, F.lit("terminal_no_text_assign_und"))
        .otherwise(F.lit("requires_text_collection")),
    )
    write_table(joined, TABLES["analysis_union"], comment="Census plus selected tail union with current enrichment coverage.")
    dispositions = joined.select(
        "channel_id",
        "subscriber_status",
        "selection_route",
        "pi_union",
        "base_weight_union",
        "language_enrichment_disposition",
        "topic_enrichment_disposition",
        "collection_disposition",
        "has_existing_language_label",
        "has_nonempty_channel_description",
        "has_recent_video_text",
        "topic_row_present",
        "has_nonempty_topic_categories",
        F.lit(DESIGN_VERSION).alias("design_version"),
    )
    write_table(dispositions, TABLES["dispositions"], comment="Complete enrichment and missingness dispositions.")

    collection_queue = joined.where(F.col("language_enrichment_disposition") == "requires_text_collection").select(
        "channel_id",
        "channel_name",
        "subscriber_count_t0",
        "accepted_positive_view_mass",
        "selection_route",
        "pi_union",
        "base_weight_union",
        F.lit(
            f"collect_channel_description_and_recent_{int(CONFIG['collection']['recent_videos_per_channel'])}_videos"
        ).alias("requested_collection"),
        F.lit(DESIGN_VERSION).alias("design_version"),
    )
    write_table(collection_queue, TABLES["collection_queue"], comment="Probability-sample IDs requiring source-text collection before LID.")

    lid_ready = joined.where(F.col("language_enrichment_disposition") == "ready_for_dual_lid")
    lid_source_channels = lid_ready.select(
        "channel_id",
        F.coalesce("description_channel_name", "channel_name").alias("channel_name"),
        "channel_description",
        "uploads_playlist_id",
        F.col("description_collected_at").alias("channel_collected_at"),
        F.col("description_collected_date").alias("channel_collected_date"),
        "selection_route",
        "pi_union",
        "base_weight_union",
    )
    write_table(lid_source_channels, TABLES["lid_source_channels"], comment="Unlabeled analysis channels with enough existing text for dual LID.")
    lid_ready_ids = lid_ready.select("channel_id")
    write_table(
        video_source.join(lid_ready_ids, "channel_id", "inner"),
        TABLES["lid_source_videos"],
        comment="Existing recent-video text for dual-LID-ready analysis channels.",
    )
    model_topic_queue = joined.where(
        F.col("topic_enrichment_disposition") == "requires_model_topic_robustness"
    ).select(
        "channel_id",
        "channel_name",
        "channel_description",
        "selection_route",
        "pi_union",
        "base_weight_union",
        F.lit(DESIGN_VERSION).alias("design_version"),
    )
    write_table(
        model_topic_queue,
        TABLES["model_topic_queue"],
        comment="Channels lacking platform topics and requiring model-completed robustness classification.",
    )
    write_table(
        all_video_source.join(model_topic_queue.select("channel_id"), "channel_id", "inner"),
        TABLES["model_topic_source_videos"],
        comment="Existing recent-video text for channels queued for model-completed topic robustness.",
    )

    inventory = joined.agg(
        F.count(F.lit(1)).alias("analysis_union_rows"),
        F.countDistinct("channel_id").alias("analysis_union_distinct"),
        F.sum(F.col("certainty_stratum").cast("long")).alias("certainty_rows"),
        F.sum(F.col("has_existing_language_label").cast("long")).alias("existing_language_labels"),
        F.sum((F.col("language_enrichment_disposition") == "ready_for_dual_lid").cast("long")).alias("ready_for_dual_lid"),
        F.sum((F.col("language_enrichment_disposition") == "requires_text_collection").cast("long")).alias("requires_text_collection"),
        F.sum((F.col("language_enrichment_disposition") == "terminal_no_text_assign_und").cast("long")).alias("terminal_no_text_assign_und"),
        F.sum(F.col("topic_row_present").cast("long")).alias("topic_rows"),
        F.sum(F.col("has_nonempty_topic_categories").cast("long")).alias("nonempty_topic_rows"),
        F.sum((F.col("topic_enrichment_disposition") == "requires_model_topic_robustness").cast("long")).alias("requires_model_topic_robustness"),
    ).first().asDict()
    if inventory["analysis_union_rows"] != inventory["analysis_union_distinct"]:
        raise AssertionError(f"Analysis union uniqueness failed: {inventory}")
    inventory_rows = [
        (DESIGN_VERSION, key, json.dumps(value, default=str), datetime.now(timezone.utc))
        for key, value in inventory.items()
    ]
    write_table(
        spark.createDataFrame(
            inventory_rows,
            "design_version string, metric string, value_json string, recorded_at timestamp",
        ),
        TABLES["enrichment_inventory"],
        comment="Expected and realized enrichment burden for the complete analysis union.",
    )
    ids.unpersist()
    for key, value in inventory.items():
        print(f"{key.upper()}: {value}")
    print("ENRICHMENT STAGING: PASS")
    print("COLLECTION QUEUE:", TABLES["collection_queue"])
    print("LID SOURCE CHANNELS:", TABLES["lid_source_channels"])
    print("LID SOURCE VIDEOS:", TABLES["lid_source_videos"])
    return inventory


STAGES = {
    "build_frame": run_build_frame,
    "simulate_design": run_simulation,
    "draw_samples": run_draw_samples,
    "stage_enrichment": run_stage_enrichment,
}
if STAGE not in STAGES:
    raise ValueError(f"Unknown stage {STAGE!r}; expected one of {sorted(STAGES)}")

print(f"RUNNING STAGE: {STAGE}")
print(f"DESIGN VERSION: {DESIGN_VERSION}")
print(f"CONFIG: {CONFIG_PATH}")
RESULT = STAGES[STAGE]()
dbutils.notebook.exit(json.dumps({"stage": STAGE, "result": RESULT}, sort_keys=True, default=str))
