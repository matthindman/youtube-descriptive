# Databricks notebook source
# ruff: noqa: F821
# MAGIC %run ./full_corpus_dual_sample_design

# COMMAND ----------
"""Post-enrichment allocations and design-based dual-sample estimates."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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


_widget("stage", "allocate")
_widget(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
_widget(
    "hierarchy_config_path",
    "dbfs:/FileStore/youtube_descriptive/youtube_topic_hierarchy_v2.yaml",
)
_widget(
    "topic_remap_path",
    "dbfs:/FileStore/youtube_descriptive/topic_remap.yaml",
)
STAGE = _get("stage", "allocate")
CONFIG_PATH = _get(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
HIERARCHY_PATH = _get(
    "hierarchy_config_path",
    "dbfs:/FileStore/youtube_descriptive/youtube_topic_hierarchy_v2.yaml",
)
TOPIC_REMAP_PATH = _get(
    "topic_remap_path",
    "dbfs:/FileStore/youtube_descriptive/topic_remap.yaml",
)
CONFIG = json.loads(dbutils.fs.head(CONFIG_PATH, 1024 * 1024))
validate_design_config(CONFIG)
DESIGN_VERSION = CONFIG["design_version"]
FRAME_VERSION = CONFIG["frame_version"]
ANALYSIS = CONFIG["analysis"]
PREFIX = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_prefix']}"

TABLES = {
    "frame": f"{PREFIX}_frame",
    "probabilities": f"{PREFIX}_frame_probabilities",
    "analysis_union": f"{PREFIX}_analysis_union",
    "language": f"{PREFIX}_channel_language_current",
    "model_calibrated": f"{PREFIX}_topic_model_calibrated",
    "allocations": f"{PREFIX}_allocations",
    "estimates": f"{PREFIX}_estimates",
    "differences": f"{PREFIX}_weighting_differences",
    "qa": f"{PREFIX}_qa",
}

UNLABELED_FAMILY = "Unlabeled"
UNLABELED_LEAF = "No YouTube topicCategories"
MODEL_INSUFFICIENT_LEAF = "Model: insufficient evidence"
UNMAPPED_FAMILY = "Other / Unmapped YouTube topic"


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
        f"'frame.version'='{FRAME_VERSION}')"
    )
    print(f"WROTE TABLE: {table_name}")


def taxonomy_frames() -> tuple[DataFrame, DataFrame]:
    hierarchy = yaml.safe_load(dbutils.fs.head(HIERARCHY_PATH, 4 * 1024 * 1024)) or {}
    remap = yaml.safe_load(dbutils.fs.head(TOPIC_REMAP_PATH, 4 * 1024 * 1024)) or {}
    aliases = {str(key): str(value) for key, value in (hierarchy.get("aliases") or {}).items()}
    topic_map: dict[str, dict[str, str]] = {}
    for family, specification in (hierarchy.get("families") or {}).items():
        for parent in specification.get("parent_slugs") or []:
            canonical = aliases.get(str(parent), str(parent))
            topic_map[canonical] = {
                "canonical_slug": canonical,
                "family": str(family),
                "leaf": f"[{family}] - unspecified",
                "node_type": "parent",
            }
        for child, leaf in (specification.get("children") or {}).items():
            canonical = aliases.get(str(child), str(child))
            topic_map[canonical] = {
                "canonical_slug": canonical,
                "family": str(family),
                "leaf": str(leaf),
                "node_type": "child",
            }
    for raw_leaf, target in (remap.get("unmapped_remap") or {}).items():
        prefix = "Unmapped: "
        if not str(raw_leaf).startswith(prefix):
            raise ValueError(f"Unsupported remap key: {raw_leaf!r}")
        canonical = str(raw_leaf)[len(prefix) :]
        topic_map[canonical] = {
            "canonical_slug": canonical,
            "family": str(target["family"]),
            "leaf": str(target["leaf"]),
            "node_type": topic_map.get(canonical, {}).get("node_type", "unmapped_remap"),
        }
    if not topic_map:
        raise RuntimeError("Topic hierarchy produced no mapped nodes")
    topic_map_df = spark.createDataFrame(list(topic_map.values()))
    alias_rows = [(raw, canonical) for raw, canonical in sorted(aliases.items())]
    alias_df = spark.createDataFrame(alias_rows, "raw_slug string, alias_canonical_slug string")
    return topic_map_df, alias_df


def platform_allocations(base: DataFrame) -> DataFrame:
    topic_map, aliases = taxonomy_frames()
    urls = base.select("channel_id", F.explode_outer("raw_topic_categories").alias("raw_topic_url"))
    clean_url = F.regexp_replace(F.trim("raw_topic_url"), r"[?#].*$", "")
    clean_url = F.regexp_replace(clean_url, r"/+$", "")
    raw_slug = F.lower(
        F.regexp_replace(F.element_at(F.split(clean_url, "/"), -1), r"\s+", "_")
    )
    slugs = (
        urls.withColumn("raw_slug", raw_slug)
        .where(F.col("raw_slug").isNotNull() & (F.length("raw_slug") > 0))
        .join(F.broadcast(aliases), "raw_slug", "left")
        .withColumn("canonical_slug", F.coalesce("alias_canonical_slug", "raw_slug"))
        .drop("alias_canonical_slug")
        .dropDuplicates(["channel_id", "canonical_slug"])
    )
    mapped = (
        slugs.join(F.broadcast(topic_map), "canonical_slug", "left")
        .withColumn("family", F.coalesce("family", F.lit(UNMAPPED_FAMILY)))
        .withColumn(
            "leaf",
            F.coalesce("leaf", F.concat(F.lit("Unmapped: "), F.col("canonical_slug"))),
        )
        .withColumn("node_type", F.coalesce("node_type", F.lit("unmapped")))
    )
    child_families = (
        mapped.where(F.col("node_type") == "child")
        .select("channel_id", "family")
        .distinct()
    )
    candidates = (
        mapped.where(F.col("node_type") != "parent")
        .unionByName(
            mapped.where(F.col("node_type") == "parent").join(
                child_families, ["channel_id", "family"], "left_anti"
            )
        )
        .select("channel_id", "family", "leaf")
        .dropDuplicates(["channel_id", "family", "leaf"])
    )
    unlabeled = (
        base.select("channel_id")
        .join(candidates.select("channel_id").distinct(), "channel_id", "left_anti")
        .select(
            "channel_id",
            F.lit(UNLABELED_FAMILY).alias("family"),
            F.lit(UNLABELED_LEAF).alias("leaf"),
        )
    )
    candidates = candidates.unionByName(unlabeled)
    family_counts = candidates.select("channel_id", "family").distinct().groupBy("channel_id").agg(
        F.count(F.lit(1)).alias("n_families")
    )
    leaf_counts = candidates.groupBy("channel_id", "family").agg(
        F.count(F.lit(1)).alias("n_family_leaves")
    )
    return (
        candidates.join(family_counts, "channel_id")
        .join(leaf_counts, ["channel_id", "family"])
        .withColumn(
            "allocation_weight",
            F.lit(1.0) / F.col("n_families") / F.col("n_family_leaves"),
        )
        .withColumn("allocation_variant", F.lit("platform_only"))
        .withColumn(
            "topic_source",
            F.when(F.col("family") == UNLABELED_FAMILY, F.lit("platform_unlabeled")).otherwise(
                F.lit("platform_category")
            ),
        )
        .select(
            "channel_id",
            "allocation_variant",
            "topic_source",
            "family",
            "leaf",
            "allocation_weight",
        )
    )


def model_completed_allocations(base: DataFrame, platform: DataFrame) -> DataFrame | None:
    if not spark.catalog.tableExists(TABLES["model_calibrated"]):
        print("MODEL-COMPLETED ALLOCATIONS: SKIPPED (calibrated table absent)")
        return None
    calibrated = spark.table(TABLES["model_calibrated"])
    required = {"channel_id", "status", "is_calibrated", "family", "leaf", "probability"}
    missing = sorted(required - set(calibrated.columns))
    if missing:
        raise RuntimeError(f"{TABLES['model_calibrated']} is missing columns: {missing}")
    bad = calibrated.where(~F.col("is_calibrated") | F.col("probability").isNull()).limit(1).count()
    if bad and ANALYSIS["model_completed_requires_calibrated_probabilities"]:
        raise RuntimeError("Model-completed probabilities include uncalibrated or null rows")
    model_channels = base.where(~F.col("has_nonempty_topic_categories")).select("channel_id")
    model = (
        calibrated.join(model_channels, "channel_id", "inner")
        .where((F.col("status") == "classified") & (F.col("probability") > 0))
        .select(
            "channel_id",
            F.lit("model_completed").alias("allocation_variant"),
            F.lit("calibrated_topic_model").alias("topic_source"),
            "family",
            "leaf",
            F.col("probability").cast("double").alias("allocation_weight"),
        )
    )
    classified_ids = model.select("channel_id").distinct()
    insufficient = (
        model_channels.join(classified_ids, "channel_id", "left_anti")
        .select(
            "channel_id",
            F.lit("model_completed").alias("allocation_variant"),
            F.lit("model_insufficient_evidence").alias("topic_source"),
            F.lit(UNLABELED_FAMILY).alias("family"),
            F.lit(MODEL_INSUFFICIENT_LEAF).alias("leaf"),
            F.lit(1.0).alias("allocation_weight"),
        )
    )
    platform_labeled = (
        platform.join(
            base.where(F.col("has_nonempty_topic_categories")).select("channel_id"),
            "channel_id",
            "inner",
        )
        .withColumn("allocation_variant", F.lit("model_completed"))
    )
    result = platform_labeled.unionByName(model).unionByName(insufficient)
    bad_sums = (
        result.groupBy("channel_id")
        .agg(F.sum("allocation_weight").alias("weight_sum"))
        .where(F.abs(F.col("weight_sum") - 1.0) > 1e-6)
        .limit(1)
        .count()
    )
    if bad_sums:
        raise RuntimeError("Calibrated model-completed probabilities do not conserve channel mass")
    print("MODEL-COMPLETED ALLOCATIONS: INCLUDED (calibration gate passed)")
    return result


def allocate() -> dict[str, int]:
    for name in ("analysis_union", "language"):
        require_table(TABLES[name])
    base = spark.table(TABLES["analysis_union"])
    language = spark.table(TABLES["language"]).select(
        "channel_id", F.coalesce(F.lower("channel_language"), F.lit("und")).alias("language")
    )
    if base.select("channel_id").distinct().count() != base.count():
        raise AssertionError("Analysis union is not one row per channel")
    missing_languages = base.select("channel_id").join(language, "channel_id", "left_anti").count()
    if missing_languages:
        raise RuntimeError(f"Final language table is missing {missing_languages:,} analysis channels")
    platform = platform_allocations(base)
    allocations = platform
    model = model_completed_allocations(base, platform)
    if model is not None:
        allocations = allocations.unionByName(model)
    allocations = (
        allocations.join(language, "channel_id", "inner")
        .join(
            base.select(
                "channel_id",
                "subscriber_status",
                "selection_route",
                "certainty_stratum",
                "accepted_positive_view_mass",
            ),
            "channel_id",
            "inner",
        )
        .withColumn("design_version", F.lit(DESIGN_VERSION))
        .withColumn("frame_version", F.lit(FRAME_VERSION))
    )
    write_table(
        allocations,
        TABLES["allocations"],
        "Conservative platform-topic and gated calibrated-model language/family/leaf allocations.",
    )
    counts = {
        row["allocation_variant"]: int(row["channels"])
        for row in allocations.groupBy("allocation_variant")
        .agg(F.countDistinct("channel_id").alias("channels"))
        .collect()
    }
    print("CHANNEL ALLOCATION CONSERVATION: PASS")
    print(json.dumps(counts, sort_keys=True))
    return counts


def rollups(allocations: DataFrame) -> DataFrame:
    base = allocations.select(
        "channel_id",
        "allocation_variant",
        "subscriber_status",
        "selection_route",
        "certainty_stratum",
        "accepted_positive_view_mass",
        "language",
        "family",
        "leaf",
        "allocation_weight",
    )

    def level(name: str, language, family, leaf) -> DataFrame:
        return (
            base.groupBy(
                "channel_id",
                "allocation_variant",
                "subscriber_status",
                "selection_route",
                "certainty_stratum",
                "accepted_positive_view_mass",
                language.alias("language"),
                family.alias("family"),
                leaf.alias("leaf"),
            )
            .agg(F.sum("allocation_weight").alias("cell_allocation"))
            .withColumn("taxonomy_level", F.lit(name))
        )

    empty = F.lit("")
    result = level("language", F.col("language"), empty, empty)
    result = result.unionByName(level("family", empty, F.col("family"), empty))
    result = result.unionByName(level("leaf", empty, F.col("family"), F.col("leaf")))
    result = result.unionByName(level("language_family", F.col("language"), F.col("family"), empty))
    result = result.unionByName(
        level("language_family_leaf", F.col("language"), F.col("family"), F.col("leaf"))
    )
    return result.withColumn(
        "cell_key", F.concat_ws("\x1f", "taxonomy_level", "language", "family", "leaf")
    )


def domain_denominators(frame: DataFrame) -> DataFrame:
    known = frame.where(F.col("subscriber_status") != "subscriber_unknown_or_hidden").agg(
        F.count(F.lit(1)).cast("double").alias("channel_denominator"),
        F.sum("accepted_positive_view_mass").cast("double").alias("view_denominator"),
    ).withColumn("population_scope", F.lit("known_subscriber"))
    all_frame = frame.agg(
        F.count(F.lit(1)).cast("double").alias("channel_denominator"),
        F.sum("accepted_positive_view_mass").cast("double").alias("view_denominator"),
    ).withColumn("population_scope", F.lit("all_retrievable"))
    return known.unionByName(all_frame)


def exact_head(cells: DataFrame, include_unknown: bool) -> DataFrame:
    allowed = ["census_ge10k"]
    if include_unknown:
        allowed.append("subscriber_unknown_or_hidden")
    return cells.where(F.col("subscriber_status").isin(allowed)).groupBy(
        "allocation_variant", "taxonomy_level", "cell_key", "language", "family", "leaf"
    ).agg(
        F.sum("cell_allocation").alias("head_channel_total"),
        F.sum(F.col("accepted_positive_view_mass") * F.col("cell_allocation")).alias(
            "head_view_total"
        ),
    )


def tail_srs(cells: DataFrame, probabilities: DataFrame, tail_n: int, sample_n: int) -> DataFrame:
    selected = cells.join(
        probabilities.where(F.col("selected_srs")).select("channel_id", "pi_srs"),
        "channel_id",
        "inner",
    )
    grouped = selected.groupBy(
        "allocation_variant", "taxonomy_level", "cell_key", "language", "family", "leaf"
    ).agg(
        F.sum("cell_allocation").alias("sum_y"),
        F.sum(F.col("cell_allocation") ** 2).alias("sum_y2"),
        F.count(F.lit(1)).alias("contributing_n"),
        F.max(F.col("cell_allocation") / F.col("pi_srs")).alias("max_weighted_value"),
        F.sum(F.col("cell_allocation") / F.col("pi_srs")).alias("weighted_sum"),
        F.sum((F.col("cell_allocation") / F.col("pi_srs")) ** 2).alias("weighted_sum2"),
    )
    sample_mean = F.col("sum_y") / F.lit(float(sample_n))
    sample_variance = (
        F.col("sum_y2") - F.lit(float(sample_n)) * sample_mean**2
    ) / F.lit(float(sample_n - 1))
    return (
        grouped.withColumn("tail_total", F.lit(float(tail_n)) * sample_mean)
        .withColumn(
            "tail_variance",
            F.lit(float(tail_n**2))
            * F.lit(1.0 - sample_n / tail_n)
            * F.greatest(sample_variance, F.lit(0.0))
            / F.lit(float(sample_n)),
        )
        .withColumn(
            "effective_contributing_n",
            F.when(F.col("weighted_sum2") > 0, F.col("weighted_sum") ** 2 / F.col("weighted_sum2")),
        )
        .select(
            "allocation_variant",
            "taxonomy_level",
            "cell_key",
            "language",
            "family",
            "leaf",
            "tail_total",
            "tail_variance",
            "contributing_n",
            "effective_contributing_n",
            "max_weighted_value",
        )
    )


def tail_pps(cells: DataFrame, probabilities: DataFrame) -> DataFrame:
    selected = cells.join(
        probabilities.where(F.col("selected_pps")).select("channel_id", "pi_pps"),
        "channel_id",
        "inner",
    ).withColumn(
        "y", F.col("accepted_positive_view_mass") * F.col("cell_allocation")
    )
    return selected.groupBy(
        "allocation_variant", "taxonomy_level", "cell_key", "language", "family", "leaf"
    ).agg(
        F.sum(F.col("y") / F.col("pi_pps")).alias("tail_total"),
        F.sum((1.0 - F.col("pi_pps")) * F.col("y") ** 2 / F.col("pi_pps") ** 2).alias(
            "tail_variance"
        ),
        F.count(F.lit(1)).alias("contributing_n"),
        (
            F.sum(F.col("y") / F.col("pi_pps")) ** 2
            / F.sum((F.col("y") / F.col("pi_pps")) ** 2)
        ).alias("effective_contributing_n"),
        F.max(F.col("y") / F.col("pi_pps")).alias("max_weighted_value"),
    )


def combine_estimates(
    head: DataFrame,
    tail: DataFrame,
    denominators: DataFrame,
    population_scope: str,
    estimator: str,
    denominator_column: str,
    tail_known_total: float,
    tail_calibration_factor: float,
) -> DataFrame:
    keys = ["allocation_variant", "taxonomy_level", "cell_key", "language", "family", "leaf"]
    head_total_column = "head_channel_total" if estimator == "equal_channel_srs" else "head_view_total"
    combined = head.select(*keys, F.col(head_total_column).alias("head_total")).join(tail, keys, "full")
    denominator = denominators.where(F.col("population_scope") == population_scope).select(
        F.col(denominator_column).alias("denominator")
    )
    combined = combined.crossJoin(denominator)
    z_value = 1.959963984540054
    return (
        combined.fillna(
            {
                "head_total": 0.0,
                "tail_total": 0.0,
                "tail_variance": 0.0,
                "contributing_n": 0,
                "effective_contributing_n": 0.0,
                "max_weighted_value": 0.0,
            }
        )
        .withColumn("population_scope", F.lit(population_scope))
        .withColumn("estimator", F.lit(estimator))
        .withColumn("tail_known_total", F.lit(float(tail_known_total)))
        .withColumn("tail_calibration_factor", F.lit(float(tail_calibration_factor)))
        .withColumn("raw_total", F.col("head_total") + F.col("tail_total"))
        .withColumn("raw_share", F.col("raw_total") / F.col("denominator"))
        .withColumn("standard_error", F.sqrt(F.col("tail_variance")) / F.col("denominator"))
        .withColumn("ci95_lower", F.greatest(F.lit(0.0), F.col("raw_share") - z_value * F.col("standard_error")))
        .withColumn("ci95_upper", F.least(F.lit(1.0), F.col("raw_share") + z_value * F.col("standard_error")))
        .withColumn(
            "display_total",
            F.col("head_total") + F.col("tail_total") * F.col("tail_calibration_factor"),
        )
        .withColumn("display_share", F.col("display_total") / F.col("denominator"))
        .withColumn(
            "coefficient_of_variation",
            F.when(F.col("raw_share") > 0, F.col("standard_error") / F.col("raw_share")),
        )
        .withColumn(
            "largest_weighted_contribution",
            F.when(F.col("raw_total") > 0, F.col("max_weighted_value") / F.col("raw_total")),
        )
        .withColumn(
            "headline_reliable",
            (F.col("effective_contributing_n") >= F.lit(float(ANALYSIS["headline_min_effective_n"])))
            & (
                z_value * F.col("standard_error")
                <= F.lit(float(ANALYSIS["headline_max_relative_half_width"])) * F.col("raw_share")
            )
            & (
                F.col("largest_weighted_contribution")
                <= F.lit(float(ANALYSIS["headline_max_channel_contribution"]))
            ),
        )
        .withColumn("design_version", F.lit(DESIGN_VERSION))
    )


def estimate() -> dict[str, float | int]:
    for name in ("allocations", "frame", "probabilities"):
        require_table(TABLES[name])
    allocations = spark.table(TABLES["allocations"])
    frame = spark.table(TABLES["frame"])
    probabilities = spark.table(TABLES["probabilities"])
    cells = rollups(allocations).persist()
    denominators = domain_denominators(frame).persist()
    tail_n = frame.where(F.col("subscriber_status") == "sample_frame_lt10k").count()
    sample_n = probabilities.where(F.col("selected_srs")).count()
    tail_view_total = float(
        frame.where(F.col("subscriber_status") == "sample_frame_lt10k")
        .agg(F.sum("accepted_positive_view_mass").alias("value"))
        .first()["value"]
        or 0.0
    )
    pps_tail_ht = float(
        frame.select("channel_id", "accepted_positive_view_mass")
        .join(probabilities.where(F.col("selected_pps")).select("channel_id", "pi_pps"), "channel_id")
        .agg(F.sum(F.col("accepted_positive_view_mass") / F.col("pi_pps")).alias("value"))
        .first()["value"]
        or 0.0
    )
    pps_factor = tail_view_total / pps_tail_ht if pps_tail_ht > 0 else 1.0
    lower = float(ANALYSIS["tail_total_ratio_lower_bound"])
    upper = float(ANALYSIS["tail_total_ratio_upper_bound"])
    if not lower <= pps_factor <= upper:
        raise RuntimeError(
            f"PPS tail-total calibration factor {pps_factor:.6f} falls outside registered [{lower}, {upper}]"
        )
    srs = tail_srs(cells, probabilities, tail_n, sample_n).persist()
    pps = tail_pps(cells, probabilities).persist()
    estimates = None
    for scope, include_unknown in (("known_subscriber", False), ("all_retrievable", True)):
        head = exact_head(cells, include_unknown).persist()
        equal = combine_estimates(
            head,
            srs,
            denominators,
            scope,
            "equal_channel_srs",
            "channel_denominator",
            float(tail_n),
            1.0,
        )
        attention = combine_estimates(
            head,
            pps,
            denominators,
            scope,
            "attention_pps",
            "view_denominator",
            tail_view_total,
            pps_factor,
        )
        scope_rows = equal.unionByName(attention)
        estimates = scope_rows if estimates is None else estimates.unionByName(scope_rows)
        head.unpersist()
    write_table(
        estimates,
        TABLES["estimates"],
        "Raw design-based and total-ratio display estimates for channel and attention ecologies.",
    )
    keys = [
        "allocation_variant",
        "population_scope",
        "taxonomy_level",
        "cell_key",
        "language",
        "family",
        "leaf",
    ]
    equal = estimates.where(F.col("estimator") == "equal_channel_srs").select(
        *keys,
        F.col("raw_share").alias("channel_share"),
        F.col("standard_error").alias("channel_share_se"),
    )
    attention = estimates.where(F.col("estimator") == "attention_pps").select(
        *keys,
        F.col("raw_share").alias("view_share"),
        F.col("standard_error").alias("view_share_se"),
    )
    difference = (
        equal.join(attention, keys, "inner")
        .withColumn("view_minus_channel_share", F.col("view_share") - F.col("channel_share"))
        .withColumn(
            "difference_standard_error",
            F.sqrt(F.col("channel_share_se") ** 2 + F.col("view_share_se") ** 2),
        )
        .withColumn(
            "difference_ci95_lower",
            F.col("view_minus_channel_share") - F.lit(1.959963984540054) * F.col("difference_standard_error"),
        )
        .withColumn(
            "difference_ci95_upper",
            F.col("view_minus_channel_share") + F.lit(1.959963984540054) * F.col("difference_standard_error"),
        )
        .withColumn(
            "view_to_channel_ratio",
            F.when(F.col("channel_share") > 0, F.col("view_share") / F.col("channel_share")),
        )
        .withColumn(
            "sampling_covariance_assumption",
            F.lit("zero; joint measurement-error replication required for final inference"),
        )
        .withColumn("design_version", F.lit(DESIGN_VERSION))
    )
    write_table(
        difference,
        TABLES["differences"],
        "Signed view-share minus channel-share distortion with independent-design sampling SE approximation.",
    )
    metrics = {
        "tail_population_n": int(tail_n),
        "srs_realized_n": int(sample_n),
        "tail_positive_view_total": tail_view_total,
        "pps_tail_ht_view_total": pps_tail_ht,
        "pps_tail_calibration_factor": pps_factor,
        "estimate_rows": estimates.count(),
        "difference_rows": difference.count(),
    }
    cells.unpersist()
    denominators.unpersist()
    srs.unpersist()
    pps.unpersist()
    print("ESTIMATION: PASS")
    print(json.dumps(metrics, sort_keys=True))
    return metrics


def qa() -> dict[str, float | int]:
    for name in ("allocations", "estimates", "differences"):
        require_table(TABLES[name])
    allocations = spark.table(TABLES["allocations"])
    estimates = spark.table(TABLES["estimates"])
    allocation_qa = allocations.groupBy("channel_id", "allocation_variant").agg(
        F.sum("allocation_weight").alias("weight_sum")
    ).agg(
        F.count(F.lit(1)).alias("channel_variants"),
        F.max(F.abs(F.col("weight_sum") - 1.0)).alias("max_allocation_error"),
    ).first().asDict()
    display_qa = estimates.groupBy(
        "allocation_variant", "population_scope", "estimator", "taxonomy_level"
    ).agg(F.sum("display_share").alias("share_sum"))
    max_display_error = float(
        display_qa.agg(F.max(F.abs(F.col("share_sum") - 1.0)).alias("value")).first()["value"] or 0.0
    )
    metrics = {
        "allocation_channel_variants": int(allocation_qa["channel_variants"]),
        "max_channel_allocation_error": float(allocation_qa["max_allocation_error"] or 0.0),
        "max_display_share_conservation_error": max_display_error,
        "estimate_rows": estimates.count(),
        "difference_rows": spark.table(TABLES["differences"]).count(),
    }
    if metrics["max_channel_allocation_error"] > 1e-6:
        raise AssertionError(f"Channel allocation conservation failed: {metrics}")
    if max_display_error > 1e-6:
        raise AssertionError(f"Display-share conservation failed: {metrics}")
    rows = [
        (DESIGN_VERSION, key, json.dumps(value), datetime.now(timezone.utc))
        for key, value in metrics.items()
    ]
    write_table(
        spark.createDataFrame(
            rows, "design_version string, metric string, value_json string, recorded_at timestamp"
        ),
        TABLES["qa"],
        "Post-enrichment allocation and estimator acceptance metrics.",
    )
    print("CHANNEL ALLOCATION CONSERVATION: PASS")
    print("VIEW ALLOCATION CONSERVATION: PASS")
    print("DISPLAY SHARE CONSERVATION: PASS")
    print(json.dumps(metrics, sort_keys=True))
    return metrics


STAGES = {"allocate": allocate, "estimate": estimate, "qa": qa}
if STAGE not in STAGES:
    raise ValueError(f"Unknown analysis stage {STAGE!r}; expected one of {sorted(STAGES)}")
print(f"RUNNING FULL-CORPUS ANALYSIS STAGE: {STAGE}")
RESULT = STAGES[STAGE]()
dbutils.notebook.exit(json.dumps({"stage": STAGE, "result": RESULT}, sort_keys=True, default=str))
