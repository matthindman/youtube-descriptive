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
from pyspark.sql import Window


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
_widget("analysis_mode", "full")
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
ANALYSIS_MODE = _get("analysis_mode", "full").lower()
if ANALYSIS_MODE not in {"full", "attention_pps"}:
    raise ValueError("analysis_mode must be one of: full, attention_pps")
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
OUTPUT_PREFIX = PREFIX if ANALYSIS_MODE == "full" else f"{PREFIX}_pps_attention"

TABLES = {
    "frame": f"{PREFIX}_frame",
    "platform_topics": f"{PREFIX}_platform_topics",
    "probabilities": f"{PREFIX}_frame_probabilities",
    "analysis_union": f"{PREFIX}_analysis_union",
    "language": f"{PREFIX}_channel_language_current",
    "pps_language": f"{PREFIX}_channel_language_pps_current",
    "remainder_routing": f"{PREFIX}_language_routing_comparison_remainder",
    "model_calibrated": f"{PREFIX}_topic_model_calibrated",
    "analysis_language": f"{OUTPUT_PREFIX}_channel_language_current",
    "allocations": f"{OUTPUT_PREFIX}_allocations",
    "platform_margins": f"{OUTPUT_PREFIX}_platform_topic_margins",
    "estimates": f"{OUTPUT_PREFIX}_estimates",
    "differences": f"{OUTPUT_PREFIX}_weighting_differences",
    "qa": f"{OUTPUT_PREFIX}_qa",
    "publication": f"{OUTPUT_PREFIX}_publication_estimates",
    "treemap_cells": f"{OUTPUT_PREFIX}_treemap_cells",
    "treemap_qa": f"{OUTPUT_PREFIX}_treemap_qa",
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


def exact_platform_topic_margins(frame: DataFrame) -> DataFrame:
    """Known platform-topic margins used only to calibrate display geometry."""
    require_table(TABLES["platform_topics"])
    topics = spark.table(TABLES["platform_topics"]).select(
        "channel_id", "raw_topic_categories"
    )
    allocated = platform_allocations(
        frame.select("channel_id").join(topics, "channel_id", "left")
    ).join(
        frame.select("channel_id", "subscriber_status", "accepted_positive_view_mass"),
        "channel_id",
        "inner",
    )
    is_tail = F.col("subscriber_status") == "sample_frame_lt10k"
    return (
        allocated.groupBy("family", "leaf")
        .agg(
            F.sum(F.when(is_tail, F.col("allocation_weight")).otherwise(0.0)).alias(
                "tail_channel_total"
            ),
            F.sum(
                F.when(
                    is_tail,
                    F.col("accepted_positive_view_mass") * F.col("allocation_weight"),
                ).otherwise(0.0)
            ).alias("tail_view_total"),
            F.sum("allocation_weight").alias("all_frame_channel_total"),
            F.sum(F.col("accepted_positive_view_mass") * F.col("allocation_weight")).alias(
                "all_frame_view_total"
            ),
        )
        .withColumn("design_version", F.lit(DESIGN_VERSION))
        .withColumn("frame_version", F.lit(FRAME_VERSION))
    )


def attention_pps_analysis_base() -> DataFrame:
    """Return every exact stratum row plus the registered Poisson PPS tail."""
    require_table(TABLES["analysis_union"])
    require_table(TABLES["probabilities"])
    analysis = spark.table(TABLES["analysis_union"])
    exact = analysis.where(F.col("subscriber_status") != "sample_frame_lt10k")
    pps_ids = (
        spark.table(TABLES["probabilities"])
        .where(F.col("selected_pps"))
        .select("channel_id")
    )
    tail = analysis.where(F.col("subscriber_status") == "sample_frame_lt10k").join(
        pps_ids, "channel_id", "inner"
    )
    return exact.unionByName(tail)


def provisional_attention_language(base: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    """Build the complete PPS-era label lookup without claiming SRS completion.

    Exact-stratum rows reuse the frozen published label where available. Exact
    rows added after that frozen LID frame use only completed dual-LID agreement;
    cases requiring the pending DeepSeek fallback remain explicit ``und``.
    Every PPS tail row comes from the finalized PPS publication.
    """
    for name in ("pps_language", "remainder_routing"):
        require_table(TABLES[name])
    exact = base.where(F.col("subscriber_status") != "sample_frame_lt10k")
    tail = base.where(F.col("subscriber_status") == "sample_frame_lt10k")

    exact_existing = exact.where(F.col("has_existing_language_label")).select(
        "channel_id",
        F.coalesce(F.lower(F.trim("channel_language")), F.lit("und")).alias(
            "channel_language"
        ),
        F.lit("reused_frozen_exact_label").alias("analysis_label_source"),
    )
    unresolved_exact = exact.where(~F.col("has_existing_language_label")).select(
        "channel_id"
    )
    routing = spark.table(TABLES["remainder_routing"]).select(
        "channel_id",
        "lid_base_language_resolved",
        "consensus_language_iso639_3",
        "openlid_primary_language_iso639_3",
        "glotlid_primary_language_iso639_3",
    )
    same_base_iso = (
        F.col("openlid_primary_language_iso639_3").isNotNull()
        & F.col("glotlid_primary_language_iso639_3").isNotNull()
        & (
            F.lower(F.col("openlid_primary_language_iso639_3"))
            == F.lower(F.col("glotlid_primary_language_iso639_3"))
        )
    )
    resolved_consensus = (
        F.coalesce(F.col("lid_base_language_resolved"), F.lit(False))
        & F.col("consensus_language_iso639_3").isNotNull()
    )
    resolved_base_iso = (
        F.coalesce(F.col("lid_base_language_resolved"), F.lit(False))
        & (~resolved_consensus)
        & same_base_iso
    )
    exact_interim = unresolved_exact.join(routing, "channel_id", "left").select(
        "channel_id",
        F.when(
            resolved_consensus,
            F.lower(F.trim("consensus_language_iso639_3")),
        )
        .when(
            resolved_base_iso,
            F.lower(F.trim("openlid_primary_language_iso639_3")),
        )
        .otherwise(F.lit("und"))
        .alias("channel_language"),
        F.when(resolved_consensus, F.lit("interim_dual_lid_consensus"))
        .when(resolved_base_iso, F.lit("interim_dual_lid_base_iso_agreement"))
        .otherwise(F.lit("interim_pending_deepseek_or_no_text"))
        .alias("analysis_label_source"),
    )
    pps = spark.table(TABLES["pps_language"]).select(
        "channel_id",
        F.coalesce(F.lower(F.trim("channel_language")), F.lit("und")).alias(
            "channel_language"
        ),
        F.lit("final_pps_language_publication").alias("analysis_label_source"),
    )
    missing_pps = tail.select("channel_id").join(pps, "channel_id", "left_anti").count()
    unexpected_pps = pps.select("channel_id").join(tail, "channel_id", "left_anti").count()
    if missing_pps or unexpected_pps:
        raise RuntimeError(
            "Final PPS language publication does not match the registered PPS sample: "
            f"missing={missing_pps:,}, unexpected={unexpected_pps:,}"
        )
    language = exact_existing.unionByName(exact_interim).unionByName(pps)
    counts = {
        row["analysis_label_source"]: int(row["rows"])
        for row in language.groupBy("analysis_label_source")
        .agg(F.count(F.lit(1)).alias("rows"))
        .collect()
    }
    counts["rows"] = language.count()
    counts["distinct_channels"] = language.select("channel_id").distinct().count()
    counts["und"] = language.where(F.col("channel_language") == "und").count()
    base_count = base.count()
    if counts["rows"] != counts["distinct_channels"] or counts["rows"] != base_count:
        raise AssertionError(f"Provisional attention labels do not conserve the analysis base: {counts}")
    write_table(
        language,
        TABLES["analysis_language"],
        "PPS-attention labels: frozen exact labels plus interim dual-LID agreement and finalized PPS labels.",
    )
    return language.select("channel_id", "channel_language"), counts


def allocate() -> dict[str, int]:
    for name in ("frame", "platform_topics"):
        require_table(TABLES[name])
    if ANALYSIS_MODE == "attention_pps":
        base = attention_pps_analysis_base()
        language_source, language_counts = provisional_attention_language(base)
    else:
        for name in ("analysis_union", "language"):
            require_table(TABLES[name])
        base = spark.table(TABLES["analysis_union"])
        language_source = spark.table(TABLES["language"]).select(
            "channel_id", "channel_language"
        )
        language_counts = {}
    language = language_source.select(
        "channel_id", F.coalesce(F.lower("channel_language"), F.lit("und")).alias("language")
    )
    if base.select("channel_id").distinct().count() != base.count():
        raise AssertionError("Analysis union is not one row per channel")
    missing_languages = base.select("channel_id").join(language, "channel_id", "left_anti").count()
    if missing_languages:
        raise RuntimeError(f"Final language table is missing {missing_languages:,} analysis channels")
    platform = platform_allocations(base)
    allocations = platform
    model = None if ANALYSIS_MODE == "attention_pps" else model_completed_allocations(base, platform)
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
    margins = exact_platform_topic_margins(spark.table(TABLES["frame"]))
    write_table(
        margins,
        TABLES["platform_margins"],
        "Exact frozen-frame platform-topic margins for post-estimation display calibration.",
    )
    counts = {
        row["allocation_variant"]: int(row["channels"])
        for row in allocations.groupBy("allocation_variant")
        .agg(F.countDistinct("channel_id").alias("channels"))
        .collect()
    }
    counts["platform_margin_cells"] = margins.count()
    counts["analysis_base_rows"] = base.count()
    counts["analysis_language_rows"] = int(language_counts.get("rows", counts["analysis_base_rows"]))
    counts["analysis_language_und"] = int(language_counts.get("und", 0))
    print("CHANNEL ALLOCATION CONSERVATION: PASS")
    print(f"ANALYSIS MODE: {ANALYSIS_MODE}")
    if language_counts:
        print("PROVISIONAL LANGUAGE SOURCES:", json.dumps(language_counts, sort_keys=True))
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
    grouped = selected.groupBy(
        "allocation_variant", "taxonomy_level", "cell_key", "language", "family", "leaf"
    ).agg(
        F.sum(F.col("y") / F.col("pi_pps")).alias("weighted_sum"),
        F.sum((1.0 - F.col("pi_pps")) * F.col("y") ** 2 / F.col("pi_pps") ** 2).alias(
            "tail_variance"
        ),
        F.count(F.lit(1)).alias("contributing_n"),
        F.sum((F.col("y") / F.col("pi_pps")) ** 2).alias("weighted_sum2"),
        F.max(F.col("y") / F.col("pi_pps")).alias("max_weighted_value"),
    )
    return (
        grouped.withColumn("tail_total", F.col("weighted_sum"))
        .withColumn(
            "effective_contributing_n",
            F.when(
                F.col("weighted_sum2") > 0,
                F.col("weighted_sum") ** 2 / F.col("weighted_sum2"),
            ).otherwise(F.lit(0.0)),
        )
        .drop("weighted_sum", "weighted_sum2")
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


def estimate() -> dict[str, float | int | str]:
    for name in ("allocations", "frame", "probabilities"):
        require_table(TABLES[name])
    allocations = spark.table(TABLES["allocations"])
    frame = spark.table(TABLES["frame"])
    probabilities = spark.table(TABLES["probabilities"])
    cells = rollups(allocations).persist()
    denominators = domain_denominators(frame).persist()
    tail_n = frame.where(F.col("subscriber_status") == "sample_frame_lt10k").count()
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
    pps = tail_pps(cells, probabilities).persist()
    if ANALYSIS_MODE == "attention_pps":
        estimates = None
        for scope, include_unknown in (("known_subscriber", False), ("all_retrievable", True)):
            head = exact_head(cells, include_unknown).persist()
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
            estimates = attention if estimates is None else estimates.unionByName(attention)
            head.unpersist()
        write_table(
            estimates,
            TABLES["estimates"],
            "PPS-only raw design-based and total-ratio display estimates for the attention ecology.",
        )
        metrics = {
            "analysis_mode": ANALYSIS_MODE,
            "tail_population_n": int(tail_n),
            "pps_realized_n": probabilities.where(F.col("selected_pps")).count(),
            "tail_positive_view_total": tail_view_total,
            "pps_tail_ht_view_total": pps_tail_ht,
            "pps_tail_calibration_factor": pps_factor,
            "estimate_rows": estimates.count(),
        }
        cells.unpersist()
        denominators.unpersist()
        pps.unpersist()
        print("PPS ATTENTION ESTIMATION: PASS")
        print(json.dumps(metrics, sort_keys=True))
        return metrics

    sample_n = probabilities.where(F.col("selected_srs")).count()
    srs = tail_srs(cells, probabilities, tail_n, sample_n).persist()
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
        F.lit(True).alias("channel_cell_observed"),
    )
    attention = estimates.where(F.col("estimator") == "attention_pps").select(
        *keys,
        F.col("raw_share").alias("view_share"),
        F.col("standard_error").alias("view_share_se"),
        F.lit(True).alias("view_cell_observed"),
    )
    difference = (
        equal.join(attention, keys, "full")
        .fillna(
            {
                "channel_share": 0.0,
                "channel_share_se": 0.0,
                "view_share": 0.0,
                "view_share_se": 0.0,
                "channel_cell_observed": False,
                "view_cell_observed": False,
            }
        )
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
    required = ["allocations", "estimates"]
    if ANALYSIS_MODE == "full":
        required.append("differences")
    for name in required:
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
        "difference_rows": (
            spark.table(TABLES["differences"]).count() if ANALYSIS_MODE == "full" else 0
        ),
    }
    estimators = {row["estimator"] for row in estimates.select("estimator").distinct().collect()}
    expected_estimators = (
        {"attention_pps"}
        if ANALYSIS_MODE == "attention_pps"
        else {"attention_pps", "equal_channel_srs"}
    )
    if estimators != expected_estimators:
        raise AssertionError(
            f"Unexpected estimators for {ANALYSIS_MODE}: {sorted(estimators)}"
        )
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


PUBLICATION_KEYS = [
    "allocation_variant",
    "population_scope",
    "taxonomy_level",
    "cell_key",
    "language",
    "family",
    "leaf",
]
PUBLICATION_METRIC_COLUMNS = [
    "head_total",
    "tail_total",
    "tail_variance",
    "tail_known_total",
    "tail_calibration_factor",
    "denominator",
    "raw_total",
    "raw_share",
    "standard_error",
    "ci95_lower",
    "ci95_upper",
    "display_total",
    "display_share",
    "coefficient_of_variation",
    "largest_weighted_contribution",
    "contributing_n",
    "effective_contributing_n",
    "max_weighted_value",
    "headline_reliable",
]


def attention_publication_estimates(estimates: DataFrame) -> DataFrame:
    return (
        estimates.where(F.col("estimator") == "attention_pps")
        .select(
            *PUBLICATION_KEYS,
            *(
                F.col(column).alias(f"view_{column}")
                for column in PUBLICATION_METRIC_COLUMNS
            ),
            F.lit(True).alias("view_cell_observed"),
        )
        .withColumn("design_version", F.lit(DESIGN_VERSION))
        .withColumn("frame_version", F.lit(FRAME_VERSION))
    )


def paired_publication_estimates(estimates: DataFrame, differences: DataFrame) -> DataFrame:
    def one_estimator(estimator: str, prefix: str) -> DataFrame:
        return estimates.where(F.col("estimator") == estimator).select(
            *PUBLICATION_KEYS,
            *(
                F.col(column).alias(f"{prefix}_{column}")
                for column in PUBLICATION_METRIC_COLUMNS
            ),
            F.lit(True).alias(f"{prefix}_cell_observed"),
        )

    paired = one_estimator("attention_pps", "view").join(
        one_estimator("equal_channel_srs", "channel"), PUBLICATION_KEYS, "full"
    )
    domain_keys = ["allocation_variant", "population_scope"]
    for estimator, prefix in (("attention_pps", "view"), ("equal_channel_srs", "channel")):
        constants = (
            estimates.where(F.col("estimator") == estimator)
            .select(
                *domain_keys,
                F.col("denominator").alias(f"_{prefix}_denominator"),
                F.col("tail_known_total").alias(f"_{prefix}_tail_known_total"),
                F.col("tail_calibration_factor").alias(f"_{prefix}_tail_calibration_factor"),
            )
            .dropDuplicates(domain_keys)
        )
        paired = (
            paired.join(constants, domain_keys, "left")
            .withColumn(
                f"{prefix}_denominator",
                F.coalesce(f"{prefix}_denominator", f"_{prefix}_denominator"),
            )
            .withColumn(
                f"{prefix}_tail_known_total",
                F.coalesce(f"{prefix}_tail_known_total", f"_{prefix}_tail_known_total"),
            )
            .withColumn(
                f"{prefix}_tail_calibration_factor",
                F.coalesce(
                    f"{prefix}_tail_calibration_factor", f"_{prefix}_tail_calibration_factor"
                ),
            )
            .drop(
                f"_{prefix}_denominator",
                f"_{prefix}_tail_known_total",
                f"_{prefix}_tail_calibration_factor",
            )
        )
    zero_columns = {}
    for prefix in ("view", "channel"):
        for column in (
            "head_total",
            "tail_total",
            "tail_variance",
            "raw_total",
            "raw_share",
            "standard_error",
            "ci95_lower",
            "ci95_upper",
            "display_total",
            "display_share",
            "contributing_n",
            "effective_contributing_n",
            "max_weighted_value",
        ):
            zero_columns[f"{prefix}_{column}"] = 0.0
    paired = paired.fillna(zero_columns).fillna(
        {
            "view_headline_reliable": False,
            "channel_headline_reliable": False,
            "view_cell_observed": False,
            "channel_cell_observed": False,
        }
    )
    difference_columns = [
        "view_minus_channel_share",
        "difference_standard_error",
        "difference_ci95_lower",
        "difference_ci95_upper",
        "view_to_channel_ratio",
        "sampling_covariance_assumption",
    ]
    return (
        paired.join(
            differences.select(*PUBLICATION_KEYS, *difference_columns),
            PUBLICATION_KEYS,
            "left",
        )
        .withColumn("design_version", F.lit(DESIGN_VERSION))
        .withColumn("frame_version", F.lit(FRAME_VERSION))
    )


def calibrated_attention_treemap_cells(
    publication: DataFrame, margins: DataFrame
) -> DataFrame:
    cells = publication.where(F.col("taxonomy_level") == "language_family_leaf").join(
        margins.select("family", "leaf", "tail_view_total"),
        ["family", "leaf"],
        "left",
    )
    topic_window = Window.partitionBy(
        "allocation_variant", "population_scope", "family", "leaf"
    )
    cells = (
        cells.withColumn(
            "sampled_topic_tail_view_total", F.sum("view_tail_total").over(topic_window)
        )
        .withColumn(
            "view_geometry_tail_factor",
            F.when(
                F.col("allocation_variant") == "platform_only",
                F.when(
                    F.col("sampled_topic_tail_view_total") > 0,
                    F.col("tail_view_total") / F.col("sampled_topic_tail_view_total"),
                ).when(
                    F.coalesce(F.col("tail_view_total"), F.lit(0.0)) == 0,
                    F.lit(1.0),
                ),
            ).otherwise(F.col("view_tail_calibration_factor")),
        )
        .withColumn(
            "view_geometry_calibration_basis",
            F.when(
                F.col("allocation_variant") == "platform_only",
                F.lit("exact frozen-frame platform family/leaf tail view margin"),
            ).otherwise(F.lit("known global tail view total")),
        )
        .withColumn(
            "view_geometry_tail_total",
            F.col("view_tail_total") * F.col("view_geometry_tail_factor"),
        )
        .withColumn(
            "view_geometry_total",
            F.col("view_head_total") + F.col("view_geometry_tail_total"),
        )
        .withColumn(
            "view_geometry_global_share",
            F.col("view_geometry_total") / F.col("view_denominator"),
        )
    )
    language_window = Window.partitionBy(
        "allocation_variant", "population_scope", "language"
    )
    language_family_window = Window.partitionBy(
        "allocation_variant", "population_scope", "language", "family"
    )
    return (
        cells.withColumn(
            "view_language_total", F.sum("view_geometry_total").over(language_window)
        )
        .withColumn(
            "view_language_family_total",
            F.sum("view_geometry_total").over(language_family_window),
        )
        .withColumn(
            "view_within_language_share",
            F.when(
                F.col("view_language_total") > 0,
                F.col("view_geometry_total") / F.col("view_language_total"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "view_within_language_family_share",
            F.when(
                F.col("view_language_family_total") > 0,
                F.col("view_geometry_total") / F.col("view_language_family_total"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "conditional_share_uncertainty_status",
            F.lit(
                "not reported: requires replicate or linearized numerator-denominator covariance"
            ),
        )
        .withColumn(
            "cell_id",
            F.concat_ws(
                "\x1f",
                "allocation_variant",
                "population_scope",
                "language",
                "family",
                "leaf",
            ),
        )
    )


def calibrated_treemap_cells(publication: DataFrame, margins: DataFrame) -> DataFrame:
    cells = publication.where(F.col("taxonomy_level") == "language_family_leaf").join(
        margins.select("family", "leaf", "tail_channel_total", "tail_view_total"),
        ["family", "leaf"],
        "left",
    )
    topic_window = Window.partitionBy(
        "allocation_variant", "population_scope", "family", "leaf"
    )
    cells = (
        cells.withColumn("sampled_topic_tail_view_total", F.sum("view_tail_total").over(topic_window))
        .withColumn(
            "sampled_topic_tail_channel_total", F.sum("channel_tail_total").over(topic_window)
        )
        .withColumn(
            "view_geometry_tail_factor",
            F.when(
                F.col("allocation_variant") == "platform_only",
                F.when(
                    F.col("sampled_topic_tail_view_total") > 0,
                    F.col("tail_view_total") / F.col("sampled_topic_tail_view_total"),
                ).when(F.coalesce(F.col("tail_view_total"), F.lit(0.0)) == 0, F.lit(1.0)),
            ).otherwise(F.col("view_tail_calibration_factor")),
        )
        .withColumn(
            "channel_geometry_tail_factor",
            F.when(
                F.col("allocation_variant") == "platform_only",
                F.when(
                    F.col("sampled_topic_tail_channel_total") > 0,
                    F.col("tail_channel_total") / F.col("sampled_topic_tail_channel_total"),
                ).when(F.coalesce(F.col("tail_channel_total"), F.lit(0.0)) == 0, F.lit(1.0)),
            ).otherwise(F.col("channel_tail_calibration_factor")),
        )
        .withColumn(
            "view_geometry_calibration_basis",
            F.when(
                F.col("allocation_variant") == "platform_only",
                F.lit("exact frozen-frame platform family/leaf tail view margin"),
            ).otherwise(F.lit("known global tail view total")),
        )
        .withColumn(
            "channel_geometry_calibration_basis",
            F.when(
                F.col("allocation_variant") == "platform_only",
                F.lit("exact frozen-frame platform family/leaf tail channel margin"),
            ).otherwise(F.lit("known global tail channel total")),
        )
        .withColumn(
            "view_geometry_tail_total",
            F.col("view_tail_total") * F.col("view_geometry_tail_factor"),
        )
        .withColumn(
            "channel_geometry_tail_total",
            F.col("channel_tail_total") * F.col("channel_geometry_tail_factor"),
        )
        .withColumn(
            "view_geometry_total", F.col("view_head_total") + F.col("view_geometry_tail_total")
        )
        .withColumn(
            "channel_geometry_total",
            F.col("channel_head_total") + F.col("channel_geometry_tail_total"),
        )
        .withColumn("view_geometry_global_share", F.col("view_geometry_total") / F.col("view_denominator"))
        .withColumn(
            "channel_geometry_global_share",
            F.col("channel_geometry_total") / F.col("channel_denominator"),
        )
    )
    language_window = Window.partitionBy("allocation_variant", "population_scope", "language")
    language_family_window = Window.partitionBy(
        "allocation_variant", "population_scope", "language", "family"
    )
    cells = (
        cells.withColumn("view_language_total", F.sum("view_geometry_total").over(language_window))
        .withColumn(
            "channel_language_total", F.sum("channel_geometry_total").over(language_window)
        )
        .withColumn(
            "view_language_family_total", F.sum("view_geometry_total").over(language_family_window)
        )
        .withColumn(
            "channel_language_family_total",
            F.sum("channel_geometry_total").over(language_family_window),
        )
        .withColumn(
            "view_within_language_share",
            F.when(
                F.col("view_language_total") > 0,
                F.col("view_geometry_total") / F.col("view_language_total"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "channel_within_language_share",
            F.when(
                F.col("channel_language_total") > 0,
                F.col("channel_geometry_total") / F.col("channel_language_total"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "view_within_language_family_share",
            F.when(
                F.col("view_language_family_total") > 0,
                F.col("view_geometry_total") / F.col("view_language_family_total"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "channel_within_language_family_share",
            F.when(
                F.col("channel_language_family_total") > 0,
                F.col("channel_geometry_total") / F.col("channel_language_family_total"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "conditional_share_uncertainty_status",
            F.lit("not reported: requires replicate or linearized numerator-denominator covariance"),
        )
        .withColumn(
            "cell_id", F.concat_ws("\x1f", "allocation_variant", "population_scope", "language", "family", "leaf")
        )
    )
    return cells


def attention_treemap_acceptance_metrics(
    cells: DataFrame, margins: DataFrame
) -> dict[str, float | int]:
    key_columns = ["allocation_variant", "population_scope", "language", "family", "leaf"]
    row_count = cells.count()
    distinct_count = cells.select(*key_columns).distinct().count()
    unsampled_margin_rows = cells.where(F.col("view_geometry_tail_factor").isNull()).count()
    negative_geometry_rows = cells.where(F.col("view_geometry_total") < 0).count()
    observed_platform_topics = (
        cells.where(F.col("allocation_variant") == "platform_only")
        .select("family", "leaf")
        .distinct()
    )
    missing_positive_topic_margins = (
        margins.where(F.col("tail_view_total") > 0)
        .select("family", "leaf")
        .join(observed_platform_topics, ["family", "leaf"], "left_anti")
        .count()
    )
    calibrated_platform_margins = (
        cells.where(F.col("allocation_variant") == "platform_only")
        .groupBy("population_scope", "family", "leaf")
        .agg(F.sum("view_geometry_tail_total").alias("calibrated_tail_view_total"))
        .join(
            margins.select("family", "leaf", "tail_view_total"),
            ["family", "leaf"],
            "inner",
        )
        .withColumn(
            "view_relative_error",
            F.abs(F.col("calibrated_tail_view_total") - F.col("tail_view_total"))
            / F.greatest(F.abs(F.col("tail_view_total")), F.lit(1.0)),
        )
    )
    max_topic_margin_error = float(
        calibrated_platform_margins.agg(F.max("view_relative_error").alias("value"))
        .first()["value"]
        or 0.0
    )
    global_checks = cells.groupBy("allocation_variant", "population_scope").agg(
        F.sum("view_geometry_global_share").alias("view_sum")
    )
    max_global_error = float(
        global_checks.agg(F.max(F.abs(F.col("view_sum") - 1.0)).alias("value"))
        .first()["value"]
        or 0.0
    )
    language_checks = cells.where(F.col("view_language_total") > 0).groupBy(
        "allocation_variant", "population_scope", "language"
    ).agg(F.sum("view_within_language_share").alias("view_sum"))
    max_language_error = float(
        language_checks.agg(F.max(F.abs(F.col("view_sum") - 1.0)).alias("value"))
        .first()["value"]
        or 0.0
    )
    family_checks = cells.where(F.col("view_language_family_total") > 0).groupBy(
        "allocation_variant", "population_scope", "language", "family"
    ).agg(F.sum("view_within_language_family_share").alias("view_sum"))
    max_family_error = float(
        family_checks.agg(F.max(F.abs(F.col("view_sum") - 1.0)).alias("value"))
        .first()["value"]
        or 0.0
    )
    metrics = {
        "treemap_cell_rows": int(row_count),
        "treemap_distinct_cell_keys": int(distinct_count),
        "unsampled_positive_topic_margin_rows": int(unsampled_margin_rows),
        "positive_topic_margins_without_sample_support": int(
            missing_positive_topic_margins
        ),
        "negative_geometry_rows": int(negative_geometry_rows),
        "max_platform_topic_margin_relative_error": max_topic_margin_error,
        "max_global_share_conservation_error": max_global_error,
        "max_within_language_conservation_error": max_language_error,
        "max_within_language_family_conservation_error": max_family_error,
    }
    if row_count != distinct_count:
        raise AssertionError(f"Treemap cell keys are not unique: {metrics}")
    if unsampled_margin_rows or missing_positive_topic_margins or negative_geometry_rows:
        raise AssertionError(f"Treemap geometry contains invalid rows: {metrics}")
    if max(max_topic_margin_error, max_global_error, max_language_error, max_family_error) > 1e-6:
        raise AssertionError(f"Treemap share conservation failed: {metrics}")
    return metrics


def equal_channel_frame_share_metrics(cells: DataFrame, frame: DataFrame) -> dict[str, float | int]:
    primary_variant = str(CONFIG["treemap"]["primary_allocation_variant"])
    primary_scope = str(CONFIG["treemap"]["primary_population_scope"])
    primary = cells.where(
        (F.col("allocation_variant") == F.lit(primary_variant))
        & (F.col("population_scope") == F.lit(primary_scope))
    )
    geometry = primary.agg(
        F.sum("channel_head_total").alias("exact_geometry_total"),
        F.sum("channel_geometry_tail_total").alias("tail_geometry_total"),
        F.max("channel_denominator").alias("geometry_denominator"),
    ).first()
    if geometry is None or geometry["geometry_denominator"] is None:
        raise AssertionError(
            "Primary equal-channel treemap cells are absent; cannot verify frame-stratum shares"
        )

    frame_counts = {
        str(row["subscriber_status"]): int(row["count"])
        for row in frame.groupBy("subscriber_status").count().collect()
    }
    census_n = frame_counts.get("census_ge10k", 0)
    tail_n = frame_counts.get("sample_frame_lt10k", 0)
    unknown_n = frame_counts.get("subscriber_unknown_or_hidden", 0)
    included_unknown_n = unknown_n if primary_scope == "all_retrievable" else 0
    expected_exact_n = census_n + included_unknown_n
    expected_denominator = expected_exact_n + tail_n
    geometry_denominator = float(geometry["geometry_denominator"])
    if expected_denominator <= 0 or geometry_denominator <= 0:
        raise AssertionError("Equal-channel frame denominator must be positive")

    expected_exact_share = expected_exact_n / expected_denominator
    expected_tail_share = tail_n / expected_denominator
    observed_exact_share = float(geometry["exact_geometry_total"] or 0.0) / geometry_denominator
    observed_tail_share = float(geometry["tail_geometry_total"] or 0.0) / geometry_denominator
    denominator_relative_error = abs(geometry_denominator - expected_denominator) / max(
        float(expected_denominator), 1.0
    )
    return {
        "primary_equal_channel_frame_denominator": int(expected_denominator),
        "primary_equal_channel_census_ge10k_n": int(census_n),
        "primary_equal_channel_tail_lt10k_n": int(tail_n),
        "primary_equal_channel_unknown_certainty_n": int(included_unknown_n),
        "primary_equal_channel_census_ge10k_share": float(census_n / expected_denominator),
        "primary_equal_channel_tail_lt10k_share": float(expected_tail_share),
        "primary_equal_channel_unknown_certainty_share": float(
            included_unknown_n / expected_denominator
        ),
        "primary_equal_channel_exact_strata_share_expected": float(expected_exact_share),
        "primary_equal_channel_exact_strata_share_observed": float(observed_exact_share),
        "primary_equal_channel_tail_share_observed": float(observed_tail_share),
        "primary_equal_channel_exact_strata_share_error": float(
            abs(observed_exact_share - expected_exact_share)
        ),
        "primary_equal_channel_tail_share_error": float(
            abs(observed_tail_share - expected_tail_share)
        ),
        "primary_equal_channel_denominator_relative_error": float(denominator_relative_error),
    }


def treemap_acceptance_metrics(
    cells: DataFrame, margins: DataFrame, frame: DataFrame
) -> dict[str, float | int]:
    key_columns = ["allocation_variant", "population_scope", "language", "family", "leaf"]
    row_count = cells.count()
    distinct_count = cells.select(*key_columns).distinct().count()
    unsampled_margin_rows = cells.where(
        F.col("view_geometry_tail_factor").isNull()
        | F.col("channel_geometry_tail_factor").isNull()
    ).count()
    negative_geometry_rows = cells.where(
        (F.col("view_geometry_total") < 0) | (F.col("channel_geometry_total") < 0)
    ).count()
    observed_platform_topics = (
        cells.where(F.col("allocation_variant") == "platform_only")
        .select("family", "leaf")
        .distinct()
    )
    missing_positive_topic_margins = (
        margins.where((F.col("tail_view_total") > 0) | (F.col("tail_channel_total") > 0))
        .select("family", "leaf")
        .join(observed_platform_topics, ["family", "leaf"], "left_anti")
        .count()
    )
    calibrated_platform_margins = (
        cells.where(F.col("allocation_variant") == "platform_only")
        .groupBy("population_scope", "family", "leaf")
        .agg(
            F.sum("view_geometry_tail_total").alias("calibrated_tail_view_total"),
            F.sum("channel_geometry_tail_total").alias("calibrated_tail_channel_total"),
        )
        .join(
            margins.select("family", "leaf", "tail_view_total", "tail_channel_total"),
            ["family", "leaf"],
            "inner",
        )
        .withColumn(
            "view_relative_error",
            F.abs(F.col("calibrated_tail_view_total") - F.col("tail_view_total"))
            / F.greatest(F.abs(F.col("tail_view_total")), F.lit(1.0)),
        )
        .withColumn(
            "channel_relative_error",
            F.abs(F.col("calibrated_tail_channel_total") - F.col("tail_channel_total"))
            / F.greatest(F.abs(F.col("tail_channel_total")), F.lit(1.0)),
        )
    )
    max_topic_margin_error = float(
        calibrated_platform_margins.agg(
            F.max(F.greatest("view_relative_error", "channel_relative_error")).alias("value")
        ).first()["value"]
        or 0.0
    )
    global_checks = cells.groupBy("allocation_variant", "population_scope").agg(
        F.sum("view_geometry_global_share").alias("view_sum"),
        F.sum("channel_geometry_global_share").alias("channel_sum"),
    )
    max_global_error = float(
        global_checks.select(
            F.greatest(F.abs(F.col("view_sum") - 1.0), F.abs(F.col("channel_sum") - 1.0)).alias(
                "error"
            )
        ).agg(F.max("error").alias("value")).first()["value"]
        or 0.0
    )
    language_checks = cells.groupBy("allocation_variant", "population_scope", "language").agg(
        F.sum("view_within_language_share").alias("view_sum"),
        F.sum("channel_within_language_share").alias("channel_sum"),
        F.max("view_language_total").alias("view_total"),
        F.max("channel_language_total").alias("channel_total"),
    )
    max_language_error = float(
        language_checks.select(
            F.greatest(
                F.when(F.col("view_total") > 0, F.abs(F.col("view_sum") - 1.0)).otherwise(
                    F.lit(0.0)
                ),
                F.when(
                    F.col("channel_total") > 0, F.abs(F.col("channel_sum") - 1.0)
                ).otherwise(F.lit(0.0)),
            ).alias(
                "error"
            )
        ).agg(F.max("error").alias("value")).first()["value"]
        or 0.0
    )
    family_checks = cells.groupBy(
        "allocation_variant", "population_scope", "language", "family"
    ).agg(
        F.sum("view_within_language_family_share").alias("view_sum"),
        F.sum("channel_within_language_family_share").alias("channel_sum"),
        F.max("view_language_family_total").alias("view_total"),
        F.max("channel_language_family_total").alias("channel_total"),
    )
    max_family_error = float(
        family_checks.select(
            F.greatest(
                F.when(F.col("view_total") > 0, F.abs(F.col("view_sum") - 1.0)).otherwise(
                    F.lit(0.0)
                ),
                F.when(
                    F.col("channel_total") > 0, F.abs(F.col("channel_sum") - 1.0)
                ).otherwise(F.lit(0.0)),
            ).alias(
                "error"
            )
        ).agg(F.max("error").alias("value")).first()["value"]
        or 0.0
    )
    metrics = {
        "treemap_cell_rows": int(row_count),
        "treemap_distinct_cell_keys": int(distinct_count),
        "unsampled_positive_topic_margin_rows": int(unsampled_margin_rows),
        "positive_topic_margins_without_sample_support": int(missing_positive_topic_margins),
        "negative_geometry_rows": int(negative_geometry_rows),
        "max_platform_topic_margin_relative_error": max_topic_margin_error,
        "max_global_share_conservation_error": max_global_error,
        "max_within_language_conservation_error": max_language_error,
        "max_within_language_family_conservation_error": max_family_error,
    }
    metrics.update(equal_channel_frame_share_metrics(cells, frame))
    max_stratum_share_error = max(
        float(metrics["primary_equal_channel_exact_strata_share_error"]),
        float(metrics["primary_equal_channel_tail_share_error"]),
        float(metrics["primary_equal_channel_denominator_relative_error"]),
    )
    if row_count != distinct_count:
        raise AssertionError(f"Treemap cell keys are not unique: {metrics}")
    if unsampled_margin_rows or missing_positive_topic_margins or negative_geometry_rows:
        raise AssertionError(f"Treemap geometry contains invalid rows: {metrics}")
    if max(max_topic_margin_error, max_global_error, max_language_error, max_family_error) > 1e-6:
        raise AssertionError(f"Treemap share conservation failed: {metrics}")
    if max_stratum_share_error > 1e-8:
        raise AssertionError(f"Equal-channel frame-stratum calibration failed: {metrics}")
    return metrics


def publish_treemap() -> dict[str, float | int | str]:
    required = ["estimates", "platform_margins", "frame"]
    if ANALYSIS_MODE == "full":
        required.append("differences")
    for name in required:
        require_table(TABLES[name])
    margins = spark.table(TABLES["platform_margins"])
    frame = spark.table(TABLES["frame"])
    if ANALYSIS_MODE == "attention_pps":
        publication = attention_publication_estimates(spark.table(TABLES["estimates"]))
        cells = calibrated_attention_treemap_cells(publication, margins)
        metrics = attention_treemap_acceptance_metrics(cells, margins)
        publication_comment = (
            "PPS attention rollups with raw design-based uncertainty; SRS channel estimates pending."
        )
        cells_comment = (
            "PPS attention language/family/leaf cells; calibrated totals are geometry only."
        )
    else:
        publication = paired_publication_estimates(
            spark.table(TABLES["estimates"]), spark.table(TABLES["differences"])
        )
        cells = calibrated_treemap_cells(publication, margins)
        metrics = treemap_acceptance_metrics(cells, margins, frame)
        publication_comment = (
            "Paired channel- and view-weighted rollup estimates with raw design-based uncertainty."
        )
        cells_comment = (
            "Additive language/family/leaf cells; calibrated totals are geometry only and raw HT/SRS fields remain inferential."
        )
    write_table(
        publication,
        TABLES["publication"],
        publication_comment,
    )
    write_table(
        cells,
        TABLES["treemap_cells"],
        cells_comment,
    )
    qa_rows = [
        (DESIGN_VERSION, key, json.dumps(value), datetime.now(timezone.utc))
        for key, value in metrics.items()
    ]
    write_table(
        spark.createDataFrame(
            qa_rows, "design_version string, metric string, value_json string, recorded_at timestamp"
        ),
        TABLES["treemap_qa"],
        "Treemap publication-cell uniqueness, nonnegativity, and additive-conservation checks.",
    )

    export_root_key = (
        "pps_attention_dbfs_export_root"
        if ANALYSIS_MODE == "attention_pps"
        else "dbfs_export_root"
    )
    export_root = CONFIG["treemap"][export_root_key].rstrip("/")
    cells_path = f"{export_root}/treemap_cells"
    publication_path = f"{export_root}/publication_estimates"
    cells.coalesce(1).write.mode("overwrite").parquet(cells_path)
    publication.coalesce(1).write.mode("overwrite").parquet(publication_path)
    manifest = {
        "design_version": DESIGN_VERSION,
        "frame_version": FRAME_VERSION,
        "analysis_mode": ANALYSIS_MODE,
        "publication_status": (
            "provisional_pending_remainder_deepseek"
            if ANALYSIS_MODE == "attention_pps"
            else "final_dual_sample"
        ),
        "tables": {
            "treemap_cells": TABLES["treemap_cells"],
            "publication_estimates": TABLES["publication"],
            "platform_topic_margins": TABLES["platform_margins"],
        },
        "exports": {
            "treemap_cells": cells_path,
            "publication_estimates": publication_path,
        },
        "primary_allocation_variant": CONFIG["treemap"]["primary_allocation_variant"],
        "primary_population_scope": CONFIG["treemap"]["primary_population_scope"],
        "geometry": {
            "attention": "view_geometry_total",
            "platform_only_calibration": "exact family/leaf tail margins",
            "model_completed_calibration": "known global tail total",
        },
        "uncertainty": "raw design-based shares and standard errors; calibrated geometry is descriptive",
        "qa": metrics,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    if ANALYSIS_MODE == "full":
        manifest["geometry"]["channel"] = "channel_geometry_total"
    manifest_path = f"{export_root}/run_manifest.json"
    dbutils.fs.put(manifest_path, json.dumps(manifest, indent=2, sort_keys=True), True)
    print("TREEMAP PUBLICATION: PASS")
    print("TREEMAP CONSERVATION: PASS")
    print(f"ANALYSIS MODE: {ANALYSIS_MODE}")
    print(f"TREEMAP CELLS EXPORT: {cells_path}")
    print(f"PUBLICATION ESTIMATES EXPORT: {publication_path}")
    print(f"TREEMAP MANIFEST: {manifest_path}")
    result: dict[str, float | int | str] = dict(metrics)
    result.update(
        {
            "treemap_cells_export": cells_path,
            "publication_estimates_export": publication_path,
            "manifest_path": manifest_path,
        }
    )
    return result


STAGES = {"allocate": allocate, "estimate": estimate, "qa": qa, "publish_treemap": publish_treemap}
if STAGE not in STAGES:
    raise ValueError(f"Unknown analysis stage {STAGE!r}; expected one of {sorted(STAGES)}")
print(f"RUNNING FULL-CORPUS ANALYSIS STAGE: {STAGE}")
RESULT = STAGES[STAGE]()
dbutils.notebook.exit(json.dumps({"stage": STAGE, "result": RESULT}, sort_keys=True, default=str))
