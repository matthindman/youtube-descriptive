# Databricks notebook source
# ruff: noqa: F821
# MAGIC %md
# MAGIC # Full-corpus YouTube language/topic treemap materialization
# MAGIC
# MAGIC Builds the publication treemap inputs from the current one-row-per-channel
# MAGIC LID silver lookup, weekly channel snapshots, and full-crawl topic arrays.
# MAGIC Raw language, topic, and traffic fields are retained; taxonomy remaps and
# MAGIC named-channel placements are separate display-layer fields.

# COMMAND ----------
from __future__ import annotations

import json
import math
import re
from datetime import date
from typing import Dict, Iterable

import yaml
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


# COMMAND ----------
def _widget(name: str, default: str, label: str | None = None) -> None:
    try:
        dbutils.widgets.text(name, default, label or name)
    except Exception:
        pass


def _get(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value != "" else default
    except Exception:
        return default


def _get_int(name: str, default: int) -> int:
    return int(_get(name, str(default)))


def _get_bool(name: str, default: bool) -> bool:
    return _get(name, str(default).lower()).strip().lower() in {"1", "true", "yes", "y"}


def _safe_token(value: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()
    if not token:
        raise ValueError(f"Unsafe empty token derived from {value!r}")
    return token


def _read_text(path: str) -> str:
    if path.startswith("dbfs:/"):
        return dbutils.fs.head(path, 10_000_000)
    if path.startswith("/dbfs/"):
        with open(path, encoding="utf-8") as handle:
            return handle.read()
    try:
        return dbutils.fs.head(path, 10_000_000)
    except Exception:
        with open(path, encoding="utf-8") as handle:
            return handle.read()


def _table(name: str) -> DataFrame:
    if not spark.catalog.tableExists(name):
        raise ValueError(f"Required table is missing or unreadable: {name}")
    return spark.table(name)


def _require_columns(df: DataFrame, table: str, columns: Iterable[str]) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"{table} is missing required columns: {missing}")


def _write_table(df: DataFrame, table: str, *, comment: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    escaped = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {table} IS '{escaped}'")
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ("
        f"'treemap_run_id'='{RUN_ID}', "
        f"'source_label_version'='{EXPECTED_LABEL_VERSION}', "
        f"'source_current_snapshot'='{CURRENT_SNAPSHOT}', "
        f"'source_prior_snapshot'='{PRIOR_SNAPSHOT}')"
    )
    print(f"WROTE TABLE: {table}")


def _first(df: DataFrame, column: str):
    return df.select(column).first()[column]


# COMMAND ----------
today_token = date.today().strftime("%Y%m%d")
_widget("run_id", f"full_corpus_lid_v3_20260715_{today_token}_v1")
_widget(
    "language_table",
    "dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_current",
)
_widget("expected_label_version", "lid_v3_channel_crawl_full_20260623_deepseek_flash_20260715_v1")
_widget("topic_table", "dev_sean.default.channel_category")
_widget("stats_table", "dev_sean.default.yt_channel_stats")
_widget("current_snapshot", "2026-06-15")
_widget("prior_snapshot", "2026-05-18")
_widget("minimum_subscribers", "10000")
_widget("top_k_languages", "12")
_widget("top_channels_per_leaf", "15")
_widget("hierarchy_config_path", "config/youtube_topic_hierarchy_v2.yaml")
_widget("topic_remap_path", "config/topic_remap.yaml")
_widget("language_normalization_path", "config/language_normalization.yaml")
_widget("language_names_path", "config/iso639_language_names.csv")
_widget("placement_csv_path", "config/treemap_top_channel_placement.csv")
_widget("output_catalog", "dev_sean")
_widget("output_schema", "matt")
_widget("table_prefix", "yt_treemap_full_corpus_lid_v3_20260715_v1")
_widget("write_delta_tables", "true")
_widget("artifact_dir", f"dbfs:/FileStore/youtube_topic_treemap_full_corpus_{today_token}_v1")

RUN_ID = _safe_token(_get("run_id", f"full_corpus_lid_v3_20260715_{today_token}_v1"))
LANGUAGE_TABLE = _get(
    "language_table",
    "dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_current",
)
EXPECTED_LABEL_VERSION = _get(
    "expected_label_version",
    "lid_v3_channel_crawl_full_20260623_deepseek_flash_20260715_v1",
)
TOPIC_TABLE = _get("topic_table", "dev_sean.default.channel_category")
STATS_TABLE = _get("stats_table", "dev_sean.default.yt_channel_stats")
CURRENT_SNAPSHOT = _get("current_snapshot", "2026-06-15")
PRIOR_SNAPSHOT = _get("prior_snapshot", "2026-05-18")
MINIMUM_SUBSCRIBERS = _get_int("minimum_subscribers", 10_000)
TOP_K_LANGUAGES = _get_int("top_k_languages", 12)
TOP_CHANNELS_PER_LEAF = _get_int("top_channels_per_leaf", 15)
HIERARCHY_CONFIG_PATH = _get("hierarchy_config_path", "config/youtube_topic_hierarchy_v2.yaml")
TOPIC_REMAP_PATH = _get("topic_remap_path", "config/topic_remap.yaml")
LANGUAGE_NORMALIZATION_PATH = _get("language_normalization_path", "config/language_normalization.yaml")
LANGUAGE_NAMES_PATH = _get("language_names_path", "config/iso639_language_names.csv")
PLACEMENT_CSV_PATH = _get("placement_csv_path", "config/treemap_top_channel_placement.csv")
OUTPUT_CATALOG = _safe_token(_get("output_catalog", "dev_sean"))
OUTPUT_SCHEMA = _safe_token(_get("output_schema", "matt"))
TABLE_PREFIX = _safe_token(_get("table_prefix", "yt_treemap_full_corpus_lid_v3_20260715_v1"))
WRITE_DELTA_TABLES = _get_bool("write_delta_tables", True)
ARTIFACT_DIR = _get("artifact_dir", f"dbfs:/FileStore/youtube_topic_treemap_full_corpus_{today_token}_v1")

TABLES = {
    "channel_base": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{TABLE_PREFIX}_channel_base",
    "topic_projection": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{TABLE_PREFIX}_topic_projection",
    "allocations_raw": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{TABLE_PREFIX}_allocations_family_balanced_raw",
    "allocations_display": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{TABLE_PREFIX}_allocations_display_v3",
    "aggregate": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{TABLE_PREFIX}_language_family_leaf",
    "top_channels": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{TABLE_PREFIX}_top15_channels_per_leaf",
    "renderer_rows": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{TABLE_PREFIX}_renderer_rows",
    "qa": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{TABLE_PREFIX}_qa",
}

UNMAPPED_FAMILY = "Other / Unmapped YouTube topic"
UNLABELED_FAMILY = "Unlabeled"
UNLABELED_LEAF = "No YouTube topicCategories"
OTHER_LANGUAGES = "Other languages"
UNDETERMINED = "Undetermined"

print("RUN ID:", RUN_ID)
print("LANGUAGE TABLE:", LANGUAGE_TABLE)
print("TOPIC TABLE:", TOPIC_TABLE)
print("STATS TABLE:", STATS_TABLE)
print("TRAFFIC SNAPSHOTS:", CURRENT_SNAPSHOT, PRIOR_SNAPSHOT)
print("SUBSCRIBER FLOOR:", MINIMUM_SUBSCRIBERS)
print("ARTIFACT DIR:", ARTIFACT_DIR)


# COMMAND ----------
# Build small taxonomy and language lookup frames from editable config.
hierarchy = yaml.safe_load(_read_text(HIERARCHY_CONFIG_PATH)) or {}
families: Dict[str, dict] = hierarchy.get("families", {})
aliases: Dict[str, str] = {str(k): str(v) for k, v in (hierarchy.get("aliases", {}) or {}).items()}
topic_remap = (yaml.safe_load(_read_text(TOPIC_REMAP_PATH)) or {}).get("unmapped_remap", {}) or {}
language_cfg = yaml.safe_load(_read_text(LANGUAGE_NORMALIZATION_PATH)) or {}

topic_map: dict[str, dict[str, str]] = {}
for family, spec in families.items():
    for raw_parent in spec.get("parent_slugs") or []:
        canonical = aliases.get(str(raw_parent), str(raw_parent))
        topic_map.setdefault(
            canonical,
            {
                "canonical_slug": canonical,
                "yt_family_raw": family,
                "yt_leaf_raw": f"[{family}] - unspecified",
                "node_type": "parent",
                "yt_family": family,
                "yt_leaf": f"[{family}] - unspecified",
                "display_mapping_source": "hierarchy_config",
            },
        )
    for raw_child, leaf in (spec.get("children") or {}).items():
        canonical = aliases.get(str(raw_child), str(raw_child))
        topic_map.setdefault(
            canonical,
            {
                "canonical_slug": canonical,
                "yt_family_raw": family,
                "yt_leaf_raw": str(leaf),
                "node_type": "child",
                "yt_family": family,
                "yt_leaf": str(leaf),
                "display_mapping_source": "hierarchy_config",
            },
        )

for old_leaf, target in topic_remap.items():
    prefix = "Unmapped: "
    if not str(old_leaf).startswith(prefix):
        raise ValueError(f"Unsupported topic remap key: {old_leaf!r}")
    canonical = str(old_leaf)[len(prefix):]
    existing = topic_map.get(canonical)
    topic_map[canonical] = {
        "canonical_slug": canonical,
        "yt_family_raw": existing["yt_family_raw"] if existing else UNMAPPED_FAMILY,
        "yt_leaf_raw": existing["yt_leaf_raw"] if existing else str(old_leaf),
        "node_type": existing["node_type"] if existing else "unmapped",
        "yt_family": str(target["family"]),
        "yt_leaf": str(target["leaf"]),
        "display_mapping_source": "topic_remap_config",
    }

topic_map_df = spark.createDataFrame(list(topic_map.values()))
alias_df = spark.createDataFrame(
    [(str(raw), str(canonical)) for raw, canonical in sorted(aliases.items())],
    "raw_slug string, alias_canonical_slug string",
)

language_catalog = (
    spark.read.option("header", "true").csv(LANGUAGE_NAMES_PATH)
    .select(
        F.lower(F.trim("language_code")).alias("catalog_language_code"),
        F.lower(F.trim("canonical_iso639_3")).alias("catalog_canonical_iso639_3"),
        F.col("display_name").alias("catalog_display_name"),
        F.col("mapping_source").alias("catalog_mapping_source"),
    )
    .dropDuplicates(["catalog_language_code"])
)

language_override_rows = []
for display_name, codes in (language_cfg.get("display_to_bases", {}) or {}).items():
    for code in codes or []:
        language_override_rows.append((str(code).lower(), str(display_name)))
override_targets: dict[str, set[str]] = {}
for code, display_name in language_override_rows:
    override_targets.setdefault(code, set()).add(display_name)
ambiguous_overrides = {code: names for code, names in override_targets.items() if len(names) > 1}
if ambiguous_overrides:
    raise ValueError(f"Language normalization codes map to multiple display names: {ambiguous_overrides}")
language_overrides = spark.createDataFrame(
    sorted(set(language_override_rows)),
    "override_language_code string, override_display_name string",
)

print(
    "CONFIG:",
    {
        "families": len(families),
        "topic_nodes": len(topic_map),
        "topic_aliases": len(aliases),
        "topic_remaps": len(topic_remap),
        "language_editorial_overrides": len(set(language_override_rows)),
    },
)


# COMMAND ----------
# Source extraction: language is the universe; traffic and topics are left joins.
language_raw = _table(LANGUAGE_TABLE)
topic_raw = _table(TOPIC_TABLE)
stats_raw = _table(STATS_TABLE)
_require_columns(
    language_raw,
    LANGUAGE_TABLE,
    ["channel_id", "channel_language", "is_language_classified", "label_version"],
)
_require_columns(topic_raw, TOPIC_TABLE, ["canonical_id", "topic_categories", "collected_at", "collected_date"])
_require_columns(
    stats_raw,
    STATS_TABLE,
    ["canonical_id", "channel_name", "subscriber_count", "total_view_count", "collected_at"],
)
if not isinstance(dict((field.name, field.dataType) for field in topic_raw.schema.fields)["topic_categories"], ArrayType):
    raise ValueError(f"{TOPIC_TABLE}.topic_categories must be array<string>")

language_ids = language_raw.select(F.col("channel_id").cast("string").alias("channel_id"))

stats_window = Window.partitionBy("canonical_id", F.to_date("collected_at")).orderBy(
    F.col("collected_at").desc()
)
stats_two_dates = (
    stats_raw
    .where(F.to_date("collected_at").isin(CURRENT_SNAPSHOT, PRIOR_SNAPSHOT))
    .select(
        F.col("canonical_id").cast("string").alias("canonical_id"),
        F.col("channel_name").cast("string").alias("channel_name"),
        F.col("subscriber_count").cast("long").alias("subscriber_count"),
        F.col("total_view_count").cast("double").alias("total_view_count"),
        F.col("collected_at").cast("timestamp").alias("collected_at"),
    )
    .withColumn("_rn", F.row_number().over(stats_window))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)
current_stats = stats_two_dates.where(F.to_date("collected_at") == F.to_date(F.lit(CURRENT_SNAPSHOT))).select(
    F.col("canonical_id").alias("channel_id"),
    F.col("channel_name").alias("current_channel_name"),
    F.col("subscriber_count").alias("current_subscriber_count"),
    F.col("total_view_count").alias("current_lifetime_views"),
    F.col("collected_at").alias("current_collected_at"),
)
prior_stats = stats_two_dates.where(F.to_date("collected_at") == F.to_date(F.lit(PRIOR_SNAPSHOT))).select(
    F.col("canonical_id").alias("channel_id"),
    F.col("total_view_count").alias("prior_lifetime_views"),
    F.col("collected_at").alias("prior_collected_at"),
)

topic_timestamp_window = Window.partitionBy("channel_id").orderBy(
    F.col("_topic_timestamp").desc_nulls_last(),
    F.col("_topic_date").desc_nulls_last(),
)
topic_latest = (
    topic_raw
    .select(
        F.col("canonical_id").cast("string").alias("channel_id"),
        F.col("topic_categories").cast("array<string>").alias("raw_topic_categories_source"),
        F.to_timestamp("collected_at").alias("_topic_timestamp"),
        F.to_date("collected_date").alias("_topic_date"),
    )
    .join(language_ids, on="channel_id", how="inner")
    .withColumn("_topic_rn", F.row_number().over(topic_timestamp_window))
    .where(F.col("_topic_rn") == 1)
    .drop("_topic_rn")
)

language_joined = (
    language_raw.alias("language")
    .join(current_stats, on="channel_id", how="left")
    .join(prior_stats, on="channel_id", how="left")
    .join(topic_latest, on="channel_id", how="left")
    .withColumn("channel_language_normalized", F.lower(F.trim(F.col("channel_language"))))
    .join(
        F.broadcast(language_catalog),
        F.col("channel_language_normalized") == F.col("catalog_language_code"),
        "left",
    )
    .join(
        F.broadcast(language_overrides),
        F.col("channel_language_normalized") == F.col("override_language_code"),
        "left",
    )
)

raw_delta = F.col("current_lifetime_views") - F.col("prior_lifetime_views")
channel_base = (
    language_joined
    .withColumn("channel_name", F.coalesce(F.col("current_channel_name"), F.col("channel_id")))
    .withColumn("raw_4wk_views", raw_delta)
    .withColumn(
        "view_count_4wk",
        F.when(
            F.col("prior_lifetime_views").isNotNull()
            & (F.col("current_lifetime_views") >= F.col("prior_lifetime_views")),
            raw_delta,
        ).cast("double"),
    )
    .withColumn("avg_weekly_view_count", F.col("view_count_4wk") / F.lit(4.0))
    .withColumn("has_current_snapshot", F.col("current_collected_at").isNotNull())
    .withColumn("has_prior_snapshot", F.col("prior_collected_at").isNotNull())
    .withColumn("has_invalid_negative_delta", F.col("raw_4wk_views") < F.lit(0.0))
    .withColumn("has_valid_4wk_views", F.col("view_count_4wk").isNotNull())
    .withColumn("has_positive_4wk_views", F.col("view_count_4wk") > F.lit(0.0))
    .withColumn(
        "in_subscriber_cohort",
        F.col("current_subscriber_count") >= F.lit(MINIMUM_SUBSCRIBERS),
    )
    .withColumn("topic_row_present", F.col("_topic_timestamp").isNotNull())
    .withColumn(
        "raw_topic_categories",
        F.coalesce(F.col("raw_topic_categories_source"), F.array().cast("array<string>")),
    )
    .withColumn("has_nonempty_topic_categories", F.size("raw_topic_categories") > 0)
    .withColumn(
        "language_display",
        F.when(F.col("channel_language_normalized") == F.lit("und"), F.lit(UNDETERMINED))
        .when(F.col("override_display_name").isNotNull(), F.col("override_display_name"))
        .when(F.col("catalog_display_name").isNotNull(), F.col("catalog_display_name"))
        .otherwise(
            F.concat(F.lit("Unregistered code ("), F.col("channel_language_normalized"), F.lit(")"))
        ),
    )
    .withColumn(
        "language_mapping_status",
        F.when(F.col("channel_language_normalized") == F.lit("und"), F.lit("undetermined"))
        .when(F.col("catalog_display_name").isNull(), F.lit("unregistered_or_legacy"))
        .when(F.col("override_display_name").isNotNull(), F.lit("editorial_override"))
        .otherwise(F.col("catalog_mapping_source")),
    )
    .withColumn(
        "canonical_iso639_3",
        F.when(F.col("channel_language_normalized") == F.lit("und"), F.lit("und"))
        .otherwise(F.coalesce(F.col("catalog_canonical_iso639_3"), F.col("channel_language_normalized"))),
    )
    .withColumn("treemap_run_id", F.lit(RUN_ID))
    .drop(
        "catalog_language_code",
        "catalog_canonical_iso639_3",
        "catalog_display_name",
        "catalog_mapping_source",
        "override_language_code",
        "override_display_name",
        "raw_topic_categories_source",
        "_topic_timestamp",
        "_topic_date",
    )
)

if WRITE_DELTA_TABLES:
    _write_table(
        channel_base,
        TABLES["channel_base"],
        comment=(
            "Full current LID silver channel universe enriched with fixed 2026-06-15/2026-05-18 "
            "traffic snapshots and latest full-crawl YouTube topic arrays. Negative deltas remain null."
        ),
    )
    channel_base = spark.table(TABLES["channel_base"])


# COMMAND ----------
# Spark-native hierarchy projection for the >=10k-subscriber analysis cohort.
cohort = channel_base.where(F.col("in_subscriber_cohort"))

topic_urls = cohort.select(
    "channel_id",
    F.explode_outer("raw_topic_categories").alias("raw_topic_url"),
)
url_without_suffix = F.regexp_replace(F.trim(F.col("raw_topic_url")), r"[?#].*$", "")
url_without_trailing_slash = F.regexp_replace(url_without_suffix, r"/+$", "")
raw_slug_expr = F.lower(
    F.regexp_replace(F.element_at(F.split(url_without_trailing_slash, "/"), -1), r"\s+", "_")
)

slug_rows = (
    topic_urls
    .withColumn("raw_slug", raw_slug_expr)
    .where(F.col("raw_slug").isNotNull() & (F.length("raw_slug") > 0))
    .join(F.broadcast(alias_df), on="raw_slug", how="left")
    .withColumn("canonical_slug", F.coalesce(F.col("alias_canonical_slug"), F.col("raw_slug")))
    .drop("alias_canonical_slug")
    .dropDuplicates(["channel_id", "canonical_slug"])
)

mapped_slug_rows = (
    slug_rows
    .join(F.broadcast(topic_map_df), on="canonical_slug", how="left")
    .withColumn("yt_family_raw", F.coalesce(F.col("yt_family_raw"), F.lit(UNMAPPED_FAMILY)))
    .withColumn(
        "yt_leaf_raw",
        F.coalesce(F.col("yt_leaf_raw"), F.concat(F.lit("Unmapped: "), F.col("canonical_slug"))),
    )
    .withColumn("node_type", F.coalesce(F.col("node_type"), F.lit("unmapped")))
    .withColumn("yt_family", F.coalesce(F.col("yt_family"), F.col("yt_family_raw")))
    .withColumn("yt_leaf", F.coalesce(F.col("yt_leaf"), F.col("yt_leaf_raw")))
    .withColumn(
        "display_mapping_source",
        F.coalesce(F.col("display_mapping_source"), F.lit("unmapped_passthrough")),
    )
)

families_with_children = (
    mapped_slug_rows.where(F.col("node_type") == F.lit("child"))
    .select("channel_id", "yt_family_raw")
    .distinct()
)
display_candidates = (
    mapped_slug_rows.where(F.col("node_type") != F.lit("parent"))
    .unionByName(
        mapped_slug_rows.where(F.col("node_type") == F.lit("parent"))
        .join(families_with_children, on=["channel_id", "yt_family_raw"], how="left_anti")
    )
    .select(
        "channel_id",
        "canonical_slug",
        "yt_family_raw",
        "yt_leaf_raw",
        "node_type",
        "yt_family",
        "yt_leaf",
        "display_mapping_source",
    )
    # Multiple canonical source slugs can resolve to the same displayed leaf.
    # Allocation is over displayed leaves, not duplicate source-label paths.
    .dropDuplicates(["channel_id", "yt_family", "yt_leaf"])
)

unlabeled_candidates = (
    cohort.select("channel_id")
    .join(display_candidates.select("channel_id").distinct(), on="channel_id", how="left_anti")
    .select(
        "channel_id",
        F.lit("").alias("canonical_slug"),
        F.lit(UNLABELED_FAMILY).alias("yt_family_raw"),
        F.lit(UNLABELED_LEAF).alias("yt_leaf_raw"),
        F.lit("unlabeled").alias("node_type"),
        F.lit(UNLABELED_FAMILY).alias("yt_family"),
        F.lit(UNLABELED_LEAF).alias("yt_leaf"),
        F.lit("no_topic_categories").alias("display_mapping_source"),
    )
)
display_candidates = display_candidates.unionByName(unlabeled_candidates)

slug_aggregate = slug_rows.groupBy("channel_id").agg(
    F.sort_array(F.collect_set("raw_slug")).alias("normalized_topic_slugs"),
    F.sort_array(F.collect_set("canonical_slug")).alias("canonical_topic_slugs"),
)
candidate_aggregate = display_candidates.groupBy("channel_id").agg(
    F.sort_array(
        F.collect_set(
            F.struct(
                "yt_family_raw",
                "yt_leaf_raw",
                "yt_family",
                "yt_leaf",
                "canonical_slug",
                "node_type",
                "display_mapping_source",
            )
        )
    ).alias("display_items"),
    F.size(F.collect_set("yt_family")).alias("n_display_families"),
    F.size(F.collect_set(F.struct("yt_family", "yt_leaf"))).alias("n_display_leaves"),
    F.max((F.col("node_type") == F.lit("unmapped")).cast("int")).cast("boolean").alias("has_unmapped_labels"),
    F.max((F.col("node_type") == F.lit("parent")).cast("int")).cast("boolean").alias("has_parent_only_label"),
    F.max((F.col("node_type") == F.lit("unlabeled")).cast("int")).cast("boolean").alias("has_no_topic_categories"),
)

projection = (
    cohort
    .join(slug_aggregate, on="channel_id", how="left")
    .join(candidate_aggregate, on="channel_id", how="inner")
    .withColumn(
        "normalized_topic_slugs",
        F.coalesce(F.col("normalized_topic_slugs"), F.array().cast("array<string>")),
    )
    .withColumn(
        "canonical_topic_slugs",
        F.coalesce(F.col("canonical_topic_slugs"), F.array().cast("array<string>")),
    )
    .withColumn("treemap_run_id", F.lit(RUN_ID))
)

if WRITE_DELTA_TABLES:
    _write_table(
        projection,
        TABLES["topic_projection"],
        comment=(
            "One row per >=10k-subscriber channel with raw topic arrays, normalized/canonical slugs, "
            "and hierarchy display items. Raw and display taxonomy fields are separate."
        ),
    )
    projection = spark.table(TABLES["topic_projection"])


# COMMAND ----------
# Family-balanced allocation, followed by the editable named-channel display layer.
candidate_leaves = (
    projection.select(
        "channel_id",
        "channel_name",
        "channel_language",
        "language_display",
        "current_subscriber_count",
        "current_lifetime_views",
        "raw_4wk_views",
        "view_count_4wk",
        "raw_topic_categories",
        "normalized_topic_slugs",
        "canonical_topic_slugs",
        F.explode("display_items").alias("di"),
    )
    .select(
        "channel_id",
        "channel_name",
        "channel_language",
        "language_display",
        "current_subscriber_count",
        "current_lifetime_views",
        "raw_4wk_views",
        "view_count_4wk",
        "raw_topic_categories",
        "normalized_topic_slugs",
        "canonical_topic_slugs",
        F.col("di.yt_family_raw").alias("yt_family_raw"),
        F.col("di.yt_leaf_raw").alias("yt_leaf_raw"),
        F.col("di.yt_family").alias("yt_family"),
        F.col("di.yt_leaf").alias("yt_leaf"),
        F.col("di.canonical_slug").alias("canonical_slug"),
        F.col("di.node_type").alias("node_type"),
        F.col("di.display_mapping_source").alias("display_mapping_source"),
    )
)
family_counts = candidate_leaves.select("channel_id", "yt_family").distinct().groupBy("channel_id").agg(
    F.count("*").alias("n_families")
)
family_leaf_counts = candidate_leaves.groupBy("channel_id", "yt_family").agg(
    F.count("*").alias("n_family_leaves")
)

raw_allocations = (
    candidate_leaves
    .join(family_counts, on="channel_id", how="left")
    .join(family_leaf_counts, on=["channel_id", "yt_family"], how="left")
    .withColumn("allocation_method", F.lit("family_balanced"))
    .withColumn(
        "allocation_weight",
        F.lit(1.0) / F.col("n_families").cast("double") / F.col("n_family_leaves").cast("double"),
    )
    .withColumn("allocated_views_4wk", F.col("view_count_4wk") * F.col("allocation_weight"))
    .withColumn("treemap_run_id", F.lit(RUN_ID))
)

if WRITE_DELTA_TABLES:
    _write_table(
        raw_allocations,
        TABLES["allocations_raw"],
        comment=(
            "Unmodified family-balanced topic allocations before named-channel placement overrides; "
            "raw topic arrays, raw/display taxonomy fields, raw traffic, and allocation weights are retained."
        ),
    )
    raw_allocations = spark.table(TABLES["allocations_raw"])

placements_raw = spark.read.option("header", "true").csv(PLACEMENT_CSV_PATH)
_require_columns(
    placements_raw,
    PLACEMENT_CSV_PATH,
    ["channel_id", "revised_primary_family", "revised_primary_leaf", "needs_manual_review"],
)
placements = (
    placements_raw
    .select(
        F.col("channel_id").cast("string").alias("placement_channel_id"),
        F.col("revised_primary_family").cast("string").alias("placement_family"),
        F.col("revised_primary_leaf").cast("string").alias("placement_leaf"),
        F.lower(F.trim(F.col("needs_manual_review"))).isin("true", "1", "yes").alias("needs_manual_review"),
        F.col("revised_primary_path").cast("string").alias("revised_primary_path"),
        F.col("non_primary_display_paths_to_retain_as_metadata").cast("string").alias("non_primary_paths"),
    )
    .dropDuplicates(["placement_channel_id"])
)
placed_ids = placements.select(F.col("placement_channel_id").alias("channel_id"))

unplaced_display = (
    raw_allocations.join(placed_ids, on="channel_id", how="left_anti")
    .withColumn("allocation_weight_raw", F.col("allocation_weight"))
    .withColumn("allocated_views_4wk_raw", F.col("allocated_views_4wk"))
    .withColumn("is_placement_override", F.lit(False))
    .withColumn("needs_manual_review", F.lit(False))
    .withColumn("revised_primary_path", F.lit(None).cast("string"))
    .withColumn("non_primary_paths", F.lit(None).cast("string"))
)

placed_display = (
    projection.alias("p")
    .join(
        placements.alias("placement"),
        F.col("p.channel_id") == F.col("placement.placement_channel_id"),
        "inner",
    )
    .select(
        F.col("p.channel_id"),
        F.col("p.channel_name"),
        F.col("p.channel_language"),
        F.col("p.language_display"),
        F.col("p.current_subscriber_count"),
        F.col("p.current_lifetime_views"),
        F.col("p.raw_4wk_views"),
        F.col("p.view_count_4wk"),
        F.col("p.raw_topic_categories"),
        F.col("p.normalized_topic_slugs"),
        F.col("p.canonical_topic_slugs"),
        F.lit(None).cast("string").alias("yt_family_raw"),
        F.lit(None).cast("string").alias("yt_leaf_raw"),
        F.col("placement.placement_family").alias("yt_family"),
        F.col("placement.placement_leaf").alias("yt_leaf"),
        F.lit("").alias("canonical_slug"),
        F.lit("placement_override").alias("node_type"),
        F.lit("treemap_top_channel_placement_csv").alias("display_mapping_source"),
        F.lit(1).alias("n_families"),
        F.lit(1).alias("n_family_leaves"),
        F.lit("family_balanced_display_v3").alias("allocation_method"),
        F.lit(None).cast("double").alias("allocation_weight_raw"),
        F.lit(None).cast("double").alias("allocated_views_4wk_raw"),
        F.lit(1.0).alias("allocation_weight"),
        F.col("p.view_count_4wk").alias("allocated_views_4wk"),
        F.lit(RUN_ID).alias("treemap_run_id"),
        F.lit(True).alias("is_placement_override"),
        F.col("placement.needs_manual_review"),
        F.col("placement.revised_primary_path"),
        F.col("placement.non_primary_paths"),
    )
)

display_columns = placed_display.columns
display_allocations = (
    unplaced_display
    .withColumn("allocation_method", F.lit("family_balanced_display_v3"))
    .select(*display_columns)
    .unionByName(placed_display.select(*display_columns))
)

if WRITE_DELTA_TABLES:
    _write_table(
        display_allocations,
        TABLES["allocations_display"],
        comment=(
            "Treemap display allocations after editable named-channel placement overrides. Raw family-balanced "
            "allocations remain in the companion raw table; raw traffic/topic columns are unchanged."
        ),
    )
    display_allocations = spark.table(TABLES["allocations_display"])


# COMMAND ----------
# Full-language compact aggregates and top-15 channel nodes.
positive_display = display_allocations.where(F.col("allocated_views_4wk") > 0)
language_family_leaf = positive_display.groupBy("language_display", "yt_family", "yt_leaf").agg(
    F.sum("allocated_views_4wk").alias("allocated_views_4wk"),
    F.countDistinct("channel_id").alias("channel_count"),
    F.max("treemap_run_id").alias("treemap_run_id"),
)

channel_leaf = positive_display.groupBy(
    "language_display", "yt_family", "yt_leaf", "channel_id"
).agg(
    F.first("channel_name", ignorenulls=True).alias("channel_name"),
    F.sum("allocated_views_4wk").alias("allocated_views_4wk"),
    F.first("view_count_4wk", ignorenulls=True).alias("view_count_4wk"),
    F.sum("allocation_weight").alias("allocation_weight"),
    F.first("raw_topic_categories", ignorenulls=True).alias("raw_topic_categories"),
    F.max("is_placement_override").alias("is_placement_override"),
    F.max("needs_manual_review").alias("needs_manual_review"),
)
leaf_rank = Window.partitionBy("language_display", "yt_family", "yt_leaf").orderBy(
    F.col("is_placement_override").desc(),
    F.col("allocated_views_4wk").desc(),
    F.col("channel_id").asc(),
)
ranked_channels = channel_leaf.withColumn("channel_rank", F.row_number().over(leaf_rank))
top_channels = (
    ranked_channels.where(F.col("channel_rank") <= F.lit(TOP_CHANNELS_PER_LEAF))
    .withColumn("is_other_channel_pool", F.lit(False))
    .withColumn("pooled_channel_count", F.lit(None).cast("long"))
)
other_channels = (
    ranked_channels.where(F.col("channel_rank") > F.lit(TOP_CHANNELS_PER_LEAF))
    .groupBy("language_display", "yt_family", "yt_leaf")
    .agg(
        F.sum("allocated_views_4wk").alias("allocated_views_4wk"),
        F.count("*").alias("pooled_channel_count"),
    )
    .withColumn("channel_id", F.lit(None).cast("string"))
    .withColumn(
        "channel_name",
        F.concat(F.lit("Other ("), F.format_number("pooled_channel_count", 0), F.lit(" channels)")),
    )
    .withColumn("view_count_4wk", F.lit(None).cast("double"))
    .withColumn("allocation_weight", F.lit(None).cast("double"))
    .withColumn("raw_topic_categories", F.lit(None).cast("array<string>"))
    .withColumn("is_placement_override", F.lit(False))
    .withColumn("needs_manual_review", F.lit(False))
    .withColumn("channel_rank", F.lit(TOP_CHANNELS_PER_LEAF + 1))
    .withColumn("is_other_channel_pool", F.lit(True))
)
top_channel_columns = top_channels.columns
top15_channels_per_leaf = top_channels.unionByName(other_channels.select(*top_channel_columns))

if WRITE_DELTA_TABLES:
    _write_table(
        language_family_leaf,
        TABLES["aggregate"],
        comment="Compact allocated 4-week view totals for every language/family/leaf in the >=10k cohort.",
    )
    _write_table(
        top15_channels_per_leaf.withColumn("treemap_run_id", F.lit(RUN_ID)),
        TABLES["top_channels"],
        comment="Top 15 channels plus one Other (N channels) node per full-language topic leaf.",
    )
    language_family_leaf = spark.table(TABLES["aggregate"])
    top15_channels_per_leaf = spark.table(TABLES["top_channels"])


# COMMAND ----------
# Bounded renderer rows: top classified languages, Undetermined, and a classified tail pool.
language_totals = positive_display.groupBy("language_display").agg(
    F.sum("allocated_views_4wk").alias("language_allocated_views")
)
top_language_names = [
    row["language_display"]
    for row in language_totals
    .where(F.col("language_display") != F.lit(UNDETERMINED))
    .orderBy(F.col("language_allocated_views").desc(), F.col("language_display").asc())
    .limit(TOP_K_LANGUAGES)
    .collect()
]
static_display = positive_display.withColumn(
    "static_language",
    F.when(F.col("language_display") == F.lit(UNDETERMINED), F.lit(UNDETERMINED))
    .when(F.col("language_display").isin(top_language_names), F.col("language_display"))
    .otherwise(F.lit(OTHER_LANGUAGES)),
)

static_channel_leaf = static_display.groupBy(
    "static_language", "yt_family", "yt_leaf", "channel_id"
).agg(
    F.first("channel_name", ignorenulls=True).alias("channel_name"),
    F.sum("allocated_views_4wk").alias("allocated_views_4wk"),
    F.first("view_count_4wk", ignorenulls=True).alias("view_count_4wk"),
    F.sum("allocation_weight").alias("allocation_weight"),
    F.first("raw_topic_categories", ignorenulls=True).alias("raw_topic_categories"),
    F.max("is_placement_override").alias("is_placement_override"),
    F.max("needs_manual_review").alias("needs_manual_review"),
)
static_rank_window = Window.partitionBy("static_language", "yt_family", "yt_leaf").orderBy(
    F.col("is_placement_override").desc(),
    F.col("allocated_views_4wk").desc(),
    F.col("channel_id").asc(),
)
static_ranked = static_channel_leaf.withColumn("channel_rank", F.row_number().over(static_rank_window))
renderer_top = (
    static_ranked.where(F.col("channel_rank") <= F.lit(TOP_CHANNELS_PER_LEAF))
    .withColumn("is_other_channel_pool", F.lit(False))
    .withColumn("pooled_channel_count", F.lit(None).cast("long"))
)
renderer_other = (
    static_ranked.where(F.col("channel_rank") > F.lit(TOP_CHANNELS_PER_LEAF))
    .groupBy("static_language", "yt_family", "yt_leaf")
    .agg(
        F.sum("allocated_views_4wk").alias("allocated_views_4wk"),
        F.count("*").alias("pooled_channel_count"),
    )
    .withColumn("channel_id", F.lit(None).cast("string"))
    .withColumn(
        "channel_name",
        F.concat(F.lit("Other ("), F.format_number("pooled_channel_count", 0), F.lit(" channels)")),
    )
    .withColumn("view_count_4wk", F.lit(None).cast("double"))
    .withColumn("allocation_weight", F.lit(None).cast("double"))
    .withColumn("raw_topic_categories", F.lit(None).cast("array<string>"))
    .withColumn("is_placement_override", F.lit(False))
    .withColumn("needs_manual_review", F.lit(False))
    .withColumn("channel_rank", F.lit(TOP_CHANNELS_PER_LEAF + 1))
    .withColumn("is_other_channel_pool", F.lit(True))
)
renderer_columns = renderer_top.columns
renderer_rows = (
    renderer_top.unionByName(renderer_other.select(*renderer_columns))
    .withColumnRenamed("static_language", "language_display")
    .withColumn("treemap_run_id", F.lit(RUN_ID))
)

if WRITE_DELTA_TABLES:
    _write_table(
        renderer_rows,
        TABLES["renderer_rows"],
        comment=(
            "Bounded local-renderer input: top classified languages, separate Undetermined, classified tail "
            "pooled as Other languages, and top 15 plus Other channels per leaf."
        ),
    )
    renderer_rows = spark.table(TABLES["renderer_rows"])


# COMMAND ----------
# Reconciliation and source/coverage QA.
source_qa = language_raw.agg(
    F.count("*").alias("rows"),
    F.countDistinct("channel_id").alias("distinct_channels"),
    F.sum(F.col("is_language_classified").cast("long")).alias("classified"),
    F.sum((F.col("channel_language") == F.lit("und")).cast("long")).alias("und"),
    F.countDistinct(F.when(F.col("channel_language") != F.lit("und"), F.col("channel_language"))).alias("classified_codes"),
    F.countDistinct("label_version").alias("label_versions"),
    F.max("label_version").alias("label_version"),
).first()
base_qa = channel_base.agg(
    F.count("*").alias("rows"),
    F.sum(F.col("has_current_snapshot").cast("long")).alias("current_snapshot"),
    F.sum(F.col("has_prior_snapshot").cast("long")).alias("prior_snapshot"),
    F.sum((F.col("has_current_snapshot") & F.col("has_prior_snapshot")).cast("long")).alias("both_snapshots"),
    F.sum(F.col("in_subscriber_cohort").cast("long")).alias("subscriber_cohort"),
    F.sum(F.col("has_valid_4wk_views").cast("long")).alias("valid_delta"),
    F.sum(F.col("has_positive_4wk_views").cast("long")).alias("positive_delta"),
    F.sum(F.col("has_invalid_negative_delta").cast("long")).alias("negative_delta"),
    F.sum(F.col("topic_row_present").cast("long")).alias("topic_row"),
    F.sum(F.col("has_nonempty_topic_categories").cast("long")).alias("nonempty_topics"),
    F.sum((F.col("language_mapping_status") == F.lit("unregistered_or_legacy")).cast("long")).alias("unregistered_channels"),
    F.countDistinct(
        F.when(F.col("language_mapping_status") == F.lit("unregistered_or_legacy"), F.col("channel_language"))
    ).alias("unregistered_codes"),
).first()
cohort_qa = cohort.agg(
    F.count("*").alias("channels"),
    F.sum(F.col("has_valid_4wk_views").cast("long")).alias("valid_delta"),
    F.sum(F.col("has_positive_4wk_views").cast("long")).alias("positive_delta"),
    F.sum(F.col("has_invalid_negative_delta").cast("long")).alias("negative_delta"),
    F.sum("view_count_4wk").alias("valid_view_total"),
).first()

raw_reconciliation = raw_allocations.groupBy("channel_id").agg(
    F.sum("allocation_weight").alias("weight_sum"),
    F.sum("allocated_views_4wk").alias("allocated_sum"),
    F.first("view_count_4wk", ignorenulls=True).alias("channel_views"),
)
display_reconciliation = display_allocations.groupBy("channel_id").agg(
    F.sum("allocation_weight").alias("weight_sum"),
    F.sum("allocated_views_4wk").alias("allocated_sum"),
    F.first("view_count_4wk", ignorenulls=True).alias("channel_views"),
)
raw_recon_qa = raw_reconciliation.agg(
    F.count("*").alias("channels"),
    F.max(F.abs(F.col("weight_sum") - F.lit(1.0))).alias("max_weight_delta"),
    F.max(
        F.when(F.col("channel_views").isNotNull(), F.abs(F.col("allocated_sum") - F.col("channel_views")))
    ).alias("max_allocation_delta"),
    F.sum("allocated_sum").alias("allocated_total"),
).first()
display_recon_qa = display_reconciliation.agg(
    F.count("*").alias("channels"),
    F.max(F.abs(F.col("weight_sum") - F.lit(1.0))).alias("max_weight_delta"),
    F.max(
        F.when(F.col("channel_views").isNotNull(), F.abs(F.col("allocated_sum") - F.col("channel_views")))
    ).alias("max_allocation_delta"),
    F.sum("allocated_sum").alias("allocated_total"),
).first()

aggregate_leaf_rows = language_family_leaf.count()
aggregate_display_languages = language_family_leaf.select("language_display").distinct().count()
interactive_rows_count = top15_channels_per_leaf.count()
interactive_leaf_rows = (
    top15_channels_per_leaf
    .select("language_display", "yt_family", "yt_leaf")
    .distinct()
    .count()
)
interactive_display_languages = top15_channels_per_leaf.select("language_display").distinct().count()

expected_total = float(cohort_qa["valid_view_total"] or 0.0)
raw_total = float(raw_recon_qa["allocated_total"] or 0.0)
display_total = float(display_recon_qa["allocated_total"] or 0.0)
total_tolerance = max(1.0, expected_total * 1e-12)

checks = {
    "source_rows": int(source_qa["rows"]) == 4_798_717,
    "source_one_row_per_channel": int(source_qa["rows"]) == int(source_qa["distinct_channels"]),
    "source_classified": int(source_qa["classified"]) == 4_642_010,
    "source_und": int(source_qa["und"]) == 156_707,
    "source_classified_codes": int(source_qa["classified_codes"]) == 566,
    "source_label_version": int(source_qa["label_versions"]) == 1 and source_qa["label_version"] == EXPECTED_LABEL_VERSION,
    "full_base_matches_source": int(base_qa["rows"]) == int(source_qa["rows"]),
    "current_snapshot_coverage": int(base_qa["current_snapshot"]) == 4_796_338,
    "prior_snapshot_coverage": int(base_qa["prior_snapshot"]) == 4_789_343,
    "both_snapshot_coverage": int(base_qa["both_snapshots"]) == 4_788_207,
    "subscriber_cohort_exact": int(base_qa["subscriber_cohort"]) == 4_786_690,
    "valid_delta_coverage": int(base_qa["valid_delta"]) == 4_597_248,
    "positive_delta_coverage": int(base_qa["positive_delta"]) == 4_351_147,
    "negative_delta_coverage": int(base_qa["negative_delta"]) == 190_959,
    "topic_row_coverage": int(base_qa["topic_row"]) == 4_795_956,
    "nonempty_topic_coverage": int(base_qa["nonempty_topics"]) == 4_526_985,
    "unregistered_codes_retained": int(base_qa["unregistered_codes"]) == 34,
    "unregistered_channels_retained": int(base_qa["unregistered_channels"]) == 135,
    "subscriber_floor_applied": int(cohort_qa["channels"]) == int(base_qa["subscriber_cohort"]),
    "raw_allocation_channel_coverage": int(raw_recon_qa["channels"]) == int(cohort_qa["channels"]),
    "display_allocation_channel_coverage": int(display_recon_qa["channels"]) == int(cohort_qa["channels"]),
    "raw_weight_per_channel": float(raw_recon_qa["max_weight_delta"] or 0.0) <= 1e-10,
    "display_weight_per_channel": float(display_recon_qa["max_weight_delta"] or 0.0) <= 1e-10,
    "raw_allocated_per_channel": float(raw_recon_qa["max_allocation_delta"] or 0.0) <= 1e-5,
    "display_allocated_per_channel": float(display_recon_qa["max_allocation_delta"] or 0.0) <= 1e-5,
    "raw_total_conservation": math.isclose(raw_total, expected_total, rel_tol=1e-12, abs_tol=total_tolerance),
    "display_total_conservation": math.isclose(display_total, expected_total, rel_tol=1e-12, abs_tol=total_tolerance),
    "interactive_leaf_coverage": interactive_leaf_rows == aggregate_leaf_rows,
    "interactive_language_coverage": interactive_display_languages == aggregate_display_languages,
    "negative_deltas_are_null": channel_base.where(
        F.col("has_invalid_negative_delta") & F.col("view_count_4wk").isNotNull()
    ).limit(1).count() == 0,
    "classified_codes_not_silently_other": channel_base.where(
        (F.col("channel_language") != F.lit("und")) & (F.col("language_display") == F.lit(OTHER_LANGUAGES))
    ).limit(1).count() == 0,
}

qa_values = {
    "language_source_rows": source_qa["rows"],
    "language_source_distinct_channels": source_qa["distinct_channels"],
    "language_source_classified": source_qa["classified"],
    "language_source_und": source_qa["und"],
    "language_source_classified_codes": source_qa["classified_codes"],
    "channels_with_current_snapshot": base_qa["current_snapshot"],
    "channels_with_prior_snapshot": base_qa["prior_snapshot"],
    "channels_with_both_snapshots": base_qa["both_snapshots"],
    "channels_in_subscriber_cohort": cohort_qa["channels"],
    "cohort_valid_4wk_delta": cohort_qa["valid_delta"],
    "cohort_positive_4wk_delta": cohort_qa["positive_delta"],
    "cohort_invalid_negative_delta": cohort_qa["negative_delta"],
    "channels_with_topic_row": base_qa["topic_row"],
    "channels_with_nonempty_topics": base_qa["nonempty_topics"],
    "unregistered_language_codes": base_qa["unregistered_codes"],
    "unregistered_language_channels": base_qa["unregistered_channels"],
    "raw_max_weight_delta": raw_recon_qa["max_weight_delta"],
    "raw_max_allocation_delta": raw_recon_qa["max_allocation_delta"],
    "display_max_weight_delta": display_recon_qa["max_weight_delta"],
    "display_max_allocation_delta": display_recon_qa["max_allocation_delta"],
    "valid_4wk_view_total": expected_total,
    "raw_allocated_view_total": raw_total,
    "display_allocated_view_total": display_total,
    "aggregate_language_family_leaf_rows": aggregate_leaf_rows,
    "interactive_rows": interactive_rows_count,
    "interactive_leaf_rows": interactive_leaf_rows,
    "interactive_display_languages": interactive_display_languages,
    "top_static_languages": ", ".join(top_language_names),
}
qa_rows = [
    (RUN_ID, metric, str(value), None, "metric")
    for metric, value in sorted(qa_values.items())
] + [
    (RUN_ID, name, str(bool(ok)).lower(), "PASS" if ok else "FAIL", "acceptance_check")
    for name, ok in sorted(checks.items())
]
qa_df = spark.createDataFrame(
    qa_rows,
    "treemap_run_id string, metric string, value string, status string, row_type string",
)
if WRITE_DELTA_TABLES:
    _write_table(qa_df, TABLES["qa"], comment="Full-corpus treemap source, coverage, and conservation QA.")

failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError("Full-corpus treemap QA failed: " + ", ".join(failed))

print("SOURCE LANGUAGE ROWS:", f"{int(source_qa['rows']):,}")
print("SOURCE CLASSIFIED CHANNELS:", f"{int(source_qa['classified']):,}")
print("SOURCE UND CHANNELS:", f"{int(source_qa['und']):,}")
print("SUBSCRIBER COHORT CHANNELS:", f"{int(cohort_qa['channels']):,}")
print("VALID 4WK DELTA CHANNELS:", f"{int(cohort_qa['valid_delta']):,}")
print("POSITIVE 4WK DELTA CHANNELS:", f"{int(cohort_qa['positive_delta']):,}")
print("INVALID NEGATIVE DELTAS:", f"{int(cohort_qa['negative_delta']):,}")
print("TOPIC ROW COVERAGE:", f"{int(base_qa['topic_row']):,} / {int(base_qa['rows']):,}")
print("NONEMPTY TOPIC COVERAGE:", f"{int(base_qa['nonempty_topics']):,} / {int(base_qa['rows']):,}")
print("UNREGISTERED LANGUAGE CODES RETAINED:", f"{int(base_qa['unregistered_codes']):,}")
print("UNREGISTERED LANGUAGE CHANNELS RETAINED:", f"{int(base_qa['unregistered_channels']):,}")
print("TOP STATIC CLASSIFIED LANGUAGES:", top_language_names)
print("RAW ALLOCATION MAX WEIGHT DELTA:", raw_recon_qa["max_weight_delta"])
print("DISPLAY ALLOCATION MAX WEIGHT DELTA:", display_recon_qa["max_weight_delta"])
print("CONSERVATION TOTAL 4WK VIEWS:", f"{display_total:,.0f}")
print("CONSERVATION: PASS")


# COMMAND ----------
# Export only bounded aggregates and renderer rows. Full channel data remains in Delta.
renderer_export = f"{ARTIFACT_DIR}/renderer_rows.parquet"
interactive_export = f"{ARTIFACT_DIR}/interactive_rows.parquet"
aggregate_export = f"{ARTIFACT_DIR}/language_family_leaf.parquet"
qa_export = f"{ARTIFACT_DIR}/qa.parquet"
renderer_rows.coalesce(1).write.mode("overwrite").parquet(renderer_export)
top15_channels_per_leaf.coalesce(1).write.mode("overwrite").parquet(interactive_export)
language_family_leaf.coalesce(1).write.mode("overwrite").parquet(aggregate_export)
qa_df.coalesce(1).write.mode("overwrite").parquet(qa_export)

manifest = {
    "run_id": RUN_ID,
    "source_tables": {
        "language": LANGUAGE_TABLE,
        "topics": TOPIC_TABLE,
        "traffic": STATS_TABLE,
    },
    "source_label_version": EXPECTED_LABEL_VERSION,
    "current_snapshot": CURRENT_SNAPSHOT,
    "prior_snapshot": PRIOR_SNAPSHOT,
    "minimum_subscribers": MINIMUM_SUBSCRIBERS,
    "top_k_languages": TOP_K_LANGUAGES,
    "top_channels_per_leaf": TOP_CHANNELS_PER_LEAF,
    "top_static_languages": top_language_names,
    "tables": TABLES,
    "exports": {
        "renderer_rows": renderer_export,
        "interactive_rows": interactive_export,
        "language_family_leaf": aggregate_export,
        "qa": qa_export,
    },
    "qa": {key: str(value) for key, value in qa_values.items()},
    "checks": checks,
}
manifest_path = f"{ARTIFACT_DIR}/run_manifest.json"
dbutils.fs.put(manifest_path, json.dumps(manifest, indent=2, sort_keys=True), True)

print("RENDERER ROWS:", renderer_rows.count())
print("INTERACTIVE ROWS:", interactive_rows_count)
print("INTERACTIVE DISPLAY LANGUAGES:", interactive_display_languages)
print("FULL LANGUAGE/FAMILY/LEAF ROWS:", aggregate_leaf_rows)
print("RENDERER EXPORT:", renderer_export)
print("INTERACTIVE EXPORT:", interactive_export)
print("AGGREGATE EXPORT:", aggregate_export)
print("QA EXPORT:", qa_export)
print("RUN MANIFEST:", manifest_path)
print("PACKING CONTRACT: squarify (renderer)")

try:
    dbutils.notebook.exit(json.dumps(manifest, sort_keys=True))
except Exception:
    pass
