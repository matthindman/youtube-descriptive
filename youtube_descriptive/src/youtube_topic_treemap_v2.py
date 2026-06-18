# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube topic treemap v2: hierarchy-aware topicCategories allocation
# MAGIC
# MAGIC This notebook builds a hierarchy-aware treemap from YouTube channel
# MAGIC `topicDetails.topicCategories[]` metadata. It treats topic categories as a
# MAGIC multi-label array, preserves unmapped labels, retains parent-only labels as
# MAGIC unspecified leaves, and allocates channel lifetime view mass without double
# MAGIC counting parent and child labels.
# MAGIC
# MAGIC The notebook deliberately does **not** classify channel content with an LLM and
# MAGIC does **not** construct a new content taxonomy. The hierarchy scaffold is read
# MAGIC from `config/youtube_topic_hierarchy_v2.yaml`.

# COMMAND ----------
# MAGIC %pip install pyyaml plotly pyarrow

# COMMAND ----------
# Restart after %pip installs in Databricks.
try:
    dbutils.library.restartPython()
except Exception:
    pass

# COMMAND ----------
import itertools
import json
import math
import os
import re
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import unquote

import pandas as pd
import yaml
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

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


def _safe_token(value: str, default: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "_", (value or "").strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token or default


def _quote_ident(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _fqtn(catalog: str, schema: str, table: str) -> str:
    return f"{_quote_ident(catalog)}.{_quote_ident(schema)}.{_quote_ident(table)}"


def _table_exists(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0).count()
        return True
    except Exception:
        return False


def _display_df(df: DataFrame, n: int = 20) -> None:
    display_func = globals().get("display")
    if callable(display_func):
        display_func(df)
    else:
        df.show(n, truncate=False)


def _first_col(df: DataFrame, candidates: Sequence[str], override: str = "") -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    if override:
        if override in df.columns:
            return override
        if override.lower() in lower:
            return lower[override.lower()]
        raise ValueError(f"Column override `{override}` was not found. Available columns: {df.columns}")
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def _choose_existing_table(candidates_csv: str) -> str:
    candidates = [c.strip() for c in candidates_csv.split(",") if c.strip()]
    for table in candidates:
        if _table_exists(table):
            return table
    raise ValueError(f"None of the candidate tables exists or is readable: {candidates}")


def _latest_per_key(df: DataFrame, key_col: str, date_col: Optional[str], tie_cols: Optional[List[F.Column]] = None) -> DataFrame:
    order_cols = []
    if date_col:
        order_cols.append(F.col(date_col).desc_nulls_last())
    if tie_cols:
        order_cols.extend(tie_cols)
    order_cols.append(F.col(key_col).asc_nulls_last())
    w = Window.partitionBy(key_col).orderBy(*order_cols)
    return df.withColumn("_treemap_rn", F.row_number().over(w)).where(F.col("_treemap_rn") == 1).drop("_treemap_rn")


def _spark_artifact_path(local_path: str) -> str:
    if local_path.startswith("/dbfs/"):
        return "dbfs:/" + local_path[len("/dbfs/"):]
    if local_path.startswith("/"):
        return "file:" + local_path
    return local_path


def _ensure_local_dir(path: str) -> None:
    if path.startswith("/dbfs/") or path.startswith("/") or not re.match(r"^[a-zA-Z]+:", path):
        os.makedirs(path, exist_ok=True)


def _candidate_local_paths(path: str) -> List[str]:
    candidates = []
    if path.startswith("file:"):
        candidates.append(path[len("file:"):])
    elif not re.match(r"^[a-zA-Z]+:", path):
        candidates.append(path)
        if not os.path.isabs(path):
            cwd = os.getcwd()
            candidates.append(os.path.join(cwd, path))
            current = cwd
            for _ in range(8):
                parent = os.path.dirname(current)
                if parent == current:
                    break
                candidates.append(os.path.join(parent, path))
                current = parent
    return list(dict.fromkeys(candidates))


def _read_text_path(path: str) -> str:
    for candidate in _candidate_local_paths(path):
        if os.path.exists(candidate):
            with open(candidate, "r") as fh:
                return fh.read()
    if path.startswith("dbfs:/"):
        try:
            return dbutils.fs.head(path, 10000000)
        except Exception as exc:
            raise FileNotFoundError(f"Could not read Databricks config path `{path}`: {exc}") from exc
    raise FileNotFoundError(
        f"Could not find hierarchy config `{path}`. Keep the canonical YAML at "
        "`config/youtube_topic_hierarchy_v2.yaml`, run from a checked-out repo, or set "
        "`hierarchy_config_path` to a readable local, /Workspace, /dbfs, or dbfs:/ path."
    )


# COMMAND ----------
# Parameters.
today_token = date.today().strftime("%Y%m%d")

_create_text_widget("run_date", today_token)
_create_text_widget("hierarchy_config_path", "config/youtube_topic_hierarchy_v2.yaml")

_create_text_widget("topic_table", "dev_sean.default.channel_category")
_create_text_widget("topic_channel_id_col", "")
_create_text_widget("topic_categories_col", "")

_create_text_widget(
    "metrics_table_candidates",
    "dev_sean.default.yt_channel_stats_full,prod_tads.youtube_too.yt_sl_channels_metrics",
)
_create_text_widget("metrics_channel_id_col", "")
_create_text_widget("metrics_views_col", "")
_create_text_widget("metrics_subscribers_col", "")
_create_text_widget("metrics_snapshot_date_col", "")
_create_text_widget("snapshot_date", "")
_create_text_widget("snapshot_min_completeness_fraction", "0.90")

_create_text_widget("channel_table", "prod_tads.youtube_too.yt_sl_channels")
_create_text_widget("channel_id_col", "")
_create_text_widget("channel_title_col", "")
_create_text_widget("channel_language_col", "")

_create_text_widget("language_table", "dev_sean.matt.yt_lid_v3_channels")
_create_text_widget("language_run_id", "default")

_create_text_widget("top_n_channels", "200000")
_create_text_widget("top_k_languages", "25")
_create_text_widget("top_n_channels_per_leaf", "10")
_create_text_widget("max_plot_rows", "20000")

_create_text_widget("output_catalog", "dev_sean")
_create_text_widget("output_schema", "matt")
_create_text_widget("write_delta_tables", "true")
_create_text_widget("artifact_dir", f"/dbfs/FileStore/youtube_topic_treemap_top_ocean_{today_token}")

RUN_DATE = _safe_token(_get_widget("run_date", today_token), today_token)
CONFIG_PATH = _get_widget("hierarchy_config_path", "config/youtube_topic_hierarchy_v2.yaml")
TOPIC_TABLE = _get_widget("topic_table", "dev_sean.default.channel_category")
TOP_N_CHANNELS = _get_int_widget("top_n_channels", 200000)
SNAPSHOT_DATE_OVERRIDE = _get_widget("snapshot_date", "").strip()
SNAPSHOT_MIN_COMPLETENESS = float(_get_widget("snapshot_min_completeness_fraction", "0.90"))
TOP_K_LANGUAGES_DEFAULT = _get_int_widget("top_k_languages", 25)
TOP_N_CHANNELS_PER_LEAF_DEFAULT = _get_int_widget("top_n_channels_per_leaf", 10)
MAX_PLOT_ROWS = _get_int_widget("max_plot_rows", 20000)
OUTPUT_CATALOG = _get_widget("output_catalog", "dev_sean")
OUTPUT_SCHEMA = _get_widget("output_schema", "matt")
WRITE_DELTA_TABLES = _get_bool_widget("write_delta_tables", True)
ARTIFACT_DIR = _get_widget("artifact_dir", f"/dbfs/FileStore/youtube_topic_treemap_top_ocean_{RUN_DATE}")
LANGUAGE_RUN_ID = _get_widget("language_run_id", "default")

_ensure_local_dir(ARTIFACT_DIR)
ARTIFACT_SPARK_DIR = _spark_artifact_path(ARTIFACT_DIR)

print("RUN_DATE:", RUN_DATE)
print("ARTIFACT_DIR:", ARTIFACT_DIR)
print("TOPIC_TABLE:", TOPIC_TABLE)

# COMMAND ----------
# Load hierarchy config and derive mapping tables.
HIERARCHY_CONFIG = yaml.safe_load(_read_text_path(CONFIG_PATH))

FAMILIES = HIERARCHY_CONFIG.get("families", {})
ALIASES = HIERARCHY_CONFIG.get("aliases", {})
UNMAPPED_FAMILY = "Other / Unmapped YouTube topic"
UNLABELED_FAMILY = "Unlabeled"
UNLABELED_LEAF = "No YouTube topicCategories"


def _canonical_slug(slug: str) -> str:
    return ALIASES.get(slug, slug)


parent_map: Dict[str, Dict[str, str]] = {}
child_map: Dict[str, Dict[str, str]] = {}
family_parent_slugs: Dict[str, set] = {}
node_map_rows = []

for family_name, spec in FAMILIES.items():
    parent_slugs = set()
    for raw_parent in spec.get("parent_slugs") or []:
        parent = _canonical_slug(str(raw_parent))
        parent_slugs.add(parent)
        if parent not in parent_map:
            parent_map[parent] = {
                "family": family_name,
                "leaf": f"[{family_name}] - unspecified",
                "node_type": "parent",
            }
            node_map_rows.append((parent, family_name, f"[{family_name}] - unspecified", "parent", "config"))
    family_parent_slugs[family_name] = parent_slugs
    for raw_child, display in (spec.get("children") or {}).items():
        child = _canonical_slug(str(raw_child))
        if child not in child_map:
            child_map[child] = {
                "family": family_name,
                "leaf": str(display),
                "node_type": "child",
            }
            node_map_rows.append((child, family_name, str(display), "child", "config"))

node_map_rows = sorted(set(node_map_rows))
topic_node_map = spark.createDataFrame(
    node_map_rows,
    "canonical_slug string, yt_family string, yt_leaf string, node_type string, mapping_source string",
)
topic_node_map = topic_node_map.withColumn("run_date", F.lit(RUN_DATE))

aliases_df = spark.createDataFrame(
    sorted((str(k), str(v)) for k, v in ALIASES.items()),
    "raw_slug string, canonical_slug string",
).withColumn("run_date", F.lit(RUN_DATE))

display_parent_map = spark.createDataFrame(
    [(fam, sorted(list(slugs))) for fam, slugs in family_parent_slugs.items()],
    "yt_family string, parent_slugs array<string>",
)

print(f"Configured topic nodes: parents={len(parent_map)}, children={len(child_map)}, aliases={len(ALIASES)}")

# COMMAND ----------
if not _table_exists(TOPIC_TABLE):
    raise ValueError(f"Topic table is missing or unreadable: {TOPIC_TABLE}")

# Resolve source tables and columns.
metrics_table = _choose_existing_table(_get_widget("metrics_table_candidates", ""))
topic_df_raw = spark.table(TOPIC_TABLE)
metrics_df_raw = spark.table(metrics_table)

topic_key_col = _first_col(topic_df_raw, ["channel_id", "canonical_id", "channel"], _get_widget("topic_channel_id_col", ""))
topic_col = _first_col(topic_df_raw, ["topic_categories", "topicDetails.topicCategories", "topic_categories_json"], _get_widget("topic_categories_col", ""))
if not topic_key_col or not topic_col:
    raise ValueError(
        f"Could not resolve topic key/category columns in {TOPIC_TABLE}. "
        f"key={topic_key_col}, topic_col={topic_col}, columns={topic_df_raw.columns}"
    )

metrics_key_col = _first_col(metrics_df_raw, ["channel_id", "canonical_id", "channel"], _get_widget("metrics_channel_id_col", ""))
views_col = _first_col(
    metrics_df_raw,
    [
        "total_view_count",
        "view_count",
        "views_count",
        "views",
        "viewCount",
        "statistics_viewCount",
        "channel_view_count",
        "lifetime_views",
        "view_count_lifetime",
        "views_count_channel",
    ],
    _get_widget("metrics_views_col", ""),
)
subs_col = _first_col(
    metrics_df_raw,
    ["subscriber_count", "subscribers", "subscriberCount", "statistics_subscriberCount", "subs"],
    _get_widget("metrics_subscribers_col", ""),
)
snapshot_col = _first_col(
    metrics_df_raw,
    ["snapshot_date", "capture_date", "collected_date", "collected_at", "as_of_date", "date", "ingestion_timestamp"],
    _get_widget("metrics_snapshot_date_col", ""),
)
if not metrics_key_col or not views_col:
    raise ValueError(
        f"Could not resolve metric key/views columns in {metrics_table}. "
        f"key={metrics_key_col}, views={views_col}, columns={metrics_df_raw.columns}"
    )

channel_table = _get_widget("channel_table", "prod_tads.youtube_too.yt_sl_channels")
channel_df_raw = spark.table(channel_table) if _table_exists(channel_table) else None
channel_key_col = channel_title_col = channel_lang_col = channel_date_col = None
if channel_df_raw is not None:
    channel_key_col = _first_col(channel_df_raw, ["channel_id", "canonical_id", "channel"], _get_widget("channel_id_col", ""))
    channel_title_col = _first_col(channel_df_raw, ["channel_title", "channel_name", "title", "name"], _get_widget("channel_title_col", ""))
    channel_lang_col = _first_col(channel_df_raw, ["language_code", "detected_language", "source_language_code"], _get_widget("channel_language_col", ""))
    channel_date_col = _first_col(channel_df_raw, ["capture_date", "last_ingestion_timestamp", "updated_at", "ingestion_timestamp"])

language_table = _get_widget("language_table", "dev_sean.matt.yt_lid_v3_channels")
language_df_raw = spark.table(language_table) if _table_exists(language_table) else None

print("Resolved source columns:")
print("  topic:", {"table": TOPIC_TABLE, "key": topic_key_col, "topic_categories": topic_col})
print("  metrics:", {"table": metrics_table, "key": metrics_key_col, "views": views_col, "subscribers": subs_col, "snapshot_date": snapshot_col})
print("  channel:", {"table": channel_table, "key": channel_key_col, "title": channel_title_col, "language": channel_lang_col})
print("  language:", {"table": language_table, "run_id": LANGUAGE_RUN_ID})
print("Topic schema:", topic_df_raw.schema.simpleString())
print("Metrics schema:", metrics_df_raw.schema.simpleString())
if channel_df_raw is not None:
    print("Channel schema:", channel_df_raw.schema.simpleString())
if language_df_raw is not None:
    print("Language schema:", language_df_raw.schema.simpleString())

# COMMAND ----------
# Source extraction.
date_candidates = [
    "snapshot_date", "capture_date", "collected_date", "as_of_date", "date",
    "updated_at", "ingestion_timestamp", "last_ingestion_timestamp",
    "collected_at",
]

metric_select = [
    F.col(metrics_key_col).cast("string").alias("channel_id"),
    F.col(views_col).cast("double").alias("latest_views"),
]
if subs_col:
    metric_select.append(F.col(subs_col).cast("double").alias("subscriber_count"))
else:
    metric_select.append(F.lit(None).cast("double").alias("subscriber_count"))
if snapshot_col:
    metric_select.append(F.to_date(F.col(snapshot_col)).alias("snapshot_date"))
else:
    metric_select.append(F.lit(None).cast("date").alias("snapshot_date"))

metrics_base = (
    metrics_df_raw
    .select(*metric_select)
    .where(F.col("channel_id").isNotNull() & F.col("latest_views").isNotNull())
)
chosen_snapshot_date = None
if snapshot_col:
    if SNAPSHOT_DATE_OVERRIDE:
        chosen_snapshot_date = SNAPSHOT_DATE_OVERRIDE
        metrics_base = metrics_base.where(F.col("snapshot_date") == F.to_date(F.lit(chosen_snapshot_date)))
        print(f"Using manual metrics snapshot_date={chosen_snapshot_date}")
    else:
        date_counts = (
            metrics_base.where(F.col("snapshot_date").isNotNull())
            .groupBy("snapshot_date")
            .agg(F.count("*").alias("n_rows"))
            .orderBy(F.col("snapshot_date").desc())
            .collect()
        )
        if not date_counts:
            raise ValueError(f"Metrics snapshot column `{snapshot_col}` resolved, but no non-null snapshot dates were found.")
        max_n = max(int(r["n_rows"]) for r in date_counts)
        min_n = int(math.ceil(max_n * SNAPSHOT_MIN_COMPLETENESS))
        eligible = [r for r in date_counts if int(r["n_rows"]) >= min_n]
        chosen_snapshot_date = str(max(r["snapshot_date"] for r in eligible))
        print(
            f"Using auto metrics snapshot_date={chosen_snapshot_date} "
            f"(min completeness rows={min_n:,}, max rows={max_n:,})"
        )
        metrics_base = metrics_base.where(F.col("snapshot_date") == F.to_date(F.lit(chosen_snapshot_date)))

metrics_latest = _latest_per_key(metrics_base, "channel_id", None, [F.col("latest_views").desc_nulls_last()])

top_order = [
    F.col("subscriber_count").desc_nulls_last(),
    F.col("latest_views").desc_nulls_last(),
    F.col("channel_id").asc(),
]
top_channels = metrics_latest.orderBy(*top_order)
if TOP_N_CHANNELS > 0:
    top_channels = top_channels.limit(TOP_N_CHANNELS)
top_channels = top_channels.cache()

topic_status_col = _first_col(topic_df_raw, ["status", "backfill_status"])
topic_timestamp_col = _first_col(topic_df_raw, ["collected_at", "updated_at", "ingestion_timestamp", "last_ingestion_timestamp"])
topic_date_col = _first_col(topic_df_raw, ["collected_date", "capture_date", "snapshot_date", "as_of_date", "date"])
topic_type = dict((f.name, f.dataType) for f in topic_df_raw.schema.fields).get(topic_col)

if isinstance(topic_type, ArrayType):
    topic_array_expr = F.col(topic_col).cast("array<string>")
else:
    topic_array_expr = F.from_json(F.col(topic_col).cast("string"), "array<string>")

topic_base = topic_df_raw
if topic_status_col:
    topic_base = topic_base.where(F.lower(F.trim(F.col(topic_status_col).cast("string"))).isin("done", "complete", "completed", "success", "succeeded"))

topic_select = [
    F.col(topic_key_col).cast("string").alias("channel_id"),
    topic_array_expr.alias("raw_topic_categories"),
]
if topic_date_col:
    topic_select.append(F.to_date(F.col(topic_date_col)).alias("_topic_date"))
else:
    topic_select.append(F.lit(None).cast("date").alias("_topic_date"))
if topic_timestamp_col:
    topic_select.append(F.to_timestamp(F.col(topic_timestamp_col)).alias("_topic_timestamp"))
else:
    topic_select.append(F.lit(None).cast("timestamp").alias("_topic_timestamp"))
topic_latest = (
    topic_base
    .select(*topic_select)
    .where(F.col("channel_id").isNotNull())
)
topic_latest = _latest_per_key(
    topic_latest,
    "channel_id",
    None,
    [F.col("_topic_timestamp").desc_nulls_last(), F.col("_topic_date").desc_nulls_last()],
).drop("_topic_date", "_topic_timestamp")

channel_dim = None
if channel_df_raw is not None and channel_key_col:
    channel_select = [F.col(channel_key_col).cast("string").alias("channel_id")]
    if channel_title_col:
        channel_select.append(F.col(channel_title_col).cast("string").alias("channel_title"))
    else:
        channel_select.append(F.lit(None).cast("string").alias("channel_title"))
    if channel_lang_col:
        channel_select.append(F.col(channel_lang_col).cast("string").alias("source_language_code"))
    else:
        channel_select.append(F.lit(None).cast("string").alias("source_language_code"))
    if channel_date_col:
        channel_select.append(F.to_timestamp(F.col(channel_date_col)).alias("_channel_date"))
    else:
        channel_select.append(F.lit(None).cast("timestamp").alias("_channel_date"))
    channel_dim = _latest_per_key(
        channel_df_raw.select(*channel_select).where(F.col("channel_id").isNotNull()),
        "channel_id",
        "_channel_date",
    ).drop("_channel_date")

language_dim = None
if language_df_raw is not None:
    lang_key = _first_col(language_df_raw, ["channel_id", "canonical_id", "channel"])
    if lang_key:
        lang = language_df_raw
        language_run_id_used = LANGUAGE_RUN_ID
        lang_run_col = _first_col(lang, ["run_id"])
        if lang_run_col:
            present_run_ids = [
                str(r[lang_run_col])
                for r in lang.select(lang_run_col).distinct().collect()
                if r[lang_run_col] is not None
            ]
            if language_run_id_used and language_run_id_used not in present_run_ids:
                fallback = max(present_run_ids) if present_run_ids else ""
                print(
                    f"WARNING: language_run_id={language_run_id_used!r} not found in {language_table}; "
                    f"falling back to {fallback!r}."
                )
                language_run_id_used = fallback
            elif not language_run_id_used:
                language_run_id_used = max(present_run_ids) if present_run_ids else ""
            if language_run_id_used:
                lang = lang.where(F.col(lang_run_col) == F.lit(language_run_id_used))
        lang_label = _first_col(lang, ["consensus_for_rollup_label", "consensus_language_label", "primary_language_label"])
        lang_iso = _first_col(lang, ["consensus_language_iso639_3", "primary_language_iso639_3"])
        language_dim = lang.select(
            F.col(lang_key).cast("string").alias("channel_id"),
            (F.col(lang_label).cast("string") if lang_label else F.lit(None).cast("string")).alias("language_label"),
            (F.col(lang_iso).cast("string") if lang_iso else F.lit(None).cast("string")).alias("language_iso"),
        ).dropDuplicates(["channel_id"])

source = top_channels
if channel_dim is not None:
    source = source.join(channel_dim, on="channel_id", how="left")
else:
    source = source.withColumn("channel_title", F.lit(None).cast("string")).withColumn("source_language_code", F.lit(None).cast("string"))

if language_dim is not None:
    source = source.join(language_dim, on="channel_id", how="left")
else:
    source = source.withColumn("language_label", F.lit(None).cast("string")).withColumn("language_iso", F.lit(None).cast("string"))

source = (
    source
    .join(topic_latest, on="channel_id", how="left")
    .withColumn("channel_title", F.coalesce(F.col("channel_title"), F.col("channel_id")))
    .withColumn("language_code", F.coalesce(F.col("language_label"), F.col("language_iso"), F.col("source_language_code"), F.lit("und")))
    .withColumn("raw_topic_categories", F.coalesce(F.col("raw_topic_categories"), F.array().cast("array<string>")))
    .cache()
)

print(f"Candidate source channels: {source.count():,}")

# COMMAND ----------
# Projection UDF.
slug_item_schema = StructType([
    StructField("raw_url", StringType(), True),
    StructField("raw_slug", StringType(), True),
    StructField("canonical_slug", StringType(), True),
])
mapped_node_schema = StructType([
    StructField("raw_slug", StringType(), True),
    StructField("canonical_slug", StringType(), True),
    StructField("yt_family", StringType(), True),
    StructField("yt_leaf", StringType(), True),
    StructField("node_type", StringType(), True),
    StructField("mapping_status", StringType(), True),
    StructField("broken_closure_imputed", BooleanType(), True),
])
display_item_schema = StructType([
    StructField("yt_family", StringType(), True),
    StructField("yt_leaf", StringType(), True),
    StructField("leaf_slug", StringType(), True),
    StructField("node_type", StringType(), True),
    StructField("source_canonical_slug", StringType(), True),
    StructField("is_unmapped", BooleanType(), True),
    StructField("is_parent_unspecified", BooleanType(), True),
    StructField("is_unlabeled", BooleanType(), True),
])
projection_schema = StructType([
    StructField("slug_items", ArrayType(slug_item_schema), True),
    StructField("normalized_slugs", ArrayType(StringType()), True),
    StructField("canonical_slugs", ArrayType(StringType()), True),
    StructField("mapped_nodes", ArrayType(mapped_node_schema), True),
    StructField("display_items", ArrayType(display_item_schema), True),
    StructField("display_families", ArrayType(StringType()), True),
    StructField("display_leaves", ArrayType(StringType()), True),
    StructField("n_raw_labels", IntegerType(), True),
    StructField("n_canonical_labels", IntegerType(), True),
    StructField("n_display_families", IntegerType(), True),
    StructField("n_display_leaves", IntegerType(), True),
    StructField("has_no_topic_categories", BooleanType(), True),
    StructField("has_unmapped_labels", BooleanType(), True),
    StructField("has_parent_only_label", BooleanType(), True),
    StructField("has_same_family_multi_child", BooleanType(), True),
    StructField("has_cross_family_labels", BooleanType(), True),
    StructField("broken_closure_imputed", BooleanType(), True),
    StructField("projection_notes", ArrayType(StringType()), True),
])

family_parent_slugs_serializable = {k: sorted(v) for k, v in family_parent_slugs.items()}


def _normalize_topic_slug(raw_value: Optional[str]) -> str:
    if raw_value is None:
        return ""
    text = str(raw_value).strip()
    if not text:
        return ""
    text = text.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    text = text.rsplit("/", 1)[-1]
    text = unquote(text)
    text = text.replace(" ", "_").strip().lower()
    return text


def _leaf_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")
    return slug or "unknown"


@F.udf(projection_schema)
def project_topics(raw_urls: Optional[List[str]]) -> dict:
    pmap = parent_map
    cmap = child_map
    aliases = ALIASES
    family_parents = family_parent_slugs_serializable

    seen_raw = set()
    slug_items = []
    for raw in raw_urls or []:
        raw_slug = _normalize_topic_slug(raw)
        if not raw_slug or raw_slug in seen_raw:
            continue
        seen_raw.add(raw_slug)
        canonical = aliases.get(raw_slug, raw_slug)
        slug_items.append({"raw_url": str(raw), "raw_slug": raw_slug, "canonical_slug": canonical})

    canonical_slugs = []
    seen_canonical = set()
    for item in slug_items:
        canonical = item["canonical_slug"]
        if canonical not in seen_canonical:
            seen_canonical.add(canonical)
            canonical_slugs.append(canonical)

    child_families = {}
    parent_families = {}
    unknown_slugs = []
    mapped_nodes = []

    for item in slug_items:
        raw_slug = item["raw_slug"]
        canonical = item["canonical_slug"]
        if canonical in cmap:
            node = cmap[canonical]
            child_families.setdefault(node["family"], set()).add(canonical)
            mapped_nodes.append({
                "raw_slug": raw_slug,
                "canonical_slug": canonical,
                "yt_family": node["family"],
                "yt_leaf": node["leaf"],
                "node_type": "child",
                "mapping_status": "mapped",
                "broken_closure_imputed": False,
            })
        elif canonical in pmap:
            node = pmap[canonical]
            parent_families.setdefault(node["family"], set()).add(canonical)
            mapped_nodes.append({
                "raw_slug": raw_slug,
                "canonical_slug": canonical,
                "yt_family": node["family"],
                "yt_leaf": node["leaf"],
                "node_type": "parent",
                "mapping_status": "mapped",
                "broken_closure_imputed": False,
            })
        else:
            unknown_slugs.append(canonical)
            mapped_nodes.append({
                "raw_slug": raw_slug,
                "canonical_slug": canonical,
                "yt_family": UNMAPPED_FAMILY,
                "yt_leaf": f"Unmapped: {canonical}",
                "node_type": "unmapped",
                "mapping_status": "unmapped",
                "broken_closure_imputed": False,
            })

    broken_families = set()
    for family, children in child_families.items():
        parents_for_family = set(family_parents.get(family, []))
        if children and not (parents_for_family & set(canonical_slugs)):
            broken_families.add(family)

    if broken_families:
        for node in mapped_nodes:
            if node["node_type"] == "child" and node["yt_family"] in broken_families:
                node["broken_closure_imputed"] = True

    display_items = []
    display_seen = set()

    def add_display(family, leaf, leaf_key, node_type, source_slug, unmapped=False, parent_unspecified=False, unlabeled=False):
        key = (family, leaf, source_slug if unmapped else "")
        if key in display_seen:
            return
        display_seen.add(key)
        display_items.append({
            "yt_family": family,
            "yt_leaf": leaf,
            "leaf_slug": leaf_key,
            "node_type": node_type,
            "source_canonical_slug": source_slug,
            "is_unmapped": bool(unmapped),
            "is_parent_unspecified": bool(parent_unspecified),
            "is_unlabeled": bool(unlabeled),
        })

    if not canonical_slugs:
        add_display(UNLABELED_FAMILY, UNLABELED_LEAF, "no_youtube_topiccategories", "unlabeled", "", unlabeled=True)
    else:
        for family in sorted(child_families):
            for child_slug in sorted(child_families[family]):
                node = cmap[child_slug]
                add_display(family, node["leaf"], child_slug, "child", child_slug)
        for family in sorted(parent_families):
            if family not in child_families:
                leaf = f"[{family}] - unspecified"
                source_slug = sorted(parent_families[family])[0]
                add_display(family, leaf, f"{_leaf_slug(family)}_unspecified", "parent_unspecified", source_slug, parent_unspecified=True)
        for unknown in sorted(set(unknown_slugs)):
            add_display(UNMAPPED_FAMILY, f"Unmapped: {unknown}", f"unmapped_{unknown}", "unmapped", unknown, unmapped=True)

    family_child_counts = {family: len(children) for family, children in child_families.items()}
    display_families = sorted(set(item["yt_family"] for item in display_items))
    display_leaves = sorted(set(item["yt_leaf"] for item in display_items))

    notes = []
    if not canonical_slugs:
        notes.append("no_topic_categories")
    if unknown_slugs:
        notes.append("unmapped_labels")
    parent_only = any(family not in child_families for family in parent_families)
    if parent_only:
        notes.append("parent_only_label")
    if any(v > 1 for v in family_child_counts.values()):
        notes.append("same_family_multi_child")
    if len(display_families) > 1:
        notes.append("cross_family_labels")
    if broken_families:
        notes.append("broken_closure_imputed")

    return {
        "slug_items": slug_items,
        "normalized_slugs": [item["raw_slug"] for item in slug_items],
        "canonical_slugs": canonical_slugs,
        "mapped_nodes": mapped_nodes,
        "display_items": display_items,
        "display_families": display_families,
        "display_leaves": display_leaves,
        "n_raw_labels": len(slug_items),
        "n_canonical_labels": len(canonical_slugs),
        "n_display_families": len(display_families),
        "n_display_leaves": len(display_leaves),
        "has_no_topic_categories": len(canonical_slugs) == 0,
        "has_unmapped_labels": bool(unknown_slugs),
        "has_parent_only_label": parent_only,
        "has_same_family_multi_child": any(v > 1 for v in family_child_counts.values()),
        "has_cross_family_labels": len(display_families) > 1,
        "broken_closure_imputed": bool(broken_families),
        "projection_notes": notes,
    }


projection = (
    source
    .withColumn("_projection", project_topics(F.col("raw_topic_categories")))
    .select(
        "channel_id",
        "channel_title",
        "language_code",
        "latest_views",
        "subscriber_count",
        "snapshot_date",
        "raw_topic_categories",
        F.col("_projection.slug_items").alias("slug_items"),
        F.col("_projection.normalized_slugs").alias("normalized_slugs"),
        F.col("_projection.canonical_slugs").alias("canonical_slugs"),
        F.col("_projection.mapped_nodes").alias("mapped_nodes"),
        F.col("_projection.display_items").alias("display_items"),
        F.col("_projection.display_families").alias("display_families"),
        F.col("_projection.display_leaves").alias("display_leaves"),
        F.col("_projection.n_raw_labels").alias("n_raw_labels"),
        F.col("_projection.n_canonical_labels").alias("n_canonical_labels"),
        F.col("_projection.n_display_families").alias("n_display_families"),
        F.col("_projection.n_display_leaves").alias("n_display_leaves"),
        F.col("_projection.has_no_topic_categories").alias("has_no_topic_categories"),
        F.col("_projection.has_unmapped_labels").alias("has_unmapped_labels"),
        F.col("_projection.has_parent_only_label").alias("has_parent_only_label"),
        F.col("_projection.has_same_family_multi_child").alias("has_same_family_multi_child"),
        F.col("_projection.has_cross_family_labels").alias("has_cross_family_labels"),
        F.col("_projection.broken_closure_imputed").alias("broken_closure_imputed"),
        F.col("_projection.projection_notes").alias("projection_notes"),
    )
    .cache()
)

projection_count = projection.count()
print(f"Projected channels: {projection_count:,}")
if projection_count <= 150000:
    print(
        "WARNING: CHANNELS PROCESSED is <=150,000. This may be an intentional smoke run or an undersized "
        "top-of-ocean input; do not treat it as the full requested treemap without checking source coverage."
    )

# COMMAND ----------
# Build allocation rows.
candidate_leaves = (
    projection
    .select(
        "channel_id", "channel_title", "language_code", "latest_views", "snapshot_date",
        "raw_topic_categories", "normalized_slugs", "canonical_slugs", "display_families", "display_leaves",
        "has_no_topic_categories", "has_unmapped_labels", "has_parent_only_label",
        "has_same_family_multi_child", "has_cross_family_labels", "broken_closure_imputed",
        F.explode("display_items").alias("di"),
    )
    .select(
        "channel_id", "channel_title", "language_code", "latest_views", "snapshot_date",
        "raw_topic_categories", "normalized_slugs", "canonical_slugs", "display_families", "display_leaves",
        "has_no_topic_categories", "has_unmapped_labels", "has_parent_only_label",
        "has_same_family_multi_child", "has_cross_family_labels", "broken_closure_imputed",
        F.col("di.yt_family").alias("yt_family"),
        F.col("di.yt_leaf").alias("yt_leaf"),
        F.col("di.leaf_slug").alias("leaf_slug"),
        F.col("di.node_type").alias("node_type"),
        F.col("di.source_canonical_slug").alias("source_canonical_slug"),
        F.col("di.is_unmapped").alias("is_unmapped"),
        F.col("di.is_parent_unspecified").alias("is_parent_unspecified"),
        F.col("di.is_unlabeled").alias("is_unlabeled"),
    )
    .cache()
)

family_counts = candidate_leaves.select("channel_id", "yt_family").distinct().groupBy("channel_id").agg(F.count("*").alias("n_families"))
family_leaf_counts = candidate_leaves.groupBy("channel_id", "yt_family").agg(F.count("*").alias("n_family_leaves"))
leaf_counts = candidate_leaves.groupBy("channel_id").agg(F.count("*").alias("n_leaves"))

alloc_base = (
    candidate_leaves
    .join(family_counts, on="channel_id", how="left")
    .join(family_leaf_counts, on=["channel_id", "yt_family"], how="left")
    .join(leaf_counts, on="channel_id", how="left")
)

family_balanced = alloc_base.withColumn(
    "allocation_method", F.lit("family_balanced")
).withColumn(
    "allocation_weight", F.lit(1.0) / F.col("n_families") / F.col("n_family_leaves")
)

equal_leaf = alloc_base.withColumn(
    "allocation_method", F.lit("equal_leaf")
).withColumn(
    "allocation_weight", F.lit(1.0) / F.col("n_leaves")
)

equal_raw = alloc_base.withColumn(
    "allocation_method", F.lit("equal_raw_label_after_parent_prune")
).withColumn(
    "allocation_weight", F.lit(1.0) / F.col("n_leaves")
)

specificity_base = alloc_base.withColumn(
    "_specificity_weight",
    F.when(F.col("is_parent_unspecified"), F.lit(0.5)).otherwise(F.lit(1.0)),
)
specificity_sum = specificity_base.groupBy("channel_id").agg(F.sum("_specificity_weight").alias("_specificity_weight_sum"))
specificity_weighted = (
    specificity_base.join(specificity_sum, on="channel_id", how="left")
    .withColumn("allocation_method", F.lit("specificity_weighted"))
    .withColumn("allocation_weight", F.col("_specificity_weight") / F.col("_specificity_weight_sum"))
    .drop("_specificity_weight", "_specificity_weight_sum")
)

family_global_views = (
    family_balanced
    .withColumn("allocated_views", F.col("latest_views") * F.col("allocation_weight"))
    .groupBy("yt_family")
    .agg(F.sum("allocated_views").alias("_global_family_allocated_views"))
)
dominant_priority = (
    alloc_base
    .join(family_global_views, on="yt_family", how="left")
    .withColumn(
        "_node_priority",
        F.when(F.col("node_type") == F.lit("child"), F.lit(0))
        .when(F.col("node_type") == F.lit("parent_unspecified"), F.lit(1))
        .when(F.col("node_type") == F.lit("unlabeled"), F.lit(2))
        .when(F.col("node_type") == F.lit("unmapped"), F.lit(3))
        .otherwise(F.lit(4)),
    )
)
dominant_window = Window.partitionBy("channel_id").orderBy(
    F.col("_node_priority").asc(),
    F.col("_global_family_allocated_views").desc_nulls_last(),
    F.col("yt_family").asc(),
    F.col("yt_leaf").asc(),
    F.col("source_canonical_slug").asc(),
)
dominant_display = (
    dominant_priority
    .withColumn("_dominant_rank", F.row_number().over(dominant_window))
    .where(F.col("_dominant_rank") == 1)
    .drop("_dominant_rank", "_node_priority", "_global_family_allocated_views")
    .withColumn("allocation_method", F.lit("dominant_display"))
    .withColumn("allocation_weight", F.lit(1.0))
)

allocation_columns = [
    "snapshot_date", "channel_id", "channel_title", "language_code", "latest_views",
    "allocation_method", "yt_family", "yt_leaf", "leaf_slug", "allocation_weight",
    "raw_topic_categories", "normalized_slugs", "canonical_slugs", "display_families", "display_leaves",
    "has_no_topic_categories", "has_unmapped_labels", "has_parent_only_label",
    "has_same_family_multi_child", "has_cross_family_labels", "broken_closure_imputed",
    "node_type", "source_canonical_slug", "is_unmapped", "is_parent_unspecified", "is_unlabeled",
]

allocations = (
    family_balanced.select(*allocation_columns)
    .unionByName(equal_leaf.select(*allocation_columns))
    .unionByName(equal_raw.select(*allocation_columns))
    .unionByName(specificity_weighted.select(*allocation_columns))
    .unionByName(dominant_display.select(*allocation_columns))
    .withColumn("allocated_views", F.col("latest_views") * F.col("allocation_weight"))
    .cache()
)

allocation_methods = [r["allocation_method"] for r in allocations.select("allocation_method").distinct().collect()]
print("Allocation methods:", allocation_methods)
print(f"Allocation rows: {allocations.count():,}")

# COMMAND ----------
# Reconciliation checks.
total_latest_views = float(projection.agg(F.sum("latest_views").alias("v")).first()["v"] or 0.0)
latest_snapshot_date = projection.agg(F.max("snapshot_date").alias("d")).first()["d"]

projection_duplicate_channels = projection.groupBy("channel_id").count().where(F.col("count") != 1)
duplicate_projection_count = projection_duplicate_channels.count()

method_channel_counts = (
    allocations
    .groupBy("allocation_method")
    .agg(F.countDistinct("channel_id").alias("n_channels"))
    .collect()
)
missing_method_counts = {
    r["allocation_method"]: projection_count - int(r["n_channels"])
    for r in method_channel_counts
}

weight_check = (
    allocations
    .groupBy("channel_id", "allocation_method")
    .agg(
        F.sum("allocation_weight").alias("sum_weight"),
        F.first("latest_views").alias("latest_views"),
        F.sum("allocated_views").alias("sum_allocated_views"),
    )
    .withColumn("weight_error", F.abs(F.col("sum_weight") - F.lit(1.0)))
    .withColumn("view_error", F.abs(F.col("sum_allocated_views") - F.col("latest_views")))
    .withColumn("view_tolerance", F.greatest(F.lit(1e-6), F.lit(1e-9) * F.abs(F.col("latest_views"))))
)
bad_weights = weight_check.where(F.col("weight_error") > F.lit(1e-9))
bad_views = weight_check.where(F.col("view_error") > F.col("view_tolerance"))

method_totals = (
    allocations.groupBy("allocation_method")
    .agg(F.sum("allocated_views").alias("method_allocated_views"))
    .withColumn("relative_error", F.abs(F.col("method_allocated_views") - F.lit(total_latest_views)) / F.lit(total_latest_views if total_latest_views else 1.0))
)
bad_method_totals = method_totals.where(F.col("relative_error") > F.lit(1e-9))

reconciliation_pass = (
    duplicate_projection_count == 0
    and all(v == 0 for v in missing_method_counts.values())
    and bad_weights.count() == 0
    and bad_views.count() == 0
    and bad_method_totals.count() == 0
)

if not reconciliation_pass:
    print("RECONCILIATION: FAIL")
    print("Duplicate projection channels:", duplicate_projection_count)
    print("Missing channels by method:", missing_method_counts)
    print("Worst weight errors:")
    _display_df(bad_weights.orderBy(F.desc("weight_error")).limit(20), n=20)
    print("Worst view errors:")
    _display_df(bad_views.orderBy(F.desc("view_error")).limit(20), n=20)
    print("Bad method totals:")
    _display_df(bad_method_totals.orderBy(F.desc("relative_error")), n=20)
    raise AssertionError("Treemap allocation reconciliation failed.")

print("RECONCILIATION: PASS")

# COMMAND ----------
# Slug inventory and co-label diagnostics.
slug_inventory = (
    projection
    .select("channel_id", "latest_views", F.explode_outer("slug_items").alias("slug"))
    .where(F.col("slug.raw_slug").isNotNull())
    .groupBy(F.col("slug.raw_slug").alias("raw_slug"), F.col("slug.canonical_slug").alias("canonical_slug"))
    .agg(
        F.first(F.col("slug.raw_url"), ignorenulls=True).alias("example_raw_url"),
        F.countDistinct("channel_id").alias("channel_count"),
        F.sum("latest_views").alias("total_latest_views"),
    )
    .join(topic_node_map.select("canonical_slug", "yt_family", "yt_leaf", "node_type"), on="canonical_slug", how="left")
    .withColumn("mapped_flag", F.col("yt_family").isNotNull())
    .withColumn("mapped_family", F.coalesce(F.col("yt_family"), F.lit(UNMAPPED_FAMILY)))
    .withColumn("mapped_leaf", F.coalesce(F.col("yt_leaf"), F.concat(F.lit("Unmapped: "), F.col("canonical_slug"))))
    .withColumn("mapping_source", F.when(F.col("mapped_flag"), F.lit("config")).otherwise(F.lit("unmapped")))
    .withColumn("mapping_confidence", F.when(F.col("mapped_flag"), F.lit("manual_config")).otherwise(F.lit("unmapped")))
    .withColumn("notes", F.when(F.col("mapped_flag"), F.lit("")).otherwise(F.lit("Needs manual mapping review.")))
    .select(
        "raw_slug", "canonical_slug", "example_raw_url", "channel_count", "total_latest_views",
        "mapped_flag", "mapped_family", "mapped_leaf", "mapping_source", "mapping_confidence", "notes",
    )
    .orderBy(F.desc("total_latest_views"))
)

print("Top 50 topic slugs by latest view mass before allocation:")
_display_df(slug_inventory.select(
    "raw_slug", "canonical_slug", "channel_count", "total_latest_views",
    "mapped_flag", "mapped_family", "mapped_leaf",
).limit(50), n=50)


@F.udf(ArrayType(StringType()))
def _pair_keys(values: Optional[List[str]]) -> List[str]:
    vals = sorted(set(v for v in (values or []) if v))
    return [f"{a} || {b}" for a, b in itertools.combinations(vals, 2)]


raw_pairs = (
    projection
    .withColumn("pair_key", F.explode_outer(_pair_keys(F.col("normalized_slugs"))))
    .where(F.col("pair_key").isNotNull())
    .groupBy("pair_key")
    .agg(F.countDistinct("channel_id").alias("channel_count"), F.sum("latest_views").alias("total_latest_views"))
    .withColumn("pair_type", F.lit("raw_slug_pair"))
)
family_pairs = (
    projection
    .withColumn("pair_key", F.explode_outer(_pair_keys(F.col("display_families"))))
    .where(F.col("pair_key").isNotNull())
    .groupBy("pair_key")
    .agg(F.countDistinct("channel_id").alias("channel_count"), F.sum("latest_views").alias("total_latest_views"))
    .withColumn("pair_type", F.lit("display_family_pair"))
)
leaf_pairs = (
    projection
    .withColumn("pair_key", F.explode_outer(_pair_keys(F.col("display_leaves"))))
    .where(F.col("pair_key").isNotNull())
    .groupBy("pair_key")
    .agg(F.countDistinct("channel_id").alias("channel_count"), F.sum("latest_views").alias("total_latest_views"))
    .withColumn("pair_type", F.lit("display_leaf_pair"))
)
colabel_intersections = (
    raw_pairs.unionByName(family_pairs).unionByName(leaf_pairs)
    .withColumn("view_share", F.col("total_latest_views") / F.lit(total_latest_views if total_latest_views else 1.0))
)

# COMMAND ----------
# Build plot rows.
main_alloc = allocations.where(F.col("allocation_method") == F.lit("family_balanced")).cache()


def _concat_array(col_name: str) -> F.Column:
    return F.array_join(F.col(col_name), " | ")


def _id_part(col: F.Column) -> F.Column:
    return F.regexp_replace(F.coalesce(col.cast("string"), F.lit("missing")), r"[/\r\n\t]", " ")


def build_plot_rows(path_mode: str, top_k_languages: int, top_n_channels_per_leaf: int) -> DataFrame:
    lang_totals = (
        main_alloc.groupBy("language_code")
        .agg(F.sum("allocated_views").alias("views"))
        .orderBy(F.desc("views"))
        .limit(top_k_languages)
        .select("language_code")
    )
    base = (
        main_alloc
        .join(lang_totals.withColumn("_is_top_language", F.lit(True)), on="language_code", how="left")
        .withColumn("language_display", F.when(F.col("_is_top_language"), F.col("language_code")).otherwise(F.lit("Other languages")))
        .drop("_is_top_language")
        .withColumn("flags_display", F.concat_ws(
            "; ",
            F.when(F.col("has_cross_family_labels"), F.lit("cross-family")),
            F.when(F.col("has_parent_only_label"), F.lit("parent-only")),
            F.when(F.col("has_unmapped_labels"), F.lit("unmapped")),
            F.when(F.col("broken_closure_imputed"), F.lit("broken-closure")),
            F.when(F.col("has_no_topic_categories"), F.lit("no topicCategories")),
        ))
        .cache()
    )

    if path_mode == "language_first":
        root_id = F.lit("root")
        language_id = F.concat(F.lit("lang::"), _id_part(F.col("language_display")))
        family_id = F.concat(language_id, F.lit("/family::"), _id_part(F.col("yt_family")))
        leaf_id = F.concat(family_id, F.lit("/leaf::"), _id_part(F.col("yt_leaf")))
        channel_parent_id = leaf_id
        channel_id_expr = F.concat(leaf_id, F.lit("/channel::"), _id_part(F.col("channel_id")), F.lit("::"), _id_part(F.col("leaf_slug")))
        other_id_expr = F.concat(leaf_id, F.lit("/other"))

        language_nodes = base.groupBy("language_display").agg(F.sum("allocated_views").alias("allocated_views")).select(
            F.concat(F.lit("lang::"), _id_part(F.col("language_display"))).alias("node_id"),
            F.lit("root").alias("parent_id"),
            F.col("language_display").alias("label"),
            F.lit("language").alias("node_type"),
            "language_display",
            F.lit(None).cast("string").alias("yt_family_display"),
            F.lit(None).cast("string").alias("yt_leaf_display"),
            F.lit(None).cast("string").alias("channel_display"),
            F.lit(None).cast("string").alias("channel_id"),
            F.col("allocated_views"),
        )
        family_nodes = base.groupBy("language_display", "yt_family").agg(F.sum("allocated_views").alias("allocated_views")).select(
            F.concat(F.lit("lang::"), _id_part(F.col("language_display")), F.lit("/family::"), _id_part(F.col("yt_family"))).alias("node_id"),
            F.concat(F.lit("lang::"), _id_part(F.col("language_display"))).alias("parent_id"),
            F.col("yt_family").alias("label"),
            F.lit("family").alias("node_type"),
            "language_display",
            F.col("yt_family").alias("yt_family_display"),
            F.lit(None).cast("string").alias("yt_leaf_display"),
            F.lit(None).cast("string").alias("channel_display"),
            F.lit(None).cast("string").alias("channel_id"),
            F.col("allocated_views"),
        )
        leaf_nodes = base.groupBy("language_display", "yt_family", "yt_leaf").agg(F.sum("allocated_views").alias("allocated_views")).select(
            F.concat(F.lit("lang::"), _id_part(F.col("language_display")), F.lit("/family::"), _id_part(F.col("yt_family")), F.lit("/leaf::"), _id_part(F.col("yt_leaf"))).alias("node_id"),
            F.concat(F.lit("lang::"), _id_part(F.col("language_display")), F.lit("/family::"), _id_part(F.col("yt_family"))).alias("parent_id"),
            F.col("yt_leaf").alias("label"),
            F.lit("leaf").alias("node_type"),
            "language_display",
            F.col("yt_family").alias("yt_family_display"),
            F.col("yt_leaf").alias("yt_leaf_display"),
            F.lit(None).cast("string").alias("channel_display"),
            F.lit(None).cast("string").alias("channel_id"),
            F.col("allocated_views"),
        )
    else:
        root_id = F.lit("root")
        family_id = F.concat(F.lit("family::"), _id_part(F.col("yt_family")))
        leaf_id = F.concat(family_id, F.lit("/leaf::"), _id_part(F.col("yt_leaf")))
        language_id = F.concat(leaf_id, F.lit("/lang::"), _id_part(F.col("language_display")))
        channel_parent_id = language_id
        channel_id_expr = F.concat(language_id, F.lit("/channel::"), _id_part(F.col("channel_id")), F.lit("::"), _id_part(F.col("leaf_slug")))
        other_id_expr = F.concat(language_id, F.lit("/other"))

        language_nodes = base.groupBy("language_display", "yt_family", "yt_leaf").agg(F.sum("allocated_views").alias("allocated_views")).select(
            F.concat(F.lit("family::"), _id_part(F.col("yt_family")), F.lit("/leaf::"), _id_part(F.col("yt_leaf")), F.lit("/lang::"), _id_part(F.col("language_display"))).alias("node_id"),
            F.concat(F.lit("family::"), _id_part(F.col("yt_family")), F.lit("/leaf::"), _id_part(F.col("yt_leaf"))).alias("parent_id"),
            F.col("language_display").alias("label"),
            F.lit("language").alias("node_type"),
            "language_display",
            F.col("yt_family").alias("yt_family_display"),
            F.col("yt_leaf").alias("yt_leaf_display"),
            F.lit(None).cast("string").alias("channel_display"),
            F.lit(None).cast("string").alias("channel_id"),
            F.col("allocated_views"),
        )
        family_nodes = base.groupBy("yt_family").agg(F.sum("allocated_views").alias("allocated_views")).select(
            F.concat(F.lit("family::"), _id_part(F.col("yt_family"))).alias("node_id"),
            F.lit("root").alias("parent_id"),
            F.col("yt_family").alias("label"),
            F.lit("family").alias("node_type"),
            F.lit(None).cast("string").alias("language_display"),
            F.col("yt_family").alias("yt_family_display"),
            F.lit(None).cast("string").alias("yt_leaf_display"),
            F.lit(None).cast("string").alias("channel_display"),
            F.lit(None).cast("string").alias("channel_id"),
            F.col("allocated_views"),
        )
        leaf_nodes = base.groupBy("yt_family", "yt_leaf").agg(F.sum("allocated_views").alias("allocated_views")).select(
            F.concat(F.lit("family::"), _id_part(F.col("yt_family")), F.lit("/leaf::"), _id_part(F.col("yt_leaf"))).alias("node_id"),
            F.concat(F.lit("family::"), _id_part(F.col("yt_family"))).alias("parent_id"),
            F.col("yt_leaf").alias("label"),
            F.lit("leaf").alias("node_type"),
            F.lit(None).cast("string").alias("language_display"),
            F.col("yt_family").alias("yt_family_display"),
            F.col("yt_leaf").alias("yt_leaf_display"),
            F.lit(None).cast("string").alias("channel_display"),
            F.lit(None).cast("string").alias("channel_id"),
            F.col("allocated_views"),
        )

    rank_cols = ["language_display", "yt_family", "yt_leaf"] if path_mode == "language_first" else ["yt_family", "yt_leaf", "language_display"]
    channel_window = Window.partitionBy(*rank_cols).orderBy(F.col("allocated_views").desc(), F.col("channel_id").asc())
    ranked = base.withColumn("_plot_rank", F.row_number().over(channel_window))

    channel_rows = (
        ranked.where(F.col("_plot_rank") <= F.lit(top_n_channels_per_leaf))
        .select(
            channel_id_expr.alias("node_id"),
            channel_parent_id.alias("parent_id"),
            F.coalesce(F.col("channel_title"), F.col("channel_id")).alias("label"),
            F.lit("channel").alias("node_type"),
            "language_display",
            F.col("yt_family").alias("yt_family_display"),
            F.col("yt_leaf").alias("yt_leaf_display"),
            F.coalesce(F.col("channel_title"), F.col("channel_id")).alias("channel_display"),
            F.col("channel_id"),
            F.col("allocated_views"),
            F.col("latest_views").alias("raw_channel_views"),
            F.col("allocation_weight"),
            _concat_array("normalized_slugs").alias("all_raw_topic_slugs"),
            _concat_array("canonical_slugs").alias("all_canonical_slugs"),
            _concat_array("display_leaves").alias("all_display_leaves"),
            "flags_display",
        )
    )

    other_rows = (
        ranked.where(F.col("_plot_rank") > F.lit(top_n_channels_per_leaf))
        .groupBy(*rank_cols)
        .agg(
            F.sum("allocated_views").alias("allocated_views"),
            F.countDistinct("channel_id").alias("other_channel_count"),
        )
        .where(F.col("allocated_views") > 0)
        .select(
            other_id_expr.alias("node_id"),
            channel_parent_id.alias("parent_id"),
            F.concat(F.lit("Other channels (n="), F.col("other_channel_count").cast("string"), F.lit(")")).alias("label"),
            F.lit("other_channel").alias("node_type"),
            "language_display",
            F.col("yt_family").alias("yt_family_display"),
            F.col("yt_leaf").alias("yt_leaf_display"),
            F.lit("Other channels").alias("channel_display"),
            F.lit(None).cast("string").alias("channel_id"),
            F.col("allocated_views"),
            F.lit(None).cast("double").alias("raw_channel_views"),
            F.lit(None).cast("double").alias("allocation_weight"),
            F.lit(None).cast("string").alias("all_raw_topic_slugs"),
            F.lit(None).cast("string").alias("all_canonical_slugs"),
            F.lit(None).cast("string").alias("all_display_leaves"),
            F.concat(F.lit("pooled channels: "), F.col("other_channel_count").cast("string")).alias("flags_display"),
        )
    )

    root_node = spark.createDataFrame(
        [("root", "", "YouTube topics", "root", None, None, None, None, None, total_latest_views)],
        "node_id string, parent_id string, label string, node_type string, language_display string, yt_family_display string, yt_leaf_display string, channel_display string, channel_id string, allocated_views double",
    )

    internal_rows = (
        root_node
        .unionByName(language_nodes, allowMissingColumns=True)
        .unionByName(family_nodes, allowMissingColumns=True)
        .unionByName(leaf_nodes, allowMissingColumns=True)
        .withColumn("raw_channel_views", F.lit(None).cast("double"))
        .withColumn("allocation_weight", F.lit(None).cast("double"))
        .withColumn("all_raw_topic_slugs", F.lit(None).cast("string"))
        .withColumn("all_canonical_slugs", F.lit(None).cast("string"))
        .withColumn("all_display_leaves", F.lit(None).cast("string"))
        .withColumn("flags_display", F.lit(None).cast("string"))
    )

    rows = internal_rows.unionByName(channel_rows).unionByName(other_rows)
    rows = rows.withColumn(
        "hover_text",
        F.concat_ws(
            "<br>",
            F.concat(F.lit("Node: "), F.col("label")),
            F.concat(F.lit("Type: "), F.col("node_type")),
            F.concat(F.lit("Allocated views: "), F.format_number(F.col("allocated_views"), 0)),
            F.when(F.col("raw_channel_views").isNotNull(), F.concat(F.lit("Raw channel views: "), F.format_number(F.col("raw_channel_views"), 0))),
            F.when(F.col("allocation_weight").isNotNull(), F.concat(F.lit("Allocation fraction: "), F.format_number(F.col("allocation_weight"), 6))),
            F.when(F.col("all_raw_topic_slugs").isNotNull(), F.concat(F.lit("Raw topic slugs: "), F.col("all_raw_topic_slugs"))),
            F.when(F.col("all_display_leaves").isNotNull(), F.concat(F.lit("Display leaves: "), F.col("all_display_leaves"))),
            F.when(F.col("flags_display").isNotNull(), F.concat(F.lit("Flags: "), F.col("flags_display"))),
        )
    )
    return rows


plot_param_options = [
    (TOP_K_LANGUAGES_DEFAULT, TOP_N_CHANNELS_PER_LEAF_DEFAULT),
    (TOP_K_LANGUAGES_DEFAULT, min(TOP_N_CHANNELS_PER_LEAF_DEFAULT, 5)),
    (min(TOP_K_LANGUAGES_DEFAULT, 20), min(TOP_N_CHANNELS_PER_LEAF_DEFAULT, 5)),
]
plot_rows = None
final_top_k_languages = TOP_K_LANGUAGES_DEFAULT
final_top_n_channels = TOP_N_CHANNELS_PER_LEAF_DEFAULT
for k_lang, n_ch in plot_param_options:
    candidate_plot = build_plot_rows("language_first", k_lang, n_ch).cache()
    n_rows = candidate_plot.count()
    print(f"Plot-row candidate: top_k_languages={k_lang}, top_n_channels_per_leaf={n_ch}, rows={n_rows:,}")
    plot_rows = candidate_plot
    final_top_k_languages = k_lang
    final_top_n_channels = n_ch
    if n_rows <= MAX_PLOT_ROWS:
        break

plot_row_count = plot_rows.count()
print(f"Final plot parameters: top_k_languages={final_top_k_languages}, top_n_channels_per_leaf={final_top_n_channels}, rows={plot_row_count:,}")

topic_first_plot_rows = build_plot_rows("topic_first", final_top_k_languages, final_top_n_channels).cache()

# COMMAND ----------
# Diagnostics and sensitivity summaries.
coverage = projection.agg(
    F.count("*").alias("total_channels"),
    F.sum("latest_views").alias("total_latest_views"),
    F.sum((~F.col("has_no_topic_categories")).cast("long")).alias("channels_with_nonempty_topicCategories"),
    F.sum(F.when(~F.col("has_no_topic_categories"), F.col("latest_views")).otherwise(F.lit(0.0))).alias("views_with_nonempty_topicCategories"),
    F.sum(F.col("has_no_topic_categories").cast("long")).alias("channels_with_no_topicCategories"),
    F.sum(F.when(F.col("has_no_topic_categories"), F.col("latest_views")).otherwise(F.lit(0.0))).alias("views_with_no_topicCategories"),
    F.sum(F.col("has_parent_only_label").cast("long")).alias("channels_with_parent_only_label"),
    F.sum(F.when(F.col("has_parent_only_label"), F.col("latest_views")).otherwise(F.lit(0.0))).alias("views_with_parent_only_label"),
    F.sum(F.col("has_cross_family_labels").cast("long")).alias("channels_with_cross_family_labels"),
    F.sum(F.when(F.col("has_cross_family_labels"), F.col("latest_views")).otherwise(F.lit(0.0))).alias("views_with_cross_family_labels"),
    F.sum(F.col("broken_closure_imputed").cast("long")).alias("channels_with_broken_closure"),
    F.sum(F.when(F.col("broken_closure_imputed"), F.col("latest_views")).otherwise(F.lit(0.0))).alias("views_with_broken_closure"),
    F.sum(F.col("has_same_family_multi_child").cast("long")).alias("channels_with_same_family_multi_child"),
    F.sum(F.when(F.col("has_same_family_multi_child"), F.col("latest_views")).otherwise(F.lit(0.0))).alias("views_with_same_family_multi_child"),
).first().asDict()

unmapped_allocated_views = float(
    main_alloc.where(F.col("yt_family") == F.lit(UNMAPPED_FAMILY)).agg(F.sum("allocated_views").alias("v")).first()["v"] or 0.0
)
unmapped_view_share = unmapped_allocated_views / total_latest_views if total_latest_views else 0.0
no_label_view_share = float(coverage["views_with_no_topicCategories"] or 0.0) / total_latest_views if total_latest_views else 0.0
view_mass_coverage = float(coverage["views_with_nonempty_topicCategories"] or 0.0) / total_latest_views if total_latest_views else 0.0
broken_closure_view_share = float(coverage["views_with_broken_closure"] or 0.0) / total_latest_views if total_latest_views else 0.0
parent_only_view_share = float(coverage["views_with_parent_only_label"] or 0.0) / total_latest_views if total_latest_views else 0.0
cross_family_view_share = float(coverage["views_with_cross_family_labels"] or 0.0) / total_latest_views if total_latest_views else 0.0

dashboard_rows = []
for k, v in coverage.items():
    dashboard_rows.append({"metric": k, "value": v})
dashboard_rows.extend([
    {"metric": "view_mass_coverage", "value": view_mass_coverage},
    {"metric": "no_label_view_share", "value": no_label_view_share},
    {"metric": "unmapped_label_view_share", "value": unmapped_view_share},
    {"metric": "broken_closure_view_share", "value": broken_closure_view_share},
    {"metric": "parent_only_view_share", "value": parent_only_view_share},
    {"metric": "cross_family_view_share", "value": cross_family_view_share},
    {"metric": "allocation_methods", "value": ", ".join(sorted(allocation_methods))},
    {"metric": "latest_snapshot_date", "value": str(latest_snapshot_date)},
    {"metric": "final_top_k_languages", "value": final_top_k_languages},
    {"metric": "final_top_n_channels_per_leaf", "value": final_top_n_channels},
    {"metric": "plot_rows", "value": plot_row_count},
])

messiness_dashboard_pdf = pd.DataFrame(dashboard_rows)

cardinality_rows = []
for col_name, label in [
    ("n_raw_labels", "raw_label_cardinality"),
    ("n_display_families", "display_family_cardinality"),
    ("n_display_leaves", "display_leaf_cardinality"),
]:
    rows = (
        projection.groupBy(col_name)
        .agg(F.count("*").alias("channel_count"), F.sum("latest_views").alias("total_latest_views"))
        .orderBy(col_name)
        .collect()
    )
    for row in rows:
        cardinality_rows.append({
            "metric": label,
            "value": int(row[col_name]),
            "channel_count": int(row["channel_count"]),
            "total_latest_views": float(row["total_latest_views"] or 0.0),
            "view_share": float(row["total_latest_views"] or 0.0) / total_latest_views if total_latest_views else 0.0,
        })

cardinality_pdf = pd.DataFrame(cardinality_rows)

broken_children = (
    projection
    .select("channel_id", "latest_views", F.explode_outer("mapped_nodes").alias("node"))
    .where((F.col("node.node_type") == F.lit("child")) & F.col("node.broken_closure_imputed"))
    .groupBy(F.col("node.canonical_slug").alias("canonical_slug"), F.col("node.yt_family").alias("yt_family"), F.col("node.yt_leaf").alias("yt_leaf"))
    .agg(F.countDistinct("channel_id").alias("channel_count"), F.sum("latest_views").alias("total_latest_views"))
    .orderBy(F.desc("total_latest_views"))
)

parent_only_by_family = (
    main_alloc.where(F.col("is_parent_unspecified"))
    .groupBy("yt_family", "yt_leaf")
    .agg(F.countDistinct("channel_id").alias("channel_count"), F.sum("allocated_views").alias("allocated_views"))
    .orderBy(F.desc("allocated_views"))
)


def _shares_pdf(df: DataFrame, level_cols: List[str], method: str) -> pd.DataFrame:
    pdf = (
        df.where(F.col("allocation_method") == F.lit(method))
        .groupBy(*level_cols)
        .agg(F.sum("allocated_views").alias("allocated_views"))
        .toPandas()
    )
    if pdf.empty:
        pdf["share"] = []
        return pdf
    pdf["share"] = pdf["allocated_views"] / pdf["allocated_views"].sum()
    return pdf


def _rank_corr(a: pd.Series, b: pd.Series) -> float:
    if len(a) < 2:
        return float("nan")
    ar = a.rank(method="average")
    br = b.rank(method="average")
    return float(ar.corr(br))


sensitivity_rows = []
for method in sorted(m for m in allocation_methods if m != "family_balanced"):
    for level_name, level_cols in [
        ("family", ["yt_family"]),
        ("language_family", ["language_code", "yt_family"]),
        ("language_family_leaf", ["language_code", "yt_family", "yt_leaf"]),
    ]:
        base_pdf = _shares_pdf(allocations, level_cols, "family_balanced")
        method_pdf = _shares_pdf(allocations, level_cols, method)
        merged = base_pdf[level_cols + ["share"]].rename(columns={"share": "base_share"}).merge(
            method_pdf[level_cols + ["share"]].rename(columns={"share": "method_share"}),
            on=level_cols,
            how="outer",
        ).fillna({"base_share": 0.0, "method_share": 0.0})
        merged["abs_share_change"] = (merged["method_share"] - merged["base_share"]).abs()
        l1 = float(merged["abs_share_change"].sum())
        top = merged.sort_values("abs_share_change", ascending=False).head(25)
        family_spearman = float("nan")
        if level_name == "family":
            family_spearman = _rank_corr(merged["base_share"], merged["method_share"])
        sensitivity_rows.append({
            "row_type": "summary",
            "allocation_method": method,
            "comparison_level": level_name,
            "key": "",
            "base_share": None,
            "method_share": None,
            "abs_share_change": None,
            "l1_distance_vs_family_balanced": l1,
            "spearman_family_share_rank": family_spearman,
        })
        for _, r in top.iterrows():
            key = " | ".join(str(r[c]) for c in level_cols)
            sensitivity_rows.append({
                "row_type": "top_change",
                "allocation_method": method,
                "comparison_level": level_name,
                "key": key,
                "base_share": float(r["base_share"]),
                "method_share": float(r["method_share"]),
                "abs_share_change": float(r["abs_share_change"]),
                "l1_distance_vs_family_balanced": None,
                "spearman_family_share_rank": None,
            })

allocation_sensitivity_pdf = pd.DataFrame(sensitivity_rows)

# COMMAND ----------
# Write artifacts and Delta tables.
def _write_delta(df: DataFrame, short_name: str) -> str:
    table_name = f"{short_name}_{RUN_DATE}"
    table_full = _fqtn(OUTPUT_CATALOG, OUTPUT_SCHEMA, table_name)
    if WRITE_DELTA_TABLES:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_full)
        print("Wrote Delta table:", table_full)
    return table_full


tables_written = {
    "node_map": _write_delta(topic_node_map, "yt_topic_node_map_v2"),
    "projection": _write_delta(projection, "yt_channel_topic_projection_v2"),
    "allocations": _write_delta(allocations, "yt_treemap_allocations_v2"),
    "plot_rows_language_first": _write_delta(plot_rows, "yt_treemap_plot_rows_language_first_v2"),
    "plot_rows_topic_first": _write_delta(topic_first_plot_rows, "yt_treemap_plot_rows_topic_first_v2"),
}

diagnostics_table = spark.createDataFrame(messiness_dashboard_pdf.astype(str))
tables_written["diagnostics"] = _write_delta(diagnostics_table, "yt_treemap_diagnostics_v2")

projection.write.mode("overwrite").parquet(f"{ARTIFACT_SPARK_DIR}/channel_topic_projection.parquet")
allocations.write.mode("overwrite").parquet(f"{ARTIFACT_SPARK_DIR}/channel_label_allocations.parquet")
plot_rows.write.mode("overwrite").parquet(f"{ARTIFACT_SPARK_DIR}/treemap_plot_rows.parquet")
topic_first_plot_rows.write.mode("overwrite").parquet(f"{ARTIFACT_SPARK_DIR}/treemap_plot_rows_topic_first.parquet")

slug_inventory.toPandas().to_csv(os.path.join(ARTIFACT_DIR, "topic_slug_inventory.csv"), index=False)
topic_node_map.toPandas().to_csv(os.path.join(ARTIFACT_DIR, "topic_node_map.csv"), index=False)
messiness_dashboard_pdf.to_csv(os.path.join(ARTIFACT_DIR, "messiness_dashboard.csv"), index=False)
cardinality_pdf.to_csv(os.path.join(ARTIFACT_DIR, "cardinality_dashboard.csv"), index=False)
allocation_sensitivity_pdf.to_csv(os.path.join(ARTIFACT_DIR, "allocation_sensitivity_summary.csv"), index=False)
colabel_intersections.orderBy("pair_type", F.desc("total_latest_views")).toPandas().to_csv(
    os.path.join(ARTIFACT_DIR, "colabel_intersections.csv"),
    index=False,
)
parent_only_by_family.toPandas().to_csv(os.path.join(ARTIFACT_DIR, "parent_only_by_family.csv"), index=False)
broken_children.limit(1000).toPandas().to_csv(os.path.join(ARTIFACT_DIR, "broken_closure_children.csv"), index=False)

visible_head_audit = (
    plot_rows.where(F.col("node_type") == F.lit("channel"))
    .select(
        "channel_id",
        F.col("channel_display").alias("channel_title"),
        "language_display",
        F.col("raw_channel_views").alias("latest_views"),
        "allocated_views",
        "allocation_weight",
        F.col("yt_family_display").alias("plotted_family"),
        F.col("yt_leaf_display").alias("plotted_leaf"),
        F.col("all_raw_topic_slugs").alias("raw_topic_categories"),
        F.col("all_raw_topic_slugs").alias("normalized_slugs"),
        F.col("all_canonical_slugs").alias("canonical_slugs"),
        F.col("all_display_leaves").alias("all_display_leaves"),
        F.col("flags_display").contains("cross-family").alias("has_cross_family_labels"),
        F.col("flags_display").contains("parent-only").alias("has_parent_only_label"),
        F.col("flags_display").contains("unmapped").alias("has_unmapped_labels"),
        F.col("flags_display").contains("broken-closure").alias("broken_closure_imputed"),
        F.lit("").alias("manual_review_status"),
        F.lit("").alias("manual_review_notes"),
    )
)
visible_head_audit.toPandas().to_csv(os.path.join(ARTIFACT_DIR, "visible_head_audit.csv"), index=False)

# COMMAND ----------
# Render Plotly treemaps.
def _write_treemap_html(rows_df: DataFrame, path: str, title: str) -> None:
    import plotly.graph_objects as go

    pdf = rows_df.toPandas()
    duplicate_ids = pdf["node_id"].duplicated().sum()
    if duplicate_ids:
        raise AssertionError(f"DUPLICATE_ID_CHECK: FAIL ({duplicate_ids} duplicate node IDs)")
    print("DUPLICATE_ID_CHECK: PASS")
    fig = go.Figure(go.Treemap(
        ids=pdf["node_id"],
        labels=pdf["label"],
        parents=pdf["parent_id"],
        values=pdf["allocated_views"],
        branchvalues="total",
        customdata=pdf[["hover_text"]],
        hovertemplate="%{customdata[0]}<extra></extra>",
        maxdepth=4,
    ))
    fig.update_layout(title=title, margin=dict(l=4, r=4, t=36, b=4), font=dict(size=12))
    fig.write_html(path, include_plotlyjs="cdn")
    print("Wrote", path)


language_first_html = os.path.join(ARTIFACT_DIR, "treemap_youtube_topics_language_first.html")
topic_first_html = os.path.join(ARTIFACT_DIR, "treemap_youtube_topics_topic_first.html")
_write_treemap_html(plot_rows, language_first_html, "YouTube topicCategories treemap: language -> topic -> channel")
_write_treemap_html(topic_first_plot_rows, topic_first_html, "YouTube topicCategories treemap: topic -> language -> channel")

# COMMAND ----------
# Diagnostics report.
top_families_pdf = (
    main_alloc.groupBy("yt_family")
    .agg(F.sum("allocated_views").alias("allocated_views"))
    .orderBy(F.desc("allocated_views"))
    .limit(10)
    .toPandas()
)
top_language_family_pdf = (
    main_alloc.groupBy("language_code", "yt_family")
    .agg(F.sum("allocated_views").alias("allocated_views"))
    .orderBy(F.desc("allocated_views"))
    .limit(10)
    .toPandas()
)
top_unmapped_pdf = (
    slug_inventory.where(~F.col("mapped_flag"))
    .orderBy(F.desc("total_latest_views"))
    .limit(15)
    .toPandas()
)
top_broken_pdf = broken_children.limit(15).toPandas()

warnings = []
if unmapped_view_share > 0.05:
    warnings.append("HIGH RISK / PROVISIONAL FIGURE: unmapped-label view share exceeds 5%.")
elif unmapped_view_share > 0.01:
    warnings.append("WARNING: unmapped-label view share exceeds 1%.")
if broken_closure_view_share > 0.05:
    warnings.append("HIGH DRIFT WARNING: broken-closure view share exceeds 5%.")
if latest_snapshot_date and str(latest_snapshot_date) > "2025-03-31":
    warnings.append(
        "Starting March 31, 2025, YouTube changed Shorts view counting so channel viewCount includes starts "
        "and replays for Shorts. Channel lifetime viewCount series spanning this date may contain level shifts "
        "for Shorts-heavy channels."
    )

diagnostics_md = os.path.join(ARTIFACT_DIR, "diagnostics.md")


def _markdown_table(pdf: pd.DataFrame) -> str:
    if pdf.empty:
        return "_No rows._"
    return "```text\n" + pdf.to_string(index=False) + "\n```"


with open(diagnostics_md, "w") as fh:
    fh.write("# YouTube Topic Treemap v2 Diagnostics\n\n")
    fh.write(f"- Run date: {RUN_DATE}\n")
    fh.write(f"- Topic table: `{TOPIC_TABLE}`\n")
    fh.write(f"- Metrics table: `{metrics_table}`\n")
    fh.write(f"- Channel table: `{channel_table}`\n")
    fh.write(f"- Language table: `{language_table}`\n")
    fh.write(f"- Latest snapshot date: {latest_snapshot_date}\n")
    fh.write(f"- Channels processed: {projection_count:,}\n")
    fh.write(f"- Total latest views: {total_latest_views:,.0f}\n")
    fh.write(f"- View-mass coverage: {view_mass_coverage:.6%}\n")
    fh.write(f"- No-label view share: {no_label_view_share:.6%}\n")
    fh.write(f"- Unmapped-label view share: {unmapped_view_share:.6%}\n")
    fh.write(f"- Broken-closure view share: {broken_closure_view_share:.6%}\n")
    fh.write(f"- Parent-only view share: {parent_only_view_share:.6%}\n")
    fh.write(f"- Cross-family view share: {cross_family_view_share:.6%}\n")
    fh.write(f"- Reconciliation: PASS\n\n")
    if warnings:
        fh.write("## Risk Flags\n\n")
        for warning in warnings:
            fh.write(f"- {warning}\n")
        fh.write("\n")
    fh.write("## Top Families By Allocated Views\n\n")
    fh.write(_markdown_table(top_families_pdf))
    fh.write("\n\n## Top Language x Family Cells\n\n")
    fh.write(_markdown_table(top_language_family_pdf))
    fh.write("\n\n## Top Unmapped Slugs By View Mass\n\n")
    fh.write(_markdown_table(top_unmapped_pdf))
    fh.write("\n\n## Top Broken-Closure Child Slugs By View Mass\n\n")
    fh.write(_markdown_table(top_broken_pdf))
    fh.write("\n\n## Allocation Sensitivity Summary\n\n")
    summary_pdf = allocation_sensitivity_pdf[allocation_sensitivity_pdf["row_type"] == "summary"]
    fh.write(_markdown_table(summary_pdf))
    fh.write("\n")

print("Wrote", diagnostics_md)

# COMMAND ----------
# Required acceptance metrics.
def _pct(x: float) -> str:
    return f"{100.0 * x:.6f}%"


print(f"CHANNELS PROCESSED: {projection_count}")
print(f"LATEST SNAPSHOT DATE: {latest_snapshot_date}")
print(f"TOTAL LATEST VIEWS: {total_latest_views:.0f}")
print(f"VIEW-MASS COVERAGE: {_pct(view_mass_coverage)}")
print(f"NO-LABEL VIEW SHARE: {_pct(no_label_view_share)}")
print(f"UNMAPPED-LABEL VIEW SHARE: {_pct(unmapped_view_share)}")
print(f"BROKEN-CLOSURE VIEW SHARE: {_pct(broken_closure_view_share)}")
print(f"PARENT-ONLY VIEW SHARE: {_pct(parent_only_view_share)}")
print(f"CROSS-FAMILY VIEW SHARE: {_pct(cross_family_view_share)}")
print("RECONCILIATION: PASS")
print(f"MAIN TREEMAP HTML: {language_first_html}")
print(f"CHANNEL ALLOCATION ROWS: {allocations.count()}")
print(f"PLOT ROWS: {plot_row_count}")
print("TOP 10 FAMILIES BY ALLOCATED VIEWS:")
_display_df(main_alloc.groupBy("yt_family").agg(F.sum("allocated_views").alias("allocated_views")).orderBy(F.desc("allocated_views")).limit(10), n=10)
print("TOP 10 LANGUAGE x FAMILY CELLS:")
_display_df(main_alloc.groupBy("language_code", "yt_family").agg(F.sum("allocated_views").alias("allocated_views")).orderBy(F.desc("allocated_views")).limit(10), n=10)
print("TOP 15 UNMAPPED SLUGS BY VIEW MASS:")
_display_df(slug_inventory.where(~F.col("mapped_flag")).orderBy(F.desc("total_latest_views")).limit(15), n=15)
print("TOP 15 BROKEN-CLOSURE CHILD SLUGS BY VIEW MASS:")
_display_df(broken_children.limit(15), n=15)
print("ALLOCATION SENSITIVITY SUMMARY:")
_display_df(spark.createDataFrame(allocation_sensitivity_pdf[allocation_sensitivity_pdf["row_type"] == "summary"].astype(str)), n=20)

print("Tables written:", json.dumps(tables_written, indent=2))
print("Artifact directory:", ARTIFACT_DIR)

acceptance_payload = {
    "run_date": RUN_DATE,
    "channels_processed": int(projection_count),
    "latest_snapshot_date": str(latest_snapshot_date),
    "total_latest_views": total_latest_views,
    "view_mass_coverage": view_mass_coverage,
    "no_label_view_share": no_label_view_share,
    "unmapped_label_view_share": unmapped_view_share,
    "broken_closure_view_share": broken_closure_view_share,
    "parent_only_view_share": parent_only_view_share,
    "cross_family_view_share": cross_family_view_share,
    "reconciliation": "PASS",
    "main_treemap_html": language_first_html,
    "topic_first_treemap_html": topic_first_html,
    "artifact_dir": ARTIFACT_DIR,
    "tables_written": tables_written,
    "channel_allocation_rows": int(allocations.count()),
    "plot_rows": int(plot_row_count),
}

try:
    dbutils.notebook.exit(json.dumps(acceptance_payload, sort_keys=True, default=str))
except Exception:
    pass
