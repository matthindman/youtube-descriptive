# Databricks notebook source
import json
import re
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CHANNELS = "prod_tads.youtube_too.yt_sl_channels"
CATALOG = "dev_sean"
SCHEMAS = [
    "default",
    "matt",
    "diagnostics",
    "validation",
    "threshold_yt_1k",
    "threshold_yt_5k",
    "llm_incitement",
    "betting",
]

CATEGORY_COL_RE = re.compile(
    r"(topic|categor|genre|archetype|primary_topic|topic_top_k_json|ai_label|all_labels|raw_json)",
    re.IGNORECASE,
)
CATEGORY_TABLE_RE = re.compile(r"(topic|categor|genre|archetype|classif|llm)", re.IGNORECASE)
IGNORED_COL_RE = re.compile(
    r"(language|sentiment|toxicity|lid_|label_raw|label_[0-9]|iso639|script)",
    re.IGNORECASE,
)

CHANNEL_KEY_CANDIDATES = [
    "channel_id",
    "canonical_id",
    "youtube_channel_id",
    "yt_channel_id",
    "channelid",
]

SENTINEL_VALUES = [
    "",
    "none",
    "null",
    "unknown",
    "uncategorized",
    "pending",
    "error",
    "api_null",
    "api_null_final",
]


spark.conf.set("spark.databricks.remoteFiltering.blockSelfJoins", "false")


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fqtn(schema_name: str, table_name: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(schema_name)}.{quote_ident(table_name)}"


def get_channel_key(columns: List[str]) -> Optional[str]:
    lowered = {c.lower(): c for c in columns}
    for candidate in CHANNEL_KEY_CANDIDATES:
        if candidate in lowered:
            return lowered[candidate]
    return None


def candidate_value_expr(df: DataFrame, column_name: str):
    dtype = dict(df.dtypes).get(column_name, "")
    c = F.col(column_name)
    if column_name.lower() == "raw_json":
        return F.coalesce(
            F.get_json_object(c, "$.topicDetails.topicCategories[0]"),
            F.get_json_object(c, "$.items[0].topicDetails.topicCategories[0]"),
            F.get_json_object(c, "$.topicDetails.topicCategories"),
            F.get_json_object(c, "$.items[0].topicDetails.topicCategories"),
        )
    if dtype.startswith("array"):
        return F.element_at(c, 1).cast("string")
    if dtype.startswith("struct"):
        return F.to_json(c)
    return c.cast("string")


def nonempty_value_filter(value_col):
    normalized = F.lower(F.trim(value_col.cast("string")))
    return value_col.isNotNull() & (F.length(F.trim(value_col.cast("string"))) > 0) & (~normalized.isin(SENTINEL_VALUES))


def list_tables(schema_name: str) -> List[str]:
    try:
        return [
            row.tableName
            for row in spark.sql(f"SHOW TABLES IN {quote_ident(CATALOG)}.{quote_ident(schema_name)}").collect()
            if not row.isTemporary
        ]
    except Exception as exc:
        print(f"[WARN] Could not list {CATALOG}.{schema_name}: {exc}")
        return []


channels = (
    spark.table(CHANNELS)
    .select(F.col("channel_id").cast("string").alias("channel_id"))
    .where(F.col("channel_id").isNotNull())
    .dropDuplicates(["channel_id"])
)
n_channels = channels.count()

metadata_matches: List[Dict[str, object]] = []
coverage_rows: List[Dict[str, object]] = []

for schema_name in SCHEMAS:
    for table_name in list_tables(schema_name):
        table_fqtn = fqtn(schema_name, table_name)
        try:
            df = spark.table(table_fqtn)
        except Exception as exc:
            metadata_matches.append(
                {
                    "table": f"{CATALOG}.{schema_name}.{table_name}",
                    "error": str(exc)[:500],
                }
            )
            continue

        columns = df.columns
        channel_key = get_channel_key(columns)
        candidate_cols = [
            col_name
            for col_name in columns
            if CATEGORY_COL_RE.search(col_name) and not IGNORED_COL_RE.search(col_name)
        ]
        table_name_match = bool(CATEGORY_TABLE_RE.search(table_name))

        if candidate_cols or table_name_match:
            metadata_matches.append(
                {
                    "table": f"{CATALOG}.{schema_name}.{table_name}",
                    "channel_key": channel_key,
                    "candidate_columns": candidate_cols,
                    "all_columns": columns[:120],
                    "matched_by_table_name": table_name_match,
                }
            )

        if not channel_key or not candidate_cols:
            continue

        for col_name in candidate_cols:
            value = candidate_value_expr(df, col_name).alias("category_value")
            labeled = (
                df.select(F.col(channel_key).cast("string").alias("channel_id"), value)
                .where(F.col("channel_id").isNotNull() & nonempty_value_filter(F.col("category_value")))
                .dropDuplicates(["channel_id"])
            )
            try:
                n_labeled_channels = labeled.count()
                n_overlap_channels = channels.join(labeled.select("channel_id"), on="channel_id", how="inner").count()
                sample_values = [
                    row.category_value
                    for row in labeled.select("category_value").where(nonempty_value_filter(F.col("category_value"))).limit(8).collect()
                ]
                top_values = [
                    row.asDict(recursive=True)
                    for row in (
                        channels.join(labeled, on="channel_id", how="inner")
                        .groupBy("category_value")
                        .agg(F.count("*").alias("n_channels"))
                        .orderBy(F.desc("n_channels"), F.asc("category_value"))
                        .limit(15)
                        .collect()
                    )
                ]
                status_counts = []
                if "status" in df.columns:
                    status_counts = [
                        row.asDict(recursive=True)
                        for row in (
                            df.select(
                                F.col(channel_key).cast("string").alias("channel_id"),
                                F.lower(F.trim(F.col("status").cast("string"))).alias("status"),
                                candidate_value_expr(df, col_name).alias("category_value"),
                            )
                            .where(F.col("channel_id").isNotNull() & nonempty_value_filter(F.col("category_value")))
                            .groupBy("status")
                            .agg(F.countDistinct("channel_id").alias("n_channels"))
                            .orderBy(F.desc("n_channels"), F.asc("status"))
                            .collect()
                        )
                    ]
                coverage_rows.append(
                    {
                        "table": f"{CATALOG}.{schema_name}.{table_name}",
                        "column": col_name,
                        "column_type": dict(df.dtypes).get(col_name),
                        "channel_key": channel_key,
                        "n_labeled_distinct_channels_in_table": n_labeled_channels,
                        "n_labeled_channels_in_youtube_too": n_overlap_channels,
                        "pct_youtube_too_coverage": round(100.0 * n_overlap_channels / n_channels, 2),
                        "sample_values": sample_values,
                        "top_values_in_youtube_too": top_values,
                        "status_counts": status_counts,
                    }
                )
            except Exception as exc:
                coverage_rows.append(
                    {
                        "table": f"{CATALOG}.{schema_name}.{table_name}",
                        "column": col_name,
                        "channel_key": channel_key,
                        "error": str(exc)[:800],
                    }
                )

coverage_rows = sorted(
    coverage_rows,
    key=lambda row: (
        row.get("pct_youtube_too_coverage", -1) if isinstance(row.get("pct_youtube_too_coverage"), (int, float)) else -1,
        row.get("table", ""),
        row.get("column", ""),
    ),
    reverse=True,
)

result = {
    "channel_universe": CHANNELS,
    "n_youtube_too_channels": n_channels,
    "metadata_matches": metadata_matches,
    "coverage_candidates_ranked": coverage_rows,
}

print(json.dumps(result, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
