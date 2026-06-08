# Databricks notebook source
import json
import re

from pyspark.sql import functions as F


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


def _safe_token(raw: str, default: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "_", (raw or "").strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token or default


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _fqtn(catalog: str, schema: str, table: str) -> str:
    return f"{_quote(catalog)}.{_quote(schema)}.{_quote(table)}"


def _rows(df, limit=200):
    return [r.asDict(recursive=True) for r in df.limit(limit).collect()]


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("output_prefix", "yt_lid_v3_validation_10k_20260608_161345_b10")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
OUTPUT_PREFIX = _safe_token(_get_widget("output_prefix", "yt_lid_v3_validation_10k_20260608_161345_b10"), "yt_lid_v3_validation_10k_20260608_161345_b10")

summary = spark.table(_fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_analysis_field_source_ablation_summary"))
segment_counts = spark.table(_fqtn(CATALOG, SCHEMA, f"{OUTPUT_PREFIX}_analysis_field_source_segment_counts"))

cols = [
    "config_order",
    "config_name",
    "config_label",
    "validation_stratum",
    "n_channels",
    "channels_with_valid_segments",
    "both_models_classified_rate",
    "exact_agreement_rate_all_channels",
    "iso_agreement_rate_all_channels",
    "exact_agreement_rate_among_classified",
    "iso_agreement_rate_among_classified",
    "any_model_mixed_screen_rate",
    "both_models_same_secondary_screen_rate",
    "mean_valid_segments_per_channel",
]
rounded = summary.select(
    *[
        F.round(F.col(c), 6).alias(c)
        if c.endswith("_rate") or c.startswith("mean_") or c.endswith("_classified")
        else F.col(c)
        for c in cols
    ]
)

dbutils.notebook.exit(json.dumps({
    "overall": _rows(rounded.where(F.col("validation_stratum") == F.lit("overall")).orderBy("config_order"), 50),
    "by_stratum": _rows(rounded.where(F.col("validation_stratum") != F.lit("overall")).orderBy("config_order", "validation_stratum"), 100),
    "segment_counts": _rows(segment_counts.orderBy("segment_type"), 20),
}, sort_keys=True))
