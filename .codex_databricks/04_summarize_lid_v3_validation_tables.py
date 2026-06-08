# Databricks notebook source
# MAGIC %md
# MAGIC # Summarize LID v3 validation analysis tables

# COMMAND ----------
from datetime import datetime, timezone
import json
import re

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


def _safe_token(raw: str, default: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "_", (raw or "").strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token or default


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _fqtn(catalog: str, schema: str, table: str) -> str:
    return f"{_quote(catalog)}.{_quote(schema)}.{_quote(table)}"


def _rows(df, limit: int = 200):
    return [row.asDict(recursive=True) for row in df.limit(limit).collect()]

# COMMAND ----------
_create_text_widget("scratch_catalog", "dev_sean")
_create_text_widget("scratch_schema", "matt")
_create_text_widget("output_prefix", "yt_lid_v3_validation_10k_20260608_161345_b10")
_create_text_widget("run_id", "codex_10k_20260608_161345_b10")
_create_text_widget("top_language_n", "10")

SCRATCH_CATALOG = _get_widget("scratch_catalog", "dev_sean")
SCRATCH_SCHEMA = _get_widget("scratch_schema", "matt")
OUTPUT_PREFIX = _safe_token(_get_widget("output_prefix", "yt_lid_v3_validation_10k_20260608_161345_b10"), "yt_lid_v3_validation_10k_20260608_161345_b10")
RUN_ID = _get_widget("run_id", "codex_10k_20260608_161345_b10")
TOP_LANGUAGE_N = _get_int_widget("top_language_n", 10)

# COMMAND ----------
cohort = spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_cohort_sample"))
table_counts = spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_analysis_table_counts"))
status_by_stratum = spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_analysis_status_by_stratum"))
consensus_by_stratum = spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_analysis_consensus_by_stratum"))
agreement_by_stratum = spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_analysis_agreement_by_stratum"))
top_languages = spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_analysis_top_languages"))
validation_sample_strata = spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_analysis_manual_validation_sample_strata"))
figures = spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_analysis_figures_svg"))

cohort_counts = (
    cohort
    .groupBy("validation_stratum")
    .agg(
        F.count(F.lit(1)).alias("n_channels"),
        F.countDistinct("channel_id").alias("distinct_channels"),
    )
    .orderBy("validation_stratum")
)

top_languages_ranked = (
    top_languages
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("validation_stratum").orderBy(F.desc("count"), "consensus_language_label")
    ))
    .where(F.col("_rn") <= F.lit(TOP_LANGUAGE_N))
    .drop("_rn")
    .orderBy("validation_stratum", F.desc("count"), "consensus_language_label")
)

figures_summary = (
    figures
    .select(
        "file_name",
        "status",
        F.length("svg_text").alias("svg_chars"),
        F.col("message"),
    )
    .orderBy("file_name")
)

result = {
    "status": "ok",
    "recorded_at": datetime.now(timezone.utc).isoformat(),
    "run_id": RUN_ID,
    "output_prefix": OUTPUT_PREFIX,
    "tables": {
        "cohort": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{OUTPUT_PREFIX}_cohort_sample",
        "channels": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{OUTPUT_PREFIX}_channels",
        "metrics": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{OUTPUT_PREFIX}_analysis_metrics",
        "figures": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{OUTPUT_PREFIX}_analysis_figures_svg",
    },
    "cohort_counts": _rows(cohort_counts),
    "table_counts": _rows(table_counts.orderBy("table_key")),
    "status_by_stratum": _rows(status_by_stratum.orderBy("validation_stratum", F.desc("count"), "language_status")),
    "consensus_by_stratum": _rows(consensus_by_stratum.orderBy("validation_stratum", F.desc("count"), "consensus_status")),
    "agreement_by_stratum": _rows(agreement_by_stratum.orderBy("validation_stratum")),
    "top_languages": _rows(top_languages_ranked),
    "manual_validation_sample_strata": _rows(validation_sample_strata.orderBy(F.desc("count"), "primary_stratum")),
    "figures": _rows(figures_summary),
}

dbutils.notebook.exit(json.dumps(result, sort_keys=True))
