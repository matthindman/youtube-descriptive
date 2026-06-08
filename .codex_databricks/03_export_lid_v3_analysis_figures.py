# Databricks notebook source
# MAGIC %md
# MAGIC # Export LID v3 validation figure SVGs

# COMMAND ----------
from datetime import datetime, timezone
import json
import os
import re
from typing import Iterable, Optional

from pyspark.sql.types import LongType, StringType, StructField, StructType

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


# COMMAND ----------
_create_text_widget("scratch_catalog", "dev_sean")
_create_text_widget("scratch_schema", "matt")
_create_text_widget("output_prefix", "yt_lid_v3_validation_10k_20260608_161345_b10")
_create_text_widget("run_id", "codex_10k_20260608_161345_b10")
_create_text_widget("figure_local_dir", "/local_disk0/tmp/yt_lid_v3_figures/yt_lid_v3_validation_10k_20260608_161345_b10/codex_10k_20260608_161345_b10")
_create_text_widget("volume_figure_dir", "/Volumes/dev_sean/matt/models/codex_lid_v3_validation_20260608_161345_b10_figures")

SCRATCH_CATALOG = _get_widget("scratch_catalog", "dev_sean")
SCRATCH_SCHEMA = _get_widget("scratch_schema", "matt")
OUTPUT_PREFIX = _safe_token(_get_widget("output_prefix", "yt_lid_v3_validation_10k_20260608_161345_b10"), "yt_lid_v3_validation_10k_20260608_161345_b10")
RUN_ID = _get_widget("run_id", "codex_10k_20260608_161345_b10")
FIGURE_LOCAL_DIR = _get_widget("figure_local_dir", "")
VOLUME_FIGURE_DIR = _get_widget("volume_figure_dir", "").rstrip("/")
OUTPUT_TABLE = f"{OUTPUT_PREFIX}_analysis_figures_svg"

FIGURE_NAMES = [
    "language_status_by_prior_stratum.svg",
    "consensus_status_by_prior_stratum.svg",
    "agreement_rates_by_prior_stratum.svg",
]

# COMMAND ----------
rows = []
volume_paths = []
if VOLUME_FIGURE_DIR:
    os.makedirs(VOLUME_FIGURE_DIR, exist_ok=True)

for name in FIGURE_NAMES:
    local_path = os.path.join(FIGURE_LOCAL_DIR, name)
    if not os.path.exists(local_path):
        rows.append((RUN_ID, name, local_path, None, None, "missing", datetime.now(timezone.utc).isoformat()))
        continue
    with open(local_path, "r", encoding="utf-8") as fh:
        svg_text = fh.read()
    volume_path = None
    if VOLUME_FIGURE_DIR:
        volume_path = os.path.join(VOLUME_FIGURE_DIR, name)
        with open(volume_path, "w", encoding="utf-8") as fh:
            fh.write(svg_text)
        volume_paths.append(volume_path)
    rows.append((RUN_ID, name, local_path, volume_path, svg_text, "ok", datetime.now(timezone.utc).isoformat()))

schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("driver_local_path", StringType(), True),
    StructField("volume_path", StringType(), True),
    StructField("svg_text", StringType(), True),
    StructField("status", StringType(), False),
    StructField("exported_at", StringType(), False),
])
figures_df = spark.createDataFrame(rows, schema=schema)
table_full = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, OUTPUT_TABLE)
_overwrite_delta(figures_df, table_full)

result = {
    "status": "ok" if all(r[5] == "ok" for r in rows) else "partial",
    "figures_table": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{OUTPUT_TABLE}",
    "volume_paths": volume_paths,
    "rows": [{"file_name": r[1], "status": r[5], "chars": len(r[4] or "")} for r in rows],
}
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
