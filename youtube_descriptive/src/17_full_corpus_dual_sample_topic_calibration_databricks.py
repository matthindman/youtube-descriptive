# Databricks notebook source
# ruff: noqa: F821
# MAGIC %run ./full_corpus_dual_sample_design

# COMMAND ----------
"""Human-validation-gated calibration of model-completed leaf probabilities."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone

import numpy as np
from scipy.optimize import minimize_scalar
from pyspark.sql import DataFrame
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


_widget(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
CONFIG_PATH = _get(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
CONFIG = json.loads(dbutils.fs.head(CONFIG_PATH, 1024 * 1024))
validate_design_config(CONFIG)
DESIGN_VERSION = CONFIG["design_version"]
TOPIC_MODEL = CONFIG["topic_model"]
CALIBRATION = CONFIG["topic_calibration"]
PREFIX = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_prefix']}"
TABLES = {
    "predictions": f"{PREFIX}_topic_model_predictions",
    "human": f"{PREFIX}_{CALIBRATION['human_validation_table_suffix']}",
    "calibrated": f"{PREFIX}_topic_model_calibrated",
    "summary": f"{PREFIX}_topic_calibration_summary",
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
        f"'calibration.method'='{CALIBRATION['method']}', "
        "'probabilities.calibrated'='true')"
    )


def parse_probabilities(raw: str | None) -> dict[tuple[str, str], float]:
    result: dict[tuple[str, str], float] = {}
    for item in json.loads(raw or "[]"):
        key = (str(item["family"]), str(item["leaf"]))
        result[key] = result.get(key, 0.0) + float(item["probability"])
    return result


def transformed(values: dict[tuple[str, str], float], temperature: float) -> dict[tuple[str, str], float]:
    powered = {
        key: max(float(value), 0.0) ** (1.0 / temperature)
        for key, value in values.items()
        if value > 0
    }
    denominator = sum(powered.values())
    if denominator <= 0:
        return {}
    return {key: value / denominator for key, value in powered.items()}


def validation_rows() -> tuple[list[dict], int]:
    predictions = (
        spark.table(TABLES["predictions"])
        .where(
            (F.col("request_domain") == "platform_topic_validation")
            & (F.col("status") == "classified")
            & F.col("parse_error").isNull()
        )
        .select("channel_id", "leaf_probabilities_json")
        .toPandas()
    )
    human = spark.table(TABLES["human"])
    required = {
        "channel_id",
        "family",
        "leaf",
        "human_probability",
        "validation_weight",
        "adjudication_status",
    }
    missing = sorted(required - set(human.columns))
    if missing:
        raise RuntimeError(f"{TABLES['human']} is missing required columns: {missing}")
    human_pdf = (
        human.where(F.col("adjudication_status") == "complete")
        .select(
            "channel_id",
            "family",
            "leaf",
            F.col("human_probability").cast("double").alias("human_probability"),
            F.col("validation_weight").cast("double").alias("validation_weight"),
        )
        .toPandas()
    )
    completed_n = int(human_pdf["channel_id"].nunique())
    if completed_n < int(CALIBRATION["minimum_completed_channels"]):
        raise RuntimeError(
            f"Topic calibration blocked: {completed_n:,} completed validation channels; "
            f"minimum={int(CALIBRATION['minimum_completed_channels']):,}"
        )
    human_sums = human_pdf.groupby("channel_id")["human_probability"].sum()
    if not np.allclose(human_sums.to_numpy(), 1.0, rtol=0.0, atol=1e-6):
        raise RuntimeError("Human topic probabilities do not sum to one per completed channel")
    weight_counts = human_pdf.groupby("channel_id")["validation_weight"].nunique()
    if int((weight_counts != 1).sum()) > 0 or bool((human_pdf["validation_weight"] <= 0).any()):
        raise RuntimeError("Validation weights must be one positive value per completed channel")
    prediction_map = {
        str(row.channel_id): parse_probabilities(row.leaf_probabilities_json)
        for row in predictions.itertuples(index=False)
    }
    rows = []
    for channel_id, group in human_pdf.groupby("channel_id"):
        raw = prediction_map.get(str(channel_id))
        if not raw:
            continue
        truth = {
            (str(row.family), str(row.leaf)): float(row.human_probability)
            for row in group.itertuples(index=False)
            if float(row.human_probability) > 0
        }
        rows.append(
            {
                "channel_id": str(channel_id),
                "raw": raw,
                "truth": truth,
                "weight": float(group["validation_weight"].iloc[0]),
            }
        )
    if len(rows) < int(CALIBRATION["minimum_completed_channels"]):
        raise RuntimeError(
            "Too few completed human-validation channels have valid model predictions: "
            f"{len(rows):,}"
        )
    return rows, completed_n


def weighted_metrics(rows: list[dict], temperature: float) -> tuple[float, float]:
    floor = float(CALIBRATION["probability_floor"])
    loss_sum = 0.0
    brier_sum = 0.0
    weight_sum = 0.0
    for row in rows:
        probabilities = transformed(row["raw"], temperature)
        keys = set(probabilities) | set(row["truth"])
        weight = row["weight"]
        loss_sum += weight * sum(
            -truth * math.log(max(probabilities.get(key, 0.0), floor))
            for key, truth in row["truth"].items()
        )
        brier_sum += weight * sum(
            (probabilities.get(key, 0.0) - row["truth"].get(key, 0.0)) ** 2
            for key in keys
        )
        weight_sum += weight
    return loss_sum / weight_sum, brier_sum / weight_sum


for table_name in (TABLES["predictions"], TABLES["human"]):
    require_table(table_name)
rows, completed_human_n = validation_rows()
lower = float(CALIBRATION["temperature_lower_bound"])
upper = float(CALIBRATION["temperature_upper_bound"])
fit = minimize_scalar(
    lambda temperature: weighted_metrics(rows, float(temperature))[0],
    bounds=(lower, upper),
    method="bounded",
    options={"xatol": 1e-5},
)
if not fit.success:
    raise RuntimeError(f"Temperature calibration failed: {fit.message}")
temperature = float(fit.x)
raw_log_loss, raw_brier = weighted_metrics(rows, 1.0)
calibrated_log_loss, calibrated_brier = weighted_metrics(rows, temperature)
if calibrated_log_loss > raw_log_loss + 1e-10:
    raise RuntimeError("Fitted calibration did not improve weighted validation log loss")

leaf_schema = T.ArrayType(
    T.StructType(
        [
            T.StructField("family", T.StringType(), False),
            T.StructField("leaf", T.StringType(), False),
            T.StructField("probability", T.DoubleType(), False),
        ]
    )
)


def calibrate_json(raw: str | None):
    calibrated = transformed(parse_probabilities(raw), temperature)
    return [
        {"family": key[0], "leaf": key[1], "probability": probability}
        for key, probability in sorted(calibrated.items())
    ]


calibrate_udf = F.udf(calibrate_json, leaf_schema)
predictions = spark.table(TABLES["predictions"]).where(
    (F.col("request_domain") == "platform_topic_missing")
    & (F.col("status") == "classified")
    & F.col("parse_error").isNull()
)
calibrated = (
    predictions.withColumn("_calibrated", calibrate_udf("leaf_probabilities_json"))
    .select(
        "channel_id",
        "status",
        F.lit(True).alias("is_calibrated"),
        F.explode("_calibrated").alias("calibrated"),
    )
    .select(
        "channel_id",
        "status",
        "is_calibrated",
        F.col("calibrated.family").alias("family"),
        F.col("calibrated.leaf").alias("leaf"),
        F.col("calibrated.probability").alias("probability"),
    )
    .withColumn("calibration_method", F.lit(CALIBRATION["method"]))
    .withColumn("temperature", F.lit(temperature))
    .withColumn("calibrated_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
    .withColumn("design_version", F.lit(DESIGN_VERSION))
)
bad_sums = (
    calibrated.groupBy("channel_id")
    .agg(F.sum("probability").alias("probability_sum"))
    .where(F.abs(F.col("probability_sum") - 1.0) > 1e-8)
    .limit(1)
    .count()
)
if bad_sums:
    raise RuntimeError("Calibrated leaf probabilities do not sum to one per channel")
write_table(
    calibrated,
    TABLES["calibrated"],
    "Human-validation-gated, weighted-temperature-calibrated topic leaf probabilities.",
)
summary = {
    "completed_human_validation_channels": completed_human_n,
    "matched_validation_channels": len(rows),
    "temperature": temperature,
    "raw_weighted_log_loss": raw_log_loss,
    "calibrated_weighted_log_loss": calibrated_log_loss,
    "raw_weighted_brier": raw_brier,
    "calibrated_weighted_brier": calibrated_brier,
    "calibrated_channels": calibrated.select("channel_id").distinct().count(),
    "calibrated_rows": calibrated.count(),
}
summary_rows = [
    (DESIGN_VERSION, key, json.dumps(value), datetime.now(timezone.utc))
    for key, value in summary.items()
]
write_table(
    spark.createDataFrame(
        summary_rows,
        "design_version string, metric string, value_json string, recorded_at timestamp",
    ),
    TABLES["summary"],
    "Human-validation topic-calibration fit and acceptance metrics.",
)
print("TOPIC CALIBRATION: PASS")
print(json.dumps(summary, sort_keys=True))
dbutils.notebook.exit(json.dumps(summary, sort_keys=True))
