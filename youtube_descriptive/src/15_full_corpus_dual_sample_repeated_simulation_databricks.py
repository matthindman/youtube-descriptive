# Databricks notebook source
# ruff: noqa: F821
# MAGIC %run ./full_corpus_dual_sample_design

# COMMAND ----------
"""Five-thousand-replicate design check on frozen head and tail pseudo-populations."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone

import numpy as np
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
FRAME_VERSION = CONFIG["frame_version"]
SIMULATION = CONFIG["simulation"]
SAMPLES = CONFIG["samples"]
PREFIX = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_prefix']}"
FRAME_TABLE = f"{PREFIX}_frame"
OUTPUT_TABLE = f"{PREFIX}_repeated_simulation"


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
        f"'frame.version'='{FRAME_VERSION}', "
        f"'simulation.replicates'='{int(SIMULATION['final_replicates'])}')"
    )


def pseudo_population(status: str, pseudo_name: str) -> tuple[dict[str, np.ndarray], int]:
    frame = spark.table(FRAME_TABLE).where(F.col("subscriber_status") == status).select(
        "channel_id",
        F.col("accepted_positive_view_mass").cast("double").alias("accepted_views"),
        (F.col("delta_status") == "positive").cast("double").alias("positive_delta"),
        F.col("has_valid_endpoint_pair").cast("double").alias("valid_endpoint_pair"),
        F.col("has_nonempty_topic_categories").cast("double").alias("platform_topic_nonempty"),
    )
    full_n = frame.count()
    max_n = min(int(SIMULATION["pseudo_population_max_n"]), full_n)
    top_n = min(int(SIMULATION["pseudo_population_top_view_n"]), max_n)
    top = frame.orderBy(F.col("accepted_views").desc(), F.col("channel_id").asc()).limit(top_n)
    top_ids = top.select("channel_id")
    remainder = (
        frame.join(top_ids, "channel_id", "left_anti")
        .withColumn(
            "_hash",
            F.sha2(
                F.concat_ws(
                    "\x1f",
                    "channel_id",
                    F.lit(FRAME_VERSION),
                    F.lit(f"repeated_simulation_{pseudo_name}"),
                ),
                256,
            ),
        )
        .orderBy("_hash", "channel_id")
        .limit(max_n - top_n)
        .drop("_hash")
    )
    pandas_frame = top.unionByName(remainder).toPandas()
    arrays = {
        "channel_count": np.ones(len(pandas_frame), dtype=np.float64),
        "accepted_view_mass": pandas_frame["accepted_views"].fillna(0.0).to_numpy(dtype=np.float64),
        "positive_delta_channels": pandas_frame["positive_delta"].fillna(0.0).to_numpy(dtype=np.float64),
        "valid_endpoint_channels": pandas_frame["valid_endpoint_pair"].fillna(0.0).to_numpy(dtype=np.float64),
        "platform_topic_nonempty_channels": pandas_frame["platform_topic_nonempty"].fillna(0.0).to_numpy(dtype=np.float64),
    }
    arrays["pps_size"] = arrays["accepted_view_mass"] / float(CONFIG["elapsed_days"] / 7)
    print(
        f"PSEUDO POPULATION: {pseudo_name} rows={len(pandas_frame):,} "
        f"top_view_take_all={top_n:,} source_domain_rows={full_n:,}"
    )
    return arrays, full_n


def summarize(values: np.ndarray, reported_variances: np.ndarray, truth: float) -> dict[str, float]:
    errors = values - truth
    standard_errors = np.sqrt(np.maximum(reported_variances, 0.0))
    empirical_variance = float(np.var(values, ddof=1))
    mean_reported_variance = float(np.mean(reported_variances))
    return {
        "truth": float(truth),
        "mean_estimate": float(np.mean(values)),
        "bias": float(np.mean(errors)),
        "empirical_standard_error": float(np.std(values, ddof=1)),
        "mean_reported_standard_error": float(np.mean(standard_errors)),
        "rmse": float(np.sqrt(np.mean(errors**2))),
        "coverage_95": float(np.mean(np.abs(errors) <= 1.959963984540054 * standard_errors)),
        "empirical_variance": empirical_variance,
        "mean_reported_variance": mean_reported_variance,
        "reported_to_empirical_variance_ratio": (
            mean_reported_variance / empirical_variance if empirical_variance > 0 else math.nan
        ),
    }


def run_domain(
    pseudo_name: str,
    status: str,
    rng: np.random.Generator,
) -> list[tuple]:
    arrays, source_domain_n = pseudo_population(status, pseudo_name)
    pseudo_n = len(arrays["channel_count"])
    full_tail_n = spark.table(FRAME_TABLE).where(
        F.col("subscriber_status") == "sample_frame_lt10k"
    ).count()
    sample_fraction = min(1.0, int(SAMPLES["pps_expected_n"]) / full_tail_n)
    target_n = max(100, min(pseudo_n, int(round(pseudo_n * sample_fraction))))
    alpha = float(SAMPLES["selected_alpha"])
    size = arrays["pps_size"]
    size_total = float(np.sum(size))
    q = (
        alpha / pseudo_n + (1.0 - alpha) * size / size_total
        if size_total > 0
        else np.full(pseudo_n, 1.0 / pseudo_n)
    )
    _, pi = solve_capped_probabilities(q.tolist(), float(target_n))
    pi = np.asarray(pi, dtype=np.float64)
    replicates = int(SIMULATION["final_replicates"])
    outcome_names = [name for name in arrays if name != "pps_size"]
    truths = {name: float(np.sum(arrays[name])) for name in outcome_names}
    srs_estimates = {name: np.empty(replicates) for name in outcome_names}
    srs_variances = {name: np.empty(replicates) for name in outcome_names}
    pps_estimates = {name: np.empty(replicates) for name in outcome_names}
    pps_variances = {name: np.empty(replicates) for name in outcome_names}
    diagnostics = {
        "srs_effective_n": np.empty(replicates),
        "srs_weight_cv": np.empty(replicates),
        "srs_max_weight_share": np.empty(replicates),
        "pps_effective_n": np.empty(replicates),
        "pps_weight_cv": np.empty(replicates),
        "pps_max_weight_share": np.empty(replicates),
        "pps_realized_n": np.empty(replicates),
    }
    for replicate in range(replicates):
        srs_indices = rng.choice(pseudo_n, size=target_n, replace=False)
        pps_mask = rng.random(pseudo_n) < pi
        pps_weights = 1.0 / pi[pps_mask]
        diagnostics["srs_effective_n"][replicate] = target_n
        diagnostics["srs_weight_cv"][replicate] = 0.0
        diagnostics["srs_max_weight_share"][replicate] = 1.0 / target_n
        diagnostics["pps_realized_n"][replicate] = int(np.sum(pps_mask))
        diagnostics["pps_effective_n"][replicate] = (
            float(np.sum(pps_weights) ** 2 / np.sum(pps_weights**2)) if len(pps_weights) else 0.0
        )
        diagnostics["pps_weight_cv"][replicate] = (
            float(np.std(pps_weights) / np.mean(pps_weights)) if len(pps_weights) > 1 else 0.0
        )
        diagnostics["pps_max_weight_share"][replicate] = (
            float(np.max(pps_weights) / np.sum(pps_weights)) if len(pps_weights) else 0.0
        )
        for outcome in outcome_names:
            sample_values = arrays[outcome][srs_indices]
            sample_mean = float(np.mean(sample_values))
            sample_variance = float(np.var(sample_values, ddof=1))
            srs_estimates[outcome][replicate] = pseudo_n * sample_mean
            srs_variances[outcome][replicate] = (
                pseudo_n**2 * (1.0 - target_n / pseudo_n) * sample_variance / target_n
            )
            pps_values = arrays[outcome][pps_mask]
            pps_estimates[outcome][replicate] = float(np.sum(pps_values * pps_weights))
            pps_variances[outcome][replicate] = float(
                np.sum((1.0 - pi[pps_mask]) * pps_values**2 / pi[pps_mask] ** 2)
            )
    rows = []
    srs_empirical_variance: dict[str, float] = {}
    for design, estimates, variances in (
        ("srs", srs_estimates, srs_variances),
        ("poisson_pps", pps_estimates, pps_variances),
    ):
        for outcome in outcome_names:
            summary = summarize(estimates[outcome], variances[outcome], truths[outcome])
            if design == "srs":
                srs_empirical_variance[outcome] = summary["empirical_standard_error"] ** 2
            baseline = srs_empirical_variance.get(outcome)
            design_effect = (
                summary["empirical_standard_error"] ** 2 / baseline
                if baseline is not None and baseline > 0
                else math.nan
            )
            design_prefix = "srs" if design == "srs" else "pps"
            rows.append(
                (
                    DESIGN_VERSION,
                    pseudo_name,
                    int(source_domain_n),
                    int(pseudo_n),
                    int(target_n),
                    int(replicates),
                    design,
                    outcome,
                    "wald_normal",
                    summary["truth"],
                    summary["mean_estimate"],
                    summary["bias"],
                    summary["empirical_standard_error"],
                    summary["mean_reported_standard_error"],
                    summary["rmse"],
                    summary["coverage_95"],
                    summary["empirical_variance"],
                    summary["mean_reported_variance"],
                    summary["reported_to_empirical_variance_ratio"],
                    float(design_effect),
                    float(np.mean(diagnostics[f"{design_prefix}_effective_n"])),
                    float(np.mean(diagnostics[f"{design_prefix}_weight_cv"])),
                    float(np.mean(diagnostics[f"{design_prefix}_max_weight_share"])),
                    float(np.mean(diagnostics["pps_realized_n"])) if design == "poisson_pps" else float(target_n),
                    datetime.now(timezone.utc),
                )
            )
    return rows


if not spark.catalog.tableExists(FRAME_TABLE):
    raise RuntimeError(f"Required frame table does not exist: {FRAME_TABLE}")
rng = np.random.default_rng(int(SIMULATION["replicate_seed"]))
rows = []
rows.extend(run_domain("tail_lt10k", "sample_frame_lt10k", rng))
rows.extend(run_domain("head_ge10k", "census_ge10k", rng))
schema = (
    "design_version string, pseudo_population string, source_domain_n long, pseudo_population_n long, "
    "target_sample_n long, replicates long, design string, outcome string, interval_method string, "
    "truth double, mean_estimate double, "
    "bias double, empirical_standard_error double, mean_reported_standard_error double, rmse double, "
    "coverage_95 double, empirical_variance double, mean_reported_variance double, "
    "reported_to_empirical_variance_ratio double, design_effect_vs_srs double, "
    "mean_effective_n double, mean_weight_cv double, "
    "mean_max_weight_share double, mean_realized_n double, recorded_at timestamp"
)
write_table(
    spark.createDataFrame(rows, schema),
    OUTPUT_TABLE,
    "Five-thousand-replicate SRS and Poisson-PPS evaluation on frozen head and tail pseudo-populations.",
)
print("REPEATED-SAMPLE DESIGN EVALUATION: PASS")
print(f"REPLICATES: {int(SIMULATION['final_replicates']):,}")
print(f"OUTPUT TABLE: {OUTPUT_TABLE}")
dbutils.notebook.exit(json.dumps({"rows": len(rows), "output_table": OUTPUT_TABLE}, sort_keys=True))
