# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube Topic Categories: Multi-label Validation Analysis
# MAGIC
# MAGIC Reads the multi-label evaluation tables, writes diagnostic tables, and renders summary plots for
# MAGIC heldout exact-label prediction performance.

# COMMAND ----------
import json
import os
from datetime import datetime, timezone

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F


def _create_text_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default, name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return default


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "category_topic_multilabel_random_1000_20260612")
_create_text_widget("output_prefix", "yt_category_topic_multilabel_1000")
_create_text_widget("analysis_output_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches/analysis")
_create_text_widget("preferred_prediction_variant", "prob_label_threshold")
_create_text_widget("top_error_cases", "100")
_create_text_widget("top_label_count", "35")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "category_topic_multilabel_random_1000_20260612")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_multilabel_1000")
ANALYSIS_OUTPUT_DIR = _get_widget("analysis_output_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches/analysis").rstrip("/")
PREFERRED_VARIANT = _get_widget("preferred_prediction_variant", "prob_label_threshold")
TOP_ERROR_CASES = int(_get_widget("top_error_cases", "100"))
TOP_LABEL_COUNT = int(_get_widget("top_label_count", "35"))


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def out_table(suffix: str) -> str:
    return fqtn(f"{OUTPUT_PREFIX}_{suffix}")


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def write_run_scoped(df, table_full: str):
    if "run_id" not in df.columns:
        df = df.withColumn("run_id", F.lit(RUN_ID))
    if not _table_exists_full(table_full):
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("run_id").saveAsTable(table_full)
        return
    existing = spark.table(table_full)
    for field in existing.schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    df = df.select(*existing.columns)
    spark.sql(f"DELETE FROM {table_full} WHERE run_id = {_sql_string(RUN_ID)}")
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_full)


model_metrics_full = out_table("model_metrics")
label_metrics_full = out_table("label_metrics")
channel_metrics_full = out_table("channel_metrics")
baselines_full = out_table("baselines")
pairwise_full = out_table("model_pairwise_set_agreement")
plot_artifacts_full = out_table("plot_artifacts")
error_cases_full = out_table("high_error_cases")

run_analysis_dir = os.path.join(ANALYSIS_OUTPUT_DIR, RUN_ID)
os.makedirs(run_analysis_dir, exist_ok=True)

# COMMAND ----------
model_pdf = (
    spark.table(model_metrics_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("eval_split") == F.lit("heldout_test"))
    .toPandas()
)
label_pdf = (
    spark.table(label_metrics_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("eval_split") == F.lit("heldout_test"))
    .where(F.col("prediction_variant") == F.lit(PREFERRED_VARIANT))
    .toPandas()
)
channel_pdf = (
    spark.table(channel_metrics_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("eval_split") == F.lit("heldout_test"))
    .where(F.col("prediction_variant") == F.lit(PREFERRED_VARIANT))
    .toPandas()
)
baseline_pdf = (
    spark.table(baselines_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("eval_split") == F.lit("heldout_test"))
    .toPandas()
)
pair_pdf = (
    spark.table(pairwise_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("eval_split") == F.lit("heldout_test"))
    .where(F.col("prediction_variant") == F.lit(PREFERRED_VARIANT))
    .toPandas()
)

if model_pdf.empty:
    raise RuntimeError(f"No model metrics found for run_id={RUN_ID}")

model_pdf["model_label"] = model_pdf["provider"] + ":" + model_pdf["model"]
label_pdf["model_label"] = label_pdf["provider"] + ":" + label_pdf["model"]
channel_pdf["model_label"] = channel_pdf["provider"] + ":" + channel_pdf["model"]

# COMMAND ----------
plot_records = []


def save_plot(path: str, title: str, description: str):
    plot_records.append({
        "run_id": RUN_ID,
        "plot_path": path,
        "title": title,
        "description": description,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    })


preferred_model_pdf = (
    model_pdf[model_pdf["prediction_variant"] == PREFERRED_VARIANT]
    .sort_values(["micro_f1", "mean_jaccard_similarity"], ascending=[False, False])
)
if not preferred_model_pdf.empty:
    fig, ax = plt.subplots(figsize=(11, max(5, 0.42 * len(preferred_model_pdf))))
    y = range(len(preferred_model_pdf))
    ax.barh(y, preferred_model_pdf["micro_f1"], color="#2f6f8f", label="Micro F1")
    ax.scatter(preferred_model_pdf["mean_jaccard_similarity"], y, color="#c75146", label="Mean Jaccard", zorder=3)
    ax.set_yticks(list(y))
    ax.set_yticklabels(preferred_model_pdf["model_label"])
    ax.invert_yaxis()
    ax.set_xlim(0, 1)
    ax.set_xlabel("Heldout score")
    ax.set_title(f"Multi-label YouTube topic prediction ({PREFERRED_VARIANT})")
    ax.legend(loc="lower right")
    fig.tight_layout()
    path = os.path.join(run_analysis_dir, "heldout_model_metrics_bar.png")
    fig.savefig(path, dpi=180)
    plt.close(fig)
    save_plot(path, "Heldout model metrics", "Micro F1 bars and mean Jaccard points for the preferred prediction variant.")

variant_pdf = (
    model_pdf.groupby("prediction_variant", as_index=False)
    .agg(median_micro_f1=("micro_f1", "median"), median_jaccard=("mean_jaccard_similarity", "median"), median_exact=("exact_set_match_rate", "median"))
    .sort_values("median_micro_f1", ascending=False)
)
fig, ax = plt.subplots(figsize=(9, 4.8))
ax.bar(variant_pdf["prediction_variant"], variant_pdf["median_micro_f1"], color="#4f7cac")
ax.scatter(variant_pdf["prediction_variant"], variant_pdf["median_jaccard"], color="#dd8a3d", label="Median Jaccard", zorder=3)
ax.scatter(variant_pdf["prediction_variant"], variant_pdf["median_exact"], color="#4d9f5b", label="Median exact set match", zorder=3)
ax.set_ylim(0, 1)
ax.set_ylabel("Heldout median across models")
ax.set_title("Prediction variant comparison")
ax.tick_params(axis="x", rotation=25)
ax.legend()
fig.tight_layout()
path = os.path.join(run_analysis_dir, "prediction_variant_comparison.png")
fig.savefig(path, dpi=180)
plt.close(fig)
save_plot(path, "Prediction variant comparison", "Median heldout scores across models for raw, thresholded, and post-processed variants.")

if not pair_pdf.empty:
    pivot = pair_pdf.pivot(index="model_a", columns="model_b", values="mean_jaccard_between_model_sets")
    fig, ax = plt.subplots(figsize=(max(7, 0.65 * len(pivot.columns)), max(6, 0.55 * len(pivot.index))))
    im = ax.imshow(pivot.values, vmin=0, vmax=1, cmap="viridis")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title(f"Model predicted-set Jaccard ({PREFERRED_VARIANT})")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    path = os.path.join(run_analysis_dir, "model_pairwise_predicted_set_jaccard.png")
    fig.savefig(path, dpi=180)
    plt.close(fig)
    save_plot(path, "Pairwise model set agreement", "Mean Jaccard similarity between model predicted label sets on heldout channels.")

if not label_pdf.empty:
    top_labels = (
        label_pdf.groupby("label_id", as_index=False)["support"].max()
        .sort_values("support", ascending=False)
        .head(TOP_LABEL_COUNT)["label_id"].tolist()
    )
    label_top = label_pdf[label_pdf["label_id"].isin(top_labels)].copy()
    label_top["label_short"] = label_top["label_id"].str.replace("_", " ", regex=False)
    pivot = label_top.pivot_table(index="label_short", columns="model_label", values="f1", aggfunc="mean").fillna(0.0)
    pivot = pivot.loc[[x.replace("_", " ") for x in top_labels if x.replace("_", " ") in pivot.index]]
    fig, ax = plt.subplots(figsize=(max(8, 0.7 * len(pivot.columns)), max(7, 0.32 * len(pivot.index))))
    im = ax.imshow(pivot.values, vmin=0, vmax=1, cmap="magma")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title(f"Per-label F1, top {len(pivot.index)} labels by support")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    path = os.path.join(run_analysis_dir, "label_f1_heatmap_top_labels.png")
    fig.savefig(path, dpi=180)
    plt.close(fig)
    save_plot(path, "Label F1 heatmap", "Per-label F1 for common heldout labels by model.")

if not channel_pdf.empty:
    fig, ax = plt.subplots(figsize=(9, 5.2))
    channel_pdf["label_cardinality_error"].hist(ax=ax, bins=range(int(channel_pdf["label_cardinality_error"].min()) - 1, int(channel_pdf["label_cardinality_error"].max()) + 2), color="#6c7a89")
    ax.set_title(f"Predicted minus reference label count ({PREFERRED_VARIANT})")
    ax.set_xlabel("Label cardinality error")
    ax.set_ylabel("Model-channel rows")
    fig.tight_layout()
    path = os.path.join(run_analysis_dir, "label_cardinality_error_histogram.png")
    fig.savefig(path, dpi=180)
    plt.close(fig)
    save_plot(path, "Label cardinality error", "Histogram of predicted label count minus exact YouTube label count.")

# COMMAND ----------
error_cases = (
    channel_pdf.sort_values(["jaccard_similarity", "n_false_negative_labels", "n_false_positive_labels"], ascending=[True, False, False])
    .head(TOP_ERROR_CASES)
    .copy()
)
error_cases["run_id"] = RUN_ID
error_cases_df = spark.createDataFrame(error_cases)
write_run_scoped(error_cases_df, error_cases_full)

plot_df = spark.createDataFrame(pd.DataFrame(plot_records))
write_run_scoped(plot_df, plot_artifacts_full)

display(preferred_model_pdf)
display(baseline_pdf.sort_values("micro_f1", ascending=False))
display(error_cases.head(25))

payload = {
    "run_id": RUN_ID,
    "preferred_prediction_variant": PREFERRED_VARIANT,
    "plot_artifacts_table": plot_artifacts_full,
    "high_error_cases_table": error_cases_full,
    "plots": plot_records,
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True, default=str))
