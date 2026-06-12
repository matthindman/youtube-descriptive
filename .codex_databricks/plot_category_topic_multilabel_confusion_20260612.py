# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-label Topic Confusion Matrix Plot

# COMMAND ----------
import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
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


_create_text_widget("run_id", "category_topic_multilabel_random_1000_20260612")
_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("output_prefix", "yt_category_topic_multilabel_1000")
_create_text_widget("provider", "gemini")
_create_text_widget("model", "gemini-3.5-flash")
_create_text_widget("prediction_variant", "prob_label_threshold_closure_postprocessed")
_create_text_widget("top_n_labels", "18")
_create_text_widget("analysis_output_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches/analysis")

RUN_ID = _get_widget("run_id", "category_topic_multilabel_random_1000_20260612")
CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_multilabel_1000")
PROVIDER = _get_widget("provider", "gemini")
MODEL = _get_widget("model", "gemini-3.5-flash")
PREDICTION_VARIANT = _get_widget("prediction_variant", "prob_label_threshold_closure_postprocessed")
TOP_N_LABELS = int(_get_widget("top_n_labels", "18"))
ANALYSIS_OUTPUT_DIR = _get_widget("analysis_output_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches/analysis").rstrip("/")


def table_ref(name: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in name.split("."))


channel_metrics = spark.table(table_ref(f"{CATALOG}.{SCHEMA}.{OUTPUT_PREFIX}_channel_metrics"))
rows = (
    channel_metrics
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("provider") == F.lit(PROVIDER))
    .where(F.col("model") == F.lit(MODEL))
    .where(F.col("prediction_variant") == F.lit(PREDICTION_VARIANT))
    .where(F.col("eval_split") == F.lit("heldout_test"))
    .select("channel_id", "reference_labels_json", "predicted_labels_json")
    .toPandas()
)
if rows.empty:
    raise RuntimeError("No channel_metrics rows found for requested model/variant.")

ref_counts = {}
for labels_json in rows["reference_labels_json"]:
    for label in json.loads(labels_json or "[]"):
        ref_counts[label] = ref_counts.get(label, 0) + 1
top_labels = [label for label, _ in sorted(ref_counts.items(), key=lambda x: (-x[1], x[0]))[:TOP_N_LABELS]]

matrix = pd.DataFrame(0, index=top_labels, columns=top_labels, dtype=int)
for _, row in rows.iterrows():
    refs = [x for x in json.loads(row["reference_labels_json"] or "[]") if x in matrix.index]
    preds = [x for x in json.loads(row["predicted_labels_json"] or "[]") if x in matrix.columns]
    for ref in refs:
        for pred in preds:
            matrix.loc[ref, pred] += 1

row_support = matrix.sum(axis=1).replace(0, np.nan)
row_norm = matrix.div(row_support, axis=0).fillna(0.0)

out_dir = os.path.join(ANALYSIS_OUTPUT_DIR, RUN_ID)
os.makedirs(out_dir, exist_ok=True)
csv_path = os.path.join(out_dir, f"confusion_matrix_{PROVIDER}_{MODEL}_{PREDICTION_VARIANT}.csv".replace("/", "_"))
png_path = os.path.join(out_dir, f"confusion_matrix_{PROVIDER}_{MODEL}_{PREDICTION_VARIANT}.png".replace("/", "_"))
matrix.to_csv(csv_path)

plt.figure(figsize=(13, 10))
sns.heatmap(
    row_norm,
    cmap="YlGnBu",
    vmin=0,
    vmax=max(0.15, float(row_norm.to_numpy().max())),
    linewidths=0.25,
    linecolor="white",
    cbar_kws={"label": "Share of true-label co-predictions"},
)
plt.title(f"Multi-label Confusion: {PROVIDER}:{MODEL} ({PREDICTION_VARIANT})")
plt.xlabel("Predicted label")
plt.ylabel("True held-out YouTube label")
plt.xticks(rotation=45, ha="right")
plt.yticks(rotation=0)
plt.tight_layout()
plt.savefig(png_path, dpi=180)
plt.close()

payload = {
    "run_id": RUN_ID,
    "provider": PROVIDER,
    "model": MODEL,
    "prediction_variant": PREDICTION_VARIANT,
    "top_n_labels": TOP_N_LABELS,
    "csv_path": csv_path,
    "png_path": png_path,
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True))
