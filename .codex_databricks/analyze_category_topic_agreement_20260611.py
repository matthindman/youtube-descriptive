# Databricks notebook source
# MAGIC %md
# MAGIC # Topic/Genre 1k Model Agreement, Confusion, and Disagreement Inspection
# MAGIC
# MAGIC **Deprecated for the current `topic_categories` target.** This analysis assumes one predicted label per
# MAGIC channel. Use `analyze_category_topic_multilabel_20260612.py` for multi-label set prediction metrics.

# COMMAND ----------
import json
import math
import os
from collections import Counter
from datetime import datetime, timezone

import matplotlib.pyplot as plt
import pandas as pd
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
_create_text_widget("run_id", "category_topic_random_1000_20260611")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")
_create_text_widget("analysis_output_dir", "/dbfs/FileStore/youtube_category_topic_batches/analysis")
_create_text_widget("top_disagreement_cases", "80")
_create_text_widget("top_confusion_labels", "24")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "category_topic_random_1000_20260611")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000")
ANALYSIS_OUTPUT_DIR = _get_widget("analysis_output_dir", "/dbfs/FileStore/youtube_category_topic_batches/analysis").rstrip("/")
TOP_DISAGREEMENT_CASES = int(_get_widget("top_disagreement_cases", "80"))
TOP_CONFUSION_LABELS = int(_get_widget("top_confusion_labels", "24"))


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def out_table(suffix: str) -> str:
    return fqtn(f"{OUTPUT_PREFIX}_{suffix}")


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


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


def slug_name(slug):
    if slug is None:
        return None
    return str(slug).replace("_", " ")


def compact_video_text(text, max_chars=900):
    if not text:
        return ""
    return " ".join(str(text).split())[:max_chars]


def normalize_topic_list(value):
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, str):
        if not value:
            return []
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                value = parsed
            else:
                return [value]
        except Exception:
            return [value]
    if isinstance(value, (list, tuple, set)):
        out = []
        for item in value:
            if item is None:
                continue
            try:
                if pd.isna(item):
                    continue
            except Exception:
                pass
            out.append(str(item))
        return out
    return []


run_analysis_dir = os.path.join(ANALYSIS_OUTPUT_DIR, RUN_ID)
os.makedirs(run_analysis_dir, exist_ok=True)

predictions_full = out_table("predictions")
prompt_inputs_full = out_table("prompt_inputs")
agreement_summary_full = out_table("agreement_summary")
model_pairwise_full = out_table("model_pairwise_agreement")
channel_agreement_full = out_table("channel_agreement")
confusion_model_full = out_table("confusion_matrix_model_reference_array")
confusion_consensus_full = out_table("confusion_matrix_consensus_reference_array")
disagreement_cases_full = out_table("disagreement_cases")
plot_artifacts_full = out_table("plot_artifacts")

print("Reading", predictions_full, prompt_inputs_full)

# COMMAND ----------
pred_spark = (
    spark.table(predictions_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .withColumn("model_label", F.concat_ws(":", F.col("provider"), F.col("model")))
)

prompt_spark = spark.table(prompt_inputs_full).where(F.col("run_id") == F.lit(RUN_ID))

model_summary = (
    spark.table(agreement_summary_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .withColumn("model_label", F.concat_ws(":", F.col("provider"), F.col("model")))
)

pred_cols = [
    "run_id", "channel_id", "channel_name", "provider", "model", "model_label",
    "category_id", "category_name", "valid_prediction", "agrees_any_topic", "agrees_primary",
    "has_reference_any", "has_reference_primary", "primary_topic_slug", "primary_topic_name",
    "topic_slugs", "confidence", "ambiguous", "rationale_short", "parse_error",
    "prediction_parse_error", "n_videos_in_prompt",
]
pred_pdf = pred_spark.select(*[c for c in pred_cols if c in pred_spark.columns]).toPandas()
prompt_pdf = (
    prompt_spark
    .select("channel_id", "channel_name", "topic_slugs", "primary_topic_slug", "primary_topic_name", "recent_videos_text", "n_videos_in_prompt")
    .toPandas()
)
summary_pdf = model_summary.toPandas()

if pred_pdf.empty:
    raise RuntimeError(f"No imported predictions found for run_id={RUN_ID}")

model_labels = sorted(pred_pdf["model_label"].dropna().unique().tolist())
print(f"Imported model outputs: {len(model_labels)}", model_labels)

# COMMAND ----------
# Pairwise prediction agreement across models.
pair_records = []
for model_a in model_labels:
    a = pred_pdf[pred_pdf["model_label"] == model_a][["channel_id", "category_id", "valid_prediction", "agrees_any_topic", "agrees_primary"]].copy()
    for model_b in model_labels:
        b = pred_pdf[pred_pdf["model_label"] == model_b][["channel_id", "category_id", "valid_prediction", "agrees_any_topic", "agrees_primary"]].copy()
        merged = a.merge(b, on="channel_id", suffixes=("_a", "_b"))
        both_valid = merged["valid_prediction_a"].fillna(False) & merged["valid_prediction_b"].fillna(False)
        n_overlap = int(len(merged))
        n_both_valid = int(both_valid.sum())
        n_same = int(((merged["category_id_a"] == merged["category_id_b"]) & both_valid).sum())
        n_both_correct_any = int((merged["agrees_any_topic_a"].fillna(False) & merged["agrees_any_topic_b"].fillna(False)).sum())
        n_one_correct_any = int((merged["agrees_any_topic_a"].fillna(False) ^ merged["agrees_any_topic_b"].fillna(False)).sum())
        pair_records.append({
            "run_id": RUN_ID,
            "model_a": model_a,
            "model_b": model_b,
            "n_channel_overlap": n_overlap,
            "n_both_valid": n_both_valid,
            "n_same_prediction": n_same,
            "prediction_agreement_rate": float(n_same / n_both_valid) if n_both_valid else None,
            "n_both_agree_any_topic": n_both_correct_any,
            "n_one_agrees_any_topic": n_one_correct_any,
            "both_agree_any_topic_rate": float(n_both_correct_any / n_overlap) if n_overlap else None,
            "one_agrees_any_topic_rate": float(n_one_correct_any / n_overlap) if n_overlap else None,
        })

pair_df = spark.createDataFrame(pd.DataFrame(pair_records))
write_run_scoped(pair_df, model_pairwise_full)

# COMMAND ----------
# Channel-level consensus and disagreement flags.
prompt_by_channel = {row["channel_id"]: row for _, row in prompt_pdf.iterrows()}

channel_records = []
for channel_id, group in pred_pdf.groupby("channel_id", dropna=False):
    group = group.copy()
    valid = group[group["valid_prediction"].fillna(False) & group["category_id"].notna()]
    prompt_row = prompt_by_channel.get(channel_id, {})
    topic_slugs = normalize_topic_list(prompt_row.get("topic_slugs"))
    primary = prompt_row.get("primary_topic_slug")
    primary_name = prompt_row.get("primary_topic_name")
    channel_name = prompt_row.get("channel_name") or (group["channel_name"].dropna().iloc[0] if group["channel_name"].notna().any() else None)

    counts = Counter(valid["category_id"].tolist())
    if counts:
        consensus_category_id, consensus_n = sorted(counts.items(), key=lambda kv: (-kv[1], str(kv[0])))[0]
    else:
        consensus_category_id, consensus_n = None, 0
    n_models = int(len(group))
    n_valid = int(len(valid))
    consensus_share = float(consensus_n / n_valid) if n_valid else None
    n_distinct = int(len(counts))
    n_agree_any = int(group["agrees_any_topic"].fillna(False).sum()) if "agrees_any_topic" in group.columns else 0
    n_agree_primary = int(group["agrees_primary"].fillna(False).sum()) if "agrees_primary" in group.columns else 0
    reference_has_consensus = consensus_category_id in topic_slugs if consensus_category_id else False
    consensus_matches_primary = consensus_category_id == primary if consensus_category_id and primary else False
    missing_reference = not bool(topic_slugs)

    if missing_reference:
        issue_type = "missing_reference_labels"
        severity = "medium"
        recommended_fix = "Exclude from agreement denominators until channel_category has a nonempty topic_categories array."
        applied_fix = "Excluded from agreement denominators and confusion matrices."
    elif reference_has_consensus and not consensus_matches_primary:
        issue_type = "primary_label_order_issue"
        severity = "medium"
        recommended_fix = "Use array-aware held-out agreement for headline metrics; keep primary-only agreement secondary."
        applied_fix = "Agreement summary uses array-aware any-topic matching as the headline metric."
    elif consensus_category_id and consensus_share is not None and consensus_share >= 0.67 and not reference_has_consensus:
        issue_type = "strong_model_consensus_disagrees_with_reference_array"
        severity = "high"
        recommended_fix = "Inspect channel evidence; if evidence supports consensus, add or correct this category in channel_category."
        applied_fix = "Flagged for manual reference-label audit; no source table mutation applied automatically."
    elif n_valid and n_agree_any == 0:
        issue_type = "all_models_disagree_with_reference"
        severity = "high"
        recommended_fix = "Inspect channel evidence and prompt context; likely reference-label or taxonomy granularity issue."
        applied_fix = "Flagged for manual audit; no source table mutation applied automatically."
    elif consensus_share is not None and consensus_share < 0.5:
        issue_type = "low_model_consensus_ambiguous"
        severity = "medium"
        recommended_fix = "Treat as ambiguous; avoid using this case for model ranking without manual adjudication."
        applied_fix = "Flagged as ambiguous in disagreement_cases."
    elif n_agree_any < n_valid:
        issue_type = "partial_model_disagreement"
        severity = "low"
        recommended_fix = "No pipeline fix required; inspect only if category-level errors cluster."
        applied_fix = "Retained in model agreement diagnostics."
    else:
        issue_type = "models_agree_with_reference"
        severity = "none"
        recommended_fix = "No action."
        applied_fix = "No action."

    model_predictions = []
    for _, row in group.sort_values(["provider", "model"]).iterrows():
        model_predictions.append({
            "model": row.get("model_label"),
            "prediction": row.get("category_id"),
            "agree_any": bool(row.get("agrees_any_topic")) if pd.notna(row.get("agrees_any_topic")) else False,
            "confidence": None if pd.isna(row.get("confidence")) else float(row.get("confidence")),
            "rationale": row.get("rationale_short"),
            "parse_error": row.get("parse_error") or row.get("prediction_parse_error"),
        })

    disagreement_score = (
        (2 if severity == "high" else 1 if severity == "medium" else 0)
        + (1 - (consensus_share or 0))
        + (1 if n_agree_any == 0 and n_valid else 0)
    )

    channel_records.append({
        "run_id": RUN_ID,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "primary_topic_slug": primary,
        "primary_topic_name": primary_name,
        "topic_slugs_json": json.dumps(topic_slugs, ensure_ascii=False),
        "n_models": n_models,
        "n_valid_predictions": n_valid,
        "n_distinct_predictions": n_distinct,
        "consensus_category_id": consensus_category_id,
        "consensus_category_name": slug_name(consensus_category_id),
        "consensus_n": int(consensus_n),
        "consensus_share": consensus_share,
        "reference_has_consensus": bool(reference_has_consensus),
        "consensus_matches_primary": bool(consensus_matches_primary),
        "n_models_agree_any_topic": n_agree_any,
        "n_models_agree_primary": n_agree_primary,
        "issue_type": issue_type,
        "severity": severity,
        "recommended_fix": recommended_fix,
        "applied_fix": applied_fix,
        "disagreement_score": float(disagreement_score),
        "recent_videos_excerpt": compact_video_text(prompt_row.get("recent_videos_text")),
        "model_predictions_json": json.dumps(model_predictions, ensure_ascii=False),
        "inspected_at_utc": datetime.now(timezone.utc).isoformat(),
    })

channel_pdf = pd.DataFrame(channel_records)
channel_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("primary_topic_slug", StringType(), True),
    StructField("primary_topic_name", StringType(), True),
    StructField("topic_slugs_json", StringType(), True),
    StructField("n_models", IntegerType(), True),
    StructField("n_valid_predictions", IntegerType(), True),
    StructField("n_distinct_predictions", IntegerType(), True),
    StructField("consensus_category_id", StringType(), True),
    StructField("consensus_category_name", StringType(), True),
    StructField("consensus_n", IntegerType(), True),
    StructField("consensus_share", DoubleType(), True),
    StructField("reference_has_consensus", BooleanType(), True),
    StructField("consensus_matches_primary", BooleanType(), True),
    StructField("n_models_agree_any_topic", IntegerType(), True),
    StructField("n_models_agree_primary", IntegerType(), True),
    StructField("issue_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("recommended_fix", StringType(), True),
    StructField("applied_fix", StringType(), True),
    StructField("disagreement_score", DoubleType(), True),
    StructField("recent_videos_excerpt", StringType(), True),
    StructField("model_predictions_json", StringType(), True),
    StructField("inspected_at_utc", StringType(), True),
])
channel_df = spark.createDataFrame(channel_pdf, schema=channel_schema)
write_run_scoped(channel_df, channel_agreement_full)

disagreement_pdf = (
    channel_pdf[channel_pdf["issue_type"] != "models_agree_with_reference"]
    .sort_values(["disagreement_score", "consensus_n", "n_models_agree_any_topic"], ascending=[False, False, True])
    .head(TOP_DISAGREEMENT_CASES)
)
disagreement_df = spark.createDataFrame(disagreement_pdf, schema=channel_schema)
write_run_scoped(disagreement_df, disagreement_cases_full)

# COMMAND ----------
# Confusion matrices.
pred_for_confusion = (
    pred_spark
    .withColumn("reference_topic_slug", F.explode_outer("topic_slugs"))
    .where(F.col("valid_prediction") & F.col("reference_topic_slug").isNotNull())
    .withColumn("reference_topic_name", F.regexp_replace(F.col("reference_topic_slug"), "_", " "))
)

conf_model = (
    pred_for_confusion
    .groupBy(
        "run_id",
        "provider",
        "model",
        F.col("reference_topic_slug").alias("true_category_id"),
        F.col("reference_topic_name").alias("true_category_name"),
        F.col("category_id").alias("predicted_category_id"),
        F.col("category_name").alias("predicted_category_name"),
    )
    .agg(
        F.count("*").alias("n"),
        F.sum(F.when(F.col("category_id") == F.col("reference_topic_slug"), 1).otherwise(0)).alias("n_agree_primary"),
        F.sum(F.when(F.col("agrees_any_topic"), 1).otherwise(0)).alias("n_agree_any_topic"),
    )
)
write_run_scoped(conf_model, confusion_model_full)

consensus_spark = (
    spark.createDataFrame(channel_pdf, schema=channel_schema)
    .withColumn("reference_topic_slug", F.explode_outer(F.from_json("topic_slugs_json", ArrayType(StringType()))))
    .withColumn("reference_topic_name", F.regexp_replace(F.col("reference_topic_slug"), "_", " "))
)
conf_consensus = (
    consensus_spark
    .where(F.col("reference_topic_slug").isNotNull() & F.col("consensus_category_id").isNotNull())
    .groupBy(
        "run_id",
        F.col("reference_topic_slug").alias("true_category_id"),
        F.col("reference_topic_name").alias("true_category_name"),
        F.col("consensus_category_id").alias("predicted_category_id"),
        F.col("consensus_category_name").alias("predicted_category_name"),
        "reference_has_consensus",
        "consensus_matches_primary",
    )
    .agg(F.count("*").alias("n"))
)
write_run_scoped(conf_consensus, confusion_consensus_full)

# COMMAND ----------
# Plots.
plot_records = []

pair_pdf = pd.DataFrame(pair_records)
pair_matrix = pair_pdf.pivot(index="model_a", columns="model_b", values="prediction_agreement_rate").loc[model_labels, model_labels]
fig, ax = plt.subplots(figsize=(max(7, len(model_labels) * 0.85), max(6, len(model_labels) * 0.75)))
im = ax.imshow(pair_matrix.values, vmin=0, vmax=1, cmap="viridis")
ax.set_xticks(range(len(model_labels)))
ax.set_xticklabels(model_labels, rotation=45, ha="right", fontsize=8)
ax.set_yticks(range(len(model_labels)))
ax.set_yticklabels(model_labels, fontsize=8)
ax.set_title("Model Prediction Agreement")
for i in range(len(model_labels)):
    for j in range(len(model_labels)):
        val = pair_matrix.iloc[i, j]
        ax.text(j, i, "" if pd.isna(val) else f"{val:.2f}", ha="center", va="center", color="white" if val < 0.65 else "black", fontsize=7)
fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
fig.tight_layout()
path = os.path.join(run_analysis_dir, "model_pairwise_agreement_heatmap.png")
fig.savefig(path, dpi=180)
plt.close(fig)
plot_records.append({"run_id": RUN_ID, "plot_name": "model_pairwise_agreement_heatmap", "plot_path": path, "created_at_utc": datetime.now(timezone.utc).isoformat()})

summary_plot = summary_pdf.sort_values("agreement_any_topic_strict", ascending=True)
fig, ax = plt.subplots(figsize=(9, max(4, 0.45 * len(summary_plot))))
labels = (summary_plot["provider"].astype(str) + ":" + summary_plot["model"].astype(str)).tolist()
values = summary_plot["agreement_any_topic_strict"].astype(float).tolist()
ax.barh(labels, values, color="#3b82f6")
ax.set_xlim(0, 1)
ax.set_xlabel("Strict agreement with any held-out topic label")
ax.set_title("Model Agreement With Held-Out Category Array")
for y, v in enumerate(values):
    ax.text(v + 0.01, y, f"{v:.1%}", va="center", fontsize=8)
fig.tight_layout()
path = os.path.join(run_analysis_dir, "model_agreement_bar.png")
fig.savefig(path, dpi=180)
plt.close(fig)
plot_records.append({"run_id": RUN_ID, "plot_name": "model_agreement_bar", "plot_path": path, "created_at_utc": datetime.now(timezone.utc).isoformat()})

consensus_conf_pdf = conf_consensus.toPandas()
if not consensus_conf_pdf.empty:
    top_true = (
        consensus_conf_pdf.groupby("true_category_id")["n"].sum().sort_values(ascending=False).head(TOP_CONFUSION_LABELS).index.tolist()
    )
    top_pred = (
        consensus_conf_pdf.groupby("predicted_category_id")["n"].sum().sort_values(ascending=False).head(TOP_CONFUSION_LABELS).index.tolist()
    )
    top_labels = sorted(set(top_true) | set(top_pred))
    cm = (
        consensus_conf_pdf[consensus_conf_pdf["true_category_id"].isin(top_labels) & consensus_conf_pdf["predicted_category_id"].isin(top_labels)]
        .pivot_table(index="true_category_id", columns="predicted_category_id", values="n", aggfunc="sum", fill_value=0)
        .reindex(index=top_labels, columns=top_labels, fill_value=0)
    )
    fig, ax = plt.subplots(figsize=(max(10, len(top_labels) * 0.42), max(9, len(top_labels) * 0.38)))
    im = ax.imshow(cm.values, cmap="magma")
    ax.set_xticks(range(len(top_labels)))
    ax.set_xticklabels(top_labels, rotation=60, ha="right", fontsize=7)
    ax.set_yticks(range(len(top_labels)))
    ax.set_yticklabels(top_labels, fontsize=7)
    ax.set_xlabel("Consensus estimated category")
    ax.set_ylabel("Held-out category label, exploded from API array")
    ax.set_title("Consensus Confusion Matrix, Exploded Reference Labels")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    path = os.path.join(run_analysis_dir, "consensus_confusion_matrix_top_labels.png")
    fig.savefig(path, dpi=180)
    plt.close(fig)
    plot_records.append({"run_id": RUN_ID, "plot_name": "consensus_confusion_matrix_top_labels", "plot_path": path, "created_at_utc": datetime.now(timezone.utc).isoformat()})

issue_counts = channel_pdf["issue_type"].value_counts().sort_values(ascending=True)
fig, ax = plt.subplots(figsize=(10, max(4, 0.35 * len(issue_counts))))
ax.barh(issue_counts.index.tolist(), issue_counts.values.tolist(), color="#10b981")
ax.set_xlabel("Channels")
ax.set_title("Disagreement Inspection Flags")
for y, v in enumerate(issue_counts.values.tolist()):
    ax.text(v + 1, y, str(v), va="center", fontsize=8)
fig.tight_layout()
path = os.path.join(run_analysis_dir, "disagreement_issue_counts.png")
fig.savefig(path, dpi=180)
plt.close(fig)
plot_records.append({"run_id": RUN_ID, "plot_name": "disagreement_issue_counts", "plot_path": path, "created_at_utc": datetime.now(timezone.utc).isoformat()})

plot_df = spark.createDataFrame(pd.DataFrame(plot_records))
write_run_scoped(plot_df, plot_artifacts_full)

# COMMAND ----------
issue_summary = (
    channel_df
    .groupBy("run_id", "issue_type", "severity", "recommended_fix", "applied_fix")
    .agg(F.count("*").alias("n_channels"))
    .orderBy(F.desc("n_channels"))
)
display(issue_summary)

top_cases = disagreement_df.orderBy(F.desc("disagreement_score"), F.desc("consensus_n"), "channel_id")
display(top_cases.select(
    "channel_id",
    "channel_name",
    "primary_topic_slug",
    "topic_slugs_json",
    "consensus_category_id",
    "consensus_n",
    "n_valid_predictions",
    "n_models_agree_any_topic",
    "issue_type",
    "recommended_fix",
    "recent_videos_excerpt",
))

payload = {
    "run_id": RUN_ID,
    "models_imported": model_labels,
    "tables": {
        "model_pairwise_agreement": model_pairwise_full,
        "channel_agreement": channel_agreement_full,
        "confusion_matrix_model": confusion_model_full,
        "confusion_matrix_consensus": confusion_consensus_full,
        "disagreement_cases": disagreement_cases_full,
        "plot_artifacts": plot_artifacts_full,
    },
    "plots": plot_records,
    "issue_counts": channel_pdf["issue_type"].value_counts().to_dict(),
    "top_disagreement_cases": [
        {
            "channel_id": r["channel_id"],
            "channel_name": r["channel_name"],
            "primary_topic_slug": r["primary_topic_slug"],
            "topic_slugs_json": r["topic_slugs_json"],
            "consensus_category_id": r["consensus_category_id"],
            "consensus_n": int(r["consensus_n"]),
            "n_valid_predictions": int(r["n_valid_predictions"]),
            "n_models_agree_any_topic": int(r["n_models_agree_any_topic"]),
            "issue_type": r["issue_type"],
            "recommended_fix": r["recommended_fix"],
            "recent_videos_excerpt": r["recent_videos_excerpt"],
        }
        for _, r in disagreement_pdf.head(15).iterrows()
    ],
}
dbutils.notebook.exit(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
