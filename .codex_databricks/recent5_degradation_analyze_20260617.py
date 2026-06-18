# Databricks notebook source
"""Analyze and visualize recent-5 vs recent-10 LID degradation results."""

from __future__ import annotations

import json
import math
import os
import random
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "dev_sean")
dbutils.widgets.text("schema", "matt")
dbutils.widgets.text("baseline_run_id", "too_full_20260609")
dbutils.widgets.text("deepseek_run_id", "too_full_20260609_recent5_degradation_deepseek_20260617")
dbutils.widgets.text("output_prefix", "yt_lid_v3_recent5_degradation_20260617")
dbutils.widgets.text("analysis_table", "yt_lid_v3_recent5_degradation_20260617_channel_analysis")
dbutils.widgets.text("disagreement_table", "yt_lid_v3_recent5_degradation_20260617_disagreement_channels")
dbutils.widgets.text("deepseek_verdicts_table", "yt_lid_v3_recent5_degradation_20260617_llm_verdicts")
dbutils.widgets.text("deepseek_raw_results_table", "yt_lid_v3_recent5_degradation_20260617_llm_raw_results")
dbutils.widgets.text("dbfs_output_dir", "/dbfs/FileStore/youtube_lid_recent5_degradation_20260617")


CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BASELINE_RUN_ID = dbutils.widgets.get("baseline_run_id")
DEEPSEEK_RUN_ID = dbutils.widgets.get("deepseek_run_id")
OUTPUT_PREFIX = dbutils.widgets.get("output_prefix")
ANALYSIS_TABLE = dbutils.widgets.get("analysis_table")
DISAGREEMENT_TABLE = dbutils.widgets.get("disagreement_table")
DEEPSEEK_VERDICTS_TABLE = dbutils.widgets.get("deepseek_verdicts_table")
DEEPSEEK_RAW_RESULTS_TABLE = dbutils.widgets.get("deepseek_raw_results_table")
DBFS_OUTPUT_DIR = dbutils.widgets.get("dbfs_output_dir")


def fqtn(table_name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"


def table_exists(table_name: str) -> bool:
    try:
        spark.table(fqtn(table_name)).limit(1).count()
        return True
    except Exception:
        return False


def col_or_null(df, name: str):
    return F.col(name) if name in df.columns else F.lit(None)


def first_existing_col(df, *names: str):
    for name in names:
        if name in df.columns:
            return F.col(name)
    return F.lit(None)


analysis = spark.table(fqtn(ANALYSIS_TABLE))
if "degradation_disagreement_reason" in analysis.columns:
    analysis = analysis.withColumn(
        "degradation_disagreement_reason",
        F.when(F.length(F.trim(F.col("degradation_disagreement_reason"))) > 0, F.col("degradation_disagreement_reason")),
    )
disagreements = spark.table(fqtn(DISAGREEMENT_TABLE)).select("channel_id", "degradation_disagreement_reason")

if table_exists(DEEPSEEK_VERDICTS_TABLE):
    deepseek_source = spark.table(fqtn(DEEPSEEK_VERDICTS_TABLE)).where(F.col("run_id") == F.lit(DEEPSEEK_RUN_ID))
    deepseek_cols = [
        "channel_id",
        col_or_null(deepseek_source, "panel_status").alias("deepseek_panel_status"),
        col_or_null(deepseek_source, "panel_language_label").alias("deepseek_language_label"),
        col_or_null(deepseek_source, "panel_language_iso639_3").alias("deepseek_iso639_3"),
        F.coalesce(
            col_or_null(deepseek_source, "panel_normalized_language_iso639_3"),
            col_or_null(deepseek_source, "panel_language_iso639_3"),
        ).alias("deepseek_base_iso"),
        first_existing_col(deepseek_source, "panel_votes_for_winner", "n_votes").alias("deepseek_votes_for_winner"),
        first_existing_col(deepseek_source, "panel_models_reached", "n_votes").alias("deepseek_models_reached"),
    ]
    deepseek_verdicts = (
        deepseek_source.select(*deepseek_cols)
    )
else:
    deepseek_verdicts = spark.createDataFrame([], "channel_id string, deepseek_panel_status string, deepseek_language_label string, deepseek_iso639_3 string, deepseek_base_iso string, deepseek_votes_for_winner long, deepseek_models_reached long")

if table_exists(DEEPSEEK_RAW_RESULTS_TABLE):
    raw = spark.table(fqtn(DEEPSEEK_RAW_RESULTS_TABLE)).where(F.col("run_id") == F.lit(DEEPSEEK_RUN_ID))
    if "parse_ok" in raw.columns:
        parse_ok_expr = F.col("parse_ok") == F.lit(True)
        parse_error_expr = F.col("parse_ok") == F.lit(False)
    elif "parse_error" in raw.columns:
        parse_ok_expr = F.col("parse_error").isNull()
        parse_error_expr = F.col("parse_error").isNotNull()
    elif "prediction_parse_error" in raw.columns:
        parse_ok_expr = F.col("prediction_parse_error").isNull()
        parse_error_expr = F.col("prediction_parse_error").isNotNull()
    else:
        parse_ok_expr = F.lit(None).cast("boolean")
        parse_error_expr = F.lit(None).cast("boolean")
    raw_summary = raw.groupBy("channel_id").agg(
        F.count(F.lit(1)).alias("deepseek_raw_rows"),
        F.sum(F.when(parse_ok_expr, 1).otherwise(0)).alias("deepseek_parse_ok_rows"),
        F.sum(F.when(parse_error_expr, 1).otherwise(0)).alias("deepseek_parse_error_rows"),
    )
else:
    raw_summary = spark.createDataFrame([], "channel_id string, deepseek_raw_rows long, deepseek_parse_ok_rows long, deepseek_parse_error_rows long")

analysis_with_routes = analysis
if "degradation_disagreement_reason" not in analysis.columns:
    analysis_with_routes = analysis.join(disagreements, on="channel_id", how="left")

final = (
    analysis_with_routes
    .join(deepseek_verdicts, on="channel_id", how="left")
    .join(raw_summary, on="channel_id", how="left")
    .withColumn(
        "baseline10_matches_llm",
        F.coalesce(
            F.col("llm_panel_base_iso").isNotNull() & (F.col("baseline10_consensus_base_iso") == F.col("llm_panel_base_iso")),
            F.lit(False),
        ),
    )
    .withColumn(
        "recent5_matches_llm",
        F.coalesce(
            F.col("llm_panel_base_iso").isNotNull() & (F.col("recent5_consensus_base_iso") == F.col("llm_panel_base_iso")),
            F.lit(False),
        ),
    )
    .withColumn(
        "baseline10_matches_deepseek",
        F.coalesce(
            F.col("deepseek_base_iso").isNotNull() & (F.col("baseline10_consensus_base_iso") == F.col("deepseek_base_iso")),
            F.lit(False),
        ),
    )
    .withColumn(
        "recent5_matches_deepseek",
        F.coalesce(
            F.col("deepseek_base_iso").isNotNull() & (F.col("recent5_consensus_base_iso") == F.col("deepseek_base_iso")),
            F.lit(False),
        ),
    )
    .withColumn(
        "deepseek_adjudication_outcome",
        F.when(F.col("deepseek_base_iso").isNull(), F.lit("DeepSeek insufficient"))
        .when(F.col("baseline10_matches_deepseek") & F.col("recent5_matches_deepseek"), F.lit("both match"))
        .when(F.col("baseline10_matches_deepseek") & ~F.col("recent5_matches_deepseek"), F.lit("10 matches DeepSeek only"))
        .when(~F.col("baseline10_matches_deepseek") & F.col("recent5_matches_deepseek"), F.lit("5 matches DeepSeek only"))
        .otherwise(F.lit("neither matches")),
    )
    .withColumn(
        "paired_change_class",
        F.when(F.col("baseline10_matches_llm") & F.col("recent5_matches_llm"), F.lit("stable_correct"))
        .when(F.col("baseline10_matches_llm") & ~F.col("recent5_matches_llm"), F.lit("degraded"))
        .when(~F.col("baseline10_matches_llm") & F.col("recent5_matches_llm"), F.lit("improved"))
        .when(F.col("llm_panel_base_iso").isNull(), F.lit("no_llm_reference"))
        .otherwise(F.lit("stable_mismatch")),
    )
)

final_table = f"{OUTPUT_PREFIX}_channel_analysis_final"
final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqtn(final_table))

pdf = final.select(
    "channel_id",
    "baseline10_consensus_base_iso",
    "recent5_consensus_base_iso",
    "baseline10_consensus_status",
    "recent5_consensus_status",
    "llm_panel_base_iso",
    "baseline10_matches_llm",
    "recent5_matches_llm",
    "paired_change_class",
    "degradation_disagreement_reason",
    "deepseek_base_iso",
    "deepseek_adjudication_outcome",
).toPandas()

ref = pdf[pdf["llm_panel_base_iso"].notna()].copy()
if len(ref) == 0:
    raise ValueError("No panel-majority LLM reference rows available for degradation metrics.")

acc10 = float(ref["baseline10_matches_llm"].mean())
acc5 = float(ref["recent5_matches_llm"].mean())
delta = acc5 - acc10

rng = random.Random(20260617)
boot = []
idx = list(range(len(ref)))
for _ in range(2000):
    sample_idx = [rng.choice(idx) for _ in idx]
    s = ref.iloc[sample_idx]
    boot.append(float(s["recent5_matches_llm"].mean() - s["baseline10_matches_llm"].mean()))
boot.sort()
ci_low = boot[int(0.025 * (len(boot) - 1))]
ci_high = boot[int(0.975 * (len(boot) - 1))]

b10 = ref["baseline10_matches_llm"].astype(bool)
b5 = ref["recent5_matches_llm"].astype(bool)
paired_counts = {
    "both_match": int((b10 & b5).sum()),
    "baseline10_only": int((b10 & ~b5).sum()),
    "recent5_only": int((~b10 & b5).sum()),
    "neither": int((~b10 & ~b5).sum()),
}
discordant = paired_counts["baseline10_only"] + paired_counts["recent5_only"]
sign_test_two_sided_p = None
if discordant:
    k = min(paired_counts["baseline10_only"], paired_counts["recent5_only"])
    cdf = sum(math.comb(discordant, i) * (0.5 ** discordant) for i in range(k + 1))
    sign_test_two_sided_p = min(1.0, 2.0 * cdf)

summary_rows = [
    ("sample_channels", len(pdf)),
    ("llm_reference_channels", len(ref)),
    ("baseline10_agreement_with_llm", acc10),
    ("recent5_agreement_with_llm", acc5),
    ("paired_delta_recent5_minus_baseline10", delta),
    ("paired_delta_bootstrap_ci_low", ci_low),
    ("paired_delta_bootstrap_ci_high", ci_high),
    ("sign_test_two_sided_p", sign_test_two_sided_p),
    *paired_counts.items(),
    ("disagreement_channels", int(pdf["degradation_disagreement_reason"].fillna("").str.len().gt(0).sum())),
    ("deepseek_adjudicated_channels", int(pdf["deepseek_base_iso"].notna().sum())),
]
summary_df = spark.createDataFrame([(k, None if v is None else str(v)) for k, v in summary_rows], ["metric", "value"])
summary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fqtn(f"{OUTPUT_PREFIX}_summary_metrics")
)

transition = (
    final.groupBy("baseline10_consensus_base_iso", "recent5_consensus_base_iso")
    .count()
    .orderBy(F.desc("count"))
)
transition.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fqtn(f"{OUTPUT_PREFIX}_transition_matrix")
)

language_delta = (
    final.where(F.col("llm_panel_base_iso").isNotNull())
    .groupBy("llm_panel_base_iso")
    .agg(
        F.count(F.lit(1)).alias("n"),
        F.avg(F.col("baseline10_matches_llm").cast("double")).alias("baseline10_agreement"),
        F.avg(F.col("recent5_matches_llm").cast("double")).alias("recent5_agreement"),
    )
    .withColumn("delta_recent5_minus_baseline10", F.col("recent5_agreement") - F.col("baseline10_agreement"))
    .where(F.col("n") >= 10)
    .orderBy(F.asc("delta_recent5_minus_baseline10"), F.desc("n"))
)
language_delta.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fqtn(f"{OUTPUT_PREFIX}_language_delta")
)

deepseek_summary = (
    final.where(F.length(F.trim(F.col("degradation_disagreement_reason"))) > 0)
    .groupBy("degradation_disagreement_reason", "deepseek_adjudication_outcome")
    .count()
    .orderBy("degradation_disagreement_reason", F.desc("count"))
)
deepseek_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    fqtn(f"{OUTPUT_PREFIX}_deepseek_adjudication_summary")
)

out = Path(DBFS_OUTPUT_DIR)
out.mkdir(parents=True, exist_ok=True)

plot_payload = {
    "summary_metrics": {k: v for k, v in summary_rows},
    "paired_counts": paired_counts,
    "change_counts": pdf["paired_change_class"].value_counts(dropna=False).to_dict(),
    "transition_matrix": transition.toPandas().to_dict(orient="records"),
    "language_delta": language_delta.toPandas().to_dict(orient="records"),
    "deepseek_adjudication_summary": deepseek_summary.toPandas().to_dict(orient="records"),
}
with (out / "visual_data.json").open("w") as f:
    json.dump(plot_payload, f, indent=2, sort_keys=True, default=str)

for name, frame in {
    "channel_analysis_final.csv": pdf,
    "transition_matrix.csv": transition.toPandas(),
    "language_delta.csv": language_delta.toPandas(),
    "deepseek_adjudication_summary.csv": deepseek_summary.toPandas(),
}.items():
    frame.to_csv(out / name, index=False)

try:
    import matplotlib.pyplot as plt

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(6.5, 4))
    ax.bar(["10 videos", "5 videos"], [acc10, acc5], color=["#3b6ea8", "#d17a22"], width=0.55)
    ax.plot([0, 1], [acc10, acc5], color="#444444", linewidth=2)
    ax.errorbar(
        [1],
        [acc5],
        yerr=[[max(0.0, acc5 - (acc10 + ci_low))], [max(0.0, (acc10 + ci_high) - acc5)]],
        fmt="none",
        ecolor="#222222",
        capsize=4,
    )
    ax.set_ylim(0, 1)
    ax.set_ylabel("Agreement with LLM panel majority")
    ax.set_title("Recent-5 vs Recent-10 LID Agreement")
    ax.text(0.5, min(0.95, max(acc10, acc5) + 0.04), f"Delta: {delta:+.1%}", ha="center")
    fig.tight_layout()
    fig.savefig(out / "agreement_dumbbell.svg")
    fig.savefig(out / "agreement_dumbbell.png", dpi=200)
    plt.close(fig)

    change_counts = pdf["paired_change_class"].value_counts().reindex(
        ["stable_correct", "degraded", "improved", "stable_mismatch", "no_llm_reference"], fill_value=0
    )
    fig, ax = plt.subplots(figsize=(7, 3.8))
    ax.barh(change_counts.index, change_counts.values, color=["#5b8c5a", "#c44e52", "#4c78a8", "#8c6d31", "#777777"])
    ax.set_xlabel("Channels")
    ax.set_title("Paired Change Classes")
    fig.tight_layout()
    fig.savefig(out / "paired_change_classes.svg")
    fig.savefig(out / "paired_change_classes.png", dpi=200)
    plt.close(fig)

    tm = transition.toPandas()
    if not tm.empty:
        tm["baseline10_consensus_base_iso"] = tm["baseline10_consensus_base_iso"].fillna("NULL")
        tm["recent5_consensus_base_iso"] = tm["recent5_consensus_base_iso"].fillna("NULL")
        changed_tm = tm[tm["baseline10_consensus_base_iso"] != tm["recent5_consensus_base_iso"]]
        top_labels = (
            pd.concat(
                [
                    changed_tm.groupby("baseline10_consensus_base_iso")["count"].sum(),
                    changed_tm.groupby("recent5_consensus_base_iso")["count"].sum(),
                ],
                axis=0,
            )
            .groupby(level=0)
            .sum()
            .sort_values(ascending=False)
            .head(12)
            .index
            .tolist()
        )
        if top_labels:
            hm = (
                tm[
                    tm["baseline10_consensus_base_iso"].isin(top_labels)
                    & tm["recent5_consensus_base_iso"].isin(top_labels)
                ]
                .pivot_table(
                    index="baseline10_consensus_base_iso",
                    columns="recent5_consensus_base_iso",
                    values="count",
                    aggfunc="sum",
                    fill_value=0,
                )
                .reindex(index=top_labels, columns=top_labels, fill_value=0)
            )
            fig, ax = plt.subplots(figsize=(8.5, 7))
            im = ax.imshow(hm.values, cmap="YlGnBu")
            ax.set_xticks(range(len(hm.columns)))
            ax.set_xticklabels(hm.columns, rotation=45, ha="right")
            ax.set_yticks(range(len(hm.index)))
            ax.set_yticklabels(hm.index)
            ax.set_xlabel("Recent-5 normalized ISO")
            ax.set_ylabel("Recent-10 normalized ISO")
            ax.set_title("Top Recent-10 to Recent-5 Label Transitions")
            fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="Channels")
            for i in range(len(hm.index)):
                for j in range(len(hm.columns)):
                    val = int(hm.iloc[i, j])
                    if val:
                        ax.text(j, i, str(val), ha="center", va="center", fontsize=8, color="#111111")
            fig.tight_layout()
            fig.savefig(out / "transition_heatmap.svg")
            fig.savefig(out / "transition_heatmap.png", dpi=200)
            plt.close(fig)

    ld = language_delta.toPandas()
    if not ld.empty:
        ld = ld.sort_values("delta_recent5_minus_baseline10").head(25)
        fig, ax = plt.subplots(figsize=(7.5, max(3.5, 0.28 * len(ld))))
        ax.hlines(ld["llm_panel_base_iso"], ld["baseline10_agreement"], ld["recent5_agreement"], color="#999999")
        ax.scatter(ld["baseline10_agreement"], ld["llm_panel_base_iso"], color="#3b6ea8", label="10 videos")
        ax.scatter(ld["recent5_agreement"], ld["llm_panel_base_iso"], color="#d17a22", label="5 videos")
        ax.set_xlim(0, 1)
        ax.set_xlabel("Agreement with LLM panel majority")
        ax.set_title("Per-Language Agreement Changes")
        ax.legend(frameon=False)
        fig.tight_layout()
        fig.savefig(out / "per_language_degradation_lollipop.svg")
        fig.savefig(out / "per_language_degradation_lollipop.png", dpi=200)
        plt.close(fig)

    ds = deepseek_summary.toPandas()
    if not ds.empty:
        reason_order = (
            ds.groupby("degradation_disagreement_reason")["count"]
            .sum()
            .sort_values(ascending=False)
            .head(12)
            .index
            .tolist()
        )
        outcome_order = [
            "10 matches DeepSeek only",
            "5 matches DeepSeek only",
            "both match",
            "neither matches",
            "DeepSeek insufficient",
        ]
        pivot = (
            ds[ds["degradation_disagreement_reason"].isin(reason_order)]
            .pivot_table(
                index="degradation_disagreement_reason",
                columns="deepseek_adjudication_outcome",
                values="count",
                aggfunc="sum",
                fill_value=0,
            )
            .reindex(index=reason_order, columns=outcome_order, fill_value=0)
        )
        fig, ax = plt.subplots(figsize=(9, max(4, 0.36 * len(pivot))))
        left = [0] * len(pivot)
        colors = ["#3b6ea8", "#d17a22", "#5b8c5a", "#7f7f7f", "#c44e52"]
        for outcome, color in zip(outcome_order, colors):
            vals = pivot[outcome].values
            ax.barh(pivot.index, vals, left=left, label=outcome, color=color)
            left = [a + b for a, b in zip(left, vals)]
        ax.set_xlabel("Channels")
        ax.set_title("DeepSeek Flash Adjudication by Disagreement Reason")
        ax.legend(frameon=False, loc="lower right")
        fig.tight_layout()
        fig.savefig(out / "deepseek_adjudication_stacked_bar.svg")
        fig.savefig(out / "deepseek_adjudication_stacked_bar.png", dpi=200)
        plt.close(fig)
except Exception as exc:
    print(f"Plot rendering failed; CSV/JSON outputs were still written: {exc}")

print(json.dumps(plot_payload["summary_metrics"], indent=2, sort_keys=True, default=str))
