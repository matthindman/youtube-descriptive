#!/usr/bin/env python3
"""Render parent-category overlap artifacts from the Databricks taxonomy tables."""

from __future__ import annotations

import json
import math
import os
import subprocess
import time
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
QUERY_FILE = ROOT / ".codex_databricks" / "sql_parent_overlap_matrix_20260613.json"
OUT_DIR = ROOT / "artifacts" / "category_taxonomy_estimation_20260612" / "parent_overlap_20260613"
PROFILE = os.environ.get("DATABRICKS_PROFILE", "matt.hindman@researchaccelerator.org")

SHORT_LABELS = {
    "Lifestyle_(sociology)": "Lifestyle",
    "Entertainment": "Entertainment",
    "Music": "Music",
    "Video_game_culture": "Video game",
    "Pop_music": "Pop music",
    "Action_game": "Action game",
    "Role-playing_video_game": "RPG",
    "Sport": "Sport",
    "Vehicle": "Vehicle",
}

NUMERIC_COLUMNS = [
    "row_rank",
    "col_rank",
    "n_nonempty_channels",
    "n_parent_a_channels",
    "n_parent_b_channels",
    "n_both_channels",
    "prevalence_a",
    "prevalence_b",
    "joint_prevalence",
    "conditional_b_given_a",
    "conditional_a_given_b",
    "jaccard",
    "lift_vs_independence",
]


def databricks_api(method: str, path: str, *extra_args: str) -> dict:
    cmd = [
        "env",
        "DATABRICKS_AUTH_STORAGE=plaintext",
        "databricks",
        "api",
        method,
        path,
        "--profile",
        PROFILE,
        "--output",
        "json",
        *extra_args,
    ]
    return json.loads(subprocess.check_output(cmd, cwd=ROOT, text=True))


def execute_query() -> dict:
    response = databricks_api("post", "/api/2.0/sql/statements", "--json", f"@{QUERY_FILE}")
    statement_id = response["statement_id"]
    state = response.get("status", {}).get("state")
    while state in {"PENDING", "RUNNING"}:
        time.sleep(5)
        response = databricks_api("get", f"/api/2.0/sql/statements/{statement_id}")
        state = response.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(json.dumps(response.get("status", response), indent=2))
    if "result" in response and "data_array" in response["result"]:
        return response
    chunk = databricks_api("get", f"/api/2.0/sql/statements/{statement_id}/result/chunks/0")
    response["result"] = chunk
    return response


def load_result() -> pd.DataFrame:
    response = execute_query()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "parent_overlap_query_response.json").write_text(json.dumps(response, indent=2), encoding="utf-8")

    columns = [col["name"] for col in response["manifest"]["schema"]["columns"]]
    rows = response["result"]["data_array"]
    df = pd.DataFrame(rows, columns=columns)
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col])
    df.to_csv(OUT_DIR / "parent_overlap_long.csv", index=False)
    return df


def ordered_labels(df: pd.DataFrame) -> list[str]:
    labels = (
        df[["row_rank", "parent_a", "empirical_role_a", "prevalence_a", "n_parent_a_channels"]]
        .drop_duplicates()
        .sort_values(["row_rank"])
    )
    labels.to_csv(OUT_DIR / "parent_prevalence.csv", index=False)
    return labels["parent_a"].tolist()


def pivot_metric(df: pd.DataFrame, labels: list[str], metric: str) -> pd.DataFrame:
    mat = df.pivot(index="parent_a", columns="parent_b", values=metric)
    return mat.loc[labels, labels]


def axis_labels(df: pd.DataFrame, labels: list[str]) -> list[str]:
    prevalence = (
        df[["parent_a", "prevalence_a", "empirical_role_a"]]
        .drop_duplicates()
        .set_index("parent_a")
    )
    out = []
    for label in labels:
        role = prevalence.loc[label, "empirical_role_a"]
        suffix = "*" if role == "intermediate_or_crosscutting_parent" else ""
        out.append(f"{SHORT_LABELS.get(label, label)}{suffix}\n{100 * prevalence.loc[label, 'prevalence_a']:.1f}%")
    return out


def annotate_heatmap(
    ax,
    values: np.ndarray,
    fmt,
    fontsize: int = 8,
    diagonal_label: str | None = None,
    text_color=None,
):
    for i in range(values.shape[0]):
        for j in range(values.shape[1]):
            value = values[i, j]
            if np.isnan(value):
                continue
            text = fmt(value, i, j)
            if diagonal_label and i == j:
                text = f"{text}\n{diagonal_label}"
            color = text_color(value, i, j) if text_color else "#111111"
            ax.text(j, i, text, ha="center", va="center", fontsize=fontsize, color=color)


def save_matrix_csv(matrix: pd.DataFrame, path: Path, pct: bool = False):
    out = matrix.copy()
    if pct:
        out = out * 100.0
    out.to_csv(path)


def plot_joint_and_conditional(df: pd.DataFrame, labels: list[str]):
    joint = pivot_metric(df, labels, "joint_prevalence") * 100.0
    conditional = pivot_metric(df, labels, "conditional_b_given_a") * 100.0
    save_matrix_csv(joint, OUT_DIR / "parent_overlap_joint_prevalence_matrix_pct.csv")
    save_matrix_csv(conditional, OUT_DIR / "parent_overlap_conditional_matrix_pct.csv")

    joint_for_color = joint.copy()
    np.fill_diagonal(joint_for_color.values, np.nan)
    offdiag_max = np.nanmax(joint_for_color.values)

    fig, axes = plt.subplots(1, 2, figsize=(16, 8.5), constrained_layout=True)
    label_text = axis_labels(df, labels)

    im0 = axes[0].imshow(joint_for_color.values, cmap="viridis", vmin=0, vmax=offdiag_max)
    axes[0].set_title("Joint overlap: % of nonempty channels with both labels", fontsize=13, pad=12)
    axes[0].set_xticks(range(len(labels)), labels=label_text, rotation=45, ha="right")
    axes[0].set_yticks(range(len(labels)), labels=label_text)
    axes[0].set_xlabel("Parent-like label B")
    axes[0].set_ylabel("Parent-like label A")
    for i in range(len(labels)):
        axes[0].add_patch(plt.Rectangle((i - 0.5, i - 0.5), 1, 1, facecolor="#f0f0f0", edgecolor="#d0d0d0"))
    annotate_heatmap(
        axes[0],
        joint.values,
        lambda value, _i, _j: "<0.01" if 0 < value < 0.01 else f"{value:.2f}",
        diagonal_label="prev",
        text_color=lambda value, i, j: "#111111" if i == j or value >= offdiag_max * 0.45 else "#ffffff",
    )
    fig.colorbar(im0, ax=axes[0], fraction=0.046, pad=0.04, label="% of nonempty channels")

    conditional_for_color = conditional.copy()
    np.fill_diagonal(conditional_for_color.values, np.nan)
    im1 = axes[1].imshow(conditional_for_color.values, cmap="magma", vmin=0, vmax=100)
    axes[1].set_title("Conditional overlap: % of row-label channels also with column label", fontsize=13, pad=12)
    axes[1].set_xticks(range(len(labels)), labels=label_text, rotation=45, ha="right")
    axes[1].set_yticks(range(len(labels)), labels=label_text)
    axes[1].set_xlabel("Column label also present")
    axes[1].set_ylabel("Given row label is present")
    for i in range(len(labels)):
        axes[1].add_patch(plt.Rectangle((i - 0.5, i - 0.5), 1, 1, facecolor="#f0f0f0", edgecolor="#d0d0d0"))
    annotate_heatmap(
        axes[1],
        conditional.values,
        lambda value, _i, _j: "<0.1" if 0 < value < 0.1 else f"{value:.1f}",
        diagonal_label="self",
        text_color=lambda value, i, j: "#111111" if i == j or value >= 55 else "#ffffff",
    )
    fig.colorbar(im1, ax=axes[1], fraction=0.046, pad=0.04, label="%")

    fig.text(
        0.01,
        0.01,
        "* intermediate/cross-cutting parent-like label. Diagonal cells show single-label prevalence, not pair overlap.",
        fontsize=9,
        color="#333333",
    )
    fig.savefig(OUT_DIR / "parent_overlap_joint_and_conditional_heatmaps.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_lift(df: pd.DataFrame, labels: list[str]):
    lift = pivot_metric(df, labels, "lift_vs_independence")
    save_matrix_csv(lift, OUT_DIR / "parent_overlap_lift_matrix.csv")
    log_lift = lift.map(lambda x: math.log2(x) if x > 0 else np.nan)
    np.fill_diagonal(log_lift.values, np.nan)
    max_abs = np.nanmax(np.abs(log_lift.values))

    fig, ax = plt.subplots(figsize=(9.5, 8.5), constrained_layout=True)
    im = ax.imshow(log_lift.values, cmap="coolwarm", vmin=-max_abs, vmax=max_abs)
    label_text = axis_labels(df, labels)
    ax.set_title("Prevalence-adjusted overlap: lift vs. independence", fontsize=13, pad=12)
    ax.set_xticks(range(len(labels)), labels=label_text, rotation=45, ha="right")
    ax.set_yticks(range(len(labels)), labels=label_text)
    for i in range(len(labels)):
        ax.add_patch(plt.Rectangle((i - 0.5, i - 0.5), 1, 1, facecolor="#f0f0f0", edgecolor="#d0d0d0"))
    annotate_heatmap(
        ax,
        lift.values,
        lambda value, _i, _j: f"{value:.2f}x",
        fontsize=8,
        diagonal_label="self",
        text_color=lambda value, i, j: "#111111" if i == j or 0.55 <= value <= 1.8 else "#ffffff",
    )
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="log2(observed / expected)")
    fig.text(
        0.01,
        0.01,
        "Lift above 1 means the pair appears more often than expected from the two marginal prevalences.",
        fontsize=9,
        color="#333333",
    )
    fig.savefig(OUT_DIR / "parent_overlap_lift_heatmap.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def markdown_matrix(matrix: pd.DataFrame, decimals: int = 2) -> str:
    short = [SHORT_LABELS.get(x, x) for x in matrix.index]
    rows = []
    header = ["Row / column", *short]
    rows.append("| " + " | ".join(header) + " |")
    rows.append("|" + "|".join(["---"] + ["---:"] * len(short)) + "|")
    for label, short_label in zip(matrix.index, short):
        cells = []
        for value in matrix.loc[label]:
            if 0 < value < 0.01:
                cells.append("<0.01")
            else:
                cells.append(f"{value:.{decimals}f}")
        rows.append("| " + " | ".join([short_label, *cells]) + " |")
    return "\n".join(rows)


def write_summary(df: pd.DataFrame, labels: list[str]):
    prevalence = (
        df[["row_rank", "parent_a", "empirical_role_a", "n_parent_a_channels", "prevalence_a", "n_nonempty_channels"]]
        .drop_duplicates()
        .sort_values("row_rank")
    )
    joint = pivot_metric(df, labels, "joint_prevalence") * 100.0
    conditional = pivot_metric(df, labels, "conditional_b_given_a") * 100.0
    lift = pivot_metric(df, labels, "lift_vs_independence")
    n_total = int(prevalence["n_nonempty_channels"].iloc[0])

    prevalence_rows = []
    for row in prevalence.itertuples(index=False):
        role = "intermediate/cross-cutting" if row.empirical_role_a == "intermediate_or_crosscutting_parent" else "empirical parent"
        prevalence_rows.append(
            f"| {SHORT_LABELS.get(row.parent_a, row.parent_a)} | `{row.parent_a}` | {role} | {row.n_parent_a_channels:,} | {100 * row.prevalence_a:.2f}% |"
        )

    offdiag = df[df["parent_a"] != df["parent_b"]].copy()
    top_joint = offdiag.sort_values("joint_prevalence", ascending=False).head(8)
    top_conditional = offdiag.sort_values("conditional_b_given_a", ascending=False).head(8)
    top_lift = offdiag[offdiag["n_both_channels"] >= 100].sort_values("lift_vs_independence", ascending=False).head(8)

    def pair_list(rows: pd.DataFrame, metric: str, pct: bool = False) -> list[str]:
        out = []
        for row in rows.itertuples(index=False):
            value = getattr(row, metric)
            display = f"{100 * value:.2f}%" if pct else f"{value:.2f}x"
            out.append(
                f"- {SHORT_LABELS.get(row.parent_a, row.parent_a)} + {SHORT_LABELS.get(row.parent_b, row.parent_b)}: {display} ({row.n_both_channels:,} channels)"
            )
        return out

    summary = f"""# Parent Category Overlap Matrix

Date: 2026-06-13

Source tables:

- `dev_sean.matt.yt_channel_topic_taxonomy_channel_labels_20260612`
- `dev_sean.matt.yt_channel_topic_taxonomy_label_roles_20260612`

Denominator: {n_total:,} channels with nonempty topic-category arrays.

Included labels: parent-like labels with empirical role `empirical_parent` or `intermediate_or_crosscutting_parent` and prevalence greater than 2% of nonempty channels.

## Included Parent-Like Labels

| Display label | Raw label | Role | Channels | Prevalence |
|---|---|---|---:|---:|
{chr(10).join(prevalence_rows)}

## Joint Overlap Matrix

Cells are the percent of all nonempty channels that have both the row and column parent-like labels. Diagonal cells are single-label prevalence.

{markdown_matrix(joint, decimals=2)}

## Conditional Matrix

Cells are `P(column label present | row label present)`, in percent. This is useful because parent prevalences vary from about 3% to 48%.

{markdown_matrix(conditional, decimals=1)}

## Lift Matrix

Cells are observed pair prevalence divided by expected pair prevalence under independence. Values above 1 indicate positive association after accounting for base rates.

{markdown_matrix(lift, decimals=2)}

## Main Patterns

Highest absolute joint overlaps:
{chr(10).join(pair_list(top_joint, "joint_prevalence", pct=True))}

Strongest conditional overlaps:
{chr(10).join(pair_list(top_conditional, "conditional_b_given_a", pct=True))}

Strongest prevalence-adjusted overlaps among pairs with at least 100 shared channels:
{chr(10).join(pair_list(top_lift, "lift_vs_independence", pct=False))}

## Interpretation

The absolute joint-prevalence matrix is the direct answer to “what portion of channels have both parents?” It should not be the only view, because common labels such as Lifestyle and Entertainment dominate absolute overlap. The conditional matrix shows near-nesting patterns, such as Pop music under Music and game subtypes under Video game culture. The lift matrix separates real association from marginal prevalence and shows that some small absolute overlaps are meaningful relative to expectation, while many broad cross-domain overlaps are lower than expected.
"""
    (OUT_DIR / "parent_overlap_summary.md").write_text(summary, encoding="utf-8")


def main():
    df = load_result()
    labels = ordered_labels(df)
    plot_joint_and_conditional(df, labels)
    plot_lift(df, labels)
    write_summary(df, labels)
    print(json.dumps({"out_dir": str(OUT_DIR), "n_rows": int(len(df)), "n_labels": len(labels)}, sort_keys=True))


if __name__ == "__main__":
    main()
