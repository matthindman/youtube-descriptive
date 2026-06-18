#!/usr/bin/env python3
"""Render lumped parent and Lifestyle/Entertainment child overlap matrices."""

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
PROFILE = os.environ.get("DATABRICKS_PROFILE", "matt.hindman@researchaccelerator.org")
ARTIFACT_ROOT = ROOT / "artifacts" / "category_taxonomy_estimation_20260612"

NUMERIC_COLUMNS = [
    "row_rank",
    "col_rank",
    "denominator_channels",
    "n_a_channels",
    "n_b_channels",
    "n_both_channels",
    "prevalence_a",
    "prevalence_b",
    "joint_prevalence",
    "conditional_b_given_a",
    "conditional_a_given_b",
    "jaccard",
    "lift_vs_independence",
]

SHORT_LABELS = {
    "Lifestyle_(sociology)": "Lifestyle",
    "Entertainment": "Entertainment",
    "Music_combined": "Music",
    "Video_game_combined": "Video game",
    "Music": "Music",
    "Video_game_culture": "Video game",
    "Pop_music": "Pop music",
    "Action_game": "Action game",
    "Role-playing_video_game": "RPG",
    "Television_program": "TV program",
    "Performing_arts": "Performing arts",
    "Professional_wrestling": "Wrestling",
    "Physical_fitness": "Fitness",
    "Physical_attractiveness": "Attractiveness",
}


CONFIGS = [
    {
        "name": "lumped_parent",
        "query_file": ROOT / ".codex_databricks" / "sql_lumped_parent_overlap_20260613.json",
        "out_dir": ARTIFACT_ROOT / "lumped_parent_overlap_20260613",
        "summary_name": "lumped_parent_overlap_summary.md",
        "long_name": "lumped_parent_overlap_long.csv",
        "prevalence_name": "lumped_parent_prevalence.csv",
        "title_prefix": "Lumped parent overlap",
        "denominator_label": "nonempty channels",
        "criteria": "Groups are included when prevalence is greater than 2% of nonempty channel-label arrays. Music and video game labels are collapsed into `Music_combined` and `Video_game_combined`.",
    },
    {
        "name": "lifestyle_entertainment_children",
        "query_file": ROOT / ".codex_databricks" / "sql_lifestyle_entertainment_child_overlap_20260613.json",
        "out_dir": ARTIFACT_ROOT / "lifestyle_entertainment_child_overlap_20260613",
        "summary_name": "lifestyle_entertainment_child_overlap_summary.md",
        "long_name": "lifestyle_entertainment_child_overlap_long.csv",
        "prevalence_name": "lifestyle_entertainment_child_prevalence.csv",
        "title_prefix": "Lifestyle/Entertainment child overlap",
        "denominator_label": "channels with Lifestyle or Entertainment",
        "criteria": "Children come from strong/moderate empirical edges to Lifestyle or Entertainment and are included when they appear in more than 1% of Lifestyle cases or more than 1% of Entertainment cases.",
    },
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


def execute_query(query_file: Path) -> dict:
    response = databricks_api("post", "/api/2.0/sql/statements", "--json", f"@{query_file}")
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


def load_result(config: dict) -> pd.DataFrame:
    response = execute_query(config["query_file"])
    config["out_dir"].mkdir(parents=True, exist_ok=True)
    (config["out_dir"] / "query_response.json").write_text(json.dumps(response, indent=2), encoding="utf-8")
    columns = [col["name"] for col in response["manifest"]["schema"]["columns"]]
    df = pd.DataFrame(response["result"]["data_array"], columns=columns)
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])
    for col in df.columns:
        if col.startswith("n_") or col.startswith("prevalence_"):
            df[col] = pd.to_numeric(df[col])
    df.to_csv(config["out_dir"] / config["long_name"], index=False)
    return df


def ordered_labels(df: pd.DataFrame, config: dict) -> list[str]:
    prevalence = (
        df[["row_rank", "label_a", "role_a", "n_a_channels", "prevalence_a", "denominator_channels"]]
        .drop_duplicates()
        .sort_values("row_rank")
    )
    extra_cols = [
        col
        for col in [
            "n_a_lifestyle_channels",
            "n_a_entertainment_channels",
            "prevalence_a_in_lifestyle",
            "prevalence_a_in_entertainment",
        ]
        if col in df.columns
    ]
    if extra_cols:
        extra = df[["label_a", *extra_cols]].drop_duplicates()
        prevalence = prevalence.merge(extra, on="label_a", how="left")
    prevalence.to_csv(config["out_dir"] / config["prevalence_name"], index=False)
    return prevalence["label_a"].tolist()


def short_label(label: str) -> str:
    return SHORT_LABELS.get(label, label.replace("_", " "))


def role_marker(role: str) -> str:
    if "Lifestyle_(sociology)" in role and "Entertainment" in role:
        return "L/E"
    if "Lifestyle_(sociology)" in role:
        return "L"
    if "Entertainment" in role:
        return "E"
    if role == "lumped_music":
        return "M"
    if role == "lumped_video_game":
        return "G"
    return ""


def axis_labels(df: pd.DataFrame, labels: list[str]) -> list[str]:
    prevalence = (
        df[["label_a", "role_a", "prevalence_a"]]
        .drop_duplicates()
        .set_index("label_a")
    )
    out = []
    for label in labels:
        marker = role_marker(str(prevalence.loc[label, "role_a"]))
        display = short_label(label)
        if marker:
            display = f"{display} [{marker}]"
        out.append(f"{display}\n{100 * prevalence.loc[label, 'prevalence_a']:.1f}%")
    return out


def pivot_metric(df: pd.DataFrame, labels: list[str], metric: str) -> pd.DataFrame:
    mat = df.pivot(index="label_a", columns="label_b", values=metric)
    return mat.loc[labels, labels]


def save_matrix_csv(matrix: pd.DataFrame, path: Path):
    matrix.to_csv(path)


def annotate_heatmap(ax, values: np.ndarray, fmt, fontsize: float, diagonal_label: str | None = None, text_color=None):
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


def plot_joint_and_conditional(df: pd.DataFrame, labels: list[str], config: dict):
    n = len(labels)
    fontsize = max(5.0, min(8.0, 76.0 / max(n, 1)))
    label_text = axis_labels(df, labels)
    joint = pivot_metric(df, labels, "joint_prevalence") * 100.0
    conditional = pivot_metric(df, labels, "conditional_b_given_a") * 100.0
    save_matrix_csv(joint, config["out_dir"] / "joint_prevalence_matrix_pct.csv")
    save_matrix_csv(conditional, config["out_dir"] / "conditional_matrix_pct.csv")

    joint_for_color = joint.copy()
    np.fill_diagonal(joint_for_color.values, np.nan)
    offdiag_max = np.nanmax(joint_for_color.values)
    fig_width = max(14, 1.25 * n + 5)
    fig, axes = plt.subplots(1, 2, figsize=(fig_width, max(7, 0.65 * n + 3)), constrained_layout=True)

    im0 = axes[0].imshow(joint_for_color.values, cmap="viridis", vmin=0, vmax=offdiag_max)
    axes[0].set_title(f"{config['title_prefix']}: joint prevalence", fontsize=13, pad=12)
    axes[0].set_xticks(range(n), labels=label_text, rotation=45, ha="right")
    axes[0].set_yticks(range(n), labels=label_text)
    axes[0].set_xlabel("Column label")
    axes[0].set_ylabel("Row label")
    for i in range(n):
        axes[0].add_patch(plt.Rectangle((i - 0.5, i - 0.5), 1, 1, facecolor="#f0f0f0", edgecolor="#d0d0d0"))
    annotate_heatmap(
        axes[0],
        joint.values,
        lambda value, _i, _j: "<0.01" if 0 < value < 0.01 else f"{value:.2f}",
        fontsize=fontsize,
        diagonal_label="prev",
        text_color=lambda value, i, j: "#111111" if i == j or value >= offdiag_max * 0.45 else "#ffffff",
    )
    fig.colorbar(im0, ax=axes[0], fraction=0.046, pad=0.04, label=f"% of {config['denominator_label']}")

    conditional_for_color = conditional.copy()
    np.fill_diagonal(conditional_for_color.values, np.nan)
    im1 = axes[1].imshow(conditional_for_color.values, cmap="magma", vmin=0, vmax=100)
    axes[1].set_title(f"{config['title_prefix']}: row-conditional overlap", fontsize=13, pad=12)
    axes[1].set_xticks(range(n), labels=label_text, rotation=45, ha="right")
    axes[1].set_yticks(range(n), labels=label_text)
    axes[1].set_xlabel("Column label also present")
    axes[1].set_ylabel("Given row label is present")
    for i in range(n):
        axes[1].add_patch(plt.Rectangle((i - 0.5, i - 0.5), 1, 1, facecolor="#f0f0f0", edgecolor="#d0d0d0"))
    annotate_heatmap(
        axes[1],
        conditional.values,
        lambda value, _i, _j: "<0.1" if 0 < value < 0.1 else f"{value:.1f}",
        fontsize=fontsize,
        diagonal_label="self",
        text_color=lambda value, i, j: "#111111" if i == j or value >= 55 else "#ffffff",
    )
    fig.colorbar(im1, ax=axes[1], fraction=0.046, pad=0.04, label="%")

    fig.text(0.01, 0.01, "Diagonal cells show single-label prevalence. Bracket codes mark label/group type where useful.", fontsize=9, color="#333333")
    fig.savefig(config["out_dir"] / "joint_and_conditional_heatmaps.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_lift(df: pd.DataFrame, labels: list[str], config: dict):
    n = len(labels)
    fontsize = max(5.0, min(8.0, 76.0 / max(n, 1)))
    lift = pivot_metric(df, labels, "lift_vs_independence")
    save_matrix_csv(lift, config["out_dir"] / "lift_matrix.csv")
    log_lift = lift.map(lambda x: math.log2(x) if x > 0 else np.nan)
    np.fill_diagonal(log_lift.values, np.nan)
    max_abs = np.nanmax(np.abs(log_lift.values))

    fig, ax = plt.subplots(figsize=(max(8.5, 0.72 * n + 4), max(7.5, 0.62 * n + 3)), constrained_layout=True)
    im = ax.imshow(log_lift.values, cmap="coolwarm", vmin=-max_abs, vmax=max_abs)
    label_text = axis_labels(df, labels)
    ax.set_title(f"{config['title_prefix']}: lift vs. independence", fontsize=13, pad=12)
    ax.set_xticks(range(n), labels=label_text, rotation=45, ha="right")
    ax.set_yticks(range(n), labels=label_text)
    for i in range(n):
        ax.add_patch(plt.Rectangle((i - 0.5, i - 0.5), 1, 1, facecolor="#f0f0f0", edgecolor="#d0d0d0"))
    annotate_heatmap(
        ax,
        lift.values,
        lambda value, _i, _j: f"{value:.2f}x",
        fontsize=fontsize,
        diagonal_label="self",
        text_color=lambda value, i, j: "#111111" if i == j or 0.55 <= value <= 1.8 else "#ffffff",
    )
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="log2(observed / expected)")
    fig.text(0.01, 0.01, "Lift above 1 means the pair appears more often than expected from marginal prevalences.", fontsize=9, color="#333333")
    fig.savefig(config["out_dir"] / "lift_heatmap.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def markdown_matrix(matrix: pd.DataFrame, decimals: int = 2) -> str:
    short = [short_label(x) for x in matrix.index]
    rows = ["| " + " | ".join(["Row / column", *short]) + " |"]
    rows.append("|" + "|".join(["---"] + ["---:"] * len(short)) + "|")
    for label, display in zip(matrix.index, short):
        cells = []
        for value in matrix.loc[label]:
            if 0 < value < 0.01:
                cells.append("<0.01")
            else:
                cells.append(f"{value:.{decimals}f}")
        rows.append("| " + " | ".join([display, *cells]) + " |")
    return "\n".join(rows)


def unique_pairs(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["row_rank"] < df["col_rank"]].copy()


def write_summary(df: pd.DataFrame, labels: list[str], config: dict):
    prevalence = (
        df[["row_rank", "label_a", "role_a", "n_a_channels", "prevalence_a", "denominator_channels"]]
        .drop_duplicates()
        .sort_values("row_rank")
    )
    denominator = int(prevalence["denominator_channels"].iloc[0])
    joint = pivot_metric(df, labels, "joint_prevalence") * 100.0
    conditional = pivot_metric(df, labels, "conditional_b_given_a") * 100.0
    lift = pivot_metric(df, labels, "lift_vs_independence")
    prevalence_rows = []
    for row in prevalence.itertuples(index=False):
        prevalence_rows.append(f"| {short_label(row.label_a)} | `{row.label_a}` | `{row.role_a}` | {row.n_a_channels:,} | {100 * row.prevalence_a:.2f}% |")

    pairs = unique_pairs(df)
    top_joint = pairs.sort_values("joint_prevalence", ascending=False).head(10)
    top_lift = pairs[pairs["n_both_channels"] >= 100].sort_values("lift_vs_independence", ascending=False).head(10)
    top_conditional = df[df["label_a"] != df["label_b"]].sort_values("conditional_b_given_a", ascending=False).head(10)

    def pair_lines(rows: pd.DataFrame, metric: str, pct: bool = False) -> str:
        lines = []
        for row in rows.itertuples(index=False):
            value = getattr(row, metric)
            display = f"{100 * value:.2f}%" if pct else f"{value:.2f}x"
            lines.append(f"- {short_label(row.label_a)} + {short_label(row.label_b)}: {display} ({row.n_both_channels:,} channels)")
        return "\n".join(lines)

    summary = f"""# {config['title_prefix']}

Date: 2026-06-13

Denominator: {denominator:,} {config['denominator_label']}.

{config['criteria']}

## Included Labels

| Display label | Raw/group label | Role | Channels | Prevalence |
|---|---|---|---:|---:|
{chr(10).join(prevalence_rows)}

## Joint Overlap Matrix

Cells are the percent of the denominator that have both row and column labels. Diagonal cells are single-label prevalence.

{markdown_matrix(joint, decimals=2)}

## Conditional Matrix

Cells are `P(column label present | row label present)`, in percent.

{markdown_matrix(conditional, decimals=1)}

## Lift Matrix

Cells are observed pair prevalence divided by expected pair prevalence under independence.

{markdown_matrix(lift, decimals=2)}

## Main Patterns

Highest absolute joint overlaps:
{pair_lines(top_joint, "joint_prevalence", pct=True)}

Strongest conditional overlaps:
{pair_lines(top_conditional, "conditional_b_given_a", pct=True)}

Strongest prevalence-adjusted overlaps among pairs with at least 100 shared channels:
{pair_lines(top_lift, "lift_vs_independence", pct=False)}
"""
    (config["out_dir"] / config["summary_name"]).write_text(summary, encoding="utf-8")


def render_config(config: dict) -> dict:
    df = load_result(config)
    labels = ordered_labels(df, config)
    plot_joint_and_conditional(df, labels, config)
    plot_lift(df, labels, config)
    write_summary(df, labels, config)
    return {"name": config["name"], "out_dir": str(config["out_dir"]), "n_rows": int(len(df)), "n_labels": len(labels)}


def main():
    results = [render_config(config) for config in CONFIGS]
    print(json.dumps(results, sort_keys=True))


if __name__ == "__main__":
    main()
