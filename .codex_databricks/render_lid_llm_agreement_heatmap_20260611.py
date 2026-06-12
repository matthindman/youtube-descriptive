#!/usr/bin/env python3
import json
import math
import os
import sys

import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import numpy as np


def short_label(display_name: str) -> str:
    deepseek_prefixes = {
        "deepseek:deepseek-v4-pro": "DeepSeek V4 Pro",
        "deepseek:deepseek-v4-flash": "DeepSeek V4 Flash",
    }
    for prefix, label in deepseek_prefixes.items():
        if display_name == prefix:
            return label
        if display_name.startswith(prefix + " (") and display_name.endswith(")"):
            setting = display_name[len(prefix) + 2:-1]
            setting = (
                setting.replace("no thinking", "no think")
                .replace("thinking low, 2k cap", "think low\n2k cap")
                .replace("thinking low, 600 cap", "think low\n600 cap")
            )
            return f"{label}\n({setting})"
    replacements = {
        "OpenLID": "OpenLID",
        "GlotLID": "GlotLID",
        "openai:gpt-5.5": "GPT-5.5",
        "openai:gpt-5.4-mini": "GPT-5.4 mini",
        "openai:gpt-5.4-nano": "GPT-5.4 nano",
        "openai:gpt-5-nano": "GPT-5 nano",
        "anthropic:claude-opus-4-8": "Claude Opus 4.8",
        "anthropic:claude-sonnet-4-6": "Claude Sonnet 4.6",
        "anthropic:claude-haiku-4-5": "Claude Haiku 4.5",
        "gemini:gemini-3.1-pro-preview": "Gemini 3.1 Pro",
    }
    return replacements.get(display_name, display_name.replace(":", "\n"))


def provider_color(provider: str) -> str:
    return {
        "lid": "#5b5f63",
        "openai": "#1b6f68",
        "anthropic": "#a25722",
        "gemini": "#5d5aa8",
        "deepseek": "#8c3f72",
    }.get(provider, "#666666")


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: render_lid_llm_agreement_heatmap_20260611.py input.json output.svg", file=sys.stderr)
        return 2
    input_path, output_path = sys.argv[1], sys.argv[2]
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    models = data["models"]
    labels = [short_label(m["display_name"]) for m in models]
    n = len(models)
    index = {m["model_key"]: i for i, m in enumerate(models)}
    mat = np.full((n, n), np.nan)
    shared = np.zeros((n, n), dtype=int)
    np.fill_diagonal(mat, 1.0)
    for m in models:
        i = index[m["model_key"]]
        shared[i, i] = int(m.get("n_valid_votes") or 0)

    for row in data["pairwise"]:
        i = index[row["model_key_a"]]
        j = index[row["model_key_b"]]
        value = float(row["normalized_base_iso_agreement_rate"])
        mat[i, j] = value
        mat[j, i] = value
        n_shared = int(row["n_both_classified"])
        shared[i, j] = n_shared
        shared[j, i] = n_shared

    fig_w = max(10, 0.72 * n + 4)
    fig_h = max(8, 0.68 * n + 3)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    cmap = plt.get_cmap("YlGnBu").copy()
    cmap.set_bad("#f2f3f4")
    image = ax.imshow(mat, cmap=cmap, vmin=0.0, vmax=1.0)

    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(labels, rotation=45, ha="right", rotation_mode="anchor", fontsize=9)
    ax.set_yticklabels(labels, fontsize=9)
    ax.tick_params(length=0)
    ax.set_title(
        "Agreement on Normalized Base ISO Language\n"
        f"Hardcase sample: {data['n_sample_channels']:,} channels where OpenLID and GlotLID disagree",
        loc="left",
        fontsize=14,
        fontweight="bold",
        pad=18,
    )
    ax.set_xlabel("Model")
    ax.set_ylabel("Model")

    for spine in ax.spines.values():
        spine.set_visible(False)

    for i in range(n):
        for j in range(n):
            if i == j:
                text = f"n={shared[i, j]:,}"
                color = "white"
            elif math.isnan(mat[i, j]):
                text = "NA"
                color = "#6b6f73"
            else:
                text = f"{mat[i, j] * 100:.0f}"
                color = "#0b1720" if mat[i, j] < 0.72 else "white"
            ax.text(j, i, text, ha="center", va="center", fontsize=7.5, color=color)

    for i, model in enumerate(models):
        color = provider_color(model["provider"])
        ax.add_patch(plt.Rectangle((-0.72, i - 0.42), 0.12, 0.84, color=color, clip_on=False))
        ax.add_patch(plt.Rectangle((i - 0.42, -0.72), 0.84, 0.12, color=color, clip_on=False))

    cbar = fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Pairwise agreement rate")
    cbar.ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))

    fig.text(
        0.01,
        0.01,
        "Off-diagonal cells are percent agreement on normalized base ISO labels. Diagonal cells show each model's valid vote count; "
        "DeepSeek thinking variants are plotted as separate models when included in the visual data.",
        ha="left",
        va="bottom",
        fontsize=8,
        color="#555",
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    ext = os.path.splitext(output_path)[1].lower().lstrip(".") or "svg"
    fig.savefig(output_path, format=ext, bbox_inches="tight", dpi=180)
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
