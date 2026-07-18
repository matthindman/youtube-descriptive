#!/usr/bin/env python3
"""Render coefficient-style comparisons for the below-10K sensitivity run."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D


ROOT = Path(__file__).resolve().parents[1]
RUN_DIR = ROOT / "outputs" / "banded_lt10k_full_corpus_sensitivity_20260716"
PNG_PATH = RUN_DIR / "full_corpus_change_coefficient_plot.png"
SVG_PATH = RUN_DIR / "full_corpus_change_coefficient_plot.svg"

CURRENT = "#59636E"
PROJECTED = "#28789B"
POSITIVE = "#17856F"
NEGATIVE = "#BE5A48"
GRID = "#D9DEE3"
TEXT = "#1F2933"
MUTED = "#66717C"

SELECTIONS = {
    "language": [
        "English", "Chinese", "French", "Japanese", "Undetermined", "Bengali",
        "Russian", "Vietnamese", "Thai", "Indonesian", "Turkish", "Korean",
        "Arabic", "Portuguese", "Hindi", "Spanish",
    ],
    "family": [
        "Music", "Gaming", "Knowledge", "Society", "Unlabeled", "Sports",
        "Entertainment", "Lifestyle",
    ],
    "leaf": [
        "Music > Hip hop music",
        "Music > Rock music",
        "Music > Music of Asia",
        "Gaming > Action-adventure game",
        "Gaming > Action game",
        "Society > Health",
        "Lifestyle > Technology",
        "Sports > Football",
        "Sports > [Sports] - unspecified",
        "Society > Religion",
        "Entertainment > TV shows",
        "Entertainment > Humor",
        "Unlabeled > No YouTube topicCategories",
        "Entertainment > [Entertainment] - unspecified",
        "Entertainment > Movies",
        "Society > Politics",
        "Lifestyle > Hobby",
        "Lifestyle > [Lifestyle] - unspecified",
    ],
}

SECTION_SPECS = {
    "language": ("Languages", "Selected high-share and high-change languages", 55, 3.1),
    "family": ("Topic families", "All families", 36, 2.3),
    "leaf": ("Subtopics", "Largest substantive point changes", 20, 1.15),
}

LABEL_REPLACEMENTS = {
    "[Sports] - unspecified": "Sports unspecified",
    "[Entertainment] - unspecified": "Entertainment unspecified",
    "[Lifestyle] - unspecified": "Lifestyle unspecified",
    "No YouTube topicCategories": "Unlabeled",
}


def load_section(kind: str) -> pd.DataFrame:
    frame = pd.read_csv(RUN_DIR / f"{kind}_estimates.csv").set_index("category")
    frame = frame.loc[SELECTIONS[kind]].copy()
    if kind == "leaf":
        frame["label"] = [category.split(" > ", 1)[1] for category in frame.index]
        frame["label"] = frame["label"].replace(LABEL_REPLACEMENTS)
    else:
        frame["label"] = frame.index
    return frame.reset_index(drop=True)


def style_axis(axis: plt.Axes) -> None:
    axis.spines[["top", "right", "left"]].set_visible(False)
    axis.spines["bottom"].set_color(GRID)
    axis.tick_params(axis="both", colors=MUTED, length=0, pad=7)
    axis.grid(axis="x", color=GRID, linewidth=0.8, zorder=0)
    axis.set_axisbelow(True)


def draw_section(
    share_axis: plt.Axes,
    change_axis: plt.Axes,
    frame: pd.DataFrame,
    title: str,
    note: str,
    share_max: float,
    change_max: float,
) -> None:
    y = np.arange(len(frame))
    current = 100 * frame["current_above10k_view_share"].to_numpy()
    projected = 100 * frame["projected_full_view_share"].to_numpy()
    se = 100 * frame["projected_full_view_se_bootstrap"].to_numpy()
    change = frame["view_share_change_pp"].to_numpy()
    ci_low = frame["view_share_change_ci_low_pp"].to_numpy()
    ci_high = frame["view_share_change_ci_high_pp"].to_numpy()
    colors = np.where(change >= 0, POSITIVE, NEGATIVE)

    for row, y_value in enumerate(y):
        share_axis.plot(
            [current[row], projected[row]], [y_value, y_value],
            color=CURRENT, linewidth=1.2, alpha=0.75, zorder=2,
        )
    share_axis.errorbar(
        projected, y, xerr=se, fmt="none", ecolor=PROJECTED,
        elinewidth=4.2, capsize=0, alpha=0.58, zorder=3,
    )
    share_axis.scatter(
        current, y, s=48, facecolors="white", edgecolors=CURRENT,
        linewidths=1.7, zorder=4,
    )
    share_axis.scatter(projected, y, s=50, color=PROJECTED, zorder=5)
    share_axis.set_xlim(0, share_max)
    share_axis.set_yticks(y, frame["label"])
    share_axis.set_xlabel("View share (%)", color=MUTED)
    share_axis.invert_yaxis()
    style_axis(share_axis)

    change_axis.axvline(0, color=CURRENT, linewidth=1.1, zorder=1)
    for row, y_value in enumerate(y):
        change_axis.plot(
            [ci_low[row], ci_high[row]], [y_value, y_value],
            color=colors[row], linewidth=1.2, solid_capstyle="round", zorder=2,
        )
        change_axis.plot(
            [change[row] - se[row], change[row] + se[row]], [y_value, y_value],
            color=colors[row], linewidth=4.2, alpha=0.62,
            solid_capstyle="round", zorder=3,
        )
    change_axis.scatter(change, y, s=50, c=colors, zorder=4)
    change_axis.set_xlim(-change_max, change_max)
    change_axis.set_yticks(y, [""] * len(y))
    change_axis.set_xlabel("Change (percentage points)", color=MUTED)
    change_axis.invert_yaxis()
    style_axis(change_axis)

    share_axis.set_title(title, loc="left", color=TEXT, fontweight="semibold", pad=28)
    share_axis.text(
        0, 1.035, note, transform=share_axis.transAxes,
        color=MUTED, va="bottom", ha="left",
    )


def main() -> None:
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 10.5,
        "axes.titlesize": 14,
        "axes.labelsize": 10.5,
        "xtick.labelsize": 9.5,
        "ytick.labelsize": 10,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "text.color": TEXT,
    })
    figure, axes = plt.subplots(
        3, 2,
        figsize=(14, 22),
        gridspec_kw={"height_ratios": [16, 8, 18], "width_ratios": [1.05, 1]},
    )
    figure.subplots_adjust(left=0.19, right=0.97, top=0.88, bottom=0.055, hspace=0.48, wspace=0.18)

    for row, kind in enumerate(["language", "family", "leaf"]):
        draw_section(axes[row, 0], axes[row, 1], load_section(kind), *SECTION_SPECS[kind])

    figure.suptitle(
        "Estimated effect of adding channels below 10K subscribers",
        x=0.19, y=0.98, ha="left", color=TEXT, fontsize=20, fontweight="semibold",
    )
    figure.text(
        0.19, 0.955,
        "Current >=10K and projected full-corpus positive four-week view shares",
        ha="left", color=MUTED, fontsize=11.5,
    )
    legend = [
        Line2D([0], [0], marker="o", color="none", markerfacecolor="white", markeredgecolor=CURRENT,
               markeredgewidth=1.7, markersize=7, label="Current, channels >=10K"),
        Line2D([0], [0], marker="o", color="none", markerfacecolor=PROJECTED,
               markeredgecolor=PROJECTED, markersize=7, label="Projected full corpus"),
        Line2D([0], [0], color=PROJECTED, linewidth=4.2, alpha=0.62, label="+/- 1 bootstrap SE"),
        Line2D([0], [0], color=PROJECTED, linewidth=1.2, label="95% percentile interval"),
    ]
    figure.legend(
        handles=legend, loc="upper left", bbox_to_anchor=(0.19, 0.935),
        frameon=False, ncol=4, columnspacing=1.6, handlelength=2.2,
    )
    figure.text(
        0.19, 0.018,
        "Current >=10K composition is treated as fixed. Intervals reflect stratified pilot sampling only; "
        "overall calibrated view-weight effective n = 24.7.",
        ha="left", color=MUTED, fontsize=9.5,
    )

    figure.savefig(PNG_PATH, dpi=200, facecolor="white")
    figure.savefig(SVG_PATH, facecolor="white")
    plt.close(figure)
    print(f"WROTE {PNG_PATH}")
    print(f"WROTE {SVG_PATH}")


if __name__ == "__main__":
    main()
