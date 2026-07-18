#!/usr/bin/env python3
"""Render census comparisons of equal-channel and view-weighted treemaps."""

from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patheffects as path_effects
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import squarify
import yaml
from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
RUN_DIR = ROOT / "outputs" / "youtube_topic_treemap_weighting_bias_20260716_v1"
INPUT_CSV = RUN_DIR / "databricks_export" / "language_family_leaf_weighting_masses.csv"
EXPORT_MANIFEST = RUN_DIR / "databricks_export" / "export_manifest.json"
PALETTE_PATH = ROOT / "config" / "family_color_map.yaml"

COMPARISON_PNG = RUN_DIR / "treemap_equal_vs_view_weighted_v1.png"
COMPARISON_SVG = RUN_DIR / "treemap_equal_vs_view_weighted_v1.svg"
BIAS_PNG = RUN_DIR / "treemap_weighting_bias_balance_v1.png"
BIAS_SVG = RUN_DIR / "treemap_weighting_bias_balance_v1.svg"
INTERACTIVE_HTML = RUN_DIR / "treemap_weighting_bias_lens_v1.html"
CELLS_CSV = RUN_DIR / "treemap_weighting_bias_cells_v1.csv"
LANGUAGE_SUMMARY_CSV = RUN_DIR / "weighting_bias_language_summary_v1.csv"
FAMILY_SUMMARY_CSV = RUN_DIR / "weighting_bias_family_summary_v1.csv"
LANGUAGE_FAMILY_SUMMARY_CSV = RUN_DIR / "weighting_bias_language_family_summary_v1.csv"
MANIFEST_PATH = RUN_DIR / "bias_manifest.json"
LOG_PATH = RUN_DIR / "render_log_weighting_bias_v1.txt"

TOP_K = 12
UNDETERMINED = "Undetermined"
OTHER_LANGUAGES = "Other languages"
OTHER_FAMILIES = "Other (smaller family shares)"
OTHER_BIASES = "Other (smaller family biases)"
BASE_MIN_TOTAL_SHARE = 0.001
BIAS_MIN_LANGUAGE_FRAC = 0.01
BIAS_MIN_CELL_FRAC = 0.004

OVER_COLOR = "#B55233"
UNDER_COLOR = "#167A78"
TEXT = "#202830"
MUTED = "#65717C"
GRID = "#D9DEE3"
NEUTRAL = "#E8E4DC"
WHITE = "#FFFFFF"


def log(message: str = "") -> None:
    print(message)
    _LOG.append(message)


_LOG: list[str] = []


def load_palette() -> dict[str, str]:
    config = yaml.safe_load(PALETTE_PATH.read_text())
    colors = dict(config["family_colors"])
    colors.update(config["residual_family_colors"])
    colors[OTHER_FAMILIES] = NEUTRAL
    colors[OTHER_BIASES] = NEUTRAL
    return colors


FAMILY_COLORS = load_palette()


def blend(color: str, target: str = "#FFFFFF", fraction: float = 0.3) -> str:
    source_rgb = np.array([int(color[index:index + 2], 16) for index in (1, 3, 5)])
    target_rgb = np.array([int(target[index:index + 2], 16) for index in (1, 3, 5)])
    mixed = np.rint(source_rgb * (1 - fraction) + target_rgb * fraction).astype(int)
    return "#" + "".join(f"{value:02X}" for value in mixed)


def text_color(fill: str) -> str:
    rgb = np.array([int(fill[index:index + 2], 16) / 255 for index in (1, 3, 5)])
    linear = np.where(rgb <= 0.04045, rgb / 12.92, ((rgb + 0.055) / 1.055) ** 2.4)
    luminance = float(np.dot(linear, [0.2126, 0.7152, 0.0722]))
    return TEXT if luminance > 0.45 else WHITE


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, float, float]:
    leaf = pd.read_csv(INPUT_CSV)
    numeric = [
        "allocated_channel_mass",
        "allocated_view_mass",
        "channel_memberships",
        "positive_view_channel_memberships",
    ]
    for column in numeric:
        leaf[column] = pd.to_numeric(leaf[column], errors="raise")
    channel_total = float(leaf["allocated_channel_mass"].sum())
    view_total = float(leaf["allocated_view_mass"].sum())
    language_family = (
        leaf.groupby(["language_display", "yt_family"], as_index=False)
        .agg(
            allocated_channel_mass=("allocated_channel_mass", "sum"),
            allocated_view_mass=("allocated_view_mass", "sum"),
            channel_memberships=("channel_memberships", "sum"),
            positive_view_channel_memberships=("positive_view_channel_memberships", "sum"),
        )
    )
    language_family["channel_share"] = language_family["allocated_channel_mass"] / channel_total
    language_family["view_share"] = language_family["allocated_view_mass"] / view_total
    language_family["bias"] = language_family["channel_share"] - language_family["view_share"]
    language_family["bias_pp"] = 100 * language_family["bias"]
    language_family["exposure_multiplier"] = np.divide(
        language_family["view_share"],
        language_family["channel_share"],
        out=np.full(len(language_family), np.nan),
        where=language_family["channel_share"] > 0,
    )
    return leaf, language_family, channel_total, view_total


def display_languages(language_family: pd.DataFrame) -> list[str]:
    totals = language_family.groupby("language_display")[["channel_share", "view_share"]].sum()
    top_channels = [
        language for language in totals.sort_values("channel_share", ascending=False).index
        if language != UNDETERMINED
    ][:TOP_K]
    top_views = [
        language for language in totals.sort_values("view_share", ascending=False).index
        if language != UNDETERMINED
    ][:TOP_K]
    return list(dict.fromkeys([*top_channels, *top_views]))


def pooled_base(language_family: pd.DataFrame, keep_languages: list[str]) -> pd.DataFrame:
    frame = language_family.copy()
    frame["display_language"] = np.where(
        frame["language_display"].isin(keep_languages) | (frame["language_display"] == UNDETERMINED),
        frame["language_display"],
        OTHER_LANGUAGES,
    )
    grouped = (
        frame.groupby(["display_language", "yt_family"], as_index=False)
        .agg(
            channel_share=("channel_share", "sum"),
            view_share=("view_share", "sum"),
            allocated_channel_mass=("allocated_channel_mass", "sum"),
            allocated_view_mass=("allocated_view_mass", "sum"),
        )
    )
    grouped["bias"] = grouped["channel_share"] - grouped["view_share"]
    grouped["bias_pp"] = 100 * grouped["bias"]
    grouped["exposure_multiplier"] = grouped["view_share"] / grouped["channel_share"]
    return grouped


def prune_base(frame: pd.DataFrame, value_column: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for language, group in frame.groupby("display_language", sort=False):
        small = group[value_column] < BASE_MIN_TOTAL_SHARE
        for row in group.loc[~small].itertuples(index=False):
            rows.append(row._asdict())
        if small.any():
            pooled = group.loc[small]
            record = {
                "display_language": language,
                "yt_family": OTHER_FAMILIES,
                "channel_share": float(pooled["channel_share"].sum()),
                "view_share": float(pooled["view_share"].sum()),
                "allocated_channel_mass": float(pooled["allocated_channel_mass"].sum()),
                "allocated_view_mass": float(pooled["allocated_view_mass"].sum()),
            }
            record["bias"] = record["channel_share"] - record["view_share"]
            record["bias_pp"] = 100 * record["bias"]
            record["exposure_multiplier"] = record["view_share"] / record["channel_share"]
            rows.append(record)
    result = pd.DataFrame(rows)
    if not math.isclose(float(result[value_column].sum()), 1.0, abs_tol=1e-10):
        raise AssertionError(f"Pruned {value_column} does not conserve total mass")
    return result


def bias_parts(language_family: pd.DataFrame, keep_languages: list[str]) -> pd.DataFrame:
    frame = language_family.loc[language_family["bias"] != 0].copy()
    frame["direction"] = np.where(frame["bias"] > 0, "Overstated", "Understated")
    frame["bias_mass"] = frame["bias"].abs()
    frame["display_language"] = np.where(
        frame["language_display"].isin(keep_languages) | (frame["language_display"] == UNDETERMINED),
        frame["language_display"],
        OTHER_LANGUAGES,
    )
    return (
        frame.groupby(["direction", "display_language", "yt_family"], as_index=False)
        .agg(
            bias_mass=("bias_mass", "sum"),
            channel_share=("channel_share", "sum"),
            view_share=("view_share", "sum"),
            source_cells=("language_display", "size"),
        )
    )


def prune_bias_side(frame: pd.DataFrame, total_variation: float) -> pd.DataFrame:
    language_totals = frame.groupby("display_language")["bias_mass"].sum()
    small_languages = set(language_totals[language_totals < BIAS_MIN_LANGUAGE_FRAC * total_variation].index)
    collapsed = frame.copy()
    collapsed["display_language"] = np.where(
        collapsed["display_language"].isin(small_languages), OTHER_LANGUAGES, collapsed["display_language"]
    )
    collapsed = (
        collapsed.groupby(["direction", "display_language", "yt_family"], as_index=False)
        .agg(
            bias_mass=("bias_mass", "sum"),
            channel_share=("channel_share", "sum"),
            view_share=("view_share", "sum"),
            source_cells=("source_cells", "sum"),
        )
    )
    rows: list[dict[str, object]] = []
    for (direction, language), group in collapsed.groupby(["direction", "display_language"], sort=False):
        small = group["bias_mass"] < BIAS_MIN_CELL_FRAC * total_variation
        for row in group.loc[~small].itertuples(index=False):
            rows.append(row._asdict())
        if small.any():
            pooled = group.loc[small]
            rows.append({
                "direction": direction,
                "display_language": language,
                "yt_family": OTHER_BIASES,
                "bias_mass": float(pooled["bias_mass"].sum()),
                "channel_share": float(pooled["channel_share"].sum()),
                "view_share": float(pooled["view_share"].sum()),
                "source_cells": int(pooled["source_cells"].sum()),
            })
    result = pd.DataFrame(rows)
    if not math.isclose(float(result["bias_mass"].sum()), total_variation, abs_tol=1e-10):
        raise AssertionError("Static bias pruning does not conserve side mass")
    return result


def padded(rectangle: dict[str, float], pad: float) -> dict[str, float]:
    width = max(0.0, rectangle["dx"] - 2 * pad)
    height = max(0.0, rectangle["dy"] - 2 * pad)
    return {"x": rectangle["x"] + pad, "y": rectangle["y"] + pad, "dx": width, "dy": height}


def nested_layout(
    frame: pd.DataFrame,
    value_column: str,
    width: float = 100.0,
    height: float = 62.0,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    language_totals = (
        frame.groupby("display_language", as_index=False)[value_column]
        .sum()
        .sort_values([value_column, "display_language"], ascending=[False, True])
    )
    language_rects = squarify.squarify(
        squarify.normalize_sizes(language_totals[value_column].tolist(), width, height),
        0,
        0,
        width,
        height,
    )
    languages: list[dict[str, object]] = []
    cells: list[dict[str, object]] = []
    for language_row, raw_rectangle in zip(language_totals.itertuples(index=False), language_rects):
        language = str(language_row.display_language)
        rectangle = padded(raw_rectangle, 0.35)
        value = float(getattr(language_row, value_column))
        languages.append({"display_language": language, "value": value, **rectangle})
        group = frame.loc[frame["display_language"] == language].sort_values(
            [value_column, "yt_family"], ascending=[False, True]
        )
        family_rects = squarify.squarify(
            squarify.normalize_sizes(group[value_column].tolist(), rectangle["dx"], rectangle["dy"]),
            rectangle["x"],
            rectangle["y"],
            rectangle["dx"],
            rectangle["dy"],
        )
        for family_row, family_rectangle in zip(group.itertuples(index=False), family_rects):
            cells.append({
                **family_row._asdict(),
                **padded(family_rectangle, 0.16),
            })
    return languages, cells


def add_stroked_text(axis: plt.Axes, x: float, y: float, text: str, **kwargs: object) -> None:
    artist = axis.text(x, y, text, **kwargs)
    artist.set_path_effects([path_effects.withStroke(linewidth=2.2, foreground=WHITE, alpha=0.9)])


def draw_nested(
    axis: plt.Axes,
    frame: pd.DataFrame,
    value_column: str,
    label_mode: str,
    record_scope: str,
) -> tuple[int, float]:
    languages, cells = nested_layout(frame, value_column)
    total = float(frame[value_column].sum())
    language_fraction = {
        str(language["display_language"]): float(language["value"]) / total
        for language in languages
    }
    for cell in cells:
        family = str(cell["yt_family"])
        fill = FAMILY_COLORS.get(family, NEUTRAL)
        axis.add_patch(patches.Rectangle(
            (cell["x"], cell["y"]), cell["dx"], cell["dy"],
            facecolor=fill, edgecolor=WHITE, linewidth=0,
        ))
        area_fraction = float(cell[value_column]) / total
        family_label_allowed = (
            label_mode != "bias"
            or language_fraction[str(cell["display_language"])] >= 0.04
        )
        if family_label_allowed and area_fraction >= (0.018 if label_mode == "comparison" else 0.025):
            if label_mode == "bias":
                label = f"{family}\n{100 * float(cell[value_column]):.2f} pp"
                fits = cell["dx"] >= 0.62 * max(len(family), 7) and cell["dy"] >= 4.0
            else:
                label = family
                fits = cell["dx"] >= 0.58 * len(family) and cell["dy"] >= 2.4
            if fits:
                axis.text(
                    cell["x"] + cell["dx"] / 2,
                    cell["y"] + cell["dy"] / 2,
                    label,
                    ha="center",
                    va="center",
                    color=text_color(fill),
                    fontsize=7.2 if label_mode == "comparison" else 7.5,
                    fontweight="semibold",
                    clip_on=True,
                )
        record = dict(cell)
        record["scope"] = record_scope
        _CELL_RECORDS.append(record)

    for language in languages:
        area_fraction = float(language["value"]) / total
        language_minimum = 0.018 if label_mode == "bias" else 0.012
        if area_fraction < language_minimum:
            continue
        language_name = str(language["display_language"])
        label = language_name
        if label_mode == "bias":
            value_label = f"{100 * float(language['value']):.2f} pp"
            label = f"{language_name}\n{value_label}"
            fits = language["dx"] >= 0.85 * max(len(language_name), len(value_label)) and language["dy"] >= 3.4
        else:
            fits = language["dx"] >= 0.72 * len(language_name) and language["dy"] >= 2.6
        if not fits:
            continue
        add_stroked_text(
            axis,
            language["x"] + 0.5,
            language["y"] + 1.25,
            label,
            ha="left",
            va="top",
            color=TEXT,
            fontsize=7.7 if label_mode == "bias" else 8.5,
            fontweight="bold",
            clip_on=True,
        )
    axis.set_xlim(0, 100)
    axis.set_ylim(0, 62)
    axis.invert_yaxis()
    axis.set_aspect("equal")
    axis.axis("off")
    minimum = min(float(cell[value_column]) / total for cell in cells)
    return len(cells), minimum


_CELL_RECORDS: list[dict[str, object]] = []


def family_legend(figure: plt.Figure, y: float) -> None:
    handles = [
        patches.Patch(facecolor=color, edgecolor="none", label=family)
        for family, color in FAMILY_COLORS.items()
        if family not in {OTHER_FAMILIES, OTHER_BIASES}
    ]
    figure.legend(
        handles=handles,
        loc="lower center",
        bbox_to_anchor=(0.5, y),
        ncol=8,
        frameon=False,
        fontsize=8.5,
        handlelength=1.2,
        columnspacing=1.3,
    )


def render_comparison(base: pd.DataFrame, channel_total: float, view_total: float) -> dict[str, float]:
    channel_frame = prune_base(base, "channel_share")
    view_frame = prune_base(base, "view_share")
    figure = plt.figure(figsize=(15, 8.8), facecolor=WHITE)
    left = figure.add_axes([0.035, 0.17, 0.45, 0.68])
    right = figure.add_axes([0.515, 0.17, 0.45, 0.68])
    left_cells, left_min = draw_nested(left, channel_frame, "channel_share", "comparison", "equal_channels")
    right_cells, right_min = draw_nested(right, view_frame, "view_share", "comparison", "view_weighted")

    figure.text(0.035, 0.955, "The same YouTube census tells two different stories", fontsize=21,
                fontweight="bold", color=TEXT, ha="left")
    figure.text(
        0.035,
        0.918,
        "Language -> topic family. Left: every channel contributes one unit. Right: area follows positive four-week views.",
        fontsize=11,
        color=MUTED,
        ha="left",
    )
    figure.text(0.035, 0.875, "COUNT CHANNELS EQUALLY", color=OVER_COLOR, fontsize=12,
                fontweight="bold", ha="left")
    figure.text(0.035, 0.85, f"{channel_total:,.0f} channels", color=MUTED, fontsize=10, ha="left")
    figure.text(0.515, 0.875, "WEIGHT BY WHAT VIEWERS SEE", color=UNDER_COLOR, fontsize=12,
                fontweight="bold", ha="left")
    figure.text(0.515, 0.85, f"{view_total / 1e12:.3f} trillion positive four-week views", color=MUTED,
                fontsize=10, ha="left")
    figure.text(
        0.035,
        0.10,
        "Equal weighting halves Entertainment's apparent share (34.1% -> 17.4%) while inflating Music, Society, "
        "Knowledge, Gaming, and unlabeled channels.",
        fontsize=10,
        color=TEXT,
        ha="left",
    )
    figure.text(
        0.035,
        0.045,
        "Full >=10K-subscriber cohort at 2026-06-15. Raw family-balanced allocations; no named-channel display overrides. "
        "Tail languages and tiny family cells are pooled only for display.",
        fontsize=8.5,
        color=MUTED,
        ha="left",
    )
    family_legend(figure, 0.115)
    figure.savefig(COMPARISON_PNG, dpi=220, facecolor=WHITE)
    figure.savefig(COMPARISON_SVG, facecolor=WHITE)
    plt.close(figure)
    return {
        "equal_cells": left_cells,
        "view_cells": right_cells,
        "equal_min_cell_fraction": left_min,
        "view_min_cell_fraction": right_min,
    }


def render_bias_balance(parts: pd.DataFrame, total_variation: float) -> dict[str, float]:
    over = prune_bias_side(parts.loc[parts["direction"] == "Overstated"], total_variation)
    under = prune_bias_side(parts.loc[parts["direction"] == "Understated"], total_variation)
    figure = plt.figure(figsize=(15, 8.8), facecolor=WHITE)
    left = figure.add_axes([0.035, 0.17, 0.45, 0.68])
    right = figure.add_axes([0.515, 0.17, 0.45, 0.68])
    left_cells, left_min = draw_nested(left, over, "bias_mass", "bias", "overstated")
    right_cells, right_min = draw_nested(right, under, "bias_mass", "bias", "understated")

    figure.text(0.035, 0.957, f"Equal channel weights put {100 * total_variation:.1f}% of exposure in the wrong cells",
                fontsize=21, fontweight="bold", color=TEXT, ha="left")
    figure.text(
        0.035,
        0.918,
        "Each side contains the same displaced mass. Tile area is absolute language-family bias in percentage points.",
        fontsize=11,
        color=MUTED,
        ha="left",
    )
    figure.text(0.035, 0.875, "LOOKS TOO LARGE WHEN CHANNELS COUNT EQUALLY", color=OVER_COLOR,
                fontsize=12, fontweight="bold", ha="left")
    figure.text(0.035, 0.85, f"Overstated mass: {100 * total_variation:.2f} percentage points", color=MUTED,
                fontsize=10, ha="left")
    figure.text(0.515, 0.875, "LOOKS TOO SMALL WHEN CHANNELS COUNT EQUALLY", color=UNDER_COLOR,
                fontsize=12, fontweight="bold", ha="left")
    figure.text(0.515, 0.85, f"Understated mass: {100 * total_variation:.2f} percentage points", color=MUTED,
                fontsize=10, ha="left")
    figure.text(
        0.035,
        0.095,
        "The imbalance is diffuse on the left but concentrated on the right: English Entertainment alone is "
        "understated by 9.75 points and English Lifestyle by 4.78 points.",
        fontsize=10,
        color=TEXT,
        ha="left",
    )
    figure.text(
        0.035,
        0.045,
        "Population comparison, not a sample: the figure isolates estimand distortion. Total variation is one-half "
        "the sum of absolute share differences across language-family cells.",
        fontsize=8.5,
        color=MUTED,
        ha="left",
    )
    family_legend(figure, 0.11)
    figure.savefig(BIAS_PNG, dpi=220, facecolor=WHITE)
    figure.savefig(BIAS_SVG, facecolor=WHITE)
    plt.close(figure)
    return {
        "over_cells": left_cells,
        "under_cells": right_cells,
        "over_min_cell_fraction": left_min,
        "under_min_cell_fraction": right_min,
        "over_mass": float(over["bias_mass"].sum()),
        "under_mass": float(under["bias_mass"].sum()),
    }


def base_trace(frame: pd.DataFrame, metric: str, name: str, visible: bool) -> go.Treemap:
    language_totals = frame.groupby("display_language")[["channel_share", "view_share"]].sum()
    ids = [f"{metric}:root"]
    labels = ["All >=10K channels"]
    parents = [""]
    values = [1.0]
    colors = [WHITE]
    custom = [[1.0, 1.0, 0.0, 1.0]]
    for language, language_row in language_totals.sort_values(metric, ascending=False).iterrows():
        language_id = f"{metric}:lang:{language}"
        ids.append(language_id)
        labels.append(language)
        parents.append(f"{metric}:root")
        values.append(float(language_row[metric]))
        colors.append("#F4F1EB" if language != OTHER_LANGUAGES else "#E9E3DA")
        multiplier = language_row["view_share"] / language_row["channel_share"]
        custom.append([
            float(language_row["channel_share"]),
            float(language_row["view_share"]),
            100 * float(language_row["channel_share"] - language_row["view_share"]),
            float(multiplier),
        ])
        children = frame.loc[frame["display_language"] == language].sort_values(metric, ascending=False)
        for row in children.itertuples(index=False):
            ids.append(f"{metric}:cell:{language}:{row.yt_family}")
            labels.append(str(row.yt_family))
            parents.append(language_id)
            values.append(float(getattr(row, metric)))
            colors.append(FAMILY_COLORS.get(str(row.yt_family), NEUTRAL))
            custom.append([
                float(row.channel_share),
                float(row.view_share),
                float(row.bias_pp),
                float(row.exposure_multiplier),
            ])
    return go.Treemap(
        name=name,
        ids=ids,
        labels=labels,
        parents=parents,
        values=values,
        branchvalues="total",
        maxdepth=2,
        sort=True,
        tiling={"packing": "squarify", "pad": 2, "squarifyratio": 1},
        marker={"colors": colors, "line": {"width": 0}},
        customdata=np.asarray(custom),
        texttemplate="%{label}<br>%{value:.1%}",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Equal-channel share: %{customdata[0]:.2%}<br>"
            "View-weighted share: %{customdata[1]:.2%}<br>"
            "Equal minus view: %{customdata[2]:+.2f} pp<br>"
            "View exposure multiplier: %{customdata[3]:.2f}x<extra></extra>"
        ),
        visible=visible,
    )


def bias_trace(parts: pd.DataFrame, total_variation: float) -> go.Treemap:
    ids = ["bias:root"]
    labels = ["Two-sided absolute distortion"]
    parents = [""]
    values = [2 * total_variation]
    colors = [WHITE]
    custom = [[1.0, 1.0, 0.0, 1.0]]
    for direction, direction_color in [("Overstated", OVER_COLOR), ("Understated", UNDER_COLOR)]:
        direction_frame = parts.loc[parts["direction"] == direction]
        direction_id = f"bias:{direction}"
        ids.append(direction_id)
        labels.append("Looks too large" if direction == "Overstated" else "Looks too small")
        parents.append("bias:root")
        values.append(total_variation)
        colors.append(direction_color)
        custom.append([
            float(direction_frame["channel_share"].sum()),
            float(direction_frame["view_share"].sum()),
            100 * total_variation * (1 if direction == "Overstated" else -1),
            np.nan,
        ])
        language_totals = direction_frame.groupby("display_language").agg(
            bias_mass=("bias_mass", "sum"),
            channel_share=("channel_share", "sum"),
            view_share=("view_share", "sum"),
        ).sort_values("bias_mass", ascending=False)
        for language, language_row in language_totals.iterrows():
            language_id = f"bias:{direction}:lang:{language}"
            ids.append(language_id)
            labels.append(language)
            parents.append(direction_id)
            values.append(float(language_row["bias_mass"]))
            colors.append(blend(direction_color, fraction=0.18))
            multiplier = language_row["view_share"] / language_row["channel_share"]
            custom.append([
                float(language_row["channel_share"]),
                float(language_row["view_share"]),
                100 * float(language_row["channel_share"] - language_row["view_share"]),
                float(multiplier),
            ])
            children = direction_frame.loc[direction_frame["display_language"] == language].sort_values(
                "bias_mass", ascending=False
            )
            for row in children.itertuples(index=False):
                ids.append(f"bias:{direction}:cell:{language}:{row.yt_family}")
                labels.append(str(row.yt_family))
                parents.append(language_id)
                values.append(float(row.bias_mass))
                colors.append(blend(direction_color, fraction=0.34))
                multiplier = float(row.view_share / row.channel_share) if row.channel_share else np.nan
                custom.append([
                    float(row.channel_share),
                    float(row.view_share),
                    100 * float(row.channel_share - row.view_share),
                    multiplier,
                ])
    return go.Treemap(
        name="Weighting distortion",
        ids=ids,
        labels=labels,
        parents=parents,
        values=values,
        branchvalues="total",
        maxdepth=3,
        sort=True,
        tiling={"packing": "squarify", "pad": 2, "squarifyratio": 1},
        marker={"colors": colors, "line": {"width": 0}},
        customdata=np.asarray(custom),
        texttemplate="%{label}<br>%{value:.2%}",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Absolute displaced mass: %{value:.2%}<br>"
            "Equal-channel share: %{customdata[0]:.2%}<br>"
            "View-weighted share: %{customdata[1]:.2%}<br>"
            "Equal minus view: %{customdata[2]:+.2f} pp<br>"
            "View exposure multiplier: %{customdata[3]:.2f}x<extra></extra>"
        ),
        visible=True,
    )


def render_interactive(base: pd.DataFrame, parts: pd.DataFrame, total_variation: float) -> int:
    figure = go.Figure(data=[
        base_trace(base, "channel_share", "Equal channels", False),
        base_trace(base, "view_share", "View weighted", False),
        bias_trace(parts, total_variation),
    ])
    figure.update_layout(
        title={
            "text": (
                "<b>Weighting bias lens</b><br>"
                "<span style='font-size:13px;color:#65717C'>Same >=10K channel census; switch the estimand or inspect displaced mass</span>"
            ),
            "x": 0.02,
            "xanchor": "left",
        },
        margin={"l": 16, "r": 16, "t": 120, "b": 46},
        height=860,
        paper_bgcolor=WHITE,
        plot_bgcolor=WHITE,
        font={"family": "Arial, sans-serif", "color": TEXT, "size": 13},
        updatemenus=[{
            "type": "buttons",
            "direction": "right",
            "x": 0.02,
            "y": 1.055,
            "xanchor": "left",
            "yanchor": "top",
            "showactive": True,
            "active": 2,
            "buttons": [
                {"label": "Equal channels", "method": "update", "args": [{"visible": [True, False, False]}]},
                {"label": "View weighted", "method": "update", "args": [{"visible": [False, True, False]}]},
                {"label": "Distortion", "method": "update", "args": [{"visible": [False, False, True]}]},
            ],
        }],
        annotations=[{
            "text": (
                f"Language-family total variation: <b>{100 * total_variation:.2f} pp</b>. "
                "Positive four-week views; tail languages pooled for display."
            ),
            "x": 0.02,
            "y": -0.035,
            "xref": "paper",
            "yref": "paper",
            "showarrow": False,
            "xanchor": "left",
            "font": {"size": 11, "color": MUTED},
        }],
    )
    figure.write_html(
        INTERACTIVE_HTML,
        include_plotlyjs=True,
        full_html=True,
        config={"displaylogo": False, "responsive": True, "scrollZoom": False},
    )
    return sum(len(trace.ids) for trace in figure.data)


def main() -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    leaf, language_family, channel_total, view_total = load_data()
    keep_languages = display_languages(language_family)
    base = pooled_base(language_family, keep_languages)
    parts = bias_parts(language_family, keep_languages)

    total_variation_lf = 0.5 * float(language_family["bias"].abs().sum())
    leaf_channel_share = leaf["allocated_channel_mass"] / channel_total
    leaf_view_share = leaf["allocated_view_mass"] / view_total
    total_variation_leaf = 0.5 * float((leaf_channel_share - leaf_view_share).abs().sum())
    language = language_family.groupby("language_display")[["channel_share", "view_share"]].sum()
    family = language_family.groupby("yt_family")[["channel_share", "view_share"]].sum()
    total_variation_language = 0.5 * float((language["channel_share"] - language["view_share"]).abs().sum())
    total_variation_family = 0.5 * float((family["channel_share"] - family["view_share"]).abs().sum())

    def finish_summary(frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        result["bias_pp"] = 100 * (result["channel_share"] - result["view_share"])
        result["absolute_bias_pp"] = result["bias_pp"].abs()
        result["view_exposure_multiplier"] = result["view_share"] / result["channel_share"]
        return result.sort_values("absolute_bias_pp", ascending=False)

    finish_summary(language).to_csv(LANGUAGE_SUMMARY_CSV)
    finish_summary(family).to_csv(FAMILY_SUMMARY_CSV)
    finish_summary(
        language_family.set_index(["language_display", "yt_family"])[["channel_share", "view_share"]]
    ).to_csv(LANGUAGE_FAMILY_SUMMARY_CSV)

    if not math.isclose(float(language_family["channel_share"].sum()), 1.0, abs_tol=1e-10):
        raise AssertionError("Channel shares do not conserve")
    if not math.isclose(float(language_family["view_share"].sum()), 1.0, abs_tol=1e-10):
        raise AssertionError("View shares do not conserve")
    over_mass = float(language_family.loc[language_family["bias"] > 0, "bias"].sum())
    under_mass = float(-language_family.loc[language_family["bias"] < 0, "bias"].sum())
    if not math.isclose(over_mass, under_mass, abs_tol=1e-10):
        raise AssertionError("Positive and negative bias mass do not balance")

    comparison = render_comparison(base, channel_total, view_total)
    bias = render_bias_balance(parts, total_variation_lf)
    interactive_nodes = render_interactive(base, parts, total_variation_lf)
    pd.DataFrame(_CELL_RECORDS).to_csv(CELLS_CSV, index=False)

    image_dimensions = {
        "comparison": Image.open(COMPARISON_PNG).size,
        "bias_balance": Image.open(BIAS_PNG).size,
    }
    export_manifest = json.loads(EXPORT_MANIFEST.read_text())
    manifest = {
        "analysis_version": "youtube_topic_treemap_weighting_bias_20260716_v1",
        "source_statement_id": export_manifest["statement_id"],
        "source_table": export_manifest["source_table"],
        "cohort_channels": channel_total,
        "positive_4wk_views": view_total,
        "source_leaf_rows": len(leaf),
        "source_languages": int(language_family["language_display"].nunique()),
        "source_families": int(language_family["yt_family"].nunique()),
        "display_languages": [*keep_languages, UNDETERMINED, OTHER_LANGUAGES],
        "total_variation": {
            "language": total_variation_language,
            "family": total_variation_family,
            "language_family": total_variation_lf,
            "language_family_leaf": total_variation_leaf,
        },
        "bias_symmetry": {"overstated_mass": over_mass, "understated_mass": under_mass},
        "comparison": comparison,
        "bias_balance": bias,
        "interactive_nodes_across_three_modes": interactive_nodes,
        "image_dimensions": image_dimensions,
        "artifacts": {
            "comparison_png": str(COMPARISON_PNG.relative_to(ROOT)),
            "comparison_svg": str(COMPARISON_SVG.relative_to(ROOT)),
            "bias_png": str(BIAS_PNG.relative_to(ROOT)),
            "bias_svg": str(BIAS_SVG.relative_to(ROOT)),
            "interactive_html": str(INTERACTIVE_HTML.relative_to(ROOT)),
            "cells_csv": str(CELLS_CSV.relative_to(ROOT)),
            "language_summary_csv": str(LANGUAGE_SUMMARY_CSV.relative_to(ROOT)),
            "family_summary_csv": str(FAMILY_SUMMARY_CSV.relative_to(ROOT)),
            "language_family_summary_csv": str(LANGUAGE_FAMILY_SUMMARY_CSV.relative_to(ROOT)),
        },
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    log(f"COHORT CHANNEL MASS: {channel_total:,.0f}")
    log(f"POSITIVE 4-WEEK VIEWS: {view_total:,.0f}")
    log(f"LANGUAGE TV: {100 * total_variation_language:.3f}%")
    log(f"FAMILY TV: {100 * total_variation_family:.3f}%")
    log(f"LANGUAGE-FAMILY TV: {100 * total_variation_lf:.3f}%")
    log(f"LANGUAGE-FAMILY-LEAF TV: {100 * total_variation_leaf:.3f}%")
    log(f"BIAS SYMMETRY: over={100 * over_mass:.6f}% under={100 * under_mass:.6f}%")
    log(f"COMPARISON CELLS: {comparison['equal_cells']} equal / {comparison['view_cells']} view")
    log(f"BIAS CELLS: {bias['over_cells']} over / {bias['under_cells']} under")
    log(f"INTERACTIVE NODES (3 traces): {interactive_nodes}")
    log(f"COMPARISON PNG: {COMPARISON_PNG}")
    log(f"BIAS PNG: {BIAS_PNG}")
    log(f"INTERACTIVE HTML: {INTERACTIVE_HTML}")
    log("CONSERVATION: PASS")
    LOG_PATH.write_text("\n".join(_LOG) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
