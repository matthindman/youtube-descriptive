#!/usr/bin/env python3
"""Render design-weighted full-frame treemaps and weighting-difference plots.

The input is the compact publication export produced by analysis stage
``publish_treemap``. Rectangle geometry uses calibrated additive totals; hover
and coefficient plots use the unmodified design-based estimates and standard
errors.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go

try:
    from scripts import render_treemap_v3 as base
except ModuleNotFoundError:  # Direct execution places scripts/ on sys.path.
    import render_treemap_v3 as base


ROOT = Path(__file__).resolve().parents[1]
LANGUAGE_NAMES = ROOT / "config" / "iso639_language_names.csv"

MEASURES = {
    "attention": {
        "prefix": "view",
        "geometry": "view_geometry_total",
        "title": "Estimated YouTube viewing, by language and topic",
        "area": "estimated four-week view share",
        "estimator": "census >=10k plus Poisson PPS below 10k",
    },
    "channels": {
        "prefix": "channel",
        "geometry": "channel_geometry_total",
        "title": "Estimated YouTube channels, by language and topic",
        "area": "estimated channel share",
        "estimator": "census >=10k plus SRS below 10k",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cells", required=True, type=Path)
    parser.add_argument("--publication-estimates", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--allocation-variant")
    parser.add_argument("--population-scope")
    parser.add_argument("--measure", choices=("attention", "channels", "both"), default="both")
    parser.add_argument("--static-cell-cap", type=int)
    parser.add_argument("--leaf-min-frac", type=float)
    parser.add_argument("--artifact-tag", default="full_frame_weighted_v1")
    return parser.parse_args()


def read_compact_frame(path: Path) -> pd.DataFrame:
    if path.is_dir():
        parts = sorted(path.glob("part-*.parquet"))
        if len(parts) != 1:
            raise RuntimeError(f"Expected one coalesced Parquet part in {path}; found {len(parts)}")
        path = parts[0]
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path)


def language_name_map() -> dict[str, str]:
    names = pd.read_csv(LANGUAGE_NAMES, dtype=str)
    result = dict(zip(names["language_code"].str.lower(), names["display_name"]))
    result["und"] = "Undetermined"
    return result


def add_language_names(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    mapping = language_name_map()
    codes = result["language"].fillna("und").astype(str).str.lower()
    result["language"] = codes
    result[base.DISPLAY_COL] = codes.map(mapping).fillna(codes.str.upper())
    return result


def validate_inputs(
    cells: pd.DataFrame, publication: pd.DataFrame, measures: tuple[str, ...]
) -> None:
    cell_required = {
        "allocation_variant",
        "population_scope",
        "language",
        "family",
        "leaf",
    }
    for measure in measures:
        prefix = MEASURES[measure]["prefix"]
        cell_required.update(
            {
                MEASURES[measure]["geometry"],
                f"{prefix}_raw_share",
                f"{prefix}_standard_error",
                f"{prefix}_ci95_lower",
                f"{prefix}_ci95_upper",
                f"{prefix}_effective_contributing_n",
                f"{prefix}_largest_weighted_contribution",
                f"{prefix}_headline_reliable",
                f"{prefix}_geometry_calibration_basis",
            }
        )
    publication_required = {"taxonomy_level"}
    if set(measures) == {"attention", "channels"}:
        publication_required.update(
            {
                "view_minus_channel_share",
                "difference_standard_error",
                "difference_ci95_lower",
                "difference_ci95_upper",
                "view_headline_reliable",
                "channel_headline_reliable",
            }
        )
    missing_cells = sorted(cell_required - set(cells.columns))
    missing_publication = sorted(publication_required - set(publication.columns))
    if missing_cells or missing_publication:
        raise RuntimeError(
            f"Publication export schema mismatch; cells missing={missing_cells}; "
            f"publication missing={missing_publication}"
        )


def select_analysis(
    frame: pd.DataFrame, allocation_variant: str, population_scope: str
) -> pd.DataFrame:
    selected = frame.loc[
        (frame["allocation_variant"] == allocation_variant)
        & (frame["population_scope"] == population_scope)
    ].copy()
    if selected.empty:
        raise RuntimeError(
            f"No rows for allocation_variant={allocation_variant!r}, "
            f"population_scope={population_scope!r}"
        )
    return add_language_names(selected)


def renderer_rows(cells: pd.DataFrame, measure: str) -> pd.DataFrame:
    geometry = MEASURES[measure]["geometry"]
    rows = cells.copy()
    rows[base.VALUE_COL] = pd.to_numeric(rows[geometry], errors="coerce").fillna(0.0)
    rows["yt_family"] = rows["family"].fillna("Unlabeled").astype(str)
    rows["yt_leaf"] = rows["leaf"].fillna("").astype(str)
    missing_leaf = rows["yt_leaf"].str.len() == 0
    rows.loc[missing_leaf, "yt_leaf"] = (
        "[" + rows.loc[missing_leaf, "yt_family"] + "] - unspecified"
    )
    rows["channel_id"] = [f"__aggregate_{index}" for index in range(len(rows))]
    rows["channel_title"] = rows["channel_id"]
    rows[base.CHANNEL_VIEW_COL] = rows[base.VALUE_COL]
    rows[base.RAW_WEIGHT_COL] = 1.0
    rows[base.WEIGHT_COL] = 1.0
    rows["is_placement_override"] = False
    rows["needs_review"] = False
    rows["needs_manual_review"] = False
    rows["is_other_channel_pool"] = False
    rows["pooled_channel_count"] = 0
    rows["raw_topic_categories"] = None
    return rows


def configure_static(
    output_dir: Path,
    tag: str,
    measure: str,
    manifest: dict,
    static_cell_cap: int | None = None,
    leaf_min_frac: float | None = None,
) -> None:
    spec = MEASURES[measure]
    population_scope = str(manifest.get("population_scope", "known_subscriber"))
    baseline = (
        "Exact >=10k census plus subscriber-unknown certainty rows"
        if population_scope == "all_retrievable"
        else "Exact >=10k census"
    )
    base.OUT_DIR = output_dir
    base.STATIC_PNG = output_dir / f"treemap_static_master_{measure}_{tag}.png"
    base.STATIC_SVG = output_dir / f"treemap_static_master_{measure}_{tag}.svg"
    base.CELLS_CSV = output_dir / f"treemap_static_cells_{measure}_{tag}.csv"
    base.STATIC_TITLE = spec["title"]
    provisional = manifest.get("publication_status") == "provisional_pending_remainder_deepseek"
    status_note = " Provisional PPS expansion; remainder DeepSeek labels pending." if provisional else ""
    base.STATIC_SUBTITLE = (
        f"Full frozen channel frame, 15 June-13 July 2026. Area = {spec['area']}; "
        f"color = content family; shades = subtopics. {baseline} plus design-weighted "
        f"below-10k sample.{status_note}"
    )
    label_source = (
        "frozen exact labels, completed exact-stratum dual-LID agreements, and final PPS labels"
        if provisional
        else "final LID labels"
    )
    base.STATIC_FOOTER = (
        f"Source: frozen channel panel, {label_source}, and YouTube topicCategories. "
        f"Estimator: {spec['estimator']}. Geometry is calibrated for additivity; raw "
        "design-based estimates and intervals are retained in the explorer and tables."
    )
    base.SOURCE_DESCRIPTION = (
        f"frozen channel panel, {label_source}, and YouTube topicCategories"
    )
    base.STATIC_INCLUDE_SUBTOPICS = True
    treemap_config = manifest.get("treemap", {})
    base.TOP_K_LANGUAGES = int(treemap_config.get("static_top_languages", 12))
    base.STATIC_CELL_CAP = int(
        static_cell_cap
        if static_cell_cap is not None
        else treemap_config.get("static_cell_cap", 250)
    )
    base.LEAF_MIN = float(
        leaf_min_frac
        if leaf_min_frac is not None
        else treemap_config.get("static_leaf_min_frac", 0.003)
    )


def render_static(
    cells: pd.DataFrame,
    measure: str,
    output_dir: Path,
    tag: str,
    manifest: dict,
    static_cell_cap: int | None = None,
    leaf_min_frac: float | None = None,
) -> dict[str, float | int | str]:
    configure_static(
        output_dir,
        tag,
        measure,
        manifest,
        static_cell_cap,
        leaf_min_frac,
    )
    full = renderer_rows(cells, measure)
    detail_suppressed: set[tuple[str, str]] = set()
    for _ in range(40):
        language_cells, total, top_languages, language_totals, pooled = base.build_static_tree(
            full, set(), set(), detail_suppressed
        )
        placed = base.layout_static(language_cells)
        if len(placed) <= base.STATIC_CELL_CAP:
            break
        optional = sorted(
            (
                (cell.value, cell.language, cell.family)
                for cell, _rect, _path in placed
                if cell.detail_rescued and (cell.language, cell.family) not in detail_suppressed
            ),
            key=lambda item: (item[0], item[1], item[2]),
        )
        if not optional:
            raise RuntimeError(
                f"Static tree has {len(placed)} cells above cap {base.STATIC_CELL_CAP} "
                "with no optional family detail left to pool"
            )
        _, language, family = optional[0]
        detail_suppressed.add((language, family))
    else:
        raise RuntimeError("Static pruning did not converge")

    stats, dimensions = base.draw_static(language_cells, total, 0.0, placed)
    static_rows = pd.DataFrame(stats.rows)
    static_rows.to_csv(base.CELLS_CSV, index=False)
    structural = static_rows.loc[
        static_rows["level"].isin(["language", "family", "leaf"])
    ]
    ordinary = structural.loc[
        ~structural[
            [
                "forced",
                "priority_topic",
                "coverage_rescued",
                "detail_rescued",
                "coverage_residual",
            ]
        ].fillna(False).any(axis=1)
    ]
    min_area = float(ordinary["area_frac"].min() * 100.0)
    family_rows = static_rows.loc[
        static_rows["level"].isin(["family", "leaf"])
    ]
    aspect = np.maximum(
        family_rows["dx"] / family_rows["dy"], family_rows["dy"] / family_rows["dx"]
    )
    thin_slivers = int((aspect > 8.0).sum())
    pooled_share = float(pooled / total * 100.0)
    print(f"STATIC MASTER ({measure.upper()}): {base.STATIC_PNG.resolve()}")
    print(f"STATIC SVG ({measure.upper()}): {base.STATIC_SVG.resolve()}")
    print(f"STATIC CELLS: {len(static_rows)}")
    print(f"MIN CELL AREA: {min_area:.3f}%")
    pooled_label = "POOLED VIEW SHARE" if measure == "attention" else "POOLED CHANNEL SHARE"
    print(f"{pooled_label}: {pooled_share:.3f}%")
    print(f"LABELED CELLS: {stats.labeled_cells}")
    print("PACKING: squarify")
    print(f"FIGURE DIMENSIONS: {dimensions[0]}x{dimensions[1]} px")
    print(
        "AUTOMATED LEGIBILITY: "
        + ("PASS" if thin_slivers == 0 else f"REVIEW ({thin_slivers} family tiles exceed 8:1)")
    )
    return {
        "measure": measure,
        "png": str(base.STATIC_PNG.resolve()),
        "svg": str(base.STATIC_SVG.resolve()),
        "cells": int(len(static_rows)),
        "min_cell_area_pct": min_area,
        "pooled_share_pct": pooled_share,
        "labeled_cells": int(stats.labeled_cells),
        "thin_slivers": thin_slivers,
        "width_px": int(dimensions[0]),
        "height_px": int(dimensions[1]),
        "top_languages": list(top_languages),
        "top_language_mass": {str(k): float(v) for k, v in language_totals.items()},
    }


def fmt_percent(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value) * 100:.3f}%"


def render_interactive(cells: pd.DataFrame, measure: str, output_dir: Path, tag: str) -> Path:
    spec = MEASURES[measure]
    prefix = spec["prefix"]
    geometry = spec["geometry"]
    output = output_dir / f"treemap_interactive_{measure}_{tag}.html"
    pos = cells.loc[pd.to_numeric(cells[geometry], errors="coerce").fillna(0) > 0].copy()
    if pos.empty:
        raise RuntimeError(f"No positive {measure} geometry")

    ids: list[str] = []
    labels: list[str] = []
    parents: list[str] = []
    values: list[float] = []
    colors: list[str] = []
    custom: list[list[object]] = []

    def add(node_id: str, label: str, parent: str, value: float, color: str, data: list[object]) -> None:
        ids.append(node_id)
        labels.append(label)
        parents.append(parent)
        values.append(float(value))
        colors.append(color)
        custom.append(data)

    language_totals = pos.groupby(["language", base.DISPLAY_COL], observed=True)[geometry].sum()
    for (code, name), value in language_totals.sort_values(ascending=False).items():
        lid = f"lang::{base.safe_id_part(code)}"
        add(lid, str(name), "", value, base.LANGUAGE_FILL, ["language", name, "", "", fmt_percent(value / pos[geometry].sum()), "", "", "", "", "", ""])

    family_totals = pos.groupby(
        ["language", base.DISPLAY_COL, "family"], observed=True
    )[geometry].sum()
    for (code, name, family), value in family_totals.items():
        lid = f"lang::{base.safe_id_part(code)}"
        fid = f"fam::{base.safe_id_part(code)}::{base.safe_id_part(family)}"
        add(
            fid,
            base.display_label("family", str(family)),
            lid,
            value,
            base.family_base_color(str(family)),
            ["family", name, base.display_label("family", str(family)), "", fmt_percent(value / pos[geometry].sum()), "", "", "", "", "", ""],
        )

    for row in pos.itertuples(index=False):
        code = str(row.language)
        family = str(row.family)
        leaf = str(row.leaf)
        fid = f"fam::{base.safe_id_part(code)}::{base.safe_id_part(family)}"
        leaf_id = f"leaf::{base.safe_id_part(code)}::{base.safe_id_part(family)}::{base.safe_id_part(leaf)}"
        value = float(getattr(row, geometry))
        reliable = bool(getattr(row, f"{prefix}_headline_reliable"))
        add(
            leaf_id,
            base.display_label("leaf", leaf),
            fid,
            value,
            base.leaf_color(family, leaf, 0, 1),
            [
                "subtopic",
                getattr(row, base.DISPLAY_COL),
                base.display_label("family", family),
                base.display_label("leaf", leaf),
                fmt_percent(value / pos[geometry].sum()),
                fmt_percent(getattr(row, f"{prefix}_raw_share")),
                fmt_percent(getattr(row, f"{prefix}_standard_error")),
                f"[{fmt_percent(getattr(row, f'{prefix}_ci95_lower'))}, {fmt_percent(getattr(row, f'{prefix}_ci95_upper'))}]",
                f"{float(getattr(row, f'{prefix}_effective_contributing_n')):,.1f}",
                fmt_percent(getattr(row, f"{prefix}_largest_weighted_contribution")),
                "PASS" if reliable else "POOL / NOT HEADLINE",
            ],
        )

    if len(ids) != len(set(ids)):
        raise RuntimeError("Interactive node IDs are not unique")
    value_by_id = dict(zip(ids, values))
    child_totals: dict[str, float] = {}
    for parent, value in zip(parents, values):
        if parent:
            child_totals[parent] = child_totals.get(parent, 0.0) + value
    for parent, child_total in child_totals.items():
        tolerance = max(1.0, value_by_id[parent] * 1e-10)
        if child_total > value_by_id[parent] + tolerance:
            raise RuntimeError(f"branchvalues=total violation at {parent}")

    hover = (
        "<b>%{label}</b><br>Type: %{customdata[0]}<br>Language: %{customdata[1]}<br>"
        "Family: %{customdata[2]}<br>Subtopic: %{customdata[3]}<br>Calibrated area share: %{customdata[4]}<br>"
        "Raw design-based share: %{customdata[5]}<br>Raw SE: %{customdata[6]}<br>Raw 95% CI: %{customdata[7]}<br>"
        "Tail effective n: %{customdata[8]}<br>Largest weighted contribution: %{customdata[9]}<br>"
        "Reliability gate: %{customdata[10]}<extra></extra>"
    )
    figure = go.Figure(
        go.Treemap(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            maxdepth=2,
            sort=True,
            tiling={"packing": "squarify", "pad": base.TILING_PAD, "squarifyratio": 1},
            marker={"colors": colors, "line": {"width": 0, "color": "white"}},
            customdata=custom,
            hovertemplate=hover,
            textinfo="label",
        )
    )
    figure.update_layout(
        title=f"{spec['title']} | click a family to inspect subtopics",
        width=1500,
        height=950,
        margin={"l": 8, "r": 8, "t": 55, "b": 8},
        uniformtext={"minsize": 10, "mode": "hide"},
    )
    figure.write_html(output, include_plotlyjs=True, full_html=True)
    external_scripts = len(re.findall(r"<script[^>]+src=", output.read_text(errors="ignore")))
    if external_scripts:
        raise RuntimeError("Interactive HTML is not self-contained")
    print(f"INTERACTIVE HTML ({measure.upper()}): {output.resolve()}")
    print("INTERACTIVE: go.Treemap branchvalues=total maxdepth=2 packing=squarify sort=True")
    return output


def coefficient_label(row: pd.Series) -> str:
    level = row["taxonomy_level"]
    if level == "language":
        return str(row[base.DISPLAY_COL])
    if level == "family":
        return base.display_label("family", str(row["family"]))
    return f"{base.display_label('family', str(row['family']))}: {base.display_label('leaf', str(row['leaf']))}"


def render_weighting_difference(publication: pd.DataFrame, output_dir: Path, tag: str) -> dict:
    levels = [("language", 16), ("family", 20), ("leaf", 16)]
    selected_frames: list[pd.DataFrame] = []
    fig, axes = plt.subplots(1, 3, figsize=(14.5, 7.5), dpi=220)
    for ax, (level, cap) in zip(axes, levels):
        frame = publication.loc[publication["taxonomy_level"] == level].copy()
        frame["label"] = frame.apply(coefficient_label, axis=1)
        frame = frame.sort_values(
            "view_minus_channel_share", key=lambda values: values.abs(), ascending=False
        ).head(cap)
        frame = frame.sort_values("view_minus_channel_share")
        frame["plot_level"] = level
        selected_frames.append(frame)
        y = np.arange(len(frame))
        estimate = frame["view_minus_channel_share"].to_numpy(float) * 100
        lower = frame["difference_ci95_lower"].to_numpy(float) * 100
        upper = frame["difference_ci95_upper"].to_numpy(float) * 100
        colors = np.where(estimate >= 0, "#0072B2", "#D55E00")
        reliable = (
            frame["view_headline_reliable"].fillna(False).astype(bool)
            & frame["channel_headline_reliable"].fillna(False).astype(bool)
        ).to_numpy()
        ax.hlines(y, lower, upper, color="#8A8F94", linewidth=1.0, zorder=1)
        ax.scatter(
            estimate[reliable],
            y[reliable],
            c=colors[reliable],
            s=28,
            edgecolor="white",
            linewidth=0.4,
            zorder=2,
        )
        ax.scatter(
            estimate[~reliable],
            y[~reliable],
            facecolor="white",
            edgecolor=colors[~reliable],
            s=28,
            linewidth=1.0,
            zorder=2,
        )
        ax.axvline(0, color="#3E4347", linewidth=0.8)
        ax.set_yticks(y, frame["label"], fontsize=7.2)
        ax.set_title({"language": "Languages", "family": "Topic families", "leaf": "Subtopics"}[level], fontsize=10, fontweight="bold")
        ax.set_xlabel("View share minus channel share (percentage points)", fontsize=7.5)
        ax.grid(axis="x", color="#E6E6E6", linewidth=0.6)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(axis="x", labelsize=7)
        ax.tick_params(axis="y", length=0)
    fig.suptitle(
        "How view weighting changes the estimated composition of YouTube",
        x=0.01,
        ha="left",
        fontsize=14,
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.945,
        "Positive values are more prominent in viewing than in an equal-channel portrait. Lines are approximate 95% design intervals; hollow points fail a reliability gate.",
        fontsize=8,
        color="#4A4F54",
    )
    fig.tight_layout(rect=[0, 0.02, 1, 0.92], w_pad=2.0)
    png = output_dir / f"weighting_difference_coefficients_{tag}.png"
    svg = output_dir / f"weighting_difference_coefficients_{tag}.svg"
    fig.savefig(png, facecolor="white")
    fig.savefig(svg, facecolor="white")
    plt.close(fig)
    plotted = pd.concat(selected_frames, ignore_index=True)
    csv_path = output_dir / f"weighting_difference_coefficients_{tag}.csv"
    plotted.to_csv(csv_path, index=False)

    all_levels = publication.loc[publication["taxonomy_level"].isin(["language", "family", "leaf"])]
    total_variation = {
        level: float(group["view_minus_channel_share"].abs().sum() / 2.0)
        for level, group in all_levels.groupby("taxonomy_level", observed=True)
    }
    summary = {
        "coefficient_png": str(png.resolve()),
        "coefficient_svg": str(svg.resolve()),
        "coefficient_csv": str(csv_path.resolve()),
        "total_variation_distance": total_variation,
        "difference_se_note": "SRS and PPS sampling components treated as independent; joint measurement-error replication remains required.",
    }
    summary_path = output_dir / f"weighting_difference_summary_{tag}.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(f"WEIGHTING-DIFFERENCE PLOT: {png.resolve()}")
    return summary


def main() -> None:
    args = parse_args()
    manifest = json.loads(args.manifest.read_text())
    allocation_variant = args.allocation_variant or manifest["primary_allocation_variant"]
    population_scope = args.population_scope or manifest["primary_population_scope"]
    cells_all = read_compact_frame(args.cells)
    publication_all = read_compact_frame(args.publication_estimates)
    measures = ("attention", "channels") if args.measure == "both" else (args.measure,)
    validate_inputs(cells_all, publication_all, measures)
    cells = select_analysis(cells_all, allocation_variant, population_scope)
    publication = select_analysis(publication_all, allocation_variant, population_scope)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    conservation = {}
    for measure in measures:
        prefix = MEASURES[measure]["prefix"]
        total = float(cells[f"{prefix}_geometry_global_share"].sum())
        conservation[measure] = total
        if not math.isclose(total, 1.0, abs_tol=1e-8):
            raise RuntimeError(f"CONSERVATION: FAIL {measure}={total:.12f}")
    print("CONSERVATION: PASS")
    print(f"ANALYSIS: {allocation_variant}; {population_scope}")

    source_treemap = manifest.get("treemap", {})
    effective_static_cell_cap = int(
        args.static_cell_cap
        if args.static_cell_cap is not None
        else source_treemap.get("static_cell_cap", 250)
    )
    effective_leaf_min_frac = float(
        args.leaf_min_frac
        if args.leaf_min_frac is not None
        else source_treemap.get("static_leaf_min_frac", 0.003)
    )
    artifact_manifest: dict[str, object] = {
        "source_manifest": str(args.manifest.resolve()),
        "allocation_variant": allocation_variant,
        "population_scope": population_scope,
        "analysis_mode": manifest.get("analysis_mode", "full"),
        "publication_status": manifest.get("publication_status", "final_dual_sample"),
        "geometry_vs_inference": "calibrated additive totals define area; raw design-based estimates define SE/CI",
        "treemap": {
            "static_top_languages": int(
                source_treemap.get("static_top_languages", 12)
            ),
            "static_cell_cap": effective_static_cell_cap,
            "static_leaf_min_frac": effective_leaf_min_frac,
        },
        "artifacts": {},
    }
    for measure in measures:
        static = render_static(
            cells,
            measure,
            args.output_dir,
            args.artifact_tag,
            artifact_manifest,
            effective_static_cell_cap,
            effective_leaf_min_frac,
        )
        interactive = render_interactive(cells, measure, args.output_dir, args.artifact_tag)
        artifact_manifest["artifacts"][measure] = {**static, "interactive": str(interactive.resolve())}
    if set(measures) == {"attention", "channels"}:
        artifact_manifest["weighting_difference"] = render_weighting_difference(
            publication, args.output_dir, args.artifact_tag
        )
    artifact_manifest_path = args.output_dir / f"artifact_manifest_{args.artifact_tag}.json"
    artifact_manifest_path.write_text(json.dumps(artifact_manifest, indent=2, sort_keys=True) + "\n")
    print(f"ARTIFACT MANIFEST: {artifact_manifest_path.resolve()}")


if __name__ == "__main__":
    main()
