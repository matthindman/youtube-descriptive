#!/usr/bin/env python3
"""Summarize and plot how the PPS-expanded view distribution differs from the census.

Rectangle geometry is the calibrated additive estimate used by the treemap. Sampling
uncertainty comes from the unmodified Horvitz-Thompson PPS estimate. The >=10K census
is measured without sampling error, so the reported change SE is the PPS-expanded
share SE. Proportional rankings exclude negligible census baselines and list cells
with a zero census baseline separately.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LANGUAGE_NAMES = ROOT / "config" / "iso639_language_names.csv"
Z95 = 1.959963984540054

LEVELS = {
    "language": ["language"],
    "family": ["family"],
    "leaf": ["family", "leaf"],
    "language_family": ["language", "family"],
    "language_family_leaf": ["language", "family", "leaf"],
}
LEVEL_TITLES = {
    "language": "Languages",
    "family": "Topic families",
    "leaf": "Subtopics",
    "language_family": "Language x topic family",
    "language_family_leaf": "Language x subtopic",
}
MIN_BASELINE_SHARE = {
    "language": 0.00005,
    "family": 0.00005,
    "leaf": 0.00002,
    "language_family": 0.00001,
    "language_family_leaf": 0.000005,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cells", required=True, type=Path)
    parser.add_argument("--publication-estimates", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--allocation-variant")
    parser.add_argument("--population-scope")
    parser.add_argument("--artifact-tag", default="full_frame_weighted_v1")
    return parser.parse_args()


def read_frame(path: Path) -> pd.DataFrame:
    if path.is_dir():
        parts = sorted(path.glob("part-*.parquet"))
        if len(parts) != 1:
            raise RuntimeError(
                f"Expected one Parquet part in {path}; found {len(parts)}"
            )
        path = parts[0]
    return pd.read_csv(path) if path.suffix.lower() == ".csv" else pd.read_parquet(path)


def language_names() -> dict[str, str]:
    frame = pd.read_csv(LANGUAGE_NAMES, dtype=str)
    result = dict(zip(frame["language_code"].str.lower(), frame["display_name"]))
    result["und"] = "Undetermined"
    return result


def cell_label(row: pd.Series, level: str, names: dict[str, str]) -> str:
    language = names.get(
        str(row.get("language", "und")).lower(), str(row.get("language", "")).upper()
    )
    family = str(row.get("family", ""))
    leaf = str(row.get("leaf", ""))
    if level == "language":
        return language
    if level == "family":
        return family
    if level == "leaf":
        return f"{family}: {leaf}"
    if level == "language_family":
        return f"{language} | {family}"
    return f"{language} | {family}: {leaf}"


def validate_inputs(cells: pd.DataFrame, publication: pd.DataFrame) -> None:
    cell_columns = {
        "allocation_variant",
        "population_scope",
        "language",
        "family",
        "leaf",
        "view_geometry_total",
    }
    publication_columns = {
        "allocation_variant",
        "population_scope",
        "taxonomy_level",
        "language",
        "family",
        "leaf",
        "view_head_total",
        "view_raw_share",
        "view_standard_error",
        "view_headline_reliable",
        "view_effective_contributing_n",
    }
    missing_cells = sorted(cell_columns - set(cells.columns))
    missing_publication = sorted(publication_columns - set(publication.columns))
    if missing_cells or missing_publication:
        raise RuntimeError(
            f"Expansion-change schema mismatch; cells missing={missing_cells}; "
            f"publication missing={missing_publication}"
        )


def select_domain(
    frame: pd.DataFrame, allocation_variant: str, population_scope: str
) -> pd.DataFrame:
    result = frame.loc[
        (frame["allocation_variant"] == allocation_variant)
        & (frame["population_scope"] == population_scope)
    ].copy()
    if result.empty:
        raise RuntimeError(
            f"No rows for allocation_variant={allocation_variant!r}, "
            f"population_scope={population_scope!r}"
        )
    return result


def baseline_label(population_scope: str, *, compact: bool = False) -> str:
    if population_scope == "known_subscriber":
        return ">=10K census" if compact else "exact >=10K census"
    if population_scope == "all_retrievable":
        return (
            "non-tail baseline"
            if compact
            else "exact non-tail baseline (>=10K census plus subscriber-unknown certainty rows)"
        )
    return population_scope.replace("_", " ")


def build_changes(cells: pd.DataFrame, publication: pd.DataFrame) -> pd.DataFrame:
    names = language_names()
    geometry_total = float(
        pd.to_numeric(cells["view_geometry_total"], errors="coerce").sum()
    )
    if geometry_total <= 0:
        raise RuntimeError("Treemap cells contain no positive view geometry")
    outputs: list[pd.DataFrame] = []
    for level, keys in LEVELS.items():
        geometry = (
            cells.groupby(keys, observed=True, dropna=False)["view_geometry_total"]
            .sum()
            .rename("platform_total_calibrated")
            .reset_index()
        )
        estimates = publication.loc[publication["taxonomy_level"] == level].copy()
        estimates = estimates.merge(
            geometry, on=keys, how="outer", validate="one_to_one"
        )
        for column in ("view_head_total", "view_raw_share", "view_standard_error"):
            estimates[column] = pd.to_numeric(
                estimates[column], errors="coerce"
            ).fillna(0.0)
        estimates["platform_total_calibrated"] = pd.to_numeric(
            estimates["platform_total_calibrated"], errors="coerce"
        ).fillna(0.0)
        census_total = float(estimates["view_head_total"].sum())
        if census_total <= 0:
            raise RuntimeError(
                f"No positive census view total at taxonomy level {level}"
            )
        estimates["taxonomy_level"] = level
        estimates["census_share"] = estimates["view_head_total"] / census_total
        estimates["platform_share"] = (
            estimates["platform_total_calibrated"] / geometry_total
        )
        estimates["absolute_change"] = (
            estimates["platform_share"] - estimates["census_share"]
        )
        estimates["raw_absolute_change"] = (
            estimates["view_raw_share"] - estimates["census_share"]
        )
        estimates["raw_ht_change_standard_error"] = estimates["view_standard_error"]
        exact_topic_margin = (
            level in {"family", "leaf"}
            and publication["allocation_variant"].dropna().eq("platform_only").all()
        )
        estimates["change_standard_error"] = (
            0.0 if exact_topic_margin else estimates["view_standard_error"]
        )
        estimates["sampling_inference_basis"] = (
            "exact frozen-frame topic/view margin; sampling SE = 0"
            if exact_topic_margin
            else "PPS Horvitz-Thompson tail estimate; census contribution exact"
        )
        estimates["change_ci95_lower"] = (
            estimates["absolute_change"] - Z95 * estimates["change_standard_error"]
        )
        estimates["change_ci95_upper"] = (
            estimates["absolute_change"] + Z95 * estimates["change_standard_error"]
        )
        positive_baseline = estimates["census_share"] > 0
        estimates["platform_to_census_ratio"] = np.where(
            positive_baseline,
            estimates["platform_share"] / estimates["census_share"],
            np.nan,
        )
        estimates["proportional_change"] = estimates["platform_to_census_ratio"] - 1.0
        estimates["proportional_change_standard_error"] = np.where(
            positive_baseline,
            estimates["change_standard_error"] / estimates["census_share"],
            np.nan,
        )
        estimates["ratio_ci95_lower"] = np.where(
            positive_baseline,
            np.maximum(
                0.0,
                estimates["platform_share"] - Z95 * estimates["change_standard_error"],
            )
            / estimates["census_share"],
            np.nan,
        )
        estimates["ratio_ci95_upper"] = np.where(
            positive_baseline,
            (estimates["platform_share"] + Z95 * estimates["change_standard_error"])
            / estimates["census_share"],
            np.nan,
        )
        estimates["log2_ratio"] = np.where(
            estimates["platform_to_census_ratio"] > 0,
            np.log2(estimates["platform_to_census_ratio"]),
            np.nan,
        )
        estimates["log2_ratio_ci95_lower"] = np.where(
            estimates["ratio_ci95_lower"] > 0,
            np.log2(estimates["ratio_ci95_lower"]),
            np.nan,
        )
        estimates["log2_ratio_ci95_upper"] = np.where(
            estimates["ratio_ci95_upper"] > 0,
            np.log2(estimates["ratio_ci95_upper"]),
            np.nan,
        )
        reliable = estimates.get("view_headline_reliable", False)
        if not isinstance(reliable, pd.Series):
            reliable = pd.Series(False, index=estimates.index)
        estimates["headline_reliable"] = (
            True if exact_topic_margin else reliable.fillna(False).astype(bool)
        )
        estimates["proportional_ranking_eligible"] = (
            estimates["headline_reliable"]
            & (estimates["census_share"] >= MIN_BASELINE_SHARE[level])
            & (estimates["platform_share"] >= MIN_BASELINE_SHARE[level])
        )
        estimates["zero_census_baseline"] = (~positive_baseline) & (
            estimates["platform_share"] > 0
        )
        estimates["label"] = estimates.apply(
            lambda row: cell_label(row, level, names), axis=1
        )
        outputs.append(estimates)
    result = pd.concat(outputs, ignore_index=True, sort=False)
    return result.sort_values(["taxonomy_level", "absolute_change", "label"])


def fmt_pct(value: float, digits: int = 3) -> str:
    return f"{value * 100:.{digits}f}%"


def fmt_pp(value: float, digits: int = 3) -> str:
    return f"{value * 100:+.{digits}f} pp"


def markdown_table(frame: pd.DataFrame, proportional: bool) -> list[str]:
    if frame.empty:
        return ["No eligible cells."]
    lines = [
        "| Cell | Baseline share | Platform share | Change | SE | Relative change |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in frame.itertuples(index=False):
        relative = (
            "new"
            if math.isnan(float(row.proportional_change))
            else f"{row.proportional_change * 100:+.1f}%"
        )
        if proportional and not row.proportional_ranking_eligible:
            relative += "*"
        lines.append(
            f"| {row.label} | {fmt_pct(row.census_share)} | {fmt_pct(row.platform_share)} | "
            f"{fmt_pp(row.absolute_change)} | {fmt_pct(row.change_standard_error)} | {relative} |"
        )
    return lines


def write_markdown_summary(changes: pd.DataFrame, output: Path) -> None:
    scopes = changes["population_scope"].dropna().unique().tolist()
    if len(scopes) != 1:
        raise RuntimeError(f"Expected one population scope; found {scopes}")
    scope = str(scopes[0])
    baseline = baseline_label(scope)
    language = changes.loc[changes["taxonomy_level"] == "language"]
    platform_total = float(language["platform_total_calibrated"].sum())
    baseline_total = float(language["view_head_total"].sum())
    tail_share = 1.0 - baseline_total / platform_total
    lines = [
        "# PPS Expansion Changes in Estimated YouTube Viewing",
        "",
        f"The baseline column is the composition of the {baseline}. "
        "The platform column adds the below-10K frame using the Poisson PPS design and calibrated "
        f"treemap geometry. The below-10K tail contributes {fmt_pct(tail_share, 2)} of the resulting "
        "observed four-week view mass. Language and language-by-topic SEs are design-based "
        "Horvitz-Thompson sampling SEs; the baseline component has no sampling error. Topic-family "
        "and subtopic marginals use exact full-frame topic/view margins and therefore have zero PPS "
        "sampling SE. These intervals do not include topic measurement or classification error. "
        "Proportional rankings require a non-negligible baseline and the registered "
        "headline reliability gate.",
        "",
    ]
    for level in LEVELS:
        frame = changes.loc[changes["taxonomy_level"] == level].copy()
        lines.extend(
            [f"## {LEVEL_TITLES[level]}", "", "### Largest percentage-point growth", ""]
        )
        lines.extend(
            markdown_table(frame.nlargest(5, "absolute_change"), proportional=False)
        )
        lines.extend(["", "### Largest percentage-point decline", ""])
        lines.extend(
            markdown_table(frame.nsmallest(5, "absolute_change"), proportional=False)
        )
        eligible = frame.loc[frame["proportional_ranking_eligible"]].copy()
        lines.extend(["", "### Largest proportional growth", ""])
        lines.extend(
            markdown_table(
                eligible.nlargest(5, "proportional_change"), proportional=True
            )
        )
        lines.extend(["", "### Largest proportional decline", ""])
        lines.extend(
            markdown_table(
                eligible.nsmallest(5, "proportional_change"), proportional=True
            )
        )
        new_cells = frame.loc[frame["zero_census_baseline"]].nlargest(
            5, "platform_share"
        )
        if not new_cells.empty:
            lines.extend(["", "### Largest cells absent from the baseline", ""])
            lines.extend(markdown_table(new_cells, proportional=False))
        lines.append("")
    lines.extend(
        [
            "## Interpretation Notes",
            "",
            "- Percentage-point change is the primary comparison because it remains defined for cells absent from the census.",
            "- Proportional change can be unstable for tiny baselines; starred or ineligible cells are not used for proportional rankings.",
            "- Calibrated geometry is used for the displayed platform composition. Raw HT shares and SEs remain in the CSV alongside the sampling SE appropriate to each displayed comparison.",
            "- Topic-family and subtopic marginals use the full frozen frame's YouTube topic arrays and have no PPS sampling error; language-by-topic allocation is estimated from PPS labels.",
            "- No interval shown here includes topic missingness, taxonomy validity, language-classification error, or other measurement error.",
            "",
        ]
    )
    output.write_text("\n".join(lines), encoding="utf-8")


def select_plot_rows(
    frame: pd.DataFrame, metric: str, per_side: int = 6
) -> pd.DataFrame:
    valid = frame.loc[np.isfinite(frame[metric])].copy()
    if metric == "log2_ratio":
        valid = valid.loc[valid["proportional_ranking_eligible"]]
    selected = pd.concat(
        [valid.nlargest(per_side, metric), valid.nsmallest(per_side, metric)],
        ignore_index=True,
    ).drop_duplicates("label")
    return selected.sort_values(metric)


def plot_panel(
    ax, frame: pd.DataFrame, level: str, metric: str, baseline_compact: str
) -> None:
    plotted = select_plot_rows(frame, metric)
    y = np.arange(len(plotted))
    if metric == "absolute_change":
        estimate = plotted[metric].to_numpy(float) * 100.0
        lower = plotted["change_ci95_lower"].to_numpy(float) * 100.0
        upper = plotted["change_ci95_upper"].to_numpy(float) * 100.0
        xlabel = f"Platform minus {baseline_compact} (percentage points)"
    else:
        estimate = plotted[metric].to_numpy(float)
        lower = plotted["log2_ratio_ci95_lower"].to_numpy(float)
        upper = plotted["log2_ratio_ci95_upper"].to_numpy(float)
        lower = np.where(np.isfinite(lower), lower, estimate)
        upper = np.where(np.isfinite(upper), upper, estimate)
        xlabel = f"Platform / {baseline_compact} (log2 ratio)"
    colors = np.where(estimate >= 0, "#0072B2", "#D55E00")
    reliable = plotted["headline_reliable"].fillna(False).to_numpy(bool)
    ax.hlines(y, lower, upper, color="#A3A7AA", linewidth=1.0, zorder=1)
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
    ax.set_yticks(y, plotted["label"], fontsize=7.0)
    ax.set_title(LEVEL_TITLES[level], fontsize=10, fontweight="bold")
    ax.set_xlabel(xlabel, fontsize=7.3)
    ax.grid(axis="x", color="#E6E6E6", linewidth=0.6)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="x", labelsize=7)
    ax.tick_params(axis="y", length=0)
    if metric == "log2_ratio":
        ticks = np.array([-3, -2, -1, 0, 1, 2, 3], dtype=float)
        limits = ax.get_xlim()
        ticks = ticks[(ticks >= limits[0]) & (ticks <= limits[1])]
        ax.set_xticks(ticks, [f"{2**tick:g}x" for tick in ticks])


def render_change_figure(
    changes: pd.DataFrame,
    levels: list[str],
    metric: str,
    output_stem: Path,
) -> dict[str, str]:
    scopes = changes["population_scope"].dropna().unique().tolist()
    if len(scopes) != 1:
        raise RuntimeError(f"Expected one population scope; found {scopes}")
    baseline = baseline_label(str(scopes[0]), compact=True)
    width = 15.5 if len(levels) == 3 else 13.0
    fig, axes = plt.subplots(1, len(levels), figsize=(width, 8.2), dpi=220)
    axes = np.atleast_1d(axes)
    for ax, level in zip(axes, levels):
        plot_panel(
            ax,
            changes.loc[changes["taxonomy_level"] == level],
            level,
            metric,
            baseline,
        )
    title = (
        "How adding the below-10K tail changes estimated YouTube viewing"
        if metric == "absolute_change"
        else "Proportional change after adding the below-10K tail"
    )
    fig.suptitle(title, x=0.01, ha="left", fontsize=14, fontweight="bold")
    fig.text(
        0.01,
        0.947,
        f"Points compare calibrated PPS-expanded composition with the {baseline}; lines use approximate 95% design intervals. Topic-only margins are exact for the frozen frame. Hollow points fail the headline reliability gate.",
        fontsize=8,
        color="#4A4F54",
    )
    fig.tight_layout(rect=[0, 0.02, 1, 0.92], w_pad=2.2)
    png = output_stem.with_suffix(".png")
    svg = output_stem.with_suffix(".svg")
    fig.savefig(png, facecolor="white")
    fig.savefig(svg, facecolor="white")
    plt.close(fig)
    return {"png": str(png.resolve()), "svg": str(svg.resolve())}


def main() -> None:
    args = parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    allocation_variant = (
        args.allocation_variant or manifest["primary_allocation_variant"]
    )
    population_scope = args.population_scope or manifest["primary_population_scope"]
    cells = select_domain(read_frame(args.cells), allocation_variant, population_scope)
    publication = select_domain(
        read_frame(args.publication_estimates), allocation_variant, population_scope
    )
    validate_inputs(cells, publication)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    changes = build_changes(cells, publication)
    csv_path = args.output_dir / f"platform_expansion_changes_{args.artifact_tag}.csv"
    md_path = args.output_dir / f"platform_expansion_summary_{args.artifact_tag}.md"
    changes.to_csv(csv_path, index=False)
    write_markdown_summary(changes, md_path)

    artifacts = {
        "marginal_absolute": render_change_figure(
            changes,
            ["language", "family", "leaf"],
            "absolute_change",
            args.output_dir
            / f"platform_expansion_marginal_absolute_{args.artifact_tag}",
        ),
        "intersection_absolute": render_change_figure(
            changes,
            ["language_family", "language_family_leaf"],
            "absolute_change",
            args.output_dir
            / f"platform_expansion_intersection_absolute_{args.artifact_tag}",
        ),
        "marginal_proportional": render_change_figure(
            changes,
            ["language", "family", "leaf"],
            "log2_ratio",
            args.output_dir
            / f"platform_expansion_marginal_proportional_{args.artifact_tag}",
        ),
        "intersection_proportional": render_change_figure(
            changes,
            ["language_family", "language_family_leaf"],
            "log2_ratio",
            args.output_dir
            / f"platform_expansion_intersection_proportional_{args.artifact_tag}",
        ),
        "csv": str(csv_path.resolve()),
        "summary_markdown": str(md_path.resolve()),
        "allocation_variant": allocation_variant,
        "population_scope": population_scope,
        "proportional_min_baseline_share": MIN_BASELINE_SHARE,
        "interval_note": (
            "Calibrated geometry defines displayed point estimates. Topic-family and subtopic "
            "marginals are exact frozen-frame margins with zero PPS sampling SE; raw "
            "Horvitz-Thompson SEs define approximate intervals for language and intersections."
        ),
    }
    manifest_path = (
        args.output_dir / f"platform_expansion_manifest_{args.artifact_tag}.json"
    )
    manifest_path.write_text(
        json.dumps(artifacts, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"EXPANSION SUMMARY: {md_path.resolve()}")
    print(f"EXPANSION ESTIMATES: {csv_path.resolve()}")
    print(f"EXPANSION MANIFEST: {manifest_path.resolve()}")


if __name__ == "__main__":
    main()
