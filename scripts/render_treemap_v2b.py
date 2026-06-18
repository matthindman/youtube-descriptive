#!/usr/bin/env python3
"""Render the v2b YouTube topic treemap artifacts.

Outputs:
- static master PNG/SVG: language -> family only, aggressively pruned
- interactive HTML: language -> family -> leaf -> top channels plus pooled other
"""

from __future__ import annotations

import math
import re
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import squarify
from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs" / "youtube_topic_treemap_20260617_v2b"
ALLOC_PATH = OUT_DIR / "channel_label_allocations.parquet"
TRAFFIC_PATH = OUT_DIR / "traffic_4wk.parquet"
STATIC_PNG = OUT_DIR / "treemap_static_master_v2b.png"
STATIC_SVG = OUT_DIR / "treemap_static_master_v2b.svg"
INTERACTIVE_HTML = OUT_DIR / "treemap_interactive_explorer_v2b.html"

METHOD = "family_balanced"
TRAFFIC_SOURCE_TABLE = "dev_sean.default.yt_channel_stats"
TOO_CHANNEL_TABLE = "prod_tads.youtube_too.yt_sl_channels"
CHANNEL_VIEW_COL = "view_count_4wk"
VALUE_COL = "allocated_views_4wk"
TOP_K_LANGUAGES = 12
STATIC_MIN_FRAC = 0.003
STATIC_LABEL_FRAC = 0.015
STATIC_CELL_CAP = 120
INITIAL_FAMILY_POOL_FRAC = 0.01
TOP_CHANNELS_PER_LEAF = 15
FIG_W_IN = 20
FIG_H_IN = 12
FIG_DPI = 200
LAYOUT_W = 100.0
LAYOUT_H = 60.0


LANGUAGE_LABELS = {
    "en": "English (en)",
    "eng": "English (eng)",
    "en-US": "English, US (en-US)",
    "en-IN": "English, India (en-IN)",
    "und": "Undetermined",
    "es": "Spanish (es)",
    "spa": "Spanish (spa)",
    "pt-PT": "Portuguese, Portugal (pt-PT)",
    "pt-BR": "Portuguese, Brazil (pt-BR)",
    "por": "Portuguese (por)",
    "id": "Indonesian (id)",
    "ind": "Indonesian (ind)",
    "ar": "Arabic (ar)",
    "ara": "Arabic (ara)",
    "hi": "Hindi (hi)",
    "hin": "Hindi (hin)",
    "ru": "Russian (ru)",
    "rus": "Russian (rus)",
    "vi": "Vietnamese (vi)",
    "vie": "Vietnamese (vie)",
    "ko": "Korean (ko)",
    "kor": "Korean (kor)",
    "tr": "Turkish (tr)",
    "tur": "Turkish (tur)",
    "ja": "Japanese (ja)",
    "jpn": "Japanese (jpn)",
    "bn": "Bengali (bn)",
    "ben": "Bengali (ben)",
    "th": "Thai (th)",
    "tha": "Thai (tha)",
    "de": "German (de)",
    "deu": "German (deu)",
    "fr": "French (fr)",
    "fra": "French (fra)",
}

FAMILY_COLORS = {
    "Entertainment": "#E76F51",
    "Gaming": "#8A5FBF",
    "Knowledge": "#2A9D8F",
    "Lifestyle": "#E9C46A",
    "Music": "#457B9D",
    "Society": "#6C757D",
    "Sports": "#2F7D32",
    "Other / Unmapped YouTube topic": "#9C6644",
    "Unlabeled": "#ADB5BD",
    "Other (families)": "#CED4DA",
}

LANGUAGE_FILL = "#F8F9FA"
LANGUAGE_EDGE = "#343A40"
ROOT_ID = "all"


@dataclass(frozen=True)
class StaticBuild:
    languages: pd.DataFrame
    families: pd.DataFrame
    top_k: int
    family_pool_frac: float
    total_views: float
    static_cells: int
    min_cell_area_pct: float
    pooled_view_share_pct: float


def language_label(code: str) -> str:
    if code == "__other_languages__":
        return "Other languages"
    if code in LANGUAGE_LABELS:
        return LANGUAGE_LABELS[code]
    if code.endswith("_cluster"):
        return code.replace("_", " ").replace(" cluster", "").title()
    return str(code)


def family_color(family: str) -> str:
    return FAMILY_COLORS.get(family, FAMILY_COLORS["Other / Unmapped YouTube topic"])


def fmt_views(value: float) -> str:
    if value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.1f}T"
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    return f"{value:,.0f}"


def html_escape(value: object) -> str:
    import html

    return html.escape("" if value is None else str(value), quote=False)


def list_values(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, np.ndarray):
        return [str(v) for v in value.tolist()]
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value]
    try:
        if pd.isna(value):
            return []
    except (TypeError, ValueError):
        pass
    return [str(value)]


def raw_topic_slugs(value: object) -> str:
    slugs: list[str] = []
    for item in list_values(value):
        if "/wiki/" in item:
            slugs.append(item.rsplit("/wiki/", 1)[-1])
        else:
            slugs.append(item)
    return "; ".join(slugs)


def load_traffic() -> tuple[pd.DataFrame, dict[str, object]]:
    traffic = pd.read_parquet(TRAFFIC_PATH)
    raw_rows = int(len(traffic))
    raw_unique = int(traffic["channel_id"].nunique())
    duplicate_rows = raw_rows - raw_unique

    traffic = traffic.sort_values(
        ["channel_id", "current_collected_at", "prior_collected_at"],
        ascending=[True, False, False],
    ).drop_duplicates("channel_id", keep="first")

    summary = {
        "traffic_rows": raw_rows,
        "traffic_unique_channels": raw_unique,
        "traffic_duplicate_rows": duplicate_rows,
        "current_snapshot": str(traffic["current_collected_at"].dropna().dt.date.max()),
        "prior_snapshot": str(traffic["prior_collected_at"].dropna().dt.date.max()),
        "channels_with_current": int(traffic["current_lifetime_views"].notna().sum()),
        "channels_with_prior": int(traffic["prior_lifetime_views"].notna().sum()),
        "negative_raw_deltas": int(((traffic["raw_4wk_views"] < 0) & traffic["raw_4wk_views"].notna()).sum()),
        "channels_with_valid_4wk": int(traffic[CHANNEL_VIEW_COL].notna().sum()),
        "total_4wk_views": float(traffic[CHANNEL_VIEW_COL].sum()),
    }
    return traffic, summary


def load_allocations() -> pd.DataFrame:
    columns = [
        "channel_id",
        "channel_title",
        "language_code",
        "latest_views",
        "allocation_method",
        "yt_family",
        "yt_leaf",
        "leaf_slug",
        "allocation_weight",
        "raw_topic_categories",
        "normalized_slugs",
        "display_leaves",
        "node_type",
        "is_unmapped",
        "is_parent_unspecified",
        "is_unlabeled",
        "allocated_views",
    ]
    df = pd.read_parquet(ALLOC_PATH, columns=columns)
    df = df.loc[df["allocation_method"] == METHOD].copy()
    if df.empty:
        raise RuntimeError(f"No allocation rows found for method={METHOD!r}")

    df["channel_title"] = df["channel_title"].fillna(df["channel_id"])
    df["language_code"] = df["language_code"].fillna("und").astype(str)
    df["yt_family"] = df["yt_family"].fillna("Unlabeled").astype(str)
    df["yt_leaf"] = df["yt_leaf"].fillna("").astype(str)
    missing_leaf = df["yt_leaf"].str.len() == 0
    df.loc[missing_leaf, "yt_leaf"] = "[" + df.loc[missing_leaf, "yt_family"] + "] - unspecified"

    traffic, traffic_summary = load_traffic()
    traffic_cols = [
        "channel_id",
        "channel_name",
        "current_subscriber_count",
        "current_lifetime_views",
        "prior_lifetime_views",
        "current_collected_at",
        "prior_collected_at",
        "raw_4wk_views",
        CHANNEL_VIEW_COL,
        "avg_weekly_view_count",
    ]
    df = df.merge(traffic[traffic_cols], on="channel_id", how="left", validate="many_to_one")
    df["channel_title"] = df["channel_name"].fillna(df["channel_title"])
    df[VALUE_COL] = df[CHANNEL_VIEW_COL] * df["allocation_weight"]
    df.attrs["traffic_summary"] = traffic_summary
    return df


def assert_conservation(df: pd.DataFrame) -> None:
    valid = df.loc[df[CHANNEL_VIEW_COL].notna()].copy()
    if valid.empty:
        raise RuntimeError("CONSERVATION: FAIL no channels have valid 4-week traffic")
    channel = (
        valid.groupby("channel_id", observed=True)
        .agg(
            weight_sum=("allocation_weight", "sum"),
            allocated_sum=(VALUE_COL, "sum"),
            channel_views=(CHANNEL_VIEW_COL, "first"),
        )
        .reset_index()
    )
    weight_ok = np.allclose(channel["weight_sum"], 1.0, rtol=0, atol=1e-8)
    alloc_ok = np.allclose(
        channel["allocated_sum"],
        channel["channel_views"],
        rtol=1e-10,
        atol=1e-2,
    )
    total_alloc = float(channel["allocated_sum"].sum())
    total_channel = float(channel["channel_views"].sum())
    total_ok = math.isclose(total_alloc, total_channel, rel_tol=1e-12, abs_tol=1e-2)
    if not (weight_ok and alloc_ok and total_ok):
        max_weight_delta = float((channel["weight_sum"] - 1.0).abs().max())
        max_alloc_delta = float((channel["allocated_sum"] - channel["channel_views"]).abs().max())
        raise RuntimeError(
            "CONSERVATION: FAIL "
            f"max_weight_delta={max_weight_delta:.12g} "
            f"max_alloc_delta={max_alloc_delta:.12g} "
            f"total_delta={abs(total_alloc - total_channel):.12g}"
        )
    print("CONSERVATION: PASS")
    print(f"CONSERVATION TOTAL 4WK VIEWS: {total_alloc:,.0f}")


def build_static_data(
    df: pd.DataFrame,
    top_k: int = TOP_K_LANGUAGES,
    family_pool_frac: float = INITIAL_FAMILY_POOL_FRAC,
) -> StaticBuild:
    plot_df = df.loc[df[VALUE_COL] > 0].copy()
    if plot_df.empty:
        raise RuntimeError("No positive 4-week allocated views available for static treemap")
    total_views = float(plot_df[VALUE_COL].sum())
    min_abs = total_views * STATIC_MIN_FRAC

    while True:
        lang_totals_raw = (
            plot_df.groupby("language_code", observed=True)[VALUE_COL]
            .sum()
            .sort_values(ascending=False)
        )
        top_languages = set(lang_totals_raw.head(top_k).index.tolist())

        work = plot_df[["language_code", "yt_family", VALUE_COL]].copy()
        work["static_language"] = np.where(
            work["language_code"].isin(top_languages),
            work["language_code"],
            "__other_languages__",
        )
        pooled_language_views = float(
            work.loc[work["static_language"] == "__other_languages__", VALUE_COL].sum()
        )

        lang_order = (
            work.groupby("static_language", observed=True)[VALUE_COL]
            .sum()
            .sort_values(ascending=False)
        )
        language_rows = []
        family_rows = []
        pooled_family_views = 0.0

        for lang_code, lang_value in lang_order.items():
            fam_values = (
                work.loc[work["static_language"] == lang_code]
                .groupby("yt_family", observed=True)[VALUE_COL]
                .sum()
                .sort_values(ascending=False)
            )
            kept: list[tuple[str, float]] = []
            pool_value = 0.0
            for family, value in fam_values.items():
                value = float(value)
                if (value / lang_value) < family_pool_frac or (value / total_views) < STATIC_MIN_FRAC:
                    pool_value += value
                else:
                    kept.append((str(family), value))

            kept.sort(key=lambda item: item[1], reverse=True)
            while 0 < pool_value < min_abs and kept:
                moved_family, moved_value = kept.pop()
                pool_value += moved_value

            if not kept and pool_value == 0:
                pool_value = float(lang_value)

            family_total = sum(value for _, value in kept) + pool_value
            if not math.isclose(family_total, float(lang_value), rel_tol=1e-10, abs_tol=1e-2):
                raise RuntimeError(f"Static family sum mismatch for {lang_code}")

            language_rows.append(
                {
                    "language_code": lang_code,
                    "language_label": language_label(str(lang_code)),
                    "value": float(lang_value),
                }
            )
            for family, value in kept:
                family_rows.append(
                    {
                        "language_code": lang_code,
                        "language_label": language_label(str(lang_code)),
                        "family": family,
                        "value": value,
                        "is_pooled": False,
                    }
                )
            if pool_value > 0:
                family_rows.append(
                    {
                        "language_code": lang_code,
                        "language_label": language_label(str(lang_code)),
                        "family": "Other (families)",
                        "value": pool_value,
                        "is_pooled": True,
                    }
                )
                pooled_family_views += pool_value

        languages = pd.DataFrame(language_rows).sort_values("value", ascending=False).reset_index(drop=True)
        families = pd.DataFrame(family_rows).sort_values(
            ["language_code", "value"],
            ascending=[True, False],
        )

        cell_values = pd.concat([languages["value"], families["value"]], ignore_index=True)
        static_cells = int(len(languages) + len(families))
        min_cell_area_pct = float(cell_values.min() / total_views * 100)
        pooled_share = float((pooled_language_views + pooled_family_views) / total_views * 100)

        if static_cells <= STATIC_CELL_CAP and min_cell_area_pct >= STATIC_MIN_FRAC * 100:
            return StaticBuild(
                languages=languages,
                families=families.reset_index(drop=True),
                top_k=top_k,
                family_pool_frac=family_pool_frac,
                total_views=total_views,
                static_cells=static_cells,
                min_cell_area_pct=min_cell_area_pct,
                pooled_view_share_pct=pooled_share,
            )

        if family_pool_frac < 0.06:
            family_pool_frac += 0.005
        elif top_k > 6:
            top_k -= 1
            family_pool_frac = INITIAL_FAMILY_POOL_FRAC
        else:
            raise RuntimeError(
                "Unable to meet static pruning constraints: "
                f"cells={static_cells}, min_area={min_cell_area_pct:.3f}%"
            )


def padded_rect(rect: dict[str, float], pad: float) -> dict[str, float]:
    dx = max(0.0, rect["dx"] - 2 * pad)
    dy = max(0.0, rect["dy"] - 2 * pad)
    return {"x": rect["x"] + pad, "y": rect["y"] + pad, "dx": dx, "dy": dy}


def wrap_for_rect(text: str, dx: float, font_size: float) -> str:
    max_chars = max(8, int(dx * 2.1 / max(font_size / 9.0, 0.8)))
    return "\n".join(textwrap.wrap(text, width=max_chars, max_lines=3, placeholder="..."))


def draw_static(build: StaticBuild) -> tuple[int, tuple[int, int]]:
    fig, ax = plt.subplots(figsize=(FIG_W_IN, FIG_H_IN), dpi=FIG_DPI)
    fig.patch.set_facecolor("white")
    ax.set_xlim(0, LAYOUT_W)
    ax.set_ylim(0, LAYOUT_H)
    ax.invert_yaxis()
    ax.axis("off")

    language_sizes = squarify.normalize_sizes(
        build.languages["value"].tolist(),
        LAYOUT_W,
        LAYOUT_H,
    )
    language_rects = squarify.squarify(language_sizes, 0, 0, LAYOUT_W, LAYOUT_H)
    labeled_cells = 0

    for lang_row, lang_rect in zip(build.languages.itertuples(index=False), language_rects):
        lang_rect = padded_rect(lang_rect, 0.12)
        ax.add_patch(
            mpatches.Rectangle(
                (lang_rect["x"], lang_rect["y"]),
                lang_rect["dx"],
                lang_rect["dy"],
                facecolor=LANGUAGE_FILL,
                edgecolor=LANGUAGE_EDGE,
                linewidth=1.2,
            )
        )

        language_families = build.families.loc[
            build.families["language_code"] == lang_row.language_code
        ].sort_values("value", ascending=False)
        family_sizes = squarify.normalize_sizes(
            language_families["value"].tolist(),
            lang_rect["dx"],
            lang_rect["dy"],
        )
        family_rects = squarify.squarify(
            family_sizes,
            lang_rect["x"],
            lang_rect["y"],
            lang_rect["dx"],
            lang_rect["dy"],
        )

        for fam_row, fam_rect in zip(language_families.itertuples(index=False), family_rects):
            fam_rect = padded_rect(fam_rect, 0.08)
            ax.add_patch(
                mpatches.Rectangle(
                    (fam_rect["x"], fam_rect["y"]),
                    fam_rect["dx"],
                    fam_rect["dy"],
                    facecolor=family_color(fam_row.family),
                    edgecolor="white",
                    linewidth=0.8,
                    alpha=0.92,
                )
            )
            frac = fam_row.value / build.total_views
            if frac >= STATIC_LABEL_FRAC and fam_rect["dx"] > 5.5 and fam_rect["dy"] > 3.0:
                font_size = min(11.5, max(8.0, min(fam_rect["dx"], fam_rect["dy"]) * 0.9))
                label = f"{fam_row.family}\n{fmt_views(fam_row.value)}"
                ax.text(
                    fam_rect["x"] + fam_rect["dx"] / 2,
                    fam_rect["y"] + fam_rect["dy"] / 2,
                    wrap_for_rect(label, fam_rect["dx"], font_size),
                    ha="center",
                    va="center",
                    fontsize=font_size,
                    color="#111111",
                    linespacing=1.05,
                )
                labeled_cells += 1

        lang_frac = lang_row.value / build.total_views
        lang_font = min(13.5, max(8.5, math.sqrt(max(lang_frac, 0.001)) * 45))
        lang_text = f"{lang_row.language_label}\n{fmt_views(lang_row.value)}"
        ax.text(
            lang_rect["x"] + 0.45,
            lang_rect["y"] + 0.65,
            wrap_for_rect(lang_text, lang_rect["dx"], lang_font),
            ha="left",
            va="top",
            fontsize=lang_font,
            fontweight="bold",
            color="#111111",
            bbox={
                "boxstyle": "round,pad=0.2",
                "facecolor": "white",
                "edgecolor": "none",
                "alpha": 0.82,
            },
        )
        labeled_cells += 1

    legend_families = [
        "Entertainment",
        "Gaming",
        "Knowledge",
        "Lifestyle",
        "Music",
        "Society",
        "Sports",
        "Other / Unmapped YouTube topic",
        "Unlabeled",
        "Other (families)",
    ]
    handles = [
        mpatches.Patch(facecolor=family_color(family), edgecolor="none", label=family)
        for family in legend_families
    ]
    fig.legend(
        handles=handles,
        loc="lower center",
        ncol=5,
        frameon=False,
        fontsize=9,
        bbox_to_anchor=(0.5, 0.01),
    )
    fig.suptitle(
        "YouTube Topic Views by Language and Family",
        x=0.012,
        y=0.985,
        ha="left",
        fontsize=18,
        fontweight="bold",
    )
    fig.text(
        0.012,
        0.945,
        "Static master: top languages with pooled low-share families; area = allocated 4-week views.",
        ha="left",
        va="top",
        fontsize=10,
        color="#333333",
    )
    fig.text(
        0.012,
        0.025,
        "Source: YouTube TOO topic allocations joined to dev_sean.default.yt_channel_stats 4-week traffic delta.",
        ha="left",
        va="bottom",
        fontsize=8.5,
        color="#555555",
    )
    fig.subplots_adjust(left=0.01, right=0.99, top=0.925, bottom=0.09)
    fig.savefig(STATIC_PNG, dpi=FIG_DPI, facecolor="white")
    fig.savefig(STATIC_SVG, facecolor="white")
    plt.close(fig)

    dimensions = Image.open(STATIC_PNG).size
    return labeled_cells, dimensions


def safe_id_part(value: object) -> str:
    text = "" if value is None else str(value)
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text)[:180]


def add_node(
    ids: list[str],
    labels: list[str],
    parents: list[str],
    values: list[float],
    colors: list[str],
    customdata: list[list[object]],
    node_id: str,
    label: str,
    parent: str,
    value: float,
    color: str,
    data: list[object],
) -> None:
    ids.append(node_id)
    labels.append(label)
    parents.append(parent)
    values.append(float(value))
    colors.append(color)
    customdata.append(data)


def build_interactive(df: pd.DataFrame) -> int:
    plot_df = df.loc[df[VALUE_COL] > 0].copy()
    if plot_df.empty:
        raise RuntimeError("No positive 4-week allocated views available for interactive treemap")

    ids: list[str] = []
    labels: list[str] = []
    parents: list[str] = []
    values: list[float] = []
    colors: list[str] = []
    customdata: list[list[object]] = []

    total_views = float(plot_df[VALUE_COL].sum())
    add_node(
        ids,
        labels,
        parents,
        values,
        colors,
        customdata,
        ROOT_ID,
        "All languages",
        "",
        total_views,
        "#F8F9FA",
        ["root", "", "", "", "", fmt_views(total_views), "", "", ""],
    )

    lang_totals = (
        plot_df.groupby("language_code", observed=True)[VALUE_COL]
        .sum()
        .sort_values(ascending=False)
    )
    for lang_code, lang_value in lang_totals.items():
        lang_id = f"lang::{safe_id_part(lang_code)}"
        add_node(
            ids,
            labels,
            parents,
            values,
            colors,
            customdata,
            lang_id,
            language_label(str(lang_code)),
            ROOT_ID,
            float(lang_value),
            LANGUAGE_FILL,
            ["language", language_label(str(lang_code)), "", "", "", fmt_views(lang_value), "", "", ""],
        )

    family_totals = (
        plot_df.groupby(["language_code", "yt_family"], observed=True)[VALUE_COL]
        .sum()
        .reset_index()
        .sort_values(["language_code", VALUE_COL], ascending=[True, False])
    )
    for row in family_totals.itertuples(index=False):
        lang_id = f"lang::{safe_id_part(row.language_code)}"
        family_id = f"family::{safe_id_part(row.language_code)}::{safe_id_part(row.yt_family)}"
        add_node(
            ids,
            labels,
            parents,
            values,
            colors,
            customdata,
            family_id,
            str(row.yt_family),
            lang_id,
            float(getattr(row, VALUE_COL)),
            family_color(str(row.yt_family)),
            [
                "family",
                language_label(str(row.language_code)),
                str(row.yt_family),
                "",
                "",
                fmt_views(float(getattr(row, VALUE_COL))),
                "",
                "",
                "",
            ],
        )

    leaf_totals = (
        plot_df.groupby(["language_code", "yt_family", "yt_leaf"], observed=True)[VALUE_COL]
        .sum()
        .reset_index()
        .sort_values(["language_code", "yt_family", VALUE_COL], ascending=[True, True, False])
    )
    for row in leaf_totals.itertuples(index=False):
        family_id = f"family::{safe_id_part(row.language_code)}::{safe_id_part(row.yt_family)}"
        leaf_id = (
            f"leaf::{safe_id_part(row.language_code)}::"
            f"{safe_id_part(row.yt_family)}::{safe_id_part(row.yt_leaf)}"
        )
        add_node(
            ids,
            labels,
            parents,
            values,
            colors,
            customdata,
            leaf_id,
            str(row.yt_leaf),
            family_id,
            float(getattr(row, VALUE_COL)),
            family_color(str(row.yt_family)),
            [
                "leaf",
                language_label(str(row.language_code)),
                str(row.yt_family),
                str(row.yt_leaf),
                "",
                fmt_views(float(getattr(row, VALUE_COL))),
                "",
                "",
                "",
            ],
        )

    channel_groups = (
        plot_df.groupby(["language_code", "yt_family", "yt_leaf", "channel_id"], observed=True)
        .agg(
            channel_title=("channel_title", "first"),
            allocated_views=(VALUE_COL, "sum"),
            channel_views=(CHANNEL_VIEW_COL, "first"),
            current_lifetime_views=("current_lifetime_views", "first"),
            allocation_weight=("allocation_weight", "sum"),
            raw_topic_categories=("raw_topic_categories", "first"),
        )
        .reset_index()
        .sort_values(
            ["language_code", "yt_family", "yt_leaf", "allocated_views"],
            ascending=[True, True, True, False],
        )
    )

    channel_node_count = 0
    for leaf_key, leaf_df in channel_groups.groupby(
        ["language_code", "yt_family", "yt_leaf"],
        observed=True,
        sort=False,
    ):
        lang_code, family, leaf = leaf_key
        leaf_id = f"leaf::{safe_id_part(lang_code)}::{safe_id_part(family)}::{safe_id_part(leaf)}"
        top = leaf_df.head(TOP_CHANNELS_PER_LEAF)
        rest = leaf_df.iloc[TOP_CHANNELS_PER_LEAF:]

        for row in top.itertuples(index=False):
            channel_id = (
                f"channel::{safe_id_part(lang_code)}::{safe_id_part(family)}::"
                f"{safe_id_part(leaf)}::{safe_id_part(row.channel_id)}"
            )
            raw_slugs = raw_topic_slugs(row.raw_topic_categories)
            add_node(
                ids,
                labels,
                parents,
                values,
                colors,
                customdata,
                channel_id,
                str(row.channel_title),
                leaf_id,
                float(row.allocated_views),
                family_color(str(family)),
                [
                    "channel",
                    language_label(str(lang_code)),
                    str(family),
                    str(leaf),
                    str(row.channel_title),
                    fmt_views(float(row.allocated_views)),
                    fmt_views(float(row.channel_views)),
                    f"{float(row.allocation_weight):.4f}",
                    raw_slugs,
                ],
            )
            channel_node_count += 1

        if len(rest) > 0:
            n_channels = int(rest["channel_id"].nunique())
            other_value = float(rest["allocated_views"].sum())
            other_id = (
                f"channel_other::{safe_id_part(lang_code)}::{safe_id_part(family)}::"
                f"{safe_id_part(leaf)}"
            )
            add_node(
                ids,
                labels,
                parents,
                values,
                colors,
                customdata,
                other_id,
                f"Other ({n_channels} channels)",
                leaf_id,
                other_value,
                family_color(str(family)),
                [
                    "pooled channels",
                    language_label(str(lang_code)),
                    str(family),
                    str(leaf),
                    f"Other ({n_channels} channels)",
                    fmt_views(other_value),
                    "",
                    "",
                    "",
                ],
            )
            channel_node_count += 1

    hovertemplate = (
        "<b>%{label}</b><br>"
        "Type: %{customdata[0]}<br>"
        "Language: %{customdata[1]}<br>"
        "Family: %{customdata[2]}<br>"
        "Leaf: %{customdata[3]}<br>"
        "Channel: %{customdata[4]}<br>"
        "Allocated 4-week views: %{customdata[5]}<br>"
        "Raw channel 4-week views: %{customdata[6]}<br>"
        "Allocation weight: %{customdata[7]}<br>"
        "Raw topic slugs: %{customdata[8]}"
        "<extra></extra>"
    )

    fig = go.Figure(
        go.Treemap(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            maxdepth=2,
            sort=True,
            tiling={"packing": "squarify", "pad": 2, "squarifyratio": 1},
            marker={"colors": colors, "line": {"width": 0.8, "color": "white"}},
            customdata=customdata,
            hovertemplate=hovertemplate,
            textinfo="label",
        )
    )
    fig.update_layout(
        title="YouTube Topic Treemap Explorer: language -> family -> leaf -> channel",
        width=1400,
        height=900,
        margin={"l": 10, "r": 10, "t": 55, "b": 10},
        uniformtext={"minsize": 10, "mode": "hide"},
    )
    fig.write_html(INTERACTIVE_HTML, include_plotlyjs=True, full_html=True)
    return channel_node_count


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = load_allocations()
    traffic_summary = df.attrs.get("traffic_summary", {})
    valid_channels = int(df.loc[df[CHANNEL_VIEW_COL].notna(), "channel_id"].nunique())
    positive_channels = int(df.loc[df[VALUE_COL] > 0, "channel_id"].nunique())
    positive_rows = int((df[VALUE_COL] > 0).sum())

    print(f"ALLOCATION METHOD: {METHOD}")
    print(f"ROWS USED: {len(df):,}")
    print(f"TRAFFIC SOURCE TABLE: {TRAFFIC_SOURCE_TABLE}")
    print(f"TOO UNIVERSE TABLE: {TOO_CHANNEL_TABLE}")
    print(f"TRAFFIC CURRENT SNAPSHOT: {traffic_summary.get('current_snapshot')}")
    print(f"TRAFFIC PRIOR SNAPSHOT: {traffic_summary.get('prior_snapshot')}")
    print(f"TRAFFIC EXTRACT ROWS: {traffic_summary.get('traffic_rows'):,}")
    print(f"TRAFFIC UNIQUE CHANNELS: {traffic_summary.get('traffic_unique_channels'):,}")
    print(f"TRAFFIC DUPLICATE ROWS DEDUPED: {traffic_summary.get('traffic_duplicate_rows'):,}")
    print(f"TRAFFIC CHANNELS WITH CURRENT: {traffic_summary.get('channels_with_current'):,}")
    print(f"TRAFFIC CHANNELS WITH PRIOR: {traffic_summary.get('channels_with_prior'):,}")
    print(f"TRAFFIC NEGATIVE RAW DELTAS: {traffic_summary.get('negative_raw_deltas'):,}")
    print(f"TRAFFIC CHANNELS WITH VALID 4WK: {traffic_summary.get('channels_with_valid_4wk'):,}")
    print(f"VALID TRAFFIC CHANNELS IN ALLOCATIONS: {valid_channels:,}")
    print(f"POSITIVE TRAFFIC CHANNELS PLOTTED: {positive_channels:,}")
    print(f"POSITIVE ALLOCATION ROWS PLOTTED: {positive_rows:,}")
    print(f"TOTAL 4WK VIEWS: {traffic_summary.get('total_4wk_views'):,.0f}")
    print("PACKING: squarify")
    print("STATIC METHOD: squarify library + matplotlib")
    print(
        "INTERACTIVE METHOD: plotly.graph_objects.go.Treemap "
        "branchvalues=total maxdepth=2 tiling.packing=squarify"
    )
    assert_conservation(df)

    static = build_static_data(df)
    labeled_cells, dimensions = draw_static(static)
    channel_nodes = build_interactive(df)

    print(f"STATIC MASTER PNG: {STATIC_PNG.resolve()}")
    print(f"STATIC MASTER SVG: {STATIC_SVG.resolve()}")
    print(f"STATIC CELLS: {static.static_cells}")
    print(f"MIN CELL AREA: {static.min_cell_area_pct:.3f}%")
    print(f"POOLED VIEW SHARE: {static.pooled_view_share_pct:.3f}%")
    print(f"LABELED CELLS: {labeled_cells}")
    print(f"STATIC PRUNING: top_k_languages={static.top_k}; family_pool_threshold={static.family_pool_frac:.3f}")
    print(f"FIGURE DIMENSIONS: {dimensions[0]}x{dimensions[1]} px, {FIG_W_IN}x{FIG_H_IN} in, {FIG_DPI} DPI")
    print(f"INTERACTIVE HTML: {INTERACTIVE_HTML.resolve()}")
    print(f"INTERACTIVE CHANNEL LEAF CAP: top {TOP_CHANNELS_PER_LEAF} + Other (N channels)")
    print(f"INTERACTIVE CHANNEL/OTHER NODES: {channel_nodes:,}")


if __name__ == "__main__":
    main()
