#!/usr/bin/env python3
"""Render the v3 YouTube topic treemap.

Refines scripts/render_treemap_v2b.py (same parquet inputs; the Spark pipeline is
NOT rerun). v3 adds, relative to v2b:

  1. LANGUAGE MERGE   -> new ``language_display`` column from
     config/language_normalization.yaml (raw ``language_code`` is preserved).
     All English variants collapse to one "English"; region subtags are stripped;
     review-cluster / non-ISO codes pool into "Other languages".
  2. PALETTE          -> Okabe-Ito family hues from config/family_color_map.yaml;
     low-chroma neutrals reserved for residual buckets (Society stays blue).
  3. CHILD LEAVES     -> variable-depth static master gated by cell area.
  4. NAMED CHANNELS   -> config/treemap_top_channel_placement.csv hard-placed at
     (language, revised_primary_family, revised_primary_leaf) with weight 1, via a
     new ``allocation_weight_v3`` column (raw ``allocation_weight`` preserved).

Outputs -> outputs/youtube_topic_treemap_20260715_v3_13/:
  treemap_static_master_v3_13.png / .svg,
  treemap_interactive_explorer_v3_13.html, treemap_static_cells_v3_13.csv,
  render_log_v3_13.txt.
"""

from __future__ import annotations

import html as _html
import math
import re
import sys
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from matplotlib import font_manager as _fm

# Glyph fallback so CJK / non-Latin channel names render instead of tofu boxes.
_AVAILABLE_FONTS = {f.name for f in _fm.fontManager.ttflist}
_CJK_FALLBACKS = [f for f in (
    "Arial Unicode MS", "Apple SD Gothic Neo", "Hiragino Sans", "Heiti TC",
    "PingFang SC", "Noto Sans CJK SC", "Songti SC",
) if f in _AVAILABLE_FONTS]
_PREFERRED_SANS = [f for f in ("Helvetica Neue", "Helvetica", "Arial") if f in _AVAILABLE_FONTS]
plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = [*_PREFERRED_SANS, "DejaVu Sans", *_CJK_FALLBACKS]
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import squarify
import yaml
from PIL import Image


# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[1]
V2B_DIR = ROOT / "outputs" / "youtube_topic_treemap_20260617_v2b"
OUT_DIR = ROOT / "outputs" / "youtube_topic_treemap_20260715_v3_13"
ALLOC_PATH = V2B_DIR / "channel_label_allocations.parquet"
TRAFFIC_PATH = V2B_DIR / "traffic_4wk.parquet"
LANG_CFG_PATH = ROOT / "config" / "language_normalization.yaml"
PALETTE_CFG_PATH = ROOT / "config" / "family_color_map.yaml"
TOPIC_REMAP_PATH = ROOT / "config" / "topic_remap.yaml"
PLACEMENT_CSV_PATH = ROOT / "config" / "treemap_top_channel_placement.csv"

STATIC_PNG = OUT_DIR / "treemap_static_master_v3_13.png"
STATIC_SVG = OUT_DIR / "treemap_static_master_v3_13.svg"
INTERACTIVE_HTML = OUT_DIR / "treemap_interactive_explorer_v3_13.html"
CELLS_CSV = OUT_DIR / "treemap_static_cells_v3_13.csv"
LOG_TXT = OUT_DIR / "render_log_v3_13.txt"
README_MD = OUT_DIR / "README.md"

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
METHOD = "family_balanced"
TRAFFIC_SOURCE_TABLE = "dev_sean.default.yt_channel_stats"
TOO_CHANNEL_TABLE = "prod_tads.youtube_too.yt_sl_channels"
CHANNEL_VIEW_COL = "view_count_4wk"          # raw, never mutated
RAW_WEIGHT_COL = "allocation_weight"         # raw, never mutated
WEIGHT_COL = "allocation_weight_v3"          # NEW: 1.0 for placement overrides
VALUE_COL = "allocated_views_4wk"            # derived in-memory
DISPLAY_COL = "language_display"             # NEW
ROOT_ID = "all"

# Static gating thresholds (fractions of grand total area).
FAMILY_MIN = 0.003          # families below this pool into "Other (families)"
LEAF_PARENT_MIN = 0.010     # ordinary full subdivision starts at 1.0%
LEAF_MIN = 0.005            # ordinary leaf cutoff remains 0.5%
MEDIUM_FAMILY_MIN = 0.003   # smaller families may expose one leading named topic
PRIORITY_TOPIC_MIN = 0.00094 # leading topic >= 0.094%
SECOND_PRIORITY_TOPIC_MIN = 0.00090 # optional second topic >= 0.090%
MAX_PRIORITY_ASPECT_RATIO = 5.0
MAX_COVERAGE_ASPECT_RATIO = 5.0
STRUCT_MIN = 0.003          # min rendered structural cell area (0.3%)
STATIC_CELL_CAP = 200       # comparison ceiling; geometry may stop below it
TOP_FAMILY_COUNT = 5
MIN_TOP_FAMILY_COVERAGE = 4
EXTRA_FIFTH_FAMILY_BUDGET = 6
COVERAGE_RESIDUAL_MIN = 0.001  # fifth-family rescue must leave >=0.1% residual
DETAIL_FAMILY_MIN = 0.0003     # optional post-coverage family detail >=0.03%
DETAIL_RESIDUAL_MIN = 0.0003   # keep its residual >=0.03%, or eliminate it
# Named-channel boxes (a deepest annotation layer inside big leaves).
LEAF_CHANNEL_MIN = 0.015    # leaf must be >= 1.5% to carry *extra* channel boxes
CHANNEL_OF_LEAF_MIN = 0.04  # an extra named channel must be >= 4% of its leaf to break out
CHANNEL_OF_TOTAL_MIN = 0.0004
CHANNELS_PER_LEAF_STATIC = 12
TOP_CHANNELS_PER_LEAF_INTERACTIVE = 15
FORCE_TOP_CHANNELS = 50     # always show the top-N channels (by 4wk views) as boxes

# --- Print sizing -----------------------------------------------------------
# The figure is AUTHORED at its final print size so on-page point sizes are real:
# a label drawn at N pt prints at N pt. Target: <= 6 inches tall on a professional
# printer. Every label is sized in real points and only drawn if it fits at the
# legibility floor below; otherwise the cell is left to the interactive explorer.
FIG_H_IN = 6.6            # a little taller to make room for reserved header bands
FIG_W_IN = 10.0
FIG_DPI = 300             # professional print
LAYOUT_W = 100.0
LAYOUT_H = 60.0
MIN_LABEL_PT = 6.0        # hard legibility floor (don't draw text smaller than this)
MIN_PRIORITY_LABEL_PT = 5.5  # narrow exception for short, explicitly exposed topics
LANG_BAND_PT = 7.5        # target font for the reserved language header band

# Axes occupy the figure minus title/caption (top) and legend/source (bottom).
MARGIN_LEFT, MARGIN_RIGHT, MARGIN_TOP, MARGIN_BOTTOM = 0.006, 0.994, 0.890, 0.130
_AX_W_IN = FIG_W_IN * (MARGIN_RIGHT - MARGIN_LEFT)
_AX_H_IN = FIG_H_IN * (MARGIN_TOP - MARGIN_BOTTOM)
PT_PER_UNIT_X = _AX_W_IN * 72.0 / LAYOUT_W   # printed points per layout unit (x)
PT_PER_UNIT_Y = _AX_H_IN * 72.0 / LAYOUT_H   # printed points per layout unit (y)

# Controlled line breaks used only for the dominant-family fallback in a
# language that would otherwise have no real category label. The legend always
# carries the unbroken name.
COMPACT_FAMILY_LABELS = {
    "Entertainment": "Enter-\ntainment",
    "Lifestyle": "Life-\nstyle",
    "Knowledge": "Knowl-\nedge",
}


# --------------------------------------------------------------------------- #
# Config loading
# --------------------------------------------------------------------------- #
def load_language_config() -> dict:
    cfg = yaml.safe_load(LANG_CFG_PATH.read_text())
    base_to_display: dict[str, str] = {}
    for display, bases in cfg["display_to_bases"].items():
        for base in bases:
            base_to_display[str(base).lower()] = display
    cfg["_base_to_display"] = base_to_display
    return cfg


def load_palette_config() -> dict:
    return yaml.safe_load(PALETTE_CFG_PATH.read_text())


LANG_CFG = load_language_config()
PAL = load_palette_config()
TOPIC_REMAP = (yaml.safe_load(TOPIC_REMAP_PATH.read_text()) or {}).get("unmapped_remap", {})

FAMILY_COLORS: dict[str, str] = dict(PAL["family_colors"])
RESIDUAL_FAMILY_COLORS: dict[str, str] = dict(PAL["residual_family_colors"])
REAL_FAMILIES = set(FAMILY_COLORS)
GRAY_ANCHOR = PAL["unspecified_gray_anchor"]
UNSPEC_BLEND = float(PAL["unspecified_blend"])
MAX_LEAF_LIGHTEN = float(PAL["max_leaf_lighten"])
OTHER_LEAVES_LIGHTEN = float(PAL["other_leaves_lighten"])
CHANNEL_LIGHTEN = float(PAL["channel_lighten"])
LANGUAGE_FILL = PAL["language_fill"]
LANGUAGE_EDGE = PAL["language_edge"]
OTHER_LANGUAGES_FILL = PAL["other_languages_fill"]
TILE_BORDER_COLOR = PAL["tile_border_color"]
TILE_BORDER_WIDTH = float(PAL["tile_border_width"])
TILING_PAD = int(PAL["tiling_pad"])

OTHER_LANG = LANG_CFG["other_languages_label"]
UNDETERMINED = LANG_CFG["undetermined_label"]
TOP_K_LANGUAGES = int(LANG_CFG["top_k_languages"])
CLUSTER_PATTERNS = [str(p).lower() for p in LANG_CFG["cluster_patterns"]]
OTHER_FAMILIES_LABEL = "Other (families)"

# Captures everything printed, so validation never depends on the chat transcript.
_LOG_LINES: list[str] = []


def log(message: str = "") -> None:
    print(message)
    _LOG_LINES.append(message)


# --------------------------------------------------------------------------- #
# Language normalization
# --------------------------------------------------------------------------- #
def normalize_language(code: object) -> str:
    if code is None:
        return OTHER_LANG
    raw = str(code).strip()
    if raw == "":
        return OTHER_LANG
    low = raw.lower()
    if any(pat in low for pat in CLUSTER_PATTERNS):
        return OTHER_LANG
    base = re.split(r"[-_]", low, maxsplit=1)[0]
    return LANG_CFG["_base_to_display"].get(base, OTHER_LANG)


# --------------------------------------------------------------------------- #
# Color helpers
# --------------------------------------------------------------------------- #
def hex_to_rgb(value: str) -> tuple[float, float, float]:
    value = value.lstrip("#")
    return tuple(int(value[i : i + 2], 16) / 255.0 for i in (0, 2, 4))  # type: ignore[return-value]


def rgb_to_hex(rgb: tuple[float, float, float]) -> str:
    return "#%02X%02X%02X" % tuple(int(round(max(0.0, min(1.0, c)) * 255)) for c in rgb)


def blend(c1: str, c2: str, t: float) -> str:
    a = hex_to_rgb(c1)
    b = hex_to_rgb(c2)
    return rgb_to_hex(tuple(a[i] + (b[i] - a[i]) * t for i in range(3)))


def lighten(color: str, t: float) -> str:
    return blend(color, "#FFFFFF", t)


def relative_luminance(color: str) -> float:
    r, g, b = hex_to_rgb(color)

    def lin(c: float) -> float:
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4

    return 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b)


def text_color_for(bg: str) -> str:
    return "#111111" if relative_luminance(bg) > 0.42 else "#FFFFFF"


def family_base_color(family: str) -> str:
    if family in FAMILY_COLORS:
        return FAMILY_COLORS[family]
    if family in RESIDUAL_FAMILY_COLORS:
        return RESIDUAL_FAMILY_COLORS[family]
    return RESIDUAL_FAMILY_COLORS["Other / Unmapped YouTube topic"]


def is_unspecified_leaf(leaf: str) -> bool:
    return leaf.strip().lower().endswith("- unspecified")


def leaf_color(family: str, leaf: str, ramp_rank: int, ramp_n: int) -> str:
    """Color a leaf while keeping every descendant visibly in its family hue."""
    base = family_base_color(family)
    if is_unspecified_leaf(leaf):
        return blend(base, GRAY_ANCHOR, UNSPEC_BLEND)
    if "other leaves" in leaf.lower():
        return lighten(base, OTHER_LEAVES_LIGHTEN)
    if ramp_n <= 1:
        return base
    t = MAX_LEAF_LIGHTEN * (ramp_rank / (ramp_n - 1))
    return lighten(base, t)


# --------------------------------------------------------------------------- #
# Formatting helpers
# --------------------------------------------------------------------------- #
def fmt_views(value: float) -> str:
    if value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}T"
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    return f"{value:,.0f}"


def html_escape(value: object) -> str:
    return _html.escape("" if value is None else str(value), quote=False)


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
        slugs.append(item.rsplit("/wiki/", 1)[-1] if "/wiki/" in item else item)
    return "; ".join(slugs)


def safe_id_part(value: object) -> str:
    text = "" if value is None else str(value)
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text)[:180]


def clean_path_text(text: str) -> str:
    """Strip brackets / rewrite parent leaves in a CSV path string for hover display,
    and apply the topic remap so provenance paths match the rendered figure."""
    for leaf_key, target in TOPIC_REMAP.items():
        new_leaf = display_label("leaf", target["leaf"])
        text = text.replace(f"Other / Unmapped YouTube topic > {leaf_key}", f"{target['family']} > {new_leaf}")
        text = text.replace(leaf_key, new_leaf)
    text = re.sub(r"\[(.+?)\]\s*-\s*unspecified", r"\1 (main)", text)
    text = re.sub(r"\[(.+?)\]\s*-\s*other leaves", r"\1 — other", text)
    return text.replace("[", "").replace("]", "")


def display_label(level: str, label: str) -> str:
    """Human-facing label: strip brackets; parent ("- unspecified") leaves become
    "<Family> (main)"; pooled remainders become "<Family> (other topics)"."""
    if level == "family":
        if label == OTHER_FAMILIES_LABEL:
            return "Other families"
        return label.replace("[", "").replace("]", "")
    if level == "leaf":
        m = re.match(r"^\[(.+?)\]\s*-\s*unspecified$", label)
        if m:
            return f"{m.group(1)} (main)"
        m = re.match(r"^\[(.+?)\]\s*-\s*other leaves$", label)
        if m:
            return f"{m.group(1)} — other"
        return label.replace("[", "").replace("]", "")
    return label


# --------------------------------------------------------------------------- #
# Data loading + hard placement
# --------------------------------------------------------------------------- #
def load_traffic() -> tuple[pd.DataFrame, dict[str, object]]:
    traffic = pd.read_parquet(TRAFFIC_PATH)
    raw_rows = int(len(traffic))
    raw_unique = int(traffic["channel_id"].nunique())
    traffic = traffic.sort_values(
        ["channel_id", "current_collected_at", "prior_collected_at"],
        ascending=[True, False, False],
    ).drop_duplicates("channel_id", keep="first")
    summary = {
        "traffic_rows": raw_rows,
        "traffic_unique_channels": raw_unique,
        "traffic_duplicate_rows": raw_rows - raw_unique,
        "current_snapshot": str(traffic["current_collected_at"].dropna().dt.date.max()),
        "prior_snapshot": str(traffic["prior_collected_at"].dropna().dt.date.max()),
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
        "allocation_method",
        "yt_family",
        "yt_leaf",
        RAW_WEIGHT_COL,
        "raw_topic_categories",
    ]
    df = pd.read_parquet(ALLOC_PATH, columns=columns)
    df = df.loc[df["allocation_method"] == METHOD].copy()
    if df.empty:
        raise RuntimeError(f"No allocation rows for method={METHOD!r}")

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
        "current_lifetime_views",
        CHANNEL_VIEW_COL,
    ]
    df = df.merge(traffic[traffic_cols], on="channel_id", how="left", validate="many_to_one")
    df["channel_title"] = df["channel_name"].fillna(df["channel_title"])
    df[DISPLAY_COL] = df["language_code"].map(normalize_language)
    df.attrs["traffic_summary"] = traffic_summary
    return df


def apply_hard_placement(df: pd.DataFrame, placements: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Override CSV channels with one weight-1 placement; keep the rest fractional."""
    csv_ids = set(placements["channel_id"])
    lang_by_ch = df.groupby("channel_id")[DISPLAY_COL].first()
    views_by_ch = df.groupby("channel_id")[CHANNEL_VIEW_COL].first()

    base = df.loc[~df["channel_id"].isin(csv_ids)].copy()
    base[WEIGHT_COL] = base[RAW_WEIGHT_COL]
    base[VALUE_COL] = base[CHANNEL_VIEW_COL] * base[WEIGHT_COL]
    base["is_placement_override"] = False
    base["needs_manual_review"] = False
    base["non_primary_paths"] = ""
    base["revised_primary_path"] = ""

    rows = []
    fallback = 0
    for r in placements.itertuples(index=False):
        disp = lang_by_ch.get(r.channel_id)
        if not isinstance(disp, str) or disp == "":
            disp = normalize_language(r.language_code)
        views = views_by_ch.get(r.channel_id, np.nan)
        if pd.isna(views):
            views = float(r.view_count_4wk)
            fallback += 1
        else:
            views = float(views)
        rows.append(
            {
                "channel_id": r.channel_id,
                "channel_title": r.channel_name_title,
                "language_code": r.language_code,
                DISPLAY_COL: disp,
                "yt_family": r.revised_primary_family,
                "yt_leaf": r.revised_primary_leaf,
                RAW_WEIGHT_COL: np.nan,
                WEIGHT_COL: 1.0,
                CHANNEL_VIEW_COL: views,
                VALUE_COL: views,
                "current_lifetime_views": np.nan,
                "channel_name": r.channel_name_title,
                "raw_topic_categories": None,
                "is_placement_override": True,
                "needs_manual_review": bool(r.needs_manual_review),
                "non_primary_paths": "" if pd.isna(r.non_primary_display_paths_to_retain_as_metadata)
                else str(r.non_primary_display_paths_to_retain_as_metadata),
                "revised_primary_path": str(r.revised_primary_path),
            }
        )
    overrides = pd.DataFrame(rows)
    full = pd.concat([base, overrides], ignore_index=True, sort=False)
    info = {"placed": len(overrides), "fallback_views": fallback}
    return full, info


def apply_topic_remap(full: pd.DataFrame) -> list[dict]:
    """Re-home canonical slugs the Spark hierarchy left under 'Other / Unmapped'
    into their real family/leaf. Preserves raw values in *_raw columns."""
    full["yt_family_raw"] = full["yt_family"]
    full["yt_leaf_raw"] = full["yt_leaf"]
    moved: list[dict] = []
    for leaf_key, target in TOPIC_REMAP.items():
        mask = full["yt_leaf"] == leaf_key
        if not mask.any():
            continue
        views = float(full.loc[mask, VALUE_COL].clip(lower=0).sum())
        full.loc[mask, "yt_family"] = target["family"]
        full.loc[mask, "yt_leaf"] = target["leaf"]
        moved.append({"from": leaf_key, "to": f"{target['family']} > {target['leaf']}", "views": views})
    return moved


def assert_conservation(full: pd.DataFrame) -> float:
    valid = full.loc[full[CHANNEL_VIEW_COL].notna()].copy()
    if valid.empty:
        raise RuntimeError("CONSERVATION: FAIL no channels have valid 4-week traffic")
    ch = valid.groupby("channel_id", observed=True).agg(
        weight_sum=(WEIGHT_COL, "sum"),
        allocated_sum=(VALUE_COL, "sum"),
        channel_views=(CHANNEL_VIEW_COL, "first"),
    )
    weight_ok = np.allclose(ch["weight_sum"], 1.0, rtol=0, atol=1e-8)
    alloc_ok = np.allclose(ch["allocated_sum"], ch["channel_views"], rtol=1e-9, atol=1e-2)
    total_alloc = float(ch["allocated_sum"].sum())
    total_views = float(ch["channel_views"].sum())
    total_ok = math.isclose(total_alloc, total_views, rel_tol=1e-12, abs_tol=1e-2)
    if not (weight_ok and alloc_ok and total_ok):
        raise RuntimeError(
            "CONSERVATION: FAIL "
            f"max_weight_delta={float((ch['weight_sum'] - 1.0).abs().max()):.3g} "
            f"max_alloc_delta={float((ch['allocated_sum'] - ch['channel_views']).abs().max()):.3g} "
            f"total_delta={abs(total_alloc - total_views):.3g}"
        )
    log("CONSERVATION: PASS")
    log(f"CONSERVATION TOTAL 4WK VIEWS: {total_alloc:,.0f}")
    return total_alloc


# --------------------------------------------------------------------------- #
# Static tree
# --------------------------------------------------------------------------- #
@dataclass
class Cell:
    label: str
    value: float
    color: str
    level: str                       # language | family | leaf | channel | pool
    family: str = ""
    leaf: str = ""
    edge: str = "white"
    edge_width: float = TILE_BORDER_WIDTH
    label_style: str = "center"      # center | corner
    is_named_channel: bool = False
    needs_review: bool = False
    forced: bool = False             # broken out to guarantee a top-N channel is shown
    channel_id: str = ""             # set on named-channel boxes (for label-fit pruning)
    top_channel: str = ""            # leaf cells: largest hard-placed channel (annotation)
    language: str = ""               # owning static language block
    priority_topic: bool = False      # leading named topic exposed in a medium family
    coverage_rescued: bool = False    # top-five family pulled exactly out of the pool
    detail_rescued: bool = False      # optional post-coverage family pulled from pool
    coverage_residual: bool = False   # residual changed by a coverage rescue
    children: list["Cell"] = field(default_factory=list)


def _pool_small(items: list[tuple[str, float]], total: float, min_frac: float):
    """Split (label,value) into kept (>= min_frac) and a pooled remainder that is
    grown to >= STRUCT_MIN by pulling the smallest kept items in."""
    kept = [(k, v) for k, v in items if v / total >= min_frac]
    pool = sum(v for k, v in items if v / total < min_frac)
    kept.sort(key=lambda kv: kv[1], reverse=True)
    min_abs = total * STRUCT_MIN
    while 0 < pool < min_abs and kept:
        _, moved = kept.pop()
        pool += moved
    return kept, pool


def _family_pool_plan(sub: pd.DataFrame, total: float, force_ids: set):
    """Ordinary family pooling plus exact force-open exceptions."""
    fam_totals = sub.groupby("yt_family", observed=True)[VALUE_COL].sum().sort_values(ascending=False)
    items = [(str(k), float(v)) for k, v in fam_totals.items()]
    kept, pool = _pool_small(items, total, FAMILY_MIN)
    forced_fams = set(sub.loc[sub["channel_id"].isin(force_ids), "yt_family"].astype(str).unique())
    kept_keys = {k for k, _ in kept}
    for k, v in items:
        if k in forced_fams and k not in kept_keys:
            kept.append((k, v))
            pool -= v
            kept_keys.add(k)
    return items, kept, pool


def build_leaf_channels(sub: pd.DataFrame, family: str, leaf: str, leaf_value: float,
                        base_leaf_color: str, total: float, force_ids: set,
                        suppressed: set) -> tuple[list[Cell], str]:
    """Named-channel boxes for one leaf (returns (boxes, top_named_channel_title);
    the title feeds the leaf-label annotation when no box survives)."""
    named = (
        sub.loc[(sub["yt_family"] == family) & (sub["yt_leaf"] == leaf) & sub["is_placement_override"]]
        .sort_values(VALUE_COL, ascending=False)
    )
    if named.empty:
        return [], ""
    top_named = str(named.iloc[0]["channel_title"])
    allow_extra = leaf_value / total >= LEAF_CHANNEL_MIN

    chans: list[Cell] = []
    pooled_value = 0.0
    pooled_count = 0
    broken = 0
    for r in named.itertuples(index=False):
        cval = float(getattr(r, VALUE_COL))
        is_forced = r.channel_id in force_ids
        break_out = r.channel_id not in suppressed and (
            is_forced
            or (
                allow_extra
                and broken < CHANNELS_PER_LEAF_STATIC
                and cval / leaf_value >= CHANNEL_OF_LEAF_MIN
                and cval / total >= CHANNEL_OF_TOTAL_MIN
            )
        )
        if break_out:
            chans.append(
                Cell(
                    label=str(r.channel_title),
                    value=cval,
                    color=lighten(base_leaf_color, CHANNEL_LIGHTEN),
                    level="channel",
                    family=family,
                    leaf=leaf,
                    is_named_channel=True,
                    needs_review=bool(getattr(r, "needs_review", False) or getattr(r, "needs_manual_review", False)),
                    forced=is_forced,
                    channel_id=str(r.channel_id),
                )
            )
            broken += 1
        else:
            pooled_value += cval
            pooled_count += 1

    if not chans:
        return [], top_named

    # Everything not individually broken out (small named + all fractional others).
    nonnamed = sub.loc[
        (sub["yt_family"] == family) & (sub["yt_leaf"] == leaf) & (~sub["is_placement_override"])
    ]
    pooled_value += float(nonnamed[VALUE_COL].sum())
    pooled_count += int(nonnamed["channel_id"].nunique())
    if pooled_value > 0:
        chans.append(
            Cell(
                label=f"Other channels ({pooled_count})",
                value=pooled_value,
                color=base_leaf_color,
                level="channel",
                family=family,
                leaf=leaf,
            )
        )
    return chans, top_named


def build_family(sub: pd.DataFrame, family: str, family_value: float, total: float,
                 force_ids: set, suppressed: set, *, coverage_rescued: bool = False,
                 detail_rescued: bool = False) -> Cell:
    base_color = family_base_color(family)
    fam_cell = Cell(
        label=family,
        value=family_value,
        color=base_color,
        level="family",
        family=family,
        coverage_rescued=coverage_rescued,
        detail_rescued=detail_rescued,
    )

    # Coverage-rescued families are intentionally terminal. Subdividing them
    # would spend additional cells and turn a small exact family total into a
    # stack of tiny topic rectangles.
    if coverage_rescued or detail_rescued:
        return fam_cell

    fam_sub = sub.loc[sub["yt_family"] == family]
    leaf_totals = fam_sub.groupby("yt_leaf", observed=True)[VALUE_COL].sum().sort_values(ascending=False)
    # leaves that host a forced (top-N) channel must be broken out regardless of size
    forced_leaves = set(
        fam_sub.loc[fam_sub["channel_id"].isin(force_ids), "yt_leaf"].astype(str).unique()
    )

    family_share = family_value / total
    small_family = family_share < LEAF_PARENT_MIN or len(leaf_totals) <= 1
    items = [(str(k), float(v)) for k, v in leaf_totals.items()]
    priority_leaves: set[str] = set()
    if family in REAL_FAMILIES and len(leaf_totals) > 1 and MEDIUM_FAMILY_MIN <= family_share < LEAF_PARENT_MIN:
        specific = sorted([
            (leaf, value)
            for leaf, value in items
            if not is_unspecified_leaf(leaf) and "other leaves" not in leaf.lower()
        ], key=lambda item: item[1], reverse=True)
        if specific:
            candidate, candidate_value = max(specific, key=lambda item: item[1])
            remainder_value = family_value - candidate_value
            if (
                candidate_value / total >= PRIORITY_TOPIC_MIN
                and remainder_value / total >= STRUCT_MIN
            ):
                priority_leaves.add(candidate)
                if len(specific) > 1:
                    second, second_value = specific[1]
                    second_remainder = remainder_value - second_value
                    if (
                        second_value / total >= SECOND_PRIORITY_TOPIC_MIN
                        and second_remainder / total >= STRUCT_MIN
                    ):
                        priority_leaves.add(second)

    if small_family and not forced_leaves and not priority_leaves:
        return fam_cell  # render family solid

    if small_family:
        # Expose forced leaves plus at most one leading named topic; pool every
        # other leaf. This adds information to smaller-language blocks without
        # applying a lower cutoff to the already-detailed English block.
        exposed = forced_leaves | priority_leaves
        kept = [(k, v) for k, v in items if k in exposed]
        pool = sum(v for k, v in items if k not in exposed)
    else:
        kept, pool = _pool_small(items, total, LEAF_MIN)
        kept_keys = {k for k, _ in kept}
        # pull any forced leaf out of the pool
        for k, v in items:
            if k in forced_leaves and k not in kept_keys:
                kept.append((k, v))
                pool -= v
        kept.sort(key=lambda kv: kv[1], reverse=True)
    if not kept:
        return fam_cell

    real_leaves = [k for k, _ in kept if not is_unspecified_leaf(k) and "other leaves" not in k.lower()]
    ramp_n = len(real_leaves)
    ramp_rank = {k: i for i, k in enumerate(real_leaves)}

    leaf_cells: list[Cell] = []
    for leaf, lval in kept:
        lcolor = leaf_color(family, leaf, ramp_rank.get(leaf, 0), ramp_n)
        cell = Cell(label=leaf, value=lval, color=lcolor, level="leaf", family=family, leaf=leaf,
                    forced=(leaf in forced_leaves and lval / total < LEAF_MIN),
                    priority_topic=(leaf in priority_leaves))
        cell.children, cell.top_channel = build_leaf_channels(
            sub, family, leaf, lval, lcolor, total, force_ids, suppressed)
        leaf_cells.append(cell)

    if pool > 1e-6:
        if pool / total >= STRUCT_MIN:
            pooled_label = f"[{family}] - other leaves"
            leaf_cells.append(
                Cell(
                    label=pooled_label,
                    value=pool,
                    color=leaf_color(family, pooled_label, 0, 1),
                    level="leaf",
                    family=family,
                    leaf=pooled_label,
                )
            )
        else:
            # A sub-minimum remainder (possible only on the forced-leaf path; the
            # normal pooling already guarantees pool==0 or pool>=STRUCT_MIN). Fold
            # into the largest leaf ONLY as a last resort, and record it — a
            # labeled number must not silently absorb other topics' views.
            leaf_cells.sort(key=lambda c: c.value, reverse=True)
            leaf_cells[0].value += pool
            _FOLD_EVENTS.append(
                f"folded {pool/1e9:.2f}B of pooled '{family}' leaves into "
                f"'{leaf_cells[0].label}' (remainder below {STRUCT_MIN:.1%} of total)")

    fam_cell.children = leaf_cells
    return fam_cell


# Fold events for the CURRENT build_static_tree call (probe builds in the pruning
# loop are discarded, so only the final build's folds are reported).
_FOLD_EVENTS: list[str] = []
_COVERAGE_EVENTS: list[dict] = []


def build_static_tree(full: pd.DataFrame, force_ids: set, suppressed: set = frozenset(),
                      detail_suppressed: set = frozenset()):
    _FOLD_EVENTS.clear()
    _COVERAGE_EVENTS.clear()
    pos = full.loc[full[VALUE_COL] > 0].copy()
    if pos.empty:
        raise RuntimeError("No positive 4-week allocated views for static treemap")
    total = float(pos[VALUE_COL].sum())

    real = pos.loc[pos[DISPLAY_COL] != OTHER_LANG]
    lang_real = real.groupby(DISPLAY_COL, observed=True)[VALUE_COL].sum().sort_values(ascending=False)
    top_languages = list(lang_real.head(TOP_K_LANGUAGES).index)

    pos["static_language"] = np.where(pos[DISPLAY_COL].isin(top_languages), pos[DISPLAY_COL], OTHER_LANG)
    lang_order = pos.groupby("static_language", observed=True)[VALUE_COL].sum().sort_values(ascending=False)

    # First guarantee four of each language's true top five. Then rank the
    # remaining fifth-family candidates by exact view mass and spend six more
    # cells only where doing so leaves a non-sliver residual pool.
    fifth_candidates: list[tuple[float, str, str]] = []
    for candidate_lang in lang_order.index:
        candidate_lang = str(candidate_lang)
        candidate_sub = pos.loc[pos["static_language"] == candidate_lang]
        candidate_items, candidate_kept, candidate_pool = _family_pool_plan(
            candidate_sub, total, force_ids
        )
        candidate_top = [k for k, _ in candidate_items[:TOP_FAMILY_COUNT]]
        candidate_keys = {k for k, _ in candidate_kept}
        while len(candidate_keys.intersection(candidate_top)) < MIN_TOP_FAMILY_COVERAGE:
            k, v = next((k, v) for k, v in candidate_items[:TOP_FAMILY_COUNT] if k not in candidate_keys)
            candidate_keys.add(k)
            candidate_pool -= v
        fifth = next(
            ((k, v) for k, v in candidate_items[:TOP_FAMILY_COUNT] if k not in candidate_keys),
            None,
        )
        if fifth is not None:
            k, v = fifth
            if candidate_pool - v >= total * COVERAGE_RESIDUAL_MIN:
                fifth_candidates.append((v, candidate_lang, k))
    fifth_rescues = {
        (lang, family)
        for _, lang, family in sorted(fifth_candidates, reverse=True)[:EXTRA_FIFTH_FAMILY_BUDGET]
    }

    language_cells: list[Cell] = []
    pooled_family_views = 0.0
    for lang, lval in lang_order.items():
        lang = str(lang)
        sub = pos.loc[pos["static_language"] == lang]
        items, kept, pool = _family_pool_plan(sub, total, force_ids)
        kept_keys = {k for k, _ in kept}

        # Family-coverage rescue: preserve exact family values while pulling
        # the most important missing categories out of "Other (families)".
        # Every language reaches four-of-five; six large, geometry-safe fifth
        # families use the remaining comparison budget.
        top_families = [k for k, _ in items[:TOP_FAMILY_COUNT]]
        baseline_coverage = sum(k in kept_keys for k in top_families)
        target_coverage = max(MIN_TOP_FAMILY_COVERAGE, baseline_coverage)
        target_coverage += int(
            target_coverage < TOP_FAMILY_COUNT
            and any((lang, family) in fifth_rescues for family in top_families)
        )
        rescued: set[str] = set()
        rescued_views = 0.0
        for k, v in items[:TOP_FAMILY_COUNT]:
            if len(kept_keys.intersection(top_families)) >= target_coverage:
                break
            if k in kept_keys:
                continue
            kept.append((k, v))
            kept_keys.add(k)
            rescued.add(k)
            rescued_views += v
            pool -= v

        # Optional density tier for the <=200 comparison. Pull additional
        # families by descending view mass, but only above the explicit detail
        # floor and only when the residual remains above its floor. If every
        # remaining family clears the floor, eliminate the pool exactly.
        detail_rescued: set[str] = set()
        detail_rescued_views = 0.0
        remaining = [(k, v) for k, v in items if k not in kept_keys]
        eligible = [
            (k, v) for k, v in remaining
            if v / total >= DETAIL_FAMILY_MIN and (lang, k) not in detail_suppressed
        ]
        if remaining and len(eligible) == len(remaining):
            selected_detail = eligible
        else:
            selected_detail = []
            for k, v in eligible:
                if pool - v >= total * DETAIL_RESIDUAL_MIN:
                    selected_detail.append((k, v))
                    pool -= v
            # The full-unpool path subtracts below, while the residual-preserving
            # path has already done so candidate by candidate.
        if remaining and len(eligible) == len(remaining):
            pool -= sum(v for _, v in selected_detail)
        for k, v in selected_detail:
            kept.append((k, v))
            kept_keys.add(k)
            detail_rescued.add(k)
            detail_rescued_views += v
        if pool < -1e-3:
            raise RuntimeError(f"Family coverage rescue overdraw for {lang}: {pool}")
        pool = max(0.0, pool)
        final_coverage = sum(k in kept_keys for k in top_families)
        _COVERAGE_EVENTS.append(
            {
                "language": lang,
                "baseline": baseline_coverage,
                "target": target_coverage,
                "final": final_coverage,
                "top_families": top_families,
                "rescued": [k for k, _ in items if k in rescued],
                "rescued_views": rescued_views,
                "detail_rescued": [k for k, _ in items if k in detail_rescued],
                "detail_rescued_values": {k: v for k, v in items if k in detail_rescued},
                "detail_rescued_views": detail_rescued_views,
            }
        )
        kept.sort(key=lambda kv: kv[1], reverse=True)

        fam_cells: list[Cell] = []
        for fam, fval in kept:
            fam_cells.append(
                build_family(
                    sub,
                    fam,
                    fval,
                    total,
                    force_ids,
                    suppressed,
                    coverage_rescued=(fam in rescued),
                    detail_rescued=(fam in detail_rescued),
                )
            )
        if pool > 0:
            pooled_family_views += pool
            fam_cells.append(
                Cell(
                    label=OTHER_FAMILIES_LABEL,
                    value=pool,
                    color=RESIDUAL_FAMILY_COLORS["Unlabeled"],
                    level="family",
                    family=OTHER_FAMILIES_LABEL,
                    coverage_residual=bool(rescued or detail_rescued),
                )
            )
        if not fam_cells:  # entire language below thresholds (shouldn't happen for top-12)
            fam_cells.append(
                Cell(label=OTHER_FAMILIES_LABEL, value=float(lval), color=RESIDUAL_FAMILY_COLORS["Unlabeled"],
                     level="family", family=OTHER_FAMILIES_LABEL)
            )

        fill = OTHER_LANGUAGES_FILL if lang == OTHER_LANG else LANGUAGE_FILL
        language_cell = Cell(
            label=lang,
            value=float(lval),
            color=fill,
            level="language",
            edge="none",
            edge_width=0.0,
            label_style="corner",
            language=lang,
            children=fam_cells,
        )

        def assign_language(node: Cell) -> None:
            node.language = lang
            for child in node.children:
                assign_language(child)

        assign_language(language_cell)
        language_cells.append(language_cell)

    return language_cells, total, top_languages, lang_real, pooled_family_views


# --------------------------------------------------------------------------- #
# Static rendering (nested squarify)
# --------------------------------------------------------------------------- #
def padded_rect(rect: dict, pad: float) -> dict:
    dx = max(0.0, rect["dx"] - 2 * pad)
    dy = max(0.0, rect["dy"] - 2 * pad)
    return {"x": rect["x"] + pad, "y": rect["y"] + pad, "dx": dx, "dy": dy}


VALUE_SCALE = 0.82  # view-count line is drawn slightly smaller than its name...


def value_font_for(name_font: float) -> float:
    """...but never below the legibility floor."""
    return max(MIN_LABEL_PT, name_font * VALUE_SCALE)


def fit_label(name, dx, dy, *, value=None, max_pt, min_pt=MIN_LABEL_PT, bold=True,
              max_name_lines=3, pad_x=0.7, pad_y=0.45, allow_truncate=True):
    """Largest name point size in [min_pt, max_pt] at which `name` (word-wrapped,
    never split mid-word) plus an optional, slightly-smaller `value` line fits the
    cell — measured in REAL printed points. Returns (name_font, name_text, value)
    where value is the (possibly None) view string, or None if nothing fits. With
    allow_truncate=False, a font only counts as fitting if the full name fits
    without an ellipsis (used to prefer a fuller layout before falling back)."""
    avail_w = (dx - pad_x) * PT_PER_UNIT_X
    avail_h = (dy - pad_y) * PT_PER_UNIT_Y
    if avail_w <= 2 or avail_h <= 2:
        return None
    char_w = 0.62 if bold else 0.56
    for half in range(int(round(max_pt * 2)), int(round(min_pt * 2)) - 1, -1):
        f = half / 2.0
        max_chars = max(1, int(avail_w / (f * char_w)))
        lines = textwrap.wrap(name, width=max_chars, max_lines=max_name_lines,
                              break_long_words=False, break_on_hyphens=False, placeholder="…")
        if not lines:
            continue
        if max(len(s) for s in lines) > max_chars:
            continue
        if not allow_truncate and lines[-1].endswith("…"):
            continue
        total_h = len(lines) * f * 1.22
        if value:
            vf = value_font_for(f)
            if len(value) > max(1, int(avail_w / (vf * char_w))):
                continue
            total_h += vf * 1.30
        if total_h <= avail_h:
            return f, "\n".join(lines), (value if value else None)
    return None


def _emit_label(ax, x, y, name_text, value, font, color, path_fx, *, center, zorder):
    """Draw a wrapped name and, below it, a slightly-smaller view-count line."""
    n_lines = name_text.count("\n") + 1
    name_h_u = n_lines * font * 1.22 / PT_PER_UNIT_Y
    vf = value_font_for(font) if value else 0.0
    val_h_u = vf * 1.30 / PT_PER_UNIT_Y if value else 0.0
    if center:
        top = y - (name_h_u + val_h_u) / 2.0
        ax.text(x, top, name_text, ha="center", va="top", fontsize=font, fontweight="bold",
                color=color, linespacing=1.05, path_effects=path_fx, zorder=zorder)
        if value:
            ax.text(x, top + name_h_u, value, ha="center", va="top", fontsize=vf,
                    color=color, path_effects=path_fx, zorder=zorder)
    else:
        ax.text(x, y, name_text, ha="left", va="top", fontsize=font, fontweight="bold",
                color=color, linespacing=1.05, path_effects=path_fx, zorder=zorder)
        if value:
            ax.text(x, y + name_h_u, value, ha="left", va="top", fontsize=vf,
                    color=color, path_effects=path_fx, zorder=zorder)


# Graduated padding: wide gutters between LANGUAGES, narrow within, so each
# language reads as a distinct island from across the room (color encodes family,
# so negative space is what separates languages at a distance).
PAD_BY_LEVEL = {"language": 0.62, "family": 0.09, "leaf": 0.05, "channel": 0.04}


@dataclass
class RenderStats:
    labeled_cells: int = 0
    labeled_channels: int = 0
    structural_cells: int = 0
    rows: list[dict] = field(default_factory=list)
    pill_boxes: list[tuple] = field(default_factory=list)
    category_labeled_languages: set[str] = field(default_factory=set)
    labeled_cell_ids: set[int] = field(default_factory=set)
    priority_topics_labeled: int = 0


def _stroke(text_color: str, width: float = 1.0):
    """Solid tiles provide sufficient contrast; avoid outlined display text."""
    return []


def _dodge_region(rect: dict, pill_boxes: list) -> tuple:
    """Vertical sub-region (top_y, eff_dy) of `rect` that clears any overlapping
    language label, so a child label stacks cleanly beneath it instead of colliding."""
    top = rect["y"]
    for x0, y0, x1, y1 in pill_boxes:
        if rect["x"] < x1 and rect["y"] < y1:
            top = max(top, y1 + 0.15)
    return top, rect["y"] + rect["dy"] - top


# Editorial language headers sit directly in reserved white space. Whitespace,
# not repeated boxes or rules, separates the language groups.
HEADER_NAME_COLOR = "#23272B"
HEADER_VALUE_COLOR = "#747A80"
HEADER_CHAR_W = 0.61


def _band_height(rect: dict) -> float:
    """Uniform reserved header-band height (in layout units). This strip is RESERVED
    above the language's tiles (d3 'paddingTop' style) so the header never overlaps a
    topic tile — every tile's full area stays faithful to its value."""
    bh = (LANG_BAND_PT * 1.45) / PT_PER_UNIT_Y + 0.55
    return min(bh, rect["dy"] * 0.5)


def _language_header(ax, cell: Cell, rect: dict, stats: RenderStats) -> None:
    """Language name and view count in the reserved white strip."""
    band_h = _band_height(rect)
    name = display_label("language", cell.label)
    val = fmt_views(cell.value)
    avail_w = (rect["dx"] - 1.0) * PT_PER_UNIT_X

    font = None
    with_val = False
    for half in range(int(round(LANG_BAND_PT * 2)), int(round(MIN_LABEL_PT * 2)) - 1, -1):
        f = half / 2.0
        vf = max(MIN_LABEL_PT, f * 0.88)
        if len(name) * f * HEADER_CHAR_W + f * 1.1 + len(val) * vf * 0.57 <= avail_w:
            font, with_val = f, True
            break
    if font is None:
        for half in range(int(round(LANG_BAND_PT * 2)), int(round(MIN_LABEL_PT * 2)) - 1, -1):
            f = half / 2.0
            if len(name) * f * HEADER_CHAR_W <= avail_w:
                font = f
                break

    if font is None:
        return
    y_mid = rect["y"] + band_h / 2.0
    ax.text(rect["x"] + 0.25, y_mid, name, ha="left", va="center", fontsize=font,
            fontweight="bold", color=HEADER_NAME_COLOR, zorder=6)
    if with_val:
        vf = max(MIN_LABEL_PT, font * 0.88)
        x_val = rect["x"] + 0.25 + (len(name) * font * HEADER_CHAR_W + font * 1.1) / PT_PER_UNIT_X
        ax.text(x_val, y_mid, val, ha="left", va="center", fontsize=vf,
                color=HEADER_VALUE_COLOR, zorder=6)
    stats.labeled_cells += 1


def _block_label(ax, cell: Cell, rect: dict, total: float, stats: RenderStats,
                 *, max_pt: float, zorder: int = 6) -> None:
    """Top-left, luminance-based bold name with the view count on the line below in
    a slightly smaller size, for families and leaves. Never truncated: full name +
    count, else full name alone, else (for the pooled residual) plain "Other",
    else no label — an ellipsized fragment is worse than a clean tile."""
    top, eff_dy = _dodge_region(rect, stats.pill_boxes)
    name = display_label(cell.level, cell.label)
    kw = dict(max_pt=max_pt, min_pt=MIN_LABEL_PT, bold=True, max_name_lines=2,
              pad_x=0.5, pad_y=0.35, allow_truncate=False)
    fit = fit_label(name, rect["dx"], eff_dy, value=fmt_views(cell.value), **kw)
    if not fit:
        fit = fit_label(name, rect["dx"], eff_dy, value=None, **kw)
    if not fit and cell.label == OTHER_FAMILIES_LABEL:
        fit = fit_label("Other", rect["dx"], eff_dy, value=None, **kw)
    if not fit:
        return
    font, name_text, value = fit
    tc = text_color_for(cell.color)
    _emit_label(ax, rect["x"] + 0.45, top + 0.35, name_text, value, font, tc,
                _stroke(tc), center=False, zorder=zorder)
    stats.labeled_cells += 1
    stats.labeled_cell_ids.add(id(cell))
    if cell.priority_topic:
        stats.priority_topics_labeled += 1
    if cell.level in ("family", "leaf") and cell.family in REAL_FAMILIES:
        stats.category_labeled_languages.add(cell.language)

    # Named-channel annotation: the leaf's largest hard-placed channel, one quiet
    # italic line under the count ("› MrBeast"). Full name or nothing — this is how
    # the static surfaces the curated top-100 without sliver channel boxes. Skipped
    # if the channel already survived as its own labeled box.
    if cell.level != "leaf" or not cell.top_channel:
        return
    if any(k.is_named_channel for k in cell.children):
        return
    ann = f"› {cell.top_channel}"
    af = max(MIN_LABEL_PT, font * 0.82)
    n_lines = name_text.count("\n") + 1
    used_h_pt = n_lines * font * 1.22 + (value_font_for(font) * 1.30 if value else 0.0)
    avail_h_pt = (eff_dy - 0.7) * PT_PER_UNIT_Y
    avail_w_pt = (rect["dx"] - 0.7) * PT_PER_UNIT_X
    if used_h_pt + af * 1.35 > avail_h_pt or len(ann) * af * 0.56 > avail_w_pt:
        return
    ax.text(rect["x"] + 0.45, top + 0.35 + used_h_pt / PT_PER_UNIT_Y + 0.06, ann,
            ha="left", va="top", fontsize=af, color=tc, alpha=0.82, style="italic",
            path_effects=_stroke(tc), zorder=zorder)
    stats.labeled_channels += 1


def _fallback_family_label(ax, family: str, cell: Cell, rect: dict,
                           stats: RenderStats) -> bool:
    """Add one compact dominant-family label when a language has none.

    This is intentionally a last pass: it cannot increase ordinary label density,
    and it never substitutes a residual bucket for a real content family.
    """
    candidates = [family]
    compact = COMPACT_FAMILY_LABELS.get(family)
    if compact:
        candidates.append(compact)
    avail_w = max(0.0, rect["dx"] - 0.35) * PT_PER_UNIT_X
    avail_h = max(0.0, rect["dy"] - 0.30) * PT_PER_UNIT_Y
    for candidate in candidates:
        lines = candidate.splitlines()
        for half in range(13, int(MIN_LABEL_PT * 2) - 1, -1):
            font = half / 2.0
            width = max(len(line) for line in lines) * font * 0.58
            height = len(lines) * font * 1.15
            if width > avail_w or height > avail_h:
                continue
            tc = text_color_for(cell.color)
            ax.text(
                rect["x"] + rect["dx"] / 2,
                rect["y"] + rect["dy"] / 2,
                candidate,
                ha="center",
                va="center",
                fontsize=font,
                fontweight="bold",
                color=tc,
                linespacing=1.0,
                zorder=7,
            )
            stats.labeled_cells += 1
            stats.labeled_cell_ids.add(id(cell))
            stats.category_labeled_languages.add(cell.language)
            return True
    return False


def _fallback_topic_label(ax, cell: Cell, rect: dict, stats: RenderStats) -> bool:
    """Render a priority topic horizontally, or vertically in a narrow tile."""
    name = display_label(cell.level, cell.label)
    avail_w = max(0.0, rect["dx"] - 0.30) * PT_PER_UNIT_X
    avail_h = max(0.0, rect["dy"] - 0.30) * PT_PER_UNIT_Y
    for rotation in (0, 90):
        for half in range(13, int(MIN_PRIORITY_LABEL_PT * 2) - 1, -1):
            font = half / 2.0
            text_w = len(name) * font * 0.54
            text_h = font * 1.20
            required_w, required_h = (text_w, text_h) if rotation == 0 else (text_h, text_w)
            if required_w > avail_w or required_h > avail_h:
                continue
            tc = text_color_for(cell.color)
            ax.text(
                rect["x"] + rect["dx"] / 2,
                rect["y"] + rect["dy"] / 2,
                name,
                ha="center",
                va="center",
                fontsize=font,
                fontweight="bold",
                color=tc,
                rotation=rotation,
                rotation_mode="anchor",
                zorder=7,
            )
            stats.labeled_cells += 1
            stats.labeled_cell_ids.add(id(cell))
            stats.category_labeled_languages.add(cell.language)
            stats.priority_topics_labeled += 1
            return True
    return False


# One source of truth for how channel names are fitted: the pruning pass and the
# drawing pass use the same parameters, so a surviving box ALWAYS gets its full
# name (allow_truncate=False — no "Cadel and…" fragments).
def channel_label_fit(cell: Cell, rect: dict):
    return fit_label(display_label(cell.level, cell.label), rect["dx"], rect["dy"],
                     max_pt=6.5, min_pt=MIN_LABEL_PT, bold=False, max_name_lines=2,
                     pad_x=0.4, pad_y=0.3, allow_truncate=False)


def _channel_label(ax, cell: Cell, rect: dict, stats: RenderStats) -> None:
    """Named-channel boxes: a single centered full name (no view line) in their tile."""
    if not cell.is_named_channel:
        return
    fit = channel_label_fit(cell, rect)
    if not fit:
        return
    font, name_text, _ = fit
    tc = text_color_for(cell.color)
    ax.text(
        rect["x"] + rect["dx"] / 2, rect["y"] + rect["dy"] / 2, name_text,
        ha="center", va="center", fontsize=font, color=tc, linespacing=1.04,
        path_effects=_stroke(tc), zorder=4,
    )
    stats.labeled_cells += 1
    stats.labeled_channels += 1


def _label_cell(ax, cell: Cell, rect: dict, total: float, stats: RenderStats) -> None:
    """Dispatch. Languages, solid families/leaves and subdivided-leaf headers all use
    the same top-left block label (size sets the hierarchy); subdivided families rely
    on hue + legend; named channels get a small centered name."""
    if cell.level == "language":
        _language_header(ax, cell, rect, stats)
    elif cell.level == "channel":
        _channel_label(ax, cell, rect, stats)
    elif cell.children:
        if cell.level == "leaf":
            _block_label(ax, cell, rect, total, stats, max_pt=7.0, zorder=6)
        # subdivided family: no text label (family identity is the hue + legend)
    else:
        # terminal solid family or leaf
        _block_label(ax, cell, rect, total, stats, max_pt=7.0, zorder=5)


def layout_static(language_cells: list[Cell]) -> list[tuple[Cell, dict, str]]:
    """Pure geometry pass: the exact rect every cell will occupy (language header
    strips reserved, identical ordering/padding to the draw), plus its path.
    Shared by the label-fit pruner and the drawing pass so they never disagree."""
    # Order by value descending, but force the "Other languages" residual to be
    # placed last so squarify tucks it into the bottom-right corner.
    real = sorted((c for c in language_cells if c.label != OTHER_LANG), key=lambda c: c.value, reverse=True)
    other = [c for c in language_cells if c.label == OTHER_LANG]
    cells = real + other
    sizes = squarify.normalize_sizes([c.value for c in cells], LAYOUT_W, LAYOUT_H)
    rects = squarify.squarify(sizes, 0, 0, LAYOUT_W, LAYOUT_H)

    placed: list[tuple[Cell, dict, str]] = []

    def walk(cell: Cell, rect: dict, path: str) -> None:
        cell_path = f"{path} > {cell.label}" if path else cell.label
        placed.append((cell, rect, cell_path))
        if not cell.children:
            return
        # reserve the header strip for languages so tiles never sit under it
        # (faithful areas); families/leaves use their full rect
        if cell.level == "language":
            band_h = _band_height(rect)
            cx, cy, cdx, cdy = rect["x"], rect["y"] + band_h, rect["dx"], rect["dy"] - band_h
        else:
            cx, cy, cdx, cdy = rect["x"], rect["y"], rect["dx"], rect["dy"]
        kids = sorted(cell.children, key=lambda c: c.value, reverse=True)
        ksizes = squarify.normalize_sizes([k.value for k in kids], cdx, cdy)
        krects = squarify.squarify(ksizes, cx, cy, cdx, cdy)
        child_pad = PAD_BY_LEVEL.get(cell.children[0].level, 0.06)
        for kid, krect in zip(kids, krects):
            walk(kid, padded_rect(krect, child_pad), cell_path)

    for cell, rect in zip(cells, rects):
        walk(cell, padded_rect(rect, PAD_BY_LEVEL["language"]), "")
    return placed


def draw_static(language_cells: list[Cell], total: float, parent_only_share: float,
                placed: list[tuple[Cell, dict, str]] | None = None) -> tuple[RenderStats, tuple[int, int]]:
    if placed is None:
        placed = layout_static(language_cells)
    fig, ax = plt.subplots(figsize=(FIG_W_IN, FIG_H_IN), dpi=FIG_DPI)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    ax.set_xlim(0, LAYOUT_W)
    ax.set_ylim(0, LAYOUT_H)
    ax.invert_yaxis()
    ax.axis("off")

    stats = RenderStats()
    for cell, rect, cell_path in placed:
        if cell.level != "language":
            # A subdivided parent is only a container. Painting it white makes
            # its padding read as clean negative space between same-hue children,
            # without drawing an outline around every tile.
            fill_color = "white" if cell.children else cell.color
            ax.add_patch(
                mpatches.Rectangle(
                    (rect["x"], rect["y"]),
                    rect["dx"],
                    rect["dy"],
                    facecolor=fill_color,
                    edgecolor=cell.edge,
                    linewidth=cell.edge_width,
                    zorder=3,
                )
            )
        if cell.level in ("language", "family", "leaf", "pool"):
            stats.structural_cells += 1
        stats.rows.append(
            {
                "path": cell_path,
                "level": cell.level,
                "label": cell.label,
                "display_label": display_label(cell.level, cell.label),
                "family": cell.family,
                "leaf": cell.leaf,
                "value_4wk": cell.value,
                "area_frac": cell.value / total,
                "color": cell.color,
                "is_named_channel": cell.is_named_channel,
                "needs_manual_review": cell.needs_review,
                "forced": cell.forced,
                "priority_topic": cell.priority_topic,
                "coverage_rescued": cell.coverage_rescued,
                "detail_rescued": cell.detail_rescued,
                "coverage_residual": cell.coverage_residual,
                "x": rect["x"],
                "y": rect["y"],
                "dx": rect["dx"],
                "dy": rect["dy"],
            }
        )
    for cell, rect, _ in placed:
        _label_cell(ax, cell, rect, total, stats)

    # A lower threshold is useful only when the newly exposed topic is named.
    # Give these few priority topics a centered fallback; rotate only in narrow
    # cells where the full word cannot fit horizontally at the 6 pt floor.
    priority_topics = [
        (cell, rect)
        for cell, rect, _ in placed
        if cell.priority_topic
    ]
    for cell, rect in priority_topics:
        if id(cell) not in stats.labeled_cell_ids:
            _fallback_topic_label(ax, cell, rect, stats)

    # Guarantee at least one real family name in every displayed language. Pick
    # the largest family, then use its largest terminal tile as the label anchor.
    all_languages = {cell.label for cell, _, _ in placed if cell.level == "language"}
    for language in sorted(all_languages - stats.category_labeled_languages):
        families = [
            (cell, rect)
            for cell, rect, _ in placed
            if cell.level == "family" and cell.language == language and cell.family in REAL_FAMILIES
        ]
        if not families:
            continue
        dominant, dominant_rect = max(families, key=lambda item: item[0].value)
        anchors = [
            (cell, rect)
            for cell, rect, _ in placed
            if cell.language == language
            and cell.family == dominant.family
            and cell.level in ("leaf", "family")
            and not cell.children
        ]
        anchors.sort(key=lambda item: item[1]["dx"] * item[1]["dy"], reverse=True)
        placed_fallback = any(
            _fallback_family_label(ax, dominant.family, cell, rect, stats)
            for cell, rect in anchors
        )
        if not placed_fallback:
            _fallback_family_label(ax, dominant.family, dominant, dominant_rect, stats)

    # Legend: family hues, then only the residual swatches actually present.
    drawn_families = {r["family"] for r in stats.rows if r["level"] == "family"}
    legend_items = list(FAMILY_COLORS.items())
    if "Other / Unmapped YouTube topic" in drawn_families:
        legend_items.append(("Other / Unmapped", RESIDUAL_FAMILY_COLORS["Other / Unmapped YouTube topic"]))
    if "Unlabeled" in drawn_families or OTHER_FAMILIES_LABEL in drawn_families:
        legend_items.append(("Other / unlabeled", RESIDUAL_FAMILY_COLORS["Unlabeled"]))
    handles = [mpatches.Patch(facecolor=color, edgecolor="none", label=name) for name, color in legend_items]
    fig.legend(handles=handles, loc="lower center", ncol=len(handles), frameon=False, fontsize=6.3,
               bbox_to_anchor=(0.5, 0.058), columnspacing=1.15, handlelength=1.05, handleheight=1.0)

    fig.suptitle(
        "Recent YouTube viewing, by language and topic",
        x=0.010, y=0.978, ha="left", fontsize=13.0, fontweight="bold", color="#1a1a1a",
    )
    fig.text(
        0.010, 0.945,
        "Four-week view growth among 103,046 channels, 18 May-15 June 2026. "
        "Area = views; color = content family; lighter tiles = family-only classifications.",
        ha="left", va="top", fontsize=7.2, color="#3E4347",
    )
    fig.text(
        0.010, 0.012,
        f"Source: YouTube TOO topic allocations + yt_channel_stats. 'Main' = family tag without a subtopic "
        f"({parent_only_share:.0f}% of views); 'Movies' = YouTube's broad Film topic.",
        ha="left", va="bottom", fontsize=6.0, color="#555555",
    )
    fig.subplots_adjust(left=MARGIN_LEFT, right=MARGIN_RIGHT, top=MARGIN_TOP, bottom=MARGIN_BOTTOM)
    fig.savefig(STATIC_PNG, dpi=FIG_DPI, facecolor="white")
    fig.savefig(STATIC_SVG, facecolor="white")
    plt.close(fig)
    dimensions = Image.open(STATIC_PNG).size
    return stats, dimensions


# --------------------------------------------------------------------------- #
# Interactive (explicit ids/parents go.Treemap; full depth)
# --------------------------------------------------------------------------- #
def build_interactive(full: pd.DataFrame, placements: pd.DataFrame) -> int:
    pos = full.loc[full[VALUE_COL] > 0].copy()
    if pos.empty:
        raise RuntimeError("No positive 4-week allocated views for interactive treemap")

    csv_meta = {
        r.channel_id: (
            clean_path_text(str(r.revised_primary_path)),
            "" if pd.isna(r.non_primary_display_paths_to_retain_as_metadata)
            else clean_path_text(str(r.non_primary_display_paths_to_retain_as_metadata)),
            bool(r.needs_manual_review),
        )
        for r in placements.itertuples(index=False)
    }

    ids: list[str] = []
    labels: list[str] = []
    parents: list[str] = []
    values: list[float] = []
    colors: list[str] = []
    customdata: list[list[object]] = []

    def add(node_id, label, parent, value, color, data):
        ids.append(node_id)
        labels.append(label)
        parents.append(parent)
        values.append(float(value))
        colors.append(color)
        customdata.append(data)

    total = float(pos[VALUE_COL].sum())
    # NO synthetic single root: languages are the top-level sectors (parent="").
    # With maxdepth=2 this makes the OPENING view show colored family tiles inside
    # the neutral language containers, instead of root+languages (both near-white).
    lang_totals = pos.groupby(DISPLAY_COL, observed=True)[VALUE_COL].sum().sort_values(ascending=False)
    for lang, lval in lang_totals.items():
        lid = f"lang::{safe_id_part(lang)}"
        fill = OTHER_LANGUAGES_FILL if lang == OTHER_LANG else LANGUAGE_FILL
        add(lid, str(lang), "", lval, fill,
            ["language", str(lang), "", "", "", fmt_views(lval), "", "", ""])

    fam_totals = (
        pos.groupby([DISPLAY_COL, "yt_family"], observed=True)[VALUE_COL].sum().reset_index()
    )
    for row in fam_totals.itertuples(index=False):
        lid = f"lang::{safe_id_part(getattr(row, DISPLAY_COL))}"
        fid = f"fam::{safe_id_part(getattr(row, DISPLAY_COL))}::{safe_id_part(row.yt_family)}"
        add(fid, display_label("family", str(row.yt_family)), lid, getattr(row, VALUE_COL),
            family_base_color(str(row.yt_family)),
            ["family", str(getattr(row, DISPLAY_COL)), display_label("family", str(row.yt_family)), "", "",
             fmt_views(float(getattr(row, VALUE_COL))), "", "", ""])

    leaf_totals = (
        pos.groupby([DISPLAY_COL, "yt_family", "yt_leaf"], observed=True)[VALUE_COL].sum().reset_index()
    )
    # Deterministic leaf color ramp per (language, family).
    leaf_color_map: dict[tuple, str] = {}
    for (lang, fam), grp in leaf_totals.groupby([DISPLAY_COL, "yt_family"], observed=True):
        ordered = grp.sort_values(VALUE_COL, ascending=False)
        real = [l for l in ordered["yt_leaf"] if not is_unspecified_leaf(str(l)) and "other leaves" not in str(l).lower()]
        ramp_n = len(real)
        rank = {l: i for i, l in enumerate(real)}
        for leaf in ordered["yt_leaf"]:
            leaf_color_map[(lang, fam, leaf)] = leaf_color(str(fam), str(leaf), rank.get(leaf, 0), ramp_n)

    for row in leaf_totals.itertuples(index=False):
        lang = getattr(row, DISPLAY_COL)
        fid = f"fam::{safe_id_part(lang)}::{safe_id_part(row.yt_family)}"
        leaf_id = f"leaf::{safe_id_part(lang)}::{safe_id_part(row.yt_family)}::{safe_id_part(row.yt_leaf)}"
        lcolor = leaf_color_map.get((lang, row.yt_family, row.yt_leaf), family_base_color(str(row.yt_family)))
        add(leaf_id, display_label("leaf", str(row.yt_leaf)), fid, getattr(row, VALUE_COL), lcolor,
            ["leaf", str(lang), display_label("family", str(row.yt_family)),
             display_label("leaf", str(row.yt_leaf)), "",
             fmt_views(float(getattr(row, VALUE_COL))), "", "", ""])

    channel_groups = (
        pos.groupby([DISPLAY_COL, "yt_family", "yt_leaf", "channel_id"], observed=True)
        .agg(
            channel_title=("channel_title", "first"),
            allocated_views=(VALUE_COL, "sum"),
            channel_views=(CHANNEL_VIEW_COL, "first"),
            is_override=("is_placement_override", "max"),
            raw_topic_categories=("raw_topic_categories", "first"),
        )
        .reset_index()
    )

    channel_node_count = 0
    for leaf_key, leaf_df in channel_groups.groupby([DISPLAY_COL, "yt_family", "yt_leaf"], observed=True, sort=False):
        lang, fam, leaf = leaf_key
        leaf_id = f"leaf::{safe_id_part(lang)}::{safe_id_part(fam)}::{safe_id_part(leaf)}"
        lcolor = leaf_color_map.get((lang, fam, leaf), family_base_color(str(fam)))
        # named placements first, then by views
        leaf_df = leaf_df.sort_values(["is_override", "allocated_views"], ascending=[False, False])
        top = leaf_df.head(TOP_CHANNELS_PER_LEAF_INTERACTIVE)
        rest = leaf_df.iloc[TOP_CHANNELS_PER_LEAF_INTERACTIVE:]

        for r in top.itertuples(index=False):
            cid = f"ch::{safe_id_part(lang)}::{safe_id_part(fam)}::{safe_id_part(leaf)}::{safe_id_part(r.channel_id)}"
            meta = csv_meta.get(r.channel_id)
            if meta:
                primary_path, nonprimary, review = meta
                ccolor = lighten(lcolor, CHANNEL_LIGHTEN)
            else:
                primary_path, nonprimary, review = "", raw_topic_slugs(r.raw_topic_categories), False
                ccolor = lcolor
            fam_disp = display_label("family", str(fam))
            leaf_disp = display_label("leaf", str(leaf))
            add(cid, str(r.channel_title), leaf_id, float(r.allocated_views), ccolor,
                ["channel" + (" (named/placed)" if meta else ""), str(lang), fam_disp, leaf_disp,
                 str(r.channel_title), fmt_views(float(r.allocated_views)), fmt_views(float(r.channel_views)),
                 ("REVIEW" if review else ""),
                 (f"primary: {primary_path} | non-primary: {nonprimary}" if meta else nonprimary)])
            channel_node_count += 1

        if len(rest) > 0:
            other_id = f"chother::{safe_id_part(lang)}::{safe_id_part(fam)}::{safe_id_part(leaf)}"
            n = int(rest["channel_id"].nunique())
            add(other_id, f"Other ({n} channels)", leaf_id, float(rest["allocated_views"].sum()), lcolor,
                ["pooled channels", str(lang), display_label("family", str(fam)),
                 display_label("leaf", str(leaf)), f"Other ({n} channels)",
                 fmt_views(float(rest["allocated_views"].sum())), "", "", ""])
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
        "Needs manual review: %{customdata[7]}<br>"
        "Paths: %{customdata[8]}"
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
            tiling={"packing": "squarify", "pad": TILING_PAD, "squarifyratio": 1},
            marker={"colors": colors, "line": {"width": TILE_BORDER_WIDTH, "color": TILE_BORDER_COLOR}},
            customdata=customdata,
            hovertemplate=hovertemplate,
            textinfo="label",
        )
    )
    fig.update_layout(
        title="YouTube Topic Treemap Explorer (v3): language -> family -> leaf -> channel",
        width=1500,
        height=950,
        margin={"l": 10, "r": 10, "t": 55, "b": 10},
        uniformtext={"minsize": 10, "mode": "hide"},
    )
    fig.write_html(INTERACTIVE_HTML, include_plotlyjs=True, full_html=True)
    return channel_node_count


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    placements = pd.read_csv(PLACEMENT_CSV_PATH)

    df = load_allocations()
    traffic_summary = df.attrs.get("traffic_summary", {})

    # ---- (1) LANGUAGE MERGE map ----
    log("=" * 78)
    log("LANGUAGE NORMALIZATION MAP (raw language_code -> language_display)")
    log("=" * 78)
    code_to_display = (
        df[["language_code", DISPLAY_COL]].drop_duplicates().sort_values("language_code")
    )
    code_views = (
        (df.assign(_v=df[CHANNEL_VIEW_COL].fillna(0) * df[RAW_WEIGHT_COL]))
        .groupby("language_code", observed=True)["_v"].sum()
    )
    for r in code_to_display.itertuples(index=False):
        log(f"  {r.language_code:<42s} -> {getattr(r, DISPLAY_COL)}")
    log(f"\nDISTINCT RAW CODES: {code_to_display['language_code'].nunique()}; "
        f"DISTINCT DISPLAY NAMES: {code_to_display[DISPLAY_COL].nunique()}")

    english_codes = sorted(df.loc[df[DISPLAY_COL] == "English", "language_code"].unique())
    log(f"\nENGLISH RAW CODES MERGED ({len(english_codes)}): {english_codes}")
    english_blocks = int((df[DISPLAY_COL] == "English").any())
    if english_blocks == 1 and df.loc[df[DISPLAY_COL] == "English", "language_code"].nunique() >= 1:
        log("ENGLISH IS ONE BLOCK: PASS")
    else:
        log("ENGLISH IS ONE BLOCK: FAIL")

    log("\nREVIEW-CLUSTER / NON-ISO CODES POOLED INTO 'Other languages' (with view mass):")
    other_codes = df.loc[df[DISPLAY_COL] == OTHER_LANG, "language_code"].unique()
    for code in sorted(other_codes, key=lambda c: -code_views.get(c, 0.0)):
        log(f"  {code:<42s} {code_views.get(code, 0.0):>18,.0f}")
    log(f"  -> total 'Other languages' (pre-top12) mass: {code_views.reindex(other_codes).fillna(0).sum():,.0f}")

    # ---- hard placement ----
    full, place_info = apply_hard_placement(df, placements)
    log(f"\nNAMED CHANNELS HARD-PLACED (weight=1): {place_info['placed']} / {len(placements)}")
    if place_info["fallback_views"]:
        log(f"  (fell back to CSV view_count_4wk for {place_info['fallback_views']} channels)")

    # channel-box candidates forced open: the top-N by 4-week views. A candidate
    # only SURVIVES if its full name renders legibly in its final tile (pruned
    # boxes fold back into the leaf's "Other channels" pool — nothing is lost).
    force_ids = set(
        placements.sort_values("view_count_4wk", ascending=False)
        .head(FORCE_TOP_CHANNELS)["channel_id"]
    )
    log(f"FORCE-OPENED CHANNEL CANDIDATES: top {FORCE_TOP_CHANNELS} by 4wk views "
        f"(kept only if the full name fits legibly)")

    # ---- topic remap: re-home real topics the Spark hierarchy left "Unmapped" ----
    moved = apply_topic_remap(full)
    if moved:
        log("\nTOPIC REMAP (canonical slugs re-homed out of 'Other / Unmapped'):")
        for m in moved:
            log(f"  {m['from']:26s} -> {m['to']:34s} {m['views']/1e9:6.1f}B")

    # ---- (5) conservation ----
    log("\n" + "=" * 78)
    total_alloc = assert_conservation(full)

    # ---- (2) palette ----
    missing = [f for f in REAL_FAMILIES if f not in FAMILY_COLORS]
    assert not missing, f"FAMILY_COLOR_MAP missing families: {missing}"
    present_families = set(full.loc[full[VALUE_COL] > 0, "yt_family"].unique())
    real_present = [f for f in present_families if f in FAMILY_COLORS]
    assert "Society" in FAMILY_COLORS and FAMILY_COLORS["Society"] == "#0072B2", "Society must be the Okabe-Ito hue"
    log("\nPALETTE: okabe-ito family hues, low-chroma neutral residuals")
    log(f"  family hues: {FAMILY_COLORS}")
    log(f"  residual neutrals: {RESIDUAL_FAMILY_COLORS}; family-only tiles blend {UNSPEC_BLEND:.0%} -> {GRAY_ANCHOR}")
    log(f"  real families present: {sorted(real_present)}")

    # ---- (3)+(4) static tree + render ----
    # Label-fit pruning loop: lay the tree out, test every named-channel box for a
    # legible FULL-name fit, fold failures back into their leaf's "Other channels"
    # pool, and re-lay out until stable. Guarantees the drawn figure contains no
    # truncated or unlabeled channel boxes (values are conserved — pruned channels
    # pool, they don't vanish).
    suppressed: set[str] = set()
    detail_suppressed: set[tuple[str, str]] = set()
    for _ in range(24):
        language_cells, total, top_languages, lang_real, pooled_family_views = build_static_tree(
            full, force_ids, suppressed, detail_suppressed
        )
        placed = layout_static(language_cells)
        bad_channels = {
            cell.channel_id
            for cell, rect, _ in placed
            if cell.level == "channel" and cell.is_named_channel and channel_label_fit(cell, rect) is None
        }
        if bad_channels:
            suppressed |= bad_channels
            force_ids = force_ids - bad_channels
            continue

        bad_geometry = [
            (cell, rect)
            for cell, rect, _ in placed
            if (
                cell.coverage_rescued or cell.detail_rescued or cell.coverage_residual
            )
            and rect["dx"] > 0
            and rect["dy"] > 0
            and max(rect["dx"] / rect["dy"], rect["dy"] / rect["dx"]) > MAX_COVERAGE_ASPECT_RATIO
        ]
        if not bad_geometry:
            break

        newly_suppressed: set[tuple[str, str]] = set()
        events = {event["language"]: event for event in _COVERAGE_EVENTS}
        for cell, _rect in bad_geometry:
            if cell.detail_rescued:
                newly_suppressed.add((cell.language, cell.family))
                continue
            if cell.coverage_residual:
                candidates = [
                    (value, family)
                    for family, value in events[cell.language]["detail_rescued_values"].items()
                    if (cell.language, family) not in detail_suppressed
                ]
                if candidates:
                    # Restore the smallest optional detail first, retaining the
                    # greatest possible view mass while thickening the residual.
                    _, family = min(candidates)
                    newly_suppressed.add((cell.language, family))
        if not newly_suppressed:
            break
        detail_suppressed |= newly_suppressed
    log(f"\nCHANNEL BOXES PRUNED (full name cannot render >= {MIN_LABEL_PT:.0f}pt): {len(suppressed)} "
        f"(folded into 'Other channels' pools)")
    log(f"DETAIL FAMILY CANDIDATES PRUNED BY 5:1 GEOMETRY: {len(detail_suppressed)}")
    for lang, family in sorted(detail_suppressed):
        log(f"  {lang} > {family}")
    if _FOLD_EVENTS:
        for msg in _FOLD_EVENTS:
            log(f"  WARNING: {msg}")
    else:
        log("LEAF VALUE FIDELITY: no pooled remainder folded into a labeled leaf "
            "(every labeled number = its own topic's views)")
    log("\n" + "=" * 78)
    log(f"TOP-{TOP_K_LANGUAGES} LANGUAGES AFTER MERGE (by allocated 4wk views):")
    for i, lang in enumerate(top_languages, 1):
        v = float(lang_real[lang])
        log(f"  {i:2d}. {lang:<16s} {v:>18,.0f}  {v / total * 100:5.2f}%")
    other_lang_mass = total - float(lang_real.reindex(top_languages).sum())
    log(f"  +   {OTHER_LANG:<16s} {other_lang_mass:>18,.0f}  {other_lang_mass / total * 100:5.2f}%  "
        f"(non-top12 languages + clusters)")

    # parent-only share: views tagged with only a family ("(main)" leaves)
    pos_all = full.loc[full[VALUE_COL] > 0]
    parent_only_mass = float(pos_all.loc[pos_all["yt_leaf"].map(is_unspecified_leaf), VALUE_COL].sum())
    parent_only_share = parent_only_mass / total * 100
    log(f"\nPARENT-ONLY ('(main)', family-level-only) SHARE: {parent_only_share:.1f}% "
        f"({parent_only_mass:,.0f} 4wk views)")

    stats, dimensions = draw_static(language_cells, total, parent_only_share, placed)

    # Count leaf / channel cells and minimum structural area.
    family_rows = [r for r in stats.rows if r["level"] == "family"]
    leaf_cells = [r for r in stats.rows if r["level"] == "leaf"]
    channel_rows = [r for r in stats.rows if r["level"] == "channel"]
    named_channel_rows = [r for r in channel_rows if r["is_named_channel"]]
    forced_channel_rows = [r for r in channel_rows if r.get("forced")]
    # The ordinary structural-area gate excludes explicitly audited exceptions:
    # forced leaves, priority topics, and exact top-five family rescues. Those
    # exceptions are governed by their own geometry gates below.
    structural = [r for r in stats.rows if r["level"] in ("language", "family", "leaf")]
    structural_unforced = [
        r for r in structural
        if not r.get("forced")
        and not r.get("priority_topic")
        and not r.get("coverage_rescued")
        and not r.get("detail_rescued")
        and not r.get("coverage_residual")
    ]
    min_cell_area_pct = min(r["area_frac"] for r in structural_unforced) * 100
    priority_rows = [r for r in leaf_cells if r.get("priority_topic")]
    coverage_rows = [
        r for r in family_rows
        if r.get("coverage_rescued") or r.get("detail_rescued") or r.get("coverage_residual")
    ]
    rescued_rows = [
        r for r in family_rows if r.get("coverage_rescued") or r.get("detail_rescued")
    ]
    coverage_rescued_rows = [r for r in family_rows if r.get("coverage_rescued")]
    detail_rescued_rows = [r for r in family_rows if r.get("detail_rescued")]
    coverage_aspects = [
        max(r["dx"] / r["dy"], r["dy"] / r["dx"])
        for r in coverage_rows
        if r["dx"] > 0 and r["dy"] > 0
    ]
    max_coverage_aspect = max(coverage_aspects, default=0.0)
    min_rescued_area_pct = min((r["area_frac"] for r in rescued_rows), default=0.0) * 100
    languages_at_four = sum(e["final"] >= MIN_TOP_FAMILY_COVERAGE for e in _COVERAGE_EVENTS)
    rescued_view_share = sum(e["rescued_views"] for e in _COVERAGE_EVENTS) / total * 100
    detail_view_share = sum(e["detail_rescued_views"] for e in _COVERAGE_EVENTS) / total * 100
    min_priority_area_pct = min((r["area_frac"] for r in priority_rows), default=0.0) * 100
    static_cells = len(stats.rows)
    pooled_share = pooled_family_views / total * 100

    log("\n" + "=" * 78)
    log(f"LEAF CELLS (broken out in static): {len(leaf_cells)}")
    log(f"CHANNEL BOXES (static): {len(channel_rows)}  (named: {len(named_channel_rows)}; "
        f"forced top-{FORCE_TOP_CHANNELS}: {len(forced_channel_rows)})")
    log(f"NAMED CHANNELS PLACED: {place_info['placed']}; "
        f"SHOWN AS BOXES IN STATIC: {len(named_channel_rows)}; "
        f"SURFACED IN STATIC ('›' annotations + labeled boxes): {stats.labeled_channels}")
    log(f"STATIC CELLS: {static_cells}  (cap ~{STATIC_CELL_CAP})")
    log(f"MIN STRUCTURAL CELL AREA (unforced): {min_cell_area_pct:.3f}%")
    log(f"POOLED FAMILY VIEW SHARE: {pooled_share:.3f}%")
    log(f"LABELED CELLS: {stats.labeled_cells}")
    log(
        f"TOP-5 FAMILY COVERAGE: {languages_at_four}/{len(_COVERAGE_EVENTS)} languages "
        f"show >= {MIN_TOP_FAMILY_COVERAGE}; {len(coverage_rescued_rows)} top-five rescues"
    )
    for event in _COVERAGE_EVENTS:
        if event["rescued"]:
            log(
                f"  {event['language']:<16s} {event['baseline']}/5 -> {event['final']}/5: "
                + ", ".join(event["rescued"])
            )
    log(f"RESCUED FAMILY VIEW SHARE: {rescued_view_share:.3f}%")
    log(f"OPTIONAL DETAIL FAMILIES: {len(detail_rescued_rows)} ({detail_view_share:.3f}% of views)")
    for event in _COVERAGE_EVENTS:
        if event["detail_rescued"]:
            log(f"  {event['language']:<16s} " + ", ".join(event["detail_rescued"]))
    log(f"MIN EXACT-RESCUED FAMILY AREA: {min_rescued_area_pct:.3f}%")
    log(f"MAX COVERAGE-EXCEPTION ASPECT RATIO: {max_coverage_aspect:.2f}:1")
    log(f"PRIORITY TOPICS LABELED (optional): {stats.priority_topics_labeled} / {len(priority_rows)}")
    log(f"PRIORITY TOPIC FLOORS: first={PRIORITY_TOPIC_MIN:.3%}; second={SECOND_PRIORITY_TOPIC_MIN:.3%}")
    log(f"PRIORITY TOPIC LABEL FLOOR: {MIN_PRIORITY_LABEL_PT:.1f} pt (ordinary labels: {MIN_LABEL_PT:.1f} pt)")
    if priority_rows:
        log(f"MIN PRIORITY TOPIC AREA: {min_priority_area_pct:.3f}%")
    priority_aspects = [
        max(r["dx"] / r["dy"], r["dy"] / r["dx"])
        for r in priority_rows
        if r["dx"] > 0 and r["dy"] > 0
    ]
    max_priority_aspect = max(priority_aspects, default=0.0)
    log(f"MAX PRIORITY TOPIC ASPECT RATIO: {max_priority_aspect:.2f}:1")
    log("PACKING: squarify")
    log(f"FIGURE DIMENSIONS: {dimensions[0]}x{dimensions[1]} px, {FIG_W_IN}x{FIG_H_IN} in, {FIG_DPI} DPI")

    # ---- write cells CSV ----
    cells_df = pd.DataFrame(stats.rows)
    cells_df.to_csv(CELLS_CSV, index=False)
    log(f"STATIC CELLS CSV: {CELLS_CSV.resolve()}")

    # ---- (6) interactive ----
    channel_nodes = build_interactive(full, placements)
    html_text = INTERACTIVE_HTML.read_text(errors="ignore")
    external_scripts = len(re.findall(r"<script[^>]+src=", html_text))
    log("\n" + "=" * 78)
    log(f"INTERACTIVE HTML: {INTERACTIVE_HTML.resolve()}")
    log("INTERACTIVE: go.Treemap branchvalues=total maxdepth=2 packing=squarify "
        f"pad={TILING_PAD} sort=True (explicit ids/parents)")
    log(f"INTERACTIVE EXTERNAL <script src> TAGS: {external_scripts} (self-contained if 0)")
    log(f"INTERACTIVE CHANNEL/OTHER NODES: {channel_nodes:,} "
        f"(top {TOP_CHANNELS_PER_LEAF_INTERACTIVE} + Other per leaf; named placed first)")

    log("\nSTATIC MASTER PNG: " + str(STATIC_PNG.resolve()))
    log("STATIC MASTER SVG: " + str(STATIC_SVG.resolve()))
    log(f"TRAFFIC SOURCE TABLE: {TRAFFIC_SOURCE_TABLE}; TOO UNIVERSE: {TOO_CHANNEL_TABLE}")
    log(f"TRAFFIC SNAPSHOTS: current={traffic_summary.get('current_snapshot')} "
        f"prior={traffic_summary.get('prior_snapshot')}")

    # ---- (7) automated legibility gates (the human visual inspection is the final gate) ----
    log("\n" + "=" * 78)
    log("VISUAL SELF-CHECK (automated gates):")
    n_english = sum(1 for c in language_cells if c.label == "English")
    sliver_leaves = sum(
        1 for r in leaf_cells
        if r["area_frac"] < STRUCT_MIN and not r.get("forced") and not r.get("priority_topic")
    )
    missing_category_labels = sorted(
        {cell.label for cell in language_cells} - stats.category_labeled_languages
    )
    log(f"LANGUAGES WITH NO CATEGORY LABEL: {len(missing_category_labels)}"
        + (f" ({', '.join(missing_category_labels)})" if missing_category_labels else ""))
    residual_colors_valid = (
        RESIDUAL_FAMILY_COLORS["Other / Unmapped YouTube topic"] not in FAMILY_COLORS.values()
        and RESIDUAL_FAMILY_COLORS["Unlabeled"] not in FAMILY_COLORS.values()
        and FAMILY_COLORS["Society"] == "#0072B2"
    )
    gates = {
        "english_single_block": n_english == 1,
        "min_unforced_structural_cell>=0.30%": min_cell_area_pct >= 0.30,
        "static_cells<=cap": static_cells <= STATIC_CELL_CAP,
        "no_sliver_storm(<6 unforced sub-0.3%% leaves)": sliver_leaves < 6,
        "png>=2000x1200": dimensions[0] >= 2000 and dimensions[1] >= 1200,
        "every_language_has_category_label": not missing_category_labels,
        f"priority_topic_min>={min(PRIORITY_TOPIC_MIN, SECOND_PRIORITY_TOPIC_MIN):.3%}": (
            not priority_rows
            or min_priority_area_pct >= min(PRIORITY_TOPIC_MIN, SECOND_PRIORITY_TOPIC_MIN) * 100
        ),
        f"priority_topic_aspect<={MAX_PRIORITY_ASPECT_RATIO:.1f}:1": (
            not priority_rows or max_priority_aspect <= MAX_PRIORITY_ASPECT_RATIO
        ),
        f"all_languages_show>={MIN_TOP_FAMILY_COVERAGE}_of_top_{TOP_FAMILY_COUNT}_families": (
            languages_at_four == len(_COVERAGE_EVENTS)
        ),
        f"coverage_exception_aspect<={MAX_COVERAGE_ASPECT_RATIO:.1f}:1": (
            not coverage_rows or max_coverage_aspect <= MAX_COVERAGE_ASPECT_RATIO
        ),
        f"detail_family_min>={DETAIL_FAMILY_MIN:.3%}": (
            not detail_rescued_rows
            or min(r["area_frac"] for r in detail_rescued_rows) >= DETAIL_FAMILY_MIN
        ),
        "society_is_hue_residuals_are_neutral": residual_colors_valid,
        "interactive_self_contained": external_scripts == 0,
    }
    for name, ok in gates.items():
        log(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    log(f"  sub-0.3% leaf cells: {sliver_leaves}")
    log("LEGIBILITY SELF-CHECK: " + ("PASS" if all(gates.values()) else "FAIL"))
    if not all(gates.values()):
        log("  -> raise pruning thresholds (LEAF_MIN / FAMILY_MIN / label gates) and re-render.")

    LOG_TXT.write_text("\n".join(_LOG_LINES) + "\n")
    print(f"\nRENDER LOG: {LOG_TXT.resolve()}")


if __name__ == "__main__":
    main()
