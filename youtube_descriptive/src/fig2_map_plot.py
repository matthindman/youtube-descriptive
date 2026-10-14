"""Plotting for Figure 2 (a map of public YouTube). Pure numpy + pandas + matplotlib.

Shared by the Databricks notebook `12_fig2_platform_map_databricks.py` (preview) and
`scripts/render_fig2_local.py`, so both draw from the same aggregate tables.

Inputs (pandas DataFrames, the exported aggregates):
  cells:    language_group, category, label, canonical_id, n_channels, weekly_views
  blocks:   scope ('all' or a language code), block, category, n_channels, weekly_views
  lang_cat: language, category, n_channels, weekly_views
"""
from __future__ import annotations

import itertools

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.patches import Patch, Rectangle  # noqa: E402

INK = "#1f1f1f"
MUTED = "#6b6b6b"
GRID = "#e6e6e3"
SURFACE = "#ffffff"

# YouTube's seven parent topics, in the reference categorical order (fixed, never cycled).
CATEGORIES = ["Music", "Gaming", "Entertainment", "Lifestyle", "Sports", "Society", "Knowledge"]
CAT_COLORS = dict(zip(CATEGORIES, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7"]))
CAT_COLORS["Unclassified"] = "#b9b9b4"
CAT_ORDER = CATEGORIES + ["Unclassified"]

LANG_NAMES = {
    "eng": "English", "spa": "Spanish", "por": "Portuguese", "hin": "Hindi", "rus": "Russian",
    "jpn": "Japanese", "kor": "Korean", "ind": "Indonesian", "arb": "Arabic", "ara": "Arabic",
    "tha": "Thai", "vie": "Vietnamese", "tur": "Turkish", "fra": "French", "deu": "German",
    "ben": "Bengali", "urd": "Urdu", "tam": "Tamil", "tel": "Telugu", "mar": "Marathi",
    "cmn": "Chinese", "zho": "Chinese", "tgl": "Tagalog", "fil": "Filipino", "ita": "Italian",
    "pol": "Polish", "ukr": "Ukrainian", "pes": "Persian", "fas": "Persian", "zsm": "Malay",
    "msa": "Malay", "mal": "Malayalam", "kan": "Kannada", "guj": "Gujarati", "pan": "Punjabi",
    "nld": "Dutch", "ron": "Romanian", "ell": "Greek", "heb": "Hebrew", "swe": "Swedish",
    "other": "Other languages", "und": "Unknown language",
}

NUMERIC = ["n_channels", "weekly_views", "block"]


def lang_name(code: str) -> str:
    return LANG_NAMES.get(code, code)


def _numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in NUMERIC:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df


# ---------------------------------------------------------------- squarified treemap (Bruls et al.)
def _worst(row, w):
    s = sum(row)
    return max(max(w * w * r / (s * s), (s * s) / (w * w * r)) for r in row)


def squarify(values, x, y, dx, dy):
    """Lay out positive `values` (sorted descending) in the rectangle; returns [(x, y, w, h)]."""
    total = float(sum(values))
    if total <= 0 or not values:
        return [(x, y, 0.0, 0.0) for _ in values]
    areas = [v * dx * dy / total for v in values]
    rects = []
    while areas:
        w = min(dx, dy)
        row = [areas[0]]
        i = 1
        while i < len(areas) and _worst(row + [areas[i]], w) <= _worst(row, w):
            row.append(areas[i])
            i += 1
        s = sum(row)
        if dx >= dy:  # lay the row down the left edge
            rw = s / dy
            yy = y
            for a in row:
                rects.append((x, yy, rw, a / rw))
                yy += a / rw
            x, dx = x + rw, dx - rw
        else:  # lay the row along the top edge
            rh = s / dx
            xx = x
            for a in row:
                rects.append((xx, y, a / rh, rh))
                xx += a / rh
            y, dy = y + rh, dy - rh
        areas = areas[i:]
    return rects


def _ink_on(hex_color: str) -> str:
    r, g, b = (int(hex_color[i:i + 2], 16) / 255 for i in (1, 3, 5))
    lum = 0.2126 * r ** 2.2 + 0.7152 * g ** 2.2 + 0.0722 * b ** 2.2
    return INK if lum > 0.33 else "#ffffff"


def _language_order(cells: pd.DataFrame) -> list[str]:
    tot = cells.groupby("language_group").weekly_views.sum().sort_values(ascending=False)
    named = [x for x in tot.index if x not in ("other", "und")]
    return named + [x for x in ("other", "und") if x in tot.index]


def draw_treemap(ax, cells: pd.DataFrame, width=1.0, height=1.0):
    cells = _numeric(cells)
    cells = cells[cells.weekly_views > 0]
    total = cells.weekly_views.sum()
    langs = _language_order(cells)
    lang_tot = cells.groupby("language_group").weekly_views.sum()
    # Keep "other" and "und" last: lay out the named languages by size, then the two pools.
    for lang, (lx, ly, lw, lh) in zip(langs, squarify([lang_tot[x] for x in langs], 0, 0, width, height)):
        lc = cells[cells.language_group == lang]
        cat_tot = lc.groupby("category").weekly_views.sum().sort_values(ascending=False)
        for cat, (cx, cy, cw, ch) in zip(cat_tot.index, squarify(list(cat_tot.values), lx, ly, lw, lh)):
            cc = lc[lc.category == cat].copy()
            cc["pooled"] = cc.canonical_id.isna()
            cc = cc.sort_values(["pooled", "weekly_views"], ascending=[True, False])
            color = CAT_COLORS.get(cat, CAT_COLORS["Unclassified"])
            for (_, r), (x, y, w, h) in zip(cc.iterrows(), squarify(list(cc.weekly_views), cx, cy, cw, ch)):
                # Pooled cells keep the category color; head channels are set off by their own borders.
                ax.add_patch(Rectangle((x, y), w, h, facecolor=color, edgecolor=SURFACE,
                                       linewidth=0.0 if r.pooled else 0.3))
                # Channel names only where they fit (about 0.6% of the map per line of text).
                if not r.pooled and w * h >= 0.0035 and w >= 0.045:
                    name = str(r.label)
                    name = name if len(name) <= 18 else name[:17] + "…"
                    ax.text(x + 0.004, y + 0.006, name, fontsize=4.2, va="top", ha="left",
                            color=_ink_on(color), clip_on=True)
            ax.add_patch(Rectangle((cx, cy), cw, ch, fill=False, edgecolor=SURFACE, linewidth=0.8))
        ax.add_patch(Rectangle((lx, ly), lw, lh, fill=False, edgecolor=SURFACE, linewidth=2.2))
        share = lang_tot[lang] / total * 100
        text = f"{lang_name(lang)} {share:.0f}%"
        need = (len(text) + 1) * 0.0125 * width / 1.6  # rough label length in map units at 6.5 pt
        box = dict(boxstyle="square,pad=0.15", facecolor=SURFACE, edgecolor="none", alpha=0.85)
        if need <= lw and lh >= 0.04:
            ax.text(lx + 0.006, ly + lh - 0.008, text, fontsize=6.5, va="bottom", ha="left", color=INK,
                    fontweight="bold", bbox=box)
        elif need <= lh and lw >= 0.03:  # tall, narrow language: label up the left edge
            ax.text(lx + 0.008, ly + lh - 0.008, text, fontsize=6.5, va="bottom", ha="left", color=INK,
                    fontweight="bold", rotation=90, bbox=box)
    ax.set_xlim(0, width)
    ax.set_ylim(height, 0)
    ax.set_axis_off()


# ---------------------------------------------------------------- category mix across traffic blocks
def block_shares(blocks: pd.DataFrame, scope: str) -> pd.DataFrame:
    b = _numeric(blocks)
    b = b[b.scope == scope]
    piv = b.pivot_table(index="block", columns="category", values="weekly_views", aggfunc="sum", fill_value=0)
    piv = piv.reindex(columns=[c for c in CAT_ORDER if c in piv.columns])
    return piv.div(piv.sum(axis=1), axis=0)


def draw_blocks(ax, shares: pd.DataFrame, title: str, show_y: bool):
    bottom = np.zeros(len(shares))
    x = shares.index.values
    for cat in shares.columns:
        ax.bar(x, shares[cat].values * 100, bottom=bottom, width=0.86, color=CAT_COLORS[cat],
               edgecolor=SURFACE, linewidth=0.4)
        bottom += shares[cat].values * 100
    ax.set_xlim(0.4, len(shares) + 0.6)
    ax.set_ylim(0, 100)
    ax.set_xticks([1, 5, 10, 15, 20])
    ax.set_title(title, fontsize=7, color=INK, loc="left", pad=3)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelsize=6.5, length=2)
    if show_y:
        ax.set_ylabel("Share of weekly views (%)", fontsize=7, color=INK)
    else:
        ax.tick_params(labelleft=False)


# ---------------------------------------------------------------- one platform or many?
def _jsd(p, q) -> float:
    """Jensen-Shannon divergence in bits between two (unnormalised) distributions."""
    p, q = np.asarray(p, float), np.asarray(q, float)
    p, q = p / p.sum(), q / q.sum()
    m = (p + q) / 2

    def kl(a, b):
        nz = a > 0
        return float(np.sum(a[nz] * np.log2(a[nz] / b[nz])))

    return 0.5 * kl(p, m) + 0.5 * kl(q, m)


def divergence_stats(lang_cat: pd.DataFrame, blocks: pd.DataFrame, top_languages: list[str]) -> pd.DataFrame:
    """Between-language vs within-language variation in category mix (classified views only).

    between: JSD between each pair of top languages' category mixes (mean over pairs).
    within:  for each block language, the view-weighted mean JSD of its traffic blocks' mixes from
             that language's own overall mix.
    """
    lc = _numeric(lang_cat)
    lc = lc[lc.category != "Unclassified"]
    mix = lc.pivot_table(index="language", columns="category", values="weekly_views", aggfunc="sum",
                         fill_value=0).reindex(columns=CATEGORIES, fill_value=0)
    rows = []
    for a, b in itertools.combinations([x for x in top_languages if x in mix.index], 2):
        rows.append({"kind": "between", "a": a, "b": b, "jsd_bits": _jsd(mix.loc[a], mix.loc[b])})
    bl = _numeric(blocks)
    bl = bl[bl.category != "Unclassified"]
    for scope in bl.scope.unique():
        s = bl[bl.scope == scope].pivot_table(index="block", columns="category", values="weekly_views",
                                              aggfunc="sum", fill_value=0).reindex(columns=CATEGORIES,
                                                                                    fill_value=0)
        overall = s.sum(axis=0)
        w = s.sum(axis=1)
        j = [_jsd(s.loc[k], overall) for k in s.index if w[k] > 0]
        rows.append({"kind": "within", "a": scope, "b": None,
                     "jsd_bits": float(np.average(j, weights=w[w > 0].values))})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- the figure
def plot_fig2(cells, blocks, lang_cat, path=None):
    cells, blocks = _numeric(cells), _numeric(blocks)
    block_langs = [s for s in blocks.scope.unique() if s != "all"]
    lang_tot = cells.groupby("language_group").weekly_views.sum()
    block_langs = sorted(block_langs, key=lambda x: -lang_tot.get(x, 0))
    top_langs = [x for x in _language_order(cells) if x not in ("other", "und")]
    stats = divergence_stats(lang_cat, blocks, top_langs)
    between = stats[stats.kind == "between"].jsd_bits.mean()
    within = stats[(stats.kind == "within") & (stats.a != "all")].jsd_bits.mean()

    fig = plt.figure(figsize=(7.2, 7.0), dpi=200)
    gs = fig.add_gridspec(2, 1 + len(block_langs), height_ratios=[2.35, 1], hspace=0.16, wspace=0.12)
    ax_a = fig.add_subplot(gs[0, :])
    draw_treemap(ax_a, cells, width=1.6, height=1.0)
    ax_a.set_aspect("equal")

    axes_b = [fig.add_subplot(gs[1, i]) for i in range(1 + len(block_langs))]
    draw_blocks(axes_b[0], block_shares(blocks, "all"), "All languages", show_y=True)
    for ax, lang in zip(axes_b[1:], block_langs):
        draw_blocks(ax, block_shares(blocks, lang), lang_name(lang), show_y=False)
    fig.text(0.5, 0.04, "Traffic block (1 = channels holding the top 5% of weekly views, 20 = the bottom 5%)",
             ha="center", fontsize=7, color=INK)
    fig.text(0.5, 0.008,
             f"Category mix differs more between languages (mean JSD {between:.2f} bits) "
             f"than across traffic blocks within one (mean {within:.2f} bits)",
             ha="center", fontsize=6.5, color=MUTED)

    handles = [Patch(facecolor=CAT_COLORS[c], edgecolor="none", label=c) for c in CAT_ORDER]
    ax_a.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 1.075), ncol=8, frameon=False,
                fontsize=6, handlelength=1.0, handletextpad=0.4, columnspacing=1.0)
    ax_a.text(-0.012, 1.075, "a", transform=ax_a.transAxes, fontsize=10, fontweight="bold", color=INK, va="bottom")
    axes_b[0].text(-0.42, 1.08, "b", transform=axes_b[0].transAxes, fontsize=10, fontweight="bold", color=INK,
                   va="bottom")
    if len(axes_b) > 1:
        axes_b[1].text(-0.1, 1.08, "c", transform=axes_b[1].transAxes, fontsize=10, fontweight="bold",
                       color=INK, va="bottom")
    named = cells[cells.canonical_id.notna()]
    cut = (named.weekly_views.min() / cells.weekly_views.sum() * 100) if len(named) else float("nan")
    fig.suptitle("Weekly views of channels with ≥10k subscribers, by language, category and channel\n"
                 f"a: outlined cells are the {len(named)} channels holding ≥{cut:.2f}% of weekly views; "
                 "the rest of each category is pooled", fontsize=8, color=MUTED, y=1.0)
    if path:
        fig.savefig(path, bbox_inches="tight")
    return fig, stats
