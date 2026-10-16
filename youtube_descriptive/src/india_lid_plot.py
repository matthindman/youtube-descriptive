"""Plotting for the India Hindi-vs-English Extended Data panel (candidate ED Fig. 4).

Category mix (share of channels and share of weekly views) for Hindi-labelled channels and for India-declared
`eng` channels split by what IndicLID calls them. Shared by `scripts/render_india_lid_local.py`; draws from the
`panel` aggregate written by `20_india_lid_pilot_databricks.py`.

Input `panel` (DataFrame): source, grp, category, n_est, n_sample, wv_est.
  source `frame` rows are exact (Hindi groups, all IN x eng); `<run>:<variant>` rows carry the IndicLID split.
Style constants follow `fig4_thresholds_plot.py` (INK / MUTED / GRID, spines, 6-8 pt text, 300 dpi); category
colours follow Figure 2 so categories keep one colour across figures.
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

INK = "#1f1f1f"
MUTED = "#6b6b6b"
GRID = "#e6e6e3"
CATEGORIES = ["Music", "Gaming", "Entertainment", "Lifestyle", "Sports", "Society", "Knowledge"]
CAT_COLORS = dict(zip(CATEGORIES, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7"]))
CAT_COLORS["Unclassified"] = "#b9b9b4"
CAT_ORDER = CATEGORIES + ["Unclassified"]
DARK_TEXT = {"Music", "Gaming", "Society", "Knowledge"}  # fills that take white labels

GROUP_LABELS = {
    "hin_all": "Hindi-labelled\n(all countries)",
    "hin_IN": "Hindi-labelled\n(India-declared)",
    "eng_IN_romanised_indic": "India-declared English,\nIndicLID: romanised\nHindi/Indic",
    "eng_IN_english": "India-declared English,\nIndicLID: English",
    "eng_IN_native": "India-declared English,\nIndicLID: native script",
    "eng_IN_all": "India-declared English\n(as labelled)",
    "eng_IN_lex_hindi": "India-declared English,\nHindi function words\nin text (lexicon)",
    "eng_IN_lex_other": "India-declared English,\nno Hindi lexicon signal",
}
# Panel variants: name -> groups, top to bottom.
VARIANTS = {
    "main": ["hin_all", "eng_IN_romanised_indic", "eng_IN_english"],
    "with_all": ["hin_all", "eng_IN_romanised_indic", "eng_IN_english", "eng_IN_all"],
    "lexicon_sensitivity": ["hin_all", "eng_IN_lex_hindi", "eng_IN_lex_other", "eng_IN_all"],
    "hin_in_sensitivity": ["hin_IN", "hin_all", "eng_IN_romanised_indic", "eng_IN_english", "eng_IN_all"],
}


def _numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ("n_est", "n_sample", "wv_est"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def mix(panel: pd.DataFrame, source: str, groups: list[str]) -> pd.DataFrame:
    """Long table: grp, category, n_est, wv_est, share_channels, share_views, n_total, n_sample_total."""
    panel = _numeric(panel)
    rows = []
    for g in groups:
        src = "frame" if g.startswith("hin") or g == "eng_IN_all" else source
        if "_lex_" in g:  # lexicon split rows are tagged <run>:lex
            src = source.split(":")[0] + ":lex"
        d = panel[(panel.source == src) & (panel.grp == g)].set_index("category").reindex(CAT_ORDER)
        d = d.fillna({"n_est": 0, "n_sample": 0, "wv_est": 0})
        d["grp"] = g
        d["category"] = d.index
        d["share_channels"] = d.n_est / d.n_est.sum()
        d["share_views"] = d.wv_est / d.wv_est.sum()
        d["n_total"] = d.n_est.sum()
        d["wv_total"] = d.wv_est.sum()
        d["n_sample_total"] = d.n_sample.sum()
        rows.append(d.reset_index(drop=True))
    return pd.concat(rows, ignore_index=True)


def _spines(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
        ax.spines[s].set_linewidth(0.6)
    ax.tick_params(colors=INK, labelsize=7, width=0.6, length=3)


def plot_india_panel(panel: pd.DataFrame, source: str, groups: list[str], path=None):
    m = mix(panel, source, groups)
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 0.62 * len(groups) + 1.25), sharey=True,
                             gridspec_kw={"wspace": 0.14})
    for ax, col, title in ((axes[0], "share_channels", "Share of channels"),
                           (axes[1], "share_views", "Share of weekly views")):
        for i, g in enumerate(groups):
            left = 0.0
            for c in CAT_ORDER:
                w = float(m[(m.grp == g) & (m.category == c)][col].iloc[0])
                ax.barh(i, w, left=left, color=CAT_COLORS[c], height=0.68, lw=0)
                if w >= 0.055:
                    ax.text(left + w / 2, i, f"{w:.0%}", ha="center", va="center", fontsize=6,
                            color="white" if c in DARK_TEXT else INK)
                left += w
        ax.set_xlim(0, 1)
        ax.set_ylim(len(groups) - 0.5, -0.5)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0%}"))
        _spines(ax)
        ax.set_title(title, fontsize=8, color=INK, loc="left", pad=4)
    n_lab = []
    for g in groups:
        r = m[m.grp == g].iloc[0]
        n_lab.append(f"{GROUP_LABELS[g]}\n(n = {r.n_total:,.0f})")
    axes[0].set_yticks(range(len(groups)), n_lab)
    axes[0].tick_params(axis="y", length=0, labelsize=6.5)
    axes[0].spines["left"].set_visible(False)
    handles = [plt.Rectangle((0, 0), 1, 1, color=CAT_COLORS[c], lw=0) for c in CAT_ORDER]
    fig.legend(handles, CAT_ORDER, ncol=len(CAT_ORDER), fontsize=6, frameon=False, loc="lower center",
               bbox_to_anchor=(0.55, -0.02), handlelength=1.0, columnspacing=1.0)
    for k, ax in zip("ab", axes):
        ax.text(-0.04 if k == "b" else -0.02, 1.13, k, transform=ax.transAxes, fontsize=11,
                fontweight="bold", color=INK)
    if path:
        fig.savefig(path, bbox_inches="tight", dpi=300)
        plt.close(fig)
    return fig, m
