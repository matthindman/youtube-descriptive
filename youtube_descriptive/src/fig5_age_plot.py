"""Plotting for Figure 5 (recently founded channels capture a disproportionate share of attention).

Shared by `scripts/render_fig5_local.py`, so every render draws from the aggregate tables written by
`15_fig5_age_databricks.py`.

Inputs (pandas DataFrames, the exported aggregates):
  blocks:    block (0 = whole universe), n_channels, wv_sum, n_dated, wv_dated, q25, median, q75, mean,
             share_le_<year>, ...
  year_dist: block, founded_year, n_channels, wv_sum

Panels: a = share of channels vs share of weekly views by founding year; b = founding-year distribution
within each traffic block; c = median founding year by block (channel-weighted with interquartile band,
and view-weighted). The view-weighted median is computed from `year_dist` (linear interpolation inside
the calendar year), so it is approximate to a few hundredths of a year.
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from matplotlib.patches import Rectangle  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

INK = "#1f1f1f"
MUTED = "#6b6b6b"
GRID = "#e6e6e3"
CHANNELS = "#2a78d6"
VIEWS = "#eb6834"
CMAP = LinearSegmentedColormap.from_list("age", ["#ffffff", "#9ec3ee", "#2a78d6", "#123e7a"])
Y0 = 2005

# Figure variants the renderer draws: (name, panels). Main figure first.
RENDER_SPECS = [("abc", "abc")]


def _numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _spines(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
        ax.spines[s].set_linewidth(0.6)
    ax.tick_params(colors=INK, labelsize=7, width=0.6, length=3)


def weighted_median(g: pd.DataFrame, col: str) -> float:
    """Median decimal founding year of a year histogram, interpolating linearly within the year."""
    g = g.sort_values("founded_year")
    c = (g[col].cumsum() / g[col].sum()).to_numpy()
    i = int(np.searchsorted(c, 0.5))
    prev = c[i - 1] if i else 0.0
    return float(g.founded_year.iloc[i] + (0.5 - prev) / (c[i] - prev))


def view_weighted_medians(year_dist: pd.DataFrame) -> pd.Series:
    """View-weighted median founding year per block (index 1..n) and overall (index 0)."""
    s = {b: weighted_median(g, "wv_sum") for b, g in year_dist.groupby("block")}
    allg = year_dist.groupby("founded_year")[["wv_sum", "n_channels"]].sum().reset_index()
    s[0] = weighted_median(allg, "wv_sum")
    return pd.Series(s).sort_index()


def year_matrix(year_dist: pd.DataFrame, n_blocks: int, y1: int):
    """Share of each block's dated channels founded in each year (rows = block 1..n, cols = year)."""
    years = np.arange(Y0, y1 + 1)
    m = (year_dist.pivot_table(index="block", columns="founded_year", values="n_channels", aggfunc="sum")
         .reindex(index=range(1, n_blocks + 1), columns=years).fillna(0.0))
    return m.div(m.sum(axis=1), axis=0), years


def plot_fig5(blocks, year_dist, panels: str = "abc", path=None):
    blocks, year_dist = _numeric(blocks), _numeric(year_dist)
    year_dist = year_dist[year_dist.block > 0]
    bl = blocks[blocks.block > 0].sort_values("block")
    allrow = blocks[blocks.block == 0].iloc[0]
    n_blocks = len(bl)
    y1 = int(year_dist.founded_year.max())
    vw = view_weighted_medians(pd.concat([year_dist]))

    fig, axes = plt.subplots(1, len(panels), figsize=(7.2, 2.55), gridspec_kw={"wspace": 0.7})
    axes = dict(zip(panels, np.atleast_1d(axes)))

    # a: share of channels and of weekly views by founding year.
    if "a" in axes:
        ax = axes["a"]
        t = (year_dist.groupby("founded_year")[["n_channels", "wv_sum"]].sum()
             .reindex(range(Y0, y1 + 1)).fillna(0.0))
        ch, vw_share = t.n_channels / t.n_channels.sum(), t.wv_sum / t.wv_sum.sum()
        x = t.index.to_numpy()
        ax.bar(x - 0.2, ch, width=0.4, color=CHANNELS, lw=0, label="Channels")
        ax.bar(x + 0.2, vw_share, width=0.4, color=VIEWS, lw=0, label="Weekly views")
        ax.bar([y1 - 0.2, y1 + 0.2], [ch[y1], vw_share[y1]], width=0.4, color="white", alpha=0.55, lw=0)
        _spines(ax)
        ax.set_xlim(Y0 - 0.8, y1 + 0.8)
        ax.set_xticks([2005, 2010, 2015, 2020, y1])
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0%}"))
        ax.grid(True, axis="y", color=GRID, linewidth=0.5, zorder=0)
        ax.set_axisbelow(True)
        ax.set_xlabel("Founding year", fontsize=8, color=INK)
        ax.set_ylabel("Share of total", fontsize=8, color=INK)
        ax.legend(fontsize=6, frameon=False, loc="upper left", handlelength=1.0, borderaxespad=0.2)

    # b: founding-year distribution within each traffic block (rows), one cell per block x year.
    if "b" in axes:
        ax = axes["b"]
        m, years = year_matrix(year_dist, n_blocks, y1)
        im = ax.imshow(m.to_numpy(), aspect="auto", cmap=CMAP, vmin=0,
                       vmax=float(np.nanpercentile(m, 99)),
                       extent=(years[0] - 0.5, years[-1] + 0.5, n_blocks + 0.5, 0.5),
                       interpolation="nearest")
        ax.plot(bl["median"] - 0.5, bl.block, color=VIEWS, lw=1.1, marker="o", ms=2.2,
                label="Median")
        # Partial final year (founded up to the 2026-08-31 snapshot): hatched.
        ax.add_patch(Rectangle((y1 - 0.5, 0.5), 1, n_blocks, fill=False, hatch="////",
                               edgecolor=MUTED, lw=0))
        _spines(ax)
        ax.set_xlim(years[0] - 0.5, years[-1] + 0.5)
        ax.set_ylim(n_blocks + 0.5, 0.5)
        ax.set_yticks([1, 5, 10, 15, 20])
        ax.set_xticks([2005, 2015, y1])
        ax.set_xlabel("Founding year", fontsize=8, color=INK)
        ax.set_ylabel("Traffic block (1 = top)", fontsize=8, color=INK)
        ax.legend(fontsize=6, frameon=True, framealpha=0.9, edgecolor="none", loc="center left",
                  handlelength=1.2, borderaxespad=0.2)
        cb = fig.colorbar(im, ax=ax, fraction=0.05, pad=0.02, shrink=0.8)
        cb.ax.tick_params(labelsize=6, width=0.5, length=2)
        cb.outline.set_visible(False)
        cb.ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0%}"))
        cb.ax.set_title("Share of\nblock", fontsize=6, color=INK, pad=3, loc="left")

    # c: median founding year by block, channel-weighted (with interquartile band) and view-weighted.
    if "c" in axes:
        ax = axes["c"]
        ax.fill_between(bl.block, bl.q25, bl.q75, color=CHANNELS, alpha=0.18, lw=0)
        ax.plot(bl.block, bl["median"], color=CHANNELS, lw=1.4, marker="o", ms=2.6,
                label="Median, channels (band: IQR)")
        ax.plot(bl.block, vw.reindex(bl.block).to_numpy(), color=VIEWS, lw=1.4, marker="s", ms=2.4,
                label="Median, weighted by views")
        ax.axhline(float(allrow["median"]), color=MUTED, lw=0.6, ls=(0, (2, 1.5)))
        ax.text(2.2, float(allrow["median"]) - 0.12, f"All channels: {allrow['median']:.1f}", ha="left",
                va="top", fontsize=6, color=MUTED)
        _spines(ax)
        ax.set_xlim(0.5, n_blocks + 0.5)
        ax.set_ylim(2014.2, 2026)
        ax.set_xticks([1, 5, 10, 15, 20])
        ax.set_xlabel("Traffic block (1 = top)", fontsize=8, color=INK)
        ax.set_ylabel("Founding year", fontsize=8, color=INK)
        ax.grid(True, axis="y", color=GRID, linewidth=0.5, zorder=0)
        ax.legend(fontsize=6, frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.22),
                  handlelength=1.2, borderaxespad=0.2)

    for k, ax in axes.items():
        ax.text(-0.2, 1.06, k, transform=ax.transAxes, fontsize=11, fontweight="bold", color=INK)
    if path:
        fig.savefig(path, bbox_inches="tight", dpi=300)
        plt.close(fig)
    return fig
