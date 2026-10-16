"""Plotting for Figure 5 (older channels dominate the highest-attention ranks).

Shared by `scripts/render_fig5_local.py`, so every render draws from the aggregate tables written by
`15_fig5_age_databricks.py`.

Inputs (pandas DataFrames, the exported aggregates):
  blocks:    block (0 = whole universe), n_channels, wv_sum, n_dated, wv_dated, q25, median, q75, mean,
             share_le_<year>, ...
  year_dist: block, founded_year, n_channels, wv_sum
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

INK = "#1f1f1f"
MUTED = "#6b6b6b"
GRID = "#e6e6e3"
CHANNELS = "#2a78d6"
VIEWS = "#eb6834"
BENCHMARK = 2015  # founded in or before this year (share_le_<year> column of `blocks`)
CMAP = LinearSegmentedColormap.from_list("age", ["#ffffff", "#9ec3ee", "#2a78d6", "#123e7a"])

# Figure variants the renderer draws: (name, panels). Main figure first.
RENDER_SPECS = [("ab", "ab"), ("a", "a"), ("b", "b")]


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


def year_matrix(year_dist: pd.DataFrame, n_blocks: int, y0: int = 2005, y1: int | None = None):
    """Share of each block's dated channels founded in each year (rows = block 1..n, cols = year)."""
    d = year_dist[year_dist.block > 0]
    y1 = int(d.founded_year.max()) if y1 is None else y1
    years = np.arange(y0, y1 + 1)
    m = (d.pivot_table(index="block", columns="founded_year", values="n_channels", aggfunc="sum")
         .reindex(index=range(1, n_blocks + 1), columns=years).fillna(0.0))
    return m.div(m.sum(axis=1), axis=0), years


def plot_fig5(blocks, year_dist, panels: str = "ab", path=None):
    blocks, year_dist = _numeric(blocks), _numeric(year_dist)
    bl = blocks[blocks.block > 0].sort_values("block")
    allrow = blocks[blocks.block == 0].iloc[0]
    n_blocks = len(bl)
    bench_col = f"share_le_{BENCHMARK}"

    ncols = len(panels)
    fig, axes = plt.subplots(1, ncols, figsize=(7.2 if ncols == 2 else 3.6, 2.55),
                             gridspec_kw={"wspace": 0.6})
    axes = dict(zip(panels, np.atleast_1d(axes)))

    # a: founding-year distribution within each traffic block (rows), one cell per block x year.
    if "a" in axes:
        ax = axes["a"]
        m, years = year_matrix(year_dist, n_blocks)
        im = ax.imshow(m.to_numpy(), aspect="auto", cmap=CMAP, vmin=0,
                       vmax=float(np.nanpercentile(m, 99)),
                       extent=(years[0] - 0.5, years[-1] + 0.5, n_blocks + 0.5, 0.5),
                       interpolation="nearest")
        ax.plot(bl["median"] - 0.5, bl.block, color=VIEWS, lw=1.1, marker="o", ms=2.2,
                label="Median founding date")
        _spines(ax)
        ax.set_xlim(years[0] - 0.5, years[-1] + 0.5)
        ax.set_ylim(n_blocks + 0.5, 0.5)
        ax.set_yticks([1, 5, 10, 15, 20])
        ax.set_xticks(np.arange(2005, years[-1] + 1, 5))
        ax.set_xlabel("Year founded", fontsize=8, color=INK)
        ax.set_ylabel("Traffic block (1 = top 5% of views)", fontsize=8, color=INK)
        ax.legend(fontsize=6, frameon=True, framealpha=0.9, edgecolor="none", loc="lower left",
                  handlelength=1.2, borderaxespad=0.2)
        cb = fig.colorbar(im, ax=ax, fraction=0.045, pad=0.02)
        cb.ax.tick_params(labelsize=6, width=0.5, length=2)
        cb.outline.set_visible(False)
        cb.ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0%}"))
        cb.set_label("Share of block", fontsize=6.5, color=INK)

    # b: median founding date with interquartile range, and the share founded by the benchmark year.
    if "b" in axes:
        ax = axes["b"]
        ax.fill_between(bl.block, bl.q25, bl.q75, color=CHANNELS, alpha=0.18, lw=0)
        ax.plot(bl.block, bl["median"], color=CHANNELS, lw=1.4, marker="o", ms=2.6,
                label="Median (band: interquartile range)")
        ax.axhline(float(allrow["median"]), color=MUTED, lw=0.6, ls=(0, (2, 1.5)))
        ax.text(n_blocks, float(allrow["median"]) + 0.15, "All channels", ha="right", va="bottom",
                fontsize=6, color=MUTED)
        _spines(ax)
        ax.set_xlim(0.5, n_blocks + 0.5)
        ax.set_xticks([1, 5, 10, 15, 20])
        ax.set_xlabel("Traffic block (1 = top 5% of views)", fontsize=8, color=INK)
        ax.set_ylabel("Founding year", fontsize=8, color=INK)
        ax.grid(True, axis="y", color=GRID, linewidth=0.5, zorder=0)
        ax2 = ax.twinx()
        ax2.plot(bl.block, bl[bench_col], color=VIEWS, lw=1.4, marker="s", ms=2.4,
                 label=f"Founded {BENCHMARK} or earlier (right)")
        ax2.plot(bl.block, bl["share_le_2010"], color=VIEWS, lw=1.0, ls=(0, (3, 1.5)), marker="s", ms=2.0,
                 mfc="white", label="Founded 2010 or earlier (right)")
        ax.set_ylim(2015.5, 2028.5)
        ax2.set_ylim(0, 1)
        ax2.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0%}"))
        ax2.tick_params(colors=VIEWS, labelsize=7, width=0.6, length=3)
        for s in ("top", "left", "bottom"):
            ax2.spines[s].set_visible(False)
        ax2.spines["right"].set_color(VIEWS)
        ax2.spines["right"].set_linewidth(0.6)
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, fontsize=6, frameon=True, framealpha=0.9, edgecolor="none",
                  loc="upper right", handlelength=1.2, borderaxespad=0.2)

    for k, ax in axes.items():
        ax.text(-0.18, 1.06, k, transform=ax.transAxes, fontsize=11, fontweight="bold", color=INK)
    if path:
        fig.savefig(path, bbox_inches="tight", dpi=300)
        plt.close(fig)
    return fig
