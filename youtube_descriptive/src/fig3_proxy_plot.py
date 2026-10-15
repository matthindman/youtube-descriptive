"""Plotting for Figure 3 (subscribers are an imprecise proxy for attention).

Shared by the Databricks notebook `13_fig3_subscriber_proxy_databricks.py` (preview) and
`scripts/render_fig3_local.py`, so both draw from the same aggregate tables.

Inputs (pandas DataFrames, the exported aggregates):
  bins:   variant, group_type, grp, bin_id, subs_geomean, n_channels, wv_p05..wv_p95, vps_p10..vps_p90, ...
  hist2d: variant, sbin, vbin, sbin_dex, vbin_dex, n_channels
  fit:    variant, ols_slope, ols_intercept, ols_r2, spearman, pooled_vps, ...
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LogNorm  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

from fig2_map_plot import CAT_COLORS, CATEGORIES  # noqa: E402

INK = "#1f1f1f"
MUTED = "#6b6b6b"
GRID = "#e6e6e3"
LINE = "#2a78d6"
BAND_INNER = "#9ec3ee"
BAND_OUTER = "#dbe8f8"
REF = "#eb6834"
SMOOTH_BINS = 5  # rolling window (bins) for the drawn quantile curves
GROWTH = ["flat or falling", "0-2%", "2-10%", ">10%"]
GROWTH_COLORS = dict(zip(GROWTH, ["#b9b9b4", "#9ec3ee", "#2a78d6", "#123e7a"]))

# (variant, panel c) pairs the renderers draw. Main figure first.
RENDER_SPECS = [("all", "category"), ("all", None), ("active", "category"), ("all", "growth")]


def _numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        if c not in ("variant", "group_type", "grp", "outcome", "model"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _fmt_count(v, _pos=None) -> str:
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if v >= div:
            x = v / div
            return f"{x:g}{suf}" if x < 10 or x == int(x) else f"{x:.0f}{suf}"
    if v >= 1:
        return f"{v:g}"
    return f"{v:g}"


def _style(ax):
    ax.set_xscale("log")
    ax.set_yscale("log")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
        ax.spines[s].set_linewidth(0.6)
    ax.tick_params(colors=INK, labelsize=7, width=0.6, length=3)
    ax.tick_params(which="minor", width=0.4, length=1.5, colors=MUTED)
    ax.grid(True, which="major", color=GRID, linewidth=0.5, zorder=0)
    ax.xaxis.set_major_locator(LogLocator(base=10, numticks=12))
    ax.xaxis.set_major_formatter(FuncFormatter(_fmt_count))
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.set_xlabel("Subscribers", fontsize=8, color=INK)


def _curve(bins: pd.DataFrame, variant: str, group_type: str = "all", grp: str = "all",
           window: int = SMOOTH_BINS) -> pd.DataFrame:
    """One binned curve, with every quantile smoothed by a centred rolling mean over `window` bins in
    log space (the binned quantiles themselves are in the CSV; only the drawn lines are smoothed)."""
    d = bins[(bins.variant == variant) & (bins.group_type == group_type) & (bins.grp == grp)]
    d = d.sort_values("subs_geomean").reset_index(drop=True)
    for col in [c for c in d.columns if c.startswith(("wv_p", "vps_p"))]:
        floor = 1.0 if col.startswith("wv_") else 1e-6
        d[col] = 10 ** np.log10(d[col].clip(lower=floor)).rolling(window, center=True, min_periods=1).mean()
    return d


def plot_fig3(bins, hist2d, fit, variant: str = "all", panel_c: str | None = "category", path=None):
    bins, hist2d, fit = _numeric(bins), _numeric(hist2d), _numeric(fit)
    f = fit[fit.variant == variant].iloc[0]
    c = _curve(bins, variant)
    x = c.subs_geomean.to_numpy()
    xlim = (1e4, x.max() * 4)

    ncols = 3 if panel_c else 2
    fig, axes = plt.subplots(1, ncols, figsize=(7.2 if ncols == 3 else 5.0, 2.55),
                             gridspec_kw={"wspace": 0.42})
    ax_a, ax_b = axes[0], axes[1]

    # a: weekly views vs subscribers (density, median, IQR, 90% envelope, OLS).
    h = hist2d[(hist2d.variant == variant) & hist2d.vbin.notna()]
    sd, vd = h.sbin_dex.iloc[0], h.vbin_dex.iloc[0]
    s_idx = np.arange(h.sbin.min(), h.sbin.max() + 2)
    v_idx = np.arange(h.vbin.min(), h.vbin.max() + 2)
    grid = np.full((len(v_idx) - 1, len(s_idx) - 1), np.nan)
    grid[(h.vbin - v_idx[0]).astype(int), (h.sbin - s_idx[0]).astype(int)] = h.n_channels
    ax_a.pcolormesh(10 ** (s_idx * sd), 10 ** (v_idx * vd), grid, cmap="Greys",
                    norm=LogNorm(vmin=MIN_N_SHADE, vmax=np.nanmax(grid) * 1.6), alpha=0.8,
                    shading="flat", rasterized=True, zorder=1, linewidth=0)
    ylo = 10.0
    clip = lambda a: np.maximum(a, ylo)  # noqa: E731
    ax_a.fill_between(x, clip(c.wv_p25), clip(c.wv_p75), color=LINE, alpha=0.22, lw=0, zorder=3,
                      label="Middle 50%")
    ax_a.plot(x, clip(c.wv_p05), color=LINE, lw=0.7, ls=(0, (2, 1.5)), zorder=3, label="5th-95th pct")
    ax_a.plot(x, clip(c.wv_p95), color=LINE, lw=0.7, ls=(0, (2, 1.5)), zorder=3)
    ax_a.plot(x, clip(c.wv_p50), color=LINE, lw=1.4, zorder=4, label="Median")
    xx = np.logspace(4, np.log10(xlim[1]), 50)
    ax_a.plot(xx, 10 ** (f.ols_intercept + f.ols_slope * np.log10(xx)), color=INK, lw=0.8, ls=(0, (5, 1.5, 1, 1.5)),
              zorder=5, label="Log-log OLS")
    ax_a.set_ylim(ylo, max(c.wv_p95.max(), np.nanmax(10 ** (v_idx * vd))) * 1.5)
    ax_a.set_ylabel("Weekly views", fontsize=8, color=INK)
    ax_a.text(0.03, 0.97, f"slope {f.ols_slope:.2f}, $R^2$ = {f.ols_r2:.2f}\nSpearman ρ = {f.spearman:.2f}",
              transform=ax_a.transAxes, ha="left", va="top", fontsize=6.5, color=INK)
    ax_a.legend(fontsize=6.5, frameon=True, framealpha=0.85, edgecolor="none", loc="lower right", handlelength=1.6)

    # b: views per subscriber vs subscribers.
    ax_b.fill_between(x, c.vps_p10, c.vps_p90, color=BAND_OUTER, alpha=0.9, lw=0, zorder=2, label="10th-90th pct")
    ax_b.fill_between(x, c.vps_p25, c.vps_p75, color=BAND_INNER, alpha=0.9, lw=0, zorder=3, label="25th-75th pct")
    ax_b.plot(x, c.vps_p50, color=LINE, lw=1.4, zorder=4, label="Median")
    ax_b.axhline(f.pooled_vps, color=REF, lw=1.0, ls=(0, (4, 2)), zorder=5,
                 label=f"Overall mean ({f.pooled_vps:.2f})")
    ax_b.set_ylabel("Weekly views per subscriber", fontsize=8, color=INK)
    lo = max(np.nanmin(c.vps_p10[c.vps_p10 > 0]), 1e-5) / 2
    ax_b.set_ylim(lo, max(c.vps_p90.max(), f.pooled_vps) * 4)
    ax_b.legend(fontsize=6.5, frameon=True, framealpha=0.85, edgecolor="none", loc="lower right", handlelength=1.4)

    # c (optional): median VPS by category or by recent subscriber growth.
    if panel_c:
        ax_c = axes[2]
        if panel_c == "category":
            groups, colors, title = CATEGORIES, CAT_COLORS, "Median by YouTube topic"
        else:
            groups, colors, title = GROWTH, GROWTH_COLORS, "Median by subscriber growth"
        top = ax_b.get_ylim()[1]
        for g in groups:
            d = _curve(bins, variant, panel_c, g)
            if len(d) < 3:
                continue
            top = max(top, d.vps_p50.max() * 2.5)
            ax_c.plot(d.subs_geomean, d.vps_p50, color=colors[g], lw=1.1, label=g, zorder=3)
        ax_c.fill_between(x, c.vps_p10, c.vps_p90, color="#ececea", lw=0, zorder=1, label="All, 10th-90th")
        ax_c.plot(x, c.vps_p50, color=INK, lw=0.8, ls=(0, (1, 1.5)), label="All, median", zorder=4)
        ax_c.axhline(f.pooled_vps, color=REF, lw=1.0, ls=(0, (4, 2)), zorder=2)
        ax_c.set_ylabel("Weekly views per subscriber", fontsize=8, color=INK)
        ax_c.set_title(title, fontsize=7, color=MUTED, loc="left")
        ax_c.set_ylim(ax_b.get_ylim()[0], top)
        ax_c.legend(fontsize=6, frameon=False, loc="lower left", ncol=2, handlelength=1.2, columnspacing=0.8)

    for i, ax in enumerate(axes):
        _style(ax)
        if ax is ax_a:
            ax.grid(False)
        ax.set_xlim(*xlim)
        ax.yaxis.set_major_formatter(FuncFormatter(_fmt_count))
        ax.text(-0.25, 1.0, "abc"[i], transform=ax.transAxes, fontsize=10, fontweight="bold", color=INK,
                va="bottom")
    ax_b.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:g}"))
    if panel_c:
        axes[2].yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:g}"))

    if path:
        fig.savefig(path, dpi=300, bbox_inches="tight")
        plt.close(fig)
    return fig


MIN_N_SHADE = 10
