"""Plotting for Figure 4 (common subscriber thresholds capture unequal shares of channels and views).

Shared by `scripts/render_fig4_local.py` (and usable from the notebook for previews), so every
render draws from the same aggregate tables written by `14_fig4_thresholds_databricks.py`.

Inputs (pandas DataFrames, the exported aggregates):
  vbins:      vbin, vbin_dex, tier, n_channels, wv_sum, wv_min, wv_max
  sbins:      sbin, sbin_dex, n_channels, wv_sum
  thresholds: threshold, n_ge, wv_ge, n_all, wv_all, share_channels, share_views_observed
  status:     tier, status, n_channels, wv_sum
  tail:       r0, model, is_headline, tail_is_finite, missing_mass, total, obs_total_all, ...
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

INK = "#1f1f1f"
MUTED = "#6b6b6b"
GRID = "#e6e6e3"
CHANNELS = "#2a78d6"
VIEWS = "#eb6834"
# Largest tier first: the order of stacking (bottom up) and of the legend.
TIERS = [">=1M", "100k-1M", "10k-100k", "1k-10k", "<1k", "hidden"]
TIER_LABELS = {">=1M": "≥1M", "100k-1M": "100k–1M", "10k-100k": "10k–100k", "1k-10k": "1k–10k",
               "<1k": "<1k", "hidden": "Hidden"}
TIER_COLORS = dict(zip(TIERS, ["#123e7a", "#2a78d6", "#9ec3ee", "#f2b077", "#eb6834", "#c9c9c4"]))
MISSING_COLOR = "#7a7a75"
MARKED = [1_000, 10_000, 100_000, 1_000_000]  # thresholds annotated in panel b

# Figure variants the renderer draws: (name, panels). Main figure first.
RENDER_SPECS = [("abc", "abc"), ("ab", "ab")]


def _numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    flags = ("is_headline", "tail_is_finite", "tail_ordering_ok")
    for c in df.columns:
        if c in flags:
            df[c] = df[c].astype(str).str.lower() == "true"
        elif c not in ("tier", "status", "model"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _fmt_count(v, _pos=None) -> str:
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if v >= div:
            x = v / div
            return f"{x:g}{suf}" if x < 10 or x == int(x) else f"{x:.0f}{suf}"
    return f"{v:g}"


def _pct(v: float) -> str:
    """Percent with enough digits for small shares (0.0008% rather than 0.00%)."""
    if v <= 0:
        return "0%"
    pct = 100 * v
    digits = 0 if pct >= 10 else max(0, int(np.ceil(-np.log10(pct))) + 1)
    return f"{pct:.{digits}f}%"


def _spines(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
        ax.spines[s].set_linewidth(0.6)
    ax.tick_params(colors=INK, labelsize=7, width=0.6, length=3)
    ax.tick_params(which="minor", width=0.4, length=1.5, colors=MUTED)


def _logx(ax):
    ax.set_xscale("log")
    ax.xaxis.set_major_locator(LogLocator(base=10, numticks=12))
    ax.xaxis.set_major_formatter(FuncFormatter(_fmt_count))
    ax.xaxis.set_minor_formatter(NullFormatter())


def headline(tail: pd.DataFrame) -> pd.Series:
    """The headline tail row (r0 x model flagged in the notebook)."""
    return tail[tail.is_headline].iloc[0]


def missing_bounds(tail: pd.DataFrame) -> tuple[float, float]:
    """(headline missing mass, largest finite missing mass over every r0 x model)."""
    fin = tail[tail.tail_is_finite]
    return float(headline(tail).missing_mass), float(fin.missing_mass.max())


def capture_curve(vbins: pd.DataFrame) -> pd.DataFrame:
    """Cumulative channels and views by tier, from the top weekly-view bin down. Exact at bin edges:
    rank order is weekly-view order, so the top r channels are the channels in the highest bins."""
    wide = (vbins.pivot_table(index="vbin", columns="tier", values="wv_sum", aggfunc="sum")
            .reindex(columns=TIERS).fillna(0.0).sort_index(ascending=False))
    n = vbins.groupby("vbin").n_channels.sum().reindex(wide.index)
    cum = wide.cumsum()
    cum["rank"] = n.cumsum().to_numpy()
    return cum.reset_index()


def threshold_curves(sbins: pd.DataFrame, n_all: float, wv_all: float) -> pd.DataFrame:
    """Share of channels and of weekly views at or above each 0.05-dex subscriber bin's lower edge."""
    d = sbins.sort_values("sbin", ascending=False).copy()
    d["subs_lo"] = 10 ** (d.sbin * d.sbin_dex)
    d["share_channels"] = d.n_channels.cumsum() / n_all
    d["share_views"] = d.wv_sum.fillna(0).cumsum() / wv_all
    return d.sort_values("subs_lo")


def plot_fig4(vbins, sbins, thresholds, status, tail, panels: str = "abc", path=None):
    vbins, sbins, thresholds = _numeric(vbins), _numeric(sbins), _numeric(thresholds)
    status, tail = _numeric(status), _numeric(tail)
    h = headline(tail)
    miss_head, miss_max = missing_bounds(tail)
    total = float(h.total)  # observed + headline missing mass

    ncols = len(panels)
    fig, axes = plt.subplots(1, ncols, figsize=(7.2 if ncols == 3 else 5.0, 2.55),
                             gridspec_kw={"wspace": 0.42})
    axes = dict(zip(panels, np.atleast_1d(axes)))

    # a: cumulative share of weekly views captured by the top-r channels, split by subscriber tier.
    ax = axes["a"]
    cc = capture_curve(vbins)
    x = cc["rank"].to_numpy()
    base = np.zeros(len(cc))
    for t in TIERS:
        top = base + cc[t].to_numpy() / total
        ax.fill_between(x, base, top, color=TIER_COLORS[t], lw=0, step="post", label=TIER_LABELS[t])
        base = top
    ax.plot(x, base, color=INK, lw=0.8, drawstyle="steps-post")
    if miss_max > 0:
        ax.axhline(1.0, color=MISSING_COLOR, lw=0.6, ls=(0, (2, 1.5)))
    _logx(ax)
    _spines(ax)
    ax.set_xlim(1, x.max() * 1.5)
    ax.xaxis.set_major_locator(LogLocator(base=100, numticks=8))
    ax.set_ylim(0, 1.02)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0%}"))
    ax.grid(True, which="major", color=GRID, linewidth=0.5, zorder=0)
    ax.set_xlabel("Channels ranked by weekly views", fontsize=8, color=INK)
    ax.set_ylabel("Cumulative share of weekly views", fontsize=8, color=INK)
    ax.legend(title="Subscribers", fontsize=6, title_fontsize=6, frameon=True, framealpha=0.9,
              edgecolor="none", loc="upper left", handlelength=1.2, borderaxespad=0.2)

    # b: share of channels and of weekly views at or above each subscriber threshold.
    ax = axes["b"]
    thr = thresholds.sort_values("threshold")
    n_all, wv_all = float(thr.n_all.iloc[0]), float(thr.wv_all.iloc[0])
    tc = threshold_curves(sbins, n_all, wv_all)
    tc = tc[(tc.subs_lo >= 10) & (tc.subs_lo <= 3e8)]
    ax.plot(tc.subs_lo, tc.share_channels, color=CHANNELS, lw=1.4, label="Channels")
    ax.plot(tc.subs_lo, tc.share_views * wv_all / total, color=VIEWS, lw=1.4, label="Views")
    for _, r in thr[thr.threshold.isin(MARKED)].iterrows():
        sv = r.wv_ge / total
        ax.plot([r.threshold] * 2, [r.share_channels, sv], color=MUTED, lw=0.5, zorder=1)
        ax.scatter([r.threshold], [r.share_channels], s=10, color=CHANNELS, zorder=3)
        ax.scatter([r.threshold], [sv], s=10, color=VIEWS, zorder=3)
        ax.annotate(f"{sv:.0%}", (r.threshold, sv), textcoords="offset points", xytext=(3, 3),
                    fontsize=6, color=VIEWS)
        ch = r.share_channels
        ax.annotate(_pct(ch), (r.threshold, ch), textcoords="offset points", xytext=(-4, -2),
                    ha="right", va="top", fontsize=6, color=CHANNELS)
    _logx(ax)
    _spines(ax)
    ax.set_yscale("log")
    ax.set_ylim(2e-4, 1.6)
    ax.yaxis.set_major_locator(LogLocator(base=10, numticks=6))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: _pct(v)))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.grid(True, which="major", color=GRID, linewidth=0.5, zorder=0)
    ax.set_xlabel("Subscriber threshold", fontsize=8, color=INK)
    ax.set_ylabel("Share at or above threshold", fontsize=8, color=INK)
    ax.set_xlim(10, 3e8)
    ax.xaxis.set_major_locator(LogLocator(base=100, numticks=8))
    ax.legend(fontsize=6, frameon=False, loc="lower left", handlelength=1.0, borderaxespad=0.2)

    # c: composition of channels and of weekly views by tier, with the modelled missing mass.
    if "c" in axes:
        ax = axes["c"]
        m = status[status.status.isin(["zero", "positive"])].groupby("tier").agg(
            n=("n_channels", "sum"), v=("wv_sum", "sum")).reindex(TIERS).fillna(0)
        bars = {"Channels": m.n / m.n.sum(), "Weekly views": m.v / total}
        for i, (label, shares) in enumerate(bars.items()):
            left = 0.0
            for t in TIERS:
                w = float(shares[t])
                ax.barh(i, w, left=left, color=TIER_COLORS[t], height=0.6, lw=0)
                if w >= 0.06:
                    ax.text(left + w / 2, i, f"{w:.0%}", ha="center", va="center", fontsize=6,
                            color="white" if t in (">=1M", "100k-1M", "<1k") else INK)
                left += w
            if label == "Weekly views" and miss_head > 0:
                ax.barh(i, miss_head / total, left=left, color="white", edgecolor=MISSING_COLOR,
                        hatch="////", height=0.6, lw=0.5)
        ax.set_yticks([0, 1], list(bars))
        ax.set_ylim(1.6, -0.6)
        ax.set_xlim(0, 1)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0%}"))
        _spines(ax)
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="y", length=0, labelsize=7)
        ax.set_xlabel("Share of total", fontsize=8, color=INK)
        note = (f"Modelled missing views: {_pct(miss_head / total)} (headline fit);\n"
                f"at most {_pct(miss_max / (float(h.obs_total_all) + miss_max))} under any fit")
        ax.set_title(note, fontsize=6, color=MUTED, loc="left", pad=4)

    for k, ax in axes.items():
        ax.text(-0.18, 1.06, k, transform=ax.transAxes, fontsize=11, fontweight="bold", color=INK)
    if path:
        fig.savefig(path, bbox_inches="tight", dpi=300)
        plt.close(fig)
    return fig
