"""Plotting for Figure 1 (discovery saturation). Pure pandas + matplotlib.

Shared by the Databricks notebook `07_fig1_discovery_saturation_databricks.py` and
`scripts/render_fig1_local.py`, so the paper figure and the in-notebook preview are
drawn by the same code from the same aggregate tables.

Inputs (pandas DataFrames, the exported aggregates):
  exhaustion: variant, bin_type, bin_order, bin_label, step_order, step_label,
              n_channels, weekly_views          (non-cumulative counts per step)
  trending:   variant, week_start, n_channels, weekly_views
  totals:     variant, bin_type, bin_order, n_channels_total, weekly_views_total
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

# Reference blue ramp (ordinal, light surface, starts no lighter than step 250).
BLUE_4 = ["#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]
BLUE_5 = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#0d366b"]
INK = "#1f1f1f"
MUTED = "#6b6b6b"
GRID = "#e6e6e3"

# (variant, bin_type, cohort) rendered by the notebook and the local renderer.
# Panel a uses the cohort; panels b-c (trending flow) always use every channel.
RENDER_SPECS = [
    ("active_panel", "subscribers", "incumbent"),   # main Fig 1
    ("all", "subscribers", "incumbent"),            # ED Fig 1
    ("active_panel", "weekly_views", "incumbent"),  # ED Fig 1
    ("active_crawl", "subscribers", "incumbent"),   # ED Fig 1 sensitivity
    ("active_panel", "subscribers", "all"),         # comparison: no incumbent filter
]

COHORT_NOTES = {
    "incumbent": "panel a: incumbents (created by 2026-03-04 and \u226510k subscribers when first recorded)",
    "all": "panel a: all channels, incumbents and entrants",
}

VARIANT_TITLES = {
    "active_panel": "Active channels (upload within 365 days of 2026-10-05, Topic channels excluded)",
    "active_crawl": "Active channels (upload within 365 days before 2026-03-04, Topic channels excluded)",
    "all": "All channels with ≥10k subscribers",
}


def _style(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelsize=8)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


NUMERIC = ["bin_order", "step_order", "n_channels", "weekly_views", "n_channels_total", "weekly_views_total"]


def _numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Spark DECIMAL columns arrive as object dtype; coerce to float."""
    df = df.copy()
    for c in NUMERIC:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df


def _cohort(df: pd.DataFrame, cohort: str) -> pd.DataFrame:
    return df[df.cohort == cohort] if "cohort" in df else df


def cumulative_curves(exhaustion: pd.DataFrame, totals: pd.DataFrame, variant: str, bin_type: str,
                      cohort: str = "all") -> pd.DataFrame:
    """Cumulative share of each bin's universe discovered by each snowball step."""
    exhaustion, totals = _numeric(_cohort(exhaustion, cohort)), _numeric(_cohort(totals, cohort))
    ex = exhaustion[(exhaustion.variant == variant) & (exhaustion.bin_type == bin_type)]
    ex = ex[ex.step_order <= 13]
    # Steps with no new channels have no row; fill them with zeros so every curve runs 0..13.
    labels = ex[["bin_order", "bin_label"]].drop_duplicates()
    grid = labels.merge(pd.DataFrame({"step_order": range(14)}), how="cross")
    ex = grid.merge(ex[["bin_order", "step_order", "n_channels", "weekly_views"]],
                    on=["bin_order", "step_order"], how="left").fillna({"n_channels": 0, "weekly_views": 0})
    ex = ex.sort_values(["bin_order", "step_order"]).copy()
    tot = totals[(totals.variant == variant) & (totals.bin_type == bin_type)]
    ex = ex.merge(tot[["bin_order", "n_channels_total", "weekly_views_total"]], on="bin_order")
    ex["cum_n"] = ex.groupby("bin_order").n_channels.cumsum()
    ex["cum_v"] = ex.groupby("bin_order").weekly_views.cumsum()
    ex["share_channels"] = ex.cum_n / ex.n_channels_total
    ex["share_views"] = ex.cum_v / ex.weekly_views_total.where(ex.weekly_views_total > 0)
    return ex


def plot_fig1(exhaustion, trending, totals, variant: str, bin_type: str = "subscribers",
              cohort: str = "all", path: str | None = None):
    curves = cumulative_curves(exhaustion, totals, variant, bin_type, cohort)
    trending, totals = _numeric(_cohort(trending, "all")), _numeric(_cohort(totals, "all"))
    bins = curves[["bin_order", "bin_label"]].drop_duplicates().sort_values("bin_order")
    colors = BLUE_4 if len(bins) <= 4 else BLUE_5

    fig = plt.figure(figsize=(7.2, 3.0), dpi=200)
    gs = fig.add_gridspec(2, 2, width_ratios=[1.35, 1], hspace=0.55, wspace=0.32)
    ax_a = fig.add_subplot(gs[:, 0])
    ax_b = fig.add_subplot(gs[0, 1])
    ax_c = fig.add_subplot(gs[1, 1], sharex=ax_b)

    # a: solid = share of channels, dashed = share of weekly views.
    end_labels = []
    for color, (_, b) in zip(colors, bins.iterrows()):
        d = curves[curves.bin_order == b.bin_order]
        ax_a.plot(d.step_order, d.share_channels * 100, color=color, lw=2, label=b.bin_label)
        ax_a.plot(d.step_order, d.share_views * 100, color=color, lw=1.2, ls=(0, (3, 2)))
        last = d.iloc[-1]
        end_labels.append([last.share_channels * 100, last.step_order, f"{last.share_channels * 100:.1f}%"])
    # End labels, nudged apart so close curves don't collide (min gap 4.5 points of y).
    end_labels.sort(key=lambda r: -r[0])
    for i in range(1, len(end_labels)):
        end_labels[i][0] = min(end_labels[i][0], end_labels[i - 1][0] - 4.5)
    for y, x, text in end_labels:
        ax_a.annotate(text, (x, y), xytext=(4, 0), textcoords="offset points", va="center",
                      fontsize=7, color=INK)
    ax_a.set_xlim(0, 15)
    ax_a.set_ylim(0, 102)
    ax_a.set_xticks(range(0, 14, 2))
    ax_a.set_xlabel("Snowball step (0 = seed list)", fontsize=8, color=INK)
    ax_a.set_ylabel("Cumulative share of bin discovered (%)", fontsize=8, color=INK)
    from matplotlib.lines import Line2D
    handles, _ = ax_a.get_legend_handles_labels()
    handles += [Line2D([], [], color=MUTED, lw=2, label="share of channels"),
                Line2D([], [], color=MUTED, lw=1.2, ls=(0, (3, 2)), label="share of weekly views")]
    leg = ax_a.legend(handles=handles, title="Subscribers" if bin_type == "subscribers" else "Weekly views",
                      fontsize=7, title_fontsize=7, frameon=False, loc="lower right")
    leg.get_title().set_color(MUTED)
    _style(ax_a)

    # b/c: channels first found by trending, per week (two panels, never a dual axis).
    tr = trending[trending.variant == variant].sort_values("week_start")
    tot_views = totals[(totals.variant == variant) & (totals.bin_type == bin_type)].weekly_views_total.sum()
    ax_b.bar(pd.to_datetime(tr.week_start), tr.n_channels, width=5, color=BLUE_4[2])
    ax_b.set_ylabel("New channels", fontsize=8, color=INK)
    ax_b.set_yscale("log")
    _style(ax_b)
    ax_c.bar(pd.to_datetime(tr.week_start), tr.weekly_views / tot_views * 100, width=5, color=BLUE_4[2])
    ax_c.set_ylabel("% of weekly views", fontsize=8, color=INK)
    _style(ax_c)
    ax_c.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b"))
    plt.setp(ax_b.get_xticklabels(), visible=False)

    ax_b.set_title("Channels first found by trending, per week", fontsize=7, color=MUTED, loc="left")
    for ax, letter in ((ax_a, "a"), (ax_b, "b"), (ax_c, "c")):
        ax.text(-0.3 if ax is not ax_a else -0.16, 1.0, letter, transform=ax.transAxes, fontsize=10,
                fontweight="bold", color=INK, va="bottom")
    title = VARIANT_TITLES.get(variant, variant)
    if cohort in COHORT_NOTES:
        title += "\n" + COHORT_NOTES[cohort]
    fig.suptitle(title, fontsize=8, color=MUTED, y=1.04)
    if path:
        fig.savefig(path, bbox_inches="tight")
    return fig
