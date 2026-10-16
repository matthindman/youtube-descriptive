"""Plotting for the language-drift Extended Data panel (ED Fig. 4f candidate).

Weekly share of views by language, mid-June to mid-October 2026, for the >=10k universe.
Input: the long table written by `scripts/render_lang_drift_local.py`
(week_end, language, n_channels, n_below_hwm, wv), where `wv` is each channel's weekly gain above
its prior high-water mark, normalised to 7 days and summed by language (see README_lang_drift.md).
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

INK, MUTED, GRID = "#1f1f1f", "#6b6b6b", "#e6e6e3"
UP, DOWN, CONTEXT = "#7a3b9e", "#1d7f79", "#b5b5b0"
NAMES = {"eng": "English", "spa": "Spanish", "hin": "Hindi", "ind": "Indonesian", "ara": "Arabic",
         "por": "Portuguese", "kor": "Korean", "jpn": "Japanese", "rus": "Russian", "tur": "Turkish",
         "vie": "Vietnamese", "ben": "Bengali", "cmn": "Chinese", "tha": "Thai"}
ARTEFACT_WEEKS = ["2026-08-17", "2026-08-24", "2026-08-31"]   # stall + catch-up, excluded from baselines
THRESH = 7.5   # % relative change highlighted as drift


def plot_lang_drift(weekly: pd.DataFrame):
    """Return (figure, summary DataFrame of June vs late Sep-Oct shares and relative change)."""
    d = weekly.copy()
    d["wv"] = pd.to_numeric(d["wv"])
    d["week_end"] = d["week_end"].astype(str)
    sh = d.pivot_table(index="week_end", columns="language", values="wv", aggfunc="sum")
    sh = sh.div(sh.sum(axis=1), axis=0) * 100
    sh = sh[sh.index >= "2026-06-15"]          # first week has no prior high-water mark
    sh.index = pd.to_datetime(sh.index)
    top = [c for c in d.groupby("language").wv.sum().sort_values(ascending=False).index if c != "und"][:13]

    clean = sh.drop(index=pd.to_datetime(ARTEFACT_WEEKS), errors="ignore")
    first4, last4 = clean.iloc[:4][top].mean(), clean.iloc[-4:][top].mean()
    rel = 100 * (last4 - first4) / first4
    idx = sh[top].div(first4) * 100
    smooth = idx.rolling(3, center=True, min_periods=2).mean()

    fig, (ax, bx) = plt.subplots(1, 2, figsize=(7.2, 3.0), gridspec_kw={"width_ratios": [1.9, 1], "wspace": 0.55})

    # a: share of weekly views, indexed to each language's June level (3-week centred mean).
    for lo, hi in [("2026-08-11", "2026-09-01"), ("2026-09-01", "2026-09-15")]:
        ax.axvspan(pd.Timestamp(lo), pd.Timestamp(hi), color="#f0efec", lw=0, zorder=0)
    ax.text(pd.Timestamp("2026-08-26"), 128.5, "refresh\nartefacts", fontsize=5.8, color=MUTED, ha="left", va="top")
    ax.axvline(pd.Timestamp("2026-08-24"), color=MUTED, lw=0.7, ls=(0, (3, 2)), zorder=1)
    ax.text(pd.Timestamp("2026-08-23"), 76.5, "view-counting\nchange (Aug 24)", fontsize=5.8, color=MUTED, ha="right", va="bottom")
    ax.axhline(100, color=MUTED, lw=0.6, zorder=1)

    labels = []
    for lang in top:
        r = rel[lang]
        color = UP if r > THRESH else DOWN if r < -THRESH else CONTEXT
        hl = color != CONTEXT
        ax.plot(idx.index, idx[lang], color=color, lw=0.6, alpha=0.25 if hl else 0, zorder=2)
        ax.plot(smooth.index, smooth[lang], color=color, lw=2.0 if hl else 0.9, zorder=4 if hl else 3,
                solid_capstyle="round")
        if hl:
            labels.append([smooth[lang].iloc[-1], NAMES[lang], color])

    # direct labels at the right end, nudged apart
    labels.sort(key=lambda t: t[0])
    min_gap = 3.3
    for i in range(1, len(labels)):
        labels[i][0] = max(labels[i][0], labels[i - 1][0] + min_gap)
    x_end = smooth.index[-1] + pd.Timedelta(days=3)
    for y, text, color in labels:
        ax.text(x_end, y, text, fontsize=6.3, color=INK, va="center")
        ax.plot([smooth.index[-1] + pd.Timedelta(days=0.5), x_end - pd.Timedelta(days=0.5)],
                [y, y], color=color, lw=0.6)

    ax.set_xlim(sh.index[0] - pd.Timedelta(days=3), sh.index[-1] + pd.Timedelta(days=22))
    ax.set_ylim(75, 130)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.set_ylabel("Share of weekly views\n(June level = 100)", fontsize=7.5, color=INK)
    ax.set_xlabel("Week ending", fontsize=7.5, color=INK)

    # b: change from June to late Sep-Oct in share of weekly views (first vs last four clean weeks).
    order = rel.sort_values()
    colors = [UP if v > THRESH else DOWN if v < -THRESH else CONTEXT for v in order]
    y = np.arange(len(order))
    bx.barh(y, order.values, color=colors, height=0.62, lw=0)
    bx.axvline(0, color=MUTED, lw=0.6)
    for yi, (lang, v) in zip(y, order.items()):
        bx.text(v + (0.8 if v >= 0 else -0.8), yi, f"{v:+.0f}%", fontsize=6, color=INK,
                va="center", ha="left" if v >= 0 else "right")
    bx.set_yticks(y, [f"{NAMES[l]} ({first4[l]:.1f}%)" for l in order.index])
    bx.set_xlim(-24, 34)
    bx.set_xlabel("Change in share, Jun → Sep–Oct", fontsize=7.5, color=INK)

    for a in (ax, bx):
        for s in ("top", "right"):
            a.spines[s].set_visible(False)
        for s in ("left", "bottom"):
            a.spines[s].set_color(MUTED)
            a.spines[s].set_linewidth(0.6)
        a.tick_params(colors=INK, labelsize=6.5, width=0.6, length=3)
    bx.tick_params(axis="y", length=0)
    ax.grid(True, axis="y", color=GRID, lw=0.5, zorder=0)
    bx.grid(True, axis="x", color=GRID, lw=0.5, zorder=0)
    for k, a in (("a", ax), ("b", bx)):
        a.text(-0.16 if k == "a" else -0.62, 1.04, k, transform=a.transAxes, fontsize=11, fontweight="bold", color=INK)

    summary = pd.DataFrame({"share_june": first4, "share_late": last4, "rel_change_pct": rel}).round(3)
    return fig, summary
