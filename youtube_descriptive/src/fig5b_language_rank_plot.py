"""Plotting for Figure 5b (alternative Figure 5: language rank vs. engagement and population).

Shared by `scripts/render_fig5b_local.py`, so every render draws from the aggregate table written by
`19_fig5b_language_rank_databricks.py` joined to the CLDR speaker table.

Inputs (pandas DataFrames, the exported aggregates):
  languages: language (label), n_universe, n_measurable, n_positive, wv_sum, wv_median, wv_p90, wv_max, wv_top10
  speakers:  analysis_code, name, speakers, ...        (scripts/build_fig5b_speakers.py, Unicode CLDR)
  lmap:      analysis_code, name, labels, cldr_codes, combine   (data/fig5b_language_map.csv)

`build_table` pools labels into analysis languages (e.g. uzn + uzb -> uzb), joins speakers and derives:
  share_views     language share of all measurable weekly views (undetermined language included)
  index           representation index = (language share of views) / (language share of speakers), both
                  shares taken over the languages that have a speaker figure; 1 = views in proportion to
                  speakers, 2 = twice as many views as its speakers would imply
  channels_per_m  measurable channels per million speakers
  wv_per_channel  weekly views per measurable channel (mean)

Panels: a = speakers vs weekly views (log-log) with the views-proportional-to-speakers line, point size = channels;
b = paired dots, share of views vs share of speakers for the 18 largest languages (needs no anchor); c = channels
per million speakers vs weekly views per channel, point size = views, with lines of equal views per speaker
(index = 0.1, 1, 10). Colour is the continuous log index; the 1x anchor depends on the speaker total (9.52 billion,
overlapping L1+L2; with world population, 8.06 billion, every index scales by 0.847).
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import matplotlib.colorbar  # noqa: E402,F401
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

INK = "#1f1f1f"
MUTED = "#6b6b6b"
GRID = "#e6e6e3"
# Diverging colour for log10(index): teal (below 1x) - light grey (1x) - purple (above). Deliberately not the
# blue/orange of Figures 3-5, where those colours mean channels / views.
CMAP = LinearSegmentedColormap.from_list("fig5b_index", ["#0b7a75", "#e6e4dc", "#7b2d8e"])
NORM = TwoSlopeNorm(vcenter=0.0, vmin=-1.6, vmax=0.6)
LABEL_FS = 6.0
TOP_N = 50  # languages shown: the top N by weekly views that have a speaker figure

# Figure variants the renderer draws: (name, panels). Main figure first.
RENDER_SPECS = [("abc", "abc"), ("ab", "ab")]

# Short display names for the plot (the map CSV carries the full names).
SHORT = {"cmn": "Chinese", "hbs": "Serbo-Croatian", "fil": "Filipino", "zsm": "Malay", "pnb": "W. Punjabi",
         "swa": "Swahili", "pan": "Punjabi", "fas": "Persian", "nor": "Norwegian", "ory": "Odia",
         "mni": "Manipuri", "pcm": "Naija", "mlg": "Malagasy", "uzb": "Uzbek", "ara": "Arabic",
         "ron": "Romanian", "sqi": "Albanian", "kmr": "Kurdish (N)", "ckb": "Kurdish (C)"}


def _numeric(df: pd.DataFrame, skip=()) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        if c not in skip:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = df[c].astype(str)  # "nan" (Min Nan) is a label, not a missing value
    return df


def build_table(languages: pd.DataFrame, speakers: pd.DataFrame, lmap: pd.DataFrame) -> pd.DataFrame:
    """One row per analysis language (labels pooled), joined to speakers, with the derived measures."""
    lang = _numeric(languages, skip=("language",))
    label2code = {lab: r.analysis_code for r in lmap.itertuples() for lab in str(r.labels).split(",")}
    lang["analysis_code"] = lang.language.map(label2code).fillna(lang.language)
    sums = ["n_universe", "n_measurable", "n_positive", "wv_sum", "wv_top10"]
    g = lang.groupby("analysis_code")
    t = g[sums].sum()
    t["wv_max"] = g.wv_max.max()
    t["labels"] = g.language.apply(lambda s: "|".join(s))
    t = t.reset_index()
    sp = speakers[["analysis_code", "name", "speakers"]].copy()
    t = t.merge(sp, on="analysis_code", how="left")
    t["name"] = t["name"].fillna(t.analysis_code)
    t["short"] = t.analysis_code.map(SHORT).fillna(t["name"])
    total = t.wv_sum.sum()
    t["share_views"] = t.wv_sum / total
    known = t.speakers.fillna(0) > 0
    wv_cov, sp_cov = t.loc[known, "wv_sum"].sum(), t.loc[known, "speakers"].sum()
    t["share_speakers"] = t.speakers / sp_cov
    t["views_per_speaker"] = t.wv_sum / t.speakers
    t["index"] = (t.wv_sum / wv_cov) / t.share_speakers
    t["channels_per_m"] = t.n_measurable / (t.speakers / 1e6)
    t["wv_per_channel"] = t.wv_sum / t.n_measurable.replace(0, np.nan)
    t["top10_share"] = t.wv_top10 / t.wv_sum.replace(0, np.nan)
    t["avg_views_per_speaker"] = wv_cov / sp_cov
    t = t[t.analysis_code != "und"].sort_values("wv_sum", ascending=False).reset_index(drop=True)
    t["rank_views"] = np.arange(1, len(t) + 1)
    return t


def summary(table: pd.DataFrame, languages: pd.DataFrame, status: pd.DataFrame) -> pd.DataFrame:
    """Headline numbers quoted in the README."""
    lang = _numeric(languages, skip=("language",))
    status = _numeric(status, skip=("status",))
    meas = lang.wv_sum.sum()
    und = lang.loc[lang.language == "und"]
    known = table[table.speakers.fillna(0) > 0]
    top = known.head(TOP_N)
    rows = [
        ("measurable_channels", status.loc[status.status.isin(["zero", "positive"]), "n_channels"].sum()),
        ("measurable_weekly_views", meas),
        ("und_share_of_views", und.wv_sum.sum() / meas),
        ("und_share_of_measurable_channels", und.n_measurable.sum() / lang.n_measurable.sum()),
        ("n_analysis_languages_with_views", int((table.wv_sum > 0).sum())),
        ("n_languages_with_speaker_figure", len(known)),
        ("views_coverage_with_speaker_figure", known.wv_sum.sum() / meas),
        ("views_coverage_excl_und", known.wv_sum.sum() / (meas - und.wv_sum.sum())),
        ("avg_weekly_views_per_speaker", float(table.avg_views_per_speaker.iloc[0])),
        ("n_languages_plotted", len(top)),
        ("plotted_share_of_views", top.wv_sum.sum() / meas),
    ]
    for label, d in (("all_with_speakers", known), (f"top{TOP_N}", top)):
        lx, ly = np.log10(d.speakers), np.log10(d.wv_sum.clip(lower=1))
        rows.append((f"pearson_log_speakers_log_views_{label}", lx.corr(ly)))
        rows.append((f"spearman_speakers_views_{label}", d.speakers.corr(d.wv_sum, method="spearman")))
        rows.append((f"log_log_slope_{label}", float(np.polyfit(lx, ly, 1)[0])))
    return pd.DataFrame(rows, columns=["metric", "value"])


def _fmt_count(v, _pos=None) -> str:
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "k")):
        if v >= div:
            return f"{v / div:g}{suf}"
    return f"{v:g}"


def _spines(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
        ax.spines[s].set_linewidth(0.6)
    ax.tick_params(colors=INK, labelsize=7, width=0.6, length=3)
    ax.tick_params(which="minor", width=0.4, length=1.5, colors=MUTED)


def _log_axis(ax, axis: str):
    a = ax.xaxis if axis == "x" else ax.yaxis
    (ax.set_xscale if axis == "x" else ax.set_yscale)("log")
    a.set_major_locator(LogLocator(base=10, numticks=12))
    a.set_major_formatter(FuncFormatter(_fmt_count))
    a.set_minor_formatter(NullFormatter())


def _rgba(index) -> np.ndarray:
    """Continuous diverging colour by log10 of the representation index (mid = 1x)."""
    li = np.clip(np.log10(np.asarray(index, dtype=float)), NORM.vmin, NORM.vmax)
    return CMAP(NORM(li))


def size_channels(n):
    """Marker area (pt^2) for panel a: log scale in measurable channels (10k -> 5, 100k -> 14, 1M -> 23)."""
    return np.clip(5 + 9 * (np.log10(np.asarray(n, dtype=float)) - 4), 4, None)


def size_views(v):
    """Marker area (pt^2) for panel c: log scale in weekly views (1B -> 9.5, 10B -> 18.5, 100B -> 27.5)."""
    return np.clip(5 + 9 * (np.log10(np.asarray(v, dtype=float)) - 8.5), 4, None)


def _note(ax, text, xy, xytext, ha="left", va="center", leader=True, fs=LABEL_FS, color=INK):
    """Label at a data-coordinate position, with a thin leader line to the point."""
    ax.annotate(text, xy, xytext=xytext, textcoords="data", ha=ha, va=va, fontsize=fs, color=color,
                annotation_clip=False,
                arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.4, shrinkA=1.5, shrinkB=2.5) if leader else None)


# Label positions: code -> (x, y, ha, va, leader), in data coordinates (panel a: speakers, weekly views;
# panel c: channels per million, weekly views per channel). Hand-placed; the data are fixed for a given run.
A_LABELS = {
    "eng": (1.7e9, 1.3e12, "center", "bottom", False),
    "spa": (2.6e9, 1.7e11, "right", "center", True),
    "hin": (3.5e9, 9.5e10, "right", "center", True),
    "ara": (3.5e9, 5.5e10, "right", "center", True),
    "por": (3.5e9, 3.4e10, "right", "center", True),
    "ind": (1.2e7, 2.2e11, "left", "center", True),
    "kor": (1.2e7, 7.5e10, "left", "center", True),
    "jpn": (1.2e8, 6.0e10, "center", "center", True),
    "tur": (1.2e7, 3.6e10, "left", "center", True),
    "tha": (1.2e7, 1.6e10, "left", "center", True),
    "urd": (2.6e8, 2.8e9, "right", "center", False),
    "swa": (2.5e8, 4.3e8, "left", "center", False),
}
C_LABELS = {
    "kor": (2.75e3, 6.0e5, "left", "center", True),
    "ind": (2.75e3, 4.4e5, "left", "center", True),
    "eng": (2.75e3, 2.7e5, "left", "center", True),
    "tha": (2.75e3, 1.7e5, "left", "center", True),
    "tur": (7.0e2, 7.4e5, "center", "bottom", True),
    "jpn": (3.3e2, 6.2e5, "center", "bottom", True),
    "cmn": (4.0e1, 3.07e5, "left", "center", False),
    "urd": (7.0e1, 1.14e5, "right", "center", False),
    "swa": (2.8e1, 8.7e4, "left", "center", False),
}
PANEL_B_N = 18


def _strip(fig, panels):
    """Colour bar and point-size legends in a strip under the panels."""
    cax = fig.add_axes([0.07, 0.085, 0.2, 0.02])
    cb = matplotlib.colorbar.ColorbarBase(cax, cmap=CMAP, norm=NORM, orientation="horizontal")
    cb.set_ticks([-1, 0, 0.5])
    cb.set_ticklabels(["0.1×", "1×", "3×"])
    cb.ax.tick_params(labelsize=LABEL_FS, width=0.5, length=2)
    cb.outline.set_visible(False)
    cax.set_title("Colour: views share ÷ speaker share (log scale)", fontsize=LABEL_FS, color=INK, pad=3,
                  loc="left")

    def handles(sizes):
        return [Line2D([], [], marker="o", ls="", mfc=MUTED, mec=INK, mew=0.3, ms=np.sqrt(s)) for s in sizes]

    if "a" in panels:
        fig.legend(handles(size_channels([1e4, 1e5, 1e6])), ["10k", "100k", "1M"], title="Size in a: channels", ncol=3,
                   fontsize=LABEL_FS, title_fontsize=LABEL_FS, frameon=False, loc="lower left",
                   bbox_to_anchor=(0.34, 0.0), handletextpad=0.2, columnspacing=1.0)
    if "c" in panels:
        fig.legend(handles(size_views([1e9, 1e10, 1e11])), ["1B", "10B", "100B"], title="Size in c: weekly views",
                   ncol=3, fontsize=LABEL_FS, title_fontsize=LABEL_FS, frameon=False, loc="lower left",
                   bbox_to_anchor=(0.56, 0.0), handletextpad=0.2, columnspacing=1.0)
    ring = Line2D([], [], marker="o", ls="", mfc="none", mec=INK, mew=0.9, ms=5)
    fig.legend([ring], ["Hollow marker, Chinese*:\nYouTube blocked in\nmainland China"], fontsize=LABEL_FS, frameon=False,
               loc="lower left", bbox_to_anchor=(0.77, 0.0), handletextpad=0.2)


def plot_fig5b(table: pd.DataFrame, panels: str = "abc", path=None):
    t = table[table.speakers.fillna(0) > 0].head(TOP_N).copy()
    fc = _rgba(t["index"])
    ec = np.tile(matplotlib.colors.to_rgba(INK), (len(t), 1))
    lw = np.full(len(t), 0.3)
    china = (t.analysis_code == "cmn").to_numpy()
    fc[china] = (0, 0, 0, 0)  # hollow: YouTube is blocked in mainland China
    lw[china] = 0.9
    avg = float(table.avg_views_per_speaker.iloc[0])
    ncols = len(panels)
    fig, axes = plt.subplots(1, ncols, figsize=(7.2 if ncols == 3 else 5.0, 3.0),
                             gridspec_kw={"wspace": 0.62 if ncols == 3 else 0.5})
    fig.subplots_adjust(left=0.07, right=0.9 if ncols == 3 else 0.97, bottom=0.3, top=0.93)
    axes = dict(zip(panels, np.atleast_1d(axes)))

    # a: speakers vs weekly views, one point per language, with the views-proportional-to-speakers line.
    if "a" in axes:
        ax = axes["a"]
        xs = np.array([t.speakers.min() / 2, t.speakers.max() * 2])
        ax.plot(xs, avg * xs, color=MUTED, lw=0.8, ls=(0, (3, 2)), zorder=1)
        ax.scatter(t.speakers, t.wv_sum, s=size_channels(t.n_measurable), facecolors=fc, edgecolors=ec,
                   linewidths=lw, zorder=3)
        for r in t[t.analysis_code.isin(A_LABELS)].itertuples():
            x, y, ha, va, lead = A_LABELS[r.analysis_code]
            _note(ax, r.short, (r.speakers, r.wv_sum), (x, y), ha, va, lead)
        cn = t[china].iloc[0]
        _note(ax, "Chinese\n(YouTube\nblocked in\nmainland China)", (cn.speakers, cn.wv_sum), (3.7e9, 5.0e9), "right",
              "top", True)
        _log_axis(ax, "x")
        _log_axis(ax, "y")
        ax.set_xlim(t.speakers.min() / 1.8, t.speakers.max() * 2.2)
        ax.set_ylim(t.wv_sum.min() / 2, t.wv_sum.max() * 3)
        ax.text(0.03, 0.97, "Dashed: views proportional\nto speakers", transform=ax.transAxes, fontsize=LABEL_FS,
                color=MUTED, ha="left", va="top")
        _spines(ax)
        ax.grid(True, color=GRID, linewidth=0.5, zorder=0)
        ax.set_axisbelow(True)
        ax.set_xlabel("Speakers (CLDR, L1 + L2)", fontsize=8, color=INK)
        ax.set_ylabel("Weekly views", fontsize=8, color=INK)

    # b: paired dots, share of views vs share of speakers for the largest languages (no anchor needed).
    if "b" in axes:
        ax = axes["b"]
        d = t.head(PANEL_B_N)
        y = np.arange(len(d))
        ax.hlines(y, d.share_speakers, d.share_views, color=_rgba(d["index"]), lw=1.6, zorder=2)
        ax.scatter(d.share_speakers, y, s=14, facecolors="white", edgecolors=INK, linewidths=0.8, zorder=3,
                   label="Speakers")
        ax.scatter(d.share_views, y, s=14, facecolors=INK, edgecolors=INK, linewidths=0.8, zorder=3,
                   label="Views")
        ax.set_xscale("log")
        ax.set_xlim(0.0045, 0.8)
        ax.set_xticks([0.01, 0.1, 0.5])
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0%}"))
        ax.xaxis.set_minor_formatter(NullFormatter())
        ax.set_yticks(y)
        ax.set_yticklabels([("Chinese*" if r.analysis_code == "cmn" else r.short) for r in d.itertuples()],
                           fontsize=LABEL_FS)
        ax.set_ylim(len(d) - 0.4, -0.8)
        ax.legend(fontsize=LABEL_FS, frameon=False, loc="upper left", handletextpad=0.1, borderaxespad=0.1,
                  labelspacing=0.3)
        _spines(ax)
        ax.tick_params(axis="y", length=0, labelsize=LABEL_FS + 0.5)
        ax.grid(True, axis="x", color=GRID, linewidth=0.5, zorder=0)
        ax.set_axisbelow(True)
        ax.set_xlabel("Share (of languages with speaker data)", fontsize=8, color=INK)

    # c: channels per million speakers vs weekly views per channel, with lines of equal views per speaker.
    if "c" in axes:
        ax = axes["c"]
        x0, x1 = t.channels_per_m.min() / 1.8, t.channels_per_m.max() * 1.8
        xs = np.array([x0, x1])
        for k, ls in ((0.1, ":"), (1, (0, (3, 2))), (10, ":")):
            ax.plot(xs, k * avg * 1e6 / xs, color=MUTED, lw=0.8 if k == 1 else 0.6, ls=ls, zorder=1)
        ax.scatter(t.channels_per_m, t.wv_per_channel, s=size_views(t.wv_sum), facecolors=fc, edgecolors=ec,
                   linewidths=lw, zorder=3)
        for r in t[t.analysis_code.isin(C_LABELS)].itertuples():
            x, y_, ha, va, lead = C_LABELS[r.analysis_code]
            _note(ax, r.short, (r.channels_per_m, r.wv_per_channel), (x, y_), ha, va, lead)
        guides = {0.1: (3.3e2, "left", 1.12), 1: (2.35e3, "right", 1.1), 10: (2.35e3, "right", 1.1)}
        for k, (gx, ha, dy) in guides.items():
            ax.text(gx, k * avg * 1e6 / gx * dy, f"{k:g}×", fontsize=LABEL_FS, color=MUTED, ha=ha, va="bottom")
        _log_axis(ax, "x")
        _log_axis(ax, "y")
        ax.set_xlim(x0, x1)
        ax.set_ylim(t.wv_per_channel.min() / 1.8, t.wv_per_channel.max() * 2)
        ax.set_yticks([50_000, 100_000, 200_000, 400_000])
        ax.yaxis.set_minor_formatter(NullFormatter())
        _spines(ax)
        ax.grid(True, color=GRID, linewidth=0.5, zorder=0)
        ax.set_axisbelow(True)
        ax.set_xlabel("Channels per million speakers", fontsize=8, color=INK)
        ax.set_ylabel("Weekly views per channel", fontsize=8, color=INK)

    for k, ax in axes.items():
        ax.text(-0.22 if k != "b" else -0.42, 1.06, k, transform=ax.transAxes, fontsize=11, fontweight="bold",
                color=INK)
    _strip(fig, panels)
    if path:
        fig.savefig(path, bbox_inches="tight", dpi=300)
        plt.close(fig)
    return fig
