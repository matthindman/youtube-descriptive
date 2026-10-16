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

Panels: a = speakers vs weekly views (log-log) with the views-proportional-to-speakers line; b = representation
index by language rank; c = channels per million speakers vs weekly views per channel, with lines of equal
views per speaker (index = 0.1, 1, 10), which splits the index into "more channels" and "more views per channel".
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
CHANNELS = "#2a78d6"  # under-represented
VIEWS = "#eb6834"  # over-represented
NEUTRAL = "#9a9a95"
TOP_N = 50  # languages shown: the top N by weekly views that have a speaker figure
OVER, UNDER = 2.0, 0.5  # index thresholds for colouring

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
            x = v / div
            return f"{x:g}{suf}"
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


def _colors(index: pd.Series) -> np.ndarray:
    return np.where(index >= OVER, VIEWS, np.where(index <= UNDER, CHANNELS, NEUTRAL))


def _label(ax, x, y, text, dx=0.0, dy=0.0, ha="left", fs=5.8, **kw):
    ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", ha=ha, va="center", fontsize=fs,
                color=INK, **kw)


# Hand-tuned labels: code -> (dx, dy, ha), in points.
A_LABELS = {"eng": (-4, -7, "right"), "spa": (-3, 5, "right"), "hin": (5, -2, "left"), "ind": (-4, 3, "right"),
            "ara": (5, -3, "left"), "por": (0, -8, "center"), "kor": (-4, 3, "right"),
            "tur": (-4, -2, "right"),
            "cmn": (4, 4, "left"), "urd": (4, 3, "left"), "swa": (4, 3, "left")}
C_LABELS = {"eng": (4, -6, "left"), "kor": (4, 3, "left"), "tur": (4, 3, "left"), "cmn": (4, 3, "left"),
            "urd": (4, 3, "left"), "swa": (4, 3, "left")}
B_LABELS = {"cmn": (-2, -6, "right"), "urd": (4, -6, "left"), "amh": (-3, -3, "right"),
            "swa": (-3, -3, "right")}


def plot_fig5b(table: pd.DataFrame, panels: str = "abc", path=None):
    t = table[table.speakers.fillna(0) > 0].head(TOP_N).copy()
    t["color"] = _colors(t["index"])
    avg = float(table.avg_views_per_speaker.iloc[0])
    ncols = len(panels)
    fig, axes = plt.subplots(1, ncols, figsize=(7.2 if ncols == 3 else 5.0, 2.55),
                             gridspec_kw={"wspace": 0.62 if ncols == 3 else 0.5})
    axes = dict(zip(panels, np.atleast_1d(axes)))

    # a: speakers vs weekly views, one point per language, with the views-proportional-to-speakers line.
    if "a" in axes:
        ax = axes["a"]
        xs = np.array([t.speakers.min() / 2, t.speakers.max() * 2])
        ax.plot(xs, avg * xs, color=MUTED, lw=0.8, ls=(0, (3, 2)), zorder=1)
        ax.scatter(t.speakers, t.wv_sum, s=np.clip(np.sqrt(t.n_measurable) / 6, 4, 30), c=t.color, lw=0,
                   alpha=0.9, zorder=3)
        for r in t[t.analysis_code.isin(A_LABELS)].itertuples():
            dx, dy, ha = A_LABELS[r.analysis_code]
            _label(ax, r.speakers, r.wv_sum, r.short, dx, dy, ha)
        _log_axis(ax, "x")
        _log_axis(ax, "y")
        ax.set_xlim(t.speakers.min() / 1.8, t.speakers.max() * 2.2)
        ax.set_ylim(t.wv_sum.min() / 2, t.wv_sum.max() * 3)
        ax.text(0.03, 0.97, "Dashed: views proportional\nto speakers", transform=ax.transAxes, fontsize=5.8,
                color=MUTED, ha="left", va="top")
        _spines(ax)
        ax.grid(True, color=GRID, linewidth=0.5, zorder=0)
        ax.set_axisbelow(True)
        ax.set_xlabel("Speakers (CLDR, L1 + L2)", fontsize=8, color=INK)
        ax.set_ylabel("Weekly views", fontsize=8, color=INK)

    # b: representation index by language rank (rank 1 = most weekly views).
    if "b" in axes:
        ax = axes["b"]
        ax.axhline(1, color=MUTED, lw=0.8, ls=(0, (3, 2)), zorder=1)
        ax.vlines(t.rank_views, 1, t["index"], color=t.color, lw=0.9, zorder=2)
        ax.scatter(t.rank_views, t["index"], s=7, c=t.color, lw=0, zorder=3)
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(LogLocator(base=10, numticks=12))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:g}×"))
        ax.yaxis.set_minor_formatter(NullFormatter())
        for r in t[t.analysis_code.isin(B_LABELS)].itertuples():
            dx, dy, ha = B_LABELS[r.analysis_code]
            _label(ax, r.rank_views, r.index, r.short, dx, dy, ha, fs=5.6)
        over = t[t["index"] >= OVER].nlargest(4, "index")
        ax.text(0.97, 0.97, "Most over-represented:\n" + "\n".join(
            f"{r.short} {r.index:.1f}×" for r in over.itertuples()), transform=ax.transAxes,
            fontsize=5.8, color=INK, ha="right", va="top", linespacing=1.25)
        ax.set_xlim(0, TOP_N + 4)
        lo, hi = t["index"].min(), t["index"].max()
        ax.set_ylim(lo / 2.2, hi * 4.5)
        _spines(ax)
        ax.grid(True, axis="y", color=GRID, linewidth=0.5, zorder=0)
        ax.set_axisbelow(True)
        ax.set_xlabel("Language rank by weekly views", fontsize=8, color=INK)
        ax.set_ylabel("Views share ÷ speaker share", fontsize=8, color=INK)

    # c: channels per million speakers vs weekly views per channel, with lines of equal views per speaker.
    if "c" in axes:
        ax = axes["c"]
        x0, x1 = t.channels_per_m.min() / 1.8, t.channels_per_m.max() * 1.8
        xs = np.array([x0, x1])
        for k, ls in ((0.1, ":"), (1, (0, (3, 2))), (10, ":")):
            ax.plot(xs, k * avg * 1e6 / xs, color=MUTED, lw=0.8 if k == 1 else 0.6, ls=ls, zorder=1)
        ax.scatter(t.channels_per_m, t.wv_per_channel, s=np.clip(np.sqrt(t.wv_sum) / 3000, 4, 30), c=t.color,
                   lw=0, alpha=0.9, zorder=3)
        for r in t[t.analysis_code.isin(C_LABELS)].itertuples():
            dx, dy, ha = C_LABELS[r.analysis_code]
            _label(ax, r.channels_per_m, r.wv_per_channel, r.short, dx, dy, ha, fs=5.6)
        ytop = t.wv_per_channel.max() * 2
        for k in (0.1, 1, 10):
            yy = ytop * 0.8
            xx = k * avg * 1e6 / yy
            if x0 < xx < x1:
                ax.text(xx, yy, f"{k:g}×", fontsize=5.8, color=MUTED, ha="right", va="bottom")
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
        ax.text(-0.22, 1.06, k, transform=ax.transAxes, fontsize=11, fontweight="bold", color=INK)
    if path:
        fig.savefig(path, bbox_inches="tight", dpi=300)
        plt.close(fig)
    return fig
