# Databricks notebook source
# MAGIC %md
# MAGIC # Attention on YouTube  -  manuscript analysis & figures
# MAGIC ### Work product of **Claude (Anthropic Opus 4.8)**  -  `10_attention_manuscript_analysis_CLAUDE`
# MAGIC
# MAGIC > **Authorship marker.** Authored by Claude. A parallel notebook from a different model exists for
# MAGIC > comparison; every artifact carries `author="claude"` and the `_CLAUDE` filename stem.
# MAGIC
# MAGIC Produces the core analysis, robustness checks, results tables, and publication-grade figures for
# MAGIC Paper 1 *"Attention on YouTube"*, to run on `research-compute` (DBR 17.3).
# MAGIC
# MAGIC #### Design contract
# MAGIC 1. **All heavy compute stays in Databricks.** Only *aggregates* and rendered figures leave the
# MAGIC    workspace. Each exported aggregate is also persisted as a Delta table for inspection/re-use; the
# MAGIC    export helper refuses oversized frames and applies count-column small-cell suppression (only small
# MAGIC    *positive* counts `0 < n < min_cell_count`; NaN/zero structural rows are kept). No full
# MAGIC    channel-/video-level frame is pulled to the driver. **One documented carve-out:** the Fig 2 treemap
# MAGIC    source (`fig2_treemap_cells`) lists individual *named, public, high-attention head channels* (those
# MAGIC    above `treemap_label_view_share`; the rest are pooled) - intentional manuscript source data, not a
# MAGIC    bulk dump. Set `treemap_anonymize_labels=true` to replace those names with hashed IDs.
# MAGIC 2. **Safe by default.** Starts in **`manifest_only`** (no scans). Set `execution_mode` to `smoke`,
# MAGIC    or `core`/`full` (requires `confirm_expensive=true`).
# MAGIC 3. **Attention = weekly views**, the **elapsed-day-normalized** change in lifetime channel views
# MAGIC    between two snapshots: `(views_t - views_{t-k}) / k x 7`. Panel = `dev_sean.default.yt_channel_stats_full`.
# MAGIC    The current **anchor** snapshot is chosen by a **completeness rule** (latest partition whose row
# MAGIC    count >= `anchor_min_fraction_of_max_partition` x the largest partition) so a half-written partition
# MAGIC    is not silently used. Universe = channels >= 10,000 subscribers ("Top of the Ocean").
# MAGIC 4. **Recompute from silver; diagnostics are cross-checks** (`dev_sean.diagnostics`/`validation`).
# MAGIC
# MAGIC #### Figure standards
# MAGIC Nature/Science spec: 90/180 mm widths, 600-DPI PNG + vector PDF, sans-serif 5-7 pt, 8-pt bold panel
# MAGIC letters, Okabe-Ito colourblind-safe palette, cividis/viridis sequential maps.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Parameters, environment, and reproducibility manifest

# COMMAND ----------
import os
import re
import json
import math
import hashlib
import platform
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    from pyspark.sql import functions as F
    from pyspark.sql import Window
    from pyspark.sql import DataFrame as SparkDataFrame
    _SPARK_AVAILABLE = True
except Exception:  # pragma: no cover
    _SPARK_AVAILABLE = False
    SparkDataFrame = object  # type: ignore

AUTHOR = "claude"
NOTEBOOK_NAME = "10_attention_manuscript_analysis_CLAUDE"


def _in_databricks() -> bool:
    return "dbutils" in globals()

# COMMAND ----------
def _create_text_widget(name: str, default: str, label: Optional[str] = None) -> None:
    try:
        dbutils.widgets.text(name, default, label or name)  # noqa: F821
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)  # noqa: F821
        return value if value not in (None, "") else default
    except Exception:
        return os.environ.get(name.upper(), default)


def _get_bool_widget(name: str, default: bool) -> bool:
    return _get_widget(name, str(default)).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _get_int_widget(name: str, default: int) -> int:
    raw = _get_widget(name, str(default)).strip()
    return int(raw) if raw else default


def _get_float_widget(name: str, default: float) -> float:
    raw = _get_widget(name, str(default)).strip()
    return float(raw) if raw else default


# ---- Run control ------------------------------------------------------------------------------------
_create_text_widget("execution_mode", "manifest_only")   # manifest_only | smoke | core | full
_create_text_widget("confirm_expensive", "false")
_create_text_widget("smoke_channel_limit", "50000")
_create_text_widget("random_seed", "20260529")
_create_text_widget("shuffle_partitions", "800")

# ---- Source tables ----------------------------------------------------------------------------------
_create_text_widget("source_catalog", "prod_tads")
_create_text_widget("source_schema", "youtube_too")
_create_text_widget("channels_dim_table", "yt_sl_channels")
_create_text_widget("videos_dim_table", "yt_sl_videos")
_create_text_widget("videos_metrics_table", "yt_sl_videos_metrics")

# Weekly-views snapshot panel (current Sunday TOO job).
_create_text_widget("channel_panel_fqtn", "dev_sean.default.yt_channel_stats_full")
_create_text_widget("channel_key_column", "")  # blank => auto-detect channel_id / canonical_id

_create_text_widget("language_catalog", "dev_sean")
_create_text_widget("language_schema", "matt")
_create_text_widget("language_channels_table", "yt_lid_v3_channels")
_create_text_widget("language_run_id", "default")  # validated run_id; blank => most recent

# Category: native YouTube topics in backfill_channels.topic_categories (array); subsample_items secondary.
_create_text_widget("category_catalog", "")
_create_text_widget("category_schema", "")
_create_text_widget("category_table", "")
_create_text_widget("category_column", "")
_create_text_widget("category_level", "")
_create_text_widget("channel_created_column", "")  # explicit YouTube publishedAt only; auto-detect is strict

# Diagnostics / validation cross-checks (NOT authoritative).
_create_text_widget("diagnostics_catalog", "dev_sean")
_create_text_widget("diagnostics_schema", "diagnostics")
_create_text_widget("suspicion_flags_table", "too_suspicion_flags")
_create_text_widget("run_summary_table", "too_run_summary")

# ---- Analysis parameters ----------------------------------------------------------------------------
_create_text_widget("too_subscriber_floor", "10000")
_create_text_widget("snapshot_current_date", "")          # blank => completeness-guarded auto-selection
_create_text_widget("snapshot_prior_date", "")            # blank => snapshot nearest (current - target window)
_create_text_widget("anchor_selection_mode", "auto_complete")   # auto_complete | latest | manual
_create_text_widget("anchor_min_fraction_of_max_partition", "0.90")
_create_text_widget("allow_incomplete_prior", "false")  # core/full refuse an incomplete prior unless true
_create_text_widget("allow_incomplete_anchor", "false")  # core/full refuse a partial current anchor unless true
_create_text_widget("target_window_days", "7")
# Optional multi-window cascade for an immature panel: if a channel has no measure at the primary
# (target) window, fall back to these longer complete-prior windows, recording per channel which window
# was used. Blank (default) = single-window only (strict). Set e.g. "14,28" to enable the cascade.
_create_text_widget("attention_fallback_windows_days", "")
_create_text_widget("n_traffic_blocks", "20")
_create_text_widget("subscriber_thresholds", "1000,10000,100000,1000000")
_create_text_widget("shorts_max_seconds", "180")
_create_text_widget("shorts_change_date", "2025-03-31")
_create_text_widget("production_window_weeks", "4")
_create_text_widget("format_lookback_days", "90")
_create_text_widget("lookback_capture_targets", "0.5,0.8,0.9,0.95")
_create_text_widget("negative_delta_policy", "floor_zero")  # floor_zero (neg->0) | drop (neg->null) | keep (raw); all retain the row, null prior stays null
_create_text_widget("min_valid_language_segments", "3")
_create_text_widget("treemap_label_view_share", "0.02")
_create_text_widget("treemap_anonymize_labels", "false")  # replace named head channels with hashed IDs in the treemap export
_create_text_widget("treemap_top_languages", "8")
_create_text_widget("treemap_top_categories", "6")
_create_text_widget("lorenz_value_bins", "200")
_create_text_widget("block_value_bins", "4000")           # resolution for binned traffic-block assignment

# ---- Output / safety --------------------------------------------------------------------------------
_create_text_widget("output_catalog", "dev_sean")
_create_text_widget("output_schema", "matt")
_create_text_widget("output_table_prefix", "claude_yt_attention")
_create_text_widget("export_root", "/Volumes/dev_sean/matt/models")
_create_text_widget("export_subdir", "")
_create_text_widget("local_fig_dir", "figs")
_create_text_widget("max_export_rows", "200000")
_create_text_widget("min_cell_count", "5")
_create_text_widget("write_outputs", "true")
_create_text_widget("write_delta_aggregates", "true")
_create_text_widget("make_figures", "true")          # render the figure plots
_create_text_widget("make_source_tables", "true")    # compute/export main-figure source-data tables
_create_text_widget("run_robustness", "true")  # ED + robustness TABLES; independent of make_figures
_create_text_widget("fail_on_missing_outputs", "true")  # core/full: raise if any figure/step failed

# COMMAND ----------
EXECUTION_MODE = _get_widget("execution_mode", "manifest_only").strip().lower()
CONFIRM_EXPENSIVE = _get_bool_widget("confirm_expensive", False)
SMOKE_LIMIT = _get_int_widget("smoke_channel_limit", 50000)
SEED = _get_int_widget("random_seed", 20260529)
SHUFFLE_PARTITIONS = _get_int_widget("shuffle_partitions", 800)

if EXECUTION_MODE not in {"manifest_only", "smoke", "core", "full"}:
    raise ValueError("execution_mode must be one of manifest_only|smoke|core|full")
if EXECUTION_MODE in {"core", "full"} and not CONFIRM_EXPENSIVE:
    raise ValueError("Set confirm_expensive=true before running core or full mode.")
RUN_COMPUTE = EXECUTION_MODE in {"smoke", "core", "full"}
DRY_RUN_LIMIT = SMOKE_LIMIT if EXECUTION_MODE == "smoke" else 0

SRC_CAT = _get_widget("source_catalog", "prod_tads")
SRC_SCH = _get_widget("source_schema", "youtube_too")
T_CH_DIM = f"{SRC_CAT}.{SRC_SCH}.{_get_widget('channels_dim_table', 'yt_sl_channels')}"
T_VID_DIM = f"{SRC_CAT}.{SRC_SCH}.{_get_widget('videos_dim_table', 'yt_sl_videos')}"
T_VID_MET = f"{SRC_CAT}.{SRC_SCH}.{_get_widget('videos_metrics_table', 'yt_sl_videos_metrics')}"
T_SUBSAMPLE = f"{SRC_CAT}.{SRC_SCH}.subsample_items"
PANEL_FQTN = _get_widget("channel_panel_fqtn", "dev_sean.default.yt_channel_stats_full").strip()
CHANNEL_KEY_OVERRIDE = _get_widget("channel_key_column", "").strip()

LANG_CAT = _get_widget("language_catalog", "dev_sean")
LANG_SCH = _get_widget("language_schema", "matt")
T_LANG = f"{LANG_CAT}.{LANG_SCH}.{_get_widget('language_channels_table', 'yt_lid_v3_channels')}"
LANG_RUN_ID = _get_widget("language_run_id", "default").strip()

CAT_CAT = _get_widget("category_catalog", "").strip() or SRC_CAT
CAT_SCH = _get_widget("category_schema", "").strip() or SRC_SCH
CAT_TABLE_OVERRIDE = _get_widget("category_table", "").strip()
CAT_COLUMN_OVERRIDE = _get_widget("category_column", "").strip()
CAT_LEVEL_OVERRIDE = _get_widget("category_level", "").strip().lower()
CHANNEL_CREATED_OVERRIDE = _get_widget("channel_created_column", "").strip()

DIAG_CAT = _get_widget("diagnostics_catalog", "dev_sean")
DIAG_SCH = _get_widget("diagnostics_schema", "diagnostics")
T_SUSPICION = f"{DIAG_CAT}.{DIAG_SCH}.{_get_widget('suspicion_flags_table', 'too_suspicion_flags')}"
T_RUN_SUMMARY = f"{DIAG_CAT}.{DIAG_SCH}.{_get_widget('run_summary_table', 'too_run_summary')}"

TOO_FLOOR = _get_int_widget("too_subscriber_floor", 10000)
SNAP_CUR = _get_widget("snapshot_current_date", "").strip()
SNAP_PRIOR = _get_widget("snapshot_prior_date", "").strip()
ANCHOR_MODE = _get_widget("anchor_selection_mode", "auto_complete").strip().lower()
ANCHOR_MIN_FRAC = _get_float_widget("anchor_min_fraction_of_max_partition", 0.90)
ALLOW_INCOMPLETE_PRIOR = _get_bool_widget("allow_incomplete_prior", False)
ALLOW_INCOMPLETE_ANCHOR = _get_bool_widget("allow_incomplete_anchor", False)
TARGET_WINDOW_DAYS = _get_int_widget("target_window_days", 7)
FALLBACK_WINDOWS = sorted({int(x) for x in _get_widget("attention_fallback_windows_days", "").split(",")
                           if x.strip() and int(x) > TARGET_WINDOW_DAYS})
N_BLOCKS = _get_int_widget("n_traffic_blocks", 20)
SUB_THRESHOLDS = sorted({int(x) for x in _get_widget("subscriber_thresholds", "1000,10000,100000,1000000").split(",") if x.strip()})
SHORTS_MAX_SECONDS = _get_int_widget("shorts_max_seconds", 180)
SHORTS_CHANGE_DATE = _get_widget("shorts_change_date", "2025-03-31").strip()
PROD_WINDOW_WEEKS = _get_int_widget("production_window_weeks", 4)
FORMAT_LOOKBACK_DAYS = _get_int_widget("format_lookback_days", 90)
LOOKBACK_TARGETS = [float(x) for x in _get_widget("lookback_capture_targets", "0.5,0.8,0.9,0.95").split(",") if x.strip()]
NEG_DELTA_POLICY = _get_widget("negative_delta_policy", "floor_zero").strip().lower()
MIN_LANG_SEGMENTS = _get_int_widget("min_valid_language_segments", 3)
TREEMAP_LABEL_SHARE = _get_float_widget("treemap_label_view_share", 0.02)
TREEMAP_ANONYMIZE_LABELS = _get_bool_widget("treemap_anonymize_labels", False)
TREEMAP_TOP_LANGS = _get_int_widget("treemap_top_languages", 8)
TREEMAP_TOP_CATS = _get_int_widget("treemap_top_categories", 6)
LORENZ_BINS = _get_int_widget("lorenz_value_bins", 200)
BLOCK_BINS = _get_int_widget("block_value_bins", 4000)

OUT_CAT = _get_widget("output_catalog", "dev_sean")
OUT_SCH = _get_widget("output_schema", "matt")
OUT_PREFIX = re.sub(r"[^A-Za-z0-9_]", "_", _get_widget("output_table_prefix", "claude_yt_attention"))
EXPORT_ROOT = _get_widget("export_root", "/Volumes/dev_sean/matt/models").rstrip("/")
EXPORT_SUBDIR = _get_widget("export_subdir", "").strip()
LOCAL_FIG_DIR = _get_widget("local_fig_dir", "figs")
MAX_EXPORT_ROWS = _get_int_widget("max_export_rows", 200000)
MIN_CELL = _get_int_widget("min_cell_count", 5)
WRITE_OUTPUTS = _get_bool_widget("write_outputs", True)
WRITE_DELTA = _get_bool_widget("write_delta_aggregates", True)
MAKE_FIGURES = _get_bool_widget("make_figures", True)
MAKE_SOURCE_TABLES = _get_bool_widget("make_source_tables", True)
RUN_ROBUSTNESS = _get_bool_widget("run_robustness", True)
FAIL_ON_MISSING_OUTPUTS = _get_bool_widget("fail_on_missing_outputs", True)
# Main-figure cells run if either the plot or its source-data table is wanted; save_fig itself no-ops when
# make_figures is false, so make_figures=false + make_source_tables=true yields tables without plots.
FIG_CELLS_ENABLED = MAKE_FIGURES or MAKE_SOURCE_TABLES

if ANCHOR_MODE not in {"auto_complete", "latest", "manual"}:
    raise ValueError("anchor_selection_mode must be auto_complete|latest|manual")

np.random.seed(SEED)
RUN_STAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
if not EXPORT_SUBDIR:
    EXPORT_SUBDIR = f"attention_paper_claude_{RUN_STAMP}"
EXPORT_DIR = f"{EXPORT_ROOT}/{EXPORT_SUBDIR}"

if _SPARK_AVAILABLE:
    try:
        spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))  # noqa: F821
    except Exception:
        pass

print(json.dumps({
    "author": AUTHOR, "run_stamp_utc": RUN_STAMP, "execution_mode": EXECUTION_MODE,
    "run_compute": RUN_COMPUTE, "smoke_limit": DRY_RUN_LIMIT or None, "panel": PANEL_FQTN,
    "anchor_mode": ANCHOR_MODE, "anchor_min_fraction": ANCHOR_MIN_FRAC, "too_floor": TOO_FLOOR,
    "target_window_days": TARGET_WINDOW_DAYS, "subscriber_thresholds": SUB_THRESHOLDS,
    "export_dir": EXPORT_DIR, "output_tables": f"{OUT_CAT}.{OUT_SCH}.{OUT_PREFIX}_*", "min_cell_count": MIN_CELL,
}, indent=2))

# COMMAND ----------
# ---- Reproducibility manifest -----------------------------------------------------------------------
RUN_MANIFEST: Dict[str, object] = {
    "author": AUTHOR, "notebook": NOTEBOOK_NAME, "run_stamp_utc": RUN_STAMP,
    "execution_mode": EXECUTION_MODE, "seed": SEED,
    "python": platform.python_version(), "numpy": np.__version__, "pandas": pd.__version__,
    "parameters": {
        "panel_fqtn": PANEL_FQTN, "anchor_mode": ANCHOR_MODE, "anchor_min_fraction": ANCHOR_MIN_FRAC,
        "too_subscriber_floor": TOO_FLOOR, "target_window_days": TARGET_WINDOW_DAYS,
        "n_traffic_blocks": N_BLOCKS, "subscriber_thresholds": SUB_THRESHOLDS,
        "shorts_max_seconds": SHORTS_MAX_SECONDS, "shorts_change_date": SHORTS_CHANGE_DATE,
        "format_lookback_days": FORMAT_LOOKBACK_DAYS, "negative_delta_policy": NEG_DELTA_POLICY,
        "min_valid_language_segments": MIN_LANG_SEGMENTS, "treemap_label_view_share": TREEMAP_LABEL_SHARE,
    },
    "outputs": [], "warnings": [],
    "data_readiness_2026_05_29": {
        "weekly_panel": "yt_channel_stats_full is the current Sunday panel but shallow (partitions 2026-05-27 "
                        "~12.4M and 2026-05-28 ~2.5M rows; not a Sunday-to-Sunday pair yet). The anchor is "
                        "chosen by a completeness rule (>= anchor_min_fraction of the largest partition), so "
                        "the partial 05-28 partition is rejected by default. Weekly views are elapsed-day "
                        "normalised; headline numbers still await a true 7-day pair.",
        "category": "Native topics in dev_sean.default.backfill_channels.topic_categories (array; many rows "
                    "still 'pending'); subsample_items.topic_top_k_json secondary. ai_label/all_labels empty.",
        "founding_date": "No platform-wide channel creation date (bronze created_at = ingest time). Age figure "
                         "self-skips unless an explicit YouTube publishedAt column is provided.",
        "language_run_id": "default",
    },
}

def _warn(msg: str) -> None:
    print(f"  [WARN] {msg}")
    RUN_MANIFEST["warnings"].append(msg)  # type: ignore


FAILED_STEPS: List[str] = []

def _fail(step: str, exc: Exception) -> None:
    """Record a failed figure/robustness step so a final required-output check can fail core/full runs."""
    _warn(f"{step} failed: {exc}")
    FAILED_STEPS.append(step)
    RUN_MANIFEST["failed_steps"] = list(FAILED_STEPS)  # type: ignore

# COMMAND ----------
# ---- manifest_only: stop before any table scan ------------------------------------------------------
if EXECUTION_MODE == "manifest_only":
    msg = ("manifest_only mode: configuration printed above; NO table scans were run. "
           "Set execution_mode=smoke for a limited test, or core/full with confirm_expensive=true.")
    print(msg)
    if _in_databricks():
        dbutils.notebook.exit(msg)  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Figure design system
# MAGIC Nature single 90 mm / double 180 mm; sans-serif 5-7 pt; 8 pt bold panel letters; Okabe-Ito CVD-safe
# MAGIC categorical palette; cividis/viridis sequential; data-ink minimised; 600-DPI PNG + vector PDF.

# COMMAND ----------
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Rectangle

MM = 1.0 / 25.4
FIG_1COL = 90 * MM
FIG_1P5COL = 136 * MM
FIG_2COL = 180 * MM

OKABE_ITO = {"black": "#000000", "orange": "#E69F00", "skyblue": "#56B4E9", "green": "#009E73",
             "yellow": "#F0E442", "blue": "#0072B2", "vermillion": "#D55E00", "purple": "#CC79A7"}
OI_CYCLE = [OKABE_ITO[k] for k in ("blue", "vermillion", "green", "orange", "purple", "skyblue", "yellow", "black")]
GREY = "#5A5A5A"
LIGHT_GREY = "#BFBFBF"
TIER_COLORS = {"observed": OKABE_ITO["blue"], "design": OKABE_ITO["orange"], "bounded": OKABE_ITO["vermillion"]}

plt.rcParams.update({
    "figure.dpi": 150, "savefig.dpi": 600, "savefig.bbox": "tight", "savefig.pad_inches": 0.02,
    "font.family": "sans-serif", "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 7, "axes.titlesize": 7, "axes.labelsize": 7, "xtick.labelsize": 6, "ytick.labelsize": 6,
    "legend.fontsize": 6, "axes.linewidth": 0.6, "axes.edgecolor": "#222222",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.prop_cycle": matplotlib.cycler(color=OI_CYCLE), "axes.grid": False,
    "lines.linewidth": 1.0, "lines.markersize": 3,
    "xtick.major.width": 0.6, "ytick.major.width": 0.6, "xtick.major.size": 2.5, "ytick.major.size": 2.5,
    "legend.frameon": False, "pdf.fonttype": 42, "ps.fonttype": 42, "svg.fonttype": "none",
})


def panel_label(ax, letter: str, dx: float = -0.16, dy: float = 1.04) -> None:
    ax.text(dx, dy, letter, transform=ax.transAxes, fontsize=8, fontweight="bold", va="top", ha="left")


def thousands(x, pos=None) -> str:
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(x) >= div:
            return f"{x/div:.0f}{suf}" if x % div == 0 else f"{x/div:.1f}{suf}"
    return f"{x:.0f}"


HUMAN_FMT = mticker.FuncFormatter(thousands)


def stable_palette(keys: Sequence[str]) -> Dict[str, str]:
    keys = list(keys)
    base = OI_CYCLE + [GREY, LIGHT_GREY]
    return {k: base[i % len(base)] for i, k in enumerate(keys)}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Export, persistence, and Spark utility helpers
# MAGIC `export_table` is the single choke-point: it persists each aggregate as a **Delta table** (when
# MAGIC enabled) *and* a `*_CLAUDE.csv`, refuses oversized frames, and suppresses count cells below
# MAGIC `min_cell_count`. Figures compute their aggregate, export it, then render from it.

# COMMAND ----------
def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as exc:
        _warn(f"Could not create dir {path}: {exc}")


if WRITE_OUTPUTS:
    _ensure_dir(EXPORT_DIR)
    _ensure_dir(LOCAL_FIG_DIR)   # only create local fig dir when actually writing (honors the dry-run contract)


def _sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()[:16]
    except Exception:
        return ""


def _delta_name(short: str) -> str:
    return f"{OUT_CAT}.{OUT_SCH}.{OUT_PREFIX}_{re.sub(r'[^A-Za-z0-9_]', '_', short)}"


def export_table(pdf: pd.DataFrame, name: str, *, suppress_count_cols: Optional[List[str]] = None,
                 description: str = "") -> pd.DataFrame:
    """Persist an aggregate pandas frame as Delta (optional) + `<name>_CLAUDE.csv`. Refuses oversized
    frames; suppresses count cells below MIN_CELL for the named count columns."""
    pdf = pdf.copy()
    if len(pdf) > MAX_EXPORT_ROWS:
        raise ValueError(f"export_table('{name}') has {len(pdf):,} rows > MAX_EXPORT_ROWS={MAX_EXPORT_ROWS:,}; "
                         "exports must be aggregates.")
    if suppress_count_cols and MIN_CELL > 1:
        mask = np.zeros(len(pdf), dtype=bool)
        for c in suppress_count_cols:
            if c in pdf.columns:
                cnt = pd.to_numeric(pdf[c], errors="coerce")
                # Suppress only small POSITIVE counts (0 < cnt < MIN_CELL); keep NaN / 0 structural rows
                # (status placeholders, "not in panel", genuine zeros).
                mask |= (cnt > 0) & (cnt < MIN_CELL)
        if mask.any():
            _warn(f"export_table('{name}'): suppressing {int(mask.sum())} small-positive rows below MIN_CELL={MIN_CELL}")
            pdf = pdf.loc[~mask].reset_index(drop=True)
    pdf.insert(0, "_author", AUTHOR)
    # Persist as Delta so aggregates are queryable/re-usable in-workspace. Record success accurately.
    delta_written = False
    if WRITE_OUTPUTS and WRITE_DELTA and _SPARK_AVAILABLE and len(pdf):
        try:
            (spark.createDataFrame(pdf).withColumn("_run_stamp", F.lit(RUN_STAMP))  # noqa: F821
             .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
             .saveAsTable(_delta_name(name)))
            delta_written = True
        except Exception as exc:
            _warn(f"export_table delta write failed for {name}: {exc}")
    fname = f"{name}_CLAUDE.csv"
    if not WRITE_OUTPUTS:
        print(f"  [dry] would export {fname} ({len(pdf):,} rows)")
        return pdf
    path = f"{EXPORT_DIR}/{fname}"
    pdf.to_csv(path, index=False)
    RUN_MANIFEST["outputs"].append({  # type: ignore
        "kind": "table", "name": fname, "path": path, "rows": int(len(pdf)),
        "delta": _delta_name(name) if delta_written else None,  # only claim Delta when it was actually written
        "cols": list(pdf.columns), "sha256_16": _sha256_of_file(path), "description": description})
    print(f"  exported {fname} ({len(pdf):,} rows)")
    return pdf


def save_fig(fig, name: str, *, description: str = "") -> None:
    stem = f"{name}_CLAUDE"
    if not MAKE_FIGURES:
        # Tables-only run (e.g. run_robustness with make_figures=false): render no figures.
        plt.close(fig)
        return
    if not WRITE_OUTPUTS:
        # Dry run: write nothing to disk (CSV, Delta, and figures are all suppressed).
        print(f"  [dry] would save figure {stem}.png/.pdf")
        plt.close(fig)
        return
    targets = [f"{EXPORT_DIR}/{stem}.png", f"{EXPORT_DIR}/{stem}.pdf",
               f"{LOCAL_FIG_DIR}/{stem}.png", f"{LOCAL_FIG_DIR}/{stem}.pdf"]
    for t in targets:
        try:
            fig.savefig(t)
        except Exception as exc:
            _warn(f"save_fig could not write {t}: {exc}")
    primary = next((t for t in targets if t.endswith(".png")), "")
    RUN_MANIFEST["outputs"].append({  # type: ignore
        "kind": "figure", "name": f"{stem}.png", "path": primary,
        "sha256_16": _sha256_of_file(primary), "description": description})
    print(f"  saved figure {stem}.png/.pdf")
    plt.close(fig)


def cols_lower(df) -> Dict[str, str]:
    return {c.lower(): c for c in df.columns}


def first_col(df, candidates: Iterable[str], override: str = "") -> Optional[str]:
    cmap = cols_lower(df)
    if override:
        return cmap.get(override.lower())
    for c in candidates:
        if c.lower() in cmap:
            return cmap[c.lower()]
    return None


def table_exists(fqtn: str) -> bool:
    if not _SPARK_AVAILABLE:
        return False
    try:
        spark.table(fqtn).schema  # noqa: F821
        return True
    except Exception:
        return False


def safe_table(fqtn: str):
    if not table_exists(fqtn):
        _warn(f"table not available: {fqtn}")
        return None
    return spark.table(fqtn)  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Analytical core (skipped entirely in manifest_only)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.0 Metadata & availability inventory (schema-drift guard)
# MAGIC Cheap existence + column check of every key table *before* the expensive work, so schema drift or a
# MAGIC missing dependency surfaces as one readable table rather than a mid-run failure. Metadata only.

# COMMAND ----------
if RUN_COMPUTE:
    _inv_tables = {
        "channel_panel": PANEL_FQTN, "channels_dim": T_CH_DIM, "videos_dim": T_VID_DIM,
        "videos_metrics": T_VID_MET, "language": T_LANG, "subsample_items": T_SUBSAMPLE,
        "category_backfill": "dev_sean.default.backfill_channels",
        "suspicion_flags": T_SUSPICION, "run_summary": T_RUN_SUMMARY,
        "too_rank_comparison": f"{DIAG_CAT}.{DIAG_SCH}.too_rank_comparison",
        "discovery_pub_subs": "dev_sean.default.pub_subs_full_pass_batches",
        "discovery_new_channels": "dev_sean.default.new_channels",
        "threshold_1k": "dev_sean.threshold_yt_1k.new_channels",
        "socialblade_top50k": "dev_sean.default.updated_sb_top50k",
    }
    _inv_rows = []
    for logical, fq in _inv_tables.items():
        try:
            cols = spark.table(fq).columns if table_exists(fq) else []  # noqa: F821
            _inv_rows.append({"logical_name": logical, "table": fq, "exists": bool(cols),
                              "n_columns": len(cols), "columns_preview": ", ".join(cols[:10])})
        except Exception as exc:
            _inv_rows.append({"logical_name": logical, "table": fq, "exists": False, "n_columns": 0,
                              "columns_preview": f"ERROR: {type(exc).__name__}"})
    export_table(pd.DataFrame(_inv_rows), "metadata_table_inventory",
                 description="Existence and column inventory of key tables (schema-drift / availability guard).")
    _missing = [r["logical_name"] for r in _inv_rows if not r["exists"]]
    if _missing:
        _warn(f"metadata inventory: missing/unavailable tables: {_missing}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.1 Anchor snapshot selection (completeness-guarded)
# MAGIC The current anchor is the latest partition whose row count is >= `anchor_min_fraction_of_max_partition`
# MAGIC x the largest partition's row count  -  so a half-written partition (e.g. a mid-run Sunday job) is not
# MAGIC silently used. The prior snapshot is the one nearest `anchor - target_window_days`, and the view delta
# MAGIC is normalised by the **actual** elapsed days.

# COMMAND ----------
channel_week = channel_master = channel_ranked = video_frame = None
KEY_COL = SUB_COL = VIEW_COL = CAPTURE_COL = NAME_COL = None
cur_date = prior_date = None
elapsed_days = None

if RUN_COMPUTE:
    ch_met = safe_table(PANEL_FQTN)
    SUBSCRIBER_CANDIDATES = ["subscriber_count", "follower_count", "subscribers", "subscriberCount"]
    VIEWS_CANDIDATES = ["total_view_count", "views_count", "view_count", "viewCount"]
    date_counts: List[Tuple[str, int]] = []
    if ch_met is not None:
        KEY_COL = first_col(ch_met, ["channel_id", "canonical_id", "channel"], CHANNEL_KEY_OVERRIDE)
        SUB_COL = first_col(ch_met, SUBSCRIBER_CANDIDATES)
        VIEW_COL = first_col(ch_met, VIEWS_CANDIDATES)
        NAME_COL = first_col(ch_met, ["channel_name", "channel_title", "title"])
        CAPTURE_COL = first_col(ch_met, ["collected_date", "capture_date", "snapshot_date"])
        print(f"panel columns: key={KEY_COL} sub={SUB_COL} view={VIEW_COL} name={NAME_COL} capture={CAPTURE_COL}")
        if not (KEY_COL and SUB_COL and VIEW_COL and CAPTURE_COL):
            _warn(f"panel missing columns (key={KEY_COL} sub={SUB_COL} view={VIEW_COL} capture={CAPTURE_COL})")
        else:
            cnts = (ch_met.groupBy(CAPTURE_COL).agg(F.count(F.lit(1)).alias("n"))
                    .orderBy(F.col(CAPTURE_COL).desc()).collect())
            date_counts = [(str(r[CAPTURE_COL]), int(r["n"])) for r in cnts]
            print("Snapshot partition row counts:", date_counts[:8])

    # ---- choose anchor (current) date ----
    if ANCHOR_MODE == "manual" and not SNAP_CUR:
        raise ValueError("anchor_selection_mode=manual requires an explicit snapshot_current_date; "
                         "set it, or use anchor_selection_mode=auto_complete/latest.")
    if date_counts:
        if SNAP_CUR:
            cur_date, sel_mode = SNAP_CUR, "manual"
        elif ANCHOR_MODE == "latest":
            cur_date, sel_mode = date_counts[0][0], "latest"
        else:  # auto_complete
            max_n = max(n for _, n in date_counts)
            complete = [(d, n) for d, n in date_counts if n >= ANCHOR_MIN_FRAC * max_n]
            # date_counts is sorted desc by date; first complete partition is the latest complete one.
            cur_date = complete[0][0] if complete else date_counts[0][0]
            sel_mode = "auto_complete" if complete else "auto_complete_fallback_latest"
        max_n = max(n for _, n in date_counts)
        cur_n = dict(date_counts).get(cur_date, 0)
        if cur_n < ANCHOR_MIN_FRAC * max_n:
            _warn(f"selected anchor {cur_date} has {cur_n:,} rows < {ANCHOR_MIN_FRAC:.0%} of max ({max_n:,}); "
                  "likely a partial partition.")
        export_table(pd.DataFrame([{"capture_date": d, "n_rows": n, "is_selected_anchor": d == cur_date,
                                     "fraction_of_max": (n / max_n if max_n else None)} for d, n in date_counts]),
                     "attention_anchor_snapshot_coverage", suppress_count_cols=None,
                     description="Per-partition row counts and the completeness-guarded anchor selection.")
    elif SNAP_CUR:
        cur_date = SNAP_CUR

    # ---- choose prior date (completeness-guarded, same rule as the anchor) ----
    prior_date = SNAP_PRIOR
    prior_complete = None
    all_dates = [d for d, _ in date_counts]
    if not prior_date and cur_date and all_dates:
        try:
            cur_dt = datetime.strptime(cur_date, "%Y-%m-%d").date()
            target = cur_dt - timedelta(days=TARGET_WINDOW_DAYS)
            max_n = max(n for _, n in date_counts) if date_counts else 0
            cnt_map = dict(date_counts)
            # Only consider earlier partitions that are themselves "complete" (>= frac of max).
            earlier = [datetime.strptime(d, "%Y-%m-%d").date() for d in all_dates
                       if datetime.strptime(d, "%Y-%m-%d").date() < cur_dt
                       and cnt_map.get(d, 0) >= ANCHOR_MIN_FRAC * max_n]
            if not earlier:  # fall back to any earlier partition, flagged as not completeness-guarded
                earlier = [datetime.strptime(d, "%Y-%m-%d").date() for d in all_dates
                           if datetime.strptime(d, "%Y-%m-%d").date() < cur_dt]
                prior_complete = False
            else:
                prior_complete = True
            prior_date = str(sorted(earlier, key=lambda d: abs((d - target).days))[0]) if earlier else None
            if prior_date and prior_complete is False:
                msg = (f"prior snapshot {prior_date} is below the completeness threshold "
                       f"({ANCHOR_MIN_FRAC:.0%} of max); weekly deltas may be biased toward channels present "
                       "in a partial partition.")
                if EXECUTION_MODE in {"core", "full"} and not ALLOW_INCOMPLETE_PRIOR:
                    raise ValueError(msg + " Refusing in core/full: set snapshot_prior explicitly, wait for a "
                                     "complete prior partition, or set allow_incomplete_prior=true to override.")
                _warn(msg + " Proceeding (smoke or allow_incomplete_prior=true).")
        except ValueError:
            raise
        except Exception:
            prior_date = all_dates[1] if len(all_dates) > 1 else None
    RUN_MANIFEST["snapshot_prior_completeness_guarded"] = prior_complete  # type: ignore
    # Record the completeness of BOTH chosen partitions regardless of how they were selected (auto, latest,
    # or manual override), so the manifest always says whether each side of the delta passed the rule.
    if date_counts:
        _max_n = max(n for _, n in date_counts)
        _cnt = dict(date_counts)

        def _completeness(dt):
            if not dt:
                return None
            n = _cnt.get(dt)
            if n is None:
                return {"date": dt, "n_rows": None, "fraction_of_max": None, "passes_rule": None, "note": "not_in_panel"}
            frac = (n / _max_n) if _max_n else None
            return {"date": dt, "n_rows": int(n), "fraction_of_max": round(frac, 4) if frac is not None else None,
                    "passes_rule": bool(frac is not None and frac >= ANCHOR_MIN_FRAC)}
        cur_comp = _completeness(cur_date); pri_comp = _completeness(prior_date)
        RUN_MANIFEST["snapshot_current_completeness"] = cur_comp   # type: ignore
        RUN_MANIFEST["snapshot_prior_completeness"] = pri_comp     # type: ignore
        if cur_comp and cur_comp.get("passes_rule") is False:
            cmsg = (f"current anchor {cur_date} does NOT pass the completeness rule "
                    f"(fraction={cur_comp.get('fraction_of_max')}); likely a partial partition "
                    "(manual snapshot_current_date or anchor_selection_mode=latest).")
            if EXECUTION_MODE in {"core", "full"} and not ALLOW_INCOMPLETE_ANCHOR:
                raise ValueError(cmsg + " Refusing in core/full: use anchor_selection_mode=auto_complete, set a "
                                 "complete snapshot_current_date, or set allow_incomplete_anchor=true to override.")
            _warn(cmsg + " Proceeding (smoke or allow_incomplete_anchor=true).")
        if pri_comp and pri_comp.get("passes_rule") is False:
            # Enforce here too, so a MANUAL snapshot_prior (which bypasses the auto-fallback guard) is also
            # refused in core/full unless overridden. (The auto path may have already raised earlier.)
            pmsg = (f"prior snapshot {prior_date} does NOT pass the completeness rule "
                    f"(fraction={pri_comp.get('fraction_of_max')}).")
            if EXECUTION_MODE in {"core", "full"} and not ALLOW_INCOMPLETE_PRIOR:
                raise ValueError(pmsg + " Refusing in core/full: set a complete snapshot_prior, wait for a "
                                 "complete prior partition, or set allow_incomplete_prior=true to override.")
            _warn(pmsg + " Proceeding (smoke or allow_incomplete_prior=true).")
    if cur_date and prior_date:
        elapsed_days = (datetime.strptime(cur_date, "%Y-%m-%d").date()
                        - datetime.strptime(prior_date, "%Y-%m-%d").date()).days
    print(f"Anchor: prior={prior_date} -> current={cur_date}  (elapsed_days={elapsed_days})")
    if elapsed_days and elapsed_days != TARGET_WINDOW_DAYS:
        _warn(f"snapshot gap is {elapsed_days}d, not {TARGET_WINDOW_DAYS}d; weekly views are rate-normalised "
              "(delta/elapsed*7). Headline numbers should await a true 7-day pair.")
    RUN_MANIFEST["snapshot_current"] = cur_date          # type: ignore
    RUN_MANIFEST["snapshot_prior"] = prior_date          # type: ignore
    RUN_MANIFEST["snapshot_elapsed_days"] = elapsed_days  # type: ignore

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.2 Weekly-views panel + entry/exit status
# MAGIC `weekly_views = (views_cur - views_prior) / elapsed_days x 7`, subscribers >= floor. Channels in the
# MAGIC current snapshot but missing from the prior one get null weekly views and are summarised explicitly.
# MAGIC
# MAGIC **Optional multi-window cascade** (`attention_fallback_windows_days`, default off): for the immature
# MAGIC panel, a channel lacking a primary (target-window) measure can be filled from a longer *complete*-prior
# MAGIC window, with the window actually used recorded per channel (`attention_window_used_days`,
# MAGIC `attention_measure_status`) and summarised in `attention_window_usage_summary` - so mixed-window
# MAGIC measurement is explicit, never silent. Default off keeps the strict single-7-day path.

# COMMAND ----------
def _dedupe_latest(df, key_col: str, date_col: Optional[str] = None, order_cols=None):
    """Keep one row per key. By default orders by `date_col` desc; pass `order_cols` (a list of Columns)
    to resolve duplicate (key, date) rows by metric recency rather than arbitrarily."""
    if order_cols is None:
        order_cols = [F.col(date_col).desc(), F.col(key_col).asc()]
    w = Window.partitionBy(key_col).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


if RUN_COMPUTE and ch_met is not None and KEY_COL and SUB_COL and VIEW_COL and CAPTURE_COL and cur_date and prior_date and elapsed_days and elapsed_days > 0:
    # Prefer a true metric/ingest timestamp to break (channel, date) duplicates by recency; fall back to
    # the larger lifetime-view value (then channel_id) for determinism if no timestamp column exists.
    TS_COL = first_col(ch_met, ["collected_at", "metric_timestamp", "last_ingestion_timestamp",
                                "ingestion_timestamp", "updated_at"])
    RUN_MANIFEST["snapshot_dedupe_recency_column"] = TS_COL or "(none; tiebreak by max views)"  # type: ignore
    cur_sel = [F.col(KEY_COL).cast("string").alias("channel_id"),
               (F.col(NAME_COL).cast("string") if NAME_COL else F.lit(None).cast("string")).alias("channel_name"),
               F.col(SUB_COL).cast("double").alias("subscribers"),
               F.col(VIEW_COL).cast("double").alias("views_cur")]
    if TS_COL:
        cur_sel.append(F.col(TS_COL).cast("timestamp").alias("_ts"))
    cur = ch_met.where(F.col(CAPTURE_COL) == F.lit(cur_date)).select(*cur_sel)
    cur_order = ([F.col("_ts").desc_nulls_last()] if TS_COL else []) + [F.col("views_cur").desc_nulls_last(), F.col("channel_id").asc()]
    cur = _dedupe_latest(cur, "channel_id", order_cols=cur_order)
    if TS_COL:
        cur = cur.drop("_ts")
    cur = cur.where(F.col("subscribers") >= F.lit(float(TOO_FLOOR)))

    # SMOKE: sample anchor-partition channel IDs (>= floor) up front, so the prior-snapshot read and the
    # delta join are bounded  -  a genuinely cheap test, not a full-panel scan limited after the fact.
    if DRY_RUN_LIMIT and DRY_RUN_LIMIT > 0:
        # Deterministic sample (ordered by channel_id) so smoke runs are reproducible. The current anchor
        # partition is scanned once to obtain the ID list; the prior read and delta join are then bounded.
        smoke_ids = cur.select("channel_id").orderBy("channel_id").limit(DRY_RUN_LIMIT).cache()
        cur = cur.join(F.broadcast(smoke_ids), on="channel_id", how="inner")
        _warn(f"SMOKE: universe limited to {DRY_RUN_LIMIT:,} anchor channels (deterministic by channel_id) before the prior join")

    pri_sel = [F.col(KEY_COL).cast("string").alias("channel_id"), F.col(VIEW_COL).cast("double").alias("views_prior")]
    if TS_COL:
        pri_sel.append(F.col(TS_COL).cast("timestamp").alias("_ts"))
    pri = ch_met.where(F.col(CAPTURE_COL) == F.lit(prior_date)).select(*pri_sel)
    pri_order = ([F.col("_ts").desc_nulls_last()] if TS_COL else []) + [F.col("views_prior").desc_nulls_last(), F.col("channel_id").asc()]
    pri = _dedupe_latest(pri, "channel_id", order_cols=pri_order)
    if TS_COL:
        pri = pri.drop("_ts")
    if DRY_RUN_LIMIT and DRY_RUN_LIMIT > 0:
        pri = pri.join(F.broadcast(smoke_ids), on="channel_id", how="inner")

    def _weekly_from_delta(delta_col, elapsed):
        """Apply the negative-delta policy and normalise to a weekly rate; null delta stays null."""
        sc = F.lit(7.0) / F.lit(float(elapsed))
        if NEG_DELTA_POLICY == "keep":
            return delta_col * sc
        if NEG_DELTA_POLICY == "drop":
            return F.when(delta_col < 0, F.lit(None).cast("double")).otherwise(delta_col * sc)
        return F.when(delta_col.isNull(), F.lit(None).cast("double")).otherwise(F.greatest(delta_col, F.lit(0.0)) * sc)

    cw = (cur.join(pri, on="channel_id", how="left")
          .withColumn("raw_delta", F.col("views_cur") - F.col("views_prior"))
          .withColumn("is_new_this_week", F.col("views_prior").isNull())
          .withColumn("weekly_views", _weekly_from_delta(F.col("raw_delta"), elapsed_days))
          # Per-channel measurement provenance (primary window where available). `attention_is_primary`
          # disambiguates primary vs fallback without relying on the window length (which a fallback could
          # coincidentally share on an off-cadence panel).
          .withColumn("attention_is_primary", F.col("weekly_views").isNotNull())
          .withColumn("attention_window_used_days",
                      F.when(F.col("weekly_views").isNotNull(), F.lit(int(elapsed_days))).otherwise(F.lit(None).cast("int")))
          .withColumn("attention_elapsed_days",
                      F.when(F.col("weekly_views").isNotNull(), F.lit(int(elapsed_days))).otherwise(F.lit(None).cast("int"))))

    # ---- Optional multi-window cascade: fill channels lacking a primary measure from longer complete windows.
    if FALLBACK_WINDOWS and date_counts:
        _cnt = dict(date_counts); _maxn = max(n for _, n in date_counts)
        _curdt = datetime.strptime(cur_date, "%Y-%m-%d").date()
        complete_earlier = sorted([datetime.strptime(d, "%Y-%m-%d").date() for d, n in date_counts
                                   if datetime.strptime(d, "%Y-%m-%d").date() < _curdt and n >= ANCHOR_MIN_FRAC * _maxn])
        used = {}
        for w in FALLBACK_WINDOWS:
            tgt = _curdt - timedelta(days=w)
            cands = [d for d in complete_earlier if d != datetime.strptime(prior_date, "%Y-%m-%d").date()]
            if not cands:
                continue
            pdate = min(cands, key=lambda d: abs((d - tgt).days)); el = (_curdt - pdate).days
            if el <= 0 or str(pdate) in used:
                continue
            used[str(pdate)] = el
            pw_sel = [F.col(KEY_COL).cast("string").alias("channel_id"), F.col(VIEW_COL).cast("double").alias("vw_prior")]
            if TS_COL:
                pw_sel.append(F.col(TS_COL).cast("timestamp").alias("_ts"))
            pw = ch_met.where(F.col(CAPTURE_COL) == F.lit(str(pdate))).select(*pw_sel)
            pw = _dedupe_latest(pw, "channel_id",
                                order_cols=([F.col("_ts").desc_nulls_last()] if TS_COL else []) + [F.col("vw_prior").desc_nulls_last(), F.col("channel_id").asc()])
            if TS_COL:
                pw = pw.drop("_ts")
            if DRY_RUN_LIMIT and DRY_RUN_LIMIT > 0:
                pw = pw.join(F.broadcast(smoke_ids), on="channel_id", how="inner")
            cw = cw.join(pw, on="channel_id", how="left")
            wv_w = _weekly_from_delta(F.col("views_cur") - F.col("vw_prior"), el)
            fill = F.col("weekly_views").isNull() & wv_w.isNotNull()
            cw = (cw.withColumn("attention_window_used_days", F.when(fill, F.lit(int(w))).otherwise(F.col("attention_window_used_days")))
                    .withColumn("attention_elapsed_days", F.when(fill, F.lit(int(el))).otherwise(F.col("attention_elapsed_days")))
                    .withColumn("weekly_views", F.coalesce(F.col("weekly_views"), wv_w))
                    .drop("vw_prior"))
        RUN_MANIFEST["attention_fallback_windows_used"] = used  # type: ignore

    cw = cw.withColumn("attention_measure_status",
                       F.when(F.col("weekly_views").isNull(), F.lit("no_prior_any_window"))
                        .when(F.col("attention_is_primary"), F.lit("primary_window"))
                        .otherwise(F.lit("fallback_window")))

    channel_week = cw.cache()
    n_ch = channel_week.count()
    # Entry/exit + measurability status summary.
    status = (channel_week.groupBy("is_new_this_week")
              .agg(F.count(F.lit(1)).alias("n_channels"),
                   F.sum(F.when(F.col("weekly_views").isNull(), 1).otherwise(0)).alias("n_null_weekly_views"),
                   F.sum(F.when(F.col("raw_delta") < 0, 1).otherwise(0)).alias("n_negative_delta")).toPandas())
    export_table(status, "attention_measurability_status", suppress_count_cols=["n_channels"],
                 description="Channels by whether a prior snapshot existed; null-weekly and negative-delta counts.")
    # Which attention window was actually used per channel (1 row when single-window; richer with the cascade).
    wu = (channel_week.groupBy("attention_measure_status", "attention_window_used_days", "attention_elapsed_days")
          .agg(F.count(F.lit(1)).alias("n_channels"), F.sum("weekly_views").alias("weekly_views")).toPandas())
    export_table(wu, "attention_window_usage_summary", suppress_count_cols=["n_channels"],
                 description="Per-channel attention measurement window used (primary vs fallback) and elapsed days.")
    print(f"channel_week rows (subs>={TOO_FLOOR:,}): {n_ch:,}")
    RUN_MANIFEST["too_universe_n_channels"] = int(n_ch)  # type: ignore
elif RUN_COMPUTE:
    _warn("channel_week not built  -  panel columns or snapshot pair unavailable; downstream cells will skip.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.3 Join channel dimension, language, and category

# COMMAND ----------
ch_dim = safe_table(T_CH_DIM) if RUN_COMPUTE else None
CREATED_COL = None
if ch_dim is not None:
    # Strict: only an explicit YouTube creation/publishedAt column  -  never a generic created_at (ingest time).
    CREATED_COL = first_col(ch_dim, ["channel_published_at", "snippet_published_at", "channel_created_at",
                                     "channel_creation_date", "channel_start_date"], CHANNEL_CREATED_OVERRIDE)
    print(f"channel creation-date column: {CREATED_COL or '<none found; age figure will self-skip>'}")

CATEGORY_LIKE = ["topic_categories", "category", "channel_category", "video_category", "category_name",
                 "category_title", "primary_category", "yt_category", "topic_top_k_json", "category_id",
                 "categoryid", "topic_category", "ai_label", "primary_topic"]


def resolve_category_source():
    if CAT_TABLE_OVERRIDE and CAT_COLUMN_OVERRIDE:
        fq = f"{CAT_CAT}.{CAT_SCH}.{CAT_TABLE_OVERRIDE}"
        lvl = CAT_LEVEL_OVERRIDE or ("video" if "video" in CAT_TABLE_OVERRIDE.lower() else "channel")
        return fq, CAT_COLUMN_OVERRIDE, lvl
    for fq, lvl in (("dev_sean.default.backfill_channels", "channel"), (T_SUBSAMPLE, "channel"),
                    (T_CH_DIM, "channel"), (T_VID_DIM, "video")):
        df = safe_table(fq)
        if df is None:
            continue
        col = first_col(df, CATEGORY_LIKE)
        if col:
            return fq, col, lvl
    return None, None, None


CAT_FQTN = CAT_COL = CAT_LEVEL = None
if RUN_COMPUTE:
    CAT_FQTN, CAT_COL, CAT_LEVEL = resolve_category_source()
    print(f"category source: table={CAT_FQTN} column={CAT_COL} level={CAT_LEVEL}")
    RUN_MANIFEST["category_source"] = {"table": CAT_FQTN, "column": CAT_COL, "level": CAT_LEVEL}  # type: ignore


def _normalize_topic(col):
    c = F.regexp_replace(col.cast("string"), r"^https?://[^/]+/wiki/", "")
    c = F.regexp_replace(c, "_", " ")
    c = F.regexp_replace(c, "%26", "&")
    return F.trim(c)


def channel_category_frame():
    if not (CAT_FQTN and CAT_COL):
        return None
    df = safe_table(CAT_FQTN)
    if df is None:
        return None
    key = first_col(df, ["channel_id", "canonical_id", "channel"])
    if not key:
        _warn(f"category source {CAT_FQTN} has no channel key; skipping category join.")
        return None
    # Treat un-backfilled rows as MISSING: if the source has a status column, drop rows that are not 'done'
    # (e.g. 'pending'/'error'), so a stale or sentinel category on a pending row can't inflate coverage.
    status_col = first_col(df, ["status", "backfill_status"])
    if status_col:
        df = df.where(F.lower(F.trim(F.col(status_col))) == F.lit("done"))
    dtype = dict((f.name, f.dataType.typeName()) for f in df.schema.fields).get(CAT_COL, "string")
    if dtype == "array":
        cat_expr = _normalize_topic(F.element_at(F.col(CAT_COL), 1))   # primary topic = first element
        base = df.select(F.col(key).cast("string").alias("channel_id"), cat_expr.alias("category"))
        return base.where(F.col("category").isNotNull() & (F.length(F.trim(F.col("category"))) > 0)).dropDuplicates(["channel_id"])
    if CAT_COL.lower().endswith("json"):
        # Secondary source. Require STRUCTURED parse success (from_json over the common shapes); if neither
        # shape parses, the category is left null (channel -> 'uncategorized') rather than risking a regex
        # capturing a JSON key. The exact schema of subsample_items.topic_top_k_json is unconfirmed.
        _warn(f"category from JSON column {CAT_COL} is a best-effort secondary source (structured parse only; "
              "no regex fallback). Prefer backfill_channels.topic_categories or the LLM categorization.")
        cj = F.col(CAT_COL).cast("string")
        arr_str = F.from_json(cj, "array<string>")
        arr_obj = F.from_json(cj, "array<struct<topic:string,category:string,label:string>>")
        first_obj = F.element_at(arr_obj, 1)
        from_struct = F.coalesce(first_obj.getField("category"), first_obj.getField("topic"), first_obj.getField("label"))
        cat_expr = F.coalesce(F.element_at(arr_str, 1), from_struct)   # null if neither shape parses
        base = df.select(F.col(key).cast("string").alias("channel_id"), _normalize_topic(cat_expr).alias("category"))
        return base.where(F.col("category").isNotNull() & (F.length(F.trim(F.col("category"))) > 0)).dropDuplicates(["channel_id"])
    base = (df.select(F.col(key).cast("string").alias("channel_id"), F.col(CAT_COL).cast("string").alias("category"))
            .where(F.col("category").isNotNull() & (F.length(F.trim(F.col("category"))) > 0)))
    if CAT_LEVEL == "video":
        roll = base.groupBy("channel_id", "category").agg(F.count(F.lit(1)).alias("n"))
        w = Window.partitionBy("channel_id").orderBy(F.col("n").desc(), F.col("category").asc())
        return roll.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).select("channel_id", "category")
    return base.dropDuplicates(["channel_id"])


def channel_language_frame():
    lang = safe_table(T_LANG)
    if lang is None:
        return None
    run_id = LANG_RUN_ID
    has_run = "run_id" in [c.lower() for c in lang.columns]
    if has_run:
        rid_col = cols_lower(lang)["run_id"]
        try:
            present = [str(r[0]) for r in lang.select(rid_col).distinct().collect()]
        except Exception:
            present = []
        if run_id and run_id not in present:
            # Guard the language_run_id="default" trap: if the configured run_id is not actually in the
            # table, don't silently collapse all labels to `und` - fall back to the latest present run_id.
            fallback = max(present) if present else ""
            _warn(f"language run_id='{run_id}' not found in {T_LANG} (present: {present[:6]}); "
                  f"falling back to '{fallback}'.")
            run_id = fallback
        elif not run_id:
            run_id = max(present) if present else ""
        if run_id:
            lang = lang.where(F.col(rid_col) == F.lit(run_id))
    RUN_MANIFEST["language_run_id"] = run_id  # type: ignore
    lkey = first_col(lang, ["channel_id", "canonical_id", "channel"])
    if not lkey:
        _warn(f"language table {T_LANG} has no channel key; skipping language join."); return None
    label_col = first_col(lang, ["consensus_for_rollup_label", "consensus_language_label", "primary_language_label"])
    iso_col = first_col(lang, ["consensus_language_iso639_3", "primary_language_iso639_3"])
    seg_col = first_col(lang, ["valid_language_segment_count"])
    out = lang.select(
        F.col(lkey).cast("string").alias("channel_id"),
        (F.col(label_col) if label_col else F.lit(None)).alias("language_raw"),
        (F.col(iso_col) if iso_col else F.lit(None)).alias("language_iso"),
        (F.col(seg_col) if seg_col else F.lit(None)).alias("_segs"))
    gate = (F.col("_segs") >= F.lit(MIN_LANG_SEGMENTS)) if seg_col else F.lit(True)
    out = out.withColumn("language", F.when(gate & F.col("language_raw").isNotNull(), F.col("language_raw")).otherwise(F.lit("und")))
    return out.select("channel_id", "language", "language_iso").dropDuplicates(["channel_id"])

# COMMAND ----------
if RUN_COMPUTE and channel_week is not None:
    cm = channel_week
    if ch_dim is not None:
        dkey = first_col(ch_dim, ["channel_id", "canonical_id", "channel"])
        if dkey:
            dim_sel = [F.col(dkey).cast("string").alias("channel_id")]
            lc = first_col(ch_dim, ["language_code"]); dl = first_col(ch_dim, ["detected_language"]); cap = first_col(ch_dim, ["capture_date"])
            if lc:
                dim_sel.append(F.col(lc).alias("source_language_code"))
            if dl:
                dim_sel.append(F.col(dl).alias("source_detected_language"))
            if CREATED_COL:
                dim_sel.append(F.to_timestamp(F.col(CREATED_COL)).alias("channel_created_at"))
            dim = ch_dim.select(*dim_sel, (F.col(cap) if cap else F.lit("x")).alias("_d"))
            dim = _dedupe_latest(dim, "channel_id", "_d").drop("_d")
            cm = cm.join(dim, on="channel_id", how="left")
        else:
            _warn(f"channel dim {T_CH_DIM} has no channel key; skipping dim join.")

    langf = channel_language_frame()
    if langf is not None:
        cm = cm.join(langf, on="channel_id", how="left")
    cm = cm.withColumn("language", F.coalesce(F.col("language") if "language" in cm.columns else F.lit(None), F.lit("und")))
    if "language_iso" not in cm.columns:
        cm = cm.withColumn("language_iso", F.lit(None).cast("string"))

    catf = channel_category_frame()
    if catf is not None:
        cm = cm.join(catf, on="channel_id", how="left")
    cm = cm.withColumn("category", F.coalesce(F.col("category") if "category" in cm.columns else F.lit(None), F.lit("uncategorized")))
    # Map sentinel/placeholder category values (incl. 'pending') to 'uncategorized' so coverage and the R7
    # missingness bounds count them as missing rather than as a real category.
    _CAT_SENTINELS = ["", "pending", "unknown", "uncategorized", "none", "null", "n/a", "na", "undefined"]
    cm = cm.withColumn("category",
                       F.when(F.lower(F.trim(F.col("category"))).isin(_CAT_SENTINELS), F.lit("uncategorized"))
                        .otherwise(F.col("category")))
    cm = cm.withColumn("founding_year",
                       F.year(F.col("channel_created_at")) if (CREATED_COL and "channel_created_at" in cm.columns)
                       else F.lit(None).cast("int"))

    channel_master = cm.cache()
    tot = channel_master.count()
    if tot == 0:
        # Don't divide by zero: a bad smoke sample / empty anchor / over-aggressive filter should read as a
        # readiness message, not a crash. Leave channel_master built (downstream cells self-skip on empties).
        _warn("channel_master has 0 rows (empty anchor, smoke sample, or filters); coverage not computed. "
              "Check the snapshot pair, subscriber floor, and smoke settings.")
        RUN_MANIFEST["too_universe_n_channels"] = 0  # type: ignore
        RUN_MANIFEST["category_coverage"] = None      # type: ignore
        RUN_MANIFEST["language_coverage"] = None       # type: ignore
    else:
        cov = channel_master.agg(
            F.sum(F.when(F.col("category") != F.lit("uncategorized"), 1).otherwise(0)).alias("cat"),
            F.sum(F.when(F.col("language") != F.lit("und"), 1).otherwise(0)).alias("lang")).first()
        print(f"category coverage: {cov['cat']/tot:.1%}   language coverage: {cov['lang']/tot:.1%}")
        RUN_MANIFEST["category_coverage"] = round(cov["cat"] / tot, 4)  # type: ignore
        RUN_MANIFEST["language_coverage"] = round(cov["lang"] / tot, 4)  # type: ignore

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.4 Traffic blocks (value-binned; no global sort) and inferential tiers
# MAGIC Blocks are **approximate** equal-view-mass (~5%) blocks: weekly views are binned on a fine log grid
# MAGIC (whole bins assigned to blocks) and mapped via a tiny broadcast table  -  avoiding a single-partition
# MAGIC global `row_number`/cumulative window, so the step scales to ~100M+ channels. Figure 4 exports the
# MAGIC **actual** view share per block (`fig4_block_actual_view_share`) so the approximation is auditable.
# MAGIC Tiers: observed (>=10k, in panel), design (1k-10k), bounded (<1k); the panel covers the observed tier.

# COMMAND ----------
def assign_traffic_blocks(cm, n_blocks: int, nbins: int):
    pos = cm.where(F.col("weekly_views") > 0)
    total = pos.agg(F.sum("weekly_views")).first()[0] or 0.0
    stats = pos.withColumn("lv", F.log10("weekly_views")).agg(F.min("lv").alias("a"), F.max("lv").alias("b")).first()
    mn, mx = stats["a"], stats["b"]
    if mn is None:
        return None, 0.0
    width = (mx - mn) / nbins if mx > mn else 1.0
    bin_expr = F.least(F.floor((F.log10("weekly_views") - F.lit(mn)) / F.lit(width)).cast("int"), F.lit(nbins - 1))
    binned = (pos.withColumn("bin", bin_expr).groupBy("bin")
              .agg(F.count(F.lit(1)).alias("n"), F.sum("weekly_views").alias("wv")).toPandas())
    binned = binned.sort_values("bin", ascending=False).reset_index(drop=True)  # high views first
    cum_lower = (binned["wv"].cumsum() - binned["wv"]) / (total if total else 1.0)
    binned["block"] = np.minimum((np.floor(cum_lower * n_blocks) + 1).astype(int), n_blocks)
    mapping = spark.createDataFrame(binned[["bin", "block"]])  # tiny (<= nbins rows)
    ranked = pos.withColumn("bin", bin_expr).join(F.broadcast(mapping), on="bin", how="left").drop("bin") \
                .withColumnRenamed("block", "traffic_block")
    return ranked, total


if RUN_COMPUTE and channel_master is not None:
    cr, total_wv = assign_traffic_blocks(channel_master, N_BLOCKS, BLOCK_BINS)
    if cr is not None:
        cr = cr.withColumn("tier",
                           F.when(F.col("subscribers") >= F.lit(float(TOO_FLOOR)), F.lit("observed"))
                            .when(F.col("subscribers") >= F.lit(1000.0), F.lit("design"))
                            .otherwise(F.lit("bounded")))
        channel_ranked = cr.cache()
        print(f"Total weekly views (observed TOO tier): {total_wv:,.0f}")
        RUN_MANIFEST["total_weekly_views"] = float(total_wv)  # type: ignore
    else:
        _warn("traffic-block assignment found no positive weekly views; figures depending on it will skip.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.5 Video-level frame for format & production
# MAGIC Restricted up front to videos published within `max(format_lookback_days, production window)` of the
# MAGIC anchor, and to video-metric snapshots **at or before the anchor**  -  so cost is bounded and Fig 6 is
# MAGIC consistent with the attention window.

# COMMAND ----------
def parse_duration_seconds(colname: str):
    """Parse a duration column that may be ISO-8601 ('PT1M30S'), plain seconds ('90'), or colon-formatted
    ('1:30', '1:02:03'). Colon strings are handled explicitly so they are NOT mis-evaluated as 0 seconds
    (which would misclassify long videos as Shorts)."""
    c = F.trim(F.col(colname).cast("string"))
    iso = (F.coalesce(F.regexp_extract(c, r"(\d+)H", 1).cast("double"), F.lit(0)) * 3600
           + F.coalesce(F.regexp_extract(c, r"(\d+)M", 1).cast("double"), F.lit(0)) * 60
           + F.coalesce(F.regexp_extract(c, r"(\d+)S", 1).cast("double"), F.lit(0)))
    parts = F.split(c, ":")
    n = F.size(parts)
    colon = (F.when(n == 3, parts.getItem(0).cast("double") * 3600 + parts.getItem(1).cast("double") * 60 + parts.getItem(2).cast("double"))
             .when(n == 2, parts.getItem(0).cast("double") * 60 + parts.getItem(1).cast("double"))
             .when(n == 1, parts.getItem(0).cast("double")))
    return (F.when(c.rlike(r"^PT"), iso)
            .when(c.rlike(r"^\d+(\.\d+)?$"), c.cast("double"))
            .when(c.contains(":"), colon)
            .otherwise(F.lit(None).cast("double")))


if RUN_COMPUTE and channel_master is not None:
    vid_dim = safe_table(T_VID_DIM)
    vid_met = safe_table(T_VID_MET)
    if vid_dim is not None:
        LEN_COL = first_col(vid_dim, ["video_length", "duration", "duration_seconds", "length_seconds"])
        # Explicit YouTube publish date only - NOT generic created_at (which is ingest time elsewhere and
        # would make the format/lookback timing wrong). If absent, production-timing filters self-skip.
        PUB_COL = first_col(vid_dim, ["published_at", "published_date", "snippet_published_at"])
        PTYPE_COL = first_col(vid_dim, ["post_type"])
        lookback_days = max(FORMAT_LOOKBACK_DAYS, PROD_WINDOW_WEEKS * 7)
        universe_ids = channel_master.select("channel_id").distinct()
        vsel = [F.col("channel_id").cast("string").alias("channel_id"), F.col("video_id").cast("string").alias("video_id")]
        if PUB_COL:
            vsel.append(F.to_timestamp(F.col(PUB_COL)).alias("published_at"))
        if LEN_COL:
            vsel.append(parse_duration_seconds(LEN_COL).alias("duration_s"))
        if PTYPE_COL:
            vsel.append(F.lower(F.col(PTYPE_COL)).alias("post_type"))
        vf = vid_dim.select(*vsel).join(universe_ids, on="channel_id", how="inner")
        if "published_at" in vf.columns and cur_date:
            lb_cutoff = (datetime.strptime(cur_date, "%Y-%m-%d") - timedelta(days=lookback_days)).date().isoformat()
            vf = vf.where((F.col("published_at") >= F.lit(lb_cutoff)) & (F.col("published_at") <= F.lit(cur_date)))
        dur_short = (F.col("duration_s") <= F.lit(float(SHORTS_MAX_SECONDS))) if LEN_COL else F.lit(False)
        ptype_short = (F.col("post_type") == F.lit("short")) if PTYPE_COL else F.lit(False)
        vf = vf.withColumn("is_short", (ptype_short | dur_short)) \
               .withColumn("format", F.when(F.col("is_short"), F.lit("Shorts")).otherwise(F.lit("Long-form")))
        if vid_met is not None:
            vmcap = first_col(vid_met, ["capture_date", "collected_date"]); vmview = first_col(vid_met, ["view_count", "views_count"])
            if vmcap and vmview:
                vm = vid_met.select(F.col("video_id").cast("string").alias("video_id"),
                                    F.col(vmview).cast("double").alias("video_views"),
                                    F.to_date(F.col(vmcap)).alias("vmdate"))
                if cur_date:
                    vm = vm.where(F.col("vmdate") <= F.to_date(F.lit(cur_date)))   # anchor-consistent, not global latest
                vm = _dedupe_latest(vm, "video_id", "vmdate").drop("vmdate")
                vf = vf.join(vm, on="video_id", how="left")
        if "video_views" not in vf.columns:
            vf = vf.withColumn("video_views", F.lit(None).cast("double"))
        video_frame = vf.cache()
        print(f"video_frame rows (within {lookback_days}d lookback): {video_frame.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Main figures

# COMMAND ----------
# MAGIC %md
# MAGIC ### Figure 1 | Discovery saturates at high attention
# MAGIC Panel a: per-batch new-channel yield (public-subscription crawl). Panel b: **high-subscriber head**
# MAGIC saturation  -  new channels with >=100k subscribers per batch (from `new_channels`). This is a
# MAGIC subscriber-threshold head proxy, not a views/attention-weighted measure (subscriber counts != weekly
# MAGIC attention; see Fig 3); a true attention-weighted curve needs per-batch weekly views.

# COMMAND ----------
def figure1_discovery():
    yield_frames = {}
    for k, fq in {"pub_subs": "dev_sean.default.pub_subs_full_pass_batches",
                  "pub_subs_seed": "dev_sean.default.pub_subs_batches"}.items():
        df = safe_table(fq)
        if df is None:
            continue
        bn = first_col(df, ["batch_number", "batch"]); nc = first_col(df, ["new_channels_count", "candidates_new", "qualified_new"])
        if bn and nc:
            yield_frames[k] = (df.select(F.col(bn).cast("int").alias("batch"), F.col(nc).cast("double").alias("new_channels"))
                               .groupBy("batch").agg(F.sum("new_channels").alias("new_channels")).orderBy("batch").toPandas())
    # Attention-weighted: high-subscriber new channels per batch from new_channels (has subscriber_count).
    hi = None
    nc_tbl = safe_table("dev_sean.default.new_channels")
    if nc_tbl is not None:
        bn = first_col(nc_tbl, ["batch_number", "batch"]); sc = first_col(nc_tbl, ["subscriber_count", "subscribers"])
        if bn and sc:
            hi = (nc_tbl.select(F.col(bn).cast("int").alias("batch"), F.col(sc).cast("double").alias("subs"))
                  .groupBy("batch").agg(F.sum(F.when(F.col("subs") >= 100000, 1).otherwise(0)).alias("new_ge_100k"),
                                        F.count(F.lit(1)).alias("new_total")).orderBy("batch").toPandas())
    if not yield_frames and hi is None:
        _warn("Figure 1: no discovery logs; skipping."); return
    if yield_frames:
        export_table(pd.concat([f.assign(source=k) for k, f in yield_frames.items()], ignore_index=True),
                     "fig1_discovery_batches", description="New channels discovered per crawl batch by source.")
    if hi is not None:
        export_table(hi, "fig1_high_attention_discovery", suppress_count_cols=["new_total"],
                     description="New channels >=100k subscribers per batch (high-subscriber head saturation; subscriber-threshold proxy, not attention-weighted).")
    fig, axes = plt.subplots(1, 2, figsize=(FIG_2COL, FIG_2COL * 0.36))
    ax = axes[0]
    plotted = False
    for k, pdf in yield_frames.items():
        if k != "pub_subs":
            continue
        pdf = pdf.sort_values("batch")
        ax.plot(pdf["batch"], pdf["new_channels"] / pdf["new_channels"].max(), marker="o", ms=2, lw=1.0, color=OKABE_ITO["blue"])
        plotted = True
    if not plotted:
        ax.text(0.5, 0.5, "pub-subs yield unavailable", ha="center", va="center", transform=ax.transAxes, color=GREY, fontsize=6)
    ax.set_xlabel("Crawl batch"); ax.set_ylabel("New channels per batch\n(fraction of peak)"); ax.set_ylim(0, 1.05)
    ax.set_title("Discovery yield falls toward zero", fontsize=7); panel_label(ax, "a")
    ax = axes[1]
    if hi is not None and len(hi):
        hi = hi.sort_values("batch")
        ax.plot(hi["batch"], hi["new_ge_100k"], marker="s", ms=2, lw=1.0, color=OKABE_ITO["vermillion"])
        ax.set_ylabel("New channels >=100k subscribers"); ax.yaxis.set_major_formatter(HUMAN_FMT)
    else:
        ax.text(0.5, 0.5, "high-attention batch data unavailable", ha="center", va="center", transform=ax.transAxes, color=GREY, fontsize=6)
    ax.set_xlabel("Batch"); ax.set_title("High-subscriber discovery saturates", fontsize=7); panel_label(ax, "b")
    fig.tight_layout()
    save_fig(fig, "figure1_discovery_saturation", description="Discovery saturation: per-batch yield and high-subscriber head saturation.")


if RUN_COMPUTE and FIG_CELLS_ENABLED:
    try:
        figure1_discovery()
    except Exception as exc:
        _fail("Figure 1", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Figure 2 | A map of public YouTube (full width)
# MAGIC True **language -> category -> channel** treemap from a bounded Spark aggregate. Within each shown
# MAGIC language, non-top categories are pooled into an "other categories" cell so the language rectangle is
# MAGIC fully tiled and **no attention mass is dropped**. Panels b/c: composition across traffic blocks and
# MAGIC major languages, with a Jensen-Shannon "one platform or many" statistic.

# COMMAND ----------
def _squarify(values, x, y, dx, dy):
    values = list(values); total = sum(values) or 1.0
    norm = [v * dx * dy / total for v in values]; rects = []
    def worst(row, length):
        s = sum(row); return max((length * length * max(row)) / (s * s), (s * s) / (length * length * min(row)))
    def layout_row(row, x, y, dx, dy):
        s = sum(row); out = []
        if dx >= dy:
            w = s / dy if dy else 0; cy = y
            for r in row:
                h = r / w if w else 0; out.append((x, cy, w, h)); cy += h
            return out, x + w, y, dx - w, dy
        h = s / dx if dx else 0; cx = x
        for r in row:
            w = r / h if h else 0; out.append((cx, y, w, h)); cx += w
        return out, x, y + h, dx, dy - h
    row = []; i = 0
    while i < len(norm):
        length = min(dx, dy)
        if not row:
            row = [norm[i]]; i += 1; continue
        if worst(row, length) >= worst(row + [norm[i]], length):
            row.append(norm[i]); i += 1
        else:
            placed, x, y, dx, dy = layout_row(row, x, y, dx, dy); rects += placed; row = []
    if row:
        placed, x, y, dx, dy = layout_row(row, x, y, dx, dy); rects += placed
    return rects


def figure2_map():
    if channel_ranked is None:
        _warn("Figure 2: channel_ranked unavailable; skipping."); return
    cr = channel_ranked

    lang_tot = (cr.where(F.col("language") != F.lit("und")).groupBy("language")
                .agg(F.sum("weekly_views").alias("wv")).orderBy(F.col("wv").desc()).limit(TREEMAP_TOP_LANGS).toPandas())
    top_langs = lang_tot["language"].tolist()
    inlang = cr.where(F.col("language").isin(top_langs))
    cat_all = inlang.groupBy("language", "category").agg(F.sum("weekly_views").alias("wv"))
    catw = Window.partitionBy("language").orderBy(F.col("wv").desc())
    cat_top = cat_all.withColumn("_r", F.row_number().over(catw)).where(F.col("_r") <= TREEMAP_TOP_CATS).drop("_r")
    cat_top_pdf = cat_top.toPandas()
    # Pooled "other categories" per language (conserves the language's full mass).
    lang_full = {r["language"]: float(r["wv"]) for _, r in lang_tot.iterrows()}
    cat_top_sum = cat_top_pdf.groupby("language")["wv"].sum().to_dict()
    other_cat_rows = [{"language": l, "category": "other categories", "wv": max(lang_full.get(l, 0) - cat_top_sum.get(l, 0), 0.0)}
                      for l in top_langs if lang_full.get(l, 0) - cat_top_sum.get(l, 0) > 0]
    cat_tot_pdf = pd.concat([cat_top_pdf, pd.DataFrame(other_cat_rows)], ignore_index=True) if other_cat_rows else cat_top_pdf

    # Channel cells within top (language, category) cells: label big channels, pool the rest.
    incell = inlang.join(cat_top.select("language", "category"), on=["language", "category"], how="inner")
    cell_tot = incell.groupBy("language", "category").agg(F.sum("weekly_views").alias("cell_wv"))
    incell = incell.join(cell_tot, on=["language", "category"], how="left").withColumn("within_share", F.col("weekly_views") / F.col("cell_wv"))
    # Named head-channel cells are intentional source data for the manuscript treemap (NOT a bulk channel
    # dump - everything below TREEMAP_LABEL_SHARE is pooled). Optionally anonymise to hashed IDs.
    _raw_label = F.coalesce(F.col("channel_name") if "channel_name" in incell.columns else F.lit(None), F.col("channel_id"))
    _label = F.concat(F.lit("ch_"), F.substring(F.sha2(F.col("channel_id"), 256), 1, 10)) if TREEMAP_ANONYMIZE_LABELS else _raw_label
    labeled = (incell.where(F.col("within_share") >= F.lit(TREEMAP_LABEL_SHARE))
               .select("language", "category", _label.alias("label"),
                       F.col("weekly_views").alias("wv"), F.lit("channel").alias("cell_type")))
    pooled = (incell.where(F.col("within_share") < F.lit(TREEMAP_LABEL_SHARE)).groupBy("language", "category")
              .agg(F.sum("weekly_views").alias("wv"), F.count(F.lit(1)).alias("n")).where(F.col("wv") > 0)
              .select("language", "category", F.concat(F.lit("other (n="), F.col("n").cast("string"), F.lit(")")).alias("label"),
                      F.col("wv"), F.lit("pooled_other").alias("cell_type")))
    treemap_cells = labeled.unionByName(pooled).toPandas()
    export_table(treemap_cells, "fig2_treemap_cells", description="Treemap channel cells + pooled 'other' per language x category.")
    # Mass conservation: a top-level "other languages" block for everything beyond the top languages
    # (incl. undetermined). Computed and exported here so the figure is reproducible from the source table.
    grand_total = cr.agg(F.sum("weekly_views")).first()[0] or 0.0
    other_lang_wv = max(grand_total - sum(lang_full.get(l, 0.0) for l in top_langs), 0.0)
    RUN_MANIFEST["fig2_other_languages_view_share"] = float(other_lang_wv / grand_total) if grand_total else None  # type: ignore
    cat_export = cat_tot_pdf.copy()
    if other_lang_wv > 0:
        cat_export = pd.concat([cat_export, pd.DataFrame([{"language": "other languages", "category": "(all)", "wv": other_lang_wv}])], ignore_index=True)
    export_table(cat_export, "fig2_language_category_views",
                 description="Weekly views by language x category (top + pooled 'other categories'; plus an 'other languages' row for mass conservation).")

    fig = plt.figure(figsize=(FIG_2COL, FIG_2COL * 0.64))
    gs = fig.add_gridspec(2, 2, height_ratios=[1.4, 1.0], width_ratios=[1, 1], hspace=0.45, wspace=0.28)
    ax = fig.add_subplot(gs[0, :]); ax.set_xlim(0, 100); ax.set_ylim(0, 100); ax.axis("off")
    panel_label(ax, "a", dx=-0.02, dy=1.02)
    ax.set_title("A map of public YouTube  -  weekly views by language -> category -> channel", fontsize=7.5, loc="left")
    lang_pal = stable_palette(top_langs)
    lvals = [lang_full.get(l, 0.0) for l in top_langs]
    layout_langs = top_langs + (["__other_langs__"] if other_lang_wv > 0 else [])
    layout_vals = lvals + ([other_lang_wv] if other_lang_wv > 0 else [])
    for lang, (lx, ly, ldx, ldy) in zip(layout_langs, _squarify(layout_vals, 0, 0, 100, 100)):
        if lang == "__other_langs__":
            ax.add_patch(Rectangle((lx, ly), ldx, ldy, facecolor=LIGHT_GREY, alpha=0.55, edgecolor="white", linewidth=1.1))
            if ldx * ldy > 28:
                ax.text(lx + ldx / 2, ly + ldy / 2, "other\nlanguages", ha="center", va="center", fontsize=6, fontweight="bold", color="white")
            continue
        cats = cat_tot_pdf[cat_tot_pdf["language"] == lang].sort_values("wv", ascending=False)
        if cats.empty:
            continue
        base = lang_pal[lang]
        for ci, (catname, (cx, cy, cdx, cdy)) in enumerate(zip(cats["category"], _squarify(cats["wv"].tolist(), lx, ly, ldx, ldy))):
            shade = 0.40 + 0.5 * (ci / max(len(cats) - 1, 1))
            cells = treemap_cells[(treemap_cells["language"] == lang) & (treemap_cells["category"] == catname)].sort_values("wv", ascending=False)
            if catname != "other categories" and not cells.empty and cdx * cdy > 4:
                for (lbl, ctype, (chx, chy, chdx, chdy)) in zip(cells["label"], cells["cell_type"], _squarify(cells["wv"].tolist(), cx, cy, cdx, cdy)):
                    ax.add_patch(Rectangle((chx, chy), chdx, chdy, facecolor=base, alpha=shade if ctype == "channel" else shade * 0.55,
                                           edgecolor="white", linewidth=0.2))
                    if ctype == "channel" and chdx * chdy > 22:
                        ax.text(chx + chdx / 2, chy + chdy / 2, str(lbl)[:18], ha="center", va="center", fontsize=4.2, color="white")
            else:
                ax.add_patch(Rectangle((cx, cy), cdx, cdy, facecolor=base, alpha=shade * (0.5 if catname == "other categories" else 1.0),
                                       edgecolor="white", linewidth=0.2))
        ax.add_patch(Rectangle((lx, ly), ldx, ldy, fill=False, edgecolor="white", linewidth=1.1))
        if ldx * ldy > 28:
            ax.text(lx + ldx / 2, ly + ldy / 2, lang, ha="center", va="center", fontsize=min(9, 5 + ldx * ldy / 220), fontweight="bold", color="white")

    ax = fig.add_subplot(gs[1, 0])
    comp = cr.groupBy("traffic_block", "category").agg(F.sum("weekly_views").alias("wv")).toPandas()
    top_cats = comp.groupby("category")["wv"].sum().sort_values(ascending=False).head(8).index.tolist()
    comp["cat2"] = np.where(comp["category"].isin(top_cats), comp["category"], "other")
    piv = comp.pivot_table(index="traffic_block", columns="cat2", values="wv", aggfunc="sum", fill_value=0).sort_index()
    piv = piv.div(piv.sum(axis=1), axis=0)
    export_table(piv.reset_index(), "fig2_block_category_composition", description="Category share of weekly views per 5% traffic block.")
    cat_pal = stable_palette(list(piv.columns)); bottom = np.zeros(len(piv))
    for c in piv.columns:
        ax.bar(piv.index, piv[c].values, bottom=bottom, width=0.9, color=cat_pal[c], label=c, linewidth=0); bottom += piv[c].values
    ax.set_xlabel("Traffic block (1 = top 5% of weekly views)"); ax.set_ylabel("Share of weekly views"); ax.set_ylim(0, 1)
    ax.set_title("Composition shifts across the distribution", fontsize=7)
    ax.legend(ncol=2, fontsize=4.6, loc="lower center", bbox_to_anchor=(0.5, -0.55)); panel_label(ax, "b")

    ax = fig.add_subplot(gs[1, 1]); big3 = top_langs[:3]
    # Include the pooled "other categories" mass (as "other") so the per-language composition and the JSD
    # are measured over each language's FULL distribution, not just its top categories.
    sub = cat_tot_pdf[cat_tot_pdf["language"].isin(big3)].copy()
    sub["cat2"] = np.where(sub["category"].isin(top_cats), sub["category"], "other")
    pv = sub.pivot_table(index="language", columns="cat2", values="wv", aggfunc="sum", fill_value=0).reindex(big3).fillna(0)
    rowsum = pv.sum(axis=1)
    pv_norm = pv.div(rowsum.where(rowsum > 0, 1.0), axis=0)   # avoid div-by-zero for an empty language row
    left = np.zeros(len(pv_norm)); ypos = np.arange(len(pv_norm))
    for c in pv_norm.columns:
        ax.barh(ypos, pv_norm[c].values, left=left, color=cat_pal.get(c, GREY), height=0.7, linewidth=0); left += pv_norm[c].values
    ax.set_yticks(ypos); ax.set_yticklabels(big3); ax.set_xlabel("Share of weekly views"); ax.set_xlim(0, 1)
    def _jsd(p, q):
        if p.sum() <= 0 or q.sum() <= 0:
            return np.nan
        p = p / p.sum(); q = q / q.sum(); m = 0.5 * (p + q)
        kl = lambda a, b: np.sum(a[a > 0] * np.log2(a[a > 0] / b[a > 0]))
        return 0.5 * kl(p, m) + 0.5 * kl(q, m)
    arr = pv.values   # raw (un-normalised) rows; _jsd normalises and guards empties
    jsds = [v for v in (_jsd(arr[i], arr[j]) for i in range(len(arr)) for j in range(i + 1, len(arr))) if not np.isnan(v)]
    mean_jsd = float(np.mean(jsds)) if jsds else float("nan")
    ax.set_title(f"Distinct language markets (mean JSD = {mean_jsd:.2f} bits)", fontsize=7); panel_label(ax, "c")
    RUN_MANIFEST["fig2_mean_language_jsd_bits"] = mean_jsd  # type: ignore
    save_fig(fig, "figure2_platform_map", description="Full-width language->category->channel map + composition.")


if RUN_COMPUTE and FIG_CELLS_ENABLED:
    try:
        figure2_map()
    except Exception as exc:
        _fail("Figure 2", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Figure 3 | Subscribers are an imprecise proxy for attention

# COMMAND ----------
def figure3_subscribers():
    if channel_master is None:
        _warn("Figure 3: channel_master unavailable; skipping."); return
    cm = (channel_master.where((F.col("subscribers") > 0) & (F.col("weekly_views") > 0))
          .withColumn("log_sub_bin", F.round(F.log10("subscribers") * 4) / 4)
          .withColumn("vps", F.col("weekly_views") / F.col("subscribers")))
    agg = (cm.groupBy("log_sub_bin").agg(
        F.count(F.lit(1)).alias("n"),
        F.expr("percentile_approx(weekly_views, 0.1, 1000)").alias("wv_p10"),
        F.expr("percentile_approx(weekly_views, 0.5, 1000)").alias("wv_p50"),
        F.expr("percentile_approx(weekly_views, 0.9, 1000)").alias("wv_p90"),
        F.expr("percentile_approx(vps, 0.25, 1000)").alias("vps_p25"),
        F.expr("percentile_approx(vps, 0.5, 1000)").alias("vps_p50"),
        F.expr("percentile_approx(vps, 0.75, 1000)").alias("vps_p75"),
        F.avg("vps").alias("vps_mean")).orderBy("log_sub_bin").toPandas())
    agg = agg[agg["n"] >= 20].copy(); agg["subscribers"] = 10 ** agg["log_sub_bin"]
    export_table(agg, "fig3_subscriber_view_dispersion", suppress_count_cols=["n"],
                 description="Weekly-views and views-per-subscriber distribution by subscriber bin.")
    corr = (cm.withColumn("ls", F.log10("subscribers")).withColumn("lv", F.log10("weekly_views"))
            .agg(F.corr("ls", "lv").alias("r")).first()["r"])
    corr = float(corr) if corr is not None else float("nan")
    RUN_MANIFEST["fig3_pearson_logsub_logweekly"] = corr  # type: ignore
    # Inactive share: fraction of channels with zero OR null weekly views, by subscriber bin  -  the missing
    # mass that proxy-failure makes substantively important (excluded from the dispersion panels above).
    inact = (channel_master.where(F.col("subscribers") > 0)
             .withColumn("log_sub_bin", F.round(F.log10("subscribers") * 4) / 4)
             .withColumn("is_inactive", (F.col("weekly_views").isNull() | (F.col("weekly_views") <= 0)).cast("int"))
             .groupBy("log_sub_bin").agg(F.count(F.lit(1)).alias("n"), F.avg("is_inactive").alias("inactive_share"))
             .orderBy("log_sub_bin").toPandas())
    inact = inact[inact["n"] >= 20].copy(); inact["subscribers"] = 10 ** inact["log_sub_bin"]
    export_table(inact, "fig3_inactive_share_by_subscriber_bin", suppress_count_cols=["n"],
                 description="Share of channels with zero/null weekly views by subscriber bin (proxy-failure mass).")
    fig, axes = plt.subplots(1, 3, figsize=(FIG_2COL, FIG_2COL * 0.32))
    ax = axes[0]
    ax.fill_between(agg["subscribers"], agg["wv_p10"], agg["wv_p90"], color=OKABE_ITO["skyblue"], alpha=0.35, label="10-90%")
    ax.plot(agg["subscribers"], agg["wv_p50"], color=OKABE_ITO["blue"], lw=1.3, label="median")
    ax.set_xscale("log"); ax.set_yscale("log"); ax.xaxis.set_major_formatter(HUMAN_FMT); ax.yaxis.set_major_formatter(HUMAN_FMT)
    ax.set_xlabel("Subscribers"); ax.set_ylabel("Weekly views")
    ax.set_title(f"Wide spread at every size (r = {corr:.2f})", fontsize=7); ax.legend(loc="upper left"); panel_label(ax, "a")
    ax = axes[1]
    ax.fill_between(agg["subscribers"], agg["vps_p25"], agg["vps_p75"], color=OKABE_ITO["orange"], alpha=0.30, label="IQR")
    ax.plot(agg["subscribers"], agg["vps_p50"], color=OKABE_ITO["vermillion"], lw=1.3, label="median")
    overall = float(np.average(agg["vps_mean"], weights=agg["n"])) if len(agg) else float("nan")
    ax.axhline(overall, color=GREY, ls="--", lw=0.8, label="overall mean")
    ax.set_xscale("log"); ax.set_yscale("log"); ax.xaxis.set_major_formatter(HUMAN_FMT)
    ax.set_xlabel("Subscribers"); ax.set_ylabel("Weekly views per subscriber")
    ax.set_title("Views per subscriber are not constant", fontsize=7); ax.legend(loc="upper right"); panel_label(ax, "b")
    ax = axes[2]
    if len(inact):
        ax.plot(inact["subscribers"], inact["inactive_share"] * 100, color=OKABE_ITO["green"], lw=1.3, marker="o", ms=2)
        ax.set_xscale("log"); ax.xaxis.set_major_formatter(HUMAN_FMT); ax.set_ylim(0, max(5, inact["inactive_share"].max() * 110))
    ax.set_xlabel("Subscribers"); ax.set_ylabel("Channels with no measurable\nweekly views (%)")
    ax.set_title("Inactive / unmeasured share", fontsize=7); panel_label(ax, "c")
    fig.tight_layout()
    save_fig(fig, "figure3_subscriber_proxy", description="Subscriber count is a noisy proxy; plus the inactive/unmeasured share by size.")


if RUN_COMPUTE and FIG_CELLS_ENABLED:
    try:
        figure3_subscribers()
    except Exception as exc:
        _fail("Figure 3", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Figure 4 | Thresholds capture unequal shares
# MAGIC Cumulative capture from a scalable log-binned aggregate. Threshold capture shares are **within the
# MAGIC observed >=10k universe** (axis labelled as such); the floor threshold (=10k) is dropped from the bar
# MAGIC panel because it is 100% by construction. Sub-floor thresholds (1k) are status placeholders, not
# MAGIC estimates. The tier panel shows the observed bar and marks design/bounded as **unknown (not in
# MAGIC panel)**  -  not zero.

# COMMAND ----------
def _weekly_view_value_bins(cr, nbins: int, valcol: str = "weekly_views"):
    """Scalable log-value-binned aggregate of any positive value column (no global sort)."""
    pos = cr.where(F.col(valcol) > 0).withColumn("lv", F.log10(valcol))
    mn, mx = pos.agg(F.min("lv").alias("a"), F.max("lv").alias("b")).first()
    if mn is None:
        return None
    width = (mx - mn) / nbins if mx > mn else 1.0
    binned = (pos.withColumn("bin", F.least(F.floor((F.col("lv") - F.lit(mn)) / F.lit(width)).cast("int"), F.lit(nbins - 1)))
              .groupBy("bin").agg(F.count(F.lit(1)).alias("n"), F.sum(valcol).alias("wv")).orderBy("bin").toPandas())
    binned["bin_value"] = 10 ** (mn + (binned["bin"] + 0.5) * width)
    return binned


def binned_top_share(cr, valcol: str, frac: float = 0.01, nbins: int = 1000) -> float:
    """Top-`frac` channels' share of total `valcol`, via log-value bins  -  avoids a global orderBy."""
    binned = _weekly_view_value_bins(cr, nbins, valcol=valcol)
    if binned is None or not len(binned):
        return float("nan")
    d = binned.sort_values("bin_value", ascending=False)
    tot_n = d["n"].sum(); tot_v = d["wv"].sum()
    if not tot_n or not tot_v:
        return float("nan")
    # Anchor at (0,0) so a `frac` smaller than the top bin interpolates within that bin (uniform
    # within-bin assumption) instead of returning the whole first bin's share.
    xp = np.r_[0.0, (d["n"].cumsum() / tot_n).values]
    fp = np.r_[0.0, (d["wv"].cumsum() / tot_v).values]
    return float(np.interp(frac, xp, fp))


def figure4_thresholds():
    if channel_master is None:
        _warn("Figure 4: channel_master unavailable; skipping."); return
    cm = channel_master
    # Denominators/counts use the FULL observed >=10k universe (channel_master), not the positive-only
    # ranked frame, so the =10k row is exactly 100% and counts mean "all observed channels", not
    # "channels with positive measurable weekly views". weekly_views is coalesced to 0 for inactive/new.
    cm_wv = cm.withColumn("wv0", F.coalesce(F.col("weekly_views"), F.lit(0.0)))
    n_total = cm_wv.count()
    total_wv = cm_wv.agg(F.sum("wv0")).first()[0] or 0.0

    binned = _weekly_view_value_bins(cm, LORENZ_BINS)
    if binned is None or not len(binned):
        _warn("Figure 4: no positive weekly views; skipping."); return
    b = binned.sort_values("bin_value", ascending=False).copy()
    b["cum_channels"] = b["n"].cumsum(); b["cum_views"] = b["wv"].cumsum()
    b["channel_pct"] = b["cum_channels"] / b["cum_channels"].iloc[-1]
    b["view_share"] = b["cum_views"] / b["cum_views"].iloc[-1]
    export_table(b[["bin_value", "n", "wv", "channel_pct", "view_share"]], "fig4_cumulative_capture",
                 description="Cumulative weekly-view share vs cumulative channel share (log value-binned; "
                             "channels with positive weekly views).")

    rows = []
    for thr in SUB_THRESHOLDS:
        if thr < TOO_FLOOR:
            rows.append({"threshold": thr, "channels": np.nan, "share_of_observed_channels": np.nan,
                         "share_of_observed_views": np.nan, "tier": "design",
                         "status": "below_panel_floor_requires_threshold_strata_or_estimator"})
            continue
        sub = cm_wv.where(F.col("subscribers") >= F.lit(float(thr)))
        nc = sub.count(); vw = sub.agg(F.sum("wv0")).first()[0] or 0.0
        rows.append({"threshold": thr, "channels": nc,
                     "share_of_observed_channels": nc / n_total if n_total else np.nan,
                     "share_of_observed_views": vw / total_wv if total_wv else np.nan,
                     "tier": "observed", "status": "observed_within_10k_universe"})
    thr_pdf = pd.DataFrame(rows)
    export_table(thr_pdf, "table1_threshold_capture",
                 description="Table 1: channel counts/shares are over the FULL observed >=10k universe "
                             "(channel_master); view shares coalesce inactive channels to 0. Sub-floor rows are "
                             "status placeholders; the =10k row is 100% by construction.")

    # Per-block actual view-share diagnostic (the value-binned blocks are APPROXIMATE 5% blocks).
    if channel_ranked is not None:
        blk = (channel_ranked.groupBy("traffic_block").agg(F.sum("weekly_views").alias("wv"), F.count(F.lit(1)).alias("n"))
               .toPandas().sort_values("traffic_block"))
        blk["actual_view_share"] = blk["wv"] / (blk["wv"].sum() or 1.0)
        export_table(blk, "fig4_block_actual_view_share", suppress_count_cols=["n"],
                     description="Actual weekly-view share per (approximate 5%) traffic block  -  value-binned, not exact ntiles.")

    # Bar panel: observed thresholds strictly above the floor (exclude the trivially-100% =floor row).
    obs = thr_pdf[(thr_pdf["tier"] == "observed") & (thr_pdf["threshold"] > TOO_FLOOR)]
    fig, axes = plt.subplots(1, 3, figsize=(FIG_2COL, FIG_2COL * 0.30))
    ax = axes[0]
    ax.plot(b["channel_pct"] * 100, b["view_share"] * 100, color=OKABE_ITO["blue"], lw=1.3)
    ax.plot([0, 100], [0, 100], color=LIGHT_GREY, ls=":", lw=0.8)
    ax.set_xlabel("Positive-attention channels (cum. %, ranked by weekly views)"); ax.set_ylabel("Weekly views captured (%)")
    ax.set_title("A few channels capture most attention", fontsize=7); panel_label(ax, "a")
    ax = axes[1]
    if len(obs):
        x = np.arange(len(obs)); wbar = 0.38
        ax.bar(x - wbar/2, obs["share_of_observed_channels"] * 100, wbar, color=OKABE_ITO["skyblue"], label="% of channels")
        ax.bar(x + wbar/2, obs["share_of_observed_views"] * 100, wbar, color=OKABE_ITO["vermillion"], label="% of weekly views")
        ax.set_xticks(x); ax.set_xticklabels([thousands(t) + "+" for t in obs["threshold"]])
        ax.legend(loc="upper right")
    ax.set_xlabel("Subscriber threshold (> floor)"); ax.set_ylabel("Share of observed >=10k universe (%)")
    ax.set_title("Thresholds capture unequal shares", fontsize=7); panel_label(ax, "b")
    ax = axes[2]
    tier_src = channel_ranked if channel_ranked is not None else cm_wv.withColumn("tier", F.lit("observed")).withColumnRenamed("wv0", "weekly_views")
    tier_mass = (tier_src.groupBy("tier").agg(F.sum("weekly_views").alias("wv")).toPandas().set_index("tier"))
    obs_mass = float(tier_mass["wv"].get("observed", 0.0))
    export_table(tier_mass.reset_index(), "fig4_tier_mass", description="Weekly-view mass by inferential tier (observed only in this panel).")
    order = ["observed", "design", "bounded"]
    # Only the observed tier has a measured magnitude; design/bounded get a baseline "?" marker (no bar
    # height) so the visual never implies a magnitude for the unknown tiers.
    ax.bar(0, obs_mass, color=TIER_COLORS["observed"])
    for i, t in enumerate(order):
        if t == "observed":
            continue
        ax.scatter([i], [0], marker="x", s=22, color=TIER_COLORS[t], zorder=3, clip_on=False)
        ax.annotate("unknown\n(not in panel)", (i, 0), xytext=(0, 10), textcoords="offset points",
                    ha="center", va="bottom", fontsize=4.6, color=GREY)
    ax.set_xlim(-0.6, 2.6)
    ax.set_xticks(range(3)); ax.set_xticklabels(order); ax.set_ylabel("Weekly views (observed scale)")
    ax.yaxis.set_major_formatter(HUMAN_FMT); ax.set_title("Observed / design / bounded", fontsize=7)
    ax.text(0.5, -0.42, "design (1k-10k) and bounded (<1k) are UNKNOWN here, not zero; they require the\n"
                        "threshold strata / external estimator.", transform=ax.transAxes, ha="center", va="top", fontsize=4.4, color=GREY)
    panel_label(ax, "c")
    fig.tight_layout()
    save_fig(fig, "figure4_threshold_capture", description="Threshold capture (observed universe) + observed/design/bounded.")


if RUN_COMPUTE and FIG_CELLS_ENABLED:
    try:
        figure4_thresholds()
    except Exception as exc:
        _fail("Figure 4", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Figure 5 | Age structure + JNS alternative (language rank vs engagement & population)

# COMMAND ----------
SPEAKER_POP_M = {"eng": 1500, "cmn": 1100, "zho": 1100, "hin": 610, "spa": 560, "fra": 310, "ara": 360,
                 "ben": 280, "rus": 255, "por": 260, "ind": 200, "urd": 230, "deu": 135, "jpn": 125,
                 "pcm": 120, "mar": 100, "tel": 96, "tur": 88, "tam": 85, "vie": 100, "kor": 82, "ita": 65, "tha": 61}


def figure5_age_and_language():
    made_any = False
    if channel_ranked is not None:
        cr = channel_ranked
        if cr.where(F.col("founding_year").isNotNull()).limit(1).count() > 0:
            age = (cr.where(F.col("founding_year").isNotNull()).groupBy("traffic_block").agg(
                F.expr("percentile_approx(founding_year, 0.5, 1000)").alias("median_year"),
                F.expr("percentile_approx(founding_year, 0.25, 1000)").alias("y25"),
                F.expr("percentile_approx(founding_year, 0.75, 1000)").alias("y75"),
                F.count(F.lit(1)).alias("n")).orderBy("traffic_block").toPandas())
            export_table(age, "fig5_age_by_traffic_block", suppress_count_cols=["n"], description="Founding-year quartiles by traffic block.")
            fig, ax = plt.subplots(figsize=(FIG_1COL, FIG_1COL * 0.7))
            ax.fill_between(age["traffic_block"], age["y25"], age["y75"], color=OKABE_ITO["skyblue"], alpha=0.35)
            ax.plot(age["traffic_block"], age["median_year"], color=OKABE_ITO["blue"], marker="o", ms=2)
            ax.set_xlabel("Traffic block (1 = top 5%)"); ax.set_ylabel("Channel founding year")
            ax.set_title("Top-attention channels are older", fontsize=7)
            save_fig(fig, "figure5_age_structure", description="Founding year by traffic block."); made_any = True
        else:
            _warn("Figure 5a: no explicit channel creation date; age figure skipped (set channel_created_column).")

        lang = (cr.where(F.col("language") != F.lit("und")).groupBy("language").agg(
            F.sum("weekly_views").alias("weekly_views"), F.countDistinct("channel_id").alias("channels"),
            F.first("language_iso", ignorenulls=True).alias("language_iso")).toPandas())
        if len(lang):
            # Key on ISO-639-3 (more robust than the Flores-style rollup label, e.g. 'eng_Latn').
            lang["iso3"] = lang["language_iso"].fillna(lang["language"]).astype(str).str[:3].str.lower()
            lang["pop_m"] = lang["iso3"].map(SPEAKER_POP_M)
            lang = lang.sort_values("weekly_views", ascending=False).reset_index(drop=True)
            lang["rank"] = np.arange(1, len(lang) + 1)
            lang["views_per_capita"] = lang["weekly_views"] / (lang["pop_m"] * 1e6)
            export_table(lang.drop(columns=["language_iso"]), "fig5alt_language_rank_engagement", suppress_count_cols=["channels"],
                         description="Per-language weekly views, channels, speaker population, views/capita.")
            sub = lang.dropna(subset=["pop_m"]).head(20)
            fig, axes = plt.subplots(1, 2, figsize=(FIG_2COL, FIG_2COL * 0.30))
            ax = axes[0]; ax.loglog(sub["rank"], sub["weekly_views"], "o", ms=3, color=OKABE_ITO["blue"])
            for _, r in sub.head(8).iterrows():
                ax.annotate(r["language"], (r["rank"], r["weekly_views"]), fontsize=5, xytext=(2, 2), textcoords="offset points")
            ax.set_xlabel("Language rank (by weekly views)"); ax.set_ylabel("Weekly views"); ax.yaxis.set_major_formatter(HUMAN_FMT)
            ax.set_title("Attention by language market", fontsize=7); panel_label(ax, "a")
            ax = axes[1]; ax.loglog(sub["pop_m"], sub["weekly_views"], "o", ms=3, color=OKABE_ITO["vermillion"])
            for _, r in sub.head(10).iterrows():
                ax.annotate(r["language"], (r["pop_m"], r["weekly_views"]), fontsize=5, xytext=(2, 2), textcoords="offset points")
            ax.set_xlabel("Speaker population (millions, approx.)"); ax.set_ylabel("Weekly views"); ax.yaxis.set_major_formatter(HUMAN_FMT)
            ax.set_title("Engagement vs population", fontsize=7); panel_label(ax, "b")
            fig.tight_layout()
            save_fig(fig, "figure5alt_language_rank_engagement", description="JNS alternative: language rank vs engagement & population.")
            made_any = True
    if not made_any:
        _warn("Figure 5: neither panel could be built.")


if RUN_COMPUTE and FIG_CELLS_ENABLED:
    try:
        figure5_age_and_language()
    except Exception as exc:
        _fail("Figure 5", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Figure 6 | Production and attention diverge by format
# MAGIC Aggregated entirely in Spark. The video frame is already restricted to the anchor-consistent lookback
# MAGIC (section 3.5). Shorts/long-form use different counting regimes; the Shorts rule changed 31 Mar 2025.

# COMMAND ----------
def figure6_production():
    if video_frame is None or channel_ranked is None:
        _warn("Figure 6: video_frame/channel_ranked unavailable; skipping."); return
    vf = video_frame
    if "published_at" in vf.columns and RUN_MANIFEST.get("snapshot_current"):
        cutoff = (datetime.strptime(RUN_MANIFEST["snapshot_current"], "%Y-%m-%d") - timedelta(weeks=PROD_WINDOW_WEEKS)).date().isoformat()
        recent = vf.where(F.col("published_at") >= F.lit(cutoff))
    else:
        recent = vf
    up = recent.groupBy("channel_id", "format").agg(F.count(F.lit(1)).alias("uploads"))
    # Production intensity must count zero-upload channels too: cross every ranked channel with both formats
    # and fill missing uploads with 0, so panel b is uploads-per-channel-in-block, not uploads-per-uploader.
    fmts_df = spark.createDataFrame(pd.DataFrame({"format": ["Shorts", "Long-form"]}))
    merged = (channel_ranked.select("channel_id", "weekly_views", "traffic_block").crossJoin(F.broadcast(fmts_df))
              .join(up, on=["channel_id", "format"], how="left")
              .withColumn("uploads", F.coalesce(F.col("uploads"), F.lit(0))))
    fmt_colors = {"Shorts": OKABE_ITO["vermillion"], "Long-form": OKABE_ITO["blue"]}
    fig, axes = plt.subplots(1, 3, figsize=(FIG_2COL, FIG_2COL * 0.30))
    sd = (merged.where((F.col("uploads") > 0) & (F.col("weekly_views") > 0))
          .withColumn("ubin", F.round(F.log10("uploads") * 3) / 3)
          .groupBy("format", "ubin").agg(F.expr("percentile_approx(uploads, 0.5, 1000)").alias("uploads_med"),
                                         F.expr("percentile_approx(weekly_views, 0.5, 1000)").alias("weekly_views_med"),
                                         F.count(F.lit(1)).alias("n")).where(F.col("n") >= 10).toPandas())
    if len(sd):
        export_table(sd, "fig6_supply_demand", suppress_count_cols=["n"], description="Median weekly views vs uploads, by format.")
        for fmt, g in sd.sort_values("uploads_med").groupby("format"):
            axes[0].plot(g["uploads_med"], g["weekly_views_med"], "o-", ms=2.5, lw=0.8, color=fmt_colors.get(fmt, GREY), label=fmt)
        axes[0].set_xscale("log"); axes[0].set_yscale("log"); axes[0].xaxis.set_major_formatter(HUMAN_FMT); axes[0].yaxis.set_major_formatter(HUMAN_FMT)
        axes[0].set_xlabel(f"Uploads in trailing {PROD_WINDOW_WEEKS} weeks"); axes[0].set_ylabel("Weekly views")
        axes[0].set_title("Supply and demand diverge", fontsize=7); axes[0].legend(loc="upper left")
    panel_label(axes[0], "a")
    pib = (merged.where(F.col("traffic_block").isNotNull()).groupBy("traffic_block", "format")
           .agg(F.expr("percentile_approx(uploads, 0.5, 1000)").alias("uploads_med")).toPandas())
    if len(pib):
        export_table(pib, "fig6_production_intensity_by_block", description="Median uploads per channel by traffic block and format (zero-upload channels included).")
        for fmt, g in pib.groupby("format"):
            g = g.sort_values("traffic_block")
            axes[1].plot(g["traffic_block"], g["uploads_med"], "o-", ms=2.5, lw=0.9, color=fmt_colors.get(fmt, GREY), label=fmt)
        axes[1].set_xlabel("Traffic block"); axes[1].set_ylabel(f"Median uploads / {PROD_WINDOW_WEEKS}w")
        axes[1].set_title("Production intensity by attention rank", fontsize=7); axes[1].legend()
    panel_label(axes[1], "b")
    ax = axes[2]
    fmt_share = vf.groupBy("format").agg(F.sum("video_views").alias("views")).toPandas()
    if fmt_share["views"].notna().any() and fmt_share["views"].sum() > 0:
        export_table(fmt_share, "fig6_format_view_share_cumulative_recent_uploads",
                     description="CUMULATIVE video views (latest snapshot) on videos UPLOADED within the lookback "
                                 "window, by format. NOT weekly attention and NOT all attention (excludes older "
                                 "videos still accruing views). Relabel/recompute as video deltas when available.")
        shares = fmt_share.set_index("format")["views"]; shares = shares / shares.sum() * 100
        ax.bar(range(len(shares)), shares.values, color=[fmt_colors.get(f, GREY) for f in shares.index])
        ax.set_xticks(range(len(shares))); ax.set_xticklabels(shares.index)
        ax.set_ylabel(f"Cumulative views on uploads\nfrom last {FORMAT_LOOKBACK_DAYS}d (%)")
    else:
        ax.text(0.5, 0.5, "video views unavailable", ha="center", va="center", transform=ax.transAxes, color=GREY, fontsize=6)
    ax.set_title("Format mix of recent uploads", fontsize=7); panel_label(ax, "c")
    ax.text(0.5, -0.4, "Cumulative views on recent uploads (not weekly deltas); counting regimes differ; "
                       "Shorts rule changed 31 Mar 2025.", transform=ax.transAxes, ha="center", va="top", fontsize=4.4, color=GREY)
    fig.tight_layout()
    save_fig(fig, "figure6_production_format", description="Production and attention diverge by format.")


if RUN_COMPUTE and FIG_CELLS_ENABLED:
    try:
        figure6_production()
    except Exception as exc:
        _fail("Figure 6", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Extended Data figures & robustness battery

# COMMAND ----------
# MAGIC %md
# MAGIC ### ED Fig. 3 | Concentration (Lorenz / Gini / top-k)  -  scalable, Spark-binned

# COMMAND ----------
def ed_concentration():
    if channel_ranked is None:
        _warn("ED concentration: unavailable; skipping."); return
    binned = _weekly_view_value_bins(channel_ranked, max(LORENZ_BINS, 500))
    if binned is None or not len(binned):
        _warn("ED concentration: no positive views; skipping."); return
    asc = binned.sort_values("bin_value").copy()
    asc["cum_n"] = asc["n"].cumsum(); asc["cum_v"] = asc["wv"].cumsum()
    pop = np.r_[0.0, (asc["cum_n"] / asc["cum_n"].iloc[-1]).values]
    lor = np.r_[0.0, (asc["cum_v"] / asc["cum_v"].iloc[-1]).values]
    gini = 1 - 2 * np.trapz(lor, pop)
    export_table(pd.DataFrame({"pop_share": pop, "view_share": lor}), "edfig3_lorenz",
                 description=f"Lorenz curve of weekly views among POSITIVE-ATTENTION channels (Gini={gini:.3f}, "
                             "log value-binned). Denominator = channels with weekly_views>0, not all observed.")
    desc = binned.sort_values("bin_value", ascending=False).copy()
    desc["cum_n"] = desc["n"].cumsum(); desc["cum_v"] = desc["wv"].cumsum()
    tot_n = desc["cum_n"].iloc[-1]; tot_v = desc["cum_v"].iloc[-1]
    topk_xp = np.r_[0.0, (desc["cum_n"] / tot_n).values]   # (0,0) anchor for sub-first-bin fractions
    topk_fp = np.r_[0.0, (desc["cum_v"] / tot_v).values]
    topk = [{"top_frac": k, "view_share": float(np.interp(k, topk_xp, topk_fp))}
            for k in [0.001, 0.004, 0.01, 0.05, 0.1]]
    export_table(pd.DataFrame(topk), "edfig3_topk_shares",
                 description="Top-k weekly-view shares among POSITIVE-ATTENTION channels (interpolated).")
    RUN_MANIFEST["gini_weekly_views_positive_attention_channels"] = float(gini)  # type: ignore
    fig, ax = plt.subplots(figsize=(FIG_1COL, FIG_1COL * 0.85))
    ax.plot(pop, lor, color=OKABE_ITO["blue"], lw=1.3); ax.fill_between(pop, pop, lor, color=OKABE_ITO["skyblue"], alpha=0.25)
    ax.plot([0, 1], [0, 1], ls=":", color=LIGHT_GREY, lw=0.8)
    ax.set_xlabel("Cumulative share of positive-attention channels"); ax.set_ylabel("Cumulative share of weekly views")
    ax.set_title(f"Concentration among active channels (Gini = {gini:.3f})", fontsize=7)
    save_fig(fig, "edfig3_concentration", description="Lorenz/Gini of weekly views among positive-attention (active) channels.")


if RUN_COMPUTE and RUN_ROBUSTNESS:
    try:
        ed_concentration()
    except Exception as exc:
        _fail("ED concentration", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### ED Fig. 6 | Format / lookback calibration

# COMMAND ----------
def ed_lookback_calibration():
    if video_frame is None or "published_at" not in video_frame.columns or not RUN_MANIFEST.get("snapshot_current"):
        _warn("ED lookback: prerequisites unavailable; skipping."); return
    asof = datetime.strptime(RUN_MANIFEST["snapshot_current"], "%Y-%m-%d").date().isoformat()
    vf = (video_frame.where(F.col("video_views").isNotNull() & (F.col("video_views") >= 0))
          .withColumn("age_days", F.datediff(F.lit(asof), F.col("published_at"))).where(F.col("age_days") >= 0))
    rows = []
    for fmt in ["Shorts", "Long-form"]:
        sub = vf.where(F.col("format") == F.lit(fmt)); tot = sub.agg(F.sum("video_views")).first()[0] or 0.0
        if tot <= 0:
            continue
        byage = (sub.withColumn("agew", F.floor(F.col("age_days") / 7)).groupBy("agew").agg(F.sum("video_views").alias("v")).orderBy("agew").toPandas())
        byage["cum"] = byage["v"].cumsum() / tot; byage["format"] = fmt; rows.append(byage)
    if not rows:
        _warn("ED lookback: no per-format mass; skipping."); return
    cal = pd.concat(rows, ignore_index=True)
    # NOTE: this cumulates *cumulative (lifetime) video views* by upload age  -  i.e. how lifetime views on
    # recent uploads distribute by age  -  NOT the share of views *accrued within* a lookback window (which
    # needs video-level deltas). Labelled accordingly; treat as an upload-age distribution, not a true
    # recent-view-capture calibration.
    export_table(cal[["agew", "cum", "format"]], "edfig6_upload_age_view_distribution",
                 description="Cumulative LIFETIME video views by upload age (weeks), per format. NOT within-window "
                             "recent-view capture (needs video deltas).")
    fig, ax = plt.subplots(figsize=(FIG_1COL, FIG_1COL * 0.8))
    for fmt, g in cal.groupby("format"):
        ax.plot(g["agew"], g["cum"], lw=1.2, label=fmt, color={"Shorts": OKABE_ITO["vermillion"], "Long-form": OKABE_ITO["blue"]}.get(fmt, GREY))
    ax.set_xlabel("Upload age (weeks)"); ax.set_ylabel("Cumulative share of lifetime views\non recent uploads"); ax.set_ylim(0, 1)
    ax.legend(); ax.set_title("Lifetime views by upload age (not recent-view capture)", fontsize=6.5)
    save_fig(fig, "edfig6_upload_age_view_distribution", description="Cumulative lifetime views by upload age, by format (not a recent-view capture calibration).")


if RUN_COMPUTE and RUN_ROBUSTNESS:
    try:
        ed_lookback_calibration()
    except Exception as exc:
        _fail("ED lookback", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Robustness battery
# MAGIC R1 negative-delta policy  |  R3 suspicion-flag exclusion  |  R4 language gate  |  R5 weekly-vs-lifetime
# MAGIC concentration  |  cross-check vs `too_run_summary.spearman_rho`.

# COMMAND ----------
def robustness_battery():
    if channel_master is None or channel_ranked is None:
        _warn("Robustness: core frames unavailable; skipping."); return
    out = []
    # Top-1% concentration via log-value bins (scalable; no global orderBy/limit).
    top_share = lambda df, valcol, frac=0.01: binned_top_share(df, valcol, frac)
    base = channel_week
    if base is not None and "raw_delta" in base.columns:
        scale = 7.0 / float(RUN_MANIFEST.get("snapshot_elapsed_days") or 7)
        for pol in ["floor_zero", "drop", "keep"]:
            if pol == "drop":
                # drop: negative deltas -> null (unmeasured), row retained - matches the core metric's policy
                d = base.withColumn("v", F.when(F.col("raw_delta") < 0, F.lit(None).cast("double"))
                                     .otherwise(F.col("raw_delta") * F.lit(scale)))
            elif pol == "keep":
                d = base.withColumn("v", F.col("raw_delta") * F.lit(scale))
            else:  # floor_zero: mirror the core metric  -  missing prior stays null (unmeasured), not zero
                d = base.withColumn("v", F.when(F.col("raw_delta").isNull(), F.lit(None).cast("double"))
                                     .otherwise(F.greatest(F.col("raw_delta"), F.lit(0.0)) * F.lit(scale)))
            out.append({"check": "R1_neg_delta_policy", "variant": pol, "metric_name": "top1pct_view_share_positive_attention",
                        "metric_value": float(top_share(d.where(F.col("v") > 0), "v"))})
    susp = safe_table(T_SUSPICION)
    skey = first_col(susp, ["channel_id", "canonical_id", "channel"]) if susp is not None else None
    if susp is not None and skey:
        flagcol = first_col(susp, ["n_flags_fired", "composite_score_norm"])
        flagged = (susp.where(F.col(flagcol) > 0) if flagcol else susp).select(F.col(skey).cast("string").alias("channel_id")).distinct()
        clean = channel_ranked.join(flagged, on="channel_id", how="left_anti")
        out.append({"check": "R3_exclude_suspicion", "variant": "all", "metric_name": "top1pct_view_share_positive_attention", "metric_value": float(top_share(channel_ranked, "weekly_views"))})
        out.append({"check": "R3_exclude_suspicion", "variant": "suspicion_excluded", "metric_name": "top1pct_view_share_positive_attention", "metric_value": float(top_share(clean, "weekly_views"))})
    out.append({"check": "R4_language_gate", "variant": f"min_segments={MIN_LANG_SEGMENTS}", "metric_name": "language_coverage", "metric_value": float(RUN_MANIFEST.get("language_coverage", np.nan))})
    out.append({"check": "R5_attention_window", "variant": "weekly_views", "metric_name": "top1pct_view_share_positive_attention", "metric_value": float(top_share(channel_master.where(F.col("weekly_views") > 0), "weekly_views"))})
    out.append({"check": "R5_attention_window", "variant": "lifetime_views", "metric_name": "top1pct_view_share_positive_attention", "metric_value": float(top_share(channel_master.where(F.col("views_cur") > 0), "views_cur"))})
    rob = pd.DataFrame(out)
    export_table(rob, "robustness_summary", description="Robustness checks in long form (check / variant / metric_name / metric_value); metrics are not all the same quantity.")
    rs = safe_table(T_RUN_SUMMARY)
    if rs is not None and "spearman_rho" in [c.lower() for c in rs.columns]:
        try:
            ts_col = first_col(rs, ["run_ts", "run_timestamp", "created_at", "run_date"])
            rs_latest = rs.orderBy(F.col(ts_col).desc()) if ts_col else rs   # only order by a recency col if one exists
            diag = rs_latest.limit(1).select("spearman_rho").collect()[0][0]
            RUN_MANIFEST["crosscheck_diagnostics_spearman"] = float(diag)  # type: ignore
            print(f"  cross-check: our Pearson(logsub,logweekly)={RUN_MANIFEST.get('fig3_pearson_logsub_logweekly')} vs too_run_summary.spearman_rho={diag}")
        except Exception as exc:
            _warn(f"cross-check spearman failed: {exc}")
    print(rob.to_string(index=False))


if RUN_COMPUTE and RUN_ROBUSTNESS:
    try:
        robustness_battery()
    except Exception as exc:
        _fail("Robustness battery", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### R7 | Category-missingness bounds
# MAGIC Per-category weekly-view share under three assumptions about the uncategorized (~20%, mostly
# MAGIC `pending`) mass: lower (none is this category), MAR (`wv / known`  -  unknowns split like the known
# MAGIC categories), adversarial upper (all of it is this category).

# COMMAND ----------
def robustness_category_bounds():
    if channel_ranked is None:
        _warn("R7: channel_ranked unavailable; skipping."); return
    cr = channel_ranked; total = RUN_MANIFEST.get("total_weekly_views", 0.0) or 1.0
    miss = cr.where(F.col("category") == F.lit("uncategorized")).agg(F.sum("weekly_views")).first()[0] or 0.0
    bycat = (cr.where(F.col("category") != F.lit("uncategorized")).groupBy("category").agg(F.sum("weekly_views").alias("wv")).toPandas())
    if not len(bycat):
        _warn("R7: no categorized mass; skipping."); return
    bycat = bycat.sort_values("wv", ascending=False); known = bycat["wv"].sum() or 1.0
    rows = [{"category": r["category"], "share_observed": r["wv"] / known,
             "share_lower": r["wv"] / total,                 # none of the missing mass is this category
             "share_mar": r["wv"] / known,                   # MAR: unknowns distribute like known categories
             "share_upper": (r["wv"] + miss) / total}        # adversarial: all missing mass is this category
            for _, r in bycat.head(10).iterrows()]
    export_table(pd.DataFrame(rows), "r7_category_missingness_bounds",
                 description="Per-category weekly-view share: lower / MAR(wv/known) / adversarial-upper for the ~20% missing.")
    RUN_MANIFEST["uncategorized_view_share"] = float(miss / total)  # type: ignore


if RUN_COMPUTE and RUN_ROBUSTNESS:
    try:
        robustness_category_bounds()
    except Exception as exc:
        _fail("R7", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### R8 | Cross-check vs the TOO diagnostics layer
# MAGIC Correlate our recomputed weekly attention (annualised) with the diagnostics layer's `views_past_year`
# MAGIC on overlapping channels - a sanity check that we are not assuming the TOO layer is ground truth.

# COMMAND ----------
def robustness_too_crosscheck():
    if channel_master is None:
        _warn("R8: channel_master unavailable; skipping."); return
    rc = safe_table(f"{DIAG_CAT}.{DIAG_SCH}.too_rank_comparison")
    if rc is None:
        _warn("R8: too_rank_comparison unavailable; skipping."); return
    rkey = first_col(rc, ["channel_id", "canonical_id", "channel"]); vpy = first_col(rc, ["views_past_year"])
    if not (rkey and vpy):
        _warn("R8: too_rank_comparison missing channel key or views_past_year; skipping."); return
    diag = rc.select(F.col(rkey).cast("string").alias("channel_id"), F.col(vpy).cast("double").alias("views_past_year")).dropDuplicates(["channel_id"])
    j = (channel_master.where(F.col("weekly_views") > 0).select("channel_id", "weekly_views")
         .join(diag.where(F.col("views_past_year") > 0), on="channel_id", how="inner")
         .withColumn("annualized_weekly", F.col("weekly_views") * F.lit(52.0))
         .withColumn("lr_annual", F.log10("annualized_weekly")).withColumn("lr_pastyear", F.log10("views_past_year")))
    summ = j.agg(F.count(F.lit(1)).alias("n_overlap"),
                 F.corr("lr_annual", "lr_pastyear").alias("pearson_log_annualized_vs_pastyear"),
                 F.expr("percentile_approx(annualized_weekly/views_past_year, array(0.1,0.5,0.9), 1000)").alias("ratio_p10_p50_p90")).toPandas()
    export_table(summ, "r8_too_diagnostics_crosscheck", suppress_count_cols=["n_overlap"],
                 description="Overlap correlation of annualised weekly attention vs diagnostics views_past_year (cross-check, not ground truth).")
    print(summ.to_string(index=False))


if RUN_COMPUTE and RUN_ROBUSTNESS:
    try:
        robustness_too_crosscheck()
    except Exception as exc:
        _fail("R8", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### R9 | Shorts-cutoff sensitivity
# MAGIC Format view-share (cumulative views on recent uploads) recomputed at alternative Shorts duration
# MAGIC cutoffs, so the long/short split is shown to be robust (or not) to the 60s vs 180s boundary.

# COMMAND ----------
def robustness_shorts_cutoff():
    if video_frame is None or "duration_s" not in video_frame.columns:
        _warn("R9: video_frame/duration_s unavailable; skipping."); return
    rows = []
    ptype_short = (F.col("post_type") == F.lit("short")) if "post_type" in video_frame.columns else F.lit(False)
    for cutoff in sorted({60, SHORTS_MAX_SECONDS, 180}):
        is_short = ptype_short | (F.col("duration_s") <= F.lit(float(cutoff)))
        vf = video_frame.withColumn("fmt2", F.when(is_short, F.lit("Shorts")).otherwise(F.lit("Long-form")))
        fs = vf.groupBy("fmt2").agg(F.sum("video_views").alias("views"), F.count(F.lit(1)).alias("n_videos")).toPandas()
        tot = fs["views"].sum()
        for _, r in fs.iterrows():
            rows.append({"shorts_cutoff_seconds": cutoff, "format": r["fmt2"], "n_videos": int(r["n_videos"]),
                         "view_share": (r["views"] / tot) if tot else None})
    export_table(pd.DataFrame(rows), "r9_shorts_cutoff_sensitivity", suppress_count_cols=["n_videos"],
                 description="Format view-share at 60/configured/180s Shorts cutoffs (cumulative views on recent uploads).")


if RUN_COMPUTE and RUN_ROBUSTNESS:
    try:
        robustness_shorts_cutoff()
    except Exception as exc:
        _fail("R9", exc)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Run manifest & data-availability export

# COMMAND ----------
def write_manifest():
    RUN_MANIFEST["finished_utc"] = datetime.now(timezone.utc).isoformat()
    RUN_MANIFEST["failed_steps"] = list(FAILED_STEPS)  # type: ignore
    payload = json.dumps(RUN_MANIFEST, indent=2, default=str)
    print(payload)
    if WRITE_OUTPUTS:
        for path in (f"{EXPORT_DIR}/run_manifest_CLAUDE.json", f"{LOCAL_FIG_DIR}/run_manifest_CLAUDE.json"):
            try:
                with open(path, "w") as fh:
                    fh.write(payload)
                print(f"  wrote {path}")
            except Exception as exc:
                _warn(f"manifest write failed at {path}: {exc}")
    # Required-output check: a core/full manuscript run must not "succeed" with missing figures/tables.
    if FAILED_STEPS and EXECUTION_MODE in {"core", "full"} and FAIL_ON_MISSING_OUTPUTS:
        raise RuntimeError(f"{len(FAILED_STEPS)} required output step(s) failed in {EXECUTION_MODE} mode: "
                           f"{FAILED_STEPS}. Manifest written; fix the inputs or set fail_on_missing_outputs=false.")


if RUN_COMPUTE:
    write_manifest()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Interpretation checklist (before inserting numbers into the manuscript)
# MAGIC Run a `core`/`full` pass, then confirm each of these against the exported tables:
# MAGIC * **Measurement window.** Is `attention_window_usage_summary` dominated by `primary_window`? If many
# MAGIC   channels fell back (or `snapshot_elapsed_days != 7`), the headline "weekly views" mix windows - say so
# MAGIC   or wait for a true 7-day pair. Both anchor partitions should pass completeness (`snapshot_*_completeness`).
# MAGIC * **Discovery.** Does high-subscriber per-batch yield approach zero late (Fig 1)? Cross-check benchmark overlap.
# MAGIC * **Composition.** Are `language` (`und`) and `category` (`uncategorized`) shares small enough for a
# MAGIC   main-text whole-platform map? `category_coverage`/`language_coverage` in the manifest; R7 bounds the rest.
# MAGIC * **Subscribers.** Report both the central fit and the conditional dispersion + the inactive/unmeasured share.
# MAGIC * **Thresholds.** Lead with one denominator-clear fact; shares are *within the observed >=10k universe*.
# MAGIC * **Age.** State whether Fig 5 uses a real `publishedAt` creation date or the language-rank alternative.
# MAGIC * **Format.** Fig 6 / ED6 are *cumulative views on recent uploads*, not weekly attention (check R9 robustness).
# MAGIC * **Concentration.** Gini/top-k are over *positive-attention* channels; cross-check R8 vs `views_past_year`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Open items requiring project decisions
# MAGIC 1. **First complete Sunday-to-Sunday pair.** Until one exists, `core`/`full` refuse a partial anchor/prior;
# MAGIC    decide the first approved anchor week (and whether to require `attention_measure_status=primary_window`).
# MAGIC 2. **1k tail estimator.** Where will the design-based/bounded sub-10k estimator live, and what aggregate
# MAGIC    does this notebook consume from it to fill the design/bounded tiers in Fig 4?
# MAGIC 3. **Category coverage.** When the LLM categorization lands, point `category_*` at it; until then
# MAGIC    `backfill_channels.topic_categories` (status='done' only) is used and the ~pending mass is bounded (R7).
# MAGIC 4. **Channel creation date.** Is there a true YouTube `snippet.publishedAt` anywhere, or do we add a
# MAGIC    backfill? Without it Fig 5a self-skips to the language-rank alternative.
# MAGIC 5. **Governance.** Promote the publication-critical aggregates out of the personal `dev_sean` catalog
# MAGIC    into a governed shared catalog/views with group grants and frozen snapshots before submission.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Artifacts (all `author="claude"` / `*_CLAUDE`):
# MAGIC * Figures -> `figs/` + export dir (600-DPI PNG + vector PDF).
# MAGIC * One `*_CLAUDE.csv` per figure/table, each **also persisted as a Delta table**
# MAGIC   (`dev_sean.matt.claude_yt_attention_*`) for in-workspace inspection/re-use.
# MAGIC * `table1_threshold_capture_CLAUDE.csv`  -  shares are within the observed >=10k universe; sub-floor
# MAGIC   rows are status placeholders.
# MAGIC * `attention_anchor_snapshot_coverage_CLAUDE.csv`, `attention_measurability_status_CLAUDE.csv`,
# MAGIC   `robustness_summary_CLAUDE.csv`, `r7_category_missingness_bounds_CLAUDE.csv`.
# MAGIC * `run_manifest_CLAUDE.json`.
# MAGIC
# MAGIC **Before camera-ready:** a true Sunday-to-Sunday pair (today only adjacent-day partitions exist, and
# MAGIC the completeness guard rejects the partial 05-28 partition); category coverage after the LLM pass; the
# MAGIC threshold strata / 1k external estimator for the design/bounded tiers; an explicit YouTube
# MAGIC `publishedAt` channel creation date for Fig 5a.
