"""Render Figure 5b (alternative Figure 5: language rank vs engagement and population) locally.

Pulls only the small aggregate tables written by `src/19_fig5b_language_rank_databricks.py` (never the
channel-level table), saves them as CSV source data next to the figure, joins them to the CLDR speaker
table (`figs/fig5b/fig5b_speakers_<cldr>.csv`, built by `scripts/build_fig5b_speakers.py`) and draws every
variant with `src/fig5b_language_rank_plot.py`.

Usage:
    python scripts/render_fig5b_local.py --profile accelerator-db --warehouse <id> [--version v1]
    python scripts/render_fig5b_local.py --from-csv --version v1      # redraw from the saved CSVs
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from fig5b_language_rank_plot import RENDER_SPECS, build_table, plot_fig5b, summary  # noqa: E402

TABLES = ("languages", "status", "reconcile")
CLDR = "cldr_release-48-2"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile")
    ap.add_argument("--warehouse")
    ap.add_argument("--prefix", default="dev_sean.matt.fig5b_langrank")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--out", default=str(ROOT / "figs" / "fig5b"))
    ap.add_argument("--speakers", default=None, help="speaker CSV (default: figs/fig5b/fig5b_speakers_<cldr>.csv)")
    ap.add_argument("--language-map", default=str(ROOT / "data" / "fig5b_language_map.csv"))
    ap.add_argument("--from-csv", action="store_true", help="re-render from the saved CSVs")
    a = ap.parse_args()
    if not a.from_csv and not (a.profile and a.warehouse):
        ap.error("--profile and --warehouse are required unless --from-csv is given")

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    tables = {}
    for name in TABLES:
        csv = out / f"fig5b_{name}_{a.version}.csv"
        if a.from_csv:
            tables[name] = pd.read_csv(csv, keep_default_na=False)  # "nan" is Min Nan, not NaN
        else:
            from render_fig1_local import query
            tables[name] = query(f"SELECT * FROM {a.prefix}_{name}_{a.version}", a.profile, a.warehouse)
            tables[name].to_csv(csv, index=False)

    speakers = pd.read_csv(a.speakers or out / f"fig5b_speakers_{CLDR}.csv", keep_default_na=False)
    speakers["speakers"] = pd.to_numeric(speakers["speakers"])
    lmap = pd.read_csv(a.language_map, keep_default_na=False)
    table = build_table(tables["languages"], speakers, lmap)
    table.to_csv(out / f"fig5b_language_table_{a.version}.csv", index=False)
    stats = summary(table, tables["languages"], tables["status"])
    stats.to_csv(out / f"fig5b_summary_{a.version}.csv", index=False)
    for name, panels in RENDER_SPECS:
        stem = out / f"fig5b_{name}_{a.version}"
        for ext in ("pdf", "png"):
            plot_fig5b(table, panels=panels, path=f"{stem}.{ext}")
        print("wrote", stem)
    print(stats.to_string(index=False))


if __name__ == "__main__":
    main()
