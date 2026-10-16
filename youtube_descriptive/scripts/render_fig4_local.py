"""Render Figure 4 variants locally from the exported aggregate tables.

Pulls only the small aggregate tables written by `src/14_fig4_thresholds_databricks.py` (never
the channel-level or ranked tables), saves them as CSV source data next to the figures, and draws
every variant with `src/fig4_thresholds_plot.py`.

Usage:
    python scripts/render_fig4_local.py --profile accelerator-db --warehouse <id> [--version v1]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from fig4_thresholds_plot import RENDER_SPECS, plot_fig4  # noqa: E402
from render_fig1_local import query  # noqa: E402

TABLES = ("vbins", "sbins", "thresholds", "status", "tail", "snapshots", "reconcile")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--prefix", default="dev_sean.matt.fig4_thresholds")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--out", default=str(ROOT / "figs" / "fig4"))
    ap.add_argument("--from-csv", action="store_true", help="re-render from the saved CSVs")
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    tables = {}
    for name in TABLES:
        csv = out / f"fig4_{name}_{a.version}.csv"
        if a.from_csv:
            import pandas as pd
            tables[name] = pd.read_csv(csv)
        else:
            tables[name] = query(f"SELECT * FROM {a.prefix}_{name}_{a.version}", a.profile, a.warehouse)
            tables[name].to_csv(csv, index=False)
    for name, panels in RENDER_SPECS:
        stem = out / f"fig4_{name}_{a.version}"
        for ext in ("pdf", "png"):
            plot_fig4(tables["vbins"], tables["sbins"], tables["thresholds"], tables["status"],
                      tables["tail"], panels=panels, path=f"{stem}.{ext}")
        print("wrote", stem)


if __name__ == "__main__":
    main()
