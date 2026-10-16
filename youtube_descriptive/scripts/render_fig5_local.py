"""Render Figure 5 variants locally from the exported aggregate tables.

Pulls only the small aggregate tables written by `src/15_fig5_age_databricks.py` (never the
channel-level table), saves them as CSV source data next to the figures, and draws every variant
with `src/fig5_age_plot.py`.

Usage:
    python scripts/render_fig5_local.py --profile accelerator-db --warehouse <id> [--version v1]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from fig5_age_plot import RENDER_SPECS, plot_fig5  # noqa: E402
from render_fig1_local import query  # noqa: E402

TABLES = ("blocks", "year_dist", "status", "reconcile")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile")
    ap.add_argument("--warehouse")
    ap.add_argument("--prefix", default="dev_sean.matt.fig5_age")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--out", default=str(ROOT / "figs" / "fig5"))
    ap.add_argument("--from-csv", action="store_true", help="re-render from the saved CSVs")
    a = ap.parse_args()
    if not a.from_csv and not (a.profile and a.warehouse):
        ap.error("--profile and --warehouse are required unless --from-csv is given")

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    tables = {}
    for name in TABLES:
        csv = out / f"fig5_{name}_{a.version}.csv"
        if a.from_csv:
            import pandas as pd
            tables[name] = pd.read_csv(csv)
        else:
            tables[name] = query(f"SELECT * FROM {a.prefix}_{name}_{a.version}", a.profile, a.warehouse)
            tables[name].to_csv(csv, index=False)
    for name, panels in RENDER_SPECS:
        stem = out / f"fig5_{name}_{a.version}"
        for ext in ("pdf", "png"):
            plot_fig5(tables["blocks"], tables["year_dist"], panels=panels, path=f"{stem}.{ext}")
        print("wrote", stem)


if __name__ == "__main__":
    main()
