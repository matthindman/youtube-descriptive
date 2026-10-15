"""Render Figure 3 variants locally from the exported aggregate tables.

Pulls only the small aggregate tables written by
`src/13_fig3_subscriber_proxy_databricks.py` (never channel-level rows), saves them as CSV
source data next to the figures, and draws every variant with `src/fig3_proxy_plot.py`.

Usage:
    python scripts/render_fig3_local.py --profile accelerator-db --warehouse <id> [--version v1]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from fig3_proxy_plot import RENDER_SPECS, plot_fig3  # noqa: E402
from render_fig1_local import query  # noqa: E402

TABLES = ("bins", "hist2d", "fit", "decomp", "window_check", "snapshots", "negatives")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--prefix", default="dev_sean.matt.fig3_subproxy")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--out", default=str(ROOT / "figs" / "fig3"))
    ap.add_argument("--from-csv", action="store_true", help="re-render from the saved CSVs")
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    tables = {}
    for name in TABLES:
        csv = out / f"fig3_{name}_{a.version}.csv"
        if a.from_csv:
            import pandas as pd
            tables[name] = pd.read_csv(csv)
        else:
            tables[name] = query(f"SELECT * FROM {a.prefix}_{name}_{a.version}", a.profile, a.warehouse)
            tables[name].to_csv(csv, index=False)
    for variant, panel_c in RENDER_SPECS:
        stem = out / f"fig3_{variant}_{panel_c or 'ab'}_{a.version}"
        for ext in ("pdf", "png"):
            plot_fig3(tables["bins"], tables["hist2d"], tables["fit"], variant, panel_c=panel_c,
                      path=f"{stem}.{ext}")
        print("wrote", stem)


if __name__ == "__main__":
    main()
