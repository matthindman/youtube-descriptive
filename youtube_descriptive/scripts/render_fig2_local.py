"""Render Figure 2 locally from the exported aggregate tables.

Pulls only the aggregate tables written by `src/12_fig2_platform_map_databricks.py` (never the
channel-level table), saves them as CSV source data next to the figure, and draws it with
`src/fig2_map_plot.py`. The `cells` table names labelled head channels by design (see the notebook).

Usage:
    python scripts/render_fig2_local.py --profile accelerator-db --warehouse <id> [--version v1]
    python scripts/render_fig2_local.py --from-csv --version v1     # redraw from saved CSVs
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from fig2_map_plot import plot_fig2  # noqa: E402

TABLES = ("cells", "blocks", "lang_cat", "coverage")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile")
    ap.add_argument("--warehouse")
    ap.add_argument("--prefix", default="dev_sean.matt.fig2_map")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--out", default=str(ROOT / "figs" / "fig2"))
    ap.add_argument("--from-csv", action="store_true")
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    tables = {}
    for name in TABLES:
        csv = out / f"fig2_{name}_{a.version}.csv"
        if a.from_csv:
            df = pd.read_csv(csv)
        else:
            from render_fig1_local import query
            df = query(f"SELECT * FROM {a.prefix}_{name}_{a.version}", a.profile, a.warehouse)
            df.to_csv(csv, index=False)
        tables[name] = df
    stem = out / f"fig2_map_{a.version}"
    for ext in ("pdf", "png"):
        _, stats = plot_fig2(tables["cells"], tables["blocks"], tables["lang_cat"], path=f"{stem}.{ext}")
    stats.to_csv(out / f"fig2_divergence_{a.version}.csv", index=False)
    print("wrote", stem)
    print(stats.to_string(index=False))


if __name__ == "__main__":
    main()
