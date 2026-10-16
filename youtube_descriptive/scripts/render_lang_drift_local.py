"""Render the language-drift panel locally.

Runs `scripts/sql/lang_drift_weekly_hwm.sql` on a SQL warehouse (aggregates only: one row per
week x language), saves it as CSV source data, and draws the panel with `src/lang_drift_plot.py`.

Usage:
    python scripts/render_lang_drift_local.py --profile accelerator-db --warehouse 86100da4e1fe8713
    python scripts/render_lang_drift_local.py --from-csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from lang_drift_plot import plot_lang_drift  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile")
    ap.add_argument("--warehouse")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--out", default=str(ROOT / "figs" / "lang_drift"))
    ap.add_argument("--from-csv", action="store_true", help="re-render from the saved CSV")
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    csv = out / f"lang_drift_weekly_{a.version}.csv"
    if a.from_csv:
        weekly = pd.read_csv(csv, keep_default_na=False)   # keep ISO code "nan" (Min Nan)
    else:
        if not (a.profile and a.warehouse):
            ap.error("--profile and --warehouse are required unless --from-csv")
        from render_fig1_local import query
        sql = (ROOT / "scripts" / "sql" / "lang_drift_weekly_hwm.sql").read_text(encoding="utf-8")
        weekly = query(sql, a.profile, a.warehouse)
        weekly = weekly[["week_end", "language", "n_channels", "n_below_hwm", "wv"]]
        weekly.sort_values(["week_end", "language"]).to_csv(csv, index=False)

    fig, summary = plot_lang_drift(weekly)
    stem = out / f"lang_drift_{a.version}"
    fig.savefig(f"{stem}.png", dpi=300, bbox_inches="tight")
    fig.savefig(f"{stem}.pdf", bbox_inches="tight")
    summary.to_csv(out / f"lang_drift_summary_{a.version}.csv")
    print("wrote", stem)
    print(summary.sort_values("rel_change_pct").to_string())


if __name__ == "__main__":
    main()
