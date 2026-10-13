"""Render Figure 1 variants locally from the exported aggregate tables.

Pulls only the small aggregate tables written by
`src/07_fig1_discovery_saturation_databricks.py` (never channel-level rows), saves them as
CSV source data next to the figures, and draws every variant with `src/fig1_discovery_plot.py`.

Usage:
    python scripts/render_fig1_local.py --profile accelerator-db --warehouse <id> [--version v1]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from fig1_discovery_plot import plot_fig1  # noqa: E402

VARIANTS = [("active_panel", "subscribers"), ("all", "subscribers"),
            ("active_panel", "weekly_views"), ("active_crawl", "subscribers")]


def query(sql: str, profile: str, warehouse: str) -> pd.DataFrame:
    def api(method, path, body=None):
        cmd = ["databricks", "api", method, path, "--profile", profile]
        if body is not None:
            cmd += ["--json", json.dumps(body)]
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", check=True)
        return json.loads(r.stdout)

    res = api("post", "/api/2.0/sql/statements", {
        "warehouse_id": warehouse, "statement": sql, "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE", "format": "JSON_ARRAY", "disposition": "INLINE"})
    while res["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        res = api("get", f"/api/2.0/sql/statements/{res['statement_id']}")
    if res["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(res["status"])
    cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
    df = pd.DataFrame(res.get("result", {}).get("data_array", []) or [], columns=cols)
    return df.apply(pd.to_numeric, errors="ignore")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--prefix", default="dev_sean.matt.fig1_discovery")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--out", default=str(ROOT / "figs" / "fig1"))
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    tables = {}
    for name in ("exhaustion", "totals", "trending", "activity"):
        df = query(f"SELECT * FROM {a.prefix}_{name}_{a.version}", a.profile, a.warehouse)
        df.to_csv(out / f"fig1_{name}_{a.version}.csv", index=False)
        tables[name] = df
    for variant, bin_type in VARIANTS:
        stem = out / f"fig1_{variant}_{bin_type}_{a.version}"
        plot_fig1(tables["exhaustion"], tables["trending"], tables["totals"], variant, bin_type,
                  path=f"{stem}.pdf")
        plot_fig1(tables["exhaustion"], tables["trending"], tables["totals"], variant, bin_type,
                  path=f"{stem}.png")
        print("wrote", stem)


if __name__ == "__main__":
    main()
