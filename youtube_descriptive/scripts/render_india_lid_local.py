"""Render the India Hindi-vs-English Extended Data panel locally from the exported aggregate tables.

Pulls only the aggregate tables written by `src/20_india_lid_pilot_databricks.py` (never the channel-level
tables), saves them as CSV source data next to the figures, and draws every variant with
`src/india_lid_plot.py`. With `--validation` it also writes the 300-channel coding sample (public channel IDs,
titles and model labels only) to `validation/india_lid_v1/validation_sample.csv`.

Usage:
    python scripts/render_india_lid_local.py --profile accelerator-db --warehouse <id> [--version v1] \
        [--source full_run:ftr] [--validation]
    python scripts/render_india_lid_local.py --from-csv --source pilot_sample_weighted:2s
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from india_lid_plot import VARIANTS, mix, plot_india_panel  # noqa: E402

TABLES = ("panel", "panel_sources", "label_shares", "agreement", "lexicon")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile")
    ap.add_argument("--warehouse")
    ap.add_argument("--prefix", default="dev_sean.matt.india_lid_pilot")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--source", default="full_run:ftr",
                    help="IndicLID split source in the panel table: <full_run|pilot_sample_weighted>:<2s|ftr>")
    ap.add_argument("--out", default=str(ROOT / "figs" / "india_lid"))
    ap.add_argument("--from-csv", action="store_true")
    ap.add_argument("--validation", action="store_true")
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    tables = {}
    for name in TABLES:
        csv = out / f"india_lid_{name}_{a.version}.csv"
        if a.from_csv:
            tables[name] = pd.read_csv(csv)
        else:
            from render_fig1_local import query
            tables[name] = query(f"SELECT * FROM {a.prefix}_{name}_{a.version}", a.profile, a.warehouse)
            tables[name].to_csv(csv, index=False)
    tag = a.source.replace(":", "_")
    for name, groups in VARIANTS.items():
        stem = out / f"india_lid_panel_{name}_{tag}_{a.version}"
        for ext in ("pdf", "png"):
            _, m = plot_india_panel(tables["panel"], a.source, groups, path=f"{stem}.{ext}")
        m.to_csv(f"{stem}_shares.csv", index=False)
        print("wrote", stem)
    summary = mix(tables["panel"], a.source, VARIANTS["hin_in_sensitivity"])
    print(summary.pivot(index="category", columns="grp", values="share_channels").round(3).to_string())
    print(summary.pivot(index="category", columns="grp", values="share_views").round(3).to_string())

    if a.validation:
        from render_fig1_local import query
        v = query(f"SELECT * FROM {a.prefix}_validation_{a.version}", a.profile, a.warehouse)
        v = v.rename(columns={"indiclid_label": "indiclid_label"})
        v["lexicon_score"] = pd.to_numeric(v["lexicon_score"], errors="coerce").round(4)
        for c in ("spoken_language", "title_language", "coder", "notes"):
            v[c] = ""
        cols = ["channel_id", "channel_url", "channel_title", "recent_video_title_1", "recent_video_title_2",
                "recent_video_title_3", "indiclid_label", "lexicon_score", "original_lid_label",
                "spoken_language", "title_language", "coder", "notes", "stratum_label", "stratum_views_tercile"]
        vdir = ROOT / "validation" / "india_lid_v1"
        vdir.mkdir(parents=True, exist_ok=True)
        v[cols].to_csv(vdir / "validation_sample.csv", index=False, encoding="utf-8")
        print(v.groupby(["stratum_label", "stratum_views_tercile"]).size())
        print("wrote", vdir / "validation_sample.csv")


if __name__ == "__main__":
    main()
