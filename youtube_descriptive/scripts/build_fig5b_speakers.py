"""Build the speaker-population table for Figure 5b from Unicode CLDR.

Reads `supplementalData.xml` (`<territoryInfo>`: per-territory population and `languagePopulation`
percentages) from a CLDR release directory and the hand-written group map
`data/fig5b_language_map.csv`, and writes one row per analysis language with approximate total
speakers (L1 + L2, as estimated by CLDR) summed over territories.

Rules (see README_fig5b_language_rank.md):
  * speakers(territory, language) = territory population x populationPercent / 100;
  * a map row lists the CLDR codes it covers. Script/region variants (`zh_Hant`, `yue_Hans`, ...) join
    the group of their base code unless another row claims that exact code (e.g. `pa_Arab` -> pnb,
    `az_Arab` -> azb);
  * several codes in one row are combined per territory by `max` (nested varieties/dialects of one
    label: Arabic dialects, script variants, so people are not double counted) or `sum` (distinct
    populations, capped at 100%: Serbian + Croatian + Bosnian).

Standard library only; run with `python -I` (the input directory is untrusted downloaded data):
    python -I scripts/build_fig5b_speakers.py --cldr-dir data/cldr_release-48-2 \
        --map data/fig5b_language_map.csv --out figs/fig5b/fig5b_speakers_cldr_release-48-2.csv
"""
from __future__ import annotations

import argparse
import csv
import xml.etree.ElementTree as ET
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cldr-dir", required=True)
    ap.add_argument("--map", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    root = ET.parse(Path(a.cldr_dir) / "supplementalData.xml").getroot()
    terr = {}  # territory -> (population, {cldr type: percent})
    for te in root.find("territoryInfo"):
        pct = {lp.get("type"): float(lp.get("populationPercent")) for lp in te.findall("languagePopulation")}
        terr[te.get("type")] = (float(te.get("population")), pct)
    world = sum(p for p, _ in terr.values())

    with open(a.map, newline="", encoding="utf-8") as f:
        groups = list(csv.DictReader(f))
    claimed = {c for g in groups for c in g["cldr_codes"].split(",")}

    out_rows, missing = [], []
    for g in groups:
        codes = g["cldr_codes"].split(",")
        total, per_terr, matched = 0.0, {}, set()
        for t, (pop, pct) in terr.items():
            vals = []
            for typ, v in pct.items():
                base = typ.split("_")[0]
                if typ in codes or (typ not in claimed and base in codes):
                    vals.append(v)
                    matched.add(typ)
            if not vals:
                continue
            share = max(vals) if g["combine"] == "max" else min(100.0, sum(vals))
            per_terr[t] = pop * share / 100
            total += per_terr[t]
        if not total:
            missing.append(g["analysis_code"])
        top = max(per_terr, key=per_terr.get) if per_terr else ""
        out_rows.append({
            "analysis_code": g["analysis_code"], "name": g["name"], "speakers": round(total),
            "n_territories": len(per_terr), "top_territory": top,
            "top_territory_share": round(per_terr[top] / total, 3) if total else "",
            "cldr_codes_matched": "|".join(sorted(matched)), "combine": g["combine"]})
    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0]))
        w.writeheader()
        w.writerows(out_rows)
    print(f"world population in territoryInfo: {world:,.0f}; rows: {len(out_rows)}; no CLDR data for: {missing}")


if __name__ == "__main__":
    main()
