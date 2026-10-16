"""Sensitivity checks for Figure 5b, recomputed from the committed CLDR file and the language table.

Writes `figs/fig5b/fig5b_sensitivities_v1.csv`. Needs pandas; the XML is the committed CLDR release, parsed with
the standard library (no download). Run from `youtube_descriptive/`:
    python scripts/fig5b_sensitivities.py
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
t = pd.read_csv(ROOT / "figs/fig5b/fig5b_language_table_v1.csv", keep_default_na=False, na_values=[""])
root = ET.parse(ROOT / "data/cldr_release-48-2/supplementalData.xml").getroot()
terr = {te.get("type"): (float(te.get("population")),
                         {lp.get("type"): float(lp.get("populationPercent")) for lp in te.findall("languagePopulation")})
        for te in root.find("territoryInfo")}
WORLD = sum(p for p, _ in terr.values())


def speakers(codes, skip=()):
    """Speakers of a set of CLDR codes: max per territory (nested varieties), summed over territories."""
    tot = 0.0
    for k, (pop, pct) in terr.items():
        if k in skip:
            continue
        v = [x for ty, x in pct.items() if ty in codes or ty.split("_")[0] in codes]
        if v:
            tot += pop * max(v) / 100
    return tot


known = t[t.speakers > 0]
WV, SP = known.wv_sum.sum(), known.speakers.sum()
row = lambda c: t[t.analysis_code == c].iloc[0]  # noqa: E731


def index(wv, sp, sp_total):
    return (wv / WV) / (sp / sp_total)


out = [("anchor: speaker total used (billions)", SP / 1e9), ("anchor: world population (billions)", WORLD / 1e9),
       ("anchor: factor on every index if world population is the base", WORLD / SP),
       ("English index, as drawn", row("eng")["index"]), ("English index, world-population anchor", row("eng")["index"] * WORLD / SP),
       ("Thai index, as drawn", row("tha")["index"]), ("Thai index, world-population anchor", row("tha")["index"] * WORLD / SP)]

cmn = row("cmn")
sp_all, sp_x = speakers(["zh", "zh_Hant"]), speakers(["zh", "zh_Hant"], skip=("CN",))
out += [("Chinese index, as drawn", cmn["index"]), ("Chinese speakers, as drawn (M)", sp_all / 1e6),
        ("Chinese speakers excluding PRC (M)", sp_x / 1e6),
        ("Chinese index excluding PRC (speaker total reduced too)", index(cmn.wv_sum, sp_x, SP - sp_all + sp_x)),
        ("Chinese index excluding PRC (speaker total unchanged)", index(cmn.wv_sum, sp_x, SP))]


def pooled(a, b, codes, label):
    ra, rb = row(a), row(b)
    wv, sp = ra.wv_sum + rb.wv_sum, speakers(codes)
    rest = SP - ra.speakers - rb.speakers
    out.append((f"{label} pooled: speakers, max per territory (M)", sp / 1e6))
    out.append((f"{label} pooled index (max per territory)", index(wv, sp, rest + sp)))
    out.append((f"{label} pooled index (plain sum of speakers)", index(wv, ra.speakers + rb.speakers, SP)))


pooled("hin", "urd", ["hi", "ur"], "Hindi+Urdu")
pooled("ind", "zsm", ["id", "ms"], "Indonesian+Malay")
out.append(("CLDR share of Indonesia speaking Indonesian (%)", terr["ID"][1]["id"]))
pd.DataFrame(out, columns=["metric", "value"]).to_csv(ROOT / "figs/fig5b/fig5b_sensitivities_v1.csv", index=False)
print(pd.DataFrame(out, columns=["metric", "value"]).to_string(index=False))
