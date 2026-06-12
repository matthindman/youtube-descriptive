# Databricks notebook source
import json
import os
import re
from typing import Any, Dict, List

from pyspark.sql import Window
from pyspark.sql import functions as F


def _create_text_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default, name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None and value != "" else default
    except Exception:
        return os.environ.get(name.upper(), default)


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
_create_text_widget("audit_table", "yt_lid_v3_too_full_20260609_llm_hardcase_disagreement_audit")
_create_text_widget("start_rank", "1")
_create_text_widget("batch_size", "30")
_create_text_widget("num_batches", "4")
_create_text_widget("output_path", "/dbfs/FileStore/youtube_lid_panel_batches/analysis/high_disagreement_batches_20260611.json")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
AUDIT_TABLE = _get_widget("audit_table", "yt_lid_v3_too_full_20260609_llm_hardcase_disagreement_audit")
START_RANK = int(_get_widget("start_rank", "1"))
BATCH_SIZE = int(_get_widget("batch_size", "30"))
NUM_BATCHES = int(_get_widget("num_batches", "4"))
OUTPUT_PATH = _get_widget("output_path", "/dbfs/FileStore/youtube_lid_panel_batches/analysis/high_disagreement_batches_20260611.json")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def _load_json(value: Any, default):
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _compact_text(text: Any, limit: int = 220) -> str:
    s = re.sub(r"\s+", " ", str(text or "")).strip()
    return s[:limit]


def _dist_string(rows: List[Dict[str, Any]], key: str) -> str:
    parts = []
    for row in rows or []:
        label = row.get(key) or row.get("normalized_base_iso") or row.get("normalized_language_label")
        parts.append(f"{label}:{row.get('n')}")
    return ", ".join(parts)


def _outliers(votes: List[Dict[str, Any]]) -> List[str]:
    out = []
    for vote in votes or []:
        if vote.get("outside_base_majority"):
            out.append(f"{vote.get('provider')}:{vote.get('model')}={vote.get('normalized_language_label') or vote.get('language_label')}")
    return out


def _segments(segments: List[Dict[str, Any]]) -> List[str]:
    compact = []
    for seg in (segments or [])[:8]:
        compact.append(f"{seg.get('segment_type')}: {_compact_text(seg.get('text'))}")
    return compact


w = Window.orderBy(
    F.desc("base_dissenting_models"),
    F.desc("label_dissenting_models"),
    F.desc("n_distinct_normalized_base_iso"),
    F.desc("n_valid_votes"),
    F.asc("channel_id"),
)

if START_RANK < 1:
    raise ValueError("start_rank must be >= 1")

limit_n = BATCH_SIZE * NUM_BATCHES
end_rank = START_RANK + limit_n - 1
rows = (
    spark.table(fqtn(AUDIT_TABLE))
    .where(F.col("run_id") == F.lit(RUN_ID))
    .withColumn("rank", F.row_number().over(w))
    .where((F.col("rank") >= F.lit(START_RANK)) & (F.col("rank") <= F.lit(end_rank)))
    .orderBy("rank")
    .collect()
)

cases = []
for row in rows:
    d = row.asDict(recursive=True)
    base_dist = _load_json(d.get("base_vote_distribution_json"), [])
    label_dist = _load_json(d.get("label_vote_distribution_json"), [])
    votes = _load_json(d.get("model_votes_json"), [])
    segments = _load_json(d.get("top_segments_json"), [])
    rank = int(d["rank"])
    cases.append({
        "rank": rank,
        "batch": ((rank - START_RANK) // BATCH_SIZE) + 1,
        "channel_id": d.get("channel_id"),
        "n_valid_votes": d.get("n_valid_votes"),
        "base_dissenting_models": d.get("base_dissenting_models"),
        "label_dissenting_models": d.get("label_dissenting_models"),
        "n_distinct_normalized_base_iso": d.get("n_distinct_normalized_base_iso"),
        "majority_normalized_base_iso": d.get("majority_normalized_base_iso"),
        "majority_normalized_language_label": d.get("majority_normalized_language_label"),
        "base_distribution": _dist_string(base_dist, "normalized_base_iso"),
        "label_distribution": _dist_string(label_dist, "normalized_language_label"),
        "openlid_primary_language_label": d.get("openlid_primary_language_label"),
        "glotlid_primary_language_label": d.get("glotlid_primary_language_label"),
        "consensus_status": d.get("consensus_status"),
        "probable_issue_flags": d.get("probable_issue_flags") or [],
        "outlier_votes": _outliers(votes),
        "segments": _segments(segments),
    })

summary = {
    "run_id": RUN_ID,
    "audit_table": f"{CATALOG}.{SCHEMA}.{AUDIT_TABLE}",
    "start_rank": START_RANK,
    "end_rank": end_rank,
    "batch_size": BATCH_SIZE,
    "num_batches": NUM_BATCHES,
    "n_cases": len(cases),
    "batches": [
        {
            "batch": batch_id,
            "start_rank": START_RANK + ((batch_id - 1) * BATCH_SIZE),
            "end_rank": min(START_RANK + (batch_id * BATCH_SIZE) - 1, START_RANK + len(cases) - 1),
            "cases": [case for case in cases if case["batch"] == batch_id],
        }
        for batch_id in range(1, NUM_BATCHES + 1)
    ],
}

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2, sort_keys=True)

print(json.dumps({
    "output_path": OUTPUT_PATH,
    "start_rank": START_RANK,
    "end_rank": end_rank,
    "n_cases": len(cases),
    "batches": [
        {"batch": b["batch"], "start_rank": b["start_rank"], "end_rank": b["end_rank"]}
        for b in summary["batches"]
    ],
}, ensure_ascii=False, sort_keys=True))
dbutils.notebook.exit(json.dumps({"output_path": OUTPUT_PATH, "n_cases": len(cases)}, ensure_ascii=False, sort_keys=True))
