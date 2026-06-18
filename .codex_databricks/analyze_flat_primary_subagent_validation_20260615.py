#!/usr/bin/env python3
"""Analyze blind subagent flat-topic classifications against tree projections."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "artifacts" / "category_taxonomy_estimation_20260612" / "flat_primary_subagent_validation_20260615"
REFERENCE_PATH = OUT_DIR / "flat_primary_subagent_reference_1000.csv"
CLASSIFICATION_DIR = OUT_DIR / "subagent_outputs"
VARIANT_MODULE_PATH = ROOT / ".codex_databricks" / "render_flat_primary_validation_iteration_20260615.py"
CURRENT_VARIANT = "v12_news_society_politics_explainers"
IMPROVEMENT_THRESHOLD = 0.005


ALLOWED_LABELS = {
    "Music",
    "Video games",
    "Film/TV/Humor",
    "Vehicles",
    "Vehicles/Motorsport",
    "Sports",
    "Religion",
    "Politics/News",
    "News/Society/Politics",
    "Food",
    "Health/Fitness",
    "Technology",
    "Pets/Animals",
    "Fashion/Beauty",
    "Travel",
    "Performing arts",
    "Business",
    "Military",
    "Education/Knowledge",
    "Education/Explainers",
    "Hobby/General interests",
    "Society/General",
    "Lifestyle/General",
    "Entertainment/General",
    "Uncategorized",
}


EVAL_LABEL_MAP = {
    "Vehicles": "Vehicles/Motorsport",
    "Performing arts": "Film/TV/Humor",
    "Politics/News": "News/Society/Politics",
    "Military": "News/Society/Politics",
    "Business": "News/Society/Politics",
    "Society/General": "News/Society/Politics",
    "Education/Knowledge": "Education/Explainers",
}


def collapse_eval_label(value: str) -> str:
    return EVAL_LABEL_MAP.get(str(value), str(value))


def load_variant_module():
    spec = importlib.util.spec_from_file_location("flat_variants", VARIANT_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {VARIANT_MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_topic_array(value) -> list[str]:
    if value is None or pd.isna(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(x) for x in parsed if x is not None]
    except json.JSONDecodeError:
        pass
    try:
        parsed = json.loads(text.replace("'", '"'))
        if isinstance(parsed, list):
            return [str(x) for x in parsed if x is not None]
    except json.JSONDecodeError:
        pass
    return [part.strip().strip('"').strip("'") for part in text.strip("[]").split(",") if part.strip()]


def normalize_label(value: str) -> str:
    raw = str(value or "").strip()
    if raw in ALLOWED_LABELS:
        return raw
    lookup = {label.lower(): label for label in ALLOWED_LABELS}
    return lookup.get(raw.lower(), raw)


def tree_label(labels: list[str], variant_name: str, variants_module) -> str:
    for flat_label, labels_or_key in variants_module.RULE_ORDERS[variant_name]:
        labels_to_match = variants_module.resolve_labels(labels_or_key)
        if set(labels).intersection(labels_to_match):
            return flat_label
    return "Uncategorized"


def tree_match_detail(labels: list[str], variant_name: str, variants_module) -> dict[str, str]:
    label_set = set(labels)
    for flat_label, labels_or_key in variants_module.RULE_ORDERS[variant_name]:
        labels_to_match = variants_module.resolve_labels(labels_or_key)
        matched = sorted(label_set.intersection(labels_to_match))
        if matched:
            key_name = labels_or_key if isinstance(labels_or_key, str) else "literal_list"
            return {
                "tree_matched_rule_key": str(key_name),
                "tree_matched_topic_labels": "; ".join(matched),
            }
    return {
        "tree_matched_rule_key": "no_topic_category",
        "tree_matched_topic_labels": "",
    }


def read_classifications(classification_dir: Path) -> pd.DataFrame:
    paths = sorted(classification_dir.glob("*.csv"))
    if not paths:
        raise FileNotFoundError(f"No subagent CSV outputs found in {classification_dir}")
    frames = []
    expected = ["case_id", "subagent_primary_label", "confidence", "evidence_summary", "ambiguity_notes"]
    for path in paths:
        df = pd.read_csv(path, quoting=csv.QUOTE_MINIMAL)
        missing = [col for col in expected if col not in df.columns]
        if missing:
            raise ValueError(f"{path} missing columns: {missing}")
        df = df[expected].copy()
        df["source_file"] = path.name
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    combined["case_id"] = pd.to_numeric(combined["case_id"], errors="coerce").astype("Int64")
    combined["subagent_primary_label_raw"] = combined["subagent_primary_label"].map(normalize_label)
    combined["subagent_primary_label"] = combined["subagent_primary_label_raw"].map(collapse_eval_label)
    return combined


def plot_variant_deltas(variant_metrics: pd.DataFrame) -> None:
    current_acc = float(variant_metrics.loc[variant_metrics["rule_variant"] == CURRENT_VARIANT, "accuracy"].iloc[0])
    df = variant_metrics.copy().sort_values("delta_vs_current_pp")
    fig, ax = plt.subplots(figsize=(10, max(5, 0.38 * len(df))))
    colors = ["#1b9e77" if x > 0 else "#d95f02" if x < 0 else "#7570b3" for x in df["delta_vs_current_pp"]]
    ax.barh(df["rule_variant"], df["delta_vs_current_pp"], color=colors)
    ax.axvline(0, color="black", linewidth=1)
    for i, row in enumerate(df.itertuples(index=False)):
        ax.text(row.delta_vs_current_pp + (0.05 if row.delta_vs_current_pp >= 0 else -0.05), i, f"{row.delta_vs_current_pp:+.2f} pp", va="center", ha="left" if row.delta_vs_current_pp >= 0 else "right", fontsize=8)
    ax.set_xlabel(f"Accuracy change vs current tree ({current_acc * 100:.2f}%)")
    ax.set_title("Subagent validation rule-variant deltas")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "subagent_rule_variant_delta.png", dpi=220)
    plt.close(fig)


def plot_confusion(confusion: pd.DataFrame) -> None:
    top_labels = sorted(set(confusion.head(20)["tree_label"]).union(set(confusion.head(20)["subagent_primary_label"])))
    matrix = (
        confusion[confusion["tree_label"].isin(top_labels) & confusion["subagent_primary_label"].isin(top_labels)]
        .pivot_table(index="tree_label", columns="subagent_primary_label", values="n_cases", aggfunc="sum", fill_value=0)
        .reindex(index=top_labels, columns=top_labels, fill_value=0)
    )
    fig, ax = plt.subplots(figsize=(10, 8))
    image = ax.imshow(matrix.values, cmap="Blues")
    ax.set_xticks(range(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(matrix.index)))
    ax.set_yticklabels(matrix.index, fontsize=8)
    ax.set_xlabel("Subagent label")
    ax.set_ylabel("Tree label")
    ax.set_title("Top flat-label disagreements and agreements")
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            value = int(matrix.iat[i, j])
            if value:
                ax.text(j, i, str(value), ha="center", va="center", fontsize=7)
    fig.colorbar(image, ax=ax, shrink=0.7, label="cases")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "subagent_tree_confusion_top_labels.png", dpi=220)
    plt.close(fig)


def markdown_table(df: pd.DataFrame, columns: list[tuple[str, str]], max_rows: int | None = None) -> str:
    if max_rows is not None:
        df = df.head(max_rows)
    rows = ["| " + " | ".join(label for label, _ in columns) + " |"]
    rows.append("|" + "|".join(["---"] + ["---:"] * (len(columns) - 1)) + "|")
    for row in df.itertuples(index=False):
        cells = []
        for _label, attr in columns:
            value = getattr(row, attr)
            if attr in {"accuracy", "pct_cases", "error_rate", "merged_accuracy"}:
                cells.append(f"{float(value) * 100:.2f}%")
            elif attr in {"delta_vs_current_pp", "gain_pp"}:
                cells.append(f"{float(value):.2f}")
            elif attr.startswith("n_"):
                cells.append(f"{int(value):,}")
            else:
                cells.append(str(value))
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def compute_pair_merge_diagnostics(merged: pd.DataFrame) -> pd.DataFrame:
    base_accuracy = float((merged["tree_label"] == merged["subagent_primary_label"]).mean())
    labels = sorted(set(merged["tree_label"]).union(set(merged["subagent_primary_label"])))
    rows = []
    for i, label_a in enumerate(labels):
        for label_b in labels[i + 1 :]:
            merged_label = f"{label_a} + {label_b}"
            tree_labels = merged["tree_label"].replace({label_a: merged_label, label_b: merged_label})
            subagent_labels = merged["subagent_primary_label"].replace({label_a: merged_label, label_b: merged_label})
            merged_accuracy = float((tree_labels == subagent_labels).mean())
            rows.append(
                {
                    "label_a": label_a,
                    "label_b": label_b,
                    "merged_accuracy": merged_accuracy,
                    "gain_pp": (merged_accuracy - base_accuracy) * 100,
                }
            )
    return pd.DataFrame(rows).sort_values(["gain_pp", "label_a", "label_b"], ascending=[False, True, True])


def write_report(
    merged: pd.DataFrame,
    variant_metrics: pd.DataFrame,
    confusion: pd.DataFrame,
    errors: pd.DataFrame,
    pair_merges: pd.DataFrame,
) -> None:
    current = variant_metrics[variant_metrics["rule_variant"] == CURRENT_VARIANT].iloc[0]
    best = variant_metrics.sort_values(["accuracy", "rule_variant"], ascending=[False, True]).iloc[0]
    legacy_v1 = variant_metrics[variant_metrics["rule_variant"] == "v1_music_vehicle_priority"]
    legacy_summary = ""
    if not legacy_v1.empty:
        legacy = legacy_v1.iloc[0]
        legacy_summary = f"- Improvement over original `v1_music_vehicle_priority`: {(float(current.accuracy) - float(legacy.accuracy)) * 100:.2f} percentage points\n"
    best_gain = float(best["accuracy"] - current["accuracy"])
    status = "continue" if best_gain > IMPROVEMENT_THRESHOLD else "stop"
    confidence = merged.groupby("confidence", dropna=False).size().reset_index(name="n_cases").sort_values("n_cases", ascending=False)
    confidence["pct_cases"] = confidence["n_cases"] / len(merged)
    top_error_pairs = confusion[confusion["tree_label"] != confusion["subagent_primary_label"]].head(20)
    label_error = (
        merged.assign(is_error=merged["tree_label"] != merged["subagent_primary_label"])
        .groupby("tree_label")
        .agg(n_cases=("case_id", "count"), n_errors=("is_error", "sum"))
        .reset_index()
    )
    label_error["error_rate"] = label_error["n_errors"] / label_error["n_cases"]
    label_error = label_error.sort_values(["n_errors", "error_rate"], ascending=[False, False])
    matched_error = (
        errors.groupby(["tree_matched_rule_key", "tree_matched_topic_labels", "tree_label"], dropna=False)
        .size()
        .reset_index(name="n_cases")
        .sort_values("n_cases", ascending=False)
    )
    change_summary = ""
    if "tree_v1_music_vehicle_priority" in merged.columns:
        changed = merged[merged["tree_v1_music_vehicle_priority"] != merged[f"tree_{CURRENT_VARIANT}"]]
        improved = (
            (changed["tree_v1_music_vehicle_priority"] != changed["subagent_primary_label"])
            & (changed[f"tree_{CURRENT_VARIANT}"] == changed["subagent_primary_label"])
        ).sum()
        regressed = (
            (changed["tree_v1_music_vehicle_priority"] == changed["subagent_primary_label"])
            & (changed[f"tree_{CURRENT_VARIANT}"] != changed["subagent_primary_label"])
        ).sum()
        still_wrong = (
            (changed["tree_v1_music_vehicle_priority"] != changed["subagent_primary_label"])
            & (changed[f"tree_{CURRENT_VARIANT}"] != changed["subagent_primary_label"])
        ).sum()
        change_summary = f"""
## Changed Cases vs Original v1

- Changed assignments: {len(changed):,}
- Improved cases: {int(improved):,}
- Regressed cases: {int(regressed):,}
- Still wrong with a different tree label: {int(still_wrong):,}

Changed row-level cases are in:

```text
flat_primary_subagent_current_changed_cases.csv
```
"""

    report = f"""# Flat Primary Subagent Validation

Date: 2026-06-15

Sample size: {len(merged):,} channels with nonempty YouTube topic-category arrays.

The subagents received only blind channel evidence: channel id, channel name, language code, rough record count, and recent video title/description snippets. They did not receive the YouTube topic-category arrays, the deterministic tree output, or the rule definitions.

## Agreement Summary

Current tree variant: `{CURRENT_VARIANT}`

- Current agreement with subagent labels: {float(current.accuracy) * 100:.2f}% ({int(current.n_correct):,}/{int(current.n_cases):,})
- Best tested variant: `{best.rule_variant}` at {float(best.accuracy) * 100:.2f}%
- Best gain over current tree: {best_gain * 100:.2f} percentage points
{legacy_summary.rstrip()}
- Iteration status with a 0.5 percentage-point threshold: `{status}`

The current tree incorporates these data- and taxonomy-supported changes from the original v1 tree:

1. `Film/TV/Humor` is evaluated after the concrete topical labels through `Travel`, so broad media/humor tags no longer override labels such as politics, food, health, technology, pets, fashion, or travel.
2. `Video_game_culture` is treated as a broad fallback. Specific game-genre tags still trigger `Video games` early, but the broad culture tag yields to more concrete non-game topics.
3. `Motorsport` is assigned to `Vehicles/Motorsport`, and `Sports` excludes motorsport.
4. `Performing arts` is folded into `Film/TV/Humor`.
5. `Politics/News`, `Military`, `Business`, and `Society/General` are folded into `News/Society/Politics`.
6. The raw `Knowledge` topic now maps to `Education/Explainers`, acknowledging that Knowledge is too weak as a flat end-user category.

## Rule Variant Accuracy

{markdown_table(variant_metrics, [("Rule variant", "rule_variant"), ("Cases", "n_cases"), ("Correct", "n_correct"), ("Accuracy", "accuracy"), ("Delta vs current pp", "delta_vs_current_pp")])}

![Rule variant deltas](subagent_rule_variant_delta.png)

## Subagent Confidence

{markdown_table(confidence, [("Confidence", "confidence"), ("Cases", "n_cases"), ("% cases", "pct_cases")])}

## Top Error Pairs

{markdown_table(top_error_pairs, [("Tree label", "tree_label"), ("Subagent label", "subagent_primary_label"), ("Cases", "n_cases")], max_rows=20)}

![Top confusion matrix](subagent_tree_confusion_top_labels.png)

## Diagnostic Pair Merges

This table estimates the apparent validation gain from collapsing two current flat labels into one label. It is a diagnostic only: the largest gains mostly come from broad, qualitatively costly collapses rather than clean taxonomy fixes.

{markdown_table(pair_merges, [("Label A", "label_a"), ("Label B", "label_b"), ("Merged accuracy", "merged_accuracy"), ("Gain pp", "gain_pp")], max_rows=15)}

## Tree Labels With Most Errors

{markdown_table(label_error, [("Tree label", "tree_label"), ("Cases", "n_cases"), ("Errors", "n_errors"), ("Error rate", "error_rate")], max_rows=20)}

## Matched Topic Labels In Residual Errors

{markdown_table(matched_error, [("Rule key", "tree_matched_rule_key"), ("Matched topic labels", "tree_matched_topic_labels"), ("Tree label", "tree_label"), ("Cases", "n_cases")], max_rows=20)}

{change_summary.rstrip()}

## Error Case File

Detailed row-level errors are in:

```text
flat_primary_subagent_validation_errors.csv
```

The error audit with matched tree-rule details is in:

```text
flat_primary_subagent_error_audit.csv
```
"""
    (OUT_DIR / "flat_primary_subagent_validation_report.md").write_text(report, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--classification-dir", type=Path, default=CLASSIFICATION_DIR)
    args = parser.parse_args()

    variants_module = load_variant_module()
    reference = pd.read_csv(REFERENCE_PATH)
    classifications = read_classifications(args.classification_dir)

    duplicate_cases = classifications[classifications.duplicated("case_id", keep=False)]["case_id"].dropna().unique().tolist()
    if duplicate_cases:
        raise ValueError(f"Duplicate classified case_ids: {duplicate_cases[:20]}")
    missing_cases = sorted(set(reference["case_id"]) - set(classifications["case_id"].dropna().astype(int)))
    extra_cases = sorted(set(classifications["case_id"].dropna().astype(int)) - set(reference["case_id"]))
    if missing_cases or extra_cases:
        raise ValueError(f"Missing cases {missing_cases[:20]} extra cases {extra_cases[:20]}")
    invalid_labels = sorted(set(classifications["subagent_primary_label_raw"]) - ALLOWED_LABELS)
    if invalid_labels:
        raise ValueError(f"Invalid labels: {invalid_labels}")

    reference["topic_label_list"] = reference["topic_categories"].map(parse_topic_array)
    merged = reference.merge(classifications, on="case_id", how="inner", validate="one_to_one")
    for variant_name in variants_module.RULE_ORDERS:
        merged[f"tree_{variant_name}"] = merged["topic_label_list"].map(
            lambda labels, v=variant_name: collapse_eval_label(tree_label(labels, v, variants_module))
        )
    merged["tree_label"] = merged[f"tree_{CURRENT_VARIANT}"]
    detail = merged["topic_label_list"].map(lambda labels: tree_match_detail(labels, CURRENT_VARIANT, variants_module))
    merged["tree_matched_rule_key"] = detail.map(lambda item: item["tree_matched_rule_key"])
    merged["tree_matched_topic_labels"] = detail.map(lambda item: item["tree_matched_topic_labels"])
    merged["tree_matches_subagent"] = merged["tree_label"] == merged["subagent_primary_label"]

    metric_rows = []
    for variant_name in variants_module.RULE_ORDERS:
        pred = merged[f"tree_{variant_name}"]
        correct = int((pred == merged["subagent_primary_label"]).sum())
        metric_rows.append(
            {
                "rule_variant": variant_name,
                "n_cases": len(merged),
                "n_correct": correct,
                "accuracy": correct / len(merged),
            }
        )
    variant_metrics = pd.DataFrame(metric_rows)
    current_accuracy = float(variant_metrics.loc[variant_metrics["rule_variant"] == CURRENT_VARIANT, "accuracy"].iloc[0])
    variant_metrics["delta_vs_current_pp"] = (variant_metrics["accuracy"] - current_accuracy) * 100
    variant_metrics = variant_metrics.sort_values(["accuracy", "rule_variant"], ascending=[False, True])

    confusion = (
        merged.groupby(["tree_label", "subagent_primary_label"])
        .size()
        .reset_index(name="n_cases")
        .sort_values("n_cases", ascending=False)
    )
    errors = merged[~merged["tree_matches_subagent"]].copy()
    pair_merges = compute_pair_merge_diagnostics(merged)
    error_audit_columns = [
        "case_id",
        "channel_id",
        "channel_name",
        "language_code",
        "topic_categories",
        "tree_label",
        "tree_matched_rule_key",
        "tree_matched_topic_labels",
        "subagent_primary_label",
        "confidence",
        "evidence_summary",
        "ambiguity_notes",
        "recent_video_evidence",
    ]
    error_audit = errors[error_audit_columns].sort_values(["tree_label", "subagent_primary_label", "case_id"])
    changed_cases = pd.DataFrame()
    if "tree_v1_music_vehicle_priority" in merged.columns:
        changed_cases = merged[
            merged["tree_v1_music_vehicle_priority"] != merged[f"tree_{CURRENT_VARIANT}"]
        ].copy()
        changed_cases["v1_was_correct"] = changed_cases["tree_v1_music_vehicle_priority"] == changed_cases["subagent_primary_label"]
        changed_cases["current_is_correct"] = changed_cases[f"tree_{CURRENT_VARIANT}"] == changed_cases["subagent_primary_label"]
        changed_cases = changed_cases[
            [
                "case_id",
                "channel_id",
                "channel_name",
                "topic_categories",
                "tree_v1_music_vehicle_priority",
                f"tree_{CURRENT_VARIANT}",
                "subagent_primary_label",
                "v1_was_correct",
                "current_is_correct",
                "confidence",
                "evidence_summary",
                "ambiguity_notes",
            ]
        ].sort_values(["current_is_correct", "v1_was_correct", "case_id"], ascending=[False, True, True])

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    classifications.to_csv(OUT_DIR / "flat_primary_subagent_classifications_combined.csv", index=False)
    merged.to_csv(OUT_DIR / "flat_primary_subagent_validation_merged.csv", index=False)
    variant_metrics.to_csv(OUT_DIR / "flat_primary_subagent_rule_variant_accuracy.csv", index=False)
    confusion.to_csv(OUT_DIR / "flat_primary_subagent_confusion_pairs.csv", index=False)
    errors.to_csv(OUT_DIR / "flat_primary_subagent_validation_errors.csv", index=False)
    error_audit.to_csv(OUT_DIR / "flat_primary_subagent_error_audit.csv", index=False)
    pair_merges.to_csv(OUT_DIR / "flat_primary_subagent_pair_merge_diagnostics.csv", index=False)
    if not changed_cases.empty:
        changed_cases.to_csv(OUT_DIR / "flat_primary_subagent_current_changed_cases.csv", index=False)

    plot_variant_deltas(variant_metrics)
    plot_confusion(confusion)
    write_report(merged, variant_metrics, confusion, errors, pair_merges)
    print(json.dumps({
        "n_cases": len(merged),
        "current_accuracy": current_accuracy,
        "best_variant": str(variant_metrics.iloc[0]["rule_variant"]),
        "best_accuracy": float(variant_metrics.iloc[0]["accuracy"]),
        "best_delta_vs_current_pp": float(variant_metrics.iloc[0]["delta_vs_current_pp"]),
        "report": str(OUT_DIR / "flat_primary_subagent_validation_report.md"),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
