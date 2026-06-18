#!/usr/bin/env python3
"""Materialize and analyze the draft one-label flat topic classifier."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROFILE = os.environ.get("DATABRICKS_PROFILE", "matt.hindman@researchaccelerator.org")
WAREHOUSE_ID = "86100da4e1fe8713"
CREATE_SQL = ROOT / ".codex_databricks" / "sql_flat_primary_draft_create_20260615.sql"
OUT_DIR = ROOT / "artifacts" / "category_taxonomy_estimation_20260612" / "flat_primary_draft_20260615"
TABLE_NAME = "dev_sean.matt.yt_channel_topic_flat_primary_draft_20260615"


def databricks_api(method: str, path: str, *extra_args: str) -> dict:
    cmd = [
        "env",
        "DATABRICKS_AUTH_STORAGE=plaintext",
        "databricks",
        "api",
        method,
        path,
        "--profile",
        PROFILE,
        "--output",
        "json",
        *extra_args,
    ]
    return json.loads(subprocess.check_output(cmd, cwd=ROOT, text=True))


def execute_sql(statement: str) -> dict:
    body = {
        "warehouse_id": WAREHOUSE_ID,
        "catalog": "dev_sean",
        "schema": "matt",
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
        "statement": statement,
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(body, handle)
        temp_path = Path(handle.name)
    try:
        response = databricks_api("post", "/api/2.0/sql/statements", "--json", f"@{temp_path}")
    finally:
        temp_path.unlink(missing_ok=True)
    statement_id = response["statement_id"]
    state = response.get("status", {}).get("state")
    while state in {"PENDING", "RUNNING"}:
        time.sleep(5)
        response = databricks_api("get", f"/api/2.0/sql/statements/{statement_id}")
        state = response.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(json.dumps(response.get("status", response), indent=2))
    if "manifest" in response and "result" not in response:
        response["result"] = databricks_api("get", f"/api/2.0/sql/statements/{statement_id}/result/chunks/0")
    return response


def query_df(statement: str) -> pd.DataFrame:
    response = execute_sql(statement)
    if "manifest" not in response:
        return pd.DataFrame()
    columns = [col["name"] for col in response["manifest"]["schema"]["columns"]]
    rows = response.get("result", {}).get("data_array", [])
    df = pd.DataFrame(rows, columns=columns)
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (TypeError, ValueError):
            pass
    return df


def pct_fmt(value: float) -> str:
    return f"{100 * float(value):.2f}%"


def write_csvs(data: dict[str, pd.DataFrame]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in data.items():
        df.to_csv(OUT_DIR / f"{name}.csv", index=False)


def plot_distribution(distribution: pd.DataFrame) -> None:
    df = distribution.sort_values("n_nonempty_channels", ascending=True)
    fig, ax = plt.subplots(figsize=(10.5, max(7, 0.34 * len(df))))
    ax.barh(df["primary_flat_label"], df["pct_nonempty_channels"] * 100, color="#4575b4")
    for i, row in enumerate(df.itertuples(index=False)):
        ax.text(row.pct_nonempty_channels * 100 + 0.15, i, f"{row.pct_nonempty_channels * 100:.1f}%", va="center", fontsize=8)
    ax.set_xlabel("% of nonempty channels")
    ax.set_title("Draft flat primary topic distribution")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "primary_flat_distribution.png", dpi=220)
    plt.close(fig)


def plot_candidate_counts(candidate_counts: pd.DataFrame) -> None:
    df = candidate_counts.sort_values("n_candidate_flat_labels")
    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.bar(df["n_candidate_flat_labels"].astype(str), df["pct_nonempty_channels"] * 100, color="#7b3294")
    for i, row in enumerate(df.itertuples(index=False)):
        ax.text(i, row.pct_nonempty_channels * 100 + 0.4, f"{row.pct_nonempty_channels * 100:.1f}%", ha="center", fontsize=8)
    ax.set_xlabel("Number of mapped candidate flat labels")
    ax.set_ylabel("% of nonempty channels")
    ax.set_title("Ambiguity before priority tie-breaking")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "candidate_count_distribution.png", dpi=220)
    plt.close(fig)


def plot_hobby(hobby_distribution: pd.DataFrame) -> None:
    df = hobby_distribution.sort_values("n_hobby_channels", ascending=True)
    fig, ax = plt.subplots(figsize=(9.5, max(5, 0.38 * len(df))))
    ax.barh(df["primary_flat_label"], df["pct_hobby_channels"] * 100, color="#1b9e77")
    for i, row in enumerate(df.itertuples(index=False)):
        ax.text(row.pct_hobby_channels * 100 + 0.3, i, f"{row.pct_hobby_channels * 100:.1f}%", va="center", fontsize=8)
    ax.set_xlabel("% of channels with Hobby label")
    ax.set_title("Where Hobby-labeled channels go under the draft tree")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "hobby_reassignment_distribution.png", dpi=220)
    plt.close(fig)


def plot_ambiguous_pairs(pair_counts: pd.DataFrame) -> None:
    df = pair_counts.head(20).sort_values("n_channels", ascending=True).copy()
    df["pair"] = df["candidate_a"] + " + " + df["candidate_b"]
    fig, ax = plt.subplots(figsize=(10.5, max(6, 0.34 * len(df))))
    ax.barh(df["pair"], df["pct_nonempty_channels"] * 100, color="#d95f02")
    for i, row in enumerate(df.itertuples(index=False)):
        ax.text(row.pct_nonempty_channels * 100 + 0.03, i, f"{row.pct_nonempty_channels * 100:.2f}%", va="center", fontsize=8)
    ax.set_xlabel("% of nonempty channels")
    ax.set_title("Most common multi-candidate conflicts before tie-breaking")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "top_ambiguous_candidate_pairs.png", dpi=220)
    plt.close(fig)


def markdown_table(df: pd.DataFrame, columns: list[tuple[str, str]], max_rows: int | None = None) -> str:
    if max_rows is not None:
        df = df.head(max_rows)
    rows = ["| " + " | ".join([label for label, _ in columns]) + " |"]
    rows.append("|" + "|".join(["---"] + ["---:"] * (len(columns) - 1)) + "|")
    for row in df.itertuples(index=False):
        cells = []
        for _label, attr in columns:
            value = getattr(row, attr)
            if attr.startswith("pct_"):
                cells.append(f"{float(value) * 100:.2f}%")
            elif attr.startswith("n_"):
                cells.append(f"{int(value):,}")
            else:
                cells.append(str(value))
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def write_report(data: dict[str, pd.DataFrame]) -> None:
    distribution = data["primary_distribution"]
    candidate_counts = data["candidate_counts"]
    specific_candidate_counts = data["specific_candidate_counts"]
    hobby = data["hobby_reassignment"]
    pairs = data["ambiguous_pairs"]
    combo = data["candidate_combos"]
    broad = data["fallback_broad_summary"]
    totals = data["totals"].iloc[0]

    report = f"""# Draft Flat Primary Topic Classifier

Date: 2026-06-15

Output table:

```text
{TABLE_NAME}
```

Goal: derive a single flat one-level label from the existing YouTube topic-category arrays using an explicit decision tree.

Rows in source table: {int(totals.n_channels):,}
Nonempty category arrays: {int(totals.n_nonempty_channels):,}

## Requested Lumps and Splits

- `Film`, `Television_program`, and `Humour` are lumped into `Film/TV/Humor`.
- `Health` and `Physical_fitness` are lumped into `Health/Fitness`.
- `Hobby` is treated as a fallback. If a channel has `Hobby` plus any more specific mapped topic, it is assigned to the more specific topic. It is assigned to `Hobby/General interests` only when Hobby is standalone or only broad parent labels remain.
- All music labels are lumped into `Music`.
- Specific video game genre labels are lumped into early `Video games`; broad `Video_game_culture` is treated as a fallback after more concrete topics.
- Music is primary whenever it is present.
- `Vehicle` and `Motorsport` are lumped into `Vehicles/Motorsport`, which outranks Sports.
- `Performing_arts` is folded into `Film/TV/Humor`.
- `Politics`, `Military`, `Business`, and `Society` are lumped into `News/Society/Politics`.
- `Knowledge` maps to `Education/Explainers`.

## Draft Decision Tree

For a channel with nonempty YouTube topic labels, assign the first matching rule:

1. Any music label -> `Music`
2. Any specific video game genre label except broad `Video_game_culture` -> `Video games`
3. `Vehicle` or `Motorsport` -> `Vehicles/Motorsport`
4. Any non-motorsport sport label, including `Professional_wrestling` -> `Sports`
5. `Religion` -> `Religion`
6. `Politics`, `Military`, `Business`, or `Society` -> `News/Society/Politics`
7. `Food` -> `Food`
8. `Health` or `Physical_fitness` -> `Health/Fitness`
9. `Technology` -> `Technology`
10. `Pet` -> `Pets/Animals`
11. `Fashion` or `Physical_attractiveness` -> `Fashion/Beauty`
12. `Tourism` -> `Travel`
13. `Film`, `Television_program`, `Humour`, or `Performing_arts` -> `Film/TV/Humor`
14. `Knowledge` -> `Education/Explainers`
15. `Hobby`, only when no specific topic matched -> `Hobby/General interests`
16. Broad-only `Lifestyle_(sociology)` -> `Lifestyle/General`
17. Broad `Video_game_culture`, only when no more concrete topic matched -> `Video games`
18. Broad-only `Entertainment` -> `Entertainment/General`
19. Missing/empty/unmapped labels -> `Uncategorized`

## Primary Label Distribution

{markdown_table(distribution, [("Primary label", "primary_flat_label"), ("Channels", "n_nonempty_channels"), ("% nonempty", "pct_nonempty_channels")])}

![Primary flat distribution](primary_flat_distribution.png)

## Candidate Coverage

All mapped candidate counts include broad fallbacks such as `Lifestyle/General`, `Entertainment/General`, `Hobby/General interests`, and broad `Video_game_culture`.

{markdown_table(candidate_counts, [("Mapped candidates", "n_candidate_flat_labels"), ("Channels", "n_nonempty_channels"), ("% nonempty", "pct_nonempty_channels")])}

The all-candidate exact-set table below should be used to detect truly unmapped rows. The old `[none]` value came from the specific-only diagnostic and was not an unclassified primary-label rate.

## Specific-Candidate Diagnostic

Specific candidate counts exclude broad fallback labels such as `Lifestyle/General`, `Entertainment/General`, and `Hobby/General interests`.

{markdown_table(specific_candidate_counts, [("Specific candidates", "n_specific_candidate_flat_labels"), ("Channels", "n_nonempty_channels"), ("% nonempty", "pct_nonempty_channels")])}

![Mapped candidate count distribution](candidate_count_distribution.png)

Most common multi-candidate conflicts:

{markdown_table(pairs, [("Candidate A", "candidate_a"), ("Candidate B", "candidate_b"), ("Channels", "n_channels"), ("% nonempty", "pct_nonempty_channels")], max_rows=15)}

![Top ambiguous candidate pairs](top_ambiguous_candidate_pairs.png)

Most common exact candidate sets, using all mapped candidates including broad fallbacks:

{markdown_table(combo, [("Candidate set", "specific_candidate_combo"), ("Channels", "n_channels"), ("% nonempty", "pct_nonempty_channels")], max_rows=15)}

## Hobby Handling

Channels with the raw `Hobby` label: {int(totals.n_hobby_channels):,}
Assigned to `Hobby/General interests`: {int(totals.n_assigned_hobby):,}
Reassigned from Hobby to a more specific primary label: {int(totals.n_hobby_reassigned):,}

{markdown_table(hobby, [("Assigned primary label", "primary_flat_label"), ("Hobby channels", "n_hobby_channels"), ("% Hobby", "pct_hobby_channels")], max_rows=15)}

![Hobby reassignment distribution](hobby_reassignment_distribution.png)

## Fallback and Broad Assignments

{markdown_table(broad, [("Primary label", "primary_flat_label"), ("Channels", "n_nonempty_channels"), ("% nonempty", "pct_nonempty_channels")])}

## Other Lump/Split Candidates

- `Professional_wrestling`: currently assigned to `Sports` for intuitive topic grouping, even though YouTube often treats it as entertainment. This is a high-priority validation item.
- `Technology`: currently split out from Lifestyle because it is an intuitive main topic. Validate whether YouTube-labeled Technology channels are truly technology-centered or often general hobby/DIY.
- `Education/Explainers`: `Knowledge` remains broad even after renaming, so this bucket should be audited for explainers vs. formal education vs. generic fact channels.
- `Hobby/General interests`: any large residual here means the existing labels do not expose the underlying topic. This bucket should be audited for possible hand-built subrules.

## Statistical Validation Plan

1. Freeze this draft tree and materialized table as version `flat_primary_draft_20260615`.
2. Draw a stratified validation sample by assigned primary label, oversampling rare labels and all fallback/broad labels.
3. Add an ambiguity stratum: sample separately from channels with 0, 1, 2, and 3+ specific candidate labels.
4. Blind-code each sampled channel from channel/video evidence into the proposed flat label set. The coder must not see the YouTube labels or tree output.
5. Use at least two independent coders, or one coder plus an LLM adjudication pass, for high-impact ambiguous strata.
6. Estimate overall accuracy, macro-F1, per-label precision/recall, and confusion matrices against the blinded flat-label judgments.
7. Report Wilson or bootstrap confidence intervals for every label with enough sample size.
8. Specifically audit priority-conflict pairs such as `Film/TV/Humor` vs. `Music`, `Film/TV/Humor` vs. `Sports`, `Vehicles/Motorsport` vs. `Sports`, and `Hobby` reassignments.
9. Revise the decision tree only after reviewing statistically meaningful confusions, then rerun the same validation design on a fresh heldout sample.
"""
    (OUT_DIR / "flat_primary_draft_report.md").write_text(report, encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    execute_sql(CREATE_SQL.read_text(encoding="utf-8"))

    data = {
        "totals": query_df(
            f"""
            SELECT
              COUNT(*) AS n_channels,
              SUM(CASE WHEN category_status = 'nonempty_topic_categories' THEN 1 ELSE 0 END) AS n_nonempty_channels,
              SUM(CASE WHEN category_status = 'nonempty_topic_categories' AND has_hobby_label THEN 1 ELSE 0 END) AS n_hobby_channels,
              SUM(CASE WHEN category_status = 'nonempty_topic_categories' AND assigned_to_hobby_fallback THEN 1 ELSE 0 END) AS n_assigned_hobby,
              SUM(CASE WHEN category_status = 'nonempty_topic_categories' AND has_hobby_label AND NOT assigned_to_hobby_fallback THEN 1 ELSE 0 END) AS n_hobby_reassigned
            FROM {TABLE_NAME}
            """
        ),
        "primary_distribution": query_df(
            f"""
            WITH base AS (
              SELECT * FROM {TABLE_NAME}
              WHERE category_status = 'nonempty_topic_categories'
            ),
            totals AS (SELECT COUNT(*) AS n FROM base)
            SELECT
              primary_flat_label,
              COUNT(*) AS n_nonempty_channels,
              COUNT(*) / MAX(t.n) AS pct_nonempty_channels
            FROM base CROSS JOIN totals t
            GROUP BY primary_flat_label
            ORDER BY n_nonempty_channels DESC, primary_flat_label
            """
        ),
        "candidate_counts": query_df(
            f"""
            WITH base AS (
              SELECT * FROM {TABLE_NAME}
              WHERE category_status = 'nonempty_topic_categories'
            ),
            totals AS (SELECT COUNT(*) AS n FROM base)
            SELECT
              n_candidate_flat_labels,
              COUNT(*) AS n_nonempty_channels,
              COUNT(*) / MAX(t.n) AS pct_nonempty_channels
            FROM base CROSS JOIN totals t
            GROUP BY n_candidate_flat_labels
            ORDER BY n_candidate_flat_labels
            """
        ),
        "specific_candidate_counts": query_df(
            f"""
            WITH base AS (
              SELECT * FROM {TABLE_NAME}
              WHERE category_status = 'nonempty_topic_categories'
            ),
            totals AS (SELECT COUNT(*) AS n FROM base)
            SELECT
              n_specific_candidate_flat_labels,
              COUNT(*) AS n_nonempty_channels,
              COUNT(*) / MAX(t.n) AS pct_nonempty_channels
            FROM base CROSS JOIN totals t
            GROUP BY n_specific_candidate_flat_labels
            ORDER BY n_specific_candidate_flat_labels
            """
        ),
        "hobby_reassignment": query_df(
            f"""
            WITH base AS (
              SELECT * FROM {TABLE_NAME}
              WHERE category_status = 'nonempty_topic_categories' AND has_hobby_label
            ),
            totals AS (SELECT COUNT(*) AS n FROM base)
            SELECT
              primary_flat_label,
              COUNT(*) AS n_hobby_channels,
              COUNT(*) / MAX(t.n) AS pct_hobby_channels
            FROM base CROSS JOIN totals t
            GROUP BY primary_flat_label
            ORDER BY n_hobby_channels DESC, primary_flat_label
            """
        ),
        "ambiguous_pairs": query_df(
            f"""
            WITH base AS (
              SELECT channel_id, specific_candidate_flat_labels
              FROM {TABLE_NAME}
              WHERE category_status = 'nonempty_topic_categories'
                AND n_specific_candidate_flat_labels > 1
            ),
            totals AS (
              SELECT COUNT(*) AS n
              FROM {TABLE_NAME}
              WHERE category_status = 'nonempty_topic_categories'
            ),
            exploded AS (
              SELECT channel_id, candidate
              FROM base LATERAL VIEW explode(specific_candidate_flat_labels) e AS candidate
            ),
            pairs AS (
              SELECT a.channel_id, a.candidate AS candidate_a, b.candidate AS candidate_b
              FROM exploded a
              INNER JOIN exploded b
                ON a.channel_id = b.channel_id
               AND a.candidate < b.candidate
            )
            SELECT
              candidate_a,
              candidate_b,
              COUNT(DISTINCT channel_id) AS n_channels,
              COUNT(DISTINCT channel_id) / MAX(t.n) AS pct_nonempty_channels
            FROM pairs CROSS JOIN totals t
            GROUP BY candidate_a, candidate_b
            ORDER BY n_channels DESC, candidate_a, candidate_b
            """
        ),
        "candidate_combos": query_df(
            f"""
            WITH base AS (
              SELECT
                CASE
                  WHEN n_candidate_flat_labels = 0 THEN '[none]'
                  ELSE concat_ws(' + ', candidate_flat_labels)
                END AS candidate_combo
              FROM {TABLE_NAME}
              WHERE category_status = 'nonempty_topic_categories'
            ),
            totals AS (SELECT COUNT(*) AS n FROM base)
            SELECT
              candidate_combo AS specific_candidate_combo,
              COUNT(*) AS n_channels,
              COUNT(*) / MAX(t.n) AS pct_nonempty_channels
            FROM base CROSS JOIN totals t
            GROUP BY candidate_combo
            ORDER BY n_channels DESC, specific_candidate_combo
            """
        ),
        "fallback_broad_summary": query_df(
            f"""
            WITH base AS (
              SELECT * FROM {TABLE_NAME}
              WHERE category_status = 'nonempty_topic_categories'
                AND primary_candidate_type IN ('fallback', 'broad', 'uncategorized')
            ),
            totals AS (
              SELECT COUNT(*) AS n
              FROM {TABLE_NAME}
              WHERE category_status = 'nonempty_topic_categories'
            )
            SELECT
              primary_flat_label,
              COUNT(*) AS n_nonempty_channels,
              COUNT(*) / MAX(t.n) AS pct_nonempty_channels
            FROM base CROSS JOIN totals t
            GROUP BY primary_flat_label
            ORDER BY n_nonempty_channels DESC, primary_flat_label
            """
        ),
    }
    write_csvs(data)
    plot_distribution(data["primary_distribution"])
    plot_candidate_counts(data["candidate_counts"])
    plot_hobby(data["hobby_reassignment"])
    plot_ambiguous_pairs(data["ambiguous_pairs"])
    write_report(data)
    print(json.dumps({"out_dir": str(OUT_DIR), "table": TABLE_NAME}, sort_keys=True))


if __name__ == "__main__":
    main()
