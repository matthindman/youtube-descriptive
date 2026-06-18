#!/usr/bin/env python3
"""Evaluate iterative flat-topic decision-tree variants.

This script uses the existing 1,000-channel multi-label LLM validation run as a
heldout scoring set by collapsing each model-predicted label set and each
reference label set to one project flat label under alternative decision trees.
It also samples Film/TV/Humor + Video-games collisions from the full channel
universe for evidence inspection.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import textwrap
import time
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROFILE = os.environ.get("DATABRICKS_PROFILE", "matt.hindman@researchaccelerator.org")
WAREHOUSE_ID = "86100da4e1fe8713"
OUT_DIR = ROOT / "artifacts" / "category_taxonomy_estimation_20260612" / "flat_primary_validation_iteration_20260615"
VALIDATION_TABLE = "dev_sean.matt.yt_channel_topic_flat_validation_iteration_20260615"
RUN_ID = "category_topic_multilabel_random_1000_20260612"
PRIMARY_PROVIDER = "gemini"
PRIMARY_MODEL = "gemini-3.5-flash"
PRIMARY_VARIANT = "prob_label_threshold_closure_postprocessed"


LABELS = {
    "video_game": [
        "Video_game_culture",
        "Action_game",
        "Action-adventure_game",
        "Casual_game",
        "Music_video_game",
        "Puzzle_video_game",
        "Racing_video_game",
        "Role-playing_video_game",
        "Simulation_video_game",
        "Sports_game",
        "Strategy_video_game",
    ],
    "music": [
        "Music",
        "Christian_music",
        "Classical_music",
        "Country_music",
        "Electronic_music",
        "Hip_hop_music",
        "Independent_music",
        "Jazz",
        "Music_of_Asia",
        "Music_of_Latin_America",
        "Pop_music",
        "Reggae",
        "Rhythm_and_blues",
        "Rock_music",
        "Soul_music",
    ],
    "film_tv_humor": ["Film", "Television_program", "Humour"],
    "sports": [
        "Sport",
        "Association_football",
        "American_football",
        "Baseball",
        "Basketball",
        "Boxing",
        "Cricket",
        "Golf",
        "Ice_hockey",
        "Mixed_martial_arts",
        "Motorsport",
        "Professional_wrestling",
        "Tennis",
        "Volleyball",
    ],
    "health_fitness": ["Health", "Physical_fitness"],
    "fashion_beauty": ["Fashion", "Physical_attractiveness"],
}


LABELS["specific_video_game"] = [
    label for label in LABELS["video_game"] if label != "Video_game_culture"
]
LABELS["video_game_culture"] = ["Video_game_culture"]
LABELS["vehicles_motorsport"] = ["Vehicle", "Motorsport"]
LABELS["film_tv_humor_performing_arts"] = LABELS["film_tv_humor"] + ["Performing_arts"]
LABELS["news_society_politics"] = ["Politics", "Military", "Business", "Society"]
SPORTS_WITHOUT_MOTORSPORT = [label for label in LABELS["sports"] if label != "Motorsport"]
SPORTS_WITHOUT_WRESTLING = [label for label in LABELS["sports"] if label != "Professional_wrestling"]


RULE_ORDERS = {
    "v0_prior_draft": [
        ("Video games", "video_game"),
        ("Music", "music"),
        ("Film/TV/Humor", "film_tv_humor"),
        ("Sports", "sports"),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Vehicles", ["Vehicle"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Performing arts", ["Performing_arts"]),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v1_music_vehicle_priority": [
        ("Music", "music"),
        ("Video games", "video_game"),
        ("Film/TV/Humor", "film_tv_humor"),
        ("Vehicles", ["Vehicle"]),
        ("Sports", "sports"),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Performing arts", ["Performing_arts"]),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v2_film_over_video_game": [
        ("Music", "music"),
        ("Film/TV/Humor", "film_tv_humor"),
        ("Video games", "video_game"),
        ("Vehicles", ["Vehicle"]),
        ("Sports", "sports"),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Performing arts", ["Performing_arts"]),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v3_motorsport_to_vehicles": [
        ("Music", "music"),
        ("Video games", "video_game"),
        ("Film/TV/Humor", "film_tv_humor"),
        ("Vehicles", ["Vehicle", "Motorsport"]),
        ("Sports", SPORTS_WITHOUT_MOTORSPORT),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Performing arts", ["Performing_arts"]),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v4_wrestling_to_film_tv": [
        ("Music", "music"),
        ("Video games", "video_game"),
        ("Film/TV/Humor", ["Film", "Television_program", "Humour", "Professional_wrestling"]),
        ("Vehicles", ["Vehicle"]),
        ("Sports", SPORTS_WITHOUT_WRESTLING),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Performing arts", ["Performing_arts"]),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v5_performing_arts_to_film_tv": [
        ("Music", "music"),
        ("Video games", "video_game"),
        ("Film/TV/Humor", ["Film", "Television_program", "Humour", "Performing_arts"]),
        ("Vehicles", ["Vehicle"]),
        ("Sports", "sports"),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v6_military_to_politics_news": [
        ("Music", "music"),
        ("Video games", "video_game"),
        ("Film/TV/Humor", "film_tv_humor"),
        ("Vehicles", ["Vehicle"]),
        ("Sports", "sports"),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics", "Military"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Performing arts", ["Performing_arts"]),
        ("Business", ["Business"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v7_business_to_politics_news": [
        ("Music", "music"),
        ("Video games", "video_game"),
        ("Film/TV/Humor", "film_tv_humor"),
        ("Vehicles", ["Vehicle"]),
        ("Sports", "sports"),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics", "Business"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Performing arts", ["Performing_arts"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v8_food_before_film_tv": [
        ("Music", "music"),
        ("Video games", "video_game"),
        ("Food", ["Food"]),
        ("Film/TV/Humor", "film_tv_humor"),
        ("Vehicles", ["Vehicle"]),
        ("Sports", "sports"),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Performing arts", ["Performing_arts"]),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v9_film_after_concrete_topics": [
        ("Music", "music"),
        ("Video games", "video_game"),
        ("Vehicles", ["Vehicle"]),
        ("Sports", "sports"),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Film/TV/Humor", "film_tv_humor"),
        ("Performing arts", ["Performing_arts"]),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v10_split_broad_video_game_culture": [
        ("Music", "music"),
        ("Video games", "specific_video_game"),
        ("Vehicles", ["Vehicle"]),
        ("Sports", "sports"),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Film/TV/Humor", "film_tv_humor"),
        ("Performing arts", ["Performing_arts"]),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Video games", "video_game_culture"),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v11_motorsport_vehicle_performing_film": [
        ("Music", "music"),
        ("Video games", "specific_video_game"),
        ("Vehicles/Motorsport", "vehicles_motorsport"),
        ("Sports", SPORTS_WITHOUT_MOTORSPORT),
        ("Religion", ["Religion"]),
        ("Politics/News", ["Politics"]),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Film/TV/Humor", "film_tv_humor_performing_arts"),
        ("Business", ["Business"]),
        ("Military", ["Military"]),
        ("Education/Knowledge", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Society/General", ["Society"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Video games", "video_game_culture"),
        ("Entertainment/General", ["Entertainment"]),
    ],
    "v12_news_society_politics_explainers": [
        ("Music", "music"),
        ("Video games", "specific_video_game"),
        ("Vehicles/Motorsport", "vehicles_motorsport"),
        ("Sports", SPORTS_WITHOUT_MOTORSPORT),
        ("Religion", ["Religion"]),
        ("News/Society/Politics", "news_society_politics"),
        ("Food", ["Food"]),
        ("Health/Fitness", "health_fitness"),
        ("Technology", ["Technology"]),
        ("Pets/Animals", ["Pet"]),
        ("Fashion/Beauty", "fashion_beauty"),
        ("Travel", ["Tourism"]),
        ("Film/TV/Humor", "film_tv_humor_performing_arts"),
        ("Education/Explainers", ["Knowledge"]),
        ("Hobby/General interests", ["Hobby"]),
        ("Lifestyle/General", ["Lifestyle_(sociology)"]),
        ("Video games", "video_game_culture"),
        ("Entertainment/General", ["Entertainment"]),
    ],
}


GAME_TERMS = [
    r"\bgameplay\b",
    r"\bwalkthrough\b",
    r"\bplaythrough\b",
    r"\blet'?s play\b",
    r"\bgaming\b",
    r"\bgamer\b",
    r"\blivestream\b",
    r"\bstream\b",
    r"\bmods?\b",
    r"\bspeedrun\b",
    r"\bboss\b",
    r"\blevel\b",
    r"\bquest\b",
    r"\bbuild\b",
    r"\bminecraft\b",
    r"\broblox\b",
    r"\bfortnite\b",
    r"\bvalorant\b",
    r"\bgta\b",
    r"\bgrand theft auto\b",
    r"\bcall of duty\b",
    r"\bwarzone\b",
    r"\bpokemon\b",
    r"\bpok[eé]mon\b",
    r"\bzelda\b",
    r"\bmario\b",
    r"\bfnaf\b",
    r"\bfive nights\b",
    r"\belden ring\b",
    r"\bskyrim\b",
    r"\bfree fire\b",
    r"\bpubg\b",
    r"\bleague of legends\b",
    r"\bmobile legends\b",
    r"\bclash royale\b",
    r"\bamong us\b",
    r"\bsims?\b",
]

MEDIA_TERMS = [
    r"\bmovie\b",
    r"\bfilm\b",
    r"\btv\b",
    r"\btelevision\b",
    r"\btrailer\b",
    r"\bepisode\b",
    r"\bseason\b",
    r"\bseries\b",
    r"\bscene\b",
    r"\bclip\b",
    r"\breaction\b",
    r"\breview\b",
    r"\banime\b",
    r"\banimation\b",
    r"\banimated\b",
    r"\bcartoon\b",
    r"\bshort film\b",
    r"\bnetflix\b",
    r"\bdisney\b",
    r"\bmarvel\b",
    r"\bdc\b",
]


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


def sql_array(values: list[str]) -> str:
    quoted = ["'" + value.replace("'", "''") + "'" for value in values]
    return "array(" + ", ".join(quoted) + ")"


def resolve_labels(labels_or_key: str | list[str]) -> list[str]:
    if isinstance(labels_or_key, str):
        return LABELS[labels_or_key]
    return labels_or_key


def flat_case_expr(labels_col: str, variant_name: str) -> str:
    clauses = []
    for flat_label, labels_or_key in RULE_ORDERS[variant_name]:
        labels = resolve_labels(labels_or_key)
        condition = f"size(array_intersect({labels_col}, {sql_array(labels)})) > 0"
        escaped_label = flat_label.replace("'", "''")
        clauses.append(f"WHEN {condition} THEN '{escaped_label}'")
    return "CASE\n    " + "\n    ".join(clauses) + "\n    ELSE 'Uncategorized'\n  END"


def create_validation_table() -> None:
    variant_selects = []
    for variant_name in RULE_ORDERS:
        ref_expr = flat_case_expr("reference_labels", variant_name)
        pred_expr = flat_case_expr("predicted_labels", variant_name)
        variant_selects.append(
            f"""
            SELECT
              '{variant_name}' AS rule_variant,
              run_id,
              provider,
              model,
              prediction_variant,
              eval_split,
              channel_id,
              reference_labels,
              predicted_labels,
              {ref_expr} AS reference_flat_label,
              {pred_expr} AS predicted_flat_label,
              size(array_intersect(reference_labels, {sql_array(LABELS['film_tv_humor'])})) > 0
                AND size(array_intersect(reference_labels, {sql_array(LABELS['video_game'])})) > 0
                AS reference_film_video_game_collision,
              size(array_intersect(predicted_labels, {sql_array(LABELS['film_tv_humor'])})) > 0
                AND size(array_intersect(predicted_labels, {sql_array(LABELS['video_game'])})) > 0
                AS predicted_film_video_game_collision
            FROM parsed
            """
        )

    sql = f"""
    CREATE OR REPLACE TABLE {VALIDATION_TABLE} AS
    WITH parsed AS (
      SELECT
        run_id,
        provider,
        model,
        prediction_variant,
        eval_split,
        channel_id,
        coalesce(from_json(reference_labels_json, 'array<string>'), array()) AS reference_labels,
        coalesce(from_json(predicted_labels_json, 'array<string>'), array()) AS predicted_labels
      FROM dev_sean.matt.yt_category_topic_multilabel_1000_channel_metrics
      WHERE run_id = '{RUN_ID}'
    )
    {" UNION ALL ".join(variant_selects)}
    """
    execute_sql(sql)


def fetch_validation_metrics() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    metrics = query_df(
        f"""
        SELECT
          rule_variant,
          provider,
          model,
          prediction_variant,
          eval_split,
          COUNT(*) AS n_channels,
          AVG(CASE WHEN predicted_flat_label = reference_flat_label THEN 1.0 ELSE 0.0 END) AS flat_accuracy,
          SUM(CASE WHEN predicted_flat_label = reference_flat_label THEN 1 ELSE 0 END) AS n_correct,
          SUM(CASE WHEN reference_film_video_game_collision THEN 1 ELSE 0 END) AS n_reference_film_video_game_collision,
          AVG(CASE WHEN reference_film_video_game_collision THEN
            CASE WHEN predicted_flat_label = reference_flat_label THEN 1.0 ELSE 0.0 END
          END) AS film_video_game_collision_accuracy
        FROM {VALIDATION_TABLE}
        GROUP BY rule_variant, provider, model, prediction_variant, eval_split
        ORDER BY eval_split, flat_accuracy DESC, provider, model, prediction_variant, rule_variant
        """
    )
    primary = query_df(
        f"""
        SELECT
          rule_variant,
          COUNT(*) AS n_channels,
          AVG(CASE WHEN predicted_flat_label = reference_flat_label THEN 1.0 ELSE 0.0 END) AS flat_accuracy,
          SUM(CASE WHEN predicted_flat_label = reference_flat_label THEN 1 ELSE 0 END) AS n_correct,
          SUM(CASE WHEN reference_film_video_game_collision THEN 1 ELSE 0 END) AS n_reference_film_video_game_collision,
          AVG(CASE WHEN reference_film_video_game_collision THEN
            CASE WHEN predicted_flat_label = reference_flat_label THEN 1.0 ELSE 0.0 END
          END) AS film_video_game_collision_accuracy
        FROM {VALIDATION_TABLE}
        WHERE eval_split = 'heldout_test'
          AND provider = '{PRIMARY_PROVIDER}'
          AND model = '{PRIMARY_MODEL}'
          AND prediction_variant = '{PRIMARY_VARIANT}'
        GROUP BY rule_variant
        ORDER BY flat_accuracy DESC, rule_variant
        """
    )
    mean_across_models = query_df(
        f"""
        WITH per_model AS (
          SELECT
            rule_variant,
            provider,
            model,
            prediction_variant,
            AVG(CASE WHEN predicted_flat_label = reference_flat_label THEN 1.0 ELSE 0.0 END) AS flat_accuracy
          FROM {VALIDATION_TABLE}
          WHERE eval_split = 'heldout_test'
          GROUP BY rule_variant, provider, model, prediction_variant
        )
        SELECT
          rule_variant,
          COUNT(*) AS n_model_variants,
          AVG(flat_accuracy) AS mean_flat_accuracy,
          percentile_approx(flat_accuracy, 0.5) AS median_flat_accuracy,
          MAX(flat_accuracy) AS max_flat_accuracy
        FROM per_model
        GROUP BY rule_variant
        ORDER BY mean_flat_accuracy DESC, rule_variant
        """
    )
    return metrics, primary, mean_across_models


def fetch_collision_sample() -> pd.DataFrame:
    sql = f"""
    WITH collision AS (
      SELECT channel_id, topic_categories
      FROM dev_sean.matt.yt_channel_topic_taxonomy_channel_labels_20260612
      WHERE size(array_intersect(topic_categories, {sql_array(LABELS['video_game'])})) > 0
        AND size(array_intersect(topic_categories, {sql_array(LABELS['film_tv_humor'])})) > 0
    ),
    sampled AS (
      SELECT *
      FROM collision
      ORDER BY xxhash64(channel_id, 'flat_film_vg_collision_20260615')
      LIMIT 100
    ),
    video_ranked AS (
      SELECT
        s.channel_id,
        ROW_NUMBER() OVER (
          PARTITION BY s.channel_id
          ORDER BY v.published_at DESC NULLS LAST, v.video_id ASC NULLS LAST
        ) AS rn,
        concat(
          '[',
          CAST(ROW_NUMBER() OVER (
            PARTITION BY s.channel_id
            ORDER BY v.published_at DESC NULLS LAST, v.video_id ASC NULLS LAST
          ) AS STRING),
          '] Title: ',
          substring(regexp_replace(coalesce(v.video_title, ''), '[\\r\\n\\t]+', ' '), 1, 240),
          CASE WHEN length(coalesce(v.description, '')) > 0
            THEN concat(' | Description: ', substring(regexp_replace(coalesce(v.description, ''), '[\\r\\n\\t]+', ' '), 1, 180))
            ELSE ''
          END
        ) AS video_line
      FROM sampled s
      LEFT JOIN prod_tads.youtube_too.yt_sl_videos v
        ON s.channel_id = v.channel_id
    ),
    video_agg AS (
      SELECT
        channel_id,
        array_join(transform(array_sort(collect_list(named_struct('rn', rn, 'line', video_line))), x -> x.line), '\\n') AS recent_video_titles
      FROM video_ranked
      WHERE rn <= 8
      GROUP BY channel_id
    )
    SELECT
      s.channel_id,
      c.channel_name,
      s.topic_categories,
      coalesce(v.recent_video_titles, '') AS recent_video_titles
    FROM sampled s
    LEFT JOIN prod_tads.youtube_too.yt_sl_channels c
      ON s.channel_id = c.channel_id
    LEFT JOIN video_agg v
      ON s.channel_id = v.channel_id
    ORDER BY xxhash64(s.channel_id, 'flat_film_vg_collision_20260615')
    """
    return query_df(sql)


def score_patterns(text: str, patterns: list[str]) -> tuple[int, list[str]]:
    hits: list[str] = []
    for pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            hits.append(pattern.strip("\\b"))
    return len(hits), hits[:8]


def assess_collision(row: pd.Series) -> pd.Series:
    text = f"{row.get('channel_name', '')}\n{row.get('recent_video_titles', '')}"
    game_score, game_hits = score_patterns(text, GAME_TERMS)
    media_score, media_hits = score_patterns(text, MEDIA_TERMS)
    if game_score >= media_score + 1 and game_score >= 1:
        assessment = "mostly_video_game_play_or_discussion"
    elif media_score >= game_score + 1 and media_score >= 1:
        assessment = "mostly_film_tv_humor_or_adaptation"
    elif game_score > 0 and media_score > 0:
        assessment = "mixed_or_ambiguous"
    else:
        assessment = "insufficient_keyword_evidence"
    return pd.Series(
        {
            "game_keyword_score": game_score,
            "media_keyword_score": media_score,
            "game_keyword_hits": ", ".join(game_hits),
            "media_keyword_hits": ", ".join(media_hits),
            "collision_assessment": assessment,
        }
    )


def save_plots(primary: pd.DataFrame, mean_across_models: pd.DataFrame) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plot_df = primary.sort_values("rule_variant")
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    ax.bar(plot_df["rule_variant"], plot_df["flat_accuracy"] * 100, color="#4575b4")
    for i, row in enumerate(plot_df.itertuples(index=False)):
        ax.text(i, row.flat_accuracy * 100 + 0.5, f"{row.flat_accuracy * 100:.1f}%", ha="center", fontsize=9)
    ax.set_ylim(0, max(100, plot_df["flat_accuracy"].max() * 110))
    ax.set_ylabel("Heldout flat accuracy (%)")
    ax.set_title(f"Primary scorer: {PRIMARY_MODEL} / {PRIMARY_VARIANT}")
    ax.tick_params(axis="x", rotation=20)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "primary_scorer_rule_variant_accuracy.png", dpi=220)
    plt.close(fig)

    base_primary = float(plot_df.loc[plot_df["rule_variant"] == "v0_prior_draft", "flat_accuracy"].iloc[0])
    delta_df = plot_df.copy()
    delta_df["delta_pp"] = (delta_df["flat_accuracy"] - base_primary) * 100
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    colors = ["#4575b4" if value >= 0 else "#d95f02" for value in delta_df["delta_pp"]]
    ax.barh(delta_df["rule_variant"], delta_df["delta_pp"], color=colors)
    ax.axvline(0, color="black", linewidth=1)
    for i, row in enumerate(delta_df.itertuples(index=False)):
        ax.text(row.delta_pp + (0.01 if row.delta_pp >= 0 else -0.01), i, f"{row.delta_pp:+.2f} pp", va="center", ha="left" if row.delta_pp >= 0 else "right", fontsize=9)
    ax.set_xlabel("Accuracy change vs prior draft (percentage points)")
    ax.set_title("Primary scorer rule-variant delta")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "primary_scorer_rule_variant_delta.png", dpi=220)
    plt.close(fig)

    plot_df = mean_across_models.sort_values("rule_variant")
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    ax.bar(plot_df["rule_variant"], plot_df["mean_flat_accuracy"] * 100, color="#1b9e77")
    for i, row in enumerate(plot_df.itertuples(index=False)):
        ax.text(i, row.mean_flat_accuracy * 100 + 0.5, f"{row.mean_flat_accuracy * 100:.1f}%", ha="center", fontsize=9)
    ax.set_ylim(0, max(100, plot_df["mean_flat_accuracy"].max() * 110))
    ax.set_ylabel("Mean heldout flat accuracy across model variants (%)")
    ax.set_title("Mean rule-variant accuracy across all evaluated LLM variants")
    ax.tick_params(axis="x", rotation=20)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "mean_model_rule_variant_accuracy.png", dpi=220)
    plt.close(fig)

    base_mean = float(plot_df.loc[plot_df["rule_variant"] == "v0_prior_draft", "mean_flat_accuracy"].iloc[0])
    delta_df = plot_df.copy()
    delta_df["delta_pp"] = (delta_df["mean_flat_accuracy"] - base_mean) * 100
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    colors = ["#1b9e77" if value >= 0 else "#d95f02" for value in delta_df["delta_pp"]]
    ax.barh(delta_df["rule_variant"], delta_df["delta_pp"], color=colors)
    ax.axvline(0, color="black", linewidth=1)
    for i, row in enumerate(delta_df.itertuples(index=False)):
        ax.text(row.delta_pp + (0.03 if row.delta_pp >= 0 else -0.03), i, f"{row.delta_pp:+.2f} pp", va="center", ha="left" if row.delta_pp >= 0 else "right", fontsize=9)
    ax.set_xlabel("Mean accuracy change vs prior draft (percentage points)")
    ax.set_title("Mean model rule-variant delta")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "mean_model_rule_variant_delta.png", dpi=220)
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
            if attr.endswith("accuracy") or attr.startswith("pct_") or attr in {"flat_accuracy", "mean_flat_accuracy", "median_flat_accuracy", "max_flat_accuracy"}:
                cells.append(f"{float(value) * 100:.2f}%")
            elif attr.startswith("n_"):
                cells.append(f"{int(value):,}")
            else:
                cells.append(str(value))
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def write_report(primary: pd.DataFrame, mean_across_models: pd.DataFrame, sample: pd.DataFrame) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    primary_sorted = primary.sort_values("rule_variant").copy()
    v0 = float(primary_sorted.loc[primary_sorted["rule_variant"] == "v0_prior_draft", "flat_accuracy"].iloc[0])
    v1 = float(primary_sorted.loc[primary_sorted["rule_variant"] == "v1_music_vehicle_priority", "flat_accuracy"].iloc[0])
    v2 = float(primary_sorted.loc[primary_sorted["rule_variant"] == "v2_film_over_video_game", "flat_accuracy"].iloc[0])
    music_vehicle_gain = v1 - v0
    film_over_game_gain = v2 - v1
    best_primary = primary.sort_values(["flat_accuracy", "rule_variant"], ascending=[False, True]).iloc[0]
    best_additional_gain = float(best_primary.flat_accuracy) - v1
    chosen_collision = "Video games stays above Film/TV/Humor" if film_over_game_gain <= 0.01 else "Film/TV/Humor moves above Video games"
    stop_decision = "stop" if max(0.0, best_additional_gain) <= 0.01 else "continue"

    assessment_counts = (
        sample.groupby("collision_assessment", dropna=False)
        .size()
        .reset_index(name="n_cases")
        .sort_values("n_cases", ascending=False)
    )
    assessment_counts["pct_cases"] = assessment_counts["n_cases"] / max(1, len(sample))

    examples = sample[
        [
            "channel_name",
            "collision_assessment",
            "game_keyword_score",
            "media_keyword_score",
            "game_keyword_hits",
            "media_keyword_hits",
        ]
    ].head(20)

    report = f"""# Flat Primary Topic Validation Iteration

Date: 2026-06-15

Validation table:

```text
{VALIDATION_TABLE}
```

Primary scorer:

```text
{PRIMARY_PROVIDER} / {PRIMARY_MODEL} / {PRIMARY_VARIANT}
```

## Rule Variants

- `v0_prior_draft`: original draft tree, where Video games outranks Music and Sports outranks Vehicles.
- `v1_music_vehicle_priority`: Music outranks every other topic when present; Vehicles outranks Sports.
- `v2_film_over_video_game`: same as `v1`, but Film/TV/Humor outranks Video games.
- `v3_motorsport_to_vehicles`: same as `v1`, but Motorsport maps to Vehicles.
- `v4_wrestling_to_film_tv`: same as `v1`, but Professional_wrestling maps to Film/TV/Humor.
- `v5_performing_arts_to_film_tv`: same as `v1`, but Performing_arts maps to Film/TV/Humor.
- `v6_military_to_politics_news`: same as `v1`, but Military maps to Politics/News.
- `v7_business_to_politics_news`: same as `v1`, but Business maps to Politics/News.
- `v8_food_before_film_tv`: same as `v1`, but Food outranks Film/TV/Humor.

## Heldout Flat Accuracy

{markdown_table(primary_sorted, [("Rule variant", "rule_variant"), ("Channels", "n_channels"), ("Flat accuracy", "flat_accuracy"), ("Correct", "n_correct"), ("Film+game collisions", "n_reference_film_video_game_collision"), ("Collision accuracy", "film_video_game_collision_accuracy")])}

![Primary scorer rule variant accuracy](primary_scorer_rule_variant_accuracy.png)

![Primary scorer rule variant delta](primary_scorer_rule_variant_delta.png)

Mean across all heldout model/prediction-variant combinations:

{markdown_table(mean_across_models.sort_values("rule_variant"), [("Rule variant", "rule_variant"), ("Model variants", "n_model_variants"), ("Mean accuracy", "mean_flat_accuracy"), ("Median accuracy", "median_flat_accuracy"), ("Max accuracy", "max_flat_accuracy")])}

![Mean model rule variant accuracy](mean_model_rule_variant_accuracy.png)

![Mean model rule variant delta](mean_model_rule_variant_delta.png)

## Iteration Decision

- Music + Vehicle priority gain over prior draft: {music_vehicle_gain * 100:.2f} percentage points.
- Film-over-game gain after Music + Vehicle priority: {film_over_game_gain * 100:.2f} percentage points.
- Best tested variant after the requested Music/Vehicle change: `{best_primary.rule_variant}` at {float(best_primary.flat_accuracy) * 100:.2f}% heldout flat accuracy.
- Best additional gain over the requested Music/Vehicle tree: {best_additional_gain * 100:.2f} percentage points.
- Collision decision: {chosen_collision}.
- Iteration status under the 1 percentage point rule: `{stop_decision}`.

## Film/TV/Humor + Video-games Evidence Sample

The 100-case sample was drawn from the full `youtube_too` channel universe among channels whose observed YouTube labels include both a Film/TV/Humor label and a video-game label. The assessment below is a keyword-assisted inspection of channel names and recent video titles/descriptions; it is not treated as final human gold-standard coding.

{markdown_table(assessment_counts, [("Assessment", "collision_assessment"), ("Cases", "n_cases"), ("% cases", "pct_cases")])}

Top sampled examples:

{markdown_table(examples, [("Channel", "channel_name"), ("Assessment", "collision_assessment"), ("Game score", "game_keyword_score"), ("Media score", "media_keyword_score"), ("Game hits", "game_keyword_hits"), ("Media hits", "media_keyword_hits")])}

## Validation Plan Update

The flat-label validation plan should treat Film/TV/Humor + Video-games as an explicit ambiguity stratum. In each validation wave:

1. Score the deterministic tree on the heldout LLM validation set after collapsing model-predicted and reference label sets.
2. Separately report channels with both Film/TV/Humor and Video-game labels.
3. Blind-inspect a supplemental sample of that collision using channel/video evidence.
4. Move the Film/TV/Humor vs Video-games priority only if it improves heldout flat accuracy by more than 1 percentage point and the evidence inspection supports the semantic move.
5. Stop iterating when the next candidate change improves heldout flat accuracy by no more than 1 percentage point.
"""
    (OUT_DIR / "flat_primary_validation_iteration_report.md").write_text(report, encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    create_validation_table()
    metrics, primary, mean_across_models = fetch_validation_metrics()
    collision_sample = fetch_collision_sample()
    assessed = pd.concat([collision_sample, collision_sample.apply(assess_collision, axis=1)], axis=1)

    metrics.to_csv(OUT_DIR / "all_rule_variant_model_metrics.csv", index=False)
    primary.to_csv(OUT_DIR / "primary_scorer_rule_variant_metrics.csv", index=False)
    mean_across_models.to_csv(OUT_DIR / "mean_model_rule_variant_metrics.csv", index=False)
    assessed.to_csv(OUT_DIR / "film_tv_humor_video_game_collision_sample_100.csv", index=False)

    save_plots(primary, mean_across_models)
    write_report(primary, mean_across_models, assessed)
    print(f"Wrote {OUT_DIR}")


if __name__ == "__main__":
    main()
