# Databricks notebook source
import json
import os

from pyspark.sql import functions as F
from pyspark.sql import types as T


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
_create_text_widget("max_cases", "30")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
AUDIT_TABLE = _get_widget("audit_table", "yt_lid_v3_too_full_20260609_llm_hardcase_disagreement_audit")
MAX_CASES = int(_get_widget("max_cases", "30"))


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


dist_schema = T.ArrayType(
    T.StructType(
        [
            T.StructField("normalized_base_iso", T.StringType()),
            T.StructField("normalized_language_label", T.StringType()),
            T.StructField("n", T.IntegerType()),
            T.StructField("providers", T.ArrayType(T.StringType())),
            T.StructField("models", T.ArrayType(T.StringType())),
        ]
    )
)

votes_schema = T.ArrayType(
    T.StructType(
        [
            T.StructField("provider", T.StringType()),
            T.StructField("model", T.StringType()),
            T.StructField("model_tier", T.StringType()),
            T.StructField("language_label", T.StringType()),
            T.StructField("normalized_base_iso", T.StringType()),
            T.StructField("normalized_language_label", T.StringType()),
            T.StructField("primary_language_script", T.StringType()),
            T.StructField("is_mixed_language", T.BooleanType()),
            T.StructField("is_romanized", T.BooleanType()),
            T.StructField("confidence", T.StringType()),
            T.StructField("outside_base_majority", T.BooleanType()),
            T.StructField("outside_label_majority", T.BooleanType()),
            T.StructField("evidence", T.StringType()),
        ]
    )
)

segments_schema = T.ArrayType(
    T.StructType(
        [
            T.StructField("segment_type", T.StringType()),
            T.StructField("text", T.StringType()),
            T.StructField("is_valid_text_for_lid", T.BooleanType()),
            T.StructField("clean_letter_count", T.IntegerType()),
            T.StructField("dominant_script", T.StringType()),
        ]
    )
)

audit = (
    spark.table(fqtn(AUDIT_TABLE))
    .where(F.col("run_id") == F.lit(RUN_ID))
    .withColumn("base_dist", F.from_json("base_vote_distribution_json", dist_schema))
    .withColumn("label_dist", F.from_json("label_vote_distribution_json", dist_schema))
    .withColumn("votes", F.from_json("model_votes_json", votes_schema))
    .withColumn("segments", F.from_json("top_segments_json", segments_schema))
    .persist()
)
significant = audit.where(F.col("base_dissenting_models") > 1).persist()

flag_counts = [
    row.asDict(recursive=True)
    for row in (
        audit.select(F.explode_outer("probable_issue_flags").alias("flag"))
        .where(F.col("flag").isNotNull())
        .groupBy("flag")
        .agg(F.count(F.lit(1)).alias("n_cases"))
        .orderBy(F.desc("n_cases"), "flag")
        .collect()
    )
]

model_outliers = [
    row.asDict(recursive=True)
    for row in (
        audit.select(F.explode("votes").alias("v"))
        .select(
            F.concat_ws(":", F.col("v.provider"), F.col("v.model")).alias("model_key"),
            F.col("v.model_tier").alias("model_tier"),
            F.coalesce(F.col("v.outside_base_majority"), F.lit(False)).alias("outside_base"),
            F.coalesce(F.col("v.outside_label_majority"), F.lit(False)).alias("outside_label"),
        )
        .groupBy("model_key", "model_tier")
        .agg(
            F.count(F.lit(1)).alias("n_valid_case_votes"),
            F.sum(F.col("outside_base").cast("int")).alias("n_outside_base_majority"),
            F.sum(F.col("outside_label").cast("int")).alias("n_outside_label_majority"),
        )
        .withColumn("outside_base_rate", F.round(F.col("n_outside_base_majority") / F.col("n_valid_case_votes"), 4))
        .orderBy(F.desc("n_outside_base_majority"), "model_key")
        .collect()
    )
]

base_split_patterns = [
    row.asDict(recursive=True)
    for row in (
        audit.withColumn(
            "base_pattern",
            F.array_join(F.sort_array(F.transform("base_dist", lambda x: x["normalized_base_iso"])), "|"),
        )
        .groupBy("base_pattern")
        .agg(F.count(F.lit(1)).alias("n_cases"))
        .orderBy(F.desc("n_cases"), "base_pattern")
        .limit(25)
        .collect()
    )
]

significant_flag_counts = [
    row.asDict(recursive=True)
    for row in (
        significant.select(F.explode_outer("probable_issue_flags").alias("flag"))
        .where(F.col("flag").isNotNull())
        .groupBy("flag")
        .agg(F.count(F.lit(1)).alias("n_cases"))
        .orderBy(F.desc("n_cases"), "flag")
        .collect()
    )
]

significant_base_split_patterns = [
    row.asDict(recursive=True)
    for row in (
        significant.withColumn(
            "base_pattern",
            F.array_join(F.sort_array(F.transform("base_dist", lambda x: x["normalized_base_iso"])), "|"),
        )
        .groupBy("base_pattern")
        .agg(F.count(F.lit(1)).alias("n_cases"))
        .orderBy(F.desc("n_cases"), "base_pattern")
        .limit(25)
        .collect()
    )
]

significant_model_outliers = [
    row.asDict(recursive=True)
    for row in (
        significant.select(F.explode("votes").alias("v"))
        .select(
            F.concat_ws(":", F.col("v.provider"), F.col("v.model")).alias("model_key"),
            F.col("v.model_tier").alias("model_tier"),
            F.coalesce(F.col("v.outside_base_majority"), F.lit(False)).alias("outside_base"),
            F.coalesce(F.col("v.outside_label_majority"), F.lit(False)).alias("outside_label"),
        )
        .groupBy("model_key", "model_tier")
        .agg(
            F.count(F.lit(1)).alias("n_valid_case_votes"),
            F.sum(F.col("outside_base").cast("int")).alias("n_outside_base_majority"),
            F.sum(F.col("outside_label").cast("int")).alias("n_outside_label_majority"),
        )
        .withColumn("outside_base_rate", F.round(F.col("n_outside_base_majority") / F.col("n_valid_case_votes"), 4))
        .orderBy(F.desc("n_outside_base_majority"), "model_key")
        .collect()
    )
]

majority_counts = [
    row.asDict(recursive=True)
    for row in (
        audit.groupBy("majority_normalized_base_iso")
        .agg(F.count(F.lit(1)).alias("n_cases"))
        .orderBy(F.desc("n_cases"), "majority_normalized_base_iso")
        .limit(25)
        .collect()
    )
]

compact_cases = (
    audit.where(F.col("base_dissenting_models") > 1)
    .withColumn(
        "base_distribution",
        F.array_join(
            F.transform("base_dist", lambda x: F.concat_ws(":", x["normalized_base_iso"], x["n"].cast("string"))),
            ", ",
        ),
    )
    .withColumn(
        "label_distribution",
        F.array_join(
            F.transform("label_dist", lambda x: F.concat_ws(":", x["normalized_language_label"], x["n"].cast("string"))),
            ", ",
        ),
    )
    .withColumn(
        "outlier_votes",
        F.array_join(
            F.transform(
                F.filter("votes", lambda x: x["outside_base_majority"]),
                lambda x: F.concat_ws("=", F.concat_ws(":", x["provider"], x["model"]), x["normalized_language_label"]),
            ),
            "; ",
        ),
    )
    .withColumn(
        "evidence_snippets",
        F.array_join(
            F.transform(
                F.slice("segments", 1, 6),
                lambda x: F.concat_ws(": ", x["segment_type"], F.substring(x["text"], 1, 180)),
            ),
            " | ",
        ),
    )
    .select(
        "channel_id",
        "n_valid_votes",
        "majority_normalized_base_iso",
        "majority_base_votes",
        "base_dissenting_models",
        "n_distinct_normalized_base_iso",
        "majority_normalized_language_label",
        "label_dissenting_models",
        "n_distinct_normalized_language_label",
        "consensus_status",
        "openlid_primary_language_label",
        "glotlid_primary_language_label",
        "probable_issue_flags",
        "base_distribution",
        "label_distribution",
        "outlier_votes",
        "evidence_snippets",
    )
    .orderBy(
        F.desc("base_dissenting_models"),
        F.desc("label_dissenting_models"),
        F.desc("n_valid_votes"),
        "channel_id",
    )
    .limit(MAX_CASES)
)

overall = {
    "run_id": RUN_ID,
    "n_audit_disagreement_cases": audit.count(),
    "n_significant_base_disagreement_cases": audit.where(F.col("base_dissenting_models") > 1).count(),
    "flag_counts": flag_counts,
    "significant_flag_counts": significant_flag_counts,
    "majority_counts": majority_counts,
    "base_split_patterns": base_split_patterns,
    "significant_base_split_patterns": significant_base_split_patterns,
    "model_outliers": model_outliers,
    "significant_model_outliers": significant_model_outliers,
    "compact_cases": [row.asDict(recursive=True) for row in compact_cases.collect()],
}

print(json.dumps(overall, ensure_ascii=False, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(overall, ensure_ascii=False, sort_keys=True, default=str))
