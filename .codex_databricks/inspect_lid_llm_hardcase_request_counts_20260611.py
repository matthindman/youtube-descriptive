# Databricks notebook source
import json
import os

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
_create_text_widget("source_run_id", "too_full_20260609")
_create_text_widget("inference_hash_buckets", "4096")
_create_text_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
_create_text_widget("comparison_table", "yt_lid_v3_too_full_20260609_channel_model_comparison")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
SOURCE_RUN_ID = _get_widget("source_run_id", "too_full_20260609")
INFERENCE_HASH_BUCKETS = int(_get_widget("inference_hash_buckets", "4096"))
REQUESTS_TABLE = _get_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
COMPARISON_TABLE = _get_widget("comparison_table", "yt_lid_v3_too_full_20260609_channel_model_comparison")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


ARABIC_FAMILY_ISO = {"ara", "arb", "ary", "arz", "arq", "apc", "ars", "ajp", "aeb", "acm", "acq", "aec", "afb", "ayl", "ayn"}
CANONICAL_BASE_ISO = {
    "zho": "cmn",
    "cmn": "cmn",
    "fil": "fil",
    "tgl": "fil",
    "ori": "ory",
    "ory": "ory",
    "uzn": "uzb",
    "uzb": "uzb",
    "msa": "zsm",
    "zsm": "zsm",
    "nep": "npi",
    "npi": "npi",
    "ku": "kmr",
    "kur": "kmr",
    "kmr": "kmr",
}


def canonical_base_iso_expr(col):
    iso = F.lower(F.trim(col.cast("string")))
    iso = F.when(iso.isin("", "null", "none"), F.lit(None).cast("string")).otherwise(iso)
    iso = F.when(iso.isin(*sorted(ARABIC_FAMILY_ISO)), F.lit("ara")).otherwise(iso)
    mapping = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in CANONICAL_BASE_ISO.items()], []))
    return F.coalesce(F.element_at(mapping, iso), iso)


requests = spark.table(fqtn(REQUESTS_TABLE)).where(F.col("run_id") == F.lit(RUN_ID)).persist()
channels = requests.select("channel_id").distinct().persist()

model_counts = [
    row.asDict(recursive=True)
    for row in (
        requests.groupBy("provider", "model", "model_tier")
        .agg(F.count(F.lit(1)).alias("n_requests"), F.countDistinct("channel_id").alias("n_channels"))
        .orderBy("provider", "model")
        .collect()
    )
]
route_counts = [
    row.asDict(recursive=True)
    for row in (
        requests.groupBy("route_reason")
        .agg(F.countDistinct("channel_id").alias("n_channels"), F.count(F.lit(1)).alias("n_requests"))
        .orderBy("route_reason")
        .collect()
    )
]

cmp = (
    spark.table(fqtn(COMPARISON_TABLE))
    .where(
        (F.col("run_id") == F.lit(SOURCE_RUN_ID))
        & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
    )
    .join(channels, on="channel_id", how="inner")
    .withColumn("openlid_norm_iso", canonical_base_iso_expr(F.col("openlid_primary_language_iso639_3")))
    .withColumn("glotlid_norm_iso", canonical_base_iso_expr(F.col("glotlid_primary_language_iso639_3")))
    .withColumn(
        "is_lid_iso_disagreement",
        F.col("openlid_norm_iso").isNotNull()
        & F.col("glotlid_norm_iso").isNotNull()
        & (F.col("openlid_norm_iso") != F.col("glotlid_norm_iso")),
    )
)

lid_sanity = [
    row.asDict(recursive=True)
    for row in (
        cmp.groupBy("is_lid_iso_disagreement")
        .agg(F.countDistinct("channel_id").alias("n_channels"))
        .orderBy("is_lid_iso_disagreement")
        .collect()
    )
]
source_status_counts = [
    row.asDict(recursive=True)
    for row in (
        cmp.groupBy("consensus_status")
        .agg(F.countDistinct("channel_id").alias("n_channels"))
        .orderBy(F.desc("n_channels"), "consensus_status")
        .collect()
    )
]

batch_file_table = REQUESTS_TABLE + "_batch_files"
batch_files = []
try:
    batch_files = [
        row.asDict(recursive=True)
        for row in (
            spark.table(fqtn(batch_file_table))
            .where(F.col("run_id") == F.lit(RUN_ID))
            .groupBy("provider", "model")
            .agg(F.count(F.lit(1)).alias("n_files"), F.sum("n_requests").alias("n_requests"))
            .orderBy("provider", "model")
            .collect()
        )
    ]
except Exception as exc:
    batch_files = [{"error": repr(exc)[:500]}]

summary = {
    "run_id": RUN_ID,
    "source_run_id": SOURCE_RUN_ID,
    "n_channels": channels.count(),
    "n_requests": requests.count(),
    "model_counts": model_counts,
    "route_counts": route_counts,
    "lid_disagreement_sanity": lid_sanity,
    "source_consensus_status_counts": source_status_counts,
    "batch_files": batch_files,
}

print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False, sort_keys=True))
