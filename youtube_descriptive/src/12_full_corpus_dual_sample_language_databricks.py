# Databricks notebook source
# ruff: noqa: F821
# MAGIC %run ./full_corpus_dual_sample_design

# COMMAND ----------
"""Gate, route, and publish language labels for the dual-sample analysis union."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass


def _get(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name).strip() or default
    except Exception:
        return default


_widget("stage", "preflight")
_widget("sample_phase", "all")
_widget(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
STAGE = _get("stage", "preflight")
SAMPLE_PHASE = _get("sample_phase", "all").lower()
if SAMPLE_PHASE not in {"all", "pps", "remainder"}:
    raise ValueError("sample_phase must be one of: all, pps, remainder")
CONFIG_PATH = _get(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
CONFIG = json.loads(dbutils.fs.head(CONFIG_PATH, 1024 * 1024))
validate_design_config(CONFIG)

DESIGN_VERSION = CONFIG["design_version"]
LANGUAGE = CONFIG["language"]
PREFIX = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_prefix']}"
PHASE_SUFFIX = "" if SAMPLE_PHASE == "all" else f"_{SAMPLE_PHASE}"
LID_PREFIX = f"{CONFIG['output_prefix']}_lid{PHASE_SUFFIX}"
LLM_PREFIX = f"{CONFIG['output_prefix']}_deepseek_flash{PHASE_SUFFIX}"
CATALOG = CONFIG["output_catalog"]
SCHEMA = CONFIG["output_schema"]
HASH_BUCKETS = int(LANGUAGE["inference_hash_buckets"])
LID_RUN_ID = f"{LANGUAGE['lid_run_id']}{PHASE_SUFFIX}"
LLM_RUN_ID = f"{LANGUAGE['llm_run_id']}{PHASE_SUFFIX}"

TABLES = {
    "analysis_union": f"{PREFIX}_analysis_union",
    "collection_queue": f"{PREFIX}_collection_queue",
    "lid_source_channels_all": f"{PREFIX}_lid_source_channels",
    "lid_source_videos_all": f"{PREFIX}_lid_source_videos",
    "lid_source_channels": f"{PREFIX}_lid_source_channels{PHASE_SUFFIX}",
    "lid_source_videos": f"{PREFIX}_lid_source_videos{PHASE_SUFFIX}",
    "preflight": f"{PREFIX}_language_preflight{PHASE_SUFFIX}",
    "lid_channels": f"{CATALOG}.{SCHEMA}.{LID_PREFIX}_channels",
    "lid_comparison": f"{CATALOG}.{SCHEMA}.{LID_PREFIX}_channel_model_comparison",
    "lid_segments": f"{CATALOG}.{SCHEMA}.{LID_PREFIX}_segments_input",
    "lid_text_features": f"{CATALOG}.{SCHEMA}.{LID_PREFIX}_channel_text_features",
    "lid_hindi_audit": f"{CATALOG}.{SCHEMA}.{LID_PREFIX}_hindi_indic_audit_candidates",
    "routing": f"{PREFIX}_language_routing_comparison{PHASE_SUFFIX}",
    "llm_verdicts": f"{CATALOG}.{SCHEMA}.{LLM_PREFIX}_llm_verdicts",
    "current": (
        f"{PREFIX}_channel_language_current"
        if SAMPLE_PHASE == "all"
        else f"{PREFIX}_channel_language_{SAMPLE_PHASE}_current"
    ),
    "summary": (
        f"{PREFIX}_channel_language_summary"
        if SAMPLE_PHASE == "all"
        else f"{PREFIX}_channel_language_{SAMPLE_PHASE}_summary"
    ),
    "pps_current": f"{PREFIX}_channel_language_pps_current",
    "remainder_current": f"{PREFIX}_channel_language_remainder_current",
}


def require_table(table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required table does not exist: {table_name}")


def write_table(frame: DataFrame, table_name: str, comment: str) -> None:
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    escaped = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{escaped}'")
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
        f"'design.version'='{DESIGN_VERSION}', "
        f"'language.lid_run_id'='{LID_RUN_ID}', "
        f"'language.llm_run_id'='{LLM_RUN_ID}')"
    )


def run_scope(frame: DataFrame, run_id: str) -> DataFrame:
    return frame.where(
        (F.col("run_id") == F.lit(run_id))
        & (F.col("inference_hash_buckets") == F.lit(HASH_BUCKETS))
    )


def phase_filter(frame: DataFrame) -> DataFrame:
    if SAMPLE_PHASE == "pps":
        return frame.where(F.col("selection_route").isin("pps_only", "srs_and_pps"))
    if SAMPLE_PHASE == "remainder":
        return frame.where(~F.col("selection_route").isin("pps_only", "srs_and_pps"))
    return frame


def preflight() -> dict[str, int]:
    for name in ("analysis_union", "collection_queue", "lid_source_channels_all", "lid_source_videos_all"):
        require_table(TABLES[name])
    analysis = phase_filter(spark.table(TABLES["analysis_union"]))
    queue = phase_filter(spark.table(TABLES["collection_queue"]))
    all_channels = spark.table(TABLES["lid_source_channels_all"])
    channels = phase_filter(all_channels)
    videos = spark.table(TABLES["lid_source_videos_all"]).join(
        channels.select("channel_id"), "channel_id", "inner"
    )
    if SAMPLE_PHASE != "all":
        write_table(
            channels,
            TABLES["lid_source_channels"],
            f"Dual-LID channel source restricted to the nonoverlapping {SAMPLE_PHASE} phase.",
        )
        write_table(
            videos,
            TABLES["lid_source_videos"],
            f"Dual-LID video source restricted to the nonoverlapping {SAMPLE_PHASE} phase.",
        )
    counts = {
        "analysis_union_rows": analysis.count(),
        "analysis_union_distinct": analysis.select("channel_id").distinct().count(),
        "existing_labels": analysis.where(F.col("has_existing_language_label")).count(),
        "missing_labels": analysis.where(~F.col("has_existing_language_label")).count(),
        "collection_queue_rows": queue.count(),
        "terminal_no_text_rows": analysis.where(
            F.col("language_enrichment_disposition") == "terminal_no_text_assign_und"
        ).count(),
        "lid_source_channel_rows": channels.count(),
        "lid_source_channel_distinct": channels.select("channel_id").distinct().count(),
        "lid_source_video_rows": videos.count(),
        "lid_source_video_channels": videos.select("channel_id").distinct().count(),
    }
    if counts["analysis_union_rows"] != counts["analysis_union_distinct"]:
        raise AssertionError(f"Analysis union is not unique: {counts}")
    if counts["collection_queue_rows"]:
        raise RuntimeError(
            f"LANGUAGE PREFLIGHT BLOCKED: {counts['collection_queue_rows']:,} selected channels still "
            f"require text collection in {TABLES['collection_queue']}. Collect descriptions/recent "
            "videos and rerun stage_enrichment before language inference."
        )
    if counts["missing_labels"] != (
        counts["lid_source_channel_distinct"] + counts["terminal_no_text_rows"]
    ):
        raise AssertionError(f"Missing-label and LID-source counts disagree: {counts}")
    rows = [
        (DESIGN_VERSION, key, int(value), datetime.now(timezone.utc)) for key, value in counts.items()
    ]
    write_table(
        spark.createDataFrame(
            rows,
            "design_version string, metric string, value long, recorded_at timestamp",
        ),
        TABLES["preflight"],
        "Language preflight counts; existence proves text collection and source conservation passed.",
    )
    print("LANGUAGE PREFLIGHT: PASS")
    print("SAMPLE PHASE:", SAMPLE_PHASE)
    print(json.dumps(counts, sort_keys=True))
    return counts


def prepare_routing() -> dict[str, int]:
    require_table(TABLES["lid_channels"])
    require_table(TABLES["lid_comparison"])
    lid = run_scope(spark.table(TABLES["lid_channels"]), LID_RUN_ID)
    comparison = run_scope(spark.table(TABLES["lid_comparison"]), LID_RUN_ID)
    same_base_iso = (
        F.col("openlid_primary_language_iso639_3").isNotNull()
        & F.col("glotlid_primary_language_iso639_3").isNotNull()
        & (
            F.lower(F.col("openlid_primary_language_iso639_3"))
            == F.lower(F.col("glotlid_primary_language_iso639_3"))
        )
    )
    base_resolved = F.col("consensus_language_iso639_3").isNotNull() | same_base_iso
    routed = (
        comparison.withColumn("source_consensus_status", F.col("consensus_status"))
        .withColumn("source_consensus_language_label", F.col("consensus_language_label"))
        .withColumn("lid_base_language_resolved", base_resolved)
        .withColumn(
            "consensus_status",
            F.when(base_resolved, F.lit("exact_model_agreement"))
            .otherwise(F.lit("model_disagreement_needs_review")),
        )
        .withColumn(
            "consensus_language_label",
            F.when(base_resolved, F.col("consensus_language_label")).otherwise(F.lit(None).cast("string")),
        )
    )
    write_table(
        routed,
        TABLES["routing"],
        "Dual-LID comparison with base-ISO agreements protected from unnecessary LLM routing.",
    )
    counts = {
        "lid_rows": lid.count(),
        "lid_distinct": lid.select("channel_id").distinct().count(),
        "routing_rows": routed.count(),
        "routing_distinct": routed.select("channel_id").distinct().count(),
        "base_language_resolved": routed.where(F.col("lid_base_language_resolved")).count(),
        "deepseek_routes": routed.where(~F.col("lid_base_language_resolved")).count(),
    }
    if counts["lid_rows"] != counts["lid_distinct"] or counts["routing_rows"] != counts["routing_distinct"]:
        raise AssertionError(f"LID or routing output is not one row per channel: {counts}")
    if counts["lid_rows"] != counts["routing_rows"]:
        raise AssertionError(f"LID and routing rows do not conserve: {counts}")
    print("LANGUAGE ROUTING: PASS")
    print(json.dumps(counts, sort_keys=True))
    return counts


def publish() -> dict[str, int]:
    for name in ("analysis_union", "lid_channels", "llm_verdicts"):
        require_table(TABLES[name])
    analysis = phase_filter(spark.table(TABLES["analysis_union"]))
    existing = analysis.where(F.col("has_existing_language_label")).select(
        "channel_id",
        F.lower(F.trim(F.col("channel_language"))).alias("channel_language"),
        "source_language_script",
        "channel_language_script",
        "channel_language_script_label",
        "is_language_classified",
        "is_mixed_language",
        "is_romanized",
        "is_script_ambiguous",
        "language_label_source",
        "language_confidence_level",
        F.col("label_version").alias("source_label_version"),
        F.lit("reused_frozen_head_label").alias("analysis_label_source"),
        F.lit(None).cast("string").alias("llm_status"),
        F.lit(None).cast("string").alias("llm_route_reason"),
    )

    lid = run_scope(spark.table(TABLES["lid_channels"]), LID_RUN_ID)
    verdicts = spark.table(TABLES["llm_verdicts"]).where(F.col("run_id") == F.lit(LLM_RUN_ID))
    lid_projection = lid.select(
        "channel_id",
        "consensus_language_iso639_3",
        "consensus_language_script",
        "consensus_is_credible_mixed_language_candidate",
        "openlid_primary_language_iso639_3",
        "openlid_primary_language_script",
        "glotlid_primary_language_iso639_3",
        "glotlid_primary_language_script",
    )
    llm_projection = verdicts.select(
        "channel_id",
        "route_reason",
        "panel_status",
        "panel_language_iso639_3",
        "panel_language_script",
        "panel_is_mixed_language",
        "panel_is_romanized",
        "panel_confidence",
    )
    new_base = analysis.where(~F.col("has_existing_language_label")).select("channel_id")
    joined = new_base.join(lid_projection, "channel_id", "left").join(llm_projection, "channel_id", "left")
    has_llm = F.col("panel_status").isNotNull()
    llm_classified = (
        (F.col("panel_status") == F.lit("panel_majority"))
        & F.col("panel_language_iso639_3").isNotNull()
    )
    same_lid_iso = (
        F.col("openlid_primary_language_iso639_3").isNotNull()
        & F.col("glotlid_primary_language_iso639_3").isNotNull()
        & (
            F.lower(F.col("openlid_primary_language_iso639_3"))
            == F.lower(F.col("glotlid_primary_language_iso639_3"))
        )
    )
    lid_consensus = (~has_llm) & F.col("consensus_language_iso639_3").isNotNull()
    lid_base_agreement = (~has_llm) & (~lid_consensus) & same_lid_iso
    channel_language = (
        F.when(llm_classified, F.lower(F.trim(F.col("panel_language_iso639_3"))))
        .when(has_llm, F.lit("und"))
        .when(lid_consensus, F.lower(F.trim(F.col("consensus_language_iso639_3"))))
        .when(lid_base_agreement, F.lower(F.trim(F.col("openlid_primary_language_iso639_3"))))
        .otherwise(F.lit("und"))
    )
    same_script = (
        F.col("openlid_primary_language_script").isNotNull()
        & F.col("glotlid_primary_language_script").isNotNull()
        & (F.col("openlid_primary_language_script") == F.col("glotlid_primary_language_script"))
    )
    source_script = (
        F.when(llm_classified, F.col("panel_language_script"))
        .when(has_llm, F.lit(None).cast("string"))
        .when(lid_consensus, F.col("consensus_language_script"))
        .when(lid_base_agreement & same_script, F.col("openlid_primary_language_script"))
        .otherwise(F.lit(None).cast("string"))
    )
    normalized_script = (
        F.when(source_script == "Japn", F.lit("Jpan"))
        .when(source_script == "Myan", F.lit("Mymr"))
        .when(source_script == "Trad", F.lit("Hant"))
        .when(source_script.isin("Sant", "Syrl"), F.lit(None).cast("string"))
        .otherwise(source_script)
    )
    label_source = (
        F.when(llm_classified, F.lit("deepseek_flash_fallback"))
        .when(has_llm, F.lit("deepseek_flash_insufficient_text"))
        .when(lid_consensus, F.lit("lid_consensus"))
        .when(lid_base_agreement, F.lit("lid_base_iso_agreement"))
        .otherwise(F.lit("unresolved"))
    )
    new = joined.select(
        "channel_id",
        channel_language.alias("channel_language"),
        source_script.alias("source_language_script"),
        normalized_script.alias("channel_language_script"),
        F.when(
            (channel_language != "und") & normalized_script.isNotNull(),
            F.concat_ws("_", channel_language, normalized_script),
        ).alias("channel_language_script_label"),
        (channel_language != "und").alias("is_language_classified"),
        F.when(llm_classified, F.coalesce(F.col("panel_is_mixed_language"), F.lit(False)))
        .when(has_llm, F.lit(None).cast("boolean"))
        .otherwise(F.coalesce(F.col("consensus_is_credible_mixed_language_candidate"), F.lit(False)))
        .alias("is_mixed_language"),
        F.when(llm_classified, F.col("panel_is_romanized")).otherwise(F.lit(None).cast("boolean")).alias("is_romanized"),
        (
            lid_base_agreement
            & F.col("openlid_primary_language_script").isNotNull()
            & F.col("glotlid_primary_language_script").isNotNull()
            & (~same_script)
        ).alias("is_script_ambiguous"),
        label_source.alias("language_label_source"),
        F.when(llm_classified, F.col("panel_confidence")).otherwise(F.lit(None).cast("string")).alias("language_confidence_level"),
        F.lit(None).cast("string").alias("source_label_version"),
        label_source.alias("analysis_label_source"),
        F.col("panel_status").alias("llm_status"),
        F.col("route_reason").alias("llm_route_reason"),
    )
    published_at = datetime.now(timezone.utc)
    current = (
        existing.unionByName(new)
        .withColumn("analysis_label_version", F.lit(f"{DESIGN_VERSION}_language{PHASE_SUFFIX}_v1"))
        .withColumn("lid_run_id", F.lit(LID_RUN_ID))
        .withColumn("llm_run_id", F.lit(LLM_RUN_ID))
        .withColumn("published_at", F.lit(published_at).cast("timestamp"))
    )
    write_table(current, TABLES["current"], "One final analysis language label per census or sampled channel.")
    counts = current.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("channel_id").alias("distinct_channels"),
        F.sum(F.col("is_language_classified").cast("long")).alias("classified"),
        F.sum((F.col("channel_language") == "und").cast("long")).alias("und"),
        F.sum((F.col("analysis_label_source") == "reused_frozen_head_label").cast("long")).alias("reused"),
        F.sum((F.col("analysis_label_source") == "deepseek_flash_fallback").cast("long")).alias("deepseek_classified"),
        F.sum((F.col("analysis_label_source") == "deepseek_flash_insufficient_text").cast("long")).alias("deepseek_insufficient"),
    ).first().asDict()
    analysis_count = analysis.count()
    if counts["rows"] != counts["distinct_channels"] or counts["rows"] != analysis_count:
        raise AssertionError(f"Published language rows do not conserve the analysis union: {counts}")
    rows = [
        (DESIGN_VERSION, key, int(value), datetime.now(timezone.utc)) for key, value in counts.items()
    ]
    write_table(
        spark.createDataFrame(rows, "design_version string, metric string, value long, recorded_at timestamp"),
        TABLES["summary"],
        "Final language publication QA and conservation metrics.",
    )
    print("LANGUAGE CONSERVATION: PASS")
    print(json.dumps(counts, sort_keys=True))
    return {key: int(value) for key, value in counts.items()}


def publish_combined() -> dict[str, int]:
    for name in ("analysis_union", "pps_current", "remainder_current"):
        require_table(TABLES[name])
    analysis = spark.table(TABLES["analysis_union"])
    pps = spark.table(TABLES["pps_current"])
    remainder = spark.table(TABLES["remainder_current"])
    combined = pps.unionByName(remainder)
    counts = {
        "analysis_rows": analysis.count(),
        "analysis_distinct": analysis.select("channel_id").distinct().count(),
        "pps_rows": pps.count(),
        "remainder_rows": remainder.count(),
        "combined_rows": combined.count(),
        "combined_distinct": combined.select("channel_id").distinct().count(),
        "missing_from_combined": analysis.select("channel_id").join(
            combined.select("channel_id"), "channel_id", "left_anti"
        ).count(),
        "unexpected_in_combined": combined.select("channel_id").join(
            analysis.select("channel_id"), "channel_id", "left_anti"
        ).count(),
    }
    label_counts = combined.agg(
        F.sum(F.col("is_language_classified").cast("long")).alias("classified"),
        F.sum((F.col("channel_language") == "und").cast("long")).alias("und"),
        F.sum((F.col("analysis_label_source") == "reused_frozen_head_label").cast("long")).alias("reused"),
        F.sum((F.col("analysis_label_source") == "deepseek_flash_fallback").cast("long")).alias("deepseek_classified"),
        F.sum((F.col("analysis_label_source") == "deepseek_flash_insufficient_text").cast("long")).alias("deepseek_insufficient"),
    ).first().asDict()
    counts.update({key: int(value or 0) for key, value in label_counts.items()})
    if not (
        counts["analysis_rows"]
        == counts["analysis_distinct"]
        == counts["combined_rows"]
        == counts["combined_distinct"]
    ) or counts["missing_from_combined"] or counts["unexpected_in_combined"]:
        raise AssertionError(f"Phase-union conservation failed: {counts}")
    write_table(
        combined,
        TABLES["current"],
        "Final analysis language labels formed from nonoverlapping PPS-first and remainder phases.",
    )
    summary_rows = [
        (DESIGN_VERSION, key, int(value), datetime.now(timezone.utc))
        for key, value in counts.items()
    ]
    write_table(
        spark.createDataFrame(
            summary_rows,
            "design_version string, metric string, value long, recorded_at timestamp",
        ),
        TABLES["summary"],
        "Conservation metrics for the nonoverlapping PPS-first and remainder language union.",
    )
    print("PHASED LANGUAGE CONSERVATION: PASS")
    print(json.dumps(counts, sort_keys=True))
    return counts


STAGES = {
    "preflight": preflight,
    "prepare_routing": prepare_routing,
    "publish": publish,
    "publish_combined": publish_combined,
}
if STAGE not in STAGES:
    raise ValueError(f"Unknown language stage {STAGE!r}; expected one of {sorted(STAGES)}")
print(f"RUNNING LANGUAGE STAGE: {STAGE}")
print(f"SAMPLE PHASE: {SAMPLE_PHASE}")
RESULT = STAGES[STAGE]()
dbutils.notebook.exit(json.dumps({"stage": STAGE, "result": RESULT}, sort_keys=True, default=str))
