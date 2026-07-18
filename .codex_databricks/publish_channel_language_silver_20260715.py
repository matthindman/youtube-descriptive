# Databricks notebook source
# ruff: noqa: F821
"""Publish the approved channel-language silver history and current snapshot.

The default ``channel_language`` is a base ISO 639-3 code, independent of
script. DeepSeek adjudications take precedence for routed channels. Exact LID
consensus and same-base-ISO LID agreement are used for non-routed channels;
everything else is explicitly ``und``.
"""

import json

from pyspark.sql import functions as F


def _widget(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except Exception:
        return default


SOURCE_RUN_ID = _widget("source_run_id", "channel_crawl_full_20260623")
INFERENCE_HASH_BUCKETS = int(_widget("inference_hash_buckets", "4096"))
LABEL_VERSION = _widget(
    "label_version",
    "lid_v3_channel_crawl_full_20260623_deepseek_flash_20260715_v1",
)
PROMPT_VERSION = _widget(
    "prompt_version",
    "llm_fallback_final_guardrails_post_review_20260630",
)

LID_TABLE = _widget(
    "lid_table",
    "dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channels",
)
FALLBACK_TABLE = _widget(
    "fallback_table",
    "dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_deepseek_flash_full_fallback_20260630_llm_verdicts",
)
HIGH_RISK_TABLE = _widget(
    "high_risk_table",
    "dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_high_risk_omitted_all95917_deepseek_20260715_llm_verdicts",
)
CANONICAL_CHANNEL_TABLE = _widget(
    "canonical_channel_table",
    "prod_tads.youtube_too.yt_sl_channels",
)
HISTORY_TABLE = _widget(
    "history_table",
    "prod_tads.youtube_too.yt_sl_channel_language_labels",
)
CURRENT_TABLE = _widget(
    "current_table",
    "prod_tads.youtube_too.yt_sl_channel_language_current",
)

EXPECTED_LID_CHANNELS = int(_widget("expected_lid_channels", "4797226"))
EXPECTED_FALLBACK_CHANNELS = int(_widget("expected_fallback_channels", "645865"))
EXPECTED_HIGH_RISK_CHANNELS = int(_widget("expected_high_risk_channels", "95917"))
EXPECTED_HIGH_RISK_EXACT_RECOVERY = int(
    _widget("expected_high_risk_exact_recovery", "4668")
)
EXPECTED_CANONICAL_ONLY_CHANNELS = int(
    _widget("expected_canonical_only_channels", "1491")
)


def _table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


for required in (LID_TABLE, FALLBACK_TABLE, HIGH_RISK_TABLE, CANONICAL_CHANNEL_TABLE):
    if not _table_exists(required):
        raise ValueError(f"Required source table does not exist: {required}")

lid = (
    spark.table(LID_TABLE)
    .where(
        (F.col("run_id") == F.lit(SOURCE_RUN_ID))
        & (F.col("inference_hash_buckets") == F.lit(INFERENCE_HASH_BUCKETS))
    )
)
canonical_channels = (
    spark.table(CANONICAL_CHANNEL_TABLE)
    .select("channel_id")
    .where(F.col("channel_id").isNotNull())
    .distinct()
)
canonical_only_channels = canonical_channels.join(
    lid.select("channel_id"), "channel_id", "left_anti"
)


def _llm_projection(df, source_name: str, source_table: str, has_group_fields: bool):
    source_run = (
        F.coalesce(F.col("classification_source_run_id"), F.col("run_id"))
        if has_group_fields
        else F.col("run_id")
    )
    group_id = (
        F.col("classification_group_id")
        if has_group_fields
        else F.lit(None).cast("string")
    )
    return df.select(
        "channel_id",
        F.lit(True).alias("has_llm_adjudication"),
        F.lit(source_name).alias("llm_source_name"),
        F.lit(source_table).alias("llm_source_table"),
        source_run.alias("llm_source_run_id"),
        group_id.alias("llm_classification_group_id"),
        F.col("route_reason").alias("llm_route_reason"),
        F.col("panel_status").alias("llm_panel_status"),
        F.col("panel_language_iso639_3").alias("llm_language_iso639_3"),
        F.col("panel_language_label").alias("llm_language_label"),
        F.col("panel_language_script").alias("llm_language_script"),
        F.col("panel_is_mixed_language").alias("llm_is_mixed_language"),
        F.col("panel_is_romanized").alias("llm_is_romanized"),
        F.col("panel_confidence").alias("llm_confidence"),
        F.col("prediction_timestamp").alias("llm_prediction_timestamp"),
    )


fallback = _llm_projection(
    spark.table(FALLBACK_TABLE),
    "deepseek_flash_full_fallback",
    FALLBACK_TABLE,
    False,
)
high_risk = _llm_projection(
    spark.table(HIGH_RISK_TABLE),
    "deepseek_flash_high_risk_omitted",
    HIGH_RISK_TABLE,
    True,
)
llm = fallback.unionByName(high_risk)

lid_count = lid.count()
lid_distinct = lid.select("channel_id").distinct().count()
fallback_count = fallback.count()
fallback_distinct = fallback.select("channel_id").distinct().count()
high_risk_count = high_risk.count()
high_risk_distinct = high_risk.select("channel_id").distinct().count()
llm_count = llm.count()
llm_distinct = llm.select("channel_id").distinct().count()

expected_counts = {
    "lid_count": (lid_count, EXPECTED_LID_CHANNELS),
    "lid_distinct": (lid_distinct, EXPECTED_LID_CHANNELS),
    "fallback_count": (fallback_count, EXPECTED_FALLBACK_CHANNELS),
    "fallback_distinct": (fallback_distinct, EXPECTED_FALLBACK_CHANNELS),
    "high_risk_count": (high_risk_count, EXPECTED_HIGH_RISK_CHANNELS),
    "high_risk_distinct": (high_risk_distinct, EXPECTED_HIGH_RISK_CHANNELS),
    "llm_count": (
        llm_count,
        EXPECTED_FALLBACK_CHANNELS + EXPECTED_HIGH_RISK_CHANNELS,
    ),
    "llm_distinct": (
        llm_distinct,
        EXPECTED_FALLBACK_CHANNELS + EXPECTED_HIGH_RISK_CHANNELS,
    ),
}
for metric, (actual, expected) in expected_counts.items():
    if actual != expected:
        raise AssertionError(f"{metric}={actual:,}; expected {expected:,}")

joined = lid.join(llm, "channel_id", "left")
has_llm = F.coalesce(F.col("has_llm_adjudication"), F.lit(False))
llm_classified = (
    has_llm
    & (F.col("llm_panel_status") == F.lit("panel_majority"))
    & F.col("llm_language_iso639_3").isNotNull()
)
lid_consensus = (~has_llm) & F.col("consensus_language_iso639_3").isNotNull()
same_lid_iso = (
    F.col("openlid_primary_language_iso639_3").isNotNull()
    & (
        F.lower(F.col("openlid_primary_language_iso639_3"))
        == F.lower(F.col("glotlid_primary_language_iso639_3"))
    )
)
lid_exact_agreement = (
    (~has_llm)
    & (~lid_consensus)
    & F.coalesce(F.col("models_agree_exact_primary"), F.lit(False))
    & same_lid_iso
)
lid_iso_agreement = (
    (~has_llm)
    & (~lid_consensus)
    & (~lid_exact_agreement)
    & F.coalesce(F.col("models_agree_iso_primary"), F.lit(False))
    & same_lid_iso
)

channel_language = (
    F.when(llm_classified, F.lower(F.trim(F.col("llm_language_iso639_3"))))
    .when(has_llm, F.lit("und"))
    .when(lid_consensus, F.lower(F.trim(F.col("consensus_language_iso639_3"))))
    .when(lid_exact_agreement | lid_iso_agreement,
          F.lower(F.trim(F.col("openlid_primary_language_iso639_3"))))
    .otherwise(F.lit("und"))
)

same_lid_script = (
    F.col("openlid_primary_language_script").isNotNull()
    & (
        F.col("openlid_primary_language_script")
        == F.col("glotlid_primary_language_script")
    )
)
source_language_script = (
    F.when(llm_classified, F.col("llm_language_script"))
    .when(has_llm, F.lit(None).cast("string"))
    .when(lid_consensus, F.col("consensus_language_script"))
    .when(lid_exact_agreement & same_lid_script,
          F.col("openlid_primary_language_script"))
    .when(lid_iso_agreement & same_lid_script,
          F.col("openlid_primary_language_script"))
    .otherwise(F.lit(None).cast("string"))
)
channel_script = (
    F.when(source_language_script == F.lit("Japn"), F.lit("Jpan"))
    .when(source_language_script == F.lit("Myan"), F.lit("Mymr"))
    .when(source_language_script == F.lit("Trad"), F.lit("Hant"))
    .when(source_language_script.isin("Sant", "Syrl"), F.lit(None).cast("string"))
    .otherwise(source_language_script)
)

label_source = (
    F.when(has_llm, F.col("llm_source_name"))
    .when(lid_consensus, F.lit("lid_consensus"))
    .when(lid_exact_agreement, F.lit("lid_exact_model_agreement"))
    .when(lid_iso_agreement, F.lit("lid_base_iso_agreement"))
    .otherwise(F.lit("unresolved"))
)

source_run_id = (
    F.when(has_llm, F.col("llm_source_run_id"))
    .otherwise(F.col("run_id"))
)
source_table = (
    F.when(has_llm, F.col("llm_source_table"))
    .otherwise(F.lit(LID_TABLE))
)
source_prediction_timestamp = (
    F.when(has_llm, F.col("llm_prediction_timestamp"))
    .otherwise(F.col("prediction_timestamp"))
)
is_script_ambiguous = (
    (lid_iso_agreement | lid_exact_agreement)
    & F.col("openlid_primary_language_script").isNotNull()
    & F.col("glotlid_primary_language_script").isNotNull()
    & (~same_lid_script)
)
is_mixed_language = (
    F.when(llm_classified,
           F.coalesce(F.col("llm_is_mixed_language"), F.lit(False)))
    .when(has_llm, F.lit(None).cast("boolean"))
    .when(lid_consensus | lid_exact_agreement | lid_iso_agreement,
          F.coalesce(F.col("consensus_is_credible_mixed_language_candidate"),
                     F.lit(False)))
    .otherwise(F.lit(None).cast("boolean"))
)

published_at = F.current_timestamp()
silver = joined.select(
    F.col("channel_id"),
    F.col("channel_hash_bucket"),
    channel_language.alias("channel_language"),
    source_language_script.alias("source_language_script"),
    channel_script.alias("channel_language_script"),
    F.when(
        (channel_language != F.lit("und")) & channel_script.isNotNull(),
        F.concat_ws("_", channel_language, channel_script),
    ).alias("channel_language_script_label"),
    (channel_language != F.lit("und")).alias("is_language_classified"),
    is_mixed_language.alias("is_mixed_language"),
    F.when(llm_classified, F.col("llm_is_romanized"))
    .otherwise(F.lit(None).cast("boolean"))
    .alias("is_romanized"),
    F.coalesce(is_script_ambiguous, F.lit(False)).alias("is_script_ambiguous"),
    label_source.alias("language_label_source"),
    F.when(llm_classified, F.col("llm_confidence"))
    .otherwise(F.lit(None).cast("string"))
    .alias("language_confidence_level"),
    F.col("consensus_status").alias("lid_consensus_status"),
    F.col("consensus_language_label").alias("lid_consensus_language_label"),
    F.col("openlid_primary_language_label"),
    F.col("glotlid_primary_language_label"),
    F.when(has_llm, F.col("llm_panel_status"))
    .otherwise(F.lit(None).cast("string"))
    .alias("llm_status"),
    F.when(has_llm, F.col("llm_route_reason"))
    .otherwise(F.lit(None).cast("string"))
    .alias("llm_route_reason"),
    F.when(has_llm, F.lit("deepseek-v4-flash"))
    .otherwise(F.lit(None).cast("string"))
    .alias("llm_model"),
    F.when(has_llm, F.lit(False))
    .otherwise(F.lit(None).cast("boolean"))
    .alias("llm_thinking_enabled"),
    F.when(has_llm, F.lit(PROMPT_VERSION))
    .otherwise(F.lit(None).cast("string"))
    .alias("llm_prompt_version"),
    source_table.alias("classification_source_table"),
    source_run_id.alias("classification_source_run_id"),
    F.when(has_llm, F.col("llm_classification_group_id"))
    .otherwise(F.lit(None).cast("string"))
    .alias("classification_group_id"),
    source_prediction_timestamp.alias("source_prediction_timestamp"),
    F.lit(LABEL_VERSION).alias("label_version"),
    published_at.alias("published_at"),
)

canonical_only = canonical_only_channels.select(
    F.col("channel_id"),
    F.pmod(F.xxhash64(F.col("channel_id")), F.lit(INFERENCE_HASH_BUCKETS))
    .cast("int")
    .alias("channel_hash_bucket"),
    F.lit("und").alias("channel_language"),
    F.lit(None).cast("string").alias("source_language_script"),
    F.lit(None).cast("string").alias("channel_language_script"),
    F.lit(None).cast("string").alias("channel_language_script_label"),
    F.lit(False).alias("is_language_classified"),
    F.lit(None).cast("boolean").alias("is_mixed_language"),
    F.lit(None).cast("boolean").alias("is_romanized"),
    F.lit(False).alias("is_script_ambiguous"),
    F.lit("not_in_lid_source_run").alias("language_label_source"),
    F.lit(None).cast("string").alias("language_confidence_level"),
    F.lit(None).cast("string").alias("lid_consensus_status"),
    F.lit(None).cast("string").alias("lid_consensus_language_label"),
    F.lit(None).cast("string").alias("openlid_primary_language_label"),
    F.lit(None).cast("string").alias("glotlid_primary_language_label"),
    F.lit(None).cast("string").alias("llm_status"),
    F.lit(None).cast("string").alias("llm_route_reason"),
    F.lit(None).cast("string").alias("llm_model"),
    F.lit(None).cast("boolean").alias("llm_thinking_enabled"),
    F.lit(None).cast("string").alias("llm_prompt_version"),
    F.lit(CANONICAL_CHANNEL_TABLE).alias("classification_source_table"),
    F.lit(None).cast("string").alias("classification_source_run_id"),
    F.lit(None).cast("string").alias("classification_group_id"),
    F.lit(None).cast("timestamp").alias("source_prediction_timestamp"),
    F.lit(LABEL_VERSION).alias("label_version"),
    published_at.alias("published_at"),
)
canonical_only_count = canonical_only.count()
if canonical_only_count != EXPECTED_CANONICAL_ONLY_CHANNELS:
    raise AssertionError(
        f"Canonical-only channels={canonical_only_count:,}; "
        f"expected={EXPECTED_CANONICAL_ONLY_CHANNELS:,}"
    )
silver = silver.unionByName(canonical_only)

silver = silver.cache()
silver_count = silver.count()
silver_distinct = silver.select("channel_id").distinct().count()
invalid_language_codes = silver.where(
    ~F.col("channel_language").rlike("^[a-z]{3}$")
).count()
known_nonstandard_scripts = silver.where(
    F.col("channel_language_script").isin("Japn", "Myan", "Syrl", "Trad", "Sant")
).count()
missing_sources = silver.where(F.col("language_label_source").isNull()).count()
high_risk_exact_recovery = silver.where(
    (F.col("lid_consensus_status") == F.lit("high_risk_tail_label_needs_review"))
    & (F.col("language_label_source") == F.lit("lid_exact_model_agreement"))
    & F.col("is_language_classified")
).count()

expected_published_channels = EXPECTED_LID_CHANNELS + EXPECTED_CANONICAL_ONLY_CHANNELS
if silver_count != expected_published_channels or silver_distinct != expected_published_channels:
    raise AssertionError(
        f"Silver cardinality failed: rows={silver_count:,}, distinct={silver_distinct:,}, "
        f"expected={expected_published_channels:,}"
    )
if invalid_language_codes:
    raise AssertionError(f"Found {invalid_language_codes:,} invalid channel_language values")
if known_nonstandard_scripts:
    raise AssertionError(
        f"Found {known_nonstandard_scripts:,} known nonstandard channel_language_script values"
    )
if missing_sources:
    raise AssertionError(f"Found {missing_sources:,} rows without language_label_source")
if high_risk_exact_recovery != EXPECTED_HIGH_RISK_EXACT_RECOVERY:
    raise AssertionError(
        f"High-risk exact-agreement recovery={high_risk_exact_recovery:,}; "
        f"expected={EXPECTED_HIGH_RISK_EXACT_RECOVERY:,}"
    )

if _table_exists(HISTORY_TABLE):
    spark.sql(
        f"DELETE FROM {HISTORY_TABLE} WHERE label_version = "
        f"'{LABEL_VERSION.replace(chr(39), chr(39) * 2)}'"
    )
    (
        silver.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(HISTORY_TABLE)
    )
else:
    (
        silver.write.format("delta")
        .mode("errorifexists")
        .partitionBy("label_version")
        .saveAsTable(HISTORY_TABLE)
    )

(
    silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CURRENT_TABLE)
)

table_comments = {
    HISTORY_TABLE: (
        "Versioned silver channel-language labels. channel_language is the default "
        "base ISO 639-3 analysis variable; script is stored separately."
    ),
    CURRENT_TABLE: (
        "Current one-row-per-channel silver language snapshot. Join by channel_id; "
        "use channel_language as the default analysis label."
    ),
}
for table_name, comment in table_comments.items():
    safe_comment = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{safe_comment}'")
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
        "'quality' = 'silver', "
        "'delta.enableChangeDataFeed' = 'true', "
        f"'language.label_version' = '{LABEL_VERSION}', "
        f"'language.source_run_id' = '{SOURCE_RUN_ID}'"
        ")"
    )

column_comments = {
    "channel_language": (
        "Default primary channel language as a lowercase ISO 639-3 code; und means unresolved."
    ),
    "channel_language_script": (
        "Primary script as ISO 15924 when supported; null when unresolved or ambiguous."
    ),
    "source_language_script": (
        "Unmodified script selected from the authoritative classification source before silver normalization."
    ),
    "is_mixed_language": (
        "Whether credible recurring evidence supports multiple languages; null when unresolved."
    ),
    "language_label_source": "Decision tier that supplied channel_language.",
    "label_version": "Immutable publication/version identifier for this classification snapshot.",
}
for table_name in (HISTORY_TABLE, CURRENT_TABLE):
    for column_name, comment in column_comments.items():
        safe_comment = comment.replace("'", "''")
        spark.sql(
            f"ALTER TABLE {table_name} ALTER COLUMN {column_name} "
            f"COMMENT '{safe_comment}'"
        )

history_version = spark.table(HISTORY_TABLE).where(
    F.col("label_version") == F.lit(LABEL_VERSION)
)
current = spark.table(CURRENT_TABLE)
post_counts = {
    "history_version_rows": history_version.count(),
    "history_version_distinct_channels": history_version.select("channel_id").distinct().count(),
    "current_rows": current.count(),
    "current_distinct_channels": current.select("channel_id").distinct().count(),
    "current_classified": current.where(F.col("is_language_classified")).count(),
    "current_und": current.where(F.col("channel_language") == F.lit("und")).count(),
    "current_mixed": current.where(F.col("is_mixed_language") == F.lit(True)).count(),
    "current_script_ambiguous": current.where(F.col("is_script_ambiguous")).count(),
}
for metric in (
    "history_version_rows",
    "history_version_distinct_channels",
    "current_rows",
    "current_distinct_channels",
):
    if post_counts[metric] != expected_published_channels:
        raise AssertionError(
            f"Post-write {metric}={post_counts[metric]:,}; "
            f"expected {expected_published_channels:,}"
        )

source_distribution = [
    row.asDict(recursive=True)
    for row in current.groupBy("language_label_source")
    .count()
    .orderBy(F.desc("count"))
    .collect()
]
top_languages = [
    row.asDict(recursive=True)
    for row in current.groupBy("channel_language")
    .count()
    .orderBy(F.desc("count"), F.asc("channel_language"))
    .limit(30)
    .collect()
]
script_distribution = [
    row.asDict(recursive=True)
    for row in current.groupBy(
        F.coalesce(F.col("channel_language_script"), F.lit("<null>")).alias("script")
    )
    .count()
    .orderBy(F.desc("count"))
    .collect()
]

summary = {
    "history_table": HISTORY_TABLE,
    "current_table": CURRENT_TABLE,
    "label_version": LABEL_VERSION,
    "source_run_id": SOURCE_RUN_ID,
    "prewrite_counts": {key: actual for key, (actual, _) in expected_counts.items()},
    "canonical_only_channels": canonical_only_count,
    "postwrite_counts": post_counts,
    "high_risk_exact_recovery": high_risk_exact_recovery,
    "source_distribution": source_distribution,
    "top_languages": top_languages,
    "script_distribution": script_distribution,
}
print("CHANNEL_LANGUAGE_SILVER_SUMMARY=" + json.dumps(summary, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(summary, sort_keys=True, default=str))
