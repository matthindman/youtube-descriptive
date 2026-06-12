# Databricks notebook source
# MAGIC %md
# MAGIC # Topic/Genre 1k Manual Audit Fix Overlay

# COMMAND ----------
import json
import os
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


def _create_text_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default, name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return default


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "category_topic_random_1000_20260611")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "category_topic_random_1000_20260611")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def out_table(suffix: str) -> str:
    return fqtn(f"{OUTPUT_PREFIX}_{suffix}")


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


def write_run_scoped(df, table_full: str):
    if "run_id" not in df.columns:
        df = df.withColumn("run_id", F.lit(RUN_ID))
    if not _table_exists_full(table_full):
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("run_id").saveAsTable(table_full)
        return
    existing = spark.table(table_full)
    for field in existing.schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    df = df.select(*existing.columns)
    spark.sql(f"DELETE FROM {table_full} WHERE run_id = {_sql_string(RUN_ID)}")
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_full)


audit_table = out_table("manual_audit_fixes")
adjusted_predictions_table = out_table("predictions_audit_adjusted")
adjusted_summary_table = out_table("agreement_summary_audit_adjusted")

# COMMAND ----------
# Non-destructive manual audit overlay for the top high-severity disagreement cases.
# Evidence source is the held prompt context: channel name plus recent video titles/descriptions.
now = datetime.now(timezone.utc).isoformat()
manual_records = [
    {
        "channel_id": "UCNqOu9KuAT4nY0VITaONdsw",
        "channel_name": "PlayGround Brasil",
        "current_topic_slugs": ["Lifestyle_(sociology)"],
        "audit_topic_slugs": ["Knowledge", "Entertainment"],
        "fix_action": "replace",
        "audit_confidence": 0.72,
        "audit_reason": "Recent videos are explainers/news-style clips about robots, dolphins, and work schedules; Lifestyle alone is too broad.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCVfaJIH2MR10172PUb3_j7w",
        "channel_name": "FDZNEWS 真實瞬間",
        "current_topic_slugs": ["Lifestyle_(sociology)"],
        "audit_topic_slugs": ["Entertainment"],
        "fix_action": "replace",
        "audit_confidence": 0.70,
        "audit_reason": "Short viral real-moment/traffic clips read as entertainment rather than lifestyle.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCkhy3CGgaX_qp3ZU0xgEVVw",
        "channel_name": "Priyodarshini",
        "current_topic_slugs": ["Entertainment"],
        "audit_topic_slugs": ["Lifestyle_(sociology)", "Entertainment"],
        "fix_action": "add",
        "audit_confidence": 0.78,
        "audit_reason": "Bengali relationship/emotional/motivation shorts are better captured by Lifestyle plus Entertainment.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UC82yFlo_suS67aJmG8tdVZA",
        "channel_name": "RDS WTH",
        "current_topic_slugs": ["Lifestyle_(sociology)", "Tourism"],
        "audit_topic_slugs": ["Entertainment", "Tourism"],
        "fix_action": "replace",
        "audit_confidence": 0.74,
        "audit_reason": "Cliff-jump/trampoline stunt clips are viral entertainment; Tourism is secondary.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UC9iozQVKe6uJE9-sB5A2zjg",
        "channel_name": "Mary Kay",
        "current_topic_slugs": ["Lifestyle_(sociology)"],
        "audit_topic_slugs": ["Business", "Lifestyle_(sociology)"],
        "fix_action": "add",
        "audit_confidence": 0.86,
        "audit_reason": "Cosmetics brand and direct-selling content makes Business a necessary reference label.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCEzyqpp5SAsJVrjGNM7L-Hg",
        "channel_name": "しゅんぽーかー",
        "current_topic_slugs": ["Lifestyle_(sociology)", "Technology"],
        "audit_topic_slugs": ["Entertainment", "Hobby"],
        "fix_action": "replace",
        "audit_confidence": 0.78,
        "audit_reason": "Poker/casino/gambling video content is entertainment/hobby; Technology appears unsupported.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCJcDnxsGrWHEOzSnIn7uqlA",
        "channel_name": "David Wilson Stories",
        "current_topic_slugs": ["Lifestyle_(sociology)"],
        "audit_topic_slugs": ["Entertainment", "Lifestyle_(sociology)"],
        "fix_action": "add",
        "audit_confidence": 0.82,
        "audit_reason": "Scripted emotional story shorts are entertainment, with lifestyle themes secondary.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCMvJuzB3dAdCuuUA3coaEww",
        "channel_name": "Somi Sharma Vlog",
        "current_topic_slugs": ["Lifestyle_(sociology)", "Music_of_Asia", "Music"],
        "audit_topic_slugs": ["Entertainment", "Music_of_Asia", "Music"],
        "fix_action": "add",
        "audit_confidence": 0.70,
        "audit_reason": "Dance/music shorts fit Music and also Entertainment; existing labels are not wrong but miss entertainment.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCUBXYDXqvP2fSGw8-jKCi7w",
        "channel_name": "Bule TV",
        "current_topic_slugs": ["Lifestyle_(sociology)"],
        "audit_topic_slugs": ["Entertainment", "Society", "Lifestyle_(sociology)"],
        "fix_action": "add",
        "audit_confidence": 0.68,
        "audit_reason": "Culture-shock and social observation shorts straddle Entertainment/Society, not just Lifestyle.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCV67pF4X1KiGYS-9XWHcqjA",
        "channel_name": "TRUE SHADOW",
        "current_topic_slugs": ["Lifestyle_(sociology)", "Entertainment"],
        "audit_topic_slugs": ["Knowledge", "Lifestyle_(sociology)", "Entertainment"],
        "fix_action": "add",
        "audit_confidence": 0.76,
        "audit_reason": "Facts, health, motivation, and animal explainer shorts require Knowledge as an accepted label.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCVHASEl6FQQFmvjLqY8SeJQ",
        "channel_name": "DP Mindset",
        "current_topic_slugs": ["Pop_music", "Music"],
        "audit_topic_slugs": ["Lifestyle_(sociology)", "Knowledge"],
        "fix_action": "replace",
        "audit_confidence": 0.88,
        "audit_reason": "Relationship psychology/facts shorts are not music; Lifestyle/Knowledge are supported by the evidence.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCY1gfjrB1kwHuluY7ZfsLqQ",
        "channel_name": "Москва Ташкент автобус",
        "current_topic_slugs": ["Society"],
        "audit_topic_slugs": ["Tourism"],
        "fix_action": "replace",
        "audit_confidence": 0.80,
        "audit_reason": "Repeated Moscow-Tashkent bus route clips are transport/travel service content, closest allowed label is Tourism.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCdXuhSJ5ESPNAYSohG61Zqw",
        "channel_name": "INFOSELEB49",
        "current_topic_slugs": ["Society"],
        "audit_topic_slugs": ["Entertainment", "Politics", "Society"],
        "fix_action": "add",
        "audit_confidence": 0.64,
        "audit_reason": "Celebrity/public-figure shorts with Indonesian political names straddle Entertainment, Politics, and Society.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCoXgIH0n6ccxZejapsjiOww",
        "channel_name": "Daily Update life",
        "current_topic_slugs": ["Music", "Music_of_Asia"],
        "audit_topic_slugs": ["Politics", "Music", "Music_of_Asia"],
        "fix_action": "add",
        "audit_confidence": 0.62,
        "audit_reason": "Mixed devotional/music and BJP/political clips; add Politics but keep music labels.",
        "evidence_source": "prompt_recent_videos",
    },
    {
        "channel_id": "UCsxQCFPkeCMLhbHq04gRQZQ",
        "channel_name": "Domenic Biagini",
        "current_topic_slugs": ["Lifestyle_(sociology)"],
        "audit_topic_slugs": ["Tourism", "Hobby"],
        "fix_action": "replace",
        "audit_confidence": 0.77,
        "audit_reason": "Whale-watching/drone wildlife channel is closer to Tourism/Hobby than Lifestyle.",
        "evidence_source": "prompt_recent_videos",
    },
]

schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("current_topic_slugs", ArrayType(StringType()), True),
    StructField("audit_topic_slugs", ArrayType(StringType()), True),
    StructField("fix_action", StringType(), True),
    StructField("audit_confidence", DoubleType(), True),
    StructField("audit_reason", StringType(), True),
    StructField("evidence_source", StringType(), True),
    StructField("audited_at_utc", StringType(), True),
])
audit_rows = [
    (
        RUN_ID,
        r["channel_id"],
        r["channel_name"],
        r["current_topic_slugs"],
        r["audit_topic_slugs"],
        r["fix_action"],
        float(r["audit_confidence"]),
        r["audit_reason"],
        r["evidence_source"],
        now,
    )
    for r in manual_records
]
audit_df = spark.createDataFrame(audit_rows, schema)
write_run_scoped(audit_df, audit_table)

# COMMAND ----------
pred = spark.table(out_table("predictions")).where(F.col("run_id") == F.lit(RUN_ID))
fixes = audit_df.select("run_id", "channel_id", "fix_action", "audit_topic_slugs", "audit_confidence", "audit_reason")

adjusted = (
    pred.join(fixes, on=["run_id", "channel_id"], how="left")
    .withColumn(
        "topic_slugs_audit_adjusted",
        F.when(F.col("fix_action") == F.lit("replace"), F.col("audit_topic_slugs"))
        .when(F.col("fix_action") == F.lit("add"), F.array_distinct(F.concat(F.coalesce(F.col("topic_slugs"), F.array()), F.col("audit_topic_slugs"))))
        .otherwise(F.col("topic_slugs")),
    )
    .withColumn(
        "agrees_any_topic_audit_adjusted",
        F.col("valid_prediction") & F.array_contains(F.col("topic_slugs_audit_adjusted"), F.col("category_id")),
    )
    .withColumn("audit_adjusted", F.col("fix_action").isNotNull())
    .withColumn("audit_adjusted_at", F.current_timestamp())
)
write_run_scoped(adjusted, adjusted_predictions_table)

summary = (
    adjusted
    .groupBy("run_id", "provider", "model", "model_tier")
    .agg(
        F.count("*").alias("n_result_rows"),
        F.sum(F.when(F.col("has_reference_any"), 1).otherwise(0)).alias("n_with_original_reference"),
        F.sum(F.when(F.size(F.col("topic_slugs_audit_adjusted")) > 0, 1).otherwise(0)).alias("n_with_adjusted_reference"),
        F.sum(F.when(F.col("valid_prediction"), 1).otherwise(0)).alias("n_valid_predictions"),
        F.sum(F.when(F.col("has_reference_any") & F.col("agrees_any_topic"), 1).otherwise(0)).alias("n_agree_any_topic_original"),
        F.sum(F.when((F.size(F.col("topic_slugs_audit_adjusted")) > 0) & F.col("agrees_any_topic_audit_adjusted"), 1).otherwise(0)).alias("n_agree_any_topic_audit_adjusted"),
        F.sum(F.when(F.col("audit_adjusted"), 1).otherwise(0)).alias("n_predictions_on_audited_channels"),
    )
    .withColumn("agreement_any_topic_original", F.col("n_agree_any_topic_original") / F.col("n_with_original_reference"))
    .withColumn("agreement_any_topic_audit_adjusted", F.col("n_agree_any_topic_audit_adjusted") / F.col("n_with_adjusted_reference"))
    .withColumn("audit_adjusted_delta", F.col("agreement_any_topic_audit_adjusted") - F.col("agreement_any_topic_original"))
    .withColumn("summary_created_at", F.current_timestamp())
)
write_run_scoped(summary, adjusted_summary_table)
display(summary.orderBy(F.desc("agreement_any_topic_audit_adjusted")))

payload = {
    "run_id": RUN_ID,
    "manual_audit_fixes_table": audit_table,
    "predictions_audit_adjusted_table": adjusted_predictions_table,
    "agreement_summary_audit_adjusted_table": adjusted_summary_table,
    "n_audited_channels": len(manual_records),
    "model_adjusted_summary": [
        row.asDict(recursive=True)
        for row in summary.orderBy(F.desc("agreement_any_topic_audit_adjusted")).select(
            "provider",
            "model",
            "agreement_any_topic_original",
            "agreement_any_topic_audit_adjusted",
            "audit_adjusted_delta",
            "n_agree_any_topic_original",
            "n_agree_any_topic_audit_adjusted",
        ).collect()
    ],
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True, default=str))
