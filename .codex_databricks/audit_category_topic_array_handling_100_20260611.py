# Databricks notebook source
# MAGIC %md
# MAGIC # Topic Category Array Handling Audit: 100 Random Cases
# MAGIC
# MAGIC Audits the YouTube API `topic_categories` array as a multi-label reference object. The first element is
# MAGIC retained only as a legacy diagnostic, not as the truth label.

# COMMAND ----------
import csv
import json
import os
from collections import Counter
from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


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
        return os.environ.get(name.upper(), default)


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "category_topic_random_1000_20260611")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")
_create_text_widget("channels_table", "prod_tads.youtube_too.yt_sl_channels")
_create_text_widget("sample_size", "100")
_create_text_widget("random_seed", "20260612")
_create_text_widget("audit_output_dir", "/dbfs/FileStore/youtube_category_topic_batches/analysis")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "category_topic_random_1000_20260611")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000")
CHANNELS_TABLE = _get_widget("channels_table", "prod_tads.youtube_too.yt_sl_channels")
SAMPLE_SIZE = int(_get_widget("sample_size", "100"))
RANDOM_SEED = int(_get_widget("random_seed", "20260612"))
AUDIT_OUTPUT_DIR = _get_widget("audit_output_dir", "/dbfs/FileStore/youtube_category_topic_batches/analysis").rstrip("/")


def fqtn(table: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in table.split("."))


def out_table(suffix: str) -> str:
    return fqtn(f"{CATALOG}.{SCHEMA}.{OUTPUT_PREFIX}_{suffix}")


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


def stable_order_col(*cols: str):
    exprs = [F.lit(str(RANDOM_SEED))]
    for c in cols:
        exprs.append(F.coalesce(F.col(c).cast("string"), F.lit("")))
    return F.sha2(F.concat_ws("||", *exprs), 256)


def compact(text, max_chars=900):
    if text is None:
        return ""
    return " ".join(str(text).split())[:max_chars]


def normalize_list(value):
    if value is None:
        return []
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                value = parsed
            else:
                return [value]
        except Exception:
            return [value] if value else []
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if v is not None and str(v) != ""]
    return []


def slug_name(slug):
    return None if slug is None else str(slug).replace("_", " ")


def infer_issue(reference_slugs, legacy_first, consensus_id, consensus_share, n_valid, n_distinct, n_agree_any, n_agree_legacy_first, n_videos):
    reference_slugs = set(reference_slugs or [])
    if not reference_slugs:
        return (
            "missing_reference_array",
            "medium",
            "No nonempty YouTube API topic_categories array is present.",
            "Exclude from agreement denominators until the API array is populated.",
        )
    if n_videos == 0:
        return (
            "insufficient_prompt_evidence",
            "medium",
            "No recent video evidence was available for blind model classification.",
            "Do not use for model ranking unless additional channel/video evidence is added.",
        )
    if consensus_id in reference_slugs and consensus_id != legacy_first:
        return (
            "legacy_first_element_false_error",
            "high",
            "The model consensus matches another element of the YouTube API array; first-element scoring would falsely mark this wrong.",
            "Use array-aware agreement; keep legacy_first_topic only for backward-compatible diagnostics.",
        )
    if consensus_id in reference_slugs:
        return (
            "array_reference_matches_ai",
            "none",
            "The AI consensus is contained in the YouTube API topic_categories array.",
            "No reference fix needed; score with any-array-element agreement.",
        )
    if consensus_id and n_valid and consensus_share is not None and consensus_share >= 0.67:
        return (
            "possible_reference_array_missing_ai_consensus",
            "high",
            "A strong AI consensus is not present in the YouTube API topic_categories array.",
            "Manual evidence review needed; if evidence supports the consensus, add an audit overlay label rather than mutating the source API table automatically.",
        )
    if n_valid and n_agree_any == 0:
        return (
            "all_models_outside_reference_array",
            "high",
            "No imported model prediction matches any element of the YouTube API topic_categories array.",
            "Manual evidence review needed; could be sparse API topics, weak evidence, or model confusion.",
        )
    if n_distinct >= 4:
        return (
            "low_model_consensus_ambiguous",
            "medium",
            "Models spread across many categories; the channel evidence or taxonomy boundary is ambiguous.",
            "Retain as diagnostic; avoid using as a clean ranking case without manual adjudication.",
        )
    if n_agree_any < n_valid:
        return (
            "partial_model_disagreement",
            "low",
            "Some models choose categories outside the reference array while others match it.",
            "No source-table fix by default; use for model-error characterization.",
        )
    return ("unclassified", "low", "No specific failure pattern detected.", "Inspect manually if needed.")


prompt_inputs_full = out_table("prompt_inputs")
predictions_full = out_table("predictions")
audit_full = out_table("array_handling_audit_100")
audit_fix_full = out_table("array_handling_audit_100_fix_recommendations")

print("Reading", prompt_inputs_full, predictions_full)

# COMMAND ----------
prompt = spark.table(prompt_inputs_full).where(F.col("run_id") == F.lit(RUN_ID))
pred = (
    spark.table(predictions_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .withColumn("model_label", F.concat_ws(":", F.col("provider"), F.col("model")))
)

channels = spark.table(fqtn(CHANNELS_TABLE))
channel_desc_col = None
for candidate in ["channel_description", "description", "about", "bio", "channel_about", "profile_description", "channel_text"]:
    if candidate in channels.columns:
        channel_desc_col = candidate
        break

channel_cols = [
    F.col("channel_id").cast("string").alias("channel_id"),
]
if channel_desc_col:
    channel_cols.append(F.substring(F.regexp_replace(F.col(channel_desc_col).cast("string"), r"[\r\n\t]+", " "), 1, 1000).alias("channel_description"))
else:
    channel_cols.append(F.lit("").cast("string").alias("channel_description"))
channel_desc = channels.select(*channel_cols).dropDuplicates(["channel_id"])

sample_ids = (
    prompt
    .select("channel_id")
    .dropDuplicates(["channel_id"])
    .orderBy(stable_order_col("channel_id"), F.col("channel_id"))
    .limit(SAMPLE_SIZE)
)

sample_prompt = (
    prompt
    .join(F.broadcast(sample_ids), on="channel_id", how="inner")
    .join(channel_desc, on="channel_id", how="left")
)

pred_pdf = pred.join(F.broadcast(sample_ids), on="channel_id", how="inner").toPandas()
prompt_pdf = sample_prompt.toPandas()

records = []
fix_records = []
audit_time = datetime.now(timezone.utc).isoformat()
for _, prow in prompt_pdf.sort_values("channel_id").iterrows():
    channel_id = prow["channel_id"]
    group = pred_pdf[pred_pdf["channel_id"] == channel_id].copy()
    valid = group[group["valid_prediction"].fillna(False) & group["category_id"].notna()]
    counts = Counter(valid["category_id"].tolist())
    if counts:
        consensus_id, consensus_n = sorted(counts.items(), key=lambda kv: (-kv[1], str(kv[0])))[0]
    else:
        consensus_id, consensus_n = None, 0
    n_valid = int(len(valid))
    n_distinct = int(len(counts))
    consensus_share = float(consensus_n / n_valid) if n_valid else None
    topic_categories = normalize_list(prow.get("topic_categories"))
    topic_slugs = normalize_list(prow.get("topic_slugs"))
    legacy_first = prow.get("primary_topic_slug")
    n_agree_any = int(group["agrees_any_topic"].fillna(False).sum()) if "agrees_any_topic" in group.columns else 0
    n_agree_legacy_first = int(group["agrees_primary"].fillna(False).sum()) if "agrees_primary" in group.columns else 0
    n_videos = int(prow.get("n_videos_in_prompt") or 0)
    issue_type, severity, issue_diagnosis, fix_recommendation = infer_issue(
        topic_slugs,
        legacy_first,
        consensus_id,
        consensus_share,
        n_valid,
        n_distinct,
        n_agree_any,
        n_agree_legacy_first,
        n_videos,
    )

    model_tags = []
    for _, row in group.sort_values(["provider", "model"]).iterrows():
        model_tags.append({
            "model": row.get("model_label"),
            "category_id": row.get("category_id"),
            "category_name": row.get("category_name"),
            "agree_any_array": bool(row.get("agrees_any_topic")) if row.get("agrees_any_topic") == row.get("agrees_any_topic") else False,
            "agree_legacy_first": bool(row.get("agrees_primary")) if row.get("agrees_primary") == row.get("agrees_primary") else False,
            "confidence": None if row.get("confidence") != row.get("confidence") else row.get("confidence"),
            "rationale": row.get("rationale_short"),
        })

    recent_videos = compact(prow.get("recent_videos_text"), 1200)
    reference_array_names = [slug_name(s) for s in topic_slugs]
    records.append({
        "run_id": RUN_ID,
        "sample_seed": RANDOM_SEED,
        "audit_sample_n": SAMPLE_SIZE,
        "channel_id": channel_id,
        "channel_name": prow.get("channel_name"),
        "channel_description": compact(prow.get("channel_description"), 1000),
        "recent_video_titles_descriptions": recent_videos,
        "youtube_topic_categories_json": json.dumps(topic_categories, ensure_ascii=False),
        "reference_topic_slugs_json": json.dumps(topic_slugs, ensure_ascii=False),
        "reference_topic_names_json": json.dumps(reference_array_names, ensure_ascii=False),
        "legacy_first_topic_slug": legacy_first,
        "legacy_first_topic_name": slug_name(legacy_first),
        "consensus_ai_category_id": consensus_id,
        "consensus_ai_category_name": slug_name(consensus_id),
        "consensus_n_models": int(consensus_n),
        "n_valid_model_predictions": n_valid,
        "n_distinct_model_categories": n_distinct,
        "consensus_share": consensus_share,
        "n_models_match_any_reference_array_element": n_agree_any,
        "n_models_match_legacy_first_topic": n_agree_legacy_first,
        "model_category_tags_json": json.dumps(model_tags, ensure_ascii=False),
        "issue_type": issue_type,
        "severity": severity,
        "issue_diagnosis": issue_diagnosis,
        "fix_recommendation": fix_recommendation,
        "source_fix_action": "none" if severity in {"none", "low"} else "manual_review_overlay",
        "audited_at_utc": audit_time,
    })
    fix_records.append({
        "run_id": RUN_ID,
        "channel_id": channel_id,
        "channel_name": prow.get("channel_name"),
        "current_reference_topic_slugs_json": json.dumps(topic_slugs, ensure_ascii=False),
        "suggested_reference_topic_slugs_json": json.dumps(sorted(set(topic_slugs + ([consensus_id] if consensus_id and issue_type in {"possible_reference_array_missing_ai_consensus", "all_models_outside_reference_array"} else []))), ensure_ascii=False),
        "consensus_ai_category_id": consensus_id,
        "issue_type": issue_type,
        "severity": severity,
        "fix_recommendation": fix_recommendation,
        "mutate_source_table": False,
        "created_at_utc": audit_time,
    })

audit_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("sample_seed", IntegerType(), True),
    StructField("audit_sample_n", IntegerType(), True),
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("channel_description", StringType(), True),
    StructField("recent_video_titles_descriptions", StringType(), True),
    StructField("youtube_topic_categories_json", StringType(), True),
    StructField("reference_topic_slugs_json", StringType(), True),
    StructField("reference_topic_names_json", StringType(), True),
    StructField("legacy_first_topic_slug", StringType(), True),
    StructField("legacy_first_topic_name", StringType(), True),
    StructField("consensus_ai_category_id", StringType(), True),
    StructField("consensus_ai_category_name", StringType(), True),
    StructField("consensus_n_models", IntegerType(), True),
    StructField("n_valid_model_predictions", IntegerType(), True),
    StructField("n_distinct_model_categories", IntegerType(), True),
    StructField("consensus_share", DoubleType(), True),
    StructField("n_models_match_any_reference_array_element", IntegerType(), True),
    StructField("n_models_match_legacy_first_topic", IntegerType(), True),
    StructField("model_category_tags_json", StringType(), True),
    StructField("issue_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("issue_diagnosis", StringType(), True),
    StructField("fix_recommendation", StringType(), True),
    StructField("source_fix_action", StringType(), True),
    StructField("audited_at_utc", StringType(), True),
])
fix_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("current_reference_topic_slugs_json", StringType(), True),
    StructField("suggested_reference_topic_slugs_json", StringType(), True),
    StructField("consensus_ai_category_id", StringType(), True),
    StructField("issue_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("fix_recommendation", StringType(), True),
    StructField("mutate_source_table", BooleanType(), True),
    StructField("created_at_utc", StringType(), True),
])

audit_df = spark.createDataFrame(records, schema=audit_schema)
fix_df = spark.createDataFrame(fix_records, schema=fix_schema)
write_run_scoped(audit_df, audit_full)
write_run_scoped(fix_df, audit_fix_full)

# COMMAND ----------
run_dir = os.path.join(AUDIT_OUTPUT_DIR, RUN_ID)
os.makedirs(run_dir, exist_ok=True)
csv_path = os.path.join(run_dir, "category_topic_array_audit_100.csv")
json_path = os.path.join(run_dir, "category_topic_array_audit_100_summary.json")

csv_columns = [
    "channel_id",
    "channel_name",
    "channel_description",
    "recent_video_titles_descriptions",
    "youtube_topic_categories_json",
    "reference_topic_slugs_json",
    "legacy_first_topic_slug",
    "consensus_ai_category_id",
    "consensus_n_models",
    "n_valid_model_predictions",
    "n_distinct_model_categories",
    "n_models_match_any_reference_array_element",
    "n_models_match_legacy_first_topic",
    "model_category_tags_json",
    "issue_type",
    "severity",
    "issue_diagnosis",
    "fix_recommendation",
]
with open(csv_path, "w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=csv_columns)
    writer.writeheader()
    for rec in records:
        writer.writerow({c: rec.get(c) for c in csv_columns})

issue_counts = Counter(r["issue_type"] for r in records)
severity_counts = Counter(r["severity"] for r in records)
summary = {
    "run_id": RUN_ID,
    "sample_size": SAMPLE_SIZE,
    "random_seed": RANDOM_SEED,
    "channel_description_column": channel_desc_col,
    "audit_table": audit_full,
    "fix_recommendations_table": audit_fix_full,
    "csv_path": csv_path,
    "issue_counts": dict(issue_counts),
    "severity_counts": dict(severity_counts),
    "array_handling_rule": "topic_categories is a multi-label array; any element is a held-out reference label; order is diagnostic only.",
    "source_table_mutated": False,
}
with open(json_path, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, ensure_ascii=False, indent=2, sort_keys=True)

display(audit_df.groupBy("issue_type", "severity").count().orderBy(F.desc("count"), "issue_type"))
display(audit_df.select(
    "channel_id",
    "channel_name",
    "reference_topic_slugs_json",
    "legacy_first_topic_slug",
    "consensus_ai_category_id",
    "n_models_match_any_reference_array_element",
    "n_models_match_legacy_first_topic",
    "issue_type",
    "issue_diagnosis",
    "fix_recommendation",
).orderBy("severity", "issue_type", "channel_id"))

dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False, sort_keys=True))
