# Databricks notebook source
# MAGIC %md
# MAGIC # Topic/Genre 1k Result Import and Agreement Evaluation
# MAGIC
# MAGIC **Deprecated for the current `topic_categories` target.** This importer expects one predicted
# MAGIC `category_id`. Use `import_evaluate_category_topic_multilabel_20260612.py` for exact multi-label
# MAGIC prediction of the observed YouTube API category array.

# COMMAND ----------
import json
import os
import re
from typing import Any, Dict, List, Optional

from pyspark.sql import functions as F
from pyspark.sql.types import (
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
        return default


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "category_topic_random_1000_20260611")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_batches/results")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "category_topic_random_1000_20260611")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000")
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_batches/results").rstrip("/")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def out_table(suffix: str) -> str:
    return fqtn(f"{OUTPUT_PREFIX}_{suffix}")


def spark_path(path: str) -> str:
    return path.replace("/dbfs/", "dbfs:/", 1) if path.startswith("/dbfs/") else path


def local_fs_path(path: str) -> str:
    return path.replace("dbfs:/", "/dbfs/", 1) if path.startswith("dbfs:/") else path


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


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


prompt_inputs_full = out_table("prompt_inputs")
requests_full = out_table("requests")
raw_results_full = out_table("raw_results")
predictions_full = out_table("predictions")
agreement_summary_full = out_table("agreement_summary")

# COMMAND ----------
prompt_inputs = spark.table(prompt_inputs_full).where(F.col("run_id") == F.lit(RUN_ID))
allowed_rows = (
    prompt_inputs
    .select(F.explode_outer("topic_slugs").alias("topic_slug"))
    .where(F.col("topic_slug").isNotNull() & (F.length(F.trim("topic_slug")) > 0))
    .dropDuplicates(["topic_slug"])
    .withColumn("topic_name", F.regexp_replace(F.col("topic_slug"), "_", " "))
    .orderBy("topic_slug")
    .collect()
)
VALID_CATEGORY_IDS = {r["topic_slug"] for r in allowed_rows}
CATEGORY_NAME_BY_ID = {r["topic_slug"]: (r["topic_name"] or r["topic_slug"]).replace("_", " ") for r in allowed_rows}
CATEGORY_ID_BY_NAME = {
    (r["topic_name"] or r["topic_slug"]).strip().lower(): r["topic_slug"]
    for r in allowed_rows
}
CATEGORY_ID_BY_NAME.update({slug.replace("_", " ").lower(): slug for slug in VALID_CATEGORY_IDS})

print(f"Allowed held-out topic-array labels: {len(VALID_CATEGORY_IDS)}")

# COMMAND ----------
parse_schema = StructType([
    StructField("request_id", StringType(), True),
    StructField("provider_result_model", StringType(), True),
    StructField("raw_text", StringType(), True),
    StructField("result_status", StringType(), True),
    StructField("input_tokens", IntegerType(), True),
    StructField("output_tokens", IntegerType(), True),
    StructField("parse_error", StringType(), True),
])


def _dig(obj: Any, path: List[Any], default=None):
    cur = obj
    for p in path:
        try:
            cur = cur[p]
        except Exception:
            return default
    return cur


def _collect_openai_response_text(body: Dict[str, Any]) -> Optional[str]:
    if not isinstance(body, dict):
        return None
    if body.get("output_text"):
        return body.get("output_text")
    chat_text = _dig(body, ["choices", 0, "message", "content"])
    if chat_text:
        return chat_text
    chunks = []
    for item in body.get("output", []) or []:
        for part in item.get("content", []) or []:
            if isinstance(part, dict) and part.get("type") in {"output_text", "text"} and part.get("text"):
                chunks.append(part.get("text"))
    return "\n".join(chunks) if chunks else None


def extract_provider_text(line: str) -> Dict[str, Any]:
    try:
        obj = json.loads(line)
    except Exception as exc:
        return {"request_id": None, "provider_result_model": None, "raw_text": None, "result_status": "json_load_error", "input_tokens": None, "output_tokens": None, "parse_error": repr(exc)[:500]}

    request_id = obj.get("custom_id") or obj.get("key") or obj.get("id")
    text = None
    model = None
    status = None
    input_tokens = None
    output_tokens = None

    body = _dig(obj, ["response", "body"])
    if body:
        status = str(_dig(obj, ["response", "status_code"], body.get("status", "succeeded")))
        model = body.get("model")
        text = _collect_openai_response_text(body)
        usage = body.get("usage", {}) or {}
        input_tokens = usage.get("input_tokens") or usage.get("prompt_tokens")
        output_tokens = usage.get("output_tokens") or usage.get("completion_tokens")
        if text is None and obj.get("error"):
            status = "error"

    if text is None and obj.get("result"):
        r = obj.get("result", {})
        status = r.get("type") if isinstance(r, dict) else None
        message = r.get("message", {}) if isinstance(r, dict) else {}
        model = message.get("model")
        content = message.get("content", [])
        if content and isinstance(content, list):
            text = "\n".join([c.get("text", "") for c in content if isinstance(c, dict) and c.get("type") == "text"])
        usage = message.get("usage", {})
        input_tokens = usage.get("input_tokens")
        output_tokens = usage.get("output_tokens")

    if text is None:
        request_id = request_id or obj.get("key")
        status = status or obj.get("status") or _dig(obj, ["response", "status"]) or "unknown"
        model = obj.get("model") or obj.get("modelVersion") or _dig(obj, ["response", "modelVersion"])
        text = (
            _dig(obj, ["response", "candidates", 0, "content", "parts", 0, "text"])
            or _dig(obj, ["response", "candidates", 0, "content", 0, "parts", 0, "text"])
            or _dig(obj, ["candidates", 0, "content", "parts", 0, "text"])
            or _dig(obj, ["inlineResponse", "candidates", 0, "content", "parts", 0, "text"])
            or _dig(obj, ["response", "text"])
        )
        input_tokens = _dig(obj, ["response", "usageMetadata", "promptTokenCount"]) or _dig(obj, ["usageMetadata", "promptTokenCount"])
        output_tokens = _dig(obj, ["response", "usageMetadata", "candidatesTokenCount"]) or _dig(obj, ["usageMetadata", "candidatesTokenCount"])

    if text is None:
        err = obj.get("error") or _dig(obj, ["response", "error"]) or _dig(obj, ["result", "error"])
        return {"request_id": request_id, "provider_result_model": model, "raw_text": None, "result_status": status, "input_tokens": input_tokens, "output_tokens": output_tokens, "parse_error": (json.dumps(err)[:500] if err else "could_not_extract_text")}

    return {"request_id": request_id, "provider_result_model": model, "raw_text": text, "result_status": status or "succeeded", "input_tokens": input_tokens, "output_tokens": output_tokens, "parse_error": None}


@F.udf(parse_schema)
def extract_provider_text_udf(line: str):
    d = extract_provider_text(line)
    return tuple(d.get(field.name) for field in parse_schema.fields)


prediction_json_schema = StructType([
    StructField("category_id", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("ambiguous", BooleanType(), True),
    StructField("rationale_short", StringType(), True),
    StructField("prediction_parse_error", StringType(), True),
])


def extract_first_json_object(text: Optional[str]) -> Optional[Dict[str, Any]]:
    if text is None:
        return None
    text = text.strip()
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(text):
        if ch != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[idx:])
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    return None


def normalize_category_id(raw: Any) -> str:
    value = str(raw or "").strip()
    if "/wiki/" in value:
        value = value.rsplit("/wiki/", 1)[1]
    value = value.replace(" ", "_") if value not in VALID_CATEGORY_IDS else value
    return value


def normalize_category_prediction(text: Optional[str]) -> Dict[str, Any]:
    obj = extract_first_json_object(text)
    if not obj:
        return {"category_id": None, "category_name": None, "confidence": None, "ambiguous": None, "rationale_short": None, "prediction_parse_error": "invalid_or_missing_json"}

    cid = normalize_category_id(obj.get("category_id"))
    cname = str(obj.get("category_name", "")).strip()
    conf_raw = obj.get("confidence")
    ambiguous_raw = obj.get("ambiguous")
    rationale = obj.get("rationale_short")

    if cid not in VALID_CATEGORY_IDS and cname:
        cid = CATEGORY_ID_BY_NAME.get(cname.lower(), cid)
    if cid in VALID_CATEGORY_IDS:
        cname = CATEGORY_NAME_BY_ID[cid]
    else:
        return {"category_id": None, "category_name": cname or None, "confidence": None, "ambiguous": None, "rationale_short": str(rationale)[:500] if rationale else None, "prediction_parse_error": f"invalid_category_id:{cid}"}

    try:
        conf = float(conf_raw) if conf_raw is not None else None
        if conf is not None:
            conf = max(0.0, min(1.0, conf))
    except Exception:
        conf = None

    if isinstance(ambiguous_raw, bool):
        ambiguous = ambiguous_raw
    elif isinstance(ambiguous_raw, str):
        ambiguous = ambiguous_raw.strip().lower() in {"true", "1", "yes", "y"}
    else:
        ambiguous = None

    return {"category_id": cid, "category_name": cname, "confidence": conf, "ambiguous": ambiguous, "rationale_short": str(rationale)[:500] if rationale else None, "prediction_parse_error": None}


@F.udf(prediction_json_schema)
def normalize_category_prediction_udf(raw_text: str):
    d = normalize_category_prediction(raw_text)
    return tuple(d.get(field.name) for field in prediction_json_schema.fields)

# COMMAND ----------
run_results_dir = os.path.join(RESULTS_INPUT_DIR, RUN_ID)
if not os.path.exists(local_fs_path(run_results_dir)):
    raise FileNotFoundError(f"Run results directory does not exist: {run_results_dir}")

print("Importing result JSONL files from", run_results_dir)
result_lines = spark.read.option("recursiveFileLookup", "true").text(spark_path(run_results_dir))
raw_results = (
    result_lines
    .withColumn("parsed", extract_provider_text_udf(F.col("value")))
    .select("value", "parsed.*")
    .where(F.col("request_id").isNotNull())
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("imported_at", F.current_timestamp())
)

request_ids_for_run = spark.table(requests_full).where(F.col("run_id") == F.lit(RUN_ID)).select("request_id").dropDuplicates(["request_id"])
raw_results = raw_results.join(request_ids_for_run, on="request_id", how="inner")
write_run_scoped(raw_results, raw_results_full)
print("Wrote raw results to", raw_results_full)

# COMMAND ----------
req_map = (
    spark.table(requests_full).where(F.col("run_id") == F.lit(RUN_ID))
    .select("run_id", "request_id", "provider", "model", "model_tier", "channel_id")
)
ref_map = (
    prompt_inputs
    .select(
        "run_id",
        "channel_id",
        "channel_name",
        "topic_categories",
        "topic_slugs",
        "primary_topic_url",
        "primary_topic_slug",
        "primary_topic_name",
        "topic_category_count",
        "primary_language_label",
        "primary_language_iso639_3",
        "n_videos_in_prompt",
    )
)

raw_results_loaded = spark.table(raw_results_full).where(F.col("run_id") == F.lit(RUN_ID))
parsed_predictions = (
    raw_results_loaded
    .withColumn("pred", normalize_category_prediction_udf(F.col("raw_text")))
    .select("run_id", "request_id", "provider_result_model", "result_status", "input_tokens", "output_tokens", "raw_text", "parse_error", "pred.*", "imported_at")
    .join(req_map, on=["run_id", "request_id"], how="inner")
    .join(ref_map, on=["run_id", "channel_id"], how="left")
    .withColumn("has_reference_primary", F.col("primary_topic_slug").isNotNull())
    .withColumn("has_reference_any", F.col("topic_slugs").isNotNull() & (F.size("topic_slugs") > 0))
    .withColumn("valid_prediction", F.col("prediction_parse_error").isNull() & F.col("category_id").isNotNull())
    .withColumn("agrees_primary", F.col("valid_prediction") & (F.col("category_id") == F.col("primary_topic_slug")))
    .withColumn("agrees_any_topic", F.col("valid_prediction") & F.array_contains(F.col("topic_slugs"), F.col("category_id")))
    .withColumn("evaluated_at", F.current_timestamp())
)
write_run_scoped(parsed_predictions, predictions_full)
print("Wrote parsed predictions to", predictions_full)

# COMMAND ----------
summary = (
    parsed_predictions
    .groupBy("run_id", "provider", "model", "model_tier")
    .agg(
        F.count("*").alias("n_result_rows"),
        F.countDistinct("channel_id").alias("n_channels_with_result"),
        F.sum(F.when(F.col("has_reference_primary"), 1).otherwise(0)).alias("n_with_primary_reference"),
        F.sum(F.when(F.col("has_reference_any"), 1).otherwise(0)).alias("n_with_any_reference"),
        F.sum(F.when(F.col("valid_prediction"), 1).otherwise(0)).alias("n_valid_predictions"),
        F.sum(F.when(F.col("has_reference_any") & F.col("valid_prediction"), 1).otherwise(0)).alias("n_valid_predictions_with_reference"),
        F.sum(F.when(F.col("has_reference_any") & F.col("agrees_any_topic"), 1).otherwise(0)).alias("n_agree_any_topic"),
        F.sum(F.when(F.col("has_reference_primary") & F.col("agrees_primary"), 1).otherwise(0)).alias("n_agree_primary"),
        F.avg("confidence").alias("mean_confidence"),
        F.sum(F.when(F.col("parse_error").isNotNull() | F.col("prediction_parse_error").isNotNull(), 1).otherwise(0)).alias("n_parse_or_prediction_errors"),
        F.avg("input_tokens").alias("mean_input_tokens"),
        F.avg("output_tokens").alias("mean_output_tokens"),
    )
    .withColumn("agreement_any_topic_strict", F.col("n_agree_any_topic") / F.col("n_with_any_reference"))
    .withColumn("agreement_primary_strict", F.col("n_agree_primary") / F.col("n_with_primary_reference"))
    .withColumn("agreement_any_topic_valid_only", F.col("n_agree_any_topic") / F.col("n_valid_predictions_with_reference"))
    .withColumn("valid_prediction_rate", F.col("n_valid_predictions") / F.col("n_result_rows"))
    .withColumn("summary_created_at", F.current_timestamp())
)
write_run_scoped(summary, agreement_summary_full)

ordered = summary.orderBy(F.desc("agreement_any_topic_strict"), "provider", "model")
display(ordered)
payload = {
    "run_id": RUN_ID,
    "raw_results_table": raw_results_full,
    "predictions_table": predictions_full,
    "agreement_summary_table": agreement_summary_full,
    "models_imported": [
        row.asDict(recursive=True)
        for row in ordered.select(
            "provider",
            "model",
            "n_result_rows",
            "n_with_any_reference",
            "n_valid_predictions",
            "n_agree_any_topic",
            "agreement_any_topic_strict",
            "agreement_primary_strict",
            "valid_prediction_rate",
        ).collect()
    ],
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True, default=str))
