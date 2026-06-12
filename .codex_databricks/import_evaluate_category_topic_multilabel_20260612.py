# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube Topic Categories: Multi-label Result Import, Calibration, and Evaluation
# MAGIC
# MAGIC Imports provider JSONL results for the multi-label topicCategories validation run, parses one
# MAGIC probability per observed YouTube topic label, calibrates thresholds on the calibration split, and
# MAGIC evaluates exact observed YouTube label-set prediction on the heldout test split.

# COMMAND ----------
import json
import math
import os
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
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
        return default


def _get_int_widget(name: str, default: int) -> int:
    raw = _get_widget(name, str(default)).strip()
    return int(raw) if raw else default


def _get_bool_widget(name: str, default: bool) -> bool:
    raw = _get_widget(name, str(default)).strip().lower()
    return raw in {"1", "true", "t", "yes", "y"}


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "category_topic_multilabel_random_1000_20260612")
_create_text_widget("output_prefix", "yt_category_topic_multilabel_1000")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches/results")
_create_text_widget("min_label_threshold_calibration_positives", "3")
_create_text_widget("min_label_threshold_calibration_negatives", "10")
_create_text_widget("closure_edges_table", "dev_sean.matt.yt_channel_topic_taxonomy_inferred_edges_20260612")
_create_text_widget("enable_closure_postprocess", "true")
_create_text_widget("closure_edge_strengths", "strong_empirical_parent")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "category_topic_multilabel_random_1000_20260612")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_multilabel_1000")
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_multilabel_batches/results").rstrip("/")
MIN_LABEL_THRESHOLD_CALIBRATION_POSITIVES = _get_int_widget("min_label_threshold_calibration_positives", 3)
MIN_LABEL_THRESHOLD_CALIBRATION_NEGATIVES = _get_int_widget("min_label_threshold_calibration_negatives", 10)
CLOSURE_EDGES_TABLE = _get_widget("closure_edges_table", "dev_sean.matt.yt_channel_topic_taxonomy_inferred_edges_20260612").strip()
ENABLE_CLOSURE_POSTPROCESS = _get_bool_widget("enable_closure_postprocess", True)
CLOSURE_EDGE_STRENGTHS = {x.strip() for x in _get_widget("closure_edge_strengths", "strong_empirical_parent").split(",") if x.strip()}


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def table_ref(name: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in name.split("."))


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
channel_predictions_full = out_table("channel_predictions")
label_predictions_full = out_table("label_predictions")
thresholds_full = out_table("thresholds")
channel_metrics_full = out_table("channel_metrics")
model_metrics_full = out_table("model_metrics")
label_metrics_full = out_table("label_metrics")
baselines_full = out_table("baselines")
model_pairwise_full = out_table("model_pairwise_set_agreement")

# COMMAND ----------
prompt_inputs = spark.table(prompt_inputs_full).where(F.col("run_id") == F.lit(RUN_ID))
requests_map = spark.table(requests_full).where(F.col("run_id") == F.lit(RUN_ID))

allowed_rows = (
    prompt_inputs
    .select(F.explode_outer("allowed_topic_labels").alias("topic_slug"))
    .where(F.col("topic_slug").isNotNull() & (F.length(F.trim("topic_slug")) > 0))
    .dropDuplicates(["topic_slug"])
    .orderBy("topic_slug")
    .collect()
)
ALLOWED_LABELS = [r["topic_slug"] for r in allowed_rows]
ALLOWED_SET = set(ALLOWED_LABELS)
LABEL_NAME_BY_ID = {label: label.replace("_", " ") for label in ALLOWED_LABELS}
LABEL_ID_BY_NAME = {label.lower(): label for label in ALLOWED_LABELS}
LABEL_ID_BY_NAME.update({label.replace("_", " ").lower(): label for label in ALLOWED_LABELS})

if not ALLOWED_LABELS:
    raise RuntimeError(f"No allowed_topic_labels found in {prompt_inputs_full} for run_id={RUN_ID}")

print(f"Allowed labels: {len(ALLOWED_LABELS)}")

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


label_prediction_struct = StructType([
    StructField("label_id", StringType(), True),
    StructField("probability", DoubleType(), True),
    StructField("model_reported_positive", BooleanType(), True),
    StructField("model_reported_uncertain", BooleanType(), True),
])

prediction_json_schema = StructType([
    StructField("predicted_positive_labels", ArrayType(StringType()), True),
    StructField("uncertain_labels", ArrayType(StringType()), True),
    StructField("rationale_short", StringType(), True),
    StructField("label_predictions", ArrayType(label_prediction_struct), True),
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


def normalize_label(raw: Any) -> Optional[str]:
    value = str(raw or "").strip()
    if not value:
        return None
    if "/wiki/" in value:
        value = value.rsplit("/wiki/", 1)[1]
    value = value.replace("%20", "_")
    if value in ALLOWED_SET:
        return value
    value2 = value.replace(" ", "_")
    if value2 in ALLOWED_SET:
        return value2
    return LABEL_ID_BY_NAME.get(value.lower())


def as_label_set(value: Any) -> Set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        if not value.strip():
            return set()
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                value = parsed
            else:
                value = [value]
        except Exception:
            value = [value]
    out = set()
    if isinstance(value, dict):
        value = value.keys()
    if isinstance(value, (list, tuple, set)):
        for item in value:
            if isinstance(item, dict):
                item = item.get("label_id") or item.get("label") or item.get("category_id") or item.get("topic_category")
            norm = normalize_label(item)
            if norm:
                out.add(norm)
    return out


def clamp_probability(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        v = float(value)
        if math.isnan(v):
            return None
        return max(0.0, min(1.0, v))
    except Exception:
        return None


def normalize_multilabel_prediction(text: Optional[str]) -> Dict[str, Any]:
    obj = extract_first_json_object(text)
    if not obj:
        return {
            "predicted_positive_labels": [],
            "uncertain_labels": [],
            "rationale_short": None,
            "label_predictions": [{"label_id": label, "probability": None, "model_reported_positive": False, "model_reported_uncertain": False} for label in ALLOWED_LABELS],
            "prediction_parse_error": "invalid_or_missing_json",
        }

    positives = as_label_set(obj.get("predicted_positive_labels") or obj.get("positive_labels") or obj.get("labels_positive"))
    uncertain = as_label_set(obj.get("uncertain_labels") or obj.get("labels_uncertain"))
    probs: Dict[str, float] = {}

    raw_probs = obj.get("label_probabilities") or obj.get("probabilities") or obj.get("label_scores")
    if isinstance(raw_probs, dict):
        for key, value in raw_probs.items():
            label = normalize_label(key)
            prob = clamp_probability(value)
            if label and prob is not None:
                probs[label] = prob

    raw_label_rows = obj.get("labels") or obj.get("label_predictions") or obj.get("decisions")
    if isinstance(raw_label_rows, list):
        for row in raw_label_rows:
            if not isinstance(row, dict):
                continue
            label = normalize_label(row.get("label_id") or row.get("label") or row.get("category_id") or row.get("topic_category"))
            if not label:
                continue
            prob = clamp_probability(row.get("probability") or row.get("confidence") or row.get("score"))
            if prob is not None:
                probs[label] = prob
            decision = row.get("positive") if "positive" in row else row.get("applicable")
            if isinstance(decision, bool) and decision:
                positives.add(label)
            if isinstance(decision, str) and decision.strip().lower() in {"true", "yes", "1", "positive"}:
                positives.add(label)
            unc = row.get("uncertain") if "uncertain" in row else row.get("ambiguous")
            if isinstance(unc, bool) and unc:
                uncertain.add(label)

    label_predictions = [
        {
            "label_id": label,
            "probability": probs.get(label),
            "model_reported_positive": label in positives,
            "model_reported_uncertain": label in uncertain,
        }
        for label in ALLOWED_LABELS
    ]

    missing_prob_count = sum(1 for label in ALLOWED_LABELS if label not in probs)
    parse_error = None
    if missing_prob_count:
        parse_error = f"missing_probabilities:{missing_prob_count}"

    return {
        "predicted_positive_labels": sorted(positives),
        "uncertain_labels": sorted(uncertain),
        "rationale_short": str(obj.get("rationale_short") or obj.get("rationale") or "")[:500] or None,
        "label_predictions": label_predictions,
        "prediction_parse_error": parse_error,
    }


@F.udf(prediction_json_schema)
def normalize_multilabel_prediction_udf(raw_text: str):
    d = normalize_multilabel_prediction(raw_text)
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

request_ids_for_run = requests_map.select("request_id").dropDuplicates(["request_id"])
raw_results = raw_results.join(request_ids_for_run, on="request_id", how="inner")
write_run_scoped(raw_results, raw_results_full)
print("Wrote raw results to", raw_results_full)

# COMMAND ----------
req_map = requests_map.select("run_id", "request_id", "provider", "model", "model_tier", "channel_id")
ref_map = (
    prompt_inputs
    .select(
        "run_id",
        "channel_id",
        "channel_name",
        "eval_split",
        "topic_category_urls",
        "topic_slugs",
        "topic_category_count",
        "primary_language_label",
        "primary_language_iso639_3",
        "n_videos_in_prompt",
    )
)

raw_results_loaded = spark.table(raw_results_full).where(F.col("run_id") == F.lit(RUN_ID))
parsed_channel_predictions = (
    raw_results_loaded
    .withColumn("pred", normalize_multilabel_prediction_udf(F.col("raw_text")))
    .select("run_id", "request_id", "provider_result_model", "result_status", "input_tokens", "output_tokens", "raw_text", "parse_error", "pred.*", "imported_at")
    .join(req_map, on=["run_id", "request_id"], how="inner")
    .join(ref_map, on=["run_id", "channel_id"], how="left")
    .withColumn("valid_json", F.col("prediction_parse_error").isNull())
    .withColumn("has_reference_array", F.col("topic_slugs").isNotNull())
    .withColumn("evaluated_at", F.current_timestamp())
)
write_run_scoped(parsed_channel_predictions, channel_predictions_full)

label_predictions = (
    parsed_channel_predictions
    .select(
        "run_id",
        "request_id",
        "provider",
        "model",
        "model_tier",
        "channel_id",
        "channel_name",
        "eval_split",
        "topic_slugs",
        "topic_category_count",
        "parse_error",
        "prediction_parse_error",
        "valid_json",
        "input_tokens",
        "output_tokens",
        F.explode("label_predictions").alias("label_pred"),
    )
    .select(
        "run_id",
        "request_id",
        "provider",
        "model",
        "model_tier",
        "channel_id",
        "channel_name",
        "eval_split",
        "topic_slugs",
        "topic_category_count",
        F.col("label_pred.label_id").alias("label_id"),
        F.col("label_pred.probability").alias("predicted_probability"),
        F.col("label_pred.model_reported_positive").alias("model_reported_positive"),
        F.col("label_pred.model_reported_uncertain").alias("model_reported_uncertain"),
        F.array_contains(F.col("topic_slugs"), F.col("label_pred.label_id")).alias("reference_positive"),
        "valid_json",
        "parse_error",
        "prediction_parse_error",
        "input_tokens",
        "output_tokens",
    )
    .withColumn("evaluated_at", F.current_timestamp())
)
write_run_scoped(label_predictions, label_predictions_full)
print("Wrote parsed predictions to", channel_predictions_full, label_predictions_full)

# COMMAND ----------
pred_pdf = (
    spark.table(label_predictions_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .select(
        "provider",
        "model",
        "model_tier",
        "channel_id",
        "eval_split",
        "label_id",
        "predicted_probability",
        "model_reported_positive",
        "reference_positive",
        "valid_json",
    )
    .toPandas()
)

channel_ref_pdf = (
    prompt_inputs
    .select("channel_id", "channel_name", "eval_split", "topic_slugs", "topic_category_count", "n_videos_in_prompt")
    .toPandas()
)

if pred_pdf.empty:
    raise RuntimeError(f"No label predictions found for run_id={RUN_ID}")

pred_pdf["model_label"] = pred_pdf["provider"] + ":" + pred_pdf["model"]
pred_pdf["predicted_probability"] = pd.to_numeric(pred_pdf["predicted_probability"], errors="coerce")
pred_pdf["reference_positive"] = pred_pdf["reference_positive"].fillna(False).astype(bool)
pred_pdf["model_reported_positive"] = pred_pdf["model_reported_positive"].fillna(False).astype(bool)
pred_pdf["valid_json"] = pred_pdf["valid_json"].fillna(False).astype(bool)

labels = ALLOWED_LABELS
label_count = len(labels)
threshold_grid = [0.01, 0.03, 0.05] + [round(x / 100.0, 2) for x in range(10, 96, 5)] + [0.97, 0.99]


def f1_from_counts(tp: int, fp: int, fn: int) -> float:
    denom = 2 * tp + fp + fn
    return (2 * tp / denom) if denom else 0.0


def choose_threshold(df: pd.DataFrame, min_pos: int = 1, min_neg: int = 1) -> Tuple[float, float, int, int, str]:
    y = df["reference_positive"].astype(bool)
    scores = df["predicted_probability"].fillna(-1.0)
    n_pos = int(y.sum())
    n_neg = int((~y).sum())
    if n_pos < min_pos or n_neg < min_neg or scores.max() < 0:
        return 0.50, 0.0, n_pos, n_neg, "fallback_insufficient_calibration_support"
    best_threshold = 0.50
    best_f1 = -1.0
    for threshold in threshold_grid:
        pred = scores >= threshold
        tp = int((pred & y).sum())
        fp = int((pred & ~y).sum())
        fn = int((~pred & y).sum())
        f1 = f1_from_counts(tp, fp, fn)
        if f1 > best_f1 or (f1 == best_f1 and abs(threshold - 0.50) < abs(best_threshold - 0.50)):
            best_threshold = threshold
            best_f1 = f1
    return float(best_threshold), float(best_f1), n_pos, n_neg, "f1_max_calibration"


threshold_records = []
global_threshold_by_model: Dict[str, float] = {}
label_threshold_by_model: Dict[Tuple[str, str], float] = {}

for model_label, model_df in pred_pdf.groupby("model_label"):
    calib = model_df[(model_df["eval_split"] == "calibration") & model_df["valid_json"]].copy()
    threshold, f1, n_pos, n_neg, method = choose_threshold(calib, min_pos=1, min_neg=1)
    global_threshold_by_model[model_label] = threshold
    provider, model = model_label.split(":", 1)
    threshold_records.append({
        "run_id": RUN_ID,
        "provider": provider,
        "model": model,
        "label_id": "__GLOBAL_MICRO__",
        "threshold": threshold,
        "calibration_f1": f1,
        "n_calibration_positive": n_pos,
        "n_calibration_negative": n_neg,
        "threshold_method": method,
    })
    for label_id, label_df in calib.groupby("label_id"):
        t, lf1, lp, ln, lmethod = choose_threshold(
            label_df,
            min_pos=MIN_LABEL_THRESHOLD_CALIBRATION_POSITIVES,
            min_neg=MIN_LABEL_THRESHOLD_CALIBRATION_NEGATIVES,
        )
        if lmethod.startswith("fallback"):
            t = threshold
            lmethod = "fallback_to_model_global_threshold"
        label_threshold_by_model[(model_label, label_id)] = t
        threshold_records.append({
            "run_id": RUN_ID,
            "provider": provider,
            "model": model,
            "label_id": label_id,
            "threshold": t,
            "calibration_f1": lf1,
            "n_calibration_positive": lp,
            "n_calibration_negative": ln,
            "threshold_method": lmethod,
        })

thresholds_df = spark.createDataFrame(pd.DataFrame(threshold_records))
write_run_scoped(thresholds_df, thresholds_full)

# COMMAND ----------
closure_edges: Dict[str, Set[str]] = defaultdict(set)
if ENABLE_CLOSURE_POSTPROCESS and CLOSURE_EDGES_TABLE and _table_exists_full(table_ref(CLOSURE_EDGES_TABLE)):
    edge_rows = (
        spark.table(table_ref(CLOSURE_EDGES_TABLE))
        .where(F.col("edge_strength").isin(*sorted(CLOSURE_EDGE_STRENGTHS)))
        .select("child_label", "parent_label")
        .collect()
    )
    for row in edge_rows:
        child = row["child_label"]
        parent = row["parent_label"]
        if child in ALLOWED_SET and parent in ALLOWED_SET and child != parent:
            closure_edges[child].add(parent)
    print(f"Loaded closure postprocess edges: {sum(len(v) for v in closure_edges.values())}")
else:
    print("Closure postprocess disabled or edge table unavailable.")


def apply_closure(label_set: Set[str]) -> Set[str]:
    expanded = set(label_set)
    changed = True
    while changed:
        changed = False
        for label in list(expanded):
            for parent in closure_edges.get(label, set()):
                if parent not in expanded:
                    expanded.add(parent)
                    changed = True
    return expanded


def label_sets_from_prediction_rows(model_df: pd.DataFrame, variant: str) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for channel_id, group in model_df.groupby("channel_id"):
        if variant == "model_reported":
            pred_set = set(group.loc[group["model_reported_positive"], "label_id"].tolist())
        elif variant == "prob_global_threshold":
            threshold = global_threshold_by_model.get(group["model_label"].iloc[0], 0.50)
            pred_set = set(group.loc[group["predicted_probability"].fillna(-1.0) >= threshold, "label_id"].tolist())
        elif variant == "prob_label_threshold":
            model_label = group["model_label"].iloc[0]
            pred_set = set()
            for _, row in group.iterrows():
                t = label_threshold_by_model.get((model_label, row["label_id"]), global_threshold_by_model.get(model_label, 0.50))
                prob = row["predicted_probability"]
                if pd.notna(prob) and prob >= t:
                    pred_set.add(row["label_id"])
        elif variant == "prob_label_threshold_closure_postprocessed":
            model_label = group["model_label"].iloc[0]
            pred_set = set()
            for _, row in group.iterrows():
                t = label_threshold_by_model.get((model_label, row["label_id"]), global_threshold_by_model.get(model_label, 0.50))
                prob = row["predicted_probability"]
                if pd.notna(prob) and prob >= t:
                    pred_set.add(row["label_id"])
            pred_set = apply_closure(pred_set)
        else:
            raise ValueError(variant)
        out[str(channel_id)] = pred_set
    return out


def normalize_topic_list(value: Any) -> Set[str]:
    if value is None:
        return set()
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, str):
        if not value:
            return set()
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                value = parsed
            else:
                value = [value]
        except Exception:
            value = [value]
    out = set()
    if isinstance(value, (list, tuple, set)):
        for item in value:
            if item is not None:
                out.add(str(item))
    return out


reference_by_channel = {
    str(row["channel_id"]): normalize_topic_list(row["topic_slugs"])
    for _, row in channel_ref_pdf.iterrows()
}
eval_split_by_channel = {
    str(row["channel_id"]): row["eval_split"]
    for _, row in channel_ref_pdf.iterrows()
}


def compute_metrics(pred_sets: Dict[str, Set[str]], channel_ids: Sequence[str], label_ids: Sequence[str]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    label_ids = list(label_ids)
    tp_by_label = {label: 0 for label in label_ids}
    fp_by_label = {label: 0 for label in label_ids}
    fn_by_label = {label: 0 for label in label_ids}
    channel_records = []
    total_tp = total_fp = total_fn = 0
    exact_matches = 0
    jaccards = []
    card_errors = []
    abs_card_errors = []
    for channel_id in channel_ids:
        pred = set(pred_sets.get(str(channel_id), set()))
        ref = set(reference_by_channel.get(str(channel_id), set()))
        tp_set = pred & ref
        fp_set = pred - ref
        fn_set = ref - pred
        for label in tp_set:
            if label in tp_by_label:
                tp_by_label[label] += 1
        for label in fp_set:
            if label in fp_by_label:
                fp_by_label[label] += 1
        for label in fn_set:
            if label in fn_by_label:
                fn_by_label[label] += 1
        tp = len(tp_set)
        fp = len(fp_set)
        fn = len(fn_set)
        total_tp += tp
        total_fp += fp
        total_fn += fn
        exact = pred == ref
        exact_matches += int(exact)
        union_n = len(pred | ref)
        jaccard = (tp / union_n) if union_n else 1.0
        card_error = len(pred) - len(ref)
        jaccards.append(jaccard)
        card_errors.append(card_error)
        abs_card_errors.append(abs(card_error))
        channel_records.append({
            "channel_id": channel_id,
            "n_predicted_labels": len(pred),
            "n_reference_labels": len(ref),
            "n_true_positive_labels": tp,
            "n_false_positive_labels": fp,
            "n_false_negative_labels": fn,
            "exact_set_match": bool(exact),
            "jaccard_similarity": float(jaccard),
            "label_cardinality_error": int(card_error),
            "predicted_labels_json": json.dumps(sorted(pred), ensure_ascii=False),
            "reference_labels_json": json.dumps(sorted(ref), ensure_ascii=False),
            "false_positive_labels_json": json.dumps(sorted(fp_set), ensure_ascii=False),
            "false_negative_labels_json": json.dumps(sorted(fn_set), ensure_ascii=False),
        })
    n_channels = len(channel_ids)
    micro_precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) else 0.0
    micro_recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) else 0.0
    micro_f1 = f1_from_counts(total_tp, total_fp, total_fn)
    hamming_loss = (total_fp + total_fn) / (n_channels * len(label_ids)) if n_channels and label_ids else None
    label_records = []
    f1_values = []
    f1_present_values = []
    for label in label_ids:
        tp = tp_by_label[label]
        fp = fp_by_label[label]
        fn = fn_by_label[label]
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = f1_from_counts(tp, fp, fn)
        support = tp + fn
        predicted = tp + fp
        f1_values.append(f1)
        if support > 0:
            f1_present_values.append(f1)
        label_records.append({
            "label_id": label,
            "label_name": LABEL_NAME_BY_ID.get(label, label),
            "support": support,
            "predicted_positive_count": predicted,
            "true_positive_count": tp,
            "false_positive_count": fp,
            "false_negative_count": fn,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        })
    aggregate = {
        "n_channels": n_channels,
        "n_labels": len(label_ids),
        "micro_precision": micro_precision,
        "micro_recall": micro_recall,
        "micro_f1": micro_f1,
        "macro_f1_all_labels": sum(f1_values) / len(f1_values) if f1_values else 0.0,
        "macro_f1_present_labels": sum(f1_present_values) / len(f1_present_values) if f1_present_values else 0.0,
        "exact_set_match_rate": exact_matches / n_channels if n_channels else 0.0,
        "mean_jaccard_similarity": sum(jaccards) / len(jaccards) if jaccards else 0.0,
        "hamming_loss": hamming_loss,
        "mean_label_cardinality_error": sum(card_errors) / len(card_errors) if card_errors else 0.0,
        "mean_abs_label_cardinality_error": sum(abs_card_errors) / len(abs_card_errors) if abs_card_errors else 0.0,
        "total_true_positive_labels": total_tp,
        "total_false_positive_labels": total_fp,
        "total_false_negative_labels": total_fn,
    }
    return aggregate, label_records, channel_records

# COMMAND ----------
all_channel_ids = sorted(reference_by_channel.keys())
heldout_channel_ids = sorted([cid for cid, split in eval_split_by_channel.items() if split == "heldout_test"])
calibration_channel_ids = sorted([cid for cid, split in eval_split_by_channel.items() if split == "calibration"])

metric_records = []
label_metric_records = []
channel_metric_records = []

variants = ["model_reported", "prob_global_threshold", "prob_label_threshold"]
if closure_edges:
    variants.append("prob_label_threshold_closure_postprocessed")

for model_label, model_df in pred_pdf.groupby("model_label"):
    provider, model = model_label.split(":", 1)
    model_df = model_df[model_df["valid_json"]].copy()
    for variant in variants:
        pred_sets = label_sets_from_prediction_rows(model_df, variant)
        for split_name, channel_ids in [("heldout_test", heldout_channel_ids), ("calibration", calibration_channel_ids), ("all_sample", all_channel_ids)]:
            aggregate, label_records, channel_records = compute_metrics(pred_sets, channel_ids, labels)
            metric_records.append({
                "run_id": RUN_ID,
                "provider": provider,
                "model": model,
                "prediction_variant": variant,
                "eval_split": split_name,
                **aggregate,
            })
            for rec in label_records:
                label_metric_records.append({
                    "run_id": RUN_ID,
                    "provider": provider,
                    "model": model,
                    "prediction_variant": variant,
                    "eval_split": split_name,
                    **rec,
                })
            for rec in channel_records:
                channel_metric_records.append({
                    "run_id": RUN_ID,
                    "provider": provider,
                    "model": model,
                    "prediction_variant": variant,
                    "eval_split": split_name,
                    **rec,
                })

# COMMAND ----------
calib_refs = [reference_by_channel[cid] for cid in calibration_channel_ids]
label_prevalence = {
    label: sum(1 for labels_for_channel in calib_refs if label in labels_for_channel) / len(calib_refs)
    for label in labels
} if calibration_channel_ids else {label: 0.0 for label in labels}
avg_cardinality = sum(len(s) for s in calib_refs) / len(calib_refs) if calib_refs else 0.0
top_k = max(0, int(round(avg_cardinality)))
top_k_labels = set(sorted(labels, key=lambda label: (-label_prevalence[label], label))[:top_k])
prevalence_05_labels = {label for label, prev in label_prevalence.items() if prev >= 0.05}

baseline_defs = {
    "baseline_empty_set": {cid: set() for cid in all_channel_ids},
    "baseline_topk_avg_cardinality": {cid: set(top_k_labels) for cid in all_channel_ids},
    "baseline_prevalence_ge_05": {cid: set(prevalence_05_labels) for cid in all_channel_ids},
}

baseline_records = []
for baseline_name, pred_sets in baseline_defs.items():
    for split_name, channel_ids in [("heldout_test", heldout_channel_ids), ("calibration", calibration_channel_ids), ("all_sample", all_channel_ids)]:
        aggregate, label_records, channel_records = compute_metrics(pred_sets, channel_ids, labels)
        baseline_records.append({
            "run_id": RUN_ID,
            "baseline_name": baseline_name,
            "eval_split": split_name,
            "baseline_labels_json": json.dumps(sorted(next(iter(pred_sets.values()))) if pred_sets else [], ensure_ascii=False),
            **aggregate,
        })

baselines_df = spark.createDataFrame(pd.DataFrame(baseline_records))
write_run_scoped(baselines_df, baselines_full)

# COMMAND ----------
model_metrics_df = spark.createDataFrame(pd.DataFrame(metric_records))
label_metrics_df = spark.createDataFrame(pd.DataFrame(label_metric_records))
channel_metrics_df = spark.createDataFrame(pd.DataFrame(channel_metric_records))

write_run_scoped(model_metrics_df, model_metrics_full)
write_run_scoped(label_metrics_df, label_metrics_full)
write_run_scoped(channel_metrics_df, channel_metrics_full)

# COMMAND ----------
pair_records = []
for variant in ["model_reported", "prob_label_threshold"]:
    model_sets = {}
    for model_label, model_df in pred_pdf.groupby("model_label"):
        model_sets[model_label] = label_sets_from_prediction_rows(model_df[model_df["valid_json"]].copy(), variant)
    model_labels = sorted(model_sets)
    for model_a in model_labels:
        for model_b in model_labels:
            jaccards = []
            exact_same = 0
            n = 0
            for channel_id in heldout_channel_ids:
                a = model_sets[model_a].get(channel_id, set())
                b = model_sets[model_b].get(channel_id, set())
                union = a | b
                j = len(a & b) / len(union) if union else 1.0
                jaccards.append(j)
                exact_same += int(a == b)
                n += 1
            pair_records.append({
                "run_id": RUN_ID,
                "prediction_variant": variant,
                "model_a": model_a,
                "model_b": model_b,
                "eval_split": "heldout_test",
                "n_channels": n,
                "mean_jaccard_between_model_sets": sum(jaccards) / len(jaccards) if jaccards else None,
                "exact_same_predicted_set_rate": exact_same / n if n else None,
            })

pair_df = spark.createDataFrame(pd.DataFrame(pair_records))
write_run_scoped(pair_df, model_pairwise_full)

# COMMAND ----------
headline = (
    spark.table(model_metrics_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("eval_split") == F.lit("heldout_test"))
    .where(F.col("prediction_variant").isin("model_reported", "prob_global_threshold", "prob_label_threshold"))
    .orderBy(F.desc("micro_f1"), F.desc("mean_jaccard_similarity"), "provider", "model", "prediction_variant")
)
display(headline)

payload = {
    "run_id": RUN_ID,
    "raw_results_table": raw_results_full,
    "channel_predictions_table": channel_predictions_full,
    "label_predictions_table": label_predictions_full,
    "thresholds_table": thresholds_full,
    "model_metrics_table": model_metrics_full,
    "label_metrics_table": label_metrics_full,
    "channel_metrics_table": channel_metrics_full,
    "baselines_table": baselines_full,
    "model_pairwise_table": model_pairwise_full,
    "allowed_label_count": len(ALLOWED_LABELS),
    "n_heldout_channels": len(heldout_channel_ids),
    "n_calibration_channels": len(calibration_channel_ids),
    "headline_metrics": [
        row.asDict(recursive=True)
        for row in headline.select(
            "provider",
            "model",
            "prediction_variant",
            "n_channels",
            "micro_precision",
            "micro_recall",
            "micro_f1",
            "macro_f1_present_labels",
            "exact_set_match_rate",
            "mean_jaccard_similarity",
            "hamming_loss",
            "mean_label_cardinality_error",
        ).collect()
    ],
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True, default=str))
