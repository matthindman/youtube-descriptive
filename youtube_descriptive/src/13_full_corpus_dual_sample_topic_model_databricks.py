# Databricks notebook source
# ruff: noqa: F821
# MAGIC %run ./full_corpus_dual_sample_design

# COMMAND ----------
"""Model-completed family/leaf probabilities for platform-topic-missing channels."""

from __future__ import annotations

import concurrent.futures
import json
import threading
import time
from datetime import datetime, timezone

import requests
import yaml
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


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


_widget("stage", "prepare")
_widget(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
_widget(
    "hierarchy_config_path",
    "dbfs:/FileStore/youtube_descriptive/youtube_topic_hierarchy_v2.yaml",
)
STAGE = _get("stage", "prepare")
CONFIG_PATH = _get(
    "design_config_path",
    "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
)
HIERARCHY_PATH = _get(
    "hierarchy_config_path",
    "dbfs:/FileStore/youtube_descriptive/youtube_topic_hierarchy_v2.yaml",
)
CONFIG = json.loads(dbutils.fs.head(CONFIG_PATH, 1024 * 1024))
validate_design_config(CONFIG)
TOPIC_MODEL = CONFIG["topic_model"]
DESIGN_VERSION = CONFIG["design_version"]
PREFIX = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_prefix']}"
RUN_ID = TOPIC_MODEL["run_id"]
UNMAPPED_FAMILY = "Other / Unmapped YouTube topic"
UNMAPPED_LEAF = "Other / Unmapped"

TABLES = {
    "analysis_union": f"{PREFIX}_analysis_union",
    "language": f"{PREFIX}_channel_language_current",
    "missing_queue": f"{PREFIX}_model_topic_queue",
    "missing_videos": f"{PREFIX}_model_topic_source_videos",
    "requests": f"{PREFIX}_topic_model_requests",
    "raw_results": f"{PREFIX}_topic_model_raw_results",
    "predictions": f"{PREFIX}_topic_model_predictions",
    "validation": f"{PREFIX}_topic_model_validation",
    "summary": f"{PREFIX}_topic_model_summary",
}


def require_table(table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required table does not exist: {table_name}")


def write_table(frame: DataFrame, table_name: str, comment: str, mode: str = "overwrite") -> None:
    writer = frame.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(table_name)
    escaped = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{escaped}'")
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
        f"'design.version'='{DESIGN_VERSION}', "
        f"'topic_model.run_id'='{RUN_ID}', "
        "'probabilities.calibrated'='false')"
    )


def taxonomy() -> tuple[dict[str, list[str]], str]:
    hierarchy = yaml.safe_load(dbutils.fs.head(HIERARCHY_PATH, 1024 * 1024)) or {}
    result: dict[str, list[str]] = {}
    for family, specification in (hierarchy.get("families") or {}).items():
        if family == "Other":
            continue
        leaves = sorted(set(str(value) for value in (specification.get("children") or {}).values()))
        leaves.append(f"[{family}] - unspecified")
        result[str(family)] = leaves
    result[UNMAPPED_FAMILY] = [UNMAPPED_LEAF]
    rendered = "\n".join(f"- {family}: {', '.join(leaves)}" for family, leaves in result.items())
    return result, rendered


TAXONOMY, TAXONOMY_TEXT = taxonomy()
ALLOWED_FAMILIES = sorted(TAXONOMY)


def deterministic_hash(channel_col: F.Column, seed: str) -> F.Column:
    return F.sha2(
        F.concat_ws("\x1f", channel_col.cast("string"), F.lit(CONFIG["frame_version"]), F.lit(seed)),
        256,
    )


def build_prompt(channel_name: str, channel_description: str, channel_language: str, videos: list[dict]) -> str:
    video_lines = []
    for item in (videos or [])[:10]:
        if hasattr(item, "asDict"):
            item = item.asDict(recursive=True)
        title = (item.get("video_title") or "")[:300]
        description = (item.get("video_description") or "")[:500]
        video_lines.append(f"- {title}\n  {description}")
    evidence = "\n".join(video_lines) if video_lines else "(no recent-video text)"
    return f"""Classify this YouTube channel using only the supplied evidence.

Return JSON only with this schema:
{{
  "status": "classified" | "insufficient_evidence",
  "family_probabilities": [{{"family": "...", "probability": 0.0}}],
  "leaf_probabilities": [{{"family": "...", "leaf": "...", "probability": 0.0}}],
  "evidence": "brief explanation"
}}

Rules:
- If status is classified, family probabilities must be nonnegative and sum to 1.
- Leaf probabilities within each family must sum to that family's probability.
- Use only the exact families and leaves below.
- Use the unspecified leaf when the family is supported but no specific leaf is.
- If there is too little content evidence, return insufficient_evidence with empty probability arrays.
- Do not infer topic from language, country, subscriber count, or popularity.

Taxonomy:
{TAXONOMY_TEXT}

Channel name: {channel_name or ''}
Detected channel language: {channel_language or 'und'}
Channel description:
{(channel_description or '')[:2000]}

Recent videos:
{evidence}
"""


def prepare() -> dict[str, int]:
    for name in ("analysis_union", "language", "missing_queue"):
        require_table(TABLES[name])
    analysis = spark.table(TABLES["analysis_union"])
    language = spark.table(TABLES["language"]).select("channel_id", "channel_language")
    missing = spark.table(TABLES["missing_queue"]).select(
        "channel_id", "channel_name", "channel_description", "selection_route", "pi_union", "base_weight_union"
    ).withColumn("request_domain", F.lit("platform_topic_missing"))

    validation = (
        analysis.where(F.col("has_nonempty_topic_categories"))
        .withColumn(
            "_validation_hash",
            deterministic_hash(F.col("channel_id"), TOPIC_MODEL["validation_seed"]),
        )
        .orderBy(F.col("_validation_hash").asc(), F.col("channel_id").asc())
        .limit(int(TOPIC_MODEL["validation_sample_n"]))
        .select(
            "channel_id",
            "channel_name",
            "channel_description",
            "selection_route",
            "pi_union",
            "base_weight_union",
            F.lit("platform_topic_validation").alias("request_domain"),
        )
    )
    request_ids = missing.unionByName(validation).dropDuplicates(["channel_id"])
    raw_source_videos = spark.table(CONFIG["source_tables"]["channel_videos"])
    source_channel_id = "channel_id" if "channel_id" in raw_source_videos.columns else "canonical_id"
    source_videos = raw_source_videos.select(
        F.col(source_channel_id).cast("string").alias("channel_id"),
        F.col("video_title").cast("string"),
        F.col("video_description").cast("string"),
        F.col("position").cast("int"),
        F.col("published_at").cast("timestamp"),
    ).join(request_ids.select("channel_id"), "channel_id", "inner")
    ranked = source_videos.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("channel_id").orderBy(
                F.col("position").asc_nulls_last(), F.col("published_at").desc_nulls_last(), F.col("video_title").asc()
            )
        ),
    ).where(F.col("_rn") <= 10)
    videos = ranked.groupBy("channel_id").agg(
        F.sort_array(
            F.collect_list(
                F.struct("_rn", "video_title", "video_description", "published_at")
            )
        ).alias("videos")
    )
    prompt_schema = T.StringType()
    prompt_udf = F.udf(build_prompt, prompt_schema)
    prepared = (
        request_ids.join(language, "channel_id", "left")
        .join(videos, "channel_id", "left")
        .withColumn(
            "request_id",
            F.concat(
                F.lit("ytp_"),
                F.substring(
                    F.sha2(F.concat_ws("\x1f", F.lit(RUN_ID), F.col("channel_id")), 256),
                    1,
                    61,
                ),
            ),
        )
        .withColumn(
            "prompt",
            prompt_udf("channel_name", "channel_description", "channel_language", "videos"),
        )
        .withColumn("run_id", F.lit(RUN_ID))
        .withColumn("model", F.lit(TOPIC_MODEL["model"]))
        .withColumn("prompt_version", F.lit(TOPIC_MODEL["prompt_version"]))
        .withColumn("created_at", F.current_timestamp())
        .drop("videos")
    )
    write_table(prepared, TABLES["requests"], "Idempotent DeepSeek requests for topic robustness and probability validation.")
    counts = {
        "requests": prepared.count(),
        "distinct_channels": prepared.select("channel_id").distinct().count(),
        "platform_topic_missing": prepared.where(F.col("request_domain") == "platform_topic_missing").count(),
        "platform_topic_validation": prepared.where(F.col("request_domain") == "platform_topic_validation").count(),
    }
    if counts["requests"] != counts["distinct_channels"]:
        raise AssertionError(f"Topic request IDs are not one row per channel: {counts}")
    print("TOPIC REQUEST PREPARATION: PASS")
    print(json.dumps(counts, sort_keys=True))
    return counts


def classify() -> dict[str, int]:
    require_table(TABLES["requests"])
    request_frame = spark.table(TABLES["requests"]).where(F.col("run_id") == F.lit(RUN_ID))
    if spark.catalog.tableExists(TABLES["raw_results"]):
        completed = spark.table(TABLES["raw_results"]).where(
            (F.col("run_id") == F.lit(RUN_ID)) & (F.col("http_status").between(200, 299))
        ).select("request_id").distinct()
        pending = request_frame.join(completed, "request_id", "left_anti")
    else:
        pending = request_frame

    api_key = dbutils.secrets.get(
        scope=TOPIC_MODEL["secret_scope"], key=TOPIC_MODEL["deepseek_secret_key"]
    )
    thread_state = threading.local()

    def session() -> requests.Session:
        value = getattr(thread_state, "session", None)
        if value is None:
            value = requests.Session()
            thread_state.session = value
        return value

    def call(row) -> tuple:
        started = time.perf_counter()
        last_error = None
        for attempt in range(int(TOPIC_MODEL["max_retries"]) + 1):
            try:
                response = session().post(
                    "https://api.deepseek.com/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={
                        "model": TOPIC_MODEL["model"],
                        "messages": [{"role": "user", "content": row["prompt"]}],
                        "max_tokens": 1400,
                        "response_format": {"type": "json_object"},
                        "thinking": {"type": "disabled"},
                    },
                    timeout=float(TOPIC_MODEL["request_timeout_seconds"]),
                )
                body = response.text
                return (
                    RUN_ID,
                    row["request_id"],
                    row["channel_id"],
                    row["request_domain"],
                    int(response.status_code),
                    body,
                    None,
                    attempt + 1,
                    float((time.perf_counter() - started) * 1000),
                    datetime.now(timezone.utc),
                )
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < int(TOPIC_MODEL["max_retries"]):
                    time.sleep(min(8.0, 2.0**attempt))
        return (
            RUN_ID,
            row["request_id"],
            row["channel_id"],
            row["request_domain"],
            None,
            None,
            last_error,
            int(TOPIC_MODEL["max_retries"]) + 1,
            float((time.perf_counter() - started) * 1000),
            datetime.now(timezone.utc),
        )

    schema = (
        "run_id string, request_id string, channel_id string, request_domain string, http_status int, "
        "response_body string, error string, attempts int, duration_ms double, completed_at timestamp"
    )
    chunk_size = int(TOPIC_MODEL["chunk_size"])
    max_workers = int(TOPIC_MODEL["max_workers"])
    chunk = []
    submitted = 0
    succeeded = 0
    failed = 0

    def flush(rows: list) -> None:
        nonlocal submitted, succeeded, failed
        if not rows:
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(call, rows))
        submitted += len(results)
        succeeded += sum(1 for result in results if result[4] is not None and 200 <= result[4] < 300)
        failed += len(results) - sum(1 for result in results if result[4] is not None and 200 <= result[4] < 300)
        write_table(
            spark.createDataFrame(results, schema),
            TABLES["raw_results"],
            "Raw idempotent DeepSeek topic-probability responses.",
            mode="append" if spark.catalog.tableExists(TABLES["raw_results"]) else "overwrite",
        )
        print(f"TOPIC DEEPSEEK PROGRESS: submitted={submitted:,} succeeded={succeeded:,} failed={failed:,}")

    for row in pending.select("request_id", "channel_id", "request_domain", "prompt").toLocalIterator():
        chunk.append(row)
        if len(chunk) >= chunk_size:
            flush(chunk)
            chunk = []
    flush(chunk)
    result = {"submitted": submitted, "succeeded": succeeded, "failed": failed}
    print("TOPIC DEEPSEEK RUN:", json.dumps(result, sort_keys=True))
    return result


prediction_schema = T.StructType(
    [
        T.StructField("status", T.StringType(), True),
        T.StructField("family_probabilities_json", T.StringType(), True),
        T.StructField("leaf_probabilities_json", T.StringType(), True),
        T.StructField("evidence", T.StringType(), True),
        T.StructField("parse_error", T.StringType(), True),
        T.StructField("family_sum", T.DoubleType(), True),
        T.StructField("max_leaf_family_error", T.DoubleType(), True),
    ]
)


def parse_response(raw: str):
    try:
        outer = json.loads(raw)
        content = outer["choices"][0]["message"]["content"]
        content = str(content).strip()
        if content.startswith("```"):
            content = content.removeprefix("```json").removeprefix("```")
            content = content.removesuffix("```").strip()
        parsed = json.loads(content)
        status = str(parsed.get("status") or "").strip().lower()
        families = parsed.get("family_probabilities") or []
        leaves = parsed.get("leaf_probabilities") or []
        if status not in {"classified", "insufficient_evidence"}:
            raise ValueError(f"invalid status {status!r}")
        if status == "insufficient_evidence":
            return status, "[]", "[]", str(parsed.get("evidence") or ""), None, 0.0, 0.0
        family_values: dict[str, float] = {}
        for item in families:
            family = str(item["family"])
            probability = float(item["probability"])
            if family not in ALLOWED_FAMILIES or not 0.0 <= probability <= 1.0:
                raise ValueError(f"invalid family probability {item!r}")
            family_values[family] = family_values.get(family, 0.0) + probability
        family_sum = sum(family_values.values())
        if abs(family_sum - 1.0) > 0.02:
            raise ValueError(f"family probability sum {family_sum}")
        leaf_sums = {family: 0.0 for family in family_values}
        for item in leaves:
            family = str(item["family"])
            leaf = str(item["leaf"])
            probability = float(item["probability"])
            if family not in TAXONOMY or leaf not in TAXONOMY[family] or not 0.0 <= probability <= 1.0:
                raise ValueError(f"invalid leaf probability {item!r}")
            leaf_sums[family] = leaf_sums.get(family, 0.0) + probability
        max_error = max(abs(leaf_sums.get(family, 0.0) - probability) for family, probability in family_values.items())
        if max_error > 0.02:
            raise ValueError(f"leaf-to-family probability error {max_error}")
        return (
            status,
            json.dumps(families, sort_keys=True),
            json.dumps(leaves, sort_keys=True),
            str(parsed.get("evidence") or ""),
            None,
            family_sum,
            max_error,
        )
    except Exception as exc:
        return None, None, None, None, f"{type(exc).__name__}: {exc}", None, None


def parse() -> dict[str, int]:
    for name in ("requests", "raw_results"):
        require_table(TABLES[name])
    raw = spark.table(TABLES["raw_results"]).where(F.col("run_id") == F.lit(RUN_ID))
    latest = raw.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("request_id").orderBy(
                F.col("completed_at").desc_nulls_last(), F.col("http_status").desc_nulls_last()
            )
        ),
    ).where(F.col("_rn") == 1)
    parser = F.udf(parse_response, prediction_schema)
    predictions = (
        latest.withColumn("_parsed", parser("response_body"))
        .select("run_id", "request_id", "channel_id", "request_domain", "http_status", "error", "completed_at", "_parsed.*")
        .withColumn("model", F.lit(TOPIC_MODEL["model"]))
        .withColumn("prompt_version", F.lit(TOPIC_MODEL["prompt_version"]))
        .withColumn("is_calibrated", F.lit(False))
    )
    write_table(
        predictions,
        TABLES["predictions"],
        "Parsed family/leaf probabilities. Raw predictions are explicitly uncalibrated pending validation.",
    )
    validation = predictions.where(F.col("request_domain") == "platform_topic_validation").join(
        spark.table(TABLES["analysis_union"]).select("channel_id", "raw_topic_categories"),
        "channel_id",
        "left",
    )
    write_table(
        validation,
        TABLES["validation"],
        "Probability validation sample retaining held-out platform topic arrays; calibration must precede use.",
    )
    counts = predictions.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("channel_id").alias("distinct_channels"),
        F.sum((F.col("status") == "classified").cast("long")).alias("classified"),
        F.sum((F.col("status") == "insufficient_evidence").cast("long")).alias("insufficient_evidence"),
        F.sum(F.col("parse_error").isNotNull().cast("long")).alias("parse_errors"),
        F.sum((~F.col("http_status").between(200, 299)).cast("long")).alias("http_failures"),
    ).first().asDict()
    rows = [
        (DESIGN_VERSION, key, int(value), datetime.now(timezone.utc)) for key, value in counts.items()
    ]
    write_table(
        spark.createDataFrame(rows, "design_version string, metric string, value long, recorded_at timestamp"),
        TABLES["summary"],
        "Topic model completion and parse QA.",
    )
    if counts["rows"] != counts["distinct_channels"]:
        raise AssertionError(f"Topic predictions are not one row per channel: {counts}")
    if counts["parse_errors"] or counts["http_failures"]:
        raise RuntimeError(f"Topic model output requires repair before calibration: {counts}")
    print("TOPIC MODEL PARSE: PASS")
    print("PROBABILITIES CALIBRATED: FALSE")
    print(json.dumps(counts, sort_keys=True))
    return {key: int(value) for key, value in counts.items()}


STAGES = {"prepare": prepare, "classify": classify, "parse": parse}
if STAGE not in STAGES:
    raise ValueError(f"Unknown topic-model stage {STAGE!r}; expected one of {sorted(STAGES)}")
print(f"RUNNING TOPIC MODEL STAGE: {STAGE}")
RESULT = STAGES[STAGE]()
dbutils.notebook.exit(json.dumps({"stage": STAGE, "result": RESULT}, sort_keys=True, default=str))
