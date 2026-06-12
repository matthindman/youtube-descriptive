# Databricks notebook source
import json
import os
import re
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


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
_create_text_widget("batch_files_table", "yt_lid_v3_too_full_20260609_llm_validation_requests_batch_files")
_create_text_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
_create_text_widget("status_snapshot_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results")
_create_text_widget("openai_records_json", "[]")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
BATCH_FILES_TABLE = _get_widget("batch_files_table", "yt_lid_v3_too_full_20260609_llm_validation_requests_batch_files")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
STATUS_SNAPSHOT_TABLE = _get_widget("status_snapshot_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results").rstrip("/")
OPENAI_RECORDS_JSON = _get_widget("openai_records_json", "[]")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", model or "model")


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("local_jsonl_path", StringType(), True),
    StructField("n_requests", IntegerType(), True),
    StructField("n_bytes", IntegerType(), True),
    StructField("provider_file_id", StringType(), True),
    StructField("provider_batch_id", StringType(), True),
    StructField("provider_status", StringType(), True),
    StructField("submission_status", StringType(), True),
    StructField("submitted_at_utc", StringType(), True),
    StructField("recorded_at_utc", StringType(), True),
    StructField("submission_error", StringType(), True),
])

batch_files = spark.table(fqtn(BATCH_FILES_TABLE)).where(F.col("run_id") == F.lit(RUN_ID))
current_jobs = spark.table(fqtn(BATCH_JOBS_TABLE)).where(F.col("run_id") == F.lit(RUN_ID))
current_openai = current_jobs.where(F.col("provider") == F.lit("openai")).select(*[f.name for f in schema.fields])
manual_openai_records = []
try:
    for item in json.loads(OPENAI_RECORDS_JSON or "[]"):
        manual_openai_records.append((
            RUN_ID,
            "openai",
            item["model"],
            int(item.get("chunk_id", 0)),
            item.get("local_jsonl_path"),
            int(item.get("n_requests", 1000)),
            int(item.get("n_bytes", 0)),
            item.get("provider_file_id"),
            item["provider_batch_id"],
            item.get("provider_status", "validating"),
            item.get("submission_status", "submitted"),
            item.get("submitted_at_utc", datetime.utcnow().isoformat()),
            item.get("recorded_at_utc", datetime.utcnow().isoformat()),
            item.get("submission_error"),
        ))
except Exception as exc:
    raise ValueError(f"Invalid openai_records_json: {exc}") from exc
manual_openai = spark.createDataFrame(manual_openai_records, schema) if manual_openai_records else spark.createDataFrame([], schema)
if current_openai.limit(1).count() == 0 and manual_openai_records:
    current_openai = manual_openai

status = spark.table(fqtn(STATUS_SNAPSHOT_TABLE)).where(F.col("run_id") == F.lit(RUN_ID))
status_cols = set(status.columns)
if "checked_at_utc" in status_cols:
    status_order_col = F.col("checked_at_utc")
elif "recorded_at_utc" in status_cols:
    status_order_col = F.col("recorded_at_utc")
else:
    status_order_col = F.current_timestamp()
status_latest = (
    status.where(F.col("provider").isin("anthropic", "gemini"))
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("provider", "model", "chunk_id").orderBy(F.desc(F.coalesce(status_order_col, F.current_timestamp())))
        ),
    )
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

now = datetime.utcnow().isoformat()
restored_batch = (
    batch_files.where(F.col("provider").isin("anthropic", "gemini"))
    .join(
        status_latest.select("provider", "model", "chunk_id", "provider_batch_id", "provider_status"),
        on=["provider", "model", "chunk_id"],
        how="inner",
    )
    .select(
        F.lit(RUN_ID).alias("run_id"),
        "provider",
        "model",
        F.col("chunk_id").cast("int").alias("chunk_id"),
        "local_jsonl_path",
        F.col("n_requests").cast("int").alias("n_requests"),
        F.col("n_bytes").cast("int").alias("n_bytes"),
        F.lit(None).cast("string").alias("provider_file_id"),
        "provider_batch_id",
        "provider_status",
        F.lit("submitted").alias("submission_status"),
        F.lit(now).alias("submitted_at_utc"),
        F.lit(now).alias("recorded_at_utc"),
        F.lit(None).cast("string").alias("submission_error"),
    )
)

deepseek_records = []
for row in batch_files.where(F.col("provider") == F.lit("deepseek")).collect():
    model = row["model"]
    chunk_id = int(row["chunk_id"])
    result_path = os.path.join(
        RESULTS_INPUT_DIR,
        RUN_ID,
        "deepseek",
        safe_model_dir(model),
        f"chunk_{chunk_id:05d}_results.jsonl",
    )
    deepseek_records.append((
        RUN_ID,
        "deepseek",
        model,
        chunk_id,
        row["local_jsonl_path"],
        int(row["n_requests"] or 0),
        int(row["n_bytes"] or 0),
        result_path,
        f"deepseek-direct:{RUN_ID}:{safe_model_dir(model)}:chunk_{chunk_id:05d}.jsonl",
        "completed; ok=1000; error=0",
        "submitted",
        now,
        now,
        None,
    ))

deepseek_df = spark.createDataFrame(deepseek_records, schema) if deepseek_records else spark.createDataFrame([], schema)
repaired = current_openai.unionByName(restored_batch, allowMissingColumns=True).unionByName(deepseek_df, allowMissingColumns=True)

spark.sql(f"DELETE FROM {fqtn(BATCH_JOBS_TABLE)} WHERE run_id = {_sql_string(RUN_ID)}")
repaired.select(*[f.name for f in schema.fields]).write.format("delta").mode("append").saveAsTable(fqtn(BATCH_JOBS_TABLE))

summary = [
    row.asDict(recursive=True)
    for row in repaired.groupBy("provider", "model", "submission_status", "provider_status")
    .agg(F.count(F.lit(1)).alias("n_records"))
    .orderBy("provider", "model")
    .collect()
]
print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False, sort_keys=True))
