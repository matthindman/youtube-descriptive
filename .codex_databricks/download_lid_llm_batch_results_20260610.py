# Databricks notebook source
# MAGIC %md
# MAGIC # LID LLM validation batch result download
# MAGIC
# MAGIC Downloads completed Anthropic/Gemini batch outputs for the validation run into the common
# MAGIC `results_input_dir` tree used by `03_language_llm_panel_databricks.py`.

# COMMAND ----------
# MAGIC %pip install anthropic "google-genai>=1.51.0" pandas pyarrow

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import re
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


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
        return default


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "too_full_20260609")
_create_text_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
_create_text_widget("status_snapshot_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")
_create_text_widget("result_files_table", "yt_lid_v3_too_full_20260609_llm_validation_result_files")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("anthropic_secret_key", "anthropic-api-key")
_create_text_widget("gemini_secret_key", "gemini-api-key")
_create_text_widget("provider_filter", "anthropic,gemini")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609")
BATCH_JOBS_TABLE = _get_widget("batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
STATUS_SNAPSHOT_TABLE = _get_widget("status_snapshot_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_status_check")
RESULT_FILES_TABLE = _get_widget("result_files_table", "yt_lid_v3_too_full_20260609_llm_validation_result_files")
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results").rstrip("/")
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
ANTHROPIC_SECRET_KEY = _get_widget("anthropic_secret_key", "anthropic-api-key")
GEMINI_SECRET_KEY = _get_widget("gemini_secret_key", "gemini-api-key")
PROVIDER_FILTER = {
    p.strip().lower()
    for p in _get_widget("provider_filter", "anthropic,gemini").split(",")
    if p.strip()
}


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", model or "model")


def as_jsonable(value: Any):
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "to_json_dict"):
        return value.to_json_dict()
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): as_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [as_jsonable(v) for v in value]
    return str(value)


def enum_name(value: Any):
    if value is None:
        return None
    return getattr(value, "name", None) or getattr(value, "value", None) or str(value)


def get_secret(key: str) -> str:
    return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)


def _table_exists_full(table_full: str) -> bool:
    try:
        spark.table(table_full).limit(0)
        return True
    except Exception:
        return False


def _table_partition_columns(table_full: str):
    try:
        row = spark.sql(f"DESCRIBE DETAIL {table_full}").select("partitionColumns").collect()[0]
        return list(row["partitionColumns"] or [])
    except Exception:
        return []


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def write_run_scoped(df, table_full: str):
    if not _table_exists_full(table_full):
        df.write.format("delta").mode("overwrite").partitionBy("run_id").saveAsTable(table_full)
        return

    actual_partitions = _table_partition_columns(table_full)
    if actual_partitions != ["run_id"]:
        raise RuntimeError(f"{table_full} partition columns are {actual_partitions}, expected ['run_id'].")

    existing = spark.table(table_full)
    existing_cols = set(existing.columns)
    new_cols = sorted(set(df.columns) - existing_cols)
    if new_cols:
        print(f"Evolving {table_full} schema with new result-file columns {new_cols}.")
        df.limit(0).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_full)
        existing = spark.table(table_full)

    write_df = df
    for field in existing.schema.fields:
        if field.name not in write_df.columns:
            write_df = write_df.withColumn(field.name, F.lit(None).cast(field.dataType))
    write_df = write_df.select(*existing.columns)

    (
        write_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"run_id = {_sql_string(RUN_ID)}")
        .partitionBy("run_id")
        .saveAsTable(table_full)
    )


status = (
    spark.table(fqtn(STATUS_SNAPSHOT_TABLE))
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("status_error").isNull())
    .select("provider", "model", "chunk_id", "provider_batch_id", "provider_status", "output_ref_json")
)
jobs = (
    spark.table(fqtn(BATCH_JOBS_TABLE))
    .where(F.col("run_id") == F.lit(RUN_ID))
    .select("provider", "model", "chunk_id", "provider_file_id", "provider_batch_id", "n_requests")
)
completed = (
    status.alias("s")
    .join(
        jobs.alias("j"),
        on=["provider", "model", "chunk_id", "provider_batch_id"],
        how="inner",
    )
    .where(F.col("provider").isin(*sorted(PROVIDER_FILTER)))
    .where(
        ((F.col("provider") == F.lit("anthropic")) & (F.col("provider_status") == F.lit("ended")))
        | ((F.col("provider") == F.lit("gemini")) & (F.col("provider_status") == F.lit("JOB_STATE_SUCCEEDED")))
    )
    .select(
        "provider", "model", "chunk_id", "provider_batch_id", "provider_file_id",
        "n_requests", "provider_status", "output_ref_json",
    )
    .orderBy("provider", "model", "chunk_id")
    .collect()
)

print(f"Completed downloadable batches for run_id={RUN_ID}: {len(completed)}")

anthropic_client = None
gemini_client = None
downloaded_at = datetime.now(timezone.utc).isoformat()
records = []

for row in completed:
    provider = row["provider"]
    model = row["model"]
    chunk_id = int(row["chunk_id"])
    provider_batch_id = row["provider_batch_id"]
    out_dir = os.path.join(RESULTS_INPUT_DIR, RUN_ID, provider, safe_model_dir(model))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"chunk_{chunk_id:05d}_results.jsonl")
    n_lines = 0
    status_label = "downloaded"
    error = None

    try:
        if provider == "anthropic":
            import anthropic

            if anthropic_client is None:
                anthropic_client = anthropic.Anthropic(api_key=get_secret(ANTHROPIC_SECRET_KEY))
            with open(out_path, "w", encoding="utf-8") as dst:
                for item in anthropic_client.messages.batches.results(provider_batch_id):
                    dst.write(json.dumps(as_jsonable(item), ensure_ascii=False) + "\n")
                    n_lines += 1
        elif provider == "gemini":
            from google import genai

            if gemini_client is None:
                gemini_client = genai.Client(api_key=get_secret(GEMINI_SECRET_KEY))
            try:
                batch = gemini_client.batches.get(name=provider_batch_id)
            except TypeError:
                batch = gemini_client.batches.get(provider_batch_id)
            state = enum_name(getattr(batch, "state", None))
            if state != "JOB_STATE_SUCCEEDED":
                raise RuntimeError(f"Gemini batch is not succeeded: {state}")
            dest = getattr(batch, "dest", None)
            result_file_name = getattr(dest, "file_name", None)
            if not result_file_name:
                raise RuntimeError("Gemini batch has no dest.file_name")
            file_content = gemini_client.files.download(file=result_file_name)
            if isinstance(file_content, str):
                text = file_content
            else:
                text = bytes(file_content).decode("utf-8")
            with open(out_path, "w", encoding="utf-8") as dst:
                dst.write(text)
                if text and not text.endswith("\n"):
                    dst.write("\n")
            n_lines = len([line for line in text.splitlines() if line.strip()])
        else:
            status_label = "skipped"
            error = f"Unsupported provider for download: {provider}"
    except Exception as exc:
        status_label = "error"
        error = repr(exc)[:2000]

    records.append((
        RUN_ID, downloaded_at, provider, model, chunk_id, provider_batch_id,
        out_path, int(row["n_requests"] or 0), n_lines, status_label, error,
    ))
    print(provider, model, provider_batch_id, status_label, f"lines={n_lines}", error or "")

schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("downloaded_at_utc", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("provider_batch_id", StringType(), True),
    StructField("result_jsonl_path", StringType(), True),
    StructField("n_expected_requests", IntegerType(), True),
    StructField("n_result_lines", IntegerType(), True),
    StructField("download_status", StringType(), True),
    StructField("download_error", StringType(), True),
])

result_files = spark.createDataFrame(records, schema)
write_run_scoped(result_files, fqtn(RESULT_FILES_TABLE))
display(result_files.orderBy("provider", "model", "chunk_id"))

errors = result_files.where(F.col("download_status") == F.lit("error")).count()
if errors:
    raise RuntimeError(f"{errors} result downloads failed; see {fqtn(RESULT_FILES_TABLE)}")
