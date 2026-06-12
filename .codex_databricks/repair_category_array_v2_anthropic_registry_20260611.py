# Databricks notebook source
import json
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


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
_create_text_widget("run_id", "category_topic_random_1000_array_v2_20260611")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "category_topic_random_1000_array_v2_20260611")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000")


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def out_table(suffix: str) -> str:
    return fqtn(f"{OUTPUT_PREFIX}_{suffix}")


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


batch_files_full = out_table("batch_files")
batch_jobs_full = out_table("batch_jobs")

anthropic_batches = {
    "claude-haiku-4-5": ("msgbatch_019y5UQGjTsh31SE7oqLehTB", "2026-06-12T00:02:53.595300"),
    "claude-opus-4-8": ("msgbatch_01NTVNX52bGN1qM9EYd2USxH", "2026-06-12T00:02:54.504942"),
    "claude-sonnet-4-6": ("msgbatch_01AgLrGTkPyVJW3hp55CqUkF", "2026-06-12T00:02:55.519363"),
}

batch_job_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("local_jsonl_path", StringType(), True),
    StructField("provider_file_id", StringType(), True),
    StructField("provider_batch_id", StringType(), True),
    StructField("provider_status", StringType(), True),
    StructField("submission_status", StringType(), True),
    StructField("submission_error", StringType(), True),
    StructField("submitted_at_utc", StringType(), True),
])

batch_files = {
    row["model"]: row["local_jsonl_path"]
    for row in (
        spark.table(batch_files_full)
        .where((F.col("run_id") == F.lit(RUN_ID)) & (F.col("provider") == F.lit("anthropic")))
        .select("model", "local_jsonl_path")
        .collect()
    )
}

repair_records = []
for model, (batch_id, submitted_at) in anthropic_batches.items():
    if model not in batch_files:
        raise RuntimeError(f"Missing batch file row for anthropic model {model}")
    repair_records.append((
        RUN_ID,
        "anthropic",
        model,
        0,
        batch_files[model],
        None,
        batch_id,
        "in_progress",
        "submitted",
        None,
        submitted_at,
    ))

repair_df = spark.createDataFrame(repair_records, batch_job_schema)

existing = spark.table(batch_jobs_full)
current = existing.where(F.col("run_id") == F.lit(RUN_ID))
replace_keys = repair_df.select("provider", "model", "chunk_id").distinct()
keep_rows = [
    row.asDict(recursive=True)
    for row in current.join(replace_keys, on=["provider", "model", "chunk_id"], how="left_anti").collect()
]
keep_df = spark.createDataFrame(keep_rows, schema=existing.schema) if keep_rows else spark.createDataFrame([], schema=existing.schema)
jobs_to_write = keep_df.unionByName(repair_df, allowMissingColumns=True)
for field in existing.schema.fields:
    if field.name not in jobs_to_write.columns:
        jobs_to_write = jobs_to_write.withColumn(field.name, F.lit(None).cast(field.dataType))
jobs_to_write = jobs_to_write.select(*existing.columns)

spark.sql(f"DELETE FROM {batch_jobs_full} WHERE run_id = {_sql_string(RUN_ID)}")
jobs_to_write.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(batch_jobs_full)

result = {
    "run_id": RUN_ID,
    "repaired_anthropic_models": sorted(anthropic_batches),
    "batch_jobs_table": batch_jobs_full,
}
print(json.dumps(result, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
