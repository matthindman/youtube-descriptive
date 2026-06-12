# Databricks notebook source
# MAGIC %md
# MAGIC # Inspect OpenAI Batch Error Files for Topic Multi-label Run

# COMMAND ----------
import json
import os
from datetime import datetime

from pyspark.sql import Window
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
        return os.environ.get(name.upper(), default)


_create_text_widget("run_id", "category_topic_multilabel_random_1000_20260612")
_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("status_snapshot_table", "yt_category_topic_multilabel_1000_batch_status_check")
_create_text_widget("error_samples_table", "yt_category_topic_multilabel_1000_openai_error_samples")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("openai_secret_key", "openai-api-key")
_create_text_widget("sample_lines_per_model", "25")

RUN_ID = _get_widget("run_id", "category_topic_multilabel_random_1000_20260612")
CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
STATUS_SNAPSHOT_TABLE = _get_widget("status_snapshot_table", "yt_category_topic_multilabel_1000_batch_status_check")
ERROR_SAMPLES_TABLE = _get_widget("error_samples_table", "yt_category_topic_multilabel_1000_openai_error_samples")
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
OPENAI_SECRET_KEY = _get_widget("openai_secret_key", "openai-api-key")
SAMPLE_LINES_PER_MODEL = int(_get_widget("sample_lines_per_model", "25"))


def table_ref(name: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in name.split("."))


def fqtn(table_name: str) -> str:
    return table_ref(table_name) if "." in table_name else table_ref(f"{CATALOG}.{SCHEMA}.{table_name}")


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


def file_content_text(file_content) -> str:
    if hasattr(file_content, "text"):
        return file_content.text
    if hasattr(file_content, "read"):
        data = file_content.read()
        return data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else str(data)
    if isinstance(file_content, (bytes, bytearray)):
        return file_content.decode("utf-8")
    return str(file_content)


def extract_openai_batch_error(parsed):
    """Return the nested error object from either OpenAI batch error shape."""
    err = parsed.get("error")
    if isinstance(err, dict) and err:
        return err
    response = parsed.get("response")
    if not isinstance(response, dict):
        return {}
    body = response.get("body")
    if not isinstance(body, dict):
        return {}
    err = body.get("error")
    return err if isinstance(err, dict) else {}


from openai import OpenAI

client = OpenAI(api_key=dbutils.secrets.get(scope=SECRET_SCOPE, key=OPENAI_SECRET_KEY))
status_full = fqtn(STATUS_SNAPSHOT_TABLE)
rows = (
    spark.table(status_full)
    .where(F.col("run_id") == F.lit(RUN_ID))
    .where(F.col("provider") == F.lit("openai"))
    .where(F.col("output_ref_json").isNotNull())
    .withColumn("_rn", F.row_number().over(Window.partitionBy("provider", "model", "chunk_id").orderBy(F.col("checked_at_utc").desc())))
    .where(F.col("_rn") == 1)
    .drop("_rn")
    .select("provider", "model", "chunk_id", "provider_batch_id", "output_ref_json")
    .collect()
)

records = []
inspected_at = datetime.utcnow().isoformat()
for row in rows:
    try:
        output_ref = json.loads(row["output_ref_json"] or "{}")
        error_file_id = output_ref.get("error_file_id")
        if not error_file_id:
            continue
        text = file_content_text(client.files.content(error_file_id))
        for line_number, line in enumerate(text.splitlines()[:SAMPLE_LINES_PER_MODEL], start=1):
            parsed = None
            error_code = None
            error_message = None
            custom_id = None
            try:
                parsed = json.loads(line)
                custom_id = parsed.get("custom_id")
                err = extract_openai_batch_error(parsed)
                error_code = err.get("code") or err.get("type")
                error_message = err.get("message")
            except Exception as exc:
                error_message = f"Could not parse error JSON: {repr(exc)}"
            records.append((
                RUN_ID,
                inspected_at,
                row["provider"],
                row["model"],
                int(row["chunk_id"]),
                row["provider_batch_id"],
                error_file_id,
                line_number,
                custom_id,
                error_code,
                error_message,
                line[:4000],
            ))
    except Exception as exc:
        records.append((
            RUN_ID,
            inspected_at,
            row["provider"],
            row["model"],
            int(row["chunk_id"]),
            row["provider_batch_id"],
            None,
            0,
            None,
            "inspection_error",
            repr(exc)[:1000],
            "",
        ))

schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("inspected_at_utc", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("chunk_id", IntegerType(), True),
    StructField("provider_batch_id", StringType(), True),
    StructField("error_file_id", StringType(), True),
    StructField("line_number", IntegerType(), True),
    StructField("request_id", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("raw_error_line", StringType(), True),
])

if records:
    write_run_scoped(spark.createDataFrame(records, schema), fqtn(ERROR_SAMPLES_TABLE))

payload = {
    "run_id": RUN_ID,
    "error_samples_table": fqtn(ERROR_SAMPLES_TABLE),
    "n_error_samples": len(records),
    "models": sorted({r[3] for r in records}),
}
dbutils.notebook.exit(json.dumps(payload, sort_keys=True))
