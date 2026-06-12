# Databricks notebook source
import json
import os

from pyspark.sql import functions as F


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


_create_text_widget("run_id", "category_topic_random_1000_20260611")
_create_text_widget("output_catalog", "dev_sean")
_create_text_widget("output_schema", "matt")
_create_text_widget("output_prefix", "yt_category_topic_random_1000")
_create_text_widget("result_files_table", "yt_category_topic_random_1000_result_files")
_create_text_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_batches")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_batches/results")

RUN_ID = _get_widget("run_id", "category_topic_random_1000_20260611")
OUTPUT_CATALOG = _get_widget("output_catalog", "dev_sean")
OUTPUT_SCHEMA = _get_widget("output_schema", "matt")
OUTPUT_PREFIX = _get_widget("output_prefix", "yt_category_topic_random_1000")
RESULT_FILES_TABLE = _get_widget("result_files_table", "yt_category_topic_random_1000_result_files")
BATCH_OUTPUT_DIR = _get_widget("batch_output_dir", "/dbfs/FileStore/youtube_category_topic_batches").rstrip("/")
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_category_topic_batches/results").rstrip("/")


def table_ref(name: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in name.split("."))


def out_table(suffix: str) -> str:
    return table_ref(f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{OUTPUT_PREFIX}_{suffix}")


def table_count(table_full: str) -> dict:
    try:
        df = spark.table(table_full).where(F.col("run_id") == F.lit(RUN_ID))
        return {"exists": True, "rows": df.count()}
    except Exception as exc:
        return {"exists": False, "error": repr(exc)[:1000]}


def prompt_input_stats(table_full: str) -> dict:
    try:
        df = spark.table(table_full).where(F.col("run_id") == F.lit(RUN_ID))
        row = df.agg(
            F.count("*").alias("rows"),
            F.countDistinct("primary_topic_slug").alias("distinct_primary_topic_slugs"),
            F.sum(F.when(F.col("primary_topic_slug").isNotNull(), 1).otherwise(0)).alias("rows_with_primary_topic_slug"),
            F.avg(F.length("prompt_user")).alias("avg_prompt_user_chars"),
            F.max(F.length("prompt_user")).alias("max_prompt_user_chars"),
            F.max(F.length("system_prompt")).alias("system_prompt_chars"),
            F.avg("n_videos_in_prompt").alias("avg_videos_in_prompt"),
        ).collect()[0].asDict(recursive=True)
        top = [
            r.asDict(recursive=True)
            for r in (
                df.groupBy("primary_topic_slug", "primary_topic_name")
                .count()
                .orderBy(F.desc("count"), "primary_topic_slug")
                .limit(15)
                .collect()
            )
        ]
        return {"exists": True, "stats": row, "top_primary_topics": top}
    except Exception as exc:
        return {"exists": False, "error": repr(exc)[:1000]}


def batch_jobs_detail(table_full: str) -> dict:
    try:
        df = spark.table(table_full).where(F.col("run_id") == F.lit(RUN_ID))
        rows = [
            r.asDict(recursive=True)
            for r in (
                df.select(
                    "provider",
                    "model",
                    "chunk_id",
                    "provider_batch_id",
                    "provider_status",
                    "submission_status",
                    "submission_error",
                    "submitted_at_utc",
                )
                .orderBy("provider", "model", "chunk_id")
                .collect()
            )
        ]
        grouped = [
            r.asDict(recursive=True)
            for r in (
                df.groupBy("provider", "submission_status")
                .count()
                .orderBy("provider", "submission_status")
                .collect()
            )
        ]
        return {"exists": True, "rows": rows, "grouped": grouped}
    except Exception as exc:
        return {"exists": False, "error": repr(exc)[:1000]}


def result_files_detail(table_full: str) -> dict:
    try:
        df = spark.table(table_full).where(F.col("run_id") == F.lit(RUN_ID))
        rows = [
            r.asDict(recursive=True)
            for r in (
                df.select(
                    "provider",
                    "model",
                    "chunk_id",
                    "result_jsonl_path",
                    "n_expected_requests",
                    "n_result_lines",
                    "download_status",
                    "download_error",
                )
                .orderBy("provider", "model", "chunk_id")
                .collect()
            )
        ]
        return {"exists": True, "rows": rows}
    except Exception as exc:
        return {"exists": False, "error": repr(exc)[:1000]}


def dir_status(path: str) -> dict:
    try:
        entries = dbutils.fs.ls(path.replace("/dbfs", "dbfs:", 1) if path.startswith("/dbfs/") else path)
        return {"exists": True, "entries": [e.path for e in entries[:50]]}
    except Exception as exc:
        return {"exists": False, "error": repr(exc)[:1000]}


summary = {
    "run_id": RUN_ID,
    "tables": {
        suffix: table_count(out_table(suffix))
        for suffix in ["prompt_inputs", "requests", "batch_files", "batch_jobs"]
    },
    "prompt_inputs_detail": prompt_input_stats(out_table("prompt_inputs")),
    "batch_jobs_detail": batch_jobs_detail(out_table("batch_jobs")),
    "result_files_detail": result_files_detail(f"`{OUTPUT_CATALOG}`.`{OUTPUT_SCHEMA}`.`{RESULT_FILES_TABLE}`"),
    "batch_dir": dir_status(f"{BATCH_OUTPUT_DIR}/{RUN_ID}"),
    "results_dir": dir_status(f"{RESULTS_INPUT_DIR}/{RUN_ID}"),
}

print(json.dumps(summary, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(summary, sort_keys=True))
