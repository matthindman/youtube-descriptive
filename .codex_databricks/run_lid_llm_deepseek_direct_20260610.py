# Databricks notebook source
# MAGIC %md
# MAGIC # Focused DeepSeek direct runner for LID LLM validation
# MAGIC
# MAGIC Consumes already generated DeepSeek request JSONL files for the reproducible 1,000-channel
# MAGIC validation sample and writes provider-compatible result JSONL files plus batch-job registry rows.

# COMMAND ----------
import json
import os
import queue
import subprocess
import time
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
)

# COMMAND ----------
def _create_text_widget(name: str, default: str, label: str = None) -> None:
    try:
        dbutils.widgets.text(name, default, label or name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None and value != "" else default
    except Exception:
        return os.environ.get(name.upper(), default)


def _get_int_widget(name: str, default: int) -> int:
    raw = _get_widget(name, str(default)).strip()
    return int(raw) if raw else default


def _get_float_widget(name: str, default: float) -> float:
    raw = _get_widget(name, str(default)).strip()
    return float(raw) if raw else default


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "too_full_20260609")
_create_text_widget("panel_batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
_create_text_widget("request_base_dir", "/dbfs/FileStore/youtube_lid_panel_batches")
_create_text_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results")
_create_text_widget("models_csv", "deepseek-v4-flash,deepseek-v4-pro")
_create_text_widget("deepseek_max_workers", "4")
_create_text_widget("deepseek_request_timeout_seconds", "45")
_create_text_widget("deepseek_max_retries", "0")
_create_text_widget("resume_existing_results", "true")
_create_text_widget("process_pending_requests", "true")
_create_text_widget("deepseek_transport", "urllib")
_create_text_widget("secret_scope", "youtube-llm-keys")
_create_text_widget("deepseek_secret_key", "deepseek-api-key")

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609")
PANEL_BATCH_JOBS_TABLE = _get_widget("panel_batch_jobs_table", "yt_lid_v3_too_full_20260609_llm_validation_batch_jobs")
REQUEST_BASE_DIR = _get_widget("request_base_dir", "/dbfs/FileStore/youtube_lid_panel_batches").rstrip("/")
RESULTS_INPUT_DIR = _get_widget("results_input_dir", "/dbfs/FileStore/youtube_lid_panel_batches/results").rstrip("/")
MODELS = [m.strip() for m in _get_widget("models_csv", "deepseek-v4-flash,deepseek-v4-pro").split(",") if m.strip()]
DEEPSEEK_MAX_WORKERS = _get_int_widget("deepseek_max_workers", 4)
DEEPSEEK_REQUEST_TIMEOUT_SECONDS = _get_float_widget("deepseek_request_timeout_seconds", 45.0)
DEEPSEEK_MAX_RETRIES = _get_int_widget("deepseek_max_retries", 0)
RESUME_EXISTING_RESULTS = _get_widget("resume_existing_results", "true").strip().lower() in {"1", "true", "t", "yes", "y"}
PROCESS_PENDING_REQUESTS = _get_widget("process_pending_requests", "true").strip().lower() in {"1", "true", "t", "yes", "y"}
DEEPSEEK_TRANSPORT = _get_widget("deepseek_transport", "urllib").strip().lower()
SECRET_SCOPE = _get_widget("secret_scope", "youtube-llm-keys")
DEEPSEEK_SECRET_KEY = _get_widget("deepseek_secret_key", "deepseek-api-key")

if DEEPSEEK_MAX_WORKERS < 1:
    raise ValueError("deepseek_max_workers must be at least 1")
if DEEPSEEK_REQUEST_TIMEOUT_SECONDS <= 0:
    raise ValueError("deepseek_request_timeout_seconds must be positive")
if DEEPSEEK_MAX_RETRIES < 0:
    raise ValueError("deepseek_max_retries must be non-negative")
if DEEPSEEK_TRANSPORT not in {"curl", "requests", "urllib"}:
    raise ValueError("deepseek_transport must be curl, requests, or urllib")

# COMMAND ----------
def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def safe_model_dir(model: str) -> str:
    return "".join(c if c.isalnum() or c in {"-", "_", "."} else "_" for c in model)


def request_path_for_model(model: str) -> str:
    return os.path.join(REQUEST_BASE_DIR, RUN_ID, "deepseek", safe_model_dir(model), "chunk_00000.jsonl")


def result_path_for_model(model: str) -> str:
    return os.path.join(RESULTS_INPUT_DIR, RUN_ID, "deepseek", safe_model_dir(model), "chunk_00000_results.jsonl")


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


batch_job_schema = StructType([
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


def write_run_scoped(df, table_full):
    if not _table_exists_full(table_full):
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("run_id")
            .saveAsTable(table_full)
        )
        return

    actual_partitions = _table_partition_columns(table_full)
    if actual_partitions != ["run_id"]:
        raise RuntimeError(f"{table_full} partition columns are {actual_partitions}, expected ['run_id'].")

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


# COMMAND ----------
api_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=DEEPSEEK_SECRET_KEY)
batch_jobs_full = fqtn(PANEL_BATCH_JOBS_TABLE)
print("Focused DeepSeek run:", RUN_ID, MODELS)
thread_state = threading.local()

try:
    import requests
except Exception:
    requests = None


def _parse_json(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        return {"text": text[:4000]}


def _post_deepseek(body: Dict[str, Any]):
    if DEEPSEEK_TRANSPORT == "requests" and requests is not None:
        session = getattr(thread_state, "session", None)
        if session is None:
            session = requests.Session()
            thread_state.session = session
        response = session.post(
            "https://api.deepseek.com/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=DEEPSEEK_REQUEST_TIMEOUT_SECONDS,
        )
        text = response.text
        return response.status_code, _parse_json(text), text

    if DEEPSEEK_TRANSPORT == "urllib":
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            "https://api.deepseek.com/chat/completions",
            data=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=DEEPSEEK_REQUEST_TIMEOUT_SECONDS) as response:
                text = response.read().decode("utf-8", errors="replace")
                return response.status, _parse_json(text), text
        except urllib.error.HTTPError as e:
            text = e.read().decode("utf-8", errors="replace")
            return e.code, _parse_json(text), text

    payload_text = json.dumps(body, ensure_ascii=False)
    marker = "\n__HTTP_STATUS__:"
    cmd = [
        "curl",
        "-sS",
        "--max-time",
        str(DEEPSEEK_REQUEST_TIMEOUT_SECONDS),
        "-w",
        marker + "%{http_code}",
        "-X",
        "POST",
        "https://api.deepseek.com/chat/completions",
        "-H",
        f"Authorization: Bearer {api_key}",
        "-H",
        "Content-Type: application/json",
        "--data-binary",
        "@-",
    ]
    try:
        completed = subprocess.run(
            cmd,
            input=payload_text.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=DEEPSEEK_REQUEST_TIMEOUT_SECONDS + 10,
        )
    except subprocess.TimeoutExpired as e:
        text = (e.stderr or b"").decode("utf-8", errors="replace")
        return 598, {"error": text[:4000] or "curl subprocess timeout"}, text

    stdout = completed.stdout.decode("utf-8", errors="replace")
    stderr = completed.stderr.decode("utf-8", errors="replace")
    if marker in stdout:
        text, status_text = stdout.rsplit(marker, 1)
        try:
            status_code = int(status_text.strip())
        except Exception:
            status_code = 500
    else:
        text = stdout
        status_code = 598 if completed.returncode == 28 else 500
    if completed.returncode not in {0, 22} and status_code == 0:
        status_code = 598 if completed.returncode == 28 else 500
    if completed.returncode != 0 and not text:
        text = stderr
    return status_code, _parse_json(text), text or stderr


def call_line(line: str):
    req = {}
    try:
        req = json.loads(line)
        custom_id = req.get("custom_id") or req.get("key")
        body = dict(req["body"])
        extra_body = body.pop("extra_body", None)
        if isinstance(extra_body, dict):
            body.update(extra_body)
        last_error = None
        for attempt in range(DEEPSEEK_MAX_RETRIES + 1):
            try:
                status_code, response_body, raw_text = _post_deepseek(body)
                out = {
                    "custom_id": custom_id,
                    "response": {
                        "status_code": status_code,
                        "body": response_body,
                    },
                }
                if 200 <= status_code < 300:
                    return out, True
                last_error = raw_text[:2000]
                if status_code not in {408, 409, 429, 500, 502, 503, 504} or attempt >= DEEPSEEK_MAX_RETRIES:
                    out["error"] = last_error
                    return out, False
            except Exception as e:
                last_error = repr(e)[:2000]
                if attempt >= DEEPSEEK_MAX_RETRIES:
                    return {
                        "custom_id": custom_id,
                        "response": {"status_code": 500, "error": last_error},
                        "error": last_error,
                    }, False
            time.sleep(min(2 ** attempt, 8))
        return {"custom_id": custom_id, "response": {"status_code": 500, "error": last_error}, "error": last_error}, False
    except Exception as e:
        custom_id = None
        try:
            custom_id = req.get("custom_id") or req.get("key")
        except Exception:
            pass
        error = repr(e)[:2000]
        return {"custom_id": custom_id, "response": {"status_code": 500, "error": error}, "error": error}, False


def _custom_id_from_line(line: str):
    try:
        req = json.loads(line)
        return req.get("custom_id") or req.get("key")
    except Exception:
        return None


def _process_worker(line: str, output_queue):
    output_queue.put(call_line(line))


def _run_pending_lines(pending_lines, dst, model: str):
    n_ok = 0
    n_error = 0
    if DEEPSEEK_MAX_WORKERS == 1:
        for i, line in enumerate(pending_lines, start=1):
            out, ok = call_line(line)
            if ok:
                n_ok += 1
            else:
                n_error += 1
            dst.write(json.dumps(out, ensure_ascii=False) + "\n")
            dst.flush()
            if i % 25 == 0 or i == len(pending_lines):
                print(f"DeepSeek direct {model}: {i:,}/{len(pending_lines):,} pending done; ok={n_ok:,}; error={n_error:,}")
        return n_ok, n_error

    with ThreadPoolExecutor(max_workers=DEEPSEEK_MAX_WORKERS) as pool:
        futures = [pool.submit(call_line, line) for line in pending_lines]
        for i, fut in enumerate(as_completed(futures), start=1):
            out, ok = fut.result()
            if ok:
                n_ok += 1
            else:
                n_error += 1
            dst.write(json.dumps(out, ensure_ascii=False) + "\n")
            dst.flush()
            if i % 25 == 0 or i == len(pending_lines):
                print(f"DeepSeek direct {model}: {i:,}/{len(pending_lines):,} pending done; ok={n_ok:,}; error={n_error:,}")
    return n_ok, n_error


def run_model(model: str):
    request_path = request_path_for_model(model)
    result_path = result_path_for_model(model)
    result_dir = os.path.dirname(result_path)
    os.makedirs(result_dir, exist_ok=True)
    with open(request_path, "r", encoding="utf-8") as src:
        lines = [line for line in src if line.strip()]

    completed_ids = set()
    existing_result_lines = []
    if RESUME_EXISTING_RESULTS and os.path.exists(result_path):
        with open(result_path, "r", encoding="utf-8") as existing:
            for line in existing:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    if obj.get("custom_id"):
                        completed_ids.add(obj["custom_id"])
                        existing_result_lines.append(line if line.endswith("\n") else line + "\n")
                except Exception:
                    pass

    pending_lines = []
    for line in lines:
        try:
            req = json.loads(line)
            custom_id = req.get("custom_id") or req.get("key")
        except Exception:
            custom_id = None
        if custom_id not in completed_ids:
            pending_lines.append(line)
    if not PROCESS_PENDING_REQUESTS:
        print(f"DeepSeek direct {model}: process_pending_requests=false; preserving existing rows only")
        pending_lines = []

    if completed_ids:
        print(f"DeepSeek direct {model}: resuming with {len(completed_ids):,} existing result rows")

    n_ok = 0
    n_error = 0
    print(f"DeepSeek direct {model}: {len(pending_lines):,}/{len(lines):,} pending requests with {DEEPSEEK_MAX_WORKERS} workers")
    with open(result_path, "w", encoding="utf-8") as dst:
        for line in existing_result_lines:
            dst.write(line)
        dst.flush()
        n_ok, n_error = _run_pending_lines(pending_lines, dst, model)

    total = 0
    total_error = 0
    with open(result_path, "r", encoding="utf-8") as final:
        for line in final:
            if not line.strip():
                continue
            total += 1
            try:
                obj = json.loads(line)
                if obj.get("error") or not (200 <= int(obj.get("response", {}).get("status_code", 500)) < 300):
                    total_error += 1
            except Exception:
                total_error += 1

    status = "completed" if total_error == 0 and total == len(lines) else "partial_or_errors"
    return {
        "request_path": request_path,
        "result_path": result_path,
        "n_requests": total,
        "n_bytes": os.path.getsize(request_path),
        "provider_status": f"{status}; ok={total - total_error}; error={total_error}",
        "submission_status": "submitted",
        "submission_error": None,
    }


# COMMAND ----------
new_records = []
for model in MODELS:
    submitted_at = datetime.utcnow().isoformat()
    try:
        result = run_model(model)
        print("deepseek", model, "submitted", result)
    except Exception as e:
        request_path = request_path_for_model(model)
        result = {
            "request_path": request_path,
            "result_path": None,
            "n_requests": 0,
            "n_bytes": os.path.getsize(request_path) if os.path.exists(request_path) else 0,
            "provider_status": None,
            "submission_status": "error",
            "submission_error": repr(e)[:2000],
        }
        print("deepseek", model, "ERROR", result["submission_error"])
    new_records.append((
        RUN_ID,
        "deepseek",
        model,
        0,
        result["request_path"],
        int(result["n_requests"]),
        int(result["n_bytes"]),
        result["result_path"],
        f"deepseek-direct:{RUN_ID}:{safe_model_dir(model)}:chunk_00000.jsonl",
        result["provider_status"],
        result["submission_status"],
        submitted_at,
        datetime.utcnow().isoformat(),
        result["submission_error"],
    ))

new_df = spark.createDataFrame(new_records, batch_job_schema)
if _table_exists_full(batch_jobs_full):
    existing_df = (
        spark.table(batch_jobs_full)
        .where(F.col("run_id") == F.lit(RUN_ID))
        .where(~((F.col("provider") == F.lit("deepseek")) & (F.col("model").isin(MODELS))))
    )
    combined_df = existing_df.unionByName(new_df, allowMissingColumns=True)
else:
    combined_df = new_df

write_run_scoped(combined_df, batch_jobs_full)
print(f"Wrote focused DeepSeek registry rows to {batch_jobs_full}")
display(new_df)
