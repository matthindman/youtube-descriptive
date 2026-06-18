#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DATE="${RUN_DATE:-20260617}"
WORKSPACE_DIR="${WORKSPACE_DIR:-/Users/matt.hindman@researchaccelerator.org/lid_v3_too_20260609}"
NOTEBOOK_PATH="${NOTEBOOK_PATH:-${WORKSPACE_DIR}/youtube_topic_treemap_v2}"
CLUSTER_ID="${CLUSTER_ID:-0601-203643-bkxsqffg}"
TIMEOUT="${TIMEOUT:-3h}"
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-matt.hindman@researchaccelerator.org}"
DATABRICKS_AUTH_STORAGE="${DATABRICKS_AUTH_STORAGE:-plaintext}"

DBFS_CONFIG_DIR="${DBFS_CONFIG_DIR:-dbfs:/FileStore/youtube_topic_treemap_top_ocean_${RUN_DATE}/config}"
CONFIG_DBFS_PATH="${CONFIG_DBFS_PATH:-${DBFS_CONFIG_DIR}/youtube_topic_hierarchy_v2.yaml}"
ARTIFACT_DIR="${ARTIFACT_DIR:-/dbfs/FileStore/youtube_topic_treemap_top_ocean_${RUN_DATE}}"

export RUN_DATE WORKSPACE_DIR NOTEBOOK_PATH CLUSTER_ID CONFIG_DBFS_PATH ARTIFACT_DIR

JOB_JSON="$(mktemp "${TMPDIR:-/tmp}/youtube_topic_treemap_v2.XXXXXX.json")"
cleanup() {
  rm -f "${JOB_JSON}"
}
trap cleanup EXIT

DATABRICKS_CMD=(env "DATABRICKS_AUTH_STORAGE=${DATABRICKS_AUTH_STORAGE}" databricks -p "${DATABRICKS_PROFILE}")

"${DATABRICKS_CMD[@]}" workspace mkdirs "${WORKSPACE_DIR}"
"${DATABRICKS_CMD[@]}" workspace import "${NOTEBOOK_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/youtube_topic_treemap_v2.py" \
  --format SOURCE \
  --language PYTHON \
  --overwrite

"${DATABRICKS_CMD[@]}" fs mkdir "${DBFS_CONFIG_DIR}"
"${DATABRICKS_CMD[@]}" fs cp "${ROOT_DIR}/config/youtube_topic_hierarchy_v2.yaml" "${CONFIG_DBFS_PATH}" --overwrite

python3 - "${JOB_JSON}" <<'PY'
import json
import os
import sys

job_path = sys.argv[1]
run_date = os.environ.get("RUN_DATE", "20260617")
workspace_dir = os.environ.get(
    "WORKSPACE_DIR",
    "/Users/matt.hindman@researchaccelerator.org/lid_v3_too_20260609",
)
notebook_path = os.environ.get("NOTEBOOK_PATH", f"{workspace_dir}/youtube_topic_treemap_v2")
cluster_id = os.environ.get("CLUSTER_ID", "0601-203643-bkxsqffg")
config_path = os.environ.get(
    "CONFIG_DBFS_PATH",
    f"dbfs:/FileStore/youtube_topic_treemap_top_ocean_{run_date}/config/youtube_topic_hierarchy_v2.yaml",
)
artifact_dir = os.environ.get(
    "ARTIFACT_DIR",
    f"/dbfs/FileStore/youtube_topic_treemap_top_ocean_{run_date}",
)

payload = {
    "run_name": f"youtube_topic_treemap_v2_{run_date}",
    "tasks": [
        {
            "task_key": "youtube_topic_treemap_v2",
            "existing_cluster_id": cluster_id,
            "timeout_seconds": 10800,
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "run_date": run_date,
                    "hierarchy_config_path": config_path,
                    "topic_table": os.environ.get("TOPIC_TABLE", "dev_sean.default.channel_category"),
                    "metrics_table_candidates": os.environ.get(
                        "METRICS_TABLE_CANDIDATES",
                        "dev_sean.default.yt_channel_stats_full,prod_tads.youtube_too.yt_sl_channels_metrics",
                    ),
                    "channel_table": os.environ.get("CHANNEL_TABLE", "prod_tads.youtube_too.yt_sl_channels"),
                    "language_table": os.environ.get("LANGUAGE_TABLE", "dev_sean.matt.yt_lid_v3_channels"),
                    "language_run_id": os.environ.get("LANGUAGE_RUN_ID", "default"),
                    "top_n_channels": os.environ.get("TOP_N_CHANNELS", "200000"),
                    "top_k_languages": os.environ.get("TOP_K_LANGUAGES", "25"),
                    "top_n_channels_per_leaf": os.environ.get("TOP_N_CHANNELS_PER_LEAF", "10"),
                    "max_plot_rows": os.environ.get("MAX_PLOT_ROWS", "20000"),
                    "snapshot_date": os.environ.get("SNAPSHOT_DATE", ""),
                    "snapshot_min_completeness_fraction": os.environ.get("SNAPSHOT_MIN_COMPLETENESS_FRACTION", "0.90"),
                    "output_catalog": os.environ.get("OUTPUT_CATALOG", "dev_sean"),
                    "output_schema": os.environ.get("OUTPUT_SCHEMA", "matt"),
                    "write_delta_tables": os.environ.get("WRITE_DELTA_TABLES", "true"),
                    "artifact_dir": artifact_dir,
                },
            },
        }
    ],
}

with open(job_path, "w") as fh:
    json.dump(payload, fh, indent=2)
PY

echo "Submitting ${NOTEBOOK_PATH} on cluster ${CLUSTER_ID}; artifacts: ${ARTIFACT_DIR}"
RUN_RESULT="$("${DATABRICKS_CMD[@]}" jobs submit --json "@${JOB_JSON}" --timeout "${TIMEOUT}" --output json)"
printf '%s\n' "${RUN_RESULT}"

TASK_RUN_ID="$(
  printf '%s\n' "${RUN_RESULT}" | python3 -c '
import json
import sys

try:
    payload = json.load(sys.stdin)
except Exception:
    sys.exit(0)

tasks = payload.get("tasks") or []
if tasks and tasks[0].get("run_id"):
    print(tasks[0]["run_id"])
elif payload.get("run_id"):
    print(payload["run_id"])
'
)"

if [[ -n "${TASK_RUN_ID}" ]]; then
  echo "Notebook task output for run ${TASK_RUN_ID}:"
  "${DATABRICKS_CMD[@]}" jobs get-run-output "${TASK_RUN_ID}" --output json
fi
