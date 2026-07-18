#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-matt.hindman@researchaccelerator.org}"
DATABRICKS_AUTH_STORAGE="${DATABRICKS_AUTH_STORAGE:-plaintext}"
CLUSTER_ID="${CLUSTER_ID:-0601-203643-bkxsqffg}"
if [[ "${DATABRICKS_PROFILE}" != "matt.hindman@researchaccelerator.org" ]]; then
  echo "Refusing unregistered Databricks profile: ${DATABRICKS_PROFILE}" >&2
  exit 2
fi
if [[ "${CLUSTER_ID}" != "0601-203643-bkxsqffg" ]]; then
  echo "Refusing unregistered cluster: ${CLUSTER_ID}" >&2
  exit 2
fi
RUN_TOKEN="${RUN_TOKEN:-20260716_v1}"
RUN_ID="${RUN_ID:-full_corpus_lid_v3_20260715_${RUN_TOKEN}}"
WORKSPACE_DIR="${WORKSPACE_DIR:-/Users/matt.hindman@researchaccelerator.org/youtube_descriptive}"
NOTEBOOK_PATH="${NOTEBOOK_PATH:-${WORKSPACE_DIR}/youtube_topic_treemap_full_corpus}"
DBFS_CONFIG_DIR="${DBFS_CONFIG_DIR:-dbfs:/FileStore/youtube_topic_treemap_full_corpus_${RUN_TOKEN}/config}"
ARTIFACT_DIR="${ARTIFACT_DIR:-dbfs:/FileStore/youtube_topic_treemap_full_corpus_${RUN_TOKEN}}"
LOCAL_EXPORT_DIR="${LOCAL_EXPORT_DIR:-${ROOT_DIR}/outputs/youtube_topic_treemap_full_corpus_${RUN_TOKEN}/databricks_export}"
TIMEOUT="${TIMEOUT:-4h}"

JOB_JSON="$(mktemp "${TMPDIR:-/tmp}/youtube_topic_treemap_full_corpus.XXXXXX.json")"
cleanup() {
  rm -f "${JOB_JSON}"
}
trap cleanup EXIT

DATABRICKS_CMD=(env "DATABRICKS_AUTH_STORAGE=${DATABRICKS_AUTH_STORAGE}" databricks -p "${DATABRICKS_PROFILE}")

"${DATABRICKS_CMD[@]}" workspace mkdirs "${WORKSPACE_DIR}"
"${DATABRICKS_CMD[@]}" workspace import "${NOTEBOOK_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/youtube_topic_treemap_full_corpus.py" \
  --format SOURCE \
  --language PYTHON \
  --overwrite

"${DATABRICKS_CMD[@]}" fs mkdir "${DBFS_CONFIG_DIR}"
for config_file in \
  youtube_topic_hierarchy_v2.yaml \
  topic_remap.yaml \
  language_normalization.yaml \
  iso639_language_names.csv \
  treemap_top_channel_placement.csv
do
  "${DATABRICKS_CMD[@]}" fs cp \
    "${ROOT_DIR}/config/${config_file}" \
    "${DBFS_CONFIG_DIR}/${config_file}" \
    --overwrite
done

export CLUSTER_ID RUN_ID NOTEBOOK_PATH DBFS_CONFIG_DIR ARTIFACT_DIR
python3 - "${JOB_JSON}" <<'PY'
import json
import os
import sys

config_dir = os.environ["DBFS_CONFIG_DIR"]
payload = {
    "run_name": f"youtube_topic_treemap_{os.environ['RUN_ID']}",
    "tasks": [
        {
            "task_key": "materialize_full_corpus_treemap",
            "existing_cluster_id": os.environ["CLUSTER_ID"],
            "timeout_seconds": 14400,
            "notebook_task": {
                "notebook_path": os.environ["NOTEBOOK_PATH"],
                "base_parameters": {
                    "run_id": os.environ["RUN_ID"],
                    "language_table": "dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_current",
                    "expected_label_version": "lid_v3_channel_crawl_full_20260623_deepseek_flash_20260715_v1",
                    "topic_table": "dev_sean.default.channel_category",
                    "stats_table": "dev_sean.default.yt_channel_stats",
                    "current_snapshot": "2026-06-15",
                    "prior_snapshot": "2026-05-18",
                    "minimum_subscribers": "10000",
                    "top_k_languages": "12",
                    "top_channels_per_leaf": "15",
                    "hierarchy_config_path": f"{config_dir}/youtube_topic_hierarchy_v2.yaml",
                    "topic_remap_path": f"{config_dir}/topic_remap.yaml",
                    "language_normalization_path": f"{config_dir}/language_normalization.yaml",
                    "language_names_path": f"{config_dir}/iso639_language_names.csv",
                    "placement_csv_path": f"{config_dir}/treemap_top_channel_placement.csv",
                    "output_catalog": "dev_sean",
                    "output_schema": "matt",
                    "table_prefix": "yt_treemap_full_corpus_lid_v3_20260715_v1",
                    "write_delta_tables": "true",
                    "artifact_dir": os.environ["ARTIFACT_DIR"],
                },
            },
        }
    ],
}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
PY

echo "Submitting ${NOTEBOOK_PATH} on existing cluster ${CLUSTER_ID}."
"${DATABRICKS_CMD[@]}" jobs submit --json "@${JOB_JSON}" --timeout "${TIMEOUT}" --output json

case "${LOCAL_EXPORT_DIR}" in
  "${ROOT_DIR}"/outputs/youtube_topic_treemap_full_corpus_*/databricks_export)
    ;;
  *)
    echo "Refusing to replace unexpected LOCAL_EXPORT_DIR: ${LOCAL_EXPORT_DIR}" >&2
    exit 2
    ;;
esac
rm -rf -- "${LOCAL_EXPORT_DIR}"
mkdir -p "${LOCAL_EXPORT_DIR}"
"${DATABRICKS_CMD[@]}" fs cp "${ARTIFACT_DIR}" "${LOCAL_EXPORT_DIR}" --recursive --overwrite
echo "Downloaded compact renderer exports to ${LOCAL_EXPORT_DIR}."
