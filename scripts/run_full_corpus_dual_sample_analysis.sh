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
WORKSPACE_DIR="${WORKSPACE_DIR:-/Users/matt.hindman@researchaccelerator.org/full_corpus_dual_sample_20260717_v1}"
NOTEBOOK_PATH="${WORKSPACE_DIR}/14_full_corpus_dual_sample_analysis_databricks"
HELPER_PATH="${WORKSPACE_DIR}/full_corpus_dual_sample_design"
LOCAL_CONFIG="${ROOT_DIR}/config/full_corpus_dual_sample_20260717_v1.json"
LOCAL_HIERARCHY="${ROOT_DIR}/config/youtube_topic_hierarchy_v2.yaml"
LOCAL_REMAP="${ROOT_DIR}/config/topic_remap.yaml"
DBFS_DIR="${DBFS_DIR:-dbfs:/FileStore/youtube_descriptive}"
DBFS_CONFIG="${DBFS_DIR}/full_corpus_dual_sample_20260717_v1.json"
DBFS_HIERARCHY="${DBFS_DIR}/youtube_topic_hierarchy_v2.yaml"
DBFS_REMAP="${DBFS_DIR}/topic_remap.yaml"
TIMEOUT="${TIMEOUT:-24h}"

JOB_JSON="$(mktemp "${TMPDIR:-/tmp}/full_corpus_dual_sample_analysis.XXXXXX.json")"
cleanup() {
  rm -f "${JOB_JSON}"
}
trap cleanup EXIT

DATABRICKS_CMD=(env "DATABRICKS_AUTH_STORAGE=${DATABRICKS_AUTH_STORAGE}" databricks -p "${DATABRICKS_PROFILE}")

"${DATABRICKS_CMD[@]}" workspace mkdirs "${WORKSPACE_DIR}"
"${DATABRICKS_CMD[@]}" workspace import "${HELPER_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/full_corpus_dual_sample_design.py" \
  --format SOURCE --language PYTHON --overwrite
"${DATABRICKS_CMD[@]}" workspace import "${NOTEBOOK_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/14_full_corpus_dual_sample_analysis_databricks.py" \
  --format SOURCE --language PYTHON --overwrite
"${DATABRICKS_CMD[@]}" fs mkdir "${DBFS_DIR}"
"${DATABRICKS_CMD[@]}" fs cp "${LOCAL_CONFIG}" "${DBFS_CONFIG}" --overwrite
"${DATABRICKS_CMD[@]}" fs cp "${LOCAL_HIERARCHY}" "${DBFS_HIERARCHY}" --overwrite
"${DATABRICKS_CMD[@]}" fs cp "${LOCAL_REMAP}" "${DBFS_REMAP}" --overwrite

python3 "${ROOT_DIR}/scripts/build_full_corpus_dual_sample_analysis_job.py" \
  --config "${LOCAL_CONFIG}" \
  --output "${JOB_JSON}" \
  --cluster-id "${CLUSTER_ID}" \
  --notebook-path "${NOTEBOOK_PATH}" \
  --dbfs-config-path "${DBFS_CONFIG}" \
  --hierarchy-config-path "${DBFS_HIERARCHY}" \
  --topic-remap-path "${DBFS_REMAP}"

echo "Submitting post-enrichment analysis job on existing cluster ${CLUSTER_ID}."
"${DATABRICKS_CMD[@]}" jobs submit --json "@${JOB_JSON}" --timeout "${TIMEOUT}" --output json
