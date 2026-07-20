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
EXPERIMENT_PATH="${WORKSPACE_DIR}/17_full_corpus_dual_sample_lid_cutoff_experiment_databricks"
LID_PATH="${WORKSPACE_DIR}/01_language_openlid_v3_databricks"
HELPER_PATH="${WORKSPACE_DIR}/full_corpus_dual_sample_design"
LOCAL_CONFIG="${ROOT_DIR}/config/full_corpus_dual_sample_20260717_v1.json"
DBFS_CONFIG="${DBFS_CONFIG:-dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json}"
TIMEOUT="${TIMEOUT:-48h}"

JOB_JSON="$(mktemp "${TMPDIR:-/tmp}/full_corpus_dual_sample_lid_cutoff.XXXXXX.json")"
cleanup() { rm -f "${JOB_JSON}"; }
trap cleanup EXIT

DATABRICKS_CMD=(env "DATABRICKS_AUTH_STORAGE=${DATABRICKS_AUTH_STORAGE}" databricks -p "${DATABRICKS_PROFILE}")
"${DATABRICKS_CMD[@]}" workspace mkdirs "${WORKSPACE_DIR}"
"${DATABRICKS_CMD[@]}" workspace import "${HELPER_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/full_corpus_dual_sample_design.py" \
  --format SOURCE --language PYTHON --overwrite
"${DATABRICKS_CMD[@]}" workspace import "${EXPERIMENT_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/17_full_corpus_dual_sample_lid_cutoff_experiment_databricks.py" \
  --format SOURCE --language PYTHON --overwrite
"${DATABRICKS_CMD[@]}" workspace import "${LID_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/01_language_openlid_v3_databricks.py" \
  --format SOURCE --language PYTHON --overwrite
"${DATABRICKS_CMD[@]}" fs mkdir "$(dirname "${DBFS_CONFIG}")"
"${DATABRICKS_CMD[@]}" fs cp "${LOCAL_CONFIG}" "${DBFS_CONFIG}" --overwrite

python3 "${ROOT_DIR}/scripts/build_full_corpus_dual_sample_lid_cutoff_job.py" \
  --config "${LOCAL_CONFIG}" \
  --output "${JOB_JSON}" \
  --cluster-id "${CLUSTER_ID}" \
  --experiment-path "${EXPERIMENT_PATH}" \
  --lid-path "${LID_PATH}" \
  --dbfs-config-path "${DBFS_CONFIG}"

echo "Submitting paired recent-video LID cutoff experiment on existing cluster ${CLUSTER_ID}."
"${DATABRICKS_CMD[@]}" jobs submit --json "@${JOB_JSON}" --timeout "${TIMEOUT}" --output json
