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
ORCHESTRATOR_PATH="${WORKSPACE_DIR}/12_full_corpus_dual_sample_language_databricks"
LID_PATH="${WORKSPACE_DIR}/01_language_openlid_v3_databricks"
LLM_PATH="${WORKSPACE_DIR}/03_language_llm_panel_databricks"
HELPER_PATH="${WORKSPACE_DIR}/full_corpus_dual_sample_design"
LOCAL_CONFIG="${ROOT_DIR}/config/full_corpus_dual_sample_20260717_v1.json"
DBFS_CONFIG="${DBFS_CONFIG:-dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json}"
TIMEOUT="${TIMEOUT:-48h}"

JOB_JSON="$(mktemp "${TMPDIR:-/tmp}/full_corpus_dual_sample_language.XXXXXX.json")"
cleanup() {
  rm -f "${JOB_JSON}"
}
trap cleanup EXIT

DATABRICKS_CMD=(env "DATABRICKS_AUTH_STORAGE=${DATABRICKS_AUTH_STORAGE}" databricks -p "${DATABRICKS_PROFILE}")

"${DATABRICKS_CMD[@]}" workspace mkdirs "${WORKSPACE_DIR}"
"${DATABRICKS_CMD[@]}" workspace import "${HELPER_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/full_corpus_dual_sample_design.py" \
  --format SOURCE --language PYTHON --overwrite
"${DATABRICKS_CMD[@]}" workspace import "${ORCHESTRATOR_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/12_full_corpus_dual_sample_language_databricks.py" \
  --format SOURCE --language PYTHON --overwrite
"${DATABRICKS_CMD[@]}" workspace import "${LID_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/01_language_openlid_v3_databricks.py" \
  --format SOURCE --language PYTHON --overwrite
"${DATABRICKS_CMD[@]}" workspace import "${LLM_PATH}" \
  --file "${ROOT_DIR}/youtube_descriptive/src/03_language_llm_panel_databricks.py" \
  --format SOURCE --language PYTHON --overwrite
"${DATABRICKS_CMD[@]}" fs mkdir "$(dirname "${DBFS_CONFIG}")"
"${DATABRICKS_CMD[@]}" fs cp "${LOCAL_CONFIG}" "${DBFS_CONFIG}" --overwrite

python3 "${ROOT_DIR}/scripts/build_full_corpus_dual_sample_language_job.py" \
  --config "${LOCAL_CONFIG}" \
  --output "${JOB_JSON}" \
  --cluster-id "${CLUSTER_ID}" \
  --orchestrator-path "${ORCHESTRATOR_PATH}" \
  --lid-path "${LID_PATH}" \
  --llm-path "${LLM_PATH}" \
  --dbfs-config-path "${DBFS_CONFIG}"

echo "Submitting gated dual-LID and DeepSeek job on existing cluster ${CLUSTER_ID}."
"${DATABRICKS_CMD[@]}" jobs submit --json "@${JOB_JSON}" --timeout "${TIMEOUT}" --output json
