#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-matt.hindman@researchaccelerator.org}"
DATABRICKS_AUTH_STORAGE="${DATABRICKS_AUTH_STORAGE:-plaintext}"
ANALYSIS_MODE="${ANALYSIS_MODE:-full}"
if [[ "${ANALYSIS_MODE}" == "attention_pps" ]]; then
  DEFAULT_DBFS_EXPORT_ROOT="dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1/treemap_publication_pps_attention"
  DEFAULT_LOCAL_EXPORT_DIR="${ROOT_DIR}/outputs/full_corpus_dual_sample_20260717_v1/treemap_publication_pps_attention_input"
  DEFAULT_OUTPUT_DIR="${ROOT_DIR}/outputs/full_corpus_dual_sample_20260717_v1/weighted_treemaps_pps_attention"
  DEFAULT_ARTIFACT_TAG="pps_attention_20260721_v3"
  DEFAULT_MEASURE="attention"
elif [[ "${ANALYSIS_MODE}" == "full" ]]; then
  DEFAULT_DBFS_EXPORT_ROOT="dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1/treemap_publication"
  DEFAULT_LOCAL_EXPORT_DIR="${ROOT_DIR}/outputs/full_corpus_dual_sample_20260717_v1/treemap_publication_input"
  DEFAULT_OUTPUT_DIR="${ROOT_DIR}/outputs/full_corpus_dual_sample_20260717_v1/weighted_treemaps"
  DEFAULT_ARTIFACT_TAG="full_frame_weighted_v1"
  DEFAULT_MEASURE="both"
else
  echo "Unknown ANALYSIS_MODE: ${ANALYSIS_MODE}" >&2
  exit 2
fi
DBFS_EXPORT_ROOT="${DBFS_EXPORT_ROOT:-${DEFAULT_DBFS_EXPORT_ROOT}}"
LOCAL_EXPORT_DIR="${LOCAL_EXPORT_DIR:-${DEFAULT_LOCAL_EXPORT_DIR}}"
OUTPUT_DIR="${OUTPUT_DIR:-${DEFAULT_OUTPUT_DIR}}"
ARTIFACT_TAG="${ARTIFACT_TAG:-${DEFAULT_ARTIFACT_TAG}}"
MEASURE="${MEASURE:-${DEFAULT_MEASURE}}"
STATIC_CELL_CAP="${STATIC_CELL_CAP:-250}"
LEAF_MIN_FRAC="${LEAF_MIN_FRAC:-0.003}"

if [[ "${DATABRICKS_PROFILE}" != "matt.hindman@researchaccelerator.org" ]]; then
  echo "Refusing unregistered Databricks profile: ${DATABRICKS_PROFILE}" >&2
  exit 2
fi

DATABRICKS_CMD=(env "DATABRICKS_AUTH_STORAGE=${DATABRICKS_AUTH_STORAGE}" databricks -p "${DATABRICKS_PROFILE}")
mkdir -p "${LOCAL_EXPORT_DIR}" "${OUTPUT_DIR}"

"${DATABRICKS_CMD[@]}" fs cp -r "${DBFS_EXPORT_ROOT}/treemap_cells" \
  "${LOCAL_EXPORT_DIR}/treemap_cells" --overwrite
"${DATABRICKS_CMD[@]}" fs cp -r "${DBFS_EXPORT_ROOT}/publication_estimates" \
  "${LOCAL_EXPORT_DIR}/publication_estimates" --overwrite
"${DATABRICKS_CMD[@]}" fs cp "${DBFS_EXPORT_ROOT}/run_manifest.json" \
  "${LOCAL_EXPORT_DIR}/run_manifest.json" --overwrite

MPLBACKEND=Agg MPLCONFIGDIR="${TMPDIR:-/tmp}/treemap-mpl" \
python3 "${ROOT_DIR}/scripts/render_full_corpus_weighted_treemaps.py" \
  --cells "${LOCAL_EXPORT_DIR}/treemap_cells" \
  --publication-estimates "${LOCAL_EXPORT_DIR}/publication_estimates" \
  --manifest "${LOCAL_EXPORT_DIR}/run_manifest.json" \
  --output-dir "${OUTPUT_DIR}" \
  --artifact-tag "${ARTIFACT_TAG}" \
  --static-cell-cap "${STATIC_CELL_CAP}" \
  --leaf-min-frac "${LEAF_MIN_FRAC}" \
  --measure "${MEASURE}"

MPLBACKEND=Agg MPLCONFIGDIR="${TMPDIR:-/tmp}/treemap-mpl" \
python3 "${ROOT_DIR}/scripts/render_full_corpus_expansion_changes.py" \
  --cells "${LOCAL_EXPORT_DIR}/treemap_cells" \
  --publication-estimates "${LOCAL_EXPORT_DIR}/publication_estimates" \
  --manifest "${LOCAL_EXPORT_DIR}/run_manifest.json" \
  --output-dir "${OUTPUT_DIR}" \
  --artifact-tag "${ARTIFACT_TAG}"

echo "Weighted treemap artifacts written under ${OUTPUT_DIR}"
