#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-matt.hindman@researchaccelerator.org}"
DATABRICKS_AUTH_STORAGE="${DATABRICKS_AUTH_STORAGE:-plaintext}"
DBFS_EXPORT_ROOT="${DBFS_EXPORT_ROOT:-dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1/treemap_publication}"
LOCAL_EXPORT_DIR="${LOCAL_EXPORT_DIR:-${ROOT_DIR}/outputs/full_corpus_dual_sample_20260717_v1/treemap_publication_input}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/outputs/full_corpus_dual_sample_20260717_v1/weighted_treemaps}"
ARTIFACT_TAG="${ARTIFACT_TAG:-full_frame_weighted_v1}"

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
  --measure both

echo "Weighted treemap artifacts written under ${OUTPUT_DIR}"
