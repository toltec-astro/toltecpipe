#!/usr/bin/env bash
# reduce_pointing.sh — v3 quick-look pointing/focus/oof/astigmatism reduction.
#
# Usage: reduce_pointing.sh <obsnum>
#
# Required env vars:
#   TOLTECA_DATA_LMT_ROOT   — root of data_lmt (raw NC + tel files)
#   TOLTECA_DATAPROD_ROOT   — output root for reduced products
#   TOLTECA_APT_FILE        — path to apt.ecsv
#   CITLALI_BIN             — path to citlali binary
#
# Optional env vars:
#   TOLTECA_RCDIR           — path to tolteca run config dir with pointing.yaml
#                             (defaults to same dir as this script)
#
# Outputs to: ${TOLTECA_DATAPROD_ROOT}/${obsnum}/
#   toltec_a1100_pointing_<obsnum>_params.txt  (JSON)
#   toltec_a1400_pointing_<obsnum>_params.txt
#   toltec_a2000_pointing_<obsnum>_params.txt
#   toltec_a1100_pointing_<obsnum>.png
#   toltec_a1400_pointing_<obsnum>.png
#   toltec_a2000_pointing_<obsnum>.png
#   pointing_params.json   (merged summary of all arrays)

set -euo pipefail

SCRIPTDIR="$(dirname "$(readlink -f "$0")")"
OBSNUM="${1:?Usage: reduce_pointing.sh <obsnum>}"
DATA_LMT="${TOLTECA_DATA_LMT_ROOT:-/data_lmt}"
DATAPROD="${TOLTECA_DATAPROD_ROOT:-/tmp/dataprod_toltec}"
APT_IN="${TOLTECA_APT_FILE:-common/apt.ecsv}"
CITLALI="${CITLALI_BIN:-/usr/local/bin/citlali}"
RCDIR="${TOLTECA_RCDIR:-${SCRIPTDIR}}"

OBSNUM_STR=$(printf "%06d" "${OBSNUM}")
TUNE_OBSNUM=$(( OBSNUM - 1 ))
TUNE_OBSNUM_STR=$(printf "%06d" "${TUNE_OBSNUM}")

OUTDIR="${DATAPROD}/${OBSNUM}"
WORKDIR="${OUTDIR}/_work"
mkdir -p "${WORKDIR}"

echo "[toltecpipe] reduce_pointing.sh obsnum=${OBSNUM}"
echo "[toltecpipe]   DATA_LMT_ROOT = ${DATA_LMT}"
echo "[toltecpipe]   DATAPROD_ROOT = ${DATAPROD}"
echo "[toltecpipe]   APT_FILE      = ${APT_IN}"
echo "[toltecpipe]   CITLALI_BIN   = ${CITLALI}"
echo "[toltecpipe]   OUTDIR        = ${OUTDIR}"

# --- Step 1: symlink raw NC files into workdir ---
echo "[toltecpipe] Symlinking raw files..."
TEL_FILE=$(ls "${DATA_LMT}/tel/tel_toltec_"*"_${OBSNUM_STR}_"*".nc" 2>/dev/null | head -1 || true)
if [[ -n "${TEL_FILE}" ]]; then
    ln -sf "${TEL_FILE}" "${WORKDIR}/"
fi
for f in "${DATA_LMT}"/toltec/tcs/toltec*/toltec*_"${OBSNUM_STR}"_*.nc; do
    [[ -e "$f" ]] && ln -sf "$f" "${WORKDIR}/"
done
# KIDs reduced clip files (if available)
for f in "${DATA_LMT}"/toltec_clip{a,o}/reduced/toltec*_"${OBSNUM_STR}"_*.txt; do
    [[ -e "$f" ]] && ln -sf "$f" "${WORKDIR}/"
done
for f in "${DATA_LMT}"/toltec_clip{a,o}/reduced/toltec*_"${TUNE_OBSNUM_STR}"_*.txt; do
    [[ -e "$f" ]] && ln -sf "$f" "${WORKDIR}/"
done

# --- Step 2: match APT ---
echo "[toltecpipe] Matching APT for obsnum=${OBSNUM}..."
APT_MATCHED="${WORKDIR}/apt_${OBSNUM}_matched.ecsv"
python3 "${SCRIPTDIR}/make_matched_apt.py" \
    --data_rootpath "${DATA_LMT}" \
    --apt_in_file "${APT_IN}" \
    --output_dir "${WORKDIR}" \
    -- "${OBSNUM}"

# --- Step 3: run citlali ---
echo "[toltecpipe] Running citlali for obsnum=${OBSNUM}..."
CITLALI_OUTDIR="${OUTDIR}/citlali"
mkdir -p "${CITLALI_OUTDIR}"

"${CITLALI}" \
    --config "${RCDIR}/pointing.yaml" \
    --input_dir "${WORKDIR}" \
    --output_dir "${CITLALI_OUTDIR}" \
    --obsnum "${OBSNUM}" \
    --apt "${APT_MATCHED}" \
    2>&1 | tee "${OUTDIR}/citlali.log"

# Find the latest redu?? subdir (citlali may write redu00, redu01, etc.)
REDU_DIR=$(python3 -c "
import sys
from pathlib import Path
dirs = sorted(Path('${CITLALI_OUTDIR}').glob('redu??'), key=lambda x: int(x.name[4:]))
print(dirs[-1] if dirs else '${CITLALI_OUTDIR}')
" 2>/dev/null || echo "${CITLALI_OUTDIR}")

echo "[toltecpipe] citlali output dir: ${REDU_DIR}"

# --- Step 4: run pointing_reader ---
echo "[toltecpipe] Running pointing_reader for obsnum=${OBSNUM}..."
python3 "${SCRIPTDIR}/pointing_reader.py" \
    --input_path "${REDU_DIR}/${OBSNUM}/raw" \
    --obsnum "${OBSNUM}" \
    --output_dir "${OUTDIR}" \
    --save_to_file

echo "[toltecpipe] reduce_pointing.sh done → ${OUTDIR}"
