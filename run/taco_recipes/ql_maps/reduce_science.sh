#!/usr/bin/env bash
# reduce_science.sh — v3 quick-look science map reduction.
#
# Usage: reduce_science.sh <obsnum>
#
# Required env vars:
#   TOLTECA_DATA_LMT_ROOT   — root of data_lmt (raw NC + tel files)
#   TOLTECA_DATAPROD_ROOT   — output root for reduced products
#   TOLTECA_APT_FILE        — path to apt.ecsv
#   CITLALI_BIN             — path to citlali binary
#
# Optional env vars:
#   TOLTECA_RCDIR           — path to tolteca run config dir with science.yaml
#                             (defaults to same dir as this script)
#
# Outputs to: ${TOLTECA_DATAPROD_ROOT}/${obsnum}/
#   toltec_a1100_science_<obsnum>.fits
#   toltec_a1400_science_<obsnum>.fits
#   toltec_a2000_science_<obsnum>.fits

set -euo pipefail

SCRIPTDIR="$(dirname "$(readlink -f "$0")")"
OBSNUM="${1:?Usage: reduce_science.sh <obsnum>}"
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

echo "[toltecpipe] reduce_science.sh obsnum=${OBSNUM}"
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
for f in "${DATA_LMT}"/toltec_clip{a,o}/reduced/toltec*_"${OBSNUM_STR}"_*.txt; do
    [[ -e "$f" ]] && ln -sf "$f" "${WORKDIR}/"
done
for f in "${DATA_LMT}"/toltec_clip{a,o}/reduced/toltec*_"${TUNE_OBSNUM_STR}"_*.txt; do
    [[ -e "$f" ]] && ln -sf "$f" "${WORKDIR}/"
done

# --- Step 2: run citlali (no APT matching for science) ---
echo "[toltecpipe] Running citlali (science) for obsnum=${OBSNUM}..."
CITLALI_OUTDIR="${OUTDIR}/citlali"
mkdir -p "${CITLALI_OUTDIR}"

"${CITLALI}" \
    --config "${RCDIR}/science.yaml" \
    --input_dir "${WORKDIR}" \
    --output_dir "${CITLALI_OUTDIR}" \
    --obsnum "${OBSNUM}" \
    --apt "${APT_IN}" \
    2>&1 | tee "${OUTDIR}/citlali.log"

echo "[toltecpipe] reduce_science.sh done → ${OUTDIR}"
