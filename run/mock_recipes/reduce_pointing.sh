#!/usr/bin/env bash
# Mock reduce_pointing.sh — simulate a pointing/focus/oof/astig reduction.
#
# Usage: reduce_pointing.sh <obsnum>
#
# Required env vars (set by RecipesResource or manually):
#   TOLTECA_DATA_LMT_ROOT   — root of data_lmt
#   TOLTECA_DATAPROD_ROOT   — output root
#   TOLTECA_APT_FILE        — path to apt.ecsv
#   CITLALI_BIN             — path to citlali binary
#
# In production, replace with the real reduce_pointing.sh from taco_recipes.

set -euo pipefail

OBSNUM="${1:?Usage: reduce_pointing.sh <obsnum>}"

DATA_LMT="${TOLTECA_DATA_LMT_ROOT:-/data_lmt}"
DATAPROD="${TOLTECA_DATAPROD_ROOT:-/tmp/dataprod_toltec}"
APT_FILE="${TOLTECA_APT_FILE:-common/apt.ecsv}"
CITLALI="${CITLALI_BIN:-/usr/local/bin/citlali}"

echo "[mock] reduce_pointing.sh obsnum=${OBSNUM}"
echo "[mock]   DATA_LMT_ROOT = ${DATA_LMT}"
echo "[mock]   DATAPROD_ROOT = ${DATAPROD}"
echo "[mock]   APT_FILE      = ${APT_FILE}"
echo "[mock]   CITLALI_BIN   = ${CITLALI}"

OUTDIR="${DATAPROD}/${OBSNUM}"
mkdir -p "${OUTDIR}"

# Simulate APT matching
echo "[mock] Matching APT for obsnum=${OBSNUM}..."
sleep 0.2

# Simulate citlali run
echo "[mock] Running citlali (pointing) for obsnum=${OBSNUM}..."
sleep 0.5

# Write mock output files
for ARRAY in a1100 a1400 a2000; do
    touch "${OUTDIR}/toltec_${ARRAY}_pointing_${OBSNUM}_params.txt"
    touch "${OUTDIR}/toltec_${ARRAY}_pointing_${OBSNUM}.png"
    echo '{"dx_arcsec": 1.2, "dy_arcsec": -0.8, "fwhm_arcsec": 5.5}' \
        > "${OUTDIR}/toltec_${ARRAY}_pointing_${OBSNUM}_params.txt"
done
touch "${OUTDIR}/lmt_tcs_quicklook_summary_v.png"

echo "[mock] reduce_pointing.sh done → ${OUTDIR}"
