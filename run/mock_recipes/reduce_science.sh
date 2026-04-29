#!/usr/bin/env bash
# Mock reduce_science.sh — simulate a science map reduction.
#
# Usage: reduce_science.sh <obsnum>

set -euo pipefail

OBSNUM="${1:?Usage: reduce_science.sh <obsnum>}"
DATAPROD="${TOLTECA_DATAPROD_ROOT:-/tmp/dataprod_toltec}"

echo "[mock] reduce_science.sh obsnum=${OBSNUM}"
sleep 1.0

OUTDIR="${DATAPROD}/${OBSNUM}"
mkdir -p "${OUTDIR}"

for ARRAY in a1100 a1400 a2000; do
    touch "${OUTDIR}/toltec_${ARRAY}_science_${OBSNUM}.fits"
done

echo "[mock] reduce_science.sh done → ${OUTDIR}"
