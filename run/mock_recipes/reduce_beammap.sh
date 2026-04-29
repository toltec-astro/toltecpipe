#!/usr/bin/env bash
# Mock reduce_beammap.sh — simulate a beammap/azscan/elscan reduction.
#
# Usage: reduce_beammap.sh <obsnum>
# See reduce_pointing.sh for env var documentation.

set -euo pipefail

OBSNUM="${1:?Usage: reduce_beammap.sh <obsnum>}"
DATAPROD="${TOLTECA_DATAPROD_ROOT:-/tmp/dataprod_toltec}"

echo "[mock] reduce_beammap.sh obsnum=${OBSNUM}"
sleep 0.5

OUTDIR="${DATAPROD}/${OBSNUM}"
mkdir -p "${OUTDIR}"

touch "${OUTDIR}/toltec_beammap_${OBSNUM}_image.png"
touch "${OUTDIR}/toltec_beammap_${OBSNUM}.fits"

echo "[mock] reduce_beammap.sh done → ${OUTDIR}"
