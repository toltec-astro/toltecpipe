#!/usr/bin/env bash
# setup_sim_dirs.sh — create simulator working directories under /tmp
#
# Run this once before starting the simulator:
#   bash setup_sim_dirs.sh
#
# Options:
#   --source-db PATH   Copy PATH as the toltec source SQLite (optional)
#   --obsnums LIST     Comma-separated obsnums to pre-create as mock NC stubs

set -euo pipefail

BASE=/tmp/toltecpipe_test
SOURCE_DB=""
OBSNUMS=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source-db) SOURCE_DB="$2"; shift 2 ;;
        --obsnums)   OBSNUMS="$2";   shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=== Setting up simulator directories under $BASE ==="

# --- directory layout ---
dirs=(
    "$BASE/data_lmt/tel"
    "$BASE/data_lmt/toltec0"
    "$BASE/dataprod_toltec"
    "$BASE/cache"
)
for d in "${dirs[@]}"; do
    mkdir -p "$d"
    echo "  created $d"
done

# --- source SQLite for tolteca_db simulator ---
if [[ -n "$SOURCE_DB" ]]; then
    cp "$SOURCE_DB" "$BASE/toltecdb_source.sqlite"
    echo "  copied source db → $BASE/toltecdb_source.sqlite"
else
    # Create minimal stub SQLite with toltec_r1 schema
    db_path="$BASE/toltecdb_source.sqlite"
    if [[ -f "$db_path" ]]; then
        echo "  source db already exists: $db_path"
    else
        # Use venv Python (has sqlite3); fall back to any python3 with it
        PYTHON=$(command -v python3 || true)
        for candidate in \
            "$(dirname "$0")/../.venv-devcontainer/bin/python" \
            "$(dirname "$0")/../.venv/bin/python" \
            "$PYTHON"; do
            if [[ -x "$candidate" ]] && "$candidate" -c "import sqlite3" 2>/dev/null; then
                PYTHON="$candidate"; break
            fi
        done
        "$PYTHON" - "$db_path" <<'PYEOF'
import sqlite3, sys
path = sys.argv[1]
conn = sqlite3.connect(path)
conn.execute("""
    CREATE TABLE IF NOT EXISTS toltec_r1 (
        RoachIndex  INTEGER,
        ObsNum      INTEGER,
        SubObsNum   INTEGER,
        ScanNum     INTEGER,
        Valid       INTEGER DEFAULT 0,
        FileName    TEXT,
        Master      TEXT DEFAULT 'tcs',
        DateTime    TEXT
    )
""")
conn.commit(); conn.close()
PYEOF
        echo "  created stub source db: $db_path"
    fi
fi

# --- mock tel NC stubs for obsnums ---
if [[ -n "$OBSNUMS" ]]; then
    IFS=',' read -ra NUMS <<< "$OBSNUMS"
    for obsnum in "${NUMS[@]}"; do
        obsnum="${obsnum// /}"  # trim spaces
        # Create zero-byte stub so glob in get_obs_goal_from_tel finds something
        # (helpers.py only reads ObsGoal; we skip tel entirely in simulator mode)
        stub="$BASE/data_lmt/tel/tel_toltec_$(printf '%06d' "$obsnum")_0001.nc"
        touch "$stub"
        echo "  created tel stub: $stub"
    done
fi

echo ""
echo "=== Done. Simulator directories ready. ==="
echo ""
echo "To start the toltecpipe simulator:"
echo "  cd $(dirname "$0")"
echo "  just sim"
echo ""
echo "To start both pipeline simulators (two-loc):"
echo "  just sim-two"
