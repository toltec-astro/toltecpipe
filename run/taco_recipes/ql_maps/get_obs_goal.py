#!/usr/bin/env python3
"""Read ObsGoal from the tel NC file and print to stdout.

Usage:
    python get_obs_goal.py <obsnum>

Prints one of: pointing / focus / astigmatism / oof / beammap /
               azscan / elscan / science / (empty string on failure)

Exits 0 on success (including "unknown" goals), 1 only on hard errors.

Required env vars:
    TOLTECA_DATA_LMT_ROOT   — root of data_lmt filesystem (default /data_lmt)
"""

from __future__ import annotations

import glob
import os
import sys


def get_obs_goal(obsnum: int, data_lmt_root: str) -> str | None:
    """Read ObsGoal from tel NC file for obsnum.

    Parameters
    ----------
    obsnum : int
        Observation number.
    data_lmt_root : str
        Root of data_lmt filesystem.

    Returns
    -------
    str | None
        Lower-case obs_goal string, or None if no tel file found.
    """
    pattern = f"{data_lmt_root}/tel/tel_toltec*_{obsnum:06d}_*.nc"
    matches = sorted(glob.glob(pattern))
    if not matches:
        return None
    tel_file = matches[0]
    try:
        import netCDF4 as nc  # noqa: N813
        with nc.Dataset(tel_file) as ds:
            goal = ds["Data"]["TelescopeBackend"]["TelBamObsGoal"][0]
            if hasattr(goal, "data"):
                goal = goal.data
            return str(goal).strip().lower()
    except Exception:  # noqa: BLE001
        return None


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: get_obs_goal.py <obsnum>", file=sys.stderr)
        sys.exit(1)

    obsnum = int(sys.argv[1])
    data_lmt_root = os.getenv("TOLTECA_DATA_LMT_ROOT", "/data_lmt")

    obs_goal = get_obs_goal(obsnum, data_lmt_root)
    if obs_goal is None:
        # No tel file found — print empty and exit 0 (not a hard error;
        # caller checks for empty output)
        print("")
    else:
        print(obs_goal)
