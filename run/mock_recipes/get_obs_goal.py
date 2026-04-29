#!/usr/bin/env python3
"""Mock get_obs_goal.py — print a fake obs_goal for local testing.

Usage:
    python get_obs_goal.py <obsnum>

Prints one of: pointing / focus / beammap / science
based on obsnum % 4 for deterministic test coverage.

In production, replace with the real script that reads
Data.TelescopeBackend.TelBamObsGoal from the tel NC file.
"""

import sys

GOALS = ["pointing", "focus", "beammap", "science"]

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: get_obs_goal.py <obsnum>", file=sys.stderr)
        sys.exit(1)
    obsnum = int(sys.argv[1])
    print(GOALS[obsnum % len(GOALS)])
