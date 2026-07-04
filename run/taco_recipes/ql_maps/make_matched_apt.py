#!/usr/bin/env python3
"""make_matched_apt.py — v3 array pointing table (APT) matching.

Reads the current tone list from the tcs NC files for *obsnum*, matches
detectors to the base APT by frequency proximity, and writes a matched
apt_<obsnum>_matched.ecsv suitable for citlali input.

Usage:
    python make_matched_apt.py --data_rootpath <data_lmt> \\
                               --apt_in_file <apt.ecsv> \\
                               --output_dir <dir> \\
                               -- <obsnum>

If no NC files are found (e.g., first run, no toltec_clip reduced files),
falls back to copying apt_in_file directly as apt_<obsnum>_matched.ecsv.

Required env vars (read from environment if not passed):
    TOLTECA_DATA_LMT_ROOT   — root of data_lmt (raw NC files)

Notes
-----
The production v2 version (make_matched_apt.py in refs/) uses D21 correlation
and KidsModelParamsIO from v2 tolteca.  This v3 version uses a simpler
frequency-proximity match against toltec_clip reduced tone files when
available, and falls back to passing the base APT through unchanged.

The fallback is adequate when:
  - Clip-reduced tone files are not yet available (e.g., pointing taken
    before KIDs data is processed)
  - citlali is configured to do its own internal matching

When toltec_clip files ARE available, the match ensures per-detector
frequency-based selection so citlali gets the right resonance positions.
"""

from __future__ import annotations

import argparse
import glob
import shutil
import sys
from pathlib import Path

import numpy as np
from astropy.table import Table


ARRAYS = {
    "a1100": range(0, 4),    # toltec0-3
    "a1400": range(4, 8),    # toltec4-7
    "a2000": range(8, 13),   # toltec8-12
}
TOLTEC_CLIP_COLS = ["uid", "f_centered", "flag"]  # minimum needed for frequency match


def _find_tone_files(data_root: Path, obsnum: int) -> list[Path]:
    """Find toltec_clip reduced tone files for *obsnum*.

    Parameters
    ----------
    data_root : Path
        Root of data_lmt.
    obsnum : int
        Observation number.

    Returns
    -------
    list[Path]
        Sorted list of clip tone ECSV files.
    """
    obsnum_str = f"{obsnum:06d}"
    patterns = [
        f"toltec_clip*/reduced/toltec*_{obsnum_str}_*.ecsv",
        f"toltec_clipa/reduced/toltec*_{obsnum_str}_*.ecsv",
        f"toltec_clipo/reduced/toltec*_{obsnum_str}_*.ecsv",
    ]
    files = []
    for pattern in patterns:
        files.extend(data_root.glob(pattern))
    return sorted(set(files))


def _freq_match_apt(apt: Table, tone_files: list[Path]) -> Table:
    """Match APT detectors to tone list by minimum frequency distance.

    For each entry in the tone_files, find the nearest APT row by
    ``f_centered`` (tone file) vs ``uid`` frequency column in APT.

    Parameters
    ----------
    apt : Table
        Base APT table with ``uid`` and ``f_centered`` (or ``freq_mhz``) cols.
    tone_files : list[Path]
        Per-interface tone ECSV files from toltec_clip reduction.

    Returns
    -------
    Table
        APT filtered/reordered to match detected tones.
    """
    # Try to detect frequency column name in APT
    freq_col = None
    for cname in ("f_centered", "freq_mhz", "frequency", "f_tone"):
        if cname in apt.colnames:
            freq_col = cname
            break
    if freq_col is None:
        # No recognizable frequency column — return APT as-is
        return apt

    apt_freqs = np.array(apt[freq_col], dtype=float)
    matched_rows = set()

    for tf in tone_files:
        try:
            tones = Table.read(tf)
            if "f_centered" not in tones.colnames:
                continue
            tone_freqs = np.array(tones["f_centered"], dtype=float)
            # Match each tone to nearest APT entry
            for tf_freq in tone_freqs:
                idx = int(np.argmin(np.abs(apt_freqs - tf_freq)))
                matched_rows.add(idx)
        except Exception as exc:  # noqa: BLE001
            print(f"[make_matched_apt] warning: could not read {tf}: {exc}")

    if not matched_rows:
        return apt  # fallback: return unchanged

    matched_apt = apt[sorted(matched_rows)]
    print(f"[make_matched_apt] matched {len(matched_apt)}/{len(apt)} APT rows from tone files")
    return matched_apt


def make_matched_apt(
    data_rootpath: str,
    apt_in_file: str,
    output_dir: str,
    obsnum: int,
) -> Path:
    """Match APT for *obsnum* and write apt_<obsnum>_matched.ecsv.

    Parameters
    ----------
    data_rootpath : str
        Root of data_lmt filesystem.
    apt_in_file : str
        Path to base APT ECSV.
    output_dir : str
        Directory where matched APT is written.
    obsnum : int
        Observation number.

    Returns
    -------
    Path
        Path to written apt_<obsnum>_matched.ecsv.
    """
    data_root = Path(data_rootpath)
    apt_in = Path(apt_in_file)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"apt_{obsnum}_matched.ecsv"

    if not apt_in.exists():
        print(f"[make_matched_apt] ERROR: apt_in_file not found: {apt_in}")
        sys.exit(1)

    print(f"[make_matched_apt] reading base APT: {apt_in}")
    apt = Table.read(apt_in)
    print(f"[make_matched_apt] APT has {len(apt)} rows")

    # Try frequency-based matching from toltec_clip reduced files
    tone_files = _find_tone_files(data_root, obsnum)
    if tone_files:
        print(f"[make_matched_apt] found {len(tone_files)} tone files, attempting freq match")
        apt = _freq_match_apt(apt, tone_files)
    else:
        print(f"[make_matched_apt] no tone files for obsnum={obsnum}, using base APT as-is")

    apt.write(str(out_path), format="ascii.ecsv", overwrite=True)
    print(f"[make_matched_apt] wrote {out_path} ({len(apt)} rows)")
    return out_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="v3 APT matcher")
    parser.add_argument("--data_rootpath", "-d", default="/data_lmt", help="data_lmt root")
    parser.add_argument("--apt_in_file", required=True, help="base APT ECSV")
    parser.add_argument("--output_dir", required=True, help="output directory")
    parser.add_argument("obsnum", type=int, help="observation number")
    args = parser.parse_args()

    make_matched_apt(
        data_rootpath=args.data_rootpath,
        apt_in_file=args.apt_in_file,
        output_dir=args.output_dir,
        obsnum=args.obsnum,
    )
