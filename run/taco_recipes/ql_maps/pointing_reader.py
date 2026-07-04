#!/usr/bin/env python3
"""Pointing reader v3 — extract pointing parameters from citlali output.

Reads the per-array pointing ecsv table and FITS images produced by citlali,
generates per-array PNG diagnostic images, and writes a JSON params summary.

Usage:
    python pointing_reader.py --input_path <dir> --obsnum <obsnum>
                              --output_dir <outdir> [--save_to_file]

Input directory should contain:
    ppt*pointing*<obsnum_padded>*.ecsv   — per-array pointing table from citlali
    toltec*a1100*pointing*.fits          — a1100 signal/weight FITS cube
    toltec*a1400*pointing*.fits
    toltec*a2000*pointing*.fits

Outputs (when --save_to_file):
    <output_dir>/toltec_a1100_pointing_<obsnum>_params.txt  (JSON, one array)
    <output_dir>/toltec_a1400_pointing_<obsnum>_params.txt
    <output_dir>/toltec_a2000_pointing_<obsnum>_params.txt
    <output_dir>/toltec_a1100_pointing_<obsnum>.png
    <output_dir>/toltec_a1400_pointing_<obsnum>.png
    <output_dir>/toltec_a2000_pointing_<obsnum>.png
    <output_dir>/pointing_params.json   (merged summary, all arrays)

Ported from pointing_reader_v1_5.py.  Key changes from v1_5:
  - Writes ``pointing_params.json`` with per-array dict (used by ingest_ql_result)
  - No dependency on v2 tolteca Python package
  - CLI flags renamed for consistency with v3 conventions
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend

import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
from astropy.modeling import fitting, models
from astropy.nddata.utils import Cutout2D
from astropy.table import Table
from astropy.wcs import WCS


ARRAYS = ["a1100", "a1400", "a2000"]
ARRAY_COLORS = {"a1100": "cyan", "a1400": "yellow", "a2000": "red"}
PARAM_NAMES = ["amp", "x_t", "y_t", "a_fwhm", "b_fwhm", "angle"]
PARAM_UNITS = ["mJy/beam", "arcsec", "arcsec", "arcsec", "arcsec", "rad"]


def _save_text_figure(filepath: str, text: str, **kwargs) -> None:
    """Write a small matplotlib figure containing only *text* to *filepath*."""
    fig, ax = plt.subplots(figsize=(4, 1))
    ax.text(0.5, 0.5, text, transform=ax.transAxes, ha="center", va="center", **kwargs)
    ax.get_xaxis().set_ticks([])
    ax.get_yaxis().set_ticks([])
    fig.savefig(filepath, bbox_inches="tight")
    plt.close(fig)


def read_pointing_table(input_path: str, obsnum: str) -> Table | None:
    """Load the citlali pointing ecsv table for *obsnum*.

    Parameters
    ----------
    input_path : str
        Directory containing citlali output.
    obsnum : str
        Observation number as string.

    Returns
    -------
    Table | None
        Astropy Table, or None if not found.
    """
    if obsnum == "none":
        candidates = glob.glob(f"{input_path}/ppt*pointing*.ecsv")
    else:
        obsnum_padded = str(obsnum).zfill(6)
        candidates = glob.glob(f"{input_path}/ppt*pointing*{obsnum_padded}*.ecsv")
    if not candidates:
        return None
    return Table.read(sorted(candidates)[0])


def process_array(
    array: str,
    fits_file: str,
    table: Table,
    array_row_index: int,
    obsnum: str,
    output_dir: str,
    save: bool,
    center_mode: str = "crpix",
) -> dict:
    """Process one array's FITS file and return its pointing params dict.

    Parameters
    ----------
    array : str
        Array name (``a1100``, ``a1400``, or ``a2000``).
    fits_file : str
        Path to citlali FITS file for this array.
    table : Table
        Full pointing table from citlali (all arrays).
    array_row_index : int
        Row index in *table* for this array.
    obsnum : str
        Observation number as string (for file naming).
    output_dir : str
        Directory where PNG and params.txt are written.
    save : bool
        Whether to write files.
    center_mode : str
        Image centering mode: ``crpix`` (default), ``fit``, or ``peak``.

    Returns
    -------
    dict
        Pointing parameters for this array with keys:
        dx_arcsec, dy_arcsec, fwhm_a_arcsec, fwhm_b_arcsec, amp_mJy, angle_rad,
        plus _err variants and _units.
    """
    i = array_row_index
    params_dict: dict = {"array": array}
    for param, unit in zip(PARAM_NAMES, PARAM_UNITS):
        params_dict[param] = {
            "value": float(table[param][i]),
            "error": float(table[f"{param}_err"][i]),
            "units": unit,
        }

    # Convenience aliases used by ingest_ql_result
    params_dict["dx_arcsec"] = params_dict["x_t"]["value"]
    params_dict["dy_arcsec"] = params_dict["y_t"]["value"]
    params_dict["fwhm_a_arcsec"] = params_dict["a_fwhm"]["value"]
    params_dict["fwhm_b_arcsec"] = params_dict["b_fwhm"]["value"]
    params_dict["amp_mJy"] = params_dict["amp"]["value"]

    if save:
        # Write per-array params JSON
        params_path = Path(output_dir) / f"toltec_{array}_pointing_{obsnum}_params.txt"
        params_path.write_text(json.dumps(params_dict, indent=2))

    try:
        img = fits.open(fits_file)
        if len(img) < 2:
            print(f"[pointing_reader] incomplete FITS for {array}, skipping plot")
            img.close()
            if save:
                _save_text_figure(
                    str(Path(output_dir) / f"toltec_{array}_pointing_{obsnum}.png"),
                    f"{array}\nFAILED: reduction failed.",
                    color="red",
                )
            return params_dict

        wcs = WCS(img[1].header).sub(2)
        pix_scale = abs(wcs.wcs.cdelt[0])
        crpix1, crpix2 = wcs.wcs.crpix
        zoom_size_pix = int(np.floor(101.0 / pix_scale))

        data_2d = img[img.index_of("signal_I")].data[0, 0, :, :]

        if center_mode == "crpix":
            center = (crpix1, crpix2)
        elif center_mode == "fit":
            cx, cy = wcs.all_world2pix(params_dict["x_t"]["value"], params_dict["y_t"]["value"], 0)
            center = (cx, cy)
        elif center_mode == "peak":
            sn_data = img[img.index_of("sig2noise_I")].data[0, 0, :, :]
            y, x = np.unravel_index(np.nanargmax(sn_data), sn_data.shape)
            center = (x, y)
        else:
            center = (crpix1, crpix2)

        cutout = Cutout2D(data_2d, center, (zoom_size_pix, zoom_size_pix), wcs=wcs)

        if save:
            fig, ax = plt.subplots(
                figsize=(10, 10),
                subplot_kw={"projection": cutout.wcs},
            )
            im = ax.imshow(cutout.data, origin="lower", cmap="viridis")
            for spine in ax.spines.values():
                spine.set_color(ARRAY_COLORS[array])
                spine.set_linewidth(3)
            ax.set_title(
                f"{array} | obsnum={obsnum} | "
                f"dx={params_dict['dx_arcsec']:.1f}\" "
                f"dy={params_dict['dy_arcsec']:.1f}\" "
                f"FWHM={params_dict['fwhm_a_arcsec']:.1f}\"",
                fontsize=12,
            )
            ax.coords[0].set_axislabel("Azimuth (arcsec)")
            ax.coords[1].set_axislabel("Elevation (arcsec)")
            fig.colorbar(im, ax=ax, label="mJy/beam")
            fig.savefig(
                str(Path(output_dir) / f"toltec_{array}_pointing_{obsnum}.png"),
                bbox_inches="tight",
            )
            plt.close(fig)

        img.close()
    except Exception as exc:  # noqa: BLE001
        print(f"[pointing_reader] warning: could not plot {array}: {exc}")
        if save:
            _save_text_figure(
                str(Path(output_dir) / f"toltec_{array}_pointing_{obsnum}.png"),
                f"{array}\nFAILED: {exc}",
                color="red",
            )

    return params_dict


def run(
    input_path: str,
    obsnum: str,
    output_dir: str,
    save: bool,
    center_mode: str = "crpix",
) -> dict:
    """Run the pointing reader on all arrays.

    Parameters
    ----------
    input_path : str
        Directory with citlali output.
    obsnum : str
        Observation number as string.
    output_dir : str
        Directory where outputs are written.
    save : bool
        Whether to write PNG and params files.
    center_mode : str
        Image centering mode.

    Returns
    -------
    dict
        Summary dict with per-array pointing params, image paths, status.
    """
    table = read_pointing_table(input_path, obsnum)
    if table is None:
        print(f"[pointing_reader] no pointing table found in {input_path}")
        if save:
            for array in ARRAYS:
                _save_text_figure(
                    str(Path(output_dir) / f"toltec_{array}_pointing_{obsnum}_reader_failed.png"),
                    f"{array}\nFAILED: no pointing table",
                    color="red",
                )
        return {"status": "failed", "reason": "no pointing table found"}

    obsnum_padded = str(obsnum).zfill(6)
    fits_files_all = glob.glob(f"{input_path}/toltec*pointing*{obsnum_padded}*_citlali.fits")
    # Build mapping: array → fits file
    fits_by_array = {}
    for f in fits_files_all:
        for array in ARRAYS:
            if array in f:
                fits_by_array[array] = f
                break

    arrays_result: dict = {}
    for row_idx, row in enumerate(table):
        array = None
        for a in ARRAYS:
            if a in str(fits_files_all[row_idx] if row_idx < len(fits_files_all) else ""):
                array = a
                break
        if array is None:
            # Try to detect array from row metadata (citlali puts 'array_name' column)
            if "array_name" in table.colnames:
                array = str(row["array_name"]).lower()
            elif row_idx < len(ARRAYS):
                array = ARRAYS[row_idx]
            else:
                continue

        fits_file = fits_by_array.get(array, "")
        if not fits_file:
            # Find by array name glob
            candidates = glob.glob(f"{input_path}/toltec*{array}*pointing*.fits")
            fits_file = sorted(candidates)[0] if candidates else ""

        if not fits_file:
            print(f"[pointing_reader] no FITS file for {array}, skipping plot")
            arrays_result[array] = {"array": array, "status": "no_fits"}
            continue

        result = process_array(array, fits_file, table, row_idx, obsnum, output_dir, save, center_mode)
        arrays_result[array] = result

    summary = {
        "obsnum": int(obsnum) if str(obsnum).isdigit() else obsnum,
        "obs_goal": "pointing",
        "status": "complete",
        "arrays": arrays_result,
        "image_paths": [
            f"toltec_{a}_pointing_{obsnum}.png"
            for a in ARRAYS
            if (Path(output_dir) / f"toltec_{a}_pointing_{obsnum}.png").exists()
        ],
        "reduced_at": datetime.now(timezone.utc).isoformat(),
    }

    if save:
        summary_path = Path(output_dir) / "pointing_params.json"
        summary_path.write_text(json.dumps(summary, indent=2))
        print(f"[pointing_reader] wrote {summary_path}")

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="v3 pointing reader")
    parser.add_argument("--input_path", "-p", required=True, help="citlali output directory")
    parser.add_argument("--obsnum", "-n", default="none", help="observation number")
    parser.add_argument("--output_dir", "-o", default=None, help="output directory (default: input_path)")
    parser.add_argument("--save_to_file", "-s", action="store_true", help="write PNG and params files")
    parser.add_argument("--center", "-c", default="crpix", choices=["crpix", "fit", "peak"],
                        help="image centering mode")
    args = parser.parse_args()

    out_dir = args.output_dir or args.input_path
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    result = run(
        input_path=args.input_path,
        obsnum=args.obsnum,
        output_dir=out_dir,
        save=args.save_to_file,
        center_mode=args.center,
    )
    print(json.dumps(result, indent=2))
    if result.get("status") == "failed":
        sys.exit(1)
