"""Dagster jobs for toltecpipe.

ql_map_job
    Three thin ops: get_obs_goal → run_ql_recipe → (done).
    Dagster wraps the taco_recipes shell script calls.

ingest_catalog_job
    One op: ingest_ql_result.
    Parses reduction output, writes ql_result.json summary, logs to Dagster.
    (Phase 3: will also write dp_ql_map DataProd to tolteca_db.)
"""

# NOTE: Cannot use `from __future__ import annotations` with Dagster.

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from dagster import Config, In, Nothing, OpExecutionContext, Out, job, op

__all__ = ["ingest_catalog_job", "ql_map_job"]


# ---------------------------------------------------------------------------
# ql_map_job ops
# ---------------------------------------------------------------------------


class GetObsGoalConfig(Config):
    """Configuration for get_obs_goal op."""

    master: str
    obsnum: int
    subobsnum: int = 0
    scannum: int = 1


class RunQlRecipeConfig(Config):
    """Configuration for run_ql_recipe op (forwarded from get_obs_goal)."""

    obsnum: int
    obs_goal: str


@op(
    name="get_obs_goal",
    required_resource_keys={"recipes"},
    out={"obsnum": Out(int), "obs_goal": Out(str)},
    description="Read obs_goal from tel NC file via get_obs_goal.py script.",
)
def get_obs_goal(context: OpExecutionContext, config: GetObsGoalConfig):
    """Determine the observation goal for this obsnum.

    Calls ``get_obs_goal.py <obsnum>`` and captures stdout.
    Skips if obs_goal is not reducible (yields Nothing for run_ql_recipe).

    Parameters
    ----------
    context : OpExecutionContext
        Dagster execution context.
    config : GetObsGoalConfig
        Quartet coordinates from sensor RunRequest.
    """
    from .helpers import get_obs_goal_from_tel

    recipes = context.resources.recipes
    obsnum = config.obsnum

    # Try fast Python path first, fall back to subprocess script
    obs_goal = get_obs_goal_from_tel(obsnum, recipes.data_lmt_root)

    if obs_goal is None:
        # Fall back to get_obs_goal.py script
        script = Path(recipes.recipes_root) / "get_obs_goal.py"
        if script.exists():
            import subprocess  # noqa: PLC0415

            result = subprocess.run(
                [sys.executable, str(script), str(obsnum)],
                env=recipes.env(),
                capture_output=True,
                text=True,
            )
            obs_goal = result.stdout.strip().lower() or None

    if obs_goal is None:
        context.log.warning(f"No tel file found for obsnum={obsnum}, skipping")
        return obsnum, "unknown"

    context.log.info(f"obsnum={obsnum} obs_goal={obs_goal!r}")
    return obsnum, obs_goal


_REDUCIBLE_GOALS = frozenset(
    {
        "pointing",
        "focus",
        "astigmatism",
        "oof",
        "beammap",
        "azscan",
        "elscan",
        "science",
    }
)


@op(
    name="run_ql_recipe",
    required_resource_keys={"recipes"},
    ins={"obsnum": In(int), "obs_goal": In(str)},
    out=Out(str, description="Path to output dataprod directory"),
    description="Call taco_recipe script for this obs_goal.",
)
def run_ql_recipe(
    context: OpExecutionContext,
    obsnum: int,
    obs_goal: str,
) -> str:
    """Dispatch the appropriate taco_recipes shell script.

    The op is a thin subprocess wrapper.  The same script can be run
    manually to reproduce what Dagster executed:

        bash run/taco_recipes/ql_maps/reduce_pointing.sh <obsnum>

    Parameters
    ----------
    context : OpExecutionContext
        Dagster execution context.
    obsnum : int
        Observation number.
    obs_goal : str
        Observation goal from get_obs_goal op.

    Returns
    -------
    str
        Path to the output dataprod directory.
    """
    recipes = context.resources.recipes

    if obs_goal not in _REDUCIBLE_GOALS:
        context.log.info(f"obs_goal={obs_goal!r} not reducible, nothing to do")
        return str(Path(recipes.dataprod_toltec_root) / str(obsnum))

    script = recipes.script_for_goal(obs_goal)
    context.log.info(f"Running: {script} {obsnum}")
    out_dir = recipes.run_script(script, obsnum, log=context.log)
    context.log.info(f"Recipe complete → {out_dir}")
    return str(out_dir)


@job(
    name="ql_map_job",
    description="Get obs_goal then dispatch taco_recipe script for QL reduction.",
)
def ql_map_job():
    """QL map reduction job: get_obs_goal → run_ql_recipe."""
    obsnum, obs_goal = get_obs_goal()
    run_ql_recipe(obsnum=obsnum, obs_goal=obs_goal)


# ---------------------------------------------------------------------------
# ingest_catalog_job ops
# ---------------------------------------------------------------------------


class IngestQlResultConfig(Config):
    """Configuration for ingest_ql_result op."""

    obsnum: int
    result_dir: str


def _parse_pointing_params(result_dir: Path, obsnum: int) -> dict:
    """Parse pointing parameters from a reduction output directory.

    Looks for:
    1. ``pointing_params.json`` — written by pointing_reader.py (preferred)
    2. ``toltec_*_pointing_*_params.txt`` — per-array JSON files

    Parameters
    ----------
    result_dir : Path
        Root of reduction output for this obsnum.
    obsnum : int
        Observation number.

    Returns
    -------
    dict
        Per-array pointing params dict, keyed by array name.
        Empty dict if no params files found.
    """
    # Prefer the merged summary JSON written by pointing_reader.py
    summary_path = result_dir / "pointing_params.json"
    if summary_path.exists():
        try:
            data = json.loads(summary_path.read_text())
            return data.get("arrays", {})
        except (json.JSONDecodeError, OSError):
            pass

    # Fall back to per-array params.txt files
    arrays: dict = {}
    for params_file in sorted(result_dir.rglob(f"*pointing_{obsnum}*_params.txt")):
        try:
            data = json.loads(params_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        array = data.get("array")
        if array is None:
            # Try to infer from filename (toltec_a1100_pointing_...)
            for a in ("a1100", "a1400", "a2000"):
                if a in params_file.name:
                    array = a
                    break
        if array:
            arrays[array] = data

    return arrays


def _infer_obs_goal(result_dir: Path, obsnum: int) -> str:
    """Infer obs_goal from filenames in result directory.

    Parameters
    ----------
    result_dir : Path
        Reduction output directory.
    obsnum : int
        Observation number.

    Returns
    -------
    str
        Inferred obs_goal, or ``"unknown"``.
    """
    for pattern, goal in [
        (f"*pointing*{obsnum}*", "pointing"),
        (f"*beammap*{obsnum}*", "beammap"),
        (f"*science*{obsnum}*", "science"),
        ("pointing_params.json", "pointing"),
    ]:
        if list(result_dir.glob(pattern)):
            return goal
    # Check pointing_params.json content
    summary = result_dir / "pointing_params.json"
    if summary.exists():
        try:
            return json.loads(summary.read_text()).get("obs_goal", "pointing")
        except (json.JSONDecodeError, OSError):
            pass
    return "unknown"


@op(
    name="ingest_ql_result",
    required_resource_keys={"recipes"},
    ins={"start": In(Nothing)},
    description="Parse QL result directory and write ql_result.json summary.",
)
def ingest_ql_result(context: OpExecutionContext, config: IngestQlResultConfig) -> None:
    """Parse QL reduction output and write a structured ql_result.json.

    Reads FITS / PNG / params files from the result directory, assembles
    a structured summary, and writes ``ql_result.json`` to the result dir.
    This JSON is consumed by the ``/api/ql`` HTTP endpoint in tolteca_web.

    Phase 3 (future): will also write a ``dp_ql_map`` DataProd entry to
    tolteca_db via SQLAlchemy.

    Parameters
    ----------
    context : OpExecutionContext
        Dagster execution context.
    config : IngestQlResultConfig
        Obsnum and result directory path from dataprod_sensor.
    """
    result_dir = Path(config.result_dir)
    obsnum = config.obsnum

    if not result_dir.exists():
        context.log.warning(f"Result dir not found: {result_dir}")
        return

    fits_files = [
        f.relative_to(result_dir).as_posix() for f in result_dir.rglob("*.fits")
    ]
    png_files = [
        f.relative_to(result_dir).as_posix() for f in result_dir.rglob("*.png")
    ]

    context.log.info(
        f"obsnum={obsnum}: {len(fits_files)} FITS, {len(png_files)} PNG files"
    )

    # Parse pointing parameters
    pointing_params = _parse_pointing_params(result_dir, obsnum)
    obs_goal = _infer_obs_goal(result_dir, obsnum)

    # Log pointing summary
    if pointing_params:
        for array, params in pointing_params.items():
            dx = params.get("dx_arcsec", params.get("x_t", {}).get("value", "?"))
            dy = params.get("dy_arcsec", params.get("y_t", {}).get("value", "?"))
            fwhm = params.get(
                "fwhm_a_arcsec", params.get("a_fwhm", {}).get("value", "?")
            )
            context.log.info(
                f'  {array}: dx={dx:.2f}" dy={dy:.2f}" fwhm={fwhm:.2f}"'
                if isinstance(dx, float)
                else f"  {array}: dx={dx} dy={dy} fwhm={fwhm}"
            )
    else:
        context.log.info(f"  No pointing params found for obsnum={obsnum}")

    # Build and write ql_result.json
    summary = {
        "obsnum": obsnum,
        "obs_goal": obs_goal,
        "status": "complete",
        "arrays": pointing_params,
        "fits_files": fits_files,
        "image_paths": [
            p for p in png_files if "summary" in p or "pointing" in p or "beammap" in p
        ],
        "ingested_at": datetime.now(UTC).isoformat(),
    }
    out_path = result_dir / "ql_result.json"
    out_path.write_text(json.dumps(summary, indent=2))
    context.log.info(f"Wrote {out_path}")
    context.log.info(f"Ingest of obsnum={obsnum} complete (obs_goal={obs_goal})")


@job(
    name="ingest_catalog_job",
    description="Ingest QL result into tolteca_db and update catalog.parquet.",
)
def ingest_catalog_job():
    """Ingest catalog job: ingest_ql_result."""
    ingest_ql_result()
