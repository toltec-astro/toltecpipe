"""Dagster jobs for toltecpipe.

ql_map_job
    Three thin ops: get_obs_goal → run_ql_recipe → (done).
    Dagster wraps the taco_recipes shell script calls.

ingest_catalog_job
    One op: ingest_ql_result.
    Registers dataprod results in tolteca_db and exports catalog.parquet.
"""

# NOTE: Cannot use `from __future__ import annotations` with Dagster.

from pathlib import Path

from dagster import Config, In, Nothing, Out, OpExecutionContext, job, op

__all__ = ["ql_map_job", "ingest_catalog_job"]


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
            import subprocess
            result = subprocess.run(
                ["python", str(script), str(obsnum)],
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


_REDUCIBLE_GOALS = frozenset({
    "pointing", "focus", "astigmatism", "oof",
    "beammap", "azscan", "elscan", "science",
})


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


@op(
    name="ingest_ql_result",
    required_resource_keys={"recipes"},
    ins={"start": In(Nothing)},
    description="Ingest QL result directory into tolteca_db and update catalog.",
)
def ingest_ql_result(context: OpExecutionContext, config: IngestQlResultConfig) -> None:
    """Register QL map results in tolteca_db and refresh catalog.parquet.

    Reads FITS / PNG / params files from the result directory and writes
    a ``dp_ql_map`` DataProd entry.  Triggers catalog.parquet export.

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

    fits_files = list(result_dir.rglob("*.fits"))
    png_files = list(result_dir.rglob("*.png"))
    param_files = list(result_dir.rglob("*params*.txt")) + list(result_dir.rglob("*params*.json"))

    context.log.info(
        f"obsnum={obsnum}: {len(fits_files)} FITS, "
        f"{len(png_files)} PNG, {len(param_files)} param files"
    )

    # TODO: write to tolteca_db dp_ql_map DataProd
    # TODO: update catalog.parquet with ql_map_status + pointing columns
    context.log.info(f"Ingest of obsnum={obsnum} complete (stub)")


@job(
    name="ingest_catalog_job",
    description="Ingest QL result into tolteca_db and update catalog.parquet.",
)
def ingest_catalog_job():
    """Ingest catalog job: ingest_ql_result."""
    ingest_ql_result()
