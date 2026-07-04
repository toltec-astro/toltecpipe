"""Dagster Definitions for toltecpipe.

Single-machine (local) layout: both sensors and all jobs in one Definitions.
For two-machine deployment, split into ``kids_definitions`` and
``maps_definitions`` and load via workspace.yaml gRPC servers.

Configuration is loaded from environment variables / .env file.
See ``toltecpipe.config`` and the example ``toltecpipe.yaml``.

Simulator mode
--------------
Set ``TOLTECPIPE_SIMULATOR_ENABLED=true`` (and source the env file before
launching dagster so os.getenv picks it up).  In simulator mode only the
``simulator_sensor`` and ``dataprod_sensor`` are registered — no MySQL
connection is required.
"""

# NOTE: Cannot use `from __future__ import annotations` with Dagster.

import os

import dagster as dg

from .jobs import ingest_catalog_job, ql_map_job
from .resources import RecipesResource, ToltecDbResource, ValidationResource
from .sensors import dataprod_sensor, quartet_sensor, simulator_sensor

__all__ = ["defs"]


def _make_resources() -> dict:
    """Build resource dict from environment / .env settings."""
    return {
        "toltec_db": ToltecDbResource(
            host=dg.EnvVar("TOLTECPIPE_TOLTEC_DB_HOST"),
            port=int(os.getenv("TOLTECPIPE_TOLTEC_DB_PORT", "3306")),
            db_name=dg.EnvVar("TOLTECPIPE_TOLTEC_DB_NAME"),
            user=dg.EnvVar("TOLTECPIPE_TOLTEC_DB_USER"),
            password=dg.EnvVar("TOLTECPIPE_TOLTEC_DB_PASSWORD"),
        ),
        "validation": ValidationResource(
            disabled_roach_indices=[
                int(x)
                for x in os.getenv("TOLTECPIPE_DISABLED_ROACH_INDICES", "").split(",")
                if x.strip()
            ],
            max_interface_count=int(os.getenv("TOLTECPIPE_MAX_INTERFACE_COUNT", "14")),
            validation_timeout_seconds=float(
                os.getenv("TOLTECPIPE_VALIDATION_TIMEOUT_SECONDS", "30.0")
            ),
        ),
        "recipes": RecipesResource(
            recipes_root=os.getenv(
                "TOLTECPIPE_RECIPES_ROOT", "run/taco_recipes/ql_maps"
            ),
            data_lmt_root=os.getenv("TOLTECPIPE_DATA_LMT_ROOT", "/data_lmt"),
            dataprod_toltec_root=os.getenv(
                "TOLTECPIPE_DATAPROD_TOLTEC_ROOT", "/data_lmt/dataprod_toltec"
            ),
            apt_file=os.getenv("TOLTECPIPE_APT_FILE", "common/apt.ecsv"),
            citlali_bin=os.getenv("TOLTECPIPE_CITLALI_BIN", "/usr/local/bin/citlali"),
        ),
    }


def _make_simulator_resources() -> dict:
    """Build resource dict for simulator mode — only recipes, no MySQL."""
    return {
        "recipes": RecipesResource(
            recipes_root=os.getenv("TOLTECPIPE_RECIPES_ROOT", "mock_recipes"),
            data_lmt_root=os.getenv(
                "TOLTECPIPE_DATA_LMT_ROOT",
                "/tmp/toltecpipe_test/data_lmt",  # noqa: S108 — sim default
            ),
            dataprod_toltec_root=os.getenv(
                "TOLTECPIPE_DATAPROD_TOLTEC_ROOT",
                "/tmp/toltecpipe_test/dataprod_toltec",  # noqa: S108 — sim default
            ),
            apt_file=os.getenv("TOLTECPIPE_APT_FILE", "common/apt.ecsv"),
            citlali_bin=os.getenv("TOLTECPIPE_CITLALI_BIN", "/bin/echo"),
        ),
    }


def get_definitions() -> dg.Definitions:
    """Build production Definitions (requires MySQL)."""
    return dg.Definitions(
        jobs=[ql_map_job, ingest_catalog_job],
        sensors=[quartet_sensor, dataprod_sensor],
        resources=_make_resources(),
    )


def get_simulator_definitions() -> dg.Definitions:
    """Build simulator Definitions — no MySQL, mock recipes, pre-set obsnums."""
    return dg.Definitions(
        jobs=[ql_map_job, ingest_catalog_job],
        sensors=[simulator_sensor, dataprod_sensor],
        resources=_make_simulator_resources(),
    )


_simulator_enabled = os.getenv("TOLTECPIPE_SIMULATOR_ENABLED", "false").lower() in (
    "true",
    "1",
    "yes",
)

defs = get_simulator_definitions() if _simulator_enabled else get_definitions()
