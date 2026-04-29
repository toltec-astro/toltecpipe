"""Dagster ConfigurableResources for toltecpipe.

All resources read configuration from environment variables / .env file
via pydantic-settings.  Dagster EnvVar is used for secrets so they are
masked in the Dagster UI.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import pymysql
import pymysql.cursors
from dagster import ConfigurableResource, EnvVar
from pydantic import Field


class ToltecDbResource(ConfigurableResource):
    """Connection to the toltec MySQL data acquisition database.

    Parameters
    ----------
    host : str
        MySQL hostname.
    port : int
        MySQL port.
    db_name : str
        Database name.
    user : str
        MySQL user.
    password : str
        MySQL password (read from ``TOLTECPIPE_TOLTEC_DB_PASSWORD``).
    """

    host: str = "localhost"
    port: int = 3306
    db_name: str = "toltec"
    user: str = "toltec"
    password: str = EnvVar("TOLTECPIPE_TOLTEC_DB_PASSWORD")

    def get_connection(self) -> pymysql.Connection:
        """Open a new MySQL connection.

        Returns
        -------
        pymysql.Connection
            Connected and ready connection.
        """
        return pymysql.connect(
            host=self.host,
            port=self.port,
            db=self.db_name,
            user=self.user,
            password=self.password,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    def query(self, sql: str, args: tuple = ()) -> list[dict[str, Any]]:
        """Execute a read-only query and return all rows.

        Parameters
        ----------
        sql : str
            SQL statement.
        args : tuple
            Positional parameters.

        Returns
        -------
        list[dict[str, Any]]
            Result rows as dicts.
        """
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, args)
            return list(cur.fetchall())


class ValidationResource(ConfigurableResource):
    """Quartet completion validation configuration.

    Parameters
    ----------
    disabled_roach_indices : list[int]
        Roach indices permanently disabled on this installation.
    max_interface_count : int
        Total number of roach interfaces (including HWPR = index 13).
    validation_timeout_seconds : float
        Seconds to wait after last Valid=1 transition before declaring
        quartet complete (handles dynamically-disabled interfaces).
    """

    disabled_roach_indices: list[int] = Field(default_factory=list)
    max_interface_count: int = 14  # toltec0-12 + hwpr
    validation_timeout_seconds: float = 30.0


class RecipesResource(ConfigurableResource):
    """Paths and environment for taco_recipes shell scripts.

    Dagster ops call these scripts via subprocess; operators can run the
    same commands manually to reproduce any Dagster-triggered reduction.

    Parameters
    ----------
    recipes_root : str
        Directory containing the taco_recipes shell scripts.
    data_lmt_root : str
        Root of the data_lmt filesystem (raw NC + tel files).
    dataprod_toltec_root : str
        Output root for reduced data products.
    apt_file : str
        Path to the array pointing table (apt.ecsv).
    citlali_bin : str
        Path to the citlali binary.
    """

    recipes_root: str = "run/taco_recipes/ql_maps"
    data_lmt_root: str = "/data_lmt"
    dataprod_toltec_root: str = "/data_lmt/dataprod_toltec"
    apt_file: str = "common/apt.ecsv"
    citlali_bin: str = "/usr/local/bin/citlali"

    def script_for_goal(self, obs_goal: str) -> Path:
        """Return the recipe script path for a given obs_goal.

        Parameters
        ----------
        obs_goal : str
            Observation goal string from the tel NC file.

        Returns
        -------
        Path
            Absolute path to the recipe shell script.

        Raises
        ------
        ValueError
            If obs_goal has no defined recipe.
        """
        goal_map = {
            "pointing": "reduce_pointing.sh",
            "focus":    "reduce_pointing.sh",
            "astigmatism": "reduce_pointing.sh",
            "oof":      "reduce_pointing.sh",
            "beammap":  "reduce_beammap.sh",
            "azscan":   "reduce_beammap.sh",
            "elscan":   "reduce_beammap.sh",
            "science":  "reduce_science.sh",
        }
        script = goal_map.get(obs_goal.lower())
        if script is None:
            msg = f"No recipe defined for obs_goal={obs_goal!r}"
            raise ValueError(msg)
        return Path(self.recipes_root) / script

    def env(self) -> dict[str, str]:
        """Build the subprocess environment for recipe scripts.

        Returns
        -------
        dict[str, str]
            Environment variables passed to subprocess.run.
        """
        base = dict(os.environ)
        base.update({
            "TOLTECA_DATA_LMT_ROOT": self.data_lmt_root,
            "TOLTECA_DATAPROD_ROOT": self.dataprod_toltec_root,
            "TOLTECA_APT_FILE": self.apt_file,
            "CITLALI_BIN": self.citlali_bin,
        })
        return base

    def run_script(self, script: Path, obsnum: int, *, log=None) -> Path:
        """Run a recipe script for an obsnum, streaming output to log.

        Parameters
        ----------
        script : Path
            Path to the shell script.
        obsnum : int
            Observation number.
        log : optional
            Dagster context log (context.log).  If None, prints to stdout.

        Returns
        -------
        Path
            Expected output directory: ``dataprod_toltec_root/{obsnum}/``.

        Raises
        ------
        RuntimeError
            If the script exits with a non-zero return code.
        """
        cmd = [str(script), str(obsnum)]
        result = subprocess.run(
            cmd,
            env=self.env(),
            capture_output=True,
            text=True,
        )
        for line in result.stdout.splitlines():
            if log:
                log.info(line)
            else:
                print(line)  # noqa: T201
        if result.returncode != 0:
            msg = f"Recipe {script.name} failed (obsnum={obsnum}):\n{result.stderr}"
            raise RuntimeError(msg)
        return Path(self.dataprod_toltec_root) / str(obsnum)
