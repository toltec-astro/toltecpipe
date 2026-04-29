"""Basic tests for toltecpipe v3.x."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import toltecpipe


def test_import():
    """Verify the package can be imported."""
    assert toltecpipe


def test_mock_reduce_pointing(tmp_path):
    """Mock reduce_pointing.sh creates expected output files."""
    repo_root = Path(__file__).parents[1]
    script = repo_root / "run" / "mock_recipes" / "reduce_pointing.sh"
    if not script.exists():
        import pytest
        pytest.skip("mock_recipes not found")

    env = {
        **os.environ,
        "TOLTECA_DATA_LMT_ROOT": str(tmp_path / "data_lmt"),
        "TOLTECA_DATAPROD_ROOT": str(tmp_path / "dataprod_toltec"),
        "TOLTECA_APT_FILE": "common/apt.ecsv",
        "CITLALI_BIN": "/bin/echo",
    }
    result = subprocess.run(
        ["bash", str(script), "12345"],
        env=env, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    outdir = tmp_path / "dataprod_toltec" / "12345"
    assert outdir.exists()
    assert len(list(outdir.glob("*.png"))) > 0
    assert len(list(outdir.glob("*params*.txt"))) > 0


def test_mock_reduce_beammap(tmp_path):
    """Mock reduce_beammap.sh creates expected output files."""
    repo_root = Path(__file__).parents[1]
    script = repo_root / "run" / "mock_recipes" / "reduce_beammap.sh"
    env = {**os.environ, "TOLTECA_DATAPROD_ROOT": str(tmp_path / "dataprod_toltec")}
    result = subprocess.run(
        ["bash", str(script), "12346"], env=env, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    outdir = tmp_path / "dataprod_toltec" / "12346"
    assert outdir.exists()
    assert len(list(outdir.glob("*.png"))) > 0


def test_get_obs_goal_mock():
    """Mock get_obs_goal.py returns a valid obs_goal."""
    repo_root = Path(__file__).parents[1]
    script = repo_root / "run" / "mock_recipes" / "get_obs_goal.py"
    result = subprocess.run(
        ["python", str(script), "100"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0
    assert result.stdout.strip() in {"pointing", "focus", "beammap", "science"}


def test_recipes_resource_script_for_goal(tmp_path):
    """RecipesResource.script_for_goal returns correct script path."""
    from toltecpipe.dagster.resources import RecipesResource
    r = RecipesResource(
        recipes_root=str(tmp_path),
        data_lmt_root="/tmp",
        dataprod_toltec_root="/tmp",
        apt_file="/tmp/apt.ecsv",
        citlali_bin="/bin/echo",
    )
    assert r.script_for_goal("pointing").name == "reduce_pointing.sh"
    assert r.script_for_goal("focus").name == "reduce_pointing.sh"
    assert r.script_for_goal("beammap").name == "reduce_beammap.sh"
    assert r.script_for_goal("science").name == "reduce_science.sh"


def test_recipes_resource_env(tmp_path):
    """RecipesResource.env() contains all required TOLTECA_ vars."""
    from toltecpipe.dagster.resources import RecipesResource
    r = RecipesResource(
        recipes_root=str(tmp_path),
        data_lmt_root="/fake/data_lmt",
        dataprod_toltec_root="/fake/dataprod",
        apt_file="/fake/apt.ecsv",
        citlali_bin="/fake/citlali",
    )
    env = r.env()
    assert env["TOLTECA_DATA_LMT_ROOT"] == "/fake/data_lmt"
    assert env["TOLTECA_DATAPROD_ROOT"] == "/fake/dataprod"
    assert env["TOLTECA_APT_FILE"] == "/fake/apt.ecsv"
    assert env["CITLALI_BIN"] == "/fake/citlali"


def test_dagster_definitions_load():
    """Dagster Definitions loads without errors."""
    os.environ.setdefault("TOLTECPIPE_TOLTEC_DB_PASSWORD", "test")
    os.environ.setdefault("TOLTECPIPE_TOLTEC_DB_HOST", "localhost")
    os.environ.setdefault("TOLTECPIPE_TOLTEC_DB_NAME", "toltec")
    os.environ.setdefault("TOLTECPIPE_TOLTEC_DB_USER", "toltec")

    from toltecpipe.dagster.definitions import defs
    assert defs is not None
    job_names = {j.name for j in defs.get_all_job_defs()}
    assert "ql_map_job" in job_names
    assert "ingest_catalog_job" in job_names
