"""Basic tests for toltecpipe v3.x."""

from __future__ import annotations

import os
import subprocess
import sys
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
        env=env,
        capture_output=True,
        text=True,
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
        ["bash", str(script), "12346"],
        env=env,
        capture_output=True,
        text=True,
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
        [sys.executable, str(script), "100"],
        capture_output=True,
        text=True,
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
    job_names = {j.name for j in defs.jobs}
    assert "ql_map_job" in job_names
    assert "ingest_catalog_job" in job_names


def test_real_get_obs_goal_script_exists():
    """Real get_obs_goal.py exists in taco_recipes and is importable."""
    repo_root = Path(__file__).parents[1]
    script = repo_root / "run" / "taco_recipes" / "ql_maps" / "get_obs_goal.py"
    assert script.exists(), "run/taco_recipes/ql_maps/get_obs_goal.py not found"


def test_real_recipes_scripts_exist():
    """Real taco_recipes reduction scripts exist."""
    repo_root = Path(__file__).parents[1]
    recipes_dir = repo_root / "run" / "taco_recipes" / "ql_maps"
    for script in [
        "reduce_pointing.sh",
        "reduce_beammap.sh",
        "reduce_science.sh",
        "pointing_reader.py",
        "make_matched_apt.py",
    ]:
        assert (recipes_dir / script).exists(), f"{script} missing from taco_recipes"


def test_real_get_obs_goal_no_data_lmt(tmp_path):
    """Real get_obs_goal.py exits 0 with empty stdout when no tel file found."""
    repo_root = Path(__file__).parents[1]
    script = repo_root / "run" / "taco_recipes" / "ql_maps" / "get_obs_goal.py"
    env = {**os.environ, "TOLTECA_DATA_LMT_ROOT": str(tmp_path)}
    result = subprocess.run(
        ["python3", str(script), "99999"],
        env=env,
        capture_output=True,
        text=True,
    )
    # Should exit 0 (missing tel file is not a hard error)
    assert result.returncode == 0, (
        f"Expected exit 0, got {result.returncode}: {result.stderr}"
    )
    # stdout should be empty (no tel file found)
    assert result.stdout.strip() == ""


def test_ingest_ql_result_parse_pointing_params(tmp_path):
    """ingest_ql_result helper _parse_pointing_params reads JSON params files."""
    import json

    from toltecpipe.dagster.jobs import _parse_pointing_params

    obsnum = 55555
    result_dir = tmp_path / str(obsnum)
    result_dir.mkdir()

    # Write mock per-array params files (format written by mock reduce_pointing.sh)
    for array in ("a1100", "a1400", "a2000"):
        params = {
            "array": array,
            "dx_arcsec": 1.2,
            "dy_arcsec": -0.8,
            "fwhm_a_arcsec": 5.5,
            "amp_mJy": 2.1,
        }
        (result_dir / f"toltec_{array}_pointing_{obsnum}_params.txt").write_text(
            json.dumps(params)
        )

    parsed = _parse_pointing_params(result_dir, obsnum)
    assert "a1100" in parsed
    assert parsed["a1100"]["dx_arcsec"] == 1.2
    assert parsed["a1400"]["dy_arcsec"] == -0.8


def test_ingest_ql_result_parse_pointing_params_summary_json(tmp_path):
    """_parse_pointing_params prefers pointing_params.json (from pointing_reader.py)."""
    import json

    from toltecpipe.dagster.jobs import _parse_pointing_params

    obsnum = 55556
    result_dir = tmp_path / str(obsnum)
    result_dir.mkdir()

    summary = {
        "obsnum": obsnum,
        "obs_goal": "pointing",
        "status": "complete",
        "arrays": {
            "a1100": {"dx_arcsec": 3.0, "dy_arcsec": -1.5},
            "a1400": {"dx_arcsec": 2.5, "dy_arcsec": -1.0},
        },
    }
    (result_dir / "pointing_params.json").write_text(json.dumps(summary))

    parsed = _parse_pointing_params(result_dir, obsnum)
    assert parsed["a1100"]["dx_arcsec"] == 3.0


def test_ingest_ql_result_writes_ql_result_json(tmp_path):
    """ingest_ql_result writes ql_result.json with expected keys."""
    import json

    import dagster

    from toltecpipe.dagster.jobs import IngestQlResultConfig, ingest_ql_result
    from toltecpipe.dagster.resources import RecipesResource

    obsnum = 55557
    result_dir = tmp_path / str(obsnum)
    result_dir.mkdir()

    # Write mock outputs from reduce_pointing.sh
    for array in ("a1100", "a1400", "a2000"):
        params = {
            "array": array,
            "dx_arcsec": 1.0,
            "dy_arcsec": 0.5,
            "fwhm_a_arcsec": 5.2,
        }
        (result_dir / f"toltec_{array}_pointing_{obsnum}_params.txt").write_text(
            json.dumps(params)
        )
        (result_dir / f"toltec_{array}_pointing_{obsnum}.png").touch()

    ctx = dagster.build_op_context(
        resources={
            "recipes": RecipesResource(
                recipes_root=str(tmp_path),
                data_lmt_root=str(tmp_path),
                dataprod_toltec_root=str(tmp_path),
                apt_file="/tmp/apt.ecsv",
                citlali_bin="/bin/echo",
            )
        }
    )
    config = IngestQlResultConfig(obsnum=obsnum, result_dir=str(result_dir))
    ingest_ql_result(ctx, config)

    out = result_dir / "ql_result.json"
    assert out.exists(), "ql_result.json not written"
    data = json.loads(out.read_text())
    assert data["obsnum"] == obsnum
    assert data["status"] == "complete"
    assert "arrays" in data
    assert "a1100" in data["arrays"]
    assert "ingested_at" in data
