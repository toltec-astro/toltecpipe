"""Configuration loading for toltecpipe.

Settings are read in priority order:
1. Environment variables
2. .env file (TOLTECPIPE_ENV_FILE or .env in cwd)
3. YAML config file (TOLTECPIPE_CONFIG or toltecpipe.yaml in cwd)

All Dagster resources read from these settings via pydantic-settings.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_env_file() -> str | None:
    explicit = os.environ.get("TOLTECPIPE_ENV_FILE")
    if explicit:
        return explicit
    local = Path(".env")
    if local.exists():
        return str(local)
    return None


def _load_yaml_defaults() -> dict:
    """Load YAML config file as default values (lowest priority)."""
    config_path = os.environ.get("TOLTECPIPE_CONFIG")
    if not config_path:
        local = Path("toltecpipe.yaml")
        config_path = str(local) if local.exists() else None
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    return {}


class ToltecpipeSettings(BaseSettings):
    """Top-level settings for toltecpipe.

    All fields can be overridden by environment variables prefixed with
    ``TOLTECPIPE_`` or set in a ``.env`` file.

    Parameters
    ----------
    toltec_db_host : str
        Hostname of the toltec MySQL database.
    toltec_db_port : int
        Port of the toltec MySQL database.
    toltec_db_name : str
        Database name.
    toltec_db_user : str
        Database user.
    toltec_db_password : str
        Database password.
    data_lmt_root : Path
        Root directory of data_lmt (raw NC files, tel files).
    dataprod_toltec_root : Path
        Root directory for reduced data products.
    recipes_root : Path
        Directory containing taco_recipes shell scripts.
    apt_file : Path
        Path to the array pointing table (apt.ecsv).
    citlali_bin : Path
        Path to the citlali binary.
    sensor_interval_seconds : int
        Polling interval for toltec_db sensors.
    sensor_start_date : str | None
        ISO date string limiting how far back sensors look (YYYY-MM-DD).
    disabled_roach_indices : list[int]
        Roach indices to skip in quartet completion detection.
    validation_timeout_seconds : float
        Timeout for quartet completion (disabled-interface tolerance).
    dagster_postgres_url : str | None
        PostgreSQL URL for shared Dagster backend. If None, uses SQLite.
    """

    model_config = SettingsConfigDict(
        env_prefix="TOLTECPIPE_",
        env_file=_find_env_file(),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- toltec_db MySQL connection ---
    toltec_db_host: str = "localhost"
    toltec_db_port: int = 3306
    toltec_db_name: str = "toltec"
    toltec_db_user: str = "toltec"
    toltec_db_password: str = ""

    # --- Filesystem paths ---
    data_lmt_root: Path = Field(default=Path("/data_lmt"))
    dataprod_toltec_root: Path = Field(default=Path("/data_lmt/dataprod_toltec"))
    recipes_root: Path = Field(default=Path("run/taco_recipes/ql_maps"))
    apt_file: Path = Field(default=Path("common/apt.ecsv"))
    citlali_bin: Path = Field(default=Path("/usr/local/bin/citlali"))

    # --- Sensor behaviour ---
    sensor_interval_seconds: int = 10
    sensor_start_date: str | None = None
    disabled_roach_indices: list[int] = Field(default_factory=list)
    validation_timeout_seconds: float = 30.0

    # --- Dagster backend ---
    dagster_postgres_url: str | None = None


def load_settings() -> ToltecpipeSettings:
    """Load settings, merging YAML defaults with env/dotenv overrides."""
    yaml_defaults = _load_yaml_defaults()
    return ToltecpipeSettings(**yaml_defaults)
