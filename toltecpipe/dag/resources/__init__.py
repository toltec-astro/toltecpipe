from dagster import StringSource, String, resource, Array, Field, Enum, EnumValue, Noneable
from loguru import logger

import os
from tolteca_web.data_prod.raw_obs_db import ToltecRawObsDB
from tolteca_datamodels.db.core import DBConfig
from ...core.data_store import ToltecDataStore, ToltecRawDataStore
import yaml


@resource(
    config_schema={
        "binds": Field(
            Array({"name": String, "url": StringSource}),
            default_value=[{"name": "dpdb", "url": "sqlite://"}],
        )
    },
    description="The db runtime config.",
)
def db_config(context):
    cfg = DBConfig.model_validate(context.resource_config)
    logger.debug(f"db_config: {db_config}")
    return cfg


@resource(
    required_resource_keys={"db_config"},
    description="The utility to query toltec raw obs db.",
)
def toltec_raw_obs_db(context):
    cfg = context.resources.db_config
    db = ToltecRawObsDB(cfg["toltec"].connect())
    logger.debug(f"toltec_raw_obs_db: {db}")
    return db


@resource(
    config_schema={
        "data_lmt_dir": Field(
            StringSource,
            default_value="data_lmt",
            description="The path to LMT data root.",
        ),
        "dataprod_toltec_dir": Field(
            StringSource,
            default_value="dataprod_toltec",
            description="The path to TolTEC data product root.",
        ),
    },
    description="The db runtime config.",
)
def toltec_data_store(context):
    ds = ToltecDataStore(
        data_lmt_dir=context.resource_config["data_lmt_dir"],
        dataprod_toltec_dir=context.resource_config["dataprod_toltec_dir"],
    )
    logger.debug(f"toltec_data_store: {ds}")
    return ds


@resource(
    config_schema={
        "data_lmt_dir": Field(
            StringSource,
            default_value="data_lmt",
            description="The path to LMT data root.",
        ),
        "layout": Field(
            Enum(
                "toltec_raw_data_store_layout",
                [
                    EnumValue("clips"),
                    EnumValue("unity"),
                    ]
                ),
            default_value="clips",
            description="The layout of the datastore.",
        ),
        "hostname_mapping": Field(
            {str: str},
            default_value={},
            description="The mapping of hostnames in layout.",
        ),
    },
    description="The raw data store config.",
)
def toltec_raw_data_store(context):
    rds = ToltecRawDataStore(
        data_lmt_dir=context.resource_config['data_lmt_dir'],
        layout=context.resource_config["layout"],
        hostname_mapping=context.resource_config['hostname_mapping']
    )
    logger.debug(f"toltec_raw_data_store: {rds}")
    return rds


_rsync_dest_config = {
    "dest_path": Field(
        StringSource,
        description="The path of target data archive root.",
    ),
    "dest_path_device_label_allowlist": Field(
        Noneable(Array(StringSource)),
        default_value=None,
        description="List of devices the dest_path must resides in.",
    ),
}


@resource(
    config_schema={
        "rsync_exec": Field(
            StringSource,
            default_value="rsync",
            description="The path to rsync executable.",
        ),
        **_rsync_dest_config,
    },
    description="The toltec data rsync runtime config.",
)
def toltec_data_rsync_config(context):
    ds = context.resource_config
    logger.debug(f"toltec_data_rsync_config: {ds}")
    return ds


@resource(
    config_schema=Array({
            "name": Field(
                StringSource,
                default_value="default",
                description="The name of this destination.",
            ),
            **_rsync_dest_config,
        }),
    description="The toltec data rsync destinations.",
)
def toltec_data_rsync_dest_presets(context):
    ds = context.resource_config
    logger.debug(f"toltec_data_rsync_dest_presets: {ds}")
    return ds



def load_resource_defs_by_deployment_name(deployment_name):

    resource_defs_by_deployment_name = {
        "local": {
            "db_config": db_config,
            "toltec_raw_obs_db": toltec_raw_obs_db,
            "toltec_data_store": toltec_data_store,
            "toltec_raw_data_store": toltec_raw_data_store,
            "toltec_data_rsync_config": toltec_data_rsync_config,
            "toltec_data_rsync_dest_presets": toltec_data_rsync_dest_presets,
        }
    }

    resource_defs = resource_defs_by_deployment_name.get(deployment_name)

    # check if additioanal yaml config is specified
    resource_defs_path = os.environ.get("TOLTECPIPE_RESOURCE_DEFS_PATH", None)
    if resource_defs_path is not None:
        logger.info(f"load resource defs from {resource_defs_path}")
        with open(resource_defs_path, 'r') as fo:
            d = yaml.safe_load(fo)
            for k, v in d.items():
                if k in resource_defs:
                    resource_defs[k] = resource_defs[k].configured(v)
    else:
        logger.info(f"no resource defs found, skip.")

    logger.debug(f"loaded resource defs for {deployment_name=}:\n{resource_defs}")
    return resource_defs
