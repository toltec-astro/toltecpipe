from dagster import StringSource, String, resource, Array, Field
from loguru import logger

from ...core.raw_obs_db import DBConfig, ToltecRawObsDB
from ...core.data_store import ToltecDataStore


@resource(
    config_schema={
        "binds": Field(
            Array({"name": String, "uri": StringSource}),
            default_value=[{"name": "dpdb", "uri": "sqlite://"}],
        )
    },
    description="The db runtime config.",
)
def db_config(context):
    cfg = DBConfig.parse_obj(context.resource_config)
    logger.debug(f"db_config: {db_config}")
    return cfg


@resource(
    required_resource_keys={"db_config"},
    description="The utility to query toltec raw obs db.",
)
def toltec_raw_obs_db(context):
    cfg = context.resources.db_config
    db = ToltecRawObsDB.from_db_config(cfg)
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


resource_defs_by_deployment_name = {
    "local": {
        "db_config": db_config.configured(
            {"binds": [{"name": "toltec", "uri": {"env": "TOLTECPIPE_TOLTEC_DB_URI"}}]}
        ),
        "toltec_raw_obs_db": toltec_raw_obs_db,
        "toltec_data_store": toltec_data_store,
    }
}
