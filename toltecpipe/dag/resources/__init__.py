from dagster import StringSource, String, resource, Array, Field, Enum, EnumValue
from loguru import logger

from ...core.raw_obs_db import DBConfig, ToltecRawObsDB
from ...core.data_store import ToltecDataStore, ToltecRawDataStore


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
            description="The path to TolTEC data product root.",
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



@resource(
    config_schema={
        "rsync_exec": Field(
            StringSource,
            default_value="rsync",
            description="The path to rsync executable.",
        ),
        "dest_path": Field(
            StringSource,
            default_value="99_data_lmt",
            description="The path of target data archive root.",
        ),
        "dest_path_device_label_allowlist": Field(
            StringSource,
            default_value="data_transfer",
            description="Comma seprated list of devices the dest_path must resides in.",
        ),
    },
    description="The toltec data rsync runtime config.",
)
def toltec_data_rsync_config(context):
    ds = context.resource_config
    logger.debug(f"toltec_data_rsync_config: {ds}")
    return ds



resource_defs_by_deployment_name = {
    "local": {
        "db_config": db_config.configured(
            {"binds": [{"name": "toltec", "uri": {"env": "TOLTECPIPE_TOLTEC_DB_URI"}}]}
        ),
        "toltec_raw_obs_db": toltec_raw_obs_db,
        "toltec_data_store": toltec_data_store,
        "toltec_raw_data_store": toltec_raw_data_store.configured(
            {"data_lmt_dir": {"env": "TOLTECPIPE_TOLTEC_RAW_DATA_STORE_DATA_LMT_DIR"}}
            ),
        "toltec_data_rsync_config": toltec_data_rsync_config.configured(
            {
                'dest_path': {"env": "TOLTECPIPE_TOLTEC_DATA_RSYNC_DEST_PATH"},
                'dest_path_device_label_allowlist': {"env": "TOLTECPIPE_TOLTEC_DATA_RSYNC_DEST_PATH_DEVICE_LABEL_ALLOWLIST"},
                },
            ),
    }
}
