from typing import Dict
from dagster import (
    DynamicOut,
    DynamicOutput,
    MetadataValue,
    Out,
    Output,
    build_resources,
    dynamic_partitioned_config,
    op,
    graph,
    AssetMaterialization,
    AssetKey,
)
import yaml
from ..sensors import make_toltec_raw_obs_db_sensor
from ...core.raw_obs_db import make_toltec_raw_obs_uid, parse_toltec_raw_obs_uid


@op(
    required_resource_keys={"toltec_raw_obs_db", "toltec_data_store"},
    config_schema={
        "master": str,
        "obsnum": int,
        "subobsnum": int,
        "scannum": int,
    },
    out=Out(Dict),
    description="Create raw obs index.",
)
def create_raw_obs_index(context):
    dataprod_toltec_dir = context.resources.toltec_data_store.dataprod_toltec_dir
    db = context.resources.toltec_raw_obs_db
    master = context.op_config["master"]
    obsnum = context.op_config["obsnum"]
    subobsnum = context.op_config["subobsnum"]
    scannum = context.op_config["scannum"]

    dp_index = db.get_dp_index_for_obs(
        obsnum=obsnum, subobsnum=subobsnum, scannum=scannum, master=master
    )
    # unpack
    dp_index = dp_index["data_items"][-1]
    name = dp_index["meta"]["name"]
    # record the asset
    filepath = dataprod_toltec_dir.joinpath(f"{name}.yaml")
    with open(filepath, "w") as fo:
        yaml.dump(dp_index, fo)
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(filepath.name),
            description=f"dp_index of name {name}",
            metadata={
                "n_data_items": len(dp_index["data_items"]),
                "filepath": filepath,
            },
        )
    )

    yield Output(
        dp_index,
        metadata={
            "raw_obs_index": MetadataValue.json(dp_index),
            "filepath": MetadataValue.path(filepath),
        },
    )


@op(out=DynamicOut(Dict), description="Dispatch data items for reduction.")
def dispatch_raw_obs_data_items(raw_obs_index):
    for data_item in raw_obs_index["data_items"]:
        yield DynamicOutput(
            data_item, mapping_key=data_item["meta"]["name"].replace("-", "_")
        )


@op(out=Out(Dict), description="Reduce raw obs data item.")
def reduce_raw_obs_data_item(raw_obs_data_item):
    reduced_obs_data_item = dict(recipe_name="kids", **raw_obs_data_item)
    return reduced_obs_data_item


@op(out=Out(Dict), description="Collect reudced obs data items to index.")
def collect_reduced_obs_data_items(reduced_data_items):
    reduced_obs_index = {
        "meta": {"name": "some_name"},
        "data_items": reduced_data_items,
    }
    yield Output(reduced_obs_index)


@graph
def reduce_raw_obs_graph():
    data_items = dispatch_raw_obs_data_items(create_raw_obs_index())
    reduced_data_items = data_items.map(reduce_raw_obs_data_item)
    collect_reduced_obs_data_items(reduced_data_items.collect())


def make_toltec_timely_analysis_jobs(resource_defs):

    _partition_info_cache = {}

    def partition_fn(_current_time=None):
        with build_resources(resource_defs) as resources:
            r = resources.toltec_raw_obs_db  # type: ignore
            # query the date entries in the db
            df = r.id_query_grouped(id=slice(-10000, None))
            entries = df.to_dict(orient="records")
            partition_keys = []
            for entry in entries:
                partition_key = make_toltec_raw_obs_uid(entry)
                partition_keys.append(partition_key)
                _partition_info_cache[partition_key] = entry
        return partition_keys

    def tags_for_partition_fn(partition_key):
        entry = _partition_info_cache[partition_key]
        include_keys = [
            "master",
            "obsnum",
            "subobsnum",
            "scannum",
            "repeat",
            "obs_type",
        ]
        return {k: entry[k] for k in include_keys}

    parse_partition_key = parse_toltec_raw_obs_uid

    @dynamic_partitioned_config(
        partition_fn, tags_for_partition_fn=tags_for_partition_fn
    )
    def raw_obs_uid_paritition_config(partition_key):
        d = parse_partition_key(partition_key)
        return {"ops": {"create_raw_obs_index": {"config": d}}}

    return [
        make_toltec_raw_obs_db_sensor(
            reduce_raw_obs_graph.to_job(
                resource_defs=resource_defs, config=raw_obs_uid_paritition_config
            ),
            resource_defs=resource_defs,
        )
    ]
