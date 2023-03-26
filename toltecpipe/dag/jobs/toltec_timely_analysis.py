from typing import Dict
from dagster import (
    DynamicOut,
    DynamicOutput,
    MetadataValue,
    Out,
    Output,
    Failure,
    build_resources,
    dynamic_partitioned_config,
    op,
    graph,
    AssetMaterialization,
    AssetKey,
    config_mapping,
)
import yaml
import os
import pty
import shlex

from ..sensors import make_toltec_raw_obs_db_sensor
from ...core.raw_obs_db import make_toltec_raw_obs_uid, parse_toltec_raw_obs_uid
from .toltec_data_rsync import toltec_data_rsync_graph
from .config_schema import raw_obs_proc_config_schema


@op(
    required_resource_keys={"toltec_raw_obs_db", "toltec_data_store"},
    config_schema=raw_obs_proc_config_schema,
    out=Out(Dict),
    description="Create raw obs index.",
)
def create_raw_obs_index(context):
    import time
    # FIXME need this to wait for db to finish...
    time.sleep(5)
    dataprod_toltec_dir = context.resources.toltec_data_store.dataprod_toltec_dir
    db = context.resources.toltec_raw_obs_db
    master = context.op_config["master"]
    obsnum = context.op_config["obsnum"]
    subobsnum = context.op_config["subobsnum"]
    scannum = context.op_config["scannum"]

    dp_index = db.get_dp_index_for_obs(
        obsnum=obsnum, subobsnum=subobsnum, scannum=scannum, master=master,
        table_name='toltec_r1',
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


@op(out=Out(Dict), description="Reduce a single raw obs data item.")
def reduce_raw_obs_data_item(context, raw_obs_data_item):
    reduced_obs_data_item = dict(recipe_name="kids", **raw_obs_data_item)
    if raw_obs_data_item['meta']['obs_type'] == 'Nominal':
        yield Output(reduced_obs_data_item)
        return
    # trigger KIDs reduction
    scriptdir = os.path.expanduser('~/kids_bin')
    filepath = raw_obs_data_item['filepath']
    name = raw_obs_data_item['meta']['name']
    cmd = f'{scriptdir}/reduce_kids.sh {filepath}'
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(f'reduce_kids_{name}'),
            description=f"kids reduce command of name {name}",
            metadata={
                "name": name,
                "command": cmd,
            },
        )
    )
    exitcode = pty.spawn(shlex.split(cmd))
    if exitcode > 0:
        raise Failure(
            description="Failed run script.",
            metadata={
                "name": name,
                "command": cmd,
            },
        )
    yield Output(reduced_obs_data_item)


@op(out=Out(Dict), description="Collect reudced obs data items to index.")
def collect_reduced_obs_data_items(reduced_data_items):
    meta = reduced_data_items[0]['meta']
    name = '{interface}_{obsnum}_{subobsnum}_{scannum}'.format(**meta)
    reduced_obs_index = {
        "meta": {"name": name},
        "data_items": reduced_data_items,
    }
    yield Output(reduced_obs_index)



@op(out=Out(Dict), description="Generate QuickLook data product for reduced raw obs.")
def quicklook_reduced_raw_obs(context, reduced_obs_index):
    quicklook_reduced_obs_index = dict(recipe_name="kids_quicklook", **reduced_obs_index)
    # this can also trigger the kids QL product.
    scriptdir = os.path.expanduser('~/kids_bin')
    filepaths = [raw_obs_data_item['filepath'] for raw_obs_data_item in reduced_obs_index['data_items']]
    name = reduced_obs_index['meta']['name']
    cmd = '{}/reduce_kids_ql.sh {}'.format(scriptdir, ' '.join(filepaths))
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(f'kids_quicklook_{name}'),
            description=f"kids quick look reduce command",
            metadata={
                "name": name,
                "command": cmd,
            },
        )
    )
    exitcode = pty.spawn(shlex.split(cmd))
    if exitcode > 0:
        raise Failure(
            description="Failed run script.",
            metadata={
                "name": name,
                "command": cmd,
            },
        )
    yield Output(quicklook_reduced_obs_index)



@op(out=Out(Dict), description="Reduce raw obs in an adhoc way using the dispatch_reduction.sh")
def reduce_raw_obs_adhoc(context, raw_obs_index):
    reduced_obs_index = dict(recipe_name="dispatch_reduction", **raw_obs_index)
    # check if this is nominal data, not tune
    if raw_obs_index['data_items'][0]['meta']['obs_type'] != 'Nominal':
        yield Output(reduced_obs_index)
        return
    obsnum = raw_obs_index['meta']['obsnum']
    name = raw_obs_index['meta']['name']
    scriptdir = os.path.expanduser('~/kids_bin')
    cmd = f'{scriptdir}/dispatch_reduction.sh {obsnum}'
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(f'dispatch_reduction_{name}'),
            description=f"dispatch reduction command of name {name}",
            metadata={
                "name": name,
                "command": cmd,
            },
        )
    )
    pty.spawn(shlex.split(cmd))
    yield Output(reduced_obs_index)


@graph
def reduce_raw_obs_graph():
    raw_obs_index = create_raw_obs_index()
    reduced_raw_obs_index_adhoc = reduce_raw_obs_adhoc(raw_obs_index)
    data_items = dispatch_raw_obs_data_items(raw_obs_index)
    reduced_data_items = data_items.map(reduce_raw_obs_data_item)
    reduced_raw_obs_index = collect_reduced_obs_data_items(reduced_data_items.collect())
    quicklook_reduced_raw_obs(reduced_raw_obs_index)


@config_mapping(config_schema=raw_obs_proc_config_schema)
def dispatch_raw_obs_config(config):
    return {
            "reduce_raw_obs_graph": {
                "ops": {
                    "create_raw_obs_index": {'config': config}
                    }
                },
            "toltec_data_rsync_graph": {
                "ops": {
                    "build_raw_data_rsync_commands": {"config": config}
                    }
                },
            }



@graph(config=dispatch_raw_obs_config)
def dispatch_raw_obs():
    reduce_raw_obs_graph()
    toltec_data_rsync_graph()


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
        return {"ops": {"config": d}}

    return [
        make_toltec_raw_obs_db_sensor(
            dispatch_raw_obs.to_job(
                resource_defs=resource_defs, config=raw_obs_uid_paritition_config
            ),
            resource_defs=resource_defs,
        )
    ]
