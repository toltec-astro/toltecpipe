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
    AssetObservation,
    AssetKey,
    config_mapping,
)
import yaml
import os
from pathlib import Path
import pty
import shlex

from ..sensors import make_toltec_raw_obs_db_sensor
from .toltec_data_rsync import toltec_data_rsync_graph
from .config_schema import raw_obs_proc_config_schema
from .partition import make_raw_obs_uid_partition_config


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
        table_name='toltec_r1', with_cal_info=False,
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
        # attach parent meta to each item for later usage
        data_item = data_item.copy()
        data_item['meta']['parent_meta'] = raw_obs_index['meta']
        yield DynamicOutput(
            data_item, mapping_key=data_item["meta"]["name"].replace("-", "_")
        )


@op(out=Out(Dict), description="Reduce a single raw obs data item.")
def reduce_raw_obs_data_item(context, raw_obs_data_item):
    reduced_obs_data_item = dict(recipe_name="kids", **raw_obs_data_item)
    if raw_obs_data_item['meta']['obs_type'] == 'Nominal':
        # skip nominal data as those are handled by the adhoc reduction.
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
    # if exitcode > 0:
        # raise Failure(
        #     description="Failed run script.",
        #     metadata={
        #         "name": name,
        #         "command": cmd,
        #     },
        # )
    context.log_event(
        AssetObservation(
            asset_key=AssetKey(f'reduce_kids_{name}'),
            metadata={
                "exitcode": exitcode
            }
        )
    )
    yield Output(reduced_obs_data_item)


@op(out=Out(Dict), description="Collect reudced obs data items to index.")
def collect_reduced_obs_data_items(reduced_data_items):
    meta = reduced_data_items[0]['meta']['parent_meta'].copy()
    meta.update({
        "data_prod_type": "dp_reduced_obs",
        })
    reduced_obs_index = {
        "meta": meta,
        "data_items": reduced_data_items,
    }
    yield Output(reduced_obs_index)



@op(out=Out(Dict), description="Generate QuickLook data product for reduced raw obs.")
def quicklook_reduced_raw_obs(context, reduced_obs_index):
    quicklook_reduced_obs_index = dict(recipe_name="kids_quicklook", **reduced_obs_index)
    # this can also trigger the kids QL product.
    if reduced_obs_index['data_items'][0]['meta']['obs_type'] == 'Nominal':
        # skip nominal data as those are handled by the adhoc reduction.
        yield Output(reduced_obs_index)
        return

    scriptdir = os.path.expanduser('~/kids_bin')
    filepaths = [raw_obs_data_item['filepath'] for raw_obs_data_item in reduced_obs_index['data_items']]
    name = reduced_obs_index['meta']['name']
    cmd = '{}/reduce_kids_ql.sh {}'.format(scriptdir, ' '.join(filepaths))
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(f'kids_quicklook_reduce_{name}'),
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
    # collect quick look images in the output directory
    # TODO properly handle the dpdir
    obsnum = reduced_obs_index['meta']['obsnum']
    dpdir = Path('/data_lmt/toltec/reduced').joinpath(f'{obsnum}')
    ql_files = list(dpdir.glob(f"**/*.png"))
    # compose an md with urls
    md_content = '\n'.join([
        f'http://taco/ql/{obsnum}/{f.relative_to(dpdir)}'
        for f in ql_files
        ])
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(f'kids_quicklook_files_{name}'),
            description=f"kids quick look reduce results",
            metadata={
                "name": name,
                "dpdir": str(dpdir),
                "quicklook_files": MetadataValue.md(md_content),
            },
        )
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
                "config": config
                },
            }



@graph(config=dispatch_raw_obs_config)
def dispatch_raw_obs():
    reduce_raw_obs_graph()
    toltec_data_rsync_graph()


def make_toltec_timely_analysis_jobs(resource_defs):
    resource_defs = resource_defs.copy()
    toltec_data_rsync_dest_presets = resource_defs.pop("toltec_data_rsync_dest_presets")
    with build_resources({"toltec_data_rsync_dest_presets": toltec_data_rsync_dest_presets}) as resources:
        dests_by_name = {d["name"]: d for d in resources.toltec_data_rsync_dest_presets}
        dest = dests_by_name.get("data_transfer", None)
        if dest is not None:
            resource_defs["toltec_data_rsync_config"] = resource_defs["toltec_data_rsync_config"].configured({
                "dest_path": dest["dest_path"],
                "dest_path_device_label_allowlist": dest["dest_path_device_label_allowlist"],
                })
        else:
            raise ValueError("no data transfer dest found")

    raw_obs_uid_partition_config = make_raw_obs_uid_partition_config(
            resource_defs=resource_defs,
            )
    return [
        make_toltec_raw_obs_db_sensor(
            dispatch_raw_obs.to_job(
                resource_defs=resource_defs, config=raw_obs_uid_partition_config
            ),
            resource_defs=resource_defs,
        )
    ]
