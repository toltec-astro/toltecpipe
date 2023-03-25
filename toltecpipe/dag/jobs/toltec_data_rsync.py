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
)
import yaml
import os
import pty
import shlex
from .config_schema import raw_obs_proc_config_schema


@op(
    required_resource_keys={"toltec_raw_obs_db", "toltec_raw_data_store", "toltec_data_rsync_config"},
    config_schema=raw_obs_proc_config_schema,
    out=Out(Dict),
    description="Collect list of files to transfer",
)
def build_raw_data_rsync_commands(context):
    import time
    # FIXME need this to wait for db to finish...
    time.sleep(5)
    db = context.resources.toltec_raw_obs_db
    master = context.op_config["master"]
    obsnum = context.op_config["obsnum"]
    subobsnum = context.op_config["subobsnum"]
    scannum = context.op_config["scannum"]

    rsync_config = context.resources.toltec_data_rsync_config
    commands = context.resources.toltec_raw_data_store.make_rsync_commands(
            master=master,
            obsnum=obsnum,
            subobsnum=subobsnum,
            scannum=scannum,
            **rsync_config
            )
    name = f"{master}_{obsnum}_{subobsnum}_{scannum}"
    output = {
            'name': name,
            'commands': commands
            }
    metadata = {'raw_data_rsync_commands': MetadataValue.json(output)}
    # record the asset
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(f'rsync_{name}'),
            description=f"rsync commands for {name}",
            metadata=metadata,
        )
    )
    yield Output(
        output,
        metadata=metadata,
    )


@op(out=DynamicOut(Dict), description="Dispatch rsync commands for execution.")
def dispatch_rsync_commands(raw_data_rsync_commands):
    for cmd_item in raw_data_rsync_commands["commands"]:
        yield DynamicOutput(
            cmd_item, mapping_key=cmd_item["name"].replace("-", "_")
        )


@op(out=Out(Dict), description="Run rsync command.")
def run_rsync(context, cmd_item):
    name = cmd_item['name']
    cmd = cmd_item['command']
    exitcode = pty.spawn(shlex.split(cmd))
    if exitcode > 0:
        raise Failure(
            description="Failed run rsync",
            metadata={
                "name": name,
                "command": cmd,
            },
        )
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(f'rsync_{name}'),
            description=f"rsync of {name}",
            metadata={
                "name": name,
                "command": cmd,
            },
        )
    )
    yield Output(cmd_item)


@graph
def toltec_data_rsync_graph():
    commands = build_raw_data_rsync_commands()
    command_items = dispatch_rsync_commands(commands)
    command_items.map(run_rsync)
