from typing import Dict
from dagster import (
    DynamicOut,
    DynamicOutput,
    MetadataValue,
    Out,
    Output,
    Failure,
    ExpectationResult,
    build_resources,
    dynamic_partitioned_config,
    op,
    graph,
    AssetMaterialization,
    AssetKey,
    config_mapping,
)
from pathlib import Path
import re
import json
import os
import subprocess
import shlex
from .config_schema import raw_obs_proc_config_schema
from .partition import make_raw_obs_uid_partition_config


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
            dest_path=rsync_config['dest_path'],
            rsync_exec=rsync_config['rsync_exec'],
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


def _query_path_device_info(path, human_readable_size=True):
    # this seems to be necessary to allow findmnt respond when the disk
    # is on standby
    path = Path(path)
    if not path.exists() or not path.is_dir():
        return {
                "success": False,
                "message": f"Path {path} does not exist or is not a directory",
                "command": '',
                "stdout": ''
                }
    subprocess.run(['touch', str(path)])

    path_device_info_cmd = f'findmnt --output-all --real --json -T {path}'
    if not human_readable_size:
        path_device_info_cmd = path_device_info_cmd + ' -b'
    p = subprocess.run(
            shlex.split(path_device_info_cmd),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout = p.stdout.decode()
    info = {
            "command": path_device_info_cmd,
            "stdout": stdout,
            }
    if p.returncode > 0:
        info.update({
            "success": False,
            "message": "Failed to run path_device_info_cmd.",
            })
        return info
    path_device_info = json.loads(stdout)['filesystems']
    if len(path_device_info) != 1:
        info.update({
                "success": False,
                "message": "Ambiguous devide info.",
                })
        return info
    path_device_info = path_device_info[0]
    info.update({
            "success": True,
            "message": None,
            "stdout": stdout,
            "data": path_device_info,
            })
    return info


@op(
    required_resource_keys={"toltec_data_rsync_config"},
    out=Out(Dict), description="Run rsync command.")
def run_rsync(context, cmd_item):
    name = cmd_item['name']
    cmd = cmd_item['command']
    dest_path = cmd_item['src_dest'][-1]

    # check if dest matches with the required device lable
    rsync_config = context.resources.toltec_data_rsync_config
    allowed_labels = rsync_config.get('dest_path_device_label_allowlist', None)
    if allowed_labels is None:
        logger.debug("allowed device label not set, allow all devices.")

    r = _query_path_device_info(dest_path, human_readable_size=False)

    dest_path_device_info = r.get('data', None)
    if r['success'] and dest_path_device_info is not None:
        # check label in allowlist:
        dest_path_device_label = dest_path_device_info.get('label', None)
        if dest_path_device_label is None:
            # use source as label
            dest_path_device_label = dest_path_device_info.get("source")
        dest_path_avail_space = dest_path_device_info.get("avail", None)
        if allowed_labels is not None and dest_path_device_label not in allowed_labels:
            device_is_bad = (True, f"Dest path device label {dest_path_device_label} not allowed.")
        elif dest_path_avail_space is not None and dest_path_avail_space < 1e6:
            device_is_bad = (True, f"Dest path device available size is too small (<1MB).")
        else:
            device_is_bad = (False, None)
    else:
        device_is_bad = (True, r['message'])

    device_check_metadata = {
                "name": f'check_device_for_{name}',
                'dest_path_device_label_allowlist': ','.join(allowed_labels or ["ALL"]),
                "dest_path_device_info_command": r['command'],
                'stdout': r['stdout'],
                }

    if device_is_bad[0]:
        raise Failure(
            description=device_is_bad[1],
            metadata=device_check_metadata,
        )
    context.log_event(
        ExpectationResult(
            success=True,
            description=f"Dest path device check passed.",
            metadata=device_check_metadata,
        )
    )
    # now ready to run rsync
    _cmd = shlex.split(cmd)
    p = subprocess.run(_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    lns = []
    for ln in p.stdout.decode().split('\n'):
        lns.append(re.sub('.*\r', '', ln))
    stdout = '\n'.join(lns)
    if p.returncode > 0:
        raise Failure(
            description="Failed run rsync",
            metadata={
                "name": name,
                "command": cmd,
                'stdout': stdout,
            },
        )
    # query dist again to update space left.
    r = _query_path_device_info(dest_path, human_readable_size=True)
    if r['success'] and r.get('data', None) is not None:
        disk_space_avail = r['data'].get('avail', None)
    else:
        disk_space_avail = None
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(f'rsync_{name}'),
            description=f"rsync of {name}",
            metadata={
                "name": name,
                "command": cmd,
                "stdout": stdout,
                "disk_space_left": disk_space_avail,
            },
        )
    )
    yield Output(cmd_item)



@config_mapping(config_schema=raw_obs_proc_config_schema)
def dispatch_raw_obs_config(config):
    return {
            "build_raw_data_rsync_commands": {"config": config}
            }


@graph(config=dispatch_raw_obs_config)
def toltec_data_rsync_graph():
    commands = build_raw_data_rsync_commands()
    command_items = dispatch_rsync_commands(commands)
    command_items.map(run_rsync)


def make_toltec_data_rsync_jobs(resource_defs):

    jobs = []
    with build_resources({"toltec_data_rsync_dest_presets": resource_defs["toltec_data_rsync_dest_presets"]}) as resources:
        dests = resources.toltec_data_rsync_dest_presets
        for dest in dests:
            rdefs = resource_defs.copy()
            rdefs.pop("toltec_data_rsync_dest_presets")
            rdefs["toltec_data_rsync_config"] = rdefs['toltec_data_rsync_config'].configured({
                    "dest_path": dest["dest_path"],
                    "dest_path_device_label_allowlist": dest["dest_path_device_label_allowlist"],
                    })
            raw_obs_uid_partition_config = make_raw_obs_uid_partition_config(
                    resource_defs=rdefs,
                    )
            dest_name = dest["name"]
            jobs.append(
                toltec_data_rsync_graph.to_job(
                    name=f"toltec_data_rsync_to_{dest_name}",
                    resource_defs=rdefs, config=raw_obs_uid_partition_config)
                )
    return jobs
