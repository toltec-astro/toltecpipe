import json
from dagster import (
    RunRequest,
    SensorDefinition,
    sensor,
    build_resources,
    SkipReason,
)

from toltecpipe.core.raw_obs_db import make_toltec_raw_obs_uid
from ..resources import toltec_raw_obs_db


def make_toltec_raw_obs_db_sensor(job, resource_defs, **kwargs) -> SensorDefinition:

    kwargs.setdefault("minimum_interval_seconds", 2)

    @sensor(name=f"{job.name}_on_toltec_raw_obs_db_update", job=job, **kwargs)
    def toltec_raw_obs_db_update_sensor(context):
        with build_resources(resource_defs) as resources:
            toltec_raw_obs_db = resources.toltec_raw_obs_db  # type: ignore
        cursor = context.cursor or None
        if cursor is None:
            id_since = None
        else:
            cursor = json.loads(cursor)
            id_since = cursor["id"]
        obs_latest = toltec_raw_obs_db.query_obs_latest()
        id_current = obs_latest["id"]
        if id_since is not None and id_current <= id_since:
            return SkipReason("No new raw files found in db.")
        if id_since is None:
            # limit the sensor to the latest 100 entries
            id_since = id_current - 100 + 1
        # re-query to collect all obs items
        df = toltec_raw_obs_db.id_query_grouped(id=slice(id_since, id_current + 1))

        def _make_run_request(entry):
            run_config = {
                "ops": {
                    "create_raw_obs_index": {
                        "config": {
                            "master": entry["master"],
                            "obsnum": entry["obsnum"],
                            "subobsnum": entry["subobsnum"],
                            "scannum": entry["scannum"],
                        }
                    }
                }
            }
            run_key = make_toltec_raw_obs_uid(entry)
            return RunRequest(run_key=run_key, run_config=run_config)

        run_requests = [
            _make_run_request(entry) for entry in df.to_dict(orient="records")
        ]
        context.update_cursor(json.dumps(obs_latest))
        return run_requests

    return toltec_raw_obs_db_update_sensor
