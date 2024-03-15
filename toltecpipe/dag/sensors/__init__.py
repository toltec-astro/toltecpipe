import json
from dagster import (
    RunRequest,
    SensorDefinition,
    sensor,
    build_resources,
    SkipReason,
)
from datetime import datetime, timedelta, timezone
from tolteca_web.data_prod.conventions import make_toltec_raw_obs_uid
from ..resources import toltec_raw_obs_db


def make_toltec_raw_obs_db_sensor(job, resource_defs, **kwargs) -> SensorDefinition:

    kwargs.setdefault("minimum_interval_seconds", 2)

    @sensor(name=f"{job.name}_on_toltec_raw_obs_db_update", job=job, **kwargs)
    def toltec_raw_obs_db_update_sensor(context):
        with build_resources(resource_defs) as resources:
            toltec_raw_obs_db = resources.toltec_raw_obs_db  # type: ignore
        cursor = context.cursor or None
        if cursor is None:
            id_cursor = None
            id_next = None
        else:
            cursor = json.loads(cursor)
            # this is always the min id of a group
            id_cursor = cursor['id_start']
            id_next = cursor['id_end']

        n_days = 7
        time_start = datetime.now(timezone.utc) - timedelta(days=n_days)

        table_name = 'toltec'

        obs_latest = toltec_raw_obs_db.query_obs_group_latest(table_name=table_name)
        id_current = obs_latest['id_start']
        # update id_next in case it is not present
        # this limits to the recet 100 entries
        if id_next is None:
            id_since, _ = toltec_raw_obs_db.query_group_id_range(time_start=time_start, table_name=table_name)
            if id_current + 1 - id_since > 100:
                id0 = id_current + 1 - 100
            else:
                id0 = id_since
            # make sure id is group id
            id_cursor, id_next = toltec_raw_obs_db.query_group_id_range_for_id(id0, table_name=table_name)

        # skip if no new group
        if id_current <= id_cursor:
            return SkipReason("No new raw files found in db.")

        # re-query to collect all new files
        df = toltec_raw_obs_db.id_query_grouped(id=slice(id_next, None), valid_only=True, table_name=table_name)
        # there is a chance that the group has not be all valid yet
        if len(df) == 0:
            return SkipReason("New raw files found but contain invalid entries.")

        # now we can make the run request with valid groups
        def _make_run_request(entry):
            run_config = {
                "ops": {
                        "config": {
                            "master": entry["master"].lower(),
                            "obsnum": entry["obsnum"],
                            "subobsnum": entry["subobsnum"],
                            "scannum": entry["scannum"],
                        }
                }
            }
            tag_keys = [
                "master",
                "obsnum",
                "subobsnum",
                "scannum",
                "repeat",
                "obs_type",
                "n_data_items",
            ]
            tags = {k: str(entry[k]) for k in tag_keys}
            run_key = make_toltec_raw_obs_uid(entry)
            return RunRequest(run_key=run_key, run_config=run_config, tags=tags)

        items = df.to_dict(orient="records")
        run_requests = [
            _make_run_request(entry) for entry in items
        ]

        def _make_cursor(item):
            d = {
                k: item[k] for k in [
                    "master",
                    "obsnum",
                    "subobsnum",
                    "scannum",
                    "obs_type",
                    ]}
            d['id_start'] = item['id_min']
            d['id_end'] = item['id_max'] + 1
            return d

        context.update_cursor(json.dumps(_make_cursor(items[-1])))
        return run_requests

    return toltec_raw_obs_db_update_sensor
