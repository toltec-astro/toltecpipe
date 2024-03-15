
from tolteca_web.data_prod.conventions import make_toltec_raw_obs_uid, parse_toltec_raw_obs_uid

from dagster import (
    build_resources,
    dynamic_partitioned_config,
)


def make_raw_obs_uid_partition_config(resource_defs):

    _partition_info_cache = {}

    def partition_fn(_current_time=None):
        with build_resources({
            "toltec_raw_obs_db": resource_defs["toltec_raw_obs_db"],
            "db_config": resource_defs["db_config"],
            }) as resources:
            r = resources.toltec_raw_obs_db  # type: ignore
            # query the date entries in the db
            df = r.id_query_grouped(id=slice(-1000, None))
            df = df.sort_values("id_min")
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
            "n_data_items",
        ]
        return {k: entry[k] for k in include_keys}

    parse_partition_key = parse_toltec_raw_obs_uid

    @dynamic_partitioned_config(
        partition_fn, tags_for_partition_fn=tags_for_partition_fn
    )
    def raw_obs_uid_partition_config(partition_key):
        d = parse_partition_key(partition_key)
        config = {"ops": {"config": d}}
        # config.update(override_config)
        return config

    return raw_obs_uid_partition_config
