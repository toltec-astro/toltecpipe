
from dagster import Enum, EnumValue


raw_obs_proc_config_schema = {
        "master": Enum(
            'master',
            [
                EnumValue('ics'),
                EnumValue('tcs'),
            ]
        ),
        "obsnum": int,
        "subobsnum": int,
        "scannum": int,
    }
