
from toltecpipe.core.data_store import ToltecRawDataStore
import json



def test_rsync_commands():
    rds = ToltecRawDataStore('/data_lmt')
    commands = rds.make_rsync_commands(
            dest_path='.',
            master='ics',
            obsnum=17514,
            subobsnum=0,
            scannum=0,
            )
    print(json.dumps(commands, indent=2))

    commands = rds.make_rsync_commands(
            dest_path='.',
            master='tcs',
            obsnum=17514,
            subobsnum=0,
            scannum=0,
            )
    print(json.dumps(commands, indent=2))
