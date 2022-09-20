#!/usr/bin/env python
from pathlib import Path


class ToltecDataStore(object):
    """A class to manage TolTEC data file storage."""

    def __init__(self, data_lmt_dir, dataprod_toltec_dir):
        self._data_lmt_dir = Path(data_lmt_dir)
        self._dataprod_toltec_dir = Path(dataprod_toltec_dir)

    @property
    def data_lmt_dir(self):
        return self._data_lmt_dir

    @property
    def dataprod_toltec_dir(self):
        return self._dataprod_toltec_dir

    @property
    def data_toltec_dir(self):
        return self._data_lmt_dir.joinpath("toltec")

    @property
    def lmt_tel_dir(self):
        return self._data_lmt_dir.joinpath("tel")

    def get_raw_obs_dir(self, interface=None, master=None):
        if interface is None and master is None:
            raise ValueError("at least one of interface/master are required")
        if interface is None and master is not None:
            return self.data_toltec_dir.joinpath(master.lower())
        if interface == "lmt":
            return self.data_lmt_dir.joinpath(interface)
        if interface.startswith("toltec"):
            return self.data_toltec_dir.joinpath(master.lower(), interface)
        raise ValueError(f"unrecognized {master=} {interface=}")
