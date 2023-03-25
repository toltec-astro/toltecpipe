#!/usr/bin/env python
from pathlib import Path
import itertools
import re
import shlex
from loguru import logger


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


class ToltecRawDataStore(object):
    """A class to manage TolTEC raw data files on the data aquisition."""

    def __init__(self, data_lmt_dir, layout='clips', hostname_mapping=None):
        if layout not in self._layouts:
            raise ValueError(f"invalid layout: {layout}")

        self._data_lmt_dir = Path(data_lmt_dir)
        self._layout = layout
        hostname_mapping = self._hostname_mapping = hostname_mapping or {}
        interface_by_host = self._interface_by_host = {}
        for interface in self._interfaces:
            hn = self._get_layout_interface_hostname(layout=self._layout, interface=interface)
            if hn in hostname_mapping:
                hn = hostname_mapping[hn]
            if hn in interface_by_host:
                interface_by_host[hn].append(interface)
            else:
                interface_by_host[hn] = [interface]

    _layouts = ['clips', 'taco', 'unity']
    _masters = ['ics', 'tcs']
    _kids_interfaces = [f'toltec{nw}' for nw in range(13)]
    _instru_data_interfaces = _kids_interfaces + ['hwpr']
    _instru_ancillary_data_interfaces = ['toltec_hk']
    _tel_interfaces = ['tel_toltec']
    _nonkids_interfaces = ['hwpr', 'toltec_hk', 'tel_toltec']
    _interfaces = _kids_interfaces + _nonkids_interfaces
    _kids_interface_glob_patterns = ['toltec[0-9]', 'toltec1[0-2]']
    _instru_data_interface_glob_patterns = _kids_interface_glob_patterns + ['hwpr']

    _ics_interface_glob_patterns = _kids_interface_glob_patterns + ['hwpr', ]
    _interface_glob_patterns = _kids_interface_glob_patterns + _nonkids_interfaces


    @classmethod
    def _get_layout_interface_hostname(cls, layout, interface):
        if interface not in cls._interfaces:
            raise ValueError(f"invalid interface: {interface}")
        if layout == 'clips':
            if interface in [f'toltec{nw}' for nw in range(7)]:
                return 'clipa'
            if interface in [f'toltec{nw}' for nw in range(7, 13)]:
                return 'clipo'
            if interface in ['hwpr', ]:
                return 'clipo'
            if interface in ['tel_toltec', 'toltec_hk']:
                return 'clipy'
            raise ValueError(f"invalid interface: {interface}")
        if layout in ['unity', 'taco']:
            return layout
        raise ValueError(f"invalid layout: {layout}")

    @classmethod
    def _build_glob_patterns(cls, master, interface, obsnum, subobsnum, scannum, suffix, ext='.nc'):
        if master is None:
            master = cls._masters
        if interface is None:
            interface = cls._interface_glob_patterns
        if obsnum is None:
            obsnum = ["*"]
        if subobsnum is None:
            subobsnum = ["*"]
        if scannum is None:
            scannum = ["*"]

        def _destar(s):
            return re.sub("\*+", '*', s)

        def _pad_num(n, n_digits):
            try:
                return (f'{{:0{n_digits}d}}').format(int(n))
            except ValueError:
                if not n.startswith("*"):
                    n = f"*{n}"
                if not n.endswith("*"):
                    n = f"{n}*"
                return _destar(s)

        def _ensure_list(n):
            if isinstance(n, list):
                return n
            return [n]

        def _make_pattern_suffix(su):
            if su is None:
                return  '*'
            if su == '*':
                return '*_*'
            return f'*_{su}'

        def _make_master_interface_glob_pattern(master, interface):
            if interface in cls._instru_data_interfaces + cls._kids_interface_glob_patterns:
                return f'toltec/{master}/{interface}/{interface}'
            if interface in cls._instru_ancillary_data_interfaces:
                return f'toltec/{interface}/{interface}'
            if interface == 'tel_toltec':
                return f'tel/tel_toltec'
            raise NotImplementedError(f"invalid interface {interface}")

        def _resolve_master_interface_glob_patterns(master, interface):
            # these are quite complicated due to the varying levels
            if interface == '*':
                if master == 'ics':
                    interfaces = cls._ics_interface_glob_patterns
                else:
                    interfaces = cls._interface_glob_patterns
                return [_make_master_interface_glob_pattern(master, i) for i in interfaces]
            if interface == 'tel':
                interface = 'tel_toltec'
            return [_make_master_interface_glob_pattern(master, interface)]

        patterns = []
        for m, i, o, so, s, su, e in itertools.product(*map(_ensure_list, [master, interface, obsnum, subobsnum, scannum, suffix, ext])):
            o = _pad_num(o, 6)
            so = _pad_num(so, 3)
            s = _pad_num(s, 4)
            su = _make_pattern_suffix(su)
            # need to handle interface differenly since we have mismach of levels
            for mi in _resolve_master_interface_glob_patterns(m, i):
                patterns.append(_destar(f"{mi}_{o}_{so}_{s}_{su}{e}"))
        return patterns

    @staticmethod
    def _patterns_to_rsync_filters(patterns):
        # here need to decompose the pattens to all parents
        parents = []
        names=[]
        for p in patterns:
            p = Path(p)
            parents.extend(filter(lambda p: p != '.', map(str, p.parents)))
            names.append(p.name)
        parents = sorted(list(set(parents)), key=lambda p: len(p))
        return [f'--include={p}/' for p in parents] + [f'--include={p}' for p in names] + ['--exclude=*']

    def make_rsync_commands(
            self,
            dest_path,
            master=None,
            obsnum=None,
            subobsnum=None,
            scannum=None,
            suffix=None,
            rsync_exec='rsync',
            ):

        commands = []
        exec_options = [rsync_exec, '-avhPHAX', '--append-verify']
        for hn, interfaces in self._interface_by_host.items():
            patterns = []
            for interface in interfaces:
                if interface in self._kids_interfaces:
                    interface_glob_patterns = self._kids_interface_glob_patterns
                else:
                    interface_glob_patterns = [interface]
                patterns.extend(
                        self._build_glob_patterns(
                            master=master,
                            interface=interface_glob_patterns,
                            obsnum=obsnum,
                            subobsnum=subobsnum,
                            scannum=scannum,
                            suffix=suffix)
                        )
            patterns = list(set(patterns))
            logger.debug(f"glob patterns for hostname={hn}: {patterns}")
            filter_args = self._patterns_to_rsync_filters(patterns)
            # build the include and exclude lists
            src_dest = [f'{hn}:{self._data_lmt_dir}', str(dest_path)]
            cmd_parts = exec_options + filter_args + src_dest
            name = "_".join(re.sub(r'(\[.+?\]|\*+)', '%', a) for a in [hn, master, obsnum, subobsnum, scannum, suffix] if isinstance(a, str))
            commands.append({
                "name": name,
                "exec_args": exec_options,
                'filter_args': filter_args,
                'src_dest': src_dest,
                "command": shlex.join(cmd_parts),
                })
        return commands
