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
        interface_group_by_host = self._interface_group_by_host = {}
        for hn, interface_group in self._interface_group_by_host_layout[self._layout].items():
            if hn in hostname_mapping:
                hn = hostname_mapping[hn]
            interface_group_by_host[hn] = interface_group
        logger.debug(f"created raw data store:\n{self._interface_group_by_host}")
    _masters = ['ics', 'tcs']

    # these are classified for building the different master-interface-glob patterns
    # glob pattern like interfaces are also included.

    class InterfaceGroup(object):
        names: list
        glob_patterns: list

        def __init__(self, names, glob_patterns=None):
            self.names = names
            self.glob_patterns = glob_patterns or names

        def __add__(self, other):
            return self.__class__(
                    names=self.names + other.names,
                    glob_patterns=self.glob_patterns + other.glob_patterns
                    )
        def __contains__(self, item):
            return (item in self.names) or (item in self.glob_patterns)
        def __repr__(self):
            return f"{self.__class__.__name__}({self.names}, {self.glob_patterns})"

    _toltec_kids_data_interfaces = InterfaceGroup(
            [f'toltec{nw}' for nw in range(13)],
            glob_patterns=['toltec[0-9]', 'toltec1[0-2]']
            )
    _toltec_polarimetry_data_interfaces = InterfaceGroup(
            ['hwpr']
            )
    _toltec_hk_data_interfaces = InterfaceGroup(
            ['toltec_hk']
            )
    _toltec_wyatt_data_interfaces = InterfaceGroup(
            ['wyatt']
            )
    _toltec_ancillary_data_interfaces = _toltec_hk_data_interfaces + _toltec_wyatt_data_interfaces
    _toltec_nonkids_data_interfaces = _toltec_ancillary_data_interfaces + _toltec_polarimetry_data_interfaces
    _toltec_data_interfaces = _toltec_kids_data_interfaces + _toltec_nonkids_data_interfaces

    _toltec_ics_data_interfaces = (
            _toltec_kids_data_interfaces
            + _toltec_polarimetry_data_interfaces
            + _toltec_hk_data_interfaces
            + _toltec_wyatt_data_interfaces
            )
    _toltec_tcs_data_interfaces = (
            _toltec_kids_data_interfaces
            + _toltec_polarimetry_data_interfaces
            + _toltec_hk_data_interfaces
            )

    # TODO include the handling ofthe non-obsnum tagged data.
    # _toltec_ancillary_interfaces = InterfaceGroup(
    #        ['thermetry']
    #        )
    _toltec_nonkids_interfaces = _toltec_nonkids_data_interfaces # + _toltec_ancillary_interfaces
    _toltec_interfaces = _toltec_data_interfaces # + _toltec_ancillary_interfaces


    _facility_interfaces = InterfaceGroup(
            ['tel_toltec', 'tel_toltec2']
            )

    _nonkids_interfaces = _toltec_nonkids_interfaces + _facility_interfaces
    _tcs_interfaces = _facility_interfaces + _toltec_tcs_data_interfaces
    _ics_interfaces = _toltec_ics_data_interfaces
    _interfaces = _toltec_interfaces + _facility_interfaces

    # for clips:
    _toltec_clipa_interfaces = InterfaceGroup(
                [f'toltec{nw}' for nw in range(7)],
                glob_patterns=_toltec_kids_data_interfaces.glob_patterns
                )
    _toltec_clipo_interfaces = InterfaceGroup(
                [f'toltec{nw}' for nw in range(7, 13)],
                glob_patterns=_toltec_kids_data_interfaces.glob_patterns
                ) + _toltec_polarimetry_data_interfaces
    _toltec_clipy_interfaces = _facility_interfaces + _toltec_ancillary_data_interfaces

    _layouts = ['clips', 'taco', 'unity']
    _interface_group_by_host_layout = {
            "clips": {
                "clipa": _toltec_clipa_interfaces,
                "clipo": _toltec_clipo_interfaces,
                "clipy": _toltec_clipy_interfaces,
                },
            "unity": _interfaces,
            "taco": _interfaces,
            }
    _interface_group_aliases = {
            "clipa": _toltec_clipa_interfaces,
            "clipo": _toltec_clipo_interfaces,
            "clipy": _toltec_clipy_interfaces,
            'kids': _toltec_kids_data_interfaces,
            'toltec': _toltec_data_interfaces,
            'tel': _facility_interfaces,
            '*': _interfaces.glob_patterns,
            }

    @classmethod
    def _build_glob_patterns(cls, master, interface, obsnum, subobsnum, scannum, suffix, ext='.nc'):
        if master is None:
            master = cls._masters
        if interface is None:
            interface = cls._interfaces.glob_patterns
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

        def _build_toltec_data_pattern_stem(obsnum, subobsnum, scannum, suffix, ext):
            o = _pad_num(obsnum, 6)
            so = _pad_num(subobsnum, 3)
            s = _pad_num(scannum, 4)
            su = _make_pattern_suffix(suffix)
            return f"_{o}_{so}_{s}_{su}{ext}"

        def _build_facility_data_pattern_stem(obsnum, subobsnum, scannum, suffix, ext):
            o = _pad_num(obsnum, 6)
            so = _pad_num(subobsnum, 2)
            s = _pad_num(scannum, 4)
            return f"_*_{o}_{so}_{s}{ext}"

        def _make_master_interface_glob_pattern(master, interface):
            # these are quite complicated due to the varying levels and pattern format
            if interface in cls._toltec_data_interfaces:
                return f'toltec/{master}/{interface}/{interface}', _build_toltec_data_pattern_stem
            # if interface in cls._toltec_ancillary_interfaces:
            #     return f'toltec/{interface}/{interface}', _build_facility_data_pattern_stem
            if interface in cls._facility_interfaces:
                return f'tel/{interface}', _build_facility_data_pattern_stem
            raise NotImplementedError(f"invalid master-interface {master}-{interface}")

        def _resolve_master_interface_glob_patterns(master, interface):
            # this is resolve a group name like (kids, tel) to the group interfaces
            # then dispatch the making of patterns to _make_master_interface_glob_pattern
            if interface in cls._interface_group_aliases.keys():
                interfaces = cls._interface_group_aliases[interface].glob_patterns
            elif interface in cls._interfaces:
                interfaces = [interface]
            else:
                interfaces = []
            patterns = []
            for i in interfaces:
                if master == 'ics' and (i not in cls._ics_interfaces):
                    continue
                if master == 'tcs' and (i not in cls._tcs_interfaces):
                    continue
                patterns.append(_make_master_interface_glob_pattern(master, i))
            return patterns

        patterns = []
        for m, i, o, so, s, su, e in itertools.product(*map(_ensure_list, [master, interface, obsnum, subobsnum, scannum, suffix, ext])):
            logger.debug(f"build patterns for {m=} {i=} {o=} {so=} {s=} {su=} {e=}")
            # need to handle interface differenly since we have mismach of levels
            for master_interface, pattern_stem_func in _resolve_master_interface_glob_patterns(m, i):
                stem = pattern_stem_func(obsnum=o, subobsnum=so, scannum=s, suffix=su, ext=e)
                patterns.append(_destar(f"{master_interface}{stem}"))
        return patterns

    @staticmethod
    def _patterns_to_rsync_filters(patterns):
        # here need to decompose the pattens to all parents
        dir_includes = []
        file_includes = []
        for p in patterns:
            p = Path(p)
            dir_includes.extend(filter(lambda p: p != '.', map(str, p.parents)))
            file_includes.append(str(p))
        dir_includes = sorted(list(set(dir_includes)), key=lambda p: len(p))
        file_includes = sorted(list(set(file_includes)), key=lambda p: len(p))
        return [f'--include=/{p}/' for p in dir_includes] + [f'--include=/{p}' for p in file_includes] + ['--exclude=*']

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
        exec_options = [rsync_exec, '-avhPHAX', '--copy-dirlinks', '--append-verify']
        for hn, interfaces in self._interface_group_by_host.items():
            patterns = list(set(
                self._build_glob_patterns(
                    master=master,
                    interface=interfaces.glob_patterns,
                    obsnum=obsnum,
                    subobsnum=subobsnum,
                    scannum=scannum,
                    suffix=suffix)
                ))
            logger.debug(f"glob patterns for hostname={hn}: {patterns}")
            filter_args = self._patterns_to_rsync_filters(patterns)
            # build the include and exclude lists
            src_dest = [f'{hn}:{self._data_lmt_dir}/', f'{dest_path}/']
            cmd_parts = exec_options + filter_args + src_dest
            name = "_".join(re.sub(r'(\[.+?\]|\*+)', '%', str(a)) for a in [hn, master, obsnum, subobsnum, scannum, suffix] if isinstance(a, (str, int)))
            commands.append({
                "name": name,
                "exec_args": exec_options,
                'filter_args': filter_args,
                'src_dest': src_dest,
                "command": shlex.join(cmd_parts),
                })
        return commands
