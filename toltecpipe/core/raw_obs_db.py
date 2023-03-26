"""Main module."""

from __future__ import annotations
from typing import List, Dict
import re
from functools import cached_property
from pydantic import Field, PrivateAttr, StrictStr, constr
from loguru import logger
import pandas as pd

import sqlalchemy.sql.expression as se
from sqlalchemy.sql import alias as sqla_alias
from sqlalchemy.sql import func as sqla_func
from .sqla_utils import SqlaDB
from .base import ImmutableBaseModel


SqlaBindUriStr = constr(strict=True, regex=r".+:\/\/.*")


class SqlaBindConfig(ImmutableBaseModel):
    """The config of a database connection."""

    name: StrictStr = Field(description="The name to identify the connection.")
    uri: SqlaBindUriStr = Field(description="The SQLAlchemy engine URI to connect.")

    @cached_property
    def db(self):
        return SqlaDB.from_uri(self.uri)


class DBConfig(ImmutableBaseModel):
    """The config for databases."""

    binds: List[SqlaBindConfig] = Field(description="The list of database binds.")
    _binds_by_name: Dict = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)
        self._binds_by_name = {bind.name: bind for bind in self.binds}

    def get_db(self, name):
        if name not in self._binds_by_name:
            raise ValueError(f"db with bind name {name} does not exist.")
        return self._binds_by_name[name].db


def make_toltec_raw_obs_uid(entry: Dict) -> str:
    return f"{entry['master'].lower()}-{entry['obsnum']}-{entry['subobsnum']}-{entry['scannum']}"


def parse_toltec_raw_obs_uid(uid: str) -> Dict:
    re_uid = r"(?P<master>ics|tcs)-(?P<obsnum>\d+)-(?P<subobsnum>\d+)-(?P<scannum>\d+)"
    dispatch_types = {"obsnum": int, "subobsnum": int, "scannum": int}
    m = re.match(re_uid, uid)
    if m is None:
        raise ValueError(f"invalid uid {uid}")
    d = m.groupdict()
    for k, v in d.items():
        if k in dispatch_types:
            v = dispatch_types[k](v)
        d[k] = v
    return d


class ToltecRawObsDB(object):
    """A class to query toltec raw observation database."""

    def __init__(self, bind: SqlaDB):
        self._bind = bind
        self._bind.reflect_tables()

    @classmethod
    def from_db_config(cls, db_config: DBConfig):
        return cls(bind=db_config.get_db("toltec"))

    _parse_dates_keys = ["Date", "DateTime"]
    _dp_raw_obs_group_keys = ["master", "obsnum", "subobsnum", "scannum", "repeat"]

    def query_obs_latest(
        self, master=None, obs_type=None, table_name="toltec", valid_only=True
    ):
        tname = table_name
        t = self._bind.tables
        where = [True]
        if valid_only:
            where.append(t[tname].c.Valid > 0)
        if master is not None:
            where.append(t["master"].c.label == master.upper())
        if obs_type is not None:
            where.append(t["obstype"].c.label == obs_type)
        where_clause = se.and_(*where)
        cols_map = {
            "master": t["master"].c.label,
            "obsnum": t[tname].c.ObsNum,
            "subobsnum": t[tname].c.SubObsNum,
            "scannum": t[tname].c.ScanNum,
            "obs_type": t["obstype"].c.label,
            "id": t[tname].c.id,
        }
        stmt = (
            se.select(cols_map.values())
            .select_from(
                t[tname]
                .join(t["obstype"], onclause=(t[tname].c.ObsType == t["obstype"].c.id))
                .join(t["master"], onclause=(t[tname].c.Master == t["master"].c.id))
            )
            .where(where_clause)
            .order_by(se.desc(t[tname].c.id))
            .limit(1)
        )
        with self._bind.session_context() as session:
            r = dict(zip(cols_map.keys(), session.execute(stmt).fetchone()))
        logger.debug(
            f'latest obs: uid={make_toltec_raw_obs_uid(r)} obs_type={r["obs_type"]}'
        )
        return r

    def query_id_range(
        self, time_start=None, time_end=None, table_name="toltec", valid_only=True
    ):
        tname = table_name
        t = self._bind.tables
        where = [True]
        if valid_only:
            where.append(t[tname].c.Valid > 0)

        if time_start is not None:
            where.append(
                sqla_func.timestamp(
                    t[tname].c.Date,
                    t[tname].c.Time) >= time_start,
                    )
        if time_end is not None:
            where.append(
                sqla_func.timestamp(
                    t[tname].c.Date,
                    t[tname].c.Time) <= time_end,
                    )
        where_clause = se.and_(*where)
        cols_map = {
            "id_min": sqla_func.min(t[tname].c.id),
            "id_max": sqla_func.max(t[tname].c.id),
        }
        stmt = (
            se.select(cols_map.values())
            .select_from(
                t[tname]
            )
            .where(where_clause)
            .order_by(se.desc(t[tname].c.id))
            .limit(1)
        )
        with self._bind.session_context() as session:
            r = dict(zip(cols_map.keys(), session.execute(stmt).fetchone()))
        id_start, id_end = r['id_min'], r['id_max'] + 1
        logger.debug(
            f'query id time_start={time_start} time_end={time_end}: id_start={id_start} id_end={id_end}'
        )
        return id_start, id_end

    def _resovle_id_range(
        self, id, master=None, obs_type=None, table_name="toltec", valid_only=True
    ):
        # resolve id
        if isinstance(id, int):
            id = slice(id, id + 1)
        elif isinstance(id, slice):
            if id.step is not None and id.step != 1:
                raise ValueError(f"id slice has to be contiguous and incrementing")
        elif id is None:
            id = slice(-1, None)
        else:
            raise ValueError(f"id has to be None, int or slice")
        if id.stop is None:
            # query for latest id
            obs_latest = self.query_obs_latest(
                master=master,
                obs_type=obs_type,
                table_name=table_name,
                valid_only=valid_only,
            )
            id_latest = obs_latest["id"]
        else:
            id_latest = id.stop
        # resolve the id slice to id range
        id_range = range(*id.indices(id_latest + 1))
        return id_range

    def id_query_grouped(
        self, id=None, table_name="toltec", master=None, obs_type=None, valid_only=True
    ):
        id_range = self._resovle_id_range(
            id,
            master=master,
            obs_type=obs_type,
            table_name=table_name,
            valid_only=valid_only,
        )
        tname = table_name
        t = self._bind.tables

        where = [
            t[tname].c.id >= id_range.start,
            t[tname].c.id < id_range.stop,
        ]
        if valid_only:
            where.append(t[tname].c.Valid > 0)
        if master is not None:
            where.append(t["master"].c.label == master.upper())
        if obs_type is not None:
            where.append(t["obstype"].c.label == obs_type)
        select_cols = [
            sqla_func.min(t[tname].c.id).label("id_min"),
            sqla_func.max(t[tname].c.id).label("id_max"),
            sqla_func.max(sqla_func.timestamp(t[tname].c.Date, t[tname].c.Time)).label(
                "time_obs"
            ),
            sqla_func.max(t[tname].c.ObsNum).label("obsnum"),
            sqla_func.max(t[tname].c.SubObsNum).label("subobsnum"),
            sqla_func.max(t[tname].c.ScanNum).label("scannum"),
            sqla_func.max(t[tname].c.RepeatLevel).label("repeat"),
            sqla_func.max(t["obstype"].c.label).label("obs_type"),
            sqla_func.max(t["master"].c.label).label("master"),
            # sqla_func.array_agg(t[tname].c.RoachIndex).label("roachids"),
        ]
        select_tbl = (
            t[tname]
            .join(t["obstype"], onclause=(t[tname].c.ObsType == t["obstype"].c.id))
            .join(t["master"], onclause=(t[tname].c.Master == t["master"].c.id))
        )

        stmt = (
            se.select(select_cols)
            .select_from(select_tbl)
            .where(se.and_(*where))
            .group_by(
                t[tname].c.Master.label("master_id"),
                t[tname].c.ObsNum.label("obsnum"),
                t[tname].c.SubObsNum.label("subobsnum"),
                t[tname].c.ScanNum.label("scannum"),
            )
        )
        with self._bind.session_context() as session:
            df = pd.read_sql_query(
                stmt,
                con=session.bind,
                parse_dates=self._parse_dates_keys,
            )
        # logger.debug(f"query result: \n{df}")
        return df

    def id_query(
        self,
        id=None,
        table_name="toltec",
        with_cal_info=True,
        master=None,
        obs_type=None,
        valid_only=True,
    ):
        id_range = self._resovle_id_range(
            id,
            master=master,
            obs_type=obs_type,
            table_name=table_name,
            valid_only=valid_only,
        )
        logger.debug(f"query toltecdb for id [{id_range.start}:{id_range.stop}]")

        tname = table_name
        t = self._bind.tables
        # add constaint for subobsnums and scannum
        where = [
            t[tname].c.id >= id_range.start,
            t[tname].c.id < id_range.stop,
        ]
        if valid_only:
            where.append(t[tname].c.Valid > 0)
        if master is not None:
            where.append(t["master"].c.label == master.upper())
        if obs_type is not None:
            where.append(t["obstype"].c.label == obs_type)
        select_cols = [
            sqla_func.timestamp(t[tname].c.Date, t[tname].c.Time).label("time_obs"),
            t[tname].c.ObsNum.label("obsnum"),
            t[tname].c.SubObsNum.label("subobsnum"),
            t[tname].c.ScanNum.label("scannum"),
            t[tname].c.RoachIndex.label("roachid"),
            t[tname].c.RepeatLevel.label("repeat"),
            t[tname].c.TargSweepObsNum.label("cal_obsnum"),
            t[tname].c.TargSweepSubObsNum.label("cal_subobsnum"),
            t[tname].c.TargSweepScanNum.label("cal_scannum"),
            t["obstype"].c.label.label("obs_type"),
            t["master"].c.label.label("master"),
            t[tname].c.FileName.label("source"),
        ]
        select_tbl = (
            t[tname]
            .join(t["obstype"], onclause=(t[tname].c.ObsType == t["obstype"].c.id))
            .join(t["master"], onclause=(t[tname].c.Master == t["master"].c.id))
        )
        if with_cal_info:
            # build stmt to join with cal table
            t_cal = sqla_alias(t[tname])
            t_cal_master = sqla_alias(t["master"])
            select_cols.extend(
                [
                    t_cal_master.c.label.label("cal_master"),
                    t_cal.c.FileName.label("cal_source"),
                ]
            )
            select_tbl = select_tbl.join(
                t_cal,
                onclause=(
                    se.and_(
                        t[tname].c.TargSweepObsNum == t_cal.c.ObsNum,
                        t[tname].c.TargSweepSubObsNum == t_cal.c.SubObsNum,
                        t[tname].c.TargSweepScanNum == t_cal.c.ScanNum,
                        t[tname].c.RoachIndex == t_cal.c.RoachIndex,
                        # t[tname].c.Master == t_cal.c.Master,
                    )
                ),
                isouter=True,
            ).join(t_cal_master, onclause=(t_cal.c.Master == t_cal_master.c.id))
        stmt = se.select(select_cols).select_from(select_tbl).where(se.and_(*where))
        with self._bind.session_context() as session:
            df_raw_obs = pd.read_sql_query(
                stmt,
                con=session.bind,
                parse_dates=self._parse_dates_keys,
            )
        return df_raw_obs

    def obs_query(
        self,
        master,
        obsnum=None,
        subobsnum=None,
        scannum=None,
        obs_type=None,
        table_name="toltec",
        with_cal_info=True,
        valid_only=True,
    ):
        # handle obsnum, subobsnu, and scannums.
        # when they are not present, the latest is returned.
        def _validate_num_arg(name, value, allow_negative=False):
            if value is None:
                return slice(-1, None)
            if isinstance(value, int):
                return slice(value, value + 1)
            elif isinstance(value, slice):
                if value.step is not None and value.step != 1:
                    raise ValueError(
                        f"{name} slice has to be contiguous and incrementing"
                    )
                if allow_negative:
                    return value
                # check if start and stop are negative
                if value.start < 0 or value.stop < 0:
                    raise ValueError(f"{name} slice start/stop has to be non-negative")
            raise ValueError(f"{name} has to be None, int or slice")

        obsnum = _validate_num_arg("obsnum", obsnum, allow_negative=True)
        subobsnum = _validate_num_arg("subobsnum", subobsnum)
        scannum = _validate_num_arg("scannum", scannum)
        logger.debug(
            f"query obs {obsnum=} {subobsnum=} {scannum=} {master=} {obs_type=}"
        )
        if obsnum.stop is None:
            # query for latest obsnum
            obs_latest = self.query_obs_latest(
                master=master, obs_type=obs_type, valid_only=valid_only
            )
            logger.debug(f"latest obs: {obs_latest}")
            obsnum_stop = obs_latest["obsnum"] + 1
        else:
            obsnum_stop = obsnum.stop
        # resolve the obsnum slice
        obsnum_range = range(*obsnum.indices(obsnum_stop))
        logger.debug(
            f"query toltecdb for obsnum [{obsnum_range.start}:{obsnum_range.stop}] to find id range"
        )

        # run a query to figure out actual id for obsnum_since to obsnum_latest
        tname = table_name
        t = self._bind.tables
        where = [
            t[tname].c.ObsNum >= obsnum_range.start,
            t[tname].c.ObsNum < obsnum_range.stop,
            t["master"].c.label == master.upper(),
        ]
        if subobsnum.start is not None:
            where.append(t[tname].c.SubObsNum >= subobsnum.start)
        if subobsnum.stop is not None:
            where.append(t[tname].c.SubObsNum < subobsnum.stop)
        if scannum.start is not None:
            where.append(t[tname].c.ScanNum >= scannum.start)
        if scannum.stop is not None:
            where.append(t[tname].c.ScanNum < scannum.stop)
        if valid_only:
            where.append(t[tname].c.Valid > 0)
        if obs_type is not None:
            where.append(t["obstype"].c.label == obs_type)
        stmt = (
            se.select(
                [
                    sqla_func.min(t[tname].c.id).label("id_min"),
                    sqla_func.max(t[tname].c.id).label("id_max"),
                    sqla_func.max(t[tname].c.ObsNum).label("obsnum"),
                    sqla_func.max(t[tname].c.SubObsNum).label("subobsnum"),
                    sqla_func.max(t[tname].c.ScanNum).label("scannum"),
                ]
            )
            .select_from(
                t[tname]
                .join(t["obstype"], onclause=(t[tname].c.ObsType == t["obstype"].c.id))
                .join(t["master"], onclause=(t[tname].c.Master == t["master"].c.id))
            )
            .where(se.and_(*where))
            .group_by(
                t[tname].c.Master.label("master_id"),
                t[tname].c.ObsNum.label("obsnum"),
                t[tname].c.SubObsNum.label("subobsnum"),
                t[tname].c.ScanNum.label("scannum"),
            )
        )
        # .order_by(
        #         se.desc(t[tname].c.id)
        # )
        with self._bind.session_context() as session:
            df_group_ids = pd.read_sql_query(
                stmt,
                con=session.bind,
                parse_dates=self._parse_dates_keys,
            )
        if len(df_group_ids) == 0:
            # no data found
            return None
        id_min = df_group_ids["id_min"].min()
        id_max = df_group_ids["id_max"].max()
        logger.debug(
            f"id range: [{id_min}, {id_max + 1}] "
            f"obsnum range: [{df_group_ids['obsnum'].min()}, {df_group_ids['obsnum'].max() + 1}]"
            f"subobsnum range: [{df_group_ids['subobsnum'].min()}, {df_group_ids['subobsnum'].max() + 1}]"
            f"scannum range: [{df_group_ids['scannum'].min()}, {df_group_ids['scannum'].max() + 1}]"
        )
        df_raw_obs = self.id_query(
            id=slice(id_min, id_max + 1),
            master=master,
            obs_type=obs_type,
            table_name=table_name,
            with_cal_info=with_cal_info,
            valid_only=valid_only,
        )
        df_raw_obs.sort_values(by=self._dp_raw_obs_group_keys)
        return df_raw_obs

    @staticmethod
    def _normalize_meta(meta):
        def _conv(v):
            # fix time_obs type for json serialization
            if isinstance(v, pd.Timestamp):
                return v.to_pydatetime().isoformat()
            return v

        return {k: _conv(v) for k, v in meta.items()}

    def _make_data_item_from_entry(self, entry):
        # TODO handle source/filepath resolving
        meta = self._normalize_meta(entry.copy())
        if "cal_source" in meta:
            meta["cal_filepath"] = meta["cal_source"]
        interface = meta["interface"] = f'toltec{meta["roachid"]}'
        meta["name"] = f"{interface}-{make_toltec_raw_obs_uid(meta)}"
        return {"meta": meta, "filepath": meta["source"]}

    def _make_dp_index_for_raw_obs(self, df):
        # create dp index for single raw obs
        df.sort_values(by=["roachid"])
        entries = df.to_dict(orient="records")
        meta = {
            "data_prod_type": "dp_raw_obs",
            "name": make_toltec_raw_obs_uid(entries[0]),
            **{k: entries[0][k] for k in self._dp_raw_obs_group_keys},
        }
        data_items = list()
        for entry in entries:
            data_items.append(self._make_data_item_from_entry(entry))
        return {"meta": meta, "data_items": data_items}

    def make_dp_index_from_query_result(self, df_raw_obs):
        master = df_raw_obs["master"][0]
        obsnum_min = df_raw_obs["obsnum"].min()
        obsnum_max = df_raw_obs["obsnum"].max()
        n_entries = len(df_raw_obs)
        meta = {
            "data_prod_type": "dp_named_group",
            "name": f"g_{master}_{obsnum_min}_{obsnum_max}",
        }
        data_items = []
        logger.debug(
            f"collect {n_entries} entries from toltec files db"
            f" obsnum range [{obsnum_min}:{obsnum_max}"
        )
        grouped = df_raw_obs.groupby(by=self._dp_raw_obs_group_keys)
        for _, df in grouped:
            data_items.append(self._make_dp_index_for_raw_obs(df))
        return {"meta": meta, "data_items": data_items}

    @staticmethod
    def _squeeze_index(index, squeeze):
        if squeeze and len(index["data_items"]) == 1:
            return index["data_items"][0]
        return index

    def get_dp_index_for_obs(self, squeeze=False, **kwargs):
        index = self.make_dp_index_from_query_result(self.obs_query(**kwargs))
        return self._squeeze_index(index, squeeze)

    def get_dp_index_for_id(self, squeeze=False, **kwargs):
        index = self.make_dp_index_from_query_result(self.id_query(**kwargs))
        return self._squeeze_index(index, squeeze)
