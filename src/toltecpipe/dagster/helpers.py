"""Database query helpers for toltecpipe sensors and ops."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .resources import ToltecDbResource


def query_toltec_db_since(
    since: datetime,
    toltec_db: ToltecDbResource,
) -> list[dict[str, Any]]:
    """Query toltec MySQL for observations updated since *since*.

    Parameters
    ----------
    since : datetime
        Lower bound timestamp (timezone-aware UTC).
    toltec_db : ToltecDbResource
        Database resource.

    Returns
    -------
    list[dict[str, Any]]
        Rows with keys: master, obsnum, subobsnum, scannum,
        roach_index, valid, filename, timestamp.
    """
    # Ensure naive UTC for MySQL comparison
    since_naive = since.replace(tzinfo=None) if since.tzinfo else since
    sql = """
        SELECT
            r.RoachIndex   AS roach_index,
            r.ObsNum       AS obsnum,
            r.SubObsNum    AS subobsnum,
            r.ScanNum      AS scannum,
            r.Valid        AS valid,
            r.FileName     AS filename,
            r.Master       AS master,
            r.DateTime     AS timestamp
        FROM toltec_r1 r
        WHERE r.DateTime >= %s
        ORDER BY r.DateTime ASC
    """
    rows = toltec_db.query(sql, (since_naive,))
    result = []
    for row in rows:
        ts = row.get("timestamp")
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        result.append({
            **row,
            "master": (row.get("master") or "tcs").lower(),
            "timestamp": ts or datetime.now(UTC),
        })
    return result


def query_quartet_interfaces(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    toltec_db: ToltecDbResource,
) -> list[dict[str, Any]]:
    """Fetch all interface rows for a specific quartet.

    Parameters
    ----------
    master, obsnum, subobsnum, scannum : str / int
        Quartet coordinates.
    toltec_db : ToltecDbResource
        Database resource.

    Returns
    -------
    list[dict[str, Any]]
        One row per interface.
    """
    sql = """
        SELECT RoachIndex AS roach_index, Valid AS valid, FileName AS filename
        FROM toltec_r1
        WHERE Master=%s AND ObsNum=%s AND SubObsNum=%s AND ScanNum=%s
    """
    return toltec_db.query(sql, (master, obsnum, subobsnum, scannum))


def get_obs_goal_from_tel(
    obsnum: int,
    data_lmt_root: str,
) -> str | None:
    """Read ObsGoal from the tel NC file for *obsnum*.

    Parameters
    ----------
    obsnum : int
        Observation number.
    data_lmt_root : str
        Root of data_lmt filesystem.

    Returns
    -------
    str | None
        Lower-case obs_goal string, or None if no tel file found.
    """
    import glob as _glob

    pattern = f"{data_lmt_root}/tel/tel_toltec*_{obsnum:06d}_*.nc"
    matches = sorted(_glob.glob(pattern))
    if not matches:
        return None
    tel_file = matches[0]
    try:
        import netCDF4 as nc  # noqa: N813
        with nc.Dataset(tel_file) as ds:
            goal = ds["Data"]["TelescopeBackend"]["TelBamObsGoal"][0]
            if hasattr(goal, "data"):
                goal = goal.data
            return str(goal).strip().lower()
    except Exception:  # noqa: BLE001
        return None
