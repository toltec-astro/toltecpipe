"""Dagster sensors for toltecpipe.

Two sensors, both polling toltec MySQL directly:

quartet_sensor
    Detects complete quartets (all enabled interfaces Valid=1) and
    triggers ``ql_map_job`` for each.  Skips ics-master observations.

dataprod_sensor
    Watches ``dataprod_toltec_root`` (NFS-mounted, shared with Machine A)
    for new result directories and triggers ``ingest_catalog_job``.
    Catches both automated runs and manual taco_recipe invocations.
"""

# NOTE: Cannot use `from __future__ import annotations` with Dagster —
# runtime type validation requires actual type objects, not strings.

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from .jobs import ingest_catalog_job, ql_map_job

__all__ = ["quartet_sensor", "dataprod_sensor"]

# Obs goals that toltecpipe should process.
_REDUCIBLE_GOALS = frozenset({
    "pointing", "focus", "astigmatism", "oof",
    "beammap", "azscan", "elscan", "science",
})


class _QuartetTracker:
    """Timeout-based quartet completion tracker (same logic as tolteca_db)."""

    def __init__(self, timeout: float, saved: dict) -> None:
        self.timeout = timeout
        self.states: dict = dict(saved)

    def update(self, key: str, valid_count: int, now: datetime) -> None:
        if key not in self.states:
            if valid_count > 0:
                self.states[key] = {
                    "last_valid_time": now.isoformat(),
                    "valid_count": valid_count,
                }
        else:
            if valid_count > self.states[key]["valid_count"]:
                self.states[key]["last_valid_time"] = now.isoformat()
                self.states[key]["valid_count"] = valid_count

    def is_complete(
        self, key: str, valid_count: int, expected: int, now: datetime
    ) -> tuple[bool, str]:
        if valid_count >= expected:
            return True, f"all {expected} interfaces valid"
        if key in self.states:
            last = datetime.fromisoformat(self.states[key]["last_valid_time"])
            elapsed = (now - last.replace(tzinfo=timezone.utc)).total_seconds()
            if elapsed >= self.timeout:
                return True, f"timeout ({elapsed:.0f}s, {valid_count}/{expected} valid)"
        return False, f"{valid_count}/{expected} valid, waiting"


@sensor(
    name="quartet_sensor",
    target=ql_map_job,
    minimum_interval_seconds=10,
    description=(
        "Poll toltec MySQL for complete quartets (tcs master only) "
        "and dispatch ql_map_job."
    ),
    required_resource_keys={"toltec_db", "validation"},
    default_status=DefaultSensorStatus.RUNNING,
)
def quartet_sensor(context: SensorEvaluationContext):
    """Detect complete tcs quartets and trigger QL map reduction.

    Only fires for master == 'tcs'.  Skips ics tune sweeps.
    Uses run_key for deduplication.

    Parameters
    ----------
    context : SensorEvaluationContext
        Dagster sensor context with cursor for last-check timestamp
        and incomplete-quartet timeout state.
    """
    from .helpers import get_obs_goal_from_tel, query_toltec_db_since

    # --- cursor / state ---
    default_start = _resolve_start_date(context)
    if context.cursor:
        try:
            cursor_data = json.loads(context.cursor)
            last_check = cursor_data.get("last_check", default_start)
            saved_states = cursor_data.get("quartet_states", {})
        except (json.JSONDecodeError, ValueError):
            last_check, saved_states = context.cursor, {}
    else:
        last_check, saved_states = default_start, {}

    last_check_dt = datetime.fromisoformat(last_check.replace("Z", "+00:00"))
    context.log.info(f"Checking toltec_db since {last_check}")

    toltec_db = context.resources.toltec_db
    validation = context.resources.validation

    rows = query_toltec_db_since(last_check_dt, toltec_db)
    if not rows:
        return SkipReason(f"No observations since {last_check}")

    # Group into quartets
    quartets: dict = {}
    for row in rows:
        if row["master"] != "tcs":
            continue  # skip ics tune sweeps
        key = f"{row['master']}-{row['obsnum']}-{row['subobsnum']}-{row['scannum']}"
        if key not in quartets:
            quartets[key] = {
                "master": row["master"],
                "obsnum": row["obsnum"],
                "subobsnum": row["subobsnum"],
                "scannum": row["scannum"],
                "interfaces": {},
                "timestamp": row["timestamp"],
            }
        quartets[key]["interfaces"][row["roach_index"]] = {
            "valid": row["valid"],
            "filename": row["filename"],
        }

    if not quartets:
        return SkipReason("No tcs observations found")

    disabled = set(validation.disabled_roach_indices)
    expected = validation.max_interface_count - len(disabled)
    tracker = _QuartetTracker(validation.validation_timeout_seconds, saved_states)
    now = datetime.now(timezone.utc)
    run_requests = []
    latest_ts = last_check_dt
    batch_size = int(os.getenv("QUARTET_SENSOR_BATCH_SIZE", "50"))

    for key, qdata in quartets.items():
        enabled = {
            ri: d for ri, d in qdata["interfaces"].items()
            if ri not in disabled
        }
        valid_count = sum(1 for d in enabled.values() if d["valid"] == 1)
        tracker.update(key, valid_count, now)
        is_done, reason = tracker.is_complete(key, valid_count, expected, now)

        if not is_done:
            context.log.debug(f"⏳ {key}: {reason}")
            continue

        # Advance cursor timestamp
        obs_ts = qdata["timestamp"]
        if obs_ts and obs_ts > latest_ts:
            latest_ts = obs_ts

        # Check obs_goal — skip if not reducible
        data_lmt_root = context.resources.validation  # just for type; use env
        data_lmt = os.getenv("TOLTECPIPE_DATA_LMT_ROOT", "/data_lmt")
        obs_goal = get_obs_goal_from_tel(qdata["obsnum"], data_lmt)
        if obs_goal and obs_goal not in _REDUCIBLE_GOALS:
            context.log.info(f"⏭  {key}: obs_goal={obs_goal!r} not reducible, skip")
            continue

        run_requests.append(RunRequest(
            run_key=f"ql_map_{key}",
            run_config={
                "ops": {
                    "get_obs_goal": {"config": {
                        "master": qdata["master"],
                        "obsnum": qdata["obsnum"],
                        "subobsnum": qdata["subobsnum"],
                        "scannum": qdata["scannum"],
                    }},
                }
            },
            tags={
                "master": qdata["master"],
                "obsnum": str(qdata["obsnum"]),
                "completion_reason": reason,
            },
        ))
        context.log.info(f"✓ {key} complete ({reason}) — queuing ql_map_job")

        if len(run_requests) >= batch_size:
            context.log.info(f"Batch size {batch_size} reached, continuing next tick")
            break

    # Update cursor
    new_cursor = json.dumps({
        "last_check": latest_ts.isoformat().replace("+00:00", "Z"),
        "quartet_states": {
            k: v for k, v in tracker.states.items()
            if k in quartets and not any(r.run_key == f"ql_map_{k}" for r in run_requests)
        },
    })
    context.update_cursor(new_cursor)

    return run_requests or SkipReason("No complete tcs quartets found")


@sensor(
    name="dataprod_sensor",
    target=ingest_catalog_job,
    minimum_interval_seconds=30,
    description=(
        "Watch dataprod_toltec_root for new result directories and "
        "trigger ingest_catalog_job.  Catches manual taco_recipe runs too."
    ),
    required_resource_keys={"recipes"},
    default_status=DefaultSensorStatus.RUNNING,
)
def dataprod_sensor(context: SensorEvaluationContext):
    """Watch dataprod_toltec for new reduction output directories.

    Checks mtime of each obsnum subdirectory.  Fires for directories
    modified more recently than the last cursor timestamp.

    Parameters
    ----------
    context : SensorEvaluationContext
        Dagster sensor context with mtime cursor.
    """
    last_mtime = float(context.cursor or 0.0)
    dataprod_root = Path(context.resources.recipes.dataprod_toltec_root)

    if not dataprod_root.exists():
        return SkipReason(f"dataprod_toltec_root not found: {dataprod_root}")

    new_dirs = [
        d for d in dataprod_root.iterdir()
        if d.is_dir() and d.stat().st_mtime > last_mtime
    ]

    if not new_dirs:
        return SkipReason("No new dataprod directories")

    run_requests = []
    for d in sorted(new_dirs, key=lambda x: x.stat().st_mtime):
        try:
            obsnum = int(d.name)
        except ValueError:
            continue
        run_requests.append(RunRequest(
            run_key=f"ingest_{d.name}_{int(d.stat().st_mtime)}",
            run_config={
                "ops": {
                    "ingest_ql_result": {"config": {
                        "obsnum": obsnum,
                        "result_dir": str(d),
                    }},
                }
            },
            tags={"obsnum": str(obsnum)},
        ))

    if run_requests:
        new_mtime = max(d.stat().st_mtime for d in new_dirs)
        context.update_cursor(str(new_mtime))
        context.log.info(f"Found {len(run_requests)} new dataprod dirs")
        return run_requests

    return SkipReason("No valid obsnum directories found")


def _resolve_start_date(context: SensorEvaluationContext) -> str:
    """Resolve sensor start date from env vars with priority order."""
    date_str = (
        os.getenv("TOLTECPIPE_SENSOR_START_DATE")
        or os.getenv("DAGSTER_SENSOR_START_DATE")
        or os.getenv("TOLTECA_SIMULATOR_DATE")
    )
    if not date_str:
        date_str = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()
        context.log.info(f"No start date configured, defaulting to 7 days ago: {date_str}")
    if "T" not in date_str:
        date_str = f"{date_str}T00:00:00Z"
    elif not date_str.endswith("Z"):
        date_str = f"{date_str}Z"
    return date_str
