"""
Purpose:
    Shape Slurm data into summaries and aggregations for the SWC Slurm Dashboard.

Execution Flow:
    (Used by swc_slurm_dashboard.py)
      ├── summarise_live_by_name()      # live queue grouped by Name
      ├── summarise_failures_by_name()  # historic failures grouped by JobName
      ├── derive_history_start_from_squeue()
      └── helper functions for parsing and identifiers

Side Effects:
    - None: all functions in this module are pure DataFrame / value transforms.

Inputs:
    - pandas.DataFrame instances produced by the Slurm parsers.

Outputs:
    - Aggregated pandas.DataFrame instances used directly by the UI.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd


# ------------------------------------------------------------------------------
# Live queue summaries
# ------------------------------------------------------------------------------


def summarise_live_by_name(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise live queue data by job name.

    Returns one row per name with RUN/WAIT/TOTAL counts, a status label,
    elapsed time (if running), a sample JobID, and a node/reason string.
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Name",
                "SampleJobID",
                "RUN",
                "WAIT",
                "TOTAL",
                "ELAPSED",
                "Status",
                "NodeReason",
            ]
        )
    failed_states = ("FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY")
    rows: List[dict] = []
    for name, group in df.groupby("Name", dropna=False):
        name = name or "(no name)"
        states = group["State"].tolist()
        reasons = group["Reason"].fillna("").tolist()
        times = group["Time"].fillna("").tolist()
        job_ids = group["JobID"].astype(str).tolist()
        run = sum(s == "RUNNING" for s in states)
        wait = sum(s == "PENDING" for s in states)
        fail = sum(any(fs in s for fs in failed_states) for s in states)
        total = len(group)
        elapsed = "-"
        node_reason = ""
        sample_job_id = ""
        for i, s in enumerate(states):
            if s == "RUNNING":
                if times[i]:
                    elapsed = times[i]
                node_reason = reasons[i] if i < len(reasons) else ""
                sample_job_id = job_ids[i] if i < len(job_ids) else ""
                break
        if not node_reason and reasons:
            node_reason = next((r for r in reasons if r), "")
        if not sample_job_id and job_ids:
            # Fall back to the last job in the group if none are RUNNING.
            sample_job_id = job_ids[-1]
        has_dep_never = any("DependencyNeverSatisfied" in r for r in reasons)
        has_dep = any("Dependency" in r for r in reasons)
        if fail > 0:
            status = "FAILED"
        elif run > 0:
            status = "RUNNING"
        elif wait > 0 and has_dep_never:
            status = "BLOCKED (dependency never satisfied)"
        elif wait > 0 and has_dep:
            status = "WAITING (dependency)"
        elif wait > 0:
            status = "WAITING"
        else:
            status = "UNKNOWN"
        rows.append(
            {
                "Name": name,
                "SampleJobID": sample_job_id,
                "RUN": run,
                "WAIT": wait,
                "TOTAL": total,
                "ELAPSED": elapsed,
                "Status": status,
                "NodeReason": node_reason,
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values("Name").reset_index(drop=True)


def _derive_array_or_job_id(job_id: str) -> str:
    """Return the array ID for a JobID, or the JobID itself if not an array."""
    if not isinstance(job_id, str):
        return ""
    base = job_id.split("_", 1)[0]
    return base


# ------------------------------------------------------------------------------
# History window and elapsed-time parsing
# ------------------------------------------------------------------------------


def _parse_squeue_elapsed_to_seconds(value: str) -> int:
    """
    Best-effort parser for squeue elapsed time strings into seconds.

    Handles formats like:
    - "MM:SS"
    - "HH:MM:SS"
    - "D-HH:MM:SS"
    Returns 0 on any parsing error.
    """
    if not isinstance(value, str):
        return 0
    s = value.strip()
    if not s:
        return 0
    try:
        days = 0
        time_part = s
        if "-" in s:
            days_part, time_part = s.split("-", 1)
            days = int(days_part)
        parts = [int(p) for p in time_part.split(":")]
        if len(parts) == 3:
            hours, mins, secs = parts
        elif len(parts) == 2:
            hours, mins = parts
            secs = 0
        elif len(parts) == 1:
            hours = 0
            mins = parts[0]
            secs = 0
        else:
            return 0
        total = days * 86400 + hours * 3600 + mins * 60 + secs
        return max(total, 0)
    except Exception:
        return 0


def derive_history_start_from_squeue(df: pd.DataFrame) -> tuple[str, str]:
    """Choose a sacct --starttime from the live queue and return it with a label.

    Uses the longest-running task to approximate a start time, or the start
    of today (UTC) if nothing is running.
    """
    now = datetime.now(timezone.utc)
    running = df[df["State"] == "RUNNING"] if not df.empty else pd.DataFrame()
    if not running.empty and "Time" in running.columns:
        elapsed_values = running["Time"].astype(str).tolist()
        elapsed_seconds = [
            _parse_squeue_elapsed_to_seconds(v) for v in elapsed_values
        ]
        elapsed_seconds = [s for s in elapsed_seconds if s > 0]
        if elapsed_seconds:
            start_dt = now - timedelta(seconds=max(elapsed_seconds))
        else:
            start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    label = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    start_for_sacct = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    return start_for_sacct, label


# ------------------------------------------------------------------------------
# Failure summaries
# ------------------------------------------------------------------------------


def summarise_failures_by_name(dfh: pd.DataFrame) -> pd.DataFrame:
    """Summarise historic failed/cancelled/timed-out jobs grouped by JobName.

    Returns one row per name with a failure count plus details from the most
    recent failing job (LastJobID, state, exit code, elapsed, node, MaxRSS,
    and selected resource fields when present).
    """
    if dfh.empty:
        return pd.DataFrame(
            columns=[
                "JobName",
                "Count",
                "LastJobID",
                "LastState",
                "LastExitCode",
                "LastElapsed",
                "LastNode",
                "MaxRSS",
            ]
        )
    state_failure = dfh["State"].str.contains(
        "FAILED|OUT_OF_MEMORY|CANCELLED|TIMEOUT",
        case=False,
        na=False,
    )
    exit_nonzero = dfh["ExitCode"].astype(str).str.len().gt(0) & ~dfh[
        "ExitCode"
    ].astype(str).str.startswith("0:", na=False)
    interesting = dfh[state_failure | exit_nonzero].copy()
    if interesting.empty:
        return pd.DataFrame(
            columns=[
                "JobName",
                "Count",
                "LastJobID",
                "LastState",
                "LastExitCode",
                "LastElapsed",
                "LastNode",
                "MaxRSS",
            ]
        )
    extra = [
        c
        for c in ["ReqMem", "Timelimit", "CPUTime", "WorkDir", "Reason"]
        if c in interesting.columns
    ]
    interesting_sorted = interesting.sort_values("JobID")
    counts = (
        interesting_sorted.groupby("JobName").size().reset_index(name="Count")
    )
    last = interesting_sorted.groupby("JobName", as_index=False).tail(1)
    merged = counts.merge(last, on="JobName", how="left")
    merged = merged.rename(
        columns={
            "JobID": "LastJobID",
            "State": "LastState",
            "ExitCode": "LastExitCode",
            "Elapsed": "LastElapsed",
            "NodeList": "LastNode",
            "MaxRSS": "MaxRSS",
        }
    )
    base_cols = [
        "JobName",
        "Count",
        "LastJobID",
        "LastState",
        "LastExitCode",
        "LastElapsed",
        "LastNode",
        "MaxRSS",
    ]
    cols = base_cols + [c for c in extra if c in merged.columns]
    merged = merged[[c for c in cols if c in merged.columns]]
    return merged.sort_values(["Count", "JobName"], ascending=[False, True])


# ------------------------------------------------------------------------------
# MaxRSS parsing
# ------------------------------------------------------------------------------


def _parse_maxrss_to_gb(value: str) -> float:
    """Best-effort parser for Slurm MaxRSS strings to GiB (returns NaN on error)."""
    if not isinstance(value, str):
        return float("nan")
    s = value.strip()
    if not s:
        return float("nan")
    try:
        m = re.match(r"^([0-9]*\.?[0-9]+)\s*([kKmMgGtT])?.*$", s)
        if not m:
            return float("nan")
        num = float(m.group(1))
        unit = (m.group(2) or "M").upper()
        if unit == "K":
            # KiB -> GiB
            return num / (1024**2)
        if unit == "M":
            # MiB -> GiB
            return num / 1024.0
        if unit == "G":
            # GiB
            return num
        if unit == "T":
            # TiB -> GiB
            return num * 1024.0
        return float("nan")
    except Exception:
        return float("nan")


__all__ = [
    "summarise_live_by_name",
    "summarise_failures_by_name",
    "derive_history_start_from_squeue",
    "_derive_array_or_job_id",
    "_parse_maxrss_to_gb",
]

