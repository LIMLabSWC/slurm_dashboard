"""
Purpose:
    Read Slurm data into pandas DataFrames for the SWC Slurm Dashboard.

Execution Flow:
    (Used by swc_slurm_dashboard.py)
      ├── shell helpers
      │     └── sh() → safe_sh()
      ├── SLURM parsers
      │     ├── parse_squeue() → _squeue_from_json()
      │     ├── parse_sacct()  → _sacct_from_json()
      │     └── list_squeue_users()
      └── job detail helper
            └── scontrol_show_job()

Side Effects:
    - Executes read-only SLURM commands via subprocess: squeue, sacct, scontrol.
    - Relies on the current environment (PATH, USER, SLURM client config).

Inputs:
    - Environment variables (e.g. USER, SLURM configuration).
    - SLURM commands available in PATH.

Outputs:
    - pandas.DataFrame instances representing live queue and historic jobs.
    - Raw `scontrol show job` text for job inspection.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import List, Optional

import pandas as pd


# ------------------------------------------------------------------------------
# Exception for CLI failures
# ------------------------------------------------------------------------------


class SlurmCommandError(Exception):
    """Raised when a SLURM CLI command returns error output instead of data."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


# ------------------------------------------------------------------------------
# Shell helpers
# ------------------------------------------------------------------------------


def sh(cmd: str) -> str:
    """Run a shell command and return stdout, raising on non-zero exit."""
    return subprocess.check_output(
        cmd,
        shell=True,
        text=True,
        stderr=subprocess.STDOUT,
    )


def safe_sh(cmd: str) -> str:
    """Run a shell command and return stdout, capturing errors as text.

    Returns normal stdout on success; on failure returns the error output
    instead of raising.
    """
    try:
        return sh(cmd)
    except subprocess.CalledProcessError as e:
        return e.output or str(e)


# ------------------------------------------------------------------------------
# Slurm column definitions
# ------------------------------------------------------------------------------

SQUEUE_COLUMNS = ["JobID", "State", "Name", "Time", "Reason", "Dependency"]

SACCT_BASE_COLUMNS = [
    "JobID",
    "JobName",
    "State",
    "ExitCode",
    "Elapsed",
    "NodeList",
    "MaxRSS",
]
SACCT_EXTRA_COLUMNS = [
    "ReqMem",
    "Timelimit",
    "CPUTime",
    "WorkDir",
    "SubmitLine",
    "Submit",
    "Reason",
]
SACCT_ALL_COLUMNS = SACCT_BASE_COLUMNS + SACCT_EXTRA_COLUMNS


# ------------------------------------------------------------------------------
# squeue parsing (live queue)
# ------------------------------------------------------------------------------


def _squeue_from_json(out: str, user: str) -> Optional[pd.DataFrame]:
    """Parse `squeue --json` output for one user into a DataFrame.

    Returns a DataFrame with SQUEUE_COLUMNS, or None if parsing fails.
    """
    try:
        data = json.loads(out)
        jobs = data.get("jobs") if isinstance(data, dict) else None
        if not jobs:
            return None
        rows: List[tuple] = []
        for j in jobs:
            if user and j.get("user_name") != user:
                continue
            jid = str(j.get("job_id", ""))
            state_obj = j.get("job_state")
            state = (
                state_obj.get("current", state_obj)
                if isinstance(state_obj, dict)
                else str(state_obj or "")
            )
            name = str(j.get("name") or j.get("job_name") or "")
            elapsed = str(j.get("elapsed_time") or j.get("time") or "")
            reason = str(j.get("reason") or "")
            dep = str(j.get("dependency") or "")
            rows.append((jid, state, name, elapsed, reason, dep))
        return pd.DataFrame(rows, columns=SQUEUE_COLUMNS)
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def parse_squeue(user: str) -> pd.DataFrame:
    """Return the current SLURM queue for a user as a DataFrame.

    Uses JSON output when available, otherwise falls back to a pipe-delimited
    format. Raises SlurmCommandError if `squeue` returns an error.
    """
    cmd_json = f"squeue -u {user} --json 2>/dev/null"
    out_json = safe_sh(cmd_json).strip()
    if out_json and "error" not in out_json.lower():
        df = _squeue_from_json(out_json, user)
        if df is not None:
            return df
    cmd = f"squeue -u {user} -h -o '%i|%T|%j|%M|%R|%E'"
    out = safe_sh(cmd).strip()
    rows: List[tuple] = []
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) != 6:
            continue
        rows.append(tuple(p.strip() for p in parts))
    if not rows and out:
        out_lower = out.lower()
        if (
            out_lower.startswith("squeue:")
            or "squeue: error" in out_lower
            or "command not found" in out_lower
            or ("error" in out_lower and "squeue" in out_lower)
        ):
            raise SlurmCommandError(out.strip())
    return pd.DataFrame(rows, columns=SQUEUE_COLUMNS)


# ------------------------------------------------------------------------------
# sacct parsing (job history)
# ------------------------------------------------------------------------------


def _sacct_from_json(out: str) -> Optional[pd.DataFrame]:
    """Parse `sacct --json` output into a history DataFrame.

    Returns a DataFrame with SACCT_ALL_COLUMNS, or None if parsing fails.
    """
    try:
        data = json.loads(out)
        jobs = data.get("jobs") if isinstance(data, dict) else None
        if not jobs:
            return None
        rows: List[tuple] = []
        for j in jobs:

            def g(*keys: str, default: str = ""):
                for k in keys:
                    v = j.get(k)
                    if v is not None and v != "":
                        return str(v)
                return default

            row = (
                g("job_id", "JobID"),
                g("job_name", "name", "JobName"),
                g("state", "job_state", "State"),
                g("exit_code", "ExitCode"),
                g("elapsed", "Elapsed"),
                g("nodelist", "node_list", "NodeList"),
                g("max_rss", "MaxRSS"),
                g("req_mem", "ReqMem"),
                g("timelimit", "time_limit", "Timelimit"),
                g("cpu_time", "cputime", "CPUTime"),
                g("work_dir", "workdir", "WorkDir"),
                g("submit_line", "SubmitLine"),
                g("submit", "Submit"),
                g("reason", "Reason"),
            )
            rows.append(row)
        return pd.DataFrame(rows, columns=SACCT_ALL_COLUMNS)
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def parse_sacct(user: str, start: str) -> pd.DataFrame:
    """Query SLURM job history for a user and start time as a DataFrame.

    Prefers JSON output, with a `--parsable2` fallback. Returns a DataFrame
    with SACCT_ALL_COLUMNS (possibly empty), or raises SlurmCommandError if
    `sacct` reports an error.
    """
    cmd_json = f"sacct -u {user} --starttime {start} --json 2>/dev/null"
    out_json = safe_sh(cmd_json).strip()
    if out_json and "error" not in out_json.lower():
        df = _sacct_from_json(out_json)
        if df is not None and not df.empty:
            return df
    fmt = (
        "JobID,JobName,State,ExitCode,Elapsed,NodeList,MaxRSS,"
        "ReqMem,Timelimit,CPUTime,WorkDir,SubmitLine,Submit,Reason"
    )
    cmd = (
        f"sacct -u {user} --starttime {start} "
        f"--format={fmt} --parsable2 --noheader"
    )
    out = safe_sh(cmd).strip()
    if out:
        out_lower = out.lower()
        if "sacct: error" in out_lower or (
            "sacct" in out_lower and "error" in out_lower and "|" not in out
        ):
            raise SlurmCommandError(out.strip())
    if not out:
        return pd.DataFrame(columns=SACCT_ALL_COLUMNS)
    rows: List[tuple] = []
    n_cols = len(SACCT_ALL_COLUMNS)
    for line in out.splitlines():
        parts = line.split("|")
        padded = (parts + [""] * n_cols)[:n_cols]
        rows.append(tuple(padded))
    return pd.DataFrame(rows, columns=SACCT_ALL_COLUMNS)


# ------------------------------------------------------------------------------
# Helper queries (users, job detail)
# ------------------------------------------------------------------------------


def list_squeue_users() -> List[str]:
    """Return a sorted list of users seen in `squeue`, plus $USER if needed."""
    out = safe_sh("squeue -h -o '%u' 2>/dev/null").strip()
    users = sorted({u for u in out.splitlines() if u})
    env_user = os.environ.get("USER", "").strip()
    if env_user and env_user not in users:
        users.append(env_user)
    if not users:
        users = [env_user or "unknown"]
    return sorted(users)


def scontrol_show_job(job_id: str) -> str:
    """Return `scontrol show job <job_id>` output, or a validation message."""
    job_id = (job_id or "").strip()
    clean = job_id.replace(" ", "")
    if not clean or not re.match(
        r"^\d+(_\d+)?(\[\d+(-\d+)?(,\d+(-\d+)?)*\])?$", clean
    ):
        return "Invalid or empty job ID."
    out = safe_sh(f"scontrol show job {job_id} 2>&1")
    return out.strip() or "No output."


__all__ = [
    "SACCT_ALL_COLUMNS",
    "SQUEUE_COLUMNS",
    "SlurmCommandError",
    "list_squeue_users",
    "parse_sacct",
    "parse_squeue",
    "scontrol_show_job",
]

