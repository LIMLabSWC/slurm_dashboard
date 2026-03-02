"""
Purpose:
    SWC Slurm Dashboard — Streamlit dashboard for monitoring SLURM jobs.

Execution Flow:
    (Streamlit entrypoint)
      ├── shell helpers
      │     └── sh() → safe_sh()
      ├── SLURM data
      │     ├── parse_squeue() → _squeue_from_json()
      │     ├── parse_sacct()  → _sacct_from_json()
      │     ├── list_squeue_users()
      │     └── scontrol_show_job()
      ├── cached wrappers
      │     ├── get_squeue_users(), get_squeue(), get_sacct()
      │     └── get_live_by_name(), get_failures_by_name(), get_scontrol_job()
      ├── summaries / helpers
      │     ├── summarise_live_by_name()
      │     └── summarise_failures_by_name()
      └── main UI
            ├── sidebar (user selection + manual refresh)
            └── tabs (Overview, Job inspector, Help)

Side Effects:
    - Runs read-only SLURM commands: squeue, sacct, scontrol.
    - Uses Streamlit caching to limit scheduler load.
    - Relies on the current environment (PATH, USER, SLURM client config).

Inputs:
    - Environment variables (e.g. USER, SLURM configuration).
    - SLURM commands available in PATH.
    - User interaction via Streamlit widgets (sidebar controls, text inputs).

Outputs:
    - Interactive web UI rendered by Streamlit.
    - Tabular summaries of live queue and historic failures.
"""
# ------------------------------------------------------------------------------
# Module layout 
# MAIN = core workflow sections: read data → shape data → show data
# ------------------------------------------------------------------------------

#  1. Styles & layout
#  2. Shell helpers
#  3. MAIN: Slurm parsers (read data)
#  4. Cached wrappers
#  5. MAIN: Summaries / aggregations (shape data)
#  6. MAIN: Sidebar (user + refresh controls)
#  7. MAIN: Tabs (Overview, Job inspector, Help)

from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timezone, timedelta
from typing import List, Optional

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="SWC Slurm Dashboard",
    page_icon="😎",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------------------
# Styles & layout (CSS, page config)
# ------------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .section-title { font-weight: 600; color: #a855f7; margin-top: 1.25rem; margin-bottom: 0.5rem; }
    .help-text { font-size: 0.85rem; color: var(--text-color); opacity: 0.9; margin-bottom: 0.75rem; }
    .legend { font-size: 0.8rem; margin-top: 0.5rem; }
    .status-running { color: #22c55e; }
    .status-waiting { color: #eab308; }
    .status-failed { color: #ef4444; }
    .status-done { color: #06b6d4; }
    .health-ok { color: #22c55e; }
    .health-warn { color: #f97316; }
    .dashboard-meta { font-size: 0.875rem; color: var(--text-color); opacity: 0.75; margin-top: -0.5rem; margin-bottom: 1.5rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------------------------------------------------------
# Shell helpers (sh, safe_sh)
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
    """
    Purpose:
        Run a shell command and return stdout, capturing errors as strings.

    Execution Flow:
        safe_sh()
          └── sh()

    Side Effects:
        - Executes the given shell command.

    Inputs:
        - cmd: Shell command string to execute.

    Outputs:
        - Command stdout on success.
        - Error output or exception text on failure (no exception raised).
    """
    try:
        return sh(cmd)
    except subprocess.CalledProcessError as e:
        return e.output or str(e)


# ------------------------------------------------------------------------------
# MAIN: Slurm parsers (read data from Slurm into DataFrames)
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


def _squeue_from_json(out: str, user: str) -> Optional[pd.DataFrame]:
    """
    Purpose:
        Build a queue DataFrame from `squeue --json` output for a single user.

    Execution Flow:
        _squeue_from_json()
          └── json.loads()

    Side Effects:
        - None (pure transformation of input string).

    Inputs:
        - out: Raw JSON string from `squeue --json`.
        - user: Username to filter jobs for.

    Outputs:
        - pandas.DataFrame with SQUEUE_COLUMNS, or None if parsing fails.
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
    """
    Purpose:
        Obtain the current SLURM queue for a user as a DataFrame.

    Execution Flow:
        parse_squeue()
          ├── safe_sh('squeue --json ...')
          ├── _squeue_from_json()
          └── safe_sh('squeue -o ...')  # pipe-delimited fallback

    Side Effects:
        - Executes `squeue` via the shell (read-only).

    Inputs:
        - user: SLURM username to query.

    Outputs:
        - pandas.DataFrame with one row per job and SQUEUE_COLUMNS.
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
    return pd.DataFrame(rows, columns=SQUEUE_COLUMNS)


def _sacct_from_json(out: str) -> Optional[pd.DataFrame]:
    """
    Purpose:
        Build a history DataFrame from `sacct --json` output.

    Execution Flow:
        _sacct_from_json()
          └── json.loads()

    Side Effects:
        - None (pure transformation of input string).

    Inputs:
        - out: Raw JSON string from `sacct --json`.

    Outputs:
        - pandas.DataFrame with SACCT_ALL_COLUMNS, or None if parsing fails.
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
    """
    Purpose:
        Query SLURM job history for a user and time window as a DataFrame.

    Execution Flow:
        parse_sacct()
          ├── safe_sh('sacct --json ...')
          ├── _sacct_from_json()
          └── safe_sh('sacct --parsable2 ...')  # extended format fallback

    Side Effects:
        - Executes `sacct` via the shell (read-only).

    Inputs:
        - user: SLURM username to query.
        - start: Start time string accepted by `sacct --starttime`.

    Outputs:
        - pandas.DataFrame with SACCT_ALL_COLUMNS (may be empty).
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
    if not out or "sacct: error" in out.lower():
        return pd.DataFrame(columns=SACCT_ALL_COLUMNS)
    rows: List[tuple] = []
    n_cols = len(SACCT_ALL_COLUMNS)
    for line in out.splitlines():
        parts = line.split("|")
        padded = (parts + [""] * n_cols)[:n_cols]
        rows.append(tuple(padded))
    return pd.DataFrame(rows, columns=SACCT_ALL_COLUMNS)


def list_squeue_users() -> List[str]:
    """
    Purpose:
        List distinct users currently present in the SLURM queue, plus $USER.

    Execution Flow:
        list_squeue_users()
          └── safe_sh('squeue -o %u')

    Side Effects:
        - Executes `squeue` via the shell (read-only).

    Inputs:
        - None (uses environment variable USER implicitly).

    Outputs:
        - Sorted list of usernames.
    """
    out = safe_sh("squeue -h -o '%u' 2>/dev/null").strip()
    users = sorted({u for u in out.splitlines() if u})
    env_user = os.environ.get("USER", "").strip()
    if env_user and env_user not in users:
        users.append(env_user)
    if not users:
        users = [env_user or "unknown"]
    return sorted(users)


def scontrol_show_job(job_id: str) -> str:
    """
    Purpose:
        Return the raw output of `scontrol show job <job_id>` for inspection.

    Execution Flow:
        scontrol_show_job()
          └── safe_sh('scontrol show job ...')

    Side Effects:
        - Executes `scontrol show job` via the shell (read-only).

    Inputs:
        - job_id: SLURM job ID or array task specifier.

    Outputs:
        - Formatted `scontrol show job` output, or a validation/error message.
    """
    job_id = (job_id or "").strip()
    clean = job_id.replace(" ", "")
    if not clean or not re.match(
        r"^\d+(_\d+)?(\[\d+(-\d+)?(,\d+(-\d+)?)*\])?$", clean
    ):
        return "Invalid or empty job ID."
    out = safe_sh(f"scontrol show job {job_id} 2>&1")
    return out.strip() or "No output."


# ------------------------------------------------------------------------------
# Cached wrappers (get_* helpers)
# ------------------------------------------------------------------------------


@st.cache_data(ttl=300)
def get_squeue_users() -> List[str]:
    return list_squeue_users()


@st.cache_data(ttl=15)
def get_squeue(user: str) -> pd.DataFrame:
    return parse_squeue(user)


@st.cache_data(ttl=120)
def get_sacct(user: str, start: str) -> pd.DataFrame:
    return parse_sacct(user, start)


@st.cache_data(ttl=30)
def get_live_by_name(df: pd.DataFrame) -> pd.DataFrame:
    return summarise_live_by_name(df)


@st.cache_data(ttl=120)
def get_failures_by_name(dfh: pd.DataFrame) -> pd.DataFrame:
    return summarise_failures_by_name(dfh)


@st.cache_data(ttl=10)
def get_scontrol_job(job_id: str) -> str:
    """Cached scontrol show job (short TTL so recent jobs show up)."""
    return scontrol_show_job(job_id)


# ------------------------------------------------------------------------------
# MAIN: Summaries / aggregations (shape data for display)
# ------------------------------------------------------------------------------


def summarise_live_by_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Purpose:
        Aggregate live queue data by job name with status and a sample JobID.

    Execution Flow:
        summarise_live_by_name()
          └── groupby('Name') and compute summary metrics per name.

    Side Effects:
        - None (pure DataFrame transformation).

    Inputs:
        - df: DataFrame from `parse_squeue`, including JobID, State, Time, Reason.

    Outputs:
        - DataFrame with one row per job name, counts, elapsed, status,
          node reason, and a representative SampleJobID.
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
    """
    Derive an array-or-job identifier from a SLURM JobID.

    For array tasks like '12345_3', this returns '12345'.
    For non-array jobs, this returns the original JobID.
    """
    if not isinstance(job_id, str):
        return ""
    base = job_id.split("_", 1)[0]
    return base


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
    """
    Derive a sacct --starttime and human-readable label from the live queue.

    - If there are RUNNING tasks, we approximate the earliest submit time as
      "now - max(elapsed)", using the squeue Time column.
    - If there are no RUNNING tasks, we default to the start of today (UTC).

    Returns:
        (starttime_for_sacct, label_for_ui)
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


def summarise_failures_by_name(dfh: pd.DataFrame) -> pd.DataFrame:
    """
    Purpose:
        Summarise historic failed/cancelled/timed-out jobs grouped by name.

    Execution Flow:
        summarise_failures_by_name()
          ├── filter interesting failure states
          ├── count failures per JobName
          └── attach most recent failure details per name

    Side Effects:
        - None (pure DataFrame transformation).

    Inputs:
        - dfh: DataFrame from `parse_sacct` for a given time window.

    Outputs:
        - DataFrame with one row per JobName and aggregated failure details.
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


if hasattr(st, "fragment"):

    @st.fragment(run_every=1)
    def render_refresh_age(started_at_ts: float) -> None:
        elapsed_s = max(
            0,
            int(datetime.now(timezone.utc).timestamp() - started_at_ts),
        )
        hours, rem = divmod(elapsed_s, 3600)
        mins, secs = divmod(rem, 60)
        st.caption(f"Elapsed since refresh: {hours:02}:{mins:02}:{secs:02}")

else:

    def render_refresh_age(started_at_ts: float) -> None:
        elapsed_s = max(
            0,
            int(datetime.now(timezone.utc).timestamp() - started_at_ts),
        )
        hours, rem = divmod(elapsed_s, 3600)
        mins, secs = divmod(rem, 60)
        st.caption(f"Elapsed since refresh: {hours:02}:{mins:02}:{secs:02}")


# ------------------------------------------------------------------------------
# MAIN: Sidebar (user, page, manual refresh)
# ------------------------------------------------------------------------------

default_user = os.environ.get("USER", "unknown")
all_users = get_squeue_users()
try:
    default_index = all_users.index(default_user)
except ValueError:
    default_index = 0

with st.sidebar:
    selected_user = st.selectbox(
        "SLURM user",
        options=all_users,
        index=default_index,
    )
    if "last_manual_refresh_ts" not in st.session_state:
        st.session_state["last_manual_refresh_ts"] = datetime.now(
            timezone.utc
        ).timestamp()

    refresh_ts = float(st.session_state["last_manual_refresh_ts"])
    render_refresh_age(refresh_ts)
    if st.button("Refresh now"):
        # Manual refresh should bypass cache TTL and fetch fresh data now.
        st.cache_data.clear()
        st.session_state["last_manual_refresh_ts"] = datetime.now(
            timezone.utc
        ).timestamp()
        st.rerun()

now_utc = datetime.now(timezone.utc).strftime("%a %d %b %H:%M:%S UTC %Y")

# ------------------------------------------------------------------------------
# MAIN: Pages as tabs (Overview, Job inspector, Help)
# ------------------------------------------------------------------------------

st.title("SWC Slurm Dashboard")
st.markdown(
    f'<p class="dashboard-meta">User: {selected_user} &nbsp;·&nbsp; '
    f"Last updated: {now_utc}</p>",
    unsafe_allow_html=True,
)

tab_overview, tab_inspector, tab_help = st.tabs(
    ["Overview", "Job inspector", "Help"]
)

with tab_overview:
    df = get_squeue(selected_user)
    if df.empty:
        total_jobs, running, pending, dep_bad = 0, 0, 0, 0
        running_names = []
        running_job_ids: List[str] = []
    else:
        total_jobs = int(len(df))
        running = int((df["State"] == "RUNNING").sum())
        pending = int((df["State"] == "PENDING").sum())
        dep_bad = int(
            df["Reason"]
            .str.contains("DependencyNeverSatisfied", na=False)
            .sum()
        )
        running_names = (
            df.loc[df["State"] == "RUNNING", "Name"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        running_job_ids = (
            df.loc[df["State"] == "RUNNING", "JobID"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

    st.markdown(
        '<p class="section-title">SUMMARY</p>', unsafe_allow_html=True
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TOTAL jobs", total_jobs)
    c2.metric("RUNNING jobs", running)
    c3.metric("WAITING jobs", pending)
    c4.metric("DEP problems", dep_bad)
    if dep_bad > 0:
        st.markdown(
            '<p class="health-warn">HEALTH: ⚠ ATTENTION NEEDED</p>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<p class="health-ok">HEALTH: OK</p>', unsafe_allow_html=True
        )

    if df.empty:
        st.info("No jobs in queue.")
    else:
        st.markdown(
            '<p class="section-title">QUEUED JOBS (by name)</p>',
            unsafe_allow_html=True,
        )
        with st.expander("How to read this", expanded=False):
            st.markdown(
                "- Rows are grouped by **JOB NAME**, so each row summarizes "
                "all queue entries with that name.\n"
                "- `SAMPLE JOB ID` is one representative job for that row "
                "(for job arrays this is a single job array element such as "
                "`12345_0`).\n"
                "- `RUN`, `WAIT`, and `TOTAL` are counts in that group (with "
                "`TOTAL` roughly equal to `RUN + WAIT` for live queue "
                "states). Finished and failed jobs are primarily surfaced via "
                "the **Finished jobs** and **Failures** sections below, using "
                "`sacct`.\n"
                "- `ELAPSED` shows runtime for a RUNNING job in that group; "
                "if none are running, it is `-`.\n"
                "- `STATUS (summary)` is the row-level state used for quick "
                "scanning (e.g. RUNNING, WAITING, BLOCKED, FAILED).\n"
                "- `NODE / REASON` shows a node name for running jobs, or the "
                "scheduler reason for waiting jobs (for example dependency).\n"
                "- `BLOCKED (dependency never satisfied)` is the key warning "
                "state to prioritize."
            )
            st.markdown(
                '<p class="legend">'
                'Legend (STATUS column): '
                '<span class="status-running">RUNNING</span>, '
                '<span class="status-waiting">WAITING</span>, '
                '<span class="status-failed">FAILED / BLOCKED</span></p>',
                unsafe_allow_html=True,
            )
        df_by_name = get_live_by_name(df)
        display_cols = [
            "Name",
            "SampleJobID",
            "RUN",
            "WAIT",
            "TOTAL",
            "ELAPSED",
            "Status",
            "NodeReason",
        ]
        df_display = df_by_name[display_cols].rename(
            columns={
                "Name": "JOB NAME",
                "SampleJobID": "SAMPLE JOB ID",
                "Status": "STATUS (summary)",
                "NodeReason": "NODE / REASON",
            }
        )

        def _status_css(val: str) -> str:
            if not isinstance(val, str):
                return ""
            if "FAILED" in val or "BLOCKED" in val:
                return "color: #ef4444; font-weight: 500;"
            if "RUNNING" in val:
                return "color: #22c55e; font-weight: 500;"
            if "WAITING" in val or "PENDING" in val:
                return "color: #eab308; font-weight: 500;"
            return ""

        try:
            styled = df_display.style.apply(
                lambda col: (
                    [_status_css(v) for v in col]
                    if col.name == "STATUS (summary)"
                    else [""] * len(col)
                ),
                axis=0,
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)
        except Exception:
            st.dataframe(df_display, use_container_width=True, hide_index=True)

    history_start, history_since_label = derive_history_start_from_squeue(df)
    dfh_window = get_sacct(selected_user, history_start)
    if dfh_window.empty:
        st.info(
            f"No sacct data (or sacct not available) since: {history_since_label}."
        )
    else:

        # ---------------- FINISHED JOBS: related vs other ----------------
        success_mask = dfh_window["State"].str.contains(
            "COMPLETED", case=False, na=False
        ) & dfh_window["ExitCode"].str.startswith("0:", na=False)
        df_success_all = dfh_window[success_mask].copy()

        st.markdown(
            f'<p class="section-title">FINISHED JOBS (since: {history_since_label})</p>',
            unsafe_allow_html=True,
        )
        with st.expander("How to read this", expanded=False):
            st.markdown(
                "- Shows jobs where `State` is `COMPLETED` and `ExitCode` "
                "starts with `0:` (successful exits) for this user.\n"
                "- The **since** date in the heading is the start of the "
                "history window, derived from the live queue: it starts roughly "
                "when your longest-running current job started (based on the "
                "elapsed time reported by `squeue`), or from the beginning of "
                "today (UTC) if nothing is running.\n"
                "- The **related** table lists finished jobs whose **array job "
                "ID** matches an array that currently has at least one RUNNING "
                "job in the queue; the **other** table lists all remaining "
                "finished jobs in this time window.\n"
                "- Each table is flat (one row per JobID), so you can sort and "
                "search directly without extra nesting."
            )

        if df_success_all.empty:
            st.info(
                f"No successfully completed jobs found since: {history_since_label}."
            )
        else:
            df_success_all["ArrayOrJobID"] = df_success_all["JobID"].astype(
                str
            ).apply(_derive_array_or_job_id)
            if running_job_ids:
                running_array_ids = {
                    _derive_array_or_job_id(j) for j in running_job_ids
                }
                df_success_all["RelatedToRunning"] = df_success_all[
                    "ArrayOrJobID"
                ].isin(running_array_ids)
            else:
                df_success_all["RelatedToRunning"] = False

            df_success_related = df_success_all[df_success_all["RelatedToRunning"]]
            df_success_other = df_success_all[~df_success_all["RelatedToRunning"]]

            def _render_finished_block(label: str, df_subset: pd.DataFrame) -> None:
                st.markdown(f"**{label}**")
                if df_subset.empty:
                    st.info("No finished jobs in this category for this window.")
                    return
                detail_cols = [
                    "ArrayOrJobID",
                    "JobID",
                    "JobName",
                    "State",
                    "ExitCode",
                    "Elapsed",
                    "NodeList",
                ]
                detail_display = df_subset[detail_cols].rename(
                    columns={
                        "ArrayOrJobID": "ARRAY JOB ID",
                        "JobID": "JOB ID",
                        "JobName": "JOB NAME",
                        "State": "STATE",
                        "ExitCode": "EXIT CODE",
                        "Elapsed": "ELAPSED",
                        "NodeList": "NODELIST",
                    }
                )
                st.dataframe(
                    detail_display,
                    use_container_width=True,
                    hide_index=True,
                )

            _render_finished_block(
                "Related to running job names", df_success_related
            )
            _render_finished_block("Other finished jobs", df_success_other)

        # ---------------- FAILURES: related vs other ----------------
        df_fail_all = get_failures_by_name(dfh_window)
        st.markdown(
            f'<p class="section-title">FAILURES (since: {history_since_label})</p>',
            unsafe_allow_html=True,
        )
        with st.expander("How to read this", expanded=False):
            st.markdown(
                "- Includes jobs in these states: `FAILED`, `CANCELLED`, "
                "`TIMEOUT`, `OUT_OF_MEMORY`, or any job with a non-zero "
                "`ExitCode` for this user.\n"
                "- The **since** date in the heading is the start of the "
                "history window: it is the earliest `Submit` time among your "
                "currently running jobs when available, or the beginning of "
                "available accounting history otherwise.\n"
                "- The **related** table shows failures whose `JobName` "
                "currently has at least one RUNNING job in the queue; the "
                "**other** table shows all remaining failures in this time "
                "window.\n"
                "- Each row is grouped by `JobName` and includes counts and "
                "the most recent failing `JobID` with its exit code and "
                "resource usage."
            )

        if df_fail_all.empty:
            st.success(f"No failures found since: {history_since_label}.")
        else:
            df_fail_all["RelatedToRunning"] = (
                df_fail_all["JobName"].isin(running_names)
                if running_names
                else False
            )
            df_fail_related = df_fail_all[df_fail_all["RelatedToRunning"]].drop(
                columns=["RelatedToRunning"], errors="ignore"
            )
            df_fail_other = df_fail_all[~df_fail_all["RelatedToRunning"]].drop(
                columns=["RelatedToRunning"], errors="ignore"
            )

            def _render_fail_block(label: str, df_subset: pd.DataFrame) -> None:
                st.markdown(f"**{label}**")
                if df_subset.empty:
                    st.info("No failures in this category for this window.")
                    return
                st.dataframe(
                    df_subset,
                    use_container_width=True,
                    hide_index=True,
                )

            _render_fail_block(
                "Related to running job names", df_fail_related
            )
            _render_fail_block("Other failures", df_fail_other)

with tab_inspector:
    st.markdown(
        "<p class='help-text'>Run <code>scontrol show job &lt;JobID&gt;</code> "
        "(read-only). Enter a job ID or pick one from the queue.</p>",
        unsafe_allow_html=True,
    )
    df_q = get_squeue(selected_user)
    job_ids = df_q["JobID"].tolist() if not df_q.empty else []
    col_input, col_pick = st.columns(2)
    with col_input:
        job_id_input = st.text_input(
            "Job ID",
            placeholder="e.g. 12345 or 12345_3",
            key="job_inspector_id",
        )
    with col_pick:
        if job_ids:
            picked = st.selectbox(
                "Or pick from your queue",
                options=[""] + job_ids,
                key="job_inspector_pick",
            )
        else:
            picked = ""
    job_id = (picked if picked else job_id_input).strip()
    if job_id:
        out = get_scontrol_job(job_id)
        st.code(out, language="text")
    else:
        st.info("Enter a job ID or pick one from the queue.")

with tab_help:
    st.markdown(
        "<p class='help-text'>Overview of SLURM jobs, arrays, job names, and "
        "how they map to each section of the dashboard.</p>",
        unsafe_allow_html=True,
    )
    help_path = os.path.join(
        os.path.dirname(__file__), "SLURM_DASHBOARD_HELP.md"
    )
    try:
        with open(help_path, "r", encoding="utf-8") as f:
            help_md = f.read()
        st.markdown(help_md)
    except OSError:
        st.error(
            "Help content file `SLURM_DASHBOARD_HELP.md` not found or "
            "unreadable. Please check the repository."
        )
