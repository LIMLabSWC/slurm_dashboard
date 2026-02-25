"""
Viktor's Slurm Portal — Streamlit app for monitoring SLURM jobs.

Features:
- Overview: live queue summary, jobs by name, historic failures (with richer sacct fields).
- Job inspector: scontrol show job <id> (read-only).
- Generate commands: build sbatch/salloc commands to copy-paste (no execution).

Uses JSON output when available (squeue/sacct --json), else fallback to
pipe-delimited / parsable2. Caching to avoid hammering the scheduler.

Side effects: runs squeue, sacct, scontrol (read-only). Never submits or cancels jobs.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timezone
from typing import List, Optional

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Viktor's Slurm Portal",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------- Styles ---------------
st.markdown(
    """
    <style>
    .section-title { font-weight: 600; color: #0ea5e9; margin-top: 1.25rem; margin-bottom: 0.5rem; }
    .help-text { font-size: 0.85rem; color: var(--text-color); opacity: 0.9; margin-bottom: 0.75rem; }
    .legend { font-size: 0.8rem; margin-top: 0.5rem; }
    .status-running { color: #22c55e; }
    .status-waiting { color: #eab308; }
    .status-failed { color: #ef4444; }
    .status-done { color: #06b6d4; }
    .health-ok { color: #22c55e; }
    .health-warn { color: #eab308; }
    .dashboard-meta { font-size: 0.875rem; color: var(--text-color); opacity: 0.75; margin-top: -0.5rem; margin-bottom: 1.5rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# --------------- Shell helpers ---------------


def sh(cmd: str) -> str:
    return subprocess.check_output(
        cmd,
        shell=True,
        text=True,
        stderr=subprocess.STDOUT,
    )


def safe_sh(cmd: str) -> str:
    try:
        return sh(cmd)
    except subprocess.CalledProcessError as e:
        return e.output or str(e)


# --------------- SLURM parsers (JSON-first, fallback pipe/parsable2) ---------------

SQUEUE_COLUMNS = ["JobID", "State", "Name", "Time", "Reason", "Dependency"]
SACCT_BASE_COLUMNS = [
    "JobID", "JobName", "State", "ExitCode", "Elapsed", "NodeList", "MaxRSS",
]
SACCT_EXTRA_COLUMNS = ["ReqMem", "Timelimit", "CPUTime", "WorkDir", "SubmitLine"]
SACCT_ALL_COLUMNS = SACCT_BASE_COLUMNS + SACCT_EXTRA_COLUMNS


def _squeue_from_json(out: str, user: str) -> Optional[pd.DataFrame]:
    """Build queue DataFrame from squeue --json. Returns None if parse fails."""
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
    """Parse squeue for user: try JSON first, else pipe-delimited."""
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
    """Build sacct DataFrame from sacct --json. Returns None if parse fails."""
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
            )
            rows.append(row)
        return pd.DataFrame(rows, columns=SACCT_ALL_COLUMNS)
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def parse_sacct(user: str, start: str) -> pd.DataFrame:
    """Parse sacct for user and time window: try JSON first, else --parsable2 with extended format."""
    cmd_json = (
        f"sacct -u {user} --starttime {start} --json 2>/dev/null"
    )
    out_json = safe_sh(cmd_json).strip()
    if out_json and "error" not in out_json.lower():
        df = _sacct_from_json(out_json)
        if df is not None and not df.empty:
            return df
    fmt = (
        "JobID,JobName,State,ExitCode,Elapsed,NodeList,MaxRSS,"
        "ReqMem,Timelimit,CPUTime,WorkDir,SubmitLine"
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
    out = safe_sh("squeue -h -o '%u' 2>/dev/null").strip()
    users = sorted({u for u in out.splitlines() if u})
    env_user = os.environ.get("USER", "").strip()
    if env_user and env_user not in users:
        users.append(env_user)
    if not users:
        users = [env_user or "unknown"]
    return sorted(users)


def scontrol_show_job(job_id: str) -> str:
    """Return output of scontrol show job <job_id>. Read-only."""
    job_id = (job_id or "").strip()
    clean = job_id.replace(" ", "")
    if not clean or not re.match(r"^\d+(_\d+)?(\[\d+(-\d+)?(,\d+(-\d+)?)*\])?$", clean):
        return "Invalid or empty job ID."
    out = safe_sh(f"scontrol show job {job_id} 2>&1")
    return out.strip() or "No output."


# --------------- Cached wrappers ---------------


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


# --------------- Summarisation (from slurm_dashboard) ---------------


def summarise_live_by_name(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Name", "RUN", "WAIT", "DONE", "FAIL", "TOTAL",
                "ELAPSED", "Status", "NodeReason",
            ]
        )
    failed_states = ("FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY")
    rows: List[dict] = []
    for name, group in df.groupby("Name", dropna=False):
        name = name or "(no name)"
        states = group["State"].tolist()
        reasons = group["Reason"].fillna("").tolist()
        times = group["Time"].fillna("").tolist()
        run = sum(s == "RUNNING" for s in states)
        wait = sum(s == "PENDING" for s in states)
        done = sum(s == "COMPLETED" for s in states)
        fail = sum(any(fs in s for fs in failed_states) for s in states)
        total = len(group)
        elapsed = "-"
        node_reason = ""
        for i, s in enumerate(states):
            if s == "RUNNING":
                if times[i]:
                    elapsed = times[i]
                node_reason = reasons[i] if i < len(reasons) else ""
                break
        if not node_reason and reasons:
            node_reason = next((r for r in reasons if r), "")
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
        elif done > 0:
            status = "DONE"
        else:
            status = "UNKNOWN"
        rows.append({
            "Name": name,
            "RUN": run,
            "WAIT": wait,
            "DONE": done,
            "FAIL": fail,
            "TOTAL": total,
            "ELAPSED": elapsed,
            "Status": status,
            "NodeReason": node_reason,
        })
    out = pd.DataFrame(rows)
    return out.sort_values("Name").reset_index(drop=True)


def summarise_failures_by_name(dfh: pd.DataFrame) -> pd.DataFrame:
    if dfh.empty:
        return pd.DataFrame(
            columns=[
                "JobName", "Count", "LastJobID", "LastState", "LastExitCode",
                "LastElapsed", "LastNode", "MaxRSS",
            ]
        )
    interesting = dfh[
        dfh["State"].str.contains(
            "FAILED|OUT_OF_MEMORY|CANCELLED|TIMEOUT",
            case=False,
            na=False,
        )
    ].copy()
    if interesting.empty:
        return pd.DataFrame(
            columns=[
                "JobName", "Count", "LastJobID", "LastState", "LastExitCode",
                "LastElapsed", "LastNode", "MaxRSS",
            ]
        )
    extra = [c for c in ["ReqMem", "Timelimit", "CPUTime", "WorkDir"] if c in interesting.columns]
    interesting_sorted = interesting.sort_values("JobID")
    counts = interesting_sorted.groupby("JobName").size().reset_index(name="Count")
    last = interesting_sorted.groupby("JobName", as_index=False).tail(1)
    merged = counts.merge(last, on="JobName", how="left")
    merged = merged.rename(columns={
        "JobID": "LastJobID",
        "State": "LastState",
        "ExitCode": "LastExitCode",
        "Elapsed": "LastElapsed",
        "NodeList": "LastNode",
        "MaxRSS": "MaxRSS",
    })
    base_cols = [
        "JobName", "Count", "LastJobID", "LastState", "LastExitCode",
        "LastElapsed", "LastNode", "MaxRSS",
    ]
    cols = base_cols + [c for c in extra if c in merged.columns]
    merged = merged[[c for c in cols if c in merged.columns]]
    return merged.sort_values(["Count", "JobName"], ascending=[False, True])


# --------------- Command builders (no execution) ---------------


def build_sbatch(
    job_name: str,
    partition: str,
    time_limit: str,
    mem: str,
    script_path: str,
    cpus_per_task: str = "1",
    extra_args: str = "",
) -> str:
    parts = ["sbatch"]
    if job_name:
        parts.append(f" --job-name={job_name}")
    if partition:
        parts.append(f" --partition={partition}")
    if time_limit:
        parts.append(f" --time={time_limit}")
    if mem:
        parts.append(f" --mem={mem}")
    if cpus_per_task:
        parts.append(f" --cpus-per-task={cpus_per_task}")
    if extra_args.strip():
        parts.append(" " + extra_args.strip())
    parts.append(f" {script_path}" if script_path else " your_script.sh")
    return "".join(parts).strip()


def build_salloc(
    job_name: str,
    partition: str,
    time_limit: str,
    mem: str,
    cpus_per_task: str = "1",
    n_tasks: str = "1",
    extra_args: str = "",
    command: str = "",
) -> str:
    parts = ["salloc"]
    if job_name:
        parts.append(f" --job-name={job_name}")
    if partition:
        parts.append(f" --partition={partition}")
    if time_limit:
        parts.append(f" --time={time_limit}")
    if mem:
        parts.append(f" --mem={mem}")
    if cpus_per_task:
        parts.append(f" --cpus-per-task={cpus_per_task}")
    if n_tasks:
        parts.append(f" --ntasks={n_tasks}")
    if extra_args.strip():
        parts.append(" " + extra_args.strip())
    base = "".join(parts).strip()
    if command.strip():
        return f"{base} {command.strip()}"
    return base + "  # then run your commands in the allocated shell"


# --------------- Sidebar: user + page ---------------

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
    page = st.radio(
        "Page",
        ["Overview", "Job inspector", "Generate commands"],
        index=0,
        label_visibility="collapsed",
    )
    refresh_s = st.slider(
        "Refresh interval (s)",
        min_value=0,
        max_value=300,
        value=0,
        help="0 = no auto-refresh",
    )

now_utc = datetime.now(timezone.utc).strftime("%a %d %b %H:%M:%S UTC %Y")

# --------------- Page: Overview ---------------

if page == "Overview":
    st.title("Viktor's Slurm Portal")
    st.markdown(
        f'<p class="dashboard-meta">User: {selected_user} &nbsp;·&nbsp; {now_utc}</p>',
        unsafe_allow_html=True,
    )

    df = get_squeue(selected_user)
    if df.empty:
        running, pending, dep_bad = 0, 0, 0
    else:
        running = int((df["State"] == "RUNNING").sum())
        pending = int((df["State"] == "PENDING").sum())
        dep_bad = int(
            df["Reason"].str.contains("DependencyNeverSatisfied", na=False).sum()
        )

    st.markdown('<p class="section-title">LIVE SUMMARY</p>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("RUNNING jobs", running)
    c2.metric("WAITING jobs", pending)
    c3.metric("DEP problems", dep_bad)
    if dep_bad > 0:
        c4.markdown('<p class="health-warn">HEALTH: ⚠ ATTENTION NEEDED</p>', unsafe_allow_html=True)
    else:
        c4.markdown('<p class="health-ok">HEALTH: OK</p>', unsafe_allow_html=True)

    if df.empty:
        st.info("No jobs in queue.")
    else:
        with st.expander("How to read this", expanded=False):
            st.markdown(
                "- Rows grouped by **JOB NAME**. RUN/WAIT/DONE/FAIL/TOTAL = counts. "
                "ELAPSED = time for a RUNNING task. STATUS = summary from SLURM."
            )
        st.markdown('<p class="section-title">JOBS BY NAME</p>', unsafe_allow_html=True)
        df_by_name = get_live_by_name(df)
        display_cols = [
            "Name", "RUN", "WAIT", "DONE", "FAIL", "TOTAL",
            "ELAPSED", "Status", "NodeReason",
        ]
        df_display = df_by_name[display_cols].rename(columns={
            "Name": "JOB NAME",
            "Status": "STATUS (summary)",
            "NodeReason": "NODE / REASON",
        })

        def _status_css(val: str) -> str:
            if not isinstance(val, str):
                return ""
            if "FAILED" in val or "BLOCKED" in val:
                return "color: #ef4444; font-weight: 500;"
            if "RUNNING" in val:
                return "color: #22c55e; font-weight: 500;"
            if "WAITING" in val or "PENDING" in val:
                return "color: #eab308; font-weight: 500;"
            if "DONE" in val or "COMPLETED" in val:
                return "color: #06b6d4; font-weight: 500;"
            return ""

        try:
            styled = df_display.style.apply(
                lambda col: [_status_css(v) for v in col]
                if col.name == "STATUS (summary)" else [""] * len(col),
                axis=0,
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)
        except Exception:
            st.dataframe(df_display, use_container_width=True, hide_index=True)
        st.markdown(
            '<p class="legend">'
            'Legend: <span class="status-running">RUNNING</span>, '
            '<span class="status-waiting">WAITING</span>, '
            '<span class="status-failed">FAILED</span>, '
            '<span class="status-done">DONE</span></p>',
            unsafe_allow_html=True,
        )

    st.markdown(
        '<p class="section-title">HISTORIC FAILURES (today, grouped by job name)</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p class='help-text'>FAILED/CANCELLED/TIMEOUT/OUT_OF_MEMORY. "
        "Other columns = most recent failure for that name. ReqMem/Timelimit/CPUTime when available.</p>",
        unsafe_allow_html=True,
    )
    dfh = get_sacct(selected_user, "today")
    if dfh.empty:
        st.info("No sacct data (or sacct not available).")
    else:
        df_fail = get_failures_by_name(dfh)
        if df_fail.empty:
            st.success("No failures today.")
        else:
            st.dataframe(df_fail, use_container_width=True, hide_index=True)

# --------------- Page: Job inspector ---------------

elif page == "Job inspector":
    st.title("Job inspector")
    st.markdown(
        "<p class='help-text'>Run <code>scontrol show job &lt;JobID&gt;</code> (read-only). "
        "Enter a job ID or pick one from the queue.</p>",
        unsafe_allow_html=True,
    )
    df = get_squeue(selected_user)
    job_ids = df["JobID"].tolist() if not df.empty else []
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

# ---------------
