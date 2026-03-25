"""
Purpose:
    SWC Slurm Dashboard — Streamlit dashboard for monitoring SLURM jobs.

Execution Flow:
    (Streamlit entrypoint)
      ├── SLURM data (read_slurm_data: parse_squeue → _squeue_from_json,
      │              parse_sacct → _sacct_from_json, list_squeue_users,
      │              scontrol_show_job)
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

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List

import pandas as pd
import streamlit as st

from read_slurm_data import (
    SQUEUE_COLUMNS,
    SlurmCommandError,
    list_squeue_users,
    parse_sacct,
    parse_squeue,
    scontrol_show_job,
)
from shape_slurm_data import (
    _derive_array_or_job_id,
    _parse_maxrss_to_gb,
    derive_history_start_from_squeue,
    summarise_failures_by_name,
    summarise_live_by_name,
)

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
    .section-title { 
        font-weight: 600;
        color: #a855f7;
        margin-top: 2rem; 
        margin-bottom: 0.75rem; 
        padding-top: 1.5rem;
    }
    .help-text { 
        font-size: 0.85rem; 
        color: var(--text-color); 
        opacity: 0.9; 
        margin-bottom: 0.75rem; 
        padding-top: 1.5rem;
    }
    .health-ok { color: #22c55e; }
    .health-warn { color: #f97316; }
    .dashboard-meta { 
        font-size: 0.875rem; 
        color: var(--text-color); 
        opacity: 0.75; 
        margin-top: -0.5rem; 
        margin-bottom: 1.5rem; 
        padding-top: 1.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


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

# Overview tab: live queue, finished jobs, failures
with tab_overview:
    try:
        df = get_squeue(selected_user)
    except SlurmCommandError as e:
        st.error(f"**squeue** failed: {e.message}")
        df = pd.DataFrame(columns=SQUEUE_COLUMNS)
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
        with st.expander("How to read this section", expanded=False):
            st.caption("Guidance for interpreting the table below.")
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

        # Optional detail view: only for array jobs (IDs with >1 queue entry).
        if running_job_ids:
            array_ids = sorted(
                {
                    _derive_array_or_job_id(str(jid))
                    for jid in running_job_ids
                    if str(jid)
                }
            )
            if array_ids:
                # Keep only IDs that correspond to more than one queue entry.
                job_ids_series = df["JobID"].astype(str)
                derived_ids = job_ids_series.map(_derive_array_or_job_id)
                counts_by_array = derived_ids.value_counts()
                array_ids_with_multiple = [
                    aid for aid in array_ids if counts_by_array.get(aid, 0) > 1
                ]

                if array_ids_with_multiple:
                    # Add a left indent so the detail expander reads as secondary.
                    _, detail_col = st.columns([0.06, 0.94])
                    with detail_col:
                        with st.expander("View job details", expanded=False):
                            detail_array_id = st.selectbox(
                                "Array job ID to inspect (details below)",
                                options=array_ids_with_multiple,
                                index=0,
                                key="queued_job_detail_array_id",
                            )
                            mask = derived_ids == detail_array_id
                            df_detail = df[mask].copy()
                            if not df_detail.empty:
                                detail_cols = [
                                    "JobID",
                                    "State",
                                    "Time",
                                    "Reason",
                                    "Dependency",
                                ]
                                detail_cols = [
                                    c for c in detail_cols if c in df_detail.columns
                                ]
                                df_detail_display = df_detail[detail_cols].rename(
                                    columns={
                                        "JobID": "JOB ID",
                                        "State": "STATE",
                                        "Time": "ELAPSED",
                                        "Reason": "NODE / REASON",
                                        "Dependency": "DEPENDENCY",
                                    }
                                )
                                st.dataframe(
                                    df_detail_display,
                                    use_container_width=True,
                                    hide_index=True,
                                )
                                st.caption(
                                    "Rows above show individual queue entries (array "
                                    "elements and their states) for the selected array "
                                    "job ID."
                                )

    history_start, history_since_label = derive_history_start_from_squeue(df)
    try:
        dfh_window = get_sacct(selected_user, history_start)
    except SlurmCommandError as e:
        st.error(f"**sacct** failed: {e.message}")
        dfh_window = pd.DataFrame()

    # ---------------- FINISHED JOBS: related vs other ----------------
    st.markdown(
        f'<p class="section-title">FINISHED JOBS (since: {history_since_label})</p>',
        unsafe_allow_html=True,
    )
    with st.expander("How to read this section", expanded=False):
        st.caption("Guidance for interpreting the table below.")
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
            "- `REQUESTED MEMORY` (ReqMem) is usually reported on the main "
            "job / array element row (for example `2469691_5`), while "
            "`MAX USED MEMORY in GB` comes from the corresponding batch "
            "step (for example `2469691_5.batch`). Use the shared "
            "`ARRAY JOB ID` / prefix of `JOB ID` to match requested and "
            "used memory for a given array element.\n"
            "- To tune memory requests, compare `REQUESTED MEMORY` and "
            "`MAX USED MEMORY in GB` across successful jobs with the same "
            "`JOB NAME` / `ARRAY JOB ID` and choose a value with a bit of "
            "headroom for future runs.\n"
            "- Each table is flat (one row per JobID), so you can sort and "
            "search directly without extra nesting."
        )

    if dfh_window.empty:
        st.info(
            f"No sacct data (or sacct not available) since: {history_since_label}."
        )
    else:
        success_mask = dfh_window["State"].str.contains(
            "COMPLETED", case=False, na=False
        ) & dfh_window["ExitCode"].str.startswith("0:", na=False)
        df_success_all = dfh_window[success_mask].copy()

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

            # Derive max used memory in GiB from MaxRSS for display.
            if "MaxRSS" in df_success_all.columns:
                df_success_all["MaxUsedMemGB"] = df_success_all["MaxRSS"].apply(
                    _parse_maxrss_to_gb
                )
            else:
                df_success_all["MaxUsedMemGB"] = float("nan")

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
                    "ReqMem",
                    "MaxUsedMemGB",
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
                        "ReqMem": "REQUESTED MEMORY",
                        "MaxUsedMemGB": "MAX USED MEMORY in GB",
                    }
                )
                if "MAX USED MEMORY in GB" in detail_display.columns:
                    detail_display["MAX USED MEMORY in GB"] = detail_display[
                        "MAX USED MEMORY in GB"
                    ].round(2)
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
    st.markdown(
        f'<p class="section-title">FAILURES (since: {history_since_label})</p>',
        unsafe_allow_html=True,
    )
    with st.expander("How to read this section", expanded=False):
        st.caption("Guidance for interpreting the table below.")
        st.markdown(
            "- Includes jobs in these states: `FAILED`, `CANCELLED`, "
            "`TIMEOUT`, `OUT_OF_MEMORY`, or any job with a non-zero "
            "`ExitCode` for this user.\n"
            "- The **since** date in the heading is the same as for "
            "`FINISHED JOBS` above.\n"
            "- The **related** table shows failures whose `JobName` "
            "currently has at least one RUNNING job in the queue; the "
            "**other** table shows all remaining failures in this time "
            "window.\n"
            "- Each row is grouped by `JobName` and includes counts and "
            "the most recent failing `JobID` with its exit code and "
            "resource usage."
        )

    if dfh_window.empty:
        st.info(
            f"No sacct data (or sacct not available) since: {history_since_label}."
        )
    else:
        df_fail_all = get_failures_by_name(dfh_window)
        if df_fail_all.empty:
            st.info(f"No failures found since: {history_since_label}.")
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
                preferred_order = [
                    "LastJobID",
                    "JobName",
                    "Count",
                    "LastState",
                    "LastExitCode",
                    "LastElapsed",
                    "LastNode",
                    "ReqMem",
                    "MaxRSS",
                    "Timelimit",
                    "CPUTime",
                    "WorkDir",
                    "Reason",
                ]
                cols = [
                    c for c in preferred_order if c in df_subset.columns
                ] + [
                    c
                    for c in df_subset.columns
                    if c not in preferred_order
                ]
                df_display = df_subset[cols]
                try:
                    def _fail_state_css(val: str) -> str:
                        if not isinstance(val, str):
                            return ""
                        upper = val.upper()
                        if "OUT_OF_MEMORY" in upper or "TIMEOUT" in upper:
                            return "color: #ef4444; font-weight: 500;"
                        if "FAILED" in upper or "CANCELLED" in upper:
                            return "color: #f97316; font-weight: 500;"
                        return ""

                    styled = df_display.style.apply(
                        lambda col: (
                            [_fail_state_css(v) for v in col]
                            if col.name == "LastState"
                            else [""] * len(col)
                        ),
                        axis=0,
                    )
                    st.dataframe(
                        styled,
                        use_container_width=True,
                        hide_index=True,
                    )
                except Exception:
                    st.dataframe(
                        df_display,
                        use_container_width=True,
                        hide_index=True,
                    )

            _render_fail_block(
                "Related to running job names", df_fail_related
            )
            _render_fail_block("Other failures", df_fail_other)

# Job inspector tab
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


# Help tab
HELP_GRAPH_DOT = """
digraph {
  rankdir=TB;
  fontsize=10;

  subgraph cluster_user {
    label="User";
    style=rounded;
    color="#999999";
    node [shape=box, style=filled, fillcolor="#d0e4ff"];
    U[label="sbatch --job-name=my_array\\n--array=0-3\\nmy_array_job.sh"];
  }

  subgraph cluster_slurm {
    label="Slurm accounting";
    style=rounded;
    color="#999999";
    node [shape=box, style=filled, fillcolor="#e3d7ff"];
    P[label="Array JobID 2473824\\n(JobName = my_array)"];
    T0[label="Task 2473824_0"];
    T1[label="Task 2473824_1"];
    T2[label="Task 2473824_2"];
    T3[label="Task 2473824_3"];
    S0B[label="Step 2473824_0.batch", fillcolor="#f5f5f5"];
    S0E[label="Step 2473824_0.extern", fillcolor="#f5f5f5"];
  }

  subgraph cluster_dash {
    label="Dashboard";
    style=rounded;
    color="#999999";
    node [shape=box, style=filled, fillcolor="#ffd8a8"];
    Q[label="QUEUED JOBS\\n(by JobName)"];
    F[label="FINISHED JOBS\\n(one row per JobID)"];
    X[label="FAILURES\\n(grouped by JobName)"];
  }

  U -> P [label="submit"];

  P -> T0 [label="array elements"];
  P -> T1;
  P -> T2;
  P -> T3;

  T0 -> S0B [label="steps"];
  T0 -> S0E;

  T3 -> Q [label="waiting"];

  T1 -> F [label="successful"];
  S0B -> F;
  S0E -> F;

  T2 -> X [label="non-zero exit"];
}
"""


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

        marker = "{{SLURM_JOB_ARRAY_DIAGRAM}}"
        if marker in help_md:
            before, after = help_md.split(marker, 1)
            if before.strip():
                st.markdown(before)
            st.graphviz_chart(HELP_GRAPH_DOT)
            if after.strip():
                st.markdown(after)
        else:
            st.markdown(help_md)
    except OSError:
        st.error(
            "Help content file `SLURM_DASHBOARD_HELP.md` not found or "
            "unreadable. Please check the repository."
        )
