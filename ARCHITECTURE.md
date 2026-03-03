## Architecture

This project is deliberately small: one Streamlit app that shells out to Slurm,
split into three Python modules plus a helper script to start it safely on a
login node.

### High-level picture

- **Process**: one Streamlit process running `swc_slurm_dashboard.py` on an HPC login
  node (e.g. `hpc-gw2`).
- **Data source**: read-only Slurm CLI commands (`squeue`, `sacct`,
  `scontrol show job`).
- **Python modules**:
  - `read_slurm_data.py` – read layer: shell helpers and Slurm parsers that turn
    `squeue` / `sacct` / `scontrol show job` into pandas DataFrames or text.
  - `shape_slurm_data.py` – shape layer: pure helpers that aggregate and reshape
    those DataFrames into the summaries used by the UI.
  - `swc_slurm_dashboard.py` – view layer: Streamlit UI, cached `get_*` wrappers
    over the read/shape layers, sidebar, and tabs.
- **UI**: a single Streamlit app with three main tabs:
  - `Overview` – live queue summary, recent finished jobs, and failures.
  - `Job inspector` – detailed view of a single job via `scontrol`.
  - `Help` – documentation on how jobs / arrays / names map onto the dashboard.

Users normally:

1. Start the app on a login node via `run_dashboard.sh` (often in `tmux`).
2. Open an SSH tunnel from their laptop.
3. Visit `http://localhost:<LOCAL_PORT>` in a browser.

```mermaid
flowchart LR
    A[Laptop Browser] -->|SSH tunnel| B[Streamlit UI: swc_slurm_dashboard.py]

    B --> C[read_slurm_data.py<br/>parse_squeue / parse_sacct / scontrol_show_job]
    B --> D[shape_slurm_data.py<br/>summarise_live_by_name / summarise_failures_by_name]

    C --> F[squeue]
    C --> G[sacct]
    C --> H[scontrol show job]

    D --> I[Live summary + Jobs by name]
    D --> J[Finished jobs + failures]

    H --> K[Job inspector output]
```

---

## Components

### 1. Startup script: `run_dashboard.sh`

**Responsibility**

- Start the Streamlit app with a sensible port and clear tunnel instructions.

**Key behavior**

- Picks a port:
  - If you pass a port: uses that directly.
  - Else: finds the first free port in `8501–8510` with a tiny Python snippet.
- Prints:
  - Hostname and chosen port.
  - A ready-to-copy SSH command:
    `ssh -J <user>@ssh.swc.ucl.ac.uk <user>@<host> -N -L <LOCAL_PORT>:127.0.0.1:<PORT>`
  - The browser URL to open: `http://localhost:<LOCAL_PORT>`.
- Runs:
  - `streamlit run swc_slurm_dashboard.py --server.port "$PORT" --server.address 0.0.0.0`

This script encodes the “official” way to start the portal and is the only
place you should need to touch for port / tunnelling conventions.

---

### 2. Python modules

#### 2.1 Read layer: `read_slurm_data.py`

**Responsibility**

- Provide a small, typed API for reading Slurm data into pandas DataFrames or
  raw text using the standard CLI tools.

**Key pieces**

- Shell helpers:
  - `sh(cmd)` – thin wrapper around `subprocess.check_output`.
  - `safe_sh(cmd)` – calls `sh` but returns error text instead of raising.
- Column definitions for `squeue` / `sacct`.
- Parsers:
  - `parse_squeue(user)` – returns a fixed-shape live-queue DataFrame.
  - `parse_sacct(user, start)` – returns a fixed-shape history DataFrame.
- Other helpers:
  - `list_squeue_users()` – distinct users in `squeue` plus `$USER`.
  - `scontrol_show_job(job_id)` – validation + raw `scontrol show job` output.

Only read‑only, fixed-format Slurm commands ever reach `safe_sh`; no free‑form
user shell is executed.

#### 2.2 Shape layer: `shape_slurm_data.py`

**Responsibility**

- Take the raw DataFrames from `read_slurm_data.py` and turn them into the
  higher-level summaries the UI needs.

**Key pieces**

- `summarise_live_by_name(df)` – live queue grouped by job name with per-name
  counts, status summary, elapsed time, and a representative sample JobID.
- `summarise_failures_by_name(dfh)` – failures grouped by JobName with a count
  and “last failure” details (JobID, State, ExitCode, Elapsed, Node, MaxRSS,
  and optional fields such as ReqMem / Timelimit / WorkDir).
- `derive_history_start_from_squeue(df)` – approximates a sensible `--starttime`
  for `sacct` based on the oldest running job’s elapsed time, or start of today
  if nothing is running.
- `_parse_maxrss_to_gb(value)` – converts Slurm MaxRSS strings into GiB.
- `_derive_array_or_job_id(job_id)` – maps `12345_3` → `12345` to group array
  elements.

All of these helpers are pure transformations (no IO).

#### 2.3 View layer: `swc_slurm_dashboard.py`

`swc_slurm_dashboard.py` is the Streamlit entrypoint and is structured into
clear sections (in order):

1. **Styles and page config**
2. **Cached wrappers** over the read/shape layers
3. **Refresh timer helper**
4. **Sidebar (user + refresh)**
5. **Tabs** (`Overview`, `Job inspector`, `Help`)

##### 2.3.1 Styles & layout

- `st.set_page_config(...)`:
  - Title: `SWC Slurm Dashboard`.
  - Layout: wide; expanded sidebar.
- CSS injected via `st.markdown(..., unsafe_allow_html=True)`:
  - Section title color and typography.
  - Status colors:
    - RUNNING, WAITING, FAILED, DONE (match the legend).
  - Health banner:
    - OK (green), ATTENTION NEEDED (orange).
  - Subtle input focus ring (neutral, not “error red”).

This gives a consistent dark theme without depending on external CSS files.

##### 2.3.2 Cached wrappers

To avoid hammering the scheduler and to give Streamlit stable entrypoints, the
view layer wraps the read/shape functions in `@st.cache_data(ttl=...)`
decorated helpers:

- `get_squeue_users()` – uses `list_squeue_users()`.
- `get_squeue(user)` – uses `parse_squeue(user)`.
- `get_sacct(user, start)` – uses `parse_sacct(user, start)`.
- `get_live_by_name(df)` – uses `summarise_live_by_name(df)`.
- `get_failures_by_name(dfh)` – uses `summarise_failures_by_name(dfh)`.
- `get_scontrol_job(job_id)` – uses `scontrol_show_job(job_id)`.

The **Refresh now** button in the sidebar does a full refresh by calling
`st.cache_data.clear()` before rerunning, so you always get fresh data when
you ask for it.

---

### 3. Sidebar

The sidebar manages:

1. **User selection**
   - `selected_user` from a `selectbox` backed by `get_squeue_users()`.
2. **Refresh control**
   - `last_manual_refresh_ts` stored in `st.session_state`.
   - A small `render_refresh_age(...)` helper shows
     `Elapsed since refresh: HH:MM:SS` in the sidebar.
   - **Refresh now** button:
     - Clears caches via `st.cache_data.clear()`.
     - Updates `last_manual_refresh_ts`.
     - Calls `st.rerun()`.

This keeps the user context and manual refresh behavior in a single, predictable place.

---

### 4. `Overview` tab

Rendered inside the `Overview` tab.

**Header + meta**

- Title: `SWC Slurm Dashboard`.
- Meta line: `User: <user> · Last updated: <UTC timestamp>`.

**Live summary**

- `df = get_squeue(selected_user)`.
- If empty:
  - Metrics = 0, info message “No jobs in queue.”
- Else:
  - Metrics:
    - TOTAL jobs
    - RUNNING jobs
    - WAITING jobs
    - DEP problems (DependencyNeverSatisfied count)
  - Health banner:
    - OK (green) if `dep_bad == 0`.
    - ATTENTION NEEDED (orange) otherwise.

**Jobs by name**

When queue isn’t empty:

1. Section title: `QUEUED JOBS (by name)`.
2. `How to read this` expander:
   - Explains:
     - Grouping by job name.
     - Meaning of each column.
     - Importance of `BLOCKED (dependency never satisfied)`.
     - Status color legend (RUNNING, WAITING, FAILED, DONE).
3. Table:
   - Data: `df_by_name = get_live_by_name(df)` → `df_display`.
   - Rendered with `st.dataframe(...)` and a style function that colors the
     **STATUS (summary)** column to match the legend.

**Finished jobs**

- Section title: `FINISHED JOBS (since: <date>)`.
- `How to read this` expander:
  - Explains:
    - The **since** date is the start of the history window, derived from the
      live queue:
      - It starts roughly when your longest-running current task started
        (based on the elapsed time reported by `squeue`), or
      - From the beginning of today (UTC) if nothing is running.
    - Only successful tasks are included (state contains `COMPLETED` and
      `ExitCode` starts with `0:`).
    - The table is split into:
      - **Related to running jobs** (jobs whose array job ID matches an array
        that currently has at least one RUNNING job).
      - **Other finished jobs** (all other successful tasks in the window).
    - Each row is one `JobID` from Slurm (which, for job arrays, may be a
      specific job array element such as `12345_0`), with its array-or-job
      identifier, name, state, exit code, elapsed time, and node list.
- Data flow:
  - A start time and label are derived from `squeue` via
    `derive_history_start_from_squeue(df)`.
  - `dfh_window = get_sacct(selected_user, start_time)`.
  - A filtered subset of successful tasks is rendered as two
    `st.dataframe(...)` tables (related vs other).

**Failures**

- Section title: `FAILURES (since: <date>)`.
- `How to read this` expander:
  - Explains:
    - The same history window as **Finished jobs** is used.
    - Included rows:
      - States matching `FAILED`, `CANCELLED`, `TIMEOUT`, `OUT_OF_MEMORY`, or
      - Any row with a non-zero `ExitCode`.
    - The table is split into:
      - **Related to running job names**.
      - **Other failures**.
    - Each row is grouped by `JobName` and includes:
      - `Count`, last failing `JobID` (for arrays this is a specific job array
        element such as `12345_0`), state, exit code, elapsed time, node,
        `MaxRSS`, and optional resource columns (e.g. `ReqMem`, `Timelimit`,
        `CPUTime`, `WorkDir`) when present.
- Data flow:
  - `df_fail_all = get_failures_by_name(dfh_window)`.
  - Two `st.dataframe(...)` tables are rendered (related vs other).

---

### 5. `Job inspector` tab

Rendered inside the `Job inspector` tab.

**Purpose**

- Let the user run `scontrol show job <JobID>` via a simple form, and see raw
  Slurm output for that job.

**UI**

- Help text explaining what the tool does and how to use it.
- Two columns:
  - Left:
    - Free text input: `Job ID` (e.g. 12345 or 12345_3).
  - Right:
    - Dropdown: `Or pick from your queue` using live `get_squeue(...)`.
- Resolution:
  - Chooses the picked ID if present, otherwise the typed ID.
  - Validates ID via `get_scontrol_job(job_id)`.

**Output**

- If a valid job ID is provided:
  - `st.code(..., language="text")` showing raw `scontrol` output.
- Otherwise:
  - Info message asking for a job ID.

The Job inspector is intentionally thin; it delegates all Slurm semantics to
`scontrol`.

---

### 6. `Help` tab

Rendered inside the `Help` tab.

- Displays the contents of `SLURM_DASHBOARD_HELP.md` using `st.markdown`.
- Explains how SLURM jobs / arrays / job names map onto:
  - **SUMMARY**
  - **QUEUED JOBS**
  - **FINISHED JOBS**
  - **FAILURES**

---

## Security / deployment assumptions

- Intended deployment is a trusted HPC environment:
  - App runs on a login node.
  - Users access it via SSH tunnelling from their own machines.
- The portal is read-only by design:
  - It calls `squeue`, `sacct`, and `scontrol show job`.
  - It never submits, cancels, or modifies jobs.
- The app is not designed as a public internet service:
  - Keep access scoped to your cluster/network policies.
  - Prefer SSH forwarding over exposing Streamlit directly.

## Known limitations

- Depends on local Slurm CLI tooling and permissions:
  - If `squeue`, `sacct`, or `scontrol` are unavailable/misconfigured,
    sections may show empty/error outputs.
- Data freshness is cache-based:
  - `@st.cache_data` TTLs reduce scheduler load but can delay updates.
  - **Refresh now** clears cached data and reruns immediately.
- Parsing depends on Slurm output behavior:
  - JSON is preferred when available; legacy fallback parsers are best-effort.
- `Refresh now` clears Streamlit data caches for this app session:
  - This is intentional for manual "fetch latest now" behavior.

---

## Extending the app

Some natural extension points:

- **New summary tables**:
  - For example, grouping by **user**, **partition**, or **node**:
    - Mirror `summarise_live_by_name` with a different `groupby`.
- **Additional history views**:
  - Configurable `start` for `get_sacct` (e.g. “last 7 days”).
- **Job detail panels**:
  - When clicking a row in **QUEUED JOBS (by name)**, pre-fill the Job inspector with its
    `SAMPLE JOB ID`.

The current structure (parsers → cached wrappers → summarizers → pages) is
meant to keep these additions straightforward.