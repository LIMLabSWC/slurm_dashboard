## SWC Slurm Dashboard – How jobs are organised


### 1. SLURM basics: jobs, arrays, job names

- A normal job submitted with:

  ```bash
  sbatch my_job.sh
  ```

  gets a single **JobID**, e.g. `2473820`, and a **job name**:

  - By default the job name is the **script filename** (`my_job.sh`).
  - You can override it with `--job-name`, for example:

    ```bash
    sbatch --job-name=my_job_name my_job.sh
    ```

- An **array job** like:

  ```bash
  sbatch --job-name=my_array --array=0-3 my_array_job.sh
  ```

  creates:

  - One array JobID (parent), e.g. `2473824`, with `JobName = my_array`.
  - One **task** per index: `2473824_0`, `2473824_1`, `2473824_2`, `2473824_3` (all share the same `JobName`).
  - SLURM may also log helper **steps** such as `2473824_0.batch` and `2473824_0.extern`.

So a single submission can produce many JobIDs in history: all of the array elements share the same array JobID (e.g. 2473824) but their recorded JobName values can differ (e.g. my_array, my_array.batch, my_array.extern), so they are tied together primarily by the array JobID, not always by an identical job name.

#### 1.1 How `JOB NAME` in the UI is chosen

- In the **QUEUED JOBS** table:
  - The `JOB NAME` column comes from the `Name` field in `squeue` (which is based on the job name / script name as described above).
- In the **FINISHED JOBS** and **FAILURES** sections:
  - The `JOB NAME` column comes from `JobName` in `sacct`, which is the job name
    recorded by accounting (usually matching the submission’s job name, but it
    can include step variants such as `.batch` / `.extern`).

All grouping “by name” in the dashboard is based on this **job name** label, not the script filename directly, even though the default job name is often the script filename.

#### 1.2 Relationship diagram (text view)

```text
[You submit]
  sbatch --job-name=my_array --array=0-3 my_array_job.sh

[Slurm objects]
  Array JobID (parent)
    2473824
      ├─ Task (array element) 2473824_0
      │    ├─ Step 2473824_0.batch
      │    └─ Step 2473824_0.extern
      ├─ Task (array element) 2473824_1
      │    ├─ Step 2473824_1.batch
      │    └─ Step 2473824_1.extern
      ├─ Task (array element) 2473824_2
      └─ Task (array element) 2473824_3
           ├─ Step 2473824_3.batch
           └─ Step 2473824_3.extern

[How the dashboard sees this]

  • QUEUED JOBS (by name, from squeue)
      - Groups live entries by JobName, e.g. "my_array".
      - While any 2473824_* task is PENDING/RUNNING, "my_array" appears here.

  • FINISHED JOBS (since: …, from sacct)
      - One row per JobID that completed successfully:
          2473824_0, 2473824_0.batch, 2473824_0.extern, …
      - Each row shows its JobID, JobName, state, exit code, elapsed, nodes.

  • FAILURES (since: …, from sacct)
      - Groups non‑zero‑exit rows by JobName.
      - 2473824_1 (EXITCODE != 0) contributes to the "my_array" failure summary.
```


### 2. Data sources used by the dashboard

- **`squeue`** – live view of what is still in the queue (PENDING / RUNNING / etc.).
- **`sacct`** – history of jobs that SLURM accounting has recorded as finished or failed.
- **`scontrol show job`** – detailed dump for one specific JobID (used on the Job inspector page).

If a job never appears in `sacct` (because of cluster settings or retention), the dashboard cannot show it under **FINISHED JOBS** or **FAILURES** once it leaves the queue.


### 3. Dashboard sections

#### 3.1 SUMMARY

- Based on `squeue`.
- Shows counts right now:
  - TOTAL jobs
  - RUNNING jobs
  - WAITING jobs
  - DEP problems (blocked by `DependencyNeverSatisfied`).

#### 3.2 QUEUED JOBS (by name)

- Based on `squeue`.
- Groups by **job name** and shows counts of RUN / WAIT / TOTAL in the **current queue snapshot**.
- As soon as a job leaves the queue (finishes or fails) it disappears from this table; long-term success/failure is tracked via the **FINISHED JOBS** and **FAILURES** sections (from `sacct`).

#### 3.3 FINISHED JOBS (since: …)

- Based on `sacct` within a time window:
  - Starts roughly when your longest-running current job started (derived
    from the elapsed time in `squeue`), or from the beginning of today (UTC) if
    nothing is running, subject to whatever accounting history your cluster
    retains.
- Shows one row per JobID where:
  - `State` contains `COMPLETED`, and
  - `ExitCode` starts with `0:`.
- Split into:
  - **Related to running jobs** – finished jobs whose array JobID matches an
    array that currently has at least one RUNNING job.
  - **Other finished jobs** – all other successful jobs in the window.

#### 3.4 FAILURES (since: …)

- Also based on `sacct` in the same time window.
- Includes rows where:
  - `State` matches `FAILED`, `CANCELLED`, `TIMEOUT`, `OUT_OF_MEMORY`, or
  - `ExitCode` is non-zero.
- Split into:
  - **Related to running job names** – failures for names that still have something RUNNING.
  - **Other failures** – everything else.

### 4. Tiny example

Array script:

```bash
#!/bin/bash
#SBATCH --job-name=instant_test
#SBATCH --array=0-1

if [ "$SLURM_ARRAY_TASK_ID" -eq 0 ]; then
  exit 0   # success
else
  exit 1   # failure
fi
```

Submitting this once may produce history rows like:

- `2473824_0` with `COMPLETED 0:0` → appears in **FINISHED JOBS**.
- `2473824_1` with `FAILED 1:1` → appears in **FAILURES**.
- Additional helper rows `2473824_0.batch`, `2473824_0.extern`, etc., depending on cluster settings.

While the array is still in the queue, `instant_test` also appears in **QUEUED JOBS**. After it leaves the queue it is only visible via FINISHED / FAILURES (if `sacct` logs it).

