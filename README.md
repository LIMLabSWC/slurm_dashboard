## Viktor's Slurm Portal

Streamlit dashboard for monitoring SLURM jobs: live queue, historic failures, job inspector (`scontrol show job`), and **sbatch/salloc command builders** (copy‑paste only; the app never submits or cancels jobs).

## Run on the cluster

```bash
./run_dashboard.sh          # port 8501, bound to 0.0.0.0
./run_dashboard.sh 8765     # custom port
```

Or directly:

```bash
streamlit run slurm_portal.py --server.port 8501 --server.address 0.0.0.0
```

## SSH tunneling (laptop / phone)

The portal runs on a cluster node, and you reach it in your browser via an SSH tunnel.

- **1. Run the portal on the cluster**
  - On the node where you want the app to run (login node or compute node), start:
    ```bash
    ./run_dashboard.sh
    ```
    or submit it as a job:
    ```bash
    sbatch dashboard.sbatch
    ```

- **2. Single‑hop tunnel (laptop → node running the portal)**
  - If you can SSH directly to the node that runs the portal (e.g. a login node or a specific compute node):
    ```bash
    ssh -L 8501:localhost:8501 <user>@<node>
    ```
  - Then open `http://localhost:8501` in your laptop browser.

- **3. Via login node + compute node**
  - If the portal runs on a compute node that you only reach via a login node, first start the portal on that compute node (e.g. via `sbatch dashboard.sbatch`).
  - After the job starts, note the node name (e.g. from the job output) and tunnel from your laptop via the login node:
    ```bash
    ssh -L 8501:<compute-node>:8501 <user>@<login-node>
    ```
  - Then open `http://localhost:8501` in your browser.

- **4. Via jump host (example: SWC ssh.swc.ucl.ac.uk → sgw2)**
  - If you need a jump host, you can combine `-J` and `-L`, for example:
    ```bash
    ssh -J <user>@ssh.swc.ucl.ac.uk \
        -L 18501:127.0.0.1:8501 \
        <user>@sgw2 \
        -N
    ```
  - Then open `http://localhost:18501` in your laptop browser.

- **5. Phone**
  - Use the same tunnel ideas from a phone SSH app (with local port forwarding), then open the corresponding `http://localhost:<port>` in the phone browser.

## Run as a Slurm job

Submit a small job that runs the dashboard (so you don’t keep an interactive session open):

```bash
sbatch dashboard.sbatch
```

The job output will print the node and port; tunnel to that node/port using one of the SSH recipes above.

## Safety

The portal only runs **read‑only** Slurm commands: `squeue`, `sacct`, `scontrol show job`. It **never** runs `sbatch`, `salloc`, `scancel`, or any other command that would submit or modify jobs.
