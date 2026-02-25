## SWC Slurm Portal

Streamlit dashboard for monitoring SLURM jobs: live queue, historic failures, job inspector (`scontrol show job`), and **sbatch/salloc command builders** (copy‑paste only; the app never submits or cancels jobs).

## Quick start (SWC)

- **1. On the SWC login node: start the portal (tmux optional but recommended)**
  ```bash
  # optionally use tmux so it survives disconnects:
  # tmux new -s slurm_portal
  ./run_dashboard.sh          # auto-picks a free port in 8501–8510
  ```
- **2. From your laptop: open a tunnel to the SWC login node**
  ```bash
  # -N = no remote command (just keep the tunnel open)
  ssh -N -L 8501:localhost:8501 <user>@ssh.swc.ucl.ac.uk
  ```
- **3. On your laptop: view in the browser**
  - Open `http://localhost:8501` (or the port printed by `run_dashboard.sh`).

That’s all most users need.

## Details

- **Where the app runs**
  - At SWC, the portal typically runs **on the login node inside a tmux session**, so it survives SSH disconnects.
  - You can also run it on a compute node or as a Slurm job if you prefer.
- **Ports**
  ```bash
  # Let the script choose the first free port in 8501–8510:
  ./run_dashboard.sh

  # Or specify a port explicitly:
  ./run_dashboard.sh 8765
  # then tunnel with: ssh -L 8765:localhost:8765 <user>@ssh.swc.ucl.ac.uk
  # and open http://localhost:8765
  ```
- **Direct Streamlit invocation**
  ```bash
  streamlit run slurm_portal.py --server.port 8501 --server.address 0.0.0.0
  ```
- **Other SSH setups**
  - **Login node → compute node**: if the app runs on a compute node only reachable via a login node, you can:
    ```bash
    ssh -L 8501:<compute-node>:8501 <user>@<login-node>
    ```
    then open `http://localhost:8501` in your browser.
  - **Jump host example (SWC ssh.swc.ucl.ac.uk → sgw2)**:
    ```bash
    ssh -J <user>@ssh.swc.ucl.ac.uk \
        -L 18501:127.0.0.1:8501 \
        <user>@sgw2 \
        -N
    ```
    then open `http://localhost:18501` in your browser.
  - **Phone**: use the same tunnel commands from a phone SSH app (with local port forwarding), then open `http://localhost:<port>` in the phone browser.

## Run as a Slurm job

Submit a small job that runs the dashboard (so you don’t keep an interactive session open):

```bash
sbatch dashboard.sbatch
```

The job output will print the node and port; tunnel to that node/port using one of the SSH recipes above.

## Safety

The portal only runs **read‑only** Slurm commands: `squeue`, `sacct`, `scontrol show job`. It **never** runs `sbatch`, `salloc`, `scancel`, or any other command that would submit or modify jobs.