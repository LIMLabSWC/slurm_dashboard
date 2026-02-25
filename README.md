## SWC Slurm Portal

Streamlit dashboard for monitoring SLURM jobs: live queue, historic failures, and a job inspector (`scontrol show job`). The app is **read‑only** and never submits or cancels jobs.

## Quick start (SWC)

### Setup (do once)

- **1. Clone the repo on HPC login node (`hpc-gw2`)**
  ```bash
  git clone git@github.com:LIMLabSWC/slurm_dashboard.git
  cd slurm_dashboard
  ```

- **2. Create a micromamba env and install requirements**
  ```bash
  micromamba create -n swc-slurm-portal python=3.11 pip -y
  micromamba activate swc-slurm-portal
  pip install -r requirements.txt
  ```

### Usage

- **1. On the HPC login node (e.g. `hpc-gw2`): start the portal (tmux optional but recommended)**
  ```bash
  tmux new -s slurm_portal
  cd slurm_dashboard
  micromamba activate swc-slurm-portal
  ./run_dashboard.sh            # script prints the chosen PORT and SSH tunnel command
  ```

- **2. From your laptop: open a tunnel to the HPC login node**
  - Copy the `ssh -N -J ... -L ...` command printed by `run_dashboard.sh` and run it in a terminal on your laptop.

- **3. On your laptop: view in the browser**
  - Open `http://localhost:<LOCAL_PORT>` where `<LOCAL_PORT>` is the first number
    in the `-L <LOCAL_PORT>:127.0.0.1:<PORT>` part of the printed SSH command.

That’s all most users need.

## Details

- **Where the app runs**
  - At SWC, the portal typically runs **on an HPC login node (e.g. `hpc-gw2`) inside a tmux session**, so it survives SSH disconnects.
  - You can also run it on a compute node if you prefer.
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

## Safety

The portal only runs **read‑only** Slurm commands: `squeue`, `sacct`, `scontrol show job`. It **never** runs `sbatch`, `salloc`, `scancel`, or any other command that would submit or modify jobs.