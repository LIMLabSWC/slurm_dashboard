#!/bin/bash
#SBATCH --job-name=slurm_portal
#SBATCH --partition=normal
#SBATCH --time=08:00:00
#SBATCH --mem=1G
#SBATCH --cpus-per-task=1
#SBATCH --output=slurm_portal_%j.out

# Optional: run Viktor's Slurm Portal as a job so you don't need an interactive session.
# After the job starts, note the node name and tunnel to it, e.g.:
#   ssh -L 8501:<NODE>:8501 <user>@<login-node>
# Then open http://localhost:8501 on your machine.

PORT="${SLURM_PORTAL_PORT:-8501}"
cd "$(dirname "$0")"
echo "Slurm Portal starting on $(hostname):$PORT at $(date)"
echo "Tunnel: ssh -L ${PORT}:$(hostname):${PORT} <user>@<login-node>"
exec streamlit run slurm_portal.py --server.port "$PORT" --server.address 0.0.0.0 --browser.gatherUsageStats false
