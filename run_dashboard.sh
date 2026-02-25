#!/usr/bin/env bash
# Start Viktor's Slurm Portal (Streamlit) safely for local or tunnel use.
#
# Usage:
#   ./run_dashboard.sh              # default: port 8501, bind to 0.0.0.0 for tunneling
#   ./run_dashboard.sh 8765        # custom port
#
# For SSH tunnel from your laptop:
#   ssh -L 8501:localhost:8501 user@login-node
#   then on login node: ./run_dashboard.sh
#   then open http://localhost:8501 in your browser.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
PORT="${1:-8501}"
exec streamlit run slurm_portal.py \
  --server.port "$PORT" \
  --server.address 0.0.0.0 \
  --browser.gatherUsageStats false
