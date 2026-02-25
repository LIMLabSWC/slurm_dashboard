#!/usr/bin/env bash
# Start the SWC Slurm Portal (Streamlit) safely for local or tunnel use.
#
# Usage:
#   ./run_dashboard.sh              # pick first free port in 8501–8510
#   ./run_dashboard.sh 8765        # explicit custom port
#
# For SSH tunnel from your laptop:
#   ssh -L 8501:localhost:8501 user@login-node
#   then on login node: ./run_dashboard.sh
#   then open http://localhost:8501 in your browser.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -n "${1:-}" ]]; then
  PORT="$1"
else
  # Try to find the first free port in 8501–8510.
  PORT="$(
    python - <<'PY'
import socket

for port in range(8501, 8511):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", port))
        except OSError:
            continue
        print(port)
        break
else:
    raise SystemExit("No free port found in 8501-8510")
PY
  )"
fi

echo "Starting SWC Slurm Portal on port ${PORT}"
exec streamlit run slurm_portal.py \
  --server.port "$PORT" \
  --server.address 0.0.0.0 \
  --browser.gatherUsageStats false
