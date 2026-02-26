#!/usr/bin/env bash
# Start the SWC Slurm Dashboard (Streamlit) safely for local or tunnel use.
#
# Usage:
#   ./run_dashboard.sh              # pick first free port in 8501–8510
#   ./run_dashboard.sh 8765        # explicit custom port
#
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

HOSTNAME="$(hostname)"
USER_NAME="${USER}"
LOCAL_PORT="${PORT}"  # default: use same port locally; change if busy on your laptop

echo "============================================================"
echo " SWC Slurm Dashboard is starting"
echo "============================================================"
echo "Host (this node):   ${USER_NAME}@${HOSTNAME}"
echo "Portal port:        ${PORT}"
echo
echo "From your LAPTOP, run this in a NEW terminal:"
echo
echo "  ssh -J ${USER_NAME}@ssh.swc.ucl.ac.uk ${USER_NAME}@${HOSTNAME} -N -L ${LOCAL_PORT}:127.0.0.1:${PORT}"
echo
echo "If ${LOCAL_PORT} is already in use on your laptop, change the first number"
echo "in the -L argument (before the colon) and use that instead below."
echo
echo "Then in your browser, open:"
echo
echo "  http://localhost:${LOCAL_PORT}"
echo "============================================================"
echo
exec streamlit run swc_slurm_dashboard.py \
  --server.port "$PORT" \
  --server.address 0.0.0.0 \
  --browser.gatherUsageStats false
