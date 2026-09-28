#!/usr/bin/env bash
# Stop the stack. Pass --volumes to also delete all data.
set -euo pipefail

if [[ "${1:-}" == "--volumes" ]]; then
    echo "==> stopping stack and DELETING all data"
    docker compose down -v
else
    echo "==> stopping stack (data preserved; use --volumes to wipe)"
    docker compose down
fi
