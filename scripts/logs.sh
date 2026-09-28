#!/usr/bin/env bash
# Tail logs from one service, or all of them.
set -euo pipefail

service="${1:-}"
if [[ -z "$service" ]]; then
    docker compose logs -f --tail=50
else
    docker compose logs -f --tail=100 "$service"
fi
