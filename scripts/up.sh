#!/usr/bin/env bash
# Bring the whole stack up and wait until the API answers.
set -euo pipefail

echo "==> starting stack"
docker compose up -d --build

echo "==> waiting for api to become ready"
for attempt in $(seq 1 30); do
    if curl -fsS http://localhost:8080/ready >/dev/null 2>&1; then
        echo "==> stack is ready after ${attempt}s"
        docker compose ps
        echo
        echo "  API        http://localhost:8080/health"
        echo "  RabbitMQ   http://localhost:15672  (pipeline / pipeline)"
        echo "  Prometheus http://localhost:9090"
        echo "  Grafana    http://localhost:3000  (admin / admin)"
        exit 0
    fi
    sleep 1
done

echo "!! did not become ready in 30s" >&2
docker compose logs --tail=50
exit 1
