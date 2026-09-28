#!/usr/bin/env bash
# Demo helper: publishes one poison event (retries 3x, then dead-letters) and
# two identical events (dedup demonstration). Requires TEST_MODE=1 on the
# consumer, which docker-compose.yml sets by default.
set -euo pipefail

cd "$(dirname "$0")/.."

PUB='docker compose exec -T rabbitmq rabbitmqadmin -u pipeline -p pipeline publish exchange=events routing_key=event.created'

echo "==> 1. poison event: watch 'docker compose logs -f consumer' for 3 retries then DLQ"
$PUB payload='{"event_id":"aaaaaaaa-0000-0000-0000-000000000001","event_type":"poison","payload":{"device_id":"dev-1"}}'
echo "==> 2. two identical events: second one is caught by redis dedup"
$PUB payload='{"event_id":"bbbbbbbb-0000-0000-0000-000000000001","event_type":"demo","payload":{"device_id":"dev-1"}}'
sleep 2
$PUB payload='{"event_id":"bbbbbbbb-0000-0000-0000-000000000001","event_type":"demo","payload":{"device_id":"dev-1"}}'
echo "==> done. results:"
echo "     docker compose logs consumer | grep -E 'attempt|duplicate|dead-letter'"
echo "     curl http://localhost:8080/dead-letters"
