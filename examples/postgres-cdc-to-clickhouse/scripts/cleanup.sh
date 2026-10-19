#!/usr/bin/env bash
# Retire the job the way a deployment would, then remove the stack.
#
#   ./scripts/cleanup.sh
#
# A replication slot outlives the job that reads it, and holds WAL for as
# long as it exists. Cancelling the job does not drop it, and must not: a
# restarted job resumes from it. So retiring the job is two steps, cancel it
# and then drop its slot once no session holds it, and this script does both
# before `docker compose down -v` removes everything.
set -euo pipefail
cd "$(dirname "$0")/.."
COORD="http://localhost:${CLINK_HTTP_PORT:-8081}"
psql() { docker compose exec -T postgres psql -U postgres -d shop -tAc "$1"; }

if curl -sf "${COORD}/api/v1/jobs" >/dev/null 2>&1; then
    echo "cancelling the job"
    curl -s -X POST "${COORD}/api/v1/jobs/1/cancel" >/dev/null || true
fi
if docker compose ps --status running postgres 2>/dev/null | grep -q postgres; then
    for _ in $(seq 1 30); do
        active="$(psql "SELECT active FROM pg_replication_slots WHERE slot_name = 'clink_orders'")"
        [ "${active}" = "t" ] || break
        sleep 1
    done
    echo "the slot before it is dropped:"
    psql "SELECT slot_name, active, confirmed_flush_lsn, wal_status FROM pg_replication_slots" || true
    psql "SELECT pg_drop_replication_slot('clink_orders') FROM pg_replication_slots WHERE slot_name = 'clink_orders'" >/dev/null
    echo "replication slot clink_orders dropped"
fi
docker compose down -v --remove-orphans
