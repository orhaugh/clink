#!/usr/bin/env bash
# Commit inserts, updates and deletes to PostgreSQL for SECONDS (default 60):
# one transaction after another, every fiftieth a 500-row update.
#
#   ./scripts/workload.sh 60
#
# The workload is the `workload` procedure postgres-init.sql created, run in
# the postgres container, so the host needs no PostgreSQL client.
set -euo pipefail
cd "$(dirname "$0")/.."
seconds="${1:-60}"
docker compose exec -T postgres psql -U postgres -d shop -v ON_ERROR_STOP=1 -qc \
    "CALL workload('${seconds} seconds')"
docker compose exec -T postgres psql -U postgres -d shop -tAc \
    "SELECT 'orders now: ' || count(*) || ', highest rev: ' || max(rev) FROM orders"
