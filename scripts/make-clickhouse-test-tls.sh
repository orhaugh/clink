#!/usr/bin/env bash
# Generate the TLS material the ClickHouse live tests use into <dir>, which must
# exist: a CA (ca.crt), a server certificate it signs for localhost and 127.0.0.1
# (server.crt, server.key), and a second CA that signs nothing here (wrong-ca.crt).
# docker/integration-services.yml's clickhouse-26-8-tls service mounts <dir> as its
# tls/ directory when CLINK_CLICKHOUSE_TLS_DIR names it.
#
#   scripts/make-clickhouse-test-tls.sh "$(mktemp -d)"
#
# Shared by scripts/clickhouse-live.sh and the wheels workflow, so the two cannot
# drift. The certificates are valid for two days. The key is world-readable, and
# <dir> made searchable by all, because the server runs as its own user inside the
# container; it protects nothing but a test run.
set -euo pipefail

if [ "$#" -ne 1 ] || [ ! -d "$1" ]; then
    echo "usage: make-clickhouse-test-tls.sh <existing directory>" >&2
    exit 2
fi
cd "$1"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=clink live test CA" \
    -keyout ca.key -out ca.crt 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=clink unrelated CA" \
    -keyout wrong-ca.key -out wrong-ca.crt 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
    -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' >server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 \
    -extfile server.ext -out server.crt 2>/dev/null
chmod 0755 .
chmod 0644 ./*.crt server.key
