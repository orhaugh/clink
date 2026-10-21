#!/usr/bin/env bash
# Generate the TLS material the Neo4j live suite uses into <dir>, which must
# exist: a CA (ca.crt), a server certificate it signs for the DNS name localhost
# and for no address (public.crt, private.key, the names Neo4j's SSL policy
# reads), and a second CA that signs nothing here (wrong-ca.crt). The
# certificate deliberately names no IP address, so a client connecting to
# 127.0.0.1 meets a certificate for another host, which the hostname check must
# refuse. docker/integration-services.yml's neo4j-tls service mounts <dir> when
# CLINK_NEO4J_TLS_DIR names it.
#
#   scripts/make-neo4j-test-tls.sh "$(mktemp -d)"
#
# The certificates are valid for two days. The key is world-readable and <dir>
# searchable by all, because the server runs as its own user inside the
# container; it protects nothing but a test run.
set -euo pipefail

if [ "$#" -ne 1 ] || [ ! -d "$1" ]; then
    echo "usage: make-neo4j-test-tls.sh <existing directory>" >&2
    exit 2
fi
cd "$1"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=clink neo4j test CA" \
    -keyout ca.key -out ca.crt 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=clink unrelated CA" \
    -keyout wrong-ca.key -out wrong-ca.crt 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
    -keyout private.key -out server.csr 2>/dev/null
printf 'subjectAltName=DNS:localhost\n' >server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 \
    -extfile server.ext -out public.crt 2>/dev/null
chmod 0755 .
chmod 0644 ./*.crt private.key
