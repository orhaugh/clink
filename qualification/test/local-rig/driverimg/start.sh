#!/bin/sh
# Driver entrypoint: take the bind-mounted key into root's own .ssh (a
# bind mount from the mac carries a foreign uid and >600 modes, which
# ssh refuses), then hold the container for docker exec.
set -e
mkdir -p /root/.ssh
if [ -f /keys/id_ed25519 ]; then
    cp /keys/id_ed25519 /root/.ssh/id_ed25519
    chmod 600 /root/.ssh/id_ed25519
fi
exec sleep infinity
