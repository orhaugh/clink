#!/bin/sh
# Rig-host entrypoint: bring up this host's OWN dockerd, then hold the
# container on sshd. The campaign reaches this host over ssh exactly as
# it reaches a cloud host.
set -e
dockerd > /var/log/dockerd.log 2>&1 &
i=0
until docker info > /dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -gt 120 ] && { echo "dockerd never came up"; cat /var/log/dockerd.log; exit 1; }
    sleep 1
done
exec /usr/sbin/sshd -D -e
