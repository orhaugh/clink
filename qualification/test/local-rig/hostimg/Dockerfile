# A qualification "rig host" for the LOCAL rig: the same Ubuntu 24.04 the
# cloud hosts run, with its OWN dockerd (so per-host container names,
# pauses and kills behave exactly as on a real host) and the sshd the
# campaign drives hosts through. One image serves every role; the
# inventory's role field decides what the campaign runs on it. The
# package list mirrors qualification/infra/provision.sh's cloud-init so
# the campaign's own bootstrap (pip3 --break-system-packages ...) takes
# the identical path it takes on the rig.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        docker.io docker-compose-v2 openssh-server python3-pip \
        iproute2 iptables curl ca-certificates \
        nfs-kernel-server nfs-common \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /root/.ssh /qual /run/sshd \
    && chmod 700 /root/.ssh

COPY authorized_keys /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/authorized_keys \
    && sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' \
        /etc/ssh/sshd_config

COPY start.sh /usr/local/bin/start.sh
RUN chmod +x /usr/local/bin/start.sh
ENTRYPOINT ["/usr/local/bin/start.sh"]
