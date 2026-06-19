# clink build image - Debian 13 (trixie).
# Mirrors /docker/Dockerfile so our CI shape stays
# consistent across the two projects.
FROM debian:trixie-20260202

ARG USERNAME=clink
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

# Install build dependencies via the shared setup script.
COPY scripts/setup-build-env.sh /tmp/setup-build-env.sh
RUN chmod +x /tmp/setup-build-env.sh && /tmp/setup-build-env.sh Debug && rm /tmp/setup-build-env.sh

# Additional debugging tools, useful when shelling into the image to
# diagnose CI failures.
RUN apt-get update && apt-get -y install --no-install-recommends \
    curl \
    wget \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Non-root user matching the host's typical UID so bind-mounted
# files don't end up root-owned on macOS Docker.
RUN groupadd --gid ${USER_GID} ${USERNAME} \
    && useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME}

USER ${USERNAME}
WORKDIR /home/${USERNAME}/clink
