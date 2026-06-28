# clink build image - Debian 13 (trixie).
# Mirrors /docker/Dockerfile so our CI shape stays
# consistent across the two projects.
FROM debian:trixie-20260202

ARG USERNAME=clink
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

# Install build dependencies via the shared setup script. It sources versions.env and
# calls build-arrow.sh + build-iceberg-cpp.sh (the from-source, version-pinned toolchain
# shared with the host), so copy the whole scripts/ dir, not just the entrypoint.
COPY scripts/ /tmp/clink-scripts/
RUN chmod +x /tmp/clink-scripts/*.sh && /tmp/clink-scripts/setup-build-env.sh Debug && rm -rf /tmp/clink-scripts

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
