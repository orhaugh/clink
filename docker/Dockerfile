# clink build image - Debian 13 (trixie).
# Mirrors /docker/Dockerfile so our CI shape stays
# consistent across the two projects.
FROM debian:trixie-20260202

ARG USERNAME=clink
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

# Build deps install in TWO layers so adding a connector's apt package does not invalidate
# the from-source toolchain (~40-60 min recompile of aws-sdk + Arrow + iceberg-cpp + ...).
# Each layer COPYs only the scripts it needs, so editing the cheap connector script cannot
# bust the expensive layer's cache. CLINK_BUILD_JOBS caps compile parallelism for the heavy
# from-source deps (Arrow's compute kernels + the aws-sdk push past 1 GB resident per TU, so
# full -j can OOM a RAM-limited Docker). Empty default = use all cores; override with --build-arg.
ARG CLINK_BUILD_JOBS=""
ENV CLINK_BUILD_JOBS=${CLINK_BUILD_JOBS}

# Layer 1 (EXPENSIVE, rarely invalidated): base toolchain + from-source toolchain. Sources
# versions.env and calls build-arrow.sh + build-iceberg-cpp.sh, so copy those alongside it.
COPY scripts/versions.env scripts/build-arrow.sh scripts/build-iceberg-cpp.sh scripts/install-system-deps.sh /tmp/clink-sys/
RUN chmod +x /tmp/clink-sys/*.sh && /tmp/clink-sys/install-system-deps.sh Debug && rm -rf /tmp/clink-sys

# Layer 2 (CHEAP, volatile): optional connector client apt packages + the Pulsar .deb.
# Re-runs in ~1-2 min when a connector adds a dep; the layer above stays cached.
COPY scripts/versions.env scripts/install-connector-deps.sh /tmp/clink-conn/
RUN chmod +x /tmp/clink-conn/*.sh && /tmp/clink-conn/install-connector-deps.sh && rm -rf /tmp/clink-conn

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
