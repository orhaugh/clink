# The campaign DRIVER for the local rig. On the cloud rig the driver is
# the operator's machine; on macOS the container bridge is unreachable
# from the host, so the driver joins the rig's network as a container.
# Based on the runtime image because the campaign's SUBMIT_BIN
# (clink_submit_sql) must be a linux binary here, and the image already
# carries it plus python3 and curl; ssh/scp is all it lacks.
ARG RUNTIME_IMAGE=clink-runtime:local-faultinj
FROM ${RUNTIME_IMAGE}

RUN apt-get update && apt-get install -y --no-install-recommends \
        openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY start.sh /usr/local/bin/driver-start.sh
RUN chmod +x /usr/local/bin/driver-start.sh
ENTRYPOINT ["/usr/local/bin/driver-start.sh"]
