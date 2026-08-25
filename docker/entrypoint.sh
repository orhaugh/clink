#!/bin/sh
# Runtime image entrypoint.
#
# The image has two audiences and they want different first commands.
#
# A CLUSTER passes flags: every compose file and every rig starts its
# container with `--role=coordinator`, `--role=worker` and friends. That has
# to keep working exactly as it did when the entrypoint was clink_node
# itself, so anything starting with a dash - or nothing at all - goes
# straight to the daemon.
#
# SOMEONE TRYING CLINK passes a subcommand: `docker run <image> run
# /opt/clink/examples/sql/hello.sql`. Without this dispatch that reads as
# clink_node's argv and dies on an unknown flag, and the alternative is
# making every newcomer discover `--entrypoint clink` before they can run
# anything. A first command that needs a flag to explain itself is a first
# command most people do not run.
#
# `clink` as a leading word is accepted and dropped, so the form people
# naturally type - `docker run <image> clink run hello.sql` - works too.
set -e

case "${1-}" in
    ''|-*)
        exec clink_node "$@"
        ;;
esac

if [ "$1" = "clink" ]; then
    shift
fi

# A one-off query should print its RESULTS, not the engine's startup
# commentary: someone running the example for the first time is reading the
# rows, and half a screen of INFO above them is noise they have no way to
# evaluate yet. The daemon path above is untouched - an operator wants those
# lines. An explicit CLINK_LOG_LEVEL always wins.
: "${CLINK_LOG_LEVEL:=warn}"
export CLINK_LOG_LEVEL

exec clink "$@"
