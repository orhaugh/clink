# Network isolation for offline verification, proven rather than assumed.
# Sourced by examples/incident-verification/run.sh and scripts/incident-gate.sh;
# tested by scripts/tests/test_network_isolation.py.
#
#   isolation_establish      choose the mechanism and prove it blocks the network
#   offline <command...>     run a command under the proven mechanism
#   isolation_qualifies      true only when every offline command ran isolated
#
# The mechanism is sandbox-exec (a deny-network profile) on macOS and
# `unshare -rn` (a new user and network namespace, loopback down) on Linux.
# isolation_establish proves it before anything relies on it: a local control
# endpoint must answer without isolation, or the probe proves nothing and the
# result is "inconclusive", and must not answer under it, or the result is
# "not-enforced". Afterwards ISOLATION_STATE is one of:
#
#   enforced       the mechanism ran and blocked the control endpoint
#   unavailable    no mechanism can run here (or CLINK_ISOLATION=none)
#   not-enforced   the mechanism ran, and the endpoint still answered under it
#   inconclusive   the control endpoint did not answer even without isolation
#
# and ISOLATION_DETAIL says why. offline() refuses (exit 2) unless the state is
# enforced. ALLOW_UNISOLATED=1 is a development mode: offline() then runs the
# command without isolation, records that it did, and isolation_qualifies is
# false for the rest of the run, so a run that used it is never a qualification.
#
# For the tests, which must take each path on any platform:
#   CLINK_ISOLATION=sandbox-exec|unshare|none|command   the mechanism to use
#   CLINK_ISOLATION_COMMAND="<wrapper>"                 the wrapper for "command"
#   CLINK_ISOLATION_OS=Darwin|Linux|<other>             the platform to choose for
#   CLINK_ISOLATION_PROBE_URL=<url>                     a control endpoint to use
#                                                       instead of starting one
# scripts/incident-gate.sh refuses every one of them, and ALLOW_UNISOLATED.

ISOLATION_STATE=""
ISOLATION_DETAIL=""
ISOLATION_MECHANISM=""
ISOLATION_WRAPPER=()
ISOLATION_USED_UNISOLATED=0

# The mechanism for this platform, or the one CLINK_ISOLATION names.
isolation_mechanism() {
    if [ -n "${CLINK_ISOLATION:-}" ]; then
        echo "${CLINK_ISOLATION}"
        return
    fi
    case "${CLINK_ISOLATION_OS:-$(uname -s)}" in
        Darwin) echo sandbox-exec ;;
        Linux) echo unshare ;;
        *) echo none ;;
    esac
}

# Sets ISOLATION_WRAPPER for a mechanism; fails when it cannot run here.
isolation_wrapper() {
    case "$1" in
        sandbox-exec)
            command -v sandbox-exec >/dev/null 2>&1 || return 1
            sandbox-exec -p '(version 1)(allow default)(deny network*)' true 2>/dev/null || return 1
            ISOLATION_WRAPPER=(sandbox-exec -p '(version 1)(allow default)(deny network*)')
            ;;
        unshare)
            command -v unshare >/dev/null 2>&1 || return 1
            unshare -rn true 2>/dev/null || return 1
            ISOLATION_WRAPPER=(unshare -rn)
            ;;
        command)
            [ -n "${CLINK_ISOLATION_COMMAND:-}" ] || return 1
            # Deliberate word-splitting: the wrapper is a flat argument string.
            # shellcheck disable=SC2206
            ISOLATION_WRAPPER=(${CLINK_ISOLATION_COMMAND})
            ;;
        *)
            return 1
            ;;
    esac
}

# Whether <url> answers: exit 0 when an HTTP request to it succeeds.
isolation_probe() {
    python3 -I -c 'import sys, urllib.request
urllib.request.urlopen(sys.argv[1], timeout=3).read()' "$1" >/dev/null 2>&1
}

isolation_establish() {
    ISOLATION_MECHANISM="$(isolation_mechanism)"
    ISOLATION_WRAPPER=()
    if ! isolation_wrapper "${ISOLATION_MECHANISM}"; then
        ISOLATION_STATE=unavailable
        ISOLATION_DETAIL="no network isolation can run here (${ISOLATION_MECHANISM})"
        return 1
    fi
    # A wrapper that runs nothing would block the probe too: it must run a
    # command before its blocking one means anything.
    if [ "$("${ISOLATION_WRAPPER[@]}" python3 -I -c 'print("ran")' 2>/dev/null)" != ran ]; then
        ISOLATION_STATE=unavailable
        ISOLATION_DETAIL="${ISOLATION_MECHANISM} does not run a command here, so it isolates nothing"
        return 1
    fi
    local url="${CLINK_ISOLATION_PROBE_URL:-}" server="" dir=""
    if [ -z "${url}" ]; then
        # A control endpoint of our own on loopback, on a port the system picks.
        dir="$(mktemp -d "${TMPDIR:-/tmp}/clink-isolation.XXXXXX")"
        python3 -I -c 'import http.server, sys
class Answer(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"answered")
    def log_message(self, *args):
        pass
server = http.server.HTTPServer(("127.0.0.1", 0), Answer)
open(sys.argv[1], "w").write(str(server.server_address[1]))
server.serve_forever()' "${dir}/port" >/dev/null 2>&1 &
        server=$!
        local i
        for i in $(seq 1 100); do
            [ -s "${dir}/port" ] && break
            sleep 0.05
        done
        url="http://127.0.0.1:$(cat "${dir}/port" 2>/dev/null)/"
    fi
    if ! isolation_probe "${url}"; then
        ISOLATION_STATE=inconclusive
        ISOLATION_DETAIL="the control endpoint ${url} did not answer even without isolation, so a probe under ${ISOLATION_MECHANISM} would prove nothing"
    elif "${ISOLATION_WRAPPER[@]}" python3 -I -c 'import sys, urllib.request
urllib.request.urlopen(sys.argv[1], timeout=3).read()' "${url}" >/dev/null 2>&1; then
        ISOLATION_STATE=not-enforced
        ISOLATION_DETAIL="${ISOLATION_MECHANISM} ran, and the control endpoint ${url} still answered under it"
    else
        ISOLATION_STATE=enforced
        ISOLATION_DETAIL="${ISOLATION_MECHANISM} blocked a local control endpoint that answered without it"
    fi
    if [ -n "${server}" ]; then
        kill "${server}" 2>/dev/null || true
        wait "${server}" 2>/dev/null || true
        rm -rf "${dir:?}"
    fi
    [ "${ISOLATION_STATE}" = enforced ]
}

offline() {
    if [ "${ISOLATION_STATE}" = enforced ]; then
        "${ISOLATION_WRAPPER[@]}" "$@"
        return
    fi
    if [ "${ALLOW_UNISOLATED:-0}" = 1 ]; then
        ISOLATION_USED_UNISOLATED=1
        echo "offline: running WITHOUT network isolation (ALLOW_UNISOLATED=1): ${ISOLATION_DETAIL:-isolation was not established}" >&2
        "$@"
        return
    fi
    echo "offline: refusing to verify without network isolation: ${ISOLATION_STATE:-not established}: ${ISOLATION_DETAIL:-isolation_establish was not run}" >&2
    return 2
}

isolation_qualifies() {
    [ "${ISOLATION_STATE}" = enforced ] && [ "${ISOLATION_USED_UNISOLATED}" = 0 ]
}
