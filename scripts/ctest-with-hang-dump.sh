#!/usr/bin/env bash
# Run ctest, and record every thread's stack of a test that runs close to its
# timeout, so a hang leaves behind where it stopped and not only what it last
# logged.
#
# A ctest timeout kills the test and keeps its output. For a hang that is the
# wrong half of the evidence: the log says what happened up to the stop, and
# nothing about which thread is blocked, or on what. The restart-drain hang in
# the ThreadSanitizer runs (2026-10-06 and 2026-10-14) is the case in point:
# its log shows the coordinator's watchdog never failing the job at its drain
# deadline, which only a stack can explain.
#
# While ctest runs, this watches the processes it starts (any process whose
# executable lies under the build directory). Once one has run for
# CLINK_HANG_DUMP_AFTER seconds (default 240, a minute inside the sanitizer
# jobs' 300 s timeout), it attaches gdb once and writes the backtrace of every
# thread to <build-dir>/Testing/Temporary/hang-stacks-<pid>.log and to this
# script's output, so the stacks are in the job log and in the uploaded
# artefacts.
#
# Usage:
#   scripts/ctest-with-hang-dump.sh <build-dir> [ctest arguments...]
#
# Exits with ctest's status. Linux only: without /proc or gdb it runs ctest
# alone and says so. Attaching needs ptrace permission over the test process,
# which in a container means root with CAP_SYS_PTRACE (the sanitizer jobs add
# it); a refused attach is reported in the dump file and does not fail the run.
set -uo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <build-dir> [ctest arguments...]" >&2
    exit 2
fi
build_dir="$(cd "$1" && pwd)" || exit 2
shift

after="${CLINK_HANG_DUMP_AFTER:-240}"
poll="${CLINK_HANG_DUMP_POLL:-10}"
out_dir="$build_dir/Testing/Temporary"

dump_stacks() {
    local pid=$1 age=$2 out="$out_dir/hang-stacks-$1.log"
    local cmd
    cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)"
    {
        echo "hang-dump: pid $pid has run ${age}s: $cmd"
        # Bounded: gdb on a large sanitizer binary can take a while to read
        # its symbols, and the dump must never outlive the test it describes.
        timeout 120 gdb -p "$pid" -batch -nx -q \
            -ex 'set pagination off' \
            -ex 'set print thread-events off' \
            -ex 'info threads' \
            -ex 'thread apply all bt' 2>&1
        echo "hang-dump: end of pid $pid"
    } > "$out"
    sed "s/^/[hang-dump $pid] /" "$out"
}

watch_for_hangs() {
    local -A dumped=()
    local pid age exe
    while sleep "$poll"; do
        while read -r pid age; do
            [ -n "${dumped[$pid]:-}" ] && continue
            [ "$age" -ge "$after" ] || continue
            exe="$(readlink "/proc/$pid/exe" 2>/dev/null)" || continue
            case "$exe" in
                "$build_dir"/*) ;;
                *) continue ;;
            esac
            dumped[$pid]=1
            dump_stacks "$pid" "$age"
        done < <(ps -eo pid=,etimes=)
    done
}

watcher=""
if [ -d /proc ] && command -v gdb >/dev/null 2>&1 && command -v ps >/dev/null 2>&1; then
    mkdir -p "$out_dir"
    watch_for_hangs &
    watcher=$!
    echo "ctest-with-hang-dump: dumping the stacks of any test past ${after}s"
else
    echo "ctest-with-hang-dump: no gdb or /proc here; running ctest without hang dumps"
fi

ctest --test-dir "$build_dir" "$@"
rc=$?

if [ -n "$watcher" ]; then
    kill "$watcher" 2>/dev/null
    wait "$watcher" 2>/dev/null
fi
exit "$rc"
