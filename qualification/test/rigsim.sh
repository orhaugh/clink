#!/usr/bin/env bash
# The simulated rig the campaign self-test runs against.
#
# Interprets the handful of remote commands campaign.sh actually issues,
# against per-host directories under $FAKERIG. State that matters to the
# campaign - whether the generator is "running", what the verifier's verdict
# says, how many faults the chaos log holds - lives in files there, so a
# scenario can be set up by writing those files and the driver exercised
# end to end with no cloud and no cluster.
#
# Scenario knobs, read from $FAKERIG/scenario:
#   job_status=RUNNING|FAILED|GONE   what the coordinator reports
#   workers_lost=N                   the metric the fault gate reads
#   metrics_unreachable=1            curl to /metrics fails
#   chaos_dies_after=N               chaos.py disappears after N polls
#   job_dies_after=N                 the job stops being RUNNING after N polls
#   no_faults=1                      the chaos log never grows
rig_scenario() {
    local key="$1" default="${2:-}"
    local f="$FAKERIG/scenario"
    [ -f "$f" ] || { echo "$default"; return; }
    local v; v=$(grep -E "^${key}=" "$f" 2>/dev/null | tail -1 | cut -d= -f2-)
    echo "${v:-$default}"
}

rig_bump() {  # counter name -> prints the new value
    local f="$FAKERIG/counter.$1"
    local n=$(( $(cat "$f" 2>/dev/null || echo 0) + 1 ))
    echo "$n" > "$f"; echo "$n"
}

rig_exec() {
    local host="$1" cmd="$2"
    local H="$FAKERIG/$host"
    mkdir -p "$H/qual"

    case "$cmd" in
        *"docker info"*) return 0;;

        *"mountpoint -q"*|*"mount -t nfs"*|*"mkdir -p"*)
            mkdir -p "$H/qual/state" "$H/qual/ha" 2>/dev/null; return 0;;

        *"> /qual/state/.probe"*)
            # The shared-state probe: one file, visible from every host.
            local tok; tok=$(echo "$cmd" | grep -oE 'shared-[0-9]+')
            echo "$tok" > "$FAKERIG/shared.probe"; return 0;;
        *"grep -q shared"*)
            local tok; tok=$(echo "$cmd" | grep -oE 'shared-[0-9]+')
            grep -q "$tok" "$FAKERIG/shared.probe" 2>/dev/null; return $?;;

        *"printf 'CLINK_IMAGE"*) return 0;;
        *"docker compose"*) return 0;;

        *"clink --capabilities-json"*)
            echo '{"build":{"git_sha":"deadbeef","git_clean":true,"sql":true,"fault_injection":true}}'
            return 0;;

        *"rpk topic delete"*)
            local t; t=$(echo "$cmd" | grep -oE 'qual01-(in|out)')
            rm -f "$FAKERIG/topic.$t"; return 0;;
        *"rpk topic list"*grep\ -qx*)
            # The campaign's "is this topic really gone yet" poll. The remote
            # command is a pipeline; the fake answers its exit status
            # directly rather than pretending to run awk and grep.
            local t; t=$(echo "$cmd" | grep -oE 'qual01-(in|out)' | tail -1)
            [ -e "$FAKERIG/topic.$t" ] && return 0 || return 1;;
        *"rpk topic list"*)
            echo "NAME PARTITIONS REPLICAS"
            for f in "$FAKERIG"/topic.*; do
                [ -e "$f" ] || continue; echo "$(basename "$f" | sed 's/^topic\.//') 4 3"
            done; return 0;;
        *"rpk topic create"*)
            local t; t=$(echo "$cmd" | grep -oE 'qual01-(in|out)')
            if [ -e "$FAKERIG/topic.$t" ]; then
                echo "TOPIC_ALREADY_EXISTS"; return 1
            fi
            touch "$FAKERIG/topic.$t"; echo "$t OK"; return 0;;

        *"pip3 install"*|*"chmod 600"*) return 0;;

        *"touch /qual/progress.json.stop"*)
            # The generator's stop file: the real generator polls for it and
            # exits promptly (the file is the delivery; see below for why the
            # signal is not).
            touch "$FAKERIG/progress.json.stop"
            rm -f "$FAKERIG/proc.generator"
            return 0;;
        *"touch /qual/verdict.json.stop"*)
            # The verifier's stop file: finalisation becomes possible only
            # once this exists (modelled in the verdict-poll case below).
            touch "$FAKERIG/verdict.json.stop"
            return 0;;
        *"touch /qual/chaos.jsonl.stop"*)
            # The chaos controller's stop file: it exits cleanly between
            # faults (clearing anything it applied), promptly in the sim.
            touch "$FAKERIG/chaos.jsonl.stop"
            rm -f "$FAKERIG/proc.chaos"
            return 0;;
        *"rm -f /qual/progress.json.stop"*)
            rm -f "$FAKERIG/progress.json.stop" "$FAKERIG/verdict.json.stop" \
                  "$FAKERIG/chaos.jsonl.stop"
            return 0;;

        *"pkill -INT"*)
            # Deliberately INERT for the generator and verifier: the spawn
            # discipline (`&` under a non-interactive shell) starts them with
            # SIGINT ignored, and Python keeps an inherited SIG_IGN, so on
            # the real rig a polite INT never reaches either loop. The sim
            # modelling INT as effective is exactly how qual01-20260818e's
            # INCONCLUSIVE got past the self-test: the stop FILES above are
            # the delivery the campaign must rely on.
            return 0;;
        *"pkill"*)
            # Non-INT pkill (the kill sweep). Match the BRACKET-SAFE
            # substring. The campaign writes its patterns as '[g]enerator.py'
            # precisely so they cannot match the shell carrying them, which
            # also means they do not contain the string "generator" - a fake
            # that greps for that sees nothing.
            echo "$cmd" | grep -q "enerator.py" && rm -f "$FAKERIG/proc.generator"
            echo "$cmd" | grep -q "erifier.py"  && rm -f "$FAKERIG/proc.verifier"
            echo "$cmd" | grep -q "haos.py"     && rm -f "$FAKERIG/proc.chaos"
            return 0;;

        *"pgrep -f '[g]enerator.py|[v]erifier.py'"*)
            local n=0
            [ -e "$FAKERIG/proc.generator" ] && n=$((n+1))
            [ -e "$FAKERIG/proc.verifier" ] && n=$((n+1))
            echo "$n"; return 0;;
        *"pgrep -f '[c]haos.py'"*)
            local dies; dies=$(rig_scenario chaos_dies_after 0)
            if [ "$dies" != "0" ]; then
                local n; n=$(rig_bump chaospoll)
                [ "$n" -gt "$dies" ] && return 1
            fi
            [ -e "$FAKERIG/proc.chaos" ] && return 0 || return 1;;
        *"pgrep -f"*)
            echo "$cmd" | grep -q "enerator.py" && { [ -e "$FAKERIG/proc.generator" ] && return 0 || return 1; }
            echo "$cmd" | grep -q "erifier.py"  && { [ -e "$FAKERIG/proc.verifier" ] && return 0 || return 1; }
            return 1;;

        *"rm -f /qual/progress.json"*)
            rm -f "$FAKERIG/progress.json" "$FAKERIG/verdict.json" "$FAKERIG/chaos.jsonl"; return 0;;

        *"setsid nohup"*)
            # Starting a long-running process: mark it and seed its output.
            if echo "$cmd" | grep -q "generator.py"; then
                touch "$FAKERIG/proc.generator"
                echo '{"produced_high":{"0":20050,"1":20050,"2":20050,"3":20050}}' \
                    > "$FAKERIG/progress.json"
                echo '{"seed":1,"partitions":4,"keys":50000,"events_per_sec_per_partition":500,
                       "base_ms":1,"max_jitter_ms":1500,"window_ms":10000,"topic":"qual01-in"}' \
                    > "$FAKERIG/progress.json.spec"
            fi
            if echo "$cmd" | grep -q "verifier.py"; then
                touch "$FAKERIG/proc.verifier"
                echo '{"evaluated_windows":4,"correct_windows":4,"output_records":102370,
                       "missing":0,"duplicate":0,"conflicting":0,"incorrect":0,"foreign":0,
                       "final":false,"pending_pairs":0,"defect_sample":[]}' \
                    > "$FAKERIG/verdict.json"
            fi
            if echo "$cmd" | grep -q "chaos.py"; then
                touch "$FAKERIG/proc.chaos"
                [ "$(rig_scenario no_faults 0)" = "1" ] || \
                    echo '{"fault":"worker_sigkill","target":"w1"}' > "$FAKERIG/chaos.jsonl"
            fi
            return 0;;

        *"wc -l < /qual/chaos.jsonl"*)
            wc -l < "$FAKERIG/chaos.jsonl" 2>/dev/null || echo 0; return 0;;

        *"verdict.json"*"final"*)
            # The finish phase polls the verifier's state line
            # ("<final> <pending> <evaluated>") before its kill sweep (the
            # real verifier runs one full evaluation over every pending
            # window first - minutes at multi-hour scale, and sweeping early
            # killed it mid-finalise on qual01-20260818c). The fake verifier
            # finalises ONLY once its stop FILE exists - the campaign's
            # signals are inert (see the pkill -INT case) - and then only
            # after verifier_final_after_polls probes; default 0 = on the
            # first post-stop probe. verifier_never_finalises=1 models a
            # wedged finalisation for the stall path.
            local need cur
            need=$(rig_scenario verifier_final_after_polls 0)
            cur=$(rig_bump finalpoll)
            if [ "$(rig_scenario verifier_never_finalises 0)" = "1" ]; then
                echo "0 235668 4"; return 0
            fi
            if [ -e "$FAKERIG/verdict.json.stop" ] && [ "$cur" -gt "$need" ]; then
                python3 - "$FAKERIG/verdict.json" <<'PY'
import json, sys
p = sys.argv[1]
try:
    v = json.load(open(p))
except Exception:
    v = {}
v["final"] = True
json.dump(v, open(p, "w"))
PY
                echo "1 0 4"; return 0
            fi
            # Not final yet: report a shrinking backlog so the campaign's
            # progress-aware wait keeps waiting rather than reading an
            # in-progress finalisation as a stall.
            echo "0 $(( 100000 - cur * 1000 )) 4"; return 0;;

        *"python3 -c"*)
            # The campaign reads the generator's progress this way. Each read
            # returns a larger number so "input flowing" is satisfied.
            local n; n=$(rig_bump progress)
            echo $(( 80200 + n * 35000 )); return 0;;

        *"tail -"*)
            local f; f=$(echo "$cmd" | grep -oE '/qual/[a-z.]+' | head -1)
            cat "$FAKERIG/$(basename "${f:-chaos.log}")" 2>/dev/null || echo "(no log)"
            return 0;;

        *) return 0;;
    esac
}
