# Incident verification

A worker dies in the middle of a checkpoint, the job recovers on another one,
and the sink ends up holding some rows twice. This example produces that
incident on purpose, gathers its evidence with `clink incident-collect`,
moves the bundle away from everything the job wrote, and checks it with
`clink incident-verify` with the network denied. It then freezes the
restored run's first epoch as a regression test with
`clink incident-regress`. The workflow is described in
[docs/internals/incident-verification.md](../../docs/internals/incident-verification.md).
`run.sh` is also the workflow's gate: `scripts/incident-gate.sh` runs the
bundle and replay test suites, requires every case to have run, and then runs
it.

```bash
CLINK_BIN_DIR=../../build ./run.sh
```

It needs `clink_node` and `clink` built in `CLINK_BIN_DIR` with fault
injection compiled in (a build with tests has it), `python3` and `curl`.
Everything runs on 127.0.0.1: no containers and no other network. `WORK`
names the directory it writes to (default a new temporary one). Its
`results/` keeps every report, the logs and `summary.txt`. The script exits
0 only when every step shows what it should.

## The job

`feed.py` serves 24,000 events over HTTP at 3,000 a second, in the cursor
form the HTTP polling source reads, so a restore asks again from the
checkpointed sequence number and gets the same bytes. Its last event is far
ahead in event time, and the job's filter drops it. Its watermark still
closes every window while the job keeps running. `job.sql.in` filters the
events and sums them per user over one-second tumbling windows into a
file. The job runs on a Coordinator with an HA directory and two Workers,
with the flight recorder on, checkpoints every 500 ms and protocol tracing
on.

`oracle.py` computes the expected windows from the feed's record function
alone. It reads nothing the job wrote.

## What each step shows

1. **The failure, at an asserted point.** worker-1 runs with
   `CLINK_FAULT_INJECT="checkpoint.after_publish=exit:1@10"` and exits right
   after publishing a snapshot. The script checks the disk afterwards: the
   newest completed checkpoint is K, a snapshot of checkpoint K+1 exists, and
   K+1 never completed. The job then recovers on worker-2 from checkpoint K.
2. **What production did.** The sink holds every expected window, nothing it
   should not, and some windows twice. The script asserts the delivery level
   the Coordinator declared at submission
   (`STATE_EXACTLY_ONCE_OUTPUT_AT_LEAST_ONCE`, set by the file sink) is the
   one the bundle recomputed. That level allows repeats and nothing else.
3. **Collection while the job runs.** A Coordinator removes a job's manifest
   when the job ends, so the evidence is gathered while the job still runs.
   A collection that races a completing checkpoint is refused and retried.
   A second, partial bundle leaves the snapshots out.
4. **Portability.** Both bundles move to another directory, and the job's own
   directories move away, before anything is verified.
5. **Offline verification.** `incident-verify --profile=full` runs under
   `sandbox-exec` with the network denied (`unshare -rn` on Linux, where
   permitted). Every check passes:
   - the restore rule, recomputed over the bundle's markers;
   - each participant's snapshot against its sidecar;
   - the traces' structure, and their conformance to the exactly-once
     specification through the TLC validator, when the pinned jars are cached
     (otherwise that check is unavailable and the run exits 3);
   - the window operator's every captured epoch, replayed twice from its own
     starting state, compared with what production handed the next operator,
     and compared with the oracle.

   The epoch the restore discarded is left out of the oracle comparison. The
   script prints its emission count beside the number of repeated rows in the
   sink: those repeats are its windows, written by the run the restore
   discarded and written again after it.
6. **A forbidden trace.** The same bundle, with the Coordinator's redeploy
   rewritten to restore from the checkpoint that never completed, fails
   `protocol.conformance`, and the run exits 1.
7. **A run the specification does not model.** The same bundle with a rescale
   traced is outside what the specification models. The validator skips the
   run, so `protocol.conformance` is unsupported rather than passed, and the
   run exits 3.
8. **A regression test.** The restored run's first epoch becomes the
   regression bundle `clink replay --emit-test` writes. `clink replay
   --run-test` passes on it, then fails once its golden output is doctored,
   naming the first divergence.
9. **A partial and a failed result.** The bundle without snapshots exits 3,
   with `state.decode` and the replays unavailable. A copy with one byte of a
   capture flipped exits 1 on `bundle.artifacts`, and nothing behind it runs.
10. **Cost.** `measure.py` runs the same query over 200,000 events from a file,
    unpaced, with the flight recorder off and on.

## What passing does not show

The bundle's digests show it is as collected, not who collected it. The
replays show this build reproduces what the captured operators did, and the
oracle comparison shows the windows are right. None of it reads the file the
sink wrote: the script checks that itself, in step 2, because offline
verification reads no destination.
