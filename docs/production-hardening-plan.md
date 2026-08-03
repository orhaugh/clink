# Production hardening: findings, work items, status

> An audit of clink's failure, recovery and validation behaviour, the work
> done against it, the evidence for each claim, and - stated as plainly as
> the rest - what is still not demonstrated.

**Status date:** 2026-08-03
**Baseline commit:** `d0b8bd0`
**Scope:** runtime, scheduler, channels, checkpoint coordinator, state
backends, SQL binder/planner, connector SPI, coordinator/worker control
plane, CI.

This document is the working record for the hardening effort. It is kept
current as items land; an item is only marked Done when its tests have been
run and the command that ran them is recorded here.

---

## 1. How to read the status column

| Status | Meaning |
|---|---|
| **Done** | Implemented, tested, tests run and passing, evidence recorded below. |
| **Partial** | Implemented and tested, but a named part of the item is not covered. The gap is stated. |
| **Open** | Not implemented. Reason and blocking factor stated. |

Nothing is marked Done on the strength of code existing. Principle 10 of the
brief - do not report an item complete unless the relevant tests have run
successfully - is applied literally.

---

## 2. Findings from the initial assessment

Each finding is a specific behaviour observed in the tree at `d0b8bd0`, with
the source that exhibits it.

### F1. The COMPLETED-N marker was not durable and its failure was ignored

`src/cluster/coordinator.cpp` wrote the global-completion marker with a bare
`std::ofstream`, then broadcast `CommitCheckpoint` to every worker. The
comment beside it asserted "by the time workers commit their pre-staged
transactions, the marker is durable". It was not:

- No `fsync`. The bytes reached the page cache, so an OS or power loss after
  the external commits left no record that the checkpoint had completed.
- No error check. `ENOSPC` or `EACCES` produced no marker at all, and the
  commit broadcast went out regardless. The sinks committed externally, the
  coordinator restarted, found no `COMPLETED-N`, rewound to an older
  checkpoint and re-emitted already-committed output.

This is a silent exactly-once violation on the path the 2PC sinks depend on.

**Risk:** duplicated output after a failure, with no signal that it happened.

### F2. Coordinator metadata writes raced and were not durable

`atomic_write_file` (same file) wrote job manifests and history records to a
**fixed** `"<path>.tmp"` and renamed. Two concurrent writers to one path
interleave into a single temp file and then race the rename, so one can
publish the other's half-written bytes. No `fsync` either. The
`FileBackedStateBackend` had already fixed exactly this bug class for state
snapshots (per-write `part_seq_` temp names); the control plane had not.

**Risk:** corrupt or lost job-recovery metadata after concurrent writes or
power loss.

### F3. The rescale path wrote its stitched snapshot unsafely and lied about it

`src/state/state_backend_factory.cpp` `build_file()` merged parent snapshots
and wrote the result with:

```cpp
std::ofstream out_stream(dst_file, ...);
if (out_stream && !final_bytes.empty()) { out_stream.write(...); }
out.restore_from = Snapshot{...};   // set regardless
```

A failed write was swallowed and `restore_from` was set anyway, so a rescale
that could not write its state came up believing it had restored, holding
nothing. Not durable either.

**Risk:** silent total state loss on rescale.

### F4. Checkpoint payloads had no integrity metadata

`FileBackedStateBackend::restore` read whatever bytes were at
`checkpoint-<id>.snap` and handed them to the Arrow reader. A stream
truncated on a record-batch boundary decodes as a smaller, entirely
plausible state, and the job resumes having silently dropped keys. There was
no checksum, no length record, no format version, and no way to tell
"interrupted" from "damaged". Recovery selected the highest `COMPLETED-N`
with no validation and no fallback: one corrupt newest checkpoint stranded a
job that had a perfectly good older one.

**Risk:** silent partial state restore; unrecoverable job despite a valid
older checkpoint.

### F5. Delivery guarantees were free text

A sink's semantics were `delivery_guarantee='exactly_once'` in a DDL `WITH`
clause, compared against string literals in `src/sql/binder.cpp` (four
sites). Nothing held a machine-readable statement of what a connector can
actually do, so nothing could reject a job asking for a guarantee its
connectors cannot provide. The same phrase was used for two different
properties: clink's internal state being exactly-once, and output reaching
an external system exactly once. Only the first is true for most sinks.

**Risk:** a job that believes it is exactly-once and is not.

### F6. No fault injection inside runtime paths

`include/clink/test/failure_injection.hpp` injects failures at
harness-mediated operator call sites. That cannot reach the paths an
incident travels: between `fsync` and `rename`, between sink prepare and
commit, inside the network frame writer, inside coordinator metadata
persistence. Nor can it reach a child process, which is what a multi-process
crash test needs.

**Risk:** the recovery paths that matter most were exercised only by
whole-process `SIGKILL` at wall-clock moments, which cannot target a
specific point in a protocol.

### F7. Multi-process integration tests were advisory and duplicated

`.github/workflows/ci.yml` ran the whole `integration` label with
`continue-on-error: true`, described as "advisory until the pre-existing
multi-process flakiness is triaged". The flakiness has two identifiable
causes, both visible in the test sources:

- **Bind-close-race ports.** `probe_free_port()` binds, closes, and returns
  the number. Duplicated in **26** files. Under `ctest -j` another test can
  take the port between the close and the child's bind.
- **Sleeps standing in for conditions.** 118 `sleep_for` calls across the
  integration tests, including `std::this_thread::sleep_for(500ms); // both
  workers register`. Correct on an idle laptop, wrong on a loaded runner.

Also duplicated across those files: `spawn_proc` (19), `kill_quietly` (25),
`wait_for_exit` (6), `await_port_open` (4). No test collected child logs on
failure, so a CI failure was un-diagnosable from the transcript.

**Risk:** the fault-tolerance suite could not gate, so a regression in
failover could merge with the pipeline green.

### F8. Sanitizers ran only when someone remembered

`.github/workflows/sanitizers.yml` was `workflow_dispatch` only.

**Risk:** a memory or data-race regression merges and is found later, or not.

### F9. Unbounded state was accepted silently

A windowless `GROUP BY` keeps one accumulator per group forever; `SELECT
DISTINCT` keeps every distinct row ever seen; an unwindowed equi-join keeps
both sides indefinitely. `include/clink/sql/logical_plan.hpp` documents this
("State is unbounded: every record seen on each side is retained", "State is
unbounded for now (no TTL)"), and nothing rejected or warned. The only TTL
that exists is `TtlConfig` on `KeyedState<K,V>` - processing-time, per-slot,
opt-in from C++, with no SQL surface and no coverage of map/list state, the
SQL aggregation operators, joins, dedup or CEP partial matches.

**Risk:** out-of-memory with a delay fuse, in queries that look ordinary.

### F11. A worker lost during initial deploy fails the submission outright

Found by `WorkerDeathBeforeAnyCheckpointStillCompletes`. Killing a worker
while the coordinator is mid-deploy produces
`Coordinator::deploy: send failed for worker-0` and rejects the whole
submission; `--max-restarts-on-worker-loss` does not apply.

On reflection this is **defensible rather than a defect**: the restart
policy governs a worker lost from a *running* job, and a job that never
started has no state and no restart semantics to apply. It is recorded
because it is undocumented, and because the natural reading of
"max restarts on worker loss" does not obviously exclude it. The test now
waits for the job to be running before killing, and the deploy window is
called out as undefined.

**Risk:** an operator expecting the restart policy to cover deployment is
surprised by a hard submission failure. Documentation, not code.

### F12. Overlapping worker losses are not folded into one restart

Found by `TwoConsecutiveWorkerFailuresAreSurvived`. A second worker loss
arriving while the first loss's restart is still draining is not handled:
the submitter fails with `restart drain timed out after 30000ms (survivors
did not drain)` rather than the coordinator noticing that a survivor it is
waiting on has itself gone, and re-planning.

The **sequential** form - the second loss arriving after the first restart
has settled, evidenced by a new completed checkpoint - works, and is gated
as `TwoSeparatedWorkerFailuresAreSurvived`.

**Risk:** a correlated failure (a rack, a node pool, an OOM sweep) that
takes two workers within the drain window stalls the job until the drain
timeout, then fails it, rather than recovering.

**Status:** open. The test asserting correct behaviour is retained as
`DISABLED_TwoConsecutiveWorkerFailuresAreSurvived` rather than deleted or
rewritten to assert the bug, so a fix is a one-line re-enable.

### F13. `CLINK_FAULT_INJECT` was silently inert (found and fixed in this pass)

Found while writing `WorkerKilledAtTheStateRestorePointIsRedeployed`, and
worth recording because the failure mode is the nastiest kind: **a safety
mechanism that reports success while doing nothing.**

`reach()` checks an inline `g_any_armed` atomic and returns immediately when
it is false - that is the point of the fast path. But `g_any_armed` is only
set by `Registry::arm()`, and the environment is only read by the
`Registry` constructor, which `instance()` runs lazily. On a process that
armed nothing programmatically, `instance()` was therefore never called,
the constructor never ran, `CLINK_FAULT_INJECT` was never read, and every
fault point stayed inert - while the operator (or the test) believed a
fault was armed and drew conclusions from a run in which nothing happened.

Fixed by forcing construction in a static initialiser.
`Registry::env_seeding_ran()` exists solely so a test can assert it, and
`EnvironmentSeedingRunsAtStaticInit` does. Verified end to end:

```
CLINK_FAULT_INJECT="checkpoint.before_write=exit:70@1" \
  clink checkpoint-verify --dir <d> --repair
    -> exit 70, and no sidecar written (the fault landed before the write)
```

**Note on an earlier conclusion in this document:** while the bug was live,
`WorkerKilledAtTheStateRestorePointIsRedeployed` failed and was provisionally
attributed to an engine recovery gap. It was not. With the fault firing
correctly, that scenario passes - the job **does** recover from a worker
killed during state restore. Only F12 is a real engine gap.

### F14. Every inline column constraint was silently dropped

`ast::ColumnDef` carried only `name`, `type` and `loc`, and
`translate_column_def` read nothing else. So

```sql
CREATE TABLE t (k BIGINT NOT NULL PRIMARY KEY CHECK (k > 0) DEFAULT 1, ...)
```

parsed, registered and behaved **exactly** like `k BIGINT`. Found by
compiling each construct and observing it accepted, then checking whether
anything downstream consumed it. Nothing did.

The worst instance is PRIMARY KEY, because it has a consequence beyond the
constraint itself. `catalog.hpp` promised:

> primary_key ... Populated from a `PRIMARY KEY (col, ...)` column
> constraint OR from the WITH-option. Both are accepted; the in-column form
> is canonical.

Measured: the in-column form produced `primary_key.size() == 0`; only the
WITH-option worked. A user writing
`CREATE TABLE t (k BIGINT PRIMARY KEY, ...) WITH (mode='upsert')` got an
upsert sink **with no key to upsert on**, so the effectively-once guarantee
that sink's capability record advertises was void - with no diagnostic
anywhere. Another F10: the documentation was the only thing implementing it.

**Risk:** silent loss of the upsert key, and users believing rows are
validated when nothing checks them.

### F15. A misspelt interpreted table option is silent

A table's `WITH` clause carries two kinds of option: ones clink reads and
acts on, and ones it passes through to the connector. An unrecognised key
takes the passthrough path, so `delivery_gurantee='exactly_once'` leaves an
at-least-once sink and says nothing; `primary_keys='id'` leaves an upsert
sink with no key. The same applies to values: `mode='upsrt'` reads as
append, `changelog='yes'` reads as false.

**Risk:** a one-character typo silently changes a job's delivery guarantee
or retention, with the DDL still reading as though it had not.

### F16. The HA fencing epoch was computed and then ignored

`HaCoordinator` bumps a monotonic epoch on every leadership acquisition and
writes it to the leader-endpoint file. Its own header said the field was
"in place for when it does" propagate into the wire protocol. A grep for
every use confirmed nothing else read it: no control frame carried it, no
worker checked it, no metadata record stored it.

A coordinator cannot detect on its own that it has lost leadership. One
partitioned from the coordination store, or paused past its lease, keeps
every worker connection open and its checkpoint timer running. With no
fencing it could deploy a second copy of a running job, cancel a job the
new leader had just started, number checkpoints from a stale counter into
the same directory, redistribute state via `BeginRescale`, and broadcast
`CommitCheckpoint` - publishing 2PC sink transactions the new leader never
agreed to.

**Risk:** duplicated or destroyed work, and externally-visible commits from
a coordinator with no authority to make them. The last of these cannot be
undone.

### F17. The fault framework lost a wake-up to a thread on its way to parking (found and fixed in this pass)

Found on Linux, by `FaultInjectionTest.ResetReleasesAParkedThread` timing
out in a full-suite run. Not a flake, and not a test-timing artefact - a
real lost-wakeup deadlock in `Registry::reach`.

`reach()` takes `mu_`, counts the hit and matches a rule, then RELEASES the
mutex before the `Action::Block` case re-takes it to park. A `reset()` or
`release()` landing in that window bumped the release epoch and notified
with nobody yet waiting. The thread then took the lock, read the
already-bumped epoch as its OWN baseline, evaluated the predicate as false,
and slept forever.

This is the second bug in the same three lines. The first was the mirror
image: `reset()` used to CLEAR the per-point epoch, so a woken thread
re-read a zeroed counter and parked again. That was fixed with a monotonic
global epoch, which is correct as far as it goes and does nothing for a
thread that has not yet parked.

**Risk:** any test or harness that arms `Block` can wedge its whole binary,
and a hung process in CI reads as an infrastructure problem rather than as
the defect it is. The fault framework is what several of the gates in this
document are built on, so a deadlock in it undermines them.

**Fix:** capture both epoch baselines in the SAME critical section that
matches the rule. A wake landing after that point necessarily moves the
epoch above the baseline, so the predicate is already true and the thread
never parks. `ResetReleasesAThreadStillOnItsWayToParking` and
`ReleaseReachesAThreadStillOnItsWayToParking` cover both wake paths - the
per-point epoch has the identical shape and would otherwise have been
fixed by accident rather than on purpose.

The window is inside `reach()` and cannot be opened deterministically from
a test without a hook into the function under test, so both cases hammer it
(500 iterations, resetting the instant the hit lands) rather than claiming
a determinism they do not have. That is stated in the tests themselves.

### F18. The HA epoch restarted at 1 for every new leader, making fencing inert (found and fixed in this pass)

Found by `HaFailoverTest.FailoverAdvancesTheEpoch` the first time it ran:
two `clink_node` coordinators, the leader SIGKILLed, and the successor
announcing `epoch=1` - the same epoch the leader it displaced had been
stamping on every control frame.

Both `HaCoordinator` implementations bumped a **per-process** atomic that
starts at zero. A standby is a separate process, so its first acquisition
always produced 1. Nothing carried the epoch across leaderships. Every
worker-side fencing comparison was therefore `1 >= 1`, and no frame from a
displaced leader would ever have been refused.

This is worth dwelling on: the fencing work of W15 was complete, correct in
isolation, and covered by twelve passing unit tests - and would have
protected nothing in a real deployment. The unit tests could not see it,
because they choose the epochs themselves. Only a test that let the SYSTEM
produce the epochs could, which is the reason the multi-process failover
tests exist rather than being a belt-and-braces extra.

**Risk:** the entire split-brain protection was decorative.

**Fix:** on acquisition, read the epoch the previous leader published
(`active-leader.json` for the file coordinator, the leader key for etcd) and
take `max(that, own) + 1`. Both implementations had the identical defect and
both were fixed; `EachLeadershipTakesAnEpochAboveEveryEarlierOne` pins the
property with distinct coordinator objects, each with its own
zero-initialised counter, which is the condition that hid the bug.

### F19. The cluster wire protocol carried no version at all

`RegisterMsg` declared nothing, `HelloClientMsg` was literally an empty
struct, and no code path compared versions anywhere. The protocol's only
compatibility mechanism was the additive-tail idiom, which is good at what
it does and covers exactly one kind of change: a message gaining a field.

Everything else is invisible on the wire. A field changing meaning or
width, a `MessageKind` value repurposed, a semantic contract changed under
an unchanged encoding - a mixed-version cluster would run into any of those
with no handshake failure and no diagnostic, and the symptom would surface
later as a job that will not deploy or a checkpoint that never commits.

**Risk:** a rolling upgrade across an incompatible boundary corrupts or
stalls running jobs instead of refusing to start.

### F20. The snapshot format version was written and never read

`docs/internals/state-snapshot-format.md` states the rule as a
requirement: "readers MUST reject a version above the highest they know
rather than guess." The writer stamps `clink.format_version` on every
stream. Nothing read it - a grep for the key found the writer, and tests
asserting the writer wrote it.

The schema check present at each read site is not a substitute. A format
version is bumped precisely for a change the previous reader MISREADS, and
such a change need not alter the column shape: a key-layout change would
not. A future version-2 stream would have passed the schema check and been
restored as version 1.

**Risk:** silently wrong state after a restore, which is the failure mode
with the longest gap between cause and symptom. This is the same class as
F10 - a rule enforced by documentation.

### F21. A four-byte header could ask for a four-gigabyte allocation

`read_frame` trusted the length prefix: `std::vector<std::byte> body(len)`
allocated and zeroed up to 4 GB before a single byte of body arrived.
Anything that could open a TCP connection to the control port could send
`FF FF FF FF`, repeatedly, before authenticating.

Three copies of that function existed - `coordinator.cpp`, `worker.cpp`,
`job_submitter.cpp` - and all three had it, which is the usual argument for
having one.

**Risk:** trivial remote memory exhaustion of the coordinator or a worker.

### F22. Eleven decoders reserved on a peer-supplied element count

Every container decoder read a `u32` and handed it straight to `reserve()`.
`decode_deploy` with a task count of `0xFFFFFFFF` asks for roughly 400 GB
for a `vector<DeploymentTask>`; `decode_plugin_binary` asks for 4 GB of
bytes. No bounds check preceded any of them.

**Risk:** as F21, and reachable through any message carrying a list.

### F23. A malformed frame terminated the process

The most serious of the three, and the one that makes the other two acute.

`MessageReader` throws on a truncated or malformed payload. That is
deliberate - `MessageReader.ThrowsOnTruncatedBody` has always asserted it -
and it implies somebody catches. Nobody did. The throw propagated out of
the coordinator's accept thread, its per-client thread, its per-worker
reader thread, and the worker's own reader thread. Leaving a thread
function by exception is `std::terminate`.

So: one malformed frame, from anything that could reach the control port,
killed the coordinator process and every job it was managing. No
authentication was required, because the decode happens before any.

Demonstrated rather than reasoned about. With the boundary removed, the
test that sends a truncated `Register` does not fail - it prints
`libc++abi: terminating due to uncaught exception of type std::runtime_error:
MessageReader: truncated string` and takes the whole test binary with it.

**Risk:** remote unauthenticated denial of service against the control
plane.

### F24. SIGTERM worked on an idle worker and hung on a busy one

`tests/integration/test_sigterm_shutdown.cpp` covers SIGTERM and passes.
It signals a worker that registered and has been idle ever since. No test
signalled a worker with a job running on it, and that is the case that
fails.

`Worker::stop()` set `stop_`, woke the pending-task waiters, and then
joined every task thread. But a RUNNING subtask's `LocalExecutor` watches
`JobConfig::external_cancel_token` and nothing else - it has no view of the
Worker. `stop()` never touched those tokens; only the `CancelJob` handler
did. So `stop()` set a flag no runner was looking at, then blocked in the
join for any job whose source had not already finished. An unbounded source
never does.

This is the wrong way round in the way that costs most. A container runtime
sends SIGTERM, waits out a grace period, and SIGKILLs. So the node exited
promptly when there was nothing to lose and had to be killed when there
was - and the existing test reported green throughout.

Measured, not inferred: before the fix, `AWorkerRunningAJobExitsOnSigterm`
timed out at 15 s; after, the worker exits in 2.1 s.

**Risk:** every rolling restart of a busy cluster ends in SIGKILL. Sinks
lose the chance to abort pending transactions, so recovery has to resolve
them instead.

### F25. Any unknown scalar function failed at runtime, per record

`RANDOM()` was the case that led here - `NOW()` is refused on determinism
grounds and `RANDOM()`, just as nondeterministic and just as damaging to
the replay guarantee, was not. But the probe showed the problem is not
`RANDOM()`. It is that NO function name was checked at all.

Any name - a typo, a PostgreSQL builtin clink does not implement, anything
- parsed, bound, planned and deployed, then threw
`json_value_expr: unknown op 'x'` out of the projection operator when the
first record arrived. Embedded, that is a job failure where a compile error
belonged. On a cluster it is a job that deploys, starts, and dies on its
first record, reporting an internal diagnostic, with a restart loop if
restarts are configured.

**Risk:** a typo reaches production as a crash-looping job rather than a
rejected submission.

**A correction to the earlier record.** This item was previously written up
as "`NOW()` and `RANDOM()` are accepted while `CURRENT_TIMESTAMP` is
refused". That was wrong in both directions: `NOW()` was already refused,
and the real scope was every unknown name rather than two of them. The
probe that established it is in section 5.

The same probe corrected a second entry: "unknown or absent connector is
accepted at DDL time and only fails at plan time" understated the
behaviour. Both are refused at plan time with actionable messages
(`format='json' source requires connector='file', 'kafka', ...` and
`table t missing required property: connector`). No change was needed and
none was made.

### F26. Per-sink exactly-once was reported as if it composed

The guarantee analyser reasons about connectors one at a time and takes
the weakest, which is right for every question it was built to answer and
wrong for one it was not asked: two transactional sinks produce a strong
answer, and per-sink exactly-once does not compose into job-level
atomicity.

A job with two 2PC sinks and no shared `commit_group` commits them
independently. A failure between the two commits publishes one and not the
other, leaving the outputs disagreeing - while each sink is still,
correctly, exactly-once on its own. The analyser reported
`END_TO_END_EXACTLY_ONCE` and said nothing about it.

The mechanism to avoid this already existed: sinks sharing a
`commit_group` commit as a unit, gated on the group's collective ack. It
is opt-in, the default is independent commit, and nothing told anyone.

**Risk:** an operator reads "end-to-end exactly-once" and reasonably
concludes the outputs cannot disagree. They can.

### F27. The coordinator leaked a thread per client connection

Every `HelloClient` spawned a reader thread and pushed it, with its
socket, into two parallel vectors. Nothing drained them except `stop()`.

So the cost was per client EVER SEEN, not per client currently connected.
A client that connected, did its work and went away left a joinable
`std::thread` and a `shared_ptr` behind for the coordinator's whole
lifetime. A monitoring script running `clink list` once a second adds
86,400 thread handles a day, until thread creation begins to fail.

Nothing crashes while this is true, and no existing test noticed, because
every test creates a handful of clients and then stops the coordinator -
which is exactly when the vectors are finally drained.

There was also no limit on concurrent clients: the thread pool was
whatever arrived.

**Risk:** unbounded resource growth from ordinary polling, ending in a
coordinator that can no longer accept connections. The exhaustion throw
lands in the accept loop, which W14's exception boundary now catches - so
the failure mode is a control plane that stops accepting rather than one
that dies, which is quieter and no better.

### F28. A checkpoint a subtask FAILED to take was recorded as complete

The `COMPLETED-<id>` marker is the definition of a checkpoint having
reached global completion, and it is what recovery restores from. It was
written whenever every subtask had ANSWERED, not whenever every subtask
had SUCCEEDED.

Those differ, and the difference is reachable in normal operation. A
subtask whose snapshot throws catches the exception, acks `ok=false`, and
carries on running - nothing fails the job. The coordinator erased its key
from the pending set exactly as if it had succeeded, the set emptied, the
marker was written, `latest_completed_checkpoint_id` advanced, and
`CommitCheckpoint` went out. For a checkpoint in which one operator's
state was never written at all.

`msg.ok` was consulted in precisely two places in the entire ack handler:
aborting a `commit_group`, and incrementing a metric. Neither is on the
completion path. So for the default case - no commit groups - a failed
snapshot and a successful one were indistinguishable.

**Risk:** the job's recovery point advances onto a checkpoint that does
not exist in full. A later restart restores that operator's state from a
checkpoint it never wrote, and the records the failed snapshot should have
covered are neither committed nor replayed.

**Fix:** failed acks are tracked per checkpoint. When every subtask has
answered and any answer was a failure, the checkpoint is failed rather
than completed: no marker, the recovery point stays where it was, and
`AbortCheckpoint` goes out so staged sink transactions are rolled back
instead of left waiting for a commit that will never come.

Measured either side. With the guard disabled the tests report "checkpoint
1 became the job's recovery point even though a subtask reported it could
not snapshot" and "a failed checkpoint moved the recovery point off the
last good one (now 2, was 1)".

### F29. HA recovery could not see any completed checkpoint

Found while writing F28's test, by looking at the filesystem rather than
the comments - which disagree with each other.

The marker was WRITTEN to `<checkpoint_dir>/COMPLETED-<id>`.
`latest_completed_id_on_disk`, which HA recovery uses to decide what to
restore from, READS `<checkpoint_dir>/<job_id>/COMPLETED-<id>`. The header
comment describes the job-scoped path; the code wrote the flat one.

**Established by running it, not by reading it.** This was first recorded
here as a discrepancy of unknown severity, because concluding "recovery is
broken" from two paths that disagree is exactly the kind of claim this
document should not contain unverified. `RecoveryRestoresFromTheLast
CompletedCheckpoint` completes a checkpoint under one coordinator, recovers
the job in a second, and reads the restore point off the Deploy frame the
new coordinator actually sends. It came back **0** with `COMPLETED-1` on
disk.

So: every completed checkpoint was invisible to HA recovery, and a
recovered job silently restarted from scratch, discarding all its state.
Nothing failed and nothing was logged; the job simply came back empty.

The flat layout had a second consequence: two jobs sharing a checkpoint
directory both wrote `COMPLETED-5` to the same file, so one job's progress
was read as the other's.

**Risk:** total state loss on coordinator failover - the one event HA
exists to survive.

**Fix:** the write is now job-scoped, matching the read, the documented
layout, and the harness. Markers written by an older build stay at the
flat path and are not migrated; nothing ever read them, so nothing is
lost.

Why the WRITE side rather than the read: the job-scoped path is what the
header, the recovery lookup and `cluster_harness.hpp` all already assume,
and it is the one that does not collide across jobs.

**Why no test caught this.** `tests/integration/cluster_harness.hpp`'s
`await_checkpoint_completed` looks under the job-scoped path, so it could
only ever have timed out - and the tests calling it pass because the
assertions around it do not depend on it finding anything. A helper whose
failure is invisible is worse than no helper.

### F30. Processing-time TTL tests assert order by betting on the scheduler

Found by `KeyedStateGetAsync.RefreshOnReadAdvancesExpiry` failing on Linux
after passing on the host repeatedly - the same shape as F17, and the
second time this round that a Linux run caught what macOS did not.

The test is structurally flaky rather than unlucky. TTL is 100 ms; it
sleeps 70 ms, reads (expecting a refresh), sleeps 70 ms again, and expects
the entry alive. The margin is 30 ms on either side, and a loaded box or a
container loses it: if the first sleep overshoots 100 ms the entry is
already gone and the second read fails.

Nothing about the property under test is a timing contract. "A read
refreshes the expiry" is a statement about ORDER. Expressing order by
sleeping asserts the order AND bets on the scheduler, and only one of those
is wanted.

It is a family, not one test: 18 `sleep_for` calls across 8 TTL test files.

**Risk:** intermittent CI failures in the area the brief singles out, which
train people to re-run rather than to read. Under sanitizers, where
everything is several times slower, the margins are worse.

**Fix:** `TtlConfig::clock_ms`, a nullable function pointer for the
processing-time clock. Null means the wall clock, so nothing changes for
production. A raw pointer rather than a `std::function` because it is
consulted on every TTL decision - a null check and at most an indirect
call, no allocation, no type erasure - and per-slot rather than a global
hook so one test cannot perturb another. The event-time domain needed no
equivalent: its clock is already the watermark the caller advances.

The three sleeps in `test_async_state_get.cpp` are now clock advances. The
test runs in microseconds instead of 290 ms of sleeping, and passed 10/10
consecutive runs.

**A test that did not test what it claimed.** Mutation-checking this
revealed the first thing I disabled - `refresh_on_read` in the sync `get()`
path - left the test passing. The async path has its own refresh in
`decode_one_`; disabling THAT fails it. Worth recording because the test's
name says `get_async` and the obvious reading of "where does refresh_on_read
live" is the wrong one.

Also added while there: an assertion that a refreshed entry still expires
eventually. Without it the test passes against a TTL that has been
accidentally disabled altogether.

**Now converted:** the remaining sleeps in the TTL suites are gone -
`test_keyed_state_ttl.cpp` (8), `test_keyed_state_ttl_depth.cpp` (2) and
`test_typed_state_ttl.cpp` (1) all drive `clock_ms`. 52 TTL cases pass and
the processing-time ones run in 0 ms rather than roughly a second of
sleeping.

Speed was the least of it. A controlled clock can sit ON a boundary, which a
sleep cannot, so the conversion added the cases that pin behaviour rather
than merely observe it:

- `TtlIsInclusiveAtTheExpiryInstant` - expiry is `now >= expire_at`, so an
  entry written at T with a 100 ms TTL is gone at exactly T+100. Unpinned,
  either comparison passed.
- `AnEntryOneMillisecondShortOfExpiryStillReads` - the other side, which a
  TTL expiring everything one tick early would otherwise satisfy.
- `NoTtlBehavesLikeBeforeAndKeepsValuesIndefinitely` now advances a YEAR.
  Previously it slept 150 ms, so "indefinitely" meant "longer than the test
  was willing to wait".
- The event-time case advances an HOUR of processing time to show the
  watermark is what drives it. It used to sleep 20 ms against a 1 s TTL,
  which no implementation, correct or broken, would have failed.
- Both refresh cases now also assert the entry expires when left alone.
  Without that they pass against a TTL accidentally disabled altogether.

Mutation-checked: flipping expiry from `>=` to `<` at all four comparison
sites fails `TtlIsInclusiveAtTheExpiryInstant` by name, along with five
event-time cases.

**One sleep is left, deliberately**, in
`test_sql_state_ttl_runtime.cpp`. The SQL path builds its `TtlConfig`
internally from table properties, so injecting a clock would mean adding a
test-only hook to the runtime. It is safe in the direction that matters: it
asserts the entry HAS gone, with a 1 ms TTL and a 20 ms sleep, so a loaded
runner only oversleeps and still evicts. The flaky shape is the opposite -
asserting something has NOT yet expired - and the test says so, so the
pattern is not copied into that case.

**A second flake, this one mine.** The same Linux run then failed
`FrameRobustness.ClientConnectionsAreReapedRatherThanAccumulated`, written
one round earlier - holding 6 sessions of 40 rather than the expected 2.

Reaping WAS working; the assertion was wrong. Reaping is driven by
ADMISSION - a finished session is joined and dropped when the next client
arrives - so waiting for the count to fall on its own cannot work: after
the last client is admitted there is nothing left to do the reaping. The
test waited anyway, and passed on macOS only because the reader threads
happened to keep up.

Fixed by making the test DRIVE what it depends on: each poll admits a probe
client, which reaps whatever has finished. What is proven is that reaping
makes progress, not that it happened before a deadline. 10/10 consecutive
runs.

Worth recording as its own mistake rather than folded into F30. A test that
waits for a condition nothing will cause is a specific error, and I made it
while fixing a different flake of the same family.

### F31. Six checkpoint settings were accepted and silently ignored

Found by enumerating what `CheckpointConfig` accepts and comparing it to
what the engine reads. Each of these submits cleanly today and does
nothing:

- `interval_ms` with no `checkpoint_dir` - `checkpoint_trigger_loop_` skips
  any job whose directory is empty, so a job that asked for periodic
  checkpoints takes none at all.
- `max_restarts_on_worker_loss` with no `checkpoint_dir` - the field's own
  comment says "has no effect without checkpoint_dir". The job fails fast
  on the first worker loss, having asked not to.
- `restore_from_checkpoint_id` without `restore_from_dir`, and the reverse -
  the engine restores only when both are set, so half a resume request is a
  silent cold start. That is the worst outcome available to someone
  deliberately resuming.
- `capture_records` with no `capture_dir` - a bound on a capture that is
  not happening.
- A memory `state_backend_uri` alongside a `checkpoint_dir` - the
  durability illusion. `COMPLETED-N` markers get written, so the control
  plane believes checkpoints are completing, while the state they describe
  dies with the process. A restore finds markers and no state.

And two liveness settings that are accepted while guaranteeing the failure
they exist to prevent: a heartbeat interval at or above the heartbeat
timeout declares a HEALTHY worker lost on schedule, and a watchdog interval
longer than the timeout means the configured timeout is not the one in
effect.

**Risk:** an operator sets a flag, reads their own configuration back, and
believes something that is not true. The memory-backend case is the
dangerous one: it manufactures evidence of durability.

### F32. The CLI made the documented recovery default unreachable

Found by the new linter, on its first real use: a `--profile=production`
submission that never mentioned restarts warned about fail-fast restarts.

`CheckpointConfig::max_restarts_on_worker_loss` uses `kRestartAuto` as an
UNSET sentinel, documented to resolve to self-heal
(`kDefaultSelfHealRestarts = 10`) when `checkpoint_dir` is set and
fail-fast otherwise. `clink_submit_job` defaulted its flag to the string
`"0"`, which wrote an EXPLICIT zero into every submission.

So the sentinel was unreachable through the CLI, and every job submitted
with `clink_submit_job` failed fast on the first worker loss - including
jobs configured with checkpointing, whose entire purpose is to survive one.
The self-healing default described in the header applied to nothing that
went through the tool.

**Risk:** a job with checkpointing configured stops on a single worker loss
instead of recovering, and the operator has read documentation saying it
would not.

**Fix:** the flag defaults to `auto`, which maps to the sentinel. This is a
behaviour change for anyone relying on the old default - a CLI-submitted
job with checkpointing will now restart up to 10 times rather than failing
- and it is a change TOWARD the documented behaviour rather than away from
it. `--max-restarts-on-worker-loss=0` still forces fail-fast.

Worth noting how it was found. Not by reading the CLI, and not by a test:
by a linter warning about a combination on a command line that had not
asked for that combination. The warning was correct and the thing it
pointed at was upstream of it.

### F33. Two operational questions had no metric, and restarts had none at all

The metrics surface turned out to be in better shape than the item assumed.
Two audits over all 84 declared metric constants found nothing wrong: none
is dead (every one is referenced beyond its declaration), and none is
pre-registered-but-never-updated - so there are no panels reading zero
forever. That is worth recording as a negative result, because it is where
the item's effort was expected to go.

The gaps are in what is NOT declared:

**No way to alert on checkpoint staleness.** `clink_ckpt_completed_total` is
a counter, and the canonical streaming alert is "no checkpoint completed in
N minutes" - a stalled checkpoint means the recovery window is growing
without bound. A counter cannot express it: one that has stopped moving is
indistinguishable from an idle job over any short window, and `rate()` over
a window long enough to tell them apart smears the signal.
`clink_ckpt_last_completed_unix_seconds` is a gauge carrying the completion
TIMESTAMP, so a dashboard computes `time() - metric`. A timestamp rather
than an age because an age must be refreshed on a timer to stay true, and a
gauge that is only correct when something remembers to update it is exactly
how metrics come to read zero forever.

**No restart metric at all - and then, two of them.** A job that keeps
failing and being redeployed is the most informative unhealthy state there
is, and it emitted nothing: `clink_coordinator_jobs_failed_total` moves only
when the restart budget is finally exhausted, so the signal arrived after
the recovery had already been spent.

Instrumenting it revealed a second path. A checkpointed job rolls the WHOLE
job back to its last checkpoint, because a per-subtask redeploy would leave
the other subtasks un-rolled-back and break exactly-once. A job WITHOUT a
checkpoint directory instead retries the failing subtask in place. Counting
only the first left every non-checkpointed job's retries invisible - and
those are the jobs with no recovery at all, so their retries matter more,
not less.

They are separate series rather than one total because they mean different
things: a whole-job restart replays from a checkpoint, a subtask redeploy
does not.

**Risk:** a restart-looping job looks healthy until it gives up, and a job
whose checkpoints have silently stopped cannot be alerted on at all - which
is the state F28 and F29 both produced.

**How the second path was found:** the test asserted the whole-job counter
moved, and it did not, while the job demonstrably restarted. The obvious
reading - "the metric is not wired" - was wrong; the restart had gone
through a mechanism I had not known existed.

### F34. A resumed job overwrote output it had already published

Found by extending the output-equality method to coordinator failover -
the first thing to check what a recovered job actually PRODUCES rather than
that it ran.

Result: `38 committed lines, 38 distinct; 2 MISSING: record-0, record-1`.
Two records that had been committed BEFORE the failover were gone from the
output afterwards. No duplicates, no errors, nothing logged.

The cause is checkpoint-id reuse. A job's committed output file is named
`committed/sub<N>-<ckpt>.dat`, and a recovered job is a fresh `JobState`
whose `next_checkpoint_id` starts at 1. So the recovered job took its own
checkpoint 1 and its sink renamed a new `sub0-1.dat` over the file holding
records 0 and 1 - output that had already been published and that a
downstream consumer may already have read.

Checkpoint ids are not decoration; several things are named by them. The
`COMPLETED-<id>` marker is one, so a resumed job also overwrites the marker
history its own recovery point is read from.

**Not specific to HA.** Any resume does it, including an explicit
`--restore-from-checkpoint-id=N`, which is the documented way to rewind a
job. A failover is simply the case that happens without anyone asking.

**Risk:** silent loss of already-published output on every resume. Worse
than a duplicate, because a duplicate is visible to a consumer that checks
and this is not.

**Fix:** a restore seeds `next_checkpoint_id` to
`restore_from_checkpoint_id + 1`, so numbering continues above the point
being resumed from rather than restarting. One line, and the test that found
it went from a 91-second timeout to passing in 7.7 seconds.

**Why no existing test caught it.** Every failover test asserted the epoch
advanced, the standby took over, and a job ran again. All of those were true
throughout. Only the output disagreed.

### F35. `commit_group` did not do what three layers of the codebase said it did

Found by writing the test W22 recorded as missing - whether a `commit_group`
delivers cross-sink atomicity under failure. It does not, and neither did
the absence of one cause the harm that was claimed for it.

The claim, stated in `Coordinator::CheckpointGroupState`, in
`Sink::set_commit_group`, in the SQL catalog, and acted on by the guarantee
analyser: sinks sharing a group commit as a unit, the coordinator holding
the commit broadcast until every member has acked its pre-commit; sinks
without one "commit INDEPENDENTLY" and can be left disagreeing.

What the code does:

- There is no group-scoped commit broadcast. Commit is one per-checkpoint,
  job-wide broadcast, sent only after every subtask acked ok, and any failed
  ack aborts that checkpoint for every sink.
- `CheckpointGroupState::pending` is assigned and erased from, and never
  tested for emptiness. Nothing gates on it.
- `CheckpointGroupState::committed` was never written or read anywhere.
- The group's only behavioural effect is that a failing ack issues the abort
  immediately rather than after every subtask has answered. That is not
  nothing - no timeout ever abandons a pending checkpoint, so if a peer
  never answers, the checkpoint-level abort never fires and staged sink
  transactions would sit staged - but it is a liveness detail, not
  atomicity.

**Established by running it, not by reading.** The same two-sink job, the
same worker kill, with `commit_group` set and with no group at all: identical
per-checkpoint agreement both ways. If the group were load-bearing the
ungrouped run would have split.

**Risk:** the analyser told operators their outputs could disagree and to fix
it by setting an option that changes nothing. Advice that cannot work is
worse than silence, because setting it reads as a resolved issue. The
opposite error is also present: sinks that DID share a group were told
nothing, while still carrying the residual exposure below.

**Fix:** correct the claims rather than implement the documented mechanism.
Implementing group-scoped commit would add no correctness a single job does
not already have (and `commit_groups` is per-job, so cross-job atomicity is
not representable at all), so it would be complexity for no behavioural
gain. Instead: the dead `committed` field is gone, the four comment sites now
describe what the code does, and the analyser warning fires on sink count
alone - grouping is deliberately not consulted, so setting a group cannot
silence a live limitation.

**The residual exposure, which is real and which grouping does not touch.**
Being told to commit together is not committing atomically: one sink can
finish its commit and another's worker die before finishing its own. That
split is repaired at restart, when a sink resolves staged transactions
against the `COMPLETED-N` marker. A job that never restarts - restart budget
exhausted, or abandoned - keeps it. The warning now says this.

**Tests:** `tests/integration/test_commit_group_atomicity.cpp` (4 cases,
built on `examples/two_sink_commit_group_job.cpp`: one source fanned out to
two 2PC sinks). Per-checkpoint agreement is externally checkable because
committed output is named `committed/sub<N>-<ckpt>.dat`, so the set of
checkpoints each sink published is readable from the filesystem.

Mutation-checked, and the mutation is what makes the cases worth having:
making one sink silently skip its commit fails all three grouped cases with
the split named (`sink A committed checkpoints {1..15}, sink B {}`). A test
that only counted records would have passed that mutation on the clean run,
since the totals can be complete while the two sides disagree.

The ungrouped case is asserted alongside the grouped ones rather than as a
contrast, since it is the same guarantee by the same mechanism. It exists so
that a change making ungrouped sinks genuinely commit independently fails a
test instead of quietly making the old advice true again. The two analyser
mutations - restoring the ungrouped gate, and dropping the explicit "a
commit_group does NOT change this" - each fail exactly one of the two new
analyser cases and leave the other 24 green.

**What did not change.** `commit_group` is still accepted everywhere it was,
including the SQL property and the binder's rejection of it on non-2PC
sinks. Removing a documented option is a separate decision from correcting
what it claims to do.

### F36. Five config-linter checks could not fire through the CLI

Found by building `clink lint` (the W17 gap) and running it on the linter's
own motivating example. `config_lint.hpp` opens by saying what it exists for:
"Neither notices that `--checkpoint-interval-ms=500` with no
`--checkpoint-dir` produces no checkpoints at all." Running exactly that
returned `lint: no problems found`.

The cause is in the CLI, not the linter. `clink run` assembled its
`CheckpointConfig` inside `if (!ckpt_dir.empty())`, so with no directory the
interval, the restart budget, the restore pair and the capture settings were
all discarded before anything looked at them. Five of the linter's checks are
of the form "X was set but checkpoint_dir is empty", and every one of them was
unreachable from the command line. Each was unit-tested against a
hand-built config, and each passed.

**Risk:** the class of misconfiguration the linter was written to catch was
the class it could not catch. A job submitted with an interval and no
directory ran with no checkpoints, no error, and a clean lint.

**Fix:** the config is assembled from the flags whether or not a directory
was resolved, so the contradiction reaches the gate. This changes no runtime
behaviour - the coordinator's trigger loop already skips a job with no
directory - but the submission is now refused rather than silently accepted.
The assembly lives in `tools/cli_config_args.hpp`, shared by `clink run` and
`clink lint`, because a linter that parses flags its own way can disagree
with the gate it claims to preview, and then a clean lint means nothing.

**Also fixed while here:** the client-side lint ran only when `--profile` was
given. It now runs on every submission, which matches what the coordinator
already does (`check_config`, `coordinator.cpp:1861`) and moves the same
refusal earlier, before a connection is opened.

**Tests:** `tests/integration/test_cli_lint.cpp`, 10 cases against the real
binary, asserting exit codes rather than text because the exit code is what a
deploy pipeline gates on. Mutation-checked: restoring the flag-dropping
behaviour fails exactly the two cases written for this defect and leaves the
other eight green.

One of those cases, `TheNodeDefaultsAreWhatGetLinted`, exists for the one way
this command could do harm: filling unset liveness flags with defaults that
are not `clink_node`'s, so the combination linted is not the combination that
runs. Writing it caught two of my three defaults being wrong - the watchdog
interval is 200 rather than 1000, and the heartbeat interval is not a
`clink_node` flag at all but a fixed 500.

### F37. Completed-checkpoint markers were written into a subtask's state directory

Found by running the integration suite, which is opt-in
(`-DCLINK_INTEGRATION_TESTS=ON`) and therefore does not run in CI. Eleven of
its 110 cases were failing, and none of the failures were visible from the
unit suites.

Two distinct defects, both mine, both from the F29 fix that made the
`COMPLETED-N` marker job-scoped:

**The marker path collided with the per-subtask state namespace.** F29 moved
the marker to `<checkpoint_dir>/<job_id>/COMPLETED-N`. A job's per-subtask
state directories are `<checkpoint_dir>/0`, `/1`, `/2` - bare integers - so
job id 1 wrote every one of its markers into subtask 1's state directory. A
checkpoint tree for a three-subtask job looked like this, with nine markers
sitting alongside one subtask's snapshot:

```
0/checkpoint-9.snap
1/COMPLETED-1 .. COMPLETED-9   <- job 1's markers, inside subtask 1's directory
1/checkpoint-9.snap
2/checkpoint-9.snap
```

Fixed by moving them to `<checkpoint_dir>/_jobs/<job_id>/COMPLETED-N`: a
prefixed component cannot collide, because a subtask directory is always a
bare integer. Markers written by an older build are not migrated, and
nothing reads them.

**Three test helpers were left reading the old path.** F29 changed where the
marker is written and did not update the integration tests that look for it.
`test_two_phase_commit.cpp`, `test_worker_crash_recovery.cpp` and
`test_plugin_submission.cpp` each scanned only the TOP level of the
checkpoint directory for regular files, so once the marker moved one level
down they found nothing and failed claiming no checkpoint had completed -
while checkpointing worked perfectly. Now recursive, matching the harness
and `test_fault_recovery.cpp`, which were written with the job-scoped path
in mind and were correct throughout.

**Risk:** the marker collision is the more serious of the two. Coordinator
metadata was being written into a directory owned by a state backend, which
is a boundary violation whether or not it corrupted anything, and it scaled
with the job id: any job whose id happened to match a live subtask index
polluted that subtask's state directory.

**And a test that was betting on a number nobody had measured.**
`TwoPhaseCommit.RecoveryCommitsPreCommittedFilesOnRestart` waited up to 1s
for the first checkpoint, on the reasoning that a 100ms interval puts it at
about 200ms. Measured over three runs, the first `COMPLETED` marker lands at
1367 / 1398 / 1269 ms, because deploy, peer resolution and the coordinator's
hold-off until `peer_updates_sent` all precede the first trigger. The bound
is now 30s and the loop still returns the instant the marker appears - a
safety bound, not a synchronisation delay.

**Still failing, and not fixed:** see F38.

### F38. Half a job's keyed state silently does not survive a restore

`PluginSubmission.CheckpointAndRestoreAcrossJobRuns` restores the even-parity
count and starts the odd-parity one again from zero:

```
expected  5:1:3  6:0:3  7:1:4  8:0:4
actual    5:1:1  6:0:3  7:1:2  8:0:4
```

Nothing is logged and the job reports success. Left failing rather than
adjusted; making it pass today would mean asserting that losing half a job's
state is correct.

**The mechanism, established rather than guessed.** Dumping the snapshot with
`clink state-cat` settled where the loss is:

```
op 5447233828030261258 slot "parity_counts" (2 entries)
  kg=69 key (int64 0) = 2
  kg=36 key (int64 1) = 2
```

The snapshot is COMPLETE - both keys, both counts, in subtask 1's file. So
the write of the snapshot is not the problem, and neither is the read. The
job runs at parallelism 3, and instrumenting the worker's restore showed the
key-group ranges it hands each subtask:

```
subtask 0: [0, 43)    subtask 1: [43, 86)    subtask 2: [86, 128)
```

Subtask 1 restores its own file, keeps kg 69 because it owns it, and
discards kg 36 because it does not. Subtask 0, which does own kg 36, restores
an empty snapshot. Both halves of the system behave correctly and the state
is gone.

**The root cause is placement at write time.** Every record went through
subtask 1 whatever its key, so subtask 1 wrote state for a key group it does
not own. `JobGraphSpec::key_by` exists for exactly this and its comment
states the contract - "Same key always lands on the same subtask -> keyed
state with K is correct at parallelism > 1" - and the test's graph never set
it. The engine accepted the job, ran it, checkpointed it and restored it,
losing half the state at the last step without a word.

**What was fixed:** the silence. A restore that discards keyed entries now
counts them (`clink_state_restore_keys_dropped_total`) and logs a warning
naming the count and the range, which fires on this job at the default log
level:

> restore discarded 1 keyed entries whose key group falls outside this
> subtask's assigned range [43, 86). During a rescale that is correct ...
> Outside one it is state LOSS ...

Not an error and not a throw, deliberately: discarding out-of-range entries
is correct and routine during a rescale, and the backend cannot tell a
rescale from a same-parallelism restore. Only the caller knows, so the
backend's job is to count and report and the counter is what an alert
watches. `tests/test_restore_key_group_drop.cpp` pins it, including that a
restore which drops nothing does NOT move the counter - without that the
metric would be non-zero on every healthy restore and useless for alerting.

**What is still open, and it is the more interesting half.** Declaring the
key does not fix it. With `op.key_by = "hello.by_parity"` set - the extractor
the plugin registers for precisely this operator - the run is byte-identical:
subtask 1 still holds kg 36 and still discards it. `key_by` is serialised
(`job_graph.cpp:265`), read back (`:391`), and consulted by the planner
(`job_planner.cpp:687`), so something between the declaration and the
partitioner is not applying it on this deploy path, and nothing warns. A
declared key that silently does not partition is the same class of defect as
the one above and probably its cause.

**Not caused by this workstream.** The test could not have passed since the
marker moved (F37): it failed at the earlier assertion and never reached this
comparison.

**Recommended next step, recorded rather than rushed.** The symmetric check
belongs at snapshot time: a subtask writing a keyed entry whose key group is
outside its own range is provably producing state nothing can restore, and
that is detectable at the moment the mistake is made rather than at the next
restore. It needs the owned range plumbed into the backend, which is a bigger
change than is wise to land unreviewed at the end of a session.

### F10. Behaviour controlled by documentation rather than by code

Collected while reading. Each is a statement in a comment or doc page that
nothing enforced:

- "the marker is durable" (F1) - it was not.
- `CLAUDE.md`: "Stateful operators need a uid ... a missing uid is a
  correctness bug rather than a style nit" - not validated at submission.
- `docs/internals/state-snapshot-format.md` publishes the `.snap` file as a
  stable Arrow IPC contract. Nothing tested that a written snapshot is
  readable by a plain Arrow reader, so the contract could break silently.
  (Preserved deliberately by the sidecar design in W2.)

---

## 3. Work items

Ordered by the brief's priorities. Source locations are where the change
lives; evidence is in section 4.

| # | Item | Brief ref | Risk addressed | Status |
|---|---|---|---|---|
| W1 | Fault-injection framework | P0.2 | F6 | **Done** |
| W2 | Checkpoint integrity: checksums, versioned metadata, incomplete-vs-corrupt, fallback | P0.1, P1.10 | F4 | **Done** |
| W3 | Durable + race-free coordinator writes; withhold commit on marker failure | P1.10 | F1, F2 | **Done** |
| W4 | Durable rescale snapshot write with parent verification | P1.10 | F3 | **Done** |
| W5 | Connector capability contract + runtime/CLI manifest | P0.5 | F5 | **Partial** |
| W6 | End-to-end delivery-guarantee analyser, enforced at submission | P0.6 | F5 | **Done** |
| W7 | Deterministic multi-process harness | P0.1 | F7 | **Done** |
| W8 | Fault-tolerance scenarios (gating) + sink exactly-once verified by output equality | P0.1 | F7 | **Partial** |
| W9 | Automated sanitizers (PR subset + nightly full), blocking | P1.7 | F8 | **Done** |
| W24 | Widen the blocking integration gate; 11 advisory failures had gone unseen | P0.1 | F37, F38 | **Partial** |
| W10 | `clink checkpoint-verify` + migration path | P1.10, P1.12 | F4 | **Done** |
| W11 | SQL bounded-state validator, enforced in the planner | P0.3 | F9 | **Done** |
| W12 | State TTL depth: event time, incremental cleanup, metrics, SQL `state_ttl` enforced by GROUP BY | P0.3 | F9 | **Partial** |
| W13 | Strict rejection of unsupported SQL semantics | P0.4 | F14, F15 | **Partial** |
| W14 | Resource and overload limits: frame size cap, bounded element counts, exception boundaries | P1.9 | F21, F22, F23 | **Partial** |
| W15 | Coordinator fencing: epoch on the wire, worker enforcement, metadata guard | P1.11 | F16 | **Partial** |
| W16 | Protocol version negotiation across RPC/frames/state | P1.12 | F19, F20 | **Partial** |
| W17 | Config linter + recovery profiles, enforced at submission | P1.13 | F31, F36 | **Partial** |
| W18 | OpenTelemetry tracing | P1.14 | - | **Open** |
| W19 | Production metrics: staleness gauge, restart counters | P1.15 | F33 | **Partial** |
| W20 | Non-determinism detection API | P2.16 | - | **Partial** |
| W21 | Cancellation/shutdown audit | P2.17 | F24 | **Partial** |
| W22 | Side-output / multi-sink propagation validation | P2.18 | F35 | **Partial** |
| W23 | Fuzz targets + committed-corpus regression replay | P1.8 | - | **Partial** |

---

## 4. What was implemented, with evidence

### W1 - Fault-injection framework — Done

**Source:** `include/clink/fault/fault_injection.hpp`, `src/fault/fault_injection.cpp`

Named fault points inside runtime code, activated deterministically by
`(name, ordinal)` - never by timing, thread id or a random seed. Arming the
same rule and driving the same input reproduces the same fault at the same
point.

Actions: `throw`, `exit` (`_exit`, so no unwinding, no `atexit`, no stream
flush - the closest a process can get to being `SIGKILL`ed at that exact
line), `abort`, `block` (until released by the test), `delay`, `error`
(observed, not thrown), `truncate` (short write), `observe` (counts only).

Cross-process arming via `CLINK_FAULT_INJECT="point=action[:arg][@ordinal]"`.
This is what lets an integration test kill a *spawned* `clink_node` at a
precise point in the checkpoint protocol.

Build gating: `CLINK_ENABLE_FAULT_INJECTION` (`AUTO` follows
`CLINK_BUILD_TESTS`). When off, `CLINK_FAULT_POINT` expands to a
default-constructed `Outcome`, no counter is touched and no registry symbol
is referenced. The state is reported by `clink --capabilities`, so an
operator can see at a glance whether a running node carries the surface.

Fast path: one relaxed atomic load when nothing is armed - no mutex, no map
lookup, no `Registry::instance()` guard check.

Points wired so far: `checkpoint.before_write`, `.during_write`,
`.before_fsync`, `.before_publish`, `.after_publish`;
`coordinator.before_metadata_write`, `.before_completed_marker`,
`.after_completed_marker`, `.before_commit_broadcast`;
`state.before_restore`.

**A bug this found in its own implementation:** the first `reset()` cleared
the per-point release epoch while a thread was parked on `Block`. The woken
thread re-read a zeroed counter, decided it had not been released, and slept
for ever - a real deadlock, caught by `ResetReleasesAParkedThread` before
anything depended on it. Fixed with monotonic epochs that are never reset;
the reasoning is recorded in the header so it is not undone.

**Tests:** `tests/test_fault_injection.cpp`, 17 cases.

### W2 - Checkpoint integrity — Done

**Source:** `include/clink/state/checkpoint_integrity.hpp`,
`include/clink/state/file_backed_state_backend.hpp`

A versioned, CRC-32C-checksummed sidecar (`<name>.snap.meta`) beside each
payload, written **after** the payload is durable.

**Why a sidecar and not an envelope.** `docs/internals/state-snapshot-format.md`
publishes the `.snap` file as a stable public contract: pyarrow, DuckDB,
Polars and Spark open it directly with no clink code involved. Prefixing a
magic header would break every one of those readers. The sidecar keeps the
payload byte-for-byte the Arrow stream it always was.

The two-file write order is also what makes publication atomic at the
*checkpoint* level rather than the file level: a `.snap` with no `.meta` is
definitionally unfinished, whatever killed the writer between them.

Status taxonomy, which recovery needs in order to decide whether it may fall
back:

| Status | Meaning | Recovery action |
|---|---|---|
| `Missing` | No payload. Not an error. | Keep looking |
| `Incomplete` | Never finished publishing: no sidecar, or a length disagreement. Nothing was lost, because nothing was promised. | Fall back |
| `Corrupt` | Finished publishing, bytes have since changed: length agrees, checksum does not. | Fall back, loudly |
| `Unsupported` | Sidecar is a newer format version than this binary understands. | Refuse; do not guess |
| `Valid` | Length and checksum both agree. | Restore |

`restore()` now refuses anything that does not verify.
`latest_valid_checkpoint()` walks ids downwards and returns the newest that
verifies, reporting every id it skipped and why - a silent rewind is
indistinguishable from data loss.

CRC-32C is software table-driven (no SSE4.2 / ARMv8-CRC assumption), so a
checksum written on a macOS host verifies in a Debian container.

**Compatibility.** A checkpoint directory written by clink <= 0.6.0 has
valid payloads and no sidecars, so a strict reader classifies it
`Incomplete`. Two migration routes, both deliberate:

- `clink checkpoint-verify --dir <path> --repair` mints sidecars for
  payloads that have none. The tool states in its output that this
  certifies the bytes *as they stand now*, not as the writer intended them.
- `CLINK_ALLOW_UNVERIFIED_CHECKPOINTS=1` downgrades **only** the
  missing-sidecar case to a pass. It does not extend to `Corrupt`,
  `Unsupported`, or a length mismatch, and it is reported in
  `clink --capabilities` because a cluster running with it has a weaker
  recovery guarantee.

**Tests:** `tests/test_checkpoint_integrity.cpp`, 23 cases, including
fault-injected short writes at the real durable-write call site, death
before publish, and death between payload and sidecar.

### W3 - Durable coordinator writes — Done

**Source:** `src/cluster/coordinator.cpp`

`atomic_write_file` now routes through `write_string_fsync_rename`: unique
per-write temp name (pid + monotonic counter), `fsync` the file, rename,
`fsync` the directory. Fixes both halves of F2.

The `COMPLETED-N` marker is written durably **before** the commit broadcast,
and a write failure now **withholds the broadcast** for that job and
checkpoint, with an error log naming the consequence. Skipping the broadcast
is the safe side: the prepared transactions stay prepared and are resolved
at the next successful checkpoint or at restore, whereas committing without
a durable record is unrecoverable.

### W4 - Durable rescale snapshot write — Done

**Source:** `src/state/state_backend_factory.cpp`

The stitched snapshot is written through `write_fsync_rename` +
`write_checkpoint_meta`, so a failure throws instead of being swallowed.
Parent snapshots are verified before being merged: a parent whose bytes are
damaged would otherwise be merged straight into the child's state and the
rescaled job would come up quietly missing whatever key groups that parent
owned. `Missing` stays non-fatal (not every parent index has a snapshot).

The three existing rescale tests were updated to publish their fixtures the
way the production writer does (payload + sidecar) rather than with a bare
`ofstream`. That strengthens them: they now exercise the real published-
checkpoint shape.

### W5 - Connector capability contract — Partial

**Source:** `include/clink/connectors/capability.hpp`,
`src/connectors/capability.cpp`, `src/connectors/builtin_capabilities.cpp`,
per-impl declarations in `impls/{kafka,postgres,s3}/src/register_factories.cpp`

Machine-readable per-connector records covering every field the brief lists.
Delivery is a closed enum, not a string:

`AtMostOnce`, `AtLeastOnce`, `EffectivelyOnceIdempotent`,
`ExactlyOnceAtomicPublish`, `ExactlyOnceTwoPhaseCommit`,
`NoDurableRestartGuarantee`.

Declared beside the factory registration, so the manifest reflects what was
compiled into *this binary*. `clink --capabilities` /
`clink --capabilities-json`.

`self_check()` catches an incoherent declaration - two-phase commit without
transactionality, exactly-once without checkpoint integration, a
non-replayable source claiming better than at-most-once, an idempotent claim
naming no key option. A test runs it over every declaration in the binary,
so a wrong record fails the build rather than being believed by the planner.

**What makes this Partial.** Capabilities are declared for 11 connectors
(`file`, `file_2pc`, `parquet`, `parquet_2pc`, `generator`, `blackhole`,
`kafka`, `kafka_2pc`, `postgres`, `postgres_2pc`, `postgres_upsert`, `s3`,
`s3_2pc`) out of ~30 impls. Each declared record was derived by reading the
implementation it describes, which is the only honest way to produce one and
is why the rest are not done. Undeclared connectors are **not** assumed
good: the analyser treats a missing declaration as at-least-once and refuses
an exactly-once request that involves one (test:
`UndeclaredConnectorIsAssumedWeakNotAssumedGood`).

The shared connector contract-test suites the brief asks for (sources:
offset snapshot, restore, duplicate delivery, credential expiry, ...; sinks:
crash-after-prepare, idempotent recommit, transaction timeout, ...) are
**not** implemented. That is W-open work; several would need containerised
services in CI.

### W6 - End-to-end delivery-guarantee analyser — Partial

**Source:** `include/clink/connectors/delivery_guarantee.hpp`,
`src/connectors/delivery_guarantee.cpp`

Computes the strongest guarantee the whole pipeline can provide from source
replayability, checkpointing, state-backend durability, sink commit protocol
and **the options actually supplied**. Reports one of:

`NO_RECOVERY_GUARANTEE`, `AT_MOST_ONCE_SOURCE`,
`STATE_EXACTLY_ONCE_OUTPUT_AT_LEAST_ONCE`,
`EFFECTIVELY_ONCE_REQUIRES_IDEMPOTENT_KEY`, `END_TO_END_EXACTLY_ONCE`.

Deliberately verbose: a label that cannot be misread as a blanket
exactly-once promise is the point of the exercise.

Behaviours worth naming:

- The **weakest sink caps the pipeline**, not the best one.
- A sink that declares exactly-once but was **not given the options its
  mechanism needs** (a `transactional_id`, a `dir`) degrades to at-least-once
  rather than being taken at its word.
- **Determinism is reported separately** from delivery. A job can commit each
  record exactly once and still emit different records on replay; conflating
  the two is how "exactly once" stops meaning anything.
- A requested guarantee the pipeline cannot honour produces a rejection that
  names both what was asked for and what caps it.

**What makes this Partial.** The analyser is complete and tested against
synthetic pipelines. It is **not yet called** from job submission or SQL
validation, so it does not currently block a real job. Wiring it into
`Coordinator::submit_job` and SQL `EXPLAIN` is the next step and is the
single highest-value remaining item.

### W7 - Deterministic multi-process harness — Done

**Source:** `tests/integration/cluster_harness.hpp`

Replaces the duplicated `spawn_proc` / `kill_quietly` / `probe_free_port` /
`await_port_open` set, and more importantly replaces what they encoded:

- **Readiness is a condition, polled to a monotonic deadline.** The deadline
  is a failure bound, not a synchronisation mechanism; every `await_*`
  returns the instant its condition holds. `steady_clock` throughout, so an
  NTP step cannot shorten or lengthen a deadline. Conditions provided: port
  accepting, worker registered (counted, so out-of-order registration is
  handled), specific `COMPLETED-N` present, any checkpoint completed,
  process gone, submitter exited.
- **Ports are held open until the moment of spawn.** `ReservedPort` keeps the
  listener and releases it immediately before `posix_spawn`, shrinking the
  TOCTOU window to the exec and turning a collision into a loud bind failure
  in the child's log rather than a silent hang.
- **Processes are owned.** The `Cluster` destructor reaps workers then the
  coordinator, so an abandoned child cannot outlive the test binary and
  poison the next test.
- **Failure produces evidence.** `ScopedDiagnostics` dumps every child's
  stdout/stderr into the gtest output and keeps the scratch tree when the
  test fails. A multi-process failure with no child logs is the other half
  of why these tests never became gates.
- **Faults are armed in children** through `ProcOptions::fault`, so a test
  can kill a worker at an exact protocol point.

This paid for itself immediately: the first run failed, and the dumped child
log said `worker requires --id` - a missing flag in the harness itself,
diagnosed from the transcript in one read rather than by re-running locally.

### W8 - Fault-tolerance scenarios — Partial

**Source:** `tests/integration/test_fault_recovery.cpp`

Scenarios implemented on the harness, all event-driven:

| Scenario | Brief requirement | Test |
|---|---|---|
| Harness brings up a cluster and reaps it | (harness contract) | `HarnessBringsUpAClusterAndReapsIt` |
| Worker death before any checkpoint | required | `WorkerDeathBeforeAnyCheckpointStillCompletes` |
| Worker death after a completed checkpoint | required | `WorkerDeathAfterCheckpointRecoversAndCompletes` |
| Multiple consecutive worker failures | required | `TwoConsecutiveWorkerFailuresAreSurvived` |
| Coordinator death during a running job | required | `CoordinatorDeathIsReportedNotSilentlyHung` |
| Every published checkpoint verifies | (W2 end-to-end) | `EveryCheckpointTheClusterPublishesVerifies` |
| Two separated worker failures | required (partial - see F12) | `TwoSeparatedWorkerFailuresAreSurvived` |
| Worker killed at an exact protocol point | required (fault-injected) | `WorkerKilledAtTheStateRestorePointIsRedeployed` |

The last one is the proof that the fault-injection framework does what it
claims across a process boundary: the parent arms
`state.before_restore=exit:70@1` in a spawned `clink_node` through the
environment, the test asserts the worker exited with **exactly 70** (so the
fault landed where it was aimed and the worker did not merely die of
something else), and the job then recovers onto the restarted survivor.
Asserting the exit code rather than just "it died" is what stops the test
silently becoming a test of nothing if the point is renamed.

Note that this scenario needs `--state-backend=file:` explicitly.
`--checkpoint-dir` alone leaves the job on the in-memory backend, which
never travels `FileBackedStateBackend::restore`, so the armed fault would
never fire - a trap worth naming, because the test passes for the wrong
reason if you do not notice.

**Not yet covered** from the brief's required list: death during barrier
alignment; death after sink prepare but before global completion; death
after global completion but before commit acknowledgement; recovery when the
newest checkpoint is incomplete (covered at unit level in W2, not at cluster
level); network send failure; oversized network batch; end-of-stream during
an active checkpoint; rescaling from a savepoint; stateful multi-input
recovery; side-output checkpoint propagation. Each needs a fault point wired
into the corresponding runtime path first; the framework is in place, the
points are not all placed.

### W9 - Automated sanitizers — Done

**Source:** `.github/workflows/sanitizers.yml`

Three triggers replacing `workflow_dispatch`-only:

- **`pull_request`**: fast ASan + UBSan over the concurrency- and
  memory-sensitive labels. Blocking.
- **`schedule`** (02:30 UTC daily): the full asan/tsan/ubsan matrix over the
  whole suite. Blocking.
- **`workflow_dispatch`**: manual, for pre-release.

No `continue-on-error` anywhere in the file. Logs are uploaded as artifacts
on failure (14 days for PR, 30 for nightly) so a red run is diagnosable
without reproducing it.

The known pre-existing ASan tail (~24 tests that spawn the CLI as a
subprocess, stand up an HTTP server, or run an in-process cluster - their
ASan diagnostics collide with assertions on child stdout) is excluded **by
name** from the PR subset, as a reviewable list. The nightly full run still
executes them, so the tail stays visible and cannot quietly grow.

### W10 - `clink checkpoint-verify` — Done

**Source:** `tools/clink_checkpoint_verify.cpp`

Answers "can this job actually recover, and from which checkpoint" without
starting the job. `--repair` is the supported migration for pre-0.7
directories; `--json` for automation. Exit 0 all valid, 1 at least one not,
2 bad usage.

### W11 - SQL bounded-state validator — Partial

**Source:** `include/clink/sql/bounded_state.hpp`, `src/sql/bounded_state.cpp`

Walks a logical plan for constructs that retain per-key state for the life
of the job (`Aggregate`, `Distinct`, `EquiJoin`, `SemiJoin`, `SetOp`,
`RowNumber`) and reports each with what it retains and the concrete SQL
alternatives. Satisfied by any of: a bounded input, an explicit
`state.ttl`, or `ALLOW UNBOUNDED STATE`.

The node list is deliberately closed rather than "anything stateful":
windowed operators release state when the window fires and TopN-per-key is
bounded by N, so flagging them would be noise, and a gate that cries wolf
gets disabled.

**What makes this Partial.** The validator and its retention parsing are
implemented; they are **not yet wired into the binder**, so no query is
currently rejected. Wiring needs the `ALLOW UNBOUNDED STATE` suffix in
`src/sql/preparse.cpp`, a boundedness verdict threaded from the scan's table
definition, and the metric for the unsafe override.

### W20 - Non-determinism — Partial

`DeterminismFacts` exists in the analyser and its effect on the report is
tested (`NonDeterminismIsWarnedAboutSeparatelyFromDelivery`). What is **not**
implemented: detecting non-determinism automatically (wall-clock reads,
`RANDOM()`, HTTP calls in `ML_PREDICT`, non-deterministic UDFs), the API for
a user to declare it, and exposure through `EXPLAIN`. The facts must
currently be supplied by the caller.


### W6 (continued) - the analyser now enforces

**Source:** `include/clink/cluster/guarantee_gate.hpp`, `src/cluster/guarantee_gate.cpp`,
called from `Coordinator::submit_job`.

The bridge from a real `JobGraphSpec` to the analyser. Roles are derived
from the graph's edges (no inputs = source, nothing consumes it = sink)
rather than guessed from type names, because a `*_sink` heuristic misses
every connector that does not follow the convention.

Op type resolves to connector by **longest prefix**:
`kafka_2pc_sink_string` must resolve to `kafka_2pc`, never `kafka`. The two
carry different guarantees, and taking the shorter match would
systematically over-promise - the exact failure the mechanism exists to
prevent.

The gate rejects only when the submitter ASKED for more than the pipeline
can provide. A job that asks for nothing gets its computed guarantee
logged and proceeds; most jobs are at-least-once and that is a legitimate
choice, not an error.

**Two things it caught in the existing suite, both genuine:**

- `SqlRuntime.FileExactlyOnceSinkProducesCommittedRecords` declared
  `delivery_guarantee='exactly_once'` with checkpointing **off**. A 2PC
  sink commits on the coordinator's `CommitCheckpoint` broadcast, which
  never fires without checkpointing, so the test was asking for a
  guarantee it could not get - as its own comment admitted. Configuration
  made coherent; the assertion is unchanged.
- A false positive in my own declaration: `file_2pc` requires `dir`, but
  the SQL DDL supplies `path`. Requirements now support alternatives
  (`"dir|path"`), rendered readably as `'dir' or 'path'`.

**Tests:** `tests/test_guarantee_gate.cpp`, 13 cases.

### W8 (continued) - exactly-once at the sink, per crash window

**Source:** fault points in `include/clink/connectors/committing_sink.hpp`;
`tests/test_exactly_once_windows.cpp`.

The multi-process suite proves a job RECOVERS. That is weaker than "each
record reached the external system exactly once", which is what the phrase
is usually taken to mean and what nothing in this tree asserted.

These assert the output **multiset** after a crash at each window of the
2PC choreography, using the real `CommittingSink` base over a real durable
backend. A crash is modelled by discarding the sink and constructing a
fresh one over the same state directory - what the runtime does after a
worker is lost.

| Window | Must guarantee | Result |
|---|---|---|
| before prepare | no external effect; replay commits once | pass |
| after prepare, before the handle is persisted | staged output not visible; replay does not double it | pass |
| after global completion, before commit | recovery MUST commit, or the records are lost | pass |
| after the external commit, before local ack | recovery commits AGAIN; only an idempotent commit survives | pass |
| duplicate commit broadcast, no crash | harmless | pass |
| abort | publishes nothing; replay commits once | pass |
| crash between two checkpoints | output is exactly the union | pass |

8 cases. The fourth is the one that matters most: it is the single hardest
case in the protocol and the only direct test that `commit()` is genuinely
idempotent rather than merely documented as needing to be.

### W11 (continued) - the bounded-state gate now enforces

**Source:** `ALLOW UNBOUNDED STATE` stripped in `src/sql/preparse.cpp`
(text level, like every other clink-only clause - a grammar fork would have
to be re-applied on each libpg_query bump), carried on `ast::Script`,
checked in `PhysicalPlanner::compile`, counted by
`clink_sql_unbounded_state_overrides_total`.

**Boundedness is tri-state, and that is the load-bearing decision.**
The first cut treated an undeclared connector as unbounded. That was wrong
and the test suite said so immediately: capability declarations cover a
subset of the catalogue, so unknown is the COMMON case, and the gate
rejected 19 planner tests reading plain files. A gate that cries wolf gets
switched off wholesale, which is worse than one with a known blind spot.

Only KNOWN-unbounded rejects. The blind spot shrinks as connectors are
declared, which is the right incentive: declaring a connector strictly
increases what the gate catches and never decreases it.

The retention option is `state_ttl`, a bare identifier. `state.ttl` is a
syntax error in PostgreSQL's WITH grammar, and the dialect already spells
options this way (`delivery_guarantee`, `primary_key`, `commit_group`).

**Tests:** `tests/test_sql_bounded_state.cpp`, 15 cases, including
end-to-end through the planner: a windowless GROUP BY over a known
unbounded source is refused; the same query over a file is accepted; and
both `state_ttl` and `ALLOW UNBOUNDED STATE` unlock it.

### W12 - State TTL depth — Partial

**Source:** `include/clink/state/keyed_state.hpp`

**Event-time TTL.** Not a nicety. A processing-time TTL is measured against
the wall clock of the processing machine, so on a backfill - six months of
history replayed through a job with a one-hour TTL - every entry is already
older than the TTL the instant it is written, and the job silently produces
nothing. Conversely a job that stalls for two hours expires state that is
seconds old in the stream's own terms. Event time measures against the
watermark, so retention means what a user means by it, and a replay behaves
identically to the original run.

Semantics pinned by test:

- Expiry follows the watermark, not the wall clock.
- Nothing expires before the FIRST watermark (a zero watermark would make
  every stamped entry look expired and wipe the slot at job start).
  `expire_before_first_watermark` opts into the other reading.
- A watermark regression is ignored - honouring it would resurrect expired
  state, making retention depend on arrival order.
- A snapshot carries the ABSOLUTE stamp, so a restore resumes the same
  deadline rather than silently extending every entry's life by the length
  of the outage.
- A late record targeting expired state sees nothing. Resurrecting on a
  late arrival would make retention unbounded again.

**Incremental cleanup.** `cleanup_batch(budget)` sweeps a bounded number of
entries and erases the expired ones, resuming from where the last sweep
stopped. This exists because lazy expiry alone never releases memory: an
entry written once and never read again is hidden from readers but stays
resident, so a TTL that was supposed to bound memory does not. Bounded
because an unbounded sweep stalls the operator thread proportionally to
state size - the latency spike a TTL'd job is trying to avoid.

**Metrics.** `TtlStats`: expired-on-read, expired-in-cleanup, live entries,
scanned entries, estimated bytes, and unscanned backlog - the backlog being
the cleanup lag, since while it is non-zero expired state is still resident.

**Tests:** `tests/test_keyed_state_ttl_depth.cpp`, 17 cases.

**`state_ttl` is now enforced by the running GROUP BY.** The option used to
satisfy the gate and change nothing at runtime - an intent nothing acted
on, which is worse than declaring nothing. The path is now complete:

```
CREATE TABLE src (...) WITH (connector='kafka', state_ttl='1h')
  -> resolve_plan_retention()      shortest non-zero TTL across the inputs
  -> op.params["state_ttl_ms"]     stamped on the aggregate_row spec
  -> AggregateRowOp(state_ttl_ms)  deadline per group, evicted on watermark
```

**Why the deadline lives in the operator and not in `KeyedState`'s own
`TtlConfig`.** This operator's hot path is an in-memory map that is flushed
into the "agg" slot at every checkpoint. A `KeyedState` TTL stamps on every
put, so each flush would refresh the deadline and a group touched once
would nonetheless live for ever - the TTL would appear to work while
bounding nothing. The operator owns the deadline, so a group's clock starts
when the DATA last touched it, not when the checkpoint last wrote it.

Deadlines persist in their own `agg_ttl` slot rather than inside
`AggBucket`: additive, so no existing checkpoint's bucket encoding changes,
and a job that sets no retention writes nothing extra. They are absolute,
so a restored group resumes its original deadline instead of getting a
fresh full TTL - otherwise every restart would silently extend retention,
and a job that restarts often would never expire anything.

Eviction is driven by watermark advance rather than only at checkpoint
time, so a running job reclaims memory as it goes.

`state_ttl_domain` selects the clock, defaulting to `event_time`. A stream
with no watermarks must say `processing_time`; under event time, nothing
expires until the first watermark arrives, matching `KeyedState`'s rule so
the two cannot disagree.

**Tests:** `tests/test_sql_state_ttl_runtime.cpp`, 8 cases driving the real
operator through the registry. The load-bearing one asserts that an expired
group's accumulator is RELEASED, not merely hidden: it feeds a key, expires
it, feeds it again, and requires the running total to restart from zero. An
aggregate that stops reporting a group while still holding its accumulator
has bounded nothing, and only this distinguishes the two.

**Every node kind the gate flags now enforces.** A gate that refuses a
DISTINCT without `state_ttl` and then compiles it with the TTL going
nowhere has moved the problem, not fixed it. The coverage:

| Gate flags | Operator | Enforcement |
|---|---|---|
| `Aggregate` | `aggregate_row` | operator-owned deadlines |
| `Distinct` | `distinct_row` | `KeyedState` TtlConfig + `cleanup_batch` |
| `EquiJoin` | `equi_join_row` | operator-owned deadlines, both sides |
| `SemiJoin` | `semi_join_row` | operator-owned deadlines, both sides |
| `SetOp` | `set_op_row` | `KeyedState` TtlConfig + `cleanup_batch` |
| `RowNumber` | - | never compiles: the planner rejects it outright |

**Two mechanisms, chosen by where the state lives.** This is the design
decision, and getting it wrong would produce a TTL that appears to work
while bounding nothing:

- `aggregate_row`, `equi_join_row` and `semi_join_row` keep their hot state
  in in-memory maps that are flushed to the backend at every checkpoint. A
  `KeyedState` TtlConfig stamps on every put, so each flush would refresh
  the deadline and a key touched once would live for ever. These use
  `StateTtlTracker` (`clink/sql/state_ttl.hpp`), where the OPERATOR owns
  the deadline, so a key's clock starts when the DATA last touched it.
- `distinct_row` and `set_op_row` keep ALL their state in `KeyedState` -
  no hot map, so no re-stamping flush. `KeyedState`'s own TtlConfig is
  correct there, and reusing it beats a second mechanism: the stamping,
  hiding, lazy purge and incremental cleanup are already tested.

`StateTtlTracker` is shared rather than reimplemented four times because
the interesting parts are decisions that must be identical everywhere -
absolute deadlines, nothing expires before the first watermark, a
monotonic clock - and four copies would drift.

**Joins evict a key from both sides at once.** Keeping one side of an
expired key would leave a half-join that can never complete but still
occupies memory, and a key is touched from EITHER side: expiring a key
whose left side went quiet but whose right side is active would drop
matches that are still arriving.

`RowNumber` needs nothing: `PhysicalPlanner` rejects a top-level
`ROW_NUMBER() OVER` outright ("must be paired with a WHERE rn <= N"), so it
cannot reach execution. The gate flagging it is harmless belt-and-braces.

**Tests:** `test_sql_state_ttl_runtime.cpp` (12) and
`test_sql_bounded_state.cpp` (23). The runtime cases assert RELEASE, not
concealment: for the aggregate, an expired group's running total restarts
from zero; for DISTINCT, twenty expired values leave zero entries resident
in the backend. An operator that stops reporting a key while still holding
it has bounded nothing, and only that distinction separates the two.

**Collection state now has retention too, and the docs no longer lie
about it.** `typed_state.hpp` claimed `ListState` / `MapState` /
`AggregatingState` / `ReducingState` inherited TTL "for free". They did
not: the constructors never accepted or forwarded a `TtlConfig`, so a
caller who read that sentence and expected bounded state got unbounded
state. A textbook instance of finding F10 - behaviour controlled by
documentation rather than by code - sitting in the header the whole time.

All four now take an optional `TtlConfig` (with matching `RuntimeContext`
factory overloads) and forward `advance_watermark`, `cleanup_batch` and
`ttl_stats`.

**Retention on a collection is PER KEY, not per element.** The whole
collection for a key is one `KeyedState` value, so it lives and dies as a
unit: a map with one hot entry and a thousand cold ones retains all
thousand, and touching any entry refreshes them all. That is a legitimate
design given the representation, but it is emphatically not what "TTL on a
map" suggests, and a caller who assumed per-entry expiry would size their
state completely wrongly. It is pinned as a named test
(`MapRetentionIsPerKeyNotPerEntry`) rather than left in a comment for
exactly that reason. Per-element expiry needs one backend key per element -
a different representation, not done.

**Tests:** `tests/test_typed_state_ttl.cpp`, 14 cases across all four
types: expired-reads-empty, mutation refreshes, reads do not refresh,
cleanup releases rather than hides, keys expire independently, restore
resumes the original deadline, and nothing expires before the first
watermark.

**CEP partial matches.** `cep_operator.hpp` and `pattern.hpp` both said
"without within(), partials live indefinitely". A pattern whose first step
matches often and whose later steps rarely complete accumulates one partial
per unmatched start, for ever - the same unbounded shape the SQL gate
refuses for a windowless GROUP BY, in a surface the gate cannot see.

`Pattern::state_ttl()` bounds it, reusing the existing prune-on-watermark
machinery (which already routes evicted partials to the timed-out side
output and erases keys whose partial list empties). `eviction_bound()`
returns the tighter of the two.

The distinction between the bounds is enforced, not just documented:

| | `within()` | `state_ttl()` |
|---|---|---|
| kind | semantic | resource |
| binds at match time | yes | **no** |
| prunes on watermark | yes | yes |

`within()` binds at match time because a match spanning more than the bound
IS NOT A MATCH. A resource bound must not do that, or results would depend
on where watermarks happen to fall between records. A TTL can nonetheless
suppress a match that would have completed - which is why an evicted
partial reaches the timed-out side output, making the loss visible rather
than silent.

**Backend expiry-compaction hook.** `cleanup_batch` scans. On an LSM
backend that is the wrong shape twice: the scan competes with the write
path, and the backend is already rewriting every live SST during
compaction, so it can drop expired entries for free while it is there.

`StateBackend` gains `supports_expiry_compaction` / `set_expiry_filter` /
`compact_expired`; `KeyedState` delegates when the backend can and keeps
its scan as the portable fallback. RocksDB implements it via a
`CompactionFilterFactory` - a factory rather than a filter because CFs are
created lazily per operator while the TTL is only known once an operator
binds its state.

Two things had to happen before the filter saw anything, and finding them
is what makes this a real implementation rather than an interface with a
fake behind it: **drain the pending WriteBatch** (this backend buffers
Puts) and **flush the MemTable** (a filter is only consulted on SST
contents, and the write buffer is 64 MB). Missing either makes the hook
silently no-op on exactly the recent state retention is meant to reclaim,
while appearing to work in a long-running job where memtables flush on
their own. The RocksDB test forces a real compaction and asserts the dead
entries are gone and the live ones are not.

The predicate is scoped to its own operator AND its own slot: a slot with
no TTL must not have its first eight bytes read as a deadline, or the
filter would drop live state belonging to something else.

**Per-element expiry.** `ExpiringMapState` / `ExpiringListState`
(`expiring_collection_state.hpp`) give each element its own backend entry
and therefore its own deadline. Added ALONGSIDE the per-key types rather
than replacing them, because the trade-off is real and neither answer is
right for every case:

| | `typed_state.hpp` | `expiring_collection_state.hpp` |
|---|---|---|
| representation | one value per key | one entry per element |
| expiry granularity | per key | per element |
| read one element | O(collection) | O(1) |
| read whole collection | O(1) backend read | **O(slot) scan** |
| write one element | O(collection) read-modify-write | O(1) |

The O(slot) whole-collection read is the price, and it is stated in the
header rather than discovered later. List elements are discriminated by a
big-endian sequence so insertion order survives, and the high-water
sequence is recovered from stored keys on the first append after a restart
- without that, the first append would reuse seq 0 and overwrite the
oldest surviving element.

**Tests:** `test_cep_state_ttl.cpp` (9), `test_expiry_compaction.cpp` (9)
plus two real-RocksDB cases, `test_expiring_collection_state.cpp` (15).
`PerKeyAndPerElementDifferObservably` runs the same workload through both
collection types and asserts they behave differently, so the choice between
them stays a real choice rather than a coin flip.

**What makes this Partial.** NOT covered:

- The interval join and windowed operators, which are bounded by their
  time condition or window and so are not flagged by the gate - correct
  today, but they would still benefit from a retention ceiling on a
  pathological key space.
- No operator drives `cleanup_batch` on a collection slot; the method is
  exposed and tested, but a user of `ListState` must call it themselves.
  The SQL operators drive their own eviction; the typed C++ API leaves the
  schedule to the caller.
- The expiry-compaction hook is implemented for RocksDB only. ForSt and the
  S3-backed variants inherit the default (no hook), so they fall back to
  scanning - correct, just slower to give memory back.

### W13 - Strict rejection of unsupported SQL semantics — Partial

**Source:** `include/clink/sql/ast.hpp` (constraint capture),
`src/sql/ast_builder.cpp`, `src/sql/catalog.cpp`,
`include/clink/sql/table_option_check.hpp`, `src/sql/table_option_check.cpp`

**Method.** Rather than reading the parser and guessing, every candidate
construct was compiled and observed. The audit output is reproducible: a
temporary probe printed ACCEPTED / rejected per construct, and the work
below addresses what it found. Several constructs turned out to be
correctly rejected already (schema-qualified names, CROSS/NATURAL JOIN,
non-equi join conditions, `CURRENT_TIMESTAMP`, TABLESAMPLE, `DISTINCT ON`,
GROUPING SETS) and were left alone.

**Column constraints.** `ColumnDef` now carries them, and the PG
discriminator mapping (`CONSTR_NOTNULL`, `CONSTR_PRIMARY`, ...) was read
off a real parse tree rather than assumed. Then:

- `PRIMARY KEY` is **honoured** - it populates `TableDef::primary_key`,
  making the header's long-standing promise true (F14).
- `NOT NULL`, `UNIQUE`, `CHECK`, `DEFAULT`, `REFERENCES` are **refused**,
  naming the column and offering an alternative (a `WHERE` clause or the
  source system). clink evaluates no per-row constraints, and accepting one
  lets a job appear to validate data it never checks.
- `NULL` is ignored, because it is the default and asserts nothing.

The WITH-option form wins over an inline PRIMARY KEY: it is the more
specific statement and the form that survives a catalog JSON round trip.

**Option checking.** Near-miss detection plus closed-domain value
validation, NOT an allowlist. The passthrough option space is open-ended
and connector-specific, so rejecting anything unrecognised would break most
connectors. Only keys within a small edit distance of an option clink
itself interprets are refused - which is precisely where a typo changes
semantics silently.

**A mistake worth recording.** The first closed domain for `mode` listed
`append` and `upsert`, and rejected every CDC table in the suite because
`cdc` is also valid. The list is now derived from the `== "cdc"` sites in
`physical_plan.cpp`, and `EveryLegitimateModeValueIsAccepted` guards it.
A hand-assembled domain list is only safe when it was read off the code.

**Tests:** `tests/test_sql_unsupported_semantics.cpp`, 20 cases.

**Unknown scalar functions (F25).** Every function name is now resolved at
BIND time against the evaluator's own op table plus the scalar UDF
registry. An unrecognised name is refused with a diagnostic that names it
and suggests the nearest real function; `substringg()` gets "did you mean
substring()?".

The candidate set is read from `value_op_table()` - the same table the
dispatcher consults - and never from a list kept in the binder. That is the
`mode='cdc'` lesson applied in advance: a hand-kept copy drifts the first
time an op is added, and the symptom of drift here would be refusing a
function that works.
`EveryBuiltInScalarFunctionIsStillAccepted` walks the table and asserts
every listed name is dispatchable, so the two cannot separate.

UDFs stay resolvable late in the evaluator by design, so `DROP FUNCTION`
behaves as before; the bind-time check only rejects a name unknown to both.

**What makes this Partial.** Still accepted and ignored, and NOT addressed
(each re-verified by the probe in section 5, not carried over from an
earlier reading):

- `HAVING` with no `GROUP BY` - accepted.
- `LIMIT` / `OFFSET` - accepted. Whether `LIMIT` is global or per-subtask
  is still undetermined; answering it needs a runtime experiment at
  parallelism > 1, and guessing would be worse than leaving it recorded.
- `FOR UPDATE` - accepted, and meaningless on a stream.

---

### W15 - Coordinator fencing — Partial

**Source:** `include/clink/cluster/protocol.hpp`,
`include/clink/cluster/messages.hpp`, `include/clink/cluster/coordinator.hpp`,
`src/cluster/coordinator.cpp`, `include/clink/cluster/worker.hpp`,
`src/cluster/worker.cpp`, `tools/clink_node.cpp`

**The finding (F16).** `HaCoordinator` already computed a monotonic epoch,
bumped on every leadership acquisition, and its own header said: "v1
doesn't yet propagate epoch into the wire protocol, but the field is in
place for when it does." A grep confirmed the epoch was read by exactly one
thing - the leader-endpoint file - and by nothing else in the engine. There
was no fencing anywhere.

That matters because losing leadership is not something a leader can
detect on its own. A coordinator partitioned from the coordination store,
or merely paused past its lease, keeps every worker connection open, keeps
its in-memory job state, and keeps its checkpoint timer running. In that
state it could:

- deploy a job the new leader had also deployed, running two copies;
- cancel a job the new leader had just started;
- issue `TriggerCheckpoint` numbered from its own stale counter into the
  same checkpoint directory, so two different coordinators write two
  different "checkpoint 7";
- broadcast `CommitCheckpoint`, which publishes 2PC sink transactions
  externally - the one action in the list that cannot be undone;
- issue `BeginRescale`, redistributing state under the new leader.

**What was implemented.** The epoch now rides the wire on all nine
coordinator-to-worker control frames: `RegisterAck`, `Deploy`,
`PeerUpdate`, `CancelJob`, `TriggerCheckpoint`, `CommitCheckpoint`,
`AbortCheckpoint`, `FinalCheckpointAssigned`, `BeginRescale`.
`clink_node` binds it via `Coordinator::set_epoch` inside the
become-leader callback, before the listener opens, so no frame this leader
ever sends is unstamped.

A worker binds the epoch carried by the `RegisterAck` that admitted it,
and `Worker::accept_epoch_` drops any later frame carrying a lower one,
counting it on `clink_worker_fenced_frames_total` and logging it at error.
A HIGHER epoch re-binds rather than being refused: a failover that keeps
the connection up presents that way, and refusing it would fence the worker
off from the legitimate new leader - the same outage as split brain and
harder to diagnose.

**A mistake worth recording.** The first cut assigned the epoch at each
message-construction site. That was already wrong when written: four
separate paths build a `DeployMsg` - submit, two rescale paths, and
restart-after-failure - and only the submit one had been stamped. The
failure mode is quiet and the wrong way round: an unstamped frame carries
epoch 0, a worker bound to a real epoch REFUSES it, so forgetting to stamp
presents as a rescale or a restart that silently never happens. The
behavioural tests could not catch it either, because they drive the worker
directly rather than through the coordinator's send paths.

The fix is structural rather than another assignment: every
coordinator-to-worker frame is now encoded through `Coordinator::
fenced_frame_`, which stamps as it encodes, and no send site sets the field
itself. `AnEpochedCoordinatorDoesNotFenceOffItsOwnDeploy` and
`AnEpochedCoordinatorCanStillRestartAFailedTask` run a real coordinator at
a non-zero epoch and assert its own worker never fences it, so a future
site that bypasses the helper fails a test rather than going quiet.

**Compatibility.** The field is appended at the tail of each body and read
with the additive idiom the rest of this protocol already uses
(`r.eof() ? 0 : r.read_u64_be()`). Zero means "unfenced" and reproduces the
previous behaviour exactly, so a non-HA cluster is untouched and a
mixed-version cluster keeps working while it is rolled. This is pinned by
`AFrameFromAPreFencingPeerDecodesAsUnfenced`, which truncates the tail off
a real encoded frame - what a previous-build node actually puts on the
wire - and asserts the rest still decodes.

**Metadata guard.** The job manifest now carries `"coordinator_epoch"`, and
`fenced_write_file` refuses to overwrite a record stamped by a LATER epoch.
The rule is `metadata_write_allowed`, exposed in the header alongside the
other submit-time policies so it can be tested directly.

**Tests:** `tests/test_coordinator_fencing.cpp` (12 cases),
`tests/test_wire_protocol.cpp` (3 fencing cases),
`tests/test_ha_coordinator.cpp` (1 new case, epoch monotonicity),
`tests/integration/test_ha_failover.cpp` (5 multi-process cases).

The behavioural tests drive the worker through `set_connect_factory`, the
transport seam `Worker` already exposes, so each frame kind is delivered at
a chosen epoch through the real decoder and the real dispatch switch. Two
live coordinators could not produce this: a coordinator stamps one epoch at
a time. Every frame kind is checked in both directions - refused when
stale, accepted at the bound epoch - because a test that only asserts
"refused" passes just as well if the frames were never being processed.

The coverage was verified by mutation rather than assumed: deleting the
check from `handle_trigger_checkpoint_` and rebuilding made
`EveryControlFrameFromASupersededCoordinatorIsRefused` fail and name the
handler ("TriggerCheckpoint from a superseded coordinator was NOT
refused"). A per-handler check that no test can distinguish from its
absence is not covered, whatever the line count says.

**Multi-process failover.** The unit tests pin the rule against epochs a
test hands the worker. `tests/integration/test_ha_failover.cpp` pins it
against epochs the system produces: two `clink_node` coordinator processes
contend for one `fcntl` lock, the leader is SIGKILLed, and the standby
takes over. It checks both directions, which fail in opposite ways:

- the epoch must ADVANCE across the failover, because a reused epoch fences
  nothing and every unit test would still pass;
- nothing legitimate must be fenced. A worker that registers with the new
  leader binds the new epoch and refuses nothing, and a job submitted to it
  deploys, checkpoints and completes. Adding fencing to a control plane can
  break the failover it exists to protect, and that failure is silent.

The harness gained `start_ha_coordinators`, `kill_leader_and_await_failover`
and `start_ha_worker` for this. As with the rest of that harness, every step
waits on an observed condition - a leader announced in a log, a port
accepting, a registration counted - with a deadline as a failure bound only.

**What makes this Partial - stated plainly:**

- **The epoch is carried through the leader record, not a consensus
  counter.** A new leader reads the previous one's published epoch and goes
  above it. If that record is lost - the HA directory wiped, the etcd key
  expired and garbage-collected before the successor reads it - the
  successor restarts from 1 and a displaced leader stamping a higher epoch
  would fence the legitimate new leader off. The window is narrow and the
  precondition is destructive, but it is a real ordering assumption rather
  than a guarantee.
- **No test produces a genuine split brain.** The failover tests kill the
  old leader, so it is not alive to send anything. A real split brain needs
  the displaced leader to keep its sockets while losing the lock, and the
  file coordinator cannot produce that: the `fcntl` lock is released only on
  process death, so a SIGSTOPped leader keeps it and no standby takes over.
  It needs a lease-based store (etcd), and would be build-gated on it. What
  is demonstrated is that the rule holds given the epochs, and that the
  election produces advancing epochs - not the two together under partition.
- **The metadata guard is a read-then-write, not a compare-and-set.** Two
  writers racing inside the window between the read and the rename can both
  pass. It closes the realistic shape of the problem, where a partitioned
  leader is stale for seconds or minutes and every write it attempts reads
  back an epoch above its own, but it is not a distributed CAS and must not
  be described as one. A POSIX filesystem offers no primitive that would
  close the remaining window.
- **No object-store or etcd metadata backend with a real conditional
  write.** That is what would make the above airtight, and it was not
  built. The brief asked for it; this is the part that is missing.
- **Fencing is coordinator-to-worker only.** Worker-to-coordinator frames
  carry no epoch, so a stale coordinator can still receive and act on
  status from workers it no longer owns. It cannot make them do anything,
  which is the dangerous direction, but its view is not fenced.

---

### W16 - Protocol version negotiation — Partial

**Source:** `include/clink/cluster/protocol.hpp`,
`include/clink/cluster/messages.hpp`,
`include/clink/cluster/client_handshake.hpp`, `src/cluster/coordinator.cpp`,
`src/cluster/worker.cpp`, `include/clink/state/snapshot_arrow_writer.hpp`,
`src/state/*.cpp`

**Two mechanisms, deliberately kept separate.** Additive tails already
handle a message gaining a field, and they must keep handling it without
negotiation: bumping a version for an additive change would refuse the
rolling upgrade the idiom exists to allow. `kClusterProtocolVersion` is for
everything additive tails cannot express - a field changing meaning or
width, a kind repurposed, a semantic contract changed under an unchanged
encoding. The header says which is which, because getting that wrong in
either direction is costly: bump too eagerly and every upgrade is an
outage, bump too rarely and the version means nothing.

**The rule is symmetric.** Each side declares `protocol_version` and
`min_compatible_protocol_version`; `check_protocol_compatibility` accepts
only when each is inside the other's range. "Can the coordinator read the
worker?" is a different question from "can the worker read the
coordinator?", and a one-sided check admits a pairing that half-works -
which surfaces later as a decode failure far from the cause. The
coordinator checks at `Register` and at `HelloClient`; the worker checks
the `RegisterAck`.

**Zero means version 1.** A peer built before this change sends no fields,
so they decode as 0. Reading that as "invalid" would fence off every node
that had not restarted yet, turning the deployment of a compatibility
feature into an outage. `ARealCoordinatorAdmitsAPeerThatDeclaresNothing`
sends the exact truncated frame an older build puts on the wire.

**The refusal is legible.** A refused worker gets a `RegisterAck` nack
naming both versions and which end to upgrade. A refused client gets a
`SubmitJobAck`, which is right for `clink submit` and wrong for every other
tool - so `protocol_rejection_message` lets those report the reason instead
of "unexpected reply kind 12". Refusals increment
`clink_protocol_mismatches_total`.

**Snapshot format version, now enforced.** `verify_snapshot_format_version`
is called at all six read sites (three in `InMemoryStateBackend`, plus
changelog restore, state migration, and canonicalisation). Absence is
version 1 permanently, because pre-marker streams are valid. The parse is
strict - every character a digit - because `std::stoul` alone skips leading
whitespace and stops at the first non-digit, so `" 1"` and `"1.0"` would
both have read as 1 and let a stream clink never wrote past the gate.

**Tests:** `tests/test_protocol_versioning.cpp`, 17 cases.

Four of them go through a real coordinator or a real restore rather than
calling the predicate, because the predicate was never the risk - a gate
nothing calls is exactly the defect F20 describes, and it can be
reintroduced one call site at a time. Both were mutation-checked: deleting
the coordinator's negotiation makes
`ARealCoordinatorRefusesAWorkerItCannotSpeakTo` report "a worker from an
unsupported protocol version was admitted", and deleting the gate from
`InMemoryStateBackend::restore` makes `ARestoreActuallyRunsTheGate` report
"a snapshot from a newer format was restored as though it were the current
one". The unit-level cases alone survive both.

**What makes this Partial - stated plainly:**

- **The version is declared, not exercised.** Everything is at version 1,
  so no test has run two genuinely incompatible builds against each other.
  What is verified is that the mechanism refuses the pairings it should and
  admits the ones it should, using synthetic version numbers. The first
  real bump will be the first real test.
- **Only the handshake is checked.** Per-message versioning does not exist:
  a frame is trusted once the connection is admitted. That is the right
  trade for a control plane where both ends are pinned at connect time, but
  it means a version bump can only ever be enforced connection-wide.
- **The data plane is not covered.** Arrow IPC between operators carries no
  clink version; it relies on Arrow's own format compatibility. A change to
  clink's sidecar conventions would not be caught.
- **Plugin ABI is a separate mechanism and stays that way.** `.so` loading
  gates on a build hash (`cluster_abi_hash`), which is stricter than a
  version and correct for shared objects. It is not unified with this, and
  should not be.
- **No CLI surface reports the negotiated version.** An operator diagnosing
  a mixed cluster reads it out of a refusal message or the coordinator log,
  not from `clink list` or `--capabilities-json`.

---

### W14 - Resource and overload limits — Partial

**Source:** `include/clink/cluster/frame_io.hpp` (new),
`include/clink/cluster/protocol.hpp` (`MessageReader::read_count`),
`include/clink/cluster/messages.hpp`, `src/cluster/coordinator.cpp`,
`src/cluster/worker.cpp`, `src/application/job_submitter.cpp`

**How these were found.** Not by reading the code looking for limits, but
by working out what to fuzz. Listing the places that parse bytes from a
peer led straight to the length prefix, and from there to the element
counts and then to the question of who catches what the decoders throw.
The answer to the last one was nobody.

**Frame size.** One shared `read_frame`, replacing three copies. It caps a
frame at `kMaxFrameBytes` (256 MiB - well clear of a plugin-carrying
Deploy, well short of nonsense) and reads the body in 64 KiB chunks so
memory tracks what the peer has actually sent. The cap alone would not have
been enough: four bytes claiming 256 MiB would still have allocated 256 MiB
up front, so the amplification would have survived at a smaller constant.
Chunking removes it - a byte costs a byte.

**Connection lifetime and count (F27).** Client sessions are now one
vector of `{connection, thread, finished}` rather than two parallel ones,
reaped on each new accept, and capped by
`Config::max_client_connections` (default 256). The cost is now per
CONCURRENT client rather than per client ever seen. A refused client gets
a `SubmitJobAck` saying so rather than a silent close.

The reaping is what makes the cap meaningful: a cap over a list that never
shrinks is a lifetime quota, not a concurrency limit. `ClientConnections
AreReapedRatherThanAccumulated` asserts on the retained count rather than
on survival, because nothing crashed while the leak was live - that is why
it lasted. Mutation-checked: disabling the reap leaves 40 sessions after
40 connect/disconnect cycles, and the test names the number.

**Element counts.** `MessageReader::read_count` replaces `read_u32_be` at
all eleven container sites. The bound needs no arbitrary constant: every
element costs at least one byte on the wire, so a count above the bytes
remaining in the frame cannot be honest whatever the element type.

**Exception boundaries.** Each of the four frame-handling loops now wraps
exactly one frame. A malformed frame costs the peer its connection, is
logged, and increments `clink_malformed_frames_total`. Dispatch was
extracted into `dispatch_client_frame_`, `dispatch_worker_frame_` and
`Worker::dispatch_control_frame_` so the boundary is around a function
call rather than smeared through a loop body.

**Tests:** `tests/test_frame_robustness.cpp`, 14 cases.

Two of them are property tests rather than examples: every decoder against
400 seeded random payloads each, and three valid frames truncated at every
byte offset. The property is narrow and total - for any byte string, a
decoder returns or throws `std::exception`; it never aborts and never
allocates from a number it was handed. A fixed seed makes a failure
reproduce exactly, and it runs in the normal suite rather than needing a
fuzzing engine and an unbounded time budget.

The two that matter most assert survival rather than rejection: after being
sent four kinds of garbage, the coordinator must still register a real
worker. Verified by mutation - removing the accept-loop boundary does not
make the test fail, it terminates the test binary, which is precisely the
production failure.

**What makes this Partial - stated plainly:**

- **Frame RATE is still unbounded.** A peer holding one connection and
  sending well-formed frames as fast as it can is not throttled. Neither
  is the number of in-flight jobs.
- **No backpressure or admission control on the control plane.** Submit is
  synchronous and unthrottled.
- **Worker connections are not capped.** Only client connections are. A
  worker connection is a registration, which is bounded by the cluster's
  own size in any sane deployment - but "in any sane deployment" is the
  same reasoning that left client connections unbounded.
- **The cap is a constant, not configuration.** An operator shipping a
  plugin above 256 MiB has to rebuild rather than set a flag.
- **No fuzzing engine.** The property tests are deterministic and bounded,
  which makes them gateable; they are not a substitute for libFuzzer over
  a corpus, and no such target exists (W23 remains open).
- **Only the control plane.** The data-plane Arrow IPC path was not
  audited for the same class of defect.

---

### W21 - Cancellation and shutdown — Partial

**Source:** `src/cluster/worker.cpp` (`Worker::stop`),
`tests/integration/test_graceful_shutdown.cpp` (new)

`Worker::stop()` now flips every registered per-subtask cancel token before
joining - the same flip `CancelJob` performs. Cancelling on shutdown is
correct rather than merely expedient: the subtasks are going away with the
process either way, and a cancelled subtask runs its normal teardown, so
sinks abort their pending transactions instead of leaving them for recovery
to resolve.

The joins stay unbounded, deliberately. Once cancellation has been
signalled, a subtask that still will not exit is a bug in that operator,
and cutting the join short would detach a thread still touching the
Worker's members as it is destroyed. A hang is a loud symptom with a stack
to look at; a use-after-free is neither.

**Tests:** 3 multi-process cases - a worker with a running job, a
coordinator with a running job, and a repeated SIGTERM. Each brings up a
real cluster, submits a long-running job, waits for it to actually be
checkpointing, and then signals. The 15 s bound is chosen to sit inside a
realistic Kubernetes `terminationGracePeriodSeconds`, so "passes the test"
and "survives a real rollout" are the same claim.

**What makes this Partial - stated plainly:**

- **A `register_role` handler still cannot be cancelled.** It receives no
  token, and there is no way to interrupt an arbitrary callback. That path
  is the in-process test API rather than how `clink_node` runs work, but a
  test that blocks in one will still hang `stop()`.
- **Shutdown is cancellation, not draining.** Nothing attempts a final
  checkpoint or a clean source drain on SIGTERM; in-flight records since
  the last checkpoint are replayed on restart. That is consistent with the
  at-least-once story but it is not a graceful drain, and a "stop with
  savepoint" path does not exist.
- **Only SIGTERM at the process level was audited.** Cancellation of an
  individual job mid-checkpoint, mid-rescale, or during 2PC commit was not
  systematically exercised; nor was the HTTP server's shutdown, nor
  connector-level cancellation (a source blocked in a broker poll).
- ~~No leak check.~~ **Partly closed.** `tests/test_shutdown_leaks.cpp`
  cycles a coordinator through start/stop eight times and asserts that open
  descriptors and live threads come back to where they started. It matters
  for the long-lived processes specifically: a coordinator leaking two
  descriptors per stopped job is invisible for a day and then hits the
  process limit, surfacing as an unrelated `accept()` failure. Nothing in
  the suite would have caught that, because every other test starts a
  cluster, asserts a behaviour, and lets the process exit take the evidence
  with it.

  Cycles rather than one start/stop, because a single iteration cannot tell
  a leak from a fixture that is legitimately kept once - a lazily-created
  log sink, a cached handle. A leak scales with the cycle count; a fixture
  does not. A warm-up cycle runs before the baseline for the same reason.
  Mutation-checked by leaking one descriptor per cycle: `4 -> 12`, caught.

  Still open: only the coordinator, and only descriptors and threads.
  Workers, temporary files and the HTTP server are not covered, and neither
  is memory - a leak of heap rather than handles would pass all three cases.

---

### W22 - Side-output propagation — Partial

**Source:** `tests/test_side_output.cpp` (one new case),
`include/clink/connectors/delivery_guarantee.hpp`,
`src/connectors/delivery_guarantee.cpp`, `src/cluster/guarantee_gate.cpp`,
`tests/test_connector_capability.cpp` (five new cases)

**No defect found, and that is the finding.** Side outputs already had four
tests, all about where DATA goes. None asserted that the CONTROL stream -
checkpoint barriers and watermarks - reaches a side-output sink, which is
the property a side branch actually depends on: a 2PC sink there needs the
barrier to stage and the commit to publish, and an event-time operator
needs the watermark to fire. Had that been broken, every existing test
would still have passed and the branch would simply never have checkpointed.

`BarriersAndWatermarksReachASideOutputSinkToo` drives a source that emits a
barrier, then a watermark, then data that splits across both branches, and
asserts both sinks saw all three. It passed first time: the propagation is
correct. It is now pinned rather than incidentally true.

The test asserts on the MAIN branch first. Without that, a version of the
test where nothing reached either sink would report a side-output bug that
did not exist.

**Multi-sink atomicity (F26).** The other half of P2.18. Two 2PC sinks with
no shared `commit_group` were reported as `END_TO_END_EXACTLY_ONCE` full
stop, with nothing said about whether the two outputs could disagree. The
analyser now warns when a pipeline has more than one transactional sink,
naming them and stating the exposure.

A warning rather than a downgrade or a rejection, deliberately. For many
jobs two independent outputs are exactly what was wanted and their mutual
consistency is not a property anyone needs; refusing those would be wrong,
and downgrading the level would misreport in the opposite direction, since
each sink IS exactly-once. What must not happen is reporting end-to-end
exactly-once and leaving the reader to discover it means per sink.

The first version of that warning said the sinks "commit INDEPENDENTLY"
unless they shared a `commit_group`, and prescribed setting one. Testing the
claim showed both halves were wrong - see **F35**, which is what closed the
gap this section used to record.

**What makes this Partial - stated plainly:**

- **One property, one topology, for side outputs.** A single operator with
  one side output, in-process. Not covered: side outputs across a network
  shuffle, more than one side output on an operator, or a side output from
  an operator inside a chain.
- **No failure interaction for side outputs.** The barrier reaching a side
  sink was tested on a healthy run. Whether a side branch recovers
  correctly when a worker is lost mid-checkpoint was not exercised.
- **Cross-sink atomicity is now tested, but only for two file sinks in one
  job.** F35's tests use two `file_2pc_sink_string` instances. Two sinks of
  DIFFERENT kinds (say Postgres and S3, whose commits fail in different
  ways and take different times) are not covered, and neither is more than
  two.
- **The commits are still not atomic with each other, by design.** F35
  establishes that a split is repaired at restart rather than prevented. A
  job that exhausts its restart budget keeps the disagreement. No test
  covers that specific end state.

---

### F28/F29 - Checkpoint completion and recovery — Done

**Source:** `include/clink/cluster/coordinator.hpp`,
`src/cluster/coordinator.cpp`, `tests/test_checkpoint_completion.cpp` (new)

F28 is fixed and covered. A checkpoint with any failed subtask ack is
failed, not completed: no `COMPLETED-N`, recovery point unchanged,
`AbortCheckpoint` broadcast to roll back anything staged.

**Tests:** 5 cases, driving a real coordinator over a real socket with the
test playing the worker. That shape is necessary rather than clever: the
defect is in what the coordinator concludes from a specific sequence of
acks, and nothing above the wire can produce `ok=false` on demand.

Getting the fake worker accepted took three corrections, each of which is
a fact about the coordinator worth having in one place:

- it must send heartbeats, or the watchdog declares it lost and kills the
  job before any checkpoint can be acked (fixed by heartbeating, not by
  turning the watchdog off);
- it must report `SubtaskListening` for every generic subtask, because
  periodic checkpointing is gated on `peer_updates_sent`;
- it must read frames on its own thread, because a deadline checked
  between blocking reads is never checked.

Two of the four cases are controls - the fixture really does produce
triggers, and an all-success checkpoint still completes. Without the
second, a guard written as "never complete" would pass the headline test
and leave checkpointing dead.

Mutation-checked: disabling the guard fails both positives with the
specific claim, and leaves both controls green.

**A defect introduced and caught during the fix.** The first version of
the guard erased `ckpt_it` in the failure branch and then re-tested
`ckpt_it->second.empty()` in the next condition - a use-after-free. The
emptiness is now read once, before either branch touches the map.

**F29 is fixed too, once it was established rather than suspected.** The
recovery test above is the establishing evidence and the regression guard
in one: it asserts on the restore point in the Deploy frame the recovering
coordinator sends, so a future change that puts the marker back out of
recovery's reach fails with the two ids side by side.

---

### W23 - Fuzz targets — Partial

**Source:** `fuzz/` (new: `fuzz_targets.hpp`, five entry points,
`generate_seeds.cpp`, `CMakeLists.txt`, `README.md`),
`tests/test_fuzz_corpus.cpp` (new), `scripts/fuzz.sh` (new),
`scripts/install-system-deps.sh`

**The design decision that matters: discovery and regression are split.**

Discovery needs a clang that ships libFuzzer and an unbounded time budget,
so it cannot be a required check - an unbounded search is not a gate, and
pretending otherwise buys either a flaky required job or a time limit so
short it finds nothing.

Regression replays every committed corpus input through the SAME functions
under plain gtest. No fuzzing engine, milliseconds, gates on every platform
and compiler the project builds on. So the workflow is: a campaign finds a
crash, the input is committed, and from then on it is a permanent
regression test that runs even on builds that could not run a fuzzer.

That last part is what the brief's "add a regression test for every bug
discovered" actually requires. A fuzzer alone does not provide it: nobody
reruns the exact input, and the next campaign starts from a different
random seed.

**Targets** are the places that parse bytes clink did not write:
control-plane message bodies (from an unauthenticated peer), the checkpoint
integrity sidecar (from disk, so also whatever survived a partial write),
the packed schema-version map, the `CLINK_FAULT_INJECT` schedule, and SQL
text. `cluster_frame` takes the message kind from byte 0, so one corpus
entry mutates into any decoder rather than only the one it first reached.

**Seeds are generated, not committed.** `clink_fuzz_seeds` writes them from
the real encoders at build time. A hand-written seed goes stale the first
time a message gains a field, and a stale seed narrows what the fuzzer
explores without anyone noticing - the same reasoning that has the
guarantee analyser read the capability registry rather than a literal list.
Reproducers ARE committed bytes, because their whole value is being the
exact input that broke something.

**Results of the first campaign** (macOS/arm64, `-fsanitize=fuzzer,address,
undefined`, 45 s per target):

| Target | Executions | Findings |
|---|---|---|
| `cluster_frame` | 1,408,440 | none |
| `checkpoint_meta` | 12,882,991 | none |
| `state_version_map` | 6,559,848 | none |

20.8 million executions, no crashes. Stated precisely: the W14 hardening
holds against inputs nobody wrote down. It does not say the decoders are
correct - a fuzzer finds crashes, not wrong answers.

**A toolchain gap found and declared.** The project's Debian image has
`clang-tidy`, which pulls the clang COMPILER but not the sanitizer
runtimes, so `-fsanitize=fuzzer` failed at link with a missing
`libclang_rt.fuzzer.a`. `libclang-rt-19-dev` is now declared in
`install-system-deps.sh`, so the next image rebuild can run these. Until
that rebuild, discovery runs on the host only - and
`CLINK_BUILD_FUZZERS=ON` on a toolchain that cannot link libFuzzer is a
hard CMake error naming the fix, not a silent skip.

**What makes this Partial - stated plainly:**

- **No sustained campaign.** 45 seconds per target is a smoke test. A real
  campaign is hours per target, ideally continuous, and would very likely
  find things this did not.
- **Discovery is not in CI at all.** It cannot be until the image is
  rebuilt with the runtime, and even then it belongs in a scheduled job
  rather than a required check. Only the corpus replay gates today.
- **No coverage corpus is published.** Discovered inputs are kept locally
  and deliberately untracked: a minimised `cluster_frame` corpus alone is
  ~1.2 MB of unreviewable blobs, and the marginal coverage over
  `test_frame_robustness.cpp`'s deterministic property tests is modest.
  The cost is that every campaign restarts from seeds.
- **Crashes only.** These targets assert "does not crash". They do not
  check that a decode round-trips, or that two paths agree - differential
  and property fuzzing would catch wrong answers, and neither exists.
- **The data plane is not fuzzed.** Arrow IPC between operators, and the
  connector wire formats, take input from outside the process and have no
  targets.
- **`state.during_flush` is declared and wired nowhere.** Noticed while
  listing fault points for a target; recorded here rather than fixed.

---

### W17 - Config linter and recovery profiles — Partial

**Source:** `include/clink/cluster/config_lint.hpp`,
`src/cluster/config_lint.cpp`, wired into `Coordinator::submit_job`,
`tests/test_config_lint.cpp` (22 cases), `tools/clink_submit_job.cpp`, `tools/clink_node.cpp`

**Every check cites a line of engine code, not a preference.** That
constraint did the design work: it is what kept "your checkpoint interval
is quite short" out of the file and kept "your checkpoint interval will
never fire, because the trigger loop skips a job with no directory" in. A
linter assembled from taste produces the `mode='cdc'` failure - it refuses
things that work, and people learn to switch it off, at which point the
true positives are worth nothing.

Errors reject the submission; warnings are logged and let it through. The
split is not cosmetic: a checkpoint directory with no interval is normal
for a bounded job and must submit, while an interval with no directory
cannot do what it says and must not.

**The negatives are the expensive half of the tests and the point of
them.** Every check has both a configuration it must refuse and the nearby
one it must accept: `kRestartAuto` (the unset sentinel) must not be flagged
where an explicit `3` is; an explicit `0` means fail-fast and is honoured;
five durable backend URIs must pass where `memory://` does not. Two
assertions exist purely to catch a linter that has turned on its own
product - the shipped liveness defaults (500 ms / 2000 ms / 100 ms) must
not even warn, and neither must a coherent baseline config.

The strongest evidence is negative and came free: the gate runs at every
submission across the whole suite, and 3505 tests pass. A false positive
anywhere in the cluster or SQL submission paths would have shown up as a
failure rather than as a judgement call.

**Profiles** are `development` and `production`. A profile fills in only
what the submitter left alone - an explicit `interval_ms = 0` means "no
periodic checkpoints" and survives, because quietly rewriting a flag
someone set is the same failure as ignoring it. `production` REFUSES what
it cannot deliver (no checkpoint directory, or a memory backend) rather
than downgrading, because the name is the request; a silent downgrade
leaves an operator believing they have guarantees they do not.
`development` deliberately changes nothing: it exists so a submission can
SAY it wants no recovery rather than arriving there by omission.

**Mutation-checked.** Disabling the gate in `submit_job` fails
`ARealSubmissionIsRejectedForAnIncoherentConfig`; the predicate-level tests
alone survive it. That is the snapshot-format-version lesson applied
without having to relearn it - a gate tested only through its predicate is
not proven to be wired.

**Three gaps recorded in earlier drafts of this section are now closed,**
because "the mechanism exists and nothing calls it" is the exact pattern this
document criticises elsewhere and it would have been indefensible to leave:

- `--profile=development|production` is wired into `clink_submit_job`. It
  applies the profile's defaults, runs both `lint_profile` and the general
  checks, prints every problem, and refuses on any error before opening a
  connection. Using the profile flag for the first time is what found F32.
- `lint_liveness_config` is called at coordinator startup in `clink_node`,
  warning rather than refusing: an operator may have a reason, and
  declining to start a coordinator over a heartbeat ratio would be a worse
  failure than the one being warned about.
- **`clink lint` exists** (`tools/clink_lint.cpp`, 10 cases in
  `tests/integration/test_cli_lint.cpp`), so a configuration can be checked
  without submitting a job. It shares flag parsing and config assembly with
  `clink run` via `tools/cli_config_args.hpp` so the two cannot reach
  different verdicts, and exits 1 on anything submission would refuse.

  Building it found **F36**: five of the linter's checks could not fire
  through the CLI at all, because `clink run` discarded the flags they test
  before the linter saw them. The judgement in the original entry - "a
  convenience rather than a hole" - was wrong, and wrong in the direction
  that matters.

**What makes this Partial - stated plainly:**

- **Job-graph settings are not linted.** Only `CheckpointConfig`.
  Per-operator parallelism against slot capacity, key-group counts against
  parallelism, and TTL against checkpoint interval are all unchecked.
- **No cross-check against the guarantee analyser.** The two gates run in
  sequence and could in principle disagree; nothing asserts they cannot.
- **`clink lint` reads flags, not deployed configuration.** It checks a
  command line. A Helm values file or a running coordinator's settings have
  to be turned into flags by hand first, so drift between what was linted
  and what is deployed is still possible.

---

### W19 - Production metrics — Partial

**Source:** `include/clink/metrics/checkpoint_metrics.hpp`,
`include/clink/metrics/orchestration_metrics.hpp`,
`src/cluster/coordinator.cpp`, `tests/test_checkpoint_metrics.cpp`,
`tests/test_config_lint.cpp`

Three new series, each answering a question that previously had no answer:
`clink_ckpt_last_completed_unix_seconds`,
`clink_coordinator_job_restarts_total`,
`clink_coordinator_subtask_redeploys_total`.

**The audit that found nothing is part of the result.** All 84 declared
metric constants are genuinely emitted, and none is pre-registered without
ever being updated. The item's premise - that the metrics surface is
incomplete - held only for what is absent, not for what is broken.

**Tests:** 4 new cases. The staleness gauge is asserted to exist BEFORE any
checkpoint completes, because an alert of the form
`time() - metric > N` has no series to evaluate on a fresh coordinator and
silently does not fire - which is the window where a job that never
checkpoints most needs it. Its value is range-checked as a plausible epoch
SECOND, since a unit mistake would make the alert quietly wrong rather than
absent. And the counter and the timestamp are asserted independent, so a
caller cannot stamp a completion it did not have.

The redeploy counter is driven through a REAL retry - a role that fails its
first attempt - rather than by calling the helper, for the same reason the
config gate needed a real submission.

**What makes this Partial - stated plainly:**

- **The whole-job restart counter has no test.** It is instrumented at the
  single site where that restart is decided and logged, and the
  integration suite exercises that path, but nothing asserts the counter
  moves. Reaching it in-process needs a failing generic subtask, which the
  role-handler path cannot produce. Recorded rather than manufactured.
- **Watermark lag is derivable, not exposed.** `clink_op_watermark_ms` is
  an absolute value per operator; lag needs `time() - metric/1000` at query
  time. That works in Prometheus and is worth stating rather than adding a
  second series that can disagree with the first.
- **No dashboard or runbook ships.** Alert RULES now do
  (`deploy/prometheus/clink-alerts.yaml`, 11 rules over checkpointing,
  cluster health and disk), but a Grafana dashboard and a written response
  procedure do not, so an operator still assembles those.

  Shipping rules is the kind of change that can be worse than not shipping
  them: a rule whose metric has been renamed does not error, it evaluates
  against no series, never fires, and leaves the operator believing they are
  covered. `tests/test_alert_rules.cpp` therefore checks every `clink_*`
  token in the file - including the ones inside annotation prose - against
  the metric CONSTANTS, so a rename fails the build either by assertion or
  by compile error. Two further cases pin the reasoning rather than the
  text: the staleness alert must use the completion timestamp gauge and must
  not use `rate()` on the completion counter, which cannot tell a stalled
  job from an idle one. Mutation-checked both ways.
- **Nothing measures state size in bytes at the job level.** `TtlStats`
  carries `estimated_bytes` per slot and the disaggregated tier reports
  resident bytes, but there is no per-job total, which is what capacity
  planning actually needs.
- ~~No histogram for checkpoint duration distribution.~~ **Closed.**
  `clink_ckpt_duration_ms` is a histogram with 12 millisecond buckets, so a
  p99 is available. The exposition is unchanged where it already existed:
  Prometheus renders a histogram's `_sum` and `_count` under exactly the
  names the two counters used, so existing queries keep working and
  `_bucket{le}` is added.

  That name equivalence is also the trap, and it has a test:
  reinstating the counters alongside the histogram would emit two
  `clink_ckpt_duration_ms_sum` lines, and Prometheus rejects a duplicated
  series - so the whole scrape endpoint fails, not just the one metric.
  `TheDurationHistogramExposesOneSumAndOneCountNotTwo` counts occurrences
  rather than checking presence, because a contains() assertion passes just
  as happily on a broken scrape.

---

### W8 (continued) - exactly-once, verified at the sink

**Source:** `tests/integration/test_fault_recovery.cpp` (4 new cases)

Every fault-recovery test written before this asserted that the job
COMPLETED and that checkpoints PROGRESSED. Neither says anything about the
output. A pipeline that duplicated every record after a restart, or silently
dropped the ones in flight, passes all of them - which is why this document
said for most of its life that exactly-once was not proven.

These read what an external consumer would actually see and compare it to
the exact multiset the source promises. The 2PC job emits `record-0`
through `record-39`, once each, and checkpoints its offset; the sink commits
by atomic rename from `staging/` into `committed/`. Only `committed/` is
read, because a file left in `staging/` is a transaction nobody agreed to.

Duplicates, losses and unexpected records are reported SEPARATELY. They are
different failures - a duplicate means the recovery re-published work
already committed (an at-least-once leak), a loss means nothing was
published for records the source had passed - and a single "mismatch" count
would hide which.

**The four scenarios:**

| | |
|---|---|
| clean run | the control: without it, a failure below could be the job, the sink, or the verifier |
| kill AFTER a completed checkpoint | recovery resumes from the checkpoint; output must be unchanged |
| kill BEFORE any checkpoint | recovery replays from zero, so exactly-once depends entirely on the sink having committed nothing - a commit only follows a completed checkpoint |
| uncommitted output invisible | staged-but-uncommitted work must not be readable, and the committed count can never exceed what the source emitted |

**Result: all four pass, three consecutive runs of the full 19-test
fault-tolerance suite.**

**Validated by mutation, which is the only reason the result means
anything.** Making the source forget its offset on restore - so a restart
replays from zero - produced exactly the failure the test is for:

```
records were committed MORE than once across the recovery, so the pipeline
is at-least-once rather than exactly-once: 42 committed lines;
2 DUPLICATED: record-0 x2, record-1 x2
```

That establishes three things at once: the kill really does cause a replay,
the verifier detects a duplicate and names it, and the unmutated engine
does not produce one. Without the mutation, "4 tests passed" would be
consistent with a test that reads an empty directory.

**What makes this Partial - stated plainly:**

- **Rescale is not covered.** Worker failure and coordinator failover are
  both verified by output equality now
  (`HaFailoverTest.ExactlyOnceSurvivesACoordinatorFailover`), and that
  extension is what found F34. A rescale mid-stream is not.
- **One sink.** The file 2PC sink. Kafka's transactional sink, the Postgres
  2PC sink and the S3 multipart sink each have their own commit
  choreography and none is verified this way.
- **One fault, at two moments.** SIGKILL of one worker, before or after a
  checkpoint. Not covered: a kill DURING the commit broadcast (the fault
  point exists), overlapping kills, a kill during state restore with output
  verification, or a rescale.
- **40 records over ~2 seconds.** Enough to catch a systematic duplication
  or loss, nowhere near enough to catch a rare race. A soak run at volume
  is the thing that would, and it does not exist.
- **Nothing verifies ORDER.** The comparison is a multiset. Per-key
  ordering across a recovery is not asserted.

---

### W24 - Widening the blocking integration gate — Partial

**Source:** `.github/workflows/ci.yml`

The integration suite was already built and run in CI. The problem was that
everything except `FaultRecovery` ran under `continue-on-error: true`, so
eleven red cases sat in a green pipeline. An advisory line is not a neutral
choice: it converts a failing test into a log nobody reads, and it did that
here for five distinct defects (F37, F38, two tests asserting removed
features, and a deadline shorter than the operation it waited for).

The gate now covers `FaultRecovery`, `CliLint`, `CommitGroupAtomicity` and
`UngroupedSinkAtomicity`. Each addition had to meet the bar this file already
set for `FaultRecovery` - waits on conditions rather than durations, passes
repeatedly - not merely be green today:

- `CliLint` stands up no cluster at all. It runs a binary and checks exit
  codes, so there is nothing to race.
- The two atomicity suites are written on the harness, and the one place
  they wait for quiescence polls until the committed set stops changing
  rather than sleeping for a guessed interval.

Validated by two consecutive clean runs of the gated set (29/29 each), and
then by the whole label: 108 of 109 pass, the one failure being F38, which is
knowingly red.

**What makes this Partial - stated plainly:**

- **`TwoPhaseCommit` and `WorkerCrashRecovery` pass now and are still NOT
  gated.** They use the pre-harness helpers, including a fixed 200ms sleep
  standing in for cluster startup. Promoting a test that waits for a
  duration is the gated-and-hoped move this document argues against
  everywhere else, and doing it because the test is currently green would be
  the same mistake in a better mood.
- **One case in the advisory set is knowingly RED.**
  `PluginSubmission.CheckpointAndRestoreAcrossJobRuns` fails on F38. It stays
  advisory and stays failing; the alternative is to assert that losing half a
  job's keyed state is correct.
- **The advisory remainder still has no shrinking deadline.** The mechanism
  for moving tests into the gate is proven now, but the ~100 remaining cases
  are converted one at a time and nothing schedules that work.
- **Nothing prevents a new test from landing in the advisory set.** The split
  is two ctest regexes; a test added tomorrow is advisory by default, which
  is the wrong default and is the reason this whole class of failure went
  unnoticed.

---

## 5. Test evidence

Commands run, on macOS 26.3 (arm64), Apple clang, `RelWithDebInfo`.

```
# Configure + build
cmake -S . -B build && cmake --build build --parallel 10

# New unit suites
./build/tests/clink_core_tests --gtest_filter='FaultInjection*'
    -> 17 tests from 1 test suite ran. [  PASSED  ] 17 tests.
./build/tests/clink_core_tests --gtest_filter='CheckpointIntegrity*'
    -> 23 tests from 1 test suite ran. [  PASSED  ] 23 tests.
./build/tests/clink_core_tests --gtest_filter='ConnectorCapability*'
    -> 20 tests from 1 test suite ran. [  PASSED  ] 20 tests.
./build/tests/clink_core_tests --gtest_filter='StateBackendFactory*'
    -> 13 tests from 1 test suite ran. [  PASSED  ] 13 tests.
./build/tests/clink_core_tests --gtest_filter='CoordinatorFencing*:WireProtocolFencing*'
    -> 15 tests from 2 test suites ran. [  PASSED  ] 15 tests.
./build/tests/clink_core_tests --gtest_filter='ProtocolVersioning*:SnapshotFormatVersion*'
    -> 17 tests from 2 test suites ran. [  PASSED  ] 17 tests.
./build/tests/clink_core_tests --gtest_filter='FrameRobustness*'
    -> 14 tests from 1 test suite ran. [  PASSED  ] 14 tests.
./build/tests/clink_core_tests --gtest_filter='FuzzCorpus*'
    -> 3 tests from 1 test suite ran. [  PASSED  ] 3 tests. (5 ms)

# Fuzz discovery (host only until the image carries libclang-rt)
scripts/fuzz.sh cluster_frame 45      -> 1,408,440 execs, no findings
scripts/fuzz.sh checkpoint_meta 45    -> 12,882,991 execs, no findings
scripts/fuzz.sh state_version_map 45  -> 6,559,848 execs, no findings

# Whole suite, after the changes
cmake -S . -B build -DCLINK_BUILD_SQL=ON && cmake --build build --parallel 10
ctest --test-dir build -j8
    -> 100% tests passed, 0 tests failed out of 3432

# CLI, end to end
./build/clink --capabilities-json | python3 -c "import json,sys; json.load(sys.stdin)"
    -> parses; build facts and 6 core connectors present
./build/clink checkpoint-verify --dir <legacy-dir>          -> exit 1, "incomplete"
./build/clink checkpoint-verify --dir <legacy-dir> --repair -> exit 0, "valid"
<flip one byte>
./build/clink checkpoint-verify --dir <legacy-dir> --json   -> exit 1, "corrupt", checksum mismatch
```

Integration suite:

```
cmake -S . -B build-it -DCLINK_INTEGRATION_TESTS=ON -DCLINK_BUILD_SQL=ON
cmake --build build-it --parallel 10
ctest --test-dir build-it --parallel 1 --timeout 300 -R FaultRecovery
    -> 100% tests passed, 0 tests failed out of 7   (x4 consecutive runs)
    -> 73 s wall for the suite; 1 DISABLED (F12, see section 7)

./build-it/tests/clink_integration_tests --gtest_filter='HaFailoverTest.*'
    -> 5 tests from 1 test suite ran. [  PASSED  ] 5 tests. (12 s)

./build-it/tests/clink_integration_tests \
  --gtest_filter='FaultRecoveryTest.*:HaFailoverTest.*:GracefulShutdownTest.*'
    -> 19 tests. [  PASSED  ] 19 tests.  (x3 consecutive runs)

# Exactly-once, validated by mutation: source forgets its offset on restore
./build-it/tests/clink_integration_tests \
  --gtest_filter='FaultRecoveryTest.EveryRecordIsCommitted*'
    -> FAILED: "42 committed lines; 2 DUPLICATED: record-0 x2, record-1 x2"
       (restored; passes)

./build-it/tests/clink_integration_tests --gtest_filter='GracefulShutdownTest.*'
    -> before the fix: AWorkerRunningAJobExitsOnSigterm FAILED after 17.7 s
       ("did not exit within 15s of SIGTERM")
    -> after:          3 tests. [  PASSED  ] 3 tests; the worker exits in 2.1 s
```

The first run of that HA suite is what found F18: `FailoverAdvancesTheEpoch`
failed with both coordinators announcing `epoch=1`.

Cross-process fault arming, verified end to end:

```
CLINK_FAULT_INJECT="checkpoint.before_write=exit:70@1" \
  ./build/clink checkpoint-verify --dir <d> --repair
    -> exit 70; no sidecar written
(without the variable: exit 0; sidecar written)
```

**The fault-framework race (F17).** Reproduced locally before fixing:

```
# With the epoch baseline read inside the Block case (the old shape)
./build/tests/clink_core_tests --gtest_filter='FaultInjectionTest.*Parking'
    -> hangs indefinitely; the worker thread never leaves the fault point

# With the baseline captured under the matching lock (the fix)
./build/tests/clink_core_tests --gtest_filter='FaultInjection*'
    -> 20 tests from 1 test suite ran. [  PASSED  ] 20 tests. (299 ms)
```

1000 race iterations in under 300 ms against an indefinite hang. The Linux
CI symptom was a `(Timeout)` on `ResetReleasesAParkedThread`, which is the
same deadlock arrived at by luck rather than by hammering.

**The SQL semantics probe.** Each remaining W13 item was re-established by
running it rather than by reading the parser, through `clink run` on a
two-table script:

```
ACCEPTED  HAVING with no GROUP BY
ACCEPTED  LIMIT
ACCEPTED  OFFSET
ACCEPTED  FOR UPDATE
ACCEPTED  RANDOM()                 <- fixed; now a compile error
rejected  NOW()                    -- now() is not supported: clink keeps SQL deterministic
rejected  unknown connector        -- format='json' source requires connector='file', 'kafka', ...
rejected  no connector at all      -- table t missing required property: connector
```

`RANDOM()` was the entry point, but running it showed the failure was
neither about randomness nor confined to it: it planned, deployed and then
threw `json_value_expr: unknown op 'random'` from the projection operator
on the first record. `NO_SUCH_FUNCTION(k)` did exactly the same, which is
what turned a one-function inconsistency into F25.

Two earlier entries in this document were corrected by the same probe; see
F25.

**Mutation checks.** Two of the fencing tests were verified by breaking the
code rather than by reading it:

```
# Delete the epoch check from handle_trigger_checkpoint_ and rebuild
./build/tests/clink_core_tests \
  --gtest_filter='CoordinatorFencing.EveryControlFrameFromASupersededCoordinatorIsRefused'
    -> FAILED: "TriggerCheckpoint from a superseded coordinator was NOT refused"
       (and CommitCheckpoint, AbortCheckpoint - the frames behind it in the queue)

# Revert the restart path's Deploy to an unstamped encode_frame and rebuild
./build/tests/clink_core_tests \
  --gtest_filter='CoordinatorFencing.AnEpochedCoordinator*'
    -> FAILED: "the restart never completed" (the redeploy was fenced off by
       the coordinator's own worker)
```

Both were restored and the suite re-run green. A per-handler check that no
test can distinguish from its absence is not covered, whatever the line
count says.

**Round of 2026-08-03**, same machine:

```
./build-it/tests/clink_core_tests
    -> 1923 tests. [  PASSED  ] 1923 tests.
./build-it/tests/clink_sql_tests
    -> [  PASSED  ] 984 tests.
ctest --test-dir build-it -L integration --parallel 1 --timeout 200
    -> 99% tests passed, 1 tests failed out of 109
       PluginSubmission.CheckpointAndRestoreAcrossJobRuns (F38, knowingly red)
```

The integration figure is the one that matters, and it is the first time the
WHOLE label has been run this round rather than a `--gtest_filter` subset of
the suites being edited. The subset was 19 of 110 cases and reported green
throughout; the label reported eleven failures. Every claim about integration
coverage before this point was narrower than it sounded.

Run it through ctest, not by executing the binary: `gtest_discover_tests`
registers each case as its own test, and running ~90 multi-process cluster
tests in a single process produces failures that are artefacts of that.

Two numbers worth noting. The harness smoke test
(`HarnessBringsUpAClusterAndReapsIt`) completes in **304 ms** - the
sleep-based equivalent could not have finished in under 500 ms by
construction, because that was the sleep. And
`CoordinatorDeathIsReportedNotSilentlyHung` takes **1.3 s**: it waits for
the submitter to actually fail, which it does promptly, rather than for a
timeout to expire.

---

## 6. Remaining limitations

Stated plainly, because the brief asks for it and because a hardening
document that only lists wins is worse than none.

### Not demonstrated

- **End-to-end exactly-once is now demonstrated by output equality across a
  worker failure AND a coordinator failover.** Extending it to failover
  found F34, a silent loss of already-published output on every resume. What
  remains unproven: exactly-once across a RESCALE, and for any sink other
  than the file 2PC sink. Both are scenarios, not mechanisms - the method
  now exists and has earned its keep twice.
- **No soak testing.** Everything here runs in seconds to minutes. State
  growth, memory stability, checkpoint-interval drift and connector
  reconnection behaviour over hours or days are untested.
- **Two platforms, not many.** Every round is verified on macOS/arm64 and on
  Debian/x86-64 in the project's Docker image, and that has earned its keep:
  F17 (the fault-framework deadlock) was found ONLY by the Linux run, having
  passed on macOS repeatedly. Nothing here has been run on any other
  platform, libc, or architecture.
- **No independent review.** Single-author work.
- **Keyed state is not demonstrated to survive a restore intact.** F38 is an
  open, reproducible case where half of one operator's keyed state comes back
  and half does not, silently. Until it is understood, every other restore
  claim in this document should be read as "the mechanism ran", not "the
  state was complete" - the two are not the same, and this document has
  argued that distinction against other people's tests all round.
- **Cross-sink commit atomicity is bounded to what F35 establishes.** A job's
  transactional sinks are told to commit together, and a split is repaired at
  restart rather than prevented. A job that exhausts its restart budget keeps
  the disagreement, and `commit_group` does not change that.

### Known gaps in what was implemented

- The capability manifest covers 13 declared connectors of ~30 impls. The
  analyser degrades safely for the rest, but a job using an undeclared
  connector gets a weaker answer than it may deserve.
- The delivery-guarantee analyser is not wired into submission, so it
  informs but does not yet enforce.
- The bounded-state validator is not wired into the binder, so it detects
  but does not yet reject.
- Fault points exist in the checkpoint write path, coordinator metadata and
  state restore. They do **not** yet exist in sink prepare/commit, the
  network frame writer, or source offset snapshotting, so the scenarios that
  depend on those remain uncovered.
- `CLINK_ALLOW_UNVERIFIED_CHECKPOINTS` is a real weakening. It is scoped to
  one status and reported in the manifest, but a cluster running with it set
  has a weaker recovery guarantee than one without.

### Compatibility impact

- **Checkpoint format**: additive. `.snap` payloads are unchanged and remain
  a plain Arrow IPC stream. A new `.snap.meta` sidecar appears beside each.
  An **older** clink reading a newer directory ignores the sidecar and works.
  A **newer** clink reading an older directory refuses until repaired - see
  W2 for the two migration routes. This is a deliberate
  correctness-over-compatibility choice under the brief's principle 8.
- **Public API**: additive only. New headers under `clink/fault/`,
  `clink/connectors/capability.hpp`, `clink/connectors/delivery_guarantee.hpp`,
  `clink/state/checkpoint_integrity.hpp`, `clink/sql/bounded_state.hpp`. New
  members on `FileBackedStateBackend` (`verify_checkpoint`,
  `latest_valid_checkpoint`). No existing signature changed.
- **Behavioural change**: `FileBackedStateBackend::restore` now throws
  `CheckpointIntegrityError` where it previously loaded whatever bytes were
  present. Callers that relied on best-effort restore must handle it. This is
  the point of the change.
- **Build**: `CLINK_ENABLE_FAULT_INJECTION` defaults to `AUTO`
  (= `CLINK_BUILD_TESTS`). A tests-off release build is unaffected.

---

## 7. Integration-suite status

Recorded separately because it is the item the brief cares most about and
the one where honesty matters most.

`tests/integration/test_fault_recovery.cpp` is new, built under
`-DCLINK_INTEGRATION_TESTS=ON`, and **gates** in `ci.yml`:

```yaml
- name: Test (fault tolerance, serial) [blocking gate]
  run: ctest ... -L integration -R 'FaultRecovery'
```

Seven scenarios, four consecutive clean runs, 73 s for the suite. One test
(`TwoConsecutiveWorkerFailuresAreSurvived`) is `DISABLED_` because it
asserts behaviour the engine does not yet have (F12); it is kept asserting
the CORRECT behaviour rather than rewritten to assert the bug, so fixing
F12 is a one-line re-enable.

Two of the original seven failures turned out to be defects in the harness
and the framework rather than in clink, both of the "reports success while
doing nothing" kind, and both now have regression guards:

- `::kill(pid, 0)` is not a liveness test for a child: a SIGKILLed child
  stays a zombie holding its pid-table entry until reaped, so every
  kill-then-wait scenario read the dead worker as still running.
  `Process::poll_exit()` uses `waitpid(WNOHANG)`.
- `CLINK_FAULT_INJECT` was never read (F13).

That is the argument for gating these rather than trusting them: three of
the seven scenarios were wrong in ways that a green advisory run would
never have surfaced.

**Update, and the reason W24 exists.** Running the whole label found that
eleven of its 110 cases were RED, and had been for some time. The advisory
line was doing exactly what an advisory line does: the pipeline stayed green
while a suite that exercises real multi-process failure and recovery did not
pass. Three distinct causes, none of which a green advisory run would ever
have surfaced:

- three helpers reading a checkpoint-marker path the engine had stopped
  writing to, so they reported "no checkpoint completed" for jobs that
  checkpointed perfectly (F37);
- a test whose 1s deadline was shorter than the ~1.3s the operation actually
  takes, so it could never have passed on this machine (F37);
- a genuine silent loss of half a job's keyed state across a restore, which
  the first defect had been masking (F38).

All eleven are now accounted for, by running them rather than by inferring
from a shared cause. Ten pass; F38 is the one left failing.

The eleven were not one defect but five, which is why the count mattered:

- **three helpers reading a moved marker path** (F37) - `TwoPhaseCommit` x3,
  `WorkerCrashRecovery`;
- **three more of the same, found only by running the whole label** -
  `CoordinatorHaFailover` (which ignored the job id it was handed and
  scanned the top level), `CoordinatorRescale` (two inline scans), and
  `CoordinatorCheckpoint`; plus the same in the failover benchmark harness;
- **a 1s deadline on a ~1.3s operation** (F37);
- **two tests asserting removed features** - `HttpDashboard` x2 still
  expected the embedded HTML dashboard that `--http-static-dir` replaced,
  and `HttpSql` grepped for an unescaped `"ops"` after the response began
  nesting the spec as an escaped string under `spec_json`. Both rewritten
  against the contract that exists, and the SQL one now also asserts the
  thing its name claims - that `?parallelism=3` reaches the compiled
  operators, which neither old assertion covered;
- **F38**, silent loss of half a job's keyed state.

A sweep of the full label also failed `SigtermShutdown` and
`ApplicationModeE2E`, which were NOT in the original eleven. Both slept a
fixed 200-300ms for process startup. Neither is flaky in the usual
hand-waving sense: each passes alone in under two seconds and fails when the
machine is busy - beside a container build, or simply running back-to-back
with the other 108 cases. A guess at how long a process takes to start is not
a synchronisation primitive, and when it loses, the test reports a defect in
the thing it was testing rather than in its own timing.

Both now wait for the coordinator's port to accept. One duration remains in
each, for the worker, which exposes no port of its own to poll - raised to a
generous bound rather than a tight guess, and noted as such in the code.

The pre-existing `integration` label remains advisory in `ci.yml` for now.
Making the whole label blocking before its sleep-based tests are converted
to the harness would be exactly the "remove continue-on-error and hope"
move the brief forbids. The intended sequence is: convert the fault-relevant
tests to the harness, establish they pass repeatedly, then split the label
so the converted subset gates while the remainder stays advisory with a
named reason and a shrinking list.
