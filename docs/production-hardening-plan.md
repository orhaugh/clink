# Production hardening: findings, work items, status

> An audit of clink's failure, recovery and validation behaviour, the work
> done against it, the evidence for each claim, and - stated as plainly as
> the rest - what is still not demonstrated.

**Status date:** 2026-08-02
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
| W8 | Fault-tolerance scenarios on the harness (gating), + sink exactly-once windows | P0.1 | F7 | **Partial** |
| W9 | Automated sanitizers (PR subset + nightly full), blocking | P1.7 | F8 | **Done** |
| W10 | `clink checkpoint-verify` + migration path | P1.10, P1.12 | F4 | **Done** |
| W11 | SQL bounded-state validator, enforced in the planner | P0.3 | F9 | **Done** |
| W12 | State TTL depth: event time, incremental cleanup, metrics, SQL `state_ttl` enforced by GROUP BY | P0.3 | F9 | **Partial** |
| W13 | Strict rejection of unsupported SQL semantics | P0.4 | F14, F15 | **Partial** |
| W14 | Resource and overload limits | P1.9 | - | **Open** |
| W15 | Coordinator fencing: epoch on the wire, worker enforcement, metadata guard | P1.11 | F16 | **Partial** |
| W16 | Protocol version negotiation across RPC/frames/state | P1.12 | F19, F20 | **Partial** |
| W17 | Config profiles + linter | P1.13 | - | **Open** |
| W18 | OpenTelemetry tracing | P1.14 | - | **Open** |
| W19 | Production metrics completion | P1.15 | - | **Open** |
| W20 | Non-determinism detection API | P2.16 | - | **Partial** |
| W21 | Cancellation/shutdown audit | P2.17 | - | **Open** |
| W22 | Side-output / multi-sink propagation validation | P2.18 | - | **Open** |
| W23 | Fuzz targets | P1.8 | - | **Open** |

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

**Tests:** `tests/test_sql_unsupported_semantics.cpp`, 15 cases.

**What makes this Partial.** Still accepted and ignored, and NOT addressed:

- `HAVING` with no `GROUP BY`.
- `LIMIT` / `OFFSET` / `FETCH FIRST` - the brief asks specifically whether
  `LIMIT` is global or per-subtask; that was not determined, so nothing was
  changed. Answering it needs a runtime experiment at parallelism > 1.
- `FOR UPDATE`, which is meaningless on a stream.
- `NOW()` and `RANDOM()` are accepted while `CURRENT_TIMESTAMP` is refused
  - an inconsistency in the existing non-determinism guard, not something
  this change introduced, but it should be made uniform.
- Unknown or absent `connector` is accepted at DDL time and only fails at
  plan time. Late rather than silent, so lower severity.

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

- **End-to-end exactly-once across process failure is not proven by these
  changes.** What IS demonstrated: a job recovers and completes after a
  worker is SIGKILLed before a checkpoint, after a checkpoint, at a
  precisely-injected point in state restore, and after two separated
  losses; every checkpoint the cluster publishes verifies; and a
  coordinator loss fails fast rather than hanging. What is NOT: that no
  record is duplicated or lost at the SINK across those events - the tests
  assert job completion and checkpoint progress, not output equality
  against an expected multiset. W2/W3 remove three specific ways it was silently violated, and
  W6 can now *describe* what a pipeline provides. Neither is a proof. The
  proof needs the fault points wired into sink prepare/commit and the
  scenarios in W8's "not yet covered" list, run repeatedly.
- **No soak testing.** Everything here runs in seconds to minutes. State
  growth, memory stability, checkpoint-interval drift and connector
  reconnection behaviour over hours or days are untested.
- **Two platforms, not many.** Every round is verified on macOS/arm64 and on
  Debian/x86-64 in the project's Docker image, and that has earned its keep:
  F17 (the fault-framework deadlock) was found ONLY by the Linux run, having
  passed on macOS repeatedly. Nothing here has been run on any other
  platform, libc, or architecture.
- **No independent review.** Single-author work.

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

The pre-existing `integration` label remains advisory in `ci.yml` for now.
Making the whole label blocking before its sleep-based tests are converted
to the harness would be exactly the "remove continue-on-error and hope"
move the brief forbids. The intended sequence is: convert the fault-relevant
tests to the harness, establish they pass repeatedly, then split the label
so the converted subset gates while the remainder stays advisory with a
named reason and a shrinking list.
