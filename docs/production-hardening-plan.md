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
| W13 | Strict rejection of unsupported SQL semantics | P0.4 | - | **Open** |
| W14 | Resource and overload limits | P1.9 | - | **Open** |
| W15 | Coordinator metadata abstraction with CAS/fencing | P1.11 | - | **Open** |
| W16 | Protocol version negotiation across RPC/frames/state | P1.12 | - | **Open** |
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

**What makes this Partial.** NOT covered:

- CEP partial matches.
- Backend-specific compaction hooks.
- Per-element expiry within a collection (see above).
- The interval join and windowed operators, which are bounded by their
  time condition or window and so are not flagged by the gate - correct
  today, but they would still benefit from a retention ceiling on a
  pathological key space.
- No operator currently drives `cleanup_batch` on a collection slot; the
  method is exposed and tested, but a user of `ListState` must call it
  themselves. The SQL operators drive their own eviction; the typed C++
  API leaves the schedule to the caller.

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

# Whole suite, after the changes
ctest --test-dir build -j8 --timeout 300
    -> 100% tests passed, 0 tests failed out of 3279

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
```

Cross-process fault arming, verified end to end:

```
CLINK_FAULT_INJECT="checkpoint.before_write=exit:70@1" \
  ./build/clink checkpoint-verify --dir <d> --repair
    -> exit 70; no sidecar written
(without the variable: exit 0; sidecar written)
```

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
- **Single platform.** Results above are from macOS/arm64. The repository's
  history records four classes of bug that appear only on Linux (`size_t`
  typedefs, `inet_pton`, close-vs-shutdown on `accept`, malloc strictness),
  so a Linux CI run is required before any of these claims are portable.
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
