# 014: Incident evidence bundles

**Status:** accepted, 2026-11-09.

## The problem

When a job misbehaves, the evidence that would explain it is spread across a
checkpoint directory, a coordinator's HA directory, a capture tree and,
when tracing was on, a directory of protocol traces. Each has a tool -
`checkpoint-verify`, `state-cat`, `capture-cat`, `replay`, the formal trace
validator - but nothing gathers the pieces that belong to one incident, moves
them off the machine, and says, offline, which facts the pieces establish and
which they do not. An engineer who was not there has to reassemble the layout
by hand and decide alone what a passing tool run means.

This record adds one workflow: collect a bounded bundle, verify it offline,
replay what can be replayed, and freeze a reproduced epoch as a regression
test. It adds no new state format, protocol or replay engine: every check
calls the production code that already decides the question.

## The workflow

```bash
# 1. On the machine that ran the job: gather the evidence for job 1.
clink incident-collect --checkpoint-dir=/data/ckpt --job=1 \
    --ha-dir=/data/ha --capture-dir=/data/capture --trace-dir=/data/traces \
    --include-state --include-captures --out=incident-1

# 2. Anywhere else, with no cluster and no services: what does it establish?
clink incident-verify incident-1 --profile=replay --expected=oracle.ndjson
clink incident-verify incident-1 --profile=replay --json > report.json

# 3. Freeze a reproduced epoch as a permanent regression test.
clink incident-regress incident-1 --op=3 --epoch=4 --out=tests/incidents/inc-1
```

The commands follow the CLI's flat, hyphenated names (`checkpoint-verify`,
`capture-cat`, `replay-diff`).

## The bundle

A directory, published by renaming a completed temporary directory into
place:

| Path | Role | Present when |
| --- | --- | --- |
| `manifest.json` | the index: format, provenance, selection, every artifact's path, size and SHA-256, every absence and truncation | always |
| `job/manifest.json` | the coordinator's persisted job manifest (graph, checkpoint config, confirmation flag, capture settings), credentials redacted and the graph before a rescale left out | `--ha-dir` names a directory holding it |
| `checkpoint/_jobs/<job>/` | the job's `COMPLETED-<id>`, `CONFIRMED-<id>` and `RESTORED` records, byte for byte | always |
| `checkpoint/_state/<job>/v<gen>/<subtask>/checkpoint-<id>.snap.meta` | each participant's integrity sidecar | always |
| `checkpoint/_state/<job>/v<gen>/<subtask>/checkpoint-<id>.snap` | each participant's snapshot, and each captured epoch's starting snapshot | `--include-state` |
| `capture/op-<id>/subtask-<s>/op.json`, `capture/op-<id>/subtask-<s>/epoch-<n>.cap` | the flight recorder's epochs and operator specs | `--include-captures` |
| `traces/<process>.ndjson` | one run directory of protocol traces | `--trace-dir` |

The checkpoint and capture subtrees keep the engine's own layout, so the
production functions that resolve a restore (`resolve_restore_source`,
`state_dir_for`) and a replay (`EpochReplay`) run on the bundle unchanged.
Paths in the manifest are relative; nothing in a bundle depends on where it
was written or on a running cluster.

The manifest is versioned (`"format": "clink-incident-bundle"`,
`"version": 1`). A reader refuses a format it does not know and a version
above its own; additive fields are ignored by older readers.

## The checks

Each check reports `passed`, `failed`, `unavailable` (the evidence it needs is
not in the bundle, or a tool it needs is not on this machine) or `unsupported`
(the check cannot be made for this kind of evidence in this release), with a
stable identifier, the evidence it used, the reason, and the next action.
`passed` means exactly what the check says and no more.

| Check | Passed means | Uses |
| --- | --- | --- |
| `bundle.manifest` | the manifest parses, is a known format and version, and every path in it is relative and inside the bundle | the bundle reader |
| `bundle.artifacts` | every listed artifact exists with the listed size and SHA-256, and nothing unlisted sits in an evidence directory | SHA-256 |
| `bundle.completeness` | every artifact the requested profile needs is listed; absences the manifest records are reported, not excused | the manifest's `absent` list |
| `checkpoint.marker` | `COMPLETED-<id>` for the selected checkpoint parses and names this job, this checkpoint and a generation | `parse_completed_marker` |
| `checkpoint.restore_rule` | the engine's restore choice, recomputed over the bundle's markers, is the selected checkpoint | `restore_point_on_disk` (the coordinator's own functions) |
| `checkpoint.participants` | every participant the marker records has its snapshot sidecar under the marker's state root and generation, and with `--include-state` the snapshot verifies against it | `resolve_restore_source`, `state_dir_for`, `verify_checkpoint` |
| `state.decode` | every included snapshot restores through the production restore path; its schema stamps and fingerprints are listed | `InMemoryStateBackend::restore` |
| `protocol.trace_structure` | every trace file parses, and each process's sequence numbers start at 1 and have no gap | the trace files |
| `protocol.conformance` | the existing TLC trace validator accepts the run | `scripts/formal-check.sh --trace` |
| `replay.determinism` | two replays of an epoch produce identical emissions | `EpochReplay` |
| `replay.recorded_output` | the replay's emissions equal the records its single downstream operator captured for the same epoch | `EpochReplay`, the capture reader |
| `replay.expected_output` | the selected epochs of one operator, each replayed from its starting state, emit an independent expectation's rows, given with `--expected`, compared as multisets | `EpochReplay` |
| `destination.reconciliation` | (reported unsupported in every `full` report and required by no profile: offline verification reads no destination) | - |

Profiles name the checks a run requires: `integrity` (the `bundle.*` checks),
`consistency` (adds `checkpoint.*` and `state.decode`), `replay` (adds
`replay.determinism` and `replay.recorded_output`, and with `--expected`
`replay.expected_output`) and `full` (adds the protocol checks).

Exit codes: `0` every check the profile requires passed; `1` a check failed;
`3` none failed but at least one required check is unavailable or
unsupported (a partial result); `2` bad usage or an unreadable bundle.

`protocol.conformance` validates the bundle's job alone, and a run the
specification does not model is unsupported: the validator skips it and
validates nothing.

Two replays agreeing establishes determinism, not agreement with production;
only `replay.recorded_output` compares with what production did, and only
where production's output was captured. A truncated capture can establish
neither for the whole epoch: every replay check reports it unavailable, and
the determinism check's details say whether its stored prefix agreed with
itself.

Each replay check answers for the selection `--op`, `--subtask` and `--epoch`
name, every captured epoch when none is given, and reports how many epochs it
selected and what became of each. `--max-replays` bounds how many one run
replays; the rest are counted as omitted, never dropped from the selection.
A check fails when any selected epoch failed, whatever the others show, so a
divergence is never hidden behind missing evidence elsewhere. It passes only
when every selected epoch was checked whole and passed. Otherwise an omitted
epoch or one without its evidence makes it unavailable, and one it cannot
replay or compare makes it unsupported: a partial result either way, which a
smaller selection or a larger bound can complete.

An epoch replays from the checkpoint its events were processed from: the one
before it, except for a restored run's first epoch, which starts from the
checkpoint that run restored. The bundle knows the restores its `RESTORED`
record (the newest) and its traces' `Redeploy` events name. Before the newest
restore, a job whose sinks need commit confirmation could have restored from
any confirmed checkpoint, so there an epoch whose predecessor was never
confirmed is unavailable rather than replayed from a state that may be wrong;
the first epoch after a rescale is unsupported. The starting snapshot must be
in the bundle. An epoch a recorded restore passes over was processed by a run
the restore discarded, so `replay.expected_output` leaves it out of the union
it compares, and an epoch whose capture holds part of it makes that check
unavailable. `replay.recorded_output` places a capture in the job graph by its
uid or, for an operator without one, by its type and parameters less the Row
layout a deployment stamps, and compares each key's rows in order, since the
engine orders nothing across keys. An operator it cannot compare by its
topology is unsupported, and one it lacks evidence for is unavailable.

## Trust

The SHA-256 digests establish that a bundle is internally intact: nothing in
it changed after collection. They do not establish who made it: anyone able
to replace the whole bundle can replace its manifest too. Signing is out of
scope.

A bundle is untrusted input. The reader refuses absolute paths, `..`
components and symbolic links; bounds the number of files, each file's size,
the manifest's size and JSON depth; refuses an unknown format or version;
and never runs a command or loads a plugin a manifest names. Replay rebuilds
operators from the binary's own registry and loads no plugin, so a plugin
operator reports unsupported (`clink replay --plugin` replays it). Operators
that call an external system for each record, lookups and model inference,
report unsupported without being built: verification contacts no service.

## Collection

The collector selects the checkpoint with the engine's restore rule over the
markers on disk, the same functions a coordinator takeover calls: the newest
`COMPLETED` marker the job's `RESTORED` record does not pass over, or, for a
job whose sinks need commit confirmation, the newest `CONFIRMED` one.
`--checkpoint` overrides the choice, and the manifest records both. The
in-doubt walk a takeover runs for the confirmation family contacts external
systems and can advance the point; offline it cannot be predicted, and the
manifest says so.

Concurrent changes: the collector lists the job's markers before and after
copying and refuses to publish when they differ; it hashes each file as it
copies and re-reads its size and modification time afterwards, refusing on
any change; it never mixes checkpoints or generations, because every state
path derives from the one marker. It does not pin anything: clink has no
retention lock, and adding one is a distributed service this record does not
want. A checkpoint that disappears during collection fails the collection
with the reason, and the user collects again or stops the job first.

A coordinator removes a job's manifest from its HA directory when the job
ends. Collecting while the job runs keeps the graph and the restore rule in
the bundle; after it has ended, `--commit-confirmation` supplies the rule and
the recorded-output check has no graph.

Bounds: `--max-bytes` (default 1 GiB), `--max-files` (default 10,000) and
`--timeout-s` (default 600) stop collection with an error; `--op` and
`--epochs` narrow the captures, and what selection leaves out is listed as
excluded in the manifest.

Sensitive data: graph parameters whose names suggest a credential
(`password`, `passwd`, `secret`, `token`, `credential`, `api_key`, `apikey`,
`access_key`, `private_key`, `sasl`, `auth`, `conninfo`) are replaced by
`<redacted>` in `job/manifest.json`, and the graph before a rescale is left
out. A bare `key` is not on the list: it names key columns far more often
than secrets. Snapshots and captures hold the job's
records and keyed state, which no redaction can make safe, so they are
collected only with `--include-state` and `--include-captures`, and the
manifest records whether they were.

## Unsupported in this release

Coordination stores other than the local filesystem (an `s3://` checkpoint
directory), RocksDB SST checkpoints for `state.decode`, state outside the
checkpoint directory, replay of multi-input operators, non-Row channels and
plugin operators, a live per-subtask view of a running job, destination
reconciliation, signing.

## Implementation and tests

- `include/clink/cluster/in_doubt_resolution.hpp`: the coordinator's
  marker-scan functions move here unchanged, with `restore_point_on_disk`.
- `include/clink/incident/bundle.hpp`, `src/incident/`: the manifest, the
  reader's bounds, collection and the checks that need no SQL runtime.
- `include/clink/sql/incident_replay.hpp`, `src/sql/incident_replay.cpp`: the
  replay checks and regression bundles, on `EpochReplay`.
- `tools/clink_incident.cpp`: the three commands; `clink replay --run-test`
  runs a regression bundle as its generated test does.
- Tests: every check's positive case and its mutations (a missing, modified or
  unlisted artifact; a truncated snapshot or capture; a wrong job, checkpoint
  or generation; mismatched participants; a restore rule that chooses another
  checkpoint; a missing starting state; an undecodable snapshot; a gap in a
  trace; altered captured events; changed recorded output; a manifest
  claiming evidence it lacks; unsafe paths; limits), collection races, atomic
  publication, and replay's determinism-without-agreement and
  truncated-capture cases.
- A runnable example, `examples/incident-verification/`, with a worker killed
  at an asserted point and an independent oracle, which is also the
  integration gate. Its verifications run under the operating system's
  network isolation, proven first on that machine against a local control
  endpoint (`scripts/network-isolation.sh`). A run without it, which only an
  explicit development override allows, and a run whose conformance check
  was unavailable are never a qualification: the example exits 3 and the gate
  fails.
