# Protocol and format compatibility

Every place where two clink builds, or a clink build and its own older
artefacts, must agree on bytes. Each domain below evolves independently and
carries its own version and its own policy; there is deliberately no single
"clink format version", because the domains change for unrelated reasons and
a shared number would force unrelated bumps.

Two global rules hold everywhere:

- **Incompatible input fails before side effects.** A frame, sidecar,
  snapshot or plugin that this build cannot honour is refused where it is
  read, before any state mutation, sink commit, or job admission it would
  have caused.
- **Additions are compatible; changes are versioned.** Every encoding here
  has a defined way to add information that old readers skip (a tail field,
  an unknown key, a new frame kind); anything that changes the meaning of
  existing bytes needs its domain's version bumped, and the old version
  either handled or refused by name.

The frozen-bytes fixtures in `tests/fixtures/` (exercised by
`tests/test_format_fixtures.cpp`) pin the persistent and wire encodings from
outside the codebase: the fixture files were written by one build and must
stay readable by every later one, so an encode/decode pair cannot silently
co-evolve past its own history.

## Domain inventory

| Domain | Current | Minimum compatible | Where enforced |
|---|---|---|---|
| Cluster control protocol | 2 | 1 | `include/clink/cluster/protocol.hpp` (`kClusterProtocolVersion`), three handshake sites |
| Data plane (operator wire frames) | unversioned by design | n/a | `include/clink/runtime/network/wire.hpp` |
| Checkpoint metadata sidecar | 1 | 1 | `include/clink/state/checkpoint_integrity.hpp` (`kCheckpointMetaVersion`) |
| State snapshots and savepoints | 1 (`clink.format_version`) | 1 (absence reads as 1) | `docs/internals/state-snapshot-format.md` |
| Job plan (`JobGraphSpec` JSON) | unversioned, additive-only | n/a | `JobGraphSpec::from_json`, `src/cluster/job_graph.cpp` |
| Connector offsets and committables | ride the snapshot format | n/a | per-connector state encodings, `docs/internals/sink-committer-framework.md` |
| Plugin ABI | `kAbiVersion` + declared-surface fingerprint + toolchain identity | exact match on all three gates | `src/cluster/plugin_loader.cpp`, `scripts/plugin-abi-surface.txt` |
| Embedded C ABI | 1 (`CLINK_EMBED_ABI_VERSION`) | caller-checked | `include/clink/embed/clink.h` |
| Incident capture (`.cap`) | 2 (`kCaptureVersion`) | header-gated | `include/clink/runtime/record_capture.hpp` |
| Capabilities manifest JSON | 1 (`schema_version`) | additive-only within 1 | `include/clink/connectors/capability.hpp` |
| Derived record codec (described types) | 1 (layout specified in the header) | 1 | `include/clink/core/derived_codec.hpp`; fixture `derived-codec-v1.bin` |
| State shape fingerprints | 1 (kind-tag table in `fields.hpp`) | additive: absence gates nothing | `clink.state_fingerprints` metadata key; fixture `state-fingerprints-v1.txt` |

## Cluster control protocol

`kClusterProtocolVersion = 2`, `kMinCompatibleClusterProtocolVersion = 1`
(`include/clink/cluster/protocol.hpp`). Peers exchange both numbers in the
handshake messages and are compatible when the ranges overlap, so versions
may differ as long as each side can speak something the other accepts.

- **Enforcement, all three directions:** the coordinator refuses an
  incompatible worker at `Register`; the worker refuses an incompatible
  coordinator at `RegisterAck` ("the coordinator can read me" does not imply
  "I can read the coordinator"); the coordinator refuses an incompatible
  client at `HelloClient`. Each refusal increments the `protocol_mismatch`
  metric, so a cluster half-refusing its peers is visible to monitoring.
- **Compatible additions** are tail-appended fields: decoders treat an
  absent tail as the field's v1 default, and a peer that declares nothing
  reads as version 1 (pre-versioning peers still decode). Bump
  `kClusterProtocolVersion` only for a change old peers would misread.
- **Version 2 capability:** worker heartbeats carry a tail sequence and a v2
  coordinator returns `HeartbeatAck`. The worker uses acknowledgements as a
  bidirectional control-session lease, detecting one-way partitions that do
  not produce TCP EOF. The new frame is sent only when the registered worker
  declares v2; a v2 worker paired with a v1 coordinator falls back to EOF, so
  the minimum remains v1 for rolling upgrades.
- **Known limitation:** client enforcement is one-directional. The
  coordinator sends no hello ack on success, so a CLI never learns the
  coordinator's version.
- **Tests:** `tests/test_protocol_versioning.cpp` (both refusal directions
  end to end, pre-versioning decode, refusal legibility);
  `fuzz/cluster_frame` with its committed-reproducer replay;
  `tests/fixtures/register-msg-v1.bin` pins the handshake bytes.

## Data plane (operator-to-operator wire frames)

Deliberately unversioned per frame (`include/clink/runtime/network/wire.hpp`).
Both ends of a data channel are subtasks of one job, deployed by one
coordinator to workers that were each admitted through the versioned control
handshake above, so the data plane inherits the control plane's negotiation
and never talks to a peer of unknown vintage.

- **Compatible additions** are new `Kind` values: framing is
  length-prefixed, so an unknown kind is skippable, and a payload-layout
  change gets a NEW kind rather than a changed one (`WatermarkIdle = 6` and
  `ArrowBatch = 7` are the precedents; `Data = 0` is retired in place as
  documentation).
- **Robustness is structural, not versioned:** `kMaxFrameBytes` is enforced
  at both read sites before allocation; `ArrowBatch` payloads are
  self-describing Arrow IPC validated with `ValidateFull` before use, and
  the receiver checks the embedded schema against its registered batcher.
- **Tests:** `tests/test_network_channel.cpp` (oversized frames),
  `fuzz/data_frame` with the decode-reencode round-trip property.

## Checkpoint metadata sidecar

`kCheckpointMetaVersion = 1` (`include/clink/state/checkpoint_integrity.hpp`).
A `key=value` text sidecar carrying the version, checkpoint id, payload byte
count and CRC32C, written next to the payload and fsynced before the
COMPLETED marker that makes a checkpoint eligible for restore.

- **Compatible additions** are new keys: `CheckpointMeta::parse` skips
  unknown keys. A sidecar declaring a NEWER version than this build is
  `Unsupported`, never guessed at
  (`CheckpointIntegrityTest.NewerSidecarVersionIsUnsupportedNotGuessed`),
  and recovery falls back to the newest checkpoint that verifies.
- **Failure ordering:** verification runs before restore and before any
  sink is told to commit; an unverifiable checkpoint cannot cause an
  external side effect.
- **Tests:** `tests/test_checkpoint_integrity.cpp`, `fuzz/checkpoint_meta`,
  `tests/fixtures/checkpoint-meta-v1.txt`.

## State snapshots and savepoints

One Apache Arrow IPC stream per snapshot, `clink.format_version = "1"` in
the schema metadata, absence reading as version 1. Checkpoints and
savepoints are the same format; the full contract, including the changelog
variant and the rule that readers must ignore unknown metadata keys, is
`docs/internals/state-snapshot-format.md`.

- **User-value evolution** is separate from format evolution: the
  `StateVersionMap` stamps (`clink.state_versions` metadata) drive
  migrate-at-restore and the pre-deploy compatibility check, so a job can
  change its own state schema without the container format moving.
- **Tests:** the snapshot round-trip and schema-evolution suites;
  `tests/fixtures/snapshot-v1.snap` is a frozen stream that must stay
  loadable by `state_processor::Savepoint`.

## Job plan (`JobGraphSpec` JSON)

Unversioned; the policy is additive optional keys with absent-tolerant
readers, and it matters more than it looks: specs are persisted in the HA
directory, so a NEW coordinator recovering leadership must read a spec an
OLD build wrote. `name`, `column_lineage`, `expected_state_versions` and
`determinism_coverage` are the precedents - each defaults sanely when
absent. `from_json` validates structure and refuses malformed specs before
admission. Pinned by `tests/fixtures/job-spec-v1.json`.

## Connector offsets and committables

Source offsets and sink prepared-transaction handles are operator state
inside the snapshot, so they version with the snapshot format; the bytes
within are each connector's own encoding, owned and evolved by that
connector. The commit-side restore contract (staged barrier-consistent
rows, CONFIRMED markers, idempotent re-delivery of executed commits) is
`docs/internals/sink-committer-framework.md` and
`docs/internals/checkpointing.md`; the cross-process crash-window tests in
`tests/integration/` are what hold it.

## Plugin ABI

Five exported constants (`include/clink/plugin/abi_version.hpp` and
`abi_surface.hpp`, both generated): `kAbiVersion` (manual bump for semantic
breaks), `kAbiFingerprint` (hash of the DECLARED extension surface - the
headers in `scripts/plugin-abi-surface.txt` - plus the build options that
surface uses, the pinned Arrow version and `kAbiVersion`), `kToolchain`
(preprocessor-composed stdlib / dual-ABI / sanitizer identity),
`kAbiSurfaceManifest` (per-header "path=sha256" lines, so a refusal names
what differs), and `kAbiHash` (exact git commit, informational). The loader
(`src/cluster/plugin_loader.cpp`) admits a plugin iff the fingerprints, the
target triples and - when the plugin exports the symbol - the toolchain
identities all match exactly; `CLINK_STRICT_PLUGIN_ABI=1` restores exact
commit-hash matching, and a legacy plugin exporting no fingerprint symbol is
held to the strict rule rather than waved through. The submitter advertises
each plugin's identity inside `SubmitJob` (`PluginAbiAdvert`, an eof-guarded
tail) and the coordinator refuses an incompatible plugin before requesting
bytes; `SubmitJobAck` carries the cluster's identity (and, on such a refusal,
its manifest) in eof-guarded tail fields. There is no N-1 tolerance by
design: a plugin links `clink_core` statically, so "compatible" means "built
against an identical declared surface with a layout-compatible toolchain"
(design record `docs/design/010-stable-extension-model.md`).

## Embedded C ABI

`CLINK_EMBED_ABI_VERSION = 1` (`include/clink/embed/clink.h`). The contract
is caller-side: compare `clink_abi_version()` against the macro you compiled
with before using anything else. Covered by `clink_c_abi_tests`.

## Incident capture (`.cap`)

Magic `CCAP`, `kCaptureVersion = 2`
(`include/clink/runtime/record_capture.hpp`); the header carries the version
and a truncated flag, and replay surfaces the file's version in
`ReplayInfo::format_version`. Determinism and cross-version A/B semantics
are `docs/internals/replay-determinism.md`.

## Capabilities manifest JSON

`schema_version = 1`, first key of `clink --capabilities-json`
(`kCapabilityManifestSchemaVersion`,
`include/clink/connectors/capability.hpp`). Bumped only when a key is
renamed, removed or changes meaning; additions do not bump it. The build
block carries `git_sha` and `git_clean` so a manifest maps back to code.
Pinned by `ConnectorCapabilityTest.ManifestJsonDeclaresItsSchemaVersionAndOrigin`.
