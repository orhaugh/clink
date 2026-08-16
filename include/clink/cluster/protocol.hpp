#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace clink::cluster {

// Coordinator ↔ Worker ↔ Client wire protocol.
//
// All messages are length-prefixed: [u32 length BE][payload].
// Payload starts with [u8 kind].
//
// String fields: [u32 length BE][bytes].
// All multi-byte integers are big-endian on the wire.

// --- protocol versioning ---------------------------------------------
//
// The wire format above is the contract between every process in a
// cluster, and a rolling upgrade necessarily runs two versions of it at
// once. Two mechanisms carry that, and they cover different things:
//
//   * ADDITIVE TAILS handle a message gaining a field. Encoders append at
//     the end; decoders read `r.eof() ? default : read()`. An older peer
//     stops reading early and gets the default, a newer peer reads the
//     value. No negotiation is needed and none happens.
//
//   * THIS VERSION handles everything additive tails cannot: a field
//     changing meaning or width, a message kind being repurposed, a
//     semantic contract changing under an unchanged encoding, or an
//     optional capability whose new frame kind must be sent only to peers
//     known to understand it. Those are invisible on the wire, so both
//     ends declare what they speak and refuse or capability-gate as needed.
//
// Bump kClusterProtocolVersion for the second kind and NEVER merely for an
// additive tail: bumping on an additive change would refuse a rolling upgrade
// the protocol was explicitly designed to survive.
//
// kMinCompatibleClusterProtocolVersion is the oldest peer this build will
// talk to. Raise it only when carrying the compatibility shim for an old
// version stops being worth it - that is a deliberate end-of-support
// decision, and it strands any peer below it, loudly.
// v2 adds a coordinator -> worker HeartbeatAck. A v2 worker uses the
// acknowledgement as a control-plane lease and tears down its task session
// when the coordinator is unreachable, including a one-way network partition
// where TCP has not reported EOF. v1 peers remain wire-compatible: a v2
// coordinator sends the new frame only to a worker that registered as v2, and
// a v2 worker connected to a v1 coordinator falls back to EOF detection.
inline constexpr std::uint32_t kClusterProtocolVersion = 2;
inline constexpr std::uint32_t kMinCompatibleClusterProtocolVersion = 1;

// What a peer that predates versioning looks like on the wire: the field
// is absent, so it decodes as 0. Treated as version 1 rather than as an
// error, because every such peer speaks exactly the protocol that was
// version 1 when the field was introduced.
inline constexpr std::uint32_t kUnversionedPeerProtocolVersion = 1;

// Decide whether two peers can talk, given what each declares.
//
// Symmetric on purpose. "Can the coordinator read the worker?" is not the
// same question as "can the worker read the coordinator?", and a handshake
// that checks only one direction admits a pairing that half-works - which
// shows up later as a decode failure on a control frame, far from the
// cause.
struct ProtocolCompatibility {
    bool compatible{};
    std::string reason;  // empty when compatible
};

[[nodiscard]] inline ProtocolCompatibility check_protocol_compatibility(
    std::uint32_t peer_version,
    std::uint32_t peer_min_compatible,
    std::string_view peer_role,
    std::uint32_t own_version = kClusterProtocolVersion,
    std::uint32_t own_min_compatible = kMinCompatibleClusterProtocolVersion) {
    // A peer from before versioning declares nothing. Fill in what it
    // must have been rather than refusing it.
    if (peer_version == 0) {
        peer_version = kUnversionedPeerProtocolVersion;
    }
    if (peer_min_compatible == 0) {
        peer_min_compatible = kUnversionedPeerProtocolVersion;
    }
    const std::string versions =
        std::string(peer_role) + " speaks protocol v" + std::to_string(peer_version) + " (min v" +
        std::to_string(peer_min_compatible) + "); this build speaks v" +
        std::to_string(own_version) + " (min v" + std::to_string(own_min_compatible) + ")";
    if (peer_version < own_min_compatible) {
        return {false,
                versions +
                    ". The peer is older than this build supports. Upgrade the peer, "
                    "or run a build that still carries the compatibility shim for it."};
    }
    if (own_version < peer_min_compatible) {
        return {false,
                versions +
                    ". This build is older than the peer supports. Upgrade this node "
                    "before the rest of the cluster."};
    }
    return {true, {}};
}

// JobId is the coordinator-assigned monotonic identifier for one submitted job.
// 0 is reserved as "no job" / unset.
using JobId = std::uint64_t;

enum class MessageKind : std::uint8_t {
    // worker → coordinator
    Register = 1,
    SubtaskFinished = 2,
    Heartbeat = 3,
    SubtaskListening = 6,
    SubtaskCheckpointed = 9,
    // worker → coordinator. A bounded source at clean end-of-stream asks the coordinator to
    // trigger one FINAL coordinator-coordinated checkpoint that durably commits the
    // post-last-checkpoint tail before the job is allowed to complete. The
    // coordinator replies with FinalCheckpointAssigned carrying the assigned id (or 0
    // to decline if the job is already completing/cancelling).
    RequestFinalCheckpoint = 10,
    // Client → coordinator
    HelloClient = 4,
    SubmitJob = 5,
    ListJobs = 7,
    // RescaleJob (kind 11) is a client-initiated request: the client
    // asks the coordinator to redeploy a running job at a new parallelism per
    // role. The coordinator responds with RescaleJobAck. Implementation triggers
    // a final checkpoint, cancels the existing task set, and redeploys
    // each new subtask with a key-group range filter and a pointer at
    // its parent old subtask's state file (see DeploymentTask).
    RescaleJob = 11,
    // Client-initiated per-operator rescale request.
    // Wraps Coordinator::request_operator_rescale (which delegates to
    // RescaleCoordinator). The coordinator responds with RescaleOperatorAck
    // carrying ok + reason; on accept the RescaleCoordinator state
    // moves Idle -> Preparing and the rest of the dual-run
    // choreography (BeginRescale dispatch, drain, cutover) runs.
    RescaleOperator = 12,
    // Savepoint (kind 13) is a client-initiated request: trigger a
    // one-off checkpoint synchronously and return a handle the user
    // can feed back into a future SubmitJob's restore_from_dir/
    // restore_from_checkpoint_id. The handle is the same (dir, id)
    // pair the periodic checkpoint machinery already produces - no
    // file relocation is performed; the user can copy elsewhere.
    // Must NOT share a value with any other client->coordinator kind: the coordinator
    // dispatch matches on kind, so a duplicate silently routes the
    // frame to the wrong handler (this previously collided with
    // RescaleOperator=12 and aborted the coordinator on every savepoint).
    Savepoint = 13,
    // client → coordinator. Stop a running job GRACEFULLY: the sources stop
    // producing, the runners take their end-of-input final checkpoint, the sinks
    // commit it, and the job completes as a success. The ack carries the
    // checkpoint id to resubmit from, so an upgrade is stop-then-resubmit rather
    // than cancel-and-replay.
    //
    // Distinct from CancelJob (103), which does not drain: records since the last
    // completed checkpoint are simply discarded and replay on restart. Both are
    // needed - a cancel must stay abrupt - so this is a separate kind rather than
    // a flag on that one.
    StopJob = 14,
    // CancelJob (kind 103) is overloaded for the client→coordinator direction:
    // the client sends it to ask the coordinator to cancel a running job. The
    // coordinator responds with CancelJobAck.
    // coordinator → worker
    RegisterAck = 100,
    Deploy = 101,
    StartJob = 102,
    CancelJob = 103,
    PeerUpdate = 104,
    TriggerCheckpoint = 108,
    // Broadcast after every SubtaskCheckpointed ack for checkpoint N has
    // arrived and the coordinator has written the COMPLETED-N marker. Sinks that
    // implement 2PC use this as the phase-2 "commit" signal: their
    // pre-committed transaction (file stage, Kafka tx, SQL PREPARED)
    // finalizes only after this message.
    CommitCheckpoint = 110,
    // Broadcast to workers hosting sinks in a commit_group when
    // the coordinator has decided the group cannot commit atomically (any
    // member's pre-commit failed). Sinks implementing 2PC roll back
    // their prepared state: file_2pc deletes staging file, kafka_2pc
    // calls abort_transaction. Mirrors CommitCheckpointMsg's payload
    // shape; the kind byte distinguishes commit-vs-abort intent.
    AbortCheckpoint = 113,
    // coordinator -> worker. Asks the worker hosting old subtasks for an
    // operator to begin the dual-run rescale: finish current barrier
    // alignment, emit DrainMarker downstream, close output channels,
    // signal shutdown via SubtaskFinished.
    // New subtasks are deployed separately via Deploy with key-group
    // ranges sliced from the cutover checkpoint.
    BeginRescale = 114,
    // coordinator → Client
    SubmitJobAck = 105,
    JobCompleted = 106,
    ListJobsAck = 107,
    CancelJobAck = 109,
    RescaleJobAck = 111,
    RescaleOperatorAck = 115,
    SavepointAck = 112,
    // coordinator → worker. Reply to RequestFinalCheckpoint: the coordinator-assigned final
    // checkpoint id (0 = declined). The requesting source subtask injects
    // this id as a normal barrier through its own drain path, then blocks
    // until it observes CommitCheckpoint for it.
    FinalCheckpointAssigned = 116,
    // worker -> coordinator: a non-recoverable-commit sink subtask's

    // commit callbacks for a checkpoint EXECUTED without throwing. The

    // coordinator gates CONFIRMED-N markers on these (commit-confirmed

    // restore protocol).

    CommitConfirmed = 117,
    // coordinator → worker. Tell every subtask of a job to stop producing and
    // run its end-of-input path. Fired by a client StopJob.
    StopSubtasks = 117,
    // coordinator → client. Reply to StopJob.
    StopJobAck = 118,
    // coordinator → worker. The post-cutover peer endpoints for a
    // rescale-eligible output group feeding `op_id` (hot rescale, design
    // record 008). The worker's registered group hooks await the group's
    // flush of the armed barrier, swap the branch endpoints to these peers
    // (index-within-operator order), park the rest, and release the held
    // split with the new live count.
    CutoverPeerUpdate = 119,
    // coordinator → worker. Tasks downstream of the rescaled operator
    // `op_id` bind one new inbound listener per entry of
    // `new_subtask_indices` (the post-cutover subtasks' job-global
    // indices) and report the bound ports via a mid-run SubtaskListening,
    // so the cutover deploy's peer resolution finds them exactly as it
    // finds at-deploy ports.
    CutoverRebind = 120,
    // worker → coordinator. Reply to an arming BeginRescale: what this
    // worker armed for the named operator. See BeginRescaleAckMsg.
    BeginRescaleAck = 121,
    // coordinator -> worker. Reply to Heartbeat, echoed only to v2+
    // workers. Besides proving that the connection can still carry bytes in
    // both directions, the epoch fences a delayed response from a superseded
    // coordinator and the sequence makes the exchange observable in tests and
    // diagnostics.
    HeartbeatAck = 122,
};

// Sentinel marking "no rescale-specific restore override" on a
// DeploymentTask. When the worker sees this value it restores from
// <restore_from_dir>/<own_subtask_idx>/, the historic behaviour.
inline constexpr std::uint32_t kRestoreFromSelf = std::numeric_limits<std::uint32_t>::max();

// Address of a peer subtask in the deployment plan. The worker uses these to
// open NetworkBridge channels to its peers.
struct PeerAddress {
    std::string role;             // peer's role name (e.g., "consumer")
    std::uint32_t subtask_idx{};  // peer's subtask index within that role
    std::string host;             // peer's data-plane host
    std::uint16_t data_port{};    // peer's data-plane port
};

// One subtask the coordinator is asking this worker to run.
struct DeploymentTask {
    std::string role;                // dispatched against worker's role registry
    std::uint32_t subtask_idx{};     // this subtask's index within the role
    std::uint16_t data_port{};       // port this subtask should listen on (0 = ephemeral)
    std::vector<PeerAddress> peers;  // addresses for cross-stage data channels
    std::string extra_config;        // role-specific config blob (JSON, etc.)

    // Rescale-aware restore directives. Set by the coordinator when a rescale
    // emits a fresh placement; left at defaults for ordinary deploys.
    //
    // restore_from_subtask_idx == kRestoreFromSelf (default) → the worker
    // restores from <restore_from_dir>/<subtask_idx>/ as before. When
    // a different value is set, the new subtask reads its parent old
    // subtask's state file at <restore_from_dir>/<that idx>/ instead.
    //
    // restore_from_parent_count is the number of parent subtasks
    // whose state this new subtask should merge. For scale-up
    // (k = new_p / old_p > 1) it's always 1 - each new subtask reads
    // one parent. For scale-down (k_down = old_p / new_p > 1) it's
    // k_down: a new subtask owns more key groups than any single old
    // subtask, so multiple contiguous parents' state files are
    // concatenated into the new subtask's working dir. Default 1
    // keeps the back-compat single-parent semantics for non-rescale
    // deploys.
    //
    // key_group_first..key_group_last is the half-open range of key
    // groups this new subtask is responsible for. Backends apply it
    // as a filter at restore time so each new subtask only loads the
    // slice of the parent file(s) that maps to its assigned groups.
    // {0, 0} is the back-compat sentinel: the worker widens it to the
    // full [0, kNumKeyGroups) range so non-rescale deploys behave
    // identically to before this field existed.
    std::uint32_t restore_from_subtask_idx{kRestoreFromSelf};
    std::uint32_t restore_from_parent_count{1};
    std::uint16_t key_group_first{0};
    std::uint16_t key_group_last{0};
};

// ----- Message bodies -----

// One plugin shared library shipped with a SubmitJob (and referenced by the
// coordinator in each Deploy so workers can dlopen it).
//
// Content-addressed shipping (item 30): EMPTY bytes with a NON-EMPTY
// content_hash is a REFERENCE - "you already hold these bytes under this
// hash". A submitter sends references first and uploads only the hashes the
// coordinator's cache reports missing (SubmitJobAckMsg.missing_plugin_hashes);
// the coordinator sends references to any worker connection it has already
// shipped the bytes to. A receiver that cannot resolve a reference fails the
// operation loudly - a reference is a claim about the peer's cache, never a
// permission to run without the module. Empty bytes WITH an empty hash keeps
// its old meaning (a degenerate empty blob).
struct PluginBinary {
    std::string name;          // Informational, from the plugin metadata.
    std::string content_hash;  // FNV-64 hex of bytes; cache key on both ends.
    std::vector<std::byte> bytes;

    [[nodiscard]] bool is_reference() const noexcept {
        return bytes.empty() && !content_hash.empty();
    }
};

struct RegisterMsg {
    std::string worker_id;
    std::string data_host;        // host the worker advertises for inbound data connections
    std::uint32_t slot_count{1};  // how many concurrent tasks this worker can host
    // HTTP port the worker is serving its /api/v1/* read API on. 0 means
    // the worker didn't start an HTTP listener; the coordinator dashboard then can't
    // proxy to it. Backward-compatible: old workers that don't send this
    // field just look like "HTTP disabled" to the coordinator.
    std::uint16_t http_port{0};
    // Wire-protocol version this worker speaks, and the oldest peer it can
    // talk to. Appended at the tail; 0 from a peer that predates
    // versioning, which reads as v1.
    std::uint32_t protocol_version{kClusterProtocolVersion};
    std::uint32_t min_compatible_protocol_version{kMinCompatibleClusterProtocolVersion};
};

struct RegisterAckMsg {
    bool ok{};
    std::string message;
    // Fencing epoch of the coordinator that sent this. Bumped on every
    // leadership acquisition (HaCoordinator). A worker binds the epoch it
    // saw at registration and REFUSES any later frame carrying a lower
    // one, so a partitioned old leader that still believes it holds the
    // lock cannot deploy, cancel, or commit behind the new leader's back.
    //
    // Appended at the tail and defaulted to 0 on read, matching the
    // additive-field idiom the rest of this protocol uses: 0 means "an
    // unfenced peer" and preserves the pre-fencing behaviour exactly, so
    // a mixed-version cluster keeps working while it is being upgraded.
    std::uint64_t coordinator_epoch{0};
    // The coordinator's side of the same declaration. A worker checks it
    // and refuses to run against a coordinator neither can honour, rather
    // than discovering the mismatch later as a decode failure on a
    // control frame far from the cause.
    std::uint32_t protocol_version{kClusterProtocolVersion};
    std::uint32_t min_compatible_protocol_version{kMinCompatibleClusterProtocolVersion};
    // A refusal caused by temporary admission pressure may be retried with
    // backoff. Protocol, identity and configuration refusals remain fatal.
    // Appended at the tail so old peers read false and retain fail-fast
    // behaviour.
    bool retryable{false};
};

struct DeployMsg {
    JobId job_id{};
    std::vector<DeploymentTask> tasks;
    // Plugins needed to instantiate the tasks. The worker writes each
    // blob to its local cache (keyed by content_hash) and dlopens
    // before running the tasks. Same bytes the coordinator received via
    // SubmitJob.
    std::vector<PluginBinary> plugins;
    // Per-job checkpoint config (echoed from SubmitJob). The worker uses
    // this to wire each subtask's FileBackedStateBackend and, when
    // restore_from_dir is set, to instruct the subtask to load its
    // saved state slice before opening operators.
    std::string checkpoint_dir;
    std::string restore_from_dir;
    std::uint64_t restore_from_checkpoint_id{0};
    // TOPOLOGY GENERATION. State lives under <base>/v<generation>/<subtask idx>,
    // because a job-global subtask index is reassigned whenever an operator is
    // resized - see docs/design/state-generations.md. `generation` is where these
    // subtasks WRITE; `restore_generation` is the one that produced the checkpoint
    // they read, which differs exactly when a restore crosses a rescale.
    //
    // Both default to 1, the generation of an initial deploy.
    std::uint32_t generation{1};
    std::uint32_t restore_generation{1};
    // Unaligned-checkpoint mode for this job, echoed from
    // CheckpointConfig.alignment. The worker passes it through to each
    // RunnerContext so multi-input operator runners can switch
    // their alignment state machine. v1 trailing field - old workers
    // see EOF and default to aligned.
    bool unaligned_checkpoints{false};
    // Adaptive checkpoint mode (CheckpointAlignment::Adaptive): sources
    // forward the barrier stamp the coordinator's trigger carried
    // instead of re-stamping the deploy-static mode above. Trailing
    // field - old workers see EOF and keep static stamping.
    bool adaptive_barrier_mode{false};
    // State schema evolution: the versions the job expects per
    // (op, state_type), packed as "op|type|ver" lines (StateVersionMap::pack).
    // The worker unpacks it into JobConfig.expected_state_versions so each
    // subtask migrates restored state up to these versions before its
    // operators run. Empty for jobs that declare none. v1 trailing field -
    // old workers see EOF and leave it empty (no migration).
    std::string expected_state_versions_packed;
    // Per-subtask state-backend URI, echoed from CheckpointConfig. When
    // non-empty it overrides checkpoint_dir as the StateBackendSpec.uri so
    // the subtask builds a remote/disaggregated backend; checkpoint_dir
    // stays the local coordination dir. Empty -> checkpoint_dir is the
    // backend URI (legacy). v1 trailing field - old workers see EOF and leave
    // it empty.
    std::string state_backend_uri;
    // Record-capture flight recorder, echoed from CheckpointConfig (see
    // there). Trailing wire fields - old workers see EOF and leave capture off.
    std::string capture_dir;
    std::uint64_t capture_records{0};
    // SQL-declared UDFs the job's expressions call, packed as a JSON array
    // (pack_udf_specs, module payloads base64 inside). The worker registers
    // each before running the job's subtasks. Trailing wire field - old
    // workers see EOF and leave it empty (no deploy-time registration).
    std::string udfs_packed;

    // Fencing epoch of the coordinator that sent this. Bumped on every
    // leadership acquisition (HaCoordinator). A worker binds the epoch it
    // saw at registration and REFUSES any later frame carrying a lower
    // one, so a partitioned old leader that still believes it holds the
    // lock cannot deploy, cancel, or commit behind the new leader's back.
    //
    // Appended at the tail and defaulted to 0 on read, matching the
    // additive-field idiom the rest of this protocol uses: 0 means "an
    // unfenced peer" and preserves the pre-fencing behaviour exactly, so
    // a mixed-version cluster keeps working while it is being upgraded.
    std::uint64_t coordinator_epoch{0};
};

struct StartJobMsg {
    JobId job_id{};
};

struct CancelJobMsg {
    JobId job_id{};
    // Fencing epoch of the coordinator that sent this. Bumped on every
    // leadership acquisition (HaCoordinator). A worker binds the epoch it
    // saw at registration and REFUSES any later frame carrying a lower
    // one, so a partitioned old leader that still believes it holds the
    // lock cannot deploy, cancel, or commit behind the new leader's back.
    //
    // Appended at the tail and defaulted to 0 on read, matching the
    // additive-field idiom the rest of this protocol uses: 0 means "an
    // unfenced peer" and preserves the pre-fencing behaviour exactly, so
    // a mixed-version cluster keeps working while it is being upgraded.
    std::uint64_t coordinator_epoch{0};
};

// coordinator -> Client reply to a client-initiated CancelJob. `ok` is false
// when the coordinator rejected the request (no such job, already finished,
// already cancelling) - `message` carries the human-readable reason.
struct CancelJobAckMsg {
    JobId job_id{};
    bool ok{false};
    std::string message;
};

// Client -> coordinator. Request to change the parallelism of one or more
// roles in a running job. `role_parallelism` lists each role's new
// parallelism; roles not listed keep their current parallelism. v1
// requires every listed parallelism to be an integer multiple of
// the role's current parallelism (scale-up only) - the coordinator rejects
// other shapes with ok=false.
struct RescaleJobMsg {
    JobId job_id{};
    std::vector<std::pair<std::string, std::uint32_t>> role_parallelism;
};

// coordinator -> Client. Reply to RescaleJob. `ok=false` carries the reason in
// `message` (no such job, parallelism not a multiple, no spare slots,
// final checkpoint failed, etc.).
struct RescaleJobAckMsg {
    JobId job_id{};
    bool ok{false};
    std::string message;
};

// Client -> coordinator. Per-operator rescale request. The coordinator
// delegates to Coordinator::request_operator_rescale, which validates
// new_parallelism against the operator's [min, max] bounds and
// transitions the operator's RescaleCoordinator state to Preparing
// on accept. The reply (RescaleOperatorAckMsg) carries the
// RequestResult.
struct RescaleOperatorMsg {
    JobId job_id{};
    std::string op_id;
    std::uint32_t new_parallelism{};
};

// coordinator -> client. Reply to RescaleOperator. `ok=false` means the
// request was rejected (out-of-bounds, equals-current,
// already-in-progress, unknown job/operator); `message` carries
// the descriptive reason from RescaleCoordinator. `ok=true` +
// `accepted_target` for accepted requests; the coordinator's state
// is now Preparing and the BeginRescale dispatch will fire on the
// next checkpoint.
struct RescaleOperatorAckMsg {
    JobId job_id{};
    bool ok{false};
    std::uint32_t accepted_target{};
    std::string message;
};

// Client -> coordinator. Trigger a synchronous savepoint for a running job.
// Returns SavepointAckMsg carrying the (dir, id) handle.
struct SavepointMsg {
    JobId job_id{};
    // Optional timeout in ms (0 = use coordinator default ~30s). The coordinator waits
    // up to this long for every subtask to ack the savepoint before
    // returning an error.
    std::int64_t timeout_ms{0};
};

// coordinator -> Client. Reply to Savepoint.
//   ok=true  : checkpoint_dir + checkpoint_id name a complete
//              snapshot. Feed them into clink_submit_job's
//              --restore-from-dir / --restore-from-checkpoint-id to
//              start a new job from this point.
//   ok=false : message carries the failure reason.
struct SavepointAckMsg {
    JobId job_id{};
    bool ok{false};
    std::uint64_t checkpoint_id{0};
    std::string checkpoint_dir;
    std::string message;
};

struct SubtaskFinishedMsg {
    JobId job_id{};
    std::string worker_id;
    std::string role;
    std::uint32_t subtask_idx{};
    bool had_error{};
    std::string error_message;
    // The error is NOT retryable: restarting cannot fix it and would
    // actively make things worse. Today that means a checkpoint-integrity
    // refusal at restore - the configured restore point is damaged, and a
    // whole-job restart quietly comes back up on FRESH state, converting a
    // loud refusal into silently empty output (found by item 19's
    // truncated-checkpoint test). The coordinator fails the job instead of
    // restarting when set. Appended at the tail, eof-guarded, defaults
    // false, so frames from older workers keep their old meaning.
    bool fatal{false};
};

struct HeartbeatMsg {
    std::string worker_id;
    // v2 tail. Zero is the value sent by v1 workers.
    std::uint64_t sequence{0};
};

struct HeartbeatAckMsg {
    std::string worker_id;
    std::uint64_t sequence{0};
    std::uint64_t coordinator_epoch{0};
};

// worker → coordinator. A bounded source subtask reached clean end-of-stream and asks the
// coordinator to coordinate one final checkpoint so its tail is durably committed
// before the job completes. See MessageKind::RequestFinalCheckpoint.
struct RequestFinalCheckpointMsg {
    JobId job_id{};
    std::string role;
    std::uint32_t subtask_idx{};
};

// coordinator → worker. Reply to RequestFinalCheckpoint. final_checkpoint_id == 0 means the
// coordinator declined (job already completing/cancelling, or no checkpoint dir); the
// source then falls back / returns and the normal restart path takes over.
struct FinalCheckpointAssignedMsg {
    JobId job_id{};
    std::string role;
    std::uint32_t subtask_idx{};
    std::uint64_t final_checkpoint_id{};
    // Fencing epoch of the sending coordinator; see CommitCheckpointMsg.
    // Zero means an unfenced coordinator and reproduces the pre-fencing
    // behaviour, so a mixed-version cluster keeps working mid-upgrade.
    std::uint64_t coordinator_epoch{0};
};

// Sent by the client as the first frame on a control connection so the coordinator
// can route the connection to its client handler instead of the worker
// register-and-reader path. Empty body.
// The client handshake. Empty until protocol versioning: a CLI built
// against one cluster version and pointed at another had no way to find
// out before it started submitting.
struct HelloClientMsg {
    std::uint32_t protocol_version{kClusterProtocolVersion};
    std::uint32_t min_compatible_protocol_version{kMinCompatibleClusterProtocolVersion};
};

// Alignment mode for distributed checkpoints. Aligned (the default
// and historical mode) waits at multi-input operators until
// every input channel has delivered a barrier - records arriving on
// already-aligned inputs get held back, which adds latency under
// backpressure. Unaligned (newer mode, since 1.11) lets the
// barrier overtake in-flight records: it forwards immediately on the
// first input that delivers, and the still-queued records on the
// other inputs are captured into the checkpoint and replayed at
// restore. Faster under backpressure; larger checkpoints. Single-
// input operators behave identically either way.
enum class CheckpointAlignment : std::uint8_t {
    Aligned = 0,
    Unaligned = 1,
    // The coordinator decides per checkpoint from measured pressure
    // (the adaptive-mode policy over recent checkpoint durations) and
    // stamps the decision on each trigger; workers stamp injected
    // barriers with it. An older coordinator receiving this submits the
    // job as Aligned - the safe default - because its decode maps
    // unknown values there.
    Adaptive = 2,
};

// Sentinel for max_restarts_on_worker_loss meaning "not set - use the recovery
// default." Resolved at the Coordinator: when checkpointing is enabled
// (checkpoint_dir set) it becomes kDefaultSelfHealRestarts (self-heal by
// default); without checkpointing it
// becomes 0 (fail-fast, since there is no checkpoint to restore from). An
// explicit 0 stays fail-fast; an explicit N stays N.
inline constexpr std::uint32_t kRestartAuto = std::numeric_limits<std::uint32_t>::max();
// Default bounded self-heal attempts for a checkpointed job that did not set a
// restart count. After this many automatic restarts the job fails loudly rather
// than looping forever.
inline constexpr std::uint32_t kDefaultSelfHealRestarts = 10;

// Distributed-checkpointing config the client attaches to a SubmitJob.
// All fields are optional - omitted ones disable that piece of the
// machinery and behaviour matches v1 (no persistence).
struct CheckpointConfig {
    // Directory the cluster uses as the snapshot root. Each job writes
    // under <dir>/<job_id>/<subtask_idx>/. Empty disables checkpointing.
    std::string checkpoint_dir;
    // Interval between coordinator-initiated periodic checkpoints. Zero disables
    // periodic triggers (the client / operator can still trigger via
    // future API surface).
    std::int64_t interval_ms{0};
    // When non-empty + non-zero, the cluster instructs every subtask to
    // restore its keyed state from <restore_from_dir>/<subtask_idx>/
    // before opening operators. Use this to resume from a prior run's
    // completed checkpoint.
    std::string restore_from_dir;
    std::uint64_t restore_from_checkpoint_id{0};
    // Max times the coordinator will automatically re-deploy this job's subtasks after a
    // worker goes lost or a subtask errors. Each restart starts from
    // latest_completed_checkpoint_id (so keyed state is preserved; source replay
    // correctness depends on the source impl). The default kRestartAuto resolves
    // to self-heal (kDefaultSelfHealRestarts) when checkpoint_dir is set and
    // fail-fast (0) otherwise; an explicit 0 forces fail-fast even with
    // checkpointing; an explicit N caps the attempts. Resolved by
    // effective_max_restarts() at the coordinator. Has no effect without checkpoint_dir.
    std::uint32_t max_restarts_on_worker_loss{kRestartAuto};

    // Aligned vs unaligned barrier handling at multi-input operators.
    // Default Aligned - back-compat with every existing job.
    CheckpointAlignment alignment{CheckpointAlignment::Aligned};

    // Per-subtask state-backend URI, decoupled from checkpoint_dir. When
    // set, each subtask builds its state backend from this URI via the
    // StateBackendFactory (e.g. "remote-read://bucket/job?endpoint=...");
    // checkpoint_dir then stays the coordinator's LOCAL coordination directory for
    // COMPLETED-N markers and HA recovery. Empty keeps the legacy
    // behaviour: checkpoint_dir doubles as the backend URI (bare path =
    // file scheme). This is what makes the remote/disaggregated backends
    // (remote-read, s3+rocksdb, changelog+s3) usable in a cluster job
    // without the coordinator writing markers to a non-filesystem path.
    std::string state_backend_uri;

    // Record-capture flight recorder (time-travel debugging). When
    // capture_dir is non-empty, every single-input operator subtask tees
    // its input records into per-checkpoint-epoch .cap files under
    // <capture_dir>/op-<id>/subtask-<idx>/ (see runtime/record_capture.hpp).
    // capture_records bounds each epoch (0 = built-in default). Trailing
    // wire fields - old peers see EOF and leave capture off.
    std::string capture_dir;
    std::uint64_t capture_records{0};
};

// Resolve max_restarts_on_worker_loss to its effective value (see the field +
// kRestartAuto). kRestartAuto -> self-heal default when checkpointing is enabled,
// fail-fast otherwise; an explicit value is used verbatim. The Coordinator calls
// this at every restart decision, so the stored/persisted value keeps the user's
// original intent (auto vs explicit) and HA recovery round-trips it.
[[nodiscard]] inline std::uint32_t effective_max_restarts(const CheckpointConfig& c) noexcept {
    if (c.max_restarts_on_worker_loss == kRestartAuto) {
        return c.checkpoint_dir.empty() ? 0u : kDefaultSelfHealRestarts;
    }
    return c.max_restarts_on_worker_loss;
}

// Client → coordinator. Carries a JobGraphSpec serialized as JSON, plus any
// plugin .so/.dylib files referenced by the graph, plus optional
// checkpointing config.
struct SubmitJobMsg {
    std::string graph_json;
    std::vector<PluginBinary> plugins;
    CheckpointConfig checkpoint;
};

// coordinator → Client. Returned in response to SubmitJob. job_id is 0 on rejection.
struct SubmitJobAckMsg {
    JobId job_id{};
    bool ok{};
    std::string message;
    // Content hashes of plugins the submission REFERENCED (empty bytes +
    // hash) that this coordinator's cache does not hold. Non-empty means
    // "re-send with bytes for these"; the submitter retries once on the
    // same connection. Appended at the tail, eof-guarded, defaults empty.
    std::vector<std::string> missing_plugin_hashes;
};

// coordinator → Client. One per submitted job, sent when every subtask has finished
// (cleanly or with errors) or the job was cancelled.
struct JobCompletedMsg {
    JobId job_id{};
    bool ok{};
    std::vector<std::string> errors;
};

// How a job ENDED, as distinct from whether it ended.
//
// completion_signalled alone cannot answer that: the coordinator sets it on
// success, on failure and on cancellation alike, so a listing that reported
// only that flag showed a job whose every restart attempt had failed as
// indistinguishable from one that finished cleanly. A test asserting "the job
// reached completion" passed on a job that had been destroyed, which is how
// this came to be noticed.
enum class JobTerminalStatus : std::uint8_t {
    Running = 0,      // still has subtasks in flight
    CompletedOk = 1,  // every subtask finished, no errors recorded
    Failed = 2,       // finished with at least one subtask error
    Cancelled = 3,    // ended because a cancel was requested
};

inline std::string_view to_string(JobTerminalStatus s) noexcept {
    switch (s) {
        case JobTerminalStatus::Running:
            return "RUNNING";
        case JobTerminalStatus::CompletedOk:
            return "COMPLETED_OK";
        case JobTerminalStatus::Failed:
            return "FAILED";
        case JobTerminalStatus::Cancelled:
            return "CANCELLED";
    }
    return "?";
}

// Client → coordinator. Stop `job_id` gracefully. timeout_ms bounds how long the
// coordinator waits for the drain and final checkpoint before giving up and
// reporting what happened; 0 means the coordinator's default.
struct StopJobMsg {
    JobId job_id{};
    std::uint64_t timeout_ms{};
};

// Coordinator → worker. Every subtask of `job_id` on the receiving worker stops
// producing and runs its end-of-input path.
struct StopSubtasksMsg {
    JobId job_id{};
    std::uint64_t coordinator_epoch{};
};

// Coordinator → client. `savepoint_checkpoint_id` is the checkpoint the stopped
// job can be resubmitted from - the final one its runners took on the way out.
// Zero means the job stopped without one, which the message explains.
struct StopJobAckMsg {
    JobId job_id{};
    bool ok{};
    std::uint64_t savepoint_checkpoint_id{};
    std::string message;
};

// JobInfo: a snapshot of one running or recently-completed job, returned
// inside ListJobsAck. The coordinator does NOT prune completed jobs immediately,
// so list_jobs() shows both live and recently-finished jobs - the
// completion_signalled flag distinguishes them, and terminal_status says which
// way a finished one went.
struct JobInfo {
    JobId job_id{};
    std::uint32_t total_subtasks{};
    std::uint32_t completed_subtasks{};
    bool completion_signalled{};
    // Rides an ADDITIVE TAIL on ListJobsAck rather than sitting inline with the
    // fields above: the jobs are a repeated group, so a field added mid-group
    // would shift every subsequent job's fields for a peer that did not expect
    // it. The tail is a parallel array in the same order, which an older
    // decoder simply stops before reading.
    JobTerminalStatus terminal_status{JobTerminalStatus::Running};
};

// Client → coordinator. Empty body; the coordinator replies with a snapshot of every job
// it currently tracks.
struct ListJobsMsg {};

// coordinator → Client.
struct ListJobsAckMsg {
    std::vector<JobInfo> jobs;
};

// coordinator → worker. Asks the worker to start a distributed checkpoint for the given
// job at the given id. The worker injects a CheckpointBarrier(checkpoint_id)
// into every source subtask it hosts for this job; the barrier flows
// downstream and each subtask snapshots its keyed state, then sends
// SubtaskCheckpointed back.
struct TriggerCheckpointMsg {
    JobId job_id{};
    std::uint64_t checkpoint_id{};
    // Fencing epoch of the sending coordinator; see CommitCheckpointMsg.
    // Zero means an unfenced coordinator and reproduces the pre-fencing
    // behaviour, so a mixed-version cluster keeps working mid-upgrade.
    std::uint64_t coordinator_epoch{0};
    // State generation this trigger was issued FOR (F84 / follow-up 49). A
    // trigger that straddles a rescale - issued against one topology,
    // arriving after the swap - used to be queued by the worker and replayed
    // into the NEW generation's sources, which then snapshotted old ids into
    // the new generation's directories: with the transition window held open,
    // 120+ snapshots per run landed outside their checkpoint's participant
    // set. The worker drops a trigger whose generation is not the one it has
    // deployed. Zero means a pre-F84 coordinator; accepted, preserving the
    // old behaviour on mixed versions.
    std::uint64_t generation{0};
    // Per-trigger barrier mode for adaptive checkpoints, offset by one
    // so zero survives the trailing-field decode as "not stamped":
    // 0 = absent (older coordinator or static alignment - the worker
    // keeps its deploy-static behaviour), 1 = aligned, 2 = unaligned.
    // Stamped by the coordinator's trigger sweep when the job's
    // alignment is CheckpointAlignment::Adaptive.
    std::uint8_t barrier_mode_plus1{0};
};

// coordinator → worker. The commit phase of the 2PC sink protocol. Broadcast to every worker
// hosting tasks for the job once SubtaskCheckpointed acks for
// `checkpoint_id` are all in and the COMPLETED-N marker is on disk.
// Sinks implementing TwoPhaseCommitSink<T> finalize their pre-
// committed transaction in response (atomic rename, Kafka commitTx,
// SQL COMMIT PREPARED).
struct CommitCheckpointMsg {
    JobId job_id{};
    std::uint64_t checkpoint_id{};
    // Fencing epoch of the coordinator that sent this. Bumped on every
    // leadership acquisition (HaCoordinator). A worker binds the epoch it
    // saw at registration and REFUSES any later frame carrying a lower
    // one, so a partitioned old leader that still believes it holds the
    // lock cannot deploy, cancel, or commit behind the new leader's back.
    //
    // Appended at the tail and defaulted to 0 on read, matching the
    // additive-field idiom the rest of this protocol uses: 0 means "an
    // unfenced peer" and preserves the pre-fencing behaviour exactly, so
    // a mixed-version cluster keeps working while it is being upgraded.
    std::uint64_t coordinator_epoch{0};
    // Retention floor (commit-confirmed restore protocol). When non-zero,
    // the worker's checkpoint retention must NOT purge any checkpoint with
    // id >= this value, whatever its retention window says: for a job
    // whose restores select CONFIRMED-N, the newest CONFIRMED checkpoint
    // (and everything after it) must stay restorable even as newer
    // checkpoints complete unconfirmed. Zero = no constraint (every job
    // without a non-recoverable-commit sink). Appended at the tail,
    // eof-guarded, defaulted to 0 by older peers - which yields exactly
    // the pre-protocol behaviour.
    std::uint64_t retain_floor{0};
};

// coordinator → worker. Broadcast when the coordinator decides a checkpoint
// must abort - one member of a commit_group failed its pre-commit,
// so every member of the group rolls back. The worker dispatches this
// to per_job_aborters_, mirroring the CommitCheckpoint path; sinks
// call their on_abort hook to release prepared state.
struct AbortCheckpointMsg {
    JobId job_id{};
    std::uint64_t checkpoint_id{};
    // Fencing epoch of the coordinator that sent this. Bumped on every
    // leadership acquisition (HaCoordinator). A worker binds the epoch it
    // saw at registration and REFUSES any later frame carrying a lower
    // one, so a partitioned old leader that still believes it holds the
    // lock cannot deploy, cancel, or commit behind the new leader's back.
    //
    // Appended at the tail and defaulted to 0 on read, matching the
    // additive-field idiom the rest of this protocol uses: 0 means "an
    // unfenced peer" and preserves the pre-fencing behaviour exactly, so
    // a mixed-version cluster keeps working while it is being upgraded.
    std::uint64_t coordinator_epoch{0};
};

// coordinator -> worker. Signal the worker hosting one or more old
// subtasks of `op_id` to begin the dual-run rescale. The worker
// dispatches to per-subtask drain callbacks: each old subtask
// runner finishes its current barrier alignment, emits a
// DrainMarker downstream announcing
// `target_parallelism` to consumers, closes its output channels,
// and signals shutdown via SubtaskFinished. The new-parallelism
// subtasks are deployed separately via Deploy with key-group
// ranges sliced from the cutover checkpoint. The coordinator's
// RescaleCoordinator tracks drained / ready acks and
// completes the rescale when both populations settle.
struct BeginRescaleMsg {
    JobId job_id{};
    std::string op_id;  // matches OperatorSpec.id / role on the worker
    std::uint32_t target_parallelism{};
    std::uint64_t cutover_checkpoint{};
    // Fencing epoch of the sending coordinator; see CommitCheckpointMsg.
    // Zero means an unfenced coordinator and reproduces the pre-fencing
    // behaviour, so a mixed-version cluster keeps working mid-upgrade.
    std::uint64_t coordinator_epoch{0};
};

// coordinator → worker. Post-cutover endpoints for the groups feeding
// `op_id` on this worker, PER FEEDING TASK: each feeder's branches connect
// to the ports the new subtasks bound for THAT feeder's edges, so an
// op-level endpoint list cannot express the swap. Each entry's `peers` is
// ordered by index within the rescaled operator; its size is the new live
// branch count. See MessageKind::CutoverPeerUpdate.
struct CutoverPeerUpdateMsg {
    struct TaskPeers {
        std::string task_role;
        std::uint32_t task_subtask_idx{};
        std::vector<PeerAddress> peers;
    };
    JobId job_id{};
    std::string op_id;
    std::vector<TaskPeers> tasks;
    std::uint64_t coordinator_epoch{0};
};

// worker → coordinator. Reply to an ARMING BeginRescale (one that names a
// cutover checkpoint): what this worker actually armed for `op_id`. The
// coordinator compares against what it expected from the deployed identity
// and the graph - a shortfall (a task built before the cutover machinery,
// or through a path that does not register hooks) aborts the hot cutover
// to the replan path BEFORE the cutover checkpoint is triggered.
struct BeginRescaleAckMsg {
    JobId job_id{};
    std::string op_id;
    std::string worker_id;
    // Arm callbacks invoked (the rescaled op's own tasks: source stops,
    // relay stops, sink commit gates - plus the feeder groups' gates,
    // which register under the same key).
    std::uint32_t armed_callbacks{0};
    // Hold-and-swap groups registered for op_id on this worker (one per
    // feeding task built through the eligible attach path).
    std::uint32_t armed_groups{0};
    // Input-rebind hooks registered for op_id (one per fed task built
    // through the eligible input path).
    std::uint32_t rebind_tasks{0};
    std::uint64_t coordinator_epoch{0};
};

// coordinator → worker. Bind new inbound listeners for the post-cutover
// subtasks of `op_id`. `upstream_role` is the role those new tasks will be
// deployed under (the port map keys edges by role + index, and the worker
// must not guess it); `new_subtask_indices` are their job-global indices.
// See MessageKind::CutoverRebind.
struct CutoverRebindMsg {
    JobId job_id{};
    std::string op_id;
    std::string upstream_role;
    std::vector<std::uint32_t> new_subtask_indices;
    std::uint64_t coordinator_epoch{0};
};

// worker → coordinator. One ack per subtask that completed its slice of checkpoint
// `checkpoint_id`. `ok=false` + `error` for snapshot failures.
struct SubtaskCheckpointedMsg {
    JobId job_id{};
    std::uint64_t checkpoint_id{};
    std::string role;
    std::uint32_t subtask_idx{};
    bool ok{};
    std::string error;
};

// worker -> coordinator. Sent from the CommitCheckpoint dispatch loop, per
// subtask whose registered commit callbacks all returned without throwing:
// the external commit for `checkpoint_id` provably EXECUTED on this
// subtask. Only consulted for jobs on the commit-confirmed restore
// protocol (a sink whose commit dies with the process - see
// ConnectorCapabilities::commit_recoverable); the coordinator ignores
// confirmations from tasks it is not tracking. The coordinator writes
// CONFIRMED-<id> once every tracked subtask confirmed, and restores for
// such jobs select the newest CONFIRMED checkpoint rather than the newest
// COMPLETED one - converting die-before-commit from a silent one-interval
// loss into a replay, at the documented price that
// die-after-commit-before-confirmation replays one interval as
// duplicates.
struct CommitConfirmedMsg {
    JobId job_id{};
    std::uint64_t checkpoint_id{};
    std::string role;
    std::uint32_t subtask_idx{};
};

// worker → coordinator. Sent after the worker has bound its inbound data-plane listeners
// for a deployed subtask. Reports one bound port per input edge so the
// coordinator can resolve the upstream's outbound bridge target.
//
// `edge_ports` is empty for subtasks with no inbound listener (sources).
// In that case the subtask still sends SubtaskListening so the coordinator knows
// when every task is ready. Each entry pairs the listening port with the
// (upstream_role, upstream_subtask_idx) it serves; the coordinator uses that
// tuple as the lookup key when populating PeerUpdate.
struct SubtaskListeningMsg {
    struct EdgePort {
        std::string upstream_role;
        std::uint32_t upstream_subtask_idx{};
        std::uint16_t port{};
    };
    JobId job_id{};
    std::string worker_id;
    std::string role;
    std::uint32_t subtask_idx{};
    std::string host;
    std::vector<EdgePort> edge_ports;
};

// coordinator → worker. Sent after every subtask of a job has reported listening, so
// each task with outbound peer references can open its NetworkBridgeSinks
// to the right addresses. tasks[] carries one entry per (role, subtask)
// owned by this worker that has at least one peer.
struct PeerUpdateMsg {
    struct TaskPeers {
        std::string role;
        std::uint32_t subtask_idx{};
        std::vector<PeerAddress> peers;
    };
    JobId job_id{};
    std::vector<TaskPeers> tasks;
    // Fencing epoch of the sending coordinator; see CommitCheckpointMsg.
    // Zero means an unfenced coordinator and reproduces the pre-fencing
    // behaviour, so a mixed-version cluster keeps working mid-upgrade.
    std::uint64_t coordinator_epoch{0};
};

// ----- Binary builder / reader -----

class MessageBuilder {
public:
    void put_u8(std::uint8_t v) { bytes_.push_back(static_cast<std::byte>(v)); }

    void put_u16_be(std::uint16_t v) {
        bytes_.push_back(static_cast<std::byte>((v >> 8) & 0xFF));
        bytes_.push_back(static_cast<std::byte>(v & 0xFF));
    }
    void put_u32_be(std::uint32_t v) {
        for (int i = 3; i >= 0; --i) {
            bytes_.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    }
    void put_u64_be(std::uint64_t v) {
        for (int i = 7; i >= 0; --i) {
            bytes_.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    }
    void put_string(const std::string& s) {
        put_u32_be(static_cast<std::uint32_t>(s.size()));
        bytes_.insert(bytes_.end(),
                      reinterpret_cast<const std::byte*>(s.data()),
                      reinterpret_cast<const std::byte*>(s.data() + s.size()));
    }

    // Wrap the payload in a length-prefixed frame.
    std::vector<std::byte> finalize() {
        std::vector<std::byte> out;
        const auto len = static_cast<std::uint32_t>(bytes_.size());
        for (int i = 3; i >= 0; --i) {
            out.push_back(static_cast<std::byte>((len >> (i * 8)) & 0xFF));
        }
        out.insert(out.end(), bytes_.begin(), bytes_.end());
        return out;
    }

private:
    std::vector<std::byte> bytes_;
};

class MessageReader {
public:
    // Owns its payload by value. Earlier versions stored a reference, which
    // was a foot-gun: callers could (and did) pass a temporary and read OOB.
    explicit MessageReader(std::vector<std::byte> payload) : bytes_(std::move(payload)) {}

    std::uint8_t read_u8() { return static_cast<std::uint8_t>(consume_byte_()); }
    std::uint16_t read_u16_be() {
        const std::uint16_t hi = static_cast<unsigned char>(consume_byte_());
        const std::uint16_t lo = static_cast<unsigned char>(consume_byte_());
        return static_cast<std::uint16_t>((hi << 8) | lo);
    }
    std::uint32_t read_u32_be() {
        std::uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v = (v << 8) | static_cast<unsigned char>(consume_byte_());
        }
        return v;
    }
    std::uint64_t read_u64_be() {
        std::uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v = (v << 8) | static_cast<unsigned char>(consume_byte_());
        }
        return v;
    }
    std::string read_string() {
        const std::uint32_t len = read_u32_be();
        if (pos_ + len > bytes_.size()) {
            throw std::runtime_error("MessageReader: truncated string");
        }
        std::string s(reinterpret_cast<const char*>(bytes_.data() + pos_), len);
        pos_ += len;
        return s;
    }

    // Read a length/count prefix for a container that follows.
    //
    // The count is peer-controlled, and every decoder used to hand it
    // straight to reserve(): a Deploy claiming 0xFFFFFFFF tasks made the
    // coordinator ask for roughly 400 GB before reading a single element.
    // Ten decoders had that shape.
    //
    // The bound needs no magic number. Every element costs at least one
    // byte on the wire, so a count larger than the bytes REMAINING in this
    // payload cannot be honest, whatever the element type. Rejecting here
    // turns an allocation the peer chose into a decode error.
    std::uint32_t read_count() {
        const auto n = read_u32_be();
        if (static_cast<std::size_t>(n) > remaining()) {
            throw std::runtime_error(
                "MessageReader: element count " + std::to_string(n) + " exceeds the " +
                std::to_string(remaining()) +
                " bytes left in this frame; the frame is malformed or truncated");
        }
        return n;
    }

    [[nodiscard]] std::size_t remaining() const noexcept {
        return pos_ >= bytes_.size() ? 0 : bytes_.size() - pos_;
    }

    bool eof() const noexcept { return pos_ >= bytes_.size(); }

private:
    std::byte consume_byte_() {
        if (pos_ >= bytes_.size()) {
            throw std::runtime_error("MessageReader: truncated payload");
        }
        return bytes_[pos_++];
    }

    std::vector<std::byte> bytes_;
    std::size_t pos_{0};
};

}  // namespace clink::cluster
