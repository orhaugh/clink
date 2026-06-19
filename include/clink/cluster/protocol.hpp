#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace clink::cluster {

// JobManager ↔ TaskManager ↔ Client wire protocol.
//
// All messages are length-prefixed: [u32 length BE][payload].
// Payload starts with [u8 kind].
//
// String fields: [u32 length BE][bytes].
// All multi-byte integers are big-endian on the wire.

// JobId is the JM-assigned monotonic identifier for one submitted job.
// 0 is reserved as "no job" / unset.
using JobId = std::uint64_t;

enum class MessageKind : std::uint8_t {
    // TM → JM
    Register = 1,
    SubtaskFinished = 2,
    Heartbeat = 3,
    SubtaskListening = 6,
    SubtaskCheckpointed = 9,
    // Client → JM
    HelloClient = 4,
    SubmitJob = 5,
    ListJobs = 7,
    // RescaleJob (kind 11) is a client-initiated request: the client
    // asks the JM to redeploy a running job at a new parallelism per
    // role. The JM responds with RescaleJobAck. Implementation triggers
    // a final checkpoint, cancels the existing task set, and redeploys
    // each new subtask with a key-group range filter and a pointer at
    // its parent old subtask's state file (see DeploymentTask).
    RescaleJob = 11,
    // Phase 29d-4: client-initiated per-operator rescale request.
    // Wraps JobManager::request_operator_rescale (which delegates to
    // RescaleCoordinator). The JM responds with RescaleOperatorAck
    // carrying ok + reason; on accept the RescaleCoordinator state
    // moves Idle -> Preparing and the rest of the dual-run
    // choreography (BeginRescale dispatch, drain, cutover) runs.
    RescaleOperator = 12,
    // Savepoint (kind 12) is a client-initiated request: trigger a
    // one-off checkpoint synchronously and return a handle the user
    // can feed back into a future SubmitJob's restore_from_dir/
    // restore_from_checkpoint_id. Clink's  analogue of
    // ` savepoint <jobid>`. The handle is the same (dir, id)
    // pair the periodic checkpoint machinery already produces - no
    // file relocation is performed; the user can copy elsewhere.
    Savepoint = 12,
    // CancelJob (kind 103) is overloaded for the client→JM direction:
    // the client sends it to ask the JM to cancel a running job. The
    // JM responds with CancelJobAck.
    // JM → TM
    RegisterAck = 100,
    Deploy = 101,
    StartJob = 102,
    CancelJob = 103,
    PeerUpdate = 104,
    TriggerCheckpoint = 108,
    // Broadcast after every SubtaskCheckpointed ack for checkpoint N has
    // arrived and the JM has written the COMPLETED-N marker. Sinks that
    // implement 2PC use this as the phase-2 "commit" signal: their
    // pre-committed transaction (file stage, Kafka tx, SQL PREPARED)
    // finalizes only after this message.
    CommitCheckpoint = 110,
    // Phase 30c: broadcast to TMs hosting sinks in a commit_group when
    // the JM has decided the group cannot commit atomically (any
    // member's pre-commit failed). Sinks implementing 2PC roll back
    // their prepared state: file_2pc deletes staging file, kafka_2pc
    // calls abort_transaction. Mirrors CommitCheckpointMsg's payload
    // shape; the kind byte distinguishes commit-vs-abort intent.
    AbortCheckpoint = 113,
    // Phase 29d: JM -> TM. Asks the TM hosting old subtasks for an
    // operator to begin the dual-run rescale: finish current barrier
    // alignment, emit DrainMarker downstream (Phase 29b primitive),
    // close output channels, signal shutdown via SubtaskFinished.
    // New subtasks are deployed separately via Deploy with key-group
    // ranges sliced from the cutover checkpoint.
    BeginRescale = 114,
    // JM → Client
    SubmitJobAck = 105,
    JobCompleted = 106,
    ListJobsAck = 107,
    CancelJobAck = 109,
    RescaleJobAck = 111,
    RescaleOperatorAck = 115,
    SavepointAck = 112,
};

// Sentinel marking "no rescale-specific restore override" on a
// DeploymentTask. When the TM sees this value it restores from
// <restore_from_dir>/<own_subtask_idx>/, the historic behaviour.
inline constexpr std::uint32_t kRestoreFromSelf = std::numeric_limits<std::uint32_t>::max();

// Address of a peer subtask in the deployment plan. The TM uses these to
// open NetworkBridge channels to its peers.
struct PeerAddress {
    std::string role;             // peer's role name (e.g., "consumer")
    std::uint32_t subtask_idx{};  // peer's subtask index within that role
    std::string host;             // peer's data-plane host
    std::uint16_t data_port{};    // peer's data-plane port
};

// One subtask the JM is asking this TM to run.
struct DeploymentTask {
    std::string role;                // dispatched against TM's role registry
    std::uint32_t subtask_idx{};     // this subtask's index within the role
    std::uint16_t data_port{};       // port this subtask should listen on (0 = ephemeral)
    std::vector<PeerAddress> peers;  // addresses for cross-stage data channels
    std::string extra_config;        // role-specific config blob (JSON, etc.)

    // Rescale-aware restore directives. Set by the JM when a rescale
    // emits a fresh placement; left at defaults for ordinary deploys.
    //
    // restore_from_subtask_idx == kRestoreFromSelf (default) → the TM
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
    // {0, 0} is the back-compat sentinel: the TM widens it to the
    // full [0, kNumKeyGroups) range so non-rescale deploys behave
    // identically to before this field existed.
    std::uint32_t restore_from_subtask_idx{kRestoreFromSelf};
    std::uint32_t restore_from_parent_count{1};
    std::uint16_t key_group_first{0};
    std::uint16_t key_group_last{0};
};

// ----- Message bodies -----

// One plugin shared library shipped with a SubmitJob (and re-shipped
// by the JM in each Deploy so TMs can dlopen it). v1 inlines the
// bytes; a future content-addressed-upload variant can avoid
// re-shipping if the same plugin is submitted multiple times.
struct PluginBinary {
    std::string name;          // Informational, from the plugin metadata.
    std::string content_hash;  // FNV-64 hex of bytes; cache key on both ends.
    std::vector<std::byte> bytes;
};

struct RegisterMsg {
    std::string tm_id;
    std::string data_host;        // host the TM advertises for inbound data connections
    std::uint32_t slot_count{1};  // how many concurrent tasks this TM can host
    // HTTP port the TM is serving its /api/v1/* read API on. 0 means
    // the TM didn't start an HTTP listener; the JM dashboard then can't
    // proxy to it. Backward-compatible: old TMs that don't send this
    // field just look like "HTTP disabled" to the JM.
    std::uint16_t http_port{0};
};

struct RegisterAckMsg {
    bool ok{};
    std::string message;
};

struct DeployMsg {
    JobId job_id{};
    std::vector<DeploymentTask> tasks;
    // Plugins needed to instantiate the tasks. The TM writes each
    // blob to its local cache (keyed by content_hash) and dlopens
    // before running the tasks. Same bytes the JM received via
    // SubmitJob.
    std::vector<PluginBinary> plugins;
    // Per-job checkpoint config (echoed from SubmitJob). The TM uses
    // this to wire each subtask's FileBackedStateBackend and, when
    // restore_from_dir is set, to instruct the subtask to load its
    // saved state slice before opening operators.
    std::string checkpoint_dir;
    std::string restore_from_dir;
    std::uint64_t restore_from_checkpoint_id{0};
    // Unaligned-checkpoint mode for this job, echoed from
    // CheckpointConfig.alignment. The TM passes it through to each
    // RunnerContext so multi-input operator runners can switch
    // their alignment state machine. v1 trailing field - old TMs
    // see EOF and default to aligned.
    bool unaligned_checkpoints{false};
    // State schema evolution: the versions the job expects per
    // (op, state_type), packed as "op|type|ver" lines (StateVersionMap::pack).
    // The TM unpacks it into JobConfig.expected_state_versions so each
    // subtask migrates restored state up to these versions before its
    // operators run. Empty for jobs that declare none. v1 trailing field -
    // old TMs see EOF and leave it empty (no migration).
    std::string expected_state_versions_packed;
    // Per-subtask state-backend URI, echoed from CheckpointConfig. When
    // non-empty it overrides checkpoint_dir as the StateBackendSpec.uri so
    // the subtask builds a remote/disaggregated backend; checkpoint_dir
    // stays the local coordination dir. Empty -> checkpoint_dir is the
    // backend URI (legacy). v1 trailing field - old TMs see EOF and leave
    // it empty.
    std::string state_backend_uri;
};

struct StartJobMsg {
    JobId job_id{};
};

struct CancelJobMsg {
    JobId job_id{};
};

// JM -> Client reply to a client-initiated CancelJob. `ok` is false
// when the JM rejected the request (no such job, already finished,
// already cancelling) - `message` carries the human-readable reason.
struct CancelJobAckMsg {
    JobId job_id{};
    bool ok{false};
    std::string message;
};

// Client -> JM. Request to change the parallelism of one or more
// roles in a running job. `role_parallelism` lists each role's new
// parallelism; roles not listed keep their current parallelism. v1
// requires every listed parallelism to be an integer multiple of
// the role's current parallelism (scale-up only) - the JM rejects
// other shapes with ok=false.
struct RescaleJobMsg {
    JobId job_id{};
    std::vector<std::pair<std::string, std::uint32_t>> role_parallelism;
};

// JM -> Client. Reply to RescaleJob. `ok=false` carries the reason in
// `message` (no such job, parallelism not a multiple, no spare slots,
// final checkpoint failed, etc.).
struct RescaleJobAckMsg {
    JobId job_id{};
    bool ok{false};
    std::string message;
};

// Phase 29d-4: client -> JM. Per-operator rescale request. The JM
// delegates to JobManager::request_operator_rescale, which validates
// new_parallelism against the operator's Phase 29a bounds and
// transitions the operator's RescaleCoordinator state to Preparing
// on accept. The reply (RescaleOperatorAckMsg) carries the
// RequestResult.
struct RescaleOperatorMsg {
    JobId job_id{};
    std::string op_id;
    std::uint32_t new_parallelism{};
};

// JM -> client. Reply to RescaleOperator. `ok=false` means the
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

// Client -> JM. Trigger a synchronous savepoint for a running job.
// Returns SavepointAckMsg carrying the (dir, id) handle.
struct SavepointMsg {
    JobId job_id{};
    // Optional timeout in ms (0 = use JM default ~30s). The JM waits
    // up to this long for every subtask to ack the savepoint before
    // returning an error.
    std::int64_t timeout_ms{0};
};

// JM -> Client. Reply to Savepoint.
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
    std::string tm_id;
    std::string role;
    std::uint32_t subtask_idx{};
    bool had_error{};
    std::string error_message;
};

struct HeartbeatMsg {
    std::string tm_id;
};

// Sent by the client as the first frame on a control connection so the JM
// can route the connection to its client handler instead of the TM
// register-and-reader path. Empty body.
struct HelloClientMsg {};

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
};

// Distributed-checkpointing config the client attaches to a SubmitJob.
// All fields are optional - omitted ones disable that piece of the
// machinery and behaviour matches v1 (no persistence).
struct CheckpointConfig {
    // Directory the cluster uses as the snapshot root. Each job writes
    // under <dir>/<job_id>/<subtask_idx>/. Empty disables checkpointing.
    std::string checkpoint_dir;
    // Interval between JM-initiated periodic checkpoints. Zero disables
    // periodic triggers (the client / operator can still trigger via
    // future API surface).
    std::int64_t interval_ms{0};
    // When non-empty + non-zero, the cluster instructs every subtask to
    // restore its keyed state from <restore_from_dir>/<subtask_idx>/
    // before opening operators. Use this to resume from a prior run's
    // completed checkpoint.
    std::string restore_from_dir;
    std::uint64_t restore_from_checkpoint_id{0};
    // Max times the JM will automatically re-deploy this job's subtasks
    // onto surviving TMs after a TM goes lost. Each restart starts from
    // latest_completed_checkpoint_id (so keyed state is preserved; source
    // replay correctness depends on the source impl). 0 keeps the
    // legacy fail-fast behavior. Has no effect without checkpoint_dir.
    std::uint32_t max_restarts_on_tm_loss{0};

    // Aligned vs unaligned barrier handling at multi-input operators.
    // Default Aligned - back-compat with every existing job.
    CheckpointAlignment alignment{CheckpointAlignment::Aligned};

    // Per-subtask state-backend URI, decoupled from checkpoint_dir. When
    // set, each subtask builds its state backend from this URI via the
    // StateBackendFactory (e.g. "remote-read://bucket/job?endpoint=...");
    // checkpoint_dir then stays the JM's LOCAL coordination directory for
    // COMPLETED-N markers and HA recovery. Empty keeps the legacy
    // behaviour: checkpoint_dir doubles as the backend URI (bare path =
    // file scheme). This is what makes the remote/disaggregated backends
    // (remote-read, s3+rocksdb, changelog+s3) usable in a cluster job
    // without the JM writing markers to a non-filesystem path.
    std::string state_backend_uri;
};

// Client → JM. Carries a JobGraphSpec serialized as JSON, plus any
// plugin .so/.dylib files referenced by the graph, plus optional
// checkpointing config.
struct SubmitJobMsg {
    std::string graph_json;
    std::vector<PluginBinary> plugins;
    CheckpointConfig checkpoint;
};

// JM → Client. Returned in response to SubmitJob. job_id is 0 on rejection.
struct SubmitJobAckMsg {
    JobId job_id{};
    bool ok{};
    std::string message;
};

// JM → Client. One per submitted job, sent when every subtask has finished
// (cleanly or with errors) or the job was cancelled.
struct JobCompletedMsg {
    JobId job_id{};
    bool ok{};
    std::vector<std::string> errors;
};

// JobInfo: a snapshot of one running or recently-completed job, returned
// inside ListJobsAck. The JM does NOT prune completed jobs immediately,
// so list_jobs() shows both live and recently-finished jobs - the
// completion_signalled flag distinguishes them.
struct JobInfo {
    JobId job_id{};
    std::uint32_t total_subtasks{};
    std::uint32_t completed_subtasks{};
    bool completion_signalled{};
};

// Client → JM. Empty body; the JM replies with a snapshot of every job
// it currently tracks.
struct ListJobsMsg {};

// JM → Client.
struct ListJobsAckMsg {
    std::vector<JobInfo> jobs;
};

// JM → TM. Asks the TM to start a distributed checkpoint for the given
// job at the given id. The TM injects a CheckpointBarrier(checkpoint_id)
// into every source subtask it hosts for this job; the barrier flows
// downstream and each subtask snapshots its keyed state, then sends
// SubtaskCheckpointed back.
struct TriggerCheckpointMsg {
    JobId job_id{};
    std::uint64_t checkpoint_id{};
};

// JM → TM. Phase-2 of the 2PC sink protocol. Broadcast to every TM
// hosting tasks for the job once SubtaskCheckpointed acks for
// `checkpoint_id` are all in and the COMPLETED-N marker is on disk.
// Sinks implementing TwoPhaseCommitSink<T> finalize their pre-
// committed transaction in response (atomic rename, Kafka commitTx,
// SQL COMMIT PREPARED).
struct CommitCheckpointMsg {
    JobId job_id{};
    std::uint64_t checkpoint_id{};
};

// JM → TM. Phase 30c: broadcast when the JM decides a checkpoint
// must abort - one member of a commit_group failed its pre-commit,
// so every member of the group rolls back. The TM dispatches this
// to per_job_aborters_, mirroring the CommitCheckpoint path; sinks
// call their on_abort hook to release prepared state.
struct AbortCheckpointMsg {
    JobId job_id{};
    std::uint64_t checkpoint_id{};
};

// JM -> TM. Phase 29d: signal the TM hosting one or more old
// subtasks of `op_id` to begin the dual-run rescale. The TM
// dispatches to per-subtask drain callbacks: each old subtask
// runner finishes its current barrier alignment, emits a
// DrainMarker downstream (Phase 29b primitive) announcing
// `target_parallelism` to consumers, closes its output channels,
// and signals shutdown via SubtaskFinished. The new-parallelism
// subtasks are deployed separately via Deploy with key-group
// ranges sliced from the cutover checkpoint. The JM's
// RescaleCoordinator (Phase 29c) tracks drained / ready acks and
// completes the rescale when both populations settle.
struct BeginRescaleMsg {
    JobId job_id{};
    std::string op_id;  // matches OperatorSpec.id / role on the TM
    std::uint32_t target_parallelism{};
    std::uint64_t cutover_checkpoint{};
};

// TM → JM. One ack per subtask that completed its slice of checkpoint
// `checkpoint_id`. `ok=false` + `error` for snapshot failures.
struct SubtaskCheckpointedMsg {
    JobId job_id{};
    std::uint64_t checkpoint_id{};
    std::string role;
    std::uint32_t subtask_idx{};
    bool ok{};
    std::string error;
};

// TM → JM. Sent after the TM has bound its inbound data-plane listeners
// for a deployed subtask. Reports one bound port per input edge so the
// JM can resolve the upstream's outbound bridge target.
//
// `edge_ports` is empty for subtasks with no inbound listener (sources).
// In that case the subtask still sends SubtaskListening so the JM knows
// when every task is ready. Each entry pairs the listening port with the
// (upstream_role, upstream_subtask_idx) it serves; the JM uses that
// tuple as the lookup key when populating PeerUpdate.
struct SubtaskListeningMsg {
    struct EdgePort {
        std::string upstream_role;
        std::uint32_t upstream_subtask_idx{};
        std::uint16_t port{};
    };
    JobId job_id{};
    std::string tm_id;
    std::string role;
    std::uint32_t subtask_idx{};
    std::string host;
    std::vector<EdgePort> edge_ports;
};

// JM → TM. Sent after every subtask of a job has reported listening, so
// each task with outbound peer references can open its NetworkBridgeSinks
// to the right addresses. tasks[] carries one entry per (role, subtask)
// owned by this TM that has at least one peer.
struct PeerUpdateMsg {
    struct TaskPeers {
        std::string role;
        std::uint32_t subtask_idx{};
        std::vector<PeerAddress> peers;
    };
    JobId job_id{};
    std::vector<TaskPeers> tasks;
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
