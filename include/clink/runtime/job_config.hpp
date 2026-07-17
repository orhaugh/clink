#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/types.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/state_backend.hpp"

namespace spdlog {
class logger;
}

namespace clink {

class MetricsRegistry;
class DeadLetterQueue;

// JobConfig is the bag of optional knobs the LocalExecutor consults at
// startup. It is intentionally a plain struct - every field can be omitted
// and the runtime will fall back to a sensible default.
struct JobConfig {
    // Execution mode (BATCH-1). Selects between the continuous streaming path
    // and the bounded run-to-completion (batch) path.
    //   Auto      - derive from the sources: a job whose sources are all
    //               bounded runs as a bounded job, one with any unbounded
    //               source runs as a streaming job. The default.
    //   Streaming - force the continuous path even over bounded sources (useful
    //               to exercise the streaming machinery on finite input).
    //   Batch     - force the bounded run-to-completion path. Rejected at start
    //               if any source is unbounded, since the job would never
    //               terminate.
    // Only the run-to-completion entry point consults this today; the streaming
    // path is byte-for-byte unchanged. BATCH-2/3 (blocking shuffle, batch
    // scheduler) key further behaviour off Batch mode.
    enum class ExecutionMode { Auto, Streaming, Batch };
    ExecutionMode execution_mode{ExecutionMode::Auto};

    // Keyed state backend used by operators. The runtime does not yet hand
    // this to operators automatically - they pick it up when constructed -
    // but the executor uses it for restore-on-start.
    std::shared_ptr<StateBackend> state_backend;

    // If present, the runtime calls state_backend->restore(*restore_from)
    // before any operator starts processing. Use this to resume a job from a
    // previous checkpoint.
    std::optional<Snapshot> restore_from;

    // Key-group filter passed to state_backend->restore(). Only used
    // when `restore_from` is set. The default {0, kNumKeyGroups} loads
    // every group - the back-compat path for restore-on-resubmit. A
    // narrower range scopes the load to one rescaled subtask's slice.
    KeyGroupRange restore_key_group_filter{};

    // FOUND-3 (relocatable savepoints): the directory the savepoint's
    // checkpoint dirs now live under. When set, the executor hands it to the
    // backend (set_restore_base) before restore, so a savepoint moved to a new
    // path restores even though restore_from's bytes embed the capture-time
    // absolute path - only the cp-dir basename is used, resolved here. Empty =
    // use the embedded path verbatim (same-location restart).
    std::string restore_base;

    // Record-capture flight recorder (time-travel debugging). When non-empty,
    // operator runners whose registration supplied an input codec tee every
    // input record into per-checkpoint-epoch .cap files under
    // <capture_dir>/op-<id>/subtask-<idx>/ (see record_capture.hpp).
    // Best-effort debug facility: a write failure disarms that runner's
    // capture, never the job. capture_records bounds each epoch's stored
    // records (0 = the built-in default cap).
    std::string capture_dir;
    std::size_t capture_records{0};
    // Names the subtask subdirectory capture files land in (cluster path:
    // the chain's subtask index; in-process default 0).
    std::size_t capture_subtask_idx{0};

    // Queryable-state identity: the DeploymentTask role and global subtask
    // index this runner executes as, threaded into every RuntimeContext so
    // operators can bind state slots under the exact (role, subtask)
    // address external clients query (see queryable_state/). Empty role in
    // in-process / legacy paths - operators then skip queryable binding.
    std::string runner_role;
    std::size_t runner_subtask_idx{0};

    // State schema evolution: the versions the live job expects, per
    // (op, state_type). When set, the executor (a) on a restore,
    // migrates the restored state up to these versions before any
    // operator reads it (throwing on a missing migration path); (b) on
    // any start, stamps the backend so produced snapshots record these
    // versions. Empty/unset preserves the legacy behaviour (no
    // migration, snapshots carry no version stamps).
    std::optional<StateVersionMap> expected_state_versions;

    // Optional sink for counter/gauge metrics. The executor seeds it with
    // per-operator gauges at startup; downstream tooling can scrape it.
    MetricsRegistry* metrics{nullptr};

    // Host-owned spdlog logger threaded across the dlopen plugin boundary by
    // data (NOT resolved via a per-.so singleton). The executor copies this
    // onto every RuntimeContext so an operator's ctx.log_*() reaches the
    // node's sinks and the /api/v1/logs ring. nullptr in in-process / legacy
    // paths, where RuntimeContext falls back to the process LogBuffer (same
    // address space, so still correct). Raw non-owning pointer, lifetime owned
    // by clink::logging.
    spdlog::logger* logger{nullptr};

    // Engine-level dead-letter queue. The executor copies this onto every
    // RuntimeContext so a connector can route a poison record (a source decode
    // failure / a sink permanent write failure) somewhere instead of silently
    // dropping it. When null (the default), the executor installs a
    // LoggingDeadLetterQueue over `logger`, so bad records are at least logged with
    // zero config; set a NullDeadLetterQueue to drop silently, or a sink-backed one
    // to capture. Raw non-owning pointer; lifetime owned by the caller (or, for the
    // default, by the executor). See clink/runtime/dead_letter.hpp.
    DeadLetterQueue* dead_letter_queue{nullptr};

    // Called by each operator runner after it successfully snapshots its
    // state in response to a CheckpointBarrier. The cluster's Worker
    // populates this with a callback that sends a SubtaskCheckpointed
    // message back to the coordinator. Empty for in-process LocalExecutor runs.
    using CheckpointAckFn =
        std::function<void(CheckpointId /*id*/, bool /*ok*/, std::string /*error*/)>;
    CheckpointAckFn on_checkpoint_ack;

    // Bounded-source end-of-stream FINAL checkpoint hooks (cluster path). The
    // Worker wires these to request a coordinator-coordinated final checkpoint at
    // EOS and block until it commits, so a bounded job is not marked complete
    // until its post-last-checkpoint tail is durable + recoverable. Empty for
    // in-process runs (the source falls back to its local terminal commit).
    // See RuntimeContext::request_final_checkpoint / wait_final_committed.
    using RequestFinalCheckpointFn = std::function<std::uint64_t()>;
    using WaitFinalCommittedFn = std::function<bool(std::uint64_t, std::chrono::milliseconds)>;
    RequestFinalCheckpointFn request_final_checkpoint;
    WaitFinalCommittedFn wait_final_committed;

    // External cancellation flag. If set, the LocalExecutor's stop
    // predicate ORs with `external_cancel_token->load()` so an outside
    // signaller (the Worker handling a CancelJob message) can ask
    // the executor to wind down without holding a reference to the
    // executor itself. shared_ptr so the executor's thread captures
    // can outlive the caller's stack frame.
    std::shared_ptr<std::atomic<bool>> external_cancel_token;

    // Checkpoint barrier alignment policy at multi-input operators.
    // Default false (Aligned, the historical Chandy-Lamport-with-
    // alignment semantic). When true, barriers overtake records on
    // the not-yet-arrived input channels - checkpoints complete
    // faster under backpressure, at the cost of larger snapshots
    // that include the captured in-flight records. Each multi-input
    // operator runner is responsible for honouring this flag (the
    // alignment state machine forwards barriers immediately; the
    // runner separately captures + replays the per-channel in-flight
    // buffer via the state backend).
    bool unaligned_checkpoints{false};

    // Shared drain-target signal. The cluster's worker wires
    // BeginRescale dispatch to set this atomic to the rescale's
    // target_parallelism; the source runner in dag.hpp polls it
    // between produce() calls and, when non-zero, emits a
    // DrainMarker downstream + exits cleanly. Default null = no
    // rescale support (LocalExecutor / non-cluster path); the
    // source runner observes a null signal and produces normally.
    std::shared_ptr<std::atomic<std::uint32_t>> drain_target;

    // Per-operator override of the global alignment mode.
    // Maps OperatorId -> Mode. When an operator's id appears in this
    // map, the runner stamps every barrier passing through that
    // operator with the override mode (instead of the upstream-stamped
    // one) before the aligner pins it. Downstream operators see the
    // override on the forwarded barrier, so the policy decision
    // applied at one operator propagates to every operator that
    // consumes its output.
    //
    // Typical usage: force a specific operator to stay aligned (e.g.,
    // a stateful join that doesn't yet implement in-flight capture)
    // while the rest of the job runs unaligned. An adaptive policy
    // may populate this map dynamically from backpressure signals;
    // here the map is a static job-config knob.
    std::unordered_map<OperatorId, CheckpointBarrier::Mode> barrier_mode_overrides_by_operator;

    // Pin each operator thread to a CPU core (round-robin over the available
    // cores). Default false: threads float and the scheduler places them. When
    // true, the executor pins operator thread i to core (i % core_count()) via
    // cpu_affinity.hpp - a stepping stone to shard-per-core, where a worker that
    // owns a key-group range keeps its shard's working set hot on one core.
    // Pinning is best-effort: it is a no-op on platforms without hard affinity
    // (macOS), so this flag never changes correctness, only placement.
    bool pin_operator_threads{false};
};

}  // namespace clink
