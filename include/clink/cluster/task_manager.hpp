#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/cluster/checkpoint_retention.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/snapshots.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::cluster {

class OperatorRegistry;
class JobBundle;

// A RoleHandler runs one task end-to-end. It receives the deployment task
// (which contains peer addresses, the bind port, and any role-specific
// extra_config) and runs synchronously until the task is done or fails.
//
// Throwing from the handler is reported back to the JobManager as a task
// error; otherwise the task is reported as a clean finish.
using RoleHandler = std::function<void(const DeploymentTask& task)>;

// TaskManager is the cluster's worker side. Each TM:
//   1. Connects to the JobManager on (host, port) and sends Register.
//   2. Reads Deploy messages and dispatches each task either to the
//      built-in generic subtask role (when role == kGenericSubtaskRole)
//      or to a user-registered RoleHandler.
//   3. For generic-role tasks: parses the OperatorChainSpec from
//      extra_config, binds any inbound NetworkBridgeSource port,
//      reports SubtaskListening, awaits PeerUpdate, then builds and
//      runs the operator chain via LocalExecutor.
//   4. Sends SubtaskFinished back when each handler completes.
//   5. Cleanly shuts down on stop() or when the JM closes the connection.
class TaskManager {
public:
    struct Config {
        std::chrono::milliseconds heartbeat_interval{500};
        std::uint32_t slot_count{1};
        // Maximum time to wait for a PeerUpdate after sending
        // SubtaskListening. Beyond this the generic role aborts the
        // task with an error so the JM can retry/cancel cleanly.
        std::chrono::milliseconds peer_update_timeout{30000};
        // HTTP port the TM reports to the JM on Register. The JM uses
        // it to proxy /api/v1/tms/:id/* through to this TM's
        // dashboard endpoints. 0 means "no HTTP" - the JM won't
        // surface this TM in proxy paths.
        std::uint16_t http_port{0};
        // Number of most-recent COMPLETED checkpoints to retain per job.
        // When a newer checkpoint completes, older ones beyond this count
        // are purged from each subtask's state backend so checkpoint
        // storage stays bounded. The latest completed checkpoint (the one
        // recovery restores from) is always kept; clamped to >= 1.
        // Defaults to 1 (retain only the latest completed checkpoint).
        std::size_t checkpoint_num_retained{1};
    };

    TaskManager(std::string tm_id, std::string data_host);
    TaskManager(std::string tm_id, std::string data_host, Config cfg);
    ~TaskManager();

    TaskManager(const TaskManager&) = delete;
    TaskManager& operator=(const TaskManager&) = delete;
    TaskManager(TaskManager&&) = delete;
    TaskManager& operator=(TaskManager&&) = delete;

    // Install a handler for a role name. The handler is dispatched when
    // the JM sends a Deploy task with this role. The kGenericSubtaskRole
    // is auto-registered in the constructor and uses the
    // OperatorRegistry::default_instance() registry; user code should
    // not override it.
    void register_role(std::string role, RoleHandler handler);

    // Connect to the JM, send Register, await RegisterAck, then start
    // the message-reader thread.
    void connect_to_jm(const std::string& jm_host, std::uint16_t jm_port);
    void connect_to_jm(class ServiceDiscovery& sd, std::chrono::milliseconds discover_timeout);

    // Block until all tasks deployed to this TM have completed.
    bool await_all_tasks(std::chrono::milliseconds timeout);

    // Close the JM connection and join all task threads.
    void stop();

    const std::string& tm_id() const noexcept { return tm_id_; }

    // True after the JM has sent CancelJob (e.g., because the watchdog
    // declared a peer TM lost). Role handlers can poll this to abort
    // long-running work cooperatively.
    bool was_cancelled() const noexcept { return cancelled_.load(std::memory_order_acquire); }

    // True once the reader_loop_ has observed the JM connection
    // dropping (peer closed, transport error). Stays true until
    // stop(). clink_node's --ha-dir TM mode polls this so it can
    // exit on disconnect; an external supervisor (or the integration
    // test) restarts the TM, which then reads active-leader.json to
    // find the new leader.
    bool disconnected() const noexcept { return disconnected_.load(std::memory_order_acquire); }

    // Set the HTTP port this TM will advertise to the JM at register
    // time. Call AFTER starting the HttpServer (so the actually-bound
    // port is known, esp. when --http-port=0 lets the OS pick) but
    // BEFORE connect_to_jm. Has no effect after Register has been
    // sent. 0 = HTTP disabled; the JM won't proxy to this TM.
    void set_advertised_http_port(std::uint16_t port) noexcept { cfg_.http_port = port; }

    // Override the factory used to open the JM control-plane connection.
    // Default = plain-TCP via connect_plain. clink_node installs a TLS
    // factory when --tls-ca is given. Must be called before connect_to_jm.
    using ConnectFactory = std::function<std::unique_ptr<network::Connection>(
        const std::string& host, std::uint16_t port)>;
    void set_connect_factory(ConnectFactory f) { connect_factory_ = std::move(f); }

    // ----- Snapshot API for the HTTP read endpoints -----
    //
    // Same shape as the JM side: take mu_ briefly, copy state into a
    // plain value-type, release. Handlers serialize outside the lock.
    TmSnapshot snapshot_tm() const;
    std::vector<SubtaskRecord> snapshot_subtasks() const;
    Config config_snapshot() const { return cfg_; }

private:
    void reader_loop_();
    void handle_deploy_(MessageReader& r);
    void handle_peer_update_(MessageReader& r);
    void run_task_(JobId job_id,
                   const DeploymentTask& task,
                   const std::string& checkpoint_dir,
                   const std::string& restore_from_dir,
                   std::uint64_t restore_from_checkpoint_id,
                   bool unaligned_checkpoints,
                   const std::string& expected_state_versions_packed);
    void run_generic_subtask_(JobId job_id,
                              const DeploymentTask& task,
                              const std::string& checkpoint_dir,
                              const std::string& restore_from_dir,
                              std::uint64_t restore_from_checkpoint_id,
                              bool unaligned_checkpoints,
                              const std::string& expected_state_versions_packed);
    void handle_trigger_checkpoint_(MessageReader& r);
    void handle_commit_checkpoint_(MessageReader& r);
    // Phase 30c: dispatch AbortCheckpoint to per-subtask abort
    // callbacks. Mirrors handle_commit_checkpoint_; sinks register
    // their abort callback alongside their commit callback at
    // startup.
    void handle_abort_checkpoint_(MessageReader& r);
    // Phase 29d-2: dispatch BeginRescale to per-(job, op) drain
    // callbacks. The TM looks up callbacks registered against the
    // op_id in the BeginRescaleMsg payload and invokes them outside
    // the lock with the target_parallelism. Subtask runners
    // belonging to other operators are unaffected.
    void handle_begin_rescale_(MessageReader& r);
    bool send_frame_(const std::vector<std::byte>& frame);
    void heartbeat_loop_();

    // Waits for the JM-supplied PeerUpdate for one in-flight subtask.
    // Returns nullopt if the wait was interrupted (TM stop / job cancel
    // / timeout). Any caller that gets nullopt must treat the task as
    // cancelled and report SubtaskFinished with had_error=true.
    struct ResolvedPeers {
        std::vector<PeerAddress> peers;
    };
    std::optional<ResolvedPeers> await_peer_update_(JobId job_id,
                                                    const std::string& role,
                                                    std::uint32_t subtask_idx);

    Config cfg_;
    std::string tm_id_;
    std::string data_host_;
    // JM connection target - populated by connect_to_jm so the
    // snapshot_tm() / /api/v1/tm endpoint can report it.
    std::string jm_host_;
    std::uint16_t jm_port_{0};
    std::unique_ptr<network::Connection> conn_;
    ConnectFactory connect_factory_;
    std::thread reader_;
    std::thread heartbeat_;
    std::vector<std::thread> task_threads_;
    std::unordered_map<std::string, RoleHandler> roles_;
    bool deployed_{false};

    std::atomic<bool> stop_{false};
    std::atomic<bool> cancelled_{false};
    std::atomic<bool> disconnected_{false};
    mutable std::mutex mu_;
    std::mutex send_mu_;
    std::condition_variable cv_;
    std::size_t in_flight_tasks_{0};

    // Per-task pending state: when Deploy arrives, we register one
    // PendingTask per (job_id, role, subtask_idx) so the reader thread
    // can hand off the resolved peers when PeerUpdate comes back.
    struct PendingTask {
        std::vector<PeerAddress> resolved_peers;
        bool ready{false};
        bool cancelled{false};
        std::condition_variable cv;
    };
    using PendingKey = std::tuple<JobId, std::string, std::uint32_t>;
    struct PendingKeyHash {
        std::size_t operator()(const PendingKey& k) const noexcept {
            const auto& [jid, role, sub] = k;
            return std::hash<JobId>{}(jid) ^ (std::hash<std::string>{}(role) << 1) ^
                   (std::hash<std::uint32_t>{}(sub) << 2);
        }
    };
    std::unordered_map<PendingKey, std::shared_ptr<PendingTask>, PendingKeyHash> pending_;

    // Per-job checkpoint state stashed on Deploy so the trigger handler
    // can address the right job. checkpoint_dir is also passed into
    // each subtask via the runner context.
    struct JobCheckpointState {
        std::string checkpoint_dir;
        std::string restore_from_dir;
        std::uint64_t restore_from_checkpoint_id{0};
    };
    std::unordered_map<JobId, JobCheckpointState> per_job_checkpoint_;

    // Source-barrier injectors registered per running subtask. The
    // runner closure pushes its Dag::source_injectors() in here at
    // startup; the TM iterates them on TriggerCheckpoint to push a
    // CheckpointBarrier into each hosted source. Keyed by
    // (job_id, subtask_idx) under a coarse mutex (mu_).
    using BarrierInjector = std::function<void(CheckpointBarrier)>;
    struct SubtaskInjectors {
        std::vector<BarrierInjector> injectors;
    };
    std::unordered_map<JobId, std::unordered_map<std::uint32_t, SubtaskInjectors>>
        per_job_injectors_;

    // Triggers that arrived before any source registered injectors for
    // the job. Drained on the first register_source_injectors() call for
    // that job. Avoids the deploy/trigger race where the JM fires a
    // periodic checkpoint while the TM is still bringing up the chain.
    std::unordered_map<JobId, std::vector<std::uint64_t>> pending_triggers_;

    // Per-(job_id, subtask_idx) commit callbacks. Sinks implementing the
    // 2PC protocol register a callback at startup; CommitCheckpoint
    // dispatches it with the just-completed checkpoint_id. Late-bind
    // analog to per_job_injectors_ above.
    using CommitCallback = std::function<void(std::uint64_t checkpoint_id)>;
    std::unordered_map<JobId, std::unordered_map<std::uint32_t, std::vector<CommitCallback>>>
        per_job_committers_;

    // Checkpoint retention. per_job_backends_ holds each hosted subtask's
    // state backend (registered via RunnerContext::register_checkpoint_backend
    // at deploy); per_job_retention_ tracks the completed-checkpoint window
    // per job. On CommitCheckpoint the TM records the completed id and purges
    // any now-subsumed checkpoint from every hosted subtask backend so
    // checkpoint storage stays bounded. See CheckpointRetention.
    std::unordered_map<JobId, std::unordered_map<std::uint32_t, std::shared_ptr<StateBackend>>>
        per_job_backends_;
    std::unordered_map<JobId, CheckpointRetention> per_job_retention_;

    // Phase 30c: per-(job_id, subtask_idx) abort callbacks. Sinks
    // register an abort closure alongside their commit closure at
    // startup; the TM dispatches it on AbortCheckpoint. Same
    // signature shape as CommitCallback so callers can use the same
    // type alias and registration plumbing.
    using AbortCallback = std::function<void(std::uint64_t checkpoint_id)>;
    std::unordered_map<JobId, std::unordered_map<std::uint32_t, std::vector<AbortCallback>>>
        per_job_aborters_;

    // Phase 29d-2: per-(job_id, op_id) drain callbacks. Subtask
    // runners register one or more closures at startup against the
    // op_id their subtask serves (op_id == DeploymentTask.role).
    // BeginRescale dispatch looks up by (job_id, op_id) and invokes
    // every closure with the target_parallelism; the closure runs
    // the drain choreography (emit DrainMarker, close output) on
    // the subtask thread. Late dispatch for an already-shutdown
    // subtask finds no callbacks registered and silently no-ops -
    // same idempotency story as commit/abort.
    using DrainCallback = std::function<void(std::uint32_t target_parallelism)>;
    std::unordered_map<JobId, std::unordered_map<std::string, std::vector<DrainCallback>>>
        per_job_drain_callbacks_;

    // Per-job registry bundle on the TM. Plugin .so bytes shipped with
    // each Deploy are dlopened into THIS job's bundle (instead of the
    // TM-process-wide default singletons), so two concurrent jobs that
    // mint overlapping _inline_<kind>_<n> names don't trample each
    // other. Subtask runner lookups go through this bundle's
    // RunnerRegistry; built-in lookups fall through to the default
    // singletons via the bundle's parent pointer.
    std::unordered_map<JobId, std::unique_ptr<JobBundle>> per_job_bundle_;

    // Per-(job, subtask_idx) cancel token. The TM allocates the
    // shared_ptr<atomic<bool>> in run_task_ before invoking the
    // runner and threads it through RunnerContext::cancel_token so
    // the LocalExecutor's stop predicate sees it. On CancelJob the
    // handler walks the map for the job_id and sets every token to
    // true; running executors observe the flip and wind down. Held
    // by shared_ptr so the runner's executor capture stays valid
    // even after the run_task_ stack frame returns.
    std::unordered_map<JobId, std::unordered_map<std::uint32_t, std::shared_ptr<std::atomic<bool>>>>
        per_job_cancel_tokens_;
};

}  // namespace clink::cluster
