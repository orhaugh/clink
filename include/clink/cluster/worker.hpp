#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/cluster/checkpoint_retention.hpp"
#include "clink/cluster/commit_dispatch_gate.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/snapshots.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::cluster {

class OperatorRegistry;
class JobBundle;

// A connect failure carries enough policy for a long-lived worker process to
// distinguish an unavailable leader from a permanent protocol/configuration
// refusal. Transport failures are retryable. A malformed handshake, a
// registration rejection, or incompatible protocol is fatal until the binary
// or configuration changes, so retrying it forever would only hammer the
// coordinator and hide the real fault.
class WorkerConnectionError : public std::runtime_error {
public:
    enum class Kind { Transient, FatalHandshake };

    WorkerConnectionError(Kind kind, std::string message)
        : std::runtime_error(std::move(message)), kind_(kind) {}

    [[nodiscard]] bool retryable() const noexcept { return kind_ == Kind::Transient; }

private:
    Kind kind_;
};

// A RoleHandler runs one task end-to-end. It receives the deployment task
// (which contains peer addresses, the bind port, and any role-specific
// extra_config) and runs synchronously until the task is done or fails.
//
// Throwing from the handler is reported back to the Coordinator as a task
// error; otherwise the task is reported as a clean finish.
using RoleHandler = std::function<void(const DeploymentTask& task)>;

// Worker is the cluster's worker side. Each worker:
//   1. Connects to the Coordinator on (host, port) and sends Register.
//   2. Reads Deploy messages and dispatches each task either to the
//      built-in generic subtask role (when role == kGenericSubtaskRole)
//      or to a user-registered RoleHandler.
//   3. For generic-role tasks: parses the OperatorChainSpec from
//      extra_config, binds any inbound NetworkBridgeSource port,
//      reports SubtaskListening, awaits PeerUpdate, then builds and
//      runs the operator chain via LocalExecutor.
//   4. Sends SubtaskFinished back when each handler completes.
//   5. Cleanly shuts down on stop() or when the coordinator closes the connection.
class Worker {
public:
    struct Config {
        std::chrono::milliseconds heartbeat_interval{500};
        // Maximum time without any coordinator frame after registration.
        // Protocol-v2 coordinators acknowledge every heartbeat, making this a
        // bidirectional control-plane lease rather than an EOF detector. Set
        // to 0 to disable the lease (used only for compatibility tests).
        std::chrono::milliseconds coordinator_heartbeat_timeout{3000};
        std::uint32_t slot_count{1};
        // Maximum time to wait for a PeerUpdate after sending
        // SubtaskListening. Beyond this the generic role aborts the
        // task with an error so the coordinator can retry/cancel cleanly.
        std::chrono::milliseconds peer_update_timeout{30000};
        // HTTP port the worker reports to the coordinator on Register. The coordinator uses
        // it to proxy /api/v1/workers/:id/* through to this worker's
        // dashboard endpoints. 0 means "no HTTP" - the coordinator won't
        // surface this worker in proxy paths.
        std::uint16_t http_port{0};
        // Number of most-recent COMPLETED checkpoints to retain per job.
        // When a newer checkpoint completes, older ones beyond this count
        // are purged from each subtask's state backend so checkpoint
        // storage stays bounded. The latest completed checkpoint (the one
        // recovery restores from) is always kept; clamped to >= 1.
        // Defaults to 1 (retain only the latest completed checkpoint).
        std::size_t checkpoint_num_retained{1};
    };

    Worker(std::string worker_id, std::string data_host);
    Worker(std::string worker_id, std::string data_host, Config cfg);
    ~Worker();

    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;
    Worker(Worker&&) = delete;
    Worker& operator=(Worker&&) = delete;

    // Install a handler for a role name. The handler is dispatched when
    // the coordinator sends a Deploy task with this role. The kGenericSubtaskRole
    // is auto-registered in the constructor and uses the
    // OperatorRegistry::default_instance() registry; user code should
    // not override it.
    void register_role(std::string role, RoleHandler handler);

    // Connect to the coordinator, send Register, await RegisterAck, then start
    // the message-reader thread.
    void connect_to_coordinator(const std::string& coordinator_host,
                                std::uint16_t coordinator_port);
    void connect_to_coordinator(class ServiceDiscovery& sd,
                                std::chrono::milliseconds discover_timeout);

    // Block until all tasks deployed to this worker have completed.
    bool await_all_tasks(std::chrono::milliseconds timeout);

    // Close the coordinator connection and join all task threads.
    void stop();

    const std::string& worker_id() const noexcept { return worker_id_; }

    // True after the coordinator has sent CancelJob (e.g., because the watchdog
    // declared a peer worker lost). Role handlers can poll this to abort
    // long-running work cooperatively.
    bool was_cancelled() const noexcept { return cancelled_.load(std::memory_order_acquire); }

    // True once the control session has ended through EOF, transport/protocol
    // failure, or expiry of the coordinator heartbeat lease. The long-lived
    // worker supervisor replaces this Worker object with a fresh session; the
    // process and container stay alive.
    bool disconnected() const noexcept { return disconnected_.load(std::memory_order_acquire); }

    // Subtasks still winding down inside stop(). Non-zero means this
    // worker is draining and will not re-register until it reaches zero.
    [[nodiscard]] std::size_t draining_subtasks() const noexcept {
        return draining_subtasks_.load(std::memory_order_acquire);
    }
    // Unix seconds when the current drain began, 0 when not draining. A
    // value that stops advancing towards zero subtasks is the signature
    // of a wedged teardown.
    [[nodiscard]] std::int64_t drain_started_unix() const noexcept {
        return drain_started_unix_.load(std::memory_order_acquire);
    }

    // Fencing epoch this worker is bound to: the epoch carried by the
    // RegisterAck that admitted it, raised if a later frame carries a
    // higher one. Zero means the coordinator is unfenced (non-HA, or an
    // older build), which reproduces the pre-fencing behaviour.
    [[nodiscard]] std::uint64_t bound_epoch() const noexcept {
        return bound_epoch_.load(std::memory_order_acquire);
    }
    // Control frames refused because they carried a lower epoch than the
    // bound one - i.e. arrived from a coordinator that has since been
    // superseded. Non-zero is a split-brain signal worth alerting on.
    [[nodiscard]] std::uint64_t fenced_frame_count() const noexcept {
        return fenced_frames_.load(std::memory_order_acquire);
    }

    // Set the HTTP port this worker will advertise to the coordinator at register
    // time. Call AFTER starting the HttpServer (so the actually-bound
    // port is known, esp. when --http-port=0 lets the OS pick) but
    // BEFORE connect_to_coordinator. Has no effect after Register has been
    // sent. 0 = HTTP disabled; the coordinator won't proxy to this worker.
    void set_advertised_http_port(std::uint16_t port) noexcept { cfg_.http_port = port; }

    // Override the factory used to open the coordinator control-plane connection.
    // Default = plain-TCP via connect_plain. clink_node installs a TLS
    // factory when --tls-ca is given. Must be called before connect_to_coordinator.
    using ConnectFactory = std::function<std::unique_ptr<network::Connection>(
        const std::string& host, std::uint16_t port)>;
    void set_connect_factory(ConnectFactory f) { connect_factory_ = std::move(f); }

    // Task-thread handles this worker is still holding. Finished
    // subtasks are joined and erased on the next deploy, so under job
    // churn this stays bounded by concurrent subtasks rather than
    // growing with the worker's lifetime total. Exposed because the leak
    // it guards is INVISIBLE to a live-thread count: an exited but
    // unjoined pthread is reaped by the kernel while its 8MB stack
    // mapping survives, so only the handle count shows it.
    [[nodiscard]] std::size_t retained_task_thread_count() const;

    // ----- Snapshot API for the HTTP read endpoints -----
    //
    // Same shape as the coordinator side: take mu_ briefly, copy state into a
    // plain value-type, release. Handlers serialize outside the lock.
    WorkerSnapshot snapshot_worker() const;

    // State-as-data: merge every hosted subtask backend's LIVE Arrow
    // export for `job_id` into one canonical state-snapshot stream (see
    // docs/internals/state-snapshot-format.md). Per-subtask atomic, NOT
    // a checkpoint-consistent global cut. Backends that refuse a live
    // export (e.g. the disaggregated backend's partial hot tier) are
    // counted in skipped_subtasks rather than silently omitted. nullopt
    // when this worker hosts no state backends for the job.
    struct JobStateExport {
        std::vector<std::byte> bytes;
        std::size_t skipped_subtasks{0};
    };
    [[nodiscard]] std::optional<JobStateExport> export_job_state_arrow(JobId job_id) const;
    std::vector<SubtaskRecord> snapshot_subtasks() const;
    Config config_snapshot() const { return cfg_; }

private:
    void reader_loop_();
    // Fence and cancel the whole session exactly once. The old coordinator
    // session must stop touching external systems before a replacement
    // registers under the same worker id.
    void signal_disconnect_(const std::string& reason);
    // Fencing check, run on every control frame that changes what this
    // worker is doing. Returns false - meaning DROP the frame - when it
    // carries a lower epoch than the one bound at registration, so a
    // partitioned old leader cannot deploy, cancel, or commit behind the
    // current leader's back. A higher epoch re-binds, which is what a
    // failover looks like from here.
    [[nodiscard]] bool accept_epoch_(std::uint64_t frame_epoch, const char* what);

    // Dispatch one decoded control frame. Separate from reader_loop_ so a
    // decode throw costs one connection rather than the process.
    void dispatch_control_frame_(MessageReader& r);

    void handle_deploy_(MessageReader& r);
    void handle_peer_update_(MessageReader& r);
    void run_task_(JobId job_id,
                   const DeploymentTask& task,
                   const std::string& checkpoint_dir,
                   const std::string& restore_from_dir,
                   std::uint64_t restore_from_checkpoint_id,
                   std::uint32_t generation,
                   std::uint32_t restore_from_generation,
                   bool unaligned_checkpoints,
                   bool adaptive_barrier_mode,
                   const std::string& expected_state_versions_packed,
                   const std::string& udfs_packed = {});
    void run_generic_subtask_(JobId job_id,
                              const DeploymentTask& task,
                              const std::string& checkpoint_dir,
                              const std::string& restore_from_dir,
                              std::uint64_t restore_from_checkpoint_id,
                              std::uint32_t generation,
                              std::uint32_t restore_from_generation,
                              bool unaligned_checkpoints,
                              bool adaptive_barrier_mode,
                              const std::string& expected_state_versions_packed);
    void handle_trigger_checkpoint_(MessageReader& r);
    void handle_commit_checkpoint_(MessageReader& r);
    // The dispatch body handle_commit_checkpoint_ enqueues: sink commit
    // callbacks, CommitConfirmed sends, the committed high-water record,
    // retention and receipt pruning. Runs on commit_dispatch_ only.
    void dispatch_commit_checkpoint_(const CommitCheckpointMsg& msg);
    void commit_dispatch_loop_();
    // Reply to a source's RequestFinalCheckpoint: records the coordinator-assigned id and
    // wakes the blocked source runner (see request_final_checkpoint hook wiring).
    void handle_final_checkpoint_assigned_(MessageReader& r);
    // Dispatch AbortCheckpoint to per-subtask abort
    // callbacks. Mirrors handle_commit_checkpoint_; sinks register
    // their abort callback alongside their commit callback at
    // startup.
    void handle_abort_checkpoint_(MessageReader& r);
    // Dispatch BeginRescale to per-(job, op) drain
    // callbacks. The worker looks up callbacks registered against the
    // op_id in the BeginRescaleMsg payload and invokes them outside
    // the lock with the target_parallelism. Subtask runners
    // belonging to other operators are unaffected.
    void handle_begin_rescale_(MessageReader& r);
    void handle_cutover_peer_update_(MessageReader& r);
    void handle_cutover_rebind_(MessageReader& r);
    void handle_stop_subtasks_(MessageReader& r);
    // Sum the last snapshot size of every backend this worker hosts, per job, and
    // publish it as a gauge. Called from the heartbeat loop.
    void publish_job_state_sizes_();
    bool send_frame_(const std::vector<std::byte>& frame);
    void heartbeat_loop_();

    // Waits for the coordinator-supplied PeerUpdate for one in-flight subtask.
    // Returns nullopt if the wait was interrupted (worker stop / job cancel
    // / timeout). Any caller that gets nullopt must treat the task as
    // cancelled and report SubtaskFinished with had_error=true.
    struct ResolvedPeers {
        std::vector<PeerAddress> peers;
    };
    std::optional<ResolvedPeers> await_peer_update_(JobId job_id,
                                                    const std::string& role,
                                                    std::uint32_t subtask_idx);

    Config cfg_;
    std::string worker_id_;
    std::string data_host_;
    // coordinator connection target - populated by connect_to_coordinator so the
    // snapshot_worker() / /api/v1/worker endpoint can report it.
    std::string coordinator_host_;
    std::uint16_t coordinator_port_{0};
    std::unique_ptr<network::Connection> conn_;
    ConnectFactory connect_factory_;
    std::thread reader_;
    std::thread heartbeat_;
    // Commit dispatch runs on its OWN thread, fed FIFO by the reader. A
    // sink's commit work blocks on the external system - a Kafka
    // commit_transaction against an unreachable broker holds for the full
    // transaction timeout - and on the reader thread that stall freezes
    // frame processing AND the coordinator-contact clock the lease check
    // reads, so the worker self-disconnects mid-outage with its process
    // perfectly healthy (qual01-20260819g severed two live workers this
    // way, feeding the restart storm). Single consumer, so per-worker
    // commit order across checkpoints is preserved - the 2PC sinks'
    // open-transaction sequencing depends on it.
    std::thread commit_dispatch_;
    std::mutex commit_dispatch_mu_;
    std::condition_variable commit_dispatch_cv_;
    std::deque<CommitCheckpointMsg> commit_dispatch_queue_;
    bool commit_dispatch_stop_{false};
    // One entry per deployed subtask. `done` is set by the task thread
    // itself as it returns, so finished entries can be joined and erased
    // on the next deploy (reap_finished_task_threads_locked_).
    //
    // Without that reaping this vector only ever grew: every completed
    // subtask left an exited-but-unjoined std::thread behind for the
    // worker's whole lifetime, and on glibc an unjoined exited pthread
    // keeps its 8MB stack mapping until join. The symptom is virtual
    // memory climbing ~8MB per completed subtask while the live thread
    // count stays flat - which is exactly why a live-thread assertion
    // cannot see it, and why a long-running worker under job churn
    // (restarts, rescales, short jobs) drifts upward for days.
    struct TaskThread {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> done;
        // Identity, carried solely so a teardown that blocks can NAME what
        // it is blocked on. The joins below are deliberately unbounded;
        // that is only defensible if the wait is visible.
        JobId job_id{0};
        std::string role;
        std::uint32_t subtask_idx{0};
    };
    std::vector<TaskThread> task_threads_;
    // Subtasks that have not yet exited during a stop(), and when the
    // drain began. Published so "up but wedged draining" is visible from
    // outside the process: a worker stuck here never re-registers, and
    // the cluster then shows healthy containers and a job parked for
    // capacity that never comes back.
    std::atomic<std::size_t> draining_subtasks_{0};
    std::atomic<std::int64_t> drain_started_unix_{0};
    // Join + erase every task thread that has signalled completion.
    // Called with mu_ held. Joining an already-exited thread returns
    // immediately, and only self-reported-done entries are touched, so
    // this never blocks on running work.
    void reap_finished_task_threads_locked_();
    std::unordered_map<std::string, RoleHandler> roles_;
    bool deployed_{false};

    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> bound_epoch_{0};
    std::atomic<std::uint64_t> fenced_frames_{0};
    std::atomic<bool> cancelled_{false};
    std::atomic<bool> disconnected_{false};
    std::atomic<std::uint32_t> coordinator_protocol_version_{0};
    std::atomic<std::uint64_t> heartbeat_sequence_{0};
    std::atomic<std::uint64_t> heartbeat_ack_sequence_{0};
    std::atomic<std::int64_t> last_coordinator_contact_ms_{0};
    // True while the reader thread is inside dispatch_control_frame_. The
    // lease check treats an in-flight dispatch as contact: the reader
    // being busy processing the coordinator's OWN frame is evidence of
    // life, not silence, and a dispatch the OS stalls (Deploy writing +
    // dlopen'ing plugin bytes while first-execution scanning holds them -
    // the nine-worker gateway-pipeline flake) must not read as a dead
    // coordinator. Dead-coordinator detection is delayed by at most one
    // dispatch duration: the contact clock is restamped when dispatch
    // returns, and the next lease window runs from there.
    std::atomic<bool> dispatching_frame_{false};
    mutable std::mutex mu_;
    std::mutex send_mu_;
    std::condition_variable cv_;
    std::size_t in_flight_tasks_{0};

    // Bounded-source EOS final-checkpoint coordination (cluster path). A source
    // runner blocks in request_final_checkpoint() until the coordinator replies with the
    // assigned id (final_assigned_, keyed "job:role:subtask"), then blocks in
    // wait_final_committed() until this worker observes CommitCheckpoint for that id
    // (final_committed_high_water_, per job). Both waits are bounded; the reader
    // thread fills these and notifies final_ckpt_cv_.
    std::mutex final_ckpt_mu_;
    std::condition_variable final_ckpt_cv_;
    std::unordered_map<std::string, std::optional<std::uint64_t>> final_assigned_;
    std::unordered_map<JobId, std::uint64_t> final_committed_high_water_;

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
        // Topology generation this deploy writes, and the one that produced the
        // checkpoint it restores from. See docs/design/state-generations.md.
        std::uint32_t generation{1};
        std::uint32_t restore_from_generation{1};
        // Per-subtask state-backend URI (decoupled from checkpoint_dir).
        // Empty -> checkpoint_dir is the backend URI (legacy).
        std::string state_backend_uri;
        // Record-capture flight recorder (echoed from DeployMsg); empty = off.
        std::string capture_dir;
        std::uint64_t capture_records{0};
    };
    std::unordered_map<JobId, JobCheckpointState> per_job_checkpoint_;

    // Source-barrier injectors registered per running subtask. The
    // runner closure pushes its Dag::source_injectors() in here at
    // startup; the worker iterates them on TriggerCheckpoint to push a
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
    // that job. Avoids the deploy/trigger race where the coordinator fires a
    // periodic checkpoint while the worker is still bringing up the chain.
    // Queued (checkpoint_id, generation) pairs. Generation-stamped so the
    // replay-at-registration path can re-validate against the CURRENT deploy:
    // a deploy may land between queueing and replay, and replaying a trigger
    // stamped for the old generation into the new one is the follow-up 49
    // leftover (F84).
    std::unordered_map<JobId, std::vector<std::pair<std::uint64_t, std::uint64_t>>>
        pending_triggers_;
    // The generation each job is currently deployed at on this worker (F84).
    std::unordered_map<JobId, std::uint64_t> per_job_generation_;

    // Per-(job_id, subtask_idx) commit callbacks. Sinks implementing the
    // 2PC protocol register a callback at startup; CommitCheckpoint
    // dispatches it with the just-completed checkpoint_id. Late-bind
    // analog to per_job_injectors_ above.
    // The callback plus the gate that owns it, so a dispatch can drop
    // entries whose runner has retired. A retired entry can never run again;
    // keeping it means every later dispatch refuses, and a refusal blocks
    // confirmation for the whole subtask - so one subtask whose teardown hung
    // would stop the job confirming anything, for good.
    using CommitCallback = GatedCallback;
    std::unordered_map<JobId, std::unordered_map<std::uint32_t, std::vector<CommitCallback>>>
        per_job_committers_;

    // Checkpoint retention. per_job_backends_ holds each hosted subtask's
    // state backend (registered via RunnerContext::register_checkpoint_backend
    // at deploy); per_job_retention_ tracks the completed-checkpoint window
    // per job. On CommitCheckpoint the worker records the completed id and purges
    // any now-subsumed checkpoint from every hosted subtask backend so
    // checkpoint storage stays bounded. See CheckpointRetention.
    std::unordered_map<JobId, std::unordered_map<std::uint32_t, std::shared_ptr<StateBackend>>>
        per_job_backends_;
    std::unordered_map<JobId, CheckpointRetention> per_job_retention_;

    // Per-(job_id, subtask_idx) abort callbacks. Sinks
    // register an abort closure alongside their commit closure at
    // startup; the worker dispatches it on AbortCheckpoint. Same
    // signature shape as CommitCallback so callers can use the same
    // type alias and registration plumbing.
    using AbortCallback = GatedCallback;
    std::unordered_map<JobId, std::unordered_map<std::uint32_t, std::vector<AbortCallback>>>
        per_job_aborters_;

    // Per-(job_id, op_id) drain callbacks. Subtask
    // runners register one or more closures at startup against every
    // operator id their chain hosts (drain_registration_keys; custom-role
    // tasks fall back to the role, which IS their op id).
    // BeginRescale dispatch looks up by (job_id, op_id) and invokes
    // every closure with the target_parallelism; the closure runs
    // the drain choreography (emit DrainMarker, close output) on
    // the subtask thread. Late dispatch for an already-shutdown
    // subtask finds no callbacks registered and silently no-ops -
    // same idempotency story as commit/abort.
    using DrainCallback = std::function<void(std::uint32_t target_parallelism)>;
    std::unordered_map<JobId, std::unordered_map<std::string, std::vector<DrainCallback>>>
        per_job_drain_callbacks_;

    // Per-(job_id, op_id) armed-cutover callbacks, keyed exactly like the
    // drain map. A BeginRescale that names a cutover checkpoint dispatches
    // THESE with the id (stop exactly at barrier C); one that names none
    // dispatches the drain callbacks above (stop now). Same registration
    // path, same idempotency story.
    using CutoverArmCallback = std::function<void(std::uint64_t cutover_checkpoint_id)>;
    std::unordered_map<JobId, std::unordered_map<std::string, std::vector<CutoverArmCallback>>>
        per_job_arm_callbacks_;

    // Per-(job_id, downstream op) hold-and-swap hooks for rescale-eligible
    // output groups, registered by the output-attach path with the identity
    // of the FEEDING task that owns each group. BeginRescale's arm reaches
    // the gates through the arm map above (the registration adds a
    // gate-arming callback there too); CutoverPeerUpdate carries per-task
    // endpoint lists (each feeder's branches connect to different ports on
    // the new subtasks) and dispatches each list to its task's hooks here.
    struct RegisteredGroupCutover {
        std::string task_role;
        std::uint32_t task_subtask_idx{0};
        RunnerContext::GroupCutoverHooks hooks;
    };
    std::unordered_map<JobId, std::unordered_map<std::string, std::vector<RegisteredGroupCutover>>>
        per_job_group_cutovers_;

    // Per-(job_id, UPSTREAM op) input-rebind hooks, registered by the
    // input-attach path of tasks whose fan-shaped inputs come from a
    // rescale-eligible operator. CutoverRebind dispatch binds one new
    // listener per new upstream subtask through these and reports the
    // ports via a mid-run SubtaskListening. Task identity rides along so
    // the report names the task the ports belong to.
    struct RegisteredInputRebind {
        std::string task_role;
        std::uint32_t task_subtask_idx{0};
        RunnerContext::InputRebindHooks hooks;
    };
    std::unordered_map<JobId, std::unordered_map<std::string, std::vector<RegisteredInputRebind>>>
        per_job_rebinds_;

    // Pump threads feeding rebound inputs (one per bound listener),
    // worker-owned: cancelled at CancelJob for their job and at stop().
    // jthread joins on destruction, so cancel MUST run first - the pump
    // blocks in the relay's pop until the peer closes or cancel wakes it.
    struct RebindPump {
        std::function<void()> cancel;
        std::jthread thread;
    };
    std::unordered_map<JobId, std::vector<RebindPump>> per_job_rebind_pumps_;

    void cancel_rebind_pumps_locked_(JobId job_id);

    // Graceful-stop closures, per job. Not keyed by role, unlike the drain map:
    // a stop addresses the WHOLE job, so there is nothing to select on. Same
    // idempotency story - a late dispatch for an already-finished subtask finds
    // nothing registered and no-ops.
    using StopCallback = std::function<void()>;
    std::unordered_map<JobId, std::vector<StopCallback>> per_job_stop_callbacks_;

    // Per-job registry bundle on the worker. Plugin .so bytes shipped with
    // each Deploy are dlopened into THIS job's bundle (instead of the
    // worker-process-wide default singletons), so two concurrent jobs that
    // mint overlapping _inline_<kind>_<n> names don't trample each
    // other. Subtask runner lookups go through this bundle's
    // RunnerRegistry; built-in lookups fall through to the default
    // singletons via the bundle's parent pointer.
    std::unordered_map<JobId, std::unique_ptr<JobBundle>> per_job_bundle_;

    // Per-(job, subtask_idx) cancel token. The worker allocates the
    // shared_ptr<atomic<bool>> in run_task_ before invoking the
    // runner and threads it through RunnerContext::cancel_token so
    // the LocalExecutor's stop predicate sees it. On CancelJob the
    // handler walks the map for the job_id and sets every token to
    // true; running executors observe the flip and wind down. Held
    // by shared_ptr so the runner's executor capture stays valid
    // even after the run_task_ stack frame returns.
    std::unordered_map<JobId, std::unordered_map<std::uint32_t, std::shared_ptr<std::atomic<bool>>>>
        per_job_cancel_tokens_;
    // Jobs whose CancelJob has been processed. The flip above only reaches
    // tokens ALREADY registered, and task construction runs on task threads:
    // a task that finishes constructing after the flip used to register a
    // fresh token nobody would ever set and run on as an orphan of a
    // cancelled deployment (followups item 75b - it parked a 292-task job's
    // fail-by-counting completion at 291/292 forever). run_task_ checks this
    // latch at registration and starts such a task pre-cancelled. A later
    // Deploy for the same job id clears the latch: a whole-job restart
    // redeploys under the same id and its tasks must run. Guarded by mu_.
    std::unordered_set<JobId> cancelled_jobs_;
};

}  // namespace clink::cluster
