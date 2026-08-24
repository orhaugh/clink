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
#include <set>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/checkpoint/adaptive_mode_policy.hpp"
#include "clink/cluster/autoscaler.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/rescale_coordinator.hpp"
#include "clink/cluster/rescale_dispatch.hpp"
#include "clink/cluster/snapshots.hpp"
#include "clink/lineage/lineage_graph.hpp"
#include "clink/runtime/network/connection.hpp"

namespace clink::cluster {

struct JobGraphSpec;
class OperatorRegistry;
class JobBundle;

// One task in the user-supplied job plan. peer_refs are resolved against
// the rest of the plan at deployment time to compute the actual host:port
// that the task should connect to for each cross-stage data channel.
struct PlannedTask {
    std::string worker_id;
    std::string role;
    std::uint32_t subtask_idx{};
    std::uint16_t data_port{};  // 0 if no inbound listener
    std::vector<std::pair<std::string /*role*/, std::uint32_t /*subtask*/>> peer_refs;
    std::string extra_config;
    // True when this task's chain hosts a sink whose external commit
    // cannot be re-executed after a crash (guarantee_gate::
    // op_type_needs_commit_confirmation). The coordinator tracks such
    // tasks per checkpoint and gates CONFIRMED-N markers on their
    // CommitConfirmed messages - the commit-confirmed restore protocol.
    bool needs_commit_confirmation{false};
    // The slice of the key space this task's keyed state belongs to,
    // [first, last). Set by the planner, which is the only place that knows
    // both numbers it depends on: the task's index WITHIN its operator, and
    // that operator's parallelism.
    //
    // Deploy used to derive this itself, from the task's GLOBAL index and a
    // count of every task sharing its role. Every operator deployed through
    // the generic subtask role shares one role name, so a three-operator job
    // of three parallelism-1 operators had the key space split three ways
    // between DIFFERENT operators - a keyed operator that should own all 128
    // groups owned 43 of them. Keyed state for the other 85 was written
    // happily and discarded at restore, silently. See F38.
    //
    // {0, 0} means unset, and the worker widens that to the full range - a
    // task that restores everything is wrong in the direction that keeps
    // data, unlike one that restores a third of it.
    std::uint32_t key_group_first{};
    std::uint32_t key_group_last{};
    // Which operator this task hosts, and which of that operator's subtasks
    // it is. Both are known to the planner - the chain head's id and the loop
    // index - and neither is recoverable from the fields above, because every
    // task carries the same role and `subtask_idx` counts across the whole job.
    //
    // The coordinator needs them to reason about an operator at all. A rescale
    // has to map each new subtask onto the parent snapshot it restores from,
    // and that mapping is a function of the index WITHIN the operator and that
    // operator's old and new parallelism. Using the global index instead is the
    // same mistake as F38 and gives the wrong parent for every operator after
    // the first.
    //
    // Empty / 0 for tasks that do not come from the chain planner (the
    // in-process test API and queryable-state routing build their own plans).
    // Callers must treat an empty op_id as "unknown", not as a match.
    std::string op_id;
    std::uint32_t subtask_idx_in_op{};
    // Rescale restore directive, mirrored onto the DeploymentTask at
    // dispatch. kRestoreFromSelf = no override (ordinary deploys); the hot
    // cutover planner sets the parents' deployed global index and count.
    std::uint32_t restore_from_subtask_idx{kRestoreFromSelf};
    std::uint32_t restore_from_parent_count{1};
};

struct JobPlan {
    std::vector<PlannedTask> tasks;
};

// One worker's identity and remaining capacity, as placement sees it.
struct PlacementWorker {
    std::string worker_id;
    std::uint32_t free_slots{};
};

// Assign a worker to every task whose worker_id is empty, CO-LOCATING the tasks that share a
// subtask index - one parallel pipeline instance per worker.
//
// Why co-locate: tasks with the same subtask index are joined by FORWARD edges (subtask i of
// the source feeds subtask i of the projection), and the data plane hands a batch across a
// forward edge as a pointer when both ends are in one process and serialises it over a socket
// when they are not. Placing one task at a time meant subtask i's operators landed on
// different hosts: on a 3-worker rig at parallelism 12, nexmark q0 - four operators, forward
// edges only, no shuffle - sent 67% of its data-plane edges over TCP.
//
// Groups are taken round-robin so instances still spread evenly, and workers are visited in
// sorted worker-id order so placement is reproducible across deploys of the same plan.
// A hash-shuffled edge is unaffected (subtask i sends to every downstream subtask either way),
// so this can only convert forward edges from remote to local.
//
// An instance too large for any single worker is split across whatever capacity is free:
// splitting is worse than co-locating but far better than refusing to deploy. Returns false
// when capacity runs out entirely, having assigned what it could.
//
// Exposed (rather than left inline in deploy) so the placement contract can be tested without
// standing up a cluster. `workers` is updated in place to reflect the capacity consumed.
[[nodiscard]] bool assign_task_placement(std::vector<PlannedTask>& tasks,
                                         std::vector<PlacementWorker>& workers);

// CompletedJobRecord - HistoryServer entry. Captured at job
// termination and kept in a bounded ring buffer on the coordinator so operators
// can answer "what happened to job N?" even after the live JobState
// is gone. The history ring holds the last `kCoordinatorHistoryCap`
// terminal events; older entries are evicted from the front.
//
// `status` is one of "ok", "failed", "cancelled" (matching the metric
// emitted by signal_job_completion_locked_).
struct CompletedJobRecord {
    JobId job_id{};
    std::string status;
    std::vector<std::string> errors;
    std::uint32_t restart_attempts{};
    std::uint64_t latest_completed_checkpoint_id{};
    std::chrono::milliseconds duration_ms{};
    // Wall-clock seconds-since-epoch when the job terminated. Kept as
    // a plain integer so the value survives serialization without a
    // timezone story; consumers format with std::localtime if needed.
    std::int64_t completed_at_unix_seconds{};
};

// Maximum number of completed-job records the coordinator retains. Bounded so
// long-running clusters don't grow memory without bound; pick a number
// big enough for "what failed last week?" but small enough to stay
// cheap.
inline constexpr std::size_t kCoordinatorHistoryCap = 128;

// Default control-plane port. The coordinator defaults to 6123; clink uses
// the same port so operators familiar with  can reach for the same
// muscle memory.
inline constexpr std::uint16_t kDefaultCoordinatorPort = 6123;

// Coordinator is the cluster's single source of truth for deployment.
//
// It supports two ways to drive it:
//   * In-process: call `expect_workers(...)`, `await_registrations(...)`,
//     `deploy(plan)`, `await_completion(...)`. This is the original test
//     harness path; it implicitly runs one job at a time.
//   * Over the wire: clients connect, send HelloClient + SubmitJob with a
//     JobGraphSpec, and the coordinator plans/deploys/tracks/reports completion
//     entirely via the protocol. Multiple jobs can be in flight at once
//     so long as the cluster has spare slots.

// Apply a cluster-level default state-backend URI to a job's CheckpointConfig
// when the submitter chose none. A non-empty checkpoint.state_backend_uri (a
// per-job --state-backend) is preserved; an empty default_uri is a no-op, so
// the legacy resolution stands (empty -> memory, bare checkpoint_dir -> file).
// checkpoint_dir is NOT a backend choice (it doubles as the HA/coordination
// dir), so a job that set only checkpoint_dir still receives the default -
// letting an operator point HA-enabled jobs at a durable deferring tier
// (remote-read://). Exposed for direct testing of the submit-time policy.
void apply_default_state_backend(CheckpointConfig& checkpoint, const std::string& default_uri);

// Pin a recovered job's state-backend URI to the backend it ALREADY ran with,
// so HA recovery never re-applies a cluster default that may have been
// configured AFTER the job was submitted (which would silently rebind the job
// - e.g. a checkpoint_dir-durable job onto a non-durable default - and abandon
// its checkpoints). The manifest stores the per-job choice verbatim; an empty
// value means the job used the legacy resolution (checkpoint_dir -> file, else
// memory), so resolve that explicitly here. Run before submit_job during
// recovery so apply_default_state_backend then sees a non-empty URI and is a
// no-op. Exposed for direct testing of the recovery policy.
void pin_recovered_state_backend(CheckpointConfig& checkpoint);

// Read the fencing epoch recorded in a control-plane metadata file, or 0
// when the file is absent, unreadable, or predates fencing.
[[nodiscard]] std::uint64_t metadata_stored_epoch(const std::string& path);

// The body-shaped form of the same extractor: the coordinator_epoch stamped
// inside a manifest/history record. This is what the coordination store's
// fenced_put takes - an object store fences on the record it read back, not
// on a filesystem path.
[[nodiscard]] std::uint64_t metadata_epoch_in_body(const std::string& body);

// The metadata fencing rule: a coordinator may overwrite a record only if
// the record was not written by a LATER epoch than its own. Exposed
// (alongside the two policies above) so the rule can be tested without
// standing up two coordinators.
//
// Read-then-write, not an atomic compare-and-set: two writers racing inside
// the window between read and rename can both pass. What it closes is the
// realistic shape - a partitioned leader is stale for seconds or minutes,
// and every write it attempts reads back an epoch above its own.
[[nodiscard]] inline bool metadata_write_allowed(std::uint64_t writer_epoch,
                                                 std::uint64_t stored_epoch) noexcept {
    return stored_epoch <= writer_epoch;
}

class Coordinator {
public:
    struct Config {
        // How often the watchdog thread re-evaluates worker liveness.
        std::chrono::milliseconds watchdog_interval{100};
        // A worker is declared lost if no message has arrived from it for
        // longer than this. Heartbeats from a healthy worker should have a
        // shorter interval - typically heartbeat_timeout / 3 - so a
        // single missed message doesn't trigger a false positive.
        std::chrono::milliseconds heartbeat_timeout{2000};
        // Interface to bind the control-plane listener on. Default is
        // loopback (single-host tests). Set to "0.0.0.0" or a specific
        // NIC address for multi-machine clusters. Pair with TLS for any
        // deployment beyond a trusted local network.
        std::string bind_host{"127.0.0.1"};
        // Most client connections the coordinator will service at once.
        // Beyond this a new client is refused with a reason rather than
        // being accepted into an unbounded thread pool. Generous by
        // default - a CLI is short-lived and a dashboard holds one - so
        // reaching it means something is wrong rather than busy.
        std::size_t max_client_connections{256};
        // Most WORKER connections the coordinator will hold at once. Beyond this
        // a registration is refused with a reason rather than admitted into a
        // thread the coordinator cannot account for - the same contract as the
        // client cap above, on the path that had none.
        //
        // The anchor is the engine's own ceiling rather than taste: a keyed
        // operator cannot be split finer than kNumKeyGroups = 128 ways, so 128
        // single-slot workers already saturate the maximum keyed parallelism of
        // ONE operator. Eight times that leaves room for concurrent jobs and
        // multi-operator graphs. The multiplier is headroom and nothing more;
        // what is derived is the anchor, and the direction - strictly above 128
        // rather than at it.
        std::size_t max_worker_connections{1024};
        // Host advertised to clients/peers in resolved peer addresses
        // when bind_host is a wildcard. Defaults to bind_host. Set to a
        // routable hostname/IP when bind_host = "0.0.0.0".
        std::string advertise_host{};
        // Maximum number of restart attempts per failing task before the
        // coordinator gives up and surfaces the failure. 0 = no retries.
        int max_restarts{0};
        // Upper bound on how long a job may sit in awaiting_restart while
        // surviving subtasks drain before the redeploy fires. A drain that
        // never completes (e.g. a survivor that is hung - neither acking the
        // cancel nor dying) would otherwise wedge the job forever. On expiry
        // the watchdog FAILS the job (it does not force a restart, because a
        // still-alive-but-slow survivor must not be redeployed concurrently).
        // The bound must dominate the worst LEGITIMATE drain, not the
        // typical one: a 2PC sink cancelled mid-call against an unreachable
        // broker sits in a bounded librdkafka operation (produce, flush,
        // commit - up to ~30s each, and they stack) before it can observe
        // the cancel. That sink is slow, not hung - it exits the moment its
        // own timeout fires - and the old 30s default read exactly that as
        // a wedged survivor and failed the job mid-broker-outage (soak
        // watch item 63; reproduced locally by the orphaned-commit gate:
        // two sinks blocked on a paused broker, drain expired, job dead).
        std::chrono::milliseconds restart_drain_timeout{120000};
        // How long the SubmitJob handler waits for spare slots before
        // returning a rejection ack to the client. 0 means "never wait,
        // reject immediately". Useful when clusters auto-scale.
        std::chrono::milliseconds submit_wait_for_slots{0};
        // Hot rescale (design record 008). When enabled, an eligible
        // per-operator rescale runs as an in-place cutover at a checkpoint
        // barrier - only the rescaled operator's subtasks cycle - and every
        // ineligible or failed attempt falls back to the stop-the-world
        // replan. Off = always replan (the proven path).
        bool hot_rescale_enabled{true};
        // Bound on each hot-cutover phase (arming acks, the cutover
        // checkpoint + drains, rebind ports, new-task listenings). Expiry
        // aborts the cutover to the replan path. Generous because the
        // deploy phase includes a state restore.
        std::chrono::milliseconds hot_cutover_phase_timeout{60000};
        // How many CONSECUTIVE whole-job restarts triggered by FAILED
        // checkpoints - with no completed checkpoint between them - the
        // coordinator tolerates before failing the job with the cause.
        // A failed checkpoint's restart rewinds and re-emits an interval
        // through every sink, so a PERSISTENT cause (a state volume at
        // ENOSPC, above all) used to crashloop indefinitely: QUAL-09
        // measured ~100 rewind-restarts in 35 minutes, each one visibly
        // shrinking upsert-sink output, bounded only by a restart budget
        // sized for worker loss (followups item 77b). Rewinding into a
        // cause that does not heal repairs nothing; failing loudly with
        // the cause is the safe escalation, exactly as the drain deadline
        // concluded. 0 disables (the pre-77b behaviour).
        std::uint32_t checkpoint_failure_restart_limit{5};
        // Cluster-level default state-backend URI applied to a submitted job
        // that chose none (empty CheckpointConfig.state_backend_uri). Lets an
        // operator point every job at a deferring backend (e.g.
        // remote-read://bucket) so the async/disaggregated execution path
        // activates by default, without each job specifying it; a per-job
        // --state-backend still wins. Applied before the HA manifest is
        // persisted, so the resolved URI survives recovery. Empty (the
        // default) preserves the legacy resolution: empty -> memory, bare
        // checkpoint_dir -> file. WARNING: disagg-local:// is process-local and
        // NOT durable across a restart - safe only for dev/test; production
        // clusters should set a durable tier (remote-read:// on S3).
        std::string default_state_backend_uri{};
        // Optional adaptive autoscaler config. When set,
        // every submitted job whose graph declares at least one
        // operator with [min, max] bounds spawns a per-job
        // Autoscaler thread that polls the sample function on this
        // cadence and calls request_operator_rescale on overload /
        // underload. Default (nullopt) disables autoscaling - manual
        // rescale via clink rescale-op still works.
        std::optional<AutoscalerConfig> autoscaler;
        // HA recovery placement: how long recover_persisted_jobs waits for
        // worker registrations to stop arriving before redeploying, and the
        // hard bound on that wait. Workers reconnect within moments of a new
        // leader binding, but not simultaneously; a redeploy fired at the
        // first registration schedules the whole job onto that one worker
        // (QUAL-01 run C ran its final minutes with all sixteen tasks on one
        // worker for exactly this reason). The settle wait ends as soon as
        // the registered set has been stable for recovery_worker_settle,
        // and unconditionally at recovery_worker_settle_deadline, so a
        // genuinely single-worker cluster still recovers promptly. Zero
        // settle disables the wait.
        std::chrono::milliseconds recovery_worker_settle{1000};
        std::chrono::milliseconds recovery_worker_settle_deadline{5000};
    };

    Coordinator();
    explicit Coordinator(Config cfg);
    ~Coordinator();

    Coordinator(const Coordinator&) = delete;
    Coordinator& operator=(const Coordinator&) = delete;
    Coordinator(Coordinator&&) = delete;
    Coordinator& operator=(Coordinator&&) = delete;

    // Bind the control-plane listener and start the accept/watchdog
    // threads. `port = 0` lets the OS pick (good for tests). Returns the
    // bound port.
    std::uint16_t start(std::uint16_t port = 0);

    // ----- Legacy single-job API (test harness path) -----

    // Declare the set of worker ids that must register before deploy() can
    // proceed. Optional; new submission flow doesn't need it.
    void expect_workers(std::vector<std::string> worker_ids);

    // Block until every expected worker has registered, or `timeout` elapses.
    bool await_registrations(std::chrono::milliseconds timeout);

    // Resolve peer addresses and dispatch Deploy messages directly.
    // Auto-assigns a JobId.
    void deploy(const JobPlan& plan);

    // Block until every task in the legacy in-process job has reported
    // SubtaskFinished, or timeout elapses.
    bool await_completion(std::chrono::milliseconds timeout);

    // Errors collected from the legacy in-process job.
    std::vector<std::string> errors() const;

    // ----- New submission API (multi-job, port-discovery) -----

    // Submit a JobGraphSpec for execution. Returns the assigned JobId on
    // success. Throws std::runtime_error if the graph is invalid or the
    // cluster has insufficient spare slots (the wait policy is
    // controlled by Config::submit_wait_for_slots).
    //
    // If `notify_client_conn != nullptr`, the coordinator will send a
    // JobCompletedMsg back on that connection when the job finishes.
    // Pass nullptr for in-process submitters that poll via
    // await_job_completion.
    JobId submit_job(const JobGraphSpec& graph,
                     const OperatorRegistry& registry,
                     network::Connection* notify_client_conn = nullptr);

    // Overload that accepts plugin binaries to ship with every Deploy
    // for this job. The coordinator also writes them to its local cache and
    // dlopens them before planning so the registry-based validation
    // can see plugin-defined op types.
    JobId submit_job(const JobGraphSpec& graph,
                     const OperatorRegistry& registry,
                     std::vector<PluginBinary> plugins,
                     network::Connection* notify_client_conn = nullptr);

    // Overload that also carries distributed-checkpointing config (root
    // directory, periodic-trigger interval, optional restore directive).
    // Each subtask gets a private FileBackedStateBackend rooted under
    // checkpoint_dir on its worker; the coordinator's coordinator triggers periodic
    // barriers across the source subtasks at interval_ms.
    JobId submit_job(const JobGraphSpec& graph,
                     const OperatorRegistry& registry,
                     std::vector<PluginBinary> plugins,
                     CheckpointConfig checkpoint,
                     network::Connection* notify_client_conn = nullptr);

    // Per-job-bundle overload. The caller has already constructed a
    // JobBundle and loaded the job's plugin .so's into its
    // PluginRegistry view; this overload threads that bundle through
    // plan_job (for per-job validation) and stashes it in JobState
    // (so the coordinator-side dispatcher / coordinator can reach it later).
    // Used by handle_submit_; in-process tests still call the simpler
    // overloads above.
    JobId submit_job(const JobGraphSpec& graph,
                     const OperatorRegistry& registry,
                     std::vector<PluginBinary> plugins,
                     CheckpointConfig checkpoint,
                     std::unique_ptr<JobBundle> bundle,
                     network::Connection* notify_client_conn = nullptr);

    // Block until the named job's tasks have all reported, or timeout.
    bool await_job_completion(JobId job_id, std::chrono::milliseconds timeout);

    // Errors for one job (empty == clean run).
    std::vector<std::string> job_errors(JobId job_id) const;

    // History-server snapshot. Returns the terminal-state record for
    // every job the coordinator has seen complete, capped at
    // kCoordinatorHistoryCap entries (oldest evicted first). Stable
    // copy - safe to inspect/serialize outside the coordinator mutex. Mirrors
    // the read surface HistoryServer exposes via /jobs/overview
    // for completed jobs.
    std::vector<CompletedJobRecord> job_history() const;

    // Convenience: look up one job by id from the history ring. Returns
    // std::nullopt if no terminal record is present (job still running,
    // never existed, or evicted from the ring).
    std::optional<CompletedJobRecord> job_history(JobId job_id) const;

    // Cluster-wide free slot count (sum across all registered workers minus
    // tasks currently in flight). Useful for tests that want to assert
    // on slot accounting.
    std::size_t free_slots() const;

    // ----- Cluster state queries -----

    // workers that the watchdog has declared lost (in registration order).
    std::vector<std::string> lost_workers() const;

    // Close the listener and all connections.
    void stop();

    std::uint16_t bound_port() const noexcept { return bound_port_; }

    // Direct config access for the /api/v1/config endpoint. Returns a
    // copy so callers can serialize outside the mutex.
    Config config_snapshot() const { return cfg_; }

    // ----- Snapshot API for the HTTP read endpoints -----
    //
    // Each method takes mu_ briefly, copies state into a plain
    // value-type (snapshots.hpp), then releases. Callers serialize to
    // JSON outside the critical section so HTTP threads don't contend
    // with control-plane writes.
    ClusterSnapshot snapshot_cluster() const;
    std::vector<WorkerSummary> snapshot_workers() const;
    std::vector<JobSummary> snapshot_jobs() const;
    // nullopt if job_id isn't a known job.
    std::optional<JobDetail> snapshot_job(JobId job_id) const;

    // Logical DAG + subtask placement for GET /api/v1/jobs/:id/graph. nullopt
    // if job_id isn't known; a detail with available=false when the job exists
    // but no graph was retained (e.g. submitted before graph retention).
    std::optional<JobGraphDetail> snapshot_job_graph(JobId job_id) const;

    // Data-lineage view for GET /api/v1/jobs/:id/lineage: the external
    // datasets the job reads from and writes to, derived from the retained
    // JobGraphSpec. nullopt if job_id isn't known; an empty graph when the
    // job exists but no graph was retained.
    std::optional<lineage::LineageGraph> snapshot_job_lineage(JobId job_id) const;

    // (data_host, http_port) for the worker with the given id, or nullopt
    // if the worker isn't registered, is lost, or didn't enable HTTP.
    // Used by the coordinator dashboard's proxy routes (/api/v1/workers/:id/*).
    std::optional<std::pair<std::string, std::uint16_t>> worker_http_target(
        const std::string& worker_id) const;

    // Unique worker HTTP targets (data_host, http_port) hosting any
    // subtask of `job_id`. Returns empty if the job is unknown, has
    // no placed subtasks, or every hosting worker has dropped HTTP.
    // Used by the Queryable State multi-worker locate endpoint so a
    // client can iterate the workers holding a job's keyed state slots.
    std::vector<std::pair<std::string, std::uint16_t>> workers_hosting_job(JobId job_id) const;

    // Key-group-aware routing for Queryable State. Given a job, role,
    // and serialized key, returns the worker HTTP target hosting the
    // subtask responsible for that key's key-group plus the subtask
    // index. nullopt if the job/role is unknown, no subtask covers
    // the key's group, or the hosting worker has dropped HTTP. The {0,0}
    // sentinel on key_group_first/last (non-rescaled deploys) is
    // expanded to the full [0, kNumKeyGroups) range, matching the
    // back-compat behaviour used by the restore-side filter.
    struct RouteTarget {
        std::string host;
        std::uint16_t port{};
        std::uint32_t subtask_idx{};
    };
    std::optional<RouteTarget> route_key_for_job(JobId job_id,
                                                 const std::string& role,
                                                 std::span<const std::byte> key_bytes) const;

    // Every (host, http_port, subtask_idx) currently hosting a subtask of
    // `role` for `job_id`, in subtask order. workers that are lost or expose
    // no HTTP port are skipped. Used by the queryable-state JSON serving
    // route to fan a key lookup out across the role's subtasks - correct
    // at any parallelism without reproducing the shuffle's key hashing on
    // the coordinator (the key-group fast path is route_key_for_job).
    [[nodiscard]] std::vector<RouteTarget> subtask_targets_for_role(JobId job_id,
                                                                    const std::string& role) const;

    // Per-job topology version. Returns 0 if the job is unknown.
    // Incremented at initial deploy (-> 1) and on every successful
    // rescale. Used by Queryable State clients to invalidate cached
    // routes when the topology shifts (rescale moves key-groups to
    // different subtasks, so a cached (kg -> WorkerTarget) entry from
    // before the rescale is stale).
    [[nodiscard]] std::uint64_t topology_version(JobId job_id) const;

    // Public cancel_job: same logic as the client-wire-protocol path
    // in handle_cancel_job_, factored out so the HTTP action endpoint
    // (POST /api/v1/jobs/:id/cancel) and (eventually) in-process
    // callers can request a cancel without forging a wire frame.
    // Returns the same ack struct the wire path emits.
    CancelJobAckMsg cancel_job(JobId job_id);

    // Hot rescale: change one or more roles' parallelism while keeping
    // the job running and preserving its keyed state. v1 supports only
    // integer scale-up (new_p must be a positive multiple of current
    // p); scale-down would have to merge multiple parent state files
    // into one new subtask, deferred. Synchronous: blocks until the
    // checkpoint + drain + redeploy chain finishes (or fails).
    //
    // Returns a RescaleJobAckMsg with ok=false + a message describing
    // why the request was rejected (no such job, bad parallelism, no
    // free slots, checkpoint failed, etc.).
    RescaleJobAckMsg rescale_job(JobId job_id,
                                 const std::unordered_map<std::string, std::uint32_t>& role_p);

    // Per-operator rescale request. Validates the new
    // parallelism against the operator's [min, max] bounds
    // (registered at submit time), refuses if a rescale is already in
    // progress for the operator, and on accept transitions the
    // operator's RescaleCoordinator state to Preparing. The actual
    // BeginRescale broadcast + drain choreography is the follow-on
    // slice; this method is the public surface a future REST
    // endpoint or autoscaler thread will call.
    //
    // Returns the underlying RescaleCoordinator::RequestResult. On
    // reject, .ok=false and .reason carries a descriptive message;
    // on accept, .ok=true and .accepted_target reflects the new
    // parallelism.
    RescaleCoordinator::RequestResult request_operator_rescale(JobId job_id,
                                                               const std::string& op_id,
                                                               std::uint32_t new_parallelism);

    // Look up an operator's current rescale state. Returns nullopt
    // if either the job or the operator is unknown. Useful for
    // dashboards / tests verifying state transitions.
    std::optional<OperatorRescaleStatus> operator_rescale_status(JobId job_id,
                                                                 const std::string& op_id) const;

    // Install the metric source the per-job autoscaler
    // consumes. Signature mirrors Autoscaler::SampleFn but adds JobId
    // so one global metric source can serve every job's operators.
    // Default (when not set) returns 0.5 (mid-band, no rescale) so
    // an autoscaler with no metric source idles harmlessly. Must be
    // called BEFORE any job is submitted; the per-job Autoscaler
    // captures the function by value at construction.
    using AutoscalerSampleFn = std::function<double(JobId, const std::string&)>;
    void set_autoscaler_sample_fn(AutoscalerSampleFn fn);

    // Diagnostic accessor. Returns the per-job autoscaler's
    // tick counter, or nullopt if the job has no autoscaler attached.
    // Used by tests / dashboards to confirm the loop is alive.
    std::optional<std::uint64_t> autoscaler_ticks(JobId job_id) const;

    // Trigger a savepoint: one-off synchronous checkpoint that
    // returns a stable (dir, id) handle. The caller can hand the
    // handle to a future clink_submit_job invocation via
    // --restore-from-dir + --restore-from-checkpoint-id to start a
    // fresh job from this point - the // ` savepoint <jobid>`. v1 returns the in-place checkpoint
    // location; physical relocation to a portable dir is the
    // operator's responsibility (rsync, S3 copy, etc.).
    //
    // `timeout` bounds how long the coordinator waits for every subtask to
    // ack the savepoint. 0 picks a 30s default.
    SavepointAckMsg take_savepoint(JobId job_id, std::chrono::milliseconds timeout = {});

    // Stop a running job GRACEFULLY and report the checkpoint to resume from.
    //
    // Tells every subtask to stop producing and run its end-of-input path, which
    // is where the runners take a coordinator-coordinated final checkpoint,
    // block until the sinks have committed it, and only then report finished. So
    // the job ends as a SUCCESS with its tail durable, and the returned
    // checkpoint id is what a resubmit restores from.
    //
    // Distinct from cancel_job, which does not drain: everything since the last
    // completed checkpoint is discarded and replays on restart. Both are wanted -
    // a cancel has to stay abrupt - so this is a separate operation, not a flag.
    StopJobAckMsg stop_job(JobId job_id, std::chrono::milliseconds timeout = {});

    // Override the factory used to wrap each accepted int fd into a
    // Connection. Default = plain-TCP; clink_node installs a TLS
    // factory when --tls-cert is given. Must be called before start().
    using AcceptFactory = std::function<std::unique_ptr<network::Connection>(int listener_fd)>;
    void set_accept_factory(AcceptFactory f);

    // Enable HA mode: every submitted job is persisted under <dir>/jobs/
    // (manifest.json + plugin-<hash>.so bytes). On standby->leader
    // takeover, recover_from_ha_dir replays every persisted job into
    // the coordinator's in-memory state, attaching restore_from at the latest
    // COMPLETED-N marker for that job. Must be called before start().
    void set_ha_dir(std::string dir);

    // Fencing epoch, bumped by HaCoordinator on every leadership
    // acquisition and set here by whoever drives election (clink_node).
    //
    // Stamped on every control message this coordinator sends, so a
    // worker can refuse a frame from a coordinator that has since been
    // superseded. Zero - the default, and what a non-HA single-coordinator
    // deployment leaves it at - means "unfenced" and reproduces the
    // pre-fencing behaviour exactly.
    //
    // Without this the epoch existed only in the leader-endpoint file and
    // was read by nothing: a partitioned old leader that still believed it
    // held the lock could deploy jobs, cancel jobs and broadcast sink
    // commits behind the new leader's back, with no mechanism anywhere to
    // notice.
    void set_epoch(std::uint64_t epoch) noexcept { epoch_.store(epoch, std::memory_order_release); }
    [[nodiscard]] std::uint64_t epoch() const noexcept {
        return epoch_.load(std::memory_order_acquire);
    }

    // The job's recovery point: the highest checkpoint that reached
    // GLOBAL completion. This is the id a restore would use, so it is
    // the value a test asserting "a failed checkpoint did not become the
    // recovery point" has to read. Zero for an unknown job or one that
    // has not completed a checkpoint.
    [[nodiscard]] std::uint64_t latest_completed_checkpoint(JobId job_id) const;

    // The latest checkpoint whose external commits are PROVEN executed.
    //
    // For a job with commit-confirming sinks this - not the completed id
    // above - is what a restart picks as its restore point, so it decides
    // where an exactly-once job resumes from. It was invisible until
    // QUAL-01 needed it: a job had inherited a previous job's confirmed
    // id from markers left in a reused checkpoint directory, and the only
    // way to see it was a log line printed during a restart.
    [[nodiscard]] std::uint64_t latest_confirmed_checkpoint(JobId job_id) const;

    // Client sessions currently held, live or awaiting reaping.
    //
    // Exposed because "the coordinator does not leak a thread per
    // client" is not observable from outside otherwise, and it was not
    // true: sessions were only ever dropped by stop(). A test that
    // connects and disconnects repeatedly watches this rather than
    // inferring health from the absence of a crash.
    [[nodiscard]] std::size_t client_session_count() const {
        std::lock_guard lock(client_mu_);
        return client_sessions_.size();
    }

    // Workers still HOLDING a connection, as distinct from workers on record.
    // The gap between the two is the point: a lost worker keeps its record, so
    // "does the coordinator leak a socket per registration" is not observable
    // from snapshot_workers().size() alone.
    [[nodiscard]] std::size_t worker_connection_count() const {
        std::lock_guard lock(mu_);
        std::size_t n = 0;
        for (const auto& [_, w] : registered_) {
            if (w && w->conn) {
                ++n;
            }
        }
        return n;
    }

    // Replay every persisted job manifest under the configured ha_dir
    // back into this coordinator, with restore_from set per job. Called by
    // clink_node when its HaCoordinator fires the become-leader
    // callback. Idempotent - already-running job_ids are skipped.
    //
    // A job whose recovery fails ONLY for capacity (no worker registered
    // yet - takeover races workers reconnecting after their old control
    // connections died with the old leader) is PARKED, not dropped: a
    // retry thread re-runs its recovery from the manifest whenever
    // capacity registers. Dropping it silently lost a running job until
    // the next failover.
    void recover_persisted_jobs();

    // Thrown by submit_job when the cluster lacks the slots the plan
    // needs after the configured wait. A distinct type so the HA
    // recovery path can tell "no workers yet" (parkable, retried) apart
    // from every non-retriable rejection (bad graph, failed gate).
    struct InsufficientSlotsError : std::runtime_error {
        using std::runtime_error::runtime_error;
    };

private:
    struct WorkerConnection {
        std::string worker_id;
        std::string data_host;
        // Content hashes of plugin modules already shipped over THIS
        // connection (item 30). Deploys send hash-only references for
        // these; a re-register builds a fresh WorkerConnection, so the new
        // session starts empty and receives full bytes again. The worker's
        // process-level content cache still deduplicates the bytes on disk.
        // Recorded at Deploy build under
        // mu_ - if the send then fails, the worker is on its way to lost
        // or re-registered, both of which reset this.
        std::unordered_set<std::string> shipped_plugin_hashes;
        // The protocol version this peer registered with. Checked at register and
        // then discarded, which left a mixed-version cluster diagnosable only by
        // reading logs - the version that refused a peer was reported, the versions
        // that were ACCEPTED were not. Retained so the read API can show them.
        std::uint32_t protocol_version{0};
        // Transport. Owns the underlying fd (or TLS session). Reader
        // thread borrows; close() runs on watchdog teardown.
        std::unique_ptr<network::Connection> conn;
        std::thread reader;
        // Set by the reader as its LAST act, so another thread can tell a
        // finished reader from a running one and join it safely.
        //
        // Without this the coordinator leaked one thread, one file descriptor
        // and one socket per worker registration, released only at stop(). A
        // worker declared lost has its reader woken - the watchdog calls
        // shutdown_read() - and the thread returns, but nothing ever joined the
        // handle, and shutdown_read is shutdown(SHUT_RD), not close. So the
        // resources stayed. Bounded by distinct worker ids ever seen, not by
        // cluster size: any supervisor that mints fresh ids (hostname+pid, a
        // Deployment rather than a StatefulSet) leaks one fd per restart until
        // the coordinator cannot accept at all - which takes client admission
        // down with it. Same shape as the client-session leak, on the path that
        // had no reaper.
        std::shared_ptr<std::atomic<bool>> reader_finished =
            std::make_shared<std::atomic<bool>>(false);
        std::chrono::steady_clock::time_point last_seen;
        bool lost{false};
        std::uint32_t slot_capacity{1};
        std::uint32_t slots_in_use{0};
        // HTTP port the worker is serving its dashboard endpoints on.
        // 0 = worker didn't opt into HTTP; coordinator proxy paths skip it.
        std::uint16_t http_port{0};
    };

    struct JobState {
        JobId id{};
        std::size_t expected_completion{0};
        std::size_t completed_count{0};
        // Captured at deploy_internal_ when the job is first added to
        // jobs_. Used by signal_job_completion_locked_ to compute the
        // job's total wall-time before pushing into history_.
        std::chrono::steady_clock::time_point submit_time{};
        // Monotonically-increasing version. Bumped at initial deploy
        // (set to 1) and on every successful rescale. Queryable State
        // clients piggyback on this for cache invalidation - if a
        // cached route entry was captured at a version that's no
        // longer current, the client knows the topology shifted and
        // refetches.
        std::uint64_t topology_version{0};
        // The generation of the STATE LAYOUT, which is not the same thing as
        // topology_version and must not be conflated with it.
        //
        // topology_version bumps on every restart, including a plain worker-loss
        // redeploy, because the PLACEMENT may have changed and route caches need
        // invalidating. The state layout does not change then: the same operators
        // hold the same job-global subtask indices, so their directories still mean
        // what they meant before.
        //
        // state_generation bumps only when the index -> operator mapping actually
        // moves, which is a rescale. Using topology_version for the state path
        // instead sent a restarted job looking for state under a generation nothing
        // had ever written, and it replayed from offset zero - caught by four
        // worker-kill tests duplicating record-0.
        // Which subtasks a checkpoint was TRIGGERED for, kept until it completes.
        //
        // pending_checkpoint_acks cannot answer this: acks are removed from it as
        // they arrive, so by the time the checkpoint completes it is empty. Recorded
        // separately so the COMPLETED marker can say what the checkpoint consists
        // of - the first cut read the drained set and wrote "subtasks=" empty, which
        // the consistency check caught immediately.
        // Both the generation and the subtask set are captured at TRIGGER. Reading
        // either at completion misattributes a checkpoint that spans a rescale: the
        // ack set is drained by then, and state_generation may already have bumped,
        // so a checkpoint triggered by the old topology gets labelled with the new
        // generation and every one of its files then looks like an outsider.
        struct CheckpointParticipants {
            std::uint32_t generation{1};
            std::set<std::uint32_t> subtasks;
        };
        std::unordered_map<std::uint64_t, CheckpointParticipants> checkpoint_participants;
        std::uint32_t state_generation{1};
        // The checkpoint id at which the CURRENT state generation began; anything at
        // or below it was written by the previous generation.
        std::uint64_t state_generation_after_checkpoint{0};
        std::vector<std::string> errors;
        // Structured counterpart to `errors`, built from each SubtaskFinished
        // failure report (role/subtask/worker + message, with the executor's
        // capture-site stack trace inside the message). Surfaced via
        // JobDetail.subtask_errors. Kept alongside the flat list, not instead.
        std::vector<SubtaskErrorRecord> subtask_errors;
        // Packed expected state-version map (schema evolution) for this
        // job, captured at deploy from the JobGraphSpec. Re-sent verbatim
        // on every Deploy (initial + rescale/recovery re-deploy) so each
        // subtask's LocalExecutor migrates restored state to the declared
        // schema. Empty when the job declared no versions.
        std::string expected_state_versions_packed;
        // Packed UDF declarations (pack_udf_specs) for this job, captured
        // at deploy from the JobGraphSpec. Re-sent verbatim on every
        // Deploy (initial + rescale/recovery re-deploy) so each worker
        // registers the functions before running subtasks. Empty when the
        // job uses none.
        std::string udfs_packed;
        // Per-task records keyed by "role:subtask_idx" so a retry can
        // re-send the original Deploy entry to the original worker.
        std::unordered_map<std::string, std::pair<std::string, DeploymentTask>> task_records;
        std::unordered_map<std::string, int> attempt_counts;
        // Per-subtask lifecycle timestamps (unix ms), keyed like task_records
        // ("role:subtask_idx"). started stamped at deploy, finished at
        // SubtaskFinished. Surfaced per subtask on the job graph / operators
        // endpoints so the console can show start time + (running) duration.
        struct SubtaskTiming {
            std::int64_t started_ms{0};
            std::int64_t finished_ms{0};
        };
        std::unordered_map<std::string, SubtaskTiming> subtask_timing;
        // Tasks per worker (for grouping PeerUpdate by worker_id).
        std::unordered_map<std::string, std::vector<DeploymentTask>> tasks_by_worker;
        // Tasks pending completion per worker (for synthesised errors when
        // a worker is declared lost mid-job).
        std::unordered_map<std::string, std::vector<std::pair<std::string, std::uint32_t>>>
            pending_per_worker;

        // Port discovery state.
        std::size_t expected_listenings{0};
        std::size_t received_listenings{0};
        // Per-edge port map. The key is the 4-tuple identifying which
        // upstream subtask connects to which downstream subtask; the
        // value is the (host, port) the upstream should connect to.
        //
        // Why 4 elements? In a multi-input topology (union/join), one
        // downstream subtask binds multiple inbound listeners (one per
        // upstream). Keying by downstream alone wouldn't tell us which
        // listener serves which upstream.
        struct EdgeKey {
            std::string downstream_role;
            std::uint32_t downstream_subtask_idx{};
            std::string upstream_role;
            std::uint32_t upstream_subtask_idx{};
            bool operator<(const EdgeKey& o) const {
                if (downstream_role != o.downstream_role)
                    return downstream_role < o.downstream_role;
                if (downstream_subtask_idx != o.downstream_subtask_idx)
                    return downstream_subtask_idx < o.downstream_subtask_idx;
                if (upstream_role != o.upstream_role)
                    return upstream_role < o.upstream_role;
                return upstream_subtask_idx < o.upstream_subtask_idx;
            }
        };
        std::map<EdgeKey, std::pair<std::string, std::uint16_t>> ports;
        bool peer_updates_sent{false};

        // Borrowed (non-owning) pointer to the client's Connection.
        // nullptr if submitted in-process. The client-loop thread owns
        // the Connection; the watcher logic clears this pointer when
        // the client closes so signal_job_completion_locked_ can no-op.
        network::Connection* notify_client_conn{nullptr};
        // Wire-order guarantee for the submitting client: JobCompleted must
        // never overtake SubmitJobAck on the connection. The deploy starts
        // the job BEFORE handle_submit_ writes the ack, and a tiny job can
        // complete inside that window - its completion push then raced the
        // ack onto the wire and the submitter read "unexpected reply kind
        // 106" (ThreadSanitizer's scheduling hit it on every small-job
        // cluster test from 2026-08-18). While false,
        // signal_job_completion_locked_ DEFERS its push; handle_submit_
        // flips it after the ack is sent and flushes the deferral. Defaults
        // true because only the wire-submit path acks: an in-process or
        // HTTP submission must never park its completion behind a flag
        // nothing will flip.
        bool submit_ack_sent{true};
        bool completion_push_deferred{false};
        bool completion_signalled{false};
        // Set by handle_cancel_job_ (a client-initiated cancel) BEFORE
        // CancelJob is broadcast to the workers. signal_job_completion uses
        // it to surface "cancelled by client" instead of the bare
        // SubtaskFinished error messages.
        bool cancel_requested{false};
        // A graceful stop is in progress: the subtasks have been told to stop
        // producing and run their end-of-input path. Deliberately not
        // cancel_requested, which decides the REPORTED OUTCOME - a job that
        // stopped cleanly at a savepoint must report success, not "cancelled".
        bool stop_requested{false};
        // An unrecoverable subtask error has already broadcast CancelJob to the
        // job's workers, so peers wedged by the failed task drain and the job can
        // complete. Deliberately NOT cancel_requested: that flag decides the
        // reported outcome, and a job that FAILED must report failed, not
        // cancelled. This one only stops the broadcast repeating.
        bool error_cancel_broadcast{false};
        // Stamped whenever a broadcast CancelJob is expected to TERMINATE the
        // job - by the fatal-error path (error_cancel_broadcast) and by a
        // client cancel (cancel_requested) alike. Both complete the job by
        // COUNTING: peer cancels drive completed_count to
        // expected_completion - and a count has no timeout. A peer whose
        // cancel never lands (its worker lost, or a task that finished
        // constructing after its worker flipped the cancel tokens and so ran
        // on as an orphan) parks the count short forever; QUAL-06 watched a
        // 292-task job sit RUNNING for 75 minutes at 291/292 with its
        // verdict already recorded (item 75a), and its run B saw a client
        // cancel "ignored" for 40 minutes the same way (item 73). The
        // watchdog force-completes the job - FAILED or CANCELLED per the
        // usual outcome precedence - when this deadline expires.
        std::chrono::steady_clock::time_point terminal_cancel_deadline{};

        // worker-crash recovery state.
        //
        // When the watchdog declares a worker lost and the job's
        // checkpoint.max_restarts_on_worker_loss permits another attempt,
        // mark_worker_lost_locked_ sets `awaiting_restart=true` and adds
        // every (role, subtask) hosted on the lost worker to
        // restart_pending. error synthesis is skipped. The existing
        // CancelJob broadcast winds down surviving subtasks; their
        // SubtaskFinished arrivals do NOT add to errors / completed_count
        // when awaiting_restart is set - instead they're recorded in
        // restart_drained_keys until every surviving-worker subtask has
        // reported. At that point handle_subtask_finished_ triggers
        // restart_job_locked_, which redeploys the entire task set onto
        // surviving workers with restore_from pointing at the coordinator's checkpoint
        // dir + latest_completed_checkpoint_id, clears the restart
        // Commit-confirmed restore protocol (jobs with a sink whose
        // external commit dies with the process -
        // PlannedTask::needs_commit_confirmation). confirm_task_keys: the
        // tracked tasks in the CURRENT topology, keyed like task_records.
        // pending_confirms: per checkpoint, the tracked tasks whose
        // CommitConfirmed has not arrived; drained -> CONFIRMED-N marker +
        // latest_confirmed advance. Empty confirm_task_keys = the job is
        // not on the protocol and none of this is consulted.
        std::set<std::string> confirm_task_keys;
        std::map<std::uint64_t, std::set<std::string>> pending_confirms;
        std::uint64_t latest_confirmed_checkpoint_id{};
        // bookkeeping, and increments restart_attempts.
        bool awaiting_restart{false};
        std::uint32_t restart_attempts{0};
        // Whole-job restarts caused by FAILED CHECKPOINTS since the last
        // checkpoint that COMPLETED. Reset on completion; at
        // Config::checkpoint_failure_restart_limit the job fails with the
        // cause instead of restarting again (item 77b).
        std::uint32_t consecutive_ckpt_failure_restarts{0};
        // (role, subtask_idx) entries from the lost worker that need to be
        // re-deployed onto a survivor.
        std::vector<std::pair<std::string, std::uint32_t>> restart_pending;
        // Set by rescale_job: maps role -> new parallelism. The next
        // restart_job_locked_ honours these overrides when synthesizing
        // the new task set, computes per-new-subtask key-group ranges,
        // and tags each new DeploymentTask with the parent old subtask
        // whose state file it should restore from. Cleared after the
        // redeploy fires. Empty for worker-loss-driven restarts (where the
        // parallelism doesn't change).
        std::unordered_map<std::string, std::uint32_t> rescale_overrides;
        // Captured per-role parallelism at the moment a rescale was
        // requested, so restart_job_locked_ can compute the integer
        // scale factor k = new_p / old_p and the parent old-subtask
        // index for each new subtask. Populated alongside
        // rescale_overrides; cleared together.
        std::unordered_map<std::string, std::uint32_t> pre_rescale_parallelism;
        // An in-incarnation restart is HELD while the in-doubt resolver is
        // finalising this job's completed-but-unconfirmed broker
        // transactions (see stage_in_doubt_resolution_locked_). Resolution
        // and restore-point selection are ONE decision: nothing may deploy
        // - and so nothing may fence the orphan - until the broker has
        // answered and latest_confirmed reflects it.
        bool resolving_in_doubt{false};
        // Cooperative cancellation for the in-doubt walk, created at stage
        // time and handed to resolve_in_doubt_commits. The watchdog's SOFT
        // deadline sets it (a slow walk - an outage stretching the wire
        // probes - must stop mutating and let the restart proceed on the
        // bounded contract); only the HARD deadline that follows treats a
        // walk that STILL has not returned as hung and fails the job. The
        // rig-night composite caught the one-stage alternative live: a
        // timed-out walk kept executing commits and wrote CONFIRMED for a
        // job the coordinator had already failed.
        std::shared_ptr<std::atomic<bool>> in_doubt_cancel;
        bool in_doubt_cancel_requested{false};
        // Wall-clock start for the clink.rescale lifecycle span recorded
        // when restart_job_locked_ emits the rescaled deploys (both the
        // whole-job drain and the per-operator replan set it). 0 = the
        // span buffer was disabled when the rescale was staged.
        std::uint64_t rescale_span_start_unix_nano{0};
        // Set by rescale_operator_parallelism: operator id -> requested
        // parallelism, with the parallelism each of those operators had when
        // the request was accepted. When non-empty, the next
        // restart_job_locked_ REPLANS the job from `graph_json` at the new
        // parallelism instead of resizing the deployed task set.
        //
        // Why replan rather than clone. The role-based path resizes a role's
        // template set by cloning one subtask's DeploymentTask, and a
        // template's operator identity lives inside its packed
        // OperatorChainSpec (`extra_config`), which cloning does not rewrite.
        // On a multi-operator job that produced N clones of one chain, with
        // edges naming subtask indices that were no longer deployed
        // ("missing resolved peer for edge"), and the job died - F41.
        // Replanning cannot produce that: the planner derives the whole task
        // set, its chain specs, its edges and its key-group ranges from the
        // graph, which is the same thing submit does.
        //
        // Keyed by OPERATOR id, unlike rescale_overrides which is keyed by
        // role. Every task shares one role, so a role key cannot name an
        // operator.
        std::unordered_map<std::string, std::uint32_t> pending_op_parallelism;
        std::unordered_map<std::string, std::uint32_t> pre_rescale_op_parallelism;
        // The operator->block layout that produced every checkpoint at or before
        // `stale_layout_through`, retained AFTER a replan rather than discarded.
        //
        // Why it has to outlive the replan. A rescale redeploys from
        // latest_completed_checkpoint_id, and that id still names a PRE-rescale
        // checkpoint until one completes under the new topology. A restart inside
        // that window is not a replan-rescale, so every task would fall back to
        // "restore from my own subtask index" - and the directory at that index
        // holds the state of whichever operator occupied it under the OLD layout,
        // because the planner allocates one contiguous block per operator in graph
        // order and resizing one operator moves every later block. The comment
        // above says a replanned task cannot restore from its own index; the same
        // is true of a RESTARTED task until the restore point moves past the
        // rescale. Cleared when a checkpoint completes above that id.
        struct StaleBlock {
            std::uint32_t base{};
            std::uint32_t parallelism{};
        };
        std::unordered_map<std::string, StaleBlock> stale_layout_blocks;
        std::uint64_t stale_layout_through{0};
        // Which operator each deployed task hosts, keyed like task_records
        // ("role:subtask_idx"). Populated at every deploy from the plan. The
        // type lives in rescale_dispatch.hpp with the translation helpers
        // that consume it (op_scoped_ack / task_hosts_op); the alias keeps
        // the JobState::TaskOpIdentity spelling working.
        using TaskOpIdentity = clink::cluster::TaskOpIdentity;
        TaskOpIdentityMap task_op_identity;
        // Surviving-worker subtasks we've already received SubtaskFinished
        // for during the awaiting_restart drain.
        std::unordered_set<std::string> restart_drained_keys;
        // The set of "role:subtask_idx" keys we expect to drain before
        // firing the restart. Equals tasks_by_worker minus lost-worker tasks
        // at the moment mark_worker_lost_locked_ fires.
        std::unordered_set<std::string> restart_drain_expected;
        // A FATAL subtask error observed while a restart was already
        // draining. Fatal means restarting cannot fix it (the named restore
        // point itself is damaged), so the pending restart must not fire -
        // but the drain branch used to swallow the report entirely: the
        // retry redeployed, re-hit the same fatal refusal, and looped until
        // the restart budget was gone, with each attempt's errors.clear()
        // wiping the verdict so the job could end carrying a peer's
        // cancellation noise instead of the diagnosis. restart_job_locked_
        // checks this at entry and fails the job with the preserved cause.
        std::string fatal_cause;
        // Deadline by which the awaiting_restart drain must complete.
        // Set when the job enters awaiting_restart (now +
        // Config.restart_drain_timeout); the watchdog fails the job if the
        // drain is still outstanding past it. Default-constructed (epoch)
        // means "no drain in progress".
        std::chrono::steady_clock::time_point restart_deadline{};
        // Set when a covered restart found insufficient capacity: the wait
        // window for a lost worker to re-register before the restart is
        // failed with the no-slot diagnosis. Zero = not waiting. Distinct
        // from restart_deadline (drain/resolution progress), whose expiry
        // semantics do not fit a capacity wait.
        std::chrono::steady_clock::time_point restart_capacity_deadline{};

        // Plugin binaries this job depends on. Held so deploy_internal_
        // can attach them to every DeployMsg. The coordinator caches the bytes
        // on disk separately via PluginLoader.
        std::vector<PluginBinary> plugins;

        // Per-operator rescale state machine. Populated at
        // deploy_internal_ by walking graph.ops and calling
        // register_operator for each op with parallelism + bounds.
        // unique_ptr because RescaleCoordinator is non-copyable (mutex
        // member) and JobState needs move semantics for its
        // unordered_map storage.
        std::unique_ptr<RescaleCoordinator> rescale_coordinator;

        // In-flight HOT cutover (design record 008): one operator changing
        // parallelism while the job keeps running. Engaged from the arming
        // request until Complete or abort. While engaged, the periodic
        // trigger sweep skips this job (rule 5: the cutover checkpoint is
        // the last of the old layout, its successor the first of the new),
        // which is what lets `cutover_checkpoint` be assigned before it is
        // triggered. The job does NOT drain: awaiting_restart stays false
        // and every unaffected subtask keeps running - that is the point.
        struct HotCutover {
            std::string op_id;
            std::uint32_t old_parallelism{0};
            std::uint32_t target_parallelism{0};
            std::uint64_t cutover_checkpoint{0};
            enum class Phase : std::uint8_t {
                Arming,       // BeginRescale sent, awaiting worker acks
                AwaitingCut,  // C triggered; awaiting completion + drains
                Rebinding,    // CutoverRebind sent; awaiting new-edge ports
                Deploying,    // new tasks deployed; awaiting their listenings
            };
            Phase phase{Phase::Arming};
            std::chrono::steady_clock::time_point phase_deadline{};
            // Wall-clock start for the clink.rescale lifecycle span
            // recorded at completion (mode=hot_cutover). 0 = span
            // disabled when the cutover began.
            std::uint64_t span_start_unix_nano{0};
            // The validated post-cutover plan, held from request time so
            // the deploy uses exactly what eligibility checked.
            std::vector<PlannedTask> planned_tasks;
            std::uint32_t appended_base{0};
            // The op's OLD contiguous block, captured while the identity
            // records still describe it: Complete writes it into
            // stale_layout_blocks so a whole-job failure inside
            // [Complete, C+1] still translates its restore correctly.
            std::uint32_t old_block_base{0};
            // Arm-ack accounting: workers still to reply, and the armed
            // totals accumulated across replies vs what the deployed
            // identity and graph say should exist.
            std::unordered_set<std::string> arm_workers_pending;
            std::uint32_t expected_op_tasks{0};
            std::uint32_t expected_groups{0};
            std::uint32_t expected_rebind_tasks{0};
            std::uint32_t acked_callbacks{0};
            std::uint32_t acked_groups{0};
            std::uint32_t acked_rebind_tasks{0};
            // Feeding / fed task keys ("role:global_idx") captured at
            // request time; the rebind and swap dispatches address their
            // workers.
            std::vector<std::string> feeder_task_keys;
            std::vector<std::string> fed_task_keys;
            // (fed task key, new upstream idx) pairs whose rebind port has
            // not yet arrived.
            std::set<std::pair<std::string, std::uint32_t>> rebind_ports_pending;
        };
        std::optional<HotCutover> hot_cutover;

        // Per-job adaptive autoscaler. Created in
        // deploy_internal_ when cfg_.autoscaler is set and at least
        // one of the job's operators carries [min, max] bounds.
        // Owns its own polling thread; destroyed (and joins) when
        // JobState is removed or when coordinator::stop_autoscalers_() runs.
        std::unique_ptr<Autoscaler> autoscaler;

        // Original JobGraphSpec JSON. Captured at submit so HA recovery
        // can rebuild the JobBundle / re-plan onto the new cluster
        // without needing the original submitter.
        std::string graph_json;

        // Human-readable job name from the submitted spec (empty when
        // unnamed). Carried so lifecycle events (lineage start / complete)
        // can identify the job by name, not just id.
        std::string name;

        // Distributed-checkpointing config carried from SubmitJob. The
        // coordinator uses checkpoint_dir to address per-job snapshot storage,
        // and interval_ms to drive its periodic trigger thread.
        CheckpointConfig checkpoint;

        // Per-checkpoint ack tracking: id -> set of "role:subtask_idx"
        // strings still pending. When the set empties the coordinator writes a
        // <checkpoint_dir>/_jobs/<job_id>/COMPLETED-<id> marker and updates
        // `latest_completed_checkpoint_id`.
        std::unordered_map<std::uint64_t, std::unordered_set<std::string>> pending_checkpoint_acks;
        // Subtasks that acked a checkpoint with ok=false, per checkpoint id.
        //
        // A failed ack means the subtask's snapshot did not happen: the
        // operator caught the exception, reported it, and carried on. It
        // used to be erased from the pending set exactly like a success,
        // so the set still emptied and the checkpoint was recorded as
        // globally complete - COMPLETED-N written, latest_completed
        // advanced, commit broadcast - for state that was never written.
        // A later restore would then restore FROM a checkpoint that does
        // not exist in full.
        //
        // Tracked rather than inferred, because "did every subtask ack?"
        // and "did every subtask SUCCEED?" are different questions and
        // the pending set only answers the first.
        std::unordered_map<std::uint64_t, std::unordered_set<std::string>> failed_checkpoint_acks;
        // Start time per in-flight
        // checkpoint id. Stamped at trigger; consumed at completion
        // to feed clink_ckpt_duration_ms_sum / count. Entries are
        // dropped when the matching pending set empties.
        std::unordered_map<std::uint64_t, std::chrono::steady_clock::time_point>
            pending_checkpoint_start_times;
        std::uint64_t latest_completed_checkpoint_id{0};
        std::uint64_t next_checkpoint_id{1};

        // When this job last had a periodic checkpoint TRIGGERED, so the
        // trigger loop can tell whether its interval has elapsed.
        //
        // Without it the loop computed a sleep from the configured
        // intervals and then triggered every eligible job on every pass,
        // so interval_ms only ever shortened the loop's sleep and never
        // gated a job. Every job checkpointed at the loop's 500ms tick
        // whatever it had asked for - QUAL-01 measured 61 checkpoints in
        // 30 seconds against a configured 10s interval, twenty times the
        // intended rate, with the corresponding multiple of state writes
        // and transactional sink commits. Default-constructed means
        // "never triggered", which fires the first one immediately.
        std::chrono::steady_clock::time_point last_checkpoint_trigger_at{};

        // Adaptive checkpoint mode (CheckpointAlignment::Adaptive). The
        // policy decides the mode stamped on each trigger; its pressure
        // observation is the LAST completed checkpoint's duration
        // relative to the configured interval - a checkpoint that takes
        // as long as its interval is the direct symptom (barrier
        // alignment stalls under backpressure) unaligned mode exists to
        // relieve. Created lazily on the first adaptive trigger; null
        // for statically-aligned jobs.
        std::unique_ptr<clink::checkpoint::AdaptiveModePolicy> adaptive_ckpt_policy;
        std::uint64_t last_checkpoint_duration_ms{0};

        // Bounded-source end-of-stream FINAL checkpoint coordination. When the
        // first bounded source reaches clean EOS and sends RequestFinalCheckpoint,
        // the coordinator assigns ONE final checkpoint id for the whole job (so every
        // parallel source subtask injects + acks the SAME id), seeds its pending
        // ack set from the live task set, and broadcasts TriggerCheckpoint for it.
        // Every requester is replied the same id. Cleared on restart so a replayed
        // EOS re-requests a fresh id seeded from the post-restart task set.
        std::optional<std::uint64_t> final_checkpoint_id;
        std::unordered_set<std::string> sources_requested_final;
        // Test-only (CLINK_TEST_STALL_FIRST_FINAL_CKPT): force the no-crash
        // EOS-timeout path. On the FIRST final checkpoint, the coordinator picks the
        // first subtask that acks it (test_stall_key) and drops EVERY ack for
        // that (key, id) pair, so its pending entry never clears and the final
        // checkpoint never completes -> the source's wait_final_committed times
        // out -> it throws. Dropping ALL acks for the key (not just one) is
        // required because the source subtask acks its key twice (source drain +
        // owner snapshot). Bound to the first final id only (test_stalled_final_id),
        // and NOT cleared on restart, so the replay's NEW final id completes
        // normally. Both default-empty in production (env unset).
        std::optional<std::uint64_t> test_stalled_final_id;
        std::string test_stall_key;

        // Commit-group memberships derived from the job's
        // sink operator params at submit time. group_name -> ordered
        // set of "role:subtask_idx" keys belonging to the group.
        // Used by handle_subtask_checkpointed_ to gate
        // CommitCheckpoint broadcasts: a group's members commit
        // together once every member has acked its pre-commit
        // successfully, or all abort together on any member's failure.
        // Empty when no sinks declared a commit_group (the default
        // behaviour). See CheckpointGroupState below for what membership
        // does and does not buy - it is less than this name implies.
        std::unordered_map<std::string, std::unordered_set<std::string>> commit_groups;
        // Reverse index: "role:subtask_idx" -> group_name. nullopt
        // entry / absent key means "no group; commits independently"
        // (the default behaviour).
        std::unordered_map<std::string, std::string> subtask_commit_group;

        // Per-checkpoint group state, for each in-flight checkpoint that
        // touches at least one commit-group.
        //
        // What this actually does, which is narrower than the name
        // suggests: a group does NOT gate the commit broadcast. There is
        // no group-scoped CommitCheckpoint. Commit is broadcast per
        // checkpoint, job-wide, once every subtask has acked ok - so all
        // of a job's sinks are told to commit together whether or not
        // they share a group, and one failed ack aborts the checkpoint
        // for all of them. Cross-sink agreement comes from that protocol,
        // not from here. Verified by running a two-sink job with and
        // without a group: identical per-checkpoint agreement either way
        // (tests/integration/test_commit_group_atomicity.cpp).
        //
        // The one thing a group adds is WHEN the abort goes out. A failing
        // ack aborts the group immediately, whereas the checkpoint-level
        // abort waits until every subtask has answered - and nothing times
        // a pending checkpoint out, so if a peer never answers the
        // checkpoint-level abort never fires and staged sink transactions
        // stay staged. A group releases them at the first failure.
        //
        // `aborted` is what carries that. There used to be a `committed`
        // flag and the pending set was described as gating a group-scoped
        // commit; neither was ever read, and the stale comment is what
        // kept the false guarantee alive. `pending` is retained because
        // it is the membership accounting a real group-scoped commit gate
        // would need, and it is cheap; it is deliberately not load-bearing
        // today.
        struct CheckpointGroupState {
            std::unordered_set<std::string> pending;  // role:subtask keys still to ack
            bool aborted{false};
        };
        std::unordered_map<std::uint64_t, std::unordered_map<std::string, CheckpointGroupState>>
            commit_group_progress;

        // Per-job registry bundle. Plugin .so registrations from
        // SubmitJob land here (instead of the process-wide singletons),
        // so two concurrent jobs whose .so's mint overlapping
        // _inline_<kind>_<n> names don't trample each other. Lookups
        // fall through to the default singletons for built-ins.
        //
        // Held by unique_ptr to a forward-declared type so this header
        // doesn't have to include job_bundle.hpp (which would create a
        // job_planner.hpp <-> coordinator.hpp cycle). The destructor is
        // declared here and defined out-of-line in coordinator.cpp where
        // JobBundle is a complete type.
        std::unique_ptr<JobBundle> bundle;

        JobState();
        ~JobState();
        // Move ctors/operators declared but NOT defaulted here: a
        // defaulted move would be implicit-inline, which needs the full
        // JobBundle type. Defined in coordinator.cpp.
        JobState(JobState&&) noexcept;
        JobState& operator=(JobState&&) noexcept;
        JobState(const JobState&) = delete;
        JobState& operator=(const JobState&) = delete;
    };

    void accept_loop_();
    // Returns true if the connection was handed off to a long-lived
    // reader; false if it was a one-shot client conversation that has
    // already ended (the connection is closed/dropped).
    bool handle_first_frame_(std::unique_ptr<network::Connection> conn);
    void handle_register_(std::unique_ptr<network::Connection> conn, MessageReader& r);
    void handle_client_loop_(std::shared_ptr<network::Connection> conn);
    // Dispatch one decoded client frame; false means close the connection.
    // Separate from the loop so the loop can bound a throw to one frame.
    [[nodiscard]] bool dispatch_client_frame_(network::Connection& conn, MessageReader& r);
    void dispatch_worker_frame_(const std::shared_ptr<WorkerConnection>& worker, MessageReader& r);
    void handle_submit_(network::Connection& conn, MessageReader& r);
    void handle_list_jobs_(network::Connection& conn);
    void handle_cancel_job_(network::Connection& conn, MessageReader& r);
    // Deferred control-plane frame: staged under mu_, sent outside it.
    struct PendingDeploy {
        network::Connection* conn;
        std::vector<std::byte> frame;
    };

    void handle_subtask_finished_(MessageReader& r);
    void handle_begin_rescale_ack_(MessageReader& r);

    // ----- Hot cutover (design record 008) -----
    // Try to run `op_id`'s rescale as an in-place cutover. Returns true when
    // the hot path was engaged (arm frames staged into `out_frames`); false
    // means ineligible, with the reason, and the caller falls back to the
    // replan path. Called under mu_.
    // Populate restart_drain_expected with the subtasks that can ACTUALLY
    // drain - those on a worker still registered and not lost - and queue
    // everything else for redeploy.
    //
    // The distinction is not cosmetic. The CancelJob broadcast that starts
    // a drain skips workers that are unregistered or lost, so a subtask on
    // a dead worker is never asked to drain and never will. Waiting for it
    // burns the entire restart_drain_timeout and then fails the job.
    // QUAL-04's rig run ended exactly there: 15 subtasks undrained at the
    // deadline, 13 of them on a worker the coordinator had itself declared
    // lost seconds earlier, with the job killed after ten minutes of
    // waiting for the impossible.
    //
    // The worker-loss path always filtered this way; the subtask-error and
    // rescale paths did not, and this exists so the three cannot drift
    // apart again.
    void populate_restart_drain_locked_(JobState& job);

    bool try_begin_hot_cutover_locked_(JobState& job,
                                       const std::string& op_id,
                                       std::uint32_t new_parallelism,
                                       std::uint32_t old_parallelism,
                                       const JobGraphSpec& graph,
                                       std::vector<PendingDeploy>& out_frames,
                                       std::string& reason);
    // All arm acks arrived and match: trigger the cutover checkpoint.
    void hot_cutover_trigger_c_locked_(JobState& job, std::vector<PendingDeploy>& out_frames);
    // Every old subtask drained (CuttingOver): tear down the old tasks and
    // dispatch CutoverRebind to the fed tasks' workers.
    void hot_cutover_begin_rebind_locked_(JobState& job, std::vector<PendingDeploy>& out_frames);
    // Every rebind port arrived: place and deploy the planned new tasks.
    void hot_cutover_deploy_locked_(JobState& job, std::vector<PendingDeploy>& out_frames);
    // Every new task listening (Complete): targeted PeerUpdate to the new
    // tasks, CutoverPeerUpdate to the feeders, durable bookkeeping.
    void hot_cutover_complete_locked_(JobState& job, std::vector<PendingDeploy>& out_frames);
    // Abort at any phase: clear the hot state and fall back to the replan
    // at the requested parallelism. Returns the CancelJob frames to send.
    void abort_hot_cutover_locked_(JobState& job,
                                   const std::string& reason,
                                   std::vector<PendingDeploy>& out_frames);
    void handle_subtask_listening_(MessageReader& r);
    void handle_rescale_job_(network::Connection& conn, MessageReader& r);
    // Per-operator rescale request dispatch.
    void handle_rescale_operator_(network::Connection& conn, MessageReader& r);
    void handle_savepoint_(network::Connection& conn, MessageReader& r);
    void handle_stop_job_(network::Connection& conn, MessageReader& r);
    void start_reader_for_(std::shared_ptr<WorkerConnection> worker);
    void watchdog_loop_();
    void mark_worker_lost_locked_(WorkerConnection& worker);
    // Shared by mark_worker_lost_locked_ and retire_previous_session_subtasks_:
    // a worker's in-flight subtasks are dead either way, and both must fold them
    // into an in-progress restart or start one. See F64.
    void fold_dead_subtasks_into_restart_locked_(JobState& job,
                                                 const std::string& worker_id,
                                                 const char* log_channel,
                                                 const std::string& cause);

    // Fold a re-registered worker's PREVIOUS session's in-flight subtasks
    // into a restart and cancel the surviving sessions so they drain. The old
    // session is gone, so its subtasks can never report; without the fold or
    // survivor broadcast the drain waits out its deadline and fails a job
    // that was recovering.
    // Takes the lock itself (called from the register path, outside it).
    void retire_previous_session_subtasks_(const std::string& worker_id);
    void send_peer_updates_locked_(JobState& job);
    // job.plugins with bytes elided for every hash `worker`'s connection
    // already received, recording what this call will ship (item 30).
    // mu_ held by the caller.
    std::vector<PluginBinary> plugins_for_worker_locked_(const JobState& job,
                                                         WorkerConnection& worker);
    void signal_job_completion_locked_(JobState& job);
    // Build and send the JobCompleted frame to job.notify_client_conn.
    // Callers hold mu_ and have checked the conn is non-null and the
    // submit ack is already on the wire (JobState::submit_ack_sent).
    void push_job_completed_locked_(JobState& job);
    // Retire a terminal job's HA manifest so no later coordinator recovery
    // can resurrect it (followups item 69: a cancelled job came back on
    // the next takeover because cancellation left the manifest in place).
    // Tombstone first, then delete the manifest and plugin blobs - a crash
    // between the two leaves the tombstone, which recovery honours.
    // Best-effort like persist_history_record_: a store hiccup must not
    // turn completion signalling into a throw.
    void retire_job_manifest_(JobId job_id, const char* status);
    // After every surviving-worker subtask of `job` has drained on
    // awaiting_restart=true, rebuild tasks_by_worker by round-robin
    // assigning the original task set onto survivor workers, reset
    // transient JobState fields, and broadcast fresh Deploys with
    // restore_from set to the coordinator's latest_completed_checkpoint_id.
    // Returns the new Deploy frames to send outside the lock.
    std::vector<PendingDeploy> restart_job_locked_(JobState& job);
    // Begin a whole-job restart under mu_: drain bookkeeping, budget count,
    // the restart log line, and either an immediate redeploy (nothing in
    // flight, no in-doubt hold - the returned deploys) or CancelJob sends
    // the caller dispatches outside the lock (`cancels`). Shared by the
    // subtask-error path and the failed-checkpoint path: a failed
    // checkpoint aborts the sinks' staged transactions, and without the
    // rewind this initiates, the aborted interval was simply GONE - the
    // job sailed on and one checkpoint's records never reached the output.
    std::vector<PendingDeploy> initiate_job_restart_locked_(
        JobState& job,
        const std::string& reason,
        const std::string& cause,
        std::vector<std::pair<network::Connection*, JobId>>& cancels);

    // Replan a job at a changed per-operator parallelism.
    //
    // Parses `job.graph_json`, applies `job.pending_op_parallelism`, and runs
    // the ordinary planner over the result using the job's own registries. The
    // returned plan is the post-rescale task set in full: chain specs, edges
    // and key-group ranges all derived from the graph, exactly as a fresh
    // submit would derive them. Placement is left to the caller (worker_id
    // stays empty).
    //
    // Throws on an unparseable graph, an operator the graph does not contain,
    // or a plan the registries reject - the caller turns that into a failed
    // rescale rather than a half-deployed job.
    [[nodiscard]] JobPlan replan_at_new_parallelism_(const JobState& job) const;

    // Coordinator-driven adaptive rescale dispatch.
    //
    // dispatch_begin_rescale_locked_: builds one BeginRescaleMsg per
    // worker hosting at least one old subtask of `op_id`. Appended to
    // `out`. Caller sends outside the lock.
    void dispatch_begin_rescale_locked_(JobState& job,
                                        const std::string& op_id,
                                        std::uint64_t cutover_checkpoint,
                                        std::uint32_t target_parallelism,
                                        std::vector<PendingDeploy>& out);

    // dispatch_cutover_deploy_locked_: fires on the coordinator's
    // Draining -> CuttingOver transition. Plans the new subtasks via
    // plan_operator_cutover, mutates job.task_records / tasks_by_worker
    // to remove the drained old subtasks and add the new ones,
    // bumps expected_completion / expected_listenings, decrements
    // the old subtasks' worker slots and claims slots for the new ones.
    // On insufficient capacity or non-integer scale factor the
    // coordinator's rescale is aborted and `out` is left empty.
    void dispatch_cutover_deploy_locked_(JobState& job,
                                         const std::string& op_id,
                                         std::vector<PendingDeploy>& out);

    JobId allocate_job_id_();
    JobId deploy_internal_(const JobPlan& plan,
                           network::Connection* notify_client_conn,
                           std::vector<PluginBinary> plugins,
                           CheckpointConfig checkpoint,
                           std::unique_ptr<JobBundle> bundle,
                           std::string expected_state_versions_packed = {},
                           std::string udfs_packed = {});
    void handle_subtask_checkpointed_(MessageReader& r);
    // Commit-confirmed restore protocol: a tracked task's commit callbacks
    // for a checkpoint executed without throwing. Drains the checkpoint's
    // pending-confirmation set; on empty, writes CONFIRMED-<id> and
    // advances latest_confirmed_checkpoint_id.
    void handle_commit_confirmed_(MessageReader& r);
    // A bounded source at clean EOS requested a final coordinated checkpoint.
    // Assigns (once per job) the final id, seeds its pending ack set, broadcasts
    // TriggerCheckpoint, and replies FinalCheckpointAssigned on `reply_conn`.
    void handle_request_final_checkpoint_(MessageReader& r, network::Connection& reply_conn);
    void checkpoint_trigger_loop_();
    // Read every <ha_dir>/history/*.json on startup so the coordinator's
    // in-memory ring picks up where the previous leader left off.
    // Bounded to kCoordinatorHistoryCap entries (oldest dropped). Called
    // from set_ha_dir; no-op if ha_dir_ is empty.
    void reload_history_from_disk_();

    // Extract every per-job Autoscaler under the lock and
    // join its thread outside the lock. Called from coordinator::stop() before
    // tearing down jobs_, and from job-completion paths so a finished
    // job's autoscaler thread doesn't linger.
    void stop_autoscalers_();

    Config cfg_;
    // Optional metric source for the per-job autoscaler.
    // Empty by default - autoscaler tick will report 0.5 and idle.
    AutoscalerSampleFn autoscaler_sample_fn_;
    // HA persistence root. When empty, no manifests are written.
    // Format: <ha_dir>/jobs/<job_id>/manifest.json + plugin-<hash>.so.
    std::string ha_dir_;
    std::atomic<std::uint64_t> epoch_{0};

    // Encode a coordinator -> worker control frame, stamping this
    // coordinator's fencing epoch on it.
    //
    // Every such frame goes through here rather than each send site
    // assigning the field itself. That was the first shape of this code and
    // it was already wrong on arrival: four separate paths build a DeployMsg
    // (submit, two rescale paths, restart-after-failure) and only one of
    // them had been stamped. An unstamped frame carries epoch 0, which a
    // worker bound to a real epoch REFUSES - so the omission presents as a
    // deploy that silently never happens, not as a compile error.
    //
    // Takes a non-const reference deliberately: the caller's message is a
    // local about to be encoded, and DeployMsg carries plugin bytes that
    // are not worth copying to preserve constness.
    template <typename Msg>
    [[nodiscard]] std::vector<std::byte> fenced_frame_(MessageKind kind, Msg& m) const {
        m.coordinator_epoch = epoch();
        return encode_frame(kind, m);
    }
    int listener_fd_{-1};
    std::uint16_t bound_port_{0};
    std::thread accept_thread_;
    std::thread watchdog_thread_;
    // The watchdog's previous sweep time, for self-pause detection: a sweep
    // late by more than watchdog_interval + heartbeat_timeout means the
    // JUDGE was suspended, and last_seen staleness measured across that gap
    // says nothing about the workers. See watchdog_loop_.
    std::chrono::steady_clock::time_point last_watchdog_sweep_{std::chrono::steady_clock::now()};
    std::thread checkpoint_thread_;
    std::atomic<bool> stop_{false};

    // HA recoveries parked for capacity (job ids whose manifest is intact
    // but no worker had registered yet), and the thread that re-runs them
    // when slots appear. Guarded by mu_; the thread is spawned on first
    // park and joined in stop(). Recovery is re-run from the MANIFEST
    // (recover_one_persisted_job_), not from parked in-memory state: a
    // failed submit_job consumes its plugin/bundle arguments, so the disk
    // copy is the only ingredient list that survives a refusal.
    std::vector<JobId> pending_recovery_ids_;
    std::thread recovery_retry_thread_;
    void recovery_retry_loop_();
    // One job dir's recovery, callable repeatedly (skips ids already in
    // jobs_). Parks the id on InsufficientSlotsError.
    void recover_one_persisted_job_(JobId job_id);

    // In-incarnation in-doubt resolution: jobs whose drain has completed
    // but whose restart is HELD while the resolver finalises their
    // completed-but-unconfirmed broker transactions off-thread (broker
    // round-trips must not run under mu_ or on the watchdog). The thread
    // is spawned on first stage and joined in stop(); when a resolution
    // returns it applies the advanced confirmed watermark and fires the
    // deferred restart itself.
    std::vector<JobId> pending_in_doubt_resolutions_;
    std::thread in_doubt_resolution_thread_;
    void in_doubt_resolution_loop_();
    // True = the restart is deferred (either just staged, or a resolution
    // is already in flight); the caller must NOT run restart_job_locked_.
    // False = nothing to resolve; restart immediately.
    [[nodiscard]] bool stage_in_doubt_resolution_locked_(JobState& job);

    // THE definition of a restart drain being complete: every expected key
    // has drained (an empty expected set is trivially ready). Draining
    // COVERS the expected set rather than emptying it, and a fold can
    // shrink the expected set to keys that already drained - so any site
    // that tests expected.empty() instead of this misses a ready restart
    // and wedges the job into its drain deadline (watch item 63's actual
    // mechanism: the survivor's drain ack arrived BEFORE the dead worker's
    // fold, and nothing re-evaluated readiness after the fold).
    [[nodiscard]] static bool restart_drain_covered_(const JobState& job);

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::vector<std::string> expected_workers_;
    std::unordered_map<std::string, std::shared_ptr<WorkerConnection>> registered_;
    std::vector<std::string> lost_worker_ids_;
    std::unordered_map<JobId, std::shared_ptr<JobState>> jobs_;
    // Ring buffer of terminated jobs. signal_job_completion_locked_
    // pushes back; the front is evicted once size exceeds
    // kCoordinatorHistoryCap. Guarded by mu_ so job_history() can take
    // a consistent snapshot.
    std::deque<CompletedJobRecord> history_;
    // Terminal JobStates, oldest first, evicted from jobs_ at the SAME cap
    // as history_ (item 32). Without this, jobs_ grew monotonically: the
    // small public CompletedJobRecord was evicted at 128 while the large
    // JobState behind it - task records, retained graph, per-subtask
    // timings - was kept forever, and every snapshot_jobs(), ListJobs and
    // watchdog tick walked O(jobs ever run) under mu_. Only ids that have
    // passed through signal_job_completion_locked_ enter this deque, so a
    // running job can never be evicted; entries are unique because the
    // signal is idempotence-guarded. Tracks THIS process's terminals only -
    // history records recovered from the HA dir on leadership have no
    // JobState to evict and deliberately do not join. jobs_ holds
    // shared_ptrs, so a reader that copied one under mu_ keeps its state
    // alive across an eviction.
    std::deque<JobId> terminal_job_order_;
    JobId next_job_id_{1};
    // Convenience: the legacy `deploy(plan)`/`await_completion`/`errors`
    // path operates on whichever job was last deployed in-process. -1
    // means none yet.
    JobId legacy_active_job_id_{0};

    // Active client connections. Held as shared_ptr so the handler
    // thread and the back-pointer both keep the Connection alive;
    // dangling-pointer risk if the thread exits before stop()
    // would otherwise let close() touch freed memory.
    // One live client connection: its socket, its reader thread, and a
    // flag the thread raises as its last act so the accept loop can tell
    // a finished session from a running one without blocking on a join.
    //
    // This replaces two parallel vectors that were only ever drained by
    // stop(). Every client that connected and went away left a joinable
    // std::thread and a shared_ptr behind for the lifetime of the
    // coordinator, so a script polling `clink list` once a second grew
    // the process by 86,400 thread handles a day until thread creation
    // began to fail.
    struct ClientSession {
        std::shared_ptr<network::Connection> conn;
        std::thread thread;
        // shared_ptr rather than a member flag: the thread outlives the
        // vector element across a reallocation, and must not write into
        // a moved-from slot.
        std::shared_ptr<std::atomic<bool>> finished;
    };
    std::vector<ClientSession> client_sessions_;
    mutable std::mutex client_mu_;

    // Join and drop every session whose thread has finished. Called on
    // each new client, so the cost is paid by the connection that would
    // otherwise have grown the list. Returns the number still live.
    std::size_t reap_finished_clients_();
    // Join finished worker readers and drop their sockets, KEEPING the
    // registration record. Returns the number of workers still holding a
    // connection, which is what max_worker_connections bounds.
    //
    // The record is kept deliberately: several paths distinguish "absent" from
    // "present and lost" and behave differently, so erasing would quietly change
    // restart and drain semantics. What is released is the thread and the socket.
    std::size_t reap_finished_workers_();

    // Wraps an accepted listener fd into a Connection. Default plain
    // TCP via make_plain_connection. TLS callers replace via
    // set_accept_factory before start().
    AcceptFactory accept_factory_;
};

}  // namespace clink::cluster
