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
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/cluster/autoscaler.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/rescale_coordinator.hpp"
#include "clink/cluster/snapshots.hpp"
#include "clink/runtime/network/connection.hpp"

namespace clink::cluster {

struct JobGraphSpec;
class OperatorRegistry;
class JobBundle;

// One task in the user-supplied job plan. peer_refs are resolved against
// the rest of the plan at deployment time to compute the actual host:port
// that the task should connect to for each cross-stage data channel.
struct PlannedTask {
    std::string tm_id;
    std::string role;
    std::uint32_t subtask_idx{};
    std::uint16_t data_port{};  // 0 if no inbound listener
    std::vector<std::pair<std::string /*role*/, std::uint32_t /*subtask*/>> peer_refs;
    std::string extra_config;
};

struct JobPlan {
    std::vector<PlannedTask> tasks;
};

// CompletedJobRecord - HistoryServer entry. Captured at job
// termination and kept in a bounded ring buffer on the JM so operators
// can answer "what happened to job N?" even after the live JobState
// is gone. The history ring holds the last `kJobManagerHistoryCap`
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

// Maximum number of completed-job records the JM retains. Bounded so
// long-running clusters don't grow memory without bound; pick a number
// big enough for "what failed last week?" but small enough to stay
// cheap.
inline constexpr std::size_t kJobManagerHistoryCap = 128;

// Default control-plane port. The JM defaults to 6123; clink uses
// the same port so operators familiar with  can reach for the same
// muscle memory.
inline constexpr std::uint16_t kDefaultJobManagerPort = 6123;

// JobManager is the cluster's single source of truth for deployment.
//
// It supports two ways to drive it:
//   * In-process: call `expect_tms(...)`, `await_registrations(...)`,
//     `deploy(plan)`, `await_completion(...)`. This is the original test
//     harness path; it implicitly runs one job at a time.
//   * Over the wire: clients connect, send HelloClient + SubmitJob with a
//     JobGraphSpec, and the JM plans/deploys/tracks/reports completion
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

class JobManager {
public:
    struct Config {
        // How often the watchdog thread re-evaluates TM liveness.
        std::chrono::milliseconds watchdog_interval{100};
        // A TM is declared lost if no message has arrived from it for
        // longer than this. Heartbeats from a healthy TM should have a
        // shorter interval - typically heartbeat_timeout / 3 - so a
        // single missed message doesn't trigger a false positive.
        std::chrono::milliseconds heartbeat_timeout{2000};
        // Interface to bind the control-plane listener on. Default is
        // loopback (single-host tests). Set to "0.0.0.0" or a specific
        // NIC address for multi-machine clusters. Pair with TLS for any
        // deployment beyond a trusted local network.
        std::string bind_host{"127.0.0.1"};
        // Host advertised to clients/peers in resolved peer addresses
        // when bind_host is a wildcard. Defaults to bind_host. Set to a
        // routable hostname/IP when bind_host = "0.0.0.0".
        std::string advertise_host{};
        // Maximum number of restart attempts per failing task before the
        // JM gives up and surfaces the failure. 0 = no retries.
        int max_restarts{0};
        // Upper bound on how long a job may sit in awaiting_restart while
        // surviving subtasks drain before the redeploy fires. A drain that
        // never completes (e.g. a survivor that is hung - neither acking the
        // cancel nor dying) would otherwise wedge the job forever. On expiry
        // the watchdog FAILS the job (it does not force a restart, because a
        // still-alive-but-slow survivor must not be redeployed concurrently).
        // Generous by default to avoid false positives under load.
        std::chrono::milliseconds restart_drain_timeout{30000};
        // How long the SubmitJob handler waits for spare slots before
        // returning a rejection ack to the client. 0 means "never wait,
        // reject immediately". Useful when clusters auto-scale.
        std::chrono::milliseconds submit_wait_for_slots{0};
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
    };

    JobManager();
    explicit JobManager(Config cfg);
    ~JobManager();

    JobManager(const JobManager&) = delete;
    JobManager& operator=(const JobManager&) = delete;
    JobManager(JobManager&&) = delete;
    JobManager& operator=(JobManager&&) = delete;

    // Bind the control-plane listener and start the accept/watchdog
    // threads. `port = 0` lets the OS pick (good for tests). Returns the
    // bound port.
    std::uint16_t start(std::uint16_t port = 0);

    // ----- Legacy single-job API (test harness path) -----

    // Declare the set of TM ids that must register before deploy() can
    // proceed. Optional; new submission flow doesn't need it.
    void expect_tms(std::vector<std::string> tm_ids);

    // Block until every expected TM has registered, or `timeout` elapses.
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
    // If `notify_client_conn != nullptr`, the JM will send a
    // JobCompletedMsg back on that connection when the job finishes.
    // Pass nullptr for in-process submitters that poll via
    // await_job_completion.
    JobId submit_job(const JobGraphSpec& graph,
                     const OperatorRegistry& registry,
                     network::Connection* notify_client_conn = nullptr);

    // Overload that accepts plugin binaries to ship with every Deploy
    // for this job. The JM also writes them to its local cache and
    // dlopens them before planning so the registry-based validation
    // can see plugin-defined op types.
    JobId submit_job(const JobGraphSpec& graph,
                     const OperatorRegistry& registry,
                     std::vector<PluginBinary> plugins,
                     network::Connection* notify_client_conn = nullptr);

    // Overload that also carries distributed-checkpointing config (root
    // directory, periodic-trigger interval, optional restore directive).
    // Each subtask gets a private FileBackedStateBackend rooted under
    // checkpoint_dir on its TM; the JM's coordinator triggers periodic
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
    // (so the JM-side dispatcher / coordinator can reach it later).
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
    // every job the JM has seen complete, capped at
    // kJobManagerHistoryCap entries (oldest evicted first). Stable
    // copy - safe to inspect/serialize outside the JM mutex. Mirrors
    // the read surface HistoryServer exposes via /jobs/overview
    // for completed jobs.
    std::vector<CompletedJobRecord> job_history() const;

    // Convenience: look up one job by id from the history ring. Returns
    // std::nullopt if no terminal record is present (job still running,
    // never existed, or evicted from the ring).
    std::optional<CompletedJobRecord> job_history(JobId job_id) const;

    // Cluster-wide free slot count (sum across all registered TMs minus
    // tasks currently in flight). Useful for tests that want to assert
    // on slot accounting.
    std::size_t free_slots() const;

    // ----- Cluster state queries -----

    // TMs that the watchdog has declared lost (in registration order).
    std::vector<std::string> lost_tms() const;

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
    std::vector<TmSummary> snapshot_tms() const;
    std::vector<JobSummary> snapshot_jobs() const;
    // nullopt if job_id isn't a known job.
    std::optional<JobDetail> snapshot_job(JobId job_id) const;

    // Logical DAG + subtask placement for GET /api/v1/jobs/:id/graph. nullopt
    // if job_id isn't known; a detail with available=false when the job exists
    // but no graph was retained (e.g. submitted before graph retention).
    std::optional<JobGraphDetail> snapshot_job_graph(JobId job_id) const;

    // (data_host, http_port) for the TM with the given id, or nullopt
    // if the TM isn't registered, is lost, or didn't enable HTTP.
    // Used by the JM dashboard's proxy routes (/api/v1/tms/:id/*).
    std::optional<std::pair<std::string, std::uint16_t>> tm_http_target(
        const std::string& tm_id) const;

    // Unique TM HTTP targets (data_host, http_port) hosting any
    // subtask of `job_id`. Returns empty if the job is unknown, has
    // no placed subtasks, or every hosting TM has dropped HTTP.
    // Used by the Queryable State multi-TM locate endpoint so a
    // client can iterate the TMs holding a job's keyed state slots.
    std::vector<std::pair<std::string, std::uint16_t>> tms_hosting_job(JobId job_id) const;

    // Key-group-aware routing for Queryable State. Given a job, role,
    // and serialized key, returns the TM HTTP target hosting the
    // subtask responsible for that key's key-group plus the subtask
    // index. nullopt if the job/role is unknown, no subtask covers
    // the key's group, or the hosting TM has dropped HTTP. The {0,0}
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

    // Per-job topology version. Returns 0 if the job is unknown.
    // Incremented at initial deploy (-> 1) and on every successful
    // rescale. Used by Queryable State clients to invalidate cached
    // routes when the topology shifts (rescale moves key-groups to
    // different subtasks, so a cached (kg -> TmTarget) entry from
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
    // `timeout` bounds how long the JM waits for every subtask to
    // ack the savepoint. 0 picks a 30s default.
    SavepointAckMsg take_savepoint(JobId job_id, std::chrono::milliseconds timeout = {});

    // Override the factory used to wrap each accepted int fd into a
    // Connection. Default = plain-TCP; clink_node installs a TLS
    // factory when --tls-cert is given. Must be called before start().
    using AcceptFactory = std::function<std::unique_ptr<network::Connection>(int listener_fd)>;
    void set_accept_factory(AcceptFactory f);

    // Enable HA mode: every submitted job is persisted under <dir>/jobs/
    // (manifest.json + plugin-<hash>.so bytes). On standby->leader
    // takeover, recover_from_ha_dir replays every persisted job into
    // the JM's in-memory state, attaching restore_from at the latest
    // COMPLETED-N marker for that job. Must be called before start().
    void set_ha_dir(std::string dir);

    // Replay every persisted job manifest under the configured ha_dir
    // back into this JM, with restore_from set per job. Called by
    // clink_node when its HaCoordinator fires the become-leader
    // callback. Idempotent - already-running job_ids are skipped.
    void recover_persisted_jobs();

private:
    struct TmConnection {
        std::string tm_id;
        std::string data_host;
        // Transport. Owns the underlying fd (or TLS session). Reader
        // thread borrows; close() runs on watchdog teardown.
        std::unique_ptr<network::Connection> conn;
        std::thread reader;
        std::chrono::steady_clock::time_point last_seen;
        bool lost{false};
        std::uint32_t slot_capacity{1};
        std::uint32_t slots_in_use{0};
        // HTTP port the TM is serving its dashboard endpoints on.
        // 0 = TM didn't opt into HTTP; JM proxy paths skip it.
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
        std::vector<std::string> errors;
        // Structured counterpart to `errors`, built from each SubtaskFinished
        // failure report (role/subtask/tm + message, with the executor's
        // capture-site stack trace inside the message). Surfaced via
        // JobDetail.subtask_errors. Kept alongside the flat list, not instead.
        std::vector<SubtaskErrorRecord> subtask_errors;
        // Packed expected state-version map (schema evolution) for this
        // job, captured at deploy from the JobGraphSpec. Re-sent verbatim
        // on every Deploy (initial + rescale/recovery re-deploy) so each
        // subtask's LocalExecutor migrates restored state to the declared
        // schema. Empty when the job declared no versions.
        std::string expected_state_versions_packed;
        // Per-task records keyed by "role:subtask_idx" so a retry can
        // re-send the original Deploy entry to the original TM.
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
        // Tasks per TM (for grouping PeerUpdate by tm_id).
        std::unordered_map<std::string, std::vector<DeploymentTask>> tasks_by_tm;
        // Tasks pending completion per TM (for synthesised errors when
        // a TM is declared lost mid-job).
        std::unordered_map<std::string, std::vector<std::pair<std::string, std::uint32_t>>>
            pending_per_tm;

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
        bool completion_signalled{false};
        // Set by handle_cancel_job_ (a client-initiated cancel) BEFORE
        // CancelJob is broadcast to the TMs. signal_job_completion uses
        // it to surface "cancelled by client" instead of the bare
        // SubtaskFinished error messages.
        bool cancel_requested{false};

        // TM-crash recovery state.
        //
        // When the watchdog declares a TM lost and the job's
        // checkpoint.max_restarts_on_tm_loss permits another attempt,
        // mark_tm_lost_locked_ sets `awaiting_restart=true` and adds
        // every (role, subtask) hosted on the lost TM to
        // restart_pending. error synthesis is skipped. The existing
        // CancelJob broadcast winds down surviving subtasks; their
        // SubtaskFinished arrivals do NOT add to errors / completed_count
        // when awaiting_restart is set - instead they're recorded in
        // restart_drained_keys until every surviving-TM subtask has
        // reported. At that point handle_subtask_finished_ triggers
        // restart_job_locked_, which redeploys the entire task set onto
        // surviving TMs with restore_from pointing at the JM's checkpoint
        // dir + latest_completed_checkpoint_id, clears the restart
        // bookkeeping, and increments restart_attempts.
        bool awaiting_restart{false};
        std::uint32_t restart_attempts{0};
        // (role, subtask_idx) entries from the lost TM that need to be
        // re-deployed onto a survivor.
        std::vector<std::pair<std::string, std::uint32_t>> restart_pending;
        // Set by rescale_job: maps role -> new parallelism. The next
        // restart_job_locked_ honours these overrides when synthesizing
        // the new task set, computes per-new-subtask key-group ranges,
        // and tags each new DeploymentTask with the parent old subtask
        // whose state file it should restore from. Cleared after the
        // redeploy fires. Empty for TM-loss-driven restarts (where the
        // parallelism doesn't change).
        std::unordered_map<std::string, std::uint32_t> rescale_overrides;
        // Captured per-role parallelism at the moment a rescale was
        // requested, so restart_job_locked_ can compute the integer
        // scale factor k = new_p / old_p and the parent old-subtask
        // index for each new subtask. Populated alongside
        // rescale_overrides; cleared together.
        std::unordered_map<std::string, std::uint32_t> pre_rescale_parallelism;
        // Surviving-TM subtasks we've already received SubtaskFinished
        // for during the awaiting_restart drain.
        std::unordered_set<std::string> restart_drained_keys;
        // The set of "role:subtask_idx" keys we expect to drain before
        // firing the restart. Equals tasks_by_tm minus lost-TM tasks
        // at the moment mark_tm_lost_locked_ fires.
        std::unordered_set<std::string> restart_drain_expected;
        // Deadline by which the awaiting_restart drain must complete.
        // Set when the job enters awaiting_restart (now +
        // Config.restart_drain_timeout); the watchdog fails the job if the
        // drain is still outstanding past it. Default-constructed (epoch)
        // means "no drain in progress".
        std::chrono::steady_clock::time_point restart_deadline{};

        // Plugin binaries this job depends on. Held so deploy_internal_
        // can attach them to every DeployMsg. The JM caches the bytes
        // on disk separately via PluginLoader.
        std::vector<PluginBinary> plugins;

        // Per-operator rescale state machine. Populated at
        // deploy_internal_ by walking graph.ops and calling
        // register_operator for each op with parallelism + bounds.
        // unique_ptr because RescaleCoordinator is non-copyable (mutex
        // member) and JobState needs move semantics for its
        // unordered_map storage.
        std::unique_ptr<RescaleCoordinator> rescale_coordinator;

        // Per-job adaptive autoscaler. Created in
        // deploy_internal_ when cfg_.autoscaler is set and at least
        // one of the job's operators carries [min, max] bounds.
        // Owns its own polling thread; destroyed (and joins) when
        // JobState is removed or when JM::stop_autoscalers_() runs.
        std::unique_ptr<Autoscaler> autoscaler;

        // Original JobGraphSpec JSON. Captured at submit so HA recovery
        // can rebuild the JobBundle / re-plan onto the new cluster
        // without needing the original submitter.
        std::string graph_json;

        // Distributed-checkpointing config carried from SubmitJob. The
        // JM uses checkpoint_dir to address per-job snapshot storage,
        // and interval_ms to drive its periodic trigger thread.
        CheckpointConfig checkpoint;

        // Per-checkpoint ack tracking: id -> set of "role:subtask_idx"
        // strings still pending. When the set empties the JM writes a
        // <checkpoint_dir>/<job_id>/COMPLETED-<id> marker and updates
        // `latest_completed_checkpoint_id`.
        std::unordered_map<std::uint64_t, std::unordered_set<std::string>> pending_checkpoint_acks;
        // Start time per in-flight
        // checkpoint id. Stamped at trigger; consumed at completion
        // to feed clink_ckpt_duration_ms_sum / count. Entries are
        // dropped when the matching pending set empties.
        std::unordered_map<std::uint64_t, std::chrono::steady_clock::time_point>
            pending_checkpoint_start_times;
        std::uint64_t latest_completed_checkpoint_id{0};
        std::uint64_t next_checkpoint_id{1};

        // Bounded-source end-of-stream FINAL checkpoint coordination. When the
        // first bounded source reaches clean EOS and sends RequestFinalCheckpoint,
        // the JM assigns ONE final checkpoint id for the whole job (so every
        // parallel source subtask injects + acks the SAME id), seeds its pending
        // ack set from the live task set, and broadcasts TriggerCheckpoint for it.
        // Every requester is replied the same id. Cleared on restart so a replayed
        // EOS re-requests a fresh id seeded from the post-restart task set.
        std::optional<std::uint64_t> final_checkpoint_id;
        std::unordered_set<std::string> sources_requested_final;
        // Test-only (CLINK_TEST_STALL_FIRST_FINAL_CKPT): force the no-crash
        // EOS-timeout path. On the FIRST final checkpoint, the JM picks the
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
        // behaviour).
        std::unordered_map<std::string, std::unordered_set<std::string>> commit_groups;
        // Reverse index: "role:subtask_idx" -> group_name. nullopt
        // entry / absent key means "no group; commits independently"
        // (the default behaviour).
        std::unordered_map<std::string, std::string> subtask_commit_group;

        // Per-checkpoint group state. For each in-flight
        // checkpoint that touches at least one commit-group,
        // group_state[ckpt_id][group_name] tracks the set of subtasks
        // that have NOT yet acked. When the set empties the JM
        // broadcasts CommitCheckpoint to the group's members; if any
        // ack reports ok=false the group is aborted and every member
        // gets AbortCheckpoint.
        struct CheckpointGroupState {
            std::unordered_set<std::string> pending;  // role:subtask keys still to ack
            bool aborted{false};
            bool committed{false};
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
        // job_planner.hpp <-> job_manager.hpp cycle). The destructor is
        // declared here and defined out-of-line in job_manager.cpp where
        // JobBundle is a complete type.
        std::unique_ptr<JobBundle> bundle;

        JobState();
        ~JobState();
        // Move ctors/operators declared but NOT defaulted here: a
        // defaulted move would be implicit-inline, which needs the full
        // JobBundle type. Defined in job_manager.cpp.
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
    void handle_submit_(network::Connection& conn, MessageReader& r);
    void handle_list_jobs_(network::Connection& conn);
    void handle_cancel_job_(network::Connection& conn, MessageReader& r);
    void handle_subtask_finished_(MessageReader& r);
    void handle_subtask_listening_(MessageReader& r);
    void handle_rescale_job_(network::Connection& conn, MessageReader& r);
    // Per-operator rescale request dispatch.
    void handle_rescale_operator_(network::Connection& conn, MessageReader& r);
    void handle_savepoint_(network::Connection& conn, MessageReader& r);
    void start_reader_for_(std::shared_ptr<TmConnection> tm);
    void watchdog_loop_();
    void mark_tm_lost_locked_(TmConnection& tm);
    void send_peer_updates_locked_(JobState& job);
    void signal_job_completion_locked_(JobState& job);
    // After every surviving-TM subtask of `job` has drained on
    // awaiting_restart=true, rebuild tasks_by_tm by round-robin
    // assigning the original task set onto survivor TMs, reset
    // transient JobState fields, and broadcast fresh Deploys with
    // restore_from set to the JM's latest_completed_checkpoint_id.
    // Returns the new Deploy frames to send outside the lock.
    struct PendingDeploy {
        network::Connection* conn;
        std::vector<std::byte> frame;
    };
    std::vector<PendingDeploy> restart_job_locked_(JobState& job);

    // Coordinator-driven adaptive rescale dispatch.
    //
    // dispatch_begin_rescale_locked_: builds one BeginRescaleMsg per
    // TM hosting at least one old subtask of `op_id`. Appended to
    // `out`. Caller sends outside the lock.
    void dispatch_begin_rescale_locked_(JobState& job,
                                        const std::string& op_id,
                                        std::uint64_t cutover_checkpoint,
                                        std::uint32_t target_parallelism,
                                        std::vector<PendingDeploy>& out);

    // dispatch_cutover_deploy_locked_: fires on the coordinator's
    // Draining -> CuttingOver transition. Plans the new subtasks via
    // plan_operator_cutover, mutates job.task_records / tasks_by_tm
    // to remove the drained old subtasks and add the new ones,
    // bumps expected_completion / expected_listenings, decrements
    // the old subtasks' TM slots and claims slots for the new ones.
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
                           std::string expected_state_versions_packed = {});
    void handle_subtask_checkpointed_(MessageReader& r);
    // A bounded source at clean EOS requested a final coordinated checkpoint.
    // Assigns (once per job) the final id, seeds its pending ack set, broadcasts
    // TriggerCheckpoint, and replies FinalCheckpointAssigned on `reply_conn`.
    void handle_request_final_checkpoint_(MessageReader& r, network::Connection& reply_conn);
    void checkpoint_trigger_loop_();
    // Read every <ha_dir>/history/*.json on startup so the JM's
    // in-memory ring picks up where the previous leader left off.
    // Bounded to kJobManagerHistoryCap entries (oldest dropped). Called
    // from set_ha_dir; no-op if ha_dir_ is empty.
    void reload_history_from_disk_();

    // Extract every per-job Autoscaler under the lock and
    // join its thread outside the lock. Called from JM::stop() before
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
    int listener_fd_{-1};
    std::uint16_t bound_port_{0};
    std::thread accept_thread_;
    std::thread watchdog_thread_;
    std::thread checkpoint_thread_;
    std::atomic<bool> stop_{false};

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::vector<std::string> expected_tms_;
    std::unordered_map<std::string, std::shared_ptr<TmConnection>> registered_;
    std::vector<std::string> lost_tm_ids_;
    std::unordered_map<JobId, std::shared_ptr<JobState>> jobs_;
    // Ring buffer of terminated jobs. signal_job_completion_locked_
    // pushes back; the front is evicted once size exceeds
    // kJobManagerHistoryCap. Guarded by mu_ so job_history() can take
    // a consistent snapshot.
    std::deque<CompletedJobRecord> history_;
    JobId next_job_id_{1};
    // Convenience: the legacy `deploy(plan)`/`await_completion`/`errors`
    // path operates on whichever job was last deployed in-process. -1
    // means none yet.
    JobId legacy_active_job_id_{0};

    // Active client connections. Held as shared_ptr so the handler
    // thread and the back-pointer both keep the Connection alive;
    // dangling-pointer risk if the thread exits before stop()
    // would otherwise let close() touch freed memory.
    std::vector<std::thread> client_threads_;
    std::vector<std::shared_ptr<network::Connection>> client_conns_;
    std::mutex client_mu_;

    // Wraps an accepted listener fd into a Connection. Default plain
    // TCP via make_plain_connection. TLS callers replace via
    // set_accept_factory before start().
    AcceptFactory accept_factory_;
};

}  // namespace clink::cluster
