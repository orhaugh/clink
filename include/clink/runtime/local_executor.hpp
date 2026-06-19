#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/runtime/batch_scheduler.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink {

class MetricsRegistry;

// LocalExecutor runs a DAG in-process by spawning one thread per operator.
//
// This is intentionally the simplest possible runtime. Each operator runs in
// its own jthread and communicates via the bounded channels owned by the DAG.
// Backpressure is automatic: a slow consumer fills its inbox, which blocks the
// producer's emit() call, which fills the producer's inbox, and so on.
//
// More sophisticated runtimes (cooperative single-thread scheduler, work-
// stealing pool, NUMA-aware task placement, Seastar-style shard-per-core) can
// reuse the same DAG type and channel abstractions.
class LocalExecutor {
public:
    explicit LocalExecutor(Dag dag, JobConfig config = {});
    ~LocalExecutor();

    LocalExecutor(const LocalExecutor&) = delete;
    LocalExecutor& operator=(const LocalExecutor&) = delete;
    LocalExecutor(LocalExecutor&&) = delete;
    LocalExecutor& operator=(LocalExecutor&&) = delete;

    // Start all operator threads. Returns immediately.
    void start();

    // Wait for natural completion (all sources exhausted, all sinks drained).
    void await_termination();

    // Ask all operators to stop ASAP. Idempotent.
    void cancel();

    // Convenience: start, await, return.
    void run();

    // Compute the batch execution plan for this job's DAG (BATCH-3): the logical
    // stages cut at blocking edges plus the source split-count input for
    // parallelism recommendations. Pure analysis - it does not change execution
    // (the pragmatic scheduler keeps eager thread launch; see batch_scheduler.hpp
    // for why). num_key_groups defaults to the engine-wide kNumKeyGroups.
    [[nodiscard]] batch::BatchPlan compute_batch_plan(
        std::size_t num_key_groups = kNumKeyGroups) const {
        return batch::compute_batch_plan(dag_, num_key_groups);
    }

    // True iff this job is bounded under its configured execution mode
    // (BATCH-1). Resolves JobConfig::execution_mode against the DAG's sources:
    //   Batch     -> true (a Batch job is always treated as bounded; whether it
    //                actually terminates is validated by run_to_completion()).
    //   Streaming -> false.
    //   Auto      -> true iff every source is bounded (Dag::all_sources_bounded).
    [[nodiscard]] bool is_bounded_job() const noexcept;

    // Run a bounded job to completion: validate the job will terminate, then
    // run every operator thread to natural exhaustion and return. Equivalent to
    // run() but with an up-front guard - if the execution mode is Batch (or Auto
    // resolving to bounded) the job is guaranteed to reach end-of-input. If the
    // mode is forced Batch but some source is unbounded, this throws
    // std::logic_error rather than block forever. After it returns, inspect
    // operator_errors() for any operator-thread failures, exactly as with run().
    void run_to_completion();

    // Capture the job's final keyed state as a savepoint (BATCH-4). Snapshots
    // the configured state backend and returns the Snapshot, which a later job
    // can restore via JobConfig::restore_from - the batch-to-stream bootstrap:
    // run a bounded backfill to completion, take_savepoint(), then start a live
    // streaming job restoring from it so the stream begins with the batch-built
    // state. Throws if no state backend is configured, or if the job is still
    // running (call after run_to_completion() / await_termination() so the state
    // is final and not being mutated concurrently). The operators that own the
    // state must carry the same uid in both jobs so the OperatorId - and thus
    // the keyed state - lines up across the boundary.
    Snapshot take_savepoint(CheckpointId id = CheckpointId{0});

    bool running() const noexcept { return running_.load(std::memory_order_acquire); }

    // Errors caught from operator threads (operator name + exception
    // message). Empty if all operators ran cleanly.
    using OperatorError = std::pair<std::string, std::string>;
    std::vector<OperatorError> operator_errors() const {
        std::lock_guard lock(error_mu_);
        return operator_errors_;
    }

private:
    void register_metrics();
    void metrics_poll_loop_();
    // Watches JobConfig::external_cancel_token and calls cancel()
    // when it flips. Lets runners pop_for(long) on idle without
    // sacrificing cancellation responsiveness: the watcher closes
    // all channels which wakes every pop_for immediately.
    void external_cancel_watch_loop_();

    Dag dag_;
    JobConfig config_;
    std::atomic<bool> running_{false};
    std::atomic<bool> cancel_{false};
    std::vector<std::jthread> threads_;
    std::vector<std::unique_ptr<RuntimeContext>> contexts_;
    std::jthread metrics_thread_;
    std::jthread external_cancel_watch_thread_;
    mutable std::mutex error_mu_;
    std::vector<OperatorError> operator_errors_;
};

}  // namespace clink
