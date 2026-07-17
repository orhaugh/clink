#pragma once

// EmbeddedEngine: the whole clink runtime in one process, no daemons.
//
// Construction starts an in-process Coordinator + Worker pair
// (control plane on an ephemeral loopback port, exactly the topology the
// SqlRuntime end-to-end suite proves). execute_script() runs a SQL
// script through the shared script runner (clink/sql/script_runner.hpp):
// DDL folds into the engine's session catalog and every compiled INSERT /
// materialized-view job is submitted straight to the in-process
// Coordinator - no HTTP, no cluster. await_all() blocks until the
// submitted jobs finish, with a caller-polled cancellation hook so a
// front end (Ctrl-C in `clink run`) can stop an unbounded pipeline and
// drain it cleanly.
//
// This is the execution core behind `clink run <file>.sql`; the
// embeddable C ABI (libclink) wraps the same class.

#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "clink/cluster/coordinator.hpp"
#include "clink/sql/catalog.hpp"

namespace clink::cluster {
class Worker;
}

namespace clink::embed {

class CollectHub;

struct EngineOptions {
    // Uniform op parallelism applied to every compiled job (>1 fans out).
    std::uint32_t parallelism = 1;
    // Worker slot count. Slots are placement bookkeeping, not threads,
    // so a generous default costs nothing and keeps deep plans deployable.
    std::size_t slots = 64;
    // Per-job state backend URI (e.g. "rocksdb:///tmp/state",
    // "remote-read://bucket/job"). Empty keeps the legacy resolution:
    // memory, or file when checkpoint_dir is set.
    std::string state_backend_uri;
    // Checkpoint root directory. Empty disables checkpointing.
    std::string checkpoint_dir;
    // Periodic checkpoint cadence, applied only when checkpoint_dir is set.
    std::int64_t checkpoint_interval_ms = 10'000;
    // Record-capture flight recorder: when non-empty, operator subtasks tee
    // their input records into per-checkpoint-epoch .cap files under this
    // directory (see runtime/record_capture.hpp). Pair with checkpoint_dir
    // so epochs align with restorable checkpoints. capture_records bounds
    // each epoch's stored records (0 = built-in default).
    std::string capture_dir;
    std::size_t capture_records = 0;
    // Optional persistent catalog directory (loaded at construction;
    // CREATE TABLE auto-saves). Empty keeps a session-only catalog.
    std::string catalog_dir;
    // Job-name override for submitted jobs (empty = per-statement defaults).
    std::string job_name;
    // Print LogicalPlans instead of submitting.
    bool explain = false;
    // Compile a bare top-level SELECT into a synthesised connector='print'
    // sink so results land on stdout (the `clink run -e "SELECT ..."` UX).
    bool bare_select_to_print = true;
    // Statement output and diagnostics. Must be non-null.
    std::ostream* out = &std::cout;
    std::ostream* err = &std::cerr;
};

class EmbeddedEngine {
public:
    // Starts the in-process cluster; throws std::runtime_error if the
    // Worker fails to register (or the catalog dir fails to load).
    explicit EmbeddedEngine(EngineOptions opts);
    ~EmbeddedEngine();

    EmbeddedEngine(const EmbeddedEngine&) = delete;
    EmbeddedEngine& operator=(const EmbeddedEngine&) = delete;
    EmbeddedEngine(EmbeddedEngine&&) = delete;
    EmbeddedEngine& operator=(EmbeddedEngine&&) = delete;

    // Process every statement of `sql` (see script_runner.hpp for the
    // statement surface). Compiled jobs are submitted immediately and
    // tracked; call await_all() to block on them. Returns 0 on success,
    // non-zero on the first failing statement.
    int execute_script(const std::string& sql);

    // Block until every submitted job reaches a terminal state. The
    // optional `cancel_requested` hook is polled between waits; the first
    // time it returns true every running job is cancelled and the drain
    // continues (bounded, so a wedged cancel cannot hang the caller).
    // Returns true when all jobs completed and either produced no errors
    // or the caller requested the stop (a user stop is a normal outcome
    // for an unbounded pipeline; teardown errors are reported as notes).
    bool await_all(const std::function<bool()>& cancel_requested = {});

    // Cancel every tracked job now (idempotent; await_all still drains).
    void cancel_all();

    // Per-job control (the libclink C surface builds on these).
    [[nodiscard]] std::vector<cluster::JobId> job_ids() const;
    // True when the job reached a terminal state within the timeout.
    bool await_job(cluster::JobId id, std::chrono::milliseconds timeout);
    void cancel_job(cluster::JobId id);
    [[nodiscard]] std::vector<std::string> job_errors(cluster::JobId id) const;

    // A blocking reader over a connector='collect' table's typed Arrow
    // batches (see collect_hub.hpp for the semantics: ReadNext blocks until
    // a batch, ends when the producing job's sinks close, and returns
    // Cancelled after the engine closes). Exactly one reader per table;
    // a second request errors. Valid before or after the producing job is
    // submitted. The reader stays safe to drain after the engine is
    // destroyed (it holds the queue alive; pending reads see Cancelled).
    arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> collect_reader(
        const std::string& table);

    // Compile ONE bare SELECT into a fresh synthesised connector='collect'
    // table and submit it as a job. The table is changelog-aware: a
    // retracting plan's stream carries a leading row_kind utf8 column.
    // Returns the synthesised table name to pass to collect_reader();
    // errors carry the script runner's diagnostics. The seam behind the
    // Flight SQL endpoint.
    arrow::Result<std::string> submit_select_to_collect(const std::string& select_sql);

    // Run a script capturing its diagnostics (no bare-SELECT synthesis:
    // updates are DDL / INSERT statements), then await the jobs it
    // submitted. The seam behind Flight SQL updates.
    arrow::Status execute_update(const std::string& sql);

    // Aggregated job errors collected by await_all (name-prefixed).
    [[nodiscard]] const std::vector<std::string>& errors() const { return errors_; }

    [[nodiscard]] std::size_t job_count() const { return jobs_.size(); }
    [[nodiscard]] sql::Catalog& catalog() { return catalog_; }
    [[nodiscard]] bool user_cancelled() const { return user_cancelled_; }

private:
    int submit_spec_(const cluster::JobGraphSpec& spec, const std::string& name, std::ostream& err);

    struct JobEntry {
        cluster::JobId id{};
        std::string name;
    };

    EngineOptions opts_;
    sql::Catalog catalog_;
    cluster::Coordinator coordinator_;
    std::uint16_t coordinator_port_{};
    std::unique_ptr<cluster::Worker> worker_;
    std::vector<JobEntry> jobs_;
    std::vector<std::string> errors_;
    bool user_cancelled_ = false;
    // connector='collect' plumbing: this engine's hub plus its token in the
    // process-wide scope registry (stamped onto collect ops at submit).
    std::shared_ptr<CollectHub> collect_hub_;
    std::string collect_scope_;
};

}  // namespace clink::embed
