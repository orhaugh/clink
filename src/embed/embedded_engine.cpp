#include "clink/embed/embedded_engine.hpp"

#include <atomic>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/task_manager.hpp"
#include "clink/embed/collect_hub.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/sql/script_runner.hpp"
#ifdef CLINK_EMBED_LINKED_WASM
#include "clink/wasm/install.hpp"
#endif

namespace clink::embed {

namespace {

// The in-process TaskManager materialises compiled chains through the
// process-wide registries, so the built-ins plus the SQL operator set
// (and the embedded-only collect sink) must be installed before the
// first submit. Once per process.
void ensure_factories_installed_once() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::sql::install(reg);
        install_collect_sink();
#ifdef CLINK_EMBED_LINKED_WASM
        // CREATE FUNCTION ... LANGUAGE wasm in embedded execution: register
        // the wasm loader so the script runner can resolve the language.
        clink::wasm::install(reg);
#endif
    });
}

// Distinct TM ids across engine instances (tests run several per process).
std::atomic<int> g_engine_seq{0};

// A RecordBatchReader over one collect table's queue: ReadNext blocks
// until a batch, nullptr at end-of-stream, Cancelled after abort. Holds
// the queue alive so draining stays safe after the engine is destroyed.
class CollectReader final : public arrow::RecordBatchReader {
public:
    CollectReader(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<CollectQueue> queue)
        : schema_(std::move(schema)), queue_(std::move(queue)) {}

    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        auto r = queue_->next();
        if (!r.ok()) {
            return r.status();
        }
        *out = *r;
        return arrow::Status::OK();
    }

private:
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<CollectQueue> queue_;
};

}  // namespace

EmbeddedEngine::EmbeddedEngine(EngineOptions opts) : opts_(std::move(opts)) {
    ensure_factories_installed_once();
    if (!opts_.catalog_dir.empty()) {
        catalog_.load_from_dir(opts_.catalog_dir);
        catalog_.set_persistence_dir(opts_.catalog_dir);
    }
    collect_hub_ = std::make_shared<CollectHub>();
    collect_scope_ = CollectScopeRegistry::instance().register_hub(collect_hub_);
    const std::string tm_id = "embedded-tm-" + std::to_string(g_engine_seq.fetch_add(1));
    jm_port_ = jm_.start();  // ephemeral loopback port
    jm_.expect_tms({tm_id});
    cluster::TaskManager::Config cfg;
    cfg.slot_count = opts_.slots;
    tm_ = std::make_unique<cluster::TaskManager>(tm_id, "127.0.0.1", cfg);
    tm_->connect_to_jm("127.0.0.1", jm_port_);
    if (!jm_.await_registrations(std::chrono::milliseconds{10'000})) {
        CollectScopeRegistry::instance().unregister(collect_scope_);
        throw std::runtime_error("EmbeddedEngine: in-process TaskManager failed to register");
    }
}

EmbeddedEngine::~EmbeddedEngine() {
    // Wake any blocked collect readers first: they hold their queues alive
    // and see Cancelled; new sink instances can no longer resolve the scope.
    CollectScopeRegistry::instance().unregister(collect_scope_);
    if (collect_hub_) {
        collect_hub_->abort_all();
    }
    if (tm_) {
        tm_->stop();
    }
    jm_.stop();
}

int EmbeddedEngine::submit_spec_(const cluster::JobGraphSpec& spec,
                                 const std::string& name,
                                 std::ostream& err) {
    cluster::CheckpointConfig ckpt;
    ckpt.checkpoint_dir = opts_.checkpoint_dir;
    if (!opts_.checkpoint_dir.empty()) {
        ckpt.interval_ms = opts_.checkpoint_interval_ms;
    }
    ckpt.state_backend_uri = opts_.state_backend_uri;
    ckpt.capture_dir = opts_.capture_dir;
    ckpt.capture_records = opts_.capture_records;
    // Stamp this engine's scope token onto every collect sink so its
    // instances resolve THIS engine's queues (the factory is
    // process-wide, the queues are per-engine).
    cluster::JobGraphSpec stamped = spec;
    for (auto& op : stamped.ops) {
        if (op.type == "collect_sink_row") {
            op.params["collect_scope"] = collect_scope_;
        }
    }
    try {
        const auto id = jm_.submit_job(
            stamped, cluster::OperatorRegistry::default_instance(), {}, std::move(ckpt), nullptr);
        jobs_.push_back(JobEntry{id, name.empty() ? ("job_" + std::to_string(id)) : name});
    } catch (const std::exception& e) {
        err << "error: submit failed: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

int EmbeddedEngine::execute_script(const std::string& sql) {
    sql::ScriptRunOptions ropts;
    ropts.explain = opts_.explain;
    ropts.parallelism = opts_.parallelism;
    ropts.job_name = opts_.job_name;
    ropts.bare_select_to_print = opts_.bare_select_to_print;
    sql::ScriptIO io{opts_.out, opts_.err};
    auto submit = [this](const cluster::JobGraphSpec& spec, const std::string& name) -> int {
        return submit_spec_(spec, name, *opts_.err);
    };
    return sql::run_script(sql, catalog_, ropts, io, submit);
}

arrow::Result<std::string> EmbeddedEngine::submit_select_to_collect(const std::string& select_sql) {
    sql::ScriptRunOptions ropts;
    ropts.parallelism = opts_.parallelism;
    ropts.job_name = opts_.job_name;
    std::vector<std::string> tables;
    ropts.bare_select_to_collect = &tables;
    std::ostringstream out, err;
    sql::ScriptIO io{&out, &err};
    auto submit = [this, &err](const cluster::JobGraphSpec& spec, const std::string& name) -> int {
        return submit_spec_(spec, name, err);
    };
    if (sql::run_script(select_sql, catalog_, ropts, io, submit) != 0) {
        return arrow::Status::Invalid("SELECT failed: ", err.str());
    }
    if (tables.size() != 1) {
        return arrow::Status::Invalid("expected exactly one bare SELECT statement, got ",
                                      tables.size(),
                                      " (DDL and INSERT belong in updates)");
    }
    return tables.front();
}

arrow::Status EmbeddedEngine::execute_update(const std::string& sql) {
    sql::ScriptRunOptions ropts;
    ropts.parallelism = opts_.parallelism;
    ropts.job_name = opts_.job_name;
    std::ostringstream out, err;
    sql::ScriptIO io{&out, &err};
    const auto jobs_before = jobs_.size();
    auto submit = [this, &err](const cluster::JobGraphSpec& spec, const std::string& name) -> int {
        return submit_spec_(spec, name, err);
    };
    if (sql::run_script(sql, catalog_, ropts, io, submit) != 0) {
        return arrow::Status::Invalid("statement failed: ", err.str());
    }
    // An update that submitted jobs (INSERT INTO ...) completes them before
    // returning - update semantics are synchronous. Streaming pipelines
    // belong in queries, not updates.
    if (jobs_.size() > jobs_before && !await_all()) {
        std::string joined;
        for (const auto& e : errors_) {
            joined += e;
            joined += "; ";
        }
        return arrow::Status::Invalid("update job failed: ", joined);
    }
    return arrow::Status::OK();
}

std::vector<cluster::JobId> EmbeddedEngine::job_ids() const {
    std::vector<cluster::JobId> ids;
    ids.reserve(jobs_.size());
    for (const auto& j : jobs_) {
        ids.push_back(j.id);
    }
    return ids;
}

bool EmbeddedEngine::await_job(cluster::JobId id, std::chrono::milliseconds timeout) {
    return jm_.await_job_completion(id, timeout);
}

void EmbeddedEngine::cancel_job(cluster::JobId id) {
    try {
        jm_.cancel_job(id);
    } catch (const std::exception& e) {
        *opts_.err << "warning: cancel of job " << id << " failed: " << e.what() << "\n";
    }
}

std::vector<std::string> EmbeddedEngine::job_errors(cluster::JobId id) const {
    return jm_.job_errors(id);
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> EmbeddedEngine::collect_reader(
    const std::string& table) {
    const auto* def = catalog_.get_table(table);
    if (def == nullptr) {
        return arrow::Status::KeyError(
            "collect_reader: no table named '", table, "' in the engine catalog");
    }
    const auto conn = def->properties.find("connector");
    if (conn == def->properties.end() || conn->second != "collect") {
        return arrow::Status::Invalid(
            "collect_reader: table '", table, "' is not a connector='collect' table");
    }
    // The reader's schema must be byte-identical to what the sink builds:
    // both come from the same schema-driven batcher over the declared
    // columns (which maps unsupported column types to their utf8 fallback),
    // and both strip the batcher's prepended engine event-time column so
    // the host sees exactly the declared SELECT columns.
    std::vector<sql::RowColumn> cols;
    cols.reserve(def->columns.size() + 1);
    auto chit = def->properties.find("changelog");
    if (chit != def->properties.end() && chit->second == "true") {
        // Changelog-aware collect: the sink prepends the row-kind column
        // (insert / delete / update_before / update_after) - mirror it here
        // so both schemas stay byte-identical.
        cols.push_back(sql::RowColumn{"row_kind", arrow::utf8()});
    }
    for (const auto& c : def->columns) {
        cols.push_back(sql::RowColumn{c.name, c.type});
    }
    auto schema_r = sql::make_row_columnar_arrow_batcher(cols).schema()->RemoveField(0);
    if (!schema_r.ok()) {
        return schema_r.status();
    }
    auto schema = *schema_r;
    auto queue = collect_hub_->queue(table);
    if (!queue->claim_consumer()) {
        return arrow::Status::Invalid(
            "collect_reader: table '", table, "' already has a consumer (one reader per table)");
    }
    return std::static_pointer_cast<arrow::RecordBatchReader>(
        std::make_shared<CollectReader>(std::move(schema), std::move(queue)));
}

void EmbeddedEngine::cancel_all() {
    user_cancelled_ = true;
    for (const auto& j : jobs_) {
        try {
            jm_.cancel_job(j.id);
        } catch (const std::exception& e) {
            *opts_.err << "warning: cancel of job " << j.id << " failed: " << e.what() << "\n";
        }
    }
}

bool EmbeddedEngine::await_all(const std::function<bool()>& cancel_requested) {
    constexpr auto kSlice = std::chrono::milliseconds{200};
    // Post-cancel drain cap: a wedged cancel must not hang the caller.
    constexpr auto kDrainCap = std::chrono::seconds{30};
    std::vector<JobEntry> pending = jobs_;
    bool cancel_issued = false;
    auto cancel_deadline = std::chrono::steady_clock::time_point::max();
    while (!pending.empty()) {
        if (!cancel_issued && cancel_requested && cancel_requested()) {
            cancel_issued = true;
            *opts_.err << "stopping: cancelling " << pending.size()
                       << " running job(s), draining\n";
            cancel_all();
            cancel_deadline = std::chrono::steady_clock::now() + kDrainCap;
        }
        for (auto it = pending.begin(); it != pending.end();) {
            if (jm_.await_job_completion(it->id, kSlice)) {
                it = pending.erase(it);
            } else {
                ++it;
            }
        }
        if (std::chrono::steady_clock::now() > cancel_deadline) {
            *opts_.err << "warning: " << pending.size()
                       << " job(s) did not drain within 30s of cancel\n";
            break;
        }
    }
    const bool all_done = pending.empty();
    for (const auto& j : jobs_) {
        for (const auto& e : jm_.job_errors(j.id)) {
            errors_.push_back(j.name + ": " + e);
        }
    }
    if (user_cancelled_) {
        // A user stop is a normal outcome for an unbounded pipeline; errors
        // raised by the teardown are reported but not fatal.
        for (const auto& e : errors_) {
            *opts_.err << "note (post-cancel): " << e << "\n";
        }
        return all_done;
    }
    if (!errors_.empty()) {
        for (const auto& e : errors_) {
            *opts_.err << "error: " << e << "\n";
        }
        return false;
    }
    return all_done;
}

}  // namespace clink::embed
