#include "clink/embed/embedded_engine.hpp"

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/task_manager.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/script_runner.hpp"

namespace clink::embed {

namespace {

// The in-process TaskManager materialises compiled chains through the
// process-wide registries, so the built-ins plus the SQL operator set
// must be installed before the first submit. Once per process.
void ensure_factories_installed_once() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::sql::install(reg);
    });
}

// Distinct TM ids across engine instances (tests run several per process).
std::atomic<int> g_engine_seq{0};

}  // namespace

EmbeddedEngine::EmbeddedEngine(EngineOptions opts) : opts_(std::move(opts)) {
    ensure_factories_installed_once();
    if (!opts_.catalog_dir.empty()) {
        catalog_.load_from_dir(opts_.catalog_dir);
        catalog_.set_persistence_dir(opts_.catalog_dir);
    }
    const std::string tm_id = "embedded-tm-" + std::to_string(g_engine_seq.fetch_add(1));
    jm_port_ = jm_.start();  // ephemeral loopback port
    jm_.expect_tms({tm_id});
    cluster::TaskManager::Config cfg;
    cfg.slot_count = opts_.slots;
    tm_ = std::make_unique<cluster::TaskManager>(tm_id, "127.0.0.1", cfg);
    tm_->connect_to_jm("127.0.0.1", jm_port_);
    if (!jm_.await_registrations(std::chrono::milliseconds{10'000})) {
        throw std::runtime_error("EmbeddedEngine: in-process TaskManager failed to register");
    }
}

EmbeddedEngine::~EmbeddedEngine() {
    if (tm_) {
        tm_->stop();
    }
    jm_.stop();
}

int EmbeddedEngine::execute_script(const std::string& sql) {
    sql::ScriptRunOptions ropts;
    ropts.explain = opts_.explain;
    ropts.parallelism = opts_.parallelism;
    ropts.job_name = opts_.job_name;
    ropts.bare_select_to_print = opts_.bare_select_to_print;
    sql::ScriptIO io{opts_.out, opts_.err};
    auto submit = [this](const cluster::JobGraphSpec& spec, const std::string& name) -> int {
        cluster::CheckpointConfig ckpt;
        ckpt.checkpoint_dir = opts_.checkpoint_dir;
        if (!opts_.checkpoint_dir.empty()) {
            ckpt.interval_ms = opts_.checkpoint_interval_ms;
        }
        ckpt.state_backend_uri = opts_.state_backend_uri;
        try {
            const auto id = jm_.submit_job(
                spec, cluster::OperatorRegistry::default_instance(), {}, std::move(ckpt), nullptr);
            jobs_.push_back(JobEntry{id, name.empty() ? ("job_" + std::to_string(id)) : name});
        } catch (const std::exception& e) {
            *opts_.err << "error: submit failed: " << e.what() << "\n";
            return 1;
        }
        return 0;
    };
    return sql::run_script(sql, catalog_, ropts, io, submit);
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
