#include "clink/runtime/local_executor.hpp"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <string>
#include <utility>
#if defined(CLINK_HAS_STACKTRACE)
#include <stacktrace>
#endif

#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/operator_metrics.hpp"
#include "clink/runtime/cpu_affinity.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/state/state_migration_on_restore.hpp"

namespace clink {

LocalExecutor::LocalExecutor(Dag dag, JobConfig config)
    : dag_(std::move(dag)), config_(std::move(config)) {}

LocalExecutor::~LocalExecutor() {
    cancel();
    // jthread destructors will join
}

void LocalExecutor::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return;
    }
    // Restore state from snapshot before any operator starts processing.
    if (config_.state_backend && config_.restore_from.has_value()) {
        // FOUND-3: hand the backend the relocated savepoint dir (if any) so it
        // can rebase cp-dir references that embed a capture-time absolute path.
        if (!config_.restore_base.empty()) {
            config_.state_backend->set_restore_base(config_.restore_base);
        }
        config_.state_backend->restore(*config_.restore_from, config_.restore_key_group_filter);
        // State schema evolution: migrate the restored state up to the
        // versions the live job expects, before any operator reads it.
        // has_path-gated; throws on a missing path (the pre-deploy
        // checker should have caught it, but the HA auto-restart path
        // can restore without that gate, so this is the last line of
        // defence against silently reading stale-schema bytes).
        if (config_.expected_state_versions.has_value()) {
            migrate_restored_state(*config_.state_backend, *config_.expected_state_versions);
        }
    } else if (config_.state_backend && config_.expected_state_versions.has_value()) {
        // Fresh start (no restore): stamp the expected versions so the
        // snapshots this job produces record them, enabling a future
        // restore to compare and migrate.
        config_.state_backend->set_state_versions(*config_.expected_state_versions);
    }
    register_metrics();

    threads_.reserve(dag_.runners().size());
    contexts_.reserve(dag_.runners().size());
    // Resolve the dead-letter queue every subtask reports poison records to. When
    // the job did not supply one, install the default (logs bad records over the
    // host logger) so they are visible with zero config. Owned by default_dlq_ so
    // it outlives the contexts that point at it.
    DeadLetterQueue* dlq = config_.dead_letter_queue;
    if (dlq == nullptr) {
        default_dlq_ = std::make_unique<LoggingDeadLetterQueue>(config_.logger);
        dlq = default_dlq_.get();
    }
    for (std::size_t i = 0; i < dag_.runners().size(); ++i) {
        const auto& runner = dag_.runners()[i];
        contexts_.push_back(std::make_unique<RuntimeContext>(
            runner.id, runner.name, config_.state_backend.get(), config_.metrics));
        contexts_.back()->set_side_output_channels(dag_.side_channels_for(i));
        // Network-bridge byte attribution: a bridge's bytes belong to its
        // chain's primary operator, not the bridge's own (internal) op id.
        contexts_.back()->set_attributed_op_id(runner.attributed_op_id);
        if (config_.on_checkpoint_ack) {
            contexts_.back()->set_checkpoint_ack(config_.on_checkpoint_ack);
        }
        // Bounded-source EOS final-checkpoint hooks (cluster path only). Only
        // the source runner invokes them at clean bounded EOS; copying onto
        // every context is harmless (no other runner calls them).
        if (config_.request_final_checkpoint) {
            contexts_.back()->set_request_final_checkpoint(config_.request_final_checkpoint);
        }
        if (config_.wait_final_committed) {
            contexts_.back()->set_wait_final_committed(config_.wait_final_committed);
        }
        contexts_.back()->set_unaligned_checkpoints(config_.unaligned_checkpoints);
        // Give operators the restore key-group range so timer restore can
        // route each timer to the subtask owning its key group on a rescale
        // (full range = same-parallelism, keep all). See Operator::restore_timers.
        contexts_.back()->set_restore_key_group_range(config_.restore_key_group_filter);
        // Stamp the per-operator mode override (if any)
        // so this operator's runner can stamp barriers passing
        // through with the override mode.
        if (auto it = config_.barrier_mode_overrides_by_operator.find(runner.id);
            it != config_.barrier_mode_overrides_by_operator.end()) {
            contexts_.back()->set_barrier_mode_override(it->second);
        }
        // Thread the shared drain-target signal so the
        // source runner can poll it and emit DrainMarker on rescale.
        // Null in non-cluster paths; the runner observes 0 and
        // produces normally.
        contexts_.back()->set_drain_target_signal(config_.drain_target);
        // Host-owned logger threaded across the plugin boundary by data (see
        // JobConfig::logger). Null in in-process / legacy paths, where the
        // operator log helpers fall back to the process LogBuffer.
        contexts_.back()->set_logger(config_.logger);
        // Ambient DLQ: a connector routes a poison record here instead of dropping
        // it silently. Same instance on every context (it is thread-safe).
        contexts_.back()->set_dead_letter_queue(dlq);
        // Record-capture flight recorder: arm runners whose registration
        // supplied an input codec (no-op for the rest).
        if (!config_.capture_dir.empty()) {
            contexts_.back()->set_capture(
                config_.capture_dir, config_.capture_records, config_.capture_subtask_idx);
        }
        auto* ctx_ptr = contexts_.back().get();
        auto run_fn = runner.run;
        auto cancel_fn = runner.cancel;
        auto op_name = runner.name;
        // Stop predicate ORs the internal cancel flag with the external
        // token from JobConfig (if set). The TaskManager wires that
        // token into CancelJob handling so a client-initiated cancel
        // can wind the executor down without a reference to it.
        auto ext_token = config_.external_cancel_token;
        auto stop_predicate = [this, ext_token] {
            if (cancel_.load(std::memory_order_acquire)) {
                return true;
            }
            return ext_token != nullptr && ext_token->load(std::memory_order_acquire);
        };
        // Per-operator core for the opt-in pinning path: round-robin the
        // operator threads over the available cores. Best-effort (no-op where
        // hard affinity is unavailable), so it never affects correctness.
        const unsigned core = static_cast<unsigned>(i);
        const bool pin = config_.pin_operator_threads;
        // Catch operator-thread exceptions so a single failing operator
        // doesn't terminate the process. The error is recorded; channels
        // get closed via the runner's cancel hook so downstream threads
        // can drain and exit cleanly.
        threads_.emplace_back(
            [this, run_fn, ctx_ptr, stop_predicate, cancel_fn, op_name, core, pin](
                std::stop_token /*tok*/) {
                // Name the thread for diagnostics (top -H, perf, gdb) and pin it
                // when requested. Both are best-effort and never throw.
                set_current_thread_name(op_name);
                if (pin) {
                    pin_current_thread_to_core(core);
                }
                try {
                    run_fn(*ctx_ptr, stop_predicate);
                } catch (const std::exception& e) {
                    std::string message = e.what();
#if defined(CLINK_HAS_STACKTRACE)
                    // Best-effort trace. This is the CAPTURE site (the runner
                    // thread, post-unwind), not the throw site, so it shows the
                    // runner -> operator call chain rather than the exact throw
                    // frame; still useful for correlating the failure.
                    message += "\n--- stack trace (capture site) ---\n";
                    message += std::to_string(std::stacktrace::current());
#endif
                    {
                        std::lock_guard lock(error_mu_);
                        operator_errors_.emplace_back(op_name, std::move(message));
                    }
                    cancel_.store(true, std::memory_order_release);
                    if (cancel_fn) {
                        cancel_fn();
                    }
                }
            });
    }
    // Spawn the metrics-poll thread once operator threads exist. If
    // there's no MetricsRegistry on this JobConfig (some legacy in-
    // process tests), skip - the loop would just be a no-op anyway.
    if (config_.metrics != nullptr) {
        metrics_thread_ = std::jthread([this](std::stop_token /*tok*/) { metrics_poll_loop_(); });
    }
    // Watch the external cancel token (if set) so that the operator
    // runners don't have to poll it directly. When the token flips,
    // call cancel() which closes every runner's channels; pop_for
    // wakes from close immediately. Without this watcher each runner
    // would need a short pop_for timeout (the old 1s fallback) just
    // to notice the token, which produced an idle wakeup storm.
    if (config_.external_cancel_token) {
        external_cancel_watch_thread_ =
            std::jthread([this](std::stop_token /*tok*/) { external_cancel_watch_loop_(); });
    }
}

void LocalExecutor::await_termination() {
    for (auto& t : threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    threads_.clear();
    contexts_.clear();
    // Operator threads done → flip running_ so the metrics-poll loop
    // and the external-cancel watcher wake and exit, then join them.
    // Without this they'd block the executor's destructor on their
    // sleep intervals.
    running_.store(false, std::memory_order_release);
    if (metrics_thread_.joinable()) {
        metrics_thread_.join();
    }
    if (external_cancel_watch_thread_.joinable()) {
        external_cancel_watch_thread_.join();
    }
}

void LocalExecutor::cancel() {
    cancel_.store(true, std::memory_order_release);
    for (const auto& runner : dag_.runners()) {
        if (runner.cancel) {
            runner.cancel();
        }
    }
}

void LocalExecutor::run() {
    start();
    await_termination();
}

bool LocalExecutor::is_bounded_job() const noexcept {
    switch (config_.execution_mode) {
        case JobConfig::ExecutionMode::Batch:
            return true;
        case JobConfig::ExecutionMode::Streaming:
            return false;
        case JobConfig::ExecutionMode::Auto:
            break;
    }
    return dag_.all_sources_bounded();
}

void LocalExecutor::run_to_completion() {
    // Guard: a job explicitly declared Batch must actually be bounded, or it
    // would block here forever. Auto/Streaming need no guard - Auto only claims
    // boundedness when the sources back it, and a Streaming caller is opting
    // into the continuous path knowingly. Catching the contradiction up front
    // turns "hangs silently" into a clear error at the call site.
    if (config_.execution_mode == JobConfig::ExecutionMode::Batch && !dag_.all_sources_bounded()) {
        throw std::logic_error(
            "LocalExecutor::run_to_completion: execution_mode is Batch but the job has an "
            "unbounded source (or no source); it would never reach end-of-input. Mark every "
            "source bounded (Source::is_bounded) or use the streaming run() path.");
    }
    start();
    await_termination();
}

Snapshot LocalExecutor::take_savepoint(CheckpointId id) {
    if (!config_.state_backend) {
        throw std::logic_error(
            "LocalExecutor::take_savepoint: no state backend configured on this job");
    }
    if (running()) {
        throw std::logic_error(
            "LocalExecutor::take_savepoint: job is still running; call after "
            "run_to_completion()/await_termination() so the captured state is final");
    }
    return config_.state_backend->snapshot(id);
}

void LocalExecutor::register_metrics() {
    if (config_.metrics == nullptr) {
        return;
    }
    for (const auto& runner : dag_.runners()) {
        const auto id = runner.id.value();
        // Touch the backpressure gauges so they exist before the runner starts;
        // the metrics-poll thread updates them in metrics_poll_loop_(). Keyed by
        // op_id (clink_op_input_depth{op_id="N"}) so the per-operator overlay
        // joins them uniformly with the other clink_op_* series.
        config_.metrics->gauge(metrics::op_metric_name("input_depth", id));
        config_.metrics->gauge(metrics::op_metric_name("input_capacity", id));
        config_.metrics->gauge(metrics::op_metric_name("input_depth_high_water", id));
        // Identity mapping so a metrics scraper can map this numeric op_id back
        // to a graph node. Only runners that carry a spec node id (user
        // operators built via apply_chain_identity) emit it; internal runners
        // (bridges, fork/split) and uid-only keyed stages do not (the JM maps
        // uid'd nodes by computing operator_id_from_uid directly).
        metrics::op::op_info_set(config_.metrics, id, runner.spec_node_id, runner.spec_uid);
    }
}

void LocalExecutor::metrics_poll_loop_() {
    using namespace std::chrono_literals;
    // Capture (id, depth_fn, capacity_fn) up front so we don't touch
    // dag_.runners() inside the loop (it's stable for the lifetime of
    // the executor, but the local snapshot keeps the loop tight).
    struct Probe {
        std::string depth_name;
        std::string capacity_name;
        std::string high_water_name;
        std::function<std::size_t()> depth;
        std::function<std::size_t()> capacity;
        std::int64_t high_water{0};
    };
    std::vector<Probe> probes;
    probes.reserve(dag_.runners().size());
    for (const auto& runner : dag_.runners()) {
        Probe p;
        // clink_op_input_depth{op_id="N"} (and _capacity / _high_water), keyed
        // like the rest of the per-operator series so the overlay joins them.
        p.depth_name = metrics::op_metric_name("input_depth", runner.id.value());
        p.capacity_name = metrics::op_metric_name("input_capacity", runner.id.value());
        p.high_water_name = metrics::op_metric_name("input_depth_high_water", runner.id.value());
        p.depth = runner.input_depth;
        p.capacity = runner.input_capacity;
        probes.push_back(std::move(p));
    }

    while (running_.load(std::memory_order_acquire) && !cancel_.load(std::memory_order_acquire)) {
        for (auto& p : probes) {
            const std::int64_t d = p.depth ? static_cast<std::int64_t>(p.depth()) : 0;
            const std::int64_t c = p.capacity ? static_cast<std::int64_t>(p.capacity()) : 0;
            config_.metrics->gauge(p.depth_name).set(d);
            config_.metrics->gauge(p.capacity_name).set(c);
            if (d > p.high_water) {
                p.high_water = d;
                config_.metrics->gauge(p.high_water_name).set(d);
            }
        }
        // 100ms is fine-grained enough to catch transient saturation
        // without flooding the registry. Real dashboards scrape every
        // 5-15s; this just makes sure the values are fresh between
        // scrapes.
        std::this_thread::sleep_for(100ms);
    }
}

void LocalExecutor::external_cancel_watch_loop_() {
    using namespace std::chrono_literals;
    const auto& token = config_.external_cancel_token;
    while (running_.load(std::memory_order_acquire) && !cancel_.load(std::memory_order_acquire)) {
        if (token && token->load(std::memory_order_acquire)) {
            cancel();
            return;
        }
        // 100ms is enough to surface a token flip as cancellation
        // well within a human-perceivable response time; runners get
        // the channel-close wake immediately once cancel() fires.
        std::this_thread::sleep_for(100ms);
    }
}

}  // namespace clink
