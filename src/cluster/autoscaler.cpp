#include "clink/cluster/autoscaler.hpp"

#include <algorithm>
#include <utility>

#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

namespace clink::cluster {

namespace {

// Map a PidController output in [-1, 1] (the default) to a discrete
// parallelism delta: +1 / 0 / -1 subtask. The actuator (parallelism)
// has an inverting effect on the controlled signal: adding more
// subtasks REDUCES saturation. The PidController uses
// error = setpoint - measured, so:
//   measured > setpoint (over-saturated) -> negative output ->
//     scale UP (+1) to bring saturation down toward setpoint
//   measured < setpoint (under-utilized) -> positive output ->
//     scale DOWN (-1) to bring saturation up toward setpoint
// Multiplicative scaling (e.g. +k subtasks proportional to |output|)
// is a future tuning lever; the per-tick +/- 1 cadence is what the
// existing whole-job rescale paths already exercise and keeps the
// cutover state machine's load predictable.
constexpr std::int32_t discretize_delta(double output) noexcept {
    if (output < 0.0) {
        return +1;
    }
    if (output > 0.0) {
        return -1;
    }
    return 0;
}

}  // namespace

Autoscaler::Autoscaler(AutoscalerConfig cfg,
                       SampleFn sample,
                       RequestRescaleFn request,
                       StatusFn status)
    : cfg_(cfg),
      sample_fn_(std::move(sample)),
      request_fn_(std::move(request)),
      status_fn_(std::move(status)) {}

Autoscaler::~Autoscaler() {
    stop();
}

void Autoscaler::register_operator(const std::string& op_id) {
    std::lock_guard lock(mu_);
    if (ops_.find(op_id) == ops_.end()) {
        OpState state{clink::async::PidController{cfg_.pid}, {}, false};
        state.pid.set_setpoint(cfg_.setpoint);
        ops_.emplace(op_id, std::move(state));
    }
}

void Autoscaler::unregister_operator(const std::string& op_id) {
    std::lock_guard lock(mu_);
    ops_.erase(op_id);
}

void Autoscaler::reset_operator(const std::string& op_id) {
    std::lock_guard lock(mu_);
    auto it = ops_.find(op_id);
    if (it != ops_.end()) {
        it->second.pid.reset();
        it->second.has_last_request = false;
    }
}

std::vector<AutoscalerDecision> Autoscaler::tick(std::chrono::milliseconds dt) {
    std::vector<AutoscalerDecision> decisions;

    // Snapshot the op list under the lock so we can release it before
    // calling user-provided callbacks (sample_fn / status_fn may
    // acquire other locks, e.g. JobManager::mu_).
    std::vector<std::string> op_ids;
    {
        std::lock_guard lock(mu_);
        op_ids.reserve(ops_.size());
        for (const auto& [op_id, _] : ops_) {
            op_ids.push_back(op_id);
        }
    }

    const auto now = std::chrono::steady_clock::now();
    for (const auto& op_id : op_ids) {
        AutoscalerDecision dec;
        dec.op_id = op_id;

        // Look up live status. If the op disappeared between snapshot
        // and now (e.g., job cancelled), skip it - the caller will
        // unregister.
        auto status = status_fn_ ? status_fn_(op_id) : std::nullopt;
        if (!status.has_value()) {
            dec.reason = "no status (op unregistered or unknown)";
            clink::metrics::orch::autoscaler_decision("idle");
            decisions.push_back(std::move(dec));
            continue;
        }
        dec.current_parallelism = status->current_parallelism;

        // Mid-rescale: do not sample or stamp the PidController. The
        // sample window during a rescale is contaminated (drain
        // transients, fresh-subtask warmup). We pick back up on the
        // first tick after the rescale completes.
        if (status->state != RescaleState::Idle && status->state != RescaleState::Complete &&
            status->state != RescaleState::Aborted) {
            dec.reason =
                "rescale in progress (state=" + std::string{to_string(status->state)} + ")";
            clink::metrics::orch::autoscaler_decision("mid_rescale");
            decisions.push_back(std::move(dec));
            continue;
        }

        // Operator without bounds is not scalable. status_fn returned
        // min=max=0 in this case; mirror request_rescale's reject
        // message so dashboards see consistent text.
        if (status->min_parallelism == 0 && status->max_parallelism == 0) {
            dec.reason = "operator has no autoscale bounds";
            clink::metrics::orch::autoscaler_decision("no_bounds");
            decisions.push_back(std::move(dec));
            continue;
        }

        const double sample = sample_fn_ ? sample_fn_(op_id) : 0.0;
        dec.sample = sample;

        double pid_output = 0.0;
        {
            std::lock_guard lock(mu_);
            auto it = ops_.find(op_id);
            if (it == ops_.end()) {
                dec.reason = "unregistered between snapshot and update";
                decisions.push_back(std::move(dec));
                continue;
            }
            pid_output = it->second.pid.update(sample, dt);
        }
        dec.pid_output = pid_output;

        if (std::abs(pid_output) < cfg_.rescale_threshold) {
            dec.reason = "below rescale_threshold (hysteresis)";
            clink::metrics::orch::autoscaler_decision("idle");
            decisions.push_back(std::move(dec));
            continue;
        }

        // Discretize the controller output to +/- 1 subtask per tick.
        // See discretize_delta() above for the actuator-inversion
        // convention (more parallelism -> less saturation).
        const std::int32_t delta = discretize_delta(pid_output);
        const auto cur = static_cast<std::int64_t>(status->current_parallelism);
        std::int64_t target = cur + delta;
        target = std::clamp<std::int64_t>(target,
                                          static_cast<std::int64_t>(status->min_parallelism),
                                          static_cast<std::int64_t>(status->max_parallelism));
        const auto target_p = static_cast<std::uint32_t>(target);
        dec.target_parallelism = target_p;

        if (target_p == status->current_parallelism) {
            // delta>0 means we wanted to scale up; clamp pinned us at
            // max_parallelism. delta<0 means we wanted to scale down;
            // clamp pinned us at min_parallelism.
            dec.reason = (delta > 0 ? "already at max_parallelism" : "already at min_parallelism");
            clink::metrics::orch::autoscaler_decision("idle");
            decisions.push_back(std::move(dec));
            continue;
        }

        // Cooldown gate. Avoid double-firing while the cluster is
        // still bringing the previous rescale online.
        {
            std::lock_guard lock(mu_);
            auto it = ops_.find(op_id);
            if (it != ops_.end() && it->second.has_last_request) {
                if (now - it->second.last_request < cfg_.cooldown) {
                    dec.reason = "cooldown active";
                    clink::metrics::orch::autoscaler_decision("cooldown");
                    decisions.push_back(std::move(dec));
                    continue;
                }
            }
        }

        dec.requested = true;
        const auto result =
            request_fn_ ? request_fn_(op_id, target_p)
                        : RescaleCoordinator::RequestResult{.ok = false, .reason = "no request_fn"};
        dec.accepted = result.ok;
        dec.reason = result.reason.empty() ? std::string{"requested"} : result.reason;
        clink::metrics::orch::autoscaler_decision(result.ok ? "accepted" : "rejected");

        {
            std::lock_guard lock(mu_);
            auto it = ops_.find(op_id);
            if (it != ops_.end()) {
                if (result.ok) {
                    it->second.last_request = now;
                    it->second.has_last_request = true;
                } else {
                    // Hard reject (out of bounds, no coordinator, etc.):
                    // reset the integral so we don't keep recommending
                    // the same impossible rescale.
                    it->second.pid.reset();
                }
            }
        }

        log::info("autoscaler",
                  "op_id=" + op_id + " sample=" + std::to_string(sample) +
                      " output=" + std::to_string(pid_output) +
                      " current=" + std::to_string(status->current_parallelism) +
                      " target=" + std::to_string(target_p) +
                      " accepted=" + (result.ok ? "true" : "false") + " reason=" + result.reason);

        decisions.push_back(std::move(dec));
    }

    ticks_.fetch_add(1, std::memory_order_relaxed);
    clink::metrics::orch::autoscaler_tick();
    return decisions;
}

void Autoscaler::start() {
    bool was_running = running_.exchange(true, std::memory_order_acq_rel);
    if (was_running) {
        return;
    }
    stop_.store(false, std::memory_order_release);
    thread_ = std::thread(&Autoscaler::run_, this);
}

void Autoscaler::stop() {
    stop_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
    running_.store(false, std::memory_order_release);
}

void Autoscaler::run_() {
    auto last_tick = std::chrono::steady_clock::now();
    while (!stop_.load(std::memory_order_acquire)) {
        // Sleep in short chunks so stop() responds within ~50ms even
        // when sample_period is multiple seconds.
        auto remaining = cfg_.sample_period;
        while (remaining.count() > 0 && !stop_.load(std::memory_order_acquire)) {
            const auto step =
                std::min<std::chrono::milliseconds>(remaining, std::chrono::milliseconds{50});
            std::this_thread::sleep_for(step);
            remaining -= step;
        }
        if (stop_.load(std::memory_order_acquire)) {
            break;
        }
        const auto now = std::chrono::steady_clock::now();
        const auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_tick);
        last_tick = now;
        try {
            (void)tick(dt);
        } catch (const std::exception& e) {
            log::warn("autoscaler", std::string{"tick threw: "} + e.what());
        } catch (...) {
            log::warn("autoscaler", "tick threw non-exception");
        }
    }
}

}  // namespace clink::cluster
