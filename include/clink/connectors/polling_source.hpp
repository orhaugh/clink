#pragma once

// Generic pull-based (polling) source base. Makes cursor-driven connectors -
// poll a REST API, tail a DB table, scan an object listing - thin: a connector
// supplies a `poll(cursor) -> {records, next_cursor}` callback and the base owns
// the produce loop, the interval throttle, and the cursor checkpoint.
//
// Unbounded by default (opt-in one-shot bounded mode via Options::bounded),
// AT-LEAST-ONCE: the cursor is persisted as operator state, so on
// restart the source re-polls from the last checkpointed cursor. The poll
// callback owns the cursor's inclusive/exclusive semantics - use an EXCLUSIVE
// cursor (fetch strictly AFTER it) to avoid re-emitting the boundary record on
// each poll and on replay.

#include <chrono>
#include <functional>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/operators/operator_base.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

template <typename T>
class PollingSource : public Source<T> {
public:
    struct PollResult {
        std::vector<T> records;
        std::string next_cursor;  // empty => keep the current cursor unchanged
    };

    // Fetch records produced since `cursor`. Runs on the source thread and MAY
    // block (an HTTP / DB round trip). Throwing propagates: the subtask fails
    // and the job restarts, replaying from the checkpointed cursor.
    using PollFn = std::function<PollResult(const std::string& cursor)>;

    struct Options {
        std::chrono::milliseconds interval{1000};  // minimum time between polls
        std::string initial_cursor;                // starting cursor on a fresh run
        // Random +/- fraction applied to each poll interval (0 = none, 0.2 =
        // +/-20%). De-synchronises many pollers so they do not all hit the
        // endpoint on the same tick (a thundering herd).
        double jitter_frac{0.0};
        // One-shot snapshot mode: read until a poll returns zero records, then
        // finish (produce() returns false, is_bounded() is true). The default
        // (false) is an endless tail. In bounded mode a zero-record poll is taken
        // as end-of-input, so the cursor query must return all currently-available
        // rows across pages before it drains (a row arriving AFTER the drain is not
        // picked up - that is the one-shot contract; use the unbounded tail for CDC).
        bool bounded{false};
        std::string name{"polling_source"};
    };

    PollingSource(Options opts, PollFn poll)
        : opts_(std::move(opts)),
          poll_(std::move(poll)),
          cursor_(opts_.initial_cursor),
          rng_(std::random_device{}()) {}

    // The interval after applying +/- jitter_frac, given a uniform u in [0,1).
    // Pure (the RNG draw is the caller's); unit-testable without timing.
    static std::chrono::milliseconds jittered_interval(std::chrono::milliseconds base,
                                                       double frac,
                                                       double u01) {
        if (frac <= 0.0) {
            return base;
        }
        if (frac > 1.0) {
            frac = 1.0;
        }
        const double factor = 1.0 + frac * (2.0 * u01 - 1.0);  // [1-frac, 1+frac]
        auto ms = static_cast<long long>(static_cast<double>(base.count()) * factor);
        return std::chrono::milliseconds{ms < 0 ? 0 : ms};
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled()) {
            return false;
        }
        throttle_();  // first poll is immediate; later polls wait out the interval
        if (this->cancelled()) {
            return false;
        }
        PollResult r = poll_(cursor_);
        if (!r.records.empty()) {
            Batch<T> batch;
            for (auto& rec : r.records) {
                batch.emplace(std::move(rec));
            }
            out.emit_data(std::move(batch));
        }
        if (!r.next_cursor.empty()) {
            cursor_ = std::move(r.next_cursor);
        }
        if (opts_.bounded && r.records.empty()) {
            return false;  // one-shot snapshot drained: no more rows
        }
        return !this->cancelled();
    }

    void cancel() override { Source<T>::cancel(); }

    [[nodiscard]] bool is_bounded() const noexcept override { return opts_.bounded; }

    std::string name() const override { return opts_.name; }

    void snapshot_offset(StateBackend& backend,
                         OperatorId op_id,
                         CheckpointId /*ckpt_id*/) override {
        backend.put_operator_state(
            op_id, kCursorKey, StateBackend::ValueView{cursor_.data(), cursor_.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        auto v = backend.get_operator_state(op_id, StateBackend::KeyView{kCursorKey});
        if (!v.has_value()) {
            return false;
        }
        cursor_.assign(reinterpret_cast<const char*>(v->data()), v->size());
        return true;
    }

    // Inspection (tests / metrics): the current resume cursor.
    [[nodiscard]] const std::string& cursor() const noexcept { return cursor_; }

private:
    void throttle_() {
        const auto now = std::chrono::steady_clock::now();
        if (polled_once_) {
            const auto target =
                jittered_interval(opts_.interval,
                                  opts_.jitter_frac,
                                  std::uniform_real_distribution<double>{0.0, 1.0}(rng_));
            const auto elapsed = now - last_poll_;
            if (elapsed < target) {
                std::this_thread::sleep_for(target - elapsed);
            }
        }
        last_poll_ = std::chrono::steady_clock::now();
        polled_once_ = true;
    }

    static constexpr const char* kCursorKey = "__poll_cursor__";

    Options opts_;
    PollFn poll_;
    std::string cursor_;
    std::chrono::steady_clock::time_point last_poll_{};
    bool polled_once_{false};
    std::mt19937 rng_;
};

}  // namespace clink
