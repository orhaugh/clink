#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/operators/operator_base.hpp"

namespace clink {

// Captures every record into a vector. Test-only helper.
template <typename T>
class CollectingSink final : public Sink<T> {
public:
    void on_data(const Batch<T>& batch) override {
        std::lock_guard lock(mu_);
        for (const auto& r : batch) {
            collected_.push_back(r.value());
            collected_with_ts_.emplace_back(r.value(), r.event_time());
            collected_records_.push_back(r);
        }
    }

    void on_watermark(Watermark wm) override {
        std::lock_guard lock(mu_);
        last_watermark_ = wm;
    }

    void on_barrier(CheckpointBarrier b) override {
        std::lock_guard lock(mu_);
        last_barrier_ = b;
    }

    std::vector<T> collected() const {
        std::lock_guard lock(mu_);
        return collected_;
    }

    // Same records as collected(), paired with their event-time. Useful
    // when the test cares about firing time (e.g. windowed emissions).
    std::vector<std::pair<T, std::optional<EventTime>>> collected_with_event_times() const {
        std::lock_guard lock(mu_);
        return collected_with_ts_;
    }

    // Full Records as observed (value + event_time + pane info).
    // Window-operator tests use this to verify pane semantics.
    std::vector<Record<T>> collected_records() const {
        std::lock_guard lock(mu_);
        return collected_records_;
    }

    Watermark last_watermark() const {
        std::lock_guard lock(mu_);
        return last_watermark_;
    }

    CheckpointBarrier last_barrier() const {
        std::lock_guard lock(mu_);
        return last_barrier_;
    }

    std::string name() const override { return "collecting_sink"; }

private:
    mutable std::mutex mu_;
    std::vector<T> collected_;
    std::vector<std::pair<T, std::optional<EventTime>>> collected_with_ts_;
    std::vector<Record<T>> collected_records_;
    Watermark last_watermark_{Watermark::min()};
    CheckpointBarrier last_barrier_{};
};

// Lambda-driven sink: invoke a user callback on each record.
template <typename T>
class FunctionSink final : public Sink<T> {
public:
    using Callback = std::function<void(const T&)>;

    explicit FunctionSink(Callback cb, std::string name = "function_sink")
        : cb_(std::move(cb)), name_(std::move(name)) {}

    void on_data(const Batch<T>& batch) override {
        for (const auto& r : batch) {
            cb_(r.value());
        }
    }

    std::string name() const override { return name_; }

private:
    Callback cb_;
    std::string name_;
};

}  // namespace clink
