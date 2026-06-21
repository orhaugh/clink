#pragma once

// CoalescingBackend (the transparent cross-record read coalescer for ASYNC-10).
//
// A StateBackend decorator that makes single-key-per-record async operators
// benefit from get_many_async WITHOUT changing the operator. Each record's
// `co_await kv.get_async(key)` lands in this backend's get_async, which does NOT
// issue a read immediately: it registers (op, key) into a pending batch and
// suspends the record. When the AsyncExecutionController is otherwise stuck (all
// in-flight records suspended on their reads, nothing ready), it calls this
// backend's flush(), which issues ONE inner get_many_async for the whole pending
// batch and scatters each result back to its waiting record. So N records that
// each read a distinct key collapse into one batched (and, on a content-
// addressed pool, hash-coalesced) round-trip instead of N.
//
// Safe by the same argument ASYNC-12 relies on: the controller's per-key gate
// means every pending read in a batch is for a DISTINCT key (a key's second
// record parks and never reaches its read until the first finishes), and the
// flush runs on the runner thread; the inner get_many_async rides the inner
// backend's normal async wiring (its resume scheduler is the controller's), so
// completion marshals back to the runner exactly as a single get_async would.
// Because drain()/drain_for_barrier() also flush when stuck, a checkpoint
// barrier drains the pending batch before capture - the cut stays consistent.
//
// Everything other than the async-read surface forwards to the inner backend
// verbatim (it is a pure decorator). Reads that arrive already batched
// (get_many_async) forward straight through - they need no further coalescing.
//
// SCOPE: this lands the mechanism + its controller hook. Wiring it into the
// production single-input / sharded runners (wrap the per-subtask backend when
// async is enabled, register flush() as the controller's flush hook, and flush
// in the steady-state poll loop as well as the drain) is the follow-on.

#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/async/task.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

class CoalescingBackend final : public StateBackend {
public:
    explicit CoalescingBackend(StateBackend& inner) : inner_(&inner) {}

    CoalescingBackend(const CoalescingBackend&) = delete;
    CoalescingBackend& operator=(const CoalescingBackend&) = delete;

    // --- synchronous surface: pure forwarding ---
    void put(OperatorId op, KeyView key, ValueView value) override { inner_->put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_->get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { inner_->erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { inner_->scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return inner_->snapshot(id); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg = {}) override {
        inner_->restore(snap, kg);
    }
    Snapshot combine_snapshots(std::vector<Snapshot> parts) const override {
        return inner_->combine_snapshots(std::move(parts));
    }
    void purge_checkpoint(CheckpointId id) override { inner_->purge_checkpoint(id); }
    void set_state_versions(StateVersionMap v) override {
        inner_->set_state_versions(std::move(v));
    }
    [[nodiscard]] StateVersionMap restored_state_versions() const override {
        return inner_->restored_state_versions();
    }
    void set_restore_base(const std::string& dir) override { inner_->set_restore_base(dir); }
    [[nodiscard]] bool supports_async_persist() const noexcept override {
        return inner_->supports_async_persist();
    }
    CaptureHandle capture(CheckpointId id) override { return inner_->capture(id); }
    Snapshot persist(CaptureHandle handle) override { return inner_->persist(std::move(handle)); }
    [[nodiscard]] std::string description() const override {
        return "coalescing(" + inner_->description() + ")";
    }

    // --- async read surface ---

    // True so the runner routes reads through get_async (and wires the resume
    // scheduler, which we forward to the inner backend so its get_many_async
    // marshals completions back to the controller).
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    void set_async_resume_scheduler(AsyncResumeScheduler s) override {
        inner_->set_async_resume_scheduler(std::move(s));
    }
    // Forward the deadline-aware hand-back to the inner backend too. Note the
    // coalesced path resumes parked records INLINE via flush()'s FlushTask (not
    // schedule_resume), so the order_key is inert when coalescing is active; the
    // forward keeps the seam complete (an op that opts into deadlines without
    // coalescing reads through the inner backend directly).
    void set_deadline_resume_scheduler(DeadlineResumeScheduler s) override {
        inner_->set_deadline_resume_scheduler(std::move(s));
    }

    // The runner's flush hook (and its post-process_async call) route here.
    bool flush_pending_reads() override { return flush(); }

    // The coalescing seam: register the read and suspend; flush() resolves it.
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await BatchedRead{this, op.value(), std::string(key), std::nullopt};
    }

    // Already-batched reads forward straight through (no further coalescing).
    async::Task<std::vector<std::optional<Value>>> get_many_async(
        OperatorId op, const std::vector<std::string>& keys) const override {
        return inner_->get_many_async(op, keys);
    }

    // Runner thread. Issue ONE inner get_many_async per pending operator and
    // scatter results back to the waiting records. Returns true if it had work
    // (so the controller knows it issued IO / made progress); false if nothing
    // was pending. The controller calls this when it is otherwise stuck.
    bool flush() {
        if (pending_.empty()) {
            return false;
        }
        std::map<std::uint64_t, std::vector<PendingRead>> batches;
        batches.swap(pending_);
        for (auto& [opv, reads] : batches) {
            launch_flush_(OperatorId{opv}, std::move(reads));
        }
        return true;
    }

    [[nodiscard]] std::size_t pending_reads() const noexcept {
        std::size_t n = 0;
        for (const auto& [_, reads] : pending_) {
            n += reads.size();
        }
        return n;
    }

private:
    struct PendingRead {
        std::string key;
        std::optional<Value>* slot;      // points into the suspended record's awaiter
        std::coroutine_handle<> handle;  // the record coroutine to resume
    };

    // Awaiter co_awaited by get_async: parks the record and records where its
    // result should land. `result` lives in the record's coroutine frame, so
    // the pointer stays valid until the record resumes.
    struct BatchedRead {
        const CoalescingBackend* cb;
        std::uint64_t op;
        std::string key;
        std::optional<Value> result;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            cb->pending_[op].push_back(PendingRead{std::move(key), &result, h});
        }
        std::optional<Value> await_resume() { return std::move(result); }
    };

    // Fire-and-forget flush coroutine: one batched read for `op`, then scatter
    // + resume each record on the runner thread. Self-destroys when done.
    struct FlushTask {
        struct promise_type {
            FlushTask get_return_object() noexcept { return {}; }
            std::suspend_never initial_suspend() noexcept { return {}; }
            std::suspend_never final_suspend() noexcept { return {}; }
            void return_void() noexcept {}
            void unhandled_exception() noexcept { std::terminate(); }
        };
    };

    FlushTask launch_flush_(OperatorId op, std::vector<PendingRead> reads) const {
        std::vector<std::string> keys;
        keys.reserve(reads.size());
        for (const auto& r : reads) {
            keys.push_back(r.key);
        }
        std::vector<std::optional<Value>> vals;
        try {
            vals = co_await inner_->get_many_async(op, keys);
        } catch (...) {
            // A backend error must not hang the parked records: leave vals
            // short so each unfilled slot resolves to nullopt (absent).
        }
        for (std::size_t i = 0; i < reads.size(); ++i) {
            if (i < vals.size()) {
                *reads[i].slot = std::move(vals[i]);
            }
            reads[i].handle.resume();  // runner thread: the record's post-read stage runs here
        }
    }

    StateBackend* inner_;
    mutable std::map<std::uint64_t, std::vector<PendingRead>> pending_;  // op -> parked reads
};

}  // namespace clink
