#pragma once

// RemoteReadBackend - the first state backend whose reads can GENUINELY block on
// a remote tier, and whose get_async rides the async substrate (ASYNC-8).
//
// State is two tiers: a hot in-memory tier (recent writes + filled-on-read keys)
// and a cold remote tier reached through a pluggable RemoteLoader (abstracted so
// the backend is testable without an object store and reusable for any slow
// tier; the production binding loads from the DISAGG object pool). A read of a
// hot key is immediate; a read of a cold key must fetch from the remote tier.
//
// The point of ASYNC-8: that cold read does NOT block the operator thread. In
// the async path (get_async, used when an operator opts in and the runtime wires
// a ResumeScheduler to the AsyncExecutionController), a cold read suspends the
// record, the load runs on an IO thread, and the suspended coroutine is handed
// back to the RUNNER thread for resume via schedule_resume - coroutines only
// ever resume on the runner thread, exactly as the controller requires. The
// foreign IO thread only (a) runs the loader and (b) posts the handle; it never
// resumes a coroutine and never touches the hot tier. The write-through that
// fills the hot tier from a completed load runs in await_resume, i.e. back on
// the runner thread, so the hot tier is single-writer and needs no lock.
//
// The sync get() is the truly-blocking baseline: a cold key blocks the caller on
// the loader. Non-async operators use it unchanged; async operators get the
// non-blocking twin. If no ResumeScheduler is wired, get_async degrades to a
// safe inline blocking load (correct, just not deferred).
//
// Two constructions:
//   * Loader-only (the original ASYNC-8 read tier): cold reads go through a
//     RemoteLoader; snapshot/restore capture the hot tier only; no durable
//     remote write-back. Kept for read-through caching over an external loader
//     and for tests.
//   * Pool-backed (production disaggregation binding): state lives in a durable
//     RemotePool. Cold reads fetch (op,key) from the pool as-of the current
//     committed checkpoint; snapshot commits only the delta since the last
//     checkpoint (state is off the checkpoint-barrier path); restore is LAZY
//     (cold reads serve the restored checkpoint, nothing is loaded eagerly),
//     which is the fast-recovery / fast-rescale win. v1 bounds: same-parallelism
//     restore (rescale deferred) and no hot-tier eviction yet.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/async/task.hpp"
#include "clink/metrics/disagg_metrics.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/remote_pool.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

class RemoteReadBackend final : public StateBackend {
public:
    // Loads a cold key from the remote tier. Runs on an IO thread, so it MUST be
    // thread-safe. Returns nullopt if the key is absent remotely.
    using RemoteLoader = std::function<std::optional<Value>(OperatorId, std::string)>;
    // Posts a suspended coroutine handle back to the runner thread for resume.
    // The runner wires this to AsyncExecutionController::schedule_resume.
    using ResumeScheduler = std::function<void(std::coroutine_handle<>)>;

    explicit RemoteReadBackend(RemoteLoader loader, std::size_t io_threads = 1)
        : loader_(std::move(loader)) {
        start_workers_(io_threads);
    }

    // Pool-backed (production) construction. State is durable in `pool`.
    explicit RemoteReadBackend(std::shared_ptr<RemotePool> pool, std::size_t io_threads = 1)
        : pool_(std::move(pool)) {
        loader_ = [this](OperatorId op, std::string key) -> std::optional<Value> {
            return pool_->read(CheckpointId{last_ckpt_.load(std::memory_order_relaxed)}, op, key);
        };
        start_workers_(io_threads);
    }

    ~RemoteReadBackend() override {
        {
            std::lock_guard<std::mutex> lk(io_mu_);
            stop_ = true;
        }
        io_cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    RemoteReadBackend(const RemoteReadBackend&) = delete;
    RemoteReadBackend& operator=(const RemoteReadBackend&) = delete;

    // Wired once by the runner before any read, so the IO thread can post
    // resumptions to the controller. Not thread-safe vs concurrent reads (set
    // at wire-up). Overrides the StateBackend hook the runner calls generically
    // for any async-capable backend (the type matches AsyncResumeScheduler).
    void set_async_resume_scheduler(AsyncResumeScheduler s) override {
        resume_scheduler_ = std::move(s);
    }

    // --- sync StateBackend surface ---

    void put(OperatorId op, KeyView key, ValueView value) override {
        hot_.put(op, key, value);
        if (pool_) {  // track the write so the next checkpoint commits the delta
            auto k = std::make_pair(op.value(), std::string(key));
            deleted_.erase(k);
            dirty_.insert(std::move(k));
        }
    }

    // Hot hit, else a BLOCKING remote load (the truly-blocking read), filled
    // through into the hot tier.
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        if (auto v = hot_.get(op, key)) {
            ++hot_hits_;
            metrics::disagg::remote_hot_hit();
            return v;
        }
        std::string owned(key);
        auto v = timed_remote_load_(op, owned);
        if (v) {
            fill_hot_(op, owned, *v);
        }
        return v;
    }

    void erase(OperatorId op, KeyView key) override {
        hot_.erase(op, key);
        if (pool_) {
            auto k = std::make_pair(op.value(), std::string(key));
            dirty_.erase(k);
            deleted_.insert(std::move(k));
        }
    }
    void scan(OperatorId op, const ScanVisitor& visit) const override { hot_.scan(op, visit); }

    Snapshot snapshot(CheckpointId id) override {
        if (!pool_) {
            return hot_.snapshot(id);  // loader-only: hot tier only
        }
        // Commit only the delta since the last checkpoint; unchanged keys are
        // inherited from the base inside the pool. State stays off the
        // checkpoint-barrier path, so the framework Snapshot is just a marker.
        std::vector<RemotePoolEntry> changed;
        changed.reserve(dirty_.size());
        for (const auto& [opv, key] : dirty_) {
            const OperatorId op{opv};
            if (auto v = hot_.get(op, std::string_view{key})) {
                changed.push_back(RemotePoolEntry{op, key, std::move(*v)});
            }
        }
        std::vector<RemotePoolKey> deleted;
        deleted.reserve(deleted_.size());
        for (const auto& [opv, key] : deleted_) {
            deleted.push_back(RemotePoolKey{OperatorId{opv}, key});
        }
        const CheckpointId base{last_ckpt_.load(std::memory_order_relaxed)};
        pool_->commit(id, base, changed, deleted);
        last_ckpt_.store(id.value(), std::memory_order_relaxed);
        dirty_.clear();
        deleted_.clear();
        Snapshot s;
        s.checkpoint_id = id;  // the authoritative bytes are in the pool
        return s;
    }

    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        if (!pool_) {
            hot_.restore(snap, kg_filter);
            return;
        }
        // Lazy restore: point cold reads at the restored checkpoint and load
        // nothing eagerly (the fast-recovery win). v1: same-parallelism; the
        // rescale key-group filter is a follow-on.
        (void)kg_filter;
        last_ckpt_.store(snap.checkpoint_id.value(), std::memory_order_relaxed);
        dirty_.clear();
        deleted_.clear();
    }

    void purge_checkpoint(CheckpointId id) override {
        if (pool_) {
            pool_->purge(id);
        }
    }

    [[nodiscard]] std::string description() const override { return "remote-read"; }

    // --- async read surface (ASYNC-8) ---

    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }

    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        std::string owned(key);  // own the bytes across the suspension
        if (auto v = hot_.get(op, std::string_view{owned})) {
            ++hot_hits_;
            metrics::disagg::remote_hot_hit();
            co_return v;  // hot hit: no suspension, single resume
        }
        if (!resume_scheduler_) {
            // No runner to marshal resumes to: safe inline blocking load.
            auto v = timed_remote_load_(op, owned);
            if (v) {
                fill_hot_(op, owned, *v);
            }
            co_return v;
        }
        // Cold key + wired runner: suspend, load on an IO thread, resume on the
        // runner. await_resume (on the runner) does the write-through.
        co_return co_await RemoteLoad{this, op, std::move(owned)};
    }

    [[nodiscard]] std::uint64_t remote_loads() const noexcept {
        return remote_loads_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t hot_hits() const noexcept {
        return hot_hits_.load(std::memory_order_relaxed);
    }

private:
    // Awaiter for a cold read: the IO thread fills `value` then posts the handle
    // back to the runner; await_resume (runner) writes through and returns.
    struct RemoteLoad {
        const RemoteReadBackend* self;
        OperatorId op;
        std::string key;
        std::optional<Value> value;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            self->enqueue_io_([this, h]() {
                value = self->timed_remote_load_(op, key);  // BLOCKING remote read, IO thread
                self->resume_scheduler_(h);                 // hand back to the runner thread
            });
        }
        std::optional<Value> await_resume() {
            if (value) {
                self->fill_hot_(op, key, *value);  // write-through on the runner
            }
            return std::move(value);
        }
    };

    // Run the (blocking) remote loader, timing it and recording the disagg
    // remote-load counter + latency histogram (OBS-3). Central so the sync,
    // inline-async, and IO-thread paths all report identically.
    std::optional<Value> timed_remote_load_(OperatorId op, const std::string& key) const {
        const auto t0 = std::chrono::steady_clock::now();
        auto v = loader_(op, key);
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        remote_loads_.fetch_add(1, std::memory_order_relaxed);
        metrics::disagg::remote_load_observe(static_cast<std::uint64_t>(ns));
        return v;
    }

    void fill_hot_(OperatorId op, const std::string& key, const Value& v) const {
        hot_.put(op,
                 std::string_view{key},
                 std::string_view{reinterpret_cast<const char*>(v.data()), v.size()});
    }

    void enqueue_io_(std::function<void()> job) const {
        {
            std::lock_guard<std::mutex> lk(io_mu_);
            io_jobs_.push_back(std::move(job));
        }
        io_cv_.notify_one();
    }

    void start_workers_(std::size_t io_threads) {
        if (io_threads == 0) {
            io_threads = 1;
        }
        for (std::size_t i = 0; i < io_threads; ++i) {
            workers_.emplace_back([this] { io_loop_(); });
        }
    }

    void io_loop_() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lk(io_mu_);
                io_cv_.wait(lk, [this] { return stop_ || !io_jobs_.empty(); });
                if (stop_ && io_jobs_.empty()) {
                    return;
                }
                job = std::move(io_jobs_.front());
                io_jobs_.pop_front();
            }
            job();
        }
    }

    RemoteLoader loader_;
    ResumeScheduler resume_scheduler_;
    // Hot tier: only ever touched on the runner thread (sync calls + async
    // await_resume), so it needs no lock. mutable because reads fill it through.
    mutable InMemoryStateBackend hot_;

    mutable std::mutex io_mu_;
    mutable std::condition_variable io_cv_;
    mutable std::deque<std::function<void()>> io_jobs_;
    bool stop_{false};
    std::vector<std::thread> workers_;

    mutable std::atomic<std::uint64_t> remote_loads_{0};
    mutable std::atomic<std::uint64_t> hot_hits_{0};

    // Pool-backed (production) state. pool_ is null for the loader-only ctor.
    // last_ckpt_ is the checkpoint cold reads serve and the base for the next
    // commit; it is read on IO threads (the loader) and written on the runner
    // thread (snapshot/restore) after the async work has drained, so atomic.
    // dirty_/deleted_ track writes since the last checkpoint and are touched
    // only on the runner thread (the hot-tier single-writer invariant).
    std::shared_ptr<RemotePool> pool_;
    mutable std::atomic<std::uint64_t> last_ckpt_{0};
    std::set<std::pair<std::uint64_t, std::string>> dirty_;
    std::set<std::pair<std::uint64_t, std::string>> deleted_;
};

}  // namespace clink
