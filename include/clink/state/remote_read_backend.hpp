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
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/async/completion_executor.hpp"
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

    // io_threads sizes the DEFAULT completion executor (a sized thread pool);
    // inject `executor` to share one sized IO pool across backends or to use
    // the io_uring-backed executor on Linux. The completion source (thread pool
    // vs io_uring) is swapped here without touching the async read path or the
    // controller - the executor only runs the load off-thread and posts the
    // resume (ASYNC-9).
    explicit RemoteReadBackend(RemoteLoader loader,
                               std::size_t io_threads = async::kDefaultIoThreads,
                               std::shared_ptr<async::CompletionExecutor> executor = nullptr)
        : loader_(std::move(loader)) {
        executor_ = executor ? std::move(executor)
                             : std::make_shared<async::ThreadPoolCompletionExecutor>(io_threads);
    }

    // Pool-backed (production) construction. State is durable in `pool`.
    //
    // hot_max_bytes bounds the in-memory hot tier (key+value bytes). 0 =
    // unbounded (the legacy behaviour: the hot tier holds the whole working
    // set, so only RESTORE is disaggregated). A non-zero budget makes working
    // state genuinely exceed RAM: once over budget, the LRU CLEAN keys (those
    // already durable in the pool at the last committed checkpoint) are evicted
    // and re-fetched from the pool on next read. Dirty keys - written since the
    // last checkpoint - are NEVER evicted (the pool does not yet hold their
    // latest value), so eviction never loses state. Only meaningful with a pool
    // (the durable tier); the loader-only ctor has no write-back, so it stays
    // unbounded regardless.
    explicit RemoteReadBackend(std::shared_ptr<RemotePool> pool,
                               std::size_t io_threads = async::kDefaultIoThreads,
                               std::size_t hot_max_bytes = 0,
                               std::shared_ptr<async::CompletionExecutor> executor = nullptr)
        : pool_(std::move(pool)), hot_max_bytes_(hot_max_bytes) {
        executor_ = executor ? std::move(executor)
                             : std::make_shared<async::ThreadPoolCompletionExecutor>(io_threads);
        loader_ = [this](OperatorId op, std::string key) -> std::optional<Value> {
            const auto ck = last_ckpt_.load(std::memory_order_relaxed);
            // No committed checkpoint yet (fresh job, nothing restored): there
            // is definitionally no cold state, so don't probe the pool. A
            // store like S3 treats a missing checkpoint manifest as an error
            // (unlike the in-memory double, which returns nullopt), so reading
            // checkpoint 0 would throw on every first-touch key and starve the
            // operator of output. Absent == nullopt is the correct contract.
            if (ck == 0) {
                return std::nullopt;
            }
            return pool_->read(CheckpointId{ck}, op, key);
        };
    }

    ~RemoteReadBackend() override {
        // Drop our reference to the executor BEFORE the members its in-flight
        // jobs touch (loader_/pool_/resume_scheduler_/hot_) are destroyed. If
        // this is the sole owner (the default per-backend executor) the reset
        // joins its workers here, so any in-flight load finishes against
        // still-live state; a shared executor (more than one owner) relies on
        // the runner having drained before teardown (the ASYNC-5/6 barrier
        // contract), and is released by the last owner.
        executor_.reset();
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

    // Deadline-aware hand-back (ASYNC-12 consumer), wired alongside the plain
    // scheduler when the operator opts into deadline-aware resume. When set,
    // post_resume_ uses it so the cold-load completion carries its order_key.
    void set_deadline_resume_scheduler(DeadlineResumeScheduler s) override {
        deadline_scheduler_ = std::move(s);
    }

    // --- sync StateBackend surface ---

    void put(OperatorId op, KeyView key, ValueView value) override {
        std::lock_guard<std::mutex> lk(sync_mu_);
        hot_.put(op, key, value);
        if (pool_) {  // track the write so the next checkpoint commits the delta
            auto k = std::make_pair(op.value(), std::string(key));
            deleted_.erase(k);
            dirty_.insert(std::move(k));
        }
        touch_(op, key, value.size());  // hot-tier recency + byte accounting
        maybe_evict_();
    }

    // Hot hit, else a BLOCKING remote load (the truly-blocking read), filled
    // through into the hot tier.
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        {
            std::lock_guard<std::mutex> lk(sync_mu_);
            if (auto v = hot_.get(op, key)) {
                ++hot_hits_;
                metrics::disagg::remote_hot_hit();
                return v;
            }
            // Erased since the last checkpoint: logically absent. The pool still
            // holds the pre-erase value (the delete is only committed at the
            // next checkpoint), so we must NOT load + return it - and must not
            // re-cache.
            if (pool_ && is_deleted_(op, key)) {
                return std::nullopt;
            }
        }
        // Blocking remote load with the lock RELEASED, so a concurrent snapshot
        // (owner thread) or put (writer thread) is not stalled behind the read.
        std::string owned(key);
        auto v = timed_remote_load_(op, owned);
        if (v) {
            std::lock_guard<std::mutex> lk(sync_mu_);
            fill_hot_(op, owned, *v);
        }
        return v;
    }

    void erase(OperatorId op, KeyView key) override {
        std::lock_guard<std::mutex> lk(sync_mu_);
        hot_.erase(op, key);
        if (pool_) {
            auto k = std::make_pair(op.value(), std::string(key));
            dirty_.erase(k);
            deleted_.insert(std::move(k));
        }
        forget_hot_(op, key);  // drop from the hot-tier index + byte total
    }
    void scan(OperatorId op, const ScanVisitor& visit) const override {
        std::lock_guard<std::mutex> lk(sync_mu_);
        hot_.scan(op, visit);
    }

    // Synchronous snapshot = capture (point-in-time delta on the operator
    // thread) + persist (durable pool commit). The async path drives the two
    // halves separately so the slow S3 commit lands on the snapshot worker
    // instead of the checkpoint-barrier / operator thread.
    Snapshot snapshot(CheckpointId id) override { return persist(capture(id)); }

    // Pool-backed remote state moves its durable commit off the operator thread;
    // loader-only (no pool) keeps the plain synchronous hot-tier snapshot.
    [[nodiscard]] bool supports_async_persist() const noexcept override { return pool_ != nullptr; }

    // Operator-thread phase: take a point-in-time copy of the delta since the
    // last checkpoint WITHOUT touching the pool. The captured (op,key) keys are
    // pinned (persisting_*) until persist() durably commits them, so during the
    // off-thread commit window a read still sees THIS checkpoint's effect (an
    // updated value stays hot + non-evictable; a deleted key keeps reading
    // absent) instead of cold-loading the stale pre-delta value from the pool.
    // dirty_/deleted_ are cleared so the NEXT checkpoint captures only writes
    // after this point - the consistent cut that also closes the concurrent-
    // writer window (a put landing after capture() lands in the fresh dirty_,
    // never in this already-detached delta).
    //
    // last_captured_ (the next delta's base) advances HERE, but last_ckpt_ (the
    // checkpoint cold reads load from) advances only in persist(): until the
    // commit lands, cold reads must still target the previously COMMITTED
    // checkpoint, never this id (which the pool does not have yet). The base
    // chain stays correct because the snapshot worker persists FIFO, so by the
    // time persist(id) commits with base=<prev id>, persist(<prev id>) has run.
    CaptureHandle capture(CheckpointId id) override {
        std::lock_guard<std::mutex> lk(sync_mu_);
        if (!pool_) {
            auto snap = hot_.snapshot(id);  // loader-only: detached hot blob
            return CaptureHandle{.checkpoint_id = id, .bytes = std::move(snap.bytes)};
        }
        if (id.value() <= last_captured_) {
            // This id was already captured (a second runner of an in-process
            // shared backend hitting the same barrier). The first capture took
            // the delta into pending_; do not re-capture an empty one and clobber
            // it. The worker still persists the stored pending_[id] exactly once
            // (a duplicate persist for the same id is a no-op).
            return CaptureHandle{.checkpoint_id = id, .bytes = {}};
        }
        PendingCommit pc;
        pc.base = CheckpointId{last_captured_};  // inherit from the prior captured ckpt
        pc.changed.reserve(dirty_.size());
        for (const auto& k : dirty_) {
            const OperatorId op{k.first};
            if (auto v = hot_.get(op, std::string_view{k.second})) {
                pc.changed.push_back(RemotePoolEntry{op, k.second, std::move(*v)});
            }
            ++persisting_dirty_[k];  // pin hot until durable
        }
        pc.deleted.reserve(deleted_.size());
        for (const auto& k : deleted_) {
            pc.deleted.push_back(RemotePoolKey{OperatorId{k.first}, k.second});
            ++persisting_deleted_[k];  // keep serving 'absent' until durable
        }
        last_captured_ = id.value();
        dirty_.clear();
        deleted_.clear();
        pending_[id.value()] = std::move(pc);
        return CaptureHandle{.checkpoint_id = id, .bytes = {}};
    }

    // Worker-thread phase: durably commit the captured delta to the pool OFF the
    // operator thread, then release the persisting_* pins (the keys are now
    // durable, hence eligible for eviction). Safe to run concurrently with live
    // put/get/erase: pool_->commit touches only the detached PendingCommit (never
    // live hot_/dirty_), and only the brief pin-release + evict re-takes the
    // lock. A failed commit propagates: the runner fails the checkpoint, which
    // triggers whole-job rollback + source replay (the existing no-loss model).
    Snapshot persist(CaptureHandle handle) override {
        if (!pool_) {
            return Snapshot{.checkpoint_id = handle.checkpoint_id,
                            .bytes = std::move(handle.bytes)};  // loader-only hot blob
        }
        PendingCommit pc;
        {
            std::lock_guard<std::mutex> lk(sync_mu_);
            auto it = pending_.find(handle.checkpoint_id.value());
            if (it == pending_.end()) {
                // No captured delta (already persisted, or a same-id duplicate
                // capture the first call already drained): the pool holds this id.
                return Snapshot{.checkpoint_id = handle.checkpoint_id};
            }
            pc = std::move(it->second);
            pending_.erase(it);
        }
        pool_->commit(handle.checkpoint_id, pc.base, pc.changed, pc.deleted);
        {
            std::lock_guard<std::mutex> lk(sync_mu_);
            // The pool now holds this id, so cold reads may target it. Advance
            // last_ckpt_ (the loader's checkpoint) BEFORE releasing the pins, so a
            // just-unpinned delta key cold-reads from this committed id, never the
            // prior one. Monotonic: a stale (out-of-order) persist never rewinds.
            if (handle.checkpoint_id.value() > last_ckpt_.load(std::memory_order_relaxed)) {
                last_ckpt_.store(handle.checkpoint_id.value(), std::memory_order_relaxed);
            }
            for (const auto& e : pc.changed) {
                unpin_(persisting_dirty_, std::make_pair(e.op.value(), e.key));
            }
            for (const auto& d : pc.deleted) {
                unpin_(persisting_deleted_, std::make_pair(d.op.value(), d.key));
            }
            // Now durable at `id`: the keys pinned during the commit are eligible
            // for eviction - run the budget down and publish the footprint.
            maybe_evict_();
            metrics::disagg::hot_resident_bytes_set(static_cast<std::int64_t>(hot_bytes_));
        }
        return Snapshot{.checkpoint_id = handle.checkpoint_id};  // bytes are in the pool
    }

    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        std::lock_guard<std::mutex> lk(sync_mu_);
        if (!pool_) {
            hot_.restore(snap, kg_filter);
            return;
        }
        // Rescale / cross-location: let the pool materialise the restored
        // checkpoint under this subtask's own location first (merge the parent
        // checkpoints, key-group-filter to `kg_filter`, relocate objects, write
        // the merged manifest as cp-restore_cp). A no-op for same-location
        // same-parallelism restore. After this, cold reads + the first
        // incremental commit work against this subtask's own prefix.
        pool_->prepare_restore(snap.checkpoint_id, kg_filter);
        // Drop any stale hot-tier entries before repointing at the new
        // checkpoint. A fresh backend has an empty hot tier (the production
        // path), so this is a no-op there; it makes restore() self-consistent
        // if a backend instance is reused to restore a DIFFERENT checkpoint
        // (the hot tier must not keep serving the old checkpoint's values).
        hot_.clear();
        lru_.clear();
        index_.clear();
        hot_bytes_ = 0;
        last_ckpt_.store(snap.checkpoint_id.value(), std::memory_order_relaxed);
        last_captured_ = snap.checkpoint_id.value();  // next delta inherits from here
        dirty_.clear();
        deleted_.clear();
        // Drop any in-flight async-persist bookkeeping: a restore reseeds the
        // delta baseline, so a partially-captured checkpoint from a prior run of
        // this instance must not leak pins or a stale pending commit.
        pending_.clear();
        persisting_dirty_.clear();
        persisting_deleted_.clear();
    }

    void purge_checkpoint(CheckpointId id) override {
        if (pool_) {
            pool_->purge(id);
        }
    }

    [[nodiscard]] std::string description() const override { return "remote-read"; }

    // --- async read surface (ASYNC-8) ---

    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }

    // The 2-arg path is the deadline-aware body with an unset (0) order_key.
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        return get_async(op, key, 0);
    }

    // Deadline-aware read (ASYNC-12 consumer): identical to the 2-arg path
    // except the cold-load awaiter carries `order_key` to its completion
    // hand-back, so a poll's ready batch resumes most-urgent-first under
    // ResumeOrder::Priority. order_key is inert on the hot-hit / inline-fallback
    // paths (no suspension to reorder).
    async::Task<std::optional<Value>> get_async(OperatorId op,
                                                KeyView key,
                                                std::uint64_t order_key) const override {
        std::string owned(key);  // own the bytes across the suspension
        {
            std::lock_guard<std::mutex> lk(sync_mu_);
            if (auto v = hot_.get(op, std::string_view{owned})) {
                ++hot_hits_;
                metrics::disagg::remote_hot_hit();
                co_return v;  // hot hit: no suspension, single resume
            }
            // Erased-but-uncommitted: absent (see get()). Short-circuit before
            // any suspension so we never load or re-cache the pre-erase value.
            if (pool_ && is_deleted_(op, std::string_view{owned})) {
                co_return std::nullopt;
            }
        }
        if (!resume_scheduler_) {
            // No runner to marshal resumes to: safe inline blocking load. The
            // lock is held only for the write-through, never the load itself.
            auto v = timed_remote_load_(op, owned);
            if (v) {
                std::lock_guard<std::mutex> lk(sync_mu_);
                fill_hot_(op, owned, *v);
            }
            co_return v;
        }
        // Cold key + wired runner: suspend, load on an IO thread, resume on the
        // runner. await_resume (on the runner) does the write-through.
        co_return co_await RemoteLoad{this, op, std::move(owned), order_key};
    }

    // Batched read (ASYNC-10): serve hot hits immediately, then fetch ALL cold
    // misses in ONE batched call with a SINGLE suspension (vs N suspensions +
    // N round-trips on get_async-per-key). The pool's read_many coalesces
    // same-content-hash objects into one fetch; write-through of every cold
    // value happens on the runner thread (await_resume), so the hot tier stays
    // single-writer. Results scatter back positionally.
    async::Task<std::vector<std::optional<Value>>> get_many_async(
        OperatorId op, const std::vector<std::string>& keys) const override {
        std::vector<std::optional<Value>> out(keys.size());
        std::vector<std::size_t> cold_idx;
        std::vector<std::string> cold_keys;
        {
            std::lock_guard<std::mutex> lk(sync_mu_);
            for (std::size_t i = 0; i < keys.size(); ++i) {
                if (auto v = hot_.get(op, std::string_view{keys[i]})) {
                    ++hot_hits_;
                    metrics::disagg::remote_hot_hit();
                    out[i] = std::move(v);  // hot hit
                } else if (pool_ && is_deleted_(op, std::string_view{keys[i]})) {
                    out[i] = std::nullopt;  // erased-but-uncommitted: absent
                } else {
                    cold_idx.push_back(i);
                    cold_keys.push_back(keys[i]);  // own across the suspension
                }
            }
        }
        if (cold_keys.empty()) {
            co_return out;  // all hot/deleted: no suspension
        }
        if (!resume_scheduler_) {
            // No runner: inline batched blocking load (lock held only for the
            // write-through, never the load).
            auto vals = batch_remote_load_(op, cold_keys);
            {
                std::lock_guard<std::mutex> lk(sync_mu_);
                for (std::size_t j = 0; j < cold_keys.size(); ++j) {
                    if (vals[j]) {
                        fill_hot_(op, cold_keys[j], *vals[j]);
                    }
                }
            }
            for (std::size_t j = 0; j < cold_idx.size(); ++j) {
                out[cold_idx[j]] = std::move(vals[j]);
            }
            co_return out;
        }
        // Cold misses + wired runner: ONE batched fetch, ONE suspension.
        auto vals = co_await RemoteLoadMany{this, op, std::move(cold_keys), {}};
        for (std::size_t j = 0; j < cold_idx.size(); ++j) {
            out[cold_idx[j]] = std::move(vals[j]);
        }
        co_return out;
    }

    [[nodiscard]] std::uint64_t remote_loads() const noexcept {
        return remote_loads_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t hot_hits() const noexcept {
        return hot_hits_.load(std::memory_order_relaxed);
    }
    // Hot-tier observability (only tracked when a byte budget is set + pooled).
    [[nodiscard]] std::uint64_t hot_evictions() const noexcept {
        return hot_evictions_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::size_t hot_resident_bytes() const noexcept { return hot_bytes_; }
    [[nodiscard]] std::size_t hot_resident_keys() const noexcept { return index_.size(); }

private:
    // Awaiter for a cold read: the IO thread fills `value` then posts the handle
    // back to the runner; await_resume (runner) writes through and returns.
    struct RemoteLoad {
        const RemoteReadBackend* self;
        OperatorId op;
        std::string key;
        std::uint64_t order_key{0};  // ASYNC-12 resume priority (0 = unset/FIFO)
        std::optional<Value> value{};

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            self->executor_->submit_blocking([this, h]() {
                value = self->timed_remote_load_(op, key);  // BLOCKING remote read, executor thread
                self->post_resume_(h, order_key);           // hand back to the runner thread
            });
        }
        std::optional<Value> await_resume() {
            if (value) {
                // Write-through on the runner thread, under the backend lock so
                // it cannot race a concurrent put/snapshot on a sibling runner.
                std::lock_guard<std::mutex> lk(self->sync_mu_);
                self->fill_hot_(op, key, *value);
            }
            return std::move(value);
        }
    };

    // Batched awaiter (ASYNC-10): one suspension for the whole cold-key batch.
    // The executor runs ONE batched remote read (the pool coalesces same-hash
    // objects), then posts the handle back; await_resume writes every cold
    // value through on the runner thread and returns them positionally.
    struct RemoteLoadMany {
        const RemoteReadBackend* self;
        OperatorId op;
        std::vector<std::string> keys;
        std::vector<std::optional<Value>> values;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            self->executor_->submit_blocking([this, h]() {
                values = self->batch_remote_load_(op, keys);  // batched read, executor thread
                self->post_resume_(h, 0);  // hand back to the runner thread (FIFO)
            });
        }
        std::vector<std::optional<Value>> await_resume() {
            std::lock_guard<std::mutex> lk(self->sync_mu_);
            for (std::size_t j = 0; j < keys.size(); ++j) {
                if (values[j]) {
                    self->fill_hot_(op, keys[j], *values[j]);
                }
            }
            return std::move(values);
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

    // Batched remote read (ASYNC-10): one round-trip class for the whole cold
    // batch. Pool path uses RemotePool::read_many (the S3 pool coalesces
    // same-content-hash objects + loads the manifest once); loader-only path
    // loops the opaque loader (no coalescing possible). Times the WHOLE batch
    // as one latency observation and counts one logical load per key; the
    // pool's own object-fetch counter reflects the coalesced GET count. Runs on
    // an executor thread, so it must be thread-safe (it touches only
    // pool_/loader_/last_ckpt_, never the sync_mu_-guarded working set).
    std::vector<std::optional<Value>> batch_remote_load_(
        OperatorId op, const std::vector<std::string>& keys) const {
        const auto t0 = std::chrono::steady_clock::now();
        std::vector<std::optional<Value>> vals;
        if (pool_) {
            const auto ck = last_ckpt_.load(std::memory_order_relaxed);
            if (ck == 0) {
                vals.assign(keys.size(), std::nullopt);  // no committed checkpoint: all absent
            } else {
                vals = pool_->read_many(CheckpointId{ck}, op, keys);
            }
        } else {
            vals.reserve(keys.size());
            for (const auto& k : keys) {
                vals.push_back(loader_(op, k));
            }
        }
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        remote_loads_.fetch_add(keys.size(), std::memory_order_relaxed);
        metrics::disagg::remote_load_observe(static_cast<std::uint64_t>(ns));
        return vals;
    }

    // Write-through of a freshly loaded cold value. The CALLER must hold
    // sync_mu_ (the load itself ran with the lock released, so re-check the
    // working set under the lock before mutating it).
    void fill_hot_(OperatorId op, const std::string& key, const Value& v) const {
        // Defensive: if the key was erased while this load was in flight, honour
        // the delete rather than resurrect the pre-erase value into the hot
        // tier. (The async per-key gate makes a same-key erase-during-load
        // unreachable today, but the backend must be correct regardless.)
        if (pool_ && is_deleted_(op, std::string_view{key})) {
            return;
        }
        // A concurrent put on a sibling runner may have landed a NEWER value
        // while the lock was released for the load. That write is authoritative
        // (and is tracked dirty); do not clobber it with the older pool value.
        if (hot_.get(op, std::string_view{key})) {
            return;
        }
        hot_.put(op,
                 std::string_view{key},
                 std::string_view{reinterpret_cast<const char*>(v.data()), v.size()});
        // A filled key is CLEAN (it equals the committed value it came from),
        // so it is immediately eligible for eviction. Account for it and run
        // the budget so a read-heavy scan can't grow the hot tier without bound.
        touch_(op, std::string_view{key}, v.size());
        maybe_evict_();
    }

    // --- hot-tier eviction (bounded cache over the durable pool) ---
    //
    // These mutate the LRU bookkeeping (lru_/index_/hot_bytes_) and hot_. They
    // assume the CALLER holds sync_mu_ (every caller - put/erase/snapshot and
    // the fill_hot_ write-through - takes it first). The IO thread only runs
    // the loader and never touches these. They are mutable + the methods const
    // because fill_hot_ runs from the const get()/get_async path.

    // True if (op, key) was erased since the last checkpoint (delete pending
    // commit). Runner-thread only (deleted_ is mutated on the runner). The
    // empty-set fast path avoids building a std::string on the common cold-read
    // (e.g. an append-only SUM workload never erases).
    bool is_deleted_(OperatorId op, std::string_view key) const {
        if (deleted_.empty() && persisting_deleted_.empty()) {
            return false;
        }
        const auto k = std::make_pair(op.value(), std::string(key));
        // deleted_ = erased-since-last-checkpoint; persisting_deleted_ = erased in
        // a checkpoint whose durable commit is still in flight off-thread. Either
        // way the pool's pre-erase value must NOT be served.
        return deleted_.count(k) != 0 || persisting_deleted_.count(k) != 0;
    }

    // Decrement a persisting_* pin's ref-count; erase the entry at zero. A key
    // may be captured by several overlapping in-flight checkpoints, so it stays
    // pinned until the last one durably commits.
    static void unpin_(std::map<std::pair<std::uint64_t, std::string>, int>& pins,
                       const std::pair<std::uint64_t, std::string>& k) {
        auto it = pins.find(k);
        if (it != pins.end() && --it->second <= 0) {
            pins.erase(it);
        }
    }

    static std::string compose_(OperatorId op, std::string_view key) {
        std::string s;
        s.reserve(20 + key.size());
        s += std::to_string(op.value());
        s.push_back('\x1f');
        s.append(key.data(), key.size());
        return s;
    }

    // Record/refresh a hot key's recency (front = most recent) and byte size.
    // No-op unless eviction is active (budget set + pooled).
    void touch_(OperatorId op, std::string_view key, std::size_t value_size) const {
        if (hot_max_bytes_ == 0 || !pool_) {
            return;
        }
        const std::size_t entry_bytes = key.size() + value_size;
        const auto ck = compose_(op, key);
        if (auto it = index_.find(ck); it != index_.end()) {
            hot_bytes_ += entry_bytes;
            hot_bytes_ -= it->second->bytes;
            it->second->bytes = entry_bytes;
            lru_.splice(lru_.begin(), lru_, it->second);  // move to front (MRU)
        } else {
            lru_.push_front(HotEntry{op.value(), std::string(key), entry_bytes});
            index_.emplace(ck, lru_.begin());
            hot_bytes_ += entry_bytes;
        }
    }

    // Drop a key from the hot-tier index + byte total (on logical erase or
    // eviction). hot_ itself is updated separately by the caller.
    void forget_hot_(OperatorId op, std::string_view key) const {
        if (hot_max_bytes_ == 0 || !pool_) {
            return;
        }
        const auto ck = compose_(op, key);
        if (auto it = index_.find(ck); it != index_.end()) {
            hot_bytes_ -= it->second->bytes;
            lru_.erase(it->second);
            index_.erase(it);
        }
    }

    // Evict least-recently-used CLEAN keys until under budget. A key that is
    // dirty (written since the last checkpoint) or deleted is NEVER evicted -
    // the pool does not yet hold its latest value, so dropping it would lose
    // state; it stays hot until the next checkpoint commits it. If every
    // over-budget key is dirty the hot tier transiently exceeds the budget
    // (correct, not a leak): the next snapshot clears dirty_ and re-runs this.
    void maybe_evict_() const {
        if (hot_max_bytes_ == 0 || !pool_) {
            return;
        }
        for (auto it = lru_.end(); hot_bytes_ > hot_max_bytes_ && it != lru_.begin();) {
            --it;
            const std::pair<std::uint64_t, std::string> ck{it->op, it->key};
            if (dirty_.count(ck) != 0 || deleted_.count(ck) != 0 ||
                persisting_dirty_.count(ck) != 0 || persisting_deleted_.count(ck) != 0) {
                continue;  // not yet durable in the pool (delta or in-flight
                           // persist): must stay hot
            }
            hot_.erase(OperatorId{it->op}, std::string_view{it->key});
            hot_bytes_ -= it->bytes;
            index_.erase(compose_(OperatorId{it->op}, it->key));
            hot_evictions_.fetch_add(1, std::memory_order_relaxed);
            metrics::disagg::hot_evicted();
            it = lru_.erase(it);
        }
    }

    // Hand a completed cold read back to the runner. Prefers the deadline-aware
    // scheduler (carries order_key) when wired; otherwise the plain one (FIFO).
    // Called on the IO/executor thread, after the load, exactly like the old
    // direct resume_scheduler_(h) call.
    void post_resume_(std::coroutine_handle<> h, std::uint64_t order_key) const {
        if (deadline_scheduler_) {
            deadline_scheduler_(h, order_key);
        } else if (resume_scheduler_) {
            resume_scheduler_(h);
        }
    }

    RemoteLoader loader_;
    ResumeScheduler resume_scheduler_;
    DeadlineResumeScheduler deadline_scheduler_;  // ASYNC-12: order_key-carrying hand-back
    // Guards the mutable working set (hot_, dirty_, deleted_, and the LRU
    // bookkeeping lru_/index_/hot_bytes_). A single backend instance is SHARED
    // by every runner of a fused subtask, so the state WRITER and the checkpoint
    // OWNER can be different threads: a source writes its offset (operator
    // state) on the source-runner thread, while the downstream owner runner
    // takes the snapshot that must commit that write. Without this lock the
    // owner's snapshot races the writer's dirty_ inserts and silently drops them
    // (the delta commits empty, so the durable value freezes at the first
    // checkpoint - a correctness bug across failover). The async cold-read path
    // never holds this lock across the blocking remote load: the IO thread only
    // runs the loader (which touches pool_/last_ckpt_, never the guarded state),
    // and the write-through (fill_hot_) re-takes the lock on the runner thread.
    mutable std::mutex sync_mu_;
    // Hot tier: recent writes + filled-on-read keys. Guarded by sync_mu_ (see
    // above). mutable because cold reads fill it through on a const get.
    mutable InMemoryStateBackend hot_;

    // The off-runner-thread IO executor (ASYNC-9). Default is a per-backend
    // sized ThreadPoolCompletionExecutor; a shared or io_uring-backed executor
    // can be injected at construction. The cold-read path posts its blocking
    // load here; the executor invokes resume_scheduler_ (schedule_resume) when
    // done. shared_ptr so it can be shared across a process's backends.
    std::shared_ptr<async::CompletionExecutor> executor_;

    mutable std::atomic<std::uint64_t> remote_loads_{0};
    mutable std::atomic<std::uint64_t> hot_hits_{0};

    // Pool-backed (production) state. pool_ is null for the loader-only ctor.
    // last_ckpt_ is the checkpoint cold reads serve and the base for the next
    // commit; it is read on IO threads (the loader) and written on the runner
    // thread (snapshot/restore) after the async work has drained, so atomic.
    // dirty_/deleted_ track writes since the last checkpoint. They are written
    // by put/erase (any runner of a fused subtask) and read+cleared by snapshot
    // (the owner runner), so all access is guarded by sync_mu_.
    std::shared_ptr<RemotePool> pool_;
    // last_ckpt_ = last DURABLY-committed checkpoint (the loader cold-reads from
    // it); advanced in persist(), read lock-free by the IO-thread loader.
    // last_captured_ = last checkpoint capture() detached (the next delta's
    // base); advanced in capture(), only ever touched under sync_mu_. They differ
    // exactly during an in-flight async persist (capture done, commit pending).
    mutable std::atomic<std::uint64_t> last_ckpt_{0};
    std::uint64_t last_captured_{0};
    std::set<std::pair<std::uint64_t, std::string>> dirty_;
    std::set<std::pair<std::uint64_t, std::string>> deleted_;

    // Async-persist split (capture/persist). capture() detaches a checkpoint's
    // delta into pending_ (keyed by checkpoint id) and pins its keys in
    // persisting_* (ref-counted, so a key captured by several overlapping
    // in-flight checkpoints stays pinned until the last commits); persist()
    // commits the delta to the pool off-thread, then drops the pins. All guarded
    // by sync_mu_ except the pool_->commit itself (it touches only the detached
    // PendingCommit, so it runs lock-free off the operator thread).
    struct PendingCommit {
        CheckpointId base{};
        std::vector<RemotePoolEntry> changed;
        std::vector<RemotePoolKey> deleted;
    };
    std::map<std::uint64_t, PendingCommit> pending_;
    std::map<std::pair<std::uint64_t, std::string>, int> persisting_dirty_;
    std::map<std::pair<std::uint64_t, std::string>, int> persisting_deleted_;

    // Bounded hot tier (eviction). hot_max_bytes_ == 0 => unbounded (no
    // eviction, no bookkeeping). lru_ front = most-recently-used; index_ maps
    // compose_(op,key) -> its node for O(1) refresh/erase. Guarded by sync_mu_;
    // mutable because the cold-read write-through is on a const get.
    struct HotEntry {
        std::uint64_t op;
        std::string key;
        std::size_t bytes;  // key.size() + value.size() at last touch
    };
    std::size_t hot_max_bytes_{0};
    mutable std::size_t hot_bytes_{0};
    mutable std::list<HotEntry> lru_;
    mutable std::unordered_map<std::string, std::list<HotEntry>::iterator> index_;
    mutable std::atomic<std::uint64_t> hot_evictions_{0};
};

}  // namespace clink
