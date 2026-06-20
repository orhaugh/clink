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
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
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
                               std::size_t io_threads = 1,
                               std::size_t hot_max_bytes = 0)
        : pool_(std::move(pool)), hot_max_bytes_(hot_max_bytes) {
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

    Snapshot snapshot(CheckpointId id) override {
        std::lock_guard<std::mutex> lk(sync_mu_);
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
        // Everything is now durable at `id`, so the keys that were pinned hot
        // as dirty are eligible for eviction - run the budget down to size and
        // publish the resident footprint.
        maybe_evict_();
        metrics::disagg::hot_resident_bytes_set(static_cast<std::int64_t>(hot_bytes_));
        Snapshot s;
        s.checkpoint_id = id;  // the authoritative bytes are in the pool
        return s;
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
        co_return co_await RemoteLoad{this, op, std::move(owned)};
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
                // Write-through on the runner thread, under the backend lock so
                // it cannot race a concurrent put/snapshot on a sibling runner.
                std::lock_guard<std::mutex> lk(self->sync_mu_);
                self->fill_hot_(op, key, *value);
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
        return !deleted_.empty() &&
               deleted_.count(std::make_pair(op.value(), std::string(key))) != 0;
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
            if (dirty_.count(ck) != 0 || deleted_.count(ck) != 0) {
                continue;  // not yet durable in the pool: must stay hot
            }
            hot_.erase(OperatorId{it->op}, std::string_view{it->key});
            hot_bytes_ -= it->bytes;
            index_.erase(compose_(OperatorId{it->op}, it->key));
            hot_evictions_.fetch_add(1, std::memory_order_relaxed);
            metrics::disagg::hot_evicted();
            it = lru_.erase(it);
        }
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
    // dirty_/deleted_ track writes since the last checkpoint. They are written
    // by put/erase (any runner of a fused subtask) and read+cleared by snapshot
    // (the owner runner), so all access is guarded by sync_mu_.
    std::shared_ptr<RemotePool> pool_;
    mutable std::atomic<std::uint64_t> last_ckpt_{0};
    std::set<std::pair<std::uint64_t, std::string>> dirty_;
    std::set<std::pair<std::uint64_t, std::string>> deleted_;

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
