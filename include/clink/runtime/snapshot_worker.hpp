#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <exception>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "clink/core/types.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

// SnapshotWorker moves the durable-write phase of a checkpoint off the
// operator thread. The operator captures a detached point-in-time blob
// (StateBackend::capture, cheap) and hands it here; this worker calls
// StateBackend::persist (the slow durable write) on its own thread and
// only then fires the checkpoint ack. That preserves the engine's
// ack-after-durable invariant while letting record processing run ahead
// of disk I/O.
//
// One worker per operator subtask. The queue is FIFO and single-consumer,
// so checkpoints persist + ack in barrier order. Capacity is 1: an
// operator may have at most one checkpoint captured-but-not-persisted
// queued behind the one being written, which bounds how far processing
// runs ahead of durability and gives backpressure for free (enqueue
// blocks once the worker falls a checkpoint behind).
class SnapshotWorker {
public:
    struct Job {
        using ack_fn_t = std::function<void(CheckpointId, bool /*ok*/, std::string /*error*/)>;
        CaptureHandle handle;
        StateBackend* backend{nullptr};
        ack_fn_t ack;
    };

    explicit SnapshotWorker(std::size_t capacity = 1) : queue_(capacity, "snapshot-worker") {}

    SnapshotWorker(const SnapshotWorker&) = delete;
    SnapshotWorker& operator=(const SnapshotWorker&) = delete;
    SnapshotWorker(SnapshotWorker&&) = delete;
    SnapshotWorker& operator=(SnapshotWorker&&) = delete;

    // Safety net for an abnormal teardown (e.g. the operator threw): drop
    // any captures not yet persisted WITHOUT acking, then join. The normal
    // paths call drain_and_join / cancel_and_join explicitly first, which
    // leaves the thread already joined so this destructor is a no-op.
    ~SnapshotWorker() {
        drop_pending_.store(true, std::memory_order_release);
        queue_.close();
        join_with_heartbeat_("destructor");
    }

    void start() {
        thread_ = std::thread([this] { loop_(); });
    }

    // Operator thread: enqueue a captured checkpoint. Blocks if the worker
    // is still persisting the previous one (backpressure). Returns false if
    // the worker has already been closed.
    bool enqueue(Job job) { return queue_.push(std::move(job)); }

    // Clean drain: persist + ack everything still queued, then join. Used
    // on a normal end-of-stream so an in-flight checkpoint the coordinator
    // is waiting on still completes.
    void drain_and_join() {
        queue_.close();
        join_with_heartbeat_("drain_and_join");
    }

    // Hard cancel: drop queued captures WITHOUT acking, then join. A
    // capture already mid-persist still completes + acks (it is durable);
    // only not-yet-started ones are dropped. Safe because an un-ack'd
    // checkpoint is simply never marked complete by the coordinator.
    void cancel_and_join() {
        drop_pending_.store(true, std::memory_order_release);
        queue_.close();
        join_with_heartbeat_("cancel_and_join");
    }

private:
    void loop_() {
        // pop() returns nullopt only once the queue is closed AND drained,
        // so a clean drain_and_join persists + acks the whole backlog
        // before this loop exits.
        while (auto job = queue_.pop()) {
            if (drop_pending_.load(std::memory_order_acquire)) {
                continue;  // hard cancel: skip without acking
            }
            const auto ckpt_id = job->handle.checkpoint_id;
            std::string err;
            bool ok = true;
            persisting_ckpt_.store(ckpt_id.value(), std::memory_order_release);
            const auto persist_start = std::chrono::steady_clock::now();
            try {
                job->backend->persist(std::move(job->handle));
            } catch (const std::exception& e) {
                ok = false;
                err = e.what();
            }
            const auto persist_s = std::chrono::duration_cast<std::chrono::seconds>(
                                       std::chrono::steady_clock::now() - persist_start)
                                       .count();
            persisting_ckpt_.store(0, std::memory_order_release);
            // A persist that took several seconds is a capacity event a
            // teardown deadline can land inside of: it holds up every
            // join below AND the queue-cap-1 backpressure above. Loud on
            // stderr, same rationale as BOUNDED_CHANNEL_STUCK.
            if (persist_s >= 5) {
                std::fprintf(stderr,
                             "SNAPSHOT_PERSIST_SLOW ckpt=%llu took=%llds ok=%d\n",
                             static_cast<unsigned long long>(ckpt_id.value()),
                             static_cast<long long>(persist_s),
                             ok ? 1 : 0);
            }
            // Ack strictly AFTER persist returns: this is the only place an
            // async checkpoint is reported durable.
            if (job->ack) {
                job->ack(ckpt_id, ok, std::move(err));
            }
        }
        {
            std::lock_guard<std::mutex> lock(done_mu_);
            loop_done_ = true;
        }
        done_cv_.notify_all();
    }

    // Join, printing a heartbeat every 3s while the worker is still mid
    // persist. A runner's cancel teardown joins this thread, and a persist
    // ground down by disk contention silently eats the whole restart-drain
    // deadline - this line is what tells that story apart from a genuine
    // lost-wakeup wedge in the post-mortem.
    void join_with_heartbeat_(const char* who) {
        if (!thread_.joinable()) {
            return;
        }
        using clock = std::chrono::steady_clock;
        const auto start = clock::now();
        std::unique_lock<std::mutex> lock(done_mu_);
        while (!loop_done_) {
            if (done_cv_.wait_for(lock, std::chrono::seconds{3}) == std::cv_status::timeout &&
                !loop_done_) {
                const auto held =
                    std::chrono::duration_cast<std::chrono::seconds>(clock::now() - start).count();
                std::fprintf(stderr,
                             "SNAPSHOT_WORKER_JOIN_STUCK via=%s held=%llds mid_persist_ckpt=%llu\n",
                             who,
                             static_cast<long long>(held),
                             static_cast<unsigned long long>(
                                 persisting_ckpt_.load(std::memory_order_acquire)));
            }
        }
        lock.unlock();
        thread_.join();
    }

    BoundedChannel<Job> queue_;
    std::atomic<bool> drop_pending_{false};
    // Checkpoint id currently inside backend->persist(), 0 when idle.
    // Read by the join heartbeat to name what the join is waiting on.
    std::atomic<std::uint64_t> persisting_ckpt_{0};
    bool loop_done_{false};
    std::mutex done_mu_;
    std::condition_variable done_cv_;
    std::thread thread_;
};

}  // namespace clink
