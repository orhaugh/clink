#pragma once

#include <atomic>
#include <cstddef>
#include <exception>
#include <functional>
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
        if (thread_.joinable()) {
            thread_.join();
        }
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
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    // Hard cancel: drop queued captures WITHOUT acking, then join. A
    // capture already mid-persist still completes + acks (it is durable);
    // only not-yet-started ones are dropped. Safe because an un-ack'd
    // checkpoint is simply never marked complete by the coordinator.
    void cancel_and_join() {
        drop_pending_.store(true, std::memory_order_release);
        queue_.close();
        if (thread_.joinable()) {
            thread_.join();
        }
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
            try {
                job->backend->persist(std::move(job->handle));
            } catch (const std::exception& e) {
                ok = false;
                err = e.what();
            }
            // Ack strictly AFTER persist returns: this is the only place an
            // async checkpoint is reported durable.
            if (job->ack) {
                job->ack(ckpt_id, ok, std::move(err));
            }
        }
    }

    BoundedChannel<Job> queue_;
    std::atomic<bool> drop_pending_{false};
    std::thread thread_;
};

}  // namespace clink
