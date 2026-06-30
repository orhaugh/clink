#pragma once

// AsyncExecutionController (ASYNC-3 + ASYNC-4)
//
// The per-subtask controller that lets a keyed operator issue non-blocking
// state reads without stalling its runner thread, while preserving per-key
// ordering AND watermark/event-time completeness. It is the substrate
// disaggregated/remote state rides on: a slow (remote) read suspends a
// record's coroutine instead of blocking, and the runner keeps making
// progress on other keys.
//
// Threading model (the load-bearing design decision):
//   - Coroutines are ONLY ever resumed on the runner thread (submit() and
//     poll()). The per-key gate, the in-flight table, the parked-waiter
//     FIFOs, and the epoch bookkeeping are therefore runner-thread-private
//     and need no locks.
//   - The ONLY cross-thread surface is schedule_resume(handle): an IO
//     completion landing on a foreign thread (an io_uring reactor, a
//     thread-pool worker) hands the suspended handle back through a
//     mutex-guarded ready-queue and wakes the runner. The foreign thread
//     never resumes the coroutine and never touches the controller state.
//     This is why a record's post-state stage (stage3) runs on the runner
//     thread, not the completion thread.
//
// Per-key gate (Key Accounting Unit): at most one in-flight computation per
// key. A second record for a busy key parks FIFO and is promoted when the
// in-flight one for that key completes, so same-key reads observe prior
// same-key writes regardless of completion timing. Distinct keys overlap.
//
// Epoch manager (ASYNC-4): records are tagged with the epoch they arrive in.
// on_watermark() closes the current epoch and opens a new one; the
// watermark's release action (for an operator: forward the watermark
// downstream and fire its due event-time timers) runs only once that epoch
// has FINISHED (every record that arrived before the watermark has completed
// its post-state stage) and every earlier epoch has already released.
// Releases run in epoch order on the runner thread, so an async-state
// operator never forwards a watermark - or fires a timer - while a record
// that arrived before it is still pending.
//
// Scope so far: the controller core, the thread-safe ready-queue, the
// per-key gate, the epoch manager, and a run-until-empty drain(). The
// checkpoint-barrier drain is ASYNC-5; operator opt-in + runner wiring is
// ASYNC-6; hosting in the sharded stage is ASYNC-7. The completion SOURCE
// (io_uring vs thread-pool) is swapped in later increments; this controller
// is agnostic to it.

#include <algorithm>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/async/task.hpp"

namespace clink {

class AsyncExecutionController {
public:
    // The per-key gate key. On the keyed-state path this is the
    // kg-byte-prefixed encoded key (keyed_state.hpp encode_key), so two
    // records that touch the same logical key serialise.
    using Key = std::string;
    // Builds a record's processing coroutine. The body does the record's
    // async work (state reads via co_await, then the post-state mutation).
    // To suspend for IO it co_awaits an awaitable whose completion (on any
    // thread) calls schedule_resume() with the suspended handle.
    using CoroFactory = std::function<async::Task<void>()>;

    // ASYNC-12: how poll() orders a batch of ready completions before resuming.
    //   Fifo (default): arrival order - byte-identical to the original AEC.
    //   Priority: ascending schedule_resume order_key (lower = sooner; e.g. a
    //     deadline in ms), ties broken by arrival. Set once at wire-up on the
    //     runner thread, before any submit/poll.
    enum class ResumeOrder { Fifo, Priority };
    void set_resume_order(ResumeOrder o) noexcept { resume_order_ = o; }
    [[nodiscard]] ResumeOrder resume_order() const noexcept { return resume_order_; }

    // ASYNC-10 consumer hook: a read coalescer (CoalescingBackend) registers
    // its flush() here. When the controller would otherwise block with records
    // in flight but nothing ready, it calls the hook FIRST, giving a pending
    // read batch the chance to issue ONE coalesced get_many_async before the
    // runner waits. The hook returns true if it had work to issue. Default
    // unset -> drain blocks exactly as before. Set on the runner thread at
    // wire-up. Runner-thread-only (not the cross-thread surface).
    void set_flush_hook(std::function<bool()> flush) { flush_hook_ = std::move(flush); }

    static constexpr std::size_t kDefaultMaxInFlight = 6000;

    explicit AsyncExecutionController(std::size_t max_in_flight = kDefaultMaxInFlight)
        : max_in_flight_(max_in_flight == 0 ? 1 : max_in_flight) {}

    AsyncExecutionController(const AsyncExecutionController&) = delete;
    AsyncExecutionController& operator=(const AsyncExecutionController&) = delete;

    // Runner thread. Submit a record under the per-key gate, tagged with the
    // current epoch. stage1 (the non-state transform) is assumed already run
    // by the caller. If the key is free the record's coroutine is kicked
    // immediately (and finishes here if it completes synchronously); if the
    // key is busy the record parks FIFO. Returns false WITHOUT enqueuing when
    // at the in-flight cap (backpressure) - the caller should poll()/drain()
    // and retry. Completions are independent of submits, so the cap cannot
    // deadlock an epoch: in-flight records still finish (freeing capacity and
    // draining their epoch) while submit is refused.
    bool submit(Key key, CoroFactory make_coro) {
        if (tracked_() >= max_in_flight_) {
            return false;
        }
        const EpochId epoch = current_epoch_;
        ++outstanding_[epoch];  // the watermark that closes this epoch waits for it
        if (key_held_.contains(key)) {
            parked_[key].push_back(ParkedRecord{epoch, std::move(make_coro)});
            ++parked_count_;
            return true;
        }
        start_record_(std::move(key), std::move(make_coro), epoch);
        process_completed_();
        return true;
    }

    // Runner thread. Mark a watermark/epoch boundary: the current epoch is
    // closed (no further records join it) and a new epoch opens. `on_release`
    // is the action to run once this epoch FINISHES (all records that arrived
    // before the watermark have completed their post-state stage) and every
    // earlier epoch has already released - for an operator: forward the
    // watermark downstream and fire its due event-time timers. Releases run
    // in epoch order on the runner thread, so a timer that touches keyed
    // state sees no in-flight record for its epoch.
    void on_watermark(std::function<void()> on_release) {
        wm_queue_.push_back(PendingWatermark{current_epoch_, std::move(on_release)});
        ++current_epoch_;
        try_release_();  // an empty epoch (no records) releases immediately
    }

    // Any thread. Hand a suspended coroutine back to the runner thread for
    // resumption and wake a runner blocked in drain(). The handle is the
    // deepest suspended coroutine (the one the IO awaitable captured); its
    // resumption propagates completion up through the continuation chain.
    void schedule_resume(std::coroutine_handle<> h) { schedule_resume(h, 0); }

    // Priority-aware variant (ASYNC-12). order_key tags this completion's resume
    // position: under ResumeOrder::Priority the runner resumes a poll's ready
    // batch in ascending order_key (lower = sooner), ties broken by arrival
    // (stable); under the default Fifo it is ignored. Reordering is always safe:
    // the per-key gate guarantees every ready completion is for a DISTINCT key,
    // and epoch/watermark releases are count-based and happen in
    // process_completed_ AFTER the resume loop, so resume order cannot change
    // which records belong to an epoch or when a watermark releases.
    void schedule_resume(std::coroutine_handle<> h, std::uint64_t order_key) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            ready_.push_back(ReadyEntry{h, order_key});
        }
        cv_.notify_one();
    }

    // Runner thread. Resume every handle posted since the last poll, then do
    // completion bookkeeping (release keys, promote FIFO waiters, settle
    // epochs, release any now-finished head watermarks). Returns the number
    // of records that completed.
    std::size_t poll() {
        std::vector<ReadyEntry> ready;
        {
            std::lock_guard<std::mutex> lk(mu_);
            ready.swap(ready_);
        }
        if (resume_order_ == ResumeOrder::Priority && ready.size() > 1) {
            // Reorder only THIS batch of already-ready, distinct-key
            // completions (see schedule_resume's safety note). Stable so equal
            // order_keys keep arrival order (FIFO within a priority class).
            std::stable_sort(
                ready.begin(), ready.end(), [](const ReadyEntry& a, const ReadyEntry& b) {
                    return a.order_key < b.order_key;
                });
        }
        for (const auto& e : ready) {
            if (e.handle && !e.handle.done()) {
                e.handle
                    .resume();  // runner-thread resume: stage3 runs here, not the completion thread
            }
        }
        const std::size_t finished = process_completed_();
        rethrow_if_error_();  // surface a faulted record on the runner thread
        return finished;
    }

    // Runner thread. poll() completions and, if none were ready, give a read
    // coalescer the chance to issue its parked batch (the same flush drain()
    // does when stuck). Use this - not bare poll() - in a submit-retry spin at
    // the in-flight cap: with coalesce_reads() on, every submitted record's
    // first step is a get_async the coalescer PARKS until flush(), so poll()
    // alone never issues those reads, never completes them, and never frees
    // capacity - the spin livelocks the runner thread. Flushing here issues the
    // parked batch so completions can land and capacity can free. A no-op when
    // not coalescing (flush_hook_ unset), so the non-coalescing path is
    // byte-identical to poll().
    std::size_t poll_or_flush() {
        const std::size_t finished = poll();
        if (finished == 0 && flush_hook_) {
            flush_hook_();
        }
        return finished;
    }

    // Runner thread. Block until all in-flight and parked work is done,
    // servicing completions as they arrive.
    void drain() {
        for (;;) {
            poll();
            if (records_.empty() && parked_count_ == 0) {
                return;
            }
            // Stuck (work in flight, nothing ready): let a read coalescer flush
            // its pending batch before we block. If it issued work the next
            // poll() settles a synchronous backend, or the cv wakes on an
            // asynchronous completion; a no-op flush (nothing pending) falls
            // through to the wait, so a record genuinely blocked on real IO is
            // unaffected.
            if (flush_hook_ && flush_hook_()) {
                continue;
            }
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait(lk, [&] { return !ready_.empty(); });
        }
    }

    // Runner thread (ASYNC-5). The checkpoint-barrier drain: drain all
    // in-flight + parked records to quiescence so that every record admitted
    // before the barrier has applied its post-state write, BEFORE the caller
    // runs capture()/snapshot(). This is what makes the captured cut
    // consistent: no record is half-applied at the barrier (no torn state),
    // none is double-counted, none is lost. After this returns
    // in_flight()==0 && parked()==0. The caller must not admit input past the
    // barrier until capture() completes (the runner stops popping). The
    // capture()->persist()->SnapshotWorker split and ack-after-durable are
    // untouched: only WHEN capture() runs moves to post-drain. The same drain
    // serves the flush/EOS path. (Aligned barriers only; the unaligned-mode
    // decision is made at the operator opt-in in ASYNC-6.)
    void drain_for_barrier() { drain(); }

    // Records currently executing (one per held key) - the concurrent
    // distinct-key in-flight count.
    [[nodiscard]] std::size_t in_flight() const noexcept { return records_.size(); }
    // Records waiting behind a busy key.
    [[nodiscard]] std::size_t parked() const noexcept { return parked_count_; }
    // High-water mark of concurrent in-flight records.
    [[nodiscard]] std::size_t max_in_flight_observed() const noexcept { return max_observed_; }
    [[nodiscard]] std::size_t max_in_flight() const noexcept { return max_in_flight_; }
    // The epoch new records are tagged with (advances on each watermark).
    [[nodiscard]] std::uint64_t current_epoch() const noexcept { return current_epoch_; }
    // Watermarks closed but not yet released (their epoch still draining).
    [[nodiscard]] std::size_t pending_watermarks() const noexcept { return wm_queue_.size(); }

private:
    using RecordId = std::uint64_t;
    using EpochId = std::uint64_t;

    struct Record {
        Key key;
        EpochId epoch;
        async::Task<void> task;
    };
    struct ParkedRecord {
        EpochId epoch;
        CoroFactory make_coro;
    };
    struct PendingWatermark {
        EpochId epoch;
        std::function<void()> release;
    };

    [[nodiscard]] std::size_t tracked_() const noexcept { return records_.size() + parked_count_; }

    void start_record_(Key key, CoroFactory make_coro, EpochId epoch) {
        const RecordId id = next_id_++;
        key_held_.insert(key);
        auto [it, inserted] =
            records_.emplace(id, Record{std::move(key), epoch, wrap_(std::move(make_coro), id)});
        (void)inserted;
        if (records_.size() > max_observed_) {
            max_observed_ = records_.size();
        }
        it->second.task.resume();  // kick past the lazy initial_suspend
    }

    // Wraps a record's body so that completion is self-reported. The
    // push_back runs on the runner thread (the final resume always lands
    // on the runner via poll()/submit()), so completed_ stays lock-free.
    //
    // A throwing record body must NOT leak the record: without the catch,
    // the exception is captured onto this wrapper's promise and
    // completed_.push_back is skipped, so the record stays in records_ with
    // its key held and its epoch outstanding forever, and the next drain()
    // spins (cv_.wait with nothing to notify). Instead, record the first
    // error and ALWAYS account the record so finish_record_ releases the key
    // and advances the epoch; poll()/drain() then rethrow the stored error on
    // the runner thread so it follows the host's normal operator-failure path
    // (the runner/executor records it and tears the subtask down) - matching
    // how the synchronous process() path propagates a throw.
    async::Task<void> wrap_(CoroFactory make_coro, RecordId id) {
        try {
            co_await make_coro();
        } catch (...) {
            if (!first_error_) {
                first_error_ = std::current_exception();
            }
        }
        completed_.push_back(id);
    }

    void rethrow_if_error_() {
        if (first_error_) {
            std::rethrow_exception(std::exchange(first_error_, nullptr));
        }
    }

    // Drain the completed list, finishing each record. A promotion may kick
    // a waiter that completes synchronously and appends to completed_, so
    // loop until it stays empty; then release any head watermarks whose
    // epoch has now finished.
    std::size_t process_completed_() {
        std::size_t finished = 0;
        while (!completed_.empty()) {
            std::vector<RecordId> batch;
            batch.swap(completed_);
            for (RecordId id : batch) {
                finish_record_(id);
                ++finished;
            }
        }
        try_release_();
        return finished;
    }

    void finish_record_(RecordId id) {
        auto it = records_.find(id);
        if (it == records_.end()) {
            return;
        }
        Key key = it->second.key;
        const EpochId epoch = it->second.epoch;
        records_.erase(it);  // destroys the wrapper Task (parked at final_suspend, safe)
        key_held_.erase(key);
        // Promote the next FIFO waiter for this key, if any (its epoch was
        // already counted at submit time, so it stays accounted across the
        // park->run transition).
        auto pit = parked_.find(key);
        if (pit != parked_.end() && !pit->second.empty()) {
            ParkedRecord next = std::move(pit->second.front());
            pit->second.pop_front();
            --parked_count_;
            if (pit->second.empty()) {
                parked_.erase(pit);
            }
            start_record_(std::move(key), std::move(next.make_coro), next.epoch);
        }
        // This record's epoch has one fewer outstanding; when it hits zero
        // the epoch is finished and its watermark (if at the head) releases.
        if (auto oit = outstanding_.find(epoch); oit != outstanding_.end()) {
            if (--oit->second == 0) {
                outstanding_.erase(oit);
            }
        }
    }

    // Release watermarks from the head of the queue while the head epoch has
    // finished (no outstanding records). An epoch with no entry in
    // outstanding_ has zero live records (entries are erased at zero), so
    // absence == finished. Strict FIFO: a later watermark never overtakes an
    // earlier one even if its epoch drains first.
    void try_release_() {
        while (!wm_queue_.empty()) {
            const EpochId head = wm_queue_.front().epoch;
            if (outstanding_.find(head) != outstanding_.end()) {
                break;  // head epoch still has in-flight records; wait
            }
            auto release = std::move(wm_queue_.front().release);
            wm_queue_.pop_front();
            if (release) {
                release();  // runner thread: forward watermark + fire due timers
            }
        }
    }

    std::size_t max_in_flight_;
    RecordId next_id_{0};
    EpochId current_epoch_{0};

    // --- Runner-thread-private state (no locks) -------------------------
    std::unordered_map<RecordId, Record> records_;              // in-flight (key held)
    std::unordered_set<Key> key_held_;                          // the per-key gate
    std::unordered_map<Key, std::deque<ParkedRecord>> parked_;  // FIFO waiters per busy key
    std::size_t parked_count_{0};
    std::vector<RecordId> completed_;  // appended by wrap_ on the runner thread
    std::size_t max_observed_{0};
    std::exception_ptr first_error_;  // first faulted-record exception, rethrown by poll()/drain()
    std::unordered_map<EpochId, std::size_t> outstanding_;  // live records per epoch
    std::deque<PendingWatermark> wm_queue_;                 // closed epochs awaiting release

    // --- Cross-thread boundary (the only shared state) ------------------
    struct ReadyEntry {
        std::coroutine_handle<> handle;
        std::uint64_t order_key;  // resume priority under ResumeOrder::Priority
    };
    std::mutex mu_;
    std::condition_variable cv_;
    std::vector<ReadyEntry> ready_;
    ResumeOrder resume_order_{ResumeOrder::Fifo};  // runner-thread config, set once at wire-up
    std::function<bool()> flush_hook_;             // ASYNC-10 coalescer flush (runner-thread)
};

}  // namespace clink
