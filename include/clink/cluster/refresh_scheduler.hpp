#pragma once

// JM-side refresh scheduler for full-refresh materialized views.
//
// A full-refresh materialized view (CREATE MATERIALIZED VIEW ... FRESHNESS='<interval>')
// recomputes its whole result on a cadence. This scheduler owns that cadence: for each
// registered view it fires a refresh callback every `interval`. The callback (wired by
// the JM host) recompiles the defining query and submits + awaits a bounded job whose
// overwrite sink atomically republishes the backing - so the scheduler itself is
// decoupled from SQL / the JobManager (it only knows names, intervals, and an opaque
// RefreshFn), which keeps it unit-testable without a cluster.
//
// v1 runs refreshes sequentially on the scheduler's own thread: a tick fires every due
// view's callback in turn and only re-arms that view `interval` after the callback
// returns. So a slow refresh cannot overlap itself or pile up (it simply runs
// back-to-back), and no per-view "in flight" bookkeeping is needed. A callback that
// throws is caught and logged; the loop keeps running.
//
// Lifecycle mirrors the autoscaler: start() spins the loop thread, stop() (and the
// dtor) signal it and join. tick() is exposed for deterministic single-step tests.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace clink::cluster {

struct RefreshSchedulerConfig {
    // How often the loop wakes to check for due views. Should be <= the smallest
    // freshness interval in use so short cadences are honoured promptly.
    std::chrono::milliseconds tick_period{std::chrono::milliseconds{500}};
};

class RefreshScheduler {
public:
    // Recompute + submit + await for one view. Throwing is caught + logged by the
    // scheduler (a failed refresh leaves the previous snapshot intact and is retried
    // on the next tick).
    using RefreshFn = std::function<void()>;

    explicit RefreshScheduler(RefreshSchedulerConfig cfg = {});
    ~RefreshScheduler();

    RefreshScheduler(const RefreshScheduler&) = delete;
    RefreshScheduler& operator=(const RefreshScheduler&) = delete;

    // Register (or replace) a view's schedule. The first refresh is armed `interval`
    // from now (the caller runs the initial population separately on CREATE).
    void register_view(std::string name, std::chrono::milliseconds interval, RefreshFn fn);

    // Drop a view's schedule. Idempotent.
    void unregister_view(const std::string& name);

    [[nodiscard]] bool has_view(const std::string& name) const;
    [[nodiscard]] std::size_t size() const;

    // Fire every currently-due view's refresh once, synchronously (sequentially),
    // re-arming each `interval` after its callback returns. Returns the number fired.
    // Public for deterministic tests; the loop thread calls it on each tick.
    std::size_t tick();

    void start();
    void stop();

    // Total refresh callbacks invoked (including failed ones). Test diagnostic.
    [[nodiscard]] std::uint64_t refreshes() const noexcept {
        return refreshes_.load(std::memory_order_relaxed);
    }

private:
    void run_();

    struct Entry {
        std::chrono::milliseconds interval{};
        RefreshFn fn;
        std::chrono::steady_clock::time_point next_due{};
    };

    RefreshSchedulerConfig cfg_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry> views_;

    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> refreshes_{0};
    std::thread thread_;
};

}  // namespace clink::cluster
