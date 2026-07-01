#include "clink/cluster/refresh_scheduler.hpp"

#include <algorithm>
#include <utility>
#include <vector>

#include "clink/runtime/log_buffer.hpp"

namespace clink::cluster {

RefreshScheduler::RefreshScheduler(RefreshSchedulerConfig cfg) : cfg_(cfg) {}

RefreshScheduler::~RefreshScheduler() {
    stop();
}

void RefreshScheduler::register_view(std::string name,
                                     std::chrono::milliseconds interval,
                                     RefreshFn fn) {
    std::lock_guard<std::mutex> lk(mu_);
    Entry e;
    e.interval = interval;
    e.fn = std::move(fn);
    e.next_due = std::chrono::steady_clock::now() + interval;
    views_[std::move(name)] = std::move(e);
}

void RefreshScheduler::unregister_view(const std::string& name) {
    std::lock_guard<std::mutex> lk(mu_);
    views_.erase(name);
}

bool RefreshScheduler::has_view(const std::string& name) const {
    std::lock_guard<std::mutex> lk(mu_);
    return views_.find(name) != views_.end();
}

std::size_t RefreshScheduler::size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return views_.size();
}

std::size_t RefreshScheduler::tick() {
    // Snapshot the due views + their callbacks under the lock, then run the callbacks
    // WITHOUT the lock (a refresh submits + awaits a job, which is slow and must not
    // block register/unregister or the interactive SQL path that shares the callback's
    // catalog). Re-arm each view `interval` after its callback returns, so a slow
    // refresh runs back-to-back rather than piling up.
    std::vector<std::pair<std::string, RefreshFn>> due;
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto& [name, e] : views_) {
            if (now >= e.next_due) {
                due.emplace_back(name, e.fn);
            }
        }
    }
    for (auto& [name, fn] : due) {
        try {
            fn();
        } catch (const std::exception& e) {
            log::warn("refresh_scheduler",
                      "refresh of '" + name + "' failed (previous snapshot kept): " + e.what());
        } catch (...) {
            log::warn("refresh_scheduler", "refresh of '" + name + "' threw non-exception");
        }
        refreshes_.fetch_add(1, std::memory_order_relaxed);
        // Re-arm from completion time so the interval spaces successive refreshes.
        std::lock_guard<std::mutex> lk(mu_);
        auto it = views_.find(name);
        if (it != views_.end()) {
            it->second.next_due = std::chrono::steady_clock::now() + it->second.interval;
        }
    }
    return due.size();
}

void RefreshScheduler::start() {
    const bool was_running = running_.exchange(true, std::memory_order_acq_rel);
    if (was_running) {
        return;
    }
    stop_.store(false, std::memory_order_release);
    thread_ = std::thread(&RefreshScheduler::run_, this);
}

void RefreshScheduler::stop() {
    stop_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
    running_.store(false, std::memory_order_release);
}

void RefreshScheduler::run_() {
    while (!stop_.load(std::memory_order_acquire)) {
        // Sleep in short chunks so stop() responds promptly even for a long tick period.
        auto remaining = cfg_.tick_period;
        while (remaining.count() > 0 && !stop_.load(std::memory_order_acquire)) {
            const auto step =
                std::min<std::chrono::milliseconds>(remaining, std::chrono::milliseconds{50});
            std::this_thread::sleep_for(step);
            remaining -= step;
        }
        if (stop_.load(std::memory_order_acquire)) {
            break;
        }
        try {
            (void)tick();
        } catch (const std::exception& e) {
            log::warn("refresh_scheduler", std::string{"tick threw: "} + e.what());
        } catch (...) {
            log::warn("refresh_scheduler", "tick threw non-exception");
        }
    }
}

}  // namespace clink::cluster
