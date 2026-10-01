#include "native/retry.hpp"

#include <algorithm>
#include <limits>
#include <thread>

namespace clink::clickhouse::native {

namespace {

using Clock = std::chrono::steady_clock;

// The longest a cancel or a stop waits to be seen.
constexpr std::chrono::milliseconds kSlice{50};

// `t + d` for a non-negative `d`, held at the clock's maximum rather than
// overflowing: the option bounds keep real windows far below it, but a window
// built directly need not be.
Clock::time_point saturating_add(Clock::time_point t, std::chrono::milliseconds d) {
    if (d <= std::chrono::milliseconds::zero()) {
        return t;
    }
    const auto since_epoch = t.time_since_epoch();
    const auto headroom = since_epoch < Clock::duration::zero()
                              ? Clock::duration::max()
                              : Clock::duration::max() - since_epoch;
    if (d >= std::chrono::duration_cast<std::chrono::milliseconds>(headroom)) {
        return Clock::time_point::max();
    }
    return t + std::chrono::duration_cast<Clock::duration>(d);
}

}  // namespace

Backoff::Backoff(std::chrono::milliseconds initial,
                 std::chrono::milliseconds cap,
                 std::uint64_t seed)
    : initial_(initial), cap_(cap), rng_(seed) {}

std::chrono::milliseconds Backoff::next() {
    const std::int64_t initial = std::max<std::int64_t>(initial_.count(), 0);
    const std::int64_t cap = std::max<std::int64_t>(cap_.count(), 0);
    // initial * 2^n, unless that passes the cap. Comparing against the cap
    // shifted down keeps the doubling from ever overflowing; from 63 doublings
    // on, any positive initial delay is past every cap.
    std::int64_t ceiling = cap;
    if (initial == 0) {
        ceiling = 0;
    } else if (attempts_ < 63 && initial <= (cap >> attempts_)) {
        ceiling = initial << attempts_;
    }
    if (attempts_ < std::numeric_limits<std::uint32_t>::max()) {
        ++attempts_;
    }
    std::uniform_int_distribution<std::int64_t> wait(0, ceiling);
    return std::chrono::milliseconds{wait(rng_)};
}

void Backoff::reset() {
    attempts_ = 0;
}

RetryWindow::RetryWindow(std::chrono::milliseconds window) : window_(window) {}

void RetryWindow::start(Clock::time_point now) {
    if (started_) {
        return;
    }
    deadline_ = saturating_add(now, window_);
    started_ = true;
}

bool RetryWindow::allows(Clock::time_point now, std::chrono::milliseconds wait) const {
    const auto deadline = started_ ? deadline_ : saturating_add(now, window_);
    return saturating_add(now, std::max(wait, std::chrono::milliseconds::zero())) < deadline;
}

bool cancellable_wait(std::chrono::milliseconds d,
                      const CancelSignal& cancel,
                      const std::atomic<bool>& stop) {
    const auto interrupted = [&] {
        return cancel.requested() || stop.load(std::memory_order_acquire);
    };
    const auto end = saturating_add(Clock::now(), d);
    for (;;) {
        if (interrupted()) {
            return false;
        }
        const auto now = Clock::now();
        if (now >= end) {
            return true;
        }
        std::this_thread::sleep_for(std::min<Clock::duration>(end - now, kSlice));
    }
}

}  // namespace clink::clickhouse::native
