#pragma once

#include <atomic>
#include <cstdint>

namespace clink {

// Monotonic non-negative integer counter.
class Counter {
public:
    Counter() = default;

    void increment(std::uint64_t amount = 1) noexcept {
        value_.fetch_add(amount, std::memory_order_relaxed);
    }

    std::uint64_t value() const noexcept { return value_.load(std::memory_order_relaxed); }

private:
    std::atomic<std::uint64_t> value_{0};
};

}  // namespace clink
