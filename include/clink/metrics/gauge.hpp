#pragma once

#include <atomic>
#include <cstdint>

namespace clink {

// Bidirectional 64-bit signed gauge for things like queue depth and lag.
class Gauge {
public:
    Gauge() = default;

    void set(std::int64_t v) noexcept { value_.store(v, std::memory_order_relaxed); }
    void add(std::int64_t delta) noexcept { value_.fetch_add(delta, std::memory_order_relaxed); }
    void sub(std::int64_t delta) noexcept { value_.fetch_sub(delta, std::memory_order_relaxed); }

    std::int64_t value() const noexcept { return value_.load(std::memory_order_relaxed); }

private:
    std::atomic<std::int64_t> value_{0};
};

}  // namespace clink
