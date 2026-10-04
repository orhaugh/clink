#pragma once

// An Arrow memory pool that records which thread frees each of its
// allocations, so a case can check where the last reference to an array went
// on any build, with or without a sanitizer watching.

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>

#include <arrow/memory_pool.h>

namespace clink::clickhouse::native::testing {

class ThreadRecordingPool final : public arrow::ProxyMemoryPool {
public:
    // The thread that builds the pool is the one every free is expected on.
    ThreadRecordingPool() : arrow::ProxyMemoryPool(arrow::default_memory_pool()) {}

    using arrow::ProxyMemoryPool::Free;
    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            ++(std::this_thread::get_id() == owner_ ? on_owner_ : elsewhere_);
        }
        arrow::ProxyMemoryPool::Free(buffer, size, alignment);
    }

    [[nodiscard]] std::size_t frees_on_owner() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return on_owner_;
    }
    [[nodiscard]] std::size_t frees_elsewhere() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return elsewhere_;
    }

private:
    const std::thread::id owner_ = std::this_thread::get_id();
    mutable std::mutex mu_;
    std::size_t on_owner_{0};
    std::size_t elsewhere_{0};
};

}  // namespace clink::clickhouse::native::testing
