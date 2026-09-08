#pragma once
#include <arrow/memory_pool.h>
#include <arrow/status.h>

#include "clink/runtime/memory_budget.hpp"

namespace clink {
// The pool must outlive every Arrow buffer allocated through it. No process
// default is changed: unrelated jobs retain their own accounting domains.
class BudgetArrowMemoryPool final : public arrow::MemoryPool {
public:
    explicit BudgetArrowMemoryPool(std::shared_ptr<MemoryBudget> budget,
                                   MemoryCategory category = MemoryCategory::Arrow,
                                   arrow::MemoryPool* upstream = arrow::default_memory_pool())
        : budget_(std::move(budget)), category_(category), pool_(upstream) {}
    using arrow::MemoryPool::Allocate;
    using arrow::MemoryPool::Free;
    using arrow::MemoryPool::Reallocate;
    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        if (size < 0)
            return arrow::Status::Invalid("negative allocation size");
        ARROW_RETURN_NOT_OK(acquire_(size));
        auto status = pool_.Allocate(size, alignment, out);
        if (!status.ok())
            release_(size);
        return status;
    }
    arrow::Status Reallocate(int64_t old_size,
                             int64_t new_size,
                             int64_t alignment,
                             uint8_t** ptr) override {
        if (old_size < 0 || new_size < 0)
            return arrow::Status::Invalid("negative allocation size");
        // Growing may temporarily hold both buffers. Reserve the entire new
        // allocation before asking the upstream pool to copy the old one.
        const auto reserve = new_size > old_size ? new_size : 0;
        ARROW_RETURN_NOT_OK(acquire_(reserve));
        auto status = pool_.Reallocate(old_size, new_size, alignment, ptr);
        if (!status.ok())
            release_(reserve);
        else
            release_(old_size + reserve - new_size);
        return status;
    }
    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
        pool_.Free(buffer, size, alignment);
        release_(size);
    }
    int64_t bytes_allocated() const override { return pool_.bytes_allocated(); }
    int64_t max_memory() const override { return pool_.max_memory(); }
    int64_t total_bytes_allocated() const override { return pool_.total_bytes_allocated(); }
    int64_t num_allocations() const override { return pool_.num_allocations(); }
    std::string backend_name() const override { return "budget/" + pool_.backend_name(); }

private:
    arrow::Status acquire_(int64_t bytes) {
        try {
            if (budget_)
                budget_->acquire(static_cast<std::size_t>(bytes), category_);
        } catch (const MemoryLimitExceeded& error) {
            return arrow::Status::OutOfMemory(error.what());
        }
        return arrow::Status::OK();
    }
    void release_(int64_t bytes) {
        if (budget_)
            budget_->release(static_cast<std::size_t>(bytes), category_);
    }
    std::shared_ptr<MemoryBudget> budget_;
    MemoryCategory category_;
    arrow::ProxyMemoryPool pool_;
};
}  // namespace clink
