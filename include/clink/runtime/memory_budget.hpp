#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace clink {

enum class MemoryCategory : std::size_t { State, Queue, Arrow, Checkpoint, Count };

inline const char* memory_category_name(MemoryCategory category) {
    constexpr std::array names{"state", "queue", "arrow", "checkpoint"};
    return names.at(static_cast<std::size_t>(category));
}

class MemoryLimitExceeded : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

inline std::size_t checked_memory_sum(std::size_t left, std::size_t right) {
    if (right > std::numeric_limits<std::size_t>::max() - left)
        throw MemoryLimitExceeded("MEMORY_LIMIT_EXCEEDED retained-size overflow");
    return left + right;
}

// A process-local accounting domain. Reservations never wait for other owners:
// those owners may need this allocation to drain a channel or checkpoint.
class MemoryBudget {
public:
    struct Usage {
        std::size_t used{0};
        std::size_t peak{0};
        std::size_t refused{0};
        std::array<std::size_t, static_cast<std::size_t>(MemoryCategory::Count)> categories{};
    };

    explicit MemoryBudget(std::size_t limit,
                          std::string name = "execution",
                          std::shared_ptr<MemoryBudget> parent = nullptr)
        : limit_(limit), name_(std::move(name)), parent_(std::move(parent)) {}

    void acquire(std::size_t bytes, MemoryCategory category) {
        const auto index = static_cast<std::size_t>(category);
        if (index >= static_cast<std::size_t>(MemoryCategory::Count)) {
            throw std::invalid_argument("invalid memory category");
        }
        std::lock_guard lock(mu_);
        if (bytes > std::numeric_limits<std::size_t>::max() - usage_.used ||
            (limit_ != 0 && bytes > limit_ - usage_.used)) {
            ++usage_.refused;
            throw MemoryLimitExceeded("MEMORY_LIMIT_EXCEEDED budget=" + name_ +
                                      " category=" + memory_category_name(category) +
                                      " requested=" + std::to_string(bytes) +
                                      " used=" + std::to_string(usage_.used) +
                                      " limit=" + std::to_string(limit_));
        }
        if (parent_)
            parent_->acquire(bytes, category);
        usage_.used += bytes;
        usage_.categories[index] += bytes;
        usage_.peak = std::max(usage_.peak, usage_.used);
    }

    void release(std::size_t bytes, MemoryCategory category) noexcept {
        std::lock_guard lock(mu_);
        usage_.used -= bytes;
        usage_.categories[static_cast<std::size_t>(category)] -= bytes;
        if (parent_)
            parent_->release(bytes, category);
    }

    [[nodiscard]] Usage usage() const {
        std::lock_guard lock(mu_);
        return usage_;
    }
    [[nodiscard]] std::size_t limit() const noexcept { return limit_; }

private:
    const std::size_t limit_;
    const std::string name_;
    const std::shared_ptr<MemoryBudget> parent_;
    mutable std::mutex mu_;
    Usage usage_;
};

// Plain decimal bytes, deliberately strict: a typo must not disable a limit.
inline std::size_t parse_memory_limit_bytes(std::string_view text) {
    std::size_t bytes = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), bytes);
    if (text.empty() || error != std::errc{} || end != text.data() + text.size())
        throw std::invalid_argument("memory limit must be an unsigned decimal byte count");
    return bytes;
}

// Reservations follow ownership, including moves onto another thread.
class MemoryReservation {
public:
    MemoryReservation() = default;
    MemoryReservation(std::shared_ptr<MemoryBudget> budget,
                      MemoryCategory category,
                      std::size_t bytes = 0)
        : budget_(std::move(budget)), category_(category) {
        resize(bytes);
    }
    ~MemoryReservation() { resize(0); }
    MemoryReservation(const MemoryReservation&) = delete;
    MemoryReservation& operator=(const MemoryReservation&) = delete;
    MemoryReservation(MemoryReservation&& other) noexcept
        : budget_(std::move(other.budget_)),
          category_(other.category_),
          bytes_(std::exchange(other.bytes_, 0)) {}
    MemoryReservation& operator=(MemoryReservation&& other) noexcept {
        if (this != &other) {
            resize(0);
            budget_ = std::move(other.budget_);
            category_ = other.category_;
            bytes_ = std::exchange(other.bytes_, 0);
        }
        return *this;
    }
    void resize(std::size_t bytes) {
        if (budget_) {
            if (bytes > bytes_)
                budget_->acquire(bytes - bytes_, category_);
            else if (bytes < bytes_)
                budget_->release(bytes_ - bytes, category_);
        }
        bytes_ = bytes;
    }
    [[nodiscard]] std::size_t size() const noexcept { return bytes_; }

private:
    std::shared_ptr<MemoryBudget> budget_;
    MemoryCategory category_{MemoryCategory::State};
    std::size_t bytes_{0};
};

// The executor installs this on its runner threads. Allocation owners capture
// the shared pointer; freeing memory never consults thread-local state.
inline thread_local std::shared_ptr<MemoryBudget> current_memory_budget;

class MemoryBudgetScope {
public:
    explicit MemoryBudgetScope(std::shared_ptr<MemoryBudget> budget)
        : previous_(std::exchange(current_memory_budget, std::move(budget))) {}
    ~MemoryBudgetScope() { current_memory_budget = std::move(previous_); }
    MemoryBudgetScope(const MemoryBudgetScope&) = delete;
    MemoryBudgetScope& operator=(const MemoryBudgetScope&) = delete;

private:
    std::shared_ptr<MemoryBudget> previous_;
};

// Allocator for engine-owned containers. Copies and allocator rebinds retain
// the same domain, so a container can safely outlive its RuntimeContext.
template <class T>
class BudgetAllocator {
public:
    using value_type = T;
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;
    BudgetAllocator() : budget(current_memory_budget) {}
    explicit BudgetAllocator(std::shared_ptr<MemoryBudget> owner) : budget(std::move(owner)) {}
    template <class U>
    BudgetAllocator(const BudgetAllocator<U>& other) noexcept : budget(other.budget) {}

    [[nodiscard]] T* allocate(std::size_t n) {
        if (n > std::numeric_limits<std::size_t>::max() / sizeof(T))
            throw std::bad_array_new_length();
        const auto bytes = n * sizeof(T);
        if (budget)
            budget->acquire(bytes, MemoryCategory::State);
        try {
            return std::allocator<T>{}.allocate(n);
        } catch (...) {
            if (budget)
                budget->release(bytes, MemoryCategory::State);
            throw;
        }
    }
    void deallocate(T* p, std::size_t n) noexcept {
        std::allocator<T>{}.deallocate(p, n);
        if (budget)
            budget->release(n * sizeof(T), MemoryCategory::State);
    }
    template <class U>
    bool operator==(const BudgetAllocator<U>& other) const noexcept {
        return budget == other.budget;
    }
    std::shared_ptr<MemoryBudget> budget;
};

}  // namespace clink
