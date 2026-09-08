#pragma once
#include <string>
#include <unordered_map>

#include "clink/runtime/memory_budget.hpp"

namespace clink {
// Incremental accounting for operator containers whose nested standard-library
// payloads do not use a common allocator. Call after each touched key changes,
// and on restore/expiry. Estimates include bookkeeping for this account itself.
class KeyedMemoryAccount {
public:
    void bind(std::shared_ptr<MemoryBudget> budget) {
        if (!sizes_.empty())
            throw std::logic_error("cannot rebind live memory account");
        enabled_ = static_cast<bool>(budget);
        reservation_ = MemoryReservation(std::move(budget), MemoryCategory::State);
    }
    void clear() {
        sizes_.clear();
        reservation_.resize(0);
    }
    bool enabled() const noexcept { return enabled_; }
    void update(const std::string& key, std::size_t bytes) {
        if (!enabled_)
            return;
        auto it = sizes_.find(key);
        const auto previous = it == sizes_.end() ? 0 : it->second;
        // Key copy, value, hash links/bucket allowance. This is an estimate,
        // not an allocator implementation's exact resident byte count.
        const auto overhead = checked_memory_sum(
            key.size(), 1 + sizeof(std::pair<const std::string, std::size_t>) + 4 * sizeof(void*));
        const auto next = checked_memory_sum(bytes, overhead);
        const auto old_total = reservation_.size();
        reservation_.resize(checked_memory_sum(old_total - previous, next));
        try {
            sizes_.insert_or_assign(key, next);
        } catch (...) {
            reservation_.resize(old_total);
            throw;
        }
    }
    void erase(const std::string& key) {
        if (!enabled_)
            return;
        auto it = sizes_.find(key);
        if (it == sizes_.end())
            return;
        const auto bytes = it->second;
        sizes_.erase(it);
        reservation_.resize(reservation_.size() - bytes);
    }

private:
    bool enabled_{false};
    MemoryReservation reservation_;
    std::unordered_map<std::string, std::size_t> sizes_;
};
}  // namespace clink
