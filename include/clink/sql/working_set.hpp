#pragma once

#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "clink/core/codec.hpp"
#include "clink/runtime/keyed_memory_account.hpp"
#include "clink/sql/spill_store.hpp"

namespace clink::sql {

inline std::string sql_spill_directory() {
    const auto* path = std::getenv("CLINK_SQL_SPILL_DIR");
    return path ? path : "";
}

// Accounting and scratch spill for one operator-owned string-keyed map.
// Mutations are explicit: load before taking references, commit after releasing
// them. A commit may invalidate every map reference. Each partition must fit.
template <class Map>
class WorkingSet {
public:
    using Value = typename Map::mapped_type;
    using Estimate = std::function<std::size_t(const Value&)>;
    explicit WorkingSet(Map& state) : state_(state) {}

    void bind(std::shared_ptr<MemoryBudget> budget,
              Codec<Value> codec,
              Estimate estimate,
              std::string directory = sql_spill_directory()) {
        budget_ = std::move(budget);
        codec_ = std::move(codec);
        estimate_ = std::move(estimate);
        directory_ = std::move(directory);
        memory_.bind(budget_);
        if (budget_ && !directory_.empty())
            spill_ = std::make_unique<SpillStore>(directory_);
    }
    bool enabled() const noexcept { return static_cast<bool>(budget_); }
    bool spilled() const noexcept { return spilled_; }
    void set_pressure_relief(std::function<void()> relief) { pressure_relief_ = std::move(relief); }
    bool spill_for_pressure() {
        if (!spill_ || spilled_ || state_.empty())
            return false;
        spill_all_();
        return true;
    }

    void load(const std::string& key) {
        if (!spilled_ || state_.contains(key))
            return;
        if (auto bytes = spill_->get(key)) {
            state_.emplace(key, decode_(*bytes));
            account_(key);
        }
    }
    void commit(const std::string& key) {
        if (!enabled())
            return;
        // key may belong to the map that spill_all_ releases.
        const std::string owned = key;
        try {
            account_(owned);
        } catch (const MemoryLimitExceeded&) {
            if (!spill_ || spilled_)
                throw;
            spill_all_();
            load(owned);
        }
        if (spilled_) {
            if (auto it = state_.find(owned); it != state_.end()) {
                spill_->put(owned, codec_.encode(it->second));
                state_.erase(it);
                memory_.erase(owned);
            }
        }
    }
    void restore(const std::string& key, Value value) {
        state_.insert_or_assign(key, std::move(value));
        commit(key);
    }
    void erase(const std::string& key) {
        if (spilled_)
            spill_->erase(key);
        state_.erase(key);
        memory_.erase(key);
    }

    // Read-only scans never hydrate the working map. Snapshot visitors may
    // copy one value to the durable backend before moving on to the next.
    template <class Visitor>
    void scan(Visitor visitor) const {
        if (spilled_) {
            spill_->scan([&](const std::string& key, const SpillStore::Bytes& bytes) {
                visitor(key, decode_(bytes));
                return true;
            });
        } else {
            for (const auto& [key, value] : state_)
                visitor(key, value);
        }
    }

    // Rebuild through a separate map/store: rewriting the files currently
    // traversed by a directory iterator could skip or duplicate partitions.
    // Empty values remain as checkpoint tombstones unless the caller erases
    // the corresponding backend key itself.
    template <class Visitor>
    void visit(Visitor visitor) {
        if (!enabled()) {
            for (auto it = state_.begin(); it != state_.end();) {
                if constexpr (std::is_same_v<
                                  std::invoke_result_t<Visitor, const std::string&, Value&>,
                                  bool>) {
                    if (!visitor(it->first, it->second)) {
                        it = state_.erase(it);
                        continue;
                    }
                } else
                    visitor(it->first, it->second);
                ++it;
            }
            return;
        }
        Map next_state;
        WorkingSet next(next_state);
        next.bind(budget_, codec_, estimate_, directory_);
        next.spilled_ = spilled_;
        next.pressure_relief_ = pressure_relief_;
        if (spilled_) {
            spill_->scan([&](const std::string& key, const SpillStore::Bytes& bytes) {
                auto value = decode_(bytes);
                // Charge the active partition before executing its scan logic.
                next.state_.emplace(key, std::move(value));
                next.account_(key);
                bool keep = true;
                if constexpr (std::is_same_v<
                                  std::invoke_result_t<Visitor, const std::string&, Value&>,
                                  bool>)
                    keep = visitor(key, next.state_.at(key));
                else
                    visitor(key, next.state_.at(key));
                if (keep)
                    next.commit(key);
                else
                    next.erase(key);
                return true;
            });
        } else {
            while (!state_.empty()) {
                auto it = state_.begin();
                const std::string key = it->first;
                auto value = std::move(it->second);
                state_.erase(it);
                memory_.erase(key);
                next.state_.emplace(key, std::move(value));
                bool keep = true;
                if constexpr (std::is_same_v<
                                  std::invoke_result_t<Visitor, const std::string&, Value&>,
                                  bool>)
                    keep = visitor(key, next.state_.at(key));
                else
                    visitor(key, next.state_.at(key));
                if (keep)
                    next.commit(key);
                else
                    next.erase(key);
            }
        }
        state_.swap(next_state);
        memory_ = std::move(next.memory_);
        spill_ = std::move(next.spill_);
        spilled_ = next.spilled_;
    }

private:
    Value decode_(const SpillStore::Bytes& bytes) const {
        auto value = codec_.decode(bytes);
        if (!value)
            throw std::runtime_error("SQL_SPILL_ERROR: invalid working partition");
        return std::move(*value);
    }
    void account_(const std::string& key) {
        const auto it = state_.find(key);
        if (it == state_.end()) {
            memory_.erase(key);
            return;
        }
        const auto bytes =
            checked_memory_sum(estimate_(it->second),
                               it->first.capacity() + 1 + sizeof(std::string) + 4 * sizeof(void*));
        try {
            memory_.update(key, bytes);
        } catch (const MemoryLimitExceeded&) {
            if (!pressure_relief_)
                throw;
            pressure_relief_();
            memory_.update(key, bytes);
        }
    }
    void spill_all_() {
        for (const auto& [key, value] : state_)
            spill_->put(key, codec_.encode(value));
        Map{}.swap(state_);
        memory_ = KeyedMemoryAccount{};
        memory_.bind(budget_);
        spilled_ = true;
    }
    std::function<void()> pressure_relief_;
    Map& state_;
    std::shared_ptr<MemoryBudget> budget_;
    Codec<Value> codec_;
    Estimate estimate_;
    std::string directory_;
    KeyedMemoryAccount memory_;
    std::unique_ptr<SpillStore> spill_;
    bool spilled_{false};
};
// Link an operator's working sets only where load/commit sites hold no live
// references into their peers. Pressure can then release a cold sibling map
// before refusing a small active entry. All sets must outlive the callbacks.
template <class... Sets>
void link_working_sets(Sets&... sets) {
    auto link = [&](auto& target) {
        target.set_pressure_relief([&sets..., owner = static_cast<const void*>(&target)] {
            ((static_cast<const void*>(&sets) != owner ? sets.spill_for_pressure() : false), ...);
        });
    };
    (link(sets), ...);
}
}  // namespace clink::sql
