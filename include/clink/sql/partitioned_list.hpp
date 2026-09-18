#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/working_set.hpp"

namespace clink::sql {

// Fine-grained ordered partition storage. Only a scalar length and one value
// are read at a time; neither a partition index nor its rows are resident.
// Insertion/removal shifts disk entries, trading I/O for bounded working state.
// Scratch files are private. Checkpoints use entry slots, not whole-list blobs.
template <class T>
class PartitionedList {
public:
    struct Entry {
        T value;
        MemoryReservation charge;
    };
    bool bind(RuntimeContext* runtime,
              std::string prefix,
              Codec<T> codec,
              std::function<std::size_t(const T&)> estimate,
              std::string directory = {},
              bool companion = false) {
        if (!runtime)
            return false;
        runtime_ = runtime;
        budget_ = runtime->memory_budget();
        prefix_ = std::move(prefix);
        codec_ = std::move(codec);
        estimate_ = std::move(estimate);
        if (runtime_->has_state_backend())
            restored_ = runtime_->state_backend()
                            ->get_operator_state(runtime_->operator_id(), prefix_ + ".format")
                            .has_value();
        if (directory.empty())
            directory = sql_spill_directory();
        // A companion store follows an already-enabled entry layout during
        // recovery, including when the job no longer has a configured budget.
        if (!restored_ && !companion && (!budget_ || directory.empty()))
            return false;
        if (runtime_->has_state_backend() && runtime_->state_backend()->supports_async_get()) {
            if (restored_)
                throw std::runtime_error(
                    "SQL_SPILL_ERROR: entry partitions require a synchronous backend");
            return false;
        }
        if (directory.empty())
            directory = std::filesystem::temp_directory_path().string();
        directory_ = directory;
        groups_ = std::make_unique<SpillStore>(directory);
        rows_ = std::make_unique<SpillStore>(directory);
        if (restored_) {
            groups_slot_().scan([&](const auto& key, std::uint64_t count) {
                groups_->put(key, uint64_codec().encode(count));
            });
            rows_slot_().scan([&](const auto& key, const std::string& bytes) {
                rows_->put(key, std::as_bytes(std::span{bytes.data(), bytes.size()}));
            });
        }
        return true;
    }
    bool enabled() const noexcept { return static_cast<bool>(rows_); }
    bool restored() const noexcept { return restored_; }
    std::uint64_t size(const std::string& key) const {
        auto bytes = groups_->get(key);
        if (!bytes)
            return 0;
        auto count = uint64_codec().decode(*bytes);
        if (!count)
            throw std::runtime_error("SQL_SPILL_ERROR: invalid partition length");
        return *count;
    }
    Entry at(const std::string& key, std::uint64_t index) const {
        auto bytes = rows_->get(cell_key_(key, index));
        if (!bytes)
            throw std::runtime_error("SQL_SPILL_ERROR: missing partition entry");
        auto value = codec_.decode(*bytes);
        if (!value)
            throw std::runtime_error("SQL_SPILL_ERROR: invalid partition entry");
        MemoryReservation charge(budget_, MemoryCategory::State);
        charge.resize(checked_memory_sum(estimate_(*value), key.size() + 64));
        return {std::move(*value), std::move(charge)};
    }
    void put(const std::string& key, std::uint64_t index, const T& value) {
        MemoryReservation charge(budget_, MemoryCategory::State);
        charge.resize(checked_memory_sum(estimate_(value), key.size() + 64));
        rows_->put(cell_key_(key, index), codec_.encode(value));
    }
    void append(const std::string& key, const T& value) {
        const auto count = size(key);
        put(key, count, value);
        set_size_(key, count + 1);
    }
    void insert(const std::string& key, std::uint64_t position, const T& value) {
        const auto count = size(key);
        if (position > count)
            throw std::logic_error("partition insertion out of range");
        for (auto index = count; index > position; --index) {
            auto entry = at(key, index - 1);
            put(key, index, entry.value);
        }
        put(key, position, value);
        set_size_(key, count + 1);
    }
    void erase(const std::string& key, std::uint64_t position) {
        const auto count = size(key);
        if (position >= count)
            throw std::logic_error("partition removal out of range");
        for (auto index = position; index + 1 < count; ++index) {
            auto entry = at(key, index + 1);
            put(key, index, entry.value);
        }
        truncate(key, count - 1);
    }
    void truncate(const std::string& key, std::uint64_t count) {
        const auto old = size(key);
        if (count > old)
            throw std::logic_error("partition truncation out of range");
        for (auto index = count; index < old; ++index) {
            const auto cell = cell_key_(key, index);
            rows_->erase(cell);
            if (runtime_->has_state_backend())
                runtime_->state_backend()->erase(runtime_->operator_id(), backend_cell_key_(cell));
        }
        // Keep a zero-length group as a checkpoint tombstone.
        set_size_(key, count);
    }
    void erase_group(const std::string& key) {
        truncate(key, 0);
        groups_->erase(key);
        if (runtime_->has_state_backend())
            groups_slot_().erase(key);
    }
    template <class Visitor>
    void scan(const std::string& key, Visitor visitor) const {
        const auto count = size(key);
        for (std::uint64_t index = 0; index < count; ++index) {
            auto entry = at(key, index);
            if (!visitor(index, entry.value))
                break;
        }
    }
    template <class Visitor>
    void groups(Visitor visitor) const {
        scan_groups([&](const auto& key, auto count) {
            visitor(key, count);
            return true;
        });
    }
    template <class Visitor>
    void scan_groups(Visitor visitor) const {
        groups_->scan([&](const auto& key, const auto& bytes) {
            auto count = uint64_codec().decode(bytes);
            if (!count)
                throw std::runtime_error("SQL_SPILL_ERROR: invalid partition length");
            return visitor(key, *count);
        });
    }
    template <class Visitor>
    void visit_groups(Visitor visitor) {
        SpillStore snapshot(directory_);
        groups_->scan([&](const auto& key, const auto& bytes) {
            snapshot.put(key, bytes);
            return true;
        });
        snapshot.scan([&](const auto& key, const auto& bytes) {
            auto count = uint64_codec().decode(bytes);
            if (!count)
                throw std::runtime_error("SQL_SPILL_ERROR: invalid partition length");
            visitor(key, *count);
            return true;
        });
    }
    void snapshot() {
        if (!runtime_->has_state_backend())
            return;
        auto group_slot = groups_slot_();
        groups([&](const auto& key, auto count) { group_slot.put(key, count); });
        rows_->scan([&](const auto& key, const auto& bytes) {
            const auto encoded = string_codec().encode(
                std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
            runtime_->state_backend()->put(
                runtime_->operator_id(),
                backend_cell_key_(key),
                std::string_view(reinterpret_cast<const char*>(encoded.data()), encoded.size()));
            return true;
        });
        runtime_->state_backend()->put_operator_state(
            runtime_->operator_id(), prefix_ + ".format", "1");
    }

private:
    static std::string cell_key_(const std::string& key, std::uint64_t index) {
        return std::to_string(key.size()) + ":" + key + ":" + std::to_string(index);
    }
    void set_size_(const std::string& key, std::uint64_t count) {
        MemoryReservation charge(budget_, MemoryCategory::State);
        charge.resize(key.size() + 64);
        groups_->put(key, uint64_codec().encode(count));
    }
    KeyedState<std::string, std::uint64_t> groups_slot_() {
        return runtime_->template keyed_state<std::string, std::uint64_t>(
            prefix_ + ".groups", string_codec(), uint64_codec());
    }
    KeyedState<std::string, std::string> rows_slot_() {
        return runtime_->template keyed_state<std::string, std::string>(
            prefix_ + ".rows", string_codec(), string_codec());
    }
    // All cells inherit the root partition's key group, rather than hashing
    // the composite cell identifier. Rescale must move a list as one unit.
    std::string backend_cell_key_(const std::string& cell) const {
        const auto separator = cell.find(':');
        const auto length = std::stoull(cell.substr(0, separator));
        if (separator == std::string::npos || length > cell.size() - separator - 1)
            throw std::runtime_error("SQL_SPILL_ERROR: invalid partition cell key");
        const auto partition = string_codec().encode(cell.substr(separator + 1, length));
        std::string result(1, static_cast<char>(key_group_for_key(partition)));
        result += prefix_ + ".rows|";
        const auto encoded = string_codec().encode(cell);
        result.append(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        return result;
    }
    RuntimeContext* runtime_{nullptr};
    std::shared_ptr<MemoryBudget> budget_;
    std::string prefix_;
    std::string directory_;
    Codec<T> codec_;
    std::function<std::size_t(const T&)> estimate_;
    std::unique_ptr<SpillStore> groups_, rows_;
    bool restored_{false};
};
}  // namespace clink::sql
