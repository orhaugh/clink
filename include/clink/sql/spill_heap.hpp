#pragma once

#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/core/codec.hpp"
#include "clink/runtime/memory_budget.hpp"
#include "clink/sql/spill_store.hpp"

namespace clink::sql {

// A binary heap with one entry per scratch file and no resident index. Reads
// charge the decoded entry for its lifetime. The comparator orders the desired
// root first; reversing it and rebuilding supports ordered top-N output.
template <class T>
class SpillHeap {
public:
    struct Entry {
        T value;
        MemoryReservation charge;
    };
    SpillHeap(const std::string& directory,
              std::shared_ptr<MemoryBudget> budget,
              Codec<T> codec,
              std::function<std::size_t(const T&)> estimate,
              std::function<bool(const T&, const T&)> before)
        : store_(directory),
          budget_(std::move(budget)),
          codec_(std::move(codec)),
          estimate_(std::move(estimate)),
          before_(std::move(before)) {}

    std::size_t size() const noexcept { return size_; }
    Entry top() const { return read_(0); }

    void push(const T& value) {
        MemoryReservation incoming(budget_, MemoryCategory::State);
        incoming.resize(estimate_(value));
        auto index = size_++;
        write_(index, value);
        while (index != 0) {
            const auto parent = (index - 1) / 2;
            auto p = read_(parent);
            auto child = read_(index);
            if (!before_(child.value, p.value))
                break;
            write_(index, p.value);
            write_(parent, child.value);
            index = parent;
        }
    }
    void replace_top(const T& value) {
        MemoryReservation incoming(budget_, MemoryCategory::State);
        incoming.resize(estimate_(value));
        if (!size_)
            throw std::logic_error("SQL_SPILL_ERROR: empty heap replacement");
        write_(0, value);
        sift_down_(0);
    }
    Entry pop() {
        auto result = top();
        --size_;
        if (size_) {
            auto last = read_(size_);
            write_(0, last.value);
        }
        store_.erase(std::to_string(size_));
        if (size_)
            sift_down_(0);
        return result;
    }
    void reverse_order() {
        auto old = std::move(before_);
        before_ = [old = std::move(old)](const T& a, const T& b) { return old(b, a); };
        for (auto index = size_ / 2; index != 0; --index)
            sift_down_(index - 1);
    }

private:
    Entry read_(std::size_t index) const {
        auto bytes = store_.get(std::to_string(index));
        if (!bytes)
            throw std::runtime_error("SQL_SPILL_ERROR: missing heap entry");
        auto value = codec_.decode(*bytes);
        if (!value)
            throw std::runtime_error("SQL_SPILL_ERROR: invalid heap entry");
        MemoryReservation charge(budget_, MemoryCategory::State);
        charge.resize(estimate_(*value));
        return {std::move(*value), std::move(charge)};
    }
    void write_(std::size_t index, const T& value) {
        // The caller owns/charges the value. Encoding buffers are transient,
        // as with WorkingSet; put performs the checksummed atomic replacement.
        store_.put(std::to_string(index), codec_.encode(value));
    }
    void sift_down_(std::size_t index) {
        while (index < size_ / 2) {
            auto child = index * 2 + 1;
            if (child + 1 < size_) {
                auto left = read_(child);
                auto right = read_(child + 1);
                if (before_(right.value, left.value))
                    ++child;
            }
            auto parent = read_(index);
            auto next = read_(child);
            if (!before_(next.value, parent.value))
                break;
            write_(index, next.value);
            write_(child, parent.value);
            index = child;
        }
    }
    SpillStore store_;
    std::shared_ptr<MemoryBudget> budget_;
    Codec<T> codec_;
    std::function<std::size_t(const T&)> estimate_;
    std::function<bool(const T&, const T&)> before_;
    std::size_t size_{0};
};
}  // namespace clink::sql
