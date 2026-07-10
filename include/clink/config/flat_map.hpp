#pragma once

// FlatMap: a sorted, contiguous, string-keyed map. Backing container
// for config::JsonObject - and therefore for every SQL Row and every
// parsed JSON document in the engine - so its layout is a first-order
// performance surface on the row hot path.
//
// Why not std::map: a node-based tree costs one allocation per key,
// pointer-chasing key compares on lookup, rebalancing on insert and a
// per-node free on destruction. Rows are small (typically 5-30 keys),
// built once, read many times and destroyed whole; a sorted vector of
// (key, value) pairs turns that pattern into one contiguous buffer:
// binary-search lookups over hot cache lines, bulk destruction, and
// cheap whole-row copies.
//
// Observable semantics match std::map<std::string, V> where the
// engine relies on them:
//   * iteration is sorted by key (serialisation order is stable),
//   * emplace/insert keep the FIRST occurrence of a duplicate key
//     (JSON parse relies on first-duplicate-key-wins),
//   * erase(iterator) returns the next iterator (erase-while-iterate),
//   * operator[] default-constructs on a missing key.
// The deliberate divergence: iterators and references are INVALIDATED
// by mutation (vector semantics). Call sites were audited for retained
// references across mutation before the swap; new code must not hold
// them. Lookups are heterogeneous (string_view) - no temporary
// std::string per probe.

#include <algorithm>
#include <cstddef>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace clink::config {

template <typename V>
class FlatMap {
public:
    using key_type = std::string;
    using mapped_type = V;
    using value_type = std::pair<std::string, V>;
    using size_type = std::size_t;

private:
    using Storage = std::vector<value_type>;

public:
    using iterator = typename Storage::iterator;
    using const_iterator = typename Storage::const_iterator;

    FlatMap() = default;
    FlatMap(std::initializer_list<value_type> init) {
        data_.reserve(init.size());
        for (const auto& kv : init) {
            emplace(kv.first, kv.second);
        }
    }

    iterator begin() noexcept { return data_.begin(); }
    iterator end() noexcept { return data_.end(); }
    const_iterator begin() const noexcept { return data_.begin(); }
    const_iterator end() const noexcept { return data_.end(); }
    const_iterator cbegin() const noexcept { return data_.cbegin(); }
    const_iterator cend() const noexcept { return data_.cend(); }

    [[nodiscard]] bool empty() const noexcept { return data_.empty(); }
    [[nodiscard]] size_type size() const noexcept { return data_.size(); }
    void clear() noexcept { data_.clear(); }
    void reserve(size_type n) { data_.reserve(n); }

    iterator find(std::string_view key) noexcept {
        auto it = lower_bound_(key);
        return (it != data_.end() && it->first == key) ? it : data_.end();
    }
    const_iterator find(std::string_view key) const noexcept {
        auto it = lower_bound_(key);
        return (it != data_.end() && it->first == key) ? it : data_.end();
    }

    size_type count(std::string_view key) const noexcept { return contains(key) ? 1 : 0; }
    bool contains(std::string_view key) const noexcept { return find(key) != data_.end(); }

    V& at(std::string_view key) {
        auto it = find(key);
        if (it == data_.end()) {
            throw std::out_of_range("FlatMap::at: missing key '" + std::string(key) + "'");
        }
        return it->second;
    }
    const V& at(std::string_view key) const {
        auto it = find(key);
        if (it == data_.end()) {
            throw std::out_of_range("FlatMap::at: missing key '" + std::string(key) + "'");
        }
        return it->second;
    }

    V& operator[](std::string_view key) {
        auto it = lower_bound_(key);
        if (it == data_.end() || it->first != key) {
            it = data_.emplace(it, std::string(key), V{});
        }
        return it->second;
    }

    // First-duplicate-wins, like std::map: an existing key is left
    // untouched and {existing_iterator, false} is returned.
    template <typename K, typename M>
    std::pair<iterator, bool> emplace(K&& key, M&& value) {
        const std::string_view probe{key};
        auto it = lower_bound_(probe);
        if (it != data_.end() && it->first == probe) {
            return {it, false};
        }
        it = data_.emplace(it, std::string(std::forward<K>(key)), V(std::forward<M>(value)));
        return {it, true};
    }

    std::pair<iterator, bool> insert(const value_type& kv) { return emplace(kv.first, kv.second); }
    std::pair<iterator, bool> insert(value_type&& kv) {
        return emplace(std::move(kv.first), std::move(kv.second));
    }

    template <typename K, typename M>
    std::pair<iterator, bool> insert_or_assign(K&& key, M&& value) {
        const std::string_view probe{key};
        auto it = lower_bound_(probe);
        if (it != data_.end() && it->first == probe) {
            it->second = V(std::forward<M>(value));
            return {it, false};
        }
        it = data_.emplace(it, std::string(std::forward<K>(key)), V(std::forward<M>(value)));
        return {it, true};
    }

    iterator erase(const_iterator pos) { return data_.erase(pos); }
    size_type erase(std::string_view key) {
        auto it = find(key);
        if (it == data_.end()) {
            return 0;
        }
        data_.erase(it);
        return 1;
    }

    void swap(FlatMap& other) noexcept { data_.swap(other.data_); }

    friend bool operator==(const FlatMap& a, const FlatMap& b) { return a.data_ == b.data_; }
    friend bool operator!=(const FlatMap& a, const FlatMap& b) { return !(a == b); }

private:
    iterator lower_bound_(std::string_view key) noexcept {
        return std::lower_bound(
            data_.begin(), data_.end(), key, [](const value_type& e, std::string_view k) noexcept {
                return e.first < k;
            });
    }
    const_iterator lower_bound_(std::string_view key) const noexcept {
        return std::lower_bound(
            data_.begin(), data_.end(), key, [](const value_type& e, std::string_view k) noexcept {
                return e.first < k;
            });
    }

    Storage data_;
};

}  // namespace clink::config
