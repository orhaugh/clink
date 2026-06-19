#pragma once

// Typed keyed-state primitives (FOUND-1): List / Map / Aggregating / Reducing
// state, the collection state types beyond the single-value ValueState that
// KeyedState<K,V> already provides.
//
// Each is a thin typed wrapper over a KeyedState slot, so it inherits the
// existing key encoding, TTL, and - crucially - snapshot/restore for free: the
// collection is just a KeyedState value, captured and restored like any other.
// Construct via the RuntimeContext factories (list_state / map_state /
// aggregating_state / reducing_state), which scope the slot to the operator.
//
// Representation: List and Map store the whole collection as ONE serialized
// value per key (vector<E> / vector<pair<MK,MV>>). add/put is therefore a
// read-modify-write that is O(collection size), the natural shape for a
// heap-resident collection slot. A true incremental O(1) append needs a backend
// merge operator (a separate optimisation); the typed API here is the
// deliverable and is correct + durable regardless.

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

// A list of E values per key K. add appends; get returns the list (empty if
// absent); update replaces it; clear removes the key.
template <typename K, typename E>
class ListState {
public:
    ListState(StateBackend& backend, OperatorId op, std::string slot, Codec<K> kc, Codec<E> ec)
        : state_(backend, op, std::move(slot), std::move(kc), vector_codec<E>(std::move(ec))) {}

    void add(const K& k, E e) {
        auto cur = state_.get(k).value_or(std::vector<E>{});
        cur.push_back(std::move(e));
        state_.put(k, cur);
    }

    void add_all(const K& k, const std::vector<E>& es) {
        if (es.empty()) {
            return;
        }
        auto cur = state_.get(k).value_or(std::vector<E>{});
        cur.insert(cur.end(), es.begin(), es.end());
        state_.put(k, cur);
    }

    [[nodiscard]] std::vector<E> get(const K& k) const {
        return state_.get(k).value_or(std::vector<E>{});
    }

    [[nodiscard]] bool empty(const K& k) const {
        auto v = state_.get(k);
        return !v.has_value() || v->empty();
    }

    // Replace the whole list (empty clears the key, so an empty list and an
    // absent key are indistinguishable).
    void update(const K& k, std::vector<E> es) {
        if (es.empty()) {
            state_.erase(k);
        } else {
            state_.put(k, std::move(es));
        }
    }

    void clear(const K& k) { state_.erase(k); }

private:
    KeyedState<K, std::vector<E>> state_;
};

// A map MK -> MV per key K, stored as one serialized assoc-list per key.
template <typename K, typename MK, typename MV>
class MapState {
public:
    MapState(StateBackend& backend,
             OperatorId op,
             std::string slot,
             Codec<K> kc,
             Codec<MK> mkc,
             Codec<MV> mvc)
        : state_(backend,
                 op,
                 std::move(slot),
                 std::move(kc),
                 vector_codec<Entry>(pair_codec<MK, MV>(std::move(mkc), std::move(mvc)))) {}

    void put(const K& k, const MK& mk, MV mv) {
        auto cur = state_.get(k).value_or(std::vector<Entry>{});
        for (auto& e : cur) {
            if (e.first == mk) {
                e.second = std::move(mv);
                state_.put(k, cur);
                return;
            }
        }
        cur.emplace_back(mk, std::move(mv));
        state_.put(k, cur);
    }

    [[nodiscard]] std::optional<MV> get(const K& k, const MK& mk) const {
        auto cur = state_.get(k);
        if (!cur) {
            return std::nullopt;
        }
        for (const auto& e : *cur) {
            if (e.first == mk) {
                return e.second;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] bool contains(const K& k, const MK& mk) const { return get(k, mk).has_value(); }

    void remove(const K& k, const MK& mk) {
        auto cur = state_.get(k);
        if (!cur) {
            return;
        }
        for (auto it = cur->begin(); it != cur->end(); ++it) {
            if (it->first == mk) {
                cur->erase(it);
                if (cur->empty()) {
                    state_.erase(k);
                } else {
                    state_.put(k, *cur);
                }
                return;
            }
        }
    }

    [[nodiscard]] std::vector<std::pair<MK, MV>> entries(const K& k) const {
        return state_.get(k).value_or(std::vector<Entry>{});
    }

    void clear(const K& k) { state_.erase(k); }

private:
    using Entry = std::pair<MK, MV>;
    KeyedState<K, std::vector<Entry>> state_;
};

// One accumulator Acc per key. add folds an In via add_fn; get finalises via
// result_fn (the classic aggregate shape: initialise an accumulator, fold each
// input in via add, then finalise to a result). The accumulator is persisted.
template <typename K, typename In, typename Acc, typename Out>
class AggregatingState {
public:
    using Initial = std::function<Acc()>;
    using AddFn = std::function<Acc(const Acc&, const In&)>;
    using ResultFn = std::function<Out(const Acc&)>;

    AggregatingState(StateBackend& backend,
                     OperatorId op,
                     std::string slot,
                     Codec<K> kc,
                     Codec<Acc> acc_codec,
                     Initial initial,
                     AddFn add_fn,
                     ResultFn result_fn)
        : state_(backend, op, std::move(slot), std::move(kc), std::move(acc_codec)),
          initial_(std::move(initial)),
          add_fn_(std::move(add_fn)),
          result_fn_(std::move(result_fn)) {}

    void add(const K& k, const In& in) {
        Acc acc = state_.get(k).value_or(initial_());
        state_.put(k, add_fn_(acc, in));
    }

    // The finalised result, or nullopt if the key has no accumulator yet.
    [[nodiscard]] std::optional<Out> get(const K& k) const {
        auto acc = state_.get(k);
        if (!acc) {
            return std::nullopt;
        }
        return result_fn_(*acc);
    }

    [[nodiscard]] std::optional<Acc> accumulator(const K& k) const { return state_.get(k); }

    void clear(const K& k) { state_.erase(k); }

private:
    KeyedState<K, Acc> state_;
    Initial initial_;
    AddFn add_fn_;
    ResultFn result_fn_;
};

// One value V per key, combined on add via reduce_fn (the reduce shape: the
// first add stores the value, each subsequent add reduces).
template <typename K, typename V>
class ReducingState {
public:
    using ReduceFn = std::function<V(const V&, const V&)>;

    ReducingState(StateBackend& backend,
                  OperatorId op,
                  std::string slot,
                  Codec<K> kc,
                  Codec<V> vc,
                  ReduceFn reduce_fn)
        : state_(backend, op, std::move(slot), std::move(kc), std::move(vc)),
          reduce_fn_(std::move(reduce_fn)) {}

    void add(const K& k, const V& v) {
        auto cur = state_.get(k);
        state_.put(k, cur.has_value() ? reduce_fn_(*cur, v) : v);
    }

    [[nodiscard]] std::optional<V> get(const K& k) const { return state_.get(k); }

    void clear(const K& k) { state_.erase(k); }

private:
    KeyedState<K, V> state_;
    ReduceFn reduce_fn_;
};

}  // namespace clink
