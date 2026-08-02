#pragma once

// Typed keyed-state primitives (FOUND-1): List / Map / Aggregating / Reducing
// state, the collection state types beyond the single-value ValueState that
// KeyedState<K,V> already provides.
//
// Each is a thin typed wrapper over a KeyedState slot, so it inherits the
// existing key encoding and - crucially - snapshot/restore for free: the
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
//
// Retention (TTL)
// ---------------
// Each type takes an optional TtlConfig, forwarded to its KeyedState slot.
// This header used to claim TTL was inherited "for free"; it was not - the
// constructors never accepted or forwarded a config, so a caller who read
// that sentence and expected bounded state got unbounded state. The claim
// is now true because the parameter exists.
//
// The semantics follow from the representation, and are worth stating
// because they are NOT what "TTL on a map" might suggest:
//
//   * Retention is PER KEY, not per element. The whole collection for a key
//     is one KeyedState value, so it lives and dies as a unit. A map with
//     one hot entry and a thousand cold ones retains all thousand.
//     Per-element expiry needs a different representation (one backend key
//     per element) and is not what this provides.
//
//   * Any mutation refreshes the whole collection. add / put / remove are
//     read-modify-write, and the write re-stamps the key. So a list that
//     keeps being appended to never expires - which is the intended
//     reading of "retain a key for an hour after its last activity".
//
//   * A read does NOT refresh unless refresh_on_read is set, so a
//     collection that is only ever read still ages out.
//
//   * An expired collection reads as EMPTY, not as an error, and the next
//     add starts a fresh one. That is the same rule ValueState follows: a
//     late record targeting expired state sees nothing.
//
// Event-time TTL needs the operator to feed the watermark in
// (advance_watermark), and releasing memory rather than merely hiding it
// needs cleanup_batch to be called periodically - both are forwarded here
// for exactly that reason. See keyed_state.hpp for why lazy expiry alone
// does not bound anything.

#include <cstddef>
#include <cstdint>
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

    ListState(StateBackend& backend,
              OperatorId op,
              std::string slot,
              Codec<K> kc,
              Codec<E> ec,
              TtlConfig ttl)
        : state_(backend, op, std::move(slot), std::move(kc), vector_codec<E>(std::move(ec)), ttl) {
    }

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

    // --- retention (see the header note) ---------------------------------
    void advance_watermark(std::int64_t watermark_ms) noexcept {
        state_.advance_watermark(watermark_ms);
    }
    std::size_t cleanup_batch(std::size_t budget = 256) { return state_.cleanup_batch(budget); }
    [[nodiscard]] const TtlStats& ttl_stats() const noexcept { return state_.ttl_stats(); }

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

    MapState(StateBackend& backend,
             OperatorId op,
             std::string slot,
             Codec<K> kc,
             Codec<MK> mkc,
             Codec<MV> mvc,
             TtlConfig ttl)
        : state_(backend,
                 op,
                 std::move(slot),
                 std::move(kc),
                 vector_codec<Entry>(pair_codec<MK, MV>(std::move(mkc), std::move(mvc))),
                 ttl) {}

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

    // --- retention (see the header note) ---------------------------------
    void advance_watermark(std::int64_t watermark_ms) noexcept {
        state_.advance_watermark(watermark_ms);
    }
    std::size_t cleanup_batch(std::size_t budget = 256) { return state_.cleanup_batch(budget); }
    [[nodiscard]] const TtlStats& ttl_stats() const noexcept { return state_.ttl_stats(); }

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

    AggregatingState(StateBackend& backend,
                     OperatorId op,
                     std::string slot,
                     Codec<K> kc,
                     Codec<Acc> acc_codec,
                     Initial initial,
                     AddFn add_fn,
                     ResultFn result_fn,
                     TtlConfig ttl)
        : state_(backend, op, std::move(slot), std::move(kc), std::move(acc_codec), ttl),
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

    // --- retention (see the header note) ---------------------------------
    void advance_watermark(std::int64_t watermark_ms) noexcept {
        state_.advance_watermark(watermark_ms);
    }
    std::size_t cleanup_batch(std::size_t budget = 256) { return state_.cleanup_batch(budget); }
    [[nodiscard]] const TtlStats& ttl_stats() const noexcept { return state_.ttl_stats(); }

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

    ReducingState(StateBackend& backend,
                  OperatorId op,
                  std::string slot,
                  Codec<K> kc,
                  Codec<V> vc,
                  ReduceFn reduce_fn,
                  TtlConfig ttl)
        : state_(backend, op, std::move(slot), std::move(kc), std::move(vc), ttl),
          reduce_fn_(std::move(reduce_fn)) {}

    void add(const K& k, const V& v) {
        auto cur = state_.get(k);
        state_.put(k, cur.has_value() ? reduce_fn_(*cur, v) : v);
    }

    [[nodiscard]] std::optional<V> get(const K& k) const { return state_.get(k); }

    void clear(const K& k) { state_.erase(k); }

    // --- retention (see the header note) ---------------------------------
    void advance_watermark(std::int64_t watermark_ms) noexcept {
        state_.advance_watermark(watermark_ms);
    }
    std::size_t cleanup_batch(std::size_t budget = 256) { return state_.cleanup_batch(budget); }
    [[nodiscard]] const TtlStats& ttl_stats() const noexcept { return state_.ttl_stats(); }

private:
    KeyedState<K, V> state_;
    ReduceFn reduce_fn_;
};

}  // namespace clink
