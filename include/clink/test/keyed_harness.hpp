#pragma once

// clink::test::KeyedOneInputOperatorHarness - the keyed extension of
// OneInputOperatorHarness: everything the base harness does, plus
// typed keyed-state inspection and key-scoped timer queries.
//
//   auto h = clink::test::make_keyed_process_function_harness(
//       MyCountFn{}, [](const Event& e) { return e.user; });
//   h.open();
//   h.process_element(Event{.user = "alice"}, 1000);
//   EXPECT_EQ(h.state_value<std::int64_t>("alice", "count"), 1);
//   EXPECT_TRUE(h.has_event_time_timer(2000, "alice"));
//
// State inspection is the PRODUCTION read path: state_value constructs
// the same KeyedState view the operator uses (same backend, operator
// id, slot and codecs) and calls get(). It never mutates state; use
// seed_state / clear_state when mutation is the point. Codecs default
// via clink::test::default_codec<T> (std::string and std::int64_t are
// pre-wired; specialise it for your own key/value types, or pass
// codecs explicitly).
//
// Timer keys in clink are opaque strings chosen by the operator
// (typically the string-encoded current key), so key-scoped timer
// queries take that string form.
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <concepts>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/test/one_input_harness.hpp"

namespace clink::test {

// Codec resolution for the concise state-inspection overloads.
// Specialise for user types:
//   template <> struct clink::test::default_codec<MyKey> {
//       static Codec<MyKey> get() { return my_key_codec(); }
//   };
template <typename T>
struct default_codec;

template <>
struct default_codec<std::string> {
    static Codec<std::string> get() { return string_codec(); }
};

template <>
struct default_codec<std::int64_t> {
    static Codec<std::int64_t> get() { return int64_codec(); }
};

template <typename T>
concept HasDefaultCodec = requires {
    { default_codec<T>::get() } -> std::same_as<Codec<T>>;
};

template <typename In, typename Out, typename K>
class KeyedOneInputOperatorHarness : public OneInputOperatorHarness<In, Out> {
public:
    using Base = OneInputOperatorHarness<In, Out>;
    using Options = typename Base::Options;

    static KeyedOneInputOperatorHarness create(std::shared_ptr<Operator<In, Out>> op,
                                               Options options = {}) {
        return KeyedOneInputOperatorHarness(Base::create(std::move(op), std::move(options)));
    }

    template <typename Op>
        requires std::derived_from<Op, Operator<In, Out>>
    static KeyedOneInputOperatorHarness create(Op op, Options options = {}) {
        return KeyedOneInputOperatorHarness(Base::create(std::move(op), std::move(options)));
    }

    // ---- Keyed state inspection (the production read path) ----

    // The value stored for `key` in `slot`, or nullopt. Explicit-codec form.
    template <typename V>
    std::optional<V> state_value(const K& key, const std::string& slot, Codec<K> kc, Codec<V> vc) {
        return this->runtime()
            .template keyed_state<K, V>(slot, std::move(kc), std::move(vc))
            .get(key);
    }

    // Default-codec convenience (std::string / std::int64_t out of the box).
    template <typename V>
        requires HasDefaultCodec<K> && HasDefaultCodec<V>
    std::optional<V> state_value(const K& key, const std::string& slot) {
        return state_value<V>(key, slot, default_codec<K>::get(), default_codec<V>::get());
    }

    // Seed state before processing (arrange-phase setup) - the production
    // write path, so codecs and key encoding match exactly.
    template <typename V>
    void seed_state(
        const K& key, const std::string& slot, const V& value, Codec<K> kc, Codec<V> vc) {
        this->runtime()
            .template keyed_state<K, V>(slot, std::move(kc), std::move(vc))
            .put(key, value);
    }

    template <typename V>
        requires HasDefaultCodec<K> && HasDefaultCodec<V>
    void seed_state(const K& key, const std::string& slot, const V& value) {
        seed_state<V>(key, slot, value, default_codec<K>::get(), default_codec<V>::get());
    }

    // Remove one key's entry from a slot.
    void clear_state(const K& key, const std::string& slot, Codec<K> kc) {
        this->runtime()
            .template keyed_state<K, std::int64_t>(slot, std::move(kc), int64_codec())
            .erase(key);
    }

    template <typename Unused = void>
        requires HasDefaultCodec<K>
    void clear_state(const K& key, const std::string& slot) {
        clear_state(key, slot, default_codec<K>::get());
    }

    // Every key currently stored in `slot` (diagnostics; decoded via the
    // codecs). Order follows the backend's key encoding.
    template <typename V>
    std::vector<K> known_keys(const std::string& slot, Codec<K> kc, Codec<V> vc) {
        std::vector<K> out;
        auto view = this->runtime().template keyed_state<K, V>(slot, std::move(kc), std::move(vc));
        view.scan([&](const K& k, const V&) { out.push_back(k); });
        return out;
    }

    template <typename V>
        requires HasDefaultCodec<K> && HasDefaultCodec<V>
    std::vector<K> known_keys(const std::string& slot) {
        return known_keys<V>(slot, default_codec<K>::get(), default_codec<V>::get());
    }

    // ---- Key-scoped timer queries (timer keys are opaque strings) ----

    bool has_event_time_timer(std::int64_t timestamp_ms, const std::string& timer_key) const {
        return this->event_time_timers().count({timestamp_ms, timer_key}) != 0;
    }

    bool has_processing_time_timer(std::int64_t timestamp_ms, const std::string& timer_key) const {
        return this->processing_time_timers().count({timestamp_ms, timer_key}) != 0;
    }

    // All timers registered under one timer key, in firing order.
    std::vector<std::int64_t> event_time_timers_for(const std::string& timer_key) const {
        std::vector<std::int64_t> out;
        for (const auto& [ts, k] : this->event_time_timers()) {
            if (k == timer_key) {
                out.push_back(ts);
            }
        }
        return out;
    }

    std::vector<std::int64_t> processing_time_timers_for(const std::string& timer_key) const {
        std::vector<std::int64_t> out;
        for (const auto& [ts, k] : this->processing_time_timers()) {
            if (k == timer_key) {
                out.push_back(ts);
            }
        }
        return out;
    }

private:
    explicit KeyedOneInputOperatorHarness(Base base) : Base(std::move(base)) {}
};

// ---- Concise factories for the ProcessFunction family ----

// Harness over a non-keyed ProcessFunction (moved into the adapter the
// production fluent API uses).
template <typename Fn>
auto make_process_function_harness(
    Fn fn,
    typename OneInputOperatorHarness<typename Fn::input_type, typename Fn::output_type>::Options
        options = {}) {
    using I = typename Fn::input_type;
    using O = typename Fn::output_type;
    auto adapter = std::make_shared<clink::detail::ProcessFunctionAdapter<I, O>>(
        std::make_shared<Fn>(std::move(fn)), "process_function_under_test");
    return OneInputOperatorHarness<I, O>::create(std::move(adapter), std::move(options));
}

// Harness over a KeyedProcessFunction + key selector, through the same
// KeyedProcessFunctionAdapter the production fluent API builds.
// `timer_key_fn` (optional) decodes a timer's string key back to K so
// on_timer sees the right current_key - pass it whenever your function
// registers timers.
template <typename Fn, typename KeyFn>
auto make_keyed_process_function_harness(
    Fn fn,
    KeyFn key_fn,
    std::function<typename Fn::key_type(const std::string&)> timer_key_fn = nullptr,
    typename OneInputOperatorHarness<typename Fn::input_type, typename Fn::output_type>::Options
        options = {}) {
    using K = typename Fn::key_type;
    using I = typename Fn::input_type;
    using O = typename Fn::output_type;
    auto adapter = std::make_shared<clink::detail::KeyedProcessFunctionAdapter<K, I, O>>(
        std::make_shared<Fn>(std::move(fn)),
        std::move(key_fn),
        std::move(timer_key_fn),
        "keyed_process_function_under_test");
    return KeyedOneInputOperatorHarness<I, O, K>::create(std::move(adapter), std::move(options));
}

}  // namespace clink::test
