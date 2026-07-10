#pragma once

// clink::test::TestSequence - a reusable, replayable input script for a
// one-input harness, and the deterministic-shuffle helper for
// order-insensitivity property tests:
//
//   clink::test::TestSequence<Purchase> seq;
//   seq.element(p1, 1000).element(p2, 1100).watermark(2000)
//      .advance_processing_time_to(5000);
//   seq.replay(harness);   // drives the harness through every entry
//
//   // Property: the aggregate is order-insensitive within a window.
//   for (std::uint64_t seed = 0; seed < 20; ++seed) {
//       auto shuffled = clink::test::deterministic_shuffle(inputs, seed);
//       ...assert the same result...
//   }
//
// Everything is deterministic: the shuffle is a fixed splitmix64-fed
// Fisher-Yates, so a given (items, seed) pair yields the same order on
// every platform and every run - a failing seed reproduces forever.
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace clink::test {

template <typename In>
class TestSequence {
public:
    TestSequence& element(In v) {
        entries_.push_back(Element{std::move(v), std::nullopt});
        return *this;
    }
    TestSequence& element(In v, std::int64_t event_time_ms) {
        entries_.push_back(Element{std::move(v), event_time_ms});
        return *this;
    }
    TestSequence& watermark(std::int64_t ts_ms) {
        entries_.push_back(WatermarkAt{ts_ms});
        return *this;
    }
    TestSequence& advance_processing_time_to(std::int64_t now_ms) {
        entries_.push_back(AdvanceTo{now_ms});
        return *this;
    }
    TestSequence& flush() {
        entries_.push_back(Flush{});
        return *this;
    }

    std::size_t size() const noexcept { return entries_.size(); }

    // Drive any one-input harness through the sequence, in order.
    template <typename Harness>
    void replay(Harness& h) const {
        for (const auto& entry : entries_) {
            if (const auto* e = std::get_if<Element>(&entry)) {
                if (e->event_time_ms) {
                    h.process_element(e->value, *e->event_time_ms);
                } else {
                    h.process_element(e->value);
                }
            } else if (const auto* w = std::get_if<WatermarkAt>(&entry)) {
                h.process_watermark(w->ts_ms);
            } else if (const auto* a = std::get_if<AdvanceTo>(&entry)) {
                h.advance_processing_time_to(a->now_ms);
            } else {
                h.flush();
            }
        }
    }

private:
    struct Element {
        In value;
        std::optional<std::int64_t> event_time_ms;
    };
    struct WatermarkAt {
        std::int64_t ts_ms;
    };
    struct AdvanceTo {
        std::int64_t now_ms;
    };
    struct Flush {};
    using Entry = std::variant<Element, WatermarkAt, AdvanceTo, Flush>;

    std::vector<Entry> entries_;
};

// Platform-stable shuffle: splitmix64-fed Fisher-Yates. The same
// (items, seed) always produces the same order, on every standard
// library - unlike std::shuffle, whose output is unspecified across
// implementations.
template <typename T>
std::vector<T> deterministic_shuffle(std::vector<T> items, std::uint64_t seed) {
    auto next = [state = seed + 0x9E3779B97F4A7C15ULL]() mutable {
        state += 0x9E3779B97F4A7C15ULL;
        std::uint64_t z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    };
    for (std::size_t i = items.size(); i > 1; --i) {
        const auto j = static_cast<std::size_t>(next() % i);
        using std::swap;
        swap(items[i - 1], items[j]);
    }
    return items;
}

}  // namespace clink::test
