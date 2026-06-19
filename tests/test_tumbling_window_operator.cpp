// Unit tests for TumblingWindowOperator. The operator is the heart of
// the event-time machinery: bucket assignment, watermark-driven firing,
// and end-of-stream flushing all need to be airtight. Two execution
// paths to cover: in-memory (default) and persistent via a state
// backend.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

template <typename T>
std::vector<StreamElement<T>> drain(BoundedChannel<StreamElement<T>>& ch) {
    std::vector<StreamElement<T>> out;
    while (auto e = ch.try_pop()) {
        out.push_back(std::move(*e));
    }
    return out;
}

// Collects all data records emitted across batches, paired with their
// event time, sorted by (key, ts) so tests don't depend on map-iteration
// order.
template <typename Key, typename Agg>
std::vector<std::tuple<Key, Agg, std::int64_t>> collect_data(
    const std::vector<StreamElement<std::pair<Key, Agg>>>& es) {
    std::vector<std::tuple<Key, Agg, std::int64_t>> out;
    for (const auto& e : es) {
        if (!e.is_data()) {
            continue;
        }
        for (const auto& r : e.as_data()) {
            const auto ts = r.event_time().has_value() ? r.event_time()->millis() : 0;
            out.emplace_back(r.value().first, r.value().second, ts);
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

// Drive an input batch (key, value, event_time_ms) into the operator.
template <typename Key, typename Value>
void send_batch(TumblingWindowOperator<Key, Value, Value>& op,
                Emitter<std::pair<Key, Value>>& em,
                std::vector<std::tuple<Key, Value, std::int64_t>> rows) {
    Batch<std::pair<Key, Value>> b;
    for (auto& [k, v, ts] : rows) {
        b.emplace(std::make_pair(std::move(k), std::move(v)), EventTime{ts});
    }
    op.process(StreamElement<std::pair<Key, Value>>::data(std::move(b)), em);
}

}  // namespace

TEST(TumblingWindowOperator, AssignsRecordsToWindowsByEventTime) {
    BoundedChannel<StreamElement<std::pair<std::string, int>>> ch(64);
    Emitter<std::pair<std::string, int>> em(&ch);

    // 1s window, sum aggregate.
    TumblingWindowOperator<std::string, int, int> op(
        1000ms, []() { return 0; }, [](const int& agg, const int& v) { return agg + v; });

    // Two records in window [0, 1000) for "a"; one record in window
    // [1000, 2000) for "a"; one record in [0, 1000) for "b".
    send_batch<std::string, int>(
        op, em, {{"a", 1, 100}, {"a", 2, 500}, {"a", 10, 1500}, {"b", 100, 200}});

    // Watermark at 999 - nothing fires yet (no window ended).
    op.process(StreamElement<std::pair<std::string, int>>::watermark(Watermark{EventTime{999}}),
               em);
    auto early = drain(ch);
    auto early_data = collect_data<std::string, int>(early);
    EXPECT_TRUE(early_data.empty());

    // Watermark at 1000 - window [0, 1000) closes for both "a" and "b".
    op.process(StreamElement<std::pair<std::string, int>>::watermark(Watermark{EventTime{1000}}),
               em);
    auto fired = drain(ch);
    auto fired_data = collect_data<std::string, int>(fired);
    // For window-end 1000 emit ts = 999 (window_end - 1). Sums:
    //   a@[0,1000) = 1 + 2 = 3
    //   b@[0,1000) = 100
    EXPECT_EQ(
        fired_data,
        (std::vector<std::tuple<std::string, int, std::int64_t>>{{"a", 3, 999}, {"b", 100, 999}}));

    // Watermark at 2000 - second "a" window fires.
    op.process(StreamElement<std::pair<std::string, int>>::watermark(Watermark{EventTime{2000}}),
               em);
    auto more = drain(ch);
    auto more_data = collect_data<std::string, int>(more);
    EXPECT_EQ(more_data,
              (std::vector<std::tuple<std::string, int, std::int64_t>>{{"a", 10, 1999}}));
}

TEST(TumblingWindowOperator, FlushEmitsAllRemainingBuckets) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, []() { return 0; }, [](const int& agg, const int& v) { return agg + v; });

    send_batch<int, int>(op, em, {{1, 5, 100}, {2, 7, 1500}});

    // No watermark fires either window; flush() should emit both.
    op.flush(em);

    auto data = collect_data<int, int>(drain(ch));
    EXPECT_EQ(data, (std::vector<std::tuple<int, int, std::int64_t>>{{1, 5, 999}, {2, 7, 1999}}));
}

TEST(TumblingWindowOperator, FiringPurgesBucketSoLateRecordsCannotResurrectIt) {
    // A record arriving after its window has fired should land in the
    // *next* window with that key - never re-open the closed bucket.
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, []() { return 0; }, [](const int& agg, const int& v) { return agg + v; });

    send_batch<int, int>(op, em, {{1, 5, 200}});
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1000}}), em);

    // Late record falls in [0, 1000) by event_time but the bucket is gone;
    // the operator simply re-buckets it into a fresh in-memory entry. We
    // assert the second flush produces a 5 once and a 5 once - the
    // already-fired bucket was not double-counted.
    send_batch<int, int>(op, em, {{1, 5, 200}});  // late
    op.flush(em);

    auto data = collect_data<int, int>(drain(ch));
    EXPECT_EQ(data, (std::vector<std::tuple<int, int, std::int64_t>>{{1, 5, 999}, {1, 5, 999}}));
}

TEST(TumblingWindowOperator, ForwardsWatermarkAfterFiring) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, []() { return 0; }, [](const int& agg, const int& v) { return agg + v; });

    send_batch<int, int>(op, em, {{1, 5, 100}});
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1000}}), em);

    auto elements = drain(ch);
    // Expect: data emission first, then watermark forwarded.
    bool saw_data = false;
    bool saw_wm = false;
    for (const auto& e : elements) {
        if (e.is_data()) {
            saw_data = true;
        }
        if (e.is_watermark()) {
            saw_wm = true;
            EXPECT_EQ(e.as_watermark().timestamp().millis(), 1000);
        }
    }
    EXPECT_TRUE(saw_data);
    EXPECT_TRUE(saw_wm);
}

TEST(TumblingWindowOperator, ForwardsBarrier) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, []() { return 0; }, [](const int& agg, const int& v) { return agg + v; });

    op.process(StreamElement<std::pair<int, int>>::barrier(CheckpointBarrier{CheckpointId{42}}),
               em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_barrier());
    EXPECT_EQ(elements[0].as_barrier().id(), CheckpointId{42});
}

namespace {

// int <-> int64 codec adapter so we can use int as a key/agg type with
// the existing int64_codec on the wire.
Codec<int> int_codec() {
    return Codec<int>{.encode = [](const int& v) { return int64_codec().encode(v); },
                      .decode = [](Codec<int>::BytesView b) -> std::optional<int> {
                          auto x = int64_codec().decode(b);
                          if (!x.has_value()) {
                              return std::nullopt;
                          }
                          return static_cast<int>(*x);
                      }};
}

}  // namespace

TEST(TumblingWindowOperator, PersistentPathStoresIntoStateBackend) {
    // When constructed with codecs and given a runtime with a state
    // backend, the operator routes its state through KeyedState. Same
    // input + watermark as the in-memory test, same expected output -
    // the operator should be path-agnostic from the outside.
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms,
        []() { return 0; },
        [](const int& agg, const int& v) { return agg + v; },
        int_codec(),
        int_codec());

    InMemoryStateBackend backend;
    RuntimeContext ctx(OperatorId{1}, "win", &backend, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    send_batch<int, int>(op, em, {{1, 5, 100}, {1, 7, 500}, {2, 9, 200}});
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1000}}), em);

    auto data = collect_data<int, int>(drain(ch));
    EXPECT_EQ(data, (std::vector<std::tuple<int, int, std::int64_t>>{{1, 12, 999}, {2, 9, 999}}));

    op.close();
}

TEST(TumblingWindowOperator, NameIsConfigurable) {
    TumblingWindowOperator<int, int, int> a(
        1000ms, []() { return 0; }, [](const int& a, const int& b) { return a + b; });
    TumblingWindowOperator<int, int, int> b(
        1000ms,
        []() { return 0; },
        [](const int& a, const int& b) { return a + b; },
        "session_count");
    EXPECT_EQ(a.name(), "tumbling_window");
    EXPECT_EQ(b.name(), "session_count");
}
