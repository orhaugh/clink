// Unit tests for KeyByOperator. Verifies the key extractor runs per
// record, the output is a (Key, Value) pair, event time is preserved,
// and watermarks/barriers are forwarded unchanged.

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/key_by_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"

using namespace clink;

namespace {

template <typename T>
std::vector<StreamElement<T>> drain(BoundedChannel<StreamElement<T>>& ch) {
    std::vector<StreamElement<T>> out;
    while (auto e = ch.try_pop()) {
        out.push_back(std::move(*e));
    }
    return out;
}

struct Event {
    std::string user;
    int action;
};

}  // namespace

TEST(KeyByOperator, ExtractsKeyAndPairsWithValue) {
    BoundedChannel<StreamElement<std::pair<std::string, Event>>> ch(64);
    Emitter<std::pair<std::string, Event>> em(&ch);

    KeyByOperator<Event, std::string> op([](const Event& e) { return e.user; });

    Batch<Event> b;
    b.emplace(Event{"alice", 1});
    b.emplace(Event{"bob", 2});
    b.emplace(Event{"alice", 3});
    op.process(StreamElement<Event>::data(std::move(b)), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    const auto& out = elements[0].as_data();
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0].value().first, "alice");
    EXPECT_EQ(out[0].value().second.action, 1);
    EXPECT_EQ(out[1].value().first, "bob");
    EXPECT_EQ(out[2].value().first, "alice");
    EXPECT_EQ(out[2].value().second.action, 3);
}

TEST(KeyByOperator, PreservesEventTime) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    KeyByOperator<int, int> op([](const int& x) { return x % 2; });

    Batch<int> b;
    b.emplace(10, EventTime{100});
    b.emplace(11, EventTime{200});
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    const auto& out = elements[0].as_data();
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].value().first, 0);
    EXPECT_EQ(out[0].event_time()->millis(), 100);
    EXPECT_EQ(out[1].value().first, 1);
    EXPECT_EQ(out[1].event_time()->millis(), 200);
}

TEST(KeyByOperator, RecordsWithoutEventTimeStayWithoutEventTime) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    KeyByOperator<int, int> op([](const int& x) { return x; });

    Batch<int> b;
    b.emplace(7);
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    EXPECT_FALSE(elements[0].as_data()[0].event_time().has_value());
}

TEST(KeyByOperator, EmptyBatchEmitsEmptyBatch) {
    // Documented: KeyByOperator emits the (empty) batch unconditionally,
    // unlike FlatMapOperator which suppresses empty output. This is
    // because keying is structurally 1:1 - there's no "filter" semantic.
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    KeyByOperator<int, int> op([](const int& x) { return x; });

    Batch<int> empty;
    op.process(StreamElement<int>::data(std::move(empty)), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_data());
    EXPECT_TRUE(elements[0].as_data().empty());
}

TEST(KeyByOperator, ForwardsWatermark) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    KeyByOperator<int, int> op([](const int&) { return 0; });
    op.process(StreamElement<int>::watermark(Watermark{EventTime{42}}), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_watermark());
    EXPECT_EQ(elements[0].as_watermark().timestamp().millis(), 42);
}

TEST(KeyByOperator, ForwardsBarrier) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    KeyByOperator<int, int> op([](const int&) { return 0; });
    op.process(StreamElement<int>::barrier(CheckpointBarrier{CheckpointId{99}}), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_barrier());
    EXPECT_EQ(elements[0].as_barrier().id(), CheckpointId{99});
}

TEST(KeyByOperator, NameIsConfigurable) {
    KeyByOperator<int, int> a([](const int&) { return 0; });
    KeyByOperator<int, int> b([](const int&) { return 0; }, "by_user");
    EXPECT_EQ(a.name(), "key_by");
    EXPECT_EQ(b.name(), "by_user");
}
