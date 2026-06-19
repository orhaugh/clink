// Unit tests for FlatMapOperator. Drives process() directly via an
// Emitter wrapping a BoundedChannel so we can pin element-level
// invariants without needing a full DAG.

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/flat_map_operator.hpp"
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

}  // namespace

TEST(FlatMapOperator, ExpandsOneToMany) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    FlatMapOperator<int, int> op([](const int& x) { return std::vector<int>{x, x * 10, x * 100}; });

    Batch<int> b;
    b.emplace(1);
    b.emplace(2);
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);  // single batch
    ASSERT_TRUE(elements[0].is_data());
    const auto& out_batch = elements[0].as_data();
    ASSERT_EQ(out_batch.size(), 6u);
    EXPECT_EQ(out_batch[0].value(), 1);
    EXPECT_EQ(out_batch[1].value(), 10);
    EXPECT_EQ(out_batch[2].value(), 100);
    EXPECT_EQ(out_batch[3].value(), 2);
    EXPECT_EQ(out_batch[4].value(), 20);
    EXPECT_EQ(out_batch[5].value(), 200);
}

TEST(FlatMapOperator, EmptyOutputSuppressesEmission) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    // fn returns nothing for any input.
    FlatMapOperator<int, int> op([](const int&) { return std::vector<int>{}; });

    Batch<int> b;
    b.emplace(1);
    b.emplace(2);
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    EXPECT_TRUE(elements.empty());
}

TEST(FlatMapOperator, PreservesEventTimePerEmittedRecord) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    FlatMapOperator<int, int> op([](const int& x) { return std::vector<int>{x, x + 1}; });

    Batch<int> b;
    b.emplace(10, EventTime{100});
    b.emplace(20, EventTime{200});
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    const auto& out_batch = elements[0].as_data();
    ASSERT_EQ(out_batch.size(), 4u);
    // Both expansions of input(10@100) inherit ts=100, both expansions of
    // input(20@200) inherit ts=200.
    EXPECT_EQ(out_batch[0].event_time()->millis(), 100);
    EXPECT_EQ(out_batch[1].event_time()->millis(), 100);
    EXPECT_EQ(out_batch[2].event_time()->millis(), 200);
    EXPECT_EQ(out_batch[3].event_time()->millis(), 200);
}

TEST(FlatMapOperator, RecordsWithoutEventTimeStayWithoutEventTime) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    FlatMapOperator<int, int> op([](const int& x) { return std::vector<int>{x}; });

    Batch<int> b;
    b.emplace(7);  // no event time
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    EXPECT_FALSE(elements[0].as_data()[0].event_time().has_value());
}

TEST(FlatMapOperator, ForwardsWatermark) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    FlatMapOperator<int, int> op([](const int& x) { return std::vector<int>{x}; });
    op.process(StreamElement<int>::watermark(Watermark{EventTime{42}}), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_watermark());
    EXPECT_EQ(elements[0].as_watermark().timestamp().millis(), 42);
}

TEST(FlatMapOperator, ForwardsBarrier) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    FlatMapOperator<int, int> op([](const int& x) { return std::vector<int>{x}; });
    op.process(StreamElement<int>::barrier(CheckpointBarrier{CheckpointId{7}}), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_barrier());
    EXPECT_EQ(elements[0].as_barrier().id(), CheckpointId{7});
}

TEST(FlatMapOperator, ChangesOutputType) {
    // FlatMapOperator<In, Out> is templatized on different In/Out - exercise
    // it with a type-changing transform: int → string.
    BoundedChannel<StreamElement<std::string>> ch(64);
    Emitter<std::string> em(&ch);

    FlatMapOperator<int, std::string> op([](const int& x) {
        std::vector<std::string> v;
        for (int i = 0; i < x; ++i) {
            v.push_back("x");
        }
        return v;
    });

    Batch<int> b;
    b.emplace(3);
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    const auto& out = elements[0].as_data();
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0].value(), "x");
    EXPECT_EQ(out[1].value(), "x");
    EXPECT_EQ(out[2].value(), "x");
}

TEST(FlatMapOperator, NameIsConfigurable) {
    FlatMapOperator<int, int> a([](const int&) { return std::vector<int>{}; });
    FlatMapOperator<int, int> b([](const int&) { return std::vector<int>{}; }, "tokenize");
    EXPECT_EQ(a.name(), "flat_map");
    EXPECT_EQ(b.name(), "tokenize");
}
