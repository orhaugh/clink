// Lifecycle smoke tests for every operator type. The per-operator
// unit-test files focus on process() semantics; this file exists so
// the open()/close()/flush()/name()/runtime() instances get exercised
// for every (Key, Value, Agg) combo that lands in production code,
// lifting gcov function coverage on operator_base.hpp template
// instantiations and on each operator's own header.
//
// Each test does the minimum - construct, attach runtime, call
// open()/close()/flush()/name() - without trying to drive operator
// semantics (that's the per-operator file's job).

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "clink/operators/filter_operator.hpp"
#include "clink/operators/flat_map_operator.hpp"
#include "clink/operators/key_by_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/session_window_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/sliding_window_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Sweep one operator through its public lifecycle. The downstream
// channel is local; its contents are discarded.
template <typename Op, typename Out>
void exercise_lifecycle(Op& op, BoundedChannel<StreamElement<Out>>& ch, Emitter<Out>& em) {
    EXPECT_FALSE(op.name().empty());
    op.open();
    op.flush(em);
    op.close();
    (void)ch;
}

}  // namespace

TEST(OperatorLifecycle, Map) {
    BoundedChannel<StreamElement<int>> ch(8);
    Emitter<int> em(&ch);
    MapOperator<int, int> op([](int v) { return v + 1; });
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, MapStringToString) {
    BoundedChannel<StreamElement<std::string>> ch(8);
    Emitter<std::string> em(&ch);
    MapOperator<std::string, std::string> op([](const std::string& s) { return s; });
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, Filter) {
    BoundedChannel<StreamElement<int>> ch(8);
    Emitter<int> em(&ch);
    FilterOperator<int> op([](int) { return true; });
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, FilterString) {
    BoundedChannel<StreamElement<std::string>> ch(8);
    Emitter<std::string> em(&ch);
    FilterOperator<std::string> op([](const std::string&) { return true; });
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, FlatMap) {
    BoundedChannel<StreamElement<int>> ch(8);
    Emitter<int> em(&ch);
    FlatMapOperator<int, int> op([](const int& x) { return std::vector<int>{x}; });
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, KeyBy) {
    using Out = std::pair<int, int>;
    BoundedChannel<StreamElement<Out>> ch(8);
    Emitter<Out> em(&ch);
    KeyByOperator<int, int> op([](const int& v) { return v; });
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, WatermarkAssigner) {
    BoundedChannel<StreamElement<int>> ch(8);
    Emitter<int> em(&ch);
    WatermarkAssignerOperator<int> op([](const int& v) { return EventTime{v}; },
                                      std::make_unique<MonotonicWatermarkStrategy<int>>());
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, TumblingWindow) {
    using Out = std::pair<int, int>;
    BoundedChannel<StreamElement<Out>> ch(8);
    Emitter<Out> em(&ch);
    TumblingWindowOperator<int, int, int> op(
        100ms, [] { return 0; }, [](int a, int v) { return a + v; });
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, SlidingWindow) {
    using Out = std::pair<int, int>;
    BoundedChannel<StreamElement<Out>> ch(8);
    Emitter<Out> em(&ch);
    SlidingWindowOperator<int, int, int> op(
        200ms, 100ms, [] { return 0; }, [](int a, int v) { return a + v; });
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, SessionWindow) {
    using Out = std::pair<int, int>;
    BoundedChannel<StreamElement<Out>> ch(8);
    Emitter<Out> em(&ch);
    SessionWindowOperator<int, int, int> op(
        100ms,
        [] { return 0; },
        [](int a, int v) { return a + v; },
        [](int a, int b) { return a + b; });
    exercise_lifecycle(op, ch, em);
}

TEST(OperatorLifecycle, CollectingSink) {
    CollectingSink<int> sink;
    sink.open();
    sink.flush();
    sink.close();
    EXPECT_EQ(sink.name(), "collecting_sink");
}

TEST(OperatorLifecycle, FunctionSink) {
    FunctionSink<int> sink([](const int&) {});
    sink.open();
    sink.flush();
    sink.close();
    EXPECT_FALSE(sink.name().empty());
}

TEST(OperatorLifecycle, VectorSourceLifecycleAndCancel) {
    std::vector<Record<int>> input;
    input.emplace_back(Record<int>{1});
    VectorSource<int> src(std::move(input));
    src.open();
    src.cancel();
    EXPECT_TRUE(src.cancelled());
    src.close();
    EXPECT_FALSE(src.name().empty());
}

TEST(OperatorLifecycle, OperatorRuntimeAndIdAccessors) {
    MapOperator<int, int> op([](int v) { return v; });
    op.set_id(OperatorId{42});
    EXPECT_EQ(op.id(), OperatorId{42});
    EXPECT_EQ(op.runtime(), nullptr);
}

TEST(OperatorLifecycle, SourceRuntimeAndIdAccessors) {
    std::vector<Record<int>> input;
    VectorSource<int> src(std::move(input));
    src.set_id(OperatorId{7});
    EXPECT_EQ(src.id(), OperatorId{7});
    EXPECT_EQ(src.runtime(), nullptr);
}

TEST(OperatorLifecycle, SinkRuntimeAndIdAccessors) {
    CollectingSink<int> sink;
    sink.set_id(OperatorId{9});
    EXPECT_EQ(sink.id(), OperatorId{9});
    EXPECT_EQ(sink.runtime(), nullptr);
}
