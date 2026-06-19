#include <chrono>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/filter_operator.hpp"
#include "clink/operators/key_by_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;
using namespace std::chrono_literals;

TEST(Watermark, MaxWatermarkReachesSink) {
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{Record<int>{1}});
    auto m = std::make_shared<MapOperator<int, int>>([](int x) { return x; });
    auto f = std::make_shared<FilterOperator<int>>([](int) { return true; });
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, m);
    auto h2 = dag.add_operator<int, int>(h1, f);
    dag.add_sink<int>(h2, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->last_watermark(), Watermark::max());
}

TEST(Watermark, TumblingWindowFiresOnWatermark) {
    // Three keyed events, all in the [0, 1000ms) window, plus one in
    // [1000ms, 2000ms). With a max-watermark from the source, both windows
    // should fire and aggregate by key.
    Dag dag;

    using KV = std::pair<std::string, int>;

    std::vector<Record<KV>> input;
    input.emplace_back(Record<KV>{KV{"a", 1}, EventTime{100}});
    input.emplace_back(Record<KV>{KV{"a", 2}, EventTime{500}});
    input.emplace_back(Record<KV>{KV{"b", 3}, EventTime{700}});
    input.emplace_back(Record<KV>{KV{"a", 100}, EventTime{1500}});

    auto src = std::make_shared<VectorSource<KV>>(std::move(input));
    auto window = std::make_shared<TumblingWindowOperator<std::string, int, int>>(
        1000ms, [] { return 0; }, [](const int& acc, const int& v) { return acc + v; });
    auto sink = std::make_shared<CollectingSink<std::pair<std::string, int>>>();

    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, std::pair<std::string, int>>(h0, window);
    dag.add_sink<std::pair<std::string, int>>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto results = sink->collected();
    // {"a": 3} and {"b": 3} from window [0,1000); {"a": 100} from window [1000,2000)
    int sum_a_first = 0;
    int sum_b_first = 0;
    int sum_a_second = 0;
    for (const auto& [k, v] : results) {
        if (k == "a" && v == 3)
            sum_a_first = v;
        if (k == "b" && v == 3)
            sum_b_first = v;
        if (k == "a" && v == 100)
            sum_a_second = v;
    }
    EXPECT_EQ(sum_a_first, 3);
    EXPECT_EQ(sum_b_first, 3);
    EXPECT_EQ(sum_a_second, 100);
    EXPECT_EQ(results.size(), 3u);
}

namespace {

// A source that emits records but deliberately never emits a watermark - this
// proves the window operator fires its residual state via flush() at EOS.
template <typename T>
class NoWatermarkVectorSource final : public Source<T> {
public:
    explicit NoWatermarkVectorSource(std::vector<Record<T>> records)
        : records_(std::move(records)) {}

    bool produce(Emitter<T>& out) override {
        if (emitted_) {
            return false;
        }
        Batch<T> b{records_};
        out.emit_data(std::move(b));
        emitted_ = true;
        return false;
    }

    std::string name() const override { return "no_watermark_source"; }

private:
    std::vector<Record<T>> records_;
    bool emitted_{false};
};

}  // namespace

TEST(Watermark, FlushDrainsWindowWithoutMaxWatermark) {
    using KV = std::pair<std::string, int>;
    Dag dag;

    std::vector<Record<KV>> input;
    input.emplace_back(Record<KV>{KV{"a", 5}, EventTime{100}});
    input.emplace_back(Record<KV>{KV{"a", 7}, EventTime{500}});
    input.emplace_back(Record<KV>{KV{"b", 9}, EventTime{900}});

    auto src = std::make_shared<NoWatermarkVectorSource<KV>>(std::move(input));
    auto window = std::make_shared<TumblingWindowOperator<std::string, int, int>>(
        1000ms, [] { return 0; }, [](const int& acc, const int& v) { return acc + v; });
    auto sink = std::make_shared<CollectingSink<std::pair<std::string, int>>>();

    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, std::pair<std::string, int>>(h0, window);
    dag.add_sink<std::pair<std::string, int>>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto results = sink->collected();
    int sum_a = 0;
    int sum_b = 0;
    for (const auto& [k, v] : results) {
        if (k == "a") {
            sum_a = v;
        } else if (k == "b") {
            sum_b = v;
        }
    }
    EXPECT_EQ(sum_a, 12);
    EXPECT_EQ(sum_b, 9);
    EXPECT_EQ(results.size(), 2u);
}
