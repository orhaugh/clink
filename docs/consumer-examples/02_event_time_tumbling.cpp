// 02 - Keyed event-time tumbling window.
//
// Counts events per user in 1-second tumbling windows over event time.
// The window fires when the watermark crosses each window boundary.
// VectorSource emits Watermark::max() at end-of-stream, so all
// outstanding windows flush before the sink shuts down.
//
// Pipeline:
//   VectorSource<Event> -> KeyBy(user_id) -> TumblingWindow(1s, count) -> Sink

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <clink/operators/key_by_operator.hpp>
#include <clink/operators/sink_operator.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/operators/tumbling_window_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>

struct Event {
    std::string user_id;
};

int main() {
    using namespace clink;
    using namespace std::chrono_literals;

    // 25 events at 100 ms spacing across two users. With 1s windows, we
    // expect ~3 windows per user.
    std::vector<Record<Event>> input;
    for (int i = 0; i < 25; ++i) {
        Event e{(i % 3 == 0) ? "alice" : "bob"};
        input.emplace_back(Record<Event>{std::move(e), EventTime{i * 100}});
    }

    Dag dag;

    auto src    = std::make_shared<VectorSource<Event>>(std::move(input));
    auto key_by = std::make_shared<KeyByOperator<Event, std::string>>(
        [](const Event& e) { return e.user_id; });
    auto window = std::make_shared<TumblingWindowOperator<std::string, Event, std::uint64_t>>(
        1000ms,
        []() -> std::uint64_t { return 0; },
        [](const std::uint64_t& acc, const Event& /*e*/) { return acc + 1; });
    auto sink   = std::make_shared<FunctionSink<std::pair<std::string, std::uint64_t>>>(
        [](const std::pair<std::string, std::uint64_t>& kv) {
            std::cout << "user=" << kv.first << " count=" << kv.second << '\n';
        });

    auto s0 = dag.add_source<Event>(src);
    auto s1 = dag.add_operator<Event, std::pair<std::string, Event>>(s0, key_by);
    auto s2 = dag.add_operator<std::pair<std::string, Event>,
                               std::pair<std::string, std::uint64_t>>(s1, window);
    dag.add_sink<std::pair<std::string, std::uint64_t>>(s2, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();
    return 0;
}
