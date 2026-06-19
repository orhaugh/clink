// Event-time tumbling-window count over a synthetic event stream.
//
// We synthesise N events at timestamps 0..N*step ms, key by user_id, and
// aggregate event count per user per 1s window. With Watermark::max() flushed
// at end of stream all windows fire and the sink prints (user, window_end,
// count).

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "clink/operators/key_by_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

int main() {
    using namespace clink;
    using namespace std::chrono_literals;

    struct Event {
        std::string user_id;
    };

    constexpr int n_events = 25;
    constexpr int step_ms = 100;
    std::vector<Record<Event>> input;
    for (int i = 0; i < n_events; ++i) {
        Event e{(i % 3 == 0) ? "alice" : "bob"};
        input.emplace_back(Record<Event>{std::move(e), EventTime{i * step_ms}});
    }

    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::move(input));
    auto key_by = std::make_shared<KeyByOperator<Event, std::string>>(
        [](const Event& e) { return e.user_id; });
    auto window = std::make_shared<TumblingWindowOperator<std::string, Event, std::uint64_t>>(
        1000ms,
        []() -> std::uint64_t { return 0; },
        [](const std::uint64_t& acc, const Event& /*e*/) -> std::uint64_t { return acc + 1; });
    auto sink = std::make_shared<FunctionSink<std::pair<std::string, std::uint64_t>>>(
        [](const std::pair<std::string, std::uint64_t>& kv) {
            std::cout << "user=" << kv.first << " count=" << kv.second << '\n';
        });

    auto h0 = dag.add_source<Event>(src);
    auto h1 = dag.add_operator<Event, std::pair<std::string, Event>>(h0, key_by);
    auto h2 =
        dag.add_operator<std::pair<std::string, Event>, std::pair<std::string, std::uint64_t>>(
            h1, window);
    dag.add_sink<std::pair<std::string, std::uint64_t>>(h2, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    return 0;
}
