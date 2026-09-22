// A clink pipeline in a project that pulls clink in with FetchContent.
//
// The point of this program is what surrounds it: the CMakeLists next to it
// clones clink from GitHub at a pinned tag, builds the engine inside this
// project's own build tree and links it as clink::core, the same target the
// installed package exports. The pipeline itself is deliberately ordinary:
//
//   VectorSource<Purchase> -> KeyBy(customer)
//     -> TumblingWindow(1s, sum of pence) -> FunctionSink(collect)
//
// It checks itself: the per-customer window totals the sink collects are
// compared with totals computed directly from the input, and the process exits
// non-zero on any difference, so it registers with CTest.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <clink/operators/key_by_operator.hpp>
#include <clink/operators/sink_operator.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/operators/tumbling_window_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>

namespace {

struct Purchase {
    std::int64_t customer;
    std::int64_t pence;
};

// What the window emits: (customer, total pence in one window).
using Total = std::pair<std::int64_t, std::int64_t>;

}  // namespace

int main() {
    using namespace clink;
    using namespace std::chrono_literals;

    // Twelve purchases from two customers, 250 ms apart in event time, so they
    // span three one-second windows. Every (customer, window) total is distinct,
    // which lets the check below compare totals without the window bounds.
    std::vector<Record<Purchase>> input;
    for (std::int64_t i = 0; i < 12; ++i) {
        const std::int64_t customer = (i % 3 == 0) ? 1 : 2;
        const std::int64_t pence = 100 * (i + 1) + customer;
        input.emplace_back(Purchase{customer, pence}, EventTime{i * 250});
    }

    // The same totals, computed without clink: one bucket per customer and
    // one-second window, using the window arithmetic the operator applies.
    std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> expected_by_window;
    for (const auto& record : input) {
        const auto window_start = record.event_time()->millis() / 1000;
        expected_by_window[{record.value().customer, window_start}] += record.value().pence;
    }
    std::vector<Total> expected;
    for (const auto& [key, total] : expected_by_window) {
        expected.emplace_back(key.first, total);
    }

    Dag dag;

    auto source = std::make_shared<VectorSource<Purchase>>(std::move(input));
    auto key_by = std::make_shared<KeyByOperator<Purchase, std::int64_t>>(
        [](const Purchase& p) { return p.customer; });
    auto window = std::make_shared<TumblingWindowOperator<std::int64_t, Purchase, std::int64_t>>(
        1000ms,
        []() -> std::int64_t { return 0; },
        [](const std::int64_t& acc, const Purchase& p) { return acc + p.pence; });
    // The window keeps per-key state, so it carries a stable uid: that is what a
    // restore or a rescale keys its state on. An operator without one cannot
    // have its state restored.
    window->set_uid("purchase-totals");

    std::vector<Total> got;
    auto sink =
        std::make_shared<FunctionSink<Total>>([&got](const Total& total) { got.push_back(total); });

    auto s0 = dag.add_source<Purchase>(source);
    auto s1 = dag.add_operator<Purchase, std::pair<std::int64_t, Purchase>>(s0, key_by);
    auto s2 = dag.add_operator<std::pair<std::int64_t, Purchase>, Total>(s1, window);
    dag.add_sink<Total>(s2, sink);

    // run() starts every operator thread and returns once the source is
    // exhausted and every sink has drained. VectorSource emits a final watermark
    // at end of stream, which flushes the open windows before that happens.
    LocalExecutor exec(std::move(dag));
    exec.run();

    int failures = 0;
    for (const auto& [op, message] : exec.operator_errors()) {
        std::cerr << "operator " << op << " failed: " << message << '\n';
        ++failures;
    }

    std::sort(got.begin(), got.end());
    std::sort(expected.begin(), expected.end());
    for (const auto& [customer, total] : got) {
        std::cout << "customer=" << customer << " window_total_pence=" << total << '\n';
    }
    if (got != expected) {
        std::cerr << "window totals differ from the input: got " << got.size()
                  << " totals, expected " << expected.size() << '\n';
        ++failures;
    }

    if (failures == 0) {
        std::cout << "fetched_pipeline: OK (clink " CLINK_VERSION_STRING ")\n";
        return 0;
    }
    return 1;
}
