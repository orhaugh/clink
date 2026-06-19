// 05 - Two-stream interval join on event time.
//
// Joins clicks against orders for the same user, where
//   delta = order.event_time - click.event_time
// falls in [-50ms, +200ms] - orders up to 50ms before, or 200ms after,
// the click. Default join type is Inner; pass `clink::Dag::JoinType::
// LeftOuter` (and friends) to emit unmatched left rows once the
// watermark advances past their upper-bound deadline.
//
// Pipeline:
//   VectorSource<Click>   ─┐
//                          ├── interval_join(key=user_id, [-50ms, +200ms])
//   VectorSource<Order>   ─┘                 -> Sink<Joined>

#include <chrono>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <clink/operators/sink_operator.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>

struct Click {
    std::string user_id;
    std::string url;
};
struct Order {
    std::string user_id;
    std::string sku;
};
struct Joined {
    std::string user_id;
    std::string url;
    std::string sku;
};

int main() {
    using namespace clink;
    using namespace std::chrono_literals;

    std::vector<Record<Click>> clicks{
        Record<Click>{{"u1", "/a"}, EventTime{100}},
        Record<Click>{{"u2", "/b"}, EventTime{200}},
        Record<Click>{{"u1", "/c"}, EventTime{300}},
    };
    std::vector<Record<Order>> orders{
        Record<Order>{{"u1", "shoe"}, EventTime{120}},
        Record<Order>{{"u1", "hat"},  EventTime{280}},
        Record<Order>{{"u2", "shirt"}, EventTime{500}},  // outside [-50, +200] vs click@200
    };

    Dag dag;
    auto h_clicks = dag.add_source<Click>(
        std::make_shared<VectorSource<Click>>(std::move(clicks), "clicks"));
    auto h_orders = dag.add_source<Order>(
        std::make_shared<VectorSource<Order>>(std::move(orders), "orders"));

    auto h_joined = dag.interval_join<Click, Order, std::string, Joined>(
        h_clicks,
        h_orders,
        [](const Click& c) { return c.user_id; },
        [](const Order& o) { return o.user_id; },
        50ms,    // look-back: order may precede click by up to 50ms
        200ms,   // look-ahead: order may follow click by up to 200ms
        [](const std::optional<Click>& c, const std::optional<Order>& o) {
            return Joined{c->user_id, c->url, o->sku};
        });

    auto sink = std::make_shared<FunctionSink<Joined>>([](const Joined& j) {
        std::cout << j.user_id << " " << j.url << " -> " << j.sku << '\n';
    });
    dag.add_sink<Joined>(h_joined, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();
    return 0;
}
