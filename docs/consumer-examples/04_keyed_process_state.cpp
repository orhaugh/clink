// 04 - Keyed process function with persistent keyed state.
//
// A running per-key counter. Each input event increments the counter for
// its key and emits the new value. State is held in a `KeyedState<K, V>`
// slot that the runtime backs with an InMemoryStateBackend; swap it for
// `clink::rocksdb` (linked via clink::rocksdb) to make the same code
// durable.
//
// Pipeline:
//   VectorSource<int64>
//     -> KeyedProcess<key=value>(counter += 1; emit running total)
//     -> Sink

#include <cstdint>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <clink/core/codec.hpp>
#include <clink/operators/process_function.hpp>
#include <clink/operators/sink_operator.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>
#include <clink/state/in_memory_state_backend.hpp>
#include <clink/state/keyed_state.hpp>

class RunningTotal final
    : public clink::KeyedProcessFunction<std::int64_t, std::int64_t, std::int64_t> {
public:
    void open(clink::RuntimeContext& ctx) override {
        state_ = std::make_unique<clink::KeyedState<std::int64_t, std::int64_t>>(
            ctx.keyed_state<std::int64_t, std::int64_t>(
                "running_total", clink::int64_codec(), clink::int64_codec()));
    }

    void process_element(const std::int64_t& v,
                         clink::ProcessFunctionContext<std::int64_t>& /*ctx*/,
                         clink::Collector<std::int64_t>& out) override {
        const auto prev = state_->get(current_key()).value_or(0);
        const auto next = prev + v;
        state_->put(current_key(), next);
        out.collect(next);
    }

private:
    std::unique_ptr<clink::KeyedState<std::int64_t, std::int64_t>> state_;
};

int main() {
    using namespace clink;

    Dag dag;
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{
        Record<std::int64_t>{1}, Record<std::int64_t>{1}, Record<std::int64_t>{2},
        Record<std::int64_t>{1}, Record<std::int64_t>{2},
    });

    auto fn      = std::make_shared<RunningTotal>();
    auto adapter =
        std::make_shared<detail::KeyedProcessFunctionAdapter<std::int64_t, std::int64_t, std::int64_t>>(
            fn, [](const std::int64_t& v) { return v; });
    auto sink = std::make_shared<FunctionSink<std::int64_t>>(
        [](const std::int64_t& v) { std::cout << "running total = " << v << '\n'; });

    auto s0 = dag.add_source<std::int64_t>(src);
    auto s1 = dag.add_operator<std::int64_t, std::int64_t>(s0, adapter);
    dag.add_sink<std::int64_t>(s1, sink);

    // Any operator that calls ctx.keyed_state() needs a backend on the
    // JobConfig. InMemoryStateBackend is fine for examples and tests;
    // RocksDbStateBackend is durable.
    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();

    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return 0;
}
