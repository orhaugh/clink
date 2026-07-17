// Operator uid + display_name tests. Validates the // stable-identifier path:
//
//   * Setting set_uid(...) on a stateful operator pins the OperatorId
//     to hash(uid). Two Dags whose stateful operator has the SAME
//     uid (and different name()) recover state correctly.
//
//   * Without uid, the legacy hash(stage<idx>/<op_name>) is used.
//     Renaming an operator under that path silently abandons state -
//     which is exactly the trap uid solves.
//
//   * Duplicate uids within a Dag throw on add_operator.
//
//   * The fluent API DataStream<T>::uid(...) writes through to
//     OperatorSpec.uid, and pipeline rejects
//     duplicates with a precise diagnostic.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/api/pipeline.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

class FixedAddFn final : public KeyedProcessFunction<std::int64_t, std::int64_t, std::int64_t> {
public:
    explicit FixedAddFn(std::int64_t add_amount) : add_(add_amount) {}

    void open(RuntimeContext& ctx) override {
        state_ = std::make_unique<KeyedState<std::int64_t, std::int64_t>>(
            ctx.keyed_state<std::int64_t, std::int64_t>("counter", int64_codec(), int64_codec()));
    }
    void process_element(const std::int64_t& /*v*/,
                         ProcessFunctionContext<std::int64_t>& /*ctx*/,
                         Collector<std::int64_t>& out) override {
        const auto prev = state_->get(current_key()).value_or(0);
        const auto next = prev + add_;
        state_->put(current_key(), next);
        out.collect(next);
    }

private:
    std::int64_t add_;
    std::unique_ptr<KeyedState<std::int64_t, std::int64_t>> state_;
};

using FixedAdapter =
    ::clink::detail::KeyedProcessFunctionAdapter<std::int64_t, std::int64_t, std::int64_t>;

std::shared_ptr<FixedAdapter> make_fixed_adapter(std::int64_t add_amount) {
    auto fn = std::make_shared<FixedAddFn>(add_amount);
    return std::make_shared<FixedAdapter>(fn, [](const std::int64_t& v) { return v; });
}

}  // namespace

TEST(OperatorUid, UidPinsOperatorIdAcrossRebuild) {
    auto backend = std::make_shared<InMemoryStateBackend>();

    // Build Dag, op has uid="fixed-add". Run input {1, 2},
    // each becomes prev+1 (so key=1 -> 1, key=2 -> 1).
    {
        Dag dag;
        auto src = std::make_shared<VectorSource<std::int64_t>>(
            std::vector<Record<std::int64_t>>{Record<std::int64_t>{1}, Record<std::int64_t>{2}});
        auto h_src = dag.add_source<std::int64_t>(src);
        auto op = make_fixed_adapter(1);
        op->set_uid("fixed-add");
        auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_src, op);
        auto sink = std::make_shared<CollectingSink<std::int64_t>>();
        dag.add_sink<std::int64_t>(h_op, sink);

        JobConfig cfg;
        cfg.state_backend = backend;
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();
        EXPECT_EQ(sink->collected(), (std::vector<std::int64_t>{1, 1}));
    }

    // Rebuild a DIFFERENT Dag - one extra map() upstream so
    // the stage_idx for the counter shifts (which would invalidate
    // the legacy hash-derived OperatorId). Same uid → same
    // OperatorId → state recovers.
    {
        Dag dag;
        auto src = std::make_shared<VectorSource<std::int64_t>>(
            std::vector<Record<std::int64_t>>{Record<std::int64_t>{1}, Record<std::int64_t>{2}});
        auto h_src = dag.add_source<std::int64_t>(src);

        // Insert an upstream identity map - shifts the counter op's
        // stage index. Under the legacy hash, this changes the id.
        auto identity = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
            [](std::int64_t v) { return v; });
        auto h_id = dag.add_operator<std::int64_t, std::int64_t>(h_src, identity);

        auto op = make_fixed_adapter(1);
        op->set_uid("fixed-add");  // same uid as the first build
        auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_id, op);
        auto sink = std::make_shared<CollectingSink<std::int64_t>>();
        dag.add_sink<std::int64_t>(h_op, sink);

        JobConfig cfg;
        cfg.state_backend = backend;
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();

        // State restored - each key's counter is now prev (1) + 1 = 2.
        EXPECT_EQ(sink->collected(), (std::vector<std::int64_t>{2, 2}))
            << "uid should have pinned the OperatorId across the topology change";
    }
}

TEST(OperatorUid, WithoutUidStateIsLostAcrossRebuildWithExtraOp) {
    // Same scenario as the preceding test but WITHOUT setting uid.
    // The legacy hash(stage<idx>/op_name) sees a different stage idx
    // after we insert the upstream map(), so the counter's OperatorId
    // changes and the old state isn't reachable.
    auto backend = std::make_shared<InMemoryStateBackend>();

    {
        Dag dag;
        auto src = std::make_shared<VectorSource<std::int64_t>>(
            std::vector<Record<std::int64_t>>{Record<std::int64_t>{1}, Record<std::int64_t>{2}});
        auto h_src = dag.add_source<std::int64_t>(src);
        auto op = make_fixed_adapter(1);  // no set_uid
        auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_src, op);
        auto sink = std::make_shared<CollectingSink<std::int64_t>>();
        dag.add_sink<std::int64_t>(h_op, sink);

        JobConfig cfg;
        cfg.state_backend = backend;
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();
        EXPECT_EQ(sink->collected(), (std::vector<std::int64_t>{1, 1}));
    }

    {
        Dag dag;
        auto src = std::make_shared<VectorSource<std::int64_t>>(
            std::vector<Record<std::int64_t>>{Record<std::int64_t>{1}, Record<std::int64_t>{2}});
        auto h_src = dag.add_source<std::int64_t>(src);
        auto identity = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
            [](std::int64_t v) { return v; });
        auto h_id = dag.add_operator<std::int64_t, std::int64_t>(h_src, identity);
        auto op = make_fixed_adapter(1);  // still no uid
        auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_id, op);
        auto sink = std::make_shared<CollectingSink<std::int64_t>>();
        dag.add_sink<std::int64_t>(h_op, sink);

        JobConfig cfg;
        cfg.state_backend = backend;
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();

        // Without uid, the operator's OperatorId differs across runs
        // (different stage_idx) so prior state is not restored. Each
        // key starts at 0, +1 = 1.
        EXPECT_EQ(sink->collected(), (std::vector<std::int64_t>{1, 1}))
            << "without uid, the legacy hash changed -> state was effectively orphaned";
    }
}

TEST(OperatorUid, DuplicateUidInSameDagThrows) {
    Dag dag;
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{});
    auto h_src = dag.add_source<std::int64_t>(src);

    auto op1 = make_fixed_adapter(1);
    op1->set_uid("dup");
    auto add_op1 = [&] { (void)dag.add_operator<std::int64_t, std::int64_t>(h_src, op1); };
    EXPECT_NO_THROW(add_op1());

    auto op2 = make_fixed_adapter(1);
    op2->set_uid("dup");
    auto add_op2 = [&] { (void)dag.add_operator<std::int64_t, std::int64_t>(h_src, op2); };
    EXPECT_THROW(add_op2(), std::runtime_error);
}

TEST(OperatorUid, FluentApiWritesOperatorSpecUid) {
    using namespace clink::api;
    auto env = Pipeline::create();
    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    auto mapped = src.map<std::int64_t>([](const std::int64_t& v) { return v * 2; })
                      .name("multiply-by-2")
                      .uid("multiply-uid");

    // Find the multiply op in the graph and verify its uid + display_name.
    const auto& graph = env.graph();
    bool found = false;
    for (const auto& op : graph.ops) {
        if (op.uid == "multiply-uid") {
            EXPECT_EQ(op.display_name, "multiply-by-2");
            found = true;
        }
    }
    EXPECT_TRUE(found) << "the fluent .uid() did not land on any OperatorSpec";
}

TEST(OperatorUid, FluentApiRejectsDuplicateUid) {
    using namespace clink::api;
    auto env = Pipeline::create();
    auto src = env.from_elements<std::int64_t>({1});
    auto a = src.map<std::int64_t>([](const std::int64_t& v) { return v; }).uid("shared");
    EXPECT_THROW(a.map<std::int64_t>([](const std::int64_t& v) { return v; }).uid("shared"),
                 std::runtime_error);
}
