// State Processor API - Savepoint reader/writer tests. Exercises:
//
//   * Empty savepoint -> write entries -> snapshot -> load -> read back
//   * File round-trip (write_to_file + load_from_file) preserves state
//   * operators() / slots() discover what's in a savepoint
//   * Round-trip from a real LocalExecutor job: snapshot a running job's
//     backend, open as a Savepoint, read keyed state offline
//   * Restore a fresh job from a Savepoint built offline

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state_processor/savepoint.hpp"

using namespace clink;
using namespace clink::state_processor;

TEST(StateProcessor, CreateWriteReadInMemoryRoundTrip) {
    auto sp = Savepoint::create();
    auto ks = sp.keyed_state<std::string, std::int64_t>(
        OperatorId{42}, "counts", string_codec(), int64_codec());
    ks.put("alpha", 1);
    ks.put("beta", 22);
    ks.put("gamma", 333);

    // Round-trip via the in-memory Snapshot blob: take the bytes,
    // load a fresh Savepoint from them, verify all KVs visible.
    auto snap = sp.snapshot(CheckpointId{7});
    EXPECT_EQ(snap.checkpoint_id.value(), 7u);
    EXPECT_FALSE(snap.bytes.empty());

    auto reopened = Savepoint::load_from_snapshot(std::move(snap));
    auto ks2 = reopened.keyed_state<std::string, std::int64_t>(
        OperatorId{42}, "counts", string_codec(), int64_codec());
    EXPECT_EQ(ks2.get("alpha"), std::optional<std::int64_t>{1});
    EXPECT_EQ(ks2.get("beta"), std::optional<std::int64_t>{22});
    EXPECT_EQ(ks2.get("gamma"), std::optional<std::int64_t>{333});
    EXPECT_FALSE(ks2.get("missing").has_value());
}

TEST(StateProcessor, FileRoundTripPreservesState) {
    const auto tmp = std::filesystem::temp_directory_path() / "clink_state_processor_test.snap";
    std::filesystem::remove(tmp);

    {
        auto sp = Savepoint::create();
        auto ks = sp.keyed_state<std::int64_t, std::string>(
            OperatorId{99}, "labels", int64_codec(), string_codec());
        ks.put(1, "one");
        ks.put(2, "two");
        ks.put(3, "three");
        sp.write_to_file(tmp);
    }

    auto sp = Savepoint::load_from_file(tmp);
    auto ks = sp.keyed_state<std::int64_t, std::string>(
        OperatorId{99}, "labels", int64_codec(), string_codec());
    EXPECT_EQ(ks.get(1), std::optional<std::string>{"one"});
    EXPECT_EQ(ks.get(2), std::optional<std::string>{"two"});
    EXPECT_EQ(ks.get(3), std::optional<std::string>{"three"});

    std::filesystem::remove(tmp);
}

TEST(StateProcessor, OperatorsAndSlotsListing) {
    auto sp = Savepoint::create();
    sp.keyed_state<std::string, std::int64_t>(OperatorId{1}, "users", string_codec(), int64_codec())
        .put("a", 100);
    sp.keyed_state<std::string, std::int64_t>(
          OperatorId{1}, "sessions", string_codec(), int64_codec())
        .put("b", 200);
    sp.keyed_state<std::string, std::int64_t>(OperatorId{2}, "users", string_codec(), int64_codec())
        .put("c", 300);

    auto ops = sp.operators();
    std::vector<std::uint64_t> op_values;
    for (auto o : ops) {
        op_values.push_back(o.value());
    }
    std::sort(op_values.begin(), op_values.end());
    EXPECT_EQ(op_values, (std::vector<std::uint64_t>{1, 2}));

    auto slots1 = sp.slots(OperatorId{1});
    std::sort(slots1.begin(), slots1.end());
    EXPECT_EQ(slots1, (std::vector<std::string>{"sessions", "users"}));

    auto slots2 = sp.slots(OperatorId{2});
    EXPECT_EQ(slots2, (std::vector<std::string>{"users"}));
}

// KeyedProcessFunction that maintains a per-key counter so the test can
// snapshot the underlying backend mid-flight and inspect it via the
// State Processor API.
class CounterFn final : public KeyedProcessFunction<std::int64_t, std::int64_t, std::int64_t> {
public:
    void open(RuntimeContext& ctx) override {
        state_ = std::make_unique<KeyedState<std::int64_t, std::int64_t>>(
            ctx.keyed_state<std::int64_t, std::int64_t>("counter", int64_codec(), int64_codec()));
    }
    void process_element(const std::int64_t& v,
                         ProcessFunctionContext<std::int64_t>& /*ctx*/,
                         Collector<std::int64_t>& out) override {
        const auto prev = state_->get(current_key()).value_or(0);
        const auto next = prev + v;
        state_->put(current_key(), next);
        out.collect(next);
    }

private:
    std::unique_ptr<KeyedState<std::int64_t, std::int64_t>> state_;
};

TEST(StateProcessor, ReadStateFromRealJobSnapshot) {
    auto backend = std::make_shared<InMemoryStateBackend>();

    Dag dag;
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{
        Record<std::int64_t>{1},
        Record<std::int64_t>{1},
        Record<std::int64_t>{2},
        Record<std::int64_t>{1},
        Record<std::int64_t>{2},
    });
    auto h_src = dag.add_source<std::int64_t>(src);

    auto fn = std::make_shared<CounterFn>();
    auto adapter = std::make_shared<
        ::clink::detail::KeyedProcessFunctionAdapter<std::int64_t, std::int64_t, std::int64_t>>(
        fn, [](const std::int64_t& v) { return v; });
    auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_src, adapter);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // Sanity check: pipeline output is the running totals - 1, 2 for
    // key=1; 2, 4 for key=2; 3 for key=1 again.
    EXPECT_EQ(sink->collected().size(), 5u);

    // Snapshot the live backend and open it as a Savepoint.
    auto sp = Savepoint::load_from_snapshot(backend->snapshot(CheckpointId{1}));
    // The operator id used by the adapter - by default the runner
    // assigns ids in the order ops were added. Source = 1, operator =
    // 2, sink = 3 in this Dag. We verify by checking which operator
    // has the "counter" slot.
    auto ops = sp.operators();
    OperatorId counter_op{0};
    for (auto o : ops) {
        auto slots = sp.slots(o);
        for (const auto& s : slots) {
            if (s == "counter") {
                counter_op = o;
                break;
            }
        }
        if (counter_op.value() != 0) {
            break;
        }
    }
    ASSERT_NE(counter_op.value(), 0u) << "counter slot not found in any operator";

    auto ks = sp.keyed_state<std::int64_t, std::int64_t>(
        counter_op, "counter", int64_codec(), int64_codec());
    EXPECT_EQ(ks.get(1), std::optional<std::int64_t>{3});  // 1 + 1 + 1
    EXPECT_EQ(ks.get(2), std::optional<std::int64_t>{4});  // 2 + 2
}

// Helper that constructs the exact same Dag shape twice - once to
// discover the operator id (by running a no-op job and inspecting the
// resulting backend), and once to actually run the seeded job. Both
// invocations use the same op name and the same stage layout, so the
// Dag's derive_id hash is identical across them.
namespace {

template <typename SetupDag>
auto build_counter_dag(SetupDag setup) {
    Dag dag;
    auto src = setup(dag);
    auto fn = std::make_shared<CounterFn>();
    auto adapter = std::make_shared<
        ::clink::detail::KeyedProcessFunctionAdapter<std::int64_t, std::int64_t, std::int64_t>>(
        fn, [](const std::int64_t& v) { return v; });
    auto h_op = dag.add_operator<std::int64_t, std::int64_t>(src, adapter);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_op, sink);
    return std::make_tuple(std::move(dag), sink);
}

}  // namespace

TEST(StateProcessor, SeedFreshJobFromOfflineSavepoint) {
    // The Dag assigns op ids by hashing "stage<idx>/<op_name>". To
    // seed an offline savepoint at the right OperatorId, we run a
    // no-op pass first to discover the id - this matches the
    // realistic State Processor use case ("take a prior savepoint,
    // mutate, restore") rather than depending on internal hash math.
    OperatorId counter_op{0};
    {
        auto probe_backend = std::make_shared<InMemoryStateBackend>();
        auto [dag, sink] = build_counter_dag([](Dag& d) {
            auto src = std::make_shared<VectorSource<std::int64_t>>(
                std::vector<Record<std::int64_t>>{Record<std::int64_t>{1}});
            return d.add_source<std::int64_t>(src);
        });
        (void)sink;
        JobConfig cfg;
        cfg.state_backend = probe_backend;
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();

        auto sp = Savepoint::load_from_snapshot(probe_backend->snapshot(CheckpointId{1}));
        for (auto o : sp.operators()) {
            for (const auto& s : sp.slots(o)) {
                if (s == "counter") {
                    counter_op = o;
                    break;
                }
            }
            if (counter_op.value() != 0) {
                break;
            }
        }
    }
    ASSERT_NE(counter_op.value(), 0u) << "could not discover counter op id";

    // Build a Savepoint seeding (1 -> 1000, 2 -> 2000) under the
    // discovered op id.
    auto sp = Savepoint::create();
    auto ks = sp.keyed_state<std::int64_t, std::int64_t>(
        counter_op, "counter", int64_codec(), int64_codec());
    ks.put(1, 1000);
    ks.put(2, 2000);

    // Start a fresh job with restore_from set to that snapshot.
    auto fresh_backend = std::make_shared<InMemoryStateBackend>();
    auto [dag, sink] = build_counter_dag([](Dag& d) {
        auto src = std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{
            Record<std::int64_t>{1},
            Record<std::int64_t>{2},
        });
        return d.add_source<std::int64_t>(src);
    });

    JobConfig cfg;
    cfg.state_backend = fresh_backend;
    cfg.restore_from = sp.snapshot(CheckpointId{1});
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    auto collected = sink->collected();
    std::sort(collected.begin(), collected.end());
    EXPECT_EQ(collected, (std::vector<std::int64_t>{1001, 2002}));
}
