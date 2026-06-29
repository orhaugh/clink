#include <cstring>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

std::string to_string(const StateBackend::Value& v) {
    std::string out(v.size(), '\0');
    if (!v.empty()) {
        std::memcpy(out.data(), v.data(), v.size());
    }
    return out;
}

}  // namespace

TEST(StateRecovery, RestoreFromSnapshotRunsBeforeOperators) {
    // Write some keyed state into a backend, then snapshot it.
    auto original = std::make_shared<InMemoryStateBackend>();
    OperatorId op{7};
    original->put(op, sv(std::string{"counter"}), sv(std::string{"100"}));
    original->put(op, sv(std::string{"label"}), sv(std::string{"alpha"}));
    auto snap = original->snapshot(CheckpointId{42});

    // Build a fresh backend, run a no-op job that asks the runtime
    // to restore from the snapshot before starting.
    auto fresh = std::make_shared<InMemoryStateBackend>();
    EXPECT_FALSE(fresh->get(op, sv(std::string{"counter"})).has_value());

    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{Record<int>{1}});
    auto m = std::make_shared<MapOperator<int, int>>([](int x) { return x; });
    auto sink = std::make_shared<CollectingSink<int>>();
    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, m);
    dag.add_sink<int>(h1, sink);

    JobConfig cfg;
    cfg.state_backend = fresh;
    cfg.restore_from = std::move(snap);

    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // The fresh backend should now contain the restored state.
    auto v1 = fresh->get(op, sv(std::string{"counter"}));
    auto v2 = fresh->get(op, sv(std::string{"label"}));
    ASSERT_TRUE(v1.has_value());
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(to_string(*v1), "100");
    EXPECT_EQ(to_string(*v2), "alpha");

    // The pipeline still ran end-to-end.
    EXPECT_EQ(sink->collected(), (std::vector<int>{1}));
}

TEST(StateRecovery, NoRestoreWhenSnapshotAbsent) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    JobConfig cfg;
    cfg.state_backend = backend;
    // No restore_from: backend should remain untouched.

    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{});
    auto sink = std::make_shared<CollectingSink<int>>();
    auto h0 = dag.add_source<int>(src);
    dag.add_sink<int>(h0, sink);

    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // We didn't write anything, didn't restore - backend stays empty.
    EXPECT_FALSE(backend->get(OperatorId{1}, sv(std::string{"x"})).has_value());
}
