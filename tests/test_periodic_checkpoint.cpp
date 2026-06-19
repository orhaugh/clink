#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_coordinator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// A source that emits one record per produce() call until cancelled. Used to
// keep the DAG alive long enough for the periodic trigger to fire.
class HeartbeatSource final : public Source<int> {
public:
    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        Batch<int> b;
        b.emplace(counter_.fetch_add(1));
        out.emit_data(std::move(b));
        std::this_thread::sleep_for(5ms);
        return true;
    }

    std::string name() const override { return "heartbeat"; }

private:
    std::atomic<int> counter_{0};
};

// Counts barriers it sees and remembers each id.
class BarrierCountingSink final : public Sink<int> {
public:
    void on_data(const Batch<int>& /*batch*/) override {}

    void on_barrier(CheckpointBarrier b) override {
        std::lock_guard lock(mu_);
        ids_.push_back(b.id());
    }

    std::vector<CheckpointId> seen_ids() const {
        std::lock_guard lock(mu_);
        return ids_;
    }

private:
    mutable std::mutex mu_;
    std::vector<CheckpointId> ids_;
};

}  // namespace

TEST(PeriodicCheckpoint, TriggerInjectsBarriersIntoLiveSource) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    CheckpointCoordinator::Config cfg;
    cfg.interval = 30ms;
    CheckpointCoordinator coord(backend, cfg);

    Dag dag;
    auto src = std::make_shared<HeartbeatSource>();
    auto map = std::make_shared<MapOperator<int, int>>([](int x) { return x; });
    auto sink = std::make_shared<BarrierCountingSink>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, map);
    dag.add_sink<int>(h1, sink);

    // Register every operator in the DAG (so coord.acknowledge bookkeeping
    // is consistent).
    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());

    LocalExecutor exec(std::move(dag));
    exec.start();
    coord.start_periodic_trigger();

    std::this_thread::sleep_for(150ms);

    coord.stop_periodic_trigger();
    src->cancel();
    exec.cancel();
    exec.await_termination();

    auto ids = sink->seen_ids();
    EXPECT_GE(ids.size(), 2u);
    // Ids should be strictly increasing (no duplicates, no regressions).
    for (std::size_t i = 1; i < ids.size(); ++i) {
        EXPECT_LT(ids[i - 1], ids[i]);
    }
}
