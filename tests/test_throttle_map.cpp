// ThrottleMap operator tests. Two contracts:
//   1. Zero `sleep_per_record` is a no-cost passthrough - measured by
//      observing the wallclock delta between source emit and sink
//      receipt and asserting it's at or below a small budget.
//   2. Non-zero `sleep_per_record` actually sleeps - the delta climbs
//      with batch size in a tight predictable bound (per-record sleep
//      time times the number of records, minus jitter).
//
// We don't assert exact timings (CI VMs are too noisy); we just check
// ranges. The point is to prove the rate-limit path runs, not to pin
// scheduler accuracy.

#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/throttle_map.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

class IntSource final : public Source<std::int64_t> {
public:
    explicit IntSource(std::vector<std::int64_t> v) : items_(std::move(v)) {}
    bool produce(Emitter<std::int64_t>& out) override {
        if (emitted_) {
            return false;
        }
        Batch<std::int64_t> b;
        for (auto x : items_) {
            b.emplace(x);
        }
        out.emit_data(std::move(b));
        emitted_ = true;
        return false;
    }
    std::string name() const override { return "int_source"; }

private:
    std::vector<std::int64_t> items_;
    bool emitted_{false};
};

class IntCollectingSink final : public Sink<std::int64_t> {
public:
    void on_data(const Batch<std::int64_t>& batch) override {
        for (const auto& r : batch) {
            received_.push_back(r.value());
        }
    }
    std::string name() const override { return "int_collecting_sink"; }
    std::vector<std::int64_t> received_;
};

}  // namespace

TEST(ThrottleMap, ZeroRateIsPassThrough) {
    Dag dag;
    auto src = std::make_shared<IntSource>(std::vector<std::int64_t>{1, 2, 3, 4, 5});
    auto throttle = std::make_shared<ThrottleMap<std::int64_t>>(0ms, "worker");
    auto sink = std::make_shared<IntCollectingSink>();
    auto h_src = dag.add_source<std::int64_t>(src);
    auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_src, throttle);
    dag.add_sink<std::int64_t>(h_op, sink);

    const auto start = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(sink->received_, (std::vector<std::int64_t>{1, 2, 3, 4, 5}));
    EXPECT_LT(elapsed, 500ms) << "zero-rate throttle shouldn't add measurable wallclock delay";
}

TEST(ThrottleMap, NonZeroRateActuallySleeps) {
    Dag dag;
    auto src = std::make_shared<IntSource>(std::vector<std::int64_t>{1, 2, 3});  // 3 records
    auto throttle = std::make_shared<ThrottleMap<std::int64_t>>(40ms, "worker");
    auto sink = std::make_shared<IntCollectingSink>();
    auto h_src = dag.add_source<std::int64_t>(src);
    auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_src, throttle);
    dag.add_sink<std::int64_t>(h_op, sink);

    const auto start = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(sink->received_, (std::vector<std::int64_t>{1, 2, 3}));
    // 3 records * 40 ms = 120 ms expected sleep. Allow generous slack
    // for executor startup and scheduler jitter on busy CI.
    EXPECT_GE(elapsed, 100ms) << "throttle should have actually slept";
    EXPECT_LT(elapsed, 1500ms)
        << "throttle slept way longer than expected (jitter unexpectedly high)";
}
