// Phase 29g: unit tests for the autoscaler loop. The autoscaler is
// callback-pluggable so these tests inject deterministic sample /
// status / request shims and verify decision shape per tick.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/autoscaler.hpp"

namespace clink::cluster {
namespace {

using namespace std::chrono_literals;

// Fixture builds a fake operator world: status_fn pulls from a map
// the test mutates between ticks; request_fn records what was asked
// and either accepts (mutating current_parallelism) or rejects.
struct FakeOp {
    std::uint32_t current{2};
    std::uint32_t min{1};
    std::uint32_t max{8};
    RescaleState state{RescaleState::Idle};
    double sample{0.5};
};

struct Fixture {
    std::unordered_map<std::string, FakeOp> ops;
    std::vector<std::pair<std::string, std::uint32_t>> requests;
    bool accept_requests{true};
    std::string reject_reason;

    Autoscaler::SampleFn sample_fn() {
        return [this](const std::string& op_id) {
            auto it = ops.find(op_id);
            return it == ops.end() ? 0.0 : it->second.sample;
        };
    }
    Autoscaler::StatusFn status_fn() {
        return [this](const std::string& op_id) -> std::optional<OperatorRescaleStatus> {
            auto it = ops.find(op_id);
            if (it == ops.end()) {
                return std::nullopt;
            }
            OperatorRescaleStatus s;
            s.op_id = op_id;
            s.current_parallelism = it->second.current;
            s.min_parallelism = it->second.min;
            s.max_parallelism = it->second.max;
            s.state = it->second.state;
            return s;
        };
    }
    Autoscaler::RequestRescaleFn request_fn() {
        return [this](const std::string& op_id,
                      std::uint32_t new_p) -> RescaleCoordinator::RequestResult {
            requests.emplace_back(op_id, new_p);
            if (!accept_requests) {
                return {.ok = false, .accepted_target = 0, .reason = reject_reason};
            }
            auto it = ops.find(op_id);
            if (it != ops.end()) {
                it->second.current = new_p;  // simulate immediate completion
            }
            return {.ok = true, .accepted_target = new_p, .reason = "ok"};
        };
    }
};

AutoscalerConfig snug_cfg() {
    AutoscalerConfig cfg;
    cfg.sample_period = 100ms;
    cfg.setpoint = 0.7;
    cfg.rescale_threshold = 0.2;
    cfg.cooldown = 0ms;  // disable cooldown for most tests
    cfg.pid.kp = 1.0;
    cfg.pid.ki = 0.0;
    cfg.pid.kd = 0.0;
    cfg.pid.output_min = -1.0;
    cfg.pid.output_max = 1.0;
    return cfg;
}

TEST(Autoscaler, ScalesUpWhenSampleAboveSetpoint) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.95};

    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    auto decs = a.tick(100ms);

    ASSERT_EQ(decs.size(), 1u);
    EXPECT_EQ(decs[0].op_id, "agg");
    EXPECT_DOUBLE_EQ(decs[0].sample, 0.95);
    // setpoint=0.7, measured=0.95 -> error = setpoint - measured < 0;
    // negative output means "scale UP" per the actuator-inversion
    // convention documented in autoscaler.cpp.
    EXPECT_LT(decs[0].pid_output, 0.0);
    EXPECT_TRUE(decs[0].requested);
    EXPECT_TRUE(decs[0].accepted);
    EXPECT_EQ(decs[0].target_parallelism, 3u);
    ASSERT_EQ(f.requests.size(), 1u);
    EXPECT_EQ(f.requests[0].second, 3u);
}

TEST(Autoscaler, ScalesDownWhenSampleBelowSetpoint) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 4, .min = 1, .max = 8, .sample = 0.1};

    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    auto decs = a.tick(100ms);

    ASSERT_EQ(decs.size(), 1u);
    // Under-utilized: positive output -> scale DOWN.
    EXPECT_GT(decs[0].pid_output, 0.0);
    EXPECT_TRUE(decs[0].requested);
    EXPECT_EQ(decs[0].target_parallelism, 3u);
}

TEST(Autoscaler, IdleWithinHysteresisBand) {
    Fixture f;
    // Setpoint 0.7, sample 0.75: |error| = 0.05, |output| = 0.05 with
    // kp=1, well below rescale_threshold=0.2.
    f.ops["agg"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.75};

    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    auto decs = a.tick(100ms);

    ASSERT_EQ(decs.size(), 1u);
    EXPECT_FALSE(decs[0].requested);
    EXPECT_NE(decs[0].reason.find("hysteresis"), std::string::npos);
    EXPECT_TRUE(f.requests.empty());
}

TEST(Autoscaler, NoRescaleAtMaxParallelism) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 8, .min = 1, .max = 8, .sample = 1.0};

    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    auto decs = a.tick(100ms);

    ASSERT_EQ(decs.size(), 1u);
    EXPECT_FALSE(decs[0].requested);
    EXPECT_NE(decs[0].reason.find("max_parallelism"), std::string::npos);
}

TEST(Autoscaler, NoRescaleAtMinParallelism) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 1, .min = 1, .max = 8, .sample = 0.0};

    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    auto decs = a.tick(100ms);

    ASSERT_EQ(decs.size(), 1u);
    EXPECT_FALSE(decs[0].requested);
    EXPECT_NE(decs[0].reason.find("min_parallelism"), std::string::npos);
}

TEST(Autoscaler, SkipsOperatorMidRescale) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.95};
    f.ops["agg"].state = RescaleState::Draining;

    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    auto decs = a.tick(100ms);

    ASSERT_EQ(decs.size(), 1u);
    EXPECT_FALSE(decs[0].requested);
    EXPECT_NE(decs[0].reason.find("rescale in progress"), std::string::npos);
}

TEST(Autoscaler, SkipsOperatorWithoutBounds) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 2, .min = 0, .max = 0, .sample = 0.95};

    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    auto decs = a.tick(100ms);

    ASSERT_EQ(decs.size(), 1u);
    EXPECT_FALSE(decs[0].requested);
    EXPECT_NE(decs[0].reason.find("no autoscale bounds"), std::string::npos);
}

TEST(Autoscaler, UnregisterDropsFromRotation) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.95};
    f.ops["snk"] = FakeOp{.current = 2, .min = 1, .max = 4, .sample = 0.1};

    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    a.register_operator("snk");
    a.unregister_operator("snk");

    auto decs = a.tick(100ms);
    ASSERT_EQ(decs.size(), 1u);
    EXPECT_EQ(decs[0].op_id, "agg");
}

TEST(Autoscaler, CooldownBlocksRapidRequests) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.95};

    auto cfg = snug_cfg();
    cfg.cooldown = std::chrono::hours{1};
    Autoscaler a(cfg, f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");

    auto first = a.tick(100ms);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_TRUE(first[0].requested);

    // Reset state to look like the rescale finished so the bounds /
    // mid-rescale guards don't trip. Sample still high -> the
    // controller would want to scale again, but cooldown blocks.
    f.ops["agg"].state = RescaleState::Complete;
    auto second = a.tick(100ms);
    ASSERT_EQ(second.size(), 1u);
    EXPECT_FALSE(second[0].requested);
    EXPECT_NE(second[0].reason.find("cooldown"), std::string::npos);
}

TEST(Autoscaler, HardRejectResetsControllerState) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.95};
    f.accept_requests = false;
    f.reject_reason = "operator not scalable";

    auto cfg = snug_cfg();
    cfg.pid.ki = 0.5;  // integral builds up if we don't reset
    Autoscaler a(cfg, f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");

    auto first = a.tick(100ms);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_TRUE(first[0].requested);
    EXPECT_FALSE(first[0].accepted);

    // After a reject the controller should be reset; the next tick's
    // pid_output should reflect only the new sample's proportional
    // contribution (no carried integral).
    f.ops["agg"].sample = 0.7;  // exactly at setpoint -> error=0
    auto second = a.tick(100ms);
    ASSERT_EQ(second.size(), 1u);
    EXPECT_NEAR(second[0].pid_output, 0.0, 1e-9);
}

TEST(Autoscaler, MultiOperatorPerTick) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.95};
    f.ops["snk"] = FakeOp{.current = 4, .min = 1, .max = 8, .sample = 0.1};

    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    a.register_operator("snk");

    auto decs = a.tick(100ms);
    ASSERT_EQ(decs.size(), 2u);
    std::unordered_map<std::string, AutoscalerDecision> by_op;
    for (auto& d : decs) {
        by_op[d.op_id] = d;
    }
    ASSERT_TRUE(by_op.contains("agg"));
    ASSERT_TRUE(by_op.contains("snk"));
    EXPECT_TRUE(by_op["agg"].requested);
    EXPECT_EQ(by_op["agg"].target_parallelism, 3u);
    EXPECT_TRUE(by_op["snk"].requested);
    EXPECT_EQ(by_op["snk"].target_parallelism, 3u);
}

TEST(Autoscaler, StatusMissingReportedAsReason) {
    Fixture f;  // empty - no ops registered with the fixture
    Autoscaler a(snug_cfg(), f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("ghost");
    auto decs = a.tick(100ms);
    ASSERT_EQ(decs.size(), 1u);
    EXPECT_FALSE(decs[0].requested);
    EXPECT_NE(decs[0].reason.find("no status"), std::string::npos);
}

TEST(Autoscaler, ResetOperatorClearsIntegralWithoutAffectingOthers) {
    Fixture f;
    f.ops["a"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.95};
    f.ops["b"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.95};

    auto cfg = snug_cfg();
    cfg.pid.ki = 0.5;
    Autoscaler a(cfg, f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("a");
    a.register_operator("b");

    // Accumulate some integral on both. Sample at setpoint after.
    a.tick(100ms);
    a.reset_operator("a");

    // Now both samples at setpoint: 'a' should have zero output
    // (reset), 'b' should still have integral contribution.
    f.ops["a"].sample = 0.7;
    f.ops["b"].sample = 0.7;
    auto decs = a.tick(100ms);
    ASSERT_EQ(decs.size(), 2u);
    std::unordered_map<std::string, double> outputs;
    for (auto& d : decs) {
        outputs[d.op_id] = d.pid_output;
    }
    EXPECT_NEAR(outputs["a"], 0.0, 1e-9);
    EXPECT_NE(outputs["b"], 0.0);
}

TEST(Autoscaler, BackgroundThreadProducesTicks) {
    Fixture f;
    f.ops["agg"] = FakeOp{.current = 2, .min = 1, .max = 8, .sample = 0.5};

    auto cfg = snug_cfg();
    cfg.sample_period = 60ms;
    Autoscaler a(cfg, f.sample_fn(), f.request_fn(), f.status_fn());
    a.register_operator("agg");
    a.start();

    // Wait for >=3 ticks. The sleep loop inside the autoscaler sleeps
    // in 50ms chunks so 300ms is enough for 3-5 ticks.
    const auto deadline = std::chrono::steady_clock::now() + 500ms;
    while (a.ticks() < 3 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(20ms);
    }
    a.stop();
    EXPECT_GE(a.ticks(), 3u);
}

}  // namespace
}  // namespace clink::cluster
