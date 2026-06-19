// Cluster orchestration metric helpers + RescaleCoordinator
// state-transition emission.

#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/rescale_coordinator.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/orchestration_metrics.hpp"

using namespace clink;

namespace {

std::uint64_t counter_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

}  // namespace

TEST(OrchestrationMetrics, RescaleStateTransitionNameShape) {
    const auto n = clink::metrics::rescale_state_transition_name("Idle", "Preparing");
    EXPECT_NE(n.find("from=\"Idle\""), std::string::npos);
    EXPECT_NE(n.find("to=\"Preparing\""), std::string::npos);
}

TEST(OrchestrationMetrics, RescaleCoordinatorEmitsAcceptedAndTransitions) {
    using namespace clink::cluster;
    using namespace clink::metrics;

    RescaleCoordinator c;
    c.register_operator("agg", /*current=*/2, /*min=*/1, /*max=*/8);

    const auto accept_before = counter_value(rescale_request_name("accepted"));
    const auto trans_before = counter_value(rescale_state_transition_name("Idle", "Preparing"));

    auto r = c.request_rescale("agg", 4);
    ASSERT_TRUE(r.ok);

    EXPECT_EQ(counter_value(rescale_request_name("accepted")) - accept_before, 1u);
    EXPECT_EQ(counter_value(rescale_state_transition_name("Idle", "Preparing")) - trans_before, 1u);
}

TEST(OrchestrationMetrics, RescaleCoordinatorEmitsRejectOnUnknownOp) {
    using namespace clink::cluster;
    using namespace clink::metrics;
    RescaleCoordinator c;
    const auto before = counter_value(rescale_request_name("rejected"));
    auto r = c.request_rescale("nope", 4);
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(counter_value(rescale_request_name("rejected")) - before, 1u);
}

TEST(OrchestrationMetrics, RescaleCoordinatorEmitsAbortAndTransition) {
    using namespace clink::cluster;
    using namespace clink::metrics;
    RescaleCoordinator c;
    c.register_operator("agg", 2, 1, 8);
    (void)c.request_rescale("agg", 4);  // Idle -> Preparing

    const auto trans_before = counter_value(rescale_state_transition_name("Preparing", "Aborted"));
    const auto abort_before = counter_value(kRescaleAborts);

    ASSERT_TRUE(c.abort("agg", "test"));

    EXPECT_EQ(counter_value(rescale_state_transition_name("Preparing", "Aborted")) - trans_before,
              1u);
    EXPECT_EQ(counter_value(kRescaleAborts) - abort_before, 1u);
}

TEST(OrchestrationMetrics, HaAndAutoscalerHelpers) {
    using namespace clink::metrics;

    const auto take_before = counter_value(kHaLeaderTakeovers);
    const auto rec_before = counter_value(kHaRecoveredJobs);
    const auto tick_before = counter_value(kAutoscalerTicks);
    const auto cut_before = counter_value(kRescaleCutoverDeploys);

    orch::ha_leader_takeover();
    orch::ha_recovered_jobs_inc(3);
    orch::autoscaler_tick();
    orch::rescale_cutover_deploy();
    orch::autoscaler_decision("accepted");
    orch::autoscaler_decision("cooldown");

    EXPECT_EQ(counter_value(kHaLeaderTakeovers) - take_before, 1u);
    EXPECT_EQ(counter_value(kHaRecoveredJobs) - rec_before, 3u);
    EXPECT_EQ(counter_value(kAutoscalerTicks) - tick_before, 1u);
    EXPECT_EQ(counter_value(kRescaleCutoverDeploys) - cut_before, 1u);
    EXPECT_GE(counter_value(autoscaler_decision_name("accepted")), 1u);
    EXPECT_GE(counter_value(autoscaler_decision_name("cooldown")), 1u);
}
