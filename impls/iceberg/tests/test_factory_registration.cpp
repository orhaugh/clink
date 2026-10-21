// Verifies clink::iceberg::install() makes the iceberg_row_sink factory reachable
// through the RunnerRegistry on the "row" channel.

#include <memory>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(IcebergFactoryRegistration, SinkIsRegistered) {
    EXPECT_NE(RunnerRegistry::default_instance().find_sink("iceberg_row_sink", "row"), nullptr);
}

// The sink writes from subtask 0 alone, so it refuses a parallelism above 1:
// a subtask beside the writer used to take its share of the rows and drop it.
TEST(IcebergFactoryRegistration, TheSinkRefusesAParallelismAbove1) {
    const auto* factory = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "iceberg_row_sink", clink::cluster::ChannelType{"row"});
    ASSERT_NE(factory, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"warehouse", "/nonexistent/clink-iceberg-refusal"}, {"table", "t"}};
    ctx.parallelism = 2;
    ctx.subtask_idx = 1;
    try {
        (void)factory->build(ctx);
        ADD_FAILURE() << "built at parallelism 2";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("requires parallelism 1"), std::string::npos)
            << e.what();
    }
}

}  // namespace
