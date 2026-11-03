// Verifies clink::webhdfs::install() registers the Parquet sink + source factories on each
// channel.

#include <memory>

#include <gtest/gtest.h>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(WebHdfsFactoryRegistration, ParquetSinkAndSourceAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("webhdfs_parquet_int64_sink", "int64"), nullptr);
    EXPECT_NE(rr.find_sink("webhdfs_parquet_string_sink", "string"), nullptr);
    EXPECT_NE(rr.find_source("webhdfs_parquet_int64_source", "int64"), nullptr);
    EXPECT_NE(rr.find_source("webhdfs_parquet_string_source", "string"), nullptr);
}

// The SQL planner emits <connector>_2pc_string_sink for delivery_guarantee='exactly_once'
// (src/sql/physical_plan.cpp); a name it emits that nothing registers fails only at deploy.
TEST(WebHdfsFactoryRegistration, ExactlyOnceParquetSinksThePlannerNamesAreRegistered) {
    const auto& rr = clink::cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("webhdfs_parquet_2pc_int64_sink", "int64"), nullptr);
    EXPECT_NE(rr.find_sink("webhdfs_parquet_2pc_string_sink", "string"), nullptr);
}

// A SQL table with declared columns reads and writes typed Parquet on the Row channel, one
// Parquet column per declared column; these are the names the planner emits for it. Each builds
// from the declared columns without reaching the store, and the exactly-once sink resolves to
// the record that declares exactly-once, so the guarantee gate admits a job through it.
TEST(WebHdfsFactoryRegistration, RowChannelParquetFactoriesBuildFromTheDeclaredColumns) {
    const auto& reg = clink::cluster::OperatorRegistry::default_instance();
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"base_url", "http://127.0.0.1:1"},
                  {"path", "/p"},
                  {"prefix", "/p"},
                  {"schema_columns", "id:i64;name:str;score:f64;ok:bool"}};

    const auto* source = reg.find_source("webhdfs_parquet_row_source", "row");
    ASSERT_NE(source, nullptr);
    EXPECT_NE(source->build(ctx), nullptr);
    const auto* sink = reg.find_sink("webhdfs_parquet_row_sink", "row");
    ASSERT_NE(sink, nullptr);
    EXPECT_NE(sink->build(ctx), nullptr);
    const auto* two_pc = reg.find_sink("webhdfs_parquet_2pc_row_sink", "row");
    ASSERT_NE(two_pc, nullptr);
    const auto staged = std::static_pointer_cast<clink::Sink<clink::sql::Row>>(two_pc->build(ctx));
    ASSERT_NE(staged, nullptr);
    EXPECT_TRUE(staged->stages_state_at_barrier());

    EXPECT_EQ(clink::cluster::connector_name_for_op_type("webhdfs_parquet_row_sink"),
              "webhdfs_parquet");
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("webhdfs_parquet_2pc_row_sink"),
              "webhdfs_parquet");
}

}  // namespace
