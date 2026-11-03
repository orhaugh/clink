// Verifies clink::s3::install() makes the s3_text_sink factory
// reachable through the RunnerRegistry.

#include <memory>

#include <gtest/gtest.h>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(S3FactoryRegistration, S3TextSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("s3_text_sink", "string"), nullptr);
}

TEST(S3FactoryRegistration, S3ExactlyOnceSinkIsRegistered) {
    // The multipart-complete-on-commit 2PC sink selected by exactly_once.
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("s3_2pc_string_sink", "string"), nullptr);
}

// The SQL planner emits <connector>_2pc_string_sink for delivery_guarantee='exactly_once'
// (src/sql/physical_plan.cpp); a name it emits that nothing registers fails only at deploy.
TEST(S3FactoryRegistration, ExactlyOnceParquetSinksThePlannerNamesAreRegistered) {
    const auto& rr = clink::cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("s3_parquet_2pc_int64_sink", "int64"), nullptr);
    EXPECT_NE(rr.find_sink("s3_parquet_2pc_string_sink", "string"), nullptr);
}

// The Parquet sinks over S3 resolve to records of their own. Their op types
// begin with "s3", so they used to resolve to the at-most-once text sink's
// record: the rolling sink was reported at-most-once, and a job asking for
// exactly-once through the 2PC sink was refused at submission.
TEST(S3FactoryRegistration, TheParquetSinksResolveToTheirOwnRecords) {
    // A job asking for exactly-once through the 2PC sink is accepted.
    clink::cluster::JobGraphSpec g;
    clink::cluster::OperatorSpec src;
    src.id = "src";
    src.type = "file_line_source";
    clink::cluster::OperatorSpec snk;
    snk.id = "snk";
    snk.type = "s3_parquet_2pc_string_sink";
    snk.inputs = {"src"};
    snk.params = {{"bucket", "b"}, {"prefix", "p"}, {"delivery_guarantee", "exactly_once"}};
    g.ops = {src, snk};
    clink::cluster::CheckpointConfig c;
    c.checkpoint_dir = "/tmp/ckpt";
    c.interval_ms = 1000;
    c.state_backend_uri = "file:///tmp/state";
    const auto reject = clink::cluster::check_delivery_guarantee(g, c, nullptr);
    EXPECT_TRUE(reject.empty()) << reject;

    using clink::cluster::connector_name_for_op_type;
    EXPECT_EQ(connector_name_for_op_type("s3_text_sink"), "s3");
    EXPECT_EQ(connector_name_for_op_type("s3_2pc_string_sink"), "s3_2pc");
    EXPECT_EQ(connector_name_for_op_type("s3_parquet_string_sink"), "s3_parquet");
    EXPECT_EQ(connector_name_for_op_type("s3_parquet_2pc_string_sink"), "s3_parquet_2pc");
    const auto& caps = clink::connectors::CapabilityRegistry::instance();
    ASSERT_NE(caps.find("s3_parquet"), nullptr);
    EXPECT_EQ(caps.find("s3_parquet")->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    ASSERT_NE(caps.find("s3_parquet_2pc"), nullptr);
    EXPECT_EQ(caps.find("s3_parquet_2pc")->delivery,
              clink::connectors::DeliveryGuarantee::ExactlyOnceAtomicPublish);
}

// A SQL table with declared columns reads and writes typed Parquet on the Row channel, one
// Parquet column per declared column; these are the names the planner emits for it. Each builds
// from the declared columns without reaching the store, and the exactly-once sink resolves to
// the record that declares exactly-once, so the guarantee gate admits a job through it.
TEST(S3FactoryRegistration, RowChannelParquetFactoriesBuildFromTheDeclaredColumns) {
    const auto& reg = clink::cluster::OperatorRegistry::default_instance();
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {
        {"bucket", "b"}, {"prefix", "p"}, {"schema_columns", "id:i64;name:str;score:f64;ok:bool"}};

    const auto* source = reg.find_source("s3_parquet_row_source", "row");
    ASSERT_NE(source, nullptr);
    EXPECT_NE(source->build(ctx), nullptr);
    const auto* sink = reg.find_sink("s3_parquet_row_sink", "row");
    ASSERT_NE(sink, nullptr);
    EXPECT_NE(sink->build(ctx), nullptr);
    const auto* two_pc = reg.find_sink("s3_parquet_2pc_row_sink", "row");
    ASSERT_NE(two_pc, nullptr);
    const auto staged = std::static_pointer_cast<clink::Sink<clink::sql::Row>>(two_pc->build(ctx));
    ASSERT_NE(staged, nullptr);
    EXPECT_TRUE(staged->stages_state_at_barrier());

    EXPECT_EQ(clink::cluster::connector_name_for_op_type("s3_parquet_row_sink"), "s3_parquet");
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("s3_parquet_2pc_row_sink"),
              "s3_parquet_2pc");
}

}  // namespace
