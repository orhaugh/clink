// Verifies clink::s3::install() makes the s3_text_sink factory
// reachable through the RunnerRegistry.

#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

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

// What a raw object sink factory makes of `params`: its refusal, or a note that
// it built a sink. Building does not reach the store.
std::string raw_sink_refusal(const std::string& type, std::map<std::string, std::string> params) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(type, "string");
    if (f == nullptr) {
        throw std::logic_error(type + " is not registered");
    }
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = std::move(params);
    try {
        (void)f->build(ctx);
    } catch (const std::runtime_error& e) {
        return e.what();
    }
    return "(built)";
}

// rollover_bytes and part_size are read strictly. Before, a typo fell back to
// the default unnoticed, and rollover_bytes='0' put an object for every record.
TEST(S3FactoryRegistration, RolloverBytesAndPartSizeAreReadStrictly) {
    for (const auto& [type, key] : std::vector<std::pair<std::string, std::string>>{
             {"s3_text_sink", "rollover_bytes"}, {"s3_2pc_string_sink", "part_size"}}) {
        EXPECT_EQ(raw_sink_refusal(type, {{"bucket", "b"}}), "(built)");
        for (const char* v : {"0", "-1", "lots", "10x", "", "+5", "1.5"}) {
            EXPECT_EQ(raw_sink_refusal(type, {{"bucket", "b"}, {key, v}}),
                      type + ": " + key + " must be a positive integer (got '" + v + "')");
        }
        EXPECT_EQ(raw_sink_refusal(type, {{"bucket", "b"}, {key, "1"}}), "(built)");
        EXPECT_EQ(raw_sink_refusal(type, {{"bucket", "b"}, {key, "6000000000"}}), "(built)");
    }
}

// The raw object record describes the text sink and the C++ source, without the
// Parquet modes' format, schema evolution and partition discovery, which their
// own records carry.
TEST(S3FactoryRegistration, TheRawObjectRecordStatesWhatTheTextSinkAndTheSourceDo) {
    const auto* rec = clink::connectors::CapabilityRegistry::instance().find("s3");
    ASSERT_NE(rec, nullptr);
    EXPECT_TRUE(rec->self_check().empty());
    EXPECT_EQ(rec->version, "2");
    EXPECT_EQ(rec->formats, (std::vector<std::string>{"text"}));
    EXPECT_FALSE(rec->schema_evolution);
    EXPECT_FALSE(rec->partition_discovery);
    EXPECT_EQ(rec->delivery, clink::connectors::DeliveryGuarantee::AtMostOnce);
    EXPECT_EQ(rec->limitations,
              (std::vector<std::string>{
                  "the text sink puts a part only at rollover_bytes and at the end of input, not "
                  "at a checkpoint: records buffered when a process dies are lost, and parts put "
                  "after the last checkpoint are put again after a restore; use s3_2pc for output "
                  "tied to checkpoint completion",
                  "the C++ source emits each object whole, as one batch held in memory, and "
                  "resumes a restore after the last object its checkpoint covered, by key: an "
                  "object added since with a key that sorts before that one is not read",
              }));
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
