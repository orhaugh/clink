// The registered factory, the capability record, and the shared-chain refusal:
// the sink holds its guarantee only while its barrier hook comes before the
// checkpoint's acknowledgement, so it declares gates_checkpoint_ack() and
// Dag::add_sink refuses it beside another sink, whichever is added first.

#include <memory>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/sql/row.hpp"

#include "options.hpp"

namespace {

using clink::sql::Row;

std::map<std::string, std::string> neo4j_params() {
    return {{"endpoint", "http://127.0.0.1:7474"},
            {"statement", "UNWIND $rows AS row MERGE (a:Account {id: row.id})"},
            {"sql_column_types", "id:BIGINT"}};
}

std::shared_ptr<clink::Sink<Row>> build_from_registry() {
    const auto* factory =
        clink::cluster::OperatorRegistry::default_instance().find_sink("neo4j_sink", "row");
    if (factory == nullptr) {
        return nullptr;
    }
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = neo4j_params();
    return std::static_pointer_cast<clink::Sink<Row>>(factory->build(ctx));
}

class RowSource final : public clink::Source<Row> {
public:
    bool produce(clink::Emitter<Row>& /*out*/) override { return false; }
};

class OtherRowSink final : public clink::Sink<Row> {
public:
    void on_data(const clink::Batch<Row>& /*batch*/) override {}
};

}  // namespace

TEST(Neo4jFactory, TheRowSinkIsRegisteredAndBuildsWithoutConnecting) {
    const auto sink = build_from_registry();
    ASSERT_NE(sink, nullptr) << "neo4j_sink is not registered on the row channel";
    EXPECT_EQ(sink->name(), "neo4j_sink");
    EXPECT_TRUE(sink->gates_checkpoint_ack());
    EXPECT_FALSE(sink->stages_state_at_barrier());
}

TEST(Neo4jFactory, TheFactoryRefusesBadOptionsWithTheirCode) {
    const auto* factory =
        clink::cluster::OperatorRegistry::default_instance().find_sink("neo4j_sink", "row");
    ASSERT_NE(factory, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = neo4j_params();
    ctx.params["delivery_guarantee"] = "exactly_once";
    try {
        (void)factory->build(ctx);
        FAIL() << "an exactly-once request was accepted";
    } catch (const clink::neo4j::Neo4jError& e) {
        EXPECT_EQ(e.code(), clink::neo4j::code::kGuaranteeUnsupported);
    }
}

TEST(Neo4jFactory, TheLookupJoinOperatorIsRegisteredAndRefusesBadOptions) {
    const auto* factory = clink::cluster::OperatorRegistry::default_instance().find_operator(
        "neo4j_lookup_join_row", "row", "row");
    ASSERT_NE(factory, nullptr) << "neo4j_lookup_join_row is not registered row to row";
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"probe_alias", "s"},
                  {"dim_alias", "d"},
                  {"probe_columns", "id"},
                  {"dim_columns", "id,name"},
                  {"join_type", "left_outer"},
                  {"probe_key_column", "id"},
                  {"probe_key_type", "BIGINT"},
                  {"dim_key_column", "id"},
                  {"dim_column_types", "id:BIGINT;name:VARCHAR"},
                  {"neo4j.endpoint", "http://127.0.0.1:7474"},
                  {"neo4j.statement", "MATCH (a:Account {id: $key}) RETURN a.name AS name"}};
    // Built without connecting: the statement is checked when it opens.
    EXPECT_NE(factory->build(ctx), nullptr);
    ctx.params["neo4j.max_in_flight"] = "0";
    try {
        (void)factory->build(ctx);
        FAIL() << "max_in_flight=0 was accepted";
    } catch (const clink::neo4j::Neo4jError& e) {
        EXPECT_EQ(e.code(), clink::neo4j::code::kOptionInvalid);
        EXPECT_EQ(std::string(e.what()).rfind("neo4j lookup: ", 0), 0U) << e.what();
    }
}

TEST(Neo4jFactory, TheCapabilityRecordIsAtLeastOnceAndCoherent) {
    const auto* rec = clink::connectors::CapabilityRegistry::instance().find("neo4j");
    ASSERT_NE(rec, nullptr);
    EXPECT_TRUE(rec->is_sink);
    EXPECT_FALSE(rec->is_source);
    EXPECT_EQ(rec->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_FALSE(rec->transactional);
    EXPECT_TRUE(rec->checkpoint_integrated);
    EXPECT_TRUE(rec->available_in_sql);
    EXPECT_TRUE(rec->self_check().empty());
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("neo4j_sink"), "neo4j");
}

TEST(Neo4jFactory, ADagRefusesTheSinkBesideAnotherSinkInEitherOrder) {
    for (const bool neo4j_first : {true, false}) {
        SCOPED_TRACE(neo4j_first ? "neo4j sink added first" : "other sink added first");
        clink::Dag dag;
        const auto src = dag.add_source<Row>(std::make_shared<RowSource>());
        const auto neo4j = build_from_registry();
        ASSERT_NE(neo4j, nullptr);
        if (neo4j_first) {
            dag.add_sink<Row>(src, neo4j);
            EXPECT_THROW(dag.add_sink<Row>(src, std::make_shared<OtherRowSink>()),
                         std::logic_error);
        } else {
            dag.add_sink<Row>(src, std::make_shared<OtherRowSink>());
            EXPECT_THROW(dag.add_sink<Row>(src, neo4j), std::logic_error);
        }
    }
}
