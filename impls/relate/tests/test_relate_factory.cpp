// The registered factory, the capability record, and the shared-chain refusal:
// the sink holds its guarantee only while its barrier hook comes before the
// checkpoint's acknowledgement, so it declares gates_checkpoint_ack() and
// Dag::add_sink refuses it beside another sink.

#include <algorithm>
#include <map>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

#include "options.hpp"

namespace {

using clink::sql::Row;

std::map<std::string, std::string> sink_params() {
    return {{"path", "/tmp/clink-relate-factory-never-opened.rl"},
            {"statement", "UNWIND $rows AS row MERGE (a:Account {id: row.id})"},
            {"sql_column_types", "id:BIGINT"}};
}

}  // namespace

TEST(RelateFactory, TheRowSinkIsRegisteredAndBuildsWithoutOpeningAStore) {
    const auto* factory =
        clink::cluster::OperatorRegistry::default_instance().find_sink("relate_sink", "row");
    ASSERT_NE(factory, nullptr) << "relate_sink is not registered on the row channel";
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = sink_params();
    const auto sink = std::static_pointer_cast<clink::Sink<Row>>(factory->build(ctx));
    ASSERT_NE(sink, nullptr);
    EXPECT_EQ(sink->name(), "relate_sink");
    EXPECT_TRUE(sink->gates_checkpoint_ack());
    EXPECT_FALSE(sink->stages_state_at_barrier());
}

TEST(RelateFactory, TheFactoryRefusesBadOptionsWithTheirCode) {
    const auto* factory =
        clink::cluster::OperatorRegistry::default_instance().find_sink("relate_sink", "row");
    ASSERT_NE(factory, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = sink_params();
    ctx.params["delivery_guarantee"] = "exactly_once";
    try {
        (void)factory->build(ctx);
        FAIL() << "an exactly-once request was accepted";
    } catch (const clink::relate::RelateError& e) {
        EXPECT_EQ(e.code(), clink::relate::code::kGuaranteeUnsupported);
    }
}

TEST(RelateFactory, TheCapabilityRecordSaysWhatTheSinkGuarantees) {
    const auto* it = clink::connectors::CapabilityRegistry::instance().find("relate");
    ASSERT_NE(it, nullptr) << "the relate capability record is not declared";
    EXPECT_TRUE(it->is_sink);
    EXPECT_FALSE(it->is_source);
    EXPECT_TRUE(it->checkpoint_integrated);
    EXPECT_EQ(it->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_FALSE(it->transactional);
    EXPECT_TRUE(it->available_in_sql);
    EXPECT_TRUE(it->self_check().empty());
}
