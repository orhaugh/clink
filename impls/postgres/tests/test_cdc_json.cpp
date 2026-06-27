// Offline unit tests for cdc_event_to_json_row (M5): the CdcEvent -> flat JSON
// row used by the Row-path postgres_cdc_source. No server.

#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/connectors/cdc_event.hpp"
#include "clink/connectors/cdc_json.hpp"

namespace {

using clink::CdcEvent;
using clink::CdcField;
using clink::pgcdc::cdc_event_to_json_row;

CdcEvent make_change(CdcEvent::Op op) {
    CdcEvent ev;
    ev.op = op;
    ev.table = "public.users";
    ev.lsn = "0/16E2A38";
    ev.xid = 42;
    ev.values.push_back({.name = "id", .value = "1", .type = "int4", .is_null = false});
    ev.values.push_back({.name = "name", .value = "alice", .type = "text", .is_null = false});
    ev.values.push_back(
        {.name = "deleted_at", .value = "", .type = "timestamptz", .is_null = true});
    return ev;
}

TEST(CdcJson, InsertEmitsFlatObjectWithColumnsAndMetadata) {
    auto out = cdc_event_to_json_row(make_change(CdcEvent::Op::Insert));
    ASSERT_TRUE(out.has_value());
    auto j = clink::config::parse(*out);
    ASSERT_TRUE(j.is_object());
    const auto& o = j.as_object();
    // Data columns are flat at the top level.
    EXPECT_EQ(o.at("id").as_string(), "1");
    EXPECT_EQ(o.at("name").as_string(), "alice");
    // A NULL cell is JSON null, not "" (CdcField::is_null is honoured).
    EXPECT_TRUE(o.at("deleted_at").is_null());
    // CDC metadata under reserved keys.
    EXPECT_EQ(o.at("__op").as_string(), "insert");
    EXPECT_EQ(o.at("__table").as_string(), "public.users");
    EXPECT_EQ(o.at("__lsn").as_string(), "0/16E2A38");
    EXPECT_TRUE(o.at("__xid").is_number());
}

TEST(CdcJson, UpdateAndDeleteAlsoEmit) {
    EXPECT_TRUE(cdc_event_to_json_row(make_change(CdcEvent::Op::Update)).has_value());
    EXPECT_TRUE(cdc_event_to_json_row(make_change(CdcEvent::Op::Delete)).has_value());
}

TEST(CdcJson, TransactionMarkersAreSkipped) {
    EXPECT_FALSE(cdc_event_to_json_row(make_change(CdcEvent::Op::Begin)).has_value());
    EXPECT_FALSE(cdc_event_to_json_row(make_change(CdcEvent::Op::Commit)).has_value());
    EXPECT_FALSE(cdc_event_to_json_row(make_change(CdcEvent::Op::Truncate)).has_value());
    EXPECT_FALSE(cdc_event_to_json_row(make_change(CdcEvent::Op::Unknown)).has_value());
}

}  // namespace
