// Column-level lineage capture from the SQL planner: parse -> bind -> compile,
// then assert the spec.column_lineage carrier traces each sink column back to
// the right source table column(s).

#include <optional>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/lineage/lineage_graph.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"

namespace clink::sql {
namespace {

void register_table(Catalog& cat, const std::string& ddl) {
    auto s = parse(ddl);
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
}

// Compile an INSERT and return the captured column-lineage fields for its
// single sink (the carrier is keyed by sink op id; there is exactly one).
std::vector<lineage::ColumnLineageField> capture(const Catalog& cat, const char* sql) {
    Binder b(cat);
    auto plan = b.bind_insert(std::get<ast::InsertStmt>(parse(sql).statements[0]));
    PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const LogicalSink&>(*plan));
    if (spec.column_lineage.empty()) {
        return {};
    }
    const auto root = config::parse(spec.column_lineage);
    for (const auto& [sink_id, arr] : root.as_object()) {
        (void)sink_id;
        return lineage::column_lineage_from_json(arr.serialize());
    }
    return {};
}

std::optional<lineage::ColumnLineageField> field_for(
    const std::vector<lineage::ColumnLineageField>& fields, const std::string& output) {
    for (const auto& f : fields) {
        if (f.output == output) {
            return f;
        }
    }
    return std::nullopt;
}

// A kafka JSON source/sink so the source dataset identity is the clear
// kafka://<brokers>/<topic> shape.
void register_kafka(Catalog& cat, const char* name, const char* cols, const char* topic) {
    register_table(cat,
                   std::string("CREATE TABLE ") + name + " " + cols +
                       " WITH (connector='kafka', format='json', brokers='b:9092', topic='" +
                       topic + "')");
}

}  // namespace

TEST(SqlColumnLineage, IdentityProjection) {
    Catalog cat;
    register_kafka(cat, "orders", "(id BIGINT, amount BIGINT)", "orders_in");
    register_kafka(cat, "out", "(id BIGINT, amount BIGINT)", "orders_out");

    const auto fields = capture(cat, "INSERT INTO out SELECT id, amount FROM orders");

    const auto id = field_for(fields, "id");
    const auto amount = field_for(fields, "amount");
    ASSERT_TRUE(id.has_value());
    ASSERT_TRUE(amount.has_value());
    EXPECT_EQ(id->transformation, "IDENTITY");
    ASSERT_EQ(id->inputs.size(), 1u);
    EXPECT_EQ(id->inputs[0].ns, "kafka://b:9092");
    EXPECT_EQ(id->inputs[0].name, "orders_in");
    EXPECT_EQ(id->inputs[0].field, "id");
    ASSERT_EQ(amount->inputs.size(), 1u);
    EXPECT_EQ(amount->inputs[0].field, "amount");
}

TEST(SqlColumnLineage, ComputedExpressionIsTransformation) {
    Catalog cat;
    register_kafka(cat, "orders", "(id BIGINT, amount BIGINT)", "orders_in");
    register_kafka(cat, "out", "(id BIGINT, bumped DOUBLE)", "orders_out");

    const auto fields =
        capture(cat, "INSERT INTO out SELECT id, amount + 100 AS bumped FROM orders");

    const auto bumped = field_for(fields, "bumped");
    ASSERT_TRUE(bumped.has_value());
    EXPECT_EQ(bumped->transformation, "TRANSFORMATION");
    ASSERT_EQ(bumped->inputs.size(), 1u);
    EXPECT_EQ(bumped->inputs[0].name, "orders_in");
    EXPECT_EQ(bumped->inputs[0].field, "amount");
}

TEST(SqlColumnLineage, AggregateTracesToInputColumn) {
    Catalog cat;
    register_kafka(cat, "orders", "(region TEXT, amount BIGINT)", "orders_in");
    register_kafka(cat, "out", "(region TEXT, total BIGINT)", "orders_out");

    const auto fields = capture(
        cat, "INSERT INTO out SELECT region, SUM(amount) AS total FROM orders GROUP BY region");

    const auto region = field_for(fields, "region");
    const auto total = field_for(fields, "total");
    ASSERT_TRUE(region.has_value());
    ASSERT_TRUE(total.has_value());
    // Group key passes through; aggregate is an AGGREGATION over amount.
    ASSERT_EQ(region->inputs.size(), 1u);
    EXPECT_EQ(region->inputs[0].field, "region");
    EXPECT_EQ(total->transformation, "AGGREGATION");
    ASSERT_EQ(total->inputs.size(), 1u);
    EXPECT_EQ(total->inputs[0].field, "amount");
}

TEST(SqlColumnLineage, JoinTracesColumnsToCorrectSide) {
    Catalog cat;
    register_kafka(cat, "orders", "(oid BIGINT, cust BIGINT)", "orders_in");
    register_kafka(cat, "customers", "(cust BIGINT, name TEXT)", "customers_in");
    register_kafka(cat, "out", "(oid BIGINT, name TEXT)", "joined_out");

    // Post-join columns are flat-named <alias>_<col>; reference those.
    const auto fields = capture(cat,
                                "INSERT INTO out "
                                "SELECT o_oid, c_name "
                                "FROM orders o JOIN customers c ON o.cust = c.cust");

    const auto oid = field_for(fields, "oid");
    const auto name = field_for(fields, "name");
    ASSERT_TRUE(oid.has_value());
    ASSERT_TRUE(name.has_value());
    ASSERT_EQ(oid->inputs.size(), 1u);
    EXPECT_EQ(oid->inputs[0].name, "orders_in");
    EXPECT_EQ(oid->inputs[0].field, "oid");
    ASSERT_EQ(name->inputs.size(), 1u);
    EXPECT_EQ(name->inputs[0].name, "customers_in");
    EXPECT_EQ(name->inputs[0].field, "name");
}

}  // namespace clink::sql
