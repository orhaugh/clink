// Pairing the declared input columns with the target's columns, the type
// rules for each pair, the refusal message, and the header check.

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "native/column_plan.hpp"
#include "native/errors.hpp"
#include "native/types.hpp"

namespace clink::clickhouse::native {
namespace {

TargetColumn plan_target(std::string name,
                         std::string type,
                         DefaultKind kind = DefaultKind::None,
                         std::uint32_t position = 0) {
    return TargetColumn{std::move(name), std::move(type), kind, position};
}

// One column `c`, declared as `sql` and stored as `ch`.
PlanResult plan_one(const std::string& sql, const std::string& ch) {
    return compile_column_plan(parse_sql_column_types("c:" + sql), {plan_target("c", ch)});
}

ColumnBinding plan_accepts(const std::string& sql, const std::string& ch) {
    const PlanResult r = plan_one(sql, ch);
    EXPECT_TRUE(r.plan.has_value())
        << sql << " into " << ch << ": " << (r.problems.empty() ? "" : r.problems[0].message);
    if (!r.plan || r.plan->columns.size() != 1) {
        return {};
    }
    return r.plan->columns.front();
}

std::string plan_refuses(const std::string& sql, const std::string& ch) {
    const PlanResult r = plan_one(sql, ch);
    EXPECT_FALSE(r.plan.has_value()) << sql << " into " << ch;
    EXPECT_EQ(r.problems.size(), 1U) << sql << " into " << ch;
    return r.problems.empty() ? std::string() : r.problems.front().message;
}

TEST(NativeColumnPlan, PlainTargetColumnTheInputLacksIsAProblem) {
    const PlanResult r =
        compile_column_plan(parse_sql_column_types("id:BIGINT"),
                            {plan_target("id", "Int64"), plan_target("tenant", "String")});
    ASSERT_FALSE(r.plan.has_value());
    ASSERT_EQ(r.problems.size(), 1U);
    EXPECT_EQ(r.problems[0].column, "tenant");
    EXPECT_EQ(r.problems[0].message,
              "target column `tenant` has no default and the query does not produce it");
}

TEST(NativeColumnPlan, InputColumnTheTargetLacksIsAProblem) {
    const PlanResult r = compile_column_plan(parse_sql_column_types("id:BIGINT;extra:VARCHAR"),
                                             {plan_target("id", "Int64")});
    ASSERT_EQ(r.problems.size(), 1U);
    EXPECT_EQ(r.problems[0].column, "extra");
    EXPECT_EQ(r.problems[0].message,
              "column `extra` is not in the target table; drop it from the SELECT or add it to "
              "the table");
}

TEST(NativeColumnPlan, ACaseOnlyDifferenceIsNamedInTheMessage) {
    const PlanResult r =
        compile_column_plan(parse_sql_column_types("UserId:BIGINT"),
                            {plan_target("userid", "Int64", DefaultKind::Default)});
    ASSERT_EQ(r.problems.size(), 1U);
    EXPECT_EQ(r.problems[0].message,
              "column `UserId` is not in the target table; the target has `userid`, which "
              "differs only in case, and ClickHouse names are case-sensitive");
}

TEST(NativeColumnPlan, MaterializedAndAliasColumnsAreRefusedWhenProduced) {
    const PlanResult r =
        compile_column_plan(parse_sql_column_types("id:BIGINT;total:BIGINT;label:VARCHAR"),
                            {plan_target("id", "Int64"),
                             plan_target("total", "Int64", DefaultKind::Materialized),
                             plan_target("label", "String", DefaultKind::Alias)});
    ASSERT_EQ(r.problems.size(), 2U);
    EXPECT_EQ(r.problems[0].column, "total");
    EXPECT_EQ(r.problems[0].message,
              "`total` is MATERIALIZED; the server computes it; drop it from the SELECT");
    EXPECT_EQ(r.problems[1].column, "label");
    EXPECT_EQ(r.problems[1].message,
              "`label` is ALIAS; the server computes it; drop it from the SELECT");
}

TEST(NativeColumnPlan, DefaultAndEphemeralColumnsAreWrittenWhenProduced) {
    const PlanResult r =
        compile_column_plan(parse_sql_column_types("id:BIGINT;created:TIMESTAMP(3);raw:VARCHAR"),
                            {plan_target("id", "Int64", DefaultKind::None, 1),
                             plan_target("created", "DateTime64(3)", DefaultKind::Default, 2),
                             plan_target("raw", "String", DefaultKind::Ephemeral, 3)});
    ASSERT_TRUE(r.plan.has_value()) << r.problems.at(0).message;
    ASSERT_EQ(r.plan->columns.size(), 3U);
    EXPECT_TRUE(r.plan->omitted.empty());
    EXPECT_EQ(r.plan->column_list_sql, "(`id`, `created`, `raw`)");
}

TEST(NativeColumnPlan, ColumnsTheInputLacksAreLeftToTheServer) {
    const PlanResult r =
        compile_column_plan(parse_sql_column_types("id:BIGINT"),
                            {plan_target("created", "DateTime", DefaultKind::Default, 1),
                             plan_target("id", "Int64", DefaultKind::None, 2),
                             plan_target("raw", "String", DefaultKind::Ephemeral, 3),
                             plan_target("day", "Date", DefaultKind::Materialized, 4),
                             plan_target("label", "String", DefaultKind::Alias, 5)});
    ASSERT_TRUE(r.plan.has_value()) << r.problems.at(0).message;
    EXPECT_EQ(r.plan->column_list_sql, "(`id`)");
    ASSERT_EQ(r.plan->omitted.size(), 4U);
    EXPECT_EQ(r.plan->omitted[0].name, "created");
    EXPECT_EQ(r.plan->omitted[0].default_kind, DefaultKind::Default);
    EXPECT_EQ(r.plan->omitted[1].name, "raw");
    EXPECT_EQ(r.plan->omitted[1].default_kind, DefaultKind::Ephemeral);
    EXPECT_EQ(r.plan->omitted[2].name, "day");
    EXPECT_EQ(r.plan->omitted[2].default_kind, DefaultKind::Materialized);
    EXPECT_EQ(r.plan->omitted[3].name, "label");
    EXPECT_EQ(r.plan->omitted[3].default_kind, DefaultKind::Alias);
}

TEST(NativeColumnPlan, EveryProblemArrivesInOneRefusal) {
    const auto input = parse_sql_column_types("amount:DECIMAL(18, 2);score:DOUBLE");
    const std::vector<TargetColumn> target = {plan_target("tenant", "String"),
                                              plan_target("amount", "Decimal(18, 4)"),
                                              plan_target("score", "Float32")};
    const PlanResult r = compile_column_plan(input, target);
    ASSERT_EQ(r.problems.size(), 3U);
    EXPECT_EQ(r.problems[0].column, "tenant");
    EXPECT_EQ(r.problems[1].column, "amount");
    EXPECT_EQ(r.problems[2].column, "score");
    try {
        (void)compile_or_refuse(input, target, "`analytics`.`events`");
        FAIL() << "no refusal";
    } catch (const NativeSinkError& e) {
        EXPECT_EQ(e.code(), code::kColumnPlan);
        EXPECT_STREQ(e.what(),
                     "[clickhouse.column_plan] `analytics`.`events` cannot take this table's "
                     "rows:\n"
                     "  - target column `tenant` has no default and the query does not produce "
                     "it\n"
                     "  - column `amount`: DECIMAL(18, 2) into Decimal(18, 4) leaves 14 integer "
                     "digits for 16; widen the target or declare the clink column DECIMAL(16, 2)\n"
                     "  - column `score`: DOUBLE into Float32 narrows; use REAL in the clink table "
                     "or CAST in the SELECT");
    }
}

TEST(NativeColumnPlan, CompileOrRefuseReturnsTheAcceptedPlan) {
    const ColumnPlan plan =
        compile_or_refuse(parse_sql_column_types("a:BIGINT;b:VARCHAR"),
                          {plan_target("a", "Int64"), plan_target("b", "String")},
                          "`db`.`t`");
    ASSERT_EQ(plan.columns.size(), 2U);
    EXPECT_EQ(plan.columns[0].name, "a");
    EXPECT_EQ(plan.columns[0].input_index, 0);
    EXPECT_EQ(plan.columns[1].name, "b");
    EXPECT_EQ(plan.columns[1].input_index, 1);
    EXPECT_EQ(plan.column_list_sql, "(`a`, `b`)");
}

TEST(NativeColumnPlan, ColumnOrderFollowsTheInputNotTheTable) {
    const PlanResult r = compile_column_plan(parse_sql_column_types("b:VARCHAR;a:BIGINT"),
                                             {plan_target("a", "Int64", DefaultKind::None, 1),
                                              plan_target("b", "String", DefaultKind::None, 2)});
    ASSERT_TRUE(r.plan.has_value());
    EXPECT_EQ(r.plan->column_list_sql, "(`b`, `a`)");
    EXPECT_EQ(r.plan->columns[0].input_index, 0);
    EXPECT_EQ(r.plan->columns[1].input_index, 1);
}

TEST(NativeColumnPlan, ColumnListQuotesAwkwardNames) {
    const PlanResult r =
        compile_column_plan(parse_sql_column_types("we`ird:BIGINT;back\\slash:BIGINT"),
                            {plan_target("we`ird", "Int64"), plan_target("back\\slash", "Int64")});
    ASSERT_TRUE(r.plan.has_value());
    EXPECT_EQ(r.plan->column_list_sql, "(`we\\`ird`, `back\\\\slash`)");
}

TEST(NativeColumnPlan, AnEmptyInputIsAProblem) {
    const PlanResult r = compile_column_plan({}, {});
    ASSERT_EQ(r.problems.size(), 1U);
    EXPECT_EQ(r.problems[0].message, "the clink table declares no columns");
}

TEST(NativeColumnPlan, ADuplicateInputColumnIsAProblem) {
    std::vector<SqlColumn> input = parse_sql_column_types("a:BIGINT");
    input.push_back(input.front());
    const PlanResult r = compile_column_plan(input, {plan_target("a", "Int64")});
    ASSERT_EQ(r.problems.size(), 1U);
    EXPECT_EQ(r.problems[0].message, "column `a` appears twice in the clink table");
}

// Every scalar pair: the accepted ones with their conversion, and every
// other pairing refused with the generic remedy, except the two that carry
// their own advice.
TEST(NativeColumnPlan, EveryScalarPairIsAcceptedOrRefused) {
    const std::vector<std::string> sql = {"TINYINT",
                                          "SMALLINT",
                                          "INTEGER",
                                          "BIGINT",
                                          "REAL",
                                          "DOUBLE",
                                          "BOOLEAN",
                                          "VARCHAR",
                                          "DECIMAL(10, 2)",
                                          "DATE",
                                          "TIMESTAMP(3)"};
    const std::vector<std::string> ch = {
        "Int8",           "Int16",           "Int32",    "Int64",         "Int128",
        "UInt8",          "UInt16",          "UInt32",   "UInt64",        "UInt128",
        "Float32",        "Float64",         "Bool",     "String",        "FixedString(4)",
        "Enum8('a' = 1)", "Enum16('a' = 1)", "UUID",     "IPv4",          "IPv6",
        "Date",           "Date32",          "DateTime", "DateTime64(3)", "Decimal(12, 4)"};
    using C = Conversion;
    const std::map<std::pair<std::string, std::string>, Conversion> accepted = {
        {{"TINYINT", "Int8"}, C::Copy},
        {{"TINYINT", "Int16"}, C::WidenInt},
        {{"TINYINT", "Int32"}, C::WidenInt},
        {{"TINYINT", "Int64"}, C::WidenInt},
        {{"TINYINT", "Int128"}, C::WidenInt},
        {{"SMALLINT", "Int8"}, C::NarrowInt},
        {{"SMALLINT", "Int16"}, C::Copy},
        {{"SMALLINT", "Int32"}, C::WidenInt},
        {{"SMALLINT", "Int64"}, C::WidenInt},
        {{"SMALLINT", "Int128"}, C::WidenInt},
        {{"INTEGER", "Int8"}, C::NarrowInt},
        {{"INTEGER", "Int16"}, C::NarrowInt},
        {{"INTEGER", "Int32"}, C::Copy},
        {{"INTEGER", "Int64"}, C::WidenInt},
        {{"INTEGER", "Int128"}, C::WidenInt},
        {{"BIGINT", "Int8"}, C::NarrowInt},
        {{"BIGINT", "Int16"}, C::NarrowInt},
        {{"BIGINT", "Int32"}, C::NarrowInt},
        {{"BIGINT", "Int64"}, C::Copy},
        {{"BIGINT", "Int128"}, C::WidenInt},
        {{"REAL", "Float32"}, C::Copy},
        {{"REAL", "Float64"}, C::RealToDouble},
        {{"DOUBLE", "Float64"}, C::Copy},
        {{"BOOLEAN", "Bool"}, C::BoolUnpack},
        {{"BOOLEAN", "UInt8"}, C::BoolUnpack},
        {{"VARCHAR", "String"}, C::StringZeroCopy},
        {{"VARCHAR", "FixedString(4)"}, C::StringToFixed},
        {{"VARCHAR", "Enum8('a' = 1)"}, C::StringToEnum},
        {{"VARCHAR", "Enum16('a' = 1)"}, C::StringToEnum},
        {{"VARCHAR", "UUID"}, C::StringToUuid},
        {{"VARCHAR", "IPv4"}, C::StringToIpv4},
        {{"VARCHAR", "IPv6"}, C::StringToIpv6},
        {{"DECIMAL(10, 2)", "Decimal(12, 4)"}, C::DecimalRescale},
        {{"DATE", "Date"}, C::DateToDate},
        {{"DATE", "Date32"}, C::DateToDate32},
        {{"TIMESTAMP(3)", "DateTime"}, C::TimestampToDateTime},
        {{"TIMESTAMP(3)", "DateTime64(3)"}, C::TimestampToDateTime64},
    };
    for (const auto& s : sql) {
        for (const auto& c : ch) {
            const bool is_int =
                s == "TINYINT" || s == "SMALLINT" || s == "INTEGER" || s == "BIGINT";
            const bool unsigned_target =
                c == "UInt8" || c == "UInt16" || c == "UInt32" || c == "UInt64";
            std::optional<Conversion> want;
            if (const auto it = accepted.find({s, c}); it != accepted.end()) {
                want = it->second;
            } else if (is_int && unsigned_target) {
                want = C::SignedToUnsigned;
            }
            if (want) {
                const ColumnBinding b = plan_accepts(s, c);
                EXPECT_EQ(b.conversion, *want) << s << " into " << c;
                EXPECT_EQ(b.zero_copy, *want == C::StringZeroCopy) << s << " into " << c;
                continue;
            }
            std::string expected = "column `c`: " + s + " into " + c;
            if (s == "DOUBLE" && c == "Float32") {
                expected += " narrows; use REAL in the clink table or CAST in the SELECT";
            } else {
                expected += " is not supported; CAST in the SELECT";
            }
            EXPECT_EQ(plan_refuses(s, c), expected);
        }
    }
}

TEST(NativeColumnPlan, NullableTargetsTakeTheSameRules) {
    const ColumnBinding i = plan_accepts("BIGINT", "Nullable(Int32)");
    EXPECT_EQ(i.conversion, Conversion::NarrowInt);
    EXPECT_TRUE(i.target.nullable);
    EXPECT_EQ(i.expected_header_type, "Nullable(Int32)");

    const ColumnBinding s = plan_accepts("VARCHAR", "Nullable(String)");
    EXPECT_EQ(s.conversion, Conversion::StringZeroCopy);
    EXPECT_TRUE(s.zero_copy);

    const ColumnBinding t = plan_accepts("TIMESTAMP(6)", "Nullable(DateTime64(6, 'UTC'))");
    EXPECT_EQ(t.conversion, Conversion::TimestampToDateTime64);
    EXPECT_EQ(t.multiplier, 1000);

    EXPECT_EQ(plan_refuses("DOUBLE", "Nullable(Float32)"),
              "column `c`: DOUBLE into Nullable(Float32) narrows; use REAL in the clink table or "
              "CAST in the SELECT");
}

TEST(NativeColumnPlan, LowCardinalityTargetsAreWrittenAsTheirNestedColumn) {
    const ColumnBinding s = plan_accepts("VARCHAR", "LowCardinality(String)");
    EXPECT_EQ(s.conversion, Conversion::StringZeroCopy);
    EXPECT_TRUE(s.zero_copy);
    EXPECT_TRUE(s.target.low_cardinality);
    EXPECT_EQ(s.expected_header_type, "LowCardinality(String)");

    const ColumnBinding n = plan_accepts("VARCHAR", "LowCardinality(Nullable(String))");
    EXPECT_EQ(n.conversion, Conversion::StringZeroCopy);
    EXPECT_TRUE(n.target.nullable);
    EXPECT_EQ(n.expected_header_type, "LowCardinality(Nullable(String))");

    const ColumnBinding f = plan_accepts("VARCHAR", "LowCardinality(FixedString(8))");
    EXPECT_EQ(f.conversion, Conversion::StringToFixed);
    EXPECT_FALSE(f.zero_copy);

    EXPECT_EQ(plan_refuses("BIGINT", "LowCardinality(String)"),
              "column `c`: BIGINT into LowCardinality(String) is not supported; CAST in the "
              "SELECT");
    EXPECT_EQ(plan_refuses("INTEGER", "LowCardinality(Int32)"),
              "column `c`: LowCardinality(Int32) is not supported by the native sink: "
              "clickhouse-cpp builds LowCardinality only over String, FixedString(N) and "
              "Nullable(String)");
}

TEST(NativeColumnPlan, OnlyZeroCopyStringsRetainChunks) {
    const PlanResult fixed =
        compile_column_plan(parse_sql_column_types("a:BIGINT;b:VARCHAR"),
                            {plan_target("a", "Int64"), plan_target("b", "FixedString(8)")});
    ASSERT_TRUE(fixed.plan.has_value());
    EXPECT_FALSE(fixed.plan->retains_chunks);

    const PlanResult strings =
        compile_column_plan(parse_sql_column_types("a:BIGINT;b:VARCHAR"),
                            {plan_target("a", "Int64"), plan_target("b", "String")});
    ASSERT_TRUE(strings.plan.has_value());
    EXPECT_TRUE(strings.plan->retains_chunks);

    const PlanResult nested = compile_column_plan(
        parse_sql_column_types("a:ROW<x BIGINT, tags VARCHAR ARRAY>"),
        {plan_target("a", "Tuple(x Int64, tags Array(LowCardinality(String)))")});
    ASSERT_TRUE(nested.plan.has_value()) << nested.problems.at(0).message;
    EXPECT_TRUE(nested.plan->retains_chunks);
    EXPECT_FALSE(nested.plan->columns[0].zero_copy);
    EXPECT_TRUE(nested.plan->columns[0].children.at(1).children.at(0).zero_copy);
}

TEST(NativeColumnPlan, DecimalNeedsEveryIntegerAndFractionalDigit) {
    EXPECT_EQ(plan_refuses("DECIMAL(10, 2)", "Decimal(10, 4)"),
              "column `c`: DECIMAL(10, 2) into Decimal(10, 4) leaves 6 integer digits for 8; "
              "widen the target or declare the clink column DECIMAL(8, 2)");

    const ColumnBinding wide = plan_accepts("DECIMAL(10, 2)", "Decimal(12, 4)");
    EXPECT_EQ(wide.conversion, Conversion::DecimalRescale);
    EXPECT_EQ(wide.multiplier, 100);
    EXPECT_EQ(wide.divisor, 1);

    const ColumnBinding same = plan_accepts("DECIMAL(18, 2)", "Decimal(18, 2)");
    EXPECT_EQ(same.conversion, Conversion::DecimalRescale);
    EXPECT_EQ(same.multiplier, 1);

    const ColumnBinding sized = plan_accepts("DECIMAL(9, 2)", "Decimal64(4)");
    EXPECT_EQ(sized.multiplier, 100);
    EXPECT_EQ(sized.expected_header_type, "Decimal(18,4)");

    EXPECT_EQ(plan_refuses("DECIMAL(18, 4)", "Decimal(20, 2)"),
              "column `c`: DECIMAL(18, 4) into Decimal(20, 2) drops 2 fractional digits; raise "
              "the target's scale to 4 or CAST in the SELECT");

    EXPECT_EQ(plan_refuses("DECIMAL(3, 0)", "Decimal(4, 4)"),
              "column `c`: DECIMAL(3, 0) into Decimal(4, 4) leaves 0 integer digits for 3; "
              "widen the target");

    EXPECT_EQ(plan_refuses("DECIMAL(10, 2)", "Decimal(40, 4)"),
              "column `c`: Decimal(40, 4) is not supported by the native sink: clickhouse-cpp "
              "holds a decimal in at most 128 bits, which caps its precision at 38");
    EXPECT_EQ(plan_refuses("DECIMAL(10, 2)", "Decimal256(4)"),
              "column `c`: Decimal256(4) is not supported by the native sink: clickhouse-cpp "
              "holds a decimal in at most 128 bits, which caps its precision at 38");
}

TEST(NativeColumnPlan, DecimalRescaleIsBoundedByTheFactorsWidth) {
    const ColumnBinding max = plan_accepts("DECIMAL(1, 0)", "Decimal(38, 18)");
    EXPECT_EQ(max.multiplier, 1000000000000000000LL);
    EXPECT_EQ(plan_refuses("DECIMAL(1, 0)", "Decimal(38, 20)"),
              "column `c`: DECIMAL(1, 0) into Decimal(38, 20) rescales by 20 digits, more than "
              "the 18 the native sink supports; declare the clink column with a scale of at "
              "least 2");
}

// Every TIMESTAMP(p) arrives as epoch milliseconds, so p plays no part:
// milliseconds go into any DateTime64(P), scaled up when P >= 3 and checked
// for whole multiples when P < 3, and into DateTime as whole seconds.
TEST(NativeColumnPlan, EveryTimestampGoesIntoEveryDateTimeUnit) {
    for (const int p : {0, 3, 6, 9}) {
        for (const std::string zone : {"", " WITH TIME ZONE"}) {
            const std::string sql = "TIMESTAMP(" + std::to_string(p) + ")" + zone;
            for (const std::string dt : {"DateTime", "DateTime('UTC')"}) {
                const ColumnBinding b = plan_accepts(sql, dt);
                EXPECT_EQ(b.conversion, Conversion::TimestampToDateTime) << sql << " into " << dt;
                EXPECT_EQ(b.multiplier, 1);
                EXPECT_EQ(b.divisor, 1000);
            }
            for (int target = 0; target <= 9; ++target) {
                for (const std::string tz : {"", ", 'Asia/Tokyo'"}) {
                    const std::string ch = "DateTime64(" + std::to_string(target) + tz + ")";
                    const ColumnBinding b = plan_accepts(sql, ch);
                    EXPECT_EQ(b.conversion, Conversion::TimestampToDateTime64)
                        << sql << " into " << ch;
                    const std::int64_t multipliers[] = {
                        1, 1, 1, 1, 10, 100, 1000, 10000, 100000, 1000000};
                    const std::int64_t divisors[] = {1000, 100, 10, 1, 1, 1, 1, 1, 1, 1};
                    EXPECT_EQ(b.multiplier, multipliers[target]) << sql << " into " << ch;
                    EXPECT_EQ(b.divisor, divisors[target]) << sql << " into " << ch;
                    EXPECT_EQ(b.expected_header_type, ch);
                }
            }
        }
    }
}

TEST(NativeColumnPlan, TemporalTypesDoNotCrossOver) {
    EXPECT_EQ(plan_refuses("DATE", "DateTime"),
              "column `c`: DATE into DateTime is not supported; CAST in the SELECT");
    EXPECT_EQ(plan_refuses("TIMESTAMP(3)", "Date32"),
              "column `c`: TIMESTAMP(3) into Date32 is not supported; CAST in the SELECT");
    EXPECT_EQ(plan_refuses("BIGINT", "DateTime64(3)"),
              "column `c`: BIGINT into DateTime64(3) is not supported; CAST in the SELECT");
}

TEST(NativeColumnPlan, ArraysBindTheirElement) {
    const ColumnBinding b = plan_accepts("BIGINT ARRAY", "Array(Nullable(Int32))");
    EXPECT_EQ(b.conversion, Conversion::List);
    EXPECT_EQ(b.expected_header_type, "Array(Nullable(Int32))");
    ASSERT_EQ(b.children.size(), 1U);
    EXPECT_EQ(b.children[0].name, "c.element");
    EXPECT_EQ(b.children[0].conversion, Conversion::NarrowInt);
    EXPECT_TRUE(b.children[0].target.nullable);

    const ColumnBinding nested = plan_accepts("VARCHAR ARRAY ARRAY", "Array(Array(String))");
    ASSERT_EQ(nested.children.size(), 1U);
    EXPECT_EQ(nested.children[0].conversion, Conversion::List);
    ASSERT_EQ(nested.children[0].children.size(), 1U);
    EXPECT_EQ(nested.children[0].children[0].conversion, Conversion::StringZeroCopy);
    EXPECT_EQ(nested.children[0].children[0].name, "c.element.element");

    EXPECT_EQ(plan_refuses("DOUBLE ARRAY", "Array(Float32)"),
              "column `c`, element: DOUBLE into Float32 narrows; use REAL in the clink table or "
              "CAST in the SELECT");
    EXPECT_EQ(plan_refuses("BIGINT ARRAY", "Int64"),
              "column `c`: BIGINT ARRAY into Int64 is not supported; CAST in the SELECT");
    EXPECT_EQ(plan_refuses("BIGINT", "Array(Int64)"),
              "column `c`: BIGINT into Array(Int64) is not supported; CAST in the SELECT");
}

TEST(NativeColumnPlan, MapsBindKeyAndValue) {
    const ColumnBinding b =
        plan_accepts("MAP<VARCHAR, BIGINT>", "Map(LowCardinality(String), UInt64)");
    EXPECT_EQ(b.conversion, Conversion::Map);
    ASSERT_EQ(b.children.size(), 2U);
    EXPECT_EQ(b.children[0].name, "c.key");
    EXPECT_EQ(b.children[0].conversion, Conversion::StringZeroCopy);
    EXPECT_TRUE(b.children[0].zero_copy);
    EXPECT_EQ(b.children[1].name, "c.value");
    EXPECT_EQ(b.children[1].input_index, 1);
    EXPECT_EQ(b.children[1].conversion, Conversion::SignedToUnsigned);

    const PlanResult both = plan_one("MAP<DOUBLE, DOUBLE>", "Map(Float32, Float32)");
    ASSERT_EQ(both.problems.size(), 2U);
    EXPECT_EQ(both.problems[0].message,
              "column `c`, key: DOUBLE into Float32 narrows; use REAL in the clink table or CAST "
              "in the SELECT");
    EXPECT_EQ(both.problems[1].message,
              "column `c`, value: DOUBLE into Float32 narrows; use REAL in the clink table or "
              "CAST in the SELECT");
}

TEST(NativeColumnPlan, RowsBindToTuplesByPosition) {
    const ColumnBinding named =
        plan_accepts("ROW<a BIGINT, b VARCHAR>", "Tuple(a Int64, b String)");
    EXPECT_EQ(named.conversion, Conversion::Struct);
    ASSERT_EQ(named.children.size(), 2U);
    EXPECT_EQ(named.children[0].name, "c.a");
    EXPECT_EQ(named.children[0].input_index, 0);
    EXPECT_EQ(named.children[0].conversion, Conversion::Copy);
    EXPECT_EQ(named.children[1].name, "c.b");
    EXPECT_EQ(named.children[1].input_index, 1);
    EXPECT_EQ(named.children[1].conversion, Conversion::StringZeroCopy);

    const ColumnBinding unnamed = plan_accepts("ROW<a BIGINT, b VARCHAR>", "Tuple(Int64, String)");
    EXPECT_EQ(unnamed.conversion, Conversion::Struct);
    EXPECT_EQ(unnamed.children.size(), 2U);

    EXPECT_EQ(plan_refuses("ROW<a BIGINT, b VARCHAR>", "Tuple(a Int64, c String)"),
              "column `c`: field 2 is `b` in the clink table and `c` in Tuple(a Int64, c "
              "String); rename one so that they match");
    EXPECT_EQ(plan_refuses("ROW<a BIGINT, b VARCHAR>", "Tuple(Int64)"),
              "column `c`: ROW<a BIGINT, b VARCHAR> has 2 fields and Tuple(Int64) has 1; the "
              "clink ROW and the Tuple must have the same fields");
    EXPECT_EQ(plan_refuses("ROW<a BIGINT, b DOUBLE>", "Tuple(a Int64, b Float32)"),
              "column `c`, field `b`: DOUBLE into Float32 narrows; use REAL in the clink table or "
              "CAST in the SELECT");
}

TEST(NativeColumnPlan, DeclaredTypesWithoutAMappingAreRefused) {
    EXPECT_EQ(plan_refuses("BYTEA", "String"),
              "column `c`: BYTEA has no mapping in the native sink; leave the column out of the "
              "table or declare it as another type");
    EXPECT_EQ(plan_refuses("TIME", "Int64"),
              "column `c`: TIME has no mapping in the native sink; leave the column out of the "
              "table or declare it as another type");
    EXPECT_EQ(plan_refuses("large_string", "String"),
              "column `c`: the clink type large_string is not supported by the native sink");
    EXPECT_EQ(plan_refuses("MAP<VARCHAR, uint8>", "Map(String, UInt8)"),
              "column `c`, value: the clink type uint8 is not supported by the native sink");
}

TEST(NativeColumnPlan, TargetTypesTheSinkCannotWriteAreRefusedByName) {
    for (const std::string ch : {"Int256",
                                 "UInt256",
                                 "BFloat16",
                                 "Variant(String, UInt64)",
                                 "Dynamic",
                                 "JSON",
                                 "Object('json')",
                                 "Time",
                                 "Time64(3)",
                                 "Point",
                                 "Ring",
                                 "Polygon",
                                 "MultiPolygon",
                                 "AggregateFunction(sum, UInt64)",
                                 "SimpleAggregateFunction(sum, UInt64)",
                                 "Nothing",
                                 "Nullable(Nothing)",
                                 "Unheard"}) {
        const std::string inner = ch == "Nullable(Nothing)" ? "Nothing" : ch;
        const std::string want =
            ch == inner
                ? "column `c`: " + ch + " is not supported by the native sink"
                : "column `c`: " + ch + ": " + inner + " is not supported by the native sink";
        EXPECT_EQ(plan_refuses("VARCHAR", ch), want);
    }
    EXPECT_EQ(plan_refuses("VARCHAR ARRAY", "Array(Int256)"),
              "column `c`: Array(Int256): Int256 is not supported by the native sink");
}

TEST(NativeColumnPlan, HeaderTypesComeFromTheClientsSpelling) {
    EXPECT_EQ(plan_accepts("BOOLEAN", "Bool").expected_header_type, "UInt8");
    EXPECT_EQ(plan_accepts("BOOLEAN", "UInt8").expected_header_type, "UInt8");
    EXPECT_EQ(plan_accepts("DECIMAL(5, 2)", "Decimal32(2)").expected_header_type, "Decimal(9,2)");
    EXPECT_EQ(plan_accepts("TIMESTAMP(3)", "DateTime('UTC')").expected_header_type,
              "DateTime('UTC')");
    EXPECT_EQ(plan_accepts("VARCHAR", "Enum8('b' = 2, 'a' = 1)").expected_header_type,
              "Enum8('a' = 1, 'b' = 2)");
}

TEST(NativeColumnPlan, ReportListsEveryColumnAndEveryOmission) {
    const PlanResult r = compile_column_plan(
        parse_sql_column_types(
            "id:BIGINT;name:VARCHAR;ts:TIMESTAMP(6);amount:DECIMAL(10, 2);tags:VARCHAR ARRAY"),
        {plan_target("id", "Int64"),
         plan_target("name", "LowCardinality(String)"),
         plan_target("ts", "DateTime64(6, 'UTC')"),
         plan_target("amount", "Decimal(12, 4)"),
         plan_target("tags", "Array(String)"),
         plan_target("created", "DateTime", DefaultKind::Default),
         plan_target("day", "Date", DefaultKind::Materialized)});
    ASSERT_TRUE(r.plan.has_value()) << r.problems.at(0).message;
    EXPECT_EQ(r.plan->report(),
              "clickhouse native sink column plan: columns=5 omitted=2 retains_chunks=true\n"
              "  `id`: sql=BIGINT target=Int64 conversion=copy zero_copy=false\n"
              "  `name`: sql=VARCHAR target=LowCardinality(String) conversion=string_zero_copy "
              "zero_copy=true\n"
              "  `ts`: sql=TIMESTAMP(6) target=DateTime64(6, 'UTC') "
              "conversion=timestamp_to_datetime64 multiplier=1000 zero_copy=false\n"
              "  `amount`: sql=DECIMAL(10, 2) target=Decimal(12, 4) conversion=decimal_rescale "
              "multiplier=100 zero_copy=false\n"
              "  `tags`: sql=VARCHAR ARRAY target=Array(String) "
              "conversion=list(string_zero_copy) zero_copy=true\n"
              "  omitted `created`: DEFAULT\n"
              "  omitted `day`: MATERIALIZED");
}

ColumnPlan plan_for_drift() {
    return compile_or_refuse(
        parse_sql_column_types("a:BIGINT;b:BIGINT;c:VARCHAR"),
        {plan_target("a", "Int64"), plan_target("b", "Int64"), plan_target("c", "String")},
        "`db`.`t`");
}

TEST(NativeColumnPlan, NoDriftWhenTheHeaderMatches) {
    EXPECT_TRUE(
        header_drift(plan_for_drift(), {{"a", "Int64"}, {"b", "Int64"}, {"c", "String"}}).empty());
}

TEST(NativeColumnPlan, DriftNamesARetypedColumn) {
    EXPECT_EQ(
        header_drift(plan_for_drift(), {{"a", "Int64"}, {"b", "Nullable(Int64)"}, {"c", "String"}}),
        (std::vector<std::string>{"column `b`: plan Int64, server Nullable(Int64)"}));
}

TEST(NativeColumnPlan, DriftNamesARenamedColumn) {
    EXPECT_EQ(
        header_drift(plan_for_drift(), {{"a", "Int64"}, {"bb", "Int64"}, {"c", "String"}}),
        (std::vector<std::string>{"position 2: plan column `b` Int64, server column `bb` Int64"}));
}

TEST(NativeColumnPlan, DriftNamesAnAddedColumn) {
    EXPECT_EQ(header_drift(plan_for_drift(),
                           {{"a", "Int64"}, {"b", "Int64"}, {"c", "String"}, {"d", "UInt8"}}),
              (std::vector<std::string>{"column `d`: not in the plan, server UInt8"}));
}

TEST(NativeColumnPlan, DriftNamesARemovedColumn) {
    EXPECT_EQ(
        header_drift(plan_for_drift(), {{"a", "Int64"}, {"b", "Int64"}}),
        (std::vector<std::string>{"column `c`: plan String, missing from the server header"}));
}

TEST(NativeColumnPlan, DriftListsEveryDifference) {
    EXPECT_EQ(
        header_drift(plan_for_drift(), {{"a", "Int32"}, {"x", "Int64"}}),
        (std::vector<std::string>{"column `a`: plan Int64, server Int32",
                                  "position 2: plan column `b` Int64, server column `x` Int64",
                                  "column `c`: plan String, missing from the server header"}));
}

// Bool is UInt8 on both sides of the check, because both are the client's
// own spelling.
TEST(NativeColumnPlan, BoolDoesNotDriftAgainstTheClientsUInt8) {
    const ColumnPlan plan = compile_or_refuse(
        parse_sql_column_types("flag:BOOLEAN"), {plan_target("flag", "Bool")}, "`db`.`t`");
    EXPECT_TRUE(header_drift(plan, {{"flag", "UInt8"}}).empty());
}

}  // namespace
}  // namespace clink::clickhouse::native
