// Arrow chunks to Native blocks: every conversion, the offsets an Arrow slice
// carries, the payload and memory measures, the split of a held block, and
// the redacted sample row.

#include <array>
#include <charconv>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/json/from_string.h>
#include <arrow/util/byte_size.h>
#include <clickhouse/base/output.h>
#include <clickhouse/columns/array.h>
#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/uuid.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/columnar_batcher.hpp"
#include "clink/core/record.hpp"

#include "native/arrow_to_block.hpp"
#include "native/column_plan.hpp"
#include "native/errors.hpp"
#include "native/types.hpp"

// A typed struct whose batcher builds the chunk. CLINK_FIELDS specialises a
// clink template, so these live at namespace scope.
struct A2bTypedLeg {
    std::string venue;
    double px;
};
CLINK_FIELDS(A2bTypedLeg, venue, px);

struct A2bTypedTrade {
    std::int64_t id;
    std::uint32_t qty;
    std::optional<std::uint64_t> volume;
    std::vector<std::uint8_t> flags;
    std::map<std::string, std::uint16_t> counts;
    A2bTypedLeg leg;
};
CLINK_FIELDS(A2bTypedTrade, id, qty, volume, flags, counts, leg);

namespace clink::clickhouse::native {
namespace {

namespace ch = ::clickhouse;

// A plan for the declared columns `spec` ("a:BIGINT;b:VARCHAR") into a target
// whose columns carry the same names, typed by `targets` in order.
ColumnPlan a2b_plan(const std::string& spec, const std::vector<std::string>& targets) {
    const auto inputs = parse_sql_column_types(spec);
    std::vector<TargetColumn> table;
    for (std::size_t i = 0; i < inputs.size(); ++i) {
        table.push_back(TargetColumn{
            inputs[i].name, targets.at(i), DefaultKind::None, static_cast<std::uint32_t>(i + 1)});
    }
    PlanResult r = compile_column_plan(inputs, table);
    if (!r.plan) {
        std::string why = "the column plan refused " + spec + ":";
        for (const auto& p : r.problems) {
            why += "\n  " + p.message;
        }
        throw std::runtime_error(why);
    }
    return std::move(*r.plan);
}

std::shared_ptr<arrow::RecordBatch> a2b_batch(const arrow::FieldVector& fields,
                                              const arrow::ArrayVector& arrays) {
    return arrow::RecordBatch::Make(arrow::schema(fields), arrays.front()->length(), arrays);
}

// A chunk laid out as the plan expects `spec`, one JSON array per column.
std::shared_ptr<arrow::RecordBatch> a2b_chunk(const std::string& spec,
                                              const std::vector<std::string>& json) {
    const auto inputs = parse_sql_column_types(spec);
    arrow::FieldVector fields;
    arrow::ArrayVector arrays;
    for (std::size_t i = 0; i < inputs.size(); ++i) {
        auto type = arrow_type_for(inputs[i].type);
        fields.push_back(arrow::field(inputs[i].name, type));
        arrays.push_back(arrow::json::ArrayFromJSONString(type, json.at(i)).ValueOrDie());
    }
    return a2b_batch(fields, arrays);
}

// A one-column chunk holding `array` as column `c`.
std::shared_ptr<arrow::RecordBatch> a2b_single(const std::shared_ptr<arrow::Array>& array) {
    return a2b_batch({arrow::field("c", array->type())}, {array});
}

ch::Block a2b_convert(const ColumnPlan& plan,
                      const arrow::RecordBatch& chunk,
                      std::int64_t offset,
                      std::int64_t length) {
    BlockBuilder builder(plan);
    builder.append(chunk, offset, length);
    return builder.take();
}

std::string a2b_hex(std::uint64_t v, int digits) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out(static_cast<std::size_t>(digits), '0');
    for (int i = digits - 1; i >= 0; --i) {
        out[static_cast<std::size_t>(i)] = kDigits[v & 15U];
        v >>= 4;
    }
    return out;
}

template <typename T>
std::string a2b_shortest(T v) {
    std::array<char, 32> buffer{};
    const auto r = std::to_chars(buffer.data(), buffer.data() + buffer.size(), v);
    return std::string(buffer.data(), r.ptr);
}

template <typename T>
std::string a2b_streamed(const T& v) {
    std::ostringstream out;
    out << v;
    return out.str();
}

// A row of a client column as text: strings quoted, NUL bytes shown as \0,
// decimals as their unscaled integer, dates and times as their raw numbers.
std::string a2b_render(const ch::Column& c, std::size_t row) {
    if (const auto* n = dynamic_cast<const ch::ColumnNullable*>(&c)) {
        return n->IsNull(row) ? "NULL" : a2b_render(*n->Nested(), row);
    }
    if (const auto* a = dynamic_cast<const ch::ColumnArray*>(&c)) {
        const auto data = a->GetData();
        std::string out = "[";
        for (std::size_t k = 0; k < a->GetSize(row); ++k) {
            out += (k == 0 ? "" : ", ") + a2b_render(*data, a->GetOffset(row) + k);
        }
        return out + "]";
    }
    if (const auto* m = dynamic_cast<const ch::ColumnMap*>(&c)) {
        const auto entries = m->GetAsColumn(row)->As<ch::ColumnTuple>();
        std::string out = "{";
        for (std::size_t k = 0; k < entries->Size(); ++k) {
            out += (k == 0 ? "" : ", ") + a2b_render(*entries->At(0), k) + ": " +
                   a2b_render(*entries->At(1), k);
        }
        return out + "}";
    }
    if (const auto* t = dynamic_cast<const ch::ColumnTuple*>(&c)) {
        std::string out = "(";
        for (std::size_t k = 0; k < t->TupleSize(); ++k) {
            out += (k == 0 ? "" : ", ") + a2b_render(*t->At(k), row);
        }
        return out + ")";
    }
    if (const auto* s = dynamic_cast<const ch::ColumnString*>(&c)) {
        return "'" + std::string(s->At(row)) + "'";
    }
    if (const auto* f = dynamic_cast<const ch::ColumnFixedString*>(&c)) {
        std::string out = "'";
        for (const char ch : f->At(row)) {
            out += ch == '\0' ? std::string("\\0") : std::string(1, ch);
        }
        return out + "'";
    }
    if (const auto* e = dynamic_cast<const ch::ColumnEnum8*>(&c)) {
        return std::string(e->NameAt(row));
    }
    if (const auto* e = dynamic_cast<const ch::ColumnEnum16*>(&c)) {
        return std::string(e->NameAt(row));
    }
    if (const auto* u = dynamic_cast<const ch::ColumnUUID*>(&c)) {
        const auto v = u->At(row);
        const std::string h = a2b_hex(v.first, 16);
        const std::string l = a2b_hex(v.second, 16);
        return h.substr(0, 8) + "-" + h.substr(8, 4) + "-" + h.substr(12, 4) + "-" +
               l.substr(0, 4) + "-" + l.substr(4);
    }
    if (const auto* ip = dynamic_cast<const ch::ColumnIPv4*>(&c)) {
        return ip->AsString(row);
    }
    if (const auto* ip = dynamic_cast<const ch::ColumnIPv6*>(&c)) {
        return ip->AsString(row);
    }
    if (const auto* d = dynamic_cast<const ch::ColumnDecimal*>(&c)) {
        return a2b_streamed(d->At(row));
    }
    if (const auto* d = dynamic_cast<const ch::ColumnDateTime64*>(&c)) {
        return std::to_string(d->At(row));
    }
    if (const auto* d = dynamic_cast<const ch::ColumnDateTime*>(&c)) {
        return std::to_string(d->RawAt(row));
    }
    if (const auto* d = dynamic_cast<const ch::ColumnDate*>(&c)) {
        return std::to_string(d->RawAt(row));
    }
    if (const auto* d = dynamic_cast<const ch::ColumnDate32*>(&c)) {
        return std::to_string(d->RawAt(row));
    }
    if (const auto* b = dynamic_cast<const ch::ColumnBool*>(&c)) {
        return b->At(row) ? "true" : "false";
    }
    if (const auto* v = dynamic_cast<const ch::ColumnInt8*>(&c)) {
        return std::to_string(static_cast<int>(v->At(row)));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnInt16*>(&c)) {
        return std::to_string(v->At(row));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnInt32*>(&c)) {
        return std::to_string(v->At(row));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnInt64*>(&c)) {
        return std::to_string(v->At(row));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnInt128*>(&c)) {
        return a2b_streamed(v->At(row));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnUInt8*>(&c)) {
        return std::to_string(static_cast<unsigned>(v->At(row)));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnUInt16*>(&c)) {
        return std::to_string(v->At(row));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnUInt32*>(&c)) {
        return std::to_string(v->At(row));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnUInt64*>(&c)) {
        return std::to_string(v->At(row));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnFloat32*>(&c)) {
        return a2b_shortest(v->At(row));
    }
    if (const auto* v = dynamic_cast<const ch::ColumnFloat64*>(&c)) {
        return a2b_shortest(v->At(row));
    }
    return "?" + c.GetType().GetName();
}

std::vector<std::string> a2b_rows(const ch::ColumnRef& c) {
    std::vector<std::string> out;
    for (std::size_t i = 0; i < c->Size(); ++i) {
        out.push_back(a2b_render(*c, i));
    }
    return out;
}

// Collects what a column writes on the wire.
class A2bWire final : public ch::OutputStream {
public:
    std::vector<std::uint8_t> bytes;

protected:
    std::size_t DoWrite(const void* data, std::size_t len) override {
        const auto* p = static_cast<const std::uint8_t*>(data);
        bytes.insert(bytes.end(), p, p + len);
        return len;
    }
};

std::vector<std::uint8_t> a2b_wire(const ch::ColumnRef& c) {
    A2bWire out;
    c->SaveBody(&out);
    return out.bytes;
}

// The ConversionError a single-column conversion of `json` throws.
std::string a2b_refusal(const std::string& sql,
                        const std::string& target,
                        const std::string& json) {
    const ColumnPlan plan = a2b_plan("c:" + sql, {target});
    const auto chunk = a2b_chunk("c:" + sql, {json});
    BlockBuilder builder(plan);
    try {
        builder.append(*chunk, 0, chunk->num_rows());
    } catch (const ConversionError& e) {
        EXPECT_EQ(e.code(), code::kConversionFailed);
        return e.what();
    }
    ADD_FAILURE() << sql << " into " << target << " accepted " << json;
    return {};
}

std::vector<std::string> a2b_accepts(const std::string& sql,
                                     const std::string& target,
                                     const std::string& json) {
    const ColumnPlan plan = a2b_plan("c:" + sql, {target});
    const auto chunk = a2b_chunk("c:" + sql, {json});
    return a2b_rows(a2b_convert(plan, *chunk, 0, chunk->num_rows())[0]);
}

using A2bRows = std::vector<std::string>;

struct A2bCase {
    std::string sql;
    std::string target;
    std::string json;  // six rows; rows 3 to 5 are converted
    A2bRows expected;
};

// Every converter, reached through a chunk that is itself an Arrow slice
// (every array has offset 1) and appended from a builder offset of 2, so only
// rows 3 to 5 of the JSON may be read. Several cases put values in rows 0 to
// 2 that the target would refuse, so reading the wrong rows fails loudly.
TEST(NativeArrowToBlock, EveryConverterHonoursTheChunkOffsetAndTheArrayOffset) {
    const std::vector<A2bCase> cases = {
        {"TINYINT", "Nullable(Int8)", "[0, 1, 2, -3, null, 127]", {"-3", "NULL", "127"}},
        {"SMALLINT", "Nullable(Int32)", "[0, 1, 2, -300, null, 32767]", {"-300", "NULL", "32767"}},
        {"INTEGER", "Nullable(UInt16)", "[-1, -1, -1, 65535, null, 0]", {"65535", "NULL", "0"}},
        {"INTEGER", "Int8", "[1000, 1000, 1000, -128, 127, 0]", {"-128", "127", "0"}},
        {"SMALLINT", "UInt8", "[-1, -1, -1, 0, 255, 7]", {"0", "255", "7"}},
        {"BIGINT",
         "Nullable(Int64)",
         "[0, 0, 0, 9007199254740993, null, -1]",
         {"9007199254740993", "NULL", "-1"}},
        {"BIGINT",
         "Int128",
         "[0, 0, 0, -9223372036854775808, 0, 9223372036854775807]",
         {"-9223372036854775808", "0", "9223372036854775807"}},
        {"BIGINT",
         "UInt64",
         "[-5, -5, -5, 0, 9223372036854775807, 1]",
         {"0", "9223372036854775807", "1"}},
        {"REAL", "Nullable(Float64)", "[0, 0, 0, 1.5, null, -2.25]", {"1.5", "NULL", "-2.25"}},
        {"REAL", "Float32", "[0, 0, 0, 0.5, 3.25, -1]", {"0.5", "3.25", "-1"}},
        {"DOUBLE", "Float64", "[0, 0, 0, 0.1, 1e300, -0.5]", {"0.1", "1e+300", "-0.5"}},
        {"BOOLEAN",
         "Nullable(Bool)",
         "[true, true, true, false, null, true]",
         {"false", "NULL", "true"}},
        {"BOOLEAN", "UInt8", "[true, true, true, false, true, true]", {"0", "1", "1"}},
        {"VARCHAR",
         "Nullable(String)",
         R"(["a", "b", "c", "dd", null, ""])",
         {"'dd'", "NULL", "''"}},
        {"VARCHAR",
         "LowCardinality(String)",
         R"(["a", "b", "c", "x", "y", "z"])",
         {"'x'", "'y'", "'z'"}},
        {"VARCHAR",
         "LowCardinality(Nullable(String))",
         R"(["a", "b", "c", "x", null, "z"])",
         {"'x'", "NULL", "'z'"}},
        {"VARCHAR",
         "FixedString(4)",
         R"(["toolong", "toolong", "toolong", "ab", "abcd", ""])",
         {"'ab\\0\\0'", "'abcd'", "'\\0\\0\\0\\0'"}},
        {"VARCHAR",
         "Nullable(Enum8('a' = 1, 'b' = 2))",
         R"(["q", "q", "q", "b", null, "a"])",
         {"b", "NULL", "a"}},
        {"VARCHAR",
         "Enum16('lo' = -300, 'hi' = 300)",
         R"(["q", "q", "q", "hi", "lo", "hi"])",
         {"hi", "lo", "hi"}},
        {"VARCHAR",
         "UUID",
         R"(["bad", "bad", "bad", "61f0c404-5cb3-11e7-907b-a6006ad3dba0",
             "00000000-0000-0000-0000-000000000001", "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"])",
         {"61f0c404-5cb3-11e7-907b-a6006ad3dba0",
          "00000000-0000-0000-0000-000000000001",
          "ffffffff-ffff-ffff-ffff-ffffffffffff"}},
        {"VARCHAR",
         "IPv4",
         R"(["bad", "bad", "bad", "1.2.3.4", "255.255.255.255", "0.0.0.0"])",
         {"1.2.3.4", "255.255.255.255", "0.0.0.0"}},
        {"VARCHAR",
         "Nullable(IPv6)",
         R"(["bad", "bad", "bad", "::1", null, "10.0.0.1"])",
         {"::1", "NULL", "::ffff:10.0.0.1"}},
        {"DECIMAL(10, 2)",
         "Nullable(Decimal(12, 4))",
         R"(["1.00", "1.00", "1.00", "123.45", null, "-0.01"])",
         {"1234500", "NULL", "-100"}},
        {"TIMESTAMP(3)",
         "Nullable(DateTime64(3))",
         "[0, 0, 0, 1700000000123, null, -1]",
         {"1700000000123", "NULL", "-1"}},
        {"TIMESTAMP(6)",
         "DateTime64(6)",
         "[0, 0, 0, 1700000000123, 0, -5]",
         {"1700000000123000", "0", "-5000"}},
        {"TIMESTAMP(3) WITH TIME ZONE",
         "DateTime('UTC')",
         "[1, 1, 1, 1700000000000, 0, 4294967295000]",
         {"1700000000", "0", "4294967295"}},
        {"DATE",
         "Nullable(Date32)",
         "[0, 0, 0, -25567, null, 120529]",
         {"-25567", "NULL", "120529"}},
        {"DATE", "Date", "[-1, -1, -1, 0, 19000, 65535]", {"0", "19000", "65535"}},
        {"BIGINT ARRAY",
         "Array(Int64)",
         "[[1], [2], [3], [4, 5], [], [6]]",
         {"[4, 5]", "[]", "[6]"}},
        {"VARCHAR ARRAY",
         "Array(Nullable(String))",
         R"([["a"], ["b"], ["c"], ["x", null], [], ["y"]])",
         {"['x', NULL]", "[]", "['y']"}},
        {"MAP<VARCHAR, BIGINT>",
         "Map(String, Int64)",
         R"([[["a", 1]], [], [], [["k", 1], ["j", 2]], [], [["z", 3]]])",
         {"{'k': 1, 'j': 2}", "{}", "{'z': 3}"}},
        {"ROW<a BIGINT, b VARCHAR>",
         "Tuple(a Int64, b String)",
         R"([{"a": 1, "b": "p"}, {"a": 2, "b": "q"}, {"a": 3, "b": "r"},
             {"a": 4, "b": "d"}, {"a": 5, "b": ""}, {"a": 6, "b": "f"}])",
         {"(4, 'd')", "(5, '')", "(6, 'f')"}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.sql + " into " + c.target);
        const ColumnPlan plan = a2b_plan("c:" + c.sql, {c.target});
        const auto parent = a2b_chunk("c:" + c.sql, {c.json});
        const auto chunk = parent->Slice(1);
        ASSERT_EQ(chunk->column(0)->offset(), 1);
        const ch::Block block = a2b_convert(plan, *chunk, 2, 3);
        EXPECT_EQ(block.GetRowCount(), 3U);
        EXPECT_EQ(block.GetColumnName(0), "c");
        EXPECT_EQ(a2b_rows(block[0]), c.expected);
    }
}

TEST(NativeArrowToBlock, ListOffsetsAreRebasedForASliceOfASlicedChunk) {
    const ColumnPlan plan = a2b_plan("c:BIGINT ARRAY", {"Array(Int64)"});
    const auto parent = a2b_chunk("c:BIGINT ARRAY", {"[[1], [2, 3], [], [4, 5, 6], [7], [8, 9]]"});
    const auto chunk = parent->Slice(1);  // [2, 3], [], [4, 5, 6], [7], [8, 9]
    BlockBuilder builder(plan);
    builder.append(*chunk, 2, 3);  // [4, 5, 6], [7], [8, 9]
    builder.append(*chunk, 0, 2);  // [2, 3], []
    const ch::Block block = builder.take();
    const auto array = block[0]->As<ch::ColumnArray>();
    ASSERT_NE(array, nullptr);
    EXPECT_EQ(array->GetOffsets()->GetWritableData(), (std::vector<std::uint64_t>{3, 4, 6, 8, 8}));
    EXPECT_EQ(array->GetData()->As<ch::ColumnInt64>()->GetWritableData(),
              (std::vector<std::int64_t>{4, 5, 6, 7, 8, 9, 2, 3}));
    EXPECT_EQ(a2b_rows(block[0]), (A2bRows{"[4, 5, 6]", "[7]", "[8, 9]", "[2, 3]", "[]"}));
}

TEST(NativeArrowToBlock, ListWhoseValuesArrayHasItsOwnOffset) {
    const auto values =
        arrow::json::ArrayFromJSONString(arrow::int64(), "[100, 101, 1, 2, 3, 4, 5]").ValueOrDie();
    const auto offsets =
        arrow::json::ArrayFromJSONString(arrow::int32(), "[0, 2, 2, 5]").ValueOrDie();
    const auto list = arrow::ListArray::FromArrays(*offsets, *values->Slice(2)).ValueOrDie();
    ASSERT_EQ(list->values()->offset(), 2);
    const auto chunk = a2b_single(list)->Slice(1);  // [], [3, 4, 5]
    const ColumnPlan plan = a2b_plan("c:BIGINT ARRAY", {"Array(Int64)"});
    const ch::Block block = a2b_convert(plan, *chunk, 0, 2);
    EXPECT_EQ(a2b_rows(block[0]), (A2bRows{"[]", "[3, 4, 5]"}));
    EXPECT_EQ(block[0]->As<ch::ColumnArray>()->GetOffsets()->GetWritableData(),
              (std::vector<std::uint64_t>{0, 3}));
}

TEST(NativeArrowToBlock, ValidityBitmapIsReadAtTheArraysOwnBitOffset) {
    const ColumnPlan plan = a2b_plan("c:BIGINT", {"Nullable(Int64)"});
    // Nulls at rows 3, 8, 9 and 15; the slice starts at bit 3 of the bitmap,
    // so every row the builder reads sits at an unaligned bit.
    const auto parent = a2b_chunk(
        "c:BIGINT",
        {"[0, 1, 2, null, 4, 5, 6, 7, null, null, 10, 11, 12, 13, 14, null, 16, 17, 18, 19]"});
    const auto chunk = parent->Slice(3);
    const ch::Block block = a2b_convert(plan, *chunk, 2, 12);  // rows 5 to 16
    const auto nullable = block[0]->As<ch::ColumnNullable>();
    ASSERT_NE(nullable, nullptr);
    EXPECT_EQ(nullable->Nulls()->As<ch::ColumnUInt8>()->GetWritableData(),
              (std::vector<std::uint8_t>{0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0}));
    // The nested column holds a 0 placeholder under every null.
    EXPECT_EQ(nullable->Nested()->As<ch::ColumnInt64>()->GetWritableData(),
              (std::vector<std::int64_t>{5, 6, 7, 0, 0, 10, 11, 12, 13, 14, 0, 16}));
}

TEST(NativeArrowToBlock, NullInANonNullableColumnNamesTheColumnAndTheChunkRow) {
    const ColumnPlan plan = a2b_plan("id:BIGINT;name:VARCHAR", {"Int64", "String"});
    const auto parent = a2b_chunk("id:BIGINT;name:VARCHAR",
                                  {"[0, 1, 2, 3, 4, 5]", R"(["a", "b", "c", "d", null, "f"])"});
    const auto chunk = parent->Slice(1);  // the null is chunk row 3
    BlockBuilder builder(plan);
    try {
        builder.append(*chunk, 1, 4);
        FAIL() << "a null went into a non-Nullable column";
    } catch (const ConversionError& e) {
        EXPECT_EQ(e.code(), code::kConversionFailed);
        EXPECT_EQ(e.column(), "name");
        EXPECT_EQ(e.row(), 3);
        EXPECT_STREQ(e.what(),
                     "[clickhouse.conversion_failed] column `name`, row 3: null in non-Nullable "
                     "column `name`");
    }
    // The columns no longer line up, so the builder refuses until reset.
    EXPECT_THROW(builder.append(*chunk, 0, 1), std::logic_error);
    EXPECT_THROW((void)builder.take(), std::logic_error);
    builder.reset();
    EXPECT_EQ(builder.rows(), 0U);
    EXPECT_EQ(builder.payload_bytes(), 0U);
    builder.append(*chunk, 0, 2);
    const ch::Block block = builder.take();
    EXPECT_EQ(a2b_rows(block[0]), (A2bRows{"1", "2"}));
    EXPECT_EQ(a2b_rows(block[1]), (A2bRows{"'b'", "'c'"}));
}

TEST(NativeArrowToBlock, AFailureInsideACompositeNamesTheElementAndItsParentRow) {
    EXPECT_EQ(a2b_refusal("BIGINT ARRAY", "Array(Int64)", "[[1], [2, 3], [4, null]]"),
              "[clickhouse.conversion_failed] column `c.element`, row 2: null in non-Nullable "
              "column `c.element`");
    EXPECT_EQ(a2b_refusal("BIGINT ARRAY", "Array(Int8)", "[[], [1, 2], [], [3, 300]]"),
              "[clickhouse.conversion_failed] column `c.element`, row 3: value out of range for "
              "Int8");
    EXPECT_EQ(
        a2b_refusal(
            "MAP<VARCHAR, BIGINT>", "Map(String, Int8)", R"([[["a", 1]], [["b", 2], ["c", 300]]])"),
        "[clickhouse.conversion_failed] column `c.value`, row 1: value out of range for "
        "Int8");
    EXPECT_EQ(a2b_refusal("ROW<a BIGINT, b VARCHAR>",
                          "Tuple(a Int64, b UUID)",
                          R"([{"a": 1, "b": "61f0c404-5cb3-11e7-907b-a6006ad3dba0"},
                              {"a": 2, "b": "nope"}])"),
              "[clickhouse.conversion_failed] column `c.b`, row 1: text of 4 bytes is not a UUID "
              "in the 8-4-4-4-12 hexadecimal form");
    EXPECT_EQ(a2b_refusal("BIGINT ARRAY ARRAY", "Array(Array(UInt8))", "[[[1]], [[], [2, -3]]]"),
              "[clickhouse.conversion_failed] column `c.element.element`, row 1: value out of "
              "range for UInt8");
}

TEST(NativeArrowToBlock, NullCompositesAreRefused) {
    EXPECT_EQ(a2b_refusal("BIGINT ARRAY", "Array(Int64)", "[[1], null]"),
              "[clickhouse.conversion_failed] column `c`, row 1: Array cannot be NULL");
    EXPECT_EQ(a2b_refusal("MAP<VARCHAR, BIGINT>", "Map(String, Int64)", "[null]"),
              "[clickhouse.conversion_failed] column `c`, row 0: Map cannot be NULL");
    EXPECT_EQ(a2b_refusal("ROW<a BIGINT>", "Tuple(a Int64)", R"([{"a": 1}, {"a": 2}, null])"),
              "[clickhouse.conversion_failed] column `c`, row 2: Tuple cannot be NULL");
}

TEST(NativeArrowToBlock, IntegerNarrowingIsCheckedPerValue) {
    EXPECT_EQ(a2b_refusal("BIGINT", "Int16", "[1, 40000]"),
              "[clickhouse.conversion_failed] column `c`, row 1: value out of range for Int16");
    EXPECT_EQ(a2b_refusal("INTEGER", "UInt32", "[-1]"),
              "[clickhouse.conversion_failed] column `c`, row 0: value out of range for UInt32");
    EXPECT_EQ(a2b_refusal("BIGINT", "UInt64", "[0, -9223372036854775808]"),
              "[clickhouse.conversion_failed] column `c`, row 1: value out of range for UInt64");
    EXPECT_EQ(a2b_accepts("BIGINT", "Int16", "[-32768, 32767]"), (A2bRows{"-32768", "32767"}));
    EXPECT_EQ(a2b_accepts("TINYINT", "Int64", "[-128, 127]"), (A2bRows{"-128", "127"}));
}

TEST(NativeArrowToBlock, TwoToThe53PlusOneStaysExact) {
    const std::string json = "[9007199254740993, -9007199254740993]";
    const A2bRows exact = {"9007199254740993", "-9007199254740993"};
    EXPECT_EQ(a2b_accepts("BIGINT", "Int64", json), exact);
    EXPECT_EQ(a2b_accepts("BIGINT", "Nullable(Int64)", json), exact);
    EXPECT_EQ(a2b_accepts("BIGINT", "Int128", json), exact);
    EXPECT_EQ(a2b_accepts("BIGINT", "UInt64", "[9007199254740993]"), (A2bRows{"9007199254740993"}));

    const ColumnPlan plan = a2b_plan("c:BIGINT", {"Int64"});
    const auto chunk = a2b_chunk("c:BIGINT", {"[9007199254740993]"});
    EXPECT_EQ(a2b_wire(a2b_convert(plan, *chunk, 0, 1)[0]),
              (std::vector<std::uint8_t>{0x01, 0, 0, 0, 0, 0, 0x20, 0}));
}

TEST(NativeArrowToBlock, DecimalIsRescaledExactly) {
    EXPECT_EQ(a2b_accepts("DECIMAL(10, 2)",
                          "Decimal(12, 4)",
                          R"(["123.45", "-0.01", "99999999.99", "-99999999.99"])"),
              (A2bRows{"1234500", "-100", "999999999900", "-999999999900"}));
    EXPECT_EQ(a2b_accepts("DECIMAL(10, 2)", "Decimal64(2)", R"(["123.45"])"), (A2bRows{"12345"}));
    EXPECT_EQ(a2b_accepts("DECIMAL(5, 0)", "Decimal(38, 0)", R"(["99999"])"), (A2bRows{"99999"}));
}

// The open rule makes every declared value fit, so these reach the backstop
// through Arrow values that break their own declared precision.
TEST(NativeArrowToBlock, DecimalOutOfRangeOrOverflowingIsRefused) {
    const auto convert = [](int p, int s, const std::string& target, const arrow::Decimal128& v) {
        arrow::Decimal128Builder builder(arrow::decimal128(p, s));
        EXPECT_TRUE(builder.Append(v).ok());
        const auto chunk = a2b_single(builder.Finish().ValueOrDie());
        const ColumnPlan plan =
            a2b_plan("c:DECIMAL(" + std::to_string(p) + ", " + std::to_string(s) + ")", {target});
        BlockBuilder block(plan);
        block.append(*chunk, 0, 1);
        return a2b_rows(block.take()[0]);
    };
    const auto ten_to_20 = arrow::Decimal128::FromString("100000000000000000000").ValueOrDie();
    const auto max_20 = arrow::Decimal128::FromString("99999999999999999999").ValueOrDie();

    EXPECT_EQ(convert(20, 0, "Decimal(38, 18)", max_20),
              (A2bRows{"99999999999999999999000000000000000000"}));
    EXPECT_EQ(convert(20, 0, "Decimal(38, 18)", -max_20),
              (A2bRows{"-99999999999999999999000000000000000000"}));
    const std::string refused =
        "[clickhouse.conversion_failed] column `c`, row 0: value out of range for Decimal(38, 18)";
    try {
        (void)convert(20, 0, "Decimal(38, 18)", ten_to_20);
        ADD_FAILURE() << "10^38 went into Decimal(38, 18)";
    } catch (const ConversionError& e) {
        EXPECT_EQ(e.what(), refused);
    }
    try {
        // 2^120 times 10^18 overflows 128 bits.
        (void)convert(20, 0, "Decimal(38, 18)", arrow::Decimal128(std::int64_t{1} << 56, 0));
        ADD_FAILURE() << "an overflowing product was accepted";
    } catch (const ConversionError& e) {
        EXPECT_EQ(e.what(), refused);
    }
    try {
        // 10^11 at scale 2 has more integer digits than DECIMAL(10, 2) declares.
        (void)convert(
            10, 2, "Decimal(12, 4)", arrow::Decimal128::FromString("100000000000").ValueOrDie());
        ADD_FAILURE() << "a value past the target precision was accepted";
    } catch (const ConversionError& e) {
        EXPECT_STREQ(
            e.what(),
            "[clickhouse.conversion_failed] column `c`, row 0: value out of range for Decimal(12, "
            "4)");
    }
}

TEST(NativeArrowToBlock, MillisecondsAreMultipliedIntoFinerDateTime64) {
    const std::string json = "[1700000000123, -1, 0]";
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime64(3)", json),
              (A2bRows{"1700000000123", "-1", "0"}));
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime64(6)", json),
              (A2bRows{"1700000000123000", "-1000", "0"}));
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime64(9, 'UTC')", json),
              (A2bRows{"1700000000123000000", "-1000000", "0"}));
    // The declared precision says nothing about the value, which is
    // milliseconds whatever p.
    EXPECT_EQ(a2b_accepts("TIMESTAMP(0)", "DateTime64(3)", json),
              (A2bRows{"1700000000123", "-1", "0"}));
    EXPECT_EQ(a2b_accepts("TIMESTAMP(9) WITH TIME ZONE", "DateTime64(3)", json),
              (A2bRows{"1700000000123", "-1", "0"}));
}

TEST(NativeArrowToBlock, CoarserTargetsTakeExactMultiplesAndRefuseTheRest) {
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime64(0)", "[1700000000000, -2000, 0]"),
              (A2bRows{"1700000000", "-2", "0"}));
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime", "[1700000000000, 0]"),
              (A2bRows{"1700000000", "0"}));
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime64(1)", "[1700000000100, -100]"),
              (A2bRows{"17000000001", "-1"}));

    EXPECT_EQ(a2b_refusal("TIMESTAMP(3)", "DateTime64(0)", "[1700000000000, 1700000000123]"),
              "[clickhouse.conversion_failed] column `c`, row 1: 1700000000123 ms has sub-second "
              "digits DateTime64(0) cannot hold; target DateTime64(3) or write whole seconds");
    EXPECT_EQ(a2b_refusal("TIMESTAMP(3)", "DateTime", "[1700000000123]"),
              "[clickhouse.conversion_failed] column `c`, row 0: 1700000000123 ms has sub-second "
              "digits DateTime cannot hold; target DateTime64(3) or write whole seconds");
    // A negative value is never floored to the second below.
    EXPECT_EQ(a2b_refusal("TIMESTAMP(3)", "DateTime64(0)", "[-1500]"),
              "[clickhouse.conversion_failed] column `c`, row 0: -1500 ms has sub-second digits "
              "DateTime64(0) cannot hold; target DateTime64(3) or write whole seconds");
    EXPECT_EQ(a2b_refusal("TIMESTAMP(3)", "DateTime64(1)", "[1700000000150]"),
              "[clickhouse.conversion_failed] column `c`, row 0: 1700000000150 ms has sub-second "
              "digits DateTime64(1) cannot hold; target DateTime64(3) or write whole seconds");
    EXPECT_EQ(a2b_refusal("TIMESTAMP(3)", "DateTime", "[-1000]"),
              "[clickhouse.conversion_failed] column `c`, row 0: -1000 ms is outside the range "
              "of DateTime, 1970-01-01 00:00:00 to 2106-02-07 06:28:15");
}

// The columnar path may deliver other units; the unit comes from the Arrow
// type, not from the plan.
TEST(NativeArrowToBlock, TimestampUnitIsReadFromTheArrowType) {
    const ColumnPlan plan = a2b_plan("c:TIMESTAMP(3)", {"DateTime64(3)"});
    const auto seconds = arrow::json::ArrayFromJSONString(arrow::timestamp(arrow::TimeUnit::SECOND),
                                                          "[1700000000, -1]")
                             .ValueOrDie();
    EXPECT_EQ(a2b_rows(a2b_convert(plan, *a2b_single(seconds), 0, 2)[0]),
              (A2bRows{"1700000000000", "-1000"}));
    const auto micros = arrow::json::ArrayFromJSONString(arrow::timestamp(arrow::TimeUnit::MICRO),
                                                         "[1700000000123000, 1700000000123456]")
                            .ValueOrDie();
    BlockBuilder builder(plan);
    try {
        builder.append(*a2b_single(micros), 0, 2);
        FAIL() << "microseconds went into DateTime64(3)";
    } catch (const ConversionError& e) {
        EXPECT_STREQ(e.what(),
                     "[clickhouse.conversion_failed] column `c`, row 1: 1700000000123456 us has "
                     "sub-second digits DateTime64(3) cannot hold; target DateTime64(6) or write "
                     "whole seconds");
    }
}

TEST(NativeArrowToBlock, DateTime64BoundsAreTheTargetsRangeInItsOwnUnits) {
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime64(3)", "[-2208988800000, 10413791999999]"),
              (A2bRows{"-2208988800000", "10413791999999"}));
    EXPECT_EQ(a2b_refusal("TIMESTAMP(3)", "DateTime64(3)", "[-2208988800001]"),
              "[clickhouse.conversion_failed] column `c`, row 0: -2208988800001 ms is outside the "
              "range of DateTime64(3), 1900-01-01 00:00:00 to 2299-12-31 23:59:59");
    EXPECT_EQ(a2b_refusal("TIMESTAMP(3)", "DateTime64(3)", "[10413792000000]"),
              "[clickhouse.conversion_failed] column `c`, row 0: 10413792000000 ms is outside the "
              "range of DateTime64(3), 1900-01-01 00:00:00 to 2299-12-31 23:59:59");
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime64(0)", "[-2208988800000, 10413791999000]"),
              (A2bRows{"-2208988800", "10413791999"}));
    // At nanoseconds int64 ends in 2262, before the documented range does.
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime64(9)", "[9223372036854, -2208988800000]"),
              (A2bRows{"9223372036854000000", "-2208988800000000000"}));
    EXPECT_EQ(a2b_refusal("TIMESTAMP(3)", "DateTime64(9)", "[9223372036855]"),
              "[clickhouse.conversion_failed] column `c`, row 0: 9223372036855 ms is outside the "
              "range of DateTime64(9), 1900-01-01 00:00:00 to 2299-12-31 23:59:59");
    EXPECT_EQ(a2b_accepts("TIMESTAMP(3)", "DateTime", "[4294967295000]"), (A2bRows{"4294967295"}));
    EXPECT_EQ(a2b_refusal("TIMESTAMP(3)", "DateTime", "[4294967296000]"),
              "[clickhouse.conversion_failed] column `c`, row 0: 4294967296000 ms is outside the "
              "range of DateTime, 1970-01-01 00:00:00 to 2106-02-07 06:28:15");
}

TEST(NativeArrowToBlock, DateBoundsAreChecked) {
    EXPECT_EQ(a2b_accepts("DATE", "Date32", "[-25567, 0, 120529]"),
              (A2bRows{"-25567", "0", "120529"}));
    EXPECT_EQ(a2b_refusal("DATE", "Date32", "[-25568]"),
              "[clickhouse.conversion_failed] column `c`, row 0: -25568 days is outside the range "
              "of Date32, -25567 to 120529 days (1900-01-01 to 2299-12-31)");
    EXPECT_EQ(a2b_refusal("DATE", "Date32", "[0, 120530]"),
              "[clickhouse.conversion_failed] column `c`, row 1: 120530 days is outside the range "
              "of Date32, -25567 to 120529 days (1900-01-01 to 2299-12-31)");
    EXPECT_EQ(a2b_accepts("DATE", "Date", "[0, 65535]"), (A2bRows{"0", "65535"}));
    EXPECT_EQ(a2b_refusal("DATE", "Date", "[-1]"),
              "[clickhouse.conversion_failed] column `c`, row 0: -1 days is outside the range of "
              "Date, 0 to 65535 days (1970-01-01 to 2149-06-06)");
    EXPECT_EQ(a2b_refusal("DATE", "Date", "[65536]"),
              "[clickhouse.conversion_failed] column `c`, row 0: 65536 days is outside the range "
              "of Date, 0 to 65535 days (1970-01-01 to 2149-06-06)");
}

TEST(NativeArrowToBlock, ZeroCopyStringsOutliveTheSourceVector) {
    const ColumnPlan plan = a2b_plan("c:VARCHAR", {"String"});
    std::shared_ptr<arrow::RecordBatch> chunk;
    {
        const std::vector<std::string> source = {
            "alpha", "", "a value long enough to live on the heap, not in the string itself"};
        arrow::StringBuilder text;
        for (const auto& s : source) {
            ASSERT_TRUE(text.Append(s).ok());
        }
        chunk = a2b_single(text.Finish().ValueOrDie());
    }
    // Only the chunk's buffers hold the text now; under ASan a view into the
    // vector would be a use after free.
    ch::Block block;
    {
        BlockBuilder builder(plan);
        builder.append(*chunk, 0, 3);
        block = builder.take();
    }
    const auto strings = block[0]->As<ch::ColumnString>();
    ASSERT_NE(strings, nullptr);
    EXPECT_EQ(strings->At(0), "alpha");
    EXPECT_EQ(strings->At(1), "");
    EXPECT_EQ(strings->At(2), "a value long enough to live on the heap, not in the string itself");
    const auto& arrow_text = static_cast<const arrow::StringArray&>(*chunk->column(0));
    const auto* first = reinterpret_cast<const char*>(arrow_text.value_data()->data());
    const auto* last = first + arrow_text.value_data()->size();
    for (const std::size_t i : {std::size_t{0}, std::size_t{2}}) {
        EXPECT_GE(strings->At(i).data(), first) << i;
        EXPECT_LE(strings->At(i).data() + strings->At(i).size(), last) << i;
    }
}

TEST(NativeArrowToBlock, OwnedBytesCountSixteenBytesForEveryReservedStringView) {
    ASSERT_EQ(sizeof(std::string_view), 16U);
    // ColumnString::Reserve(n) also reserves its table of storage blocks, one
    // entry per 16 values: two size_t fields and a pointer.
    const std::size_t entry = 2 * sizeof(std::size_t) + sizeof(char*);
    constexpr std::size_t kValues = 100000;
    arrow::StringBuilder text;
    for (std::size_t i = 0; i < kValues; ++i) {
        ASSERT_TRUE(text.Append("v" + std::to_string(i)).ok());
    }
    const auto chunk = a2b_single(text.Finish().ValueOrDie());
    const auto rows = static_cast<std::int64_t>(kValues);

    BlockBuilder strings(a2b_plan("c:VARCHAR", {"String"}));
    strings.append(*chunk, 0, rows);
    EXPECT_EQ(strings.owned_bytes(), kValues * 16 + (kValues / 16) * entry);
    // A second append grows the reservation to twice the values; the slack
    // beyond what is used is counted too.
    strings.append(*chunk, 0, rows);
    EXPECT_EQ(strings.owned_bytes(), 2 * kValues * 16 + (2 * kValues / 16) * entry);
    (void)strings.take();
    EXPECT_EQ(strings.owned_bytes(), 0U);
    EXPECT_EQ(strings.payload_bytes(), 0U);

    BlockBuilder nullable(a2b_plan("c:VARCHAR", {"Nullable(String)"}));
    nullable.append(*chunk, 0, rows);
    EXPECT_EQ(nullable.owned_bytes(), kValues * 16 + (kValues / 16) * entry + kValues);

    // Inside an Array the element views count the same way, beside the
    // offsets.
    BlockBuilder lists(a2b_plan("c:VARCHAR ARRAY", {"Array(String)"}));
    lists.append(*a2b_chunk("c:VARCHAR ARRAY", {R"([["a", "b"], ["c"]])"}), 0, 2);
    EXPECT_EQ(lists.owned_bytes(), 2 * 8 + 3 * 16 + 1 * entry);
}

TEST(NativeArrowToBlock, OwnedBytesCountFixedWidthCopiesAndTheirGrowthSlack) {
    const ColumnPlan plan = a2b_plan("c:BIGINT", {"Int64"});
    std::string json = "[";
    for (int i = 0; i < 1000; ++i) {
        json += (i == 0 ? "" : ", ") + std::to_string(i);
    }
    const auto chunk = a2b_chunk("c:BIGINT", {json + "]"});
    BlockBuilder builder(plan);
    builder.append(*chunk, 0, 1000);
    EXPECT_EQ(builder.owned_bytes(), 8000U);
    builder.append(*chunk, 0, 1);  // grows by half again, to 1500 values
    EXPECT_EQ(builder.owned_bytes(), 12000U);
    EXPECT_EQ(builder.payload_bytes(), 8008U);
}

TEST(NativeArrowToBlock, FixedStringIsZeroPaddedAndRefusesLongerText) {
    const ColumnPlan plan = a2b_plan("c:VARCHAR", {"FixedString(4)"});
    const auto chunk = a2b_chunk("c:VARCHAR", {R"(["ab", "abcd"])"});
    EXPECT_EQ(a2b_wire(a2b_convert(plan, *chunk, 0, 2)[0]),
              (std::vector<std::uint8_t>{'a', 'b', 0, 0, 'a', 'b', 'c', 'd'}));
    EXPECT_EQ(a2b_accepts("VARCHAR", "Nullable(FixedString(3))", R"([null, "x"])"),
              (A2bRows{"NULL", "'x\\0\\0'"}));
    const std::string refused = a2b_refusal("VARCHAR", "FixedString(4)", R"(["ok", "secret"])");
    EXPECT_EQ(refused,
              "[clickhouse.conversion_failed] column `c`, row 1: string of 6 bytes is longer than "
              "FixedString(4)");
    EXPECT_EQ(refused.find("secret"), std::string::npos);
}

TEST(NativeArrowToBlock, EnumTakesItsValueByExactName) {
    const ColumnPlan plan = a2b_plan("c:VARCHAR", {"Enum8('a' = 1, 'b' = 2)"});
    const auto chunk = a2b_chunk("c:VARCHAR", {R"(["b", "a"])"});
    const auto enums = a2b_convert(plan, *chunk, 0, 2)[0]->As<ch::ColumnEnum8>();
    ASSERT_NE(enums, nullptr);
    EXPECT_EQ(enums->At(0), 2);
    EXPECT_EQ(enums->At(1), 1);
    EXPECT_EQ(enums->Type()->GetName(), "Enum8('a' = 1, 'b' = 2)");

    const auto wide = a2b_plan("c:VARCHAR", {"Enum16('lo' = -300, 'hi' = 300)"});
    const auto wide_chunk = a2b_chunk("c:VARCHAR", {R"(["lo", "hi"])"});
    EXPECT_EQ(a2b_wire(a2b_convert(wide, *wide_chunk, 0, 2)[0]),
              (std::vector<std::uint8_t>{0xd4, 0xfe, 0x2c, 0x01}));

    // A NULL gets the first item underneath, never a value outside the enum.
    const auto nullable = a2b_plan("c:VARCHAR", {"Nullable(Enum8('a' = 1, 'b' = 2))"});
    const auto null_chunk = a2b_chunk("c:VARCHAR", {"[null]"});
    const auto column = a2b_convert(nullable, *null_chunk, 0, 1)[0]->As<ch::ColumnNullable>();
    EXPECT_EQ(column->Nested()->As<ch::ColumnEnum8>()->At(0), 1);

    const std::string refused =
        a2b_refusal("VARCHAR", "Enum8('a' = 1, 'b' = 2)", R"(["a", "classified"])");
    EXPECT_EQ(refused,
              "[clickhouse.conversion_failed] column `c`, row 1: text of 10 bytes is not an item "
              "of Enum8('a' = 1, 'b' = 2)");
    EXPECT_EQ(refused.find("classified"), std::string::npos);
    // Names are matched exactly, case included.
    (void)a2b_refusal("VARCHAR", "Enum8('a' = 1, 'b' = 2)", R"(["A"])");
}

// toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') stores the first 16 digits as
// the first UInt64 and the last 16 as the second, each little-endian.
TEST(NativeArrowToBlock, UuidHalvesFollowTheTextOrder) {
    const ColumnPlan plan = a2b_plan("c:VARCHAR", {"UUID"});
    const auto chunk = a2b_chunk(
        "c:VARCHAR",
        {R"(["61f0c404-5cb3-11e7-907b-a6006ad3dba0", "61F0C404-5CB3-11E7-907B-A6006AD3DBA0"])"});
    const auto column = a2b_convert(plan, *chunk, 0, 2)[0];
    const auto uuids = column->As<ch::ColumnUUID>();
    ASSERT_NE(uuids, nullptr);
    EXPECT_EQ(uuids->At(0), (ch::UUID{0x61f0c4045cb311e7ULL, 0x907ba6006ad3dba0ULL}));
    EXPECT_EQ(uuids->At(1), uuids->At(0));
    const std::vector<std::uint8_t> one = {0xe7,
                                           0x11,
                                           0xb3,
                                           0x5c,
                                           0x04,
                                           0xc4,
                                           0xf0,
                                           0x61,
                                           0xa0,
                                           0xdb,
                                           0xd3,
                                           0x6a,
                                           0x00,
                                           0xa6,
                                           0x7b,
                                           0x90};
    std::vector<std::uint8_t> both = one;
    both.insert(both.end(), one.begin(), one.end());
    EXPECT_EQ(a2b_wire(column), both);

    for (const std::string bad : {"61f0c4045cb311e7907ba6006ad3dba0",
                                  "{61f0c404-5cb3-11e7-907b-a6006ad3dba0}",
                                  "61f0c404-5cb3-11e7-907b-a6006ad3dbaZ",
                                  "61f0c404-5cb311e7-907b-a6006ad3dba0-"}) {
        SCOPED_TRACE(bad);
        EXPECT_EQ(a2b_refusal("VARCHAR", "UUID", "[\"" + bad + "\"]"),
                  "[clickhouse.conversion_failed] column `c`, row 0: text of " +
                      std::to_string(bad.size()) +
                      " bytes is not a UUID in the 8-4-4-4-12 hexadecimal form");
    }
}

TEST(NativeArrowToBlock, Ipv4IsStoredAsTheHostOrderNumber) {
    const ColumnPlan plan = a2b_plan("c:VARCHAR", {"IPv4"});
    const auto chunk = a2b_chunk("c:VARCHAR", {R"(["1.2.3.4", "192.168.0.1"])"});
    const auto column = a2b_convert(plan, *chunk, 0, 2)[0];
    EXPECT_EQ(a2b_wire(column),
              (std::vector<std::uint8_t>{0x04, 0x03, 0x02, 0x01, 0x01, 0x00, 0xa8, 0xc0}));
    const auto addresses = column->As<ch::ColumnIPv4>();
    ASSERT_NE(addresses, nullptr);
    EXPECT_EQ(addresses->AsString(0), "1.2.3.4");
    EXPECT_EQ(addresses->AsString(1), "192.168.0.1");
    for (const std::string bad : {"1.2.3", "256.1.1.1", "1.2.3.4 ", "", "1.2.3.4.5.6.7.8.9.10"}) {
        SCOPED_TRACE(bad);
        EXPECT_EQ(a2b_refusal("VARCHAR", "IPv4", "[\"" + bad + "\"]"),
                  "[clickhouse.conversion_failed] column `c`, row 0: text of " +
                      std::to_string(bad.size()) + " bytes is not a dotted-quad IPv4 address");
    }
}

TEST(NativeArrowToBlock, Ipv6IsParsedFromViewsThatAreNotTerminated) {
    const ColumnPlan plan = a2b_plan("c:VARCHAR", {"IPv6"});
    const auto chunk = a2b_chunk("c:VARCHAR", {R"(["::1", "2001:db8::ff", "1.2.3.4"])"});
    // The values sit back to back in one buffer: "::12001:db8::ff1.2.3.4".
    const auto& text = static_cast<const arrow::StringArray&>(*chunk->column(0));
    ASSERT_EQ(std::string_view(reinterpret_cast<const char*>(text.value_data()->data()), 22),
              "::12001:db8::ff1.2.3.4");
    const auto column = a2b_convert(plan, *chunk, 0, 3)[0];
    EXPECT_EQ(a2b_rows(column), (A2bRows{"::1", "2001:db8::ff", "::ffff:1.2.3.4"}));
    const auto wire = a2b_wire(column);
    ASSERT_EQ(wire.size(), 48U);
    EXPECT_EQ(std::vector<std::uint8_t>(wire.begin(), wire.begin() + 16),
              (std::vector<std::uint8_t>{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}));
    EXPECT_EQ(a2b_refusal("VARCHAR", "IPv6", R"(["2001:db8::zz"])"),
              "[clickhouse.conversion_failed] column `c`, row 0: text of 12 bytes is not an IPv6 "
              "or dotted-quad IPv4 address");
}

TEST(NativeArrowToBlock, NestedListsMapsAndStructs) {
    EXPECT_EQ(a2b_accepts(
                  "BIGINT ARRAY ARRAY", "Array(Array(Int64))", "[[[1, 2], [3]], [], [[]], [[4]]]"),
              (A2bRows{"[[1, 2], [3]]", "[]", "[[]]", "[[4]]"}));
    EXPECT_EQ(a2b_accepts("MAP<VARCHAR, BIGINT ARRAY>",
                          "Map(String, Array(Nullable(Int64)))",
                          R"([[["a", [1, null]], ["b", []]], []])"),
              (A2bRows{"{'a': [1, NULL], 'b': []}", "{}"}));
    EXPECT_EQ(a2b_accepts("ROW<a BIGINT ARRAY, b MAP<VARCHAR, INTEGER>>",
                          "Tuple(a Array(Int64), b Map(String, Int32))",
                          R"([{"a": [1, 2], "b": [["k", 7]]}, {"a": [], "b": []}])"),
              (A2bRows{"([1, 2], {'k': 7})", "([], {})"}));
    EXPECT_EQ(a2b_accepts(
                  "ROW<x BIGINT> ARRAY", "Array(Tuple(x Int64))", R"([[{"x": 1}, {"x": 2}], []])"),
              (A2bRows{"[(1), (2)]", "[]"}));
    EXPECT_EQ(
        a2b_accepts(
            "MAP<BIGINT, VARCHAR>", "Map(Int64, Nullable(String))", R"([[[1, "x"], [2, null]]])"),
        (A2bRows{"{1: 'x', 2: NULL}"}));

    // Slices of a sliced chunk keep nested offsets right at every level.
    const ColumnPlan plan = a2b_plan("c:MAP<VARCHAR, BIGINT ARRAY>", {"Map(String, Array(Int64))"});
    const auto parent = a2b_chunk(
        "c:MAP<VARCHAR, BIGINT ARRAY>",
        {R"([[["p", [9]]], [["q", [8, 8]]], [["a", [1]], ["b", [2, 3]]], [], [["c", [4]]]])"});
    const ch::Block block = a2b_convert(plan, *parent->Slice(1), 1, 3);
    EXPECT_EQ(a2b_rows(block[0]), (A2bRows{"{'a': [1], 'b': [2, 3]}", "{}", "{'c': [4]}"}));
}

TEST(NativeArrowToBlock, ColumnTypesAreTheOnesTheServerShouldSee) {
    const auto type_of =
        [](const std::string& sql, const std::string& target, const std::string& json) {
            const ColumnPlan plan = a2b_plan("c:" + sql, {target});
            const auto chunk = a2b_chunk("c:" + sql, {json});
            return a2b_convert(plan, *chunk, 0, 1)[0]->Type()->GetName();
        };
    // Bool is sent as Bool, though the client reads the header's Bool as
    // UInt8; LowCardinality targets are sent as their nested type.
    EXPECT_EQ(type_of("BOOLEAN", "Bool", "[true]"), "Bool");
    EXPECT_EQ(type_of("BOOLEAN", "UInt8", "[true]"), "UInt8");
    EXPECT_EQ(type_of("VARCHAR", "LowCardinality(String)", R"(["x"])"), "String");
    EXPECT_EQ(type_of("VARCHAR", "LowCardinality(Nullable(String))", R"(["x"])"),
              "Nullable(String)");
    EXPECT_EQ(type_of("VARCHAR", "LowCardinality(FixedString(2))", R"(["x"])"), "FixedString(2)");
    // The rest are exactly the target, time zones and element names included.
    for (const auto& [sql, target, json] : std::vector<std::array<std::string, 3>>{
             {"TIMESTAMP(3)", "DateTime64(3, 'UTC')", "[0]"},
             {"TIMESTAMP(3)", "DateTime('Europe/London')", "[0]"},
             {"DECIMAL(10, 2)", "Decimal(12, 4)", R"(["1.00"])"},
             {"BIGINT", "Nullable(Int128)", "[1]"},
             {"VARCHAR", "Enum16('lo' = -300, 'hi' = 300)", R"(["hi"])"},
             {"VARCHAR", "Nullable(IPv6)", R"(["::1"])"},
             {"ROW<a BIGINT, b VARCHAR>", "Tuple(a Int64, b String)", R"([{"a": 1, "b": "x"}])"},
             {"MAP<VARCHAR, BIGINT>", "Map(String, Int64)", R"([[["k", 1]]])"},
             {"DATE", "Date32", "[0]"},
         }) {
        SCOPED_TRACE(target);
        EXPECT_EQ(type_of(sql, target, json), client_header_spelling(target));
    }
}

TEST(NativeArrowToBlock, PayloadBytesFollowTheBatchMeasure) {
    const std::string spec = "id:BIGINT;name:VARCHAR;tags:INTEGER ARRAY";
    const ColumnPlan plan = a2b_plan(spec, {"Int64", "Nullable(String)", "Array(Int32)"});
    const auto chunk = a2b_chunk(spec, {"[1, 2, 3]", R"(["abc", null, ""])", "[[1, 2], [], [3]]"});
    // id: 3 x 8. name: 3 null flags, then 3 + 1, 0 + 1 and 0 + 1. tags: 3
    // offsets of 8, then 3 elements of 4.
    BlockBuilder builder(plan);
    builder.append(*chunk, 0, 3);
    EXPECT_EQ(builder.payload_bytes(), 24U + 9U + 36U);
    EXPECT_DOUBLE_EQ(builder.bytes_per_row(*chunk), 23.0);
    builder.reset();
    builder.append(*chunk, 1, 2);
    EXPECT_EQ(builder.payload_bytes(), 16U + 4U + 20U);
    EXPECT_DOUBLE_EQ(builder.bytes_per_row(*chunk->Slice(1)), 20.0);
    EXPECT_DOUBLE_EQ(builder.bytes_per_row(*chunk->Slice(3)), 0.0);
}

TEST(NativeArrowToBlock, NoZeroRowBlockIsEverTaken) {
    const ColumnPlan plan = a2b_plan("c:BIGINT", {"Int64"});
    const auto chunk = a2b_chunk("c:BIGINT", {"[1, 2, 3]"});
    BlockBuilder builder(plan);
    EXPECT_THROW((void)builder.take(), std::logic_error);
    builder.append(*chunk, 3, 0);
    EXPECT_EQ(builder.rows(), 0U);
    EXPECT_THROW((void)builder.take(), std::logic_error);
    builder.append(*chunk, 1, 2);
    EXPECT_EQ(builder.rows(), 2U);
    const ch::Block block = builder.take();
    EXPECT_EQ(block.GetRowCount(), 2U);
    EXPECT_EQ(builder.rows(), 0U);
    EXPECT_THROW((void)builder.take(), std::logic_error);
}

TEST(NativeArrowToBlock, RangesOutsideTheChunkAreRefusedWithoutBreakingTheBuilder) {
    const ColumnPlan plan = a2b_plan("c:BIGINT", {"Int64"});
    const auto chunk = a2b_chunk("c:BIGINT", {"[1, 2, 3]"});
    BlockBuilder builder(plan);
    EXPECT_THROW(builder.append(*chunk, 2, 2), std::out_of_range);
    EXPECT_THROW(builder.append(*chunk, -1, 1), std::out_of_range);
    EXPECT_THROW(builder.append(*chunk, 0, -1), std::out_of_range);
    builder.append(*chunk, 2, 1);
    EXPECT_EQ(a2b_rows(builder.take()[0]), (A2bRows{"3"}));
}

TEST(NativeArrowToBlock, AChunkLaidOutAgainstThePlanIsADefectNotABadRow) {
    const ColumnPlan plan = a2b_plan("c:BIGINT", {"Int64"});
    const auto wrong =
        a2b_single(arrow::json::ArrayFromJSONString(arrow::int32(), "[1]").ValueOrDie());
    BlockBuilder builder(plan);
    try {
        builder.append(*wrong, 0, 1);
        FAIL() << "an int32 column was taken for BIGINT";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(),
                     "clickhouse native sink: column `c` arrives as int32, but the plan declares "
                     "it BIGINT");
    }
    // The plan's multiplier assumes the declared decimal scale.
    const ColumnPlan decimals = a2b_plan("c:DECIMAL(10, 2)", {"Decimal(12, 4)"});
    const auto rescaled = a2b_single(
        arrow::json::ArrayFromJSONString(arrow::decimal128(10, 3), R"(["1.000"])").ValueOrDie());
    BlockBuilder decimal_builder(decimals);
    EXPECT_THROW(decimal_builder.append(*rescaled, 0, 1), std::invalid_argument);
    EXPECT_THROW((void)BlockBuilder(plan).bytes_per_row(*wrong), std::invalid_argument);
}

TEST(NativeArrowToBlock, ManySmallAppendsBuildOneConsistentBlock) {
    const std::string spec = "id:BIGINT;name:VARCHAR;tags:BIGINT ARRAY";
    const ColumnPlan plan = a2b_plan(spec, {"Int64", "String", "Array(Int64)"});
    const auto chunk = a2b_chunk(spec, {"[1, 2, 3]", R"(["a", "bb", "ccc"])", "[[1], [], [2, 3]]"});
    BlockBuilder builder(plan);
    for (int i = 0; i < 300; ++i) {
        builder.append(*chunk, i % 3, 1);
    }
    EXPECT_EQ(builder.rows(), 300U);
    EXPECT_GE(builder.owned_bytes(), 300U * (8 + 16 + 8) + 200U * 8);
    const ch::Block block = builder.take();
    ASSERT_EQ(block.GetRowCount(), 300U);
    EXPECT_EQ(a2b_render(*block[1], 299), "'ccc'");
    EXPECT_EQ(a2b_render(*block[2], 298), "[]");
    EXPECT_EQ(a2b_render(*block[2], 299), "[2, 3]");
    EXPECT_EQ(block[2]->As<ch::ColumnArray>()->GetOffsets()->At(299), 300U);
}

TEST(NativeArrowToBlock, AMovedBuilderKeepsItsRows) {
    const ColumnPlan plan = a2b_plan("c:BIGINT", {"Int64"});
    const auto chunk = a2b_chunk("c:BIGINT", {"[1, 2]"});
    BlockBuilder first(plan);
    first.append(*chunk, 0, 2);
    BlockBuilder second(std::move(first));
    EXPECT_EQ(second.rows(), 2U);
    EXPECT_EQ(a2b_rows(second.take()[0]), (A2bRows{"1", "2"}));
}

TEST(NativeArrowToBlock, ChunkBytesCountEveryBufferOnce) {
    const auto chunk = a2b_chunk("id:BIGINT;name:VARCHAR;tags:BIGINT ARRAY",
                                 {"[1, 2, null]", R"(["a", "bb", null])", "[[1], [], [2, 3]]"});
    const std::size_t bytes = chunk_bytes(*chunk);
    EXPECT_EQ(bytes, static_cast<std::size_t>(arrow::util::TotalBufferSize(*chunk)));
    std::size_t summed = 0;
    for (const auto& column : chunk->columns()) {
        for (const auto* data = column->data().get(); data != nullptr;
             data = data->child_data.empty() ? nullptr : data->child_data.front().get()) {
            for (const auto& buffer : data->buffers) {
                summed += buffer ? static_cast<std::size_t>(buffer->size()) : 0;
            }
        }
    }
    EXPECT_EQ(bytes, summed);
    // A slice still holds its parent's buffers, so it costs the same.
    EXPECT_EQ(chunk_bytes(*chunk->Slice(1, 1)), bytes);
}

TEST(NativeArrowToBlock, SliceBlockCopiesStringsAndCountsWhatTheCopiesOwn) {
    const std::string spec =
        "id:BIGINT;name:VARCHAR;tags:VARCHAR ARRAY;m:MAP<VARCHAR, BIGINT>;r:ROW<a BIGINT, b "
        "VARCHAR>";
    const ColumnPlan plan = a2b_plan(spec,
                                     {"Int64",
                                      "Nullable(String)",
                                      "Array(String)",
                                      "Map(String, Int64)",
                                      "Tuple(a Int64, b String)"});
    BlockSlice slice;
    std::size_t builder_payload = 0;
    {
        const auto chunk = a2b_chunk(
            spec,
            {"[1, 2, 3, 4]",
             R"(["a", null, "ccc", "dd"])",
             R"([["x"], ["yy", "z"], [], ["w"]])",
             R"([[["k", 1]], [], [["kk", 2], ["j", 3]], []])",
             R"([{"a": 1, "b": "p"}, {"a": 2, "b": "qq"}, {"a": 3, "b": ""}, {"a": 4, "b": "s"}])"});
        BlockBuilder builder(plan);
        builder.append(*chunk, 1, 2);
        builder_payload = builder.payload_bytes();
        builder.reset();
        builder.append(*chunk, 0, 4);
        const ch::Block block = builder.take();
        slice = slice_block(block, 1, 2);
        EXPECT_THROW((void)slice_block(block, 1, 0), std::out_of_range);
        EXPECT_THROW((void)slice_block(block, 3, 2), std::out_of_range);
    }
    // The chunk, the builder and the parent block are gone: the slice reads
    // only its own copies, which ASan would otherwise catch.
    EXPECT_EQ(slice.rows, 2U);
    EXPECT_EQ(slice.block.GetRowCount(), 2U);
    EXPECT_EQ(slice.block.GetColumnName(4), "r");
    EXPECT_EQ(a2b_rows(slice.block[0]), (A2bRows{"2", "3"}));
    EXPECT_EQ(a2b_rows(slice.block[1]), (A2bRows{"NULL", "'ccc'"}));
    EXPECT_EQ(a2b_rows(slice.block[2]), (A2bRows{"['yy', 'z']", "[]"}));
    EXPECT_EQ(a2b_rows(slice.block[3]), (A2bRows{"{}", "{'kk': 2, 'j': 3}"}));
    EXPECT_EQ(a2b_rows(slice.block[4]), (A2bRows{"(2, 'qq')", "(3, '')"}));

    // The same rows built afresh measure the same payload.
    EXPECT_EQ(slice.payload_bytes, 101U);
    EXPECT_EQ(slice.payload_bytes, builder_payload);
    // Each String slice owns 16 bytes a view, its text, and one storage-block
    // entry: id 16; name 2 + (32 + 3 + entry); tags 16 + (32 + 3 + entry);
    // m 16 + (32 + 3 + entry) + 16; r 16 + (32 + 2 + entry).
    const std::size_t entry = 2 * sizeof(std::size_t) + sizeof(char*);
    EXPECT_EQ(
        slice.owned_bytes,
        16U + (2 + 35 + entry) + (16 + 35 + entry) + (16 + 35 + entry + 16) + (16 + 34 + entry));
}

// A split part goes out under the parent INSERT's header, which refuses type
// conversion, so every column must keep its parent's type. The client's own
// DateTime slice drops the time zone, at the top level and inside every
// composite that slices its children.
TEST(NativeArrowToBlock, SliceBlockKeepsEveryColumnTypeTimeZonesIncluded) {
    const std::string spec = std::string("t:TIMESTAMP(3);tn:TIMESTAMP(3);") +
                             "ta:TIMESTAMP(3) ARRAY;tan:TIMESTAMP(3) ARRAY;" +
                             "tm:MAP<VARCHAR, TIMESTAMP(3)>;tr:ROW<a TIMESTAMP(3), b BIGINT>;" +
                             "plain:TIMESTAMP(3);t64:TIMESTAMP(3);" +
                             "d:DECIMAL(10, 2);e:VARCHAR;f:VARCHAR";
    const std::vector<std::string> targets = {"DateTime('UTC')",
                                              "Nullable(DateTime('Europe/London'))",
                                              "Array(DateTime('UTC'))",
                                              "Array(Nullable(DateTime('UTC')))",
                                              "Map(String, DateTime('UTC'))",
                                              "Tuple(a DateTime('UTC'), b Int64)",
                                              "DateTime",
                                              "DateTime64(3, 'UTC')",
                                              "Decimal(12, 4)",
                                              "Enum8('a' = 1, 'b' = 2)",
                                              "FixedString(3)"};
    const ColumnPlan plan = a2b_plan(spec, targets);
    const auto chunk = a2b_chunk(
        spec,
        {"[1000, 2000, 3000, 4000]",
         "[1000, null, 3000, 4000]",
         "[[1000], [2000, 3000], [], [4000]]",
         "[[null], [2000, null], [3000], []]",
         R"([[["a", 1000]], [["b", 2000]], [], [["c", 3000], ["d", 4000]]])",
         R"([{"a": 1000, "b": 1}, {"a": 2000, "b": 2}, {"a": 3000, "b": 3}, {"a": 4000, "b": 4}])",
         "[1000, 2000, 3000, 4000]",
         "[1, 2, 3, 4]",
         R"(["1.00", "2.00", "3.00", "4.00"])",
         R"(["a", "b", "a", "b"])",
         R"(["w", "xx", "yyy", ""])"});
    const ch::Block block = a2b_convert(plan, *chunk, 0, 4);
    const BlockSlice part = slice_block(block, 1, 2);
    const std::vector<A2bRows> expected = {{"2", "3"},
                                           {"NULL", "3"},
                                           {"[2, 3]", "[]"},
                                           {"[2, NULL]", "[3]"},
                                           {"{'b': 2}", "{}"},
                                           {"(2, 2)", "(3, 3)"},
                                           {"2", "3"},
                                           {"2", "3"},
                                           {"20000", "30000"},
                                           {"b", "a"},
                                           {"'xx\\0'", "'yyy'"}};
    ASSERT_EQ(part.block.GetColumnCount(), targets.size());
    for (std::size_t k = 0; k < targets.size(); ++k) {
        SCOPED_TRACE(targets[k]);
        EXPECT_EQ(block[k]->Type()->GetName(), client_header_spelling(targets[k]));
        EXPECT_EQ(part.block[k]->Type()->GetName(), block[k]->Type()->GetName());
        EXPECT_EQ(part.block[k]->CloneEmpty()->Type()->GetName(), block[k]->Type()->GetName());
        EXPECT_EQ(a2b_rows(part.block[k]), expected[k]);
    }
    // A part of a part, as a split of a split half makes, keeps the zone too.
    const BlockSlice quarter = slice_block(part.block, 1, 1);
    EXPECT_EQ(quarter.block[0]->Type()->GetName(), "DateTime('UTC')");
    EXPECT_EQ(a2b_rows(quarter.block[2]), (A2bRows{"[]"}));
    EXPECT_EQ(quarter.block[2]->Type()->GetName(), "Array(DateTime('UTC'))");
}

// The memory charge for a split part counts what its copies really hold. An
// Array slice appends its offsets one value at a time, so the vector grows
// past the row count, and so does the one under a Map.
TEST(NativeArrowToBlock, SliceBlockCountsTheGrowthSlackOfArrayAndMapOffsets) {
    constexpr std::size_t kRows = 600;
    constexpr std::size_t kHalf = 300;
    std::string lists = "[";
    std::string maps = "[";
    for (std::size_t i = 0; i < kRows; ++i) {
        lists += i == 0 ? "[1]" : ", [1]";
        maps += i == 0 ? R"([["k", 1]])" : R"(, [["k", 1]])";
    }
    lists += "]";
    maps += "]";
    const std::string spec = "tags:BIGINT ARRAY;m:MAP<VARCHAR, BIGINT>";
    const ColumnPlan plan = a2b_plan(spec, {"Array(Int64)", "Map(String, Int64)"});
    const auto chunk = a2b_chunk(spec, {lists, maps});
    const ch::Block block = a2b_convert(plan, *chunk, 0, static_cast<std::int64_t>(kRows));

    ch::Block tags;
    tags.AppendColumn("tags", block[0]);
    tags.RefreshRowCount();
    const BlockSlice tags_part = slice_block(tags, 0, kHalf);
    const std::size_t capacity =
        tags_part.block[0]->As<ch::ColumnArray>()->GetOffsets()->Capacity();
    // Without slack to count, this case would prove nothing.
    ASSERT_GT(capacity, kHalf);
    EXPECT_EQ(tags_part.owned_bytes, capacity * 8 + kHalf * 8);
    EXPECT_EQ(tags_part.payload_bytes, kHalf * 8 + kHalf * 8);

    // The Map's entries are an Array slice of the same rows, grown the same
    // way: offsets, then a one-byte key of 16 bytes a view plus its text and
    // one storage-block entry, then the values.
    ch::Block m;
    m.AppendColumn("m", block[1]);
    m.RefreshRowCount();
    const BlockSlice m_part = slice_block(m, 0, kHalf);
    const std::size_t entry = 2 * sizeof(std::size_t) + sizeof(char*);
    EXPECT_EQ(m_part.owned_bytes, capacity * 8 + (kHalf * 16 + kHalf + entry) + kHalf * 8);
    EXPECT_EQ(m_part.payload_bytes, kHalf * 8 + 2 * kHalf + kHalf * 8);

    // The whole block's part is the sum of its columns'.
    EXPECT_EQ(slice_block(block, 0, kHalf).owned_bytes, tags_part.owned_bytes + m_part.owned_bytes);
}

TEST(NativeArrowToBlock, RedactedRowShowsTypesAndOnlyTheOffendingNumber) {
    const std::string spec =
        "id:BIGINT;email:VARCHAR;n:SMALLINT;tags:BIGINT ARRAY;at:TIMESTAMP(3);amount:DECIMAL(10, "
        "2);day:DATE;ip:VARCHAR;ok:BOOLEAN;ratio:DOUBLE";
    const ColumnPlan plan = a2b_plan(spec,
                                     {"Int64",
                                      "String",
                                      "Int8",
                                      "Array(Int8)",
                                      "DateTime64(0)",
                                      "Decimal(12, 4)",
                                      "Date32",
                                      "IPv4",
                                      "Bool",
                                      "Float64"});
    const auto chunk = a2b_chunk(spec,
                                 {"[7, null]",
                                  R"(["someone@example.invalid", "x"])",
                                  "[200, 1]",
                                  "[[1, 300], []]",
                                  "[1700000000123, 0]",
                                  R"(["12.34", "0.00"])",
                                  "[-25568, 0]",
                                  R"(["10.0.0.300", "1.1.1.1"])",
                                  "[true, false]",
                                  "[0.25, 1]"});
    const std::vector<std::pair<std::string, std::string>> columns = {
        {"id", "Int64"},
        {"email", "String"},
        {"n", "Int8"},
        {"tags", "Array(Int8)"},
        {"at", "DateTime64(0)"},
        {"amount", "Decimal(12, 4)"},
        {"day", "Date32"},
        {"ip", "IPv4"},
        {"ok", "Bool"},
        {"ratio", "Float64"},
    };
    // Every column with its target type, and `value` beside `shown` alone.
    const auto line = [&](const std::string& shown, const std::string& value) {
        std::string out;
        for (const auto& [name, type] : columns) {
            out += (out.empty() ? "" : ", ") + name + "=" + type;
            if (name == shown) {
                out += "(" + value + ")";
            }
        }
        return out;
    };
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "n"),
              "id=Int64, email=String, n=Int8(200), tags=Array(Int8), at=DateTime64(0), "
              "amount=Decimal(12, 4), day=Date32, ip=IPv4, ok=Bool, ratio=Float64");
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "at"), line("at", "1700000000123 ms"));
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "amount"), line("amount", "12.34"));
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "day"), line("day", "-25568 days"));
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "ok"), line("ok", "true"));
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "ratio"), line("ratio", "0.25"));
    EXPECT_EQ(redacted_row(plan, *chunk, 1, "id"), line("id", "NULL"));
    // Text and composites show only their length; an element points at its
    // top-level column.
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "email"),
              "id=Int64, email=String(len 23), n=Int8, tags=Array(Int8), at=DateTime64(0), "
              "amount=Decimal(12, 4), day=Date32, ip=IPv4, ok=Bool, ratio=Float64");
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "ip"), line("ip", "len 10"));
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "tags.element"), line("tags", "len 2"));
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "tags"), line("tags", "len 2"));
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "nothing"), line("", ""));
    EXPECT_EQ(redacted_row(plan, *chunk, 5, "n"), line("n", "row not in chunk"));

    // The log line for a real failure carries no string content at all.
    BlockBuilder builder(plan);
    try {
        builder.append(*chunk, 0, 1);
        FAIL() << "200 went into Int8";
    } catch (const ConversionError& e) {
        EXPECT_EQ(e.column(), "n");
        const std::string logged = redacted_row(plan, *chunk, e.row(), e.column());
        EXPECT_EQ(logged.find("someone"), std::string::npos);
        EXPECT_EQ(logged.find("10.0.0.300"), std::string::npos);
        EXPECT_NE(logged.find("n=Int8(200)"), std::string::npos);
    }
}

// --- Typed struct inputs ----------------------------------------------------------

// The plan for one typed column `c` of Arrow type `type` into `target`.
ColumnPlan a2b_typed_plan(const std::shared_ptr<arrow::DataType>& type, const std::string& target) {
    PlanResult r =
        compile_column_plan(columns_from_arrow_schema(*arrow::schema({arrow::field("c", type)})),
                            {TargetColumn{"c", target, DefaultKind::None, 1}},
                            InputKind::TypedStruct);
    if (!r.plan) {
        throw std::runtime_error("the column plan refused " + type->ToString() + " into " + target +
                                 ": " + r.problems.at(0).message);
    }
    return std::move(*r.plan);
}

std::vector<std::string> a2b_typed_accepts(const std::shared_ptr<arrow::DataType>& type,
                                           const std::string& target,
                                           const std::string& json) {
    const ColumnPlan plan = a2b_typed_plan(type, target);
    const auto chunk = a2b_single(arrow::json::ArrayFromJSONString(type, json).ValueOrDie());
    return a2b_rows(a2b_convert(plan, *chunk, 0, chunk->num_rows())[0]);
}

// The ConversionError a typed single-column conversion throws, checked for
// the column and the row it names.
std::string a2b_typed_refusal(const std::shared_ptr<arrow::DataType>& type,
                              const std::string& target,
                              const std::string& json,
                              std::int64_t row) {
    const ColumnPlan plan = a2b_typed_plan(type, target);
    const auto chunk = a2b_single(arrow::json::ArrayFromJSONString(type, json).ValueOrDie());
    BlockBuilder builder(plan);
    try {
        builder.append(*chunk, 0, chunk->num_rows());
    } catch (const ConversionError& e) {
        EXPECT_EQ(e.code(), code::kConversionFailed);
        EXPECT_EQ(e.column(), "c");
        EXPECT_EQ(e.row(), row);
        return e.what();
    }
    ADD_FAILURE() << type->ToString() << " into " << target << " accepted " << json;
    return {};
}

TEST(NativeArrowToBlock, UnsignedNarrowingIsCheckedPerValue) {
    EXPECT_EQ(a2b_typed_refusal(arrow::uint64(), "Int64", "[1, 9223372036854775808]", 1),
              "[clickhouse.conversion_failed] column `c`, row 1: value out of range for Int64");
    EXPECT_EQ(a2b_typed_refusal(arrow::uint16(), "UInt8", "[255, 300]", 1),
              "[clickhouse.conversion_failed] column `c`, row 1: value out of range for UInt8");
    EXPECT_EQ(a2b_typed_refusal(arrow::uint8(), "Int8", "[128]", 0),
              "[clickhouse.conversion_failed] column `c`, row 0: value out of range for Int8");
    EXPECT_EQ(a2b_typed_refusal(arrow::uint32(), "Int32", "[2147483648]", 0),
              "[clickhouse.conversion_failed] column `c`, row 0: value out of range for Int32");
    EXPECT_EQ(a2b_typed_accepts(arrow::uint64(), "Int64", "[0, 9223372036854775807]"),
              (A2bRows{"0", "9223372036854775807"}));
    EXPECT_EQ(a2b_typed_accepts(arrow::uint16(), "UInt8", "[0, 255]"), (A2bRows{"0", "255"}));
    EXPECT_EQ(a2b_typed_accepts(arrow::uint8(), "Int8", "[0, 127]"), (A2bRows{"0", "127"}));
}

TEST(NativeArrowToBlock, UnsignedWideningAndCopiesAreExact) {
    EXPECT_EQ(a2b_typed_accepts(arrow::uint32(), "Int64", "[4294967295, 0]"),
              (A2bRows{"4294967295", "0"}));
    EXPECT_EQ(a2b_typed_accepts(arrow::uint64(), "Int128", "[18446744073709551615, 0]"),
              (A2bRows{"18446744073709551615", "0"}));
    EXPECT_EQ(a2b_typed_accepts(arrow::uint64(), "UInt64", "[18446744073709551615, 1]"),
              (A2bRows{"18446744073709551615", "1"}));
    EXPECT_EQ(a2b_typed_accepts(arrow::uint8(), "UInt64", "[255, 0]"), (A2bRows{"255", "0"}));
    EXPECT_EQ(a2b_typed_accepts(arrow::uint16(), "Nullable(Int32)", "[65535, null]"),
              (A2bRows{"65535", "NULL"}));
}

TEST(NativeArrowToBlock, AMicrosecondFieldIsExactInDateTime64Of6AndRefusedNotFlooredIn3) {
    const auto micros = arrow::timestamp(arrow::TimeUnit::MICRO);
    EXPECT_EQ(a2b_typed_accepts(micros, "DateTime64(6)", "[1700000000123456, -1, 0]"),
              (A2bRows{"1700000000123456", "-1", "0"}));
    EXPECT_EQ(a2b_typed_accepts(micros, "DateTime64(3)", "[1700000000123000, -2000]"),
              (A2bRows{"1700000000123", "-2"}));
    EXPECT_EQ(a2b_typed_refusal(micros, "DateTime64(3)", "[1700000000123000, 1700000000123456]", 1),
              "[clickhouse.conversion_failed] column `c`, row 1: 1700000000123456 us has "
              "sub-second digits DateTime64(3) cannot hold; target DateTime64(6) or write whole "
              "seconds");
    // A negative value is never floored to the millisecond below.
    EXPECT_EQ(a2b_typed_refusal(micros, "DateTime64(3)", "[-1500]", 0),
              "[clickhouse.conversion_failed] column `c`, row 0: -1500 us has sub-second digits "
              "DateTime64(3) cannot hold; target DateTime64(6) or write whole seconds");
    const auto seconds = arrow::timestamp(arrow::TimeUnit::SECOND, "UTC");
    EXPECT_EQ(a2b_typed_accepts(seconds, "DateTime", "[1700000000]"), (A2bRows{"1700000000"}));
}

TEST(NativeArrowToBlock, AnUnsignedCellIsShownInTheRedactedRow) {
    const ColumnPlan plan = a2b_typed_plan(arrow::uint64(), "Int64");
    const auto chunk = a2b_single(
        arrow::json::ArrayFromJSONString(arrow::uint64(), "[18446744073709551615]").ValueOrDie());
    EXPECT_EQ(redacted_row(plan, *chunk, 0, "c"), "c=Int64(18446744073709551615)");
}

// The chunk a typed struct's batcher builds, less its event_time column, is
// the plan's input as it stands.
TEST(NativeArrowToBlock, ATypedStructsBatchConvertsOnceItsEventTimeIsDropped) {
    const auto batcher = clink::make_columnar_arrow_batcher<A2bTypedTrade>();
    Batch<A2bTypedTrade> batch;
    batch.emplace(A2bTypedTrade{
        7, 4000000000U, 18446744073709551615ULL, {1, 255}, {{"a", 65535}}, {"XLON", 0.25}});
    batch.emplace(A2bTypedTrade{-1, 0, std::nullopt, {}, {}, {"", -2.5}});
    const auto built = batcher.build(batch);
    ASSERT_NE(built, nullptr);
    const auto chunk = built->RemoveColumn(0).ValueOrDie();

    const auto columns = columns_from_arrow_schema(*built->schema());
    const std::vector<std::string> targets = {"Int64",
                                              "UInt32",
                                              "Nullable(UInt64)",
                                              "Array(UInt8)",
                                              "Map(String, UInt16)",
                                              "Tuple(venue String, px Float64)"};
    ASSERT_EQ(columns.size(), targets.size());
    std::vector<TargetColumn> table;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        table.push_back(TargetColumn{
            columns[i].name, targets[i], DefaultKind::None, static_cast<std::uint32_t>(i + 1)});
    }
    PlanResult r = compile_column_plan(columns, table, InputKind::TypedStruct);
    ASSERT_TRUE(r.plan.has_value()) << r.problems.at(0).message;
    const ch::Block block = a2b_convert(*r.plan, *chunk, 0, chunk->num_rows());
    ASSERT_EQ(block.GetColumnCount(), 6U);
    EXPECT_EQ(a2b_rows(block[0]), (A2bRows{"7", "-1"}));
    EXPECT_EQ(a2b_rows(block[1]), (A2bRows{"4000000000", "0"}));
    EXPECT_EQ(a2b_rows(block[2]), (A2bRows{"18446744073709551615", "NULL"}));
    EXPECT_EQ(a2b_rows(block[3]), (A2bRows{"[1, 255]", "[]"}));
    EXPECT_EQ(a2b_rows(block[4]), (A2bRows{"{'a': 65535}", "{}"}));
    EXPECT_EQ(a2b_rows(block[5]), (A2bRows{"('XLON', 0.25)", "('', -2.5)"}));
}

}  // namespace
}  // namespace clink::clickhouse::native
