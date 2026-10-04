// Schema-driven columnar batcher for the SQL Row type: each declared
// column round-trips as its own typed Arrow column (over the real Arrow
// IPC wire path), nulls survive, and the schema/param (de)serialisation
// is lossless.

#ifndef CLINK_HAS_ARROW
#error "test_row_columnar_batcher requires Arrow"
#endif

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <typeinfo>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/sql/row_columnar_output.hpp"
#include "clink/sql/vector_value.hpp"

using namespace clink;
using clink::sql::make_row_columnar_arrow_batcher;
using clink::sql::Row;
using clink::sql::RowColumn;
namespace cfg = clink::config;

namespace {

std::vector<RowColumn> trade_schema() {
    return {
        {"id", arrow::int64()},
        {"symbol", arrow::utf8()},
        {"price", arrow::float64()},
        {"qty", arrow::int32()},
        {"active", arrow::boolean()},
        {"amount", arrow::decimal128(10, 2)},
    };
}

Row make_row(std::int64_t id,
             const std::string& sym,
             double px,
             std::int32_t qty,
             bool active,
             const std::string& amount_dec) {
    Row r;
    r.values["id"] = cfg::JsonValue{id};
    r.values["symbol"] = cfg::JsonValue{sym};
    r.values["price"] = cfg::JsonValue{px};
    r.values["qty"] = cfg::JsonValue{static_cast<std::int64_t>(qty)};
    r.values["active"] = cfg::JsonValue{active};
    r.values["amount"] = cfg::make_dec_value(*cfg::dec_parse(amount_dec));
    return r;
}

}  // namespace

TEST(RowColumnarBatcher, SchemaIsTypedPerColumn) {
    auto batcher = make_row_columnar_arrow_batcher(trade_schema());
    auto schema = batcher.schema();

    ASSERT_EQ(schema->num_fields(), 7);
    EXPECT_EQ(schema->field(0)->name(), "event_time");
    EXPECT_EQ(schema->field(1)->name(), "id");
    EXPECT_EQ(schema->field(1)->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(schema->field(2)->type()->id(), arrow::Type::STRING);
    EXPECT_EQ(schema->field(3)->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(schema->field(4)->type()->id(), arrow::Type::INT32);
    EXPECT_EQ(schema->field(5)->type()->id(), arrow::Type::BOOL);
    ASSERT_EQ(schema->field(6)->type()->id(), arrow::Type::DECIMAL128);
    const auto& dt = static_cast<const arrow::Decimal128Type&>(*schema->field(6)->type());
    EXPECT_EQ(dt.precision(), 10);
    EXPECT_EQ(dt.scale(), 2);
    // No opaque binary fallback column.
    for (int i = 0; i < schema->num_fields(); ++i)
        EXPECT_NE(schema->field(i)->type()->id(), arrow::Type::BINARY);
}

TEST(RowColumnarBatcher, RoundTripOverArrowIpc) {
    auto batcher = make_row_columnar_arrow_batcher(trade_schema());

    Batch<Row> in;
    in.emplace(make_row(1, "AAPL", 191.25, 100, true, "123.45"), EventTime{1000});
    in.emplace(make_row(2, "MSFT", 410.10, 50, false, "0.07"), EventTime{1001});

    auto rb = batcher.build(in);
    ASSERT_NE(rb, nullptr);
    ASSERT_EQ(rb->num_columns(), 7);

    auto ipc = arrow_batch_to_ipc(*rb);
    auto rb2 = arrow_batch_from_ipc(ipc.data(), ipc.size());
    ASSERT_NE(rb2, nullptr);

    auto out = batcher.parse(*rb2);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->size(), 2u);

    const Row& r0 = (*out)[0].value();
    EXPECT_EQ(r0.values.at("id").as_number(), 1);
    EXPECT_EQ(r0.values.at("symbol").as_string(), "AAPL");
    EXPECT_DOUBLE_EQ(r0.values.at("price").as_number(), 191.25);
    EXPECT_EQ(r0.values.at("qty").as_number(), 100);
    EXPECT_TRUE(r0.values.at("active").as_bool());
    ASSERT_TRUE(cfg::is_dec_string(r0.values.at("amount")));
    EXPECT_EQ(r0.values.at("amount").as_string(),
              cfg::make_dec_value(*cfg::dec_parse("123.45")).as_string());

    const Row& r1 = (*out)[1].value();
    EXPECT_EQ(r1.values.at("symbol").as_string(), "MSFT");
    EXPECT_FALSE(r1.values.at("active").as_bool());
    EXPECT_EQ(r1.values.at("amount").as_string(),
              cfg::make_dec_value(*cfg::dec_parse("0.07")).as_string());

    // Event-times survive.
    ASSERT_TRUE((*out)[0].event_time().has_value());
    EXPECT_EQ((*out)[0].event_time()->millis(), 1000);
    EXPECT_EQ((*out)[1].event_time()->millis(), 1001);
}

TEST(RowColumnarBatcher, NullAndMissingColumnsBecomeArrowNull) {
    auto batcher = make_row_columnar_arrow_batcher(trade_schema());

    Batch<Row> in;
    Row r;  // only some columns present
    r.values["id"] = cfg::JsonValue{std::int64_t{7}};
    r.values["symbol"] = cfg::JsonValue{};  // explicit JSON null
    // price, qty, active, amount all absent
    in.emplace(std::move(r));

    auto rb = batcher.build(in);
    ASSERT_NE(rb, nullptr);
    auto out = batcher.parse(*rb);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->size(), 1u);

    const Row& got = (*out)[0].value();
    EXPECT_EQ(got.values.at("id").as_number(), 7);
    EXPECT_TRUE(got.values.at("symbol").is_null());
    EXPECT_TRUE(got.values.at("price").is_null());
    EXPECT_TRUE(got.values.at("qty").is_null());
    EXPECT_TRUE(got.values.at("active").is_null());
    EXPECT_TRUE(got.values.at("amount").is_null());
}

TEST(RowColumnarBatcher, ListFloat32RoundTripsOverArrowIpc) {
    // An embedding column (list<float32>) rides the columnar wire as a contiguous Arrow
    // list, not a stringified JSON array, and round-trips back to a JSON number array.
    std::vector<RowColumn> schema{{"id", arrow::int64()}, {"emb", arrow::list(arrow::float32())}};
    auto batcher = make_row_columnar_arrow_batcher(schema);
    ASSERT_EQ(batcher.schema()->field(2)->type()->id(), arrow::Type::LIST);

    auto make_emb_row = [](std::int64_t id, std::vector<double> emb) {
        Row r;
        r.values["id"] = cfg::JsonValue{id};
        cfg::JsonArray arr;
        for (double v : emb) {
            arr.push_back(cfg::JsonValue{v});
        }
        r.values["emb"] = cfg::JsonValue{std::move(arr)};
        return r;
    };
    Batch<Row> in;
    in.emplace(make_emb_row(1, {1.5, 2.0, -3.25}), EventTime{10});
    in.emplace(make_emb_row(2, {0.0, 100.0}), EventTime{11});

    auto rb = batcher.build(in);
    ASSERT_NE(rb, nullptr);
    auto ipc = arrow_batch_to_ipc(*rb);
    auto rb2 = arrow_batch_from_ipc(ipc.data(), ipc.size());
    ASSERT_NE(rb2, nullptr);
    auto out = batcher.parse(*rb2);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->size(), 2u);

    const auto& e0 = (*out)[0].value().values.at("emb");
    ASSERT_TRUE(e0.is_array());
    ASSERT_EQ(e0.as_array().size(), 3u);
    EXPECT_DOUBLE_EQ(e0.as_array()[0].as_number(), 1.5);
    EXPECT_DOUBLE_EQ(e0.as_array()[2].as_number(), -3.25);
    const auto& e1 = (*out)[1].value().values.at("emb");
    ASSERT_EQ(e1.as_array().size(), 2u);
    EXPECT_DOUBLE_EQ(e1.as_array()[1].as_number(), 100.0);
}

TEST(RowColumnarBatcher, ListFloat32SchemaCodeRoundTrips) {
    std::vector<RowColumn> cols{{"emb", arrow::list(arrow::float32())}};
    const auto spec = clink::sql::serialize_row_schema(cols);
    EXPECT_NE(spec.find("list_f32"), std::string::npos);
    const auto parsed = clink::sql::parse_row_schema(spec);
    ASSERT_EQ(parsed.size(), 1u);
    EXPECT_TRUE(parsed[0].type->Equals(*arrow::list(arrow::float32())));
}

TEST(VectorValue, FromListCellReadsFloat32Directly) {
    // vector_from_list_cell copies float32 straight out of an Arrow ListArray - no JSON
    // round-trip, no double narrowing.
    auto vb = std::make_shared<arrow::FloatBuilder>();
    arrow::ListBuilder lb(arrow::default_memory_pool(), vb);
    ASSERT_TRUE(lb.Append().ok());
    ASSERT_TRUE(vb->AppendValues({1.5F, 2.5F, 3.5F}).ok());
    ASSERT_TRUE(lb.Append().ok());  // second row: 2 values
    ASSERT_TRUE(vb->AppendValues({-1.0F, 0.0F}).ok());
    ASSERT_TRUE(lb.AppendNull().ok());  // third row: null
    std::shared_ptr<arrow::Array> arr;
    ASSERT_TRUE(lb.Finish(&arr).ok());
    const auto& list = static_cast<const arrow::ListArray&>(*arr);

    auto c0 = clink::sql::vector_from_list_cell(list, 0);
    ASSERT_TRUE(c0.present);
    EXPECT_EQ(c0.data, (std::vector<float>{1.5F, 2.5F, 3.5F}));

    auto c1 = clink::sql::vector_from_list_cell(list, 1, /*expected_dim*/ 2);
    ASSERT_TRUE(c1.present);
    EXPECT_TRUE(c1.dim_ok);
    EXPECT_EQ(c1.data, (std::vector<float>{-1.0F, 0.0F}));

    auto c1_bad = clink::sql::vector_from_list_cell(list, 1, /*expected_dim*/ 5);
    EXPECT_TRUE(c1_bad.present);
    EXPECT_FALSE(c1_bad.dim_ok);

    auto c2 = clink::sql::vector_from_list_cell(list, 2);  // null cell
    EXPECT_FALSE(c2.present);
}

TEST(RowColumnarBatcher, SchemaParamRoundTrips) {
    const auto cols = trade_schema();
    const auto spec = clink::sql::serialize_row_schema(cols);
    const auto parsed = clink::sql::parse_row_schema(spec);

    ASSERT_EQ(parsed.size(), cols.size());
    for (std::size_t i = 0; i < cols.size(); ++i) {
        EXPECT_EQ(parsed[i].name, cols[i].name);
        EXPECT_TRUE(parsed[i].type->Equals(*cols[i].type))
            << "column " << cols[i].name << " type mismatch: " << parsed[i].type->ToString()
            << " vs " << cols[i].type->ToString();
    }
}

TEST(RowColumnarBatcher, UnsupportedTypeFallsBackToUtf8) {
    // A declared TIMESTAMP(3) reaches a batcher through its V1 code, which is
    // text, so it is stored as utf8. A declared type handed over directly is
    // outside the carried set unless it is one a row-schema code names: a
    // microsecond timestamp still falls back.
    std::vector<RowColumn> declared = {{"ts", arrow::timestamp(arrow::TimeUnit::MILLI)}};
    auto batcher = make_row_columnar_arrow_batcher(clink::sql::parse_row_schema(
        clink::sql::serialize_row_schema(declared, clink::sql::RowLayout::V1)));
    auto schema = batcher.schema();
    ASSERT_EQ(schema->num_fields(), 2);
    EXPECT_EQ(schema->field(1)->type()->id(), arrow::Type::STRING);

    auto micros =
        make_row_columnar_arrow_batcher({{"ts", arrow::timestamp(arrow::TimeUnit::MICRO)}});
    EXPECT_EQ(micros.schema()->field(1)->type()->id(), arrow::Type::STRING);
}

// --- Born-columnar operator output (RowColumnarOutput) -----------------------
//
// The builder an operator uses to emit a typed Arrow batch without ever
// constructing a Row. Its whole safety argument is that it shares
// append_json_cell with the row-batch converter, so these tests pin the two
// against each other over the awkward cells (absent, JSON-null, wrong kind) as
// well as the ordinary ones.

TEST(RowColumnarOutput, MatchesTheRowBatchConverterCellForCell) {
    const auto cols = trade_schema();

    // Rows deliberately including the cases where a cell is not simply present
    // and well-typed: a missing column, an explicit JSON null, and a value of
    // the wrong JSON kind for its declared column.
    std::vector<Row> rows;
    rows.push_back(make_row(1, "AAPL", 1.5, 7, true, "12.34"));
    rows.push_back(make_row(-2, "", -0.25, -8, false, "-0.01"));
    Row sparse;  // id only: every other column absent
    sparse.values["id"] = cfg::JsonValue{3};
    rows.push_back(sparse);
    Row nulled = make_row(4, "MSFT", 2.0, 1, true, "5.00");
    nulled.values["symbol"] = cfg::JsonValue{};                   // explicit null
    nulled.values["price"] = cfg::JsonValue{std::string{"nan"}};  // wrong kind
    nulled.values["active"] = cfg::JsonValue{std::int64_t{1}};    // wrong kind
    rows.push_back(nulled);

    // Carrier A: build rows, then convert the row batch (wire / Parquet path).
    Batch<Row> row_batch;
    for (const auto& r : rows) {
        row_batch.push(Record<Row>{r});
    }
    auto from_rows = make_row_columnar_arrow_batcher(cols).build(row_batch);
    ASSERT_NE(from_rows, nullptr);

    // Carrier B: append the same values straight into the columnar builder,
    // never constructing an output Row (the operator-emission path).
    clink::sql::RowColumnarOutput out{cols};
    out.reserve(static_cast<std::int64_t>(rows.size()));
    for (const auto& r : rows) {
        out.append_row_projection(r);
    }
    auto from_output = out.finish();
    ASSERT_NE(from_output, nullptr);

    EXPECT_TRUE(from_output->schema()->Equals(*from_rows->schema()))
        << from_output->schema()->ToString() << "\nvs\n"
        << from_rows->schema()->ToString();
    EXPECT_TRUE(from_output->Equals(*from_rows)) << from_output->ToString() << "\nvs\n"
                                                 << from_rows->ToString();
}

TEST(RowColumnarOutput, MaterializesBackToTheSameRows) {
    const auto cols = trade_schema();
    const std::vector<Row> rows = {make_row(10, "A", 1.0, 1, true, "1.00"),
                                   make_row(11, "B", 2.5, 2, false, "2.50")};

    clink::sql::RowColumnarOutput out{cols};
    for (const auto& r : rows) {
        out.append_row_projection(r);
    }
    auto rb = out.finish();
    ASSERT_NE(rb, nullptr);

    // A row consumer downstream sees exactly the rows it would have been handed.
    auto batch = clink::sql::columnar_row_batch(rb);
    ASSERT_TRUE(batch.is_columnar());
    ASSERT_EQ(batch.size(), rows.size());
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const Row& got = batch[i].value();
        for (const auto& c : cols) {
            ASSERT_TRUE(got.values.find(c.name) != got.values.end()) << c.name;
            EXPECT_EQ(got.values.find(c.name)->second.serialize(0),
                      rows[i].values.find(c.name)->second.serialize(0))
                << "row " << i << " column " << c.name;
        }
    }
}

TEST(RowColumnarOutput, CarriesTheEventTimeColumnSoValuesAreNotShifted) {
    // Column 0 must be the nullable int64 event time: the self-describing
    // reader takes value columns from index 1, so a builder that omitted it
    // would have its first value column eaten as a timestamp.
    std::vector<RowColumn> cols = {{"a", arrow::int64()}, {"b", arrow::int64()}};
    clink::sql::RowColumnarOutput out{cols};
    Row r;
    r.values["a"] = cfg::JsonValue{std::int64_t{7}};
    r.values["b"] = cfg::JsonValue{std::int64_t{9}};
    out.append_row_projection(r, EventTime{1234});
    auto rb = out.finish();
    ASSERT_NE(rb, nullptr);

    ASSERT_EQ(rb->num_columns(), 3);
    EXPECT_EQ(rb->schema()->field(0)->name(), "event_time");
    EXPECT_TRUE(rb->schema()->field(0)->nullable());

    auto batch = clink::sql::columnar_row_batch(rb);
    ASSERT_EQ(batch.size(), 1U);
    EXPECT_EQ(batch[0].value().values.find("a")->second.as_number(), 7);
    EXPECT_EQ(batch[0].value().values.find("b")->second.as_number(), 9);
    ASSERT_TRUE(batch[0].event_time().has_value());
    EXPECT_EQ(batch[0].event_time()->millis(), 1234);
}

TEST(RowColumnarOutput, EmptyBuilderYieldsNoBatch) {
    clink::sql::RowColumnarOutput out{trade_schema()};
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(out.finish(), nullptr);
}

TEST(RowColumnarOutput, ReusableAcrossBatches) {
    const auto cols = trade_schema();
    clink::sql::RowColumnarOutput out{cols};
    out.append_row_projection(make_row(1, "A", 1.0, 1, true, "1.00"));
    auto first = out.finish();
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->num_rows(), 1);
    EXPECT_TRUE(out.empty());

    out.append_row_projection(make_row(2, "B", 2.0, 2, false, "2.00"));
    out.append_row_projection(make_row(3, "C", 3.0, 3, true, "3.00"));
    auto second = out.finish();
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second->num_rows(), 2);
    // The first batch's arrays are untouched by the reuse.
    EXPECT_EQ(first->num_rows(), 1);
}

// The emission-size damper. A RecordBatch costs a fixed amount per batch and
// saves per row, so an operator emitting a handful of rows at a time is faster in
// row form. The row count is only known after a batch is built, so the damper
// chooses from the previous emissions.
TEST(ColumnarOutputDamper, StartsActiveAndStaysActiveForLargeEmissions) {
    clink::sql::ColumnarOutputDamper d;
    EXPECT_TRUE(d.active());
    for (int i = 0; i < 100; ++i) {
        d.observed(clink::sql::kColumnarOutputMinRows);
        EXPECT_TRUE(d.active()) << "iteration " << i;
    }
}

TEST(ColumnarOutputDamper, BacksOffAfterARunOfSmallEmissions) {
    clink::sql::ColumnarOutputDamper d;
    // A short run is tolerated: one big batch resets the count, so an operator
    // with mixed sizes is not condemned by a single small one.
    d.observed(1);
    d.observed(1);
    d.observed(clink::sql::kColumnarOutputMinRows * 2);
    EXPECT_TRUE(d.active());

    for (int i = 0; i < 4; ++i) {
        d.observed(1);
    }
    EXPECT_FALSE(d.active()) << "a sustained run of small emissions must back off";
}

TEST(ColumnarOutputDamper, ReprobesSoGrowingBatchesRecover) {
    clink::sql::ColumnarOutputDamper d;
    for (int i = 0; i < 4; ++i) {
        d.observed(1);
    }
    ASSERT_FALSE(d.active());

    // Backed off, it re-probes periodically rather than staying off forever.
    bool recovered = false;
    for (int i = 0; i < 200 && !recovered; ++i) {
        d.observed(clink::sql::kColumnarOutputMinRows * 4);
        recovered = d.active();
    }
    EXPECT_TRUE(recovered) << "a backed-off damper must eventually retry";
}

// --- exact integer and number cells --------------------------------------------

namespace {

std::shared_ptr<arrow::Array> one_cell(const std::shared_ptr<arrow::DataType>& type,
                                       const cfg::JsonValue& v) {
    auto builder = clink::sql::row_columnar_detail::make_cell_builder(type);
    clink::sql::row_columnar_detail::append_json_cell(*builder, *type, &v);
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(builder->Finish(&out).ok());
    return out;
}

}  // namespace

TEST(RowColumnarBatcher, Int64CellsPastTwoToTheFiftyThreeAreExact) {
    // 2^53 + 1 has no double: through as_number() it came back as 2^53.
    const std::int64_t big = (std::int64_t{1} << 53) + 1;
    auto a =
        std::static_pointer_cast<arrow::Int64Array>(one_cell(arrow::int64(), cfg::JsonValue{big}));
    ASSERT_FALSE(a->IsNull(0));
    EXPECT_EQ(a->Value(0), big);
    const auto max = std::numeric_limits<std::int64_t>::max();
    a = std::static_pointer_cast<arrow::Int64Array>(one_cell(arrow::int64(), cfg::JsonValue{max}));
    EXPECT_EQ(a->Value(0), max);
}

TEST(RowColumnarBatcher, AnInt64CellNoInt64HoldsIsNullNotUndefined) {
    auto a = one_cell(arrow::int64(), cfg::JsonValue{1e300});
    EXPECT_TRUE(a->IsNull(0));
    a = one_cell(arrow::int64(), cfg::JsonValue{std::numeric_limits<double>::infinity()});
    EXPECT_TRUE(a->IsNull(0));
    auto t =
        std::static_pointer_cast<arrow::Int64Array>(one_cell(arrow::int64(), cfg::JsonValue{-7.9}));
    EXPECT_EQ(t->Value(0), -7) << "a double still truncates toward zero";
}

TEST(RowColumnarBatcher, AnInt32CellOutOfRangeIsNull) {
    auto a = one_cell(arrow::int32(), cfg::JsonValue{std::int64_t{2147483648}});
    EXPECT_TRUE(a->IsNull(0)) << "2^31 used to wrap to a negative int32";
    a = one_cell(arrow::int32(), cfg::JsonValue{std::int64_t{-2147483649}});
    EXPECT_TRUE(a->IsNull(0));
    auto ok = std::static_pointer_cast<arrow::Int32Array>(
        one_cell(arrow::int32(), cfg::JsonValue{std::int64_t{-2147483648}}));
    ASSERT_FALSE(ok->IsNull(0));
    EXPECT_EQ(ok->Value(0), std::numeric_limits<std::int32_t>::min());
    a = one_cell(arrow::int32(), cfg::JsonValue{3e9});
    EXPECT_TRUE(a->IsNull(0));
}

TEST(RowColumnarBatcher, ARealCellBeyondFloatIsNullNotInfinity) {
    auto a = one_cell(arrow::float32(), cfg::JsonValue{1e300});
    EXPECT_TRUE(a->IsNull(0)) << "a finite double beyond float landed as a value";
    auto ok = std::static_pointer_cast<arrow::FloatArray>(one_cell(
        arrow::float32(), cfg::JsonValue{static_cast<double>(std::numeric_limits<float>::max())}));
    ASSERT_FALSE(ok->IsNull(0));
    EXPECT_EQ(ok->Value(0), std::numeric_limits<float>::max());
    ok = std::static_pointer_cast<arrow::FloatArray>(
        one_cell(arrow::float32(), cfg::JsonValue{-2.5}));
    EXPECT_EQ(ok->Value(0), -2.5F);
}

TEST(RowColumnarBatcher, ADecimalCellWiderThanItsPrecisionIsNull) {
    // DECIMAL(5, 2) holds at most 999.99: 1234.5 needs six digits at scale 2.
    auto a = one_cell(arrow::decimal128(5, 2), cfg::make_dec_value(*cfg::dec_parse("1234.5")));
    EXPECT_TRUE(a->IsNull(0)) << "an over-precision value built an invalid decimal array";
    auto fits = one_cell(arrow::decimal128(5, 2), cfg::make_dec_value(*cfg::dec_parse("999.99")));
    EXPECT_FALSE(fits->IsNull(0));
    EXPECT_TRUE(fits->ValidateFull().ok());
    auto integral = std::static_pointer_cast<arrow::Decimal128Array>(
        one_cell(arrow::decimal128(20, 0), cfg::JsonValue{(std::int64_t{1} << 53) + 1}));
    ASSERT_FALSE(integral->IsNull(0));
    EXPECT_EQ(integral->FormatValue(0), "9007199254740993")
        << "an integer reached the decimal through a double";
}

TEST(RowColumnarBatcher, ADecimalCellFromADoubleBeyondInt64IsNullNotUndefined) {
    auto a = one_cell(arrow::decimal128(38, 0), cfg::JsonValue{1e300});
    EXPECT_TRUE(a->IsNull(0));
}

TEST(RowColumnarBatcher, NumbersRenderedAsTextAreExact) {
    using clink::sql::row_columnar_detail::to_utf8;
    const std::int64_t big = (std::int64_t{1} << 53) + 1;
    EXPECT_EQ(to_utf8(cfg::JsonValue{big}), "9007199254740993");
    EXPECT_EQ(to_utf8(cfg::JsonValue{1e-7}), "1e-07") << "six decimals rendered it as 0.000000";
    EXPECT_EQ(to_utf8(cfg::JsonValue{1.23456789012}), "1.23456789012");
    EXPECT_EQ(to_utf8(cfg::JsonValue{42.0}), "42");
    EXPECT_EQ(to_utf8(cfg::JsonValue{std::int64_t{-5}}), "-5");
}

// --- Row layouts --------------------------------------------------------------
//
// V1 is the layout every producer writes: TIMESTAMP and TIMESTAMPTZ travel as
// text. V2 adds millisecond timestamp codes. Readers learn them first; nothing
// writes them yet, and a reader that is not admitted to V2 resolves them to
// their V1 type.

namespace {

namespace rcd = clink::sql::row_columnar_detail;
using clink::sql::parse_row_schema;
using clink::sql::RowLayout;
using clink::sql::serialize_row_schema;

std::vector<RowColumn> timestamp_columns() {
    return {
        {"t0", arrow::timestamp(arrow::TimeUnit::SECOND)},
        {"t3", arrow::timestamp(arrow::TimeUnit::MILLI)},
        {"t6", arrow::timestamp(arrow::TimeUnit::MICRO)},
        {"t9", arrow::timestamp(arrow::TimeUnit::NANO)},
        {"z0", arrow::timestamp(arrow::TimeUnit::SECOND, "UTC")},
        {"z3", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")},
        {"z6", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")},
        {"z9", arrow::timestamp(arrow::TimeUnit::NANO, "UTC")},
    };
}

// Every other type the SQL catalog declares, one column each.
std::vector<RowColumn> non_timestamp_columns() {
    return {
        {"i64", arrow::int64()},
        {"i32", arrow::int32()},
        {"i16", arrow::int16()},
        {"f64", arrow::float64()},
        {"f32", arrow::float32()},
        {"b", arrow::boolean()},
        {"s", arrow::utf8()},
        {"d", arrow::decimal128(10, 2)},
        {"dd", arrow::decimal128(38, 9)},
        {"emb", arrow::list(arrow::float32())},
        {"arr", arrow::list(arrow::int64())},
        {"arr2", arrow::list(arrow::list(arrow::float32()))},
        {"day", arrow::date32()},
        {"tod", arrow::time64(arrow::TimeUnit::MICRO)},
        {"raw", arrow::binary()},
        {"m", arrow::map(arrow::utf8(), arrow::int64())},
        {"r", arrow::struct_({arrow::field("x", arrow::int64())})},
    };
}

std::shared_ptr<arrow::Array> timestamp_array(const std::shared_ptr<arrow::DataType>& type,
                                              const std::vector<std::optional<std::int64_t>>& v) {
    arrow::TimestampBuilder b(type, arrow::default_memory_pool());
    for (const auto& x : v) {
        if (x) {
            EXPECT_TRUE(b.Append(*x).ok());
        } else {
            EXPECT_TRUE(b.AppendNull().ok());
        }
    }
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}

}  // namespace

TEST(RowLayout, V1WritesEveryTimestampAsText) {
    const std::string text = "t0:str;t3:str;t6:str;t9:str;z0:str;z3:str;z6:str;z9:str";
    EXPECT_EQ(serialize_row_schema(timestamp_columns()), text);
    EXPECT_EQ(serialize_row_schema(timestamp_columns(), RowLayout::V1), text);
}

TEST(RowLayout, V1IsTheDefaultAndUnchangedForEveryOtherType) {
    const auto cols = non_timestamp_columns();
    // The spelling every released planner wrote; a change here changes every
    // persisted spec.
    const std::string text =
        "i64:i64;i32:i32;i16:str;f64:f64;f32:f32;b:bool;s:str;d:dec_10_2;dd:dec_38_9;"
        "emb:list_f32;arr:str;arr2:str;day:str;tod:str;raw:str;m:str;r:str";
    EXPECT_EQ(serialize_row_schema(cols), text);
    EXPECT_EQ(serialize_row_schema(cols, RowLayout::V1), text);
}

TEST(RowLayout, V2MapsTimestampAndTimestampTzToMillisecondCodesWhateverThePrecision) {
    EXPECT_EQ(serialize_row_schema(timestamp_columns(), RowLayout::V2),
              "t0:ts_ms;t3:ts_ms;t6:ts_ms;t9:ts_ms;z0:tstz_ms;z3:tstz_ms;z6:tstz_ms;z9:tstz_ms");
}

TEST(RowLayout, V2LeavesEveryOtherTypeOnItsV1Code) {
    const auto cols = non_timestamp_columns();
    EXPECT_EQ(serialize_row_schema(cols, RowLayout::V2), serialize_row_schema(cols, RowLayout::V1));
}

TEST(RowLayout, AnAdmittedParseRoundTripsTheV2Codes) {
    const std::string spec = "a:ts_ms;b:tstz_ms;c:i64";
    const auto cols = parse_row_schema(spec, RowLayout::V2);
    ASSERT_EQ(cols.size(), 3u);
    EXPECT_TRUE(cols[0].type->Equals(*arrow::timestamp(arrow::TimeUnit::MILLI)))
        << cols[0].type->ToString();
    EXPECT_TRUE(cols[1].type->Equals(*arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")))
        << cols[1].type->ToString();
    EXPECT_TRUE(cols[2].type->Equals(*arrow::int64()));
    EXPECT_EQ(serialize_row_schema(cols, RowLayout::V2), spec);
    // The V1 projection of the same columns is text, as it always was.
    EXPECT_EQ(serialize_row_schema(cols, RowLayout::V1), "a:str;b:str;c:i64");
}

TEST(RowLayout, AnUnadmittedParseResolvesV2CodesToTheirV1Types) {
    for (const auto& cols : {parse_row_schema("a:ts_ms;b:tstz_ms"),
                             parse_row_schema("a:ts_ms;b:tstz_ms", RowLayout::V1)}) {
        ASSERT_EQ(cols.size(), 2u);
        EXPECT_EQ(cols[0].name, "a");
        EXPECT_TRUE(cols[0].type->Equals(*arrow::utf8())) << cols[0].type->ToString();
        EXPECT_TRUE(cols[1].type->Equals(*arrow::utf8())) << cols[1].type->ToString();
    }
}

TEST(RowLayout, UnknownAndReservedCodesGiveUtf8) {
    for (const auto layout : {RowLayout::V1, RowLayout::V2}) {
        // i16 is reserved for SMALLINT and not emitted; until it is, it reads
        // as any unknown code does.
        const auto cols = parse_row_schema("a:i16;b:no_such_code;c:;d:dec_x_y;e:ts_us", layout);
        ASSERT_EQ(cols.size(), 5u);
        for (const auto& c : cols) {
            EXPECT_TRUE(c.type->Equals(*arrow::utf8())) << c.name << ": " << c.type->ToString();
        }
    }
}

TEST(RowLayout, NoCodeContainsTheNameOrColumnSeparator) {
    // parse_row_schema splits columns on ';' and a column on its last ':', so
    // a code holding either would be misread.
    auto all = non_timestamp_columns();
    for (auto& c : timestamp_columns()) {
        all.push_back(c);
    }
    for (const auto layout : {RowLayout::V1, RowLayout::V2}) {
        for (const auto& c : all) {
            const auto spec = serialize_row_schema({c}, layout);
            const auto code = spec.substr(c.name.size() + 1);
            EXPECT_EQ(code.find(':'), std::string::npos) << code;
            EXPECT_EQ(code.find(';'), std::string::npos) << code;
            ASSERT_EQ(parse_row_schema(spec, layout).size(), 1u) << spec;
        }
    }
}

TEST(RowLayoutReceive, EffectiveTypePassesMillisecondTimestampsThrough) {
    const auto ms = arrow::timestamp(arrow::TimeUnit::MILLI);
    const auto ms_utc = arrow::timestamp(arrow::TimeUnit::MILLI, "UTC");
    EXPECT_TRUE(rcd::effective_type(ms)->Equals(*ms));
    EXPECT_TRUE(rcd::effective_type(ms_utc)->Equals(*ms_utc));
    for (const auto& other : {arrow::timestamp(arrow::TimeUnit::SECOND),
                              arrow::timestamp(arrow::TimeUnit::MICRO),
                              arrow::timestamp(arrow::TimeUnit::NANO, "UTC"),
                              arrow::date32(),
                              arrow::int16()}) {
        EXPECT_TRUE(rcd::effective_type(other)->Equals(*arrow::utf8())) << other->ToString();
    }
}

TEST(RowLayoutReceive, ReadCellGivesEpochMillisAsAJsonInteger) {
    const std::vector<std::optional<std::int64_t>> values = {
        -1700000000123,
        0,
        (std::int64_t{1} << 53) + 1,
        std::numeric_limits<std::int64_t>::max(),
        std::numeric_limits<std::int64_t>::min(),
        std::nullopt,
    };
    for (const auto& type : {arrow::timestamp(arrow::TimeUnit::MILLI),
                             arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")}) {
        const auto arr = timestamp_array(type, values);
        for (std::size_t i = 0; i < values.size(); ++i) {
            const auto v = rcd::read_cell(type, *arr, static_cast<std::int64_t>(i));
            if (!values[i]) {
                EXPECT_TRUE(v.is_null());
                continue;
            }
            ASSERT_TRUE(v.is_integral_number()) << type->ToString() << " row " << i;
            EXPECT_EQ(v.as_int(), *values[i]) << type->ToString() << " row " << i;
        }
    }
}

// The refusal read_cell raises for an array it has no reading for: a plain
// std::logic_error naming the array's type. A subclass (std::length_error
// from a std::string built over a mis-cast array's garbage offsets, say) is
// the defect, not the refusal.
namespace {
std::string read_cell_refusal(const std::shared_ptr<arrow::DataType>& layout,
                              const arrow::Array& arr) {
    try {
        (void)rcd::read_cell(layout, arr, 0);
    } catch (const std::logic_error& e) {
        if (typeid(e) == typeid(std::logic_error)) {
            return e.what();
        }
        return std::string{"wrong exception: "} + typeid(e).name() + ": " + e.what();
    }
    return "no exception";
}
}  // namespace

TEST(RowLayoutReceive, ReadCellRefusesAnArrayItCannotReadInsteadOfMiscasting) {
    // A text layout over a non-text array used to be a blind cast to a
    // StringArray.
    arrow::Int16Builder b;
    ASSERT_TRUE(b.Append(7).ok());
    std::shared_ptr<arrow::Array> shorts;
    ASSERT_TRUE(b.Finish(&shorts).ok());
    EXPECT_NE(read_cell_refusal(arrow::utf8(), *shorts).find("int16"), std::string::npos)
        << read_cell_refusal(arrow::utf8(), *shorts);
    EXPECT_NE(read_cell_refusal(arrow::int16(), *shorts).find("int16"), std::string::npos)
        << read_cell_refusal(arrow::int16(), *shorts);
    // Only milliseconds are read as epoch ms.
    const auto micros = arrow::timestamp(arrow::TimeUnit::MICRO);
    const auto us = timestamp_array(micros, {5});
    EXPECT_NE(read_cell_refusal(micros, *us).find("timestamp[us]"), std::string::npos)
        << read_cell_refusal(micros, *us);
    // A millisecond layout over an array of another unit is refused too,
    // rather than handing its ticks on as epoch milliseconds.
    const auto millis = arrow::timestamp(arrow::TimeUnit::MILLI);
    const auto us_value = timestamp_array(micros, {1700000000123000});
    EXPECT_NE(read_cell_refusal(millis, *us_value).find("timestamp[us]"), std::string::npos)
        << read_cell_refusal(millis, *us_value);
    const auto seconds = timestamp_array(arrow::timestamp(arrow::TimeUnit::SECOND, "UTC"), {5});
    EXPECT_NE(read_cell_refusal(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"), *seconds)
                  .find("timestamp[s"),
              std::string::npos)
        << read_cell_refusal(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"), *seconds);
    // The zone is not part of the check: a zoned millisecond array under an
    // unzoned millisecond layout holds the same epoch milliseconds.
    const auto zoned = timestamp_array(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"), {42});
    EXPECT_EQ(rcd::read_cell(millis, *zoned, 0).as_int(), 42);
}

TEST(RowLayoutReceive, ATimestampCellTakesAnIntegralNumberAndOtherwiseIsNull) {
    const auto type = arrow::timestamp(arrow::TimeUnit::MILLI, "UTC");
    auto builder = rcd::make_cell_builder(type);
    ASSERT_TRUE(builder->type()->Equals(*type)) << builder->type()->ToString();
    const std::vector<cfg::JsonValue> cells = {
        cfg::JsonValue{std::int64_t{1700000000123}},
        cfg::JsonValue{std::numeric_limits<std::int64_t>::min()},
        cfg::JsonValue{5.0},
        cfg::JsonValue{5.5},
        cfg::JsonValue{1e300},
        cfg::JsonValue{std::string{"2023-11-14 22:13:20.123"}},
        cfg::JsonValue{true},
    };
    for (const auto& c : cells) {
        rcd::append_json_cell(*builder, *type, &c);
    }
    rcd::append_json_cell(*builder, *type, nullptr);
    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(builder->Finish(&out).ok());
    ASSERT_EQ(out->type_id(), arrow::Type::TIMESTAMP);
    const auto& ts = static_cast<const arrow::TimestampArray&>(*out);
    ASSERT_EQ(ts.length(), 8);
    EXPECT_EQ(ts.Value(0), 1700000000123);
    EXPECT_EQ(ts.Value(1), std::numeric_limits<std::int64_t>::min());
    EXPECT_EQ(ts.Value(2), 5);
    for (std::int64_t i = 3; i < 8; ++i) {
        EXPECT_TRUE(ts.IsNull(i)) << "row " << i;
    }
}

TEST(RowLayoutReceive, CellIsExactForATimestampLayoutTakesIntegersOnly) {
    const auto type = arrow::timestamp(arrow::TimeUnit::MILLI);
    const cfg::JsonValue integer{std::int64_t{1700000000123}};
    const cfg::JsonValue whole{5.0};
    const cfg::JsonValue fraction{5.5};
    const cfg::JsonValue text{std::string{"1700000000123"}};
    EXPECT_TRUE(rcd::cell_is_exact(*type, &integer));
    EXPECT_TRUE(rcd::cell_is_exact(*type, &whole));
    EXPECT_FALSE(rcd::cell_is_exact(*type, &fraction));
    EXPECT_FALSE(rcd::cell_is_exact(*type, &text));
    EXPECT_TRUE(rcd::cell_is_exact(*type, nullptr));
}
