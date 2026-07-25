// Schema-driven columnar batcher for the SQL Row type: each declared
// column round-trips as its own typed Arrow column (over the real Arrow
// IPC wire path), nulls survive, and the schema/param (de)serialisation
// is lossless.

#ifndef CLINK_HAS_ARROW
#error "test_row_columnar_batcher requires Arrow"
#endif

#include <cstdint>
#include <memory>
#include <string>
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
    // A timestamp column is not in the v1 native set -> stored as utf8.
    std::vector<RowColumn> cols = {{"ts", arrow::timestamp(arrow::TimeUnit::MILLI)}};
    auto batcher = make_row_columnar_arrow_batcher(cols);
    auto schema = batcher.schema();
    ASSERT_EQ(schema->num_fields(), 2);
    EXPECT_EQ(schema->field(1)->type()->id(), arrow::Type::STRING);
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
