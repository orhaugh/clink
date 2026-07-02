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
