// Columnar-into-SQL, increment 3: the sidecar-preserving Row wire batcher.
//
// make_row_wire_batcher keeps columnar Row data columnar across a TM boundary
// while staying lossless for row-form data. Claims under test (each goes
// through the REAL wire path: build -> Arrow IPC bytes -> parse):
//   - a columnar Batch<Row> survives the wire AS columnar (sidecar set), with no
//     row materialization on either side, and its values round-trip exactly,
//   - a row-form Batch<Row> round-trips via the JSON binary-fallback layout and
//     arrives as a row batch (unchanged from the default batcher),
//   - nested ARRAY / MAP / ROW values (JsonValue arrays/objects) survive the
//     row-form path byte-for-byte - the switch to a columnar wire must NOT
//     regress structured-type pipelines,
//   - event-time survives both paths,
//   - the batcher exposes a null schema so the receiver skips its fixed-schema
//     equality gate (the columnar schema varies per edge).

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace {

using namespace clink;
using clink::sql::Row;
using clink::sql::RowColumn;

std::uint64_t materialize_count() {
    return detail::batch_materialize_counter().load(std::memory_order_relaxed);
}

std::string serialize(const Row& r) {
    return clink::config::JsonValue{clink::config::JsonObject{r.values}}.serialize(0);
}

// Push the batch through the real wire: build -> IPC stream bytes -> reparse.
std::optional<Batch<Row>> wire_roundtrip(const ArrowBatcher<Row>& b, const Batch<Row>& in) {
    auto rb = b.build(in);
    if (!rb) {
        return std::nullopt;
    }
    auto bytes = clink::arrow_batch_to_ipc(*rb);
    auto rb2 = clink::arrow_batch_from_ipc(bytes.data(), bytes.size());
    if (!rb2) {
        return std::nullopt;
    }
    return b.parse(*rb2);
}

Row scalar_row(std::int64_t amount, std::string region) {
    Row r;
    r.values["amount"] = clink::config::JsonValue{static_cast<double>(amount)};
    r.values["region"] = clink::config::JsonValue{std::move(region)};
    return r;
}

TEST(RowWireBatcher, NullSchemaSkipsRecvGate) {
    auto b = clink::sql::make_row_wire_batcher(clink::sql::row_json_codec());
    EXPECT_FALSE(static_cast<bool>(b.schema)) << "schema must be empty so the recv gate is skipped";
}

TEST(RowWireBatcher, ColumnarSurvivesWireAsColumnar) {
    const std::vector<RowColumn> schema = {{"amount", arrow::int64()}, {"region", arrow::utf8()}};
    const std::vector<Row> rows = {
        scalar_row(10, "eu"), scalar_row(60, "us"), scalar_row(50, "eu")};
    // Build a columnar Batch<Row> the way a Parquet source / columnar op would.
    auto col_batcher = clink::sql::make_row_columnar_arrow_batcher(schema);
    Batch<Row> in;
    in.emplace(rows[0], EventTime{100});
    in.emplace(rows[1], EventTime{200});
    in.emplace(rows[2], EventTime{300});
    Batch<Row> columnar{col_batcher.build(in), rows.size(), clink::sql::row_materialize_fn()};
    ASSERT_TRUE(columnar.is_columnar());

    auto wire = clink::sql::make_row_wire_batcher(clink::sql::row_json_codec());
    const auto before = materialize_count();
    auto out = wire_roundtrip(wire, columnar);
    ASSERT_TRUE(out.has_value());
    EXPECT_TRUE(out->is_columnar()) << "columnar data must arrive columnar after the wire";
    EXPECT_EQ(materialize_count() - before, 0u) << "no row decode on send or receive";

    // Now materialize (the test's decode) and check values + event-time.
    ASSERT_EQ(out->size(), 3u);
    std::vector<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& rec : *out) {
        ASSERT_TRUE(rec.event_time().has_value());
        got.emplace_back(static_cast<std::int64_t>(rec.value().values.at("amount").as_number()),
                         rec.event_time()->millis());
        EXPECT_TRUE(rec.value().has_column("region"));
    }
    EXPECT_EQ(
        got, (std::vector<std::pair<std::int64_t, std::int64_t>>{{10, 100}, {60, 200}, {50, 300}}));
}

TEST(RowWireBatcher, RowFormRoundTripsAsRows) {
    auto wire = clink::sql::make_row_wire_batcher(clink::sql::row_json_codec());
    Batch<Row> in;  // pure row-form, no sidecar
    in.emplace(scalar_row(7, "eu"), EventTime{11});
    in.emplace(scalar_row(8, "us"));

    auto out = wire_roundtrip(wire, in);
    ASSERT_TRUE(out.has_value());
    EXPECT_FALSE(out->is_columnar()) << "row-form data takes the JSON fallback (a row batch)";
    ASSERT_EQ(out->size(), 2u);
    EXPECT_EQ(serialize(out->records()[0].value()), serialize(scalar_row(7, "eu")));
    EXPECT_EQ(serialize(out->records()[1].value()), serialize(scalar_row(8, "us")));
    EXPECT_TRUE(out->records()[0].event_time().has_value());
    EXPECT_EQ(out->records()[0].event_time()->millis(), 11);
    EXPECT_FALSE(out->records()[1].event_time().has_value());
}

TEST(RowWireBatcher, UnsupportedSidecarSchemaRejectedAtParse) {
    // A columnar frame whose column 0 is not an int64 event-time column is an
    // unexpected sidecar. parse() must reject it (nullopt) so the receiver fails
    // loudly rather than silently materializing zero rows and dropping the batch.
    auto wire = clink::sql::make_row_wire_batcher(clink::sql::row_json_codec());
    arrow::StringBuilder c0;
    (void)c0.Append("not-a-timestamp");
    arrow::Int64Builder c1;
    (void)c1.Append(1);
    std::shared_ptr<arrow::Array> a0;
    std::shared_ptr<arrow::Array> a1;
    (void)c0.Finish(&a0);
    (void)c1.Finish(&a1);
    auto rb = arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::utf8()),
                                                      arrow::field("x", arrow::int64())}),
                                       1,
                                       {a0, a1});
    EXPECT_FALSE(wire.parse(*rb).has_value()) << "unexpected sidecar schema must be rejected";
}

TEST(RowWireBatcher, NestedArrayMapRowSurviveLossless) {
    // The switch to a columnar wire must not regress ARRAY/MAP/ROW pipelines:
    // structured JsonValues ride the row-form JSON path and must round-trip
    // exactly (a typed columnar wire would have stringified them).
    Row r;
    // ARRAY: [1, 2, 3]
    clink::config::JsonArray arr;
    arr.push_back(clink::config::JsonValue{1.0});
    arr.push_back(clink::config::JsonValue{2.0});
    arr.push_back(clink::config::JsonValue{3.0});
    r.values["tags"] = clink::config::JsonValue{std::move(arr)};
    // ROW / MAP: {"a": 1, "b": "x"}
    clink::config::JsonObject obj;
    obj["a"] = clink::config::JsonValue{1.0};
    obj["b"] = clink::config::JsonValue{std::string{"x"}};
    r.values["nested"] = clink::config::JsonValue{std::move(obj)};
    r.values["scalar"] = clink::config::JsonValue{std::string{"hello"}};

    Batch<Row> in;
    in.emplace(r);

    auto wire = clink::sql::make_row_wire_batcher(clink::sql::row_json_codec());
    auto out = wire_roundtrip(wire, in);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->size(), 1u);
    EXPECT_EQ(serialize(out->records()[0].value()), serialize(r))
        << "nested ARRAY/MAP/ROW values must survive the wire byte-for-byte";
}

}  // namespace
