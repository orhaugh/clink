// Wave 2 increment 1: source-side columnar JSON decode.
//
// json_string_to_row_columnar must be byte-equivalent to json_string_to_row
// (the row-form Kafka decode) while attaching an Arrow sidecar so the emitted
// batch is_columnar(). The interesting cases are the ones where build_column
// would SILENTLY coerce (wrong type, non-integer, number-in-string) or change
// the row shape (extra / missing column): the operator must fall back to the
// row form rather than emit a silently-divergent columnar batch.

#include <memory>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/sql/json_string_to_row_columnar.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

using clink::Batch;
using clink::BoundedChannel;
using clink::Emitter;
using clink::MapOperator;
using clink::Record;
using clink::StreamElement;
using clink::sql::JsonStringToRowColumnarOperator;
using clink::sql::Row;
using clink::sql::RowColumn;

namespace {

// int64 'a', double 'b', string 'c', bool 'd' - all columnar-capable types.
std::vector<RowColumn> demo_schema() {
    return {
        {"a", arrow::int64()},
        {"b", arrow::float64()},
        {"c", arrow::utf8()},
        {"d", arrow::boolean()},
    };
}

Batch<std::string> lines_batch(const std::vector<std::string>& lines) {
    Batch<std::string> b;
    for (const auto& l : lines) {
        b.emplace(std::string(l));
    }
    return b;
}

// Drive an Operator<string,Row> over one data batch; return the emitted element.
template <typename Op>
StreamElement<Row> run_one(Op& op, Batch<std::string> in) {
    BoundedChannel<StreamElement<Row>> ch(64);
    Emitter<Row> em(&ch);
    op.process(StreamElement<std::string>::data(std::move(in)), em);
    auto e = ch.try_pop();
    EXPECT_TRUE(e.has_value());
    return std::move(*e);
}

// The row-path oracle: identical to the json_string_to_row factory.
MapOperator<std::string, Row> make_row_oracle() {
    auto fmt = std::make_shared<clink::TextFormat<Row>>(clink::sql::row_json_text_format());
    return MapOperator<std::string, Row>(
        [fmt](const std::string& line) -> Row { return fmt->decode(line).value_or(Row{}); },
        "json_string_to_row");
}

// Canonical NDJSON for each row. The same formatter on both sides yields equal
// strings iff the rows are equal, so this is the byte-equivalence comparison.
std::vector<std::string> encoded_rows(const Batch<Row>& b) {
    auto fmt = clink::sql::row_json_text_format();
    std::vector<std::string> out;
    for (const auto& rec : b) {
        out.push_back(fmt.encode(rec.value()));
    }
    return out;
}

// Run both operators over the same lines and assert the columnar op fell back
// to row form AND stayed byte-equivalent to the row oracle. Used by every
// not-faithfully-representable case.
void expect_fallback_and_equivalent(const std::vector<std::string>& lines) {
    auto oracle = make_row_oracle();
    auto row_el = run_one(oracle, lines_batch(lines));
    ASSERT_TRUE(row_el.is_data());

    JsonStringToRowColumnarOperator col_op(demo_schema());
    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.is_data());

    const Batch<Row>& col_batch = col_el.as_data();
    EXPECT_FALSE(col_batch.is_columnar());
    EXPECT_EQ(encoded_rows(col_batch), encoded_rows(row_el.as_data()));
}

}  // namespace

// Conforming JSON: the columnar op emits a COLUMNAR batch whose rows are
// byte-equivalent to the row decode, and the sidecar is NOT materialised until
// a row accessor is touched.
TEST(JsonColumnarDecode, ConformingJsonFiresColumnarAndIsByteEquivalent) {
    const std::vector<std::string> lines = {
        R"({"a":1,"b":1.5,"c":"x","d":true})",
        R"({"a":2,"b":2.5,"c":"y","d":false})",
        R"({"a":-3,"b":0.0,"c":"","d":true})",
    };

    auto oracle = make_row_oracle();
    auto row_el = run_one(oracle, lines_batch(lines));
    ASSERT_TRUE(row_el.is_data());
    EXPECT_FALSE(row_el.as_data().is_columnar());

    JsonStringToRowColumnarOperator col_op(demo_schema());

    const auto mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);

    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.is_data());
    const Batch<Row>& col_batch = col_el.as_data();

    // Fires columnar and answers size() without materialising the sidecar.
    EXPECT_TRUE(col_batch.is_columnar());
    EXPECT_EQ(col_batch.size(), lines.size());
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);

    // Byte-equivalence. Touching the rows materialises the sidecar exactly once.
    EXPECT_EQ(encoded_rows(col_batch), encoded_rows(row_el.as_data()));
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before + 1);
}

// A line that is not a JSON object decodes to an empty Row on both sides; the
// columnar op must fall back (an empty Row would reconstruct as an all-null
// row) and stay byte-equivalent.
TEST(JsonColumnarDecode, NonJsonLineFallsBackToRowFormByteEquivalent) {
    expect_fallback_and_equivalent({
        R"({"a":1,"b":1.5,"c":"x","d":true})",
        "this is not json",
        R"({"a":2,"b":2.5,"c":"y","d":false})",
    });
}

// 'a' is int64 but the JSON carries a string. build_column would silently NULL
// it; the faithful guard must fall back so the string survives.
TEST(JsonColumnarDecode, WrongTypeValueFallsBackNotSilentlyNulled) {
    expect_fallback_and_equivalent({
        R"({"a":"oops","b":1.5,"c":"x","d":true})",
    });
}

// A non-integer in an int64 column would be truncated by build_column; the
// guard must fall back so the fractional value survives.
TEST(JsonColumnarDecode, NonIntegerInIntColumnFallsBack) {
    expect_fallback_and_equivalent({
        R"({"a":1.5,"b":1.5,"c":"x","d":true})",
    });
}

// An integer beyond INT64_MAX: the static_cast in build_column would overflow
// (lossy / UB), so the guard must fall back rather than emit a corrupted value.
TEST(JsonColumnarDecode, OutOfRangeIntegerFallsBack) {
    expect_fallback_and_equivalent({
        R"({"a":10000000000000000000,"b":1.5,"c":"x","d":true})",
    });
}

// An undeclared JSON field is dropped by the columnar build; fall back so it
// survives.
TEST(JsonColumnarDecode, ExtraColumnFallsBack) {
    expect_fallback_and_equivalent({
        R"({"a":1,"b":1.5,"c":"x","d":true,"extra":9})",
    });
}

// A missing declared column reconstructs as a present-null cell, which differs
// from the row decode (where the key is absent); fall back.
TEST(JsonColumnarDecode, MissingColumnFallsBack) {
    expect_fallback_and_equivalent({
        R"({"a":1,"b":1.5,"c":"x"})",
    });
}

// A declared FLOAT column is lossy on the double<->float round-trip, so the
// whole schema is excluded from the columnar path (always row form here).
TEST(JsonColumnarDecode, FloatSchemaTakesRowPath) {
    auto oracle = make_row_oracle();
    const std::vector<std::string> lines = {R"({"x":1.5})"};
    auto row_el = run_one(oracle, lines_batch(lines));

    JsonStringToRowColumnarOperator col_op({{"x", arrow::float32()}});
    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.is_data());
    EXPECT_FALSE(col_el.as_data().is_columnar());
    EXPECT_EQ(encoded_rows(col_el.as_data()), encoded_rows(row_el.as_data()));
}

// A partitioned source (every Kafka record carries source_partition) goes
// columnar, carrying source_partition through the engine-only
// __source_partition sidecar column so the downstream partition-aware watermark
// assigner can read it. The lazy materialize must restore source_partition (and
// event_time) onto the rows - the metadata the value-only oracle does not see.
TEST(JsonColumnarDecode, PartitionedFaithfulRecordsGoColumnarCarryingPartition) {
    const std::vector<std::string> lines = {
        R"({"a":1,"b":1.5,"c":"x","d":true})",
        R"({"a":2,"b":2.5,"c":"y","d":false})",
    };

    // Build a partitioned input the way the Kafka string source does: each
    // record carries a source_partition and an inline event_time.
    Batch<std::string> in;
    {
        Record<std::string> r0(lines[0], clink::EventTime{100});
        r0.set_source_partition(0);
        Record<std::string> r1(lines[1], clink::EventTime{200});
        r1.set_source_partition(3);
        in.push(std::move(r0));
        in.push(std::move(r1));
    }

    JsonStringToRowColumnarOperator col_op(demo_schema());
    const auto mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    auto col_el = run_one(col_op, std::move(in));
    ASSERT_TRUE(col_el.is_data());
    const Batch<Row>& out = col_el.as_data();

    // Fires columnar, with the partition carried as a sidecar column (read
    // without materialising rows).
    EXPECT_TRUE(out.is_columnar());
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);
    ASSERT_NE(out.arrow(), nullptr);
    auto pcol = out.arrow()->GetColumnByName(clink::sql::kSourcePartitionColumn);
    ASSERT_NE(pcol, nullptr);
    const auto& parr = static_cast<const arrow::Int32Array&>(*pcol);
    ASSERT_EQ(parr.length(), 2);
    EXPECT_EQ(parr.Value(0), 0);
    EXPECT_EQ(parr.Value(1), 3);

    // Values byte-equivalent to the row oracle...
    auto oracle = make_row_oracle();
    Batch<std::string> in2;
    in2.emplace(std::string(lines[0]));
    in2.emplace(std::string(lines[1]));
    auto row_el = run_one(oracle, std::move(in2));
    EXPECT_EQ(encoded_rows(out), encoded_rows(row_el.as_data()));

    // ...and the lazy materialize restores source_partition + event_time, so a
    // row consumer (or the assigner's row fallback) is metadata-equivalent.
    const auto& recs = out.records();
    ASSERT_EQ(recs.size(), 2u);
    ASSERT_TRUE(recs[0].source_partition().has_value());
    EXPECT_EQ(*recs[0].source_partition(), 0);
    ASSERT_TRUE(recs[1].source_partition().has_value());
    EXPECT_EQ(*recs[1].source_partition(), 3);
    ASSERT_TRUE(recs[0].event_time().has_value());
    EXPECT_EQ(recs[0].event_time()->millis(), 100);
    EXPECT_EQ(recs[1].event_time()->millis(), 200);
}

// A partitioned batch that is NOT value-faithful (here a wrong-type value in an
// int column) still falls back to the row form, and the fallback preserves
// source_partition (set on every record before the faithful check).
TEST(JsonColumnarDecode, PartitionedNonFaithfulFallsBackPreservingPartition) {
    Batch<std::string> in;
    {
        Record<std::string> r0(R"({"a":"oops","b":1.5,"c":"x","d":true})", clink::EventTime{100});
        r0.set_source_partition(2);
        in.push(std::move(r0));
    }

    JsonStringToRowColumnarOperator col_op(demo_schema());
    auto col_el = run_one(col_op, std::move(in));
    ASSERT_TRUE(col_el.is_data());
    const Batch<Row>& out = col_el.as_data();

    EXPECT_FALSE(out.is_columnar());  // wrong-type value forces the row fallback
    const auto& recs = out.records();
    ASSERT_EQ(recs.size(), 1u);
    ASSERT_TRUE(recs[0].source_partition().has_value());
    EXPECT_EQ(*recs[0].source_partition(), 2);
}

// The engine partition sidecar column must NEVER leak into a materialised Row
// value. The self-describing reader (rows_from_record_batch) is what a row-only
// downstream op (OVER / semi-join / top-N-per-key, none columnar) uses to
// materialise a columnar batch that still carries __source_partition - it must
// drop the column, or it surfaces as a spurious field in the output JSON.
TEST(JsonColumnarDecode, SourcePartitionColumnDoesNotLeakIntoMaterialisedRows) {
    Batch<std::string> in;
    {
        Record<std::string> r0(R"({"a":1,"b":1.5,"c":"x","d":true})", clink::EventTime{100});
        r0.set_source_partition(7);
        in.push(std::move(r0));
    }
    JsonStringToRowColumnarOperator col_op(demo_schema());
    auto col_el = run_one(col_op, std::move(in));
    ASSERT_TRUE(col_el.is_data());
    const Batch<Row>& out = col_el.as_data();
    ASSERT_TRUE(out.is_columnar());
    ASSERT_NE(out.arrow(), nullptr);
    // The sidecar carries the partition column (for the assigner)...
    ASSERT_NE(out.arrow()->GetColumnByName(clink::sql::kSourcePartitionColumn), nullptr);

    // ...but the self-describing reader must not inject it into Row.values.
    auto rows = clink::sql::rows_from_record_batch(*out.arrow());
    ASSERT_TRUE(rows.has_value());
    ASSERT_EQ(rows->size(), 1u);
    const auto& vals = (*rows)[0].value().values;
    EXPECT_EQ(vals.count(clink::sql::kSourcePartitionColumn), 0u);  // no leak
    EXPECT_EQ(vals.count("a"), 1u);
    EXPECT_EQ(vals.count("b"), 1u);
    EXPECT_EQ(vals.count("c"), 1u);
    EXPECT_EQ(vals.count("d"), 1u);
}

// A declared column in the engine-reserved "__" namespace would collide with
// the appended partition sidecar column (ambiguous name -> partition silently
// lost -> global-watermark collapse). Such a schema must take the row path.
TEST(JsonColumnarDecode, ReservedDunderColumnForcesRowPath) {
    std::vector<RowColumn> schema = {{"__source_partition", arrow::int64()}, {"a", arrow::int64()}};
    Batch<std::string> in;
    {
        Record<std::string> r0(R"({"__source_partition":5,"a":1})", clink::EventTime{1});
        r0.set_source_partition(2);
        in.push(std::move(r0));
    }
    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, std::move(in));
    ASSERT_TRUE(col_el.is_data());
    EXPECT_FALSE(col_el.as_data().is_columnar());  // forced row fallback, no collision
}

// A present-null value for a declared column round-trips faithfully (null cell
// -> null), so the batch still fires columnar.
TEST(JsonColumnarDecode, ExplicitNullValueStaysColumnar) {
    auto oracle = make_row_oracle();
    const std::vector<std::string> lines = {
        R"({"a":1,"b":null,"c":"x","d":true})",
    };
    auto row_el = run_one(oracle, lines_batch(lines));

    JsonStringToRowColumnarOperator col_op(demo_schema());
    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.is_data());
    const Batch<Row>& col_batch = col_el.as_data();
    // A JSON null decodes to a present-null cell on both sides, so it is
    // faithfully representable and the batch stays columnar.
    EXPECT_TRUE(col_batch.is_columnar());
    EXPECT_EQ(encoded_rows(col_batch), encoded_rows(row_el.as_data()));
}
