// Wave 2 increment 1: source-side columnar JSON decode.
//
// json_string_to_row_columnar must be byte-equivalent to json_string_to_row
// (the row-form Kafka decode) while attaching an Arrow sidecar so the emitted
// batch is_columnar(). The interesting cases are the ones where build_column
// would SILENTLY coerce (wrong type, non-integer, number-in-string) or change
// the row shape (extra / missing column): the operator must fall back to the
// row form rather than emit a silently-divergent columnar batch.

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <memory>
#include <string>
#include <utility>
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

// The row-path oracle for a TYPED schema: what json_string_to_row now does when
// the planner hands it the table's columns (declared DECIMAL ingested exactly and
// quantised, declared FLOAT rounded to float precision). This is the reference the
// columnar decoder must match for a schema carrying either type - the plain oracle
// above is only the reference for schemas that declare neither.
MapOperator<std::string, Row> make_typed_row_oracle(const std::vector<RowColumn>& schema) {
    auto fmt = std::make_shared<clink::TextFormat<Row>>(
        clink::sql::row_json_text_format_for_columns(schema));
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

// FULL-PRECISION per-cell rendering, for cases where encoded_rows is not
// discriminating enough. The text format encodes doubles with "%g" (6 significant
// digits), so 0.1 and (double)(float)0.1 both render as "0.1" - meaning an
// encoded_rows comparison alone would pass even if one carrier skipped a float
// coercion entirely. This renders each numeric cell with all 17 digits, so the
// FLOAT/DECIMAL parity tests actually bite. Non-numeric cells (including the
// dec-string form a DECIMAL column carries) compare by their exact text.
std::vector<std::string> exact_cells(const Batch<Row>& b) {
    std::vector<std::string> out;
    for (const auto& rec : b) {
        std::string line;
        for (const auto& [k, v] : rec.value().values) {
            char buf[40] = {0};
            if (v.is_number() && !clink::config::is_dec_string(v)) {
                std::snprintf(buf, sizeof(buf), "%.17g", v.as_number());
                line += k.str() + "=" + buf + ";";
            } else {
                line += k.str() + "=" + v.serialize(0) + ";";
            }
        }
        out.push_back(std::move(line));
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
TEST(JsonColumnarDecode, FloatSchemaGoesColumnarAndMatchesTheTypedRowDecode) {
    const std::vector<RowColumn> schema = {{"x", arrow::float32()}};
    // 1.5 is exact in float32; 0.1 is not, and is where a carrier that did NOT
    // round would diverge - which is why the row decode coerces too.
    const std::vector<std::string> lines = {
        R"({"x":1.5})", R"({"x":0.1})", R"({"x":3.4028235e38})"};

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));

    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.is_data());
    EXPECT_TRUE(col_el.as_data().is_columnar()) << "a declared FLOAT column is columnar-capable";
    EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()));
}

// The declared width is honoured: a REAL column carries float precision, not the
// full double the JSON numeral parsed to. Both carriers, identically - this is a
// deliberate, documented behaviour change from before inc4.
TEST(JsonColumnarDecode, FloatColumnRoundsToDeclaredPrecisionOnBothCarriers) {
    const std::vector<RowColumn> schema = {{"x", arrow::float32()}};
    const std::vector<std::string> lines = {R"({"x":0.1})"};

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));
    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));

    // Compared at full precision on purpose: the text encoder uses "%g", under
    // which the coerced value and the raw double are indistinguishable.
    EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()));

    ASSERT_EQ(row_el.as_data().size(), 1U);
    const double got = row_el.as_data()[0].value().values.find("x")->second.as_number();
    EXPECT_EQ(got, static_cast<double>(static_cast<float>(0.1)))
        << "a declared FLOAT column must carry float precision";
    EXPECT_NE(got, 0.1) << "...which is NOT the double the numeral parsed to";
}

TEST(JsonColumnarDecode, DecimalSchemaGoesColumnarAndMatchesTheTypedRowDecode) {
    const std::vector<RowColumn> schema = {{"x", arrow::decimal128(10, 2)}};
    const std::vector<std::string> lines = {R"({"x":10.03})", R"({"x":0})", R"({"x":-7.5})"};

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));

    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.is_data());
    EXPECT_TRUE(col_el.as_data().is_columnar()) << "a declared DECIMAL column is columnar-capable";
    EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()));
}

// The point of the exercise, for money columns: a numeral with more significant
// digits than a double can hold is ingested EXACTLY on the columnar carrier, by
// re-reading the raw token from the line rather than the already-rounded parse.
// 17 significant digits, so the double parse is provably lossy.
TEST(JsonColumnarDecode, DecimalColumnIngestsExactDigitsBeyondDoublePrecision) {
    const std::vector<RowColumn> schema = {{"x", arrow::decimal128(30, 2)}};
    const std::vector<std::string> lines = {R"({"x":12345678901234.57})"};

    auto oracle = make_typed_row_oracle(schema);
    const auto row_out = encoded_rows(run_one(oracle, lines_batch(lines)).as_data());

    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.as_data().is_columnar());
    const auto col_out = encoded_rows(col_el.as_data());

    ASSERT_EQ(col_out.size(), 1U);
    EXPECT_EQ(col_out, row_out) << "carriers must agree on an exact decimal";
    EXPECT_EQ(col_out[0], R"({"x":12345678901234.57})") << "digits must survive: " << col_out[0];
}

// Quantisation to the declared scale, on both carriers: more input digits than
// the column holds are rounded (HALF_UP), not truncated or carried.
TEST(JsonColumnarDecode, DecimalColumnQuantisesToDeclaredScaleOnBothCarriers) {
    const std::vector<RowColumn> schema = {{"x", arrow::decimal128(10, 2)}};
    const std::vector<std::string> lines = {R"({"x":1.005})", R"({"x":2.994})"};

    auto oracle = make_typed_row_oracle(schema);
    const auto row_out = encoded_rows(run_one(oracle, lines_batch(lines)).as_data());

    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));
    const auto col_out = encoded_rows(col_el.as_data());

    EXPECT_EQ(col_out, row_out);
    ASSERT_EQ(row_out.size(), 2U);
    EXPECT_EQ(row_out[0], R"({"x":1.01})") << row_out[0];
    EXPECT_EQ(row_out[1], R"({"x":2.99})") << row_out[1];
}

// A value that does not denote a decimal at all (a non-numeric string) cannot be
// stored in a DECIMAL128 column, and the row decode leaves it as-is - so the
// batch must fall back rather than store something else.
TEST(JsonColumnarDecode, NonDecimalValueInDecimalColumnFallsBack) {
    const std::vector<RowColumn> schema = {{"x", arrow::decimal128(10, 2)}};
    const std::vector<std::string> lines = {R"({"x":"not a number"})"};

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));

    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.is_data());
    EXPECT_FALSE(col_el.as_data().is_columnar());
    EXPECT_EQ(encoded_rows(col_el.as_data()), encoded_rows(row_el.as_data()));
}

// A mixed schema - the ordinary types alongside both newly-capable ones - stays
// columnar and equivalent, so the widening composes rather than working only in
// isolation.
TEST(JsonColumnarDecode, MixedSchemaWithFloatAndDecimalStaysColumnarAndEquivalent) {
    const std::vector<RowColumn> schema = {{"id", arrow::int64()},
                                           {"name", arrow::utf8()},
                                           {"ratio", arrow::float32()},
                                           {"amount", arrow::decimal128(12, 2)},
                                           {"ok", arrow::boolean()}};
    const std::vector<std::string> lines = {
        R"({"id":1,"name":"a","ratio":0.25,"amount":10.05,"ok":true})",
        R"({"id":2,"name":"b","ratio":0.1,"amount":-3.999,"ok":false})"};

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));

    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.is_data());
    EXPECT_TRUE(col_el.as_data().is_columnar());
    EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()));
}

// A declared TIMESTAMP column is not a distinct columnar-capable type: the
// planner writes it with the V1 code `str`, so the decoder sees utf8. A JSON
// STRING value round-trips faithfully so the batch fires columnar as a string
// column; a numeric (epoch) value is not a string and forces the row fallback.
// Both stay byte-equivalent to the row decode, which is the contract.
TEST(JsonColumnarDecode, TimestampStringColumnRidesStringPathEquivalent) {
    const std::vector<RowColumn> declared = {{"a", arrow::int64()},
                                             {"ts", arrow::timestamp(arrow::TimeUnit::MILLI)}};
    // The first layout, the one a deployment not admitted to the second decodes
    // with: TIMESTAMP is carried as text.
    const auto schema = clink::sql::parse_row_schema(
        clink::sql::serialize_row_schema(declared, clink::sql::RowLayout::V1));

    // String timestamp value: utf8 round-trip is faithful -> columnar.
    {
        auto oracle = make_row_oracle();
        const std::vector<std::string> lines = {R"({"a":1,"ts":"2026-06-29T12:00:00Z"})"};
        auto row_el = run_one(oracle, lines_batch(lines));
        JsonStringToRowColumnarOperator col_op(schema);
        auto col_el = run_one(col_op, lines_batch(lines));
        ASSERT_TRUE(col_el.is_data());
        EXPECT_TRUE(col_el.as_data().is_columnar());  // utf8 string round-trips
        EXPECT_EQ(encoded_rows(col_el.as_data()), encoded_rows(row_el.as_data()));
    }
    // Numeric timestamp value into the utf8 column: not a string -> row fallback,
    // still byte-equivalent (the row decode keeps the number).
    {
        auto oracle = make_row_oracle();
        const std::vector<std::string> lines = {R"({"a":1,"ts":1719662400000})"};
        auto row_el = run_one(oracle, lines_batch(lines));
        JsonStringToRowColumnarOperator col_op(schema);
        auto col_el = run_one(col_op, lines_batch(lines));
        ASSERT_TRUE(col_el.is_data());
        EXPECT_FALSE(col_el.as_data().is_columnar());  // number in a utf8 column -> fallback
        EXPECT_EQ(encoded_rows(col_el.as_data()), encoded_rows(row_el.as_data()));
    }
    // The second layout, the one an admitted deployment decodes with: TIMESTAMP
    // and TIMESTAMPTZ are timestamp(ms[, "UTC"]) columns holding the epoch
    // milliseconds, and the decode takes a value columnar exactly when it would
    // take it into a BIGINT column. Each value is checked on both arms against
    // the row decode the planner gives json_string_to_row, which reads the
    // schema in the first layout.
    for (const bool zoned : {false, true}) {
        SCOPED_TRACE(zoned ? "TIMESTAMPTZ" : "TIMESTAMP");
        const std::vector<RowColumn> typed = {
            {"a", arrow::int64()},
            {"ts",
             zoned ? arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")
                   : arrow::timestamp(arrow::TimeUnit::MILLI)}};
        const auto first = clink::sql::parse_row_schema(
            clink::sql::serialize_row_schema(typed, clink::sql::RowLayout::V1));
        const auto second = clink::sql::parse_row_schema(
            clink::sql::serialize_row_schema(typed, clink::sql::RowLayout::V2),
            clink::sql::RowLayout::V2);
        ASSERT_EQ(second[1].type->id(), arrow::Type::TIMESTAMP);
        // `force_dom` adds a duplicate key, which the on-demand arm declines, so
        // the DOM arm decides instead; the counter says which arm did.
        const auto expect = [&](const std::vector<std::string>& values, bool columnar) {
            SCOPED_TRACE(values.back());
            for (const bool force_dom : {false, true}) {
                SCOPED_TRACE(force_dom ? "DOM arm" : "on-demand arm");
                std::vector<std::string> lines;
                for (const auto& v : values) {
                    lines.push_back((force_dom ? R"({"a":1,"a":1,"ts":)" : R"({"a":1,"ts":)") + v +
                                    "}");
                }
                auto oracle = make_typed_row_oracle(first);
                auto row_el = run_one(oracle, lines_batch(lines));
                JsonStringToRowColumnarOperator col_op(second);
                const auto dom_before = clink::detail::json_columnar_dom_arm_counter().load();
                auto col_el = run_one(col_op, lines_batch(lines));
                const auto dom_runs =
                    clink::detail::json_columnar_dom_arm_counter().load() - dom_before;
                ASSERT_TRUE(col_el.is_data());
                EXPECT_EQ(col_el.as_data().is_columnar(), columnar);
                EXPECT_EQ(dom_runs, (force_dom && columnar) ? 1U : 0U);
                EXPECT_EQ(encoded_rows(col_el.as_data()), encoded_rows(row_el.as_data()));
                EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()));
                if (columnar && col_el.as_data().is_columnar()) {
                    const auto& sidecar = *col_el.as_data().arrow();
                    const auto column = sidecar.GetColumnByName("ts");
                    ASSERT_NE(column, nullptr);
                    EXPECT_TRUE(column->type()->Equals(*second[1].type))
                        << column->type()->ToString();
                }
            }
        };
        // Epoch-millisecond integers go columnar, the range's ends, negative
        // epochs, values past 2^53 and a null included.
        expect({"1719662400000"}, true);
        expect({"1719662400000", "-1", "0", "-62135596800000"}, true);
        expect({"9007199254740993", "-9007199254740993"}, true);
        expect({"9223372036854775807", "-9223372036854775807"}, true);
        expect({"1719662400000", "null"}, true);
        // An integral numeral, digit text, ISO text, a fraction, a value outside
        // int64 and a boolean send the batch to the row decode, and the rows are
        // the same. The row decode keeps 5.0 a double, and the evaluator's
        // arithmetic follows the value's kind, so an integer cell for it would
        // change what `ts / 1000` computes even though it prints the same.
        expect({"5.0"}, false);
        expect({"1719662400000", "1700000000500.0"}, false);
        expect({"1719662400000", R"("1719662400000")"}, false);
        expect({R"("2024-06-29T12:00:00Z")"}, false);
        expect({"1719662400000.5"}, false);
        expect({"9223372036854775808"}, false);
        expect({"-1e300"}, false);
        expect({"true"}, false);
    }
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
// ---- projection pushdown ----------------------------------------------------------
//
// The bridge can be told to build only some declared columns. The design decision under
// test: a declared-but-unprojected column is CONSUMED AND DISCARDED, not treated as
// undeclared. Narrowing the declared schema instead would make every unprojected field
// "undeclared", bail every batch on the faithfulness gate, and after eight consecutive
// failures the damper would abandon the columnar path entirely - so the "optimisation"
// would disable the fast path it was meant to speed up.

// The projected columns come through; the unprojected ones are gone; the batch is still
// COLUMNAR (that is the whole point).
TEST(JsonColumnarDecode, ProjectionKeepsOnlyTheRequestedColumnsAndStaysColumnar) {
    const std::vector<std::string> lines = {
        R"({"a":1,"b":1.5,"c":"x","d":true})",
        R"({"a":2,"b":2.5,"c":"y","d":false})",
    };
    JsonStringToRowColumnarOperator op(demo_schema(), {"a", "c"});
    auto el = run_one(op, lines_batch(lines));
    ASSERT_TRUE(el.is_data());
    const Batch<Row>& b = el.as_data();

    EXPECT_TRUE(b.is_columnar())
        << "an unprojected column must be consumed and discarded, NOT treated as undeclared "
           "- treating it as undeclared bails the batch and kills the columnar path";
    EXPECT_EQ(b.size(), lines.size());

    // The emitted Arrow schema carries only the projected columns (plus the mandatory
    // event-time column 0 the carrier prepends).
    const auto& rb = b.arrow();
    ASSERT_NE(rb, nullptr);
    EXPECT_EQ(rb->schema()->GetFieldIndex("a") >= 0, true);
    EXPECT_EQ(rb->schema()->GetFieldIndex("c") >= 0, true);
    EXPECT_EQ(rb->schema()->GetFieldIndex("b"), -1) << "unprojected column b was still built";
    EXPECT_EQ(rb->schema()->GetFieldIndex("d"), -1) << "unprojected column d was still built";

    for (const auto& rec : b) {
        const auto& v = rec.value().values;
        EXPECT_TRUE(v.find("a") != v.end());
        EXPECT_TRUE(v.find("c") != v.end());
        EXPECT_TRUE(v.find("b") == v.end());
        EXPECT_TRUE(v.find("d") == v.end());
    }
}

// The gate must NOT be weakened by projection. A genuinely undeclared field still bails,
// even when it would have been unprojected anyway - otherwise projection would silently
// turn a faithfulness check into a pass and let an unknown field be dropped.
TEST(JsonColumnarDecode, ProjectionDoesNotWeakenTheUndeclaredFieldGate) {
    JsonStringToRowColumnarOperator op(demo_schema(), {"a"});
    Batch<std::string> in;
    in.emplace(std::string(R"({"a":1,"b":1.5,"c":"x","d":true,"surprise":9})"));
    auto el = run_one(op, std::move(in));
    ASSERT_TRUE(el.is_data());
    EXPECT_FALSE(el.as_data().is_columnar())
        << "an undeclared field must still bail the columnar carrier under projection";
}

// A missing DECLARED column still bails, projected or not: the gate's job is to prove the
// record matched the declared schema, and projection only changes what is stored.
TEST(JsonColumnarDecode, ProjectionStillRequiresEveryDeclaredColumnPresent) {
    JsonStringToRowColumnarOperator op(demo_schema(), {"a"});
    Batch<std::string> in;
    in.emplace(std::string(R"({"a":1,"b":1.5,"c":"x"})"));  // d missing
    auto el = run_one(op, std::move(in));
    ASSERT_TRUE(el.is_data());
    EXPECT_FALSE(el.as_data().is_columnar())
        << "a missing declared column must bail even when it is unprojected";
}

// Both carriers of this one operator must agree. The row FALLBACK has to drop exactly the
// same columns, or a batch's shape would depend on which arm decoded it - and it must keep
// the TYPED decode while doing so, which is why the projection goes through
// row_json_text_format_for_columns_projected rather than the decimal-only helper.
TEST(JsonColumnarDecode, ProjectedRowFallbackDropsTheSameColumns) {
    // "surprise" forces the row fallback; the projection must still apply there.
    JsonStringToRowColumnarOperator op(demo_schema(), {"a", "c"});
    Batch<std::string> in;
    in.emplace(std::string(R"({"a":7,"b":1.5,"c":"z","d":true,"surprise":1})"));
    auto el = run_one(op, std::move(in));
    ASSERT_TRUE(el.is_data());
    ASSERT_FALSE(el.as_data().is_columnar()) << "expected the row fallback for this input";
    ASSERT_EQ(el.as_data().size(), 1u);
    const auto& v = el.as_data()[0].value().values;
    EXPECT_TRUE(v.find("a") != v.end());
    EXPECT_TRUE(v.find("c") != v.end());
    EXPECT_TRUE(v.find("b") == v.end()) << "the row fallback kept an unprojected column, so "
                                           "the two carriers disagree on row shape";
    EXPECT_TRUE(v.find("d") == v.end());
}

// An empty keep-list means keep everything - the behaviour every caller had before
// projection existed.
TEST(JsonColumnarDecode, EmptyProjectionKeepsEveryColumn) {
    const std::vector<std::string> lines = {R"({"a":1,"b":1.5,"c":"x","d":true})"};
    JsonStringToRowColumnarOperator with_empty(demo_schema(), {});
    JsonStringToRowColumnarOperator without(demo_schema());
    auto a = run_one(with_empty, lines_batch(lines));
    auto b = run_one(without, lines_batch(lines));
    ASSERT_TRUE(a.is_data());
    ASSERT_TRUE(b.is_data());
    EXPECT_TRUE(a.as_data().is_columnar());
    EXPECT_EQ(exact_cells(a.as_data()), exact_cells(b.as_data()));
}

// Values must be unchanged by projection: a projected column decodes to exactly what it
// would have without the keep-list, including declared FLOAT and DECIMAL semantics.
TEST(JsonColumnarDecode, ProjectedValuesMatchTheUnprojectedDecode) {
    const std::vector<std::string> lines = {
        R"({"a":1,"b":0.1,"c":"x","d":true})",
        R"({"a":-2,"b":2.5,"c":"","d":false})",
    };
    JsonStringToRowColumnarOperator full(demo_schema());
    JsonStringToRowColumnarOperator proj(demo_schema(), {"a", "b"});
    auto f = run_one(full, lines_batch(lines));
    auto p = run_one(proj, lines_batch(lines));
    ASSERT_TRUE(f.is_data());
    ASSERT_TRUE(p.is_data());
    ASSERT_EQ(f.as_data().size(), p.as_data().size());
    for (std::size_t i = 0; i < p.as_data().size(); ++i) {
        const auto& fv = f.as_data()[i].value().values;
        const auto& pv = p.as_data()[i].value().values;
        for (const char* col : {"a", "b"}) {
            auto fit = fv.find(col);
            auto pit = pv.find(col);
            ASSERT_TRUE(fit != fv.end() && pit != pv.end());
            EXPECT_EQ(fit->second.serialize(0), pit->second.serialize(0))
                << "column " << col << " decoded differently under projection";
        }
    }
}

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

// A schema with DUPLICATE declared column names defeats the count+per-key-find
// faithfulness gate (a line with an extra undeclared field has the same size as
// the inflated column count and the duplicate key is found twice, so the extra
// field would be silently dropped while the batch still went columnar). The
// catalog does not reject duplicate names, so the operator must force the row
// path for such a schema.
TEST(JsonColumnarDecode, DuplicateColumnNameForcesRowPath) {
    std::vector<RowColumn> schema = {{"a", arrow::int64()}, {"a", arrow::int64()}};
    JsonStringToRowColumnarOperator col_op(schema);
    // size==2 line matches the inflated resolved_.size()==2 - the gap the guard closes.
    auto col_el = run_one(col_op, lines_batch({R"({"a":1,"b":2})"}));
    ASSERT_TRUE(col_el.is_data());
    EXPECT_FALSE(col_el.as_data().is_columnar());  // forced row fallback, no silent drop
    // Row fallback preserves the full object (both a and b), like json_string_to_row.
    const auto& vals = col_el.as_data().records()[0].value().values;
    EXPECT_EQ(vals.count("a"), 1u);
    EXPECT_EQ(vals.count("b"), 1u);
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

// The adaptive damper (columnar decode is the planner DEFAULT, so a
// systematically unfaithful stream must not pay a double parse forever):
// after 8 consecutive fallback batches the operator stops attempting the
// columnar parse - a faithful batch inside the damped stretch decodes
// row-form, still byte-equivalent - and probes again every 64th batch; a
// faithful probe goes columnar and re-arms the always-attempt mode.
TEST(JsonColumnarDecode, AdaptiveDamperStopsAttemptingAndReArms) {
    JsonStringToRowColumnarOperator op(demo_schema());
    auto oracle = make_row_oracle();
    const std::vector<std::string> bad = {R"({"a":1,"b":1.5,"c":"x","d":true,"extra":1})"};
    const std::vector<std::string> good = {R"({"a":1,"b":1.5,"c":"x","d":true})"};

    // 8 unfaithful batches: each attempts, falls back row-form, and stays
    // byte-equivalent to the row oracle.
    for (int i = 0; i < 8; ++i) {
        auto el = run_one(op, lines_batch(bad));
        ASSERT_TRUE(el.is_data());
        EXPECT_FALSE(el.as_data().is_columnar());
        auto want = run_one(oracle, lines_batch(bad));
        EXPECT_EQ(encoded_rows(el.as_data()), encoded_rows(want.as_data()));
    }

    // Damped: even a FAITHFUL batch decodes row-form (no attempt), content
    // unchanged.
    for (int i = 0; i < 63; ++i) {
        auto el = run_one(op, lines_batch(good));
        ASSERT_TRUE(el.is_data());
        EXPECT_FALSE(el.as_data().is_columnar()) << "batch " << i << " should still be damped";
        auto want = run_one(oracle, lines_batch(good));
        EXPECT_EQ(encoded_rows(el.as_data()), encoded_rows(want.as_data()));
    }

    // The 64th batch since damping is the probe: faithful, so it goes
    // columnar and re-arms.
    {
        auto el = run_one(op, lines_batch(good));
        ASSERT_TRUE(el.is_data());
        EXPECT_TRUE(el.as_data().is_columnar()) << "probe batch should attempt and succeed";
    }
    // Re-armed: the next faithful batch is columnar immediately.
    {
        auto el = run_one(op, lines_batch(good));
        ASSERT_TRUE(el.is_data());
        EXPECT_TRUE(el.as_data().is_columnar());
    }
}

// Differential coverage for the on-demand decoder against the row decode, aimed
// at where a hand-written field walk can realistically diverge from building a
// DOM: string unescaping. The on-demand path takes keys through
// unescaped_key() and values through get_string(), which are different code
// from the DOM's unescaping, and a mismatch here would silently corrupt data
// rather than fall back.
TEST(JsonColumnarDecode, EscapedAndUnicodeStringsMatchTheRowDecode) {
    const std::vector<RowColumn> schema = {{"a", arrow::int64()}, {"s", arrow::utf8()}};
    const std::vector<std::string> lines = {
        R"({"a":1,"s":"plain"})",
        R"({"a":2,"s":"with \"quotes\" inside"})",
        R"({"a":3,"s":"back\\slash"})",
        R"({"a":4,"s":"tab\там"})",
        R"({"a":5,"s":"newline\nsecond"})",
        R"({"a":6,"s":"unicode éü中"})",
        R"({"a":7,"s":""})",
        R"({"a":8,"s":"emoji 😀 end"})",
        R"({"a":9,"s":"slash\/escaped"})",
    };

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));
    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));

    ASSERT_TRUE(col_el.is_data());
    EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()))
        << "on-demand string decoding must match the row decode byte for byte";
}

// An escaped KEY must still produce the right row, and this test previously did not
// check that: its JSON said "b" with a comment claiming the escape was there, so the
// escape had been lost and the case went uncovered. With a real b in the input it
// bites.
//
// The decoder compares the RAW (still-escaped) key against declared column names,
// because unescaping every key first was a large share of decode cost. An escaped key
// therefore does not match and the ON-DEMAND arm bails - but the fallback beneath it is
// the DOM columnar arm, which parses with full unescaping, so the batch still comes out
// COLUMNAR and correct. The escape costs the fastest path and nothing else. What must
// never happen is the field being dropped or filed under the wrong column, which is
// what the cell comparison asserts.
TEST(JsonColumnarDecode, EscapedKeyFallsBackAndStillResolvesToItsColumn) {
    const std::vector<RowColumn> schema = {{"a", arrow::int64()}, {"b", arrow::int64()}};
    // "\u0062" is 'b', so this document IS {"a":1,"b":2} to any conforming reader.
    const std::vector<std::string> lines = {R"({"a":1,"\u0062":2})"};

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));
    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));

    ASSERT_TRUE(col_el.is_data());
    EXPECT_TRUE(col_el.as_data().is_columnar())
        << "the DOM columnar arm unescapes, so an escaped key costs the on-demand path "
           "but not the columnar carrier";
    EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()));
}

// The same dependency from the other side: an escaped key in the MIDDLE of a batch must
// not corrupt the records around it.
TEST(JsonColumnarDecode, EscapedKeyMidBatchDoesNotCorruptNeighbours) {
    const std::vector<RowColumn> schema = {{"a", arrow::int64()}, {"b", arrow::int64()}};
    const std::vector<std::string> lines = {
        R"({"a":1,"b":2})",
        R"({"a":3,"\u0062":4})",  // escaped key, mid-batch
        R"({"a":5,"b":6})",
    };
    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));
    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));

    ASSERT_TRUE(col_el.is_data());
    EXPECT_TRUE(col_el.as_data().is_columnar());
    // The neighbours of the escaped record must be untouched, which is the point.
    EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()));
}

// A BIGINT past 2^53 through the DOM columnar arm. The on-demand arm bails on a
// duplicate key, and the DOM arm, whose parse keeps one value per key exactly as the row
// decode does, takes the batch. It must read an integral number exactly: 2^53 + 1 has
// no double, and reading it through one lands 2^53 in the column while the row decode
// keeps the integer. The duplicate repeats the same value so which one a reader keeps
// does not matter.
TEST(JsonColumnarDecode, DomArmKeepsABigintPastTwoToTheFiftyThree) {
    const std::vector<RowColumn> schema = {{"a", arrow::int64()}, {"b", arrow::int64()}};
    const std::vector<std::string> lines = {
        R"({"a":9007199254740993,"b":-9007199254740993,"b":-9007199254740993})",
        R"({"a":-9007199254740995,"b":9007199254740995})",
    };

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));
    JsonStringToRowColumnarOperator col_op(schema);
    const auto dom_before = clink::detail::json_columnar_dom_arm_counter().load();
    auto col_el = run_one(col_op, lines_batch(lines));

    ASSERT_TRUE(col_el.is_data());
    ASSERT_TRUE(col_el.as_data().is_columnar())
        << "the DOM columnar arm must take this batch, or the case tests the row path";
    ASSERT_EQ(clink::detail::json_columnar_dom_arm_counter().load() - dom_before, 1U)
        << "the on-demand arm took the batch, so the case no longer tests the DOM arm";
    EXPECT_EQ(encoded_rows(col_el.as_data()), encoded_rows(row_el.as_data()));
    const auto& sidecar = *col_el.as_data().arrow();
    const auto& a = static_cast<const arrow::Int64Array&>(*sidecar.GetColumnByName("a"));
    const auto& b = static_cast<const arrow::Int64Array&>(*sidecar.GetColumnByName("b"));
    EXPECT_EQ(a.Value(0), 9007199254740993);
    EXPECT_EQ(b.Value(0), -9007199254740993);
    EXPECT_EQ(a.Value(1), -9007199254740995);
    EXPECT_EQ(b.Value(1), 9007199254740995);
}

// A DOUBLE column receiving an integer past 2^53. The row decode rounds it to
// the nearest double (coerce_row_doubles), but it has no exact double, so both
// arms leave it to the row decode rather than decide it themselves. An integer
// up to 2^53 has a double, reads back as the same number, and stays columnar. `force_dom` adds a
// duplicate key, which the on-demand arm declines, so the DOM arm decides instead; the counter says
// which arm did.
namespace {
void expect_double_integer_parity(const std::vector<std::string>& values,
                                  bool force_dom,
                                  bool columnar) {
    const std::vector<RowColumn> schema = {{"x", arrow::float64()}, {"y", arrow::int64()}};
    std::vector<std::string> lines;
    lines.reserve(values.size());
    for (const auto& v : values) {
        lines.push_back(R"({"x":)" + v + (force_dom ? R"(,"y":1,"y":1})" : R"(,"y":1})"));
    }
    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));
    JsonStringToRowColumnarOperator col_op(schema);
    const auto dom_before = clink::detail::json_columnar_dom_arm_counter().load();
    auto col_el = run_one(col_op, lines_batch(lines));
    const auto dom_runs = clink::detail::json_columnar_dom_arm_counter().load() - dom_before;

    ASSERT_TRUE(col_el.is_data());
    EXPECT_EQ(col_el.as_data().is_columnar(), columnar);
    EXPECT_EQ(dom_runs, (force_dom && columnar) ? 1U : 0U);
    EXPECT_EQ(encoded_rows(col_el.as_data()), encoded_rows(row_el.as_data()));
}
}  // namespace

TEST(JsonColumnarDecode, DoubleColumnSendsAnIntegerPastTwoToTheFiftyThreeToRows) {
    for (const bool force_dom : {false, true}) {
        SCOPED_TRACE(force_dom ? "DOM arm" : "on-demand arm");
        expect_double_integer_parity({"1.5", "9007199254740993"}, force_dom, false);
        expect_double_integer_parity({"-9007199254740993", "2"}, force_dom, false);
        expect_double_integer_parity({"9223372036854775807"}, force_dom, false);
    }
}

TEST(JsonColumnarDecode, DoubleColumnKeepsAnIntegerUpToTwoToTheFiftyThreeColumnar) {
    for (const bool force_dom : {false, true}) {
        SCOPED_TRACE(force_dom ? "DOM arm" : "on-demand arm");
        expect_double_integer_parity(
            {"9007199254740992", "-9007199254740992", "5", "-0", "0", "1.5", "1e300"},
            force_dom,
            true);
    }
}

// A BIGINT or INTEGER column fed a numeral with a decimal point or an exponent
// but an integral value, such as 5.0, 5e0 or -0.0. Both arms take an integer
// token only and send the rest to the row decode, which makes such a numeral the
// integer it names (coerce_row_integers), so the kinds compared below agree
// either way; a fraction stays a double there, as before. An integer
// token outside 64 bits fails the row decode's parse of the whole line, so it
// has no faithful cell either; -9223372036854775809 reads through a double as
// exactly -2^63, which a double fallback took for INT64_MIN. `force_dom` adds a
// duplicate key, which the on-demand arm declines, so the DOM arm decides
// instead; the counter says which arm did.
namespace {

// Each cell with its JSON kind: `i:` an integer, `d:` a double. encoded_rows
// and exact_cells print 5 and 5.0 alike, so they cannot see a carrier that
// changed one into the other.
std::vector<std::string> kinded_cells(const Batch<Row>& b) {
    std::vector<std::string> out;
    for (const auto& rec : b) {
        std::string line;
        for (const auto& [k, v] : rec.value().values) {
            const char* kind = v.is_integral_number() ? "i:" : (v.is_number() ? "d:" : "");
            line += k.str() + "=" + kind + v.serialize(0) + ";";
        }
        out.push_back(line);
    }
    return out;
}

void expect_kinded_parity(const std::shared_ptr<arrow::DataType>& type,
                          const std::vector<std::string>& values,
                          bool columnar) {
    SCOPED_TRACE(type->ToString() + " " + values.back());
    const std::vector<RowColumn> schema = {{"a", arrow::int64()}, {"v", type}};
    for (const bool force_dom : {false, true}) {
        SCOPED_TRACE(force_dom ? "DOM arm" : "on-demand arm");
        std::vector<std::string> lines;
        lines.reserve(values.size());
        for (const auto& v : values) {
            lines.push_back((force_dom ? R"({"a":1,"a":1,"v":)" : R"({"a":1,"v":)") + v + "}");
        }
        auto oracle = make_typed_row_oracle(schema);
        auto row_el = run_one(oracle, lines_batch(lines));
        JsonStringToRowColumnarOperator col_op(schema);
        const auto dom_before = clink::detail::json_columnar_dom_arm_counter().load();
        auto col_el = run_one(col_op, lines_batch(lines));
        const auto dom_runs = clink::detail::json_columnar_dom_arm_counter().load() - dom_before;

        ASSERT_TRUE(col_el.is_data());
        EXPECT_EQ(col_el.as_data().is_columnar(), columnar);
        EXPECT_EQ(dom_runs, (force_dom && columnar) ? 1U : 0U);
        EXPECT_EQ(kinded_cells(col_el.as_data()), kinded_cells(row_el.as_data()));
    }
}

}  // namespace

TEST(JsonColumnarDecode, IntegerColumnsTakeOnlyIntegerTokens) {
    for (const auto& type : {arrow::int64(), arrow::int32()}) {
        // Integer tokens go columnar, zero written as -0 and a null included.
        expect_kinded_parity(type, {"5"}, true);
        expect_kinded_parity(type, {"-1", "0", "-0", "123456"}, true);
        expect_kinded_parity(type, {"7", "null"}, true);
        // Integral numerals, in every spelling, send the batch to the row decode.
        expect_kinded_parity(type, {"5.0"}, false);
        expect_kinded_parity(type, {"7", "5e0"}, false);
        expect_kinded_parity(type, {"1E3"}, false);
        expect_kinded_parity(type, {"-0.0"}, false);
        expect_kinded_parity(type, {"7", "1.1e1"}, false);
        // So do a fraction, text and a boolean, as before.
        expect_kinded_parity(type, {"5.5"}, false);
        expect_kinded_parity(type, {R"("5")"}, false);
        expect_kinded_parity(type, {"true"}, false);
    }
    // The ends of each range stay columnar; one past them does not.
    expect_kinded_parity(
        arrow::int64(), {"9223372036854775807", "-9223372036854775808", "9007199254740993"}, true);
    expect_kinded_parity(arrow::int64(), {"9223372036854775808"}, false);
    expect_kinded_parity(arrow::int64(), {"7", "-9223372036854775809"}, false);
    expect_kinded_parity(arrow::int64(), {"18446744073709551616"}, false);
    expect_kinded_parity(arrow::int32(), {"2147483647", "-2147483648"}, true);
    expect_kinded_parity(arrow::int32(), {"2147483648"}, false);
    expect_kinded_parity(arrow::int32(), {"-2147483649"}, false);
}

// DOUBLE, REAL and DECIMAL columns fed an integer token outside 64 bits: below
// INT64_MIN, at or above 2^64, or more digits than either. The row decode's DOM
// parse refuses the whole line for one, while a double read takes any digit
// count and the decimal parser takes the raw token, so neither arm may hand the
// record on columnar. An unsigned token below 2^64 parses on both and stays
// columnar. A DOUBLE column's integer token is a double on both carriers, which
// the kinded comparison sees where a printed one would not.
TEST(JsonColumnarDecode, FloatingAndDecimalColumnsRefuseIntegerTokensPastSixtyFourBits) {
    for (const auto& type : {arrow::float64(), arrow::float32(), arrow::decimal128(38, 0)}) {
        expect_kinded_parity(type, {"7", "-9223372036854775809"}, false);
        expect_kinded_parity(type, {"7", "18446744073709551616"}, false);
        expect_kinded_parity(type, {"7", "100000000000000000000"}, false);
        expect_kinded_parity(type, {"7", "-100000000000000000000"}, false);
        expect_kinded_parity(type, {"7", "18446744073709551615"}, true);
        expect_kinded_parity(type, {"7", "-3", "0", "1.5", "null"}, true);
    }
}

// Reordered fields between two columns of the SAME TYPE. This is the case that can
// detect a wrong-column error, and until it existed the suite could not: the decoder
// caches which column each field POSITION matched last time as a first guess, and if
// that guess were ever trusted without verifying the name, a record that reorders its
// fields would put each value under the other column. Every other field-order test in
// this file uses columns of DIFFERENT types, so a mis-filed value fails the type check,
// bails the batch and lands on the row fallback with the right answer - the bug is
// invisible. With both columns int64 there is no type check to save it.
//
// Verified to fail if the guess is trusted unverified.
TEST(JsonColumnarDecode, ReorderedSameTypedFieldsLandInTheRightColumns) {
    const std::vector<RowColumn> schema = {{"x", arrow::int64()}, {"y", arrow::int64()}};
    const std::vector<std::string> lines = {
        R"({"x":10,"y":20})",  // establishes the position -> column guess
        R"({"y":21,"x":11})",  // swapped: an unverified guess files both wrongly
        R"({"x":12,"y":22})",  // and back again
    };

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));
    ASSERT_TRUE(row_el.is_data());

    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));
    ASSERT_TRUE(col_el.is_data());

    EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()))
        << "a reordered field must decode into its own column, not the one that "
           "position held on the previous record";
}

// A declared column name containing a BACKSLASH is the one case where a raw-key
// comparison could match while the key's unescaped form differs - which would file a
// field under the wrong column. Such a schema is refused for the columnar carrier up
// front, so it always takes the row decode.
TEST(JsonColumnarDecode, BackslashInColumnNameForcesRowPath) {
    const std::vector<RowColumn> schema = {{R"(a\u0062)", arrow::int64()}, {"c", arrow::int64()}};
    JsonStringToRowColumnarOperator col_op(schema);
    Batch<std::string> in;
    in.emplace(std::string(R"({"a\u0062":1,"c":2})"));
    auto el = run_one(col_op, std::move(in));
    ASSERT_TRUE(el.is_data());
    EXPECT_FALSE(el.as_data().is_columnar())
        << "a backslash-bearing column name must never take the columnar carrier";
}

// Field order is not schema order. The walk dispatches per field to a resolved
// column index, so a document whose keys arrive in a different order (or vary
// between records) must still land in the right columns.
TEST(JsonColumnarDecode, FieldOrderIndependence) {
    const std::vector<RowColumn> schema = {
        {"a", arrow::int64()}, {"b", arrow::utf8()}, {"c", arrow::float64()}};
    const std::vector<std::string> lines = {
        R"({"a":1,"b":"x","c":1.5})",
        R"({"c":2.5,"a":2,"b":"y"})",
        R"({"b":"z","c":3.5,"a":3})",
    };

    auto oracle = make_typed_row_oracle(schema);
    auto row_el = run_one(oracle, lines_batch(lines));
    JsonStringToRowColumnarOperator col_op(schema);
    auto col_el = run_one(col_op, lines_batch(lines));

    ASSERT_TRUE(col_el.as_data().is_columnar()) << "reordered keys must still go columnar";
    EXPECT_EQ(exact_cells(col_el.as_data()), exact_cells(row_el.as_data()));
}

// ---- lines the row decode refuses ----------------------------------------------------
//
// The row decode parses each line with a validating DOM parse, and a line that parse
// refuses decodes to an empty row, on json_string_to_row and on this operator's own row
// fallback alike. The columnar arms must hand on that same empty row, so a line the DOM
// parse refuses must never ride columnar: whichever column holds the offending value,
// and whether or not the query reads that column. The on-demand arm reads only the
// columns it builds, so a value in a declared column outside the keep-list is the case
// these tests exist for: the arm still has to reach the DOM parse's verdict on it.
namespace {

// One declared column of each type a columnar-capable schema can hold, plus `id`, which
// every keep-list below keeps and which never holds an offending value.
std::vector<RowColumn> refusal_schema() {
    return {
        {"id", arrow::int64()},
        {"n", arrow::int64()},
        {"i", arrow::int32()},
        {"x", arrow::float64()},
        {"r", arrow::float32()},
        {"m", arrow::decimal128(10, 2)},
        {"s", arrow::utf8()},
        {"f", arrow::boolean()},
        {"t", arrow::timestamp(arrow::TimeUnit::MILLI)},
    };
}

// The columns an offending value is placed in: every declared one but `id`, and "zz",
// which the schema does not declare.
const std::vector<std::string>& refusal_columns() {
    static const std::vector<std::string> cols = {"n", "i", "x", "r", "m", "s", "f", "t", "zz"};
    return cols;
}

// A well-formed line for refusal_schema() with id `id`, except that column `col`, when
// given, holds the raw JSON text `raw`. For "zz" the line gains that undeclared key.
std::string refusal_line(int id, const std::string& col = {}, const std::string& raw = {}) {
    const std::vector<std::pair<std::string, std::string>> fields = {
        {"id", std::to_string(id)},
        {"n", "10"},
        {"i", "-3"},
        {"x", "1.5"},
        {"r", "0.25"},
        {"m", "12.34"},
        {"s", R"("txt")"},
        {"f", "true"},
        {"t", "1700000000000"},
    };
    std::string out = "{";
    for (const auto& [name, value] : fields) {
        if (out.size() > 1) {
            out += ',';
        }
        out += '"' + name + "\":" + (name == col ? raw : value);
    }
    if (col == "zz") {
        out += R"(,"zz":)" + raw;
    }
    return out + "}";
}

// The keep-lists a column is tried under: outside the projection, inside it, and with no
// projection at all. An undeclared key has no "inside".
std::vector<std::pair<std::string, std::vector<std::string>>> keep_lists(const std::string& col) {
    std::vector<std::pair<std::string, std::vector<std::string>>> out = {
        {"unprojected", {"id"}},
        {"no projection", {}},
    };
    if (col != "zz") {
        out.emplace_back("projected", std::vector<std::string>{"id", col});
    }
    return out;
}

// The typed row decode under the same keep-list: this operator's own row fallback, and
// what json_string_to_row hands on once the query has dropped the columns it does not
// read.
MapOperator<std::string, Row> make_projected_row_oracle(const std::vector<RowColumn>& schema,
                                                        const std::vector<std::string>& keep) {
    auto fmt = std::make_shared<clink::TextFormat<Row>>(
        clink::sql::row_json_text_format_for_columns_projected(schema, keep));
    return MapOperator<std::string, Row>(
        [fmt](const std::string& line) -> Row { return fmt->decode(line).value_or(Row{}); },
        "json_string_to_row");
}

// Raw bytes made readable in a failure message, and a long value shortened.
std::string printable(const std::string& raw) {
    std::string out;
    for (const char c : raw.size() > 40 ? raw.substr(0, 12) : raw) {
        const auto u = static_cast<unsigned char>(c);
        if (u < 0x20 || u >= 0x7f) {
            char buf[8];
            std::snprintf(buf, sizeof(buf), "\\x%02x", u);
            out += buf;
        } else {
            out += c;
        }
    }
    if (raw.size() > 40) {
        out += "...(" + std::to_string(raw.size()) + " bytes)";
    }
    return out;
}

std::string joined(const std::vector<std::string>& cells) {
    std::string out = "(" + std::to_string(cells.size()) + ")";
    for (const auto& c : cells) {
        out += " [" + c + "]";
    }
    return out;
}

// How one batch decoded on the columnar operator, against the row decode under the same
// keep-list. `divergence` is empty when both handed on the same records with the same
// cells and kinds, and otherwise says how they differ, so a test can list every
// diverging case rather than stop at the first. A throw out of process() is a divergence.
struct Decoded {
    std::string divergence;
    bool columnar{false};
    std::uint64_t dom_runs{0};
};

Decoded decode_against_rows(const std::vector<std::string>& lines,
                            const std::vector<std::string>& keep) {
    const auto schema = refusal_schema();
    auto oracle = make_projected_row_oracle(schema, keep);
    const auto want = kinded_cells(run_one(oracle, lines_batch(lines)).as_data());
    JsonStringToRowColumnarOperator col_op(schema, keep);
    Decoded out;
    const auto dom_before = clink::detail::json_columnar_dom_arm_counter().load();
    try {
        auto el = run_one(col_op, lines_batch(lines));
        out.columnar = el.as_data().is_columnar();
        const auto got = kinded_cells(el.as_data());
        if (got != want) {
            out.divergence = "rows " + joined(want) + " vs columnar " + joined(got);
        }
    } catch (const std::exception& e) {
        out.divergence = std::string("the columnar decode threw: ") + e.what();
    }
    out.dom_runs = clink::detail::json_columnar_dom_arm_counter().load() - dom_before;
    return out;
}

// Values the DOM parse refuses wherever they stand, so a line holding one decodes to an
// empty row.
std::vector<std::string> parse_refused_values() {
    std::vector<std::string> v = {
        // Integers outside 64 bits.
        "123456789012345678901234567890",
        "-9223372036854775809",
        "18446744073709551616",
        // Numerals the JSON grammar does not have.
        "01",
        "-01",
        "1.",
        "-",
        ".5",
        "+1",
        "1e",
        "1e+",
        "1.2.3",
        "0x1F",
        "1ee5",
        "--1",
        "1-2",
        "Infinity",
        "NaN",
        // Numbers past the largest double.
        "1e400",
        "-1e400",
        "1e309",
        "2e308",
        // Atoms other than true, false and null.
        "tru",
        "nul",
        "fals",
        "True",
        "truex",
        "null1",
        "nan",
        "undefined",
        // Escapes the parse refuses.
        R"("\q")",
        R"("\x41")",
        R"("\u12G4")",
        R"("\u12")",
        R"("\ud800")",
        R"("\udc00")",
        R"("\ud800\u0041")",
        // The same past the first eight bytes of the token, and at the end of one.
        R"("abcdefg\q")",
        R"("abcdefghijklmnopqrstuvw\x")",
        R"("abcdefghijklmnop\u12")",
        // Invalid UTF-8 and unescaped control characters.
        "\"\xff\"",
        "\"\xc0\xaf\"",
        "\"\xe2\x82\"",
        "\"\xed\xa0\x80\"",
        "\"a\tb\"",
        "\"a\x01"
        "b\"",
        // A missing or truncated value.
        "",
        "[1,2",
        R"({"a":1)",
        // Nested values the parse refuses.
        "[1,,2]",
        "[1 2]",
        "[1,]",
        "[,1]",
        R"({"a" 1})",
        R"({"a":1,})",
        R"({"a":})",
        "{1:2}",
        "[1,2}",
        R"({"a":1])",
        R"([1,"\q"])",
        R"({"a":01})",
        R"({"\q":1})",
        "[tru]",
        "[1e400]",
        // A string followed by a colon, where only a comma or a closing bracket may
        // follow a value. An unread string is skipped by the iterator, whose skip
        // takes the colon as a key's and runs on to the next closing bracket.
        R"("txt":0])",
        R"(["x":0],1])",
        R"({"a":"x":0],"b":1})",
    };
    // Nesting past the parse's depth limit.
    v.push_back(std::string(1100, '[') + std::string(1100, ']'));
    return v;
}

}  // namespace

// A value the DOM parse refuses, in each declared column and in an undeclared key, read
// by the query or not. The columnar decode must hand on what the row decode does, an
// empty row between two whole ones, and must not carry that batch columnar.
TEST(JsonColumnarDecode, ALineTheRowDecodeRefusesNeverRidesColumnar) {
    const auto row_decode = clink::sql::row_json_text_format_for_columns(refusal_schema());
    std::vector<std::string> diverged;
    std::size_t cases = 0;
    for (const auto& raw : parse_refused_values()) {
        for (const auto& col : refusal_columns()) {
            const std::string bad = refusal_line(2, col, raw);
            ASSERT_FALSE(row_decode.decode(bad).has_value())
                << "the row decode takes " << printable(bad) << ", so the case tests nothing";
            for (const auto& [mode, keep] : keep_lists(col)) {
                ++cases;
                const auto d = decode_against_rows({refusal_line(1), bad, refusal_line(3)}, keep);
                if (!d.divergence.empty() || d.columnar) {
                    diverged.push_back(mode + " " + col + "=" + printable(raw) + ": " +
                                       (d.divergence.empty() ? "rode columnar" : d.divergence));
                }
            }
        }
    }
    std::string list;
    for (const auto& d : diverged) {
        list += "  " + d + "\n";
    }
    EXPECT_TRUE(diverged.empty()) << diverged.size() << " of " << cases
                                  << " cases diverged from the row decode:\n"
                                  << list;
}

// Content after the closing brace makes the DOM parse refuse the line, whatever the
// object holds. The on-demand walk stops at the brace, so it has to look for it.
TEST(JsonColumnarDecode, ContentAfterTheObjectSendsTheLineToRows) {
    const auto row_decode = clink::sql::row_json_text_format_for_columns(refusal_schema());
    for (const std::string tail : {" {}", "}", ",{}", R"( {"id":9})", "{}", " []", " 1", " x"}) {
        SCOPED_TRACE("tail '" + tail + "'");
        const std::string bad = refusal_line(2) + tail;
        ASSERT_FALSE(row_decode.decode(bad).has_value());
        for (const auto& [mode, keep] : keep_lists("zz")) {
            SCOPED_TRACE(mode);
            const auto d = decode_against_rows({refusal_line(1), bad, refusal_line(3)}, keep);
            EXPECT_EQ(d.divergence, "");
            EXPECT_FALSE(d.columnar);
        }
    }
    // Whitespace around the object and a byte order mark are not content: both
    // decodes take the line, and the on-demand arm keeps it.
    for (const std::string& line :
         {refusal_line(2) + " \t ", "  " + refusal_line(2), "\xEF\xBB\xBF" + refusal_line(2)}) {
        SCOPED_TRACE(printable(line));
        ASSERT_TRUE(row_decode.decode(line).has_value());
        for (const auto& [mode, keep] : keep_lists("zz")) {
            SCOPED_TRACE(mode);
            const auto d = decode_against_rows({refusal_line(1), line, refusal_line(3)}, keep);
            EXPECT_EQ(d.divergence, "");
            EXPECT_TRUE(d.columnar);
            EXPECT_EQ(d.dom_runs, 0U);
        }
    }
}

// A REAL column fed -0. The row decode reads the integer token 0 and rounds it to float
// precision, +0.0; the on-demand arm read the token as a double, -0.0, which prints the
// same, so only the sign bit tells them apart. Both arms must hold +0.0.
TEST(JsonColumnarDecode, ARealColumnFedMinusZeroHoldsPositiveZeroOnEitherArm) {
    const std::vector<RowColumn> schema = {{"a", arrow::int64()}, {"r", arrow::float32()}};
    const auto r_of = [](const Batch<Row>& b) {
        std::vector<double> out;
        for (const auto& rec : b) {
            out.push_back(rec.value().values.find("r")->second.as_number());
        }
        return out;
    };
    for (const bool force_dom : {false, true}) {
        SCOPED_TRACE(force_dom ? "DOM arm" : "on-demand arm");
        const std::string line = force_dom ? R"({"a":1,"a":1,"r":-0})" : R"({"a":1,"r":-0})";
        auto oracle = make_typed_row_oracle(schema);
        auto row_el = run_one(oracle, lines_batch({line}));
        JsonStringToRowColumnarOperator col_op(schema);
        auto col_el = run_one(col_op, lines_batch({line}));
        ASSERT_TRUE(row_el.is_data());
        ASSERT_TRUE(col_el.is_data());
        EXPECT_TRUE(col_el.as_data().is_columnar());
        const auto row_r = r_of(row_el.as_data());
        const auto col_r = r_of(col_el.as_data());
        ASSERT_EQ(row_r.size(), 1U);
        ASSERT_EQ(col_r.size(), 1U);
        EXPECT_FALSE(std::signbit(row_r[0]));
        EXPECT_FALSE(std::signbit(col_r[0]));
    }
}

// The other side of the same contract, and the speed the projection exists for: a value
// the DOM parse accepts, in a column outside the keep-list, keeps the batch on the
// on-demand arm whatever its type, and its record matches the row decode. Inside the
// keep-list the carrier depends on the value's type, so only the records are compared.
TEST(JsonColumnarDecode, WellFormedValuesOutsideTheProjectionStayOnTheOnDemandArm) {
    const auto row_decode = clink::sql::row_json_text_format_for_columns(refusal_schema());
    const std::vector<std::string> values = {
        "0",
        "-0",
        "7",
        "-0.0",
        "1.5e-3",
        "1E+2",
        "1e308",
        "-1e308",
        "1e-400",
        "0e999999",
        "1e-99999999999999999999",
        "123456789012345678",
        "1234567890123456789",
        "-1234567890123456789",
        "12345678901234567890",
        "18446744073709551615",
        "9223372036854775807",
        "-9223372036854775808",
        "3.14159265358979323846264338327950288419716939937510",
        "true",
        "false",
        "null",
        R"("plain")",
        R"("")",
        R"("\ud83d\ude00")",
        R"("\n\t\\\"\/\b\f\r")",
        R"("\u00e9")",
        R"("abcdefg\n")",
        R"("abcdefghijklmnopqrstuvw\u00e9xyz")",
        R"("the quick brown fox jumps over the lazy dog")",
        "\"\xc3\xa9\xe4\xb8\xad\"",
        "[]",
        "{}",
        "[1,2]",
        R"([1,"\u0041",[true,null],{"k":-2.5e3}])",
        R"({"a":[1,{"b":null}],"\u0063":"d"})",
        std::string(20, '[') + std::string(20, ']'),
    };
    std::vector<std::string> diverged;
    for (const auto& raw : values) {
        for (const auto& col : refusal_columns()) {
            if (col == "zz") {
                continue;  // an undeclared key always sends the batch to the DOM arm
            }
            const std::string line = refusal_line(2, col, raw);
            ASSERT_TRUE(row_decode.decode(line).has_value()) << printable(line);
            for (const auto& [mode, keep] : keep_lists(col)) {
                const auto d = decode_against_rows({refusal_line(1), line, refusal_line(3)}, keep);
                const bool unprojected = mode == "unprojected";
                if (!d.divergence.empty() || (unprojected && (!d.columnar || d.dom_runs != 0))) {
                    diverged.push_back(
                        mode + " " + col + "=" + printable(raw) + ": " +
                        (d.divergence.empty()
                             ? (d.columnar ? "left the on-demand arm" : "fell to rows")
                             : d.divergence));
                }
            }
        }
    }
    std::string list;
    for (const auto& d : diverged) {
        list += "  " + d + "\n";
    }
    EXPECT_TRUE(diverged.empty()) << diverged.size() << " cases:\n" << list;
}
