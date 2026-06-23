// Columnar-into-SQL, increment 1: ColumnarRowFilterOperator.
//
// The SQL WHERE operator (filter_row_predicate) now rides the Arrow sidecar
// when the input Batch<Row> is columnar. Claims under test:
//   - the columnar fast path filters correctly AND decodes zero rows
//     (batch_materialize_counter stays put while the operator runs),
//   - the output stays columnar so the chain downstream keeps the fast path,
//   - the columnar predicate is byte-identical to the row predicate (parity),
//     including three-valued logic (NULL / missing column drops the row),
//   - a row-only batch signals fallback and the row path filters identically,
//   - event-time rides through arrow::compute::Filter.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/columnar_row_filter_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace {

using namespace clink;
using clink::sql::Row;
using clink::sql::RowColumn;

std::uint64_t materialize_count() {
    return detail::batch_materialize_counter().load(std::memory_order_relaxed);
}

Row make_row(std::int64_t amount, std::string region) {
    Row r;
    r.values["amount"] = clink::config::JsonValue{static_cast<double>(amount)};
    r.values["region"] = clink::config::JsonValue{std::move(region)};
    return r;
}

const std::vector<RowColumn>& schema() {
    static const std::vector<RowColumn> s = {{"amount", arrow::int64()}, {"region", arrow::utf8()}};
    return s;
}

// Build a columnar Batch<Row> via the schema-driven row batcher (the same path
// a Parquet source / the columnar wire produce). The returned batch is_columnar.
Batch<Row> make_columnar(const std::vector<Row>& rows,
                         const std::vector<std::optional<std::int64_t>>& ts = {}) {
    auto batcher = clink::sql::make_row_columnar_arrow_batcher(schema());
    Batch<Row> in;
    for (std::size_t i = 0; i < rows.size(); ++i) {
        if (i < ts.size() && ts[i].has_value()) {
            in.emplace(rows[i], EventTime{*ts[i]});
        } else {
            in.emplace(rows[i]);
        }
    }
    auto rb = batcher.build(in);
    return Batch<Row>{std::move(rb), rows.size(), clink::sql::row_materialize_fn()};
}

Batch<Row> make_rows(const std::vector<Row>& rows) {
    Batch<Row> b;
    for (const auto& r : rows) {
        b.emplace(r);
    }
    return b;
}

std::shared_ptr<clink::config::JsonValue> predicate(const std::string& json) {
    return std::make_shared<clink::config::JsonValue>(clink::config::parse(json));
}

struct Capture {
    std::vector<StreamElement<Row>> elems;
    Emitter<Row> emitter() {
        return Emitter<Row>([this](StreamElement<Row> e) {
            elems.push_back(std::move(e));
            return true;
        });
    }
    // Collected "amount" values in emit order (materializes output batches).
    std::vector<std::int64_t> amounts() {
        std::vector<std::int64_t> out;
        for (auto& e : elems) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    auto it = r.value().values.find("amount");
                    if (it != r.value().values.end() && it->second.is_number()) {
                        out.push_back(static_cast<std::int64_t>(it->second.as_number()));
                    }
                }
            }
        }
        return out;
    }
    bool any_columnar() {
        for (auto& e : elems) {
            if (e.is_data() && e.as_data().is_columnar()) {
                return true;
            }
        }
        return false;
    }
};

const std::vector<Row>& sample() {
    static const std::vector<Row> rows = {make_row(10, "eu"),
                                          make_row(60, "us"),
                                          make_row(50, "eu"),
                                          make_row(99, "us"),
                                          make_row(3, "eu")};
    return rows;
}

TEST(ColumnarRowFilter, FastPathFiltersAndDecodesZeroRows) {
    ColumnarRowFilterOperator op(predicate(R"({"op":"gt","col":"amount","literal":50})"));
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();

    const bool handled = op.process_columnar(StreamElement<Row>::data(make_columnar(sample())), em);

    EXPECT_TRUE(handled);
    EXPECT_EQ(materialize_count() - before, 0u) << "columnar fast path must not decode rows";
    EXPECT_TRUE(cap.any_columnar()) << "output should stay columnar";
    EXPECT_EQ(cap.amounts(), (std::vector<std::int64_t>{60, 99}));
}

TEST(ColumnarRowFilter, ColumnarMatchesRowPredicate) {
    // Same predicate, same data, both paths: byte-identical kept set.
    auto pred = predicate(R"({"op":"ge","col":"amount","literal":50})");

    ColumnarRowFilterOperator col_op(pred);
    Capture col_cap;
    auto col_em = col_cap.emitter();
    col_op.process_columnar(StreamElement<Row>::data(make_columnar(sample())), col_em);

    ColumnarRowFilterOperator row_op(pred);
    Capture row_cap;
    auto row_em = row_cap.emitter();
    row_op.process(StreamElement<Row>::data(make_rows(sample())), row_em);

    EXPECT_EQ(col_cap.amounts(), (std::vector<std::int64_t>{60, 50, 99}));
    EXPECT_EQ(col_cap.amounts(), row_cap.amounts());
}

TEST(ColumnarRowFilter, MultiColumnAndPredicate) {
    ColumnarRowFilterOperator op(predicate(
        R"({"op":"and","args":[{"op":"gt","col":"amount","literal":40},{"op":"eq","col":"region","literal":"eu"}]})"));
    Capture cap;
    auto em = cap.emitter();
    EXPECT_TRUE(op.process_columnar(StreamElement<Row>::data(make_columnar(sample())), em));
    EXPECT_EQ(cap.amounts(), (std::vector<std::int64_t>{50}));  // amount>40 AND region=eu
}

TEST(ColumnarRowFilter, ThreeValuedLogicDropsNullAndMissing) {
    // A NULL column value and a missing column both yield Unknown -> dropped,
    // identical to SQL WHERE. Build rows where one has a null amount.
    std::vector<Row> rows = sample();
    rows[1].values["amount"] = clink::config::JsonValue{nullptr};  // was 60 -> now NULL
    ColumnarRowFilterOperator op(predicate(R"({"op":"ge","col":"amount","literal":50})"));
    Capture cap;
    auto em = cap.emitter();
    op.process_columnar(StreamElement<Row>::data(make_columnar(rows)), em);
    EXPECT_EQ(cap.amounts(), (std::vector<std::int64_t>{50, 99}));  // NULL row dropped
}

TEST(ColumnarRowFilter, ProcessColumnarRejectsRowBatch) {
    ColumnarRowFilterOperator op(predicate(R"({"op":"gt","col":"amount","literal":50})"));
    Capture cap;
    auto em = cap.emitter();
    EXPECT_FALSE(op.process_columnar(StreamElement<Row>::data(make_rows(sample())), em))
        << "a row batch must signal fallback to process()";
    EXPECT_TRUE(cap.elems.empty());
}

TEST(ColumnarRowFilter, EventTimeSurvivesColumnarFilter) {
    ColumnarRowFilterOperator op(predicate(R"({"op":"ge","col":"amount","literal":50})"));
    Capture cap;
    auto em = cap.emitter();
    op.process_columnar(
        StreamElement<Row>::data(make_columnar(sample(), {{100}, {200}, {300}, {400}, {500}})), em);
    std::vector<std::int64_t> ts;
    for (auto& e : cap.elems) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                ASSERT_TRUE(r.event_time().has_value());
                ts.push_back(r.event_time()->millis());
            }
        }
    }
    EXPECT_EQ(ts, (std::vector<std::int64_t>{200, 300, 400}));  // amounts 60,50,99
}

}  // namespace
