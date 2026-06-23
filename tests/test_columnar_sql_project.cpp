// Columnar-into-SQL, increment 2: ColumnarRowProjectOperator.
//
// The SQL projection operator (project_row) rides the Arrow sidecar for pure
// column-reference outputs. Claims under test:
//   - column pruning + rename run columnar and decode zero rows,
//   - the output stays columnar so the chain downstream keeps the fast path,
//   - a computed / literal output forces the row fallback (process_columnar
//     returns false) and the row path computes the identical result,
//   - columnar and row projection are byte-identical for ref-only outputs,
//   - event-time rides through,
//   - a __row_kind changelog marker column is carried through unchanged.

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
#include "clink/operators/columnar_row_project_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/sql/row_kind.hpp"

namespace {

using namespace clink;
using clink::sql::Row;
using clink::sql::RowColumn;
using Output = clink::ColumnarRowProjectOperator::Output;

std::uint64_t materialize_count() {
    return detail::batch_materialize_counter().load(std::memory_order_relaxed);
}

clink::config::JsonValue col(const std::string& name) {
    return clink::config::parse(R"({"col":")" + name + R"("})");
}

Row make_row(std::int64_t amount, std::string region) {
    Row r;
    r.values["amount"] = clink::config::JsonValue{static_cast<double>(amount)};
    r.values["region"] = clink::config::JsonValue{std::move(region)};
    return r;
}

Batch<Row> make_columnar(const std::vector<Row>& rows,
                         const std::vector<RowColumn>& schema,
                         const std::vector<std::optional<std::int64_t>>& ts = {}) {
    auto batcher = clink::sql::make_row_columnar_arrow_batcher(schema);
    Batch<Row> in;
    for (std::size_t i = 0; i < rows.size(); ++i) {
        if (i < ts.size() && ts[i].has_value()) {
            in.emplace(rows[i], EventTime{*ts[i]});
        } else {
            in.emplace(rows[i]);
        }
    }
    return Batch<Row>{batcher.build(in), rows.size(), clink::sql::row_materialize_fn()};
}

const std::vector<RowColumn>& schema2() {
    static const std::vector<RowColumn> s = {{"amount", arrow::int64()}, {"region", arrow::utf8()}};
    return s;
}

const std::vector<Row>& sample() {
    static const std::vector<Row> rows = {
        make_row(10, "eu"), make_row(60, "us"), make_row(50, "eu")};
    return rows;
}

struct Capture {
    std::vector<StreamElement<Row>> elems;
    Emitter<Row> emitter() {
        return Emitter<Row>([this](StreamElement<Row> e) {
            elems.push_back(std::move(e));
            return true;
        });
    }
    bool any_columnar() {
        for (auto& e : elems) {
            if (e.is_data() && e.as_data().is_columnar()) {
                return true;
            }
        }
        return false;
    }
    // (column-name -> stringified value) for each emitted row, in order.
    std::vector<Row> rows() {
        std::vector<Row> out;
        for (auto& e : elems) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    out.push_back(r.value());
                }
            }
        }
        return out;
    }
};

TEST(ColumnarRowProject, RenameRunsColumnarZeroDecode) {
    ColumnarRowProjectOperator op({{"amt", col("amount")}, {"loc", col("region")}});
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();

    EXPECT_TRUE(
        op.process_columnar(StreamElement<Row>::data(make_columnar(sample(), schema2())), em));
    EXPECT_EQ(materialize_count() - before, 0u) << "columnar projection must not decode rows";
    EXPECT_TRUE(cap.any_columnar());

    auto rows = cap.rows();
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(static_cast<std::int64_t>(rows[1].values.at("amt").as_number()), 60);
    EXPECT_EQ(rows[1].values.at("loc").as_string(), "us");
    EXPECT_FALSE(rows[1].has_column("amount")) << "renamed away";
}

TEST(ColumnarRowProject, ColumnPruningDropsUnreferenced) {
    ColumnarRowProjectOperator op({{"amount", col("amount")}});  // SELECT amount
    Capture cap;
    auto em = cap.emitter();
    op.process_columnar(StreamElement<Row>::data(make_columnar(sample(), schema2())), em);
    auto rows = cap.rows();
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_TRUE(rows[0].has_column("amount"));
    EXPECT_FALSE(rows[0].has_column("region")) << "pruned column must be gone";
}

TEST(ColumnarRowProject, ComputedOutputFallsBackAndMatches) {
    // amt2 = amount * 2 is computed -> the columnar path must decline.
    auto outs = std::vector<Output>{
        {"amt2", clink::config::parse(R"({"op":"mul","args":[{"col":"amount"},{"lit":2}]})")}};
    {
        ColumnarRowProjectOperator op(outs);
        Capture cap;
        auto em = cap.emitter();
        EXPECT_FALSE(
            op.process_columnar(StreamElement<Row>::data(make_columnar(sample(), schema2())), em))
            << "computed projection must signal row fallback";
        EXPECT_TRUE(cap.elems.empty());
    }
    {
        ColumnarRowProjectOperator op(outs);
        Capture cap;
        auto em = cap.emitter();
        Batch<Row> b;
        for (const auto& r : sample()) {
            b.emplace(r);
        }
        op.process(StreamElement<Row>::data(std::move(b)), em);
        auto rows = cap.rows();
        ASSERT_EQ(rows.size(), 3u);
        EXPECT_EQ(static_cast<std::int64_t>(rows[1].values.at("amt2").as_number()), 120);  // 60*2
    }
}

TEST(ColumnarRowProject, ColumnarMatchesRowForRefs) {
    std::vector<Output> outs{{"amt", col("amount")}, {"loc", col("region")}};

    ColumnarRowProjectOperator col_op(outs);
    Capture col_cap;
    auto col_em = col_cap.emitter();
    col_op.process_columnar(StreamElement<Row>::data(make_columnar(sample(), schema2())), col_em);

    ColumnarRowProjectOperator row_op(outs);
    Capture row_cap;
    auto row_em = row_cap.emitter();
    Batch<Row> b;
    for (const auto& r : sample()) {
        b.emplace(r);
    }
    row_op.process(StreamElement<Row>::data(std::move(b)), row_em);

    auto a = col_cap.rows();
    auto c = row_cap.rows();
    ASSERT_EQ(a.size(), c.size());
    for (std::size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].values.at("amt").as_number(), c[i].values.at("amt").as_number());
        EXPECT_EQ(a[i].values.at("loc").as_string(), c[i].values.at("loc").as_string());
    }
}

TEST(ColumnarRowProject, EventTimeSurvives) {
    ColumnarRowProjectOperator op({{"amt", col("amount")}});
    Capture cap;
    auto em = cap.emitter();
    op.process_columnar(
        StreamElement<Row>::data(make_columnar(sample(), schema2(), {{100}, {200}, {300}})), em);
    std::vector<std::int64_t> ts;
    for (auto& e : cap.elems) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                ASSERT_TRUE(r.event_time().has_value());
                ts.push_back(r.event_time()->millis());
            }
        }
    }
    EXPECT_EQ(ts, (std::vector<std::int64_t>{100, 200, 300}));
}

TEST(ColumnarRowProject, RowKindMarkerCarriesThrough) {
    // A declared __row_kind column rides through projection unchanged so a
    // changelog stream keeps its insert/delete semantics on the columnar path.
    const std::vector<RowColumn> schema = {{"amount", arrow::int64()},
                                           {std::string{sql::kRowKindField}, arrow::utf8()}};
    std::vector<Row> rows;
    Row r0 = make_row(10, "eu");
    r0.values.erase("region");
    sql::set_row_kind(r0, sql::kRowKindDelete);
    rows.push_back(r0);

    ColumnarRowProjectOperator op({{"amount", col("amount")}});
    Capture cap;
    auto em = cap.emitter();
    EXPECT_TRUE(op.process_columnar(StreamElement<Row>::data(make_columnar(rows, schema)), em));
    auto out = cap.rows();
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(sql::row_kind_of(out[0]), std::string{sql::kRowKindDelete})
        << "__row_kind must survive the columnar projection";
}

}  // namespace
