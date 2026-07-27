// The keyed shuffle's columnar split must route every row to the same place, in the same
// order, with the same content as a row-by-row split would.
//
// This is a correctness-critical function with an unusually quiet failure mode: a
// misrouted record does not crash or throw, it lands in the wrong subtask's keyed state
// and corrupts an aggregate that nobody notices until the output is wrong. It had no
// direct tests. These compare it against a reference split computed by hand from the same
// inputs, so any divergence in routing, ordering or content fails here rather than in
// somebody's results.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/runtime/columnar_split.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace {

using clink::Batch;
using clink::sql::Row;

// Same shape the SQL Row channel carries through a shuffle: event time at column 0, then
// declared columns.
std::shared_ptr<arrow::RecordBatch> make_rb(std::int64_t rows) {
    arrow::Int64Builder ts, key;
    arrow::StringBuilder name;
    for (std::int64_t i = 0; i < rows; ++i) {
        EXPECT_TRUE(ts.Append(1'700'000'000'000 + i).ok());
        EXPECT_TRUE(key.Append(i * 7).ok());
        EXPECT_TRUE(name.Append("n" + std::to_string(i)).ok());
    }
    std::shared_ptr<arrow::Array> a_ts, a_key, a_name;
    EXPECT_TRUE(ts.Finish(&a_ts).ok());
    EXPECT_TRUE(key.Finish(&a_key).ok());
    EXPECT_TRUE(name.Finish(&a_name).ok());
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::int64(), true),
                                                   arrow::field("k", arrow::int64(), true),
                                                   arrow::field("name", arrow::utf8(), true)}),
                                    rows,
                                    {a_ts, a_key, a_name});
}

Batch<Row> make_batch(std::int64_t rows) {
    return Batch<Row>{
        make_rb(rows), static_cast<std::size_t>(rows), clink::sql::row_materialize_fn()};
}

// A row rendered for comparison. Serialising the whole object catches a wrong value in
// any column, not just the key.
std::string render(const Row& r) {
    std::string out;
    for (const auto& [k, v] : r.values) {
        out += k + "=" + v.serialize(0) + ";";
    }
    return out;
}

std::vector<std::string> rows_of(const Batch<Row>& b) {
    std::vector<std::string> out;
    for (const auto& rec : b) {
        out.push_back(render(rec.value()));
    }
    return out;
}

}  // namespace

// The core contract, against a reference split of the same inputs.
TEST(ColumnarSplit, MatchesARowByRowSplitExactly) {
    constexpr std::int64_t kRows = 500;
    constexpr std::size_t kTargets = 4;
    auto batch = make_batch(kRows);

    // A deliberately uneven, non-round-robin assignment: round-robin would hide an
    // off-by-one that a real key hash would expose.
    std::vector<int> targets(kRows);
    for (std::int64_t i = 0; i < kRows; ++i) {
        const auto slot = static_cast<std::uint64_t>((i * i) + (3 * i)) % kTargets;
        targets[static_cast<std::size_t>(i)] = static_cast<int>(slot);
    }

    // Reference: materialise the parent's rows, then bucket them by target in row order.
    const auto all = rows_of(batch);
    ASSERT_EQ(all.size(), static_cast<std::size_t>(kRows));
    std::vector<std::vector<std::string>> expected(kTargets);
    for (std::int64_t i = 0; i < kRows; ++i) {
        expected[static_cast<std::size_t>(targets[static_cast<std::size_t>(i)])].push_back(
            all[static_cast<std::size_t>(i)]);
    }

    auto parts = clink::gather_columnar_by_target<Row>(batch, targets, kTargets);
    ASSERT_TRUE(parts.has_value());
    ASSERT_EQ(parts->size(), kTargets);

    std::size_t seen = 0;
    for (std::size_t t = 0; t < kTargets; ++t) {
        const auto got = rows_of((*parts)[t]);
        EXPECT_EQ(got, expected[t]) << "target " << t
                                    << " received different rows, or the "
                                       "same rows in a different order";
        seen += got.size();
    }
    EXPECT_EQ(seen, static_cast<std::size_t>(kRows)) << "rows were dropped or duplicated";
}

// Skew is the normal case for a real key distribution, not an edge case.
TEST(ColumnarSplit, AllRowsToOneTargetLeavesTheOthersEmpty) {
    constexpr std::int64_t kRows = 64;
    auto batch = make_batch(kRows);
    const std::vector<int> targets(kRows, 2);

    auto parts = clink::gather_columnar_by_target<Row>(batch, targets, 4);
    ASSERT_TRUE(parts.has_value());
    EXPECT_EQ((*parts)[0].size(), 0u);
    EXPECT_EQ((*parts)[1].size(), 0u);
    EXPECT_EQ((*parts)[2].size(), static_cast<std::size_t>(kRows));
    EXPECT_EQ((*parts)[3].size(), 0u);
    EXPECT_EQ(rows_of((*parts)[2]), rows_of(batch));
}

// A target outside the range drops its row, which is what the row-form add_split does.
// Asserted rather than assumed, because the index path and the previous mask path reach
// that behaviour by different routes.
TEST(ColumnarSplit, OutOfRangeTargetDropsTheRow) {
    constexpr std::int64_t kRows = 6;
    auto batch = make_batch(kRows);
    std::vector<int> targets = {0, 1, 99, 0, -1, 1};

    auto parts = clink::gather_columnar_by_target<Row>(batch, targets, 2);
    ASSERT_TRUE(parts.has_value());
    // Rows 0 and 3 to target 0; rows 1 and 5 to target 1; rows 2 and 4 dropped.
    EXPECT_EQ((*parts)[0].size(), 2u);
    EXPECT_EQ((*parts)[1].size(), 2u);

    const auto all = rows_of(batch);
    EXPECT_EQ(rows_of((*parts)[0]), (std::vector<std::string>{all[0], all[3]}));
    EXPECT_EQ(rows_of((*parts)[1]), (std::vector<std::string>{all[1], all[5]}));
}

// The split must refuse rather than guess when its inputs disagree, so the caller can
// fall back to the row split.
TEST(ColumnarSplit, RefusesWhenTargetCountDoesNotMatchRowCount) {
    auto batch = make_batch(10);
    const std::vector<int> targets(9, 0);
    EXPECT_FALSE(clink::gather_columnar_by_target<Row>(batch, targets, 2).has_value());
}

// A row-only batch has no sidecar to gather, and must decline instead of producing empty
// output that would silently swallow the batch.
TEST(ColumnarSplit, RefusesARowOnlyBatch) {
    Batch<Row> rows_only;
    Row r;
    r.values.emplace("k", clink::config::JsonValue{std::int64_t{1}});
    rows_only.emplace(std::move(r));
    const std::vector<int> targets(1, 0);
    EXPECT_FALSE(clink::gather_columnar_by_target<Row>(rows_only, targets, 2).has_value());
}

// ---- declared schema vs emitted batch ---------------------------------------------
//
// A materialise closure built from a table's DECLARED schema must resolve columns by NAME,
// never by declared position. The batch it is handed may legitimately carry a SUBSET (a
// projected read) or a different order, and a positional mapping then either pairs a column
// with the wrong name - silent corruption - or indexes past the end of the batch, which
// segfaults inside Arrow's lazy column boxing. That crash is how the bug was found; the
// corruption variant would not have announced itself at all.

// The batcher's parse() guards on column COUNT, so a narrowed batch is refused outright
// rather than mis-read. Asserted because that refusal is the only thing standing between a
// short batch and a positional read here - the closure itself no longer depends on it, but
// if the guard is ever loosened, this records what it was doing.
TEST(ColumnarSchemaBinding, NarrowedBatchIsRefusedByParseNotMisread) {
    const std::vector<clink::sql::RowColumn> declared = {
        {"a", arrow::int64()}, {"b", arrow::int64()}, {"c", arrow::int64()}};
    auto batcher = clink::sql::make_row_columnar_arrow_batcher(declared);

    arrow::Int64Builder ts_b, c_b;
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(ts_b.Append(1'700'000'000'000 + i).ok());
        EXPECT_TRUE(c_b.Append(300 + i).ok());
    }
    std::shared_ptr<arrow::Array> ts_a, c_a;
    ASSERT_TRUE(ts_b.Finish(&ts_a).ok());
    ASSERT_TRUE(c_b.Finish(&c_a).ok());
    auto rb =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::int64(), true),
                                                arrow::field("c", arrow::int64(), true)}),
                                 4,
                                 {ts_a, c_a});

    EXPECT_FALSE(batcher.parse(*rb).has_value())
        << "parse() must refuse a batch narrower than the declared schema rather than pair "
           "columns with the wrong names";
}

// Declared order and batch order need not agree either.
TEST(ColumnarSchemaBinding, ReorderedBatchMaterialisesByName) {
    const std::vector<clink::sql::RowColumn> declared = {{"a", arrow::int64()},
                                                         {"b", arrow::int64()}};
    auto batcher = clink::sql::make_row_columnar_arrow_batcher(declared);

    // Batch order is (event_time, b, a) - the reverse of the declared order.
    arrow::Int64Builder ts_b, a_b, b_b;
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(ts_b.Append(1'700'000'000'000).ok());
        EXPECT_TRUE(a_b.Append(10 + i).ok());
        EXPECT_TRUE(b_b.Append(90 + i).ok());
    }
    std::shared_ptr<arrow::Array> ts_a, a_a, b_a;
    ASSERT_TRUE(ts_b.Finish(&ts_a).ok());
    ASSERT_TRUE(a_b.Finish(&a_a).ok());
    ASSERT_TRUE(b_b.Finish(&b_a).ok());
    auto rb =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::int64(), true),
                                                arrow::field("b", arrow::int64(), true),
                                                arrow::field("a", arrow::int64(), true)}),
                                 3,
                                 {ts_a, b_a, a_a});

    auto parsed = batcher.parse(*rb);
    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->size(), 3u);
    for (std::size_t i = 0; i < parsed->size(); ++i) {
        const auto& v = (*parsed)[i].value().values;
        ASSERT_TRUE(v.find("a") != v.end() && v.find("b") != v.end());
        EXPECT_EQ(v.at("a").as_int(), 10 + static_cast<std::int64_t>(i))
            << "a and b were swapped, so the closure read positionally";
        EXPECT_EQ(v.at("b").as_int(), 90 + static_cast<std::int64_t>(i));
    }
}

// Splitting must not force the parent's rows into existence: the point of the columnar
// shuffle is that a batch crosses it without ever being decoded.
TEST(ColumnarSplit, DoesNotMaterialiseTheParentBatch) {
    auto batch = make_batch(128);
    std::vector<int> targets(128);
    for (std::size_t i = 0; i < targets.size(); ++i) {
        targets[i] = static_cast<int>(i % 3);
    }
    const auto before = clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    auto parts = clink::gather_columnar_by_target<Row>(batch, targets, 3);
    ASSERT_TRUE(parts.has_value());
    for (const auto& p : *parts) {
        (void)p.size();  // size() answers from the sidecar
    }
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed), before)
        << "the split decoded rows it did not need";
}
