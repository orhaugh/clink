// Contract tests for make_columnar_arrow_batcher<T> + CLINK_ARROW_FIELDS.
//
// Unlike the binary-fallback batcher (make_default_arrow_batcher), a
// generated columnar batcher must:
//   1. emit one TYPED Arrow column per described field (not a single
//      value_bytes:binary blob),
//   2. round-trip parse(build(batch)) == batch over the real Arrow IPC
//      wire path (arrow_batch_to_ipc / arrow_batch_from_ipc),
//   3. REJECT a RecordBatch whose schema does not match (the dynamic_cast
//      guard in parse), matching the built-in batchers' contract.

#ifndef CLINK_HAS_ARROW
#error "test_columnar_batcher requires Arrow"
#endif

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/columnar_batcher.hpp"
#include "clink/core/record.hpp"

using namespace clink;

// Plain user aggregates. Defined at namespace scope with unique names so
// the CLINK_ARROW_FIELDS specialisations of clink::ArrowFields<> are
// well-formed and do not collide with other test TUs.
struct ColBatcherTrade {
    std::int64_t id;
    std::string symbol;
    double price;
    std::int32_t qty;
    bool buy;
};

// Same column count and event_time, but the first value column is a
// string where ColBatcherTrade has an int64 - used for the negative test.
struct ColBatcherWrongTypes {
    std::string a;
    std::string symbol;
    double price;
    std::int32_t qty;
    bool buy;
};

CLINK_ARROW_FIELDS(ColBatcherTrade, id, symbol, price, qty, buy);
CLINK_ARROW_FIELDS(ColBatcherWrongTypes, a, symbol, price, qty, buy);

namespace {

bool trade_eq(const ColBatcherTrade& a, const ColBatcherTrade& b) {
    return a.id == b.id && a.symbol == b.symbol && a.price == b.price && a.qty == b.qty &&
           a.buy == b.buy;
}

ColBatcherTrade trade_at(const Batch<ColBatcherTrade>& b, std::size_t i) {
    return b[i].value();
}

Batch<ColBatcherTrade> sample_batch() {
    Batch<ColBatcherTrade> in;
    in.emplace(ColBatcherTrade{1, "AAPL", 191.25, 100, true}, EventTime{1000});
    in.emplace(ColBatcherTrade{2, "MSFT", 410.10, 50, false}, EventTime{1001});
    in.emplace(ColBatcherTrade{3, "NVDA", 1203.99, 7, true});  // no event-time
    return in;
}

}  // namespace

TEST(ColumnarBatcher, SchemaIsTypedColumnsNotBinaryFallback) {
    auto batcher = make_columnar_arrow_batcher<ColBatcherTrade>();
    auto schema = batcher.schema();

    ASSERT_EQ(schema->num_fields(), 6);
    EXPECT_EQ(schema->field(0)->name(), "event_time");
    EXPECT_EQ(schema->field(0)->type()->id(), arrow::Type::INT64);
    EXPECT_TRUE(schema->field(0)->nullable());

    EXPECT_EQ(schema->field(1)->name(), "id");
    EXPECT_EQ(schema->field(1)->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(schema->field(2)->name(), "symbol");
    EXPECT_EQ(schema->field(2)->type()->id(), arrow::Type::STRING);
    EXPECT_EQ(schema->field(3)->name(), "price");
    EXPECT_EQ(schema->field(3)->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(schema->field(4)->name(), "qty");
    EXPECT_EQ(schema->field(4)->type()->id(), arrow::Type::INT32);
    EXPECT_EQ(schema->field(5)->name(), "buy");
    EXPECT_EQ(schema->field(5)->type()->id(), arrow::Type::BOOL);

    // The opaque fallback would have produced a single value_bytes:binary
    // column. Assert we did NOT.
    for (int i = 0; i < schema->num_fields(); ++i) {
        EXPECT_NE(schema->field(i)->type()->id(), arrow::Type::BINARY);
    }
}

TEST(ColumnarBatcher, SchemaIsStable) {
    auto batcher = make_columnar_arrow_batcher<ColBatcherTrade>();
    EXPECT_TRUE(batcher.schema()->Equals(*batcher.schema(), /*check_metadata=*/false));
}

TEST(ColumnarBatcher, RoundTripOverArrowIpcWire) {
    auto batcher = make_columnar_arrow_batcher<ColBatcherTrade>();
    auto in = sample_batch();

    auto rb = batcher.build(in);
    ASSERT_NE(rb, nullptr);
    EXPECT_EQ(rb->num_rows(), 3);
    EXPECT_EQ(rb->num_columns(), 6);
    EXPECT_TRUE(rb->schema()->Equals(*batcher.schema(), /*check_metadata=*/false));

    // Same bytes the NetworkChannel puts on the wire.
    auto ipc = arrow_batch_to_ipc(*rb);
    auto rb2 = arrow_batch_from_ipc(ipc.data(), ipc.size());
    ASSERT_NE(rb2, nullptr);

    auto out = batcher.parse(*rb2);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->size(), in.size());
    for (std::size_t i = 0; i < in.size(); ++i) {
        EXPECT_TRUE(trade_eq(trade_at(*out, i), trade_at(in, i)));
        const auto at = in[i].event_time();
        const auto bt = (*out)[i].event_time();
        ASSERT_EQ(at.has_value(), bt.has_value());
        if (at.has_value())
            EXPECT_EQ(at->millis(), bt->millis());
    }
}

TEST(ColumnarBatcher, ParseRejectsTooFewColumns) {
    // The int64 built-in batcher emits {event_time, value:int64} - 2 cols,
    // far fewer than the 6 a ColBatcherTrade batch needs.
    auto trade_batcher = make_columnar_arrow_batcher<ColBatcherTrade>();
    Batch<std::int64_t> ints;
    ints.emplace(42, EventTime{1});
    auto rb = int64_arrow_batcher().build(ints);
    ASSERT_NE(rb, nullptr);
    EXPECT_FALSE(trade_batcher.parse(*rb).has_value());
}

TEST(ColumnarBatcher, ParseRejectsTypeMismatch) {
    // Right column count, wrong column type in position 1 (string vs int64).
    // The dynamic_cast guard in parse must reject it.
    auto wrong = make_columnar_arrow_batcher<ColBatcherWrongTypes>();
    Batch<ColBatcherWrongTypes> wb;
    wb.emplace(ColBatcherWrongTypes{"not-an-int", "AAPL", 1.0, 1, true}, EventTime{1});
    auto rb = wrong.build(wb);
    ASSERT_NE(rb, nullptr);
    ASSERT_EQ(rb->num_columns(), 6);

    auto trade_batcher = make_columnar_arrow_batcher<ColBatcherTrade>();
    EXPECT_FALSE(trade_batcher.parse(*rb).has_value());
}
